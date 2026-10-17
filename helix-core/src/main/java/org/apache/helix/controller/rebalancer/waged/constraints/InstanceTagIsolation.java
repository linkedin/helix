package org.apache.helix.controller.rebalancer.waged.constraints;

/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;

import org.apache.helix.HelixRebalanceException;
import org.apache.helix.controller.rebalancer.waged.model.AssignableNode;
import org.apache.helix.controller.rebalancer.waged.model.AssignableReplica;
import org.apache.helix.controller.rebalancer.waged.model.ClusterModel;
import org.apache.helix.controller.rebalancer.waged.model.OptimalAssignment;
import org.apache.helix.model.Partition;
import org.apache.helix.model.ResourceAssignment;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Optional instance-tag ("clique") failure isolation for {@link ConstraintBasedAlgorithm}.
 *
 * WAGED is a global rebalancer: it walks one globally sorted list of every replica and aborts the
 * whole pass as soon as one replica cannot be placed. In a cluster carved into disjoint cliques
 * (each instance carries one instance tag and each resource is pinned to one tag through
 * INSTANCE_GROUP_TAG) that means one unplaceable clique freezes every other clique.
 *
 * This class does not change how a placement is chosen, and it does not reorder anything. It only
 * changes what happens when something fails. When a placement fails, the replicas already placed
 * for that replica's share block (its isolation group and every group connected to it through
 * shared nodes, see below) are released, the rest of the block is skipped, and the pass carries
 * on. When the cluster wide capacity check that precedes any placement fails,
 * {@link #absorbCapacityDeficit} sets the groups that caused the deficit aside before anything is
 * placed. The caller then carries the skipped resources' previous assignment forward, so no
 * resource is emitted half assigned, and leaves out a skipped resource that has none.
 *
 * <h3>Why the isolation unit is the tag and not the resource</h3>
 * Rolling back only the broken resource would free capacity that its healthy siblings on the same
 * tag would immediately consume, so the emitted result (recalculated siblings plus the carried over
 * broken resource) could overcommit the clique's nodes.
 *
 * <h3>Why the isolation unit is the whole share block</h3>
 * The same overcommit argument applies across groups whenever two groups can land on the same node.
 * So a group is never isolated alone: it is carried over together with every group it shares a node
 * with, followed transitively. Sharing a node is symmetric, so that relation partitions the groups
 * into blocks, and by construction no group outside a block can reach a node the block occupies.
 * Rolling a whole block back therefore frees capacity that nothing still being calculated can take.
 * The blocks follow the tags instances carry in this calculation, so WagedRebalanceUtil also
 * checks the carried over entries against the fresh ones by instance name and carries a colliding
 * resource forward too. Two carried over entries that name one instance are left as they are.
 *
 * The blocks are what keeps this useful rather than all or nothing. A cluster of disjoint cliques
 * is one block per clique, so a single broken clique is carried over while every other clique is
 * rebalanced normally. Overlap only widens the block that actually overlaps. An untagged resource
 * can go anywhere, so a calculation that includes it pulls every group it meets into one cluster
 * wide block, and a failure there fails the run exactly as in the default mode. The baseline, the
 * emergency rebalance and the delayed rebalance overwrite include a newly added untagged resource
 * from the start, although the last two place none of its replicas. The partial rebalance leaves
 * out a resource the baseline does not hold yet, as the default mode does, so the new resource
 * merges the groups there only once the baseline places it. This mode is never worse than the
 * default one.
 *
 * <h3>Parity</h3>
 * Every method is a no-op while disabled. While enabled, {@link #shouldSkip} and
 * {@link #recordPlacement} run for every replica but are bookkeeping only: until something fails,
 * the first registers the replica's group and returns false, and the second remembers the
 * placement. {@link #failureSink} only changes where a failure is recorded, {@link #finish} returns
 * at once unless something failed, and the remaining entry points are only reached on a path where
 * the default mode has already decided to throw. A run in which nothing fails therefore produces
 * exactly the same assignment either way, for any topology.
 *
 * Instances are stateful and scoped to a single
 * {@link ConstraintBasedAlgorithm#calculate(ClusterModel)} run. They are not thread safe, which
 * matches the single threaded assignment loop that owns them.
 */
class InstanceTagIsolation {
  private static final Logger LOG = LoggerFactory.getLogger(InstanceTagIsolation.class);
  // Prefixes for the isolation group keys, so a tag and a resource with the same name can never
  // collide into one group.
  private static final String TAG_GROUP_PREFIX = "tag:";
  private static final String UNTAGGED_GROUP_PREFIX = "untagged-resource:";
  // Names the block of a node that no group reaches. Only ever used by the deficit attribution.
  private static final String INSTANCE_GROUP_PREFIX = "instance:";
  private static final Comparator<AssignableReplica> REPLICA_ORDER =
      Comparator.comparing(AssignableReplica::getResourceName)
          .thenComparing(AssignableReplica::getPartitionName)
          .thenComparing(AssignableReplica::getReplicaState);

  private final boolean _enabled;
  private final ClusterModel _clusterModel;
  private final List<AssignableNode> _nodes;

  // Every isolation group observed during the run, reported in the summary logged when some of them
  // are skipped.
  private final Set<String> _allGroups = new HashSet<>();
  private final Set<String> _failedGroups = new HashSet<>();
  private final Set<String> _skippedResources = new HashSet<>();
  // What this run has placed so far per group, so a group can be rolled back exactly.
  private final Map<String, List<Placement>> _placementsByGroup = new HashMap<>();
  // Failures are funneled here while isolating so a tolerated group failure never marks the
  // returned OptimalAssignment as failed, which would make getOptimalResourceAssignment throw.
  // Replaced on every tolerated failure so each group's diagnosis only reports its own reasons.
  private OptimalAssignment _failureSink;
  private HelixRebalanceException _firstFailure;
  // Computed on the first failure only, so the happy path stays identical to the default mode.
  private List<Set<String>> _shareBlocks;
  private List<Set<String>> _attributionBlocks;
  private Map<String, String> _tagByGroup;

  InstanceTagIsolation(ClusterModel clusterModel, List<AssignableNode> nodes) {
    _enabled = clusterModel.getContext().isInstanceTagIsolationEnabled();
    _clusterModel = clusterModel;
    _nodes = nodes;
    _failureSink = _enabled ? new OptimalAssignment() : null;
  }

  /**
   * Where hard constraint failures for the current replica should be recorded.
   *
   * While disabled this is the caller's own {@link OptimalAssignment}, which is exactly what the
   * algorithm uses without isolation. While enabled it is a throwaway sink, so a tolerated group
   * failure never leaves the returned assignment marked as failed.
   */
  OptimalAssignment failureSink(OptimalAssignment defaultSink) {
    return _enabled ? _failureSink : defaultSink;
  }

  /**
   * Whether this replica should be skipped because its group was set aside with its share block.
   *
   * Also registers the replica's group, which the summary logged by {@link #finish} counts.
   *
   * @return true when the caller should move on to the next replica.
   */
  boolean shouldSkip(AssignableReplica replica) {
    if (!_enabled) {
      return false;
    }
    String group = groupKey(replica);
    _allGroups.add(group);
    if (!_failedGroups.contains(group)) {
      return false;
    }
    // The whole group is being carried over unchanged, so do not assign the rest of it piecemeal.
    _skippedResources.add(replica.getResourceName());
    return true;
  }

  /**
   * Remember a placement so it can be released if this replica's group later fails.
   */
  void recordPlacement(AssignableReplica replica, AssignableNode node) {
    if (!_enabled) {
      return;
    }
    _placementsByGroup.computeIfAbsent(groupKey(replica), key -> new ArrayList<>()).add(
        new Placement(replica.getResourceName(), replica.getPartitionName(),
            replica.getReplicaState(), node.getInstanceName()));
  }

  /**
   * Try to tolerate a replica that could not be placed by rolling back its whole share block and
   * skipping whatever the block has left to place.
   *
   * @return true when the group was isolated and the caller should continue with the next replica,
   *         false when the caller must throw and fail the whole rebalance as it does by default.
   */
  boolean tryIsolate(AssignableReplica replica, HelixRebalanceException failure) {
    if (!_enabled) {
      return false;
    }
    String group = groupKey(replica);
    // Start the next group's diagnosis from a clean sink. The caller has already read the current
    // one to build the failure passed in.
    _failureSink = new OptimalAssignment();
    // Carry over the failing group together with every group connected to it through shared
    // nodes, so the capacity the whole set occupies is off limits to everything still being
    // calculated.
    Set<String> closure = blockOf(group);
    // Isolation is pointless when the cluster holds no second block to recalculate around, which is
    // what an untagged resource or a tag bridging instance produces by merging every group it meets
    // into one. A group that reaches no node still makes a block of its own here, so it can let
    // isolation go ahead, and finish then fails the run when every group that reaches a node
    // failed. Measured over every block the cluster's resources form, including the ones whose
    // replicas are all placed already, rather than over the groups that happen to have outstanding
    // replicas this run: a partial rebalance can carry work for a single clique, and comparing
    // against that would read as "shares nodes with every other group" on a cluster of twenty
    // independent cliques and fail them all. A tag carried only by nodes no resource is pinned to
    // (a spare pool, or a clique whose resources do not exist yet) does not make a block here. It
    // holds nothing that could keep rebalancing, so counting it would turn a failure of the only
    // clique into a rebalance that quietly carries everything over.
    if (shareBlocks().size() < 2) {
      LOG.warn(
          "Instance tag isolation cannot isolate group {} during the {} rebalance of cluster {}: "
              + "it shares nodes with every other group that holds resources, so there is nothing "
              + "left to recalculate around it. Failing "
              + "the whole rebalance exactly like the default global mode.", group,
          _clusterModel.getRebalanceScopeType(),
          _clusterModel.getContext().getClusterName());
      return false;
    }
    int released = 0;
    // Releasing gives back the node capacity this run's placements for the whole block used and
    // removes their fault zone entries, leaving the other blocks' placements in place. Those
    // updates commute, so the release order is not significant.
    for (String member : closure) {
      List<Placement> placements = _placementsByGroup.remove(member);
      if (placements != null) {
        for (int i = placements.size() - 1; i >= 0; i--) {
          Placement placement = placements.get(i);
          _clusterModel.release(placement._resourceName, placement._partitionName, placement._state,
              placement._instanceName);
          _skippedResources.add(placement._resourceName);
          released++;
        }
      }
      _failedGroups.add(member);
    }
    _skippedResources.add(replica.getResourceName());
    if (_firstFailure == null) {
      _firstFailure = failure;
    }
    LOG.warn(
        "Instance tag isolation: rolling back and skipping group {} together with {} group(s) it "
            + "shares nodes with, during the {} rebalance of cluster {}. {} replica(s) already "
            + "placed were released. Skipped set: {}. Every other group keeps its newly calculated "
            + "assignment unless every group that reaches a node fails, which fails the whole "
            + "rebalance.", group, closure.size() - 1, _clusterModel.getRebalanceScopeType(),
        _clusterModel.getContext().getClusterName(), released, closure, failure);
    return true;
  }

  /**
   * Publish the isolation outcome onto the assignment the algorithm is about to return.
   *
   * @throws HelixRebalanceException when every independent block of the cluster that holds
   *         resources and reaches a node of the scope's model failed, so that the caller's usual
   *         failure handling, metrics and last known good fallback all apply.
   */
  void finish(OptimalAssignment optimalAssignment) throws HelixRebalanceException {
    if (!_enabled || _failedGroups.isEmpty()) {
      return;
    }
    // Outstanding work can belong only to the failing clique, and a baseline can be incremental, so
    // whether anything survived is judged over the full resource inventory, allocated groups
    // included, rather than over the replicas being reassigned. An unrelated resource edit must not
    // turn one broken clique into a global failure. The same rule holds in every scope, and only
    // groups that hold resources and reach a node of the scope's model count. A tag carried only by
    // nodes no resource is pinned to never fails, so counting it as a surviving block would hide a
    // genuine cluster wide failure behind a spare pool. A resource group whose tag no node of the
    // model carries (a resource added before any instance carries its tag, every instance retagged
    // away, or in the delayed rebalance overwrite every instance offline inside the delay window)
    // is the same case from the other side. It has no node to rebalance onto, and with nothing to
    // place it never fails, so counting it as a survivor would carry everything over in place of
    // the failure. A failure always takes its whole share block with it, so every reaching group
    // failing is the same as every block that reaches a node failing.
    if (_failedGroups.containsAll(groupsReachingANode())) {
      // Every independent part of the cluster that reaches a node failed, so behave exactly like
      // the default global mode.
      throw _firstFailure;
    }
    List<Set<String>> blocks = shareBlocks();
    long failedBlocks =
        blocks.stream().filter(block -> block.stream().anyMatch(_failedGroups::contains)).count();
    _clusterModel.getContext().getResourceInstanceGroupTags().forEach((resource, tag) -> {
      if (_failedGroups.contains(groupKey(resource, tag))) {
        _skippedResources.add(resource);
      }
    });
    LOG.warn(
        "Instance tag isolation skipped {} of {} group(s) ({} resource(s)) in {} of {} independent "
            + "block(s) during the {} rebalance of cluster {}. Skipped groups: {}.",
        _failedGroups.size(), Math.max(_allGroups.size(), tagByGroup().size()),
        _skippedResources.size(), failedBlocks, blocks.size(),
        _clusterModel.getRebalanceScopeType(), _clusterModel.getContext().getClusterName(),
        _failedGroups);
    optimalAssignment.setSkippedResources(_skippedResources);
  }

  /**
   * Attribute a cluster wide capacity deficit to the cliques that caused it.
   *
   * The cluster wide check that precedes any placement is a tag blind sum, so one wildly
   * oversubscribed clique can drag it negative while every other clique still fits comfortably on
   * its own nodes. Throwing there would freeze the whole cluster, which is exactly what this mode
   * exists to prevent. This splits the check's totals across the attribution blocks, sets aside
   * the ones whose own replicas cannot fit on their own nodes, and re-evaluates the deficit on the
   * remainder. The blocks set aside are then carried over like any other failed group.
   *
   * A block is first judged by what this scope asks of its nodes: the replicas placed on them, the
   * ones to assign and the ones not placed anywhere yet. A replica parked on a node outside the
   * scope's model, typically one still offline inside its delayed rebalance window, is part of the
   * check's total but needs no room in this scope, so a clique running above its live share while a
   * node is away is never blamed for another clique's shortfall. Only when that verdict still
   * leaves a deficit behind is every block judged by all the replicas it owns. The remainder is
   * always held to the check's full totals, the test the default mode applies to a cluster made of
   * the remainder alone, so a clique running above its live share keeps rebalancing only while the
   * remainder's spare capacity covers its parked replicas, as the default mode requires.
   *
   * Only reachable on a path where the default mode has already decided to throw, so parity is
   * unaffected by construction.
   *
   * @param deficit the failure the default mode would have thrown.
   * @param divGuard the share of each key's capacity the algorithm adds to keep the scoring
   *                 denominators above zero, also used here as their floor.
   * @return the scoring capacity map to use for the remainder of the cluster, or null when the
   *         deficit cannot be attributed and the caller should throw as usual.
   */
  Map<String, Float> absorbCapacityDeficit(HelixRebalanceException deficit, float divGuard) {
    if (!_enabled) {
      return null;
    }
    // Setting a block aside only helps when a second block that holds resources is left to
    // rebalance around it, which is the rule tryIsolate applies too, so a block made only of tags
    // no resource is pinned to does not count toward it. A single block of resources spanning the
    // cluster is the usual effect of an untagged resource, since it can be placed on any node and
    // so pulls every group it meets into one block. It is not guaranteed though: a tag that no
    // node in the cluster model carries reaches no node, so it stays a block of its own alongside
    // the big one, and the attribution below then correctly blames that unplaceable group.
    if (shareBlocks().size() < 2) {
      return null;
    }
    DeficitLedger ledger = new DeficitLedger();
    boolean judgedByScope = true;
    Set<Integer> atFault = ledger.overCommittedBlocks(true);
    Map<String, Float> residualScoringCap = ledger.residualScoringCapacity(atFault, divGuard);
    if (residualScoringCap == null) {
      judgedByScope = false;
      atFault = ledger.overCommittedBlocks(false);
      residualScoringCap = ledger.residualScoringCapacity(atFault, divGuard);
    }
    if (residualScoringCap == null) {
      return null;
    }

    Set<String> deficitGroups = new TreeSet<>();
    for (Integer block : atFault) {
      deficitGroups.addAll(ledger.groupsOf(block));
    }
    // Carry the groups at fault over like any other failed group, and seed the failure that a
    // fully failed run rethrows. A block made only of nodes no resource is pinned to has nothing to
    // carry over.
    Set<String> carried = new TreeSet<>();
    deficitGroups.stream().filter(tagByGroup()::containsKey).forEach(carried::add);
    _failedGroups.addAll(carried);
    _firstFailure = deficit;
    if (carried.isEmpty()) {
      LOG.warn(
          "Instance tag isolation set aside a cluster wide capacity deficit during the {} "
              + "rebalance of cluster {}: it comes only from replicas left on nodes no resource "
              + "can be placed on ({}). Nothing is carried over and the whole cluster is "
              + "rebalanced.", _clusterModel.getRebalanceScopeType(),
          _clusterModel.getContext().getClusterName(), deficitGroups, deficit);
    } else {
      LOG.warn(
          "Instance tag isolation attributed a cluster wide capacity deficit during the {} "
              + "rebalance of cluster {} to group(s) {}, which cannot hold {} on their own nodes. "
              + "They are carried over and the rest of the cluster is rebalanced normally.",
          _clusterModel.getRebalanceScopeType(), _clusterModel.getContext().getClusterName(),
          deficitGroups,
          judgedByScope ? "what this rebalance must place" : "all the replicas they own", deficit);
    }
    return residualScoringCap;
  }

  /**
   * The cluster wide capacity check's totals, split across the attribution blocks.
   *
   * Every replica the check sums starts out charged to the block that owns it, and every node's
   * capacity is credited to exactly one block, so the blocks always add up to the check's totals
   * and a residual computed from them stays consistent with it. A node no group reaches, because it
   * carries no tag or only labels shared with nodes that resources do use, gets a block of its own:
   * nothing can be placed on it, but what is left on it still counts toward the total.
   */
  private final class DeficitLedger {
    private final List<Set<String>> _blocks;
    private final Set<Integer> _resourceBlocks = new HashSet<>();
    private final Map<Integer, Map<String, Long>> _capacity = new HashMap<>();
    private final Map<Integer, Map<String, Long>> _demand = new HashMap<>();
    // The part of each block's demand parked on nodes outside this scope's model.
    private final Map<Integer, Map<String, Long>> _parked = new HashMap<>();

    private DeficitLedger() {
      _blocks = new ArrayList<>(attributionBlocks());
      Map<String, Integer> blockOfGroup = new HashMap<>();
      for (int i = 0; i < _blocks.size(); i++) {
        for (String group : _blocks.get(i)) {
          blockOfGroup.put(group, i);
        }
      }
      Integer untaggedBlock = null;
      for (Map.Entry<String, String> entry : tagByGroup().entrySet()) {
        Integer block = blockOfGroup.get(entry.getKey());
        if (block == null) {
          continue;
        }
        _resourceBlocks.add(block);
        if (entry.getValue() == null) {
          // Every untagged group can use every node, so they all share one block.
          untaggedBlock = block;
        }
      }

      // The population the check sums, per partition.
      Map<String, Map<String, PartitionShare>> shares = new HashMap<>();
      for (AssignableReplica replica : _clusterModel.getContext().getManagedReplicas()) {
        Integer owner = blockOfGroup.get(groupKey(replica));
        PartitionShare share =
            shares.computeIfAbsent(replica.getResourceName(), key -> new HashMap<>())
                .computeIfAbsent(replica.getPartitionName(),
                    key -> new PartitionShare(owner, replica.getCapacity()));
        share._population++;
        if (owner != null) {
          charge(_demand, owner, replica.getCapacity(), 1);
        }
      }
      for (Set<AssignableReplica> replicas : _clusterModel.getAssignableReplicaMap().values()) {
        for (AssignableReplica replica : replicas) {
          PartitionShare share = shareOf(shares, replica);
          if (share != null) {
            share._toAssign++;
          }
        }
      }

      // Sorted, so which replicas count toward a partition that has more placed than it owns never
      // depends on hash order.
      List<AssignableNode> nodes = new ArrayList<>(_nodes);
      nodes.sort(Comparator.comparing(AssignableNode::getInstanceName));
      int[] nodeBlock = new int[nodes.size()];
      List<Map<String, Long>> room = new ArrayList<>(nodes.size());
      List<List<AssignableReplica>> foreign = new ArrayList<>(nodes.size());
      for (int i = 0; i < nodes.size(); i++) {
        AssignableNode node = nodes.get(i);
        // Every group reaching a node is in that node's block by construction, so any one of them
        // identifies the block and the node's capacity is credited exactly once. A node reachable
        // from two blocks would have merged them, so nothing is ever counted twice. An untagged
        // group reaches every node, so it names the block of all of them.
        Integer block = untaggedBlock;
        if (block == null) {
          for (String tag : node.getInstanceTags()) {
            block = blockOfGroup.get(TAG_GROUP_PREFIX + tag);
            if (block != null) {
              break;
            }
          }
        }
        if (block == null) {
          block = _blocks.size();
          _blocks.add(Collections.singleton(INSTANCE_GROUP_PREFIX + node.getInstanceName()));
        }
        int blockId = block;
        nodeBlock[i] = blockId;
        charge(_capacity, blockId, node.getMaxCapacity(), 1);
        Map<String, Long> nodeRoom = new HashMap<>();
        node.getMaxCapacity().forEach((key, value) -> nodeRoom.put(key, (long) value));
        room.add(nodeRoom);
        // A block's own replicas are counted first, so a replica beyond what its partition owns is
        // always one left on another block's node.
        List<AssignableReplica> away = new ArrayList<>();
        for (AssignableReplica replica : node.getAssignedReplicas()) {
          PartitionShare share = shareOf(shares, replica);
          if (share == null || share._owner == null) {
            continue;
          }
          if (share._owner == blockId) {
            if (share.place()) {
              replica.getCapacity()
                  .forEach((key, value) -> nodeRoom.merge(key, (long) -value, Long::sum));
            }
          } else {
            away.add(replica);
          }
        }
        // Sorted so which replicas fit on a node too full for all of them never depends on hash
        // order.
        away.sort(REPLICA_ORDER);
        foreign.add(away);
      }
      for (int i = 0; i < nodes.size(); i++) {
        chargeForeignReplicas(foreign.get(i), nodeBlock[i], room.get(i), shares);
      }

      Set<String> modelled = _clusterModel.getAssignableLogicalIds();
      Map<String, ResourceAssignment> current =
          _clusterModel.getContext().getBestPossibleAssignment();
      shares.forEach((resource, byPartition) -> {
        ResourceAssignment assignment = current.get(resource);
        if (assignment == null) {
          return;
        }
        byPartition.forEach((partition, share) -> {
          int unaccounted = share._population - share._placed - share._toAssign;
          if (unaccounted <= 0 || share._owner == null) {
            return;
          }
          // A replica to assign takes the place of one that sits on a node outside the model, so
          // only what is left over is parked.
          int away = -share._toAssign;
          for (String logicalId : assignment.getReplicaMap(new Partition(partition)).keySet()) {
            if (!modelled.contains(logicalId)) {
              away++;
            }
          }
          int parked = Math.min(unaccounted, away);
          if (parked > 0) {
            charge(_parked, share._owner, share._weight, parked);
          }
        });
      });
    }

    /**
     * Charge the replicas left on another block's node to the block that really pays for them.
     *
     * Every replica starts out charged to the block that owns it. One left on another block's node,
     * typically a carried clique's replica on a node that has since been retagged, takes that
     * node's capacity away from the node's own groups, so it moves to the node's block, but only as
     * far as the room those groups leave on the node. A replica past that point takes nothing more
     * from them, and charging it there would blame a clique for a node another clique over filled,
     * so it stays with the clique it belongs to. A block of nodes no resource is pinned to protects
     * no work, so it takes every replica left on its nodes and can then be set aside at no cost. A
     * replica beyond what its partition owns, such as a partition a carried resource no longer has,
     * is outside the check's total and moves nothing.
     */
    private void chargeForeignReplicas(List<AssignableReplica> replicas, int block,
        Map<String, Long> room, Map<String, Map<String, PartitionShare>> shares) {
      boolean nodeOnly = !_resourceBlocks.contains(block);
      for (AssignableReplica replica : replicas) {
        PartitionShare share = shareOf(shares, replica);
        if (!share.place()) {
          continue;
        }
        Map<String, Integer> weight = replica.getCapacity();
        if (!nodeOnly && weight.entrySet().stream()
            .anyMatch(used -> used.getValue() > room.getOrDefault(used.getKey(), 0L))) {
          continue;
        }
        charge(_demand, share._owner, weight, -1);
        charge(_demand, block, weight, 1);
        weight.forEach((key, value) -> room.merge(key, (long) -value, Long::sum));
      }
    }

    /**
     * The blocks whose demand exceeds their own capacity on any key, judged either by what this
     * scope must place or by every replica they own.
     */
    private Set<Integer> overCommittedBlocks(boolean scopeOnly) {
      Set<Integer> atFault = new TreeSet<>();
      for (Map.Entry<Integer, Map<String, Long>> entry : _demand.entrySet()) {
        Map<String, Long> capacity =
            _capacity.getOrDefault(entry.getKey(), Collections.emptyMap());
        Map<String, Long> parked = scopeOnly
            ? _parked.getOrDefault(entry.getKey(), Collections.emptyMap())
            : Collections.emptyMap();
        for (Map.Entry<String, Long> demand : entry.getValue().entrySet()) {
          if (demand.getValue() - parked.getOrDefault(demand.getKey(), 0L)
              > capacity.getOrDefault(demand.getKey(), 0L)) {
            atFault.add(entry.getKey());
            break;
          }
        }
      }
      return atFault;
    }

    /**
     * The scoring capacity left once the given blocks are set aside with everything they own, or
     * null when the default mode's verdict stands.
     */
    private Map<String, Float> residualScoringCapacity(Set<Integer> atFault, float divGuard) {
      // Nothing to blame, or every block that holds resources is to blame, both of which mean the
      // default mode's verdict stands. A block made only of nodes no resource is pinned to can
      // still be set aside for its own overcommitment, such as a stale placement left on a spare
      // node, but it holds no work of its own, so it never counts as the part of the cluster that
      // keeps rebalancing.
      if (atFault.isEmpty() || atFault.containsAll(_resourceBlocks)) {
        return null;
      }
      Map<String, Long> residualCapacity =
          new HashMap<>(_clusterModel.getContext().getClusterCapacityMap());
      Map<String, Long> residualDemand = new HashMap<>();
      _clusterModel.getContext().getEstimateUtilizationMap().forEach((key, remaining) ->
          residualDemand.put(key, residualCapacity.getOrDefault(key, 0L) - remaining));
      for (Integer block : atFault) {
        _demand.getOrDefault(block, Collections.emptyMap())
            .forEach((key, value) -> residualDemand.merge(key, -value, Long::sum));
        _capacity.getOrDefault(block, Collections.emptyMap())
            .forEach((key, value) -> residualCapacity.merge(key, -value, Long::sum));
      }
      Map<String, Float> residualScoringCap = new HashMap<>();
      for (Map.Entry<String, Long> entry : residualCapacity.entrySet()) {
        long remaining = entry.getValue() - residualDemand.getOrDefault(entry.getKey(), 0L);
        if (remaining < 0) {
          // What is left over still does not fit, so this is a genuine cluster wide shortfall
          // rather than one bad clique.
          return null;
        }
        // Floored so the denominator stays above zero. The default path divides by the estimated
        // remainder plus a DIV_GUARD share of the full cluster capacity, which is positive for any
        // key the cluster has capacity for, but a residual can reach zero when every node belongs
        // to a block that was set aside, and dividing by it would score Infinity or NaN and break
        // the ordering the algorithm sorts on.
        residualScoringCap.put(entry.getKey(),
            Math.max((float) remaining + (entry.getValue() * divGuard), divGuard));
      }
      return residualScoringCap;
    }

    private Set<String> groupsOf(int block) {
      return _blocks.get(block);
    }

    private PartitionShare shareOf(Map<String, Map<String, PartitionShare>> shares,
        AssignableReplica replica) {
      Map<String, PartitionShare> byPartition = shares.get(replica.getResourceName());
      return byPartition == null ? null : byPartition.get(replica.getPartitionName());
    }

    private void charge(Map<Integer, Map<String, Long>> ledger, int block,
        Map<String, Integer> weight, long times) {
      Map<String, Long> entry = ledger.computeIfAbsent(block, key -> new HashMap<>());
      weight.forEach((key, value) -> entry.merge(key, times * value, Long::sum));
    }
  }

  /**
   * What the check's population holds of one partition, and how much of it this scope has placed
   * on its nodes or has still to assign. A replica's weight depends only on its partition, never on
   * its state.
   */
  private static final class PartitionShare {
    private final Integer _owner;
    private final Map<String, Integer> _weight;
    private int _population;
    private int _placed;
    private int _toAssign;

    private PartitionShare(Integer owner, Map<String, Integer> weight) {
      _owner = owner;
      _weight = weight;
    }

    /** Count one placed replica, or report that the partition has no replica left to place. */
    private boolean place() {
      if (_placed >= _population) {
        return false;
      }
      _placed++;
      return true;
    }
  }

  /**
   * The isolation group of a replica.
   *
   * A resource pinned to an instance group tag can only ever be placed on that tag's nodes, so the
   * tag is the failure domain the operator declared: every resource sharing the tag competes for
   * the same nodes and is carried over together. A resource with no tag has no declared domain and
   * can be placed anywhere, so it is keyed on its own, though it then shares nodes with everything.
   */
  private static String groupKey(AssignableReplica replica) {
    return groupKey(replica.getResourceName(), replica.getResourceInstanceGroupTag());
  }

  private static String groupKey(String resource, String tag) {
    return (tag == null || tag.isEmpty()) ? UNTAGGED_GROUP_PREFIX + resource
        : TAG_GROUP_PREFIX + tag;
  }

  /**
   * The groups partitioned into share blocks, computed once per run and cached.
   *
   * Sharing a node is a symmetric relation on groups, so its transitive closure is an equivalence
   * and the blocks are a genuine partition of the groups. No node is ever reachable from two
   * blocks, because such a node would have merged them. Note this is not a partition of the nodes:
   * an instance carrying only labels no resource is pinned to belongs to no block at all, unless
   * the model holds an untagged resource, which reaches every node.
   *
   * In the clique topology this mode targets every instance carries exactly one clique tag, so
   * every block is a single clique and this degenerates to one block per clique.
   */
  private List<Set<String>> shareBlocks() {
    if (_shareBlocks != null) {
      return _shareBlocks;
    }
    _shareBlocks = blocksOf(tagByGroup());
    return _shareBlocks;
  }

  /**
   * The same partition computed over the groups the cluster wide capacity deficit has to be
   * attributed across, which is a wider set than the one above.
   *
   * Resource groups include already allocated replicas. Node-only tags also contribute blocks for
   * models that have nodes outside the represented workload, without letting operational labels
   * join otherwise independent resource groups. That keeps a spare pool's capacity, and anything
   * stale still sitting on it, accounted to the pool rather than to the rest of the cluster.
   *
   * A tag carried by a node that also carries some resource's instance group tag is deliberately
   * left out. Such a tag is an operational label spanning cliques (an availability zone, a hardware
   * generation, a pool name) rather than the name of a clique that lost its replicas. Adding it
   * would create a group no resource can ever be placed in, and because the label is shared it
   * would union every clique carrying it into one block. The deficit attribution would then see a
   * single block and could never set one clique aside on any fleet whose instances carry an
   * ordinary label alongside their clique tag.
   *
   * Kept separate from the resource partition on purpose. A block made only of node tags holds no
   * work, so it never fails and nothing in it is ever carried over. Whether some part of the
   * cluster survived is therefore judged over the resource partition alone, and counting such a
   * block as a survivor would hide a genuine cluster wide failure behind a spare pool.
   */
  private List<Set<String>> attributionBlocks() {
    if (_attributionBlocks != null) {
      return _attributionBlocks;
    }
    Map<String, String> tagByGroup = new HashMap<>(tagByGroup());
    Set<String> resourceTags = new HashSet<>(tagByGroup.values());
    resourceTags.remove(null);
    // Operational labels on reachable nodes must not connect otherwise independent cliques.
    Set<String> spanningTags = new HashSet<>();
    for (AssignableNode node : _nodes) {
      Set<String> nodeTags = node.getInstanceTags();
      boolean reachable = false;
      for (String tag : nodeTags) {
        if (resourceTags.contains(tag)) {
          reachable = true;
          break;
        }
      }
      if (reachable) {
        spanningTags.addAll(nodeTags);
      }
    }
    for (AssignableNode node : _nodes) {
      for (String tag : node.getInstanceTags()) {
        if (!spanningTags.contains(tag)) {
          tagByGroup.putIfAbsent(TAG_GROUP_PREFIX + tag, tag);
        }
      }
    }
    _attributionBlocks = blocksOf(tagByGroup);
    return _attributionBlocks;
  }

  private List<Set<String>> blocksOf(Map<String, String> tagByGroup) {
    List<String> groups = new ArrayList<>(new TreeSet<>(tagByGroup.keySet()));
    Map<String, Integer> indexOf = new HashMap<>();
    for (int i = 0; i < groups.size(); i++) {
      indexOf.put(groups.get(i), i);
    }
    int[] parent = new int[groups.size()];
    for (int i = 0; i < parent.length; i++) {
      parent[i] = i;
    }

    // A tagged group's key is derived from its tag, so tag to group is a bijection. That is what
    // keeps this cheap: the groups reaching a node are just the node's own tags rather than a scan
    // of every group, which on a cluster with thousands of untagged resources is the difference
    // between milliseconds and tens of seconds on the failure path.
    Map<String, Integer> groupByTag = new HashMap<>();
    int untaggedRoot = -1;
    for (Map.Entry<String, String> entry : tagByGroup.entrySet()) {
      int index = indexOf.get(entry.getKey());
      if (entry.getValue() == null) {
        // An untagged group can use every node, so all of them share every node with each other,
        // provided there is a node at all to share.
        if (untaggedRoot < 0) {
          untaggedRoot = index;
        } else if (!_nodes.isEmpty()) {
          union(parent, untaggedRoot, index);
        }
      } else {
        groupByTag.put(entry.getValue(), index);
      }
    }

    for (AssignableNode node : _nodes) {
      // Seeded with the untagged groups, which reach this node as well, so any tagged group here
      // is joined to them too.
      int previous = untaggedRoot;
      for (String tag : node.getInstanceTags()) {
        Integer index = groupByTag.get(tag);
        if (index == null) {
          // A label that names no group here, such as an AZ or hardware tag, joins nothing.
          continue;
        }
        if (previous >= 0) {
          union(parent, previous, index);
        }
        previous = index;
      }
    }

    Map<Integer, Set<String>> byRoot = new LinkedHashMap<>();
    for (int i = 0; i < groups.size(); i++) {
      byRoot.computeIfAbsent(find(parent, i), key -> new TreeSet<>()).add(groups.get(i));
    }
    return new ArrayList<>(byRoot.values());
  }

  private static int find(int[] parent, int i) {
    while (parent[i] != i) {
      parent[i] = parent[parent[i]];
      i = parent[i];
    }
    return i;
  }

  private static void union(int[] parent, int a, int b) {
    int rootA = find(parent, a);
    int rootB = find(parent, b);
    if (rootA != rootB) {
      parent[Math.max(rootA, rootB)] = Math.min(rootA, rootB);
    }
  }

  /** The share block containing this group, which is the unit that is carried over together. */
  private Set<String> blockOf(String group) {
    for (Set<String> block : shareBlocks()) {
      if (block.contains(group)) {
        return block;
      }
    }
    return Collections.singleton(group);
  }

  /**
   * The resource groups that reach at least one node of the scope's model, by the rule the share
   * blocks are built from: an untagged group reaches every node, and a tagged group the nodes
   * carrying its tag. Computed only on the failure path.
   */
  private Set<String> groupsReachingANode() {
    Set<String> nodeTags = new HashSet<>();
    for (AssignableNode node : _nodes) {
      nodeTags.addAll(node.getInstanceTags());
    }
    Set<String> reaching = new HashSet<>();
    tagByGroup().forEach((group, tag) -> {
      if (tag == null ? !_nodes.isEmpty() : nodeTags.contains(tag)) {
        reaching.add(group);
      }
    });
    return reaching;
  }

  /** A group's tag, or null for an untagged group, which can use every node. */
  private Map<String, String> tagByGroup() {
    if (_tagByGroup == null) {
      Map<String, String> tagByGroup = new HashMap<>();
      _clusterModel.getContext().getResourceInstanceGroupTags().forEach((resource, tag) ->
          tagByGroup.put(groupKey(resource, tag), (tag == null || tag.isEmpty()) ? null : tag));
      _tagByGroup = tagByGroup;
    }
    return _tagByGroup;
  }


  /**
   * A placement made during this run, kept so it can be released if the group later fails.
   *
   * All four fields are needed because the release path is keyed by state. With any other state
   * the node keeps the replica and only logs a warning, while the fault zone entry is still
   * removed, and a state the model does not index throws.
   */
  private static final class Placement {
    private final String _resourceName;
    private final String _partitionName;
    private final String _state;
    private final String _instanceName;

    private Placement(String resourceName, String partitionName, String state,
        String instanceName) {
      _resourceName = resourceName;
      _partitionName = partitionName;
      _state = state;
      _instanceName = instanceName;
    }
  }
}
