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
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import java.util.function.Predicate;

import com.google.common.base.Strings;
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
 * {@link #recordPlacement} run for every replica but never change where a replica goes: until
 * something fails, the first returns false and the second only records the placement.
 * {@link #failureSink} only changes where a failure is recorded, {@link #finish} returns at once
 * unless something failed, and the remaining entry points are only reached on a path where the
 * default mode has already decided to throw. A run in which nothing fails therefore produces
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
  // Names the block of a node that no group reaches, only when skipping blocks for a deficit.
  private static final String INSTANCE_GROUP_PREFIX = "instance:";
  private static final Comparator<AssignableReplica> REPLICA_ORDER =
      Comparator.comparing(AssignableReplica::getResourceName)
          .thenComparing(AssignableReplica::getPartitionName)
          .thenComparing(AssignableReplica::getReplicaState);

  private final boolean _enabled;
  private final ClusterModel _clusterModel;
  private final List<AssignableNode> _nodes;

  private final Set<String> _failedGroups = new HashSet<>();
  private final Set<String> _skippedResources = new HashSet<>();
  // What this run has placed so far per group, so a group can be rolled back exactly.
  private final Map<String, List<Placement>> _placementsByGroup = new HashMap<>();
  // Failures are recorded here while isolating so a tolerated group failure never marks the
  // returned OptimalAssignment as failed, which would make getOptimalResourceAssignment throw.
  // Replaced on every tolerated failure so each group's diagnosis only reports its own reasons.
  private OptimalAssignment _failureSink;
  private HelixRebalanceException _firstFailure;
  private boolean _placementFailureTolerated;
  // Computed on the first failure only, so the happy path stays identical to the default mode.
  private List<Set<String>> _shareBlocks;
  private Map<String, Set<String>> _blockByGroup;
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
   * @return true when the caller should move on to the next replica.
   */
  boolean shouldSkip(AssignableReplica replica) {
    if (!_enabled || _failedGroups.isEmpty() || !_failedGroups.contains(groupKey(replica))) {
      return false;
    }
    _skippedResources.add(replica.getResourceName());
    return true;
  }

  /**
   * Whether this run already tolerated a replica that could not be placed. Every later one is
   * tolerated too, since the share blocks do not change during a run, so its failure is never
   * thrown.
   */
  boolean hasToleratedPlacementFailure() {
    return _placementFailureTolerated;
  }

  /**
   * Remember a placement so it can be released if this replica's group later fails.
   */
  void recordPlacement(AssignableReplica replica, AssignableNode node) {
    if (!_enabled) {
      return;
    }
    _placementsByGroup.computeIfAbsent(groupKey(replica), key -> new ArrayList<>())
        .add(new Placement(replica, node));
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
    // Isolation needs a second share block to keep rebalancing. Blocks are counted over every
    // resource group, placed or not, since a partial rebalance may only have work for one clique.
    // A tag no resource is pinned to (a spare pool) makes no block, so it cannot hide a failure of
    // the only clique. A group that reaches no node is a block of its own, and finish fails the
    // run if every group that reaches a node failed.
    if (shareBlocks().size() < 2) {
      LOG.warn("Instance tag isolation cannot isolate group {} in the {} rebalance of cluster {}, "
              + "since its resource groups form {} share block(s) and isolation needs at least "
              + "two. Failing the rebalance.", group, _clusterModel.getRebalanceScopeType(),
          _clusterModel.getContext().getClusterName(), shareBlocks().size());
      return false;
    }
    int released = 0;
    // Releasing gives back the node capacity this run's placements for the whole block used and
    // removes their fault zone entries, leaving the other blocks' placements in place. Each release
    // only undoes its own placement, so the order of the releases does not matter.
    for (String member : closure) {
      List<Placement> placements = _placementsByGroup.remove(member);
      if (placements != null) {
        for (int i = placements.size() - 1; i >= 0; i--) {
          Placement placement = placements.get(i);
          AssignableReplica placed = placement._replica;
          // The release path is keyed by state, so pass the placed replica's own state. With any
          // other state the node keeps the replica and only logs a warning, while the fault zone
          // entry is still removed, and a state the model does not index throws.
          _clusterModel.release(placed.getResourceName(), placed.getPartitionName(),
              placed.getReplicaState(), placement._node.getInstanceName());
          _skippedResources.add(placed.getResourceName());
          released++;
        }
      }
      _failedGroups.add(member);
    }
    _skippedResources.add(replica.getResourceName());
    _placementFailureTolerated = true;
    if (_firstFailure == null) {
      _firstFailure = failure;
    }
    LOG.warn("Instance tag isolation rolled back and skipped share block {} after group {} failed "
            + "in the {} rebalance of cluster {}. Released {} replica(s) placed in this run.",
        closure, group, _clusterModel.getRebalanceScopeType(),
        _clusterModel.getContext().getClusterName(), released, failure);
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
    // Survival is judged over every resource group, not only the replicas being reassigned, since a
    // partial or incremental rebalance may only have work for the failing clique. Only groups that
    // hold resources and reach a node of this model count. A spare pool never fails, and a group
    // whose tag no node carries has nothing to place, so counting either as a survivor would hide a
    // cluster wide failure.
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
        _failedGroups.size(), tagByGroup().size(),
        _skippedResources.size(), failedBlocks, blocks.size(),
        _clusterModel.getRebalanceScopeType(), _clusterModel.getContext().getClusterName(),
        _failedGroups);
    optimalAssignment.setSkippedResources(_skippedResources);
  }

  /**
   * Skips the blocks that cause a cluster wide capacity deficit, so the rest can still rebalance.
   *
   * The check is a sum over every replica that ignores tags, so one clique far over capacity can
   * push it below zero while every other clique fits on its own nodes. This splits the check's
   * totals by block and skips the blocks that do not fit on their own nodes. Replicas parked on
   * nodes outside the model, such as a node still inside its delay window, are not placed in this
   * scope, so they count first on no key, then only on the short keys, then on every key. A block
   * only the parked replicas push over is skipped only while the rest still does not fit, largest
   * overflow first. The rest is always held to the full totals, as the default check would hold a
   * cluster made of the rest alone. Only called where the default mode throws.
   *
   * @param deficit the failure the default mode would throw.
   * @param divGuard the share of each key's capacity added to the scoring denominators, also their
   *                 floor.
   * @return the scoring capacity for the rest of the cluster, or null to throw as usual.
   */
  Map<String, Float> absorbCapacityDeficit(HelixRebalanceException deficit, float divGuard) {
    if (!_enabled) {
      return null;
    }
    // Same rule as tryIsolate: skipping a block only helps if another block with resources is
    // left. A group whose tag no node carries is a block of its own, so it can be skipped here.
    if (shareBlocks().size() < 2) {
      return null;
    }
    BlockTotals totals = new BlockTotals();
    Map<String, Long> left = _clusterModel.getContext().getEstimateUtilizationMap();
    // Every pass skips the blocks over without their parked replicas.
    List<Predicate<String>> countParked =
        Arrays.asList(key -> false, key -> left.getOrDefault(key, 0L) < 0, key -> true);
    Set<Integer> atFault = totals.blocksOverCapacity(countParked.get(0));
    Map<String, Float> residualScoringCap = totals.residualScoringCapacity(atFault, divGuard);
    for (Predicate<String> counted : countParked.subList(1, countParked.size())) {
      if (residualScoringCap != null) {
        break;
      }
      // A block only over once its parked replicas count still places what this scope gives it, so
      // these are skipped one at a time, largest overflow first, until the rest fits.
      List<Integer> candidates = new ArrayList<>(totals.blocksOverCapacity(counted));
      candidates.removeAll(atFault);
      candidates.sort(Comparator.comparingLong((Integer block) -> -totals.overflow(block, counted))
          .thenComparing(block -> block));
      Set<Integer> blamed = new TreeSet<>(atFault);
      for (int block : candidates) {
        blamed.add(block);
        residualScoringCap = totals.residualScoringCapacity(blamed, divGuard);
        if (residualScoringCap != null) {
          atFault = blamed;
          break;
        }
      }
    }
    if (residualScoringCap == null) {
      return null;
    }

    Set<String> deficitGroups = new TreeSet<>();
    for (int block : atFault) {
      deficitGroups.addAll(totals._blocks.get(block));
    }
    // Skip the groups at fault like any failed group. If every group fails, the run throws this
    // deficit. A block of nodes no resource can use has no group to skip.
    Set<String> carried = new TreeSet<>(deficitGroups);
    carried.retainAll(tagByGroup().keySet());
    _failedGroups.addAll(carried);
    _firstFailure = deficit;
    if (carried.isEmpty()) {
      LOG.warn("Instance tag isolation found the {} rebalance of cluster {} over capacity only on "
              + "nodes no resource can use: {}. No group is skipped.",
          _clusterModel.getRebalanceScopeType(), _clusterModel.getContext().getClusterName(),
          deficitGroups, deficit);
    } else {
      LOG.warn("Instance tag isolation skipped group(s) {} in the {} rebalance of cluster {}, "
              + "since their replicas do not fit on their own nodes.", carried,
          _clusterModel.getRebalanceScopeType(), _clusterModel.getContext().getClusterName(),
          deficit);
    }
    return residualScoringCap;
  }

  /**
   * The check's totals split by block. Each replica is charged to one block and each node's
   * capacity to one block, so the blocks add up to the check's totals. A node no group reaches
   * gets a block of its own.
   */
  private final class BlockTotals {
    private final List<Set<String>> _blocks = deficitBlocks();
    private final Set<Integer> _resourceBlocks = new HashSet<>();
    private final Map<Integer, Map<String, Long>> _capacity = new HashMap<>();
    private final Map<Integer, Map<String, Long>> _demand = new HashMap<>();
    // The part of each block's demand parked on nodes outside this scope's model.
    private final Map<Integer, Map<String, Long>> _parked = new HashMap<>();

    private BlockTotals() {
      Map<String, Integer> blockOfGroup = new HashMap<>();
      for (int i = 0; i < _blocks.size(); i++) {
        for (String group : _blocks.get(i)) {
          blockOfGroup.put(group, i);
        }
      }
      Integer untaggedBlock = null;
      for (Map.Entry<String, String> entry : tagByGroup().entrySet()) {
        int block = blockOfGroup.get(entry.getKey());
        _resourceBlocks.add(block);
        if (entry.getValue() == null) {
          // Every untagged group can use every node, so they all share one block.
          untaggedBlock = block;
        }
      }
      Map<String, Map<String, PartitionShare>> shares = chargeOwners(blockOfGroup);
      chargeNodes(blockOfGroup, untaggedBlock, shares);
      chargeParked(shares);
    }

    /** Charges every replica the check counts to the block that owns it. */
    private Map<String, Map<String, PartitionShare>> chargeOwners(
        Map<String, Integer> blockOfGroup) {
      Map<String, Map<String, PartitionShare>> shares = new HashMap<>();
      for (AssignableReplica replica : _clusterModel.getContext().getManagedReplicas()) {
        int owner = blockOfGroup.get(groupKey(replica));
        PartitionShare share =
            shares.computeIfAbsent(replica.getResourceName(), key -> new HashMap<>())
                .computeIfAbsent(replica.getPartitionName(),
                    key -> new PartitionShare(owner, replica.getCapacity()));
        share._owned++;
        charge(_demand, owner, replica.getCapacity(), 1);
      }
      for (Set<AssignableReplica> replicas : _clusterModel.getAssignableReplicaMap().values()) {
        for (AssignableReplica replica : replicas) {
          PartitionShare share = shareOf(shares, replica);
          if (share != null) {
            share._toAssign++;
          }
        }
      }
      return shares;
    }

    /** Credits each node's capacity to its block, then charges the replicas placed on it. */
    private void chargeNodes(Map<String, Integer> blockOfGroup, Integer untaggedBlock,
        Map<String, Map<String, PartitionShare>> shares) {
      // Sorted, so which replicas count toward a partition that has more placed than it owns never
      // depends on hash order.
      List<AssignableNode> nodes = new ArrayList<>(_nodes);
      nodes.sort(Comparator.comparing(AssignableNode::getInstanceName));
      int[] nodeBlock = new int[nodes.size()];
      List<Map<String, Long>> room = new ArrayList<>(nodes.size());
      List<List<AssignableReplica>> foreign = new ArrayList<>(nodes.size());
      for (int i = 0; i < nodes.size(); i++) {
        AssignableNode node = nodes.get(i);
        int blockId = blockOfNode(node, blockOfGroup, untaggedBlock);
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
          if (share == null) {
            continue;
          }
          if (share._owner != blockId) {
            away.add(replica);
          } else if (share.place()) {
            replica.getCapacity()
                .forEach((key, value) -> nodeRoom.merge(key, (long) -value, Long::sum));
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
    }

    /** The node's block. A node no group reaches gets a new block of its own. */
    private int blockOfNode(AssignableNode node, Map<String, Integer> blockOfGroup,
        Integer untaggedBlock) {
      // All groups that reach a node are in one block, so any of them names the node's block.
      // An untagged group reaches every node.
      if (untaggedBlock != null) {
        return untaggedBlock;
      }
      for (String tag : node.getInstanceTags()) {
        Integer block = blockOfGroup.get(TAG_GROUP_PREFIX + tag);
        if (block != null) {
          return block;
        }
      }
      _blocks.add(Collections.singleton(INSTANCE_GROUP_PREFIX + node.getInstanceName()));
      return _blocks.size() - 1;
    }

    /** Charges each block for its replicas parked on nodes outside this scope's model. */
    private void chargeParked(Map<String, Map<String, PartitionShare>> shares) {
      Set<String> modelled = _clusterModel.getAssignableLogicalIds();
      Map<String, ResourceAssignment> current =
          _clusterModel.getContext().getBestPossibleAssignment();
      shares.forEach((resource, byPartition) -> {
        ResourceAssignment assignment = current.get(resource);
        if (assignment == null) {
          return;
        }
        byPartition.forEach((partition, share) -> {
          int unaccounted = share._owned - share._placed - share._toAssign;
          if (unaccounted <= 0) {
            return;
          }
          // A replica to assign takes the place of one that sits on a node outside the model, so
          // only what is left over is parked.
          int outside = -share._toAssign;
          for (String logicalId : assignment.getReplicaMap(new Partition(partition)).keySet()) {
            if (!modelled.contains(logicalId)) {
              outside++;
            }
          }
          int parked = Math.min(unaccounted, outside);
          if (parked > 0) {
            charge(_parked, share._owner, share._weight, parked);
          }
        });
      });
    }

    /**
     * Moves each replica left on another block's node to that node's block, as far as the room
     * left on the node allows. The rest stay with their own block. A block with no resources takes
     * them all. A replica beyond what its partition owns moves nothing.
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
     * The blocks that need more than their own nodes have on any key, counting the parked
     * replicas only on the keys given.
     */
    private Set<Integer> blocksOverCapacity(Predicate<String> countParked) {
      Set<Integer> atFault = new TreeSet<>();
      for (Integer block : _demand.keySet()) {
        if (overflow(block, countParked) > 0) {
          atFault.add(block);
        }
      }
      return atFault;
    }

    /** The most the block needs beyond its own nodes on any key, counting parked as given. */
    private long overflow(int block, Predicate<String> countParked) {
      Map<String, Long> capacity = _capacity.getOrDefault(block, Collections.emptyMap());
      Map<String, Long> parked = _parked.getOrDefault(block, Collections.emptyMap());
      long overflow = Long.MIN_VALUE;
      for (Map.Entry<String, Long> demand : _demand.get(block).entrySet()) {
        String key = demand.getKey();
        long need =
            demand.getValue() - (countParked.test(key) ? 0L : parked.getOrDefault(key, 0L));
        overflow = Math.max(overflow, need - capacity.getOrDefault(key, 0L));
      }
      return overflow;
    }

    /**
     * The scoring capacity left once the given blocks are skipped with everything they own, or
     * null when the default mode's result stands.
     */
    private Map<String, Float> residualScoringCapacity(Set<Integer> atFault, float divGuard) {
      // No block is over (first pass only), or every block with resources is. A block of nodes no
      // resource can use may be skipped, but it never counts as a block left to rebalance.
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
          // The rest does not fit. Once the last pass skips all it finds, every block left fits on
          // its own nodes.
          return null;
        }
        // Floored so the denominator stays above zero when the skipped blocks hold all of a key.
        residualScoringCap.put(entry.getKey(),
            Math.max((float) remaining + (entry.getValue() * divGuard), divGuard));
      }
      return residualScoringCap;
    }

    private PartitionShare shareOf(Map<String, Map<String, PartitionShare>> shares,
        AssignableReplica replica) {
      Map<String, PartitionShare> byPartition = shares.get(replica.getResourceName());
      return byPartition == null ? null : byPartition.get(replica.getPartitionName());
    }

    private void charge(Map<Integer, Map<String, Long>> totals, int block,
        Map<String, Integer> weight, long times) {
      Map<String, Long> entry = totals.computeIfAbsent(block, key -> new HashMap<>());
      weight.forEach((key, value) -> entry.merge(key, times * value, Long::sum));
    }
  }

  /**
   * How many replicas of one partition the check counts, and how many of them this scope has
   * placed on its nodes or has still to assign. A replica's weight depends only on its partition,
   * never on its state.
   */
  private static final class PartitionShare {
    private final int _owner;
    private final Map<String, Integer> _weight;
    private int _owned;
    private int _placed;
    private int _toAssign;

    private PartitionShare(int owner, Map<String, Integer> weight) {
      _owner = owner;
      _weight = weight;
    }

    /** Count one placed replica, or report that the partition has no replica left to place. */
    private boolean place() {
      if (_placed >= _owned) {
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
   * the same set of nodes and is carried over together. A resource with no tag has no declared
   * domain and can be placed anywhere, so it is keyed on its own, though it then shares nodes with
   * everything.
   */
  private static String groupKey(AssignableReplica replica) {
    return groupKey(replica.getResourceName(), replica.getResourceInstanceGroupTag());
  }

  private static String groupKey(String resource, String tag) {
    return Strings.isNullOrEmpty(tag) ? UNTAGGED_GROUP_PREFIX + resource : TAG_GROUP_PREFIX + tag;
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
    List<Set<String>> blocks = blocksOf(tagByGroup());
    // The blocks partition the groups, so every group maps to exactly one block.
    Map<String, Set<String>> blockByGroup = new HashMap<>();
    for (Set<String> block : blocks) {
      for (String group : block) {
        blockByGroup.put(group, block);
      }
    }
    _blockByGroup = blockByGroup;
    _shareBlocks = blocks;
    return _shareBlocks;
  }

  /**
   * Like the share blocks, but also gives a block to each tag found only on nodes that carry no
   * resource's tag, such as a spare pool, so its capacity and anything left on it stay with it.
   * Tags on nodes that also carry a resource's tag (zone, hardware or pool labels) are left out,
   * since they would join every clique into one block. Used only to split a capacity deficit.
   */
  private List<Set<String>> deficitBlocks() {
    Map<String, String> tagByGroup = new HashMap<>(tagByGroup());
    Set<String> resourceTags = new HashSet<>(tagByGroup.values());
    resourceTags.remove(null);
    // Operational labels on reachable nodes must not connect otherwise independent cliques.
    Set<String> spanningTags = new HashSet<>();
    for (AssignableNode node : _nodes) {
      if (!Collections.disjoint(node.getInstanceTags(), resourceTags)) {
        spanningTags.addAll(node.getInstanceTags());
      }
    }
    for (AssignableNode node : _nodes) {
      for (String tag : node.getInstanceTags()) {
        if (!spanningTags.contains(tag)) {
          tagByGroup.putIfAbsent(TAG_GROUP_PREFIX + tag, tag);
        }
      }
    }
    return blocksOf(tagByGroup);
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
    // Builds the group to block lookup on first use.
    shareBlocks();
    Set<String> block = _blockByGroup.get(group);
    return block != null ? block : Collections.singleton(group);
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

  /** Maps each group to its tag, or to null for an untagged group, which can use every node. */
  private Map<String, String> tagByGroup() {
    if (_tagByGroup != null) {
      return _tagByGroup;
    }
    Map<String, String> tagByGroup = new HashMap<>();
    _clusterModel.getContext().getResourceInstanceGroupTags().forEach((resource, tag) -> {
      tagByGroup.put(groupKey(resource, tag), Strings.emptyToNull(tag));
    });
    _tagByGroup = tagByGroup;
    return _tagByGroup;
  }

  /**
   * A placement made during this run, kept so it can be released if the group later fails.
   *
   * Holds only the replica and the node it went to, two references or 24 bytes with compressed
   * oops, and reads the fields the release needs only during a rollback. Those fields are final, so
   * they still hold the placed values. One record is kept per placed replica while isolation is
   * enabled, until calculate returns, and none while it is disabled. A record cannot be dropped
   * earlier, since a later failure anywhere in its share block still rolls its group back.
   */
  private static final class Placement {
    private final AssignableReplica _replica;
    private final AssignableNode _node;

    private Placement(AssignableReplica replica, AssignableNode node) {
      _replica = replica;
      _node = node;
    }
  }
}
