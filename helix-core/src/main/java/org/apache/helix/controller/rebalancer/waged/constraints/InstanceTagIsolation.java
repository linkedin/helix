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
 * changes what happens when a placement fails: the replicas already placed for that replica's
 * isolation group are released, the rest of the group is skipped, and the pass carries on. The
 * caller then carries the skipped resources' previous assignment forward, so the emitted result is
 * still complete.
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
 * Rolling a whole block back therefore frees capacity that nothing still being calculated can take,
 * which is what makes the emitted result safe.
 *
 * The blocks are what keeps this useful rather than all or nothing. A cluster of disjoint cliques
 * is one block per clique, so a single broken clique is carried over while every other clique is
 * rebalanced normally. Overlap only widens the block that actually overlaps. An untagged resource
 * can go anywhere, so it pulls every group it meets into one cluster wide block, and the rebalance
 * then fails exactly as it does today. This mode is never worse than the default one.
 *
 * <h3>Parity</h3>
 * Every method is a no-op while disabled, and while enabled every method other than
 * {@link #failureSink} is only reached on a path where the default mode has already decided to
 * throw. A run in which nothing fails therefore produces exactly the same assignment either way,
 * for any topology.
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

  private final boolean _enabled;
  private final ClusterModel _clusterModel;
  private final List<AssignableReplica> _allReplicas;
  private final List<AssignableNode> _nodes;

  // Every isolation group observed during the run, used to tell "some groups failed" from
  // "everything failed", which has to keep behaving like the default mode.
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

  InstanceTagIsolation(ClusterModel clusterModel, List<AssignableReplica> allReplicas,
      List<AssignableNode> nodes) {
    _enabled = clusterModel.getContext().isInstanceTagIsolationEnabled();
    _clusterModel = clusterModel;
    _allReplicas = allReplicas;
    _nodes = nodes;
    _failureSink = _enabled ? new OptimalAssignment() : null;
  }

  /**
   * Where hard constraint failures for the current replica should be recorded.
   *
   * While disabled this is the caller's own {@link OptimalAssignment}, which is exactly what the
   * algorithm did before this class existed. While enabled it is a throwaway sink, so a tolerated
   * group failure never leaves the returned assignment marked as failed.
   */
  OptimalAssignment failureSink(OptimalAssignment defaultSink) {
    return _enabled ? _failureSink : defaultSink;
  }

  /**
   * Whether this replica should be skipped because an earlier replica of its group already failed.
   *
   * Also registers the replica's group, so the run can tell a partial failure from a total one.
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
   * Try to tolerate a replica that could not be placed by rolling its whole group back and skipping
   * the rest of it.
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
    // one to build the failure message above.
    _failureSink = new OptimalAssignment();
    // Carry over the failing group together with every group it shares a node with, so the
    // capacity the whole set occupies is off limits to everything still being calculated.
    Set<String> closure = blockOf(group);
    // Isolation is pointless only when the cluster holds no second block to recalculate around,
    // which is what an untagged resource or a tag bridging instance produces by merging everything
    // into one. Measured over every block the cluster's nodes form rather than over the groups that
    // happen to have outstanding replicas this run: a partial rebalance can carry work for a single
    // clique, and comparing against that would read as "shares nodes with every other group" on a
    // cluster of twenty independent cliques and fail them all.
    if (attributionBlocks().size() < 2) {
      LOG.warn(
          "Instance tag isolation cannot isolate group {} during the {} rebalance of cluster {}: "
              + "it shares nodes with every other group, so there is nothing left to recalculate "
              + "around it. Failing "
              + "the whole rebalance exactly like the default global mode.", group,
          _clusterModel.getRebalanceScopeType(),
          _clusterModel.getContext().getClusterName());
      return false;
    }
    int released = 0;
    // Releasing in reverse order restores the node capacities and the fault zone map to exactly the
    // state they had before this group's first replica was placed.
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
            + "assignment.", group, closure.size() - 1, _clusterModel.getRebalanceScopeType(),
        _clusterModel.getContext().getClusterName(), released, closure, failure);
    return true;
  }

  /**
   * Publish the isolation outcome onto the assignment the algorithm is about to return.
   *
   * @throws HelixRebalanceException when every independent block of the cluster failed, so that the
   *         caller's existing failure handling, metrics and last known good fallback all still
   *         apply.
   */
  void finish(OptimalAssignment optimalAssignment) throws HelixRebalanceException {
    if (!_enabled || _failedGroups.isEmpty()) {
      return;
    }
    // Outstanding work can belong only to the failing clique. Judge containment using the full
    // resource inventory and node-reachability blocks, not only replicas being reassigned.
    List<Set<String>> blocks = attributionBlocks();
    long failedBlocks =
        blocks.stream().filter(block -> block.stream().anyMatch(_failedGroups::contains)).count();
    // A baseline can also be incremental. Include allocated groups when deciding whether every
    // resource failed; an unrelated resource edit must not turn one broken clique into a global
    // failure. Node-only blocks cannot establish that any resource survived.
    boolean everyWorkingGroupFailed = !_allGroups.isEmpty()
        && _clusterModel.getRebalanceScopeType() == ClusterModel.RebalanceScopeType.GLOBAL_BASELINE
        && _failedGroups.containsAll(tagByGroup().keySet());
    if (blocks.isEmpty() || failedBlocks >= blocks.size() || everyWorkingGroupFailed) {
      // Every independent part of the cluster failed, so behave exactly like the default global
      // mode. A block holding a group that never had work still counts as failed when any of its
      // groups failed, so padding the universe cannot hide a genuine cluster wide failure.
      throw _firstFailure;
    }
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
   * exists to prevent. This walks the share blocks, sets aside the ones whose own replicas cannot
   * fit on their own nodes, and re-evaluates the deficit on the remainder. The blocks set aside are
   * then carried over like any other failed group.
   *
   * Only reachable on a path where the default mode has already decided to throw, so parity is
   * unaffected by construction.
   *
   * @param deficit the failure the default mode would have thrown.
   * @param divGuard the epsilon the algorithm adds to keep the scoring denominators above zero.
   * @return the scoring capacity map to use for the remainder of the cluster, or null when the
   *         deficit cannot be attributed and the caller should throw as usual.
   */
  Map<String, Float> absorbCapacityDeficit(HelixRebalanceException deficit, float divGuard) {
    if (!_enabled) {
      return null;
    }
    List<Set<String>> blocks = attributionBlocks();
    // A single block spanning the cluster means nothing can be set aside on its own. That is the
    // usual effect of an untagged resource, since it can be placed on any node and so pulls every
    // group it meets into one block. It is not guaranteed though: a tag that no live instance
    // carries reaches no node, so it stays a block of its own alongside the big one, and the
    // attribution below then correctly blames that unplaceable group.
    if (blocks.size() < 2) {
      return null;
    }
    Map<String, Integer> blockOfGroup = new HashMap<>();
    for (int i = 0; i < blocks.size(); i++) {
      for (String group : blocks.get(i)) {
        blockOfGroup.put(group, i);
      }
    }

    Map<Integer, Map<String, Long>> demandByBlock = new HashMap<>();
    // Only what still has to be assigned. In the partial, emergency and delayed overwrite scopes
    // that is a subset of the cluster, so the already placed replicas are added back per block from
    // the nodes below. The residual demand further down is a full cluster figure, and comparing a
    // partial demand against a full residual is what made attribution silently impossible outside
    // the global baseline scope: a block that was already placed looks like it demands nothing, so
    // nothing is ever over committed and nothing is ever blamed.
    for (AssignableReplica replica : _allReplicas) {
      Integer block = blockOfGroup.get(groupKey(replica));
      if (block == null) {
        continue;
      }
      Map<String, Long> demand = demandByBlock.computeIfAbsent(block, key -> new HashMap<>());
      replica.getCapacity().forEach((key, value) -> demand.merge(key, (long) value, Long::sum));
    }

    // Every group reaching a node is in that node's block by construction, so any one of them
    // identifies the block and the node's capacity is credited exactly once. A node reachable from
    // two blocks would have merged them, so nothing is ever counted twice.
    Map<Integer, Map<String, Long>> capacityByBlock = new HashMap<>();
    Map<String, String> tagByGroup = tagByGroup();
    String untaggedGroup = tagByGroup.entrySet().stream().filter(e -> e.getValue() == null)
        .map(Map.Entry::getKey).findFirst().orElse(null);
    for (AssignableNode node : _nodes) {
      Integer block = null;
      if (untaggedGroup != null) {
        // An untagged group can use every node, so it reaches this one and names its block. Without
        // this the untagged block would be credited no capacity at all and would always look
        // over committed, which would wrongly blame it and give up on the whole rebalance.
        block = blockOfGroup.get(untaggedGroup);
      } else {
        for (String tag : node.getInstanceTags()) {
          block = blockOfGroup.get(TAG_GROUP_PREFIX + tag);
          if (block != null) {
            break;
          }
        }
      }
      if (block == null) {
        // No resource can be placed here, so this node's capacity belongs to no block.
        continue;
      }
      Map<String, Long> capacity = capacityByBlock.computeIfAbsent(block, k -> new HashMap<>());
      node.getMaxCapacity().forEach((key, value) -> capacity.merge(key, (long) value, Long::sum));

      // What is already sitting on the node belongs to the same block as the node, so credit it as
      // demand there. The replicas allocated before this run are loaded onto the nodes rather than
      // left in the replica list above, so without this a block carrying a full load of already
      // placed replicas would be measured as demanding nothing at all. Remaining capacity can go
      // negative on an over assigned node, which correctly reports more used than the node holds.
      Map<String, Integer> remaining = node.getRemainingCapacity();
      Map<String, Long> placed = demandByBlock.computeIfAbsent(block, k -> new HashMap<>());
      node.getMaxCapacity().forEach((key, max) -> placed.merge(key,
          (long) max - remaining.getOrDefault(key, max), Long::sum));
    }

    Map<String, Long> residualCapacity =
        new HashMap<>(_clusterModel.getContext().getClusterCapacityMap());
    Map<String, Long> residualDemand = new HashMap<>();
    _clusterModel.getContext().getEstimateUtilizationMap().forEach((key, remaining) -> residualDemand
        .put(key, residualCapacity.getOrDefault(key, 0L) - remaining));

    Set<Integer> deficitBlocks = new HashSet<>();
    Set<String> deficitGroups = new TreeSet<>();
    for (Map.Entry<Integer, Map<String, Long>> entry : demandByBlock.entrySet()) {
      Map<String, Long> capacity =
          capacityByBlock.getOrDefault(entry.getKey(), Collections.emptyMap());
      boolean overCommitted = entry.getValue().entrySet().stream()
          .anyMatch(demand -> demand.getValue() > capacity.getOrDefault(demand.getKey(), 0L));
      if (!overCommitted) {
        continue;
      }
      deficitBlocks.add(entry.getKey());
      deficitGroups.addAll(blocks.get(entry.getKey()));
      entry.getValue().forEach((key, value) -> residualDemand.merge(key, -value, Long::sum));
      capacity.forEach((key, value) -> residualCapacity.merge(key, -value, Long::sum));
    }
    // Nothing to blame, or everything is to blame, both of which mean the default mode's verdict
    // stands.
    if (deficitGroups.isEmpty() || deficitBlocks.size() == blocks.size()) {
      return null;
    }

    Map<String, Float> residualScoringCap = new HashMap<>();
    for (Map.Entry<String, Long> entry : residualCapacity.entrySet()) {
      long remaining = entry.getValue() - residualDemand.getOrDefault(entry.getKey(), 0L);
      if (remaining < 0) {
        // What is left over still does not fit, so this is a genuine cluster wide shortfall rather
        // than one bad clique. Report it exactly as the default mode does.
        return null;
      }
      // Floored so the denominator stays above zero. The default path divides by a full cluster
      // capacity, which is always positive, but a residual can reach zero when every node belongs
      // to a block that was set aside, and dividing by it would score Infinity or NaN and break the
      // ordering the algorithm sorts on.
      residualScoringCap.put(entry.getKey(),
          Math.max((float) remaining + (entry.getValue() * divGuard), divGuard));
    }

    // Carry the groups at fault over like any other failed group, and seed the failure that a
    // fully failed run rethrows.
    _failedGroups.addAll(deficitGroups);
    _firstFailure = deficit;
    LOG.warn(
        "Instance tag isolation attributed a cluster wide capacity deficit during the {} rebalance "
            + "of cluster {} to group(s) {}, which cannot hold their own replicas on their own nodes. They are "
            + "carried over and the rest of the cluster is rebalanced normally.",
        _clusterModel.getRebalanceScopeType(), _clusterModel.getContext().getClusterName(),
        deficitGroups, deficit);
    return residualScoringCap;
  }

  /**
   * The isolation unit of a replica.
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
   * and the blocks are a genuine partition of the groups. The weaker property the arithmetic
   * actually needs is that no node is ever counted for two blocks, which holds because a node
   * reachable from two blocks would have merged them. Note this is not a partition of the nodes: an
   * instance carrying only labels no resource is pinned to belongs to no block at all.
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
   * join otherwise independent resource groups.
   *
   * A tag carried by a node that some resource can already be placed on is deliberately left out.
   * Such a tag is an operational label spanning cliques (an availability zone, a hardware
   * generation, a pool name) rather than the name of a clique that lost its replicas. Adding it
   * would create a group no resource can ever be placed in, and because the label is shared it
   * would union every clique carrying it into one block. Both guards below then read "there is only
   * one block", and the whole mode silently degrades into the default global one on any fleet whose
   * instances carry an ordinary label alongside their clique tag.
   *
   * Kept separate from the resource partition so an unused node tag cannot be mistaken for a
   * resource that survived a failed baseline.
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
          // A label no resource is pinned to, such as an AZ or hardware tag, joins nothing.
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
   * All four fields are needed because the release path is keyed by state: releasing with the wrong
   * state is a silent no-op.
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
