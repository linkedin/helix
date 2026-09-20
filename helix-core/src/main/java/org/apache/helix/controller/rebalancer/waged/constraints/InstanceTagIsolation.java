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
          "Instance tag isolation cannot isolate group {} in cluster {}: it shares nodes with "
              + "every other group, so there is nothing left to recalculate around it. Failing "
              + "the whole rebalance exactly like the default global mode.", group,
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
    // "Nothing could be placed anywhere" has to be judged over the whole cluster, not over the
    // groups that happen to carry work in this run. Only a full rebalance sees every group; the
    // partial, emergency and delayed overwrite scopes carry just the replicas that still need
    // moving, which on a broken clique is frequently that clique alone. Counting groups there reads
    // as "every group failed" and throws, the caller discards the entire pipeline result and falls
    // back to the last known good assignment, and every healthy clique is frozen again. That is the
    // exact freeze this mode exists to prevent, and it is why the count is taken over the blocks
    // the cluster's nodes form instead.
    List<Set<String>> blocks = attributionBlocks();
    long failedBlocks =
        blocks.stream().filter(block -> block.stream().anyMatch(_failedGroups::contains)).count();
    if (blocks.isEmpty() || failedBlocks >= blocks.size()) {
      // Every independent part of the cluster failed, so behave exactly like the default global
      // mode. A block holding a group that never had work still counts as failed when any of its
      // groups failed, so padding the universe cannot hide a genuine cluster wide failure.
      throw _firstFailure;
    }
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
   * The isolation unit of a replica.
   *
   * A resource pinned to an instance group tag can only ever be placed on that tag's nodes, so the
   * tag is the failure domain the operator declared: every resource sharing the tag competes for
   * the same nodes and is carried over together. A resource with no tag has no declared domain and
   * can be placed anywhere, so it is keyed on its own, though it then shares nodes with everything.
   */
  private static String groupKey(AssignableReplica replica) {
    String tag = replica.getResourceInstanceGroupTag();
    return (tag == null || tag.isEmpty()) ? UNTAGGED_GROUP_PREFIX + replica.getResourceName()
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
   * The replica list only holds what still needs assigning, so a clique whose replicas are all
   * already placed contributes no group at all. It would then have no block, no demand and no way
   * to be blamed, and a deficit it alone caused would be declared unattributable. Adding a group
   * for every tag any node carries gives such a clique a block to be blamed in.
   *
   * Kept separate from the partition above on purpose. That one is sized by the replica derived
   * group count, which is what the "shares nodes with every other group" and "every group failed"
   * guards compare against, and padding it with groups that own no replicas and can never fail
   * would stop those guards ever firing.
   */
  private List<Set<String>> attributionBlocks() {
    if (_attributionBlocks != null) {
      return _attributionBlocks;
    }
    Map<String, String> tagByGroup = new HashMap<>(tagByGroup());
    for (AssignableNode node : _nodes) {
      for (String tag : node.getInstanceTags()) {
        tagByGroup.putIfAbsent(TAG_GROUP_PREFIX + tag, tag);
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
      for (AssignableReplica replica : _allReplicas) {
        String tag = replica.getResourceInstanceGroupTag();
        tagByGroup.put(groupKey(replica), (tag == null || tag.isEmpty()) ? null : tag);
      }
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
