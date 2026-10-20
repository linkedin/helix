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

import com.google.common.base.Strings;
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
 * share block (its isolation group and every group connected to it through shared nodes) are
 * released, the rest of the block is skipped, and the pass carries on. The caller then carries the
 * skipped resources' previous assignment forward, so no resource is emitted half assigned, and
 * leaves out a skipped resource that has none.
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
