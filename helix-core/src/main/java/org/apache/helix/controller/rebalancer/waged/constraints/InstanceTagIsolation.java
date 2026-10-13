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
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;

import org.apache.helix.controller.rebalancer.waged.model.AssignableNode;
import org.apache.helix.controller.rebalancer.waged.model.AssignableReplica;
import org.apache.helix.controller.rebalancer.waged.model.ClusterModel;
import org.apache.helix.controller.rebalancer.waged.model.OptimalAssignment;


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
 * While disabled, {@link #failureSink} hands back the caller's own sink, so nothing changes. While
 * enabled, it only changes where a failure is recorded, so a run in which nothing fails produces
 * exactly the same assignment either way, for any topology.
 *
 * Instances are stateful and scoped to a single
 * {@link ConstraintBasedAlgorithm#calculate(ClusterModel)} run. They are not thread safe, which
 * matches the single threaded assignment loop that owns them.
 */
class InstanceTagIsolation {
  // Prefixes for the isolation group keys, so a tag and a resource with the same name can never
  // collide into one group.
  private static final String TAG_GROUP_PREFIX = "tag:";
  private static final String UNTAGGED_GROUP_PREFIX = "untagged-resource:";

  private final boolean _enabled;
  private final ClusterModel _clusterModel;
  private final List<AssignableNode> _nodes;

  // Failures are funneled here while isolating so a tolerated group failure never marks the
  // returned OptimalAssignment as failed, which would make getOptimalResourceAssignment throw.
  private final OptimalAssignment _failureSink;
  // Computed on the first failure only, so the happy path stays identical to the default mode.
  private List<Set<String>> _shareBlocks;
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
}
