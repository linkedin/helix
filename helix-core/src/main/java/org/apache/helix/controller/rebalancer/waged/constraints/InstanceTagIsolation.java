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
import java.util.List;
import java.util.Map;
import java.util.Set;

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
 * <h3>Why isolation only engages for exclusive groups</h3>
 * The same overcommit argument applies across groups whenever two groups can land on the same node.
 * A group is therefore only isolated when no other group can use its nodes. An untagged resource
 * can go anywhere, so it never qualifies once anything else exists, and neither does any group
 * whose nodes it could also use. When the domains overlap the rebalance fails exactly as it does
 * today, so this mode is never worse than the default one.
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
  private Set<String> _exclusiveGroups;

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
    if (!exclusiveGroups().contains(group)) {
      LOG.warn(
          "Instance tag isolation cannot isolate group {} in cluster {}: its nodes are also usable "
              + "by another group, so carrying it over could overcommit them. Failing the whole "
              + "rebalance exactly like the default global mode.", group,
          _clusterModel.getContext().getClusterName());
      return false;
    }
    // Releasing in reverse order restores the node capacities and the fault zone map to exactly the
    // state they had before this group's first replica was placed.
    List<Placement> placements = _placementsByGroup.remove(group);
    if (placements != null) {
      for (int i = placements.size() - 1; i >= 0; i--) {
        Placement placement = placements.get(i);
        _clusterModel.release(placement._resourceName, placement._partitionName, placement._state,
            placement._instanceName);
        _skippedResources.add(placement._resourceName);
      }
    }
    _failedGroups.add(group);
    _skippedResources.add(replica.getResourceName());
    if (_firstFailure == null) {
      _firstFailure = failure;
    }
    LOG.warn(
        "Instance tag isolation: rolling back and skipping group {} during the {} rebalance of "
            + "cluster {}. {} replica(s) already placed for this group were released. Every other "
            + "group keeps its newly calculated assignment.", group,
        _clusterModel.getRebalanceScopeType(), _clusterModel.getContext().getClusterName(),
        placements == null ? 0 : placements.size(), failure);
    return true;
  }

  /**
   * Publish the isolation outcome onto the assignment the algorithm is about to return.
   *
   * @throws HelixRebalanceException when every group failed, so that the caller's existing failure
   *         handling, metrics and last known good fallback all still apply.
   */
  void finish(OptimalAssignment optimalAssignment) throws HelixRebalanceException {
    if (!_enabled || _failedGroups.isEmpty()) {
      return;
    }
    if (_failedGroups.size() == _allGroups.size()) {
      // Nothing could be placed anywhere, so behave exactly like the default global mode.
      throw _firstFailure;
    }
    LOG.warn(
        "Instance tag isolation skipped {} of {} group(s) ({} resource(s)) during the {} rebalance "
            + "of cluster {}. Skipped groups: {}.", _failedGroups.size(), _allGroups.size(),
        _skippedResources.size(), _clusterModel.getRebalanceScopeType(),
        _clusterModel.getContext().getClusterName(), _failedGroups);
    optimalAssignment.setSkippedResources(_skippedResources);
  }

  /**
   * Attribute a cluster wide capacity deficit to the cliques that caused it.
   *
   * The cluster wide check that precedes any placement is a tag blind sum, so one wildly
   * oversubscribed clique can drag it negative while every other clique still fits comfortably on
   * its own nodes. Throwing there would freeze the whole cluster, which is exactly what this mode
   * exists to prevent. This walks the exclusive groups, sets aside the ones whose own replicas
   * cannot fit on their own nodes, and re-evaluates the deficit on the remainder. The groups set
   * aside are then carried over like any other failed group.
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
    Set<String> exclusiveGroups = exclusiveGroups();
    if (exclusiveGroups.isEmpty()) {
      return null;
    }

    Map<String, Map<String, Long>> demandByGroup = new HashMap<>();
    Map<String, String> tagByGroup = new HashMap<>();
    // Deliberately conservative: in the partial, emergency and delayed overwrite scopes the replica
    // list only holds what still has to be assigned, while the residual demand below is derived from
    // the full cluster utilization. A group is therefore credited with all of its node capacity but
    // only its outstanding demand, which can only make the remainder look healthier, never a group
    // look more innocent than it is. The worst case is that isolation declines to attribute and the
    // rebalance fails exactly like the default mode. Do not "fix" this into an unsafe direction.
    for (AssignableReplica replica : _allReplicas) {
      String group = groupKey(replica);
      String tag = replica.getResourceInstanceGroupTag();
      // Attribution is by "the group's own nodes", which only means something for a tagged group.
      // An untagged one can use every node, so it is never exclusive once anything else exists.
      if (tag == null || tag.isEmpty() || !exclusiveGroups.contains(group)) {
        continue;
      }
      tagByGroup.put(group, tag);
      Map<String, Long> demand = demandByGroup.computeIfAbsent(group, key -> new HashMap<>());
      replica.getCapacity().forEach((key, value) -> demand.merge(key, (long) value, Long::sum));
    }

    // An exclusive group owns its nodes outright, so no node is counted for two groups.
    Map<String, Map<String, Long>> capacityByGroup = new HashMap<>();
    for (AssignableNode node : _nodes) {
      for (Map.Entry<String, String> entry : tagByGroup.entrySet()) {
        if (node.getInstanceTags().contains(entry.getValue())) {
          Map<String, Long> capacity =
              capacityByGroup.computeIfAbsent(entry.getKey(), key -> new HashMap<>());
          node.getMaxCapacity()
              .forEach((key, value) -> capacity.merge(key, (long) value, Long::sum));
        }
      }
    }

    Map<String, Long> residualCapacity =
        new HashMap<>(_clusterModel.getContext().getClusterCapacityMap());
    Map<String, Long> residualDemand = new HashMap<>();
    _clusterModel.getContext().getEstimateUtilizationMap().forEach((key, remaining) -> residualDemand
        .put(key, residualCapacity.getOrDefault(key, 0L) - remaining));

    Set<String> deficitGroups = new HashSet<>();
    for (Map.Entry<String, Map<String, Long>> entry : demandByGroup.entrySet()) {
      Map<String, Long> capacity =
          capacityByGroup.getOrDefault(entry.getKey(), Collections.emptyMap());
      boolean overCommitted = entry.getValue().entrySet().stream()
          .anyMatch(demand -> demand.getValue() > capacity.getOrDefault(demand.getKey(), 0L));
      if (!overCommitted) {
        continue;
      }
      deficitGroups.add(entry.getKey());
      entry.getValue().forEach((key, value) -> residualDemand.merge(key, -value, Long::sum));
      capacity.forEach((key, value) -> residualCapacity.merge(key, -value, Long::sum));
    }
    if (deficitGroups.isEmpty()) {
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
      residualScoringCap.put(entry.getKey(), (float) remaining + (entry.getValue() * divGuard));
    }

    // Carry the groups at fault over like any other failed group, and seed the failure that a
    // fully failed run rethrows.
    _failedGroups.addAll(deficitGroups);
    _firstFailure = deficit;
    LOG.warn(
        "Instance tag isolation attributed a cluster wide capacity deficit in cluster {} to "
            + "group(s) {}, which cannot hold their own replicas on their own nodes. They are "
            + "carried over and the rest of the cluster is rebalanced normally.",
        _clusterModel.getContext().getClusterName(), deficitGroups, deficit);
    return residualScoringCap;
  }

  /**
   * The isolation unit of a replica.
   *
   * A resource pinned to an instance group tag can only ever be placed on that tag's nodes, so the
   * tag is the failure domain the operator declared: every resource sharing the tag competes for
   * the same nodes and is carried over together. A resource with no tag has no declared domain and
   * can be placed anywhere, so it is keyed on its own, though it can never be exclusive.
   */
  private static String groupKey(AssignableReplica replica) {
    String tag = replica.getResourceInstanceGroupTag();
    return (tag == null || tag.isEmpty()) ? UNTAGGED_GROUP_PREFIX + replica.getResourceName()
        : TAG_GROUP_PREFIX + tag;
  }

  /**
   * The groups whose nodes no other group can use, computed once per run and cached.
   *
   * Only such a group can be safely carried over while the rest of the cluster is recalculated,
   * because the capacity freed by rolling it back cannot be claimed by anything else. In the clique
   * topology this mode targets every instance carries exactly one clique tag, so every clique
   * qualifies.
   */
  private Set<String> exclusiveGroups() {
    if (_exclusiveGroups != null) {
      return _exclusiveGroups;
    }
    // A group's tag, or null for an untagged group, which can use every node.
    Map<String, String> tagByGroup = new HashMap<>();
    for (AssignableReplica replica : _allReplicas) {
      String tag = replica.getResourceInstanceGroupTag();
      tagByGroup.put(groupKey(replica), (tag == null || tag.isEmpty()) ? null : tag);
    }

    Set<String> shared = new HashSet<>();
    for (AssignableNode node : _nodes) {
      Set<String> groupsOnNode = new HashSet<>();
      for (Map.Entry<String, String> entry : tagByGroup.entrySet()) {
        if (entry.getValue() == null || node.getInstanceTags().contains(entry.getValue())) {
          groupsOnNode.add(entry.getKey());
        }
      }
      if (groupsOnNode.size() > 1) {
        shared.addAll(groupsOnNode);
      }
    }
    Set<String> exclusive = new HashSet<>(tagByGroup.keySet());
    exclusive.removeAll(shared);
    _exclusiveGroups = exclusive;
    return _exclusiveGroups;
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
