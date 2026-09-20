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
