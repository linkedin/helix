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
  private final boolean _enabled;
  // Failures are funneled here while isolating so a tolerated group failure never marks the
  // returned OptimalAssignment as failed, which would make getOptimalResourceAssignment throw.
  private final OptimalAssignment _failureSink;

  InstanceTagIsolation(ClusterModel clusterModel) {
    _enabled = clusterModel.getContext().isInstanceTagIsolationEnabled();
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
}
