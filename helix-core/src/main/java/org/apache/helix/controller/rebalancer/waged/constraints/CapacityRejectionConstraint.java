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

import org.apache.helix.controller.rebalancer.waged.model.AssignableNode;
import org.apache.helix.controller.rebalancer.waged.model.AssignableReplica;
import org.apache.helix.controller.rebalancer.waged.model.ClusterContext;

/**
 * Rejects a placement that the WAGED capacity check already rejected earlier in the same rebalance
 * pass.
 * <p>
 * The rebalancer computes an assignment and then validates it against the real capacity ledger.
 * Those two steps do not use the same occupancy view: the planner only counts replicas that are in
 * a counted state <em>and</em> are where the target assignment expects them, while the capacity
 * ledger counts everything physically present on the instance. An instance holding replicas that
 * the planner cannot see therefore looks empty to the planner and full to the capacity check, so
 * the planner keeps proposing it and the capacity check keeps rejecting it.
 * <p>
 * This constraint closes that loop by feeding the rejection back, letting the planner pick a
 * different instance instead of re-deriving the same rejected placement. It relies only on
 * rejections recorded during the current pass; see
 * {@code ResourceControllerDataProvider#clearCapacityRejections()}.
 */
class CapacityRejectionConstraint extends HardConstraint {

  @Override
  boolean isAssignmentValid(AssignableNode node, AssignableReplica replica,
      ClusterContext clusterContext) {
    return !clusterContext.isCapacityRejected(node.getInstanceName(), replica.getResourceName(),
        replica.getPartitionName());
  }

  @Override
  String getDescription() {
    return "Placement was rejected by the capacity check earlier in this rebalance pass";
  }

  @Override
  Type getType() {
    return Type.CAPACITY_REJECTED;
  }
}
