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
 * Discourage placing a replica on an instance that is already physically occupied by replicas the
 * rebalancer did not allocate -- for example a replica wedged in a state whose drop transition
 * never completes. Such an instance looks empty to the rest of the model, so it keeps winning
 * placements that the capacity check downstream then prunes, and the partition never converges.
 * <p>
 * This deliberately does not extend {@link UsageSoftConstraint}. That base class normalizes with
 * SIGMOID(-(score - 1) * 44), which only discriminates within roughly +/-5% of the cluster average
 * utilization; a physically full instance and a half empty one both collapse to approximately zero
 * there, so the signal disappears exactly where it is needed. The inherited linear scaler is used
 * instead.
 * <p>
 * Expressing this as a soft constraint rather than a hard one or a candidate filter is what keeps
 * it safe. A hard constraint that rejects every instance aborts the whole rebalance with
 * NO_CANDIDATE_NODE, and the cluster then reverts to its last known good assignment. A soft
 * constraint can only rank, never reject, so when no instance has room the greedy selection still
 * picks one and the pass completes -- and because the score degrades continuously, the one it
 * picks is the least overcommitted rather than an arbitrary choice.
 */
class PhysicalCapacitySoftConstraint extends SoftConstraint {
  private static final float MAX_SCORE = 1f;
  private static final float MIN_SCORE = 0f;

  PhysicalCapacitySoftConstraint() {
    super(MAX_SCORE, MIN_SCORE);
  }

  @Override
  protected double getAssignmentScore(AssignableNode node, AssignableReplica replica,
      ClusterContext clusterContext) {
    return node.getPhysicalRoomScore(replica);
  }
}
