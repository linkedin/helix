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

package org.apache.helix.guardrail.rules;

import java.util.Collections;

import org.apache.helix.guardrail.GuardrailContext;
import org.apache.helix.guardrail.GuardrailRule;
import org.apache.helix.guardrail.MinActiveReplicaChecker;
import org.apache.helix.guardrail.ValidationResult;
import org.apache.helix.guardrail.Violation;
import org.apache.helix.util.MinActiveReplicaCheckResult;

/**
 * Guard rail that blocks disabling an instance when doing so would push one or more of the partitions
 * it currently hosts below its {@code minActiveReplicas} on the remaining instances.
 * <p>
 * Disabling an instance (writing {@code HELIX_ENABLED=false}) is a controller-coordinated drain: the
 * instance keeps its ephemeral {@code LIVEINSTANCES} znode but is removed from the pool that hosts
 * partitions, so on the next rebalance the controller transitions every replica it holds down to
 * OFFLINE and tries to recreate them elsewhere. During that drain-and-recover window every partition
 * the instance was serving loses one active replica; if the remaining instances do not already hold
 * {@code minActiveReplicas} healthy replicas, the partition falls below its minimum &mdash; risking
 * reduced availability or, for the top state (MASTER/LEADER), a no-top-state window. Unlike
 * {@code EVACUATE} (guarded by {@link InstanceOperationRebalanceFeasibilityGuardrailRule}), a plain
 * disable runs no such check today: {@code ZKHelixAdmin.enableInstance(..., false)} simply writes the
 * disabled flag.
 * <p>
 * The rule evaluates the immediate post-disable state through the injected
 * {@link MinActiveReplicaChecker} &mdash; a read-only what-if over the current ExternalViews, reused
 * from {@code InstanceValidationUtil.siblingNodesActiveReplicaCheckWithDetails}. It applies to all
 * rebalance modes, since {@code minActiveReplicas} is a property of the resource, not of WAGED. The
 * verdict is overridable with {@code force=true} (draining a failing node is often mandatory) and can
 * be previewed with {@code dryRun=true}. Resources with no committed ExternalView are skipped by the
 * underlying check: with no placement they are not hosted on the instance and cannot be driven below
 * their minimum by disabling it.
 * <p>
 * This is a pure, read-only function of its {@link GuardrailContext}: it performs no ZooKeeper writes
 * and, being always-on, needs no cluster-config opt-in. If the injected checker throws, the
 * {@code GuardrailPipeline} treats it as a (force-able) violation, so the guard fails closed.
 */
public class InstanceDisableMinActiveReplicaGuardrailRule implements GuardrailRule {
  public static final String RULE_ID = "MIN_ACTIVE_REPLICA_ON_INSTANCE_DISABLE";

  @Override
  public String getId() {
    return RULE_ID;
  }

  @Override
  public ValidationResult validate(GuardrailContext context) {
    String instanceName = context.getInstanceName();
    if (instanceName == null) {
      // No target instance to evaluate; nothing for this rule to certify.
      return ValidationResult.feasible();
    }

    MinActiveReplicaChecker checker = context.getMinActiveReplicaChecker();
    if (checker == null) {
      // No checker seam was supplied, so this call is not wired for the min-active what-if. Certify
      // feasible rather than block every disable on a wiring gap; the endpoints that enforce this
      // rule always inject a checker (covered by tests).
      return ValidationResult.feasible();
    }

    // Single-instance disable: only this instance's replicas stop serving, so no other instance is
    // presumed stopped.
    MinActiveReplicaCheckResult result = checker.check(instanceName, Collections.emptySet());
    if (result.isPassed()) {
      return ValidationResult.feasible();
    }

    Violation violation = Violation.newBuilder(RULE_ID)
        .resource(result.getResourceName())
        .partition(result.getPartitionName())
        .message(String.format(
            "Disabling instance %s would leave resource %s partition %s with %d active replica(s) on "
                + "the remaining instances, below its required minimum of %d. Disabling drains this "
                + "instance's replicas, so the partition would fall below minActiveReplicas until the "
                + "controller can recover it elsewhere, risking reduced availability or loss of its top "
                + "state. Wait for the partition to recover or add capacity before disabling, or pass "
                + "force=true to override.",
            instanceName, result.getResourceName(), result.getPartitionName(),
            result.getCurrentActiveReplicas(), result.getRequiredMinActiveReplicas()))
        .build();
    return ValidationResult.infeasible(violation);
  }
}
