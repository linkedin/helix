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

import org.apache.helix.HelixException;
import org.apache.helix.guardrail.GuardrailContext;
import org.apache.helix.guardrail.GuardrailRule;
import org.apache.helix.guardrail.ValidationResult;
import org.apache.helix.guardrail.Violation;
import org.apache.helix.util.InstanceValidationUtil;
import org.apache.helix.util.MinActiveReplicaCheckResult;

/**
 * Guard rail that blocks removing an instance when doing so would drop any partition hosted on it
 * below its configured minimum active replica count.
 * <p>
 * The check itself is delegated to
 * {@link InstanceValidationUtil#siblingNodesActiveReplicaCheckWithDetails}, which walks every
 * enabled, non-Task resource and, for each partition hosted on the target instance, counts the
 * healthy replicas that would remain on sibling instances. If that count is below the resource's
 * {@code minActiveReplicas}, the mutation is unsafe and this rule reports the first offending
 * resource / partition as a {@link Violation}.
 * <p>
 * The evaluation is scoped to resources actually hosted on the instance: a resource whose
 * ExternalView has not been computed yet has no committed placement, so it is skipped rather than
 * blocking the drop. This prevents a single not-yet-placed resource from blocking instance removal
 * across the entire cluster while its ExternalView is still being computed.
 */
public class MinActiveReplicaGuardrailRule implements GuardrailRule {
  public static final String RULE_ID = "MIN_ACTIVE_REPLICA_ON_INSTANCE_DROP";

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

    MinActiveReplicaCheckResult checkResult;
    try {
      checkResult = InstanceValidationUtil.siblingNodesActiveReplicaCheckWithDetails(
          context.getDataAccessor(), instanceName, Collections.emptySet(), true);
    } catch (HelixException e) {
      // Resources not yet placed (no ExternalView) are skipped above, so this only trips on an
      // unexpected evaluation failure. We cannot certify the drop as safe, so fail closed with an
      // honest message rather than mislabeling an evaluation failure as a min-active-replica
      // shortfall.
      return ValidationResult.infeasible(Violation.newBuilder(RULE_ID)
          .message(String.format(
              "Could not verify minimum active replicas for instance %s: %s Blocking as a "
                  + "precaution; retry once cluster state is fully available, or use force=true to "
                  + "override.", instanceName, e.getMessage()))
          .build());
    }

    if (checkResult.isPassed()) {
      return ValidationResult.feasible();
    }

    Violation violation = Violation.newBuilder(RULE_ID)
        .resource(checkResult.getResourceName())
        .partition(checkResult.getPartitionName())
        .message(String.format(
            "Dropping instance %s would leave resource %s partition %s with %d active replica(s), "
                + "below the required minimum of %d.", instanceName, checkResult.getResourceName(),
            checkResult.getPartitionName(), checkResult.getCurrentActiveReplicas(),
            checkResult.getRequiredMinActiveReplicas()))
        .build();
    return ValidationResult.infeasible(violation);
  }
}
