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

import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.apache.helix.HelixDefinedState;
import org.apache.helix.PropertyKey;
import org.apache.helix.guardrail.GuardrailContext;
import org.apache.helix.guardrail.GuardrailRule;
import org.apache.helix.guardrail.ReadOnlyDataAccessor;
import org.apache.helix.guardrail.ValidationResult;
import org.apache.helix.guardrail.Violation;
import org.apache.helix.model.ExternalView;

/**
 * Guard rail that blocks dropping a resource while it is still in use -- that is, while its
 * {@code EXTERNALVIEW} still has replicas placed somewhere in the cluster. Dropping a live resource
 * transitions every partition to {@code DROPPED}, tearing down the serving assignment; doing it by
 * mistake (wrong resource name, or a resource an operator forgot is still serving) is an outage, and
 * nothing downstream catches it.
 * <p>
 * Unlike an instance drop -- which the admin layer independently rejects while the instance is live
 * ({@code ZKHelixAdmin.dropInstance} throws) -- a resource drop has <em>no</em> such backstop:
 * {@code ZKHelixAdmin.dropResource} performs no safety check and writes straight through to
 * ZooKeeper. This rule is therefore the primary safety gate for the operation, not merely a
 * dry-run-friendly front end for a check that also lives deeper down. Authorization (the REST
 * {@code @ClusterAuth} filter) answers <em>who</em> may drop the resource; this rule answers
 * <em>whether the drop is safe right now</em>, a dimension authorization does not cover.
 * <p>
 * "In use" is judged from the external view, the controller's view of the resource's actual current
 * placement, rather than from the ideal state (which is only intent). A replica counts as still
 * placed when its state is anything other than {@link HelixDefinedState#DROPPED}, matching the
 * "placed replica" convention already used by {@link WagedRebalanceFeasibilityWhatIf}. This keeps
 * the rule state-model-agnostic: it never hardcodes which states are "serving" (MASTER / LEADER /
 * ONLINE / ...), so it works unchanged for every built-in and custom state model. It is also
 * deliberately conservative -- {@code OFFLINE} and {@code ERROR} replicas still count as placed, so
 * a resource mid-transition is treated as in use and the drop is blocked.
 * <p>
 * The verdict is forceable: {@code force=true} overrides it (the operator asserts they really mean
 * to tear the resource down), and {@code dryRun=true} reports it without dropping. A resource with no
 * external view, an empty external view, or one whose replicas are all {@code DROPPED} is <em>not</em>
 * in use, so the drop is certified feasible -- letting dead or never-placed resources be cleaned up
 * freely.
 */
public class ResourceInUseGuardrailRule implements GuardrailRule {
  public static final String RULE_ID = "RESOURCE_IN_USE_ON_RESOURCE_DROP";

  @Override
  public String getId() {
    return RULE_ID;
  }

  @Override
  public ValidationResult validate(GuardrailContext context) {
    String resourceName = context.getResourceName();
    if (resourceName == null) {
      // No target resource to evaluate; nothing for this rule to certify.
      return ValidationResult.feasible();
    }

    ReadOnlyDataAccessor dataAccessor = context.getDataAccessor();
    PropertyKey externalViewKey = dataAccessor.keyBuilder().externalView(resourceName);
    ExternalView externalView = dataAccessor.getProperty(externalViewKey);
    if (externalView == null) {
      // No external view: the controller is not placing this resource anywhere, so the drop tears
      // down nothing that is live.
      return ValidationResult.feasible();
    }

    int activePartitions = 0;
    int activeReplicas = 0;
    Set<String> activeInstances = new HashSet<>();
    for (String partition : externalView.getPartitionSet()) {
      Map<String, String> stateMap = externalView.getStateMap(partition);
      if (stateMap == null) {
        continue;
      }
      boolean partitionActive = false;
      for (Map.Entry<String, String> replica : stateMap.entrySet()) {
        String state = replica.getValue();
        // A replica in any state other than DROPPED still occupies a placement slot and is counted
        // as in use; this never enumerates "serving" states, so the rule stays state-model-agnostic.
        if (state != null && !HelixDefinedState.DROPPED.name().equals(state)) {
          partitionActive = true;
          activeReplicas++;
          activeInstances.add(replica.getKey());
        }
      }
      if (partitionActive) {
        activePartitions++;
      }
    }

    if (activePartitions == 0) {
      // Empty external view, or every replica already DROPPED: nothing is actively placed, so the
      // drop is safe to proceed.
      return ValidationResult.feasible();
    }

    Violation violation = Violation.newBuilder(RULE_ID)
        .message(String.format(
            "Resource %s is still in use: its external view has %d partition(s) with %d active "
                + "replica(s) placed across %d instance(s), so dropping it now would tear down that "
                + "live assignment (every partition transitions to DROPPED). Disable the resource and "
                + "let it drain first, or retry with force=true to override.",
            resourceName, activePartitions, activeReplicas, activeInstances.size()))
        .build();
    return ValidationResult.infeasible(violation);
  }
}
