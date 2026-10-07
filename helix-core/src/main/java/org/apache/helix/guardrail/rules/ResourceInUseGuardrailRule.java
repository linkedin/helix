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
import org.apache.helix.model.IdealState;
import org.apache.helix.model.StateModelDefinition;

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
 * placement, rather than from the ideal state (which is only intent). A replica counts as still in
 * use when its state is anything other than a <em>drained</em> state: {@link HelixDefinedState#DROPPED}
 * or the resource's state-model <em>initial</em> state (where a disabled resource settles --
 * {@code OFFLINE} for the built-in MasterSlave / LeaderStandby / OnlineOffline models, but resolved
 * from the resource's own state model, so a model whose initial state differs works unchanged). This
 * keeps the rule state-model-agnostic: it hardcodes neither which states are "serving" (MASTER /
 * LEADER / ONLINE / ...) nor which state is "drained". It stays conservative about genuinely
 * non-drained states -- {@code ERROR} (and every serving state) still counts as in use, so an
 * actively serving or unhealthy resource is blocked -- while recognizing that a resource drained to
 * its initial state is idle, so the standard "disable the resource, let it drain, then drop" workflow
 * reaches a feasible verdict without {@code force=true}. If the state model cannot be resolved, only
 * {@code DROPPED} is treated as drained -- a conservative fallback that keeps a possibly-still-serving
 * resource blocked.
 * <p>
 * The verdict is forceable: {@code force=true} overrides it (the operator asserts they really mean
 * to tear the resource down), and {@code dryRun=true} reports it without dropping. A resource with no
 * external view, an empty external view, or one whose replicas are all drained (its state-model
 * initial state or {@code DROPPED}) is <em>not</em> in use, so the drop is certified feasible --
 * letting dead, drained, or never-placed resources be cleaned up freely.
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

    // The states a replica can hold without keeping the resource "in use": DROPPED plus the
    // resource's state-model initial state (OFFLINE for the built-in models), resolved from the
    // resource's own state model so the rule stays correct for every state model.
    Set<String> drainedStates = resolveDrainedStates(dataAccessor, resourceName);

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
        // A replica keeps the resource "in use" unless it has drained to one of the drained states
        // (DROPPED or the state-model initial state). This never enumerates "serving" states, so the
        // rule stays state-model-agnostic; ERROR and every serving state still count as in use.
        if (state != null && !drainedStates.contains(state)) {
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
      // Empty external view, or every replica already drained (initial state / DROPPED): nothing is
      // actively placed, so the drop is safe to proceed.
      return ValidationResult.feasible();
    }

    Violation violation = Violation.newBuilder(RULE_ID)
        .message(String.format(
            "Resource %s is still in use: its external view has %d partition(s) with %d active "
                + "replica(s) placed across %d instance(s), so dropping it now would tear down that "
                + "live assignment (every partition transitions to DROPPED). Disable the resource and "
                + "wait for its replicas to drain to its initial state (e.g. OFFLINE), or otherwise "
                + "remove its assignment, before retrying -- or retry with force=true to override.",
            resourceName, activePartitions, activeReplicas, activeInstances.size()))
        .build();
    return ValidationResult.infeasible(violation);
  }

  /**
   * Resolves the resource's drained states -- the states a replica can hold without keeping the
   * resource "in use". {@link HelixDefinedState#DROPPED} is always drained. The resource's state-model
   * initial state (where a disabled resource settles -- {@code OFFLINE} for the built-in MasterSlave /
   * LeaderStandby / OnlineOffline models, model-defined in general) is added when it can be resolved
   * from the current ideal state's state-model definition. Resolving it from the model, instead of
   * hardcoding {@code OFFLINE}, keeps the rule correct for every state model, including custom ones.
   * If the model cannot be resolved -- no ideal state, no state-model ref, or a missing definition --
   * only {@code DROPPED} is treated as drained: a conservative fallback that keeps a
   * possibly-still-serving resource blocked rather than wrongly certifying the drop.
   */
  private static Set<String> resolveDrainedStates(ReadOnlyDataAccessor dataAccessor,
      String resourceName) {
    Set<String> drainedStates = new HashSet<>();
    drainedStates.add(HelixDefinedState.DROPPED.name());

    PropertyKey.Builder keyBuilder = dataAccessor.keyBuilder();
    IdealState idealState = dataAccessor.getProperty(keyBuilder.idealStates(resourceName));
    if (idealState == null) {
      return drainedStates;
    }
    String stateModelDefRef = idealState.getStateModelDefRef();
    if (stateModelDefRef == null) {
      return drainedStates;
    }
    StateModelDefinition stateModelDef =
        dataAccessor.getProperty(keyBuilder.stateModelDef(stateModelDefRef));
    if (stateModelDef != null && stateModelDef.getInitialState() != null) {
      drainedStates.add(stateModelDef.getInitialState());
    }
    return drainedStates;
  }
}
