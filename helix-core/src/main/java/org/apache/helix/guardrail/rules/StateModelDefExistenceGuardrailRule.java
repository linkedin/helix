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

import org.apache.helix.PropertyKey;
import org.apache.helix.guardrail.GuardrailContext;
import org.apache.helix.guardrail.GuardrailRule;
import org.apache.helix.guardrail.ReadOnlyDataAccessor;
import org.apache.helix.guardrail.ValidationResult;
import org.apache.helix.guardrail.Violation;
import org.apache.helix.model.IdealState;
import org.apache.helix.model.StateModelDefinition;


/**
 * Blocks writing a resource ideal state whose {@code STATE_MODEL_DEF_REF} names a
 * {@link StateModelDefinition} that does not exist in the cluster's {@code STATEMODELDEFS}.
 * <p>
 * The controller drives every partition's state transitions from the state model named by the
 * resource's {@code STATE_MODEL_DEF_REF}, so if that definition is absent the controller cannot
 * compute any transition and the resource can never be brought online. The plain {@code addResource}
 * admin path already rejects a missing state model up front, but the two helix-rest write paths that
 * bypass that check each fail worse and later. The ideal-state update path ({@code updateIdealState})
 * persists the ideal state and returns success, so the failure surfaces only later &mdash; on the
 * controller's next pipeline run &mdash; rather than as an error on the write. The WAGED add path
 * ({@code addResourceWithWeight}) fails late, <em>after a partial write</em>: it persists the resource
 * config and ideal state and only then calls {@code rebalance()}, which throws because the state model
 * cannot be found, so the caller gets a generic failure while the half-created resource is left behind
 * in ZooKeeper. This rule closes the gap by applying the same existence check at pre-write time, so
 * both paths reject the request with a clear, actionable error before any write.
 * <p>
 * The rule is a <em>delta</em> guard, consistent with the other ideal-state rules: it fires only when
 * the write <em>introduces or changes</em> the reference to an unresolvable one. A resource that
 * already references a missing state model &mdash; e.g. because the definition was removed after the
 * resource was created &mdash; is grandfathered, so an edit to an unrelated field of such a resource
 * is not blocked; only an add, or an edit that points {@code STATE_MODEL_DEF_REF} at a (still)
 * non-existent definition, is rejected. A null or empty reference is a separate structural defect that
 * is outside this rule's concern and is certified feasible so a dedicated structural check can own it.
 */
public class StateModelDefExistenceGuardrailRule implements GuardrailRule {

  public static final String RULE_ID = "STATE_MODEL_DEF_NOT_FOUND";

  @Override
  public String getId() {
    return RULE_ID;
  }

  @Override
  public ValidationResult validate(GuardrailContext context) {
    IdealState proposedIdealState = context.getProposedIdealState();
    if (proposedIdealState == null) {
      // Not an ideal-state mutation; there is nothing for this rule to certify.
      return ValidationResult.feasible();
    }

    String proposedStateModelDefRef = proposedIdealState.getStateModelDefRef();
    if (proposedStateModelDefRef == null || proposedStateModelDefRef.isEmpty()) {
      // A missing/empty reference is a separate structural defect, not this rule's concern.
      return ValidationResult.feasible();
    }

    ReadOnlyDataAccessor dataAccessor = context.getDataAccessor();
    PropertyKey.Builder keyBuilder = dataAccessor.keyBuilder();
    if (stateModelDefExists(dataAccessor, keyBuilder, proposedStateModelDefRef)) {
      return ValidationResult.feasible();
    }

    // The proposed reference does not resolve. Grandfather a resource that already referenced this
    // same missing definition, so an edit to an unrelated field of an already-broken resource is not
    // blocked; only an add, or an edit that newly points at a non-existent definition, is rejected.
    String resourceName = proposedIdealState.getResourceName();
    IdealState existingIdealState = readIdealState(dataAccessor, keyBuilder, resourceName);
    if (existingIdealState != null
        && proposedStateModelDefRef.equals(existingIdealState.getStateModelDefRef())) {
      return ValidationResult.feasible();
    }

    return ValidationResult.infeasible(Violation.newBuilder(RULE_ID)
        .resource(resourceName)
        .message(String.format(
            "Writing the ideal state of resource \"%s\" would set STATE_MODEL_DEF_REF to \"%s\", "
                + "which does not exist in the cluster's STATEMODELDEFS. The controller cannot "
                + "compute any state transition for a resource whose state model is missing, so the "
                + "resource would be accepted but could never be brought online. Register the state "
                + "model definition first, or set STATE_MODEL_DEF_REF to an existing state model.",
            resourceName, proposedStateModelDefRef))
        .build());
  }

  private static boolean stateModelDefExists(ReadOnlyDataAccessor dataAccessor,
      PropertyKey.Builder keyBuilder, String stateModelDefRef) {
    StateModelDefinition stateModelDef =
        dataAccessor.getProperty(keyBuilder.stateModelDef(stateModelDefRef));
    return stateModelDef != null;
  }

  private static IdealState readIdealState(ReadOnlyDataAccessor dataAccessor,
      PropertyKey.Builder keyBuilder, String resourceName) {
    if (resourceName == null) {
      return null;
    }
    return dataAccessor.getProperty(keyBuilder.idealStates(resourceName));
  }
}
