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

import org.apache.helix.HelixDataAccessor;
import org.apache.helix.PropertyKey;
import org.apache.helix.guardrail.GuardrailContext;
import org.apache.helix.guardrail.ValidationResult;
import org.apache.helix.guardrail.Violation;
import org.apache.helix.model.IdealState;
import org.apache.helix.model.StateModelDefinition;
import org.testng.Assert;
import org.testng.annotations.Test;

import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * This is a delta guard: it fires only when an ideal-state write introduces or changes
 * STATE_MODEL_DEF_REF to a state model that is absent from the cluster's STATEMODELDEFS, and
 * grandfathers a resource that already referenced that same missing model. The tests mock the
 * metadata store the rule reads back &mdash; the state model definition keyed by the proposed
 * reference, and the current (pre-write) ideal state keyed by resource name &mdash; and pass the
 * proposed (for an add, the new; for an update, the merged post-write) ideal state through the
 * context.
 */
public class TestStateModelDefExistenceGuardrailRule {
  private static final String CLUSTER = "testCluster";
  private static final String RESOURCE = "testResource";
  private static final PropertyKey.Builder BUILDER = new PropertyKey.Builder(CLUSTER);
  private final StateModelDefExistenceGuardrailRule rule =
      new StateModelDefExistenceGuardrailRule();

  @Test
  public void testNullProposedIdealStateIsFeasible() {
    // No proposed ideal state means this is not an ideal-state mutation; nothing to certify.
    GuardrailContext context = GuardrailContext.newBuilder(CLUSTER)
        .dataAccessor(mock(HelixDataAccessor.class))
        .build();
    Assert.assertTrue(rule.validate(context).isFeasible());
  }

  @Test
  public void testNullStateModelDefRefIsFeasible() {
    // An unset reference is a separate structural defect, not this rule's concern; defer.
    GuardrailContext context = GuardrailContext.newBuilder(CLUSTER)
        .dataAccessor(mock(HelixDataAccessor.class))
        .proposedIdealState(idealState(null))
        .build();
    Assert.assertTrue(rule.validate(context).isFeasible());
  }

  @Test
  public void testEmptyStateModelDefRefIsFeasible() {
    // An empty reference is likewise out of scope and must not be treated as a missing model.
    GuardrailContext context = GuardrailContext.newBuilder(CLUSTER)
        .dataAccessor(mock(HelixDataAccessor.class))
        .proposedIdealState(idealState(""))
        .build();
    Assert.assertTrue(rule.validate(context).isFeasible());
  }

  @Test
  public void testExistingStateModelDefIsFeasible() {
    // The proposed reference resolves in STATEMODELDEFS, so the write is safe to certify.
    ValidationResult result =
        rule.validate(context(idealState("MasterSlave"), "MasterSlave", null));
    Assert.assertTrue(result.isFeasible());
    Assert.assertTrue(result.getViolations().isEmpty());
  }

  @Test
  public void testMissingStateModelDefOnAddIsInfeasible() {
    // An add (no existing ideal state) whose reference does not resolve must be blocked.
    ValidationResult result = rule.validate(context(idealState("Bogus"), null, null));
    Assert.assertFalse(result.isFeasible());
    Assert.assertEquals(result.getViolations().size(), 1);
    Violation violation = result.getViolations().get(0);
    Assert.assertEquals(violation.getRuleId(), StateModelDefExistenceGuardrailRule.RULE_ID);
    Assert.assertEquals(violation.getResourceName(), RESOURCE);
    Assert.assertTrue(violation.getMessage().contains("Bogus"));
  }

  @Test
  public void testNewlyIntroducedMissingStateModelDefIsInfeasible() {
    // The existing resource referenced a valid model; the edit repoints STATE_MODEL_DEF_REF at a
    // model absent from STATEMODELDEFS, newly introducing the hazard, so it must be blocked.
    ValidationResult result =
        rule.validate(context(idealState("Bogus"), "MasterSlave", idealState("MasterSlave")));
    Assert.assertFalse(result.isFeasible());
    Violation violation = result.getViolations().get(0);
    Assert.assertEquals(violation.getRuleId(), StateModelDefExistenceGuardrailRule.RULE_ID);
    Assert.assertTrue(violation.getMessage().contains("Bogus"));
  }

  @Test
  public void testPreExistingMissingStateModelDefIsFeasible() {
    // The resource already referenced this (now) missing model before the edit; the edit does not
    // introduce or change the reference, so the pre-existing state is grandfathered rather than
    // blocked. This keeps edits to unrelated fields of an already-broken resource unblocked.
    ValidationResult result =
        rule.validate(context(idealState("Bogus"), null, idealState("Bogus")));
    Assert.assertTrue(result.isFeasible());
  }

  private GuardrailContext context(IdealState proposedIdealState, String resolvableStateModelDef,
      IdealState existingIdealState) {
    return GuardrailContext.newBuilder(CLUSTER)
        .dataAccessor(accessor(resolvableStateModelDef, existingIdealState))
        .proposedIdealState(proposedIdealState)
        .build();
  }

  private static HelixDataAccessor accessor(String resolvableStateModelDef,
      IdealState existingIdealState) {
    HelixDataAccessor dataAccessor = mock(HelixDataAccessor.class);
    when(dataAccessor.keyBuilder()).thenReturn(BUILDER);
    // Only the named state model resolves; getProperty for any other reference returns the mock
    // default of null, modeling a definition absent from STATEMODELDEFS.
    if (resolvableStateModelDef != null) {
      doReturn(mock(StateModelDefinition.class)).when(dataAccessor)
          .getProperty(BUILDER.stateModelDef(resolvableStateModelDef));
    }
    doReturn(existingIdealState).when(dataAccessor).getProperty(BUILDER.idealStates(RESOURCE));
    return dataAccessor;
  }

  private static IdealState idealState(String stateModelDefRef) {
    IdealState idealState = new IdealState(RESOURCE);
    idealState.setRebalanceMode(IdealState.RebalanceMode.FULL_AUTO);
    if (stateModelDefRef != null) {
      idealState.setStateModelDefRef(stateModelDefRef);
    }
    return idealState;
  }
}
