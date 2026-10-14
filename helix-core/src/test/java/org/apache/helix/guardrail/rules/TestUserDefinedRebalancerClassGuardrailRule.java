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
import org.apache.helix.controller.rebalancer.Rebalancer;
import org.apache.helix.controller.rebalancer.SemiAutoRebalancer;
import org.apache.helix.guardrail.GuardrailContext;
import org.apache.helix.guardrail.ValidationResult;
import org.apache.helix.guardrail.Violation;
import org.apache.helix.model.IdealState;
import org.testng.Assert;
import org.testng.annotations.Test;

import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * This is a delta guard: it fires only when an ideal-state write introduces or changes a resource
 * into a USER_DEFINED configuration whose REBALANCER_CLASS_NAME is unset, unloadable, or not a
 * {@link Rebalancer}, and grandfathers a resource that already referenced that same unresolvable
 * class. The tests mock the metadata store the rule reads back &mdash; the current (pre-write) ideal
 * state keyed by resource name &mdash; and pass the proposed (for an add, the new; for an update, the
 * merged post-write) ideal state through the context. Real classes are used to exercise class
 * resolution: {@link SemiAutoRebalancer} is loadable and a {@code Rebalancer}; {@code java.lang.String}
 * is loadable but not a {@code Rebalancer}; a made-up name is unloadable.
 */
public class TestUserDefinedRebalancerClassGuardrailRule {
  private static final String CLUSTER = "testCluster";
  private static final String RESOURCE = "testResource";
  private static final String VALID_REBALANCER = SemiAutoRebalancer.class.getName();
  private static final String NOT_A_REBALANCER = String.class.getName();
  private static final String UNLOADABLE = "com.example.NoSuchRebalancer";
  private static final PropertyKey.Builder BUILDER = new PropertyKey.Builder(CLUSTER);
  private final UserDefinedRebalancerClassGuardrailRule rule =
      new UserDefinedRebalancerClassGuardrailRule();

  @Test
  public void testNullProposedIdealStateIsFeasible() {
    // No proposed ideal state means this is not an ideal-state mutation; nothing to certify.
    GuardrailContext context = GuardrailContext.newBuilder(CLUSTER)
        .dataAccessor(mock(HelixDataAccessor.class))
        .build();
    Assert.assertTrue(rule.validate(context).isFeasible());
  }

  @Test
  public void testNonUserDefinedModeIsFeasible() {
    // A non-USER_DEFINED resource resolves its rebalancer from a built-in/framework source, so the
    // rule does not apply even when no rebalancer class name is set.
    Assert.assertTrue(
        rule.validate(context(idealState(IdealState.RebalanceMode.FULL_AUTO, null), null))
            .isFeasible());
    Assert.assertTrue(
        rule.validate(context(idealState(IdealState.RebalanceMode.SEMI_AUTO, null), null))
            .isFeasible());
  }

  @Test
  public void testNonUserDefinedModeIgnoresBadClassName() {
    // Even an unloadable class name is irrelevant when the mode is not USER_DEFINED.
    Assert.assertTrue(
        rule.validate(context(idealState(IdealState.RebalanceMode.FULL_AUTO, UNLOADABLE), null))
            .isFeasible());
  }

  @Test
  public void testUserDefinedWithValidRebalancerIsFeasible() {
    // USER_DEFINED pointing at a loadable Rebalancer implementation is safe to certify.
    ValidationResult result =
        rule.validate(context(idealState(IdealState.RebalanceMode.USER_DEFINED, VALID_REBALANCER),
            null));
    Assert.assertTrue(result.isFeasible());
    Assert.assertTrue(result.getViolations().isEmpty());
  }

  @Test
  public void testUserDefinedWithMissingClassOnAddIsInfeasible() {
    // An add (no existing ideal state) that is USER_DEFINED with no rebalancer class must be blocked.
    assertInfeasible(
        rule.validate(context(idealState(IdealState.RebalanceMode.USER_DEFINED, null), null)));
    assertInfeasible(
        rule.validate(context(idealState(IdealState.RebalanceMode.USER_DEFINED, "  "), null)));
  }

  @Test
  public void testUserDefinedWithUnloadableClassOnAddIsInfeasible() {
    // USER_DEFINED naming a class absent from the classpath must be blocked.
    ValidationResult result =
        rule.validate(context(idealState(IdealState.RebalanceMode.USER_DEFINED, UNLOADABLE), null));
    assertInfeasible(result);
    Assert.assertTrue(result.getViolations().get(0).getMessage().contains(UNLOADABLE));
  }

  @Test
  public void testUserDefinedWithNonRebalancerClassIsInfeasible() {
    // A class that loads but is not a Rebalancer would fail the controller's cast, so block it.
    ValidationResult result = rule.validate(
        context(idealState(IdealState.RebalanceMode.USER_DEFINED, NOT_A_REBALANCER), null));
    assertInfeasible(result);
    Assert.assertTrue(result.getViolations().get(0).getMessage().contains(NOT_A_REBALANCER));
  }

  @Test
  public void testSwitchToUserDefinedWithBadClassIsInfeasible() {
    // The existing resource was a valid FULL_AUTO resource; the edit switches it to USER_DEFINED with
    // an unloadable class, newly introducing the hazard, so it must be blocked.
    ValidationResult result =
        rule.validate(context(idealState(IdealState.RebalanceMode.USER_DEFINED, UNLOADABLE),
            idealState(IdealState.RebalanceMode.FULL_AUTO, null)));
    assertInfeasible(result);
  }

  @Test
  public void testChangedUnresolvableClassIsInfeasible() {
    // The existing resource was already USER_DEFINED but with a different unloadable class; the edit
    // changes REBALANCER_CLASS_NAME to yet another unresolvable class, so it is not grandfathered.
    ValidationResult result =
        rule.validate(context(idealState(IdealState.RebalanceMode.USER_DEFINED, NOT_A_REBALANCER),
            idealState(IdealState.RebalanceMode.USER_DEFINED, UNLOADABLE)));
    assertInfeasible(result);
  }

  @Test
  public void testPreExistingUnresolvableClassIsFeasible() {
    // The resource was already USER_DEFINED with this same unloadable class before the edit; the edit
    // does not introduce or change the reference, so the pre-existing state is grandfathered rather
    // than blocked, keeping edits to unrelated fields of an already-broken resource unblocked.
    ValidationResult result =
        rule.validate(context(idealState(IdealState.RebalanceMode.USER_DEFINED, UNLOADABLE),
            idealState(IdealState.RebalanceMode.USER_DEFINED, UNLOADABLE)));
    Assert.assertTrue(result.isFeasible());
  }

  private void assertInfeasible(ValidationResult result) {
    Assert.assertFalse(result.isFeasible());
    Assert.assertEquals(result.getViolations().size(), 1);
    Violation violation = result.getViolations().get(0);
    Assert.assertEquals(violation.getRuleId(), UserDefinedRebalancerClassGuardrailRule.RULE_ID);
    Assert.assertEquals(violation.getResourceName(), RESOURCE);
  }

  private GuardrailContext context(IdealState proposedIdealState, IdealState existingIdealState) {
    return GuardrailContext.newBuilder(CLUSTER)
        .dataAccessor(accessor(existingIdealState))
        .proposedIdealState(proposedIdealState)
        .build();
  }

  private static HelixDataAccessor accessor(IdealState existingIdealState) {
    HelixDataAccessor dataAccessor = mock(HelixDataAccessor.class);
    when(dataAccessor.keyBuilder()).thenReturn(BUILDER);
    doReturn(existingIdealState).when(dataAccessor).getProperty(BUILDER.idealStates(RESOURCE));
    return dataAccessor;
  }

  private static IdealState idealState(IdealState.RebalanceMode mode, String rebalancerClassName) {
    IdealState idealState = new IdealState(RESOURCE);
    idealState.setStateModelDefRef("MasterSlave");
    idealState.setRebalanceMode(mode);
    if (rebalancerClassName != null) {
      idealState.setRebalancerClassName(rebalancerClassName);
    }
    return idealState;
  }
}
