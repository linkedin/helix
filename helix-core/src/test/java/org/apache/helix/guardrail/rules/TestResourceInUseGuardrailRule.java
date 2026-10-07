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
import org.apache.helix.HelixDefinedState;
import org.apache.helix.PropertyKey;
import org.apache.helix.PropertyType;
import org.apache.helix.guardrail.GuardrailContext;
import org.apache.helix.guardrail.GuardrailPipeline;
import org.apache.helix.guardrail.ValidationResult;
import org.apache.helix.guardrail.Violation;
import org.apache.helix.model.BuiltInStateModelDefinitions;
import org.apache.helix.model.ExternalView;
import org.apache.helix.model.IdealState;
import org.apache.helix.model.StateModelDefinition;
import org.mockito.ArgumentMatcher;
import org.testng.Assert;
import org.testng.annotations.Test;

import static org.mockito.Mockito.argThat;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Unit tests for {@link ResourceInUseGuardrailRule}. The rule reads the target resource's
 * {@code EXTERNALVIEW} through a mocked {@link HelixDataAccessor}: replicas placed in any state other
 * than a drained state -- {@code DROPPED} or the resource's state-model initial state (resolved from
 * the ideal state's state-model definition, {@code OFFLINE} for the built-in models) -- mean the
 * resource is still in use (drop must be blocked), while a missing / empty / all-drained external
 * view means it is not (drop may proceed). When the state model cannot be resolved, only
 * {@code DROPPED} counts as drained.
 */
public class TestResourceInUseGuardrailRule {
  private static final String CLUSTER = "testCluster";
  private static final String RESOURCE = "testResource";
  private static final PropertyKey.Builder BUILDER = new PropertyKey.Builder(CLUSTER);

  private final ResourceInUseGuardrailRule rule = new ResourceInUseGuardrailRule();

  @Test
  public void testNullResourceIsFeasible() {
    GuardrailContext context = GuardrailContext.newBuilder(CLUSTER)
        .dataAccessor(mock(HelixDataAccessor.class))
        .build();
    ValidationResult result = rule.validate(context);
    Assert.assertTrue(result.isFeasible());
  }

  @Test
  public void testNoExternalViewIsFeasible() {
    // No EXTERNALVIEW znode: getProperty(...) returns null (the mock default), so the resource is
    // not placed and the drop is safe.
    HelixDataAccessor dataAccessor = mock(HelixDataAccessor.class);
    when(dataAccessor.keyBuilder()).thenReturn(BUILDER);
    GuardrailContext context = contextFor(dataAccessor);

    ValidationResult result = rule.validate(context);

    Assert.assertTrue(result.isFeasible());
    Assert.assertTrue(result.getViolations().isEmpty());
  }

  @Test
  public void testEmptyExternalViewIsFeasible() {
    // An external view with no partitions means the resource holds no placement.
    ValidationResult result = rule.validate(contextWithExternalView(new ExternalView(RESOURCE)));
    Assert.assertTrue(result.isFeasible());
    Assert.assertTrue(result.getViolations().isEmpty());
  }

  @Test
  public void testAllDroppedReplicasIsFeasible() {
    // Every replica is already DROPPED, so nothing is actively placed and the drop is safe.
    ExternalView externalView = new ExternalView(RESOURCE);
    externalView.setState("p0", "instance0", HelixDefinedState.DROPPED.name());
    externalView.setState("p1", "instance1", HelixDefinedState.DROPPED.name());

    ValidationResult result = rule.validate(contextWithExternalView(externalView));

    Assert.assertTrue(result.isFeasible());
    Assert.assertTrue(result.getViolations().isEmpty());
  }

  @Test
  public void testActiveReplicasAreInfeasible() {
    ExternalView externalView = new ExternalView(RESOURCE);
    externalView.setState("p0", "instance0", "MASTER");
    externalView.setState("p0", "instance1", "SLAVE");
    externalView.setState("p1", "instance2", HelixDefinedState.DROPPED.name()); // ignored (drained)
    externalView.setState("p1", "instance3", "OFFLINE"); // ignored (drained, not in use)

    ValidationResult result = rule.validate(contextWithExternalView(externalView));

    Assert.assertFalse(result.isFeasible());
    Assert.assertEquals(result.getViolations().size(), 1);
    Violation violation = result.getViolations().get(0);
    Assert.assertEquals(violation.getRuleId(), ResourceInUseGuardrailRule.RULE_ID);
    Assert.assertTrue(violation.getMessage().contains(RESOURCE));
    Assert.assertTrue(violation.getMessage().contains("in use"));
  }

  @Test
  public void testAllOfflineReplicasIsFeasible() {
    // OFFLINE is a drained, non-serving state (a disabled resource settles here), so -- like DROPPED
    // -- it does not keep the resource in use; the drop is allowed without force.
    ExternalView externalView = new ExternalView(RESOURCE);
    externalView.setState("p0", "instance0", "OFFLINE");
    externalView.setState("p1", "instance1", "OFFLINE");

    ValidationResult result = rule.validate(contextWithExternalView(externalView));

    Assert.assertTrue(result.isFeasible());
    Assert.assertTrue(result.getViolations().isEmpty());
  }

  @Test
  public void testErrorReplicaCountsAsInUse() {
    // ERROR is not a drained state: a resource with partitions in ERROR is not cleanly disabled, so
    // it still counts as in use and the drop is blocked (the operator can still override with force).
    ExternalView externalView = new ExternalView(RESOURCE);
    externalView.setState("p0", "instance0", HelixDefinedState.ERROR.name());

    ValidationResult result = rule.validate(contextWithExternalView(externalView));

    Assert.assertFalse(result.isFeasible());
    Assert.assertEquals(result.getViolations().get(0).getRuleId(),
        ResourceInUseGuardrailRule.RULE_ID);
  }

  @Test
  public void testCustomStateModelStateCountsAsInUse() {
    // The rule never enumerates "serving" states, so an arbitrary custom-state-model state (not
    // DROPPED) still counts as placed -- proving the check is state-model-agnostic.
    ExternalView externalView = new ExternalView(RESOURCE);
    externalView.setState("p0", "instance0", "SOME_CUSTOM_STATE");

    ValidationResult result = rule.validate(contextWithExternalView(externalView));

    Assert.assertFalse(result.isFeasible());
    Assert.assertEquals(result.getViolations().get(0).getRuleId(),
        ResourceInUseGuardrailRule.RULE_ID);
  }

  @Test
  public void testDrainedToStateModelInitialStateIsFeasible() {
    // The drained state is resolved from the resource's own state model, not hardcoded to OFFLINE.
    // For a model whose initial state is PARKED, an all-PARKED external view is fully drained, so the
    // drop is certified feasible -- directly proving the rule is state-model-agnostic about draining.
    StateModelDefinition customDef = mock(StateModelDefinition.class);
    when(customDef.getInitialState()).thenReturn("PARKED");
    ExternalView externalView = new ExternalView(RESOURCE);
    externalView.setState("p0", "instance0", "PARKED");
    externalView.setState("p1", "instance1", "PARKED");

    ValidationResult result =
        rule.validate(contextWithExternalView(externalView, "CustomLifecycle", customDef));

    Assert.assertTrue(result.isFeasible());
    Assert.assertTrue(result.getViolations().isEmpty());
  }

  @Test
  public void testNonInitialStateCountsAsInUseForCustomModel() {
    // The counterpart: for a model whose initial state is PARKED (not OFFLINE), an OFFLINE replica is
    // NOT drained, so it keeps the resource in use. This proves the rule resolves the drained state
    // from the model rather than hardcoding OFFLINE.
    StateModelDefinition customDef = mock(StateModelDefinition.class);
    when(customDef.getInitialState()).thenReturn("PARKED");
    ExternalView externalView = new ExternalView(RESOURCE);
    externalView.setState("p0", "instance0", "OFFLINE");

    ValidationResult result =
        rule.validate(contextWithExternalView(externalView, "CustomLifecycle", customDef));

    Assert.assertFalse(result.isFeasible());
    Assert.assertEquals(result.getViolations().get(0).getRuleId(),
        ResourceInUseGuardrailRule.RULE_ID);
  }

  @Test
  public void testUnresolvableStateModelFallsBackToDroppedOnly() {
    // If the resource's state model can't be resolved (no ideal state / state-model def read returns
    // null), the rule falls back to treating only DROPPED as drained: an OFFLINE replica then counts
    // as in use (conservative fail-safe that keeps a possibly-still-serving resource blocked).
    HelixDataAccessor dataAccessor = mock(HelixDataAccessor.class);
    when(dataAccessor.keyBuilder()).thenReturn(BUILDER);
    ExternalView externalView = new ExternalView(RESOURCE);
    externalView.setState("p0", "instance0", "OFFLINE");
    doReturn(externalView).when(dataAccessor)
        .getProperty(argThat(new PropertyKeyArgument(PropertyType.EXTERNALVIEW)));
    // IDEALSTATES / STATEMODELDEFS reads are left unstubbed => return null (model unresolvable).

    ValidationResult result = rule.validate(contextFor(dataAccessor));

    Assert.assertFalse(result.isFeasible());
    Assert.assertEquals(result.getViolations().get(0).getRuleId(),
        ResourceInUseGuardrailRule.RULE_ID);
  }

  @Test
  public void testExternalViewReadErrorFailsClosed() {
    // A transient read error must surface as a rejection (fail closed), not a silent "feasible".
    // The pipeline converts the thrown exception into a violation carrying the rule's id.
    HelixDataAccessor dataAccessor = mock(HelixDataAccessor.class);
    when(dataAccessor.keyBuilder()).thenReturn(BUILDER);
    doThrow(new RuntimeException("ZooKeeper read failed")).when(dataAccessor)
        .getProperty(argThat(new PropertyKeyArgument(PropertyType.EXTERNALVIEW)));
    GuardrailContext context = contextFor(dataAccessor);

    ValidationResult result = new GuardrailPipeline(rule).validate(context);

    Assert.assertFalse(result.isFeasible());
    Assert.assertEquals(result.getViolations().size(), 1);
    Assert.assertEquals(result.getViolations().get(0).getRuleId(),
        ResourceInUseGuardrailRule.RULE_ID);
  }

  private GuardrailContext contextFor(HelixDataAccessor dataAccessor) {
    return GuardrailContext.newBuilder(CLUSTER)
        .dataAccessor(dataAccessor)
        .resourceName(RESOURCE)
        .build();
  }

  private GuardrailContext contextWithExternalView(ExternalView externalView) {
    // Default to the built-in MasterSlave model (initial state OFFLINE), resolved via the ideal
    // state's state-model ref -- mirroring how the rule reads the model in production.
    return contextWithExternalView(externalView, "MasterSlave",
        BuiltInStateModelDefinitions.MasterSlave.getStateModelDefinition());
  }

  private GuardrailContext contextWithExternalView(ExternalView externalView,
      String stateModelDefRef, StateModelDefinition stateModelDef) {
    HelixDataAccessor dataAccessor = mock(HelixDataAccessor.class);
    when(dataAccessor.keyBuilder()).thenReturn(BUILDER);
    doReturn(externalView).when(dataAccessor)
        .getProperty(argThat(new PropertyKeyArgument(PropertyType.EXTERNALVIEW)));
    IdealState idealState = new IdealState(RESOURCE);
    idealState.setStateModelDefRef(stateModelDefRef);
    doReturn(idealState).when(dataAccessor)
        .getProperty(argThat(new PropertyKeyArgument(PropertyType.IDEALSTATES)));
    doReturn(stateModelDef).when(dataAccessor)
        .getProperty(argThat(new PropertyKeyArgument(PropertyType.STATEMODELDEFS)));
    return contextFor(dataAccessor);
  }

  private static class PropertyKeyArgument implements ArgumentMatcher<PropertyKey> {
    private final PropertyType propertyType;

    PropertyKeyArgument(PropertyType propertyType) {
      this.propertyType = propertyType;
    }

    @Override
    public boolean matches(PropertyKey propertyKey) {
      return this.propertyType == propertyKey.getType();
    }
  }
}
