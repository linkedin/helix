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
import org.apache.helix.model.ExternalView;
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
 * than {@code DROPPED} mean the resource is still in use (drop must be blocked), while a missing /
 * empty / all-{@code DROPPED} external view means it is not (drop may proceed).
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
    externalView.setState("p1", "instance2", HelixDefinedState.DROPPED.name()); // ignored

    ValidationResult result = rule.validate(contextWithExternalView(externalView));

    Assert.assertFalse(result.isFeasible());
    Assert.assertEquals(result.getViolations().size(), 1);
    Violation violation = result.getViolations().get(0);
    Assert.assertEquals(violation.getRuleId(), ResourceInUseGuardrailRule.RULE_ID);
    Assert.assertTrue(violation.getMessage().contains(RESOURCE));
    Assert.assertTrue(violation.getMessage().contains("in use"));
  }

  @Test
  public void testOfflineReplicaStillCountsAsInUse() {
    // OFFLINE/ERROR are not DROPPED, so a resource mid-transition is conservatively treated as in
    // use and the drop is blocked.
    ExternalView externalView = new ExternalView(RESOURCE);
    externalView.setState("p0", "instance0", "OFFLINE");

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
    HelixDataAccessor dataAccessor = mock(HelixDataAccessor.class);
    when(dataAccessor.keyBuilder()).thenReturn(BUILDER);
    doReturn(externalView).when(dataAccessor)
        .getProperty(argThat(new PropertyKeyArgument(PropertyType.EXTERNALVIEW)));
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
