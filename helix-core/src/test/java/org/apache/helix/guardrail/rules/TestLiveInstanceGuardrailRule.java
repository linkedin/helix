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
import org.apache.helix.PropertyType;
import org.apache.helix.guardrail.GuardrailContext;
import org.apache.helix.guardrail.ValidationResult;
import org.apache.helix.guardrail.Violation;
import org.apache.helix.model.LiveInstance;
import org.mockito.ArgumentMatcher;
import org.testng.Assert;
import org.testng.annotations.Test;

import static org.mockito.Mockito.argThat;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Unit tests for {@link LiveInstanceGuardrailRule}. The rule reads a single {@code LIVEINSTANCES}
 * znode for the target instance through a mocked {@link HelixDataAccessor}: a present
 * {@link LiveInstance} means the participant is still connected (drop must be blocked), while a
 * {@code null} means it is offline (drop may proceed).
 */
public class TestLiveInstanceGuardrailRule {
  private static final String CLUSTER = "testCluster";
  private static final String INSTANCE = "instance0";
  private static final PropertyKey.Builder BUILDER = new PropertyKey.Builder(CLUSTER);

  private final LiveInstanceGuardrailRule rule = new LiveInstanceGuardrailRule();

  @Test
  public void testNullInstanceIsFeasible() {
    GuardrailContext context = GuardrailContext.newBuilder(CLUSTER)
        .dataAccessor(mock(HelixDataAccessor.class))
        .build();
    ValidationResult result = rule.validate(context);
    Assert.assertTrue(result.isFeasible());
  }

  @Test
  public void testOfflineInstanceIsFeasible() {
    // No LIVEINSTANCES znode: getProperty(...) returns null (the mock default), so the drop is safe.
    HelixDataAccessor dataAccessor = mock(HelixDataAccessor.class);
    when(dataAccessor.keyBuilder()).thenReturn(BUILDER);
    GuardrailContext context = GuardrailContext.newBuilder(CLUSTER)
        .dataAccessor(dataAccessor)
        .instanceName(INSTANCE)
        .build();

    ValidationResult result = rule.validate(context);

    Assert.assertTrue(result.isFeasible());
    Assert.assertTrue(result.getViolations().isEmpty());
  }

  @Test
  public void testLiveInstanceIsInfeasible() {
    HelixDataAccessor dataAccessor = mock(HelixDataAccessor.class);
    when(dataAccessor.keyBuilder()).thenReturn(BUILDER);
    LiveInstance liveInstance = mock(LiveInstance.class);
    when(liveInstance.getEphemeralOwner()).thenReturn("session-abc");
    doReturn(liveInstance).when(dataAccessor)
        .getProperty(argThat(new PropertyKeyArgument(PropertyType.LIVEINSTANCES)));
    GuardrailContext context = GuardrailContext.newBuilder(CLUSTER)
        .dataAccessor(dataAccessor)
        .instanceName(INSTANCE)
        .build();

    ValidationResult result = rule.validate(context);

    Assert.assertFalse(result.isFeasible());
    Assert.assertEquals(result.getViolations().size(), 1);
    Violation violation = result.getViolations().get(0);
    Assert.assertEquals(violation.getRuleId(), LiveInstanceGuardrailRule.RULE_ID);
    Assert.assertTrue(violation.getMessage().contains(INSTANCE));
    Assert.assertTrue(violation.getMessage().contains("still live"));
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
