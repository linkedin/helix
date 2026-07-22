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

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import org.apache.helix.HelixDataAccessor;
import org.apache.helix.PropertyKey;
import org.apache.helix.PropertyType;
import org.apache.helix.guardrail.GuardrailContext;
import org.apache.helix.guardrail.ValidationResult;
import org.apache.helix.guardrail.Violation;
import org.apache.helix.model.ExternalView;
import org.apache.helix.model.IdealState;
import org.apache.helix.model.StateModelDefinition;
import org.mockito.ArgumentMatcher;
import org.testng.Assert;
import org.testng.annotations.Test;

import static org.mockito.Mockito.argThat;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Unit tests for {@link MinActiveReplicaGuardrailRule}. The rule delegates to
 * {@code InstanceValidationUtil.siblingNodesActiveReplicaCheckWithDetails}, so the cluster state it
 * reads is supplied through a mocked {@link HelixDataAccessor} mirroring the pattern used by
 * {@code TestInstanceValidationUtil}.
 */
public class TestMinActiveReplicaGuardrailRule {
  private static final String CLUSTER = "testCluster";
  private static final String INSTANCE = "instance0";
  private static final String RESOURCE = "testResource";
  private static final String PARTITION = "testResource_0";
  private static final PropertyKey.Builder BUILDER = new PropertyKey.Builder(CLUSTER);

  private final MinActiveReplicaGuardrailRule rule = new MinActiveReplicaGuardrailRule();

  @Test
  public void testNullInstanceIsFeasible() {
    GuardrailContext context = GuardrailContext.newBuilder(CLUSTER)
        .dataAccessor(mock(HelixDataAccessor.class))
        .build();
    ValidationResult result = rule.validate(context);
    Assert.assertTrue(result.isFeasible());
  }

  @Test
  public void testSufficientReplicasIsFeasible() {
    HelixDataAccessor dataAccessor = mockAccessor(
        ImmutableMap.of(INSTANCE, "Master", "instance1", "Slave", "instance2", "Slave"), 2);
    GuardrailContext context = GuardrailContext.newBuilder(CLUSTER)
        .dataAccessor(dataAccessor)
        .instanceName(INSTANCE)
        .build();

    ValidationResult result = rule.validate(context);

    Assert.assertTrue(result.isFeasible());
    Assert.assertTrue(result.getViolations().isEmpty());
  }

  @Test
  public void testInsufficientReplicasIsInfeasible() {
    HelixDataAccessor dataAccessor = mockAccessor(
        ImmutableMap.of(INSTANCE, "Master", "instance1", "ERROR", "instance2", "ERROR"), 2);
    GuardrailContext context = GuardrailContext.newBuilder(CLUSTER)
        .dataAccessor(dataAccessor)
        .instanceName(INSTANCE)
        .build();

    ValidationResult result = rule.validate(context);

    Assert.assertFalse(result.isFeasible());
    Assert.assertEquals(result.getViolations().size(), 1);
    Violation violation = result.getViolations().get(0);
    Assert.assertEquals(violation.getRuleId(), MinActiveReplicaGuardrailRule.RULE_ID);
    Assert.assertEquals(violation.getResourceName(), RESOURCE);
    Assert.assertEquals(violation.getPartitionName(), PARTITION);
    Assert.assertTrue(violation.getMessage().contains(INSTANCE));
  }

  /**
   * A resource whose ExternalView has not been computed yet has no committed placement, so it is
   * not hosted on the instance and must be skipped rather than blocking the drop cluster-wide.
   */
  @Test
  public void testResourceWithoutExternalViewIsSkipped() {
    HelixDataAccessor dataAccessor = mock(HelixDataAccessor.class);
    when(dataAccessor.keyBuilder()).thenReturn(BUILDER);

    IdealState idealState = mock(IdealState.class);
    when(idealState.isEnabled()).thenReturn(true);
    when(idealState.isValid()).thenReturn(true);
    when(idealState.getStateModelDefRef()).thenReturn("MasterSlave");

    doReturn(ImmutableList.of(RESOURCE)).when(dataAccessor)
        .getChildNames(argThat(new PropertyKeyArgument(PropertyType.IDEALSTATES)));
    doReturn(idealState).when(dataAccessor)
        .getProperty(argThat(new PropertyKeyArgument(PropertyType.IDEALSTATES)));
    // No ExternalView computed yet: the rule must skip it, not fail closed for the whole cluster.
    doReturn(null).when(dataAccessor)
        .getProperty(argThat(new PropertyKeyArgument(PropertyType.EXTERNALVIEW)));

    GuardrailContext context = GuardrailContext.newBuilder(CLUSTER)
        .dataAccessor(dataAccessor)
        .instanceName(INSTANCE)
        .build();

    ValidationResult result = rule.validate(context);

    Assert.assertTrue(result.isFeasible());
    Assert.assertTrue(result.getViolations().isEmpty());
  }

  /**
   * Builds a mock {@link HelixDataAccessor} exposing a single MasterSlave resource with one
   * partition whose replica states / minActiveReplicas are supplied by the caller. OFFLINE, ERROR
   * and DROPPED are treated as unhealthy (via the default state-model initial state).
   */
  private HelixDataAccessor mockAccessor(java.util.Map<String, String> replicaStates,
      int minActiveReplicas) {
    HelixDataAccessor dataAccessor = mock(HelixDataAccessor.class);
    when(dataAccessor.keyBuilder()).thenReturn(BUILDER);

    IdealState idealState = mock(IdealState.class);
    when(idealState.isEnabled()).thenReturn(true);
    when(idealState.isValid()).thenReturn(true);
    when(idealState.getStateModelDefRef()).thenReturn("MasterSlave");

    ExternalView externalView = mock(ExternalView.class);
    when(externalView.getMinActiveReplicas()).thenReturn(minActiveReplicas);
    when(externalView.getStateModelDefRef()).thenReturn("MasterSlave");
    when(externalView.getPartitionSet()).thenReturn(ImmutableSet.of(PARTITION));
    when(externalView.getStateMap(PARTITION)).thenReturn(replicaStates);

    StateModelDefinition stateModelDefinition = mock(StateModelDefinition.class);
    when(stateModelDefinition.getInitialState()).thenReturn("OFFLINE");

    doReturn(ImmutableList.of(RESOURCE)).when(dataAccessor)
        .getChildNames(argThat(new PropertyKeyArgument(PropertyType.IDEALSTATES)));
    doReturn(idealState).when(dataAccessor)
        .getProperty(argThat(new PropertyKeyArgument(PropertyType.IDEALSTATES)));
    doReturn(externalView).when(dataAccessor)
        .getProperty(argThat(new PropertyKeyArgument(PropertyType.EXTERNALVIEW)));
    doReturn(stateModelDefinition).when(dataAccessor)
        .getProperty(argThat(new PropertyKeyArgument(PropertyType.STATEMODELDEFS)));
    return dataAccessor;
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
