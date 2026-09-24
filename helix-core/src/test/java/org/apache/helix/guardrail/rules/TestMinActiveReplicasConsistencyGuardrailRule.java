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
import org.apache.helix.model.ResourceConfig;
import org.testng.Assert;
import org.testng.annotations.Test;

import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * This is a delta guard: it fires only when an ideal-state edit introduces or worsens a
 * MIN_ACTIVE_REPLICAS &gt; REPLICAS inconsistency, and grandfathers a pre-existing one. The tests
 * mock the current (pre-write) ideal state and resource config that the rule reads back from the
 * metadata store, and pass the proposed (merged, post-write) ideal state through the context.
 */
public class TestMinActiveReplicasConsistencyGuardrailRule {
  private static final String CLUSTER = "testCluster";
  private static final String RESOURCE = "testResource";
  private static final PropertyKey.Builder BUILDER = new PropertyKey.Builder(CLUSTER);
  private final MinActiveReplicasConsistencyGuardrailRule rule =
      new MinActiveReplicasConsistencyGuardrailRule();

  @Test
  public void testNullProposedIdealStateIsFeasible() {
    // No proposed ideal state means this is not an ideal-state mutation; nothing to certify.
    GuardrailContext context = GuardrailContext.newBuilder(CLUSTER)
        .dataAccessor(mock(HelixDataAccessor.class))
        .build();
    Assert.assertTrue(rule.validate(context).isFeasible());
  }

  @Test
  public void testAnyLiveInstanceReplicasIsFeasible() {
    // ANY_LIVEINSTANCE has no fixed replica count, so the comparison is a runtime concern even when
    // MIN_ACTIVE_REPLICAS is large; the rule defers rather than guessing.
    ValidationResult result = rule.validate(
        context(idealState("ANY_LIVEINSTANCE", null), null, idealState("ANY_LIVEINSTANCE", 5)));
    Assert.assertTrue(result.isFeasible());
  }

  @Test
  public void testMinActiveReplicasUnsetIsFeasible() {
    // Unset MIN_ACTIVE_REPLICAS defaults to the replica count, so it can never exceed it.
    ValidationResult result =
        rule.validate(context(idealState("3", null), null, idealState("3", null)));
    Assert.assertTrue(result.isFeasible());
    Assert.assertTrue(result.getViolations().isEmpty());
  }

  @Test
  public void testMinActiveEqualsReplicasIsFeasible() {
    ValidationResult result =
        rule.validate(context(idealState("3", 2), null, idealState("3", 3)));
    Assert.assertTrue(result.isFeasible());
  }

  @Test
  public void testNewlyIntroducedViolationIsInfeasible() {
    // Consistent before (min 2 <= rep 3); the edit raises MIN_ACTIVE_REPLICAS to 5 above REPLICAS,
    // newly introducing the inconsistency, so it must be blocked.
    ValidationResult result =
        rule.validate(context(idealState("3", 2), null, idealState("3", 5)));
    Assert.assertFalse(result.isFeasible());
    Assert.assertEquals(result.getViolations().size(), 1);
    Violation violation = result.getViolations().get(0);
    Assert.assertEquals(violation.getRuleId(),
        MinActiveReplicasConsistencyGuardrailRule.RULE_ID);
    Assert.assertEquals(violation.getResourceName(), RESOURCE);
    Assert.assertTrue(violation.getMessage().contains("5"));
    Assert.assertTrue(violation.getMessage().contains("3"));
  }

  @Test
  public void testPreExistingViolationLeftUnchangedIsFeasible() {
    // Already inconsistent before the edit (min 3 > rep 2) and left exactly as inconsistent by the
    // edit; the gap does not grow, so the pre-existing state is grandfathered rather than blocked.
    // This mirrors the standard helix-rest test fixture, which creates resources this way.
    ValidationResult result =
        rule.validate(context(idealState("2", 3), null, idealState("2", 3)));
    Assert.assertTrue(result.isFeasible());
  }

  @Test
  public void testWorsenedViolationIsInfeasible() {
    // Already inconsistent (min 3 > rep 2, gap 1); the edit lowers REPLICAS to 1 (gap 2), deepening
    // the inconsistency, so it is blocked even though the resource was not consistent to begin with.
    ValidationResult result =
        rule.validate(context(idealState("2", 3), null, idealState("1", 3)));
    Assert.assertFalse(result.isFeasible());
    Violation violation = result.getViolations().get(0);
    Assert.assertEquals(violation.getRuleId(),
        MinActiveReplicasConsistencyGuardrailRule.RULE_ID);
    Assert.assertTrue(violation.getMessage().contains("3"));
    Assert.assertTrue(violation.getMessage().contains("1"));
  }

  @Test
  public void testShrinkingExistingGapIsFeasible() {
    // Already inconsistent (min 5 > rep 2, gap 3); the edit raises REPLICAS to 3 (gap 2), which
    // improves the situation, so it is allowed even though the result is still inconsistent.
    ValidationResult result =
        rule.validate(context(idealState("2", 5), null, idealState("3", 5)));
    Assert.assertTrue(result.isFeasible());
  }

  @Test
  public void testResourceConfigMinActivePrecedenceNewlyIntroducedIsInfeasible() {
    // The resource config MIN_ACTIVE_REPLICAS (5) takes precedence over the ideal state's, exactly
    // as the rebalancer resolves it. It was consistent while REPLICAS was 5; lowering REPLICAS to 3
    // newly introduces the inconsistency (effective min 5 > rep 3), so the edit is blocked.
    ResourceConfig resourceConfig = resourceConfigWithMinActive(5);
    ValidationResult result = rule.validate(
        context(idealState("5", null), resourceConfig, idealState("3", null)));
    Assert.assertFalse(result.isFeasible());
    Violation violation = result.getViolations().get(0);
    Assert.assertEquals(violation.getRuleId(),
        MinActiveReplicasConsistencyGuardrailRule.RULE_ID);
    Assert.assertTrue(violation.getMessage().contains("5"));
    Assert.assertTrue(violation.getMessage().contains("3"));
  }

  @Test
  public void testResourceConfigMinActiveWithinReplicasIsFeasible() {
    // Resource config MIN_ACTIVE_REPLICAS (3) is within REPLICAS (5): feasible, and the resource
    // config precedence does not produce a false positive.
    ResourceConfig resourceConfig = resourceConfigWithMinActive(3);
    ValidationResult result = rule.validate(
        context(idealState("5", null), resourceConfig, idealState("5", null)));
    Assert.assertTrue(result.isFeasible());
  }

  private GuardrailContext context(IdealState existingIdealState, ResourceConfig resourceConfig,
      IdealState proposedIdealState) {
    return GuardrailContext.newBuilder(CLUSTER)
        .dataAccessor(accessor(existingIdealState, resourceConfig))
        .proposedIdealState(proposedIdealState)
        .build();
  }

  private static HelixDataAccessor accessor(IdealState existingIdealState,
      ResourceConfig resourceConfig) {
    HelixDataAccessor dataAccessor = mock(HelixDataAccessor.class);
    when(dataAccessor.keyBuilder()).thenReturn(BUILDER);
    doReturn(existingIdealState).when(dataAccessor).getProperty(BUILDER.idealStates(RESOURCE));
    doReturn(resourceConfig).when(dataAccessor).getProperty(BUILDER.resourceConfig(RESOURCE));
    return dataAccessor;
  }

  private static IdealState idealState(String replicas, Integer minActiveReplicas) {
    IdealState idealState = new IdealState(RESOURCE);
    idealState.setRebalanceMode(IdealState.RebalanceMode.FULL_AUTO);
    if (replicas != null) {
      idealState.setReplicas(replicas);
    }
    if (minActiveReplicas != null) {
      idealState.setMinActiveReplicas(minActiveReplicas);
    }
    return idealState;
  }

  private static ResourceConfig resourceConfigWithMinActive(int minActiveReplicas) {
    ResourceConfig resourceConfig = new ResourceConfig(RESOURCE);
    resourceConfig.getRecord().setIntField(
        ResourceConfig.ResourceConfigProperty.MIN_ACTIVE_REPLICAS.name(), minActiveReplicas);
    return resourceConfig;
  }
}
