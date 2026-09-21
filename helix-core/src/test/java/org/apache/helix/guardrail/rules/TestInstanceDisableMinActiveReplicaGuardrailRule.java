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

import java.util.Set;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.helix.HelixDataAccessor;
import org.apache.helix.guardrail.GuardrailContext;
import org.apache.helix.guardrail.MinActiveReplicaChecker;
import org.apache.helix.guardrail.ValidationResult;
import org.apache.helix.guardrail.Violation;
import org.apache.helix.util.MinActiveReplicaCheckResult;
import org.testng.Assert;
import org.testng.annotations.Test;

import static org.mockito.Mockito.mock;

/**
 * Unit tests for {@link InstanceDisableMinActiveReplicaGuardrailRule}. The min-active what-if is
 * supplied through a stubbed {@link MinActiveReplicaChecker} that returns a controlled
 * {@link MinActiveReplicaCheckResult}, so the rule's short-circuit and violation-building logic is
 * exercised with no ZooKeeper and no ExternalView reads.
 */
public class TestInstanceDisableMinActiveReplicaGuardrailRule {
  private static final String CLUSTER = "testCluster";
  private static final String INSTANCE = "instance0";
  private static final String RESOURCE = "testResource";
  private static final String PARTITION = "testResource_3";

  // A checker that must never be invoked: any call fails the test. Used to prove the rule
  // short-circuits before running the (relatively expensive) min-active what-if.
  private static final MinActiveReplicaChecker CHECKER_MUST_NOT_RUN =
      (instanceName, toBeStoppedInstances) -> {
        throw new AssertionError("min-active what-if must not run on this path");
      };

  private final InstanceDisableMinActiveReplicaGuardrailRule rule =
      new InstanceDisableMinActiveReplicaGuardrailRule();

  @Test
  public void testRuleIdIsStable() {
    Assert.assertEquals(rule.getId(), "MIN_ACTIVE_REPLICA_ON_INSTANCE_DISABLE");
    Assert.assertEquals(rule.getId(), InstanceDisableMinActiveReplicaGuardrailRule.RULE_ID);
  }

  @Test
  public void testNullInstanceNameIsFeasible() {
    // No target instance -> nothing to certify, and the checker must not run.
    GuardrailContext context = GuardrailContext.newBuilder(CLUSTER)
        .dataAccessor(mock(HelixDataAccessor.class))
        .minActiveReplicaChecker(CHECKER_MUST_NOT_RUN)
        .build();

    ValidationResult result = rule.validate(context);

    Assert.assertTrue(result.isFeasible());
    Assert.assertTrue(result.getViolations().isEmpty());
  }

  @Test
  public void testNullCheckerIsFeasible() {
    // No checker seam wired -> certify feasible rather than block a disable on a wiring gap.
    GuardrailContext context = GuardrailContext.newBuilder(CLUSTER)
        .dataAccessor(mock(HelixDataAccessor.class))
        .instanceName(INSTANCE)
        .build();

    ValidationResult result = rule.validate(context);

    Assert.assertTrue(result.isFeasible());
  }

  @Test
  public void testMinActiveSatisfiedIsFeasible() {
    GuardrailContext context = GuardrailContext.newBuilder(CLUSTER)
        .dataAccessor(mock(HelixDataAccessor.class))
        .instanceName(INSTANCE)
        .minActiveReplicaChecker(
            (instanceName, toBeStoppedInstances) -> MinActiveReplicaCheckResult.passed())
        .build();

    ValidationResult result = rule.validate(context);

    Assert.assertTrue(result.isFeasible());
    Assert.assertTrue(result.getViolations().isEmpty());
  }

  @Test
  public void testMinActiveViolationIsInfeasible() {
    GuardrailContext context = GuardrailContext.newBuilder(CLUSTER)
        .dataAccessor(mock(HelixDataAccessor.class))
        .instanceName(INSTANCE)
        .minActiveReplicaChecker((instanceName, toBeStoppedInstances) -> MinActiveReplicaCheckResult
            .failed(RESOURCE, PARTITION, 1, 2))
        .build();

    ValidationResult result = rule.validate(context);

    Assert.assertFalse(result.isFeasible());
    Assert.assertEquals(result.getViolations().size(), 1);
    Violation violation = result.getViolations().get(0);
    Assert.assertEquals(violation.getRuleId(), InstanceDisableMinActiveReplicaGuardrailRule.RULE_ID);
    Assert.assertEquals(violation.getResourceName(), RESOURCE);
    Assert.assertEquals(violation.getPartitionName(), PARTITION);
    Assert.assertTrue(violation.getMessage().contains(INSTANCE));
    Assert.assertTrue(violation.getMessage().contains(PARTITION));
    Assert.assertTrue(violation.getMessage().contains("force=true"));
  }

  @Test
  public void testCheckerInvokedWithTargetInstanceAndEmptyStoppedSet() {
    AtomicReference<String> capturedInstance = new AtomicReference<>();
    AtomicReference<Set<String>> capturedStopped = new AtomicReference<>();
    GuardrailContext context = GuardrailContext.newBuilder(CLUSTER)
        .dataAccessor(mock(HelixDataAccessor.class))
        .instanceName(INSTANCE)
        .minActiveReplicaChecker((instanceName, toBeStoppedInstances) -> {
          capturedInstance.set(instanceName);
          capturedStopped.set(toBeStoppedInstances);
          return MinActiveReplicaCheckResult.passed();
        })
        .build();

    rule.validate(context);

    Assert.assertEquals(capturedInstance.get(), INSTANCE);
    Assert.assertNotNull(capturedStopped.get());
    Assert.assertTrue(capturedStopped.get().isEmpty(),
        "single-instance disable must presume no other instance stopped");
  }
}
