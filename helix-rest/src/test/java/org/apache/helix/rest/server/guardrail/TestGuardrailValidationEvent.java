package org.apache.helix.rest.server.guardrail;

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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import org.apache.helix.guardrail.ValidationResult;
import org.apache.helix.guardrail.Violation;
import org.apache.helix.rest.server.guardrail.GuardrailValidationEvent.Outcome;
import org.apache.helix.rest.server.guardrail.GuardrailValidationEvent.RuleEvaluation;
import org.testng.Assert;
import org.testng.annotations.Test;

/**
 * Unit tests for {@link GuardrailValidationEvent}, the shared per-rule outcome and latency
 * derivation consumed by both the JMX guard rail counters ({@code GuardrailMetrics}) and
 * {@link GuardrailValidationListener}s. The derivation runs in the constructor and is read back via
 * {@link GuardrailValidationEvent#getRuleEvaluations()}, so it needs no REST server or JAX-RS
 * context.
 */
public class TestGuardrailValidationEvent {

  private static final String NS = "TestNamespace";

  /** An infeasible verdict carrying one violation per supplied rule id (ids may repeat). */
  private static ValidationResult infeasible(String... ruleIds) {
    List<Violation> violations = new ArrayList<>();
    for (String ruleId : ruleIds) {
      violations.add(Violation.newBuilder(ruleId).message("nope").build());
    }
    return ValidationResult.of(violations);
  }

  private static RuleEvaluation evaluationFor(GuardrailValidationEvent event, String ruleId) {
    for (RuleEvaluation evaluation : event.getRuleEvaluations()) {
      if (evaluation.getRuleId().equals(ruleId)) {
        return evaluation;
      }
    }
    return null;
  }

  @Test
  public void testFeasibleVerdictMarksEveryRulePassed() {
    GuardrailValidationEvent event = new GuardrailValidationEvent(NS, Arrays.asList("A", "B", "C"),
        Collections.emptyMap(), ValidationResult.feasible(), false, false);

    Assert.assertEquals(event.getRuleEvaluations().size(), 3);
    Assert.assertEquals(evaluationFor(event, "A").getOutcome(), Outcome.PASSED);
    Assert.assertEquals(evaluationFor(event, "B").getOutcome(), Outcome.PASSED);
    Assert.assertEquals(evaluationFor(event, "C").getOutcome(), Outcome.PASSED);
    Assert.assertTrue(event.isFeasible());
    // The namespace is carried through for listeners to tag their metrics.
    Assert.assertEquals(event.getNamespace(), NS);
  }

  @Test
  public void testBlockedOnEnforceInfeasibleNotForced() {
    GuardrailValidationEvent event = new GuardrailValidationEvent(NS, Arrays.asList("A", "B"),
        Collections.emptyMap(), infeasible("B"), false, false);

    // The violating rule is blocked; the rule that did not flag the mutation still passed.
    Assert.assertEquals(evaluationFor(event, "B").getOutcome(), Outcome.BLOCKED);
    Assert.assertEquals(evaluationFor(event, "A").getOutcome(), Outcome.PASSED);
    Assert.assertFalse(event.isFeasible());
  }

  @Test
  public void testForcedWhenForceTrue() {
    GuardrailValidationEvent event = new GuardrailValidationEvent(NS,
        Collections.singletonList("A"), Collections.emptyMap(), infeasible("A"), true, false);

    Assert.assertEquals(evaluationFor(event, "A").getOutcome(), Outcome.FORCED);
  }

  @Test
  public void testDryRunInfeasibleWhenDryRunTrue() {
    GuardrailValidationEvent event = new GuardrailValidationEvent(NS,
        Collections.singletonList("A"), Collections.emptyMap(), infeasible("A"), false, true);

    Assert.assertEquals(evaluationFor(event, "A").getOutcome(), Outcome.DRY_RUN_INFEASIBLE);
  }

  @Test
  public void testDryRunTakesPrecedenceOverForce() {
    // dryRun=true wins even when force=true, mirroring AbstractHelixResource#preflight.
    GuardrailValidationEvent event = new GuardrailValidationEvent(NS,
        Collections.singletonList("A"), Collections.emptyMap(), infeasible("A"), true, true);

    Assert.assertEquals(evaluationFor(event, "A").getOutcome(), Outcome.DRY_RUN_INFEASIBLE);
  }

  @Test
  public void testDryRunFeasibleMarksRulePassedAsDryRunPassed() {
    // A dryRun probe that raises no violation must land in DRY_RUN_PASSED, never the real PASSED
    // count, so simulations can't inflate a rule's success number or skew its block rate.
    GuardrailValidationEvent event = new GuardrailValidationEvent(NS, Arrays.asList("A", "B"),
        Collections.emptyMap(), ValidationResult.feasible(), false, true);

    Assert.assertEquals(evaluationFor(event, "A").getOutcome(), Outcome.DRY_RUN_PASSED);
    Assert.assertEquals(evaluationFor(event, "B").getOutcome(), Outcome.DRY_RUN_PASSED);
    Assert.assertTrue(event.isFeasible());
    Assert.assertTrue(event.isDryRun());
  }

  @Test
  public void testPerRuleDurationCapturedAndDefaultedToZero() {
    Map<String, Long> durations = new HashMap<>();
    durations.put("A", TimeUnit.MILLISECONDS.toNanos(5));
    // "B" is evaluated but carries no recorded duration.
    GuardrailValidationEvent event = new GuardrailValidationEvent(NS, Arrays.asList("A", "B"),
        durations, ValidationResult.feasible(), false, false);

    Assert.assertEquals(evaluationFor(event, "A").getDurationNanos(),
        TimeUnit.MILLISECONDS.toNanos(5));
    Assert.assertEquals(evaluationFor(event, "B").getDurationNanos(), 0L);
  }

  @Test
  public void testNullDurationMapDefaultsToZero() {
    GuardrailValidationEvent event = new GuardrailValidationEvent(NS,
        Collections.singletonList("A"), null, ValidationResult.feasible(), false, false);

    Assert.assertEquals(evaluationFor(event, "A").getDurationNanos(), 0L);
  }

  @Test
  public void testDuplicateEvaluatedRuleIdsDedupedToSingleEvaluation() {
    GuardrailValidationEvent event = new GuardrailValidationEvent(NS, Arrays.asList("A", "A"),
        Collections.emptyMap(), ValidationResult.feasible(), false, false);

    Assert.assertEquals(event.getRuleEvaluations().size(), 1);
    Assert.assertEquals(evaluationFor(event, "A").getOutcome(), Outcome.PASSED);
  }

  @Test
  public void testMultipleViolationsFromSameRuleProduceOneEvaluation() {
    // The same rule reports two violations (e.g. two partitions) but blocked the write once.
    GuardrailValidationEvent event = new GuardrailValidationEvent(NS,
        Collections.singletonList("A"), Collections.emptyMap(), infeasible("A", "A"), false, false);

    Assert.assertEquals(event.getRuleEvaluations().size(), 1);
    Assert.assertEquals(evaluationFor(event, "A").getOutcome(), Outcome.BLOCKED);
  }

  @Test
  public void testEvaluationOrderFollowsEvaluatedRuleIds() {
    GuardrailValidationEvent event = new GuardrailValidationEvent(NS, Arrays.asList("C", "A", "B"),
        Collections.emptyMap(), ValidationResult.feasible(), false, false);

    List<RuleEvaluation> evaluations = event.getRuleEvaluations();
    Assert.assertEquals(evaluations.get(0).getRuleId(), "C");
    Assert.assertEquals(evaluations.get(1).getRuleId(), "A");
    Assert.assertEquals(evaluations.get(2).getRuleId(), "B");
  }

  @Test(expectedExceptions = UnsupportedOperationException.class)
  public void testRuleEvaluationsAreUnmodifiable() {
    // The event is handed to concurrently-running listeners, so its view must be immutable.
    GuardrailValidationEvent event = new GuardrailValidationEvent(NS,
        Collections.singletonList("A"), Collections.emptyMap(), ValidationResult.feasible(), false,
        false);
    event.getRuleEvaluations().clear();
  }
}
