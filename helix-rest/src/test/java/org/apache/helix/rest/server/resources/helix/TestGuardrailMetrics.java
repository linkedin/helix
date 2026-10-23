package org.apache.helix.rest.server.resources.helix;

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

import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.Timer;
import org.apache.helix.guardrail.ValidationResult;
import org.apache.helix.guardrail.Violation;
import org.testng.Assert;
import org.testng.annotations.Test;

/**
 * Unit tests for {@link GuardrailMetrics}, the per-rule guard rail metric emitter. The logic is
 * exercised directly against a plain {@link MetricRegistry} so it needs no REST server, ZooKeeper,
 * or JAX-RS context.
 */
public class TestGuardrailMetrics {

  private static long count(MetricRegistry metrics, String ruleId, String outcome) {
    return metrics.counter(GuardrailMetrics.metricName(ruleId, outcome)).getCount();
  }

  private static Timer evaluatedTimer(MetricRegistry metrics, String ruleId) {
    return metrics.timer(GuardrailMetrics.metricName(ruleId, GuardrailMetrics.EVALUATED));
  }

  private static long evaluatedCount(MetricRegistry metrics, String ruleId) {
    return evaluatedTimer(metrics, ruleId).getCount();
  }

  /** The sum of a rule's terminal outcome counters, which must equal its evaluated count. */
  private static long outcomeSum(MetricRegistry metrics, String ruleId) {
    return count(metrics, ruleId, GuardrailMetrics.PASSED)
        + count(metrics, ruleId, GuardrailMetrics.BLOCKED)
        + count(metrics, ruleId, GuardrailMetrics.FORCED)
        + count(metrics, ruleId, GuardrailMetrics.DRY_RUN_INFEASIBLE);
  }

  /** An infeasible verdict carrying one violation per supplied rule id (ids may repeat). */
  private static ValidationResult infeasible(String... ruleIds) {
    List<Violation> violations = new ArrayList<>();
    for (String ruleId : ruleIds) {
      violations.add(Violation.newBuilder(ruleId).message("nope").build());
    }
    return ValidationResult.of(violations);
  }

  @Test
  public void testEvaluatedTimedAndPassedForFeasibleVerdict() {
    MetricRegistry metrics = new MetricRegistry();
    GuardrailMetrics.record(metrics, Arrays.asList("A", "B", "C"), Collections.emptyMap(),
        ValidationResult.feasible(), false, false);

    // Every rule that ran is timed, and a feasible verdict tallies each as passed.
    Assert.assertEquals(evaluatedCount(metrics, "A"), 1);
    Assert.assertEquals(evaluatedCount(metrics, "B"), 1);
    Assert.assertEquals(evaluatedCount(metrics, "C"), 1);
    Assert.assertEquals(count(metrics, "A", GuardrailMetrics.PASSED), 1);
    Assert.assertEquals(count(metrics, "B", GuardrailMetrics.PASSED), 1);
    Assert.assertEquals(count(metrics, "C", GuardrailMetrics.PASSED), 1);
    // A feasible verdict raises no failure counter.
    Assert.assertEquals(count(metrics, "A", GuardrailMetrics.BLOCKED), 0);
    Assert.assertEquals(count(metrics, "A", GuardrailMetrics.FORCED), 0);
    Assert.assertEquals(count(metrics, "A", GuardrailMetrics.DRY_RUN_INFEASIBLE), 0);
  }

  @Test
  public void testEvaluatedTimerRecordsPerRuleLatency() {
    MetricRegistry metrics = new MetricRegistry();
    Map<String, Long> durations = new HashMap<>();
    durations.put("A", TimeUnit.MILLISECONDS.toNanos(5));
    durations.put("B", TimeUnit.MILLISECONDS.toNanos(20));
    // "C" is evaluated but carries no recorded duration.
    GuardrailMetrics.record(metrics, Arrays.asList("A", "B", "C"), durations,
        ValidationResult.feasible(), false, false);

    Assert.assertEquals(evaluatedTimer(metrics, "A").getSnapshot().getMax(),
        TimeUnit.MILLISECONDS.toNanos(5));
    Assert.assertEquals(evaluatedTimer(metrics, "B").getSnapshot().getMax(),
        TimeUnit.MILLISECONDS.toNanos(20));
    // A rule with no recorded duration is timed as zero but still counted.
    Assert.assertEquals(evaluatedTimer(metrics, "C").getSnapshot().getMax(), 0);
    Assert.assertEquals(evaluatedCount(metrics, "C"), 1);
  }

  @Test
  public void testBlockedOnEnforceInfeasibleNotForced() {
    MetricRegistry metrics = new MetricRegistry();
    GuardrailMetrics.record(metrics, Arrays.asList("A", "B"), Collections.emptyMap(),
        infeasible("B"), false, false);

    Assert.assertEquals(count(metrics, "B", GuardrailMetrics.BLOCKED), 1);
    Assert.assertEquals(count(metrics, "B", GuardrailMetrics.FORCED), 0);
    Assert.assertEquals(count(metrics, "B", GuardrailMetrics.DRY_RUN_INFEASIBLE), 0);
    Assert.assertEquals(count(metrics, "B", GuardrailMetrics.PASSED), 0);
    // The rule that did not flag the mutation passed; neither rule is double-counted.
    Assert.assertEquals(count(metrics, "A", GuardrailMetrics.PASSED), 1);
    Assert.assertEquals(count(metrics, "A", GuardrailMetrics.BLOCKED), 0);
    // Both rules still timed as evaluated.
    Assert.assertEquals(evaluatedCount(metrics, "A"), 1);
    Assert.assertEquals(evaluatedCount(metrics, "B"), 1);
  }

  @Test
  public void testForcedWhenForceTrue() {
    MetricRegistry metrics = new MetricRegistry();
    GuardrailMetrics.record(metrics, Collections.singletonList("A"), Collections.emptyMap(),
        infeasible("A"), true, false);

    Assert.assertEquals(count(metrics, "A", GuardrailMetrics.FORCED), 1);
    Assert.assertEquals(count(metrics, "A", GuardrailMetrics.BLOCKED), 0);
    Assert.assertEquals(count(metrics, "A", GuardrailMetrics.DRY_RUN_INFEASIBLE), 0);
    Assert.assertEquals(count(metrics, "A", GuardrailMetrics.PASSED), 0);
  }

  @Test
  public void testDryRunInfeasibleWhenDryRunTrue() {
    MetricRegistry metrics = new MetricRegistry();
    GuardrailMetrics.record(metrics, Collections.singletonList("A"), Collections.emptyMap(),
        infeasible("A"), false, true);

    Assert.assertEquals(count(metrics, "A", GuardrailMetrics.DRY_RUN_INFEASIBLE), 1);
    Assert.assertEquals(count(metrics, "A", GuardrailMetrics.BLOCKED), 0);
    Assert.assertEquals(count(metrics, "A", GuardrailMetrics.FORCED), 0);
    Assert.assertEquals(count(metrics, "A", GuardrailMetrics.PASSED), 0);
  }

  @Test
  public void testDryRunTakesPrecedenceOverForce() {
    MetricRegistry metrics = new MetricRegistry();
    // dryRun=true wins even when force=true, mirroring AbstractHelixResource#preflight.
    GuardrailMetrics.record(metrics, Collections.singletonList("A"), Collections.emptyMap(),
        infeasible("A"), true, true);

    Assert.assertEquals(count(metrics, "A", GuardrailMetrics.DRY_RUN_INFEASIBLE), 1);
    Assert.assertEquals(count(metrics, "A", GuardrailMetrics.BLOCKED), 0);
    Assert.assertEquals(count(metrics, "A", GuardrailMetrics.FORCED), 0);
  }

  @Test
  public void testMultipleViolationsFromSameRuleCountedOncePerOutcome() {
    MetricRegistry metrics = new MetricRegistry();
    // The same rule reports two violations (e.g. two partitions) but blocked the write once.
    GuardrailMetrics.record(metrics, Collections.singletonList("A"), Collections.emptyMap(),
        infeasible("A", "A"), false, false);

    Assert.assertEquals(count(metrics, "A", GuardrailMetrics.BLOCKED), 1);
    Assert.assertEquals(count(metrics, "A", GuardrailMetrics.PASSED), 0);
    Assert.assertEquals(evaluatedCount(metrics, "A"), 1);
  }

  @Test
  public void testDistinctViolatingRulesEachCounted() {
    MetricRegistry metrics = new MetricRegistry();
    GuardrailMetrics.record(metrics, Arrays.asList("A", "B"), Collections.emptyMap(),
        infeasible("A", "B"), false, false);

    Assert.assertEquals(count(metrics, "A", GuardrailMetrics.BLOCKED), 1);
    Assert.assertEquals(count(metrics, "B", GuardrailMetrics.BLOCKED), 1);
    Assert.assertEquals(count(metrics, "A", GuardrailMetrics.PASSED), 0);
    Assert.assertEquals(count(metrics, "B", GuardrailMetrics.PASSED), 0);
  }

  @Test
  public void testCountersAccumulateAcrossCalls() {
    MetricRegistry metrics = new MetricRegistry();
    GuardrailMetrics.record(metrics, Collections.singletonList("A"), Collections.emptyMap(),
        infeasible("A"), false, false);
    GuardrailMetrics.record(metrics, Collections.singletonList("A"), Collections.emptyMap(),
        infeasible("A"), false, false);

    Assert.assertEquals(evaluatedCount(metrics, "A"), 2);
    Assert.assertEquals(count(metrics, "A", GuardrailMetrics.BLOCKED), 2);
  }

  @Test
  public void testEvaluatedCountEqualsSumOfOutcomesPerRule() {
    MetricRegistry metrics = new MetricRegistry();
    // Rule A passes, is blocked, then forced across three calls; rule B always passes.
    GuardrailMetrics.record(metrics, Arrays.asList("A", "B"), Collections.emptyMap(),
        ValidationResult.feasible(), false, false);
    GuardrailMetrics.record(metrics, Arrays.asList("A", "B"), Collections.emptyMap(),
        infeasible("A"), false, false);
    GuardrailMetrics.record(metrics, Arrays.asList("A", "B"), Collections.emptyMap(),
        infeasible("A"), true, false);

    // The evaluated timer count is exactly the sum of the terminal outcome counters per rule.
    Assert.assertEquals(evaluatedCount(metrics, "A"), 3);
    Assert.assertEquals(outcomeSum(metrics, "A"), 3);
    Assert.assertEquals(count(metrics, "A", GuardrailMetrics.PASSED), 1);
    Assert.assertEquals(count(metrics, "A", GuardrailMetrics.BLOCKED), 1);
    Assert.assertEquals(count(metrics, "A", GuardrailMetrics.FORCED), 1);

    Assert.assertEquals(evaluatedCount(metrics, "B"), 3);
    Assert.assertEquals(outcomeSum(metrics, "B"), 3);
    Assert.assertEquals(count(metrics, "B", GuardrailMetrics.PASSED), 3);
  }

  @Test
  public void testMetricNameFormat() {
    Assert.assertEquals(GuardrailMetrics.metricName("MY_RULE", GuardrailMetrics.BLOCKED),
        "org.apache.helix.rest.guardrail.MY_RULE.blocked");
  }
}
