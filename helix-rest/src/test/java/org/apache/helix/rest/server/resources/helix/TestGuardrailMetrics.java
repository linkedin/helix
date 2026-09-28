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
import java.util.List;

import com.codahale.metrics.MetricRegistry;
import org.apache.helix.guardrail.ValidationResult;
import org.apache.helix.guardrail.Violation;
import org.testng.Assert;
import org.testng.annotations.Test;

/**
 * Unit tests for {@link GuardrailMetrics}, the per-rule guard rail counter emitter. The logic is
 * exercised directly against a plain {@link MetricRegistry} so it needs no REST server, ZooKeeper,
 * or JAX-RS context.
 */
public class TestGuardrailMetrics {

  private static long count(MetricRegistry metrics, String ruleId, String outcome) {
    return metrics.counter(GuardrailMetrics.metricName(ruleId, outcome)).getCount();
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
  public void testEvaluatedCountedForEveryRuleRegardlessOfVerdict() {
    MetricRegistry metrics = new MetricRegistry();
    GuardrailMetrics.record(metrics, Arrays.asList("A", "B", "C"), ValidationResult.feasible(),
        false, false);

    Assert.assertEquals(count(metrics, "A", GuardrailMetrics.EVALUATED), 1);
    Assert.assertEquals(count(metrics, "B", GuardrailMetrics.EVALUATED), 1);
    Assert.assertEquals(count(metrics, "C", GuardrailMetrics.EVALUATED), 1);
    // A feasible verdict produces no outcome counters.
    Assert.assertEquals(count(metrics, "A", GuardrailMetrics.BLOCKED), 0);
    Assert.assertEquals(count(metrics, "A", GuardrailMetrics.FORCED), 0);
    Assert.assertEquals(count(metrics, "A", GuardrailMetrics.DRY_RUN_INFEASIBLE), 0);
  }

  @Test
  public void testBlockedOnEnforceInfeasibleNotForced() {
    MetricRegistry metrics = new MetricRegistry();
    GuardrailMetrics.record(metrics, Arrays.asList("A", "B"), infeasible("B"), false, false);

    Assert.assertEquals(count(metrics, "B", GuardrailMetrics.BLOCKED), 1);
    Assert.assertEquals(count(metrics, "B", GuardrailMetrics.FORCED), 0);
    Assert.assertEquals(count(metrics, "B", GuardrailMetrics.DRY_RUN_INFEASIBLE), 0);
    // Only the rule that actually flagged the mutation gets a blocked counter.
    Assert.assertEquals(count(metrics, "A", GuardrailMetrics.BLOCKED), 0);
    // Both rules still counted as evaluated.
    Assert.assertEquals(count(metrics, "A", GuardrailMetrics.EVALUATED), 1);
    Assert.assertEquals(count(metrics, "B", GuardrailMetrics.EVALUATED), 1);
  }

  @Test
  public void testForcedWhenForceTrue() {
    MetricRegistry metrics = new MetricRegistry();
    GuardrailMetrics.record(metrics, Collections.singletonList("A"), infeasible("A"), true, false);

    Assert.assertEquals(count(metrics, "A", GuardrailMetrics.FORCED), 1);
    Assert.assertEquals(count(metrics, "A", GuardrailMetrics.BLOCKED), 0);
    Assert.assertEquals(count(metrics, "A", GuardrailMetrics.DRY_RUN_INFEASIBLE), 0);
  }

  @Test
  public void testDryRunInfeasibleWhenDryRunTrue() {
    MetricRegistry metrics = new MetricRegistry();
    GuardrailMetrics.record(metrics, Collections.singletonList("A"), infeasible("A"), false, true);

    Assert.assertEquals(count(metrics, "A", GuardrailMetrics.DRY_RUN_INFEASIBLE), 1);
    Assert.assertEquals(count(metrics, "A", GuardrailMetrics.BLOCKED), 0);
    Assert.assertEquals(count(metrics, "A", GuardrailMetrics.FORCED), 0);
  }

  @Test
  public void testDryRunTakesPrecedenceOverForce() {
    MetricRegistry metrics = new MetricRegistry();
    // dryRun=true wins even when force=true, mirroring AbstractHelixResource#preflight.
    GuardrailMetrics.record(metrics, Collections.singletonList("A"), infeasible("A"), true, true);

    Assert.assertEquals(count(metrics, "A", GuardrailMetrics.DRY_RUN_INFEASIBLE), 1);
    Assert.assertEquals(count(metrics, "A", GuardrailMetrics.BLOCKED), 0);
    Assert.assertEquals(count(metrics, "A", GuardrailMetrics.FORCED), 0);
  }

  @Test
  public void testMultipleViolationsFromSameRuleCountedOncePerOutcome() {
    MetricRegistry metrics = new MetricRegistry();
    // The same rule reports two violations (e.g. two partitions) but blocked the write once.
    GuardrailMetrics.record(metrics, Collections.singletonList("A"), infeasible("A", "A"), false,
        false);

    Assert.assertEquals(count(metrics, "A", GuardrailMetrics.BLOCKED), 1);
    Assert.assertEquals(count(metrics, "A", GuardrailMetrics.EVALUATED), 1);
  }

  @Test
  public void testDistinctViolatingRulesEachCounted() {
    MetricRegistry metrics = new MetricRegistry();
    GuardrailMetrics.record(metrics, Arrays.asList("A", "B"), infeasible("A", "B"), false, false);

    Assert.assertEquals(count(metrics, "A", GuardrailMetrics.BLOCKED), 1);
    Assert.assertEquals(count(metrics, "B", GuardrailMetrics.BLOCKED), 1);
  }

  @Test
  public void testCountersAccumulateAcrossCalls() {
    MetricRegistry metrics = new MetricRegistry();
    GuardrailMetrics.record(metrics, Collections.singletonList("A"), infeasible("A"), false, false);
    GuardrailMetrics.record(metrics, Collections.singletonList("A"), infeasible("A"), false, false);

    Assert.assertEquals(count(metrics, "A", GuardrailMetrics.EVALUATED), 2);
    Assert.assertEquals(count(metrics, "A", GuardrailMetrics.BLOCKED), 2);
  }

  @Test
  public void testMetricNameFormat() {
    Assert.assertEquals(GuardrailMetrics.metricName("MY_RULE", GuardrailMetrics.BLOCKED),
        "org.apache.helix.rest.guardrail.MY_RULE.blocked");
  }
}
