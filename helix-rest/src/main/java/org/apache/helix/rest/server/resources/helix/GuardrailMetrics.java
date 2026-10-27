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

import java.util.concurrent.TimeUnit;

import com.codahale.metrics.MetricRegistry;
import org.apache.helix.rest.server.guardrail.GuardrailValidationEvent;

/**
 * Emits per-rule guard rail counters into a helix-rest {@link MetricRegistry} so operators can
 * quantify how often each rule stopped an unsafe mutation &mdash; i.e. answer "rule X prevented N
 * failures in production".
 * <p>
 * For every guard rail evaluation the following metrics are emitted, keyed by the
 * rule's {@link org.apache.helix.guardrail.GuardrailRule#getId() id}:
 * <ul>
 *   <li>{@code evaluated} &mdash; a {@link com.codahale.metrics.Timer} updated once per rule that
 *       ran, regardless of verdict or mode. Its count is the denominator for a per-rule "block
 *       rate" (and, because a rule also runs during a dry-run simulation, includes dry-run
 *       evaluations); its percentiles expose how long the rule takes to evaluate. Every rule run is
 *       also tallied into exactly one terminal outcome counter below, so
 *       {@code evaluated.count == passed + blocked + forced + dryRunInfeasible} per rule.</li>
 *   <li>{@code passed} &mdash; the rule ran and raised no violation, certifying the mutation as
 *       safe. This is the per-rule "successes" number.</li>
 *   <li>{@code blocked} &mdash; the rule flagged the mutation and the write was rejected with
 *       {@code 400} (enforce mode, not forced): a real prevented production change. This is the
 *       headline "prevented failures" number.</li>
 *   <li>{@code forced} &mdash; the rule flagged the mutation but the caller overrode the block with
 *       {@code force=true}, so the write proceeded anyway.</li>
 *   <li>{@code dryRunInfeasible} &mdash; the rule flagged the mutation during a {@code dryRun=true}
 *       simulation, where no write is ever attempted.</li>
 * </ul>
 * Metrics are named {@code org.apache.helix.rest.guardrail.<RULE_ID>.<outcome>} so the helix-rest
 * JMX reporter exports them under the same domain as every other REST metric. These are Codahale
 * metrics in the per-namespace registry, matching the existing helix-rest metric convention;
 * nothing here assumes a particular downstream metric backend.
 */
final class GuardrailMetrics {
  static final String METRIC_PREFIX = "org.apache.helix.rest.guardrail";
  static final String EVALUATED = "evaluated";
  static final String PASSED = GuardrailValidationEvent.Outcome.PASSED.getMetricName();
  static final String BLOCKED = GuardrailValidationEvent.Outcome.BLOCKED.getMetricName();
  static final String FORCED = GuardrailValidationEvent.Outcome.FORCED.getMetricName();
  static final String DRY_RUN_INFEASIBLE =
      GuardrailValidationEvent.Outcome.DRY_RUN_INFEASIBLE.getMetricName();

  private GuardrailMetrics() {
  }

  /**
   * Record the outcome of a single guard rail evaluation. Every rule that ran is timed via its
   * {@code evaluated} {@link com.codahale.metrics.Timer} and tallied into exactly one terminal
   * outcome counter ({@code passed}, or one of {@code blocked}/{@code forced}/
   * {@code dryRunInfeasible}). The per-rule outcomes are derived once in
   * {@link GuardrailValidationEvent} and shared with any
   * {@link org.apache.helix.rest.server.guardrail.GuardrailValidationListener}, so the JMX counters
   * here and a listener can never disagree on a rule's verdict.
   *
   * @param metrics the per-namespace registry to update
   * @param event   the guard rail validation whose per-rule outcomes and latencies to record
   */
  static void record(MetricRegistry metrics, GuardrailValidationEvent event) {
    for (GuardrailValidationEvent.RuleEvaluation evaluation : event.getRuleEvaluations()) {
      metrics.timer(metricName(evaluation.getRuleId(), EVALUATED))
          .update(evaluation.getDurationNanos(), TimeUnit.NANOSECONDS);
      metrics.counter(metricName(evaluation.getRuleId(), evaluation.getOutcome().getMetricName()))
          .inc();
    }
  }

  static String metricName(String ruleId, String outcome) {
    return MetricRegistry.name(METRIC_PREFIX, ruleId, outcome);
  }
}
