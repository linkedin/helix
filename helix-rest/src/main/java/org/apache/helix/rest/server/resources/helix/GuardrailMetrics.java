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

import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import com.codahale.metrics.MetricRegistry;
import org.apache.helix.guardrail.ValidationResult;
import org.apache.helix.guardrail.Violation;

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
  static final String PASSED = "passed";
  static final String BLOCKED = "blocked";
  static final String FORCED = "forced";
  static final String DRY_RUN_INFEASIBLE = "dryRunInfeasible";

  private GuardrailMetrics() {
  }

  /**
   * Record the outcome of a single guard rail evaluation. Every rule that ran is timed via its
   * {@code evaluated} {@link com.codahale.metrics.Timer} and tallied into exactly one terminal
   * outcome counter ({@code passed}, or one of {@code blocked}/{@code forced}/
   * {@code dryRunInfeasible}).
   *
   * @param metrics           the per-namespace registry to update
   * @param evaluatedRuleIds  the ids of every rule that ran (see
   *                          {@link org.apache.helix.guardrail.GuardrailPipeline#getRuleIds()})
   * @param ruleDurationNanos per-rule evaluation time in nanoseconds, keyed by rule id; a rule with
   *                          no entry is timed as {@code 0}
   * @param result            the aggregated verdict for the mutation
   * @param force             whether the caller passed {@code force=true}
   * @param dryRun            whether the caller passed {@code dryRun=true}
   */
  static void record(MetricRegistry metrics, List<String> evaluatedRuleIds,
      Map<String, Long> ruleDurationNanos, ValidationResult result, boolean force, boolean dryRun) {
    // A rule may report several violations (e.g. one per partition), but it blocked the write once,
    // so attribute its outcome once per distinct rule.
    Set<String> violatedRuleIds = new LinkedHashSet<>();
    for (Violation violation : result.getViolations()) {
      violatedRuleIds.add(violation.getRuleId());
    }
    // dryRun takes precedence over force, mirroring AbstractHelixResource#preflight.
    String failureOutcome = dryRun ? DRY_RUN_INFEASIBLE : (force ? FORCED : BLOCKED);
    for (String ruleId : new LinkedHashSet<>(evaluatedRuleIds)) {
      long durationNanos =
          ruleDurationNanos == null ? 0L : ruleDurationNanos.getOrDefault(ruleId, 0L);
      metrics.timer(metricName(ruleId, EVALUATED)).update(durationNanos, TimeUnit.NANOSECONDS);
      String outcome = violatedRuleIds.contains(ruleId) ? failureOutcome : PASSED;
      metrics.counter(metricName(ruleId, outcome)).inc();
    }
  }

  static String metricName(String ruleId, String outcome) {
    return MetricRegistry.name(METRIC_PREFIX, ruleId, outcome);
  }
}
