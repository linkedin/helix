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
import java.util.Set;

import com.codahale.metrics.MetricRegistry;
import org.apache.helix.guardrail.ValidationResult;
import org.apache.helix.guardrail.Violation;

/**
 * Emits per-rule guard rail counters into a helix-rest {@link MetricRegistry} so operators can
 * quantify how often each rule stopped an unsafe mutation &mdash; i.e. answer "rule X prevented N
 * failures in production".
 * <p>
 * For every guard rail evaluation the following counters are incremented, keyed by the offending
 * rule's {@link org.apache.helix.guardrail.GuardrailRule#getId() id}:
 * <ul>
 *   <li>{@code evaluated} &mdash; once per rule that ran, regardless of verdict or mode. This is the
 *       denominator for a per-rule "block rate". Because a rule also runs during a dry-run
 *       simulation, this count includes dry-run evaluations.</li>
 *   <li>{@code blocked} &mdash; the rule flagged the mutation and the write was rejected with
 *       {@code 400} (enforce mode, not forced): a real prevented production change. This is the
 *       headline "prevented failures" number.</li>
 *   <li>{@code forced} &mdash; the rule flagged the mutation but the caller overrode the block with
 *       {@code force=true}, so the write proceeded anyway.</li>
 *   <li>{@code dryRunInfeasible} &mdash; the rule flagged the mutation during a {@code dryRun=true}
 *       simulation, where no write is ever attempted.</li>
 * </ul>
 * Counters are named {@code org.apache.helix.rest.guardrail.<RULE_ID>.<outcome>} so the helix-rest
 * JMX reporter exports them under the same domain as every other REST metric. These are Codahale
 * {@link com.codahale.metrics.Counter}s in the per-namespace registry, matching the existing
 * helix-rest metric convention; nothing here assumes a particular downstream metric backend.
 */
final class GuardrailMetrics {
  static final String METRIC_PREFIX = "org.apache.helix.rest.guardrail";
  static final String EVALUATED = "evaluated";
  static final String BLOCKED = "blocked";
  static final String FORCED = "forced";
  static final String DRY_RUN_INFEASIBLE = "dryRunInfeasible";

  private GuardrailMetrics() {
  }

  /**
   * Record the outcome of a single guard rail evaluation.
   *
   * @param metrics          the per-namespace registry to increment
   * @param evaluatedRuleIds the ids of every rule that ran (see
   *                         {@link org.apache.helix.guardrail.GuardrailPipeline#getRuleIds()})
   * @param result           the aggregated verdict for the mutation
   * @param force            whether the caller passed {@code force=true}
   * @param dryRun           whether the caller passed {@code dryRun=true}
   */
  static void record(MetricRegistry metrics, List<String> evaluatedRuleIds, ValidationResult result,
      boolean force, boolean dryRun) {
    for (String ruleId : evaluatedRuleIds) {
      metrics.counter(metricName(ruleId, EVALUATED)).inc();
    }
    if (result.isFeasible()) {
      return;
    }
    // dryRun takes precedence over force, mirroring AbstractHelixResource#preflight.
    String outcome = dryRun ? DRY_RUN_INFEASIBLE : (force ? FORCED : BLOCKED);
    // A rule may report several violations (e.g. one per partition), but it blocked the write once,
    // so attribute the outcome once per distinct rule.
    Set<String> violatedRuleIds = new LinkedHashSet<>();
    for (Violation violation : result.getViolations()) {
      violatedRuleIds.add(violation.getRuleId());
    }
    for (String ruleId : violatedRuleIds) {
      metrics.counter(metricName(ruleId, outcome)).inc();
    }
  }

  static String metricName(String ruleId, String outcome) {
    return MetricRegistry.name(METRIC_PREFIX, ruleId, outcome);
  }
}
