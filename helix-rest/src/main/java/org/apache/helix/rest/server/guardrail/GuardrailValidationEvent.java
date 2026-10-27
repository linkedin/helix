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
import java.util.Collections;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.helix.guardrail.ValidationResult;
import org.apache.helix.guardrail.Violation;

/**
 * Immutable description of a single guard rail validation, delivered to
 * {@link GuardrailValidationListener}s. It carries, for every rule that ran, the rule's terminal
 * {@link Outcome} and its evaluation latency, plus the request-level {@code force}/{@code dryRun}
 * flags and the aggregated {@link ValidationResult}.
 * <p>
 * The per-rule outcome derivation is shared verbatim with the JMX counters emitted by
 * {@code org.apache.helix.rest.server.resources.helix.GuardrailMetrics} (both consume
 * {@link #getRuleEvaluations()}), so a listener and the JMX metrics can never disagree on a rule's
 * verdict.
 */
public class GuardrailValidationEvent {

  /**
   * The terminal verdict for one rule that ran during a validation. Every rule that ran lands in
   * exactly one {@code Outcome}. The {@link #getMetricName() metric name} is the stable token used
   * as the trailing segment of the JMX counter name and of any downstream metric emitted from a
   * listener, so the two surfaces never drift.
   */
  public enum Outcome {
    /** The rule ran and raised no violation, certifying the mutation as safe. */
    PASSED("passed"),
    /** The rule flagged the mutation and the write was rejected (enforce mode, not forced). */
    BLOCKED("blocked"),
    /** The rule flagged the mutation but the caller overrode the block with {@code force=true}. */
    FORCED("forced"),
    /** The rule flagged the mutation during a {@code dryRun=true} simulation (no write attempted). */
    DRY_RUN_INFEASIBLE("dryRunInfeasible");

    private final String _metricName;

    Outcome(String metricName) {
      _metricName = metricName;
    }

    /**
     * @return the stable lower-camel token for this outcome, e.g. {@code "dryRunInfeasible"}, used
     *         as the trailing segment of the guard rail metric name.
     */
    public String getMetricName() {
      return _metricName;
    }
  }

  /**
   * The outcome and evaluation latency of a single rule that ran during the validation.
   */
  public static final class RuleEvaluation {
    private final String _ruleId;
    private final Outcome _outcome;
    private final long _durationNanos;

    private RuleEvaluation(String ruleId, Outcome outcome, long durationNanos) {
      _ruleId = ruleId;
      _outcome = outcome;
      _durationNanos = durationNanos;
    }

    /** @return the {@link org.apache.helix.guardrail.GuardrailRule#getId() id} of the rule. */
    public String getRuleId() {
      return _ruleId;
    }

    /** @return the terminal outcome for this rule. */
    public Outcome getOutcome() {
      return _outcome;
    }

    /** @return how long this rule took to evaluate, in nanoseconds ({@code 0} if not measured). */
    public long getDurationNanos() {
      return _durationNanos;
    }
  }

  private final String _namespace;
  private final boolean _force;
  private final boolean _dryRun;
  private final ValidationResult _result;
  private final List<RuleEvaluation> _ruleEvaluations;

  /**
   * @param namespace         the helix-rest namespace that served the request
   * @param evaluatedRuleIds  the ids of every rule that ran (see
   *                          {@link org.apache.helix.guardrail.GuardrailPipeline#getRuleIds()})
   * @param ruleDurationNanos per-rule evaluation time in nanoseconds, keyed by rule id; a rule with
   *                          no entry is timed as {@code 0}. May be {@code null}.
   * @param result            the aggregated verdict for the mutation
   * @param force             whether the caller passed {@code force=true}
   * @param dryRun            whether the caller passed {@code dryRun=true}
   */
  public GuardrailValidationEvent(String namespace, List<String> evaluatedRuleIds,
      Map<String, Long> ruleDurationNanos, ValidationResult result, boolean force, boolean dryRun) {
    _namespace = namespace;
    _force = force;
    _dryRun = dryRun;
    _result = result;
    _ruleEvaluations =
        deriveRuleEvaluations(evaluatedRuleIds, ruleDurationNanos, result, force, dryRun);
  }

  /** @return the helix-rest namespace that served the request. */
  public String getNamespace() {
    return _namespace;
  }

  /** @return whether the caller requested {@code force=true}. */
  public boolean isForce() {
    return _force;
  }

  /** @return whether the caller requested {@code dryRun=true}. */
  public boolean isDryRun() {
    return _dryRun;
  }

  /** @return the aggregated guard rail verdict for the mutation. */
  public ValidationResult getResult() {
    return _result;
  }

  /** @return whether the mutation was feasible, i.e. no rule raised a violation. */
  public boolean isFeasible() {
    return _result.isFeasible();
  }

  /**
   * @return the per-rule outcomes and latencies for every rule that ran, in evaluation order and
   *         de-duplicated by rule id. Never {@code null}; unmodifiable.
   */
  public List<RuleEvaluation> getRuleEvaluations() {
    return _ruleEvaluations;
  }

  /**
   * Derive the per-rule outcome and latency for every rule that ran. This is the single source of
   * truth shared by the JMX counters ({@code GuardrailMetrics}) and the push-based
   * {@link GuardrailValidationListener}, so the two surfaces can never disagree on a rule's verdict.
   */
  private static List<RuleEvaluation> deriveRuleEvaluations(List<String> evaluatedRuleIds,
      Map<String, Long> ruleDurationNanos, ValidationResult result, boolean force, boolean dryRun) {
    // A rule may report several violations (e.g. one per partition), but it blocked the write once,
    // so attribute its outcome once per distinct rule.
    Set<String> violatedRuleIds = new LinkedHashSet<>();
    for (Violation violation : result.getViolations()) {
      violatedRuleIds.add(violation.getRuleId());
    }
    // dryRun takes precedence over force, mirroring AbstractHelixResource#preflight.
    Outcome failureOutcome =
        dryRun ? Outcome.DRY_RUN_INFEASIBLE : (force ? Outcome.FORCED : Outcome.BLOCKED);
    List<RuleEvaluation> ruleEvaluations = new ArrayList<>();
    for (String ruleId : new LinkedHashSet<>(evaluatedRuleIds)) {
      long durationNanos =
          ruleDurationNanos == null ? 0L : ruleDurationNanos.getOrDefault(ruleId, 0L);
      Outcome outcome = violatedRuleIds.contains(ruleId) ? failureOutcome : Outcome.PASSED;
      ruleEvaluations.add(new RuleEvaluation(ruleId, outcome, durationNanos));
    }
    return Collections.unmodifiableList(ruleEvaluations);
  }
}
