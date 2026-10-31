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

package org.apache.helix.guardrail;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.function.BiConsumer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Runs an ordered list of {@link GuardrailRule}s against a {@link GuardrailContext} and aggregates
 * their {@link Violation}s into a single {@link ValidationResult}.
 * <p>
 * Each endpoint constructs a pipeline with exactly the rules relevant to its mutation, so no
 * central rule registry or per-operation dispatch is required. Because {@link #validate} is
 * read-only, callers can use it either to gate a real write or to answer a dry-run ("simulate")
 * request.
 * <p>
 * A rule that throws is treated as unable to certify the mutation as safe: its failure is recorded
 * as a violation so the pipeline fails closed rather than silently allowing an unchecked write.
 */
public class GuardrailPipeline {
  private static final Logger LOG = LoggerFactory.getLogger(GuardrailPipeline.class);

  private final List<GuardrailRule> rules;

  public GuardrailPipeline(List<GuardrailRule> rules) {
    this.rules = new ArrayList<>(rules);
  }

  public GuardrailPipeline(GuardrailRule... rules) {
    this(Arrays.asList(rules));
  }

  /**
   * The IDs of the rules in this pipeline, in evaluation order (duplicates preserved). Exposed so
   * callers such as metric emitters can attribute a per-rule outcome even for the rules that
   * produced no violation &mdash; e.g. an "evaluated" counter that serves as the denominator for a
   * per-rule "how often did this rule block a write" ratio.
   */
  public List<String> getRuleIds() {
    List<String> ids = new ArrayList<>(rules.size());
    for (GuardrailRule rule : rules) {
      ids.add(rule.getId());
    }
    return Collections.unmodifiableList(ids);
  }

  /**
   * Evaluate every rule against {@code context} and return the combined result. The result is
   * feasible only if no rule produced a violation.
   */
  public ValidationResult validate(GuardrailContext context) {
    return validate(context, null);
  }

  /**
   * Evaluate every rule against {@code context} and return the combined result, reporting how long
   * each rule took so callers can record per-rule latency. {@code ruleDurationNanosObserver}, when
   * non-null, is invoked once per rule with that rule's {@link GuardrailRule#getId() id} and its
   * wall-clock evaluation time in nanoseconds &mdash; including the time spent failing closed when a
   * rule throws or returns no verdict, since that work still contributes to request latency.
   */
  public ValidationResult validate(GuardrailContext context,
      BiConsumer<String, Long> ruleDurationNanosObserver) {
    List<Violation> violations = new ArrayList<>();
    for (GuardrailRule rule : rules) {
      long startNanos = System.nanoTime();
      try {
        ValidationResult result = rule.validate(context);
        if (result == null) {
          // A rule that returns no verdict has not certified the mutation as safe; fail closed,
          // symmetrically with a rule that throws.
          LOG.warn("Guard rail rule {} returned null for cluster {}; failing closed.", rule.getId(),
              context.getClusterName());
          violations.add(Violation.newBuilder(rule.getId())
              .message("Rule returned no result.").indeterminate().build());
        } else if (!result.isFeasible()) {
          violations.addAll(result.getViolations());
        }
      } catch (Exception e) {
        LOG.warn("Guard rail rule {} failed to evaluate for cluster {}; failing closed.",
            rule.getId(), context.getClusterName(), e);
        violations.add(Violation.newBuilder(rule.getId())
            .message("Rule evaluation failed: " + e.getMessage()).indeterminate().build());
      } finally {
        if (ruleDurationNanosObserver != null) {
          ruleDurationNanosObserver.accept(rule.getId(), System.nanoTime() - startNanos);
        }
      }
    }
    return violations.isEmpty() ? ValidationResult.feasible() : ValidationResult.of(violations);
  }
}
