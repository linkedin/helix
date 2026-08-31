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
import java.util.List;

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
   * Evaluate every rule against {@code context} and return the combined result. The result is
   * feasible only if no rule produced a violation.
   */
  public ValidationResult validate(GuardrailContext context) {
    List<Violation> violations = new ArrayList<>();
    for (GuardrailRule rule : rules) {
      try {
        ValidationResult result = rule.validate(context);
        if (result == null) {
          // A rule that returns no verdict has not certified the mutation as safe; fail closed,
          // symmetrically with a rule that throws.
          LOG.warn("Guard rail rule {} returned null for cluster {}; failing closed.", rule.getId(),
              context.getClusterName());
          violations.add(Violation.newBuilder(rule.getId())
              .message("Rule returned no result.").build());
        } else if (!result.isFeasible()) {
          violations.addAll(result.getViolations());
        }
      } catch (Exception e) {
        LOG.warn("Guard rail rule {} failed to evaluate for cluster {}; failing closed.",
            rule.getId(), context.getClusterName(), e);
        violations.add(Violation.newBuilder(rule.getId())
            .message("Rule evaluation failed: " + e.getMessage()).build());
      }
    }
    return violations.isEmpty() ? ValidationResult.feasible() : ValidationResult.of(violations);
  }
}
