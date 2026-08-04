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
import java.util.Collections;
import java.util.List;

/**
 * The outcome of evaluating one or more {@link GuardrailRule}s against a {@link GuardrailContext}.
 * <p>
 * A result is <em>feasible</em> when it carries no {@link Violation}s. Because evaluation is
 * read-only, the same result is used both to gate a real mutation and to answer a dry-run
 * ("simulate") request. The list of violations is exposed for serialization back to REST callers.
 */
public class ValidationResult {
  private final List<Violation> violations;

  private ValidationResult(List<Violation> violations) {
    this.violations = Collections.unmodifiableList(violations);
  }

  /** A passing result with no violations. */
  public static ValidationResult feasible() {
    return new ValidationResult(Collections.emptyList());
  }

  /** A result carrying the given violations. Feasible iff {@code violations} is empty. */
  public static ValidationResult of(List<Violation> violations) {
    return new ValidationResult(new ArrayList<>(violations));
  }

  /** A failing result carrying a single violation. */
  public static ValidationResult infeasible(Violation violation) {
    return new ValidationResult(Collections.singletonList(violation));
  }

  public boolean isFeasible() {
    return violations.isEmpty();
  }

  public List<Violation> getViolations() {
    return violations;
  }

  @Override
  public String toString() {
    return "ValidationResult{feasible=" + isFeasible() + ", violations=" + violations + '}';
  }
}
