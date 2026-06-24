package org.apache.helix.util;

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
import java.util.List;

/**
 * Outcome of a {@link RebalanceFeasibilityEvaluator} check: whether a proposed cluster mutation
 * keeps the cluster rebalanceable, plus the list of {@link FeasibilityViolation}s if not.
 */
public class FeasibilityResult {
  private final List<FeasibilityViolation> violations;

  private FeasibilityResult(List<FeasibilityViolation> violations) {
    this.violations = violations;
  }

  /**
   * @return a feasible result with no violations.
   */
  public static FeasibilityResult feasible() {
    return new FeasibilityResult(Collections.emptyList());
  }

  /**
   * @param violations the violations found; must be non-empty for an infeasible result.
   * @return a result wrapping the given violations.
   */
  public static FeasibilityResult of(List<FeasibilityViolation> violations) {
    return new FeasibilityResult(new ArrayList<>(violations));
  }

  /**
   * Combines several check results into one, unioning their violations. The merged result is
   * feasible only if every input result is feasible.
   *
   * @param results the per-check results to combine
   * @return a single aggregated result
   */
  public static FeasibilityResult merge(List<FeasibilityResult> results) {
    List<FeasibilityViolation> all = new ArrayList<>();
    for (FeasibilityResult result : results) {
      all.addAll(result.getViolations());
    }
    return all.isEmpty() ? feasible() : of(all);
  }

  /**
   * @return {@code true} when there are no violations.
   */
  public boolean isFeasible() {
    return violations.isEmpty();
  }

  public List<FeasibilityViolation> getViolations() {
    return Collections.unmodifiableList(violations);
  }

  @Override
  public String toString() {
    if (violations.isEmpty()) {
      return "FeasibilityResult: feasible";
    }
    return "FeasibilityResult: infeasible, violations=" + violations;
  }
}
