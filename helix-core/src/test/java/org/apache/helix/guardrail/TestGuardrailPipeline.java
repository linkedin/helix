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

import java.util.List;

import org.testng.Assert;
import org.testng.annotations.Test;

/**
 * Unit tests for the guard rail framework mechanics ({@link GuardrailPipeline},
 * {@link ValidationResult}, {@link Violation}), independent of any concrete rule.
 */
public class TestGuardrailPipeline {
  private static final String CLUSTER = "testCluster";

  private static GuardrailContext context() {
    return GuardrailContext.newBuilder(CLUSTER).build();
  }

  /** A rule that always certifies the mutation as safe. */
  private static GuardrailRule passingRule(String id) {
    return new GuardrailRule() {
      @Override
      public String getId() {
        return id;
      }

      @Override
      public ValidationResult validate(GuardrailContext context) {
        return ValidationResult.feasible();
      }
    };
  }

  /** A rule that always reports a single violation. */
  private static GuardrailRule failingRule(String id) {
    return new GuardrailRule() {
      @Override
      public String getId() {
        return id;
      }

      @Override
      public ValidationResult validate(GuardrailContext context) {
        return ValidationResult.infeasible(Violation.newBuilder(id).message("nope").build());
      }
    };
  }

  @Test
  public void testNoRulesIsFeasible() {
    ValidationResult result = new GuardrailPipeline().validate(context());
    Assert.assertTrue(result.isFeasible());
    Assert.assertTrue(result.getViolations().isEmpty());
  }

  @Test
  public void testAllPassingIsFeasible() {
    ValidationResult result =
        new GuardrailPipeline(passingRule("a"), passingRule("b")).validate(context());
    Assert.assertTrue(result.isFeasible());
    Assert.assertTrue(result.getViolations().isEmpty());
  }

  @Test
  public void testSingleFailingIsInfeasible() {
    ValidationResult result = new GuardrailPipeline(failingRule("a")).validate(context());
    Assert.assertFalse(result.isFeasible());
    Assert.assertEquals(result.getViolations().size(), 1);
    Assert.assertEquals(result.getViolations().get(0).getRuleId(), "a");
  }

  @Test
  public void testViolationsAggregateAcrossRules() {
    ValidationResult result = new GuardrailPipeline(passingRule("ok"), failingRule("x"),
        failingRule("y")).validate(context());
    Assert.assertFalse(result.isFeasible());
    List<Violation> violations = result.getViolations();
    Assert.assertEquals(violations.size(), 2);
    Assert.assertEquals(violations.get(0).getRuleId(), "x");
    Assert.assertEquals(violations.get(1).getRuleId(), "y");
  }

  @Test
  public void testThrowingRuleFailsClosed() {
    GuardrailRule boom = new GuardrailRule() {
      @Override
      public String getId() {
        return "boom";
      }

      @Override
      public ValidationResult validate(GuardrailContext context) {
        throw new RuntimeException("kaboom");
      }
    };

    ValidationResult result = new GuardrailPipeline(boom).validate(context());
    Assert.assertFalse(result.isFeasible());
    Assert.assertEquals(result.getViolations().size(), 1);
    Violation violation = result.getViolations().get(0);
    Assert.assertEquals(violation.getRuleId(), "boom");
    Assert.assertTrue(violation.getMessage().contains("kaboom"));
  }

  @Test
  public void testViolationsAreImmutable() {
    ValidationResult result = new GuardrailPipeline(failingRule("a")).validate(context());
    try {
      result.getViolations().add(Violation.newBuilder("b").build());
      Assert.fail("Expected the violations list to be unmodifiable");
    } catch (UnsupportedOperationException expected) {
      // expected
    }
  }
}
