package org.apache.helix.controller.rebalancer.waged.constraints;

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

import java.lang.reflect.Field;
import java.util.Map;

import org.apache.helix.controller.rebalancer.waged.RebalanceAlgorithm;
import org.apache.helix.model.ClusterConfig;
import org.testng.Assert;
import org.testng.annotations.Test;

import com.google.common.collect.ImmutableMap;

/**
 * The physical capacity constraint is only useful if it actually outweighs the rest of the model,
 * and the weight it is given has to survive whatever rebalance preferences the cluster is
 * configured with.
 */
public class TestConstraintWeightDominance {
  private static final float PHYSICAL_CAPACITY_WEIGHT = 100000f;

  @SuppressWarnings("unchecked")
  private Map<SoftConstraint, Float> weightsFor(int evenness, int lessMovement) throws Exception {
    RebalanceAlgorithm algorithm = ConstraintBasedAlgorithmFactory.getInstance(
        ImmutableMap.of(ClusterConfig.GlobalRebalancePreferenceKey.EVENNESS, evenness,
            ClusterConfig.GlobalRebalancePreferenceKey.LESS_MOVEMENT, lessMovement));
    Field field = algorithm.getClass().getDeclaredField("_softConstraints");
    field.setAccessible(true);
    return (Map<SoftConstraint, Float>) field.get(algorithm);
  }

  private float physicalCapacityWeight(Map<SoftConstraint, Float> weights) {
    return weights.entrySet().stream()
        .filter(e -> e.getKey() instanceof PhysicalCapacitySoftConstraint).map(Map.Entry::getValue)
        .findFirst().orElseThrow(() -> new AssertionError("constraint not registered"));
  }

  private float everythingElse(Map<SoftConstraint, Float> weights) {
    return (float) weights.entrySet().stream()
        .filter(e -> !(e.getKey() instanceof PhysicalCapacitySoftConstraint))
        .mapToDouble(Map.Entry::getValue).sum();
  }

  /**
   * EVENNESS is allowed to be zero, which is a meaningful setting for a cluster that would rather
   * not move replicas than balance them. Scaling this constraint by that preference would set its
   * weight to zero and disable the fix silently, with the feature flag still reading enabled.
   */
  @Test
  public void testWeightSurvivesEveryPreferenceCombination() throws Exception {
    int[] settings = {0, 1, 10, 500, 1000};
    for (int evenness : settings) {
      for (int lessMovement : settings) {
        Map<SoftConstraint, Float> weights = weightsFor(evenness, lessMovement);
        float physical = physicalCapacityWeight(weights);
        Assert.assertEquals(physical, PHYSICAL_CAPACITY_WEIGHT,
            "Weight must not depend on rebalance preferences, but at EVENNESS=" + evenness
                + " LESS_MOVEMENT=" + lessMovement + " it was " + physical);

        // A non-fitting instance concedes at least half a point, so half the weight is the
        // smallest influence this constraint can bring to bear.
        Assert.assertTrue(0.5f * physical > everythingElse(weights),
            "At EVENNESS=" + evenness + " LESS_MOVEMENT=" + lessMovement
                + " the rest of the model sums to " + everythingElse(weights)
                + ", which the constraint's guaranteed " + 0.5f * physical + " must exceed");
      }
    }
  }

  /**
   * The 13500 quoted in the factory: the six pre-existing constraints at their default weights,
   * scaled by the maximum preference. If a future weight change invalidates that number the
   * dominance argument needs revisiting, so pin it here.
   */
  @Test
  public void testPreExistingModelCeiling() throws Exception {
    Assert.assertEquals(everythingElse(weightsFor(1000, 1000)), 13500f,
        "The dominance argument is sized against this ceiling");
  }
}
