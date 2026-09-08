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
  private static final float PHYSICAL_CAPACITY_WEIGHT = 1000000f;

  @SuppressWarnings("unchecked")
  private Map<SoftConstraint, Float> weightsFor(int evenness, int lessMovement) throws Exception {
    RebalanceAlgorithm algorithm = ConstraintBasedAlgorithmFactory.getInstance(
        ImmutableMap.of(ClusterConfig.GlobalRebalancePreferenceKey.EVENNESS, evenness,
            ClusterConfig.GlobalRebalancePreferenceKey.LESS_MOVEMENT, lessMovement));
    Field field = algorithm.getClass().getDeclaredField("_softConstraints");
    field.setAccessible(true);
    return (Map<SoftConstraint, Float>) field.get(algorithm);
  }

  @SuppressWarnings("unchecked")
  private Map<SoftConstraint, Float> weightsWithForcedBaselineConverge() throws Exception {
    RebalanceAlgorithm algorithm = ConstraintBasedAlgorithmFactory.getInstance(
        ImmutableMap.of(ClusterConfig.GlobalRebalancePreferenceKey.EVENNESS, 1,
            ClusterConfig.GlobalRebalancePreferenceKey.LESS_MOVEMENT, 1,
            ClusterConfig.GlobalRebalancePreferenceKey.FORCE_BASELINE_CONVERGE, 1));
    Field field = algorithm.getClass().getDeclaredField("_softConstraints");
    field.setAccessible(true);
    return (Map<SoftConstraint, Float>) field.get(algorithm);
  }

  private float baselineInfluenceWeight(Map<SoftConstraint, Float> weights) {
    return weights.entrySet().stream()
        .filter(e -> e.getKey() instanceof BaselineInfluenceConstraint).map(Map.Entry::getValue)
        .findFirst().orElseThrow(() -> new AssertionError("constraint not registered"));
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
   * FORCE_BASELINE_CONVERGE raises BaselineInfluenceConstraint to FORCE_BASELINE_CONVERGE_WEIGHT,
   * and the baseline is exactly the ledger that cannot see wedged replicas, because occupancy is
   * collected for PARTIAL scope only. A saturated instance the baseline names collects the full
   * baseline weight while a healthy instance the baseline omits collects none of it, so the blind
   * plan gets to argue directly against physical reality.
   * <p>
   * At equal weights the baseline won and this fix silently did nothing on any cluster opting into
   * forced convergence, with the feature flag still reading enabled. The physical weight now has to
   * clear FORCE_BASELINE_CONVERGE_WEIGHT plus the 13500 ceiling of the remaining constraints, twice
   * over because a penalised instance still keeps half the weight.
   */
  @Test
  public void testForcedBaselineConvergeCannotOutvotePhysicalCapacity() throws Exception {
    Map<SoftConstraint, Float> weights = weightsWithForcedBaselineConverge();

    float physical = physicalCapacityWeight(weights);
    float baseline = baselineInfluenceWeight(weights);

    Assert.assertTrue(physical > baseline,
        "the physical constraint must outweigh a forced baseline converge, otherwise the baseline "
            + "decides. physical=" + physical + " baseline=" + baseline);

    // Best case for the saturated instance: the baseline matches it exactly (1.0), its physical
    // room is at the highest value a penalised instance can hold (just under 0.5), and every other
    // constraint also favours it. Worst case for the healthy instance: no baseline credit and no
    // support from any other constraint.
    double saturatedInBaseline = baseline * 1.0d + physical * 0.5d + everythingElse(weights);
    double healthyNotInBaseline = physical * 1.0d;

    Assert.assertTrue(healthyNotInBaseline > saturatedInBaseline,
        "a healthy instance must still win even when the baseline, and everything else, favours "
            + "the saturated one. healthy=" + healthyNotInBaseline
            + " saturated=" + saturatedInBaseline);
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
   * The PR offers a weight ramp as the rollout story: an operator lowers the weight through the
   * soft constraint weight properties instead of switching the behaviour on at full strength. That
   * override writes into MODEL, so the weight has to be read from MODEL at getInstance time rather
   * than captured from the constant, otherwise the ramp silently does nothing.
   */
  @Test
  public void testWeightIsRampableThroughTheModelOverride() throws Exception {
    Field modelField = ConstraintBasedAlgorithmFactory.class.getDeclaredField("MODEL");
    modelField.setAccessible(true);
    @SuppressWarnings("unchecked")
    Map<String, Float> model = (Map<String, Float>) modelField.get(null);
    String key = PhysicalCapacitySoftConstraint.class.getSimpleName();
    Float original = model.get(key);
    Assert.assertNotNull(original, "the constraint must be addressable by its simple name, which is "
        + "the key an operator writes in the properties file");
    try {
      for (float ramped : new float[] {0f, 1f, 250f, 20000f}) {
        model.put(key, ramped);
        Assert.assertEquals(physicalCapacityWeight(weightsFor(1, 1)), ramped,
            "a weight override of " + ramped + " must reach the constraint");
      }
    } finally {
      model.put(key, original);
    }
    Assert.assertEquals(physicalCapacityWeight(weightsFor(1, 1)), original,
        "restoring the model must restore the default weight");
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
