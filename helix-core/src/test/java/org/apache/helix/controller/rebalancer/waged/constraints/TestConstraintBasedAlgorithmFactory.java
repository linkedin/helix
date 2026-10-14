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
import java.util.HashMap;
import java.util.Map;

import com.google.common.collect.ImmutableMap;
import org.apache.helix.controller.rebalancer.waged.RebalanceAlgorithm;
import org.apache.helix.model.ClusterConfig;
import org.testng.Assert;
import org.testng.annotations.Test;

import static org.apache.helix.model.ClusterConfig.GlobalRebalancePreferenceKey.EVENNESS;
import static org.apache.helix.model.ClusterConfig.GlobalRebalancePreferenceKey.LESS_MOVEMENT;
import static org.apache.helix.model.ClusterConfig.GlobalRebalancePreferenceKey.TOP_STATE_EVENNESS;

public class TestConstraintBasedAlgorithmFactory {
  private static final Class<?> TOP_STATE = TopStateMaxCapacityUsageInstanceConstraint.class;

  @Test
  public void testUnsetTopStateEvennessKeepsTheWeights() throws Exception {
    Map<Class<?>, Float> defaults = weightsFor(ClusterConfig.DEFAULT_GLOBAL_REBALANCE_PREFERENCE);
    Assert.assertEquals(weightsFor(ImmutableMap.of(EVENNESS, 1, LESS_MOVEMENT, 1)), defaults);
    Assert.assertEquals(
        weightsFor(ImmutableMap.of(EVENNESS, 1, LESS_MOVEMENT, 1, TOP_STATE_EVENNESS, 1)),
        defaults);
  }

  @Test
  public void testTopStateEvennessMultipliesTheTopStateWeightOnly() throws Exception {
    Map<Class<?>, Float> base = weightsFor(ImmutableMap.of(EVENNESS, 1, LESS_MOVEMENT, 2));
    Map<Class<?>, Float> boosted =
        weightsFor(ImmutableMap.of(EVENNESS, 1, LESS_MOVEMENT, 2, TOP_STATE_EVENNESS, 4));
    Assert.assertEquals(boosted.keySet(), base.keySet());
    for (Map.Entry<Class<?>, Float> entry : base.entrySet()) {
      float expected = entry.getKey() == TOP_STATE ? 4 * entry.getValue() : entry.getValue();
      Assert.assertEquals(boosted.get(entry.getKey()).floatValue(), expected, 0f,
          entry.getKey().getSimpleName());
    }
  }

  @Test
  public void testTopStateEvennessCombinesWithTheOtherPreferences() throws Exception {
    Map<Class<?>, Float> unit = weightsFor(ImmutableMap.of(EVENNESS, 1, LESS_MOVEMENT, 1));
    Map<Class<?>, Float> combined =
        weightsFor(ImmutableMap.of(EVENNESS, 2, LESS_MOVEMENT, 5, TOP_STATE_EVENNESS, 3));
    Assert.assertEquals(combined.get(TOP_STATE).floatValue(), 6 * unit.get(TOP_STATE), 0f);
    Assert.assertEquals(combined.get(MaxCapacityUsageInstanceConstraint.class).floatValue(),
        2 * unit.get(MaxCapacityUsageInstanceConstraint.class), 0f);
    Assert.assertEquals(combined.get(PartitionMovementConstraint.class).floatValue(),
        5 * unit.get(PartitionMovementConstraint.class), 0f);
  }

  @Test
  public void testZeroTopStateEvennessTurnsTheTopStateConstraintOff() throws Exception {
    Map<Class<?>, Float> off =
        weightsFor(ImmutableMap.of(EVENNESS, 1, LESS_MOVEMENT, 1, TOP_STATE_EVENNESS, 0));
    Assert.assertEquals(off.get(TOP_STATE).floatValue(), 0f, 0f);
  }

  @Test
  public void testInvalidTopStateEvennessInTheConfig() throws Exception {
    // A REST update of the config writes the record without the checks of the setter.
    ClusterConfig config = new ClusterConfig("testCluster");
    Map<String, String> preference = new HashMap<>();
    preference.put(EVENNESS.name(), "1");
    preference.put(LESS_MOVEMENT.name(), "2");
    preference.put(TOP_STATE_EVENNESS.name(), "-5");
    config.getRecord()
        .setMapField(ClusterConfig.ClusterConfigProperty.REBALANCE_PREFERENCE.name(), preference);
    // A negative value is clamped to 0 instead of making the weight negative.
    Assert.assertEquals(
        weightsFor(config.getGlobalRebalancePreference()).get(TOP_STATE).floatValue(), 0f, 0f);

    // A value that is not an integer falls back to the default instead of failing the rebalance.
    preference.put(TOP_STATE_EVENNESS.name(), "2.5");
    config.getRecord()
        .setMapField(ClusterConfig.ClusterConfigProperty.REBALANCE_PREFERENCE.name(), preference);
    Assert.assertEquals(weightsFor(config.getGlobalRebalancePreference()),
        weightsFor(ImmutableMap.of(EVENNESS, 1, LESS_MOVEMENT, 2)));
  }

  private static Map<Class<?>, Float> weightsFor(
      Map<ClusterConfig.GlobalRebalancePreferenceKey, Integer> preferences) throws Exception {
    return weights(ConstraintBasedAlgorithmFactory.getInstance(preferences));
  }

  // The algorithm does not expose its soft constraints, so the weights are read reflectively.
  @SuppressWarnings("unchecked")
  private static Map<Class<?>, Float> weights(RebalanceAlgorithm algorithm) throws Exception {
    Field field = ConstraintBasedAlgorithm.class.getDeclaredField("_softConstraints");
    field.setAccessible(true);
    Map<Class<?>, Float> weights = new HashMap<>();
    ((Map<SoftConstraint, Float>) field.get(algorithm))
        .forEach((constraint, weight) -> weights.put(constraint.getClass(), weight));
    return weights;
  }
}
