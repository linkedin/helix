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
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import com.google.common.collect.ImmutableMap;
import org.apache.helix.controller.rebalancer.waged.RebalanceAlgorithm;
import org.apache.helix.model.ClusterConfig;
import org.testng.Assert;
import org.testng.annotations.Test;

public class TestConstraintBasedAlgorithmFactoryWeights {
  private static final Map<ClusterConfig.GlobalRebalancePreferenceKey, Integer> PREFERENCES =
      ImmutableMap.of(ClusterConfig.GlobalRebalancePreferenceKey.EVENNESS, 1,
          ClusterConfig.GlobalRebalancePreferenceKey.LESS_MOVEMENT, 2);
  private static final String TOP_STATE =
      TopStateMaxCapacityUsageInstanceConstraint.class.getSimpleName();
  private static final String MOVEMENT = PartitionMovementConstraint.class.getSimpleName();

  @Test
  public void testOverrideAppliesToThatInstanceOnly() throws Exception {
    Map<String, Float> configured = ConstraintBasedAlgorithmFactory.getConstraintWeights();

    RebalanceAlgorithm overridden = ConstraintBasedAlgorithmFactory.getInstance(PREFERENCES,
        Collections.singletonMap(TOP_STATE, 12f));
    RebalanceAlgorithm defaults = ConstraintBasedAlgorithmFactory.getInstance(PREFERENCES);

    Assert.assertEquals(weight(overridden, TopStateMaxCapacityUsageInstanceConstraint.class), 12f);
    Assert.assertEquals(weight(defaults, TopStateMaxCapacityUsageInstanceConstraint.class),
        configured.get(TOP_STATE));
    // Constraints without an override keep the configured weight and the preference multiplier.
    Assert.assertEquals(weight(overridden, PartitionMovementConstraint.class),
        configured.get(MOVEMENT) * 2);
    Assert.assertEquals(ConstraintBasedAlgorithmFactory.getConstraintWeights(), configured);
  }

  @Test
  public void testOverrideKeepsPreferenceMultipliers() throws Exception {
    Map<String, Float> overrides = new HashMap<>();
    overrides.put(MOVEMENT, 3f);
    overrides.put(TOP_STATE, 5f);
    RebalanceAlgorithm algorithm = ConstraintBasedAlgorithmFactory.getInstance(
        ImmutableMap.of(ClusterConfig.GlobalRebalancePreferenceKey.EVENNESS, 2,
            ClusterConfig.GlobalRebalancePreferenceKey.LESS_MOVEMENT, 4), overrides);

    Assert.assertEquals(weight(algorithm, PartitionMovementConstraint.class), 12f);
    Assert.assertEquals(weight(algorithm, TopStateMaxCapacityUsageInstanceConstraint.class), 10f);
  }

  @Test
  public void testZeroWeightIsAllowed() throws Exception {
    RebalanceAlgorithm algorithm = ConstraintBasedAlgorithmFactory.getInstance(PREFERENCES,
        ImmutableMap.of(MOVEMENT, 0f, BaselineInfluenceConstraint.class.getSimpleName(), 0f));

    Assert.assertEquals(weight(algorithm, PartitionMovementConstraint.class), 0f);
    Assert.assertEquals(weight(algorithm, BaselineInfluenceConstraint.class), 0f);
  }

  @Test(expectedExceptions = IllegalArgumentException.class)
  public void testUnknownConstraintIsRejected() {
    ConstraintBasedAlgorithmFactory.getInstance(PREFERENCES,
        Collections.singletonMap("NoSuchConstraint", 1f));
  }

  @Test(expectedExceptions = UnsupportedOperationException.class)
  public void testConfiguredWeightsAreReadOnly() {
    ConstraintBasedAlgorithmFactory.getConstraintWeights().put(TOP_STATE, 1f);
  }

  @SuppressWarnings("unchecked")
  private static float weight(RebalanceAlgorithm algorithm, Class<?> constraintType)
      throws Exception {
    Field field = ConstraintBasedAlgorithm.class.getDeclaredField("_softConstraints");
    field.setAccessible(true);
    Map<SoftConstraint, Float> softConstraints = (Map<SoftConstraint, Float>) field.get(algorithm);
    return softConstraints.entrySet().stream()
        .filter(entry -> constraintType.isInstance(entry.getKey())).findFirst().get().getValue();
  }
}
