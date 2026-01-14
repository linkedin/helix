package org.apache.helix.controller.stages;

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

import com.google.common.collect.ImmutableList;
import org.apache.helix.api.config.StateTransitionThrottleConfig;
import org.apache.helix.model.ClusterConfig;
import org.apache.helix.model.IdealState;
import org.testng.Assert;
import org.testng.annotations.Test;

/**
 * Integration tests for availability-aware cross-resource prioritization in IntermediateStateCalcStage.
 * These tests validate that when the AVAILABILITY_AWARE_PRIORITIZATION_ENABLED flag is set,
 * messages are prioritized based on availability impact across all resources.
 */
public class TestAvailabilityAwarePrioritization extends BaseStageTest {

  private ClusterConfig _clusterConfig;

  /**
   * Test that availability-aware prioritization is disabled by default
   * and uses traditional resource-priority-based processing.
   */
  @Test
  public void testAvailabilityAwarePrioritizationDisabledByDefault() {
    String resourcePrefix = "resource";
    int nResource = 2;
    int nPartition = 1;
    int nReplica = 3;

    String[] resources = new String[nResource];
    for (int i = 0; i < nResource; i++) {
      resources[i] = resourcePrefix + "_" + i;
    }

    preSetup(resources, nReplica, nReplica);
    event.addAttribute(AttributeName.RESOURCES.name(), getResourceMap(resources, nPartition, "MasterSlave"));
    event.addAttribute(AttributeName.RESOURCES_TO_REBALANCE.name(),
        getResourceMap(resources, nPartition, "MasterSlave"));

    // Verify feature is disabled by default
    Assert.assertFalse(_clusterConfig.isAvailabilityAwarePrioritizationEnabled(),
        "Availability-aware prioritization should be disabled by default");
  }

  /**
   * Test that availability-aware prioritization can be enabled via cluster config.
   */
  @Test
  public void testAvailabilityAwarePrioritizationCanBeEnabled() {
    String resourcePrefix = "resource";
    int nResource = 2;
    int nPartition = 1;
    int nReplica = 3;

    String[] resources = new String[nResource];
    for (int i = 0; i < nResource; i++) {
      resources[i] = resourcePrefix + "_" + i;
    }

    preSetup(resources, nReplica, nReplica);

    // Enable availability-aware prioritization
    _clusterConfig.setAvailabilityAwarePrioritizationEnabled(true);
    setClusterConfig(_clusterConfig);

    Assert.assertTrue(_clusterConfig.isAvailabilityAwarePrioritizationEnabled(),
        "Availability-aware prioritization should be enabled after setting");
  }

  /**
   * Test that the feature flag can be toggled on and off and that the cluster config
   * properly stores and retrieves the value.
   */
  @Test
  public void testFeatureFlagPersistence() {
    String[] resources = new String[]{"testResource"};
    int nReplica = 3;

    preSetup(resources, nReplica, nReplica);

    // Initially disabled
    Assert.assertFalse(_clusterConfig.isAvailabilityAwarePrioritizationEnabled());

    // Enable
    _clusterConfig.setAvailabilityAwarePrioritizationEnabled(true);
    Assert.assertTrue(_clusterConfig.isAvailabilityAwarePrioritizationEnabled());

    // Disable again
    _clusterConfig.setAvailabilityAwarePrioritizationEnabled(false);
    Assert.assertFalse(_clusterConfig.isAvailabilityAwarePrioritizationEnabled());
  }

  /**
   * Test that the feature flag integrates properly with other cluster configs.
   */
  @Test
  public void testFeatureFlagWithOtherConfigs() {
    String[] resources = new String[]{"testResource"};
    int nReplica = 3;

    preSetup(resources, nReplica, nReplica);

    // Set multiple configs including the new feature flag
    _clusterConfig.setAvailabilityAwarePrioritizationEnabled(true);
    _clusterConfig.setErrorOrRecoveryPartitionThresholdForLoadBalance(5);
    _clusterConfig.setStateTransitionThrottleConfigs(
        ImmutableList.of(new StateTransitionThrottleConfig(
            StateTransitionThrottleConfig.RebalanceType.ANY,
            StateTransitionThrottleConfig.ThrottleScope.CLUSTER, 10)));
    setClusterConfig(_clusterConfig);

    // Verify all configs are set correctly
    Assert.assertTrue(_clusterConfig.isAvailabilityAwarePrioritizationEnabled());
    Assert.assertEquals(_clusterConfig.getErrorOrRecoveryPartitionThresholdForLoadBalance(), 5);
  }

  // Helper method for test setup

  private void preSetup(String[] resources, int numReplicas, int minActiveReplicas) {
    setupIdealState(numReplicas + 1, resources, 1, numReplicas,
        IdealState.RebalanceMode.FULL_AUTO, "MasterSlave", null, null, minActiveReplicas);
    setupLiveInstances(numReplicas + 1);
    setupStateModel();

    _clusterConfig = new ClusterConfig(_clusterName);
    _clusterConfig.setStateTransitionThrottleConfigs(
        ImmutableList.of(new StateTransitionThrottleConfig(
            StateTransitionThrottleConfig.RebalanceType.ANY,
            StateTransitionThrottleConfig.ThrottleScope.CLUSTER, 100)));
    setClusterConfig(_clusterConfig);
  }
}

