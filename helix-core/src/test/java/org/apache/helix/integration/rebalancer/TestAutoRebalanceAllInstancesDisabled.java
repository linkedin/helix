package org.apache.helix.integration.rebalancer;

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

import java.util.Date;
import java.util.Map;
import java.util.Set;

import org.apache.helix.HelixAdmin;
import org.apache.helix.TestHelper;
import org.apache.helix.common.ZkTestBase;
import org.apache.helix.controller.rebalancer.AutoRebalancer;
import org.apache.helix.controller.rebalancer.strategy.AutoRebalanceStrategy;
import org.apache.helix.integration.manager.ClusterControllerManager;
import org.apache.helix.integration.manager.MockParticipantManager;
import org.apache.helix.manager.zk.ZKHelixAdmin;
import org.apache.helix.model.ExternalView;
import org.apache.helix.model.IdealState;
import org.apache.helix.model.IdealState.RebalanceMode;
import org.apache.helix.tools.ClusterVerifiers.BestPossibleExternalViewVerifier;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

/**
 * Integration test for verifying that when ALL instances are disabled in a cluster,
 * partitions correctly transition to OFFLINE state instead of remaining in their
 * previous states (e.g., STANDBY, SLAVE, MASTER).
 *
 * This test validates the fix for the issue where AutoRebalanceStrategy would return
 * an empty ZNRecord when all instances were disabled, causing partitions to become orphans.
 * The fix preserves the current mapping when all instances are disabled, allowing
 * BestPossibleState to correctly set disabled instances to OFFLINE.
 */
public class TestAutoRebalanceAllInstancesDisabled extends ZkTestBase {

  private static final int NUM_NODES = 3;
  private static final int START_PORT = 12918;
  private static final int NUM_PARTITIONS = 4;
  private static final int NUM_REPLICAS = 2;
  private static final String STATE_MODEL = "MasterSlave";
  private static final String TEST_DB = "TestDB_AllDisabled";

  private String _clusterName;
  private HelixAdmin _admin;
  private ClusterControllerManager _controller;
  private MockParticipantManager[] _participants = new MockParticipantManager[NUM_NODES];
  private BestPossibleExternalViewVerifier _clusterVerifier;

  @BeforeClass
  public void beforeClass() throws Exception {
    super.beforeClass();
    String className = TestHelper.getTestClassName();
    String methodName = TestHelper.getTestMethodName();
    _clusterName = className + "_" + methodName;

    System.out.println("START " + _clusterName + " at " + new Date(System.currentTimeMillis()));

    // Setup cluster
    _gSetupTool.addCluster(_clusterName, true);

    // Add instances
    for (int i = 0; i < NUM_NODES; i++) {
      String instanceName = PARTICIPANT_PREFIX + "_" + (START_PORT + i);
      _gSetupTool.addInstanceToCluster(_clusterName, instanceName);
    }

    // Add resource with AutoRebalanceStrategy
    _gSetupTool.addResourceToCluster(_clusterName, TEST_DB, NUM_PARTITIONS, STATE_MODEL,
        RebalanceMode.FULL_AUTO.name(), AutoRebalanceStrategy.class.getName());

    // Set AutoRebalancer as the rebalancer class
    _admin = new ZKHelixAdmin(_gZkClient);
    IdealState idealState = _admin.getResourceIdealState(_clusterName, TEST_DB);
    idealState.setRebalancerClassName(AutoRebalancer.class.getName());
    _admin.setResourceIdealState(_clusterName, TEST_DB, idealState);

    // Rebalance
    _gSetupTool.rebalanceResource(_clusterName, TEST_DB, NUM_REPLICAS);

    // Start participants
    for (int i = 0; i < NUM_NODES; i++) {
      String instanceName = PARTICIPANT_PREFIX + "_" + (START_PORT + i);
      _participants[i] = new MockParticipantManager(ZK_ADDR, _clusterName, instanceName);
      _participants[i].syncStart();
    }

    // Start controller
    String controllerName = CONTROLLER_PREFIX + "_0";
    _controller = new ClusterControllerManager(ZK_ADDR, _clusterName, controllerName);
    _controller.syncStart();

    // Setup verifier
    _clusterVerifier = new BestPossibleExternalViewVerifier.Builder(_clusterName)
        .setZkClient(_gZkClient)
        .setResources(Set.of(TEST_DB))
        .setWaitTillVerify(TestHelper.DEFAULT_REBALANCE_PROCESSING_WAIT_TIME)
        .build();

    // Wait for initial convergence
    Assert.assertTrue(_clusterVerifier.verifyByPolling(), "Cluster should converge initially");
  }

  @AfterClass
  public void afterClass() throws Exception {
    // Stop controller
    if (_controller != null && _controller.isConnected()) {
      _controller.syncStop();
    }

    // Stop participants
    for (int i = 0; i < NUM_NODES; i++) {
      if (_participants[i] != null && _participants[i].isConnected()) {
        _participants[i].syncStop();
      }
    }

    // Close verifier
    if (_clusterVerifier != null) {
      _clusterVerifier.close();
    }

    // Delete cluster
    deleteCluster(_clusterName);
    System.out.println("END " + _clusterName + " at " + new Date(System.currentTimeMillis()));
  }

  /**
   * Test that when ALL instances are disabled, partitions should transition to OFFLINE state.
   */
  @Test
  public void testDisableAllInstancesShouldTransitionToOffline() throws Exception {
    // Verify partitions are assigned before disabling
    ExternalView evBefore = _admin.getResourceExternalView(_clusterName, TEST_DB);
    Assert.assertNotNull(evBefore, "ExternalView should exist before test");
    Assert.assertFalse(evBefore.getPartitionSet().isEmpty(), "Partitions should be assigned");

    // Verify at least one partition has non-OFFLINE state
    boolean hasNonOfflineState = false;
    for (String partition : evBefore.getPartitionSet()) {
      Map<String, String> stateMap = evBefore.getStateMap(partition);
      if (stateMap != null) {
        for (String state : stateMap.values()) {
          if (!"OFFLINE".equals(state)) {
            hasNonOfflineState = true;
            break;
          }
        }
      }
      if (hasNonOfflineState) {
        break;
      }
    }
    Assert.assertTrue(hasNonOfflineState, "Some partitions should be in non-OFFLINE state initially");

    // Disable ALL instances
    for (int i = 0; i < NUM_NODES; i++) {
      String instanceName = _participants[i].getInstanceName();
      _admin.enableInstance(_clusterName, instanceName, false);
    }

    // Verify all instances are disabled
    for (int i = 0; i < NUM_NODES; i++) {
      String instanceName = _participants[i].getInstanceName();
      boolean result = TestHelper.verify(
          () -> !_admin.getInstanceConfig(_clusterName, instanceName).getInstanceEnabled(),
          TestHelper.WAIT_DURATION);
      Assert.assertTrue(result, "Instance " + instanceName + " should be disabled");
    }

    // Wait for state transitions to complete
    // Note: We can't use the standard verifier here because it expects all states to match
    // the ideal state, but with all instances disabled, the ideal state computation differs.
    // Instead, we verify directly that all states become OFFLINE.
    boolean allOffline = TestHelper.verify(() -> {
      ExternalView ev = _admin.getResourceExternalView(_clusterName, TEST_DB);
      if (ev == null) {
        return false;
      }
      for (String partition : ev.getPartitionSet()) {
        Map<String, String> stateMap = ev.getStateMap(partition);
        if (stateMap == null || stateMap.isEmpty()) {
          continue;
        }
        for (String state : stateMap.values()) {
          if (!"OFFLINE".equals(state)) {
            return false;
          }
        }
      }
      return true;
    }, 60000); // Wait up to 60 seconds for transitions

    // Get final external view for assertion message
    ExternalView evAfterDisable = _admin.getResourceExternalView(_clusterName, TEST_DB);
    StringBuilder stateInfo = new StringBuilder("Final states after disabling all instances: ");
    if (evAfterDisable != null) {
      for (String partition : evAfterDisable.getPartitionSet()) {
        Map<String, String> stateMap = evAfterDisable.getStateMap(partition);
        stateInfo.append(partition).append("=").append(stateMap).append(", ");
      }
    }

    Assert.assertTrue(allOffline, "All partitions should be in OFFLINE state when all instances " +
        "are disabled. " + stateInfo);

    // Re-enable all instances
    for (int i = 0; i < NUM_NODES; i++) {
      String instanceName = _participants[i].getInstanceName();
      _admin.enableInstance(_clusterName, instanceName, true);
    }

    // Verify all instances are enabled
    for (int i = 0; i < NUM_NODES; i++) {
      String instanceName = _participants[i].getInstanceName();
      boolean result = TestHelper.verify(
          () -> _admin.getInstanceConfig(_clusterName, instanceName).getInstanceEnabled(),
          TestHelper.WAIT_DURATION);
      Assert.assertTrue(result, "Instance " + instanceName + " should be enabled");
    }

    // Wait for cluster to reach stable state
    Assert.assertTrue(_clusterVerifier.verifyByPolling(),
        "Cluster should converge after re-enabling instances");

    // Verify partitions are re-assigned with non-OFFLINE states
    boolean hasNonOfflineStateAfterReenable = TestHelper.verify(() -> {
      ExternalView ev = _admin.getResourceExternalView(_clusterName, TEST_DB);
      if (ev == null) {
        return false;
      }
      for (String partition : ev.getPartitionSet()) {
        Map<String, String> stateMap = ev.getStateMap(partition);
        if (stateMap != null) {
          for (String state : stateMap.values()) {
            if (!"OFFLINE".equals(state)) {
              return true;
            }
          }
        }
      }
      return false;
    }, TestHelper.WAIT_DURATION);

    Assert.assertTrue(hasNonOfflineStateAfterReenable,
        "Partitions should be re-assigned with non-OFFLINE states after re-enabling instances");
  }
}

