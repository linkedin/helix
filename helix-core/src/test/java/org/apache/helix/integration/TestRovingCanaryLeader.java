package org.apache.helix.integration;

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
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.helix.HelixAdmin;
import org.apache.helix.TestHelper;
import org.apache.helix.common.ZkTestBase;
import org.apache.helix.integration.manager.ClusterDistributedController;
import org.apache.helix.integration.manager.MockParticipantManager;
import org.apache.helix.model.ExternalView;
import org.apache.helix.model.IdealState;
import org.apache.helix.model.IdealState.RebalanceMode;
import org.apache.helix.tools.ClusterVerifiers.BestPossibleExternalViewVerifier;
import org.apache.helix.tools.ClusterVerifiers.ZkHelixClusterVerifier;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

/**
 * Validates the "roving canary leader" idea from the Outside-In Monitoring design:
 * a single canary (managed) cluster is registered as one LeaderStandby resource in the
 * distributed-controller (grand) cluster, and its controller leadership can be pinned to a
 * chosen controller pod by editing the grand-cluster IdealState (CUSTOMIZED + explicit
 * instance state map). Re-pinning relocates leadership across pods, and after each hop the
 * canary cluster still converges to its best-possible ExternalView -- proving the chosen pod
 * is actually running the controller pipeline for the canary.
 */
public class TestRovingCanaryLeader extends ZkTestBase {
  private static final int NUM_CONTROLLERS = 3;
  private static final int NUM_PARTICIPANTS = 3;
  private static final int START_PORT = 12918;
  private static final String CANARY_DB = "CanaryDB";

  private final String CLASS_NAME = getShortClassName();
  private final String GRAND_CLUSTER = CONTROLLER_CLUSTER_PREFIX + "_" + CLASS_NAME;
  private final String CANARY_CLUSTER = CLUSTER_PREFIX + "_" + CLASS_NAME + "_canary";

  private final ClusterDistributedController[] _distControllers =
      new ClusterDistributedController[NUM_CONTROLLERS];
  private final MockParticipantManager[] _participants =
      new MockParticipantManager[NUM_PARTICIPANTS];
  private final List<String> _controllerNames = new ArrayList<>();

  @BeforeClass
  public void beforeClass() throws Exception {
    System.out.println("START " + CLASS_NAME + " at " + new Date(System.currentTimeMillis()));

    // 1) Grand (distributed-controller) cluster with NUM_CONTROLLERS controller instances.
    _gSetupTool.addCluster(GRAND_CLUSTER, true);
    for (int i = 0; i < NUM_CONTROLLERS; i++) {
      String controllerName = CONTROLLER_PREFIX + "_" + i;
      _controllerNames.add(controllerName);
      _gSetupTool.addInstanceToCluster(GRAND_CLUSTER, controllerName);
    }

    // 2) A single canary managed cluster, activated into the grand cluster. This creates a
    // 1-partition LeaderStandby resource (FULL_AUTO + WagedRebalancer) named after the cluster.
    _gSetupTool.addCluster(CANARY_CLUSTER, true);
    _gSetupTool.activateCluster(CANARY_CLUSTER, GRAND_CLUSTER, true);

    // Give the canary cluster real work so its ExternalView only converges if a controller
    // pipeline is actually running for it.
    _gSetupTool.addResourceToCluster(CANARY_CLUSTER, CANARY_DB, 6, "MasterSlave");
    for (int i = 0; i < NUM_PARTICIPANTS; i++) {
      String instanceName = PARTICIPANT_PREFIX + "_" + (START_PORT + i);
      _gSetupTool.addInstanceToCluster(CANARY_CLUSTER, instanceName);
    }
    _gSetupTool.rebalanceStorageCluster(CANARY_CLUSTER, CANARY_DB, 2);
    for (int i = 0; i < NUM_PARTICIPANTS; i++) {
      String instanceName = PARTICIPANT_PREFIX + "_" + (START_PORT + i);
      _participants[i] = new MockParticipantManager(ZK_ADDR, CANARY_CLUSTER, instanceName);
      _participants[i].syncStart();
    }

    // 3) Start the distributed controllers.
    for (int i = 0; i < NUM_CONTROLLERS; i++) {
      _distControllers[i] =
          new ClusterDistributedController(ZK_ADDR, GRAND_CLUSTER, _controllerNames.get(i));
      _distControllers[i].syncStart();
    }

    Assert.assertTrue(grandClusterVerifier().verifyByPolling(), "Grand cluster did not converge");
    Assert.assertTrue(canaryClusterVerifier().verifyByPolling(), "Canary cluster did not converge");
  }

  @Test
  public void testRovingLeaderAcrossAllControllers() throws Exception {
    HelixAdmin admin = _gSetupTool.getClusterManagementTool();

    String initialLeader = getCanaryLeaderController(admin);
    Assert.assertNotNull(initialLeader, "Canary should have a controller LEADER to begin with");
    Assert.assertTrue(_controllerNames.contains(initialLeader));
    System.out.println("[ROVING] WAGED-assigned initial canary LEADER = " + initialLeader);

    // Rove the canary's leadership onto every controller pod in turn, proving we -- not WAGED --
    // choose the pod, and that the chosen pod actually runs the pipeline (canary converges).
    for (String target : _controllerNames) {
      String before = getCanaryLeaderController(admin);
      pinCanaryLeaderTo(admin, target);

      Assert.assertTrue(TestHelper.verify(() -> target.equals(getCanaryLeaderController(admin)),
          TestHelper.WAIT_DURATION),
          "Canary leadership was not relocated to chosen controller " + target);
      System.out.println("[ROVING] pinned canary LEADER: " + before + " -> " + target
          + " (canary cluster re-converged under " + target + ")");

      // The pod we pinned must be genuinely running the canary's controller pipeline: its
      // ExternalView can only reach best-possible if the leader pipeline is processing.
      Assert.assertTrue(canaryClusterVerifier().verifyByPolling(),
          "Canary cluster did not converge while led by " + target);

      // And leadership is exclusive: no other controller is LEADER for the canary.
      ExternalView ev = admin.getResourceExternalView(GRAND_CLUSTER, CANARY_CLUSTER);
      int leaderCount = 0;
      for (String instance : ev.getStateMap(CANARY_CLUSTER).keySet()) {
        if ("LEADER".equals(ev.getStateMap(CANARY_CLUSTER).get(instance))) {
          leaderCount++;
        }
      }
      Assert.assertEquals(leaderCount, 1, "Expected exactly one LEADER for the canary");
    }
  }

  private void pinCanaryLeaderTo(HelixAdmin admin, String targetController) {
    IdealState is = admin.getResourceIdealState(GRAND_CLUSTER, CANARY_CLUSTER);
    // Take the canary out of WAGED's hands and place the top state explicitly. CUSTOMIZED routes
    // to CustomRebalancer, which honors the instance->state map we set (WAGED className ignored).
    is.setRebalanceMode(RebalanceMode.CUSTOMIZED);
    Map<String, String> stateMap = new HashMap<>();
    for (String controller : _controllerNames) {
      stateMap.put(controller, controller.equals(targetController) ? "LEADER" : "STANDBY");
    }
    // The single partition of the canary resource is named after the canary cluster.
    is.setInstanceStateMap(CANARY_CLUSTER, stateMap);
    admin.setResourceIdealState(GRAND_CLUSTER, CANARY_CLUSTER, is);
  }

  private String getCanaryLeaderController(HelixAdmin admin) {
    ExternalView ev = admin.getResourceExternalView(GRAND_CLUSTER, CANARY_CLUSTER);
    if (ev == null) {
      return null;
    }
    Map<String, String> stateMap = ev.getStateMap(CANARY_CLUSTER);
    if (stateMap == null) {
      return null;
    }
    for (Map.Entry<String, String> e : stateMap.entrySet()) {
      if ("LEADER".equals(e.getValue())) {
        return e.getKey();
      }
    }
    return null;
  }

  private ZkHelixClusterVerifier grandClusterVerifier() {
    return new BestPossibleExternalViewVerifier.Builder(GRAND_CLUSTER).setZkClient(_gZkClient)
        .setWaitTillVerify(TestHelper.DEFAULT_REBALANCE_PROCESSING_WAIT_TIME).build();
  }

  private ZkHelixClusterVerifier canaryClusterVerifier() {
    return new BestPossibleExternalViewVerifier.Builder(CANARY_CLUSTER).setZkClient(_gZkClient)
        .setWaitTillVerify(TestHelper.DEFAULT_REBALANCE_PROCESSING_WAIT_TIME).build();
  }

  @AfterClass
  public void afterClass() throws Exception {
    for (ClusterDistributedController controller : _distControllers) {
      if (controller != null && controller.isConnected()) {
        controller.syncStop();
      }
    }
    for (MockParticipantManager participant : _participants) {
      if (participant != null && participant.isConnected()) {
        participant.syncStop();
      }
    }
    deleteCluster(CANARY_CLUSTER);
    deleteCluster(GRAND_CLUSTER);
    System.out.println("END " + CLASS_NAME + " at " + new Date(System.currentTimeMillis()));
  }
}
