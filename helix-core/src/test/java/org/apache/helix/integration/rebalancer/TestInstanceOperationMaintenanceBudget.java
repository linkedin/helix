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

import java.util.ArrayList;
import java.util.Date;
import java.util.List;

import org.apache.helix.ConfigAccessor;
import org.apache.helix.HelixDataAccessor;
import org.apache.helix.TestHelper;
import org.apache.helix.common.ZkTestBase;
import org.apache.helix.integration.manager.ClusterControllerManager;
import org.apache.helix.integration.manager.MockParticipantManager;
import org.apache.helix.manager.zk.ZKHelixDataAccessor;
import org.apache.helix.model.BuiltInStateModelDefinitions;
import org.apache.helix.model.ClusterConfig;
import org.apache.helix.model.InstanceConfig;
import org.apache.helix.model.MaintenanceSignal;
import org.apache.helix.tools.ClusterVerifiers.BestPossibleExternalViewVerifier;
import org.apache.helix.tools.ClusterVerifiers.ZkHelixClusterVerifier;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

/**
 * End-to-end check that INSTANCE_OPERATION_MAINTENANCE_UNTIL_MS markers exempt instances
 * from the cluster-wide offline budget that drives auto Maintenance Mode entry, while
 * preserving the trigger for unplanned losses.
 */
public class TestInstanceOperationMaintenanceBudget extends ZkTestBase {
  private static final int NUM_NODE = 6;
  private static final int START_PORT = 13918;
  private static final int PARTITIONS = 4;
  private static final int MAX_OFFLINE_INSTANCES_ALLOWED = 1;
  private static final long ONE_HOUR_MS = 3_600_000L;

  private final String _className = getShortClassName();
  private final String _clusterName = CLUSTER_PREFIX + "_" + _className;
  private ClusterControllerManager _controller;
  private final List<MockParticipantManager> _participants = new ArrayList<>();
  private HelixDataAccessor _dataAccessor;
  private ConfigAccessor _configAccessor;

  @BeforeClass
  public void beforeClass() throws Exception {
    System.out.println("START " + _className + " at " + new Date(System.currentTimeMillis()));

    _gSetupTool.addCluster(_clusterName, true);
    for (int i = 0; i < NUM_NODE; i++) {
      String instanceName = PARTICIPANT_PREFIX + "_" + (START_PORT + i);
      _gSetupTool.addInstanceToCluster(_clusterName, instanceName);
      MockParticipantManager participant =
          new MockParticipantManager(ZK_ADDR, _clusterName, instanceName);
      participant.syncStart();
      _participants.add(participant);
    }

    String controllerName = CONTROLLER_PREFIX + "_0";
    _controller = new ClusterControllerManager(ZK_ADDR, _clusterName, controllerName);
    _controller.syncStart();

    enablePersistBestPossibleAssignment(_gZkClient, _clusterName, true);
    _dataAccessor = new ZKHelixDataAccessor(_clusterName, _baseAccessor);
    _configAccessor = new ConfigAccessor(_gZkClient);

    ClusterConfig clusterConfig = _configAccessor.getClusterConfig(_clusterName);
    clusterConfig.setMaxOfflineInstancesAllowed(MAX_OFFLINE_INSTANCES_ALLOWED);
    clusterConfig.setNumOfflineInstancesForAutoExit(0);
    _configAccessor.setClusterConfig(_clusterName, clusterConfig);

    createResourceWithDelayedRebalance(_clusterName, "Test-DB",
        BuiltInStateModelDefinitions.MasterSlave.name(), PARTITIONS, 3, 3, -1);

    ZkHelixClusterVerifier verifier =
        new BestPossibleExternalViewVerifier.Builder(_clusterName).setZkClient(_gZkClient)
            .setWaitTillVerify(TestHelper.DEFAULT_REBALANCE_PROCESSING_WAIT_TIME).build();
    Assert.assertTrue(verifier.verifyByPolling());
  }

  /**
   * Take two participants down. Without markers, the offline count of 2 exceeds the configured
   * MAX_OFFLINE_INSTANCES_ALLOWED of 1 and the cluster enters MM. With valid markers on both
   * downed instances, the count drops to 0 from the budget's perspective and MM is not entered.
   */
  @Test
  public void testValidMarkersPreventMMTrigger() throws Exception {
    Assert.assertNull(_dataAccessor.getProperty(_dataAccessor.keyBuilder().maintenance()));

    long futureExpiry = System.currentTimeMillis() + ONE_HOUR_MS;
    setMarker(_participants.get(0).getInstanceName(), futureExpiry);
    setMarker(_participants.get(1).getInstanceName(), futureExpiry);

    _participants.get(0).syncStop();
    _participants.get(1).syncStop();

    // MM must NOT trigger because both offline instances carry valid markers.
    boolean stayedOutOfMM = TestHelper.verify(() -> {
      MaintenanceSignal ms = _dataAccessor.getProperty(_dataAccessor.keyBuilder().maintenance());
      return ms == null;
    }, TestHelper.WAIT_DURATION);
    Assert.assertTrue(stayedOutOfMM,
        "Cluster entered MM despite both offline instances having valid instance-operation "
            + "maintenance markers");

    // Now bring a third instance down without a marker. Unmarked count = 1, equal to the budget,
    // so still no MM. Bring a fourth instance down (also unmarked) -> unmarked count exceeds
    // the budget and MM should fire.
    _participants.get(2).syncStop();
    _participants.get(3).syncStop();

    boolean enteredMM = TestHelper.verify(() -> {
      MaintenanceSignal ms = _dataAccessor.getProperty(_dataAccessor.keyBuilder().maintenance());
      return ms != null && ms.getReason() != null;
    }, TestHelper.WAIT_DURATION);
    Assert.assertTrue(enteredMM,
        "Cluster did not enter MM after two unplanned offlines exceeded the budget");

    restoreClusterState();
  }

  /**
   * An expired marker provides no exemption. Two offline instances with markers that have already
   * passed their TTL behave exactly like unmarked offlines and trigger MM.
   */
  @Test(dependsOnMethods = "testValidMarkersPreventMMTrigger")
  public void testExpiredMarkersDoNotExempt() throws Exception {
    Assert.assertNull(_dataAccessor.getProperty(_dataAccessor.keyBuilder().maintenance()));

    long pastExpiry = System.currentTimeMillis() - 1L;
    setMarker(_participants.get(0).getInstanceName(), pastExpiry);
    setMarker(_participants.get(1).getInstanceName(), pastExpiry);

    _participants.get(0).syncStop();
    _participants.get(1).syncStop();

    boolean enteredMM = TestHelper.verify(() -> {
      MaintenanceSignal ms = _dataAccessor.getProperty(_dataAccessor.keyBuilder().maintenance());
      return ms != null && ms.getReason() != null;
    }, TestHelper.WAIT_DURATION);
    Assert.assertTrue(enteredMM,
        "Expired markers must not exempt; cluster must enter MM as if the markers were absent");

    restoreClusterState();
  }

  private void setMarker(String instanceName, long expiresAtMillis) {
    InstanceConfig cfg = _configAccessor.getInstanceConfig(_clusterName, instanceName);
    cfg.setInstanceOperationMaintenanceUntilMs(expiresAtMillis);
    _configAccessor.setInstanceConfig(_clusterName, instanceName, cfg);
  }

  private void clearMarker(String instanceName) {
    InstanceConfig cfg = _configAccessor.getInstanceConfig(_clusterName, instanceName);
    cfg.setInstanceOperationMaintenanceUntilMs(
        InstanceConfig.INSTANCE_OPERATION_MAINTENANCE_NOT_SET);
    _configAccessor.setInstanceConfig(_clusterName, instanceName, cfg);
  }

  private void restoreClusterState() throws Exception {
    for (int i = 0; i < _participants.size(); i++) {
      MockParticipantManager p = _participants.get(i);
      if (!p.isConnected()) {
        MockParticipantManager replacement =
            new MockParticipantManager(ZK_ADDR, _clusterName, p.getInstanceName());
        replacement.syncStart();
        _participants.set(i, replacement);
      }
      clearMarker(_participants.get(i).getInstanceName());
    }
    if (_dataAccessor.getProperty(_dataAccessor.keyBuilder().maintenance()) != null) {
      _gSetupTool.getClusterManagementTool().enableMaintenanceMode(_clusterName, false);
    }
    ZkHelixClusterVerifier verifier =
        new BestPossibleExternalViewVerifier.Builder(_clusterName).setZkClient(_gZkClient)
            .setWaitTillVerify(TestHelper.DEFAULT_REBALANCE_PROCESSING_WAIT_TIME).build();
    Assert.assertTrue(verifier.verifyByPolling());
  }

  @AfterClass
  public void afterClass() throws Exception {
    _controller.syncStop();
    for (MockParticipantManager participant : _participants) {
      participant.syncStop();
    }
    deleteCluster(_clusterName);
    System.out.println("END " + _className + " at " + new Date(System.currentTimeMillis()));
  }
}
