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
import org.apache.helix.constants.InstanceConstants;
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
 * from the cluster-wide offline budget that drives auto Maintenance Mode -- at both entry
 * (MAX_OFFLINE_INSTANCES_ALLOWED) and exit (NUM_OFFLINE_INSTANCES_FOR_AUTO_EXIT) -- while
 * preserving the trigger for unplanned losses. Also covers the EVACUATE auto-exit path,
 * which previously diverged from entry and could oscillate the cluster in and out of MM.
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

  /**
   * Covers the DISABLE operation: an instance with operation = DISABLE is in the MM-entry
   * baseline (DISABLE is a member of {ENABLE, DISABLE, EVACUATE}) but absent from
   * enabledLive (DISABLE removes it), so without a marker it counts toward
   * MAX_OFFLINE_INSTANCES_ALLOWED. The marker should exempt it.
   */
  @Test(dependsOnMethods = "testExpiredMarkersDoNotExempt")
  public void testMarkedDisabledInstanceDoesNotTripMM() throws Exception {
    Assert.assertNull(_dataAccessor.getProperty(_dataAccessor.keyBuilder().maintenance()));

    long futureExpiry = System.currentTimeMillis() + ONE_HOUR_MS;
    String h0 = _participants.get(0).getInstanceName();
    setMarker(h0, futureExpiry);
    _gSetupTool.getClusterManagementTool().enableInstance(_clusterName, h0, false);

    boolean stayedOutOfMM = TestHelper.verify(() -> {
      MaintenanceSignal ms = _dataAccessor.getProperty(_dataAccessor.keyBuilder().maintenance());
      return ms == null;
    }, TestHelper.WAIT_DURATION);
    Assert.assertTrue(stayedOutOfMM,
        "Marker must exempt a DISABLE+offline instance from the offline budget at MM entry");

    // Restore: re-enable h0 and clear the marker.
    _gSetupTool.getClusterManagementTool().enableInstance(_clusterName, h0, true);
    restoreClusterState();
  }

  /**
   * Covers the EVACUATE operation: an instance with operation = EVACUATE is in the MM-entry
   * baseline and not in enabledLive, so it counts toward the budget without a marker. The
   * marker should exempt it from both MM entry and MM exit thresholds, which both now
   * delegate to the shared offline-budget accessor.
   */
  @Test(dependsOnMethods = "testMarkedDisabledInstanceDoesNotTripMM")
  public void testMarkedEvacuatingInstanceDoesNotTripMM() throws Exception {
    Assert.assertNull(_dataAccessor.getProperty(_dataAccessor.keyBuilder().maintenance()));

    long futureExpiry = System.currentTimeMillis() + ONE_HOUR_MS;
    String h0 = _participants.get(0).getInstanceName();
    setMarker(h0, futureExpiry);
    _gSetupTool.getClusterManagementTool().setInstanceOperation(_clusterName, h0,
        InstanceConstants.InstanceOperation.EVACUATE);

    boolean stayedOutOfMM = TestHelper.verify(() -> {
      MaintenanceSignal ms = _dataAccessor.getProperty(_dataAccessor.keyBuilder().maintenance());
      return ms == null;
    }, TestHelper.WAIT_DURATION);
    Assert.assertTrue(stayedOutOfMM,
        "Marker must exempt an EVACUATE+offline instance from the offline budget at MM entry");

    // Restore: return to ENABLE and clear the marker.
    _gSetupTool.getClusterManagementTool().setInstanceOperation(_clusterName, h0,
        InstanceConstants.InstanceOperation.ENABLE);
    restoreClusterState();
  }

  /**
   * Locks in the auto-exit half of the entry/exit consistency fix. An unmarked EVACUATE
   * instance counts toward the offline budget at MM entry (it always did) and now also
   * counts at MM exit (this PR's behavior change). So while the EVACUATE instance is
   * present, the cluster must stay in MM even after the original triggering condition
   * clears -- previously the exit-side baseline excluded EVACUATE and the cluster would
   * oscillate in and out of MM on every pipeline tick.
   *
   * <p>Sequence:
   * <ol>
   *   <li>Put h0 into EVACUATE (no marker). Entry count = 1, which equals
   *       MAX_OFFLINE_INSTANCES_ALLOWED, so MM does not enter yet.</li>
   *   <li>Stop h1 (no EVACUATE, no marker). Entry count = 2 &gt; 1 -- MM enters.</li>
   *   <li>Restart h1. Entry count drops to 1, but MaintenanceRecoveryStage is the only
   *       path that can auto-exit. With the shared accessor, exit sees the EVACUATE
   *       instance and reports count = 1 &gt; NUM_OFFLINE_INSTANCES_FOR_AUTO_EXIT (0),
   *       so it must hold MM.</li>
   *   <li>Flip h0 back to ENABLE. Exit count drops to 0 and the cluster auto-exits.</li>
   * </ol>
   */
  @Test(dependsOnMethods = "testMarkedEvacuatingInstanceDoesNotTripMM")
  public void testUnmarkedEvacuatingInstanceHoldsMMUntilCleared() throws Exception {
    Assert.assertNull(_dataAccessor.getProperty(_dataAccessor.keyBuilder().maintenance()));

    String h0 = _participants.get(0).getInstanceName();
    _gSetupTool.getClusterManagementTool().setInstanceOperation(_clusterName, h0,
        InstanceConstants.InstanceOperation.EVACUATE);

    // h0 alone is at the boundary; MM does not enter yet.
    Assert.assertNull(_dataAccessor.getProperty(_dataAccessor.keyBuilder().maintenance()));

    // Stop h1 -- now offline budget is 2 (EVACUATE h0 + offline h1), which exceeds
    // MAX_OFFLINE_INSTANCES_ALLOWED = 1, so MM fires.
    _participants.get(1).syncStop();
    boolean enteredMM = TestHelper.verify(() -> {
      MaintenanceSignal ms = _dataAccessor.getProperty(_dataAccessor.keyBuilder().maintenance());
      return ms != null && ms.getReason() != null;
    }, TestHelper.WAIT_DURATION);
    Assert.assertTrue(enteredMM,
        "MM must enter once EVACUATE + an unplanned offline exceed the budget");

    // Bring h1 back. With pre-fix behavior MM would auto-exit immediately because the
    // exit path ignored EVACUATE; with the shared accessor, exit still sees one
    // (EVACUATE) instance > the exit threshold of 0, so MM must stay on.
    MockParticipantManager h1Replacement = new MockParticipantManager(ZK_ADDR, _clusterName,
        _participants.get(1).getInstanceName());
    h1Replacement.syncStart();
    _participants.set(1, h1Replacement);
    boolean stayedInMM = TestHelper.verify(() -> {
      MaintenanceSignal ms = _dataAccessor.getProperty(_dataAccessor.keyBuilder().maintenance());
      return ms != null;
    }, TestHelper.WAIT_DURATION);
    Assert.assertTrue(stayedInMM,
        "MM must hold while an unmarked EVACUATE instance is still in the offline budget; "
            + "without the shared accessor the exit path would have ignored it and "
            + "auto-exited prematurely");

    // Flip h0 back to ENABLE. Exit count drops to 0 (h0 is live again and routable)
    // and MM auto-exits.
    _gSetupTool.getClusterManagementTool().setInstanceOperation(_clusterName, h0,
        InstanceConstants.InstanceOperation.ENABLE);
    boolean exitedMM = TestHelper.verify(() -> {
      MaintenanceSignal ms = _dataAccessor.getProperty(_dataAccessor.keyBuilder().maintenance());
      return ms == null;
    }, TestHelper.WAIT_DURATION);
    Assert.assertTrue(exitedMM,
        "MM must auto-exit once the EVACUATE op clears and the offline budget drops to 0");

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
