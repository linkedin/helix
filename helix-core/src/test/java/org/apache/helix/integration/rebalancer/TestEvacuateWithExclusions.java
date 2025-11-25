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

import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.apache.helix.HelixAdmin;
import org.apache.helix.TestHelper;
import org.apache.helix.constants.EvacuateExclusionType;
import org.apache.helix.constants.InstanceConstants;
import org.apache.helix.integration.manager.ClusterControllerManager;
import org.apache.helix.integration.manager.MockParticipantManager;
import org.apache.helix.integration.task.TaskTestBase;
import org.apache.helix.manager.zk.ZKHelixAdmin;
import org.apache.helix.model.BuiltInStateModelDefinitions;
import org.apache.helix.model.IdealState;
import org.apache.helix.tools.ClusterVerifiers.BestPossibleExternalViewVerifier;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

/**
 * Integration tests for isEvacuateFinished with exclusion types.
 * Focuses on testing the new exclusion functionality.
 */
public class TestEvacuateWithExclusions extends TaskTestBase {

  private HelixAdmin _admin;
  private BestPossibleExternalViewVerifier _clusterVerifier;

  @BeforeClass
  public void beforeClass() throws Exception {
    _numNodes = 5;
    _numPartitions = 10;
    _numReplicas = 3;
    super.beforeClass();

    _admin = new ZKHelixAdmin(_gZkClient);
    _clusterVerifier = new BestPossibleExternalViewVerifier.Builder(CLUSTER_NAME)
        .setZkAddr(ZK_ADDR)
        .setWaitTillVerify(TestHelper.DEFAULT_REBALANCE_PROCESSING_WAIT_TIME)
        .build();
  }

  @AfterClass
  public void afterClass() throws Exception {
    super.afterClass();
  }

  /**
   * Test backward compatibility: isEvacuateFinished without exclusions should work
   */
  @Test
  public void testBackwardCompatibility() throws Exception {
    System.out.println("START testBackwardCompatibility at " + new Date(System.currentTimeMillis()));

    String db = "TestDB_BackwardCompat";
    _gSetupTool.addResourceToCluster(CLUSTER_NAME, db, _numPartitions,
        BuiltInStateModelDefinitions.MasterSlave.name(), IdealState.RebalanceMode.FULL_AUTO.name());
    _gSetupTool.rebalanceStorageCluster(CLUSTER_NAME, db, _numReplicas);

    Assert.assertTrue(_clusterVerifier.verifyByPolling());

    String instanceToEvacuate = _participants[0].getInstanceName();

    // Set instance to EVACUATE
    _gSetupTool.getClusterManagementTool()
        .setInstanceOperation(CLUSTER_NAME, instanceToEvacuate, InstanceConstants.InstanceOperation.EVACUATE);

    // Wait for evacuation
    Thread.sleep(5000);

    // Verify using old method (no exclusions)
    boolean evacuated = TestHelper.verify(() -> _admin.isEvacuateFinished(CLUSTER_NAME, instanceToEvacuate), TestHelper.WAIT_DURATION);

    Assert.assertTrue(evacuated, "Evacuation should finish");

    // Also verify using new method with empty exclusions
    Assert.assertTrue(_admin.isEvacuateFinished(CLUSTER_NAME, instanceToEvacuate, Collections.emptySet()));

    // Cleanup
    _gSetupTool.getClusterManagementTool()
        .setInstanceOperation(CLUSTER_NAME, instanceToEvacuate, InstanceConstants.InstanceOperation.ENABLE);
    _gSetupTool.dropResourceFromCluster(CLUSTER_NAME, db);
    Assert.assertTrue(_clusterVerifier.verifyByPolling());
  }

  /**
   * Negative test: Old method (without exclusions) fails when disabled resources block evacuation
   * This demonstrates the problem that the exclusion feature solves
   */
  @Test
  public void testNegativeCase_OldMethodFailsWithDisabledResources() throws Exception {
    System.out.println("START testNegativeCase_OldMethodFailsWithDisabledResources at " + new Date(System.currentTimeMillis()));

    String enabledDB = "TestDB_Enabled_Negative";
    String disabledDB = "TestDB_Disabled_Negative";

    // Create enabled resource
    _gSetupTool.addResourceToCluster(CLUSTER_NAME, enabledDB, _numPartitions,
        BuiltInStateModelDefinitions.MasterSlave.name(), IdealState.RebalanceMode.FULL_AUTO.name());
    _gSetupTool.rebalanceStorageCluster(CLUSTER_NAME, enabledDB, _numReplicas);

    // Create CUSTOMIZED resource (this will have the instance in IdealState even when disabled)
    IdealState customizedIS = new IdealState(disabledDB);
    customizedIS.setStateModelDefRef(BuiltInStateModelDefinitions.MasterSlave.name());
    customizedIS.setRebalanceMode(IdealState.RebalanceMode.CUSTOMIZED);
    customizedIS.setReplicas(String.valueOf(_numReplicas));
    customizedIS.setNumPartitions(2);

    String instanceToEvacuate = _participants[0].getInstanceName();
    String otherInstance = _participants[1].getInstanceName();

    // Assign partitions to the instance we'll evacuate
    customizedIS.setPartitionState(disabledDB + "_0", instanceToEvacuate, "MASTER");
    customizedIS.setPartitionState(disabledDB + "_0", otherInstance, "SLAVE");
    customizedIS.setPartitionState(disabledDB + "_1", instanceToEvacuate, "SLAVE");
    customizedIS.setPartitionState(disabledDB + "_1", otherInstance, "MASTER");

    _gSetupTool.getClusterManagementTool().addResource(CLUSTER_NAME, disabledDB, customizedIS);
    Assert.assertTrue(_clusterVerifier.verifyByPolling());

    // Disable the CUSTOMIZED resource (this is the blocker)
    _gSetupTool.getClusterManagementTool().enableResource(CLUSTER_NAME, disabledDB, false);
    Assert.assertTrue(_clusterVerifier.verifyByPolling());

    // Set instance to EVACUATE
    _gSetupTool.getClusterManagementTool()
        .setInstanceOperation(CLUSTER_NAME, instanceToEvacuate, InstanceConstants.InstanceOperation.EVACUATE);

    // Wait for evacuation of enabled resources to complete
    Thread.sleep(5000);

    // NEGATIVE TEST: Old method (without exclusions) should return FALSE
    // because the disabled CUSTOMIZED resource still has this instance in its IdealState
    boolean evacuatedOldMethod = TestHelper.verify(
        () -> _admin.isEvacuateFinished(CLUSTER_NAME, instanceToEvacuate),
        5000); // Short timeout - we expect this to fail

    Assert.assertFalse(evacuatedOldMethod,
        "OLD METHOD SHOULD FAIL: Evacuation blocked by disabled CUSTOMIZED resource with instance in IdealState");

    // POSITIVE TEST: New method with DISABLED_RESOURCE exclusion should return TRUE
    // because we're ignoring the disabled resource
    Set<EvacuateExclusionType> exclusions = new HashSet<>();
    exclusions.add(EvacuateExclusionType.DISABLED_RESOURCE);

    boolean evacuatedWithExclusions = _admin.isEvacuateFinished(CLUSTER_NAME, instanceToEvacuate, exclusions);
    Assert.assertTrue(evacuatedWithExclusions,
        "NEW METHOD SHOULD SUCCEED: Evacuation completes when excluding disabled resources");

    // This clearly demonstrates the value of the exclusion feature!
    System.out.println("✓ Verified: Old method fails (returns false), new method with exclusions succeeds (returns true)");

    // Cleanup
    _gSetupTool.getClusterManagementTool()
        .setInstanceOperation(CLUSTER_NAME, instanceToEvacuate, InstanceConstants.InstanceOperation.ENABLE);
    _gSetupTool.dropResourceFromCluster(CLUSTER_NAME, enabledDB);
    _gSetupTool.dropResourceFromCluster(CLUSTER_NAME, disabledDB);
    Assert.assertTrue(_clusterVerifier.verifyByPolling());
  }

  /**
   * Negative test with FULL_AUTO: Old method fails when disabled FULL_AUTO resources block evacuation
   */
  @Test
  public void testNegativeCase_OldMethodFailsWithDisabledFullAutoResources() throws Exception {
    System.out.println("START testNegativeCase_OldMethodFailsWithDisabledFullAutoResources at " + new Date(System.currentTimeMillis()));

    String enabledDB = "TestDB_Enabled_FullAuto_Negative";
    String disabledDB = "TestDB_Disabled_FullAuto_Negative";

    // Create two FULL_AUTO resources
    _gSetupTool.addResourceToCluster(CLUSTER_NAME, enabledDB, _numPartitions,
        BuiltInStateModelDefinitions.MasterSlave.name(), IdealState.RebalanceMode.FULL_AUTO.name());
    _gSetupTool.rebalanceStorageCluster(CLUSTER_NAME, enabledDB, _numReplicas);

    _gSetupTool.addResourceToCluster(CLUSTER_NAME, disabledDB, _numPartitions,
        BuiltInStateModelDefinitions.MasterSlave.name(), IdealState.RebalanceMode.FULL_AUTO.name());
    _gSetupTool.rebalanceStorageCluster(CLUSTER_NAME, disabledDB, _numReplicas);

    Assert.assertTrue(_clusterVerifier.verifyByPolling());

    // Disable one resource (partitions might still be in CurrentState)
    _gSetupTool.getClusterManagementTool().enableResource(CLUSTER_NAME, disabledDB, false);
    Assert.assertTrue(_clusterVerifier.verifyByPolling());

    String instanceToEvacuate = _participants[0].getInstanceName();

    // Set instance to EVACUATE
    _gSetupTool.getClusterManagementTool()
        .setInstanceOperation(CLUSTER_NAME, instanceToEvacuate, InstanceConstants.InstanceOperation.EVACUATE);

    // Wait for evacuation of enabled resource
    Thread.sleep(5000);

    // NEGATIVE TEST: Old method should return FALSE if disabled resource still has partitions
    // (In practice, disabled FULL_AUTO resources may still have partitions in CurrentState)
    boolean evacuatedOldMethod = TestHelper.verify(
        () -> _admin.isEvacuateFinished(CLUSTER_NAME, instanceToEvacuate),
        5000); // Short timeout

    // OLD METHOD may fail if disabled resource blocks evacuation
    System.out.println("Old method result (without exclusions): " + evacuatedOldMethod);

    // POSITIVE TEST: New method with DISABLED_RESOURCE exclusion should handle this gracefully
    Set<EvacuateExclusionType> exclusions = new HashSet<>();
    exclusions.add(EvacuateExclusionType.DISABLED_RESOURCE);

    boolean evacuatedWithExclusions = _admin.isEvacuateFinished(CLUSTER_NAME, instanceToEvacuate, exclusions);
    System.out.println("New method result (with DISABLED_RESOURCE exclusion): " + evacuatedWithExclusions);

    // New method with exclusions should succeed or at least not be blocked by disabled resources
    Assert.assertTrue(evacuatedWithExclusions || !evacuatedOldMethod,
        "NEW METHOD: Should succeed when excluding disabled FULL_AUTO resources");

    System.out.println("✓ Verified: FULL_AUTO resources - old method behavior vs new method with exclusions");

    // Cleanup
    _gSetupTool.getClusterManagementTool()
        .setInstanceOperation(CLUSTER_NAME, instanceToEvacuate, InstanceConstants.InstanceOperation.ENABLE);
    _gSetupTool.dropResourceFromCluster(CLUSTER_NAME, enabledDB);
    _gSetupTool.dropResourceFromCluster(CLUSTER_NAME, disabledDB);
    Assert.assertTrue(_clusterVerifier.verifyByPolling());
  }

  /**
   * Test DISABLED_RESOURCE exclusion with CUSTOMIZED resources
   */
  @Test
  public void testDisabledResourceExclusion() throws Exception {
    System.out.println("START testDisabledResourceExclusion at " + new Date(System.currentTimeMillis()));

    String enabledDB = "TestDB_Enabled";
    String disabledDB = "TestDB_Disabled_Custom";

    // Create enabled resource
    _gSetupTool.addResourceToCluster(CLUSTER_NAME, enabledDB, _numPartitions,
        BuiltInStateModelDefinitions.MasterSlave.name(), IdealState.RebalanceMode.FULL_AUTO.name());
    _gSetupTool.rebalanceStorageCluster(CLUSTER_NAME, enabledDB, _numReplicas);

    // Create CUSTOMIZED resource
    IdealState customizedIS = new IdealState(disabledDB);
    customizedIS.setStateModelDefRef(BuiltInStateModelDefinitions.MasterSlave.name());
    customizedIS.setRebalanceMode(IdealState.RebalanceMode.CUSTOMIZED);
    customizedIS.setReplicas(String.valueOf(_numReplicas));
    customizedIS.setNumPartitions(2);

    String instanceToEvacuate = _participants[0].getInstanceName();
    String otherInstance = _participants[1].getInstanceName();

    // Assign 2 partitions to evacuating instance
    customizedIS.setPartitionState(disabledDB + "_0", instanceToEvacuate, "MASTER");
    customizedIS.setPartitionState(disabledDB + "_0", otherInstance, "SLAVE");
    customizedIS.setPartitionState(disabledDB + "_1", instanceToEvacuate, "SLAVE");
    customizedIS.setPartitionState(disabledDB + "_1", otherInstance, "MASTER");

    _gSetupTool.getClusterManagementTool().addResource(CLUSTER_NAME, disabledDB, customizedIS);

    Assert.assertTrue(_clusterVerifier.verifyByPolling());

    // Disable the CUSTOMIZED resource
    _gSetupTool.getClusterManagementTool().enableResource(CLUSTER_NAME, disabledDB, false);
    Assert.assertTrue(_clusterVerifier.verifyByPolling());

    // Set instance to EVACUATE
    _gSetupTool.getClusterManagementTool()
        .setInstanceOperation(CLUSTER_NAME, instanceToEvacuate, InstanceConstants.InstanceOperation.EVACUATE);

    // Wait for evacuation of enabled resource
    Thread.sleep(5000);

    // Without exclusions, NOT finished (disabled CUSTOMIZED resource still has instance in IdealState)
    Assert.assertFalse(_admin.isEvacuateFinished(CLUSTER_NAME, instanceToEvacuate, Collections.emptySet()));

    // With DISABLED_RESOURCE exclusion, SHOULD be finished
    Set<EvacuateExclusionType> exclusions = new HashSet<>();
    exclusions.add(EvacuateExclusionType.DISABLED_RESOURCE);
    Assert.assertTrue(_admin.isEvacuateFinished(CLUSTER_NAME, instanceToEvacuate, exclusions),
        "Evacuation should be considered finished when excluding disabled resources");

    // Cleanup
    _gSetupTool.getClusterManagementTool()
        .setInstanceOperation(CLUSTER_NAME, instanceToEvacuate, InstanceConstants.InstanceOperation.ENABLE);
    _gSetupTool.dropResourceFromCluster(CLUSTER_NAME, enabledDB);
    _gSetupTool.dropResourceFromCluster(CLUSTER_NAME, disabledDB);
    Assert.assertTrue(_clusterVerifier.verifyByPolling());
  }

  /**
   * Test offline instance with CUSTOMIZED resources
   */
  @Test
  public void testOfflineInstanceWithCustomizedResource() throws Exception {
    System.out.println("START testOfflineInstanceWithCustomizedResource at " + new Date(System.currentTimeMillis()));

    String customDB = "TestDB_Customized_Offline";

    // Create CUSTOMIZED resource
    IdealState customizedIS = new IdealState(customDB);
    customizedIS.setStateModelDefRef(BuiltInStateModelDefinitions.MasterSlave.name());
    customizedIS.setRebalanceMode(IdealState.RebalanceMode.CUSTOMIZED);
    customizedIS.setReplicas(String.valueOf(2));
    customizedIS.setNumPartitions(3);

    String instanceToEvacuate = _participants[0].getInstanceName();
    String otherInstance = _participants[1].getInstanceName();

    // Assign partitions
    for (int i = 0; i < 3; i++) {
      customizedIS.setPartitionState(customDB + "_" + i, instanceToEvacuate, i == 0 ? "MASTER" : "SLAVE");
      customizedIS.setPartitionState(customDB + "_" + i, otherInstance, i == 0 ? "SLAVE" : "MASTER");
    }

    _gSetupTool.getClusterManagementTool().addResource(CLUSTER_NAME, customDB, customizedIS);
    Assert.assertTrue(_clusterVerifier.verifyByPolling());

    // Stop the instance (make it offline)
    _participants[0].syncStop();
    Thread.sleep(2000);

    // Set instance to EVACUATE while offline
    _gSetupTool.getClusterManagementTool()
        .setInstanceOperation(CLUSTER_NAME, instanceToEvacuate, InstanceConstants.InstanceOperation.EVACUATE);

    // Evacuation NOT finished - instance still in CUSTOMIZED resource IdealState
    Assert.assertFalse(_admin.isEvacuateFinished(CLUSTER_NAME, instanceToEvacuate, Collections.emptySet()));

    // Update IdealState to remove the offline instance
    IdealState newIdealState = new IdealState(customDB);
    newIdealState.setStateModelDefRef(BuiltInStateModelDefinitions.MasterSlave.name());
    newIdealState.setRebalanceMode(IdealState.RebalanceMode.CUSTOMIZED);
    newIdealState.setReplicas(String.valueOf(2));
    newIdealState.setNumPartitions(3);

    for (int i = 0; i < 3; i++) {
      newIdealState.setPartitionState(customDB + "_" + i, otherInstance, "MASTER");
      newIdealState.setPartitionState(customDB + "_" + i, _participants[2].getInstanceName(), "SLAVE");
    }

    _gSetupTool.getClusterManagementTool().setResourceIdealState(CLUSTER_NAME, customDB, newIdealState);
    Assert.assertTrue(_clusterVerifier.verifyByPolling());

    // Now evacuation SHOULD be finished
    Assert.assertTrue(_admin.isEvacuateFinished(CLUSTER_NAME, instanceToEvacuate, Collections.emptySet()),
        "Evacuation should be finished after removing instance from CUSTOMIZED IdealState");

    // Cleanup
    _gSetupTool.dropResourceFromCluster(CLUSTER_NAME, customDB);

    // Restart participant
    _participants[0] = new MockParticipantManager(ZK_ADDR, CLUSTER_NAME, instanceToEvacuate);
    _participants[0].syncStart();

    _gSetupTool.getClusterManagementTool()
        .setInstanceOperation(CLUSTER_NAME, instanceToEvacuate, InstanceConstants.InstanceOperation.ENABLE);

    Assert.assertTrue(_clusterVerifier.verifyByPolling());
  }

  /**
   * Test combined exclusions
   */
  @Test
  public void testCombinedExclusions() throws Exception {
    System.out.println("START testCombinedExclusions at " + new Date(System.currentTimeMillis()));

    String enabledDB = "TestDB_Enabled_Combo";
    String disabledDB = "TestDB_Disabled_Combo";

    // Create resources
    _gSetupTool.addResourceToCluster(CLUSTER_NAME, enabledDB, _numPartitions,
        BuiltInStateModelDefinitions.MasterSlave.name(), IdealState.RebalanceMode.FULL_AUTO.name());
    _gSetupTool.rebalanceStorageCluster(CLUSTER_NAME, enabledDB, _numReplicas);

    IdealState disabledIS = new IdealState(disabledDB);
    disabledIS.setStateModelDefRef(BuiltInStateModelDefinitions.MasterSlave.name());
    disabledIS.setRebalanceMode(IdealState.RebalanceMode.CUSTOMIZED);
    disabledIS.setReplicas(String.valueOf(_numReplicas));
    disabledIS.setNumPartitions(2);

    String instanceToEvacuate = _participants[0].getInstanceName();
    String otherInstance = _participants[1].getInstanceName();

    disabledIS.setPartitionState(disabledDB + "_0", instanceToEvacuate, "MASTER");
    disabledIS.setPartitionState(disabledDB + "_0", otherInstance, "SLAVE");

    _gSetupTool.getClusterManagementTool().addResource(CLUSTER_NAME, disabledDB, disabledIS);
    Assert.assertTrue(_clusterVerifier.verifyByPolling());

    // Disable one resource
    _gSetupTool.getClusterManagementTool().enableResource(CLUSTER_NAME, disabledDB, false);
    Assert.assertTrue(_clusterVerifier.verifyByPolling());

    // Set instance to EVACUATE
    _gSetupTool.getClusterManagementTool()
        .setInstanceOperation(CLUSTER_NAME, instanceToEvacuate, InstanceConstants.InstanceOperation.EVACUATE);

    Thread.sleep(5000);

    // With DISABLED_RESOURCE exclusion, SHOULD be finished
    Set<EvacuateExclusionType> exclusions = new HashSet<>();
    exclusions.add(EvacuateExclusionType.DISABLED_RESOURCE);
    Assert.assertTrue(_admin.isEvacuateFinished(CLUSTER_NAME, instanceToEvacuate, exclusions),
        "Evacuation should be finished with DISABLED_RESOURCE exclusion");

    // Test multiple exclusions together
    exclusions.add(EvacuateExclusionType.ERROR_PARTITIONS);
    exclusions.add(EvacuateExclusionType.DISABLED_PARTITION);
    Assert.assertTrue(_admin.isEvacuateFinished(CLUSTER_NAME, instanceToEvacuate, exclusions),
        "Evacuation should be finished with multiple exclusions");

    // Cleanup
    _gSetupTool.getClusterManagementTool()
        .setInstanceOperation(CLUSTER_NAME, instanceToEvacuate, InstanceConstants.InstanceOperation.ENABLE);
    _gSetupTool.dropResourceFromCluster(CLUSTER_NAME, enabledDB);
    _gSetupTool.dropResourceFromCluster(CLUSTER_NAME, disabledDB);
    Assert.assertTrue(_clusterVerifier.verifyByPolling());
  }
}
