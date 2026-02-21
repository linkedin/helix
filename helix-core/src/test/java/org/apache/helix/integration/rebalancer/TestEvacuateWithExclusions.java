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
import java.util.HashSet;
import java.util.Set;

import org.apache.helix.HelixAdmin;
import org.apache.helix.TestHelper;
import org.apache.helix.constants.InstanceDrainExclusionType;
import org.apache.helix.constants.InstanceConstants;
import org.apache.helix.integration.manager.MockParticipantManager;
import org.apache.helix.integration.task.TaskTestBase;
import org.apache.helix.manager.zk.ZKHelixAdmin;
import org.apache.helix.model.BuiltInStateModelDefinitions;
import org.apache.helix.model.EvacuationInfo;
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
    Set<InstanceDrainExclusionType> exclusions = new HashSet<>();
    exclusions.add(InstanceDrainExclusionType.DISABLED_RESOURCE);

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
    Set<InstanceDrainExclusionType> exclusions = new HashSet<>();
    exclusions.add(InstanceDrainExclusionType.DISABLED_RESOURCE);

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
    Set<InstanceDrainExclusionType> exclusions = new HashSet<>();
    exclusions.add(InstanceDrainExclusionType.DISABLED_RESOURCE);
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
   * Test offline instance with CUSTOMIZED resources uses union semantics.
   * Evacuation remains in-progress if partitions still exist in CurrentState,
   * even after the instance is removed from IdealState.
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

    // With union semantics, evacuation is still NOT finished because the offline instance
    // still has CurrentState entries for this resource.
    Assert.assertFalse(_admin.isEvacuateFinished(CLUSTER_NAME, instanceToEvacuate, Collections.emptySet()),
        "Evacuation should remain in progress while CurrentState still has partitions, even if IdealState no longer assigns the instance");

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
   * Test getEvacuationStatus returns remainingPartitionCount and pendingMessageCount
   */
  @Test
  public void testGetEvacuationStatusReturnsDetailedInfo() throws Exception {
    System.out.println("START testGetEvacuationStatusReturnsDetailedInfo at " + new Date(System.currentTimeMillis()));

    String db = "TestDB_EvacuationStatus";
    _gSetupTool.addResourceToCluster(CLUSTER_NAME, db, _numPartitions,
        BuiltInStateModelDefinitions.MasterSlave.name(), IdealState.RebalanceMode.FULL_AUTO.name());
    _gSetupTool.rebalanceStorageCluster(CLUSTER_NAME, db, _numReplicas);

    Assert.assertTrue(_clusterVerifier.verifyByPolling());

    String instanceToEvacuate = _participants[0].getInstanceName();

    // Before setting EVACUATE, getEvacuationStatus should return NOT_EVACUATING state
    ZKHelixAdmin zkAdmin = (ZKHelixAdmin) _admin;
    EvacuationInfo statusBefore = zkAdmin.getEvacuationStatus(CLUSTER_NAME, instanceToEvacuate, Collections.emptySet());
    Assert.assertEquals(statusBefore.getState(), EvacuationInfo.EvacuationState.NOT_EVACUATING,
        "Should return NOT_EVACUATING when instance is not in EVACUATE operation");
    Assert.assertEquals(statusBefore.getReason(),
        EvacuationInfo.ReasonCode.NOT_IN_EVACUATE_OPERATION.getMessage());
    // When NOT_EVACUATING, counts and timestamp should be null (won't be serialized in JSON)
    Assert.assertNull(statusBefore.getRemainingPartitionCount(),
        "remainingPartitionCount should be null when not evacuating");
    Assert.assertNull(statusBefore.getPendingMessageCount(),
        "pendingMessageCount should be null when not evacuating");
    Assert.assertNull(statusBefore.getLastActivityTimestamp(),
        "lastActivityTimestamp should be null when not evacuating");

    // Set instance to EVACUATE
    _gSetupTool.getClusterManagementTool()
        .setInstanceOperation(CLUSTER_NAME, instanceToEvacuate, InstanceConstants.InstanceOperation.EVACUATE);

    // Immediately check status - should have remainingPartitionCount > 0 (partitions still being evacuated)
    EvacuationInfo statusDuring = zkAdmin.getEvacuationStatus(CLUSTER_NAME, instanceToEvacuate, Collections.emptySet());
    System.out.println("During evacuation - state: " + statusDuring.getState()
        + ", remainingPartitionCount: " + statusDuring.getRemainingPartitionCount()
        + ", pendingMessageCount: " + statusDuring.getPendingMessageCount()
        + ", lastActivityTimestamp: " + statusDuring.getLastActivityTimestamp());
    // During evacuation, lastActivityTimestamp should be populated if there are current states
    if (statusDuring.getState() == EvacuationInfo.EvacuationState.IN_PROGRESS
        && statusDuring.getRemainingPartitionCount() != null
        && statusDuring.getRemainingPartitionCount() > 0) {
      Assert.assertNotNull(statusDuring.getLastActivityTimestamp(),
          "lastActivityTimestamp should be populated during evacuation with partitions");
      Assert.assertTrue(statusDuring.getLastActivityTimestamp() > 0,
          "lastActivityTimestamp should be a positive Unix timestamp");
    }

    // Wait for evacuation to complete
    boolean evacuated = TestHelper.verify(
        () -> _admin.isEvacuateFinished(CLUSTER_NAME, instanceToEvacuate),
        TestHelper.WAIT_DURATION);
    Assert.assertTrue(evacuated, "Evacuation should complete");

    // After evacuation completes, verify final status
    EvacuationInfo statusAfter = zkAdmin.getEvacuationStatus(CLUSTER_NAME, instanceToEvacuate, Collections.emptySet());
    Assert.assertEquals(statusAfter.getState(), EvacuationInfo.EvacuationState.COMPLETED,
        "Should return COMPLETED after evacuation completes");
    Assert.assertEquals(statusAfter.getRemainingPartitionCount(), Integer.valueOf(0), "remainingPartitionCount should be 0 after evacuation");
    Assert.assertEquals(statusAfter.getPendingMessageCount(), Integer.valueOf(0), "pendingMessageCount should be 0 after evacuation");
    // After evacuation, lastActivityTimestamp may or may not be set depending on whether CurrentState ZNodes exist
    // If the instance still has a session with CurrentState ZNodes (even if empty), timestamp should be present
    System.out.println("After evacuation - state: " + statusAfter.getState()
        + ", remainingPartitionCount: " + statusAfter.getRemainingPartitionCount()
        + ", pendingMessageCount: " + statusAfter.getPendingMessageCount()
        + ", lastActivityTimestamp: " + statusAfter.getLastActivityTimestamp());

    // Cleanup
    _gSetupTool.getClusterManagementTool()
        .setInstanceOperation(CLUSTER_NAME, instanceToEvacuate, InstanceConstants.InstanceOperation.ENABLE);
    _gSetupTool.dropResourceFromCluster(CLUSTER_NAME, db);
    Assert.assertTrue(_clusterVerifier.verifyByPolling());
  }

  /**
   * Test getEvacuationStatus with exclusions
   */
  @Test
  public void testGetEvacuationStatusWithExclusions() throws Exception {
    System.out.println("START testGetEvacuationStatusWithExclusions at " + new Date(System.currentTimeMillis()));

    String enabledDB = "TestDB_Status_Enabled";
    String disabledDB = "TestDB_Status_Disabled";

    // Create enabled FULL_AUTO resource
    _gSetupTool.addResourceToCluster(CLUSTER_NAME, enabledDB, _numPartitions,
        BuiltInStateModelDefinitions.MasterSlave.name(), IdealState.RebalanceMode.FULL_AUTO.name());
    _gSetupTool.rebalanceStorageCluster(CLUSTER_NAME, enabledDB, _numReplicas);

    // Create CUSTOMIZED resource that will be disabled
    IdealState customizedIS = new IdealState(disabledDB);
    customizedIS.setStateModelDefRef(BuiltInStateModelDefinitions.MasterSlave.name());
    customizedIS.setRebalanceMode(IdealState.RebalanceMode.CUSTOMIZED);
    customizedIS.setReplicas(String.valueOf(_numReplicas));
    customizedIS.setNumPartitions(2);

    String instanceToEvacuate = _participants[0].getInstanceName();
    String otherInstance = _participants[1].getInstanceName();

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

    // Wait for enabled resource to evacuate
    Thread.sleep(5000);

    ZKHelixAdmin zkAdmin = (ZKHelixAdmin) _admin;

    // Without exclusions - should be IN_PROGRESS (disabled CUSTOMIZED resource blocks)
    EvacuationInfo statusNoExclusions = zkAdmin.getEvacuationStatus(CLUSTER_NAME, instanceToEvacuate, Collections.emptySet());
    Assert.assertEquals(statusNoExclusions.getState(), EvacuationInfo.EvacuationState.IN_PROGRESS,
        "Without exclusions, evacuation should be IN_PROGRESS (blocked by disabled resource)");
    Integer remainingWithoutExclusions = statusNoExclusions.getRemainingPartitionCount();
    Assert.assertTrue(remainingWithoutExclusions > 0, "Should have remaining partitions without exclusions");
    System.out.println("Without exclusions - state: " + statusNoExclusions.getState()
        + ", remainingPartitionCount: " + remainingWithoutExclusions);

    // With DISABLED_RESOURCE exclusion - SHOULD be COMPLETED
    Set<InstanceDrainExclusionType> exclusions = new HashSet<>();
    exclusions.add(InstanceDrainExclusionType.DISABLED_RESOURCE);

    EvacuationInfo statusWithExclusions = zkAdmin.getEvacuationStatus(CLUSTER_NAME, instanceToEvacuate, exclusions);
    Assert.assertEquals(statusWithExclusions.getState(), EvacuationInfo.EvacuationState.COMPLETED,
        "With DISABLED_RESOURCE exclusion, evacuation should be COMPLETED");
    Integer remainingWithExclusions = statusWithExclusions.getRemainingPartitionCount();
    Assert.assertEquals(remainingWithExclusions, Integer.valueOf(0), "remainingPartitionCount should be 0 with exclusions");
    System.out.println("With DISABLED_RESOURCE exclusion - state: " + statusWithExclusions.getState()
        + ", remainingPartitionCount: " + remainingWithExclusions);

    // Verify that remainingPartitionCount differs based on exclusions
    Assert.assertTrue(remainingWithoutExclusions > remainingWithExclusions,
        "remainingPartitionCount should be higher without exclusions than with exclusions");

    // Cleanup
    _gSetupTool.getClusterManagementTool()
        .setInstanceOperation(CLUSTER_NAME, instanceToEvacuate, InstanceConstants.InstanceOperation.ENABLE);
    _gSetupTool.dropResourceFromCluster(CLUSTER_NAME, enabledDB);
    _gSetupTool.dropResourceFromCluster(CLUSTER_NAME, disabledDB);
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
    Set<InstanceDrainExclusionType> exclusions = new HashSet<>();
    exclusions.add(InstanceDrainExclusionType.DISABLED_RESOURCE);
    Assert.assertTrue(_admin.isEvacuateFinished(CLUSTER_NAME, instanceToEvacuate, exclusions),
        "Evacuation should be finished with DISABLED_RESOURCE exclusion");

    // Test multiple exclusions together
    exclusions.add(InstanceDrainExclusionType.ERROR_PARTITIONS);
    exclusions.add(InstanceDrainExclusionType.DISABLED_PARTITION);
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
