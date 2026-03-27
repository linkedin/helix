package org.apache.helix.integration.rebalancer;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.helix.HelixException;
import org.apache.helix.constants.InstanceConstants;
import org.apache.helix.controller.rebalancer.strategy.CrushEdRebalanceStrategy;
import org.apache.helix.integration.manager.MockParticipantManager;
import org.apache.helix.model.ExternalView;
import org.apache.helix.model.IdealState;
import org.apache.helix.model.InstanceConfig;
import org.testng.Assert;
import org.testng.annotations.Test;


public class TestInstanceOperationMisc extends TestInstanceOperationBase {

  @Test(expectedExceptions = HelixException.class)
  public void testUnsetInstanceOperationOnSwapInWhenSwapping() throws Exception {
    System.out.println(
        "START TestInstanceOperationMisc.testUnsetInstanceOperationOnSwapInWhenSwapping() at "
            + new Date(System.currentTimeMillis()));

    enabledTopologyAwareRebalance();

    String instanceToSwapOutName = _participants.get(0).getInstanceName();
    InstanceConfig instanceToSwapOutInstanceConfig = _gSetupTool.getClusterManagementTool()
        .getInstanceConfig(CLUSTER_NAME, instanceToSwapOutName);

    String instanceToSwapInName = PARTICIPANT_PREFIX + "_" + _nextStartPort.get();
    addParticipant(instanceToSwapInName, instanceToSwapOutInstanceConfig.getLogicalId(LOGICAL_ID),
        instanceToSwapOutInstanceConfig.getDomainAsMap().get(ZONE),
        InstanceConstants.InstanceOperation.SWAP_IN, -1);

    Assert.assertTrue(_bestPossibleClusterVerifier.verifyByPolling());

    // Should throw HelixException: cannot ENABLE a SWAP_IN instance while
    // another instance with the same logicalId is still active.
    _gSetupTool.getClusterManagementTool()
        .setInstanceOperation(CLUSTER_NAME, instanceToSwapInName,
            InstanceConstants.InstanceOperation.ENABLE);
  }

  @Test
  public void testNodeSwapAddSwapInFirst() throws Exception {
    System.out.println(
        "START TestInstanceOperationMisc.testNodeSwapAddSwapInFirst() at " + new Date(
            System.currentTimeMillis()));

    enabledTopologyAwareRebalance();

    String instanceToSwapOutName = _participants.get(0).getInstanceName();
    InstanceConfig instanceToSwapOutInstanceConfig = _gSetupTool.getClusterManagementTool()
        .getInstanceConfig(CLUSTER_NAME, instanceToSwapOutName);

    String instanceToSwapInName = PARTICIPANT_PREFIX + "_" + _nextStartPort.get();
    addParticipant(instanceToSwapInName, instanceToSwapOutInstanceConfig.getLogicalId(LOGICAL_ID),
        instanceToSwapOutInstanceConfig.getDomainAsMap().get(ZONE),
        InstanceConstants.InstanceOperation.SWAP_IN, -1);

    Assert.assertTrue(_bestPossibleClusterVerifier.verifyByPolling());
  }

  @Test(expectedExceptions = HelixException.class)
  public void testSwapEvacuateAddRemoveEvacuate() throws Exception {
    System.out.println(
        "START TestInstanceOperationMisc.testSwapEvacuateAddRemoveEvacuate() at " + new Date(
            System.currentTimeMillis()));

    enabledTopologyAwareRebalance();

    String instanceToSwapOutName = _participants.get(0).getInstanceName();
    InstanceConfig instanceToSwapOutInstanceConfig = _gSetupTool.getClusterManagementTool()
        .getInstanceConfig(CLUSTER_NAME, instanceToSwapOutName);
    _gSetupTool.getClusterManagementTool().setInstanceOperation(CLUSTER_NAME,
        instanceToSwapOutName, InstanceConstants.InstanceOperation.EVACUATE);

    String instanceToSwapInName = PARTICIPANT_PREFIX + "_" + _nextStartPort.get();
    addParticipant(instanceToSwapInName, instanceToSwapOutInstanceConfig.getLogicalId(LOGICAL_ID),
        instanceToSwapOutInstanceConfig.getDomainAsMap().get(ZONE),
        InstanceConstants.InstanceOperation.ENABLE, -1);

    // Should throw HelixException: cannot remove EVACUATE while an ENABLE instance
    // with the same logicalId exists.
    _gSetupTool.getClusterManagementTool()
        .setInstanceOperation(CLUSTER_NAME, instanceToSwapOutName,
            InstanceConstants.InstanceOperation.ENABLE);
  }

  @Test
  public void testUnknownDoesNotTriggerRebalance() throws Exception {
    System.out.println(
        "START TestInstanceOperationMisc.testUnknownDoesNotTriggerRebalance() at " + new Date(
            System.currentTimeMillis()));

    enabledTopologyAwareRebalance();

    Map<String, IdealState> idealStatesBefore = getISs();

    String instanceToAdd = PARTICIPANT_PREFIX + "_" + _nextStartPort.get();
    addParticipant(instanceToAdd, "foo", "bar",
        InstanceConstants.InstanceOperation.UNKNOWN, -1);
    List<MockParticipantManager> testParticipants = new ArrayList<>();
    testParticipants.add(_participants.get(_participants.size() - 1));
    Assert.assertTrue(_clusterVerifier.verifyByPolling());

    Assert.assertEquals(idealStatesBefore, getISs());

    String instanceToAdd2 = PARTICIPANT_PREFIX + "_" + _nextStartPort.get();
    InstanceConfig swapOutInstanceConfig = _gSetupTool.getClusterManagementTool()
        .getInstanceConfig(CLUSTER_NAME, _participants.get(0).getInstanceName());
    addParticipant(instanceToAdd2, swapOutInstanceConfig.getLogicalId(LOGICAL_ID),
        swapOutInstanceConfig.getDomainAsMap().get(ZONE),
        InstanceConstants.InstanceOperation.UNKNOWN, -1);
    testParticipants.add(_participants.get(_participants.size() - 1));
    Assert.assertTrue(_clusterVerifier.verifyByPolling());
    Assert.assertEquals(idealStatesBefore, getISs());

    for (MockParticipantManager participant : testParticipants) {
      participant.syncStop();
    }
  }

  @Test
  public void testEvacuationWithOfflineInstancesInCluster() throws Exception {
    System.out.println(
        "START TestInstanceOperationMisc.testEvacuationWithOfflineInstancesInCluster() at "
            + new Date(System.currentTimeMillis()));

    enabledTopologyAwareRebalance();

    _participants.get(1).syncStop();
    _participants.get(2).syncStop();

    String evacuateInstanceName = _participants.get(_participants.size() - 2).getInstanceName();
    _gSetupTool.getClusterManagementTool().setInstanceOperation(CLUSTER_NAME,
        evacuateInstanceName, InstanceConstants.InstanceOperation.EVACUATE);

    verifier(() -> {
      Map<String, ExternalView> assignment = getEVs();
      for (String resource : _allDBs) {
        ExternalView ev = assignment.get(resource);
        for (String partition : ev.getPartitionSet()) {
          AtomicInteger activeReplicaCount = new AtomicInteger();
          ev.getStateMap(partition).values().stream().filter(
                  v -> v.equals("MASTER") || v.equals("LEADER") || v.equals("SLAVE")
                      || v.equals("FOLLOWER") || v.equals("STANDBY"))
              .forEach(v -> activeReplicaCount.getAndIncrement());
          if (activeReplicaCount.get() < REPLICA - 1
              || (ev.getStateMap(partition).containsKey(evacuateInstanceName)
              && (ev.getStateMap(partition).get(evacuateInstanceName).equals("MASTER")
              || ev.getStateMap(partition).get(evacuateInstanceName).equals("LEADER")))) {
            return false;
          }
        }
      }
      return true;
    }, 30000);
  }

  @Test
  public void testEvacuateWithDisabledPartition() throws Exception {
    System.out.println(
        "START TestInstanceOperationMisc.testEvacuateWithDisabledPartition() at " + new Date(
            System.currentTimeMillis()));

    enabledTopologyAwareRebalance();

    StateTransitionCountStateModelFactory stateTransitionCountStateModelFactory =
        new StateTransitionCountStateModelFactory();
    String testCrushedDBName = "testEvacuateWithDisabledPartition_CRUSHED_DB0";
    String testWagedDBName = "testEvacuateWithDisabledPartition_WAGED_DB1";
    String toDisableThenEvacuateInstanceName = "disable_then_evacuate_host";
    addParticipant(toDisableThenEvacuateInstanceName, stateTransitionCountStateModelFactory);
    MockParticipantManager toDisableThenEvacuateParticipant =
        _participants.get(_participants.size() - 1);

    List<String> testResources = Arrays.asList(testCrushedDBName, testWagedDBName);
    createResourceWithDelayedRebalance(CLUSTER_NAME, testCrushedDBName, "MasterSlave",
        PARTITIONS, REPLICA, REPLICA - 1, 200000,
        CrushEdRebalanceStrategy.class.getName());
    _allDBs.add(testCrushedDBName);
    createResourceWithWagedRebalance(CLUSTER_NAME, testWagedDBName, "MasterSlave", PARTITIONS,
        REPLICA, REPLICA - 1);
    _allDBs.add(testWagedDBName);

    Assert.assertTrue(_clusterVerifier.verifyByPolling());
    int upwardSTCountBeforeDisableThenEvacuate =
        stateTransitionCountStateModelFactory.getUpwardStateTransitionCounter();
    int downwardSTCountBeforeDisableThenEvacuate =
        stateTransitionCountStateModelFactory.getDownwardStateTransitionCounter();

    InstanceConfig instanceConfig = _gSetupTool.getClusterManagementTool()
        .getInstanceConfig(CLUSTER_NAME, toDisableThenEvacuateInstanceName);
    instanceConfig.setInstanceEnabledForPartition(
        InstanceConstants.ALL_RESOURCES_DISABLED_PARTITION_KEY, "", false);
    _gSetupTool.getClusterManagementTool()
        .setInstanceConfig(CLUSTER_NAME, toDisableThenEvacuateInstanceName, instanceConfig);
    Assert.assertTrue(_clusterVerifier.verifyByPolling());

    verifier(() -> {
      for (String resource : testResources) {
        ExternalView ev = _gSetupTool.getClusterManagementTool()
            .getResourceExternalView(CLUSTER_NAME, resource);
        for (String partition : ev.getPartitionSet()) {
          if (ev.getStateMap(partition).containsKey(toDisableThenEvacuateInstanceName)
              && !ev.getStateMap(partition).get(toDisableThenEvacuateInstanceName)
              .equals("OFFLINE")) {
            return false;
          }
        }
      }
      return true;
    }, 5000);

    Assert.assertEquals(
        stateTransitionCountStateModelFactory.getUpwardStateTransitionCounter(),
        upwardSTCountBeforeDisableThenEvacuate,
        "Upward state transitions should not have been received");
    Assert.assertTrue(
        stateTransitionCountStateModelFactory.getDownwardStateTransitionCounter()
            > downwardSTCountBeforeDisableThenEvacuate,
        "Should have received downward state transitions");

    _gSetupTool.getClusterManagementTool().setInstanceOperation(CLUSTER_NAME,
        toDisableThenEvacuateInstanceName, InstanceConstants.InstanceOperation.EVACUATE);

    verifier(() -> _admin.isEvacuateFinished(CLUSTER_NAME, toDisableThenEvacuateInstanceName),
        30000);
    int downwardSTCountAfterEvacuateComplete =
        stateTransitionCountStateModelFactory.getDownwardStateTransitionCounter();

    Assert.assertEquals(
        stateTransitionCountStateModelFactory.getUpwardStateTransitionCounter(),
        upwardSTCountBeforeDisableThenEvacuate,
        "Upward state transitions should not have been received");

    instanceConfig = _gSetupTool.getClusterManagementTool()
        .getInstanceConfig(CLUSTER_NAME, toDisableThenEvacuateInstanceName);
    instanceConfig.setInstanceEnabledForPartition(
        InstanceConstants.ALL_RESOURCES_DISABLED_PARTITION_KEY, "", true);
    _gSetupTool.getClusterManagementTool()
        .setInstanceConfig(CLUSTER_NAME, toDisableThenEvacuateInstanceName, instanceConfig);
    Assert.assertTrue(_clusterVerifier.verifyByPolling());

    Assert.assertEquals(
        stateTransitionCountStateModelFactory.getUpwardStateTransitionCounter(),
        upwardSTCountBeforeDisableThenEvacuate,
        "Upward state transitions should not have been received");

    instanceConfig = _gSetupTool.getClusterManagementTool()
        .getInstanceConfig(CLUSTER_NAME, toDisableThenEvacuateInstanceName);
    instanceConfig.setInstanceEnabledForPartition(
        InstanceConstants.ALL_RESOURCES_DISABLED_PARTITION_KEY, "", false);
    _gSetupTool.getClusterManagementTool()
        .setInstanceConfig(CLUSTER_NAME, toDisableThenEvacuateInstanceName, instanceConfig);
    Assert.assertTrue(_clusterVerifier.verifyByPolling());

    Assert.assertEquals(
        stateTransitionCountStateModelFactory.getUpwardStateTransitionCounter(),
        upwardSTCountBeforeDisableThenEvacuate,
        "Upward state transitions should not have been received");
    Assert.assertEquals(
        stateTransitionCountStateModelFactory.getDownwardStateTransitionCounter(),
        downwardSTCountAfterEvacuateComplete,
        "Downward state transitions should not have been received");

    toDisableThenEvacuateParticipant.syncStop();
  }
}
