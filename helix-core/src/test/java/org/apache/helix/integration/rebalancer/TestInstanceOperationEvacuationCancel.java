package org.apache.helix.integration.rebalancer;

import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import org.apache.helix.constants.InstanceConstants;
import org.apache.helix.controller.rebalancer.strategy.CrushEdRebalanceStrategy;
import org.apache.helix.model.ExternalView;
import org.testng.Assert;
import org.testng.annotations.Test;


public class TestInstanceOperationEvacuationCancel extends TestInstanceOperationBase {

  /**
   * Adds two extra MasterSlave resources to increase the partition count so that
   * evacuation has enough work to avoid completing prematurely during cancel tests.
   */
  private void createAdditionalMasterSlaveResources() throws InterruptedException {
    createResourceWithDelayedRebalance(CLUSTER_NAME, "TEST_DB3_DELAYED_CRUSHED", "MasterSlave",
        PARTITIONS, REPLICA, REPLICA - 1, 200000, CrushEdRebalanceStrategy.class.getName());
    _allDBs.add("TEST_DB3_DELAYED_CRUSHED");
    createResourceWithWagedRebalance(CLUSTER_NAME, "TEST_DB4_DELAYED_WAGED", "MasterSlave",
        PARTITIONS, REPLICA, REPLICA - 1);
    _allDBs.add("TEST_DB4_DELAYED_WAGED");
    Assert.assertTrue(_clusterVerifier.verifyByPolling());
  }

  @Test
  public void testEvacuateAndCancelBeforeBootstrapFinish() throws Exception {
    System.out.println(
        "START TestInstanceOperationEvacuationCancel.testEvacuateAndCancelBeforeBootstrapFinish() at "
            + new Date(System.currentTimeMillis()));

    enabledTopologyAwareRebalance();
    createAdditionalMasterSlaveResources();

    // Negative delay = slow upward transitions (OFFLINE→SLAVE, SLAVE→MASTER)
    _stateModelDelay = -10000L;
    String instanceToEvacuate = _participants.get(0).getInstanceName();
    _gSetupTool.getClusterManagementTool().setInstanceOperation(CLUSTER_NAME, instanceToEvacuate,
        InstanceConstants.InstanceOperation.EVACUATE);

    for (String participant : _participantNames) {
      if (participant.equals(instanceToEvacuate)) {
        continue;
      }
      verifier(() -> ((_dataAccessor.getChildNames(
          _dataAccessor.keyBuilder().messages(participant))).isEmpty()), 30000);
    }
    Assert.assertFalse(_admin.isEvacuateFinished(CLUSTER_NAME, instanceToEvacuate));
    Assert.assertFalse(_admin.isReadyForPreparingJoiningCluster(CLUSTER_NAME, instanceToEvacuate));

    Thread.sleep(Math.abs(_stateModelDelay / 100));
    Map<String, ExternalView> assignment = getEVs();
    for (String resource : _allDBs) {
      validateAssignmentInEv(assignment.get(resource));
    }

    _gSetupTool.getClusterManagementTool().setInstanceOperation(CLUSTER_NAME, instanceToEvacuate,
        InstanceConstants.InstanceOperation.ENABLE);

    assignment = getEVs();
    for (String resource : _allDBs) {
      validateAssignmentInEv(assignment.get(resource));
    }

    // ST delay exceeds verifier timeout, so convergence only succeeds because
    // cancelling the evacuation (ENABLE) triggers state transition cancellation.
    Assert.assertTrue(_clusterVerifier.verifyByPolling());

    assignment = getEVs();
    for (String resource : _allDBs) {
      Assert.assertTrue(
          getParticipantsInEv(assignment.get(resource)).containsAll(_participantNames));
      validateAssignmentInEv(assignment.get(resource));
    }
  }

  @Test
  public void testEvacuateAndCancelBeforeDropFinish() throws Exception {
    System.out.println(
        "START TestInstanceOperationEvacuationCancel.testEvacuateAndCancelBeforeDropFinish() at "
            + new Date(System.currentTimeMillis()));

    enabledTopologyAwareRebalance();
    createAdditionalMasterSlaveResources();

    // Positive delay = slow downward transitions (MASTER→SLAVE, SLAVE→OFFLINE, OFFLINE→DROPPED)
    _stateModelDelay = 10000L;

    String instanceToEvacuate = _participants.get(0).getInstanceName();
    _gSetupTool.getClusterManagementTool().setInstanceOperation(CLUSTER_NAME, instanceToEvacuate,
        InstanceConstants.InstanceOperation.EVACUATE);

    verifier(() -> ((_dataAccessor.getChildNames(
        _dataAccessor.keyBuilder().messages(instanceToEvacuate))).isEmpty()), 30000);
    Assert.assertFalse(_admin.isEvacuateFinished(CLUSTER_NAME, instanceToEvacuate));

    _gSetupTool.getClusterManagementTool().setInstanceOperation(CLUSTER_NAME, instanceToEvacuate,
        InstanceConstants.InstanceOperation.ENABLE);
    Map<String, ExternalView> assignment = getEVs();
    for (String resource : _allDBs) {
      validateAssignmentInEv(assignment.get(resource));
    }

    Assert.assertTrue(_clusterVerifier.verifyByPolling());

    assignment = getEVs();
    for (String resource : _allDBs) {
      Assert.assertTrue(
          getParticipantsInEv(assignment.get(resource)).containsAll(_participantNames));
      validateAssignmentInEv(assignment.get(resource));
    }
  }

  @Test
  public void testMarkEvacuationAfterEMM() throws Exception {
    System.out.println(
        "START TestInstanceOperationEvacuationCancel.testMarkEvacuationAfterEMM() at "
            + new Date(System.currentTimeMillis()));

    enabledTopologyAwareRebalance();

    _stateModelDelay = 1000L;
    Assert.assertFalse(
        _gSetupTool.getClusterManagementTool().isInMaintenanceMode(CLUSTER_NAME));
    _gSetupTool.getClusterManagementTool()
        .manuallyEnableMaintenanceMode(CLUSTER_NAME, true, null, null);
    String newParticipantName = PARTICIPANT_PREFIX + "_" + _nextStartPort.get();
    addParticipant(newParticipantName);

    Assert.assertTrue(_clusterVerifier.verifyByPolling());
    Map<String, ExternalView> assignment = getEVs();
    for (String resource : _allDBs) {
      Assert.assertFalse(
          getParticipantsInEv(assignment.get(resource)).contains(newParticipantName));
    }

    String instanceToEvacuate = _participants.get(0).getInstanceName();
    _gSetupTool.getClusterManagementTool().setInstanceOperation(CLUSTER_NAME, instanceToEvacuate,
        InstanceConstants.InstanceOperation.EVACUATE);

    for (String resource : _allDBs) {
      Assert.assertTrue(
          getParticipantsInEv(assignment.get(resource)).contains(instanceToEvacuate));
    }

    _gSetupTool.getClusterManagementTool()
        .manuallyEnableMaintenanceMode(CLUSTER_NAME, false, null, null);

    Assert.assertTrue(_clusterVerifier.verifyByPolling());

    assignment = getEVs();
    List<String> currentActiveInstances =
        _participantNames.stream().filter(n -> !n.equals(instanceToEvacuate))
            .collect(Collectors.toList());
    for (String resource : _allDBs) {
      validateAssignmentInEv(assignment.get(resource));
      Set<String> newPAssignedParticipants = getParticipantsInEv(assignment.get(resource));
      Assert.assertFalse(newPAssignedParticipants.contains(instanceToEvacuate));
      Assert.assertTrue(newPAssignedParticipants.containsAll(currentActiveInstances));
    }
    verifier(
        () -> _admin.isReadyForPreparingJoiningCluster(CLUSTER_NAME, instanceToEvacuate),
        60000);

    // Stop the evacuated participant so beforeMethod can clean it up.
    // Without this, the instance stays connected in EVACUATE state and
    // _participants grows unboundedly across test runs.
    _participants.stream()
        .filter(p -> p.getInstanceName().equals(instanceToEvacuate))
        .findFirst().ifPresent(p -> p.syncStop());
  }
}
