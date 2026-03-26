package org.apache.helix.integration.rebalancer;

import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import org.apache.helix.constants.InstanceConstants;
import org.apache.helix.manager.zk.ZKHelixManager;
import org.apache.helix.model.BuiltInStateModelDefinitions;
import org.apache.helix.model.ExternalView;
import org.apache.helix.model.IdealState;
import org.testng.Assert;
import org.testng.annotations.Test;


public class TestInstanceOperationEvacuation extends TestInstanceOperationBase {

  @Test
  public void testEvacuate() throws Exception {
    System.out.println("START TestInstanceOperationEvacuation.testEvacuate() at "
        + new Date(System.currentTimeMillis()));

    String semiAutoDB = "SemiAutoTestDB_1";
    createDBInSemiAuto(_gSetupTool, CLUSTER_NAME, semiAutoDB,
        _participants.stream().map(ZKHelixManager::getInstanceName).collect(Collectors.toList()),
        BuiltInStateModelDefinitions.OnlineOffline.name(), 1, _participants.size());
    Assert.assertTrue(_clusterVerifier.verifyByPolling());

    Map<String, ExternalView> assignment = getEVs();
    for (String resource : _allDBs) {
      Assert.assertTrue(
          getParticipantsInEv(assignment.get(resource)).containsAll(_participantNames));
    }

    String instanceToEvacuate = _participants.get(0).getInstanceName();
    _gSetupTool.getClusterManagementTool()
        .setInstanceOperation(CLUSTER_NAME, instanceToEvacuate,
            InstanceConstants.InstanceOperation.EVACUATE);

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

    Assert.assertTrue(_admin.isEvacuateFinished(CLUSTER_NAME, instanceToEvacuate));
    Assert.assertTrue(_admin.isReadyForPreparingJoiningCluster(CLUSTER_NAME, instanceToEvacuate));

    _gSetupTool.dropResourceFromCluster(CLUSTER_NAME, semiAutoDB);
    Assert.assertTrue(_clusterVerifier.verifyByPolling());

    Assert.assertTrue(_clusterVerifier.verifyByPolling());
    Assert.assertEquals(getEVs(), assignment);
  }

  @Test
  public void testRevertEvacuation() throws Exception {
    System.out.println("START TestInstanceOperationEvacuation.testRevertEvacuation() at "
        + new Date(System.currentTimeMillis()));

    String instanceToEvacuate = _participants.get(0).getInstanceName();

    // First evacuate the instance so we can test reverting it
    _gSetupTool.getClusterManagementTool()
        .setInstanceOperation(CLUSTER_NAME, instanceToEvacuate,
            InstanceConstants.InstanceOperation.EVACUATE);
    Assert.assertTrue(_clusterVerifier.verifyByPolling());
    Assert.assertTrue(_admin.isEvacuateFinished(CLUSTER_NAME, instanceToEvacuate));

    // Now revert the evacuation
    _gSetupTool.getClusterManagementTool().setInstanceOperation(CLUSTER_NAME, instanceToEvacuate,
        InstanceConstants.InstanceOperation.ENABLE);

    Assert.assertTrue(
        _gSetupTool.getClusterManagementTool().getInstanceConfig(CLUSTER_NAME, instanceToEvacuate)
            .getInstanceEnabled());
    Assert.assertTrue(_clusterVerifier.verifyByPolling());

    Map<String, ExternalView> assignment = getEVs();
    for (String resource : _allDBs) {
      Assert.assertTrue(
          getParticipantsInEv(assignment.get(resource)).containsAll(_participantNames));
      validateAssignmentInEv(assignment.get(resource));
    }
  }

  @Test
  public void testAddingNodeWithEvacuationTag() throws Exception {
    System.out.println("START TestInstanceOperationEvacuation.testAddingNodeWithEvacuationTag() at "
        + new Date(System.currentTimeMillis()));

    String mockNewInstance = _participants.get(0).getInstanceName();
    _gSetupTool.getClusterManagementTool()
        .enableInstance(CLUSTER_NAME, mockNewInstance, false);
    Assert.assertTrue(_clusterVerifier.verifyByPolling());

    Map<String, ExternalView> assignment = getEVs();
    List<String> currentActiveInstances =
        _participantNames.stream().filter(n -> !n.equals(mockNewInstance))
            .collect(Collectors.toList());
    for (String resource : _allDBs) {
      validateAssignmentInEv(assignment.get(resource), REPLICA - 1);
      Set<String> newPAssignedParticipants = getParticipantsInEv(assignment.get(resource));
      Assert.assertFalse(newPAssignedParticipants.contains(mockNewInstance));
      Assert.assertTrue(newPAssignedParticipants.containsAll(currentActiveInstances));
    }

    _gSetupTool.getClusterManagementTool()
        .setInstanceOperation(CLUSTER_NAME, mockNewInstance,
            InstanceConstants.InstanceOperation.EVACUATE);

    _gSetupTool.getClusterManagementTool().enableInstance(CLUSTER_NAME, mockNewInstance, true);

    assignment = getEVs();
    currentActiveInstances =
        _participantNames.stream().filter(n -> !n.equals(mockNewInstance))
            .collect(Collectors.toList());
    for (String resource : _allDBs) {
      validateAssignmentInEv(assignment.get(resource), REPLICA - 1);
      Set<String> newPAssignedParticipants = getParticipantsInEv(assignment.get(resource));
      Assert.assertFalse(newPAssignedParticipants.contains(mockNewInstance));
      Assert.assertTrue(newPAssignedParticipants.containsAll(currentActiveInstances));
    }

    _gSetupTool.getClusterManagementTool()
        .setInstanceOperation(CLUSTER_NAME, mockNewInstance,
            InstanceConstants.InstanceOperation.ENABLE);

    Assert.assertTrue(_clusterVerifier.verifyByPolling());

    assignment = getEVs();
    for (String resource : _allDBs) {
      Assert.assertTrue(
          getParticipantsInEv(assignment.get(resource)).containsAll(_participantNames));
      validateAssignmentInEv(assignment.get(resource));
    }
  }

  @Test
  public void testEvacuateWithCustomizedResource() throws Exception {
    System.out.println(
        "START TestInstanceOperationEvacuation.testEvacuateWithCustomizedResource() at "
            + new Date(System.currentTimeMillis()));

    for (String resource : _allDBs) {
      _gSetupTool.dropResourceFromCluster(CLUSTER_NAME, resource);
    }
    Assert.assertTrue(_clusterVerifier.verifyByPolling());

    String instanceToEvacuate = _participants.get(0).getInstanceName();
    String customizedDB = "CustomizedTestDB";
    Map<Integer, String> partitionInstanceMap = new HashMap<>();
    partitionInstanceMap.put(Integer.valueOf(0), _participants.get(0).getInstanceName());
    createResourceInCustomizedMode(_gSetupTool, CLUSTER_NAME, customizedDB, partitionInstanceMap);
    Assert.assertTrue(_clusterVerifier.verifyByPolling());

    _gSetupTool.getClusterManagementTool()
        .manuallyEnableMaintenanceMode(CLUSTER_NAME, true, null, null);
    _gSetupTool.getClusterManagementTool()
        .setInstanceOperation(CLUSTER_NAME, instanceToEvacuate,
            InstanceConstants.InstanceOperation.EVACUATE);
    _gSetupTool.getClusterManagementTool()
        .manuallyEnableMaintenanceMode(CLUSTER_NAME, false, null, null);
    Assert.assertTrue(_clusterVerifier.verifyByPolling());
    Assert.assertFalse(_admin.isEvacuateFinished(CLUSTER_NAME, instanceToEvacuate));

    _gSetupTool.dropResourceFromCluster(CLUSTER_NAME, customizedDB);
    createTestDBs(DEFAULT_RESOURCE_DELAY_TIME);
  }

  @Test
  public void testEvacuateWithCustomizedResourceOfflineInstance() throws Exception {
    System.out.println(
        "START TestInstanceOperationEvacuation.testEvacuateWithCustomizedResourceOfflineInstance() at "
            + new Date(System.currentTimeMillis()));

    for (String resource : _allDBs) {
      _gSetupTool.dropResourceFromCluster(CLUSTER_NAME, resource);
    }
    Assert.assertTrue(_clusterVerifier.verifyByPolling());

    String instanceToEvacuate = _participants.get(0).getInstanceName();
    String customizedDB = "CustomizedTestDB";
    Map<Integer, String> partitionInstanceMap = new HashMap<>();
    partitionInstanceMap.put(Integer.valueOf(0), _participants.get(0).getInstanceName());
    partitionInstanceMap.put(Integer.valueOf(1), _participants.get(0).getInstanceName());
    createResourceInCustomizedMode(_gSetupTool, CLUSTER_NAME, customizedDB, partitionInstanceMap);
    Assert.assertTrue(_clusterVerifier.verifyByPolling());

    _participants.get(0).syncStop();
    _gSetupTool.getClusterManagementTool()
        .manuallyEnableMaintenanceMode(CLUSTER_NAME, true, null, null);
    _gSetupTool.getClusterManagementTool()
        .setInstanceOperation(CLUSTER_NAME, instanceToEvacuate,
            InstanceConstants.InstanceOperation.EVACUATE);
    Assert.assertTrue(_clusterVerifier.verifyByPolling());
    _gSetupTool.getClusterManagementTool()
        .manuallyEnableMaintenanceMode(CLUSTER_NAME, false, null, null);
    Assert.assertFalse(_admin.isEvacuateFinished(CLUSTER_NAME, instanceToEvacuate));

    partitionInstanceMap.put(Integer.valueOf(0), _participants.get(1).getInstanceName());
    partitionInstanceMap.put(Integer.valueOf(1), _participants.get(1).getInstanceName());
    IdealState newIdealState =
        createCustomizedResourceIdealState(customizedDB, partitionInstanceMap);
    _gSetupTool.getClusterManagementTool()
        .setResourceIdealState(CLUSTER_NAME, customizedDB, newIdealState);
    Assert.assertTrue(_clusterVerifier.verifyByPolling());
    Assert.assertTrue(_admin.isEvacuateFinished(CLUSTER_NAME, instanceToEvacuate));

    _gSetupTool.dropResourceFromCluster(CLUSTER_NAME, customizedDB);
    createTestDBs(DEFAULT_RESOURCE_DELAY_TIME);
  }
}
