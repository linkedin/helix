package org.apache.helix.integration.rebalancer;

import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;

import com.google.common.collect.ImmutableSet;
import org.apache.helix.constants.InstanceConstants;
import org.apache.helix.model.ExternalView;
import org.apache.helix.model.IdealState;
import org.apache.helix.model.InstanceConfig;
import org.testng.Assert;
import org.testng.annotations.Test;


public class TestInstanceOperationSwapDisabledPartitions extends TestInstanceOperationBase {

  @Test
  public void testSwapEvacuateAdd() throws Exception {
    System.out.println(
        "START TestInstanceOperationSwapDisabledPartitions.testSwapEvacuateAdd() at "
            + new Date(System.currentTimeMillis()));

    enabledTopologyAwareRebalance();

    Assert.assertTrue(_clusterVerifier.verifyByPolling());

    Map<String, ExternalView> originalEVs = getEVs();
    Map<String, String> swapOutInstancesToSwapInInstances = new HashMap<>();

    _gSetupTool.getClusterManagementTool()
        .manuallyEnableMaintenanceMode(CLUSTER_NAME, true, null, null);

    Assert.assertTrue(_clusterVerifier.verifyByPolling());

    String instanceToSwapOutName = _participants.get(0).getInstanceName();
    InstanceConfig instanceToSwapOutInstanceConfig = _gSetupTool.getClusterManagementTool()
        .getInstanceConfig(CLUSTER_NAME, instanceToSwapOutName);
    _gSetupTool.getClusterManagementTool().setInstanceOperation(CLUSTER_NAME,
        instanceToSwapOutName, InstanceConstants.InstanceOperation.EVACUATE);

    validateEVsCorrect(getEVs(), originalEVs, swapOutInstancesToSwapInInstances,
        Collections.emptySet(), Collections.emptySet());

    String instanceToSwapInName = PARTICIPANT_PREFIX + "_" + _nextStartPort.get();
    swapOutInstancesToSwapInInstances.put(instanceToSwapOutName, instanceToSwapInName);
    addParticipant(instanceToSwapInName, instanceToSwapOutInstanceConfig.getLogicalId(LOGICAL_ID),
        instanceToSwapOutInstanceConfig.getDomainAsMap().get(ZONE),
        InstanceConstants.InstanceOperation.ENABLE, -1);

    _gSetupTool.getClusterManagementTool()
        .manuallyEnableMaintenanceMode(CLUSTER_NAME, false, null, null);

    Assert.assertTrue(_clusterVerifier.verifyByPolling());

    // WAGED rebalancer may recompute a different global optimum after MM-exit + evacuate,
    // so validate swap outcome structurally rather than via exact state-map comparison.
    verifier(() -> {
      validateSwapCompletedSuccessfully(getEVs(), instanceToSwapOutName, instanceToSwapInName);
      return true;
    }, TIMEOUT);

    Assert.assertTrue(_gSetupTool.getClusterManagementTool()
        .isEvacuateFinished(CLUSTER_NAME, instanceToSwapOutName));

    _gSetupTool.getClusterManagementTool().setInstanceOperation(CLUSTER_NAME,
        instanceToSwapOutName, InstanceConstants.InstanceOperation.UNKNOWN);

    Assert.assertTrue(_clusterVerifier.verifyByPolling());

    verifier(() -> {
      validateSwapCompletedSuccessfully(getEVs(), instanceToSwapOutName, instanceToSwapInName);
      return true;
    }, TIMEOUT);

    // Stop the swap-out participant so beforeMethod can clean it up
    _participants.stream()
        .filter(p -> p.getInstanceName().equals(instanceToSwapOutName))
        .findFirst().ifPresent(p -> p.syncStop());
  }

  @Test
  public void testDisabledPartitionsBeforeSwapInitiated() throws Exception {
    System.out.println(
        "START TestInstanceOperationSwapDisabledPartitions.testDisabledPartitionsBeforeSwapInitiated() at "
            + new Date(System.currentTimeMillis()));

    enabledTopologyAwareRebalance();

    String toAddParticipant = PARTICIPANT_PREFIX + "_" + _nextStartPort.get();
    addParticipant(toAddParticipant);
    Assert.assertTrue(_clusterVerifier.verifyByPolling());

    String instanceToSwapOutName = _participants.get(_participants.size() - 1).getInstanceName();
    InstanceConfig instanceToSwapOutConfig = _gSetupTool.getClusterManagementTool()
        .getInstanceConfig(CLUSTER_NAME, instanceToSwapOutName);
    String instanceToSwapInName = PARTICIPANT_PREFIX + "_" + _nextStartPort.get();

    addParticipant(instanceToSwapInName, instanceToSwapOutConfig.getLogicalId(LOGICAL_ID),
        instanceToSwapOutConfig.getDomainAsMap().get(ZONE),
        InstanceConstants.InstanceOperation.UNKNOWN, -1);

    Assert.assertTrue(_clusterVerifier.verifyByPolling());
    Map<String, ExternalView> beforeEVs = getEVs();

    InstanceConfig swapInInstanceConfig = _gSetupTool.getClusterManagementTool()
        .getInstanceConfig(CLUSTER_NAME, instanceToSwapInName);
    swapInInstanceConfig.setInstanceEnabledForPartition(
        InstanceConstants.ALL_RESOURCES_DISABLED_PARTITION_KEY, "", false);
    _gSetupTool.getClusterManagementTool()
        .setInstanceConfig(CLUSTER_NAME, instanceToSwapInName, swapInInstanceConfig);
    _gSetupTool.getClusterManagementTool().setInstanceOperation(CLUSTER_NAME, instanceToSwapInName,
        InstanceConstants.InstanceOperation.SWAP_IN);
    Assert.assertEquals(
        _gSetupTool.getClusterManagementTool()
            .getInstanceConfig(CLUSTER_NAME, instanceToSwapInName).getInstanceOperation()
            .getOperation(),
        InstanceConstants.InstanceOperation.SWAP_IN);
    Assert.assertTrue(_bestPossibleClusterVerifier.verifyByPolling());

    for (String resource : _allDBs) {
      IdealState is = _gSetupTool.getClusterManagementTool()
          .getResourceIdealState(CLUSTER_NAME, resource);
      ExternalView ev = beforeEVs.get(resource);
      for (String partition : is.getPartitionSet()) {
        if (ev.getStateMap(partition).containsKey(instanceToSwapOutName)) {
          Assert.assertEquals(is.getInstanceStateMap(partition).get(instanceToSwapInName),
              "OFFLINE");
        }
      }
    }

    Assert.assertFalse(_gSetupTool.getClusterManagementTool()
        .canCompleteSwap(CLUSTER_NAME, instanceToSwapOutName));

    swapInInstanceConfig = _gSetupTool.getClusterManagementTool()
        .getInstanceConfig(CLUSTER_NAME, instanceToSwapInName);
    swapInInstanceConfig.setInstanceEnabledForPartition(
        InstanceConstants.ALL_RESOURCES_DISABLED_PARTITION_KEY, "", true);
    _gSetupTool.getClusterManagementTool()
        .setInstanceConfig(CLUSTER_NAME, instanceToSwapInName, swapInInstanceConfig);

    verifier(() -> _gSetupTool.getClusterManagementTool()
        .canCompleteSwap(CLUSTER_NAME, instanceToSwapOutName), 30000);
    Assert.assertTrue(_gSetupTool.getClusterManagementTool()
        .completeSwapIfPossible(CLUSTER_NAME, instanceToSwapOutName, false));
    Assert.assertTrue(_clusterVerifier.verifyByPolling());
    validateSwapCompletedSuccessfully(getEVs(), instanceToSwapOutName, instanceToSwapInName);

    // Stop the swap-out participant so beforeMethod can clean it up
    _participants.stream()
        .filter(p -> p.getInstanceName().equals(instanceToSwapOutName))
        .findFirst().ifPresent(p -> p.syncStop());
  }

  @Test
  public void testDisabledPartitionsAfterSwapInitiated() throws Exception {
    System.out.println(
        "START TestInstanceOperationSwapDisabledPartitions.testDisabledPartitionsAfterSwapInitiated() at "
            + new Date(System.currentTimeMillis()));

    enabledTopologyAwareRebalance();

    String toAddParticipant = PARTICIPANT_PREFIX + "_" + _nextStartPort.get();
    addParticipant(toAddParticipant);
    Assert.assertTrue(_clusterVerifier.verifyByPolling());
    Map<String, ExternalView> beforeSwapAndDisableEVs = getEVs();

    String swapOutInstanceName =
        _participants.get(_participants.size() - 1).getInstanceName();
    InstanceConfig swapOutInstanceConfig = _gSetupTool.getClusterManagementTool()
        .getInstanceConfig(CLUSTER_NAME, swapOutInstanceName);

    String swapInInstanceName = PARTICIPANT_PREFIX + "_" + _nextStartPort.get();
    addParticipant(swapInInstanceName, swapOutInstanceConfig.getLogicalId(LOGICAL_ID),
        swapOutInstanceConfig.getDomainAsMap().get(ZONE),
        InstanceConstants.InstanceOperation.SWAP_IN, -1);
    Map<String, String> swapOutInstancesToSwapInInstances = new HashMap<>();
    swapOutInstancesToSwapInInstances.put(swapOutInstanceName, swapInInstanceName);

    Assert.assertEquals(
        _gSetupTool.getClusterManagementTool()
            .getInstanceConfig(CLUSTER_NAME, swapInInstanceName).getInstanceOperation()
            .getOperation(),
        InstanceConstants.InstanceOperation.SWAP_IN);
    Assert.assertTrue(_bestPossibleClusterVerifier.verifyByPolling());
    Assert.assertTrue(_gSetupTool.getClusterManagementTool()
        .canCompleteSwap(CLUSTER_NAME, swapOutInstanceName));

    Map<String, ExternalView> beforeDisableEVs = getEVs();
    validateEVsCorrect(beforeDisableEVs, beforeSwapAndDisableEVs,
        swapOutInstancesToSwapInInstances, ImmutableSet.of(swapInInstanceName),
        Collections.emptySet());

    InstanceConfig swapInInstanceConfig = _gSetupTool.getClusterManagementTool()
        .getInstanceConfig(CLUSTER_NAME, swapInInstanceName);
    swapInInstanceConfig.setInstanceEnabledForPartition(
        InstanceConstants.ALL_RESOURCES_DISABLED_PARTITION_KEY, "", false);
    _gSetupTool.getClusterManagementTool()
        .setInstanceConfig(CLUSTER_NAME, swapInInstanceName, swapInInstanceConfig);
    Assert.assertTrue(_bestPossibleClusterVerifier.verifyByPolling());
    Assert.assertFalse(_gSetupTool.getClusterManagementTool()
        .canCompleteSwap(CLUSTER_NAME, swapOutInstanceName));

    Map<String, ExternalView> currentEVs = getEVs();
    for (String resource : _allDBs) {
      validateEVCorrect(currentEVs.get(resource), beforeDisableEVs.get(resource),
          swapOutInstancesToSwapInInstances, ImmutableSet.of(swapInInstanceName),
          Collections.emptySet(), ImmutableSet.of(swapInInstanceName));
    }

    swapInInstanceConfig = _gSetupTool.getClusterManagementTool()
        .getInstanceConfig(CLUSTER_NAME, swapInInstanceName);
    swapInInstanceConfig.setInstanceEnabledForPartition(
        InstanceConstants.ALL_RESOURCES_DISABLED_PARTITION_KEY, "", true);
    _gSetupTool.getClusterManagementTool()
        .setInstanceConfig(CLUSTER_NAME, swapInInstanceName, swapInInstanceConfig);
    Assert.assertTrue(_bestPossibleClusterVerifier.verifyByPolling());
    Assert.assertTrue(_gSetupTool.getClusterManagementTool()
        .canCompleteSwap(CLUSTER_NAME, swapOutInstanceName));
    Assert.assertTrue(_gSetupTool.getClusterManagementTool()
        .completeSwapIfPossible(CLUSTER_NAME, swapOutInstanceName, false));
    Assert.assertTrue(_clusterVerifier.verifyByPolling());

    validateSwapCompletedSuccessfully(getEVs(), swapOutInstanceName, swapInInstanceName);

    // Stop the swap-out participant so beforeMethod can clean it up
    _participants.stream()
        .filter(p -> p.getInstanceName().equals(swapOutInstanceName))
        .findFirst().ifPresent(p -> p.syncStop());
  }
}
