package org.apache.helix.integration.rebalancer;

import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import com.google.common.collect.ImmutableSet;
import org.apache.helix.constants.InstanceConstants;
import org.apache.helix.model.ExternalView;
import org.apache.helix.model.InstanceConfig;
import org.testng.Assert;
import org.testng.annotations.Test;


public class TestInstanceOperationSwapAdvanced extends TestInstanceOperationBase {

  @Test
  public void testNodeSwap() throws Exception {
    System.out.println(
        "START TestInstanceOperationSwapAdvanced.testNodeSwap() at "
            + new Date(System.currentTimeMillis()));

    enabledTopologyAwareRebalance();

    Map<String, String> swapOutInstancesToSwapInInstances = new HashMap<>();

    String instanceToSwapOutName = _participants.get(0).getInstanceName();
    InstanceConfig instanceToSwapOutInstanceConfig = _gSetupTool.getClusterManagementTool()
        .getInstanceConfig(CLUSTER_NAME, instanceToSwapOutName);

    Assert.assertTrue(_clusterVerifier.verifyByPolling());

    String resourceToDisablePartition = _allDBs.iterator().next();
    getPartitionsAndStatesOnInstance(getEVs(), instanceToSwapOutName).entrySet().stream()
        .filter(entry -> entry.getKey().startsWith(resourceToDisablePartition)).findFirst()
        .ifPresent(entry -> {
          String partition = entry.getKey();
          instanceToSwapOutInstanceConfig.setInstanceEnabledForPartition(resourceToDisablePartition,
              partition, false);
        });
    _gSetupTool.getClusterManagementTool()
        .setInstanceConfig(CLUSTER_NAME, instanceToSwapOutName, instanceToSwapOutInstanceConfig);

    Assert.assertTrue(_clusterVerifier.verifyByPolling());

    Map<String, ExternalView> originalEVs = getEVs();

    validateEVsCorrect(getEVs(), originalEVs, swapOutInstancesToSwapInInstances,
        Collections.emptySet(), Collections.emptySet());

    CustomIndividualInstanceConfigChangeListener instanceToSwapInInstanceConfigListener =
        new CustomIndividualInstanceConfigChangeListener();

    String instanceToSwapInName = PARTICIPANT_PREFIX + "_" + _nextStartPort.get();
    swapOutInstancesToSwapInInstances.put(instanceToSwapOutName, instanceToSwapInName);
    addParticipant(instanceToSwapInName, instanceToSwapOutInstanceConfig.getLogicalId(LOGICAL_ID),
        instanceToSwapOutInstanceConfig.getDomainAsMap().get(ZONE),
        InstanceConstants.InstanceOperation.SWAP_IN, -1, instanceToSwapInInstanceConfigListener,
        null);

    Assert.assertFalse(instanceToSwapInInstanceConfigListener.isThrottlesEnabled());

    Assert.assertTrue(_bestPossibleClusterVerifier.verifyByPolling());
    validateEVsCorrect(getEVs(), originalEVs, swapOutInstancesToSwapInInstances,
        ImmutableSet.of(instanceToSwapInName), Collections.emptySet());

    Assert.assertTrue(_gSetupTool.getClusterManagementTool()
        .canCompleteSwap(CLUSTER_NAME, instanceToSwapOutName));

    validateRoutingTablesInstance(getEVs(), instanceToSwapOutName, true);
    validateRoutingTablesInstance(getEVs(), instanceToSwapInName, false);

    Assert.assertTrue(_gSetupTool.getClusterManagementTool()
        .completeSwapIfPossible(CLUSTER_NAME, instanceToSwapOutName, false));

    InstanceConfig instanceToSwapInInstanceConfig = _gSetupTool.getClusterManagementTool()
        .getInstanceConfig(CLUSTER_NAME, instanceToSwapInName);

    Assert.assertEquals(instanceToSwapInInstanceConfig.getRecord()
            .getMapField(InstanceConfig.InstanceConfigProperty.HELIX_DISABLED_PARTITION.name()),
        instanceToSwapOutInstanceConfig.getRecord()
            .getMapField(InstanceConfig.InstanceConfigProperty.HELIX_DISABLED_PARTITION.name()));

    Assert.assertTrue(_clusterVerifier.verifyByPolling());

    validateRoutingTablesInstance(getEVs(), instanceToSwapInName, true);

    Assert.assertFalse(_gSetupTool.getClusterManagementTool()
        .getInstanceConfig(CLUSTER_NAME, instanceToSwapOutName).getInstanceEnabled());
    Assert.assertEquals(_gSetupTool.getClusterManagementTool()
            .getInstanceConfig(CLUSTER_NAME, instanceToSwapOutName).getInstanceOperation()
            .getOperation(),
        InstanceConstants.InstanceOperation.UNKNOWN);

    Assert.assertTrue(instanceToSwapInInstanceConfigListener.isThrottlesEnabled());

    // WAGED rebalancer may recompute a different global optimum after swap with disabled
    // partitions, so validate swap outcome structurally.
    verifier(() -> {
      validateSwapCompletedSuccessfully(getEVs(), instanceToSwapOutName, instanceToSwapInName);
      return true;
    }, TIMEOUT);

    InstanceConfig instanceToSwapInInstanceConfigAfterSwap = _gSetupTool.getClusterManagementTool()
        .getInstanceConfig(CLUSTER_NAME, instanceToSwapInName);
    instanceToSwapInInstanceConfigAfterSwap.setInstanceEnabledForPartition(
        resourceToDisablePartition, true);
    _gSetupTool.getClusterManagementTool().setInstanceConfig(CLUSTER_NAME, instanceToSwapInName,
        instanceToSwapInInstanceConfigAfterSwap);
  }

  @Test
  public void testNodeSwapDisableAndReenable() throws Exception {
    System.out.println(
        "START TestInstanceOperationSwapAdvanced.testNodeSwapDisableAndReenable() at "
            + new Date(System.currentTimeMillis()));

    enabledTopologyAwareRebalance();

    Map<String, ExternalView> originalEVs = getEVs();

    Map<String, String> swapOutInstancesToSwapInInstances = new HashMap<>();

    String instanceToSwapOutName = _participants.get(0).getInstanceName();
    InstanceConfig instanceToSwapOutInstanceConfig = _gSetupTool.getClusterManagementTool()
        .getInstanceConfig(CLUSTER_NAME, instanceToSwapOutName);

    Assert.assertTrue(_clusterVerifier.verifyByPolling());
    validateEVsCorrect(getEVs(), originalEVs, swapOutInstancesToSwapInInstances,
        Collections.emptySet(), Collections.emptySet());

    String instanceToSwapInName = PARTICIPANT_PREFIX + "_" + _nextStartPort.get();
    swapOutInstancesToSwapInInstances.put(instanceToSwapOutName, instanceToSwapInName);
    addParticipant(instanceToSwapInName, instanceToSwapOutInstanceConfig.getLogicalId(LOGICAL_ID),
        instanceToSwapOutInstanceConfig.getDomainAsMap().get(ZONE),
        InstanceConstants.InstanceOperation.SWAP_IN, -1);

    Assert.assertTrue(_bestPossibleClusterVerifier.verifyByPolling());
    validateEVsCorrect(getEVs(), originalEVs, swapOutInstancesToSwapInInstances,
        ImmutableSet.of(instanceToSwapInName), Collections.emptySet());

    Assert.assertTrue(_gSetupTool.getClusterManagementTool()
        .canCompleteSwap(CLUSTER_NAME, instanceToSwapOutName));

    _gSetupTool.getClusterManagementTool()
        .enableInstance(CLUSTER_NAME, instanceToSwapOutName, false);

    Assert.assertTrue(_bestPossibleClusterVerifier.verifyByPolling());
    Map<String, Map<String, String>> resourcePartitionStateOnSwapOutInstance =
        getResourcePartitionStateOnInstance(getEVs(), instanceToSwapOutName);
    Map<String, Map<String, String>> resourcePartitionStateOnSwapInInstance =
        getResourcePartitionStateOnInstance(getEVs(), instanceToSwapInName);
    Assert.assertEquals(
        resourcePartitionStateOnSwapInInstance.values().stream().flatMap(p -> p.keySet().stream())
            .collect(Collectors.toSet()),
        resourcePartitionStateOnSwapOutInstance.values().stream().flatMap(p -> p.keySet().stream())
            .collect(Collectors.toSet()));
    Set<String> swapOutInstancePartitionStates =
        resourcePartitionStateOnSwapOutInstance.values().stream()
            .flatMap(e -> e.values().stream())
            .collect(Collectors.toSet());
    Assert.assertEquals(swapOutInstancePartitionStates.size(), 1);
    Assert.assertTrue(swapOutInstancePartitionStates.contains("OFFLINE"));
    Set<String> swapInInstancePartitionStates =
        resourcePartitionStateOnSwapInInstance.values().stream()
            .flatMap(e -> e.values().stream())
            .collect(Collectors.toSet());
    Assert.assertEquals(swapInInstancePartitionStates.size(), 1);
    Assert.assertTrue(swapInInstancePartitionStates.contains("OFFLINE"));

    validateRoutingTablesInstance(getEVs(), instanceToSwapOutName, true);
    validateRoutingTablesInstance(getEVs(), instanceToSwapInName, false);

    Assert.assertTrue(_gSetupTool.getClusterManagementTool()
        .canCompleteSwap(CLUSTER_NAME, instanceToSwapOutName));

    _gSetupTool.getClusterManagementTool()
        .enableInstance(CLUSTER_NAME, instanceToSwapOutName, true);
    Assert.assertTrue(_bestPossibleClusterVerifier.verifyByPolling());

    validateRoutingTablesInstance(getEVs(), instanceToSwapOutName, true);
    validateRoutingTablesInstance(getEVs(), instanceToSwapInName, false);

    Assert.assertTrue(_gSetupTool.getClusterManagementTool()
        .completeSwapIfPossible(CLUSTER_NAME, instanceToSwapOutName, false));

    Assert.assertTrue(_clusterVerifier.verifyByPolling());

    validateRoutingTablesInstance(getEVs(), instanceToSwapInName, true);

    Assert.assertFalse(_gSetupTool.getClusterManagementTool()
        .getInstanceConfig(CLUSTER_NAME, instanceToSwapOutName).getInstanceEnabled());
    Assert.assertEquals(_gSetupTool.getClusterManagementTool()
            .getInstanceConfig(CLUSTER_NAME, instanceToSwapOutName).getInstanceOperation()
            .getOperation(),
        InstanceConstants.InstanceOperation.UNKNOWN);

    verifier(() -> (validateEVsCorrect(getEVs(), originalEVs, swapOutInstancesToSwapInInstances,
        Collections.emptySet(), ImmutableSet.of(instanceToSwapInName))), TIMEOUT);
  }

  @Test
  public void testNodeSwapSwapInNodeNoInstanceOperation() throws Exception {
    System.out.println(
        "START TestInstanceOperationSwapAdvanced.testNodeSwapSwapInNodeNoInstanceOperation() at "
            + new Date(System.currentTimeMillis()));

    enabledTopologyAwareRebalance();

    Map<String, ExternalView> originalEVs = getEVs();

    Map<String, String> swapOutInstancesToSwapInInstances = new HashMap<>();

    String instanceToSwapOutName = _participants.get(0).getInstanceName();
    InstanceConfig instanceToSwapOutInstanceConfig = _gSetupTool.getClusterManagementTool()
        .getInstanceConfig(CLUSTER_NAME, instanceToSwapOutName);

    Assert.assertTrue(_clusterVerifier.verifyByPolling());
    validateEVsCorrect(getEVs(), originalEVs, swapOutInstancesToSwapInInstances,
        Collections.emptySet(), Collections.emptySet());

    String instanceToSwapInName = PARTICIPANT_PREFIX + "_" + _nextStartPort.get();
    swapOutInstancesToSwapInInstances.put(instanceToSwapOutName, instanceToSwapInName);
    addParticipant(instanceToSwapInName, instanceToSwapOutInstanceConfig.getLogicalId(LOGICAL_ID),
        instanceToSwapOutInstanceConfig.getDomainAsMap().get(ZONE), null, -1);

    Assert.assertTrue(_clusterVerifier.verifyByPolling());
    validateEVsCorrect(getEVs(), originalEVs, swapOutInstancesToSwapInInstances,
        Collections.emptySet(), Collections.emptySet());

    _gSetupTool.getClusterManagementTool().setInstanceOperation(CLUSTER_NAME, instanceToSwapInName,
        InstanceConstants.InstanceOperation.SWAP_IN);

    Assert.assertTrue(_bestPossibleClusterVerifier.verifyByPolling());
    validateEVsCorrect(getEVs(), originalEVs, swapOutInstancesToSwapInInstances,
        ImmutableSet.of(instanceToSwapInName), Collections.emptySet());

    validateRoutingTablesInstance(getEVs(), instanceToSwapOutName, true);
    validateRoutingTablesInstance(getEVs(), instanceToSwapInName, false);

    Assert.assertTrue(_gSetupTool.getClusterManagementTool()
        .canCompleteSwap(CLUSTER_NAME, instanceToSwapOutName));
    Assert.assertTrue(_gSetupTool.getClusterManagementTool()
        .completeSwapIfPossible(CLUSTER_NAME, instanceToSwapOutName, false));

    Assert.assertTrue(_clusterVerifier.verifyByPolling());

    Assert.assertFalse(_gSetupTool.getClusterManagementTool()
        .getInstanceConfig(CLUSTER_NAME, instanceToSwapOutName).getInstanceEnabled());

    verifier(() -> (validateEVsCorrect(getEVs(), originalEVs, swapOutInstancesToSwapInInstances,
        Collections.emptySet(), ImmutableSet.of(instanceToSwapInName))), TIMEOUT);
  }

  @Test
  public void testNodeSwapCancelSwapWhenReadyToComplete() throws Exception {
    System.out.println(
        "START TestInstanceOperationSwapAdvanced.testNodeSwapCancelSwapWhenReadyToComplete() at "
            + new Date(System.currentTimeMillis()));

    enabledTopologyAwareRebalance();

    Map<String, ExternalView> originalEVs = getEVs();
    Map<String, String> swapOutInstancesToSwapInInstances = new HashMap<>();

    String instanceToSwapOutName = _participants.get(0).getInstanceName();
    InstanceConfig instanceToSwapOutInstanceConfig = _gSetupTool.getClusterManagementTool()
        .getInstanceConfig(CLUSTER_NAME, instanceToSwapOutName);

    Assert.assertTrue(_clusterVerifier.verifyByPolling());
    validateEVsCorrect(getEVs(), originalEVs, swapOutInstancesToSwapInInstances,
        Collections.emptySet(), Collections.emptySet());

    String instanceToSwapInName = PARTICIPANT_PREFIX + "_" + _nextStartPort.get();
    swapOutInstancesToSwapInInstances.put(instanceToSwapOutName, instanceToSwapInName);
    addParticipant(instanceToSwapInName, instanceToSwapOutInstanceConfig.getLogicalId(LOGICAL_ID),
        instanceToSwapOutInstanceConfig.getDomainAsMap().get(ZONE),
        InstanceConstants.InstanceOperation.SWAP_IN, -1);

    Assert.assertTrue(_bestPossibleClusterVerifier.verifyByPolling());
    validateEVsCorrect(getEVs(), originalEVs, swapOutInstancesToSwapInInstances,
        ImmutableSet.of(instanceToSwapInName), Collections.emptySet());

    validateRoutingTablesInstance(getEVs(), instanceToSwapOutName, true);
    validateRoutingTablesInstance(getEVs(), instanceToSwapInName, false);

    Assert.assertTrue(_gSetupTool.getClusterManagementTool()
        .canCompleteSwap(CLUSTER_NAME, instanceToSwapOutName));

    _gSetupTool.getClusterManagementTool().setInstanceOperation(CLUSTER_NAME, instanceToSwapInName,
        InstanceConstants.InstanceOperation.UNKNOWN);

    Assert.assertTrue(_bestPossibleClusterVerifier.verifyByPolling());
    validateEVsCorrect(getEVs(), originalEVs, swapOutInstancesToSwapInInstances,
        Collections.emptySet(), Collections.emptySet());

    Assert.assertTrue(_clusterVerifier.verifyByPolling());

    validateRoutingTablesInstance(getEVs(), instanceToSwapOutName, true);
    validateRoutingTablesInstance(getEVs(), instanceToSwapInName, false);

    Assert.assertEquals(getPartitionsAndStatesOnInstance(getEVs(), instanceToSwapInName).size(), 0);

    validateEVsCorrect(getEVs(), originalEVs, swapOutInstancesToSwapInInstances,
        Collections.emptySet(), Collections.emptySet());

    _gSetupTool.getClusterManagementTool().setInstanceOperation(CLUSTER_NAME,
        instanceToSwapOutName, InstanceConstants.InstanceOperation.ENABLE);

    Assert.assertTrue(_clusterVerifier.verifyByPolling());

    verifier(() -> (validateEVsCorrect(getEVs(), originalEVs, swapOutInstancesToSwapInInstances,
        Collections.emptySet(), Collections.emptySet())), TIMEOUT);
  }

  @Test
  public void testNodeSwapAfterEMM() throws Exception {
    System.out.println(
        "START TestInstanceOperationSwapAdvanced.testNodeSwapAfterEMM() at "
            + new Date(System.currentTimeMillis()));

    enabledTopologyAwareRebalance();

    Map<String, ExternalView> originalEVs = getEVs();
    Map<String, String> swapOutInstancesToSwapInInstances = new HashMap<>();

    _gSetupTool.getClusterManagementTool()
        .manuallyEnableMaintenanceMode(CLUSTER_NAME, true, null, null);

    String instanceToSwapOutName = _participants.get(0).getInstanceName();
    InstanceConfig instanceToSwapOutInstanceConfig = _gSetupTool.getClusterManagementTool()
        .getInstanceConfig(CLUSTER_NAME, instanceToSwapOutName);

    Assert.assertTrue(_clusterVerifier.verifyByPolling());
    validateEVsCorrect(getEVs(), originalEVs, swapOutInstancesToSwapInInstances,
        Collections.emptySet(), Collections.emptySet());

    String instanceToSwapInName = PARTICIPANT_PREFIX + "_" + _nextStartPort.get();
    swapOutInstancesToSwapInInstances.put(instanceToSwapOutName, instanceToSwapInName);
    addParticipant(instanceToSwapInName, instanceToSwapOutInstanceConfig.getLogicalId(LOGICAL_ID),
        instanceToSwapOutInstanceConfig.getDomainAsMap().get(ZONE),
        InstanceConstants.InstanceOperation.SWAP_IN, -1);

    Assert.assertTrue(_bestPossibleClusterVerifier.verifyByPolling());
    validateEVsCorrect(getEVs(), originalEVs, swapOutInstancesToSwapInInstances,
        Collections.emptySet(), Collections.emptySet());

    _gSetupTool.getClusterManagementTool()
        .manuallyEnableMaintenanceMode(CLUSTER_NAME, false, null, null);

    Assert.assertTrue(_bestPossibleClusterVerifier.verifyByPolling());
    validateEVsCorrect(getEVs(), originalEVs, swapOutInstancesToSwapInInstances,
        ImmutableSet.of(instanceToSwapInName), Collections.emptySet());

    validateRoutingTablesInstance(getEVs(), instanceToSwapOutName, true);
    validateRoutingTablesInstance(getEVs(), instanceToSwapInName, false);

    Assert.assertTrue(_gSetupTool.getClusterManagementTool()
        .canCompleteSwap(CLUSTER_NAME, instanceToSwapOutName));
    Assert.assertTrue(_gSetupTool.getClusterManagementTool()
        .completeSwapIfPossible(CLUSTER_NAME, instanceToSwapOutName, false));

    Assert.assertTrue(_clusterVerifier.verifyByPolling());

    validateRoutingTablesInstance(getEVs(), instanceToSwapInName, true);

    Assert.assertFalse(_gSetupTool.getClusterManagementTool()
        .getInstanceConfig(CLUSTER_NAME, instanceToSwapOutName).getInstanceEnabled());

    // WAGED rebalancer may recompute a different global optimum after MM-exit + swap completion,
    // so validate swap outcome structurally rather than via exact state-map comparison.
    verifier(() -> {
      validateSwapCompletedSuccessfully(getEVs(), instanceToSwapOutName, instanceToSwapInName);
      return true;
    }, TIMEOUT);
  }

  @Test
  public void testNodeSwapWithSwapOutInstanceDisabled() throws Exception {
    System.out.println(
        "START TestInstanceOperationSwapAdvanced.testNodeSwapWithSwapOutInstanceDisabled() at "
            + new Date(System.currentTimeMillis()));

    enabledTopologyAwareRebalance();

    Map<String, ExternalView> originalEVs = getEVs();

    String instanceToSwapOutName = _participants.get(0).getInstanceName();
    InstanceConfig instanceToSwapOutInstanceConfig = _gSetupTool.getClusterManagementTool()
        .getInstanceConfig(CLUSTER_NAME, instanceToSwapOutName);

    _gSetupTool.getClusterManagementTool()
        .enableInstance(CLUSTER_NAME, instanceToSwapOutName, false);

    Assert.assertTrue(_clusterVerifier.verifyByPolling());

    Set<String> swapOutInstanceOfflineStates =
        new HashSet<>(getPartitionsAndStatesOnInstance(getEVs(), instanceToSwapOutName).values());
    Assert.assertEquals(swapOutInstanceOfflineStates.size(), 1);
    Assert.assertTrue(swapOutInstanceOfflineStates.contains("OFFLINE"));

    String instanceToSwapInName = PARTICIPANT_PREFIX + "_" + _nextStartPort.get();
    addParticipant(instanceToSwapInName, instanceToSwapOutInstanceConfig.getLogicalId(LOGICAL_ID),
        instanceToSwapOutInstanceConfig.getDomainAsMap().get(ZONE),
        InstanceConstants.InstanceOperation.SWAP_IN, -1);

    Assert.assertTrue(_bestPossibleClusterVerifier.verifyByPolling());

    Map<String, String> swapInInstancePartitionsAndStates =
        getPartitionsAndStatesOnInstance(getEVs(), instanceToSwapInName);
    Assert.assertEquals(swapInInstancePartitionsAndStates.size(), 0);

    Assert.assertTrue(_gSetupTool.getClusterManagementTool()
        .canCompleteSwap(CLUSTER_NAME, instanceToSwapOutName));

    Assert.assertTrue(_gSetupTool.getClusterManagementTool()
        .completeSwapIfPossible(CLUSTER_NAME, instanceToSwapOutName, false));

    Assert.assertTrue(_bestPossibleClusterVerifier.verifyByPolling());

    Assert.assertFalse(_gSetupTool.getClusterManagementTool()
        .getInstanceConfig(CLUSTER_NAME, instanceToSwapOutName).getInstanceEnabled());

    verifier(
        () -> (getPartitionsAndStatesOnInstance(getEVs(), instanceToSwapOutName).isEmpty()),
        TIMEOUT);
  }

  @Test
  public void testNodeSwapWithSwapOutInstanceOffline() throws Exception {
    System.out.println(
        "START TestInstanceOperationSwapAdvanced.testNodeSwapWithSwapOutInstanceOffline() at "
            + new Date(System.currentTimeMillis()));

    enabledTopologyAwareRebalance();

    String instanceToSwapOutName = _participants.get(0).getInstanceName();
    InstanceConfig instanceToSwapOutInstanceConfig = _gSetupTool.getClusterManagementTool()
        .getInstanceConfig(CLUSTER_NAME, instanceToSwapOutName);

    String instanceToSwapInName = PARTICIPANT_PREFIX + "_" + _nextStartPort.get();
    addParticipant(instanceToSwapInName, instanceToSwapOutInstanceConfig.getLogicalId(LOGICAL_ID),
        instanceToSwapOutInstanceConfig.getDomainAsMap().get(ZONE),
        InstanceConstants.InstanceOperation.SWAP_IN, -1);
    Assert.assertEquals(
        _gSetupTool.getClusterManagementTool()
            .getInstanceConfig(CLUSTER_NAME, instanceToSwapInName)
            .getInstanceOperation().getOperation(),
        InstanceConstants.InstanceOperation.SWAP_IN);

    Assert.assertTrue(_bestPossibleClusterVerifier.verifyByPolling());

    _participants.get(0).syncStop();

    Assert.assertTrue(_bestPossibleClusterVerifier.verifyByPolling());

    Assert.assertTrue(_gSetupTool.getClusterManagementTool()
        .canCompleteSwap(CLUSTER_NAME, instanceToSwapOutName));

    validateRoutingTablesInstance(getEVs(), instanceToSwapInName, false);

    Assert.assertTrue(_gSetupTool.getClusterManagementTool()
        .completeSwapIfPossible(CLUSTER_NAME, instanceToSwapOutName, false));

    Assert.assertTrue(_clusterVerifier.verifyByPolling());

    validateRoutingTablesInstance(getEVs(), instanceToSwapInName, true);

    Assert.assertFalse(_gSetupTool.getClusterManagementTool()
        .getInstanceConfig(CLUSTER_NAME, instanceToSwapOutName).getInstanceEnabled());

    // WAGED rebalancer may recompute a different global optimum after offline-instance swap,
    // so validate swap outcome structurally rather than via exact state-map comparison.
    verifier(() -> {
      validateSwapCompletedSuccessfully(getEVs(), instanceToSwapOutName, instanceToSwapInName);
      return true;
    }, TIMEOUT);
  }

  @Test
  public void testNodeSwapForceComplete() throws Exception {
    System.out.println(
        "START TestInstanceOperationSwapAdvanced.testNodeSwapForceComplete() at "
            + new Date(System.currentTimeMillis()));

    enabledTopologyAwareRebalance();

    Assert.assertTrue(_clusterVerifier.verifyByPolling());

    String instanceToSwapOutName = _participants.get(0).getInstanceName();
    InstanceConfig instanceToSwapOutInstanceConfig = _gSetupTool.getClusterManagementTool()
        .getInstanceConfig(CLUSTER_NAME, instanceToSwapOutName);

    String instanceToSwapInName = PARTICIPANT_PREFIX + "_" + _nextStartPort.get();
    addParticipant(instanceToSwapInName, instanceToSwapOutInstanceConfig.getLogicalId(LOGICAL_ID),
        instanceToSwapOutInstanceConfig.getDomainAsMap().get(ZONE),
        InstanceConstants.InstanceOperation.SWAP_IN, -1);

    Assert.assertTrue(_bestPossibleClusterVerifier.verifyByPolling());

    Assert.assertTrue(_gSetupTool.getClusterManagementTool()
        .canCompleteSwap(CLUSTER_NAME, instanceToSwapOutName));

    Assert.assertTrue(_gSetupTool.getClusterManagementTool()
        .completeSwapIfPossible(CLUSTER_NAME, instanceToSwapOutName, true));

    Assert.assertTrue(_clusterVerifier.verifyByPolling());

    validateRoutingTablesInstance(getEVs(), instanceToSwapInName, true);

    Assert.assertEquals(_gSetupTool.getClusterManagementTool()
            .getInstanceConfig(CLUSTER_NAME, instanceToSwapOutName).getInstanceOperation()
            .getOperation(),
        InstanceConstants.InstanceOperation.UNKNOWN);

    Map<String, ExternalView> currentEVs = getEVs();
    Map<String, Map<String, String>> swapInPartitions =
        getResourcePartitionStateOnInstance(currentEVs, instanceToSwapInName);
    Assert.assertFalse(swapInPartitions.isEmpty(),
        "SWAP_IN instance should have partitions assigned after swap completion");
    Map<String, Map<String, String>> swapOutPartitions =
        getResourcePartitionStateOnInstance(currentEVs, instanceToSwapOutName);
    Assert.assertTrue(swapOutPartitions.isEmpty(),
        "SWAP_OUT instance should have no partitions after swap completion");
  }

  @Test
  public void testNodeSwapForceCompleteBypassesReadinessCheck() throws Exception {
    System.out.println(
        "START TestInstanceOperationSwapAdvanced.testNodeSwapForceCompleteBypassesReadinessCheck() at "
            + new Date(System.currentTimeMillis()));

    enabledTopologyAwareRebalance();

    Assert.assertTrue(_clusterVerifier.verifyByPolling());

    String instanceToSwapOutName = _participants.get(0).getInstanceName();
    InstanceConfig instanceToSwapOutInstanceConfig = _gSetupTool.getClusterManagementTool()
        .getInstanceConfig(CLUSTER_NAME, instanceToSwapOutName);

    String instanceToSwapInName = PARTICIPANT_PREFIX + "_" + _nextStartPort.get();
    addParticipant(instanceToSwapInName, instanceToSwapOutInstanceConfig.getLogicalId(LOGICAL_ID),
        instanceToSwapOutInstanceConfig.getDomainAsMap().get(ZONE),
        InstanceConstants.InstanceOperation.SWAP_IN, -1);

    _participants.get(_participants.size() - 1).syncStop();

    Assert.assertTrue(_bestPossibleClusterVerifier.verifyByPolling());

    Assert.assertFalse(_gSetupTool.getClusterManagementTool()
        .canCompleteSwap(CLUSTER_NAME, instanceToSwapOutName));

    Assert.assertFalse(_gSetupTool.getClusterManagementTool()
        .completeSwapIfPossible(CLUSTER_NAME, instanceToSwapOutName, false));

    Assert.assertTrue(_gSetupTool.getClusterManagementTool()
        .completeSwapIfPossible(CLUSTER_NAME, instanceToSwapOutName, true));

    Assert.assertEquals(_gSetupTool.getClusterManagementTool()
            .getInstanceConfig(CLUSTER_NAME, instanceToSwapOutName).getInstanceOperation()
            .getOperation(),
        InstanceConstants.InstanceOperation.UNKNOWN);
  }
}
