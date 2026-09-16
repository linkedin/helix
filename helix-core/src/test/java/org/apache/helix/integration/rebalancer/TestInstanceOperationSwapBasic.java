package org.apache.helix.integration.rebalancer;

import java.util.Date;

import org.apache.helix.HelixException;
import org.apache.helix.constants.InstanceConstants;
import org.apache.helix.model.InstanceConfig;
import org.testng.Assert;
import org.testng.annotations.Test;


public class TestInstanceOperationSwapBasic extends TestInstanceOperationBase {

  @Test
  public void testNodeSwapNoTopologySetup() throws Exception {
    System.out.println("START TestInstanceOperationSwapBasic.testNodeSwapNoTopologySetup() at "
        + new Date(System.currentTimeMillis()));

    String instanceToSwapOutName = _participants.get(0).getInstanceName();

    // Add instance with InstanceOperation set to SWAP_IN as default.
    // The instance will be added with UNKNOWN because the logicalId will not match the
    // swap out instance since the topology configs are not set.
    String instanceToSwapInName = PARTICIPANT_PREFIX + "_" + _nextStartPort.get();
    InstanceConfig instanceToSwapOutInstanceConfig = _gSetupTool.getClusterManagementTool()
        .getInstanceConfig(CLUSTER_NAME, instanceToSwapOutName);
    addParticipant(instanceToSwapInName, instanceToSwapOutInstanceConfig.getLogicalId(LOGICAL_ID),
        instanceToSwapOutInstanceConfig.getDomainAsMap().get(ZONE),
        InstanceConstants.InstanceOperation.SWAP_IN, -1);

    Assert.assertEquals(
        _gSetupTool.getClusterManagementTool()
            .getInstanceConfig(CLUSTER_NAME, instanceToSwapInName)
            .getInstanceOperation().getOperation(),
        InstanceConstants.InstanceOperation.UNKNOWN);
  }

  @Test
  public void testAddingNodeWithEnableInstanceOperation() throws Exception {
    System.out.println(
        "START TestInstanceOperationSwapBasic.testAddingNodeWithEnableInstanceOperation() at "
            + new Date(System.currentTimeMillis()));

    enabledTopologyAwareRebalance();

    String instanceToSwapOutName = _participants.get(0).getInstanceName();
    InstanceConfig instanceToSwapOutInstanceConfig = _gSetupTool.getClusterManagementTool()
        .getInstanceConfig(CLUSTER_NAME, instanceToSwapOutName);

    // Add instance with InstanceOperation set to ENABLE.
    // The instance should be added with UNKNOWN since there is already an instance with
    // the same logicalId in the cluster and this instance is not being set to SWAP_IN when
    // added.
    String instanceToSwapInName = PARTICIPANT_PREFIX + "_" + _nextStartPort.get();
    addParticipant(instanceToSwapInName, instanceToSwapOutInstanceConfig.getLogicalId(LOGICAL_ID),
        instanceToSwapOutInstanceConfig.getDomainAsMap().get(ZONE),
        InstanceConstants.InstanceOperation.ENABLE, -1);

    Assert.assertEquals(
        _gSetupTool.getClusterManagementTool()
            .getInstanceConfig(CLUSTER_NAME, instanceToSwapInName)
            .getInstanceOperation().getOperation(),
        InstanceConstants.InstanceOperation.UNKNOWN);
  }

  @Test
  public void testNodeSwapWithNoSwapOutNode() throws Exception {
    System.out.println(
        "START TestInstanceOperationSwapBasic.testNodeSwapWithNoSwapOutNode() at "
            + new Date(System.currentTimeMillis()));

    enabledTopologyAwareRebalance();

    // Add new instance with InstanceOperation set to SWAP_IN.
    // The instance should be added with UNKNOWN since there is not an instance with a matching
    // logicalId in the cluster to swap with.
    String instanceToSwapInName = PARTICIPANT_PREFIX + "_" + _nextStartPort.get();
    addParticipant(instanceToSwapInName, "1000", "zone_1000",
        InstanceConstants.InstanceOperation.SWAP_IN, -1);

    Assert.assertEquals(
        _gSetupTool.getClusterManagementTool()
            .getInstanceConfig(CLUSTER_NAME, instanceToSwapInName)
            .getInstanceOperation().getOperation(),
        InstanceConstants.InstanceOperation.UNKNOWN);
  }

  @Test
  public void testNodeSwapSwapInNodeNoInstanceOperationEnabled() throws Exception {
    System.out.println(
        "START TestInstanceOperationSwapBasic.testNodeSwapSwapInNodeNoInstanceOperationEnabled() at "
            + new Date(System.currentTimeMillis()));

    enabledTopologyAwareRebalance();

    String instanceToSwapOutName = _participants.get(0).getInstanceName();
    InstanceConfig instanceToSwapOutInstanceConfig = _gSetupTool.getClusterManagementTool()
        .getInstanceConfig(CLUSTER_NAME, instanceToSwapOutName);

    // Add instance with same logicalId with InstanceOperation unset, this is the same as default
    // which is ENABLE.
    // The instance should be set to UNKNOWN since there is already a matching logicalId in the
    // cluster.
    String instanceToSwapInName = PARTICIPANT_PREFIX + "_" + _nextStartPort.get();
    addParticipant(instanceToSwapInName, instanceToSwapOutInstanceConfig.getLogicalId(LOGICAL_ID),
        instanceToSwapOutInstanceConfig.getDomainAsMap().get(ZONE), null, -1);

    Assert.assertEquals(
        _gSetupTool.getClusterManagementTool()
            .getInstanceConfig(CLUSTER_NAME, instanceToSwapInName)
            .getInstanceOperation().getOperation(),
        InstanceConstants.InstanceOperation.UNKNOWN);

    // Setting the InstanceOperation to SWAP_IN should work because there is a matching logicalId
    // in the cluster and the InstanceCapacityWeights and FaultZone match.
    _gSetupTool.getClusterManagementTool().setInstanceOperation(CLUSTER_NAME, instanceToSwapInName,
        InstanceConstants.InstanceOperation.SWAP_IN);
    Assert.assertEquals(
        _gSetupTool.getClusterManagementTool()
            .getInstanceConfig(CLUSTER_NAME, instanceToSwapInName)
            .getInstanceOperation().getOperation(),
        InstanceConstants.InstanceOperation.SWAP_IN);

    Assert.assertTrue(_bestPossibleClusterVerifier.verifyByPolling());
    Assert.assertTrue(_gSetupTool.getClusterManagementTool()
        .completeSwapIfPossible(CLUSTER_NAME, instanceToSwapOutName, false));
    Assert.assertTrue(_clusterVerifier.verifyByPolling());

    validateSwapCompletedSuccessfully(getEVs(), instanceToSwapOutName, instanceToSwapInName);
  }

  @Test(expectedExceptions = HelixException.class)
  public void testNodeSwapSwapInNodeWithAlreadySwappingPair() throws Exception {
    System.out.println(
        "START TestInstanceOperationSwapBasic.testNodeSwapSwapInNodeWithAlreadySwappingPair() at "
            + new Date(System.currentTimeMillis()));

    enabledTopologyAwareRebalance();

    String instanceToSwapOutName = _participants.get(0).getInstanceName();
    InstanceConfig instanceToSwapOutInstanceConfig = _gSetupTool.getClusterManagementTool()
        .getInstanceConfig(CLUSTER_NAME, instanceToSwapOutName);

    // Add instance with InstanceOperation set to SWAP_IN
    String instanceToSwapInName = PARTICIPANT_PREFIX + "_" + _nextStartPort.get();
    addParticipant(instanceToSwapInName, instanceToSwapOutInstanceConfig.getLogicalId(LOGICAL_ID),
        instanceToSwapOutInstanceConfig.getDomainAsMap().get(ZONE),
        InstanceConstants.InstanceOperation.SWAP_IN, -1);

    // Add another instance with InstanceOperation set to SWAP_IN with same logicalId as
    // previously added SWAP_IN instance.
    String secondInstanceToSwapInName = PARTICIPANT_PREFIX + "_" + _nextStartPort.get();
    addParticipant(secondInstanceToSwapInName,
        instanceToSwapOutInstanceConfig.getLogicalId(LOGICAL_ID),
        instanceToSwapOutInstanceConfig.getDomainAsMap().get(ZONE),
        InstanceConstants.InstanceOperation.SWAP_IN, -1);

    // Instance should be UNKNOWN since there was already a swapping pair.
    Assert.assertEquals(
        _gSetupTool.getClusterManagementTool()
            .getInstanceConfig(CLUSTER_NAME, secondInstanceToSwapInName)
            .getInstanceOperation().getOperation(),
        InstanceConstants.InstanceOperation.UNKNOWN);

    // Try to set the InstanceOperation to SWAP_IN, it should throw an exception since there is
    // already a swapping pair.
    _gSetupTool.getClusterManagementTool()
        .setInstanceOperation(CLUSTER_NAME, secondInstanceToSwapInName,
            InstanceConstants.InstanceOperation.SWAP_IN);
  }
}
