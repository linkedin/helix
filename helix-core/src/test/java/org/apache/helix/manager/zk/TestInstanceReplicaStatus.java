package org.apache.helix.manager.zk;

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

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import org.apache.helix.BaseDataAccessor;
import org.apache.helix.HelixDataAccessor;
import org.apache.helix.PropertyKey;
import org.apache.helix.PropertyPathBuilder;
import org.apache.helix.ZkUnitTestBase;
import org.apache.helix.api.exceptions.HelixMetaDataAccessException;
import org.apache.helix.constants.InstanceConstants;
import org.apache.helix.model.CurrentState;
import org.apache.helix.model.IdealState;
import org.apache.helix.model.InstanceConfig;
import org.apache.helix.model.InstanceReplicaStatus;
import org.apache.helix.model.InstanceReplicaStatus.BlockerCode;
import org.apache.helix.model.InstanceReplicaStatus.CoverageStatus;
import org.apache.helix.model.InstanceReplicaStatus.ResourceEvaluation;
import org.apache.helix.model.InstanceReplicaStatus.ResourceInfo;
import org.apache.helix.model.LiveInstance;
import org.apache.helix.task.TaskConstants;
import org.apache.helix.zookeeper.datamodel.ZNRecord;
import org.mockito.Mockito;
import org.testng.Assert;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

public class TestInstanceReplicaStatus extends ZkUnitTestBase {
  private static final String CLUSTER_NAME = "TestInstanceReplicaStatus";
  private static final String INSTANCE_NAME = "localhost_12918";
  private static final String OTHER_INSTANCE = "localhost_12919";
  private static final String SESSION_1 = "1001";
  private static final String SESSION_2 = "1002";

  private ZKHelixAdmin _admin;
  private ZKHelixDataAccessor _accessor;

  @BeforeMethod
  public void beforeMethod() {
    if (_gZkClient.exists("/" + CLUSTER_NAME)) {
      _gZkClient.deleteRecursively("/" + CLUSTER_NAME);
    }
    _admin = new ZKHelixAdmin(_gZkClient);
    _admin.addCluster(CLUSTER_NAME, true);
    _admin.addInstance(CLUSTER_NAME, new InstanceConfig(INSTANCE_NAME));
    _admin.addInstance(CLUSTER_NAME, new InstanceConfig(OTHER_INSTANCE));
    _accessor = new ZKHelixDataAccessor(CLUSTER_NAME,
        new ZkBaseDataAccessor<>(_gZkClient));
  }

  @AfterMethod
  public void afterMethod() {
    if (_gZkClient.exists("/" + CLUSTER_NAME)) {
      _gZkClient.deleteRecursively("/" + CLUSTER_NAME);
    }
  }

  @Test
  public void testMixedModesAndTaskResourcesReportScopedCoverage() {
    String sessionId = setLive();
    setCurrentState(sessionId, "fullAuto", "fullAuto_0", "OFFLINE", "MasterSlave");
    setIdealState("fullAuto", IdealState.RebalanceMode.FULL_AUTO);
    setCurrentState(sessionId, "customized", "customized_0", "ERROR", "MasterSlave");
    setCustomizedIdealState("customized", "customized_0", OTHER_INSTANCE);
    setCurrentState(sessionId, "semiAuto", "semiAuto_0", "OFFLINE", "MasterSlave");
    setIdealState("semiAuto", IdealState.RebalanceMode.SEMI_AUTO);
    setTaskCurrentState(sessionId, "workflow", "workflow_0");

    InstanceReplicaStatus status =
        _admin.getInstanceReplicaStatus(CLUSTER_NAME, INSTANCE_NAME);

    Assert.assertTrue(status.isLive());
    Assert.assertEquals(status.getActiveSessionId(), sessionId);
    Assert.assertEquals(status.getCurrentStateSessions(), List.of(sessionId));
    Assert.assertEquals(status.getReplicaStates().getCoverage(), CoverageStatus.COMPLETE);
    Assert.assertEquals(status.getReplicaStates().getReplicaCount(), 3);
    Assert.assertFalse(status.getReplicaStates().isReplicasEmpty());
    Assert.assertFalse(status.getReplicaStates().isAllOffline());
    Assert.assertTrue(status.getReplicaStates().isAllOfflineOrError());
    Assert.assertFalse(status.getReplicaStates().isAllError());
    Assert.assertEquals(status.getReplicaStates().getStateCounts(),
        Map.of("ERROR", 1, "OFFLINE", 2));
    Assert.assertEquals(status.getReplicaStates().getExcludedTaskResources(),
        List.of("workflow"));

    Assert.assertEquals(status.getDrain().getCoverage(), CoverageStatus.UNSUPPORTED);
    Assert.assertFalse(status.getDrain().isDrained());
    Map<String, ResourceInfo> resources = resourcesByName(status);
    Assert.assertEquals(resources.get("fullAuto").getEvaluation(),
        ResourceEvaluation.EVALUATED);
    Assert.assertEquals(resources.get("customized").getEvaluation(),
        ResourceEvaluation.EVALUATED);
    Assert.assertEquals(resources.get("semiAuto").getEvaluation(),
        ResourceEvaluation.UNSUPPORTED_REBALANCE_MODE);
    Assert.assertEquals(resources.get("fullAuto").getDrainBlockingReplicaCount(), 1);
    Assert.assertEquals(resources.get("customized").getDrainBlockingReplicaCount(), 1);
    Assert.assertTrue(hasBlocker(status, BlockerCode.UNSUPPORTED_REBALANCE_MODE));
  }

  @Test
  public void testOfflineAndOfflineOrErrorRemainDistinct() {
    String sessionId = setLive();
    setCurrentState(sessionId, "database", "database_0", "OFFLINE", "MasterSlave");
    addCurrentStatePartition(sessionId, "database", "database_1", "ERROR");
    setIdealState("database", IdealState.RebalanceMode.FULL_AUTO);

    InstanceReplicaStatus status =
        _admin.getInstanceReplicaStatus(CLUSTER_NAME, INSTANCE_NAME);

    Assert.assertEquals(status.getReplicaStates().getCoverage(), CoverageStatus.COMPLETE);
    Assert.assertFalse(status.getReplicaStates().isAllOffline());
    Assert.assertTrue(status.getReplicaStates().isAllOfflineOrError());
    Assert.assertFalse(status.getReplicaStates().isAllError());
    Assert.assertEquals(status.getReplicaStates().getErrorReplicas().size(), 1);
    Assert.assertEquals(
        status.getReplicaStates().getErrorReplicas().get(0).getPartitionName(), "database_1");
    Assert.assertFalse(status.getDrain().isDrained());

    addCurrentStatePartition(sessionId, "database", "database_0", "ERROR");
    status = _admin.getInstanceReplicaStatus(CLUSTER_NAME, INSTANCE_NAME);
    Assert.assertTrue(status.getReplicaStates().isAllError());

    addCurrentStatePartition(sessionId, "database", "database_0", "OFFLINE");
    addCurrentStatePartition(sessionId, "database", "database_1", "OFFLINE");
    status = _admin.getInstanceReplicaStatus(CLUSTER_NAME, INSTANCE_NAME);
    Assert.assertTrue(status.getReplicaStates().isAllOffline());
    Assert.assertTrue(status.getReplicaStates().isAllOfflineOrError());
    Assert.assertTrue(status.getReplicaStates().getErrorReplicas().isEmpty());
  }

  @Test
  public void testEmptyReplicaScopeDoesNotHidePendingMessages() {
    String sessionId = setLive();
    setEmptyCurrentState(sessionId, "droppedResource");
    String messagePath =
        PropertyPathBuilder.instanceMessage(CLUSTER_NAME, INSTANCE_NAME, "message_0");
    _gZkClient.createPersistent(messagePath, true);

    InstanceReplicaStatus status =
        _admin.getInstanceReplicaStatus(CLUSTER_NAME, INSTANCE_NAME);

    Assert.assertEquals(status.getReplicaStates().getCoverage(), CoverageStatus.COMPLETE);
    Assert.assertTrue(status.getReplicaStates().isReplicasEmpty());
    Assert.assertFalse(status.getReplicaStates().isAllOffline());
    Assert.assertFalse(status.getReplicaStates().isAllOfflineOrError());
    Assert.assertEquals(status.getDrain().getCoverage(), CoverageStatus.COMPLETE);
    Assert.assertEquals(status.getDrain().getPendingMessageCount(), 1);
    Assert.assertFalse(status.getDrain().isDrained());
    Assert.assertTrue(hasBlocker(status, BlockerCode.PENDING_MESSAGES));

    _gZkClient.delete(messagePath);
    status = _admin.getInstanceReplicaStatus(CLUSTER_NAME, INSTANCE_NAME);
    Assert.assertTrue(status.getReplicaStates().isReplicasEmpty());
    Assert.assertTrue(status.getDrain().isDrained());
  }

  @Test
  public void testLiveAndOfflineDrainUseDifferentNativeRules() {
    setCurrentState(SESSION_1, "fullAuto", "fullAuto_0", "MASTER", "MasterSlave");
    setIdealState("fullAuto", IdealState.RebalanceMode.FULL_AUTO);
    setCurrentState(SESSION_1, "customized", "customized_0", "SLAVE", "MasterSlave");
    setCustomizedIdealState("customized", "customized_0", INSTANCE_NAME);

    InstanceReplicaStatus offlineStatus =
        _admin.getInstanceReplicaStatus(CLUSTER_NAME, INSTANCE_NAME);
    Assert.assertFalse(offlineStatus.isLive());
    Assert.assertEquals(offlineStatus.getDrain().getCoverage(), CoverageStatus.COMPLETE);
    Assert.assertFalse(offlineStatus.getDrain().isDrained());
    Assert.assertEquals(resourcesByName(offlineStatus).get("fullAuto")
        .getDrainBlockingReplicaCount(), 0);
    Assert.assertEquals(resourcesByName(offlineStatus).get("customized")
        .getDrainBlockingReplicaCount(), 1);

    setCustomizedIdealState("customized", "customized_0", OTHER_INSTANCE);
    offlineStatus = _admin.getInstanceReplicaStatus(CLUSTER_NAME, INSTANCE_NAME);
    Assert.assertTrue(offlineStatus.getDrain().isDrained());

    String liveSession = setLive();
    _gZkClient.deleteRecursively(
        PropertyPathBuilder.instanceCurrentState(CLUSTER_NAME, INSTANCE_NAME));
    setCurrentState(liveSession, "fullAuto", "fullAuto_0", "MASTER", "MasterSlave");
    setCurrentState(liveSession, "customized", "customized_0", "SLAVE", "MasterSlave");
    InstanceReplicaStatus liveStatus =
        _admin.getInstanceReplicaStatus(CLUSTER_NAME, INSTANCE_NAME);
    Assert.assertFalse(liveStatus.getDrain().isDrained());
    Assert.assertEquals(resourcesByName(liveStatus).get("fullAuto")
        .getDrainBlockingReplicaCount(), 1);
    Assert.assertEquals(resourcesByName(liveStatus).get("customized")
        .getDrainBlockingReplicaCount(), 1);
  }

  @Test
  public void testMissingMetadataAndMultipleSessionsAreIncomplete() {
    String sessionId = setLive();
    setCurrentState(sessionId, "database", "database_0", "OFFLINE", "MasterSlave");

    InstanceReplicaStatus missingIdealState =
        _admin.getInstanceReplicaStatus(CLUSTER_NAME, INSTANCE_NAME);
    Assert.assertEquals(missingIdealState.getReplicaStates().getCoverage(),
        CoverageStatus.COMPLETE);
    Assert.assertEquals(missingIdealState.getDrain().getCoverage(),
        CoverageStatus.INCOMPLETE);
    Assert.assertFalse(missingIdealState.getDrain().isDrained());
    Assert.assertTrue(hasBlocker(missingIdealState, BlockerCode.IDEAL_STATE_MISSING));

    setIdealState("database", IdealState.RebalanceMode.FULL_AUTO);
    setCurrentState(SESSION_2, "database", "database_0", "OFFLINE", "MasterSlave");
    InstanceReplicaStatus multipleSessions =
        _admin.getInstanceReplicaStatus(CLUSTER_NAME, INSTANCE_NAME);
    Assert.assertEquals(multipleSessions.getReplicaStates().getCoverage(),
        CoverageStatus.INCOMPLETE);
    Assert.assertEquals(multipleSessions.getDrain().getCoverage(),
        CoverageStatus.INCOMPLETE);
    Assert.assertFalse(multipleSessions.getDrain().isDrained());
    Assert.assertTrue(hasBlocker(multipleSessions,
        BlockerCode.MULTIPLE_CURRENT_STATE_SESSIONS));

    _accessor.removeProperty(_accessor.keyBuilder().instanceConfig(INSTANCE_NAME));
    InstanceReplicaStatus missingInstanceConfig =
        _admin.getInstanceReplicaStatus(CLUSTER_NAME, INSTANCE_NAME);
    Assert.assertEquals(missingInstanceConfig.getAssignment().getCoverage(),
        CoverageStatus.INCOMPLETE);
    Assert.assertFalse(missingInstanceConfig.getAssignment()
        .isFutureAssignmentEligible());
    Assert.assertEquals(missingInstanceConfig.getDrain().getCoverage(),
        CoverageStatus.INCOMPLETE);
    Assert.assertTrue(hasBlocker(missingInstanceConfig,
        BlockerCode.INSTANCE_CONFIG_MISSING));
  }

  @Test
  public void testFutureAssignmentEligibilityIsIndependentOfDrain() {
    InstanceReplicaStatus status =
        _admin.getInstanceReplicaStatus(CLUSTER_NAME, INSTANCE_NAME);
    Assert.assertEquals(status.getAssignment().getCoverage(), CoverageStatus.COMPLETE);
    Assert.assertTrue(status.getAssignment().isFutureAssignmentEligible());
    Assert.assertTrue(status.getDrain().isDrained());

    _admin.setInstanceOperation(CLUSTER_NAME, INSTANCE_NAME,
        InstanceConstants.InstanceOperation.DISABLE);
    status = _admin.getInstanceReplicaStatus(CLUSTER_NAME, INSTANCE_NAME);
    Assert.assertFalse(status.getAssignment().isFutureAssignmentEligible());
    Assert.assertEquals(status.getAssignment().getInstanceOperation(), "DISABLE");
    Assert.assertTrue(status.getDrain().isDrained());

    _admin.setInstanceOperation(CLUSTER_NAME, INSTANCE_NAME,
        InstanceConstants.InstanceOperation.ENABLE);
    _admin.setInstanceOperation(CLUSTER_NAME, INSTANCE_NAME,
        InstanceConstants.InstanceOperation.EVACUATE);
    status = _admin.getInstanceReplicaStatus(CLUSTER_NAME, INSTANCE_NAME);
    Assert.assertFalse(status.getAssignment().isFutureAssignmentEligible());
    Assert.assertEquals(status.getAssignment().getInstanceOperation(), "EVACUATE");
  }

  @Test
  public void testMetadataReadFailureIsNotConvertedToSuccessfulCoverage() {
    HelixDataAccessor accessor = Mockito.mock(HelixDataAccessor.class);
    BaseDataAccessor<ZNRecord> baseAccessor = Mockito.mock(BaseDataAccessor.class);
    Mockito.when(accessor.keyBuilder()).thenReturn(new PropertyKey.Builder(CLUSTER_NAME));
    Mockito.when(accessor.getProperty(Mockito.anyList(), Mockito.eq(true)))
        .thenThrow(new HelixMetaDataAccessException("read failed"));

    try {
      InstanceReplicaStatusCalculator.calculate(accessor, baseAccessor, CLUSTER_NAME,
          INSTANCE_NAME);
      Assert.fail("A failed metadata read must be propagated.");
    } catch (HelixMetaDataAccessException expected) {
      Assert.assertEquals(expected.getMessage(), "read failed");
    }
  }

  private String setLive() {
    LiveInstance liveInstance = new LiveInstance(INSTANCE_NAME);
    liveInstance.setSessionId(SESSION_1);
    liveInstance.setHelixVersion("1.0.0");
    liveInstance.setLiveInstance(INSTANCE_NAME);
    _accessor.setProperty(_accessor.keyBuilder().liveInstance(INSTANCE_NAME),
        liveInstance);
    LiveInstance storedLiveInstance =
        _accessor.getProperty(_accessor.keyBuilder().liveInstance(INSTANCE_NAME));
    return storedLiveInstance.getEphemeralOwner();
  }

  private void setCurrentState(String sessionId, String resourceName, String partitionName,
      String state, String stateModelDefRef) {
    CurrentState currentState = new CurrentState(resourceName);
    currentState.setSessionId(sessionId);
    currentState.setStateModelDefRef(stateModelDefRef);
    currentState.setState(partitionName, state);
    _accessor.setProperty(
        _accessor.keyBuilder().currentState(INSTANCE_NAME, sessionId, resourceName),
        currentState);
  }

  private void setEmptyCurrentState(String sessionId, String resourceName) {
    CurrentState currentState = new CurrentState(resourceName);
    currentState.setSessionId(sessionId);
    currentState.setStateModelDefRef("MasterSlave");
    _accessor.setProperty(
        _accessor.keyBuilder().currentState(INSTANCE_NAME, sessionId, resourceName),
        currentState);
  }

  private void addCurrentStatePartition(String sessionId, String resourceName,
      String partitionName, String state) {
    CurrentState currentState = _accessor.getProperty(
        _accessor.keyBuilder().currentState(INSTANCE_NAME, sessionId, resourceName));
    currentState.setState(partitionName, state);
    _accessor.setProperty(
        _accessor.keyBuilder().currentState(INSTANCE_NAME, sessionId, resourceName),
        currentState);
  }

  private void setTaskCurrentState(String sessionId, String resourceName,
      String partitionName) {
    CurrentState currentState = new CurrentState(resourceName);
    currentState.setSessionId(sessionId);
    currentState.setStateModelDefRef(TaskConstants.STATE_MODEL_NAME);
    currentState.setState(partitionName, "RUNNING");
    _accessor.setProperty(
        _accessor.keyBuilder().taskCurrentState(INSTANCE_NAME, sessionId, resourceName),
        currentState);
  }

  private void setIdealState(String resourceName, IdealState.RebalanceMode rebalanceMode) {
    IdealState idealState = new IdealState(resourceName);
    idealState.setRebalanceMode(rebalanceMode);
    idealState.setStateModelDefRef("MasterSlave");
    idealState.setNumPartitions(1);
    idealState.setReplicas("1");
    _accessor.setProperty(_accessor.keyBuilder().idealStates(resourceName), idealState);
  }

  private void setCustomizedIdealState(String resourceName, String partitionName,
      String assignedInstance) {
    IdealState idealState = new IdealState(resourceName);
    idealState.setRebalanceMode(IdealState.RebalanceMode.CUSTOMIZED);
    idealState.setStateModelDefRef("MasterSlave");
    idealState.setNumPartitions(1);
    idealState.setReplicas("1");
    idealState.setPartitionState(partitionName, assignedInstance, "MASTER");
    _accessor.setProperty(_accessor.keyBuilder().idealStates(resourceName), idealState);
  }

  private static Map<String, ResourceInfo> resourcesByName(InstanceReplicaStatus status) {
    return status.getDrain().getResources().stream().collect(
        Collectors.toMap(ResourceInfo::getResourceName, resource -> resource));
  }

  private static boolean hasBlocker(InstanceReplicaStatus status, BlockerCode code) {
    return status.getDrain().getBlockers().stream()
        .anyMatch(blocker -> blocker.getCode() == code);
  }
}
