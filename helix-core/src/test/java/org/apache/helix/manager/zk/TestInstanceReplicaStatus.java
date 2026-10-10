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
import org.apache.helix.BaseDataAccessor;
import org.apache.helix.HelixDataAccessor;
import org.apache.helix.PropertyKey;
import org.apache.helix.PropertyPathBuilder;
import org.apache.helix.ZkUnitTestBase;
import org.apache.helix.api.exceptions.HelixMetaDataAccessException;
import org.apache.helix.constants.InstanceConstants;
import org.apache.helix.model.BuiltInStateModelDefinitions;
import org.apache.helix.model.CurrentState;
import org.apache.helix.model.IdealState;
import org.apache.helix.model.InstanceConfig;
import org.apache.helix.model.InstanceReplicaStatus;
import org.apache.helix.model.InstanceReplicaStatus.CoverageStatus;
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
  private static final String STATE_MODEL = "MasterSlave";

  private ZKHelixAdmin _admin;
  private ZKHelixDataAccessor _accessor;

  @BeforeMethod
  public void beforeMethod() {
    if (_gZkClient.exists("/" + CLUSTER_NAME)) {
      _gZkClient.deleteRecursively("/" + CLUSTER_NAME);
    }
    _admin = new ZKHelixAdmin(_gZkClient);
    _admin.addCluster(CLUSTER_NAME, true);
    // A replica counts as offline when it sits in its resource's state-model initial state, so the
    // state model every fixture references has to be resolvable or the observation is incomplete.
    _admin.addStateModelDef(CLUSTER_NAME, STATE_MODEL,
        BuiltInStateModelDefinitions.MasterSlave.getStateModelDefinition());
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
    setCurrentState(sessionId, "fullAuto", "fullAuto_0", "OFFLINE", STATE_MODEL);
    setIdealState("fullAuto", IdealState.RebalanceMode.FULL_AUTO);
    setCurrentState(sessionId, "customized", "customized_0", "ERROR", STATE_MODEL);
    setCustomizedIdealState("customized", "customized_0", OTHER_INSTANCE);
    setCurrentState(sessionId, "semiAuto", "semiAuto_0", "OFFLINE", STATE_MODEL);
    setIdealState("semiAuto", IdealState.RebalanceMode.SEMI_AUTO);
    setTaskCurrentState(sessionId, "workflow", "workflow_0");

    InstanceReplicaStatus status =
        _admin.getInstanceReplicaStatus(CLUSTER_NAME, INSTANCE_NAME);

    Assert.assertTrue(status.isLive());
    Assert.assertEquals(status.getClusterName(), CLUSTER_NAME);
    Assert.assertEquals(status.getInstanceName(), INSTANCE_NAME);
    Assert.assertEquals(status.getReplicaCoverage(), CoverageStatus.COMPLETE);
    Assert.assertEquals(status.getReplicaCount(), 3);
    Assert.assertFalse(status.isReplicaScopeEmpty());
    Assert.assertFalse(status.isAllOffline());
    Assert.assertTrue(status.isAllOfflineOrError());
    Assert.assertFalse(status.isAllError());
    Assert.assertEquals(status.getOfflineReplicaCount(), 2);
    Assert.assertEquals(status.getErrorReplicaCount(), 1);
    Assert.assertEquals(status.getStateCounts(), Map.of("ERROR", 1, "OFFLINE", 2));
    Assert.assertEquals(status.getExcludedTaskResources(), List.of("workflow"));

    Assert.assertEquals(status.getDrainCoverage(), CoverageStatus.UNSUPPORTED);
    Assert.assertFalse(status.isDrained());
    // fullAuto and customized are inside the native drain scope, so each of their replicas is
    // reported as blocking the drain of this live instance.
    Assert.assertTrue(
        hasBlocker(status, "1 replica(s) of resource fullAuto remain in current state"));
    Assert.assertTrue(
        hasBlocker(status, "1 replica(s) of resource customized remain in current state"));
    // semiAuto is outside the native drain scope, so it is reported as unsupported and is never
    // evaluated for blocking replicas.
    Assert.assertTrue(hasBlocker(status,
        "Resource semiAuto uses rebalance mode SEMI_AUTO, which is outside the native drain "
            + "scope."));
    Assert.assertFalse(hasBlocker(status, "replica(s) of resource semiAuto"));
  }

  @Test
  public void testOfflineAndOfflineOrErrorRemainDistinct() {
    String sessionId = setLive();
    setCurrentState(sessionId, "database", "database_0", "OFFLINE", STATE_MODEL);
    addCurrentStatePartition(sessionId, "database", "database_1", "ERROR");
    setIdealState("database", IdealState.RebalanceMode.FULL_AUTO);

    InstanceReplicaStatus status =
        _admin.getInstanceReplicaStatus(CLUSTER_NAME, INSTANCE_NAME);

    Assert.assertEquals(status.getReplicaCoverage(), CoverageStatus.COMPLETE);
    Assert.assertFalse(status.isAllOffline());
    Assert.assertTrue(status.isAllOfflineOrError());
    Assert.assertFalse(status.isAllError());
    Assert.assertEquals(status.getErrorReplicaCount(), 1);
    Assert.assertEquals(status.getErrorPartitionNames(), List.of("database_1"));
    Assert.assertFalse(status.isDrained());

    addCurrentStatePartition(sessionId, "database", "database_0", "ERROR");
    status = _admin.getInstanceReplicaStatus(CLUSTER_NAME, INSTANCE_NAME);
    Assert.assertTrue(status.isAllError());
    Assert.assertEquals(status.getErrorReplicaCount(), 2);
    Assert.assertEquals(status.getErrorPartitionNames(),
        List.of("database_0", "database_1"));

    addCurrentStatePartition(sessionId, "database", "database_0", "OFFLINE");
    addCurrentStatePartition(sessionId, "database", "database_1", "OFFLINE");
    status = _admin.getInstanceReplicaStatus(CLUSTER_NAME, INSTANCE_NAME);
    Assert.assertTrue(status.isAllOffline());
    Assert.assertTrue(status.isAllOfflineOrError());
    Assert.assertEquals(status.getErrorReplicaCount(), 0);
    Assert.assertTrue(status.getErrorPartitionNames().isEmpty());
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

    Assert.assertEquals(status.getReplicaCoverage(), CoverageStatus.COMPLETE);
    Assert.assertTrue(status.isReplicaScopeEmpty());
    Assert.assertFalse(status.isAllOffline());
    Assert.assertFalse(status.isAllOfflineOrError());
    Assert.assertEquals(status.getDrainCoverage(), CoverageStatus.COMPLETE);
    Assert.assertEquals(status.getPendingMessageCount(), 1);
    Assert.assertFalse(status.isDrained());
    Assert.assertTrue(hasBlocker(status, "1 pending message(s) remain on the live instance."));

    _gZkClient.delete(messagePath);
    status = _admin.getInstanceReplicaStatus(CLUSTER_NAME, INSTANCE_NAME);
    Assert.assertTrue(status.isReplicaScopeEmpty());
    Assert.assertEquals(status.getPendingMessageCount(), 0);
    Assert.assertTrue(status.isDrained());
    Assert.assertTrue(status.getBlockers().isEmpty());
  }

  @Test
  public void testLiveAndOfflineDrainUseDifferentNativeRules() {
    setCurrentState(SESSION_1, "fullAuto", "fullAuto_0", "MASTER", STATE_MODEL);
    setIdealState("fullAuto", IdealState.RebalanceMode.FULL_AUTO);
    setCurrentState(SESSION_1, "customized", "customized_0", "SLAVE", STATE_MODEL);
    setCustomizedIdealState("customized", "customized_0", INSTANCE_NAME);

    InstanceReplicaStatus offlineStatus =
        _admin.getInstanceReplicaStatus(CLUSTER_NAME, INSTANCE_NAME);
    Assert.assertFalse(offlineStatus.isLive());
    Assert.assertEquals(offlineStatus.getReplicaCoverage(), CoverageStatus.COMPLETE);
    Assert.assertEquals(offlineStatus.getDrainCoverage(), CoverageStatus.COMPLETE);
    Assert.assertFalse(offlineStatus.isDrained());
    // An offline instance is only blocked by customized assignments that still name it, so the
    // FULL_AUTO replica contributes nothing and the customized replica contributes one.
    Assert.assertTrue(hasBlocker(offlineStatus, "1 replica(s) of resource customized are still "
        + "referenced by a customized assignment on the offline instance."));
    Assert.assertFalse(hasBlocker(offlineStatus, "resource fullAuto"));

    setCustomizedIdealState("customized", "customized_0", OTHER_INSTANCE);
    offlineStatus = _admin.getInstanceReplicaStatus(CLUSTER_NAME, INSTANCE_NAME);
    Assert.assertTrue(offlineStatus.isDrained());
    Assert.assertTrue(offlineStatus.getBlockers().isEmpty());

    String liveSession = setLive();
    _gZkClient.deleteRecursively(
        PropertyPathBuilder.instanceCurrentState(CLUSTER_NAME, INSTANCE_NAME));
    setCurrentState(liveSession, "fullAuto", "fullAuto_0", "MASTER", STATE_MODEL);
    setCurrentState(liveSession, "customized", "customized_0", "SLAVE", STATE_MODEL);
    InstanceReplicaStatus liveStatus =
        _admin.getInstanceReplicaStatus(CLUSTER_NAME, INSTANCE_NAME);
    Assert.assertEquals(liveStatus.getDrainCoverage(), CoverageStatus.COMPLETE);
    Assert.assertFalse(liveStatus.isDrained());
    // A live instance is blocked by every replica it still holds, whatever the ideal state says.
    Assert.assertTrue(hasBlocker(liveStatus,
        "1 replica(s) of resource fullAuto remain in current state on the live instance."));
    Assert.assertTrue(hasBlocker(liveStatus,
        "1 replica(s) of resource customized remain in current state on the live instance."));
  }

  @Test
  public void testMissingMetadataAndMultipleSessionsAreIncomplete() {
    String sessionId = setLive();
    setCurrentState(sessionId, "database", "database_0", "OFFLINE", STATE_MODEL);

    InstanceReplicaStatus missingIdealState =
        _admin.getInstanceReplicaStatus(CLUSTER_NAME, INSTANCE_NAME);
    Assert.assertEquals(missingIdealState.getReplicaCoverage(), CoverageStatus.COMPLETE);
    Assert.assertEquals(missingIdealState.getDrainCoverage(), CoverageStatus.INCOMPLETE);
    Assert.assertFalse(missingIdealState.isDrained());
    Assert.assertTrue(hasBlocker(missingIdealState,
        "Ideal-state metadata is missing for current-state resource database."));

    setIdealState("database", IdealState.RebalanceMode.FULL_AUTO);
    setCurrentState(SESSION_2, "database", "database_0", "OFFLINE", STATE_MODEL);
    InstanceReplicaStatus multipleSessions =
        _admin.getInstanceReplicaStatus(CLUSTER_NAME, INSTANCE_NAME);
    Assert.assertEquals(multipleSessions.getReplicaCoverage(), CoverageStatus.INCOMPLETE);
    Assert.assertEquals(multipleSessions.getDrainCoverage(), CoverageStatus.INCOMPLETE);
    Assert.assertFalse(multipleSessions.isDrained());
    Assert.assertTrue(
        hasBlocker(multipleSessions, "Multiple current-state sessions were observed:"));

    _accessor.removeProperty(_accessor.keyBuilder().instanceConfig(INSTANCE_NAME));
    InstanceReplicaStatus missingInstanceConfig =
        _admin.getInstanceReplicaStatus(CLUSTER_NAME, INSTANCE_NAME);
    Assert.assertEquals(missingInstanceConfig.getDrainCoverage(), CoverageStatus.INCOMPLETE);
    Assert.assertFalse(missingInstanceConfig.isDrained());
    Assert.assertTrue(
        hasBlocker(missingInstanceConfig, "Instance configuration is missing."));
  }

  @Test
  public void testDrainObservationIsIndependentOfInstanceOperation() {
    InstanceReplicaStatus status =
        _admin.getInstanceReplicaStatus(CLUSTER_NAME, INSTANCE_NAME);
    Assert.assertEquals(status.getDrainCoverage(), CoverageStatus.COMPLETE);
    Assert.assertTrue(status.isDrained());
    Assert.assertTrue(status.getBlockers().isEmpty());

    _admin.setInstanceOperation(CLUSTER_NAME, INSTANCE_NAME,
        InstanceConstants.InstanceOperation.DISABLE);
    status = _admin.getInstanceReplicaStatus(CLUSTER_NAME, INSTANCE_NAME);
    Assert.assertEquals(status.getDrainCoverage(), CoverageStatus.COMPLETE);
    Assert.assertTrue(status.isDrained());
    Assert.assertTrue(status.getBlockers().isEmpty());

    _admin.setInstanceOperation(CLUSTER_NAME, INSTANCE_NAME,
        InstanceConstants.InstanceOperation.ENABLE);
    _admin.setInstanceOperation(CLUSTER_NAME, INSTANCE_NAME,
        InstanceConstants.InstanceOperation.EVACUATE);
    status = _admin.getInstanceReplicaStatus(CLUSTER_NAME, INSTANCE_NAME);
    Assert.assertEquals(status.getDrainCoverage(), CoverageStatus.COMPLETE);
    Assert.assertTrue(status.isDrained());
    Assert.assertTrue(status.getBlockers().isEmpty());
  }

  @Test
  public void testAbsentCurrentStateRootsRepresentAnEmptyScope() {
    _gZkClient.deleteRecursively(
        PropertyPathBuilder.instanceCurrentState(CLUSTER_NAME, INSTANCE_NAME));
    _gZkClient.deleteRecursively(
        PropertyPathBuilder.instanceTaskCurrentState(CLUSTER_NAME, INSTANCE_NAME));

    InstanceReplicaStatus status =
        _admin.getInstanceReplicaStatus(CLUSTER_NAME, INSTANCE_NAME);

    Assert.assertEquals(status.getReplicaCoverage(), CoverageStatus.COMPLETE);
    Assert.assertTrue(status.isReplicaScopeEmpty());
    Assert.assertTrue(status.getExcludedTaskResources().isEmpty());
    Assert.assertEquals(status.getDrainCoverage(), CoverageStatus.COMPLETE);
    Assert.assertTrue(status.isDrained());
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
    currentState.setStateModelDefRef(STATE_MODEL);
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
    idealState.setStateModelDefRef(STATE_MODEL);
    idealState.setNumPartitions(1);
    idealState.setReplicas("1");
    _accessor.setProperty(_accessor.keyBuilder().idealStates(resourceName), idealState);
  }

  private void setCustomizedIdealState(String resourceName, String partitionName,
      String assignedInstance) {
    IdealState idealState = new IdealState(resourceName);
    idealState.setRebalanceMode(IdealState.RebalanceMode.CUSTOMIZED);
    idealState.setStateModelDefRef(STATE_MODEL);
    idealState.setNumPartitions(1);
    idealState.setReplicas("1");
    idealState.setPartitionState(partitionName, assignedInstance, "MASTER");
    _accessor.setProperty(_accessor.keyBuilder().idealStates(resourceName), idealState);
  }

  private static boolean hasBlocker(InstanceReplicaStatus status, String messageFragment) {
    return status.getBlockers().stream()
        .anyMatch(blocker -> blocker.contains(messageFragment));
  }
}
