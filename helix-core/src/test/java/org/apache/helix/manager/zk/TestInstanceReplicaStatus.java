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

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.common.collect.ImmutableSet;
import org.apache.helix.PropertyKey;
import org.apache.helix.ZkUnitTestBase;
import org.apache.helix.constants.InstanceDrainExclusionType;
import org.apache.helix.model.BuiltInStateModelDefinitions;
import org.apache.helix.model.CurrentState;
import org.apache.helix.model.InstanceConfig;
import org.apache.helix.model.InstanceReplicaStatus;
import org.apache.helix.model.LiveInstance;
import org.apache.helix.model.StateModelDefinition;
import org.apache.helix.task.TaskConstants;
import org.testng.Assert;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

public class TestInstanceReplicaStatus extends ZkUnitTestBase {
  private static final String CLUSTER_NAME = "TestInstanceReplicaStatus";
  private static final String INSTANCE_NAME = "localhost_12918";
  private static final String MASTER_SLAVE = "MasterSlave";
  private static final Set<InstanceDrainExclusionType> ERROR_AS_OFFLINE =
      Collections.singleton(InstanceDrainExclusionType.ERROR_PARTITIONS);
  private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

  private ZKHelixAdmin _admin;
  private ZKHelixDataAccessor _accessor;

  @BeforeMethod
  public void beforeMethod() {
    _admin = new ZKHelixAdmin(_gZkClient);
    _admin.addCluster(CLUSTER_NAME, true);
    _admin.addStateModelDef(CLUSTER_NAME, MASTER_SLAVE,
        BuiltInStateModelDefinitions.MasterSlave.getStateModelDefinition());
    _admin.addInstance(CLUSTER_NAME, new InstanceConfig(INSTANCE_NAME));
    _accessor = new ZKHelixDataAccessor(CLUSTER_NAME, new ZkBaseDataAccessor<>(_gZkClient));
  }

  @AfterMethod
  public void afterMethod() {
    _gZkClient.deleteRecursively("/" + CLUSTER_NAME);
  }

  @Test
  public void testReplicaStatesOfLiveSession() {
    String sessionId = addLiveInstance();
    setCurrentState(sessionId, "db", MASTER_SLAVE,
        Map.of("db_0", "OFFLINE", "db_1", "ERROR", "db_2", "MASTER"));
    setCurrentState(sessionId, "job", TaskConstants.STATE_MODEL_NAME, Map.of("job_0", "RUNNING"));
    // A replica without a current state counts toward replicaCount only.
    PropertyKey currentStateKey =
        _accessor.keyBuilder().currentState(INSTANCE_NAME, sessionId, "db");
    CurrentState currentState = _accessor.getProperty(currentStateKey);
    currentState.getRecord().setMapField("db_3", Collections.singletonMap("INFO", "no state yet"));
    _accessor.setProperty(currentStateKey, currentState);

    InstanceReplicaStatus replicaStatus = getReplicaStatus(Collections.emptySet());
    Assert.assertFalse(replicaStatus.isOffline());
    Assert.assertEquals(replicaStatus.getReplicaCount(), Integer.valueOf(4));
    Assert.assertEquals(replicaStatus.getOfflineReplicaCount(), Integer.valueOf(1));
    Assert.assertEquals(replicaStatus.getErrorPartitions(), List.of("db_1"));
    Assert.assertFalse(replicaStatus.areAllReplicasInError());
    Assert.assertFalse(getReplicaStatus(ERROR_AS_OFFLINE).isOffline());
  }

  @Test
  public void testErrorReplicas() {
    String sessionId = addLiveInstance();
    setCurrentState(sessionId, "db", MASTER_SLAVE, Map.of("db_0", "OFFLINE", "db_1", "ERROR"));
    Assert.assertFalse(getReplicaStatus(Collections.emptySet()).isOffline());
    Assert.assertTrue(getReplicaStatus(ERROR_AS_OFFLINE).isOffline());

    setCurrentState(sessionId, "db", MASTER_SLAVE, Map.of("db_1", "ERROR", "db_0", "ERROR"));
    // Partition names can repeat across resources; each replica in ERROR is listed.
    setCurrentState(sessionId, "other", MASTER_SLAVE, Map.of("db_0", "ERROR"));
    InstanceReplicaStatus replicaStatus = getReplicaStatus(Collections.emptySet());
    Assert.assertEquals(replicaStatus.getOfflineReplicaCount(), Integer.valueOf(0));
    Assert.assertEquals(replicaStatus.getErrorPartitions(), List.of("db_0", "db_0", "db_1"));
    Assert.assertTrue(replicaStatus.areAllReplicasInError());
    Assert.assertTrue(getReplicaStatus(ERROR_AS_OFFLINE).isOffline());
  }

  @Test
  public void testOfflineIsTheInitialStateOfEachStateModel() {
    _admin.addStateModelDef(CLUSTER_NAME, "InitFirst",
        new StateModelDefinition.Builder("InitFirst").initialState("INIT").addState("ONLINE", 0)
            .addState("INIT", 1).addState("DROPPED", 2).addTransition("INIT", "ONLINE")
            .addTransition("ONLINE", "INIT").addTransition("INIT", "DROPPED").build());
    String sessionId = addLiveInstance();
    setCurrentState(sessionId, "db", MASTER_SLAVE, Map.of("db_0", "OFFLINE"));
    setCurrentState(sessionId, "custom", "InitFirst", Map.of("custom_0", "INIT"));
    InstanceReplicaStatus replicaStatus = getReplicaStatus(Collections.emptySet());
    Assert.assertEquals(replicaStatus.getOfflineReplicaCount(), Integer.valueOf(2));
    Assert.assertTrue(replicaStatus.isOffline());

    // Without its state model definition, a replica is never offline.
    setCurrentState(sessionId, "unknown", "Missing", Map.of("unknown_0", "OFFLINE"));
    replicaStatus = getReplicaStatus(Collections.emptySet());
    Assert.assertEquals(replicaStatus.getReplicaCount(), Integer.valueOf(3));
    Assert.assertEquals(replicaStatus.getOfflineReplicaCount(), Integer.valueOf(2));
    Assert.assertFalse(replicaStatus.isOffline());
  }

  @Test
  public void testNotLiveInstanceHasNoReplicas() {
    setCurrentState("1001", "db", MASTER_SLAVE, Map.of("db_0", "MASTER"));
    InstanceReplicaStatus replicaStatus = getReplicaStatus(Collections.emptySet());
    Assert.assertTrue(replicaStatus.isOffline());
    Assert.assertEquals(replicaStatus.getReplicaCount(), Integer.valueOf(0));
    Assert.assertEquals(replicaStatus.getErrorPartitions(), Collections.emptyList());
    Assert.assertTrue(replicaStatus.areAllReplicasInError());
  }

  @Test(expectedExceptions = IllegalArgumentException.class)
  public void testOnlyErrorPartitionsCanBeExcluded() {
    getReplicaStatus(Collections.singleton(InstanceDrainExclusionType.DISABLED_PARTITION));
  }

  @Test
  public void testJsonFields() throws Exception {
    String sessionId = addLiveInstance();
    setCurrentState(sessionId, "db", MASTER_SLAVE, Map.of("db_0", "OFFLINE", "db_1", "ERROR"));
    InstanceReplicaStatus replicaStatus = getReplicaStatus(Collections.emptySet());

    ObjectNode json = OBJECT_MAPPER.valueToTree(replicaStatus);
    Assert.assertEquals(ImmutableSet.copyOf(json.fieldNames()),
        Set.of("offline", "replicaCount", "offlineReplicaCount", "errorPartitions"));
    // Readers ignore fields added later.
    json.put("futureField", 1);
    Assert.assertEquals(OBJECT_MAPPER.treeToValue(json, InstanceReplicaStatus.class).toString(),
        replicaStatus.toString());

    // Without the replica fields, nothing counts as offline or in ERROR.
    InstanceReplicaStatus empty = OBJECT_MAPPER.readValue("{}", InstanceReplicaStatus.class);
    Assert.assertFalse(empty.isOffline());
    Assert.assertFalse(empty.areAllReplicasInError());
  }

  private InstanceReplicaStatus getReplicaStatus(Set<InstanceDrainExclusionType> exclusionTypes) {
    return _admin.getInstanceReplicaStatus(CLUSTER_NAME, INSTANCE_NAME, exclusionTypes);
  }

  private String addLiveInstance() {
    LiveInstance liveInstance = new LiveInstance(INSTANCE_NAME);
    liveInstance.setSessionId("1000");
    liveInstance.setHelixVersion("1.0.0");
    PropertyKey key = _accessor.keyBuilder().liveInstance(INSTANCE_NAME);
    _accessor.setProperty(key, liveInstance);
    // The node is ephemeral, so the live session is the ZK session that wrote it.
    return _accessor.<LiveInstance>getProperty(key).getEphemeralOwner();
  }

  private void setCurrentState(String sessionId, String resource, String stateModelDefRef,
      Map<String, String> partitionStates) {
    CurrentState currentState = new CurrentState(resource);
    currentState.setSessionId(sessionId);
    currentState.setStateModelDefRef(stateModelDefRef);
    partitionStates.forEach(currentState::setState);
    _accessor.setProperty(_accessor.keyBuilder().currentState(INSTANCE_NAME, sessionId, resource),
        currentState);
  }
}
