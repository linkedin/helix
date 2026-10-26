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

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

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
import org.apache.helix.model.InstanceReplicaStatus.State;
import org.apache.helix.model.LiveInstance;
import org.apache.helix.model.Message;
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
    // A replica without a current state counts toward replicaCount only.
    PropertyKey currentStateKey =
        _accessor.keyBuilder().currentState(INSTANCE_NAME, sessionId, "db");
    CurrentState currentState = _accessor.getProperty(currentStateKey);
    currentState.getRecord().setMapField("db_3", Collections.singletonMap("INFO", "no state yet"));
    _accessor.setProperty(currentStateKey, currentState);
    setCurrentState(sessionId, "job", TaskConstants.STATE_MODEL_NAME, Map.of("job_0", "RUNNING"));

    InstanceReplicaStatus replicaStatus = getReplicaStatus(Collections.emptySet());
    Assert.assertEquals(replicaStatus.getState(), State.IN_PROGRESS);
    Assert.assertFalse(replicaStatus.isAllReplicasOffline());
    Assert.assertEquals(replicaStatus.getReplicaCount(), Integer.valueOf(4));
    Assert.assertEquals(replicaStatus.getOfflineReplicaCount(), Integer.valueOf(1));
    Assert.assertEquals(replicaStatus.getErrorReplicaCount(), Integer.valueOf(1));
    Assert.assertEquals(replicaStatus.getPendingMessageCount(), Integer.valueOf(0));
    Assert.assertEquals(replicaStatus.getErrorPartitions(), Map.of("db", List.of("db_1")));
    Assert.assertEquals(replicaStatus.getErrorPartitionsTruncated(), Boolean.FALSE);
    // The task resource is left out, so the last activity is the change to the db resource.
    Assert.assertEquals(replicaStatus.getLastActivityTimestamp(),
        Long.valueOf(_accessor.getPropertyStat(currentStateKey).getModifiedTime()));
    Assert.assertFalse(replicaStatus.areAllReplicasInError());
    Assert.assertEquals(getReplicaStatus(ERROR_AS_OFFLINE).getState(), State.IN_PROGRESS);
  }

  @Test
  public void testErrorReplicas() {
    String sessionId = addLiveInstance();
    setCurrentState(sessionId, "db", MASTER_SLAVE, Map.of("db_0", "OFFLINE", "db_1", "ERROR"));
    InstanceReplicaStatus replicaStatus = getReplicaStatus(Collections.emptySet());
    Assert.assertEquals(replicaStatus.getState(), State.BLOCKED_BY_ERROR);
    Assert.assertFalse(replicaStatus.isAllReplicasOffline());
    Assert.assertFalse(replicaStatus.areAllReplicasInError());
    replicaStatus = getReplicaStatus(ERROR_AS_OFFLINE);
    Assert.assertEquals(replicaStatus.getState(), State.REPLICAS_OFFLINE);
    Assert.assertTrue(replicaStatus.isAllReplicasOffline());

    setCurrentState(sessionId, "db", MASTER_SLAVE, Map.of("db_1", "ERROR", "db_0", "ERROR"));
    // Partition names can repeat across resources; each is listed under its own resource.
    setCurrentState(sessionId, "other", MASTER_SLAVE, Map.of("db_0", "ERROR"));
    replicaStatus = getReplicaStatus(Collections.emptySet());
    Assert.assertEquals(replicaStatus.getState(), State.BLOCKED_BY_ERROR);
    Assert.assertEquals(replicaStatus.getOfflineReplicaCount(), Integer.valueOf(0));
    Assert.assertEquals(replicaStatus.getErrorReplicaCount(), Integer.valueOf(3));
    Assert.assertEquals(replicaStatus.getErrorPartitions(),
        Map.of("db", List.of("db_0", "db_1"), "other", List.of("db_0")));
    Assert.assertTrue(replicaStatus.areAllReplicasInError());
    Assert.assertEquals(getReplicaStatus(ERROR_AS_OFFLINE).getState(), State.REPLICAS_OFFLINE);
  }

  @Test
  public void testErrorPartitionsAreCapped() {
    String sessionId = addLiveInstance();
    Map<String, String> errorStatesOfA = errorStates("a", 60);
    Map<String, String> errorStatesOfB = errorStates("b", 60);
    setCurrentState(sessionId, "a", MASTER_SLAVE, errorStatesOfA);
    setCurrentState(sessionId, "b", MASTER_SLAVE, errorStatesOfB);
    InstanceReplicaStatus replicaStatus = getReplicaStatus(Collections.emptySet());
    Assert.assertEquals(replicaStatus.getErrorReplicaCount(), Integer.valueOf(120));
    Assert.assertTrue(replicaStatus.areAllReplicasInError());
    Assert.assertEquals(replicaStatus.getErrorPartitionsTruncated(), Boolean.TRUE);
    // Resources and their partitions are listed in name order until the cap is reached.
    Assert.assertEquals(replicaStatus.getErrorPartitions().get("a"),
        new ArrayList<>(new TreeSet<>(errorStatesOfA.keySet())));
    Assert.assertEquals(replicaStatus.getErrorPartitions().get("b"),
        new ArrayList<>(new TreeSet<>(errorStatesOfB.keySet()))
            .subList(0, InstanceReplicaStatus.MAX_ERROR_PARTITIONS - 60));

    // Exactly at the cap, nothing is left out.
    setCurrentState(sessionId, "b", MASTER_SLAVE, errorStates("b", 40));
    replicaStatus = getReplicaStatus(Collections.emptySet());
    Assert.assertEquals(replicaStatus.getErrorReplicaCount(),
        Integer.valueOf(InstanceReplicaStatus.MAX_ERROR_PARTITIONS));
    Assert.assertEquals(replicaStatus.getErrorPartitions().get("b").size(), 40);
    Assert.assertEquals(replicaStatus.getErrorPartitionsTruncated(), Boolean.FALSE);
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
    Assert.assertEquals(replicaStatus.getState(), State.REPLICAS_OFFLINE);
    Assert.assertTrue(replicaStatus.isAllReplicasOffline());

    // Without its state model definition, a replica is never offline.
    setCurrentState(sessionId, "unknown", "Missing", Map.of("unknown_0", "OFFLINE"));
    replicaStatus = getReplicaStatus(Collections.emptySet());
    Assert.assertEquals(replicaStatus.getReplicaCount(), Integer.valueOf(3));
    Assert.assertEquals(replicaStatus.getOfflineReplicaCount(), Integer.valueOf(2));
    Assert.assertEquals(replicaStatus.getState(), State.IN_PROGRESS);
  }

  @Test
  public void testNotLiveInstance() {
    setCurrentState("1001", "db", MASTER_SLAVE, Map.of("db_0", "MASTER"));
    InstanceReplicaStatus replicaStatus = getReplicaStatus(Collections.emptySet());
    Assert.assertEquals(replicaStatus.getState(), State.NOT_LIVE);
    Assert.assertTrue(replicaStatus.isAllReplicasOffline());
    Assert.assertEquals(replicaStatus.getReplicaCount(), Integer.valueOf(0));
    Assert.assertEquals(replicaStatus.getPendingMessageCount(), Integer.valueOf(0));
    Assert.assertEquals(replicaStatus.getErrorPartitions(), Collections.emptyMap());
    Assert.assertNull(replicaStatus.getLastActivityTimestamp());
    Assert.assertTrue(replicaStatus.areAllReplicasInError());
  }

  @Test
  public void testCarryingOverAPreviousSession() {
    String sessionId = addLiveInstance();
    // A frozen participant carries over the current states of its previous session only when it
    // unfreezes, so until then they may describe replicas that are serving.
    setCurrentState("1001", "db", MASTER_SLAVE, Map.of("db_0", "MASTER"));
    InstanceReplicaStatus replicaStatus = getReplicaStatus(ERROR_AS_OFFLINE);
    Assert.assertEquals(replicaStatus.getState(), State.MULTIPLE_SESSIONS);
    Assert.assertFalse(replicaStatus.isAllReplicasOffline());
    Assert.assertFalse(replicaStatus.areAllReplicasInError());
    Assert.assertEquals(ImmutableSet.copyOf(OBJECT_MAPPER.valueToTree(replicaStatus).fieldNames()),
        Set.of("state", "allReplicasOffline"));

    setCurrentState(sessionId, "db", MASTER_SLAVE, Map.of("db_0", "OFFLINE"));
    Assert.assertEquals(getReplicaStatus(ERROR_AS_OFFLINE).getState(), State.MULTIPLE_SESSIONS);

    _accessor.removeProperty(_accessor.keyBuilder().currentStates(INSTANCE_NAME, "1001"));
    Assert.assertEquals(getReplicaStatus(ERROR_AS_OFFLINE).getState(), State.REPLICAS_OFFLINE);
  }

  @Test
  public void testPendingMessages() {
    String sessionId = addLiveInstance();
    setCurrentState(sessionId, "db", MASTER_SLAVE, Map.of("db_0", "OFFLINE"));
    // A message on its way can still bring a replica up.
    Message message = new Message(Message.MessageType.STATE_TRANSITION, "message_0");
    message.setTgtName(INSTANCE_NAME);
    message.setTgtSessionId(sessionId);
    message.setResourceName("db");
    message.setPartitionName("db_0");
    message.setStateModelDef(MASTER_SLAVE);
    message.setFromState("OFFLINE");
    message.setToState("SLAVE");
    PropertyKey messageKey = _accessor.keyBuilder().message(INSTANCE_NAME, message.getId());
    _accessor.setProperty(messageKey, message);
    InstanceReplicaStatus replicaStatus = getReplicaStatus(ERROR_AS_OFFLINE);
    Assert.assertEquals(replicaStatus.getState(), State.IN_PROGRESS);
    Assert.assertFalse(replicaStatus.isAllReplicasOffline());
    Assert.assertEquals(replicaStatus.getPendingMessageCount(), Integer.valueOf(1));

    _accessor.removeProperty(messageKey);
    Assert.assertEquals(getReplicaStatus(ERROR_AS_OFFLINE).getState(), State.REPLICAS_OFFLINE);
  }

  @Test
  public void testLastActivityOfABucketizedResource() throws Exception {
    String sessionId = addLiveInstance();
    // Like a participant, write the resource's metadata to its node and its states to a bucket.
    CurrentState metadata = new CurrentState("db");
    metadata.setSessionId(sessionId);
    metadata.setStateModelDefRef(MASTER_SLAVE);
    metadata.setBucketSize(2);
    PropertyKey currentStateKey =
        _accessor.keyBuilder().currentState(INSTANCE_NAME, sessionId, "db");
    _accessor.setProperty(currentStateKey, metadata);
    PropertyKey bucketKey =
        _accessor.keyBuilder().currentState(INSTANCE_NAME, sessionId, "db", "db_p0-p1");
    CurrentState partitionStates = new CurrentState("db");
    partitionStates.setState("db_0", "OFFLINE");
    partitionStates.setState("db_1", "OFFLINE");
    _accessor.updateProperty(bucketKey, partitionStates);

    // A later state transition updates only the bucket.
    Thread.sleep(10);
    CurrentState transition = new CurrentState("db");
    transition.setState("db_0", "SLAVE");
    _accessor.updateProperty(bucketKey, transition);

    InstanceReplicaStatus replicaStatus = getReplicaStatus(Collections.emptySet());
    Assert.assertEquals(replicaStatus.getReplicaCount(), Integer.valueOf(2));
    Assert.assertEquals(replicaStatus.getState(), State.IN_PROGRESS);
    long bucketModifiedTime = _accessor.getPropertyStat(bucketKey).getModifiedTime();
    Assert.assertTrue(
        bucketModifiedTime > _accessor.getPropertyStat(currentStateKey).getModifiedTime());
    Assert.assertEquals(replicaStatus.getLastActivityTimestamp(), Long.valueOf(bucketModifiedTime));
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
        Set.of("state", "allReplicasOffline", "replicaCount", "offlineReplicaCount",
            "errorReplicaCount", "pendingMessageCount", "lastActivityTimestamp", "errorPartitions",
            "errorPartitionsTruncated"));
    // Readers ignore fields added later, and allReplicasOffline, which is derived from the state.
    json.put("futureField", 1);
    Assert.assertEquals(OBJECT_MAPPER.treeToValue(json, InstanceReplicaStatus.class).toString(),
        replicaStatus.toString());

    // Without the state and the counts, nothing counts as offline or in ERROR.
    InstanceReplicaStatus empty = OBJECT_MAPPER.readValue("{}", InstanceReplicaStatus.class);
    Assert.assertFalse(empty.isAllReplicasOffline());
    Assert.assertFalse(empty.areAllReplicasInError());
  }

  private static Map<String, String> errorStates(String resource, int partitionCount) {
    return IntStream.range(0, partitionCount).boxed()
        .collect(Collectors.toMap(i -> resource + "_" + i, i -> "ERROR"));
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
