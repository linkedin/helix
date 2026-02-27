package org.apache.helix.controller.stages.intermediate;

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
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.UUID;

import org.apache.helix.controller.dataproviders.ResourceControllerDataProvider;
import org.apache.helix.controller.stages.BestPossibleStateOutput;
import org.apache.helix.controller.stages.CurrentStateOutput;
import org.apache.helix.model.BuiltInStateModelDefinitions;
import org.apache.helix.model.ClusterConfig;
import org.apache.helix.model.IdealState;
import org.apache.helix.model.Message;
import org.apache.helix.model.Partition;
import org.apache.helix.model.ResourceConfig;
import org.apache.helix.model.StateModelDefinition;
import org.testng.Assert;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;


public class TestResourcePriorityOrderingStrategy {

  private static final String RESOURCE_A = "ResourceA";
  private static final String RESOURCE_B = "ResourceB";
  private static final String PARTITION_A0 = "ResourceA_0";
  private static final String PARTITION_A1 = "ResourceA_1";
  private static final String PARTITION_B0 = "ResourceB_0";
  private static final String INSTANCE_0 = "localhost_0";
  private static final String INSTANCE_1 = "localhost_1";
  private static final String INSTANCE_2 = "localhost_2";
  private static final String PRIORITY_FIELD = "PRIORITY";

  private StateModelDefinition _masterSlaveSMD;
  private ResourceControllerDataProvider _cache;
  private BestPossibleStateOutput _bestPossibleStateOutput;
  private CurrentStateOutput _currentStateOutput;
  private ClusterConfig _clusterConfig;

  @BeforeMethod
  public void setup() {
    _masterSlaveSMD = BuiltInStateModelDefinitions.MasterSlave.getStateModelDefinition();
    _cache = mock(ResourceControllerDataProvider.class);
    _bestPossibleStateOutput = new BestPossibleStateOutput();
    _currentStateOutput = new CurrentStateOutput();
    _clusterConfig = new ClusterConfig("TestCluster");

    when(_cache.getClusterConfig()).thenReturn(_clusterConfig);
    when(_cache.getIdealState(RESOURCE_A)).thenReturn(createIdealState(RESOURCE_A));
    when(_cache.getIdealState(RESOURCE_B)).thenReturn(createIdealState(RESOURCE_B));
    when(_cache.getStateModelDef("MasterSlave")).thenReturn(_masterSlaveSMD);
  }

  // ========================================
  // Resource priority ordering
  // ========================================

  @Test
  public void testResourcePriority_HigherPriorityResourceFirst() {
    _clusterConfig.setResourcePriorityField(PRIORITY_FIELD);

    IdealState isA = createIdealState(RESOURCE_A);
    isA.getRecord().setSimpleField(PRIORITY_FIELD, "10");
    IdealState isB = createIdealState(RESOURCE_B);
    isB.getRecord().setSimpleField(PRIORITY_FIELD, "5");
    when(_cache.getIdealState(RESOURCE_A)).thenReturn(isA);
    when(_cache.getIdealState(RESOURCE_B)).thenReturn(isB);

    Message msgA = createMessage(RESOURCE_A, PARTITION_A0, "OFFLINE", "SLAVE", INSTANCE_0);
    Message msgB = createMessage(RESOURCE_B, PARTITION_B0, "OFFLINE", "SLAVE", INSTANCE_1);

    assertComesBefore(msgA, msgB);
  }

  @Test
  public void testResourcePriority_ResourceConfigOverridesIdealState() {
    _clusterConfig.setResourcePriorityField(PRIORITY_FIELD);

    // IdealState says priority=1 but ResourceConfig overrides to 20 for RESOURCE_A
    IdealState isA = createIdealState(RESOURCE_A);
    isA.getRecord().setSimpleField(PRIORITY_FIELD, "1");
    when(_cache.getIdealState(RESOURCE_A)).thenReturn(isA);

    ResourceConfig rcA = new ResourceConfig(RESOURCE_A);
    rcA.putSimpleConfig(PRIORITY_FIELD, "20");
    when(_cache.getResourceConfig(RESOURCE_A)).thenReturn(rcA);

    IdealState isB = createIdealState(RESOURCE_B);
    isB.getRecord().setSimpleField(PRIORITY_FIELD, "10");
    when(_cache.getIdealState(RESOURCE_B)).thenReturn(isB);

    Message msgA = createMessage(RESOURCE_A, PARTITION_A0, "OFFLINE", "SLAVE", INSTANCE_0);
    Message msgB = createMessage(RESOURCE_B, PARTITION_B0, "OFFLINE", "SLAVE", INSTANCE_1);

    // RESOURCE_A has configured priority 20 > RESOURCE_B's 10
    assertComesBefore(msgA, msgB);
  }

  @Test
  public void testResourcePriority_NoPriorityField_AlphabeticalTiebreak() {
    // No priority field configured -> all resources get Integer.MIN_VALUE, tiebreak by
    // alphabetical resource name. "ResourceA" < "ResourceB" so ResourceA always comes first,
    // regardless of the order messages are presented to the sort.
    Message msgA = createMessage(RESOURCE_A, PARTITION_A0, "OFFLINE", "SLAVE", INSTANCE_0);
    Message msgB = createMessage(RESOURCE_B, PARTITION_B0, "OFFLINE", "SLAVE", INSTANCE_1);

    // ResourceA first in input -> ResourceA first in output
    assertComesBefore(msgA, msgB);

    // ResourceB first in input -> ResourceA still first (alphabetical, not insertion order)
    List<MessageOrderingStrategy.MessageContext> msgs = new ArrayList<>(
        Arrays.asList(toContext(msgB), toContext(msgA)));
    newStrategy().sortMessages(msgs);
    Assert.assertEquals(msgs.get(0).message, msgA,
        "ResourceA should sort before ResourceB alphabetically, regardless of input order");
  }

  @Test
  public void testResourcePriority_InvalidPriorityString_TreatedAsMinValue() {
    _clusterConfig.setResourcePriorityField(PRIORITY_FIELD);

    IdealState isA = createIdealState(RESOURCE_A);
    isA.getRecord().setSimpleField(PRIORITY_FIELD, "not-a-number");
    when(_cache.getIdealState(RESOURCE_A)).thenReturn(isA);

    IdealState isB = createIdealState(RESOURCE_B);
    isB.getRecord().setSimpleField(PRIORITY_FIELD, "5");
    when(_cache.getIdealState(RESOURCE_B)).thenReturn(isB);

    Message msgA = createMessage(RESOURCE_A, PARTITION_A0, "OFFLINE", "SLAVE", INSTANCE_0);
    Message msgB = createMessage(RESOURCE_B, PARTITION_B0, "OFFLINE", "SLAVE", INSTANCE_1);

    // RESOURCE_A has invalid priority -> falls back to Integer.MIN_VALUE < 5 -> RESOURCE_B first
    assertComesBefore(msgB, msgA);
  }

  // ========================================
  // Partition priority ordering
  // ========================================

  @Test
  public void testPartitionPriority_MissingTopState_HighestWithinResource() {
    // PARTITION_A0: no MASTER (missing top state), PARTITION_A1: has MASTER
    setBestPossibleState(RESOURCE_A, PARTITION_A0, INSTANCE_0, "MASTER");
    setBestPossibleState(RESOURCE_A, PARTITION_A1, INSTANCE_1, "MASTER");
    setCurrentState(RESOURCE_A, PARTITION_A1, INSTANCE_1, "MASTER");
    // PARTITION_A0 has no current MASTER

    Message msgA0 = createMessage(RESOURCE_A, PARTITION_A0, "OFFLINE", "SLAVE", INSTANCE_2);
    Message msgA1 = createMessage(RESOURCE_A, PARTITION_A1, "OFFLINE", "SLAVE", INSTANCE_2);

    // PARTITION_A0 is missing top state -> higher priority
    assertComesBefore(msgA0, msgA1);
  }

  @Test
  public void testPartitionPriority_FewerActiveReplicas_HigherPriority() {
    // PARTITION_A0: 0 active SLAVE replicas, PARTITION_A1: 1 active SLAVE replica.
    // Both have MASTER on INSTANCE_2 (a separate instance not in best-possible) so neither
    // partition triggers the missing-top-state priority. Using INSTANCE_2 for MASTER avoids
    // overwriting the SLAVE assignment on INSTANCE_0/INSTANCE_1.
    setBestPossibleState(RESOURCE_A, PARTITION_A0, INSTANCE_0, "SLAVE");
    setBestPossibleState(RESOURCE_A, PARTITION_A1, INSTANCE_1, "SLAVE");
    // MASTER on INSTANCE_2 for both — prevents missing-top-state scoring
    setCurrentState(RESOURCE_A, PARTITION_A0, INSTANCE_2, "MASTER");
    setCurrentState(RESOURCE_A, PARTITION_A1, INSTANCE_2, "MASTER");
    // PARTITION_A1 also has its SLAVE filled — active=1
    setCurrentState(RESOURCE_A, PARTITION_A1, INSTANCE_1, "SLAVE");

    Message msgA0 = createMessage(RESOURCE_A, PARTITION_A0, "OFFLINE", "SLAVE", INSTANCE_2);
    Message msgA1 = createMessage(RESOURCE_A, PARTITION_A1, "OFFLINE", "SLAVE", INSTANCE_2);

    assertComesBefore(msgA0, msgA1);
  }

  @Test
  public void testPartitionPriority_FewerIdealMatches_HigherPriority() {
    // Both partitions have MASTER so neither is missing top state.
    // PARTITION_A0: 0 ideal matches (MASTER on wrong instance), PARTITION_A1: 1 ideal match.
    setBestPossibleState(RESOURCE_A, PARTITION_A0, INSTANCE_0, "MASTER");
    setBestPossibleState(RESOURCE_A, PARTITION_A1, INSTANCE_1, "MASTER");
    // Both have same current MASTER count (1), but A0's MASTER is on wrong instance.
    setCurrentState(RESOURCE_A, PARTITION_A0, INSTANCE_1, "MASTER"); // wrong instance
    setCurrentState(RESOURCE_A, PARTITION_A1, INSTANCE_1, "MASTER"); // correct instance

    Message msgA0 = createMessage(RESOURCE_A, PARTITION_A0, "OFFLINE", "SLAVE", INSTANCE_2);
    Message msgA1 = createMessage(RESOURCE_A, PARTITION_A1, "OFFLINE", "SLAVE", INSTANCE_2);

    // PARTITION_A0 has fewer ideal matches -> higher priority
    assertComesBefore(msgA0, msgA1);
  }

  // ========================================
  // Message priority ordering (within same partition)
  // ========================================

  @Test
  public void testMessagePriority_HigherTargetState_First() {
    // MASTER has higher state priority than SLAVE in MasterSlave model
    Message toMaster = createMessage(RESOURCE_A, PARTITION_A0, "OFFLINE", "MASTER", INSTANCE_0);
    Message toSlave = createMessage(RESOURCE_A, PARTITION_A0, "OFFLINE", "SLAVE", INSTANCE_1);

    assertComesBefore(
        toContext(toMaster),
        toContext(toSlave));
  }

  @Test
  public void testMessagePriority_SamePartition_PreferenceListOrder() {
    // INSTANCE_0 appears before INSTANCE_1 in preference list -> INSTANCE_0 msg comes first
    List<String> prefList = Arrays.asList(INSTANCE_0, INSTANCE_1, INSTANCE_2);
    Message toInst0 = createMessage(RESOURCE_A, PARTITION_A0, "OFFLINE", "SLAVE", INSTANCE_0);
    Message toInst1 = createMessage(RESOURCE_A, PARTITION_A0, "OFFLINE", "SLAVE", INSTANCE_1);

    assertComesBefore(
        toContext(toInst0, prefList),
        toContext(toInst1, prefList));
  }

  @Test
  public void testMessagePriority_CrossPartition_SameToState_PartitionNameTiebreak() {
    // Messages from DIFFERENT partitions with the same toState and equal partition scores.
    // Preference-list comparison must NOT apply across partitions.
    // sortMessages applies the partition-name tiebreak before reaching compareMessagePriority:
    // PARTITION_A0 < PARTITION_A1 alphabetically → A0 first.
    List<String> prefListA0 = Arrays.asList(INSTANCE_0, INSTANCE_1);
    List<String> prefListA1 = Arrays.asList(INSTANCE_2, INSTANCE_0); // different order

    Message msgA0 = createMessage(RESOURCE_A, PARTITION_A0, "OFFLINE", "SLAVE", INSTANCE_0);
    Message msgA1 = createMessage(RESOURCE_A, PARTITION_A1, "OFFLINE", "SLAVE", INSTANCE_2);

    assertComesBefore(
        toContext(msgA0, prefListA0),
        toContext(msgA1, prefListA1));
  }

  @Test
  public void testMessagePriority_CrossPartition_DifferentToState_PartitionNameWinsOverStatePriority() {
    // Key behavioral alignment with IntermediateStateCalcStage:
    // When two messages are from different partitions with equal partition scores,
    // partition name (alphabetical) is the tiebreak — NOT message state priority.
    //
    // Without the partition-name tiebreak in sortMessages, compareMessagePriority would
    // apply state-priority first, causing Z_99/MASTER to beat A_0/SLAVE. The existing
    // PartitionPriorityComparator always resolves cross-partition ties by partition name,
    // so A_0 must come before Z_99 regardless of toState.
    setBestPossibleState(RESOURCE_A, PARTITION_A0, INSTANCE_0, "SLAVE");
    setBestPossibleState(RESOURCE_A, PARTITION_A1, INSTANCE_1, "MASTER");
    // Give both partitions equal partition scores: both have a MASTER present, same active count.
    setCurrentState(RESOURCE_A, PARTITION_A0, INSTANCE_2, "MASTER"); // MASTER present, 0 SLAVE active
    setCurrentState(RESOURCE_A, PARTITION_A1, INSTANCE_2, "MASTER"); // MASTER present, 0 SLAVE active

    // PARTITION_A0 toState=SLAVE, PARTITION_A1 toState=MASTER.
    // State priority alone would put MASTER (A1) first, but partition name puts A0 first.
    Message msgA0 = createMessage(RESOURCE_A, PARTITION_A0, "OFFLINE", "SLAVE", INSTANCE_0);
    Message msgA1 = createMessage(RESOURCE_A, PARTITION_A1, "OFFLINE", "MASTER", INSTANCE_1);

    // PARTITION_A0 ("ResourceA_0") < PARTITION_A1 ("ResourceA_1") → A0/SLAVE first.
    assertComesBefore(msgA0, msgA1);
  }

  // ========================================
  // Edge cases
  // ========================================

  @Test
  public void testEdgeCase_NullIdealState_NoNPE() {
    // IdealState absent from cache for this resource — strategy must handle gracefully.
    when(_cache.getIdealState(RESOURCE_A)).thenReturn(null);
    Message msg = createMessage(RESOURCE_A, PARTITION_A0, "OFFLINE", "SLAVE", INSTANCE_0);
    List<MessageOrderingStrategy.MessageContext> msgs = new ArrayList<>(
        Collections.singletonList(toContext(msg)));
    // Must not throw
    newStrategy().sortMessages(msgs);
    Assert.assertEquals(msgs.size(), 1);
  }

  @Test
  public void testEdgeCase_NullPreferenceList_NoNPE() {
    Message m1 = createMessage(RESOURCE_A, PARTITION_A0, "OFFLINE", "SLAVE", INSTANCE_0);
    Message m2 = createMessage(RESOURCE_A, PARTITION_A0, "OFFLINE", "SLAVE", INSTANCE_1);
    List<MessageOrderingStrategy.MessageContext> msgs = new ArrayList<>(
        Arrays.asList(
            toContext(m1),
            toContext(m2)));
    // Must not throw
    newStrategy().sortMessages(msgs);
    Assert.assertEquals(msgs.size(), 2);
  }

  @Test
  public void testEdgeCase_EmptyMessages_NoException() {
    List<MessageOrderingStrategy.MessageContext> msgs = new ArrayList<>();
    newStrategy().sortMessages(msgs);
    Assert.assertTrue(msgs.isEmpty());
  }

  @Test
  public void testEdgeCase_NullBestPossibleState_NoNPE() {
    // Resource with no best-possible state map -> partition scores default to {0,0,0}
    Message msg = createMessage(RESOURCE_A, PARTITION_A0, "OFFLINE", "SLAVE", INSTANCE_0);
    List<MessageOrderingStrategy.MessageContext> msgs = new ArrayList<>(
        Collections.singletonList(toContext(msg)));
    newStrategy().sortMessages(msgs);
    Assert.assertEquals(msgs.size(), 1);
  }

  // ========================================
  // Helper methods
  // ========================================

  private ResourcePriorityOrderingStrategy newStrategy() {
    return new ResourcePriorityOrderingStrategy(_cache, _bestPossibleStateOutput,
        _currentStateOutput);
  }

  private MessageOrderingStrategy.MessageContext toContext(Message msg) {
    return toContext(msg, null);
  }

  private MessageOrderingStrategy.MessageContext toContext(Message msg,
      List<String> preferenceList) {
    return new MessageOrderingStrategy.MessageContext(
        msg,
        new Partition(msg.getPartitionName()),
        msg.getResourceName(),
        preferenceList);
  }

  /**
   * Asserts that {@code higher} sorts before {@code lower}, testing both input orderings.
   */
  private void assertComesBefore(Message higher, Message lower) {
    assertComesBefore(toContext(higher), toContext(lower));
  }

  private void assertComesBefore(MessageOrderingStrategy.MessageContext higher,
      MessageOrderingStrategy.MessageContext lower) {
    List<MessageOrderingStrategy.MessageContext> msgs = new ArrayList<>(
        Arrays.asList(higher, lower));
    newStrategy().sortMessages(msgs);
    Assert.assertEquals(msgs.get(0), higher,
        higher.message.getPartitionName() + " should sort before "
            + lower.message.getPartitionName());

    List<MessageOrderingStrategy.MessageContext> msgsReversed = new ArrayList<>(
        Arrays.asList(lower, higher));
    newStrategy().sortMessages(msgsReversed);
    Assert.assertEquals(msgsReversed.get(0), higher,
        higher.message.getPartitionName() + " should sort before "
            + lower.message.getPartitionName() + " (reversed input)");
  }

  private void setBestPossibleState(String resource, String partition, String instance,
      String state) {
    _bestPossibleStateOutput.setState(resource, new Partition(partition), instance, state);
  }

  private void setCurrentState(String resource, String partition, String instance, String state) {
    _currentStateOutput.setCurrentState(resource, new Partition(partition), instance, state);
  }

  private IdealState createIdealState(String resource) {
    IdealState idealState = new IdealState(resource);
    idealState.setStateModelDefRef("MasterSlave");
    idealState.setRebalanceMode(IdealState.RebalanceMode.FULL_AUTO);
    idealState.setReplicas("3");
    idealState.setNumPartitions(2);
    return idealState;
  }

  private Message createMessage(String resource, String partition, String fromState,
      String toState, String tgtName) {
    Message message =
        new Message(Message.MessageType.STATE_TRANSITION, UUID.randomUUID().toString());
    message.setResourceName(resource);
    message.setPartitionName(partition);
    message.setFromState(fromState);
    message.setToState(toState);
    message.setTgtName(tgtName);
    message.setSrcName("Controller");
    message.setTgtSessionId("session_0");
    message.setSrcSessionId("session_controller");
    message.setStateModelDef("MasterSlave");
    return message;
  }
}
