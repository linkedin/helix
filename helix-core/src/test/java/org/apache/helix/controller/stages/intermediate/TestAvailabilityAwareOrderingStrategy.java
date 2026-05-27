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
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.UUID;

import org.apache.helix.controller.dataproviders.ResourceControllerDataProvider;
import org.apache.helix.controller.stages.CurrentStateOutput;
import org.apache.helix.model.BuiltInStateModelDefinitions;
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


public class TestAvailabilityAwareOrderingStrategy {

  private static final String RESOURCE_A = "ResourceA";
  private static final String RESOURCE_B = "ResourceB";
  private static final String PARTITION_0 = "ResourceA_0";
  private static final String PARTITION_1 = "ResourceA_1";
  private static final String INSTANCE_0 = "localhost_0";
  private static final String INSTANCE_1 = "localhost_1";
  private static final String INSTANCE_2 = "localhost_2";

  private StateModelDefinition _masterSlaveSMD;
  private ResourceControllerDataProvider _cache;
  private CurrentStateOutput _currentStateOutput;

  @BeforeMethod
  public void setup() {
    _masterSlaveSMD = BuiltInStateModelDefinitions.MasterSlave.getStateModelDefinition();
    _cache = mock(ResourceControllerDataProvider.class);
    _currentStateOutput = new CurrentStateOutput();

    when(_cache.getIdealState(RESOURCE_A)).thenReturn(createIdealState(RESOURCE_A, 2));
    when(_cache.getIdealState(RESOURCE_B)).thenReturn(createIdealState(RESOURCE_B, 2));
    when(_cache.getStateModelDef("MasterSlave")).thenReturn(_masterSlaveSMD);
  }

  // ========================================
  // Null / missing metadata
  // ========================================

  @Test
  public void testNullIdealState_ScoresNegativeOneAndTiebreaksByPartition() {
    // Null idealState (deleted or bad resource) now scores -1.0, below even downward transitions.
    when(_cache.getIdealState(RESOURCE_A)).thenReturn(null);

    Message m1 = createMessage(RESOURCE_A, PARTITION_0, "OFFLINE", "SLAVE", INSTANCE_0);
    Message m2 = createMessage(RESOURCE_A, PARTITION_1, "OFFLINE", "SLAVE", INSTANCE_1);

    // Both -1.0 -> tiebreak by partition name (PARTITION_0 < PARTITION_1)
    assertComesBefore(m1, m2);
  }

  @Test
  public void testNullStateModelDef_ScoresNegativeOneAndTiebreaksByPartition() {
    when(_cache.getStateModelDef("MasterSlave")).thenReturn(null);

    Message m1 = createMessage(RESOURCE_A, PARTITION_0, "OFFLINE", "SLAVE", INSTANCE_0);
    Message m2 = createMessage(RESOURCE_A, PARTITION_1, "OFFLINE", "SLAVE", INSTANCE_1);

    // Both -1.0 -> tiebreak by partition name
    assertComesBefore(m1, m2);
  }

  @Test
  public void testNullIdealState_LowerPriorityThanDownwardTransition() {
    // Messages for deleted resources (-1.0) should rank below downward transitions (0.0).
    when(_cache.getIdealState(RESOURCE_B)).thenReturn(null);
    setCurrentState(RESOURCE_A, PARTITION_0, INSTANCE_0, "MASTER");

    Message downward = createMessage(RESOURCE_A, PARTITION_0, "MASTER", "SLAVE", INSTANCE_0);
    Message nullIdealState = createMessage(RESOURCE_B, "ResourceB_0", "OFFLINE", "SLAVE", INSTANCE_1);

    assertComesBefore(downward, nullIdealState);
  }

  // ========================================
  // Top state missing prioritization
  // ========================================

  @Test
  public void testTopStateMissing_HighestPriority() {
    setCurrentState(RESOURCE_A, PARTITION_0, INSTANCE_0, "SLAVE"); // no MASTER
    setCurrentState(RESOURCE_A, PARTITION_1, INSTANCE_1, "MASTER");

    Message topStateMissing = createMessage(RESOURCE_A, PARTITION_0, "SLAVE", "MASTER", INSTANCE_0);
    Message normalUpward = createMessage(RESOURCE_A, PARTITION_1, "OFFLINE", "SLAVE", INSTANCE_2);

    assertComesBefore(topStateMissing, normalUpward);
  }

  @Test
  public void testTopStateMissing_NotTransitioningToTopState_TreatedAsNormalUpward() {
    // Partition has no MASTER but the message goes to SLAVE, not MASTER.
    // Should NOT receive the top-state-missing boost; treated as a regular upward transition.
    setCurrentState(RESOURCE_A, PARTITION_0, INSTANCE_0, "OFFLINE");

    Message offlineToSlave = createMessage(RESOURCE_A, PARTITION_0, "OFFLINE", "SLAVE", INSTANCE_0);
    Message anotherUpward = createMessage(RESOURCE_A, PARTITION_1, "OFFLINE", "SLAVE", INSTANCE_1);

    // Both equal upward scores -> tiebreak by partition name (PARTITION_0 before PARTITION_1)
    assertComesBefore(offlineToSlave, anotherUpward);
  }

  @Test
  public void testTopStateMissing_HigherThanHandoff() {
    setCurrentState(RESOURCE_A, PARTITION_0, INSTANCE_0, "SLAVE"); // no MASTER
    setCurrentState(RESOURCE_A, PARTITION_1, INSTANCE_1, "MASTER");

    Message missingTopState = createMessage(RESOURCE_A, PARTITION_0, "SLAVE", "MASTER", INSTANCE_0);
    Message handoff = createMessage(RESOURCE_A, PARTITION_1, "MASTER", "SLAVE", INSTANCE_1);

    assertComesBefore(missingTopState, handoff);
  }

  // ========================================
  // Top state handoff prioritization
  // ========================================

  @Test
  public void testTopStateHandoff_HigherThanNormalUpward() {
    setCurrentState(RESOURCE_A, PARTITION_0, INSTANCE_0, "MASTER");
    setCurrentState(RESOURCE_A, PARTITION_1, INSTANCE_1, "SLAVE");

    Message handoff = createMessage(RESOURCE_A, PARTITION_0, "MASTER", "SLAVE", INSTANCE_0);
    Message normalUpward = createMessage(RESOURCE_A, PARTITION_1, "OFFLINE", "SLAVE", INSTANCE_2);

    assertComesBefore(handoff, normalUpward);
  }

  // ========================================
  // Downward transitions
  // ========================================

  @Test
  public void testDownwardTransition_LowerPriorityThanUpward() {
    setCurrentState(RESOURCE_A, PARTITION_0, INSTANCE_0, "MASTER");
    setCurrentState(RESOURCE_A, PARTITION_0, INSTANCE_1, "SLAVE");

    Message downward = createMessage(RESOURCE_A, PARTITION_0, "SLAVE", "OFFLINE", INSTANCE_1);
    Message upward = createMessage(RESOURCE_A, PARTITION_1, "OFFLINE", "SLAVE", INSTANCE_2);

    assertComesBefore(upward, downward);
  }

  // ========================================
  // Upward transition score
  // ========================================

  @Test
  public void testFewerActiveReplicas_HigherScore() {
    // PARTITION_0: 0 active, PARTITION_1: 1 active
    setCurrentState(RESOURCE_A, PARTITION_1, INSTANCE_1, "SLAVE");

    Message toNoActive = createMessage(RESOURCE_A, PARTITION_0, "OFFLINE", "SLAVE", INSTANCE_0);
    Message toOneActive = createMessage(RESOURCE_A, PARTITION_1, "OFFLINE", "SLAVE", INSTANCE_2);

    assertComesBefore(toNoActive, toOneActive);
  }

  @Test
  public void testConfiguredActiveStates_OnlyConfiguredStatesCountAsActive() {
    // Resource is configured so that only MASTER counts as active (not SLAVE).
    ResourceConfig resourceConfig = new ResourceConfig(RESOURCE_A);
    resourceConfig.setActiveStatesForMinActiveReplicaCheck(Arrays.asList("MASTER"));
    when(_cache.getResourceConfig(RESOURCE_A)).thenReturn(resourceConfig);

    // PARTITION_0: 1 SLAVE + 1 MASTER — with default logic both are active (count=2)
    //                                  — with configured logic only MASTER counts (count=1)
    // PARTITION_1: 1 SLAVE only       — with default logic count=1, with configured count=0
    setCurrentState(RESOURCE_A, PARTITION_0, INSTANCE_0, "MASTER");
    setCurrentState(RESOURCE_A, PARTITION_0, INSTANCE_1, "SLAVE");
    setCurrentState(RESOURCE_A, PARTITION_1, INSTANCE_2, "SLAVE");

    Message toP0 = createMessage(RESOURCE_A, PARTITION_0, "OFFLINE", "MASTER", INSTANCE_2);
    Message toP1 = createMessage(RESOURCE_A, PARTITION_1, "OFFLINE", "MASTER", INSTANCE_0);

    // PARTITION_1 has 0 configured-active replicas vs PARTITION_0 has 1 -> PARTITION_1 scores higher
    assertComesBefore(toP1, toP0);
  }

  @Test
  public void testConfiguredActiveStates_FallsBackToDefaultWhenNotConfigured() {
    // No ResourceConfig set -> _cache.getResourceConfig returns null -> default logic applies.
    when(_cache.getResourceConfig(RESOURCE_A)).thenReturn(null);

    // Default: SLAVE is active, so PARTITION_1 (1 SLAVE) has more active than PARTITION_0 (none).
    setCurrentState(RESOURCE_A, PARTITION_1, INSTANCE_1, "SLAVE");

    Message toP0 = createMessage(RESOURCE_A, PARTITION_0, "OFFLINE", "SLAVE", INSTANCE_0);
    Message toP1 = createMessage(RESOURCE_A, PARTITION_1, "OFFLINE", "SLAVE", INSTANCE_2);

    assertComesBefore(toP0, toP1);
  }

  @Test
  public void testMinActiveNotConfigured_UsesBaseScore() {
    // minActiveReplicas = -1 (unconfigured) -> guard (minActive <= 0) -> score = 1.0/(count+1)
    when(_cache.getIdealState(RESOURCE_A)).thenReturn(createIdealState(RESOURCE_A, -1));

    Message m1 = createMessage(RESOURCE_A, PARTITION_0, "OFFLINE", "SLAVE", INSTANCE_0);
    Message m2 = createMessage(RESOURCE_A, PARTITION_1, "OFFLINE", "SLAVE", INSTANCE_1);

    // Both: 1.0/(0+1) = 1.0 -> tiebreak by partition name
    assertComesBefore(m1, m2);
  }

  @Test
  public void testMinActiveZero_UsesBaseScore() {
    when(_cache.getIdealState(RESOURCE_A)).thenReturn(createIdealState(RESOURCE_A, 0));

    Message m1 = createMessage(RESOURCE_A, PARTITION_0, "OFFLINE", "SLAVE", INSTANCE_0);
    Message m2 = createMessage(RESOURCE_A, PARTITION_1, "OFFLINE", "SLAVE", INSTANCE_1);

    // Both: 1.0/(0+1) = 1.0 -> tiebreak by partition name
    assertComesBefore(m1, m2);
  }

  @Test
  public void testHigherMinActive_HigherScore() {
    when(_cache.getIdealState(RESOURCE_A)).thenReturn(createIdealState(RESOURCE_A, 2));
    when(_cache.getIdealState(RESOURCE_B)).thenReturn(createIdealState(RESOURCE_B, 1));

    Message msgA = createMessage(RESOURCE_A, PARTITION_0, "OFFLINE", "SLAVE", INSTANCE_0);
    Message msgB = createMessage(RESOURCE_B, "ResourceB_0", "OFFLINE", "SLAVE", INSTANCE_1);

    // ResourceA: 2.0/(0+1) = 2.0, ResourceB: 1.0/(0+1) = 1.0
    assertComesBefore(msgA, msgB);
  }

  // ========================================
  // Cross-resource prioritization
  // ========================================

  @Test
  public void testCrossResourcePrioritization_FewerActiveFirst() {
    // ResourceA: 0 active, ResourceB: 2 active
    setCurrentState(RESOURCE_B, "ResourceB_0", INSTANCE_0, "MASTER");
    setCurrentState(RESOURCE_B, "ResourceB_0", INSTANCE_1, "SLAVE");

    Message msgA = createMessage(RESOURCE_A, PARTITION_0, "OFFLINE", "SLAVE", INSTANCE_2);
    Message msgB = createMessage(RESOURCE_B, "ResourceB_0", "OFFLINE", "SLAVE", INSTANCE_2);

    assertComesBefore(msgA, msgB);
  }

  // ========================================
  // Tiebreaking
  // ========================================

  @Test
  public void testTiebreaksByResourceNameThenPartitionName() {
    // Same score, different resources -> tiebreak by resource name ("ResourceA" < "ResourceB")
    Message msgA = createMessage(RESOURCE_A, PARTITION_0, "OFFLINE", "SLAVE", INSTANCE_0);
    Message msgB = createMessage(RESOURCE_B, "ResourceB_0", "OFFLINE", "SLAVE", INSTANCE_1);
    assertComesBefore(msgA, msgB);

    // Same score, same resource, different partitions -> tiebreak by partition name
    Message m1 = createMessage(RESOURCE_A, PARTITION_0, "OFFLINE", "SLAVE", INSTANCE_0);
    Message m2 = createMessage(RESOURCE_A, PARTITION_1, "OFFLINE", "SLAVE", INSTANCE_1);
    assertComesBefore(m1, m2);
  }

  // ========================================
  // Caching & pending messages
  // ========================================

  @Test
  public void testScoreIsCachedPerCacheKey_StableSortForEqualScores() {
    setCurrentState(RESOURCE_A, PARTITION_0, INSTANCE_0, "SLAVE");

    // Two message objects with identical cacheKey (resource:partition:from:to:tgt)
    Message m1 = createMessage(RESOURCE_A, PARTITION_0, "OFFLINE", "SLAVE", INSTANCE_1);
    Message m2 = createMessage(RESOURCE_A, PARTITION_0, "OFFLINE", "SLAVE", INSTANCE_1);

    // The second message hits the impact cache -> same score -> stable sort preserves original order
    List<MessageOrderingStrategy.MessageContext> msgs = new ArrayList<>(
        Arrays.asList(toContext(m1), toContext(m2)));
    newStrategy().sortMessages(msgs);
    Assert.assertEquals(msgs.get(0).message, m1,
        "Stable sort should preserve original order for equal scores");
    Assert.assertEquals(msgs.get(1).message, m2);
  }

  @Test
  public void testPendingUpwardMessages_ReduceScore() {
    // PARTITION_0: 0 active, 1 pending upward transition
    // PARTITION_1: 0 active, no pending
    Message pendingMsg = createMessage(RESOURCE_A, PARTITION_0, "OFFLINE", "SLAVE", INSTANCE_0);
    _currentStateOutput.setPendingMessage(
        RESOURCE_A, new Partition(PARTITION_0), INSTANCE_0, pendingMsg);

    Message toPartitionWithPending =
        createMessage(RESOURCE_A, PARTITION_0, "OFFLINE", "SLAVE", INSTANCE_1);
    Message toPartitionWithNoPending =
        createMessage(RESOURCE_A, PARTITION_1, "OFFLINE", "SLAVE", INSTANCE_2);

    // PARTITION_0: effectiveCount = 0 + 1(pending) + 0 = 1 -> score = 2.0/2 = 1.0
    // PARTITION_1: effectiveCount = 0 + 0       + 0 = 0 -> score = 2.0/1 = 2.0
    assertComesBefore(toPartitionWithNoPending, toPartitionWithPending);
  }

  // ========================================
  // messageIndexTracker diminishing scores
  // ========================================

  @Test
  public void testMessageIndexTracker_DiminishingScoresForSamePartition() {
    // Three upward messages all targeting PARTITION_0 plus one downward message on PARTITION_1.
    // messageIndexTracker increments per message encountered for a given partition key,
    // making each successive message's effective count higher and its score lower.
    // All three upward messages should still rank above the downward message (score 0.0).
    // The relative ordering among the three upward messages is non-deterministic because
    // the stateful messageIndexTracker assigns scores based on the sort algorithm's
    // internal comparison order.
    Message msg0 = createMessage(RESOURCE_A, PARTITION_0, "OFFLINE", "SLAVE", INSTANCE_0);
    Message msg1 = createMessage(RESOURCE_A, PARTITION_0, "OFFLINE", "SLAVE", INSTANCE_1);
    Message msg2 = createMessage(RESOURCE_A, PARTITION_0, "OFFLINE", "SLAVE", INSTANCE_2);

    setCurrentState(RESOURCE_A, PARTITION_1, INSTANCE_0, "MASTER");
    setCurrentState(RESOURCE_A, PARTITION_1, INSTANCE_1, "SLAVE");
    Message downward = createMessage(RESOURCE_A, PARTITION_1, "SLAVE", "OFFLINE", INSTANCE_1);

    List<MessageOrderingStrategy.MessageContext> msgs = new ArrayList<>(
        Arrays.asList(toContext(downward), toContext(msg0), toContext(msg1), toContext(msg2)));
    newStrategy().sortMessages(msgs);

    // All three upward messages should come before the downward message
    Set<Message> upwardMsgs = new HashSet<>(Arrays.asList(msg0, msg1, msg2));
    Assert.assertTrue(upwardMsgs.contains(msgs.get(0).message),
        "First position should be an upward message");
    Assert.assertTrue(upwardMsgs.contains(msgs.get(1).message),
        "Second position should be an upward message");
    Assert.assertTrue(upwardMsgs.contains(msgs.get(2).message),
        "Third position should be an upward message");
    Assert.assertEquals(msgs.get(3).message, downward,
        "Downward transition should be last");
  }

  // ========================================
  // Configured active states — absolute count verification
  // ========================================

  @Test
  public void testConfiguredActiveStates_AbsoluteReplicaCountDrivesScore() {
    // Verifies that getUnhealthyStates integration produces the correct *absolute* active
    // replica count, not just a relative ordering.
    //
    // Configuration: only MASTER counts as active (not SLAVE).
    // PARTITION_0: 1 MASTER + 1 SLAVE  -> configured-active count = 1
    //              score = 2.0 / (1 + 0 pending + 0 index + 1) = 2.0 / 2 = 1.0
    // PARTITION_1: 1 SLAVE only        -> configured-active count = 0
    //              score = 2.0 / (0 + 0 pending + 0 index + 1) = 2.0 / 1 = 2.0
    //
    // A ResourceB partition with minActive=2 and 0 active also scores 2.0/(0+1) = 2.0,
    // tying with PARTITION_1 and confirming PARTITION_1's absolute score is 2.0 (not lower).
    ResourceConfig resourceConfig = new ResourceConfig(RESOURCE_A);
    resourceConfig.setActiveStatesForMinActiveReplicaCheck(Arrays.asList("MASTER"));
    when(_cache.getResourceConfig(RESOURCE_A)).thenReturn(resourceConfig);

    setCurrentState(RESOURCE_A, PARTITION_0, INSTANCE_0, "MASTER");
    setCurrentState(RESOURCE_A, PARTITION_0, INSTANCE_1, "SLAVE");
    setCurrentState(RESOURCE_A, PARTITION_1, INSTANCE_2, "SLAVE");

    Message toP0 = createMessage(RESOURCE_A, PARTITION_0, "OFFLINE", "MASTER", INSTANCE_2);
    Message toP1 = createMessage(RESOURCE_A, PARTITION_1, "OFFLINE", "MASTER", INSTANCE_0);

    // Primary assertion: PARTITION_1 (0 configured-active) outranks PARTITION_0 (1 configured-active)
    assertComesBefore(toP1, toP0);

    // Absolute-score pin: a ResourceB partition with 0 active and minActive=2 also has
    // score 2.0/1 = 2.0, so it should tie with toP1 and tiebreak by resource name
    // ("ResourceA" < "ResourceB" means toP1 still comes first).
    when(_cache.getResourceConfig(RESOURCE_B)).thenReturn(null);
    Message toBp0 = createMessage(RESOURCE_B, "ResourceB_0", "OFFLINE", "SLAVE", INSTANCE_0);
    List<MessageOrderingStrategy.MessageContext> threeWay = new ArrayList<>(
        Arrays.asList(toContext(toP0), toContext(toBp0), toContext(toP1)));
    newStrategy().sortMessages(threeWay);

    // toP1 and toBp0 tie on score 2.0; tiebreak by resource name: "ResourceA" < "ResourceB"
    Assert.assertEquals(threeWay.get(0).message, toP1, "PARTITION_1 (ResourceA) ties on score but wins tiebreak");
    Assert.assertEquals(threeWay.get(1).message, toBp0, "ResourceB ties on score, loses tiebreak to ResourceA");
    Assert.assertEquals(threeWay.get(2).message, toP0, "PARTITION_0 has lower score (1 active)");
  }

  // ========================================
  // End-to-end sorting
  // ========================================

  @Test
  public void testSort_MultipleMessages_CorrectOrder() {
    String resourceBP0 = "ResourceB_0";
    String resourceBP1 = "ResourceB_1";

    setCurrentState(RESOURCE_A, PARTITION_0, INSTANCE_0, "SLAVE"); // no MASTER -> top state missing
    setCurrentState(RESOURCE_A, PARTITION_1, INSTANCE_0, "MASTER");
    setCurrentState(RESOURCE_A, PARTITION_1, INSTANCE_1, "SLAVE");  // 2 active replicas
    setCurrentState(RESOURCE_B, resourceBP1, INSTANCE_1, "MASTER");
    setCurrentState(RESOURCE_B, resourceBP1, INSTANCE_2, "SLAVE");

    Message topStateMissingMsg =
        createMessage(RESOURCE_A, PARTITION_0, "SLAVE", "MASTER", INSTANCE_0);
    Message highScoreUpward =    // ResourceB_0 has 0 active replicas -> score = 2.0/1 = 2.0
        createMessage(RESOURCE_B, resourceBP0, "OFFLINE", "SLAVE", INSTANCE_0);
    Message lowScoreUpward =     // ResourceA PARTITION_1 has 2 active -> score = 2.0/3 = 0.67
        createMessage(RESOURCE_A, PARTITION_1, "OFFLINE", "SLAVE", INSTANCE_2);
    Message downwardMsg =
        createMessage(RESOURCE_B, resourceBP1, "SLAVE", "OFFLINE", INSTANCE_2);

    List<MessageOrderingStrategy.MessageContext> messages = new ArrayList<>(Arrays.asList(
        toContext(downwardMsg), toContext(lowScoreUpward),
        toContext(topStateMissingMsg), toContext(highScoreUpward)));
    newStrategy().sortMessages(messages);

    Assert.assertEquals(messages.get(0).message, topStateMissingMsg,
        "Top state missing should be first");
    Assert.assertEquals(messages.get(1).message, highScoreUpward,
        "High score upward (0 active) should be second");
    Assert.assertEquals(messages.get(2).message, lowScoreUpward,
        "Low score upward (2 active) should be third");
    Assert.assertEquals(messages.get(3).message, downwardMsg,
        "Downward transition should be last");
  }

  // ========================================
  // Helper methods
  // ========================================

  private AvailabilityAwareOrderingStrategy newStrategy() {
    return new AvailabilityAwareOrderingStrategy(_cache, _currentStateOutput);
  }

  /**
   * Wraps a message in a MessageContext for use with sortMessages().
   * AvailabilityAwareOrderingStrategy resolves stateModelDef from the cache internally.
   */
  private MessageOrderingStrategy.MessageContext toContext(Message msg) {
    return new MessageOrderingStrategy.MessageContext(
        msg,
        new Partition(msg.getPartitionName()),
        msg.getResourceName(),
        null);
  }

  /**
   * Asserts that {@code higher} sorts before {@code lower}, testing both input orderings
   * to confirm the result is independent of initial list order.
   */
  private void assertComesBefore(Message higher, Message lower) {
    List<MessageOrderingStrategy.MessageContext> msgs = new ArrayList<>(
        Arrays.asList(toContext(higher), toContext(lower)));
    newStrategy().sortMessages(msgs);
    Assert.assertEquals(msgs.get(0).message, higher,
        higher.getPartitionName() + " should sort before " + lower.getPartitionName());

    // Also verify with reversed initial order
    List<MessageOrderingStrategy.MessageContext> msgsReversed = new ArrayList<>(
        Arrays.asList(toContext(lower), toContext(higher)));
    newStrategy().sortMessages(msgsReversed);
    Assert.assertEquals(msgsReversed.get(0).message, higher,
        higher.getPartitionName() + " should sort before " + lower.getPartitionName()
            + " (reversed input)");
  }

  private IdealState createIdealState(String resource, int minActiveReplicas) {
    IdealState idealState = new IdealState(resource);
    idealState.setStateModelDefRef("MasterSlave");
    idealState.setRebalanceMode(IdealState.RebalanceMode.FULL_AUTO);
    idealState.setReplicas("3");
    idealState.setNumPartitions(2);
    idealState.setMinActiveReplicas(minActiveReplicas);
    return idealState;
  }

  private void setCurrentState(String resource, String partition, String instance, String state) {
    _currentStateOutput.setCurrentState(resource, new Partition(partition), instance, state);
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
