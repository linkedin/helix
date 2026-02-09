package org.apache.helix.controller.stages;

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
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

import org.apache.helix.controller.dataproviders.ResourceControllerDataProvider;
import org.apache.helix.model.BuiltInStateModelDefinitions;
import org.apache.helix.model.IdealState;
import org.apache.helix.model.Message;
import org.apache.helix.model.Partition;
import org.apache.helix.model.StateModelDefinition;
import org.testng.Assert;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;


public class TestAvailabilityAwareMessageComparator {

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

    IdealState idealStateA = createIdealState(RESOURCE_A, 2);
    IdealState idealStateB = createIdealState(RESOURCE_B, 2);

    when(_cache.getIdealState(RESOURCE_A)).thenReturn(idealStateA);
    when(_cache.getIdealState(RESOURCE_B)).thenReturn(idealStateB);
    when(_cache.getStateModelDef("MasterSlave")).thenReturn(_masterSlaveSMD);

    Set<String> enabledLiveInstances =
        new HashSet<>(Arrays.asList(INSTANCE_0, INSTANCE_1, INSTANCE_2));
    when(_cache.getEnabledLiveInstances()).thenReturn(enabledLiveInstances);
  }

  // ========================================
  // Null / missing metadata
  // ========================================

  @Test
  public void testCompare_NullIdealState_FallsBackToTiebreaker() {
    when(_cache.getIdealState(RESOURCE_A)).thenReturn(null);
    AvailabilityAwareMessageComparator comparator = newComparator();

    Message m1 = createMessage(RESOURCE_A, PARTITION_0, "OFFLINE", "SLAVE", INSTANCE_0);
    Message m2 = createMessage(RESOURCE_A, PARTITION_1, "OFFLINE", "SLAVE", INSTANCE_1);

    // Both 0.0 impact -> tiebreak by partition name
    Assert.assertTrue(comparator.compare(m1, m2) < 0,
        "Should sort by partition name when impact is equal");
  }

  @Test
  public void testCompare_NullStateModelDef_FallsBackToTiebreaker() {
    when(_cache.getStateModelDef("MasterSlave")).thenReturn(null);
    AvailabilityAwareMessageComparator comparator = newComparator();

    Message m1 = createMessage(RESOURCE_A, PARTITION_0, "OFFLINE", "SLAVE", INSTANCE_0);
    Message m2 = createMessage(RESOURCE_A, PARTITION_1, "OFFLINE", "SLAVE", INSTANCE_1);

    Assert.assertTrue(comparator.compare(m1, m2) < 0,
        "Should sort by partition name when impact is equal");
  }

  // ========================================
  // Top state missing prioritization
  // ========================================

  @Test
  public void testCompare_TopStateMissing_HighestPriority() {
    // Partition_0: missing MASTER, message transitions to MASTER
    // Partition_1: has MASTER, message is a normal upward transition
    setCurrentState(RESOURCE_A, PARTITION_0, INSTANCE_0, "SLAVE");
    setCurrentState(RESOURCE_A, PARTITION_1, INSTANCE_1, "MASTER");

    AvailabilityAwareMessageComparator comparator = newComparator();

    Message topStateMissing = createMessage(RESOURCE_A, PARTITION_0, "SLAVE", "MASTER", INSTANCE_0);
    Message normalUpward = createMessage(RESOURCE_A, PARTITION_1, "OFFLINE", "SLAVE", INSTANCE_2);

    Assert.assertTrue(comparator.compare(topStateMissing, normalUpward) < 0,
        "Top state missing message should have higher priority");
  }

  @Test
  public void testCompare_TopStateMissing_ButNotTransitioningToTopState() {
    // Partition_0: missing MASTER but message is OFFLINE->SLAVE (not to MASTER)
    setCurrentState(RESOURCE_A, PARTITION_0, INSTANCE_0, "OFFLINE");

    AvailabilityAwareMessageComparator comparator = newComparator();

    Message offlineToSlave = createMessage(RESOURCE_A, PARTITION_0, "OFFLINE", "SLAVE", INSTANCE_0);
    Message anotherUpward = createMessage(RESOURCE_A, PARTITION_1, "OFFLINE", "SLAVE", INSTANCE_1);

    // Should NOT get MAX_VALUE impact — treated as normal upward
    Assert.assertTrue(comparator.compare(offlineToSlave, anotherUpward) != 0,
        "Different partitions should have different impact or tiebreak");
  }

  @Test
  public void testCompare_TopStateMissing_HigherThanHandoff() {
    setCurrentState(RESOURCE_A, PARTITION_0, INSTANCE_0, "SLAVE"); // no MASTER
    setCurrentState(RESOURCE_A, PARTITION_1, INSTANCE_1, "MASTER");

    AvailabilityAwareMessageComparator comparator = newComparator();

    Message missingTopState = createMessage(RESOURCE_A, PARTITION_0, "SLAVE", "MASTER", INSTANCE_0);
    Message handoff = createMessage(RESOURCE_A, PARTITION_1, "MASTER", "SLAVE", INSTANCE_1);

    Assert.assertTrue(comparator.compare(missingTopState, handoff) < 0,
        "Missing top state should rank higher than handoff");
  }

  // ========================================
  // Top state handoff prioritization
  // ========================================

  @Test
  public void testCompare_TopStateHandoff_HigherThanNormalUpward() {
    setCurrentState(RESOURCE_A, PARTITION_0, INSTANCE_0, "MASTER");
    setCurrentState(RESOURCE_A, PARTITION_1, INSTANCE_1, "SLAVE");

    AvailabilityAwareMessageComparator comparator = newComparator();

    Message handoff = createMessage(RESOURCE_A, PARTITION_0, "MASTER", "SLAVE", INSTANCE_0);
    Message normalUpward = createMessage(RESOURCE_A, PARTITION_1, "OFFLINE", "SLAVE", INSTANCE_2);

    Assert.assertTrue(comparator.compare(handoff, normalUpward) < 0,
        "Top state handoff should have higher priority than normal upward");
  }

  // ========================================
  // Downward transitions
  // ========================================

  @Test
  public void testCompare_DownwardTransition_LowerPriorityThanUpward() {
    setCurrentState(RESOURCE_A, PARTITION_0, INSTANCE_0, "MASTER");
    setCurrentState(RESOURCE_A, PARTITION_0, INSTANCE_1, "SLAVE");

    AvailabilityAwareMessageComparator comparator = newComparator();

    Message downward = createMessage(RESOURCE_A, PARTITION_0, "SLAVE", "OFFLINE", INSTANCE_1);
    Message upward = createMessage(RESOURCE_A, PARTITION_1, "OFFLINE", "SLAVE", INSTANCE_2);

    Assert.assertTrue(comparator.compare(downward, upward) > 0,
        "Downward transition should have lower priority than upward");
  }

  // ========================================
  // Upward transition impact scoring
  // ========================================

  @Test
  public void testCompare_FewerActiveReplicas_HigherPriority() {
    // Partition_0: 0 active replicas
    // Partition_1: 1 active replica
    setCurrentState(RESOURCE_A, PARTITION_1, INSTANCE_1, "SLAVE");

    AvailabilityAwareMessageComparator comparator = newComparator();

    Message toNoActive = createMessage(RESOURCE_A, PARTITION_0, "OFFLINE", "SLAVE", INSTANCE_0);
    Message toOneActive = createMessage(RESOURCE_A, PARTITION_1, "OFFLINE", "SLAVE", INSTANCE_2);

    Assert.assertTrue(comparator.compare(toNoActive, toOneActive) < 0,
        "Partition with fewer active replicas should have higher priority");
  }

  @Test
  public void testCompare_MinActiveZero_UsesBaseImpact() {
    when(_cache.getIdealState(RESOURCE_A)).thenReturn(createIdealState(RESOURCE_A, 0));

    AvailabilityAwareMessageComparator comparator = newComparator();

    Message m1 = createMessage(RESOURCE_A, PARTITION_0, "OFFLINE", "SLAVE", INSTANCE_0);
    Message m2 = createMessage(RESOURCE_A, PARTITION_1, "OFFLINE", "SLAVE", INSTANCE_1);

    // Both use base impact 1.0/(effectiveCount+1), same conditions -> tiebreak by partition
    Assert.assertTrue(comparator.compare(m1, m2) < 0, "Should tiebreak by partition name");
  }

  @Test
  public void testCompare_HigherMinActive_HigherImpact() {
    // ResourceA: minActive=2, ResourceB: minActive=1  — both have 0 active replicas
    when(_cache.getIdealState(RESOURCE_A)).thenReturn(createIdealState(RESOURCE_A, 2));
    when(_cache.getIdealState(RESOURCE_B)).thenReturn(createIdealState(RESOURCE_B, 1));

    AvailabilityAwareMessageComparator comparator = newComparator();

    Message msgA = createMessage(RESOURCE_A, PARTITION_0, "OFFLINE", "SLAVE", INSTANCE_0);
    Message msgB = createMessage(RESOURCE_B, "ResourceB_0", "OFFLINE", "SLAVE", INSTANCE_1);

    // ResourceA impact = 2.0/1 = 2.0, ResourceB impact = 1.0/1 = 1.0
    Assert.assertTrue(comparator.compare(msgA, msgB) < 0,
        "Resource with higher minActive should have higher priority when both have 0 active");
  }

  // ========================================
  // Cross-resource prioritization
  // ========================================

  @Test
  public void testCompare_CrossResourcePrioritization() {
    // ResourceA: 0 active,  ResourceB: 2 active
    setCurrentState(RESOURCE_B, "ResourceB_0", INSTANCE_0, "MASTER");
    setCurrentState(RESOURCE_B, "ResourceB_0", INSTANCE_1, "SLAVE");

    AvailabilityAwareMessageComparator comparator = newComparator();

    Message msgA = createMessage(RESOURCE_A, PARTITION_0, "OFFLINE", "SLAVE", INSTANCE_2);
    Message msgB = createMessage(RESOURCE_B, "ResourceB_0", "OFFLINE", "SLAVE", INSTANCE_2);

    Assert.assertTrue(comparator.compare(msgA, msgB) < 0,
        "Resource with fewer active replicas should be prioritized across resources");
  }

  // ========================================
  // Tiebreaking
  // ========================================

  @Test
  public void testCompare_TiebreaksByResourceNameThenPartitionName() {
    AvailabilityAwareMessageComparator comparator = newComparator();

    // Different resources, same impact -> tiebreak by resource name
    Message msgA = createMessage(RESOURCE_A, PARTITION_0, "OFFLINE", "SLAVE", INSTANCE_0);
    Message msgB = createMessage(RESOURCE_B, "ResourceB_0", "OFFLINE", "SLAVE", INSTANCE_1);
    Assert.assertTrue(comparator.compare(msgA, msgB) < 0, "Should tiebreak by resource name");

    // Same resource, same impact -> tiebreak by partition name
    comparator = newComparator(); // fresh comparator to reset index tracker
    Message m1 = createMessage(RESOURCE_A, PARTITION_0, "OFFLINE", "SLAVE", INSTANCE_0);
    Message m2 = createMessage(RESOURCE_A, PARTITION_1, "OFFLINE", "SLAVE", INSTANCE_1);
    Assert.assertTrue(comparator.compare(m1, m2) < 0, "Should tiebreak by partition name");
  }

  // ========================================
  // Caching & pending messages
  // ========================================

  @Test
  public void testCompare_SameMessageComparesAsEqual() {
    setCurrentState(RESOURCE_A, PARTITION_0, INSTANCE_0, "SLAVE");
    AvailabilityAwareMessageComparator comparator = newComparator();

    Message m1 = createMessage(RESOURCE_A, PARTITION_0, "OFFLINE", "SLAVE", INSTANCE_1);

    Assert.assertEquals(comparator.compare(m1, m1), 0, "Same message should compare as equal");
  }

  @Test
  public void testCompare_PendingMessagesReduceImpact() {
    // Partition_0: 0 active, 1 pending upward
    // Partition_1: 0 active, no pending
    Message pendingMsg = createMessage(RESOURCE_A, PARTITION_0, "OFFLINE", "SLAVE", INSTANCE_0);
    _currentStateOutput.setPendingMessage(RESOURCE_A, new Partition(PARTITION_0),
        INSTANCE_0, pendingMsg);

    AvailabilityAwareMessageComparator comparator = newComparator();

    Message toPartitionWithPending =
        createMessage(RESOURCE_A, PARTITION_0, "OFFLINE", "SLAVE", INSTANCE_1);
    Message toPartitionWithNoPending =
        createMessage(RESOURCE_A, PARTITION_1, "OFFLINE", "SLAVE", INSTANCE_2);

    Assert.assertTrue(comparator.compare(toPartitionWithPending, toPartitionWithNoPending) > 0,
        "Partition with pending upward messages should have lower impact (higher effective count)");
  }

  // ========================================
  // End-to-end sorting
  // ========================================

  @Test
  public void testSort_MultipleMessages_CorrectOrder() {
    String resourceBP0 = "ResourceB_0";
    String resourceBP1 = "ResourceB_1";

    setCurrentState(RESOURCE_A, PARTITION_0, INSTANCE_0, "SLAVE"); // missing MASTER
    setCurrentState(RESOURCE_A, PARTITION_1, INSTANCE_0, "MASTER");
    setCurrentState(RESOURCE_A, PARTITION_1, INSTANCE_1, "SLAVE");
    setCurrentState(RESOURCE_B, resourceBP1, INSTANCE_1, "MASTER");
    setCurrentState(RESOURCE_B, resourceBP1, INSTANCE_2, "SLAVE");

    AvailabilityAwareMessageComparator comparator = newComparator();

    Message topStateMissingMsg =
        createMessage(RESOURCE_A, PARTITION_0, "SLAVE", "MASTER", INSTANCE_0);
    Message highImpactUpward =
        createMessage(RESOURCE_B, resourceBP0, "OFFLINE", "SLAVE", INSTANCE_0);
    Message lowImpactUpward =
        createMessage(RESOURCE_A, PARTITION_1, "OFFLINE", "SLAVE", INSTANCE_2);
    Message downwardMsg =
        createMessage(RESOURCE_B, resourceBP1, "SLAVE", "OFFLINE", INSTANCE_2);

    List<Message> messages = new ArrayList<>(
        Arrays.asList(downwardMsg, lowImpactUpward, topStateMissingMsg, highImpactUpward));
    messages.sort(comparator);

    Assert.assertEquals(messages.get(0), topStateMissingMsg,
        "Top state missing should be first");
    Assert.assertEquals(messages.get(1), highImpactUpward,
        "High impact upward (0 active) should be second");
    Assert.assertEquals(messages.get(2), lowImpactUpward,
        "Low impact upward (2 active) should be third");
    Assert.assertEquals(messages.get(3), downwardMsg,
        "Downward transition should be last");
  }

  // ========================================
  // Helper methods
  // ========================================

  private AvailabilityAwareMessageComparator newComparator() {
    return new AvailabilityAwareMessageComparator(_cache, _currentStateOutput);
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
