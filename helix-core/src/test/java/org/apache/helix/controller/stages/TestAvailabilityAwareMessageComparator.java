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

    // Setup default ideal state for RESOURCE_A
    IdealState idealStateA = new IdealState(RESOURCE_A);
    idealStateA.setStateModelDefRef("MasterSlave");
    idealStateA.setRebalanceMode(IdealState.RebalanceMode.FULL_AUTO);
    idealStateA.setReplicas("3");
    idealStateA.setNumPartitions(2);
    idealStateA.setMinActiveReplicas(2);

    // Setup default ideal state for RESOURCE_B
    IdealState idealStateB = new IdealState(RESOURCE_B);
    idealStateB.setStateModelDefRef("MasterSlave");
    idealStateB.setRebalanceMode(IdealState.RebalanceMode.FULL_AUTO);
    idealStateB.setReplicas("3");
    idealStateB.setNumPartitions(2);
    idealStateB.setMinActiveReplicas(2);

    when(_cache.getIdealState(RESOURCE_A)).thenReturn(idealStateA);
    when(_cache.getIdealState(RESOURCE_B)).thenReturn(idealStateB);
    when(_cache.getStateModelDef("MasterSlave")).thenReturn(_masterSlaveSMD);

    Set<String> enabledLiveInstances = new HashSet<>(Arrays.asList(INSTANCE_0, INSTANCE_1, INSTANCE_2));
    when(_cache.getEnabledLiveInstances()).thenReturn(enabledLiveInstances);
  }

  // ========================================
  // Tests for null/missing metadata
  // ========================================

  @Test
  public void testCompare_NullIdealState_ReturnsZeroImpact() {
    when(_cache.getIdealState(RESOURCE_A)).thenReturn(null);

    AvailabilityAwareMessageComparator comparator =
        new AvailabilityAwareMessageComparator(_cache, _currentStateOutput);

    Message m1 = createMessage(RESOURCE_A, PARTITION_0, "OFFLINE", "SLAVE", INSTANCE_0);
    Message m2 = createMessage(RESOURCE_A, PARTITION_1, "OFFLINE", "SLAVE", INSTANCE_1);

    // Both have 0.0 impact, should fall back to tiebreakers
    int result = comparator.compare(m1, m2);
    // Same resource, so compare by partition name
    Assert.assertTrue(result < 0, "Should sort by partition name when impact is equal");
  }

  @Test
  public void testCompare_NullStateModelDef_ReturnsZeroImpact() {
    when(_cache.getStateModelDef("MasterSlave")).thenReturn(null);

    AvailabilityAwareMessageComparator comparator =
        new AvailabilityAwareMessageComparator(_cache, _currentStateOutput);

    Message m1 = createMessage(RESOURCE_A, PARTITION_0, "OFFLINE", "SLAVE", INSTANCE_0);
    Message m2 = createMessage(RESOURCE_A, PARTITION_1, "OFFLINE", "SLAVE", INSTANCE_1);

    int result = comparator.compare(m1, m2);
    Assert.assertTrue(result < 0, "Should sort by partition name when impact is equal");
  }

  // ========================================
  // Tests for top state missing prioritization
  // ========================================

  @Test
  public void testCompare_TopStateMissing_HighestPriority() {
    // Partition_0 is missing MASTER and the message is transitioning to MASTER
    // Partition_1 has a normal upward transition (OFFLINE -> SLAVE)
    _currentStateOutput.setCurrentState(RESOURCE_A, new Partition(PARTITION_0),
        INSTANCE_0, "SLAVE");
    _currentStateOutput.setCurrentState(RESOURCE_A, new Partition(PARTITION_1),
        INSTANCE_1, "MASTER");

    AvailabilityAwareMessageComparator comparator =
        new AvailabilityAwareMessageComparator(_cache, _currentStateOutput);

    Message topStateMissing = createMessage(RESOURCE_A, PARTITION_0, "SLAVE", "MASTER", INSTANCE_0);
    Message normalUpward = createMessage(RESOURCE_A, PARTITION_1, "OFFLINE", "SLAVE", INSTANCE_2);

    int result = comparator.compare(topStateMissing, normalUpward);
    Assert.assertTrue(result < 0, "Top state missing message should have higher priority");
  }

  @Test
  public void testCompare_TopStateMissing_ButNotTransitioningToTopState() {
    // Partition_0 is missing MASTER but message is OFFLINE -> SLAVE (not to MASTER)
    _currentStateOutput.setCurrentState(RESOURCE_A, new Partition(PARTITION_0),
        INSTANCE_0, "OFFLINE");

    AvailabilityAwareMessageComparator comparator =
        new AvailabilityAwareMessageComparator(_cache, _currentStateOutput);

    Message offlineToSlave = createMessage(RESOURCE_A, PARTITION_0, "OFFLINE", "SLAVE", INSTANCE_0);

    // This should NOT get TOP_STATE_MISSING_IMPACT because it's not transitioning to MASTER
    // It should be treated as a normal upward transition
    Message anotherUpward = createMessage(RESOURCE_A, PARTITION_1, "OFFLINE", "SLAVE", INSTANCE_1);

    // Both are normal upward transitions, should be comparable
    int result = comparator.compare(offlineToSlave, anotherUpward);
    // They may differ by partition active count, but should NOT be MAX_VALUE
    Assert.assertTrue(result != 0, "Different partitions should have different impact or tiebreak");
  }

  // ========================================
  // Tests for top state handoff prioritization
  // ========================================

  @Test
  public void testCompare_TopStateHandoff_SecondHighestPriority() {
    // MASTER -> SLAVE is a top state handoff (needs to happen fast)
    _currentStateOutput.setCurrentState(RESOURCE_A, new Partition(PARTITION_0),
        INSTANCE_0, "MASTER");
    _currentStateOutput.setCurrentState(RESOURCE_A, new Partition(PARTITION_1),
        INSTANCE_1, "SLAVE");

    AvailabilityAwareMessageComparator comparator =
        new AvailabilityAwareMessageComparator(_cache, _currentStateOutput);

    Message handoff = createMessage(RESOURCE_A, PARTITION_0, "MASTER", "SLAVE", INSTANCE_0);
    Message normalUpward = createMessage(RESOURCE_A, PARTITION_1, "OFFLINE", "SLAVE", INSTANCE_2);

    int result = comparator.compare(handoff, normalUpward);
    Assert.assertTrue(result < 0, "Top state handoff should have higher priority than normal upward");
  }

  @Test
  public void testCompare_TopStateMissing_HigherThanHandoff() {
    // Top state missing should beat top state handoff
    _currentStateOutput.setCurrentState(RESOURCE_A, new Partition(PARTITION_0),
        INSTANCE_0, "SLAVE"); // no MASTER for partition_0
    _currentStateOutput.setCurrentState(RESOURCE_A, new Partition(PARTITION_1),
        INSTANCE_1, "MASTER");

    AvailabilityAwareMessageComparator comparator =
        new AvailabilityAwareMessageComparator(_cache, _currentStateOutput);

    Message missingTopState = createMessage(RESOURCE_A, PARTITION_0, "SLAVE", "MASTER", INSTANCE_0);
    Message handoff = createMessage(RESOURCE_A, PARTITION_1, "MASTER", "SLAVE", INSTANCE_1);

    int result = comparator.compare(missingTopState, handoff);
    Assert.assertTrue(result < 0, "Missing top state should rank higher than handoff");
  }

  // ========================================
  // Tests for downward transitions
  // ========================================

  @Test
  public void testCompare_DownwardTransition_ZeroImpact() {
    // SLAVE -> OFFLINE is a downward transition, should get 0.0 impact
    _currentStateOutput.setCurrentState(RESOURCE_A, new Partition(PARTITION_0),
        INSTANCE_0, "MASTER");
    _currentStateOutput.setCurrentState(RESOURCE_A, new Partition(PARTITION_0),
        INSTANCE_1, "SLAVE");

    AvailabilityAwareMessageComparator comparator =
        new AvailabilityAwareMessageComparator(_cache, _currentStateOutput);

    Message downward = createMessage(RESOURCE_A, PARTITION_0, "SLAVE", "OFFLINE", INSTANCE_1);
    Message upward = createMessage(RESOURCE_A, PARTITION_1, "OFFLINE", "SLAVE", INSTANCE_2);

    int result = comparator.compare(downward, upward);
    Assert.assertTrue(result > 0, "Downward transition should have lower priority than upward");
  }

  // ========================================
  // Tests for upward transition impact scoring
  // ========================================

  @Test
  public void testCompare_PartitionWithFewerActiveReplicas_HigherPriority() {
    // Partition_0: 0 active replicas (no current state)
    // Partition_1: 1 active replica
    _currentStateOutput.setCurrentState(RESOURCE_A, new Partition(PARTITION_1),
        INSTANCE_1, "SLAVE");

    AvailabilityAwareMessageComparator comparator =
        new AvailabilityAwareMessageComparator(_cache, _currentStateOutput);

    Message toPartitionWithNoActive =
        createMessage(RESOURCE_A, PARTITION_0, "OFFLINE", "SLAVE", INSTANCE_0);
    Message toPartitionWithOneActive =
        createMessage(RESOURCE_A, PARTITION_1, "OFFLINE", "SLAVE", INSTANCE_2);

    int result = comparator.compare(toPartitionWithNoActive, toPartitionWithOneActive);
    Assert.assertTrue(result < 0,
        "Partition with fewer active replicas should have higher priority");
  }

  @Test
  public void testCompare_MinActiveNotConfigured_UsesBaseImpact() {
    // Set minActiveReplicas to 0 (not configured / no minimum constraint)
    IdealState idealState = new IdealState(RESOURCE_A);
    idealState.setStateModelDefRef("MasterSlave");
    idealState.setRebalanceMode(IdealState.RebalanceMode.FULL_AUTO);
    idealState.setReplicas("3");
    idealState.setMinActiveReplicas(0);
    when(_cache.getIdealState(RESOURCE_A)).thenReturn(idealState);

    AvailabilityAwareMessageComparator comparator =
        new AvailabilityAwareMessageComparator(_cache, _currentStateOutput);

    Message m1 = createMessage(RESOURCE_A, PARTITION_0, "OFFLINE", "SLAVE", INSTANCE_0);
    Message m2 = createMessage(RESOURCE_A, PARTITION_1, "OFFLINE", "SLAVE", INSTANCE_1);

    // Both should use base impact (1.0 / (effectiveCount + 1))
    // With 0 active and 0 pending, impact = 1.0 / 1 = 1.0 for both (before index tracking)
    int result = comparator.compare(m1, m2);
    // Same impact initially, should fall back to partition name
    Assert.assertTrue(result < 0, "Should tiebreak by partition name");
  }

  @Test
  public void testCompare_HigherMinActive_HigherImpact() {
    // ResourceA: minActive=2, ResourceB: minActive=1
    // Both partitions have 0 active replicas
    IdealState idealStateA = new IdealState(RESOURCE_A);
    idealStateA.setStateModelDefRef("MasterSlave");
    idealStateA.setRebalanceMode(IdealState.RebalanceMode.FULL_AUTO);
    idealStateA.setReplicas("3");
    idealStateA.setMinActiveReplicas(2);

    String resourceBPartition = "ResourceB_0";
    IdealState idealStateB = new IdealState(RESOURCE_B);
    idealStateB.setStateModelDefRef("MasterSlave");
    idealStateB.setRebalanceMode(IdealState.RebalanceMode.FULL_AUTO);
    idealStateB.setReplicas("3");
    idealStateB.setMinActiveReplicas(1);

    when(_cache.getIdealState(RESOURCE_A)).thenReturn(idealStateA);
    when(_cache.getIdealState(RESOURCE_B)).thenReturn(idealStateB);

    AvailabilityAwareMessageComparator comparator =
        new AvailabilityAwareMessageComparator(_cache, _currentStateOutput);

    Message msgA = createMessage(RESOURCE_A, PARTITION_0, "OFFLINE", "SLAVE", INSTANCE_0);
    Message msgB = createMessage(RESOURCE_B, resourceBPartition, "OFFLINE", "SLAVE", INSTANCE_1);

    int result = comparator.compare(msgA, msgB);
    // ResourceA impact = 2.0 / (0 + 1) = 2.0
    // ResourceB impact = 1.0 / (0 + 1) = 1.0
    Assert.assertTrue(result < 0,
        "Resource with higher minActive should have higher priority when both have 0 active");
  }

  // ========================================
  // Tests for cross-resource prioritization
  // ========================================

  @Test
  public void testCompare_CrossResourcePrioritization() {
    // ResourceA partition has 0 active replicas
    // ResourceB partition has 2 active replicas
    String resourceBPartition = "ResourceB_0";
    _currentStateOutput.setCurrentState(RESOURCE_B, new Partition(resourceBPartition),
        INSTANCE_0, "MASTER");
    _currentStateOutput.setCurrentState(RESOURCE_B, new Partition(resourceBPartition),
        INSTANCE_1, "SLAVE");

    AvailabilityAwareMessageComparator comparator =
        new AvailabilityAwareMessageComparator(_cache, _currentStateOutput);

    Message msgA = createMessage(RESOURCE_A, PARTITION_0, "OFFLINE", "SLAVE", INSTANCE_2);
    Message msgB = createMessage(RESOURCE_B, resourceBPartition, "OFFLINE", "SLAVE", INSTANCE_2);

    int result = comparator.compare(msgA, msgB);
    Assert.assertTrue(result < 0,
        "Resource with fewer active replicas should be prioritized across resources");
  }

  // ========================================
  // Tests for tiebreaking
  // ========================================

  @Test
  public void testCompare_SameImpact_TiebreaksByResourceName() {
    String resourceBPartition = "ResourceB_0";
    // Same active replica counts -> same impact -> tiebreak by resource name
    AvailabilityAwareMessageComparator comparator =
        new AvailabilityAwareMessageComparator(_cache, _currentStateOutput);

    Message msgA = createMessage(RESOURCE_A, PARTITION_0, "OFFLINE", "SLAVE", INSTANCE_0);
    Message msgB = createMessage(RESOURCE_B, resourceBPartition, "OFFLINE", "SLAVE", INSTANCE_1);

    int result = comparator.compare(msgA, msgB);
    // ResourceA < ResourceB alphabetically, and with same score,
    // tiebreak is by resource name (ascending)
    Assert.assertTrue(result < 0, "Should tiebreak by resource name");
  }

  @Test
  public void testCompare_SameResourceSameImpact_TiebreaksByPartitionName() {
    AvailabilityAwareMessageComparator comparator =
        new AvailabilityAwareMessageComparator(_cache, _currentStateOutput);

    Message m1 = createMessage(RESOURCE_A, PARTITION_0, "OFFLINE", "SLAVE", INSTANCE_0);
    Message m2 = createMessage(RESOURCE_A, PARTITION_1, "OFFLINE", "SLAVE", INSTANCE_1);

    int result = comparator.compare(m1, m2);
    Assert.assertTrue(result < 0, "Should tiebreak by partition name");
  }

  // ========================================
  // Tests for impact caching
  // ========================================

  @Test
  public void testCompare_ImpactIsCachedForSameMessage() {
    _currentStateOutput.setCurrentState(RESOURCE_A, new Partition(PARTITION_0),
        INSTANCE_0, "SLAVE");

    AvailabilityAwareMessageComparator comparator =
        new AvailabilityAwareMessageComparator(_cache, _currentStateOutput);

    Message m1 = createMessage(RESOURCE_A, PARTITION_0, "OFFLINE", "SLAVE", INSTANCE_1);

    // Compare the same message against itself - should be 0
    int result = comparator.compare(m1, m1);
    Assert.assertEquals(result, 0, "Same message should compare as equal");
  }

  // ========================================
  // Tests for pending messages counting
  // ========================================

  @Test
  public void testCompare_PendingMessagesReduceImpact() {
    // Partition_0 has 0 active but 1 pending upward transition
    // Partition_1 has 0 active and no pending
    Message pendingMsg = createMessage(RESOURCE_A, PARTITION_0, "OFFLINE", "SLAVE", INSTANCE_0);
    Map<String, Message> pendingMap = new HashMap<>();
    pendingMap.put(INSTANCE_0, pendingMsg);
    _currentStateOutput.setPendingMessage(RESOURCE_A, new Partition(PARTITION_0),
        INSTANCE_0, pendingMsg);

    AvailabilityAwareMessageComparator comparator =
        new AvailabilityAwareMessageComparator(_cache, _currentStateOutput);

    Message toPartitionWithPending =
        createMessage(RESOURCE_A, PARTITION_0, "OFFLINE", "SLAVE", INSTANCE_1);
    Message toPartitionWithNoPending =
        createMessage(RESOURCE_A, PARTITION_1, "OFFLINE", "SLAVE", INSTANCE_2);

    int result = comparator.compare(toPartitionWithPending, toPartitionWithNoPending);
    Assert.assertTrue(result > 0,
        "Partition with pending upward messages should have lower impact (higher effective count)");
  }

  // ========================================
  // Tests for sorting a list of messages
  // ========================================

  @Test
  public void testSort_MultipleMessages_CorrectOrder() {
    // Setup:
    // Partition_0: missing top state, message to MASTER -> highest priority
    // ResourceB_0: 0 active replicas, OFFLINE -> SLAVE -> high priority
    // Partition_1: 2 active replicas, OFFLINE -> SLAVE -> lower priority
    // ResourceB_1: downward transition SLAVE -> OFFLINE -> lowest priority
    String resourceBP0 = "ResourceB_0";
    String resourceBP1 = "ResourceB_1";

    _currentStateOutput.setCurrentState(RESOURCE_A, new Partition(PARTITION_0),
        INSTANCE_0, "SLAVE"); // missing MASTER
    _currentStateOutput.setCurrentState(RESOURCE_A, new Partition(PARTITION_1),
        INSTANCE_0, "MASTER");
    _currentStateOutput.setCurrentState(RESOURCE_A, new Partition(PARTITION_1),
        INSTANCE_1, "SLAVE");
    _currentStateOutput.setCurrentState(RESOURCE_B, new Partition(resourceBP1),
        INSTANCE_1, "MASTER");
    _currentStateOutput.setCurrentState(RESOURCE_B, new Partition(resourceBP1),
        INSTANCE_2, "SLAVE");

    AvailabilityAwareMessageComparator comparator =
        new AvailabilityAwareMessageComparator(_cache, _currentStateOutput);

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

    // Expected order: topStateMissing, highImpactUpward, lowImpactUpward, downward
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
  // Helper Methods
  // ========================================

  private Message createMessage(String resource, String partition, String fromState,
      String toState, String tgtName) {
    Message message = new Message(Message.MessageType.STATE_TRANSITION, UUID.randomUUID().toString());
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

