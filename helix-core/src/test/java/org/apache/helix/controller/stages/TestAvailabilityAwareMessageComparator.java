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
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import org.apache.helix.controller.dataproviders.ResourceControllerDataProvider;
import org.apache.helix.model.BuiltInStateModelDefinitions;
import org.apache.helix.model.ClusterConfig;
import org.apache.helix.model.IdealState;
import org.apache.helix.model.Message;
import org.apache.helix.model.Partition;
import org.apache.helix.model.StateModelDefinition;
import org.apache.helix.tools.StateModelConfigGenerator;
import org.apache.helix.zookeeper.datamodel.ZNRecord;
import org.mockito.Mockito;
import org.testng.Assert;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import static org.mockito.Mockito.when;

/**
 * Tests for {@link AvailabilityAwareMessageComparator}.
 * Validates that messages are prioritized correctly based on:
 * 1. Top state missing (highest priority)
 * 2. Top state handoff downward transitions
 * 3. Availability impact score
 * 4. Resource priority
 * 5. Deterministic ordering
 */
public class TestAvailabilityAwareMessageComparator {

  private ResourceControllerDataProvider _cache;
  private CurrentStateOutput _currentStateOutput;
  private StateModelDefinition _masterSlaveStateModel;
  private Map<String, Integer> _resourcePriorityMap;

  @BeforeMethod
  public void setUp() {
    _cache = Mockito.mock(ResourceControllerDataProvider.class);
    _currentStateOutput = new CurrentStateOutput();
    _resourcePriorityMap = new HashMap<>();

    // Setup MasterSlave state model
    _masterSlaveStateModel = BuiltInStateModelDefinitions.MasterSlave.getStateModelDefinition();
    when(_cache.getStateModelDef("MasterSlave")).thenReturn(_masterSlaveStateModel);
    when(_cache.getEnabledLiveInstances()).thenReturn(
        new java.util.HashSet<>(java.util.Arrays.asList("instance0", "instance1", "instance2")));
  }

  @Test
  public void testTopStateMissingHasHighestPriority() {
    // Setup two resources
    String resource1 = "resource1"; // Partition missing top state
    String resource2 = "resource2"; // Partition has top state

    setupResource(resource1, 2, 3);
    setupResource(resource2, 2, 3);

    // Resource1 partition is missing MASTER
    Partition partition1 = new Partition(resource1 + "_0");
    _currentStateOutput.setCurrentState(resource1, partition1, "instance0", "SLAVE");
    _currentStateOutput.setCurrentState(resource1, partition1, "instance1", "SLAVE");

    // Resource2 partition has MASTER
    Partition partition2 = new Partition(resource2 + "_0");
    _currentStateOutput.setCurrentState(resource2, partition2, "instance0", "MASTER");
    _currentStateOutput.setCurrentState(resource2, partition2, "instance1", "SLAVE");

    // Create messages
    Message msg1 = createMessage(resource1, partition1.getPartitionName(), "instance2", "OFFLINE", "SLAVE");
    Message msg2 = createMessage(resource2, partition2.getPartitionName(), "instance2", "OFFLINE", "SLAVE");

    // Give resource2 higher priority (should not matter - top state missing wins)
    _resourcePriorityMap.put(resource1, 1);
    _resourcePriorityMap.put(resource2, 100);

    AvailabilityAwareMessageComparator comparator =
        new AvailabilityAwareMessageComparator(_cache, _currentStateOutput, _resourcePriorityMap);

    // msg1 should have higher priority (top state missing)
    Assert.assertTrue(comparator.compare(msg1, msg2) < 0,
        "Message for partition missing top state should have higher priority");
  }

  @Test
  public void testTopStateHandoffDownwardHasHighPriority() {
    String resource = "resource1";
    setupResource(resource, 2, 3);

    Partition partition = new Partition(resource + "_0");
    // Current state: instance0 is MASTER, instance1 is SLAVE
    _currentStateOutput.setCurrentState(resource, partition, "instance0", "MASTER");
    _currentStateOutput.setCurrentState(resource, partition, "instance1", "SLAVE");

    // Top state handoff: MASTER -> SLAVE on instance0 (should have high priority)
    Message handoffMsg = createMessage(resource, partition.getPartitionName(), "instance0", "MASTER", "SLAVE");

    // Regular load balance: OFFLINE -> SLAVE on instance2
    Message loadBalanceMsg = createMessage(resource, partition.getPartitionName(), "instance2", "OFFLINE", "SLAVE");

    AvailabilityAwareMessageComparator comparator =
        new AvailabilityAwareMessageComparator(_cache, _currentStateOutput, _resourcePriorityMap);

    // Handoff message should have higher priority
    Assert.assertTrue(comparator.compare(handoffMsg, loadBalanceMsg) < 0,
        "Top state handoff downward transition should have higher priority than load balance");
  }

  @Test
  public void testHigherAvailabilityImpactHasHigherPriority() {
    // Setup two resources with different current replica counts
    String resource1 = "resource1"; // minActiveReplicas=2, currentActive=1 (needs recovery badly)
    String resource2 = "resource2"; // minActiveReplicas=2, currentActive=2 (just load balance)

    setupResource(resource1, 2, 3);
    setupResource(resource2, 2, 3);

    // Resource1: only 1 active replica
    Partition partition1 = new Partition(resource1 + "_0");
    _currentStateOutput.setCurrentState(resource1, partition1, "instance0", "MASTER");

    // Resource2: 2 active replicas
    Partition partition2 = new Partition(resource2 + "_0");
    _currentStateOutput.setCurrentState(resource2, partition2, "instance0", "MASTER");
    _currentStateOutput.setCurrentState(resource2, partition2, "instance1", "SLAVE");

    Message msg1 = createMessage(resource1, partition1.getPartitionName(), "instance1", "OFFLINE", "SLAVE");
    Message msg2 = createMessage(resource2, partition2.getPartitionName(), "instance2", "OFFLINE", "SLAVE");

    AvailabilityAwareMessageComparator comparator =
        new AvailabilityAwareMessageComparator(_cache, _currentStateOutput, _resourcePriorityMap);

    // msg1 should have higher priority (fewer active replicas = higher impact)
    Assert.assertTrue(comparator.compare(msg1, msg2) < 0,
        "Message for partition with fewer active replicas should have higher priority");
  }

  @Test
  public void testResourcePriorityAsSecondarySort() {
    // Setup two resources with same availability impact
    String resource1 = "resource1";
    String resource2 = "resource2";

    setupResource(resource1, 2, 3);
    setupResource(resource2, 2, 3);

    // Both have same current state (2 active replicas)
    Partition partition1 = new Partition(resource1 + "_0");
    _currentStateOutput.setCurrentState(resource1, partition1, "instance0", "MASTER");
    _currentStateOutput.setCurrentState(resource1, partition1, "instance1", "SLAVE");

    Partition partition2 = new Partition(resource2 + "_0");
    _currentStateOutput.setCurrentState(resource2, partition2, "instance0", "MASTER");
    _currentStateOutput.setCurrentState(resource2, partition2, "instance1", "SLAVE");

    Message msg1 = createMessage(resource1, partition1.getPartitionName(), "instance2", "OFFLINE", "SLAVE");
    Message msg2 = createMessage(resource2, partition2.getPartitionName(), "instance2", "OFFLINE", "SLAVE");

    // Give resource2 higher priority
    _resourcePriorityMap.put(resource1, 1);
    _resourcePriorityMap.put(resource2, 100);

    AvailabilityAwareMessageComparator comparator =
        new AvailabilityAwareMessageComparator(_cache, _currentStateOutput, _resourcePriorityMap);

    // msg2 should have higher priority (higher resource priority)
    Assert.assertTrue(comparator.compare(msg1, msg2) > 0,
        "Message for higher priority resource should have higher priority when impact is same");
  }

  @Test
  public void testDeterministicOrderingByResourceAndPartitionName() {
    // Setup resources with same impact and same priority
    String resourceA = "resourceA";
    String resourceB = "resourceB";

    setupResource(resourceA, 2, 3);
    setupResource(resourceB, 2, 3);

    // Same current state
    Partition partitionA = new Partition(resourceA + "_0");
    _currentStateOutput.setCurrentState(resourceA, partitionA, "instance0", "MASTER");
    _currentStateOutput.setCurrentState(resourceA, partitionA, "instance1", "SLAVE");

    Partition partitionB = new Partition(resourceB + "_0");
    _currentStateOutput.setCurrentState(resourceB, partitionB, "instance0", "MASTER");
    _currentStateOutput.setCurrentState(resourceB, partitionB, "instance1", "SLAVE");

    Message msgA = createMessage(resourceA, partitionA.getPartitionName(), "instance2", "OFFLINE", "SLAVE");
    Message msgB = createMessage(resourceB, partitionB.getPartitionName(), "instance2", "OFFLINE", "SLAVE");

    // Same priority
    _resourcePriorityMap.put(resourceA, 1);
    _resourcePriorityMap.put(resourceB, 1);

    AvailabilityAwareMessageComparator comparator =
        new AvailabilityAwareMessageComparator(_cache, _currentStateOutput, _resourcePriorityMap);

    // resourceA should come first alphabetically
    Assert.assertTrue(comparator.compare(msgA, msgB) < 0,
        "When all else is equal, should sort alphabetically by resource name");
  }

  @Test
  public void testMinActiveReplicaZeroFallsBackToTargetReplicas() {
    // Test the edge case where minActiveReplicas = 0
    String resource = "resource1";

    // Setup resource with minActiveReplicas = 0 (fallback to targetReplicas = 3)
    IdealState idealState = new IdealState(resource);
    idealState.setStateModelDefRef("MasterSlave");
    idealState.setRebalanceMode(IdealState.RebalanceMode.FULL_AUTO);
    idealState.setNumPartitions(1);
    idealState.setReplicas("3");
    idealState.setMinActiveReplicas(0); // This should fallback to targetReplicas
    idealState.setPreferenceList(resource + "_0",
        java.util.Arrays.asList("instance0", "instance1", "instance2"));

    when(_cache.getIdealState(resource)).thenReturn(idealState);

    Partition partition = new Partition(resource + "_0");
    _currentStateOutput.setCurrentState(resource, partition, "instance0", "MASTER");

    Message msg = createMessage(resource, partition.getPartitionName(), "instance1", "OFFLINE", "SLAVE");

    AvailabilityAwareMessageComparator comparator =
        new AvailabilityAwareMessageComparator(_cache, _currentStateOutput, _resourcePriorityMap);

    // The message should still get a valid impact score (using targetReplicas=3)
    // Impact = 3 / (1 + 1) = 1.5
    comparator.compare(msg, msg); // Just verify it doesn't throw
  }

  @Test
  public void testDownwardTransitionDeprioritized() {
    String resource = "resource1";
    setupResource(resource, 2, 3);

    Partition partition = new Partition(resource + "_0");
    _currentStateOutput.setCurrentState(resource, partition, "instance0", "MASTER");
    _currentStateOutput.setCurrentState(resource, partition, "instance1", "SLAVE");
    _currentStateOutput.setCurrentState(resource, partition, "instance2", "SLAVE");

    // Upward transition: OFFLINE -> SLAVE
    Message upwardMsg = createMessage(resource, partition.getPartitionName(), "instance3", "OFFLINE", "SLAVE");

    // Downward transition: SLAVE -> OFFLINE (not top state handoff)
    Message downwardMsg = createMessage(resource, partition.getPartitionName(), "instance2", "SLAVE", "OFFLINE");

    AvailabilityAwareMessageComparator comparator =
        new AvailabilityAwareMessageComparator(_cache, _currentStateOutput, _resourcePriorityMap);

    // Upward should have higher priority than non-top-state downward
    Assert.assertTrue(comparator.compare(upwardMsg, downwardMsg) < 0,
        "Upward transition should have higher priority than non-top-state downward transition");
  }

  @Test
  public void testCrossResourcePrioritizationScenario() {
    // Simulate the scenario from the RFC:
    // R1:P1 SLAVE→MASTER (recovery)
    // R2:P2 OFFLINE→SLAVE (load balance, 2 active)
    // R3:P3 OFFLINE→SLAVE (load balance, 1 active) <- should be prioritized over R2:P2

    String r1 = "R1";
    String r2 = "R2";
    String r3 = "R3";

    setupResource(r1, 1, 3);
    setupResource(r2, 1, 3);
    setupResource(r3, 1, 3);

    // R1:P1 - missing MASTER
    Partition p1 = new Partition(r1 + "_P1");
    _currentStateOutput.setCurrentState(r1, p1, "instance0", "SLAVE");
    _currentStateOutput.setCurrentState(r1, p1, "instance1", "SLAVE");

    // R2:P2 - has MASTER and 2 replicas
    Partition p2 = new Partition(r2 + "_P2");
    _currentStateOutput.setCurrentState(r2, p2, "instance0", "MASTER");
    _currentStateOutput.setCurrentState(r2, p2, "instance1", "SLAVE");

    // R3:P3 - has MASTER but only 1 replica
    Partition p3 = new Partition(r3 + "_P3");
    _currentStateOutput.setCurrentState(r3, p3, "instance0", "MASTER");

    Message msg1 = createMessage(r1, p1.getPartitionName(), "instance0", "SLAVE", "MASTER");
    Message msg2 = createMessage(r2, p2.getPartitionName(), "instance2", "OFFLINE", "SLAVE");
    Message msg3 = createMessage(r3, p3.getPartitionName(), "instance1", "OFFLINE", "SLAVE");

    AvailabilityAwareMessageComparator comparator =
        new AvailabilityAwareMessageComparator(_cache, _currentStateOutput, _resourcePriorityMap);

    List<Message> messages = new ArrayList<>();
    messages.add(msg2);
    messages.add(msg3);
    messages.add(msg1);

    messages.sort(comparator);

    // Expected order: msg1 (top state missing), msg3 (1 replica), msg2 (2 replicas)
    Assert.assertEquals(messages.get(0).getResourceName(), r1,
        "R1:P1 (top state missing) should be first");
    Assert.assertEquals(messages.get(1).getResourceName(), r3,
        "R3:P3 (fewer replicas) should be second");
    Assert.assertEquals(messages.get(2).getResourceName(), r2,
        "R2:P2 (more replicas) should be last");
  }

  // Helper methods

  private void setupResource(String resourceName, int minActiveReplicas, int targetReplicas) {
    IdealState idealState = new IdealState(resourceName);
    idealState.setStateModelDefRef("MasterSlave");
    idealState.setRebalanceMode(IdealState.RebalanceMode.FULL_AUTO);
    idealState.setNumPartitions(1);
    idealState.setReplicas(String.valueOf(targetReplicas));
    idealState.setMinActiveReplicas(minActiveReplicas);
    idealState.setPreferenceList(resourceName + "_0",
        java.util.Arrays.asList("instance0", "instance1", "instance2"));

    when(_cache.getIdealState(resourceName)).thenReturn(idealState);
  }

  private Message createMessage(String resourceName, String partitionName,
      String targetInstance, String fromState, String toState) {
    String msgId = UUID.randomUUID().toString();
    Message message = new Message(Message.MessageType.STATE_TRANSITION, msgId);
    message.setResourceName(resourceName);
    message.setPartitionName(partitionName);
    message.setTgtName(targetInstance);
    message.setFromState(fromState);
    message.setToState(toState);
    message.setTgtSessionId("session_" + targetInstance);
    message.setMsgState(Message.MessageState.NEW);
    return message;
  }
}









