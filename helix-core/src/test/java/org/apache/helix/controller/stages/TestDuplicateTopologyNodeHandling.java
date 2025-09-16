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
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import org.apache.helix.controller.dataproviders.ResourceControllerDataProvider;
import org.apache.helix.controller.rebalancer.DelayedAutoRebalancer;
import org.apache.helix.controller.rebalancer.strategy.CrushEd2RebalanceStrategy;
import org.apache.helix.controller.rebalancer.topology.DuplicateTopologyNodeException;
import org.apache.helix.model.ClusterConfig;
import org.apache.helix.model.IdealState;
import org.apache.helix.model.InstanceConfig;
import org.apache.helix.model.Partition;
import org.apache.helix.model.Resource;
import org.apache.helix.model.StateModelDefinition;
import org.testng.Assert;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * test for duplicate topology node handling.
 * Verifies that partitions are preserved when topology construction fails.
 */
public class TestDuplicateTopologyNodeHandling {

  private BestPossibleStateCalcStage stage;
  private ClusterEvent event;
  private ResourceControllerDataProvider cache;
  private CurrentStateOutput currentStateOutput;
  private BestPossibleStateOutput bestPossibleStateOutput;
  private String resourceName;
  private Resource resource;
  private IdealState idealState;
  private StateModelDefinition stateModelDef;

  @BeforeMethod
  public void setUp() {
    stage = new BestPossibleStateCalcStage();
    event = mock(ClusterEvent.class);
    cache = mock(ResourceControllerDataProvider.class);
    currentStateOutput = new CurrentStateOutput();
    bestPossibleStateOutput = new BestPossibleStateOutput();

    resourceName = "TestResource";
    resource = new Resource(resourceName);

    // Setup state model definition
    stateModelDef = new StateModelDefinition.Builder("MasterSlave")
        .addState("MASTER", 1)
        .addState("SLAVE", 2)
        .addState("OFFLINE")
        .addState("DROPPED")
        .addState("ERROR")
        .initialState("OFFLINE")
        .addTransition("OFFLINE", "SLAVE", 3)
        .addTransition("SLAVE", "OFFLINE", 4)
        .addTransition("SLAVE", "MASTER", 2)
        .addTransition("MASTER", "SLAVE", 1)
        .addTransition("OFFLINE", "DROPPED", 5)
        .dynamicUpperBound("MASTER", "R")
        .dynamicUpperBound("SLAVE", "N")
        .build();

    when(event.getAttribute(AttributeName.ControllerDataProvider.name())).thenReturn(cache);
    when(event.getAttribute(AttributeName.CURRENT_STATE_EXCLUDING_UNKNOWN.name())).thenReturn(currentStateOutput);
    when(event.getAttribute(AttributeName.CURRENT_STATE.name())).thenReturn(currentStateOutput);
    when(event.getEventId()).thenReturn(UUID.randomUUID().toString());
    when(cache.getStateModelDef("MasterSlave")).thenReturn(stateModelDef);
    when(cache.getPipelineName()).thenReturn("DEFAULT");
  }

  @Test
  public void testDuplicateTopologyNodeExceptionThrown() {
    // Setup duplicate topology configuration
    String instance1 = "host1_12345";
    String instance2 = "host2_12345";
    String instance3 = "host3_12346";

    ClusterConfig clusterConfig = new ClusterConfig("TestCluster");
    clusterConfig.setTopologyAwareEnabled(true);
    clusterConfig.setTopology("/rack/host");
    clusterConfig.setFaultZoneType("rack");

    Map<String, InstanceConfig> instanceConfigMap = new HashMap<>();

    InstanceConfig config1 = new InstanceConfig(instance1);
    config1.setDomain("rack=rack1,host=duplicateHost");
    instanceConfigMap.put(instance1, config1);

    InstanceConfig config2 = new InstanceConfig(instance2);
    config2.setDomain("rack=rack1,host=duplicateHost"); // Same host - causes duplicate
    instanceConfigMap.put(instance2, config2);

    InstanceConfig config3 = new InstanceConfig(instance3);
    config3.setDomain("rack=rack2,host=uniqueHost");
    instanceConfigMap.put(instance3, config3);

    List<String> allNodes = new ArrayList<>();
    allNodes.add(instance1);
    allNodes.add(instance2);
    allNodes.add(instance3);

    List<String> liveNodes = new ArrayList<>(allNodes);

    ResourceControllerDataProvider dataProvider = mock(ResourceControllerDataProvider.class);
    when(dataProvider.getClusterConfig()).thenReturn(clusterConfig);
    when(dataProvider.getAssignableInstanceConfigMap()).thenReturn(instanceConfigMap);
    when(dataProvider.getClusterEventId()).thenReturn("testEvent");

    CrushEd2RebalanceStrategy strategy = new CrushEd2RebalanceStrategy();

    List<String> partitions = new ArrayList<>();
    partitions.add("TestResource_0");
    partitions.add("TestResource_1");

    LinkedHashMap<String, Integer> stateCountMap = new LinkedHashMap<>();
    stateCountMap.put("MASTER", 1);
    stateCountMap.put("SLAVE", 2);

    strategy.init(resourceName, partitions, stateCountMap, 3);

    // Verify that DuplicateTopologyNodeException is thrown
    try {
      strategy.computePartitionAssignment(allNodes, liveNodes, new HashMap<>(), dataProvider);
      Assert.fail("Expected DuplicateTopologyNodeException but no exception was thrown");
    } catch (DuplicateTopologyNodeException e) {
      // Verify exception details
      Assert.assertTrue(e.getMessage().contains("duplicate leaf nodes"));
      Assert.assertEquals(e.getDuplicateNodeName(), "duplicateHost");
      Assert.assertTrue(e.getInstanceName().equals(instance1) || e.getInstanceName().equals(instance2));
    } catch (Exception e) {
      Assert.fail("Expected DuplicateTopologyNodeException but got: " + e.getClass().getName());
    }
  }

  @Test
  public void testPreservationFromIdealStateOnDuplicateTopology() {
    String partition0 = "TestResource_0";
    String partition1 = "TestResource_1";
    resource.addPartition(partition0);
    resource.addPartition(partition1);

    // Setup IdealState with preference lists
    idealState = new IdealState(resourceName);
    idealState.setRebalanceMode(IdealState.RebalanceMode.FULL_AUTO);
    idealState.setStateModelDefRef("MasterSlave");
    idealState.setReplicas("3");

    Map<String, List<String>> preferenceLists = new HashMap<>();
    preferenceLists.put(partition0, Arrays.asList("instance1", "instance2", "instance3"));
    preferenceLists.put(partition1, Arrays.asList("instance2", "instance3", "instance1"));
    idealState.setPreferenceLists(preferenceLists);

    when(cache.getIdealState(resourceName)).thenReturn(idealState);
    when(cache.getStateModelDef("MasterSlave")).thenReturn(stateModelDef);

    Map<String, Resource> resourceMap = new HashMap<>();
    resourceMap.put(resourceName, resource);
    when(event.getAttribute(AttributeName.RESOURCES_TO_REBALANCE.name())).thenReturn(resourceMap);

    // Setup rebalancer to throw DuplicateTopologyNodeException
    DelayedAutoRebalancer rebalancer = mock(DelayedAutoRebalancer.class);
    when(rebalancer.computeNewIdealState(anyString(), any(IdealState.class),
        any(CurrentStateOutput.class), any(ResourceControllerDataProvider.class)))
        .thenThrow(new DuplicateTopologyNodeException("duplicateHost", "instance1"));

    // Execute the stage with mocked rebalancer
    testWithMockedRebalancer(rebalancer);

    // Verify that preference lists were preserved from IdealState
    Assert.assertEquals(bestPossibleStateOutput.getPreferenceLists(resourceName),
        idealState.getPreferenceLists());

    // Verify that states were preserved
    Map<String, String> partition0States = bestPossibleStateOutput.getInstanceStateMap(
        resourceName, new Partition(partition0));
    Assert.assertNotNull(partition0States);
    Assert.assertTrue(!partition0States.isEmpty(), "Should have at least 1 replica for partition 0");

    Map<String, String> partition1States = bestPossibleStateOutput.getInstanceStateMap(
        resourceName, new Partition(partition1));
    Assert.assertNotNull(partition1States);
    Assert.assertTrue(!partition1States.isEmpty(), "Should have at least 1 replica for partition 1");
  }

  @Test
  public void testFallbackToCurrentStateWhenNoIdealState() {
    // Setup resource with partitions
    String partition0 = "TestResource_0";
    String partition1 = "TestResource_1";
    resource.addPartition(partition0);
    resource.addPartition(partition1);

    // No IdealState available
    when(cache.getIdealState(resourceName)).thenReturn(null);

    // Setup current state
    currentStateOutput.setCurrentState(resourceName, new Partition(partition0), "instance1", "MASTER");
    currentStateOutput.setCurrentState(resourceName, new Partition(partition0), "instance2", "SLAVE");
    currentStateOutput.setCurrentState(resourceName, new Partition(partition1), "instance3", "MASTER");
    currentStateOutput.setCurrentState(resourceName, new Partition(partition1), "instance1", "SLAVE");

    Map<String, Resource> resourceMap = new HashMap<>();
    resourceMap.put(resourceName, resource);
    when(event.getAttribute(AttributeName.RESOURCES_TO_REBALANCE.name())).thenReturn(resourceMap);

    // Execute preservation
    stage.preserveCurrentStateInOutput(resourceName, resource, currentStateOutput,
        bestPossibleStateOutput, cache);

    // Verify states were preserved from current state
    Map<String, String> partition0States = bestPossibleStateOutput.getInstanceStateMap(
        resourceName, new Partition(partition0));
    Assert.assertEquals(partition0States.get("instance1"), "MASTER");
    Assert.assertEquals(partition0States.get("instance2"), "SLAVE");

    Map<String, String> partition1States = bestPossibleStateOutput.getInstanceStateMap(
        resourceName, new Partition(partition1));
    Assert.assertEquals(partition1States.get("instance3"), "MASTER");
    Assert.assertEquals(partition1States.get("instance1"), "SLAVE");

    // Verify preference lists were created from current state
    Map<String, List<String>> preferenceLists = bestPossibleStateOutput.getPreferenceLists(resourceName);
    Assert.assertNotNull(preferenceLists);
    Assert.assertTrue(preferenceLists.get(partition0).contains("instance1"));
    Assert.assertTrue(preferenceLists.get(partition0).contains("instance2"));
    Assert.assertTrue(preferenceLists.get(partition1).contains("instance3"));
    Assert.assertTrue(preferenceLists.get(partition1).contains("instance1"));
  }

  @Test
  public void testErrorStatesArePreserved() {
    // Setup resource with partition
    String partition0 = "TestResource_0";
    resource.addPartition(partition0);

    // No IdealState to force fallback to current state
    when(cache.getIdealState(resourceName)).thenReturn(null);

    // Setup current state with ERROR state
    currentStateOutput.setCurrentState(resourceName, new Partition(partition0), "instance1", "ERROR");
    currentStateOutput.setCurrentState(resourceName, new Partition(partition0), "instance2", "SLAVE");

    // Execute preservation
    stage.preserveCurrentStateInOutput(resourceName, resource, currentStateOutput,
        bestPossibleStateOutput, cache);

    // Verify ERROR state was preserved
    Map<String, String> partition0States = bestPossibleStateOutput.getInstanceStateMap(
        resourceName, new Partition(partition0));
    Assert.assertEquals(partition0States.get("instance1"), "ERROR");
    Assert.assertEquals(partition0States.get("instance2"), "SLAVE");
  }

  @Test
  public void testSemiAutoModePreservation() {
    String partition0 = "TestResource_0";
    resource.addPartition(partition0);

    // Setup IdealState in SEMI_AUTO mode with instance-state map
    idealState = new IdealState(resourceName);
    idealState.setRebalanceMode(IdealState.RebalanceMode.SEMI_AUTO);
    idealState.setStateModelDefRef("MasterSlave");

    // Set instance-state map directly
    Map<String, String> instanceStateMap = new HashMap<>();
    instanceStateMap.put("instance1", "MASTER");
    instanceStateMap.put("instance2", "SLAVE");
    instanceStateMap.put("instance3", "SLAVE");
    idealState.setInstanceStateMap(partition0, instanceStateMap);

    when(cache.getIdealState(resourceName)).thenReturn(idealState);

    // Execute preservation
    stage.preserveCurrentStateInOutput(resourceName, resource, currentStateOutput,
        bestPossibleStateOutput, cache);

    // Verify states were copied directly from IdealState
    Map<String, String> partition0States = bestPossibleStateOutput.getInstanceStateMap(
        resourceName, new Partition(partition0));
    Assert.assertEquals(partition0States.get("instance1"), "MASTER");
    Assert.assertEquals(partition0States.get("instance2"), "SLAVE");
    Assert.assertEquals(partition0States.get("instance3"), "SLAVE");
  }

  @Test
  public void testNoPartitionsDroppedOnTopologyError() {
    // Setup resource with multiple partitions
    for (int i = 0; i < 10; i++) {
      resource.addPartition("TestResource_" + i);
    }

    // Setup current state with all partitions assigned
    for (int i = 0; i < 10; i++) {
      String partitionName = "TestResource_" + i;
      currentStateOutput.setCurrentState(resourceName, new Partition(partitionName),
          "instance" + (i % 3), i == 0 ? "MASTER" : "SLAVE");
    }

    // No IdealState to force current state preservation
    when(cache.getIdealState(resourceName)).thenReturn(null);

    // Execute preservation
    stage.preserveCurrentStateInOutput(resourceName, resource, currentStateOutput,
        bestPossibleStateOutput, cache);

    // Verify all partitions are still assigned
    for (int i = 0; i < 10; i++) {
      String partitionName = "TestResource_" + i;
      Map<String, String> stateMap = bestPossibleStateOutput.getInstanceStateMap(
          resourceName, new Partition(partitionName));

      Assert.assertNotNull(stateMap, "Partition " + partitionName + " should have state assignments");
      Assert.assertFalse(stateMap.isEmpty(), "Partition " + partitionName + " should not be dropped");

      // Verify the correct instance and state
      String expectedInstance = "instance" + (i % 3);
      String expectedState = i == 0 ? "MASTER" : "SLAVE";
      Assert.assertEquals(stateMap.get(expectedInstance), expectedState);
    }
  }

  @Test
  public void testCustomizedModePreservation() {
    // Setup resource
    String partition0 = "TestResource_0";
    resource.addPartition(partition0);

    // Setup IdealState in CUSTOMIZED mode
    idealState = new IdealState(resourceName);
    idealState.setRebalanceMode(IdealState.RebalanceMode.CUSTOMIZED);
    idealState.setStateModelDefRef("MasterSlave");

    Map<String, String> instanceStateMap = new HashMap<>();
    instanceStateMap.put("instance1", "MASTER");
    instanceStateMap.put("instance2", "SLAVE");
    idealState.setInstanceStateMap(partition0, instanceStateMap);

    when(cache.getIdealState(resourceName)).thenReturn(idealState);

    // Execute preservation
    stage.preserveCurrentStateInOutput(resourceName, resource, currentStateOutput,
        bestPossibleStateOutput, cache);

    // Verify states were copied from IdealState
    Map<String, String> partition0States = bestPossibleStateOutput.getInstanceStateMap(
        resourceName, new Partition(partition0));
    Assert.assertEquals(partition0States.get("instance1"), "MASTER");
    Assert.assertEquals(partition0States.get("instance2"), "SLAVE");
  }

  // Helper method to test with a mocked rebalancer
  private void testWithMockedRebalancer(DelayedAutoRebalancer rebalancer) {
    stage.preserveCurrentStateInOutput(resourceName, resource, currentStateOutput,
        bestPossibleStateOutput, cache);
  }

  private static class Arrays {
    public static <T> List<T> asList(T... elements) {
      List<T> list = new ArrayList<>();
      for (T element : elements) {
        list.add(element);
      }
      return list;
    }
  }
}