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

import java.util.Arrays;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import org.apache.helix.HelixDefinedState;
import org.apache.helix.controller.dataproviders.ResourceControllerDataProvider;
import org.apache.helix.controller.rebalancer.DelayedAutoRebalancer;
import org.apache.helix.controller.rebalancer.strategy.GreedyRebalanceStrategy;
import org.apache.helix.model.BuiltInStateModelDefinitions;
import org.apache.helix.model.ClusterConfig;
import org.apache.helix.model.IdealState;
import org.apache.helix.model.IdealState.RebalanceMode;
import org.apache.helix.model.Partition;
import org.apache.helix.model.Resource;
import org.apache.helix.util.StageThreadPoolHelper;
import org.testng.Assert;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.Test;

public class TestBestPossibleStateCalcStage extends BaseStageTest {

  @AfterMethod
  public void afterMethod() {
    // Clean up thread pool after each test
    StageThreadPoolHelper.shutdown();
  }

  @Test
  public void testSimple() {
    System.out.println("START TestBestPossibleStateCalcStage at "
        + new Date(System.currentTimeMillis()));
    // List<IdealState> idealStates = new ArrayList<IdealState>();

    String[] resources = new String[] {
      "testResourceName"
    };

    int numPartition = 5;
    int numReplica = 1;

    setupIdealState(5, resources, numPartition, numReplica, RebalanceMode.SEMI_AUTO,
        BuiltInStateModelDefinitions.MasterSlave.name());
    setupLiveInstances(5);
    setupStateModel();
    setupInstances(5);

    Map<String, Resource> resourceMap =
        getResourceMap(resources, numPartition, BuiltInStateModelDefinitions.MasterSlave.name());
    CurrentStateOutput currentStateOutput = new CurrentStateOutput();
    event.addAttribute(AttributeName.RESOURCES.name(), resourceMap);
    event.addAttribute(AttributeName.RESOURCES_TO_REBALANCE.name(), resourceMap);
    event.addAttribute(AttributeName.CURRENT_STATE.name(), currentStateOutput);
    event.addAttribute(AttributeName.CURRENT_STATE_EXCLUDING_UNKNOWN.name(), currentStateOutput);
    event.addAttribute(AttributeName.ControllerDataProvider.name(), new ResourceControllerDataProvider());

    ReadClusterDataStage stage1 = new ReadClusterDataStage();
    runStage(event, stage1);
    BestPossibleStateCalcStage stage2 = new BestPossibleStateCalcStage();
    runStage(event, stage2);

    BestPossibleStateOutput output =
        event.getAttribute(AttributeName.BEST_POSSIBLE_STATE.name());
    for (int p = 0; p < 5; p++) {
      Partition resource = new Partition("testResourceName_" + p);
      Assert.assertEquals("MASTER", output.getInstanceStateMap("testResourceName", resource)
          .get("localhost_" + (p + 1) % 5));
    }
    System.out.println("END TestBestPossibleStateCalcStage at "
        + new Date(System.currentTimeMillis()));
  }

  /*
   * Tests the pipeline detects offline instances exceed the threshold and auto enters maintenance,
   * the maintenance rebalancer is used immediately. No bootstraps in the best possible output.
   */
  @Test
  public void testAutoEnterMaintenanceWhenExceedingOfflineNodes() {
    String[] resources = new String[]{"testResourceName"};
    int numInstances = 3;
    int numPartitions = 3;

    setupIdealState(numInstances, resources, numPartitions, 1, RebalanceMode.FULL_AUTO,
        BuiltInStateModelDefinitions.MasterSlave.name());
    setupInstances(numInstances);
    List<String> liveInstances = setupLiveInstances(numInstances);
    setupStateModel();

    // Set offline instances threshold
    ClusterConfig clusterConfig = accessor.getProperty(accessor.keyBuilder().clusterConfig());
    clusterConfig.setMaxOfflineInstancesAllowed(1);
    setClusterConfig(clusterConfig);

    Map<String, Resource> resourceMap =
        getResourceMap(resources, numPartitions, BuiltInStateModelDefinitions.MasterSlave.name());
    CurrentStateOutput currentStateOutput = new CurrentStateOutput();

    for (int p = 0; p < numPartitions; p++) {
      Partition partition = new Partition("testResourceName_" + p);
      currentStateOutput
          .setCurrentState("testResourceName", partition, "localhost_" + (p + 1) % numInstances,
              "MASTER");
    }

    // Disable 2 instances so the pipeline should enter maintenance
    for (int i = 0; i < 2; i++) {
      admin.enableInstance(_clusterName, liveInstances.get(i), false);
    }

    event.addAttribute(AttributeName.helixmanager.name(), manager);
    event.addAttribute(AttributeName.RESOURCES.name(), resourceMap);
    event.addAttribute(AttributeName.RESOURCES_TO_REBALANCE.name(), resourceMap);
    event.addAttribute(AttributeName.CURRENT_STATE.name(), currentStateOutput);
    event.addAttribute(AttributeName.CURRENT_STATE_EXCLUDING_UNKNOWN.name(), currentStateOutput);
    event.addAttribute(AttributeName.ControllerDataProvider.name(),
        new ResourceControllerDataProvider());

    runStage(event, new ReadClusterDataStage());
    runStage(event, new BestPossibleStateCalcStage());

    BestPossibleStateOutput output = event.getAttribute(AttributeName.BEST_POSSIBLE_STATE.name());

    // State on the disabled instances should be OFFLINE instead of DROPPED
    // because of maintenance rebalancer.
    Assert.assertEquals(
        output.getInstanceStateMap("testResourceName", new Partition("testResourceName_2"))
            .get("localhost_0"),
        "OFFLINE",
        "Actual state should not be DROPPED");

    Assert.assertEquals(
        output.getInstanceStateMap("testResourceName", new Partition("testResourceName_0"))
            .get("localhost_1"),
        "OFFLINE",
        "Actual state should not be DROPPED");

    // No state change for localhost_2 because the replica is already MASTER
    Assert.assertNull(
        output.getInstanceStateMap("testResourceName", new Partition("testResourceName_1"))
            .get("localhost_2"));
  }

  /**
   * Tests that when all instances are disabled, the pipeline continues and computes DROPPED
   * transitions for existing replicas. This verifies the fix for the bug where the last instance
   * would stay stuck as LEADER when all nodes are disabled.
   */
  @Test
  public void testAllNodesDisabledComputesDroppedForExistingReplicas() {
    String[] resources = new String[]{"resource_1"};
    int numInstances = 3;
    int numPartitions = 1;

    setupIdealState(numInstances, resources, numPartitions, 1, RebalanceMode.FULL_AUTO,
        BuiltInStateModelDefinitions.LeaderStandby.name(),
        DelayedAutoRebalancer.class.getName());
    setupInstances(numInstances);
    List<String> liveInstances = setupLiveInstances(numInstances);
    setupStateModel();

    // Short delay so disabled instances are immediately inactive (no delay window)
    ClusterConfig clusterConfig = accessor.getProperty(accessor.keyBuilder().clusterConfig());
    clusterConfig.setRebalanceDelayTime(0);
    clusterConfig.setDelayRebalaceEnabled(true);
    setClusterConfig(clusterConfig);

    Map<String, Resource> resourceMap =
        getResourceMap(resources, numPartitions, BuiltInStateModelDefinitions.LeaderStandby.name());
    CurrentStateOutput currentStateOutput = new CurrentStateOutput();

    // Simulate existing replica: localhost_2 holds LEADER for resource_1_0
    Partition partition = new Partition("resource_1_0");
    currentStateOutput.setCurrentState("resource_1", partition, "localhost_2", "LEADER");

    // Disable ALL instances
    for (String instance : liveInstances) {
      admin.enableInstance(_clusterName, instance, false);
    }

    event.addAttribute(AttributeName.helixmanager.name(), manager);
    event.addAttribute(AttributeName.RESOURCES.name(), resourceMap);
    event.addAttribute(AttributeName.RESOURCES_TO_REBALANCE.name(), resourceMap);
    event.addAttribute(AttributeName.CURRENT_STATE.name(), currentStateOutput);
    event.addAttribute(AttributeName.CURRENT_STATE_EXCLUDING_UNKNOWN.name(), currentStateOutput);
    event.addAttribute(AttributeName.ControllerDataProvider.name(),
        new ResourceControllerDataProvider());

    runStage(event, new ReadClusterDataStage());
    runStage(event, new BestPossibleStateCalcStage());

    BestPossibleStateOutput output = event.getAttribute(AttributeName.BEST_POSSIBLE_STATE.name());

    // Pipeline should continue and compute DROPPED for the instance that had LEADER
    Assert.assertTrue(output.containsResource("resource_1"),
        "Resource should be in output when all nodes disabled but replicas exist");
    Assert.assertEquals(
        output.getInstanceStateMap("resource_1", partition).get("localhost_2"),
        HelixDefinedState.DROPPED.name(),
        "Instance with LEADER should transition to DROPPED when all nodes are disabled");
  }

  /**
   * Tests that when all instances are disabled AND no current state exists (resource not
   * initialized), the pipeline correctly rejects and does not add the resource to output.
   */
  @Test
  public void testAllNodesDisabledRejectsWhenNoCurrentState() {
    String[] resources = new String[]{"resource_1"};
    int numInstances = 3;
    int numPartitions = 1;

    setupIdealState(numInstances, resources, numPartitions, 1, RebalanceMode.FULL_AUTO,
        BuiltInStateModelDefinitions.LeaderStandby.name(),
        DelayedAutoRebalancer.class.getName());
    setupInstances(numInstances);
    List<String> liveInstances = setupLiveInstances(numInstances);
    setupStateModel();

    ClusterConfig clusterConfig = accessor.getProperty(accessor.keyBuilder().clusterConfig());
    clusterConfig.setRebalanceDelayTime(0);
    clusterConfig.setDelayRebalaceEnabled(true);
    setClusterConfig(clusterConfig);

    Map<String, Resource> resourceMap =
        getResourceMap(resources, numPartitions, BuiltInStateModelDefinitions.LeaderStandby.name());
    // No current state - resource not initialized
    CurrentStateOutput currentStateOutput = new CurrentStateOutput();

    // Disable ALL instances
    for (String instance : liveInstances) {
      admin.enableInstance(_clusterName, instance, false);
    }

    event.addAttribute(AttributeName.helixmanager.name(), manager);
    event.addAttribute(AttributeName.RESOURCES.name(), resourceMap);
    event.addAttribute(AttributeName.RESOURCES_TO_REBALANCE.name(), resourceMap);
    event.addAttribute(AttributeName.CURRENT_STATE.name(), currentStateOutput);
    event.addAttribute(AttributeName.CURRENT_STATE_EXCLUDING_UNKNOWN.name(), currentStateOutput);
    event.addAttribute(AttributeName.ControllerDataProvider.name(),
        new ResourceControllerDataProvider());

    runStage(event, new ReadClusterDataStage());
    runStage(event, new BestPossibleStateCalcStage());

    BestPossibleStateOutput output = event.getAttribute(AttributeName.BEST_POSSIBLE_STATE.name());

    // Resource should NOT be in output when no current state exists (not initialized)
    Assert.assertFalse(output.containsResource("resource_1"),
        "Resource should not be in output when all nodes disabled and no replicas exist");
  }

  /**
   * Tests that SEMI_AUTO mode is unaffected - empty preference lists are allowed
   * (rebalancing is not controlled by Helix) and the pipeline continues.
   */
  @Test
  public void testSemiAutoModeUnaffectedByEmptyPreferenceList() {
    String[] resources = new String[]{"resource_1"};
    int numInstances = 3;
    int numPartitions = 1;

    setupIdealState(numInstances, resources, numPartitions, 1, RebalanceMode.SEMI_AUTO,
        BuiltInStateModelDefinitions.MasterSlave.name());
    setupInstances(numInstances);
    setupLiveInstances(numInstances);
    setupStateModel();

    Map<String, Resource> resourceMap =
        getResourceMap(resources, numPartitions, BuiltInStateModelDefinitions.MasterSlave.name());
    CurrentStateOutput currentStateOutput = new CurrentStateOutput();

    event.addAttribute(AttributeName.RESOURCES.name(), resourceMap);
    event.addAttribute(AttributeName.RESOURCES_TO_REBALANCE.name(), resourceMap);
    event.addAttribute(AttributeName.CURRENT_STATE.name(), currentStateOutput);
    event.addAttribute(AttributeName.CURRENT_STATE_EXCLUDING_UNKNOWN.name(), currentStateOutput);
    event.addAttribute(AttributeName.ControllerDataProvider.name(),
        new ResourceControllerDataProvider());

    runStage(event, new ReadClusterDataStage());
    runStage(event, new BestPossibleStateCalcStage());

    BestPossibleStateOutput output = event.getAttribute(AttributeName.BEST_POSSIBLE_STATE.name());
    // SEMI_AUTO uses preference list from IdealState; pipeline should complete
    Assert.assertNotNull(output, "Output should not be null");
  }

  /**
   * Tests that when the rebalancer silently returns empty preference lists while enabled live
   * instances still exist, the pipeline blocks the rebalance entirely to protect existing replicas.
   *
   * <p>This distinguishes a rebalancer failure from the legitimate "all nodes disabled" scenario:
   * <ul>
   *   <li>All nodes disabled: {@code getEnabledLiveInstances()} is empty → allow cleanup</li>
   *   <li>Rebalancer failure: {@code getEnabledLiveInstances()} is non-empty but rebalancer
   *       returned empty lists → block to prevent accidentally dropping all replicas</li>
   * </ul>
   *
   * <p>The failure is simulated by setting an instance group tag on the resource that no live
   * instance has. The DelayedAutoRebalancer finds no eligible instances for the tag and returns
   * an empty assignment, but {@code getEnabledLiveInstances()} still returns all untagged instances.
   */
  @Test
  public void testSilentRebalancerFailureDoesNotDropExistingReplicas() {
    String resourceName = "resource_1";
    String[] resources = new String[]{resourceName};
    int numInstances = 3;
    int numPartitions = 1;

    List<IdealState> idealStates = setupIdealState(numInstances, resources, numPartitions, 1,
        RebalanceMode.FULL_AUTO, BuiltInStateModelDefinitions.LeaderStandby.name(),
        DelayedAutoRebalancer.class.getName());
    setupInstances(numInstances);
    setupLiveInstances(numInstances);  // all instances are live and enabled (no tag)
    setupStateModel();

    // Set an instance group tag that NO instance has. This causes DelayedAutoRebalancer to find
    // zero eligible instances and silently return empty preference lists — while
    // getEnabledLiveInstances() still returns the full set of untagged live instances.
    // This simulates a silent rebalancer failure (e.g., tag misconfiguration or a rebalancer bug
    // that returns empty results without throwing).
    IdealState idealState = idealStates.get(0);
    idealState.setInstanceGroupTag("ghost-tag-no-instance-has-this");
    accessor.setProperty(accessor.keyBuilder().idealStates(resourceName), idealState);

    ClusterConfig clusterConfig = accessor.getProperty(accessor.keyBuilder().clusterConfig());
    clusterConfig.setRebalanceDelayTime(0);
    clusterConfig.setDelayRebalaceEnabled(true);
    setClusterConfig(clusterConfig);

    Map<String, Resource> resourceMap =
        getResourceMap(resources, numPartitions, BuiltInStateModelDefinitions.LeaderStandby.name());
    CurrentStateOutput currentStateOutput = new CurrentStateOutput();

    // Existing replica: localhost_2 holds LEADER — it must not be dropped due to a rebalancer bug.
    Partition partition = new Partition(resourceName + "_0");
    currentStateOutput.setCurrentState(resourceName, partition, "localhost_2", "LEADER");

    event.addAttribute(AttributeName.helixmanager.name(), manager);
    event.addAttribute(AttributeName.RESOURCES.name(), resourceMap);
    event.addAttribute(AttributeName.RESOURCES_TO_REBALANCE.name(), resourceMap);
    event.addAttribute(AttributeName.CURRENT_STATE.name(), currentStateOutput);
    event.addAttribute(AttributeName.CURRENT_STATE_EXCLUDING_UNKNOWN.name(), currentStateOutput);
    event.addAttribute(AttributeName.ControllerDataProvider.name(),
        new ResourceControllerDataProvider());

    runStage(event, new ReadClusterDataStage());
    runStage(event, new BestPossibleStateCalcStage());

    BestPossibleStateOutput output = event.getAttribute(AttributeName.BEST_POSSIBLE_STATE.name());

    // The pipeline must block the rebalance. If it proceeded with empty preference lists while
    // enabled live instances exist, the downstream mapping calculator would assign all replicas
    // to DROPPED/OFFLINE, causing catastrophic data loss.
    Assert.assertFalse(output.containsResource(resourceName),
        "Resource should NOT be in output when enabled live instances exist but the rebalancer "
            + "returned empty preference lists — this indicates a silent rebalancer failure, not "
            + "an all-nodes-disabled scenario.");
  }

  /**
   * Test parallel computation with multiple FULL_AUTO resources.
   * Verifies that parallel computation using StageThreadPoolHelper produces correct results.
   */
  @Test
  public void testBestPossibleComputationWithMultipleResources() {
    System.out.println("START testParallelComputationWithMultipleResources at "
        + new Date(System.currentTimeMillis()));

    int numInstances = 5;
    int numPartitions = 5;

    // Create multiple SEMI_AUTO resources - these will be computed in parallel
    String[] resources = new String[]{"res1", "res2", "res3", "res4"};
    setupIdealState(numInstances, resources, numPartitions, 1, RebalanceMode.FULL_AUTO,
        BuiltInStateModelDefinitions.MasterSlave.name());

    setupLiveInstances(numInstances);
    setupStateModel();
    setupInstances(numInstances);

    Map<String, Resource> resourceMap =
        getResourceMap(resources, numPartitions, BuiltInStateModelDefinitions.MasterSlave.name());

    CurrentStateOutput currentStateOutput = new CurrentStateOutput();
    event.addAttribute(AttributeName.RESOURCES.name(), resourceMap);
    event.addAttribute(AttributeName.RESOURCES_TO_REBALANCE.name(), resourceMap);
    event.addAttribute(AttributeName.CURRENT_STATE.name(), currentStateOutput);
    event.addAttribute(AttributeName.CURRENT_STATE_EXCLUDING_UNKNOWN.name(), currentStateOutput);
    event.addAttribute(AttributeName.ControllerDataProvider.name(),
        new ResourceControllerDataProvider());

    ReadClusterDataStage stage1 = new ReadClusterDataStage();
    runStage(event, stage1);
    BestPossibleStateCalcStage stage2 = new BestPossibleStateCalcStage();
    runStage(event, stage2);

    BestPossibleStateOutput output =
        event.getAttribute(AttributeName.BEST_POSSIBLE_STATE.name());

    // Verify all resources were computed successfully via parallel execution
    Assert.assertNotNull(output, "BestPossibleStateOutput should not be null");

    // Verify all resources have correct assignments
    for (String resourceName : resources) {
      Assert.assertTrue(output.containsResource(resourceName),
          "Output should contain resource: " + resourceName);
      for (int p = 0; p < numPartitions; p++) {
        Partition partition = new Partition(resourceName + "_" + p);
        Map<String, String> stateMap = output.getInstanceStateMap(resourceName, partition);
        Assert.assertNotNull(stateMap,
            "State map should not be null for " + resourceName + " partition " + p);
        // Each partition should have exactly one MASTER
        long masterCount = stateMap.values().stream().filter(state -> "MASTER".equals(state)).count();
        Assert.assertEquals(masterCount, 1,
            "Each partition should have exactly one MASTER for " + resourceName + "_" + p);
      }
    }
  }

  /**
   * Determinism regression guard for the global per-instance-partition-limit (greedy) path.
   *
   * <p>When {@code globalMaxPartitionAllowedPerInstance} is set, all {@link GreedyRebalanceStrategy}
   * resources share a single mutable {@link org.apache.helix.controller.common.CapacityNode} set, so
   * {@link BestPossibleStateCalcStage} computes them sequentially in a deterministic (sorted) order.
   * If that computation is ever parallelized again, threads reserve capacity in a non-deterministic
   * order and the assignment differs from round to round, causing perpetual rebalance churn (and
   * previously a ConcurrentModificationException).
   *
   * <p>This runs the stage over several pipeline rounds (reusing one data provider, exactly like the
   * real controller reuses its cache) and asserts the greedy assignment is byte-for-byte identical
   * across every round, that every partition is fully placed, and that no node exceeds the cap. It
   * fails the instant someone reintroduces parallel computation of the shared-scoreboard resources.
   */
  @Test
  public void testGreedyGlobalCapacityAssignmentIsDeterministicAcrossRounds() {
    final int numInstances = 6;
    final int numPartitions = 5;
    final int numReplicas = 1;
    final int globalMaxPartitionPerInstance = 2;
    final int numRounds = 5;
    // Two resources that both use the greedy strategy so they share the global CapacityNode set.
    String[] resources = new String[]{"greedyDB1", "greedyDB2"};

    setupIdealState(numInstances, resources, numPartitions, numReplicas, RebalanceMode.FULL_AUTO,
        BuiltInStateModelDefinitions.OnlineOffline.name(), null,
        GreedyRebalanceStrategy.class.getName(), -1 /* minActiveReplica not set */);
    setupInstances(numInstances);
    setupLiveInstances(numInstances);
    setupStateModel();

    // Activate the global per-instance partition limit (the shared-scoreboard path).
    ClusterConfig clusterConfig = accessor.getProperty(accessor.keyBuilder().clusterConfig());
    clusterConfig.setGlobalMaxPartitionAllowedPerInstance(globalMaxPartitionPerInstance);
    setClusterConfig(clusterConfig);

    Map<String, Resource> resourceMap =
        getResourceMap(resources, numPartitions, BuiltInStateModelDefinitions.OnlineOffline.name());
    CurrentStateOutput currentStateOutput = new CurrentStateOutput();

    // Reuse a single data provider across rounds, exactly like the real controller reuses its cache.
    ResourceControllerDataProvider cache = new ResourceControllerDataProvider();
    event.addAttribute(AttributeName.helixmanager.name(), manager);
    event.addAttribute(AttributeName.RESOURCES.name(), resourceMap);
    event.addAttribute(AttributeName.RESOURCES_TO_REBALANCE.name(), resourceMap);
    event.addAttribute(AttributeName.CURRENT_STATE.name(), currentStateOutput);
    event.addAttribute(AttributeName.CURRENT_STATE_EXCLUDING_UNKNOWN.name(), currentStateOutput);
    event.addAttribute(AttributeName.ControllerDataProvider.name(), cache);

    String firstRoundAssignment = null;
    for (int round = 0; round < numRounds; round++) {
      runStage(event, new ReadClusterDataStage());
      runStage(event, new BestPossibleStateCalcStage());

      BestPossibleStateOutput output = event.getAttribute(AttributeName.BEST_POSSIBLE_STATE.name());
      Assert.assertNotNull(output, "BestPossibleStateOutput should not be null in round " + round);

      // Every partition must be fully placed (so we compare real assignments, not coincidentally
      // equal empty maps) and no node may exceed the global cap.
      Map<String, Integer> perInstanceCount = new HashMap<>();
      for (String resource : resources) {
        for (int p = 0; p < numPartitions; p++) {
          Partition partition = new Partition(resource + "_" + p);
          Map<String, String> stateMap = output.getInstanceStateMap(resource, partition);
          Assert.assertNotNull(stateMap,
              "State map should not be null for " + partition.getPartitionName() + " in round "
                  + round);
          Assert.assertEquals(stateMap.size(), numReplicas,
              "Greedy should place exactly " + numReplicas + " replica(s) for "
                  + partition.getPartitionName() + " in round " + round);
          for (String instance : stateMap.keySet()) {
            perInstanceCount.merge(instance, 1, Integer::sum);
          }
        }
      }
      for (Map.Entry<String, Integer> entry : perInstanceCount.entrySet()) {
        Assert.assertTrue(entry.getValue() <= globalMaxPartitionPerInstance,
            "Instance " + entry.getKey() + " holds " + entry.getValue()
                + " partitions, exceeding the global cap of " + globalMaxPartitionPerInstance
                + " in round " + round);
      }

      String assignment = canonicalizeAssignment(output, resources, numPartitions);
      if (round == 0) {
        firstRoundAssignment = assignment;
      } else {
        Assert.assertEquals(assignment, firstRoundAssignment,
            "Greedy global-capacity assignment must be identical across pipeline rounds; round "
                + round + " differs from round 0. A non-deterministic result indicates the "
                + "shared-scoreboard (greedy) resources are being computed in parallel again.");
      }
    }
  }

  /**
   * Builds a stable, iteration-order-independent string representation of the greedy resources'
   * assignment so two pipeline rounds can be compared byte-for-byte.
   */
  private String canonicalizeAssignment(BestPossibleStateOutput output, String[] resources,
      int numPartitions) {
    String[] sortedResources = resources.clone();
    Arrays.sort(sortedResources);
    StringBuilder sb = new StringBuilder();
    for (String resource : sortedResources) {
      sb.append(resource).append('{');
      for (int p = 0; p < numPartitions; p++) {
        Partition partition = new Partition(resource + "_" + p);
        Map<String, String> stateMap = output.getInstanceStateMap(resource, partition);
        sb.append(partition.getPartitionName()).append('=')
            .append(new TreeMap<>(stateMap == null ? new HashMap<>() : stateMap)).append(';');
      }
      sb.append('}');
    }
    return sb.toString();
  }
}
