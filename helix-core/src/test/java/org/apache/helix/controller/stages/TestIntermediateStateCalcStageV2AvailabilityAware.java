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

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.google.common.collect.ImmutableList;
import org.apache.helix.api.config.StateTransitionThrottleConfig;
import org.apache.helix.controller.common.PartitionStateMap;
import org.apache.helix.controller.dataproviders.ResourceControllerDataProvider;
import org.apache.helix.model.ClusterConfig;
import org.apache.helix.model.IdealState;
import org.apache.helix.model.Message;
import org.apache.helix.model.Partition;
import org.testng.Assert;
import org.testng.annotations.Test;


/**
 * Runs all V1 IntermediateStateCalcStage test scenarios with V2 + availability-aware
 * prioritization enabled. Inherits the 8 V1 tests from the grandparent class via
 * {@link TestIntermediateStateCalcStageV2}; the override chain injects both the V2 flag
 * and the availability-aware flag before writing to ZK.
 *
 * Additionally defines new tests that verify availability-aware-specific ordering behavior
 * under tight throttle budgets — scenarios where the ordering strategy determines which
 * resource's transitions are approved vs throttled.
 */
public class TestIntermediateStateCalcStageV2AvailabilityAware
    extends TestIntermediateStateCalcStageV2 {

  @Override
  protected void setClusterConfig(ClusterConfig clusterConfig) {
    clusterConfig.setAvailabilityAwarePrioritizationEnabled(true);
    super.setClusterConfig(clusterConfig);
  }

  // V1→V2 routing is already covered at the V2 level.
  // Since this subclass always enables both V2 and availability-aware, skip this structural test.
  @Override
  @Test(enabled = false)
  public void testDelegatesToV2WhenEnabled() {
  }

  private String getPartitionState(IntermediateStateOutput output, String resource,
      Partition partition, String instance) {
    PartitionStateMap psm = output.getPartitionStateMap(resource);
    Assert.assertNotNull(psm, "No output produced for resource: " + resource);
    Map<String, String> stateMap = psm.getStateMap().get(partition);
    Assert.assertNotNull(stateMap,
        "No state map for partition " + partition.getPartitionName() + " in resource: " + resource);
    return stateMap.get(instance);
  }

  /**
   * Generate a message with resource and partition names set. The AvailabilityAwareOrderingStrategy
   * reads message.getResourceName() to look up IdealState for scoring, so these must be set.
   */
  private Message generateMessageWithContext(String resourceName, String partitionName,
      String fromState, String toState, String tgtName) {
    Message message = generateMessage(fromState, toState, tgtName);
    message.setResourceName(resourceName);
    message.setPartitionName(partitionName);
    return message;
  }

  /**
   * Verifies that availability-aware ordering prioritizes fully-degraded resources over
   * partially-degraded ones under a tight cluster-level recovery throttle.
   *
   * Setup:
   * - 3 resources (resourceA, resourceB, resourceC), 1 partition each, OnlineOffline
   * - Cluster RECOVERY throttle = 2 (only 2 of 3 recovery messages can proceed)
   * - resourceA: 2/3 replicas ONLINE (partially degraded, NOT missing top state)
   * - resourceB: 0/3 replicas ONLINE (fully degraded, top state missing)
   * - resourceC: 0/3 replicas ONLINE (fully degraded, top state missing)
   *
   * Expected: resourceB and resourceC get the 2 recovery slots (availability score 1,000,000
   * each — top state missing). resourceA (score ~1.0) is throttled.
   *
   * Under ResourcePriority ordering (alphabetical tiebreak), resourceA would consume a slot
   * first, leaving only 1 slot for B or C.
   */
  @Test
  public void testAvailabilityAwarePrioritizesMostDegradedResource() {
    String resourceA = "resourceA";
    String resourceB = "resourceB";
    String resourceC = "resourceC";
    String[] resources = new String[]{resourceA, resourceB, resourceC};
    int nPartition = 1;
    int nInstances = 3;
    int nReplica = 3;

    setupIdealState(nInstances, resources, nPartition, nReplica,
        IdealState.RebalanceMode.FULL_AUTO, "OnlineOffline");
    setupStateModel();
    setupInstances(nInstances);
    setupLiveInstances(nInstances);

    // Cluster-level recovery throttle = 2: only 2 recovery transitions per pipeline run.
    ClusterConfig clusterConfig = accessor.getProperty(accessor.keyBuilder().clusterConfig());
    clusterConfig.setStateTransitionThrottleConfigs(ImmutableList.of(
        new StateTransitionThrottleConfig(
            StateTransitionThrottleConfig.RebalanceType.RECOVERY_BALANCE,
            StateTransitionThrottleConfig.ThrottleScope.CLUSTER, 2),
        new StateTransitionThrottleConfig(
            StateTransitionThrottleConfig.RebalanceType.LOAD_BALANCE,
            StateTransitionThrottleConfig.ThrottleScope.CLUSTER, 10)));
    setClusterConfig(clusterConfig);

    event.addAttribute(AttributeName.RESOURCES.name(),
        getResourceMap(resources, nPartition, "OnlineOffline"));
    event.addAttribute(AttributeName.RESOURCES_TO_REBALANCE.name(),
        getResourceMap(resources, nPartition, "OnlineOffline"));

    BestPossibleStateOutput bestPossibleStateOutput = new BestPossibleStateOutput();
    CurrentStateOutput currentStateOutput = new CurrentStateOutput();
    MessageOutput messageSelectOutput = new MessageOutput();

    String instance0 = HOSTNAME_PREFIX + 0;
    String instance1 = HOSTNAME_PREFIX + 1;
    String instance2 = HOSTNAME_PREFIX + 2;

    // resourceA: partition_0 has 2/3 replicas ONLINE (partially degraded).
    // Recovery message: OFFLINE→ONLINE on instance_2.
    // Availability score: NOT missing top state (ONLINE exists), upward score ≈ low.
    Partition partA = new Partition(resourceA + "_0");
    Map<String, List<String>> prefA = new HashMap<>();
    prefA.put(partA.getPartitionName(), ImmutableList.of(instance0, instance1, instance2));
    currentStateOutput.setCurrentState(resourceA, partA, instance0, "ONLINE");
    currentStateOutput.setCurrentState(resourceA, partA, instance1, "ONLINE");
    currentStateOutput.setCurrentState(resourceA, partA, instance2, "OFFLINE");
    bestPossibleStateOutput.setState(resourceA, partA, instance0, "ONLINE");
    bestPossibleStateOutput.setState(resourceA, partA, instance1, "ONLINE");
    bestPossibleStateOutput.setState(resourceA, partA, instance2, "ONLINE");
    bestPossibleStateOutput.setPreferenceLists(resourceA, prefA);
    messageSelectOutput.addMessage(resourceA, partA,
        generateMessageWithContext(resourceA, partA.getPartitionName(),
            "OFFLINE", "ONLINE", instance2));

    // resourceB: partition_0 has 0/3 replicas ONLINE (fully degraded, top state missing).
    // Recovery message: OFFLINE→ONLINE on instance_0.
    // Availability score: 1,000,000 (top state missing + transitioning to top state).
    Partition partB = new Partition(resourceB + "_0");
    Map<String, List<String>> prefB = new HashMap<>();
    prefB.put(partB.getPartitionName(), ImmutableList.of(instance0, instance1, instance2));
    currentStateOutput.setCurrentState(resourceB, partB, instance0, "OFFLINE");
    currentStateOutput.setCurrentState(resourceB, partB, instance1, "OFFLINE");
    currentStateOutput.setCurrentState(resourceB, partB, instance2, "OFFLINE");
    bestPossibleStateOutput.setState(resourceB, partB, instance0, "ONLINE");
    bestPossibleStateOutput.setState(resourceB, partB, instance1, "ONLINE");
    bestPossibleStateOutput.setState(resourceB, partB, instance2, "ONLINE");
    bestPossibleStateOutput.setPreferenceLists(resourceB, prefB);
    messageSelectOutput.addMessage(resourceB, partB,
        generateMessageWithContext(resourceB, partB.getPartitionName(),
            "OFFLINE", "ONLINE", instance0));

    // resourceC: partition_0 has 0/3 replicas ONLINE (fully degraded, top state missing).
    // Recovery message: OFFLINE→ONLINE on instance_1.
    // Availability score: 1,000,000 (top state missing + transitioning to top state).
    Partition partC = new Partition(resourceC + "_0");
    Map<String, List<String>> prefC = new HashMap<>();
    prefC.put(partC.getPartitionName(), ImmutableList.of(instance0, instance1, instance2));
    currentStateOutput.setCurrentState(resourceC, partC, instance0, "OFFLINE");
    currentStateOutput.setCurrentState(resourceC, partC, instance1, "OFFLINE");
    currentStateOutput.setCurrentState(resourceC, partC, instance2, "OFFLINE");
    bestPossibleStateOutput.setState(resourceC, partC, instance0, "ONLINE");
    bestPossibleStateOutput.setState(resourceC, partC, instance1, "ONLINE");
    bestPossibleStateOutput.setState(resourceC, partC, instance2, "ONLINE");
    bestPossibleStateOutput.setPreferenceLists(resourceC, prefC);
    messageSelectOutput.addMessage(resourceC, partC,
        generateMessageWithContext(resourceC, partC.getPartitionName(),
            "OFFLINE", "ONLINE", instance1));

    event.addAttribute(AttributeName.BEST_POSSIBLE_STATE.name(), bestPossibleStateOutput);
    event.addAttribute(AttributeName.MESSAGES_SELECTED.name(), messageSelectOutput);
    event.addAttribute(AttributeName.CURRENT_STATE.name(), currentStateOutput);
    event.addAttribute(AttributeName.CURRENT_STATE_EXCLUDING_UNKNOWN.name(), currentStateOutput);
    event.addAttribute(AttributeName.ControllerDataProvider.name(),
        new ResourceControllerDataProvider());
    runStage(event, new ReadClusterDataStage());
    runStage(event, new IntermediateStateCalcStage());

    IntermediateStateOutput output = event.getAttribute(AttributeName.INTERMEDIATE_STATE.name());

    // resourceB: recovery approved (top state missing, score 1,000,000)
    Assert.assertEquals(getPartitionState(output, resourceB, partB, instance0),
        "ONLINE",
        "resourceB (fully degraded) should have its recovery approved");

    // resourceC: recovery approved (top state missing, score 1,000,000)
    Assert.assertEquals(getPartitionState(output, resourceC, partC, instance1),
        "ONLINE",
        "resourceC (fully degraded) should have its recovery approved");

    // resourceA: recovery throttled (partially degraded, low score)
    Assert.assertEquals(getPartitionState(output, resourceA, partA, instance2),
        "OFFLINE",
        "resourceA (partially degraded) should be throttled when cluster recovery quota is exhausted");
  }

  /**
   * Verifies that availability-aware ordering prioritizes top-state handoff transitions
   * (e.g., MASTER→SLAVE during leadership migration) above normal upward transitions,
   * ensuring in-progress leadership handoffs complete before new replicas are brought up.
   *
   * Setup:
   * - 2 resources, 1 partition each, MasterSlave model
   * - Both messages target instance_0, instance ANY throttle = 1
   * - resourceA: needs OFFLINE→SLAVE on instance_0 (upward, low score)
   * - resourceB: handoff MASTER→SLAVE on instance_0 (score 999,999)
   *
   * Expected: resourceB handoff gets the single slot.
   * resourceA upward transition is throttled.
   */
  @Test
  public void testAvailabilityAwareHandoffPrioritizedOverUpwardTransition() {
    String resourceA = "resourceA";
    String resourceB = "resourceB";
    String[] resources = new String[]{resourceA, resourceB};
    int nPartition = 1;
    int nInstances = 3;
    int nReplica = 3;

    setupIdealState(nInstances, resources, nPartition, nReplica,
        IdealState.RebalanceMode.FULL_AUTO, "MasterSlave");
    setupStateModel();
    setupInstances(nInstances);
    setupLiveInstances(nInstances);

    // Instance-level ANY throttle = 1: only 1 transition of any type per instance.
    // Both messages target instance_0, so they compete for the same single slot.
    ClusterConfig clusterConfig = accessor.getProperty(accessor.keyBuilder().clusterConfig());
    clusterConfig.setStateTransitionThrottleConfigs(ImmutableList.of(
        new StateTransitionThrottleConfig(
            StateTransitionThrottleConfig.RebalanceType.ANY,
            StateTransitionThrottleConfig.ThrottleScope.INSTANCE, 1)));
    setClusterConfig(clusterConfig);

    event.addAttribute(AttributeName.RESOURCES.name(),
        getResourceMap(resources, nPartition, "MasterSlave"));
    event.addAttribute(AttributeName.RESOURCES_TO_REBALANCE.name(),
        getResourceMap(resources, nPartition, "MasterSlave"));

    BestPossibleStateOutput bestPossibleStateOutput = new BestPossibleStateOutput();
    CurrentStateOutput currentStateOutput = new CurrentStateOutput();
    MessageOutput messageSelectOutput = new MessageOutput();

    String instance0 = HOSTNAME_PREFIX + 0;
    String instance1 = HOSTNAME_PREFIX + 1;
    String instance2 = HOSTNAME_PREFIX + 2;

    // resourceA: partition_0 has Master on instance_1 and Slave on instance_2.
    // Upward message: OFFLINE→SLAVE on instance_0 to add a replica.
    // Availability score: small positive (upward, partition has top state already).
    Partition partA = new Partition(resourceA + "_0");
    Map<String, List<String>> prefA = new HashMap<>();
    prefA.put(partA.getPartitionName(), ImmutableList.of(instance0, instance1, instance2));
    currentStateOutput.setCurrentState(resourceA, partA, instance0, "OFFLINE");
    currentStateOutput.setCurrentState(resourceA, partA, instance1, "MASTER");
    currentStateOutput.setCurrentState(resourceA, partA, instance2, "SLAVE");
    bestPossibleStateOutput.setState(resourceA, partA, instance0, "SLAVE");
    bestPossibleStateOutput.setState(resourceA, partA, instance1, "MASTER");
    bestPossibleStateOutput.setState(resourceA, partA, instance2, "SLAVE");
    bestPossibleStateOutput.setPreferenceLists(resourceA, prefA);
    messageSelectOutput.addMessage(resourceA, partA,
        generateMessageWithContext(resourceA, partA.getPartitionName(),
            "OFFLINE", "SLAVE", instance0));

    // resourceB: partition_0 has a leadership handoff in progress.
    // Master on instance_0 is stepping down: MASTER→SLAVE on instance_0.
    // Availability score: 999,999 (top-state handoff).
    Partition partB = new Partition(resourceB + "_0");
    Map<String, List<String>> prefB = new HashMap<>();
    prefB.put(partB.getPartitionName(), ImmutableList.of(instance1, instance0, instance2));
    currentStateOutput.setCurrentState(resourceB, partB, instance0, "MASTER");
    currentStateOutput.setCurrentState(resourceB, partB, instance1, "SLAVE");
    currentStateOutput.setCurrentState(resourceB, partB, instance2, "OFFLINE");
    bestPossibleStateOutput.setState(resourceB, partB, instance0, "SLAVE");
    bestPossibleStateOutput.setState(resourceB, partB, instance1, "MASTER");
    bestPossibleStateOutput.setState(resourceB, partB, instance2, "SLAVE");
    bestPossibleStateOutput.setPreferenceLists(resourceB, prefB);
    messageSelectOutput.addMessage(resourceB, partB,
        generateMessageWithContext(resourceB, partB.getPartitionName(),
            "MASTER", "SLAVE", instance0));

    event.addAttribute(AttributeName.BEST_POSSIBLE_STATE.name(), bestPossibleStateOutput);
    event.addAttribute(AttributeName.MESSAGES_SELECTED.name(), messageSelectOutput);
    event.addAttribute(AttributeName.CURRENT_STATE.name(), currentStateOutput);
    event.addAttribute(AttributeName.CURRENT_STATE_EXCLUDING_UNKNOWN.name(), currentStateOutput);
    event.addAttribute(AttributeName.ControllerDataProvider.name(),
        new ResourceControllerDataProvider());
    runStage(event, new ReadClusterDataStage());
    runStage(event, new IntermediateStateCalcStage());

    IntermediateStateOutput output = event.getAttribute(AttributeName.INTERMEDIATE_STATE.name());

    // resourceB: handoff approved (score 999,999 — top-state handoff)
    Assert.assertEquals(getPartitionState(output, resourceB, partB, instance0),
        "SLAVE",
        "resourceB handoff (MASTER→SLAVE) should be approved — it scores 999,999");

    // resourceA: upward transition throttled (low score)
    Assert.assertEquals(getPartitionState(output, resourceA, partA, instance0),
        "OFFLINE",
        "resourceA upward transition should be throttled when instance ANY quota is exhausted by handoff");
  }

  /**
   * Verifies that availability-aware ordering uses the upward score gradient to prioritize
   * more-degraded resources over less-degraded ones for load-balance type messages.
   *
   * All resources have at least one active replica (top state is present), so messages are
   * classified as LOAD_BALANCE — not RECOVERY. The scoring formula
   * {@code minActive / (currentActive + 1)} produces different scores based on how many
   * active replicas each resource currently has.
   *
   * Setup:
   * - 3 resources (resourceA, resourceB, resourceC), 1 partition each, OnlineOffline
   * - 4 instances, 4 replicas, minActiveReplica = 1
   * - Cluster LOAD_BALANCE throttle = 2 (only 2 of 3 load-balance messages can proceed)
   * - resourceA: 3/4 replicas ONLINE → score 1/(3+1) = 0.25 (least degraded)
   * - resourceB: 2/4 replicas ONLINE → score 1/(2+1) ≈ 0.33 (moderately degraded)
   * - resourceC: 1/4 replicas ONLINE → score 1/(1+1) = 0.50 (most degraded)
   *
   * Expected: resourceC and resourceB get the 2 load-balance slots (scores 0.50 and 0.33).
   * resourceA (score 0.25, least degraded) is throttled.
   *
   * Under ResourcePriority ordering (alphabetical tiebreak), resourceA would consume a slot
   * first, leaving only 1 slot for B or C.
   */
  @Test
  public void testAvailabilityAwareOrdersByDegradationForLoadBalance() {
    String resourceA = "resourceA";
    String resourceB = "resourceB";
    String resourceC = "resourceC";
    String[] resources = new String[]{resourceA, resourceB, resourceC};
    int nPartition = 1;
    int nInstances = 4;
    int nReplica = 4;

    // Set minActiveReplica=1 so that resources with ≥1 ONLINE have their additional
    // OFFLINE→ONLINE transitions classified as LOAD_BALANCE (not RECOVERY).
    setupIdealState(nInstances, resources, nPartition, nReplica,
        IdealState.RebalanceMode.FULL_AUTO, "OnlineOffline", null, null, 1);
    setupStateModel();
    setupInstances(nInstances);
    setupLiveInstances(nInstances);

    // Cluster-level load balance throttle = 2: only 2 load-balance transitions per pipeline run.
    // Recovery budget is generous so it doesn't interfere.
    ClusterConfig clusterConfig = accessor.getProperty(accessor.keyBuilder().clusterConfig());
    clusterConfig.setStateTransitionThrottleConfigs(ImmutableList.of(
        new StateTransitionThrottleConfig(
            StateTransitionThrottleConfig.RebalanceType.RECOVERY_BALANCE,
            StateTransitionThrottleConfig.ThrottleScope.CLUSTER, 10),
        new StateTransitionThrottleConfig(
            StateTransitionThrottleConfig.RebalanceType.LOAD_BALANCE,
            StateTransitionThrottleConfig.ThrottleScope.CLUSTER, 2)));
    setClusterConfig(clusterConfig);

    event.addAttribute(AttributeName.RESOURCES.name(),
        getResourceMap(resources, nPartition, "OnlineOffline"));
    event.addAttribute(AttributeName.RESOURCES_TO_REBALANCE.name(),
        getResourceMap(resources, nPartition, "OnlineOffline"));

    BestPossibleStateOutput bestPossibleStateOutput = new BestPossibleStateOutput();
    CurrentStateOutput currentStateOutput = new CurrentStateOutput();
    MessageOutput messageSelectOutput = new MessageOutput();

    String instance0 = HOSTNAME_PREFIX + 0;
    String instance1 = HOSTNAME_PREFIX + 1;
    String instance2 = HOSTNAME_PREFIX + 2;
    String instance3 = HOSTNAME_PREFIX + 3;

    // resourceA: 3/4 ONLINE (least degraded).
    // Load-balance message: OFFLINE→ONLINE on instance_3.
    // Upward score: minActive / (currentActive + 1) = 1 / (3 + 1) = 0.25
    Partition partA = new Partition(resourceA + "_0");
    Map<String, List<String>> prefA = new HashMap<>();
    prefA.put(partA.getPartitionName(),
        ImmutableList.of(instance0, instance1, instance2, instance3));
    currentStateOutput.setCurrentState(resourceA, partA, instance0, "ONLINE");
    currentStateOutput.setCurrentState(resourceA, partA, instance1, "ONLINE");
    currentStateOutput.setCurrentState(resourceA, partA, instance2, "ONLINE");
    currentStateOutput.setCurrentState(resourceA, partA, instance3, "OFFLINE");
    bestPossibleStateOutput.setState(resourceA, partA, instance0, "ONLINE");
    bestPossibleStateOutput.setState(resourceA, partA, instance1, "ONLINE");
    bestPossibleStateOutput.setState(resourceA, partA, instance2, "ONLINE");
    bestPossibleStateOutput.setState(resourceA, partA, instance3, "ONLINE");
    bestPossibleStateOutput.setPreferenceLists(resourceA, prefA);
    messageSelectOutput.addMessage(resourceA, partA,
        generateMessageWithContext(resourceA, partA.getPartitionName(),
            "OFFLINE", "ONLINE", instance3));

    // resourceB: 2/4 ONLINE (moderately degraded).
    // Load-balance message: OFFLINE→ONLINE on instance_2.
    // Upward score: 1 / (2 + 1) ≈ 0.33
    Partition partB = new Partition(resourceB + "_0");
    Map<String, List<String>> prefB = new HashMap<>();
    prefB.put(partB.getPartitionName(),
        ImmutableList.of(instance0, instance1, instance2, instance3));
    currentStateOutput.setCurrentState(resourceB, partB, instance0, "ONLINE");
    currentStateOutput.setCurrentState(resourceB, partB, instance1, "ONLINE");
    currentStateOutput.setCurrentState(resourceB, partB, instance2, "OFFLINE");
    currentStateOutput.setCurrentState(resourceB, partB, instance3, "OFFLINE");
    bestPossibleStateOutput.setState(resourceB, partB, instance0, "ONLINE");
    bestPossibleStateOutput.setState(resourceB, partB, instance1, "ONLINE");
    bestPossibleStateOutput.setState(resourceB, partB, instance2, "ONLINE");
    bestPossibleStateOutput.setState(resourceB, partB, instance3, "ONLINE");
    bestPossibleStateOutput.setPreferenceLists(resourceB, prefB);
    messageSelectOutput.addMessage(resourceB, partB,
        generateMessageWithContext(resourceB, partB.getPartitionName(),
            "OFFLINE", "ONLINE", instance2));

    // resourceC: 1/4 ONLINE (most degraded).
    // Load-balance message: OFFLINE→ONLINE on instance_1.
    // Upward score: 1 / (1 + 1) = 0.50
    Partition partC = new Partition(resourceC + "_0");
    Map<String, List<String>> prefC = new HashMap<>();
    prefC.put(partC.getPartitionName(),
        ImmutableList.of(instance0, instance1, instance2, instance3));
    currentStateOutput.setCurrentState(resourceC, partC, instance0, "ONLINE");
    currentStateOutput.setCurrentState(resourceC, partC, instance1, "OFFLINE");
    currentStateOutput.setCurrentState(resourceC, partC, instance2, "OFFLINE");
    currentStateOutput.setCurrentState(resourceC, partC, instance3, "OFFLINE");
    bestPossibleStateOutput.setState(resourceC, partC, instance0, "ONLINE");
    bestPossibleStateOutput.setState(resourceC, partC, instance1, "ONLINE");
    bestPossibleStateOutput.setState(resourceC, partC, instance2, "ONLINE");
    bestPossibleStateOutput.setState(resourceC, partC, instance3, "ONLINE");
    bestPossibleStateOutput.setPreferenceLists(resourceC, prefC);
    messageSelectOutput.addMessage(resourceC, partC,
        generateMessageWithContext(resourceC, partC.getPartitionName(),
            "OFFLINE", "ONLINE", instance1));

    event.addAttribute(AttributeName.BEST_POSSIBLE_STATE.name(), bestPossibleStateOutput);
    event.addAttribute(AttributeName.MESSAGES_SELECTED.name(), messageSelectOutput);
    event.addAttribute(AttributeName.CURRENT_STATE.name(), currentStateOutput);
    event.addAttribute(AttributeName.CURRENT_STATE_EXCLUDING_UNKNOWN.name(), currentStateOutput);
    event.addAttribute(AttributeName.ControllerDataProvider.name(),
        new ResourceControllerDataProvider());
    runStage(event, new ReadClusterDataStage());
    runStage(event, new IntermediateStateCalcStage());

    IntermediateStateOutput output = event.getAttribute(AttributeName.INTERMEDIATE_STATE.name());

    // resourceC: load-balance approved (most degraded, score 0.50)
    Assert.assertEquals(getPartitionState(output, resourceC, partC, instance1),
        "ONLINE",
        "resourceC (most degraded, 1/4 active, score 0.50) should have its load-balance approved");

    // resourceB: load-balance approved (moderately degraded, score 0.33)
    Assert.assertEquals(getPartitionState(output, resourceB, partB, instance2),
        "ONLINE",
        "resourceB (moderately degraded, 2/4 active, score 0.33) should have its load-balance approved");

    // resourceA: load-balance throttled (least degraded, score 0.25)
    Assert.assertEquals(getPartitionState(output, resourceA, partA, instance3),
        "OFFLINE",
        "resourceA (least degraded, 3/4 active, score 0.25) should be throttled when cluster "
            + "load-balance quota is exhausted");
  }
}
