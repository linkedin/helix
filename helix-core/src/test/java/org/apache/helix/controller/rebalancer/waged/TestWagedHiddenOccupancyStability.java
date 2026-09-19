package org.apache.helix.controller.rebalancer.waged;

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

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import org.apache.helix.HelixConstants;
import org.apache.helix.HelixRebalanceException;
import org.apache.helix.controller.dataproviders.ResourceControllerDataProvider;
import org.apache.helix.controller.rebalancer.waged.constraints.ConstraintBasedAlgorithmFactory;
import org.apache.helix.controller.stages.CurrentStateOutput;
import org.apache.helix.model.BuiltInStateModelDefinitions;
import org.apache.helix.model.ClusterConfig;
import org.apache.helix.model.CurrentState;
import org.apache.helix.model.IdealState;
import org.apache.helix.model.InstanceConfig;
import org.apache.helix.model.LiveInstance;
import org.apache.helix.model.Partition;
import org.apache.helix.model.Resource;
import org.apache.helix.model.ResourceAssignment;
import org.apache.helix.model.ResourceConfig;
import org.apache.helix.controller.rebalancer.constraint.MonitoredAbnormalResolver;
import org.apache.helix.constants.InstanceConstants;
import org.mockito.Mockito;
import org.mockito.stubbing.Answer;
import org.testng.Assert;
import org.testng.annotations.Test;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Flip-flop tests for the hidden-occupancy eligibility gate.
 *
 * The integration churn harness measures movement through ZooKeeper and a live controller, so its
 * readings carry timing noise and a quiet run there is not proof of a quiet rebalancer. These tests
 * drive the real {@link ConstraintBasedAlgorithm} through {@link WagedRebalancer} against a mocked
 * data provider instead, so every round is deterministic and the exact placement of every partition
 * is comparable across rounds. That makes a single re-chosen instance visible, which is the
 * granularity the churn question actually needs.
 *
 * The shape under test is the production one: a replica physically resident on an instance that the
 * plan no longer places there, in a state the state model gives no positive count, so the capacity
 * ledger charges it while the planner cannot model it.
 */
public class TestWagedHiddenOccupancyStability {
  private static final String RESOURCE = "TestDB";
  private static final String GHOST_STATE = "OFFLINE";
  private static final String SERVING_STATE = "ONLINE";
  private static final String STATE_MODEL = "OnlineOffline";
  private static final String CAPACITY_KEY = "slot";
  private static final String SESSION = "testSession";
  private static final int NODE_COUNT = 4;
  private static final int SLOTS_PER_NODE = 2;
  private static final int PARTITION_COUNT = 5;

  private List<String> instances() {
    List<String> instances = new ArrayList<>();
    for (int i = 0; i < NODE_COUNT; i++) {
      instances.add("instance_" + i);
    }
    return instances;
  }

  private List<String> partitions() {
    List<String> partitions = new ArrayList<>();
    for (int i = 0; i < PARTITION_COUNT; i++) {
      partitions.add(RESOURCE + "_" + i);
    }
    return partitions;
  }

  /**
   * @param ghosts instance -> partitions physically resident there in an uncounted state.
   */
  private ResourceControllerDataProvider newCache(boolean featureEnabled,
      Map<String, Set<String>> ghosts) throws IOException {
    ResourceControllerDataProvider cache = mock(ResourceControllerDataProvider.class);
    List<String> instances = instances();

    ClusterConfig clusterConfig = new ClusterConfig("testCluster");
    clusterConfig.setInstanceCapacityKeys(Collections.singletonList(CAPACITY_KEY));
    clusterConfig.setDefaultInstanceCapacityMap(
        Collections.singletonMap(CAPACITY_KEY, SLOTS_PER_NODE));
    clusterConfig.setDefaultPartitionWeightMap(Collections.singletonMap(CAPACITY_KEY, 1));
    clusterConfig.setWagedCountUnallocatedOccupancyEnabled(featureEnabled);
    when(cache.getClusterConfig()).thenReturn(clusterConfig);
    when(cache.getAbnormalStateResolver(any()))
        .thenReturn(MonitoredAbnormalResolver.DUMMY_STATE_RESOLVER);

    Map<String, InstanceConfig> instanceConfigs = new LinkedHashMap<>();
    Map<String, LiveInstance> liveInstances = new LinkedHashMap<>();
    for (String instance : instances) {
      InstanceConfig instanceConfig = new InstanceConfig(instance);
      instanceConfig.setInstanceCapacityMap(
          Collections.singletonMap(CAPACITY_KEY, SLOTS_PER_NODE));
      instanceConfig.setInstanceOperation(InstanceConstants.InstanceOperation.ENABLE);
      instanceConfigs.put(instance, instanceConfig);

      LiveInstance liveInstance = new LiveInstance(instance);
      liveInstance.setSessionId(SESSION);
      liveInstances.put(instance, liveInstance);
    }
    when(cache.getAssignableInstanceConfigMap()).thenReturn(instanceConfigs);
    when(cache.getInstanceConfigMap()).thenReturn(instanceConfigs);
    when(cache.getAssignableLiveInstances()).thenReturn(liveInstances);
    when(cache.getLiveInstances()).thenReturn(liveInstances);
    Set<String> instanceSet = new HashSet<>(instances);
    when(cache.getEnabledInstances()).thenReturn(instanceSet);
    when(cache.getEnabledLiveInstances()).thenReturn(instanceSet);
    when(cache.getAssignableInstances()).thenReturn(instanceSet);
    when(cache.getAllInstances()).thenReturn(instanceSet);

    IdealState idealState = new IdealState(RESOURCE);
    idealState.setNumPartitions(PARTITION_COUNT);
    idealState.setRebalanceMode(IdealState.RebalanceMode.FULL_AUTO);
    idealState.setStateModelDefRef(STATE_MODEL);
    idealState.setReplicas("1");
    idealState.setRebalancerClassName(WagedRebalancer.class.getName());
    partitions().forEach(p -> idealState.setPreferenceList(p, Collections.emptyList()));
    Map<String, IdealState> idealStates = Collections.singletonMap(RESOURCE, idealState);
    when(cache.getIdealState(anyString()))
        .thenAnswer((Answer<IdealState>) call -> idealStates.get(call.getArguments()[0]));
    when(cache.getIdealStates()).thenReturn(idealStates);

    ResourceConfig resourceConfig = new ResourceConfig(RESOURCE);
    resourceConfig.setPartitionCapacityMap(Collections.singletonMap(
        ResourceConfig.DEFAULT_PARTITION_KEY, Collections.singletonMap(CAPACITY_KEY, 1)));
    when(cache.getResourceConfig(RESOURCE)).thenReturn(resourceConfig);
    when(cache.getResourceConfigMap())
        .thenReturn(Collections.singletonMap(RESOURCE, resourceConfig));

    for (BuiltInStateModelDefinitions definition : BuiltInStateModelDefinitions.values()) {
      when(cache.getStateModelDef(definition.name()))
          .thenReturn(definition.getStateModelDefinition());
    }

    // Collection only runs when the capacity check it reconciles with is active, and the capacity
    // check itself must accept placements or every round would prune and nothing would be
    // measurable.
    when(cache.getWagedInstanceCapacity()).thenReturn(mock(WagedInstanceCapacity.class));
    when(cache.checkAndReduceCapacity(Mockito.any(), Mockito.any(), Mockito.any()))
        .thenReturn(true);

    for (String instance : instances) {
      Set<String> resident = ghosts.getOrDefault(instance, Collections.emptySet());
      Map<String, CurrentState> currentStates = new HashMap<>();
      if (!resident.isEmpty()) {
        CurrentState currentState = new CurrentState(RESOURCE);
        currentState.setSessionId(SESSION);
        currentState.setStateModelDefRef(STATE_MODEL);
        resident.forEach(partition -> currentState.setState(partition, GHOST_STATE));
        currentStates.put(RESOURCE, currentState);
      }
      when(cache.getCurrentState(instance, SESSION)).thenReturn(currentStates);
      when(cache.getCurrentState(instance, SESSION, false)).thenReturn(currentStates);
    }

    return cache;
  }

  private Map<String, Resource> resourceMap() {
    Resource resource = new Resource(RESOURCE);
    partitions().forEach(resource::addPartition);
    return Collections.singletonMap(RESOURCE, resource);
  }

  /** partition -> instance, for one rebalance round. */
  private Map<String, String> placements(Map<String, IdealState> idealStates) {
    Map<String, String> placement = new LinkedHashMap<>();
    IdealState idealState = idealStates.get(RESOURCE);
    if (idealState == null) {
      return placement;
    }
    for (String partition : idealState.getPartitionSet()) {
      Map<String, String> stateMap = idealState.getInstanceStateMap(partition);
      if (stateMap == null) {
        continue;
      }
      stateMap.entrySet().stream().filter(e -> SERVING_STATE.equals(e.getValue()))
          .map(Map.Entry::getKey).sorted().findFirst()
          .ifPresent(instance -> placement.put(partition, instance));
    }
    return placement;
  }

  /**
   * Run {@code rounds} consecutive rebalances against one cache and return the placement seen after
   * each. Only the first round reports a cluster change, so every later round is the steady-state
   * case: nothing about the cluster moved, and any difference between consecutive rounds is the
   * rebalancer moving a replica on its own.
   */
  /**
   * Take an instance out of the cluster the way a process death does: its live instance and
   * enabled entries disappear while its configuration remains. Replicas assigned to it are then
   * homeless, which is the condition emergency rebalance exists to resolve and the only scope in
   * which the gate is allowed to withhold room.
   */
  private void takeDown(ResourceControllerDataProvider cache, String downInstance) {
    Map<String, LiveInstance> live = new LinkedHashMap<>(cache.getAssignableLiveInstances());
    live.remove(downInstance);
    when(cache.getAssignableLiveInstances()).thenReturn(live);
    when(cache.getLiveInstances()).thenReturn(live);
    Set<String> remaining = new LinkedHashSet<>(live.keySet());
    when(cache.getEnabledInstances()).thenReturn(remaining);
    when(cache.getEnabledLiveInstances()).thenReturn(remaining);
    when(cache.getAssignableInstances()).thenReturn(remaining);
  }

  private List<Map<String, String>> run(ResourceControllerDataProvider cache, int rounds)
      throws HelixRebalanceException {
    WagedRebalancer rebalancer = new WagedRebalancer(new MockAssignmentMetadataStore(),
        ConstraintBasedAlgorithmFactory
            .getInstance(ClusterConfig.DEFAULT_GLOBAL_REBALANCE_PREFERENCE),
        Optional.empty());
    List<Map<String, String>> history = new ArrayList<>();
    for (int round = 0; round < rounds; round++) {
      when(cache.getRefreshedChangeTypes()).thenReturn(round == 0
          ? Collections.singleton(HelixConstants.ChangeType.CLUSTER_CONFIG)
          : Collections.emptySet());
      history.add(placements(
          rebalancer.computeNewIdealStates(cache, resourceMap(), new CurrentStateOutput())));
    }
    return history;
  }

  private int movesAfterFirstRound(List<Map<String, String>> history) {
    int moves = 0;
    for (int round = 1; round < history.size(); round++) {
      Map<String, String> previous = history.get(round - 1);
      for (Map.Entry<String, String> entry : history.get(round).entrySet()) {
        String before = previous.get(entry.getKey());
        if (before != null && !before.equals(entry.getValue())) {
          moves++;
        }
      }
    }
    return moves;
  }

  /**
   * The core churn question. Hidden occupancy is constant across every round here, so once the
   * first round has placed everything there is nothing left for the rebalancer to react to. If the
   * gate could feed its own effect back into the next round's input -- the failure mode a scoring
   * signal has -- placements would keep shifting. They must not.
   */
  @Test
  public void testConstantHiddenOccupancyIsStableAcrossRounds()
      throws IOException, HelixRebalanceException {
    Map<String, Set<String>> ghosts = new HashMap<>();
    ghosts.put("instance_0", Collections.singleton(RESOURCE + "_3"));
    ghosts.put("instance_1", Collections.singleton(RESOURCE + "_4"));

    List<Map<String, String>> history = run(newCache(true, ghosts), 8);

    Assert.assertFalse(history.get(0).isEmpty(), "the first round must place something, "
        + "otherwise the later rounds would be vacuously stable");
    Assert.assertEquals(movesAfterFirstRound(history), 0,
        "hidden occupancy did not change between rounds, so no replica may be re-chosen: "
            + history);
  }

  /**
   * The round trip that a filter can still cause: a replica is kept off an instance while that
   * instance is secretly full, and the occupancy later drains. The replica may correct itself once,
   * but it must then settle -- an instance that a replica has arrived on cannot block it, because a
   * candidate is excluded from its own hidden occupancy, so there is no way back out again.
   */
  @Test
  public void testDrainingHiddenOccupancyDoesNotOscillate()
      throws IOException, HelixRebalanceException {
    Map<String, Set<String>> ghosts = new HashMap<>();
    ghosts.put("instance_0", Collections.singleton(RESOURCE + "_3"));
    ghosts.put("instance_1", Collections.singleton(RESOURCE + "_4"));
    ResourceControllerDataProvider cache = newCache(true, ghosts);

    WagedRebalancer rebalancer = new WagedRebalancer(new MockAssignmentMetadataStore(),
        ConstraintBasedAlgorithmFactory
            .getInstance(ClusterConfig.DEFAULT_GLOBAL_REBALANCE_PREFERENCE),
        Optional.empty());
    List<Map<String, String>> history = new ArrayList<>();
    for (int round = 0; round < 8; round++) {
      when(cache.getRefreshedChangeTypes()).thenReturn(round == 0
          ? Collections.singleton(HelixConstants.ChangeType.CLUSTER_CONFIG)
          : Collections.emptySet());
      if (round == 4) {
        // The stuck replicas finally drop: the instances are now genuinely as empty as the planner
        // always believed they were.
        for (String instance : instances()) {
          when(cache.getCurrentState(instance, SESSION)).thenReturn(Collections.emptyMap());
          when(cache.getCurrentState(instance, SESSION, false))
              .thenReturn(Collections.emptyMap());
        }
      }
      history.add(placements(
          rebalancer.computeNewIdealStates(cache, resourceMap(), new CurrentStateOutput())));
    }

    List<Map<String, String>> afterDrain = history.subList(5, history.size());
    for (int round = 1; round < afterDrain.size(); round++) {
      Assert.assertEquals(afterDrain.get(round), afterDrain.get(round - 1),
          "once the occupancy drained the assignment must settle rather than keep moving: "
              + history);
    }

    for (String partition : partitions()) {
      long distinct = history.stream().map(round -> round.get(partition))
          .filter(java.util.Objects::nonNull).distinct().count();
      Assert.assertTrue(distinct <= 2,
          partition + " occupied " + distinct + " different instances across the run, which is a "
              + "round trip rather than a one-way correction: " + history);
    }
  }

  /** Partitions placed on {@code instance} that were not already physically resident there. */
  private Set<String> newArrivals(Map<String, String> placement, String instance,
      Set<String> alreadyResident) {
    Set<String> arrivals = new HashSet<>();
    placement.forEach((partition, host) -> {
      if (instance.equals(host) && !alreadyResident.contains(partition)) {
        arrivals.add(partition);
      }
    });
    return arrivals;
  }

  /**
   * Non-vacuity, and the incident replay. Both of an instance's slots are held by replicas sitting
   * in a state carrying no count, so the capacity ledger sees it full while the planner sees it
   * empty -- the production shape exactly. With the gate off the planner ranks that instance among
   * the emptiest and sends fresh work to it, which is the placement the capacity check then rejects
   * every round. With the gate on nothing new may arrive there.
   * <p>
   * What the gate must not do is evict the replicas already sitting there. A candidate is excluded
   * from its own hidden occupancy, so a stuck replica still counts its own instance as eligible and
   * can stay put at no cost. That asymmetry -- closed to newcomers, open to the replica already
   * resident -- is what keeps the gate from generating movement of its own.
   * <p>
   * The gate is restricted to emergency rebalance, so this exercises the case it still covers: a
   * replica whose instance has gone away and which must be rehomed somewhere. The starting
   * assignment is seeded rather than grown round by round, because partial rebalance no longer
   * reads hidden occupancy and would otherwise fill the instance under test before emergency ever
   * ran.
   * <p>
   * This is also what stops the stability results above from being vacuous: it proves the gate
   * really does change placement, so a quiet run elsewhere is quiet because the gate is stable
   * rather than because it never fired.
   */
  @Test
  public void testFullyHiddenInstanceTakesNoNewWorkOnlyWhenEnabled()
      throws IOException, HelixRebalanceException {
    Set<String> resident = new HashSet<>(
        java.util.Arrays.asList(RESOURCE + "_3", RESOURCE + "_4"));
    Map<String, Set<String>> ghosts = Collections.singletonMap("instance_0", resident);

    Map<String, String> gated = rehomeAfterKill(newCache(true, ghosts), ghosts);
    Map<String, String> ungated = rehomeAfterKill(newCache(false, ghosts), ghosts);

    Assert.assertEquals(newArrivals(gated, "instance_0", resident), Collections.emptySet(),
        "instance_0's room is entirely spoken for, so a replica that lost its instance must not "
            + "be rehomed there: " + gated);
    Assert.assertFalse(newArrivals(ungated, "instance_0", resident).isEmpty(),
        "without the gate the planner should still treat instance_0 as empty; if it does not, "
            + "this fixture no longer reproduces the condition and proves nothing: " + ungated);
    Assert.assertFalse(gated.isEmpty(),
        "withholding the instance must not cost every partition its placement: " + gated);
  }

  /**
   * Seed an assignment that deliberately avoids {@code instance_0}, kill the instance holding two
   * of those replicas, and return where emergency rebalance puts them.
   */
  private Map<String, String> rehomeAfterKill(ResourceControllerDataProvider cache,
      Map<String, Set<String>> ghosts) throws HelixRebalanceException {
    Map<String, String> seed = new LinkedHashMap<>();
    seed.put(RESOURCE + "_0", "instance_1");
    seed.put(RESOURCE + "_1", "instance_1");
    seed.put(RESOURCE + "_2", "instance_2");
    seed.put(RESOURCE + "_3", "instance_3");
    seed.put(RESOURCE + "_4", "instance_2");

    MockAssignmentMetadataStore store = new MockAssignmentMetadataStore();
    store.persistBaseline(planPlacing(seed));
    store.persistBestPossibleAssignment(planPlacing(seed));

    WagedRebalancer rebalancer = new WagedRebalancer(store, ConstraintBasedAlgorithmFactory
        .getInstance(ClusterConfig.DEFAULT_GLOBAL_REBALANCE_PREFERENCE), Optional.empty());

    takeDown(cache, "instance_1");
    when(cache.getRefreshedChangeTypes())
        .thenReturn(Collections.singleton(HelixConstants.ChangeType.INSTANCE_CONFIG));
    return placements(
        rebalancer.computeNewIdealStates(cache, resourceMap(), new CurrentStateOutput()));
  }

  private Map<String, ResourceAssignment> planPlacing(Map<String, String> placement) {
    ResourceAssignment assignment = new ResourceAssignment(RESOURCE);
    placement.forEach((partition, instance) -> assignment.addReplicaMap(new Partition(partition),
        Collections.singletonMap(instance, SERVING_STATE)));
    return Collections.singletonMap(RESOURCE, assignment);
  }

  /**
   * A cluster with nothing stuck must rebalance exactly as it did before the feature existed. This
   * is what makes the gate safe to enable ahead of an incident rather than during one.
   */
  @Test
  public void testNoHiddenOccupancyMatchesFeatureDisabled()
      throws IOException, HelixRebalanceException {
    List<Map<String, String>> enabled = run(newCache(true, Collections.emptyMap()), 4);
    List<Map<String, String>> disabled = run(newCache(false, Collections.emptyMap()), 4);

    Assert.assertEquals(enabled, disabled,
        "with no hidden occupancy the gate withholds nothing and must be invisible");
    Assert.assertEquals(movesAfterFirstRound(enabled), 0,
        "a healthy cluster must not move anything after the first round");
  }

  /**
   * The gate must never take the last eligible instance away from a replica. Withholding room is
   * only ever meant to redirect a placement, so a cluster whose every instance carries hidden
   * occupancy must still place every partition it can fit.
   */
  @Test
  public void testGateNeverLeavesPartitionsUnplaced()
      throws IOException, HelixRebalanceException {
    Map<String, Set<String>> ghosts = new HashMap<>();
    ghosts.put("instance_0", Collections.singleton(RESOURCE + "_4"));
    ghosts.put("instance_2", Collections.singleton(RESOURCE + "_0"));

    List<Map<String, String>> history = run(newCache(true, ghosts), 4);
    Map<String, String> settled = history.get(history.size() - 1);

    Assert.assertEquals(new java.util.TreeSet<>(settled.keySet()),
        new java.util.TreeSet<>(partitions()),
        "every partition still fits in the cluster and must be placed: " + settled);
  }
}
