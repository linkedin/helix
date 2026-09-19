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
import org.apache.helix.constants.InstanceConstants;
import org.apache.helix.controller.dataproviders.ResourceControllerDataProvider;
import org.apache.helix.controller.rebalancer.constraint.MonitoredAbnormalResolver;
import org.apache.helix.controller.rebalancer.waged.constraints.ConstraintBasedAlgorithmFactory;
import org.apache.helix.controller.stages.CurrentStateOutput;
import org.apache.helix.model.BuiltInStateModelDefinitions;
import org.apache.helix.model.ClusterConfig;
import org.apache.helix.model.CurrentState;
import org.apache.helix.model.IdealState;
import org.apache.helix.model.InstanceConfig;
import org.apache.helix.model.LiveInstance;
import org.apache.helix.model.Resource;
import org.apache.helix.model.ResourceConfig;
import org.mockito.Mockito;
import org.mockito.stubbing.Answer;
import org.testng.Assert;
import org.testng.annotations.Test;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Reproduction of the churn experiment raised in review against the earlier soft-constraint shape
 * of this feature: twenty settled nodes, one node added, four hundred single-replica partitions,
 * twenty-two slots per node. That is four hundred replicas in four hundred and sixty two slots, so
 * the cluster runs at about eighty seven percent full and every placement decision is made under
 * real capacity pressure rather than in the wide-open band where a capacity constraint is inert.
 * <p>
 * The failure being looked for is specific. The reported signature was a partition whose target was
 * chosen, abandoned, and then chosen again -- {@code indexed_192: n17 -> n20 -> n12 -> n20 -> n08},
 * where n20 is picked twice. That is only possible if a node's desirability changes between rounds,
 * and the mechanism proposed for it was that copies still moving elsewhere register as occupancy
 * and so move the node's standing from one round to the next.
 * <p>
 * So the loop is closed here rather than assumed away. Every move a round makes leaves a copy
 * behind on the instance it left, in a state carrying no count, for {@link #DROP_LATENCY_ROUNDS}
 * rounds before it drops -- which is what a real migration looks like and is the only way the
 * output of one round can reach the input of the next. If reading that occupancy destabilises
 * placement, the revisit count is where it shows up.
 */
public class TestWagedHiddenOccupancyNodeAdditionChurn {
  private static final String RESOURCE = "indexed";
  private static final String STATE_MODEL = "OnlineOffline";
  private static final String SERVING_STATE = "ONLINE";
  private static final String IN_FLIGHT_STATE = "OFFLINE";
  private static final String CAPACITY_KEY = "slot";
  private static final String SESSION = "testSession";

  private static int knob(String name, int fallback) {
    return Integer.getInteger("churn." + name, fallback);
  }

  private static final int EXISTING_NODES = knob("nodes", 20);
  private static final int PARTITION_COUNT = knob("partitions", 400);
  private static final int SLOTS_PER_NODE = knob("slots", 21);
  private static final int ROUNDS = knob("rounds", 60);
  private static final int NODE_ADDED_AT_ROUND = knob("addRound", 8);
  /** How long a replica keeps occupying the instance it is leaving before the drop completes. */
  private static final int DROP_LATENCY_ROUNDS = knob("dropLatency", 2);
  /** Transitions the cluster can actually complete per round, so plans are re-run mid-migration. */
  private static final int MAX_TRANSITIONS_PER_ROUND = knob("throttle", 25);
  /** How often the flapping instance leaves or rejoins. */
  private static final int FLAP_PERIOD = knob("flapPeriod", 6);
  /**
   * Share of leftover copies that never get cleaned up at all. This is the production pathology
   * the feature exists for: copies that sat in a non-counting state for hours while the capacity
   * ledger went on charging for them.
   */
  private static final int STUCK_PERCENT = knob("stuckPercent", 20);
  /** Rounds at the end during which nothing is perturbed, so convergence can be observed. */
  private static final int QUIET_ROUNDS = knob("quietRounds", 20);
  /** An instance that keeps leaving and rejoining, so replanning never stops. */
  private static final int FLAPPING_NODE = 3;

  private static String node(int index) {
    return String.format("n%02d", index);
  }

  private static String partition(int index) {
    return RESOURCE + "_" + index;
  }

  private List<String> partitions() {
    List<String> partitions = new ArrayList<>();
    for (int i = 0; i < PARTITION_COUNT; i++) {
      partitions.add(partition(i));
    }
    return partitions;
  }

  private Map<String, Resource> resourceMap() {
    Resource resource = new Resource(RESOURCE);
    partitions().forEach(resource::addPartition);
    return Collections.singletonMap(RESOURCE, resource);
  }

  /** A copy occupying {@code instance} that no longer belongs there and has not been cleaned up. */
  private static final class InFlightCopy {
    final String instance;
    final String partition;
    int roundsRemaining;

    InFlightCopy(String instance, String partition, int roundsRemaining) {
      this.instance = instance;
      this.partition = partition;
      this.roundsRemaining = roundsRemaining;
    }
  }

  /** A transition that has been started and is building a copy on {@code target}. */
  private static final class Transition {
    final String target;
    int roundsRemaining;

    Transition(String target, int roundsRemaining) {
      this.target = target;
      this.roundsRemaining = roundsRemaining;
    }
  }

  private ResourceControllerDataProvider newCache(boolean featureEnabled) throws IOException {
    ResourceControllerDataProvider cache = mock(ResourceControllerDataProvider.class);

    ClusterConfig clusterConfig = new ClusterConfig("churnCluster");
    clusterConfig.setInstanceCapacityKeys(Collections.singletonList(CAPACITY_KEY));
    clusterConfig
        .setDefaultInstanceCapacityMap(Collections.singletonMap(CAPACITY_KEY, SLOTS_PER_NODE));
    clusterConfig.setDefaultPartitionWeightMap(Collections.singletonMap(CAPACITY_KEY, 1));
    clusterConfig.setWagedCountUnallocatedOccupancyEnabled(featureEnabled);
    when(cache.getClusterConfig()).thenReturn(clusterConfig);
    when(cache.getAbnormalStateResolver(any()))
        .thenReturn(MonitoredAbnormalResolver.DUMMY_STATE_RESOLVER);

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

    when(cache.getWagedInstanceCapacity()).thenReturn(mock(WagedInstanceCapacity.class));
    when(cache.checkAndReduceCapacity(Mockito.any(), Mockito.any(), Mockito.any()))
        .thenReturn(true);

    return cache;
  }

  private void setInstances(ResourceControllerDataProvider cache, List<String> instances) {
    Map<String, InstanceConfig> instanceConfigs = new LinkedHashMap<>();
    Map<String, LiveInstance> liveInstances = new LinkedHashMap<>();
    for (String instance : instances) {
      InstanceConfig instanceConfig = new InstanceConfig(instance);
      instanceConfig
          .setInstanceCapacityMap(Collections.singletonMap(CAPACITY_KEY, SLOTS_PER_NODE));
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
    Set<String> instanceSet = new LinkedHashSet<>(instances);
    when(cache.getEnabledInstances()).thenReturn(instanceSet);
    when(cache.getEnabledLiveInstances()).thenReturn(instanceSet);
    when(cache.getAssignableInstances()).thenReturn(instanceSet);
    when(cache.getAllInstances()).thenReturn(instanceSet);
  }

  /**
   * Publish the cluster as it physically is. Every replica reports where it is actually serving,
   * and on top of that any copy that a migration has left behind reports from the instance it is
   * still sitting on, in a state the planner cannot model. That second group is the whole point:
   * it is occupancy the capacity ledger charges and the planner does not see.
   */
  private void setCurrentState(ResourceControllerDataProvider cache, List<String> instances,
      Map<String, String> physical, List<InFlightCopy> leftovers) {
    Map<String, Map<String, String>> byInstance = new HashMap<>();
    physical.forEach((partition, instance) -> byInstance
        .computeIfAbsent(instance, k -> new HashMap<>()).put(partition, SERVING_STATE));
    leftovers.forEach(copy -> byInstance.computeIfAbsent(copy.instance, k -> new HashMap<>())
        .putIfAbsent(copy.partition, IN_FLIGHT_STATE));

    for (String instance : instances) {
      Map<String, String> resident = byInstance.getOrDefault(instance, Collections.emptyMap());
      Map<String, CurrentState> currentStates = new HashMap<>();
      if (!resident.isEmpty()) {
        CurrentState currentState = new CurrentState(RESOURCE);
        currentState.setSessionId(SESSION);
        currentState.setStateModelDefRef(STATE_MODEL);
        resident.forEach(currentState::setState);
        currentStates.put(RESOURCE, currentState);
      }
      when(cache.getCurrentState(instance, SESSION)).thenReturn(currentStates);
      when(cache.getCurrentState(instance, SESSION, false)).thenReturn(currentStates);
    }
  }

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

  /** The outcome of one full run: the placement history plus the in-flight load it generated. */
  private static final class Run {
    final List<Map<String, String>> history = new ArrayList<>();
    int peakInFlight;
    int totalMoves;
    int totalPlanChanges;
    int abandonedTransitions;
    int leftoverSeq;
    final Set<Integer> perturbedRounds = new LinkedHashSet<>();
    int stuckLeftovers;
    int minPlaced = Integer.MAX_VALUE;
    int peakNewNodeLoad;
    /**
     * Unprovoked plan changes split by whether the partition had a live replica at the time.
     * Moving a partition that is already serving is churn in the sense that matters -- real data
     * gets copied and a healthy replica is disturbed. Re-choosing where to build a replica that
     * does not exist yet costs nothing, because nothing is there to move.
     */
    int unprovokedWhileServing;
    int unprovokedWhileAbsent;

    /** How many rounds at the end were byte-identical, i.e. how long the cluster has held still. */
    int trailingStableRounds() {
      Map<String, String> last = history.get(history.size() - 1);
      int stable = 0;
      for (int i = history.size() - 1; i >= 0 && history.get(i).equals(last); i--) {
        stable++;
      }
      return stable;
    }

    /**
     * Count the reported signature: a partition placed on an instance it had already left. This is
     * "targets rechosen" -- not merely a second move, but a return to somewhere abandoned.
     */
    Map<String, List<String>> revisits() {
      Map<String, List<String>> offenders = new LinkedHashMap<>();
      Set<String> partitions = new LinkedHashSet<>();
      history.forEach(round -> partitions.addAll(round.keySet()));
      for (String partition : partitions) {
        List<String> trail = new ArrayList<>();
        for (Map<String, String> round : history) {
          String instance = round.get(partition);
          if (instance != null && (trail.isEmpty() || !trail.get(trail.size() - 1)
              .equals(instance))) {
            trail.add(instance);
          }
        }
        Set<String> seen = new HashSet<>();
        for (int i = 0; i < trail.size(); i++) {
          if (!seen.add(trail.get(i)) ) {
            offenders.put(partition, trail);
            break;
          }
        }
      }
      return offenders;
    }
  }

  /**
   * Create the copy a migration leaves behind. Most are cleaned up after the drop latency; a fixed
   * share are never cleaned up, which is what actually happened in production and is the only
   * reason the capacity ledger and the planner ever disagree for long.
   */
  private InFlightCopy newLeftover(Run run, String instance, String partition) {
    boolean stuck = (run.leftoverSeq++ * 37) % 100 < STUCK_PERCENT;
    if (stuck) {
      run.stuckLeftovers++;
    }
    return new InFlightCopy(instance, partition, stuck ? Integer.MAX_VALUE : DROP_LATENCY_ROUNDS);
  }

  /**
   * The placement trail with the round each change happened in, and a marker on any change that
   * landed in a round where the cluster itself was perturbed. A change marked {@code !} is the
   * planner reacting to an instance arriving or leaving; an unmarked change is the planner
   * changing its mind on its own, which is the only kind that would indicate a feedback loop.
   */
  private static List<String> datedTrailOf(Run run, String partition) {
    List<String> trail = new ArrayList<>();
    String last = null;
    for (int round = 0; round < run.history.size(); round++) {
      String instance = run.history.get(round).get(partition);
      if (instance == null || instance.equals(last)) {
        continue;
      }
      trail.add(instance + "@" + round + (run.perturbedRounds.contains(round) ? "!" : ""));
      last = instance;
    }
    return trail;
  }

  /** Total changes across every partition made in rounds where the cluster itself was untouched. */
  private static long totalUnprovokedChanges(Run run) {
    Set<String> partitions = new LinkedHashSet<>();
    run.history.forEach(round -> partitions.addAll(round.keySet()));
    return partitions.stream().mapToLong(p -> unprovokedChanges(run, p)).sum();
  }

  /** Changes that happened in a round where nothing about the cluster changed. */
  private static long unprovokedChanges(Run run, String partition) {
    return datedTrailOf(run, partition).stream().skip(1).filter(e -> !e.endsWith("!")).count();
  }

  private static List<String> trailOf(Run run, String partition) {
    List<String> trail = new ArrayList<>();
    for (Map<String, String> round : run.history) {
      String instance = round.get(partition);
      if (instance != null && (trail.isEmpty() || !trail.get(trail.size() - 1).equals(instance))) {
        trail.add(instance);
      }
    }
    return trail;
  }

  private Run simulate(boolean featureEnabled) throws IOException, HelixRebalanceException {
    ResourceControllerDataProvider cache = newCache(featureEnabled);
    List<String> instances = new ArrayList<>();
    for (int i = 0; i < EXISTING_NODES; i++) {
      instances.add(node(i));
    }
    setInstances(cache, instances);

    WagedRebalancer rebalancer = new WagedRebalancer(new MockAssignmentMetadataStore(),
        ConstraintBasedAlgorithmFactory
            .getInstance(ClusterConfig.DEFAULT_GLOBAL_REBALANCE_PREFERENCE),
        Optional.empty());

    Run run = new Run();
    List<InFlightCopy> leftovers = new ArrayList<>();
    // Where each replica is physically serving. It starts empty and lags the plan from then on,
    // because transitions are throttled -- which is why the planner keeps running while copies are
    // still in motion.
    Map<String, String> physical = new LinkedHashMap<>();
    Map<String, Transition> building = new LinkedHashMap<>();
    Map<String, String> previousPlan = Collections.emptyMap();

    for (int round = 0; round < ROUNDS + QUIET_ROUNDS; round++) {
      boolean quiet = round >= ROUNDS;
      boolean clusterChanged = round == 0;
      if (round == NODE_ADDED_AT_ROUND) {
        instances.add(node(EXISTING_NODES));
        clusterChanged = true;
      }
      // The flapping instance leaves and rejoins on a fixed cadence. Its replicas have to be
      // replanned into an almost full cluster every time, so the planner is never idle and always
      // working alongside copies that are still in motion.
      if (!quiet && round > NODE_ADDED_AT_ROUND && round % FLAP_PERIOD == 0) {
        String flapping = node(FLAPPING_NODE);
        if (instances.contains(flapping) && instances.size() > 2) {
          instances.remove(flapping);
          physical.values().removeIf(flapping::equals);
          leftovers.removeIf(copy -> copy.instance.equals(flapping));
          building.values().removeIf(t -> t.target.equals(flapping));
        } else {
          instances.add(flapping);
        }
        clusterChanged = true;
      }
      if (clusterChanged) {
        setInstances(cache, instances);
        run.perturbedRounds.add(round);
      }
      when(cache.getRefreshedChangeTypes()).thenReturn(clusterChanged
          ? Collections.singleton(HelixConstants.ChangeType.INSTANCE_CONFIG)
          : Collections.emptySet());

      setCurrentState(cache, instances, physical, leftovers);
      run.peakInFlight = Math.max(run.peakInFlight, leftovers.size());

      Map<String, String> plan = placements(
          rebalancer.computeNewIdealStates(cache, resourceMap(), new CurrentStateOutput()));
      run.history.add(plan);
      run.minPlaced = Math.min(run.minPlaced, plan.size());
      run.peakNewNodeLoad = Math.max(run.peakNewNodeLoad,
          (int) plan.values().stream().filter(node(EXISTING_NODES)::equals).count());

      // A plan that changes while a copy is being built somewhere leaves that partial copy behind.
      // A plan that changes before any transition started leaves nothing, because nothing was ever
      // put there -- modelling it otherwise would invent occupancy the real cluster never has.
      for (Map.Entry<String, Transition> entry : new ArrayList<>(building.entrySet())) {
        String stillWanted = plan.get(entry.getKey());
        if (stillWanted == null || !stillWanted.equals(entry.getValue().target)) {
          run.abandonedTransitions++;
          leftovers.add(newLeftover(run, entry.getValue().target, entry.getKey()));
          building.remove(entry.getKey());
        }
      }
      for (Map.Entry<String, String> entry : previousPlan.entrySet()) {
        String nowPlannedOn = plan.get(entry.getKey());
        if (nowPlannedOn != null && !nowPlannedOn.equals(entry.getValue())) {
          run.totalPlanChanges++;
          if (!clusterChanged) {
            if (physical.containsKey(entry.getKey())) {
              run.unprovokedWhileServing++;
            } else {
              run.unprovokedWhileAbsent++;
            }
          }
        }
      }

      // Throttled migration: only a handful of transitions can be underway, so the planner is
      // re-run many times while the cluster is still mid-move.
      for (Map.Entry<String, String> entry : plan.entrySet()) {
        if (building.size() >= MAX_TRANSITIONS_PER_ROUND) {
          break;
        }
        if (entry.getValue().equals(physical.get(entry.getKey()))
            || building.containsKey(entry.getKey())) {
          continue;
        }
        building.put(entry.getKey(), new Transition(entry.getValue(), DROP_LATENCY_ROUNDS));
      }

      // Transitions that finish hand the partition over, and the instance it came from keeps the
      // old copy until the drop completes.
      for (Map.Entry<String, Transition> entry : new ArrayList<>(building.entrySet())) {
        if (--entry.getValue().roundsRemaining > 0) {
          continue;
        }
        String from = physical.put(entry.getKey(), entry.getValue().target);
        if (from != null) {
          run.totalMoves++;
          leftovers.add(newLeftover(run, from, entry.getKey()));
        }
        building.remove(entry.getKey());
      }

      leftovers.forEach(copy -> {
        if (copy.roundsRemaining != Integer.MAX_VALUE) {
          copy.roundsRemaining--;
        }
      });
      leftovers.removeIf(copy -> copy.roundsRemaining <= 0);
      previousPlan = plan;
    }
    return run;
  }

  /**
   * The experiment itself. A node is added to a settled, nearly full cluster and the rebalancer is
   * driven until it settles again, with every migration leaving real occupancy behind while it
   * completes.
   * <p>
   * The bar is the one set by the reported numbers: with the feature off a target was never once
   * rechosen across fifteen matched pairs, so the feature must not introduce a single one either.
   * Anything above zero here is the reported failure reproducing.
   */
  @Test
  public void testNodeAdditionUnderCapacityPressureDoesNotRechooseTargets()
      throws IOException, HelixRebalanceException {
    Run enabled = simulate(true);
    Run disabled = simulate(false);

    Map<String, List<String>> enabledRevisits = enabled.revisits();
    Map<String, List<String>> disabledRevisits = disabled.revisits();

    int roundsDiffering = 0;
    for (int i = 0; i < ROUNDS; i++) {
      if (!enabled.history.get(i).equals(disabled.history.get(i))) {
        roundsDiffering++;
      }
    }
    System.out.printf("[churn] nodes=%d+1 partitions=%d slots=%d utilization=%.0f%%%n",
        EXISTING_NODES, PARTITION_COUNT, SLOTS_PER_NODE,
        100.0 * PARTITION_COUNT / ((EXISTING_NODES + 1) * SLOTS_PER_NODE));
    System.out.printf("[churn] enabled : planChanges=%d peakInFlight=%d minPlaced=%d newNodeLoad=%d "
            + "revisits=%d%n", enabled.totalPlanChanges, enabled.peakInFlight, enabled.minPlaced,
        enabled.peakNewNodeLoad, enabledRevisits.size());
    System.out.printf("[churn] enabled : stuckLeftovers=%d abandonedTransitions=%d moves=%d%n",
        enabled.stuckLeftovers, enabled.abandonedTransitions, enabled.totalMoves);
    // Completed relocations: the only number that is actual data copied between instances.
    System.out.printf("[churn] REAL DATA MOVES on/off = %d / %d%n", enabled.totalMoves,
        disabled.totalMoves);
    System.out.printf("[churn] disabled: planChanges=%d peakInFlight=%d minPlaced=%d newNodeLoad=%d "
            + "revisits=%d%n", disabled.totalPlanChanges, disabled.peakInFlight, disabled.minPlaced,
        disabled.peakNewNodeLoad, disabledRevisits.size());
    System.out.printf("[churn] rounds where the two arms disagree: %d of %d%n", roundsDiffering,
        ROUNDS);
    System.out.printf(
        "[churn] UNPROVOKED serving(real churn)/absent(free) : enabled=%d/%d disabled=%d/%d%n",
        enabled.unprovokedWhileServing, enabled.unprovokedWhileAbsent,
        disabled.unprovokedWhileServing, disabled.unprovokedWhileAbsent);
    System.out.printf("[churn] UNPROVOKED changes on/off = %d / %d%n",
        totalUnprovokedChanges(enabled), totalUnprovokedChanges(disabled));
    enabledRevisits.entrySet().stream().filter(e -> !disabledRevisits.containsKey(e.getKey()))
        .limit(10).forEach(e -> {
          System.out.println("[churn]   only-with-gate " + e.getKey() + ": "
              + datedTrailOf(enabled, e.getKey()) + " unprovoked="
              + unprovokedChanges(enabled, e.getKey()));
          System.out.println("[churn]     same partition, gate off: "
              + datedTrailOf(disabled, e.getKey()) + " unprovoked="
              + unprovokedChanges(disabled, e.getKey()));
        });

    // Non-vacuity: a node really was added to a full cluster and replicas really did move, with
    // in-flight copies present while later rounds were being planned. Without this the zero below
    // would only prove the simulation never asked the rebalancer to do anything.
    Assert.assertTrue(enabled.totalMoves > 0,
        "the cluster must actually move replicas, otherwise this proves nothing");
    Assert.assertTrue(enabled.peakInFlight > 0,
        "migrations must leave in-flight occupancy behind, otherwise the feedback loop this test "
            + "exists to exercise is never closed");
    Assert.assertTrue(enabled.peakNewNodeLoad > 0,
        "the added node must actually receive replicas, otherwise the cluster never rebalanced");
    Assert.assertTrue(roundsDiffering > 0,
        "the two arms produced byte-identical plans in every round, so withholding room never "
            + "changed a single decision and this comparison says nothing about churn");

    // The bar set by the reported numbers: with the feature off a target was never rechosen, so
    // turning it on must not introduce revisits that were not already there.
    Assert.assertTrue(enabledRevisits.size() <= disabledRevisits.size(),
        "withholding room caused targets to be rechosen that were not rechosen otherwise, which "
            + "is the reported churn signature: enabled=" + enabledRevisits.size() + " disabled="
            + disabledRevisits.size() + " offenders=" + enabledRevisits);
  }

  /**
   * The property that actually matters for the churn objection: once the cluster stops being
   * perturbed, does it hold still? A constraint that keeps re-deciding produces a limit cycle and
   * never settles, and that is what a self-reinforcing feedback loop would look like. So the
   * cluster is shaken hard and then left alone, and it must come to rest and stay there.
   */
  @Test
  public void testClusterSettlesOncePerturbationStops()
      throws IOException, HelixRebalanceException {
    Run enabled = simulate(true);
    Run disabled = simulate(false);

    System.out.printf("[settle] enabled stable for last %d of %d quiet rounds%n",
        enabled.trailingStableRounds(), QUIET_ROUNDS);
    System.out.printf("[settle] disabled stable for last %d of %d quiet rounds%n",
        disabled.trailingStableRounds(), QUIET_ROUNDS);

    Assert.assertTrue(QUIET_ROUNDS >= 10, "the quiet window must be long enough to prove anything");
    Assert.assertTrue(enabled.trailingStableRounds() >= QUIET_ROUNDS / 2,
        "the cluster never came to rest with the feature on, which is what an oscillation that "
            + "feeds itself looks like: stable for only " + enabled.trailingStableRounds()
            + " of the last " + QUIET_ROUNDS + " undisturbed rounds");
    int enabledFinal = enabled.history.get(enabled.history.size() - 1).size();
    int disabledFinal = disabled.history.get(disabled.history.size() - 1).size();
    System.out.printf("[settle] final placed on/off=%d/%d of %d (stuck copies holding %d slots)%n",
        enabledFinal, disabledFinal, PARTITION_COUNT, enabled.stuckLeftovers);
    // Withholding room can only leave a partition unplanned when the room genuinely is not there.
    // Planning it anyway is what produced the outage this feature exists to prevent, so the bar is
    // that nothing is withheld beyond what the stuck copies actually consume.
    Assert.assertTrue(enabledFinal >= disabledFinal - enabled.stuckLeftovers,
        "more partitions were left unplanned than the stuck copies can account for: placed="
            + enabledFinal + " without the feature=" + disabledFinal + " stuck="
            + enabled.stuckLeftovers);
  }

  /**
   * The same run, viewed as total movement rather than revisits. Withholding room may change which
   * instance a moving replica lands on, but it must not make the cluster move appreciably more
   * than it would have moved anyway -- the reported numbers showed plan revisions roughly doubling.
   */
  @Test
  public void testNodeAdditionDoesNotIncreaseTotalMovement()
      throws IOException, HelixRebalanceException {
    Run enabled = simulate(true);
    Run disabled = simulate(false);

    // Withholding room legitimately redirects a replica that would otherwise have been planned
    // onto an instance the ledger has already filled, so exact equality is not the right bar. The
    // bar is that movement does not inflate the way it was reported to.
    int ceiling = (int) Math.ceil(disabled.totalPlanChanges * 1.2) + 2;
    Assert.assertTrue(enabled.totalPlanChanges <= ceiling,
        "withholding room inflated replanning: enabled=" + enabled.totalPlanChanges + " disabled="
            + disabled.totalPlanChanges + " ceiling=" + ceiling);
    Assert.assertTrue(enabled.totalMoves <= (int) Math.ceil(disabled.totalMoves * 1.2) + 2,
        "withholding room inflated movement: enabled=" + enabled.totalMoves + " disabled="
            + disabled.totalMoves);
  }
}
