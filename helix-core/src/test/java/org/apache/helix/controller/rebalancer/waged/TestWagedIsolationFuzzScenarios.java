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

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.function.Consumer;
import org.apache.helix.HelixRebalanceException;
import org.apache.helix.controller.rebalancer.waged.WagedFuzzSim.Driver;
import org.apache.helix.controller.rebalancer.waged.WagedFuzzSim.SimNode;
import org.apache.helix.controller.rebalancer.waged.WagedFuzzSim.SimResource;
import org.apache.helix.controller.rebalancer.waged.WagedFuzzSim.StepResult;
import org.apache.helix.controller.rebalancer.waged.WagedFuzzSim.SubStep;
import org.apache.helix.controller.rebalancer.waged.constraints.FuzzRecordingAlgorithm.Outcome;
import org.apache.helix.model.ClusterConfig;
import org.apache.helix.model.IdealState;
import org.apache.helix.model.Partition;
import org.apache.helix.model.ResourceAssignment;
import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.core.config.Configurator;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

/**
 * Deterministic hand-built scenarios that complement {@link TestWagedIsolationFuzz}, run through
 * the real WagedRebalancer with an in memory assignment store. Some cover cases the
 * fuzz generator cannot reach.
 */
public class TestWagedIsolationFuzzScenarios {
  private static final Consumer<ClusterConfig> FLAG_ON =
      config -> config.setWagedInstanceTagIsolationEnabled(true);
  private static final String DELAYED = TestWagedIsolationFuzz.DELAYED;

  private Level _helixLogLevel;

  @BeforeClass
  public void quiet() {
    _helixLogLevel = LogManager.getLogger("org.apache.helix").getLevel();
    Configurator.setLevel("org.apache.helix", Level.OFF);
  }

  @AfterClass
  public void restoreLogging() {
    Configurator.setLevel("org.apache.helix", _helixLogLevel);
  }

  @DataProvider(name = "overPlaced")
  public Object[][] overPlaced() {
    return new Object[][] {{true}, {false}};
  }

  /**
   * Clique c1 gets a resource it can never hold, large enough to fail the tag blind cluster wide
   * precheck too, and the baseline carries c1 and keeps rebalancing c0. Then a c0 instance goes
   * down. The emergency model does not hold the resource that was never placed, yet it is still
   * part of the cluster wide demand, so the attribution must charge that resource to c1. Charged
   * to nobody, or to c0, it would make the emergency rethrow, the whole pipeline would fall back
   * to the last best possible, and c0 would never fail over.
   *
   * With overPlaced, c1 also overflows with the replicas already on its nodes; without it, only
   * the unplaced resource over commits c1.
   */
  @Test(dataProvider = "overPlaced")
  public void testHealthyCliqueFailsOverPastUnplacedResourceOfBrokenClique(boolean overPlaced)
      throws Exception {
    WagedFuzzSim sim = WagedFuzzSim.handBuilt();
    for (int i = 0; i < 4; i++) {
      sim.handNode("c0", 100);
    }
    sim.handNode("c1", 100);
    sim.handNode("c1", 100);
    SimResource healthy = sim.handResource("c0", "OnlineOffline", 4, 2, 10);
    SimResource placed = sim.handResource("c1", "OnlineOffline", 2, 1, 20);

    TestWagedIsolationFuzz.Tally tally = new TestWagedIsolationFuzz.Tally("unplaced-resource");
    try (Driver on = new Driver(sim.clusterName, "on", false, FLAG_ON);
        Driver off = new Driver(sim.clusterName, "off", false, null)) {
      on.algorithm.capture = true;
      off.algorithm.capture = true;
      step(tally, sim, on, off, 0, "INIT");

      sim.handResource("c1", "OnlineOffline", 6, 1, 90);
      if (overPlaced) {
        placed.weight = 150;
      }
      StepResult broken = step(tally, sim, on, off, 1, "break c1")[0];
      IdealState before = broken.subSteps.get(0).idealStates.get(healthy.name);
      String victim = before.getPreferenceList(healthy.name + "_0").get(0);

      sim.handOffline(victim);
      StepResult[] down = step(tally, sim, on, off, 2, "offline " + victim);
      SubStep sub = down[0].subSteps.get(0);
      Assert.assertNull(sub.failure);
      Outcome emergency = calculation(sub, "EMERGENCY");
      Assert.assertNotNull(emergency, "a down instance must trigger an emergency rebalance");
      Assert.assertNull(emergency.failure, emergency.failure);
      IdealState after = sub.idealStates.get(healthy.name);
      for (String partition : healthy.partitionNames()) {
        List<String> instances = after.getPreferenceList(partition);
        Assert.assertEquals(instances.size(), healthy.replicas, partition + " " + instances);
        Assert.assertFalse(instances.contains(victim), partition + " still on " + victim);
      }
      tally.finish();

      // In the default mode the same deficit fails the emergency rebalance.
      Outcome offEmergency = calculation(down[1].subSteps.get(0), "EMERGENCY");
      Assert.assertNotNull(offEmergency);
      Assert.assertNotNull(offEmergency.failure);
      Assert.assertTrue(offEmergency.failure.contains(
          HelixRebalanceException.FailureCategory.CAPACITY_DEFICIT.name()), offEmergency.failure);
    }
  }

  /**
   * Clique c1 breaks by weight and is carried forward, then the c1 node holding the most of its
   * replicas is retagged into c0 while c1 is still broken, so it keeps c1's carried replicas. The
   * partial and emergency models preload them onto it at their current weight, and that load must
   * be charged to c1, the clique that owns the replicas. Charged to c0, the clique the node is in
   * now, it would blame every clique, both scopes would rethrow, and c0 could not fail over when
   * one of its own instances goes down.
   */
  @Test
  public void testHealthyCliqueFailsOverPastCarriedReplicasOnRetaggedNode() throws Exception {
    WagedFuzzSim sim = WagedFuzzSim.handBuilt();
    for (int i = 0; i < 3; i++) {
      sim.handNode("c0", 100);
    }
    for (int i = 0; i < 3; i++) {
      sim.handNode("c1", 100);
    }
    SimResource healthy = sim.handResource("c0", "OnlineOffline", 3, 1, 10);
    SimResource broken = sim.handResource("c1", "OnlineOffline", 6, 1, 30);

    TestWagedIsolationFuzz.Tally tally = new TestWagedIsolationFuzz.Tally("retagged-node");
    try (Driver on = new Driver(sim.clusterName, "on", false, FLAG_ON);
        Driver off = new Driver(sim.clusterName, "off", false, null)) {
      on.algorithm.capture = true;
      off.algorithm.capture = true;
      step(tally, sim, on, off, 0, "INIT");

      broken.weight = 250;
      StepResult carried = step(tally, sim, on, off, 1, "break c1")[0];
      Map<String, Integer> held = new TreeMap<>();
      IdealState brokenState = carried.subSteps.get(0).idealStates.get(broken.name);
      for (String partition : broken.partitionNames()) {
        for (String instance : brokenState.getPreferenceList(partition)) {
          held.merge(instance, 1, Integer::sum);
        }
      }
      String retagged = Collections.max(held.entrySet(), Map.Entry.comparingByValue()).getKey();
      // One carried replica fits in c0's slack; two or more would overflow it if charged to c0.
      Assert.assertTrue(held.get(retagged) >= 2, held.toString());

      sim.handRetag(retagged, "c1", "c0");
      SubStep moved = step(tally, sim, on, off, 2, "retag " + retagged)[0].subSteps.get(0);
      Assert.assertNull(moved.failure);
      for (String scope : new String[] {"GLOBAL_BASELINE", "PARTIAL"}) {
        Outcome outcome = calculation(moved, scope);
        Assert.assertNotNull(outcome, scope);
        Assert.assertNull(outcome.failure, scope + " " + outcome.failure);
      }

      IdealState before = moved.idealStates.get(healthy.name);
      String victim = null;
      for (String partition : healthy.partitionNames()) {
        String instance = before.getPreferenceList(partition).get(0);
        if (!instance.equals(retagged)) {
          victim = instance;
          break;
        }
      }
      Assert.assertNotNull(victim, before.toString());
      sim.handOffline(victim);
      SubStep sub = step(tally, sim, on, off, 3, "offline " + victim)[0].subSteps.get(0);
      Assert.assertNull(sub.failure);
      Outcome emergency = calculation(sub, "EMERGENCY");
      Assert.assertNotNull(emergency, "a down instance must trigger an emergency rebalance");
      Assert.assertNull(emergency.failure, emergency.failure);
      IdealState after = sub.idealStates.get(healthy.name);
      for (String partition : healthy.partitionNames()) {
        List<String> instances = after.getPreferenceList(partition);
        Assert.assertEquals(instances.size(), healthy.replicas, partition + " " + instances);
        Assert.assertFalse(instances.contains(victim), partition + " still on " + victim);
      }
      tally.finish();
    }
  }

  @DataProvider(name = "awayInWindow")
  public Object[][] awayInWindow() {
    return new Object[][] {{"offline"}, {"disabled"}};
  }

  /**
   * Clique c0 serves more than its live nodes can hold while one of its nodes is away inside the
   * delay window, and clique c2 breaks by a weight nothing can hold, so the delayed rebalance
   * overwrite meets a cluster wide deficit. The overwrite models only the live enabled nodes, and
   * c0's demand is over their capacity, but what the overwrite must place for c0 fits: the away
   * node's replicas of the resource with a low minimum stay counted on while the window is open,
   * so they are not the overwrite's to place. c0 must get its top up and only c2 be carried.
   * Judging c0 by every replica it owns would carry it too and leave it below its minimum. Built
   * by hand, since the fuzz modes that check isolation keep the demand of every healthy clique
   * within half the capacity of its live enabled nodes.
   */
  @Test(dataProvider = "awayInWindow")
  public void testCliqueOverItsLiveShareIsToppedUpPastClusterWideDeficit(String away)
      throws Exception {
    WagedFuzzSim sim = WagedFuzzSim.handBuilt();
    sim.clusterConfig.setDelayRebalaceEnabled(true);
    for (int i = 0; i < 4; i++) {
      sim.handNode("c0", 100);
    }
    for (int i = 0; i < 4; i++) {
      sim.handNode("c1", 100);
    }
    sim.handNode("c2", 100);
    sim.handNode("c2", 100);
    SimResource toppedUp = sim.handResource("c0", "OnlineOffline", 4, 2, 10);
    toppedUp.minActive = 2;
    SimResource parked = sim.handResource("c0", "OnlineOffline", 4, 3, 20);
    parked.minActive = 1;
    sim.handResource("c1", "OnlineOffline", 4, 2, 10);
    SimResource broken = sim.handResource("c2", "OnlineOffline", 2, 1, 10);

    TestWagedIsolationFuzz.Tally tally = new TestWagedIsolationFuzz.Tally("over-live-share");
    try (Driver on = new Driver(sim.clusterName, "on", false, FLAG_ON);
        Driver off = new Driver(sim.clusterName, "off", false, null)) {
      on.algorithm.capture = true;
      off.algorithm.capture = true;
      step(tally, sim, on, off, 0, "INIT");

      broken.weight = 5000;
      Map<String, IdealState> carried =
          step(tally, sim, on, off, 1, "break c2")[0].subSteps.get(0).idealStates;
      // The c0 node with the most replicas of the resource with the low minimum parks the most.
      Map<String, Integer> held = new TreeMap<>();
      for (String partition : parked.partitionNames()) {
        for (String instance : carried.get(parked.name).getPreferenceList(partition)) {
          held.merge(instance, 1, Integer::sum);
        }
      }
      String victim = Collections.max(held.entrySet(), Map.Entry.comparingByValue()).getKey();
      Set<String> needTopUp = new TreeSet<>();
      for (String partition : toppedUp.partitionNames()) {
        if (carried.get(toppedUp.name).getPreferenceList(partition).contains(victim)) {
          needTopUp.add(partition);
        }
      }
      Assert.assertFalse(needTopUp.isEmpty(), victim + " holds no replica of " + toppedUp.name);

      if ("offline".equals(away)) {
        sim.handOfflineInWindow(victim);
      } else {
        sim.handDisableInWindow(victim);
      }
      long demand = toppedUp.demand() + parked.demand();
      long liveCapacity = 0;
      for (SimNode n : sim.nodes.values()) {
        if (n.healthy() && n.tags().contains("c0")) {
          liveCapacity += n.capacity();
        }
      }
      Assert.assertTrue(demand > liveCapacity,
          "c0 demand " + demand + " must be over its live capacity " + liveCapacity);
      long mustPlace = demand - (long) parked.weight * held.get(victim);
      Assert.assertTrue(mustPlace <= liveCapacity,
          "what the overwrite must place for c0, " + mustPlace + ", must fit " + liveCapacity);

      StepResult[] down = step(tally, sim, on, off, 2, away + " " + victim + " in-window");
      SubStep sub = down[0].subSteps.get(0);
      Assert.assertNull(sub.failure);
      Outcome delayed = calculation(sub, DELAYED);
      Assert.assertNotNull(delayed, "a partition below its minimum must trigger the overwrite");
      Assert.assertNull(delayed.failure, delayed.failure);
      Assert.assertTrue(delayed.estimatedRemaining.values().stream().anyMatch(v -> v < 0),
          "the overwrite must meet a cluster wide deficit: " + delayed.estimatedRemaining);
      Assert.assertEquals(hook(sub, DELAYED).skipped, Collections.singleton(broken.name));
      ResourceAssignment served = sub.served.get(toppedUp.name);
      for (String partition : toppedUp.partitionNames()) {
        Set<String> instances = served.getReplicaMap(new Partition(partition)).keySet();
        Assert.assertEquals(instances.contains(victim), needTopUp.contains(partition),
            partition + " " + instances);
        Assert.assertEquals(instances.size() - (instances.contains(victim) ? 1 : 0),
            toppedUp.minActive, partition + " " + instances);
      }
      Assert.assertEquals(TestWagedIsolationFuzz.canonical(sub.served.get(parked.name)),
          TestWagedIsolationFuzz.canonical(delayed.storeBestPossibleAtStart.get(parked.name)));
      tally.finish();

      // The default mode fails the partial rebalance on the same deficit, before any overwrite.
      SubStep offSub = down[1].subSteps.get(0);
      Outcome offPartial = calculation(offSub, "PARTIAL");
      Assert.assertNotNull(offPartial);
      Assert.assertNotNull(offPartial.failure);
      Assert.assertTrue(offPartial.failure.contains(
          HelixRebalanceException.FailureCategory.CAPACITY_DEFICIT.name()), offPartial.failure);
      Assert.assertNull(calculation(offSub, DELAYED));
    }
  }

  /** Runs one step on both drivers and checks the flag on result; returns {on, off}. */
  private static StepResult[] step(TestWagedIsolationFuzz.Tally tally, WagedFuzzSim sim,
      Driver on, Driver off, int step, String event) throws Exception {
    StepResult result = on.step(sim, step, event);
    TestWagedIsolationFuzz.checkStep(tally, "step=" + step + " event=" + event, sim, result,
        false);
    return new StepResult[] {result, off.step(sim, step, event)};
  }

  private static Outcome calculation(SubStep sub, String scope) {
    for (Outcome o : sub.outcomes) {
      if (o.calculate && scope.equals(o.scope)) {
        return o;
      }
    }
    return null;
  }

  private static Outcome hook(SubStep sub, String scope) {
    for (Outcome o : sub.outcomes) {
      if (!o.calculate && scope.equals(o.scope)) {
        return o;
      }
    }
    return null;
  }
}
