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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.apache.helix.HelixRebalanceException;
import org.apache.helix.constants.InstanceConstants;
import org.apache.helix.controller.rebalancer.waged.ScopeMatrixSim.Calc;
import org.apache.helix.controller.rebalancer.waged.ScopeMatrixSim.NodeSpec;
import org.apache.helix.controller.rebalancer.waged.ScopeMatrixSim.Report;
import org.apache.helix.controller.rebalancer.waged.ScopeMatrixSim.ResourceSpec;
import org.apache.helix.controller.rebalancer.waged.ScopeMatrixSim.Run;
import org.apache.helix.controller.rebalancer.waged.model.ClusterModel.RebalanceScopeType;
import org.apache.helix.model.Partition;
import org.apache.helix.model.ResourceAssignment;
import org.apache.helix.monitoring.mbeans.ClusterStatusMonitor;
import org.testng.Assert;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

/**
 * Drives the real WAGED rebalancer through every rebalance scope, in every combination of
 * synchronous and asynchronous baseline and partial rebalance, with one or more cliques broken,
 * and checks that the healthy cliques' work lands while each broken clique is carried from the
 * right map: a baseline from the previous baseline, an emergency or partial result from the
 * previous best possible, and a delayed overwrite left out. Each case has a flag off control that
 * must look exactly like stock. The sections cover the scopes, topology changes applied to a
 * healthy clique and to the broken one, the capacity precheck in narrow scopes, swaps, the
 * lifecycle (several and all cliques broken, repairs, flag flips) and audits of the retry set and
 * the outcome reports. Capacity checks score a carried resource at the weights it was computed
 * with, since a carried clique keeps the placement it had then.
 */
public class TestWagedIsolationScopeMatrix {
  private static final AtomicInteger SEQUENCE = new AtomicInteger();

  private static String cluster(String base) {
    return "ScopeMatrix_" + base + "_" + SEQUENCE.incrementAndGet();
  }

  /**
   * A outgrows the whole cluster and is carried by the baseline; then a B node dies. The emergency
   * model also counts the partitions A never had placed, so the tag blind precheck sees the same
   * deficit, and attributing it must measure each block over the replicas the precheck summed.
   * Otherwise the shortfall reads as cluster wide and the emergency fails B's recovery as well.
   */
  @Test
  public void testEmergencyRecoversHealthyCliqueWhileDeficitCliqueIsCarried() throws Exception {
    try (ScopeMatrixSim sim = ScopeMatrixSim.standard(cluster("emergencyDeficit"), true)) {
      verifyAllFresh(sim, sim.run());
      ResourceSpec ra = sim.resources.get("RA");
      ra.partitions = 20;
      ra.weight = 40;
      Run broken = sim.run();
      Assert.assertEquals(broken.retry, set("RA"), broken.toString());

      sim.offline("b3", false);
      Run heal = sim.run();
      Assert.assertNull(heal.computeFailure, heal.toString());
      Assert.assertTrue(heal.skippedIn(RebalanceScopeType.EMERGENCY).contains("RA"),
          heal.toString());
      verify(sim, heal, tags("A"), tags("A"), false);
    }
  }

  /**
   * The delayed overwrite counterpart: a B node goes offline inside the delay window while A is
   * carried for a cluster wide deficit. The overwrite model counts the offline node's share too,
   * and B's top ups must still land while A's are left out.
   */
  @Test
  public void testDelayedOverwriteTopsUpHealthyCliqueWhileDeficitCliqueIsCarried()
      throws Exception {
    try (ScopeMatrixSim sim = ScopeMatrixSim.standard(cluster("delayedDeficit"), true)) {
      sim.enableDelay(3_600_000L);
      verifyAllFresh(sim, sim.run());
      ResourceSpec ra = sim.resources.get("RA");
      ra.partitions = 20;
      ra.weight = 40;
      Run broken = sim.run();
      Assert.assertEquals(broken.retry, set("RA"), broken.toString());

      sim.offline("b3", true);
      Run topUp = sim.run();
      Assert.assertNull(topUp.computeFailure, topUp.toString());
      verify(sim, topUp, tags("A"), tags("A"), true);
    }
  }

  /**
   * The same with B above its live share: B's resources have three replicas and min active 2, and
   * two of B's four nodes go offline inside the delay window, so B holds 240 against 200 of live
   * capacity. Only 160 of it is the overwrite's work, the rest stays parked on the offline nodes,
   * so only A is carried and every B partition is topped up to two replicas on the live nodes.
   */
  @Test
  public void testDelayedOverwriteTopsUpCliqueAboveItsLiveShareWhileDeficitCliqueIsCarried()
      throws Exception {
    try (ScopeMatrixSim sim = ScopeMatrixSim.standard(cluster("delayedAboveLiveShare"), true)) {
      sim.enableDelay(3_600_000L);
      for (String resource : sim.resourcesOf("B")) {
        sim.resources.get(resource).replicas = 3;
        sim.resources.get(resource).minActive = 2;
      }
      verifyAllFresh(sim, sim.run());
      ResourceSpec ra = sim.resources.get("RA");
      ra.partitions = 20;
      ra.weight = 40;
      Run broken = sim.run();
      Assert.assertEquals(broken.retry, set("RA"), broken.toString());

      sim.offline("b2", true);
      sim.offline("b3", true);
      Run topUp = sim.run();
      Assert.assertNull(topUp.computeFailure, topUp.toString());
      Calc overwrite = only(topUp.calcs(RebalanceScopeType.DELAYED_REBALANCE_OVERWRITES));
      Assert.assertEquals(overwrite.skipped, set("RA"), topUp.toString());
      Assert.assertFalse(Collections.disjoint(overwrite.toAssign, sim.resourcesOf("B")),
          topUp.toString());
      verify(sim, topUp, tags("A"), tags("A"), true);
      Set<String> live = set("b0", "b1");
      for (String resource : sim.resourcesOf("B")) {
        ResourceAssignment best = topUp.bestAfter.get(resource);
        for (Partition partition : best.getMappedPartitions()) {
          Set<String> emitted = topUp.emitted.get(resource).getReplicaMap(partition).keySet();
          Assert.assertTrue(emitted.containsAll(best.getReplicaMap(partition).keySet()),
              partition + " " + emitted);
          Assert.assertEquals(emitted.stream().filter(live::contains).count(), 2L,
              partition + " " + emitted);
        }
      }
    }
  }

  @DataProvider(name = "modes")
  public static Object[][] modes() {
    return new Object[][] {{false, false}, {true, false}, {false, true}, {true, true}};
  }

  /** How A is broken. */
  enum Break {
    /** One partition heavier than any node in the clique: NO_CANDIDATE_NODE. */
    TAG_LOCAL,
    /** More demand than the whole cluster holds: the tag blind capacity precheck. */
    DEFICIT
  }

  @DataProvider(name = "modesAndBreaks")
  public Object[][] modesAndBreaks() {
    List<Object[]> rows = new ArrayList<>();
    for (Object[] mode : modes()) {
      for (Break breakMode : Break.values()) {
        rows.add(new Object[] {mode[0], mode[1], breakMode});
      }
    }
    return rows.toArray(new Object[0][]);
  }

  /** The flag off failure proves which path the break mode took. */
  static void assertCategory(Calc calc, Break breakMode) {
    Assert.assertNotNull(calc.failure, calc.toString());
    Assert.assertEquals(calc.failure.getFailureCategory(),
        breakMode == Break.TAG_LOCAL ? HelixRebalanceException.FailureCategory.NO_CANDIDATE_NODE
            : HelixRebalanceException.FailureCategory.CAPACITY_DEFICIT, calc.toString());
  }

  static void breakA(ScopeMatrixSim sim, Break breakMode) {
    if (breakMode == Break.TAG_LOCAL) {
      breakA(sim, "RA_0");
    } else {
      // 40 replicas of 40 against 1000 in the whole cluster and 300 in A.
      ResourceSpec ra = sim.resources.get("RA");
      ra.partitions = 20;
      ra.weight = 40;
    }
  }

  /** Makes one RA partition heavier than any node in A. */
  static void breakA(ScopeMatrixSim sim, String partition) {
    sim.resources.get("RA").partitionWeights.put(partition, 150);
  }

  /**
   * Full baseline: a B node shrinks while A is broken, so the full baseline must move B. Flag on,
   * the full baseline carries A from the previous baseline and fits B under the new capacity, and
   * the partial serves it. Flag off, the whole baseline fails and B stays over capacity.
   */
  @Test(dataProvider = "modesAndBreaks")
  public void testFullBaselineWorkLandsWhileCliqueIsCarried(boolean asyncGlobal,
      boolean asyncPartial, Break breakMode) throws Exception {
    for (boolean isolation : new boolean[] {true, false}) {
      try (ScopeMatrixSim sim = sim("fullBaseline", isolation, asyncGlobal, asyncPartial)) {
        Set<String> b = sim.resourcesOf("B");
        Run start = bootstrap(sim);
        Assert.assertTrue(load(sim, served(start), b, "b0") > 20, start.toString());
        breakA(sim, breakMode);
        step(sim, isolation, tags("A"), tags("A"), false);

        sim.nodes.get("b0").disk = 20;
        List<Run> runs = step(sim, isolation, tags("A"), tags("A"), false);
        Run event = runs.get(0);
        Run last = last(runs);
        Calc baseline = only(event.calcs(RebalanceScopeType.GLOBAL_BASELINE));
        Assert.assertTrue(baseline.toAssign.containsAll(sim.resources.keySet()), event.toString());
        if (isolation) {
          Assert.assertNull(baseline.failure, event.toString());
          Assert.assertEquals(baseline.skipped, set("RA"), event.toString());
          Assert.assertTrue(load(sim, last.baselineAfter, b, "b0") <= 20, last.toString());
          Assert.assertTrue(load(sim, served(last), b, "b0") <= 20, last.toString());
          Assert.assertEquals(last.gauge, 1L, last.toString());
        } else {
          assertCategory(baseline, breakMode);
          Assert.assertTrue(load(sim, last.baselineAfter, b, "b0") > 20, last.toString());
          Assert.assertTrue(load(sim, served(last), b, "b0") > 20, last.toString());
        }
      }
    }
  }

  /**
   * Incremental baseline: one resource config event breaks A and changes a healthy B resource.
   * Flag on, the incremental baseline evaluates exactly the two changed resources, carries A and
   * lands the B change. Flag off, the same incremental round fails as a whole.
   */
  @Test(dataProvider = "modesAndBreaks")
  public void testIncrementalBaselineLandsHealthyChangeInTheSameEvent(boolean asyncGlobal,
      boolean asyncPartial, Break breakMode) throws Exception {
    for (boolean isolation : new boolean[] {true, false}) {
      try (ScopeMatrixSim sim = sim("incremental", isolation, asyncGlobal, asyncPartial)) {
        bootstrap(sim);
        breakA(sim, breakMode);
        sim.resources.get("RB1").weight = 30;
        List<Run> runs = step(sim, isolation, tags("A"), tags("A"), false);
        Run event = runs.get(0);
        Calc baseline = only(event.calcs(RebalanceScopeType.GLOBAL_BASELINE));
        Assert.assertEquals(baseline.toAssign, set("RA", "RB1"), event.toString());
        if (isolation) {
          Assert.assertNull(baseline.failure, event.toString());
          Assert.assertEquals(baseline.skipped, set("RA"), event.toString());
        } else {
          assertCategory(baseline, breakMode);
          Assert.assertEquals(canon(last(runs).baselineAfter, null),
              canon(event.baselineBefore, null));
        }
      }
    }
  }

  /**
   * Once A is carried, an unrelated change to one healthy resource still retries A through the
   * retry set, and the healthy change lands exactly as it does with the flag off, where the stock
   * incremental round succeeds because A is preloaded without a capacity check.
   */
  @Test(dataProvider = "modes")
  public void testRetriedCliqueDoesNotChangeAHealthyIncrementalResult(boolean asyncGlobal,
      boolean asyncPartial) throws Exception {
    Run[] results = new Run[2];
    for (boolean isolation : new boolean[] {true, false}) {
      try (ScopeMatrixSim sim = sim("retryParity", isolation, asyncGlobal, asyncPartial)) {
        bootstrap(sim);
        sim.resources.get("RA").partitionWeights.put("RA_0", 150);
        step(sim, isolation, tags("A"), tags("A"), false);
        sim.resources.get("RB1").partitions = 6;
        List<Run> runs = step(sim, isolation, tags("A"), tags("A"), false);
        Run event = runs.get(0);
        Calc baseline = only(event.calcs(RebalanceScopeType.GLOBAL_BASELINE));
        if (isolation) {
          Assert.assertEquals(baseline.toAssign, set("RA", "RB1"), event.toString());
          Assert.assertEquals(baseline.skipped, set("RA"), event.toString());
        } else {
          Assert.assertEquals(baseline.toAssign, set("RB1"), event.toString());
          Assert.assertNull(baseline.failure, event.toString());
        }
        results[isolation ? 0 : 1] = last(runs);
      }
    }
    Set<String> healthy = set("RB1", "RB2", "RC");
    Assert.assertEquals(canon(results[0].baselineAfter, healthy),
        canon(results[1].baselineAfter, healthy));
    Assert.assertEquals(canon(served(results[0]), healthy), canon(served(results[1]), healthy));
  }

  /**
   * PARTIAL only: A's baseline fits on its three assignable nodes but not on the two that are
   * still enabled, so only the partial fails for A. In the same event B gains two partitions.
   * Flag on, the partial carries A from the previous best possible and serves B's new partitions.
   * Flag off, the partial fails as a whole and B's new partitions are never served.
   */
  @Test(dataProvider = "modes")
  public void testPartialWorkLandsWhileCliqueFailsOnlyInPartial(boolean asyncGlobal,
      boolean asyncPartial) throws Exception {
    for (boolean isolation : new boolean[] {true, false}) {
      try (ScopeMatrixSim sim = sim("partial", isolation, asyncGlobal, asyncPartial)) {
        ResourceSpec ra = sim.resources.get("RA");
        ra.partitions = 3;
        ra.weight = 25;
        bootstrap(sim);
        sim.nodes.get("a2").operation = InstanceConstants.InstanceOperation.DISABLE;
        Run disabled = last(step(sim, isolation, none(), none(), false));
        Assert.assertFalse(uses(served(disabled), set("RA"), "a2"), disabled.toString());

        ra.partitions = 5;
        sim.resources.get("RB1").partitions = 6;
        List<Run> runs = step(sim, isolation, none(), tags("A"), false);
        Run last = last(runs);
        Assert.assertEquals(partitions(last.baselineAfter, "RA"), 5, last.toString());
        Assert.assertEquals(partitions(last.baselineAfter, "RB1"), 6, last.toString());
        if (isolation) {
          Assert.assertTrue(runs.stream().anyMatch(
              r -> r.skippedIn(RebalanceScopeType.PARTIAL).equals(set("RA"))), runs.toString());
          assertScopeDidSomewhere(runs, RebalanceScopeType.PARTIAL, "RB1");
          Assert.assertEquals(partitions(served(last), "RB1"), 6, last.toString());
          Assert.assertEquals(partitions(served(last), "RA"), 3, last.toString());
          Assert.assertEquals(last.gauge, 1L, last.toString());
          Assert.assertTrue(last.retry.isEmpty(), last.toString());
        } else {
          Assert.assertEquals(partitions(served(last), "RB1"), 4, last.toString());
        }
      }
    }
  }

  /**
   * EMERGENCY: a node goes down in A, which has no room to absorb its replicas, and another in B.
   * Flag on, the emergency carries A from the previous best possible and moves B off its dead
   * node. Flag off, the emergency fails and B keeps replicas on a dead node.
   */
  @Test(dataProvider = "modes")
  public void testEmergencyWorkLandsWhileCliqueFailsInEmergency(boolean asyncGlobal,
      boolean asyncPartial) throws Exception {
    for (boolean isolation : new boolean[] {true, false}) {
      try (ScopeMatrixSim sim = sim("emergency", isolation, asyncGlobal, asyncPartial)) {
        sim.resources.get("RA").weight = 30;
        Run start = bootstrap(sim);
        Assert.assertTrue(uses(served(start), set("RA"), "a2"), start.toString());
        Assert.assertTrue(uses(served(start), sim.resourcesOf("B"), "b2"), start.toString());
        sim.offline("a2", false);
        sim.offline("b2", false);
        List<Run> runs = step(sim, isolation, none(), tags("A"), false);
        Run event = runs.get(0);
        Assert.assertTrue(event.calcs(RebalanceScopeType.GLOBAL_BASELINE).isEmpty(),
            event.toString());
        Calc emergency = only(event.calcs(RebalanceScopeType.EMERGENCY));
        Assert.assertEquals(emergency.toAssign, set("RA", "RB1", "RB2"), event.toString());
        if (isolation) {
          verify(sim, event, none(), tags("A"), false);
          Assert.assertEquals(emergency.skipped, set("RA"), event.toString());
          Assert.assertTrue(uses(event.emitted, set("RA"), "a2"), event.toString());
          Assert.assertFalse(uses(event.emitted, sim.resourcesOf("B"), "b2"), event.toString());
          Assert.assertEquals(event.gauge, 1L, event.toString());
        } else {
          Assert.assertNotNull(emergency.failure, event.toString());
          Assert.assertNotNull(event.computeFailure, event.toString());
          Assert.assertTrue(uses(served(last(runs)), sim.resourcesOf("B"), "b2"),
              last(runs).toString());
        }
      }
    }
  }

  /**
   * DELAYED_REBALANCE_OVERWRITES: nodes in A and B go offline inside the delay window, so both
   * cliques drop below min active. Flag on, B's top ups land and A's are omitted so its pre
   * overwrite assignment stands. Flag off, the overwrite fails and nothing is topped up.
   */
  @Test(dataProvider = "modes")
  public void testDelayedOverwriteTopUpsLandWhileCliqueCannotTopUp(boolean asyncGlobal,
      boolean asyncPartial) throws Exception {
    for (boolean isolation : new boolean[] {true, false}) {
      try (ScopeMatrixSim sim = sim("delayed", isolation, asyncGlobal, asyncPartial)) {
        sim.enableDelay(3_600_000L);
        sim.resources.get("RA").weight = 30;
        Run start = bootstrap(sim);
        Assert.assertTrue(uses(served(start), set("RA"), "a2"), start.toString());
        Assert.assertTrue(uses(served(start), sim.resourcesOf("B"), "b2"), start.toString());
        sim.offline("a2", true);
        sim.offline("b2", true);
        List<Run> runs = step(sim, isolation, none(), tags("A"), true);
        Run event = runs.get(0);
        Assert.assertTrue(event.calcs(RebalanceScopeType.EMERGENCY).isEmpty(), event.toString());
        Calc overwrite = only(event.calcs(RebalanceScopeType.DELAYED_REBALANCE_OVERWRITES));
        Assert.assertEquals(overwrite.toAssign, set("RA", "RB1", "RB2"), event.toString());
        if (isolation) {
          verify(sim, event, none(), tags("A"), true);
          Assert.assertEquals(overwrite.skipped, set("RA"), event.toString());
          assertToppedUp(sim, event.emitted, sim.resourcesOf("B"), "b2");
          Assert.assertEquals(event.gauge, 1L, event.toString());
        } else {
          Assert.assertNotNull(overwrite.failure, event.toString());
          Assert.assertNotNull(event.computeFailure, event.toString());
          assertNotToppedUp(sim, served(last(runs)), sim.resourcesOf("B"), "b2");
        }
      }
    }
  }

  /**
   * RA's baseline and best possible once they differ, so a carried RA shows which map it was
   * carried from.
   */
  static final class Diverged {
    final Map<String, Map<String, String>> baselineRA;
    final Map<String, Map<String, String>> bestRA;
    /** An RA partition with a baseline replica on a2. */
    final String broken;

    Diverged(Map<String, Map<String, String>> baselineRA, Map<String, Map<String, String>> bestRA,
        String broken) {
      this.baselineRA = baselineRA;
      this.bestRA = bestRA;
      this.broken = broken;
    }
  }

  /**
   * Settles the simulation, then disables a2 and settles again. RA's baseline stays on a2, while
   * its best possible and served maps move off it.
   */
  static Diverged diverge(ScopeMatrixSim sim) throws Exception {
    Run start = last(settle(sim));
    Assert.assertNull(start.computeFailure, start.toString());
    Assert.assertTrue(uses(start.baselineAfter, set("RA"), "a2"), start.toString());
    sim.nodes.get("a2").operation = InstanceConstants.InstanceOperation.DISABLE;
    Run run = last(settle(sim));
    Assert.assertTrue(uses(run.baselineAfter, set("RA"), "a2"), "baseline keeps a2: " + run);
    Assert.assertFalse(uses(run.bestAfter, set("RA"), "a2"), "best possible avoids a2: " + run);
    Assert.assertFalse(uses(served(run), set("RA"), "a2"), "served avoids a2: " + run);
    Map<String, Map<String, String>> baselineRA =
        ScopeMatrixSim.canon(run.baselineAfter.get("RA"));
    String broken = baselineRA.entrySet().stream()
        .filter(partition -> partition.getValue().containsKey("a2")).map(Map.Entry::getKey)
        .findFirst().get();
    return new Diverged(baselineRA, ScopeMatrixSim.canon(run.bestAfter.get("RA")), broken);
  }

  /**
   * On every run RA's baseline is the diverged baseline, and RA's best possible and served maps
   * are the diverged best possible.
   */
  static void assertCarried(List<Run> runs, Diverged diverged, String where) {
    for (Run run : runs) {
      Assert.assertEquals(ScopeMatrixSim.canon(run.baselineAfter.get("RA")), diverged.baselineRA,
          where + ": baseline RA must be carried from the baseline: " + run);
      Assert.assertEquals(ScopeMatrixSim.canon(run.bestAfter.get("RA")), diverged.bestRA,
          where + ": best possible RA must be carried from the best possible: " + run);
      Assert.assertEquals(ScopeMatrixSim.canon(served(run).get("RA")), diverged.bestRA,
          where + ": served RA must be carried from the best possible: " + run);
    }
  }

  /**
   * Each scope carries a broken clique from its own map. RA's baseline and best possible are made
   * to differ, and four events follow: the RA partition with a baseline replica on a2 becomes
   * heavier than any node in A, and the incremental baseline skips RA; a2 is enabled, which runs
   * no baseline, and the partial evaluates RA and skips only RA; b0 shrinks to DISK 20, the full
   * baseline evaluates every resource and skips only RA, and B holds at most 20 on b0 in the
   * baseline and served maps; an A node serving the broken partition goes offline with a B node,
   * the emergency evaluates RA and skips only RA, and B moves off its offline node. On every run
   * RA's baseline stays the diverged baseline, and RA's best possible and served maps stay the
   * diverged best possible.
   */
  @Test(dataProvider = "modes")
  public void testEachScopeCarriesFromItsOwnMap(boolean asyncGlobal, boolean asyncPartial)
      throws Exception {
    try (ScopeMatrixSim sim = carrySim("carrySource", true, asyncGlobal, asyncPartial)) {
      Diverged diverged = diverge(sim);
      Set<String> b = sim.resourcesOf("B");

      breakA(sim, diverged.broken);
      List<Run> broken = settle(sim);
      Assert.assertTrue(broken.stream()
          .anyMatch(run -> run.skippedIn(RebalanceScopeType.GLOBAL_BASELINE).contains("RA")),
          broken.toString());
      assertCarried(broken, diverged, "incremental baseline");

      // Change detection trims the instance operation, so no baseline runs.
      sim.nodes.get("a2").operation = InstanceConstants.InstanceOperation.ENABLE;
      List<Run> enabled = settle(sim);
      Assert.assertTrue(enabled.stream()
          .allMatch(run -> run.calcs(RebalanceScopeType.GLOBAL_BASELINE).isEmpty()),
          enabled.toString());
      Assert.assertTrue(enabled.stream()
          .flatMap(run -> run.calcs(RebalanceScopeType.PARTIAL).stream())
          .anyMatch(calc -> calc.failure == null && calc.toAssign.contains("RA")
              && calc.skipped.equals(set("RA"))), enabled.toString());
      assertCarried(enabled, diverged, "partial");

      sim.nodes.get("b0").disk = 20;
      List<Run> shrunk = settle(sim);
      Assert.assertTrue(shrunk.stream()
          .flatMap(run -> run.calcs(RebalanceScopeType.GLOBAL_BASELINE).stream())
          .anyMatch(calc -> calc.failure == null
              && calc.toAssign.containsAll(sim.resources.keySet())
              && calc.skipped.equals(set("RA"))), shrunk.toString());
      assertCarried(shrunk, diverged, "full baseline");
      Run fitted = last(shrunk);
      Assert.assertTrue(load(sim, fitted.baselineAfter, b, "b0") <= 20, fitted.toString());
      Assert.assertTrue(load(sim, served(fitted), b, "b0") <= 20, fitted.toString());

      String aDown = diverged.bestRA.get(diverged.broken).keySet().iterator().next();
      String bDown = servingNode(sim, fitted, "B", "b0");
      sim.offline(aDown, false);
      sim.offline(bDown, false);
      List<Run> recovered = settle(sim);
      Run event = recovered.get(0);
      Calc emergency = only(event.calcs(RebalanceScopeType.EMERGENCY));
      Assert.assertNull(emergency.failure, event.toString());
      Assert.assertTrue(emergency.toAssign.contains("RA"), event.toString());
      Assert.assertEquals(emergency.skipped, set("RA"), event.toString());
      assertCarried(recovered, diverged, "emergency");
      Assert.assertFalse(uses(served(last(recovered)), b, bDown), last(recovered).toString());
    }
  }

  // ---------------------------------------------------------------- topology perturbations

  enum Perturbation {
    ADD_NODE, REMOVE_CONFIG, OFFLINE, EVACUATE, UNKNOWN, DISABLE, DISK_SHRINK, DISK_GROW,
    PARTITION_WEIGHT, ADD_RESOURCE, DELETE_RESOURCE, RETAG_IN, RETAG_OUT, MIN_ACTIVE
  }

  @DataProvider(name = "perturbations")
  public Object[][] perturbations() {
    List<Object[]> rows = new ArrayList<>();
    for (Perturbation perturbation : Perturbation.values()) {
      for (String target : new String[] {"B", "A"}) {
        for (boolean async : new boolean[] {false, true}) {
          rows.add(new Object[] {perturbation, target, async});
        }
      }
    }
    return rows.toArray(new Object[0][]);
  }

  /**
   * One topology change applied to a healthy clique (B) while A is broken, or to the broken clique
   * A itself. Four runs of the same script: flag on and off with A broken, and flag on and off with
   * nothing broken. With nothing broken the flag must change nothing at all. With A broken and the
   * flag on, every run must satisfy the isolation invariants and the healthy cliques must honour
   * the change; the flag off control must behave exactly like stock WAGED, whose baseline stays
   * stuck whenever the change makes it reassign A.
   */
  @Test(dataProvider = "perturbations")
  public void testPerturbation(Perturbation perturbation, String target, boolean async)
      throws Exception {
    boolean repairs = target.equals("A") && (perturbation == Perturbation.DISK_GROW
        || perturbation == Perturbation.DELETE_RESOURCE);
    List<Run> isolated = perturbationScript(perturbation, target, async, true, true, repairs);
    perturbationScript(perturbation, target, async, false, true, repairs);
    List<Run> healthyOn = perturbationScript(perturbation, target, async, true, false, false);
    List<Run> healthyOff = perturbationScript(perturbation, target, async, false, false, false);
    Assert.assertEquals(fingerprint(healthyOn), fingerprint(healthyOff),
        "the flag changed a cluster with nothing broken");

    Run last = last(isolated);
    if (repairs) {
      Assert.assertTrue(last.retry.isEmpty(), last.toString());
      Assert.assertEquals(last.gauge, 0L, last.toString());
    } else {
      Assert.assertTrue(last.gauge >= 1L, last.toString());
    }
  }

  private List<Run> perturbationScript(Perturbation perturbation, String target, boolean async,
      boolean isolation, boolean breakA, boolean repairs) throws Exception {
    try (ScopeMatrixSim sim = sim("perturb" + perturbation, isolation, async, async)) {
      List<Run> runs = new ArrayList<>();
      runs.add(bootstrap(sim));
      if (breakA) {
        sim.resources.get("RA").partitionWeights.put("RA_0", 150);
        runs.addAll(looseStep(sim, isolation, tags("A")));
      }
      Run before = last(runs);
      perturb(sim, perturbation, target);
      // Stock WAGED builds its logical view of the previous assignment from the instance configs,
      // so the replicas on a node whose config is removed simply vanish from it. A clique whose
      // baseline is not recomputed stays one replica short on those partitions, flag on or off.
      String vanished = perturbation == Perturbation.REMOVE_CONFIG && target.equals("A") && breakA
          ? "a1" : null;
      List<Run> after =
          looseStep(sim, isolation, breakA && !repairs ? tags("A") : none(), vanished != null);
      runs.addAll(after);
      Run event = after.get(0);
      Run last = last(after);
      if (isolation || !breakA) {
        checkPerturbationHonoured(sim, perturbation, target, last);
      }
      if (vanished != null) {
        Assert.assertEquals(ScopeMatrixSim.canon(served(last).get("RA")),
            ScopeMatrixSim.canon(without(before.bestAfter.get("RA"), vanished)), last.toString());
      }
      if (breakA && !isolation) {
        boolean reassignsA = event.calcs(RebalanceScopeType.GLOBAL_BASELINE).stream()
            .anyMatch(c -> c.toAssign.contains("RA"));
        if (reassignsA && !repairs) {
          Assert.assertEquals(canon(last.baselineAfter, null), canon(before.baselineAfter, null),
              "stock baseline must stay stuck: " + last);
        }
      }
      if (breakA && isolation && !repairs) {
        Assert.assertEquals(last.retry.contains("RA"),
            !perturbation.equals(Perturbation.DELETE_RESOURCE) || !target.equals("A"),
            last.toString());
      }
      if (repairs && isolation) {
        for (String resource : sim.resourcesOf("A")) {
          sim.assertFresh("repaired", last.emitted, resource, sim.servingNodes("A"), false);
          sim.assertFresh("repaired", last.baselineAfter, resource, sim.baselineNodes("A"),
              false);
        }
      }
      return runs;
    }
  }

  private static void perturb(ScopeMatrixSim sim, Perturbation perturbation, String tag) {
    String lower = tag.toLowerCase();
    String first = lower + "0";
    String second = lower + "1";
    String lastNode = tag.equals("A") ? "a2" : "b3";
    String resource = tag.equals("A") ? "RA" : "RB1";
    switch (perturbation) {
      case ADD_NODE:
        sim.node(lower + "9", tag, 100);
        break;
      case REMOVE_CONFIG:
        sim.nodes.get(second).hasConfig = false;
        sim.nodes.get(second).live = false;
        break;
      case OFFLINE:
        sim.offline(second, false);
        break;
      case EVACUATE:
        sim.nodes.get(second).operation = InstanceConstants.InstanceOperation.EVACUATE;
        break;
      case UNKNOWN:
        sim.nodes.get(second).operation = InstanceConstants.InstanceOperation.UNKNOWN;
        break;
      case DISABLE:
        sim.nodes.get(second).operation = InstanceConstants.InstanceOperation.DISABLE;
        break;
      case DISK_SHRINK:
        sim.nodes.get(first).disk = 20;
        break;
      case DISK_GROW:
        sim.nodes.get(first).disk = 300;
        sim.nodes.get(second).disk = 300;
        break;
      case PARTITION_WEIGHT:
        sim.resources.get(resource).partitionWeights.put(resource + "_1", 40);
        break;
      case ADD_RESOURCE:
        sim.resource("R" + tag + "9", tag, 4, 10);
        break;
      case DELETE_RESOURCE:
        sim.resources.remove(tag.equals("A") ? "RA" : "RB2");
        break;
      case RETAG_IN:
        sim.nodes.get("c2").tags.clear();
        sim.nodes.get("c2").tags.add(tag);
        break;
      case RETAG_OUT:
        sim.nodes.get(lastNode).tags.clear();
        sim.nodes.get(lastNode).tags.add("C");
        break;
      case MIN_ACTIVE:
        sim.resources.get(resource).minActive = 1;
        break;
      default:
        throw new IllegalArgumentException(perturbation.name());
    }
  }

  /** The change itself is honoured by every clique that is not broken. */
  private static void checkPerturbationHonoured(ScopeMatrixSim sim, Perturbation perturbation,
      String tag, Run last) {
    Map<String, ResourceAssignment> served = served(last);
    Set<String> others = new TreeSet<>(sim.resources.keySet());
    others.removeAll(sim.resourcesOf("A"));
    switch (perturbation) {
      case REMOVE_CONFIG:
      case EVACUATE:
      case UNKNOWN:
        Assert.assertFalse(uses(served, others, tag.toLowerCase() + "1"), last.toString());
        Assert.assertFalse(uses(last.baselineAfter, others, tag.toLowerCase() + "1"),
            last.toString());
        break;
      case OFFLINE:
      case DISABLE:
        Assert.assertFalse(uses(served, others, tag.toLowerCase() + "1"), last.toString());
        break;
      case DELETE_RESOURCE:
        String deleted = tag.equals("A") ? "RA" : "RB2";
        Assert.assertFalse(last.emitted != null && last.emitted.containsKey(deleted),
            last.toString());
        Assert.assertFalse(last.bestAfter.containsKey(deleted), last.toString());
        Assert.assertFalse(last.baselineAfter.containsKey(deleted), last.toString());
        break;
      case RETAG_IN:
        Assert.assertFalse(uses(served, sim.resourcesOf("C"), "c2"), last.toString());
        break;
      case RETAG_OUT:
        if (tag.equals("B")) {
          Assert.assertFalse(uses(served, sim.resourcesOf("B"), "b3"), last.toString());
        }
        break;
      default:
        break;
    }
  }

  /** Every run's emitted, baseline and best possible maps, for comparing two scripts. */
  static List<Object> fingerprint(List<Run> runs) {
    List<Object> out = new ArrayList<>();
    for (Run run : runs) {
      out.add(Arrays.asList(canon(run.emitted, null), canon(run.baselineAfter, null),
          canon(run.bestAfter, null), run.computeFailure == null));
    }
    return out;
  }

  /**
   * Like step, but for scripts where a broken clique may still be partly served by a scope that
   * could place it, so its entries are only required to be well formed. Flag off runs must look
   * exactly like stock.
   */
  static List<Run> looseStep(ScopeMatrixSim sim, boolean isolation, Set<String> brokenTags)
      throws Exception {
    return looseStep(sim, isolation, brokenTags, false);
  }

  static List<Run> looseStep(ScopeMatrixSim sim, boolean isolation, Set<String> brokenTags,
      boolean shortBrokenOk) throws Exception {
    int lag = sim.asyncGlobal && sim.asyncPartial ? 2 : sim.anyAsync() ? 1 : 0;
    List<Run> runs = new ArrayList<>();
    Set<String> retry = sim.retrySet();
    for (int i = 0; i < lag + 5; i++) {
      Run run = sim.run();
      if (!isolation) {
        assertStock(run);
      } else {
        verifyLoose(sim, run, retry, brokenTags, i < lag, shortBrokenOk);
      }
      assertBookkeeping(sim, run);
      retry = run.retry;
      Run previous = runs.isEmpty() ? null : last(runs);
      runs.add(run);
      if (i > lag && canon(served(run), null).equals(canon(served(previous), null))
          && canon(run.baselineAfter, null).equals(canon(previous.baselineAfter, null))) {
        break;
      }
    }
    return runs;
  }

  /**
   * The invariants every isolated run must satisfy.
   * (a) every emitted, persisted best possible and baseline entry is complete or carried;
   * (b) no node holding a freshly computed replica is over capacity;
   * (c) every resource the baseline skipped is byte identical to the previous baseline;
   * (d) the persisted maps hold only resources that still exist;
   * (e) the retry set is exactly what the last baseline left unresolved.
   * A lagging run in an asynchronous mode serves stale maps by design, so only (c), (d) and (e)
   * are checked on it.
   */
  static void verifyLoose(ScopeMatrixSim sim, Run run, Set<String> previousRetry,
      Set<String> brokenTags, boolean lagging) {
    verifyLoose(sim, run, previousRetry, brokenTags, lagging, false);
  }

  static void verifyLoose(ScopeMatrixSim sim, Run run, Set<String> previousRetry,
      Set<String> brokenTags, boolean lagging, boolean shortBrokenOk) {
    Assert.assertNull(run.computeFailure, run.toString());
    Assert.assertNull(run.pipelineFailure, run.toString());
    Assert.assertNotNull(run.emitted, run.toString());
    for (Map<String, ResourceAssignment> map : Arrays.asList(run.bestAfter, run.baselineAfter)) {
      Assert.assertTrue(sim.resources.keySet().containsAll(map.keySet()),
          "stale entries " + map.keySet() + ": " + run);
    }
    Assert.assertTrue(sim.resources.keySet().containsAll(run.emitted.keySet()),
        "stale emitted entries " + run.emitted.keySet() + ": " + run);
    Set<String> broken = sim.resourcesOf(brokenTags.toArray(new String[0]));
    Set<String> baselineSkipped = run.skippedIn(RebalanceScopeType.GLOBAL_BASELINE);
    Set<String> baselineEvaluated = new TreeSet<>();
    boolean baselineSucceeded = false;
    for (Calc calc : run.calcs(RebalanceScopeType.GLOBAL_BASELINE)) {
      if (calc.failure == null) {
        baselineEvaluated.addAll(calc.toAssign);
        baselineSucceeded = true;
      }
    }
    Set<String> carried = new TreeSet<>(broken);
    carried.addAll(baselineSkipped);
    for (RebalanceScopeType scope : Arrays.asList(RebalanceScopeType.PARTIAL,
        RebalanceScopeType.EMERGENCY, RebalanceScopeType.DELAYED_REBALANCE_OVERWRITES)) {
      carried.addAll(run.skippedIn(scope));
    }
    Set<String> fresh = new TreeSet<>();
    for (ResourceSpec spec : sim.resources.values()) {
      String resource = spec.name;
      if (baselineSkipped.contains(resource)
          || (broken.contains(resource) && !baselineEvaluated.contains(resource))) {
        Assert.assertEquals(ScopeMatrixSim.canon(run.baselineAfter.get(resource)),
            ScopeMatrixSim.canon(run.baselineBefore.get(resource)),
            resource + " baseline must be carried: " + run);
      } else if (!lagging) {
        sim.assertFresh("baseline", run.baselineAfter, resource, sim.baselineNodes(spec.tag),
            false);
      }
      if (lagging) {
        continue;
      }
      if (carried.contains(resource)) {
        boolean shortOk = shortBrokenOk && broken.contains(resource);
        assertWellFormed(sim, "emitted", run.emitted, resource, shortOk);
        assertWellFormed(sim, "best possible", run.bestAfter, resource, shortOk);
      } else {
        sim.assertFresh("emitted", run.emitted, resource, sim.servingNodes(spec.tag), false);
        sim.assertFresh("best possible", run.bestAfter, resource, sim.servingNodes(spec.tag),
            false);
        fresh.add(resource);
      }
    }
    if (!lagging) {
      sim.assertCapacity("emitted", run.emitted, carried);
      sim.assertCapacity("best possible", run.bestAfter, carried);
      Set<String> carriedBaseline = new TreeSet<>(baselineSkipped);
      broken.stream().filter(r -> !baselineEvaluated.contains(r)).forEach(carriedBaseline::add);
      sim.assertCapacity("baseline", run.baselineAfter, carriedBaseline);
      sim.recordComputedWeights(fresh);
    }
    Set<String> expectedRetry = new TreeSet<>(baselineSucceeded ? baselineSkipped : previousRetry);
    Assert.assertEquals(run.retry, expectedRetry, "retry set: " + run);
  }

  /**
   * Absent, or every partition present has exactly the configured replicas and one master. With
   * shortOk a partition may be short of replicas (see the removed config case) but never over.
   */
  static void assertWellFormed(ScopeMatrixSim sim, String where,
      Map<String, ResourceAssignment> map, String resource, boolean shortOk) {
    ResourceAssignment assignment = map.get(resource);
    if (assignment == null) {
      return;
    }
    ResourceSpec spec = sim.resources.get(resource);
    Assert.assertEquals(assignment.getMappedPartitions().size(), spec.partitions,
        where + ": " + resource + " partitions");
    for (Partition partition : assignment.getMappedPartitions()) {
      Map<String, String> replicas = assignment.getReplicaMap(partition);
      long masters = replicas.values().stream().filter("MASTER"::equals).count();
      if (shortOk) {
        Assert.assertTrue(replicas.size() <= spec.replicas && masters <= 1L,
            where + ": " + partition + replicas);
      } else {
        Assert.assertEquals(replicas.size(), spec.replicas, where + ": " + partition + replicas);
        Assert.assertEquals(masters, 1L, where + ": " + partition + replicas);
      }
    }
  }

  /** The assignment with one node's replicas removed. */
  static ResourceAssignment without(ResourceAssignment assignment, String node) {
    ResourceAssignment out = new ResourceAssignment(assignment.getResourceName());
    for (Partition partition : assignment.getMappedPartitions()) {
      Map<String, String> replicas = new java.util.HashMap<>(assignment.getReplicaMap(partition));
      replicas.remove(node);
      out.addReplicaMap(partition, replicas);
    }
    return out;
  }

  // ---------------------------------------------------------------- precheck in narrow scopes

  /**
   * EMERGENCY and PARTIAL in one event, with A broken in each by either break mode. A lives on one
   * big node, a2, with a single replica per partition; a2 is disabled, which no baseline sees.
   * In the same event B loses b1 and RB1 gains two partitions. With DEFICIT the demand no longer
   * fits the active nodes of the whole cluster, so both scopes hit the tag blind precheck; with
   * TAG_LOCAL there is room in the cluster but not on a0 or a1. Flag on, both scopes carry A from
   * the previous best possible, the emergency moves B off b1 and the partial serves RB1's new
   * partitions. Flag off, the emergency fails and neither lands.
   */
  @Test(dataProvider = "modesAndBreaks")
  public void testEmergencyAndPartialPrecheckCarryOnlyTheBrokenClique(boolean asyncGlobal,
      boolean asyncPartial, Break breakMode) throws Exception {
    for (boolean isolation : new boolean[] {true, false}) {
      try (ScopeMatrixSim sim = sim("narrowPrecheck", isolation, asyncGlobal, asyncPartial)) {
        sim.nodes.get("a2").disk = 1000;
        ResourceSpec ra = sim.resources.get("RA");
        ra.replicas = 1;
        ra.partitions = breakMode == Break.DEFICIT ? 4 : 2;
        ra.weight = breakMode == Break.DEFICIT ? 200 : 150;
        Run start = bootstrap(sim);
        Assert.assertEquals(sim.servingNodes("A").size(), 3);
        for (String node : new String[] {"a0", "a1"}) {
          Assert.assertFalse(uses(served(start), set("RA"), node), start.toString());
        }
        Assert.assertTrue(uses(served(start), sim.resourcesOf("B"), "b1"), start.toString());
        Map<String, ResourceAssignment> previous = served(start);

        sim.nodes.get("a2").operation = InstanceConstants.InstanceOperation.DISABLE;
        sim.offline("b1", false);
        sim.resources.get("RB1").partitions = 6;
        List<Run> runs = step(sim, isolation, none(), tags("A"), false);
        Run event = runs.get(0);
        Run last = last(runs);
        Calc emergency = only(event.calcs(RebalanceScopeType.EMERGENCY));
        Assert.assertTrue(emergency.toAssign.containsAll(set("RA", "RB1", "RB2")),
            event.toString());
        if (isolation) {
          Assert.assertNull(emergency.failure, event.toString());
          Assert.assertEquals(emergency.skipped, set("RA"), event.toString());
          Assert.assertTrue(runs.stream().anyMatch(
              r -> r.skippedIn(RebalanceScopeType.PARTIAL).equals(set("RA"))), runs.toString());
          assertScopeDidSomewhere(runs, RebalanceScopeType.PARTIAL, "RB1");
          Assert.assertEquals(ScopeMatrixSim.canon(served(last).get("RA")),
              ScopeMatrixSim.canon(previous.get("RA")), last.toString());
          Assert.assertFalse(uses(served(last), sim.resourcesOf("B"), "b1"), last.toString());
          Assert.assertEquals(partitions(served(last), "RB1"), 6, last.toString());
          Assert.assertEquals(last.gauge, 1L, last.toString());
          Assert.assertTrue(last.retry.isEmpty(), "no baseline ever failed: " + last);
        } else {
          assertCategory(emergency, breakMode);
          Assert.assertNotNull(event.computeFailure, event.toString());
          Assert.assertTrue(uses(served(last), sim.resourcesOf("B"), "b1"), last.toString());
          Assert.assertEquals(partitions(served(last), "RB1"), 4, last.toString());
        }
      }
    }
  }

  // ---------------------------------------------------------------- swap

  @DataProvider(name = "swaps")
  public Object[][] swaps() {
    return new Object[][] {{"B", false}, {"B", true}, {"A", false}, {"A", true}};
  }

  /**
   * A node swap, in a healthy clique while A is broken or inside the broken clique A. WAGED never
   * places on a SWAP_IN node, which is not assignable; the swap-in replicas are mirrored later by
   * BestPossibleStateCalcStage. Completing the swap copies the swap-out config to the swap-in node
   * and marks the swap-out node UNKNOWN, and because both share a logical id WAGED keeps every
   * replica where it was, now named by the new node. That must hold for a carried clique too.
   */
  @Test(dataProvider = "swaps")
  public void testSwapKeepsPlacementByLogicalId(String tag, boolean async) throws Exception {
    List<Run> isolated = swapScript(tag, async, true, true);
    swapScript(tag, async, false, true);
    Assert.assertEquals(fingerprint(swapScript(tag, async, true, false)),
        fingerprint(swapScript(tag, async, false, false)),
        "the flag changed a cluster with nothing broken");
    Run last = last(isolated);
    Assert.assertEquals(last.retry, set("RA"), last.toString());
    Assert.assertTrue(last.gauge >= 1L, last.toString());
  }

  private List<Run> swapScript(String tag, boolean async, boolean isolation, boolean breakA)
      throws Exception {
    try (ScopeMatrixSim sim = sim("swap" + tag, isolation, async, async)) {
      sim.clusterConfig.setTopology("/zone/host");
      sim.clusterConfig.setFaultZoneType("zone");
      sim.clusterConfig.setTopologyAwareEnabled(true);
      sim.nodes.values().forEach(n -> n.domain = "zone=z_" + n.name + ",host=" + n.name);
      List<Run> runs = new ArrayList<>();
      runs.add(bootstrap(sim));
      if (breakA) {
        sim.resources.get("RA").partitionWeights.put("RA_0", 150);
        runs.addAll(looseStep(sim, isolation, tags("A")));
      }
      Set<String> broken = breakA ? tags("A") : none();
      String out = tag.toLowerCase() + "1";
      String in = out + "s";
      Run before = last(runs);
      Assert.assertTrue(uses(served(before), sim.resourcesOf(tag), out), "precondition");

      ScopeMatrixSim.NodeSpec swapIn = sim.node(in, tag, 100);
      swapIn.domain = "zone=z_" + out + ",host=" + out;
      swapIn.operation = InstanceConstants.InstanceOperation.SWAP_IN;
      List<Run> pending = looseStep(sim, isolation, broken);
      runs.addAll(pending);
      for (Run run : pending) {
        Assert.assertTrue(run.calcs(RebalanceScopeType.GLOBAL_BASELINE).isEmpty(),
            "a SWAP_IN node is not assignable, so no baseline: " + run);
        Assert.assertEquals(canon(served(run), null), canon(served(before), null),
            "a pending swap changes nothing WAGED computes: " + run);
      }

      swapIn.operation = InstanceConstants.InstanceOperation.ENABLE;
      sim.nodes.get(out).operation = InstanceConstants.InstanceOperation.UNKNOWN;
      List<Run> completed = looseStep(sim, isolation, broken);
      runs.addAll(completed);
      Run last = last(completed);
      if (isolation || !breakA || tag.equals("A")) {
        // The whole served map is the pre-swap one with the swap-out node renamed.
        Assert.assertEquals(canon(served(last), null),
            canon(renamed(served(before), out, in), null), "the swap moved replicas: " + last);
      } else {
        // Stock keeps the healthy clique served by logical id as well, from a stuck baseline.
        Assert.assertEquals(canon(served(last), sim.resourcesOf(tag)),
            canon(renamed(served(before), out, in), sim.resourcesOf(tag)), last.toString());
      }
      if (breakA && isolation) {
        Assert.assertEquals(ScopeMatrixSim.canon(last.baselineAfter.get("RA")),
            ScopeMatrixSim.canon(before.baselineAfter.get("RA")),
            "the carried baseline still names the swap-out node: " + last);
      }
      return runs;
    }
  }

  /** The map with one instance renamed. */
  static Map<String, ResourceAssignment> renamed(Map<String, ResourceAssignment> map, String from,
      String to) {
    Map<String, ResourceAssignment> out = new java.util.HashMap<>();
    map.forEach((resource, assignment) -> {
      ResourceAssignment copy = new ResourceAssignment(resource);
      for (Partition partition : assignment.getMappedPartitions()) {
        Map<String, String> replicas = new java.util.HashMap<>();
        assignment.getReplicaMap(partition)
            .forEach((node, state) -> replicas.put(node.equals(from) ? to : node, state));
        copy.addReplicaMap(partition, replicas);
      }
      out.put(resource, copy);
    });
    return out;
  }

  // ---------------------------------------------------------------- lifecycle

  @DataProvider(name = "syncAndAsync")
  public Object[][] syncAndAsync() {
    return new Object[][] {{false}, {true}};
  }

  @DataProvider(name = "breaksSyncAndAsync")
  public Object[][] breaksSyncAndAsync() {
    List<Object[]> rows = new ArrayList<>();
    for (Break breakMode : Break.values()) {
      for (boolean async : new boolean[] {false, true}) {
        rows.add(new Object[] {breakMode, async});
      }
    }
    return rows.toArray(new Object[0][]);
  }

  /**
   * A and C break in one event, then a B node shrinks. Flag on, the full baseline carries both
   * broken cliques, fits B under the new capacity, and retries and counts both. Flag off, the
   * baseline fails and B stays over capacity.
   */
  @Test(dataProvider = "syncAndAsync")
  public void testSeveralBrokenCliquesAreCarriedTogether(boolean async) throws Exception {
    for (boolean isolation : new boolean[] {true, false}) {
      try (ScopeMatrixSim sim = sim("severalBroken", isolation, async, async)) {
        Set<String> b = sim.resourcesOf("B");
        bootstrap(sim);
        sim.resources.get("RA").partitionWeights.put("RA_0", 150);
        sim.resources.get("RC").partitionWeights.put("RC_0", 150);
        Run broken = last(looseStep(sim, isolation, tags("A", "C")));
        sim.nodes.get("b0").disk = 20;
        List<Run> runs = looseStep(sim, isolation, tags("A", "C"));
        Calc baseline = only(runs.get(0).calcs(RebalanceScopeType.GLOBAL_BASELINE));
        Run last = last(runs);
        if (isolation) {
          Assert.assertEquals(broken.retry, set("RA", "RC"), broken.toString());
          Assert.assertEquals(broken.gauge, 2L, broken.toString());
          Assert.assertEquals(baseline.skipped, set("RA", "RC"), runs.get(0).toString());
          Assert.assertEquals(last.retry, set("RA", "RC"), last.toString());
          Assert.assertEquals(last.gauge, 2L, last.toString());
          Assert.assertTrue(load(sim, last.baselineAfter, b, "b0") <= 20, last.toString());
          Assert.assertTrue(load(sim, served(last), b, "b0") <= 20, last.toString());
        } else {
          Assert.assertNotNull(baseline.failure, runs.get(0).toString());
          Assert.assertTrue(load(sim, served(last), b, "b0") > 20, last.toString());
        }
      }
    }
  }

  /**
   * Every clique breaks in one event, by either break mode. Nothing healthy is left to protect, so
   * the flag on rebalancer must fail exactly as the flag off one: the same exception type,
   * category and message all the way down the cause chain, nothing persisted, the same thing
   * served. The retry set keeps every resource queued and the gauge stays at zero.
   */
  @Test(dataProvider = "breaksSyncAndAsync")
  public void testEveryCliqueBrokenFailsExactlyLikeStock(Break breakMode, boolean async)
      throws Exception {
    List<Object> outcomes = new ArrayList<>();
    for (boolean isolation : new boolean[] {true, false}) {
      try (ScopeMatrixSim sim = sim("allBroken", isolation, async, async)) {
        bootstrap(sim);
        for (String resource : Arrays.asList("RA", "RB1", "RC")) {
          ResourceSpec spec = sim.resources.get(resource);
          if (breakMode == Break.TAG_LOCAL) {
            // Heavier than any node, while the cluster as a whole still has room.
            spec.partitionWeights.put(resource + "_0", 110);
          } else {
            spec.partitions = 20;
            spec.weight = 40;
          }
        }
        Run run = sim.run();
        Calc baseline = only(run.calcs(RebalanceScopeType.GLOBAL_BASELINE));
        assertCategory(baseline, breakMode);
        Assert.assertEquals(canon(run.baselineAfter, null), canon(run.baselineBefore, null),
            run.toString());
        // In asynchronous mode the pipeline's own partial still runs on the old baseline's
        // partitions. With DEFICIT only A and C are short there, since B's old partitions still
        // fit B, so the flag on partial isolates those two instead of failing like stock.
        Assert.assertEquals(run.gauge,
            isolation ? run.skippedIn(RebalanceScopeType.PARTIAL).size() : 0L, run.toString());
        Assert.assertEquals(run.retry, isolation ? sim.resources.keySet() : none(),
            run.toString());
        Assert.assertEquals(run.computeFailure != null, !async, run.toString());
        outcomes.add(Arrays.asList(describe(sim, baseline.failure),
            describe(sim, run.computeFailure), describe(sim, run.pipelineFailure),
            run.emitted == null, canon(served(run), null), canon(run.bestAfter, null)));
      }
    }
    Assert.assertEquals(outcomes.get(0), outcomes.get(1));
  }

  /**
   * Every clique breaks in the emergency: each clique keeps one live node for two replicas, so no
   * clique can move the replicas of its dead node anywhere, while the cluster as a whole has room.
   * The flag on emergency must fail exactly as the flag off one.
   */
  @Test(dataProvider = "syncAndAsync")
  public void testEveryCliqueBrokenInTheEmergencyFailsExactlyLikeStock(boolean async)
      throws Exception {
    List<Object> outcomes = new ArrayList<>();
    for (boolean isolation : new boolean[] {true, false}) {
      try (ScopeMatrixSim sim = sim("allBrokenEmergency", isolation, async, async)) {
        for (String node : Arrays.asList("a2", "b2", "b3", "c2")) {
          sim.nodes.remove(node);
        }
        sim.nodes.values().forEach(node -> node.disk = 200);
        bootstrap(sim);
        for (String node : Arrays.asList("a1", "b1", "c1")) {
          sim.offline(node, false);
        }
        Run run = sim.run();
        Assert.assertTrue(run.calcs(RebalanceScopeType.GLOBAL_BASELINE).isEmpty(),
            run.toString());
        Calc emergency = only(run.calcs(RebalanceScopeType.EMERGENCY));
        Assert.assertEquals(emergency.toAssign, sim.resources.keySet(), run.toString());
        Assert.assertNotNull(emergency.failure, run.toString());
        Assert.assertEquals(emergency.failure.getFailureCategory(),
            HelixRebalanceException.FailureCategory.NO_CANDIDATE_NODE, run.toString());
        Assert.assertNotNull(run.computeFailure, run.toString());
        Assert.assertEquals(run.gauge, 0L, run.toString());
        Assert.assertTrue(run.retry.isEmpty(), run.toString());
        outcomes.add(Arrays.asList(describe(sim, emergency.failure),
            describe(sim, run.computeFailure), describe(sim, run.pipelineFailure),
            run.emitted == null, canon(served(run), null), canon(run.bestAfter, null)));
      }
    }
    Assert.assertEquals(outcomes.get(0), outcomes.get(1));
  }

  /** How every clique is broken from the first pipeline on. */
  enum BootstrapBreak {
    /**
     * Every partition weighs 60, so every clique is short of its own capacity and the cluster
     * wide deficit cannot be attributed to some cliques only.
     */
    EVERY_CLIQUE_SHORT,
    /**
     * RA's partitions weigh 200 while B and C fit in aggregate, so the cluster wide deficit is
     * attributed to A. RB1 and RC then have one partition each of 110, heavier than any node, so
     * B and C fail on placement after A.
     */
    DEFICIT_THEN_NO_CANDIDATE
  }

  @DataProvider(name = "modesAndBootstrapBreaks")
  public Object[][] modesAndBootstrapBreaks() {
    List<Object[]> rows = new ArrayList<>();
    for (Object[] mode : modes()) {
      for (BootstrapBreak shape : BootstrapBreak.values()) {
        rows.add(new Object[] {mode[0], mode[1], shape});
      }
    }
    return rows.toArray(new Object[0][]);
  }

  /**
   * Every clique is broken from the first pipeline on, so there is no healthy clique to protect
   * and the flag on rebalancer must fail exactly as the flag off one on each of three pipelines.
   * Compared are every calculation's scope, resources, skipped resources and failure, the
   * computation and pipeline failures (type, category and message down the cause chain), whether
   * anything was emitted, the persisted and served maps, the isolation gauge, and the WAGED
   * failure counters and gauges. The flag off baseline fails with CAPACITY_DEFICIT in both
   * shapes, so the flag on one must report that category too, not the failure of the last clique
   * it tried, since the category picks the failure counters the monitor ticks. The flag on retry
   * set keeps every resource queued.
   */
  @Test(dataProvider = "modesAndBootstrapBreaks")
  public void testEveryCliqueBrokenAtBootstrapFailsExactlyLikeStock(boolean asyncGlobal,
      boolean asyncPartial, BootstrapBreak shape) throws Exception {
    try (ScopeMatrixSim isolated = carrySim("bootstrapBroken", true, asyncGlobal, asyncPartial);
        ScopeMatrixSim stock = carrySim("bootstrapBroken", false, asyncGlobal, asyncPartial)) {
      for (ScopeMatrixSim sim : new ScopeMatrixSim[] {isolated, stock}) {
        if (shape == BootstrapBreak.EVERY_CLIQUE_SHORT) {
          sim.resources.values().forEach(resource -> resource.weight = 60);
        } else {
          sim.resources.get("RA").weight = 200;
          for (String resource : Arrays.asList("RB1", "RC")) {
            sim.resources.get(resource).partitions = 1;
            sim.resources.get(resource).weight = 110;
          }
        }
      }
      List<Object> isolatedOutcomes = new ArrayList<>();
      List<Object> stockOutcomes = new ArrayList<>();
      for (int i = 0; i < 3; i++) {
        Run isolatedRun = isolated.run();
        Assert.assertEquals(isolatedRun.retry, isolated.resources.keySet(),
            isolatedRun.toString());
        isolatedOutcomes.add(outcome(isolated, isolatedRun));
        Run stockRun = stock.run();
        if (i == 0) {
          Calc baseline = only(stockRun.calcs(RebalanceScopeType.GLOBAL_BASELINE));
          Assert.assertNotNull(baseline.failure, stockRun.toString());
          Assert.assertEquals(baseline.failure.getFailureType(),
              HelixRebalanceException.Type.FAILED_TO_CALCULATE, stockRun.toString());
          Assert.assertEquals(baseline.failure.getFailureCategory(),
              HelixRebalanceException.FailureCategory.CAPACITY_DEFICIT, stockRun.toString());
        }
        stockOutcomes.add(outcome(stock, stockRun));
      }
      Assert.assertEquals(isolatedOutcomes, stockOutcomes);
    }
  }

  /** What one run did, in a form a flag on and a flag off simulation can be compared by. */
  private static List<Object> outcome(ScopeMatrixSim sim, Run run) {
    List<Object> outcome = new ArrayList<>();
    for (Calc calc : run.calcs) {
      outcome.add(
          Arrays.asList(calc.scope, calc.toAssign, calc.skipped, describe(sim, calc.failure)));
    }
    outcome.add(describe(sim, run.computeFailure));
    outcome.add(describe(sim, run.pipelineFailure));
    outcome.add(run.emitted == null);
    outcome.add(canon(run.baselineAfter, null));
    outcome.add(canon(run.bestAfter, null));
    outcome.add(canon(served(run), null));
    outcome.add(run.gauge);
    outcome.add(failureMetrics(sim.monitor));
    return outcome;
  }

  /** The WAGED failure counters and gauges, by category or name. */
  private static Map<String, Long> failureMetrics(ClusterStatusMonitor monitor) {
    Map<String, Long> metrics = new LinkedHashMap<>();
    metrics.put("CAPACITY_DEFICIT", monitor.getWagedFailureCapacityDeficitCounter());
    metrics.put("NO_CANDIDATE_NODE", monitor.getWagedFailureNoCandidateNodeCounter());
    metrics.put("INVALID_RESOURCE_CONFIG", monitor.getWagedFailureInvalidResourceConfigCounter());
    metrics.put("INVALID_CLUSTER_CONFIG", monitor.getWagedFailureInvalidClusterConfigCounter());
    metrics.put("METADATA_STORE_IO", monitor.getWagedFailureMetadataStoreIoCounter());
    metrics.put("ALGORITHM_INTERNAL", monitor.getWagedFailureAlgorithmInternalCounter());
    metrics.put("ASYNC_EXECUTION", monitor.getWagedFailureAsyncExecutionCounter());
    metrics.put("UNKNOWN", monitor.getWagedFailureUnknownCounter());
    metrics.put("customerActionable", monitor.getWagedCustomerActionableFailureCounter());
    metrics.put("internal", monitor.getWagedInternalFailureCounter());
    metrics.put("customerActionableGauge", monitor.getWagedCustomerActionableFailureGauge());
    metrics.put("internalGauge", monitor.getWagedInternalFailureGauge());
    metrics.put("baselineComputeFailing", monitor.getWagedBaselineComputeFailingGauge());
    metrics.put("rebalanceOverwriteFailing", monitor.getWagedRebalanceOverwriteFailingGauge());
    metrics.put("fallbackInUse", monitor.getWagedFallbackInUseGauge());
    return metrics;
  }

  /**
   * Type, category and message of an exception and of every cause below it, with the cluster
   * name taken out and the per node fail reasons sorted, since both differ between two
   * simulations (the reasons are listed in hash order of the node objects).
   */
  static List<Object> describe(ScopeMatrixSim sim, Throwable failure) {
    List<Object> out = new ArrayList<>();
    for (Throwable t = failure; t != null; t = t.getCause()) {
      out.add(t.getClass().getName());
      if (t instanceof HelixRebalanceException) {
        out.add(((HelixRebalanceException) t).getFailureType());
        out.add(((HelixRebalanceException) t).getFailureCategory());
      }
      out.add(t.getMessage() == null ? null
          : sortReasons(t.getMessage().replace(sim.cluster, "<cluster>")));
    }
    return out;
  }

  private static final Pattern INNER_MAP = Pattern.compile("\\{([^{}]*)\\}");

  private static String sortReasons(String message) {
    Matcher matcher = INNER_MAP.matcher(message);
    StringBuffer sorted = new StringBuffer();
    while (matcher.find()) {
      List<String> entries =
          new ArrayList<>(Arrays.asList(matcher.group(1).split(", (?=[^,\\[\\]]+=\\[)")));
      Collections.sort(entries);
      matcher.appendReplacement(sorted,
          Matcher.quoteReplacement("{" + String.join(", ", entries) + "}"));
    }
    matcher.appendTail(sorted);
    return sorted.toString();
  }

  /** The config change that repairs A. */
  enum Repair {
    RESOURCE_CONFIG, INSTANCE_CONFIG, CLUSTER_CONFIG
  }

  @DataProvider(name = "repairs")
  public Object[][] repairs() {
    List<Object[]> rows = new ArrayList<>();
    for (Repair repair : Repair.values()) {
      for (boolean async : new boolean[] {false, true}) {
        rows.add(new Object[] {repair, async});
      }
    }
    return rows.toArray(new Object[0][]);
  }

  /**
   * A is broken by one heavy partition and repaired by one config change of each kind: the
   * partition's weight goes back down, A's nodes grow, or the cluster default capacity that only
   * A's nodes use grows. The repair must land on that very event: the baseline evaluates RA, skips
   * nothing, the retry set and the gauge drop to empty and RA is served fresh. A cluster config
   * change the change detector trims, beforehand, must neither run a baseline nor clear anything.
   */
  @Test(dataProvider = "repairs")
  public void testBrokenCliqueRecoversOnTheRepairingEvent(Repair repair, boolean async)
      throws Exception {
    try (ScopeMatrixSim sim = sim("repair", true, async, async)) {
      for (String node : Arrays.asList("a0", "a1", "a2")) {
        sim.nodes.get(node).disk = null;
      }
      bootstrap(sim);
      sim.resources.get("RA").partitionWeights.put("RA_0", 150);
      Run broken = last(looseStep(sim, true, tags("A")));
      Assert.assertEquals(broken.retry, set("RA"), broken.toString());
      Assert.assertEquals(broken.gauge, 1L, broken.toString());

      sim.clusterConfig.setMaxConcurrentTaskPerInstance(7);
      Run trimmed = sim.run();
      Assert.assertTrue(trimmed.calcs(RebalanceScopeType.GLOBAL_BASELINE).isEmpty(),
          trimmed.toString());
      Assert.assertEquals(trimmed.retry, set("RA"), trimmed.toString());
      Assert.assertEquals(trimmed.gauge, 1L, trimmed.toString());

      switch (repair) {
        case RESOURCE_CONFIG:
          sim.resources.get("RA").partitionWeights.remove("RA_0");
          break;
        case INSTANCE_CONFIG:
          for (String node : Arrays.asList("a0", "a1", "a2")) {
            sim.nodes.get(node).disk = 200;
          }
          break;
        default:
          sim.clusterConfig.setDefaultInstanceCapacityMap(
              Collections.singletonMap(ScopeMatrixSim.DISK, 200));
      }
      List<Run> runs = looseStep(sim, true, none());
      Run event = runs.get(0);
      Calc baseline = only(event.calcs(RebalanceScopeType.GLOBAL_BASELINE));
      Assert.assertTrue(baseline.toAssign.contains("RA"), event.toString());
      Assert.assertNull(baseline.failure, event.toString());
      Assert.assertTrue(baseline.skipped.isEmpty(), event.toString());
      Assert.assertTrue(event.retry.isEmpty(), event.toString());
      Assert.assertEquals(event.gauge, 0L, event.toString());
      sim.assertFresh("served", served(last(runs)), "RA", sim.servingNodes("A"), false);
    }
  }

  /**
   * A carried resource is deleted while a block mate in its clique is healthy but has a pending
   * change. The block mate is carried with it while RA is broken. Deleting RA is itself a relevant
   * change, so the very next baseline must drop RA from every map, the retry set and the gauge,
   * and land the block mate's pending change.
   */
  @Test(dataProvider = "syncAndAsync")
  public void testDeletingTheBrokenResourceRecoversItsBlockMate(boolean async) throws Exception {
    try (ScopeMatrixSim sim = sim("deleteCarried", true, async, async)) {
      sim.resource("RA2", "A", 4, 10);
      bootstrap(sim);
      sim.resources.get("RA").partitionWeights.put("RA_0", 150);
      sim.resources.get("RA2").weight = 12;
      Run broken = last(looseStep(sim, true, tags("A")));
      Assert.assertEquals(broken.retry, set("RA", "RA2"), broken.toString());
      Assert.assertEquals(broken.gauge, 2L, broken.toString());

      sim.resources.remove("RA");
      List<Run> runs = looseStep(sim, true, none());
      Run event = runs.get(0);
      Calc baseline = only(event.calcs(RebalanceScopeType.GLOBAL_BASELINE));
      Assert.assertTrue(baseline.toAssign.contains("RA2"), event.toString());
      Assert.assertFalse(baseline.toAssign.contains("RA"), event.toString());
      Assert.assertTrue(baseline.skipped.isEmpty(), event.toString());
      Assert.assertTrue(event.retry.isEmpty(), event.toString());
      Run last = last(runs);
      Assert.assertEquals(last.gauge, 0L, last.toString());
      for (Map<String, ResourceAssignment> map : Arrays.asList(last.baselineAfter,
          last.bestAfter, last.emitted)) {
        Assert.assertFalse(map.containsKey("RA"), last.toString());
      }
      Assert.assertEquals(sim.computedWeights.get("RA2").get("RA2_0"), Integer.valueOf(12));
    }
  }

  /**
   * The flag is turned off while A is carried. The flip is a cluster config change the change
   * detector keeps, so its own full baseline runs with the flag off and must be stock at once: it
   * fails on A exactly as a cluster that never had the flag fails on the same kind of event, the
   * gauge and the retry set are empty on that event, and nothing is persisted.
   */
  @Test(dataProvider = "syncAndAsync")
  public void testFlagTurnedOffWhileCarriedIsStockAtOnce(boolean async) throws Exception {
    List<Object> outcomes = new ArrayList<>();
    for (boolean everOn : new boolean[] {true, false}) {
      try (ScopeMatrixSim sim = sim("flagOff", everOn, async, async)) {
        bootstrap(sim);
        sim.resources.get("RA").partitionWeights.put("RA_0", 150);
        Run broken = last(looseStep(sim, everOn, tags("A")));
        if (everOn) {
          Assert.assertEquals(broken.retry, set("RA"), broken.toString());
          Assert.assertEquals(broken.gauge, 1L, broken.toString());
          sim.setIsolation(false);
        } else {
          sim.clusterConfig.setMaxPartitionsPerInstance(1000);
        }
        Run flip = sim.run();
        assertStock(flip);
        Calc baseline = only(flip.calcs(RebalanceScopeType.GLOBAL_BASELINE));
        Assert.assertTrue(baseline.toAssign.containsAll(sim.resources.keySet()), flip.toString());
        assertCategory(baseline, Break.TAG_LOCAL);
        Assert.assertEquals(canon(flip.baselineAfter, null), canon(flip.baselineBefore, null),
            flip.toString());
        outcomes.add(Arrays.asList(describe(sim, baseline.failure),
            describe(sim, flip.computeFailure), canon(flip.baselineAfter, null),
            canon(served(flip), null)));
        Run after = sim.run();
        assertStock(after);
      }
    }
    Assert.assertEquals(outcomes.get(0), outcomes.get(1));
  }

  /**
   * The flag is turned on in a cluster that is already failing on A, with B's work stuck behind
   * it. The flip is itself a relevant cluster config change, so its own full baseline must isolate
   * at once: A is skipped, B lands, and A is retried and counted.
   */
  @Test(dataProvider = "syncAndAsync")
  public void testFlagTurnedOnInAFailingClusterIsolatesOnTheFlip(boolean async)
      throws Exception {
    try (ScopeMatrixSim sim = sim("flagOn", false, async, async)) {
      Set<String> b = sim.resourcesOf("B");
      bootstrap(sim);
      sim.resources.get("RA").partitionWeights.put("RA_0", 150);
      looseStep(sim, false, tags("A"));
      sim.nodes.get("b0").disk = 20;
      Run stuck = last(looseStep(sim, false, tags("A")));
      Assert.assertTrue(load(sim, served(stuck), b, "b0") > 20, stuck.toString());

      sim.setIsolation(true);
      List<Run> runs = looseStep(sim, true, tags("A"));
      Run flip = runs.get(0);
      Calc baseline = only(flip.calcs(RebalanceScopeType.GLOBAL_BASELINE));
      Assert.assertTrue(baseline.toAssign.containsAll(sim.resources.keySet()), flip.toString());
      Assert.assertNull(baseline.failure, flip.toString());
      Assert.assertEquals(baseline.skipped, set("RA"), flip.toString());
      Assert.assertEquals(flip.retry, set("RA"), flip.toString());
      Run last = last(runs);
      Assert.assertTrue(load(sim, last.baselineAfter, b, "b0") <= 20, last.toString());
      Assert.assertTrue(load(sim, served(last), b, "b0") <= 20, last.toString());
      Assert.assertEquals(last.gauge, 1L, last.toString());
    }
  }

  // ---------------------------------------------------------------- audits

  /**
   * The only change that clears A's failure is one the change detector trims. The
   * detector keeps the key of the disabled partition map but not its values, so a0 and a1 carry an
   * unrelated disabled partition from the start, and disabling and re-enabling RA_0 on them is
   * invisible to it. RA grows while RA_0 is disabled on two of A's three nodes, so the incremental
   * baseline cannot place RA_0 twice and carries A at its old size; the partial only places
   * partitions the baseline has, so RA is served at its old size too. Re-enabling RA_0 triggers no
   * baseline, so RA stays at its old size, and the retry set and the gauge keep naming it, which
   * is accurate since its growth has not been applied. Nothing is stuck: the next relevant change
   * of any resource retries RA, grows it and clears both. That is the same latency as stock, which
   * fails the whole round when RA grows and places RA's missing partitions on the next baseline,
   * since a baseline always assigns the replicas it lacks.
   */
  @Test
  public void testRetryWaitsForTheNextTriggerWhenATrimmedChangeClearsTheCause()
      throws Exception {
    for (boolean isolation : new boolean[] {true, false}) {
      try (ScopeMatrixSim sim = sim("trimmedCause", isolation, false, false)) {
        for (String node : Arrays.asList("a0", "a1")) {
          sim.nodes.get(node).disabledPartitions.put("RX", Collections.singletonList("RX_0"));
        }
        bootstrap(sim);
        for (String node : Arrays.asList("a0", "a1")) {
          sim.nodes.get(node).disabledPartitions.put("RA", Collections.singletonList("RA_0"));
        }
        Run disabled = sim.run();
        Assert.assertTrue(disabled.calcs(RebalanceScopeType.GLOBAL_BASELINE).isEmpty(),
            disabled.toString());

        sim.resources.get("RA").partitions = 6;
        Run grown = sim.run();
        Calc baseline = only(grown.calcs(RebalanceScopeType.GLOBAL_BASELINE));
        Assert.assertEquals(baseline.toAssign, set("RA"), grown.toString());
        Assert.assertEquals(partitions(grown.baselineAfter, "RA"), 4, grown.toString());
        if (isolation) {
          Assert.assertEquals(baseline.skipped, set("RA"), grown.toString());
          Assert.assertEquals(grown.retry, set("RA"), grown.toString());
          Assert.assertEquals(grown.gauge, 1L, grown.toString());
        } else {
          Assert.assertEquals(baseline.failure.getFailureCategory(),
              HelixRebalanceException.FailureCategory.NO_CANDIDATE_NODE, grown.toString());
        }

        Assert.assertEquals(partitions(served(grown), "RA"), 4, grown.toString());
        for (String node : Arrays.asList("a0", "a1")) {
          sim.nodes.get(node).disabledPartitions.remove("RA");
        }
        Run enabled = sim.run();
        Assert.assertTrue(enabled.calcs(RebalanceScopeType.GLOBAL_BASELINE).isEmpty(),
            enabled.toString());
        Assert.assertEquals(enabled.retry, isolation ? set("RA") : none(), enabled.toString());
        Assert.assertEquals(enabled.gauge, isolation ? 1L : 0L, enabled.toString());
        Assert.assertEquals(partitions(served(enabled), "RA"), 4, enabled.toString());
        Assert.assertEquals(partitions(enabled.baselineAfter, "RA"), 4, enabled.toString());

        sim.resources.get("RC").weight = 11;
        Run next = sim.run();
        Calc retried = only(next.calcs(RebalanceScopeType.GLOBAL_BASELINE));
        Assert.assertEquals(retried.toAssign, set("RA", "RC"), next.toString());
        Assert.assertNull(retried.failure, next.toString());
        Assert.assertEquals(partitions(next.baselineAfter, "RA"), 6, next.toString());
        Assert.assertEquals(partitions(served(next), "RA"), 6, next.toString());
        Assert.assertTrue(next.retry.isEmpty(), next.toString());
        Assert.assertEquals(next.gauge, 0L, next.toString());
        assertBookkeeping(sim, next);
      }
    }
  }

  /**
   * A failure only a live instance change can clear. A cannot absorb a2's replicas when
   * a2 goes down, so the emergency and the partial carry A with its replicas still on a2, and no
   * baseline runs because liveness is not a baseline input. When a2 comes back only a live
   * instance change happens: the emergency has nothing left to do and reports an empty phase, the
   * partial has nothing to move, and the gauge drops to zero on that very event.
   */
  @Test(dataProvider = "syncAndAsync")
  public void testCliqueCarriedForADeadNodeRecoversWhenTheNodeReturns(boolean async)
      throws Exception {
    try (ScopeMatrixSim sim = sim("nodeReturns", true, async, async)) {
      sim.resources.get("RA").weight = 30;
      bootstrap(sim);
      sim.offline("a2", false);
      Run down = sim.run();
      assertBookkeeping(sim, down);
      Assert.assertEquals(only(down.calcs(RebalanceScopeType.EMERGENCY)).skipped, set("RA"),
          down.toString());
      Assert.assertEquals(down.gauge, 1L, down.toString());
      Assert.assertTrue(down.retry.isEmpty(), down.toString());
      Assert.assertTrue(uses(served(down), set("RA"), "a2"), down.toString());

      sim.online("a2");
      Run back = sim.run();
      assertBookkeeping(sim, back);
      Assert.assertTrue(back.calcs(RebalanceScopeType.GLOBAL_BASELINE).isEmpty(),
          back.toString());
      Assert.assertTrue(back.calcs(RebalanceScopeType.EMERGENCY).isEmpty(), back.toString());
      Assert.assertTrue(back.reports.stream().anyMatch(
          r -> r.scope == RebalanceScopeType.EMERGENCY && isEmpty(r)), back.toString());
      Assert.assertEquals(back.gauge, 0L, back.toString());
      verify(sim, back, none(), none(), false);
    }
  }

  /**
   * Two baseline submissions queue on the single baseline thread before the first runs.
   * The first carries A and leaves RA to retry; the second must pick that retry up, evaluate RA
   * again alongside its own change and land the healthy change.
   */
  @Test
  public void testBackToBackAsyncBaselinesChainTheRetrySet() throws Exception {
    try (ScopeMatrixSim sim = sim("backToBack", true, true, false)) {
      bootstrap(sim);
      CountDownLatch gate = sim.holdBaselineWorker();
      int calcStart = sim.algorithm.calcs.size();
      sim.resources.get("RA").partitionWeights.put("RA_0", 150);
      sim.run(false);
      sim.resources.get("RB1").weight = 12;
      sim.run(false);
      gate.countDown();
      sim.drain();
      List<Calc> baselines = new ArrayList<>();
      for (Calc calc : sim.algorithm.calcs.subList(calcStart, sim.algorithm.calcs.size())) {
        if (calc.scope == RebalanceScopeType.GLOBAL_BASELINE) {
          baselines.add(calc);
        }
      }
      Assert.assertEquals(baselines.size(), 2, baselines.toString());
      Assert.assertEquals(baselines.get(0).toAssign, set("RA"), baselines.toString());
      Assert.assertEquals(baselines.get(0).skipped, set("RA"), baselines.toString());
      Assert.assertEquals(baselines.get(1).toAssign, set("RA", "RB1"), baselines.toString());
      Assert.assertEquals(baselines.get(1).skipped, set("RA"), baselines.toString());
      Assert.assertEquals(sim.retrySet(), set("RA"));
      Run settled = last(looseStep(sim, true, tags("A")));
      Assert.assertEquals(settled.gauge, 1L, settled.toString());
      sim.assertFresh("baseline", settled.baselineAfter, "RB1", sim.baselineNodes("B"), false);
    }
  }

  /** How a baseline round fails as a whole. */
  enum WholeFailure {
    PERSIST, CALCULATION
  }

  @DataProvider(name = "wholeFailures")
  public Object[][] wholeFailures() {
    List<Object[]> rows = new ArrayList<>();
    for (WholeFailure failure : WholeFailure.values()) {
      for (boolean async : new boolean[] {false, true}) {
        rows.add(new Object[] {failure, async});
      }
    }
    return rows.toArray(new Object[0][]);
  }

  /**
   * A baseline round with A carried fails as a whole while RB1 changes. Either the store
   * rejects the write, with RB1 grown so the baseline surely changes and the write is attempted,
   * or the calculation throws, with RB1's weight doubled. Flag on, every resource stays queued for
   * retry, so the next relevant event re-evaluates everything, carries A again and applies RB1's
   * change. Flag off, the failed round's change is consumed: a growth is still picked up, since
   * every baseline assigns the replicas it lacks, but the weight change is lost and RB1 keeps the
   * placement computed at its old weight.
   */
  @Test(dataProvider = "wholeFailures")
  public void testWholeBaselineFailureKeepsEveryResourceQueued(WholeFailure failure,
      boolean async) throws Exception {
    for (boolean isolation : new boolean[] {true, false}) {
      try (ScopeMatrixSim sim = sim("wholeFailure", isolation, async, async)) {
        bootstrap(sim);
        sim.resources.get("RA").partitionWeights.put("RA_0", 150);
        Run broken = last(looseStep(sim, isolation, tags("A")));
        if (failure == WholeFailure.PERSIST) {
          sim.store.failBaselinePersist = true;
          sim.resources.get("RB1").partitions = 6;
        } else {
          sim.algorithm.failScope = RebalanceScopeType.GLOBAL_BASELINE;
          sim.algorithm.failWith = new IllegalStateException("Injected calculation failure");
          sim.resources.get("RB1").weight = 20;
        }
        Run failed = sim.run();
        sim.store.failBaselinePersist = false;
        sim.algorithm.failScope = null;
        Calc baseline = only(failed.calcs(RebalanceScopeType.GLOBAL_BASELINE));
        Assert.assertEquals(baseline.toAssign, isolation ? set("RA", "RB1") : set("RB1"),
            failed.toString());
        Assert.assertEquals(canon(failed.baselineAfter, null), canon(failed.baselineBefore, null),
            failed.toString());
        Assert.assertEquals(failed.retry, isolation ? sim.resources.keySet() : none(),
            failed.toString());
        Assert.assertEquals(failed.computeFailure != null, !async, failed.toString());

        sim.resources.get("RC").weight = 11;
        List<Run> runs = looseStep(sim, isolation, tags("A"));
        Calc retried = only(runs.get(0).calcs(RebalanceScopeType.GLOBAL_BASELINE));
        Run last = last(runs);
        if (isolation) {
          Assert.assertTrue(retried.toAssign.containsAll(sim.resources.keySet()),
              runs.get(0).toString());
          Assert.assertEquals(retried.skipped, set("RA"), runs.get(0).toString());
          Assert.assertEquals(last.retry, set("RA"), last.toString());
          Assert.assertEquals(last.gauge, 1L, last.toString());
        } else if (failure == WholeFailure.PERSIST) {
          Assert.assertEquals(retried.toAssign, set("RB1", "RC"), runs.get(0).toString());
        } else {
          Assert.assertEquals(retried.toAssign, set("RC"), runs.get(0).toString());
          Assert.assertEquals(canon(last.baselineAfter, set("RB1")),
              canon(broken.baselineAfter, set("RB1")), last.toString());
        }
        int size = sim.resources.get("RB1").partitions;
        Assert.assertEquals(partitions(last.baselineAfter, "RB1"), size, last.toString());
        Assert.assertEquals(partitions(served(last), "RB1"), size, last.toString());
      }
    }
  }

  /**
   * The rebalancer is reset while a baseline calculation is in flight. The stale
   * calculation must neither restore its retry set nor publish to the gauge after the reset; the
   * next pipeline runs a full baseline, which carries A again and rebuilds both.
   */
  @Test
  public void testResetDuringAnInFlightBaselineCannotRestoreStaleState() throws Exception {
    try (ScopeMatrixSim sim = sim("reset", true, true, false)) {
      bootstrap(sim);
      sim.resources.get("RA").partitionWeights.put("RA_0", 150);
      Run broken = last(looseStep(sim, true, tags("A")));
      Assert.assertEquals(broken.gauge, 1L, broken.toString());
      sim.algorithm.pauseScope = RebalanceScopeType.GLOBAL_BASELINE;
      sim.algorithm.pausedSignal = new CountDownLatch(1);
      sim.algorithm.pauseGate = new CountDownLatch(1);
      sim.resources.get("RB1").weight = 12;
      sim.run(false);
      Assert.assertTrue(sim.algorithm.pausedSignal.await(30, TimeUnit.SECONDS));
      Assert.assertEquals(sim.retrySet(), sim.resources.keySet());
      sim.reset();
      Assert.assertTrue(sim.retrySet().isEmpty());
      Assert.assertEquals(sim.monitor.getWagedInstanceTagIsolationSkippedResourcesGauge(), 0L);
      sim.algorithm.pauseGate.countDown();
      sim.algorithm.pauseGate = null;
      sim.drain();
      Assert.assertTrue(sim.retrySet().isEmpty(), "the stale calculation restored its retry set");
      Assert.assertEquals(sim.monitor.getWagedInstanceTagIsolationSkippedResourcesGauge(), 0L,
          "the stale calculation published after the reset");

      List<Run> runs = looseStep(sim, true, tags("A"));
      Calc full = only(runs.get(0).calcs(RebalanceScopeType.GLOBAL_BASELINE));
      Assert.assertTrue(full.toAssign.containsAll(sim.resources.keySet()), runs.toString());
      Assert.assertEquals(full.skipped, set("RA"), runs.toString());
      Run last = last(runs);
      Assert.assertEquals(last.retry, set("RA"), last.toString());
      Assert.assertEquals(last.gauge, 1L, last.toString());
    }
  }

  @DataProvider(name = "flipDirections")
  public Object[][] flipDirections() {
    return new Object[][] {{false}, {true}};
  }

  /**
   * The flag flips while a baseline calculation started under the old value is still in
   * flight on the asynchronous worker. Every calculation reads and writes the retry set on that one
   * worker thread, and the gauge only takes reports of the current flag generation, so neither
   * direction leaks. Turned off, the gauge reads zero from the flip on, the late calculation cannot
   * publish when it lands, and the flip's own baseline, queued behind it, leaves no retry behind.
   * Turned on, the flip's own baseline runs after the late flag off one, isolates A and publishes.
   */
  @Test(dataProvider = "flipDirections")
  public void testFlagFlipDuringAnInFlightBaselineTakesEffectAtOnce(boolean turnOn)
      throws Exception {
    try (ScopeMatrixSim sim = sim("flipInFlight", !turnOn, true, false)) {
      bootstrap(sim);
      sim.resources.get("RA").partitionWeights.put("RA_0", 150);
      looseStep(sim, !turnOn, tags("A"));
      sim.algorithm.pauseScope = RebalanceScopeType.GLOBAL_BASELINE;
      sim.algorithm.pausedSignal = new CountDownLatch(1);
      sim.algorithm.pauseGate = new CountDownLatch(1);
      sim.resources.get("RB1").weight = 12;
      sim.run(false);
      Assert.assertTrue(sim.algorithm.pausedSignal.await(30, TimeUnit.SECONDS));
      sim.setIsolation(turnOn);
      Run flip = sim.run(false);
      if (!turnOn) {
        Assert.assertEquals(flip.gauge, 0L, flip.toString());
      }
      sim.algorithm.pauseGate.countDown();
      sim.algorithm.pauseGate = null;
      sim.drain();
      long gauge = sim.monitor.getWagedInstanceTagIsolationSkippedResourcesGauge();
      if (turnOn) {
        Assert.assertEquals(sim.retrySet(), set("RA"));
        Assert.assertTrue(gauge >= 1L, "gauge " + gauge);
      } else {
        Assert.assertTrue(sim.retrySet().isEmpty(), "the late calculation left a retry set");
        Assert.assertEquals(gauge, 0L, "the late calculation published after the flip");
        Report late = null;
        for (Report report : sim.algorithm.reports) {
          if (report.scope == RebalanceScopeType.GLOBAL_BASELINE) {
            late = report;
          }
        }
        Assert.assertNotNull(late);
        Assert.assertEquals(late.skipped, set("RA"), "the late calculation did not carry A");
      }

      Run last = last(looseStep(sim, turnOn, tags("A")));
      Assert.assertEquals(last.retry, turnOn ? set("RA") : none(), last.toString());
      Assert.assertEquals(last.gauge, turnOn ? 1L : 0L, last.toString());
    }
  }

  /**
   * An emergency whose only work is the broken clique. A cannot absorb a2's replicas and
   * no other clique has anything to do. With three attribution blocks and one failed, isolation
   * carries A instead of rethrowing, so the pipeline succeeds and serves exactly the previous best
   * possible, which is also what the flag off Helix controller keeps after its pipeline fails.
   */
  @Test(dataProvider = "syncAndAsync")
  public void testNarrowScopeWhoseOnlyWorkIsTheBrokenClique(boolean async) throws Exception {
    List<Object> served = new ArrayList<>();
    for (boolean isolation : new boolean[] {true, false}) {
      try (ScopeMatrixSim sim = sim("onlyBrokenWork", isolation, async, async)) {
        sim.resources.get("RA").weight = 30;
        bootstrap(sim);
        sim.offline("a2", false);
        Run run = sim.run();
        assertBookkeeping(sim, run);
        Calc emergency = only(run.calcs(RebalanceScopeType.EMERGENCY));
        Assert.assertEquals(emergency.toAssign, set("RA"), run.toString());
        if (isolation) {
          Assert.assertNull(run.computeFailure, run.toString());
          Assert.assertEquals(emergency.skipped, set("RA"), run.toString());
          Assert.assertEquals(canon(run.emitted, null), canon(run.bestBefore, null),
              run.toString());
          Assert.assertEquals(run.gauge, 1L, run.toString());
        } else {
          Assert.assertNotNull(emergency.failure, run.toString());
          Assert.assertNotNull(run.computeFailure, run.toString());
          Assert.assertNull(run.emitted, run.toString());
        }
        served.add(canon(served(run), null));
      }
    }
    Assert.assertEquals(served.get(0), served.get(1));
  }

  // ---------------------------------------------------------------- verification helpers

  /**
   * Bookkeeping on one run. Every successful calculation reports exactly the resources it had
   * replicas to place and at least the ones it skipped, a phase with nothing to do reports two
   * empty sets, a pipeline with no baseline due may re-report the retry set the last persisted
   * baseline committed and nothing else, and once the baseline has not failed the gauge counts
   * exactly what is still waiting for a baseline retry or was carried by the last run of a serving
   * phase.
   */
  static void assertBookkeeping(ScopeMatrixSim sim, Run run) {
    for (RebalanceScopeType scope : RebalanceScopeType.values()) {
      List<Report> reports = new ArrayList<>();
      run.reports.stream().filter(r -> r.scope == scope).forEach(reports::add);
      int next = 0;
      for (Calc calc : run.calcs(scope)) {
        if (calc.failure != null) {
          continue;
        }
        while (next < reports.size() && isEmpty(reports.get(next)) && !calc.toAssign.isEmpty()) {
          next++;
        }
        Assert.assertTrue(next < reports.size(), scope + " calculation never reported: " + run);
        Report report = reports.get(next++);
        Assert.assertEquals(report.evaluated, calc.toAssign, scope + " evaluated: " + run);
        Assert.assertTrue(report.skipped.containsAll(calc.skipped), scope + " skipped: " + run);
      }
      boolean mayReportAgain =
          scope == RebalanceScopeType.GLOBAL_BASELINE && run.calcs(scope).isEmpty();
      for (; next < reports.size(); next++) {
        Report report = reports.get(next);
        if (mayReportAgain && isCommittedRetryReport(report, run)) {
          mayReportAgain = false;
          continue;
        }
        Assert.assertTrue(isEmpty(report), scope + " reported without calculating: " + run);
      }
    }
    boolean baselineFailed = run.calcs(RebalanceScopeType.GLOBAL_BASELINE).stream()
        .anyMatch(calc -> calc.failure != null);
    if (!baselineFailed) {
      Assert.assertEquals(run.gauge, sim.expectedGauge(run), "gauge: " + run);
    }
  }

  static boolean isEmpty(Report report) {
    return report.evaluated.isEmpty() && report.skipped.isEmpty();
  }

  /**
   * The report a pipeline with no baseline due may send so a monitor reset cannot hide what the
   * persisted baseline carries: evaluated and skipped are both the committed retry set, which no
   * baseline changed during the run.
   */
  static boolean isCommittedRetryReport(Report report, Run run) {
    return !report.skipped.isEmpty() && report.skipped.equals(run.retry)
        && report.evaluated.equals(report.skipped);
  }

  private static ScopeMatrixSim sim(String name, boolean isolation, boolean asyncGlobal,
      boolean asyncPartial) {
    ScopeMatrixSim sim = ScopeMatrixSim.standard(cluster(name), isolation);
    sim.setModes(asyncGlobal, asyncPartial);
    return sim;
  }

  /** The standard simulation with RA at three partitions of 25, which two nodes of A still hold. */
  static ScopeMatrixSim carrySim(String name, boolean isolation, boolean asyncGlobal,
      boolean asyncPartial) {
    ScopeMatrixSim sim = sim(name, isolation, asyncGlobal, asyncPartial);
    ResourceSpec ra = sim.resources.get("RA");
    ra.partitions = 3;
    ra.weight = 25;
    return sim;
  }

  /**
   * Runs pipelines until everything is served. In asynchronous modes the very first pipeline has
   * no baseline or best possible to serve from yet, which is stock behaviour.
   */
  static Run bootstrap(ScopeMatrixSim sim) throws Exception {
    Run run = sim.run();
    for (int i = 0; i < 4 && !(run.emitted != null
        && run.emitted.keySet().containsAll(sim.resources.keySet())); i++) {
      run = sim.run();
    }
    verifyAllFresh(sim, run);
    assertBookkeeping(sim, run);
    return run;
  }

  /**
   * Runs the pipeline for a change that was just made, then runs pipelines with no further change
   * until the served assignment and the baseline stop moving, checking the invariants on every
   * run. In an asynchronous mode the served assignment lags the change, which is stock behaviour:
   * one run when either calculation is asynchronous (the change is served from the maps as they
   * stood), two when both are (the partial that uses the new baseline lands one run later still).
   * Only the frozen cliques are checked on those lagging runs.
   *
   * @return every run, the one that saw the change first.
   */
  static List<Run> step(ScopeMatrixSim sim, boolean isolation, Set<String> brokenBaseline,
      Set<String> frozen, boolean topUps) throws Exception {
    int lag = sim.asyncGlobal && sim.asyncPartial ? 2 : sim.anyAsync() ? 1 : 0;
    List<Run> runs = new ArrayList<>();
    for (int i = 0; i < lag + 5; i++) {
      Run run = sim.run();
      if (!isolation) {
        assertStock(run);
      } else if (i < lag) {
        verifyFrozen(sim, run, frozen);
      } else {
        verify(sim, run, brokenBaseline, frozen, topUps);
      }
      assertBookkeeping(sim, run);
      Run previous = runs.isEmpty() ? null : last(runs);
      runs.add(run);
      if (i > lag && canon(served(run), null).equals(canon(served(previous), null))
          && canon(run.baselineAfter, null).equals(canon(previous.baselineAfter, null))) {
        break;
      }
    }
    return runs;
  }

  /**
   * Runs pipelines with no further change until three runs in a row persist the same baseline and
   * best possible and serve the same maps, checking the bookkeeping on every run.
   *
   * @return every run, in order.
   */
  static List<Run> settle(ScopeMatrixSim sim) throws Exception {
    List<Run> runs = new ArrayList<>();
    int unchanged = 0;
    for (int i = 0; i < 14; i++) {
      Run run = sim.run();
      assertBookkeeping(sim, run);
      if (!runs.isEmpty()) {
        Run prior = last(runs);
        boolean same = canon(served(run), null).equals(canon(served(prior), null))
            && canon(run.baselineAfter, null).equals(canon(prior.baselineAfter, null))
            && canon(run.bestAfter, null).equals(canon(prior.bestAfter, null));
        unchanged = same ? unchanged + 1 : 0;
      }
      runs.add(run);
      if (unchanged == 2) {
        return runs;
      }
    }
    throw new AssertionError("did not settle: " + runs);
  }

  static Run last(List<Run> runs) {
    return runs.get(runs.size() - 1);
  }

  static Set<String> none() {
    return Collections.emptySet();
  }

  /** A frozen clique is emitted and persisted exactly as the store's best possible stood. */
  static void verifyFrozen(ScopeMatrixSim sim, Run run, Set<String> frozen) {
    for (String resource : sim.resourcesOf(frozen.toArray(new String[0]))) {
      if (run.emitted != null) {
        Assert.assertEquals(ScopeMatrixSim.canon(run.emitted.get(resource)),
            ScopeMatrixSim.canon(run.bestBefore.get(resource)),
            resource + " must be emitted as it stood: " + run);
      }
      Assert.assertEquals(ScopeMatrixSim.canon(run.bestAfter.get(resource)),
          ScopeMatrixSim.canon(run.bestBefore.get(resource)),
          resource + " must be persisted as it stood: " + run);
    }
  }

  /** DISK held on one node by the given resources, at their current weights. */
  static long load(ScopeMatrixSim sim, Map<String, ResourceAssignment> map, Set<String> resources,
      String node) {
    long used = 0;
    for (String resource : resources) {
      ResourceAssignment assignment = map.get(resource);
      if (assignment == null) {
        continue;
      }
      for (Partition partition : assignment.getMappedPartitions()) {
        if (assignment.getReplicaMap(partition).containsKey(node)) {
          used += sim.resources.get(resource).weightOf(partition.getPartitionName());
        }
      }
    }
    return used;
  }

  static int partitions(Map<String, ResourceAssignment> map, String resource) {
    ResourceAssignment assignment = map.get(resource);
    return assignment == null ? 0 : assignment.getMappedPartitions().size();
  }

  /** No partition with a replica on the offline node was given an extra live replica. */
  static void assertNotToppedUp(ScopeMatrixSim sim, Map<String, ResourceAssignment> map,
      Set<String> resources, String offlineNode) {
    int affected = 0;
    for (String resource : resources) {
      ResourceAssignment assignment = map.get(resource);
      for (Partition partition : assignment.getMappedPartitions()) {
        Map<String, String> replicas = assignment.getReplicaMap(partition);
        if (replicas.containsKey(offlineNode)) {
          Assert.assertEquals(replicas.size(), sim.resources.get(resource).replicas,
              resource + " " + partition + " was topped up: " + replicas);
          affected++;
        }
      }
    }
    Assert.assertTrue(affected > 0, "nothing on " + offlineNode);
  }

  /** What the pipeline served: the emitted assignment, or the stored one it fell back to. */
  static Map<String, ResourceAssignment> served(Run run) {
    return run.emitted != null ? run.emitted : run.bestAfter;
  }

  static boolean uses(Map<String, ResourceAssignment> map, Set<String> resources, String node) {
    for (String resource : resources) {
      ResourceAssignment assignment = map.get(resource);
      if (assignment == null) {
        continue;
      }
      for (Partition partition : assignment.getMappedPartitions()) {
        if (assignment.getReplicaMap(partition).containsKey(node)) {
          return true;
        }
      }
    }
    return false;
  }

  /** A node with the tag, other than the excepted one, that serves one of the tag's resources. */
  static String servingNode(ScopeMatrixSim sim, Run run, String tag, String except) {
    for (NodeSpec node : sim.nodes.values()) {
      if (node.tags.contains(tag) && !node.name.equals(except)
          && uses(served(run), sim.resourcesOf(tag), node.name)) {
        return node.name;
      }
    }
    throw new AssertionError("no " + tag + " node other than " + except + " serves: " + run);
  }

  /** Every partition with a replica on the offline node also has min active replicas elsewhere. */
  static void assertToppedUp(ScopeMatrixSim sim, Map<String, ResourceAssignment> map,
      Set<String> resources, String offlineNode) {
    int toppedUp = 0;
    for (String resource : resources) {
      ResourceAssignment assignment = map.get(resource);
      for (Partition partition : assignment.getMappedPartitions()) {
        Map<String, String> replicas = assignment.getReplicaMap(partition);
        if (replicas.containsKey(offlineNode)) {
          long live = replicas.keySet().stream().filter(i -> sim.nodes.get(i).live).count();
          Assert.assertTrue(live >= sim.resources.get(resource).replicas,
              resource + " " + partition + " not topped up: " + replicas);
          toppedUp++;
        }
      }
    }
    Assert.assertTrue(toppedUp > 0, "nothing needed a top up on " + offlineNode);
  }

  /** Flag off: nothing is ever skipped, retried or counted. */
  static void assertStock(Run run) {
    Assert.assertEquals(run.gauge, 0L, run.toString());
    Assert.assertTrue(run.retry.isEmpty(), run.toString());
    for (Calc calc : run.calcs) {
      Assert.assertTrue(calc.skipped.isEmpty(), run.toString());
    }
    for (ScopeMatrixSim.Report report : run.reports) {
      Assert.assertTrue(report.skipped.isEmpty(), run.toString());
    }
    if (run.computeFailure != null) {
      Assert.assertNull(run.emitted, run.toString());
    }
  }

  static void assertScopeDidSomewhere(List<Run> runs, RebalanceScopeType scope,
      String resource) {
    for (Run run : runs) {
      for (Calc calc : run.calcs(scope)) {
        if (calc.failure == null && calc.toAssign.contains(resource)
            && !calc.skipped.contains(resource)) {
          return;
        }
      }
    }
    Assert.fail(scope + " never placed " + resource + ": " + runs);
  }

  static Calc only(List<Calc> calcs) {
    Assert.assertEquals(calcs.size(), 1, calcs.toString());
    return calcs.get(0);
  }

  static Map<String, Map<String, Map<String, String>>> canon(
      Map<String, ResourceAssignment> map, Set<String> resources) {
    return ScopeMatrixSim.canon(map, resources);
  }

  private static void verifyAllFresh(ScopeMatrixSim sim, Run run) {
    Assert.assertNull(run.computeFailure, run.toString());
    verify(sim, run, Collections.emptySet(), Collections.emptySet(), false);
  }

  /**
   * @param brokenBaseline tags whose resources the baseline must have carried from the previous
   *        baseline and must be retrying.
   * @param frozen tags whose resources must be emitted and persisted exactly as the store's best
   *        possible assignment stood before the run.
   * @param topUps whether the emitted map may carry delayed rebalance top up replicas.
   */
  static void verify(ScopeMatrixSim sim, Run run, Set<String> brokenBaseline, Set<String> frozen,
      boolean topUps) {
    Assert.assertNotNull(run.emitted, "nothing emitted: " + run);
    Set<String> carriedBaseline = sim.resourcesOf(brokenBaseline.toArray(new String[0]));
    Set<String> carriedServing = sim.resourcesOf(frozen.toArray(new String[0]));
    Set<String> carriedAny = new TreeSet<>(carriedBaseline);
    carriedAny.addAll(carriedServing);
    Set<String> fresh = new TreeSet<>();
    for (ResourceSpec spec : sim.resources.values()) {
      String resource = spec.name;
      if (carriedServing.contains(resource)) {
        Assert.assertEquals(ScopeMatrixSim.canon(run.emitted.get(resource)),
            ScopeMatrixSim.canon(run.bestBefore.get(resource)),
            resource + " must be emitted as it stood: " + run);
        Assert.assertEquals(ScopeMatrixSim.canon(run.bestAfter.get(resource)),
            ScopeMatrixSim.canon(run.bestBefore.get(resource)),
            resource + " must be persisted as it stood: " + run);
      } else {
        sim.assertFresh("emitted", run.emitted, resource, sim.servingNodes(spec.tag), topUps);
        sim.assertFresh("best possible", run.bestAfter, resource, sim.servingNodes(spec.tag),
            false);
      }
      if (carriedBaseline.contains(resource)) {
        Assert.assertEquals(ScopeMatrixSim.canon(run.baselineAfter.get(resource)),
            ScopeMatrixSim.canon(run.baselineBefore.get(resource)),
            resource + " baseline must be carried: " + run);
      } else {
        sim.assertFresh("baseline", run.baselineAfter, resource, sim.baselineNodes(spec.tag),
            false);
      }
      if (!carriedAny.contains(resource)) {
        fresh.add(resource);
      }
    }
    sim.assertCapacity("emitted", run.emitted, carriedAny);
    sim.assertCapacity("best possible", run.bestAfter, carriedAny);
    sim.assertCapacity("baseline", run.baselineAfter, carriedBaseline);
    Assert.assertEquals(run.retry, carriedBaseline, "retry set: " + run);
    sim.recordComputedWeights(fresh);
  }

  static void assertScopeDid(Run run, RebalanceScopeType scope, String resource) {
    for (Calc calc : run.calcs(scope)) {
      if (calc.failure == null && calc.toAssign.contains(resource)
          && !calc.skipped.contains(resource)) {
        return;
      }
    }
    Assert.fail(scope + " did not place " + resource + ": " + run);
  }

  static Set<String> set(String... values) {
    return new TreeSet<>(Arrays.asList(values));
  }

  static Set<String> tags(String... values) {
    return set(values);
  }
}
