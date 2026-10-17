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
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

import java.io.IOException;
import java.lang.reflect.Field;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.helix.BucketDataAccessor;
import org.apache.helix.HelixConstants;
import org.apache.helix.HelixProperty;
import org.apache.helix.HelixRebalanceException;
import org.apache.helix.constants.InstanceConstants;
import org.apache.helix.controller.dataproviders.ResourceControllerDataProvider;
import org.apache.helix.controller.rebalancer.waged.ScopeMatrixSim.Run;
import org.apache.helix.controller.rebalancer.waged.TestWagedIsolationScopeMatrix.Diverged;
import org.apache.helix.controller.rebalancer.waged.constraints.ConstraintBasedAlgorithmFactory;
import org.apache.helix.controller.rebalancer.waged.model.ClusterModel;
import org.apache.helix.controller.rebalancer.waged.model.OptimalAssignment;
import org.apache.helix.controller.stages.CurrentStateOutput;
import org.apache.helix.model.BuiltInStateModelDefinitions;
import org.apache.helix.model.ClusterConfig;
import org.apache.helix.model.IdealState;
import org.apache.helix.model.InstanceConfig;
import org.apache.helix.model.LiveInstance;
import org.apache.helix.model.Resource;
import org.apache.helix.model.ResourceAssignment;
import org.apache.helix.model.ResourceConfig;
import org.apache.helix.monitoring.mbeans.ClusterStatusMonitor;
import org.apache.helix.zookeeper.datamodel.ZNRecord;
import org.apache.helix.zookeeper.datamodel.serializer.ZNRecordJacksonSerializer;
import org.apache.helix.zookeeper.zkclient.exception.ZkNoNodeException;
import org.testng.Assert;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import static org.apache.helix.controller.rebalancer.waged.TestWagedIsolationScopeMatrix.breakA;
import static org.apache.helix.controller.rebalancer.waged.TestWagedIsolationScopeMatrix.canon;
import static org.apache.helix.controller.rebalancer.waged.TestWagedIsolationScopeMatrix.carrySim;
import static org.apache.helix.controller.rebalancer.waged.TestWagedIsolationScopeMatrix.diverge;
import static org.apache.helix.controller.rebalancer.waged.TestWagedIsolationScopeMatrix.last;
import static org.apache.helix.controller.rebalancer.waged.TestWagedIsolationScopeMatrix.modes;
import static org.apache.helix.controller.rebalancer.waged.TestWagedIsolationScopeMatrix.served;
import static org.apache.helix.controller.rebalancer.waged.TestWagedIsolationScopeMatrix.settle;
import static org.apache.helix.controller.rebalancer.waged.TestWagedIsolationScopeMatrix.uses;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Leadership lifecycle races and handovers. A Helix controller that loses leadership resets its
 * ClusterStatusMonitor and invalidates the WagedRebalancer; on regaining leadership it calls
 * WagedRebalancer.reset(); on shutdown it calls WagedRebalancer.close(). The race tests hold an
 * asynchronous baseline inside calculate while one of those calls happens, then let it finish
 * late, which cannot be timed end to end. Their metadata store is an in-memory stand-in for
 * ZooKeeper that survives across rebalancer instances, like the persisted assignment metadata
 * does. The handover tests drive ScopeMatrixSim, where a new Helix controller leader starts from
 * only the persisted BASELINE and BEST_POSSIBLE maps and the participants' current state, and
 * must settle where a leader that never stopped settles. After the event that repairs a broken
 * clique a new leader settles elsewhere even with the flag off, so there it must settle where the
 * flag off rebalancer settles across the same handover.
 */
public class TestWagedIsolationLeadershipLifecycle {
  private static final String CLUSTER = "IsolationLeadershipLifecycle";
  private static final long WAIT_SECONDS = 30L;
  private static final Set<String> ALL = set("broken", "healthy", "sibling");
  private static final Set<String> BLOCK = set("broken", "sibling");
  private static final ZNRecordJacksonSerializer SERIALIZER = new ZNRecordJacksonSerializer();

  /**
   * The new term resets the rebalancer while the previous term's baseline, which retries and
   * skips the broken clique, is still computing. The late result must not restore its retry set
   * or report its skips at all. It is still persisted, exactly as base Helix persists a late
   * baseline.
   */
  @Test
  public void testResetDuringAnInFlightBaselineDropsItsRetrySetAndReport() throws Exception {
    try (Lifecycle cluster = new Lifecycle()) {
      cluster.isolate();
      int mark = cluster.startBlockedBaseline();
      Assert.assertEquals(cluster.retrySet(), ALL, "The in-flight marker covers every resource");
      Assert.assertEquals(cluster._algorithm.held(), ALL,
          "The late baseline retries the carried clique together with the changed resource");

      cluster.loseLeadership();
      cluster.regainLeadership();
      Set<String> fresh = cluster.retryReference();
      Assert.assertTrue(fresh.isEmpty());
      Map<String, String> before = cluster._zk.baseline();
      int writes = cluster._zk.baselineWrites();

      cluster.releaseAndDrain();
      Assert.assertEquals(cluster._algorithm.baselines(), mark,
          "A baseline superseded by the reset reports nothing, not even to its own pipeline");
      Assert.assertSame(cluster.retryReference(), fresh,
          "A baseline from before the reset must not restore its retry set");
      Assert.assertEquals(cluster.gauge(), 0L,
          "A baseline from before the reset must not publish its skipped resources");
      Assert.assertEquals(cluster._zk.baselineWrites(), writes + 1,
          "Base parity: the late baseline is still persisted after the reset");
      Assert.assertFalse(cluster._zk.baseline().equals(before), "The late baseline was written");
      for (String carried : BLOCK) {
        Assert.assertEquals(cluster._zk.baseline().get(carried), before.get(carried),
            "The late baseline carries the broken clique forward: " + carried);
      }
      cluster.assertCompleteIn("BASELINE", cluster._zk.baseline());

      int next = cluster._algorithm.baselines();
      cluster.settle();
      Assert.assertEquals(cluster._algorithm.evaluatedAt(next), ALL,
          "The first baseline of the new term is a full recompute");
      Assert.assertEquals(cluster._algorithm.skippedAt(next), BLOCK);
      Assert.assertEquals(cluster.gauge(), 2L);
      Assert.assertEquals(cluster.retrySet(), BLOCK);
      cluster.assertComplete();
    }
  }

  /**
   * Leadership is lost and not regained while the baseline is in flight: the standby keeps an
   * invalidated rebalancer. The late result may repopulate that object's retry set, but it cannot
   * reach the gauge, and the reset on the next leadership term discards it.
   */
  @Test
  public void testLateBaselineOnAStandbyNeverReachesTheGauge() throws Exception {
    try (Lifecycle cluster = new Lifecycle()) {
      cluster.isolate();
      cluster.startBlockedBaseline();
      cluster.loseLeadership();
      cluster.releaseAndDrain();
      Assert.assertEquals(cluster.gauge(), 0L, "The standby must not publish a stale report");
      Assert.assertEquals(cluster.retrySet(), BLOCK,
          "Only the invalidated rebalancer of the standby carries the late retry set");

      cluster.regainLeadership();
      Assert.assertTrue(cluster.retrySet().isEmpty(), "The next leadership term starts clean");
      int next = cluster._algorithm.baselines();
      cluster.settle();
      Assert.assertEquals(cluster._algorithm.evaluatedAt(next), ALL);
      Assert.assertEquals(cluster.gauge(), 2L);
      Assert.assertEquals(cluster.retrySet(), BLOCK);
      cluster.assertComplete();
    }
  }

  /**
   * A second baseline from the old term is queued behind the blocked one when the reset
   * happens, and the new term's first baseline is queued behind both. The executor is FIFO, so
   * the stale work always runs before the new term's full recompute and cannot outlive it. The
   * blocked baseline was superseded by the reset and reports nothing; the queued one starts
   * after the reset and reports against the new term's retry set, but only its own change.
   */
  @Test
  public void testQueuedBaselinesFromTheOldTermCannotOutliveTheNewTerm() throws Exception {
    try (Lifecycle cluster = new Lifecycle()) {
      cluster.isolate();
      int mark = cluster.startBlockedBaseline();
      cluster.setWeight("healthy", 11);
      cluster.pipeline();
      cluster.loseLeadership();
      cluster.regainLeadership();
      cluster.pipeline();

      cluster.releaseAndDrain();
      Assert.assertEquals(cluster._algorithm.held(), ALL);
      Assert.assertEquals(cluster._algorithm.baselines(), mark + 2,
          "The blocked baseline, superseded by the reset, reports nothing");
      Assert.assertEquals(cluster._algorithm.evaluatedAt(mark), set("healthy"),
          "The queued stale baseline only saw its own change");
      Assert.assertEquals(cluster._algorithm.evaluatedAt(mark + 1), ALL,
          "The new term's full recompute runs last");
      Assert.assertEquals(cluster._algorithm.skippedAt(mark + 1), BLOCK);
      Assert.assertEquals(cluster.gauge(), 2L);
      Assert.assertEquals(cluster.retrySet(), BLOCK);
      // Stands in for the pipeline that a changed baseline schedules.
      cluster.pipeline();
      cluster.assertComplete();
    }
  }

  /**
   * A Helix controller that shuts down closes the rebalancer while the baseline is in flight.
   * The late result must not change the persisted assignment or the isolation gauge. Its write
   * is attempted and fails on the closed store, which is also what base Helix does.
   */
  @Test
  public void testCloseDuringAnInFlightBaselineCannotPersistOrReport() throws Exception {
    try (Lifecycle cluster = new Lifecycle()) {
      cluster.isolate();
      cluster.startBlockedBaseline();
      Map<String, String> before = cluster._zk.baseline();
      int writes = cluster._zk.baselineWrites();

      cluster.loseLeadership();
      cluster.shutdown();
      cluster._algorithm.release();
      Assert.assertTrue(cluster.executor().awaitTermination(WAIT_SECONDS, TimeUnit.SECONDS),
          "The interrupted baseline must still finish");
      Assert.assertEquals(cluster._zk.baseline(), before);
      Assert.assertEquals(cluster._zk.baselineWrites(), writes);
      Assert.assertEquals(cluster._zk.writesAfterClose(), 1,
          "Base parity: the late baseline attempts its write on the closed store");
      Assert.assertEquals(cluster.gauge(), 0L);
    }
  }

  /**
   * A rebalancer created for a new leadership term, with a new store over the same persisted
   * assignment, starts with an empty retry set and gauge. Its first baseline is a full recompute
   * that rebuilds the gauge and reproduces the persisted baseline without rewriting it.
   */
  @Test
  public void testRebalancerCreatedForANewTermStartsClean() throws Exception {
    try (Lifecycle cluster = new Lifecycle()) {
      cluster.isolate();
      Map<String, String> before = cluster._zk.baseline();
      int writes = cluster._zk.baselineWrites();
      cluster.loseLeadership();
      cluster.shutdown();

      cluster.newTerm();
      Assert.assertTrue(cluster.retrySet().isEmpty());
      Assert.assertEquals(cluster.gauge(), 0L);
      cluster.settle();
      Assert.assertEquals(cluster._algorithm.evaluatedAt(0), ALL);
      Assert.assertEquals(cluster._algorithm.skippedAt(0), BLOCK);
      Assert.assertEquals(cluster.gauge(), 2L);
      Assert.assertEquals(cluster.retrySet(), BLOCK);
      Assert.assertEquals(cluster._zk.baseline(), before,
          "A full recompute over the same state reproduces the carried baseline");
      Assert.assertEquals(cluster._zk.baselineWrites(), writes);
      cluster.assertComplete();
    }
  }

  /**
   * The flag is turned off while the baseline is in flight. The late flag-on result cannot
   * publish, the stock baseline that follows clears the retry set and blocks as base Helix does,
   * and turning the flag back on resumes isolation with a full recompute.
   */
  @Test
  public void testFlagTurnedOffDuringAnInFlightBaselineKeepsTheGaugeAtZero() throws Exception {
    try (Lifecycle cluster = new Lifecycle()) {
      cluster.isolate();
      cluster.startBlockedBaseline();
      cluster.setIsolation(false);
      int stock = cluster._algorithm.baselines() + 1;
      cluster.pipeline();
      Assert.assertEquals(cluster.gauge(), 0L);

      cluster.releaseAndDrain();
      Assert.assertEquals(cluster.gauge(), 0L, "A late flag-on result must not publish");
      Assert.assertTrue(cluster.retrySet().isEmpty(), "The stock baseline clears the retry set");
      Assert.assertEquals(cluster._algorithm.baselines(), stock,
          "The stock baseline fails as a whole and reports nothing");
      Assert.assertEquals(cluster._monitor.getWagedBaselineComputeFailingGauge(), 1L);
      cluster.assertCompleteIn("BASELINE", cluster._zk.baseline());

      cluster.setIsolation(true);
      int next = cluster._algorithm.baselines();
      cluster.settle();
      Assert.assertEquals(cluster._algorithm.evaluatedAt(next), ALL);
      Assert.assertEquals(cluster._algorithm.skippedAt(next), BLOCK);
      Assert.assertEquals(cluster.gauge(), 2L);
      Assert.assertEquals(cluster.retrySet(), BLOCK);
      Assert.assertEquals(cluster._monitor.getWagedBaselineComputeFailingGauge(), 0L);
      cluster.assertComplete();
    }
  }

  /** When the Helix controller leader changes, relative to the persisted writes of one event. */
  enum Crash {
    /** A is broken and carried, and no event is pending. */
    IDLE,
    /** b0 shrinks to DISK 20, and the leader stops before the BASELINE write lands. */
    BEFORE_BASELINE_WRITE,
    /** b0 shrinks to DISK 20, and the leader stops after BASELINE and before BEST_POSSIBLE. */
    BETWEEN_BASELINE_AND_BEST,
    /** b0 shrinks to DISK 20, and the leader stops after the pipeline that saw it. */
    AFTER_EVENT,
    /**
     * The event that breaks A, which also grows RB1 to six partitions, and the leader stops after
     * BASELINE and before BEST_POSSIBLE.
     */
    BREAK_EVENT_BETWEEN,
    /**
     * The event that repairs A, which also grows RB1 to six partitions, and the leader stops after
     * BASELINE and before BEST_POSSIBLE.
     */
    REPAIR_EVENT_BETWEEN
  }

  @DataProvider(name = "modesAndCrashes")
  public Object[][] modesAndCrashes() {
    List<Object[]> rows = new ArrayList<>();
    for (Object[] mode : modes()) {
      for (Crash crash : Crash.values()) {
        rows.add(new Object[] {mode[0], mode[1], crash});
      }
    }
    return rows.toArray(new Object[0][]);
  }

  /**
   * RA's baseline and best possible differ, A is broken, or broken by the event itself in
   * BREAK_EVENT_BETWEEN, and the Helix controller leader changes around one event.
   * REPAIR_EVENT_BETWEEN is checked by {@link #assertRepairAcrossHandoversMatchesStock}. For every
   * other crash the new leader settles on exactly the baseline, best possible and served maps of a
   * leader that never stopped, and on every run after the handover RA's baseline stays the
   * diverged baseline while RA's best possible and served maps stay the diverged best possible. B
   * and C are only ever served as the uninterrupted leader served them, or, when the event shrinks
   * b0, as they stood before it. After an idle handover their baseline and served maps do not
   * move at all.
   */
  @Test(dataProvider = "modesAndCrashes")
  public void testNewLeaderConvergesToUninterruptedResult(boolean asyncGlobal,
      boolean asyncPartial, Crash crash) throws Exception {
    try (ScopeMatrixSim steady = carrySim("steady" + crash, true, asyncGlobal, asyncPartial);
        ScopeMatrixSim failover = carrySim("failover" + crash, true, asyncGlobal, asyncPartial)) {
      Set<String> healthy = failover.resourcesOf("B", "C");
      Diverged diverged = diverge(steady);
      Diverged twin = diverge(failover);
      Assert.assertEquals(twin.baselineRA, diverged.baselineRA);
      Assert.assertEquals(twin.bestRA, diverged.bestRA);

      if (crash == Crash.BREAK_EVENT_BETWEEN) {
        for (ScopeMatrixSim sim : new ScopeMatrixSim[] {steady, failover}) {
          breakA(sim, diverged.broken);
          sim.resources.get("RB1").partitions = 6;
        }
        List<Run> steadyRuns = settle(steady);
        crashBetween(failover);
        failover.newLeader();
        List<Run> failoverRuns = settle(failover);
        TestWagedIsolationScopeMatrix.assertCarried(failoverRuns, diverged, crash.name());
        assertSameEnd(last(steadyRuns), last(failoverRuns), crash.name());
        Assert.assertTrue(
            servedStates(steadyRuns, healthy).containsAll(servedStates(failoverRuns, healthy)),
            "B and C served in a state the uninterrupted leader never served: " + failoverRuns);
        return;
      }

      for (ScopeMatrixSim sim : new ScopeMatrixSim[] {steady, failover}) {
        breakA(sim, diverged.broken);
      }
      Run steadyBroken = last(settle(steady));
      Run before = last(settle(failover));
      assertSameEnd(steadyBroken, before, "broken");

      if (crash == Crash.IDLE) {
        List<Run> steadyRuns = settle(steady);
        failover.newLeader();
        List<Run> failoverRuns = settle(failover);
        for (Run run : failoverRuns) {
          Assert.assertEquals(canon(served(run), healthy), canon(served(before), healthy),
              "B and C served maps moved: " + run);
          Assert.assertEquals(canon(run.baselineAfter, healthy),
              canon(before.baselineAfter, healthy), "B and C baseline moved: " + run);
        }
        TestWagedIsolationScopeMatrix.assertCarried(failoverRuns, diverged, crash.name());
        assertSameEnd(last(steadyRuns), last(failoverRuns), crash.name());
        return;
      }
      if (crash == Crash.REPAIR_EVENT_BETWEEN) {
        assertRepairAcrossHandoversMatchesStock(steady, failover, diverged, before);
        return;
      }

      for (ScopeMatrixSim sim : new ScopeMatrixSim[] {steady, failover}) {
        sim.nodes.get("b0").disk = 20;
      }
      List<Run> steadyRuns = settle(steady);
      if (crash == Crash.BEFORE_BASELINE_WRITE) {
        Map<String, ResourceAssignment> durable = failover.store.durableBaseline();
        failover.store.failBaselinePersist = true;
        Run crashed = failover.run();
        Assert.assertEquals(canon(failover.store.durableBaseline(), null), canon(durable, null),
            "the BASELINE write must not land: " + crashed);
      } else if (crash == Crash.BETWEEN_BASELINE_AND_BEST) {
        crashBetween(failover);
      } else {
        failover.run();
      }
      failover.newLeader();
      List<Run> failoverRuns = settle(failover);
      TestWagedIsolationScopeMatrix.assertCarried(failoverRuns, diverged, crash.name());
      assertSameEnd(last(steadyRuns), last(failoverRuns), crash.name());
      Set<Object> allowed = servedStates(steadyRuns, healthy);
      allowed.add(canon(served(before), healthy));
      Assert.assertTrue(allowed.containsAll(servedStates(failoverRuns, healthy)),
          "B and C served in a state the uninterrupted leader never served: " + failoverRuns);
    }
  }

  /**
   * The event that repairs A, RA's broken partition shrinking to 40 while RB1 grows to six
   * partitions, lands with BASELINE written and BEST_POSSIBLE not, and the Helix controller leader
   * changes. Across this event a new leader does not settle where the uninterrupted one does, even
   * with the flag off, so three flag off simulations start from the same maps and take the same
   * event, minus the break. Each flag on leader settles on exactly the baseline, best possible and
   * served maps of its flag off counterpart: the one that never stopped, the one that stopped
   * between the writes, and one that handed over only after the event had settled. The new
   * leader's first baseline evaluates every resource, it ends with nothing to retry, and its best
   * possible holds the repaired RA exactly where the uninterrupted leader's does.
   */
  private static void assertRepairAcrossHandoversMatchesStock(ScopeMatrixSim steady,
      ScopeMatrixSim failover, Diverged diverged, Run before) throws Exception {
    boolean asyncGlobal = steady.asyncGlobal;
    boolean asyncPartial = steady.asyncPartial;
    try (ScopeMatrixSim idle = carrySim("repairIdle", true, asyncGlobal, asyncPartial);
        ScopeMatrixSim stockSteady = carrySim("stockSteady", false, asyncGlobal, asyncPartial);
        ScopeMatrixSim stockFailover = carrySim("stockFailover", false, asyncGlobal, asyncPartial);
        ScopeMatrixSim stockIdle = carrySim("stockIdle", false, asyncGlobal, asyncPartial)) {
      diverge(idle);
      breakA(idle, diverged.broken);
      assertSameEnd(before, last(settle(idle)), "broken");
      for (ScopeMatrixSim stock : new ScopeMatrixSim[] {stockSteady, stockFailover, stockIdle}) {
        diverge(stock);
        assertSameEnd(before, last(settle(stock)), "flag off before the repair");
      }
      for (ScopeMatrixSim sim : new ScopeMatrixSim[] {
          steady, failover, idle, stockSteady, stockFailover, stockIdle}) {
        sim.resources.get("RA").partitionWeights.put(diverged.broken, 40);
        sim.resources.get("RB1").partitions = 6;
      }
      List<Run> steadyRuns = settle(steady);
      List<Run> stockSteadyRuns = settle(stockSteady);
      crashBetween(failover);
      failover.newLeader();
      List<Run> failoverRuns = settle(failover);
      crashBetween(stockFailover);
      stockFailover.newLeader();
      List<Run> stockFailoverRuns = settle(stockFailover);
      settle(idle);
      idle.newLeader();
      List<Run> idleRuns = settle(idle);
      settle(stockIdle);
      stockIdle.newLeader();
      List<Run> stockIdleRuns = settle(stockIdle);

      Run first = failoverRuns.get(0);
      Assert.assertEquals(
          first.calcs(ClusterModel.RebalanceScopeType.GLOBAL_BASELINE).get(0).toAssign,
          failover.resources.keySet(), "the new leader's first baseline: " + first);
      Assert.assertTrue(last(failoverRuns).retry.isEmpty(), last(failoverRuns).toString());
      assertSameEnd(last(stockSteadyRuns), last(steadyRuns), "uninterrupted");
      assertSameEnd(last(stockFailoverRuns), last(failoverRuns), "handover between the writes");
      assertSameEnd(last(stockIdleRuns), last(idleRuns), "handover after the event");
      Assert.assertEquals(ScopeMatrixSim.canon(last(failoverRuns).bestAfter.get("RA")),
          ScopeMatrixSim.canon(last(steadyRuns).bestAfter.get("RA")), "repaired RA");
    }
  }

  /**
   * A is broken and the delay window is an hour. Inside the window an A node holding RA_0 in the
   * best possible and a B node serving B go offline, and the Helix controller leader changes after
   * the pipeline that saw it. The new leader settles on exactly the maps of a leader that never
   * stopped, with no computation failure: the delayed rebalance overwrites skip RA, RA is served
   * as its best possible stood, and every B partition with a replica on the offline B node is
   * topped up to its replica count on live nodes. When the window ends both leaders settle on the
   * same maps again, B does not use its offline node, and RA keeps its baseline and is still
   * served as its best possible stood.
   */
  @Test(dataProvider = "modes", dataProviderClass = TestWagedIsolationScopeMatrix.class)
  public void testNewLeaderInsideTheDelayWindowConvergesToUninterruptedResult(
      boolean asyncGlobal, boolean asyncPartial) throws Exception {
    try (ScopeMatrixSim steady = carrySim("delaySteady", true, asyncGlobal, asyncPartial);
        ScopeMatrixSim failover = carrySim("delayFailover", true, asyncGlobal, asyncPartial)) {
      for (ScopeMatrixSim sim : new ScopeMatrixSim[] {steady, failover}) {
        sim.enableDelay(3_600_000L);
        settle(sim);
        breakA(sim, "RA_0");
      }
      Run steadyBroken = last(settle(steady));
      Run broken = last(settle(failover));
      assertSameEnd(steadyBroken, broken, "broken");
      Assert.assertEquals(steadyBroken.retry, set("RA"), steadyBroken.toString());
      Map<String, Map<String, String>> baselineRA =
          ScopeMatrixSim.canon(broken.baselineAfter.get("RA"));
      Map<String, Map<String, String>> bestRA = ScopeMatrixSim.canon(broken.bestAfter.get("RA"));
      String aDown = bestRA.get("RA_0").keySet().iterator().next();
      String bDown = TestWagedIsolationScopeMatrix.servingNode(failover, broken, "B", "b0");
      for (ScopeMatrixSim sim : new ScopeMatrixSim[] {steady, failover}) {
        sim.offline(aDown, true);
        sim.offline(bDown, true);
      }
      Run steadyInside = last(settle(steady));
      failover.run();
      failover.newLeader();
      List<Run> insideRuns = settle(failover);
      Run inside = last(insideRuns);
      Assert.assertNull(inside.computeFailure, inside.toString());
      Assert.assertTrue(insideRuns.stream().anyMatch(run -> run
          .skippedIn(ClusterModel.RebalanceScopeType.DELAYED_REBALANCE_OVERWRITES).contains("RA")),
          insideRuns.toString());
      Assert.assertEquals(ScopeMatrixSim.canon(served(inside).get("RA")), bestRA,
          "RA must be served as its best possible stood: " + inside);
      TestWagedIsolationScopeMatrix.assertToppedUp(failover, served(inside),
          failover.resourcesOf("B"), bDown);
      assertSameEnd(steadyInside, inside, "inside the window");

      for (ScopeMatrixSim sim : new ScopeMatrixSim[] {steady, failover}) {
        sim.offline(aDown, false);
        sim.offline(bDown, false);
      }
      Run steadyEnd = last(settle(steady));
      Run end = last(settle(failover));
      assertSameEnd(steadyEnd, end, "window end");
      Assert.assertFalse(uses(served(end), failover.resourcesOf("B"), bDown), end.toString());
      Assert.assertEquals(ScopeMatrixSim.canon(served(end).get("RA")), bestRA,
          "RA must still be served as its best possible stood: " + end);
      Assert.assertEquals(ScopeMatrixSim.canon(end.baselineAfter.get("RA")), baselineRA,
          "RA must keep its baseline: " + end);
    }
  }

  /** Runs one pipeline whose BASELINE write lands and whose BEST_POSSIBLE write does not. */
  private static void crashBetween(ScopeMatrixSim sim) throws Exception {
    Map<String, ResourceAssignment> baseline = sim.store.durableBaseline();
    Map<String, ResourceAssignment> best = sim.store.durableBest();
    sim.store.failBestPersist = true;
    Run crashed = sim.run();
    Assert.assertFalse(canon(sim.store.durableBaseline(), null).equals(canon(baseline, null)),
        "the BASELINE write must land: " + crashed);
    Assert.assertEquals(canon(sim.store.durableBest(), null), canon(best, null),
        "the BEST_POSSIBLE write must not land: " + crashed);
  }

  /** Both runs persist the same baseline and best possible and serve the same maps. */
  private static void assertSameEnd(Run expected, Run actual, String where) {
    Assert.assertEquals(canon(actual.baselineAfter, null), canon(expected.baselineAfter, null),
        where + ": baseline");
    Assert.assertEquals(canon(actual.bestAfter, null), canon(expected.bestAfter, null),
        where + ": best possible");
    Assert.assertEquals(canon(served(actual), null), canon(served(expected), null),
        where + ": served");
  }

  /** The distinct maps the runs served for the resources, in order of first appearance. */
  private static Set<Object> servedStates(List<Run> runs, Set<String> resources) {
    Set<Object> states = new LinkedHashSet<>();
    runs.forEach(run -> states.add(canon(served(run), resources)));
    return states;
  }

  private static Set<String> set(String... values) {
    return new TreeSet<>(Arrays.asList(values));
  }

  private static Object field(Object target, String name) throws ReflectiveOperationException {
    Field field = target.getClass().getDeclaredField(name);
    field.setAccessible(true);
    return field.get(target);
  }

  /**
   * Three cliques of two nodes with DISK capacity 100. The broken clique holds two resources and
   * breaks when one partition weighs 150; the healthy clique holds one resource.
   */
  private static final class Lifecycle implements AutoCloseable {
    private final ResourceControllerDataProvider _data = mock(ResourceControllerDataProvider.class);
    private final Map<String, Resource> _resources = new HashMap<>();
    private final Map<String, IdealState> _ideals = new HashMap<>();
    private final Map<String, ResourceConfig> _configs = new HashMap<>();
    private final Set<String> _nodes = new TreeSet<>();
    private final InMemoryAssignmentMetadata _zk = new InMemoryAssignmentMetadata();
    private final List<WagedRebalancer> _terms = new ArrayList<>();
    private volatile ClusterConfig _clusterConfig = clusterConfig(true);
    private ClusterStatusMonitor _monitor;
    private LatchAlgorithm _algorithm;
    private WagedRebalancer _rebalancer;

    private Lifecycle() throws IOException {
      Map<String, InstanceConfig> instances = new HashMap<>();
      Map<String, LiveInstance> live = new HashMap<>();
      for (int clique = 0; clique < 3; clique++) {
        for (int index = 0; index < 2; index++) {
          String name = "node_" + clique + "_" + index;
          InstanceConfig instance = new InstanceConfig(name);
          instance.addTag("clique_" + clique);
          instance.setInstanceOperation(InstanceConstants.InstanceOperation.ENABLE);
          instances.put(name, instance);
          live.put(name, new LiveInstance(name));
          _nodes.add(name);
        }
      }
      addResource("broken", "clique_0");
      addResource("sibling", "clique_0");
      addResource("healthy", "clique_2");
      when(_data.getClusterName()).thenReturn(CLUSTER);
      when(_data.getClusterConfig()).thenAnswer(call -> _clusterConfig);
      when(_data.getAssignableInstanceConfigMap()).thenReturn(instances);
      when(_data.getInstanceConfigMap()).thenReturn(instances);
      when(_data.getAssignableInstances()).thenReturn(instances.keySet());
      when(_data.getAssignableLiveInstances()).thenReturn(live);
      when(_data.getLiveInstances()).thenReturn(live);
      when(_data.getEnabledLiveInstances()).thenReturn(_nodes);
      when(_data.getIdealStates()).thenReturn(_ideals);
      when(_data.getIdealState(anyString())).thenAnswer(call -> _ideals.get(call.getArgument(0)));
      when(_data.getResourceConfigMap()).thenReturn(_configs);
      when(_data.getResourceConfig(anyString()))
          .thenAnswer(call -> _configs.get(call.getArgument(0)));
      when(_data.getStateModelDef(anyString()))
          .thenReturn(BuiltInStateModelDefinitions.OnlineOffline.getStateModelDefinition());
      when(_data.getRefreshedChangeTypes()).thenReturn(EnumSet.of(
          HelixConstants.ChangeType.CLUSTER_CONFIG, HelixConstants.ChangeType.INSTANCE_CONFIG,
          HelixConstants.ChangeType.IDEAL_STATE, HelixConstants.ChangeType.RESOURCE_CONFIG,
          HelixConstants.ChangeType.LIVE_INSTANCE));
      newTerm();
    }

    private static ClusterConfig clusterConfig(boolean isolation) {
      ClusterConfig config = new ClusterConfig(CLUSTER);
      config.setWagedInstanceTagIsolationEnabled(isolation);
      config.setInstanceCapacityKeys(Collections.singletonList("DISK"));
      config.setDefaultInstanceCapacityMap(Collections.singletonMap("DISK", 100));
      config.setDefaultPartitionWeightMap(Collections.singletonMap("DISK", 10));
      return config;
    }

    /**
     * What a newly elected Helix controller in another process builds: a new monitor, a new store
     * over the same persisted metadata, and a new rebalancer with the default async baseline.
     */
    private void newTerm() {
      _monitor = new ClusterStatusMonitor(CLUSTER);
      _algorithm = new LatchAlgorithm();
      _rebalancer = new WagedRebalancer(new AssignmentMetadataStore(_zk.connect(), CLUSTER),
          _algorithm, Optional.empty());
      _rebalancer.setGlobalRebalanceAsyncMode(true);
      _rebalancer.setPartialRebalanceAsyncMode(false);
      _rebalancer.setClusterStatusMonitor(_monitor);
      _terms.add(_rebalancer);
    }

    /** GenericHelixController.enableClusterStatusMonitor(false) on losing leadership. */
    private void loseLeadership() {
      _monitor.reset();
    }

    /** StatefulRebalancerRef.getRebalancer on the invalidated rebalancer of a new term. */
    private void regainLeadership() {
      _rebalancer.reset();
    }

    /** GenericHelixController.shutdown, which closes the rebalancer. */
    private void shutdown() {
      _terms.remove(_rebalancer);
      _rebalancer.close();
    }

    private void addResource(String name, String tag) throws IOException {
      Resource resource = new Resource(name);
      resource.addPartition(name + "_0");
      _resources.put(name, resource);
      IdealState ideal = new IdealState(name);
      ideal.setRebalanceMode(IdealState.RebalanceMode.FULL_AUTO);
      ideal.setRebalancerClassName(WagedRebalancer.class.getName());
      ideal.setStateModelDefRef(BuiltInStateModelDefinitions.OnlineOffline.name());
      ideal.setInstanceGroupTag(tag);
      ideal.setNumPartitions(1);
      ideal.setReplicas("1");
      ideal.setPreferenceList(name + "_0", Collections.emptyList());
      _ideals.put(name, ideal);
      _configs.put(name, new ResourceConfig(name));
      setWeight(name, 10);
    }

    private void setWeight(String name, int weight) throws IOException {
      _configs.get(name).setPartitionCapacityMap(Collections.singletonMap(
          ResourceConfig.DEFAULT_PARTITION_KEY, Collections.singletonMap("DISK", weight)));
    }

    private void setIsolation(boolean enabled) {
      _clusterConfig = clusterConfig(enabled);
    }

    /** Settle healthy, then break clique_0 so that it is carried forward and retried. */
    private void isolate() throws Exception {
      settle();
      Assert.assertEquals(new TreeSet<>(_zk.baseline().keySet()), ALL);
      Assert.assertEquals(gauge(), 0L);
      setWeight("broken", 150);
      int mark = _algorithm.baselines();
      settle();
      Assert.assertEquals(_algorithm.evaluatedAt(mark), set("broken"));
      Assert.assertEquals(_algorithm.skippedAt(mark), BLOCK);
      Assert.assertEquals(gauge(), 2L);
      Assert.assertEquals(retrySet(), BLOCK);
      assertComplete();
    }

    /**
     * Add a partition to the healthy resource and hold the resulting baseline inside calculate.
     * It evaluates the change plus the retried clique and will skip the clique again.
     */
    private int startBlockedBaseline() throws Exception {
      int mark = _algorithm.baselines();
      _resources.get("healthy").addPartition("healthy_1");
      IdealState ideal = _ideals.get("healthy");
      ideal.setNumPartitions(2);
      ideal.setPreferenceList("healthy_1", Collections.emptyList());
      _algorithm.arm();
      pipeline();
      _algorithm.awaitEntered();
      return mark;
    }

    private void pipeline() throws HelixRebalanceException {
      _rebalancer.computeBestPossibleAssignment(_data, _resources, _nodes,
          new CurrentStateOutput(), _algorithm);
    }

    private void settle() throws Exception {
      pipeline();
      drain();
      pipeline();
      drain();
    }

    private void releaseAndDrain() throws Exception {
      _algorithm.release();
      drain();
    }

    private void drain() throws Exception {
      executor().submit(() -> { }).get(WAIT_SECONDS, TimeUnit.SECONDS);
    }

    private ExecutorService executor() throws ReflectiveOperationException {
      return (ExecutorService) field(field(_rebalancer, "_globalRebalanceRunner"),
          "_baselineCalculateExecutor");
    }

    @SuppressWarnings("unchecked")
    private Set<String> retryReference() throws ReflectiveOperationException {
      return ((AtomicReference<Set<String>>) field(field(_rebalancer, "_globalRebalanceRunner"),
          "_isolationResourcesToRetry")).get();
    }

    private Set<String> retrySet() throws ReflectiveOperationException {
      return new TreeSet<>(retryReference());
    }

    private long gauge() {
      return _monitor.getWagedInstanceTagIsolationSkippedResourcesGauge();
    }

    /** Every partition of every resource has a replica in both persisted assignments. */
    private void assertComplete() {
      assertCompleteIn("BASELINE", _zk.baseline());
      assertCompleteIn("BEST_POSSIBLE", _zk.bestPossible());
    }

    private void assertCompleteIn(String name, Map<String, String> assignment) {
      Assert.assertEquals(new TreeSet<>(assignment.keySet()), ALL, name);
      for (Map.Entry<String, Resource> resource : _resources.entrySet()) {
        ZNRecord record = (ZNRecord) SERIALIZER.deserialize(
            assignment.get(resource.getKey()).getBytes(StandardCharsets.UTF_8));
        resource.getValue().getPartitions().forEach(partition -> {
          Map<String, String> replicas = record.getMapField(partition.getPartitionName());
          Assert.assertTrue(replicas != null && !replicas.isEmpty(),
              name + " half-assigns " + resource.getKey() + ": " + record.getMapFields());
        });
      }
    }

    @Override
    public void close() {
      _algorithm.release();
      _terms.forEach(WagedRebalancer::close);
    }
  }

  /** The production algorithm, able to hold one baseline inside calculate until released. */
  private static final class LatchAlgorithm implements RebalanceAlgorithm {
    private final RebalanceAlgorithm _delegate =
        ConstraintBasedAlgorithmFactory.getInstance(Collections.emptyMap());
    private final List<Set<String>> _evaluated = new CopyOnWriteArrayList<>();
    private final List<Set<String>> _skipped = new CopyOnWriteArrayList<>();
    private final AtomicBoolean _armed = new AtomicBoolean();
    private volatile CountDownLatch _entered = new CountDownLatch(0);
    private volatile CountDownLatch _release = new CountDownLatch(0);
    private volatile Set<String> _held = Collections.emptySet();

    private void arm() {
      _entered = new CountDownLatch(1);
      _release = new CountDownLatch(1);
      _armed.set(true);
    }

    private void awaitEntered() throws InterruptedException {
      Assert.assertTrue(_entered.await(WAIT_SECONDS, TimeUnit.SECONDS),
          "The baseline never reached calculate");
    }

    private void release() {
      _release.countDown();
    }

    /** Reported baselines. A baseline that is computed but superseded is not counted. */
    private int baselines() {
      return _evaluated.size();
    }

    /** The resources the held baseline evaluates, known even when it never reports. */
    private Set<String> held() {
      return _held;
    }

    private Set<String> evaluatedAt(int index) {
      Assert.assertTrue(_evaluated.size() > index, "Only " + _evaluated.size() + " baselines");
      return _evaluated.get(index);
    }

    private Set<String> skippedAt(int index) {
      Assert.assertTrue(_skipped.size() > index, "Only " + _skipped.size() + " baselines");
      return _skipped.get(index);
    }

    @Override
    public OptimalAssignment calculate(ClusterModel model) throws HelixRebalanceException {
      boolean interrupted = false;
      if (model.getRebalanceScopeType() == ClusterModel.RebalanceScopeType.GLOBAL_BASELINE
          && _armed.compareAndSet(true, false)) {
        _held = new TreeSet<>(model.getAssignableReplicaMap().keySet());
        _entered.countDown();
        // The production algorithm ignores interrupts, so shutdownNow cannot stop it either.
        while (true) {
          try {
            _release.await();
            break;
          } catch (InterruptedException e) {
            interrupted = true;
          }
        }
      }
      try {
        return _delegate.calculate(model);
      } finally {
        if (interrupted) {
          Thread.currentThread().interrupt();
        }
      }
    }

    @Override
    public void onAssignmentComputed(ClusterModel.RebalanceScopeType scope,
        Set<String> evaluatedResources, Set<String> skippedResources) {
      if (scope == ClusterModel.RebalanceScopeType.GLOBAL_BASELINE) {
        _evaluated.add(new TreeSet<>(evaluatedResources));
        _skipped.add(new TreeSet<>(skippedResources));
      }
    }
  }

  /**
   * Persisted assignment metadata shared by every store, like the ZooKeeper nodes are. Each
   * connection can be closed on its own; a closed connection rejects reads and writes.
   */
  private static final class InMemoryAssignmentMetadata {
    private static final String BASELINE = "/" + CLUSTER + "/ASSIGNMENT_METADATA/BASELINE";
    private static final String BEST_POSSIBLE =
        "/" + CLUSTER + "/ASSIGNMENT_METADATA/BEST_POSSIBLE";
    private final Map<String, ZNRecord> _znodes = new ConcurrentHashMap<>();
    private final AtomicInteger _baselineWrites = new AtomicInteger();
    private final AtomicInteger _writesAfterClose = new AtomicInteger();

    private BucketDataAccessor connect() {
      AtomicBoolean closed = new AtomicBoolean();
      return new BucketDataAccessor() {
        @Override
        public <T extends HelixProperty> boolean compressedBucketWrite(String path, T value) {
          if (closed.get()) {
            _writesAfterClose.incrementAndGet();
            throw new IllegalStateException("ZkClient already closed!");
          }
          _znodes.put(path, new ZNRecord(value.getRecord()));
          if (BASELINE.equals(path)) {
            _baselineWrites.incrementAndGet();
          }
          return true;
        }

        @Override
        public <T extends HelixProperty> HelixProperty compressedBucketRead(String path,
            Class<T> helixPropertySubType) {
          if (closed.get()) {
            throw new IllegalStateException("ZkClient already closed!");
          }
          ZNRecord record = _znodes.get(path);
          if (record == null) {
            throw new ZkNoNodeException(path);
          }
          return new HelixProperty(new ZNRecord(record));
        }

        @Override
        public void compressedBucketDelete(String path) {
          _znodes.remove(path);
        }

        @Override
        public void disconnect() {
          closed.set(true);
        }
      };
    }

    private Map<String, String> baseline() {
      return fields(BASELINE);
    }

    private Map<String, String> bestPossible() {
      return fields(BEST_POSSIBLE);
    }

    private Map<String, String> fields(String path) {
      ZNRecord record = _znodes.get(path);
      return record == null ? Collections.emptyMap() : new HashMap<>(record.getSimpleFields());
    }

    private int baselineWrites() {
      return _baselineWrites.get();
    }

    private int writesAfterClose() {
      return _writesAfterClose.get();
    }
  }
}
