package org.apache.helix.integration.rebalancer.WagedRebalancer;

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
import java.lang.management.ManagementFactory;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Predicate;
import javax.management.MBeanServer;
import javax.management.ObjectName;

import org.apache.helix.ConfigAccessor;
import org.apache.helix.HelixAdmin;
import org.apache.helix.HelixDataAccessor;
import org.apache.helix.HelixManager;
import org.apache.helix.HelixRebalanceException;
import org.apache.helix.NotificationContext;
import org.apache.helix.TestHelper;
import org.apache.helix.ZkTestHelper;
import org.apache.helix.common.ZkTestBase;
import org.apache.helix.constants.InstanceConstants;
import org.apache.helix.controller.GenericHelixController;
import org.apache.helix.controller.rebalancer.waged.AssignmentMetadataStore;
import org.apache.helix.controller.rebalancer.waged.RebalanceAlgorithm;
import org.apache.helix.controller.rebalancer.waged.WagedRebalancer;
import org.apache.helix.controller.rebalancer.waged.model.ClusterModel;
import org.apache.helix.controller.rebalancer.waged.model.OptimalAssignment;
import org.apache.helix.integration.manager.ClusterControllerManager;
import org.apache.helix.integration.manager.MockParticipantManager;
import org.apache.helix.manager.zk.ZKHelixDataAccessor;
import org.apache.helix.manager.zk.ZkBucketDataAccessor;
import org.apache.helix.mock.participant.MockTransition;
import org.apache.helix.model.BuiltInStateModelDefinitions;
import org.apache.helix.model.ClusterConfig;
import org.apache.helix.model.ExternalView;
import org.apache.helix.model.IdealState;
import org.apache.helix.model.InstanceConfig;
import org.apache.helix.model.Message;
import org.apache.helix.model.ResourceAssignment;
import org.apache.helix.model.ResourceConfig;
import org.apache.helix.monitoring.mbeans.ClusterStatusMonitor;
import org.apache.helix.monitoring.metrics.MetricCollector;
import org.apache.helix.monitoring.metrics.WagedRebalancerMetricCollector;
import org.apache.helix.monitoring.metrics.model.CountMetric;
import org.apache.helix.tools.ClusterVerifiers.StrictMatchExternalViewVerifier;
import org.apache.helix.tools.ClusterVerifiers.ZkHelixClusterVerifier;
import org.apache.zookeeper.data.Stat;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

/**
 * Instance tag isolation across Helix controller leadership changes, end to end on a real
 * ZooKeeper with real Helix controllers and participants.
 *
 * <p>A new leader inherits exactly one thing from the old one: the assignment metadata store in
 * ZooKeeper. The WAGED rebalancer, its change detector, the isolation retry set and the
 * ClusterStatus isolation gauge are all rebuilt by the new leader. Every scenario here checks that
 * the rebuilt state isolates a broken clique exactly the way the old leader did, and that no
 * failover leaves a half assigned resource in the store.
 */
public class TestWagedRebalanceInstanceTagIsolationFailover extends ZkTestBase {
  private static final int CLIQUE_COUNT = 3;
  private static final int NODES_PER_CLIQUE = 3;
  private static final int START_PORT = 14718;
  private static final int PARTITIONS = 3;
  private static final int REPLICA = 2;
  private static final String CAPACITY_KEY = "DISK";
  private static final int NODE_CAPACITY = 100;
  private static final int HEALTHY_WEIGHT = 5;
  private static final int BROKEN_CLIQUE = 0;
  private static final String SIBLING = "DB_clique_0_sibling";
  private static final String ISOLATION_GAUGE = "WagedInstanceTagIsolationSkippedResourcesGauge";
  private static final String METADATA = "ASSIGNMENT_METADATA";
  private static final String BASELINE = "BASELINE";
  private static final String BEST_POSSIBLE = "BEST_POSSIBLE";
  private static final int STABLE_SNAPSHOTS = 3;

  private final String CLASS_NAME = getShortClassName();
  private Fixture _cluster;

  enum BreakMode {
    // Heavier than any single node, far lighter than the cluster: the per clique failure.
    TAG_LOCAL(NODE_CAPACITY + 1),
    // Clique 0 alone needs more than the whole cluster holds: the cluster wide capacity deficit.
    CLUSTER_DEFICIT(4 * NODE_CAPACITY);

    private final int _weight;

    BreakMode(int weight) {
      _weight = weight;
    }
  }

  @DataProvider(name = "breakModes")
  public Object[][] breakModes() {
    return new Object[][] {{BreakMode.TAG_LOCAL}, {BreakMode.CLUSTER_DEFICIT}};
  }

  @BeforeClass
  public void beforeClass() throws Exception {
    System.out.println("START " + CLASS_NAME + " at " + new Date(System.currentTimeMillis()));
    _cluster = new Fixture(CLUSTER_PREFIX + "_" + CLASS_NAME, 2);
  }

  @AfterClass(alwaysRun = true)
  public void afterClass() throws Exception {
    if (_cluster != null) {
      _cluster.close();
    }
    System.out.println("END " + CLASS_NAME + " at " + new Date(System.currentTimeMillis()));
  }

  /**
   * Three leadership changes in a row, two session expiries and one clean stop, each
   * overlapping a change of clique 1 while clique 0 stays broken. The end state must be the one a
   * single Helix controller reaches on an identical cluster that never fails over. Runs first so
   * both clusters start from the same fresh first baseline.
   *
   * Each round makes exactly one change. With two changes in a round, how they are batched into
   * baselines depends on event timing in both clusters, so even two single Helix controllers can
   * legitimately end on different valid placements for the changing clique.
   */
  @Test(priority = 0)
  public void testLeadershipFlipFlopEndsWhereASingleHelixControllerEnds() throws Exception {
    Fixture flipping = _cluster;
    Fixture single = new Fixture(CLUSTER_PREFIX + "_" + CLASS_NAME + "_Single", 1);
    try {
      List<Fixture> both = Arrays.asList(flipping, single);
      awaitSameAssignments(flipping, single, "two fresh clusters with identical inputs");
      for (Fixture fixture : both) {
        fixture.breakClique(BreakMode.TAG_LOCAL);
      }
      for (Fixture fixture : both) {
        fixture.awaitIsolation(fixture.awaitLeader(), 1, snapshot -> true,
            "clique 0 must be isolated in " + fixture._clusterName);
      }
      Snapshot carried = flipping.awaitStableSnapshot(flipping.awaitLeader());
      awaitSameAssignments(flipping, single, "both clusters after breaking clique 0");

      String moving = flipping.node(1, 0);
      String changing = resourceName(1);
      try (BlobSampler sampler = flipping.sampler(false)) {
        // Round 1: the leader crashes while clique 1 starts evacuating a node.
        ClusterControllerManager leader = flipping.failOverBySessionExpiry(flipping.awaitLeader(),
            () -> both.forEach(fixture -> fixture.setInstanceOperation(moving,
                InstanceConstants.InstanceOperation.EVACUATE)));
        awaitRound(both, "round 1", "the evacuation", view -> !assignableIn(view, moving),
            snapshot -> !holds(snapshot._baseline, moving)
                && !holds(snapshot._bestPossible, moving));

        // Round 2: crash again. Leadership goes back to the first Helix controller, whose
        // rebalancer was only invalidated and now takes the reset path, while clique 1 gets a new
        // weight.
        leader = flipping.failOverBySessionExpiry(leader, () -> both.forEach(
            fixture -> fixture.setPartitionWeight(changing, HEALTHY_WEIGHT + 1)));
        awaitRound(both, "round 2", "the new weight",
            view -> weightIn(view, changing) == HEALTHY_WEIGHT + 1, snapshot -> true);

        // Round 3: a clean stop while the evacuated node of clique 1 comes back.
        flipping.stop(leader);
        both.forEach(fixture -> fixture.setInstanceOperation(moving,
            InstanceConstants.InstanceOperation.ENABLE));
        flipping.startController();
        awaitRound(both, "round 3", "the node coming back", view -> assignableIn(view, moving),
            snapshot -> true);
        sampler.assertClean("leadership flip flop");
      }
      for (Fixture fixture : both) {
        ClusterControllerManager leader = fixture.awaitLeader();
        Snapshot end = fixture.awaitStableSnapshot(leader);
        Assert.assertEquals(end._baseline.get(resourceName(BROKEN_CLIQUE)),
            carried._baseline.get(resourceName(BROKEN_CLIQUE)),
            "Clique 0 must still be the carried baseline in " + fixture._clusterName);
        Assert.assertEquals(end._bestPossible.get(resourceName(BROKEN_CLIQUE)),
            carried._bestPossible.get(resourceName(BROKEN_CLIQUE)),
            "Clique 0 must still be the carried best possible in " + fixture._clusterName);
        Assert.assertEquals(isolationGauge(leader), 1L);
        Assert.assertEquals(retrySet(rebalancer(leader)),
            Collections.singleton(resourceName(BROKEN_CLIQUE)));
      }
    } finally {
      single.close();
    }
  }

  /**
   * Clean stop of the leader while one clique is broken and the others are settled. The standby
   * takes over and must reproduce the store byte for byte without a single write or transition.
   */
  @Test(priority = 1, dataProvider = "breakModes")
  public void testCleanFailoverKeepsTheStoreByteIdentical(BreakMode mode) throws Exception {
    Fixture cluster = _cluster;
    cluster.restoreHealthy(2);
    ClusterControllerManager oldLeader = cluster.awaitLeader();
    cluster.breakClique(mode);
    cluster.awaitIsolation(oldLeader, 1, snapshot -> true, mode + ": isolate before failover");
    Snapshot before = cluster.awaitStableSnapshot(oldLeader);
    cluster.assertHealthyWithinCapacity(before, mode + " before failover");

    ClusterControllerManager standby = cluster.standbyOf(oldLeader);
    RecordingAlgorithm probe = RecordingAlgorithm.install(standby);
    ClusterStatusMonitor oldMonitor = monitor(oldLeader);
    int transitions = cluster._transitions.count();
    ClusterControllerManager newLeader;
    Snapshot after;
    try (BlobSampler sampler = cluster.sampler(false)) {
      cluster.stop(oldLeader);
      newLeader = cluster.awaitLeader();
      Assert.assertSame(newLeader, standby, "The standby Helix controller must take over");
      cluster.awaitIsolation(newLeader, 1, snapshot -> true, mode + ": rebuild after failover");
      // One more baseline on the new leader, from its own change detector this time.
      long calculations = baselineCalculations(rebalancer(newLeader));
      cluster.touchClusterConfig();
      await(mode + ": the new leader must run another baseline",
          () -> baselineCalculations(rebalancer(newLeader)) > calculations);
      after = cluster.awaitStableSnapshot(newLeader);
      sampler.assertClean(mode + " clean failover");
    }

    assertByteIdenticalFailover(before, after, mode + " clean failover");
    Assert.assertEquals(cluster._transitions.count(), transitions,
        mode + ": a byte identical failover must not send a single state transition");
    Assert.assertEquals(isolationGauge(newLeader), 1L);
    Assert.assertEquals(oldMonitor.getWagedInstanceTagIsolationSkippedResourcesGauge(), 0L,
        "The stopped leader's monitor must be reset");
    Assert.assertEquals(jmxIsolationGauge(cluster._clusterName), Long.valueOf(1L),
        "The registered ClusterStatus MBean must be the new leader's, reporting the broken clique");
    Assert.assertEquals(retrySet(rebalancer(newLeader)),
        Collections.singleton(resourceName(BROKEN_CLIQUE)));
    probe.assertFirstBaselineWasFull(cluster._resources,
        Collections.singleton(resourceName(BROKEN_CLIQUE)));
  }

  /**
   * The leader's ZooKeeper session expires instead. Leadership is lost uncleanly, the standby
   * takes over, and the old Helix controller reconnects as a standby that must stay silent.
   */
  @Test(priority = 2, dataProvider = "breakModes")
  public void testSessionExpiryFailoverKeepsTheStoreByteIdentical(BreakMode mode)
      throws Exception {
    Fixture cluster = _cluster;
    cluster.restoreHealthy(2);
    ClusterControllerManager oldLeader = cluster.awaitLeader();
    cluster.breakClique(mode);
    cluster.awaitIsolation(oldLeader, 1, snapshot -> true, mode + ": isolate before expiry");
    Snapshot before = cluster.awaitStableSnapshot(oldLeader);

    ClusterControllerManager standby = cluster.standbyOf(oldLeader);
    RecordingAlgorithm probe = RecordingAlgorithm.install(standby);
    WagedRebalancer oldRebalancer = rebalancer(oldLeader);
    long oldCalculations = baselineCalculations(oldRebalancer);
    String oldSession = oldLeader.getSessionId();
    int transitions = cluster._transitions.count();
    ClusterControllerManager newLeader;
    Snapshot after;
    try (BlobSampler sampler = cluster.sampler(false)) {
      newLeader = cluster.failOverBySessionExpiry(oldLeader, () -> { });
      Assert.assertSame(newLeader, standby, "The standby Helix controller must take over");
      cluster.awaitIsolation(newLeader, 1, snapshot -> true, mode + ": rebuild after expiry");
      await("The expired Helix controller must reconnect as a standby with a reset monitor",
          () -> oldLeader.isConnected() && !oldSession.equals(oldLeader.getSessionId())
              && !oldLeader.isLeader() && isolationGauge(oldLeader) == 0);
      long calculations = baselineCalculations(rebalancer(newLeader));
      cluster.touchClusterConfig();
      await(mode + ": the new leader must run another baseline",
          () -> baselineCalculations(rebalancer(newLeader)) > calculations);
      after = cluster.awaitStableSnapshot(newLeader);
      drain(oldRebalancer);
      sampler.assertClean(mode + " session expiry failover");
    }

    assertByteIdenticalFailover(before, after, mode + " session expiry failover");
    Assert.assertEquals(cluster._transitions.count(), transitions,
        mode + ": a byte identical failover must not send a single state transition");
    Assert.assertEquals(baselineCalculations(oldRebalancer), oldCalculations,
        "The expired Helix controller must not compute a baseline as a standby");
    Assert.assertSame(rebalancer(oldLeader), oldRebalancer,
        "The standby keeps its invalidated rebalancer until it leads again");
    Assert.assertEquals(isolationGauge(newLeader), 1L);
    Assert.assertEquals(isolationGauge(oldLeader), 0L);
    Assert.assertEquals(retrySet(rebalancer(newLeader)),
        Collections.singleton(resourceName(BROKEN_CLIQUE)));
    probe.assertFirstBaselineWasFull(cluster._resources,
        Collections.singleton(resourceName(BROKEN_CLIQUE)));
    // Both Helix controllers live in this JVM and share one MBean name, so the expired leader's
    // late reset may unregister the new leader's bean. That cannot happen across processes; the
    // object level gauges above are the real check. Whatever is registered must not be stale.
    Long registered = jmxIsolationGauge(cluster._clusterName);
    System.out.println(CLASS_NAME + " " + mode + " session expiry, registered isolation gauge: "
        + registered);
    Assert.assertTrue(registered == null || registered == 1L,
        "A registered ClusterStatus MBean must report the new leader's value, got " + registered);
  }

  /**
   * Topology changes that land right after a failover, on the new leader: EVACUATE, a new
   * node and a capacity shrink after a clean stop, then a participant crash and UNKNOWN after a
   * session expiry. The healthy cliques react, clique 0 stays carried and the store stays complete.
   */
  @Test(priority = 3)
  public void testTopologyChangesRightAfterFailoverStayIsolated() throws Exception {
    Fixture cluster = _cluster;
    cluster.restoreHealthy(2);
    ClusterControllerManager leader = cluster.awaitLeader();
    cluster.breakClique(BreakMode.TAG_LOCAL);
    cluster.awaitIsolation(leader, 1, snapshot -> true, "isolate before the failovers");
    Snapshot isolated = cluster.awaitStableSnapshot(leader);
    String evacuating = cluster.node(1, 0);
    String shrunk = cluster.node(2, 0);
    Assert.assertTrue(replicasOn(isolated._bestPossible, shrunk) >= 2,
        "The node to shrink must hold at least two replicas: " + isolated._bestPossible);

    try (BlobSampler sampler = cluster.sampler(false)) {
      cluster.stop(leader);
      cluster.setInstanceOperation(evacuating, InstanceConstants.InstanceOperation.EVACUATE);
      String added = cluster.addNode(2);
      cluster.setInstanceCapacity(shrunk, HEALTHY_WEIGHT);
      leader = cluster.awaitLeader();
      cluster.startController();
      cluster.awaitIsolation(leader, 1,
          snapshot -> !holds(snapshot._baseline, evacuating)
              && !holds(snapshot._bestPossible, evacuating)
              && replicasOn(snapshot._baseline, shrunk) <= 1
              && replicasOn(snapshot._bestPossible, shrunk) <= 1,
          "EVACUATE, a new node and a capacity shrink after a clean failover");
      Snapshot afterClean = cluster.awaitStableSnapshot(leader);
      assertCarried(isolated, afterClean, "after the clean failover");
      cluster.assertHealthyWithinCapacity(afterClean, "after the clean failover");
      System.out.println(CLASS_NAME + " added node " + added + " holds "
          + replicasOn(afterClean._baseline, added) + " baseline replicas");

      // UNKNOWN drops every replica of a node like a hard evacuation, so first bring the evacuated
      // node of clique 1 back. Every healthy clique keeps two assignable live nodes, otherwise it
      // would rightly break and be isolated as well.
      String killed = cluster.node(2, 1);
      String unknown = cluster.node(1, 1);
      leader = cluster.failOverBySessionExpiry(leader, () -> {
        cluster.setInstanceOperation(evacuating, InstanceConstants.InstanceOperation.ENABLE);
        cluster.killParticipant(killed);
        cluster.setInstanceOperation(unknown, InstanceConstants.InstanceOperation.UNKNOWN);
      });
      // The baseline keeps a dead but assignable node, only the best possible leaves it.
      cluster.awaitIsolation(leader, 1,
          snapshot -> !holds(snapshot._bestPossible, killed)
              && !holds(snapshot._baseline, unknown) && !holds(snapshot._bestPossible, unknown),
          "a participant crash and UNKNOWN after a session expiry failover");
      Snapshot afterExpiry = cluster.awaitStableSnapshot(leader);
      assertCarried(isolated, afterExpiry, "after the session expiry failover");
      cluster.assertHealthyWithinCapacity(afterExpiry, "after the session expiry failover");
      sampler.assertClean("topology changes after failover");
    }
  }

  /**
   * Repairing the broken clique after a failover. First with no Helix controller running at
   * all, then with a sibling resource on the broken clique, where the repair only changes the
   * broken resource. The new leader's first baseline is a full recompute, so it rebuilds the retry
   * set from scratch, and the incremental repair baseline must then retry the whole skipped block.
   */
  @Test(priority = 4)
  public void testRepairAfterFailoverIsPlacedByTheNewLeader() throws Exception {
    Fixture cluster = _cluster;
    cluster.restoreHealthy(1);
    ClusterControllerManager leader = cluster.awaitLeader();
    cluster.breakClique(BreakMode.TAG_LOCAL);
    cluster.awaitIsolation(leader, 1, snapshot -> true, "isolate before the repair");
    cluster.stopAllControllers();
    cluster.setPartitionWeight(resourceName(BROKEN_CLIQUE), HEALTHY_WEIGHT);
    ClusterControllerManager fresh = cluster.startController();
    Assert.assertSame(cluster.awaitLeader(), fresh);
    cluster.awaitStrictConvergence("a repair made while no Helix controller ran");
    await("the repaired clique must leave the gauge", () -> isolationGauge(fresh) == 0);
    Assert.assertTrue(retrySet(rebalancer(fresh)).isEmpty());

    cluster.startController();
    cluster.createTaggedResource(SIBLING, BROKEN_CLIQUE);
    cluster.awaitStrictConvergence("the sibling resource on clique 0");
    cluster.breakClique(BreakMode.TAG_LOCAL);
    cluster.awaitIsolation(fresh, 2, snapshot -> true, "clique 0 blocks both of its resources");
    Set<String> block = new TreeSet<>(Arrays.asList(resourceName(BROKEN_CLIQUE), SIBLING));
    Assert.assertEquals(retrySet(rebalancer(fresh)), block);

    ClusterControllerManager standby = cluster.standbyOf(fresh);
    RecordingAlgorithm probe = RecordingAlgorithm.install(standby);
    try (BlobSampler sampler = cluster.sampler(false)) {
      cluster.stop(fresh);
      ClusterControllerManager newLeader = cluster.awaitLeader();
      Assert.assertSame(newLeader, standby);
      cluster.awaitIsolation(newLeader, 2, snapshot -> true, "the block after failover");
      probe.assertFirstBaselineWasFull(cluster._resources, block);
      Assert.assertEquals(retrySet(rebalancer(newLeader)), block,
          "The new leader's first, full baseline must rebuild the retry set");

      // A sibling change while the clique is still broken is carried, not half applied.
      cluster.awaitStableSnapshot(newLeader);
      int baselinesBeforeChange = probe.baselines();
      cluster.setReplicas(SIBLING, REPLICA + 1);
      await("the sibling change must be evaluated and skipped",
          () -> probe.baselines() > baselinesBeforeChange && isolationGauge(newLeader) == 2);
      Snapshot stillBroken = cluster.awaitStableSnapshot(newLeader);
      stillBroken._baseline.get(SIBLING).values().forEach(states -> Assert
          .assertEquals(states.size(), REPLICA, "A carried sibling keeps its old replica count"));

      // Repair only the broken resource. The sibling has no new event of its own.
      int baselinesBeforeRepair = probe.baselines();
      cluster.setPartitionWeight(resourceName(BROKEN_CLIQUE), HEALTHY_WEIGHT);
      cluster.awaitIsolation(newLeader, 0,
          snapshot -> snapshot._baseline.get(SIBLING).values().stream()
              .allMatch(states -> states.size() == REPLICA + 1),
          "the repair must place the whole block, including the sibling's pending change");
      // Only DB_clique_0 changed, so without the retry set rebuilt by the first baseline the
      // repair would evaluate DB_clique_0 alone.
      Assert.assertEquals(probe.evaluatedAt(baselinesBeforeRepair), block,
          "The incremental repair baseline must retry exactly the skipped block");
      Assert.assertTrue(retrySet(rebalancer(newLeader)).isEmpty());
      sampler.assertClean("repair after failover");
    }
    cluster.dropResource(SIBLING);
  }

  /**
   * The clique breaks while no Helix controller runs at all. The first leader afterwards
   * starts with nothing but the store the previous leader wrote, and its first baseline must
   * isolate the clique by carrying that store forward.
   */
  @Test(priority = 5, dataProvider = "breakModes")
  public void testBreakWithoutAnyLeaderIsCarriedFromThePreviousStore(BreakMode mode)
      throws Exception {
    Fixture cluster = _cluster;
    cluster.restoreHealthy(1);
    Snapshot healthy = cluster.awaitStableSnapshot(cluster.awaitLeader());
    cluster.stopAllControllers();
    cluster.breakClique(mode);
    int transitions = cluster._transitions.count();
    try (BlobSampler sampler = cluster.sampler(false)) {
      ClusterControllerManager fresh = cluster.startController();
      Assert.assertSame(cluster.awaitLeader(), fresh);
      cluster.awaitIsolation(fresh, 1, snapshot -> true, mode + ": the first baseline isolates");
      Snapshot after = cluster.awaitStableSnapshot(fresh);
      sampler.assertClean(mode + " break without a leader");
      assertByteIdenticalFailover(healthy, after, mode + " break without a leader");
      Assert.assertEquals(cluster._transitions.count(), transitions);
      Assert.assertEquals(retrySet(rebalancer(fresh)),
          Collections.singleton(resourceName(BROKEN_CLIQUE)));
      Assert.assertEquals(monitor(fresh).getWagedBaselineComputeFailingGauge(), 0L);
    }
  }

  /**
   * The flag is turned off while no leader is present. The next leader must behave exactly like
   * stock Helix: the whole baseline fails, no baseline is written, so no clique reacts to a
   * topology change, and the isolation gauge stays 0. Stock Helix still runs the partial
   * rebalance against that frozen baseline, which may adjust the best possible. Turning the flag
   * back on resumes isolation on the same leader.
   */
  @Test(priority = 7)
  public void testFlagFlippedWhileNoLeaderIsHonouredByTheNextLeader() throws Exception {
    Fixture cluster = _cluster;
    cluster.restoreHealthy(1);
    ClusterControllerManager leader = cluster.awaitLeader();
    cluster.breakClique(BreakMode.TAG_LOCAL);
    cluster.awaitIsolation(leader, 1, snapshot -> true, "isolate before the flag flip");
    Snapshot isolated = cluster.awaitStableSnapshot(leader);
    cluster.stopAllControllers();
    cluster.setIsolationEnabled(false);
    String shrunk = cluster.node(2, 0);
    Assert.assertTrue(replicasOn(isolated._baseline, shrunk) >= 2);
    cluster.setInstanceCapacity(shrunk, HEALTHY_WEIGHT);

    try (BlobSampler sampler = cluster.sampler(false)) {
      ClusterControllerManager stock = cluster.startController();
      Assert.assertSame(cluster.awaitLeader(), stock);
      await("with the flag off the whole baseline must fail",
          () -> rebalancer(stock) != null && baselineCalculations(rebalancer(stock)) >= 1
              && monitor(stock).getWagedBaselineComputeFailingGauge() == 1L);
      Snapshot frozen = cluster.awaitStableSnapshot(stock);
      Assert.assertEquals(isolationGauge(stock), 0L);
      Assert.assertEquals(frozen._baseline, isolated._baseline,
          "Stock behaviour: the failing baseline must not be written");
      Assert.assertEquals(frozen._baselineWrites, isolated._baselineWrites);
      Assert.assertTrue(replicasOn(frozen._baseline, shrunk) >= 2,
          "Stock behaviour: the frozen baseline cannot react to the shrink in clique 2");
      Assert.assertEquals(frozen._bestPossible.get(resourceName(BROKEN_CLIQUE)),
          isolated._bestPossible.get(resourceName(BROKEN_CLIQUE)),
          "The broken clique must not move in the best possible either");
      Assert.assertTrue(retrySet(rebalancer(stock)).isEmpty());

      cluster.setIsolationEnabled(true);
      cluster.awaitIsolation(stock, 1,
          snapshot -> replicasOn(snapshot._baseline, shrunk) <= 1
              && replicasOn(snapshot._bestPossible, shrunk) <= 1,
          "turning the flag back on must resume isolation");
      Snapshot resumed = cluster.awaitStableSnapshot(stock);
      assertCarried(isolated, resumed, "after the flag came back on");
      cluster.assertHealthyWithinCapacity(resumed, "after the flag came back on");
      Assert.assertEquals(monitor(stock).getWagedBaselineComputeFailingGauge(), 0L);
      sampler.assertClean("flag flipped during the failover window");
    }
  }

  /**
   * A from scratch recompute by a new leader on unchanged cluster state reproduces the previous
   * leader's store byte for byte. If the store is lost while a clique is broken, stock Helix cannot
   * write a baseline at all, while isolation rebuilds a complete one, carrying the broken clique
   * from its current states.
   */
  @Test(priority = 9)
  public void testNewLeadersRebuildAByteIdenticalCompleteStore() throws Exception {
    Fixture cluster = _cluster;
    cluster.restoreHealthy(1);
    ClusterControllerManager leader = cluster.awaitLeader();
    cluster.breakClique(BreakMode.TAG_LOCAL);
    cluster.awaitIsolation(leader, 1, snapshot -> true, "isolate before the restarts");
    Snapshot reference = cluster.awaitStableSnapshot(leader);
    try (BlobSampler sampler = cluster.sampler(false)) {
      for (int round = 0; round < 2; round++) {
        cluster.stopAllControllers();
        ClusterControllerManager fresh = cluster.startController();
        Assert.assertSame(cluster.awaitLeader(), fresh);
        cluster.awaitIsolation(fresh, 1, snapshot -> true, "restart " + round);
        assertByteIdenticalFailover(reference, cluster.awaitStableSnapshot(fresh),
            "restart " + round);
      }
      sampler.assertClean("restarts with a broken clique");
    }

    Map<String, Map<String, Set<String>>> placement = nodeSets(cluster.readExternalViews());
    try (BlobSampler sampler = cluster.sampler(true)) {
      // Control: with the flag off a lost store cannot get a baseline back while clique 0 is
      // broken.
      cluster.stopAllControllers();
      cluster.setIsolationEnabled(false);
      cluster.deleteAssignmentMetadata();
      ClusterControllerManager stock = cluster.startController();
      await("stock Helix must fail the baseline",
          () -> rebalancer(stock) != null
              && monitor(stock).getWagedBaselineComputeFailingGauge() == 1L);
      Snapshot stockSnapshot = cluster.awaitStableSnapshot(stock);
      Assert.assertTrue(stockSnapshot._baseline.isEmpty(),
          "Stock Helix cannot write any baseline: " + stockSnapshot._baseline);

      cluster.setIsolationEnabled(true);
      cluster.awaitIsolation(stock, 1, snapshot -> !snapshot._baseline.isEmpty(),
          "isolation must rebuild a complete baseline from current states");
      Snapshot rebuilt = cluster.awaitStableSnapshot(stock);
      Assert.assertEquals(nodeSets(rebuilt._baseline).get(resourceName(BROKEN_CLIQUE)),
          placement.get(resourceName(BROKEN_CLIQUE)),
          "The broken clique must be carried from where its replicas actually are");
      Assert.assertEquals(nodeSets(rebuilt._bestPossible).get(resourceName(BROKEN_CLIQUE)),
          placement.get(resourceName(BROKEN_CLIQUE)));
      Assert.assertEquals(nodeSets(rebuilt._externalView), placement,
          "Rebuilding a lost store must not move a replica");
      sampler.assertClean("a lost store rebuilt with a broken clique");
    }
  }

  private void awaitRound(List<Fixture> clusters, String round, String change,
      Predicate<Object> seen, Predicate<Snapshot> condition) throws Exception {
    for (Fixture fixture : clusters) {
      fixture.awaitBaselineSaw(change, seen);
      fixture.awaitIsolation(fixture.awaitLeader(), 1, condition,
          round + " in " + fixture._clusterName);
    }
    awaitSameAssignments(clusters.get(0), clusters.get(1), round);
  }

  /** Whether the change detector snapshot of a baseline has the node as assignable. */
  private static boolean assignableIn(Object changeSnapshot, String node) {
    Map<String, InstanceConfig> assignable =
        invokeNoArg(changeSnapshot, "getAssignableInstanceConfigMap");
    return assignable.containsKey(node);
  }

  /**
   * The DISK partition weight a baseline's change detector snapshot saw for a resource, or -1 if
   * the snapshot has none.
   */
  private static int weightIn(Object changeSnapshot, String resource) {
    Map<String, ResourceConfig> resourceConfigs =
        invokeNoArg(changeSnapshot, "getResourceConfigMap");
    ResourceConfig resourceConfig = resourceConfigs.get(resource);
    try {
      Map<String, Integer> weights = resourceConfig == null ? null
          : resourceConfig.getPartitionCapacityMap().get(ResourceConfig.DEFAULT_PARTITION_KEY);
      return weights == null ? -1 : weights.getOrDefault(CAPACITY_KEY, -1);
    } catch (IOException e) {
      throw new IllegalStateException(e);
    }
  }

  @SuppressWarnings("unchecked")
  private static <T> T invokeNoArg(Object target, String name) {
    try {
      Method method = target.getClass().getDeclaredMethod(name);
      method.setAccessible(true);
      return (T) method.invoke(target);
    } catch (ReflectiveOperationException e) {
      throw new IllegalStateException(e);
    }
  }

  private void awaitSameAssignments(Fixture first, Fixture second, String what) throws Exception {
    AtomicReference<String> difference = new AtomicReference<>();
    boolean same = TestHelper.verify(() -> {
      Snapshot one = first.awaitStableSnapshot(first.awaitLeader());
      Snapshot two = second.awaitStableSnapshot(second.awaitLeader());
      boolean equal = one._baseline.equals(two._baseline)
          && one._bestPossible.equals(two._bestPossible)
          && one._externalView.equals(two._externalView);
      difference.set(equal ? null : "\n" + first._clusterName + ": " + one + "\n"
          + second._clusterName + ": " + two);
      return equal;
    }, TestHelper.WAIT_DURATION);
    Assert.assertTrue(same, what + ": the clusters must end in the same state" + difference.get());
  }

  private static void assertByteIdenticalFailover(Snapshot before, Snapshot after, String what) {
    Assert.assertEquals(after._baseline, before._baseline, what + ": BASELINE");
    Assert.assertEquals(after._bestPossible, before._bestPossible, what + ": BEST_POSSIBLE");
    Assert.assertEquals(after._externalView, before._externalView, what + ": external view");
    Assert.assertEquals(after._baselineWrites, before._baselineWrites,
        what + ": an identical recompute must not rewrite BASELINE");
    Assert.assertEquals(after._bestPossibleWrites, before._bestPossibleWrites,
        what + ": an identical recompute must not rewrite BEST_POSSIBLE");
  }

  private static void assertCarried(Snapshot before, Snapshot after, String what) {
    String broken = resourceName(BROKEN_CLIQUE);
    Assert.assertEquals(after._baseline.get(broken), before._baseline.get(broken),
        what + ": clique 0 baseline must be carried byte for byte");
    Assert.assertEquals(after._bestPossible.get(broken), before._bestPossible.get(broken),
        what + ": clique 0 best possible must be carried byte for byte");
    Assert.assertEquals(after._externalView.get(broken), before._externalView.get(broken),
        what + ": no clique 0 replica may move");
  }

  private static String cliqueTag(int clique) {
    return "clique_" + clique;
  }

  private static String resourceName(int clique) {
    return "DB_clique_" + clique;
  }

  private static int cliqueOf(String resource) {
    return SIBLING.equals(resource) ? BROKEN_CLIQUE
        : Integer.parseInt(resource.substring(resource.lastIndexOf('_') + 1));
  }

  private static boolean holds(Map<String, Map<String, Map<String, String>>> blob, String node) {
    return replicasOn(blob, node) > 0;
  }

  private static long replicasOn(Map<String, Map<String, Map<String, String>>> blob,
      String node) {
    return blob.values().stream().flatMap(partitions -> partitions.values().stream())
        .filter(states -> states.containsKey(node)).count();
  }

  private static Map<String, Map<String, Map<String, String>>> normalize(
      Map<String, ResourceAssignment> assignment) {
    Map<String, Map<String, Map<String, String>>> normalized = new TreeMap<>();
    assignment.forEach((resource, resourceAssignment) -> {
      Map<String, Map<String, String>> byPartition = new TreeMap<>();
      resourceAssignment.getMappedPartitions().forEach(partition -> byPartition
          .put(partition.getPartitionName(),
              new TreeMap<>(resourceAssignment.getReplicaMap(partition))));
      normalized.put(resource, byPartition);
    });
    return normalized;
  }

  private static Map<String, Map<String, Set<String>>> nodeSets(
      Map<String, Map<String, Map<String, String>>> blob) {
    Map<String, Map<String, Set<String>>> nodes = new TreeMap<>();
    blob.forEach((resource, partitions) -> {
      Map<String, Set<String>> byPartition = new TreeMap<>();
      partitions.forEach((partition, states) -> byPartition.put(partition,
          new TreeSet<>(states.keySet())));
      nodes.put(resource, byPartition);
    });
    return nodes;
  }

  private static void await(String what, TestHelper.Verifier condition) throws Exception {
    Assert.assertTrue(TestHelper.verify(condition, TestHelper.WAIT_DURATION), what);
  }

  private static Object readField(Object target, String name) {
    for (Class<?> type = target.getClass(); type != null; type = type.getSuperclass()) {
      try {
        Field field = type.getDeclaredField(name);
        field.setAccessible(true);
        return field.get(target);
      } catch (NoSuchFieldException next) {
        // Keep walking up the hierarchy.
      } catch (IllegalAccessException ex) {
        throw new IllegalStateException(ex);
      }
    }
    throw new IllegalStateException("No field " + name + " on " + target.getClass());
  }

  private static void writeField(Object target, String name, Object value) {
    for (Class<?> type = target.getClass(); type != null; type = type.getSuperclass()) {
      try {
        Field field = type.getDeclaredField(name);
        field.setAccessible(true);
        field.set(target, value);
        return;
      } catch (NoSuchFieldException next) {
        // Keep walking up the hierarchy.
      } catch (IllegalAccessException ex) {
        throw new IllegalStateException(ex);
      }
    }
    throw new IllegalStateException("No field " + name + " on " + target.getClass());
  }

  private static GenericHelixController helixController(ClusterControllerManager manager) {
    return (GenericHelixController) readField(manager, "_controller");
  }

  private static ClusterStatusMonitor monitor(ClusterControllerManager manager) {
    return (ClusterStatusMonitor) readField(helixController(manager), "_clusterStatusMonitor");
  }

  private static long isolationGauge(ClusterControllerManager manager) {
    return monitor(manager).getWagedInstanceTagIsolationSkippedResourcesGauge();
  }

  /** The Helix controller's WAGED rebalancer, or null if it has not led a pipeline yet. */
  private static WagedRebalancer rebalancer(ClusterControllerManager manager) {
    Object reference = readField(helixController(manager), "_rebalancerRef");
    return (WagedRebalancer) readField(reference, "_rebalancer");
  }

  private static long baselineCalculations(WagedRebalancer rebalancer) {
    if (rebalancer == null) {
      return 0L;
    }
    MetricCollector collector = (MetricCollector) readField(rebalancer, "_metricCollector");
    return collector.getMetric(
        WagedRebalancerMetricCollector.WagedRebalancerMetricNames.GlobalBaselineCalcCounter.name(),
        CountMetric.class).getValue();
  }

  @SuppressWarnings("unchecked")
  private static Set<String> retrySet(WagedRebalancer rebalancer) {
    Object runner = readField(rebalancer, "_globalRebalanceRunner");
    return new TreeSet<>(
        ((AtomicReference<Set<String>>) readField(runner, "_isolationResourcesToRetry")).get());
  }

  /** Waits until the rebalancer's baseline and partial executors have run everything queued. */
  private static void drain(WagedRebalancer rebalancer) throws Exception {
    if (rebalancer == null) {
      return;
    }
    drainExecutor(readField(readField(rebalancer, "_globalRebalanceRunner"),
        "_baselineCalculateExecutor"));
    drainExecutor(readField(readField(rebalancer, "_partialRebalanceRunner"),
        "_bestPossibleCalculateExecutor"));
  }

  private static void drainExecutor(Object executor) throws Exception {
    ExecutorService service = (ExecutorService) executor;
    try {
      service.submit(() -> { }).get(TestHelper.WAIT_DURATION, TimeUnit.MILLISECONDS);
    } catch (RejectedExecutionException closed) {
      service.awaitTermination(TestHelper.WAIT_DURATION, TimeUnit.MILLISECONDS);
    }
  }

  private static Long jmxIsolationGauge(String clusterName) throws Exception {
    MBeanServer server = ManagementFactory.getPlatformMBeanServer();
    ObjectName name = new ObjectName("ClusterStatus:cluster=" + clusterName);
    return server.isRegistered(name) ? (Long) server.getAttribute(name, ISOLATION_GAUGE) : null;
  }

  private static final class Snapshot {
    private final Map<String, Map<String, Map<String, String>>> _baseline;
    private final Map<String, Map<String, Map<String, String>>> _bestPossible;
    private final Map<String, Map<String, Map<String, String>>> _externalView;
    private final int _baselineWrites;
    private final int _bestPossibleWrites;

    private Snapshot(Map<String, Map<String, Map<String, String>>> baseline,
        Map<String, Map<String, Map<String, String>>> bestPossible,
        Map<String, Map<String, Map<String, String>>> externalView, int baselineWrites,
        int bestPossibleWrites) {
      _baseline = baseline;
      _bestPossible = bestPossible;
      _externalView = externalView;
      _baselineWrites = baselineWrites;
      _bestPossibleWrites = bestPossibleWrites;
    }

    @Override
    public boolean equals(Object other) {
      if (!(other instanceof Snapshot)) {
        return false;
      }
      Snapshot that = (Snapshot) other;
      return _baseline.equals(that._baseline) && _bestPossible.equals(that._bestPossible)
          && _externalView.equals(that._externalView) && _baselineWrites == that._baselineWrites
          && _bestPossibleWrites == that._bestPossibleWrites;
    }

    @Override
    public int hashCode() {
      return Objects.hash(_baseline, _bestPossible, _externalView, _baselineWrites,
          _bestPossibleWrites);
    }

    @Override
    public String toString() {
      return "baseline=" + _baseline + " bestPossible=" + _bestPossible + " externalView="
          + _externalView + " writes=" + _baselineWrites + "/" + _bestPossibleWrites;
    }
  }

  /** Counts every state transition any participant executes. */
  private static final class CountingTransition extends MockTransition {
    private final AtomicInteger _count = new AtomicInteger();

    @Override
    public void doTransition(Message message, NotificationContext context) {
      _count.incrementAndGet();
    }

    private int count() {
      return _count.get();
    }
  }

  /**
   * Wraps the algorithm of a standby's WAGED rebalancer, created exactly as the Helix controller
   * would create it lazily, to record what the first pipelines of its leadership term evaluate.
   */
  private static final class RecordingAlgorithm implements RebalanceAlgorithm {
    private final RebalanceAlgorithm _delegate;
    private final List<Set<String>> _evaluated = new CopyOnWriteArrayList<>();
    private final List<Set<String>> _skipped = new CopyOnWriteArrayList<>();

    private RecordingAlgorithm(RebalanceAlgorithm delegate) {
      _delegate = delegate;
    }

    private static RecordingAlgorithm install(ClusterControllerManager standby) throws Exception {
      Assert.assertFalse(standby.isLeader(), "The probe must be installed on a standby");
      // A standby that led before keeps its invalidated rebalancer, and the reset it gets on the
      // next leadership keeps the algorithm, so only wrap it. A standby that never led gets its
      // rebalancer created exactly the way its first pipeline would create it.
      WagedRebalancer rebalancer = rebalancer(standby);
      if (rebalancer == null) {
        Object reference = readField(helixController(standby), "_rebalancerRef");
        Method getRebalancer = reference.getClass().getSuperclass()
            .getDeclaredMethod("getRebalancer", HelixManager.class);
        getRebalancer.setAccessible(true);
        rebalancer = (WagedRebalancer) getRebalancer.invoke(reference, standby);
      }
      RecordingAlgorithm recorder =
          new RecordingAlgorithm((RebalanceAlgorithm) readField(rebalancer, "_rebalanceAlgorithm"));
      writeField(rebalancer, "_rebalanceAlgorithm", recorder);
      return recorder;
    }

    @Override
    public OptimalAssignment calculate(ClusterModel clusterModel) throws HelixRebalanceException {
      return _delegate.calculate(clusterModel);
    }

    @Override
    public void onAssignmentComputed(ClusterModel.RebalanceScopeType scope,
        Set<String> evaluatedResources, Set<String> skippedResources) {
      if (scope == ClusterModel.RebalanceScopeType.GLOBAL_BASELINE) {
        _evaluated.add(new TreeSet<>(evaluatedResources));
        _skipped.add(new TreeSet<>(skippedResources));
      }
      _delegate.onAssignmentComputed(scope, evaluatedResources, skippedResources);
    }

    private int baselines() {
      return _evaluated.size();
    }

    private Set<String> evaluatedAt(int index) {
      Assert.assertTrue(_evaluated.size() > index, "No baseline ran at index " + index);
      return _evaluated.get(index);
    }

    private void assertFirstBaselineWasFull(Set<String> resources, Set<String> skipped) {
      Assert.assertFalse(_evaluated.isEmpty(), "The new leader never ran a baseline");
      Assert.assertEquals(_evaluated.get(0), new TreeSet<>(resources),
          "The new leader's first baseline must evaluate every resource");
      Assert.assertEquals(_skipped.get(0), new TreeSet<>(skipped),
          "The new leader's first baseline must skip exactly the broken block");
    }
  }

  /**
   * Samples both persisted blobs in the background and records any sample that is not complete:
   * a required resource missing, a partition missing, a partition with fewer replicas than
   * required, or a replica outside its clique. A blob may be empty only when allowed.
   */
  private final class BlobSampler implements AutoCloseable {
    private final Fixture _fixture;
    private final boolean _allowEmpty;
    private final AssignmentMetadataStore _reader;
    private final AtomicBoolean _running = new AtomicBoolean(true);
    private final List<String> _violations = new CopyOnWriteArrayList<>();
    private final AtomicInteger _samples = new AtomicInteger();
    private final Thread _thread;

    private BlobSampler(Fixture fixture, boolean allowEmpty) {
      _fixture = fixture;
      _allowEmpty = allowEmpty;
      _reader = fixture.newReadThroughStore();
      _thread = new Thread(this::sample, "IsolationFailoverBlobSampler");
      _thread.setDaemon(true);
      _thread.start();
    }

    private void sample() {
      while (_running.get()) {
        try {
          check(BASELINE, normalize(_reader.getBaseline()));
          check(BEST_POSSIBLE, normalize(_reader.getBestPossibleAssignment()));
          _samples.incrementAndGet();
        } catch (Exception transientRead) {
          // A read can race the bucket accessor's cleanup of an old version; take the next one.
        }
        try {
          Thread.sleep(20);
        } catch (InterruptedException stop) {
          return;
        }
      }
    }

    private void check(String blobName, Map<String, Map<String, Map<String, String>>> blob) {
      String problem = _fixture.incompleteness(blob, _allowEmpty);
      if (problem != null && _violations.size() < 20) {
        _violations.add(blobName + ": " + problem);
      }
    }

    private void assertClean(String what) {
      Assert.assertTrue(_samples.get() > 0, what + ": the sampler never read the store");
      Assert.assertTrue(_violations.isEmpty(),
          what + ": the store held a half assigned resource " + _violations);
      System.out.println(CLASS_NAME + " " + what + ": " + _samples.get()
          + " complete store samples");
    }

    @Override
    public void close() throws Exception {
      _running.set(false);
      _thread.join(TestHelper.WAIT_DURATION);
      _reader.close();
    }
  }

  /** One cluster of three cliques with three nodes each and one tagged resource per clique. */
  private final class Fixture implements AutoCloseable {
    private final String _clusterName;
    private final ConfigAccessor _configAccessor = new ConfigAccessor(_gZkClient);
    private final HelixAdmin _admin = _gSetupTool.getClusterManagementTool();
    private final Map<Integer, List<String>> _nodesByClique = new ConcurrentHashMap<>();
    private final Map<String, MockParticipantManager> _participants = new ConcurrentHashMap<>();
    private final List<ClusterControllerManager> _controllers = new CopyOnWriteArrayList<>();
    private final Set<String> _resources = ConcurrentHashMap.newKeySet();
    private final CountingTransition _transitions = new CountingTransition();
    private final AssignmentMetadataStore _store;
    private int _nextPort = START_PORT + CLIQUE_COUNT * NODES_PER_CLIQUE;
    private int _controllerSequence;

    private Fixture(String clusterName, int controllers) throws Exception {
      _clusterName = clusterName;
      _gSetupTool.addCluster(_clusterName, true);
      ClusterConfig clusterConfig = _configAccessor.getClusterConfig(_clusterName);
      clusterConfig.setInstanceCapacityKeys(Collections.singletonList(CAPACITY_KEY));
      clusterConfig.setDefaultInstanceCapacityMap(
          Collections.singletonMap(CAPACITY_KEY, NODE_CAPACITY));
      clusterConfig.setDefaultPartitionWeightMap(
          Collections.singletonMap(CAPACITY_KEY, HEALTHY_WEIGHT));
      clusterConfig.setWagedInstanceTagIsolationEnabled(true);
      _configAccessor.setClusterConfig(_clusterName, clusterConfig);
      enablePersistBestPossibleAssignment(_gZkClient, _clusterName, true);

      int port = START_PORT;
      for (int clique = 0; clique < CLIQUE_COUNT; clique++) {
        _nodesByClique.put(clique, new CopyOnWriteArrayList<>());
        for (int index = 0; index < NODES_PER_CLIQUE; index++) {
          addInstance(PARTICIPANT_PREFIX + "_" + port++, clique);
        }
      }
      for (List<String> nodes : _nodesByClique.values()) {
        nodes.forEach(this::startParticipant);
      }
      // Every resource exists before the first Helix controller starts, so the first baseline is
      // one deterministic full computation.
      for (int clique = 0; clique < CLIQUE_COUNT; clique++) {
        createTaggedResource(resourceName(clique), clique);
      }
      _store = newReadThroughStore();
      for (int index = 0; index < controllers; index++) {
        startController();
      }
      awaitStrictConvergence("a fresh cluster must converge");
    }

    private AssignmentMetadataStore newReadThroughStore() {
      // Every read goes to ZooKeeper and returns a defensive copy, because reset() clears the very
      // map instance it handed out earlier.
      return new AssignmentMetadataStore(new ZkBucketDataAccessor(ZK_ADDR), _clusterName) {
        @Override
        public synchronized Map<String, ResourceAssignment> getBaseline() {
          super.reset();
          return new HashMap<>(super.getBaseline());
        }

        @Override
        public synchronized Map<String, ResourceAssignment> getBestPossibleAssignment() {
          super.reset();
          return new HashMap<>(super.getBestPossibleAssignment());
        }
      };
    }

    private void addInstance(String node, int clique) {
      _nodesByClique.get(clique).add(node);
      _gSetupTool.addInstanceToCluster(_clusterName, node);
      _admin.addInstanceTag(_clusterName, node, cliqueTag(clique));
    }

    private void startParticipant(String node) {
      MockParticipantManager participant = new MockParticipantManager(ZK_ADDR, _clusterName, node);
      participant.setTransition(_transitions);
      participant.syncStart();
      _participants.put(node, participant);
    }

    private void killParticipant(String node) {
      _participants.get(node).syncStop();
    }

    private String addNode(int clique) {
      String node = PARTICIPANT_PREFIX + "_" + _nextPort++;
      addInstance(node, clique);
      startParticipant(node);
      return node;
    }

    private String node(int clique, int index) {
      return _nodesByClique.get(clique).get(index);
    }

    private void createTaggedResource(String resource, int clique) {
      _resources.add(resource);
      createResourceWithWagedRebalance(_clusterName, resource,
          BuiltInStateModelDefinitions.MasterSlave.name(), PARTITIONS, REPLICA, REPLICA);
      IdealState idealState = _admin.getResourceIdealState(_clusterName, resource);
      idealState.setInstanceGroupTag(cliqueTag(clique));
      _admin.setResourceIdealState(_clusterName, resource, idealState);
      _gSetupTool.rebalanceStorageCluster(_clusterName, resource, REPLICA);
    }

    private void dropResource(String resource) throws Exception {
      _admin.dropResource(_clusterName, resource);
      await(resource + " must leave the store", () -> {
        drain(rebalancer(awaitLeader()));
        return !_store.getBaseline().containsKey(resource)
            && !_store.getBestPossibleAssignment().containsKey(resource);
      });
      _resources.remove(resource);
    }

    private void setReplicas(String resource, int replicas) {
      IdealState idealState = _admin.getResourceIdealState(_clusterName, resource);
      idealState.setReplicas(String.valueOf(replicas));
      _admin.setResourceIdealState(_clusterName, resource, idealState);
    }

    private ClusterControllerManager startController() {
      ClusterControllerManager controller = new ClusterControllerManager(ZK_ADDR, _clusterName,
          CONTROLLER_PREFIX + "_" + _controllerSequence++);
      controller.syncStart();
      _controllers.add(controller);
      return controller;
    }

    private void stop(ClusterControllerManager controller) {
      controller.syncStop();
      _controllers.remove(controller);
    }

    private void stopAllControllers() {
      new ArrayList<>(_controllers).forEach(this::stop);
    }

    private ClusterControllerManager awaitLeader() throws Exception {
      AtomicReference<ClusterControllerManager> leader = new AtomicReference<>();
      await("exactly one Helix controller must lead " + _clusterName, () -> {
        List<ClusterControllerManager> leaders = new ArrayList<>();
        for (ClusterControllerManager controller : _controllers) {
          if (controller.isConnected() && controller.isLeader()) {
            leaders.add(controller);
          }
        }
        leader.set(leaders.size() == 1 ? leaders.get(0) : null);
        return leader.get() != null;
      });
      return leader.get();
    }

    private ClusterControllerManager standbyOf(ClusterControllerManager leader) {
      List<ClusterControllerManager> others = new ArrayList<>(_controllers);
      others.remove(leader);
      Assert.assertEquals(others.size(), 1, "Expected exactly one standby Helix controller");
      return others.get(0);
    }

    /**
     * Expires the leader's session and applies the given change in the failover window. The old
     * leader can occasionally win the re-election, in which case its new session is expired again.
     */
    private ClusterControllerManager failOverBySessionExpiry(ClusterControllerManager leader,
        Runnable duringFailover) throws Exception {
      String expiredSession = leader.getSessionId();
      ZkTestHelper.asyncExpireSession(leader.getZkClient());
      duringFailover.run();
      for (int attempt = 0; attempt < 3; attempt++) {
        String session = expiredSession;
        await(leader.getInstanceName() + " must reconnect with a new session",
            () -> leader.isConnected() && !session.equals(leader.getSessionId()));
        ClusterControllerManager newLeader = awaitLeader();
        if (newLeader != leader) {
          return newLeader;
        }
        expiredSession = leader.getSessionId();
        ZkTestHelper.expireSession(leader.getZkClient());
      }
      throw new AssertionError(leader.getInstanceName() + " kept winning the re-election");
    }

    private void breakClique(BreakMode mode) {
      setPartitionWeight(resourceName(BROKEN_CLIQUE), mode._weight);
    }

    private void setPartitionWeight(String resource, int weight) {
      ResourceConfig resourceConfig = _configAccessor.getResourceConfig(_clusterName, resource);
      if (resourceConfig == null) {
        resourceConfig = new ResourceConfig(resource);
      }
      Map<String, Map<String, Integer>> weights = Collections.singletonMap(
          ResourceConfig.DEFAULT_PARTITION_KEY, Collections.singletonMap(CAPACITY_KEY, weight));
      try {
        if (weights.equals(resourceConfig.getPartitionCapacityMap())) {
          return;
        }
        resourceConfig.setPartitionCapacityMap(weights);
      } catch (java.io.IOException ex) {
        throw new IllegalStateException(ex);
      }
      _configAccessor.setResourceConfig(_clusterName, resource, resourceConfig);
    }

    private int partitionWeight(String resource) {
      ResourceConfig resourceConfig = _configAccessor.getResourceConfig(_clusterName, resource);
      try {
        Map<String, Map<String, Integer>> weights =
            resourceConfig == null ? null : resourceConfig.getPartitionCapacityMap();
        return weights == null || !weights.containsKey(ResourceConfig.DEFAULT_PARTITION_KEY)
            ? HEALTHY_WEIGHT
            : weights.get(ResourceConfig.DEFAULT_PARTITION_KEY).get(CAPACITY_KEY);
      } catch (java.io.IOException ex) {
        throw new IllegalStateException(ex);
      }
    }

    private void setInstanceCapacity(String node, int capacity) {
      InstanceConfig instanceConfig = _configAccessor.getInstanceConfig(_clusterName, node);
      if (Collections.singletonMap(CAPACITY_KEY, capacity)
          .equals(instanceConfig.getInstanceCapacityMap())) {
        return;
      }
      instanceConfig.setInstanceCapacityMap(Collections.singletonMap(CAPACITY_KEY, capacity));
      _configAccessor.setInstanceConfig(_clusterName, node, instanceConfig);
    }

    private int capacity(String node) {
      Map<String, Integer> capacity =
          _configAccessor.getInstanceConfig(_clusterName, node).getInstanceCapacityMap();
      return capacity == null || !capacity.containsKey(CAPACITY_KEY) ? NODE_CAPACITY
          : capacity.get(CAPACITY_KEY);
    }

    private void setInstanceOperation(String node, InstanceConstants.InstanceOperation operation) {
      _admin.setInstanceOperation(_clusterName, node, operation);
    }

    private void setIsolationEnabled(boolean enabled) {
      ClusterConfig clusterConfig = _configAccessor.getClusterConfig(_clusterName);
      if (clusterConfig.isWagedInstanceTagIsolationEnabled() == enabled) {
        return;
      }
      clusterConfig.setWagedInstanceTagIsolationEnabled(enabled);
      _configAccessor.setClusterConfig(_clusterName, clusterConfig);
    }

    /**
     * Forces a full baseline recompute without changing a single input. The change detector drops
     * every ClusterConfig field except a few topology ones, so toggle the presence of one of those,
     * TOPOLOGY_AWARE_ENABLED, between absent and its default value false.
     */
    private void touchClusterConfig() {
      ClusterConfig clusterConfig = _configAccessor.getClusterConfig(_clusterName);
      String field = ClusterConfig.ClusterConfigProperty.TOPOLOGY_AWARE_ENABLED.name();
      if (clusterConfig.getRecord().getSimpleFields().remove(field) == null) {
        clusterConfig.getRecord().setBooleanField(field, false);
      }
      _configAccessor.setClusterConfig(_clusterName, clusterConfig);
    }

    /**
     * Waits until the leader's global baseline has run over data that contains a change, read
     * from the snapshot its change detector took for that baseline, then waits for that baseline
     * to finish. A comparison made after this cannot pass before the change was seen.
     */
    private void awaitBaselineSaw(String what, Predicate<Object> seen) throws Exception {
      await(_clusterName + ": a baseline must see " + what, () -> {
        ClusterControllerManager leader = awaitLeader();
        WagedRebalancer rebalancer = rebalancer(leader);
        if (rebalancer == null) {
          return false;
        }
        Object detector = readField(readField(rebalancer, "_globalRebalanceRunner"),
            "_changeDetector");
        Object snapshot = readField(detector, "_newSnapshot");
        // A reset leaves an empty snapshot, with no ClusterConfig, until a pipeline takes one.
        if (snapshot == null || invokeNoArg(snapshot, "getClusterConfig") == null
            || !seen.test(snapshot)) {
          return false;
        }
        // The pipeline that took the snapshot submits its baseline before its event thread next
        // waits, on ZooKeeper or on the event queue, so the drain below cannot run ahead of it.
        Thread.State state =
            ((Thread) readField(helixController(leader), "_eventThread")).getState();
        return state == Thread.State.WAITING || state == Thread.State.TIMED_WAITING;
      });
      drain(rebalancer(awaitLeader()));
    }

    private void deleteAssignmentMetadata() {
      _gZkClient.deleteRecursively("/" + _clusterName + "/" + METADATA);
    }

    private int writeVersion(String blob) {
      String path = "/" + _clusterName + "/" + METADATA + "/" + blob + "/LAST_WRITE";
      Stat stat = _gZkClient.exists(path) ? _gZkClient.getStat(path) : null;
      return stat == null ? -1 : stat.getVersion();
    }

    private Map<String, Map<String, Map<String, String>>> readExternalViews() {
      HelixDataAccessor accessor = new ZKHelixDataAccessor(_clusterName, _baseAccessor);
      Map<String, Map<String, Map<String, String>>> views = new TreeMap<>();
      for (String resource : new TreeSet<>(_resources)) {
        ExternalView externalView =
            accessor.getProperty(accessor.keyBuilder().externalView(resource));
        if (externalView == null) {
          continue;
        }
        Map<String, Map<String, String>> byPartition = new TreeMap<>();
        externalView.getPartitionSet().forEach(partition -> byPartition
            .put(partition, new TreeMap<>(externalView.getStateMap(partition))));
        views.put(resource, byPartition);
      }
      return views;
    }

    private Snapshot snapshot() {
      return new Snapshot(normalize(_store.getBaseline()),
          normalize(_store.getBestPossibleAssignment()), readExternalViews(),
          writeVersion(BASELINE), writeVersion(BEST_POSSIBLE));
    }

    /** Null if the blob is complete, otherwise a description of what is missing. */
    private String incompleteness(Map<String, Map<String, Map<String, String>>> blob,
        boolean allowEmpty) {
      if (blob.isEmpty() && allowEmpty) {
        return null;
      }
      for (int clique = 0; clique < CLIQUE_COUNT; clique++) {
        if (!blob.containsKey(resourceName(clique))) {
          return resourceName(clique) + " is missing from " + blob.keySet();
        }
      }
      for (Map.Entry<String, Map<String, Map<String, String>>> entry : blob.entrySet()) {
        String resource = entry.getKey();
        if (!resource.startsWith("DB_clique_")) {
          continue;
        }
        if (entry.getValue().size() != PARTITIONS) {
          return resource + " has partitions " + entry.getValue().keySet();
        }
        List<String> clique = _nodesByClique.get(cliqueOf(resource));
        for (Map.Entry<String, Map<String, String>> partition : entry.getValue().entrySet()) {
          if (partition.getValue().size() < REPLICA) {
            return partition.getKey() + " has only " + partition.getValue();
          }
          if (!clique.containsAll(partition.getValue().keySet())) {
            return partition.getKey() + " left its clique: " + partition.getValue();
          }
        }
      }
      return null;
    }

    private boolean externalViewMatchesBestPossible(Snapshot snapshot) {
      for (String resource : _resources) {
        Map<String, Map<String, String>> expected = snapshot._bestPossible.get(resource);
        Map<String, Map<String, String>> actual = snapshot._externalView.get(resource);
        if (expected == null || actual == null || !expected.keySet().equals(actual.keySet())) {
          return false;
        }
        for (String partition : expected.keySet()) {
          Set<String> serving = new TreeSet<>();
          actual.get(partition).forEach((node, state) -> {
            if ("MASTER".equals(state) || "SLAVE".equals(state)) {
              serving.add(node);
            }
          });
          if (!serving.equals(actual.get(partition).keySet())
              || !serving.equals(expected.get(partition).keySet())) {
            return false;
          }
        }
      }
      return true;
    }

    /**
     * Waits until the leader reports the expected isolation gauge, the store is complete, the
     * external view serves the persisted best possible assignment and the condition holds.
     */
    private void awaitIsolation(ClusterControllerManager leader, long gauge,
        Predicate<Snapshot> condition, String what) throws Exception {
      AtomicReference<String> last = new AtomicReference<>();
      boolean reached = TestHelper.verify(() -> {
        WagedRebalancer rebalancer = rebalancer(leader);
        if (rebalancer == null || !leader.isLeader()) {
          last.set("no leading rebalancer");
          return false;
        }
        drain(rebalancer);
        Snapshot snapshot = snapshot();
        last.set("gauge=" + isolationGauge(leader) + " " + snapshot);
        return isolationGauge(leader) == gauge && incompleteness(snapshot._baseline, false) == null
            && incompleteness(snapshot._bestPossible, false) == null
            && externalViewMatchesBestPossible(snapshot) && condition.test(snapshot);
      }, TestHelper.WAIT_DURATION);
      Assert.assertTrue(reached, what + ": " + last.get());
    }

    /** Drains the leader's rebalancer until several consecutive snapshots are identical. */
    private Snapshot awaitStableSnapshot(ClusterControllerManager leader) throws Exception {
      AtomicReference<Snapshot> previous = new AtomicReference<>();
      AtomicInteger identical = new AtomicInteger();
      await("the store and the external view of " + _clusterName + " must settle", () -> {
        drain(rebalancer(leader));
        Snapshot now = snapshot();
        identical.set(now.equals(previous.get()) ? identical.get() + 1 : 0);
        previous.set(now);
        return identical.get() >= STABLE_SNAPSHOTS - 1;
      });
      return previous.get();
    }

    private void assertHealthyWithinCapacity(Snapshot snapshot, String what) {
      for (Map<String, Map<String, Map<String, String>>> blob : Arrays.asList(snapshot._baseline,
          snapshot._bestPossible)) {
        Map<String, Integer> load = new TreeMap<>();
        blob.forEach((resource, partitions) -> {
          if (cliqueOf(resource) == BROKEN_CLIQUE) {
            return;
          }
          int weight = partitionWeight(resource);
          partitions.values().forEach(
              states -> states.keySet().forEach(node -> load.merge(node, weight, Integer::sum)));
        });
        load.forEach((node, used) -> Assert.assertTrue(used <= capacity(node),
            what + ": " + node + " holds " + used + " but has " + capacity(node)));
      }
    }

    private void awaitStrictConvergence(String what) throws Exception {
      try (ZkHelixClusterVerifier verifier = new StrictMatchExternalViewVerifier.Builder(
          _clusterName).setZkAddr(ZK_ADDR).setDeactivatedNodeAwareness(true)
          .setResources(new HashSet<>(_resources))
          .setWaitTillVerify(TestHelper.DEFAULT_REBALANCE_PROCESSING_WAIT_TIME).build()) {
        Assert.assertTrue(verifier.verifyByPolling(), what);
      }
    }

    private BlobSampler sampler(boolean allowEmpty) {
      return new BlobSampler(this, allowEmpty);
    }

    /**
     * Puts the cluster back to healthy, fully enabled, with the given number of Helix controllers.
     */
    private void restoreHealthy(int controllers) throws Exception {
      setIsolationEnabled(true);
      if (_resources.contains(SIBLING)) {
        if (_controllers.isEmpty()) {
          startController();
        }
        dropResource(SIBLING);
      }
      for (int clique = 0; clique < CLIQUE_COUNT; clique++) {
        setPartitionWeight(resourceName(clique), HEALTHY_WEIGHT);
        List<String> nodes = _nodesByClique.get(clique);
        while (nodes.size() > NODES_PER_CLIQUE) {
          String node = nodes.remove(nodes.size() - 1);
          MockParticipantManager participant = _participants.remove(node);
          if (participant.isConnected()) {
            participant.syncStop();
          }
          _admin.dropInstance(_clusterName, _configAccessor.getInstanceConfig(_clusterName, node));
        }
        for (String node : nodes) {
          InstanceConfig instanceConfig = _configAccessor.getInstanceConfig(_clusterName, node);
          if (instanceConfig.getInstanceOperation().getOperation()
              != InstanceConstants.InstanceOperation.ENABLE) {
            setInstanceOperation(node, InstanceConstants.InstanceOperation.ENABLE);
          }
          setInstanceCapacity(node, NODE_CAPACITY);
          if (!_participants.get(node).isConnected()) {
            startParticipant(node);
          }
        }
      }
      while (_controllers.size() > controllers) {
        stop(_controllers.get(_controllers.size() - 1));
      }
      while (_controllers.size() < controllers) {
        startController();
      }
      ClusterControllerManager leader = awaitLeader();
      awaitStrictConvergence(_clusterName + " must be healthy before the scenario starts");
      await("the isolation gauge must be 0 on a healthy cluster",
          () -> rebalancer(leader) != null && isolationGauge(leader) == 0);
    }

    @Override
    public void close() {
      stopAllControllers();
      _participants.values().stream().filter(MockParticipantManager::isConnected)
          .forEach(MockParticipantManager::syncStop);
      _store.close();
      deleteCluster(_clusterName);
    }
  }
}
