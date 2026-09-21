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

import java.lang.management.ManagementFactory;
import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import javax.management.ObjectName;

import org.apache.helix.ConfigAccessor;
import org.apache.helix.HelixDataAccessor;
import org.apache.helix.PropertyKey;
import org.apache.helix.TestHelper;
import org.apache.helix.common.ZkTestBase;
import org.apache.helix.constants.InstanceConstants;
import org.apache.helix.controller.GenericHelixController;
import org.apache.helix.controller.rebalancer.util.DelayedRebalanceUtil;
import org.apache.helix.controller.rebalancer.util.RebalanceScheduler;
import org.apache.helix.controller.rebalancer.waged.AssignmentMetadataStore;
import org.apache.helix.controller.rebalancer.waged.model.ClusterModel;
import org.apache.helix.integration.manager.ClusterControllerManager;
import org.apache.helix.integration.manager.MockParticipantManager;
import org.apache.helix.manager.zk.ZKHelixDataAccessor;
import org.apache.helix.manager.zk.ZkBucketDataAccessor;
import org.apache.helix.model.BuiltInStateModelDefinitions;
import org.apache.helix.model.ClusterConfig;
import org.apache.helix.model.ExternalView;
import org.apache.helix.model.IdealState;
import org.apache.helix.model.InstanceConfig;
import org.apache.helix.model.ParticipantHistory;
import org.apache.helix.model.ResourceAssignment;
import org.apache.helix.model.ResourceConfig;
import org.apache.helix.monitoring.mbeans.ClusterStatusMonitor;
import org.apache.helix.tools.ClusterVerifiers.StrictMatchExternalViewVerifier;
import org.apache.helix.tools.ClusterVerifiers.ZkHelixClusterVerifier;
import org.apache.helix.util.RebalanceUtil;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

/**
 * Instance tag isolation through the WAGED delayed rebalance window, against a real ZooKeeper, real
 * Helix controllers, real participants and the real assignment metadata store.
 *
 * Four cliques of four nodes, one LeaderStandby resource per clique with three replicas and a
 * minimum of three active replicas, DISK capacity only. Requiring every replica to stay active is
 * what makes a single participant loss need a min-active top-up inside the window. With four nodes
 * and three replicas every partition has exactly one clique node that does not hold it, so the
 * top-up and the permanent move at the window end each have a single legal target, which the tests
 * assert exactly. Clique 0 is the one that gets broken.
 *
 * Inside the window the lost node is still active: the emergency scope has nothing to move and the
 * persisted best possible assignment keeps naming it, while the DELAYED_REBALANCE_OVERWRITES scope
 * tops the partitions up on the live nodes without persisting anything. The window end is taken
 * from the offline time the participant history records in ZooKeeper, which is also what a new
 * Helix controller derives it from.
 */
public class TestWagedRebalanceInstanceTagIsolationDelayed extends ZkTestBase {
  private static final int CLIQUE_COUNT = 4;
  private static final int NODES_PER_CLIQUE = 4;
  private static final int START_PORT = 14918;
  private static final int PARTITIONS = 3;
  private static final int REPLICA = 3;
  private static final int MIN_ACTIVE_REPLICA = 3;
  private static final String CAPACITY_KEY = "DISK";
  private static final int NODE_CAPACITY = 1000;
  private static final int HEALTHY_WEIGHT = 10;
  // Above any node's capacity, while clique 0's whole demand (9 x 1001) stays below the capacity
  // the cluster keeps even with a few nodes gone, so only the tag-local NO_CANDIDATE_NODE path
  // fires and never the tag-blind cluster-wide capacity precheck.
  private static final int TAG_LOCAL_BREAK = NODE_CAPACITY + 1;
  // Clique 0 alone then demands more than the whole cluster holds (9 x 4000 against 16 x 1000), so
  // the cluster-wide capacity precheck fails in every scope.
  private static final int CLUSTER_WIDE_BREAK = 4 * NODE_CAPACITY;
  private static final long DELAY_MS = 20_000L;
  private static final long TIMEOUT = 90_000L;
  private static final int REALIGN_ATTEMPTS = 3;
  private static final int BROKEN = 0;
  private static final String BASELINE_SCOPE =
      ClusterModel.RebalanceScopeType.GLOBAL_BASELINE.name();
  private static final String PARTIAL_SCOPE = ClusterModel.RebalanceScopeType.PARTIAL.name();
  private static final String EMERGENCY_SCOPE = ClusterModel.RebalanceScopeType.EMERGENCY.name();
  private static final String OVERWRITE_SCOPE =
      ClusterModel.RebalanceScopeType.DELAYED_REBALANCE_OVERWRITES.name();

  private final String CLASS_NAME = getShortClassName();
  private final String CLUSTER_NAME = CLUSTER_PREFIX + "_" + CLASS_NAME;
  private final Map<Integer, List<String>> _nodesByClique = new TreeMap<>();
  private final Map<String, MockParticipantManager> _participants = new HashMap<>();
  private final Map<Integer, Integer> _weights = new HashMap<>();
  private final Set<String> _shrunkNodes = new HashSet<>();
  private final Set<String> _nonEnabledNodes = new HashSet<>();
  private final AtomicInteger _controllerCounter = new AtomicInteger();
  private ClusterControllerManager _leader;
  private ConfigAccessor _configAccessor;
  private HelixDataAccessor _accessor;
  private AssignmentMetadataStore _store;

  private static String cliqueTag(int clique) {
    return "delay_clique_" + clique;
  }

  // Resource names are unique to this class because the delayed rebalance timer is keyed by the
  // resource name alone, JVM wide.
  private static String resourceName(int clique) {
    return "DelayDB_clique_" + clique;
  }

  @BeforeClass
  public void beforeClass() throws Exception {
    System.out.println("START " + CLASS_NAME + " at " + new Date(System.currentTimeMillis()));
    _gSetupTool.addCluster(CLUSTER_NAME, true);
    _configAccessor = new ConfigAccessor(_gZkClient);
    _accessor = new ZKHelixDataAccessor(CLUSTER_NAME, _baseAccessor);

    int port = START_PORT;
    for (int clique = 0; clique < CLIQUE_COUNT; clique++) {
      List<String> cliqueNodes = new ArrayList<>();
      for (int i = 0; i < NODES_PER_CLIQUE; i++) {
        String node = PARTICIPANT_PREFIX + "_" + port++;
        _gSetupTool.addInstanceToCluster(CLUSTER_NAME, node);
        _gSetupTool.getClusterManagementTool()
            .addInstanceTag(CLUSTER_NAME, node, cliqueTag(clique));
        cliqueNodes.add(node);
      }
      _nodesByClique.put(clique, cliqueNodes);
    }

    ClusterConfig clusterConfig = _accessor.getProperty(_accessor.keyBuilder().clusterConfig());
    clusterConfig.setInstanceCapacityKeys(Collections.singletonList(CAPACITY_KEY));
    clusterConfig
        .setDefaultInstanceCapacityMap(Collections.singletonMap(CAPACITY_KEY, NODE_CAPACITY));
    clusterConfig
        .setDefaultPartitionWeightMap(Collections.singletonMap(CAPACITY_KEY, HEALTHY_WEIGHT));
    clusterConfig.setDelayRebalaceEnabled(true);
    clusterConfig.setRebalanceDelayTime(DELAY_MS);
    clusterConfig.setWagedInstanceTagIsolationEnabled(true);
    clusterConfig.setPersistBestPossibleAssignment(true);
    _accessor.setProperty(_accessor.keyBuilder().clusterConfig(), clusterConfig);

    for (String node : allNodes()) {
      MockParticipantManager participant = new MockParticipantManager(ZK_ADDR, CLUSTER_NAME, node);
      participant.syncStart();
      _participants.put(node, participant);
    }
    _leader = startController();

    // Read through to ZooKeeper on every access, returning defensive copies, so the assertions see
    // what the Helix controller persisted rather than a cached or cleared map.
    _store = new AssignmentMetadataStore(new ZkBucketDataAccessor(ZK_ADDR), CLUSTER_NAME) {
      @Override
      public Map<String, ResourceAssignment> getBaseline() {
        super.reset();
        return new HashMap<>(super.getBaseline());
      }

      @Override
      public synchronized Map<String, ResourceAssignment> getBestPossibleAssignment() {
        super.reset();
        return new HashMap<>(super.getBestPossibleAssignment());
      }
    };

    for (int clique = 0; clique < CLIQUE_COUNT; clique++) {
      createResourceWithWagedRebalance(CLUSTER_NAME, resourceName(clique),
          BuiltInStateModelDefinitions.LeaderStandby.name(), PARTITIONS, REPLICA,
          MIN_ACTIVE_REPLICA);
      IdealState idealState = _gSetupTool.getClusterManagementTool()
          .getResourceIdealState(CLUSTER_NAME, resourceName(clique));
      idealState.setInstanceGroupTag(cliqueTag(clique));
      _gSetupTool.getClusterManagementTool()
          .setResourceIdealState(CLUSTER_NAME, resourceName(clique), idealState);
      _gSetupTool.rebalanceStorageCluster(CLUSTER_NAME, resourceName(clique), REPLICA);
      _weights.put(clique, HEALTHY_WEIGHT);
    }
    awaitConvergence("initial setup");
  }

  @AfterClass(alwaysRun = true)
  public void afterClass() {
    if (_store != null) {
      _store.close();
    }
    if (_leader != null && _leader.isConnected()) {
      _leader.syncStop();
    }
    _participants.values().stream().filter(MockParticipantManager::isConnected)
        .forEach(MockParticipantManager::syncStop);
    deleteCluster(CLUSTER_NAME);
    System.out.println("END " + CLASS_NAME + " at " + new Date(System.currentTimeMillis()));
  }

  /**
   * Every scenario starts from the same place: every participant up and enabled, capacities and
   * weights healthy, isolation on, and the store settled on the baseline. A restore that changes a
   * field the change detector keeps starts a baseline, so the reset asserts that one started
   * before it waits for the baseline thread to go idle.
   */
  @BeforeMethod
  public void resetCluster() throws Exception {
    for (String node : allNodes()) {
      restartParticipant(node);
    }
    long baselines = Math.max(0L, rebalancerMetricOrMinusOne("GlobalBaselineCalcCounter"));
    boolean startsBaseline = false;
    for (String node : new ArrayList<>(_nonEnabledNodes)) {
      // Only an operation that takes the node out of the assignable set is visible to the change
      // detector, so only its restore starts a baseline.
      startsBaseline |= !_configAccessor.getInstanceConfig(CLUSTER_NAME, node).isAssignable();
      setInstanceOperation(node, InstanceConstants.InstanceOperation.ENABLE);
    }
    for (String node : new ArrayList<>(_shrunkNodes)) {
      setNodeCapacity(node, NODE_CAPACITY);
      startsBaseline = true;
    }
    for (int clique = 0; clique < CLIQUE_COUNT; clique++) {
      if (_weights.get(clique) != HEALTHY_WEIGHT) {
        setPartitionWeight(clique, HEALTHY_WEIGHT);
        startsBaseline = true;
      }
    }
    ClusterConfig config = _configAccessor.getClusterConfig(CLUSTER_NAME);
    if (!config.isWagedInstanceTagIsolationEnabled()) {
      config.setWagedInstanceTagIsolationEnabled(true);
      _configAccessor.setClusterConfig(CLUSTER_NAME, config);
      startsBaseline = true;
    }
    if (startsBaseline) {
      awaitBaselineStarted(baselines, "reset: restoring the cluster must start a baseline");
    }
    settle("reset");
  }

  /**
   * Bring the store to rest on the baseline. Each config write starts an asynchronous baseline, so
   * nothing is compared until every one of them has landed.
   */
  private void settle(String when) throws Exception {
    Assert.assertTrue(TestHelper.verify(() -> _accessor
            .getChildNames(_accessor.keyBuilder().liveInstances()).containsAll(allNodes()),
        TIMEOUT), when + ": every participant must be live");
    awaitBaselineIdle(when);
    realignBestPossibleWithBaseline(when);
    awaitConvergence(when);
  }

  /**
   * A permanent move leaves the best possible assignment away from the baseline, which still names
   * the lost node because liveness never triggers a baseline. When that node returns the partial
   * rebalance prefers to stay where it is, so the two never meet again on their own. Force the
   * convergence, then restore the default preference, so every scenario starts with the best
   * possible assignment equal to the baseline. Each preference write recomputes the baseline from
   * scratch and the result need not match the one before it, so repeat until the two agree.
   */
  private void realignBestPossibleWithBaseline(String when) throws Exception {
    Map<ClusterConfig.GlobalRebalancePreferenceKey, Integer> forceConverge =
        new HashMap<>(ClusterConfig.DEFAULT_GLOBAL_REBALANCE_PREFERENCE);
    forceConverge.put(ClusterConfig.GlobalRebalancePreferenceKey.FORCE_BASELINE_CONVERGE, 1);
    for (int attempt = 1; !bestPossible().equals(baseline()); attempt++) {
      Assert.assertTrue(attempt <= REALIGN_ATTEMPTS,
          when + ": the best possible must realign with the baseline");
      setRebalancePreference(forceConverge, when);
      Assert.assertTrue(TestHelper.verify(() -> {
        Map<String, Map<String, Map<String, String>>> bestPossible = bestPossible();
        return isComplete(bestPossible) && bestPossible.equals(baseline());
      }, TIMEOUT), when + ": forcing the baseline convergence must realign the best possible");
      setRebalancePreference(null, when);
    }
  }

  /**
   * The preference is a cluster config field the change detector keeps, so it starts a baseline.
   */
  private void setRebalancePreference(
      Map<ClusterConfig.GlobalRebalancePreferenceKey, Integer> preference, String when)
      throws Exception {
    long baselines = rebalancerMetric("GlobalBaselineCalcCounter");
    ClusterConfig config = _configAccessor.getClusterConfig(CLUSTER_NAME);
    config.setGlobalRebalancePreference(preference);
    _configAccessor.setClusterConfig(CLUSTER_NAME, config);
    awaitBaselineStarted(baselines, when + ": a preference write must start a baseline");
    awaitBaselineIdle(when);
  }

  /**
   * Wait until more baseline calculations have started than {@code baselines}. It runs after a
   * write known to start a baseline and before the idle wait that follows it, so that wait cannot
   * pass in the gap before the Helix controller's pipeline picks the write up.
   */
  private void awaitBaselineStarted(long baselines, String message) throws Exception {
    Assert.assertTrue(TestHelper.verify(
        () -> rebalancerMetricOrMinusOne("GlobalBaselineCalcCounter") > baselines, TIMEOUT),
        message);
  }

  /**
   * Wait until no baseline calculation is running or queued, then for the external view to
   * converge. A snapshot taken while one is in flight can see the partial rebalance that follows it
   * move replicas the scenario never touched. The baseline thread carries the cluster name only
   * while a calculation runs, and a queued one starts as soon as the thread is free, so the thread
   * has to stay idle with the calculation counter constant across a processing wait.
   */
  private void awaitBaselineIdle(String when) throws Exception {
    Assert.assertTrue(TestHelper.verify(() -> {
      // A new Helix controller registers the rebalancer metrics in its first pipeline, and that
      // pipeline starts a baseline, so a missing MBean or a counter at zero counts as busy.
      long started = rebalancerMetricOrMinusOne("GlobalBaselineCalcCounter");
      if (started <= 0L || baselineRunning()) {
        return false;
      }
      Thread.sleep(TestHelper.DEFAULT_REBALANCE_PROCESSING_WAIT_TIME);
      return !baselineRunning()
          && rebalancerMetricOrMinusOne("GlobalBaselineCalcCounter") == started;
    }, TIMEOUT), when + ": every baseline calculation must land");
    Assert.assertTrue(verifier().verifyByPolling(TIMEOUT, 200),
        when + ": the external view must converge");
  }

  /**
   * Wait until the baseline a write started has landed and the partial rebalance that follows it
   * has run, then for the external view to converge. A changed baseline schedules a pipeline whose
   * partial rebalance can move the best possible assignment toward it, and the pipeline after that
   * persists the result, so neither calculation may run and neither counter may move across a
   * processing wait.
   */
  private void awaitRebalanceIdle(String when) throws Exception {
    Assert.assertTrue(TestHelper.verify(() -> {
      long baselines = rebalancerMetricOrMinusOne("GlobalBaselineCalcCounter");
      long partials = rebalancerMetricOrMinusOne("PartialRebalanceCounter");
      if (baselines <= 0L || baselineRunning() || partialRunning()) {
        return false;
      }
      Thread.sleep(TestHelper.DEFAULT_REBALANCE_PROCESSING_WAIT_TIME);
      return !baselineRunning() && !partialRunning()
          && rebalancerMetricOrMinusOne("GlobalBaselineCalcCounter") == baselines
          && rebalancerMetricOrMinusOne("PartialRebalanceCounter") == partials;
    }, TIMEOUT), when + ": the baseline and the partial rebalance after it must land");
    // The idle wait above already spans a processing wait, so the verifier polls at once.
    Assert.assertTrue(verifier(0).verifyByPolling(TIMEOUT, 200),
        when + ": the external view must converge");
  }

  private boolean baselineRunning() {
    return threadRunning("WagedGlobalRebalance-" + CLUSTER_NAME);
  }

  private boolean partialRunning() {
    return threadRunning("WagedPartialRebalance-" + CLUSTER_NAME);
  }

  private static boolean threadRunning(String name) {
    return Thread.getAllStackTraces().keySet().stream().anyMatch(t -> name.equals(t.getName()));
  }

  /**
   * A healthy clique loses a participant while clique 0 is broken. Inside the window the
   * min-active top-up happens for the healthy clique only, and nothing permanent moves: the ideal
   * state carries the top-up, the persisted best possible and baseline assignments do not. At the
   * window end the healthy clique's replicas leave the lost node permanently, onto the same nodes
   * the top-up used, while the broken clique stays carried forward.
   */
  @Test
  public void testTopUpStaysInItsCliqueAndBecomesPermanentOnlyAtTheWindowEnd() throws Exception {
    breakClique(TAG_LOCAL_BREAK);
    Snapshot before = new Snapshot();
    int healthy = 1;
    String db = resourceName(healthy);
    String lost = busiestNode(healthy, before._bestPossible);
    Map<String, Set<String>> topUp = withTopUp(healthy, before._bestPossible, lost);
    long overwrites = rebalancerMetric("RebalanceOverwriteCounter");
    long baselines = rebalancerMetric("GlobalBaselineCalcCounter");

    stopParticipant(lost);
    long windowEnd = windowEnd(lost);
    Assert.assertTrue(TestHelper.verify(() -> activeReplicas(db).equals(topUp), TIMEOUT),
        "The healthy clique must be topped up on its own spare nodes: " + activeReplicas(db));

    // Inside the window the top-up exists only in the ideal state of the healthy clique.
    Snapshot inWindow = new Snapshot();
    assertInWindow(windowEnd, "the in-window observations");
    Assert.assertEquals(inWindow._bestPossible, before._bestPossible,
        "Nothing may be persisted as best possible while the lost node is inside its window");
    Assert.assertEquals(inWindow._baseline, before._baseline,
        "The loss of a live instance must not recompute the baseline");
    assertPreferenceListsCarryTopUp(inWindow, before, healthy, lost);
    Assert.assertEquals(stateMapInstances(inWindow._idealStateMaps.get(db)), topUp,
        "The ideal state's best possible state map must hold the topped up placement");
    assertOtherCliquesUntouched(inWindow, before, healthy);
    Assert.assertTrue(rebalancerMetric("RebalanceOverwriteCounter") > overwrites,
        "The top-up must come from the delayed rebalance overwrite scope");
    Assert.assertEquals(rebalancerMetric("GlobalBaselineCalcCounter"), baselines,
        "No baseline may run for a live instance change");
    assertSkippedByScope(Collections.singleton(resourceName(BROKEN)), Collections.emptySet(),
        Collections.emptySet(), Collections.emptySet());
    Assert.assertEquals(isolationGauge(), 1L);
    assertInWindow(windowEnd, "the in-window observations");

    // At the window end the replicas move off the lost node for good.
    long movedAt = awaitBestPossibleDropsNode(healthy, lost, windowEnd);
    Assert.assertTrue(movedAt >= windowEnd,
        "The permanent move happened " + (windowEnd - movedAt) + " ms before the window end");
    Assert.assertTrue(TestHelper.verify(() -> {
      Snapshot after = new Snapshot();
      return instancesOf(after._bestPossible.get(db)).equals(topUp)
          && preferenceListInstances(after._preferenceLists.get(db)).equals(topUp)
          && activeReplicas(db).equals(topUp);
    }, TIMEOUT), "The permanent move must land on the nodes the top-up used");
    Snapshot after = new Snapshot();
    assertComplete(after);
    for (int clique = 0; clique < CLIQUE_COUNT; clique++) {
      if (clique != healthy) {
        Assert.assertEquals(after._bestPossible.get(resourceName(clique)),
            before._bestPossible.get(resourceName(clique)),
            "Clique " + clique + " must not move when another clique's window expires");
      }
    }
    Assert.assertEquals(after._baseline, before._baseline,
        "The window end is not a baseline trigger, so the baseline still names the lost node");
    Assert.assertTrue(TestHelper.verify(() -> skippedByScope().get(EMERGENCY_SCOPE).isEmpty()
        && skippedByScope().get(OVERWRITE_SCOPE).isEmpty(), TIMEOUT));
    assertSkippedByScope(Collections.singleton(resourceName(BROKEN)), Collections.emptySet(),
        Collections.emptySet(), Collections.emptySet());
    Assert.assertEquals(isolationGauge(), 1L);
    Assert.assertTrue(verifier().verifyByPolling(TIMEOUT, 200));
  }

  /**
   * The lost node returns inside the window: the overwrite disappears, the healthy clique goes
   * back to exactly its original placement, and nothing moves when the original window end passes.
   */
  @Test
  public void testNodeReturningInsideTheWindowRestoresTheOriginalPlacement() throws Exception {
    runWithReturningNode(this::nodeReturnsInsideTheWindow);
  }

  private void nodeReturnsInsideTheWindow() throws Exception {
    breakClique(TAG_LOCAL_BREAK);
    Snapshot before = new Snapshot();
    int healthy = 2;
    String db = resourceName(healthy);
    String lost = busiestNode(healthy, before._bestPossible);
    Map<String, Set<String>> topUp = withTopUp(healthy, before._bestPossible, lost);

    stopParticipant(lost);
    long windowEnd = windowEnd(lost);
    Assert.assertTrue(TestHelper.verify(() -> activeReplicas(db).equals(topUp), TIMEOUT));
    assertInWindow(windowEnd, "the top-up");

    restartInsideTheWindow(lost, windowEnd);
    assertInWindow(windowEnd, "the node return");
    Assert.assertTrue(TestHelper.verify(() -> {
      Snapshot now = new Snapshot();
      return now._externalViews.equals(before._externalViews)
          && now._preferenceLists.equals(before._preferenceLists);
    }, TIMEOUT), "The overwrite must disappear once the node is back");
    long overwrites = rebalancerMetric("RebalanceOverwriteCounter");
    assertInWindow(windowEnd, "the restored placement");

    // Let the original window end pass and prove it changes nothing.
    awaitTime(windowEnd + 2000L);
    Snapshot after = new Snapshot();
    Assert.assertEquals(after._bestPossible, before._bestPossible);
    Assert.assertEquals(after._baseline, before._baseline);
    Assert.assertEquals(after._externalViews, before._externalViews);
    Assert.assertEquals(after._preferenceLists, before._preferenceLists);
    Assert.assertEquals(rebalancerMetric("RebalanceOverwriteCounter"), overwrites,
        "No overwrite may run once every node is back");
    assertSkippedByScope(Collections.singleton(resourceName(BROKEN)), Collections.emptySet(),
        Collections.emptySet(), Collections.emptySet());
    Assert.assertEquals(isolationGauge(), 1L);
  }

  /**
   * The lost node belongs to the broken clique. Its top-up cannot be placed, so the overwrite
   * scope skips clique 0 and its pre-overwrite assignment stands, without any fallback, while a
   * healthy clique that loses a node in the same window is still topped up. Repairing clique 0
   * inside the window brings its top-up, and the window end moves both cliques off their lost
   * nodes.
   */
  @Test
  public void testLostNodeInTheBrokenCliqueHasItsTopUpOmittedUntilRepaired() throws Exception {
    breakClique(TAG_LOCAL_BREAK);
    Snapshot before = new Snapshot();
    String brokenDb = resourceName(BROKEN);
    String brokenLost = busiestNode(BROKEN, before._bestPossible);
    Map<String, Set<String>> brokenWithoutTopUp =
        withoutTopUp(before._bestPossible.get(brokenDb), brokenLost);
    long overwrites = rebalancerMetric("RebalanceOverwriteCounter");

    stopParticipant(brokenLost);
    long brokenWindowEnd = windowEnd(brokenLost);
    Assert.assertTrue(TestHelper.verify(
        () -> rebalancerMetric("RebalanceOverwriteCounter") > overwrites
            && skippedByScope().get(OVERWRITE_SCOPE).equals(Collections.singleton(brokenDb))
            && activeReplicas(brokenDb).equals(brokenWithoutTopUp), TIMEOUT),
        "The overwrite scope must skip the broken clique: " + skippedByScope());
    Snapshot omitted = new Snapshot();
    assertInWindow(brokenWindowEnd, "the omitted top-up");
    Assert.assertEquals(omitted._preferenceLists.get(brokenDb),
        before._preferenceLists.get(brokenDb),
        "The broken clique's pre-overwrite assignment must stand, with no top-up entry");
    Assert.assertEquals(omitted._bestPossible, before._bestPossible);
    Assert.assertEquals(omitted._baseline, before._baseline);
    assertOtherCliquesUntouched(omitted, before, BROKEN);
    Assert.assertEquals(clusterStatusGauge("WagedRebalanceOverwriteFailingGauge"), 0L,
        "Isolating the broken clique must keep the overwrite phase clean");
    Assert.assertEquals(clusterStatusGauge("WagedFallbackInUseGauge"), 0L,
        "Isolating the broken clique must not fall back to the last known good assignment");
    Assert.assertEquals(isolationGauge(), 1L);

    int healthy = 3;
    String healthyDb = resourceName(healthy);
    String healthyLost = busiestNode(healthy, before._bestPossible);
    Map<String, Set<String>> healthyTopUp = withTopUp(healthy, before._bestPossible, healthyLost);
    stopParticipant(healthyLost);
    long healthyWindowEnd = windowEnd(healthyLost);
    Assert.assertTrue(TestHelper.verify(() -> activeReplicas(healthyDb).equals(healthyTopUp)
            && activeReplicas(brokenDb).equals(brokenWithoutTopUp), TIMEOUT),
        "A healthy clique must be topped up while the broken clique's top-up stays omitted");
    Assert.assertEquals(skippedByScope().get(OVERWRITE_SCOPE), Collections.singleton(brokenDb));
    assertInWindow(brokenWindowEnd, "the healthy top-up next to an omitted one");

    // Repair clique 0 inside its node's window, which gives the clique its own top-up. The weight
    // is a resource config field the change detector keeps, so the repair starts a baseline, and
    // the partial rebalance after it can move the repaired clique before its placement is read.
    TestHelper.Verifier repairedTopUp = () -> {
      Map<String, Map<String, Map<String, String>>> bestPossible = bestPossible();
      return namesInstance(bestPossible.get(brokenDb), brokenLost)
          && activeReplicas(brokenDb).equals(withTopUp(BROKEN, bestPossible, brokenLost))
          && activeReplicas(healthyDb).equals(healthyTopUp) && isolationGauge() == 0L;
    };
    long repairBaselines = rebalancerMetric("GlobalBaselineCalcCounter");
    setPartitionWeight(BROKEN, HEALTHY_WEIGHT);
    Assert.assertTrue(TestHelper.verify(repairedTopUp, TIMEOUT),
        "The repaired clique must get its own top-up inside the window");
    awaitBaselineStarted(repairBaselines, "The repair must start a baseline");
    awaitRebalanceIdle("the repair");
    Assert.assertTrue(TestHelper.verify(repairedTopUp, TIMEOUT),
        "The repaired placement must land with its top-up inside the window");
    assertInWindow(brokenWindowEnd, "the repaired top-up");
    Assert.assertEquals(skippedByScope().values().stream().mapToInt(Set::size).sum(), 0,
        "Every scope must stop reporting once the clique is repaired: " + skippedByScope());

    // The window ends: both cliques leave their lost nodes permanently.
    Map<String, Map<String, String>> brokenBeforeExpiry = bestPossible().get(brokenDb);
    long brokenMovedAt = awaitBestPossibleDropsNode(BROKEN, brokenLost, brokenWindowEnd);
    Assert.assertTrue(brokenMovedAt >= brokenWindowEnd);
    long healthyMovedAt = awaitBestPossibleDropsNode(healthy, healthyLost, healthyWindowEnd);
    Assert.assertTrue(healthyMovedAt >= healthyWindowEnd);
    Assert.assertTrue(TestHelper.verify(() -> {
      Map<String, Map<String, Map<String, String>>> bestPossible = bestPossible();
      return instancesOf(bestPossible.get(brokenDb))
          .equals(withTopUp(BROKEN, Collections.singletonMap(brokenDb, brokenBeforeExpiry),
              brokenLost))
          && instancesOf(bestPossible.get(healthyDb)).equals(healthyTopUp);
    }, TIMEOUT), "Both cliques must move onto the nodes their top-ups used");
    Snapshot after = new Snapshot();
    assertComplete(after);
    Assert.assertEquals(after._bestPossible.get(resourceName(1)),
        before._bestPossible.get(resourceName(1)));
    Assert.assertEquals(after._bestPossible.get(resourceName(2)),
        before._bestPossible.get(resourceName(2)));
    Assert.assertEquals(isolationGauge(), 0L);
  }

  /**
   * The broken clique's window expires without a repair. The emergency scope cannot
   * move its replicas either, so clique 0 stays carried forward, still naming the lost node, while
   * a healthy clique whose window ends at the same time moves permanently. Repairing afterwards
   * lets clique 0 leave the lost node too.
   */
  @Test
  public void testBrokenCliqueStaysCarriedWhenItsWindowExpiresUnrepaired() throws Exception {
    breakClique(TAG_LOCAL_BREAK);
    Snapshot before = new Snapshot();
    String brokenDb = resourceName(BROKEN);
    String brokenLost = busiestNode(BROKEN, before._bestPossible);
    int healthy = 2;
    String healthyDb = resourceName(healthy);
    String healthyLost = busiestNode(healthy, before._bestPossible);
    Map<String, Set<String>> healthyTopUp = withTopUp(healthy, before._bestPossible, healthyLost);

    stopParticipant(brokenLost);
    stopParticipant(healthyLost);
    long brokenWindowEnd = windowEnd(brokenLost);
    long healthyWindowEnd = windowEnd(healthyLost);
    Assert.assertTrue(TestHelper.verify(() -> activeReplicas(healthyDb).equals(healthyTopUp),
        TIMEOUT));
    assertInWindow(Math.min(brokenWindowEnd, healthyWindowEnd), "the top-up");

    long healthyMovedAt = awaitBestPossibleDropsNode(healthy, healthyLost, healthyWindowEnd);
    Assert.assertTrue(healthyMovedAt >= healthyWindowEnd);
    awaitTime(brokenWindowEnd);
    // Past its window the lost node is inactive: the emergency scope and the partial rebalance both
    // try to place the broken clique's replicas from it and both skip it, while the overwrite scope
    // has nothing left to top up.
    Map<String, Set<String>> expectedScopes = new TreeMap<>();
    expectedScopes.put(BASELINE_SCOPE, Collections.singleton(brokenDb));
    expectedScopes.put(PARTIAL_SCOPE, Collections.singleton(brokenDb));
    expectedScopes.put(EMERGENCY_SCOPE, Collections.singleton(brokenDb));
    expectedScopes.put(OVERWRITE_SCOPE, Collections.emptySet());
    Assert.assertTrue(TestHelper.verify(() -> skippedByScope().equals(expectedScopes)
            && instancesOf(bestPossible().get(healthyDb)).equals(healthyTopUp), TIMEOUT),
        "After the window every placing scope must skip the broken clique: " + skippedByScope());
    logScopes("broken window expired unrepaired");
    Snapshot expired = new Snapshot();
    assertComplete(expired);
    Assert.assertEquals(expired._bestPossible.get(brokenDb), before._bestPossible.get(brokenDb),
        "The broken clique must be carried forward whole, still naming its lost node");
    Assert.assertEquals(activeReplicas(brokenDb),
        withoutTopUp(before._bestPossible.get(brokenDb), brokenLost));
    for (int clique : new int[] {1, 3}) {
      Assert.assertEquals(expired._bestPossible.get(resourceName(clique)),
          before._bestPossible.get(resourceName(clique)));
      Assert.assertEquals(expired._externalViews.get(resourceName(clique)),
          before._externalViews.get(resourceName(clique)));
    }
    Assert.assertEquals(expired._baseline, before._baseline);
    Assert.assertEquals(clusterStatusGauge("WagedFallbackInUseGauge"), 0L);
    Assert.assertEquals(isolationGauge(), 1L);

    // Repair after the window: clique 0 leaves its lost node for good. The repair starts a
    // baseline, and the partial rebalance after it settles where the repaired clique lands.
    long repairBaselines = rebalancerMetric("GlobalBaselineCalcCounter");
    setPartitionWeight(BROKEN, HEALTHY_WEIGHT);
    Assert.assertTrue(TestHelper.verify(() -> {
      Map<String, Map<String, Map<String, String>>> bestPossible = bestPossible();
      return !namesInstance(bestPossible.get(brokenDb), brokenLost)
          && isComplete(bestPossible) && isolationGauge() == 0L;
    }, TIMEOUT), "The repaired clique must move off its expired node: " + skippedByScope());
    awaitBaselineStarted(repairBaselines, "The repair must start a baseline");
    awaitRebalanceIdle("the repair");
    Assert.assertTrue(TestHelper.verify(
        () -> activeReplicas(brokenDb).equals(instancesOf(bestPossible().get(brokenDb))), TIMEOUT),
        "The repaired placement must land in the external view");
    Assert.assertEquals(instancesOf(bestPossible().get(brokenDb)),
        withTopUp(BROKEN, before._bestPossible, brokenLost));
    Assert.assertTrue(verifier().verifyByPolling(TIMEOUT, 200));
  }

  /**
   * Two healthy cliques each lose a node in the same window while clique 0 is broken. Both are
   * topped up independently, and each moves permanently at its own window end.
   */
  @Test
  public void testTwoHealthyCliquesLosingANodeInTheSameWindow() throws Exception {
    breakClique(TAG_LOCAL_BREAK);
    Snapshot before = new Snapshot();
    String firstLost = busiestNode(1, before._bestPossible);
    String secondLost = busiestNode(2, before._bestPossible);
    Map<String, Set<String>> firstTopUp = withTopUp(1, before._bestPossible, firstLost);
    Map<String, Set<String>> secondTopUp = withTopUp(2, before._bestPossible, secondLost);

    stopParticipant(firstLost);
    long firstWindowEnd = windowEnd(firstLost);
    Assert.assertTrue(TestHelper.verify(
        () -> activeReplicas(resourceName(1)).equals(firstTopUp), TIMEOUT));
    // Stagger the second loss so the two windows end at clearly different times.
    awaitTime(firstWindowEnd - DELAY_MS + 5000L);
    stopParticipant(secondLost);
    long secondWindowEnd = windowEnd(secondLost);
    Assert.assertTrue(TestHelper.verify(
        () -> activeReplicas(resourceName(1)).equals(firstTopUp)
            && activeReplicas(resourceName(2)).equals(secondTopUp), TIMEOUT));
    Snapshot inWindow = new Snapshot();
    assertInWindow(firstWindowEnd, "both top-ups");
    Assert.assertEquals(inWindow._bestPossible, before._bestPossible);
    Assert.assertEquals(inWindow._baseline, before._baseline);
    assertPreferenceListsCarryTopUp(inWindow, before, 1, firstLost);
    assertPreferenceListsCarryTopUp(inWindow, before, 2, secondLost);
    for (int clique : new int[] {BROKEN, 3}) {
      Assert.assertEquals(inWindow._preferenceLists.get(resourceName(clique)),
          before._preferenceLists.get(resourceName(clique)));
      Assert.assertEquals(inWindow._externalViews.get(resourceName(clique)),
          before._externalViews.get(resourceName(clique)));
    }
    assertSkippedByScope(Collections.singleton(resourceName(BROKEN)), Collections.emptySet(),
        Collections.emptySet(), Collections.emptySet());
    assertInWindow(firstWindowEnd, "both top-ups");

    long firstMovedAt = awaitBestPossibleDropsNode(1, firstLost, firstWindowEnd);
    Assert.assertTrue(firstMovedAt >= firstWindowEnd);
    Map<String, Map<String, Map<String, String>>> between = bestPossible();
    boolean secondStillInWindow = System.currentTimeMillis() < secondWindowEnd;
    if (secondStillInWindow) {
      Assert.assertEquals(between.get(resourceName(2)), before._bestPossible.get(resourceName(2)),
          "The second clique must wait for its own window end");
      Assert.assertEquals(activeReplicas(resourceName(2)), secondTopUp);
    }
    long secondMovedAt = awaitBestPossibleDropsNode(2, secondLost, secondWindowEnd);
    Assert.assertTrue(secondMovedAt >= secondWindowEnd);
    Assert.assertTrue(TestHelper.verify(() -> {
      Map<String, Map<String, Map<String, String>>> bestPossible = bestPossible();
      return instancesOf(bestPossible.get(resourceName(1))).equals(firstTopUp)
          && instancesOf(bestPossible.get(resourceName(2))).equals(secondTopUp);
    }, TIMEOUT));
    Snapshot after = new Snapshot();
    assertComplete(after);
    Assert.assertEquals(after._bestPossible.get(resourceName(BROKEN)),
        before._bestPossible.get(resourceName(BROKEN)));
    Assert.assertEquals(after._bestPossible.get(resourceName(3)),
        before._bestPossible.get(resourceName(3)));
    Assert.assertEquals(after._baseline, before._baseline);
    Assert.assertEquals(isolationGauge(), 1L);
    Assert.assertTrue(secondStillInWindow,
        "The second window ended before the first one was observed, so the staggered case was "
            + "not exercised");
  }

  /**
   * While a healthy clique has a node inside its window, a node of another healthy clique is
   * evacuated and a node of a third has its DISK capacity shrunk. Both are instance config changes,
   * so both run a baseline while the lost node is still active and clique 0 is still broken. The
   * evacuated node is not delayed and is vacated at once, the shrunk node sheds replicas, and the
   * windowed clique keeps its top-up without any permanent move.
   */
  @Test
  public void testEvacuateAndCapacityShrinkDuringAnActiveWindow() throws Exception {
    runWithReturningNode(this::evacuateAndShrinkDuringAnActiveWindow);
  }

  private void evacuateAndShrinkDuringAnActiveWindow() throws Exception {
    breakClique(TAG_LOCAL_BREAK);
    Snapshot before = new Snapshot();
    int windowed = 1;
    String windowedDb = resourceName(windowed);
    String lost = busiestNode(windowed, before._bestPossible);
    Map<String, Set<String>> topUp = withTopUp(windowed, before._bestPossible, lost);
    stopParticipant(lost);
    long windowEnd = windowEnd(lost);
    Assert.assertTrue(TestHelper.verify(() -> activeReplicas(windowedDb).equals(topUp), TIMEOUT));

    int evacuatedClique = 2;
    String evacuatedDb = resourceName(evacuatedClique);
    String evacuated = busiestNode(evacuatedClique, before._bestPossible);
    Map<String, Set<String>> vacated = withTopUp(evacuatedClique, before._bestPossible, evacuated);
    long baselines = rebalancerMetric("GlobalBaselineCalcCounter");
    setInstanceOperation(evacuated, InstanceConstants.InstanceOperation.EVACUATE);
    Assert.assertTrue(TestHelper.verify(() -> {
      Snapshot now = new Snapshot();
      return instancesOf(now._bestPossible.get(evacuatedDb)).equals(vacated)
          && instancesOf(now._baseline.get(evacuatedDb)).equals(vacated)
          && activeReplicas(evacuatedDb).equals(vacated)
          && rebalancerMetric("GlobalBaselineCalcCounter") > baselines;
    }, TIMEOUT), "The evacuated node must be vacated at once, delay window or not");
    // The evacuation is an instance config change, so its baseline reassigns every replica, and the
    // partial rebalance after it can move any clique that baseline changed. The placements are
    // read once both have landed.
    awaitRebalanceIdle("the evacuation");
    Snapshot evacuatedState = new Snapshot();
    assertInWindow(windowEnd, "the evacuation");
    assertWindowedCliqueHeld(evacuatedState, before, windowed, lost, topUp);

    int shrunkClique = 3;
    String shrunkDb = resourceName(shrunkClique);
    String shrunk = busiestNode(shrunkClique, before._bestPossible);
    long shrinkBaselines = rebalancerMetric("GlobalBaselineCalcCounter");
    // Room for a single healthy replica.
    setNodeCapacity(shrunk, HEALTHY_WEIGHT + HEALTHY_WEIGHT / 2);
    Assert.assertTrue(TestHelper.verify(() -> {
      Snapshot now = new Snapshot();
      return replicaCountOn(now._bestPossible.get(shrunkDb), shrunk) == 1
          && replicaCountOn(now._baseline.get(shrunkDb), shrunk) == 1
          && activeReplicas(shrunkDb).equals(instancesOf(now._bestPossible.get(shrunkDb)))
          && rebalancerMetric("GlobalBaselineCalcCounter") > shrinkBaselines;
    }, TIMEOUT), "The shrunk node must shed all but one replica");
    awaitRebalanceIdle("the capacity shrink");
    Snapshot shrunkState = new Snapshot();
    assertInWindow(windowEnd, "the capacity shrink");
    assertWindowedCliqueHeld(shrunkState, before, windowed, lost, topUp);
    Assert.assertEquals(shrunkState._bestPossible.get(evacuatedDb),
        evacuatedState._bestPossible.get(evacuatedDb));
    assertComplete(shrunkState);
    assertSkippedByScope(Collections.singleton(resourceName(BROKEN)), Collections.emptySet(),
        Collections.emptySet(), Collections.emptySet());
    Assert.assertEquals(isolationGauge(), 1L);

    // The node returns inside the window: the overwrite disappears with no permanent move.
    restartInsideTheWindow(lost, windowEnd);
    assertInWindow(windowEnd, "the node return");
    Assert.assertTrue(TestHelper.verify(() -> {
      Snapshot now = new Snapshot();
      return now._externalViews.get(windowedDb).equals(before._externalViews.get(windowedDb))
          && now._bestPossible.get(windowedDb).equals(before._bestPossible.get(windowedDb));
    }, TIMEOUT));
  }

  /**
   * Helix controller failover inside an active window. The standby takes over, runs its own
   * full baseline from a fresh change detector, and reproduces the persisted state byte for byte:
   * the broken clique carried forward, the healthy clique's top-up in place, the lost node still
   * named. It derives the remaining window from the offline time in ZooKeeper, and when the window
   * ends the new leader makes the permanent move.
   */
  @Test
  public void testControllerFailoverInsideAnActiveWindow() throws Exception {
    ClusterControllerManager standby = startController();
    breakClique(TAG_LOCAL_BREAK);
    Snapshot before = new Snapshot();
    int healthy = 1;
    String db = resourceName(healthy);
    String lost = busiestNode(healthy, before._bestPossible);
    Map<String, Set<String>> topUp = withTopUp(healthy, before._bestPossible, lost);

    stopParticipant(lost);
    long windowEnd = windowEnd(lost);
    Assert.assertTrue(TestHelper.verify(() -> activeReplicas(db).equals(topUp), TIMEOUT));
    Snapshot topUpState = new Snapshot();

    ClusterControllerManager oldLeader = _leader;
    oldLeader.syncStop();
    _leader = standby;
    Assert.assertTrue(TestHelper.verify(() -> standby.isLeader()
            && GenericHelixController.getLeaderController(CLUSTER_NAME) != null
            && rebalancerMetricOrMinusOne("GlobalBaselineCalcCounter") > 0
            && skippedByScope().get(BASELINE_SCOPE)
            .equals(Collections.singleton(resourceName(BROKEN))), TIMEOUT),
        "The new leader must run its own baseline and isolate the broken clique again");
    Assert.assertTrue(TestHelper.verify(() -> {
      Snapshot now = new Snapshot();
      return now._baseline.equals(before._baseline)
          && now._bestPossible.equals(before._bestPossible)
          && now._preferenceLists.equals(topUpState._preferenceLists)
          && activeReplicas(db).equals(topUp);
    }, TIMEOUT), "The new leader must keep the top-up, the carried clique and the lost node");
    assertSkippedByScope(Collections.singleton(resourceName(BROKEN)), Collections.emptySet(),
        Collections.emptySet(), Collections.emptySet());
    Assert.assertEquals(isolationGauge(), 1L);
    Assert.assertEquals(clusterStatusGauge("WagedFallbackInUseGauge"), 0L);

    // Both Helix controllers share one JVM wide delayed rebalance timer, so the old leader's timer
    // would still fire into the new leader. Drop it, as a new leader in its own process never had
    // it, and prove the new leader schedules the same window end on its own from ZooKeeper.
    RebalanceScheduler timers = delayedRebalanceTimers();
    for (int clique = 0; clique < CLIQUE_COUNT; clique++) {
      timers.removeScheduledRebalance(resourceName(clique));
    }
    RebalanceUtil.scheduleOnDemandPipeline(CLUSTER_NAME, 0L);
    Assert.assertTrue(TestHelper.verify(() -> timers.getRebalanceTime(db) == windowEnd, TIMEOUT),
        "The new leader must schedule the window end it derives from the offline time, "
            + windowEnd + ", not " + timers.getRebalanceTime(db));
    assertInWindow(windowEnd, "the failover");

    long movedAt = awaitBestPossibleDropsNode(healthy, lost, windowEnd);
    Assert.assertTrue(movedAt >= windowEnd,
        "The new leader moved " + (windowEnd - movedAt) + " ms before the window end");
    Assert.assertTrue(movedAt - windowEnd < DELAY_MS,
        "The new leader's own timer must drive the permanent move at the window end");
    Assert.assertTrue(TestHelper.verify(() -> {
      Map<String, Map<String, Map<String, String>>> bestPossible = bestPossible();
      return instancesOf(bestPossible.get(db)).equals(topUp)
          && bestPossible.get(resourceName(BROKEN))
          .equals(before._bestPossible.get(resourceName(BROKEN)));
    }, TIMEOUT), "The new leader must make the permanent move and keep the broken clique");
    assertComplete(new Snapshot());
    Assert.assertEquals(baseline(), before._baseline);
    Assert.assertEquals(isolationGauge(), 1L);
  }

  /**
   * The flag off control for a tag-local break. The broken clique has no work in the
   * overwrite scope, so stock WAGED already tops the healthy clique up: the default mode only
   * blocks top-ups when the broken clique itself takes part.
   */
  @Test
  public void testFlagOffTagLocalBreakWithoutBrokenWorkStillTopsUp() throws Exception {
    breakCliqueWithIsolationOff(TAG_LOCAL_BREAK);
    Snapshot before = new Snapshot();
    int healthy = 1;
    String db = resourceName(healthy);
    String lost = busiestNode(healthy, before._bestPossible);
    Map<String, Set<String>> topUp = withTopUp(healthy, before._bestPossible, lost);

    stopParticipant(lost);
    long windowEnd = windowEnd(lost);
    Assert.assertTrue(TestHelper.verify(() -> activeReplicas(db).equals(topUp), TIMEOUT),
        "Stock WAGED tops up a healthy clique when the broken one has no overwrite work");
    assertInWindow(windowEnd, "the stock top-up");
    Assert.assertEquals(clusterStatusGauge("WagedRebalanceOverwriteFailingGauge"), 0L);
    Assert.assertEquals(clusterStatusGauge("WagedFallbackInUseGauge"), 0L);
    Assert.assertEquals(isolationGauge(), 0L);
    Assert.assertEquals(bestPossible(), before._bestPossible);
    Assert.assertEquals(baseline(), before._baseline);
  }

  /**
   * Flag off, the broken clique loses a node too. Its unplaceable top-up fails the whole
   * overwrite, the Helix controller falls back to the last known good assignment, and the healthy
   * clique's top-up is blocked. Turning isolation on inside the same window unblocks it.
   */
  @Test
  public void testFlagOffBrokenCliqueTopUpBlocksEveryTopUp() throws Exception {
    breakCliqueWithIsolationOff(TAG_LOCAL_BREAK);
    Snapshot before = new Snapshot();
    String brokenDb = resourceName(BROKEN);
    String brokenLost = busiestNode(BROKEN, before._bestPossible);
    int healthy = 2;
    String healthyDb = resourceName(healthy);
    String healthyLost = busiestNode(healthy, before._bestPossible);
    Map<String, Set<String>> blocked =
        withoutTopUp(before._bestPossible.get(healthyDb), healthyLost);
    Map<String, Set<String>> topUp = withTopUp(healthy, before._bestPossible, healthyLost);

    stopParticipant(brokenLost);
    long windowEnd = windowEnd(brokenLost);
    Assert.assertTrue(TestHelper.verify(
        () -> clusterStatusGauge("WagedRebalanceOverwriteFailingGauge") == 1L
            && clusterStatusGauge("WagedFallbackInUseGauge") == 1L, TIMEOUT));
    long overwrites = rebalancerMetric("RebalanceOverwriteCounter");
    stopParticipant(healthyLost);
    windowEnd = Math.min(windowEnd, windowEnd(healthyLost));
    Assert.assertTrue(TestHelper.verify(
        () -> rebalancerMetric("RebalanceOverwriteCounter") > overwrites
            && activeReplicas(healthyDb).equals(blocked), TIMEOUT));
    Snapshot blockedState = new Snapshot();
    assertInWindow(windowEnd, "the blocked top-up");
    Assert.assertEquals(blockedState._preferenceLists.get(healthyDb),
        before._preferenceLists.get(healthyDb),
        "With the flag off the healthy clique must get no top-up");
    Assert.assertEquals(clusterStatusGauge("WagedRebalanceOverwriteFailingGauge"), 1L);
    Assert.assertEquals(clusterStatusGauge("WagedFallbackInUseGauge"), 1L);
    Assert.assertEquals(isolationGauge(), 0L);

    long baselines = rebalancerMetric("GlobalBaselineCalcCounter");
    setIsolationEnabled(true);
    Assert.assertTrue(TestHelper.verify(() -> activeReplicas(healthyDb).equals(topUp)
            && activeReplicas(brokenDb)
            .equals(withoutTopUp(before._bestPossible.get(brokenDb), brokenLost))
            && clusterStatusGauge("WagedRebalanceOverwriteFailingGauge") == 0L
            && clusterStatusGauge("WagedFallbackInUseGauge") == 0L, TIMEOUT),
        "With isolation on the healthy clique must be topped up and the broken one skipped");
    assertInWindow(windowEnd, "the isolated top-up");
    Assert.assertEquals(skippedByScope().get(OVERWRITE_SCOPE), Collections.singleton(brokenDb));
    Assert.assertEquals(isolationGauge(), 1L);

    // The checks above can pass on the first pipeline after the flip, before the baseline the flip
    // starts has landed. That baseline recomputes every replica, so the outcome has to hold on the
    // state it and the partial rebalance after it land on.
    awaitBaselineStarted(baselines, "Turning isolation on must start a baseline");
    awaitRebalanceIdle("the isolation baseline");
    assertIsolatedTopUpLanded(before, healthy, healthyLost, windowEnd);
    Assert.assertEquals(activeReplicas(brokenDb),
        withoutTopUp(before._bestPossible.get(brokenDb), brokenLost),
        "The broken clique's top-up must stay omitted once the baseline lands");
  }

  /**
   * A cluster-wide capacity deficit. Clique 0 alone demands more than the cluster holds, so with
   * the flag off the overwrite scope's precheck fails for everyone and the healthy clique gets no
   * top-up. With isolation on the deficit is attributed to clique 0 and the top-up happens.
   */
  @Test
  public void testClusterWideDeficitBlocksEveryTopUpUnlessIsolated() throws Exception {
    breakCliqueWithIsolationOff(CLUSTER_WIDE_BREAK);
    Snapshot before = new Snapshot();
    int healthy = 3;
    String db = resourceName(healthy);
    String lost = busiestNode(healthy, before._bestPossible);
    Map<String, Set<String>> blocked = withoutTopUp(before._bestPossible.get(db), lost);
    Map<String, Set<String>> topUp = withTopUp(healthy, before._bestPossible, lost);

    stopParticipant(lost);
    long windowEnd = windowEnd(lost);
    Assert.assertTrue(TestHelper.verify(
        () -> clusterStatusGauge("WagedRebalanceOverwriteFailingGauge") == 1L
            && clusterStatusGauge("WagedFallbackInUseGauge") == 1L
            && activeReplicas(db).equals(blocked), TIMEOUT));
    Snapshot blockedState = new Snapshot();
    assertInWindow(windowEnd, "the blocked top-up");
    Assert.assertEquals(blockedState._preferenceLists.get(db), before._preferenceLists.get(db));

    long baselines = rebalancerMetric("GlobalBaselineCalcCounter");
    setIsolationEnabled(true);
    Assert.assertTrue(TestHelper.verify(() -> activeReplicas(db).equals(topUp)
            && clusterStatusGauge("WagedRebalanceOverwriteFailingGauge") == 0L
            && clusterStatusGauge("WagedFallbackInUseGauge") == 0L, TIMEOUT),
        "With isolation on the deficit is attributed to clique 0 and the top-up happens");
    assertInWindow(windowEnd, "the isolated top-up");
    Assert.assertEquals(bestPossible().get(resourceName(BROKEN)),
        before._bestPossible.get(resourceName(BROKEN)));
    Assert.assertEquals(isolationGauge(), 1L);

    // The baseline the flag starts attributes the deficit to clique 0 as well, and the top-up has
    // to hold on the state it and the partial rebalance after it land on.
    awaitBaselineStarted(baselines, "Turning isolation on must start a baseline");
    awaitRebalanceIdle("the isolation baseline");
    assertIsolatedTopUpLanded(before, healthy, lost, windowEnd);
  }

  // ---------------------------------------------------------------------------------------------
  // Helpers
  // ---------------------------------------------------------------------------------------------

  /** One consistent read of everything the scenarios assert on. */
  private final class Snapshot {
    final Map<String, Map<String, Map<String, String>>> _bestPossible = bestPossible();
    final Map<String, Map<String, Map<String, String>>> _baseline = baseline();
    final Map<String, Map<String, List<String>>> _preferenceLists = new TreeMap<>();
    final Map<String, Map<String, Map<String, String>>> _idealStateMaps = new TreeMap<>();
    final Map<String, Map<String, Map<String, String>>> _externalViews = externalViews();

    Snapshot() {
      for (int clique = 0; clique < CLIQUE_COUNT; clique++) {
        IdealState idealState =
            _accessor.getProperty(_accessor.keyBuilder().idealStates(resourceName(clique)));
        _preferenceLists.put(resourceName(clique),
            new TreeMap<>(idealState.getRecord().getListFields()));
        Map<String, Map<String, String>> stateMaps = new TreeMap<>();
        idealState.getRecord().getMapFields()
            .forEach((partition, states) -> stateMaps.put(partition, new TreeMap<>(states)));
        _idealStateMaps.put(resourceName(clique), stateMaps);
      }
    }
  }

  private ClusterControllerManager startController() {
    ClusterControllerManager controller = new ClusterControllerManager(ZK_ADDR, CLUSTER_NAME,
        CONTROLLER_PREFIX + "_" + _controllerCounter.getAndIncrement());
    controller.syncStart();
    return controller;
  }

  private List<String> allNodes() {
    List<String> nodes = new ArrayList<>();
    _nodesByClique.values().forEach(nodes::addAll);
    return nodes;
  }

  private ZkHelixClusterVerifier verifier() {
    return verifier(TestHelper.DEFAULT_REBALANCE_PROCESSING_WAIT_TIME);
  }

  private ZkHelixClusterVerifier verifier(int waitTillVerify) {
    Set<String> resources = new HashSet<>();
    for (int clique = 0; clique < CLIQUE_COUNT; clique++) {
      resources.add(resourceName(clique));
    }
    return new StrictMatchExternalViewVerifier.Builder(CLUSTER_NAME).setZkAddr(ZK_ADDR)
        .setDeactivatedNodeAwareness(true).setResources(resources)
        .setWaitTillVerify(waitTillVerify).build();
  }

  private void awaitConvergence(String when) throws Exception {
    Assert.assertTrue(TestHelper.verify(() -> _accessor
            .getChildNames(_accessor.keyBuilder().liveInstances()).containsAll(allNodes()),
        TIMEOUT), when + ": every participant must be live");
    Assert.assertTrue(TestHelper.verify(() -> {
      Map<String, Map<String, Map<String, String>>> bestPossible = bestPossible();
      return isComplete(bestPossible) && isComplete(baseline())
          && bestPossible.equals(baseline()) && isolationGaugeOrMinusOne() == 0L;
    }, TIMEOUT), when + ": the store must settle on the baseline. Best possible " + bestPossible()
        + ", baseline " + baseline());
    Assert.assertTrue(verifier().verifyByPolling(TIMEOUT, 200),
        when + ": the external view must converge");
  }

  private void setIsolationEnabled(boolean enabled) {
    ClusterConfig config = _configAccessor.getClusterConfig(CLUSTER_NAME);
    config.setWagedInstanceTagIsolationEnabled(enabled);
    _configAccessor.setClusterConfig(CLUSTER_NAME, config);
  }

  /**
   * Break clique 0 with isolation on and wait until the baseline has isolated it: clique 0 carried
   * forward unchanged, every other clique converged, the gauge reporting exactly clique 0.
   */
  private void breakClique(int weight) throws Exception {
    Map<String, Map<String, Map<String, String>>> before = bestPossible();
    Map<String, Map<String, Map<String, String>>> beforeBaseline = baseline();
    long baselines = rebalancerMetric("GlobalBaselineCalcCounter");
    setPartitionWeight(BROKEN, weight);
    Assert.assertTrue(TestHelper.verify(
        () -> rebalancerMetric("GlobalBaselineCalcCounter") > baselines
            && skippedByScope().get(BASELINE_SCOPE)
            .equals(Collections.singleton(resourceName(BROKEN)))
            && isolationGauge() == 1L, TIMEOUT), "Clique 0 must be isolated by the baseline");
    Assert.assertTrue(verifier().verifyByPolling(TIMEOUT, 200));
    Assert.assertEquals(bestPossible(), before, "Breaking clique 0 must move nothing");
    Assert.assertEquals(baseline(), beforeBaseline, "The broken clique must be carried forward");
  }

  /**
   * Turn isolation off, then break clique 0 and wait until the baseline has failed on it. The flag
   * is a cluster config field the change detector keeps, so turning it off first recomputes the
   * baseline from scratch, and the store has to settle on that result before the break.
   */
  private void breakCliqueWithIsolationOff(int weight) throws Exception {
    long baselines = rebalancerMetric("GlobalBaselineCalcCounter");
    setIsolationEnabled(false);
    awaitBaselineStarted(baselines, "Turning isolation off must start a baseline");
    settle("isolation off");
    Map<String, Map<String, Map<String, String>>> before = bestPossible();
    Map<String, Map<String, Map<String, String>>> beforeBaseline = baseline();
    setPartitionWeight(BROKEN, weight);
    Assert.assertTrue(TestHelper.verify(
        () -> clusterStatusGauge("WagedBaselineComputeFailingGauge") == 1L, TIMEOUT),
        "With the flag off the baseline must fail as a whole");
    Assert.assertTrue(verifier().verifyByPolling(TIMEOUT, 200));
    Assert.assertEquals(bestPossible(), before);
    Assert.assertEquals(baseline(), beforeBaseline);
  }

  private void setPartitionWeight(int clique, int weight) {
    ResourceConfig resourceConfig =
        _configAccessor.getResourceConfig(CLUSTER_NAME, resourceName(clique));
    if (resourceConfig == null) {
      resourceConfig = new ResourceConfig(resourceName(clique));
    }
    try {
      resourceConfig.setPartitionCapacityMap(Collections
          .singletonMap(ResourceConfig.DEFAULT_PARTITION_KEY,
              Collections.singletonMap(CAPACITY_KEY, weight)));
    } catch (java.io.IOException ex) {
      throw new IllegalStateException(ex);
    }
    _configAccessor.setResourceConfig(CLUSTER_NAME, resourceName(clique), resourceConfig);
    _weights.put(clique, weight);
  }

  private void setNodeCapacity(String node, int capacity) {
    InstanceConfig config = _configAccessor.getInstanceConfig(CLUSTER_NAME, node);
    config.setInstanceCapacityMap(Collections.singletonMap(CAPACITY_KEY, capacity));
    _configAccessor.setInstanceConfig(CLUSTER_NAME, node, config);
    if (capacity == NODE_CAPACITY) {
      _shrunkNodes.remove(node);
    } else {
      _shrunkNodes.add(node);
    }
  }

  private void setInstanceOperation(String node, InstanceConstants.InstanceOperation operation) {
    _gSetupTool.getClusterManagementTool().setInstanceOperation(CLUSTER_NAME, node, operation);
    if (operation == InstanceConstants.InstanceOperation.ENABLE) {
      _nonEnabledNodes.remove(node);
    } else {
      _nonEnabledNodes.add(node);
    }
  }

  private void stopParticipant(String node) {
    _participants.get(node).syncStop();
  }

  private void restartParticipant(String node) {
    MockParticipantManager participant = _participants.get(node);
    if (!participant.isConnected()) {
      MockParticipantManager replacement = new MockParticipantManager(ZK_ADDR, CLUSTER_NAME, node);
      replacement.syncStart();
      _participants.put(node, replacement);
    }
  }

  /**
   * Restarts a lost node inside its window. A restart normally takes well under a second, but it
   * opens new ZooKeeper connections, and a connection attempt that goes unanswered is only retried
   * after the client's 30 second connect timeout, which outlasts the window. A restart that alone
   * outlasts the rest of the window says nothing about the rebalancer, so it throws
   * {@link StalledRestart} for {@link #runWithReturningNode} to handle.
   */
  private void restartInsideTheWindow(String node, long windowEnd) {
    long started = System.currentTimeMillis();
    restartParticipant(node);
    long finished = System.currentTimeMillis();
    if (started < windowEnd && finished >= windowEnd) {
      throw new StalledRestart(node, finished - started, windowEnd - started);
    }
  }

  /**
   * Runs a scenario whose lost node returns inside its window, and runs it once more from a reset
   * cluster if the return missed the window only because the restart itself stalled.
   */
  private void runWithReturningNode(Scenario scenario) throws Exception {
    try {
      scenario.run();
    } catch (StalledRestart stalled) {
      System.out.println(CLASS_NAME + ": " + stalled.getMessage() + ", so the scenario runs again");
      resetCluster();
      scenario.run();
    }
  }

  @FunctionalInterface
  private interface Scenario {
    void run() throws Exception;
  }

  /** A participant restart that alone outlasted the rest of its window. */
  private static final class StalledRestart extends RuntimeException {
    private StalledRestart(String node, long took, long left) {
      super("restarting " + node + " took " + took + " ms with " + left
          + " ms of its window left");
    }
  }

  /**
   * The window end as every Helix controller computes it: the offline time the participant history
   * records in ZooKeeper plus the configured delay.
   */
  private long windowEnd(String node) throws Exception {
    PropertyKey key = _accessor.keyBuilder().participantHistory(node);
    AtomicLong offlineTime = new AtomicLong(-1L);
    Assert.assertTrue(TestHelper.verify(() -> {
      ParticipantHistory history = _accessor.getProperty(key);
      offlineTime.set(history == null ? -1L : history.getLastOfflineTime());
      return offlineTime.get() > 0L;
    }, TIMEOUT), "The participant history must record when " + node + " went offline");
    return offlineTime.get()
        + _configAccessor.getClusterConfig(CLUSTER_NAME).getRebalanceDelayTime();
  }

  private void assertInWindow(long windowEnd, String what) {
    long now = System.currentTimeMillis();
    Assert.assertTrue(now < windowEnd, what + " finished " + (now - windowEnd)
        + " ms after the window end, so they no longer describe the window");
    System.out.println(CLASS_NAME + ": " + what + " held " + (windowEnd - now)
        + " ms before the window end");
  }

  private static void awaitTime(long time) throws Exception {
    TestHelper.verify(() -> System.currentTimeMillis() >= time, Long.MAX_VALUE);
  }

  /**
   * Poll the persisted best possible assignment until the clique no longer names the node, and
   * return when that was first seen.
   */
  private long awaitBestPossibleDropsNode(int clique, String node, long windowEnd)
      throws Exception {
    long timeout = Math.max(0L, windowEnd - System.currentTimeMillis()) + TIMEOUT;
    Assert.assertTrue(TestHelper.verify(
        () -> !namesInstance(bestPossible().get(resourceName(clique)), node), timeout),
        "Clique " + clique + " never moved off " + node);
    long movedAt = System.currentTimeMillis();
    System.out.println(CLASS_NAME + ": clique " + clique + " moved off " + node + " "
        + (movedAt - windowEnd) + " ms after its window end");
    return movedAt;
  }

  private Map<String, Map<String, Map<String, String>>> bestPossible() {
    return normalize(_store.getBestPossibleAssignment());
  }

  private Map<String, Map<String, Map<String, String>>> baseline() {
    return normalize(_store.getBaseline());
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

  private Map<String, Map<String, Map<String, String>>> externalViews() {
    Map<String, Map<String, Map<String, String>>> views = new TreeMap<>();
    for (int clique = 0; clique < CLIQUE_COUNT; clique++) {
      ExternalView view =
          _accessor.getProperty(_accessor.keyBuilder().externalView(resourceName(clique)));
      Map<String, Map<String, String>> byPartition = new TreeMap<>();
      if (view != null) {
        view.getPartitionSet().forEach(
            partition -> byPartition.put(partition, new TreeMap<>(view.getStateMap(partition))));
      }
      views.put(resourceName(clique), byPartition);
    }
    return views;
  }

  /** The instances holding an active (LEADER or STANDBY) replica of each partition. */
  private Map<String, Set<String>> activeReplicas(String resource) {
    Map<String, Set<String>> active = new TreeMap<>();
    externalViews().get(resource).forEach((partition, states) -> {
      Set<String> instances = new TreeSet<>();
      states.forEach((instance, state) -> {
        if (state.equals("LEADER") || state.equals("STANDBY")) {
          instances.add(instance);
        }
      });
      active.put(partition, instances);
    });
    return active;
  }

  private static Map<String, Set<String>> instancesOf(
      Map<String, Map<String, String>> assignment) {
    Map<String, Set<String>> instances = new TreeMap<>();
    if (assignment != null) {
      assignment.forEach(
          (partition, states) -> instances.put(partition, new TreeSet<>(states.keySet())));
    }
    return instances;
  }

  private static Map<String, Set<String>> stateMapInstances(
      Map<String, Map<String, String>> stateMaps) {
    return instancesOf(stateMaps);
  }

  private static Map<String, Set<String>> preferenceListInstances(
      Map<String, List<String>> preferenceLists) {
    Map<String, Set<String>> instances = new TreeMap<>();
    preferenceLists.forEach((partition, list) -> instances.put(partition, new TreeSet<>(list)));
    return instances;
  }

  private static boolean namesInstance(Map<String, Map<String, String>> assignment,
      String instance) {
    return assignment != null && assignment.values().stream()
        .anyMatch(states -> states.containsKey(instance));
  }

  private static int replicaCountOn(Map<String, Map<String, String>> assignment,
      String instance) {
    return assignment == null ? 0
        : (int) assignment.values().stream().filter(states -> states.containsKey(instance))
            .count();
  }

  /** The clique node with the most replicas of the clique's resource, ties broken by name. */
  private String busiestNode(int clique,
      Map<String, Map<String, Map<String, String>>> bestPossible) {
    Map<String, Map<String, String>> assignment = bestPossible.get(resourceName(clique));
    String busiest = null;
    int most = -1;
    for (String node : new TreeSet<>(_nodesByClique.get(clique))) {
      int count = replicaCountOn(assignment, node);
      if (count > most) {
        most = count;
        busiest = node;
      }
    }
    Assert.assertTrue(most > 0, "Clique " + clique + " holds no replica");
    return busiest;
  }

  /**
   * The expected placement with the lost node's partitions topped up. Each such partition gains the
   * one clique node that does not already hold it, which is also where the permanent move goes.
   */
  private Map<String, Set<String>> withTopUp(int clique,
      Map<String, Map<String, Map<String, String>>> bestPossible, String lost) {
    Map<String, Set<String>> expected = new TreeMap<>();
    bestPossible.get(resourceName(clique)).forEach((partition, states) -> {
      Set<String> instances = new TreeSet<>(states.keySet());
      if (instances.remove(lost)) {
        List<String> spare = new ArrayList<>(_nodesByClique.get(clique));
        spare.removeAll(states.keySet());
        spare.removeAll(_nonEnabledNodes);
        Assert.assertEquals(spare.size(), 1, "Partition " + partition + " has no single spare");
        instances.addAll(spare);
      }
      expected.put(partition, instances);
    });
    return expected;
  }

  private static Map<String, Set<String>> withoutTopUp(
      Map<String, Map<String, String>> assignment, String lost) {
    Map<String, Set<String>> expected = instancesOf(assignment);
    expected.values().forEach(instances -> instances.remove(lost));
    return expected;
  }

  private void assertPreferenceListsCarryTopUp(Snapshot now, Snapshot before, int clique,
      String lost) {
    String db = resourceName(clique);
    Map<String, Set<String>> topUp = withTopUp(clique, before._bestPossible, lost);
    before._bestPossible.get(db).forEach((partition, states) -> {
      List<String> list = now._preferenceLists.get(db).get(partition);
      if (states.containsKey(lost)) {
        Set<String> expected = new TreeSet<>(topUp.get(partition));
        expected.add(lost);
        Assert.assertEquals(new TreeSet<>(list), expected,
            "The ideal state must keep the lost node and add the top-up for " + partition);
        Assert.assertEquals(list.size(), REPLICA + 1);
      } else {
        Assert.assertEquals(list, before._preferenceLists.get(db).get(partition));
      }
    });
  }

  private void assertOtherCliquesUntouched(Snapshot now, Snapshot before, int changedClique) {
    for (int clique = 0; clique < CLIQUE_COUNT; clique++) {
      if (clique == changedClique) {
        continue;
      }
      String db = resourceName(clique);
      Assert.assertEquals(now._preferenceLists.get(db), before._preferenceLists.get(db),
          "The ideal state of clique " + clique + " must not change");
      Assert.assertEquals(now._idealStateMaps.get(db), before._idealStateMaps.get(db),
          "The ideal state map of clique " + clique + " must not change");
      Assert.assertEquals(now._externalViews.get(db), before._externalViews.get(db),
          "The external view of clique " + clique + " must not change");
    }
  }

  private void assertWindowedCliqueHeld(Snapshot now, Snapshot before, int clique, String lost,
      Map<String, Set<String>> topUp) {
    String db = resourceName(clique);
    Assert.assertEquals(activeReplicas(db), topUp, "The windowed clique must keep its top-up");
    Assert.assertEquals(now._bestPossible.get(db), before._bestPossible.get(db),
        "The windowed clique must not move permanently inside its window");
    Assert.assertTrue(namesInstance(now._baseline.get(db), lost),
        "The baseline must keep the lost node while it is inside its window");
    Assert.assertEquals(now._bestPossible.get(resourceName(BROKEN)),
        before._bestPossible.get(resourceName(BROKEN)), "The broken clique must stay carried");
    Assert.assertEquals(now._baseline.get(resourceName(BROKEN)),
        before._baseline.get(resourceName(BROKEN)), "The broken clique must stay carried");
  }

  /**
   * The state the flag-on baseline and the partial rebalance after it land on inside the window:
   * the baseline isolates the broken clique and carries it forward, and the healthy clique keeps
   * its lost node and is topped up on the best possible assignment that landed, with no fallback.
   */
  private void assertIsolatedTopUpLanded(Snapshot before, int healthy, String lost,
      long windowEnd) throws Exception {
    Snapshot landed = new Snapshot();
    String brokenDb = resourceName(BROKEN);
    String healthyDb = resourceName(healthy);
    Assert.assertEquals(skippedByScope().get(BASELINE_SCOPE), Collections.singleton(brokenDb),
        "The landed baseline must isolate the broken clique");
    Assert.assertEquals(landed._baseline.get(brokenDb), before._baseline.get(brokenDb),
        "The landed baseline must carry the broken clique forward");
    Assert.assertEquals(landed._bestPossible.get(brokenDb), before._bestPossible.get(brokenDb),
        "The broken clique must stay carried in the best possible assignment");
    Assert.assertTrue(namesInstance(landed._bestPossible.get(healthyDb), lost),
        "The healthy clique must not move off its lost node inside the window");
    Assert.assertEquals(activeReplicas(healthyDb), withTopUp(healthy, landed._bestPossible, lost),
        "The healthy clique must be topped up on the best possible assignment that landed");
    assertComplete(landed);
    Assert.assertEquals(clusterStatusGauge("WagedRebalanceOverwriteFailingGauge"), 0L);
    Assert.assertEquals(clusterStatusGauge("WagedFallbackInUseGauge"), 0L);
    Assert.assertEquals(isolationGauge(), 1L);
    assertInWindow(windowEnd, "the landed isolation baseline");
  }

  private static boolean isComplete(Map<String, Map<String, Map<String, String>>> assignment) {
    if (assignment.size() != CLIQUE_COUNT) {
      return false;
    }
    for (int clique = 0; clique < CLIQUE_COUNT; clique++) {
      Map<String, Map<String, String>> byPartition = assignment.get(resourceName(clique));
      if (byPartition == null || byPartition.size() != PARTITIONS || byPartition.values().stream()
          .anyMatch(states -> states.size() != REPLICA)) {
        return false;
      }
    }
    return true;
  }

  private static void assertComplete(Snapshot snapshot) {
    Assert.assertTrue(isComplete(snapshot._bestPossible),
        "The persisted best possible assignment must stay complete: " + snapshot._bestPossible);
    Assert.assertTrue(isComplete(snapshot._baseline),
        "The persisted baseline must stay complete: " + snapshot._baseline);
  }

  /**
   * Each scope's own snapshot of skipped resources, read from the leader's cluster status monitor.
   * The gauge only exposes their union, and the scenarios need to know which scope reported what.
   */
  @SuppressWarnings("unchecked")
  private Map<String, Set<String>> skippedByScope() throws Exception {
    Map<String, Set<String>> byScope = new TreeMap<>();
    for (ClusterModel.RebalanceScopeType scope : ClusterModel.RebalanceScopeType.values()) {
      byScope.put(scope.name(), new TreeSet<>());
    }
    GenericHelixController leader = GenericHelixController.getLeaderController(CLUSTER_NAME);
    if (leader == null) {
      return byScope;
    }
    Field monitorField = GenericHelixController.class.getDeclaredField("_clusterStatusMonitor");
    monitorField.setAccessible(true);
    ClusterStatusMonitor monitor = (ClusterStatusMonitor) monitorField.get(leader);
    Field scopesField =
        ClusterStatusMonitor.class.getDeclaredField("_wagedIsolationSkippedByScope");
    scopesField.setAccessible(true);
    synchronized (monitor) {
      ((Map<ClusterModel.RebalanceScopeType, Set<String>>) scopesField.get(monitor))
          .forEach((scope, skipped) -> byScope.get(scope.name()).addAll(skipped));
    }
    return byScope;
  }

  private void logScopes(String when) throws Exception {
    System.out.println(CLASS_NAME + ": skipped resources by scope, " + when + ": "
        + skippedByScope());
  }

  private static RebalanceScheduler delayedRebalanceTimers() throws Exception {
    Field field = DelayedRebalanceUtil.class.getDeclaredField("REBALANCE_SCHEDULER");
    field.setAccessible(true);
    return (RebalanceScheduler) field.get(null);
  }

  private void assertSkippedByScope(Set<String> baseline, Set<String> partial,
      Set<String> emergency, Set<String> overwrite) throws Exception {
    Map<String, Set<String>> expected = new TreeMap<>();
    expected.put(BASELINE_SCOPE, new TreeSet<>(baseline));
    expected.put(PARTIAL_SCOPE, new TreeSet<>(partial));
    expected.put(EMERGENCY_SCOPE, new TreeSet<>(emergency));
    expected.put(OVERWRITE_SCOPE, new TreeSet<>(overwrite));
    Assert.assertEquals(skippedByScope(), expected, "Unexpected per scope isolation report");
  }

  private long isolationGauge() throws Exception {
    return clusterStatusGauge("WagedInstanceTagIsolationSkippedResourcesGauge");
  }

  private long isolationGaugeOrMinusOne() {
    try {
      return isolationGauge();
    } catch (Exception ex) {
      return -1L;
    }
  }

  private long clusterStatusGauge(String name) throws Exception {
    return ((Number) ManagementFactory.getPlatformMBeanServer()
        .getAttribute(new ObjectName("ClusterStatus:cluster=" + CLUSTER_NAME), name)).longValue();
  }

  private long rebalancerMetric(String metric) throws Exception {
    return ((Number) ManagementFactory.getPlatformMBeanServer().getAttribute(
        new ObjectName("Rebalancer:ClusterName=" + CLUSTER_NAME + ", EntityName=WagedRebalancer"),
        metric)).longValue();
  }

  private long rebalancerMetricOrMinusOne(String metric) {
    try {
      return rebalancerMetric(metric);
    } catch (Exception ex) {
      return -1L;
    }
  }
}
