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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import com.google.common.collect.ImmutableMap;
import org.apache.helix.HelixDataAccessor;
import org.apache.helix.NotificationContext;
import org.apache.helix.TestHelper;
import org.apache.helix.common.ZkTestBase;
import org.apache.helix.controller.rebalancer.waged.AssignmentMetadataStore;
import org.apache.helix.integration.manager.ClusterControllerManager;
import org.apache.helix.integration.manager.MockParticipantManager;
import org.apache.helix.manager.zk.ZKHelixDataAccessor;
import org.apache.helix.manager.zk.ZkBucketDataAccessor;
import org.apache.helix.model.ClusterConfig;
import org.apache.helix.model.ExternalView;
import org.apache.helix.model.IdealState;
import org.apache.helix.model.Message;
import org.apache.helix.model.ResourceAssignment;
import org.apache.helix.model.StateModelDefinition;
import org.apache.helix.participant.StateMachineEngine;
import org.apache.helix.participant.statemachine.StateModel;
import org.apache.helix.participant.statemachine.StateModelFactory;
import org.apache.helix.participant.statemachine.StateModelInfo;
import org.apache.helix.participant.statemachine.Transition;
import org.apache.helix.zookeeper.datamodel.ZNRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

/**
 * END-TO-END integration reproduction attempt for the chi_5 non-placement incident
 * (cluster uic-hs-31, fabric prod-lva1).
 *
 * <p>Scenario faithfully recreated:
 * <ul>
 *   <li>WagedRebalancer, SEAS {@code IndexerStateModel}, per-instance capacity = 2, partition
 *       weight = 1, MIN_ACTIVE = 1 (incident-exact).</li>
 *   <li>Two "poisoned" instances physically hold replicas of a resource ({@code stuck}, the
 *       admintest25 analog) that are wedged in {@code INDEX_DOWNLOADED} (the transition to the
 *       counted top state {@code CAUGHT_UP} blocks forever, as SEAS build 1.0.1716.15 did). The
 *       drop transition is also blocked so the poison persists (the replicas cannot vacate).</li>
 *   <li>The poisoned instances are then re-tagged so the planner wants {@code stuck} elsewhere
 *       (freeing them in the planner's ledger), while a freshly-created victim resource
 *       ({@code chi}) is tag-pinned onto them.</li>
 * </ul>
 *
 * <p>Decisive diagnostic printed/asserted below:
 * <ul>
 *   <li>best-possible assignment (the PLANNER's output) for {@code chi} -&gt; expected PLACED on a
 *       poisoned node.</li>
 *   <li>ExternalView (the ENFORCER's output) for {@code chi} -&gt; expected EMPTY (0 replicas,
 *       below MIN_ACTIVE).</li>
 * </ul>
 * planner-placed + enforcer-empty == the two-ledger divergence, reproduced end-to-end. If BOTH are
 * empty, the run instead shows benign capacity exhaustion (reported honestly, not asserted as the
 * bug).
 */
public class TestWagedIndexerStuckStateStarvation extends ZkTestBase {
  private static final Logger LOG =
      LoggerFactory.getLogger(TestWagedIndexerStuckStateStarvation.class);

  private static final String STATE_MODEL = "IndexerStateModel";
  private static final String CAPACITY_KEY = "SLOT";
  private static final int INSTANCE_CAPACITY = 2; // incident-exact
  private static final int PARTITION_WEIGHT = 1;  // incident-exact

  private static final String POISON_TAG = "POISON"; // stuck resource's tag
  private static final String CHI_TAG = "CHI";        // victim resource's tag

  private static final String STUCK_RESOURCE = "stuck"; // admintest25 analog
  private static final String CHI_RESOURCE = "chi";     // chi analog

  private static final int START_PORT = 13000;

  // Blocks INDEX_DOWNLOADED->CAUGHT_UP (catch-up) and INDEX_DOWNLOADED->OFFLINE (drop) so replicas
  // wedge in INDEX_DOWNLOADED and cannot vacate. Released only at teardown.
  private static final CountDownLatch BLOCK = new CountDownLatch(1);

  private final String CLASS_NAME = getShortClassName();
  private final String CLUSTER_NAME = CLUSTER_PREFIX + "_" + CLASS_NAME;

  private final List<MockParticipantManager> _participants = new ArrayList<>();
  private final List<String> _poisonNodes = new ArrayList<>(); // P0, P1
  private final List<String> _spareNodes = new ArrayList<>();   // S0..S3
  private ClusterControllerManager _controller;
  private AssignmentMetadataStore _assignmentMetadataStore;
  private HelixDataAccessor _dataAccessor;

  @BeforeClass
  public void beforeClass() throws Exception {
    System.out.println("START " + CLASS_NAME + " at " + new Date(System.currentTimeMillis()));

    _gSetupTool.addCluster(CLUSTER_NAME, true);
    _gSetupTool.addStateModelDef(CLUSTER_NAME, STATE_MODEL, buildIndexerStateModelDef());

    // 2 poisoned + 4 spare instances. Poisoned nodes start with POISON_TAG so `stuck` lands there.
    for (int i = 0; i < 2; i++) {
      String node = PARTICIPANT_PREFIX + "_" + (START_PORT + i);
      _gSetupTool.addInstanceToCluster(CLUSTER_NAME, node);
      _gSetupTool.getClusterManagementTool().addInstanceTag(CLUSTER_NAME, node, POISON_TAG);
      _poisonNodes.add(node);
    }
    for (int i = 2; i < 6; i++) {
      String node = PARTICIPANT_PREFIX + "_" + (START_PORT + i);
      _gSetupTool.addInstanceToCluster(CLUSTER_NAME, node);
      _spareNodes.add(node);
    }

    // WAGED capacity: every instance holds 2 slots; every partition weighs 1 (incident-exact).
    _dataAccessor = new ZKHelixDataAccessor(CLUSTER_NAME, _baseAccessor);
    ClusterConfig clusterConfig = _dataAccessor.getProperty(_dataAccessor.keyBuilder().clusterConfig());
    clusterConfig.setInstanceCapacityKeys(Collections.singletonList(CAPACITY_KEY));
    clusterConfig.setDefaultInstanceCapacityMap(ImmutableMap.of(CAPACITY_KEY, INSTANCE_CAPACITY));
    clusterConfig.setDefaultPartitionWeightMap(ImmutableMap.of(CAPACITY_KEY, PARTITION_WEIGHT));
    _dataAccessor.setProperty(_dataAccessor.keyBuilder().clusterConfig(), clusterConfig);

    // Participants with the custom IndexerStateModel factory (blocking transitions).
    for (String node : allNodes()) {
      MockParticipantManager participant =
          new MockParticipantManager(ZK_ADDR, CLUSTER_NAME, node);
      StateMachineEngine engine = participant.getStateMachineEngine();
      engine.registerStateModelFactory(STATE_MODEL, new IndexerModelFactory());
      participant.syncStart();
      _participants.add(participant);
    }

    _controller = new ClusterControllerManager(ZK_ADDR, CLUSTER_NAME, CONTROLLER_PREFIX + "_0");
    _controller.syncStart();

    enablePersistBestPossibleAssignment(_gZkClient, CLUSTER_NAME, true);
    _assignmentMetadataStore =
        new AssignmentMetadataStore(new ZkBucketDataAccessor(ZK_ADDR), CLUSTER_NAME) {
          public Map<String, ResourceAssignment> getBaseline() {
            super.reset();
            return super.getBaseline();
          }

          public synchronized Map<String, ResourceAssignment> getBestPossibleAssignment() {
            super.reset();
            return super.getBestPossibleAssignment();
          }
        };
  }

  @Test
  public void reproduceStuckStateStarvation() throws Exception {
    // ---------- Phase 1: poison P0/P1 with wedged INDEX_DOWNLOADED replicas ----------
    createWagedResource(STUCK_RESOURCE, 4 /*partitions*/, 1 /*replica*/, POISON_TAG);

    // All `stuck` replicas must wedge in INDEX_DOWNLOADED on the two poisoned nodes (2 each),
    // fully consuming their physical capacity (2 slots/instance).
    Assert.assertTrue(TestHelper.verify(() -> {
      Map<String, String> ev = flatExternalView(STUCK_RESOURCE);
      long wedgedOnPoison = ev.entrySet().stream()
          .filter(e -> "INDEX_DOWNLOADED".equals(e.getValue()))
          .filter(e -> _poisonNodes.contains(instanceOf(e.getKey())))
          .count();
      return ev.size() == 4 && wedgedOnPoison == 4;
    }, 30_000), "Phase 1 setup: `stuck` must wedge 4 replicas in INDEX_DOWNLOADED on the poisoned "
        + "nodes. Actual ExternalView=" + flatExternalView(STUCK_RESOURCE));
    System.out.println("[phase1] stuck ExternalView = " + flatExternalView(STUCK_RESOURCE));

    // ---------- Phase 2: steer the victim onto the (physically full) poisoned nodes ----------
    // Re-tag so the planner wants `stuck` on the spares (freeing P0/P1 in its ledger), while the
    // wedged replicas physically remain on P0/P1 (the drop transition is blocked).
    for (String p : _poisonNodes) {
      _gSetupTool.getClusterManagementTool().removeInstanceTag(CLUSTER_NAME, p, POISON_TAG);
      _gSetupTool.getClusterManagementTool().addInstanceTag(CLUSTER_NAME, p, CHI_TAG);
    }
    for (String s : _spareNodes) {
      _gSetupTool.getClusterManagementTool().addInstanceTag(CLUSTER_NAME, s, POISON_TAG);
    }

    createWagedResource(CHI_RESOURCE, 2 /*partitions*/, 1 /*replica*/, CHI_TAG);

    // ---------- The divergence, asserted end-to-end ----------
    // Stable steady state expected:
    //   * PLANNER (best-possible) places BOTH chi partitions onto the poisoned nodes (they look
    //     free in the planner's ledger because the wedged INDEX_DOWNLOADED occupants are in a
    //     non-counted state).
    //   * ENFORCER (ExternalView, via DelayedAutoRebalancer#computeBestPossiblePartitionState ->
    //     cache.checkAndReduceCapacity, DelayedAutoRebalancer.java:378-385) prunes them for "no
    //     capacity to hold", so chi has ZERO active replicas -> below MIN_ACTIVE = 1.
    boolean divergence = TestHelper.verify(() -> {
      Map<String, String> bp = flatBestPossible(CHI_RESOURCE);
      Map<String, String> ev = flatExternalView(CHI_RESOURCE);
      boolean plannerPlacedBothOnPoison = bp.size() == 2
          && bp.keySet().stream().allMatch(k -> _poisonNodes.contains(instanceOf(k)));
      long activeInEv = ev.values().stream().filter(TestWagedIndexerStuckStateStarvation::isActive)
          .count();
      return plannerPlacedBothOnPoison && activeInEv == 0;
    }, 60_000);

    Map<String, String> chiBestPossible = flatBestPossible(CHI_RESOURCE);
    Map<String, String> chiExternalView = flatExternalView(CHI_RESOURCE);
    Map<String, String> stuckExternalView = flatExternalView(STUCK_RESOURCE);

    System.out.println("=====================================================================");
    System.out.println("[DIAG] stuck ExternalView (enforcer, poison persists) = " + stuckExternalView);
    System.out.println("[DIAG] chi   best-possible (planner)  = " + chiBestPossible);
    System.out.println("[DIAG] chi   ExternalView   (enforcer) = " + chiExternalView);
    System.out.println("[DIAG] chi placed-by-planner=" + chiBestPossible.size()
        + " ; chi active-in-EV="
        + chiExternalView.values().stream().filter(TestWagedIndexerStuckStateStarvation::isActive).count());
    System.out.println("=====================================================================");

    // The planner placed both chi partitions on the poisoned nodes...
    Assert.assertEquals(chiBestPossible.size(), 2,
        "PLANNER (best-possible) should place both chi partitions. Actual=" + chiBestPossible);
    Assert.assertTrue(
        chiBestPossible.keySet().stream().allMatch(k -> _poisonNodes.contains(instanceOf(k))),
        "PLANNER should place chi onto the physically-full poisoned nodes. Actual=" + chiBestPossible);

    // ...but the enforcer pruned them, so chi is starved below MIN_ACTIVE (the incident symptom).
    long chiActiveInEv =
        chiExternalView.values().stream().filter(TestWagedIndexerStuckStateStarvation::isActive).count();
    Assert.assertEquals(chiActiveInEv, 0L,
        "ENFORCER should prune every chi placement (planner assigned onto full nodes) -> chi below "
            + "MIN_ACTIVE. Actual ExternalView=" + chiExternalView);

    // Corroboration: the poison persisted -- the original wedged replicas are still on P0/P1.
    long stuckStillWedgedOnPoison = stuckExternalView.entrySet().stream()
        .filter(e -> "INDEX_DOWNLOADED".equals(e.getValue()))
        .filter(e -> _poisonNodes.contains(instanceOf(e.getKey())))
        .count();
    Assert.assertEquals(stuckStillWedgedOnPoison, 4L,
        "The poison must persist: the 4 original stuck replicas remain wedged in INDEX_DOWNLOADED "
            + "on the poisoned nodes. Actual=" + stuckExternalView);

    Assert.assertTrue(divergence,
        "REPRODUCTION: planner placed chi on poisoned nodes while the enforcer pruned it (two-ledger "
            + "capacity divergence), leaving chi below MIN_ACTIVE.");
    System.out.println("[RESULT] REPRODUCED end-to-end: planner placed chi on physically-full "
        + "poisoned nodes; enforcer pruned it; chi starved below MIN_ACTIVE (two-ledger divergence).");
  }

  private static boolean isActive(String state) {
    return "CAUGHT_UP".equals(state) || "INDEX_DOWNLOADED".equals(state) || "ASSIGNED".equals(state);
  }

  @AfterClass
  public void afterClass() throws Exception {
    BLOCK.countDown(); // release any wedged transition threads so participants can stop
    if (_controller != null) {
      _controller.syncStop();
    }
    for (MockParticipantManager p : _participants) {
      p.syncStop();
    }
    deleteCluster(CLUSTER_NAME);
    System.out.println("END " + CLASS_NAME + " at " + new Date(System.currentTimeMillis()));
  }

  // ------------------------------------------------------------------ helpers

  private List<String> allNodes() {
    List<String> all = new ArrayList<>(_poisonNodes);
    all.addAll(_spareNodes);
    return all;
  }

  private void createWagedResource(String resource, int partitions, int replica, String tag) {
    createResourceWithWagedRebalance(CLUSTER_NAME, resource, STATE_MODEL, partitions, replica,
        replica);
    IdealState is = _gSetupTool.getClusterManagementTool().getResourceIdealState(CLUSTER_NAME, resource);
    is.setInstanceGroupTag(tag);
    _gSetupTool.getClusterManagementTool().setResourceIdealState(CLUSTER_NAME, resource, is);
    _gSetupTool.rebalanceStorageCluster(CLUSTER_NAME, resource, replica);
  }

  /** partition:instance -> state, from the live ExternalView. */
  private Map<String, String> flatExternalView(String resource) {
    ExternalView ev = _dataAccessor.getProperty(_dataAccessor.keyBuilder().externalView(resource));
    Map<String, String> flat = new HashMap<>();
    if (ev == null) {
      return flat;
    }
    for (String partition : ev.getPartitionSet()) {
      for (Map.Entry<String, String> e : ev.getStateMap(partition).entrySet()) {
        flat.put(partition + ":" + e.getKey(), e.getValue());
      }
    }
    return flat;
  }

  /** partition:instance -> state, from the persisted best-possible assignment (planner output). */
  private Map<String, String> flatBestPossible(String resource) {
    Map<String, ResourceAssignment> best = _assignmentMetadataStore.getBestPossibleAssignment();
    Map<String, String> flat = new HashMap<>();
    ResourceAssignment ra = best.get(resource);
    if (ra == null) {
      return flat;
    }
    ra.getMappedPartitions().forEach(p ->
        ra.getReplicaMap(p).forEach((inst, state) -> flat.put(p.getPartitionName() + ":" + inst, state)));
    return flat;
  }

  private static String instanceOf(String partitionColonInstance) {
    return partitionColonInstance.substring(partitionColonInstance.indexOf(':') + 1);
  }

  // ------------------------------------------------------------------ SEAS IndexerStateModel

  private static StateModelDefinition buildIndexerStateModelDef() {
    ZNRecord record = new ZNRecord(STATE_MODEL);
    record.setSimpleField("INITIAL_STATE", "OFFLINE");

    record.setMapField("CAUGHT_UP.meta", meta("R"));
    record.setMapField("INDEX_DOWNLOADED.meta", meta("-1"));
    record.setMapField("ASSIGNED.meta", meta("-1"));
    record.setMapField("OFFLINE.meta", meta("-1"));
    record.setMapField("DROPPED.meta", meta("-1"));
    record.setMapField("ERROR.meta", meta("-1"));

    record.setMapField("CAUGHT_UP.next", mapOf("CAUGHT_UP", "CAUGHT_UP", "DROPPED", "OFFLINE",
        "INDEX_DOWNLOADED", "OFFLINE", "ASSIGNED", "OFFLINE", "OFFLINE", "OFFLINE"));
    record.setMapField("INDEX_DOWNLOADED.next", mapOf("CAUGHT_UP", "CAUGHT_UP", "DROPPED", "OFFLINE",
        "INDEX_DOWNLOADED", "INDEX_DOWNLOADED", "ASSIGNED", "OFFLINE", "OFFLINE", "OFFLINE"));
    record.setMapField("ASSIGNED.next", mapOf("CAUGHT_UP", "INDEX_DOWNLOADED", "DROPPED", "OFFLINE",
        "INDEX_DOWNLOADED", "INDEX_DOWNLOADED", "ASSIGNED", "ASSIGNED", "OFFLINE", "OFFLINE"));
    record.setMapField("OFFLINE.next", mapOf("DROPPED", "DROPPED", "CAUGHT_UP", "ASSIGNED",
        "INDEX_DOWNLOADED", "ASSIGNED", "ASSIGNED", "ASSIGNED", "OFFLINE", "OFFLINE"));
    record.setMapField("DROPPED.next", mapOf("DROPPED", "DROPPED"));
    record.setMapField("ERROR.next", mapOf("DROPPED", "DROPPED", "ERROR", "ERROR", "OFFLINE",
        "OFFLINE"));

    record.setListField("STATE_PRIORITY_LIST",
        Arrays.asList("CAUGHT_UP", "INDEX_DOWNLOADED", "ASSIGNED", "OFFLINE", "DROPPED", "ERROR"));
    record.setListField("STATE_TRANSITION_PRIORITYLIST",
        Arrays.asList("INDEX_DOWNLOADED-CAUGHT_UP", "CAUGHT_UP-OFFLINE", "ASSIGNED-INDEX_DOWNLOADED",
            "INDEX_DOWNLOADED-OFFLINE", "OFFLINE-ASSIGNED", "ASSIGNED-OFFLINE", "OFFLINE-DROPPED"));

    return new StateModelDefinition(record);
  }

  private static Map<String, String> meta(String count) {
    return ImmutableMap.of("count", count);
  }

  private static Map<String, String> mapOf(String... kv) {
    Map<String, String> m = new HashMap<>();
    for (int i = 0; i < kv.length; i += 2) {
      m.put(kv[i], kv[i + 1]);
    }
    return m;
  }

  public static class IndexerModelFactory extends StateModelFactory<IndexerStateModel> {
    @Override
    public IndexerStateModel createNewStateModel(String resourceName, String partitionKey) {
      return new IndexerStateModel();
    }
  }

  @StateModelInfo(initialState = "OFFLINE", states = {
      "CAUGHT_UP", "INDEX_DOWNLOADED", "ASSIGNED", "OFFLINE", "DROPPED", "ERROR"
  })
  public static class IndexerStateModel extends StateModel {
    @Transition(to = "ASSIGNED", from = "OFFLINE")
    public void onBecomeAssignedFromOffline(Message m, NotificationContext c) {
      LOG.info("{} OFFLINE->ASSIGNED", m.getPartitionName());
    }

    @Transition(to = "INDEX_DOWNLOADED", from = "ASSIGNED")
    public void onBecomeIndexDownloadedFromAssigned(Message m, NotificationContext c) {
      LOG.info("{} ASSIGNED->INDEX_DOWNLOADED", m.getPartitionName());
    }

    // The wedge: catch-up to the counted top state never completes (SEAS build 1.0.1716.15).
    @Transition(to = "CAUGHT_UP", from = "INDEX_DOWNLOADED")
    public void onBecomeCaughtUpFromIndexDownloaded(Message m, NotificationContext c)
        throws InterruptedException {
      LOG.info("{} INDEX_DOWNLOADED->CAUGHT_UP BLOCKING (wedged)", m.getPartitionName());
      BLOCK.await(5, TimeUnit.MINUTES);
    }

    // The drop is also blocked so the poison persists on the node.
    @Transition(to = "OFFLINE", from = "INDEX_DOWNLOADED")
    public void onBecomeOfflineFromIndexDownloaded(Message m, NotificationContext c)
        throws InterruptedException {
      LOG.info("{} INDEX_DOWNLOADED->OFFLINE BLOCKING (cannot vacate)", m.getPartitionName());
      BLOCK.await(5, TimeUnit.MINUTES);
    }

    @Transition(to = "OFFLINE", from = "CAUGHT_UP")
    public void onBecomeOfflineFromCaughtUp(Message m, NotificationContext c) {
      LOG.info("{} CAUGHT_UP->OFFLINE", m.getPartitionName());
    }

    @Transition(to = "OFFLINE", from = "ASSIGNED")
    public void onBecomeOfflineFromAssigned(Message m, NotificationContext c) {
      LOG.info("{} ASSIGNED->OFFLINE", m.getPartitionName());
    }

    @Transition(to = "DROPPED", from = "OFFLINE")
    public void onBecomeDroppedFromOffline(Message m, NotificationContext c) {
      LOG.info("{} OFFLINE->DROPPED", m.getPartitionName());
    }

    @Transition(to = "OFFLINE", from = "ERROR")
    public void onBecomeOfflineFromError(Message m, NotificationContext c) {
      LOG.info("{} ERROR->OFFLINE", m.getPartitionName());
    }
  }
}
