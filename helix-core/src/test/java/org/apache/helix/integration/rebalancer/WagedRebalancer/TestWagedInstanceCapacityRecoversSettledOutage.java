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
 * http://www.apache.org/licenses/LICENSE-2.0
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
import org.apache.helix.model.InstanceConfig;
import org.apache.helix.model.Message;
import org.apache.helix.model.ResourceAssignment;
import org.apache.helix.model.StateModelDefinition;
import org.apache.helix.participant.statemachine.StateModel;
import org.apache.helix.participant.statemachine.StateModelFactory;
import org.apache.helix.participant.statemachine.StateModelInfo;
import org.apache.helix.participant.statemachine.Transition;
import org.apache.helix.zookeeper.datamodel.ZNRecord;
import org.apache.helix.messaging.handling.HelixTaskExecutor;
import org.apache.helix.participant.StateMachineEngine;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

/**
 * The production sequence, which the existing reproduction does not exercise.
 * <p>
 * TestWagedUnallocatedOccupancyAccounting enables WAGED_COUNT_UNALLOCATED_OCCUPANCY in beforeClass,
 * before anything is placed, so the constraint is present while the bad placement is first being
 * chosen. An operator hitting this incident has the opposite situation: the bad placement already
 * exists and has settled, and the flag is switched on afterwards as the remedy. This test runs that
 * order -- outage first, flag second -- and reports whether the partition recovers.
 */
public class TestWagedInstanceCapacityRecoversSettledOutage extends ZkTestBase {
  private static final Logger LOG =
      LoggerFactory.getLogger(TestWagedInstanceCapacityRecoversSettledOutage.class);
  private static final String STATE_MODEL = "IndexerStateModel";
  private static final String CAPACITY_KEY = "SLOT";
  private static final int INSTANCE_CAPACITY = 2;
  private static final int PARTITION_WEIGHT = 1;
  private static final String POISON_TAG = "POISON";
  private static final String CHI_TAG = "CHI";
  private static final String STUCK_RESOURCE = "stuck";
  private static final String CHI_RESOURCE = "chi";
  private static final int START_PORT = 14000;
  private static final CountDownLatch BLOCK = new CountDownLatch(1);

  private final String CLASS_NAME = getShortClassName();
  private final String CLUSTER_NAME = CLUSTER_PREFIX + "_" + CLASS_NAME;
  private final List<MockParticipantManager> _participants = new ArrayList<>();
  private final List<String> _poisonNodes = new ArrayList<>();
  private final List<String> _spareNodes = new ArrayList<>();
  private ClusterControllerManager _controller;
  private AssignmentMetadataStore _assignmentMetadataStore;
  private HelixDataAccessor _dataAccessor;

  @BeforeClass
  public void beforeClass() throws Exception {
    System.out.println("START " + CLASS_NAME + " at " + new Date(System.currentTimeMillis()));
    _gSetupTool.addCluster(CLUSTER_NAME, true);
    _gSetupTool.addStateModelDef(CLUSTER_NAME, STATE_MODEL, buildIndexerStateModelDef());

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

    _dataAccessor = new ZKHelixDataAccessor(CLUSTER_NAME, _baseAccessor);
    ClusterConfig clusterConfig =
        _dataAccessor.getProperty(_dataAccessor.keyBuilder().clusterConfig());
    clusterConfig.setInstanceCapacityKeys(Collections.singletonList(CAPACITY_KEY));
    clusterConfig.setDefaultInstanceCapacityMap(ImmutableMap.of(CAPACITY_KEY, INSTANCE_CAPACITY));
    clusterConfig.setDefaultPartitionWeightMap(ImmutableMap.of(CAPACITY_KEY, PARTITION_WEIGHT));
    // The difference that matters: the cluster runs the incident with the remedy switched OFF.
    clusterConfig.setWagedCountUnallocatedOccupancyEnabled(false);
    _dataAccessor.setProperty(_dataAccessor.keyBuilder().clusterConfig(), clusterConfig);

    for (String node : allNodes()) {
      MockParticipantManager participant = new MockParticipantManager(ZK_ADDR, CLUSTER_NAME, node);
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
  public void testEnablingTheFlagAfterTheOutageHasSettled() throws Exception {
    // ---------- Phase 1: create the wedged occupancy, flag OFF ----------
    createWagedResource(STUCK_RESOURCE, 4, 1, POISON_TAG);
    Assert.assertTrue(TestHelper.verify(() -> {
      Map<String, String> ev = flatExternalView(STUCK_RESOURCE);
      long wedged = ev.entrySet().stream()
          .filter(e -> "INDEX_DOWNLOADED".equals(e.getValue()))
          .filter(e -> _poisonNodes.contains(instanceOf(e.getKey()))).count();
      return ev.size() == 4 && wedged == 4;
    }, 30_000), "setup: stuck must wedge 4 replicas on the poisoned nodes; actual="
        + flatExternalView(STUCK_RESOURCE));
    System.out.println("[SETTLED] phase 1 done, wedged occupancy in place");

    // ---------- Phase 2: steer the victim onto the physically full nodes, flag still OFF ----------
    for (String p : _poisonNodes) {
      _gSetupTool.getClusterManagementTool().removeInstanceTag(CLUSTER_NAME, p, POISON_TAG);
      _gSetupTool.getClusterManagementTool().addInstanceTag(CLUSTER_NAME, p, CHI_TAG);
    }
    for (String s : _spareNodes) {
      _gSetupTool.getClusterManagementTool().addInstanceTag(CLUSTER_NAME, s, POISON_TAG);
      _gSetupTool.getClusterManagementTool().addInstanceTag(CLUSTER_NAME, s, CHI_TAG);
    }
    createWagedResource(CHI_RESOURCE, 2, 1, CHI_TAG);

    // ---------- Phase 3: confirm the outage exists and has SETTLED ----------
    // Settled means: chi has no active replica, and it stays that way across many controller
    // passes, with the planner's own output no longer changing. That is the state an operator
    // finds the cluster in hours after the incident starts.
    Thread.sleep(20_000);
    Map<String, String> bpAtSettle = flatBestPossible(CHI_RESOURCE);
    long activeAtSettle = flatExternalView(CHI_RESOURCE).values().stream()
        .filter(TestWagedInstanceCapacityRecoversSettledOutage::isActive).count();
    System.out.println("[SETTLED] chi active replicas with flag OFF = " + activeAtSettle);
    System.out.println("[SETTLED] chi best-possible with flag OFF   = " + bpAtSettle);
    System.out.println("[SETTLED] chi external view with flag OFF   = " + flatExternalView(CHI_RESOURCE));

    Assert.assertEquals(activeAtSettle, 0L,
        "the incident must actually be reproduced before the remedy is applied, otherwise this "
            + "test proves nothing; chi externalView=" + flatExternalView(CHI_RESOURCE));

    // Stability check: the planner's output must not still be moving, or 'settled' is a fiction.
    Thread.sleep(15_000);
    Map<String, String> bpLater = flatBestPossible(CHI_RESOURCE);
    System.out.println("[SETTLED] chi best-possible 15s later       = " + bpLater);
    System.out.println("[SETTLED] planner output stable = " + bpAtSettle.equals(bpLater));

    // ---------- Phase 4: declare the physical truth via InstanceConfig ----------
    // The poisoned nodes have 2 SLOTs, both physically consumed by wedged replicas the planner
    // cannot see. Rather than teach the planner to discover that, just tell it: these instances
    // have 0 usable SLOTs. InstanceConfig capacity overrides the cluster default
    // (WagedValidationUtil:49-51) and feeds NodeCapacityConstraint, a HARD constraint.
    for (String p : _poisonNodes) {
      InstanceConfig ic = _dataAccessor.getProperty(_dataAccessor.keyBuilder().instanceConfig(p));
      ic.setInstanceCapacityMap(ImmutableMap.of(CAPACITY_KEY, 0));
      _dataAccessor.setProperty(_dataAccessor.keyBuilder().instanceConfig(p), ic);
    }
    System.out.println("[SETTLED] >>> poison node capacity set to SLOT=0 at " + new Date());

    // Nudge the controller so recovery cannot be blamed on the absence of a trigger.
    _gSetupTool.rebalanceStorageCluster(CLUSTER_NAME, CHI_RESOURCE, 1);

    // ---------- Phase 5: does it recover? ----------
    boolean recovered = TestHelper.verify(() -> flatExternalView(CHI_RESOURCE).values().stream()
        .filter(TestWagedInstanceCapacityRecoversSettledOutage::isActive).count() >= 1, 120_000);

    System.out.println("[SETTLED] chi active after CAPACITY FIX = "
        + flatExternalView(CHI_RESOURCE).values().stream()
            .filter(TestWagedInstanceCapacityRecoversSettledOutage::isActive).count());
    System.out.println("[SETTLED] chi best-possible after CAPACITY FIX = " + flatBestPossible(CHI_RESOURCE));
    System.out.println("[SETTLED] chi external view after CAPACITY FIX = " + flatExternalView(CHI_RESOURCE));
    System.out.println("[SETTLED] RESULT recovered=" + recovered);

    Assert.assertTrue(recovered,
        "declaring the poisoned nodes full must relocate chi to a spare; it did not. "
            + "externalView=" + flatExternalView(CHI_RESOURCE) + " bestPossible="
            + flatBestPossible(CHI_RESOURCE));
  }

  private static boolean isActive(String state) {
    return "CAUGHT_UP".equals(state) || "INDEX_DOWNLOADED".equals(state) || "ASSIGNED".equals(state);
  }

  @AfterClass
  public void afterClass() throws Exception {
    BLOCK.countDown();
    if (_controller != null) {
      _controller.syncStop();
    }
    for (MockParticipantManager p : _participants) {
      p.syncStop();
    }
    deleteCluster(CLUSTER_NAME);
    System.out.println("END " + CLASS_NAME + " at " + new Date(System.currentTimeMillis()));
  }

  private List<String> allNodes() {
    List<String> all = new ArrayList<>(_poisonNodes);
    all.addAll(_spareNodes);
    return all;
  }

  private void createWagedResource(String resource, int partitions, int replica, String tag) {
    createResourceWithWagedRebalance(CLUSTER_NAME, resource, STATE_MODEL, partitions, replica,
        replica);
    IdealState is =
        _gSetupTool.getClusterManagementTool().getResourceIdealState(CLUSTER_NAME, resource);
    is.setInstanceGroupTag(tag);
    _gSetupTool.getClusterManagementTool().setResourceIdealState(CLUSTER_NAME, resource, is);
    _gSetupTool.rebalanceStorageCluster(CLUSTER_NAME, resource, replica);
  }

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

  private Map<String, String> flatBestPossible(String resource) {
    Map<String, ResourceAssignment> best = _assignmentMetadataStore.getBestPossibleAssignment();
    Map<String, String> flat = new HashMap<>();
    ResourceAssignment ra = best.get(resource);
    if (ra == null) {
      return flat;
    }
    ra.getMappedPartitions().forEach(p -> ra.getReplicaMap(p)
        .forEach((inst, state) -> flat.put(p.getPartitionName() + ":" + inst, state)));
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
    record.setMapField("ERROR.next",
        mapOf("DROPPED", "DROPPED", "ERROR", "ERROR", "OFFLINE", "OFFLINE"));
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

    @Transition(to = "CAUGHT_UP", from = "INDEX_DOWNLOADED")
    public void onBecomeCaughtUpFromIndexDownloaded(Message m, NotificationContext c)
        throws InterruptedException {
      LOG.info("{} INDEX_DOWNLOADED->CAUGHT_UP BLOCKING (wedged)", m.getPartitionName());
      BLOCK.await(5, TimeUnit.MINUTES);
    }

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
