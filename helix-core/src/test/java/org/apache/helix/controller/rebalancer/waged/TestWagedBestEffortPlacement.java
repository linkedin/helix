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
import java.util.Comparator;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import com.google.common.collect.ImmutableMap;
import com.google.common.util.concurrent.MoreExecutors;
import org.apache.helix.HelixConstants;
import org.apache.helix.HelixRebalanceException;
import org.apache.helix.NotificationContext;
import org.apache.helix.common.ZkTestBase;
import org.apache.helix.constants.InstanceConstants;
import org.apache.helix.controller.dataproviders.ResourceControllerDataProvider;
import org.apache.helix.controller.rebalancer.util.WagedRebalanceUtil;
import org.apache.helix.controller.rebalancer.waged.constraints.ConstraintBasedAlgorithmFactory;
import org.apache.helix.controller.rebalancer.waged.model.ClusterModel;
import org.apache.helix.controller.rebalancer.waged.model.ClusterModelProvider;
import org.apache.helix.controller.stages.AttributeName;
import org.apache.helix.controller.stages.BestPossibleStateCalcStage;
import org.apache.helix.controller.stages.BestPossibleStateOutput;
import org.apache.helix.controller.stages.ClusterEvent;
import org.apache.helix.controller.stages.ClusterEventType;
import org.apache.helix.controller.stages.CurrentStateOutput;
import org.apache.helix.integration.manager.ClusterControllerManager;
import org.apache.helix.integration.manager.MockParticipantManager;
import org.apache.helix.manager.zk.ZKHelixDataAccessor;
import org.apache.helix.manager.zk.ZkBaseDataAccessor;
import org.apache.helix.model.BuiltInStateModelDefinitions;
import org.apache.helix.model.ClusterConfig;
import org.apache.helix.model.ExternalView;
import org.apache.helix.model.IdealState;
import org.apache.helix.model.InstanceConfig;
import org.apache.helix.model.LiveInstance;
import org.apache.helix.model.Message;
import org.apache.helix.model.Partition;
import org.apache.helix.model.Resource;
import org.apache.helix.model.ResourceAssignment;
import org.apache.helix.model.ResourceConfig;
import org.apache.helix.participant.statemachine.StateModel;
import org.apache.helix.participant.statemachine.StateModelFactory;
import org.apache.helix.participant.statemachine.StateModelInfo;
import org.apache.helix.participant.statemachine.Transition;
import org.testng.Assert;
import org.testng.annotations.Test;

public class TestWagedBestEffortPlacement extends ZkTestBase {
  private static final String RES = "TestResource";
  private static final String KEY = "PARTITION_COUNT";

  @StateModelInfo(initialState = "OFFLINE", states = {"OFFLINE", "ONLINE", "DROPPED", "ERROR"})
  public static class SingleSlotModel extends StateModel {
    private final String _part;
    private final Set<String> _held;
    private final AtomicInteger _violations;

    public SingleSlotModel(String p, Set<String> h, AtomicInteger v) { _part = p; _held = h; _violations = v; }

    @Transition(to = "ONLINE", from = "OFFLINE")
    public void onBecomeOnlineFromOffline(Message m, NotificationContext c) {
      synchronized (_held) {
        if (!_held.add(_part) || _held.size() == 1) { return; }
        _held.remove(_part);
        _violations.incrementAndGet();
        throw new IllegalStateException("Slot violation on " + _part);
      }
    }

    @Transition(to = "OFFLINE", from = "ONLINE")
    public void onBecomeOfflineFromOnline(Message m, NotificationContext c) throws Exception { Thread.sleep(40L); }

    @Transition(to = "DROPPED", from = "OFFLINE")
    public void onBecomeDroppedFromOffline(Message m, NotificationContext c) throws Exception {
      Thread.sleep(40L);
      synchronized (_held) { _held.remove(_part); }
    }
  }

  @Test
  public void testColdStartTrapZeroSpareRemovalAndSkewScaleUp() throws Exception {
    List<InstanceConfig> trapNodes = buildZoneNodes(new int[]{4, 2, 2, 2, 2, 2, 2, 2, 2, 1});
    try {
      calc(buildClusterData(trapNodes, 7, 3, 2, false, null, 1), null, null, null);
      Assert.fail("Default mode should fail on cold-start skewed fault-zone trap");
    } catch (HelixRebalanceException ex) {
      Assert.assertEquals(ex.getFailureCategory(), HelixRebalanceException.FailureCategory.NO_CANDIDATE_NODE);
    }
    ResourceAssignment full = calc(clusterData(trapNodes, 7, 3, 2), null, null, null);
    assertValidPlacement(full, trapNodes, 7, 21, 3);
    List<InstanceConfig> survivors = new ArrayList<>(trapNodes.subList(1, trapNodes.size()));
    ResourceAssignment reduced = calc(clusterData(survivors, 7, 3, 2), null, full, null);
    assertValidPlacement(reduced, survivors, 7, 20, 2);
    survivors.add(buildNodeConfig("zone11-node1", "zone11", 1));
    ResourceAssignment scaledUp = calc(clusterData(survivors, 7, 3, 2), null, reduced, null);
    assertValidPlacement(scaledUp, survivors, 7, 21, 3);
    reduced.getMappedPartitions().forEach(p -> Assert.assertTrue(
        scaledUp.getReplicaMap(p).keySet().containsAll(reduced.getReplicaMap(p).keySet())));
    List<InstanceConfig> skewedNodes = buildZoneNodes(new int[]{10, 2, 2, 1, 1, 1, 1, 1, 1, 1});
    assertValidPlacement(calc(clusterData(skewedNodes, 7, 3, 2), null, null, null), skewedNodes, 7, 18, 2);
    assertZoneEditTieBreakOutcome();
  }

  @Test
  public void testPostDelayReturnSpareNonEvictionAndCapacityCut() throws Exception {
    List<InstanceConfig> t2Configs = buildZoneNodes(new int[]{2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 1});
    ResourceControllerDataProvider fullCluster = clusterData(t2Configs, 7, 3, 2);
    ResourceAssignment baseline = calc(fullCluster, null, null, null);
    List<Partition> zone5Partitions = baseline.getMappedPartitions().stream()
        .filter(p -> baseline.getReplicaMap(p).containsKey("zone05-node1")
            || baseline.getReplicaMap(p).containsKey("zone05-node2"))
        .sorted(Comparator.comparing(Partition::getPartitionName)).collect(Collectors.toList());
    Partition partitionA = zone5Partitions.get(0);
    Partition partitionB = zone5Partitions.get(1);
    String hostA = baseline.getReplicaMap(partitionA).containsKey("zone05-node1") ? "zone05-node1" : "zone05-node2";
    String hostB = "zone05-node1".equals(hostA) ? "zone05-node2" : "zone05-node1";
    Set<String> degradedLiveSet = new LinkedHashSet<>(fullCluster.getAssignableInstances());
    degradedLiveSet.removeAll(Arrays.asList(hostA, hostB));
    ResourceAssignment degraded =
        calc(buildClusterData(t2Configs, 7, 3, 2, true, degradedLiveSet, 1), degradedLiveSet, baseline, baseline);
    Set<String> oneBack = new LinkedHashSet<>(degradedLiveSet);
    oneBack.add(hostB);
    Partition partition0 = new Partition(RES + "_0");
    ResourceAssignment spareBaseline = new ResourceAssignment(RES);
    spareBaseline.addReplicaMap(partition0,
        ImmutableMap.of("zone01-node1", "ONLINE", "zone02-node1", "ONLINE", "zone03-node1", "ONLINE"));
    ResourceAssignment spareBestPossible = new ResourceAssignment(RES);
    spareBestPossible.addReplicaMap(partition0, ImmutableMap.of("zone01-node1", "ONLINE", "zone04-node1", "ONLINE"));
    Set<String> fourLive =
        new HashSet<>(Arrays.asList("zone01-node1", "zone02-node1", "zone03-node1", "zone04-node1"));
    for (int evenness : new int[]{1, 0}) {
      ResourceAssignment restored = calc(buildClusterData(t2Configs, 7, 3, 2, true, null, evenness),
          fullCluster.getAssignableInstances(), baseline, degraded);
      Assert.assertEquals(restored.getReplicaMap(partitionA).get(hostA), "ONLINE");
      Assert.assertEquals(restored.getReplicaMap(partitionB).get(hostB), "ONLINE");
      ResourceAssignment afterOne =
          calc(buildClusterData(t2Configs, 7, 3, 2, true, oneBack, evenness), oneBack, baseline, degraded);
      Assert.assertEquals(afterOne.getReplicaMap(partitionB).get(hostB), "ONLINE");
      Assert.assertFalse(afterOne.getReplicaMap(partitionA).containsKey(hostB));
      Assert.assertEquals(calc(buildClusterData(buildZoneNodes(new int[]{1, 1, 1, 1}), 1, 3, 2, true, null, evenness),
          fourLive, spareBaseline, spareBestPossible).getReplicaMap(partition0).get("zone04-node1"), "ONLINE");
    }
    ResourceAssignment preCutBaseline = new ResourceAssignment(RES);
    preCutBaseline.addReplicaMap(partition0, Collections.singletonMap("zone02-node1", "ONLINE"));
    preCutBaseline.addReplicaMap(new Partition(RES + "_1"), Collections.singletonMap("zone02-node1", "ONLINE"));
    preCutBaseline.addReplicaMap(new Partition(RES + "_2"), Collections.singletonMap("zone01-node1", "ONLINE"));
    ResourceControllerDataProvider cutCluster = clusterData(buildZoneNodes(new int[]{1, 1}), 3, 1, 1);
    cutCluster.getIdealState(RES).setMaxPartitionsPerInstance(3);
    Assert.assertEquals(calc(cutCluster, null, preCutBaseline, null)
        .getReplicaMap(new Partition(RES + "_2")).get("zone01-node1"), "ONLINE");
  }

  @Test
  public void testMixedWeightPackingAndSoleCopyRetention() throws Exception {
    ResourceControllerDataProvider mixedData = clusterData(
        Arrays.asList(buildNodeConfig("zone01-a", "zone01", 3), buildNodeConfig("zone02-a", "zone02", 3)), 1, 2, 1);
    mixedData.setIdealStates(Arrays.asList(
        buildIdealState(RES, 1, 2, 1, 5, null), buildIdealState("Light", 2, 2, 1, 5, null)));
    ResourceConfig heavyConfig = new ResourceConfig(RES);
    heavyConfig.setPartitionCapacityMap(
        Collections.singletonMap(ResourceConfig.DEFAULT_PARTITION_KEY, Collections.singletonMap(KEY, 2)));
    mixedData.setResourceConfigMap(ImmutableMap.of(RES, heavyConfig, "Light", new ResourceConfig("Light")));
    Assert.assertEquals(calc(mixedData, null, null, null).getReplicaMap(new Partition(RES + "_0")).size(), 2);

    // Unconditional sole-copy retention keeps unplaceable TestResource_0 (weight 4, tagB) ONLINE on
    // zone01-a (cap 4, tagA), while WagedInstanceCapacity charges 4/4 and blocks PeerResource_0's bootstrap.
    ResourceConfig p0Weight4 = new ResourceConfig(RES);
    p0Weight4.setPartitionCapacityMap(ImmutableMap.of(ResourceConfig.DEFAULT_PARTITION_KEY,
        Collections.singletonMap(KEY, 1), RES + "_0", Collections.singletonMap(KEY, 4)));
    ResourceControllerDataProvider soleData = clusterData(Arrays.asList(
        buildNodeConfig("zone01-a", "zone01", 4, "tagA"), buildNodeConfig("zone02-a", "zone02", 1, "tagB")), 2, 1, 1);
    soleData.setIdealStates(Arrays.asList(
        buildIdealState(RES, 2, 1, 1, 4, "tagB"), buildIdealState("PeerResource", 1, 1, 1, 4, "tagA")));
    soleData.setResourceConfigMap(ImmutableMap.of(RES, p0Weight4, "PeerResource", new ResourceConfig("PeerResource")));
    Map<String, IdealState> soleOut = computeWithSoleCopy(soleData, "ONLINE");
    Assert.assertEquals(soleOut.get(RES).getInstanceStateMap(RES + "_0").get("zone01-a"), "ONLINE");
    Map<String, String> peerStates = soleOut.get("PeerResource").getInstanceStateMap("PeerResource_0");
    Assert.assertTrue(peerStates == null || !"ONLINE".equals(peerStates.get("zone01-a")));
    Assert.assertFalse("DROPPED".equals(
        computeWithSoleCopy(soleData, "OFFLINE").get(RES).getInstanceStateMap(RES + "_0").get("zone01-a")));
    soleData.getInstanceConfigMap().get("zone01-a").setInstanceOperation(InstanceConstants.InstanceOperation.EVACUATE);
    Assert.assertEquals(
        computeWithSoleCopy(soleData, "ONLINE").get(RES).getInstanceStateMap(RES + "_0").get("zone01-a"), "ONLINE");

    ResourceControllerDataProvider stageData =
        clusterData(Collections.singletonList(buildNodeConfig("zone01-a", "zone01", 1)), 2, 1, 1);
    stageData.setIdealStates(Collections.singletonList(buildIdealState(RES, 2, 1, 1, 0, null)));
    stageData.getClusterConfig().setGlobalRebalanceAsyncMode(false);
    CurrentStateOutput emptyCs = new CurrentStateOutput();
    WagedRebalancer stageRebalancer = newSyncRebalancer(stageData);
    try {
      ClusterEvent event = new ClusterEvent("TestCluster", ClusterEventType.Unknown);
      event.addAttribute(AttributeName.ControllerDataProvider.name(), stageData);
      event.addAttribute(AttributeName.RESOURCES_TO_REBALANCE.name(), attachCapacity(stageData, emptyCs));
      event.addAttribute(AttributeName.CURRENT_STATE_EXCLUDING_UNKNOWN.name(), emptyCs);
      event.addAttribute(AttributeName.STATEFUL_REBALANCER.name(), stageRebalancer);
      new BestPossibleStateCalcStage().process(event);
      Assert.assertFalse(event.<BestPossibleStateOutput>getAttribute(AttributeName.BEST_POSSIBLE_STATE.name())
          .getPreferenceLists(RES).isEmpty());
    } finally { stageRebalancer.close(); }
  }

  @Test
  public void testRelocationGuardsTwoPassAndScaleBound() throws Exception {
    Partition p0 = new Partition(RES + "_0");
    Partition p1 = new Partition(RES + "_1");
    List<InstanceConfig> emptyNodes = hostList("z1-n1", "z1-sp", "z2-n1", "z3-n1", "z4-dead");
    ResourceAssignment prior = assign("z2-n1:MASTER,z3-n1:SLAVE", "z1-n1:MASTER,z4-dead:SLAVE");
    Set<String> fourLive = setOf("z1-n1", "z1-sp", "z2-n1", "z3-n1");
    ResourceControllerDataProvider emptyCapData = buildClusterData(emptyNodes, 2, 2, 1, true, fourLive, 1);
    emptyCapData.getIdealState(RES).setStateModelDefRef("MasterSlave");
    emptyCapData.getClusterConfig().setInstanceCapacityKeys(Collections.emptyList());
    emptyCapData.getClusterConfig().setDefaultInstanceCapacityMap(Collections.emptyMap());
    emptyCapData.getClusterConfig().setDefaultPartitionWeightMap(Collections.emptyMap());
    emptyNodes.forEach(ic -> ic.setInstanceCapacityMap(Collections.emptyMap()));
    ResourceAssignment vacated = calculateEmergency(emptyCapData, fourLive, prior, null);
    Assert.assertEquals(vacated.getReplicaMap(p0).get("z2-n1"), "MASTER");
    Assert.assertEquals(vacated.getReplicaMap(p0).get("z1-sp"), "SLAVE");
    Assert.assertEquals(vacated.getReplicaMap(p1).get("z3-n1"), "SLAVE");

    List<InstanceConfig> weighted = hostList("z1-n1", "z1-sp", "z2-n1", "z3-n1", "z4-dead");
    CurrentStateOutput pendingCs = new CurrentStateOutput();
    Message dropMsg = new Message(Message.MessageType.STATE_TRANSITION, "msg-drop");
    dropMsg.setFromState("OFFLINE");
    dropMsg.setToState("DROPPED");
    pendingCs.setPendingMessage(RES, p1, "z1-sp", dropMsg);
    Assert.assertEquals(
        msEmerg(weighted, 1, fourLive, fourLive, prior, pendingCs).getReplicaMap(p0).get("z3-n1"), "SLAVE");
    Assert.assertEquals(
        msEmerg(weighted, 1, setOf("z1-n1", "z2-n1", "z3-n1"), fourLive, prior, null).getReplicaMap(p0).get("z3-n1"),
        "SLAVE");
    Assert.assertEquals(msEmerg(weighted, 2, fourLive, fourLive, prior, null).getReplicaMap(p0).get("z3-n1"), "SLAVE");

    Set<String> fiveLive = setOf("z1-n1", "z1-sp", "z2-n1", "z2-n2", "z3-n1");
    ResourceAssignment twoPassOut = calculateEmergency(buildClusterData(
        hostList("z1-n1", "z1-sp", "z2-n1", "z2-n2", "z3-n1", "z4-dead", "z5-dead"), 2, 3, 2, true, fiveLive, 1),
        fiveLive, assign("z1-n1:ONLINE,z2-n1:ONLINE,z4-dead:ONLINE", "z2-n2:ONLINE,z3-n1:ONLINE,z5-dead:ONLINE"), null);
    Assert.assertEquals(twoPassOut.getReplicaMap(p1).get("z1-sp"), "ONLINE");
    Assert.assertEquals(twoPassOut.getReplicaMap(p1).get("z3-n1"), "ONLINE");

    ResourceAssignment afterEmergency = calculateEmergency(buildClusterData(
        hostList("z1-n1", "z1-sp", "z2-n1", "z2-n2", "z3-off", "z4-n1", "z5-dead"), 2, 3, 2, true,
        setOf("z1-n1", "z1-sp", "z2-n1", "z2-n2", "z4-n1"), 1),
        setOf("z1-n1", "z1-sp", "z2-n1", "z2-n2", "z3-off", "z4-n1"),
        assign("z2-n1:ONLINE,z3-off:ONLINE,z4-n1:ONLINE", "z1-n1:ONLINE,z2-n2:ONLINE,z5-dead:ONLINE"), null);
    Assert.assertEquals(afterEmergency.getReplicaMap(p0).get("z3-off"), "ONLINE");
    Assert.assertFalse(afterEmergency.getReplicaMap(p1).containsKey("z3-off"));

    Set<String> overwriteLive = setOf("z1-n1", "z1-sp", "z2-n1", "z3-n1", "z4-n1");
    ResourceControllerDataProvider overwriteData = buildClusterData(
        hostList("z1-n1", "z1-sp", "z2-n1", "z3-n1", "z4-n1", "z5-off"), 2, 3, 2, true, overwriteLive, 1);
    overwriteData.getClusterConfig().setDelayRebalaceEnabled(true);
    overwriteData.getClusterConfig().setRebalanceDelayTime(3_600_000L);
    ClusterModel overwriteModel = ClusterModelProvider.generateClusterModelForDelayedRebalanceOverwrites(overwriteData,
        attachCapacity(overwriteData, new CurrentStateOutput()), overwriteLive, Collections.singletonMap(RES,
            assign("z2-n1:ONLINE,z3-n1:ONLINE,z4-n1:ONLINE", "z1-n1:ONLINE,z5-off:ONLINE")));
    ResourceAssignment overwriteOut = WagedRebalanceUtil.calculateAssignment(overwriteModel,
        ConstraintBasedAlgorithmFactory.getInstance(overwriteData.getClusterConfig().getGlobalRebalancePreference()),
        Collections.emptyMap()).get(RES);
    Assert.assertTrue(overwriteOut == null || !overwriteOut.getReplicaMap(p0).containsKey("z1-sp"));

    assertDeterministicTwoHopMoveZone();
    List<InstanceConfig> cliqueNodes = IntStream.range(0, 600).mapToObj(i -> buildNodeConfig(
        String.format("zone%02d-n%03d", (i % 20) + 1, i), String.format("zone%02d", (i % 20) + 1), 1,
        i < 100 ? "tagFull" : "tagSpare")).collect(Collectors.toList());
    ResourceControllerDataProvider cliqueData = clusterData(cliqueNodes, 100, 3, 2);
    cliqueData.getIdealState(RES).setInstanceGroupTag("tagFull");
    long startMs = System.currentTimeMillis();
    ResourceAssignment cliqueOut = calc(cliqueData, null, null, null);
    Assert.assertTrue(System.currentTimeMillis() - startMs < 30000L);
    Assert.assertEquals(cliqueOut.getMappedPartitions().stream().mapToInt(p -> cliqueOut.getReplicaMap(p).size()).sum(),
        100);
  }

  private static void assertDeterministicTwoHopMoveZone() throws Exception {
    for (boolean threeLevel : new boolean[]{false, true}) {
      Map<ClusterConfig.GlobalRebalancePreferenceKey, Integer> pref = ImmutableMap.of(
          ClusterConfig.GlobalRebalancePreferenceKey.EVENNESS, 1,
          ClusterConfig.GlobalRebalancePreferenceKey.LESS_MOVEMENT, 1,
          ClusterConfig.GlobalRebalancePreferenceKey.FORCE_BASELINE_CONVERGE, 0);
      Map<String, String> before = new LinkedHashMap<>(assign(
          "n0:zone01,n1:zone04,n2:zone02,n3:zone02,n4:zone03,n5:zone04").getReplicaMap(new Partition(RES + "_0")));
      Map<String, String> after = new LinkedHashMap<>(before);
      after.put("n1", "zone01");
      ResourceAssignment seed = assign("n0:ONLINE,n1:ONLINE,n2:ONLINE", "n3:ONLINE,n4:ONLINE,n5:ONLINE");
      CurrentStateOutput cs0 = new CurrentStateOutput();
      seed.getMappedPartitions()
          .forEach(p -> seed.getReplicaMap(p).forEach((h, s) -> cs0.setCurrentState(RES, p, h, s)));
      MockAssignmentMetadataStore store = new MockAssignmentMetadataStore();
      store.persistBaseline(Collections.singletonMap(RES, seed));
      store.persistBestPossibleAssignment(Collections.singletonMap(RES, seed));
      WagedRebalancer reb =
          new WagedRebalancer(store, ConstraintBasedAlgorithmFactory.getInstance(pref), Optional.empty());
      reb.setGlobalRebalanceAsyncMode(false);
      reb.setPartialRebalanceAsyncMode(false);
      CurrentStateOutput cur = cs0;
      try {
        for (int step = 0; step < 14; step++) {
          Map<String, String> activeZones = step == 0 ? before : after;
          List<InstanceConfig> nodes = activeZones.entrySet().stream().map(e -> {
            InstanceConfig ic = buildNodeConfig(e.getKey(), e.getValue(), 1);
            if (threeLevel) { ic.setDomain("zone=" + e.getValue() + ",host=" + e.getKey() + ",id=l-" + e.getKey()); }
            return ic;
          }).collect(Collectors.toList());
          ResourceControllerDataProvider dp = clusterData(nodes, 2, 3, 2);
          if (threeLevel) { dp.getClusterConfig().setTopology("/zone/host/id"); }
          dp.getClusterConfig().setGlobalRebalancePreference(pref);
          IdealState out = reb.computeNewIdealStates(dp, attachCapacity(dp, cur), cur).get(RES);
          CurrentStateOutput next = new CurrentStateOutput();
          Set<String> usedHosts = new HashSet<>();
          for (int i = 0; i < 2; i++) {
            Partition p = new Partition(RES + "_" + i);
            Map<String, String> desired = Optional.ofNullable(out.getInstanceStateMap(p.getPartitionName()))
                .orElse(Collections.emptyMap());
            Map<String, String> current = cur.getCurrentStateMap(RES, p);
            Set<String> hosts = new TreeSet<>(current.keySet());
            hosts.addAll(desired.keySet());
            Set<String> onlineZones = new HashSet<>();
            for (String h : hosts) {
              String cst = current.get(h);
              String des = desired.getOrDefault(h, "DROPPED");
              String nst = "ONLINE".equals(des) ? "ONLINE" : "OFFLINE".equals(des) ? (cst == null ? null : "OFFLINE")
                  : ("ONLINE".equals(cst) ? "OFFLINE" : null);
              if (nst != null) {
                next.setCurrentState(RES, p, h, nst);
                Assert.assertTrue(usedHosts.add(h) && (!"ONLINE".equals(nst) || step == 0
                    || onlineZones.add(activeZones.get(h))));
              }
            }
          }
          cur = next;
        }
        for (int i = 0; i < 2; i++) {
          Assert.assertEquals(cur.getCurrentStateMap(RES, new Partition(RES + "_" + i)).entrySet().stream()
              .filter(e -> "ONLINE".equals(e.getValue())).map(e -> after.get(e.getKey())).distinct().count(), 3L);
        }
      } finally { reb.close(); }
    }
  }

  @Test
  public void testLiveZkSlotGate() throws Exception {
    for (boolean useClusterMax : new boolean[]{true, false}) {
      String cluster = useClusterMax ? "BestEffortClusterMax" : "BestEffortResourceMax";
      _gSetupTool.addCluster(cluster, true);
      ZKHelixDataAccessor accessor = new ZKHelixDataAccessor(cluster, new ZkBaseDataAccessor<>(_gZkClient));
      ClusterControllerManager controller = new ClusterControllerManager(ZK_ADDR, cluster, "ctrl_0");
      Map<String, MockParticipantManager> participants = new LinkedHashMap<>();
      AtomicInteger violations = new AtomicInteger();
      try {
        ClusterConfig cfg = new ClusterConfig(cluster);
        cfg.setTopology("/zone/instance");
        cfg.setFaultZoneType("zone");
        cfg.setTopologyAwareEnabled(true);
        cfg.setWagedBestEffortPlacementEnabled(true);
        cfg.setMaxPartitionsPerInstance(useClusterMax ? 1 : -1);
        cfg.setInstanceCapacityKeys(useClusterMax ? Collections.singletonList(KEY) : Collections.emptyList());
        cfg.setDefaultInstanceCapacityMap(useClusterMax ? Collections.singletonMap(KEY, 1000) : Collections.emptyMap());
        cfg.setDefaultPartitionWeightMap(useClusterMax ? Collections.singletonMap(KEY, 1) : Collections.emptyMap());
        accessor.setProperty(accessor.keyBuilder().clusterConfig(), cfg);
        IntStream.rangeClosed(1, 4)
            .forEach(z -> addLiveNode(cluster, "z" + z + "-n1", "z" + z, useClusterMax, participants, violations));
        accessor.setProperty(accessor.keyBuilder().idealStates(RES),
            buildIdealState(RES, 2, 2, 1, useClusterMax ? 0 : 1, null));
        controller.syncStart();
        ExternalView pinned = waitForConvergedOnline(accessor, participants.keySet());
        Assert.assertNotNull(pinned);
        addLiveNode(cluster, "z1-sp", "z1", useClusterMax, participants, violations);
        String z1Part = pinned.getStateMap(RES + "_0").containsKey("z1-n1") ? RES + "_0" : RES + "_1";
        String peer = pinned.getStateMap(z1Part).keySet().stream().filter(h -> !h.startsWith("z1-")).findFirst().get();
        participants.remove(peer).syncStop();
        InstanceConfig droppedCfg = accessor.getProperty(accessor.keyBuilder().instanceConfig(peer));
        _gSetupTool.getClusterManagementTool().dropInstance(cluster, droppedCfg);
        Assert.assertNotNull(waitForConvergedOnline(accessor, participants.keySet()));
        Assert.assertNull(accessor.getProperty(accessor.keyBuilder().maintenance()));
        Assert.assertEquals(violations.get(), 0);
      } finally {
        if (controller.isConnected()) { controller.syncStop(); }
        participants.values().stream()
            .filter(MockParticipantManager::isConnected).forEach(MockParticipantManager::syncStop);
        accessor.getBaseDataAccessor().close();
        deleteCluster(cluster);
      }
    }
  }

  private void addLiveNode(String cluster, String name, String zone, boolean useClusterMax,
      Map<String, MockParticipantManager> participants, AtomicInteger violations) {
    InstanceConfig cfg = buildNodeConfig(name, zone, useClusterMax ? 1000 : 1);
    if (!useClusterMax) { cfg.setInstanceCapacityMap(Collections.emptyMap()); }
    _gSetupTool.getClusterManagementTool().addInstance(cluster, cfg);
    Set<String> held = ConcurrentHashMap.newKeySet();
    MockParticipantManager p = new MockParticipantManager(ZK_ADDR, cluster, name);
    p.getStateMachineEngine().registerStateModelFactory("OnlineOffline", new StateModelFactory<StateModel>() {
      @Override
      public StateModel createNewStateModel(String r, String k) { return new SingleSlotModel(k, held, violations); }
    });
    p.syncStart();
    participants.put(name, p);
  }

  private static ExternalView waitForConvergedOnline(ZKHelixDataAccessor accessor, Set<String> liveHosts)
      throws InterruptedException {
    for (long end = System.currentTimeMillis() + 15_000L; System.currentTimeMillis() < end; Thread.sleep(100L)) {
      ExternalView view = accessor.getProperty(accessor.keyBuilder().externalView(RES));
      if (view == null) { continue; }
      boolean ok = true;
      Set<String> hosts = new HashSet<>();
      for (int i = 0; i < 2; i++) {
        Map<String, String> raw = Optional.ofNullable(view.getStateMap(RES + "_" + i)).orElse(Collections.emptyMap());
        Set<String> zones = new HashSet<>();
        for (Map.Entry<String, String> e : raw.entrySet()) {
          InstanceConfig ic = accessor.getProperty(accessor.keyBuilder().instanceConfig(e.getKey()));
          ok &= "ONLINE".equals(e.getValue())
              ? (liveHosts.contains(e.getKey()) && ic != null && hosts.add(e.getKey())
                  && zones.add(ic.getDomainAsMap().get("zone")))
              : "DROPPED".equals(e.getValue());
        }
        ok &= zones.size() == 2;
      }
      if (ok && hosts.size() == 4) { return view; }
    }
    return null;
  }

  private static Set<String> setOf(String... xs) { return new HashSet<>(Arrays.asList(xs)); }

  private static List<InstanceConfig> hostList(String... names) {
    return Arrays.stream(names).map(n -> buildNodeConfig(n, n.substring(0, n.indexOf('-')), 1))
        .collect(Collectors.toList());
  }

  private static ResourceAssignment assign(String... partSpecs) {
    ResourceAssignment ra = new ResourceAssignment(RES);
    IntStream.range(0, partSpecs.length).forEach(i -> ra.addReplicaMap(new Partition(RES + "_" + i),
        Arrays.stream(partSpecs[i].split(",")).collect(Collectors.toMap(
            s -> s.substring(0, s.indexOf(':')), s -> s.substring(s.indexOf(':') + 1)))));
    return ra;
  }

  private static ResourceAssignment msEmerg(List<InstanceConfig> nodes, int minActive, Set<String> liveSubset,
      Set<String> active, ResourceAssignment prior, CurrentStateOutput cs) throws HelixRebalanceException {
    ResourceControllerDataProvider dp = buildClusterData(nodes, 2, 2, minActive, true, liveSubset, 1);
    dp.getIdealState(RES).setStateModelDefRef("MasterSlave");
    return calculateEmergency(dp, active, prior, cs);
  }

  private static ResourceAssignment calculateEmergency(ResourceControllerDataProvider dp, Set<String> active,
      ResourceAssignment priorBestPossible, CurrentStateOutput cs) throws HelixRebalanceException {
    CurrentStateOutput cur = cs == null ? new CurrentStateOutput() : cs;
    MockAssignmentMetadataStore store = new MockAssignmentMetadataStore();
    store.persistBestPossibleAssignment(Collections.singletonMap(RES, priorBestPossible));
    RebalanceAlgorithm alg =
        ConstraintBasedAlgorithmFactory.getInstance(dp.getClusterConfig().getGlobalRebalancePreference());
    WagedRebalancer reb = new WagedRebalancer(store, alg, Optional.empty());
    reb.setPartialRebalanceAsyncMode(true);
    try {
      return reb.emergencyRebalance(dp, attachCapacity(dp, cur), active, cur, alg).get(RES);
    } finally {
      reb.close();
    }
  }

  private static void assertZoneEditTieBreakOutcome() throws Exception {
    // Row 6 pin: [2, 2, 2] with PARTITION_COUNT=1 only, two-hop drops, mz01-h1: mz01 -> mz02 -> 6/6 ONLINE.
    Map<String, String> zones = new LinkedHashMap<>();
    IntStream.rangeClosed(1, 3).forEach(z -> IntStream.rangeClosed(1, 2)
        .forEach(h -> zones.put(String.format("mz%02d-h%d", z, h), String.format("mz%02d", z))));
    WagedRebalancer rebalancer = newSyncRebalancer(clusterData(Collections.emptyList(), 1, 1, 1));
    CurrentStateOutput currentStates = new CurrentStateOutput();
    try {
      for (int step = 0; step < 12; step++) {
        if (step == 2) { zones.put("mz01-h1", "mz02"); }
        List<InstanceConfig> nodes = zones.entrySet().stream()
            .map(e -> buildNodeConfig(e.getKey(), e.getValue(), 1)).collect(Collectors.toList());
        ResourceControllerDataProvider dp = clusterData(nodes, 3, 2, 2);
        dp.setIdealStates(Collections.singletonList(buildIdealState(RES, 3, 2, 2, 0, null)));
        IdealState desired =
            rebalancer.computeNewIdealStates(dp, attachCapacity(dp, currentStates), currentStates).get(RES);
        CurrentStateOutput next = new CurrentStateOutput();
        for (int i = 0; i < 3; i++) {
          Partition p = new Partition(RES + "_" + i);
          Map<String, String> targetMap = desired.getInstanceStateMap(p.getPartitionName());
          Map<String, String> current = currentStates.getCurrentStateMap(RES, p);
          Set<String> hosts = new TreeSet<>(current.keySet());
          if (targetMap != null) { hosts.addAll(targetMap.keySet()); }
          for (String host : hosts) {
            String cur = current.get(host);
            String target = targetMap == null ? "DROPPED" : targetMap.getOrDefault(host, "DROPPED");
            String nextState = "ONLINE".equals(target) ? "ONLINE"
                : "OFFLINE".equals(target) ? (cur == null ? null : "OFFLINE")
                : ("ONLINE".equals(cur) ? "OFFLINE" : null);
            if (nextState != null) { next.setCurrentState(RES, p, host, nextState); }
          }
        }
        currentStates = next;
      }
      for (int i = 0; i < 3; i++) {
        Assert.assertEquals(currentStates.getCurrentStateMap(RES, new Partition(RES + "_" + i)).entrySet().stream()
            .filter(e -> "ONLINE".equals(e.getValue())).map(e -> zones.get(e.getKey())).distinct().count(), 2L);
      }
    } finally { rebalancer.close(); }
  }

  private static ResourceAssignment calc(ResourceControllerDataProvider dp, Set<String> active,
      ResourceAssignment baseline, ResourceAssignment bestPossible) throws HelixRebalanceException {
    Map<String, Resource> resourceMap = attachCapacity(dp, new CurrentStateOutput());
    Map<String, ResourceAssignment> baseMap =
        baseline == null ? Collections.emptyMap() : Collections.singletonMap(RES, baseline);
    Map<String, ResourceAssignment> priorMap =
        bestPossible == null ? baseMap : Collections.singletonMap(RES, bestPossible);
    ClusterModel model = bestPossible == null
        ? ClusterModelProvider.generateClusterModelForBaseline(dp, resourceMap, dp.getAssignableInstances(),
            Collections.singletonMap(HelixConstants.ChangeType.CLUSTER_CONFIG, Collections.emptySet()), baseMap)
        : ClusterModelProvider.generateClusterModelForPartialRebalance(dp, resourceMap, active, baseMap, priorMap);
    return WagedRebalanceUtil.calculateAssignment(model, ConstraintBasedAlgorithmFactory.getInstance(
        dp.getClusterConfig().getGlobalRebalancePreference()), priorMap).get(RES);
  }

  private static void assertValidPlacement(ResourceAssignment assignment, List<InstanceConfig> nodeConfigs,
      int partitionCount, int expectedTotalReplicas, int minPerPartition) {
    Map<String, String> zoneByHost = nodeConfigs.stream()
        .collect(Collectors.toMap(InstanceConfig::getInstanceName, c -> c.getDomainAsMap().get("zone")));
    int totalReplicas = 0;
    Set<String> usedHosts = new HashSet<>();
    for (int i = 0; i < partitionCount; i++) {
      Map<String, String> stateMap = assignment.getReplicaMap(new Partition(RES + "_" + i));
      Assert.assertTrue(stateMap.size() >= minPerPartition);
      Set<String> occupiedZones = new HashSet<>();
      for (String hostName : stateMap.keySet()) {
        totalReplicas++;
        Assert.assertTrue(usedHosts.add(hostName) && occupiedZones.add(zoneByHost.get(hostName)));
      }
    }
    Assert.assertEquals(totalReplicas, expectedTotalReplicas);
  }

  private static Map<String, Resource> attachCapacity(ResourceControllerDataProvider dp, CurrentStateOutput cs) {
    Map<String, Resource> resourceMap = new HashMap<>();
    dp.getIdealStates().values().forEach(is -> {
      Resource resource = new Resource(is.getResourceName());
      is.getPartitionSet().forEach(resource::addPartition);
      resource.setStateModelDefRef(is.getStateModelDefRef());
      resourceMap.put(is.getResourceName(), resource);
    });
    WagedResourceWeightsProvider weights = new WagedResourceWeightsProvider(dp);
    WagedInstanceCapacity capacity = new WagedInstanceCapacity(dp);
    capacity.process(dp, cs, resourceMap, weights);
    dp.setWagedCapacityProviders(capacity, weights);
    return resourceMap;
  }

  private static WagedRebalancer newSyncRebalancer(ResourceControllerDataProvider dp) {
    WagedRebalancer rebalancer = new WagedRebalancer(new MockAssignmentMetadataStore(),
        ConstraintBasedAlgorithmFactory.getInstance(dp.getClusterConfig().getGlobalRebalancePreference()),
        Optional.empty());
    rebalancer.setGlobalRebalanceAsyncMode(false);
    rebalancer.setPartialRebalanceAsyncMode(false);
    return rebalancer;
  }

  private static Map<String, IdealState> computeWithSoleCopy(
      ResourceControllerDataProvider dp, String currentState) throws HelixRebalanceException {
    CurrentStateOutput cs = new CurrentStateOutput();
    cs.setCurrentState(RES, new Partition(RES + "_0"), "zone01-a", currentState);
    WagedRebalancer rebalancer = newSyncRebalancer(dp);
    try {
      rebalancer.computeNewIdealStates(dp, attachCapacity(dp, cs), cs);
      return rebalancer.computeNewIdealStates(dp, attachCapacity(dp, cs), cs);
    } finally { rebalancer.close(); }
  }

  private static IdealState buildIdealState(String name, int parts, int rf, int minActive, int maxPerNode, String tag) {
    IdealState is = new IdealState(name);
    is.setRebalanceMode(IdealState.RebalanceMode.FULL_AUTO);
    is.setRebalancerClassName(WagedRebalancer.class.getName());
    is.setReplicas(String.valueOf(rf));
    is.setMinActiveReplicas(minActive);
    is.setNumPartitions(parts);
    is.setStateModelDefRef("OnlineOffline");
    if (maxPerNode > 0) { is.setMaxPartitionsPerInstance(maxPerNode); }
    if (tag != null) { is.setInstanceGroupTag(tag); }
    IntStream.range(0, parts).forEach(i -> is.setPreferenceList(name + "_" + i, Collections.emptyList()));
    return is;
  }

  private static InstanceConfig buildNodeConfig(String name, String zone, int capacity, String... tags) {
    InstanceConfig cfg = new InstanceConfig(name);
    cfg.setDomain("zone=" + zone + ",instance=" + name);
    cfg.setInstanceEnabled(true);
    cfg.setInstanceCapacityMap(Collections.singletonMap(KEY, capacity));
    Arrays.asList(tags).forEach(cfg::addTag);
    return cfg;
  }

  private static List<InstanceConfig> buildZoneNodes(int[] zoneSizes) {
    List<InstanceConfig> configs = new ArrayList<>();
    IntStream.range(0, zoneSizes.length).forEach(z -> IntStream.rangeClosed(1, zoneSizes[z]).forEach(h ->
        configs.add(buildNodeConfig(String.format("zone%02d-node%d", z + 1, h), String.format("zone%02d", z + 1), 1))));
    return configs;
  }

  private static ResourceControllerDataProvider clusterData(List<InstanceConfig> nodes, int parts, int rf, int minAct) {
    return buildClusterData(nodes, parts, rf, minAct, true, null, 1);
  }

  private static ResourceControllerDataProvider buildClusterData(List<InstanceConfig> nodeConfigs, int parts, int rf,
      int minActive, boolean bestEffort, Set<String> liveSubset, int evennessWeight) {
    ResourceControllerDataProvider dp = new ResourceControllerDataProvider("TestCluster") {
      @Override public Map<String, Message> getMessages(String instance) { return Collections.emptyMap(); }

      @Override
      public Set<HelixConstants.ChangeType> getRefreshedChangeTypes() {
        return EnumSet.of(HelixConstants.ChangeType.CLUSTER_CONFIG, HelixConstants.ChangeType.INSTANCE_CONFIG,
            HelixConstants.ChangeType.IDEAL_STATE, HelixConstants.ChangeType.RESOURCE_CONFIG,
            HelixConstants.ChangeType.LIVE_INSTANCE);
      }
    };
    dp.setAsyncTasksThreadPool(MoreExecutors.newDirectExecutorService());
    ClusterConfig config = new ClusterConfig("TestCluster");
    config.setTopology("/zone/instance");
    config.setFaultZoneType("zone");
    config.setTopologyAwareEnabled(true);
    config.setInstanceCapacityKeys(Collections.singletonList(KEY));
    config.setDefaultInstanceCapacityMap(Collections.singletonMap(KEY, 1));
    config.setDefaultPartitionWeightMap(Collections.singletonMap(KEY, 1));
    config.setGlobalRebalancePreference(ImmutableMap.of(ClusterConfig.GlobalRebalancePreferenceKey.EVENNESS,
        evennessWeight, ClusterConfig.GlobalRebalancePreferenceKey.LESS_MOVEMENT, 16));
    config.setWagedBestEffortPlacementEnabled(bestEffort);
    dp.setClusterConfig(config);
    Map<String, InstanceConfig> instanceConfigMap = new LinkedHashMap<>();
    List<LiveInstance> liveInstances = new ArrayList<>();
    nodeConfigs.forEach(c -> {
      instanceConfigMap.put(c.getInstanceName(), c);
      if (liveSubset == null || liveSubset.contains(c.getInstanceName())) {
        LiveInstance live = new LiveInstance(c.getInstanceName());
        live.setSessionId("session_" + c.getInstanceName());
        liveInstances.add(live);
      }
    });
    dp.setInstanceConfigMap(instanceConfigMap);
    dp.setLiveInstances(liveInstances);
    dp.setIdealStates(Collections.singletonList(buildIdealState(RES, parts, rf, minActive, 1, null)));
    dp.setResourceConfigMap(Collections.singletonMap(RES, new ResourceConfig(RES)));
    BuiltInStateModelDefinitions onlineOffline = BuiltInStateModelDefinitions.OnlineOffline;
    BuiltInStateModelDefinitions masterSlave = BuiltInStateModelDefinitions.MasterSlave;
    dp.setStateModelDefMap(ImmutableMap.of("OnlineOffline", onlineOffline.getStateModelDefinition(),
        "MasterSlave", masterSlave.getStateModelDefinition()));
    return dp;
  }
}
