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

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import com.google.common.collect.ImmutableMap;
import com.google.common.util.concurrent.MoreExecutors;
import org.apache.helix.*;
import org.apache.helix.common.ZkTestBase;
import org.apache.helix.constants.InstanceConstants;
import org.apache.helix.controller.changedetector.ResourceChangeDetector;
import org.apache.helix.controller.changedetector.trimmer.ClusterConfigTrimmer;
import org.apache.helix.controller.dataproviders.ResourceControllerDataProvider;
import org.apache.helix.controller.rebalancer.util.WagedRebalanceUtil;
import org.apache.helix.controller.rebalancer.waged.constraints.ConstraintBasedAlgorithmFactory;
import org.apache.helix.controller.rebalancer.waged.model.*;
import org.apache.helix.integration.manager.*;
import org.apache.helix.manager.zk.*;
import org.apache.helix.model.*;
import org.apache.helix.participant.statemachine.*;
import org.apache.helix.participant.statemachine.Transition;
import org.testng.Assert;
import org.testng.annotations.Test;

public class TestWagedBestEffortPlacement extends ZkTestBase {
  private static final String RES = "TestResource", CAP = "PARTITION_COUNT";

  @StateModelInfo(initialState = "OFFLINE", states = {"OFFLINE", "ONLINE", "DROPPED", "ERROR"})
  public static class SingleSlotModel extends StateModel {
    private final String _p; private final Set<String> _held; private final AtomicInteger _v; private final long _lag;
    public SingleSlotModel(String p, Set<String> held, AtomicInteger v, long lag) { _p = p; _held = held; _v = v; _lag = lag; }
    @Transition(to = "ONLINE", from = "OFFLINE") public void onOnline(Message m, NotificationContext ctx) {
      synchronized (_held) { if (!_held.isEmpty() && !_held.contains(_p)) { _v.incrementAndGet(); throw new IllegalStateException(); } _held.add(_p); }
    }
    @Transition(to = "OFFLINE", from = "ONLINE") public void onOffline(Message m, NotificationContext ctx) throws Exception { if (_lag > 0) Thread.sleep(_lag); }
    @Transition(to = "DROPPED", from = "OFFLINE") public void onDropped(Message m, NotificationContext ctx) throws Exception { if (_lag > 0) Thread.sleep(_lag); synchronized (_held) { _held.remove(_p); } }
  }

  private final class LiveCluster implements AutoCloseable {
    final String name; final ZKHelixDataAccessor acc; final ClusterControllerManager ctrl;
    final Map<String, MockParticipantManager> parts = new LinkedHashMap<>(); final AtomicInteger violations = new AtomicInteger(); final long lag;
    LiveCluster(String name, long delayMs, int maxPart, int defCap, int np, int rf, int minActive, boolean setResMax, List<InstanceConfig> nodes, long lag) {
      this.name = name; this.lag = lag; _gSetupTool.addCluster(name, true);
      acc = new ZKHelixDataAccessor(name, new ZkBaseDataAccessor<>(_gZkClient));
      acc.setProperty(acc.keyBuilder().clusterConfig(), makeCc(name, true, delayMs, defCap, maxPart));
      nodes.forEach(ic -> _gSetupTool.getClusterManagementTool().addInstance(name, ic));
      acc.setProperty(acc.keyBuilder().idealStates(RES), makeIs(np, rf, minActive, "OnlineOffline", setResMax ? 1 : -1));
      ctrl = new ClusterControllerManager(ZK_ADDR, name, "ctrl_0"); ctrl.syncStart(); nodes.forEach(ic -> addPart(ic.getInstanceName()));
    }
    void addPart(String inst) {
      Set<String> held = ConcurrentHashMap.newKeySet(); MockParticipantManager p = new MockParticipantManager(ZK_ADDR, name, inst);
      p.getStateMachineEngine().registerStateModelFactory("OnlineOffline", new StateModelFactory<StateModel>() {
        @Override public StateModel createNewStateModel(String r, String pk) { return new SingleSlotModel(pk, held, violations, lag); }
      });
      p.syncStart(); parts.put(inst, p);
    }
    void dropNode(String inst) {
      MockParticipantManager p = parts.remove(inst); if (p != null && p.isConnected()) p.syncStop();
      _gSetupTool.getClusterManagementTool().dropInstance(name, acc.getProperty(acc.keyBuilder().instanceConfig(inst)));
    }
    ExternalView pinSpareInZone01(int np, int expected, int minP, int maxP, String spare, int cap) throws Exception {
      ExternalView ev0 = waitForOnline(np, expected, minP, maxP, 15000L); Assert.assertNotNull(ev0); Set<String> busy = new HashSet<>();
      for (int i = 0; i < np; i++) busy.addAll(ev0.getStateMap(RES + "_" + i).keySet());
      _gSetupTool.getClusterManagementTool().addInstance(name, makeNode(spare, "zone01", cap)); addPart(spare);
      for (String n : new ArrayList<>(parts.keySet())) if (!busy.contains(n) && !spare.equals(n)) dropNode(n);
      ExternalView ev = waitForOnline(np, expected, minP, maxP, 15000L); Assert.assertNotNull(ev); return ev;
    }
    ExternalView waitForOnline(int np, int expected, int minP, int maxP, long timeoutMs) throws InterruptedException {
      for (long deadline = System.currentTimeMillis() + timeoutMs; System.currentTimeMillis() < deadline; Thread.sleep(100L)) {
        ExternalView ev = acc.getProperty(acc.keyBuilder().externalView(RES)); if (ev == null) continue;
        int online = 0, errors = 0; boolean ok = true; Map<String, Integer> hostCounts = new HashMap<>();
        for (int i = 0; i < np; i++) {
          Map<String, String> sm = ev.getStateMap(RES + "_" + i); Set<String> zones = new HashSet<>(); int pOn = 0;
          for (Map.Entry<String, String> e : (sm == null ? Collections.<String, String>emptyMap() : sm).entrySet()) {
            if ("ONLINE".equals(e.getValue())) {
              online++; pOn++; if (!parts.containsKey(e.getKey()) || hostCounts.merge(e.getKey(), 1, Integer::sum) > 1 || !zones.add(e.getKey().split("-")[0])) ok = false;
            } else if ("ERROR".equals(e.getValue())) errors++;
          }
          if (pOn < minP || pOn > maxP) ok = false;
        }
        if (errors == 0 && online == expected && ok) return ev;
      }
      return null;
    }
    @Override public void close() {
      if (ctrl.isConnected()) ctrl.syncStop(); parts.values().stream().filter(MockParticipantManager::isConnected).forEach(MockParticipantManager::syncStop);
      acc.getBaseDataAccessor().close(); deleteCluster(name);
    }
  }

  @Test
  public void testColdStartTrapZeroSpareRemovalAndSkewScaleUp() throws Exception {
    int[] trap = {4, 2, 2, 2, 2, 2, 2, 2, 2, 1};
    try {
      calcBaseline(buildData("def", makeNodes(trap), 7, 3, 2, "OnlineOffline", false, null), null);
      Assert.fail("Default mode should fail on cold-start skewed fault-zone trap");
    } catch (HelixRebalanceException ex) {
      Assert.assertEquals(ex.getFailureCategory(), HelixRebalanceException.FailureCategory.NO_CANDIDATE_NODE);
    }
    ResourceControllerDataProvider be = buildData("be", makeNodes(trap), 7, 3, 2, "OnlineOffline", true, null);
    ResourceAssignment full = calcBaseline(be, null); assertValid(full, be.getInstanceConfigMap(), 7, 21, 3);
    List<InstanceConfig> surv = new ArrayList<>(be.getInstanceConfigMap().values()); String removed = surv.remove(0).getInstanceName();
    ResourceControllerDataProvider redData = buildData("red", surv, 7, 3, 2, "OnlineOffline", true, null);
    ResourceAssignment red = calcBaseline(redData, full); assertValid(red, redData.getInstanceConfigMap(), 7, 20, 2);
    for (Partition p : red.getMappedPartitions()) Assert.assertFalse(red.getReplicaMap(p).containsKey(removed));
    ResourceControllerDataProvider skewData = buildData("skew", makeNodes(new int[]{10, 2, 2, 1, 1, 1, 1, 1, 1, 1}), 7, 3, 2, "OnlineOffline", true, null);
    ResourceAssignment initSkew = calcBaseline(skewData, null); assertValid(initSkew, skewData.getInstanceConfigMap(), 7, 18, 2);
    List<InstanceConfig> exp = new ArrayList<>(skewData.getInstanceConfigMap().values()); exp.add(makeNode("zone11-node1", "zone11", 1));
    ResourceControllerDataProvider upData = buildData("up", exp, 7, 3, 2, "OnlineOffline", true, null);
    ResourceAssignment afterAdd = calcBaseline(upData, initSkew); assertValid(afterAdd, upData.getInstanceConfigMap(), 7, 19, 2);
    for (Partition p : initSkew.getMappedPartitions()) Assert.assertTrue(afterAdd.getReplicaMap(p).keySet().containsAll(initSkew.getReplicaMap(p).keySet()));
  }

  @Test
  public void testRelocationCostLiveTargetCapacityCutAndDeletedPartition() throws Exception {
    List<InstanceConfig> nodes = Arrays.asList(makeNode("zone01-node1", "zone01", 1), makeNode("zone01-spare", "zone01", 1), makeNode("zone02-node1", "zone02", 1), makeNode("zone03-node1", "zone03", 1), makeNode("zone04-dead", "zone04", 1));
    ResourceAssignment prev = new ResourceAssignment(RES);
    prev.addReplicaMap(new Partition(RES + "_0"), ImmutableMap.of("zone02-node1", "MASTER", "zone03-node1", "SLAVE"));
    prev.addReplicaMap(new Partition(RES + "_1"), ImmutableMap.of("zone01-node1", "MASTER", "zone04-dead", "SLAVE"));
    Set<String> live4 = new HashSet<>(Arrays.asList("zone01-node1", "zone01-spare", "zone02-node1", "zone03-node1"));
    ResourceAssignment reloc = calcEmergency(buildData("msLive", nodes, 2, 2, 1, "MasterSlave", true, live4), live4, prev);
    Assert.assertEquals(reloc.getReplicaMap(new Partition(RES + "_0")).get("zone02-node1"), "MASTER");
    Assert.assertEquals(reloc.getReplicaMap(new Partition(RES + "_0")).get("zone01-spare"), "SLAVE");
    Assert.assertEquals(reloc.getReplicaMap(new Partition(RES + "_1")).get("zone03-node1"), "SLAVE");
    Set<String> live3 = new HashSet<>(Arrays.asList("zone01-node1", "zone02-node1", "zone03-node1"));
    Assert.assertEquals(calcEmergency(buildData("msOff", nodes, 2, 2, 1, "MasterSlave", true, live3), live4, prev).getReplicaMap(new Partition(RES + "_0")).get("zone03-node1"), "SLAVE");
    ResourceAssignment prevCut = new ResourceAssignment(RES); String[] cutHosts = {"zone02-n2", "zone02-n2", "zone01-n1", "zone01-n1"};
    for (int i = 0; i < 4; i++) prevCut.addReplicaMap(new Partition(RES + "_" + i), Collections.singletonMap(cutHosts[i], "ONLINE"));
    prevCut.addReplicaMap(new Partition(RES + "_gone"), Collections.singletonMap("zone03-off", "ONLINE"));
    List<InstanceConfig> cutNodes = Arrays.asList(makeNode("zone01-n1", "zone01", 2), makeNode("zone02-n2", "zone02", 1), makeNode("zone03-off", "zone03", 1));
    Set<String> active2 = new HashSet<>(Arrays.asList("zone01-n1", "zone02-n2"));
    ResourceControllerDataProvider cutData = buildData("cut", cutNodes, 4, 1, 1, "OnlineOffline", true, active2);
    cutData.getIdealState(RES).setMaxPartitionsPerInstance(4); Map<String, ResourceAssignment> mPrev = Collections.singletonMap(RES, prevCut);
    ResourceAssignment cutOut = calc(ClusterModelProvider.generateClusterModelForBaseline(cutData, resMap(cutData), active2, Collections.singletonMap(HelixConstants.ChangeType.INSTANCE_CONFIG, Collections.emptySet()), mPrev), cutData, mPrev);
    Assert.assertEquals(cutOut.getReplicaMap(new Partition(RES + "_2")).get("zone01-n1"), "ONLINE");
    Assert.assertEquals(cutOut.getReplicaMap(new Partition(RES + "_3")).get("zone01-n1"), "ONLINE");
    Assert.assertTrue(cutOut.getReplicaMap(new Partition(RES + "_gone")).isEmpty());
  }

  @Test
  public void testLiveDelayedOverwriteMaxPartitionsAndEvacuate() throws Exception {
    int[] layout = new int[20]; Arrays.fill(layout, 1); layout[0] = 3;
    try (LiveCluster lc = new LiveCluster("BeDelayOw", 3600_000L, -1, 1, 7, 3, 2, true, makeNodes(layout), 40L)) {
      ExternalView evReady = lc.pinSpareInZone01(7, 21, 3, 3, "zone01-node4", 1); String deg = findZone01Part(evReady, 7);
      for (String down : evReady.getStateMap(deg).keySet()) if (!down.startsWith("zone01-")) lc.parts.remove(down).syncStop();
      ExternalView evOw = lc.waitForOnline(7, 20, 2, 3, 20000L); Assert.assertNotNull(evOw);
      Assert.assertEquals(evOw.getStateMap(deg).values().stream().filter("ONLINE"::equals).count(), 2L); Assert.assertEquals(lc.violations.get(), 0);
    }
    List<InstanceConfig> five = new ArrayList<>(); for (int z = 1; z <= 5; z++) five.add(makeNode("zone0" + z + "-node1", "zone0" + z, 1000));
    try (LiveCluster lc = new LiveCluster("BeMaxPart", 0L, 1, 1000, 2, 2, 1, false, five, 40L)) {
      ExternalView evReady = lc.pinSpareInZone01(2, 4, 2, 2, "zone01-node2", 1000); String p = findZone01Part(evReady, 2);
      lc.dropNode(evReady.getStateMap(p).keySet().stream().filter(h -> !h.startsWith("zone01-")).findFirst().get());
      Assert.assertNotNull(lc.waitForOnline(2, 4, 2, 2, 20000L)); Assert.assertNull(lc.acc.getProperty(lc.acc.keyBuilder().maintenance())); Assert.assertEquals(lc.violations.get(), 0);
    }
    try (LiveCluster lc = new LiveCluster("BeEvac", 0L, -1, 1, 3, 1, 1, false, Arrays.asList(makeNode("zone01-node1", "zone01", 1), makeNode("zone02-node1", "zone02", 1)), 0L)) {
      Assert.assertNotNull(lc.waitForOnline(3, 2, 0, 1, 15000L));
      InstanceConfig evac = lc.acc.getProperty(lc.acc.keyBuilder().instanceConfig("zone01-node1")); evac.setInstanceOperation(InstanceConstants.InstanceOperation.EVACUATE);
      lc.acc.setProperty(lc.acc.keyBuilder().instanceConfig("zone01-node1"), evac); Thread.sleep(800L); Assert.assertNotNull(lc.waitForOnline(3, 2, 0, 1, 5000L));
    }
  }

  @Test
  public void testTrimmerAndLargeTagCliqueScale() throws Exception {
    ClusterConfig cfg = new ClusterConfig("trimTest"); ClusterConfig trimmedDefault = ClusterConfigTrimmer.getInstance().trimProperty(cfg);
    cfg.setWagedBestEffortPlacementEnabled(false); Assert.assertEquals(trimmedDefault.getRecord(), ClusterConfigTrimmer.getInstance().trimProperty(cfg).getRecord());
    ResourceControllerDataProvider dp = buildData("trimTest", Collections.emptyList(), 1, 1, 1, "OnlineOffline", false, null);
    ResourceChangeDetector detector = new ResourceChangeDetector(true); detector.updateSnapshots(dp);
    dp.getClusterConfig().setWagedBestEffortPlacementEnabled(true); detector.updateSnapshots(dp);
    Assert.assertEquals(detector.getChangesByType(HelixConstants.ChangeType.CLUSTER_CONFIG).size(), 1);
    List<InstanceConfig> scaleNodes = new ArrayList<>();
    for (int i = 0; i < 600; i++) { String z = String.format("zone%02d", (i % 20) + 1); InstanceConfig ic = makeNode(String.format("%s-node%03d", z, i), z, 1); ic.addTag(i < 100 ? "tagFull" : "tagSpare"); scaleNodes.add(ic); }
    ResourceControllerDataProvider scaleData = buildData("scaleBound", scaleNodes, 100, 3, 2, "OnlineOffline", true, null);
    scaleData.getIdealState(RES).setInstanceGroupTag("tagFull"); long startMs = System.currentTimeMillis();
    ResourceAssignment assignment = calcBaseline(scaleData, null); Assert.assertTrue(System.currentTimeMillis() - startMs < 5000L);
    Assert.assertEquals(assignment.getMappedPartitions().stream().mapToInt(p -> assignment.getReplicaMap(p).size()).sum(), 100);
  }

  private static String findZone01Part(ExternalView ev, int np) {
    for (int i = 0; i < np; i++) if (ev.getStateMap(RES + "_" + i).keySet().stream().anyMatch(h -> h.startsWith("zone01-"))) return RES + "_" + i;
    throw new AssertionError("No partition in zone01");
  }
  private static ResourceAssignment calcBaseline(ResourceControllerDataProvider d, ResourceAssignment cur) throws HelixRebalanceException {
    Map<String, ResourceAssignment> m = cur == null ? Collections.emptyMap() : Collections.singletonMap(RES, cur);
    return calc(ClusterModelProvider.generateClusterModelForBaseline(d, resMap(d), d.getAssignableInstances(),
        Collections.singletonMap(HelixConstants.ChangeType.CLUSTER_CONFIG, Collections.emptySet()), m), d, m);
  }
  private static ResourceAssignment calcEmergency(ResourceControllerDataProvider d, Set<String> active, ResourceAssignment prev) throws HelixRebalanceException {
    Map<String, ResourceAssignment> m = Collections.singletonMap(RES, prev);
    return calc(ClusterModelProvider.generateClusterModelForEmergencyRebalance(d, resMap(d), active, m), d, m);
  }
  private static ResourceAssignment calc(ClusterModel m, ResourceControllerDataProvider d, Map<String, ResourceAssignment> prev) throws HelixRebalanceException {
    return WagedRebalanceUtil.calculateAssignment(m, ConstraintBasedAlgorithmFactory.getInstance(d.getClusterConfig().getGlobalRebalancePreference()), prev).get(RES);
  }
  private static void assertValid(ResourceAssignment a, Map<String, InstanceConfig> icMap, int np, int totalExp, int minP) {
    int total = 0; Map<String, Integer> hostLoad = new HashMap<>();
    for (int i = 0; i < np; i++) {
      Map<String, String> sm = a.getReplicaMap(new Partition(RES + "_" + i)); Assert.assertTrue(sm.size() >= minP); Set<String> zones = new HashSet<>();
      for (String h : sm.keySet()) { total++; Assert.assertEquals(hostLoad.merge(h, 1, Integer::sum).intValue(), 1); Assert.assertTrue(zones.add(icMap.get(h).getDomainAsMap().get("zone"))); }
    }
    Assert.assertEquals(total, totalExp);
  }
  private static Map<String, Resource> resMap(ResourceControllerDataProvider d) {
    IdealState is = d.getIdealState(RES); Resource res = new Resource(RES);
    is.getPartitionSet().forEach(res::addPartition); res.setStateModelDefRef(is.getStateModelDefRef()); return Collections.singletonMap(RES, res);
  }
  private static ClusterConfig makeCc(String cluster, boolean bestEffort, long delayMs, int defCap, int maxPart) {
    ClusterConfig cc = new ClusterConfig(cluster); cc.setTopology("/zone/instance"); cc.setFaultZoneType("zone"); cc.setTopologyAwareEnabled(true);
    cc.setInstanceCapacityKeys(Collections.singletonList(CAP)); cc.setDefaultInstanceCapacityMap(Collections.singletonMap(CAP, defCap));
    cc.setDefaultPartitionWeightMap(Collections.singletonMap(CAP, 1)); if (maxPart > 0) cc.setMaxPartitionsPerInstance(maxPart);
    cc.setGlobalRebalancePreference(ImmutableMap.of(ClusterConfig.GlobalRebalancePreferenceKey.EVENNESS, 1, ClusterConfig.GlobalRebalancePreferenceKey.LESS_MOVEMENT, 16));
    cc.setPersistBestPossibleAssignment(true); cc.setWagedBestEffortPlacementEnabled(bestEffort);
    if (delayMs > 0) { cc.setDelayRebalaceEnabled(true); cc.setRebalanceDelayTime(delayMs); }
    return cc;
  }
  private static IdealState makeIs(int np, int rf, int minActive, String sm, int resMax) {
    IdealState is = new IdealState(RES); is.setRebalanceMode(IdealState.RebalanceMode.FULL_AUTO); is.setRebalancerClassName(WagedRebalancer.class.getName());
    is.setReplicas(String.valueOf(rf)); is.setMinActiveReplicas(minActive); is.setNumPartitions(np); is.setStateModelDefRef(sm);
    if (resMax > 0) is.setMaxPartitionsPerInstance(resMax);
    for (int i = 0; i < np; i++) is.getRecord().setListField(RES + "_" + i, new ArrayList<>());
    return is;
  }
  private static InstanceConfig makeNode(String name, String zone, int cap) {
    InstanceConfig ic = new InstanceConfig(name); ic.setHostName(name); ic.setPort("9000"); ic.setInstanceEnabled(true);
    ic.setDomain("zone=" + zone + ",instance=" + name); ic.setInstanceCapacityMap(Collections.singletonMap(CAP, cap)); return ic;
  }
  private static List<InstanceConfig> makeNodes(int[] layout) {
    List<InstanceConfig> list = new ArrayList<>();
    for (int z = 0; z < layout.length; z++) for (int h = 1; h <= layout[z]; h++) list.add(makeNode(String.format("zone%02d-node%d", z + 1, h), String.format("zone%02d", z + 1), 1));
    return list;
  }
  private static ResourceControllerDataProvider buildData(String cluster, List<InstanceConfig> nodes, int np, int rf, int minActive, String sm, boolean bestEffort, Set<String> liveSubset) {
    ResourceControllerDataProvider d = new ResourceControllerDataProvider(cluster); d.setAsyncTasksThreadPool(MoreExecutors.newDirectExecutorService());
    d.setClusterConfig(makeCc(cluster, bestEffort, 0L, 1, -1)); Map<String, InstanceConfig> icMap = new LinkedHashMap<>(); List<LiveInstance> lives = new ArrayList<>();
    for (InstanceConfig ic : nodes) {
      icMap.put(ic.getInstanceName(), ic);
      if (liveSubset == null || liveSubset.contains(ic.getInstanceName())) {
        LiveInstance li = new LiveInstance(ic.getInstanceName()); li.setSessionId("s_" + ic.getInstanceName()); li.setHelixVersion("1.0"); li.setLiveInstance(ic.getInstanceName()); lives.add(li);
      }
    }
    d.setInstanceConfigMap(icMap); d.setLiveInstances(lives); d.setIdealStates(Collections.singletonList(makeIs(np, rf, minActive, sm, 1)));
    d.setResourceConfigMap(Collections.singletonMap(RES, new ResourceConfig(RES)));
    d.setStateModelDefMap(ImmutableMap.of("OnlineOffline", BuiltInStateModelDefinitions.OnlineOffline.getStateModelDefinition(),
        "MasterSlave", BuiltInStateModelDefinitions.MasterSlave.getStateModelDefinition()));
    return d;
  }
}
