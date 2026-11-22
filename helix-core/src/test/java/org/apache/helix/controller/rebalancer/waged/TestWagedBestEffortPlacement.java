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
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import com.google.common.collect.ImmutableMap;
import com.google.common.util.concurrent.MoreExecutors;
import org.apache.helix.HelixConstants;
import org.apache.helix.HelixRebalanceException;
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
import org.apache.helix.model.BuiltInStateModelDefinitions;
import org.apache.helix.model.ClusterConfig;
import org.apache.helix.model.IdealState;
import org.apache.helix.model.InstanceConfig;
import org.apache.helix.model.LiveInstance;
import org.apache.helix.model.Message;
import org.apache.helix.model.Partition;
import org.apache.helix.model.Resource;
import org.apache.helix.model.ResourceAssignment;
import org.apache.helix.model.ResourceConfig;
import org.testng.Assert;
import org.testng.annotations.Test;

public class TestWagedBestEffortPlacement {
  private static final String RES = "TestResource";
  private static final String KEY = "PARTITION_COUNT";

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
    dp.setStateModelDefMap(Collections.singletonMap("OnlineOffline", onlineOffline.getStateModelDefinition()));
    return dp;
  }
}
