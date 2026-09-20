package org.apache.helix.controller.rebalancer.util;

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
import java.util.EnumSet;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.helix.BucketDataAccessor;
import org.apache.helix.HelixConstants;
import org.apache.helix.controller.dataproviders.ResourceControllerDataProvider;
import org.apache.helix.controller.rebalancer.waged.AssignmentMetadataStore;
import org.apache.helix.controller.rebalancer.waged.WagedRebalancer;
import org.apache.helix.controller.rebalancer.waged.constraints.ConstraintBasedAlgorithmFactory;
import org.apache.helix.controller.stages.CurrentStateOutput;
import org.apache.helix.model.BuiltInStateModelDefinitions;
import org.apache.helix.model.ClusterConfig;
import org.apache.helix.model.IdealState;
import org.apache.helix.model.InstanceConfig;
import org.apache.helix.model.LiveInstance;
import org.apache.helix.model.Partition;
import org.apache.helix.model.Resource;
import org.apache.helix.model.ResourceAssignment;
import org.apache.helix.model.ResourceConfig;
import org.apache.helix.zookeeper.zkclient.exception.ZkNoNodeException;
import org.mockito.Mockito;
import org.testng.Assert;
import org.testng.annotations.Test;

/**
 * Drives the delayed rebalance overwrite phase through
 * {@code WagedRebalancer.computeNewIdealStates}, with delayed rebalance on and nodes offline inside
 * the delay window, so the phase has to top up the partitions that fell below their minimum active
 * replicas. Each resource has two replicas and no minimum active replicas of its own, so every
 * partition needs both replicas on live nodes.
 *
 * <p>Every test compares the preference lists the rebalancer returns with the best possible
 * assignment it persisted, read back from the metadata store after the run. The overwrite phase
 * merges its result into that assignment without persisting it, so the two differ by exactly the
 * top-ups the phase added.
 */
public class TestWagedIsolationDelayedOverwriteMerge {
  private static final String DISK = "DISK";
  private static final String MASTER_SLAVE = BuiltInStateModelDefinitions.MasterSlave.name();
  private static final EnumSet<HelixConstants.ChangeType> ALL_CHANGES = EnumSet.of(
      HelixConstants.ChangeType.CLUSTER_CONFIG, HelixConstants.ChangeType.INSTANCE_CONFIG,
      HelixConstants.ChangeType.IDEAL_STATE, HelixConstants.ChangeType.RESOURCE_CONFIG,
      HelixConstants.ChangeType.LIVE_INSTANCE);
  private static final AtomicInteger SEQUENCE = new AtomicInteger();

  /**
   * Clique A (a0 to a2) and clique B (b0, b1) each serve one resource of four partitions, and a2
   * and b1 go offline inside the delay window. B's only live node already hosts every B partition,
   * so B cannot be topped up. With isolation on, the returned B is exactly the persisted B, and
   * every A partition is its persisted placement plus both live A nodes. With isolation off, the
   * failure on B fails the calculation and the rebalancer returns the persisted assignment for
   * both cliques, so A is not topped up either.
   */
  @Test
  public void testSkippedCliqueIsLeftUntouchedWhileTheOthersAreToppedUp() throws Exception {
    for (boolean isolation : new boolean[] {true, false}) {
      try (Pipeline pipeline = new Pipeline(isolation)) {
        pipeline.clique("A", "a0", "a1", "a2").clique("B", "b0", "b1");
        pipeline.resource("RA", "A", 4).resource("RB", "B", 4);
        pipeline.run();
        pipeline.offline("a2", "b1");
        Map<String, Map<String, Set<String>>> returned = pipeline.run();
        Map<String, Map<String, Set<String>>> persisted = pipeline.persisted();

        Map<String, Set<String>> toppedUpA = toppedUp(persisted.get("RA"), "a0", "a1");
        Assert.assertFalse(toppedUpA.equals(persisted.get("RA")),
            "Setup: some A partition must be below its minimum active replicas");
        if (isolation) {
          Assert.assertEquals(returned.get("RB"), persisted.get("RB"),
              "Isolation skips B, so B must keep exactly its persisted assignment");
          Assert.assertEquals(returned.get("RA"), toppedUpA,
              "A must still be topped up onto its live nodes while B is skipped");
        } else {
          Assert.assertEquals(returned, persisted,
              "With isolation off, B fails the calculation and both cliques fall back to the "
                  + "persisted assignment");
        }
      }
    }
  }

  /**
   * Clique B has a third node, so with a2 and b1 offline every partition of both cliques can be
   * topped up. Each clique has a partition below its minimum active replicas, and in both modes
   * every returned partition is its persisted placement plus both live nodes of its clique, so
   * isolation skips nothing. The two modes return the same preference lists.
   */
  @Test
  public void testNothingSkippedLeavesTheOverwritePhaseUnchanged() throws Exception {
    Map<Boolean, Map<String, Map<String, Set<String>>>> returnedByMode = new HashMap<>();
    for (boolean isolation : new boolean[] {true, false}) {
      try (Pipeline pipeline = new Pipeline(isolation)) {
        pipeline.clique("A", "a0", "a1", "a2").clique("B", "b0", "b1", "b2");
        pipeline.resource("RA", "A", 4).resource("RB", "B", 4);
        pipeline.run();
        pipeline.offline("a2", "b1");
        Map<String, Map<String, Set<String>>> returned = pipeline.run();
        Map<String, Map<String, Set<String>>> persisted = pipeline.persisted();

        Map<String, Map<String, Set<String>>> expected = new TreeMap<>();
        expected.put("RA", toppedUp(persisted.get("RA"), "a0", "a1"));
        expected.put("RB", toppedUp(persisted.get("RB"), "b0", "b2"));
        for (String resource : expected.keySet()) {
          Assert.assertFalse(expected.get(resource).equals(persisted.get(resource)),
              "Setup: some " + resource + " partition must be below its minimum active replicas");
        }
        Assert.assertEquals(returned, expected,
            "Every partition must be topped up onto the live nodes of its clique, isolation "
                + isolation);
        returnedByMode.put(isolation, returned);
      }
    }
    Assert.assertEquals(returnedByMode.get(true), returnedByMode.get(false),
        "With nothing skipped, isolation must not change the overwrite phase's result");
  }

  /**
   * Clique A has a0 and a2 and serves two partitions, so both partitions sit on a0 and a2. Clique B
   * has b0 and b1, which have room for two replicas each, and x, so B's six replicas need x. Then
   * x is retagged into A while it still hosts B's replicas, and a2 and b1 go offline inside the
   * delay window. x is the only live A node the A partitions are missing, and B cannot be topped
   * up, so isolation skips B. The returned B is exactly the persisted B, which still names x, and
   * every A partition is topped up onto x.
   *
   * <p>The last assertion fails if the overwrite phase hands {@code calculateAssignment} the
   * persisted assignment as its fallback instead of none. B's persisted entry is then carried
   * forward and names x, the carried over collision check finds x in A's freshly calculated entry,
   * and A gives up its top-up.
   */
  @Test
  public void testTopUpOntoANodeStillHostingTheSkippedCliqueIsKept() throws Exception {
    try (Pipeline pipeline = new Pipeline(true)) {
      pipeline.clique("A", "a0", "a2");
      pipeline.node("b0", "B", 20).node("b1", "B", 20).node("x", "B", 100);
      pipeline.resource("RA", "A", 2).resource("RB", "B", 3);
      pipeline.run();
      pipeline.retag("x", "A");
      pipeline.offline("a2", "b1");
      Map<String, Map<String, Set<String>>> returned = pipeline.run();
      Map<String, Map<String, Set<String>>> persisted = pipeline.persisted();

      Map<String, Set<String>> onA0AndA2 = new TreeMap<>();
      onA0AndA2.put("RA_0", new TreeSet<>(Arrays.asList("a0", "a2")));
      onA0AndA2.put("RA_1", new TreeSet<>(Arrays.asList("a0", "a2")));
      Assert.assertEquals(persisted.get("RA"), onA0AndA2,
          "Setup: both A partitions must be persisted on a0 and a2");
      Assert.assertTrue(instancesOf(persisted.get("RB")).contains("x"),
          "Setup: the persisted B must still name x after the retag");
      Assert.assertEquals(returned.get("RB"), persisted.get("RB"),
          "Isolation skips B, so B must keep exactly its persisted assignment");
      Assert.assertEquals(returned.get("RA"), toppedUp(persisted.get("RA"), "a0", "x"),
          "A must be topped up onto x even though the skipped B still names x");
    }
  }

  /** The given placements, with every partition also placed on each of the given live nodes. */
  private static Map<String, Set<String>> toppedUp(Map<String, Set<String>> placements,
      String... liveNodes) {
    Map<String, Set<String>> toppedUp = new TreeMap<>();
    placements.forEach((partition, instances) -> {
      Set<String> placement = new TreeSet<>(instances);
      placement.addAll(Arrays.asList(liveNodes));
      toppedUp.put(partition, placement);
    });
    return toppedUp;
  }

  /** Every instance the placements name, and none when the resource is missing. */
  private static Set<String> instancesOf(Map<String, Set<String>> placements) {
    Set<String> instances = new TreeSet<>();
    if (placements != null) {
      placements.values().forEach(instances::addAll);
    }
    return instances;
  }

  /**
   * Runs the real WAGED rebalancer the way the Helix controller does, with the global baseline and
   * the partial rebalance synchronous, delayed rebalance on and a one hour delay window. Every node
   * has capacity 100 unless given its own, and every replica weighs 10. Every run rebuilds the
   * cluster data and marks every change type refreshed, so the change detector compares
   * consecutive runs by content.
   */
  private static final class Pipeline implements AutoCloseable {
    private final String _cluster = "OverwriteMergeCluster" + SEQUENCE.incrementAndGet();
    private final boolean _isolation;
    private final Map<String, String> _nodeTags = new TreeMap<>();
    private final Map<String, Integer> _nodeCapacity = new HashMap<>();
    private final Map<String, Long> _offlineSince = new HashMap<>();
    private final Map<String, String> _resourceTags = new TreeMap<>();
    private final Map<String, Integer> _partitionCounts = new HashMap<>();
    private final OfflineTimeProvider _provider;
    private final AssignmentMetadataStore _store;
    private final WagedRebalancer _rebalancer;

    Pipeline(boolean isolation) {
      _isolation = isolation;
      _provider = new OfflineTimeProvider(_cluster);
      _provider.setStateModelDefMap(Collections.singletonMap(MASTER_SLAVE,
          BuiltInStateModelDefinitions.MasterSlave.getStateModelDefinition()));
      // Nothing is stored yet, so the store starts empty and from then on serves what it was last
      // given, from memory.
      BucketDataAccessor accessor = Mockito.mock(BucketDataAccessor.class);
      Mockito.when(accessor.compressedBucketRead(Mockito.anyString(), Mockito.any()))
          .thenThrow(new ZkNoNodeException("No assignment metadata"));
      _store = new AssignmentMetadataStore(accessor, _cluster) {
      };
      _rebalancer = new WagedRebalancer(_store,
          ConstraintBasedAlgorithmFactory.getInstance(Collections.emptyMap()), Optional.empty()) {
      };
      _rebalancer.setGlobalRebalanceAsyncMode(false);
      _rebalancer.setPartialRebalanceAsyncMode(false);
    }

    Pipeline clique(String tag, String... nodes) {
      for (String node : nodes) {
        _nodeTags.put(node, tag);
      }
      return this;
    }

    Pipeline node(String node, String tag, int capacity) {
      _nodeTags.put(node, tag);
      _nodeCapacity.put(node, capacity);
      return this;
    }

    Pipeline resource(String resource, String tag, int partitions) {
      _resourceTags.put(resource, tag);
      _partitionCounts.put(resource, partitions);
      return this;
    }

    void retag(String node, String tag) {
      _nodeTags.put(node, tag);
    }

    /** Takes the nodes offline at the current time, which is inside the delay window. */
    void offline(String... nodes) {
      long offlineTime = System.currentTimeMillis();
      for (String node : nodes) {
        _offlineSince.put(node, offlineTime);
      }
    }

    /** Runs the rebalancer and returns the preference lists it computed, as instance sets. */
    Map<String, Map<String, Set<String>>> run() throws Exception {
      ClusterConfig clusterConfig = new ClusterConfig(_cluster);
      clusterConfig.setInstanceCapacityKeys(Collections.singletonList(DISK));
      clusterConfig.setDefaultInstanceCapacityMap(Collections.singletonMap(DISK, 100));
      clusterConfig.setDefaultPartitionWeightMap(Collections.singletonMap(DISK, 10));
      clusterConfig.setWagedInstanceTagIsolationEnabled(_isolation);
      clusterConfig.setDelayRebalaceEnabled(true);
      clusterConfig.setRebalanceDelayTime(TimeUnit.HOURS.toMillis(1));
      _provider.setClusterConfig(clusterConfig);
      Map<String, InstanceConfig> instanceConfigs = new HashMap<>();
      List<LiveInstance> liveInstances = new ArrayList<>();
      for (Map.Entry<String, String> node : _nodeTags.entrySet()) {
        InstanceConfig instanceConfig = new InstanceConfig(node.getKey());
        instanceConfig.addTag(node.getValue());
        if (_nodeCapacity.containsKey(node.getKey())) {
          instanceConfig.setInstanceCapacityMap(
              Collections.singletonMap(DISK, _nodeCapacity.get(node.getKey())));
        }
        instanceConfigs.put(node.getKey(), instanceConfig);
        if (!_offlineSince.containsKey(node.getKey())) {
          LiveInstance liveInstance = new LiveInstance(node.getKey());
          liveInstance.setSessionId(node.getKey() + "_session");
          liveInstances.add(liveInstance);
        }
      }
      _provider.setInstanceConfigMap(instanceConfigs);
      _provider.setLiveInstances(liveInstances);
      _provider._offlineTimes = new HashMap<>(_offlineSince);
      List<IdealState> idealStates = new ArrayList<>();
      Map<String, ResourceConfig> resourceConfigs = new HashMap<>();
      Map<String, Resource> resources = new TreeMap<>();
      for (Map.Entry<String, String> entry : _resourceTags.entrySet()) {
        String name = entry.getKey();
        List<String> partitions = partitions(name);
        IdealState idealState = new IdealState(name);
        idealState.setRebalanceMode(IdealState.RebalanceMode.FULL_AUTO);
        idealState.setRebalancerClassName(WagedRebalancer.class.getName());
        idealState.setStateModelDefRef(MASTER_SLAVE);
        idealState.setReplicas("2");
        idealState.setNumPartitions(partitions.size());
        idealState.setInstanceGroupTag(entry.getValue());
        Resource resource = new Resource(name);
        resource.setStateModelDefRef(MASTER_SLAVE);
        for (String partition : partitions) {
          idealState.getRecord().setListField(partition, new ArrayList<>());
          resource.addPartition(partition);
        }
        ResourceConfig resourceConfig = new ResourceConfig(name);
        resourceConfig.setPartitionCapacityMap(Collections.singletonMap(
            ResourceConfig.DEFAULT_PARTITION_KEY, Collections.singletonMap(DISK, 10)));
        idealStates.add(idealState);
        resourceConfigs.put(name, resourceConfig);
        resources.put(name, resource);
      }
      _provider.setIdealStates(idealStates);
      _provider.setResourceConfigMap(resourceConfigs);
      _provider.getRefreshedChangeTypes().addAll(ALL_CHANGES);

      Map<String, Map<String, Set<String>>> returned = new TreeMap<>();
      for (IdealState idealState : _rebalancer
          .computeNewIdealStates(_provider, resources, new CurrentStateOutput()).values()) {
        Map<String, Set<String>> placements = new TreeMap<>();
        idealState.getPreferenceLists()
            .forEach((partition, instances) -> placements.put(partition, new TreeSet<>(instances)));
        returned.put(idealState.getResourceName(), placements);
      }
      return returned;
    }

    /** The best possible assignment the rebalancer persisted, as instance sets. */
    Map<String, Map<String, Set<String>>> persisted() {
      Map<String, Map<String, Set<String>>> persisted = new TreeMap<>();
      for (Map.Entry<String, ResourceAssignment> entry : _store.getBestPossibleAssignment()
          .entrySet()) {
        Map<String, Set<String>> placements = new TreeMap<>();
        for (Partition partition : entry.getValue().getMappedPartitions()) {
          placements.put(partition.getPartitionName(),
              new TreeSet<>(entry.getValue().getReplicaMap(partition).keySet()));
        }
        persisted.put(entry.getKey(), placements);
      }
      return persisted;
    }

    @Override
    public void close() {
      _rebalancer.close();
    }

    private List<String> partitions(String resource) {
      List<String> partitions = new ArrayList<>();
      for (int i = 0; i < _partitionCounts.get(resource); i++) {
        partitions.add(resource + "_" + i);
      }
      return partitions;
    }
  }

  /** Serves the offline times the scenario sets, as the Helix controller reads them from ZK. */
  private static final class OfflineTimeProvider extends ResourceControllerDataProvider {
    private volatile Map<String, Long> _offlineTimes = new HashMap<>();

    OfflineTimeProvider(String cluster) {
      super(cluster);
    }

    @Override
    public Map<String, Long> getInstanceOfflineTimeMap() {
      return _offlineTimes;
    }
  }
}
