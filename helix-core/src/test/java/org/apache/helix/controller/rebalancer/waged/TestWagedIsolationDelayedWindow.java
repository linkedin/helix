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

import java.io.IOException;
import java.lang.reflect.Field;
import java.util.Arrays;
import java.util.Collections;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import org.apache.helix.BucketDataAccessor;
import org.apache.helix.HelixConstants;
import org.apache.helix.HelixProperty;
import org.apache.helix.HelixRebalanceException;
import org.apache.helix.constants.InstanceConstants;
import org.apache.helix.controller.dataproviders.ResourceControllerDataProvider;
import org.apache.helix.controller.rebalancer.util.DelayedRebalanceUtil;
import org.apache.helix.controller.rebalancer.waged.constraints.ConstraintBasedAlgorithmFactory;
import org.apache.helix.controller.rebalancer.waged.model.ClusterModel;
import org.apache.helix.controller.rebalancer.waged.model.OptimalAssignment;
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
import org.apache.helix.monitoring.mbeans.ClusterStatusMonitor;
import org.apache.helix.zookeeper.zkclient.exception.ZkNoNodeException;
import org.testng.Assert;
import org.testng.annotations.Test;

import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Drives the real WagedRebalancer through a delayed rebalance window with instance tag isolation:
 * the synchronous global baseline, the emergency rebalance, the delayed rebalance overwrite (the
 * min active top-up of handleDelayedRebalanceMinActiveReplica) and the asynchronous partial
 * rebalance, in the production order and with the real constraint based algorithm. The assignment
 * metadata store is the real one over a mocked bucket accessor, so its defensive copies are the
 * production ones.
 */
public class TestWagedIsolationDelayedWindow {
  private static final String CLUSTER = "DelayedWindowIsolation";
  private static final long DELAY_MS = TimeUnit.HOURS.toMillis(1);
  private static final int CAPACITY = 100;
  private static final int WEIGHT = 10;
  // No node can hold a single replica while the cluster wide sum still has room: a tag local break.
  private static final int TAG_LOCAL_BREAK = CAPACITY + 1;
  // Clique a alone then demands more than the whole cluster holds (4 x 1000 against 8 x 100), so
  // the tag blind capacity precheck fails in every scope before any placement.
  private static final int CLUSTER_WIDE_BREAK = 10 * CAPACITY;
  private static final String ONLINE = "ONLINE";
  private static final ClusterModel.RebalanceScopeType BASELINE =
      ClusterModel.RebalanceScopeType.GLOBAL_BASELINE;
  private static final ClusterModel.RebalanceScopeType EMERGENCY =
      ClusterModel.RebalanceScopeType.EMERGENCY;
  private static final ClusterModel.RebalanceScopeType PARTIAL =
      ClusterModel.RebalanceScopeType.PARTIAL;
  private static final ClusterModel.RebalanceScopeType OVERWRITE =
      ClusterModel.RebalanceScopeType.DELAYED_REBALANCE_OVERWRITES;

  /**
   * Clique a has no spare node, so its top-up is impossible for the whole window. It is left out of
   * the overwrite whole while clique b is topped up, nothing temporary reaches the store, and the
   * overwrite snapshot clears once clique a needs no top-up and again once nothing does.
   */
  @Test
  public void testCliqueWithoutASpareIsLeftOutOfTheOverwriteUntilItsNodeReturns()
      throws Exception {
    try (Fixture fixture = Fixture.threeCliques(true)) {
      View healthy = fixture.converge();
      String lostB = fixture.busiest("B");
      fixture.kill("a0");
      fixture.kill(lostB);

      View result = fixture.run();
      Assert.assertEquals(result.of("A"), healthy.of("A"));
      fixture.assertToppedUp(result, healthy, "B", lostB);
      Assert.assertEquals(result.of("C"), healthy.of("C"));
      fixture.assertReport(EMERGENCY, set(), set());
      fixture.assertReport(OVERWRITE, set("A", "B"), set("A"));
      Assert.assertTrue(fixture.estimate(OVERWRITE) >= 0,
          "Clique a must fail on its own nodes, clear of the cluster wide capacity precheck");
      fixture.assertGauges(1, false);
      Assert.assertEquals(fixture.stored(), healthy, "A top-up must never reach the store");

      fixture.revive("a0");
      result = fixture.run();
      Assert.assertEquals(result.of("A"), healthy.of("A"));
      fixture.assertToppedUp(result, healthy, "B", lostB);
      fixture.assertReport(OVERWRITE, set("B"), set());
      fixture.assertGauges(0, false);

      fixture.revive(lostB);
      result = fixture.run();
      Assert.assertEquals(result, healthy);
      fixture.assertReport(OVERWRITE, set(), set());
      fixture.assertGauges(0, false);
      Assert.assertEquals(fixture.stored(), healthy);
    }
  }

  /**
   * Only the overwrite scope skips anything, so its snapshot alone holds the gauge up. Once the
   * node returns inside the window no overwrite is needed, and the phase must still clear it.
   */
  @Test
  public void testOverwriteSnapshotClearsWhenTheNodeReturnsInsideTheWindow() throws Exception {
    try (Fixture fixture = Fixture.threeCliques(true)) {
      View healthy = fixture.converge();
      fixture.kill("a0");
      Assert.assertEquals(fixture.run(), healthy);
      fixture.assertReport(EMERGENCY, set(), set());
      fixture.assertReport(OVERWRITE, set("A"), set("A"));
      fixture.assertGauges(1, false);
      Assert.assertEquals(fixture.stored(), healthy);

      fixture.revive("a0");
      Assert.assertEquals(fixture.run(), healthy);
      fixture.assertGauges(0, false);
      fixture.assertReport(OVERWRITE, set(), set());
      Assert.assertEquals(fixture.stored(), healthy);
    }
  }

  /**
   * The window closes while both nodes are still down. The overwrite has nothing left to do, so its
   * snapshot clears, and the emergency rebalance takes the skip over: the healthy clique leaves its
   * dead node for good, the broken clique is carried and keeps naming its own dead node, so it
   * resumes in place when that node returns.
   */
  @Test
  public void testOverwriteSnapshotClearsWhenTheWindowClosesAndEmergencyTakesOverTheSkip()
      throws Exception {
    try (Fixture fixture = Fixture.threeCliques(true)) {
      View healthy = fixture.converge();
      String lostB = fixture.busiest("B");
      fixture.kill("a0");
      fixture.kill(lostB);
      fixture.run();
      fixture.assertReport(OVERWRITE, set("A", "B"), set("A"));
      fixture.assertGauges(1, false);

      fixture.expire("a0");
      fixture.expire(lostB);
      View result = fixture.run();
      fixture.assertReport(OVERWRITE, set(), set());
      fixture.assertReport(EMERGENCY, set("A", "B"), set("A"));
      fixture.assertReport(PARTIAL, set("A"), set("A"));
      Assert.assertEquals(result.of("A"), healthy.of("A"));
      fixture.assertMovedOff(result, healthy, "B", lostB);
      Assert.assertEquals(result.of("C"), healthy.of("C"));
      Assert.assertEquals(fixture.stored(), result, "The permanent move is the stored assignment");
      fixture.assertGauges(1, false);

      fixture.revive("a0");
      result = fixture.run();
      Assert.assertEquals(result.of("A"), healthy.of("A"));
      fixture.assertMovedOff(result, healthy, "B", lostB);
      fixture.assertReport(EMERGENCY, set(), set());
      fixture.assertReport(OVERWRITE, set(), set());
      fixture.assertReport(PARTIAL, set(), set());
      fixture.assertGauges(0, false);
    }
  }

  /**
   * A cluster wide capacity deficit instead of a tag local break. Isolation attributes it to clique
   * a in the overwrite scope inside the window, and again in the emergency scope at the window end,
   * so clique b is topped up and then moved for good while clique a stays carried, still naming its
   * dead node.
   */
  @Test
  public void testClusterWideDeficitIsAttributedInsideAndAtTheEndOfTheWindow() throws Exception {
    try (Fixture fixture = Fixture.threeCliques(true)) {
      View healthy = fixture.converge();
      fixture.setWeight("A", CLUSTER_WIDE_BREAK);
      Assert.assertEquals(fixture.run(), healthy);
      fixture.assertSkipped(BASELINE, set("A"));
      fixture.assertGauges(1, false);

      String lostB = fixture.busiest("B");
      fixture.kill("a0");
      fixture.kill(lostB);
      View result = fixture.run();
      fixture.assertReport(EMERGENCY, set(), set());
      fixture.assertReport(OVERWRITE, set("A", "B"), set("A"));
      Assert.assertTrue(fixture.estimate(OVERWRITE) < 0, "The overwrite must see the deficit");
      Assert.assertEquals(result.of("A"), healthy.of("A"));
      fixture.assertToppedUp(result, healthy, "B", lostB);
      Assert.assertEquals(result.of("C"), healthy.of("C"));
      Assert.assertEquals(fixture.stored(), healthy);
      fixture.assertGauges(1, false);

      fixture.expire("a0");
      fixture.expire(lostB);
      result = fixture.run();
      fixture.assertReport(EMERGENCY, set("A", "B"), set("A"));
      Assert.assertTrue(fixture.estimate(EMERGENCY) < 0, "The emergency must see the deficit");
      fixture.assertReport(OVERWRITE, set(), set());
      Assert.assertEquals(result.of("A"), healthy.of("A"));
      fixture.assertMovedOff(result, healthy, "B", lostB);
      Assert.assertEquals(result.of("C"), healthy.of("C"));
      Assert.assertEquals(fixture.stored(), result);
      fixture.assertGauges(1, false);
    }
  }

  /**
   * A resource added to a broken clique inside the window has no baseline, best possible or current
   * state entry. The overwrite skips its group whole, which reports it as skipped, but it has
   * nothing to carry, so it stays absent instead of being fabricated or half assigned. After the
   * repair it is placed by the partial rebalance and served, topped up, by the next pipeline.
   */
  @Test
  public void testSkippedResourceWithoutAPreOverwriteEntryIsNeverFabricated() throws Exception {
    try (Fixture fixture = new Fixture(true)) {
      fixture.addClique("clique_a", "a0", "a1", "a2");
      fixture.addClique("clique_b", "b0", "b1", "b2");
      fixture.addClique("clique_c", "c0", "c1", "c2");
      fixture.addResource("A", "clique_a", 2, 2, 2);
      fixture.addResource("B", "clique_b", 3, 2, 2);
      fixture.addResource("C", "clique_c", 3, 2, 2);
      View healthy = fixture.converge();
      String lostA = fixture.busiest("A");
      String lostB = fixture.busiest("B");

      fixture.setWeight("A", TAG_LOCAL_BREAK);
      fixture.addResource("A2", "clique_a", 2, 2, 2);
      fixture.kill(lostA);
      fixture.kill(lostB);
      View result = fixture.run();
      fixture.assertSkipped(BASELINE, set("A", "A2"));
      Assert.assertFalse(fixture._store.getBaseline().containsKey("A2"));
      fixture.assertReport(EMERGENCY, set(), set());
      fixture.assertReport(OVERWRITE, set("A", "B"), set("A", "A2"));
      Assert.assertTrue(fixture.estimate(OVERWRITE) >= 0,
          "A tag local break must stay clear of the cluster wide capacity precheck");
      Assert.assertFalse(result.has("A2"), "A skipped resource with nothing to carry stays absent");
      Assert.assertEquals(result.of("A"), healthy.of("A"));
      fixture.assertToppedUp(result, healthy, "B", lostB);
      Assert.assertEquals(result.of("C"), healthy.of("C"));
      Assert.assertEquals(fixture.stored(), healthy);
      fixture.assertGauges(2, false);

      // This pipeline's overwrite starts from the emergency result, which predates the first
      // placement of A2, so A2 is served by the next pipeline, the one the partial schedules.
      fixture.setWeight("A", WEIGHT);
      result = fixture.run();
      fixture.assertSkipped(BASELINE, set());
      fixture.assertReport(OVERWRITE, set("A", "B"), set());
      fixture.assertToppedUp(result, healthy, "A", lostA);
      fixture.assertToppedUp(result, healthy, "B", lostB);
      Assert.assertFalse(result.has("A2"));
      fixture.assertGauges(0, false);

      result = fixture.run();
      Assert.assertTrue(result.has("A2"));
      fixture.assertMinActiveInClique(result, "A");
      fixture.assertMinActiveInClique(result, "A2");
      fixture.assertToppedUp(result, healthy, "B", lostB);
      Assert.assertEquals(result.of("C"), healthy.of("C"));
      fixture.assertGauges(0, false);
    }
  }

  /**
   * Instance x is retagged from broken clique a to clique b, so a's carried placement still names
   * it, and x is the only node clique b can top up on. The overwrite pre-loads every current
   * replica, the carried ones included, so it is capacity aware of them: the top-up may share the
   * instance name without overcommitting it, and the name based collision yield is not needed in
   * this phase.
   */
  @Test
  public void testTopUpMayShareAnInstanceWithACarriedPlacementWithinItsCapacity()
      throws Exception {
    try (Fixture fixture = Fixture.retagged(CAPACITY)) {
      View healthy = fixture.convergeRetagged();
      fixture.kill("a0");
      fixture.kill("b0");
      View result = fixture.run();
      fixture.assertReport(EMERGENCY, set(), set());
      fixture.assertReport(OVERWRITE, set("A", "B"), set("A"));
      Assert.assertEquals(result.of("A"), healthy.of("A"));
      for (String partition : healthy.of("B").keySet()) {
        Assert.assertEquals(result.nodes("B", partition), set("b0", "b1", "x"));
      }
      Assert.assertEquals(fixture.usage(result, "x"), 4 * WEIGHT);
      Assert.assertEquals(result.of("C"), healthy.of("C"));
      Assert.assertEquals(fixture.stored(), healthy);
      Assert.assertEquals(fixture._monitor.getWagedRebalanceOverwriteFailingGauge(), 0L);
    }
  }

  /**
   * Same retag, but x only has room for one of clique b's two top-ups next to the replicas clique a
   * still carries there. Clique b is withheld whole rather than half topped up or overcommitting x,
   * and the rebalance still succeeds because clique c is an unaffected block.
   */
  @Test
  public void testTopUpIsWithheldWholeWhenACarriedPlacementLeavesNoRoom() throws Exception {
    try (Fixture fixture = Fixture.retagged(3 * WEIGHT)) {
      View healthy = fixture.convergeRetagged();
      fixture.kill("a0");
      fixture.kill("b0");
      View result = fixture.run();
      fixture.assertReport(OVERWRITE, set("A", "B"), set("A", "B"));
      Assert.assertEquals(result, healthy);
      Assert.assertEquals(fixture.usage(result, "x"), 2 * WEIGHT);
      Assert.assertEquals(fixture._monitor.getWagedRebalanceOverwriteFailingGauge(), 0L);
    }
  }

  /**
   * Stock behaviour without isolation: clique a's impossible top-up fails the whole overwrite, and
   * the stored assignment computeNewIdealStates falls back to has no top-up for clique b either.
   * Turning isolation on inside the same window releases clique b's top-up. A run in which nothing
   * fails produces the same assignment either way.
   */
  @Test
  public void testFlagOffOverwriteFailureBlocksEveryTopUp() throws Exception {
    View isolated;
    try (Fixture control = Fixture.threeCliques(true)) {
      isolated = control.converge();
    }
    try (Fixture fixture = Fixture.threeCliques(false)) {
      View healthy = fixture.converge();
      Assert.assertEquals(healthy, isolated, "A clean run must not depend on the flag");
      String lostB = fixture.busiest("B");
      fixture.kill("a0");
      fixture.kill(lostB);
      try {
        fixture.run();
        Assert.fail("Without isolation the impossible top-up of clique a must fail the overwrite");
      } catch (HelixRebalanceException expected) {
        Assert.assertEquals(expected.getFailureType(),
            HelixRebalanceException.Type.FAILED_TO_CALCULATE);
      }
      fixture.assertGauges(0, true);
      Assert.assertEquals(fixture.stored(), healthy);

      fixture.isolate(true);
      View result = fixture.run();
      Assert.assertEquals(result.of("A"), healthy.of("A"));
      fixture.assertToppedUp(result, healthy, "B", lostB);
      fixture.assertReport(OVERWRITE, set("A", "B"), set("A"));
      fixture.assertGauges(1, false);
    }
  }

  private static Set<String> set(String... values) {
    return new TreeSet<>(Arrays.asList(values));
  }

  /** An order independent copy of an assignment: resource, partition, instance, state. */
  private static final class View {
    private final Map<String, Map<String, Map<String, String>>> _resources = new TreeMap<>();

    private View(Map<String, ResourceAssignment> assignment) {
      assignment.forEach((resource, resourceAssignment) -> {
        Map<String, Map<String, String>> partitions = new TreeMap<>();
        for (Partition partition : resourceAssignment.getMappedPartitions()) {
          partitions.put(partition.getPartitionName(),
              new TreeMap<>(resourceAssignment.getReplicaMap(partition)));
        }
        _resources.put(resource, partitions);
      });
    }

    private boolean has(String resource) {
      return _resources.containsKey(resource);
    }

    private Map<String, Map<String, String>> of(String resource) {
      Assert.assertTrue(has(resource), resource + " is missing from " + this);
      return _resources.get(resource);
    }

    private Set<String> nodes(String resource, String partition) {
      return new TreeSet<>(of(resource).get(partition).keySet());
    }

    @Override
    public boolean equals(Object other) {
      return other instanceof View && _resources.equals(((View) other)._resources);
    }

    @Override
    public int hashCode() {
      return _resources.hashCode();
    }

    @Override
    public String toString() {
      return _resources.toString();
    }
  }

  private static final class Report {
    private final Set<String> _evaluated;
    private final Set<String> _skipped;

    private Report(Set<String> evaluated, Set<String> skipped) {
      _evaluated = new TreeSet<>(evaluated);
      _skipped = new TreeSet<>(skipped);
    }
  }

  private static final class Fixture implements AutoCloseable {
    private final ClusterConfig _clusterConfig = new ClusterConfig(CLUSTER);
    private final ResourceControllerDataProvider _data = mock(ResourceControllerDataProvider.class);
    private final Map<String, InstanceConfig> _instances = new TreeMap<>();
    private final Map<String, LiveInstance> _live = new TreeMap<>();
    private final Map<String, Long> _offlineSince = new HashMap<>();
    private final Map<String, Resource> _resources = new TreeMap<>();
    private final Map<String, IdealState> _ideals = new HashMap<>();
    private final Map<String, ResourceConfig> _configs = new HashMap<>();
    private final Map<ClusterModel.RebalanceScopeType, Report> _reports = new HashMap<>();
    private final Map<ClusterModel.RebalanceScopeType, Long> _estimates = new HashMap<>();
    private final ClusterStatusMonitor _monitor = new ClusterStatusMonitor(CLUSTER);
    private final AssignmentMetadataStore _store;
    private final RebalanceAlgorithm _algorithm;
    private final WagedRebalancer _rebalancer;

    private Fixture(boolean isolation) {
      _clusterConfig.setWagedInstanceTagIsolationEnabled(isolation);
      _clusterConfig.setInstanceCapacityKeys(Collections.singletonList("DISK"));
      _clusterConfig.setDefaultInstanceCapacityMap(Collections.singletonMap("DISK", CAPACITY));
      _clusterConfig.setDefaultPartitionWeightMap(Collections.singletonMap("DISK", WEIGHT));
      _clusterConfig.setDelayRebalaceEnabled(true);
      _clusterConfig.setRebalanceDelayTime(DELAY_MS);
      when(_data.getClusterName()).thenReturn(CLUSTER);
      when(_data.getClusterConfig()).thenReturn(_clusterConfig);
      when(_data.getAssignableInstanceConfigMap()).thenReturn(_instances);
      when(_data.getInstanceConfigMap()).thenReturn(_instances);
      when(_data.getAssignableInstances()).thenAnswer(call -> new HashSet<>(_instances.keySet()));
      when(_data.getAssignableLiveInstances()).thenAnswer(call -> new HashMap<>(_live));
      when(_data.getEnabledLiveInstances()).thenAnswer(call -> new HashSet<>(_live.keySet()));
      when(_data.getInstanceOfflineTimeMap()).thenAnswer(call -> new HashMap<>(_offlineSince));
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
      BucketDataAccessor accessor = mock(BucketDataAccessor.class);
      when(accessor.compressedBucketRead(anyString(), eq(HelixProperty.class)))
          .thenThrow(new ZkNoNodeException("Nothing is stored yet"));
      _store = new AssignmentMetadataStore(accessor, CLUSTER);
      RebalanceAlgorithm delegate =
          ConstraintBasedAlgorithmFactory.getInstance(Collections.emptyMap());
      _algorithm = new RebalanceAlgorithm() {
        @Override
        public OptimalAssignment calculate(ClusterModel clusterModel)
            throws HelixRebalanceException {
          synchronized (_reports) {
            _estimates.put(clusterModel.getRebalanceScopeType(),
                clusterModel.getContext().getEstimateUtilizationMap().get("DISK"));
          }
          return delegate.calculate(clusterModel);
        }

        @Override
        public void onAssignmentComputed(ClusterModel.RebalanceScopeType scope,
            Set<String> evaluatedResources, Set<String> skippedResources) {
          synchronized (_reports) {
            _reports.put(scope, new Report(evaluatedResources, skippedResources));
          }
        }
      };
      _rebalancer = new WagedRebalancer(_store, _algorithm, Optional.empty());
      _rebalancer.setClusterStatusMonitor(_monitor);
      _rebalancer.setPartialRebalanceAsyncMode(true);
    }

    /** Clique a has exactly as many nodes as replicas, so it cannot top up inside a window. */
    private static Fixture threeCliques(boolean isolation) throws IOException {
      Fixture fixture = new Fixture(isolation);
      fixture.addClique("clique_a", "a0", "a1");
      fixture.addClique("clique_b", "b0", "b1", "b2");
      fixture.addClique("clique_c", "c0", "c1", "c2");
      fixture.addResource("A", "clique_a", 2, 2, 2);
      fixture.addResource("B", "clique_b", 3, 2, 2);
      fixture.addResource("C", "clique_c", 3, 2, 2);
      return fixture;
    }

    /**
     * Every partition of A covers all of clique a, including x, and every partition of B covers
     * all of clique b, so both the carried placement and the only top-up target are forced.
     */
    private static Fixture retagged(int capacityOfX) throws IOException {
      Fixture fixture = new Fixture(true);
      fixture.addClique("clique_a", "a0", "a1", "x");
      fixture.addClique("clique_b", "b0", "b1");
      fixture.addClique("clique_c", "c0", "c1", "c2");
      fixture.addResource("A", "clique_a", 2, 3, 3);
      fixture.addResource("B", "clique_b", 2, 2, 2);
      fixture.addResource("C", "clique_c", 3, 2, 2);
      fixture._instances.get("x")
          .setInstanceCapacityMap(Collections.singletonMap("DISK", capacityOfX));
      return fixture;
    }

    private void addClique(String tag, String... nodes) {
      for (String node : nodes) {
        InstanceConfig instance = new InstanceConfig(node);
        instance.addTag(tag);
        instance.setInstanceOperation(InstanceConstants.InstanceOperation.ENABLE);
        _instances.put(node, instance);
        _live.put(node, new LiveInstance(node));
      }
    }

    private void addResource(String name, String tag, int partitions, int replicas,
        int minActive) throws IOException {
      Resource resource = new Resource(name);
      IdealState ideal = new IdealState(name);
      ideal.setRebalanceMode(IdealState.RebalanceMode.FULL_AUTO);
      ideal.setRebalancerClassName(WagedRebalancer.class.getName());
      ideal.setStateModelDefRef(BuiltInStateModelDefinitions.OnlineOffline.name());
      ideal.setInstanceGroupTag(tag);
      ideal.setNumPartitions(partitions);
      ideal.setReplicas(String.valueOf(replicas));
      ideal.setMinActiveReplicas(minActive);
      for (int i = 0; i < partitions; i++) {
        resource.addPartition(name + "_" + i);
        ideal.setPreferenceList(name + "_" + i, Collections.emptyList());
      }
      _resources.put(name, resource);
      _ideals.put(name, ideal);
      _configs.put(name, new ResourceConfig(name));
      setWeight(name, WEIGHT);
    }

    private void setWeight(String resource, int weight) throws IOException {
      _configs.get(resource).setPartitionCapacityMap(Collections.singletonMap(
          ResourceConfig.DEFAULT_PARTITION_KEY, Collections.singletonMap("DISK", weight)));
    }

    /**
     * Replace the instance config with a new object, as a ZooKeeper refresh would. Editing the tag
     * list in place would also edit the change detector's previous snapshot, which shares it.
     */
    private void retag(String node, String tag) {
      InstanceConfig previous = _instances.get(node);
      InstanceConfig retagged = new InstanceConfig(node);
      retagged.addTag(tag);
      retagged.setInstanceOperation(InstanceConstants.InstanceOperation.ENABLE);
      retagged.setInstanceCapacityMap(previous.getInstanceCapacityMap());
      _instances.put(node, retagged);
    }

    private void isolate(boolean enabled) {
      _clusterConfig.setWagedInstanceTagIsolationEnabled(enabled);
    }

    /** The node goes offline now, so it stays active until its window closes an hour from now. */
    private void kill(String node) {
      _live.remove(node);
      _offlineSince.put(node, System.currentTimeMillis() - 1000L);
    }

    /** The node stays offline, but its window has closed. */
    private void expire(String node) {
      Assert.assertFalse(_live.containsKey(node));
      _offlineSince.put(node, System.currentTimeMillis() - DELAY_MS - 1000L);
    }

    private void revive(String node) {
      _live.put(node, new LiveInstance(node));
      _offlineSince.remove(node);
    }

    /** One pipeline: baseline, emergency, overwrite, then the asynchronous partial. */
    private View run() throws Exception {
      synchronized (_reports) {
        _reports.clear();
        _estimates.clear();
      }
      Set<String> active = DelayedRebalanceUtil.getActiveNodes(_data.getAssignableInstances(),
          _data.getEnabledLiveInstances(), _data.getInstanceOfflineTimeMap(),
          _data.getAssignableLiveInstances().keySet(), _data.getAssignableInstanceConfigMap(),
          _clusterConfig);
      View result = new View(_rebalancer.computeBestPossibleAssignment(_data,
          new HashMap<>(_resources), active, new CurrentStateOutput(), _algorithm));
      awaitPartial();
      return result;
    }

    /**
     * The partial rebalance of a pipeline runs after the overwrite, on its own thread. Wait for it
     * so its report and its stored result are part of the pipeline being checked.
     */
    private void awaitPartial() throws Exception {
      Field runnerField = WagedRebalancer.class.getDeclaredField("_partialRebalanceRunner");
      runnerField.setAccessible(true);
      Field resultField =
          PartialRebalanceRunner.class.getDeclaredField("_asyncPartialRebalanceResult");
      resultField.setAccessible(true);
      Future<?> partial = (Future<?>) resultField.get(runnerField.get(_rebalancer));
      if (partial != null) {
        partial.get(60, TimeUnit.SECONDS);
      }
    }

    /**
     * The first pipeline only computes the baseline; the partial it schedules produces the first
     * best possible assignment, which the second pipeline serves.
     */
    private View converge() throws Exception {
      View result = run();
      for (int attempt = 0; attempt < 3 && !result.equals(stored()); attempt++) {
        result = run();
      }
      Assert.assertEquals(result, stored());
      for (String resource : _resources.keySet()) {
        assertMinActiveInClique(result, resource);
        Assert.assertEquals(result.of(resource).size(), _resources.get(resource).getPartitions()
            .size());
      }
      assertGauges(0, false);
      return result;
    }

    /**
     * Converge, check the forced placements, then retag x from clique a to clique b while every
     * node is live. Clique a can no longer place three replicas on its two remaining nodes, so the
     * baseline carries it, and nothing moves.
     */
    private View convergeRetagged() throws Exception {
      View healthy = converge();
      for (String partition : healthy.of("A").keySet()) {
        Assert.assertEquals(healthy.nodes("A", partition), set("a0", "a1", "x"));
      }
      for (String partition : healthy.of("B").keySet()) {
        Assert.assertEquals(healthy.nodes("B", partition), set("b0", "b1"));
      }
      retag("x", "clique_b");
      Assert.assertEquals(run(), healthy);
      Assert.assertTrue(skipped(BASELINE).contains("A"), "The baseline must carry clique a");
      Assert.assertEquals(stored(), healthy);
      return healthy;
    }

    private View stored() {
      return new View(_store.getBestPossibleAssignment());
    }

    private Set<String> tagged(String tag) {
      Set<String> nodes = new TreeSet<>();
      _instances.forEach((name, config) -> {
        if (config.containsTag(tag)) {
          nodes.add(name);
        }
      });
      return nodes;
    }

    private String tagOf(String resource) {
      return _ideals.get(resource).getInstanceGroupTag();
    }

    /** The clique node holding the most replicas of the resource, lowest name first on a tie. */
    private String busiest(String resource) {
      View stored = stored();
      String busiest = null;
      int most = -1;
      for (String node : tagged(tagOf(resource))) {
        int held = 0;
        for (Map<String, String> replicas : stored.of(resource).values()) {
          held += replicas.containsKey(node) ? 1 : 0;
        }
        if (held > most) {
          most = held;
          busiest = node;
        }
      }
      Assert.assertTrue(most > 0);
      return busiest;
    }

    /** The one live clique node a partition of the resource does not already use. */
    private String spare(String resource, Set<String> holders) {
      Set<String> spare = tagged(tagOf(resource));
      spare.removeAll(holders);
      spare.retainAll(_live.keySet());
      Assert.assertEquals(spare.size(), 1, "The topology must leave exactly one spare");
      return spare.iterator().next();
    }

    /**
     * Every partition that held the lost node keeps it, since it is still inside its window, and
     * gains the one spare of its clique. Every other partition is untouched.
     */
    private void assertToppedUp(View result, View before, String resource, String lost) {
      int toppedUp = 0;
      for (Map.Entry<String, Map<String, String>> entry : before.of(resource).entrySet()) {
        Map<String, String> expected = new TreeMap<>(entry.getValue());
        if (expected.containsKey(lost)) {
          expected.put(spare(resource, expected.keySet()), ONLINE);
          toppedUp++;
        }
        Assert.assertEquals(result.of(resource).get(entry.getKey()), expected,
            resource + " " + entry.getKey());
      }
      Assert.assertTrue(toppedUp > 0, lost + " held no replica of " + resource);
      Assert.assertEquals(result.of(resource).keySet(), before.of(resource).keySet());
    }

    /** Every partition that held the expired node moved to the one spare of its clique. */
    private void assertMovedOff(View result, View before, String resource, String lost) {
      int moved = 0;
      for (Map.Entry<String, Map<String, String>> entry : before.of(resource).entrySet()) {
        Map<String, String> expected = new TreeMap<>(entry.getValue());
        if (expected.containsKey(lost)) {
          String spare = spare(resource, expected.keySet());
          expected.remove(lost);
          expected.put(spare, ONLINE);
          moved++;
        }
        Assert.assertEquals(result.of(resource).get(entry.getKey()), expected,
            resource + " " + entry.getKey());
      }
      Assert.assertTrue(moved > 0, lost + " held no replica of " + resource);
    }

    /** Every partition sits in the resource's clique and has at least min active live replicas. */
    private void assertMinActiveInClique(View result, String resource) {
      Set<String> clique = tagged(tagOf(resource));
      int minActive = _ideals.get(resource).getMinActiveReplicas();
      Assert.assertEquals(result.of(resource).keySet().size(),
          _resources.get(resource).getPartitions().size());
      result.of(resource).forEach((partition, replicas) -> {
        Assert.assertTrue(clique.containsAll(replicas.keySet()), resource + " " + replicas);
        Set<String> live = new TreeSet<>(replicas.keySet());
        live.retainAll(_live.keySet());
        Assert.assertTrue(live.size() >= minActive, resource + " " + partition + " " + replicas);
      });
    }

    /** Total partition weight the assignment places on the node, whichever group placed it. */
    private int usage(View result, String node) throws IOException {
      int usage = 0;
      for (String resource : _resources.keySet()) {
        if (!result.has(resource)) {
          continue;
        }
        int weight = _configs.get(resource).getPartitionCapacityMap()
            .get(ResourceConfig.DEFAULT_PARTITION_KEY).get("DISK");
        for (Map<String, String> replicas : result.of(resource).values()) {
          usage += replicas.containsKey(node) ? weight : 0;
        }
      }
      return usage;
    }

    private Set<String> skipped(ClusterModel.RebalanceScopeType scope) {
      synchronized (_reports) {
        Report report = _reports.get(scope);
        Assert.assertNotNull(report, scope + " did not report in this pipeline");
        return report._skipped;
      }
    }

    /**
     * The tag blind remaining DISK estimate the scope's capacity precheck saw before any placement.
     * Negative is a cluster wide deficit.
     */
    private long estimate(ClusterModel.RebalanceScopeType scope) {
      synchronized (_reports) {
        Long estimate = _estimates.get(scope);
        Assert.assertNotNull(estimate, scope + " did not calculate in this pipeline");
        return estimate;
      }
    }

    private void assertSkipped(ClusterModel.RebalanceScopeType scope, Set<String> skipped) {
      Assert.assertEquals(skipped(scope), skipped, scope + " skipped");
    }

    private void assertReport(ClusterModel.RebalanceScopeType scope, Set<String> evaluated,
        Set<String> skipped) {
      synchronized (_reports) {
        Report report = _reports.get(scope);
        Assert.assertNotNull(report, scope + " did not report in this pipeline");
        Assert.assertEquals(report._evaluated, evaluated, scope + " evaluated");
        Assert.assertEquals(report._skipped, skipped, scope + " skipped");
      }
    }

    private void assertGauges(long skipped, boolean overwriteFailing) {
      Assert.assertEquals(_monitor.getWagedInstanceTagIsolationSkippedResourcesGauge(), skipped,
          "skipped resources gauge");
      Assert.assertEquals(_monitor.getWagedRebalanceOverwriteFailingGauge(),
          overwriteFailing ? 1L : 0L, "overwrite failing gauge");
    }

    @Override
    public void close() {
      _rebalancer.close();
    }
  }
}
