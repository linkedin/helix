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
import java.lang.reflect.Method;
import java.util.Arrays;
import java.util.Collections;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.TreeMap;
import org.apache.helix.HelixConstants;
import org.apache.helix.HelixRebalanceException;
import org.apache.helix.constants.InstanceConstants;
import org.apache.helix.controller.dataproviders.ResourceControllerDataProvider;
import org.apache.helix.controller.rebalancer.constraint.MonitoredAbnormalResolver;
import org.apache.helix.controller.rebalancer.waged.constraints.ConstraintBasedAlgorithmFactory;
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
import org.apache.helix.monitoring.mbeans.ClusterStatusMonitorMBean;
import org.apache.helix.monitoring.metrics.MetricCollector;
import org.apache.helix.monitoring.metrics.WagedRebalancerMetricCollector;
import org.apache.helix.monitoring.metrics.model.CountMetric;
import org.apache.helix.monitoring.metrics.model.Metric;
import org.testng.Assert;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Drives the real WagedRebalancer through a node loss that no rebalance can recover from, on a
 * cluster whose only other nodes carry a tag no resource is pinned to. Such a standby pool can
 * never fail, so with instance tag isolation on it must not count as a part of the cluster that
 * survived: the failure has to reach the operator exactly as it does with the flag off, through
 * the same failure counters and gauges and the same last known good fallback.
 */
public class TestWagedIsolationTotalFailureReporting {
  private static final String CLIQUE = "prod";
  private static final String RESOURCE = "resource_prod";
  private static final String SKIPPED_GAUGE = "getWagedInstanceTagIsolationSkippedResourcesGauge";
  private static final String FAILURES = WagedRebalancerMetricCollector.WagedRebalancerMetricNames
      .RebalanceFailureCounter.name();

  @DataProvider(name = "standby")
  public Object[][] standby() {
    return new Object[][] {
        // No standby node at all, the control: both modes fail with a capacity deficit.
        {0, "getWagedFailureCapacityDeficitCounter"},
        // A standby node too small to hide the shortfall from the cluster wide capacity check.
        {100, "getWagedFailureCapacityDeficitCounter"},
        // A standby node big enough to pass that check, so the first replica finds no node.
        {200, "getWagedFailureNoCandidateNodeCounter"}
    };
  }

  @Test(dataProvider = "standby")
  public void testLosingMostOfTheOnlyCliqueIsReportedLikeTheDefaultMode(int standbyCapacity,
      String categoryCounter) throws Exception {
    assertReportedLikeTheDefaultMode(false, standbyCapacity, categoryCounter);
  }

  @Test(dataProvider = "standby")
  public void testDelayedOverwriteFailureOfTheOnlyCliqueIsReportedLikeTheDefaultMode(
      int standbyCapacity, String categoryCounter) throws Exception {
    Map<String, Object> off = assertReportedLikeTheDefaultMode(true, standbyCapacity,
        categoryCounter);
    Assert.assertEquals(off.get("getWagedRebalanceOverwriteFailingGauge"), 1L,
        "The failure must come from the delayed rebalance overwrite");
  }

  @Test
  public void testLosingABrokenCliqueNextToAHealthyOneAndAStandbyNodeIsStillIsolated()
      throws Exception {
    Map<String, Map<String, Map<String, String>>> before;
    try (Pipeline off = new Pipeline("TotalFailureGuardOff", false, false)) {
      off.healthyCliqueAndStandby();
      off.run(Pipeline.EVERYTHING);
      off.lose("prod_1", "prod_2", off.nodeHolding("resource_other"));
      Assert.assertEquals(off.observe(off.run(HelixConstants.ChangeType.LIVE_INSTANCE))
          .get(FAILURES), 1L, "The scenario must fail with the flag off");
    }
    try (Pipeline on = new Pipeline("TotalFailureGuardOn", true, false)) {
      on.healthyCliqueAndStandby();
      on.run(Pipeline.EVERYTHING);
      before = on.bestPossible();
      String lost = on.nodeHolding("resource_other");
      on.lose("prod_1", "prod_2", lost);
      Map<String, Object> observed = on.observe(on.run(HelixConstants.ChangeType.LIVE_INSTANCE));
      Assert.assertEquals(observed.get(FAILURES), 0L,
          "A clique that holds resources survived, so the failure must stay isolated");
      Assert.assertEquals(observed.get("getWagedFallbackInUseGauge"), 0L);
      Assert.assertEquals(on._monitor.getWagedInstanceTagIsolationSkippedResourcesGauge(), 1L);
      Map<String, Map<String, Map<String, String>>> after = on.bestPossible();
      Assert.assertEquals(after.get(RESOURCE), before.get(RESOURCE),
          "The broken clique must be carried forward unchanged");
      Assert.assertFalse(after.get("resource_other").equals(before.get("resource_other")),
          "The healthy clique must get a fresh assignment");
      after.get("resource_other").values().forEach(replicas -> {
        Assert.assertFalse(replicas.containsKey(lost), "Nothing may stay on the lost node");
        Assert.assertTrue(replicas.keySet().stream().allMatch(node -> node.startsWith("other_")));
      });
    }
  }

  /**
   * Lose two of the three nodes of the only clique that holds resources, permanently or within the
   * delay window, and require the flag on to produce the same observable outcome as the flag off.
   */
  private static Map<String, Object> assertReportedLikeTheDefaultMode(boolean delayed,
      int standbyCapacity, String categoryCounter) throws Exception {
    Map<String, Object> off = loseMostOfTheOnlyClique(false, delayed, standbyCapacity);
    Assert.assertEquals(off.get(FAILURES), 1L, "The scenario must fail with the flag off");
    Assert.assertEquals(off.get(categoryCounter), 1L);
    Assert.assertEquals(off.get("getWagedFallbackInUseGauge"), 1L);
    Map<String, Object> on = loseMostOfTheOnlyClique(true, delayed, standbyCapacity);
    long skipped = (Long) on.remove(SKIPPED_GAUGE);
    Assert.assertEquals(on, off, "The flag on must report the failure exactly like the flag off");
    Assert.assertEquals(skipped, 0L, "A rebalance that failed as a whole isolated nothing");
    return off;
  }

  private static Map<String, Object> loseMostOfTheOnlyClique(boolean isolation, boolean delayed,
      int standbyCapacity) throws Exception {
    String name = "TotalFailure" + (delayed ? "Delayed" : "Emergency") + standbyCapacity
        + (isolation ? "On" : "Off");
    try (Pipeline pipeline = new Pipeline(name, isolation, delayed)) {
      for (int i = 0; i < 3; i++) {
        pipeline.addNode("prod_" + i, CLIQUE, 100);
      }
      if (standbyCapacity > 0) {
        pipeline.addNode("standby_0", "standby", standbyCapacity);
      }
      // Two partitions per clique node: they fit on three nodes and cannot fit on one.
      pipeline.addResource(RESOURCE, CLIQUE, 6, 40);
      pipeline.run(Pipeline.EVERYTHING);
      pipeline.lose("prod_1", "prod_2");
      Map<String, Object> observed = pipeline.observe(
          pipeline.run(HelixConstants.ChangeType.LIVE_INSTANCE));
      if (!isolation) {
        Assert.assertEquals(observed.remove(SKIPPED_GAUGE), 0L);
      }
      return observed;
    }
  }

  private static final class Pipeline implements AutoCloseable {
    private static final HelixConstants.ChangeType[] EVERYTHING = {
        HelixConstants.ChangeType.CLUSTER_CONFIG, HelixConstants.ChangeType.INSTANCE_CONFIG,
        HelixConstants.ChangeType.IDEAL_STATE, HelixConstants.ChangeType.RESOURCE_CONFIG,
        HelixConstants.ChangeType.LIVE_INSTANCE};

    private final ClusterConfig _clusterConfig;
    private final boolean _delayed;
    private final ResourceControllerDataProvider _data = mock(ResourceControllerDataProvider.class);
    private final Map<String, InstanceConfig> _instances = new HashMap<>();
    private final Map<String, LiveInstance> _live = new HashMap<>();
    private final Map<String, Long> _offlineTimes = new HashMap<>();
    private final Map<String, Resource> _resources = new HashMap<>();
    private final Map<String, IdealState> _ideals = new HashMap<>();
    private final Map<String, ResourceConfig> _configs = new HashMap<>();
    private final MockAssignmentMetadataStore _store = new MockAssignmentMetadataStore();
    private final ClusterStatusMonitor _monitor;
    private final WagedRebalancerMetricCollector _metrics;
    private final WagedRebalancer _rebalancer;
    private Set<HelixConstants.ChangeType> _changes = Collections.emptySet();

    private Pipeline(String name, boolean isolation, boolean delayed) {
      _delayed = delayed;
      _clusterConfig = new ClusterConfig(name);
      if (isolation) {
        _clusterConfig.setWagedInstanceTagIsolationEnabled(true);
      }
      if (delayed) {
        _clusterConfig.setDelayRebalaceEnabled(true);
        _clusterConfig.setRebalanceDelayTime(3600000L);
      }
      _clusterConfig.setInstanceCapacityKeys(Collections.singletonList("DISK"));
      _clusterConfig.setDefaultInstanceCapacityMap(Collections.singletonMap("DISK", 100));
      _clusterConfig.setDefaultPartitionWeightMap(Collections.singletonMap("DISK", 10));
      when(_data.getClusterName()).thenReturn(name);
      when(_data.getClusterConfig()).thenReturn(_clusterConfig);
      when(_data.getAssignableInstanceConfigMap()).thenReturn(_instances);
      when(_data.getInstanceConfigMap()).thenReturn(_instances);
      when(_data.getAssignableInstances()).thenReturn(_instances.keySet());
      when(_data.getEnabledInstances()).thenReturn(_instances.keySet());
      when(_data.getAssignableLiveInstances()).thenReturn(_live);
      when(_data.getLiveInstances()).thenReturn(_live);
      when(_data.getEnabledLiveInstances()).thenAnswer(call -> new HashSet<>(_live.keySet()));
      when(_data.getInstanceOfflineTimeMap()).thenReturn(_offlineTimes);
      when(_data.getIdealStates()).thenReturn(_ideals);
      when(_data.getIdealState(anyString())).thenAnswer(call -> _ideals.get(call.getArgument(0)));
      when(_data.getResourceConfigMap()).thenReturn(_configs);
      when(_data.getResourceConfig(anyString()))
          .thenAnswer(call -> _configs.get(call.getArgument(0)));
      when(_data.getStateModelDef(anyString()))
          .thenReturn(BuiltInStateModelDefinitions.OnlineOffline.getStateModelDefinition());
      when(_data.getAbnormalStateResolver(any()))
          .thenReturn(MonitoredAbnormalResolver.DUMMY_STATE_RESOLVER);
      when(_data.getRefreshedChangeTypes()).thenAnswer(call -> _changes);
      _metrics = new WagedRebalancerMetricCollector(name);
      _rebalancer = new WagedRebalancer(_store,
          ConstraintBasedAlgorithmFactory.getInstance(Collections.emptyMap()),
          Optional.<MetricCollector>of(_metrics));
      _monitor = new ClusterStatusMonitor(name);
      _rebalancer.setClusterStatusMonitor(_monitor);
    }

    private void healthyCliqueAndStandby() throws IOException {
      for (int i = 0; i < 3; i++) {
        addNode("prod_" + i, CLIQUE, 100);
      }
      addNode("other_0", "other", 100);
      addNode("other_1", "other", 100);
      addNode("standby_0", "standby", 200);
      addResource(RESOURCE, CLIQUE, 6, 40);
      addResource("resource_other", "other", 2, 40);
    }

    private void addNode(String name, String tag, int capacity) {
      InstanceConfig instance = new InstanceConfig(name);
      instance.addTag(tag);
      instance.setInstanceOperation(InstanceConstants.InstanceOperation.ENABLE);
      instance.setInstanceCapacityMap(Collections.singletonMap("DISK", capacity));
      _instances.put(name, instance);
      _live.put(name, new LiveInstance(name));
    }

    private void addResource(String name, String tag, int partitions, int weight)
        throws IOException {
      Resource resource = new Resource(name);
      IdealState ideal = new IdealState(name);
      ideal.setRebalanceMode(IdealState.RebalanceMode.FULL_AUTO);
      ideal.setRebalancerClassName(WagedRebalancer.class.getName());
      ideal.setStateModelDefRef(BuiltInStateModelDefinitions.OnlineOffline.name());
      ideal.setInstanceGroupTag(tag);
      ideal.setNumPartitions(partitions);
      ideal.setReplicas("1");
      ideal.setMinActiveReplicas(1);
      for (int i = 0; i < partitions; i++) {
        resource.addPartition(name + "_" + i);
        ideal.setPreferenceList(name + "_" + i, Collections.emptyList());
      }
      ResourceConfig config = new ResourceConfig(name);
      config.setPartitionCapacityMap(Collections.singletonMap(
          ResourceConfig.DEFAULT_PARTITION_KEY, Collections.singletonMap("DISK", weight)));
      _resources.put(name, resource);
      _ideals.put(name, ideal);
      _configs.put(name, config);
    }

    /**
     * Take nodes down. Without delayed rebalance they are gone for good, which leaves the recovery
     * to the emergency rebalance. With it they stay active for an hour, which leaves the recovery
     * to the delayed rebalance overwrite that tops up the minimum active replicas.
     */
    private void lose(String... nodes) {
      for (String node : nodes) {
        _live.remove(node);
        if (_delayed) {
          _offlineTimes.put(node, System.currentTimeMillis());
        }
      }
    }

    private Map<String, IdealState> run(HelixConstants.ChangeType... changes)
        throws HelixRebalanceException {
      _changes = EnumSet.copyOf(Arrays.asList(changes));
      Map<String, IdealState> result =
          _rebalancer.computeNewIdealStates(_data, _resources, new CurrentStateOutput());
      if (changes.length == EVERYTHING.length) {
        Assert.assertEquals(_metrics.getMetric(FAILURES, CountMetric.class).getValue(), (Long) 0L,
            "The cluster must rebalance cleanly before it loses nodes");
        Assert.assertEquals(_store.getBestPossibleAssignment().keySet(), _resources.keySet());
      }
      return result;
    }

    private String nodeHolding(String resource) {
      return _store.getBestPossibleAssignment().get(resource).getRecord().getMapFields().values()
          .iterator().next().keySet().iterator().next();
    }

    private Map<String, Map<String, Map<String, String>>> bestPossible() {
      return mapFields(_store.getBestPossibleAssignment());
    }

    /**
     * Everything an operator or the pipeline can observe after a rebalance: every WAGED counter and
     * gauge on the cluster status monitor, every counter of the rebalancer itself, the ideal states
     * handed to the pipeline and the persisted assignments.
     */
    private Map<String, Object> observe(Map<String, IdealState> result) throws Exception {
      Map<String, Object> observed = new TreeMap<>();
      for (Method getter : ClusterStatusMonitorMBean.class.getMethods()) {
        if (getter.getName().startsWith("getWaged") && getter.getParameterCount() == 0
            && getter.getReturnType() == long.class) {
          observed.put(getter.getName(), getter.invoke(_monitor));
        }
      }
      for (Map.Entry<String, Metric> metric : _metrics.getMetricMap().entrySet()) {
        if (metric.getValue() instanceof CountMetric) {
          observed.put(metric.getKey(), ((CountMetric) metric.getValue()).getValue());
        }
      }
      Map<String, Object> idealStates = new TreeMap<>();
      result.forEach((resource, ideal) -> idealStates.put(resource,
          Arrays.asList(ideal.getRecord().getListFields(), ideal.getRecord().getMapFields())));
      observed.put("idealStates", idealStates);
      observed.put("bestPossible", bestPossible());
      observed.put("baseline", mapFields(_store.getBaseline()));
      return observed;
    }

    private static Map<String, Map<String, Map<String, String>>> mapFields(
        Map<String, ResourceAssignment> assignment) {
      Map<String, Map<String, Map<String, String>>> fields = new TreeMap<>();
      assignment.forEach((resource, value) -> fields.put(resource,
          new TreeMap<>(value.getRecord().getMapFields())));
      return fields;
    }

    @Override
    public void close() {
      _rebalancer.close();
      _store.close();
    }
  }
}
