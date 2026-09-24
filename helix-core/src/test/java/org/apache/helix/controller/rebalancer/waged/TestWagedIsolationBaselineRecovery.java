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
import java.util.Arrays;
import java.util.Collections;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import org.apache.helix.HelixConstants;
import org.apache.helix.HelixRebalanceException;
import org.apache.helix.constants.InstanceConstants;
import org.apache.helix.controller.dataproviders.ResourceControllerDataProvider;
import org.apache.helix.controller.rebalancer.waged.constraints.ConstraintBasedAlgorithmFactory;
import org.apache.helix.controller.rebalancer.waged.model.ClusterModel;
import org.apache.helix.controller.rebalancer.waged.model.OptimalAssignment;
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
import org.apache.helix.monitoring.metrics.WagedRebalancerMetricCollector;
import org.apache.helix.monitoring.metrics.model.LatencyMetric;
import org.testng.Assert;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class TestWagedIsolationBaselineRecovery {
  @DataProvider(name = "sharing")
  public Object[][] sharing() {
    return new Object[][] {{false}, {true}};
  }

  @Test(dataProvider = "sharing")
  public void testIncrementalRepairRetriesTheEntireSkippedBlock(boolean sharedTags) throws Exception {
    try (BaselineFixture fixture = new BaselineFixture(sharedTags)) {
      fixture.isolate();
      fixture.setWeight("broken", 10);
      fixture.run();
      Assert.assertEquals(fixture._evaluated, new HashSet<>(Arrays.asList("broken", "sibling")));
      Assert.assertEquals(fixture._monitor.getWagedInstanceTagIsolationSkippedResourcesGauge(), 0L);
      Assert.assertEquals(fixture._store.getBaseline().keySet(), fixture._resources.keySet());
    }
  }

  @Test(dataProvider = "sharing")
  public void testUnrelatedChangeCannotMaskAStillBrokenBlock(boolean sharedTags) throws Exception {
    try (BaselineFixture fixture = new BaselineFixture(sharedTags)) {
      fixture.isolate();
      fixture.setWeight("healthy", 11);
      fixture.run();
      Assert.assertEquals(fixture._evaluated, fixture._resources.keySet());
      Assert.assertEquals(fixture._monitor.getWagedInstanceTagIsolationSkippedResourcesGauge(), 2L);
      Assert.assertEquals(fixture._store.getBaseline().keySet(), fixture._resources.keySet());
    }
  }

  @Test(dataProvider = "sharing")
  public void testHealthyHistoryKeepsStockIncrementalSelection(boolean sharedTags) throws Exception {
    try (BaselineFixture fixture = new BaselineFixture(sharedTags)) {
      fixture.run();
      Map<String, Map<String, String>> sibling =
          fixture._store.getBaseline().get("sibling").getRecord().getMapFields();
      fixture.setWeight("broken", 11);
      fixture.run();
      Assert.assertEquals(fixture._evaluated, Collections.singleton("broken"));
      Assert.assertEquals(fixture._store.getBaseline().get("sibling").getRecord().getMapFields(),
          sibling);
      Assert.assertEquals(fixture._monitor.getWagedInstanceTagIsolationSkippedResourcesGauge(), 0L);
    }
  }

  @Test(dataProvider = "sharing")
  public void testFailedMetadataWriteRetainsTheRetrySet(boolean sharedTags) throws Exception {
    try (BaselineFixture fixture = new BaselineFixture(sharedTags)) {
      fixture.isolate();
      fixture.setWeight("broken", 10);
      fixture._resources.get("broken").addPartition("broken_1");
      fixture._ideals.get("broken").setNumPartitions(2);
      fixture._ideals.get("broken").setPreferenceList("broken_1", Collections.emptyList());
      fixture._failWrites = true;
      try {
        fixture.run();
        Assert.fail("The injected baseline write failure must propagate");
      } catch (HelixRebalanceException expected) {
        Assert.assertEquals(expected.getFailureCategory(),
            HelixRebalanceException.FailureCategory.METADATA_STORE_IO);
      } finally {
        fixture._failWrites = false;
      }
      fixture.setWeight("healthy", 11);
      fixture.run();
      Assert.assertEquals(fixture._evaluated, fixture._resources.keySet(),
          "An unpersisted calculation must not discard a sibling's pending retry");
      Assert.assertEquals(fixture._monitor.getWagedInstanceTagIsolationSkippedResourcesGauge(), 0L);
    }
  }

  @Test(dataProvider = "sharing")
  public void testWholeBaselineFailureCannotLoseAnotherBlocksFailure(boolean sharedTags)
      throws Exception {
    try (BaselineFixture fixture = new BaselineFixture(sharedTags)) {
      fixture.isolate();
      fixture.setWeight("healthy", 150);
      try {
        fixture.run();
        Assert.fail("Both independent blocks failing must fail the whole baseline");
      } catch (HelixRebalanceException expected) {
        Assert.assertEquals(expected.getFailureCategory(),
            HelixRebalanceException.FailureCategory.NO_CANDIDATE_NODE);
      }
      fixture.setWeight("broken", 10);
      fixture.run();
      Assert.assertEquals(fixture._evaluated, fixture._resources.keySet(),
          "The next baseline must revisit work whose change events the failed calculation consumed");
      Assert.assertEquals(fixture._monitor.getWagedInstanceTagIsolationSkippedResourcesGauge(), 1L,
          "Repairing the first block must not hide the second block's unplaceable resource");
    }
  }

  @Test(dataProvider = "sharing")
  public void testIsolationDoesNotScheduleBaselinesWithoutARelevantChange(boolean sharedTags)
      throws Exception {
    try (BaselineFixture fixture = new BaselineFixture(sharedTags)) {
      fixture.isolate();
      int calculations = fixture._calculations;
      fixture.run();
      Assert.assertEquals(fixture._calculations, calculations,
          "A skipped block must not create a baseline retry loop");
      Assert.assertEquals(fixture._monitor.getWagedInstanceTagIsolationSkippedResourcesGauge(), 2L);
    }
  }

  @Test(dataProvider = "sharing")
  public void testDeletingTheFailedResourceReevaluatesItsSibling(boolean sharedTags) throws Exception {
    try (BaselineFixture fixture = new BaselineFixture(sharedTags)) {
      fixture.isolate();
      fixture._resources.remove("broken");
      fixture._ideals.remove("broken");
      fixture._configs.remove("broken");
      fixture.run();
      Assert.assertEquals(fixture._evaluated, Collections.singleton("sibling"));
      Assert.assertEquals(fixture._monitor.getWagedInstanceTagIsolationSkippedResourcesGauge(), 0L);
      Assert.assertFalse(fixture._store.getBaseline().containsKey("broken"));
    }
  }

  @Test(dataProvider = "sharing")
  public void testDisableRestoresStockIncrementalSelection(boolean sharedTags) throws Exception {
    try (BaselineFixture fixture = new BaselineFixture(sharedTags)) {
      fixture.isolate();
      fixture._clusterConfig.setWagedInstanceTagIsolationEnabled(false);
      try {
        fixture.run();
        Assert.fail("The unplaceable resource must fail the global mode");
      } catch (HelixRebalanceException expected) {
        Assert.assertEquals(expected.getFailureCategory(),
            HelixRebalanceException.FailureCategory.NO_CANDIDATE_NODE);
      }
      fixture.setWeight("broken", 10);
      fixture.run();
      Assert.assertEquals(fixture._evaluated, Collections.singleton("broken"));
      Assert.assertEquals(fixture._monitor.getWagedInstanceTagIsolationSkippedResourcesGauge(), 0L);
    }
  }

  @Test(dataProvider = "sharing")
  public void testNewRunnerRecoversWithoutPersistedRetryState(boolean sharedTags) throws Exception {
    try (BaselineFixture fixture = new BaselineFixture(sharedTags)) {
      fixture.isolate();
      fixture._runner.close();
      fixture._monitor.resetWagedInstanceTagIsolation();
      fixture._runner = fixture.newRunner();
      fixture.run();
      Assert.assertEquals(fixture._evaluated, fixture._resources.keySet());
      Assert.assertEquals(fixture._monitor.getWagedInstanceTagIsolationSkippedResourcesGauge(), 2L);
      fixture.setWeight("broken", 10);
      fixture.run();
      Assert.assertEquals(fixture._monitor.getWagedInstanceTagIsolationSkippedResourcesGauge(), 0L);
    }
  }

  private static class BaselineFixture implements AutoCloseable {
    private final ClusterConfig _clusterConfig = new ClusterConfig("BaselineIsolationRecovery");
    private final ResourceControllerDataProvider _data = mock(ResourceControllerDataProvider.class);
    private final Map<String, Resource> _resources = new HashMap<>();
    private final Map<String, IdealState> _ideals = new HashMap<>();
    private final Map<String, ResourceConfig> _configs = new HashMap<>();
    private boolean _failWrites;
    private final MockAssignmentMetadataStore _store = new MockAssignmentMetadataStore() {
      @Override
      public void persistBaseline(Map<String, ResourceAssignment> baseline) {
        if (_failWrites) {
          throw new IllegalStateException("Injected baseline persistence failure");
        }
        super.persistBaseline(baseline);
      }
    };
    private final ClusterStatusMonitor _monitor =
        new ClusterStatusMonitor("BaselineIsolationRecovery");
    private GlobalRebalanceRunner _runner;
    private Set<String> _evaluated = Collections.emptySet();
    private int _calculations;

    private BaselineFixture(boolean sharedTags) throws IOException {
      _clusterConfig.setWagedInstanceTagIsolationEnabled(true);
      _clusterConfig.setInstanceCapacityKeys(Collections.singletonList("DISK"));
      _clusterConfig.setDefaultInstanceCapacityMap(Collections.singletonMap("DISK", 100));
      _clusterConfig.setDefaultPartitionWeightMap(Collections.singletonMap("DISK", 10));
      Map<String, InstanceConfig> instances = new HashMap<>();
      Map<String, LiveInstance> live = new HashMap<>();
      for (int clique = 0; clique < 3; clique++) {
        for (int index = 0; index < 2; index++) {
          String name = "node_" + clique + "_" + index;
          InstanceConfig instance = new InstanceConfig(name);
          instance.addTag("clique_" + clique);
          instance.addTag("shared_pool");
          instance.setInstanceOperation(InstanceConstants.InstanceOperation.ENABLE);
          if (sharedTags && clique == 0 && index == 0) {
            instance.addTag("clique_1");
          }
          instances.put(name, instance);
          live.put(name, new LiveInstance(name));
        }
      }
      addResource("broken", "clique_0");
      addResource("sibling", sharedTags ? "clique_1" : "clique_0");
      addResource("healthy", "clique_2");
      when(_data.getClusterName()).thenReturn(_clusterConfig.getClusterName());
      when(_data.getClusterConfig()).thenReturn(_clusterConfig);
      when(_data.getAssignableInstanceConfigMap()).thenReturn(instances);
      when(_data.getInstanceConfigMap()).thenReturn(instances);
      when(_data.getAssignableInstances()).thenReturn(instances.keySet());
      when(_data.getAssignableLiveInstances()).thenReturn(live);
      when(_data.getIdealStates()).thenReturn(_ideals);
      when(_data.getIdealState(anyString())).thenAnswer(call -> _ideals.get(call.getArgument(0)));
      when(_data.getResourceConfigMap()).thenReturn(_configs);
      when(_data.getResourceConfig(anyString())).thenAnswer(call -> _configs.get(call.getArgument(0)));
      when(_data.getStateModelDef(anyString()))
          .thenReturn(BuiltInStateModelDefinitions.OnlineOffline.getStateModelDefinition());
      when(_data.getRefreshedChangeTypes()).thenReturn(EnumSet.of(
          HelixConstants.ChangeType.CLUSTER_CONFIG, HelixConstants.ChangeType.INSTANCE_CONFIG,
          HelixConstants.ChangeType.IDEAL_STATE, HelixConstants.ChangeType.RESOURCE_CONFIG,
          HelixConstants.ChangeType.LIVE_INSTANCE));
      _runner = newRunner();
    }

    private GlobalRebalanceRunner newRunner() {
      LatencyMetric latency = mock(LatencyMetric.class);
      return new GlobalRebalanceRunner(new AssignmentManager(latency), _store,
          new WagedRebalancerMetricCollector(_clusterConfig.getClusterName()), latency,
          failure -> Assert.fail("The synchronous runner must not report an async failure", failure),
          success -> { }, false);
    }

    private void addResource(String name, String tag) throws IOException {
      Resource resource = new Resource(name);
      resource.addPartition(name + "_0");
      _resources.put(name, resource);
      IdealState ideal = new IdealState(name);
      ideal.setRebalanceMode(IdealState.RebalanceMode.FULL_AUTO);
      ideal.setRebalancerClassName(WagedRebalancer.class.getName());
      ideal.setStateModelDefRef(BuiltInStateModelDefinitions.OnlineOffline.name());
      ideal.setInstanceGroupTag(tag);
      ideal.setNumPartitions(1);
      ideal.setReplicas("1");
      ideal.setPreferenceList(name + "_0", Collections.emptyList());
      _ideals.put(name, ideal);
      _configs.put(name, new ResourceConfig(name));
      setWeight(name, 10);
    }

    private void setWeight(String name, int weight) throws IOException {
      _configs.get(name).setPartitionCapacityMap(Collections.singletonMap(
          ResourceConfig.DEFAULT_PARTITION_KEY, Collections.singletonMap("DISK", weight)));
    }

    private void isolate() throws HelixRebalanceException, IOException {
      run();
      Assert.assertEquals(_store.getBaseline().keySet(), _resources.keySet());
      setWeight("broken", 150);
      run();
      Assert.assertEquals(_evaluated, Collections.singleton("broken"));
      Assert.assertEquals(_monitor.getWagedInstanceTagIsolationSkippedResourcesGauge(), 2L);
    }

    private void run() throws HelixRebalanceException {
      long generation = _monitor.configureWagedInstanceTagIsolation(
          _clusterConfig.isWagedInstanceTagIsolationEnabled(), _resources.keySet());
      RebalanceAlgorithm delegate = ConstraintBasedAlgorithmFactory.getInstance(Collections.emptyMap());
      _runner.globalRebalance(_data, _resources, new CurrentStateOutput(), new RebalanceAlgorithm() {
        @Override
        public OptimalAssignment calculate(ClusterModel model) throws HelixRebalanceException {
          _calculations++;
          _evaluated = new HashSet<>(model.getAssignableReplicaMap().keySet());
          return delegate.calculate(model);
        }

        @Override
        public void onAssignmentComputed(ClusterModel.RebalanceScopeType scope,
            Set<String> evaluatedResources, Set<String> skippedResources) {
          _monitor.updateWagedInstanceTagIsolationSkippedResources(generation, scope,
              evaluatedResources, skippedResources);
        }
      });
    }

    @Override
    public void close() {
      _runner.close();
      _store.close();
    }
  }
}
