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
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

import org.apache.helix.HelixConstants;
import org.apache.helix.HelixRebalanceException;
import org.apache.helix.controller.dataproviders.ResourceControllerDataProvider;
import org.apache.helix.controller.rebalancer.waged.constraints.MockRebalanceAlgorithm;
import org.apache.helix.controller.rebalancer.waged.model.AbstractTestClusterModel;
import org.apache.helix.controller.rebalancer.waged.model.ClusterModel;
import org.apache.helix.controller.rebalancer.waged.model.OptimalAssignment;
import org.apache.helix.controller.stages.CurrentStateOutput;
import org.apache.helix.model.IdealState;
import org.apache.helix.model.InstanceConfig;
import org.apache.helix.model.LiveInstance;
import org.apache.helix.model.Resource;
import org.mockito.Mockito;
import org.mockito.stubbing.Answer;
import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.when;

/**
 * Exercises the WAGED async executor thread-naming behavior added so that failure
 * logs from {@link GlobalRebalanceRunner} and {@link PartialRebalanceRunner} carry
 * the cluster name as part of the thread column. This matters when a single
 * controller JVM hosts multiple WAGED-managed clusters and operators need to
 * correlate failure logs back to a specific cluster.
 *
 * <p>Tests run the full {@link WagedRebalancer#computeNewIdealStates} synchronously
 * (via the protected test constructor that disables async on both runners) and
 * inject a {@link RebalanceAlgorithm} that records what {@code Thread.currentThread()}
 * was named at the moment it was invoked, plus retains the {@link Thread} reference
 * so the test can verify the original name was restored after the task completed.
 */
public class TestWagedRebalanceThreadNaming extends AbstractTestClusterModel {

  private static final String GLOBAL_REBALANCE_THREAD_NAME_PREFIX = "WagedGlobalRebalance-";
  private static final String PARTIAL_REBALANCE_THREAD_NAME_PREFIX = "WagedPartialRebalance-";
  private static final String TEST_CLUSTER_NAME = "TestWagedThreadNamingCluster";

  private MockAssignmentMetadataStore _metadataStore;

  @BeforeClass
  public void initialize() {
    super.initialize();
    _metadataStore = new MockAssignmentMetadataStore();
  }

  @Override
  protected ResourceControllerDataProvider setupClusterDataCache() throws IOException {
    ResourceControllerDataProvider testCache = super.setupClusterDataCache();

    when(testCache.getClusterName()).thenReturn(TEST_CLUSTER_NAME);

    Map<String, IdealState> isMap = new HashMap<>();
    for (String resource : _resourceNames) {
      IdealState is = new IdealState(resource);
      is.setNumPartitions(_partitionNames.size());
      is.setRebalanceMode(IdealState.RebalanceMode.FULL_AUTO);
      is.setStateModelDefRef("MasterSlave");
      is.setReplicas("3");
      is.setRebalancerClassName(WagedRebalancer.class.getName());
      _partitionNames.forEach(partition -> is.setPreferenceList(partition, Collections.emptyList()));
      isMap.put(resource, is);
    }
    when(testCache.getIdealState(anyString())).thenAnswer(
        (Answer<IdealState>) invocationOnMock -> isMap.get(invocationOnMock.getArguments()[0]));
    when(testCache.getIdealStates()).thenReturn(isMap);

    for (int i = 1; i < 3; i++) {
      String instanceName = _testInstanceId + i;
      _instances.add(instanceName);
      InstanceConfig testInstanceConfig = createMockInstanceConfig(instanceName);
      Map<String, InstanceConfig> instanceConfigMap = testCache.getAssignableInstanceConfigMap();
      instanceConfigMap.put(instanceName, testInstanceConfig);
      when(testCache.getAssignableInstanceConfigMap()).thenReturn(instanceConfigMap);
      when(testCache.getInstanceConfigMap()).thenReturn(instanceConfigMap);
      LiveInstance testLiveInstance = createMockLiveInstance(instanceName);
      Map<String, LiveInstance> liveInstanceMap = testCache.getAssignableLiveInstances();
      liveInstanceMap.put(instanceName, testLiveInstance);
      when(testCache.getAssignableLiveInstances()).thenReturn(liveInstanceMap);
      when(testCache.getLiveInstances()).thenReturn(liveInstanceMap);
      when(testCache.getEnabledInstances()).thenReturn(liveInstanceMap.keySet());
      when(testCache.getEnabledLiveInstances()).thenReturn(liveInstanceMap.keySet());
      when(testCache.getAssignableInstances()).thenReturn(_instances);
      when(testCache.getAllInstances()).thenReturn(_instances);
    }

    return testCache;
  }

  /**
   * Happy path: both the baseline (global) and best-possible (partial) calculations
   * should observe a thread name carrying the cluster name, and the original executor
   * thread name should be restored once each task completes (otherwise subsequent
   * baseline calcs on a different cluster would inherit a stale name on the reused
   * single-thread executor).
   */
  @Test
  public void testThreadNameContainsClusterNameAndIsRestored()
      throws IOException, HelixRebalanceException {
    _metadataStore.reset();
    ThreadNameCapturingAlgorithm algorithm = new ThreadNameCapturingAlgorithm();
    WagedRebalancer rebalancer =
        new WagedRebalancer(_metadataStore, algorithm, Optional.empty());

    runRebalanceOnce(rebalancer);

    Assert.assertTrue(algorithm.sawGlobalCall,
        "Expected at least one calculate() invocation on the global-rebalance executor "
            + "thread. Observed names=" + algorithm.observedNames);
    Assert.assertTrue(algorithm.sawPartialCall,
        "Expected at least one calculate() invocation on the partial-rebalance executor "
            + "thread. Observed names=" + algorithm.observedNames);

    for (String name : algorithm.observedNames) {
      Assert.assertTrue(
          name.equals(GLOBAL_REBALANCE_THREAD_NAME_PREFIX + TEST_CLUSTER_NAME)
              || name.equals(PARTIAL_REBALANCE_THREAD_NAME_PREFIX + TEST_CLUSTER_NAME),
          "Observed thread name '" + name + "' did not match either WAGED prefix + "
              + "cluster name. All observed=" + algorithm.observedNames);
    }

    for (Thread t : algorithm.observedThreads) {
      String nameAfter = t.getName();
      Assert.assertFalse(
          nameAfter.startsWith(GLOBAL_REBALANCE_THREAD_NAME_PREFIX)
              || nameAfter.startsWith(PARTIAL_REBALANCE_THREAD_NAME_PREFIX),
          "Executor thread name was not restored after the rebalance task completed. "
              + "Current name='" + nameAfter + "'.");
    }
  }

  /**
   * Failure path: when the algorithm throws, the thread name must still be restored
   * via the {@code finally} block. This is the scenario where stale names would do
   * the most damage -- a failing rebalance on cluster A would leave the executor
   * thread named WagedGlobalRebalance-A, and the very next baseline calc (potentially
   * on the same cluster after retry, or on a different cluster if the runner were
   * reused) would log under the wrong cluster context.
   */
  @Test(dependsOnMethods = "testThreadNameContainsClusterNameAndIsRestored")
  public void testThreadNameRestoredEvenWhenAlgorithmThrows() throws IOException {
    _metadataStore.reset();
    ThrowingThreadNameCapturingAlgorithm algorithm =
        new ThrowingThreadNameCapturingAlgorithm();
    WagedRebalancer rebalancer =
        new WagedRebalancer(_metadataStore, algorithm, Optional.empty());

    try {
      runRebalanceOnce(rebalancer);
    } catch (HelixRebalanceException expected) {
      // expected: the throwing algorithm propagates failure up through the sync path
    }

    Assert.assertFalse(algorithm.observedNames.isEmpty(),
        "Expected the throwing algorithm to be invoked at least once before failing");
    for (String name : algorithm.observedNames) {
      Assert.assertTrue(name.contains(TEST_CLUSTER_NAME),
          "Even on the failure path, the thread name observed inside the task must carry "
              + "the cluster name. Observed='" + name + "'");
    }

    for (Thread t : algorithm.observedThreads) {
      String nameAfter = t.getName();
      Assert.assertFalse(
          nameAfter.startsWith(GLOBAL_REBALANCE_THREAD_NAME_PREFIX)
              || nameAfter.startsWith(PARTIAL_REBALANCE_THREAD_NAME_PREFIX),
          "Executor thread name was not restored after a failing rebalance task. "
              + "Current name='" + nameAfter + "'.");
    }
  }

  private void runRebalanceOnce(WagedRebalancer rebalancer)
      throws IOException, HelixRebalanceException {
    ResourceControllerDataProvider clusterData = setupClusterDataCache();
    Map<String, Resource> resourceMap = clusterData.getIdealStates().entrySet().stream()
        .collect(Collectors.toMap(Map.Entry::getKey, entry -> {
          Resource resource = new Resource(entry.getKey());
          entry.getValue().getPartitionSet().forEach(resource::addPartition);
          return resource;
        }));
    when(clusterData.getRefreshedChangeTypes())
        .thenReturn(Collections.singleton(HelixConstants.ChangeType.CLUSTER_CONFIG));
    when(clusterData.checkAndReduceCapacity(Mockito.any(), Mockito.any(), Mockito.any()))
        .thenReturn(true);

    rebalancer.computeNewIdealStates(clusterData, resourceMap, new CurrentStateOutput());
  }

  /**
   * Records the executor thread and its name at the moment {@code calculate} is
   * invoked, then defers to {@link MockRebalanceAlgorithm} so the pipeline produces
   * a valid assignment and the rebalance completes normally.
   */
  private static class ThreadNameCapturingAlgorithm implements RebalanceAlgorithm {
    private final MockRebalanceAlgorithm _delegate = new MockRebalanceAlgorithm();
    final List<String> observedNames = Collections.synchronizedList(new ArrayList<>());
    final List<Thread> observedThreads = Collections.synchronizedList(new ArrayList<>());
    boolean sawGlobalCall;
    boolean sawPartialCall;

    @Override
    public OptimalAssignment calculate(ClusterModel clusterModel) throws HelixRebalanceException {
      String name = Thread.currentThread().getName();
      observedNames.add(name);
      observedThreads.add(Thread.currentThread());
      if (name.startsWith(GLOBAL_REBALANCE_THREAD_NAME_PREFIX)) {
        sawGlobalCall = true;
      } else if (name.startsWith(PARTIAL_REBALANCE_THREAD_NAME_PREFIX)) {
        sawPartialCall = true;
      }
      return _delegate.calculate(clusterModel);
    }
  }

  /**
   * Records the thread name observed at invocation time and then throws.
   * Used to verify the {@code finally} clause restores the thread name even when
   * the body of the lambda exits via exception.
   */
  private static class ThrowingThreadNameCapturingAlgorithm implements RebalanceAlgorithm {
    final List<String> observedNames = Collections.synchronizedList(new ArrayList<>());
    final List<Thread> observedThreads = Collections.synchronizedList(new ArrayList<>());

    @Override
    public OptimalAssignment calculate(ClusterModel clusterModel) throws HelixRebalanceException {
      observedNames.add(Thread.currentThread().getName());
      observedThreads.add(Thread.currentThread());
      throw new HelixRebalanceException(
          "Synthetic failure from TestWagedRebalanceThreadNaming",
          HelixRebalanceException.Type.FAILED_TO_CALCULATE);
    }
  }
}
