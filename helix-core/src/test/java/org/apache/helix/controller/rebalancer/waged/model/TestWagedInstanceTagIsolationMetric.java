package org.apache.helix.controller.rebalancer.waged.model;

/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
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
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import org.apache.helix.HelixRebalanceException;
import org.apache.helix.controller.rebalancer.util.WagedRebalanceUtil;
import org.apache.helix.controller.rebalancer.waged.RebalanceAlgorithm;
import org.apache.helix.model.ClusterConfig;
import org.apache.helix.monitoring.mbeans.ClusterStatusMonitor;
import org.testng.Assert;
import org.testng.annotations.Test;

/**
 * Isolation deliberately turns a thrown rebalance failure into a quiet partial success. That is the
 * whole point of the mode, but it also means every other failure signal stays clean while a
 * clique goes unplaced indefinitely: no exception reaches the caller, no failure counter moves, and
 * the baseline health gauge still reads healthy.
 * <p>
 * Without a dedicated signal a permanently frozen clique is invisible, which trades a loud outage
 * for a silent one. These tests pin the signal that makes it visible again.
 */
public class TestWagedInstanceTagIsolationMetric extends AbstractTestWagedInstanceTagIsolation {

  /**
   * Three baseline runs each publish exactly one outcome, as checked by {@link #calculateOnce}.
   * The skipped set is empty, not null, on a healthy cluster, is exactly the broken clique's
   * resource while that clique cannot be placed, and is empty again once the clique is repaired.
   */
  @Test
  public void testIsolationReporterFiresAndReverses() throws Exception {
    RecordingAlgorithm algorithm = new RecordingAlgorithm(createAlgorithm());
    ClusterConfig clusterConfig = createClusterConfig(true);

    // A healthy cluster reports an empty set, never null, so the gauge reads zero.
    Set<String> skipped =
        calculateOnce(algorithm, createClusterModel(clusterConfig, allHealthy()));
    Assert.assertNotNull(skipped,
        "The outcome must be published even when nothing was isolated, otherwise the gauge can "
            + "never fall back to zero after a clique recovers");
    Assert.assertTrue(skipped.isEmpty(),
        "A healthy cluster must report no skipped resources, saw " + skipped);

    // Breaking one clique must report exactly that clique's resource.
    Map<Integer, CliqueSpec> broken = new HashMap<>(allHealthy());
    broken.put(3, CliqueSpec.healthy().withPartitionWeight(UNPLACEABLE_PARTITION_WEIGHT));
    skipped = calculateOnce(algorithm, createClusterModel(clusterConfig, broken));
    Assert.assertEquals(skipped, Collections.singleton(resourceName(3)),
        "The isolated clique's resource must be reported, otherwise a frozen clique is invisible: "
            + "isolation throws no exception and moves no failure counter");

    // Repairing it must bring the report back to empty.
    skipped = calculateOnce(algorithm, createClusterModel(clusterConfig, allHealthy()));
    Assert.assertTrue(skipped.isEmpty(),
        "The reported set must return to empty once the cluster is healthy again, saw " + skipped);
  }

  /**
   * A baseline run with three broken cliques publishes exactly one outcome, as checked by
   * {@link #calculateOnce}, whose skipped set is exactly those three cliques' resources.
   */
  @Test
  public void testEveryIsolatedCliqueIsCounted() throws Exception {
    RecordingAlgorithm algorithm = new RecordingAlgorithm(createAlgorithm());
    ClusterConfig clusterConfig = createClusterConfig(true);
    Map<Integer, CliqueSpec> specs = new HashMap<>(allHealthy());
    Set<String> brokenResources = new HashSet<>();
    for (int clique : new int[] {2, 7, 11}) {
      specs.put(clique, CliqueSpec.healthy().withPartitionWeight(UNPLACEABLE_PARTITION_WEIGHT));
      brokenResources.add(resourceName(clique));
    }
    Set<String> skipped = calculateOnce(algorithm, createClusterModel(clusterConfig, specs));

    Assert.assertTrue(brokenResources.equals(skipped),
        "All three broken cliques and nothing else must be counted, saw " + skipped);
  }

  /**
   * With the feature disabled, a broken clique fails the whole rebalance with FAILED_TO_CALCULATE
   * and no outcome is published, because the default mode throws instead of skipping.
   */
  @Test
  public void testDisabledFeatureNeverReportsSkippedResources() throws Exception {
    RecordingAlgorithm algorithm = new RecordingAlgorithm(createAlgorithm());
    ClusterConfig clusterConfig = createClusterConfig(false);
    Map<Integer, CliqueSpec> broken = new HashMap<>(allHealthy());
    broken.put(3, CliqueSpec.healthy().withPartitionWeight(UNPLACEABLE_PARTITION_WEIGHT));
    try {
      WagedRebalanceUtil.calculateAssignment(createClusterModel(clusterConfig, broken), algorithm);
      Assert.fail("The default global mode must still fail the whole rebalance");
    } catch (HelixRebalanceException expected) {
      Assert.assertEquals(expected.getFailureType(),
          HelixRebalanceException.Type.FAILED_TO_CALCULATE);
    }
    Assert.assertTrue(algorithm.skipped.isEmpty(),
        "A failed rebalance must publish no outcome, so nothing is ever reported as skipped with "
            + "isolation disabled, saw " + algorithm.skipped);
  }

  /**
   * The monitor gauge itself has to be reversible and start at zero, so an alert on "greater than
   * zero for an hour" means a clique really is still frozen rather than that one ever was.
   */
  @Test
  public void testMonitorGaugeIsReversible() {
    ClusterStatusMonitor monitor = new ClusterStatusMonitor("TestIsolationMetricCluster");
    Assert.assertEquals(monitor.getWagedInstanceTagIsolationSkippedResourcesGauge(), 0L,
        "The gauge must start at zero on a cluster that never isolated anything");

    Set<String> resources = new HashSet<>(Arrays.asList("R1", "R2", "R3"));
    long generation = monitor.configureWagedInstanceTagIsolation(true, resources);
    monitor.updateWagedInstanceTagIsolationSkippedResources(generation,
        ClusterModel.RebalanceScopeType.GLOBAL_BASELINE, resources, resources);
    Assert.assertEquals(monitor.getWagedInstanceTagIsolationSkippedResourcesGauge(), 3L);

    monitor.updateWagedInstanceTagIsolationSkippedResources(generation,
        ClusterModel.RebalanceScopeType.GLOBAL_BASELINE, resources, Collections.emptySet());
    Assert.assertEquals(monitor.getWagedInstanceTagIsolationSkippedResourcesGauge(), 0L,
        "The gauge must fall back to zero once a baseline places everything, otherwise it latches "
            + "on and a recovered clique still looks broken");
  }

  @Test
  public void testIncrementalBaselineCannotClearAnUnevaluatedResource() {
    ClusterStatusMonitor monitor = new ClusterStatusMonitor("TestIsolationIncrementalMetric");
    Set<String> resources = new HashSet<>(Arrays.asList("broken", "healthy"));
    long generation = monitor.configureWagedInstanceTagIsolation(true, resources);
    monitor.updateWagedInstanceTagIsolationSkippedResources(generation,
        ClusterModel.RebalanceScopeType.GLOBAL_BASELINE, resources,
        Collections.singleton("broken"));
    monitor.updateWagedInstanceTagIsolationSkippedResources(generation,
        ClusterModel.RebalanceScopeType.GLOBAL_BASELINE, Collections.singleton("healthy"),
        Collections.emptySet());
    Assert.assertEquals(monitor.getWagedInstanceTagIsolationSkippedResourcesGauge(), 1L);
    monitor.updateWagedInstanceTagIsolationSkippedResources(generation,
        ClusterModel.RebalanceScopeType.GLOBAL_BASELINE, Collections.singleton("broken"),
        Collections.emptySet());
    Assert.assertEquals(monitor.getWagedInstanceTagIsolationSkippedResourcesGauge(), 0L);
  }

  @Test
  public void testConcurrentScopesAreDeduplicatedAndRecoverIndependently() throws Exception {
    ClusterStatusMonitor monitor = new ClusterStatusMonitor("TestIsolationConcurrentMetric");
    Set<String> resources = new HashSet<>(Collections.singleton("shared"));
    for (ClusterModel.RebalanceScopeType scope : ClusterModel.RebalanceScopeType.values()) {
      resources.add(scope.name());
    }
    long generation = monitor.configureWagedInstanceTagIsolation(true, resources);
    ExecutorService executor = Executors.newFixedThreadPool(4);
    CountDownLatch start = new CountDownLatch(1);
    List<Future<?>> results = new ArrayList<>();
    try {
      for (ClusterModel.RebalanceScopeType scope : ClusterModel.RebalanceScopeType.values()) {
        results.add(executor.submit(() -> {
          if (!start.await(10, TimeUnit.SECONDS)) {
            throw new AssertionError("Concurrent reporters never started");
          }
          Set<String> skipped = new HashSet<>(Arrays.asList("shared", scope.name()));
          for (int i = 0; i < 100; i++) {
            monitor.updateWagedInstanceTagIsolationSkippedResources(generation, scope, resources,
                skipped);
          }
          return null;
        }));
      }
      start.countDown();
      for (Future<?> result : results) {
        result.get(20, TimeUnit.SECONDS);
      }
      Assert.assertEquals(monitor.getWagedInstanceTagIsolationSkippedResourcesGauge(), 5L);
      int remainingScopes = ClusterModel.RebalanceScopeType.values().length;
      for (ClusterModel.RebalanceScopeType scope : ClusterModel.RebalanceScopeType.values()) {
        monitor.updateWagedInstanceTagIsolationSkippedResources(generation, scope, resources,
            Collections.emptySet());
        remainingScopes--;
        Assert.assertEquals(monitor.getWagedInstanceTagIsolationSkippedResourcesGauge(),
            remainingScopes == 0 ? 0L : remainingScopes + 1L);
      }
    } finally {
      executor.shutdownNow();
      Assert.assertTrue(executor.awaitTermination(10, TimeUnit.SECONDS));
    }
  }

  @Test
  public void testDeletedResourcesAndOldLeadershipReportsCannotLatchTheGauge() {
    ClusterStatusMonitor monitor = new ClusterStatusMonitor("TestIsolationMetricLifecycle");
    Set<String> resources = new HashSet<>(Arrays.asList("deleted", "remaining"));
    long generation = monitor.configureWagedInstanceTagIsolation(true, resources);
    monitor.updateWagedInstanceTagIsolationSkippedResources(generation,
        ClusterModel.RebalanceScopeType.EMERGENCY, resources, resources);
    monitor.configureWagedInstanceTagIsolation(true, Collections.singleton("remaining"));
    Assert.assertEquals(monitor.getWagedInstanceTagIsolationSkippedResourcesGauge(), 1L);
    monitor.updateWagedInstanceTagIsolationSkippedResources(generation,
        ClusterModel.RebalanceScopeType.PARTIAL, resources, Collections.singleton("deleted"));
    Assert.assertEquals(monitor.getWagedInstanceTagIsolationSkippedResourcesGauge(), 1L);
    monitor.reset();
    monitor.updateWagedInstanceTagIsolationSkippedResources(generation,
        ClusterModel.RebalanceScopeType.EMERGENCY, resources, resources);
    Assert.assertEquals(monitor.getWagedInstanceTagIsolationSkippedResourcesGauge(), 0L);
    long newGeneration =
        monitor.configureWagedInstanceTagIsolation(true, Collections.singleton("remaining"));
    monitor.updateWagedInstanceTagIsolationSkippedResources(newGeneration,
        ClusterModel.RebalanceScopeType.PARTIAL, Collections.singleton("remaining"),
        Collections.singleton("remaining"));
    Assert.assertEquals(monitor.getWagedInstanceTagIsolationSkippedResourcesGauge(), 1L);
  }

  /**
   * Runs one assignment calculation and returns the skipped set it published, after asserting that
   * the run published exactly one outcome, for the GLOBAL_BASELINE scope the fixture builds, with
   * every resource reported as evaluated.
   */
  private static Set<String> calculateOnce(RecordingAlgorithm algorithm, ClusterModel model)
      throws HelixRebalanceException {
    int published = algorithm.scopes.size();
    WagedRebalanceUtil.calculateAssignment(model, algorithm);
    Assert.assertEquals(algorithm.scopes.size(), published + 1,
        "Every successful rebalance must publish exactly one outcome");
    Assert.assertEquals(algorithm.scopes.get(published),
        ClusterModel.RebalanceScopeType.GLOBAL_BASELINE,
        "The outcome must name the rebalanced scope, since the gauge treats a baseline differently "
            + "from every other scope");
    Set<String> evaluated = algorithm.evaluated.get(published);
    Assert.assertTrue(allResources().equals(evaluated),
        "A baseline must report every resource as evaluated, otherwise the gauge cannot clear a "
            + "recovered clique, saw " + evaluated);
    return algorithm.skipped.get(published);
  }

  private static Set<String> allResources() {
    Set<String> resources = new HashSet<>();
    for (int clique = 0; clique < CLIQUE_COUNT; clique++) {
      resources.add(resourceName(clique));
    }
    return resources;
  }

  /**
   * Observes each published outcome the way WagedRebalancer's reporting decorator does: it
   * delegates the calculation and the name, and forwards every outcome to the wrapped algorithm
   * before recording it.
   */
  private static final class RecordingAlgorithm implements RebalanceAlgorithm {
    private final RebalanceAlgorithm _delegate;
    final List<ClusterModel.RebalanceScopeType> scopes = new ArrayList<>();
    final List<Set<String>> evaluated = new ArrayList<>();
    final List<Set<String>> skipped = new ArrayList<>();

    RecordingAlgorithm(RebalanceAlgorithm delegate) {
      _delegate = delegate;
    }

    @Override
    public OptimalAssignment calculate(ClusterModel clusterModel) throws HelixRebalanceException {
      return _delegate.calculate(clusterModel);
    }

    @Override
    public String getName() {
      return _delegate.getName();
    }

    @Override
    public void onAssignmentComputed(ClusterModel.RebalanceScopeType scope,
        Set<String> evaluatedResources, Set<String> skippedResources) {
      _delegate.onAssignmentComputed(scope, evaluatedResources, skippedResources);
      scopes.add(scope);
      evaluated.add(evaluatedResources);
      skipped.add(skippedResources);
    }
  }
}
