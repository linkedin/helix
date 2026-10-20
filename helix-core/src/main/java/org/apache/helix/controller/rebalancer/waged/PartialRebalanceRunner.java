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

import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;
import org.apache.helix.HelixRebalanceException;
import org.apache.helix.controller.dataproviders.ResourceControllerDataProvider;
import org.apache.helix.controller.rebalancer.util.ResourceUsageCalculator;
import org.apache.helix.controller.rebalancer.util.WagedRebalanceUtil;
import org.apache.helix.controller.rebalancer.waged.model.ClusterModel;
import org.apache.helix.controller.rebalancer.waged.model.ClusterModelProvider;
import org.apache.helix.controller.stages.CurrentStateOutput;
import org.apache.helix.model.Resource;
import org.apache.helix.model.ResourceAssignment;
import org.apache.helix.monitoring.metrics.MetricCollector;
import org.apache.helix.monitoring.metrics.WagedRebalancerMetricCollector;
import org.apache.helix.monitoring.metrics.implementation.BaselineDivergenceGauge;
import org.apache.helix.monitoring.metrics.model.CountMetric;
import org.apache.helix.monitoring.metrics.model.LatencyMetric;
import org.apache.helix.util.RebalanceUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Compute the best possible assignment based on the Baseline and the previous Best Possible assignment.
 * The coordinator compares the previous Best Possible assignment with the current cluster state so as to derive a
 * minimal rebalance scope. In short, the rebalance scope only contains the following two types of partitions.
 * 1. The partition's current assignment becomes invalid.
 * 2. The Baseline contains some new partition assignments that do not exist in the current assignment.
 */
class PartialRebalanceRunner implements AutoCloseable {

  private static final Logger LOG = LoggerFactory.getLogger(PartialRebalanceRunner.class);

  // Thread name prefix used while the best-possible calculation task is running, suffixed with the
  // cluster name. Makes failure logs correlatable to a specific cluster when a single controller
  // JVM hosts multiple WAGED-managed clusters.
  private static final String PARTIAL_REBALANCE_THREAD_NAME_PREFIX = "WagedPartialRebalance-";

  private final ExecutorService _bestPossibleCalculateExecutor;
  private final AssignmentManager _assignmentManager;
  private final AssignmentMetadataStore _assignmentMetadataStore;
  private final BaselineDivergenceGauge _baselineDivergenceGauge;
  // Reporter that ticks RebalanceFailureCounter plus the per-FailureCategory counters on both
  // the Rebalancer-domain and ClusterStatus-domain MBeans, and lights the reversible serving rollup
  // gauge. Owned by WagedRebalancer; injected so the runner doesn't need a direct
  // ClusterStatusMonitor reference.
  private final Consumer<HelixRebalanceException> _asyncFailureReporter;
  // Reporter invoked when a partial (serving) computation succeeds, to reset the reversible serving
  // rollup gauges. Owned by WagedRebalancer. Driving the reset from the partial outcome (not the
  // synchronous fallback path) is what keeps the rollup reversible under async mode, where partial
  // failures never reach WagedRebalancer.computeNewIdealStates' synchronous catch.
  private final Runnable _partialRebalanceSuccessReporter;
  private final CountMetric _partialRebalanceCounter;
  private final LatencyMetric _partialRebalanceLatency;
  // Sub-phase latencies of _partialRebalanceLatency: time spent building the cluster model vs.
  // solving it with the assignment algorithm. Together they let a slow partial rebalance be
  // attributed to either half.
  private final LatencyMetric _partialRebalanceBuildLatency;
  private final LatencyMetric _partialRebalanceSolveLatency;
  // Count of replica placements in the new best possible assignment that differ from the previous
  // one, i.e. the churn (state-transition blast radius) this partial rebalance introduces.
  private final CountMetric _partialRebalanceReplicaMovementCounter;

  private boolean _asyncPartialRebalanceEnabled;
  private Future<Boolean> _asyncPartialRebalanceResult;
  // Captures the original exception thrown inside the executor task so we can preserve its
  // FailureCategory when re-throwing on the synchronous path. Reset before each submit.
  private final AtomicReference<HelixRebalanceException> _lastAsyncFailure = new AtomicReference<>();

  public PartialRebalanceRunner(AssignmentManager assignmentManager,
      AssignmentMetadataStore assignmentMetadataStore,
      MetricCollector metricCollector,
      Consumer<HelixRebalanceException> asyncFailureReporter,
      Runnable partialRebalanceSuccessReporter,
      boolean isAsyncPartialRebalanceEnabled) {
    _assignmentManager = assignmentManager;
    _assignmentMetadataStore = assignmentMetadataStore;
    _bestPossibleCalculateExecutor = Executors.newSingleThreadExecutor();
    _asyncFailureReporter = asyncFailureReporter;
    _partialRebalanceSuccessReporter = partialRebalanceSuccessReporter;
    _asyncPartialRebalanceEnabled = isAsyncPartialRebalanceEnabled;

    _partialRebalanceCounter = metricCollector.getMetric(
        WagedRebalancerMetricCollector.WagedRebalancerMetricNames.PartialRebalanceCounter.name(),
        CountMetric.class);
    _partialRebalanceLatency = metricCollector.getMetric(
        WagedRebalancerMetricCollector.WagedRebalancerMetricNames.PartialRebalanceLatencyGauge
            .name(),
        LatencyMetric.class);
    _partialRebalanceBuildLatency = metricCollector.getMetric(
        WagedRebalancerMetricCollector.WagedRebalancerMetricNames.PartialRebalanceBuildLatencyGauge
            .name(),
        LatencyMetric.class);
    _partialRebalanceSolveLatency = metricCollector.getMetric(
        WagedRebalancerMetricCollector.WagedRebalancerMetricNames.PartialRebalanceSolveLatencyGauge
            .name(),
        LatencyMetric.class);
    _partialRebalanceReplicaMovementCounter = metricCollector.getMetric(
        WagedRebalancerMetricCollector.WagedRebalancerMetricNames.PartialRebalanceReplicaMovementCounter
            .name(),
        CountMetric.class);
    _baselineDivergenceGauge = metricCollector.getMetric(
        WagedRebalancerMetricCollector.WagedRebalancerMetricNames.BaselineDivergenceGauge.name(),
        BaselineDivergenceGauge.class);
  }

  public void partialRebalance(ResourceControllerDataProvider clusterData, Map<String, Resource> resourceMap,
      Set<String> activeNodes, final CurrentStateOutput currentStateOutput, RebalanceAlgorithm algorithm)
      throws HelixRebalanceException {
    // If partial rebalance is async and the previous result is not completed yet,
    // do not start another partial rebalance.
    if (_asyncPartialRebalanceEnabled && _asyncPartialRebalanceResult != null
        && !_asyncPartialRebalanceResult.isDone()) {
      return;
    }

    _lastAsyncFailure.set(null);
    final String clusterName = clusterData.getClusterName();
    _asyncPartialRebalanceResult = _bestPossibleCalculateExecutor.submit(() -> {
      final Thread currentThread = Thread.currentThread();
      final String originalThreadName = currentThread.getName();
      currentThread.setName(PARTIAL_REBALANCE_THREAD_NAME_PREFIX + clusterName);
      try {
        doPartialRebalance(clusterData, resourceMap, activeNodes, algorithm,
            currentStateOutput);
      } catch (HelixRebalanceException e) {
        // Capture the original exception so the synchronous caller can preserve the
        // FailureCategory when re-throwing. The Type is intentionally NOT preserved on the
        // re-throw to keep WagedRebalancer.computeNewIdealStates' fallback decision unchanged.
        _lastAsyncFailure.set(e);
        if (_asyncPartialRebalanceEnabled) {
          // Async mode: synchronous caller will not see this exception. Tick the aggregate
          // RebalanceFailureCounter plus the per-FailureCategory counters on both MBeans via
          // the injected reporter.
          _asyncFailureReporter.accept(e);
        }
        LOG.error("Failed to calculate best possible assignment for cluster {}! category={}",
            clusterName, e.getFailureCategory(), e);
        return false;
      } finally {
        currentThread.setName(originalThreadName);
      }
      // Partial (serving) computation succeeded -- reset the reversible serving rollup gauges.
      _partialRebalanceSuccessReporter.run();
      return true;
    });
    if (!_asyncPartialRebalanceEnabled) {
      try {
        if (!_asyncPartialRebalanceResult.get()) {
          // Preserve the original FailureCategory for downstream attribution, but intentionally
          // collapse Type to FAILED_TO_CALCULATE -- this matches the pre-FailureCategory
          // behavior so WagedRebalancer.computeNewIdealStates' fallback decision is unchanged.
          HelixRebalanceException original = _lastAsyncFailure.get();
          HelixRebalanceException.FailureCategory category = original != null
              ? original.getFailureCategory()
              : HelixRebalanceException.FailureCategory.ASYNC_EXECUTION;
          throw new HelixRebalanceException("Failed to calculate for the new best possible.",
              HelixRebalanceException.Type.FAILED_TO_CALCULATE, category, original);
        }
      } catch (InterruptedException | ExecutionException e) {
        throw new HelixRebalanceException("Failed to execute new best possible calculation.",
            HelixRebalanceException.Type.FAILED_TO_CALCULATE,
            HelixRebalanceException.FailureCategory.ASYNC_EXECUTION, e);
      }
    }
  }

  /**
   * Calculate and update the Best Possible assignment
   * If the result differ from the persisted result, persist it to memory (only if the version is not stale);
   * If persisted, trigger the pipeline so that main thread logic can run again.
   */
  private void doPartialRebalance(ResourceControllerDataProvider clusterData, Map<String, Resource> resourceMap,
      Set<String> activeNodes, RebalanceAlgorithm algorithm, CurrentStateOutput currentStateOutput)
      throws HelixRebalanceException {
    LOG.info("Start calculating the new best possible assignment.");
    _partialRebalanceCounter.increment(1L);
    _partialRebalanceLatency.startMeasuringLatency();

    int newBestPossibleAssignmentVersion = -1;
    if (_assignmentMetadataStore != null) {
      newBestPossibleAssignmentVersion = _assignmentMetadataStore.getBestPossibleVersion() + 1;
    } else {
      LOG.debug("Assignment Metadata Store is null. Skip getting best possible assignment version.");
    }

    // Read the baseline from metadata store
    Map<String, ResourceAssignment> currentBaseline =
        _assignmentManager.getBaselineAssignment(_assignmentMetadataStore, currentStateOutput, resourceMap.keySet());

    // Read the best possible assignment from metadata store
    Map<String, ResourceAssignment> currentBestPossibleAssignment =
        _assignmentManager.getBestPossibleAssignment(_assignmentMetadataStore, currentStateOutput,
            resourceMap.keySet());
    ClusterModel clusterModel;
    _partialRebalanceBuildLatency.startMeasuringLatency();
    try {
      clusterModel = ClusterModelProvider
          .generateClusterModelForPartialRebalance(clusterData, resourceMap, activeNodes,
              currentBaseline, currentBestPossibleAssignment);
    } catch (Exception ex) {
      throw new HelixRebalanceException("Failed to generate cluster model for partial rebalance.",
          HelixRebalanceException.Type.INVALID_CLUSTER_STATUS,
          HelixRebalanceException.FailureCategory.INVALID_CLUSTER_CONFIG, ex);
    } finally {
      _partialRebalanceBuildLatency.endMeasuringLatency();
    }
    _partialRebalanceSolveLatency.startMeasuringLatency();
    Map<String, ResourceAssignment> newAssignment;
    try {
      newAssignment = WagedRebalanceUtil.calculateAssignment(clusterModel, algorithm);
    } finally {
      _partialRebalanceSolveLatency.endMeasuringLatency();
    }
    boolean isBestPossibleChanged = _assignmentMetadataStore != null
        && _assignmentMetadataStore.isBestPossibleChanged(newAssignment);
    // Report how many replica placements changed relative to the previous best possible assignment.
    // This best possible assignment is what actually drives state transitions, so this is the churn
    // magnitude the cluster will experience. Skip the diff pass when nothing changed.
    if (isBestPossibleChanged) {
      _partialRebalanceReplicaMovementCounter.increment(ResourceUsageCalculator
          .countReplicaMovements(currentBestPossibleAssignment, newAssignment));
    }

    // Asynchronously report baseline divergence metric before persisting to metadata store,
    // just in case if persisting fails, we still have the metric.
    // To avoid changes of the new assignment and make it safe when being used to measure baseline
    // divergence, use a deep copy of the new assignment.
    Map<String, ResourceAssignment> newAssignmentCopy = new HashMap<>();
    for (Map.Entry<String, ResourceAssignment> entry : newAssignment.entrySet()) {
      newAssignmentCopy.put(entry.getKey(), new ResourceAssignment(entry.getValue().getRecord()));
    }

    _baselineDivergenceGauge.asyncMeasureAndUpdateValue(clusterData.getAsyncTasksThreadPool(),
        currentBaseline, newAssignmentCopy);

    boolean bestPossibleUpdateSuccessful = false;
    if (isBestPossibleChanged) {
      // This will not persist the new Best Possible Assignment into ZK. It will only update the in-memory cache.
      // If this is done successfully, the new Best Possible Assignment will be persisted into ZK the next time that
      // the pipeline is triggered. We schedule the pipeline to run below.
      bestPossibleUpdateSuccessful = _assignmentMetadataStore.asyncUpdateBestPossibleAssignmentCache(newAssignment,
          newBestPossibleAssignmentVersion);
    } else {
      LOG.debug("Assignment Metadata Store is null. Skip persisting the baseline assignment.");
    }
    _partialRebalanceLatency.endMeasuringLatency();
    LOG.info("Finish calculating the new best possible assignment.");

    if (bestPossibleUpdateSuccessful) {
      LOG.info("Schedule a new rebalance after the new best possible calculation has finished.");
      RebalanceUtil.scheduleOnDemandPipeline(clusterData.getClusterName(), 0L, false);
    }
  }

  public void setPartialRebalanceAsyncMode(boolean isAsyncPartialRebalanceEnabled) {
    _asyncPartialRebalanceEnabled = isAsyncPartialRebalanceEnabled;
  }

  public boolean isAsyncPartialRebalanceEnabled() {
    return _asyncPartialRebalanceEnabled;
  }

  @Override
  public void close() {
    if (_bestPossibleCalculateExecutor != null) {
      _bestPossibleCalculateExecutor.shutdownNow();
    }
  }
}
