package org.apache.helix.monitoring.metrics;

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

import javax.management.JMException;

import org.apache.helix.HelixException;
import org.apache.helix.monitoring.mbeans.MonitorDomainNames;
import org.apache.helix.monitoring.metrics.implementation.BaselineDivergenceGauge;
import org.apache.helix.monitoring.metrics.implementation.RebalanceCounter;
import org.apache.helix.monitoring.metrics.implementation.RebalanceFailureCount;
import org.apache.helix.monitoring.metrics.implementation.RebalanceLatencyGauge;
import org.apache.helix.monitoring.metrics.model.CountMetric;
import org.apache.helix.monitoring.metrics.model.LatencyMetric;
import org.apache.helix.monitoring.metrics.model.RatioMetric;


public class WagedRebalancerMetricCollector extends MetricCollector {
  private static final String WAGED_REBALANCER_ENTITY_NAME = "WagedRebalancer";

  /**
   * This enum class contains all metric names defined for WagedRebalancer. Note that all enums are
   * in camel case for readability.
   */
  public enum WagedRebalancerMetricNames {
    // Per-stage latency metrics
    GlobalBaselineCalcLatencyGauge,
    PartialRebalanceLatencyGauge,
    EmergencyRebalanceLatencyGauge,
    RebalanceOverwriteLatencyGauge,

    // Sub-phase breakdown of the Global Baseline and Partial rebalance latencies above. Each of the
    // two phases is timed separately so a slow rebalance can be attributed to either building the
    // cluster model (constructing assignable nodes/replicas and pinning existing placement) or
    // solving it (running the constraint-based assignment algorithm).
    GlobalBaselineCalcBuildLatencyGauge,
    GlobalBaselineCalcSolveLatencyGauge,
    PartialRebalanceBuildLatencyGauge,
    PartialRebalanceSolveLatencyGauge,

    // The following latency metrics are related to AssignmentMetadataStore
    StateReadLatencyGauge,
    StateWriteLatencyGauge,

    /*
     * Gauge of the difference (state and partition allocation) between the baseline and the best
     * possible assignment.
     */
    BaselineDivergenceGauge,

    // Count of any rebalance compute failure.
    // Note the rebalancer may still be able to return the last known-good assignment on a rebalance
    // compute failure. And this fallback logic won't impact this counting.
    RebalanceFailureCounter,

    // Per-category breakdown of RebalanceFailureCounter. Each WAGED failure increments exactly one
    // of these in addition to RebalanceFailureCounter. See HelixRebalanceException.FailureCategory.
    FailureCategoryCapacityDeficitCounter,
    FailureCategoryNoCandidateNodeCounter,
    FailureCategoryInvalidResourceConfigCounter,
    FailureCategoryInvalidClusterConfigCounter,
    FailureCategoryMetadataStoreIoCounter,
    FailureCategoryAlgorithmInternalCounter,
    FailureCategoryAsyncExecutionCounter,
    FailureCategoryUnknownCounter,

    // Per-HardConstraint sub-dimension of FailureCategoryNoCandidateNodeCounter. When a partition
    // fails to find any eligible node, every hard constraint that rejected at least one candidate
    // gets its counter ticked once for that partition. See HardConstraint.Type.
    HardConstraintFaultZoneFailureCounter,
    HardConstraintNodeCapacityFailureCounter,
    HardConstraintNodeMaxPartitionLimitFailureCounter,
    HardConstraintReplicaActivateFailureCounter,
    HardConstraintSamePartitionOnInstanceFailureCounter,
    HardConstraintValidGroupTagFailureCounter,
    HardConstraintUnknownFailureCounter,

    // Waged rebalance counters.
    GlobalBaselineCalcCounter,
    PartialRebalanceCounter,
    EmergencyRebalanceCounter,
    RebalanceOverwriteCounter,

    // Count of replica placements in the new best possible assignment that changed relative to the
    // previous best possible assignment, i.e. the churn (state-transition blast radius) that the
    // partial rebalance introduces (the best possible assignment is what actually drives state
    // transitions in the cluster).
    PartialRebalanceReplicaMovementCounter
  }

  public WagedRebalancerMetricCollector(String clusterName) {
    super(MonitorDomainNames.Rebalancer.name(), clusterName, WAGED_REBALANCER_ENTITY_NAME);
    createMetrics();
    if (clusterName != null) {
      try {
        register();
      } catch (JMException e) {
        throw new HelixException("Failed to register MBean for the WagedRebalancerMetricCollector.",
            e);
      }
    }
  }

  /**
   * This constructor will create but will not register metrics. This constructor will be used in
   * case of JMException so that the rebalancer could proceed without registering and emitting
   * metrics.
   */
  public WagedRebalancerMetricCollector() {
    this(null);
  }

  /**
   * Creates and registers all metrics in MetricCollector for WagedRebalancer.
   */
  private void createMetrics() {
    // Define all metrics
    LatencyMetric globalBaselineCalcLatencyGauge =
        new RebalanceLatencyGauge(WagedRebalancerMetricNames.GlobalBaselineCalcLatencyGauge.name(),
            getResetIntervalInMs());
    LatencyMetric partialRebalanceLatencyGauge =
        new RebalanceLatencyGauge(WagedRebalancerMetricNames.PartialRebalanceLatencyGauge.name(),
            getResetIntervalInMs());
    LatencyMetric emergencyRebalanceLatencyGauge =
        new RebalanceLatencyGauge(WagedRebalancerMetricNames.EmergencyRebalanceLatencyGauge.name(),
            getResetIntervalInMs());
    LatencyMetric rebalanceOverwriteLatencyGauge =
        new RebalanceLatencyGauge(WagedRebalancerMetricNames.RebalanceOverwriteLatencyGauge.name(),
            getResetIntervalInMs());
    LatencyMetric globalBaselineCalcBuildLatencyGauge =
        new RebalanceLatencyGauge(
            WagedRebalancerMetricNames.GlobalBaselineCalcBuildLatencyGauge.name(),
            getResetIntervalInMs());
    LatencyMetric globalBaselineCalcSolveLatencyGauge =
        new RebalanceLatencyGauge(
            WagedRebalancerMetricNames.GlobalBaselineCalcSolveLatencyGauge.name(),
            getResetIntervalInMs());
    LatencyMetric partialRebalanceBuildLatencyGauge =
        new RebalanceLatencyGauge(WagedRebalancerMetricNames.PartialRebalanceBuildLatencyGauge.name(),
            getResetIntervalInMs());
    LatencyMetric partialRebalanceSolveLatencyGauge =
        new RebalanceLatencyGauge(WagedRebalancerMetricNames.PartialRebalanceSolveLatencyGauge.name(),
            getResetIntervalInMs());
    LatencyMetric stateReadLatencyGauge =
        new RebalanceLatencyGauge(WagedRebalancerMetricNames.StateReadLatencyGauge.name(),
            getResetIntervalInMs());
    LatencyMetric stateWriteLatencyGauge =
        new RebalanceLatencyGauge(WagedRebalancerMetricNames.StateWriteLatencyGauge.name(),
            getResetIntervalInMs());
    RatioMetric baselineDivergenceGauge =
        new BaselineDivergenceGauge(WagedRebalancerMetricNames.BaselineDivergenceGauge.name());
    CountMetric calcFailureCount =
        new RebalanceFailureCount(WagedRebalancerMetricNames.RebalanceFailureCounter.name());
    CountMetric failureCategoryCapacityDeficitCounter =
        new RebalanceFailureCount(WagedRebalancerMetricNames.FailureCategoryCapacityDeficitCounter.name());
    CountMetric failureCategoryNoCandidateNodeCounter =
        new RebalanceFailureCount(WagedRebalancerMetricNames.FailureCategoryNoCandidateNodeCounter.name());
    CountMetric failureCategoryInvalidResourceConfigCounter =
        new RebalanceFailureCount(WagedRebalancerMetricNames.FailureCategoryInvalidResourceConfigCounter.name());
    CountMetric failureCategoryInvalidClusterConfigCounter =
        new RebalanceFailureCount(WagedRebalancerMetricNames.FailureCategoryInvalidClusterConfigCounter.name());
    CountMetric failureCategoryMetadataStoreIoCounter =
        new RebalanceFailureCount(WagedRebalancerMetricNames.FailureCategoryMetadataStoreIoCounter.name());
    CountMetric failureCategoryAlgorithmInternalCounter =
        new RebalanceFailureCount(WagedRebalancerMetricNames.FailureCategoryAlgorithmInternalCounter.name());
    CountMetric failureCategoryAsyncExecutionCounter =
        new RebalanceFailureCount(WagedRebalancerMetricNames.FailureCategoryAsyncExecutionCounter.name());
    CountMetric failureCategoryUnknownCounter =
        new RebalanceFailureCount(WagedRebalancerMetricNames.FailureCategoryUnknownCounter.name());
    CountMetric hardConstraintFaultZoneFailureCounter =
        new RebalanceFailureCount(WagedRebalancerMetricNames.HardConstraintFaultZoneFailureCounter.name());
    CountMetric hardConstraintNodeCapacityFailureCounter =
        new RebalanceFailureCount(WagedRebalancerMetricNames.HardConstraintNodeCapacityFailureCounter.name());
    CountMetric hardConstraintNodeMaxPartitionLimitFailureCounter =
        new RebalanceFailureCount(WagedRebalancerMetricNames.HardConstraintNodeMaxPartitionLimitFailureCounter.name());
    CountMetric hardConstraintReplicaActivateFailureCounter =
        new RebalanceFailureCount(WagedRebalancerMetricNames.HardConstraintReplicaActivateFailureCounter.name());
    CountMetric hardConstraintSamePartitionOnInstanceFailureCounter =
        new RebalanceFailureCount(WagedRebalancerMetricNames.HardConstraintSamePartitionOnInstanceFailureCounter.name());
    CountMetric hardConstraintValidGroupTagFailureCounter =
        new RebalanceFailureCount(WagedRebalancerMetricNames.HardConstraintValidGroupTagFailureCounter.name());
    CountMetric hardConstraintUnknownFailureCounter =
        new RebalanceFailureCount(WagedRebalancerMetricNames.HardConstraintUnknownFailureCounter.name());
    CountMetric globalBaselineCalcCounter =
        new RebalanceCounter(WagedRebalancerMetricNames.GlobalBaselineCalcCounter.name());
    CountMetric partialRebalanceCounter =
        new RebalanceCounter(WagedRebalancerMetricNames.PartialRebalanceCounter.name());
    CountMetric emergencyRebalanceCounter =
        new RebalanceCounter(WagedRebalancerMetricNames.EmergencyRebalanceCounter.name());
    CountMetric rebalanceOverwriteCounter =
        new RebalanceCounter(WagedRebalancerMetricNames.RebalanceOverwriteCounter.name());
    CountMetric partialRebalanceReplicaMovementCounter =
        new RebalanceCounter(
            WagedRebalancerMetricNames.PartialRebalanceReplicaMovementCounter.name());

    // Add metrics to WagedRebalancerMetricCollector
    addMetric(globalBaselineCalcLatencyGauge);
    addMetric(partialRebalanceLatencyGauge);
    addMetric(emergencyRebalanceLatencyGauge);
    addMetric(rebalanceOverwriteLatencyGauge);
    addMetric(globalBaselineCalcBuildLatencyGauge);
    addMetric(globalBaselineCalcSolveLatencyGauge);
    addMetric(partialRebalanceBuildLatencyGauge);
    addMetric(partialRebalanceSolveLatencyGauge);
    addMetric(stateReadLatencyGauge);
    addMetric(stateWriteLatencyGauge);
    addMetric(baselineDivergenceGauge);
    addMetric(calcFailureCount);
    addMetric(failureCategoryCapacityDeficitCounter);
    addMetric(failureCategoryNoCandidateNodeCounter);
    addMetric(failureCategoryInvalidResourceConfigCounter);
    addMetric(failureCategoryInvalidClusterConfigCounter);
    addMetric(failureCategoryMetadataStoreIoCounter);
    addMetric(failureCategoryAlgorithmInternalCounter);
    addMetric(failureCategoryAsyncExecutionCounter);
    addMetric(failureCategoryUnknownCounter);
    addMetric(hardConstraintFaultZoneFailureCounter);
    addMetric(hardConstraintNodeCapacityFailureCounter);
    addMetric(hardConstraintNodeMaxPartitionLimitFailureCounter);
    addMetric(hardConstraintReplicaActivateFailureCounter);
    addMetric(hardConstraintSamePartitionOnInstanceFailureCounter);
    addMetric(hardConstraintValidGroupTagFailureCounter);
    addMetric(hardConstraintUnknownFailureCounter);
    addMetric(globalBaselineCalcCounter);
    addMetric(partialRebalanceCounter);
    addMetric(emergencyRebalanceCounter);
    addMetric(rebalanceOverwriteCounter);
    addMetric(partialRebalanceReplicaMovementCounter);
  }
}
