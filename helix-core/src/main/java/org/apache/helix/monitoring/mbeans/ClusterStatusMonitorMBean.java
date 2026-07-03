package org.apache.helix.monitoring.mbeans;

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

import org.apache.helix.monitoring.SensorNameProvider;

public interface ClusterStatusMonitorMBean extends SensorNameProvider {

  /**
   * @return number of instances that are down (non-live instances)
   */
  long getDownInstanceGauge();

  /**
   * @return total number of instances
   */
  long getInstancesGauge();

  /**
   * @return number of disabled instances
   */
  long getDisabledInstancesGauge();

  /**
   * @return number of disabled partitions
   */
  long getDisabledPartitionsGauge();

  /**
   * @return 1 if rebalance failed; 0 if rebalance did not fail
   */
  long getRebalanceFailureGauge();

  /**
   * The max message queue size across all instances including controller
   * @return
   */
  long getMaxMessageQueueSizeGauge();

  /**
   * The sum of all message queue sizes for instances in this cluster
   * @return
   */
  long getInstanceMessageQueueBacklog();

  /**
   * Total count of all messages that have not been completed
   * after their expected completion time for instances in this cluster
   * @return
   */
  long getTotalPastDueMessageGauge();

  /**
   * @return 1 if cluster is enabled, otherwise 0
   */
  long getEnabled();

  /**
   * @return 1 if cluster is in maintenance mode, otherwise 0
   */
  long getMaintenance();

  /**
   * @return 1 if cluster is paused, otherwise 0
   */
  long getPaused();

  /**
   * @return The number of failures during rebalance pipeline.
   */
  long getRebalanceFailureCounter();

  /**
   * @return The number of continuous resource rebalance failure count
   */
  long getContinuousResourceRebalanceFailureCount();

  /**
   * @return The number of continuous task rebalance failure count
   */
  long getContinuousTaskRebalanceFailureCount();

  /**
   * Backlog of the DEFAULT controller cluster-event pipeline (events enqueued but not yet
   * processed). Stays near 0 on a healthy controller because the pipeline drains quickly and the
   * queue dedups by event type; climbs when the controller holds leadership but has stopped
   * processing events ("zombie leader").
   * <p>
   * Depth alone is ambiguous: under load a healthy controller also reads &gt; 0 (events queue
   * behind the in-flight pipeline run), but it keeps draining them, so
   * {@code ClusterEventStatus...TotalProcessed.EventCounter} advances. A wedged controller instead
   * shows depth stuck &gt; 0 with that counter flat. The alert threshold and windowing belong in
   * the alerting layer, not here.
   * @return The current DEFAULT controller event queue size.
   */
  long getControllerEventQueueSizeGauge();

  /**
   * Reversible 0/1 wedged-controller ("zombie leader") gauge. 1 when the DEFAULT event queue is
   * non-empty but no pipeline run has completed within the stall threshold, i.e. the controller
   * holds events but is not processing them; 0 when idle (empty queue) or actively draining.
   * Unlike the raw queue size, this is producer-rate-independent and does not false-positive on a
   * busy-but-progressing controller. Gate EKG/alerts on {@code == 1}.
   * @return 1 if the controller pipeline appears wedged, otherwise 0.
   */
  long getControllerPipelineStalledGauge();

  // ---- WAGED failure-category counters (mirror of WagedRebalancerMetricCollector) ----
  // Each WAGED HelixRebalanceException increments exactly one of these. The pair
  // {WagedCustomerActionableFailureCounter, WagedInternalFailureCounter} is the recommended
  // rollup signal for alert routing.

  /**
   * @return Number of WAGED failures attributable to customer-controlled cluster/resource
   *         configuration (sum of capacity, candidate-node, resource-config, cluster-config).
   */
  long getWagedCustomerActionableFailureCounter();

  /**
   * @return Number of WAGED failures attributable to Helix-controlled infrastructure
   *         (metadata store, algorithm engine, async executor, unknown).
   */
  long getWagedInternalFailureCounter();

  /** @return Number of WAGED failures caused by cluster capacity being insufficient. */
  long getWagedFailureCapacityDeficitCounter();

  /** @return Number of WAGED failures where no candidate node satisfied all hard constraints. */
  long getWagedFailureNoCandidateNodeCounter();

  /** @return Number of WAGED failures caused by invalid resource configuration. */
  long getWagedFailureInvalidResourceConfigCounter();

  /** @return Number of WAGED failures caused by invalid cluster/instance configuration. */
  long getWagedFailureInvalidClusterConfigCounter();

  /** @return Number of WAGED failures caused by assignment-metadata-store I/O. */
  long getWagedFailureMetadataStoreIoCounter();

  /** @return Number of WAGED failures inside the constraint algorithm internals. */
  long getWagedFailureAlgorithmInternalCounter();

  /** @return Number of WAGED failures originating in the async runner execution. */
  long getWagedFailureAsyncExecutionCounter();

  /** @return Number of WAGED failures with no specific category attribution. */
  long getWagedFailureUnknownCounter();

  /**
   * @return 1 if the most recent WAGED rebalance returned the last-known-good fallback assignment
   *         instead of a freshly computed one; 0 otherwise. A sustained 1 indicates WAGED is
   *         silently serving stale assignments and the underlying failure should be investigated.
   */
  long getWagedFallbackInUseGauge();

  /**
   * Reversible rollup of {@link #getWagedCustomerActionableFailureCounter()}: 1 while WAGED's most
   * recent SERVING (partial) computation failed for a customer-controlled reason (capacity /
   * candidate-node / resource or cluster config), 0 once a later partial computation succeeds.
   * Scoped to the serving phase so a stale-baseline-only failure does not page the customer. Unlike
   * the counter, this resets on recovery -- alert on {@code == 1 for Xm} to page the customer while
   * serving is persistently failing.
   * @return 1 if WAGED serving is currently failing for a customer-actionable reason; 0 otherwise.
   */
  long getWagedCustomerActionableFailureGauge();

  /**
   * Reversible rollup of {@link #getWagedInternalFailureCounter()}: 1 while WAGED's most recent
   * SERVING (partial) computation failed for a Helix-controlled reason (metadata store / algorithm /
   * async / unknown), 0 once a later partial computation succeeds. Alert on {@code == 1 for Xm} to
   * page Helix oncall.
   * @return 1 if WAGED serving is currently failing for a Helix-internal reason; 0 otherwise.
   */
  long getWagedInternalFailureGauge();

  /**
   * Reversible gauge for the Baseline (global) computation: 1 while WAGED's most recent Baseline
   * computation failed, 0 once a later Baseline computation succeeds. Owned by the GLOBAL_BASELINE
   * phase. This is the latent signal -- serving can be healthy (partial succeeds off the last-good
   * baseline) while WAGED can no longer recompute the ideal target, which will bite on the next
   * disruption. Lower urgency than the serving gauges: alert on {@code == 1 for 1h} as a ticket, not
   * a page. The specific blocking reason is available from the per-HardConstraint counters.
   * @return 1 if WAGED Baseline computation is currently failing; 0 otherwise.
   */
  long getWagedBaselineComputeFailingGauge();

  /**
   * Reversible gauge for the delayed-rebalance-overwrite phase: 1 while the most recent overwrite
   * computation failed, 0 once a later one succeeds or is not needed. Owned by the
   * DELAYED_REBALANCE_OVERWRITES phase -- its only dedicated reversible signal (it otherwise shares
   * getWagedFallbackInUseGauge() with emergency). This phase is the temporary, non-persisted
   * min-active-replica top-up applied during the delayed window, so a sustained 1 means partitions
   * below minActiveReplicas cannot be topped up while their instances are offline-yet-active. The
   * specific blocking reason is available from the per-HardConstraint counters.
   * @return 1 if the WAGED delayed-rebalance-overwrite computation is currently failing; 0 otherwise.
   */
  long getWagedRebalanceOverwriteFailingGauge();

  // ---- WAGED hard-constraint failure sub-breakdown (subset of WagedFailureNoCandidateNodeCounter) ----
  // When a partition cannot find any eligible node, every hard constraint that rejected at least
  // one candidate gets its counter incremented once for that partition. These sub-dimensions let
  // operators distinguish fault-zone failures from tag failures from capacity failures within the
  // broader "no candidate node" bucket.

  /** @return Partitions that failed placement because the fault-zone constraint rejected every node. */
  long getWagedHardConstraintFaultZoneFailureCounter();

  /** @return Partitions that failed placement because per-node capacity constraints rejected every node. */
  long getWagedHardConstraintNodeCapacityFailureCounter();

  /** @return Partitions that failed placement because the max-partitions-per-instance limit rejected every node. */
  long getWagedHardConstraintNodeMaxPartitionLimitFailureCounter();

  /** @return Partitions that failed placement because every candidate instance was inactive. */
  long getWagedHardConstraintReplicaActivateFailureCounter();

  /** @return Partitions that failed placement because the same-partition-on-instance rule rejected every node. */
  long getWagedHardConstraintSamePartitionOnInstanceFailureCounter();

  /** @return Partitions that failed placement because no instance had the required group tag. */
  long getWagedHardConstraintValidGroupTagFailureCounter();

  /** @return Partitions that failed placement due to a hard constraint with no specific type tag. */
  long getWagedHardConstraintUnknownFailureCounter();

  // ---- WAGED per-HardConstraint "currently blocking" gauges (reversible) ----
  // Each is 1 while that hard constraint blocked placement in the most recent WAGED computation and
  // 0 once a later computation places everything. Unlike the monotonic counters above, these reset
  // on recovery, so a value sustained at 1 means the reason is *currently and persistently* blocking
  // (alert on `== 1 for Xm`), while a transient blip falls back to 0 on the next clean run -- the
  // per-reason transient-vs-persistent discriminator the cumulative counters cannot provide.
  //
  // SCOPE: these gauges reflect the SERVING phases -- PARTIAL and EMERGENCY -- which produce the
  // assignment that is actually persisted and served. WAGED runs calculate() up to four times per
  // pipeline (baseline, emergency, delayed-overwrite, partial); a reversible gauge must not be shared
  // with a phase whose health it does not represent, or a clean run of one phase would clobber (mask)
  // a failing run of another. Partial and emergency do not race: within a pass emergency runs first
  // and, when it fails, throws before partial is reached -- so partial does not run, making emergency
  // the sole serving writer exactly when it is the phase that failed (its per-reason attribution would
  // otherwise be lost, since node-down-can't-reassign is precisely an emergency failure). The
  // non-serving phases are intentionally NOT wired to these gauges:
  //   - Baseline (global) runs concurrently with partial on a separate executor and computes the
  //     from-scratch ideal, not the served assignment; its failures surface via the binary
  //     getWagedBaselineComputeFailingGauge() (no per-reason breakdown) plus the per-HardConstraint
  //     counters above.
  //   - Delayed-overwrite is a temporary, non-persisted top-up; its failures surface via
  //     getWagedFallbackInUseGauge() plus the per-HardConstraint counters above.
  // So to attribute a per-reason failure outside the serving path, read the monotonic counters; these
  // reversible gauges answer specifically "which reason is blocking the *served* assignment right now".

  /** @return 1 if the fault-zone constraint is currently blocking placement; 0 otherwise. */
  long getWagedHardConstraintFaultZoneBlockingGauge();

  /** @return 1 if per-node capacity is currently blocking placement; 0 otherwise. */
  long getWagedHardConstraintNodeCapacityBlockingGauge();

  /** @return 1 if the max-partitions-per-instance limit is currently blocking placement; 0 otherwise. */
  long getWagedHardConstraintNodeMaxPartitionLimitBlockingGauge();

  /** @return 1 if replica-activation (inactive instances) is currently blocking placement; 0 otherwise. */
  long getWagedHardConstraintReplicaActivateBlockingGauge();

  /** @return 1 if the same-partition-on-instance rule is currently blocking placement; 0 otherwise. */
  long getWagedHardConstraintSamePartitionOnInstanceBlockingGauge();

  /** @return 1 if the required-group-tag constraint is currently blocking placement; 0 otherwise. */
  long getWagedHardConstraintValidGroupTagBlockingGauge();

  /** @return 1 if a hard constraint with no specific type tag is currently blocking placement; 0 otherwise. */
  long getWagedHardConstraintUnknownBlockingGauge();

  /**
   * Cluster-wide estimated max capacity utilization for WAGED-managed resources, derived from the
   * current assignment as {@code max} over capacity keys of
   * {@code sum(replica usage) / sum(node capacity)}.
   * <ul>
   *   <li>{@code 0.0} - cluster is empty, or no WAGED capacity is configured (no signal).</li>
   *   <li>{@code 1.0} - the most-constrained capacity dimension is exactly full.</li>
   *   <li>{@code > 1.0} - the cluster is over-subscribed on at least one capacity dimension.</li>
   * </ul>
   * This is a cluster aggregate ({@code sum(usage) / sum(capacity)}), NOT the maximum of the
   * per-instance {@code MaxCapacityUsageGauge} values, so a few hot instances can be averaged out
   * by idle ones. Use it as an early near-capacity signal that ramps up before WAGED begins
   * failing placement with capacity-deficit / node-capacity errors.
   *
   * @return cluster-wide estimated max capacity utilization ({@code >= 0.0})
   */
  double getEstimatedMaxClusterCapacityUsageGauge();

  /**
   * @return number of all resources in this cluster
   */
  long getTotalResourceGauge();

  /**
   * @return number of all partitions in this cluster
   */
  long getTotalPartitionGauge();

  /**
   * @return number of all partitions in this cluster that have errors
   */
  long getErrorPartitionGauge();

  /**
   * @return number of all partitions in this cluster without any top-state replicas
   */
  long getMissingTopStatePartitionGauge();

  /**
   * @return number of all partitions in this cluster without enough active replica
   */
  long getMissingMinActiveReplicaPartitionGauge();

  /**
   * @return number of all partitions in this cluster withouth enough expected replica
   */
  long getMissingReplicaPartitionGauge();

  /**
   * @return number of all partitions in this cluster whose ExternalView and IdealState have discrepancies
   */
  long getDifferenceWithIdealStateGauge();

  /**
   * @return number of sent state transition messages in this cluster
   */
  long getStateTransitionCounter();

  /**
   * @return number of pending state transitions in this cluster
   */
  long getPendingStateTransitionGuage();

  /**
   * @return number of resources will only do downward state transition because the number of ERROR
   * state partition is larger than configured threshold (default is 1).
   */
  long getNumOfResourcesRebalanceThrottledGauge();

  /**
   * @return number of instances currently in ENABLE operation
   */
  long getInstancesInOperationEnableGauge();

  /**
   * @return number of instances currently in DISABLE operation
   */
  long getInstancesInOperationDisableGauge();

  /**
   * @return number of instances currently in EVACUATE operation
   */
  long getInstancesInOperationEvacuateGauge();

  /**
   * @return number of instances currently in SWAP_IN operation
   */
  long getInstancesInOperationSwapInGauge();

  /**
   * @return number of instances currently in UNKNOWN operation
   */
  long getInstancesInOperationUnknownGauge();
}
