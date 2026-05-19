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
