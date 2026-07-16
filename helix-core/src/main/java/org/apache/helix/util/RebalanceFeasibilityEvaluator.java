package org.apache.helix.util;

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
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.helix.HelixException;
import org.apache.helix.controller.rebalancer.util.WagedValidationUtil;
import org.apache.helix.model.ClusterConfig;
import org.apache.helix.model.InstanceConfig;

/**
 * Evaluates whether a proposed cluster mutation (dropping an instance, shrinking capacity,
 * marking an instance EVACUATE, or onboarding a resource) would leave the cluster in a
 * rebalanceable state. It asserts post-conditions over a computed assignment that the WAGED /
 * FULL_AUTO rebalancer would produce for the proposed state, without applying the mutation.
 *
 * <p>The assignment computation itself is performed by the existing read-only rebalancer
 * primitives ({@code HelixUtil#getTargetAssignmentForWagedFullAuto} /
 * {@code HelixUtil#getIdealAssignmentForFullAuto}); this class is the ZooKeeper-free invariant
 * checker applied to the result.
 */
public class RebalanceFeasibilityEvaluator {

  /**
   * Checks the minimum-active-replica post-condition for a single resource's computed assignment.
   *
   * @param resourceName       the resource being checked
   * @param partitionStateMap  computed assignment for the resource: partition -> (instance ->
   *                           state)
   * @param minActiveReplicas  the resource's required minimum active replicas; {@code -1} means no
   *                           constraint (the check is skipped)
   * @param unhealthyStates    states that do NOT count as an active replica (e.g. OFFLINE, ERROR,
   *                           DROPPED), as defined by
   *                           {@code InstanceValidationUtil#getUnhealthyStates}
   * @return a {@link FeasibilityResult} listing every partition that would fall below the threshold
   */
  public FeasibilityResult checkMinActiveReplicas(String resourceName,
      Map<String, Map<String, String>> partitionStateMap, int minActiveReplicas,
      Set<String> unhealthyStates) {
    // -1 means the resource declares no min-active-replica constraint; nothing to enforce.
    if (minActiveReplicas < 0) {
      return FeasibilityResult.feasible();
    }

    List<FeasibilityViolation> violations = new ArrayList<>();
    for (Map.Entry<String, Map<String, String>> partitionEntry : partitionStateMap.entrySet()) {
      String partitionName = partitionEntry.getKey();
      int activeReplicas = 0;
      for (String state : partitionEntry.getValue().values()) {
        if (!unhealthyStates.contains(state)) {
          activeReplicas++;
        }
      }
      if (activeReplicas < minActiveReplicas) {
        violations.add(FeasibilityViolation.minActiveReplica(resourceName, partitionName,
            activeReplicas, minActiveReplicas));
      }
    }

    return violations.isEmpty() ? FeasibilityResult.feasible() : FeasibilityResult.of(violations);
  }

  /**
   * Checks the no-unassigned-partition post-condition: every expected partition of a resource must
   * be assigned to at least one instance in the computed assignment.
   *
   * @param resourceName        the resource being checked
   * @param partitionStateMap   computed assignment for the resource: partition -> (instance ->
   *                            state)
   * @param expectedPartitions  the full set of partitions the resource is expected to have
   * @return a {@link FeasibilityResult} listing every partition that would be left unassigned
   */
  public FeasibilityResult checkNoUnassignedPartitions(String resourceName,
      Map<String, Map<String, String>> partitionStateMap, Set<String> expectedPartitions) {
    List<FeasibilityViolation> violations = new ArrayList<>();
    for (String partition : expectedPartitions) {
      Map<String, String> stateByInstance = partitionStateMap.get(partition);
      if (stateByInstance == null || stateByInstance.isEmpty()) {
        violations.add(FeasibilityViolation.unassignedPartition(resourceName, partition));
      }
    }

    return violations.isEmpty() ? FeasibilityResult.feasible() : FeasibilityResult.of(violations);
  }

  /**
   * Checks the WAGED capacity post-condition for a set of proposed instance configs: every
   * instance must declare all of the cluster's required capacity keys. Reuses the canonical
   * {@code WagedValidationUtil#validateAndGetInstanceCapacity} so the verdict matches what the
   * rebalancer would accept. A cluster with no capacity keys configured (non-WAGED) is a no-op.
   *
   * @param clusterConfig    the (proposed) cluster config defining required capacity keys
   * @param instanceConfigs  the (proposed) instance configs to validate
   * @return a {@link FeasibilityResult} listing every instance that would break capacity validation
   */
  public FeasibilityResult checkInstanceCapacities(ClusterConfig clusterConfig,
      List<InstanceConfig> instanceConfigs) {
    List<String> requiredCapacityKeys = clusterConfig.getInstanceCapacityKeys();
    // No capacity keys configured (non-WAGED cluster): nothing to enforce.
    if (requiredCapacityKeys == null || requiredCapacityKeys.isEmpty()) {
      return FeasibilityResult.feasible();
    }

    List<FeasibilityViolation> violations = new ArrayList<>();
    for (InstanceConfig instanceConfig : instanceConfigs) {
      try {
        WagedValidationUtil.validateAndGetInstanceCapacity(clusterConfig, instanceConfig);
      } catch (HelixException e) {
        violations.add(
            FeasibilityViolation.capacity(instanceConfig.getInstanceName(), e.getMessage()));
      }
    }

    return violations.isEmpty() ? FeasibilityResult.feasible() : FeasibilityResult.of(violations);
  }
}
