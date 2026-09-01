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

package org.apache.helix.guardrail.rules;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.helix.HelixDefinedState;
import org.apache.helix.PropertyKey;
import org.apache.helix.controller.rebalancer.util.WagedValidationUtil;
import org.apache.helix.guardrail.GuardrailContext;
import org.apache.helix.guardrail.ReadOnlyDataAccessor;
import org.apache.helix.guardrail.ValidationResult;
import org.apache.helix.guardrail.Violation;
import org.apache.helix.guardrail.WagedAssignmentProvider;
import org.apache.helix.model.ClusterConfig;
import org.apache.helix.model.IdealState;
import org.apache.helix.model.InstanceConfig;
import org.apache.helix.model.Partition;
import org.apache.helix.model.ResourceAssignment;
import org.apache.helix.model.ResourceConfig;
import org.apache.helix.zookeeper.datamodel.ZNRecord;

/**
 * Shared read-only WAGED what-if used by the instance-mutation rebalance-feasibility guard rails.
 * <p>
 * Several instance-scoped mutations (moving an instance out of the assignable pool, removing an
 * instance tag a WAGED resource is pinned to, &hellip;) can newly make a partition unplaceable in
 * exactly the same way: they shrink the set of instances WAGED may place replicas on. Each rule
 * differs only in how it detects the mutation and how it builds the target's mutated
 * {@link InstanceConfig} (the <em>candidate</em>); the actual feasibility check &mdash; run the real
 * {@code ReadOnlyWagedRebalancer} on current state (baseline) vs. the candidate and flag partitions
 * whose placeable replica count drops &mdash; is identical. That check lives here so the rules stay
 * thin and cannot drift apart.
 */
final class WagedRebalanceFeasibilityWhatIf {
  // Upper bound on per-partition violations enumerated in a single verdict. A large mutation can
  // under-replicate many partitions at once; a short, readable preview keeps the message actionable
  // while the trailing summary still reports the true total, so a pathological case cannot return a
  // multi-megabyte body. Ten names are enough to characterize the failure.
  static final int MAX_REPORTED_VIOLATIONS = 10;

  private WagedRebalanceFeasibilityWhatIf() {
  }

  /**
   * The WAGED resources whose placement this what-if reasons about: FULL_AUTO + WagedRebalancer
   * ideal states, excluding {@code ANY_LIVEINSTANCE} resources (their by-design N&rarr;N-1 reduction
   * when an instance leaves the pool is not a capacity deficit and must not be mistaken for one).
   * Returned empty when there is nothing for a mutation to break, letting callers short-circuit
   * before the (relatively expensive) double what-if.
   */
  static List<IdealState> collectWagedIdealStates(ReadOnlyDataAccessor dataAccessor) {
    PropertyKey.Builder keyBuilder = dataAccessor.keyBuilder();
    List<IdealState> wagedIdealStates = new ArrayList<>();
    for (IdealState idealState : dataAccessor.<IdealState>getChildValues(keyBuilder.idealStates(),
        true)) {
      if (idealState == null || !WagedValidationUtil.isWagedEnabled(idealState)) {
        continue;
      }
      if (ResourceConfig.ResourceConfigConstants.ANY_LIVEINSTANCE.name()
          .equalsIgnoreCase(idealState.getReplicas())) {
        continue;
      }
      wagedIdealStates.add(idealState);
    }
    return wagedIdealStates;
  }

  /**
   * Run the baseline-vs-candidate WAGED what-if and report partitions that lose placeable replicas.
   *
   * @param context the guard-rail context (supplies the {@link WagedAssignmentProvider}, the
   *     read-only accessor and the cluster name)
   * @param clusterConfig the cluster config to simulate against (already read by the caller)
   * @param instanceName the target instance whose config the mutation changes
   * @param currentConfig the target's current {@link InstanceConfig} (baseline)
   * @param candidateConfig the target's {@link InstanceConfig} with the mutation applied (candidate)
   * @param wagedIdealStates the non-empty WAGED ideal states from
   *     {@link #collectWagedIdealStates(ReadOnlyDataAccessor)}
   * @param mutationDescription a human-readable noun phrase for the mutation used in messages, e.g.
   *     {@code "operation EVACUATE"} or {@code "removal of instance tag(s) [heavy]"}
   * @param ruleId the reporting rule's id, used to tag every {@link Violation}
   */
  static ValidationResult evaluate(GuardrailContext context, ClusterConfig clusterConfig,
      String instanceName, InstanceConfig currentConfig, InstanceConfig candidateConfig,
      List<IdealState> wagedIdealStates, String mutationDescription, String ruleId) {
    ReadOnlyDataAccessor dataAccessor = context.getDataAccessor();
    WagedAssignmentProvider provider = context.getWagedAssignmentProvider();
    PropertyKey.Builder keyBuilder = dataAccessor.keyBuilder();

    Map<String, ResourceConfig> resourceConfigByName = new HashMap<>();
    for (ResourceConfig resourceConfig : dataAccessor.<ResourceConfig>getChildValues(
        keyBuilder.resourceConfigs(), true)) {
      if (resourceConfig != null) {
        resourceConfigByName.put(resourceConfig.getResourceName(), resourceConfig);
      }
    }
    List<ResourceConfig> wagedResourceConfigs = new ArrayList<>();
    for (IdealState idealState : wagedIdealStates) {
      ResourceConfig resourceConfig = resourceConfigByName.get(idealState.getResourceName());
      if (resourceConfig != null) {
        wagedResourceConfigs.add(resourceConfig);
      }
    }

    List<InstanceConfig> baselineInstanceConfigs =
        dataAccessor.getChildValues(keyBuilder.instanceConfigs(), true);
    List<String> liveInstances = dataAccessor.getChildNames(keyBuilder.liveInstances());
    if (liveInstances == null) {
      liveInstances = Collections.emptyList();
    }

    // Candidate instance-config list = baseline with the target replaced by its mutated copy.
    List<InstanceConfig> candidateInstanceConfigs =
        new ArrayList<>(baselineInstanceConfigs.size() + 1);
    boolean replaced = false;
    for (InstanceConfig instanceConfig : baselineInstanceConfigs) {
      if (instanceConfig != null && instanceName.equals(instanceConfig.getInstanceName())) {
        candidateInstanceConfigs.add(candidateConfig);
        replaced = true;
      } else {
        candidateInstanceConfigs.add(instanceConfig);
      }
    }
    if (!replaced) {
      // The target's config was not in the bulk instance-config read (a race with a concurrent
      // change). Keep the two simulations symmetric: the candidate must include the mutated copy,
      // and the baseline must include the target as it is now. Otherwise the diff could falsely
      // pass. Including both makes the diff reflect only this mutation.
      candidateInstanceConfigs.add(candidateConfig);
      baselineInstanceConfigs = new ArrayList<>(baselineInstanceConfigs);
      baselineInstanceConfigs.add(currentConfig);
    }

    // Simulate against a copy of the cluster config with delayed rebalance disabled, so the what-if
    // reflects the eventual steady state (every live instance participating) rather than a transient
    // delay window in which a temporarily-down-but-still-"active" instance could mask a real deficit.
    // Mirrors ResourceAssignmentOptimizerAccessor's what-if setup.
    ClusterConfig simulationClusterConfig =
        new ClusterConfig(new ZNRecord(clusterConfig.getRecord()));
    simulationClusterConfig.setDelayRebalaceEnabled(false);

    Map<String, ResourceAssignment> baseline;
    try {
      baseline = provider.computeTargetAssignment(simulationClusterConfig, baselineInstanceConfigs,
          liveInstances, wagedIdealStates, wagedResourceConfigs);
    } catch (Exception e) {
      // No baseline to compare against: the cluster may already be unable to compute a WAGED
      // assignment. We cannot attribute a deficit to this mutation, so fail closed (block) with a
      // forceable message rather than certify a write we could not validate.
      return ValidationResult.infeasible(Violation.newBuilder(ruleId)
          .message(String.format(
              "Could not compute a baseline WAGED assignment for cluster %s to validate %s on "
                  + "instance %s against (%s). The cluster may already be unable to compute a WAGED "
                  + "assignment. Resolve the cluster's rebalance health, or retry with force=true to "
                  + "override this guard rail.", context.getClusterName(), mutationDescription,
              instanceName, e.getMessage()))
          .build());
    }

    Map<String, ResourceAssignment> candidate;
    try {
      candidate = provider.computeTargetAssignment(simulationClusterConfig, candidateInstanceConfigs,
          liveInstances, wagedIdealStates, wagedResourceConfigs);
    } catch (Exception e) {
      // Applying the mutation makes WAGED unable to compute any assignment at all (e.g. a
      // cluster-wide CAPACITY_DEFICIT) -- the strongest signal that it breaks placement.
      return ValidationResult.infeasible(Violation.newBuilder(ruleId)
          .message(String.format(
              "Applying %s to instance %s makes the WAGED rebalancer unable to compute an "
                  + "assignment for cluster %s (%s), which would stall the cluster-wide WAGED "
                  + "rebalance. Free up assignable capacity first, or retry with force=true if this "
                  + "is an intentional operational override.", mutationDescription, instanceName,
              context.getClusterName(), e.getMessage()))
          .build());
    }

    // Flag only partitions that lose placeable replicas as a result of the mutation.
    List<Violation> violations = new ArrayList<>();
    int totalViolations = 0;
    List<String> resourceNames = new ArrayList<>(baseline.keySet());
    Collections.sort(resourceNames);
    for (String resourceName : resourceNames) {
      ResourceAssignment baselineAssignment = baseline.get(resourceName);
      if (baselineAssignment == null) {
        continue;
      }
      ResourceAssignment candidateAssignment = candidate.get(resourceName);
      List<Partition> partitions = new ArrayList<>(baselineAssignment.getMappedPartitions());
      partitions.sort(Comparator.comparing(Partition::getPartitionName));
      for (Partition partition : partitions) {
        int baselineReplicas = countPlacedReplicas(baselineAssignment.getReplicaMap(partition));
        int candidateReplicas = candidateAssignment == null ? 0
            : countPlacedReplicas(candidateAssignment.getReplicaMap(partition));
        if (candidateReplicas < baselineReplicas) {
          totalViolations++;
          // Enumerate at most MAX_REPORTED_VIOLATIONS; the overflow is summarized after the loop.
          if (violations.size() >= MAX_REPORTED_VIOLATIONS) {
            continue;
          }
          violations.add(Violation.newBuilder(ruleId)
              .resource(resourceName)
              .partition(partition.getPartitionName())
              .message(String.format(
                  "%s on instance %s reduces the placeable replicas of partition %s from %d to %d: "
                      + "the WAGED rebalancer cannot re-place all of its replicas on the remaining "
                      + "assignable instances. Add or free assignable capacity (or a compatible "
                      + "fault domain), then retry; use force=true only if the resulting "
                      + "under-replication is an accepted operational tradeoff.", mutationDescription,
                  instanceName, partition.getPartitionName(), baselineReplicas, candidateReplicas))
              .build());
        }
      }
    }

    if (violations.isEmpty()) {
      return ValidationResult.feasible();
    }
    if (totalViolations > violations.size()) {
      int reported = violations.size();
      violations.add(Violation.newBuilder(ruleId)
          .message(String.format(
              "Showing the first %d of %d partitions that would lose replicas from %s on instance "
                  + "%s; %d were omitted to bound the response size. Fix the reported capacity "
                  + "shortfall and resubmit.", reported, totalViolations, mutationDescription,
              instanceName, totalViolations - reported))
          .build());
    }
    return ValidationResult.of(violations);
  }

  /**
   * Number of replicas actually placed for a partition: instance entries whose state is a real
   * placement (anything other than {@code DROPPED}).
   */
  private static int countPlacedReplicas(Map<String, String> replicaMap) {
    if (replicaMap == null || replicaMap.isEmpty()) {
      return 0;
    }
    int count = 0;
    for (String state : replicaMap.values()) {
      if (state != null && !HelixDefinedState.DROPPED.name().equals(state)) {
        count++;
      }
    }
    return count;
  }
}
