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
import org.apache.helix.constants.InstanceConstants;
import org.apache.helix.controller.rebalancer.util.WagedValidationUtil;
import org.apache.helix.guardrail.GuardrailContext;
import org.apache.helix.guardrail.GuardrailRule;
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
 * Guard rail that blocks a {@code setInstanceOperation} request when draining the target instance out
 * of the WAGED <em>assignable pool</em> would leave one or more partitions unable to place all their
 * replicas &mdash; i.e. the operation would cause a WAGED rebalance failure. Catching it before the ZK
 * write turns a silent, cluster-wide {@code CAPACITY_DEFICIT} into an actionable {@code 400}.
 * <p>
 * <b>Trigger.</b> WAGED only places replicas on instances where {@link InstanceConfig#isAssignable()}
 * is true, so an operation can newly break placement only when it moves the target <em>out</em> of the
 * pool (currently assignable, proposed not). The rule derives that from {@code isAssignable()} rather
 * than a hard-coded operation list, so it covers the capacity-reducing transitions
 * ({@code ENABLE|DISABLE -> EVACUATE|UNKNOWN}) and skips operations that keep or return the instance to
 * the pool ({@code ENABLE}, {@code DISABLE}, {@code SWAP_IN}) or act on an already non-assignable one.
 * <p>
 * <b>Check.</b> The rule runs the read-only WAGED what-if (via the injected
 * {@link WagedAssignmentProvider}) twice &mdash; once on current state (baseline) and once with the
 * operation applied to a copy of the target's {@link InstanceConfig} (candidate) &mdash; and flags only
 * partitions whose placeable replica count drops between them, so a pre-existing deficit is never
 * blamed on this operation. Because the what-if runs the real {@code ReadOnlyWagedRebalancer}, the diff
 * already reflects WAGED's own hard constraints (capacity, replica-count, fault-zone); the rule adds no
 * separate capacity math of its own.
 * <p>
 * <b>Behavior.</b> Opt-in via
 * {@link ClusterConfig#setInstanceOperationRebalanceGuardrailEnabled(boolean)}, disabled by default;
 * the disabled path returns before any simulation or ZK read, doubling as a kill switch. Because
 * draining a node is often operationally mandatory (failing hardware, decommission), the verdict is
 * overridable with {@code force=true}. It is best-effort admission control, not a serialized invariant:
 * computed from a snapshot (concurrent drains are not mutually serialized), it exempts
 * {@code ANY_LIVEINSTANCE} resources (their by-design N&rarr;N-1 reduction is not a deficit) and fails
 * closed with a force-able message if the baseline what-if itself cannot be computed.
 */
public class InstanceOperationRebalanceFeasibilityGuardrailRule implements GuardrailRule {
  public static final String RULE_ID = "INSTANCE_OPERATION_CAUSES_WAGED_UNPLACEABLE";

  // Upper bound on the number of per-partition violations enumerated in a single verdict. Draining a
  // large instance can under-replicate many partitions at once; a short, readable preview keeps the
  // message actionable while the trailing summary still reports the true total, so a pathological
  // case cannot return a multi-megabyte body. Ten names are enough to characterize the failure.
  private static final int MAX_REPORTED_VIOLATIONS = 10;

  @Override
  public String getId() {
    return RULE_ID;
  }

  @Override
  public ValidationResult validate(GuardrailContext context) {
    String instanceName = context.getInstanceName();
    InstanceConstants.InstanceOperation proposedOp = context.getProposedInstanceOperation();
    if (instanceName == null || proposedOp == null) {
      // Not an instance-operation mutation; nothing for this rule to certify.
      return ValidationResult.feasible();
    }

    WagedAssignmentProvider provider = context.getWagedAssignmentProvider();
    if (provider == null) {
      // No what-if seam was supplied, so this call is not wired for simulation. Certify feasible
      // rather than block every setInstanceOperation on a wiring gap; the endpoints that intend to
      // enforce this rule always inject a provider (covered by tests).
      return ValidationResult.feasible();
    }

    ReadOnlyDataAccessor dataAccessor = context.getDataAccessor();
    PropertyKey.Builder keyBuilder = dataAccessor.keyBuilder();

    ClusterConfig clusterConfig = dataAccessor.getProperty(keyBuilder.clusterConfig());
    if (clusterConfig == null) {
      // No cluster config to simulate against; defer to downstream validation.
      return ValidationResult.feasible();
    }
    if (!clusterConfig.isInstanceOperationRebalanceGuardrailEnabled()) {
      // Opt-in guard rail, disabled by default. Returning here (before the WAGED what-if and the
      // fail-closed reads below) is also the kill switch: disabling the rule via ClusterConfig backs
      // it out for every caller with a single config change.
      return ValidationResult.feasible();
    }

    InstanceConfig currentConfig =
        dataAccessor.getProperty(keyBuilder.instanceConfig(instanceName));
    if (currentConfig == null) {
      // No config to change; let the write path reject a missing instance.
      return ValidationResult.feasible();
    }
    if (!currentConfig.isAssignable()) {
      // The instance is already outside the assignable pool, so the operation removes no capacity
      // (covers SWAP_IN and any change out of an already non-assignable state).
      return ValidationResult.feasible();
    }

    // Candidate config = the target with the proposed operation applied. Copy the ZNRecord so the
    // baseline config object (read above and reused below) is never mutated.
    InstanceConfig candidateConfig = new InstanceConfig(new ZNRecord(currentConfig.getRecord()));
    candidateConfig.setInstanceOperation(proposedOp);
    if (candidateConfig.isAssignable()) {
      // The operation keeps the instance in the assignable pool (ENABLE / DISABLE), so no replicas
      // need to relocate and WAGED placement feasibility is unchanged.
      return ValidationResult.feasible();
    }

    // From here: currently assignable, proposed non-assignable -> the operation drains this
    // instance's capacity from the pool, forcing its replicas elsewhere. Simulate to see if they fit.

    List<IdealState> wagedIdealStates = new ArrayList<>();
    for (IdealState idealState : dataAccessor.<IdealState>getChildValues(keyBuilder.idealStates(),
        true)) {
      // Reuse the controller's own WAGED test (FULL_AUTO + WagedRebalancer) rather than a local check.
      if (!WagedValidationUtil.isWagedEnabled(idealState)) {
        continue;
      }
      // ANY_LIVEINSTANCE resources keep exactly one replica per live instance, so removing an
      // instance simply drops that instance's own replica -- no replica has to relocate onto the
      // remaining instances, hence such a resource can never be made unplaceable by this operation.
      // Skip it so its intentional, by-design N->N-1 reduction is not mistaken for a capacity
      // deficit (which would be a false rejection).
      if (ResourceConfig.ResourceConfigConstants.ANY_LIVEINSTANCE.name()
          .equalsIgnoreCase(idealState.getReplicas())) {
        continue;
      }
      wagedIdealStates.add(idealState);
    }
    if (wagedIdealStates.isEmpty()) {
      // No WAGED resources: there is no WAGED global rebalance for this operation to break.
      return ValidationResult.feasible();
    }

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
      // and the baseline must include the target as it is now (assignable). Otherwise both pools
      // would be effectively identical -- WAGED ignores the non-assignable candidate -- and the diff
      // would falsely pass. Including both makes the diff reflect only this operation.
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
      // assignment. We cannot attribute a deficit to this operation, so fail closed (block) with a
      // forceable message rather than certify a write we could not validate.
      return ValidationResult.infeasible(Violation.newBuilder(RULE_ID)
          .message(String.format(
              "Could not compute a baseline WAGED assignment for cluster %s to validate operation %s "
                  + "on instance %s against (%s). The cluster may already be unable to compute a WAGED "
                  + "assignment. Resolve the cluster's rebalance health, or retry with force=true to "
                  + "override this guard rail.", context.getClusterName(), proposedOp, instanceName,
              e.getMessage()))
          .build());
    }

    Map<String, ResourceAssignment> candidate;
    try {
      candidate = provider.computeTargetAssignment(simulationClusterConfig, candidateInstanceConfigs,
          liveInstances, wagedIdealStates, wagedResourceConfigs);
    } catch (Exception e) {
      // Applying the operation makes WAGED unable to compute any assignment at all (e.g. a
      // cluster-wide CAPACITY_DEFICIT) -- the strongest signal that it breaks placement.
      return ValidationResult.infeasible(Violation.newBuilder(RULE_ID)
          .message(String.format(
              "Applying operation %s to instance %s makes the WAGED rebalancer unable to compute an "
                  + "assignment for cluster %s (%s), which would stall the cluster-wide WAGED "
                  + "rebalance. Free up assignable capacity first, or retry with force=true if this "
                  + "is an intentional operational override.", proposedOp, instanceName,
              context.getClusterName(), e.getMessage()))
          .build());
    }

    // Flag only partitions that lose placeable replicas as a result of the operation.
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
          violations.add(Violation.newBuilder(RULE_ID)
              .resource(resourceName)
              .partition(partition.getPartitionName())
              .message(String.format(
                  "Operation %s on instance %s reduces the placeable replicas of partition %s from %d "
                      + "to %d: the WAGED rebalancer cannot re-place all of its replicas on the "
                      + "remaining assignable instances. Add or free assignable capacity (or a "
                      + "compatible fault domain), then retry; use force=true only if the resulting "
                      + "under-replication is an accepted operational tradeoff.", proposedOp,
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
      violations.add(Violation.newBuilder(RULE_ID)
          .message(String.format(
              "Showing the first %d of %d partitions that would lose replicas from operation %s on "
                  + "instance %s; %d were omitted to bound the response size. Fix the reported "
                  + "capacity shortfall and resubmit.", reported, totalViolations, proposedOp,
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
