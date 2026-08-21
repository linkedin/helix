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
import org.apache.helix.controller.rebalancer.waged.WagedRebalancer;
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
 * Guard rail that blocks a {@code setInstanceOperation} request when moving the target instance out
 * of the WAGED <em>assignable pool</em> would leave one or more partitions unable to place all their
 * replicas &mdash; i.e. the operation would cause a WAGED rebalance failure.
 * <p>
 * <b>Which operations this covers, and why it needs no hard-coded list.</b> WAGED only ever places
 * replicas on instances where {@link InstanceConfig#isAssignable()} is true (the assignable pool is
 * exactly {@code InstanceConstants.ASSIGNABLE_INSTANCE_OPERATIONS = {ENABLE, DISABLE}}). An operation
 * can only force replicas to be re-placed &mdash; and therefore can only newly break placement
 * &mdash; when it takes the instance <em>out</em> of that pool: the target is currently assignable and
 * the proposed operation is not. Rather than enumerate operations, this rule derives the trigger from
 * {@code isAssignable()} directly, so it automatically covers the capacity-reducing transitions
 * ({@code ENABLE|DISABLE -> EVACUATE|UNKNOWN}) and automatically excludes the others:
 * <ul>
 *   <li><b>ENABLE</b> keeps/returns the instance to the pool &mdash; it adds capacity, never removes
 *       it.</li>
 *   <li><b>DISABLE</b> stays <em>in</em> the assignable pool: its replicas are forced OFFLINE in
 *       place rather than relocated, so placement feasibility is unchanged. (Disabling too many
 *       instances is an availability concern, enforced elsewhere, not a WAGED placement-feasibility
 *       failure.)</li>
 *   <li><b>SWAP_IN</b> is only reachable from an already non-assignable ({@code UNKNOWN}) state, so
 *       the target is not in the pool to begin with; its matching swap-out partner preserves capacity
 *       (a like-for-like trade).</li>
 *   <li>Any operation set on an <em>already</em> non-assignable instance removes no capacity.</li>
 * </ul>
 * <p>
 * <b>How the check works.</b> The rule runs the read-only WAGED what-if twice via the injected
 * {@link WagedAssignmentProvider}: once on current cluster state (the <em>baseline</em>) and once with
 * the proposed operation applied to a copy of the target's {@link InstanceConfig} (the
 * <em>candidate</em>). It then flags only partitions whose placeable replica count drops from baseline
 * to candidate. Comparing against a baseline (rather than an absolute target) means a pre-existing
 * deficit is never blamed on this operation &mdash; the rule fails a request only for the shortfall the
 * operation itself introduces. WAGED excludes the mutated instance automatically because the candidate
 * config is no longer {@code isAssignable()}, so no manual pool surgery is needed.
 * <p>
 * <b>Why up front rather than at rebalance.</b> An {@code EVACUATE}/{@code UNKNOWN} that over-drains
 * the cluster is accepted into ZooKeeper today with no capacity preflight, and only surfaces later as
 * a WAGED {@code CAPACITY_DEFICIT} that can stall the cluster-wide rebalance. Catching it before the
 * write turns a silent, cluster-wide failure into an actionable {@code 400}.
 * <p>
 * <b>force is a legitimate override here.</b> Unlike adding an unplaceable resource, draining a node is
 * often operationally mandatory (failing hardware, decommission). The messages therefore point at
 * {@code force=true} as an accepted escape hatch when the operator knowingly accepts the resulting
 * under-replication &mdash; the guard rail's job is to make that a deliberate choice, not to forbid it.
 * <p>
 * <b>Opt-in.</b> Runs only when the cluster enables it via
 * {@link ClusterConfig#setInstanceOperationRebalanceGuardrailEnabled(boolean)}; disabled by default.
 * The disabled path returns feasible before any WAGED simulation or fail-closed ZK read, so a disabled
 * cluster is never exposed to the (relatively expensive) what-if or to read failures, and the flag
 * doubles as a single-config-change kill switch to back out a false positive.
 */
public class InstanceOperationRebalanceFeasibilityGuardrailRule implements GuardrailRule {
  public static final String RULE_ID = "INSTANCE_OPERATION_CAUSES_WAGED_UNPLACEABLE";

  // Upper bound on the number of per-partition violations enumerated in a single verdict. Draining a
  // large instance can under-replicate many partitions at once; beyond this cap the extras are
  // summarized in one trailing entry so a pathological case cannot return a multi-megabyte body.
  private static final int MAX_REPORTED_VIOLATIONS = 100;

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
      if (idealState != null && WagedRebalancer.class.getName()
          .equals(idealState.getRebalancerClassName())) {
        wagedIdealStates.add(idealState);
      }
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
      // The target's config was not in the listed instance configs (a race with a concurrent
      // change); include the candidate so the simulated state reflects the proposed operation.
      candidateInstanceConfigs.add(candidateConfig);
    }

    Map<String, ResourceAssignment> baseline;
    try {
      baseline = provider.computeTargetAssignment(clusterConfig, baselineInstanceConfigs,
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
      candidate = provider.computeTargetAssignment(clusterConfig, candidateInstanceConfigs,
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
   * Number of replicas actually placed for a partition in a computed assignment: the count of
   * instance entries whose state is a real placement (anything other than {@code DROPPED}). WAGED's
   * target (preference-list) assignment does not emit {@code DROPPED}, but excluding it keeps the
   * count robust if the source ever includes drop markers.
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
