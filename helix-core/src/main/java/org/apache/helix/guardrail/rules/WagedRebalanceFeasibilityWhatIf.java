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
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

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
   * The effective {@code INSTANCE_GROUP_TAG} of each of the given WAGED resources, read from the same
   * merged (ResourceConfig-over-IdealState) view WAGED itself uses to resolve the pinning tag. WAGED
   * merges the two via
   * {@link ResourceConfig#mergeIdealStateWithResourceConfig(ResourceConfig, IdealState)} (see
   * {@code AssignableReplica}), where a tag set on the {@link ResourceConfig} wins over one on the
   * {@link IdealState}. Reading the tag off the IdealState alone would miss a resource pinned only
   * through its ResourceConfig (e.g. a task/JobConfig resource), silently skipping exactly the
   * resource a tag-removal guard rail must protect. Returned empty when no resource is pinned.
   */
  static Set<String> collectWagedInstanceGroupTags(ReadOnlyDataAccessor dataAccessor,
      List<IdealState> wagedIdealStates) {
    PropertyKey.Builder keyBuilder = dataAccessor.keyBuilder();
    Map<String, ResourceConfig> resourceConfigByName = new HashMap<>();
    for (ResourceConfig resourceConfig : dataAccessor.<ResourceConfig>getChildValues(
        keyBuilder.resourceConfigs(), true)) {
      if (resourceConfig != null) {
        resourceConfigByName.put(resourceConfig.getResourceName(), resourceConfig);
      }
    }
    Set<String> groupTags = new HashSet<>();
    for (IdealState idealState : wagedIdealStates) {
      String groupTag = ResourceConfig.mergeIdealStateWithResourceConfig(
          resourceConfigByName.get(idealState.getResourceName()), idealState).getInstanceGroupTag();
      if (groupTag != null) {
        groupTags.add(groupTag);
      }
    }
    return groupTags;
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
   * @param remedyHint a short, mutation-specific remedy fragment spliced into the operator-facing
   *     violation messages, e.g. {@code "Free up assignable capacity"} or {@code "Add the removed tag
   *     to another live instance, or lower the pinned resource's replica count"}
   * @param ruleId the reporting rule's id, used to tag every {@link Violation}
   */
  static ValidationResult evaluate(GuardrailContext context, ClusterConfig clusterConfig,
      String instanceName, InstanceConfig currentConfig, InstanceConfig candidateConfig,
      List<IdealState> wagedIdealStates, String mutationDescription, String remedyHint,
      String ruleId) {
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
                  + "rebalance. %s, or retry with force=true if this is an intentional operational "
                  + "override.", mutationDescription, instanceName, context.getClusterName(),
              e.getMessage(), remedyHint))
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
                      + "assignable instances. %s, then retry; use force=true only if the resulting "
                      + "under-replication is an accepted operational tradeoff.", mutationDescription,
                  instanceName, partition.getPartitionName(), baselineReplicas, candidateReplicas,
                  remedyHint))
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
                  + "%s; %d were omitted to bound the response size. Fix the reported shortfall and "
                  + "resubmit.", reported, totalViolations, mutationDescription,
              instanceName, totalViolations - reported))
          .build());
    }
    return ValidationResult.of(violations);
  }

  /**
   * Run a WAGED what-if for a proposed {@code updateResourceIdealState} edit and report partitions the
   * edit would leave unplaceable.
   * <p>
   * Unlike {@link #evaluate}, which perturbs one instance's config, this variant perturbs a single
   * resource's ideal state: the candidate what-if replaces (or, for a newly-WAGED resource, adds) the
   * edited resource's ideal state with the proposed one, keeping every other resource, every instance
   * config and the live set identical, so the diff reflects only the edit. Two complementary checks are
   * run:
   * <ul>
   *   <li><b>The edited resource vs. its own proposed demand.</b> The candidate placement of the edited
   *   resource is compared against the replica count the proposed ideal state asks for -- not against
   *   the baseline. This is what makes a replica <em>increase</em> the current capacity cannot satisfy a
   *   violation (the baseline placed fewer, so a baseline diff would miss it) while a legitimate replica
   *   <em>decrease</em> is not falsely flagged (the candidate places exactly the new, lower target).</li>
   *   <li><b>Every other WAGED resource vs. baseline.</b> Enlarging the edited resource's demand can
   *   steal capacity another resource needs; those are flagged the usual baseline-vs-candidate way.</li>
   * </ul>
   * Only an edit to a WAGED (FULL_AUTO + WagedRebalancer, non-{@code ANY_LIVEINSTANCE}) resource is
   * simulated; everything else -- including un-WAGEDing a resource, which only frees capacity -- returns
   * feasible. The caller is expected to short-circuit on the opt-in flag before calling this.
   *
   * @param context the guard-rail context (supplies the {@link WagedAssignmentProvider}, the read-only
   *     accessor and the cluster name)
   * @param clusterConfig the cluster config to simulate against (already read by the caller)
   * @param proposedIdealState the merged (post-write) ideal state the edit would persist
   * @param remedyHint a short remedy fragment spliced into the operator-facing messages
   * @param ruleId the reporting rule's id, used to tag every {@link Violation}
   */
  static ValidationResult evaluateIdealStateChange(GuardrailContext context,
      ClusterConfig clusterConfig, IdealState proposedIdealState, String remedyHint, String ruleId) {
    // Only a WAGED resource is placed by the WAGED rebalancer, so only a WAGED proposed ideal state is
    // a WAGED capacity concern. A non-WAGED (or ANY_LIVEINSTANCE) edit -- including un-WAGEDing a
    // resource, which only frees capacity and can never make placement infeasible -- is certified here.
    if (!WagedValidationUtil.isWagedEnabled(proposedIdealState)
        || ResourceConfig.ResourceConfigConstants.ANY_LIVEINSTANCE.name()
            .equalsIgnoreCase(proposedIdealState.getReplicas())) {
      return ValidationResult.feasible();
    }
    String targetResource = proposedIdealState.getResourceName();

    ReadOnlyDataAccessor dataAccessor = context.getDataAccessor();
    WagedAssignmentProvider provider = context.getWagedAssignmentProvider();
    PropertyKey.Builder keyBuilder = dataAccessor.keyBuilder();

    // Baseline = the cluster's current WAGED resources. Candidate = the same set with the edited
    // resource's ideal state swapped for the proposed one (or added, when the edit newly makes the
    // resource WAGED). Every other input is shared, so the diff isolates this one edit.
    List<IdealState> baselineIdealStates = collectWagedIdealStates(dataAccessor);
    List<IdealState> candidateIdealStates = new ArrayList<>(baselineIdealStates.size() + 1);
    boolean replaced = false;
    for (IdealState idealState : baselineIdealStates) {
      if (targetResource.equals(idealState.getResourceName())) {
        candidateIdealStates.add(proposedIdealState);
        replaced = true;
      } else {
        candidateIdealStates.add(idealState);
      }
    }
    if (!replaced) {
      candidateIdealStates.add(proposedIdealState);
    }

    Map<String, ResourceConfig> resourceConfigByName = new HashMap<>();
    for (ResourceConfig resourceConfig : dataAccessor.<ResourceConfig>getChildValues(
        keyBuilder.resourceConfigs(), true)) {
      if (resourceConfig != null) {
        resourceConfigByName.put(resourceConfig.getResourceName(), resourceConfig);
      }
    }
    List<ResourceConfig> baselineResourceConfigs =
        resourceConfigsFor(baselineIdealStates, resourceConfigByName);
    List<ResourceConfig> candidateResourceConfigs =
        resourceConfigsFor(candidateIdealStates, resourceConfigByName);

    List<InstanceConfig> instanceConfigs =
        dataAccessor.getChildValues(keyBuilder.instanceConfigs(), true);
    List<String> liveInstances = dataAccessor.getChildNames(keyBuilder.liveInstances());
    if (liveInstances == null) {
      liveInstances = Collections.emptyList();
    }

    // Simulate against a copy of the cluster config with delayed rebalance disabled, so the what-if
    // reflects the eventual steady state rather than a transient delay window. Mirrors
    // ResourceAssignmentOptimizerAccessor's what-if setup.
    ClusterConfig simulationClusterConfig =
        new ClusterConfig(new ZNRecord(clusterConfig.getRecord()));
    simulationClusterConfig.setDelayRebalaceEnabled(false);

    String subject = "the proposed ideal-state edit to resource " + targetResource;

    Map<String, ResourceAssignment> baseline;
    try {
      // Skip the baseline run when there is no current WAGED resource to compare against (e.g. the
      // edit newly makes the only resource WAGED): there is nothing for check 2 to diff, and check 1
      // only needs the candidate.
      baseline = baselineIdealStates.isEmpty() ? Collections.emptyMap()
          : provider.computeTargetAssignment(simulationClusterConfig, instanceConfigs, liveInstances,
              baselineIdealStates, baselineResourceConfigs);
    } catch (Exception e) {
      // No baseline to compare against: the cluster may already be unable to compute a WAGED
      // assignment. Because this rule is always on, we must not block every edit on an already-
      // unhealthy cluster -- and we cannot attribute a deficit to this edit without a baseline. Fail
      // open (certify): a deficit is only actionable when the cluster could compute a baseline and this
      // edit then regresses it (checks 1 and 2 below).
      return ValidationResult.feasible();
    }

    Map<String, ResourceAssignment> candidate;
    try {
      candidate = provider.computeTargetAssignment(simulationClusterConfig, instanceConfigs,
          liveInstances, candidateIdealStates, candidateResourceConfigs);
    } catch (Exception e) {
      // Applying the edit makes WAGED unable to compute any assignment at all (e.g. a cluster-wide
      // CAPACITY_DEFICIT) -- the strongest signal that the proposed ideal state is unplaceable.
      return ValidationResult.infeasible(Violation.newBuilder(ruleId)
          .message(String.format(
              "Applying %s makes the WAGED rebalancer unable to compute an assignment for cluster %s "
                  + "(%s), which would stall the cluster-wide WAGED rebalance. %s, or retry with "
                  + "force=true if this is an intentional operational override.", subject,
              context.getClusterName(), e.getMessage(), remedyHint))
          .build());
    }

    List<Violation> violations = new ArrayList<>();
    int totalViolations = 0;

    // Check 1: the edited resource must place every replica its proposed ideal state asks for. Compared
    // against the proposed target (not the baseline) so an under-capacity increase is caught and a
    // deliberate decrease is not falsely flagged.
    int targetReplicaCount = proposedIdealState.getReplicaCount(liveInstances.size());
    ResourceAssignment candidateTarget = candidate.get(targetResource);
    if (targetReplicaCount > 0 && candidateTarget != null) {
      List<Partition> partitions = new ArrayList<>(candidateTarget.getMappedPartitions());
      partitions.sort(Comparator.comparing(Partition::getPartitionName));
      for (Partition partition : partitions) {
        int placed = countPlacedReplicas(candidateTarget.getReplicaMap(partition));
        if (placed < targetReplicaCount) {
          totalViolations++;
          if (violations.size() < MAX_REPORTED_VIOLATIONS) {
            violations.add(Violation.newBuilder(ruleId)
                .resource(targetResource)
                .partition(partition.getPartitionName())
                .message(String.format(
                    "%s requires %d replica(s) of partition %s but only %d can be placed on the "
                        + "current cluster: the WAGED rebalancer cannot satisfy the proposed ideal "
                        + "state with the available instance capacity. %s, then retry; use force=true "
                        + "only if the resulting under-replication is an accepted operational "
                        + "tradeoff.", capitalize(subject), targetReplicaCount,
                    partition.getPartitionName(), placed, remedyHint))
                .build());
          }
        }
      }
    }

    // Check 2: no other WAGED resource may lose placeable replicas as collateral of the edit.
    List<String> resourceNames = new ArrayList<>(baseline.keySet());
    Collections.sort(resourceNames);
    for (String resourceName : resourceNames) {
      if (resourceName.equals(targetResource)) {
        continue;
      }
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
          if (violations.size() < MAX_REPORTED_VIOLATIONS) {
            violations.add(Violation.newBuilder(ruleId)
                .resource(resourceName)
                .partition(partition.getPartitionName())
                .message(String.format(
                    "%s reduces the placeable replicas of partition %s (resource %s) from %d to %d: "
                        + "the edit consumes capacity another WAGED resource needs. %s, then retry; "
                        + "use force=true only if the resulting under-replication is an accepted "
                        + "operational tradeoff.", capitalize(subject), partition.getPartitionName(),
                    resourceName, baselineReplicas, candidateReplicas, remedyHint))
                .build());
          }
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
              "Showing the first %d of %d partitions that %s would leave under-replicated; %d were "
                  + "omitted to bound the response size. Fix the reported shortfall and resubmit.",
              reported, totalViolations, subject, totalViolations - reported))
          .build());
    }
    return ValidationResult.of(violations);
  }

  // The ResourceConfigs for the given ideal states, in order, skipping resources that have none.
  private static List<ResourceConfig> resourceConfigsFor(List<IdealState> idealStates,
      Map<String, ResourceConfig> resourceConfigByName) {
    List<ResourceConfig> resourceConfigs = new ArrayList<>();
    for (IdealState idealState : idealStates) {
      ResourceConfig resourceConfig = resourceConfigByName.get(idealState.getResourceName());
      if (resourceConfig != null) {
        resourceConfigs.add(resourceConfig);
      }
    }
    return resourceConfigs;
  }

  // Upper-cases the first character so a noun-phrase subject can start a sentence.
  private static String capitalize(String text) {
    if (text == null || text.isEmpty()) {
      return text;
    }
    return Character.toUpperCase(text.charAt(0)) + text.substring(1);
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
