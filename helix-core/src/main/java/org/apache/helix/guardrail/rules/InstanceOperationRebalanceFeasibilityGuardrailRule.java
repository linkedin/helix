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

import java.io.IOException;
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
import org.apache.helix.constants.InstanceConstants;
import org.apache.helix.controller.rebalancer.util.WagedRebalanceUtil;
import org.apache.helix.controller.rebalancer.util.WagedValidationUtil;
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
 * <p>
 * <b>Cheap pre-checks short-circuit the common failures without a simulation.</b> Before running the
 * (relatively expensive) double WAGED what-if, two sound O(instances&times;keys) arithmetic checks run
 * against the post-operation assignable pool, each mirroring a WAGED <em>hard</em>-constraint failure
 * exactly:
 * <ul>
 *   <li><b>Replica-count pigeonhole.</b> {@code SamePartitionOnInstanceConstraint} forbids two replicas
 *       of a partition on one instance, so a resource with {@code R} replicas needs {@code R} distinct
 *       assignable instances. If fewer remain, WAGED throws {@code NO_CANDIDATE_NODE} &mdash; we block
 *       up-front.</li>
 *   <li><b>Aggregate capacity.</b> WAGED throws {@code CAPACITY_DEFICIT} when the total weight of all
 *       WAGED replicas exceeds the summed capacity of the assignable nodes for any capacity key. We
 *       recompute that same global necessary condition using WAGED's own resolvers
 *       ({@link WagedValidationUtil}/{@link WagedRebalanceUtil}) so the numbers are identical.</li>
 * </ul>
 * Both are <em>sound</em>: a block here is one the candidate simulation would also produce, just faster
 * and with a clearer message. When inputs are ambiguous (missing/malformed weights, non-enumerable
 * partitions) the pre-check yields to the full what-if rather than risk a false block, and both run only
 * when the target is currently live (an already-dead instance removes nothing from WAGED's pool).
 * <p>
 * <b>Scope and limitations.</b>
 * <ul>
 *   <li><b>Best-effort, not atomic admission control.</b> The verdict is computed from a cluster
 *       snapshot and the actual write happens afterwards, so two concurrent drains could each be
 *       certified against four-remaining-instances and both proceed. Treat this as a strong safety
 *       net that catches the common single-operation mistake, not as a serialized invariant.</li>
 *   <li><b>{@code ANY_LIVEINSTANCE} resources are exempt.</b> They keep one replica per live instance,
 *       so removing an instance never forces a replica to relocate; their by-design N&rarr;N-1
 *       reduction is not a deficit and would otherwise be a false rejection.</li>
 *   <li><b>Fail-closed on an uncomputable baseline.</b> If the baseline what-if itself cannot be
 *       computed (e.g. a transient metadata-store error, or a cluster already unable to rebalance),
 *       the request is blocked with a {@code force=true}-able message rather than silently allowed;
 *       this trades a possible false block (recoverable via force) for never certifying a write that
 *       could not be validated.</li>
 * </ul>
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
      if (idealState == null || !WagedRebalancer.class.getName()
          .equals(idealState.getRebalancerClassName())) {
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

    // Cheap, sound pre-checks: block obvious infeasibility without running the WAGED what-if. Both
    // mirror a WAGED hard-constraint failure exactly, so any block here is one the candidate
    // simulation would also produce. They run only when the target is currently live (and thus in
    // WAGED's eligible pool); draining an already-dead instance removes nothing, so we defer to the
    // simulation, which correctly reports no change.
    Set<String> liveInstanceSet = new HashSet<>(liveInstances);
    if (liveInstanceSet.contains(instanceName)) {
      // The eligible pool after the operation: assignable AND live, excluding the drained target.
      List<InstanceConfig> remainingAssignableInstances = new ArrayList<>();
      for (InstanceConfig instanceConfig : baselineInstanceConfigs) {
        if (instanceConfig == null || instanceName.equals(instanceConfig.getInstanceName())
            || !instanceConfig.isAssignable()
            || !liveInstanceSet.contains(instanceConfig.getInstanceName())) {
          continue;
        }
        remainingAssignableInstances.add(instanceConfig);
      }
      int remainingAssignableCount = remainingAssignableInstances.size();

      ValidationResult countVerdict = precheckReplicaCount(wagedIdealStates, remainingAssignableCount,
          proposedOp, instanceName, context.getClusterName());
      if (countVerdict != null) {
        return countVerdict;
      }
      ValidationResult capacityVerdict = precheckAggregateCapacity(wagedIdealStates,
          resourceConfigByName, remainingAssignableInstances, remainingAssignableCount, clusterConfig,
          proposedOp, instanceName, context.getClusterName());
      if (capacityVerdict != null) {
        return capacityVerdict;
      }
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
   * (a) Sound replica-count pre-check. Returns an infeasible result naming every WAGED resource that
   * would need more distinct assignable instances than remain after the operation (each such resource
   * makes WAGED throw {@code NO_CANDIDATE_NODE}); returns {@code null} when inconclusive so the caller
   * proceeds to the full what-if. {@code ANY_LIVEINSTANCE} resources are already excluded upstream, so
   * their replica count is never compared here.
   */
  private ValidationResult precheckReplicaCount(List<IdealState> wagedIdealStates,
      int remainingAssignableCount, InstanceConstants.InstanceOperation proposedOp, String instanceName,
      String clusterName) {
    List<Violation> violations = new ArrayList<>();
    for (IdealState idealState : wagedIdealStates) {
      int replicaCount = idealState.getReplicaCount(remainingAssignableCount);
      if (replicaCount > remainingAssignableCount) {
        violations.add(Violation.newBuilder(RULE_ID).resource(idealState.getResourceName()).message(
            String.format(
                "Operation %s on instance %s would leave %d assignable instance(s) in cluster %s, but "
                    + "WAGED resource %s needs %d distinct assignable instances to place every replica "
                    + "of each partition; the rebalance would fail to place them (NO_CANDIDATE_NODE). "
                    + "Add assignable capacity, or retry with force=true to override.", proposedOp,
                instanceName, remainingAssignableCount, clusterName, idealState.getResourceName(),
                replicaCount)).build());
        if (violations.size() >= MAX_REPORTED_VIOLATIONS) {
          break;
        }
      }
    }
    return violations.isEmpty() ? null : ValidationResult.of(violations);
  }

  /**
   * (b) Sound aggregate-capacity pre-check. Mirrors WAGED's global {@code CAPACITY_DEFICIT} condition
   * (see {@code ConstraintBasedAlgorithm}): if the total weight of all WAGED replicas exceeds the
   * summed capacity of the post-operation assignable nodes for any capacity key, WAGED cannot compute
   * an assignment. Demand and capacity are resolved with WAGED's own utilities so the numbers are
   * identical to the rebalancer's. Returns an infeasible result on a deficit, or {@code null} when the
   * check is inconclusive (no capacity model, or missing/malformed/ non-enumerable weights) so the
   * caller defers to the full what-if rather than risk a false block.
   */
  private ValidationResult precheckAggregateCapacity(List<IdealState> wagedIdealStates,
      Map<String, ResourceConfig> resourceConfigByName,
      List<InstanceConfig> remainingAssignableInstances, int remainingAssignableCount,
      ClusterConfig clusterConfig, InstanceConstants.InstanceOperation proposedOp, String instanceName,
      String clusterName) {
    List<String> capacityKeys = clusterConfig.getInstanceCapacityKeys();
    if (capacityKeys == null || capacityKeys.isEmpty()) {
      // No capacity model configured -> WAGED performs no capacity check -> nothing to pre-validate.
      return null;
    }

    // Summed capacity of the post-operation assignable node pool, per key.
    Map<String, Long> availableCapacity = new HashMap<>();
    for (InstanceConfig instanceConfig : remainingAssignableInstances) {
      Map<String, Integer> nodeCapacity;
      try {
        nodeCapacity =
            WagedValidationUtil.validateAndGetInstanceCapacity(clusterConfig, instanceConfig);
      } catch (Exception e) {
        // A node is missing required capacity keys: a pre-existing misconfiguration, not an effect of
        // this operation. Skip the capacity pre-check and defer to the full what-if.
        return null;
      }
      for (String key : capacityKeys) {
        Integer value = nodeCapacity.get(key);
        if (value != null) {
          availableCapacity.merge(key, value.longValue(), Long::sum);
        }
      }
    }

    // Total demand of all WAGED replicas, per key, computed exactly as WAGED does.
    Map<String, Long> requiredCapacity = new HashMap<>();
    for (IdealState idealState : wagedIdealStates) {
      ResourceConfig resourceConfig = resourceConfigByName.get(idealState.getResourceName());
      int replicaCount = idealState.getReplicaCount(remainingAssignableCount);
      if (replicaCount <= 0) {
        continue;
      }
      Map<String, Map<String, Integer>> partitionCapacityMap;
      try {
        partitionCapacityMap = resourceConfig == null ? Collections.emptyMap()
            : resourceConfig.getPartitionCapacityMap();
      } catch (IOException e) {
        return null; // malformed weights -> defer to the what-if
      }
      boolean hasPerPartitionOverrides = partitionCapacityMap.keySet().stream()
          .anyMatch(partition -> !ResourceConfig.DEFAULT_PARTITION_KEY.equals(partition));
      try {
        if (hasPerPartitionOverrides) {
          // Non-uniform weights: sum each partition's exact weight. If the partitions cannot be
          // enumerated (e.g. a freshly-created resource with no computed assignment yet), skip rather
          // than risk an inexact (possibly false) block.
          Set<String> partitions = idealState.getPartitionSet();
          if (partitions == null || partitions.isEmpty()) {
            return null;
          }
          for (String partition : partitions) {
            Map<String, Integer> weight =
                WagedRebalanceUtil.fetchCapacityUsage(partition, resourceConfig, clusterConfig);
            addWeightedDemand(requiredCapacity, capacityKeys, weight, (long) replicaCount);
          }
        } else {
          // Uniform default weight: demand = numPartitions * replicaCount * defaultWeight.
          Map<String, Integer> weight = WagedRebalanceUtil
              .fetchCapacityUsage(ResourceConfig.DEFAULT_PARTITION_KEY, resourceConfig, clusterConfig);
          long multiplier = (long) idealState.getNumPartitions() * replicaCount;
          addWeightedDemand(requiredCapacity, capacityKeys, weight, multiplier);
        }
      } catch (Exception e) {
        return null; // any weight-resolution problem -> defer to the what-if (never a false block)
      }
    }

    List<Violation> violations = new ArrayList<>();
    for (String key : capacityKeys) {
      long demand = requiredCapacity.getOrDefault(key, 0L);
      long available = availableCapacity.getOrDefault(key, 0L);
      if (demand > available) {
        violations.add(Violation.newBuilder(RULE_ID).message(String.format(
            "Operation %s on instance %s would leave cluster %s with %d unit(s) of capacity key '%s' "
                + "across its assignable instances, but its WAGED resources require %d; the rebalance "
                + "would fail (CAPACITY_DEFICIT). Free up or add '%s' capacity, or retry with "
                + "force=true to override.", proposedOp, instanceName, clusterName, available, key,
            demand, key)).build());
      }
    }
    return violations.isEmpty() ? null : ValidationResult.of(violations);
  }

  private static void addWeightedDemand(Map<String, Long> demand, List<String> capacityKeys,
      Map<String, Integer> weight, long multiplier) {
    for (String key : capacityKeys) {
      Integer unit = weight.get(key);
      if (unit != null && unit != 0) {
        demand.merge(key, unit.longValue() * multiplier, Long::sum);
      }
    }
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
