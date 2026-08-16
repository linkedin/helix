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

import org.apache.helix.PropertyKey;
import org.apache.helix.guardrail.GuardrailContext;
import org.apache.helix.guardrail.GuardrailRule;
import org.apache.helix.guardrail.ReadOnlyDataAccessor;
import org.apache.helix.guardrail.ValidationResult;
import org.apache.helix.guardrail.Violation;
import org.apache.helix.model.ClusterConfig;
import org.apache.helix.model.IdealState;
import org.apache.helix.model.InstanceConfig;
import org.apache.helix.model.ResourceConfig;

/**
 * Guard rail that blocks adding a WAGED resource whose per-partition weight, in any capacity
 * dimension, exceeds the largest capacity advertised by any single <em>assignable</em> instance in
 * that dimension.
 * <p>
 * WAGED places a partition on exactly one instance per replica, so a partition can only ever be
 * placed if, for every weight dimension {@code d}, some instance has {@code capacity_d >= weight_d}.
 * If {@code weight_d} is larger than the maximum instance capacity in {@code d}, no arrangement of
 * the cluster can ever host that partition &mdash; it is <em>permanently unplaceable</em>. Existing
 * validation on {@code addWagedResource} only checks that the required weight <em>keys</em> are
 * present; it never compares their magnitudes to instance capacity, so today such a resource is
 * accepted into ZooKeeper and only fails later at rebalance time. This rule closes that gap by
 * rejecting the mutation up front.
 * <p>
 * The check is a necessary (not sufficient) condition for placeability: it compares each dimension
 * independently against the best instance in that dimension. It is deliberately conservative so it
 * never blocks a resource that could plausibly be placed &mdash; it only fails the cases that are
 * provably impossible.
 * <p>
 * <b>Why this is enforced up front rather than left to rebalance.</b> An unplaceable WAGED resource
 * is not a resource-local failure. Once it exists, the WAGED global rebalance fails to compute a
 * baseline assignment for the whole cluster (a {@code CAPACITY_DEFICIT} error), so <em>no</em>
 * resource added after it gets placed anywhere until the offending resource is dropped. Resources
 * already assigned keep their assignment, so the breakage is silent. That cluster-wide blast radius
 * is why this is a hard pre-write guard rail; it is also why callers should not reach for
 * {@code force=true} to bypass it, as forcing the resource in is exactly what triggers the deficit.
 * <p>
 * Only weights for the resource's <em>real</em> partitions are evaluated. A resource's
 * {@code PARTITION_CAPACITY_MAP} is operator-supplied and may carry stale or mistyped entries naming
 * partitions the resource does not actually have (e.g. leftovers after lowering
 * {@code NUM_PARTITIONS}). WAGED ignores such ghost entries at placement time and
 * {@code ZKHelixAdmin.validateWeightForResourceConfig} tolerates them on the write path, so this
 * rule skips any weight-map key that is neither {@code DEFAULT} nor a real partition of the proposed
 * ideal state &mdash; blocking on a partition that will never exist would be a false positive
 * stricter than the operation it fronts.
 * <p>
 * <b>Opt-in.</b> This guard rail runs only when the cluster explicitly enables it via
 * {@link ClusterConfig#setPartitionWeightGuardrailEnabled(boolean)}; it is disabled by default. That
 * makes turning it on a deliberate per-cluster decision and, just as importantly, gives operators a
 * single-config-change kill switch: if the rule ever produces a false positive, disabling it via
 * ClusterConfig immediately backs it out for every caller with no client change and no helix-rest
 * redeploy. When the cluster has it disabled the rule returns feasible before reading any instance
 * config, so a disabled cluster is never exposed to the fail-closed instance-config scan below.
 */
public class PartitionWeightCapacityGuardrailRule implements GuardrailRule {
  public static final String RULE_ID = "PARTITION_WEIGHT_EXCEEDS_INSTANCE_CAPACITY";

  // Upper bound on the number of individual weight violations enumerated in a single verdict. A
  // resource that sets explicit per-partition weights can breach capacity on every partition and
  // dimension at once (e.g. a 10k-partition, 3-dimension resource is ~30k violations), which would
  // otherwise produce a multi-megabyte 400 response. Beyond this cap the extra violations are
  // summarized in a single trailing entry that records how many were omitted.
  private static final int MAX_REPORTED_VIOLATIONS = 100;

  @Override
  public String getId() {
    return RULE_ID;
  }

  @Override
  public ValidationResult validate(GuardrailContext context) {
    ResourceConfig proposedResourceConfig = context.getProposedResourceConfig();
    if (proposedResourceConfig == null) {
      // Not a resource-scoped mutation; nothing for this rule to certify.
      return ValidationResult.feasible();
    }

    ReadOnlyDataAccessor dataAccessor = context.getDataAccessor();
    PropertyKey.Builder keyBuilder = dataAccessor.keyBuilder();
    ClusterConfig clusterConfig = dataAccessor.getProperty(keyBuilder.clusterConfig());
    if (clusterConfig == null) {
      // No cluster config to interpret weights against; defer to downstream validation.
      return ValidationResult.feasible();
    }

    if (!clusterConfig.isPartitionWeightGuardrailEnabled()) {
      // Opt-in guard rail, disabled by default. Returning here (before the instance-config scan
      // below) is also the kill switch: disabling the rule via ClusterConfig backs it out for every
      // caller with a single config change, and a disabled cluster never runs the fail-closed
      // instance-config read, so one unreadable znode cannot take addWagedResource down.
      return ValidationResult.feasible();
    }

    List<String> capacityKeys = clusterConfig.getInstanceCapacityKeys();
    if (capacityKeys.isEmpty()) {
      // Cluster does not use the WAGED capacity/weight model, so weights carry no meaning here.
      return ValidationResult.feasible();
    }

    // Largest capacity any single ASSIGNABLE instance advertises, per dimension, folding in the
    // cluster-level default instance capacity the same way the WAGED rebalancer does. Only
    // assignable instances are counted: WAGED places exclusively on the instances in
    // BaseControllerDataProvider#getAssignableInstanceConfigMap(), i.e. those where
    // InstanceConfig#isAssignable() is true (this excludes EVACUATE / SWAP_IN / UNKNOWN operations).
    // Counting capacity advertised by a non-assignable instance would let this rule certify a
    // resource that WAGED can never actually place.
    //
    // getChildValues(..., true) reads instance configs fail-closed: a transient ZK read error or a
    // single unreadable instance-config znode propagates out, and the guard rail pipeline then turns
    // the add into a 400 rather than silently validating against partial cluster state. That is the
    // safe default for a guard rail, at the cost of coupling addWagedResource availability to
    // instance-config readability.
    Map<String, Integer> defaultInstanceCapacity = clusterConfig.getDefaultInstanceCapacityMap();
    List<InstanceConfig> instanceConfigs =
        dataAccessor.getChildValues(keyBuilder.instanceConfigs(), true);
    Map<String, Integer> maxInstanceCapacity = new HashMap<>();
    for (InstanceConfig instanceConfig : instanceConfigs) {
      if (instanceConfig == null || !instanceConfig.isAssignable()) {
        // WAGED will not place on a non-assignable instance, so its capacity is irrelevant here.
        continue;
      }
      Map<String, Integer> instanceCapacity = new HashMap<>(defaultInstanceCapacity);
      instanceCapacity.putAll(instanceConfig.getInstanceCapacityMap());
      for (Map.Entry<String, Integer> entry : instanceCapacity.entrySet()) {
        maxInstanceCapacity.merge(entry.getKey(), entry.getValue(), Math::max);
      }
    }

    if (maxInstanceCapacity.isEmpty()) {
      // No assignable instance advertises any capacity yet, so there is nothing to compare against.
      // Leave this to existing key-coverage validation rather than emit a misleading "unplaceable"
      // verdict.
      return ValidationResult.feasible();
    }

    Map<String, Map<String, Integer>> partitionCapacityMap;
    try {
      partitionCapacityMap = proposedResourceConfig.getPartitionCapacityMap();
    } catch (IOException e) {
      // The weight map is malformed; we cannot certify the resource as placeable.
      return ValidationResult.infeasible(Violation.newBuilder(RULE_ID)
          .resource(proposedResourceConfig.getResourceName())
          .message(String.format("Could not parse partition weight map for resource %s: %s",
              proposedResourceConfig.getResourceName(), e.getMessage()))
          .build());
    }

    if (partitionCapacityMap.isEmpty()) {
      // No explicit weights: the resource relies entirely on cluster defaults. Evaluate the DEFAULT
      // partition so those defaults are still checked against instance capacity.
      partitionCapacityMap =
          Collections.singletonMap(ResourceConfig.DEFAULT_PARTITION_KEY, Collections.emptyMap());
    }

    Map<String, Integer> defaultPartitionWeight = clusterConfig.getDefaultPartitionWeightMap();
    Set<String> realPartitions = realPartitionNames(context.getProposedIdealState());

    // Evaluate partitions in a deterministic order (the DEFAULT placeholder first, then the rest in
    // natural order) rather than HashMap iteration order, so that when several partitions or
    // dimensions are over capacity the set and order of reported violations is stable between runs.
    List<String> orderedPartitions = new ArrayList<>(partitionCapacityMap.keySet());
    orderedPartitions.sort(
        Comparator.comparing((String p) -> !ResourceConfig.DEFAULT_PARTITION_KEY.equals(p))
            .thenComparing(Comparator.<String>naturalOrder()));

    List<Violation> violations = new ArrayList<>();
    int totalViolations = 0;
    for (String partitionName : orderedPartitions) {
      // Skip weights for partitions this resource does not actually have. The capacity map is
      // operator-supplied and can carry stale/typo'd entries; WAGED ignores them at placement time,
      // so blocking on them would be a false positive stricter than the write path we front. When
      // the real partition list is unknown (no proposed ideal state) we cannot tell ghosts apart,
      // so every entry is evaluated as before.
      if (!ResourceConfig.DEFAULT_PARTITION_KEY.equals(partitionName) && realPartitions != null
          && !realPartitions.contains(partitionName)) {
        continue;
      }

      // Effective weight = cluster default weight overridden by this partition's explicit weight,
      // mirroring WagedValidationUtil#validateAndGetPartitionCapacity.
      Map<String, Integer> effectiveWeight = new HashMap<>(defaultPartitionWeight);
      effectiveWeight.putAll(partitionCapacityMap.get(partitionName));

      // Only the cluster's declared capacity dimensions are meaningful to WAGED placement, and
      // capacityKeys is a List, so iterating it gives a fixed dimension order. A required dimension
      // missing from the weight is a key-coverage problem enforced separately by
      // addResourceWithWeight, so it is skipped here rather than reported as an over-weight. Every
      // over-capacity dimension is collected (not just the first) so a caller sees all problems in
      // one response instead of fixing one and resubmitting to discover the next.
      for (String dimension : capacityKeys) {
        Integer weight = effectiveWeight.get(dimension);
        if (weight == null) {
          continue;
        }
        Integer maxCapacity = maxInstanceCapacity.get(dimension);
        if (maxCapacity == null) {
          // No assignable instance advertises capacity for this dimension: a cluster-declared
          // capacity key that is missing from every instance. This is an instance-side
          // misconfiguration, not a fault of the resource being added, so we deliberately skip it
          // rather than fail the add. Treating the absent dimension as capacity 0 would blame the
          // resource author and tell them to lower a weight that cannot go below 0, and in this
          // state every WAGED resource is already unplaceable, not just this one.
          //
          // This gap is intentionally left uncovered here: nothing on the addResourceWithWeight
          // path validates instance-side capacity coverage. WagedValidationUtil#
          // validateAndGetInstanceCapacity runs only inside the rebalancer and from the separate
          // validateInstancesForWagedRebalance admin call, neither of which is on this path, so such
          // a resource is accepted at add time and only surfaces later as a WAGED placement failure.
          continue;
        }
        if (weight > maxCapacity) {
          totalViolations++;
          // Enumerate at most MAX_REPORTED_VIOLATIONS; any overflow is summarized after the loop so
          // a pathological resource cannot return a multi-megabyte body.
          if (violations.size() >= MAX_REPORTED_VIOLATIONS) {
            continue;
          }
          // DEFAULT_PARTITION_KEY is a placeholder for "every partition", not a real partition, so
          // report it as unscoped for a clearer message.
          String reportedPartition =
              ResourceConfig.DEFAULT_PARTITION_KEY.equals(partitionName) ? null : partitionName;
          // Intentionally no force=true hint: forcing an unplaceable resource in is what triggers
          // the cluster-wide CAPACITY_DEFICIT described in the class javadoc, so the message only
          // points at the safe remedies.
          violations.add(Violation.newBuilder(RULE_ID)
              .resource(proposedResourceConfig.getResourceName())
              .partition(reportedPartition)
              .message(String.format(
                  "Partition weight %d for dimension '%s' exceeds the largest single instance "
                      + "capacity %d in that dimension, making %s permanently unplaceable. Lower the "
                      + "weight or raise instance capacity.", weight, dimension, maxCapacity,
                  reportedPartition == null ? "every partition" : "partition " + reportedPartition))
              .build());
        }
      }
    }

    if (violations.isEmpty()) {
      return ValidationResult.feasible();
    }
    if (totalViolations > violations.size()) {
      // More partitions/dimensions breached capacity than we enumerated. Record the overflow so the
      // caller knows the list is truncated and by how much, instead of silently dropping them.
      int reported = violations.size();
      violations.add(Violation.newBuilder(RULE_ID)
          .resource(proposedResourceConfig.getResourceName())
          .message(String.format(
              "Showing the first %d of %d partition-weight violations; %d were omitted to bound the "
                  + "response size. The omitted violations are further partitions breaching the same "
                  + "dimension(s); fix the reported dimensions and resubmit.",
              reported, totalViolations, totalViolations - reported))
          .build());
    }
    return ValidationResult.of(violations);
  }

  /**
   * The names of the partitions the proposed resource actually has, or {@code null} if they cannot
   * be determined (no proposed ideal state supplied).
   * <p>
   * A freshly-proposed WAGED ideal state carries {@code NUM_PARTITIONS} but no computed assignment,
   * so its preference lists &mdash; and therefore {@link IdealState#getPartitionSet()} &mdash; are
   * still empty at pre-validation time. When that is the case the names are reconstructed from the
   * partition count using Helix's canonical {@code <resource>_<index>} scheme (the same naming the
   * controller applies in {@code ResourceComputationStage}). If preference lists are already
   * populated (e.g. a CUSTOMIZED ideal state), those partition names are used directly.
   */
  private static Set<String> realPartitionNames(IdealState idealState) {
    if (idealState == null) {
      return null;
    }
    Set<String> declaredPartitions = idealState.getPartitionSet();
    if (declaredPartitions != null && !declaredPartitions.isEmpty()) {
      return declaredPartitions;
    }
    int numPartitions = idealState.getNumPartitions();
    String resourceName = idealState.getResourceName();
    Set<String> partitionNames = new HashSet<>();
    for (int i = 0; i < numPartitions; i++) {
      partitionNames.add(resourceName + "_" + i);
    }
    return partitionNames;
  }
}
