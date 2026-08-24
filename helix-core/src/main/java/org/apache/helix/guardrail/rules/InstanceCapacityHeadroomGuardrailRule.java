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
import org.apache.helix.controller.rebalancer.util.WagedValidationUtil;
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
 * Guard rail that blocks an instance-config update which <em>reduces</em> an instance's capacity so
 * far that the cluster's total remaining capacity, in some dimension, can no longer cover the total
 * capacity already committed to existing WAGED resources.
 * <p>
 * WAGED places every replica of every partition on some instance subject to per-dimension capacity.
 * A necessary condition for the whole assignment to fit is that, in each dimension {@code d},
 * <pre>
 *   sum over instances (capacity_d)  &gt;=  sum over WAGED resources (numPartitions * replicas * weight_d)
 * </pre>
 * When an operator shrinks an instance's capacity, the left-hand side drops. If it drops below the
 * committed demand, the freed load has nowhere to go and partitions are left permanently unassigned.
 * Today {@code updateInstanceConfig} writes the reduced capacity straight to ZooKeeper and the
 * failure only surfaces later at rebalance time; this rule rejects the reduction up front.
 * <p>
 * The check is deterministic &mdash; it reads only cluster/instance/resource <em>config</em>, never
 * live assignment state &mdash; so the same verdict powers both enforcement and dry-run. It is a
 * necessary (not sufficient) condition: aggregate headroom can hold while fragmentation or fault
 * domains still block a specific placement, so it only fails cases that are provably over-committed.
 * It is complementary to {@link PartitionWeightCapacityGuardrailRule}, which guards the orthogonal
 * per-partition "does the biggest single piece fit the biggest single shelf" condition.
 * <p>
 * The rule is a no-op unless the update actually lowers a capacity dimension: raising or leaving
 * capacity unchanged can never newly break feasibility, and clusters that do not use the WAGED
 * capacity model have no dimensions to evaluate.
 * <p>
 * <b>Scope.</b> This rule only evaluates changes to the instance capacity map. It deliberately does
 * not reason about instance-operation transitions (for example EVACUATE, DISABLE, or SWAP) that pull
 * an instance out of the WAGED assignable pool and thereby remove its entire capacity from supply:
 * those requests carry no capacity map, and their placement feasibility is owned by the dedicated
 * InstanceOperationRebalanceFeasibilityGuardrailRule. A single request that both reduces capacity and
 * changes the instance operation is certified here only for its capacity reduction.
 */
public class InstanceCapacityHeadroomGuardrailRule implements GuardrailRule {
  public static final String RULE_ID = "INSTANCE_CAPACITY_BELOW_CLUSTER_DEMAND";

  // Upper bound on the number of over-committed dimensions enumerated in a single verdict. The set of
  // dimensions is normally small (the cluster's capacity keys), but this bounds the response for a
  // pathologically long capacity-key list; any overflow is summarized in a trailing entry.
  private static final int MAX_REPORTED_VIOLATIONS = 100;

  @Override
  public String getId() {
    return RULE_ID;
  }

  @Override
  public ValidationResult validate(GuardrailContext context) {
    InstanceConfig proposedDelta = context.getProposedInstanceConfig();
    String instanceName = context.getInstanceName();
    if (proposedDelta == null || instanceName == null) {
      // Not an instance-config-scoped mutation; nothing for this rule to certify.
      return ValidationResult.feasible();
    }

    Map<String, Integer> deltaCapacity = proposedDelta.getInstanceCapacityMap();
    if (deltaCapacity.isEmpty()) {
      // The update does not touch capacity (e.g. a topology-only change, or an instance-operation
      // change such as EVACUATE/DISABLE). Out of scope here: instance-operation feasibility is owned
      // by InstanceOperationRebalanceFeasibilityGuardrailRule.
      return ValidationResult.feasible();
    }

    ReadOnlyDataAccessor dataAccessor = context.getDataAccessor();
    PropertyKey.Builder keyBuilder = dataAccessor.keyBuilder();
    ClusterConfig clusterConfig = dataAccessor.getProperty(keyBuilder.clusterConfig());
    if (clusterConfig == null) {
      return ValidationResult.feasible();
    }

    if (!clusterConfig.isInstanceCapacityHeadroomGuardrailEnabled()) {
      // Opt-in guard rail, disabled by default. Returning here (before the instance-config scan below)
      // is also the kill switch: one ClusterConfig change backs the rule out for every caller, and a
      // disabled cluster never runs the fail-closed config reads, so an unreadable znode cannot take
      // updateInstanceConfig down.
      return ValidationResult.feasible();
    }

    List<String> capacityKeys = clusterConfig.getInstanceCapacityKeys();
    if (capacityKeys.isEmpty()) {
      // Cluster does not use the WAGED capacity model, so capacity carries no meaning here.
      return ValidationResult.feasible();
    }

    Map<String, Integer> defaultInstanceCapacity = clusterConfig.getDefaultInstanceCapacityMap();
    List<InstanceConfig> instanceConfigs =
        dataAccessor.getChildValues(keyBuilder.instanceConfigs(), true);

    // Locate the existing config for the target instance so we can compute its post-merge capacity.
    InstanceConfig existingTarget = null;
    for (InstanceConfig instanceConfig : instanceConfigs) {
      if (instanceName.equals(instanceConfig.getInstanceName())) {
        existingTarget = instanceConfig;
        break;
      }
    }
    if (existingTarget == null) {
      // No existing instance to reduce; defer to downstream validation.
      return ValidationResult.feasible();
    }

    if (!existingTarget.isAssignable()) {
      // WAGED never places on a non-assignable instance (e.g. EVACUATE / SWAP_IN), so changing its
      // capacity cannot lower the supply available to WAGED. Nothing for this rule to certify.
      return ValidationResult.feasible();
    }

    // WAGED draws capacity only from assignable instances, so supply (and the effective replica
    // count for ANY_LIVEINSTANCE resources) is computed over exactly those, mirroring
    // PartitionWeightCapacityGuardrailRule. Counting non-assignable instances would overstate supply
    // and let this rule certify a reduction WAGED cannot actually absorb.
    List<InstanceConfig> assignableInstances = new ArrayList<>();
    for (InstanceConfig instanceConfig : instanceConfigs) {
      if (instanceConfig != null && instanceConfig.isAssignable()) {
        assignableInstances.add(instanceConfig);
      }
    }

    // Effective capacity of the target before and after the update. updateInstanceConfig REPLACES the
    // whole INSTANCE_CAPACITY_MAP (ConfigAccessor.updateInstanceConfig -> ZKUtil.createOrUpdate ->
    // ZNRecord.update does mapFields.putAll, swapping the map wholesale rather than merging inner
    // keys), so the post-update capacity is the cluster default overlaid by ONLY the incoming map. A
    // dimension the caller omits is dropped back to the cluster default (or 0), which is itself a
    // reduction from any higher explicit value it held before.
    Map<String, Integer> beforeCapacity = new HashMap<>(defaultInstanceCapacity);
    beforeCapacity.putAll(existingTarget.getInstanceCapacityMap());
    Map<String, Integer> afterCapacity = new HashMap<>(defaultInstanceCapacity);
    afterCapacity.putAll(deltaCapacity);

    // Only dimensions this update actually lowers can newly break feasibility. Tying the verdict to
    // the reduced dimensions avoids blaming an unrelated, pre-existing shortfall on this mutation.
    Set<String> reducedDimensions = new HashSet<>();
    for (String dimension : capacityKeys) {
      if (afterCapacity.getOrDefault(dimension, 0) < beforeCapacity.getOrDefault(dimension, 0)) {
        reducedDimensions.add(dimension);
      }
    }
    if (reducedDimensions.isEmpty()) {
      // A raise or no-op is always safe.
      return ValidationResult.feasible();
    }

    // supply_after(d) = sum over assignable instances of capacity in d, using the post-update value
    // for the target instance. Longs guard against overflow when many large capacities are summed.
    Map<String, Long> supplyAfter = new HashMap<>();
    for (InstanceConfig instanceConfig : assignableInstances) {
      Map<String, Integer> capacity = new HashMap<>(defaultInstanceCapacity);
      if (instanceName.equals(instanceConfig.getInstanceName())) {
        // Target: the incoming map fully replaces its capacity (see above), so a dimension the caller
        // omits contributes only the cluster default here, not the target's old value.
        capacity.putAll(deltaCapacity);
      } else {
        capacity.putAll(instanceConfig.getInstanceCapacityMap());
      }
      for (String dimension : reducedDimensions) {
        supplyAfter.merge(dimension, (long) capacity.getOrDefault(dimension, 0), Long::sum);
      }
    }

    // demand(d) = sum over existing WAGED resources of replicas * sum over partitions of weight in d.
    Map<String, Long> demand;
    try {
      demand = computeCommittedDemand(dataAccessor, keyBuilder, clusterConfig, reducedDimensions,
          assignableInstances.size());
    } catch (IOException e) {
      // An existing resource's weight map is malformed; we cannot certify remaining headroom.
      return ValidationResult.infeasible(Violation.newBuilder(RULE_ID)
          .message("Could not parse an existing resource's partition weight map while computing "
              + "cluster demand: " + e.getMessage())
          .build());
    }

    // Report every dimension that ends up over-committed, not just the first, so an operator sees the
    // full shortfall in one response instead of fixing dimensions one 400 at a time. Worst deficit
    // first (ties broken by dimension name) for a deterministic, actionable ordering.
    List<String> shortDimensions = new ArrayList<>();
    for (String dimension : reducedDimensions) {
      if (supplyAfter.getOrDefault(dimension, 0L) < demand.getOrDefault(dimension, 0L)) {
        shortDimensions.add(dimension);
      }
    }
    if (shortDimensions.isEmpty()) {
      return ValidationResult.feasible();
    }
    shortDimensions.sort(Comparator
        .comparingLong((String d) -> demand.getOrDefault(d, 0L) - supplyAfter.getOrDefault(d, 0L))
        .reversed()
        .thenComparing(Comparator.naturalOrder()));

    List<Violation> violations = new ArrayList<>();
    for (String dimension : shortDimensions) {
      // Cap the enumerated violations so a pathologically long capacity-key list cannot produce an
      // unbounded response; any overflow is summarized in the trailing entry below.
      if (violations.size() >= MAX_REPORTED_VIOLATIONS) {
        break;
      }
      long supply = supplyAfter.getOrDefault(dimension, 0L);
      long need = demand.getOrDefault(dimension, 0L);
      // Intentionally no force=true hint: forcing the reduction through is exactly what leaves the
      // freed load with no home, so the message points only at the safe remedies.
      violations.add(Violation.newBuilder(RULE_ID)
          .message(String.format(
              "Reducing instance %s leaves total cluster capacity %d for dimension '%s' below the %d "
                  + "already committed to WAGED resources in that dimension. The freed load would have "
                  + "no home and partitions would be left unassigned. Keep more capacity or reduce "
                  + "committed demand.",
              instanceName, supply, dimension, need))
          .build());
    }
    if (shortDimensions.size() > violations.size()) {
      // More dimensions breached than we enumerated. Record the overflow so the caller knows the list
      // is truncated and by how much, instead of silently dropping them.
      int reported = violations.size();
      violations.add(Violation.newBuilder(RULE_ID)
          .message(String.format(
              "Showing the first %d of %d over-committed dimensions; %d were omitted to bound the "
                  + "response size. Fix the reported dimensions and resubmit.",
              reported, shortDimensions.size(), shortDimensions.size() - reported))
          .build());
    }
    return ValidationResult.of(violations);
  }

  /**
   * Sums, per requested dimension, the capacity every existing WAGED resource is committed to:
   * {@code replicas * sum over partitions of that partition's weight}. Each partition's weight is
   * resolved the way the WAGED rebalancer does in
   * {@link WagedValidationUtil#validateAndGetPartitionCapacity}: a partition the resource lists
   * explicitly uses its own weight, every other partition falls back to the resource's mandatory
   * {@code DEFAULT_PARTITION_KEY} weight, both layered over the cluster default. Reading only the
   * DEFAULT weight (a flat {@code numPartitions * default}) would over-count when per-partition
   * overrides sit below the default and under-count when they sit above, so the real per-partition
   * weights are summed instead. The partition <em>count</em> still comes from the declared
   * {@code numPartitions}, keeping the estimate independent of whether the controller has yet computed
   * an assignment (a freshly created resource's preference lists may still be empty). Non-WAGED
   * resources and dimensions a resource does not declare are ignored (key coverage is enforced
   * separately by add-time validation).
   */
  private Map<String, Long> computeCommittedDemand(ReadOnlyDataAccessor dataAccessor,
      PropertyKey.Builder keyBuilder, ClusterConfig clusterConfig, Set<String> dimensions,
      int eligibleInstanceCount) throws IOException {
    Map<String, Integer> defaultPartitionWeight = clusterConfig.getDefaultPartitionWeightMap();

    List<ResourceConfig> resourceConfigs =
        dataAccessor.getChildValues(keyBuilder.resourceConfigs(), true);
    Map<String, ResourceConfig> configByResource = new HashMap<>();
    for (ResourceConfig resourceConfig : resourceConfigs) {
      configByResource.put(resourceConfig.getResourceName(), resourceConfig);
    }

    Map<String, Long> demand = new HashMap<>();
    List<IdealState> idealStates = dataAccessor.getChildValues(keyBuilder.idealStates(), true);
    for (IdealState idealState : idealStates) {
      if (!WagedValidationUtil.isWagedEnabled(idealState)) {
        continue;
      }
      ResourceConfig resourceConfig = configByResource.get(idealState.getResourceName());
      if (resourceConfig == null) {
        // No weights declared for this WAGED resource; add-time key-coverage validation owns that.
        continue;
      }
      int numPartitions = idealState.getNumPartitions();
      int replicaCount = idealState.getReplicaCount(eligibleInstanceCount);
      if (numPartitions <= 0 || replicaCount <= 0) {
        continue;
      }
      // Sum each partition's real weight, mirroring WagedValidationUtil.validateAndGetPartitionCapacity:
      // an explicitly listed partition uses its own weight; every other partition falls back to the
      // resource's DEFAULT_PARTITION_KEY weight (both layered over the cluster default). Reading only
      // the DEFAULT weight would over- or under-count whenever per-partition overrides differ from it.
      Map<String, Map<String, Integer>> partitionWeights = resourceConfig.getPartitionCapacityMap();
      Map<String, Integer> defaultWeight = new HashMap<>(defaultPartitionWeight);
      defaultWeight.putAll(partitionWeights.getOrDefault(ResourceConfig.DEFAULT_PARTITION_KEY,
          Collections.emptyMap()));

      // Partitions the resource lists explicitly are counted at their own weight; the DEFAULT pseudo-
      // partition is not a real partition and is excluded from the count.
      int overriddenPartitions = 0;
      for (Map.Entry<String, Map<String, Integer>> entry : partitionWeights.entrySet()) {
        if (ResourceConfig.DEFAULT_PARTITION_KEY.equals(entry.getKey())) {
          continue;
        }
        overriddenPartitions++;
        Map<String, Integer> weight = new HashMap<>(defaultPartitionWeight);
        weight.putAll(entry.getValue());
        for (String dimension : dimensions) {
          Integer dimensionWeight = weight.get(dimension);
          if (dimensionWeight != null) {
            demand.merge(dimension, (long) dimensionWeight * replicaCount, Long::sum);
          }
        }
      }

      // The remaining partitions (declared count minus those with explicit overrides) use the resource
      // DEFAULT weight. Clamp at zero in case stale overrides outnumber the declared partitions.
      int defaultPartitions = Math.max(0, numPartitions - overriddenPartitions);
      for (String dimension : dimensions) {
        Integer dimensionWeight = defaultWeight.get(dimension);
        if (dimensionWeight != null) {
          demand.merge(dimension,
              (long) dimensionWeight * defaultPartitions * replicaCount, Long::sum);
        }
      }
    }
    return demand;
  }
}
