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
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.helix.PropertyKey;
import org.apache.helix.guardrail.GuardrailContext;
import org.apache.helix.guardrail.GuardrailRule;
import org.apache.helix.guardrail.ReadOnlyDataAccessor;
import org.apache.helix.guardrail.ValidationResult;
import org.apache.helix.guardrail.Violation;
import org.apache.helix.model.ClusterConfig;
import org.apache.helix.model.InstanceConfig;
import org.apache.helix.model.ResourceConfig;

/**
 * Guard rail that blocks adding a WAGED resource whose per-partition weight, in any capacity
 * dimension, exceeds the largest capacity advertised by any single instance in that dimension.
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
 */
public class PartitionWeightCapacityGuardrailRule implements GuardrailRule {
  public static final String RULE_ID = "PARTITION_WEIGHT_EXCEEDS_INSTANCE_CAPACITY";

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

    List<String> capacityKeys = clusterConfig.getInstanceCapacityKeys();
    if (capacityKeys.isEmpty()) {
      // Cluster does not use the WAGED capacity/weight model, so weights carry no meaning here.
      return ValidationResult.feasible();
    }

    // Largest capacity any single instance advertises, per dimension, folding in the cluster-level
    // default instance capacity the same way the WAGED rebalancer does.
    Map<String, Integer> defaultInstanceCapacity = clusterConfig.getDefaultInstanceCapacityMap();
    List<InstanceConfig> instanceConfigs =
        dataAccessor.getChildValues(keyBuilder.instanceConfigs(), true);
    Map<String, Integer> maxInstanceCapacity = new HashMap<>();
    for (InstanceConfig instanceConfig : instanceConfigs) {
      Map<String, Integer> instanceCapacity = new HashMap<>(defaultInstanceCapacity);
      instanceCapacity.putAll(instanceConfig.getInstanceCapacityMap());
      for (Map.Entry<String, Integer> entry : instanceCapacity.entrySet()) {
        maxInstanceCapacity.merge(entry.getKey(), entry.getValue(), Math::max);
      }
    }

    if (maxInstanceCapacity.isEmpty()) {
      // No instance advertises any capacity yet, so there is nothing to compare against. Leave this
      // to existing key-coverage validation rather than emit a misleading "unplaceable" verdict.
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
    for (Map.Entry<String, Map<String, Integer>> partitionEntry : partitionCapacityMap.entrySet()) {
      String partitionName = partitionEntry.getKey();
      // Effective weight = cluster default weight overridden by this partition's explicit weight,
      // mirroring WagedValidationUtil#validateAndGetPartitionCapacity.
      Map<String, Integer> effectiveWeight = new HashMap<>(defaultPartitionWeight);
      effectiveWeight.putAll(partitionEntry.getValue());

      // Only the cluster's declared capacity dimensions are meaningful to WAGED placement. A
      // required dimension missing from the weight is a key-coverage problem enforced separately by
      // addResourceWithWeight, so it is skipped here rather than reported as an over-weight.
      for (String dimension : capacityKeys) {
        Integer weight = effectiveWeight.get(dimension);
        if (weight == null) {
          continue;
        }
        int maxCapacity = maxInstanceCapacity.getOrDefault(dimension, 0);
        if (weight > maxCapacity) {
          // DEFAULT_PARTITION_KEY is a placeholder for "every partition", not a real partition, so
          // report it as unscoped for a clearer message.
          String reportedPartition =
              ResourceConfig.DEFAULT_PARTITION_KEY.equals(partitionName) ? null : partitionName;
          return ValidationResult.infeasible(Violation.newBuilder(RULE_ID)
              .resource(proposedResourceConfig.getResourceName())
              .partition(reportedPartition)
              .message(String.format(
                  "Partition weight %d for dimension '%s' exceeds the largest single instance "
                      + "capacity %d in that dimension, making %s permanently unplaceable. Lower the "
                      + "weight, raise instance capacity, or use force=true to override.", weight,
                  dimension, maxCapacity,
                  reportedPartition == null ? "every partition" : "partition " + reportedPartition))
              .build());
        }
      }
    }

    return ValidationResult.feasible();
  }
}
