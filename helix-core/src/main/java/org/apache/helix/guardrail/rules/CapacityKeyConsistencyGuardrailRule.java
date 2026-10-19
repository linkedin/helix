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
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

import org.apache.helix.HelixException;
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
 * Guard rail that blocks a mutation which would leave the cluster's WAGED
 * {@code INSTANCE_CAPACITY_KEYS} unsatisfiable &mdash; i.e. when some assignable instance does not
 * declare a value for every capacity dimension (key) the cluster requires. It runs on the write
 * paths that can newly subject resources to that model: adding a WAGED resource and flipping an
 * existing resource (or every resource in the cluster) to WAGED, each validated against the committed
 * cluster config; and changing the cluster config's capacity keys or default capacity map (validated
 * against the proposed, not-yet-written config, and only when that change actually alters the
 * capacity contract, so a newly-added key is checked before it is persisted while an unrelated edit
 * is not).
 * <p>
 * WAGED accounts for capacity along the fixed set of keys declared once in
 * {@link ClusterConfig#getInstanceCapacityKeys()}. To build its cluster model it requires that
 * <em>every</em> assignable instance advertises a value for <em>every</em> key
 * ({@code WagedValidationUtil#validateAndGetInstanceCapacity}). If any assignable instance omits a
 * key, building the model throws and the WAGED global rebalance cannot compute an assignment for the
 * resource &mdash; so the resource is accepted but silently never places.
 * <p>
 * <b>Instance side vs. resource side.</b> WAGED needs two things to build its model: every assignable
 * <em>instance</em> must declare every capacity key, and every WAGED <em>resource</em> must declare a
 * weight for every key. Which side this rule checks depends on the path:
 * <ul>
 *   <li><b>Resource add</b> ({@code HelixAdmin#addResourceWithWeight}) already validates the resource
 *       side up front &mdash; {@code ZKHelixAdmin#validateWeightForResourceConfig} rejects the add
 *       before any ZooKeeper write if the resource's weight map omits a required key &mdash; but does
 *       <em>not</em> check the instance side ({@code validateInstancesForWagedRebalance} is a separate
 *       admin call it never invokes). So on the add path this rule contributes only the missing
 *       instance-side check.</li>
 *   <li><b>Bulk flip</b> ({@code enableWagedRebalanceForAllResources}) sweeps <em>every</em> resource
 *       that has an IdealState into WAGED through {@code ZKHelixAdmin#enableWagedRebalance}, which does
 *       <em>no</em> validation at all. Legacy resources flipped this way never went through
 *       {@code addResourceWithWeight}, so their weights were never validated: a single flipped resource
 *       whose weight map omits a required key (and that the cluster's
 *       {@code DEFAULT_PARTITION_WEIGHT_MAP} does not cover) makes WAGED fail to build the one
 *       cluster-wide model, stalling the rebalance for <em>every</em> resource. So on the bulk path this
 *       rule checks <em>both</em> sides &mdash; the instance side and, naming each offending resource,
 *       the resource side.</li>
 * </ul>
 * Either way an unvalidated gap would otherwise be accepted with a {@code 200} and surface only as a
 * rebalancer exception in the controller log; this rule turns it into an up-front rejection (or
 * dry-run verdict) before anything reaches ZooKeeper.
 * <p>
 * It is the coverage complement to {@link PartitionWeightCapacityGuardrailRule}: that rule checks
 * whether each partition's weight <em>fits</em> on some instance, and deliberately skips a dimension
 * that no instance advertises (it cannot fairly blame the resource author for an instance-side gap);
 * this rule is what turns that same instance-side gap into an explicit, actionable rejection.
 * <p>
 * Only <em>assignable</em> instances are checked, because WAGED builds its model from exactly those
 * (matching the rebalancer and {@link PartitionWeightCapacityGuardrailRule}); a non-assignable
 * instance's capacity is never consulted, so a gap there cannot break placement.
 * <p>
 * <b>Always on.</b> This guard rail runs on every {@code addWagedResource} request, every
 * {@code enableWagedRebalance} request (both the single-resource and the bulk all-resources forms),
 * and every cluster-config update; it is not gated behind a cluster config toggle. On the cluster-config path
 * it enforces only when the update actually changes the capacity contract &mdash; its
 * {@code INSTANCE_CAPACITY_KEYS} or {@code DEFAULT_INSTANCE_CAPACITY_MAP} &mdash; so an unrelated
 * cluster-config edit is never rejected for a pre-existing capacity gap it does not touch (mirroring
 * {@link RetiredClusterConfigGuardrailRule}). It is a no-op for a cluster that does not use the WAGED
 * capacity model (empty {@code INSTANCE_CAPACITY_KEYS}), returning feasible before reading any
 * instance config, so only a cluster that actually declares capacity keys is exposed to the
 * fail-closed instance-config scan (and, on the bulk flip, resource-config scan) below.
 * {@code force=true} overrides the verdict; {@code dryRun=true} reports it without writing.
 */
public class CapacityKeyConsistencyGuardrailRule implements GuardrailRule {
  public static final String RULE_ID = "WAGED_INSTANCE_CAPACITY_KEY_MISSING";

  // Upper bound on the number of individual coverage violations enumerated in a single verdict. A
  // cluster can be missing a key on every instance at once; beyond this cap the extra violations are
  // summarized in a single trailing entry recording how many were omitted, so a pathological case
  // cannot return a multi-megabyte 400 body.
  private static final int MAX_REPORTED_VIOLATIONS = 100;

  @Override
  public String getId() {
    return RULE_ID;
  }

  @Override
  public ValidationResult validate(GuardrailContext context) {
    ResourceConfig proposedResourceConfig = context.getProposedResourceConfig();
    ClusterConfig proposedClusterConfig = context.getProposedClusterConfig();
    boolean wagedEnableForAllResources = context.isWagedEnableForAllResources();
    if (proposedResourceConfig == null && proposedClusterConfig == null
        && !wagedEnableForAllResources) {
      // Not a resource-scoped add/flip, a cluster-config capacity change, or a bulk WAGED enable;
      // nothing for this rule to certify.
      return ValidationResult.feasible();
    }

    // Cluster-config path: enforce only when the change actually touches the WAGED capacity contract.
    // The instance-side coverage check below is an invariant over the cluster's INSTANCE_CAPACITY_KEYS
    // and DEFAULT_INSTANCE_CAPACITY_MAP; an update that leaves both unchanged cannot create or widen a
    // coverage gap, so re-running the full scan would only re-litigate a pre-existing gap and reject
    // unrelated cluster-config edits until it is fixed or forced. Mirrors
    // RetiredClusterConfigGuardrailRule, which fires only when the retired key actually changes. The
    // resource-add path has no committed-vs-proposed delta and is unaffected. A missing current config
    // (brand-new cluster) cannot be proven unchanged, so it falls through and is enforced.
    if (proposedClusterConfig != null) {
      ClusterConfig currentClusterConfig = context.getCurrentClusterConfig();
      if (currentClusterConfig != null
          && Objects.equals(currentClusterConfig.getInstanceCapacityKeys(),
              proposedClusterConfig.getInstanceCapacityKeys())
          && Objects.equals(currentClusterConfig.getDefaultInstanceCapacityMap(),
              proposedClusterConfig.getDefaultInstanceCapacityMap())) {
        return ValidationResult.feasible();
      }
    }

    ReadOnlyDataAccessor dataAccessor = context.getDataAccessor();
    PropertyKey.Builder keyBuilder = dataAccessor.keyBuilder();

    // On the cluster-config path validate the proposed (not-yet-written) config so the check reflects
    // the keys the change would require; on the resource-add path read the committed cluster config.
    ClusterConfig clusterConfig = proposedClusterConfig != null
        ? proposedClusterConfig
        : dataAccessor.getProperty(keyBuilder.clusterConfig());
    if (clusterConfig == null) {
      // No cluster config to read the required capacity keys from; defer to downstream validation.
      return ValidationResult.feasible();
    }

    List<String> capacityKeys = clusterConfig.getInstanceCapacityKeys();
    if (capacityKeys.isEmpty()) {
      // Cluster does not use the WAGED capacity/weight model, so there are no keys to require. This
      // also short-circuits before the fail-closed instance-config scan below, so a cluster that
      // does not use capacity keys is never exposed to it.
      return ValidationResult.feasible();
    }

    // On the resource-add/flip path the verdict is scoped to the resource being added or flipped; the
    // cluster-config and bulk WAGED-enable paths have no single such resource, so their violations
    // carry no resource name.
    String resourceName =
        proposedResourceConfig != null ? proposedResourceConfig.getResourceName() : null;

    List<Violation> violations = new ArrayList<>();
    int totalInstanceViolations = collectInstanceViolations(dataAccessor, keyBuilder, resourceName,
        wagedEnableForAllResources, clusterConfig, violations);
    // Append the instance-side overflow summary (if the cap was hit) before collecting any
    // resource-side violations, so the instance cap math reflects only the instance violations above.
    if (totalInstanceViolations > violations.size()) {
      int reported = violations.size();
      Violation.Builder overflow = Violation.newBuilder(RULE_ID)
          .message(String.format(
              "Showing the first %d of %d instances missing a required capacity key; %d were omitted "
                  + "to bound the response size. The omitted instances are missing the same key(s); "
                  + "declare the reported keys on every assignable instance and resubmit.",
              reported, totalInstanceViolations, totalInstanceViolations - reported));
      if (resourceName != null) {
        overflow.resource(resourceName);
      }
      violations.add(overflow.build());
    }

    if (wagedEnableForAllResources) {
      // The bulk flip (enableWagedRebalanceForAllResources) sweeps every resource that has an
      // IdealState into WAGED through an admin path that does no weight validation. Legacy resources
      // flipped this way never went through addResourceWithWeight, so a single one whose weight map
      // omits a required key fails the one cluster-wide WAGED model and stalls the rebalance for every
      // resource. Validate the resource side here too, naming each offending resource. (The
      // resource-add path is already resource-validated by addResourceWithWeight, so this runs only
      // for the bulk flip.)
      int instanceViolationCount = violations.size();
      int totalResourceViolations =
          collectResourceViolations(dataAccessor, keyBuilder, clusterConfig, violations);
      int reportedResourceViolations = violations.size() - instanceViolationCount;
      if (totalResourceViolations > reportedResourceViolations) {
        violations.add(Violation.newBuilder(RULE_ID)
            .message(String.format(
                "Showing the first %d of %d resources that cannot be flipped to WAGED; %d were "
                    + "omitted to bound the response size. Give each reported resource a complete "
                    + "partition weight map (or declare the missing key(s) in the cluster's "
                    + "DEFAULT_PARTITION_WEIGHT_MAP) and resubmit.",
                reportedResourceViolations, totalResourceViolations,
                totalResourceViolations - reportedResourceViolations))
            .build());
      }
    }

    if (violations.isEmpty()) {
      return ValidationResult.feasible();
    }
    return ValidationResult.of(violations);
  }

  /**
   * Checks every assignable instance against
   * {@link WagedValidationUtil#validateAndGetInstanceCapacity}, the exact effective-capacity merge and
   * required-key coverage check WAGED runs when it builds its cluster model, and records a violation
   * for each instance that check rejects. Delegating (rather than re-deriving the merge here) keeps the
   * guard rail from drifting from what WAGED actually enforces: a single assignable instance missing a
   * key fails the whole model, so every WAGED resource would be accepted but never place. Reads
   * instance configs fail-closed so a transient read error surfaces as a rejection rather than
   * validation against partial state. {@code resourceName} scopes the resource-add/flip path's
   * violations to the resource being added or flipped and is {@code null} on the cluster-config and
   * bulk WAGED-enable paths. Returns the total number
   * of violations found (which may exceed the number appended to {@code violations} once the report cap
   * is reached).
   */
  private int collectInstanceViolations(ReadOnlyDataAccessor dataAccessor,
      PropertyKey.Builder keyBuilder, String resourceName, boolean wagedEnableForAllResources,
      ClusterConfig clusterConfig, List<Violation> violations) {
    List<InstanceConfig> instanceConfigs =
        dataAccessor.getChildValues(keyBuilder.instanceConfigs(), true);

    // Evaluate assignable instances in a deterministic (name) order so reported violations are stable.
    List<InstanceConfig> assignableInstances = new ArrayList<>();
    for (InstanceConfig instanceConfig : instanceConfigs) {
      if (instanceConfig != null && instanceConfig.isAssignable()) {
        assignableInstances.add(instanceConfig);
      }
    }
    assignableInstances.sort(Comparator.comparing(InstanceConfig::getInstanceName));

    int total = 0;
    for (InstanceConfig instanceConfig : assignableInstances) {
      // Delegate the effective-capacity merge and required-key coverage check to the exact method
      // WAGED runs at model-build time (WagedValidationUtil#validateAndGetInstanceCapacity), so this
      // guard rail can never drift from what WAGED enforces: the HelixException it throws is precisely
      // the model-build failure the resource add would hit on every rebalance.
      try {
        WagedValidationUtil.validateAndGetInstanceCapacity(clusterConfig, instanceConfig);
        continue;
      } catch (HelixException e) {
        total++;
        if (violations.size() >= MAX_REPORTED_VIOLATIONS) {
          continue;
        }
        Violation.Builder violation = Violation.newBuilder(RULE_ID)
            .message(instanceViolationMessage(resourceName, wagedEnableForAllResources,
                instanceConfig.getInstanceName(), e.getMessage()));
        if (resourceName != null) {
          violation.resource(resourceName);
        }
        violations.add(violation.build());
      }
    }
    return total;
  }

  /**
   * Builds the per-instance violation message, wrapping the exact WAGED model-build error
   * ({@code wagedError}). The resource-add/flip path ({@code resourceName != null}) blames the
   * specific resource being added or flipped; the bulk WAGED-enable path
   * ({@code wagedEnableForAllResources}) frames the gap as a consequence of enabling WAGED for every
   * resource in the cluster; and the cluster-config path frames it as a consequence of the proposed
   * capacity-key change. All point the operator at the two places a key can be declared (the
   * instance's {@code INSTANCE_CAPACITY_MAP} or the cluster's {@code DEFAULT_INSTANCE_CAPACITY_MAP}).
   */
  private static String instanceViolationMessage(String resourceName,
      boolean wagedEnableForAllResources, String instanceName, String wagedError) {
    if (resourceName != null) {
      return String.format(
          "Instance %s is not valid for the WAGED capacity model: %s WAGED cannot build a "
              + "cluster model that includes it, so resource %s (and every other WAGED "
              + "resource) would be accepted but never place. Add the missing key(s) to the "
              + "instance's INSTANCE_CAPACITY_MAP or to the cluster's "
              + "DEFAULT_INSTANCE_CAPACITY_MAP.",
          instanceName, wagedError, resourceName);
    }
    if (wagedEnableForAllResources) {
      return String.format(
          "Instance %s is not valid for the WAGED capacity model that enabling WAGED for all "
              + "resources would require: %s WAGED cannot build a cluster model that includes it, so "
              + "every WAGED resource would be accepted but never place. Add the missing key(s) to "
              + "the instance's INSTANCE_CAPACITY_MAP or to the cluster's "
              + "DEFAULT_INSTANCE_CAPACITY_MAP before enabling WAGED for all resources.",
          instanceName, wagedError);
    }
    return String.format(
        "Instance %s is not valid for the WAGED capacity model that this cluster config change would "
            + "require: %s WAGED cannot build a cluster model that includes it, so every WAGED "
            + "resource would be accepted but never place. Add the missing key(s) to the instance's "
            + "INSTANCE_CAPACITY_MAP or to the cluster's DEFAULT_INSTANCE_CAPACITY_MAP before "
            + "applying this change.",
        instanceName, wagedError);
  }

  /**
   * Validates the <em>resource</em> side of the WAGED capacity model on the bulk-flip path: every
   * resource that has an IdealState (exactly what {@code ZKHelixAdmin#enableWagedRebalance} flips to
   * WAGED) must declare &mdash; itself or via the cluster's {@code DEFAULT_PARTITION_WEIGHT_MAP}
   * &mdash; a weight for every required capacity key, or WAGED cannot build the one cluster-wide model
   * and the rebalance stalls for every resource. Mirrors
   * {@code ZKHelixAdmin#validateWeightForResourceConfig} and the exact per-partition check WAGED runs
   * at model-build time ({@link WagedValidationUtil#validateAndGetPartitionCapacity}) so the guard rail
   * cannot drift from what WAGED enforces, and records a violation naming each offending resource.
   * Reads ideal states and resource configs fail-closed so a transient read error surfaces as a
   * rejection rather than validation against partial state. Returns the total number of offending
   * resources (which may exceed the number appended to {@code violations} once the report cap is
   * reached).
   */
  private int collectResourceViolations(ReadOnlyDataAccessor dataAccessor,
      PropertyKey.Builder keyBuilder, ClusterConfig clusterConfig, List<Violation> violations) {
    List<IdealState> idealStates = dataAccessor.getChildValues(keyBuilder.idealStates(), true);
    List<ResourceConfig> resourceConfigs =
        dataAccessor.getChildValues(keyBuilder.resourceConfigs(), true);

    Map<String, ResourceConfig> resourceConfigByName = new HashMap<>();
    for (ResourceConfig resourceConfig : resourceConfigs) {
      if (resourceConfig != null) {
        resourceConfigByName.put(resourceConfig.getResourceName(), resourceConfig);
      }
    }

    // Evaluate resources in a deterministic (name) order so reported violations are stable.
    List<IdealState> sortedIdealStates = new ArrayList<>();
    for (IdealState idealState : idealStates) {
      if (idealState != null) {
        sortedIdealStates.add(idealState);
      }
    }
    sortedIdealStates.sort(Comparator.comparing(IdealState::getResourceName));

    int total = 0;
    int reported = 0;
    for (IdealState idealState : sortedIdealStates) {
      String resourceName = idealState.getResourceName();
      String reason = resourceWeightViolationReason(idealState,
          resourceConfigByName.get(resourceName), clusterConfig);
      if (reason == null) {
        continue;
      }
      total++;
      if (reported >= MAX_REPORTED_VIOLATIONS) {
        continue;
      }
      reported++;
      violations.add(Violation.newBuilder(RULE_ID)
          .resource(resourceName)
          .message(resourceViolationMessage(resourceName, reason))
          .build());
    }
    return total;
  }

  /**
   * Returns a human-readable reason this resource cannot join the WAGED capacity model, or
   * {@code null} if its partition weights fully cover the cluster's required capacity keys. Resolves
   * weights exactly as {@link WagedValidationUtil#validateAndGetPartitionCapacity} does at model-build
   * time: a partition's effective weight is its own weight (or the resource's
   * {@code DEFAULT_PARTITION_KEY} weight) overlaid on the cluster's
   * {@code DEFAULT_PARTITION_WEIGHT_MAP}, and every required key must be present. A resource with no
   * {@code ResourceConfig} (a legacy resource the bulk flip would sweep in) has no weights of its own,
   * so it is covered only if the cluster default weight map declares every required key &mdash; that
   * null-config case is handled separately because
   * {@code WagedValidationUtil#validateAndGetPartitionCapacity} dereferences the resource config when
   * building its error message.
   */
  private static String resourceWeightViolationReason(IdealState idealState,
      ResourceConfig resourceConfig, ClusterConfig clusterConfig) {
    List<String> requiredKeys = clusterConfig.getInstanceCapacityKeys();
    Map<String, Integer> clusterDefaultWeights = clusterConfig.getDefaultPartitionWeightMap();

    if (resourceConfig == null) {
      // Legacy resource with no weights of its own: every partition falls back to the cluster default
      // weight map, so that map alone must declare every required key.
      if (clusterDefaultWeights.keySet().containsAll(requiredKeys)) {
        return null;
      }
      return String.format(
          "has no ResourceConfig, so every partition falls back to the cluster's "
              + "DEFAULT_PARTITION_WEIGHT_MAP %s, which does not declare every required capacity key %s",
          clusterDefaultWeights, requiredKeys);
    }

    Map<String, Map<String, Integer>> capacityMap;
    try {
      capacityMap = resourceConfig.getPartitionCapacityMap();
    } catch (IOException e) {
      return "has a malformed partition capacity configuration: " + e.getMessage();
    }

    // Every explicitly-weighted partition (including the DEFAULT_PARTITION_KEY entry, if any) must
    // itself resolve to full key coverage -- exactly the check WAGED runs per partition at model-build
    // time.
    for (String partitionName : capacityMap.keySet()) {
      try {
        WagedValidationUtil.validateAndGetPartitionCapacity(partitionName, resourceConfig,
            capacityMap, clusterConfig);
      } catch (HelixException e) {
        return e.getMessage();
      }
    }

    // Partitions with no explicit weight fall back to the DEFAULT_PARTITION_KEY entry, or -- absent
    // that -- to the cluster default weight map alone. If some partition is unweighted and there is no
    // DEFAULT_PARTITION_KEY entry, that cluster-default fallback must itself declare every required key
    // (this mirrors WagedValidationUtil#validateAndGetPartitionCapacity's resolution for a partition
    // that has neither its own nor a DEFAULT_PARTITION_KEY weight).
    boolean hasDefaultEntry = capacityMap.containsKey(ResourceConfig.DEFAULT_PARTITION_KEY);
    int explicitlyWeightedPartitions = capacityMap.size() - (hasDefaultEntry ? 1 : 0);
    if (!hasDefaultEntry && explicitlyWeightedPartitions < idealState.getNumPartitions()
        && !clusterDefaultWeights.keySet().containsAll(requiredKeys)) {
      return String.format(
          "has %d partition(s) with no weight and no DEFAULT_PARTITION_KEY entry, so they fall back to "
              + "the cluster's DEFAULT_PARTITION_WEIGHT_MAP %s, which does not declare every required "
              + "capacity key %s",
          idealState.getNumPartitions() - explicitlyWeightedPartitions, clusterDefaultWeights,
          requiredKeys);
    }
    return null;
  }

  /**
   * Builds the per-resource violation message for the bulk-flip path, wrapping the specific
   * {@code reason} the resource fails the WAGED capacity model and pointing the operator at the two
   * places a weight can be declared.
   */
  private static String resourceViolationMessage(String resourceName, String reason) {
    return String.format(
        "Resource %s cannot be flipped to WAGED: it %s. WAGED builds one cluster-wide model from every "
            + "WAGED resource, so this single resource would make the model fail to build and stall the "
            + "WAGED rebalance for the whole cluster -- every resource would be accepted but none would "
            + "place. Give the resource a complete partition weight map (as addResourceWithWeight "
            + "requires) or declare the missing key(s) in the cluster's DEFAULT_PARTITION_WEIGHT_MAP "
            + "before enabling WAGED for all resources.",
        resourceName, reason);
  }
}
