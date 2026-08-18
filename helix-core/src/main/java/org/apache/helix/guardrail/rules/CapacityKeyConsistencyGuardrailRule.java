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
import java.util.Comparator;
import java.util.HashMap;
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
import org.apache.helix.model.InstanceConfig;
import org.apache.helix.model.ResourceConfig;

/**
 * Guard rail that blocks adding a WAGED resource when some assignable instance does not declare a
 * value for every capacity dimension (key) the cluster requires, i.e. when the instance-side
 * capacity map is inconsistent with the cluster's {@code INSTANCE_CAPACITY_KEYS}.
 * <p>
 * WAGED accounts for capacity along the fixed set of keys declared once in
 * {@link ClusterConfig#getInstanceCapacityKeys()}. To build its cluster model it requires that
 * <em>every</em> assignable instance advertises a value for <em>every</em> key
 * ({@code WagedValidationUtil#validateAndGetInstanceCapacity}). If any assignable instance omits a
 * key, building the model throws and the WAGED global rebalance cannot compute an assignment for the
 * resource &mdash; so the resource is accepted but silently never places.
 * <p>
 * <b>Why only the instance side.</b> A WAGED resource add goes through
 * {@code HelixAdmin#addResourceWithWeight}, which already validates the <em>resource</em> side
 * up front: {@code ZKHelixAdmin#validateWeightForResourceConfig} rejects the add (before writing
 * anything to ZooKeeper) if the resource's weight map omits a required key. So a resource-side gap is
 * already pre-validated and never reaches ZooKeeper. The instance side, however, is <em>not</em>
 * checked on that path ({@code validateInstancesForWagedRebalance} lives on a separate admin call
 * that {@code addResourceWithWeight} does not invoke). So an operator can add a perfectly-formed
 * WAGED resource while some instance omits a key, get a {@code 200}, and the resource then sits in
 * ZooKeeper while WAGED fails to build a model on every rebalance &mdash; the only signal is a
 * rebalancer exception in the controller log. This rule runs the instance-side key-coverage check up
 * front so that mutation is rejected (or dry-run reported) before it reaches ZooKeeper.
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
 * <b>Opt-in.</b> This guard rail runs only when the cluster explicitly enables it via
 * {@link ClusterConfig#setCapacityKeyConsistencyGuardrailEnabled(boolean)}; it is disabled by
 * default. That makes turning it on a deliberate per-cluster decision and gives operators a
 * single-config-change kill switch: if it ever produces a false positive, disabling it via
 * ClusterConfig immediately backs it out for every caller with no client change and no helix-rest
 * redeploy. When disabled the rule returns feasible before reading any instance config, so a disabled
 * cluster is never exposed to the fail-closed instance-config scan below. {@code force=true} overrides
 * the verdict; {@code dryRun=true} reports it without writing.
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
    if (proposedResourceConfig == null) {
      // Not a resource-scoped mutation; nothing for this rule to certify on the resource-add path.
      return ValidationResult.feasible();
    }

    ReadOnlyDataAccessor dataAccessor = context.getDataAccessor();
    PropertyKey.Builder keyBuilder = dataAccessor.keyBuilder();
    ClusterConfig clusterConfig = dataAccessor.getProperty(keyBuilder.clusterConfig());
    if (clusterConfig == null) {
      // No cluster config to read the required capacity keys from; defer to downstream validation.
      return ValidationResult.feasible();
    }

    if (!clusterConfig.isCapacityKeyConsistencyGuardrailEnabled()) {
      // Opt-in guard rail, disabled by default. Returning here (before the instance-config scan
      // below) is also the kill switch: disabling the rule via ClusterConfig backs it out for every
      // caller with a single config change, and a disabled cluster never runs the fail-closed
      // instance-config read, so one unreadable znode cannot take addWagedResource down.
      return ValidationResult.feasible();
    }

    List<String> capacityKeys = clusterConfig.getInstanceCapacityKeys();
    if (capacityKeys.isEmpty()) {
      // Cluster does not use the WAGED capacity/weight model, so there are no keys to require.
      return ValidationResult.feasible();
    }

    List<Violation> violations = new ArrayList<>();
    int totalViolations = collectInstanceViolations(dataAccessor, keyBuilder, proposedResourceConfig,
        clusterConfig, capacityKeys, violations);

    if (violations.isEmpty()) {
      return ValidationResult.feasible();
    }
    if (totalViolations > violations.size()) {
      int reported = violations.size();
      violations.add(Violation.newBuilder(RULE_ID)
          .resource(proposedResourceConfig.getResourceName())
          .message(String.format(
              "Showing the first %d of %d instances missing a required capacity key; %d were omitted "
                  + "to bound the response size. The omitted instances are missing the same key(s); "
                  + "declare the reported keys on every assignable instance and resubmit.",
              reported, totalViolations, totalViolations - reported))
          .build());
    }
    return ValidationResult.of(violations);
  }

  /**
   * Checks that every assignable instance's effective capacity map (cluster default instance capacity
   * overridden by the instance's capacity) declares every required capacity key. This is a
   * precondition for the proposed resource to place: WAGED builds its model from every assignable
   * instance, so a single instance missing a key fails the whole model. Reads instance configs
   * fail-closed so a transient read error surfaces as a rejection rather than validation against
   * partial state. Returns the total number of violations found (which may exceed the number appended
   * to {@code violations} once the report cap is reached).
   */
  private int collectInstanceViolations(ReadOnlyDataAccessor dataAccessor,
      PropertyKey.Builder keyBuilder, ResourceConfig proposedResourceConfig,
      ClusterConfig clusterConfig, List<String> capacityKeys, List<Violation> violations) {
    Map<String, Integer> defaultInstanceCapacity = clusterConfig.getDefaultInstanceCapacityMap();
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
      // Effective capacity = cluster default instance capacity overridden by the instance's own
      // capacity, mirroring WagedValidationUtil#validateAndGetInstanceCapacity.
      Map<String, Integer> effectiveCapacity = new HashMap<>(defaultInstanceCapacity);
      effectiveCapacity.putAll(instanceConfig.getInstanceCapacityMap());
      List<String> missing = missingKeys(capacityKeys, effectiveCapacity.keySet());
      if (missing.isEmpty()) {
        continue;
      }
      total++;
      if (violations.size() >= MAX_REPORTED_VIOLATIONS) {
        continue;
      }
      violations.add(Violation.newBuilder(RULE_ID)
          .resource(proposedResourceConfig.getResourceName())
          .message(String.format(
              "Instance %s does not declare WAGED capacity key(s) %s required by the cluster "
                  + "(INSTANCE_CAPACITY_KEYS=%s); WAGED cannot build a model that includes it, so "
                  + "resource %s (and every other WAGED resource) would be accepted but never place. "
                  + "Add the missing key(s) to the instance's INSTANCE_CAPACITY_MAP or to the "
                  + "cluster's DEFAULT_INSTANCE_CAPACITY_MAP.",
              instanceConfig.getInstanceName(), missing, capacityKeys,
              proposedResourceConfig.getResourceName()))
          .build());
    }
    return total;
  }

  /**
   * The required keys, in their cluster-declared order, that are absent from {@code presentKeys}.
   */
  private static List<String> missingKeys(List<String> requiredKeys, Set<String> presentKeys) {
    List<String> missing = new ArrayList<>();
    for (String key : requiredKeys) {
      if (!presentKeys.contains(key)) {
        missing.add(key);
      }
    }
    return missing;
  }
}
