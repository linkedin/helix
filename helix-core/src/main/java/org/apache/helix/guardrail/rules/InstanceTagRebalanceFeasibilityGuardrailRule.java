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
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.helix.PropertyKey;
import org.apache.helix.guardrail.GuardrailContext;
import org.apache.helix.guardrail.GuardrailRule;
import org.apache.helix.guardrail.ReadOnlyDataAccessor;
import org.apache.helix.guardrail.ValidationResult;
import org.apache.helix.guardrail.WagedAssignmentProvider;
import org.apache.helix.model.ClusterConfig;
import org.apache.helix.model.IdealState;
import org.apache.helix.model.InstanceConfig;
import org.apache.helix.zookeeper.datamodel.ZNRecord;

/**
 * Guard rail that blocks a {@code removeInstanceTag} request when it would leave one or more
 * partitions of a tag-restricted WAGED resource unable to place all their replicas. A WAGED resource
 * with an {@code INSTANCE_GROUP_TAG} may only place replicas on instances carrying that tag, so
 * removing the tag from an instance shrinks the assignable pool for those resources exactly the way
 * draining an instance does &mdash; the same failure class as
 * {@link InstanceOperationRebalanceFeasibilityGuardrailRule}. Catching it before the ZK write turns a
 * silent, cluster-wide {@code CAPACITY_DEFICIT} into an actionable {@code 400}.
 * <p>
 * <b>Trigger.</b> Only the tags the instance actually carries are considered, and only when at least
 * one of them is an {@code INSTANCE_GROUP_TAG} of some WAGED resource. Removing a tag no WAGED
 * resource is pinned to cannot change WAGED placement, so those requests short-circuit before the
 * (relatively expensive) double what-if.
 * <p>
 * <b>Check.</b> The rule delegates to the shared {@link WagedRebalanceFeasibilityWhatIf}: it runs the
 * read-only WAGED what-if (via the injected {@link WagedAssignmentProvider}) twice &mdash; once on
 * current state (baseline) and once with the tags removed from a copy of the target's
 * {@link InstanceConfig} (candidate) &mdash; and flags only partitions whose placeable replica count
 * drops between them, so a pre-existing deficit is never blamed on this removal. WAGED reads instance
 * tags straight from {@link InstanceConfig}, so the candidate run already reflects the shrunken pool.
 * <p>
 * <b>Behavior.</b> Enforced by default, with a dedicated per-cluster kill switch
 * ({@link ClusterConfig#setInstanceTagRebalanceGuardrailEnabled(boolean)}); when explicitly disabled
 * the rule returns before any simulation or ZK read. The verdict is overridable with
 * {@code force=true} at the REST layer, it exempts {@code ANY_LIVEINSTANCE} resources, and fails
 * closed with a force-able message if the baseline what-if itself cannot be computed.
 */
public class InstanceTagRebalanceFeasibilityGuardrailRule implements GuardrailRule {
  public static final String RULE_ID = "INSTANCE_TAG_REMOVAL_CAUSES_WAGED_UNPLACEABLE";

  @Override
  public String getId() {
    return RULE_ID;
  }

  @Override
  public ValidationResult validate(GuardrailContext context) {
    String instanceName = context.getInstanceName();
    List<String> proposedRemovedTags = context.getProposedRemovedInstanceTags();
    if (instanceName == null || proposedRemovedTags == null || proposedRemovedTags.isEmpty()) {
      // Not a tag-removal mutation; nothing for this rule to certify.
      return ValidationResult.feasible();
    }

    WagedAssignmentProvider provider = context.getWagedAssignmentProvider();
    if (provider == null) {
      // No what-if seam was supplied, so this call is not wired for simulation. Certify feasible
      // rather than block every removeInstanceTag on a wiring gap; the endpoints that intend to
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
    if (!clusterConfig.isInstanceTagRebalanceGuardrailEnabled()) {
      // Enforced by default; this is the per-cluster kill switch. Returning here (before the WAGED
      // what-if and the fail-closed reads below) backs the rule out for every caller with a single
      // ClusterConfig change when it is explicitly disabled.
      return ValidationResult.feasible();
    }

    InstanceConfig currentConfig =
        dataAccessor.getProperty(keyBuilder.instanceConfig(instanceName));
    if (currentConfig == null) {
      // No config to change; let the write path reject a missing instance.
      return ValidationResult.feasible();
    }

    // Only the tags the instance actually carries can be removed; the rest are no-ops.
    List<String> effectiveRemovedTags = new ArrayList<>();
    for (String tag : proposedRemovedTags) {
      if (tag != null && currentConfig.containsTag(tag)) {
        effectiveRemovedTags.add(tag);
      }
    }
    if (effectiveRemovedTags.isEmpty()) {
      // The instance carries none of these tags, so the removal changes nothing.
      return ValidationResult.feasible();
    }

    List<IdealState> wagedIdealStates =
        WagedRebalanceFeasibilityWhatIf.collectWagedIdealStates(dataAccessor);
    if (wagedIdealStates.isEmpty()) {
      // No WAGED resources: there is no WAGED global rebalance for this removal to break.
      return ValidationResult.feasible();
    }

    // Pre-filter: a removal can only shrink placement for a resource pinned (INSTANCE_GROUP_TAG) to a
    // removed tag. If no WAGED resource is pinned to any of these tags, skip the double what-if.
    Set<String> wagedGroupTags = new HashSet<>();
    for (IdealState idealState : wagedIdealStates) {
      String groupTag = idealState.getInstanceGroupTag();
      if (groupTag != null) {
        wagedGroupTags.add(groupTag);
      }
    }
    boolean affectsPinnedResource = false;
    for (String tag : effectiveRemovedTags) {
      if (wagedGroupTags.contains(tag)) {
        affectsPinnedResource = true;
        break;
      }
    }
    if (!affectsPinnedResource) {
      return ValidationResult.feasible();
    }

    // Candidate config = the target with the tags removed. Copy the ZNRecord so the baseline config
    // object (read above and reused by the what-if) is never mutated.
    InstanceConfig candidateConfig = new InstanceConfig(new ZNRecord(currentConfig.getRecord()));
    for (String tag : effectiveRemovedTags) {
      candidateConfig.removeTag(tag);
    }

    return WagedRebalanceFeasibilityWhatIf.evaluate(context, clusterConfig, instanceName,
        currentConfig, candidateConfig, wagedIdealStates,
        "removal of instance tag(s) " + effectiveRemovedTags, RULE_ID);
  }
}
