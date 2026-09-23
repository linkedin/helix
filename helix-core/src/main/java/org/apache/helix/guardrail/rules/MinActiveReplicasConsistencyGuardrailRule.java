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

import org.apache.helix.PropertyKey;
import org.apache.helix.controller.rebalancer.util.DelayedRebalanceUtil;
import org.apache.helix.guardrail.GuardrailContext;
import org.apache.helix.guardrail.GuardrailRule;
import org.apache.helix.guardrail.ReadOnlyDataAccessor;
import org.apache.helix.guardrail.ValidationResult;
import org.apache.helix.guardrail.Violation;
import org.apache.helix.model.IdealState;
import org.apache.helix.model.ResourceConfig;

/**
 * Guard rail that blocks an ideal-state edit which would <em>introduce or worsen</em> a resource
 * having a minimum active replica count greater than its replica count.
 * <p>
 * {@code MIN_ACTIVE_REPLICAS} is the floor the controller tries to keep in an active (top or
 * secondary) state during delayed rebalance and node outages, while {@code REPLICAS} is the total
 * number of replicas the resource ever places per partition. A partition can never have more active
 * replicas than it has replicas, so {@code MIN_ACTIVE_REPLICAS > REPLICAS} is a logically
 * inconsistent configuration: every partition is permanently accounted below its minimum active
 * count. That defeats delayed rebalance (the mechanism that holds replicas in place to avoid churn
 * assumes the minimum can be met) and any min-active-based health/throttling guarantee. The raw
 * {@code POST .../idealState} write path never validates this today, so an inconsistent ideal state
 * is accepted into ZooKeeper and only manifests later as degraded rebalance behavior.
 * <p>
 * The effective minimum active replica count is resolved exactly the way the rebalancer resolves it
 * ({@link DelayedRebalanceUtil#getMinActiveReplica}): the resource config value takes precedence,
 * then the ideal state value, then the replica count itself (which is always safe). The existing
 * resource config is read from the metadata store because an ideal-state edit does not change it;
 * this makes <em>lowering</em> {@code REPLICAS} below an existing {@code MIN_ACTIVE_REPLICAS}
 * detectable, not only edits that raise {@code MIN_ACTIVE_REPLICAS}.
 * <p>
 * This is a <em>delta</em> guard: it compares the resulting (merged, post-write) ideal state
 * against the current one and fires only when the edit makes things worse &mdash; that is, when the
 * post-write gap {@code (effectiveMinActive - replicas)} is positive <em>and</em> strictly larger
 * than the gap the resource already has. A resource that is already inconsistent is deliberately
 * grandfathered: re-writing it unchanged, editing an unrelated field, or an edit that shrinks the
 * gap is allowed, because such states demonstrably exist in practice and blocking every subsequent
 * edit to them would be surprising and unhelpful. The guard's job is to stop an operator from
 * turning a consistent resource inconsistent, or from deepening an existing inconsistency.
 * <p>
 * The rule is deliberately conservative about non-concrete replica counts. It only compares when
 * the post-write {@code REPLICAS} is a concrete non-negative integer: a value of
 * {@code ANY_LIVEINSTANCE} (or any other non-numeric value) has no fixed replica count, so the
 * comparison is a runtime concern and is left to downstream logic. When {@code MIN_ACTIVE_REPLICAS}
 * is unset it defaults to the replica count, so it can never exceed it and the rule reports
 * feasible.
 */
public class MinActiveReplicasConsistencyGuardrailRule implements GuardrailRule {

  public static final String RULE_ID = "MIN_ACTIVE_REPLICAS_EXCEEDS_REPLICAS";

  @Override
  public String getId() {
    return RULE_ID;
  }

  @Override
  public ValidationResult validate(GuardrailContext context) {
    IdealState proposedIdealState = context.getProposedIdealState();
    if (proposedIdealState == null) {
      // Not an ideal-state mutation; nothing for this rule to certify.
      return ValidationResult.feasible();
    }
    String resourceName = proposedIdealState.getResourceName();

    // REPLICAS must be a concrete non-negative integer to compare statically. ANY_LIVEINSTANCE (and
    // any other non-numeric value) has no fixed replica count, so the min-active comparison depends
    // on the live cluster and is deferred to runtime.
    Integer proposedReplicaCount = parseConcreteReplicaCount(proposedIdealState.getReplicas());
    if (proposedReplicaCount == null) {
      return ValidationResult.feasible();
    }

    // Resolve the effective minimum active replica count the same way the rebalancer does: the
    // resource config value wins, then the ideal state value, then the replica count. The resource
    // config is unchanged by an ideal-state edit, so the same value applies to the before and after
    // states.
    ResourceConfig resourceConfig = readResourceConfig(context, resourceName);
    int proposedMinActiveReplicas = DelayedRebalanceUtil.getMinActiveReplica(resourceConfig,
        proposedIdealState, proposedReplicaCount);
    int proposedGap = proposedMinActiveReplicas - proposedReplicaCount;
    if (proposedGap <= 0) {
      // Resulting state is consistent: MIN_ACTIVE_REPLICAS <= REPLICAS.
      return ValidationResult.feasible();
    }

    // The resulting state is inconsistent. Only block when this edit introduces or worsens it; a
    // pre-existing gap of equal-or-greater size is grandfathered so re-writing an already
    // inconsistent resource, editing an unrelated field, or shrinking the gap is not rejected.
    int existingGap = existingGap(context, resourceName, resourceConfig);
    if (proposedGap <= existingGap) {
      return ValidationResult.feasible();
    }

    return ValidationResult.infeasible(Violation.newBuilder(RULE_ID)
        .resource(resourceName)
        .message(String.format(
            "Updating the ideal state of resource %s would set MIN_ACTIVE_REPLICAS (%d) greater "
                + "than REPLICAS (%d). A partition can never have more active replicas than it has "
                + "replicas, so every partition would be permanently treated as below its minimum "
                + "active count, defeating delayed rebalance and min-active health guarantees. "
                + "Lower MIN_ACTIVE_REPLICAS to at most %d, or raise REPLICAS to at least %d.",
            resourceName, proposedMinActiveReplicas, proposedReplicaCount, proposedReplicaCount,
            proposedMinActiveReplicas))
        .build());
  }

  /**
   * The current (pre-write) {@code effectiveMinActive - replicas} gap for the resource, using the
   * same resource config that applies to the proposed state. Returns {@link Integer#MIN_VALUE} when
   * no concrete benign baseline can be established (the ideal state does not yet exist, or its
   * replica count is not a concrete number) so that any resulting inconsistency is treated as newly
   * introduced.
   */
  private static int existingGap(GuardrailContext context, String resourceName,
      ResourceConfig resourceConfig) {
    IdealState existingIdealState = readIdealState(context, resourceName);
    if (existingIdealState == null) {
      return Integer.MIN_VALUE;
    }
    Integer existingReplicaCount = parseConcreteReplicaCount(existingIdealState.getReplicas());
    if (existingReplicaCount == null) {
      return Integer.MIN_VALUE;
    }
    int existingMinActiveReplicas = DelayedRebalanceUtil.getMinActiveReplica(resourceConfig,
        existingIdealState, existingReplicaCount);
    return existingMinActiveReplicas - existingReplicaCount;
  }

  private static IdealState readIdealState(GuardrailContext context, String resourceName) {
    if (resourceName == null) {
      return null;
    }
    ReadOnlyDataAccessor dataAccessor = context.getDataAccessor();
    PropertyKey.Builder keyBuilder = dataAccessor.keyBuilder();
    return dataAccessor.getProperty(keyBuilder.idealStates(resourceName));
  }

  private static ResourceConfig readResourceConfig(GuardrailContext context, String resourceName) {
    if (resourceName == null) {
      return null;
    }
    ReadOnlyDataAccessor dataAccessor = context.getDataAccessor();
    PropertyKey.Builder keyBuilder = dataAccessor.keyBuilder();
    return dataAccessor.getProperty(keyBuilder.resourceConfig(resourceName));
  }

  private static Integer parseConcreteReplicaCount(String replicas) {
    if (replicas == null) {
      return null;
    }
    try {
      int value = Integer.parseInt(replicas.trim());
      // A negative replica count is itself malformed and outside this rule's concern; only certify
      // (or reject) when the replica count is a concrete non-negative number.
      return value >= 0 ? value : null;
    } catch (NumberFormatException e) {
      return null;
    }
  }
}
