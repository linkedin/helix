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
import org.apache.helix.guardrail.GuardrailContext;
import org.apache.helix.guardrail.GuardrailRule;
import org.apache.helix.guardrail.ReadOnlyDataAccessor;
import org.apache.helix.guardrail.ValidationResult;
import org.apache.helix.guardrail.WagedAssignmentProvider;
import org.apache.helix.model.ClusterConfig;
import org.apache.helix.model.IdealState;

/**
 * Guard rail that blocks an {@code updateResourceIdealState} edit to a WAGED resource when the proposed
 * (merged, post-write) ideal state cannot be placed on the current cluster &mdash; i.e. the edit would
 * cause a WAGED rebalance failure. Catching it before the ZK write turns a silent, cluster-wide
 * {@code CAPACITY_DEFICIT} (or an indefinitely under-replicated resource) into an actionable
 * {@code 400}.
 * <p>
 * This complements {@link MinActiveReplicasConsistencyGuardrailRule} on the same endpoint: that rule
 * catches a purely <em>logical</em> inconsistency ({@code MIN_ACTIVE_REPLICAS > REPLICAS}) with a cheap
 * field comparison; this rule catches a <em>capacity</em> infeasibility (e.g. raising {@code REPLICAS}
 * or partition count beyond what the live instances can hold) by actually running the WAGED rebalancer.
 * <p>
 * <b>Check.</b> The rule delegates to the shared {@link WagedRebalanceFeasibilityWhatIf}: it runs the
 * read-only WAGED what-if (via the injected {@link WagedAssignmentProvider}) on the cluster with the
 * edited resource's ideal state swapped for the proposed one, and (a) flags any partition of the edited
 * resource that cannot place the replica count the proposed ideal state asks for, and (b) flags any
 * <em>other</em> WAGED resource that loses placeable replicas as collateral. Comparing the edited
 * resource against its own proposed demand (not the baseline) is what lets a replica increase that
 * current capacity cannot satisfy be caught while a deliberate replica decrease is not falsely flagged.
 * Because the what-if runs the real {@code ReadOnlyWagedRebalancer}, the verdict already reflects
 * WAGED's own hard constraints (capacity, replica-count, fault-zone); the rule adds no capacity math of
 * its own.
 * <p>
 * <b>Behavior.</b> The rule is always on &mdash; it runs on every {@code updateResourceIdealState}
 * edit, with no per-cluster enable flag, so protection does not depend on anyone remembering to turn
 * it on (matching {@link MinActiveReplicasConsistencyGuardrailRule} on the same endpoint). The verdict
 * is overridable with {@code force=true} (an operator may knowingly accept transient under-replication)
 * and previewable with {@code dryRun=true}. It is best-effort admission control, not a serialized
 * invariant: computed from a snapshot, it only concerns WAGED (non-{@code ANY_LIVEINSTANCE}) resources.
 * Because it is always on, it only blocks when it can attribute a new deficit to the edit: if a
 * baseline WAGED assignment cannot be computed for the cluster at all (the cluster is already unable to
 * rebalance), the rule certifies the edit rather than blocking every edit on an already-unhealthy
 * cluster.
 */
public class IdealStateRebalanceFeasibilityGuardrailRule implements GuardrailRule {
  public static final String RULE_ID = "IDEAL_STATE_REBALANCE_FEASIBILITY";

  @Override
  public String getId() {
    return RULE_ID;
  }

  @Override
  public ValidationResult validate(GuardrailContext context) {
    IdealState proposedIdealState = context.getProposedIdealState();
    if (proposedIdealState == null) {
      // Not an ideal-state edit; nothing for this rule to certify.
      return ValidationResult.feasible();
    }

    WagedAssignmentProvider provider = context.getWagedAssignmentProvider();
    if (provider == null) {
      // No what-if seam was supplied, so this call is not wired for simulation. Certify feasible
      // rather than block every ideal-state edit on a wiring gap; the endpoints that intend to
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

    // The what-if itself short-circuits to feasible for a non-WAGED (or ANY_LIVEINSTANCE) proposed
    // ideal state, so no separate WAGED check is needed here.
    return WagedRebalanceFeasibilityWhatIf.evaluateIdealStateChange(context, clusterConfig,
        proposedIdealState,
        "Lower the resource's replica count, or add instance capacity to the cluster", RULE_ID);
  }
}
