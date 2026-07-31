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

import org.apache.helix.HelixDataAccessor;
import org.apache.helix.PropertyKey;
import org.apache.helix.guardrail.GuardrailContext;
import org.apache.helix.guardrail.GuardrailRule;
import org.apache.helix.guardrail.ValidationResult;
import org.apache.helix.guardrail.Violation;
import org.apache.helix.model.LiveInstance;

/**
 * Guard rail that blocks removing an instance while it is still live -- that is, while its ephemeral
 * {@code LIVEINSTANCES} znode is present, meaning the participant is connected and may still be
 * serving partitions.
 * <p>
 * Dropping a live instance is already rejected deep in the admin layer by
 * {@code ZKHelixAdmin.dropInstance} (which throws once it sees the live znode). Evaluating the same
 * condition here, in the guard rail pipeline, lets the REST layer surface a structured verdict and
 * answer dry-run ("simulate") requests truthfully -- before any ZooKeeper write is attempted --
 * rather than only discovering the problem as a bare exception once the drop is under way. The admin
 * layer check remains the authoritative enforcer (it also protects non-REST callers and closes the
 * time-of-check-to-time-of-use gap); this rule is the pre-flight, dry-run-friendly front end for it.
 * <p>
 * The rule reads only the target instance's single {@code LIVEINSTANCES} znode, so it adds
 * negligible load compared with checks that scan every resource's ideal state / external view.
 */
public class LiveInstanceGuardrailRule implements GuardrailRule {
  public static final String RULE_ID = "LIVE_INSTANCE_ON_INSTANCE_DROP";

  @Override
  public String getId() {
    return RULE_ID;
  }

  @Override
  public ValidationResult validate(GuardrailContext context) {
    String instanceName = context.getInstanceName();
    if (instanceName == null) {
      // No target instance to evaluate; nothing for this rule to certify.
      return ValidationResult.feasible();
    }

    HelixDataAccessor dataAccessor = context.getDataAccessor();
    PropertyKey liveInstanceKey = dataAccessor.keyBuilder().liveInstance(instanceName);
    LiveInstance liveInstance = dataAccessor.getProperty(liveInstanceKey);
    if (liveInstance == null) {
      // No ephemeral live znode: the participant is disconnected, so the drop is safe to proceed.
      return ValidationResult.feasible();
    }

    Violation violation = Violation.newBuilder(RULE_ID)
        .message(String.format(
            "Instance %s is still live (participant session %s is connected) and cannot be dropped. "
                + "Stop the participant so its LIVEINSTANCES znode is released, then retry. Dropping "
                + "a live instance is rejected by the cluster and would risk removing metadata for "
                + "partitions it is actively serving.", instanceName, liveInstance.getEphemeralOwner()))
        .build();
    return ValidationResult.infeasible(violation);
  }
}
