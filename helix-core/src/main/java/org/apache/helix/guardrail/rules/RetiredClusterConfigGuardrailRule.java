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

import java.util.Objects;

import org.apache.helix.guardrail.GuardrailContext;
import org.apache.helix.guardrail.GuardrailRule;
import org.apache.helix.guardrail.ValidationResult;
import org.apache.helix.guardrail.Violation;
import org.apache.helix.model.ClusterConfig;
import org.apache.helix.zookeeper.datamodel.ZNRecord;

/**
 * Rejects additions or edits to the retired load-balance threshold while allowing its removal or
 * unchanged retention. Evaluated against the stored snapshot and complete proposed record inside
 * ConfigAccessor's version-checked write, including each concurrent-write retry.
 */
public class RetiredClusterConfigGuardrailRule implements GuardrailRule {
  public static final String RULE_ID = "RETIRED_CLUSTER_CONFIG";
  private static final String RETIRED_KEY = "ERROR_PARTITION_THRESHOLD_FOR_LOAD_BALANCE";

  @Override
  public String getId() {
    return RULE_ID;
  }

  @Override
  public ValidationResult validate(GuardrailContext context) {
    ClusterConfig proposed = context.getProposedClusterConfig();
    if (proposed == null || !proposed.getRecord().getSimpleFields().containsKey(RETIRED_KEY)) {
      return ValidationResult.feasible();
    }
    ClusterConfig current = context.getCurrentClusterConfig();
    if (current != null) {
      ZNRecord stored = current.getRecord();
      if (stored.getSimpleFields().containsKey(RETIRED_KEY)
          && Objects.equals(stored.getSimpleField(RETIRED_KEY),
              proposed.getRecord().getSimpleField(RETIRED_KEY))) {
        return ValidationResult.feasible();
      }
    }
    return ValidationResult.infeasible(Violation.newBuilder(RULE_ID)
        .message(RETIRED_KEY + " is retired and cannot be added or changed. Use "
            + ClusterConfig.ClusterConfigProperty.ERROR_OR_RECOVERY_PARTITION_THRESHOLD_FOR_LOAD_BALANCE
                .name()
            + " instead. Existing unchanged values may be retained or deleted.")
        .build());
  }
}
