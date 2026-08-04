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

package org.apache.helix.guardrail;

/**
 * A pre-write safety check for a proposed cluster mutation.
 * <p>
 * A rule is a pure, read-only function of a {@link GuardrailContext}: it inspects current cluster
 * state and returns a {@link ValidationResult} describing whether the mutation is safe. Rules must
 * not mutate cluster state, which is what lets the same rule power both enforcement and dry-run
 * ("simulate") requests.
 * <p>
 * New guard rails are added simply by implementing this interface and including the rule in the
 * {@link GuardrailPipeline} an endpoint constructs; the framework needs no other changes.
 */
public interface GuardrailRule {
  /** A short, stable identifier for this rule, used in violations and logs. */
  String getId();

  /**
   * Evaluate the mutation described by {@code context} against current cluster state.
   *
   * @param context the cluster state and mutation target
   * @return a feasible result if the mutation is safe, otherwise a result carrying violations
   */
  ValidationResult validate(GuardrailContext context);
}
