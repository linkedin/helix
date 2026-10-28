package org.apache.helix.rest.server.guardrail;

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

/**
 * Notified once for every guard rail validation that helix-rest runs during request preflight,
 * carrying the per-rule outcomes and evaluation latency of that validation.
 * <p>
 * This is the push-based counterpart to the per-namespace JMX metrics that helix-rest already emits
 * for guard rail validations (see
 * {@code org.apache.helix.rest.server.resources.helix.GuardrailMetrics}). Those Codahale metrics
 * suit a standalone server whose registry is scraped by a JMX-to-metrics adaptor. An
 * <em>embedded</em> helix-rest runs no such adaptor, so a listener lets the embedding process
 * forward the same guard rail counts and latency straight into its own metrics pipeline &mdash;
 * without polling or re-exporting the JMX registry. Register implementations through the
 * {@code HelixRestServer} constructor, mirroring how
 * {@link org.apache.helix.rest.server.auditlog.AuditLogger}s are supplied.
 * <p>
 * Implementations must be thread-safe, as helix-rest serves requests concurrently. Notification is
 * best-effort and runs inline on the request path: helix-rest swallows any exception thrown here so
 * a faulty listener can never fail a request, but a listener should still avoid blocking work.
 */
public interface GuardrailValidationListener {

  /**
   * Invoked after a guard rail pipeline has been evaluated for a request, before the request
   * proceeds (or is rejected). The {@code event} describes every rule that ran, its outcome, and how
   * long it took.
   *
   * @param event the outcome of the guard rail validation
   */
  void onValidation(GuardrailValidationEvent event);
}
