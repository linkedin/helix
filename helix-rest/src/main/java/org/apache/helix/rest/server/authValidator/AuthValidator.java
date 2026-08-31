package org.apache.helix.rest.server.authValidator;

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

import javax.ws.rs.container.ContainerRequestContext;


public interface AuthValidator {
  boolean validate(ContainerRequestContext request);

  /**
   * Validate whether the caller of the given request is authorized to act with the specified role.
   * This is used by role-restricted endpoints (e.g. those annotated with {@code @HelixAdminAuth})
   * to enforce that only a specific role may access them.
   * <p>
   * This method is intentionally <b>abstract</b>: every {@link AuthValidator} must implement it
   * explicitly and grant access only when the caller holds {@code role}. It is deliberately not a
   * default that delegates to {@link #validate(ContainerRequestContext)} — such a default fails
   * open, silently re-running only the base authorization so any caller who passes base auth also
   * passes the role check.
   * <p>
   * The abstract contract matters most for <b>decorators</b> — a validator that wraps another one
   * (for quota, metrics, etc.). A decorator MUST forward this call to its delegate
   * (e.g. {@code delegate.validate(request, role)}); forwarding only
   * {@link #validate(ContainerRequestContext)} would skip the wrapped role check and silently
   * disable the gate. Keeping this method abstract forces every implementor — decorators
   * included — to make that choice at compile time instead of inheriting a fail-open default.
   *
   * @param request the incoming request context
   * @param role the role required to access the endpoint (e.g. {@code helix-admin})
   * @return true if the caller is authorized for the given role
   */
  boolean validate(ContainerRequestContext request, String role);
}
