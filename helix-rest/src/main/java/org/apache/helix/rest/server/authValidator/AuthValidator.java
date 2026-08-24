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
   * This is used by role-restricted endpoints (e.g. those annotated with
   * {@code @HelixAdminAuth}) to enforce that only a specific role may access them.
   * <p>
   * The default implementation is a backward-compatible <b>no-op</b>: it delegates to
   * {@link #validate(ContainerRequestContext)} and ignores {@code role}, so it adds no role-based
   * restriction of its own. In particular, for an endpoint that is also covered by the base
   * authorization (such as one annotated with {@code @ClusterAuth}) this simply runs that same
   * check again, so any caller who passes base authorization also passes the role check.
   * <p>
   * To actually enforce a role, provide an {@link AuthValidator} that <b>overrides</b> this method
   * and grants access only when the caller holds {@code role}. When the configured validator does
   * not override it, {@code HelixRestServer} logs a startup warning so operators are not misled into
   * believing role-restricted endpoints are enforced.
   *
   * @param request the incoming request context
   * @param role the role required to access the endpoint (e.g. {@code helix-admin})
   * @return true if the caller is authorized for the given role
   */
  default boolean validate(ContainerRequestContext request, String role) {
    return validate(request);
  }
}
