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
   * {@code @HelixZKAdminAuth}) to enforce that only a specific role may access them.
   * <p>
   * The default implementation delegates to {@link #validate(ContainerRequestContext)} so existing
   * validators remain backward-compatible. Implementations that need to enforce role-based access
   * should override this method and grant access only when the caller holds {@code role}.
   *
   * @param request the incoming request context
   * @param role the role required to access the endpoint (e.g. {@code helix-zk-admin})
   * @return true if the caller is authorized for the given role
   */
  default boolean validate(ContainerRequestContext request, String role) {
    return validate(request);
  }
}
