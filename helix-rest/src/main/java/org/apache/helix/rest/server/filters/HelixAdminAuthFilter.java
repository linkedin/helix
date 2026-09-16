package org.apache.helix.rest.server.filters;

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

import java.util.Set;

import javax.ws.rs.HttpMethod;
import javax.ws.rs.container.ContainerRequestContext;
import javax.ws.rs.container.ContainerRequestFilter;
import javax.ws.rs.core.Response;
import javax.ws.rs.ext.Provider;

import org.apache.helix.rest.server.authValidator.AuthValidator;
import org.apache.helix.rest.server.resources.AbstractResource;


/**
 * Request filter for endpoints annotated with {@link HelixAdminAuth}. It enforces that the caller
 * is authorized for the {@link #HELIX_ADMIN_ROLE} role by delegating to
 * {@link AuthValidator#validate(ContainerRequestContext, String)}. Requests that are not authorized
 * for the role are rejected with {@code 403 Forbidden}.
 */
@HelixAdminAuth
@Provider
public class HelixAdminAuthFilter implements ContainerRequestFilter {

  /** Role required to invoke admin-only cluster endpoints such as dropping an instance. */
  public static final String HELIX_ADMIN_ROLE = "helix-admin";

  /**
   * Destructive {@code command} values on multi-command handlers (e.g. {@code updateCluster} and
   * {@code updateInstance}) that remove or forcibly take down an instance and therefore require the
   * {@link #HELIX_ADMIN_ROLE} role. Non-destructive commands on those handlers are not gated.
   *
   * <p>This is an allow-list: a new destructive command added to one of those handlers must be
   * added here too, otherwise it will not be role-gated.
   */
  private static final Set<String> ADMIN_ONLY_COMMANDS = Set.of(
      AbstractResource.Command.purgeOfflineParticipants.name(),
      AbstractResource.Command.forceKillInstance.name());

  private final AuthValidator _authValidator;

  public HelixAdminAuthFilter(AuthValidator authValidator) {
    _authValidator = authValidator;
  }

  @Override
  public void filter(ContainerRequestContext request) {
    // The command-dispatched write handlers (updateInstance, updateCluster) are POSTs that expose
    // both destructive and non-destructive operations through the "command" query param; for those,
    // gate only the destructive commands so unrelated operations (enable/disable/...) are not
    // over-restricted. Every other @HelixAdminAuth endpoint is single-purpose and is always gated.
    //
    // The POST check is load-bearing, not cosmetic: deleteInstance (a DELETE) does not bind a
    // "command" @QueryParam, so JAX-RS ignores a stray "?command=..." on it -- but this filter reads
    // the param straight off the request URI. Without the method guard, "DELETE .../{instance}
    // ?command=enable" would hit the early return and skip the role check while deleteInstance still
    // runs. Because this filter runs post-matching, the HTTP method corresponds exactly to the
    // matched handler (DELETE -> deleteInstance; POST -> updateInstance/updateCluster).
    if (HttpMethod.POST.equals(request.getMethod())) {
      String command = request.getUriInfo().getQueryParameters().getFirst("command");
      if (command != null && !ADMIN_ONLY_COMMANDS.contains(command)) {
        return;
      }
    }
    if (!_authValidator.validate(request, HELIX_ADMIN_ROLE)) {
      request.abortWith(Response.status(Response.Status.FORBIDDEN).build());
    }
  }
}
