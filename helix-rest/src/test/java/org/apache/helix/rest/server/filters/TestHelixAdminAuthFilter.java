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

import javax.ws.rs.container.ContainerRequestContext;
import javax.ws.rs.core.MultivaluedHashMap;
import javax.ws.rs.core.MultivaluedMap;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.UriInfo;

import org.apache.helix.rest.server.authValidator.AuthValidator;
import org.mockito.Mockito;
import org.testng.annotations.Test;


/**
 * Unit tests for {@link HelixAdminAuthFilter}'s command-aware gating. These are pure Mockito tests
 * (no server, no ZooKeeper) that assert exactly when the filter enforces the helix-admin role:
 * always for a request with no {@code command} (the single-purpose deleteInstance endpoint) and for
 * the destructive commands ({@code purgeOfflineParticipants}, {@code forceKillInstance}) that ride
 * on the multi-command updateCluster/updateInstance handlers, but never for other commands on those
 * handlers.
 */
public class TestHelixAdminAuthFilter {

  private static ContainerRequestContext mockRequest(String command) {
    ContainerRequestContext request = Mockito.mock(ContainerRequestContext.class);
    UriInfo uriInfo = Mockito.mock(UriInfo.class);
    MultivaluedMap<String, String> params = new MultivaluedHashMap<>();
    if (command != null) {
      params.putSingle("command", command);
    }
    Mockito.when(uriInfo.getQueryParameters()).thenReturn(params);
    Mockito.when(request.getUriInfo()).thenReturn(uriInfo);
    return request;
  }

  private static AuthValidator validatorReturning(boolean allow) {
    AuthValidator validator = Mockito.mock(AuthValidator.class);
    Mockito.when(validator.validate(Mockito.any(), Mockito.anyString())).thenReturn(allow);
    return validator;
  }

  @Test
  public void noCommandRequestIsGated() {
    // A request with no "command" (e.g. DELETE .../instances/{name}) must always run the role check.
    AuthValidator validator = validatorReturning(true);
    ContainerRequestContext request = mockRequest(null);

    new HelixAdminAuthFilter(validator).filter(request);

    Mockito.verify(validator).validate(request, HelixAdminAuthFilter.HELIX_ADMIN_ROLE);
    Mockito.verify(request, Mockito.never()).abortWith(Mockito.any());
  }

  @Test
  public void destructiveCommandsAreGated() {
    for (String command : new String[] {"purgeOfflineParticipants", "forceKillInstance"}) {
      AuthValidator validator = validatorReturning(true);
      ContainerRequestContext request = mockRequest(command);

      new HelixAdminAuthFilter(validator).filter(request);

      Mockito.verify(validator).validate(request, HelixAdminAuthFilter.HELIX_ADMIN_ROLE);
      Mockito.verify(request, Mockito.never()).abortWith(Mockito.any());
    }
  }

  @Test
  public void nonDestructiveCommandsAreNotGated() {
    // Non-destructive commands on the same multi-command handlers must not be role-gated, otherwise
    // annotating updateCluster/updateInstance would over-restrict unrelated operations.
    for (String command : new String[] {"enable", "disable", "activate", "addInstanceTag"}) {
      AuthValidator validator = validatorReturning(false);
      ContainerRequestContext request = mockRequest(command);

      new HelixAdminAuthFilter(validator).filter(request);

      Mockito.verify(validator, Mockito.never()).validate(Mockito.any(), Mockito.anyString());
      Mockito.verify(request, Mockito.never()).abortWith(Mockito.any());
    }
  }

  @Test
  public void deniedRoleOnDestructiveCommandIsForbidden() {
    AuthValidator validator = validatorReturning(false);
    ContainerRequestContext request = mockRequest("purgeOfflineParticipants");

    new HelixAdminAuthFilter(validator).filter(request);

    Mockito.verify(request).abortWith(Mockito.argThat(
        (Response response) -> response.getStatus() == Response.Status.FORBIDDEN.getStatusCode()));
  }

  @Test
  public void deniedRoleOnNoCommandIsForbidden() {
    AuthValidator validator = validatorReturning(false);
    ContainerRequestContext request = mockRequest(null);

    new HelixAdminAuthFilter(validator).filter(request);

    Mockito.verify(request).abortWith(Mockito.argThat(
        (Response response) -> response.getStatus() == Response.Status.FORBIDDEN.getStatusCode()));
  }

  @Test
  public void grantedRoleIsNotForbidden() {
    AuthValidator validator = validatorReturning(true);
    ContainerRequestContext request = mockRequest("forceKillInstance");

    new HelixAdminAuthFilter(validator).filter(request);

    Mockito.verify(request, Mockito.never()).abortWith(Mockito.any());
  }
}
