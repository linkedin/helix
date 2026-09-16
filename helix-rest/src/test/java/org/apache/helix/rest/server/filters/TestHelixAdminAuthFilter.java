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

import javax.ws.rs.HttpMethod;
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
 * (no server, no ZooKeeper) that assert exactly when the filter enforces the helix-admin role.
 *
 * <p>The gate is skipped for one case only: a {@code POST} carrying a non-destructive
 * {@code command} (the multi-command updateInstance/updateCluster handlers doing e.g.
 * enable/disable). Everything else is gated: {@code POST} with a destructive command, and every
 * non-POST request regardless of any query param. The non-POST rule matters because
 * {@code deleteInstance} (a {@code DELETE}) does not bind a {@code command} param, so a stray
 * {@code ?command=...} on it must not skip the role check.
 */
public class TestHelixAdminAuthFilter {

  private static ContainerRequestContext mockRequest(String method, String command) {
    ContainerRequestContext request = Mockito.mock(ContainerRequestContext.class);
    UriInfo uriInfo = Mockito.mock(UriInfo.class);
    MultivaluedMap<String, String> params = new MultivaluedHashMap<>();
    if (command != null) {
      params.putSingle("command", command);
    }
    Mockito.when(uriInfo.getQueryParameters()).thenReturn(params);
    Mockito.when(request.getUriInfo()).thenReturn(uriInfo);
    Mockito.when(request.getMethod()).thenReturn(method);
    return request;
  }

  private static AuthValidator validatorReturning(boolean allow) {
    AuthValidator validator = Mockito.mock(AuthValidator.class);
    Mockito.when(validator.validate(Mockito.any(), Mockito.anyString())).thenReturn(allow);
    return validator;
  }

  @Test
  public void deleteInstanceShapeIsGated() {
    // DELETE .../instances/{name} with no command -> always run the role check.
    AuthValidator validator = validatorReturning(true);
    ContainerRequestContext request = mockRequest(HttpMethod.DELETE, null);

    new HelixAdminAuthFilter(validator).filter(request);

    Mockito.verify(validator).validate(request, HelixAdminAuthFilter.HELIX_ADMIN_ROLE);
    Mockito.verify(request, Mockito.never()).abortWith(Mockito.any());
  }

  @Test
  public void deleteInstanceWithStrayCommandParamIsStillGated() {
    // The bypass Pratyush found: deleteInstance does not bind @QueryParam("command"), so JAX-RS
    // ignores a stray "?command=enable", but the filter reads it off the URI. The gate must NOT be
    // skipped just because a command string is present on a non-POST request.
    for (String strayCommand : new String[] {"enable", "disable", "purgeOfflineParticipants"}) {
      AuthValidator validator = validatorReturning(true);
      ContainerRequestContext request = mockRequest(HttpMethod.DELETE, strayCommand);

      new HelixAdminAuthFilter(validator).filter(request);

      Mockito.verify(validator).validate(request, HelixAdminAuthFilter.HELIX_ADMIN_ROLE);
      Mockito.verify(request, Mockito.never()).abortWith(Mockito.any());
    }
  }

  @Test
  public void destructiveCommandsAreGated() {
    for (String command : new String[] {"purgeOfflineParticipants", "forceKillInstance"}) {
      AuthValidator validator = validatorReturning(true);
      ContainerRequestContext request = mockRequest(HttpMethod.POST, command);

      new HelixAdminAuthFilter(validator).filter(request);

      Mockito.verify(validator).validate(request, HelixAdminAuthFilter.HELIX_ADMIN_ROLE);
      Mockito.verify(request, Mockito.never()).abortWith(Mockito.any());
    }
  }

  @Test
  public void nonDestructiveCommandsAreNotGated() {
    // Non-destructive commands on the POST multi-command handlers must not be role-gated, otherwise
    // annotating updateCluster/updateInstance would over-restrict unrelated operations.
    for (String command : new String[] {"enable", "disable", "activate", "addInstanceTag"}) {
      AuthValidator validator = validatorReturning(false);
      ContainerRequestContext request = mockRequest(HttpMethod.POST, command);

      new HelixAdminAuthFilter(validator).filter(request);

      Mockito.verify(validator, Mockito.never()).validate(Mockito.any(), Mockito.anyString());
      Mockito.verify(request, Mockito.never()).abortWith(Mockito.any());
    }
  }

  @Test
  public void deniedRoleOnDestructiveCommandIsForbidden() {
    AuthValidator validator = validatorReturning(false);
    ContainerRequestContext request = mockRequest(HttpMethod.POST, "purgeOfflineParticipants");

    new HelixAdminAuthFilter(validator).filter(request);

    Mockito.verify(request).abortWith(Mockito.argThat(
        (Response response) -> response.getStatus() == Response.Status.FORBIDDEN.getStatusCode()));
  }

  @Test
  public void deniedRoleOnDeleteInstanceIsForbidden() {
    AuthValidator validator = validatorReturning(false);
    ContainerRequestContext request = mockRequest(HttpMethod.DELETE, null);

    new HelixAdminAuthFilter(validator).filter(request);

    Mockito.verify(request).abortWith(Mockito.argThat(
        (Response response) -> response.getStatus() == Response.Status.FORBIDDEN.getStatusCode()));
  }

  @Test
  public void deniedRoleOnDeleteWithStrayCommandIsForbidden() {
    // The bypass closed: even with a stray "?command=enable", a denied role still yields 403.
    AuthValidator validator = validatorReturning(false);
    ContainerRequestContext request = mockRequest(HttpMethod.DELETE, "enable");

    new HelixAdminAuthFilter(validator).filter(request);

    Mockito.verify(request).abortWith(Mockito.argThat(
        (Response response) -> response.getStatus() == Response.Status.FORBIDDEN.getStatusCode()));
  }

  @Test
  public void duplicateCommandParamUsesFirstValue() {
    // Defense against the filter/handler read-divergence bug class: the filter reads
    // getQueryParameters().getFirst("command"), and a single-String @QueryParam binds the FIRST
    // value too, so the filter and the handler always agree on which command applies.
    // First value non-destructive -> not gated (matches the handler running that first command).
    AuthValidator validatorA = validatorReturning(false);
    ContainerRequestContext requestA = Mockito.mock(ContainerRequestContext.class);
    UriInfo uriInfoA = Mockito.mock(UriInfo.class);
    MultivaluedMap<String, String> paramsA = new MultivaluedHashMap<>();
    paramsA.add("command", "enable");
    paramsA.add("command", "purgeOfflineParticipants");
    Mockito.when(uriInfoA.getQueryParameters()).thenReturn(paramsA);
    Mockito.when(requestA.getUriInfo()).thenReturn(uriInfoA);
    Mockito.when(requestA.getMethod()).thenReturn(HttpMethod.POST);
    new HelixAdminAuthFilter(validatorA).filter(requestA);
    Mockito.verify(validatorA, Mockito.never()).validate(Mockito.any(), Mockito.anyString());

    // First value destructive -> gated.
    AuthValidator validatorB = validatorReturning(true);
    ContainerRequestContext requestB = Mockito.mock(ContainerRequestContext.class);
    UriInfo uriInfoB = Mockito.mock(UriInfo.class);
    MultivaluedMap<String, String> paramsB = new MultivaluedHashMap<>();
    paramsB.add("command", "purgeOfflineParticipants");
    paramsB.add("command", "enable");
    Mockito.when(uriInfoB.getQueryParameters()).thenReturn(paramsB);
    Mockito.when(requestB.getUriInfo()).thenReturn(uriInfoB);
    Mockito.when(requestB.getMethod()).thenReturn(HttpMethod.POST);
    new HelixAdminAuthFilter(validatorB).filter(requestB);
    Mockito.verify(validatorB).validate(requestB, HelixAdminAuthFilter.HELIX_ADMIN_ROLE);
  }

  @Test
  public void postWithNoCommandIsGated() {
    // A POST with no command cannot dispatch to a non-destructive operation (updateInstance/
    // updateCluster reject a null command with 400), so gating it is correct and fails closed.
    AuthValidator validator = validatorReturning(true);
    ContainerRequestContext request = mockRequest(HttpMethod.POST, null);

    new HelixAdminAuthFilter(validator).filter(request);

    Mockito.verify(validator).validate(request, HelixAdminAuthFilter.HELIX_ADMIN_ROLE);
  }

  @Test
  public void nonPostVerbsAreAlwaysGatedRegardlessOfCommand() {
    // Only POST consults the command param. Any other verb on a @HelixAdminAuth endpoint (today only
    // DELETE deleteInstance, but PUT/GET too if annotated later) is always gated, so a stray command
    // param can never open a bypass on a non-POST endpoint. Fails closed by construction.
    for (String method : new String[] {HttpMethod.DELETE, HttpMethod.PUT, HttpMethod.GET}) {
      AuthValidator validator = validatorReturning(true);
      ContainerRequestContext request = mockRequest(method, "enable");

      new HelixAdminAuthFilter(validator).filter(request);

      Mockito.verify(validator).validate(request, HelixAdminAuthFilter.HELIX_ADMIN_ROLE);
    }
  }

  @Test
  public void grantedRoleIsNotForbidden() {
    AuthValidator validator = validatorReturning(true);
    ContainerRequestContext request = mockRequest(HttpMethod.POST, "forceKillInstance");

    new HelixAdminAuthFilter(validator).filter(request);

    Mockito.verify(request, Mockito.never()).abortWith(Mockito.any());
  }
}
