package org.apache.helix.rest.server;

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

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import javax.ws.rs.client.Entity;
import javax.ws.rs.container.ContainerRequestContext;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import org.apache.helix.TestHelper;
import org.apache.helix.rest.acl.NoopAclRegister;
import org.apache.helix.rest.common.HelixRestNamespace;
import org.apache.helix.rest.common.HttpConstants;
import org.apache.helix.rest.server.authValidator.AuthValidator;
import org.apache.helix.rest.server.filters.HelixAdminAuthFilter;
import org.apache.helix.rest.server.resources.helix.ClusterAccessor;
import org.apache.http.HttpResponse;
import org.apache.http.client.methods.HttpDelete;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpPut;
import org.apache.http.client.methods.HttpUriRequest;
import org.apache.http.entity.ContentType;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.mockito.Mockito;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.Test;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.when;


public class TestAuthValidator extends AbstractTestClass {
  private String _mockBaseUri;
  private CloseableHttpClient _httpClient;

  private static String CLASSNAME_TEST_DEFAULT_AUTH = "testDefaultAuthValidator";
  private static String CLASSNAME_TEST_CST_AUTH = "testCustomAuthValidator";
  private static String CLASSNAME_TEST_ADMIN_AUTH = "testHelixAdminAuthValidator";

  @AfterClass
  public void afterClass() {
    TestHelper.dropCluster(CLASSNAME_TEST_DEFAULT_AUTH, _gZkClient);
    TestHelper.dropCluster(CLASSNAME_TEST_CST_AUTH, _gZkClient);
    TestHelper.dropCluster(CLASSNAME_TEST_ADMIN_AUTH, _gZkClient);
  }

  @Test
  public void testDefaultAuthValidator() throws JsonProcessingException {
    put("clusters/" + CLASSNAME_TEST_DEFAULT_AUTH, null, Entity.entity("", MediaType.APPLICATION_JSON_TYPE),
        Response.Status.CREATED.getStatusCode());
    String body = get("clusters/", null, Response.Status.OK.getStatusCode(), true);
    JsonNode node = OBJECT_MAPPER.readTree(body);
    String clustersStr = node.get(ClusterAccessor.ClusterProperties.clusters.name()).toString();
    Assert.assertTrue(clustersStr.contains(CLASSNAME_TEST_DEFAULT_AUTH));
  }

  @Test(dependsOnMethods = "testDefaultAuthValidator")
  public void testCustomAuthValidator() throws IOException, InterruptedException {
    int newPort = getBaseUri().getPort() + 1;

    // Start a second server for testing Distributed Leader Election for writes
    _mockBaseUri = HttpConstants.HTTP_PROTOCOL_PREFIX + getBaseUri().getHost() + ":" + newPort;
    _httpClient = HttpClients.createDefault();

    AuthValidator mockAuthValidatorPass = Mockito.mock(AuthValidator.class);
    when(mockAuthValidatorPass.validate(any())).thenReturn(true);
    AuthValidator mockAuthValidatorReject = Mockito.mock(AuthValidator.class);
    when(mockAuthValidatorReject.validate(any())).thenReturn(false);

    List<HelixRestNamespace> namespaces = new ArrayList<>();
    namespaces.add(new HelixRestNamespace(HelixRestNamespace.DEFAULT_NAMESPACE_NAME,
        HelixRestNamespace.HelixMetadataStoreType.ZOOKEEPER, ZK_ADDR, true));

    // Create a server that allows operations based on namespace auth and rejects operations based
    // on cluster auth
    HelixRestServer server =
        new HelixRestServer(namespaces, newPort, getBaseUri().getPath(), Collections.emptyList(),
            mockAuthValidatorReject, mockAuthValidatorPass, new NoopAclRegister());
    server.start();

    HttpUriRequest request =
        buildRequest("/clusters/" + CLASSNAME_TEST_CST_AUTH, HttpConstants.RestVerbs.PUT, "");
    sendRequestAndValidate(request, Response.Status.CREATED.getStatusCode());
    request = buildRequest("/clusters/" + CLASSNAME_TEST_CST_AUTH, HttpConstants.RestVerbs.GET, "");
    sendRequestAndValidate(request, Response.Status.FORBIDDEN.getStatusCode());

    server.shutdown();
    _httpClient.close();

    // Create a server that rejects operations based on namespace auth and allows operations based
    // on cluster auth
    server =
        new HelixRestServer(namespaces, newPort, getBaseUri().getPath(), Collections.emptyList(),
            mockAuthValidatorPass, mockAuthValidatorReject, new NoopAclRegister());
    server.start();
    _httpClient = HttpClients.createDefault();

    request = buildRequest("/clusters/" + CLASSNAME_TEST_CST_AUTH, HttpConstants.RestVerbs.GET, "");
    sendRequestAndValidate(request, Response.Status.OK.getStatusCode());
    request = buildRequest("/clusters", HttpConstants.RestVerbs.GET, "");
    sendRequestAndValidate(request, Response.Status.FORBIDDEN.getStatusCode());

    server.shutdown();
    _httpClient.close();
  }

  /*
   * Verifies the @HelixAdminAuth gate on deleteInstance end-to-end against a real HelixRestServer:
   *  - a real validator that only implements validate(request) exercises the interface's default
   *    delegation (a Mockito mock would stub that default out): base=false -> 403, base=true -> 200
   *    (the default adds no role restriction, matching the documented no-op);
   *  - a real validator that overrides validate(request, role) enforces the role: reject -> 403,
   *    accept -> 200 (a real delete), and the filter forwards exactly HELIX_ADMIN_ROLE;
   *  - the sibling GET getInstanceById (not annotated) is never gated, proving @HelixAdminAuth is
   *    scoped to the method and not the whole resource.
   * Real clusters/instances are created so DELETE/GET reach the actual handlers and return real
   * status codes (deleting a non-existent instance would 400 regardless of the auth outcome).
   */
  @Test(dependsOnMethods = "testCustomAuthValidator")
  public void testHelixAdminAuthValidator() throws IOException, InterruptedException {
    int newPort = getBaseUri().getPort() + 2;
    _mockBaseUri = HttpConstants.HTTP_PROTOCOL_PREFIX + getBaseUri().getHost() + ":" + newPort;

    List<HelixRestNamespace> namespaces = new ArrayList<>();
    namespaces.add(new HelixRestNamespace(HelixRestNamespace.DEFAULT_NAMESPACE_NAME,
        HelixRestNamespace.HelixMetadataStoreType.ZOOKEEPER, ZK_ADDR, true));

    String cluster = CLASSNAME_TEST_ADMIN_AUTH;
    _gSetupTool.addCluster(cluster, true);
    String probeInstance = "probeInstance_12000";           // survives; used for the sibling GET
    String adminDeleteInstance = "adminDelInstance_12001";   // deleted by the role-granting validator
    String delegateDeleteInstance = "delegateDelInstance_12002"; // deleted via default delegation
    _gSetupTool.addInstanceToCluster(cluster, probeInstance);
    _gSetupTool.addInstanceToCluster(cluster, adminDeleteInstance);
    _gSetupTool.addInstanceToCluster(cluster, delegateDeleteInstance);

    // (1) Override REJECTS the admin role: base auth passes but the role check denies -> 403.
    RoleAwareAuthValidator roleReject = new RoleAwareAuthValidator(false);
    HelixRestServer server = new HelixRestServer(namespaces, newPort, getBaseUri().getPath(),
        Collections.emptyList(), roleReject, roleReject, new NoopAclRegister());
    server.start();
    _httpClient = HttpClients.createDefault();
    try {
      // deleteInstance is @HelixAdminAuth -> forbidden even though the instance exists.
      sendRequestAndValidate(
          buildRequest("/clusters/" + cluster + "/instances/" + probeInstance,
              HttpConstants.RestVerbs.DELETE, ""),
          Response.Status.FORBIDDEN.getStatusCode());
      // The filter must forward exactly HELIX_ADMIN_ROLE (not just some arbitrary string).
      Assert.assertEquals(roleReject.getLastRole(), HelixAdminAuthFilter.HELIX_ADMIN_ROLE);
      // getInstanceById on the SAME resource is not @HelixAdminAuth -> not gated -> 200, not 403.
      sendRequestAndValidate(
          buildRequest("/clusters/" + cluster + "/instances/" + probeInstance,
              HttpConstants.RestVerbs.GET, ""),
          Response.Status.OK.getStatusCode());
    } finally {
      server.shutdown();
      _httpClient.close();
    }

    // (2) Override GRANTS the admin role: deleteInstance succeeds with a real 200 OK.
    RoleAwareAuthValidator roleAccept = new RoleAwareAuthValidator(true);
    server = new HelixRestServer(namespaces, newPort, getBaseUri().getPath(),
        Collections.emptyList(), roleAccept, roleAccept, new NoopAclRegister());
    server.start();
    _httpClient = HttpClients.createDefault();
    try {
      sendRequestAndValidate(
          buildRequest("/clusters/" + cluster + "/instances/" + adminDeleteInstance,
              HttpConstants.RestVerbs.DELETE, ""),
          Response.Status.OK.getStatusCode());
      Assert.assertEquals(roleAccept.getLastRole(), HelixAdminAuthFilter.HELIX_ADMIN_ROLE);
    } finally {
      server.shutdown();
      _httpClient.close();
    }

    // (3) Real base-only validator exercises the interface default (which a mock would stub out):
    // base denies -> 403; base allows -> 200. The 200 documents that the default is a no-op and an
    // override is required to actually restrict the admin role.
    BaseOnlyAuthValidator baseDeny = new BaseOnlyAuthValidator(false);
    server = new HelixRestServer(namespaces, newPort, getBaseUri().getPath(),
        Collections.emptyList(), baseDeny, baseDeny, new NoopAclRegister());
    server.start();
    _httpClient = HttpClients.createDefault();
    try {
      sendRequestAndValidate(
          buildRequest("/clusters/" + cluster + "/instances/" + probeInstance,
              HttpConstants.RestVerbs.DELETE, ""),
          Response.Status.FORBIDDEN.getStatusCode());
    } finally {
      server.shutdown();
      _httpClient.close();
    }

    BaseOnlyAuthValidator baseAllow = new BaseOnlyAuthValidator(true);
    server = new HelixRestServer(namespaces, newPort, getBaseUri().getPath(),
        Collections.emptyList(), baseAllow, baseAllow, new NoopAclRegister());
    server.start();
    _httpClient = HttpClients.createDefault();
    try {
      sendRequestAndValidate(
          buildRequest("/clusters/" + cluster + "/instances/" + delegateDeleteInstance,
              HttpConstants.RestVerbs.DELETE, ""),
          Response.Status.OK.getStatusCode());
    } finally {
      server.shutdown();
      _httpClient.close();
    }
  }

  private HttpUriRequest buildRequest(String urlSuffix, HttpConstants.RestVerbs requestMethod,
      String jsonEntity) {
    String url = _mockBaseUri + urlSuffix;
    switch (requestMethod) {
      case PUT:
        HttpPut httpPut = new HttpPut(url);
        httpPut.setEntity(new StringEntity(jsonEntity, ContentType.APPLICATION_JSON));
        return httpPut;
      case DELETE:
        return new HttpDelete(url);
      case GET:
        return new HttpGet(url);
      default:
        throw new IllegalArgumentException("Unsupported requestMethod: " + requestMethod);
    }
  }

  private void sendRequestAndValidate(HttpUriRequest request, int expectedResponseCode)
      throws IllegalArgumentException, IOException {
    HttpResponse response = _httpClient.execute(request);
    Assert.assertEquals(response.getStatusLine().getStatusCode(), expectedResponseCode);
  }

  /**
   * A real {@link AuthValidator} that only implements {@link #validate(ContainerRequestContext)}
   * and does NOT override the role-aware overload, so it exercises the interface's default
   * delegation. A Mockito mock cannot stand in here because it stubs the default method out.
   */
  private static class BaseOnlyAuthValidator implements AuthValidator {
    private final boolean _allowBase;

    BaseOnlyAuthValidator(boolean allowBase) {
      _allowBase = allowBase;
    }

    @Override
    public boolean validate(ContainerRequestContext request) {
      return _allowBase;
    }
  }

  /**
   * A real {@link AuthValidator} whose base check always passes but which overrides the role-aware
   * overload to grant access only for the exact {@link HelixAdminAuthFilter#HELIX_ADMIN_ROLE}
   * string (when {@code grantRole} is true). It records the last role it was asked about so tests
   * can assert the filter forwards the expected role.
   */
  private static class RoleAwareAuthValidator implements AuthValidator {
    private final boolean _grantRole;
    private volatile String _lastRole;

    RoleAwareAuthValidator(boolean grantRole) {
      _grantRole = grantRole;
    }

    @Override
    public boolean validate(ContainerRequestContext request) {
      return true;
    }

    @Override
    public boolean validate(ContainerRequestContext request, String role) {
      _lastRole = role;
      return _grantRole && HelixAdminAuthFilter.HELIX_ADMIN_ROLE.equals(role);
    }

    String getLastRole() {
      return _lastRole;
    }
  }

}
