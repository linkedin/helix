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
import java.util.Arrays;
import java.util.List;
import javax.ws.rs.client.Entity;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;

import com.fasterxml.jackson.databind.JsonNode;
import com.google.common.collect.ImmutableMap;
import org.apache.helix.AccessOption;
import org.apache.helix.HelixAdmin;
import org.apache.helix.PropertyPathBuilder;
import org.apache.helix.model.IdealState;
import org.apache.helix.rest.server.service.ResourceReplicaCountService;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

/**
 * End to end coverage of the scoped bulk replica count endpoint against a live REST server, using a
 * cluster owned by this class so that shared fixtures are not mutated.
 */
public class TestResourceReplicaCounts extends AbstractTestClass {
  private static final String CLUSTER_NAME = "TestReplicaCountCluster";
  private static final String RESOURCE_PREFIX = CLUSTER_NAME + "_db_";
  private static final int NUM_RESOURCES = 3;
  private static final String MISSING_RESOURCE = CLUSTER_NAME + "_db_absent";
  private static final String COMMAND = "updateReplicaCounts";
  private static final String URI = "clusters/" + CLUSTER_NAME + "/resources";

  private HelixAdmin _admin;

  @BeforeClass
  public void beforeClass() {
    _admin = _gSetupTool.getClusterManagementTool();
    _gSetupTool.addCluster(CLUSTER_NAME, true);
    for (int i = 0; i < NUM_RESOURCES; i++) {
      String resource = RESOURCE_PREFIX + i;
      _gSetupTool.addResourceToCluster(CLUSTER_NAME, resource, 4, "MasterSlave");
      IdealState idealState = _admin.getResourceIdealState(CLUSTER_NAME, resource);
      idealState.setReplicas("1");
      idealState.setMinActiveReplicas(1);
      _admin.setResourceIdealState(CLUSTER_NAME, resource, idealState);
    }
  }

  @AfterClass
  public void afterClass() {
    _gSetupTool.deleteCluster(CLUSTER_NAME);
  }

  @Test
  public void testBulkUpdateOfEveryResource() throws IOException {
    JsonNode response = updateReplicaCounts(
        "{\"selection\":\"ALL_RESOURCES\",\"replicas\":3,\"minActiveReplicas\":2}",
        Response.Status.OK.getStatusCode());

    Assert.assertEquals(response.get("id").textValue(), CLUSTER_NAME);
    Assert.assertEquals(response.get("selection").textValue(), "ALL_RESOURCES");
    Assert.assertEquals(response.get("selectedResourceCount").intValue(), NUM_RESOURCES);
    Assert.assertTrue(response.get("allAtDesiredValues").booleanValue());
    assertStatusCount(response, ResourceReplicaCountService.ResourceUpdateStatus.APPLIED,
        NUM_RESOURCES);
    // All statuses are reported, so a consumer cannot read an absent key as a zero count.
    Assert.assertEquals(response.get("statusCounts").size(),
        ResourceReplicaCountService.ResourceUpdateStatus.values().length);

    for (int i = 0; i < NUM_RESOURCES; i++) {
      String resource = RESOURCE_PREFIX + i;
      Assert.assertEquals(
          response.get("resourceResults").get(resource).get("status").textValue(), "APPLIED");
      IdealState idealState = _admin.getResourceIdealState(CLUSTER_NAME, resource);
      Assert.assertEquals(idealState.getReplicas(), "3");
      Assert.assertEquals(idealState.getMinActiveReplicas(), 2);
      // Fields the request did not name are left exactly as they were.
      Assert.assertEquals(idealState.getNumPartitions(), 4);
      Assert.assertEquals(idealState.getStateModelDefRef(), "MasterSlave");
    }
  }

  @Test(dependsOnMethods = "testBulkUpdateOfEveryResource")
  public void testResendingTheSameValuesWritesNothing() throws IOException {
    int versionBefore = idealStateVersion(RESOURCE_PREFIX + "0");

    JsonNode response = updateReplicaCounts(
        "{\"selection\":\"ALL_RESOURCES\",\"replicas\":3,\"minActiveReplicas\":2}",
        Response.Status.OK.getStatusCode());

    Assert.assertTrue(response.get("allAtDesiredValues").booleanValue());
    assertStatusCount(response, ResourceReplicaCountService.ResourceUpdateStatus.UNCHANGED,
        NUM_RESOURCES);
    assertStatusCount(response, ResourceReplicaCountService.ResourceUpdateStatus.APPLIED, 0);
    Assert.assertEquals(idealStateVersion(RESOURCE_PREFIX + "0"), versionBefore,
        "A resource that already carries the desired values must not be written again.");
  }

  @Test(dependsOnMethods = "testResendingTheSameValuesWritesNothing")
  public void testMissingResourceIsReportedAndNotCreated() throws IOException {
    String body = "{\"selection\":\"EXPLICIT\",\"resources\":[\"" + RESOURCE_PREFIX + "0\",\""
        + MISSING_RESOURCE + "\"],\"replicas\":4,\"minActiveReplicas\":2}";

    JsonNode response = updateReplicaCounts(body, Response.Status.OK.getStatusCode());

    Assert.assertFalse(response.get("allAtDesiredValues").booleanValue(),
        "A selection containing a missing resource is not fully applied.");
    Assert.assertEquals(response.get("selectedResourceCount").intValue(), 2);
    Assert.assertEquals(
        response.get("resourceResults").get(RESOURCE_PREFIX + "0").get("status").textValue(),
        "APPLIED");
    JsonNode missing = response.get("resourceResults").get(MISSING_RESOURCE);
    Assert.assertEquals(missing.get("status").textValue(), "NOT_FOUND");
    Assert.assertEquals(missing.get("version").intValue(), -1);
    Assert.assertNotNull(missing.get("message"));
    Assert.assertNull(_admin.getResourceIdealState(CLUSTER_NAME, MISSING_RESOURCE),
        "The endpoint must not create an IdealState for a resource that has none.");
    // The named resource was still updated even though another selected resource was absent.
    Assert.assertEquals(_admin.getResourceIdealState(CLUSTER_NAME, RESOURCE_PREFIX + "0")
        .getReplicas(), "4");
  }

  @Test(dependsOnMethods = "testMissingResourceIsReportedAndNotCreated")
  public void testOnlyTheNamedFieldIsChanged() throws IOException {
    String resource = RESOURCE_PREFIX + "1";
    int minActiveBefore =
        _admin.getResourceIdealState(CLUSTER_NAME, resource).getMinActiveReplicas();

    updateReplicaCounts(
        "{\"selection\":\"EXPLICIT\",\"resources\":[\"" + resource + "\"],\"replicas\":5}",
        Response.Status.OK.getStatusCode());

    IdealState idealState = _admin.getResourceIdealState(CLUSTER_NAME, resource);
    Assert.assertEquals(idealState.getReplicas(), "5");
    Assert.assertEquals(idealState.getMinActiveReplicas(), minActiveBefore);
  }

  @Test
  public void testInvalidRequestsAreRejected() throws IOException {
    List<String> invalidBodies = Arrays.asList(
        // No desired value at all.
        "{\"selection\":\"ALL_RESOURCES\"}",
        // A replica count the rebalancer cannot honour.
        "{\"selection\":\"ALL_RESOURCES\",\"replicas\":0}",
        "{\"selection\":\"ALL_RESOURCES\",\"minActiveReplicas\":-1}",
        // A minimum that cannot be met by the replica count set in the same request.
        "{\"selection\":\"ALL_RESOURCES\",\"replicas\":2,\"minActiveReplicas\":3}",
        // A misspelled field must not be silently ignored.
        "{\"selection\":\"ALL_RESOURCES\",\"replicas\":3,\"minActiveReplica\":2}",
        // An unknown or missing scope.
        "{\"selection\":\"EVERYTHING\",\"replicas\":3}",
        "{\"replicas\":3}",
        // An explicit scope must actually name resources.
        "{\"selection\":\"EXPLICIT\",\"replicas\":3}",
        "{\"selection\":\"EXPLICIT\",\"resources\":[],\"replicas\":3}",
        "{\"selection\":\"EXPLICIT\",\"resources\":[17],\"replicas\":3}",
        // A name that would address a different znode than the one named.
        "{\"selection\":\"EXPLICIT\",\"resources\":[\"../CONFIGS/x\"],\"replicas\":3}",
        // An unscoped selection must not carry a resource list.
        "{\"selection\":\"ALL_RESOURCES\",\"resources\":[\"" + RESOURCE_PREFIX
            + "0\"],\"replicas\":3}",
        // Wrong shapes.
        "{\"selection\":\"ALL_RESOURCES\",\"replicas\":\"3\"}",
        "[\"not\",\"an\",\"object\"]",
        "not json at all");

    for (String body : invalidBodies) {
      post(URI, ImmutableMap.of("command", COMMAND),
          Entity.entity(body, MediaType.APPLICATION_JSON_TYPE),
          Response.Status.BAD_REQUEST.getStatusCode());
    }
    // Nothing was written by any of the rejected requests.
    Assert.assertEquals(_admin.getResourceIdealState(CLUSTER_NAME, RESOURCE_PREFIX + "2")
        .getReplicas(), "3");
  }

  @Test
  public void testUnsupportedCommandIsRejected() {
    String body = "{\"selection\":\"ALL_RESOURCES\",\"replicas\":3}";
    post(URI, ImmutableMap.of("command", "enable"),
        Entity.entity(body, MediaType.APPLICATION_JSON_TYPE),
        Response.Status.BAD_REQUEST.getStatusCode());
    post(URI, ImmutableMap.of("command", "notACommand"),
        Entity.entity(body, MediaType.APPLICATION_JSON_TYPE),
        Response.Status.BAD_REQUEST.getStatusCode());
    post(URI, null, Entity.entity(body, MediaType.APPLICATION_JSON_TYPE),
        Response.Status.BAD_REQUEST.getStatusCode());
  }

  @Test
  public void testUnknownClusterIsNotFound() {
    post("clusters/NoSuchReplicaCountCluster/resources", ImmutableMap.of("command", COMMAND),
        Entity.entity("{\"selection\":\"ALL_RESOURCES\",\"replicas\":3}",
            MediaType.APPLICATION_JSON_TYPE),
        Response.Status.NOT_FOUND.getStatusCode());
  }

  private JsonNode updateReplicaCounts(String body, int expectedStatus) throws IOException {
    Response response = post(URI, ImmutableMap.of("command", COMMAND),
        Entity.entity(body, MediaType.APPLICATION_JSON_TYPE), expectedStatus, true);
    return OBJECT_MAPPER.readTree(response.readEntity(String.class));
  }

  private void assertStatusCount(JsonNode response,
      ResourceReplicaCountService.ResourceUpdateStatus status, int expected) {
    Assert.assertEquals(response.get("statusCounts").get(status.name()).intValue(), expected,
        "Unexpected count of " + status + " in " + response.get("statusCounts"));
  }

  private int idealStateVersion(String resource) {
    return _baseAccessor
        .getStat(PropertyPathBuilder.idealState(CLUSTER_NAME, resource), AccessOption.PERSISTENT)
        .getVersion();
  }
}
