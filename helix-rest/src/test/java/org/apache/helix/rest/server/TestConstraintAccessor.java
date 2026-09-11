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
import java.util.Map;
import javax.ws.rs.client.Entity;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;

import com.fasterxml.jackson.databind.JsonNode;
import com.google.common.collect.ImmutableMap;
import org.apache.helix.TestHelper;
import org.apache.helix.model.ClusterConstraints;
import org.apache.helix.model.ClusterConstraints.ConstraintAttribute;
import org.apache.helix.model.ClusterConstraints.ConstraintType;
import org.apache.helix.model.ConstraintItem;
import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

public class TestConstraintAccessor extends AbstractTestClass {
  private static final String CLUSTER = "TestConstraintCluster";
  private static final String MESSAGE_CONSTRAINT = ConstraintType.MESSAGE_CONSTRAINT.name();
  private static final String CONSTRAINTS_URI = "clusters/" + CLUSTER + "/constraints";
  private static final String INSTANCE = "localhost_12918";

  @BeforeClass
  public void beforeClass() {
    _gSetupTool.addCluster(CLUSTER, true);
  }

  private static String messageConstraintUri(String constraintId) {
    return CONSTRAINTS_URI + "/" + MESSAGE_CONSTRAINT + "/" + constraintId;
  }

  private static Entity<String> constraintEntity(Map<String, String> attributes)
      throws IOException {
    return Entity.entity(OBJECT_MAPPER.writeValueAsString(attributes),
        MediaType.APPLICATION_JSON_TYPE);
  }

  @Test
  public void testCreateGetAndDeleteMessageConstraint() throws IOException {
    System.out.println("Start test :" + TestHelper.getTestMethodName());
    String constraintId = "bootstrapConstraint";
    Map<String, String> attributes = ImmutableMap.of(
        "MESSAGE_TYPE", "STATE_TRANSITION",
        "TRANSITION", "OFFLINE-BOOTSTRAP",
        "INSTANCE", INSTANCE,
        "CONSTRAINT_VALUE", "0");

    // Create the constraint through REST.
    put(messageConstraintUri(constraintId), null, constraintEntity(attributes),
        Response.Status.OK.getStatusCode());

    // Verify it landed in ZK with the expected attributes.
    ClusterConstraints constraints = _gSetupTool.getClusterManagementTool()
        .getConstraints(CLUSTER, ConstraintType.MESSAGE_CONSTRAINT);
    Assert.assertNotNull(constraints);
    ConstraintItem item = constraints.getConstraintItem(constraintId);
    Assert.assertNotNull(item);
    Assert.assertEquals(item.getConstraintValue(), "0");
    Assert.assertEquals(item.getAttributeValue(ConstraintAttribute.MESSAGE_TYPE),
        "STATE_TRANSITION");
    Assert.assertEquals(item.getAttributeValue(ConstraintAttribute.TRANSITION), "OFFLINE-BOOTSTRAP");
    Assert.assertEquals(item.getAttributeValue(ConstraintAttribute.INSTANCE), INSTANCE);

    // GET all constraints of the type.
    String body = get(CONSTRAINTS_URI + "/" + MESSAGE_CONSTRAINT, null,
        Response.Status.OK.getStatusCode(), true);
    JsonNode node = OBJECT_MAPPER.readTree(body);
    Assert.assertTrue(node.get("mapFields").has(constraintId));

    // GET the single constraint item.
    body = get(messageConstraintUri(constraintId), null, Response.Status.OK.getStatusCode(), true);
    node = OBJECT_MAPPER.readTree(body);
    Assert.assertEquals(node.get("CONSTRAINT_VALUE").asText(), "0");
    Assert.assertEquals(node.get("INSTANCE").asText(), INSTANCE);

    // DELETE it.
    delete(messageConstraintUri(constraintId), Response.Status.OK.getStatusCode());
    constraints = _gSetupTool.getClusterManagementTool()
        .getConstraints(CLUSTER, ConstraintType.MESSAGE_CONSTRAINT);
    Assert.assertTrue(
        constraints == null || constraints.getConstraintItem(constraintId) == null);

    // GET the deleted item now 404s.
    get(messageConstraintUri(constraintId), null, Response.Status.NOT_FOUND.getStatusCode(), false);
    System.out.println("End test :" + TestHelper.getTestMethodName());
  }

  @Test
  public void testCreateMultipleConstraintsSameType() throws IOException {
    System.out.println("Start test :" + TestHelper.getTestMethodName());
    String[] ids = {"bootstrapConstraint0", "bootstrapConstraint1", "bootstrapConstraint2"};
    for (int i = 0; i < ids.length; i++) {
      String id = ids[i];
      Map<String, String> attributes = ImmutableMap.of(
          "MESSAGE_TYPE", "STATE_TRANSITION",
          "TRANSITION", "OFFLINE-BOOTSTRAP",
          "INSTANCE", "localhost_1291" + i,
          "CONSTRAINT_VALUE", "0");
      put(messageConstraintUri(id), null, constraintEntity(attributes),
          Response.Status.OK.getStatusCode());
    }

    ClusterConstraints constraints = _gSetupTool.getClusterManagementTool()
        .getConstraints(CLUSTER, ConstraintType.MESSAGE_CONSTRAINT);
    Assert.assertNotNull(constraints);
    for (String id : ids) {
      Assert.assertNotNull(constraints.getConstraintItem(id),
          "Expected constraint " + id + " to exist");
      delete(messageConstraintUri(id), Response.Status.OK.getStatusCode());
    }
    System.out.println("End test :" + TestHelper.getTestMethodName());
  }

  @Test
  public void testInvalidConstraintType() throws IOException {
    System.out.println("Start test :" + TestHelper.getTestMethodName());
    Map<String, String> attributes =
        ImmutableMap.of("MESSAGE_TYPE", "STATE_TRANSITION", "CONSTRAINT_VALUE", "0");
    put(CONSTRAINTS_URI + "/NOT_A_TYPE/someId", null, constraintEntity(attributes),
        Response.Status.BAD_REQUEST.getStatusCode());
    get(CONSTRAINTS_URI + "/NOT_A_TYPE", null, Response.Status.BAD_REQUEST.getStatusCode(), false);
    System.out.println("End test :" + TestHelper.getTestMethodName());
  }

  @Test
  public void testMissingCluster() throws IOException {
    System.out.println("Start test :" + TestHelper.getTestMethodName());
    Map<String, String> attributes =
        ImmutableMap.of("MESSAGE_TYPE", "STATE_TRANSITION", "CONSTRAINT_VALUE", "0");
    put("clusters/NonExistentCluster/constraints/" + MESSAGE_CONSTRAINT + "/someId", null,
        constraintEntity(attributes), Response.Status.NOT_FOUND.getStatusCode());
    System.out.println("End test :" + TestHelper.getTestMethodName());
  }

  @Test
  public void testInvalidConstraintBody() throws IOException {
    System.out.println("Start test :" + TestHelper.getTestMethodName());
    // Missing CONSTRAINT_VALUE -> rejected.
    Map<String, String> noValue =
        ImmutableMap.of("MESSAGE_TYPE", "STATE_TRANSITION", "TRANSITION", "OFFLINE-BOOTSTRAP");
    put(messageConstraintUri("noValue"), null, constraintEntity(noValue),
        Response.Status.BAD_REQUEST.getStatusCode());

    // Only unrecognized attribute keys -> nothing valid parsed -> rejected.
    Map<String, String> onlyBogus = ImmutableMap.of("BOGUS_ATTR", "x", "CONSTRAINT_VALUE", "0");
    put(messageConstraintUri("onlyBogus"), null, constraintEntity(onlyBogus),
        Response.Status.BAD_REQUEST.getStatusCode());

    // Empty body -> rejected.
    put(messageConstraintUri("empty"), null,
        Entity.entity("{}", MediaType.APPLICATION_JSON_TYPE),
        Response.Status.BAD_REQUEST.getStatusCode());
    System.out.println("End test :" + TestHelper.getTestMethodName());
  }
}
