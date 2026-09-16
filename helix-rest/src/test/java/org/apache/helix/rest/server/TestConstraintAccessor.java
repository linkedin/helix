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

  private static Entity<String> batchEntity(Map<String, Map<String, String>> constraints)
      throws IOException {
    return Entity.entity(OBJECT_MAPPER.writeValueAsString(constraints),
        MediaType.APPLICATION_JSON_TYPE);
  }

  private static ConstraintItem getConstraintItem(String constraintId) {
    ClusterConstraints constraints = _gSetupTool.getClusterManagementTool()
        .getConstraints(CLUSTER, ConstraintType.MESSAGE_CONSTRAINT);
    return constraints == null ? null : constraints.getConstraintItem(constraintId);
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

  @Test
  public void testNullAttributeValueIsBadRequest() throws IOException {
    System.out.println("Start test :" + TestHelper.getTestMethodName());
    // A null CONSTRAINT_VALUE used to reach ConstraintValue.valueOf(null) inside the builder. The
    // resulting NullPointerException is not an IllegalArgumentException, so the builder's own
    // catch missed it and Jersey turned it into a 500 for what is plainly bad input.
    put(messageConstraintUri("nullValue"), null,
        Entity.entity("{\"MESSAGE_TYPE\":\"STATE_TRANSITION\",\"CONSTRAINT_VALUE\":null}",
            MediaType.APPLICATION_JSON_TYPE),
        Response.Status.BAD_REQUEST.getStatusCode());
    Assert.assertNull(getConstraintItem("nullValue"));

    // Same for any other attribute.
    put(messageConstraintUri("nullInstance"), null,
        Entity.entity("{\"INSTANCE\":null,\"CONSTRAINT_VALUE\":\"1\"}",
            MediaType.APPLICATION_JSON_TYPE),
        Response.Status.BAD_REQUEST.getStatusCode());
    Assert.assertNull(getConstraintItem("nullInstance"));
    System.out.println("End test :" + TestHelper.getTestMethodName());
  }

  @Test
  public void testInvalidAttributeValuesAreRejected() throws IOException {
    System.out.println("Start test :" + TestHelper.getTestMethodName());
    // A negative throttle makes no sense and would silently block every message.
    put(messageConstraintUri("negative"), null,
        constraintEntity(ImmutableMap.of("MESSAGE_TYPE", "STATE_TRANSITION",
            "CONSTRAINT_VALUE", "-1")),
        Response.Status.BAD_REQUEST.getStatusCode());
    Assert.assertNull(getConstraintItem("negative"));

    // Not a number and not ANY.
    put(messageConstraintUri("notANumber"), null,
        constraintEntity(ImmutableMap.of("MESSAGE_TYPE", "STATE_TRANSITION",
            "CONSTRAINT_VALUE", "lots")),
        Response.Status.BAD_REQUEST.getStatusCode());
    Assert.assertNull(getConstraintItem("notANumber"));

    // A message type that matches nothing can never throttle anything, so it is a typo.
    put(messageConstraintUri("badMessageType"), null,
        constraintEntity(ImmutableMap.of("MESSAGE_TYPE", "STATE_TRANSTION",
            "CONSTRAINT_VALUE", "1")),
        Response.Status.BAD_REQUEST.getStatusCode());
    Assert.assertNull(getConstraintItem("badMessageType"));

    // Attribute values are matched as regexes, so an uncompilable one would throw inside the
    // controller pipeline rather than at write time.
    put(messageConstraintUri("badRegex"), null,
        constraintEntity(ImmutableMap.of("MESSAGE_TYPE", "STATE_TRANSITION", "INSTANCE", "local[",
            "CONSTRAINT_VALUE", "1")),
        Response.Status.BAD_REQUEST.getStatusCode());
    Assert.assertNull(getConstraintItem("badRegex"));

    // A CONSTRAINT_VALUE on its own constrains nothing.
    put(messageConstraintUri("valueOnly"), null,
        constraintEntity(ImmutableMap.of("CONSTRAINT_VALUE", "1")),
        Response.Status.BAD_REQUEST.getStatusCode());
    Assert.assertNull(getConstraintItem("valueOnly"));
    System.out.println("End test :" + TestHelper.getTestMethodName());
  }

  @Test
  public void testUnknownAttributeIsRejectedRatherThanDropped() throws IOException {
    System.out.println("Start test :" + TestHelper.getTestMethodName());
    // The builder logs and then drops keys it does not recognize. Accepting this body would
    // return 200 while persisting a constraint that ignores the WRONG_KEY the caller asked for.
    put(messageConstraintUri("partlyBogus"), null,
        constraintEntity(ImmutableMap.of("MESSAGE_TYPE", "STATE_TRANSITION", "WRONG_KEY", "x",
            "CONSTRAINT_VALUE", "1")),
        Response.Status.BAD_REQUEST.getStatusCode());
    Assert.assertNull(getConstraintItem("partlyBogus"));
    System.out.println("End test :" + TestHelper.getTestMethodName());
  }

  @Test
  public void testRegexAttributeValuesAreAccepted() throws IOException {
    System.out.println("Start test :" + TestHelper.getTestMethodName());
    // Wildcards are the documented way to write a cluster wide throttle, so validation must not
    // reject them.
    Map<String, String> attributes = ImmutableMap.of(
        "MESSAGE_TYPE", "STATE_TRANSITION",
        "TRANSITION", "OFFLINE-BOOTSTRAP",
        "INSTANCE", ".*",
        "RESOURCE", ".*",
        "CONSTRAINT_VALUE", "ANY");
    put(messageConstraintUri("wildcard"), null, constraintEntity(attributes),
        Response.Status.OK.getStatusCode());
    ConstraintItem item = getConstraintItem("wildcard");
    Assert.assertNotNull(item);
    Assert.assertEquals(item.getConstraintValue(), "ANY");
    Assert.assertEquals(item.getAttributeValue(ConstraintAttribute.INSTANCE), ".*");

    delete(messageConstraintUri("wildcard"), Response.Status.OK.getStatusCode());
    System.out.println("End test :" + TestHelper.getTestMethodName());
  }

  @Test
  public void testBatchSetConstraints() throws IOException {
    System.out.println("Start test :" + TestHelper.getTestMethodName());
    Map<String, Map<String, String>> batch = ImmutableMap.of(
        "batchPerInstance", ImmutableMap.of(
            "MESSAGE_TYPE", "STATE_TRANSITION",
            "TRANSITION", "OFFLINE-BOOTSTRAP",
            "INSTANCE", ".*",
            "CONSTRAINT_VALUE", "1"),
        "batchPerResource", ImmutableMap.of(
            "MESSAGE_TYPE", "STATE_TRANSITION",
            "TRANSITION", "OFFLINE-BOOTSTRAP",
            "RESOURCE", "myDB",
            "CONSTRAINT_VALUE", "5"));

    put(CONSTRAINTS_URI + "/" + MESSAGE_CONSTRAINT, null, batchEntity(batch),
        Response.Status.OK.getStatusCode());

    ConstraintItem perInstance = getConstraintItem("batchPerInstance");
    Assert.assertNotNull(perInstance);
    Assert.assertEquals(perInstance.getConstraintValue(), "1");
    Assert.assertEquals(perInstance.getAttributeValue(ConstraintAttribute.INSTANCE), ".*");

    ConstraintItem perResource = getConstraintItem("batchPerResource");
    Assert.assertNotNull(perResource);
    Assert.assertEquals(perResource.getConstraintValue(), "5");
    Assert.assertEquals(perResource.getAttributeValue(ConstraintAttribute.RESOURCE), "myDB");

    for (String id : batch.keySet()) {
      delete(messageConstraintUri(id), Response.Status.OK.getStatusCode());
    }
    System.out.println("End test :" + TestHelper.getTestMethodName());
  }

  @Test
  public void testBatchIsAllOrNothing() throws IOException {
    System.out.println("Start test :" + TestHelper.getTestMethodName());
    Map<String, Map<String, String>> batch = ImmutableMap.of(
        "batchGood", ImmutableMap.of(
            "MESSAGE_TYPE", "STATE_TRANSITION",
            "INSTANCE", ".*",
            "CONSTRAINT_VALUE", "1"),
        "batchBad", ImmutableMap.of(
            "MESSAGE_TYPE", "STATE_TRANSITION",
            "INSTANCE", ".*",
            "CONSTRAINT_VALUE", "-3"));

    put(CONSTRAINTS_URI + "/" + MESSAGE_CONSTRAINT, null, batchEntity(batch),
        Response.Status.BAD_REQUEST.getStatusCode());

    // The valid half of the batch must not have been written either.
    Assert.assertNull(getConstraintItem("batchGood"));
    Assert.assertNull(getConstraintItem("batchBad"));

    // An empty batch is rejected rather than treated as a no-op success.
    put(CONSTRAINTS_URI + "/" + MESSAGE_CONSTRAINT, null,
        Entity.entity("{}", MediaType.APPLICATION_JSON_TYPE),
        Response.Status.BAD_REQUEST.getStatusCode());
    System.out.println("End test :" + TestHelper.getTestMethodName());
  }

  @Test
  public void testBatchOnInvalidClusterAndType() throws IOException {
    System.out.println("Start test :" + TestHelper.getTestMethodName());
    Map<String, Map<String, String>> batch = ImmutableMap.of("someId",
        ImmutableMap.of("MESSAGE_TYPE", "STATE_TRANSITION", "CONSTRAINT_VALUE", "1"));

    put("clusters/NonExistentCluster/constraints/" + MESSAGE_CONSTRAINT, null, batchEntity(batch),
        Response.Status.NOT_FOUND.getStatusCode());
    put(CONSTRAINTS_URI + "/NOT_A_TYPE", null, batchEntity(batch),
        Response.Status.BAD_REQUEST.getStatusCode());
    System.out.println("End test :" + TestHelper.getTestMethodName());
  }
}
