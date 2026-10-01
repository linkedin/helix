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
import java.util.HashMap;
import java.util.Map;
import javax.ws.rs.core.Response;

import com.fasterxml.jackson.databind.JsonNode;
import com.google.common.collect.ImmutableMap;
import org.apache.helix.AccessOption;
import org.apache.helix.HelixProperty;
import org.apache.helix.TestHelper;
import org.apache.helix.controller.rebalancer.waged.AssignmentMetadataStore;
import org.apache.helix.manager.zk.ZkBucketDataAccessor;
import org.apache.helix.model.Partition;
import org.apache.helix.model.ResourceAssignment;
import org.apache.helix.zookeeper.datamodel.serializer.ZNRecordJacksonSerializer;
import org.apache.helix.zookeeper.zkclient.serialize.ZkSerializer;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;


/**
 * Verifies that the WAGED assignment REST accessor decodes what the controller persisted through
 * the bucketized, GZIP compressed assignment metadata store.
 */
public class TestWagedAssignmentAccessor extends AbstractTestClass {
  private static final String TEST_CLUSTER = "TestCluster_0";
  private static final String UNKNOWN_CLUSTER = "NonExistentWagedAssignmentCluster";
  private static final String RESOURCE_0 = "wagedDb0";
  private static final String RESOURCE_1 = "wagedDb1";
  private static final String INSTANCE_0 = "wagedInstance0";
  private static final String INSTANCE_1 = "wagedInstance1";
  private static final String MASTER = "MASTER";
  private static final String SLAVE = "SLAVE";
  private static final ZkSerializer SERIALIZER = new ZNRecordJacksonSerializer();

  private ZkBucketDataAccessor _bucketDataAccessor;

  @BeforeClass
  public void beforeClass() throws IOException {
    _bucketDataAccessor = new ZkBucketDataAccessor(ZK_ADDR);
    _bucketDataAccessor.compressedBucketWrite(
        AssignmentMetadataStore.getBestPossiblePath(TEST_CLUSTER),
        combineAssignments("BEST_POSSIBLE", buildBestPossibleAssignment()));
    _bucketDataAccessor.compressedBucketWrite(AssignmentMetadataStore.getBaselinePath(TEST_CLUSTER),
        combineAssignments("BASELINE", buildBaselineAssignment()));
  }

  @AfterClass
  public void afterClass() {
    if (_bucketDataAccessor != null) {
      _bucketDataAccessor.close();
    }
    _baseAccessor.remove("/" + TEST_CLUSTER + "/ASSIGNMENT_METADATA", AccessOption.PERSISTENT);
  }

  @Test
  public void testGetBestPossibleAssignmentIsDecoded() throws IOException {
    System.out.println("Start test :" + TestHelper.getTestMethodName());
    JsonNode node = getAssignmentNode("bestPossible", null);

    Assert.assertEquals(node.get("cluster").asText(), TEST_CLUSTER);
    Assert.assertEquals(node.get("assignmentType").asText(), "BEST_POSSIBLE");
    Assert.assertEquals(node.get("format").asText(), "IdealStateFormat");

    JsonNode assignment = node.get("assignment");
    Assert.assertEquals(assignment.size(), 2);
    Assert.assertEquals(assignment.get(RESOURCE_0).get(RESOURCE_0 + "_0").get(INSTANCE_0).asText(),
        MASTER);
    Assert.assertEquals(assignment.get(RESOURCE_0).get(RESOURCE_0 + "_0").get(INSTANCE_1).asText(),
        SLAVE);
    Assert.assertEquals(assignment.get(RESOURCE_0).get(RESOURCE_0 + "_1").get(INSTANCE_1).asText(),
        MASTER);
    Assert.assertEquals(assignment.get(RESOURCE_1).get(RESOURCE_1 + "_0").get(INSTANCE_0).asText(),
        MASTER);
    System.out.println("End test :" + TestHelper.getTestMethodName());
  }

  @Test
  public void testGetBaselineAssignmentIsDecoded() throws IOException {
    System.out.println("Start test :" + TestHelper.getTestMethodName());
    JsonNode node = getAssignmentNode("baseline", null);

    Assert.assertEquals(node.get("assignmentType").asText(), "BASELINE");
    JsonNode assignment = node.get("assignment");
    Assert.assertEquals(assignment.size(), 1);
    Assert.assertEquals(assignment.get(RESOURCE_0).get(RESOURCE_0 + "_0").get(INSTANCE_1).asText(),
        MASTER);
    System.out.println("End test :" + TestHelper.getTestMethodName());
  }

  @Test
  public void testCurrentStateFormat() throws IOException {
    System.out.println("Start test :" + TestHelper.getTestMethodName());
    JsonNode node =
        getAssignmentNode("bestPossible", ImmutableMap.of("format", "CurrentStateFormat"));

    Assert.assertEquals(node.get("format").asText(), "CurrentStateFormat");
    JsonNode assignment = node.get("assignment");
    // Inverted: instance -> resource -> partition -> state
    Assert.assertEquals(assignment.size(), 2);
    Assert.assertEquals(
        assignment.get(INSTANCE_0).get(RESOURCE_0).get(RESOURCE_0 + "_0").asText(), MASTER);
    Assert.assertEquals(
        assignment.get(INSTANCE_0).get(RESOURCE_1).get(RESOURCE_1 + "_0").asText(), MASTER);
    Assert.assertEquals(
        assignment.get(INSTANCE_1).get(RESOURCE_0).get(RESOURCE_0 + "_1").asText(), MASTER);
    Assert.assertFalse(assignment.get(INSTANCE_1).has(RESOURCE_1));
    System.out.println("End test :" + TestHelper.getTestMethodName());
  }

  @Test
  public void testResourceFilter() throws IOException {
    System.out.println("Start test :" + TestHelper.getTestMethodName());
    JsonNode assignment =
        getAssignmentNode("bestPossible", ImmutableMap.of("resources", RESOURCE_1))
            .get("assignment");

    Assert.assertEquals(assignment.size(), 1);
    Assert.assertTrue(assignment.has(RESOURCE_1));
    System.out.println("End test :" + TestHelper.getTestMethodName());
  }

  @Test
  public void testInstanceFilterDropsEmptyResources() throws IOException {
    System.out.println("Start test :" + TestHelper.getTestMethodName());
    JsonNode assignment =
        getAssignmentNode("bestPossible", ImmutableMap.of("instances", INSTANCE_1))
            .get("assignment");

    // wagedDb1 only lives on instance0, so it drops out entirely.
    Assert.assertEquals(assignment.size(), 1);
    Assert.assertEquals(assignment.get(RESOURCE_0).get(RESOURCE_0 + "_0").size(), 1);
    Assert.assertEquals(assignment.get(RESOURCE_0).get(RESOURCE_0 + "_0").get(INSTANCE_1).asText(),
        SLAVE);
    System.out.println("End test :" + TestHelper.getTestMethodName());
  }

  @Test
  public void testPartitionFilter() throws IOException {
    System.out.println("Start test :" + TestHelper.getTestMethodName());
    JsonNode assignment =
        getAssignmentNode("bestPossible", ImmutableMap.of("partitions", RESOURCE_0 + "_1"))
            .get("assignment");

    Assert.assertEquals(assignment.size(), 1);
    Assert.assertEquals(assignment.get(RESOURCE_0).size(), 1);
    Assert.assertTrue(assignment.get(RESOURCE_0).has(RESOURCE_0 + "_1"));
    System.out.println("End test :" + TestHelper.getTestMethodName());
  }

  @Test
  public void testWriteMetadata() throws IOException {
    System.out.println("Start test :" + TestHelper.getTestMethodName());
    JsonNode metadata = getAssignmentNode("bestPossible", null).get("metadata");

    Assert.assertNotNull(metadata);
    Assert.assertNotNull(metadata.get("lastSuccessfulWriteVersion"));
    Assert.assertTrue(metadata.get("lastSuccessfulWriteTimeMs").asLong() > 0);
    Assert.assertTrue(metadata.get("bucketMetadata").get("DATA_SIZE").asInt() > 0);

    // Metadata can be turned off for callers that only want the placement.
    JsonNode noMetadata =
        getAssignmentNode("bestPossible", ImmutableMap.of("includeMetadata", "false"));
    Assert.assertFalse(noMetadata.has("metadata"));
    System.out.println("End test :" + TestHelper.getTestMethodName());
  }

  @Test
  public void testInvalidFormatIsRejected() {
    System.out.println("Start test :" + TestHelper.getTestMethodName());
    get("clusters/" + TEST_CLUSTER + "/wagedAssignment/bestPossible",
        ImmutableMap.of("format", "NotAFormat"), Response.Status.BAD_REQUEST.getStatusCode(), true);
    System.out.println("End test :" + TestHelper.getTestMethodName());
  }

  @Test
  public void testMissingAssignmentReturnsNotFound() {
    System.out.println("Start test :" + TestHelper.getTestMethodName());
    get("clusters/" + UNKNOWN_CLUSTER + "/wagedAssignment/bestPossible", null,
        Response.Status.NOT_FOUND.getStatusCode(), true);
    System.out.println("End test :" + TestHelper.getTestMethodName());
  }

  private JsonNode getAssignmentNode(String assignmentType, Map<String, String> queryParams)
      throws IOException {
    String body = get("clusters/" + TEST_CLUSTER + "/wagedAssignment/" + assignmentType,
        queryParams, Response.Status.OK.getStatusCode(), true);
    return OBJECT_MAPPER.readTree(body);
  }

  private static Map<String, ResourceAssignment> buildBestPossibleAssignment() {
    Map<String, ResourceAssignment> assignments = new HashMap<>();
    ResourceAssignment resource0 = new ResourceAssignment(RESOURCE_0);
    resource0.addReplicaMap(new Partition(RESOURCE_0 + "_0"),
        ImmutableMap.of(INSTANCE_0, MASTER, INSTANCE_1, SLAVE));
    resource0.addReplicaMap(new Partition(RESOURCE_0 + "_1"), ImmutableMap.of(INSTANCE_1, MASTER));
    assignments.put(RESOURCE_0, resource0);

    ResourceAssignment resource1 = new ResourceAssignment(RESOURCE_1);
    resource1.addReplicaMap(new Partition(RESOURCE_1 + "_0"), ImmutableMap.of(INSTANCE_0, MASTER));
    assignments.put(RESOURCE_1, resource1);
    return assignments;
  }

  private static Map<String, ResourceAssignment> buildBaselineAssignment() {
    Map<String, ResourceAssignment> assignments = new HashMap<>();
    ResourceAssignment resource0 = new ResourceAssignment(RESOURCE_0);
    resource0.addReplicaMap(new Partition(RESOURCE_0 + "_0"),
        ImmutableMap.of(INSTANCE_1, MASTER, INSTANCE_0, SLAVE));
    assignments.put(RESOURCE_0, resource0);
    return assignments;
  }

  /**
   * Mirrors {@code AssignmentMetadataStore#combineAssignments} so the test exercises the exact wire
   * format the controller writes.
   */
  private static HelixProperty combineAssignments(String name,
      Map<String, ResourceAssignment> assignmentMap) {
    HelixProperty property = new HelixProperty(name);
    assignmentMap.forEach((resource, assignment) -> property.getRecord()
        .setSimpleField(resource, new String(SERIALIZER.serialize(assignment.getRecord()))));
    return property;
  }
}
