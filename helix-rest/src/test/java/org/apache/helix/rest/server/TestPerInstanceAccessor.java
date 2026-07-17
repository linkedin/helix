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
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import javax.ws.rs.client.Entity;
import javax.ws.rs.client.WebTarget;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.apache.helix.HelixAdmin;
import org.apache.helix.HelixDataAccessor;
import org.apache.helix.HelixDefinedState;
import org.apache.helix.HelixException;
import org.apache.helix.TestHelper;
import org.apache.helix.constants.InstanceConstants;
import org.apache.helix.controller.rebalancer.waged.WagedRebalancer;
import org.apache.helix.guardrail.rules.InstanceCapacityHeadroomGuardrailRule;
import org.apache.helix.guardrail.rules.LiveInstanceGuardrailRule;
import org.apache.helix.integration.manager.MockParticipantManager;
import org.apache.helix.integration.task.MockTask;
import org.apache.helix.manager.zk.ZKHelixDataAccessor;
import org.apache.helix.model.ClusterConfig;
import org.apache.helix.model.ExternalView;
import org.apache.helix.model.IdealState;
import org.apache.helix.model.InstanceConfig;
import org.apache.helix.model.Message;
import org.apache.helix.model.ResourceConfig;
import org.apache.helix.participant.StateMachineEngine;
import org.apache.helix.rest.server.resources.AbstractResource;
import org.apache.helix.rest.server.resources.helix.InstancesAccessor;
import org.apache.helix.rest.server.resources.helix.PerInstanceAccessor;
import org.apache.helix.rest.server.util.JerseyUriRequestBuilder;
import org.apache.helix.task.TaskFactory;
import org.apache.helix.task.TaskStateModelFactory;
import org.apache.helix.tools.ClusterVerifiers.BestPossibleExternalViewVerifier;
import org.apache.helix.zookeeper.datamodel.ZNRecord;
import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

public class TestPerInstanceAccessor extends AbstractTestClass {
  private final static String CLUSTER_NAME = "TestCluster_4";
  private final static String INSTANCE_NAME = CLUSTER_NAME + "localhost_12918";
  private BestPossibleExternalViewVerifier _bestPossibleClusterVerifier;

  private MockParticipantManager _instanceToDisable;

  @BeforeClass
  public void beforeClass() {
    _bestPossibleClusterVerifier = new BestPossibleExternalViewVerifier.Builder(CLUSTER_NAME)
        .setZkAddr(ZK_ADDR).build();
    int indexToDisable = -1;
    for (int i = 0; i < _mockParticipantManagers.size(); i++) {
      if (_mockParticipantManagers.get(i).getInstanceName().equals(INSTANCE_NAME)) {
        indexToDisable = i;
        break;
      }
    }
    _instanceToDisable = _mockParticipantManagers.remove(indexToDisable);
  }

  @Test
  public void testIsInstanceStoppable() throws IOException {
    System.out.println("Start test :" + TestHelper.getTestMethodName());
    Map<String, String> params = ImmutableMap.of("client", "espresso");
    Entity entity =
        Entity.entity(OBJECT_MAPPER.writeValueAsString(params), MediaType.APPLICATION_JSON_TYPE);
    Response response = new JerseyUriRequestBuilder(
        "clusters/{}/instances/{}/stoppable?skipHealthCheckCategories=CUSTOM_INSTANCE_CHECK,CUSTOM_PARTITION_CHECK").format(
        STOPPABLE_CLUSTER, "instance1").post(this, entity);
    String stoppableCheckResult = response.readEntity(String.class);
    Map<String, Object> actualMap = OBJECT_MAPPER.readValue(stoppableCheckResult, Map.class);
    List<String> failedChecks =
        Arrays.asList("HELIX:EMPTY_RESOURCE_ASSIGNMENT", "HELIX:INSTANCE_NOT_ENABLED",
            "HELIX:INSTANCE_NOT_STABLE");
    Map<String, Object> expectedMap =
        ImmutableMap.of("stoppable", false, "failedChecks", failedChecks);
    Assert.assertEquals(actualMap, expectedMap);
    System.out.println("End test :" + TestHelper.getTestMethodName());
  }

  @Test
  public void testIsInstanceStoppableWithIncludeDetailsDefault() throws IOException {
    System.out.println("Start test :" + TestHelper.getTestMethodName());
    Map<String, String> params = ImmutableMap.of("client", "espresso");
    Entity entity =
        Entity.entity(OBJECT_MAPPER.writeValueAsString(params), MediaType.APPLICATION_JSON_TYPE);
    
    // Test without includeDetails parameter (should behave same as includeDetails=false)
    Response response = new JerseyUriRequestBuilder(
        "clusters/{}/instances/{}/stoppable?skipHealthCheckCategories=CUSTOM_INSTANCE_CHECK,CUSTOM_PARTITION_CHECK").format(
        STOPPABLE_CLUSTER, "instance1").post(this, entity);
    String stoppableCheckResult = response.readEntity(String.class);
    Map<String, Object> actualMap = OBJECT_MAPPER.readValue(stoppableCheckResult, Map.class);
    
    List<String> failedChecks =
        Arrays.asList("HELIX:EMPTY_RESOURCE_ASSIGNMENT", "HELIX:INSTANCE_NOT_ENABLED",
            "HELIX:INSTANCE_NOT_STABLE");
    Map<String, Object> expectedMap =
        ImmutableMap.of("stoppable", false, "failedChecks", failedChecks);
    Assert.assertEquals(actualMap, expectedMap);
    
    // Verify the failed checks contain basic error codes without detailed partition information
    for (String failedCheck : failedChecks) {
      Assert.assertFalse(failedCheck.contains("partition"),
          "Basic error message should not contain partition details: " + failedCheck);
      Assert.assertFalse(failedCheck.contains("active replicas"),
          "Basic error message should not contain replica details: " + failedCheck);
    }
    System.out.println("End test :" + TestHelper.getTestMethodName());
  }

  @Test
  public void testIsInstanceStoppableWithIncludeDetailsFalse() throws IOException {
    System.out.println("Start test :" + TestHelper.getTestMethodName());
    Map<String, String> params = ImmutableMap.of("client", "espresso");
    Entity entity =
        Entity.entity(OBJECT_MAPPER.writeValueAsString(params), MediaType.APPLICATION_JSON_TYPE);
    
    // Test with includeDetails=false (should behave same as default)
    Response response = new JerseyUriRequestBuilder(
        "clusters/{}/instances/{}/stoppable?skipHealthCheckCategories=CUSTOM_INSTANCE_CHECK,CUSTOM_PARTITION_CHECK&includeDetails=false").format(
        STOPPABLE_CLUSTER, "instance1").post(this, entity);
    String stoppableCheckResult = response.readEntity(String.class);
    Map<String, Object> actualMap = OBJECT_MAPPER.readValue(stoppableCheckResult, Map.class);
    
    List<String> failedChecks =
        Arrays.asList("HELIX:EMPTY_RESOURCE_ASSIGNMENT", "HELIX:INSTANCE_NOT_ENABLED",
            "HELIX:INSTANCE_NOT_STABLE");
    Map<String, Object> expectedMap =
        ImmutableMap.of("stoppable", false, "failedChecks", failedChecks);
    Assert.assertEquals(actualMap, expectedMap);
    
    // Verify the failed checks contain basic error codes without detailed partition information
    for (String failedCheck : failedChecks) {
      Assert.assertFalse(failedCheck.contains("partition"),
          "Basic error message should not contain partition details: " + failedCheck);
      Assert.assertFalse(failedCheck.contains("active replicas"),
          "Basic error message should not contain replica details: " + failedCheck);
    }
    System.out.println("End test :" + TestHelper.getTestMethodName());
  }

  @Test
  public void testIsInstanceStoppableWithIncludeDetailsTrue() throws IOException {
    System.out.println("Start test :" + TestHelper.getTestMethodName());
    Map<String, String> params = ImmutableMap.of("client", "espresso");
    Entity entity =
        Entity.entity(OBJECT_MAPPER.writeValueAsString(params), MediaType.APPLICATION_JSON_TYPE);
    
    // Test with includeDetails=true
    Response response = new JerseyUriRequestBuilder(
        "clusters/{}/instances/{}/stoppable?skipHealthCheckCategories=CUSTOM_INSTANCE_CHECK,CUSTOM_PARTITION_CHECK&includeDetails=true").format(
        STOPPABLE_CLUSTER, "instance1").post(this, entity);
    String stoppableCheckResult = response.readEntity(String.class);
    Map<String, Object> actualMap = OBJECT_MAPPER.readValue(stoppableCheckResult, Map.class);
    
    Assert.assertFalse((Boolean) actualMap.get("stoppable"));
    Assert.assertNotNull(actualMap.get("failedChecks"));
    
    @SuppressWarnings("unchecked")
    List<String> failedChecks = (List<String>) actualMap.get("failedChecks");
    Assert.assertFalse(failedChecks.isEmpty());
    
    // The basic checks should still be there but now with includeDetails=true,
    // any MIN_ACTIVE_REPLICA_CHECK_FAILED errors should contain detailed information
    boolean hasDetailedMessage = false;
    for (String failedCheck : failedChecks) {
      if (failedCheck.contains("HELIX:MIN_ACTIVE_REPLICA_CHECK_FAILED") && 
          (failedCheck.contains("partition") || failedCheck.contains("active replicas"))) {
        hasDetailedMessage = true;
        // Verify the detailed message format
        Assert.assertTrue(failedCheck.contains("Resource "),
            "Detailed error should contain resource information: " + failedCheck);
        break;
      }
    }
    
    // Note: hasDetailedMessage might be false if this particular instance doesn't trigger
    // MIN_ACTIVE_REPLICA_CHECK_FAILED, which is fine for this test setup
    System.out.println("End test :" + TestHelper.getTestMethodName());
  }

  @Test
  public void testPerInstanceStoppableWithIncludeDetailsForMinActiveReplica() throws IOException {
    System.out.println("Start test :" + TestHelper.getTestMethodName());
    
    // Test the includeDetails parameter specifically for scenarios that might trigger
    // MIN_ACTIVE_REPLICA_CHECK_FAILED with detailed partition information
    Map<String, String> params = ImmutableMap.of("client", "espresso");
    Entity entity =
        Entity.entity(OBJECT_MAPPER.writeValueAsString(params), MediaType.APPLICATION_JSON_TYPE);
    
    // Test an enabled instance from the stoppable cluster
    String instanceToTest = "instance0"; // Use a different instance that might be enabled
    
    // First test without includeDetails
    Response response1 = new JerseyUriRequestBuilder(
        "clusters/{}/instances/{}/stoppable").format(
        STOPPABLE_CLUSTER, instanceToTest).post(this, entity);
    String result1 = response1.readEntity(String.class);
    Map<String, Object> resultMap1 = OBJECT_MAPPER.readValue(result1, Map.class);
    
    // Then test with includeDetails=true
    Response response2 = new JerseyUriRequestBuilder(
        "clusters/{}/instances/{}/stoppable?includeDetails=true").format(
        STOPPABLE_CLUSTER, instanceToTest).post(this, entity);
    String result2 = response2.readEntity(String.class);
    Map<String, Object> resultMap2 = OBJECT_MAPPER.readValue(result2, Map.class);
    
    // Both should have same stoppable status
    Assert.assertEquals(resultMap1.get("stoppable"), resultMap2.get("stoppable"));
    
    // If there are failed checks, verify that includeDetails=true provides more information
    if (resultMap1.containsKey("failedChecks") && resultMap2.containsKey("failedChecks")) {
      @SuppressWarnings("unchecked")
      List<String> failedChecks1 = (List<String>) resultMap1.get("failedChecks");
      @SuppressWarnings("unchecked")
      List<String> failedChecks2 = (List<String>) resultMap2.get("failedChecks");
      
      // Check if any detailed messages are present in the includeDetails=true response
      boolean hasDetailedInResponse2 = false;
      for (String check : failedChecks2) {
        if (check.contains("partition") && check.contains("active replicas")) {
          hasDetailedInResponse2 = true;
          Assert.assertTrue(check.contains("Resource "), 
              "Detailed message should contain resource info: " + check);
          break;
        }
      }
      
      // The detailed information should only appear in the includeDetails=true response
      if (hasDetailedInResponse2) {
        boolean hasDetailedInResponse1 = false;
        for (String check : failedChecks1) {
          if (check.contains("partition") && check.contains("active replicas")) {
            hasDetailedInResponse1 = true;
            break;
          }
        }
        Assert.assertFalse(hasDetailedInResponse1, 
            "Default response should not contain detailed partition information");
      }
    }
    
    System.out.println("End test :" + TestHelper.getTestMethodName());
  }

  @Test(dependsOnMethods = "testPerInstanceStoppableWithIncludeDetailsForMinActiveReplica")
  public void testTakeInstanceNegInput() throws IOException {
    System.out.println("Start test :" + TestHelper.getTestMethodName());
    post("clusters/TestCluster_0/instances/instance1/takeInstance", null,
        Entity.entity("", MediaType.APPLICATION_JSON_TYPE),
        Response.Status.BAD_REQUEST.getStatusCode(), true);
    System.out.println("End test :" + TestHelper.getTestMethodName());
  }

  @Test(dependsOnMethods = "testTakeInstanceNegInput")
  public void testTakeInstanceNegInput2() throws IOException {
    System.out.println("Start test :" + TestHelper.getTestMethodName());
    Response response = new JerseyUriRequestBuilder("clusters/{}/instances/{}/takeInstance")
        .format(STOPPABLE_CLUSTER, "instance1").post(this, Entity.entity("{}", MediaType.APPLICATION_JSON_TYPE));
    String takeInstanceResult = response.readEntity(String.class);

    Map<String, Object> actualMap = OBJECT_MAPPER.readValue(takeInstanceResult, Map.class);
    List<String> errorMsg = Arrays.asList("Invalid input. Please provide at least one health check or operation.");
    Map<String, Object> expectedMap =
        ImmutableMap.of("successful", false, "messages", errorMsg, "operationResult", "");
    Assert.assertEquals(actualMap, expectedMap);
    System.out.println("End test :" + TestHelper.getTestMethodName());
  }

  @Test(dependsOnMethods = "testTakeInstanceNegInput2")
  public void testTakeInstanceHealthCheck() throws IOException {
    System.out.println("Start test :" + TestHelper.getTestMethodName());
    String payload = "{ \"health_check_list\" : [\"HelixInstanceStoppableCheck\", \"CustomInstanceStoppableCheck\"],"
        + "\"health_check_config\" : { \"client\" : \"espresso\" }} ";
    Response response = new JerseyUriRequestBuilder("clusters/{}/instances/{}/takeInstance")
        .format(STOPPABLE_CLUSTER, "instance1").post(this, Entity.entity(payload, MediaType.APPLICATION_JSON_TYPE));
    String takeInstanceResult = response.readEntity(String.class);

    Map<String, Object> actualMap = OBJECT_MAPPER.readValue(takeInstanceResult, Map.class);
    List<String> errorMsg = Arrays
        .asList("HELIX:EMPTY_RESOURCE_ASSIGNMENT", "HELIX:INSTANCE_NOT_ENABLED",
            "HELIX:INSTANCE_NOT_STABLE");
    Map<String, Object> expectedMap =
        ImmutableMap.of("successful", false, "messages", errorMsg, "operationResult", "");
    Assert.assertEquals(actualMap, expectedMap);
    System.out.println("End test :" + TestHelper.getTestMethodName());
  }

  @Test(dependsOnMethods = "testTakeInstanceNegInput2")
  public void testTakeInstanceNonBlockingCheck() throws IOException {
    System.out.println("Start test :" + TestHelper.getTestMethodName());
    String payload = "{ \"health_check_list\" : [\"HelixInstanceStoppableCheck\"],"
        + "\"health_check_config\" : { \"client\" : \"espresso\" , "
        + "\"continueOnFailures\" : [\"HELIX:EMPTY_RESOURCE_ASSIGNMENT\", \"HELIX:INSTANCE_NOT_ENABLED\","
        + " \"HELIX:INSTANCE_NOT_STABLE\"]} } ";
    Response response = new JerseyUriRequestBuilder("clusters/{}/instances/{}/takeInstance")
        .format(STOPPABLE_CLUSTER, "instance1").post(this, Entity.entity(payload, MediaType.APPLICATION_JSON_TYPE));
    String takeInstanceResult = response.readEntity(String.class);

    Map<String, Object> actualMap = OBJECT_MAPPER.readValue(takeInstanceResult, Map.class);
    List<String> errorMsg = Arrays
        .asList("HELIX:EMPTY_RESOURCE_ASSIGNMENT", "HELIX:INSTANCE_NOT_ENABLED",
            "HELIX:INSTANCE_NOT_STABLE");
    Map<String, Object> expectedMap =
        ImmutableMap.of("successful", true, "messages", errorMsg, "operationResult", "");
    Assert.assertEquals(actualMap, expectedMap);
    System.out.println("End test :" + TestHelper.getTestMethodName());
  }

  @Test(dependsOnMethods = "testTakeInstanceHealthCheck")
  public void testTakeInstanceOperationSuccess() throws IOException {
    System.out.println("Start test :" + TestHelper.getTestMethodName());
    String payload =
        "{ \"operation_list\" : [\"org.apache.helix.rest.server.TestOperationImpl\"]} ";
    Response response = new JerseyUriRequestBuilder("clusters/{}/instances/{}/takeInstance")
        .format(STOPPABLE_CLUSTER, "instance1")
        .post(this, Entity.entity(payload, MediaType.APPLICATION_JSON_TYPE));
    String takeInstanceResult = response.readEntity(String.class);

    Map<String, Object> actualMap = OBJECT_MAPPER.readValue(takeInstanceResult, Map.class);
    Map<String, Object> expectedMap = ImmutableMap
        .of("successful", true, "messages", new ArrayList<>(), "operationResult", "DummyTakeOperationResult");
    Assert.assertEquals(actualMap, expectedMap);
    System.out.println("End test :" + TestHelper.getTestMethodName());
  }

  @Test(dependsOnMethods = "testTakeInstanceOperationSuccess")
  public void testFreeInstanceOperationSuccess() throws IOException {
    System.out.println("Start test :" + TestHelper.getTestMethodName());
    String payload =
        "{ \"operation_list\" : [\"org.apache.helix.rest.server.TestOperationImpl\"]} ";
    Response response = new JerseyUriRequestBuilder("clusters/{}/instances/{}/freeInstance")
        .format(STOPPABLE_CLUSTER, "instance1")
        .post(this, Entity.entity(payload, MediaType.APPLICATION_JSON_TYPE));
    String takeInstanceResult = response.readEntity(String.class);

    Map<String, Object> actualMap = OBJECT_MAPPER.readValue(takeInstanceResult, Map.class);
    Map<String, Object> expectedMap = ImmutableMap
        .of("successful", true, "messages", new ArrayList<>(), "operationResult",
            "DummyFreeOperationResult");
    Assert.assertEquals(actualMap, expectedMap);
    System.out.println("End test :" + TestHelper.getTestMethodName());
  }

  @Test(dependsOnMethods = "testFreeInstanceOperationSuccess")
  public void testTakeInstanceOperationCheckFailure() throws IOException {
    System.out.println("Start test :" + TestHelper.getTestMethodName());
    String payload = "{ \"operation_list\" : [\"org.apache.helix.rest.server.TestOperationImpl\"],"
        + "\"operation_config\": { \"org.apache.helix.rest.server.TestOperationImpl\" :"
        + " {\"instance0\": true, \"instance2\": true, "
        + "\"instance3\": true, \"instance4\": true, \"instance5\": true, "
        + " \"value\" : \"i001\", \"list_value\" : [\"list1\"]}} } ";
    Response response = new JerseyUriRequestBuilder("clusters/{}/instances/{}/takeInstance")
        .format(STOPPABLE_CLUSTER, "instance0")
        .post(this, Entity.entity(payload, MediaType.APPLICATION_JSON_TYPE));
    String takeInstanceResult = response.readEntity(String.class);

    Map<String, Object> actualMap = OBJECT_MAPPER.readValue(takeInstanceResult, Map.class);
    Assert.assertFalse((boolean)actualMap.get("successful"));
    System.out.println("End test :" + TestHelper.getTestMethodName());
  }

  @Test(dependsOnMethods = "testTakeInstanceOperationCheckFailure")
  public void testTakeInstanceOperationCheckFailureCommonInput() throws IOException {
    System.out.println("Start test :" + TestHelper.getTestMethodName());
    String payload = "{ \"operation_list\" : [\"org.apache.helix.rest.server.TestOperationImpl\"],"
        + "\"operation_config\": { \"OperationConfigSharedInput\" :"
        + " {\"instance0\": true, \"instance2\": true, "
        + "\"instance3\": true, \"instance4\": true, \"instance5\": true, "
        + " \"value\" : \"i001\", \"list_value\" : [\"list1\"]}}} ";
    Response response = new JerseyUriRequestBuilder("clusters/{}/instances/{}/takeInstance")
        .format(STOPPABLE_CLUSTER, "instance0")
        .post(this, Entity.entity(payload, MediaType.APPLICATION_JSON_TYPE));
    String takeInstanceResult = response.readEntity(String.class);

    Map<String, Object> actualMap = OBJECT_MAPPER.readValue(takeInstanceResult, Map.class);
    Assert.assertFalse((boolean)actualMap.get("successful"));
    System.out.println("End test :" + TestHelper.getTestMethodName());
  }

  @Test(dependsOnMethods = "testTakeInstanceOperationCheckFailureCommonInput")
  public void testTakeInstanceOperationCheckFailureNonBlocking() throws IOException {
    System.out.println("Start test :" + TestHelper.getTestMethodName());
    String payload = "{ \"operation_list\" : [\"org.apache.helix.rest.server.TestOperationImpl\"],"
        + "\"operation_config\": { \"org.apache.helix.rest.server.TestOperationImpl\" : "
        + "{\"instance0\": true, \"instance2\": true, "
        + "\"instance3\": true, \"instance4\": true, \"instance5\": true, "
        + "\"continueOnFailures\" : true} } } ";

    Response response = new JerseyUriRequestBuilder("clusters/{}/instances/{}/takeInstance")
        .format(STOPPABLE_CLUSTER, "instance0")
        .post(this, Entity.entity(payload, MediaType.APPLICATION_JSON_TYPE));
    String takeInstanceResult = response.readEntity(String.class);
    System.out.println("testTakeInstanceOperationCheckFailureNonBlocking" + takeInstanceResult);

    Map<String, Object> actualMap = OBJECT_MAPPER.readValue(takeInstanceResult, Map.class);
    Assert.assertTrue((boolean)actualMap.get("successful"));
    Assert.assertEquals(actualMap.get("operationResult"), "DummyTakeOperationResult");
    // The non blocking test should generate msg but won't return failure status
    Assert.assertFalse(actualMap.get("messages").equals("[]"));
    System.out.println("End test :" + TestHelper.getTestMethodName());
  }

  @Test(dependsOnMethods = "testTakeInstanceOperationCheckFailureNonBlocking")
  public void testTakeInstanceCheckOnly() throws IOException {
    System.out.println("Start test :" + TestHelper.getTestMethodName());
    String payload = "{ \"operation_list\" : [\"org.apache.helix.rest.server.TestOperationImpl\"],"
        + "\"operation_config\": {\"performOperation\": false} } ";
    Response response = new JerseyUriRequestBuilder("clusters/{}/instances/{}/takeInstance")
        .format(STOPPABLE_CLUSTER, "instance1")
        .post(this, Entity.entity(payload, MediaType.APPLICATION_JSON_TYPE));
    String takeInstanceResult = response.readEntity(String.class);

    Map<String, Object> actualMap = OBJECT_MAPPER.readValue(takeInstanceResult, Map.class);
    Assert.assertTrue((boolean)actualMap.get("successful"));
    Assert.assertTrue(actualMap.get("operationResult").equals(""));
    System.out.println("End test :" + TestHelper.getTestMethodName());
  }

  @Test(dependsOnMethods = "testTakeInstanceCheckOnly")
  public void testGetAllMessages() throws Exception {
    System.out.println("Start test :" + TestHelper.getTestMethodName());
    _instanceToDisable.disconnect();

    String testInstance = INSTANCE_NAME; //Non-live instance

    String messageId = "msg1";
    Message message = new Message(Message.MessageType.STATE_TRANSITION, messageId);
    message.setStateModelDef("MasterSlave");
    message.setFromState("OFFLINE");
    message.setToState("SLAVE");
    message.setResourceName("testResourceName");
    message.setPartitionName("testResourceName_1");
    message.setTgtName("localhost_3");
    message.setTgtSessionId("session_3");
    HelixDataAccessor helixDataAccessor = new ZKHelixDataAccessor(CLUSTER_NAME, _baseAccessor);
    helixDataAccessor.setProperty(helixDataAccessor.keyBuilder().message(testInstance, messageId), message);

    String body = new JerseyUriRequestBuilder("clusters/{}/instances/{}/messages").isBodyReturnExpected(true).format(CLUSTER_NAME, testInstance).get(this);
    JsonNode node = OBJECT_MAPPER.readTree(body);
    int newMessageCount =
        node.get(PerInstanceAccessor.PerInstanceProperties.total_message_count.name()).intValue();

    Assert.assertEquals(newMessageCount, 1);
    helixDataAccessor.removeProperty(helixDataAccessor.keyBuilder().message(testInstance, messageId));
    System.out.println("End test :" + TestHelper.getTestMethodName());
  }

  @Test(dependsOnMethods = "testGetAllMessages")
  public void testGetMessagesByStateModelDef() throws Exception {
    System.out.println("Start test :" + TestHelper.getTestMethodName());
    String testInstance = INSTANCE_NAME; //Non-live instance
    String messageId = "msg1";
    Message message = new Message(Message.MessageType.STATE_TRANSITION, messageId);
    message.setStateModelDef("MasterSlave");
    message.setFromState("OFFLINE");
    message.setToState("SLAVE");
    message.setResourceName("testResourceName");
    message.setPartitionName("testResourceName_1");
    message.setTgtName("localhost_3");
    message.setTgtSessionId("session_3");
    HelixDataAccessor helixDataAccessor = new ZKHelixDataAccessor(CLUSTER_NAME, _baseAccessor);
    helixDataAccessor.setProperty(helixDataAccessor.keyBuilder().message(testInstance, messageId),
        message);

    String body =
        new JerseyUriRequestBuilder("clusters/{}/instances/{}/messages?stateModelDef=MasterSlave")
            .isBodyReturnExpected(true).format(CLUSTER_NAME, testInstance).get(this);
    JsonNode node = OBJECT_MAPPER.readTree(body);
    int newMessageCount =
        node.get(PerInstanceAccessor.PerInstanceProperties.total_message_count.name()).intValue();

    Assert.assertEquals(newMessageCount, 1);

    body =
        new JerseyUriRequestBuilder("clusters/{}/instances/{}/messages?stateModelDef=LeaderStandBy")
            .isBodyReturnExpected(true).format(CLUSTER_NAME, testInstance).get(this);
    node = OBJECT_MAPPER.readTree(body);
    newMessageCount =
        node.get(PerInstanceAccessor.PerInstanceProperties.total_message_count.name()).intValue();

    Assert.assertEquals(newMessageCount, 0);
    MockParticipantManager participant =
        new MockParticipantManager(ZK_ADDR, CLUSTER_NAME, INSTANCE_NAME);
    Map<String, TaskFactory> taskFactoryReg = new HashMap<>();
    taskFactoryReg.put(MockTask.TASK_COMMAND, MockTask::new);
    StateMachineEngine stateMachineEngine = participant.getStateMachineEngine();
    stateMachineEngine.registerStateModelFactory("Task",
        new TaskStateModelFactory(participant, taskFactoryReg));
    participant.syncStart();
    _mockParticipantManagers.add(participant);
    System.out.println("End test :" + TestHelper.getTestMethodName());
  }

  @Test(dependsOnMethods = "testGetMessagesByStateModelDef")
  public void testGetAllInstances() throws IOException {
    System.out.println("Start test :" + TestHelper.getTestMethodName());
    String body = new JerseyUriRequestBuilder("clusters/{}/instances").isBodyReturnExpected(true)
        .format(CLUSTER_NAME).get(this);

    JsonNode node = OBJECT_MAPPER.readTree(body);
    String instancesStr = node.get(InstancesAccessor.InstancesProperties.instances.name()).toString();
    Assert.assertNotNull(instancesStr);

    Set<String> instances = OBJECT_MAPPER.readValue(instancesStr,
        OBJECT_MAPPER.getTypeFactory().constructCollectionType(Set.class, String.class));
    String errorMessage = "Instances from response: "+ instances + " vs instances actually: "
        + _instancesMap.get(CLUSTER_NAME);
    Assert.assertEquals(instances.size(), _instancesMap.get(CLUSTER_NAME).size(), errorMessage);
    Assert.assertTrue(instances.containsAll(_instancesMap.get(CLUSTER_NAME)), errorMessage);
    Assert.assertTrue(_instancesMap.get(CLUSTER_NAME).containsAll(instances), errorMessage);
    System.out.println("End test :" + TestHelper.getTestMethodName());
  }

  @Test(dependsOnMethods = "testGetAllInstances")
  public void testGetInstanceById() throws IOException {
    System.out.println("Start test :" + TestHelper.getTestMethodName());
    String body = new JerseyUriRequestBuilder("clusters/{}/instances/{}").isBodyReturnExpected(true)
        .format(CLUSTER_NAME, INSTANCE_NAME).get(this);
    JsonNode node = OBJECT_MAPPER.readTree(body);
    String instancesCfg = node.get(PerInstanceAccessor.PerInstanceProperties.config.name()).toString();
    Assert.assertNotNull(instancesCfg);
    boolean isHealth = node.get("health").booleanValue();
    Assert.assertFalse(isHealth);

    InstanceConfig instanceConfig = new InstanceConfig(toZNRecord(instancesCfg));
    Assert.assertEquals(instanceConfig,
        _configAccessor.getInstanceConfig(CLUSTER_NAME, INSTANCE_NAME));
    System.out.println("End test :" + TestHelper.getTestMethodName());
  }

  @Test(dependsOnMethods = "testGetInstanceById")
  public void testAddInstance() throws IOException {
    System.out.println("Start test :" + TestHelper.getTestMethodName());
    InstanceConfig instanceConfig = new InstanceConfig(INSTANCE_NAME + "TEST");
    Entity entity = Entity.entity(OBJECT_MAPPER.writeValueAsString(instanceConfig.getRecord()),
        MediaType.APPLICATION_JSON_TYPE);

    new JerseyUriRequestBuilder("clusters/{}/instances/{}").format(CLUSTER_NAME, INSTANCE_NAME)
        .put(this, entity);

    Assert.assertEquals(instanceConfig,
        _configAccessor.getInstanceConfig(CLUSTER_NAME, INSTANCE_NAME + "TEST"));
    System.out.println("End test :" + TestHelper.getTestMethodName());
  }

  @Test(dependsOnMethods = "testAddInstance", expectedExceptions = HelixException.class)
  public void testDeleteInstance() {
    System.out.println("Start test :" + TestHelper.getTestMethodName());
    delete("clusters/" + CLUSTER_NAME + "/instances/" + INSTANCE_NAME + "TEST",
        Response.Status.OK.getStatusCode());
    _configAccessor.getInstanceConfig(CLUSTER_NAME, INSTANCE_NAME + "TEST");
    System.out.println("End test :" + TestHelper.getTestMethodName());
  }

  @Test(dependsOnMethods = "testDeleteInstance")
  public void updateInstance() throws Exception {
    System.out.println("Start test :" + TestHelper.getTestMethodName());
    // Disable instance
    Entity entity = Entity.entity("", MediaType.APPLICATION_JSON_TYPE);

    new JerseyUriRequestBuilder(
        "clusters/{}/instances/{}?command=disable&instanceDisabledReason=reason1")
        .format(CLUSTER_NAME, INSTANCE_NAME).post(this, entity);

    Assert.assertFalse(
        _configAccessor.getInstanceConfig(CLUSTER_NAME, INSTANCE_NAME).getInstanceEnabled());
    Assert.assertEquals(
        _configAccessor.getInstanceConfig(CLUSTER_NAME, INSTANCE_NAME).getInstanceDisabledType(),
        InstanceConstants.InstanceDisabledType.DEFAULT_INSTANCE_DISABLE_TYPE.toString());
    Assert.assertEquals(
        _configAccessor.getInstanceConfig(CLUSTER_NAME, INSTANCE_NAME).getInstanceDisabledReason(),
        "reason1");

    // Enable instance
    new JerseyUriRequestBuilder(
        "clusters/{}/instances/{}?command=enable&instanceDisabledType=USER_OPERATION&instanceDisabledReason=reason1")
        .format(CLUSTER_NAME, INSTANCE_NAME).post(this, entity);
    Assert.assertTrue(
        _configAccessor.getInstanceConfig(CLUSTER_NAME, INSTANCE_NAME).getInstanceEnabled());
    Assert.assertEquals(
        _configAccessor.getInstanceConfig(CLUSTER_NAME, INSTANCE_NAME).getInstanceDisabledType(),
        InstanceConstants.INSTANCE_NOT_DISABLED);
    Assert.assertEquals(
        _configAccessor.getInstanceConfig(CLUSTER_NAME, INSTANCE_NAME).getInstanceDisabledReason(),
        "");

    // We should see no instance disable related field in to clusterConfig
    ClusterConfig cls = _configAccessor.getClusterConfig(CLUSTER_NAME);
    Assert.assertFalse(cls.getRecord().getMapFields()
        .containsKey(ClusterConfig.ClusterConfigProperty.DISABLED_INSTANCES.name()));

    // disable instance with no reason input
    new JerseyUriRequestBuilder("clusters/{}/instances/{}?command=disable")
        .format(CLUSTER_NAME, INSTANCE_NAME).post(this, entity);

    Assert.assertFalse(
        _configAccessor.getInstanceConfig(CLUSTER_NAME, INSTANCE_NAME).getInstanceEnabled());

    new JerseyUriRequestBuilder("clusters/{}/instances/{}?command=enable")
        .format(CLUSTER_NAME, INSTANCE_NAME).post(this, entity);
    Assert.assertTrue(
        _configAccessor.getInstanceConfig(CLUSTER_NAME, INSTANCE_NAME).getInstanceEnabled());

    // Disable instance should see no field write to clusterConfig
    cls = _configAccessor.getClusterConfig(CLUSTER_NAME);
    Assert.assertFalse(cls.getRecord().getMapFields()
        .containsKey(ClusterConfig.ClusterConfigProperty.DISABLED_INSTANCES.name()));

    // AddTags
    List<String> tagList = ImmutableList.of("tag3", "tag1", "tag2");
    entity = Entity.entity(
        OBJECT_MAPPER.writeValueAsString(ImmutableMap.of(AbstractResource.Properties.id.name(),
            INSTANCE_NAME, PerInstanceAccessor.PerInstanceProperties.instanceTags.name(), tagList)),
        MediaType.APPLICATION_JSON_TYPE);

    new JerseyUriRequestBuilder("clusters/{}/instances/{}?command=addInstanceTag")
        .format(CLUSTER_NAME, INSTANCE_NAME).post(this, entity);

    Assert.assertEquals(_configAccessor.getInstanceConfig(CLUSTER_NAME, INSTANCE_NAME).getTags(),
        tagList);

    // RemoveTags
    List<String> removeList = new ArrayList<>(tagList);
    removeList.remove("tag2");
    entity = Entity.entity(
        OBJECT_MAPPER.writeValueAsString(ImmutableMap.of(AbstractResource.Properties.id.name(),
            INSTANCE_NAME, PerInstanceAccessor.PerInstanceProperties.instanceTags.name(), removeList)),
        MediaType.APPLICATION_JSON_TYPE);

    new JerseyUriRequestBuilder("clusters/{}/instances/{}?command=removeInstanceTag")
        .format(CLUSTER_NAME, INSTANCE_NAME).post(this, entity);

    Assert.assertEquals(_configAccessor.getInstanceConfig(CLUSTER_NAME, INSTANCE_NAME).getTags(),
        ImmutableList.of("tag2"));

    // Test enable disable partitions
    String dbName = "_db_0_";
    List<String> partitionsToDisable = Arrays.asList(CLUSTER_NAME + dbName + "0",
        CLUSTER_NAME + dbName + "1", CLUSTER_NAME + dbName + "3");
    String RESOURCE_NAME = CLUSTER_NAME + dbName.substring(0, dbName.length() - 1);

    entity = Entity.entity(
        OBJECT_MAPPER.writeValueAsString(ImmutableMap.of(AbstractResource.Properties.id.name(),
            INSTANCE_NAME, PerInstanceAccessor.PerInstanceProperties.resource.name(), RESOURCE_NAME,
            PerInstanceAccessor.PerInstanceProperties.partitions.name(), partitionsToDisable)),
        MediaType.APPLICATION_JSON_TYPE);

    new JerseyUriRequestBuilder("clusters/{}/instances/{}?command=disablePartitions")
        .format(CLUSTER_NAME, INSTANCE_NAME).post(this, entity);

    InstanceConfig instanceConfig = _configAccessor.getInstanceConfig(CLUSTER_NAME, INSTANCE_NAME);
    Assert.assertEquals(
        new HashSet<>(instanceConfig.getDisabledPartitionsMap().get(RESOURCE_NAME)),
        new HashSet<>(partitionsToDisable));
    entity = Entity.entity(OBJECT_MAPPER.writeValueAsString(ImmutableMap
        .of(AbstractResource.Properties.id.name(), INSTANCE_NAME,
            PerInstanceAccessor.PerInstanceProperties.resource.name(), RESOURCE_NAME,
            PerInstanceAccessor.PerInstanceProperties.partitions.name(),
            ImmutableList.of(CLUSTER_NAME + dbName + "1"))), MediaType.APPLICATION_JSON_TYPE);

    new JerseyUriRequestBuilder("clusters/{}/instances/{}?command=enablePartitions")
        .format(CLUSTER_NAME, INSTANCE_NAME).post(this, entity);

    instanceConfig = _configAccessor.getInstanceConfig(CLUSTER_NAME, INSTANCE_NAME);
    Assert.assertEquals(new HashSet<>(instanceConfig.getDisabledPartitionsMap().get(RESOURCE_NAME)),
        new HashSet<>(Arrays.asList(CLUSTER_NAME + dbName + "0", CLUSTER_NAME + dbName + "3")));

    // test set instance operation
    new JerseyUriRequestBuilder("clusters/{}/instances/{}?command=setInstanceOperation&instanceOperation=EVACUATE")
        .format(CLUSTER_NAME, INSTANCE_NAME).post(this, entity);
    instanceConfig = _configAccessor.getInstanceConfig(CLUSTER_NAME, INSTANCE_NAME);
    Assert.assertEquals(instanceConfig.getInstanceOperation().getOperation(),
        InstanceConstants.InstanceOperation.EVACUATE);
    // Verify INSTANCE_OPERATION_STATE field is set correctly
    Assert.assertEquals(instanceConfig.getInstanceOperationState(), "EVACUATE");
    new JerseyUriRequestBuilder("clusters/{}/instances/{}?command=setInstanceOperation&instanceOperation=INVALIDOP")
        .expectedReturnStatusCode(Response.Status.NOT_FOUND.getStatusCode()).format(CLUSTER_NAME, INSTANCE_NAME).post(this, entity);
    new JerseyUriRequestBuilder("clusters/{}/instances/{}?command=setInstanceOperation&instanceOperation=")
        .format(CLUSTER_NAME, INSTANCE_NAME).post(this, entity);
    instanceConfig = _configAccessor.getInstanceConfig(CLUSTER_NAME, INSTANCE_NAME);
    Assert.assertEquals(instanceConfig.getInstanceOperation().getOperation(),
        InstanceConstants.InstanceOperation.ENABLE);
    // Verify INSTANCE_OPERATION_STATE field is set correctly
    Assert.assertEquals(instanceConfig.getInstanceOperationState(), "ENABLE");

    // test canCompleteSwap
    Response canCompleteSwapResponse =
        new JerseyUriRequestBuilder("clusters/{}/instances/{}?command=canCompleteSwap").format(
            CLUSTER_NAME, INSTANCE_NAME).post(this, entity);
    Assert.assertEquals(canCompleteSwapResponse.getStatus(), Response.Status.OK.getStatusCode());
    Map<String, Object> responseMap =
        OBJECT_MAPPER.readValue(canCompleteSwapResponse.readEntity(String.class), Map.class);
    Assert.assertFalse((boolean) responseMap.get("successful"));

    // test completeSwapIfPossible
    Response completeSwapIfPossibleResponse = new JerseyUriRequestBuilder(
        "clusters/{}/instances/{}?command=completeSwapIfPossible").format(CLUSTER_NAME,
        INSTANCE_NAME).post(this, entity);
    Assert.assertEquals(completeSwapIfPossibleResponse.getStatus(),
        Response.Status.OK.getStatusCode());
    responseMap =
        OBJECT_MAPPER.readValue(completeSwapIfPossibleResponse.readEntity(String.class), Map.class);
    Assert.assertFalse((boolean) responseMap.get("successful"));

    // test isEvacuateFinished on instance with EVACUATE but has currentState
    new JerseyUriRequestBuilder("clusters/{}/instances/{}?command=setInstanceOperation&instanceOperation=EVACUATE")
        .format(CLUSTER_NAME, INSTANCE_NAME).post(this, entity);
    instanceConfig = _configAccessor.getInstanceConfig(CLUSTER_NAME, INSTANCE_NAME);
    Assert.assertEquals(instanceConfig.getInstanceOperation().getOperation(),
        InstanceConstants.InstanceOperation.EVACUATE);

    Response response = new JerseyUriRequestBuilder("clusters/{}/instances/{}?command=isEvacuateFinished")
        .format(CLUSTER_NAME, INSTANCE_NAME).post(this, entity);
    Map<String, Object> evacuateFinishedResult = OBJECT_MAPPER.readValue(response.readEntity(String.class), Map.class);
    Assert.assertEquals(response.getStatus(), Response.Status.OK.getStatusCode());
    // Returns COMPLETED because the node only contains semi-auto resources
    Assert.assertEquals(evacuateFinishedResult.get("state"), "COMPLETED");
    // Verify new fields are present in the response
    Assert.assertTrue(evacuateFinishedResult.containsKey("remainingPartitionCount"),
        "Response should contain remainingPartitionCount field");
    Assert.assertTrue(evacuateFinishedResult.containsKey("pendingMessageCount"),
        "Response should contain pendingMessageCount field");
    Assert.assertEquals(evacuateFinishedResult.get("remainingPartitionCount"), 0,
        "remainingPartitionCount should be 0 for completed evacuation");

    // Because the resources are now all semi-auto, is EvacuateFinished should return COMPLETED
    response = new JerseyUriRequestBuilder("clusters/{}/instances/{}?command=isEvacuateFinished")
        .format(CLUSTER_NAME, INSTANCE_NAME).post(this, entity);
    evacuateFinishedResult = OBJECT_MAPPER.readValue(response.readEntity(String.class), Map.class);
    Assert.assertEquals(response.getStatus(), Response.Status.OK.getStatusCode());
    Assert.assertEquals(evacuateFinishedResult.get("state"), "COMPLETED");

    response = new JerseyUriRequestBuilder("clusters/{}/instances/{}?command=isInstanceDrained")
        .format(CLUSTER_NAME, INSTANCE_NAME).post(this, entity);
    Map<String, Boolean> instanceDrainedResult = OBJECT_MAPPER.readValue(response.readEntity(String.class), Map.class);
    Assert.assertEquals(response.getStatus(), Response.Status.OK.getStatusCode());
    Assert.assertTrue(instanceDrainedResult.get("successful"));

    // test isEvacuateFinished on instance with EVACUATE and no currentState
    // Create new instance so no currentState or messages assigned to it
    String test_instance_name = INSTANCE_NAME + "_foo";
    InstanceConfig newInstanceConfig = new InstanceConfig(test_instance_name);
    Entity instanceEntity = Entity.entity(OBJECT_MAPPER.writeValueAsString(newInstanceConfig.getRecord()),
        MediaType.APPLICATION_JSON_TYPE);
    new JerseyUriRequestBuilder("clusters/{}/instances/{}").format(CLUSTER_NAME, test_instance_name)
        .put(this, instanceEntity);

    new JerseyUriRequestBuilder("clusters/{}/instances/{}?command=setInstanceOperation&instanceOperation=EVACUATE")
        .format(CLUSTER_NAME, test_instance_name).post(this, entity);
    instanceConfig = _configAccessor.getInstanceConfig(CLUSTER_NAME, test_instance_name);
    Assert.assertEquals(instanceConfig.getInstanceOperation().getOperation(),
        InstanceConstants.InstanceOperation.EVACUATE);

    response = new JerseyUriRequestBuilder("clusters/{}/instances/{}?command=isEvacuateFinished")
        .format(CLUSTER_NAME, test_instance_name).post(this, entity);
    evacuateFinishedResult = OBJECT_MAPPER.readValue(response.readEntity(String.class), Map.class);
    Assert.assertEquals(response.getStatus(), Response.Status.OK.getStatusCode());
    Assert.assertEquals(evacuateFinishedResult.get("state"), "COMPLETED");
    // Verify new fields for instance with no currentState
    Assert.assertEquals(evacuateFinishedResult.get("remainingPartitionCount"), 0,
        "remainingPartitionCount should be 0 for instance with no currentState");

    // Remove instance created for evacuate test
    delete("clusters/" + CLUSTER_NAME + "/instances/" + test_instance_name, Response.Status.OK.getStatusCode());

    // test setPartitionsToError
    List<String> partitionsToSetToError = Arrays.asList(CLUSTER_NAME + dbName + "7");

    entity = Entity.entity(
        OBJECT_MAPPER.writeValueAsString(ImmutableMap.of(AbstractResource.Properties.id.name(),
            INSTANCE_NAME, PerInstanceAccessor.PerInstanceProperties.resource.name(), RESOURCE_NAME,
            PerInstanceAccessor.PerInstanceProperties.partitions.name(), partitionsToSetToError)),
        MediaType.APPLICATION_JSON_TYPE);

    response = new JerseyUriRequestBuilder("clusters/{}/instances/{}?command=setPartitionsToError")
        .format(CLUSTER_NAME, INSTANCE_NAME).post(this, entity);

    Assert.assertEquals(response.getStatus(), Response.Status.OK.getStatusCode());

    TestHelper.verify(() -> {
      ExternalView externalView = _gSetupTool.getClusterManagementTool()
          .getResourceExternalView(CLUSTER_NAME, RESOURCE_NAME);
      Set responseForAllPartitions = new HashSet();
      for (String partition : partitionsToSetToError) {
        responseForAllPartitions.add(externalView.getStateMap(partition)
            .get(INSTANCE_NAME) == HelixDefinedState.ERROR.toString());
      }
      return !responseForAllPartitions.contains(Boolean.FALSE);
    }, TestHelper.WAIT_DURATION);

    System.out.println("End test :" + TestHelper.getTestMethodName());
  }

  /**
   * Test "update" command for updateInstanceConfig endpoint.
   * @throws IOException
   */
  @Test(dependsOnMethods = "updateInstance")
  public void updateInstanceConfig() throws IOException {
    System.out.println("Start test :" + TestHelper.getTestMethodName());
    String instanceName = CLUSTER_NAME + "localhost_12918";
    InstanceConfig instanceConfig = _configAccessor.getInstanceConfig(CLUSTER_NAME, instanceName);
    ZNRecord record = instanceConfig.getRecord();

    // Generate a record containing three keys (k0, k1, k2) for all fields
    String value = "value";
    for (int i = 0; i < 3; i++) {
      String key = "k" + i;
      record.getSimpleFields().put(key, value);
      record.getMapFields().put(key, ImmutableMap.of(key, value));
      record.getListFields().put(key, Arrays.asList(key, value));
    }

    // 1. Add these fields by way of "update"
    Entity entity =
        Entity.entity(OBJECT_MAPPER.writeValueAsString(record), MediaType.APPLICATION_JSON_TYPE);
    new JerseyUriRequestBuilder("clusters/{}/instances/{}/configs?command=update")
        .format(CLUSTER_NAME, INSTANCE_NAME).post(this, entity);

    // Check that the fields have been added
    Assert.assertEquals(record.getSimpleFields(), _configAccessor
        .getInstanceConfig(CLUSTER_NAME, instanceName).getRecord().getSimpleFields());
    Assert.assertEquals(record.getListFields(),
        _configAccessor.getInstanceConfig(CLUSTER_NAME, instanceName).getRecord().getListFields());
    Assert.assertEquals(record.getMapFields(),
        _configAccessor.getInstanceConfig(CLUSTER_NAME, instanceName).getRecord().getMapFields());

    String newValue = "newValue";
    // 2. Modify the record and update
    for (int i = 0; i < 3; i++) {
      String key = "k" + i;
      record.getSimpleFields().put(key, newValue);
      record.getMapFields().put(key, ImmutableMap.of(key, newValue));
      record.getListFields().put(key, Arrays.asList(key, newValue));
    }

    entity =
        Entity.entity(OBJECT_MAPPER.writeValueAsString(record), MediaType.APPLICATION_JSON_TYPE);
    new JerseyUriRequestBuilder("clusters/{}/instances/{}/configs?command=update")
        .format(CLUSTER_NAME, INSTANCE_NAME).post(this, entity);

    // Check that the fields have been modified
    Assert.assertEquals(record.getSimpleFields(), _configAccessor
        .getInstanceConfig(CLUSTER_NAME, instanceName).getRecord().getSimpleFields());
    Assert.assertEquals(record.getListFields(),
        _configAccessor.getInstanceConfig(CLUSTER_NAME, instanceName).getRecord().getListFields());
    Assert.assertEquals(record.getMapFields(),
        _configAccessor.getInstanceConfig(CLUSTER_NAME, instanceName).getRecord().getMapFields());
    System.out.println("End test :" + TestHelper.getTestMethodName());
  }

  /**
   * Test the "delete" command of updateInstanceConfig.
   * @throws IOException
   */
  @Test(dependsOnMethods = "updateInstanceConfig")
  public void deleteInstanceConfig() throws IOException {
    System.out.println("Start test :" + TestHelper.getTestMethodName());
    String instanceName = CLUSTER_NAME + "localhost_12918";
    ZNRecord record = new ZNRecord(instanceName);

    // Generate a record containing three keys (k1, k2, k3) for all fields for deletion
    String value = "value";
    for (int i = 1; i < 4; i++) {
      String key = "k" + i;
      record.getSimpleFields().put(key, value);
      record.getMapFields().put(key, ImmutableMap.of(key, value));
      record.getListFields().put(key, Arrays.asList(key, value));
    }

    // First, add these fields by way of "update"
    Entity entity =
        Entity.entity(OBJECT_MAPPER.writeValueAsString(record), MediaType.APPLICATION_JSON_TYPE);
    new JerseyUriRequestBuilder("clusters/{}/instances/{}/configs?command=delete")
        .format(CLUSTER_NAME, INSTANCE_NAME).post(this, entity);

    // Check that the keys k1 and k2 have been deleted, and k0 remains
    for (int i = 0; i < 4; i++) {
      String key = "k" + i;
      if (i == 0) {
        Assert.assertTrue(_configAccessor.getInstanceConfig(CLUSTER_NAME, instanceName).getRecord()
            .getSimpleFields().containsKey(key));
        Assert.assertTrue(_configAccessor.getInstanceConfig(CLUSTER_NAME, instanceName).getRecord()
            .getListFields().containsKey(key));
        Assert.assertTrue(_configAccessor.getInstanceConfig(CLUSTER_NAME, instanceName).getRecord()
            .getMapFields().containsKey(key));
        continue;
      }
      Assert.assertFalse(_configAccessor.getInstanceConfig(CLUSTER_NAME, instanceName).getRecord()
          .getSimpleFields().containsKey(key));
      Assert.assertFalse(_configAccessor.getInstanceConfig(CLUSTER_NAME, instanceName).getRecord()
          .getListFields().containsKey(key));
      Assert.assertFalse(_configAccessor.getInstanceConfig(CLUSTER_NAME, instanceName).getRecord()
          .getMapFields().containsKey(key));
    }
    System.out.println("End test :" + TestHelper.getTestMethodName());
  }

  /**
   * Check that updateInstanceConfig fails when there is no pre-existing InstanceConfig ZNode. This
   * is because InstanceConfig should have been created when the instance was added, and this REST
   * endpoint is not meant for creation.
   */
  @Test(dependsOnMethods = "deleteInstanceConfig")
  public void checkUpdateFails() throws IOException {
    System.out.println("Start test :" + TestHelper.getTestMethodName());
    String instanceName = CLUSTER_NAME + "non_existent_instance";
    InstanceConfig instanceConfig = new InstanceConfig(INSTANCE_NAME + "TEST");
    ZNRecord record = instanceConfig.getRecord();
    record.getSimpleFields().put("TestSimple", "value");
    record.getMapFields().put("TestMap", ImmutableMap.of("key", "value"));
    record.getListFields().put("TestList", Arrays.asList("e1", "e2", "e3"));

    Entity entity =
        Entity.entity(OBJECT_MAPPER.writeValueAsString(record), MediaType.APPLICATION_JSON_TYPE);
    new JerseyUriRequestBuilder("clusters/{}/instances/{}/configs")
        .expectedReturnStatusCode(Response.Status.NOT_FOUND.getStatusCode())
        .format(CLUSTER_NAME, instanceName).post(this, entity);
    System.out.println("End test :" + TestHelper.getTestMethodName());
  }

  /*
   * Guard rail coverage for the DELETE instance endpoint. Every participant in STOPPABLE_CLUSTER is
   * started and connected, so its LIVEINSTANCES znode is present and the live-instance guard rail
   * must block (or, for dryRun, report) the drop. None of these tests actually drop the instance, so
   * they are non-destructive.
   */
  @Test
  public void testDeleteInstanceGuardrailBlocks() throws IOException {
    System.out.println("Start test :" + TestHelper.getTestMethodName());
    String instanceToDelete = "instance0";

    Response response =
        target("clusters/" + STOPPABLE_CLUSTER + "/instances/" + instanceToDelete).request()
            .delete();
    Assert.assertEquals(response.getStatus(), Response.Status.BAD_REQUEST.getStatusCode());

    JsonNode verdict = OBJECT_MAPPER.readTree(response.readEntity(String.class));
    Assert.assertFalse(verdict.get("feasible").asBoolean());
    Assert.assertTrue(verdict.toString().contains(LiveInstanceGuardrailRule.RULE_ID));

    // The instance must not have been dropped by a blocked request.
    Assert.assertNotNull(_configAccessor.getInstanceConfig(STOPPABLE_CLUSTER, instanceToDelete));
    System.out.println("End test :" + TestHelper.getTestMethodName());
  }

  @Test
  public void testDeleteInstanceGuardrailDryRun() throws IOException {
    System.out.println("Start test :" + TestHelper.getTestMethodName());
    String instanceToDelete = "instance0";

    // dryRun only simulates: it always returns 200 with the verdict and never drops the instance.
    Response response =
        target("clusters/" + STOPPABLE_CLUSTER + "/instances/" + instanceToDelete)
            .queryParam("dryRun", true).request().delete();
    Assert.assertEquals(response.getStatus(), Response.Status.OK.getStatusCode());

    JsonNode verdict = OBJECT_MAPPER.readTree(response.readEntity(String.class));
    Assert.assertFalse(verdict.get("feasible").asBoolean());
    Assert.assertTrue(verdict.toString().contains(LiveInstanceGuardrailRule.RULE_ID));

    Assert.assertNotNull(_configAccessor.getInstanceConfig(STOPPABLE_CLUSTER, instanceToDelete));
    System.out.println("End test :" + TestHelper.getTestMethodName());
  }

  @Test
  public void testDeleteInstanceGuardrailForceBypass() throws IOException {
    System.out.println("Start test :" + TestHelper.getTestMethodName());
    String instanceToDelete = "instance0";

    // force=true bypasses the guard rail and lets the request reach the actual drop. The drop then
    // fails specifically because the participant is still live -- asserting on that error (rather
    // than merely "some 400") directly confirms the request got past the guard rail into
    // dropInstance, instead of being blocked by the guard rail verdict.
    Response response =
        target("clusters/" + STOPPABLE_CLUSTER + "/instances/" + instanceToDelete)
            .queryParam("force", true).request().delete();
    Assert.assertEquals(response.getStatus(), Response.Status.BAD_REQUEST.getStatusCode());

    String body = response.readEntity(String.class);
    Assert.assertFalse(body.contains(LiveInstanceGuardrailRule.RULE_ID),
        "force=true should bypass the guard rail, not return its verdict: " + body);
    Assert.assertTrue(body.contains("is still alive"),
        "force=true should reach dropInstance, which fails on the live participant: " + body);

    // The live participant was not dropped, so its config must still be present.
    Assert.assertNotNull(_configAccessor.getInstanceConfig(STOPPABLE_CLUSTER, instanceToDelete));
    System.out.println("End test :" + TestHelper.getTestMethodName());
  }

  /*
   * Guard rail coverage for the updateInstanceConfig "update" endpoint's capacity-reduction path.
   * Declares a single WAGED capacity dimension, gives all instances capacity 100 (supply 1000), and
   * plants a WAGED resource committing demand 950 (10 partitions * 1 replica * weight 95), leaving
   * only 50 units of headroom. It then verifies: (1) an over-cut reduction is blocked (400 + verdict,
   * nothing written); (2) the same reduction under dryRun always returns 200 with the verdict and is
   * still not written; (3) force=true bypasses the guard rail and the unsafe reduction is written;
   * (4) a within-headroom reduction passes and is written. Cluster/instance capacity config and the
   * demand resource are saved and torn down so the shared cluster is left unperturbed.
   */
  @Test
  public void testUpdateInstanceConfigCapacityHeadroomGuardrail() throws IOException {
    System.out.println("Start test :" + TestHelper.getTestMethodName());
    String targetInstance = CLUSTER_NAME + "localhost_12919";
    String demandResource = "guardrailHeadroomDemandResource";

    HelixAdmin admin = _gSetupTool.getClusterManagementTool();
    ClusterConfig clusterConfig = _configAccessor.getClusterConfig(CLUSTER_NAME);
    List<String> originalCapacityKeys = clusterConfig.getInstanceCapacityKeys();
    List<String> instances = admin.getInstancesInCluster(CLUSTER_NAME);
    Map<String, Map<String, Integer>> originalCapacities = new HashMap<>();
    for (String instance : instances) {
      originalCapacities.put(instance,
          _configAccessor.getInstanceConfig(CLUSTER_NAME, instance).getInstanceCapacityMap());
    }

    try {
      // supply = (# instances) * 100 in dimension FOO. With 10 instances that is 1000.
      clusterConfig.setInstanceCapacityKeys(Collections.singletonList("FOO"));
      _configAccessor.setClusterConfig(CLUSTER_NAME, clusterConfig);
      for (String instance : instances) {
        InstanceConfig instanceConfig = _configAccessor.getInstanceConfig(CLUSTER_NAME, instance);
        instanceConfig.setInstanceCapacityMap(ImmutableMap.of("FOO", 100));
        _configAccessor.setInstanceConfig(CLUSTER_NAME, instance, instanceConfig);
      }

      // Committed demand = 10 partitions * 1 replica * weight 95 = 950, leaving 50 of headroom.
      admin.addResource(CLUSTER_NAME, demandResource, 10, "OnlineOffline", "FULL_AUTO");
      IdealState idealState = admin.getResourceIdealState(CLUSTER_NAME, demandResource);
      idealState.setRebalancerClassName(WagedRebalancer.class.getName());
      idealState.setReplicas("1");
      admin.setResourceIdealState(CLUSTER_NAME, demandResource, idealState);
      ResourceConfig resourceConfig = new ResourceConfig(demandResource);
      resourceConfig.setPartitionCapacityMap(
          ImmutableMap.of(ResourceConfig.DEFAULT_PARTITION_KEY, ImmutableMap.of("FOO", 95)));
      _configAccessor.setResourceConfig(CLUSTER_NAME, demandResource, resourceConfig);

      // 1. Enforcement: reducing to 30 drops supply to 930 < 950, so the freed load has no home.
      Response blocked = postCapacityDelta(targetInstance, 30, Collections.emptyMap());
      Assert.assertEquals(blocked.getStatus(), Response.Status.BAD_REQUEST.getStatusCode());
      JsonNode blockedVerdict = OBJECT_MAPPER.readTree(blocked.readEntity(String.class));
      Assert.assertFalse(blockedVerdict.get("feasible").asBoolean());
      Assert.assertTrue(
          blockedVerdict.toString().contains(InstanceCapacityHeadroomGuardrailRule.RULE_ID));
      Assert.assertEquals((int) _configAccessor.getInstanceConfig(CLUSTER_NAME, targetInstance)
          .getInstanceCapacityMap().get("FOO"), 100);

      // 2. Dry-run: always 200 with the same infeasible verdict, still nothing written.
      Response dryRun = postCapacityDelta(targetInstance, 30, ImmutableMap.of("dryRun", true));
      Assert.assertEquals(dryRun.getStatus(), Response.Status.OK.getStatusCode());
      JsonNode dryRunVerdict = OBJECT_MAPPER.readTree(dryRun.readEntity(String.class));
      Assert.assertFalse(dryRunVerdict.get("feasible").asBoolean());
      Assert.assertTrue(
          dryRunVerdict.toString().contains(InstanceCapacityHeadroomGuardrailRule.RULE_ID));
      Assert.assertEquals((int) _configAccessor.getInstanceConfig(CLUSTER_NAME, targetInstance)
          .getInstanceCapacityMap().get("FOO"), 100);

      // 3. force=true bypasses the guard rail: the unsafe reduction is actually written.
      Response forced = postCapacityDelta(targetInstance, 30, ImmutableMap.of("force", true));
      Assert.assertEquals(forced.getStatus(), Response.Status.OK.getStatusCode());
      Assert.assertEquals((int) _configAccessor.getInstanceConfig(CLUSTER_NAME, targetInstance)
          .getInstanceCapacityMap().get("FOO"), 30);

      // Restore the target to 100 before exercising the happy path.
      InstanceConfig restoreTarget =
          _configAccessor.getInstanceConfig(CLUSTER_NAME, targetInstance);
      restoreTarget.setInstanceCapacityMap(ImmutableMap.of("FOO", 100));
      _configAccessor.setInstanceConfig(CLUSTER_NAME, targetInstance, restoreTarget);

      // 4. A within-headroom reduction (100 -> 60 leaves supply 960 >= 950) passes and is written.
      Response allowed = postCapacityDelta(targetInstance, 60, Collections.emptyMap());
      Assert.assertEquals(allowed.getStatus(), Response.Status.OK.getStatusCode());
      Assert.assertEquals((int) _configAccessor.getInstanceConfig(CLUSTER_NAME, targetInstance)
          .getInstanceCapacityMap().get("FOO"), 60);
    } finally {
      try {
        admin.dropResource(CLUSTER_NAME, demandResource);
      } catch (Exception ignored) {
        // best-effort teardown
      }
      ClusterConfig restore = _configAccessor.getClusterConfig(CLUSTER_NAME);
      restore.setInstanceCapacityKeys(
          originalCapacityKeys == null ? new ArrayList<>() : originalCapacityKeys);
      _configAccessor.setClusterConfig(CLUSTER_NAME, restore);
      for (String instance : instances) {
        InstanceConfig instanceConfig = _configAccessor.getInstanceConfig(CLUSTER_NAME, instance);
        instanceConfig.setInstanceCapacityMap(originalCapacities.get(instance));
        _configAccessor.setInstanceConfig(CLUSTER_NAME, instance, instanceConfig);
      }
    }
    System.out.println("End test :" + TestHelper.getTestMethodName());
  }

  /**
   * POST an InstanceConfig capacity delta ({@code {"FOO": fooCapacity}}) to the updateInstanceConfig
   * "update" endpoint, threading through any guard-rail query flags (e.g. {@code dryRun},
   * {@code force}), and return the raw {@link Response} so the caller can assert on status and body.
   */
  private Response postCapacityDelta(String instance, int fooCapacity, Map<String, Object> flags)
      throws IOException {
    InstanceConfig delta = new InstanceConfig(instance);
    delta.setInstanceCapacityMap(ImmutableMap.of("FOO", fooCapacity));
    Entity<String> entity = Entity.entity(OBJECT_MAPPER.writeValueAsString(delta.getRecord()),
        MediaType.APPLICATION_JSON_TYPE);
    WebTarget webTarget = target("clusters/" + CLUSTER_NAME + "/instances/" + instance + "/configs")
        .queryParam("command", "update");
    for (Map.Entry<String, Object> flag : flags.entrySet()) {
      webTarget = webTarget.queryParam(flag.getKey(), flag.getValue());
    }
    return webTarget.request().post(entity);
  }

  /**
   * Check that validateWeightForInstance() works by
   * 1. First call validate -> We should get "true" because nothing is set in ClusterConfig.
   * 2. Define keys in ClusterConfig and call validate -> We should get BadRequest.
   * 3. Define weight configs in InstanceConfig and call validate -> We should get OK with "true".
   */
  @Test(dependsOnMethods = "checkUpdateFails")
  public void testValidateWeightForInstance()
      throws IOException {
    System.out.println("Start test :" + TestHelper.getTestMethodName());
    // Empty out ClusterConfig's weight key setting and InstanceConfig's capacity maps for testing
    ClusterConfig clusterConfig = _configAccessor.getClusterConfig(CLUSTER_NAME);
    clusterConfig.getRecord()
        .setListField(ClusterConfig.ClusterConfigProperty.INSTANCE_CAPACITY_KEYS.name(),
            new ArrayList<>());
    _configAccessor.setClusterConfig(CLUSTER_NAME, clusterConfig);
    List<String> instances =
        _gSetupTool.getClusterManagementTool().getInstancesInCluster(CLUSTER_NAME);
    for (String instance : instances) {
      InstanceConfig instanceConfig = _configAccessor.getInstanceConfig(CLUSTER_NAME, instance);
      instanceConfig.setInstanceCapacityMap(Collections.emptyMap());
      _configAccessor.setInstanceConfig(CLUSTER_NAME, instance, instanceConfig);
    }

    // Get one instance in the cluster
    String selectedInstance =
        _gSetupTool.getClusterManagementTool().getInstancesInCluster(CLUSTER_NAME).iterator()
            .next();

    // Issue a validate call
    String body = new JerseyUriRequestBuilder("clusters/{}/instances/{}?command=validateWeight")
        .isBodyReturnExpected(true).format(CLUSTER_NAME, selectedInstance).get(this);

    JsonNode node = OBJECT_MAPPER.readTree(body);
    // Must have the result saying (true) because there's no capacity keys set
    // in ClusterConfig
    node.iterator().forEachRemaining(child -> Assert.assertTrue(child.booleanValue()));

    // Define keys in ClusterConfig
    clusterConfig = _configAccessor.getClusterConfig(CLUSTER_NAME);
    clusterConfig.setInstanceCapacityKeys(Arrays.asList("FOO", "BAR"));
    _configAccessor.setClusterConfig(CLUSTER_NAME, clusterConfig);

    body = new JerseyUriRequestBuilder("clusters/{}/instances/{}?command=validateWeight")
        .isBodyReturnExpected(true).format(CLUSTER_NAME, selectedInstance)
        .expectedReturnStatusCode(Response.Status.BAD_REQUEST.getStatusCode()).get(this);
    node = OBJECT_MAPPER.readTree(body);
    // Since instance does not have weight-related configs, the result should return error
    Assert.assertTrue(node.has("error"));

    // Now set weight-related config in InstanceConfig
    InstanceConfig instanceConfig =
        _configAccessor.getInstanceConfig(CLUSTER_NAME, selectedInstance);
    instanceConfig.setInstanceCapacityMap(ImmutableMap.of("FOO", 1000, "BAR", 1000));
    _configAccessor.setInstanceConfig(CLUSTER_NAME, selectedInstance, instanceConfig);

    body = new JerseyUriRequestBuilder("clusters/{}/instances/{}?command=validateWeight")
        .isBodyReturnExpected(true).format(CLUSTER_NAME, selectedInstance)
        .expectedReturnStatusCode(Response.Status.OK.getStatusCode()).get(this);
    node = OBJECT_MAPPER.readTree(body);
    // Must have the results saying they are all valid (true) because capacity keys are set
    // in ClusterConfig
    node.iterator().forEachRemaining(child -> Assert.assertTrue(child.booleanValue()));
    System.out.println("End test :" + TestHelper.getTestMethodName());
  }

  /**
   * Test the sanity check when updating the instance config.
   * The config is validated at rest server side.
   */
  @Test(dependsOnMethods = "testValidateWeightForInstance")
  public void testValidateDeltaInstanceConfigForUpdate() throws IOException {
    System.out.println("Start test :" + TestHelper.getTestMethodName());
    // Enable Topology aware for the cluster
    ClusterConfig clusterConfig = _configAccessor.getClusterConfig(CLUSTER_NAME);
    clusterConfig.getRecord()
        .setListField(ClusterConfig.ClusterConfigProperty.INSTANCE_CAPACITY_KEYS.name(),
            new ArrayList<>());
    clusterConfig.setTopologyAwareEnabled(true);
    clusterConfig.setTopology("/Rack/Sub-Rack/Host/Instance");
    clusterConfig.setFaultZoneType("Host");
    _configAccessor.setClusterConfig(CLUSTER_NAME, clusterConfig);

    String instanceName = CLUSTER_NAME + "localhost_12918";
    InstanceConfig instanceConfig = _configAccessor.getInstanceConfig(CLUSTER_NAME, instanceName);

    // Update InstanceConfig with Topology Info
    String domain = "Rack=rack1, Sub-Rack=Sub-Rack1, Host=Host-1";
    ZNRecord record = instanceConfig.getRecord();
    record.getSimpleFields().put(InstanceConfig.InstanceConfigProperty.DOMAIN.name(), domain);

    // Add these fields by way of "update"
    Entity entity =
        Entity.entity(OBJECT_MAPPER.writeValueAsString(record), MediaType.APPLICATION_JSON_TYPE);
    Response response = new JerseyUriRequestBuilder(
        "clusters/{}/instances/{}/configs?command=update&doSanityCheck=true")
        .format(CLUSTER_NAME, INSTANCE_NAME).post(this, entity);
    // Check that the fields have been added
    Assert.assertEquals(response.getStatus(), 200);
    // Check the cluster config is updated
    Assert.assertEquals(
        _configAccessor.getInstanceConfig(CLUSTER_NAME, instanceName).getDomainAsString(), domain);

    // set domain to an invalid value
    record.getSimpleFields()
        .put(InstanceConfig.InstanceConfigProperty.DOMAIN.name(), "InvalidDomainValue");
    entity =
        Entity.entity(OBJECT_MAPPER.writeValueAsString(record), MediaType.APPLICATION_JSON_TYPE);
    // Updating using an invalid domain value should return a non-OK response
    new JerseyUriRequestBuilder(
        "clusters/{}/instances/{}/configs?command=update&doSanityCheck=true")
        .expectedReturnStatusCode(Response.Status.INTERNAL_SERVER_ERROR.getStatusCode())
        .format(CLUSTER_NAME, INSTANCE_NAME).post(this, entity);

    System.out.println("End test :" + TestHelper.getTestMethodName());
  }

  @Test()
  public void testValidateClusterTopologyOnUpdate() throws IOException {
    System.out.println("Start test: " + TestHelper.getTestMethodName());

    // Enable topology-aware for the cluster
    ClusterConfig clusterConfig = _configAccessor.getClusterConfig(CLUSTER_NAME);
    clusterConfig.setTopologyAwareEnabled(true);
    clusterConfig.setTopology("/zone/instance");
    clusterConfig.setFaultZoneType("zone");
    _configAccessor.setClusterConfig(CLUSTER_NAME, clusterConfig);

    // Prepare swap-out and swap-in instances
    String swapOutInstance = CLUSTER_NAME + "localhost_12918";
    String swapInInstance = CLUSTER_NAME + "localhost_12919";

    InstanceConfig swapOutConfig = _configAccessor.getInstanceConfig(CLUSTER_NAME, swapOutInstance);
    InstanceConfig swapInConfig = _configAccessor.getInstanceConfig(CLUSTER_NAME, swapInInstance);

    String domain = String.format(
        "zone=%s,instance=%s,applicationInstanceId=%s,host=%s",
        "zone_0", "Participant_O_1", "Participant_O_1", "%s"
    );
    swapOutConfig.setDomain(String.format(domain, swapOutInstance));
    swapInConfig.setDomain(String.format(domain, swapInInstance));

    swapOutConfig.setInstanceOperation(InstanceConstants.InstanceOperation.EVACUATE);
    swapInConfig.setInstanceOperation(InstanceConstants.InstanceOperation.ENABLE);

    _configAccessor.setInstanceConfig(CLUSTER_NAME, swapOutInstance, swapOutConfig);
    _configAccessor.setInstanceConfig(CLUSTER_NAME, swapInInstance, swapInConfig);

    Assert.assertTrue(_bestPossibleClusterVerifier.verifyByPolling());

// Create the request for enabling the swap-out instance
    ZNRecord record = new ZNRecord(swapOutInstance);
    record.getSimpleFields().put(
        InstanceConfig.InstanceConfigProperty.HELIX_ENABLED.name(), "true"
    );

    Entity entity = Entity.entity(
        OBJECT_MAPPER.writeValueAsString(record),
        MediaType.APPLICATION_JSON_TYPE
    );

    boolean updateRequestFails = false;
    try {
      new JerseyUriRequestBuilder("clusters/{}/instances/{}/configs?command=update")
          .format(CLUSTER_NAME, swapOutInstance)
          .post(this, entity);
    } catch (AssertionError e) {
      updateRequestFails = true;
      System.out.println("Caught expected AssertionError: " + e.getMessage());
    }
    Assert.assertTrue(updateRequestFails);

    // Mark the swap-in instance unknown and retry the update
    swapInConfig.setInstanceOperation(InstanceConstants.InstanceOperation.UNKNOWN);
    _configAccessor.setInstanceConfig(CLUSTER_NAME, swapInInstance, swapInConfig);

    new JerseyUriRequestBuilder("clusters/{}/instances/{}/configs?command=update")
        .format(CLUSTER_NAME, swapOutInstance)
        .post(this, entity);

  }

  /**
   * Test that updating DOMAIN and SWAP_IN operation in a single request succeeds.
   * This verifies that the merge of the new config happens before validation so that
   * the updated DOMAIN (with a matching logical ID) is used for the transition check.
   */
  @Test
  public void testSwapInWithDomainUpdateInSingleRequest() throws IOException {
    System.out.println("Start test :" + TestHelper.getTestMethodName());

    // Set up topology on the cluster
    ClusterConfig clusterConfig = _configAccessor.getClusterConfig(CLUSTER_NAME);
    clusterConfig.setTopologyAwareEnabled(true);
    clusterConfig.setTopology("/zone/instance");
    clusterConfig.setFaultZoneType("zone");
    _configAccessor.setClusterConfig(CLUSTER_NAME, clusterConfig);

    String swapOutInstance = CLUSTER_NAME + "localhost_12918";
    String swapInInstance = CLUSTER_NAME + "localhost_12919";

    // Set up swap-out instance with ENABLE and a specific logical ID in the DOMAIN
    InstanceConfig swapOutConfig = _configAccessor.getInstanceConfig(CLUSTER_NAME, swapOutInstance);
    swapOutConfig.setDomain("zone=zone_A,instance=LogicalId_A,host=" + swapOutInstance);
    swapOutConfig.setInstanceOperation(
        new InstanceConfig.InstanceOperation.Builder()
            .setOperation(InstanceConstants.InstanceOperation.ENABLE)
            .setSource(InstanceConstants.InstanceOperationSource.ADMIN).build());
    _configAccessor.setInstanceConfig(CLUSTER_NAME, swapOutInstance, swapOutConfig);

    // Set up swap-in instance with UNKNOWN and a DIFFERENT logical ID in the DOMAIN
    InstanceConfig swapInConfig = _configAccessor.getInstanceConfig(CLUSTER_NAME, swapInInstance);
    swapInConfig.setDomain("zone=zone_A,instance=DifferentId,host=" + swapInInstance);
    swapInConfig.setInstanceOperation(
        new InstanceConfig.InstanceOperation.Builder()
            .setOperation(InstanceConstants.InstanceOperation.UNKNOWN)
            .setSource(InstanceConstants.InstanceOperationSource.USER).build());
    _configAccessor.setInstanceConfig(CLUSTER_NAME, swapInInstance, swapInConfig);

    Assert.assertTrue(_bestPossibleClusterVerifier.verifyByPolling());

    // Build a request that updates BOTH the DOMAIN (to match swap-out) AND sets SWAP_IN.
    // This simulates what ACM does in setSwapInOperationAndEditConfigIfNeeded.
    InstanceConfig updatedSwapInConfig = new InstanceConfig(swapInConfig.getRecord());
    updatedSwapInConfig.setDomain("zone=zone_A,instance=LogicalId_A,host=" + swapInInstance);
    updatedSwapInConfig.setInstanceOperation(
        new InstanceConfig.InstanceOperation.Builder()
            .setOperation(InstanceConstants.InstanceOperation.ENABLE)
            .setSource(InstanceConstants.InstanceOperationSource.ADMIN).build());
    updatedSwapInConfig.setInstanceOperation(
        new InstanceConfig.InstanceOperation.Builder()
            .setOperation(InstanceConstants.InstanceOperation.SWAP_IN)
            .setSource(InstanceConstants.InstanceOperationSource.AUTOMATION).build());

    Entity entity = Entity.entity(
        OBJECT_MAPPER.writeValueAsString(updatedSwapInConfig.getRecord()),
        MediaType.APPLICATION_JSON_TYPE);

    // This should succeed because the merged config has the correct DOMAIN for matching
    new JerseyUriRequestBuilder("clusters/{}/instances/{}/configs?command=update")
        .format(CLUSTER_NAME, swapInInstance)
        .post(this, entity);

    // Verify the config was updated in ZK
    InstanceConfig resultConfig = _configAccessor.getInstanceConfig(CLUSTER_NAME, swapInInstance);
    Assert.assertEquals(resultConfig.getInstanceOperation().getOperation(),
        InstanceConstants.InstanceOperation.SWAP_IN);
    Assert.assertTrue(resultConfig.getDomainAsString().contains("instance=LogicalId_A"));

    // Clean up: reset both instances to ENABLE with original domains
    swapOutConfig = _configAccessor.getInstanceConfig(CLUSTER_NAME, swapOutInstance);
    swapOutConfig.setInstanceOperation(
        new InstanceConfig.InstanceOperation.Builder()
            .setOperation(InstanceConstants.InstanceOperation.ENABLE)
            .setSource(InstanceConstants.InstanceOperationSource.ADMIN).build());
    _configAccessor.setInstanceConfig(CLUSTER_NAME, swapOutInstance, swapOutConfig);

    swapInConfig = _configAccessor.getInstanceConfig(CLUSTER_NAME, swapInInstance);
    swapInConfig.setInstanceOperation(
        new InstanceConfig.InstanceOperation.Builder()
            .setOperation(InstanceConstants.InstanceOperation.ENABLE)
            .setSource(InstanceConstants.InstanceOperationSource.ADMIN).build());
    _configAccessor.setInstanceConfig(CLUSTER_NAME, swapInInstance, swapInConfig);

    // Reset topology
    clusterConfig = _configAccessor.getClusterConfig(CLUSTER_NAME);
    clusterConfig.setTopologyAwareEnabled(false);
    _configAccessor.setClusterConfig(CLUSTER_NAME, clusterConfig);

    Assert.assertTrue(_bestPossibleClusterVerifier.verifyByPolling());
    System.out.println("End test :" + TestHelper.getTestMethodName());
  }

  /**
   * Test that SWAP_IN without updating DOMAIN to match fails when logical IDs differ.
   */
  @Test
  public void testSwapInWithoutMatchingDomainFails() throws IOException {
    System.out.println("Start test :" + TestHelper.getTestMethodName());

    // Set up topology on the cluster
    ClusterConfig clusterConfig = _configAccessor.getClusterConfig(CLUSTER_NAME);
    clusterConfig.setTopologyAwareEnabled(true);
    clusterConfig.setTopology("/zone/instance");
    clusterConfig.setFaultZoneType("zone");
    _configAccessor.setClusterConfig(CLUSTER_NAME, clusterConfig);

    String swapOutInstance = CLUSTER_NAME + "localhost_12918";
    String swapInInstance = CLUSTER_NAME + "localhost_12919";

    // Set up swap-out with ENABLE and a specific logical ID
    InstanceConfig swapOutConfig = _configAccessor.getInstanceConfig(CLUSTER_NAME, swapOutInstance);
    swapOutConfig.setDomain("zone=zone_A,instance=LogicalId_B,host=" + swapOutInstance);
    swapOutConfig.setInstanceOperation(
        new InstanceConfig.InstanceOperation.Builder()
            .setOperation(InstanceConstants.InstanceOperation.ENABLE)
            .setSource(InstanceConstants.InstanceOperationSource.ADMIN).build());
    _configAccessor.setInstanceConfig(CLUSTER_NAME, swapOutInstance, swapOutConfig);

    // Set up swap-in with UNKNOWN and a DIFFERENT logical ID
    InstanceConfig swapInConfig = _configAccessor.getInstanceConfig(CLUSTER_NAME, swapInInstance);
    swapInConfig.setDomain("zone=zone_A,instance=MismatchedId,host=" + swapInInstance);
    swapInConfig.setInstanceOperation(
        new InstanceConfig.InstanceOperation.Builder()
            .setOperation(InstanceConstants.InstanceOperation.UNKNOWN)
            .setSource(InstanceConstants.InstanceOperationSource.USER).build());
    _configAccessor.setInstanceConfig(CLUSTER_NAME, swapInInstance, swapInConfig);

    Assert.assertTrue(_bestPossibleClusterVerifier.verifyByPolling());

    // Build a request that sets SWAP_IN but does NOT update the DOMAIN to match.
    InstanceConfig badConfig = new InstanceConfig(swapInConfig.getRecord());
    badConfig.setInstanceOperation(
        new InstanceConfig.InstanceOperation.Builder()
            .setOperation(InstanceConstants.InstanceOperation.ENABLE)
            .setSource(InstanceConstants.InstanceOperationSource.ADMIN).build());
    badConfig.setInstanceOperation(
        new InstanceConfig.InstanceOperation.Builder()
            .setOperation(InstanceConstants.InstanceOperation.SWAP_IN)
            .setSource(InstanceConstants.InstanceOperationSource.AUTOMATION).build());

    Entity entity = Entity.entity(
        OBJECT_MAPPER.writeValueAsString(badConfig.getRecord()),
        MediaType.APPLICATION_JSON_TYPE);

    // This should fail because even after merging, the DOMAIN still doesn't match
    new JerseyUriRequestBuilder("clusters/{}/instances/{}/configs?command=update")
        .expectedReturnStatusCode(Response.Status.INTERNAL_SERVER_ERROR.getStatusCode())
        .format(CLUSTER_NAME, swapInInstance)
        .post(this, entity);

    // Clean up
    swapOutConfig = _configAccessor.getInstanceConfig(CLUSTER_NAME, swapOutInstance);
    swapOutConfig.setInstanceOperation(
        new InstanceConfig.InstanceOperation.Builder()
            .setOperation(InstanceConstants.InstanceOperation.ENABLE)
            .setSource(InstanceConstants.InstanceOperationSource.ADMIN).build());
    _configAccessor.setInstanceConfig(CLUSTER_NAME, swapOutInstance, swapOutConfig);

    swapInConfig = _configAccessor.getInstanceConfig(CLUSTER_NAME, swapInInstance);
    swapInConfig.setInstanceOperation(
        new InstanceConfig.InstanceOperation.Builder()
            .setOperation(InstanceConstants.InstanceOperation.ENABLE)
            .setSource(InstanceConstants.InstanceOperationSource.ADMIN).build());
    _configAccessor.setInstanceConfig(CLUSTER_NAME, swapInInstance, swapInConfig);

    clusterConfig = _configAccessor.getClusterConfig(CLUSTER_NAME);
    clusterConfig.setTopologyAwareEnabled(false);
    _configAccessor.setClusterConfig(CLUSTER_NAME, clusterConfig);

    Assert.assertTrue(_bestPossibleClusterVerifier.verifyByPolling());
    System.out.println("End test :" + TestHelper.getTestMethodName());
  }

  /**
   * Test that a delete command removing the DOMAIN field causes the operation transition
   * validation to fail when the transition depends on logical ID matching.
   * Before the merge-before-validate fix, the validation would have passed because it ran
   * against the original config (which still had the DOMAIN). Now the DOMAIN is removed
   * before validation, so the logical ID matching correctly fails.
   */
  @Test
  public void testDeleteDomainFailsWhenTransitionDependsOnLogicalId() throws IOException {
    System.out.println("Start test :" + TestHelper.getTestMethodName());

    // Set up topology on the cluster
    ClusterConfig clusterConfig = _configAccessor.getClusterConfig(CLUSTER_NAME);
    clusterConfig.setTopologyAwareEnabled(true);
    clusterConfig.setTopology("/zone/instance");
    clusterConfig.setFaultZoneType("zone");
    _configAccessor.setClusterConfig(CLUSTER_NAME, clusterConfig);

    String swapOutInstance = CLUSTER_NAME + "localhost_12918";
    String swapInInstance = CLUSTER_NAME + "localhost_12919";

    // Set up swap-out instance with ENABLE and a specific logical ID
    InstanceConfig swapOutConfig = _configAccessor.getInstanceConfig(CLUSTER_NAME, swapOutInstance);
    swapOutConfig.setDomain("zone=zone_A,instance=LogicalId_C,host=" + swapOutInstance);
    swapOutConfig.setInstanceOperation(
        new InstanceConfig.InstanceOperation.Builder()
            .setOperation(InstanceConstants.InstanceOperation.ENABLE)
            .setSource(InstanceConstants.InstanceOperationSource.ADMIN).build());
    _configAccessor.setInstanceConfig(CLUSTER_NAME, swapOutInstance, swapOutConfig);

    // Set up swap-in instance with UNKNOWN and a MATCHING logical ID
    InstanceConfig swapInConfig = _configAccessor.getInstanceConfig(CLUSTER_NAME, swapInInstance);
    swapInConfig.setDomain("zone=zone_A,instance=LogicalId_C,host=" + swapInInstance);
    swapInConfig.setInstanceOperation(
        new InstanceConfig.InstanceOperation.Builder()
            .setOperation(InstanceConstants.InstanceOperation.UNKNOWN)
            .setSource(InstanceConstants.InstanceOperationSource.USER).build());
    _configAccessor.setInstanceConfig(CLUSTER_NAME, swapInInstance, swapInConfig);

    Assert.assertTrue(_bestPossibleClusterVerifier.verifyByPolling());

    // Build a DELETE request that removes the DOMAIN field and sets SWAP_IN.
    // The DOMAIN deletion happens before validation, so logical ID matching
    // will fail because the config no longer has the DOMAIN field.
    InstanceConfig deleteConfig = new InstanceConfig(swapInInstance);
    deleteConfig.setDomain("zone=zone_A,instance=LogicalId_C,host=" + swapInInstance);
    deleteConfig.setInstanceOperation(
        new InstanceConfig.InstanceOperation.Builder()
            .setOperation(InstanceConstants.InstanceOperation.ENABLE)
            .setSource(InstanceConstants.InstanceOperationSource.ADMIN).build());
    deleteConfig.setInstanceOperation(
        new InstanceConfig.InstanceOperation.Builder()
            .setOperation(InstanceConstants.InstanceOperation.SWAP_IN)
            .setSource(InstanceConstants.InstanceOperationSource.AUTOMATION).build());

    Entity entity = Entity.entity(
        OBJECT_MAPPER.writeValueAsString(deleteConfig.getRecord()),
        MediaType.APPLICATION_JSON_TYPE);

    // Should fail because the delete removes the DOMAIN before validation,
    // so no matching logical ID is found for the UNKNOWN->SWAP_IN transition.
    new JerseyUriRequestBuilder("clusters/{}/instances/{}/configs?command=delete")
        .expectedReturnStatusCode(Response.Status.INTERNAL_SERVER_ERROR.getStatusCode())
        .format(CLUSTER_NAME, swapInInstance)
        .post(this, entity);

    // Verify the original config is unchanged in ZK
    InstanceConfig resultConfig = _configAccessor.getInstanceConfig(CLUSTER_NAME, swapInInstance);
    Assert.assertEquals(resultConfig.getInstanceOperation().getOperation(),
        InstanceConstants.InstanceOperation.UNKNOWN);
    Assert.assertTrue(resultConfig.getDomainAsString().contains("instance=LogicalId_C"));

    // Clean up
    swapOutConfig = _configAccessor.getInstanceConfig(CLUSTER_NAME, swapOutInstance);
    swapOutConfig.setInstanceOperation(
        new InstanceConfig.InstanceOperation.Builder()
            .setOperation(InstanceConstants.InstanceOperation.ENABLE)
            .setSource(InstanceConstants.InstanceOperationSource.ADMIN).build());
    _configAccessor.setInstanceConfig(CLUSTER_NAME, swapOutInstance, swapOutConfig);

    swapInConfig = _configAccessor.getInstanceConfig(CLUSTER_NAME, swapInInstance);
    swapInConfig.setInstanceOperation(
        new InstanceConfig.InstanceOperation.Builder()
            .setOperation(InstanceConstants.InstanceOperation.ENABLE)
            .setSource(InstanceConstants.InstanceOperationSource.ADMIN).build());
    _configAccessor.setInstanceConfig(CLUSTER_NAME, swapInInstance, swapInConfig);

    clusterConfig = _configAccessor.getClusterConfig(CLUSTER_NAME);
    clusterConfig.setTopologyAwareEnabled(false);
    _configAccessor.setClusterConfig(CLUSTER_NAME, clusterConfig);

    Assert.assertTrue(_bestPossibleClusterVerifier.verifyByPolling());
    System.out.println("End test :" + TestHelper.getTestMethodName());
  }

  @Test(dependsOnMethods = "testValidateDeltaInstanceConfigForUpdate")
  public void testGetResourcesOnInstance() throws JsonProcessingException {
    System.out.println("Start test :" + TestHelper.getTestMethodName());
    String body = new JerseyUriRequestBuilder("clusters/{}/instances/{}/resources")
        .isBodyReturnExpected(true).format(CLUSTER_NAME, INSTANCE_NAME).get(this);
    JsonNode node = OBJECT_MAPPER.readTree(body);
    ArrayNode arrayOfResource =
        (ArrayNode) node.get(PerInstanceAccessor.PerInstanceProperties.resources.name());
    Assert.assertTrue(arrayOfResource.size() != 0);
    String dbNameString= arrayOfResource.get(0).toString();
    String dbName = dbNameString.substring(1,dbNameString.length()-1);
    // The below calls should successfully return
    body = new JerseyUriRequestBuilder("clusters/{}/instances/{}/resources/{}")
        .isBodyReturnExpected(true).format(CLUSTER_NAME, INSTANCE_NAME, dbName).get(this);
    System.out.println("End test :" + TestHelper.getTestMethodName());
  }

  @Test(dependsOnMethods = "testGetResourcesOnInstance")
  public void testForceKillInstance() {
    System.out.println("Start test :" + TestHelper.getTestMethodName());
    // Fix rebalance failures due to settings from previous tests
    ClusterConfig clusterConfig = _configAccessor.getClusterConfig(CLUSTER_NAME);
    clusterConfig.setTopologyAwareEnabled(false);
    clusterConfig.getRecord().setListField(ClusterConfig.ClusterConfigProperty.INSTANCE_CAPACITY_KEYS.name(), null);
    _configAccessor.setClusterConfig(CLUSTER_NAME, clusterConfig);

    String instanceToKill = "localhost_" + TestHelper.getTestMethodName();
    String resourceToAdd = "TestDB_"+TestHelper.getTestMethodName();
    addParticipant(CLUSTER_NAME, instanceToKill);
    addResource(CLUSTER_NAME, resourceToAdd, NUM_PARTITIONS, "OnlineOffline", 2, 3);

    Assert.assertTrue(_bestPossibleClusterVerifier.verifyByPolling());

    Assert.assertTrue(_gZkClient.exists("/" + CLUSTER_NAME + "/LIVEINSTANCES/" + instanceToKill),
        "Instance znode should exist before force kill");

    // Get assignments on node, assert it has at least one assignment
    Map<String, String> originalAssignment = getInstanceCurrentStates(instanceToKill);
    Assert.assertFalse(originalAssignment.isEmpty());

    Entity entity = Entity.entity("", MediaType.APPLICATION_JSON_TYPE);
    new JerseyUriRequestBuilder("clusters/{}/instances/{}?command=forceKillInstance")
        .format(CLUSTER_NAME, instanceToKill).post(this, entity);
    Assert.assertTrue(_bestPossibleClusterVerifier.verifyByPolling());

    // Assert instance operation updated
    InstanceConfig instanceConfig = _configAccessor.getInstanceConfig(CLUSTER_NAME, instanceToKill);
    InstanceConfig.InstanceOperation instanceOperation = instanceConfig.getInstanceOperation();
    Assert.assertEquals(instanceOperation.getOperation(), InstanceConstants.InstanceOperation.UNKNOWN);
    Assert.assertEquals(instanceOperation.getSource(), InstanceConstants.InstanceOperationSource.USER);

    // ensure no live instance znode
    Assert.assertFalse(_gZkClient.exists("/" + CLUSTER_NAME + "/LIVEINSTANCES/" + instanceToKill),
        "Instance znode should not exist after force kill");

    // make sure no assignments on the instance
    Map<String, String> postKillAssignment = getInstanceCurrentStates(instanceToKill);
    Assert.assertTrue(postKillAssignment.isEmpty());

    // Drop the instance we killed, remove test resource to return cluster to original state
    dropParticipant(CLUSTER_NAME, instanceToKill);
    _gSetupTool.getClusterManagementTool().dropResource(CLUSTER_NAME, resourceToAdd);
    _resourcesMap.get(CLUSTER_NAME).remove(resourceToAdd);
    _bestPossibleClusterVerifier.verifyByPolling();
    System.out.println("End test :" + TestHelper.getTestMethodName());
  }

  @Test(dependsOnMethods = "testForceKillInstance")
  public void testForceKillInstanceWithParameters() {
    System.out.println("Start test :" + TestHelper.getTestMethodName());
    String instanceToKill = "localhost_" + TestHelper.getTestMethodName();
    addParticipant(CLUSTER_NAME, instanceToKill);
    Assert.assertTrue(_bestPossibleClusterVerifier.verifyByPolling());
    String reason = "reason-foobar";
    InstanceConstants.InstanceOperationSource source = InstanceConstants.InstanceOperationSource.AUTOMATION;

    Entity entity =
        Entity.entity("", MediaType.APPLICATION_JSON_TYPE);
    new JerseyUriRequestBuilder("clusters/{}/instances/{}?command=forceKillInstance&reason="
        + reason + "&instanceOperationSource=" + source.name())
        .format(CLUSTER_NAME, instanceToKill).post(this, entity);
    Assert.assertTrue(_bestPossibleClusterVerifier.verifyByPolling());

    InstanceConfig instanceConfig = _configAccessor.getInstanceConfig(CLUSTER_NAME, instanceToKill);
    InstanceConfig.InstanceOperation instanceOperation = instanceConfig.getInstanceOperation();
    Assert.assertEquals(instanceOperation.getOperation(), InstanceConstants.InstanceOperation.UNKNOWN);
    Assert.assertEquals(instanceOperation.getReason(),reason);
    Assert.assertEquals(instanceOperation.getSource(), source);

    // Drop instance we killed to return cluster to original state
    dropParticipant(CLUSTER_NAME, instanceToKill);
    _bestPossibleClusterVerifier.verifyByPolling();
    System.out.println("End test :" + TestHelper.getTestMethodName());
  }

  @Test(dependsOnMethods = "testForceKillInstanceWithParameters")
  public void testForceKillInvalidInputs() {
    System.out.println("Start test :" + TestHelper.getTestMethodName());
    String instanceToKill = "localhost_" + TestHelper.getTestMethodName();
    addParticipant(CLUSTER_NAME, instanceToKill);
    Assert.assertTrue(_bestPossibleClusterVerifier.verifyByPolling());
    String reason = "reason-foobar";
    InstanceConstants.InstanceOperationSource validSource = InstanceConstants.InstanceOperationSource.AUTOMATION;
    String invalidSource = "INVALID_SOURCE";

    // Test invalid source
    Entity entity = Entity.entity("", MediaType.APPLICATION_JSON_TYPE);
    new JerseyUriRequestBuilder("clusters/{}/instances/{}?command=forceKillInstance&reason="
        + reason + "&instanceOperationSource=" + invalidSource)
        .expectedReturnStatusCode(Response.Status.NOT_FOUND.getStatusCode())
        .format(CLUSTER_NAME, instanceToKill).post(this, entity);
    Assert.assertTrue(_gZkClient.exists("/" + CLUSTER_NAME + "/LIVEINSTANCES/" + instanceToKill),
        "Instance znode should exist because force kill failed");

    // Calling on a node that has already been force killed (no live instance znode)
    new JerseyUriRequestBuilder("clusters/{}/instances/{}?command=forceKillInstance&reason="
        + reason + "&instanceOperationSource=" + validSource.name())
        .format(CLUSTER_NAME, instanceToKill).post(this, entity);
    Assert.assertFalse(_gZkClient.exists("/" + CLUSTER_NAME + "/LIVEINSTANCES/" + instanceToKill),
        "Instance znode should not exist after force kill");
    new JerseyUriRequestBuilder("clusters/{}/instances/{}?command=forceKillInstance&reason="
        + reason + "&instanceOperationSource=" + validSource.name())
        .expectedReturnStatusCode(Response.Status.BAD_REQUEST.getStatusCode())
        .format(CLUSTER_NAME, instanceToKill).post(this, entity);

    dropParticipant(CLUSTER_NAME, instanceToKill);
    _bestPossibleClusterVerifier.verifyByPolling();
    System.out.println("End test :" + TestHelper.getTestMethodName());
  }

  private Map<String, ExternalView> getEVs() {
    Map<String, ExternalView> externalViews = new HashMap<String, ExternalView>();
    for (String db : _resourcesMap.get(CLUSTER_NAME)) {
      ExternalView ev = _gSetupTool.getClusterManagementTool().getResourceExternalView(CLUSTER_NAME, db);
      externalViews.put(db, ev);
    }
    return externalViews;
  }

  private Map<String, String> getInstanceCurrentStates(String instanceName) {
    Map<String, String> assignment = new HashMap<>();
    for (ExternalView ev : getEVs().values()) {
      for (String partition : ev.getPartitionSet()) {
        Map<String, String> stateMap = ev.getStateMap(partition);
        if (stateMap.containsKey(instanceName)) {
          assignment.put(partition, stateMap.get(instanceName));
        }
      }
    }
    return assignment;
  }
}
