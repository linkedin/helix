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
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import javax.ws.rs.client.Entity;
import javax.ws.rs.client.WebTarget;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.JsonNode;
import com.google.common.collect.ImmutableMap;
import org.apache.helix.AccessOption;
import org.apache.helix.HelixDataAccessor;
import org.apache.helix.HelixManager;
import org.apache.helix.HelixManagerFactory;
import org.apache.helix.InstanceType;
import org.apache.helix.PropertyPathBuilder;
import org.apache.helix.TestHelper;
import org.apache.helix.controller.rebalancer.SemiAutoRebalancer;
import org.apache.helix.controller.rebalancer.waged.WagedRebalancer;
import org.apache.helix.guardrail.rules.CapacityKeyConsistencyGuardrailRule;
import org.apache.helix.guardrail.rules.IdealStateRebalanceFeasibilityGuardrailRule;
import org.apache.helix.guardrail.rules.MinActiveReplicasConsistencyGuardrailRule;
import org.apache.helix.guardrail.rules.PartitionWeightCapacityGuardrailRule;
import org.apache.helix.guardrail.rules.ResourceInUseGuardrailRule;
import org.apache.helix.guardrail.rules.StateModelDefExistenceGuardrailRule;
import org.apache.helix.guardrail.rules.UserDefinedRebalancerClassGuardrailRule;
import org.apache.helix.integration.manager.ClusterControllerManager;
import org.apache.helix.integration.manager.MockParticipantManager;
import org.apache.helix.model.ClusterConfig;
import org.apache.helix.model.CustomizedView;
import org.apache.helix.model.ExternalView;
import org.apache.helix.model.IdealState;
import org.apache.helix.model.InstanceConfig;
import org.apache.helix.model.ResourceConfig;
import org.apache.helix.model.builder.FullAutoModeISBuilder;
import org.apache.helix.rest.server.resources.helix.ResourceAccessor;
import org.apache.helix.rest.server.util.JerseyUriRequestBuilder;
import org.apache.helix.tools.ClusterVerifiers.BestPossibleExternalViewVerifier;
import org.apache.helix.zookeeper.datamodel.ZNRecord;
import org.testng.Assert;
import org.testng.annotations.Test;

public class TestResourceAccessor extends AbstractTestClass {
  private final static String CLUSTER_NAME = "TestCluster_0";
  private static final Map<String, String> SUMMARIES_QUERY =
      Collections.singletonMap("command", "getResourceSummaries");
  private final static String RESOURCE_NAME = CLUSTER_NAME + "_db_0";
  private final static String ANY_INSTANCE = "ANY_LIVEINSTANCE";
  private final static String CUSTOMIZED_STATE_TYPE = "Customized_state_type_0";

  @Test
  public void testGetResources() throws IOException {
    System.out.println("Start test :" + TestHelper.getTestMethodName());

    String body = get("clusters/" + CLUSTER_NAME + "/resources", null,
        Response.Status.OK.getStatusCode(), true);

    JsonNode node = OBJECT_MAPPER.readTree(body);
    String idealStates =
        node.get(ResourceAccessor.ResourceProperties.idealStates.name()).toString();
    Assert.assertNotNull(idealStates);

    Set<String> resources = OBJECT_MAPPER.readValue(idealStates,
        OBJECT_MAPPER.getTypeFactory().constructCollectionType(Set.class, String.class));
    Assert.assertTrue(resources.size() == _resourcesMap.get("TestCluster_0").size()
        && resources.containsAll(_resourcesMap.get("TestCluster_0"))
        && _resourcesMap.get("TestCluster_0").containsAll(resources),
        "Sets are not equal. Resources from response: " + resources + " vs clusters actually: " + _resourcesMap.get("TestCluster_0"));
    System.out.println("End test :" + TestHelper.getTestMethodName());
  }

  @Test
  public void testGetResourceSummaries() throws IOException {
    System.out.println("Start test :" + TestHelper.getTestMethodName());

    String cluster = "TestResourceSummariesCluster";
    _gSetupTool.addCluster(cluster, true);
    _clusters.add(cluster);
    _gSetupTool.addResourceToCluster(cluster, "rsDb1", 4, "OnlineOffline", "SEMI_AUTO");
    _gSetupTool.addResourceToCluster(cluster, "rsDb0", 8, "MasterSlave", "FULL_AUTO");
    // A resource that kept the WAGED class after leaving FULL_AUTO is not WAGED.
    _gSetupTool.addResourceToCluster(cluster, "rsDb2", 2, "OnlineOffline", "SEMI_AUTO");
    IdealState retained =
        _gSetupTool.getClusterManagementTool().getResourceIdealState(cluster, "rsDb2");
    retained.setRebalancerClassName(WagedRebalancer.class.getName());
    _gSetupTool.getClusterManagementTool().setResourceIdealState(cluster, "rsDb2", retained);
    // An existing resource named "summaries" must keep its own detail response.
    _gSetupTool.addResourceToCluster(cluster, "summaries", 2, "OnlineOffline", "SEMI_AUTO");
    IdealState idealState =
        _gSetupTool.getClusterManagementTool().getResourceIdealState(cluster, "rsDb0");
    idealState.setRebalancerClassName(WagedRebalancer.class.getName());
    idealState.setReplicas("2");
    idealState.setInstanceGroupTag("tagA");
    idealState.enable(false);
    _gSetupTool.getClusterManagementTool().setResourceIdealState(cluster, "rsDb0", idealState);

    JsonNode summaries = OBJECT_MAPPER
        .readTree(get("clusters/" + cluster + "/resources", SUMMARIES_QUERY,
            Response.Status.OK.getStatusCode(), true))
        .get(ResourceAccessor.ResourceProperties.resource_summaries.name());

    List<String> listed = new ArrayList<>();
    summaries.fieldNames().forEachRemaining(listed::add);
    Assert.assertEquals(listed, Arrays.asList("rsDb0", "rsDb1", "rsDb2", "summaries"));

    JsonNode waged = summaries.get("rsDb0");
    Assert.assertEquals(waged.get("rebalanceMode").asText(), "FULL_AUTO");
    Assert.assertEquals(waged.get("stateModelDef").asText(), "MasterSlave");
    Assert.assertEquals(waged.get("numPartitions").asInt(), 8);
    Assert.assertEquals(waged.get("replicas").asText(), "2");
    Assert.assertEquals(waged.get("instanceGroupTag").asText(), "tagA");
    Assert.assertFalse(waged.get("enabled").asBoolean());
    Assert.assertTrue(waged.get("waged").asBoolean());
    Assert.assertFalse(waged.get("hasExternalView").asBoolean());

    JsonNode semiAuto = summaries.get("rsDb1");
    Assert.assertEquals(semiAuto.get("rebalanceMode").asText(), "SEMI_AUTO");
    Assert.assertTrue(semiAuto.get("enabled").asBoolean());
    Assert.assertFalse(semiAuto.get("waged").asBoolean());

    Assert.assertFalse(summaries.get("rsDb2").get("waged").asBoolean());

    JsonNode detail = OBJECT_MAPPER.readTree(get("clusters/" + cluster + "/resources/summaries",
        null, Response.Status.OK.getStatusCode(), true));
    Assert.assertEquals(
        detail.get(ResourceAccessor.ResourceProperties.idealState.name()).get("id").asText(),
        "summaries");
    Assert.assertNull(detail.get(ResourceAccessor.ResourceProperties.resource_summaries.name()));

    get("clusters/TestResourceSummariesClusterDoesNotExist/resources", SUMMARIES_QUERY,
        Response.Status.NOT_FOUND.getStatusCode(), false);

    System.out.println("End test :" + TestHelper.getTestMethodName());
  }

  @Test(dependsOnMethods = "testGetResources")
  public void testGetResource() throws IOException {
    System.out.println("Start test :" + TestHelper.getTestMethodName());
    String body = get("clusters/" + CLUSTER_NAME + "/resources/" + RESOURCE_NAME, null,
        Response.Status.OK.getStatusCode(), true);

    JsonNode node = OBJECT_MAPPER.readTree(body);
    String idealStateStr =
        node.get(ResourceAccessor.ResourceProperties.idealState.name()).toString();
    IdealState idealState = new IdealState(toZNRecord(idealStateStr));
    IdealState originIdealState =
        _gSetupTool.getClusterManagementTool().getResourceIdealState(CLUSTER_NAME, RESOURCE_NAME);
    Assert.assertEquals(idealState, originIdealState);
    System.out.println("End test :" + TestHelper.getTestMethodName());
  }

  @Test(dependsOnMethods = "testGetResource")
  public void testAddResources() throws IOException {
    System.out.println("Start test :" + TestHelper.getTestMethodName());
    String newResourceName = "newResource";
    IdealState idealState = new IdealState(newResourceName);
    idealState.getRecord().getSimpleFields().putAll(_gSetupTool.getClusterManagementTool()
        .getResourceIdealState(CLUSTER_NAME, RESOURCE_NAME).getRecord().getSimpleFields());

    // Add resource by IdealState
    Entity entity = Entity.entity(OBJECT_MAPPER.writeValueAsString(idealState.getRecord()),
        MediaType.APPLICATION_JSON_TYPE);
    put("clusters/" + CLUSTER_NAME + "/resources/" + newResourceName, null, entity,
        Response.Status.OK.getStatusCode());

    Assert.assertEquals(idealState, _gSetupTool.getClusterManagementTool()
        .getResourceIdealState(CLUSTER_NAME, newResourceName));

    // Add resource by query param
    entity = Entity.entity("", MediaType.APPLICATION_JSON_TYPE);

    put("clusters/" + CLUSTER_NAME + "/resources/" + newResourceName + "0", ImmutableMap
        .of("numPartitions", "4", "stateModelRef", "OnlineOffline", "rebalancerMode", "FULL_AUTO"),
        entity, Response.Status.OK.getStatusCode());

    IdealState queryIdealState = new FullAutoModeISBuilder(newResourceName + 0).setNumPartitions(4)
        .setStateModel("OnlineOffline").setRebalancerMode(IdealState.RebalanceMode.FULL_AUTO)
        .setRebalanceStrategy("DEFAULT").build();
    Assert.assertEquals(queryIdealState, _gSetupTool.getClusterManagementTool()
        .getResourceIdealState(CLUSTER_NAME, newResourceName + "0"));
    for (String resource : new String[]{newResourceName, newResourceName + "0"}) {
      JsonNode fields = OBJECT_MAPPER.readTree(get(
          "clusters/" + CLUSTER_NAME + "/resources/" + resource, null,
          Response.Status.OK.getStatusCode(), true)).get("idealState").get("simpleFields");
      Assert.assertTrue(fields.has("REBALANCE_MODE"));
      Assert.assertFalse(fields.has("IDEAL_STATE_MODE"));
    }
    System.out.println("End test :" + TestHelper.getTestMethodName());
  }

  @Test(dependsOnMethods = "testAddResources")
  public void testResourceConfig() throws IOException {
    System.out.println("Start test :" + TestHelper.getTestMethodName());

    String body = get("clusters/" + CLUSTER_NAME + "/resources/" + RESOURCE_NAME + "/configs", null,
        Response.Status.OK.getStatusCode(), true);
    ResourceConfig resourceConfig = new ResourceConfig(toZNRecord(body));
    Assert.assertEquals(resourceConfig,
        _configAccessor.getResourceConfig(CLUSTER_NAME, RESOURCE_NAME));
    System.out.println("End test :" + TestHelper.getTestMethodName());
  }

  @Test(dependsOnMethods = "testResourceConfig")
  public void testIdealState() throws IOException {
    System.out.println("Start test :" + TestHelper.getTestMethodName());

    String body = get("clusters/" + CLUSTER_NAME + "/resources/" + RESOURCE_NAME + "/idealState",
        null, Response.Status.OK.getStatusCode(), true);
    IdealState idealState = new IdealState(toZNRecord(body));
    Assert.assertEquals(idealState,
        _gSetupTool.getClusterManagementTool().getResourceIdealState(CLUSTER_NAME, RESOURCE_NAME));
    System.out.println("End test :" + TestHelper.getTestMethodName());
  }

  @Test(dependsOnMethods = "testIdealState")
  public void testExternalView() throws IOException {
    System.out.println("Start test :" + TestHelper.getTestMethodName());

    String body = get("clusters/" + CLUSTER_NAME + "/resources/" + RESOURCE_NAME + "/externalView",
        null, Response.Status.OK.getStatusCode(), true);
    ExternalView externalView = new ExternalView(toZNRecord(body));
    Assert.assertEquals(externalView, _gSetupTool.getClusterManagementTool()
        .getResourceExternalView(CLUSTER_NAME, RESOURCE_NAME));
    System.out.println("End test :" + TestHelper.getTestMethodName());
  }

  @Test(dependsOnMethods = "testExternalView")
  public void testCustomizedView() throws IOException {
    System.out.println("Start test :" + TestHelper.getTestMethodName());
    ZNRecord znRecord = new ZNRecord("test_customizedView");
    _baseAccessor
        .set(PropertyPathBuilder.customizedView(CLUSTER_NAME, CUSTOMIZED_STATE_TYPE, RESOURCE_NAME),
            znRecord, 1);
    String body =
        get("clusters/" + CLUSTER_NAME + "/resources/" + RESOURCE_NAME + "/" + CUSTOMIZED_STATE_TYPE
            + "/customizedView", null, Response.Status.OK.getStatusCode(), true);
    CustomizedView customizedView = new CustomizedView(toZNRecord(body));
    Assert.assertEquals(customizedView, _gSetupTool.getClusterManagementTool()
        .getResourceCustomizedView(CLUSTER_NAME, RESOURCE_NAME, CUSTOMIZED_STATE_TYPE));
    System.out.println("End test :" + TestHelper.getTestMethodName());
  }

  @Test(dependsOnMethods = "testExternalView")
  public void testPartitionHealth() throws Exception {
    System.out.println("Start test :" + TestHelper.getTestMethodName());

    String clusterName = "TestCluster_1";
    String resourceName = clusterName + "_db_0";

    // Disable the cluster to prevent external view from being removed
    _gSetupTool.getClusterManagementTool().enableCluster(clusterName, false);

    // Use mock numbers for testing
    Map<String, String> idealStateParams = new HashMap<>();
    idealStateParams.put("MinActiveReplicas", "2");
    idealStateParams.put("StateModelDefRef", "MasterSlave");
    idealStateParams.put("MaxPartitionsPerInstance", "3");
    idealStateParams.put("Replicas", "3");
    idealStateParams.put("NumPartitions", "3");

    // Create a mock state mapping for testing
    Map<String, List<String>> partitionReplicaStates = new LinkedHashMap<>();
    String[] p0 = {
        "MASTER", "SLAVE", "SLAVE"
    };
    String[] p1 = {
        "MASTER", "SLAVE", "ERROR"
    };
    String[] p2 = {
        "ERROR", "SLAVE", "SLAVE"
    };
    partitionReplicaStates.put("p0", Arrays.asList(p0));
    partitionReplicaStates.put("p1", Arrays.asList(p1));
    partitionReplicaStates.put("p2", Arrays.asList(p2));

    createDummyMapping(clusterName, resourceName, idealStateParams, partitionReplicaStates);

    // Get the result of getPartitionHealth
    String body = get("clusters/" + clusterName + "/resources/" + resourceName + "/health", null,
        Response.Status.OK.getStatusCode(), true);

    JsonNode node = OBJECT_MAPPER.readTree(body);
    Map<String, String> healthStatus =
        OBJECT_MAPPER.convertValue(node, new TypeReference<Map<String, String>>() {
        });

    Assert.assertEquals(healthStatus.get("p0"), "HEALTHY");
    Assert.assertEquals(healthStatus.get("p1"), "PARTIAL_HEALTHY");
    Assert.assertEquals(healthStatus.get("p2"), "UNHEALTHY");
    System.out.println("End test :" + TestHelper.getTestMethodName());

    // Re-enable the cluster
    _gSetupTool.getClusterManagementTool().enableCluster(clusterName, true);
  }

  @Test(dependsOnMethods = "testPartitionHealth")
  public void testResourceHealth() throws Exception {
    System.out.println("Start test :" + TestHelper.getTestMethodName());

    String clusterName = "TestCluster_1";
    Map<String, String> idealStateParams = new HashMap<>();
    idealStateParams.put("MinActiveReplicas", "2");
    idealStateParams.put("StateModelDefRef", "MasterSlave");
    idealStateParams.put("MaxPartitionsPerInstance", "3");
    idealStateParams.put("Replicas", "3");
    idealStateParams.put("NumPartitions", "3");

    // Disable the cluster to prevent external view from being removed
    _gSetupTool.getClusterManagementTool().enableCluster(clusterName, false);

    // Create a healthy resource
    String resourceNameHealthy = clusterName + "_db_0";
    Map<String, List<String>> partitionReplicaStates = new LinkedHashMap<>();
    String[] p0 = {
        "MASTER", "SLAVE", "SLAVE"
    };
    String[] p1 = {
        "MASTER", "SLAVE", "SLAVE"
    };
    String[] p2 = {
        "MASTER", "SLAVE", "SLAVE"
    };
    partitionReplicaStates.put("p0", Arrays.asList(p0));
    partitionReplicaStates.put("p1", Arrays.asList(p1));
    partitionReplicaStates.put("p2", Arrays.asList(p2));

    createDummyMapping(clusterName, resourceNameHealthy, idealStateParams, partitionReplicaStates);

    // Create a partially healthy resource
    String resourceNamePartiallyHealthy = clusterName + "_db_1";
    Map<String, List<String>> partitionReplicaStates_1 = new LinkedHashMap<>();
    String[] p0_1 = {
        "MASTER", "SLAVE", "SLAVE"
    };
    String[] p1_1 = {
        "MASTER", "SLAVE", "SLAVE"
    };
    String[] p2_1 = {
        "MASTER", "SLAVE", "ERROR"
    };
    partitionReplicaStates_1.put("p0", Arrays.asList(p0_1));
    partitionReplicaStates_1.put("p1", Arrays.asList(p1_1));
    partitionReplicaStates_1.put("p2", Arrays.asList(p2_1));

    createDummyMapping(clusterName, resourceNamePartiallyHealthy, idealStateParams,
        partitionReplicaStates_1);

    // Create a partially healthy resource
    String resourceNameUnhealthy = clusterName + "_db_2";
    Map<String, List<String>> partitionReplicaStates_2 = new LinkedHashMap<>();
    String[] p0_2 = {
        "MASTER", "SLAVE", "SLAVE"
    };
    String[] p1_2 = {
        "MASTER", "SLAVE", "SLAVE"
    };
    String[] p2_2 = {
        "ERROR", "SLAVE", "ERROR"
    };
    partitionReplicaStates_2.put("p0", Arrays.asList(p0_2));
    partitionReplicaStates_2.put("p1", Arrays.asList(p1_2));
    partitionReplicaStates_2.put("p2", Arrays.asList(p2_2));

    createDummyMapping(clusterName, resourceNameUnhealthy, idealStateParams,
        partitionReplicaStates_2);

    // Get the result of getResourceHealth
    String body = get("clusters/" + clusterName + "/resources/health", null,
        Response.Status.OK.getStatusCode(), true);

    JsonNode node = OBJECT_MAPPER.readTree(body);
    Map<String, String> healthStatus =
        OBJECT_MAPPER.convertValue(node, new TypeReference<Map<String, String>>() {
        });

    Assert.assertEquals(healthStatus.get(resourceNameHealthy), "HEALTHY");
    Assert.assertEquals(healthStatus.get(resourceNamePartiallyHealthy), "PARTIAL_HEALTHY");
    Assert.assertEquals(healthStatus.get(resourceNameUnhealthy), "UNHEALTHY");
    System.out.println("End test :" + TestHelper.getTestMethodName());

    // Re-enable the cluster
    _gSetupTool.getClusterManagementTool().enableCluster(clusterName, true);
  }

  /**
   * Test "update" command of updateResourceConfig.
   * @throws Exception
   */
  @Test(dependsOnMethods = "testResourceHealth")
  public void updateResourceConfig() throws Exception {
    // Get ResourceConfig
    ResourceConfig resourceConfig = _configAccessor.getResourceConfig(CLUSTER_NAME, RESOURCE_NAME);
    ZNRecord record = resourceConfig.getRecord();

    // Generate a record containing three keys (k0, k1, k2) for all fields
    String value = "RESOURCE_TEST";
    for (int i = 0; i < 3; i++) {
      String key = "k" + i;
      record.getSimpleFields().put(key, value);
      record.getMapFields().put(key, ImmutableMap.of(key, value));
      record.getListFields().put(key, Arrays.asList(key, value));
    }

    // 1. Add these fields by way of "update"
    Entity entity =
        Entity.entity(OBJECT_MAPPER.writeValueAsString(record), MediaType.APPLICATION_JSON_TYPE);
    post("clusters/" + CLUSTER_NAME + "/resources/" + RESOURCE_NAME + "/configs",
        Collections.singletonMap("command", "update"), entity, Response.Status.OK.getStatusCode());

    // Check that the fields have been added
    ResourceConfig updatedConfig = _configAccessor.getResourceConfig(CLUSTER_NAME, RESOURCE_NAME);
    Assert.assertEquals(record.getSimpleFields(), updatedConfig.getRecord().getSimpleFields());
    Assert.assertEquals(record.getListFields(), updatedConfig.getRecord().getListFields());
    Assert.assertEquals(record.getMapFields(), updatedConfig.getRecord().getMapFields());

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
    post("clusters/" + CLUSTER_NAME + "/resources/" + RESOURCE_NAME + "/configs",
        Collections.singletonMap("command", "update"), entity, Response.Status.OK.getStatusCode());

    updatedConfig = _configAccessor.getResourceConfig(CLUSTER_NAME, RESOURCE_NAME);
    // Check that the fields have been modified
    Assert.assertEquals(record.getSimpleFields(), updatedConfig.getRecord().getSimpleFields());
    Assert.assertEquals(record.getListFields(), updatedConfig.getRecord().getListFields());
    Assert.assertEquals(record.getMapFields(), updatedConfig.getRecord().getMapFields());
    System.out.println("End test :" + TestHelper.getTestMethodName());
  }

  /**
   * Test "delete" command of updateResourceConfig.
   * @throws Exception
   */
  @Test(dependsOnMethods = "updateResourceConfig")
  public void updateResourceConfigIDMissing() throws Exception {
    System.out.println("Start test :" + TestHelper.getTestMethodName());
    // An invalid input which does not have any ID
    String dummyInput = "{\"simpleFields\":{}}";

    String dummyResourceName = "RESOURCE_TEST_DUMMY";
    // Update the config with dummy input
    Entity entity = Entity.entity(dummyInput, MediaType.APPLICATION_JSON_TYPE);
    // As id field is missing, the response of the post request should be BAD_REQUEST
    post("clusters/" + CLUSTER_NAME + "/resources/" + dummyResourceName + "/configs", null, entity,
        Response.Status.BAD_REQUEST.getStatusCode());
    ResourceConfig resourceConfig =
        _configAccessor.getResourceConfig(CLUSTER_NAME, dummyResourceName);
    // Since the id is missing in the input, the znode should not get created.
    Assert.assertNull(resourceConfig);
    System.out.println("End test :" + TestHelper.getTestMethodName());
  }

  /**
   * Test "delete" command of updateResourceConfig.
   * @throws Exception
   */
  @Test(dependsOnMethods = "updateResourceConfigIDMissing")
  public void deleteFromResourceConfig() throws Exception {
    ZNRecord record = new ZNRecord(RESOURCE_NAME);

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
    post("clusters/" + CLUSTER_NAME + "/resources/" + RESOURCE_NAME + "/configs",
        Collections.singletonMap("command", "delete"), entity, Response.Status.OK.getStatusCode());

    ResourceConfig configAfterDelete =
        _configAccessor.getResourceConfig(CLUSTER_NAME, RESOURCE_NAME);

    // Check that the keys k1 and k2 have been deleted, and k0 remains
    for (int i = 0; i < 4; i++) {
      String key = "k" + i;
      if (i == 0) {
        Assert.assertTrue(configAfterDelete.getRecord().getSimpleFields().containsKey(key));
        Assert.assertTrue(configAfterDelete.getRecord().getListFields().containsKey(key));
        Assert.assertTrue(configAfterDelete.getRecord().getMapFields().containsKey(key));
        continue;
      }
      Assert.assertFalse(configAfterDelete.getRecord().getSimpleFields().containsKey(key));
      Assert.assertFalse(configAfterDelete.getRecord().getListFields().containsKey(key));
      Assert.assertFalse(configAfterDelete.getRecord().getMapFields().containsKey(key));
    }
    System.out.println("End test :" + TestHelper.getTestMethodName());
  }

  /**
   * Test "update" command of updateResourceIdealState.
   * @throws Exception
   */
  @Test(dependsOnMethods = "deleteFromResourceConfig")
  public void updateResourceIdealState() throws Exception {
    // Get IdealState ZNode
    String zkPath = PropertyPathBuilder.idealState(CLUSTER_NAME, RESOURCE_NAME);
    ZNRecord record = _baseAccessor.get(zkPath, null, AccessOption.PERSISTENT);

    // 1. Add these fields by way of "update"
    Entity entity =
        Entity.entity(OBJECT_MAPPER.writeValueAsString(record), MediaType.APPLICATION_JSON_TYPE);
    post("clusters/" + CLUSTER_NAME + "/resources/" + RESOURCE_NAME + "/idealState",
        Collections.singletonMap("command", "update"), entity, Response.Status.OK.getStatusCode());

    // Check that the fields have been added
    ZNRecord newRecord = _baseAccessor.get(zkPath, null, AccessOption.PERSISTENT);
    Assert.assertEquals(record.getSimpleFields(), newRecord.getSimpleFields());
    Assert.assertEquals(record.getListFields(), newRecord.getListFields());
    Assert.assertEquals(record.getMapFields(), newRecord.getMapFields());

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
    post("clusters/" + CLUSTER_NAME + "/resources/" + RESOURCE_NAME + "/idealState",
        Collections.singletonMap("command", "update"), entity, Response.Status.OK.getStatusCode());

    // Check that the fields have been modified
    newRecord = _baseAccessor.get(zkPath, null, AccessOption.PERSISTENT);
    Assert.assertEquals(record.getSimpleFields(), newRecord.getSimpleFields());
    Assert.assertEquals(record.getListFields(), newRecord.getListFields());
    Assert.assertEquals(record.getMapFields(), newRecord.getMapFields());
    System.out.println("End test :" + TestHelper.getTestMethodName());
  }

  /**
   * Test "enableWagedRebalance" command of updateResource.
   */
  @Test(dependsOnMethods = "updateResourceIdealState")
  public void testEnableWagedRebalance() {
    IdealState idealState =
        _gSetupTool.getClusterManagementTool().getResourceIdealState(CLUSTER_NAME, RESOURCE_NAME);
    Assert.assertNotSame(idealState.getRebalancerClassName(), WagedRebalancer.class.getName());

    // Enable waged rebalance, which should change the rebalancer class name
    Entity entity = Entity.entity(null, MediaType.APPLICATION_JSON_TYPE);
    post("clusters/" + CLUSTER_NAME + "/resources/" + RESOURCE_NAME,
        Collections.singletonMap("command", "enableWagedRebalance"), entity,
        Response.Status.OK.getStatusCode());

    idealState =
        _gSetupTool.getClusterManagementTool().getResourceIdealState(CLUSTER_NAME, RESOURCE_NAME);
    Assert.assertEquals(idealState.getRebalancerClassName(), WagedRebalancer.class.getName());
  }

  /**
   * Test "delete" command of updateResourceIdealState.
   * @throws Exception
   */
  @Test(dependsOnMethods = "testEnableWagedRebalance")
  public void deleteFromResourceIdealState() throws Exception {
    String zkPath = PropertyPathBuilder.idealState(CLUSTER_NAME, RESOURCE_NAME);
    ZNRecord record = new ZNRecord(RESOURCE_NAME);

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
    post("clusters/" + CLUSTER_NAME + "/resources/" + RESOURCE_NAME + "/idealState",
        Collections.singletonMap("command", "delete"), entity, Response.Status.OK.getStatusCode());

    ZNRecord recordAfterDelete = _baseAccessor.get(zkPath, null, AccessOption.PERSISTENT);

    // Check that the keys k1 and k2 have been deleted, and k0 remains
    for (int i = 0; i < 4; i++) {
      String key = "k" + i;
      if (i == 0) {
        Assert.assertTrue(recordAfterDelete.getSimpleFields().containsKey(key));
        Assert.assertTrue(recordAfterDelete.getListFields().containsKey(key));
        Assert.assertTrue(recordAfterDelete.getMapFields().containsKey(key));
        continue;
      }
      Assert.assertFalse(recordAfterDelete.getSimpleFields().containsKey(key));
      Assert.assertFalse(recordAfterDelete.getListFields().containsKey(key));
      Assert.assertFalse(recordAfterDelete.getMapFields().containsKey(key));
    }
    System.out.println("End test :" + TestHelper.getTestMethodName());
  }

  @Test(dependsOnMethods = "deleteFromResourceIdealState")
  public void testAddResourceWithWeight() throws IOException {
    // Test case 1: Add a valid resource with valid weights
    // Create a resource with IdealState and ResourceConfig
    String wagedResourceName = "newWagedResource";

    // Create an IdealState on full-auto with 1 partition
    IdealState idealState = new IdealState(wagedResourceName);
    idealState.getRecord().getSimpleFields().putAll(_gSetupTool.getClusterManagementTool()
        .getResourceIdealState(CLUSTER_NAME, RESOURCE_NAME).getRecord().getSimpleFields());
    idealState.setRebalanceMode(IdealState.RebalanceMode.FULL_AUTO);
    idealState.setRebalancerClassName(WagedRebalancer.class.getName());
    idealState.setNumPartitions(1); // 1 partition for convenience of testing

    // Create a ResourceConfig with FOO and BAR at 100 respectively
    ResourceConfig resourceConfig = new ResourceConfig(wagedResourceName);
    Map<String, Map<String, Integer>> partitionCapacityMap = new HashMap<>();
    Map<String, Integer> partitionCapacity = ImmutableMap.of("FOO", 100, "BAR", 100);
    partitionCapacityMap.put(wagedResourceName + "_0", partitionCapacity);
    // Also add a default key
    partitionCapacityMap.put(ResourceConfig.DEFAULT_PARTITION_KEY, partitionCapacity);
    resourceConfig.setPartitionCapacityMap(partitionCapacityMap);

    // Put both IdealState and ResourceConfig into a map as required
    Map<String, ZNRecord> inputMap = ImmutableMap.of(
        ResourceAccessor.ResourceProperties.idealState.name(), idealState.getRecord(),
        ResourceAccessor.ResourceProperties.resourceConfig.name(), resourceConfig.getRecord());

    // Create an entity using the inputMap
    Entity entity =
        Entity.entity(OBJECT_MAPPER.writeValueAsString(inputMap), MediaType.APPLICATION_JSON_TYPE);

    // Make a HTTP call to the REST endpoint
    put("clusters/" + CLUSTER_NAME + "/resources/" + wagedResourceName,
        ImmutableMap.of("command", "addWagedResource"), entity, Response.Status.OK.getStatusCode());

    // Test case 2: Add a resource with invalid weights
    String invalidResourceName = "invalidWagedResource";
    ResourceConfig invalidWeightResourceConfig = new ResourceConfig(invalidResourceName);
    IdealState invalidWeightIdealState = new IdealState(invalidResourceName);

    Map<String, ZNRecord> invalidInputMap = ImmutableMap.of(
        ResourceAccessor.ResourceProperties.idealState.name(), invalidWeightIdealState.getRecord(),
        ResourceAccessor.ResourceProperties.resourceConfig.name(),
        invalidWeightResourceConfig.getRecord());

    // Create an entity using invalidInputMap
    entity = Entity.entity(OBJECT_MAPPER.writeValueAsString(invalidInputMap),
        MediaType.APPLICATION_JSON_TYPE);

    // Make a HTTP call to the REST endpoint
    put("clusters/" + CLUSTER_NAME + "/resources/" + invalidResourceName,
        ImmutableMap.of("command", "addWagedResource"), entity,
        Response.Status.BAD_REQUEST.getStatusCode());
  }

  /**
   * Guard rail: adding a WAGED resource whose partition weight exceeds the largest single instance's
   * capacity in any dimension is rejected before the resource is written to ZooKeeper, because such
   * a resource is permanently unplaceable. Verifies enforcement (400 + verdict), dry-run (200 +
   * verdict, no write), force bypass (created), and the within-capacity happy path (created). The
   * cluster/instance capacity configuration is saved and restored so this test does not perturb the
   * other resource tests that share {@value #CLUSTER_NAME}.
   */
  @Test(dependsOnMethods = "testAddResourceWithWeight")
  public void testAddWagedResourceWeightGuardrail() throws Exception {
    System.out.println("Start test :" + TestHelper.getTestMethodName());

    ClusterConfig clusterConfig = _configAccessor.getClusterConfig(CLUSTER_NAME);
    List<String> originalCapacityKeys = clusterConfig.getInstanceCapacityKeys();
    List<String> instances =
        _gSetupTool.getClusterManagementTool().getInstancesInCluster(CLUSTER_NAME);
    Map<String, Map<String, Integer>> originalInstanceCapacities = new HashMap<>();
    for (String instance : instances) {
      originalInstanceCapacities.put(instance,
          _configAccessor.getInstanceConfig(CLUSTER_NAME, instance).getInstanceCapacityMap());
    }

    String blockedResource = "guardrailBlockedWagedResource";
    String forcedResource = "guardrailForcedWagedResource";
    String validResource = "guardrailValidWagedResource";
    String disabledResource = "guardrailDisabledWagedResource";

    try {
      // Declare two capacity dimensions and give every instance capacity 100 in each.
      clusterConfig.setInstanceCapacityKeys(Arrays.asList("FOO", "BAR"));
      _configAccessor.setClusterConfig(CLUSTER_NAME, clusterConfig);
      Map<String, Integer> instanceCapacity = ImmutableMap.of("FOO", 100, "BAR", 100);
      for (String instance : instances) {
        InstanceConfig instanceConfig = _configAccessor.getInstanceConfig(CLUSTER_NAME, instance);
        instanceConfig.setInstanceCapacityMap(instanceCapacity);
        _configAccessor.setInstanceConfig(CLUSTER_NAME, instance, instanceConfig);
      }

      // FOO weight 1000 exceeds the largest instance's FOO capacity (100): permanently unplaceable.
      Map<String, Map<String, Integer>> overWeight = ImmutableMap.of(
          ResourceConfig.DEFAULT_PARTITION_KEY, ImmutableMap.of("FOO", 1000, "BAR", 100));

      // 0) Opt-in: the guard rail is disabled by default, so an over-capacity resource is allowed
      //    through and actually created even without force=true.
      Response disabled = putWagedResource(disabledResource,
          wagedResourceConfig(disabledResource, overWeight), Collections.emptyMap());
      Assert.assertEquals(disabled.getStatus(), Response.Status.OK.getStatusCode());
      Assert.assertTrue(_gSetupTool.getClusterManagementTool().getResourcesInCluster(CLUSTER_NAME)
          .contains(disabledResource));

      // Enable the guard rail for the remainder of the test (opt-in per cluster).
      clusterConfig = _configAccessor.getClusterConfig(CLUSTER_NAME);
      clusterConfig.setPartitionWeightGuardrailEnabled(true);
      _configAccessor.setClusterConfig(CLUSTER_NAME, clusterConfig);

      // 1) Enforcement: blocked with 400 + verdict, and nothing written to ZK.
      Response blocked = putWagedResource(blockedResource,
          wagedResourceConfig(blockedResource, overWeight), Collections.emptyMap());
      Assert.assertEquals(blocked.getStatus(), Response.Status.BAD_REQUEST.getStatusCode());
      JsonNode blockedVerdict = OBJECT_MAPPER.readTree(blocked.readEntity(String.class));
      Assert.assertFalse(blockedVerdict.get("feasible").asBoolean());
      Assert.assertTrue(
          blockedVerdict.toString().contains(PartitionWeightCapacityGuardrailRule.RULE_ID));
      Assert.assertFalse(_gSetupTool.getClusterManagementTool().getResourcesInCluster(CLUSTER_NAME)
          .contains(blockedResource));

      // 2) Dry-run: always 200 with the same infeasible verdict, and still nothing written.
      Response dryRun = putWagedResource(blockedResource,
          wagedResourceConfig(blockedResource, overWeight), ImmutableMap.of("dryRun", true));
      Assert.assertEquals(dryRun.getStatus(), Response.Status.OK.getStatusCode());
      JsonNode dryRunVerdict = OBJECT_MAPPER.readTree(dryRun.readEntity(String.class));
      Assert.assertFalse(dryRunVerdict.get("feasible").asBoolean());
      Assert.assertTrue(
          dryRunVerdict.toString().contains(PartitionWeightCapacityGuardrailRule.RULE_ID));
      Assert.assertFalse(_gSetupTool.getClusterManagementTool().getResourcesInCluster(CLUSTER_NAME)
          .contains(blockedResource));

      // 3) force=true bypasses the guard rail: the over-weight resource is actually created.
      Response forced = putWagedResource(forcedResource,
          wagedResourceConfig(forcedResource, overWeight), ImmutableMap.of("force", true));
      Assert.assertEquals(forced.getStatus(), Response.Status.OK.getStatusCode());
      Assert.assertTrue(_gSetupTool.getClusterManagementTool().getResourcesInCluster(CLUSTER_NAME)
          .contains(forcedResource));

      // 4) A resource within capacity passes the guard rail and is created normally.
      Map<String, Map<String, Integer>> withinCapacity = ImmutableMap.of(
          ResourceConfig.DEFAULT_PARTITION_KEY, ImmutableMap.of("FOO", 100, "BAR", 100));
      Response valid = putWagedResource(validResource,
          wagedResourceConfig(validResource, withinCapacity), Collections.emptyMap());
      Assert.assertEquals(valid.getStatus(), Response.Status.OK.getStatusCode());
      Assert.assertTrue(_gSetupTool.getClusterManagementTool().getResourcesInCluster(CLUSTER_NAME)
          .contains(validResource));
    } finally {
      // Drop any resources this test created (blockedResource was never created; ignore failures).
      for (String resource : Arrays.asList(forcedResource, validResource, disabledResource,
          blockedResource)) {
        try {
          _gSetupTool.getClusterManagementTool().dropResource(CLUSTER_NAME, resource);
        } catch (Exception ignored) {
        }
      }
      // Restore cluster + instance capacity configuration to its original values, and disable the
      // opt-in guard rail again so it does not leak into other tests sharing this cluster.
      ClusterConfig restore = _configAccessor.getClusterConfig(CLUSTER_NAME);
      restore.setInstanceCapacityKeys(originalCapacityKeys);
      restore.setPartitionWeightGuardrailEnabled(false);
      _configAccessor.setClusterConfig(CLUSTER_NAME, restore);
      for (String instance : instances) {
        InstanceConfig instanceConfig = _configAccessor.getInstanceConfig(CLUSTER_NAME, instance);
        instanceConfig.setInstanceCapacityMap(originalInstanceCapacities.get(instance));
        _configAccessor.setInstanceConfig(CLUSTER_NAME, instance, instanceConfig);
      }
    }
    System.out.println("End test :" + TestHelper.getTestMethodName());
  }

  /**
   * Guard rail: adding a WAGED resource is rejected before it is written to ZooKeeper when some
   * assignable instance omits a capacity dimension (a key in the cluster's INSTANCE_CAPACITY_KEYS),
   * which would make WAGED unable to build a model for the resource so it is accepted but never
   * places. This is the instance-side gap that neither the weight guard rail nor the admin API's
   * resource-side validation ({@code addResourceWithWeight}) covers. The guard rail always runs on
   * addWagedResource (it is not gated behind a cluster config toggle). Verifies enforcement (400 +
   * verdict naming the instance), dry-run, force bypass (which works here because the admin write
   * path does not re-check instance capacities), and the fully-covered happy path.
   * The cluster/instance capacity configuration is saved and restored so this test does not perturb
   * the other resource tests that share {@value #CLUSTER_NAME}.
   */
  @Test(dependsOnMethods = "testAddResourceWithWeight")
  public void testAddWagedResourceCapacityKeyGuardrail() throws Exception {
    System.out.println("Start test :" + TestHelper.getTestMethodName());

    ClusterConfig clusterConfig = _configAccessor.getClusterConfig(CLUSTER_NAME);
    List<String> originalCapacityKeys = clusterConfig.getInstanceCapacityKeys();
    List<String> instances =
        _gSetupTool.getClusterManagementTool().getInstancesInCluster(CLUSTER_NAME);
    Map<String, Map<String, Integer>> originalInstanceCapacities = new HashMap<>();
    for (String instance : instances) {
      originalInstanceCapacities.put(instance,
          _configAccessor.getInstanceConfig(CLUSTER_NAME, instance).getInstanceCapacityMap());
    }
    String starvedInstance = instances.get(0);

    String blockedResource = "keyGuardrailBlockedWagedResource";
    String forcedResource = "keyGuardrailForcedWagedResource";
    String validResource = "keyGuardrailValidWagedResource";

    // The resource weight always covers both dimensions; the gap under test is on the instance side.
    Map<String, Map<String, Integer>> fullWeight = ImmutableMap.of(
        ResourceConfig.DEFAULT_PARTITION_KEY, ImmutableMap.of("FOO", 10, "BAR", 5));

    try {
      // Declare two capacity dimensions and give every instance capacity in both dimensions, except
      // starve one assignable instance of BAR to create the instance-side gap.
      clusterConfig.setInstanceCapacityKeys(Arrays.asList("FOO", "BAR"));
      _configAccessor.setClusterConfig(CLUSTER_NAME, clusterConfig);
      for (String instance : instances) {
        InstanceConfig instanceConfig = _configAccessor.getInstanceConfig(CLUSTER_NAME, instance);
        instanceConfig.setInstanceCapacityMap(instance.equals(starvedInstance)
            ? ImmutableMap.of("FOO", 100) : ImmutableMap.of("FOO", 100, "BAR", 100));
        _configAccessor.setInstanceConfig(CLUSTER_NAME, instance, instanceConfig);
      }

      // 1) Enforcement: blocked with 400 + a verdict naming the starved instance, nothing written.
      Response blocked = putWagedResource(blockedResource,
          wagedResourceConfig(blockedResource, fullWeight), Collections.emptyMap());
      Assert.assertEquals(blocked.getStatus(), Response.Status.BAD_REQUEST.getStatusCode());
      JsonNode blockedVerdict = OBJECT_MAPPER.readTree(blocked.readEntity(String.class));
      Assert.assertFalse(blockedVerdict.get("feasible").asBoolean());
      Assert.assertTrue(
          blockedVerdict.toString().contains(CapacityKeyConsistencyGuardrailRule.RULE_ID));
      Assert.assertTrue(blockedVerdict.toString().contains(starvedInstance));
      Assert.assertFalse(_gSetupTool.getClusterManagementTool().getResourcesInCluster(CLUSTER_NAME)
          .contains(blockedResource));

      // 2) Dry-run: always 200 with the same infeasible verdict, and still nothing written.
      Response dryRun = putWagedResource(blockedResource,
          wagedResourceConfig(blockedResource, fullWeight), ImmutableMap.of("dryRun", true));
      Assert.assertEquals(dryRun.getStatus(), Response.Status.OK.getStatusCode());
      JsonNode dryRunVerdict = OBJECT_MAPPER.readTree(dryRun.readEntity(String.class));
      Assert.assertFalse(dryRunVerdict.get("feasible").asBoolean());
      Assert.assertTrue(
          dryRunVerdict.toString().contains(CapacityKeyConsistencyGuardrailRule.RULE_ID));
      Assert.assertFalse(_gSetupTool.getClusterManagementTool().getResourcesInCluster(CLUSTER_NAME)
          .contains(blockedResource));

      // 3) force=true bypasses the guard rail: the resource is actually created despite the gap
      //    (the admin write path does not validate instance capacities, which is exactly the silent
      //    failure this guard rail exists to prevent).
      Response forced = putWagedResource(forcedResource,
          wagedResourceConfig(forcedResource, fullWeight), ImmutableMap.of("force", true));
      Assert.assertEquals(forced.getStatus(), Response.Status.OK.getStatusCode());
      Assert.assertTrue(_gSetupTool.getClusterManagementTool().getResourcesInCluster(CLUSTER_NAME)
          .contains(forcedResource));

      // 4) Close the gap (every instance now declares both keys) and the add passes the guard rail.
      InstanceConfig restored = _configAccessor.getInstanceConfig(CLUSTER_NAME, starvedInstance);
      restored.setInstanceCapacityMap(ImmutableMap.of("FOO", 100, "BAR", 100));
      _configAccessor.setInstanceConfig(CLUSTER_NAME, starvedInstance, restored);

      Response valid = putWagedResource(validResource,
          wagedResourceConfig(validResource, fullWeight), Collections.emptyMap());
      Assert.assertEquals(valid.getStatus(), Response.Status.OK.getStatusCode());
      Assert.assertTrue(_gSetupTool.getClusterManagementTool().getResourcesInCluster(CLUSTER_NAME)
          .contains(validResource));
    } finally {
      // Drop any resources this test created (blockedResource was never created; ignore failures).
      for (String resource : Arrays.asList(forcedResource, validResource, blockedResource)) {
        try {
          _gSetupTool.getClusterManagementTool().dropResource(CLUSTER_NAME, resource);
        } catch (Exception ignored) {
        }
      }
      // Restore cluster + instance capacity configuration so it does not leak into other tests
      // sharing this cluster.
      ClusterConfig restore = _configAccessor.getClusterConfig(CLUSTER_NAME);
      restore.setInstanceCapacityKeys(originalCapacityKeys);
      _configAccessor.setClusterConfig(CLUSTER_NAME, restore);
      for (String instance : instances) {
        InstanceConfig instanceConfig = _configAccessor.getInstanceConfig(CLUSTER_NAME, instance);
        instanceConfig.setInstanceCapacityMap(originalInstanceCapacities.get(instance));
        _configAccessor.setInstanceConfig(CLUSTER_NAME, instance, instanceConfig);
      }
    }
    System.out.println("End test :" + TestHelper.getTestMethodName());
  }

  /**
   * Guard rail: editing an existing WAGED resource's partition weight via updateResourceConfig is
   * rejected when the new weight, in any dimension, exceeds the largest single instance's capacity
   * -- the same permanently-unplaceable condition the add-path weight guard rail blocks, but on the
   * raw config-edit write path, which otherwise never re-runs the resource-side weight validation.
   * The merged (post-write) config is validated, matching how the ZK write merges the incoming
   * record into the existing config. Verifies enforcement (400 + verdict, config unchanged),
   * dry-run (200 + verdict, config unchanged), force bypass (written), and the within-capacity
   * happy path (written). Shares the opt-in enablement and the save/restore discipline of the
   * add-path weight guard test so it does not perturb the other tests sharing {@value #CLUSTER_NAME}.
   */
  @Test(dependsOnMethods = "testAddResourceWithWeight")
  public void testUpdateResourceConfigWeightGuardrail() throws Exception {
    System.out.println("Start test :" + TestHelper.getTestMethodName());

    ClusterConfig clusterConfig = _configAccessor.getClusterConfig(CLUSTER_NAME);
    List<String> originalCapacityKeys = clusterConfig.getInstanceCapacityKeys();
    List<String> instances =
        _gSetupTool.getClusterManagementTool().getInstancesInCluster(CLUSTER_NAME);
    Map<String, Map<String, Integer>> originalInstanceCapacities = new HashMap<>();
    for (String instance : instances) {
      originalInstanceCapacities.put(instance,
          _configAccessor.getInstanceConfig(CLUSTER_NAME, instance).getInstanceCapacityMap());
    }

    String editResource = "guardrailWeightEditResource";
    Map<String, Map<String, Integer>> withinCapacity = ImmutableMap.of(
        ResourceConfig.DEFAULT_PARTITION_KEY, ImmutableMap.of("FOO", 100, "BAR", 100));
    Map<String, Map<String, Integer>> overWeight = ImmutableMap.of(
        ResourceConfig.DEFAULT_PARTITION_KEY, ImmutableMap.of("FOO", 1000, "BAR", 100));

    try {
      // Two capacity dimensions, every instance capacity 100 in each, and the opt-in guard enabled.
      clusterConfig.setInstanceCapacityKeys(Arrays.asList("FOO", "BAR"));
      clusterConfig.setPartitionWeightGuardrailEnabled(true);
      _configAccessor.setClusterConfig(CLUSTER_NAME, clusterConfig);
      Map<String, Integer> instanceCapacity = ImmutableMap.of("FOO", 100, "BAR", 100);
      for (String instance : instances) {
        InstanceConfig instanceConfig = _configAccessor.getInstanceConfig(CLUSTER_NAME, instance);
        instanceConfig.setInstanceCapacityMap(instanceCapacity);
        _configAccessor.setInstanceConfig(CLUSTER_NAME, instance, instanceConfig);
      }

      // Create the resource within capacity so there is an existing config to edit.
      Response created = putWagedResource(editResource,
          wagedResourceConfig(editResource, withinCapacity), Collections.emptyMap());
      Assert.assertEquals(created.getStatus(), Response.Status.OK.getStatusCode());
      Assert.assertEquals(storedDefaultWeight(editResource, "FOO"), 100);

      // 1) Enforcement: raising the weight above capacity is blocked with 400 + verdict, and the
      //    stored config is unchanged (still within capacity).
      Response blocked = postResourceConfigUpdate(editResource, overWeight, Collections.emptyMap());
      Assert.assertEquals(blocked.getStatus(), Response.Status.BAD_REQUEST.getStatusCode());
      JsonNode blockedVerdict = OBJECT_MAPPER.readTree(blocked.readEntity(String.class));
      Assert.assertFalse(blockedVerdict.get("feasible").asBoolean());
      Assert.assertTrue(
          blockedVerdict.toString().contains(PartitionWeightCapacityGuardrailRule.RULE_ID));
      Assert.assertEquals(storedDefaultWeight(editResource, "FOO"), 100);

      // 2) Dry-run: 200 with the same infeasible verdict, and the stored config still unchanged.
      Response dryRun =
          postResourceConfigUpdate(editResource, overWeight, ImmutableMap.of("dryRun", "true"));
      Assert.assertEquals(dryRun.getStatus(), Response.Status.OK.getStatusCode());
      JsonNode dryRunVerdict = OBJECT_MAPPER.readTree(dryRun.readEntity(String.class));
      Assert.assertFalse(dryRunVerdict.get("feasible").asBoolean());
      Assert.assertTrue(
          dryRunVerdict.toString().contains(PartitionWeightCapacityGuardrailRule.RULE_ID));
      Assert.assertEquals(storedDefaultWeight(editResource, "FOO"), 100);

      // 3) force=true bypasses the guard rail: the over-weight edit is actually written.
      Response forced =
          postResourceConfigUpdate(editResource, overWeight, ImmutableMap.of("force", "true"));
      Assert.assertEquals(forced.getStatus(), Response.Status.OK.getStatusCode());
      Assert.assertEquals(storedDefaultWeight(editResource, "FOO"), 1000);

      // 4) A within-capacity edit passes the guard rail and is written normally.
      Response valid =
          postResourceConfigUpdate(editResource, withinCapacity, Collections.emptyMap());
      Assert.assertEquals(valid.getStatus(), Response.Status.OK.getStatusCode());
      Assert.assertEquals(storedDefaultWeight(editResource, "FOO"), 100);
    } finally {
      try {
        _gSetupTool.getClusterManagementTool().dropResource(CLUSTER_NAME, editResource);
      } catch (Exception ignored) {
      }
      // Restore cluster + instance capacity configuration and disable the opt-in guard rail again so
      // it does not leak into other tests sharing this cluster.
      ClusterConfig restore = _configAccessor.getClusterConfig(CLUSTER_NAME);
      restore.setInstanceCapacityKeys(originalCapacityKeys);
      restore.setPartitionWeightGuardrailEnabled(false);
      _configAccessor.setClusterConfig(CLUSTER_NAME, restore);
      for (String instance : instances) {
        InstanceConfig instanceConfig = _configAccessor.getInstanceConfig(CLUSTER_NAME, instance);
        instanceConfig.setInstanceCapacityMap(originalInstanceCapacities.get(instance));
        _configAccessor.setInstanceConfig(CLUSTER_NAME, instance, instanceConfig);
      }
    }
    System.out.println("End test :" + TestHelper.getTestMethodName());
  }

  /**
   * Guard rail: flipping an existing resource to WAGED via the {@code enableWagedRebalance} command
   * is rejected before it is written to ZooKeeper when some assignable instance omits a capacity
   * dimension (a key in the cluster's INSTANCE_CAPACITY_KEYS). That is the same instance-side gap
   * {@code addWagedResource} guards: {@code enableWagedRebalance} sets WagedRebalancer + FULL_AUTO on
   * the resource's IdealState without any instance validation, so with such a gap WAGED cannot build
   * a model and the resource is flipped to WAGED but never places. The guard rail always runs on
   * enableWagedRebalance (it is not gated behind a cluster config toggle). Verifies enforcement (400 +
   * verdict naming the instance, and the resource is NOT flipped), dry-run (200 + verdict, not
   * flipped), force bypass (flipped despite the gap, since the admin path does not re-check instance
   * capacities), and the fully-covered happy path (flipped).
   * The cluster/instance capacity configuration is saved and restored so this test does not perturb
   * the other resource tests that share {@value #CLUSTER_NAME}.
   */
  @Test(dependsOnMethods = "testAddWagedResourceCapacityKeyGuardrail")
  public void testEnableWagedRebalanceCapacityKeyGuardrail() throws Exception {
    System.out.println("Start test :" + TestHelper.getTestMethodName());

    ClusterConfig clusterConfig = _configAccessor.getClusterConfig(CLUSTER_NAME);
    List<String> originalCapacityKeys = clusterConfig.getInstanceCapacityKeys();
    List<String> instances =
        _gSetupTool.getClusterManagementTool().getInstancesInCluster(CLUSTER_NAME);
    Map<String, Map<String, Integer>> originalInstanceCapacities = new HashMap<>();
    for (String instance : instances) {
      originalInstanceCapacities.put(instance,
          _configAccessor.getInstanceConfig(CLUSTER_NAME, instance).getInstanceCapacityMap());
    }
    String starvedInstance = instances.get(0);

    String blockedResource = "enableWagedKeyGuardrailBlockedResource";
    String forcedResource = "enableWagedKeyGuardrailForcedResource";
    String validResource = "enableWagedKeyGuardrailValidResource";

    try {
      // Create three non-WAGED resources to flip; each starts on SEMI_AUTO, not WagedRebalancer.
      for (String resource : Arrays.asList(blockedResource, forcedResource, validResource)) {
        _gSetupTool.getClusterManagementTool()
            .addResource(CLUSTER_NAME, resource, 1, "OnlineOffline");
        Assert.assertFalse(isWagedRebalanced(resource));
      }

      // Declare two capacity dimensions and give every instance capacity in both dimensions, except
      // starve one assignable instance of BAR to create the instance-side gap.
      clusterConfig.setInstanceCapacityKeys(Arrays.asList("FOO", "BAR"));
      _configAccessor.setClusterConfig(CLUSTER_NAME, clusterConfig);
      for (String instance : instances) {
        InstanceConfig instanceConfig = _configAccessor.getInstanceConfig(CLUSTER_NAME, instance);
        instanceConfig.setInstanceCapacityMap(instance.equals(starvedInstance)
            ? ImmutableMap.of("FOO", 100) : ImmutableMap.of("FOO", 100, "BAR", 100));
        _configAccessor.setInstanceConfig(CLUSTER_NAME, instance, instanceConfig);
      }

      // 1) Enforcement: blocked with 400 + a verdict naming the starved instance; NOT flipped.
      Response blocked = postEnableWagedRebalance(blockedResource, Collections.emptyMap());
      Assert.assertEquals(blocked.getStatus(), Response.Status.BAD_REQUEST.getStatusCode());
      JsonNode blockedVerdict = OBJECT_MAPPER.readTree(blocked.readEntity(String.class));
      Assert.assertFalse(blockedVerdict.get("feasible").asBoolean());
      Assert.assertTrue(
          blockedVerdict.toString().contains(CapacityKeyConsistencyGuardrailRule.RULE_ID));
      Assert.assertTrue(blockedVerdict.toString().contains(starvedInstance));
      Assert.assertFalse(isWagedRebalanced(blockedResource));

      // 2) Dry-run: always 200 with the same infeasible verdict, and still NOT flipped.
      Response dryRun =
          postEnableWagedRebalance(blockedResource, ImmutableMap.of("dryRun", true));
      Assert.assertEquals(dryRun.getStatus(), Response.Status.OK.getStatusCode());
      JsonNode dryRunVerdict = OBJECT_MAPPER.readTree(dryRun.readEntity(String.class));
      Assert.assertFalse(dryRunVerdict.get("feasible").asBoolean());
      Assert.assertTrue(
          dryRunVerdict.toString().contains(CapacityKeyConsistencyGuardrailRule.RULE_ID));
      Assert.assertFalse(isWagedRebalanced(blockedResource));

      // 3) force=true bypasses the guard rail: the resource is actually flipped despite the gap
      //    (the admin path does not validate instance capacities, which is exactly the silent
      //    failure this guard rail exists to prevent).
      Response forced = postEnableWagedRebalance(forcedResource, ImmutableMap.of("force", true));
      Assert.assertEquals(forced.getStatus(), Response.Status.OK.getStatusCode());
      Assert.assertTrue(isWagedRebalanced(forcedResource));

      // 4) Close the gap (every instance now declares both keys) and the flip passes the guard rail.
      InstanceConfig restored = _configAccessor.getInstanceConfig(CLUSTER_NAME, starvedInstance);
      restored.setInstanceCapacityMap(ImmutableMap.of("FOO", 100, "BAR", 100));
      _configAccessor.setInstanceConfig(CLUSTER_NAME, starvedInstance, restored);

      Response valid = postEnableWagedRebalance(validResource, Collections.emptyMap());
      Assert.assertEquals(valid.getStatus(), Response.Status.OK.getStatusCode());
      Assert.assertTrue(isWagedRebalanced(validResource));
    } finally {
      // Drop any resources this test created (ignore failures).
      for (String resource : Arrays.asList(blockedResource, forcedResource, validResource)) {
        try {
          _gSetupTool.getClusterManagementTool().dropResource(CLUSTER_NAME, resource);
        } catch (Exception ignored) {
        }
      }
      // Restore cluster + instance capacity configuration so it does not leak into other tests
      // sharing this cluster.
      ClusterConfig restore = _configAccessor.getClusterConfig(CLUSTER_NAME);
      restore.setInstanceCapacityKeys(originalCapacityKeys);
      _configAccessor.setClusterConfig(CLUSTER_NAME, restore);
      for (String instance : instances) {
        InstanceConfig instanceConfig = _configAccessor.getInstanceConfig(CLUSTER_NAME, instance);
        instanceConfig.setInstanceCapacityMap(originalInstanceCapacities.get(instance));
        _configAccessor.setInstanceConfig(CLUSTER_NAME, instance, instanceConfig);
      }
    }
    System.out.println("End test :" + TestHelper.getTestMethodName());
  }

  /**
   * The force / dryRun flags on the resource-update (POST) endpoint are only meaningful for the
   * guard-rail-backed enableWagedRebalance command. For any other command they are rejected up front
   * with a 400 so a caller is never misled into thinking, e.g., a plain enable/disable was simulated
   * (dryRun) or its verdict overridden (force) -- neither of which those commands honor.
   */
  @Test(dependsOnMethods = "testAddResourceWithWeight")
  public void testDryRunAndForceRejectedForNonWagedUpdateCommand() {
    System.out.println("Start test :" + TestHelper.getTestMethodName());

    Entity<?> entity = Entity.entity(null, MediaType.APPLICATION_JSON_TYPE);
    for (String flag : Arrays.asList("dryRun", "force")) {
      Response response = target("clusters/" + CLUSTER_NAME + "/resources/" + RESOURCE_NAME)
          .queryParam("command", "enable").queryParam(flag, true).request().post(entity);
      Assert.assertEquals(response.getStatus(), Response.Status.BAD_REQUEST.getStatusCode());
      Assert.assertTrue(response.readEntity(String.class).contains("enableWagedRebalance"));
    }

    System.out.println("End test :" + TestHelper.getTestMethodName());
  }

  @Test(dependsOnMethods = "testAddResourceWithWeight")
  public void testDryRunAndForceRejectedForNonWagedCommand() throws IOException {
    System.out.println("Start test :" + TestHelper.getTestMethodName());

    String dryRunResource = "dryRunRejectedResource";
    put("clusters/" + CLUSTER_NAME + "/resources/" + dryRunResource,
        ImmutableMap.of("command", "addResource", "numPartitions", "1", "stateModelRef",
            "OnlineOffline", "dryRun", "true"),
        Entity.entity("", MediaType.APPLICATION_JSON_TYPE),
        Response.Status.BAD_REQUEST.getStatusCode());
    Assert.assertFalse(_gSetupTool.getClusterManagementTool().getResourcesInCluster(CLUSTER_NAME)
        .contains(dryRunResource));

    String forceResource = "forceRejectedResource";
    put("clusters/" + CLUSTER_NAME + "/resources/" + forceResource,
        ImmutableMap.of("command", "addResource", "numPartitions", "1", "stateModelRef",
            "OnlineOffline", "force", "true"),
        Entity.entity("", MediaType.APPLICATION_JSON_TYPE),
        Response.Status.BAD_REQUEST.getStatusCode());
    Assert.assertFalse(_gSetupTool.getClusterManagementTool().getResourcesInCluster(CLUSTER_NAME)
        .contains(forceResource));

    System.out.println("End test :" + TestHelper.getTestMethodName());
  }

  private Response postResourceConfigUpdate(String resourceName,
      Map<String, Map<String, Integer>> partitionWeights, Map<String, Object> flags)
      throws IOException {
    ResourceConfig resourceConfig = new ResourceConfig(resourceName);
    resourceConfig.setPartitionCapacityMap(partitionWeights);
    Entity entity = Entity.entity(OBJECT_MAPPER.writeValueAsString(resourceConfig.getRecord()),
        MediaType.APPLICATION_JSON_TYPE);
    WebTarget webTarget =
        target("clusters/" + CLUSTER_NAME + "/resources/" + resourceName + "/configs")
            .queryParam("command", "update");
    for (Map.Entry<String, Object> flag : flags.entrySet()) {
      webTarget = webTarget.queryParam(flag.getKey(), flag.getValue());
    }
    return webTarget.request().post(entity);
  }

  private int storedDefaultWeight(String resourceName, String dimension) throws IOException {
    return _configAccessor.getResourceConfig(CLUSTER_NAME, resourceName).getPartitionCapacityMap()
        .get(ResourceConfig.DEFAULT_PARTITION_KEY).get(dimension);
  }

  /**
   * Always-on WAGED rebalance-feasibility guard rail on {@code updateResourceIdealState?command=update}.
   * Uses a self-contained WAGED cluster whose replica count already equals the assignable-instance
   * count, so raising {@code REPLICAS} beyond it is a deterministic placement deficit (no instance can
   * hold a second replica of a partition). Verifies enforcement (400 + verdict, nothing written),
   * dry-run (200 + verdict, nothing written), that a legitimate replica <em>decrease</em> is not
   * falsely flagged, and force bypass.
   */
  @Test(dependsOnMethods = "testAddWagedResourceWeightGuardrail")
  public void testUpdateIdealStateRebalanceFeasibilityGuardrail() throws Exception {
    System.out.println("Start test :" + TestHelper.getTestMethodName());
    String cluster = "TestClusterIdealStateWagedGuardrail";
    String resource = "TestDB_WAGED";
    String capacityKey = "CU";
    int numNodes = 3;
    int numPartitions = 3;
    int replica = 3;
    try {
      _gSetupTool.addCluster(cluster, true);
      ClusterConfig clusterConfig = _configAccessor.getClusterConfig(cluster);
      clusterConfig.setInstanceCapacityKeys(Collections.singletonList(capacityKey));
      clusterConfig.setDefaultInstanceCapacityMap(Collections.singletonMap(capacityKey, 100));
      clusterConfig.setDefaultPartitionWeightMap(Collections.singletonMap(capacityKey, 1));
      _configAccessor.setClusterConfig(cluster, clusterConfig);

      for (int i = 0; i < numNodes; i++) {
        String instance = cluster + "_localhost_" + (14100 + i);
        _gSetupTool.addInstanceToCluster(cluster, instance);
        MockParticipantManager participant =
            new MockParticipantManager(ZK_ADDR, cluster, instance);
        participant.syncStart();
        _mockParticipantManagers.add(participant);
      }
      _clusterControllerManagers.add(startController(cluster));

      _gSetupTool.addResourceToCluster(cluster, resource, numPartitions, "MasterSlave",
          IdealState.RebalanceMode.FULL_AUTO.toString(), null);
      IdealState idealState =
          _gSetupTool.getClusterManagementTool().getResourceIdealState(cluster, resource);
      idealState.setMinActiveReplicas(1);
      idealState.setRebalancerClassName(WagedRebalancer.class.getName());
      _gSetupTool.getClusterManagementTool().setResourceIdealState(cluster, resource, idealState);
      _gSetupTool.rebalanceStorageCluster(cluster, resource, replica);

      try (BestPossibleExternalViewVerifier verifier =
          new BestPossibleExternalViewVerifier.Builder(cluster).setZkAddr(ZK_ADDR).build()) {
        Assert.assertTrue(verifier.verifyByPolling(),
            "cluster should converge before exercising the guard rail");
      }

      String endpoint = "clusters/{}/resources/{}/idealState?command=update";

      // 1. Enforcement: raising REPLICAS to 4 is unplaceable on 3 instances -> 400 + verdict, and the
      //    ideal state is not written.
      Response blocked = new JerseyUriRequestBuilder(endpoint)
          .expectedReturnStatusCode(Response.Status.BAD_REQUEST.getStatusCode())
          .format(cluster, resource).post(this, replicaUpdateEntity(resource, 4));
      Assert.assertTrue(
          blocked.readEntity(String.class)
              .contains(IdealStateRebalanceFeasibilityGuardrailRule.RULE_ID),
          "blocked verdict should carry the rule id");
      Assert.assertEquals(
          _gSetupTool.getClusterManagementTool().getResourceIdealState(cluster, resource)
              .getReplicas(), String.valueOf(replica),
          "a blocked replica increase must not be written");

      // 2. dryRun: 200 with the infeasible verdict, still nothing written.
      Response dryRun = new JerseyUriRequestBuilder(endpoint + "&dryRun=true").format(cluster, resource)
          .post(this, replicaUpdateEntity(resource, 4));
      JsonNode dryRunVerdict = OBJECT_MAPPER.readTree(dryRun.readEntity(String.class));
      Assert.assertFalse(dryRunVerdict.get("feasible").asBoolean());
      Assert.assertTrue(
          dryRunVerdict.toString().contains(IdealStateRebalanceFeasibilityGuardrailRule.RULE_ID));
      Assert.assertEquals(
          _gSetupTool.getClusterManagementTool().getResourceIdealState(cluster, resource)
              .getReplicas(), String.valueOf(replica), "dryRun must not write");

      // 3. A legitimate replica decrease (3 -> 2) is placeable, so the guard rail must allow it.
      new JerseyUriRequestBuilder(endpoint).format(cluster, resource)
          .post(this, replicaUpdateEntity(resource, 2));
      Assert.assertEquals(
          _gSetupTool.getClusterManagementTool().getResourceIdealState(cluster, resource)
              .getReplicas(), "2", "a feasible replica decrease should be written");

      // 4. force: an operator override bypasses the verdict and writes the (infeasible) increase.
      new JerseyUriRequestBuilder(endpoint + "&force=true").format(cluster, resource)
          .post(this, replicaUpdateEntity(resource, 4));
      Assert.assertEquals(
          _gSetupTool.getClusterManagementTool().getResourceIdealState(cluster, resource)
              .getReplicas(), "4", "force=true should write the replica increase");
    } finally {
      deleteTestCluster(cluster);
    }
    System.out.println("End test :" + TestHelper.getTestMethodName());
  }

  /**
   * Guard rail regression: repointing a WAGED resource's {@code STATE_MODEL_DEF_REF} at a state model
   * absent from the cluster's STATEMODELDEFS must produce exactly ONE verdict violation &mdash; the
   * clear {@code STATE_MODEL_DEF_NOT_FOUND} from {@link StateModelDefExistenceGuardrailRule} &mdash;
   * and NOT a second, misleading {@code IDEAL_STATE_REBALANCE_FEASIBILITY} violation. The WAGED
   * what-if cannot resolve the missing state model, so before
   * {@link IdealStateRebalanceFeasibilityGuardrailRule}'s short-circuit it surfaced a capacity
   * violation whose remedy (lower replicas / add capacity) is wrong for a missing state model, which
   * only drops that one resource. Uses a self-contained, converged WAGED cluster so the feasibility
   * what-if's baseline is computable and it would actually run were it not short-circuited.
   */
  @Test(dependsOnMethods = "testUpdateIdealStateRebalanceFeasibilityGuardrail")
  public void testUpdateWagedIdealStateMissingStateModelSingleViolation() throws Exception {
    System.out.println("Start test :" + TestHelper.getTestMethodName());
    String cluster = "TestClusterWagedStateModelSingleViolation";
    String resource = "TestDB_WAGED_SMD";
    String capacityKey = "CU";
    String missingStateModel = "NonExistentStateModel";
    int numNodes = 3;
    int numPartitions = 3;
    int replica = 3;
    try {
      _gSetupTool.addCluster(cluster, true);
      ClusterConfig clusterConfig = _configAccessor.getClusterConfig(cluster);
      clusterConfig.setInstanceCapacityKeys(Collections.singletonList(capacityKey));
      clusterConfig.setDefaultInstanceCapacityMap(Collections.singletonMap(capacityKey, 100));
      clusterConfig.setDefaultPartitionWeightMap(Collections.singletonMap(capacityKey, 1));
      _configAccessor.setClusterConfig(cluster, clusterConfig);

      for (int i = 0; i < numNodes; i++) {
        String instance = cluster + "_localhost_" + (14200 + i);
        _gSetupTool.addInstanceToCluster(cluster, instance);
        MockParticipantManager participant =
            new MockParticipantManager(ZK_ADDR, cluster, instance);
        participant.syncStart();
        _mockParticipantManagers.add(participant);
      }
      _clusterControllerManagers.add(startController(cluster));

      _gSetupTool.addResourceToCluster(cluster, resource, numPartitions, "MasterSlave",
          IdealState.RebalanceMode.FULL_AUTO.toString(), null);
      IdealState idealState =
          _gSetupTool.getClusterManagementTool().getResourceIdealState(cluster, resource);
      idealState.setMinActiveReplicas(1);
      idealState.setRebalancerClassName(WagedRebalancer.class.getName());
      _gSetupTool.getClusterManagementTool().setResourceIdealState(cluster, resource, idealState);
      _gSetupTool.rebalanceStorageCluster(cluster, resource, replica);

      try (BestPossibleExternalViewVerifier verifier =
          new BestPossibleExternalViewVerifier.Builder(cluster).setZkAddr(ZK_ADDR).build()) {
        Assert.assertTrue(verifier.verifyByPolling(),
            "cluster should converge before exercising the guard rail");
      }

      String endpoint = "clusters/{}/resources/{}/idealState?command=update";

      // 1. Enforcement: repointing to a missing state model is blocked with a 400 infeasible verdict
      //    that names STATE_MODEL_DEF_NOT_FOUND and NOT IDEAL_STATE_REBALANCE_FEASIBILITY -- a single
      //    violation, because the feasibility rule defers to the existence rule instead of adding a
      //    second, misleading capacity violation.
      Response blocked = new JerseyUriRequestBuilder(endpoint)
          .expectedReturnStatusCode(Response.Status.BAD_REQUEST.getStatusCode())
          .format(cluster, resource)
          .post(this, stateModelUpdateEntity(resource, missingStateModel));
      String blockedBody = blocked.readEntity(String.class);
      Assert.assertTrue(blockedBody.contains(StateModelDefExistenceGuardrailRule.RULE_ID),
          "verdict should name the state-model existence rule: " + blockedBody);
      Assert.assertFalse(blockedBody.contains(IdealStateRebalanceFeasibilityGuardrailRule.RULE_ID),
          "verdict must not carry a misleading feasibility violation: " + blockedBody);
      Assert.assertEquals(
          _gSetupTool.getClusterManagementTool().getResourceIdealState(cluster, resource)
              .getStateModelDefRef(), "MasterSlave", "a blocked update must not be written");

      // 2. dryRun reports the same single-violation verdict (200) without writing.
      Response dryRun = new JerseyUriRequestBuilder(endpoint + "&dryRun=true")
          .format(cluster, resource).post(this, stateModelUpdateEntity(resource, missingStateModel));
      JsonNode dryRunVerdict = OBJECT_MAPPER.readTree(dryRun.readEntity(String.class));
      Assert.assertFalse(dryRunVerdict.get("feasible").asBoolean());
      Assert.assertTrue(
          dryRunVerdict.toString().contains(StateModelDefExistenceGuardrailRule.RULE_ID));
      Assert.assertFalse(
          dryRunVerdict.toString().contains(IdealStateRebalanceFeasibilityGuardrailRule.RULE_ID),
          "dryRun verdict must not carry a misleading feasibility violation");
      Assert.assertEquals(
          _gSetupTool.getClusterManagementTool().getResourceIdealState(cluster, resource)
              .getStateModelDefRef(), "MasterSlave", "dryRun must not write");
    } finally {
      deleteTestCluster(cluster);
    }
    System.out.println("End test :" + TestHelper.getTestMethodName());
  }

  // An ideal-state "update" body carrying only a new REPLICAS count; the endpoint merges it into the
  // existing (WAGED) ideal state, so the guard rail sees the proposed replica count.
  private static Entity replicaUpdateEntity(String resource, int replicas) throws IOException {
    IdealState update = new IdealState(resource);
    update.setReplicas(String.valueOf(replicas));
    return Entity.entity(OBJECT_MAPPER.writeValueAsString(update.getRecord()),
        MediaType.APPLICATION_JSON_TYPE);
  }

  // An ideal-state "update" body carrying only a new STATE_MODEL_DEF_REF; the endpoint merges it into
  // the existing (WAGED) ideal state, so the guard rails see the proposed state model.
  private static Entity stateModelUpdateEntity(String resource, String stateModelDefRef)
      throws IOException {
    IdealState update = new IdealState(resource);
    update.setStateModelDefRef(stateModelDefRef);
    return Entity.entity(OBJECT_MAPPER.writeValueAsString(update.getRecord()),
        MediaType.APPLICATION_JSON_TYPE);
  }

  private Response postEnableWagedRebalance(String resourceName, Map<String, Object> flags) {
    WebTarget webTarget = target("clusters/" + CLUSTER_NAME + "/resources/" + resourceName)
        .queryParam("command", "enableWagedRebalance");
    for (Map.Entry<String, Object> flag : flags.entrySet()) {
      webTarget = webTarget.queryParam(flag.getKey(), flag.getValue());
    }
    return webTarget.request().post(Entity.entity(null, MediaType.APPLICATION_JSON_TYPE));
  }

  private boolean isWagedRebalanced(String resourceName) {
    IdealState idealState =
        _gSetupTool.getClusterManagementTool().getResourceIdealState(CLUSTER_NAME, resourceName);
    return WagedRebalancer.class.getName().equals(idealState.getRebalancerClassName());
  }

  private Response putWagedResource(String resourceName, ResourceConfig resourceConfig,
      Map<String, Object> flags) throws IOException {
    IdealState idealState = new IdealState(resourceName);
    idealState.getRecord().getSimpleFields().putAll(_gSetupTool.getClusterManagementTool()
        .getResourceIdealState(CLUSTER_NAME, RESOURCE_NAME).getRecord().getSimpleFields());
    idealState.setRebalanceMode(IdealState.RebalanceMode.FULL_AUTO);
    idealState.setRebalancerClassName(WagedRebalancer.class.getName());
    idealState.setNumPartitions(1);

    Map<String, ZNRecord> inputMap = ImmutableMap.of(
        ResourceAccessor.ResourceProperties.idealState.name(), idealState.getRecord(),
        ResourceAccessor.ResourceProperties.resourceConfig.name(), resourceConfig.getRecord());
    Entity entity =
        Entity.entity(OBJECT_MAPPER.writeValueAsString(inputMap), MediaType.APPLICATION_JSON_TYPE);

    WebTarget webTarget = target("clusters/" + CLUSTER_NAME + "/resources/" + resourceName)
        .queryParam("command", "addWagedResource");
    for (Map.Entry<String, Object> flag : flags.entrySet()) {
      webTarget = webTarget.queryParam(flag.getKey(), flag.getValue());
    }
    return webTarget.request().put(entity);
  }

  private static ResourceConfig wagedResourceConfig(String resourceName,
      Map<String, Map<String, Integer>> partitionWeights) throws IOException {
    ResourceConfig resourceConfig = new ResourceConfig(resourceName);
    resourceConfig.setPartitionCapacityMap(partitionWeights);
    return resourceConfig;
  }

  private static ResourceConfig rawWagedResourceConfig(String resourceName,
      Map<String, Map<String, Integer>> partitionWeights) throws IOException {
    // Build PARTITION_CAPACITY_MAP directly on the record, bypassing
    // ResourceConfig#setPartitionCapacityMap so values it would reject (e.g. negatives) can be
    // exercised through the raw-ZNRecord path the endpoint actually uses.
    ResourceConfig resourceConfig = new ResourceConfig(resourceName);
    Map<String, String> rawCapacityRecord = new HashMap<>();
    for (Map.Entry<String, Map<String, Integer>> entry : partitionWeights.entrySet()) {
      rawCapacityRecord.put(entry.getKey(), OBJECT_MAPPER.writeValueAsString(entry.getValue()));
    }
    resourceConfig.getRecord().setMapField(
        ResourceConfig.ResourceConfigProperty.PARTITION_CAPACITY_MAP.name(), rawCapacityRecord);
    return resourceConfig;
  }

  /**
   * Structural checks (IdealState/ResourceConfig name match, non-negative weights) run before the
   * guard rail, so a dry-run reflects them and a structurally invalid request never reaches ZK.
   * Capacity configuration is saved and restored so this test does not perturb the other resource
   * tests that share {@value #CLUSTER_NAME}.
   */
  @Test(dependsOnMethods = "testAddResourceWithWeight")
  public void testWagedStructuralChecksAppliedBeforeGuardrail() throws Exception {
    System.out.println("Start test :" + TestHelper.getTestMethodName());

    ClusterConfig clusterConfig = _configAccessor.getClusterConfig(CLUSTER_NAME);
    List<String> originalCapacityKeys = clusterConfig.getInstanceCapacityKeys();
    List<String> instances =
        _gSetupTool.getClusterManagementTool().getInstancesInCluster(CLUSTER_NAME);
    Map<String, Map<String, Integer>> originalInstanceCapacities = new HashMap<>();
    for (String instance : instances) {
      originalInstanceCapacities.put(instance,
          _configAccessor.getInstanceConfig(CLUSTER_NAME, instance).getInstanceCapacityMap());
    }

    String resourceName = "structuralCheckResource";
    try {
      clusterConfig.setInstanceCapacityKeys(Arrays.asList("FOO", "BAR"));
      _configAccessor.setClusterConfig(CLUSTER_NAME, clusterConfig);
      Map<String, Integer> instanceCapacity = ImmutableMap.of("FOO", 100, "BAR", 100);
      for (String instance : instances) {
        InstanceConfig instanceConfig = _configAccessor.getInstanceConfig(CLUSTER_NAME, instance);
        instanceConfig.setInstanceCapacityMap(instanceCapacity);
        _configAccessor.setInstanceConfig(CLUSTER_NAME, instance, instanceConfig);
      }

      // 1) Name mismatch is a structural failure. Even a dry-run must report it (400) instead of
      // returning a feasible verdict for a request that would then fail for real.
      Map<String, Map<String, Integer>> withinCapacity = ImmutableMap.of(
          ResourceConfig.DEFAULT_PARTITION_KEY, ImmutableMap.of("FOO", 100, "BAR", 100));
      Response mismatchDryRun = putWagedResource(resourceName,
          wagedResourceConfig("someOtherName", withinCapacity), ImmutableMap.of("dryRun", true));
      Assert.assertEquals(mismatchDryRun.getStatus(), Response.Status.BAD_REQUEST.getStatusCode());
      Assert.assertFalse(_gSetupTool.getClusterManagementTool().getResourcesInCluster(CLUSTER_NAME)
          .contains(resourceName));

      // 2) Negative weights are rejected (400) even though they do not exceed capacity, and even
      // though the endpoint builds the ResourceConfig from a raw ZNRecord that bypasses
      // ResourceConfig#setPartitionCapacityMap's own negative check.
      Response negative = putWagedResource(resourceName,
          rawWagedResourceConfig(resourceName, ImmutableMap.of(
              ResourceConfig.DEFAULT_PARTITION_KEY, ImmutableMap.of("FOO", -5, "BAR", 100))),
          Collections.emptyMap());
      Assert.assertEquals(negative.getStatus(), Response.Status.BAD_REQUEST.getStatusCode());
      Assert.assertFalse(_gSetupTool.getClusterManagementTool().getResourcesInCluster(CLUSTER_NAME)
          .contains(resourceName));
    } finally {
      try {
        _gSetupTool.getClusterManagementTool().dropResource(CLUSTER_NAME, resourceName);
      } catch (Exception ignored) {
      }
      ClusterConfig restore = _configAccessor.getClusterConfig(CLUSTER_NAME);
      restore.setInstanceCapacityKeys(originalCapacityKeys);
      _configAccessor.setClusterConfig(CLUSTER_NAME, restore);
      for (String instance : instances) {
        InstanceConfig instanceConfig = _configAccessor.getInstanceConfig(CLUSTER_NAME, instance);
        instanceConfig.setInstanceCapacityMap(originalInstanceCapacities.get(instance));
        _configAccessor.setInstanceConfig(CLUSTER_NAME, instance, instanceConfig);
      }
    }
    System.out.println("End test :" + TestHelper.getTestMethodName());
  }

  /**
   * An ideal-state "update" that would leave MIN_ACTIVE_REPLICAS greater than REPLICAS is logically
   * impossible -- a partition can never have more active replicas than it has replicas -- and would
   * leave every partition perpetually below its minimum active count, defeating delayed rebalance
   * and min-active health guarantees. The guard rail validates the merged (post-write) ideal state.
   * This exercises the happy path (within bounds, written), enforcement (400 + verdict, unchanged),
   * dry-run (200 + verdict, unchanged) and force bypass (written). The resource is created and
   * dropped locally so it does not perturb the other tests that share {@value #CLUSTER_NAME}.
   */
  @Test(dependsOnMethods = "testAddResourceWithWeight")
  public void testUpdateResourceIdealStateMinActiveGuardrail() throws Exception {
    System.out.println("Start test :" + TestHelper.getTestMethodName());

    String resourceName = "minActiveGuardrailResource";
    try {
      // Baseline: a valid resource with REPLICAS=3 and MIN_ACTIVE_REPLICAS=2.
      _gSetupTool.addResourceToCluster(CLUSTER_NAME, resourceName, 1, "MasterSlave",
          IdealState.RebalanceMode.FULL_AUTO.name());
      IdealState baseline =
          _gSetupTool.getClusterManagementTool().getResourceIdealState(CLUSTER_NAME, resourceName);
      baseline.setReplicas("3");
      baseline.setMinActiveReplicas(2);
      _gSetupTool.getClusterManagementTool()
          .setResourceIdealState(CLUSTER_NAME, resourceName, baseline);

      // 1) Happy path: raising MIN_ACTIVE_REPLICAS to 3 stays within REPLICAS=3, so it is written.
      Response valid = postIdealStateUpdate(resourceName, minActiveUpdate(resourceName, 3),
          Collections.emptyMap());
      Assert.assertEquals(valid.getStatus(), Response.Status.OK.getStatusCode());
      Assert.assertEquals(currentMinActiveReplicas(resourceName), 3);

      // 2) Enforcement: raising MIN_ACTIVE_REPLICAS to 5 above REPLICAS=3 is blocked with a 400
      // infeasible verdict, and nothing is written (still 3).
      Response blocked = postIdealStateUpdate(resourceName, minActiveUpdate(resourceName, 5),
          Collections.emptyMap());
      Assert.assertEquals(blocked.getStatus(), Response.Status.BAD_REQUEST.getStatusCode());
      JsonNode blockedVerdict = OBJECT_MAPPER.readTree(blocked.readEntity(String.class));
      Assert.assertFalse(blockedVerdict.get("feasible").asBoolean());
      Assert.assertTrue(blockedVerdict.toString()
          .contains(MinActiveReplicasConsistencyGuardrailRule.RULE_ID));
      Assert.assertEquals(currentMinActiveReplicas(resourceName), 3);

      // 3) Dry-run reports the same infeasible verdict (200) without writing (still 3).
      Response dryRun = postIdealStateUpdate(resourceName, minActiveUpdate(resourceName, 5),
          ImmutableMap.of("dryRun", true));
      Assert.assertEquals(dryRun.getStatus(), Response.Status.OK.getStatusCode());
      JsonNode dryRunVerdict = OBJECT_MAPPER.readTree(dryRun.readEntity(String.class));
      Assert.assertFalse(dryRunVerdict.get("feasible").asBoolean());
      Assert.assertTrue(dryRunVerdict.toString()
          .contains(MinActiveReplicasConsistencyGuardrailRule.RULE_ID));
      Assert.assertEquals(currentMinActiveReplicas(resourceName), 3);

      // 4) force=true bypasses the guard rail: the inconsistent value is actually written.
      Response forced = postIdealStateUpdate(resourceName, minActiveUpdate(resourceName, 5),
          ImmutableMap.of("force", true));
      Assert.assertEquals(forced.getStatus(), Response.Status.OK.getStatusCode());
      Assert.assertEquals(currentMinActiveReplicas(resourceName), 5);
    } finally {
      try {
        _gSetupTool.getClusterManagementTool().dropResource(CLUSTER_NAME, resourceName);
      } catch (Exception ignored) {
      }
    }
    System.out.println("End test :" + TestHelper.getTestMethodName());
  }

  private int currentMinActiveReplicas(String resourceName) {
    return _gSetupTool.getClusterManagementTool().getResourceIdealState(CLUSTER_NAME, resourceName)
        .getMinActiveReplicas();
  }

  private static ZNRecord minActiveUpdate(String resourceName, int minActiveReplicas) {
    // A partial "update" record carrying only MIN_ACTIVE_REPLICAS; the endpoint merges it into the
    // existing ideal state, so REPLICAS is taken from what is already in ZK.
    IdealState update = new IdealState(resourceName);
    update.setMinActiveReplicas(minActiveReplicas);
    return update.getRecord();
  }

  private Response postIdealStateUpdate(String resourceName, ZNRecord record,
      Map<String, Object> flags) throws IOException {
    Entity entity =
        Entity.entity(OBJECT_MAPPER.writeValueAsString(record), MediaType.APPLICATION_JSON_TYPE);
    WebTarget webTarget =
        target("clusters/" + CLUSTER_NAME + "/resources/" + resourceName + "/idealState")
            .queryParam("command", "update");
    for (Map.Entry<String, Object> flag : flags.entrySet()) {
      webTarget = webTarget.queryParam(flag.getKey(), flag.getValue());
    }
    return webTarget.request().post(entity);
  }

  /**
   * Guard rail: adding a WAGED resource whose STATE_MODEL_DEF_REF names a state model absent from the
   * cluster's STATEMODELDEFS is rejected before the resource is written to ZooKeeper. The controller
   * drives every partition's transitions from that state model, so a resource referencing a missing
   * one is accepted but can never be brought online. The plain addResource admin path already rejects
   * this, but addResourceWithWeight does not, so the guard rail closes that gap. Verifies enforcement
   * (400 + verdict naming the rule, not created) and dry-run (200 + same verdict, not created); both
   * short-circuit before the write, so they isolate this rule from addResourceWithWeight's own WAGED
   * capacity validation. The valid-reference happy path and force bypass of this rule are covered by
   * {@link #testUpdateResourceIdealStateStateModelDefGuardrail}, {@link #testAddResourceWithWeight}
   * and the unit tests.
   */
  @Test(dependsOnMethods = "testAddResourceWithWeight")
  public void testAddWagedResourceStateModelDefGuardrail() throws Exception {
    System.out.println("Start test :" + TestHelper.getTestMethodName());

    Map<String, Map<String, Integer>> withinCapacity = ImmutableMap.of(
        ResourceConfig.DEFAULT_PARTITION_KEY, ImmutableMap.of("FOO", 100, "BAR", 100));
    String missingStateModel = "NonExistentStateModel";
    String blockedResource = "stateModelDefBlockedWagedResource";
    try {
      // 1) Enforcement: a WAGED add whose STATE_MODEL_DEF_REF is absent from STATEMODELDEFS is
      // blocked with a 400 infeasible verdict naming the rule, before anything is written.
      Response blocked = putWagedResourceWithStateModel(blockedResource, missingStateModel,
          wagedResourceConfig(blockedResource, withinCapacity), Collections.emptyMap());
      Assert.assertEquals(blocked.getStatus(), Response.Status.BAD_REQUEST.getStatusCode());
      JsonNode blockedVerdict = OBJECT_MAPPER.readTree(blocked.readEntity(String.class));
      Assert.assertFalse(blockedVerdict.get("feasible").asBoolean());
      Assert.assertTrue(blockedVerdict.toString()
          .contains(StateModelDefExistenceGuardrailRule.RULE_ID));
      Assert.assertFalse(_gSetupTool.getClusterManagementTool().getResourcesInCluster(CLUSTER_NAME)
          .contains(blockedResource));

      // 2) Dry-run reports the same infeasible verdict (200) without writing.
      Response dryRun = putWagedResourceWithStateModel(blockedResource, missingStateModel,
          wagedResourceConfig(blockedResource, withinCapacity), ImmutableMap.of("dryRun", true));
      Assert.assertEquals(dryRun.getStatus(), Response.Status.OK.getStatusCode());
      JsonNode dryRunVerdict = OBJECT_MAPPER.readTree(dryRun.readEntity(String.class));
      Assert.assertFalse(dryRunVerdict.get("feasible").asBoolean());
      Assert.assertTrue(dryRunVerdict.toString()
          .contains(StateModelDefExistenceGuardrailRule.RULE_ID));
      Assert.assertFalse(_gSetupTool.getClusterManagementTool().getResourcesInCluster(CLUSTER_NAME)
          .contains(blockedResource));
    } finally {
      try {
        _gSetupTool.getClusterManagementTool().dropResource(CLUSTER_NAME, blockedResource);
      } catch (Exception ignored) {
      }
    }
    System.out.println("End test :" + TestHelper.getTestMethodName());
  }

  /**
   * Guard rail: an ideal-state "update" that repoints STATE_MODEL_DEF_REF at a state model absent
   * from the cluster's STATEMODELDEFS is rejected before the merged ideal state is written. The
   * controller cannot compute any transition for a resource whose state model is missing, so it would
   * be accepted but never come online; updateIdealState does not validate this. The guard rail
   * validates the merged (post-write) ideal state and, like the other ideal-state rules, fires only
   * on an edit that introduces the hazard (a pre-existing missing reference is grandfathered).
   * Exercises the happy path (repoint to another existing model, written), enforcement (400 +
   * verdict, unchanged), dry-run (200 + verdict, unchanged) and force bypass (written). The resource
   * is created and dropped locally so it does not perturb the other tests that share
   * {@value #CLUSTER_NAME}.
   */
  @Test(dependsOnMethods = "testAddResourceWithWeight")
  public void testUpdateResourceIdealStateStateModelDefGuardrail() throws Exception {
    System.out.println("Start test :" + TestHelper.getTestMethodName());

    String resourceName = "stateModelDefGuardrailResource";
    String missingStateModel = "NonExistentStateModel";
    try {
      // Baseline: a valid resource on the MasterSlave state model.
      _gSetupTool.addResourceToCluster(CLUSTER_NAME, resourceName, 1, "MasterSlave",
          IdealState.RebalanceMode.FULL_AUTO.name());
      Assert.assertEquals(currentStateModelDefRef(resourceName), "MasterSlave");

      // 1) Happy path: repointing to another state model that exists (LeaderStandby) is written.
      Response valid = postIdealStateUpdate(resourceName,
          stateModelDefUpdate(resourceName, "LeaderStandby"), Collections.emptyMap());
      Assert.assertEquals(valid.getStatus(), Response.Status.OK.getStatusCode());
      Assert.assertEquals(currentStateModelDefRef(resourceName), "LeaderStandby");

      // 2) Enforcement: repointing to a state model absent from STATEMODELDEFS is blocked with a 400
      // infeasible verdict, and nothing is written (still LeaderStandby).
      Response blocked = postIdealStateUpdate(resourceName,
          stateModelDefUpdate(resourceName, missingStateModel), Collections.emptyMap());
      Assert.assertEquals(blocked.getStatus(), Response.Status.BAD_REQUEST.getStatusCode());
      JsonNode blockedVerdict = OBJECT_MAPPER.readTree(blocked.readEntity(String.class));
      Assert.assertFalse(blockedVerdict.get("feasible").asBoolean());
      Assert.assertTrue(blockedVerdict.toString()
          .contains(StateModelDefExistenceGuardrailRule.RULE_ID));
      Assert.assertEquals(currentStateModelDefRef(resourceName), "LeaderStandby");

      // 3) Dry-run reports the same infeasible verdict (200) without writing (still LeaderStandby).
      Response dryRun = postIdealStateUpdate(resourceName,
          stateModelDefUpdate(resourceName, missingStateModel), ImmutableMap.of("dryRun", true));
      Assert.assertEquals(dryRun.getStatus(), Response.Status.OK.getStatusCode());
      JsonNode dryRunVerdict = OBJECT_MAPPER.readTree(dryRun.readEntity(String.class));
      Assert.assertFalse(dryRunVerdict.get("feasible").asBoolean());
      Assert.assertTrue(dryRunVerdict.toString()
          .contains(StateModelDefExistenceGuardrailRule.RULE_ID));
      Assert.assertEquals(currentStateModelDefRef(resourceName), "LeaderStandby");

      // 4) force=true bypasses the guard rail: the missing reference is actually written.
      Response forced = postIdealStateUpdate(resourceName,
          stateModelDefUpdate(resourceName, missingStateModel), ImmutableMap.of("force", true));
      Assert.assertEquals(forced.getStatus(), Response.Status.OK.getStatusCode());
      Assert.assertEquals(currentStateModelDefRef(resourceName), missingStateModel);
    } finally {
      try {
        _gSetupTool.getClusterManagementTool().dropResource(CLUSTER_NAME, resourceName);
      } catch (Exception ignored) {
      }
    }
    System.out.println("End test :" + TestHelper.getTestMethodName());
  }

  private String currentStateModelDefRef(String resourceName) {
    return _gSetupTool.getClusterManagementTool().getResourceIdealState(CLUSTER_NAME, resourceName)
        .getStateModelDefRef();
  }

  private static ZNRecord stateModelDefUpdate(String resourceName, String stateModelDefRef) {
    // A partial "update" record carrying only STATE_MODEL_DEF_REF; the endpoint merges it into the
    // existing ideal state, so the other fields are taken from what is already in ZK.
    IdealState update = new IdealState(resourceName);
    update.setStateModelDefRef(stateModelDefRef);
    return update.getRecord();
  }

  private Response putWagedResourceWithStateModel(String resourceName,
      String stateModelDefRefOverride, ResourceConfig resourceConfig, Map<String, Object> flags)
      throws IOException {
    IdealState idealState = new IdealState(resourceName);
    idealState.getRecord().getSimpleFields().putAll(_gSetupTool.getClusterManagementTool()
        .getResourceIdealState(CLUSTER_NAME, RESOURCE_NAME).getRecord().getSimpleFields());
    idealState.setRebalanceMode(IdealState.RebalanceMode.FULL_AUTO);
    idealState.setRebalancerClassName(WagedRebalancer.class.getName());
    idealState.setNumPartitions(1);
    if (stateModelDefRefOverride != null) {
      idealState.setStateModelDefRef(stateModelDefRefOverride);
    }
    Map<String, ZNRecord> inputMap = ImmutableMap.of(
        ResourceAccessor.ResourceProperties.idealState.name(), idealState.getRecord(),
        ResourceAccessor.ResourceProperties.resourceConfig.name(), resourceConfig.getRecord());
    Entity entity =
        Entity.entity(OBJECT_MAPPER.writeValueAsString(inputMap), MediaType.APPLICATION_JSON_TYPE);
    WebTarget webTarget = target("clusters/" + CLUSTER_NAME + "/resources/" + resourceName)
        .queryParam("command", "addWagedResource");
    for (Map.Entry<String, Object> flag : flags.entrySet()) {
      webTarget = webTarget.queryParam(flag.getKey(), flag.getValue());
    }
    return webTarget.request().put(entity);
  }

  /**
   * An ideal-state "update" that leaves a resource in USER_DEFINED mode with a REBALANCER_CLASS_NAME
   * that cannot be resolved to a {@link org.apache.helix.controller.rebalancer.Rebalancer} is
   * unplaceable: the controller loads that class to drive the resource and, when it cannot be
   * resolved, computes no assignment, so the resource is accepted into ZooKeeper but never comes
   * online. The guard rail validates the merged (post-write) ideal state and rejects such a write.
   * This also proves the rule is actually wired into the updateResourceIdealState pipeline (the unit
   * tests cover the rule in isolation). It exercises the happy path (switch to USER_DEFINED with a
   * loadable rebalancer, written), enforcement (400 + verdict naming the rule, unchanged), dry-run
   * (200 + verdict, unchanged) and force bypass (written). The resource is created and dropped
   * locally so it does not perturb the other tests that share {@value #CLUSTER_NAME}.
   */
  @Test(dependsOnMethods = "testAddResourceWithWeight")
  public void testUpdateResourceIdealStateUserDefinedRebalancerGuardrail() throws Exception {
    System.out.println("Start test :" + TestHelper.getTestMethodName());

    String resourceName = "userDefinedRebalancerGuardrailResource";
    String validRebalancer = SemiAutoRebalancer.class.getName();
    String unloadableRebalancer = "com.example.NoSuchRebalancer";
    try {
      // Baseline: a valid FULL_AUTO resource (not yet USER_DEFINED).
      _gSetupTool.addResourceToCluster(CLUSTER_NAME, resourceName, 1, "MasterSlave",
          IdealState.RebalanceMode.FULL_AUTO.name());

      // 1) Happy path: switching to USER_DEFINED with a loadable Rebalancer is written.
      Response valid = postIdealStateUpdate(resourceName,
          userDefinedUpdate(resourceName, validRebalancer), Collections.emptyMap());
      Assert.assertEquals(valid.getStatus(), Response.Status.OK.getStatusCode());
      Assert.assertEquals(currentRebalancerClassName(resourceName), validRebalancer);

      // 2) Enforcement: repointing REBALANCER_CLASS_NAME at an unloadable class is blocked with a 400
      // infeasible verdict naming the rule, and nothing is written (still the valid rebalancer).
      Response blocked = postIdealStateUpdate(resourceName,
          userDefinedUpdate(resourceName, unloadableRebalancer), Collections.emptyMap());
      Assert.assertEquals(blocked.getStatus(), Response.Status.BAD_REQUEST.getStatusCode());
      JsonNode blockedVerdict = OBJECT_MAPPER.readTree(blocked.readEntity(String.class));
      Assert.assertFalse(blockedVerdict.get("feasible").asBoolean());
      Assert.assertTrue(blockedVerdict.toString()
          .contains(UserDefinedRebalancerClassGuardrailRule.RULE_ID));
      Assert.assertEquals(currentRebalancerClassName(resourceName), validRebalancer);

      // 3) Dry-run reports the same infeasible verdict (200) without writing.
      Response dryRun = postIdealStateUpdate(resourceName,
          userDefinedUpdate(resourceName, unloadableRebalancer), ImmutableMap.of("dryRun", true));
      Assert.assertEquals(dryRun.getStatus(), Response.Status.OK.getStatusCode());
      JsonNode dryRunVerdict = OBJECT_MAPPER.readTree(dryRun.readEntity(String.class));
      Assert.assertFalse(dryRunVerdict.get("feasible").asBoolean());
      Assert.assertTrue(dryRunVerdict.toString()
          .contains(UserDefinedRebalancerClassGuardrailRule.RULE_ID));
      Assert.assertEquals(currentRebalancerClassName(resourceName), validRebalancer);

      // 4) force=true bypasses the guard rail: the unresolvable class is actually written.
      Response forced = postIdealStateUpdate(resourceName,
          userDefinedUpdate(resourceName, unloadableRebalancer), ImmutableMap.of("force", true));
      Assert.assertEquals(forced.getStatus(), Response.Status.OK.getStatusCode());
      Assert.assertEquals(currentRebalancerClassName(resourceName), unloadableRebalancer);
    } finally {
      try {
        _gSetupTool.getClusterManagementTool().dropResource(CLUSTER_NAME, resourceName);
      } catch (Exception ignored) {
      }
    }
    System.out.println("End test :" + TestHelper.getTestMethodName());
  }

  private String currentRebalancerClassName(String resourceName) {
    return _gSetupTool.getClusterManagementTool().getResourceIdealState(CLUSTER_NAME, resourceName)
        .getRebalancerClassName();
  }

  private static ZNRecord userDefinedUpdate(String resourceName, String rebalancerClassName) {
    // A partial "update" record switching the resource to USER_DEFINED with the given
    // REBALANCER_CLASS_NAME; the endpoint merges it into the existing ideal state.
    IdealState update = new IdealState(resourceName);
    update.setRebalanceMode(IdealState.RebalanceMode.USER_DEFINED);
    update.setRebalancerClassName(rebalancerClassName);
    return update.getRecord();
  }

  @Test(dependsOnMethods = "testAddResourceWithWeight")
  public void testValidateResource() throws IOException {
    // Define weight keys in ClusterConfig
    ClusterConfig clusterConfig = _configAccessor.getClusterConfig(CLUSTER_NAME);
    clusterConfig.setInstanceCapacityKeys(Arrays.asList("FOO", "BAR"));
    _configAccessor.setClusterConfig(CLUSTER_NAME, clusterConfig);

    // Remove all weight configs in InstanceConfig for testing
    for (String instance : _gSetupTool.getClusterManagementTool().getInstancesInCluster(CLUSTER_NAME)) {
      InstanceConfig instanceConfig = _configAccessor.getInstanceConfig(CLUSTER_NAME, instance);
      instanceConfig.setInstanceCapacityMap(Collections.emptyMap());
      _configAccessor.setInstanceConfig(CLUSTER_NAME, instance, instanceConfig);
    }

    // Validate the resource added in testAddResourceWithWeight()
    String resourceToValidate = "newWagedResource";
    // This should fail because none of the instances have weight configured
    get("clusters/" + CLUSTER_NAME + "/resources/" + resourceToValidate,
        ImmutableMap.of("command", "validateWeight"), Response.Status.BAD_REQUEST.getStatusCode(),
        true);

    // Add back weight configurations to all instance configs
    Map<String, Integer> instanceCapacityMap = ImmutableMap.of("FOO", 1000, "BAR", 1000);
    for (String instance : _gSetupTool.getClusterManagementTool().getInstancesInCluster(CLUSTER_NAME)) {
      InstanceConfig instanceConfig = _configAccessor.getInstanceConfig(CLUSTER_NAME, instance);
      instanceConfig.setInstanceCapacityMap(instanceCapacityMap);
      _configAccessor.setInstanceConfig(CLUSTER_NAME, instance, instanceConfig);
    }

    // Now try validating again - it should go through and return a 200
    String body = get("clusters/" + CLUSTER_NAME + "/resources/" + resourceToValidate,
        ImmutableMap.of("command", "validateWeight"), Response.Status.OK.getStatusCode(), true);
    JsonNode node = OBJECT_MAPPER.readTree(body);
    Assert.assertEquals(node.get(resourceToValidate).toString(), "true");
  }

  /**
   * Verifies the {@link ResourceInUseGuardrailRule} wired into the resource-delete path:
   * a resource whose external view still has serving (non-OFFLINE, non-DROPPED) replicas cannot be
   * dropped unless the caller forces it, while a resource that is empty, all-DROPPED, or drained to
   * all-OFFLINE drops normally. The admin drop path performs no such safety check, so this preflight
   * is the only gate.
   */
  @Test(dependsOnMethods = "testResourceHealth")
  public void testDeleteResourceInUseGuardrail() throws Exception {
    System.out.println("Start test :" + TestHelper.getTestMethodName());
    String clusterName = "TestCluster_1";
    String idleResource = clusterName + "_db_delguard_idle";
    String offlineResource = clusterName + "_db_delguard_offline";
    String inUseResource = clusterName + "_db_delguard_inuse";

    Map<String, String> idealStateParams = new HashMap<>();
    idealStateParams.put("MinActiveReplicas", "2");
    idealStateParams.put("StateModelDefRef", "MasterSlave");
    idealStateParams.put("MaxPartitionsPerInstance", "3");
    idealStateParams.put("Replicas", "3");
    idealStateParams.put("NumPartitions", "3");

    // Disable the cluster so the controller neither removes the external views created below nor
    // places new replicas while the guard rail is being exercised.
    _gSetupTool.getClusterManagementTool().enableCluster(clusterName, false);
    try {
      // A resource whose every replica is DROPPED is not in use: the drop is certified feasible.
      Map<String, List<String>> idleStates = new LinkedHashMap<>();
      idleStates.put("p0", Arrays.asList("DROPPED", "DROPPED", "DROPPED"));
      createDummyMapping(clusterName, idleResource, idealStateParams, idleStates);
      Assert.assertTrue(_gSetupTool.getClusterManagementTool().getResourcesInCluster(clusterName)
          .contains(idleResource));

      delete("clusters/" + clusterName + "/resources/" + idleResource, Collections.emptyMap(),
          Response.Status.OK.getStatusCode());
      Assert.assertFalse(_gSetupTool.getClusterManagementTool().getResourcesInCluster(clusterName)
          .contains(idleResource), "A resource with no placed replicas should drop normally");

      // A resource whose replicas have all drained to OFFLINE (e.g. it was disabled) is not in use:
      // the standard "disable, let it drain, then drop" path is certified feasible without force.
      Map<String, List<String>> offlineStates = new LinkedHashMap<>();
      offlineStates.put("p0", Arrays.asList("OFFLINE", "OFFLINE", "OFFLINE"));
      createDummyMapping(clusterName, offlineResource, idealStateParams, offlineStates);
      delete("clusters/" + clusterName + "/resources/" + offlineResource, Collections.emptyMap(),
          Response.Status.OK.getStatusCode());
      Assert.assertFalse(_gSetupTool.getClusterManagementTool().getResourcesInCluster(clusterName)
          .contains(offlineResource),
          "A drained (all-OFFLINE) resource should drop without force");

      // A resource with placed (non-DROPPED) replicas is in use.
      Map<String, List<String>> inUseStates = new LinkedHashMap<>();
      inUseStates.put("p0", Arrays.asList("MASTER", "SLAVE", "SLAVE"));
      createDummyMapping(clusterName, inUseResource, idealStateParams, inUseStates);

      // 1) Enforcement: blocked with 400 + an infeasible verdict naming the rule; nothing dropped.
      Response blocked = delete("clusters/" + clusterName + "/resources/" + inUseResource,
          Collections.emptyMap(), Response.Status.BAD_REQUEST.getStatusCode());
      JsonNode blockedVerdict = OBJECT_MAPPER.readTree(blocked.readEntity(String.class));
      Assert.assertFalse(blockedVerdict.get("feasible").asBoolean());
      Assert.assertTrue(blockedVerdict.toString().contains(ResourceInUseGuardrailRule.RULE_ID));
      Assert.assertTrue(_gSetupTool.getClusterManagementTool().getResourcesInCluster(clusterName)
          .contains(inUseResource),
          "An in-use resource must not be dropped when the guard rail blocks the delete");

      // 2) Dry-run: 200 with the same infeasible verdict, still nothing dropped.
      Response dryRun = delete("clusters/" + clusterName + "/resources/" + inUseResource,
          ImmutableMap.of("dryRun", "true"), Response.Status.OK.getStatusCode());
      JsonNode dryRunVerdict = OBJECT_MAPPER.readTree(dryRun.readEntity(String.class));
      Assert.assertFalse(dryRunVerdict.get("feasible").asBoolean());
      Assert.assertTrue(_gSetupTool.getClusterManagementTool().getResourcesInCluster(clusterName)
          .contains(inUseResource), "dryRun must not drop the resource");

      // 3) force=true overrides the guard rail: the in-use resource is actually dropped.
      delete("clusters/" + clusterName + "/resources/" + inUseResource,
          ImmutableMap.of("force", "true"), Response.Status.OK.getStatusCode());
      Assert.assertFalse(_gSetupTool.getClusterManagementTool().getResourcesInCluster(clusterName)
          .contains(inUseResource), "force=true must override the guard rail and drop the resource");
    } finally {
      for (String resource : Arrays.asList(idleResource, offlineResource, inUseResource)) {
        try {
          _gSetupTool.getClusterManagementTool().dropResource(clusterName, resource);
        } catch (Exception ignored) {
        }
      }
      _gSetupTool.getClusterManagementTool().enableCluster(clusterName, true);
    }
    System.out.println("End test :" + TestHelper.getTestMethodName());
  }

  /**
   * Creates a setup where the health API can be tested.
   * @param clusterName
   * @param resourceName
   * @param idealStateParams
   * @param partitionReplicaStates maps partitionName to its replicas' states
   * @throws Exception
   */
  private void createDummyMapping(String clusterName, String resourceName,
      Map<String, String> idealStateParams, Map<String, List<String>> partitionReplicaStates)
      throws Exception {
    IdealState idealState = new IdealState(resourceName);
    idealState.setMinActiveReplicas(Integer.parseInt(idealStateParams.get("MinActiveReplicas"))); // 2
    idealState.setStateModelDefRef(idealStateParams.get("StateModelDefRef")); // MasterSlave
    idealState.setMaxPartitionsPerInstance(
        Integer.parseInt(idealStateParams.get("MaxPartitionsPerInstance"))); // 3
    idealState.setReplicas(idealStateParams.get("Replicas")); // 3
    idealState.setNumPartitions(Integer.parseInt(idealStateParams.get("NumPartitions"))); // 3
    idealState.enable(false);

    Map<String, List<String>> partitionNames = new LinkedHashMap<>();
    List<String> dummyPrefList = new ArrayList<>();

    for (int i = 0; i < Integer.parseInt(idealStateParams.get("MaxPartitionsPerInstance")); i++) {
      dummyPrefList.add(ANY_INSTANCE);
      partitionNames.put("p" + i, dummyPrefList);
    }
    idealState.getRecord().getListFields().putAll(partitionNames);

    if (!_gSetupTool.getClusterManagementTool().getClusters().contains(clusterName)) {
      _gSetupTool.getClusterManagementTool().addCluster(clusterName);
    }
    _gSetupTool.getClusterManagementTool().setResourceIdealState(clusterName, resourceName,
        idealState);

    // Set ExternalView's replica states for a given parameter map
    ExternalView externalView = new ExternalView(resourceName);

    Map<String, Map<String, String>> mappingCurrent = new LinkedHashMap<>();

    List<String> partitionReplicaStatesList = new ArrayList<>(partitionReplicaStates.keySet());
    for (int k = 0; k < partitionReplicaStatesList.size(); k++) {
      Map<String, String> replicaStatesForPartition = new LinkedHashMap<>();
      List<String> replicaStateList = partitionReplicaStates.get(partitionReplicaStatesList.get(k));
      for (int i = 0; i < replicaStateList.size(); i++) {
        replicaStatesForPartition.put("r" + i, replicaStateList.get(i));
      }
      mappingCurrent.put("p" + k, replicaStatesForPartition);
    }

    externalView.getRecord().getMapFields().putAll(mappingCurrent);

    HelixManager helixManager = HelixManagerFactory.getZKHelixManager(clusterName, "p1",
        InstanceType.ADMINISTRATOR, ZK_ADDR);
    helixManager.connect();
    HelixDataAccessor helixDataAccessor = helixManager.getHelixDataAccessor();
    helixDataAccessor.setProperty(helixDataAccessor.keyBuilder().externalView(resourceName),
        externalView);
    System.out.println("End test :" + TestHelper.getTestMethodName());
  }
}
