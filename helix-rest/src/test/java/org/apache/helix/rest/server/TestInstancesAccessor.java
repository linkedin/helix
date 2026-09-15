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
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.TreeSet;
import javax.ws.rs.client.Entity;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import org.apache.helix.AccessOption;
import org.apache.helix.ConfigAccessor;
import org.apache.helix.HelixDataAccessor;
import org.apache.helix.PropertyPathBuilder;
import org.apache.helix.TestHelper;
import org.apache.helix.constants.InstanceConstants;
import org.apache.helix.manager.zk.ZKHelixDataAccessor;
import org.apache.helix.model.ClusterConfig;
import org.apache.helix.model.IdealState;
import org.apache.helix.model.InstanceConfig;
import org.apache.helix.model.ParticipantHistory;
import org.apache.helix.model.RESTConfig;
import org.apache.helix.rest.server.resources.helix.InstancesAccessor;
import org.apache.helix.rest.server.util.JerseyUriRequestBuilder;
import org.apache.helix.tools.ClusterVerifiers.BestPossibleExternalViewVerifier;
import org.apache.helix.zookeeper.datamodel.ZNRecord;
import org.testng.Assert;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

public class TestInstancesAccessor extends AbstractTestClass {
  private final static String CLUSTER_NAME = "TestCluster_5";

  private static final String SCOPE =
      InstancesAccessor.DelayedRebalanceProperties.scope.name();
  private static final String LIVE_VIEW =
      InstancesAccessor.DelayedRebalanceProperties.liveView.name();
  private static final String OBSERVED_AT_MILLIS =
      InstancesAccessor.DelayedRebalanceProperties.observedAtMillis.name();
  private static final String DELAY_ENABLED =
      InstancesAccessor.DelayedRebalanceProperties.delayEnabled.name();
  private static final String DELAYED_INSTANCES =
      InstancesAccessor.DelayedRebalanceProperties.delayedInstances.name();
  private static final String EXPIRES_AT_MILLIS =
      InstancesAccessor.DelayedRebalanceProperties.expiresAtMillis.name();
  private static final String LIVE = InstancesAccessor.DelayedRebalanceProperties.live.name();
  private static final String ENABLED = InstancesAccessor.DelayedRebalanceProperties.enabled.name();

  @DataProvider
  public Object[][] generatePayloadCrossZoneStoppableCheckWithZoneOrder() {
    return new Object[][]{
        {String.format(
            "{\"%s\":\"%s\",\"%s\":[\"%s\",\"%s\",\"%s\",\"%s\", \"%s\", \"%s\", \"%s\","
                + " \"%s\", \"%s\", \"%s\", \"%s\", \"%s\", \"%s\", \"%s\", \"%s\"],"
                + "\"%s\":[\"%s\", \"%s\", \"%s\", \"%s\", \"%s\"], \"%s\":[\"%s\"]}",
            InstancesAccessor.InstancesProperties.selection_base.name(),
            InstancesAccessor.InstanceHealthSelectionBase.cross_zone_based.name(),
            InstancesAccessor.InstancesProperties.instances.name(), "instance1", "instance2",
            "instance3", "instance4", "instance5", "instance6", "instance7", "instance8",
            "instance9", "instance10", "instance11", "instance12", "instance13", "instance14",
            "invalidInstance",
            InstancesAccessor.InstancesProperties.zone_order.name(),"zone5", "zone4", "zone3", "zone2",
            "zone1",
            InstancesAccessor.InstancesProperties.to_be_stopped_instances.name(),
            "instance0"),
        },
        {String.format(
            "{\"%s\":\"%s\",\"%s\":[\"%s\",\"%s\",\"%s\",\"%s\", \"%s\", \"%s\", \"%s\",\"%s\", \"%s\", \"%s\"],"
                + "\"%s\":[\"%s\", \"%s\", \"%s\", \"%s\", \"%s\"], \"%s\":[\"%s\", \"%s\", \"%s\"]}",
            InstancesAccessor.InstancesProperties.selection_base.name(),
            InstancesAccessor.InstanceHealthSelectionBase.cross_zone_based.name(),
            InstancesAccessor.InstancesProperties.instances.name(), "instance1", "instance3",
            "instance6", "instance9", "instance10", "instance11", "instance12", "instance13",
            "instance14", "invalidInstance",
            InstancesAccessor.InstancesProperties.zone_order.name(), "zone5", "zone4", "zone1",
            "zone3", "zone2", InstancesAccessor.InstancesProperties.to_be_stopped_instances.name(),
            "instance0", "invalidInstance1", "invalidInstance1"),
        }
    };
  }

  @Test
  public void testInstanceStoppableZoneBasedWithToBeStoppedInstances() throws IOException {
    System.out.println("Start test :" + TestHelper.getTestMethodName());

    String content = String.format(
        "{\"%s\":\"%s\",\"%s\":[\"%s\",\"%s\",\"%s\",\"%s\",\"%s\", \"%s\"], \"%s\":[\"%s\",\"%s\"], \"%s\":[\"%s\", \"%s\", \"%s\"]}",
        InstancesAccessor.InstancesProperties.selection_base.name(),
        InstancesAccessor.InstanceHealthSelectionBase.zone_based.name(),
        InstancesAccessor.InstancesProperties.instances.name(), "instance1",
        "instance2", "instance3", "instance4", "instance5", "invalidInstance",
        InstancesAccessor.InstancesProperties.zone_order.name(), "zone2", "zone1",
        InstancesAccessor.InstancesProperties.to_be_stopped_instances.name(), "instance0", "instance6", "invalidInstance1");

    Response response = new JerseyUriRequestBuilder(
        "clusters/{}/instances?command=stoppable&skipHealthCheckCategories=CUSTOM_INSTANCE_CHECK,CUSTOM_PARTITION_CHECK").format(
        STOPPABLE_CLUSTER2).post(this, Entity.entity(content, MediaType.APPLICATION_JSON_TYPE));
    JsonNode jsonNode = OBJECT_MAPPER.readTree(response.readEntity(String.class));

    Set<String> stoppableSet = getStringSet(jsonNode,
        InstancesAccessor.InstancesProperties.instance_stoppable_parallel.name());
    Assert.assertTrue(stoppableSet.contains("instance4") && stoppableSet.contains("instance3"));

    JsonNode nonStoppableInstances = jsonNode.get(
        InstancesAccessor.InstancesProperties.instance_not_stoppable_with_reasons.name());
    //  "StoppableTestCluster2_db_0_3" : { "instance0" : "MASTER", "instance13" : "SLAVE", "instance5" : "SLAVE"}.
    //  Since instance0 is to_be_stopped and MIN_ACTIVE_REPLICA is 2, instance5 is not stoppable.
    Assert.assertEquals(getStringSet(nonStoppableInstances, "instance5"),
        ImmutableSet.of("HELIX:MIN_ACTIVE_REPLICA_CHECK_FAILED"));
    Assert.assertEquals(getStringSet(nonStoppableInstances, "invalidInstance"),
        ImmutableSet.of("HELIX:INSTANCE_NOT_EXIST"));

    System.out.println("End test :" + TestHelper.getTestMethodName());
  }

  @Test(dependsOnMethods = "testInstanceStoppableZoneBasedWithToBeStoppedInstances")
  public void testInstanceStoppableZoneBasedWithoutZoneOrder() throws IOException {
    System.out.println("Start test :" + TestHelper.getTestMethodName());
    String content = String.format(
        "{\"%s\":\"%s\",\"%s\":[\"%s\",\"%s\",\"%s\",\"%s\",\"%s\",\"%s\"], \"%s\":[\"%s\", \"%s\", \"%s\"]}",
        InstancesAccessor.InstancesProperties.selection_base.name(),
        InstancesAccessor.InstanceHealthSelectionBase.zone_based.name(),
        InstancesAccessor.InstancesProperties.instances.name(), "instance0", "instance1",
        "instance2", "instance3", "instance4", "invalidInstance",
        InstancesAccessor.InstancesProperties.to_be_stopped_instances.name(),
        "instance7", "instance9", "instance10");

    Response response = new JerseyUriRequestBuilder(
        "clusters/{}/instances?command=stoppable&skipHealthCheckCategories=CUSTOM_INSTANCE_CHECK,CUSTOM_PARTITION_CHECK").format(
        STOPPABLE_CLUSTER2).post(this, Entity.entity(content, MediaType.APPLICATION_JSON_TYPE));
    JsonNode jsonNode = OBJECT_MAPPER.readTree(response.readEntity(String.class));

    // Without zone order, helix should pick the zone1 because it has higher instance count than zone2.
    Set<String> stoppableSet = getStringSet(jsonNode,
        InstancesAccessor.InstancesProperties.instance_stoppable_parallel.name());
    Assert.assertTrue(stoppableSet.contains("instance0") && stoppableSet.contains("instance1"));

    JsonNode nonStoppableInstances = jsonNode.get(
        InstancesAccessor.InstancesProperties.instance_not_stoppable_with_reasons.name());
    Assert.assertEquals(getStringSet(nonStoppableInstances, "instance2"),
        ImmutableSet.of("HELIX:MIN_ACTIVE_REPLICA_CHECK_FAILED"));
    Assert.assertEquals(getStringSet(nonStoppableInstances, "invalidInstance"),
        ImmutableSet.of("HELIX:INSTANCE_NOT_EXIST"));
    System.out.println("End test :" + TestHelper.getTestMethodName());
  }

  @Test(dataProvider = "generatePayloadCrossZoneStoppableCheckWithZoneOrder",
      dependsOnMethods = "testInstanceStoppableZoneBasedWithoutZoneOrder")
  public void testCrossZoneStoppableWithZoneOrder(String content) throws IOException {
    System.out.println("Start test :" + TestHelper.getTestMethodName());
    Response response = new JerseyUriRequestBuilder(
        "clusters/{}/instances?command=stoppable&skipHealthCheckCategories=CUSTOM_INSTANCE_CHECK,CUSTOM_PARTITION_CHECK").format(
        STOPPABLE_CLUSTER2).post(this, Entity.entity(content, MediaType.APPLICATION_JSON_TYPE));
    JsonNode jsonNode = OBJECT_MAPPER.readTree(response.readEntity(String.class));

    Set<String> stoppableSet = getStringSet(jsonNode,
        InstancesAccessor.InstancesProperties.instance_stoppable_parallel.name());
    Assert.assertTrue(stoppableSet.contains("instance14") && stoppableSet.contains("instance12")
        && stoppableSet.contains("instance11") && stoppableSet.contains("instance10"));

    JsonNode nonStoppableInstances = jsonNode.get(
        InstancesAccessor.InstancesProperties.instance_not_stoppable_with_reasons.name());
    Assert.assertEquals(getStringSet(nonStoppableInstances, "instance13"),
        ImmutableSet.of("HELIX:MIN_ACTIVE_REPLICA_CHECK_FAILED"));
    Assert.assertEquals(getStringSet(nonStoppableInstances, "invalidInstance"),
        ImmutableSet.of("HELIX:INSTANCE_NOT_EXIST"));
    System.out.println("End test :" + TestHelper.getTestMethodName());
  }

  @Test(dependsOnMethods = "testCrossZoneStoppableWithZoneOrder")
  public void testCrossZoneStoppableWithoutZoneOrder() throws IOException {
    System.out.println("Start test :" + TestHelper.getTestMethodName());
    String content = String.format(
        "{\"%s\":\"%s\",\"%s\":[\"%s\",\"%s\",\"%s\",\"%s\", \"%s\", \"%s\", \"%s\",\"%s\", \"%s\", \"%s\"],"
            + "\"%s\":[\"%s\", \"%s\", \"%s\"]}",
        InstancesAccessor.InstancesProperties.selection_base.name(),
        InstancesAccessor.InstanceHealthSelectionBase.cross_zone_based.name(),
        InstancesAccessor.InstancesProperties.instances.name(), "instance1", "instance3",
        "instance6", "instance9", "instance10", "instance11", "instance12", "instance13",
        "instance14", "invalidInstance",
        InstancesAccessor.InstancesProperties.to_be_stopped_instances.name(), "instance0",
        "invalidInstance1", "invalidInstance1");

    Response response = new JerseyUriRequestBuilder(
        "clusters/{}/instances?command=stoppable&skipHealthCheckCategories=CUSTOM_INSTANCE_CHECK,CUSTOM_PARTITION_CHECK").format(
        STOPPABLE_CLUSTER2).post(this, Entity.entity(content, MediaType.APPLICATION_JSON_TYPE));
    JsonNode jsonNode = OBJECT_MAPPER.readTree(response.readEntity(String.class));

    Set<String> stoppableSet = getStringSet(jsonNode,
        InstancesAccessor.InstancesProperties.instance_stoppable_parallel.name());
    Assert.assertTrue(stoppableSet.contains("instance14") && stoppableSet.contains("instance12")
        && stoppableSet.contains("instance11") && stoppableSet.contains("instance10"));

    JsonNode nonStoppableInstances = jsonNode.get(
        InstancesAccessor.InstancesProperties.instance_not_stoppable_with_reasons.name());
    Assert.assertEquals(getStringSet(nonStoppableInstances, "instance13"),
        ImmutableSet.of("HELIX:MIN_ACTIVE_REPLICA_CHECK_FAILED"));
    Assert.assertEquals(getStringSet(nonStoppableInstances, "invalidInstance"),
        ImmutableSet.of("HELIX:INSTANCE_NOT_EXIST"));
    System.out.println("End test :" + TestHelper.getTestMethodName());
  }

  @Test(dependsOnMethods = "testCrossZoneStoppableWithoutZoneOrder")
  public void testInstanceStoppableCrossZoneBasedWithSelectedCheckList() throws IOException {
    System.out.println("Start test :" + TestHelper.getTestMethodName());
    // Select instances with cross zone based and perform all checks
    String content =
        String.format("{\"%s\":\"%s\",\"%s\":[\"%s\",\"%s\",\"%s\",\"%s\",\"%s\",\"%s\", \"%s\"], \"%s\":[\"%s\"]}",
            InstancesAccessor.InstancesProperties.selection_base.name(),
            InstancesAccessor.InstanceHealthSelectionBase.cross_zone_based.name(),
            InstancesAccessor.InstancesProperties.instances.name(), "instance0", "instance1",
            "instance2", "instance3", "instance4", "instance5", "invalidInstance",
            InstancesAccessor.InstancesProperties.skip_stoppable_check_list.name(), "DUMMY_TEST_NO_EXISTS");

    new JerseyUriRequestBuilder("clusters/{}/instances?command=stoppable").format(STOPPABLE_CLUSTER)
        .isBodyReturnExpected(true)
        .expectedReturnStatusCode(Response.Status.BAD_REQUEST.getStatusCode())
        .post(this, Entity.entity(content, MediaType.APPLICATION_JSON_TYPE));

    // Select instances with cross zone based and perform a subset of checks
    content = String.format(
        "{\"%s\":\"%s\",\"%s\":[\"%s\",\"%s\",\"%s\",\"%s\",\"%s\",\"%s\", \"%s\"], \"%s\":[\"%s\",\"%s\"], \"%s\":[\"%s\", \"%s\"]}",
        InstancesAccessor.InstancesProperties.selection_base.name(),
        InstancesAccessor.InstanceHealthSelectionBase.cross_zone_based.name(),
        InstancesAccessor.InstancesProperties.instances.name(), "instance0", "instance1",
        "instance2", "instance3", "instance4", "instance5", "invalidInstance",
        InstancesAccessor.InstancesProperties.zone_order.name(), "zone2", "zone1",
        InstancesAccessor.InstancesProperties.skip_stoppable_check_list.name(), "INSTANCE_NOT_ENABLED", "INSTANCE_NOT_STABLE");
    Response response = new JerseyUriRequestBuilder(
        "clusters/{}/instances?command=stoppable&skipHealthCheckCategories=CUSTOM_INSTANCE_CHECK,CUSTOM_PARTITION_CHECK").format(
        STOPPABLE_CLUSTER).post(this, Entity.entity(content, MediaType.APPLICATION_JSON_TYPE));
    JsonNode jsonNode = OBJECT_MAPPER.readTree(response.readEntity(String.class));
    JsonNode nonStoppableInstances = jsonNode.get(
        InstancesAccessor.InstancesProperties.instance_not_stoppable_with_reasons.name());
    Assert.assertEquals(getStringSet(nonStoppableInstances, "instance5"),
        ImmutableSet.of("HELIX:EMPTY_RESOURCE_ASSIGNMENT", "HELIX:INSTANCE_NOT_ALIVE"));
    Assert.assertEquals(getStringSet(nonStoppableInstances, "instance4"),
        ImmutableSet.of("HELIX:EMPTY_RESOURCE_ASSIGNMENT", "HELIX:INSTANCE_NOT_ALIVE"));
    Assert.assertEquals(getStringSet(nonStoppableInstances, "instance1"),
        ImmutableSet.of("HELIX:EMPTY_RESOURCE_ASSIGNMENT"));
    Assert.assertEquals(getStringSet(nonStoppableInstances, "invalidInstance"),
        ImmutableSet.of("HELIX:INSTANCE_NOT_EXIST"));

    System.out.println("End test :" + TestHelper.getTestMethodName());
  }

  @Test(dependsOnMethods = "testCrossZoneStoppableWithoutZoneOrder")
  public void testSkipCustomChecksIfInstanceNotAlive() throws JsonProcessingException {
    System.out.println("Start test :" + TestHelper.getTestMethodName());

    // Instance 4 and 5 in stoppable cluster 1 are not alive
    String content = String.format(
        "{\"%s\":\"%s\",\"%s\":[\"%s\",\"%s\", \"%s\"], \"%s\":[\"%s\", \"%s\", \"%s\"], \"%s"
            + "\": \"%b\"}",
        InstancesAccessor.InstancesProperties.selection_base.name(),
        InstancesAccessor.InstanceHealthSelectionBase.cross_zone_based.name(),
        InstancesAccessor.InstancesProperties.instances.name(), "instance4", "instance5", "invalidInstance",
        InstancesAccessor.InstancesProperties.skip_stoppable_check_list.name(), "INSTANCE_NOT_ALIVE", "EMPTY_RESOURCE_ASSIGNMENT", "INSTANCE_NOT_STABLE",
        InstancesAccessor.InstancesProperties.skip_custom_check_if_instance_not_alive.name(), true);

    // Set the dummy custom checks for the cluster. The custom checks should be skipped.
    ConfigAccessor configAccessor = new ConfigAccessor(ZK_ADDR);
    Assert.assertNull(configAccessor.getRESTConfig(STOPPABLE_CLUSTER));
    RESTConfig restConfig = new RESTConfig(STOPPABLE_CLUSTER);
    restConfig.set(RESTConfig.SimpleFields.CUSTOMIZED_HEALTH_URL, "TEST_URL");
    configAccessor.setRESTConfig(STOPPABLE_CLUSTER, restConfig);
    Assert.assertEquals(restConfig, configAccessor.getRESTConfig(STOPPABLE_CLUSTER));

    // Even if we don't skip custom stoppable checks, the instance is not alive so it should be stoppable
    Response response = new JerseyUriRequestBuilder(
        "clusters/{}/instances?command=stoppable").format(
        STOPPABLE_CLUSTER).post(this, Entity.entity(content, MediaType.APPLICATION_JSON_TYPE));
    JsonNode jsonNode = OBJECT_MAPPER.readTree(response.readEntity(String.class));
    Set<String> stoppableSet = getStringSet(jsonNode,
        InstancesAccessor.InstancesProperties.instance_stoppable_parallel.name());
    Assert.assertTrue(stoppableSet.contains("instance4"));
    Assert.assertTrue(stoppableSet.contains("instance5"));
    JsonNode nonStoppableInstances = jsonNode.get(
        InstancesAccessor.InstancesProperties.instance_not_stoppable_with_reasons.name());

    Assert.assertEquals(getStringSet(nonStoppableInstances, "invalidInstance"),
        ImmutableSet.of("HELIX:INSTANCE_NOT_EXIST"));

    // After the test finishes, remove the dummy custom checks REST config
    configAccessor.deleteRESTConfig(STOPPABLE_CLUSTER);
    Assert.assertNull(configAccessor.getRESTConfig(STOPPABLE_CLUSTER));

    System.out.println("End test :" + TestHelper.getTestMethodName());
  }

  @Test(dependsOnMethods = "testSkipCustomChecksIfInstanceNotAlive")
  public void testInstanceStoppableCrossZoneBasedWithEvacuatingInstances() throws IOException {
    System.out.println("Start test :" + TestHelper.getTestMethodName());
    String content = String.format(
        "{\"%s\":\"%s\",\"%s\":[\"%s\",\"%s\",\"%s\",\"%s\", \"%s\", \"%s\", \"%s\",\"%s\", \"%s\", \"%s\"]}",
        InstancesAccessor.InstancesProperties.selection_base.name(),
        InstancesAccessor.InstanceHealthSelectionBase.cross_zone_based.name(),
        InstancesAccessor.InstancesProperties.instances.name(), "instance1", "instance3",
        "instance6", "instance9", "instance10", "instance11", "instance12", "instance13",
        "instance14", "invalidInstance");

    // Change instance config of instance1 & instance0 to be evacuating
    String instance0 = "instance0";
    InstanceConfig instanceConfig = _configAccessor.getInstanceConfig(STOPPABLE_CLUSTER2, instance0);
    instanceConfig.setInstanceOperation(InstanceConstants.InstanceOperation.EVACUATE);
    _configAccessor.setInstanceConfig(STOPPABLE_CLUSTER2, instance0, instanceConfig);
    String instance1 = "instance1";
    InstanceConfig instanceConfig1 = _configAccessor.getInstanceConfig(STOPPABLE_CLUSTER2, instance1);
    instanceConfig1.setInstanceOperation(InstanceConstants.InstanceOperation.EVACUATE);
    _configAccessor.setInstanceConfig(STOPPABLE_CLUSTER2, instance1, instanceConfig1);
    // It takes time to reflect the changes.
    BestPossibleExternalViewVerifier verifier =
        new BestPossibleExternalViewVerifier.Builder(STOPPABLE_CLUSTER2).setZkAddr(ZK_ADDR).build();
    Assert.assertTrue(verifier.verifyByPolling());

    Response response = new JerseyUriRequestBuilder(
        "clusters/{}/instances?command=stoppable&skipHealthCheckCategories=CUSTOM_INSTANCE_CHECK,CUSTOM_PARTITION_CHECK").format(
        STOPPABLE_CLUSTER2).post(this, Entity.entity(content, MediaType.APPLICATION_JSON_TYPE));
    JsonNode jsonNode = OBJECT_MAPPER.readTree(response.readEntity(String.class));

    try {
      Set<String> stoppableSet = getStringSet(jsonNode,
          InstancesAccessor.InstancesProperties.instance_stoppable_parallel.name());
      Assert.assertTrue(stoppableSet.contains("instance12")
          && stoppableSet.contains("instance11") && stoppableSet.contains("instance10"));
      // instance0 and instance1 are evacuating, but they stay routable and keep serving their
      // replicas until a replacement is bootstrapped, so their siblings are still above min active
      // replicas and remain stoppable.
      Assert.assertTrue(stoppableSet.contains("instance13"));
      Assert.assertTrue(stoppableSet.contains("instance14"));

      JsonNode nonStoppableInstances = jsonNode.get(
          InstancesAccessor.InstancesProperties.instance_not_stoppable_with_reasons.name());
      Assert.assertEquals(getStringSet(nonStoppableInstances, "invalidInstance"),
          ImmutableSet.of("HELIX:INSTANCE_NOT_EXIST"));
    } finally {
      instanceConfig.setInstanceOperation(InstanceConstants.InstanceOperation.ENABLE);
      _configAccessor.setInstanceConfig(STOPPABLE_CLUSTER2, instance0, instanceConfig);
      instanceConfig1.setInstanceOperation(InstanceConstants.InstanceOperation.ENABLE);
      _configAccessor.setInstanceConfig(STOPPABLE_CLUSTER2, instance1, instanceConfig1);
    }
    System.out.println("End test :" + TestHelper.getTestMethodName());
  }

  @Test(dependsOnMethods = "testInstanceStoppableCrossZoneBasedWithEvacuatingInstances")
  public void testInstanceStoppable_zoneBased_zoneOrder() throws IOException {
    System.out.println("Start test :" + TestHelper.getTestMethodName());
    // Select instances with zone based
    String content = String.format(
        "{\"%s\":\"%s\",\"%s\":[\"%s\",\"%s\",\"%s\",\"%s\",\"%s\",\"%s\", \"%s\"], \"%s\":[\"%s\",\"%s\"]}",
        InstancesAccessor.InstancesProperties.selection_base.name(),
        InstancesAccessor.InstanceHealthSelectionBase.zone_based.name(),
        InstancesAccessor.InstancesProperties.instances.name(), "instance0", "instance1",
        "instance2", "instance3", "instance4", "instance5", "invalidInstance",
        InstancesAccessor.InstancesProperties.zone_order.name(), "zone2", "zone1");
    Response response = new JerseyUriRequestBuilder(
        "clusters/{}/instances?command=stoppable&skipHealthCheckCategories=CUSTOM_INSTANCE_CHECK,CUSTOM_PARTITION_CHECK").format(
        STOPPABLE_CLUSTER).post(this, Entity.entity(content, MediaType.APPLICATION_JSON_TYPE));
    JsonNode jsonNode = OBJECT_MAPPER.readTree(response.readEntity(String.class));
    Assert.assertFalse(
        jsonNode.withArray(InstancesAccessor.InstancesProperties.instance_stoppable_parallel.name())
            .elements().hasNext());
    JsonNode nonStoppableInstances = jsonNode.get(
        InstancesAccessor.InstancesProperties.instance_not_stoppable_with_reasons.name());
    Assert.assertEquals(getStringSet(nonStoppableInstances, "instance5"),
        ImmutableSet.of("HELIX:EMPTY_RESOURCE_ASSIGNMENT", "HELIX:INSTANCE_NOT_ALIVE",
            "HELIX:INSTANCE_NOT_STABLE"));
    Assert.assertEquals(getStringSet(nonStoppableInstances, "invalidInstance"),
        ImmutableSet.of("HELIX:INSTANCE_NOT_EXIST"));
    System.out.println("End test :" + TestHelper.getTestMethodName());
  }

  @Test(dependsOnMethods = "testInstanceStoppable_zoneBased_zoneOrder")
  public void testInstancesStoppable_zoneBased() throws IOException {
    System.out.println("Start test :" + TestHelper.getTestMethodName());
    // Select instances with zone based
    String content =
        String.format("{\"%s\":\"%s\",\"%s\":[\"%s\",\"%s\",\"%s\",\"%s\",\"%s\",\"%s\", \"%s\"]}",
            InstancesAccessor.InstancesProperties.selection_base.name(),
            InstancesAccessor.InstanceHealthSelectionBase.zone_based.name(),
            InstancesAccessor.InstancesProperties.instances.name(), "instance0", "instance1",
            "instance2", "instance3", "instance4", "instance5", "invalidInstance");
    Response response =
        new JerseyUriRequestBuilder("clusters/{}/instances?command=stoppable").format(
            STOPPABLE_CLUSTER).post(this, Entity.entity(content, MediaType.APPLICATION_JSON_TYPE));
    JsonNode jsonNode = OBJECT_MAPPER.readTree(response.readEntity(String.class));
    Assert.assertFalse(
        jsonNode.withArray(InstancesAccessor.InstancesProperties.instance_stoppable_parallel.name())
            .elements().hasNext());
    JsonNode nonStoppableInstances = jsonNode.get(
        InstancesAccessor.InstancesProperties.instance_not_stoppable_with_reasons.name());
    Assert.assertEquals(getStringSet(nonStoppableInstances, "instance0"),
        ImmutableSet.of("HELIX:MIN_ACTIVE_REPLICA_CHECK_FAILED"));
    Assert.assertEquals(getStringSet(nonStoppableInstances, "instance1"),
        ImmutableSet.of("HELIX:EMPTY_RESOURCE_ASSIGNMENT", "HELIX:INSTANCE_NOT_ENABLED",
            "HELIX:INSTANCE_NOT_STABLE"));
    Assert.assertEquals(getStringSet(nonStoppableInstances, "instance2"),
        ImmutableSet.of("HELIX:MIN_ACTIVE_REPLICA_CHECK_FAILED"));
    Assert.assertEquals(getStringSet(nonStoppableInstances, "instance3"),
        ImmutableSet.of("HELIX:HAS_DISABLED_PARTITION", "HELIX:MIN_ACTIVE_REPLICA_CHECK_FAILED"));
    Assert.assertEquals(getStringSet(nonStoppableInstances, "instance4"),
        ImmutableSet.of("HELIX:EMPTY_RESOURCE_ASSIGNMENT", "HELIX:INSTANCE_NOT_ALIVE",
            "HELIX:INSTANCE_NOT_STABLE"));
    Assert.assertEquals(getStringSet(nonStoppableInstances, "invalidInstance"), ImmutableSet.of("HELIX:INSTANCE_NOT_EXIST"));
    System.out.println("End test :" + TestHelper.getTestMethodName());
  }

  @Test(dependsOnMethods = "testInstancesStoppable_zoneBased")
  public void testInstancesStoppable_disableOneInstance() throws IOException {
    // Disable one selected instance0, it should failed to check
    String instance = "instance0";
    InstanceConfig instanceConfig = _configAccessor.getInstanceConfig(STOPPABLE_CLUSTER, instance);
    instanceConfig.setInstanceOperation(InstanceConstants.InstanceOperation.DISABLE);
    instanceConfig.setInstanceEnabledForPartition("FakeResource", "FakePartition", false);
    _configAccessor.setInstanceConfig(STOPPABLE_CLUSTER, instance, instanceConfig);

    // It takes time to reflect the changes.
    BestPossibleExternalViewVerifier verifier =
        new BestPossibleExternalViewVerifier.Builder(STOPPABLE_CLUSTER).setZkAddr(ZK_ADDR).build();
    Assert.assertTrue(verifier.verifyByPolling());

    Entity entity = Entity.entity("\"{}\"", MediaType.APPLICATION_JSON_TYPE);
    Response response = new JerseyUriRequestBuilder("clusters/{}/instances/{}/stoppable")
        .format(STOPPABLE_CLUSTER, instance).post(this, entity);
    JsonNode jsonResult = OBJECT_MAPPER.readTree(response.readEntity(String.class));
    Assert.assertFalse(jsonResult.get("stoppable").asBoolean());
    Assert.assertEquals(getStringSet(jsonResult, "failedChecks"),
            ImmutableSet.of("HELIX:HAS_DISABLED_PARTITION","HELIX:INSTANCE_NOT_ENABLED","HELIX:INSTANCE_NOT_STABLE"));

    // Reenable instance0, it should passed the check
    instanceConfig.setInstanceOperation(InstanceConstants.InstanceOperation.ENABLE);
    instanceConfig.setInstanceEnabledForPartition("FakeResource", "FakePartition", true);
    _configAccessor.setInstanceConfig(STOPPABLE_CLUSTER, instance, instanceConfig);
    Assert.assertTrue(verifier.verifyByPolling());

    entity = Entity.entity("\"{}\"", MediaType.APPLICATION_JSON_TYPE);
    response = new JerseyUriRequestBuilder("clusters/{}/instances/{}/stoppable")
        .format(STOPPABLE_CLUSTER, instance).post(this, entity);
    jsonResult = OBJECT_MAPPER.readTree(response.readEntity(String.class));

    Assert.assertFalse(jsonResult.get("stoppable").asBoolean());
    Assert.assertEquals(getStringSet(jsonResult, "failedChecks"), ImmutableSet.of("HELIX:MIN_ACTIVE_REPLICA_CHECK_FAILED"));
    System.out.println("End test :" + TestHelper.getTestMethodName());
  }

  @Test(dependsOnMethods = "testInstancesStoppable_disableOneInstance")
  public void testGetAllInstances() throws IOException {
    System.out.println("Start test :" + TestHelper.getTestMethodName());
    String body = new JerseyUriRequestBuilder("clusters/{}/instances").isBodyReturnExpected(true)
        .format(CLUSTER_NAME).get(this);

    JsonNode node = OBJECT_MAPPER.readTree(body);
    String instancesStr =
        node.get(InstancesAccessor.InstancesProperties.instances.name()).toString();
    Assert.assertNotNull(instancesStr);

    Set<String> instances = OBJECT_MAPPER.readValue(instancesStr,
        OBJECT_MAPPER.getTypeFactory().constructCollectionType(Set.class, String.class));
    Assert.assertEquals(instances.size(), _instancesMap.get(CLUSTER_NAME).size(), "Different amount of elements in "
        + "the sets: " + instances.size() + " vs: " + _instancesMap.get(CLUSTER_NAME).size());
    Assert.assertTrue(instances.containsAll(_instancesMap.get(CLUSTER_NAME)), "instances set does not contain all "
        + "elements of _instanceMap");
    Assert.assertTrue(_instancesMap.get(CLUSTER_NAME).containsAll(instances), "_instanceMap set does not contain all "
        + "elements of instances");
    System.out.println("End test :" + TestHelper.getTestMethodName());
  }

  @Test
  public void testGetAllInstancesWithOperationStates() throws IOException {
    System.out.println("Start test :" + TestHelper.getTestMethodName());

    // Get all instances from the cluster
    List<String> allInstances = _gSetupTool.getClusterManagementTool().getInstancesInCluster(CLUSTER_NAME);
    Assert.assertTrue(allInstances.size() >= 4, "Need at least 4 instances for this test");

    // Set different operation states on instances
    String enabledInstance = allInstances.get(0);
    String disabledInstance = allInstances.get(1);
    String evacuatedInstance = allInstances.get(2);
    String swapInInstance = allInstances.get(3);

    // Set DISABLE operation
    InstanceConfig disabledConfig = _configAccessor.getInstanceConfig(CLUSTER_NAME, disabledInstance);
    disabledConfig.setInstanceOperation(
        new InstanceConfig.InstanceOperation.Builder()
            .setOperation(InstanceConstants.InstanceOperation.DISABLE)
            .setReason("Test disable")
            .build());
    _configAccessor.setInstanceConfig(CLUSTER_NAME, disabledInstance, disabledConfig);

    // Set EVACUATE operation
    InstanceConfig evacuatedConfig = _configAccessor.getInstanceConfig(CLUSTER_NAME, evacuatedInstance);
    evacuatedConfig.setInstanceOperation(
        new InstanceConfig.InstanceOperation.Builder()
            .setOperation(InstanceConstants.InstanceOperation.EVACUATE)
            .setReason("Test evacuate")
            .build());
    _configAccessor.setInstanceConfig(CLUSTER_NAME, evacuatedInstance, evacuatedConfig);

    // Set SWAP_IN operation
    InstanceConfig swapInConfig = _configAccessor.getInstanceConfig(CLUSTER_NAME, swapInInstance);
    swapInConfig.setInstanceOperation(
        new InstanceConfig.InstanceOperation.Builder()
            .setOperation(InstanceConstants.InstanceOperation.SWAP_IN)
            .setReason("Test swap in")
            .build());
    _configAccessor.setInstanceConfig(CLUSTER_NAME, swapInInstance, swapInConfig);

    // Keep one instance with ENABLE (default) - enabledInstance already has ENABLE by default

    // Make the API call
    String body = new JerseyUriRequestBuilder("clusters/{}/instances").isBodyReturnExpected(true)
        .format(CLUSTER_NAME).get(this);

    JsonNode node = OBJECT_MAPPER.readTree(body);

    // Verify all expected fields are present
    Assert.assertNotNull(node.get(InstancesAccessor.InstancesProperties.instances.name()));
    Assert.assertNotNull(node.get(InstancesAccessor.InstancesProperties.online.name()));
    Assert.assertNotNull(node.get(InstancesAccessor.InstancesProperties.enabled.name()));
    Assert.assertNotNull(node.get(InstancesAccessor.InstancesProperties.disabled.name()));
    Assert.assertNotNull(node.get(InstancesAccessor.InstancesProperties.evacuated.name()));
    Assert.assertNotNull(node.get(InstancesAccessor.InstancesProperties.swap_in.name()));
    Assert.assertNotNull(node.get(InstancesAccessor.InstancesProperties.unknown.name()));

    // Parse the response arrays
    Set<String> enabledInstances = OBJECT_MAPPER.readValue(
        node.get(InstancesAccessor.InstancesProperties.enabled.name()).toString(),
        OBJECT_MAPPER.getTypeFactory().constructCollectionType(Set.class, String.class));
    Set<String> disabledInstances = OBJECT_MAPPER.readValue(
        node.get(InstancesAccessor.InstancesProperties.disabled.name()).toString(),
        OBJECT_MAPPER.getTypeFactory().constructCollectionType(Set.class, String.class));
    Set<String> evacuatedInstances = OBJECT_MAPPER.readValue(
        node.get(InstancesAccessor.InstancesProperties.evacuated.name()).toString(),
        OBJECT_MAPPER.getTypeFactory().constructCollectionType(Set.class, String.class));
    Set<String> swapInInstances = OBJECT_MAPPER.readValue(
        node.get(InstancesAccessor.InstancesProperties.swap_in.name()).toString(),
        OBJECT_MAPPER.getTypeFactory().constructCollectionType(Set.class, String.class));
    Set<String> unknownInstances = OBJECT_MAPPER.readValue(
        node.get(InstancesAccessor.InstancesProperties.unknown.name()).toString(),
        OBJECT_MAPPER.getTypeFactory().constructCollectionType(Set.class, String.class));

    // Verify the categorization is correct
    Assert.assertTrue(enabledInstances.contains(enabledInstance),
        "Enabled instance should be in enabled list");
    Assert.assertTrue(disabledInstances.contains(disabledInstance),
        "Disabled instance should be in disabled list");
    Assert.assertTrue(evacuatedInstances.contains(evacuatedInstance),
        "Evacuated instance should be in evacuated list");
    Assert.assertTrue(swapInInstances.contains(swapInInstance),
        "Swap-in instance should be in swap_in list");

    // Verify instances are not in wrong categories
    Assert.assertFalse(disabledInstances.contains(enabledInstance),
        "Enabled instance should not be in disabled list");
    Assert.assertFalse(evacuatedInstances.contains(disabledInstance),
        "Disabled instance should not be in evacuated list");
    Assert.assertFalse(enabledInstances.contains(evacuatedInstance),
        "Evacuated instance should not be in enabled list");
    Assert.assertFalse(enabledInstances.contains(swapInInstance),
        "Swap-in instance should not be in enabled list");

    // Clean up - reset all instances to ENABLE
    disabledConfig.setInstanceOperation(
        new InstanceConfig.InstanceOperation.Builder()
            .setOperation(InstanceConstants.InstanceOperation.ENABLE)
            .build());
    _configAccessor.setInstanceConfig(CLUSTER_NAME, disabledInstance, disabledConfig);

    evacuatedConfig.setInstanceOperation(
        new InstanceConfig.InstanceOperation.Builder()
            .setOperation(InstanceConstants.InstanceOperation.ENABLE)
            .build());
    _configAccessor.setInstanceConfig(CLUSTER_NAME, evacuatedInstance, evacuatedConfig);

    swapInConfig.setInstanceOperation(
        new InstanceConfig.InstanceOperation.Builder()
            .setOperation(InstanceConstants.InstanceOperation.ENABLE)
            .build());
    _configAccessor.setInstanceConfig(CLUSTER_NAME, swapInInstance, swapInConfig);

    System.out.println("End test :" + TestHelper.getTestMethodName());
  }

  @Test(dependsOnMethods = "testGetAllInstances")
  public void testValidateWeightForAllInstances() throws IOException {
    System.out.println("Start test :" + TestHelper.getTestMethodName());

    // Empty out ClusterConfig's weight key setting and InstanceConfig's capacity maps for testing
    ClusterConfig clusterConfig = _configAccessor.getClusterConfig(CLUSTER_NAME);
    clusterConfig.getRecord().setListField(
        ClusterConfig.ClusterConfigProperty.INSTANCE_CAPACITY_KEYS.name(), new ArrayList<>());
    _configAccessor.setClusterConfig(CLUSTER_NAME, clusterConfig);
    List<String> instances =
        _gSetupTool.getClusterManagementTool().getInstancesInCluster(CLUSTER_NAME);
    for (String instance : instances) {
      InstanceConfig instanceConfig = _configAccessor.getInstanceConfig(CLUSTER_NAME, instance);
      instanceConfig.setInstanceCapacityMap(Collections.emptyMap());
      _configAccessor.setInstanceConfig(CLUSTER_NAME, instance, instanceConfig);
    }

    // Issue a validate call
    String body = new JerseyUriRequestBuilder("clusters/{}/instances?command=validateWeight")
        .isBodyReturnExpected(true).format(CLUSTER_NAME).get(this);

    JsonNode node = OBJECT_MAPPER.readTree(body);
    // Must have the results saying they are all valid (true) because there's no capacity keys set
    // in ClusterConfig
    node.iterator().forEachRemaining(child -> Assert.assertTrue(child.booleanValue()));

    clusterConfig = _configAccessor.getClusterConfig(CLUSTER_NAME);
    clusterConfig.setInstanceCapacityKeys(Arrays.asList("FOO", "BAR"));
    _configAccessor.setClusterConfig(CLUSTER_NAME, clusterConfig);

    body = new JerseyUriRequestBuilder("clusters/{}/instances?command=validateWeight")
        .isBodyReturnExpected(true).format(CLUSTER_NAME)
        .expectedReturnStatusCode(Response.Status.BAD_REQUEST.getStatusCode()).get(this);
    node = OBJECT_MAPPER.readTree(body);
    // Since instances do not have weight-related configs, the result should return error
    Assert.assertTrue(node.has("error"));

    // Now set weight-related configs in InstanceConfigs
    instances = _gSetupTool.getClusterManagementTool().getInstancesInCluster(CLUSTER_NAME);
    for (String instance : instances) {
      InstanceConfig instanceConfig = _configAccessor.getInstanceConfig(CLUSTER_NAME, instance);
      instanceConfig.setInstanceCapacityMap(ImmutableMap.of("FOO", 1000, "BAR", 1000));
      _configAccessor.setInstanceConfig(CLUSTER_NAME, instance, instanceConfig);
    }

    body = new JerseyUriRequestBuilder("clusters/{}/instances?command=validateWeight")
        .isBodyReturnExpected(true).format(CLUSTER_NAME)
        .expectedReturnStatusCode(Response.Status.OK.getStatusCode()).get(this);
    node = OBJECT_MAPPER.readTree(body);
    // Must have the results saying they are all valid (true) because capacity keys are set
    // in ClusterConfig
    node.iterator().forEachRemaining(child -> Assert.assertTrue(child.booleanValue()));

    System.out.println("End test :" + TestHelper.getTestMethodName());
  }

  @Test(dependsOnMethods = "testValidateWeightForAllInstances")
  public void testMultipleReplicasInSameMZ() throws Exception {
    System.out.println("Start test :" + TestHelper.getTestMethodName());
    // Create SemiAuto DB so that we can control assignment
    String testDb = TestHelper.getTestMethodName() + "_resource";
    _gSetupTool.getClusterManagementTool().addResource(STOPPABLE_CLUSTER2, testDb, 3, "MasterSlave",
        IdealState.RebalanceMode.SEMI_AUTO.toString());
    _gSetupTool.getClusterManagementTool().rebalance(STOPPABLE_CLUSTER2, testDb, 3);

    // Manually set ideal state to have the 3 replcias assigned to 3 instances all in the same zone
    List<String> preferenceList = Arrays.asList("instance0", "instance1", "instance2");
    IdealState is = _gSetupTool.getClusterManagementTool().getResourceIdealState(STOPPABLE_CLUSTER2, testDb);
    for (String p : is.getPartitionSet()) {
      is.setPreferenceList(p, preferenceList);
    }
    is.setMinActiveReplicas(2);
    _gSetupTool.getClusterManagementTool().setResourceIdealState(STOPPABLE_CLUSTER2, testDb, is);

    // Wait for assignments to take place
    BestPossibleExternalViewVerifier verifier =
        new BestPossibleExternalViewVerifier.Builder(STOPPABLE_CLUSTER2).setZkAddr(ZK_ADDR).build();
    Assert.assertTrue(verifier.verifyByPolling());

    // Run stoppable check against the 3 instances where SemiAuto DB was assigned
    String content =
        String.format("{\"%s\":\"%s\",\"%s\":[\"%s\",\"%s\",\"%s\"]}",
            InstancesAccessor.InstancesProperties.selection_base.name(),
            InstancesAccessor.InstanceHealthSelectionBase.zone_based.name(),
            InstancesAccessor.InstancesProperties.instances.name(), "instance0", "instance1",
            "instance2");
    Response response =
        new JerseyUriRequestBuilder("clusters/{}/instances?command=stoppable&skipHealthCheckCategories=CUSTOM_INSTANCE_CHECK,CUSTOM_PARTITION_CHECK").format(
            STOPPABLE_CLUSTER2).post(this, Entity.entity(content, MediaType.APPLICATION_JSON_TYPE));
    JsonNode jsonNode = OBJECT_MAPPER.readTree(response.readEntity(String.class));

    // Resource has 3 replicas with min_active of 2
    // First instance should be stoppable as min_active still satisfied
    Set<String> stoppableSet = getStringSet(jsonNode,
        InstancesAccessor.InstancesProperties.instance_stoppable_parallel.name());
    Assert.assertTrue(Collections.singleton("instance0").equals(stoppableSet));

    // Next 2 instances should fail stoppable due to MIN_ACTIVE_REPLICA_CHECK_FAILED
    JsonNode nonStoppableInstances = jsonNode.get(
        InstancesAccessor.InstancesProperties.instance_not_stoppable_with_reasons.name());
    Assert.assertFalse(getStringSet(nonStoppableInstances, "instance0")
        .contains("HELIX:MIN_ACTIVE_REPLICA_CHECK_FAILED"));
    Assert.assertTrue(getStringSet(nonStoppableInstances, "instance1")
        .contains("HELIX:MIN_ACTIVE_REPLICA_CHECK_FAILED"));
    Assert.assertTrue(getStringSet(nonStoppableInstances, "instance2")
        .contains("HELIX:MIN_ACTIVE_REPLICA_CHECK_FAILED"));
    System.out.println("End test :" + TestHelper.getTestMethodName());
  }

  @Test(dependsOnMethods = "testMultipleReplicasInSameMZ")
  public void testSkipClusterLevelHealthCheck() throws IOException {
    System.out.println("Start test :" + TestHelper.getTestMethodName());
    String content = String.format(
        "{\"%s\":\"%s\",\"%s\":[\"%s\",\"%s\",\"%s\",\"%s\", \"%s\", \"%s\", \"%s\",\"%s\", \"%s\", \"%s\"]}",
        InstancesAccessor.InstancesProperties.selection_base.name(),
        InstancesAccessor.InstanceHealthSelectionBase.cross_zone_based.name(),
        InstancesAccessor.InstancesProperties.instances.name(), "instance1", "instance3",
        "instance6", "instance9", "instance10", "instance11", "instance12", "instance13",
        "instance14", "invalidInstance");

    // Change instance config of instance1 & instance0 to be evacuating
    String instance0 = "instance0";
    InstanceConfig instanceConfig =
        _configAccessor.getInstanceConfig(STOPPABLE_CLUSTER2, instance0);
    instanceConfig.setInstanceOperation(InstanceConstants.InstanceOperation.EVACUATE);
    _configAccessor.setInstanceConfig(STOPPABLE_CLUSTER2, instance0, instanceConfig);
    String instance1 = "instance1";
    InstanceConfig instanceConfig1 =
        _configAccessor.getInstanceConfig(STOPPABLE_CLUSTER2, instance1);
    instanceConfig1.setInstanceOperation(InstanceConstants.InstanceOperation.EVACUATE);
    _configAccessor.setInstanceConfig(STOPPABLE_CLUSTER2, instance1, instanceConfig1);
    RESTConfig restConfig = new RESTConfig(STOPPABLE_CLUSTER2);
    restConfig.set(RESTConfig.SimpleFields.CUSTOMIZED_HEALTH_URL, "http://localhost:1234");
    _configAccessor.setRESTConfig(STOPPABLE_CLUSTER2, restConfig);
    // It takes time to reflect the changes.
    BestPossibleExternalViewVerifier verifier =
        new BestPossibleExternalViewVerifier.Builder(STOPPABLE_CLUSTER2).setZkAddr(ZK_ADDR).build();
    Assert.assertTrue(verifier.verifyByPolling());

    Response response = new JerseyUriRequestBuilder(
        "clusters/{}/instances?command=stoppable&skipHealthCheckCategories=CUSTOM_AGGREGATED_CHECK").format(
        STOPPABLE_CLUSTER2).post(this, Entity.entity(content, MediaType.APPLICATION_JSON_TYPE));
    JsonNode jsonNode = OBJECT_MAPPER.readTree(response.readEntity(String.class));

    Set<String> stoppableSet = getStringSet(jsonNode,
        InstancesAccessor.InstancesProperties.instance_stoppable_parallel.name());
    Assert.assertTrue(stoppableSet.contains("instance12") && stoppableSet.contains("instance11")
        && stoppableSet.contains("instance10"));

    JsonNode nonStoppableInstances = jsonNode.get(
        InstancesAccessor.InstancesProperties.instance_not_stoppable_with_reasons.name());
    // instance0 and instance1 are evacuating but still host their replicas, so they keep their
    // siblings above min active replicas.
    Assert.assertTrue(stoppableSet.contains("instance13"));
    Assert.assertTrue(stoppableSet.contains("instance14"));
    Assert.assertEquals(getStringSet(nonStoppableInstances, "invalidInstance"),
        ImmutableSet.of("HELIX:INSTANCE_NOT_EXIST"));
    instanceConfig.setInstanceOperation(InstanceConstants.InstanceOperation.ENABLE);
    _configAccessor.setInstanceConfig(STOPPABLE_CLUSTER2, instance0, instanceConfig);
    instanceConfig1.setInstanceOperation(InstanceConstants.InstanceOperation.ENABLE);
    _configAccessor.setInstanceConfig(STOPPABLE_CLUSTER2, instance1, instanceConfig1);
    _configAccessor.deleteRESTConfig(STOPPABLE_CLUSTER2);
    System.out.println("End test :" + TestHelper.getTestMethodName());
  }

  @Test(dependsOnMethods = "testSkipClusterLevelHealthCheck")
  public void testNonTopoAwareStoppableCheck() throws JsonProcessingException {
    System.out.println("Start test :" + TestHelper.getTestMethodName());

    // STOPPABLE_CLUSTER3 is a cluster is non topology aware cluster
    String content = String.format(
        "{\"%s\":\"%s\",\"%s\":[\"%s\",\"%s\",\"%s\",\"%s\", \"%s\", \"%s\", \"%s\",\"%s\", \"%s\", \"%s\"], \"%s\":[\"%s\", \"%s\"]}",
        InstancesAccessor.InstancesProperties.selection_base.name(),
        InstancesAccessor.InstanceHealthSelectionBase.non_zone_based.name(),
        InstancesAccessor.InstancesProperties.instances.name(), "instance1", "instance3",
        "instance6", "instance9", "instance10", "instance11", "instance12", "instance13",
        "instance14", "invalidInstance",
        InstancesAccessor.InstancesProperties.skip_stoppable_check_list.name(), "INSTANCE_NOT_ENABLED", "INSTANCE_NOT_STABLE");

    // Change instance config of instance1 & instance0 to be evacuating
    String instance0 = "instance0";
    InstanceConfig instanceConfig =
        _configAccessor.getInstanceConfig(STOPPABLE_CLUSTER3, instance0);
    instanceConfig.setInstanceOperation(InstanceConstants.InstanceOperation.EVACUATE);
    _configAccessor.setInstanceConfig(STOPPABLE_CLUSTER3, instance0, instanceConfig);
    String instance1 = "instance1";
    InstanceConfig instanceConfig1 =
        _configAccessor.getInstanceConfig(STOPPABLE_CLUSTER3, instance1);
    instanceConfig1.setInstanceOperation(InstanceConstants.InstanceOperation.SWAP_IN);
    _configAccessor.setInstanceConfig(STOPPABLE_CLUSTER3, instance1, instanceConfig1);

    // It takes time to reflect the changes.
    BestPossibleExternalViewVerifier verifier =
        new BestPossibleExternalViewVerifier.Builder(STOPPABLE_CLUSTER3).setZkAddr(ZK_ADDR).build();
    Assert.assertTrue(verifier.verifyByPolling());

    Response response = new JerseyUriRequestBuilder(
        "clusters/{}/instances?command=stoppable&skipHealthCheckCategories=CUSTOM_INSTANCE_CHECK,CUSTOM_PARTITION_CHECK").format(
        STOPPABLE_CLUSTER3).post(this, Entity.entity(content, MediaType.APPLICATION_JSON_TYPE));
    JsonNode jsonNode = OBJECT_MAPPER.readTree(response.readEntity(String.class));

    Set<String> stoppableSet = getStringSet(jsonNode,
        InstancesAccessor.InstancesProperties.instance_stoppable_parallel.name());
    Assert.assertTrue(stoppableSet.contains("instance12") && stoppableSet.contains("instance3")
        && stoppableSet.contains("instance10"));

    JsonNode nonStoppableInstances = jsonNode.get(
        InstancesAccessor.InstancesProperties.instance_not_stoppable_with_reasons.name());
    // instance0 is evacuating but still hosts its replicas, so it keeps its siblings above min
    // active replicas. instance1 is SWAP_IN, which is not serving traffic and stays presumed
    // stopped.
    Assert.assertTrue(stoppableSet.contains("instance13"));
    Assert.assertEquals(getStringSet(nonStoppableInstances, "instance14"),
        ImmutableSet.of("HELIX:MIN_ACTIVE_REPLICA_CHECK_FAILED"));
    Assert.assertEquals(getStringSet(nonStoppableInstances, "invalidInstance"),
        ImmutableSet.of("HELIX:INSTANCE_NOT_EXIST"));
    instanceConfig.setInstanceOperation(InstanceConstants.InstanceOperation.ENABLE);
    _configAccessor.setInstanceConfig(STOPPABLE_CLUSTER3, instance0, instanceConfig);
    instanceConfig1.setInstanceOperation(InstanceConstants.InstanceOperation.ENABLE);
    _configAccessor.setInstanceConfig(STOPPABLE_CLUSTER3, instance1, instanceConfig1);

    System.out.println("End test :" + TestHelper.getTestMethodName());
  }

  @Test(dependsOnMethods = "testSkipClusterLevelHealthCheck")
  public void testNonTopoAwareStoppableCheckWithException() throws JsonProcessingException {
    System.out.println("Start test :" + TestHelper.getTestMethodName());

    // STOPPABLE_CLUSTER3 is a cluster is non topology aware cluster
    String content = String.format(
        "{\"%s\":\"%s\",\"%s\":[\"%s\",\"%s\",\"%s\",\"%s\", \"%s\", \"%s\", \"%s\",\"%s\", \"%s\", \"%s\"], \"%s\":[\"%s\", \"%s\"]}",
        InstancesAccessor.InstancesProperties.selection_base.name(),
        InstancesAccessor.InstanceHealthSelectionBase.cross_zone_based.name(),
        InstancesAccessor.InstancesProperties.instances.name(), "instance1", "instance3",
        "instance6", "instance9", "instance10", "instance11", "instance12", "instance13",
        "instance14", "invalidInstance",
        InstancesAccessor.InstancesProperties.skip_stoppable_check_list.name(), "INSTANCE_NOT_ENABLED", "INSTANCE_NOT_STABLE");

    // It takes time to reflect the changes.
    BestPossibleExternalViewVerifier verifier =
        new BestPossibleExternalViewVerifier.Builder(STOPPABLE_CLUSTER3).setZkAddr(ZK_ADDR).build();
    Assert.assertTrue(verifier.verifyByPolling());

    // Making the REST Call to cross zone stoppable check while the cluster has no topology aware
    // setup. The call should return an error.
    Response response = new JerseyUriRequestBuilder(
        "clusters/{}/instances?command=stoppable&skipHealthCheckCategories=CUSTOM_INSTANCE_CHECK,CUSTOM_PARTITION_CHECK").format(
        STOPPABLE_CLUSTER3)
        .isBodyReturnExpected(true)
        .expectedReturnStatusCode(Response.Status.BAD_REQUEST.getStatusCode())
        .post(this, Entity.entity(content, MediaType.APPLICATION_JSON_TYPE));

    System.out.println("End test :" + TestHelper.getTestMethodName());
  }

  @Test(description = "Test zone selection base with instance that don't have topology set in the config",
   dependsOnMethods = "testNonTopoAwareStoppableCheckWithException")
  public void testZoneSelectionBaseWithInstanceThatDontHaveTopologySet() {
    System.out.println("Start test :" + TestHelper.getTestMethodName());

    // STOPPABLE_CLUSTER3 is a cluster is non topology aware cluster
    String content = String.format(
        "{\"%s\":\"%s\",\"%s\":[\"%s\",\"%s\",\"%s\",\"%s\", \"%s\", \"%s\", \"%s\",\"%s\", \"%s\", \"%s\"], \"%s\":[\"%s\", \"%s\"]}",
        InstancesAccessor.InstancesProperties.selection_base.name(),
        InstancesAccessor.InstanceHealthSelectionBase.cross_zone_based.name(),
        InstancesAccessor.InstancesProperties.instances.name(), "instance1", "instance3",
        "instance6", "instance9", "instance10", "instance11", "instance12", "instance13",
        "instance14", "invalidInstance",
        InstancesAccessor.InstancesProperties.skip_stoppable_check_list.name(), "INSTANCE_NOT_ENABLED", "INSTANCE_NOT_STABLE");

    String instance1 = "instance1";
    InstanceConfig instanceConfig1 =
        _configAccessor.getInstanceConfig(STOPPABLE_CLUSTER2, instance1);
    String domain = instanceConfig1.getDomainAsString();
    instanceConfig1.setDomain("FALSE_DOMAIN");
    _configAccessor.setInstanceConfig(STOPPABLE_CLUSTER2, instance1, instanceConfig1);

    // It takes time to reflect the changes.
    BestPossibleExternalViewVerifier verifier =
        new BestPossibleExternalViewVerifier.Builder(STOPPABLE_CLUSTER3).setZkAddr(ZK_ADDR).build();
    Assert.assertTrue(verifier.verifyByPolling());

    // Making the REST Call to cross zone stoppable check while the cluster has no topology aware
    // setup. The call should return an error.
    Response response = new JerseyUriRequestBuilder(
        "clusters/{}/instances?command=stoppable&skipHealthCheckCategories=CUSTOM_INSTANCE_CHECK,CUSTOM_PARTITION_CHECK").format(
            STOPPABLE_CLUSTER3)
        .isBodyReturnExpected(true)
        .expectedReturnStatusCode(Response.Status.BAD_REQUEST.getStatusCode())
        .post(this, Entity.entity(content, MediaType.APPLICATION_JSON_TYPE));

    // Restore the changes on instance 1
    instanceConfig1.setDomain(domain);
    _configAccessor.setInstanceConfig(STOPPABLE_CLUSTER2, instance1, instanceConfig1);

    System.out.println("End test :" + TestHelper.getTestMethodName());
  }

  @Test
  public void testInstanceStoppableWithIncludeDetails() throws IOException {
    System.out.println("Start test :" + TestHelper.getTestMethodName());

    // Test the same scenario as testInstanceStoppableZoneBasedWithToBeStoppedInstances
    // but with includeDetails=true to get enhanced error messages
    String content = String.format(
        "{\"%s\":\"%s\",\"%s\":[\"%s\"], \"%s\":[\"%s\"]}",
        InstancesAccessor.InstancesProperties.selection_base.name(),
        InstancesAccessor.InstanceHealthSelectionBase.zone_based.name(),
        InstancesAccessor.InstancesProperties.instances.name(), "instance5",
        InstancesAccessor.InstancesProperties.to_be_stopped_instances.name(), "instance0");

    Response response = new JerseyUriRequestBuilder(
        "clusters/{}/instances?command=stoppable&includeDetails=true&skipHealthCheckCategories=CUSTOM_INSTANCE_CHECK,CUSTOM_PARTITION_CHECK").format(
        STOPPABLE_CLUSTER2).post(this, Entity.entity(content, MediaType.APPLICATION_JSON_TYPE));
    JsonNode jsonNode = OBJECT_MAPPER.readTree(response.readEntity(String.class));

    JsonNode nonStoppableInstances = jsonNode.get(
        InstancesAccessor.InstancesProperties.instance_not_stoppable_with_reasons.name());

    // Instance5 should not be stoppable due to MIN_ACTIVE_REPLICA_CHECK_FAILED
    // and should now have detailed information about which partition failed
    Set<String> instance5Reasons = getStringSet(nonStoppableInstances, "instance5");
    Assert.assertEquals(instance5Reasons.size(), 1);
    String reason = instance5Reasons.iterator().next();

    // With includeDetails=true, we should get a detailed message
    Assert.assertTrue(reason.startsWith("HELIX:MIN_ACTIVE_REPLICA_CHECK_FAILED"),
        "Expected detailed reason to start with HELIX:MIN_ACTIVE_REPLICA_CHECK_FAILED but got: " + reason);

    // The detailed message should contain partition information
    // Expected format: "HELIX:MIN_ACTIVE_REPLICA_CHECK_FAILED: Resource StoppableTestCluster2_db_0 partition StoppableTestCluster2_db_0_3 has 1/2 active replicas"
    Assert.assertTrue(reason.contains("partition"),
        "Expected detailed reason to contain partition information but got: " + reason);
    Assert.assertTrue(reason.contains("has"),
        "Expected detailed reason to contain 'has' but got: " + reason);
    Assert.assertTrue(reason.contains("active replicas"),
        "Expected detailed reason to contain 'active replicas' but got: " + reason);

    System.out.println("End test :" + TestHelper.getTestMethodName());
  }

  @Test
  public void testGetInstancesUnableToAcceptOnlineReplicas() throws Exception {
    System.out.println("Start test :" + TestHelper.getTestMethodName());

    // Dedicated cluster so the population asserted here is not perturbed by other tests.
    String clusterName = "TestOfflineBudgetCluster";
    _gSetupTool.addCluster(clusterName, true);
    _clusters.add(clusterName);
    List<String> instances =
        Arrays.asList("obInstance0", "obInstance1", "obInstance2", "obInstance3", "obInstance4");
    for (String instance : instances) {
      _gSetupTool.addInstanceToCluster(clusterName, instance);
    }
    // Baseline: no participants are running, so all 5 instances are offline and unmarked and all
    // 5 count.
    Assert.assertEquals(fetchOfflineBudgetPopulation(clusterName), sorted(instances));

    // Liveness alone is not enough to be excluded; the instance must be enabled AND live. Start
    // participants for obInstance3 and obInstance4, then mark obInstance4 DISABLE. obInstance3
    // (ENABLE + live) must drop out of the population, obInstance4 (live but DISABLE) must stay.
    // These two assertions are what pin the enabled-and-live rule: without a live participant the
    // rule is inert and the whole test passes even if liveness is never read.
    startInstances(clusterName, new TreeSet<>(Arrays.asList("obInstance3", "obInstance4")), 2);
    setInstanceOperation(clusterName, "obInstance4", InstanceConstants.InstanceOperation.DISABLE);

    List<String> expectedAfterStart =
        sorted(Arrays.asList("obInstance0", "obInstance1", "obInstance2", "obInstance4"));
    Assert.assertTrue(
        TestHelper.verify(() -> fetchOfflineBudgetPopulation(clusterName).equals(
            expectedAfterStart), TestHelper.WAIT_DURATION),
        "An enabled and live instance must be excluded, and a live DISABLE instance must still "
            + "count; got " + fetchOfflineBudgetPopulation(clusterName));

    // A valid marker exempts an instance; an expired marker does not. A SWAP_IN instance is
    // never counted regardless of marker state.
    long nowMs = System.currentTimeMillis();
    setInstanceOperationMaintenanceUntilMs(clusterName, "obInstance0", nowMs + 600_000L);
    setInstanceOperationMaintenanceUntilMs(clusterName, "obInstance1", nowMs - 1L);
    setInstanceOperation(clusterName, "obInstance2", InstanceConstants.InstanceOperation.SWAP_IN);

    Assert.assertEquals(fetchOfflineBudgetPopulation(clusterName),
        sorted(Arrays.asList("obInstance1", "obInstance4")),
        "Valid marker and SWAP_IN must be excluded; the expired marker must still count");

    // The population is a property of the instances alone. Changing the budget thresholds the
    // controller compares it against must not change who is in it.
    ClusterConfig clusterConfig = _configAccessor.getClusterConfig(clusterName);
    clusterConfig.setMaxOfflineInstancesAllowed(1);
    clusterConfig.setNumOfflineInstancesForAutoExit(0);
    _configAccessor.setClusterConfig(clusterName, clusterConfig);
    Assert.assertEquals(fetchOfflineBudgetPopulation(clusterName),
        sorted(Arrays.asList("obInstance1", "obInstance4")),
        "Offline-budget thresholds must not affect which instances are counted");

    // An unknown cluster must 404 rather than answer with an empty population, which would read
    // as "the whole offline budget is free". This deliberately differs from getAllInstances and
    // validateWeight on this route, which both answer 200 with an empty body.
    new JerseyUriRequestBuilder(
        "clusters/{}/instances?command=getInstancesUnableToAcceptOnlineReplicas")
        .expectedReturnStatusCode(Response.Status.NOT_FOUND.getStatusCode())
        .format("TestOfflineBudgetClusterDoesNotExist").get(this);

    System.out.println("End test :" + TestHelper.getTestMethodName());
  }

  @Test
  public void testGetDelayedRebalanceStatus() throws Exception {
    System.out.println("Start test :" + TestHelper.getTestMethodName());

    // Dedicated cluster with no controller: the endpoint answers from metadata alone, and a
    // controller running against the same cluster would keep rewriting that metadata underneath
    // the assertions.
    String clusterName = "TestDelayedRebalanceCluster";
    _gSetupTool.addCluster(clusterName, true);
    _clusters.add(clusterName);

    String offlineInWindow = "drInstance0";
    String disabledLive = "drInstance1";
    String enabledLive = "drInstance2";
    String delayOptedOut = "drInstance3";
    String evacuating = "drInstance4";
    String swappingIn = "drInstance5";
    String unknownOperation = "drInstance6";
    String disabledLongAgo = "drInstance7";
    String neverJoined = "drInstance8";
    List<String> instances =
        Arrays.asList(offlineInWindow, disabledLive, enabledLive, delayOptedOut, evacuating,
            swappingIn, unknownOperation, disabledLongAgo, neverJoined);
    for (String instance : instances) {
      _gSetupTool.addInstanceToCluster(clusterName, instance);
    }
    // Instance creation initializes history; remove this one to cover genuinely absent metadata.
    Assert.assertTrue(_baseAccessor.remove(
        PropertyPathBuilder.instanceHistory(clusterName, neverJoined), AccessOption.PERSISTENT));
    Assert.assertNull(readParticipantHistory(clusterName, neverJoined));

    // Wide enough that no window opened during this test can expire while it runs.
    long delayMs = 10 * 60 * 1000L;
    ClusterConfig clusterConfig = _configAccessor.getClusterConfig(clusterName);
    clusterConfig.setDelayRebalaceEnabled(true);
    clusterConfig.setRebalanceDelayTime(delayMs);
    _configAccessor.setClusterConfig(clusterName, clusterConfig);

    // Every instance is offline and none has a recorded offline time yet, so nothing is being
    // retained. An empty population is a meaningful answer and is not an error.
    JsonNode emptyStatus = fetchDelayedRebalanceStatus(clusterName);
    assertDelayedRebalanceEnvelope(emptyStatus, clusterName);
    Assert.assertTrue(emptyStatus.get(DELAYED_INSTANCES).isEmpty(),
        "Instances with no recorded offline time are not in a delay window");

    startInstances(clusterName, new TreeSet<>(Arrays.asList(disabledLive, enabledLive)), 2);

    long offlineTime = recordOfflineTime(clusterName, offlineInWindow);
    recordOfflineTime(clusterName, delayOptedOut);
    recordOfflineTime(clusterName, evacuating);
    recordOfflineTime(clusterName, swappingIn);
    recordOfflineTime(clusterName, unknownOperation);

    disableInstance(clusterName, disabledLive);
    disableInstance(clusterName, disabledLongAgo);
    // A disable recorded on the cluster config that predates the window: the calculation takes the
    // earlier of the two disable timestamps, so this instance is already out of its window.
    setBatchDisableTimestamp(clusterName, disabledLongAgo,
        System.currentTimeMillis() - 2 * delayMs);

    InstanceConfig optedOutConfig = _configAccessor.getInstanceConfig(clusterName, delayOptedOut);
    optedOutConfig.setDelayRebalanceEnabled(false);
    _configAccessor.setInstanceConfig(clusterName, delayOptedOut, optedOutConfig);

    setInstanceOperation(clusterName, evacuating, InstanceConstants.InstanceOperation.EVACUATE);
    setInstanceOperation(clusterName, swappingIn, InstanceConstants.InstanceOperation.SWAP_IN);
    setInstanceOperation(clusterName, unknownOperation, InstanceConstants.InstanceOperation.UNKNOWN);
    ParticipantHistory recordedHistory = readParticipantHistory(clusterName, offlineInWindow);

    Set<String> expectedDelayedInstances = ImmutableSet.of(offlineInWindow, disabledLive);
    Assert.assertTrue(TestHelper.verify(
        () -> fieldNames(fetchDelayedRebalanceStatus(clusterName).get(DELAYED_INSTANCES))
            .equals(expectedDelayedInstances), TestHelper.WAIT_DURATION),
        "Expected exactly the delay-retained instances but got " + fieldNames(
            fetchDelayedRebalanceStatus(clusterName).get(DELAYED_INSTANCES)));

    long beforeCallMs = System.currentTimeMillis();
    JsonNode status = fetchDelayedRebalanceStatus(clusterName);
    long afterCallMs = System.currentTimeMillis();

    assertDelayedRebalanceEnvelope(status, clusterName);
    long observedAtMillis = status.get(OBSERVED_AT_MILLIS).longValue();
    Assert.assertTrue(observedAtMillis >= beforeCallMs && observedAtMillis <= afterCallMs,
        "observedAtMillis must be the server time the answer was computed at, but " + observedAtMillis
            + " is outside [" + beforeCallMs + ", " + afterCallMs + "]");

    JsonNode delayedInstances = status.get(DELAYED_INSTANCES);
    Assert.assertEquals(fieldNames(delayedInstances), expectedDelayedInstances,
        "Only delay-retained instances are reported: an enabled and live instance is active on "
            + "its own, an instance that opted out of delayed rebalance and instances under a "
            + "non-assignable operation are never retained, and a window that has expired is over");

    JsonNode offlineEntry = delayedInstances.get(offlineInWindow);
    Assert.assertEquals(fieldNames(offlineEntry),
        ImmutableSet.of(EXPIRES_AT_MILLIS, LIVE, ENABLED));
    Assert.assertTrue(offlineEntry.get(EXPIRES_AT_MILLIS).isIntegralNumber());
    Assert.assertEquals(offlineEntry.get(EXPIRES_AT_MILLIS).longValue(), offlineTime + delayMs,
        "The window closes at the recorded offline time plus the cluster delay");
    Assert.assertTrue(offlineEntry.get(EXPIRES_AT_MILLIS).longValue() > observedAtMillis,
        "A reported instance is still inside its window at the observation timestamp");
    Assert.assertFalse(offlineEntry.get(LIVE).booleanValue());
    Assert.assertTrue(offlineEntry.get(ENABLED).booleanValue());

    JsonNode disabledEntry = delayedInstances.get(disabledLive);
    Assert.assertEquals(fieldNames(disabledEntry),
        ImmutableSet.of(EXPIRES_AT_MILLIS, LIVE, ENABLED));
    Assert.assertTrue(disabledEntry.get(EXPIRES_AT_MILLIS).longValue() > observedAtMillis);
    Assert.assertTrue(disabledEntry.get(LIVE).booleanValue(),
        "A disabled instance is retained while it is still live, and is reported as live");
    Assert.assertFalse(disabledEntry.get(ENABLED).booleanValue());

    // The read must not create or update participant history the way the controller's refresh
    // does, so an instance that never joined still has none and a recorded history is untouched.
    Assert.assertNull(readParticipantHistory(clusterName, neverJoined),
        "Reading the status must not create participant history");
    ParticipantHistory afterReads = readParticipantHistory(clusterName, offlineInWindow);
    Assert.assertEquals(afterReads.getRecord(), recordedHistory.getRecord(),
        "Reading the status must not change participant history");
    Assert.assertEquals(afterReads.getRecord().getVersion(), recordedHistory.getRecord().getVersion(),
        "Reading the status must not write participant history");

    // The cluster-level switch turns the whole population off rather than reporting a population
    // no rebalancer is acting on.
    clusterConfig = _configAccessor.getClusterConfig(clusterName);
    clusterConfig.setDelayRebalaceEnabled(false);
    _configAccessor.setClusterConfig(clusterName, clusterConfig);
    JsonNode disabledStatus = fetchDelayedRebalanceStatus(clusterName);
    Assert.assertFalse(disabledStatus.get(DELAY_ENABLED).booleanValue());
    Assert.assertTrue(disabledStatus.get(DELAYED_INSTANCES).isEmpty(),
        "Delayed rebalance being off for the cluster means nothing is being retained");

    clusterConfig.setDelayRebalaceEnabled(true);
    _configAccessor.setClusterConfig(clusterName, clusterConfig);
    Assert.assertEquals(fieldNames(fetchDelayedRebalanceStatus(clusterName).get(DELAYED_INSTANCES)),
        expectedDelayedInstances, "Turning the switch back on restores the same population");

    // Required cluster metadata that cannot be read must surface as an error. An empty population
    // would read as "no instance is in a delay window", which is the answer a caller acts on.
    String clusterConfigPath = PropertyPathBuilder.clusterConfig(clusterName);
    ZNRecord clusterConfigRecord = _baseAccessor.get(clusterConfigPath, null, AccessOption.PERSISTENT);
    Assert.assertTrue(_baseAccessor.remove(clusterConfigPath, AccessOption.PERSISTENT));
    new JerseyUriRequestBuilder("clusters/{}/instances?command=getDelayedRebalanceStatus")
        .expectedReturnStatusCode(Response.Status.NOT_FOUND.getStatusCode()).format(clusterName)
        .get(this);
    Assert.assertTrue(
        _baseAccessor.set(clusterConfigPath, clusterConfigRecord, AccessOption.PERSISTENT));
    Assert.assertEquals(fieldNames(fetchDelayedRebalanceStatus(clusterName).get(DELAYED_INSTANCES)),
        expectedDelayedInstances);

    new JerseyUriRequestBuilder("clusters/{}/instances?command=getDelayedRebalanceStatus")
        .expectedReturnStatusCode(Response.Status.NOT_FOUND.getStatusCode())
        .format("TestDelayedRebalanceClusterDoesNotExist").get(this);

    System.out.println("End test :" + TestHelper.getTestMethodName());
  }

  private void assertDelayedRebalanceEnvelope(JsonNode status, String clusterName) {
    Assert.assertEquals(fieldNames(status),
        ImmutableSet.of(InstancesAccessor.Properties.id.name(), SCOPE, LIVE_VIEW,
            OBSERVED_AT_MILLIS, DELAY_ENABLED, DELAYED_INSTANCES));
    Assert.assertEquals(status.get(InstancesAccessor.Properties.id.name()).textValue(),
        clusterName);
    Assert.assertEquals(status.get(SCOPE).textValue(), "CLUSTER_DEFAULT",
        "This command answers with the cluster-default rules, not resource overrides");
    Assert.assertEquals(status.get(LIVE_VIEW).textValue(), "RAW",
        "Liveness is the raw ZooKeeper membership");
    Assert.assertTrue(status.get(OBSERVED_AT_MILLIS).isIntegralNumber());
    Assert.assertTrue(status.get(DELAY_ENABLED).isBoolean());
    Assert.assertTrue(status.get(DELAYED_INSTANCES).isObject());
  }

  private JsonNode fetchDelayedRebalanceStatus(String clusterName) {
    try {
      return OBJECT_MAPPER.readTree(
          new JerseyUriRequestBuilder("clusters/{}/instances?command=getDelayedRebalanceStatus")
              .isBodyReturnExpected(true).format(clusterName).get(this));
    } catch (IOException e) {
      throw new IllegalStateException("Failed to read the delayed rebalance status response", e);
    }
  }

  /**
   * Records that an instance went offline, which is what the controller does when it first sees a
   * participant missing. The endpoint reads this metadata and must never write it.
   *
   * @return the recorded offline timestamp.
   */
  private long recordOfflineTime(String clusterName, String instanceName) {
    HelixDataAccessor accessor = new ZKHelixDataAccessor(clusterName, _baseAccessor);
    ParticipantHistory history = new ParticipantHistory(instanceName);
    history.reportOffline();
    Assert.assertTrue(accessor.setProperty(accessor.keyBuilder().participantHistory(instanceName),
        history));
    return history.getLastOfflineTime();
  }

  private ParticipantHistory readParticipantHistory(String clusterName, String instanceName) {
    HelixDataAccessor accessor = new ZKHelixDataAccessor(clusterName, _baseAccessor);
    return accessor.getProperty(accessor.keyBuilder().participantHistory(instanceName));
  }

  private void disableInstance(String clusterName, String instanceName) {
    InstanceConfig instanceConfig = _configAccessor.getInstanceConfig(clusterName, instanceName);
    instanceConfig.setInstanceOperation(new InstanceConfig.InstanceOperation.Builder().setOperation(
        InstanceConstants.InstanceOperation.DISABLE).build());
    _configAccessor.setInstanceConfig(clusterName, instanceName, instanceConfig);
  }

  private void setBatchDisableTimestamp(String clusterName, String instanceName,
      long disabledTimeMs) {
    ClusterConfig clusterConfig = _configAccessor.getClusterConfig(clusterName);
    clusterConfig.setDisabledInstancesWithInfo(Collections.singletonMap(instanceName,
        ClusterConfig.ClusterConfigProperty.HELIX_ENABLED_DISABLE_TIMESTAMP + "="
            + disabledTimeMs));
    _configAccessor.setClusterConfig(clusterName, clusterConfig);
  }

  private static Set<String> fieldNames(JsonNode node) {
    Set<String> names = new HashSet<>();
    node.fieldNames().forEachRemaining(names::add);
    return names;
  }

  private List<String> fetchOfflineBudgetPopulation(String clusterName) throws IOException {
    JsonNode node = OBJECT_MAPPER.readTree(
        new JerseyUriRequestBuilder(
            "clusters/{}/instances?command=getInstancesUnableToAcceptOnlineReplicas")
            .isBodyReturnExpected(true).format(clusterName).get(this));
    return getSortedStringList(node,
        InstancesAccessor.InstancesProperties.instances_unable_to_accept_online_replicas.name());
  }

  private void setInstanceOperation(String clusterName, String instanceName,
      InstanceConstants.InstanceOperation operation) {
    InstanceConfig instanceConfig = _configAccessor.getInstanceConfig(clusterName, instanceName);
    instanceConfig.setInstanceOperation(
        new InstanceConfig.InstanceOperation.Builder().setOperation(operation).build());
    _configAccessor.setInstanceConfig(clusterName, instanceName, instanceConfig);
  }

  private void setInstanceOperationMaintenanceUntilMs(String clusterName, String instanceName,
      long untilMs) {
    InstanceConfig instanceConfig = _configAccessor.getInstanceConfig(clusterName, instanceName);
    instanceConfig.setInstanceOperationMaintenanceUntilMs(untilMs);
    _configAccessor.setInstanceConfig(clusterName, instanceName, instanceConfig);
  }

  private static List<String> sorted(Collection<String> names) {
    List<String> result = new ArrayList<>(names);
    Collections.sort(result);
    return result;
  }

  /**
   * Reads a JSON array field as a sorted list. Sorting both sides keeps the comparison
   * order-insensitive while still producing a readable diff on failure (TestNG compares
   * collections element-by-element in iteration order).
   */
  private List<String> getSortedStringList(JsonNode jsonNode, String key) {
    return sorted(getStringSet(jsonNode, key));
  }

  private Set<String> getStringSet(JsonNode jsonNode, String key) {
    Set<String> result = new HashSet<>();
    jsonNode.withArray(key).forEach(s -> result.add(s.textValue()));
    return result;
  }
}
