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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.function.Consumer;
import javax.ws.rs.client.Entity;
import javax.ws.rs.core.MediaType;

import com.google.common.collect.ImmutableMap;
import org.apache.helix.TestHelper;
import org.apache.helix.integration.manager.ClusterControllerManager;
import org.apache.helix.integration.manager.MockParticipantManager;
import org.apache.helix.model.ClusterConfig;
import org.apache.helix.rest.server.resources.helix.InstancesAccessor;
import org.apache.helix.rest.server.util.JerseyUriRequestBuilder;
import org.apache.helix.tools.ClusterVerifiers.StrictMatchExternalViewVerifier;
import org.apache.helix.tools.ClusterVerifiers.ZkHelixClusterVerifier;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

/**
 * Covers the rebalanceIfMinActiveReplicaNotMet option of both stoppable endpoints. The cluster has
 * three instances and one resource with three replicas that all have to stay up, so every instance
 * fails the min active replica check and the option is always considered.
 */
public class TestStoppableCheckRebalance extends AbstractTestClass {
  private static final String CLUSTER_NAME = "TestRebalanceOnStoppableCheckCluster";
  private static final String RESOURCE_NAME = CLUSTER_NAME + "_db";
  private static final List<String> INSTANCES =
      Arrays.asList(CLUSTER_NAME + "localhost_12930", CLUSTER_NAME + "localhost_12931",
          CLUSTER_NAME + "localhost_12932");
  private static final String DISABLED_INSTANCE = INSTANCES.get(2);
  private static final long ONE_HOUR = 3600000L;
  private static final long NEVER_REBALANCED = -1L;

  private static final String BATCH_URI = "clusters/{}/instances?command=stoppable"
      + "&skipHealthCheckCategories=CUSTOM_INSTANCE_CHECK,CUSTOM_PARTITION_CHECK";
  private static final String PER_INSTANCE_URI = "clusters/{}/instances/{}/stoppable"
      + "?skipHealthCheckCategories=CUSTOM_INSTANCE_CHECK,CUSTOM_PARTITION_CHECK";
  private static final String REBALANCE_PARAM = "&rebalanceIfMinActiveReplicaNotMet=true";

  @BeforeClass
  public void beforeClass() throws Exception {
    _gSetupTool.addCluster(CLUSTER_NAME, true);
    _instancesMap.put(CLUSTER_NAME, new HashSet<>());
    _liveInstancesMap.put(CLUSTER_NAME, new HashSet<>());
    for (String instance : INSTANCES) {
      addParticipant(CLUSTER_NAME, instance);
    }
    addResource(CLUSTER_NAME, RESOURCE_NAME, 4, "MasterSlave", INSTANCES.size(), INSTANCES.size());
    _clusterControllerManagers.add(startController(CLUSTER_NAME));
    Assert.assertTrue(waitTillConverged());
  }

  @AfterClass
  public void afterClass() {
    deleteTestCluster(CLUSTER_NAME);
  }

  @Test
  public void testDefaultRequestIsUnchanged() throws Exception {
    System.out.println("Start test :" + TestHelper.getTestMethodName());
    prepareRebalanceCandidate();

    String batchDefault = postBatchStoppable("");
    Assert.assertTrue(batchDefault.contains("MIN_ACTIVE_REPLICA_CHECK_FAILED"),
        "Expected a min active replica failure, got " + batchDefault);
    Assert.assertEquals(postBatchStoppable("&rebalanceIfMinActiveReplicaNotMet=false"),
        batchDefault);
    Assert.assertEquals(lastOnDemandRebalanceTimestamp(), NEVER_REBALANCED);

    String perInstanceDefault = postPerInstanceStoppable("");
    Assert.assertTrue(perInstanceDefault.contains("MIN_ACTIVE_REPLICA_CHECK_FAILED"),
        "Expected a min active replica failure, got " + perInstanceDefault);
    Assert.assertEquals(postPerInstanceStoppable("&rebalanceIfMinActiveReplicaNotMet=false"),
        perInstanceDefault);
    Assert.assertEquals(lastOnDemandRebalanceTimestamp(), NEVER_REBALANCED);

    // Opting in reports the same answer, only the side effect differs.
    Assert.assertEquals(postBatchStoppable(REBALANCE_PARAM), batchDefault);
    System.out.println("End test :" + TestHelper.getTestMethodName());
  }

  @Test
  public void testRebalanceRequestedWhenAllGatesPass() throws Exception {
    System.out.println("Start test :" + TestHelper.getTestMethodName());
    prepareRebalanceCandidate();

    long beforeRequest = System.currentTimeMillis();
    postBatchStoppable(REBALANCE_PARAM);
    Assert.assertTrue(lastOnDemandRebalanceTimestamp() >= beforeRequest,
        "Expected an on-demand rebalance timestamp, got " + lastOnDemandRebalanceTimestamp());

    // The per instance endpoint takes the same option.
    prepareRebalanceCandidate();
    beforeRequest = System.currentTimeMillis();
    postPerInstanceStoppable(REBALANCE_PARAM);
    Assert.assertTrue(lastOnDemandRebalanceTimestamp() >= beforeRequest,
        "Expected an on-demand rebalance timestamp, got " + lastOnDemandRebalanceTimestamp());
    System.out.println("End test :" + TestHelper.getTestMethodName());
  }

  @Test
  public void testNoRebalanceWhenNoInstanceIsInDelayedWindow() throws Exception {
    System.out.println("Start test :" + TestHelper.getTestMethodName());
    prepareRebalanceCandidate();
    enableInstance(DISABLED_INSTANCE, true);
    Assert.assertTrue(waitTillConverged());

    postBatchStoppable(REBALANCE_PARAM);
    Assert.assertEquals(lastOnDemandRebalanceTimestamp(), NEVER_REBALANCED,
        "A rebalance cannot move anything while every instance is live and enabled");
    System.out.println("End test :" + TestHelper.getTestMethodName());
  }

  @Test(dependsOnMethods = {"testDefaultRequestIsUnchanged", "testRebalanceRequestedWhenAllGatesPass",
      "testNoRebalanceWhenNoInstanceIsInDelayedWindow"})
  public void testNoRebalanceWhenClusterIsNotConverged() throws Exception {
    System.out.println("Start test :" + TestHelper.getTestMethodName());
    prepareRebalanceCandidate();

    // Stopping the controller and then an instance leaves the external view describing a replica
    // that is no longer there, which is what an unconverged cluster looks like.
    stopController();
    stopParticipant(INSTANCES.get(1));

    postBatchStoppable(REBALANCE_PARAM);
    Assert.assertEquals(lastOnDemandRebalanceTimestamp(), NEVER_REBALANCED,
        "An unconverged cluster should not be rebalanced");
    System.out.println("End test :" + TestHelper.getTestMethodName());
  }

  private void prepareRebalanceCandidate() throws Exception {
    setClusterConfig(config -> {
      config.setDelayRebalaceEnabled(true);
      config.setRebalanceDelayTime(ONE_HOUR);
      config.setLastOnDemandRebalanceTimestamp(NEVER_REBALANCED);
    });
    enableInstance(DISABLED_INSTANCE, false);
    Assert.assertTrue(waitTillConverged());
  }

  private void setClusterConfig(Consumer<ClusterConfig> update) {
    ClusterConfig clusterConfig = _configAccessor.getClusterConfig(CLUSTER_NAME);
    update.accept(clusterConfig);
    _configAccessor.setClusterConfig(CLUSTER_NAME, clusterConfig);
  }

  private void enableInstance(String instance, boolean enabled) {
    _gSetupTool.getClusterManagementTool().enableInstance(CLUSTER_NAME, instance, enabled);
  }

  private long lastOnDemandRebalanceTimestamp() {
    return _configAccessor.getClusterConfig(CLUSTER_NAME).getLastOnDemandRebalanceTimestamp();
  }

  private boolean waitTillConverged() {
    ZkHelixClusterVerifier verifier =
        new StrictMatchExternalViewVerifier.Builder(CLUSTER_NAME).setZkClient(_gZkClient)
            .setLenientMatch(true).build();
    try {
      return verifier.verifyByPolling(TestHelper.WAIT_DURATION, 100);
    } finally {
      verifier.close();
    }
  }

  private void stopController() {
    for (ClusterControllerManager controller : new ArrayList<>(_clusterControllerManagers)) {
      if (CLUSTER_NAME.equals(controller.getClusterName())) {
        controller.syncStop();
      }
    }
  }

  private void stopParticipant(String instance) {
    for (MockParticipantManager participant : new ArrayList<>(_mockParticipantManagers)) {
      if (instance.equals(participant.getInstanceName())) {
        participant.syncStop();
      }
    }
  }

  private String postBatchStoppable(String extraQuery) throws Exception {
    String content = String.format("{\"%s\":\"%s\",\"%s\":[\"%s\",\"%s\",\"%s\"]}",
        InstancesAccessor.InstancesProperties.selection_base.name(),
        InstancesAccessor.InstanceHealthSelectionBase.non_zone_based.name(),
        InstancesAccessor.InstancesProperties.instances.name(), INSTANCES.get(0), INSTANCES.get(1),
        INSTANCES.get(2));
    return new JerseyUriRequestBuilder(BATCH_URI + extraQuery).format(CLUSTER_NAME)
        .post(this, Entity.entity(content, MediaType.APPLICATION_JSON_TYPE))
        .readEntity(String.class);
  }

  private String postPerInstanceStoppable(String extraQuery) throws Exception {
    Entity entity = Entity.entity(OBJECT_MAPPER.writeValueAsString(ImmutableMap.of()),
        MediaType.APPLICATION_JSON_TYPE);
    return new JerseyUriRequestBuilder(PER_INSTANCE_URI + extraQuery)
        .format(CLUSTER_NAME, INSTANCES.get(0)).post(this, entity).readEntity(String.class);
  }
}
