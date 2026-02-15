package org.apache.helix.sharding.integration;

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
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.helix.HelixAdmin;
import org.apache.helix.common.ZkTestBase;
import org.apache.helix.integration.manager.ClusterControllerManager;
import org.apache.helix.model.IdealState;
import org.apache.helix.sharding.HelixShardingAdmin;
import org.apache.helix.sharding.HelixShardingNode;
import org.apache.helix.sharding.ShardingRebalanceStrategy;
import org.apache.helix.tools.ClusterVerifiers.BestPossibleExternalViewVerifier;
import org.testng.Assert;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.Test;


/**
 * Integration tests for admin-level operations against a live cluster.
 */
public class TestShardingAdminOperations extends ZkTestBase {

  private final List<HelixShardingNode> nodes = new ArrayList<>();
  private final List<ClusterControllerManager> controllers = new ArrayList<>();
  private HelixShardingAdmin admin;

  @AfterMethod
  public void teardown() {
    for (HelixShardingNode node : nodes) {
      try { node.stop(); } catch (Exception e) { /* ignore */ }
    }
    nodes.clear();
    for (ClusterControllerManager ctrl : controllers) {
      try { ctrl.syncStop(); } catch (Exception e) { /* ignore */ }
    }
    controllers.clear();
    if (admin != null) { admin.close(); admin = null; }
  }

  private String uniqueCluster(String suffix) {
    return "TestAdminOps_" + suffix + "_" + System.currentTimeMillis();
  }

  private HelixShardingAdmin createAdmin() {
    admin = new HelixShardingAdmin.Builder().zkAddress(ZK_ADDR).build();
    return admin;
  }

  private ClusterControllerManager startController(String clusterName) {
    ClusterControllerManager ctrl =
        new ClusterControllerManager(ZK_ADDR, clusterName, "controller_0");
    ctrl.syncStart();
    controllers.add(ctrl);
    return ctrl;
  }

  private BestPossibleExternalViewVerifier createVerifier(String clusterName) {
    return new BestPossibleExternalViewVerifier.Builder(clusterName)
        .setZkAddress(ZK_ADDR)
        .setWaitTillVerify(1000)
        .build();
  }

  @Test
  public void testExpandPartitions() throws Exception {
    String clusterName = uniqueCluster("expand");
    createAdmin();
    admin.addCluster(clusterName, 16, 1, ShardingRebalanceStrategy.AUTO);
    startController(clusterName);

    Set<String> leaders = ConcurrentHashMap.newKeySet();
    HelixShardingNode node = new HelixShardingNode.Builder()
        .clusterName(clusterName)
        .zkAddress(ZK_ADDR)
        .instanceName("localhost_12918")
        .onStateTransition((p, f, t) -> {
          if ("LEADER".equals(t)) leaders.add(p);
          else leaders.remove(p);
        })
        .build();
    node.start();
    nodes.add(node);

    Assert.assertTrue(createVerifier(clusterName).verifyByPolling());
    Assert.assertEquals(leaders.size(), 16, "Should have 16 partitions initially");

    // Expand to 32
    admin.expandPartitions(clusterName, 32);

    Assert.assertTrue(createVerifier(clusterName).verifyByPolling());

    // Verify IdealState has 32 partitions
    IdealState is = admin.getResourceIdealState(clusterName);
    Assert.assertEquals(is.getNumPartitions(), 32, "IdealState should have 32 partitions");

    // Node should eventually get all 32 partitions
    Assert.assertEquals(leaders.size(), 32,
        "Node should own all 32 partitions after expansion");
  }

  @Test
  public void testExpandPartitionsNoOpWhenSameOrSmaller() {
    String clusterName = uniqueCluster("expandNoop");
    createAdmin();
    admin.addCluster(clusterName, 16, 1, ShardingRebalanceStrategy.AUTO);

    // Expand to same size — should be no-op
    admin.expandPartitions(clusterName, 16);
    IdealState is = admin.getResourceIdealState(clusterName);
    Assert.assertEquals(is.getNumPartitions(), 16);

    // Expand to smaller — should be no-op
    admin.expandPartitions(clusterName, 8);
    is = admin.getResourceIdealState(clusterName);
    Assert.assertEquals(is.getNumPartitions(), 16,
        "Partition count should not decrease");

    admin.dropCluster(clusterName);
  }

  @Test
  public void testGetResourceIdealState() {
    String clusterName = uniqueCluster("idealState");
    createAdmin();
    admin.addCluster(clusterName, 24, 3, ShardingRebalanceStrategy.CRUSH);

    IdealState is = admin.getResourceIdealState(clusterName);
    Assert.assertNotNull(is);
    Assert.assertEquals(is.getNumPartitions(), 24);
    Assert.assertEquals(Integer.parseInt(is.getReplicas()), 3);
    Assert.assertEquals(is.getRebalanceMode(), IdealState.RebalanceMode.FULL_AUTO);

    admin.dropCluster(clusterName);
  }

  @Test
  public void testGetHelixAdmin() throws Exception {
    String clusterName = uniqueCluster("helixAdmin");
    createAdmin();
    admin.addCluster(clusterName, 8, 1, ShardingRebalanceStrategy.AUTO);
    startController(clusterName);

    HelixShardingNode node = new HelixShardingNode.Builder()
        .clusterName(clusterName)
        .zkAddress(ZK_ADDR)
        .instanceName("localhost_12918")
        .onStateTransition((p, f, t) -> { })
        .build();
    node.start();
    nodes.add(node);

    Assert.assertTrue(createVerifier(clusterName).verifyByPolling());

    // Get underlying HelixAdmin and verify it works
    HelixAdmin helixAdmin = admin.getHelixAdmin();
    Assert.assertNotNull(helixAdmin);

    List<String> instances = helixAdmin.getInstancesInCluster(clusterName);
    Assert.assertTrue(instances.contains("localhost_12918"),
        "HelixAdmin should list the participant instance");

    List<String> resources = helixAdmin.getResourcesInCluster(clusterName);
    Assert.assertTrue(resources.contains(HelixShardingAdmin.DEFAULT_RESOURCE_NAME),
        "HelixAdmin should list the sharding resource");
  }

  @Test
  public void testAddClusterIdempotent() {
    String clusterName = uniqueCluster("idempotent");
    createAdmin();

    // First creation
    admin.addCluster(clusterName, 16, 2, ShardingRebalanceStrategy.AUTO);

    // Second creation with same name should not throw
    // (Helix addCluster with recreateIfExists=false returns silently if cluster exists)
    try {
      admin.addCluster(clusterName, 16, 2, ShardingRebalanceStrategy.AUTO);
      // If we get here without exception, it's fine for the POC
      // The state model def and resource will be re-added but that's idempotent
    } catch (Exception e) {
      // Some Helix versions may throw if the resource already exists
      // That's acceptable behavior — log it
      System.out.println("addCluster second call threw (acceptable): " + e.getMessage());
    }

    // Verify cluster still works
    IdealState is = admin.getResourceIdealState(clusterName);
    Assert.assertNotNull(is, "Cluster should still be valid after second addCluster");

    admin.dropCluster(clusterName);
  }

  @Test
  public void testDropClusterCleansUp() {
    String clusterName = uniqueCluster("cleanup");
    createAdmin();
    admin.addCluster(clusterName, 8, 1, ShardingRebalanceStrategy.AUTO);

    Assert.assertTrue(admin.getHelixAdmin().getClusters().contains(clusterName));

    admin.dropCluster(clusterName);

    Assert.assertFalse(admin.getHelixAdmin().getClusters().contains(clusterName),
        "Cluster should be completely removed after drop");
  }
}
