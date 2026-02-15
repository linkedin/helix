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
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.helix.common.ZkTestBase;
import org.apache.helix.integration.manager.ClusterControllerManager;
import org.apache.helix.model.ExternalView;
import org.apache.helix.sharding.HelixShardingAdmin;
import org.apache.helix.sharding.HelixShardingNode;
import org.apache.helix.sharding.ShardingRebalanceStrategy;
import org.apache.helix.tools.ClusterVerifiers.BestPossibleExternalViewVerifier;
import org.testng.Assert;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.Test;


/**
 * Integration tests for failover and recovery scenarios.
 */
public class TestShardingNodeFailover extends ZkTestBase {

  private static final int NUM_PARTITIONS = 16;

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
    return "TestFailover_" + suffix + "_" + System.currentTimeMillis();
  }

  private HelixShardingAdmin createAdmin() {
    admin = new HelixShardingAdmin.Builder().zkAddress(ZK_ADDR).build();
    return admin;
  }

  private ClusterControllerManager startController(String clusterName, String controllerName) {
    ClusterControllerManager ctrl =
        new ClusterControllerManager(ZK_ADDR, clusterName, controllerName);
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

  private HelixShardingNode startNode(String clusterName, String instanceName,
      Set<String> leaderPartitions) throws Exception {
    HelixShardingNode node = new HelixShardingNode.Builder()
        .clusterName(clusterName)
        .zkAddress(ZK_ADDR)
        .instanceName(instanceName)
        .onStateTransition((partition, from, to) -> {
          if ("LEADER".equals(to)) {
            leaderPartitions.add(partition);
          } else {
            leaderPartitions.remove(partition);
          }
        })
        .build();
    node.start();
    nodes.add(node);
    return node;
  }

  private int countExternalViewLeaders(String clusterName) {
    ExternalView ev = admin.getHelixAdmin()
        .getResourceExternalView(clusterName, HelixShardingAdmin.DEFAULT_RESOURCE_NAME);
    if (ev == null) {
      return 0;
    }
    int leaders = 0;
    for (String partition : ev.getPartitionSet()) {
      Map<String, String> stateMap = ev.getStateMap(partition);
      for (String state : stateMap.values()) {
        if ("LEADER".equals(state)) {
          leaders++;
        }
      }
    }
    return leaders;
  }

  @Test
  public void testGracefulShutdownNoPartitionLoss() throws Exception {
    String clusterName = uniqueCluster("graceful");
    createAdmin();
    admin.addCluster(clusterName, NUM_PARTITIONS, 1, ShardingRebalanceStrategy.AUTO);
    startController(clusterName, "controller_0");

    Set<String> node1Leaders = ConcurrentHashMap.newKeySet();
    Set<String> node2Leaders = ConcurrentHashMap.newKeySet();
    startNode(clusterName, "localhost_12918", node1Leaders);
    startNode(clusterName, "localhost_12919", node2Leaders);

    Assert.assertTrue(createVerifier(clusterName).verifyByPolling());
    Assert.assertEquals(node1Leaders.size() + node2Leaders.size(), NUM_PARTITIONS);

    // Gracefully stop node 1
    nodes.get(0).stop();
    nodes.remove(0);

    // Wait for rebalance
    Assert.assertTrue(createVerifier(clusterName).verifyByPolling());

    // All partitions should now be on node 2
    Assert.assertEquals(countExternalViewLeaders(clusterName), NUM_PARTITIONS,
        "All partitions should still have leaders after graceful shutdown");
  }

  @Test
  public void testControllerFailoverDoesNotLoseState() throws Exception {
    String clusterName = uniqueCluster("ctrlFailover");
    createAdmin();
    admin.addCluster(clusterName, NUM_PARTITIONS, 1, ShardingRebalanceStrategy.AUTO);

    // Start 2 controllers (only 1 will be active leader)
    ClusterControllerManager ctrl1 = startController(clusterName, "controller_0");
    ClusterControllerManager ctrl2 = startController(clusterName, "controller_1");

    Set<String> node1Leaders = ConcurrentHashMap.newKeySet();
    startNode(clusterName, "localhost_12918", node1Leaders);

    Assert.assertTrue(createVerifier(clusterName).verifyByPolling());
    Assert.assertEquals(node1Leaders.size(), NUM_PARTITIONS);

    // Kill the first controller
    ctrl1.syncStop();
    controllers.remove(ctrl1);

    // The second controller should take over — partitions should remain stable
    Thread.sleep(5000); // Allow controller failover
    Assert.assertTrue(createVerifier(clusterName).verifyByPolling());
    Assert.assertEquals(countExternalViewLeaders(clusterName), NUM_PARTITIONS,
        "All partitions should still have leaders after controller failover");
  }

  @Test
  public void testNodeRejoinAfterStop() throws Exception {
    String clusterName = uniqueCluster("rejoin");
    createAdmin();
    admin.addCluster(clusterName, NUM_PARTITIONS, 1, ShardingRebalanceStrategy.AUTO);
    startController(clusterName, "controller_0");

    Set<String> node1Leaders = ConcurrentHashMap.newKeySet();
    Set<String> node2Leaders = ConcurrentHashMap.newKeySet();
    startNode(clusterName, "localhost_12918", node1Leaders);
    HelixShardingNode node2 = startNode(clusterName, "localhost_12919", node2Leaders);

    Assert.assertTrue(createVerifier(clusterName).verifyByPolling());
    int node2InitialCount = node2Leaders.size();
    Assert.assertTrue(node2InitialCount > 0, "Node 2 should have some partitions initially");

    // Stop node 2
    node2.stop();
    nodes.remove(node2);

    Assert.assertTrue(createVerifier(clusterName).verifyByPolling());

    // Node 1 should have all partitions now
    Assert.assertEquals(node1Leaders.size(), NUM_PARTITIONS);

    // Rejoin with a NEW node at the same logical position
    Set<String> node2RejoinLeaders = ConcurrentHashMap.newKeySet();
    startNode(clusterName, "localhost_12920", node2RejoinLeaders);

    Assert.assertTrue(createVerifier(clusterName).verifyByPolling());

    // Both nodes should share the partitions again
    int total = node1Leaders.size() + node2RejoinLeaders.size();
    Assert.assertEquals(total, NUM_PARTITIONS,
        "Total partition count should remain " + NUM_PARTITIONS + " after rejoin");
    Assert.assertTrue(node2RejoinLeaders.size() > 0,
        "Rejoined node should receive some partitions");
  }

  @Test
  public void testMultipleNodeFailuresStillConverge() throws Exception {
    String clusterName = uniqueCluster("multiFailure");
    createAdmin();
    admin.addCluster(clusterName, NUM_PARTITIONS, 1, ShardingRebalanceStrategy.AUTO);
    startController(clusterName, "controller_0");

    // Start 4 nodes
    Set<String> lastNodeLeaders = ConcurrentHashMap.newKeySet();
    for (int i = 0; i < 3; i++) {
      startNode(clusterName, "localhost_" + (12918 + i), ConcurrentHashMap.newKeySet());
    }
    startNode(clusterName, "localhost_12921", lastNodeLeaders);

    Assert.assertTrue(createVerifier(clusterName).verifyByPolling());

    // Stop 2 nodes simultaneously
    nodes.get(0).stop();
    nodes.get(1).stop();
    // Remove in reverse order to maintain correct indices
    nodes.remove(1);
    nodes.remove(0);

    // System should still converge
    Assert.assertTrue(createVerifier(clusterName).verifyByPolling());
    Assert.assertEquals(countExternalViewLeaders(clusterName), NUM_PARTITIONS,
        "All partitions should have leaders after 2 simultaneous node failures");
  }
}
