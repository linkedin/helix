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
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

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
 * Integration tests for the basic HelixShardingAdmin + HelixShardingNode lifecycle
 * using a real in-memory ZooKeeper.
 */
public class TestShardingAdminNodeLifecycle extends ZkTestBase {

  private static final int NUM_PARTITIONS = 16;
  private static final int REPLICA_COUNT = 2;
  private static final long VERIFY_TIMEOUT = 30000L;

  private final List<HelixShardingNode> nodes = new ArrayList<>();
  private final List<ClusterControllerManager> controllers = new ArrayList<>();
  private HelixShardingAdmin admin;

  @AfterMethod
  public void teardown() {
    for (HelixShardingNode node : nodes) {
      try {
        node.stop();
      } catch (Exception e) {
        // ignore
      }
    }
    nodes.clear();
    for (ClusterControllerManager ctrl : controllers) {
      try {
        ctrl.syncStop();
      } catch (Exception e) {
        // ignore
      }
    }
    controllers.clear();
    if (admin != null) {
      admin.close();
      admin = null;
    }
  }

  private String uniqueCluster(String suffix) {
    return "TestLifecycle_" + suffix + "_" + System.currentTimeMillis();
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
  public void testCreateClusterAndVerifyZkStructure() {
    String clusterName = uniqueCluster("create");
    HelixShardingAdmin admin = createAdmin();

    admin.addCluster(clusterName, NUM_PARTITIONS, REPLICA_COUNT, ShardingRebalanceStrategy.AUTO);

    // Verify cluster was created — check IdealState exists
    IdealState idealState = admin.getResourceIdealState(clusterName);
    Assert.assertNotNull(idealState, "IdealState should exist after addCluster");
    Assert.assertEquals(idealState.getNumPartitions(), NUM_PARTITIONS);
    Assert.assertEquals(Integer.parseInt(idealState.getReplicas()), REPLICA_COUNT);
    Assert.assertEquals(idealState.getRebalanceMode(), IdealState.RebalanceMode.FULL_AUTO);

    // Verify state model definition
    List<String> stateModelDefs =
        admin.getHelixAdmin().getStateModelDefs(clusterName);
    Assert.assertTrue(stateModelDefs.contains("LeaderStandby"),
        "LeaderStandby state model should be defined");

    // Cleanup
    admin.dropCluster(clusterName);
  }

  @Test
  public void testSingleNodeJoinAndReceivePartitions() throws Exception {
    String clusterName = uniqueCluster("singleNode");
    HelixShardingAdmin admin = createAdmin();
    admin.addCluster(clusterName, NUM_PARTITIONS, 1, ShardingRebalanceStrategy.AUTO);

    startController(clusterName);

    Set<String> leaderPartitions = ConcurrentHashMap.newKeySet();
    HelixShardingNode node = new HelixShardingNode.Builder()
        .clusterName(clusterName)
        .zkAddress(ZK_ADDR)
        .instanceName("localhost_12918")
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

    Assert.assertTrue(createVerifier(clusterName).verifyByPolling(),
        "Cluster should reach stable state");

    // With 1 node and replica=1, all partitions should be LEADER on this node
    Assert.assertEquals(leaderPartitions.size(), NUM_PARTITIONS,
        "Single node should own all " + NUM_PARTITIONS + " partitions");
  }

  @Test
  public void testMultipleNodesJoinAndPartitionsDistributed() throws Exception {
    String clusterName = uniqueCluster("multiNode");
    int numNodes = 3;
    HelixShardingAdmin admin = createAdmin();
    admin.addCluster(clusterName, NUM_PARTITIONS, 1, ShardingRebalanceStrategy.AUTO);

    startController(clusterName);

    List<Set<String>> nodeLeaderSets = new ArrayList<>();
    for (int i = 0; i < numNodes; i++) {
      Set<String> leaderPartitions = ConcurrentHashMap.newKeySet();
      nodeLeaderSets.add(leaderPartitions);
      HelixShardingNode node = new HelixShardingNode.Builder()
          .clusterName(clusterName)
          .zkAddress(ZK_ADDR)
          .instanceName("localhost_" + (12918 + i))
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
    }

    Assert.assertTrue(createVerifier(clusterName).verifyByPolling(),
        "Cluster should reach stable state");

    // Verify all partitions assigned
    int totalLeaders = nodeLeaderSets.stream().mapToInt(Set::size).sum();
    Assert.assertEquals(totalLeaders, NUM_PARTITIONS,
        "Total leader count should equal partition count");

    // Verify roughly even distribution (±2 tolerance)
    int expectedPerNode = NUM_PARTITIONS / numNodes;
    for (int i = 0; i < numNodes; i++) {
      int count = nodeLeaderSets.get(i).size();
      Assert.assertTrue(count >= expectedPerNode - 2 && count <= expectedPerNode + 2,
          "Node " + i + " has " + count + " partitions, expected ~" + expectedPerNode);
    }
  }

  @Test
  public void testNodeShutdownAndPartitionsReassigned() throws Exception {
    String clusterName = uniqueCluster("shutdown");
    int numNodes = 3;
    HelixShardingAdmin admin = createAdmin();
    admin.addCluster(clusterName, NUM_PARTITIONS, 1, ShardingRebalanceStrategy.AUTO);

    startController(clusterName);

    List<Set<String>> nodeLeaderSets = new ArrayList<>();
    for (int i = 0; i < numNodes; i++) {
      Set<String> leaderPartitions = ConcurrentHashMap.newKeySet();
      nodeLeaderSets.add(leaderPartitions);
      HelixShardingNode node = new HelixShardingNode.Builder()
          .clusterName(clusterName)
          .zkAddress(ZK_ADDR)
          .instanceName("localhost_" + (12918 + i))
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
    }

    Assert.assertTrue(createVerifier(clusterName).verifyByPolling(),
        "Cluster should reach stable state");

    // Stop node 2
    nodes.get(2).stop();
    nodes.remove(2);

    // Wait for rebalance
    Assert.assertTrue(createVerifier(clusterName).verifyByPolling(),
        "Cluster should re-stabilize after node removal");

    // Remaining 2 nodes should have all partitions
    int totalLeaders = nodeLeaderSets.get(0).size() + nodeLeaderSets.get(1).size();
    Assert.assertEquals(totalLeaders, NUM_PARTITIONS,
        "All partitions should be reassigned to remaining nodes");
  }

  @Test
  public void testStateTransitionListenerReceivesCallbacks() throws Exception {
    String clusterName = uniqueCluster("listener");
    HelixShardingAdmin admin = createAdmin();
    admin.addCluster(clusterName, 4, 1, ShardingRebalanceStrategy.AUTO);

    startController(clusterName);

    List<String> transitions = new ArrayList<>();
    CountDownLatch latch = new CountDownLatch(4); // expect 4 LEADER transitions

    HelixShardingNode node = new HelixShardingNode.Builder()
        .clusterName(clusterName)
        .zkAddress(ZK_ADDR)
        .instanceName("localhost_12918")
        .onStateTransition((partition, from, to) -> {
          synchronized (transitions) {
            transitions.add(partition + ":" + from + "->" + to);
          }
          if ("LEADER".equals(to)) {
            latch.countDown();
          }
        })
        .build();
    node.start();
    nodes.add(node);

    Assert.assertTrue(latch.await(VERIFY_TIMEOUT, TimeUnit.MILLISECONDS),
        "Should receive LEADER transitions for all 4 partitions");

    // Verify we received STANDBY transitions before LEADER transitions
    synchronized (transitions) {
      Assert.assertFalse(transitions.isEmpty(), "Should have received transitions");
      // Every partition should have gone through OFFLINE->STANDBY and STANDBY->LEADER
      long standbyCount = transitions.stream()
          .filter(t -> t.contains("->STANDBY")).count();
      long leaderCount = transitions.stream()
          .filter(t -> t.contains("->LEADER")).count();
      Assert.assertTrue(standbyCount >= 4,
          "Should have at least 4 STANDBY transitions, got " + standbyCount);
      Assert.assertEquals(leaderCount, 4,
          "Should have exactly 4 LEADER transitions");
    }
  }

  @Test
  public void testClusterDropRemovesZkState() throws Exception {
    String clusterName = uniqueCluster("drop");
    HelixShardingAdmin admin = createAdmin();
    admin.addCluster(clusterName, 4, 1, ShardingRebalanceStrategy.AUTO);

    // Verify cluster exists
    List<String> clusters = admin.getHelixAdmin().getClusters();
    Assert.assertTrue(clusters.contains(clusterName), "Cluster should exist after creation");

    // Drop it
    admin.dropCluster(clusterName);

    // Verify cluster is gone
    clusters = admin.getHelixAdmin().getClusters();
    Assert.assertFalse(clusters.contains(clusterName), "Cluster should be removed after drop");
  }

  @Test
  public void testNodeIsConnectedAfterStart() throws Exception {
    String clusterName = uniqueCluster("connected");
    HelixShardingAdmin admin = createAdmin();
    admin.addCluster(clusterName, 4, 1, ShardingRebalanceStrategy.AUTO);

    startController(clusterName);

    HelixShardingNode node = new HelixShardingNode.Builder()
        .clusterName(clusterName)
        .zkAddress(ZK_ADDR)
        .instanceName("localhost_12918")
        .onStateTransition((p, f, t) -> { })
        .build();

    Assert.assertFalse(node.isStarted(), "Node should not be started before start()");
    Assert.assertFalse(node.isConnected(), "Node should not be connected before start()");

    node.start();
    nodes.add(node);

    Assert.assertTrue(node.isStarted(), "Node should be started after start()");
    Assert.assertTrue(node.isConnected(), "Node should be connected after start()");

    node.stop();
    nodes.remove(node);

    Assert.assertFalse(node.isStarted(), "Node should not be started after stop()");
  }
}
