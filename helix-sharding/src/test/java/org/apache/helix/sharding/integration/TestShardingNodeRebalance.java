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
 * Integration tests verifying rebalance strategy behavior with real ZooKeeper.
 */
public class TestShardingNodeRebalance extends ZkTestBase {

  private static final int NUM_PARTITIONS = 32;

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
    return "TestRebalance_" + suffix + "_" + System.currentTimeMillis();
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

  /**
   * Capture current ExternalView as partition → leader mapping.
   */
  private Map<String, String> captureLeaderAssignment(String clusterName) {
    Map<String, String> assignment = new HashMap<>();
    ExternalView ev = admin.getHelixAdmin()
        .getResourceExternalView(clusterName, HelixShardingAdmin.DEFAULT_RESOURCE_NAME);
    if (ev != null) {
      for (String partition : ev.getPartitionSet()) {
        Map<String, String> stateMap = ev.getStateMap(partition);
        for (Map.Entry<String, String> entry : stateMap.entrySet()) {
          if ("LEADER".equals(entry.getValue())) {
            assignment.put(partition, entry.getKey());
          }
        }
      }
    }
    return assignment;
  }

  @Test
  public void testAutoRebalanceDistributesEvenly() throws Exception {
    String clusterName = uniqueCluster("auto");
    int numNodes = 3;
    createAdmin();
    admin.addCluster(clusterName, NUM_PARTITIONS, 1, ShardingRebalanceStrategy.AUTO);
    startController(clusterName);

    List<Set<String>> nodeLeaderSets = new ArrayList<>();
    for (int i = 0; i < numNodes; i++) {
      Set<String> leaders = ConcurrentHashMap.newKeySet();
      nodeLeaderSets.add(leaders);
      startNode(clusterName, "localhost_" + (12918 + i), leaders);
    }

    Assert.assertTrue(createVerifier(clusterName).verifyByPolling());

    int expectedPerNode = NUM_PARTITIONS / numNodes;
    for (int i = 0; i < numNodes; i++) {
      int count = nodeLeaderSets.get(i).size();
      Assert.assertTrue(count >= expectedPerNode - 1 && count <= expectedPerNode + 2,
          "AUTO: Node " + i + " has " + count + " leaders, expected ~" + expectedPerNode);
    }
  }

  @Test
  public void testStickyRebalancePreservesAssignment() throws Exception {
    String clusterName = uniqueCluster("sticky");
    createAdmin();
    admin.addCluster(clusterName, NUM_PARTITIONS, 1, ShardingRebalanceStrategy.STICKY);
    startController(clusterName);

    // Start 3 nodes
    List<Set<String>> nodeLeaderSets = new ArrayList<>();
    for (int i = 0; i < 3; i++) {
      Set<String> leaders = ConcurrentHashMap.newKeySet();
      nodeLeaderSets.add(leaders);
      startNode(clusterName, "localhost_" + (12918 + i), leaders);
    }

    Assert.assertTrue(createVerifier(clusterName).verifyByPolling());
    Map<String, String> snapshotBefore = captureLeaderAssignment(clusterName);

    // Add a 4th node
    Set<String> node4Leaders = ConcurrentHashMap.newKeySet();
    nodeLeaderSets.add(node4Leaders);
    startNode(clusterName, "localhost_12921", node4Leaders);

    Assert.assertTrue(createVerifier(clusterName).verifyByPolling());
    Map<String, String> snapshotAfter = captureLeaderAssignment(clusterName);

    // Verify: partitions on live nodes should NOT have moved
    int moved = 0;
    for (Map.Entry<String, String> entry : snapshotBefore.entrySet()) {
      String partitionAfter = snapshotAfter.get(entry.getKey());
      if (!entry.getValue().equals(partitionAfter)) {
        moved++;
      }
    }

    // STICKY should keep all existing assignments — moved should be 0
    Assert.assertEquals(moved, 0,
        "STICKY: No existing partitions should move when adding a node, but " + moved + " moved");
  }

  @Test
  public void testCrushRebalanceMinimalMovement() throws Exception {
    String clusterName = uniqueCluster("crush");
    createAdmin();
    admin.addCluster(clusterName, NUM_PARTITIONS, 1, ShardingRebalanceStrategy.CRUSH);
    startController(clusterName);

    // Start 3 nodes
    for (int i = 0; i < 3; i++) {
      startNode(clusterName, "localhost_" + (12918 + i), ConcurrentHashMap.newKeySet());
    }

    Assert.assertTrue(createVerifier(clusterName).verifyByPolling());
    Map<String, String> snapshotBefore = captureLeaderAssignment(clusterName);
    Assert.assertEquals(snapshotBefore.size(), NUM_PARTITIONS);

    // Add a 4th node
    startNode(clusterName, "localhost_12921", ConcurrentHashMap.newKeySet());
    Assert.assertTrue(createVerifier(clusterName).verifyByPolling());
    Map<String, String> snapshotAfter = captureLeaderAssignment(clusterName);

    // Count movements
    int moved = 0;
    for (Map.Entry<String, String> entry : snapshotBefore.entrySet()) {
      if (!entry.getValue().equals(snapshotAfter.get(entry.getKey()))) {
        moved++;
      }
    }

    // CRUSH should move approximately 1/4 of partitions (ideal = N/4 ≈ 8)
    Assert.assertTrue(moved <= NUM_PARTITIONS / 2,
        "CRUSH: Too many partitions moved (" + moved + "), expected <= " + (NUM_PARTITIONS / 2));
    Assert.assertTrue(moved > 0,
        "CRUSH: At least some partitions should move to the new node");
  }

  @Test
  public void testCrushEdRebalanceEvenDistribution() throws Exception {
    String clusterName = uniqueCluster("crushed");
    int numNodes = 4;
    createAdmin();
    admin.addCluster(clusterName, NUM_PARTITIONS, 1, ShardingRebalanceStrategy.CRUSH_ED);
    startController(clusterName);

    List<Set<String>> nodeLeaderSets = new ArrayList<>();
    for (int i = 0; i < numNodes; i++) {
      Set<String> leaders = ConcurrentHashMap.newKeySet();
      nodeLeaderSets.add(leaders);
      startNode(clusterName, "localhost_" + (12918 + i), leaders);
    }

    Assert.assertTrue(createVerifier(clusterName).verifyByPolling());

    // CRUSH_ED should produce very even distribution
    int expectedPerNode = NUM_PARTITIONS / numNodes; // 8
    for (int i = 0; i < numNodes; i++) {
      int count = nodeLeaderSets.get(i).size();
      Assert.assertTrue(count >= expectedPerNode - 1 && count <= expectedPerNode + 1,
          "CRUSH_ED: Node " + i + " has " + count + " leaders, expected ~" + expectedPerNode);
    }
  }

  @Test
  public void testRebalanceAfterNodeAddition() throws Exception {
    String clusterName = uniqueCluster("addNode");
    createAdmin();
    admin.addCluster(clusterName, NUM_PARTITIONS, 1, ShardingRebalanceStrategy.AUTO);
    startController(clusterName);

    // Start with 2 nodes
    Set<String> node1Leaders = ConcurrentHashMap.newKeySet();
    Set<String> node2Leaders = ConcurrentHashMap.newKeySet();
    startNode(clusterName, "localhost_12918", node1Leaders);
    startNode(clusterName, "localhost_12919", node2Leaders);
    Assert.assertTrue(createVerifier(clusterName).verifyByPolling());

    // Each should have ~16 partitions
    Assert.assertEquals(node1Leaders.size() + node2Leaders.size(), NUM_PARTITIONS);

    // Add 3rd node
    Set<String> node3Leaders = ConcurrentHashMap.newKeySet();
    startNode(clusterName, "localhost_12920", node3Leaders);
    Assert.assertTrue(createVerifier(clusterName).verifyByPolling());

    // Now all 3 should have partitions, total still NUM_PARTITIONS
    int total = node1Leaders.size() + node2Leaders.size() + node3Leaders.size();
    Assert.assertEquals(total, NUM_PARTITIONS);
    Assert.assertTrue(node3Leaders.size() > 0,
        "New node should have received some partitions");
  }

  @Test
  public void testRebalanceAfterNodeRemoval() throws Exception {
    String clusterName = uniqueCluster("removeNode");
    createAdmin();
    admin.addCluster(clusterName, NUM_PARTITIONS, 1, ShardingRebalanceStrategy.AUTO);
    startController(clusterName);

    Set<String> node1Leaders = ConcurrentHashMap.newKeySet();
    Set<String> node2Leaders = ConcurrentHashMap.newKeySet();
    Set<String> node3Leaders = ConcurrentHashMap.newKeySet();
    startNode(clusterName, "localhost_12918", node1Leaders);
    startNode(clusterName, "localhost_12919", node2Leaders);
    startNode(clusterName, "localhost_12920", node3Leaders);
    Assert.assertTrue(createVerifier(clusterName).verifyByPolling());

    // Stop node 3
    nodes.get(2).stop();
    nodes.remove(2);

    Assert.assertTrue(createVerifier(clusterName).verifyByPolling());

    // Remaining 2 nodes should have all partitions
    int total = node1Leaders.size() + node2Leaders.size();
    Assert.assertEquals(total, NUM_PARTITIONS,
        "All partitions should be on remaining nodes after removal");
  }
}
