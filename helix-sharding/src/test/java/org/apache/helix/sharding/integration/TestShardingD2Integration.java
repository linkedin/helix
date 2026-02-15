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
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import org.apache.helix.common.ZkTestBase;
import org.apache.helix.d2.D2PartitionAnnouncer;
import org.apache.helix.integration.manager.ClusterControllerManager;
import org.apache.helix.sharding.HelixShardingAdmin;
import org.apache.helix.sharding.HelixShardingNode;
import org.apache.helix.sharding.ShardingRebalanceStrategy;
import org.apache.helix.tools.ClusterVerifiers.BestPossibleExternalViewVerifier;
import org.testng.Assert;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.Test;


/**
 * Integration tests for D2 announcement lifecycle with real ZooKeeper.
 */
public class TestShardingD2Integration extends ZkTestBase {

  private static final int NUM_PARTITIONS = 8;

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
    return "TestD2_" + suffix + "_" + System.currentTimeMillis();
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

  /**
   * A recording D2 announcer that captures all calls for verification.
   */
  private static class RecordingD2Announcer implements D2PartitionAnnouncer {
    final List<Map<Integer, Double>> partitionDataHistory =
        java.util.Collections.synchronizedList(new ArrayList<>());
    final List<String> callLog =
        java.util.Collections.synchronizedList(new ArrayList<>());
    volatile Map<Integer, Double> lastPartitionData = java.util.Collections.emptyMap();
    volatile CountDownLatch markUpLatch;

    RecordingD2Announcer() {}

    RecordingD2Announcer(int expectedMarkUps) {
      this.markUpLatch = new CountDownLatch(expectedMarkUps);
    }

    @Override
    public void setPartitionDataMap(Map<Integer, Double> partitionWeights) {
      lastPartitionData = new java.util.HashMap<>(partitionWeights);
      partitionDataHistory.add(lastPartitionData);
      callLog.add("setPartitionDataMap(" + partitionWeights.keySet() + ")");
    }

    @Override
    public void markUp() {
      callLog.add("markUp");
      if (markUpLatch != null) {
        markUpLatch.countDown();
      }
    }

    @Override
    public void markDown() {
      callLog.add("markDown");
    }

    @Override
    public void shutdown() {
      callLog.add("shutdown");
    }
  }

  @Test
  public void testD2AnnouncementsOnLeaderTransition() throws Exception {
    String clusterName = uniqueCluster("leaderAnn");
    createAdmin();
    admin.addCluster(clusterName, NUM_PARTITIONS, 1, ShardingRebalanceStrategy.AUTO);
    startController(clusterName);

    RecordingD2Announcer recorder = new RecordingD2Announcer(2); // start markUp + at least 1 update

    HelixShardingNode node = new HelixShardingNode.Builder()
        .clusterName(clusterName)
        .zkAddress(ZK_ADDR)
        .instanceName("localhost_12918")
        .addD2Announcer(recorder)
        .onStateTransition((p, f, t) -> { })
        .build();
    node.start();
    nodes.add(node);

    Assert.assertTrue(createVerifier(clusterName).verifyByPolling());

    // Wait for D2 announcements
    Assert.assertTrue(recorder.markUpLatch.await(30, TimeUnit.SECONDS),
        "Should have received markUp calls");

    // Verify partition data contains entries
    Assert.assertFalse(recorder.lastPartitionData.isEmpty(),
        "D2 should have announced partition data");

    // Verify partition indices are valid (0 to NUM_PARTITIONS-1)
    for (int idx : recorder.lastPartitionData.keySet()) {
      Assert.assertTrue(idx >= 0 && idx < NUM_PARTITIONS,
          "Partition index " + idx + " should be in valid range");
    }

    // With 1 node, all partitions should be announced
    Assert.assertEquals(recorder.lastPartitionData.size(), NUM_PARTITIONS,
        "Single node should announce all " + NUM_PARTITIONS + " partitions");
  }

  @Test
  public void testAlwaysAnnouncePartitionZero() throws Exception {
    String clusterName = uniqueCluster("partZero");
    createAdmin();
    // Create cluster with only 4 partitions
    admin.addCluster(clusterName, 4, 1, ShardingRebalanceStrategy.AUTO);
    startController(clusterName);

    RecordingD2Announcer recorder = new RecordingD2Announcer(2);

    // Start 2 nodes so each gets ~2 partitions
    HelixShardingNode node1 = new HelixShardingNode.Builder()
        .clusterName(clusterName)
        .zkAddress(ZK_ADDR)
        .instanceName("localhost_12918")
        .addD2Announcer(recorder)
        .alwaysAnnouncePartitionZero(true)
        .onStateTransition((p, f, t) -> { })
        .build();
    node1.start();
    nodes.add(node1);

    // Second node — no D2, just to take some partitions away from node1
    HelixShardingNode node2 = new HelixShardingNode.Builder()
        .clusterName(clusterName)
        .zkAddress(ZK_ADDR)
        .instanceName("localhost_12919")
        .onStateTransition((p, f, t) -> { })
        .build();
    node2.start();
    nodes.add(node2);

    Assert.assertTrue(createVerifier(clusterName).verifyByPolling());
    Thread.sleep(3000); // Allow D2 announcement cycle to complete

    // Partition 0 should always be in the announced set
    Assert.assertTrue(recorder.lastPartitionData.containsKey(0),
        "Partition 0 should always be announced with alwaysAnnouncePartitionZero=true");
  }

  @Test
  public void testMultipleD2Announcers() throws Exception {
    String clusterName = uniqueCluster("multiAnn");
    createAdmin();
    admin.addCluster(clusterName, NUM_PARTITIONS, 1, ShardingRebalanceStrategy.AUTO);
    startController(clusterName);

    RecordingD2Announcer recorder1 = new RecordingD2Announcer(2);
    RecordingD2Announcer recorder2 = new RecordingD2Announcer(2);

    HelixShardingNode node = new HelixShardingNode.Builder()
        .clusterName(clusterName)
        .zkAddress(ZK_ADDR)
        .instanceName("localhost_12918")
        .addD2Announcer(recorder1)
        .addD2Announcer(recorder2)
        .onStateTransition((p, f, t) -> { })
        .build();
    node.start();
    nodes.add(node);

    Assert.assertTrue(createVerifier(clusterName).verifyByPolling());
    Assert.assertTrue(recorder1.markUpLatch.await(30, TimeUnit.SECONDS));
    Assert.assertTrue(recorder2.markUpLatch.await(30, TimeUnit.SECONDS));

    // Both announcers should have the same partition data
    Assert.assertEquals(recorder1.lastPartitionData, recorder2.lastPartitionData,
        "Both D2 announcers should have identical partition data");
  }

  @Test
  public void testD2ShutdownOnNodeStop() throws Exception {
    String clusterName = uniqueCluster("d2shutdown");
    createAdmin();
    admin.addCluster(clusterName, 4, 1, ShardingRebalanceStrategy.AUTO);
    startController(clusterName);

    RecordingD2Announcer recorder = new RecordingD2Announcer(2);

    HelixShardingNode node = new HelixShardingNode.Builder()
        .clusterName(clusterName)
        .zkAddress(ZK_ADDR)
        .instanceName("localhost_12918")
        .addD2Announcer(recorder)
        .onStateTransition((p, f, t) -> { })
        .build();
    node.start();
    nodes.add(node);

    Assert.assertTrue(createVerifier(clusterName).verifyByPolling());
    Thread.sleep(2000); // Let D2 settle

    // Stop the node
    node.stop();
    nodes.remove(node);

    // Verify shutdown sequence: markDown + shutdown should have been called
    Assert.assertTrue(recorder.callLog.contains("markDown"),
        "markDown should be called during node stop");
    Assert.assertTrue(recorder.callLog.contains("shutdown"),
        "shutdown should be called during node stop (cleanup)");
  }

  @Test
  public void testD2AnnouncementsWithNodeFailover() throws Exception {
    String clusterName = uniqueCluster("d2failover");
    createAdmin();
    admin.addCluster(clusterName, NUM_PARTITIONS, 1, ShardingRebalanceStrategy.AUTO);
    startController(clusterName);

    RecordingD2Announcer recorder1 = new RecordingD2Announcer();
    RecordingD2Announcer recorder2 = new RecordingD2Announcer();

    HelixShardingNode node1 = new HelixShardingNode.Builder()
        .clusterName(clusterName)
        .zkAddress(ZK_ADDR)
        .instanceName("localhost_12918")
        .addD2Announcer(recorder1)
        .onStateTransition((p, f, t) -> { })
        .build();
    node1.start();
    nodes.add(node1);

    HelixShardingNode node2 = new HelixShardingNode.Builder()
        .clusterName(clusterName)
        .zkAddress(ZK_ADDR)
        .instanceName("localhost_12919")
        .addD2Announcer(recorder2)
        .onStateTransition((p, f, t) -> { })
        .build();
    node2.start();
    nodes.add(node2);

    Assert.assertTrue(createVerifier(clusterName).verifyByPolling());
    Thread.sleep(2000);

    int node2PartsBefore = recorder2.lastPartitionData.size();

    // Stop node 1 — its partitions should failover to node 2
    node1.stop();
    nodes.remove(node1);

    Assert.assertTrue(createVerifier(clusterName).verifyByPolling());
    Thread.sleep(3000); // Wait for D2 announcement cycle

    // Node 2 should now have MORE partitions than before
    Assert.assertTrue(recorder2.lastPartitionData.size() > node2PartsBefore,
        "Node 2 should have gained partitions after node 1 failure. Before: "
            + node2PartsBefore + ", After: " + recorder2.lastPartitionData.size());

    // Node 2 should have ALL partitions now
    Assert.assertEquals(recorder2.lastPartitionData.size(), NUM_PARTITIONS,
        "Node 2 should have all partitions after failover");
  }

  @Test
  public void testD2PartitionDataWeightsAreOne() throws Exception {
    String clusterName = uniqueCluster("d2weights");
    createAdmin();
    admin.addCluster(clusterName, 4, 1, ShardingRebalanceStrategy.AUTO);
    startController(clusterName);

    RecordingD2Announcer recorder = new RecordingD2Announcer(2);

    HelixShardingNode node = new HelixShardingNode.Builder()
        .clusterName(clusterName)
        .zkAddress(ZK_ADDR)
        .instanceName("localhost_12918")
        .addD2Announcer(recorder)
        .onStateTransition((p, f, t) -> { })
        .build();
    node.start();
    nodes.add(node);

    Assert.assertTrue(createVerifier(clusterName).verifyByPolling());
    Assert.assertTrue(recorder.markUpLatch.await(30, TimeUnit.SECONDS));

    // All weights should be 1.0
    for (Map.Entry<Integer, Double> entry : recorder.lastPartitionData.entrySet()) {
      Assert.assertEquals(entry.getValue(), 1.0,
          "Partition " + entry.getKey() + " weight should be 1.0");
    }
  }
}
