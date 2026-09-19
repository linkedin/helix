package org.apache.helix.integration.rebalancer.WagedRebalancer;

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
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import org.apache.helix.HelixDataAccessor;
import org.apache.helix.manager.zk.ZKHelixDataAccessor;
import org.apache.helix.integration.manager.ClusterControllerManager;
import org.apache.helix.integration.manager.MockParticipantManager;
import org.apache.helix.common.ZkTestBase;
import org.apache.helix.model.BuiltInStateModelDefinitions;
import org.apache.helix.model.ClusterConfig;
import org.apache.helix.model.ExternalView;
import org.apache.helix.tools.ClusterVerifiers.BestPossibleExternalViewVerifier;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.google.common.collect.ImmutableMap;

/**
 * Enabling the flag has to be a non-event on a cluster that is not sick. The unit tests show the
 * score is inert when no occupancy is unaccounted for, but that is an argument about one method;
 * this measures the thing an operator actually cares about, which is whether turning the flag on
 * moves any partition.
 */
public class TestWagedHealthyClusterNoChurn extends ZkTestBase {
  private static final String CAPACITY_KEY = "SLOT";
  private static final int INSTANCE_CAPACITY = 20;
  private static final int PARTITION_WEIGHT = 1;
  private static final int NUM_NODES = 6;
  private static final int NUM_PARTITIONS = 20;
  private static final int NUM_REPLICAS = 2;
  private static final String RESOURCE = "healthyResource";
  private static final int START_PORT = 13400;

  private final String CLASS_NAME = getShortClassName();
  private final String CLUSTER_NAME = CLUSTER_PREFIX + "_" + CLASS_NAME;
  private final List<MockParticipantManager> _participants = new ArrayList<>();
  private ClusterControllerManager _controller;
  private HelixDataAccessor _dataAccessor;

  @BeforeClass
  public void beforeClass() throws Exception {
    System.out.println("START " + CLASS_NAME + " at " + new Date(System.currentTimeMillis()));
    _gSetupTool.addCluster(CLUSTER_NAME, true);

    List<String> nodes = new ArrayList<>();
    for (int i = 0; i < NUM_NODES; i++) {
      String node = PARTICIPANT_PREFIX + "_" + (START_PORT + i);
      _gSetupTool.addInstanceToCluster(CLUSTER_NAME, node);
      nodes.add(node);
    }

    _dataAccessor = new ZKHelixDataAccessor(CLUSTER_NAME, _baseAccessor);
    ClusterConfig clusterConfig =
        _dataAccessor.getProperty(_dataAccessor.keyBuilder().clusterConfig());
    clusterConfig.setInstanceCapacityKeys(Collections.singletonList(CAPACITY_KEY));
    clusterConfig.setDefaultInstanceCapacityMap(ImmutableMap.of(CAPACITY_KEY, INSTANCE_CAPACITY));
    clusterConfig.setDefaultPartitionWeightMap(ImmutableMap.of(CAPACITY_KEY, PARTITION_WEIGHT));
    // Start with the behaviour switched off, which is the state a cluster upgrades into.
    clusterConfig.setWagedCountUnallocatedOccupancyEnabled(false);
    _dataAccessor.setProperty(_dataAccessor.keyBuilder().clusterConfig(), clusterConfig);

    for (String node : nodes) {
      MockParticipantManager p = new MockParticipantManager(ZK_ADDR, CLUSTER_NAME, node);
      p.syncStart();
      _participants.add(p);
    }
    _controller = new ClusterControllerManager(ZK_ADDR, CLUSTER_NAME, "controller_healthy");
    _controller.syncStart();

    enablePersistBestPossibleAssignment(_gZkClient, CLUSTER_NAME, true);

    createResourceWithWagedRebalance(CLUSTER_NAME, RESOURCE,
        BuiltInStateModelDefinitions.LeaderStandby.name(), NUM_PARTITIONS, NUM_REPLICAS,
        NUM_REPLICAS);
    _gSetupTool.rebalanceStorageCluster(CLUSTER_NAME, RESOURCE, NUM_REPLICAS);
  }

  private Map<String, Map<String, String>> snapshotPlacement() {
    ExternalView ev = _dataAccessor.getProperty(
        _dataAccessor.keyBuilder().externalView(RESOURCE));
    Map<String, Map<String, String>> snapshot = new TreeMap<>();
    if (ev == null) {
      return snapshot;
    }
    for (String partition : ev.getPartitionSet()) {
      snapshot.put(partition, new TreeMap<>(ev.getStateMap(partition)));
    }
    return snapshot;
  }

  private void setFlag(boolean enabled) {
    ClusterConfig cfg = _dataAccessor.getProperty(_dataAccessor.keyBuilder().clusterConfig());
    cfg.setWagedCountUnallocatedOccupancyEnabled(enabled);
    _dataAccessor.setProperty(_dataAccessor.keyBuilder().clusterConfig(), cfg);
  }

  /**
   * The convergence verifier is satisfied by an empty best-possible matching an empty external
   * view, so it returns immediately before anything has been placed. Wait for the resource to
   * actually exist first.
   */
  private void awaitPlaced() throws Exception {
    long deadline = System.currentTimeMillis() + 60000L;
    int active = 0;
    while (System.currentTimeMillis() < deadline) {
      active = replicaCountsPerInstance().values().stream().mapToInt(Integer::intValue).sum();
      if (active == NUM_PARTITIONS * NUM_REPLICAS) {
        awaitConverged();
        return;
      }
      Thread.sleep(250L);
    }
    Assert.fail("resource never placed: " + active + "/" + (NUM_PARTITIONS * NUM_REPLICAS)
        + " active replicas");
  }

  private void awaitConverged() throws Exception {
    BestPossibleExternalViewVerifier verifier =
        new BestPossibleExternalViewVerifier.Builder(CLUSTER_NAME).setZkAddr(ZK_ADDR)
            .setResources(Collections.singleton(RESOURCE)).build();
    Assert.assertTrue(verifier.verifyByPolling(60000L, 200L), "cluster did not converge");
  }

  @Test
  public void testEnablingFlagOnHealthyClusterMovesNothing() throws Exception {
    awaitPlaced();
    Map<String, Map<String, String>> before = snapshotPlacement();
    Assert.assertEquals(before.size(), NUM_PARTITIONS, "resource did not come up fully");
    System.out.println("[HEALTHY] converged with flag OFF, " + before.size() + " partitions");

    setFlag(true);
    // Force the controller to recompute rather than waiting for a coincidental event.
    _gSetupTool.rebalanceStorageCluster(CLUSTER_NAME, RESOURCE, NUM_REPLICAS);
    Thread.sleep(3000);
    awaitPlaced();

    Map<String, Map<String, String>> after = snapshotPlacement();

    int moved = 0;
    for (Map.Entry<String, Map<String, String>> e : before.entrySet()) {
      Map<String, String> now = after.get(e.getKey());
      if (!e.getValue().equals(now)) {
        moved++;
        System.out.println("[HEALTHY] MOVED " + e.getKey() + ": " + e.getValue() + " -> " + now);
      }
    }
    System.out.println("[HEALTHY] partitions moved after enabling flag: " + moved + "/"
        + before.size());
    Assert.assertEquals(moved, 0,
        "Enabling the flag on a healthy cluster must not move any partition");
  }

  /**
   * The scores only diverge once an instance is genuinely short of room, so a cluster that is
   * merely busy -- replicas in flight, nothing wedged -- must also be left alone.
   */
  @Test(dependsOnMethods = "testEnablingFlagOnHealthyClusterMovesNothing")
  public void testBouncingAnInstanceStillConvergesWithFlagOn() throws Exception {
    Map<String, Integer> countsBefore = replicaCountsPerInstance();

    MockParticipantManager victim = _participants.get(NUM_NODES - 1);
    String victimName = victim.getInstanceName();
    victim.syncStop();
    awaitConverged();
    Thread.sleep(1000);
    System.out.println("[HEALTHY] survived losing " + victimName);

    MockParticipantManager restarted =
        new MockParticipantManager(ZK_ADDR, CLUSTER_NAME, victimName);
    restarted.syncStart();
    _participants.set(NUM_NODES - 1, restarted);
    awaitPlaced();

    Map<String, Integer> countsAfter = replicaCountsPerInstance();
    int total = countsAfter.values().stream().mapToInt(Integer::intValue).sum();
    System.out.println("[HEALTHY] replicas per instance before=" + countsBefore + " after="
        + countsAfter);
    Assert.assertEquals(total, NUM_PARTITIONS * NUM_REPLICAS,
        "every replica must be placed after a bounce with the flag on");
    for (Map.Entry<String, Integer> e : countsAfter.entrySet()) {
      Assert.assertTrue(e.getValue() <= INSTANCE_CAPACITY,
          "instance " + e.getKey() + " exceeded capacity with " + e.getValue());
    }
  }

  private Map<String, Integer> replicaCountsPerInstance() {
    ExternalView ev =
        _dataAccessor.getProperty(_dataAccessor.keyBuilder().externalView(RESOURCE));
    Map<String, Integer> counts = new TreeMap<>();
    if (ev == null) {
      return counts;
    }
    for (String partition : ev.getPartitionSet()) {
      for (Map.Entry<String, String> e : ev.getStateMap(partition).entrySet()) {
        if (!"DROPPED".equals(e.getValue()) && !"OFFLINE".equals(e.getValue())) {
          counts.merge(e.getKey(), 1, Integer::sum);
        }
      }
    }
    return counts;
  }

  @AfterClass
  public void afterClass() {
    for (MockParticipantManager p : _participants) {
      if (p != null && p.isConnected()) {
        p.syncStop();
      }
    }
    if (_controller != null && _controller.isConnected()) {
      _controller.syncStop();
    }
    deleteCluster(CLUSTER_NAME);
    System.out.println("END " + CLASS_NAME);
  }
}
