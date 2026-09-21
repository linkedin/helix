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
 *   http://www.apache.org/licenses/LICENSE-2.0
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
import org.apache.helix.ConfigAccessor;
import org.apache.helix.HelixDataAccessor;
import org.apache.helix.TestHelper;
import org.apache.helix.common.ZkTestBase;
import org.apache.helix.integration.manager.ClusterControllerManager;
import org.apache.helix.integration.manager.MockParticipantManager;
import org.apache.helix.manager.zk.ZKHelixDataAccessor;
import org.apache.helix.model.BuiltInStateModelDefinitions;
import org.apache.helix.model.ClusterConfig;
import org.apache.helix.model.ExternalView;
import org.apache.helix.model.IdealState;
import org.apache.helix.model.ResourceConfig;
import org.apache.helix.tools.ClusterVerifiers.StrictMatchExternalViewVerifier;
import org.apache.helix.tools.ClusterVerifiers.ZkHelixClusterVerifier;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

/**
 * End to end coverage for {@link ClusterConfig#setWagedInstanceTagIsolationEnabled} on a cluster
 * shaped the way a real fleet is shaped, rather than the idealised one tag per node shape.
 * <p>
 * The sibling suite {@link TestWagedRebalanceInstanceTagIsolation} gives every instance exactly
 * one tag, which is its clique. Real deployments never look like that: an instance also carries
 * operational labels such as an availability zone, a hardware generation, or a capacity pool, and
 * those labels are shared by instances belonging to different cliques. No resource is ever pinned
 * to such a label.
 * <p>
 * That difference is not cosmetic. A label shared across cliques is exactly what made isolation
 * silently degrade into the old global behaviour, because the shared label formed an attribution
 * group that touched every clique's nodes and merged them all into a single blast radius. These
 * tests exist so that regression cannot come back unnoticed.
 */
public class TestWagedIsolationSharedLabel extends ZkTestBase {
  private static final int CLIQUE_COUNT = 3;
  private static final int NODES_PER_CLIQUE = 3;
  private static final int START_PORT = 14618;
  private static final int PARTITIONS = 3;
  private static final int REPLICA = 2;
  private static final String CAPACITY_KEY = "DISK";
  private static final int NODE_CAPACITY = 100;
  private static final int HEALTHY_PARTITION_WEIGHT = 5;
  private static final int UNPLACEABLE_PARTITION_WEIGHT = NODE_CAPACITY + 1;
  private static final int BROKEN_CLIQUE = 0;

  // The operational labels that make this cluster realistic. No resource is pinned to either.
  private static final String POOL_LABEL = "prod_pool";

  private final String CLASS_NAME = getShortClassName();
  private final String CLUSTER_NAME = CLUSTER_PREFIX + "_" + CLASS_NAME;

  private ClusterControllerManager _controller;
  private final List<MockParticipantManager> _participants = new ArrayList<>();
  private final Map<Integer, List<String>> _nodesByClique = new HashMap<>();
  private ConfigAccessor _configAccessor;

  private static String cliqueTag(int clique) {
    return "clique_" + clique;
  }

  private static String zoneLabel(int index) {
    return "zone_" + (index % 2);
  }

  private static String resourceName(int clique) {
    return "DB_clique_" + clique;
  }

  @BeforeClass
  public void beforeClass() throws Exception {
    System.out.println("START " + CLASS_NAME + " at " + new Date(System.currentTimeMillis()));
    _gSetupTool.addCluster(CLUSTER_NAME, true);
    _configAccessor = new ConfigAccessor(_gZkClient);

    int port = START_PORT;
    int nodeIndex = 0;
    for (int clique = 0; clique < CLIQUE_COUNT; clique++) {
      List<String> cliqueNodes = new ArrayList<>();
      for (int i = 0; i < NODES_PER_CLIQUE; i++) {
        String node = PARTICIPANT_PREFIX + "_" + port++;
        _gSetupTool.addInstanceToCluster(CLUSTER_NAME, node);
        _gSetupTool.getClusterManagementTool()
            .addInstanceTag(CLUSTER_NAME, node, cliqueTag(clique));
        // The realistic part: labels that span cliques and that no resource is pinned to.
        _gSetupTool.getClusterManagementTool().addInstanceTag(CLUSTER_NAME, node, POOL_LABEL);
        _gSetupTool.getClusterManagementTool()
            .addInstanceTag(CLUSTER_NAME, node, zoneLabel(nodeIndex++));
        cliqueNodes.add(node);
      }
      _nodesByClique.put(clique, cliqueNodes);
    }

    for (List<String> cliqueNodes : _nodesByClique.values()) {
      for (String node : cliqueNodes) {
        MockParticipantManager participant =
            new MockParticipantManager(ZK_ADDR, CLUSTER_NAME, node);
        participant.syncStart();
        _participants.add(participant);
      }
    }

    _controller = new ClusterControllerManager(ZK_ADDR, CLUSTER_NAME, CONTROLLER_PREFIX + "_0");
    _controller.syncStart();
    enablePersistBestPossibleAssignment(_gZkClient, CLUSTER_NAME, true);

    HelixDataAccessor dataAccessor = new ZKHelixDataAccessor(CLUSTER_NAME, _baseAccessor);
    ClusterConfig clusterConfig =
        dataAccessor.getProperty(dataAccessor.keyBuilder().clusterConfig());
    clusterConfig.setInstanceCapacityKeys(Collections.singletonList(CAPACITY_KEY));
    clusterConfig
        .setDefaultInstanceCapacityMap(Collections.singletonMap(CAPACITY_KEY, NODE_CAPACITY));
    clusterConfig.setDefaultPartitionWeightMap(
        Collections.singletonMap(CAPACITY_KEY, HEALTHY_PARTITION_WEIGHT));
    dataAccessor.setProperty(dataAccessor.keyBuilder().clusterConfig(), clusterConfig);

    for (int clique = 0; clique < CLIQUE_COUNT; clique++) {
      createTaggedResource(clique);
    }
    // Turn isolation on and let the cluster settle in that mode before anything is broken, so a
    // test never races the config write against the change that it is meant to isolate.
    setIsolationEnabled(true);
    Assert.assertTrue(verifier().verifyByPolling(),
        "The cluster must converge before any isolation test runs");
    Thread.sleep(3000);
  }

  @AfterClass
  public void afterClass() {
    if (_controller != null && _controller.isConnected()) {
      _controller.syncStop();
    }
    _participants.stream().filter(MockParticipantManager::isConnected)
        .forEach(MockParticipantManager::syncStop);
    deleteCluster(CLUSTER_NAME);
    System.out.println("END " + CLASS_NAME + " at " + new Date(System.currentTimeMillis()));
  }

  private ZkHelixClusterVerifier verifier() {
    return new StrictMatchExternalViewVerifier.Builder(CLUSTER_NAME).setZkAddr(ZK_ADDR)
        .setDeactivatedNodeAwareness(true).setResources(allResources())
        .setWaitTillVerify(TestHelper.DEFAULT_REBALANCE_PROCESSING_WAIT_TIME).build();
  }

  private java.util.Set<String> allResources() {
    java.util.Set<String> resources = new java.util.HashSet<>();
    for (int clique = 0; clique < CLIQUE_COUNT; clique++) {
      resources.add(resourceName(clique));
    }
    return resources;
  }

  private void createTaggedResource(int clique) {
    String db = resourceName(clique);
    createResourceWithWagedRebalance(CLUSTER_NAME, db,
        BuiltInStateModelDefinitions.LeaderStandby.name(), PARTITIONS, REPLICA, REPLICA);
    IdealState idealState =
        _gSetupTool.getClusterManagementTool().getResourceIdealState(CLUSTER_NAME, db);
    idealState.setInstanceGroupTag(cliqueTag(clique));
    _gSetupTool.getClusterManagementTool().setResourceIdealState(CLUSTER_NAME, db, idealState);
    _gSetupTool.rebalanceStorageCluster(CLUSTER_NAME, db, REPLICA);
  }

  private void setIsolationEnabled(boolean enabled) {
    ClusterConfig clusterConfig = _configAccessor.getClusterConfig(CLUSTER_NAME);
    clusterConfig.setWagedInstanceTagIsolationEnabled(enabled);
    _configAccessor.setClusterConfig(CLUSTER_NAME, clusterConfig);
  }

  private void setPartitionWeight(int clique, int weight) {
    ResourceConfig resourceConfig =
        _configAccessor.getResourceConfig(CLUSTER_NAME, resourceName(clique));
    if (resourceConfig == null) {
      resourceConfig = new ResourceConfig(resourceName(clique));
    }
    try {
      resourceConfig.setPartitionCapacityMap(Collections
          .singletonMap(ResourceConfig.DEFAULT_PARTITION_KEY,
              Collections.singletonMap(CAPACITY_KEY, weight)));
    } catch (java.io.IOException ex) {
      throw new IllegalStateException(ex);
    }
    _configAccessor.setResourceConfig(CLUSTER_NAME, resourceName(clique), resourceConfig);
  }

  private Map<String, Map<String, Map<String, String>>> readExternalViews() {
    HelixDataAccessor accessor = new ZKHelixDataAccessor(CLUSTER_NAME, _baseAccessor);
    Map<String, Map<String, Map<String, String>>> views = new TreeMap<>();
    for (int clique = 0; clique < CLIQUE_COUNT; clique++) {
      ExternalView externalView =
          accessor.getProperty(accessor.keyBuilder().externalView(resourceName(clique)));
      if (externalView == null) {
        continue;
      }
      Map<String, Map<String, String>> byPartition = new TreeMap<>();
      externalView.getPartitionSet().forEach(partition -> byPartition
          .put(partition, new TreeMap<>(externalView.getStateMap(partition))));
      views.put(resourceName(clique), byPartition);
    }
    return views;
  }

  private int placedReplicas(int clique) {
    Map<String, Map<String, Map<String, String>>> views = readExternalViews();
    Map<String, Map<String, String>> resourceView = views.get(resourceName(clique));
    if (resourceView == null) {
      return 0;
    }
    return resourceView.values().stream().mapToInt(Map::size).sum();
  }

  /**
   * The headline case. Clique 0 is made unplaceable while every instance also carries a pool label
   * and a zone label shared with the other cliques. The healthy cliques must keep all of their
   * replicas placed.
   * <p>
   * Before the attribution fix this failed: the shared labels merged all three cliques into one
   * blast radius, so the mode degraded to the global behaviour and clique 0's failure emptied the
   * whole cluster.
   */
  @Test
  public void testSharedLabelsDoNotWidenTheBlastRadius() throws Exception {
    setIsolationEnabled(true);
    Assert.assertTrue(
        _configAccessor.getClusterConfig(CLUSTER_NAME).isWagedInstanceTagIsolationEnabled(),
        "The isolation flag must actually be set before the cluster is broken");
    setPartitionWeight(BROKEN_CLIQUE, UNPLACEABLE_PARTITION_WEIGHT);
    _gSetupTool.getClusterManagementTool()
        .rebalance(CLUSTER_NAME, resourceName(BROKEN_CLIQUE), REPLICA);
    Thread.sleep(4000);

    for (int clique = 0; clique < CLIQUE_COUNT; clique++) {
      if (clique == BROKEN_CLIQUE) {
        continue;
      }
      Assert.assertEquals(placedReplicas(clique), PARTITIONS * REPLICA,
          "Clique " + clique + " shares a pool label and a zone label with the broken clique, but "
              + "no resource is pinned to those labels, so they must not drag it into the failure");
    }
  }

  /**
   * A healthy clique must still be able to react to a change of its own while another clique is
   * unplaceable. This is the property that makes the isolation useful rather than merely quiet:
   * the rest of the cluster stays live, not just frozen in a good state.
   */
  @Test(dependsOnMethods = "testSharedLabelsDoNotWidenTheBlastRadius")
  public void testHealthyCliqueStillRebalancesWhileAnotherIsBroken() throws Exception {
    setIsolationEnabled(true);
    int healthyClique = CLIQUE_COUNT - 1;

    // Drop a node from the healthy clique. Its replicas must be replaced on its surviving nodes.
    String victim = _nodesByClique.get(healthyClique).get(0);
    MockParticipantManager participant = _participants.stream()
        .filter(p -> p.getInstanceName().equals(victim)).findFirst().orElseThrow(
            () -> new IllegalStateException("no participant for " + victim));
    participant.syncStop();
    Thread.sleep(5000);

    Map<String, Map<String, Map<String, String>>> views = readExternalViews();
    Map<String, Map<String, String>> healthyView = views.get(resourceName(healthyClique));
    Assert.assertNotNull(healthyView, "The healthy clique must still have an external view");
    boolean stillOnDeadNode = healthyView.values().stream()
        .anyMatch(stateMap -> stateMap.containsKey(victim));
    Assert.assertFalse(stillOnDeadNode,
        "The healthy clique must recompute and move off the stopped node even though another "
            + "clique is unplaceable and every node shares a pool label");
  }

  /**
   * Repairing the broken clique must bring it back with no manual intervention, and must not
   * disturb the cliques that stayed healthy throughout.
   */
  @Test(dependsOnMethods = "testHealthyCliqueStillRebalancesWhileAnotherIsBroken")
  public void testBrokenCliqueRecoversWhenRepaired() throws Exception {
    setIsolationEnabled(true);
    setPartitionWeight(BROKEN_CLIQUE, HEALTHY_PARTITION_WEIGHT);
    _gSetupTool.getClusterManagementTool()
        .rebalance(CLUSTER_NAME, resourceName(BROKEN_CLIQUE), REPLICA);
    Thread.sleep(6000);

    Assert.assertEquals(placedReplicas(BROKEN_CLIQUE), PARTITIONS * REPLICA,
        "The repaired clique must come back on its own once its partitions fit again");
  }

  /**
   * The decisive, log independent check that isolation really is recomputing the healthy cliques
   * rather than merely leaving the cluster parked on its last known good assignment.
   * <p>
   * A brand new node is added to a healthy clique while another clique is unplaceable. A replica
   * can only land on that node if the healthy clique was actually recalculated during a rebalance
   * that also saw the broken clique. If the broken clique had failed the whole calculation, the
   * controller would fall back to the previous assignment, which cannot name a node that did not
   * exist when it was computed, and nothing would ever move onto it.
   */
  @Test(dependsOnMethods = "testBrokenCliqueRecoversWhenRepaired")
  public void testHealthyCliqueUsesABrandNewNodeWhileAnotherCliqueIsBroken() throws Exception {
    setIsolationEnabled(true);
    int healthyClique = 1;

    // Break clique 0 again and let the controller observe it.
    setPartitionWeight(BROKEN_CLIQUE, UNPLACEABLE_PARTITION_WEIGHT);
    _gSetupTool.getClusterManagementTool()
        .rebalance(CLUSTER_NAME, resourceName(BROKEN_CLIQUE), REPLICA);
    Thread.sleep(4000);

    String newNode = PARTICIPANT_PREFIX + "_" + (START_PORT + 900);
    _gSetupTool.addInstanceToCluster(CLUSTER_NAME, newNode);
    _gSetupTool.getClusterManagementTool()
        .addInstanceTag(CLUSTER_NAME, newNode, cliqueTag(healthyClique));
    _gSetupTool.getClusterManagementTool().addInstanceTag(CLUSTER_NAME, newNode, POOL_LABEL);
    _gSetupTool.getClusterManagementTool().addInstanceTag(CLUSTER_NAME, newNode, zoneLabel(0));
    MockParticipantManager participant =
        new MockParticipantManager(ZK_ADDR, CLUSTER_NAME, newNode);
    participant.syncStart();
    _participants.add(participant);

    boolean landed = TestHelper.verify(() -> {
      Map<String, Map<String, Map<String, String>>> views = readExternalViews();
      Map<String, Map<String, String>> healthyView = views.get(resourceName(healthyClique));
      return healthyView != null && healthyView.values().stream()
          .anyMatch(stateMap -> stateMap.containsKey(newNode));
    }, 30000);

    Assert.assertTrue(landed,
        "A replica of the healthy clique must move onto the newly added node while another clique "
            + "is unplaceable. If nothing lands there, the broken clique failed the whole "
            + "calculation and the cluster is parked on its last known good assignment, which is "
            + "precisely the freeze this mode exists to prevent");
  }
}
