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
import java.util.Set;
import java.util.TreeMap;
import java.util.stream.Collectors;

import org.apache.helix.ConfigAccessor;
import org.apache.helix.HelixDataAccessor;
import org.apache.helix.TestHelper;
import org.apache.helix.common.ZkTestBase;
import org.apache.helix.constants.InstanceConstants;
import org.apache.helix.controller.rebalancer.waged.AssignmentMetadataStore;
import org.apache.helix.integration.manager.ClusterControllerManager;
import org.apache.helix.integration.manager.MockParticipantManager;
import org.apache.helix.manager.zk.ZKHelixDataAccessor;
import org.apache.helix.manager.zk.ZkBucketDataAccessor;
import org.apache.helix.model.BuiltInStateModelDefinitions;
import org.apache.helix.model.ClusterConfig;
import org.apache.helix.model.IdealState;
import org.apache.helix.model.InstanceConfig;
import org.apache.helix.model.ResourceAssignment;
import org.apache.helix.model.ResourceConfig;
import org.apache.helix.tools.ClusterVerifiers.StrictMatchExternalViewVerifier;
import org.apache.helix.tools.ClusterVerifiers.ZkHelixClusterVerifier;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

/**
 * Multi-round lifecycle coverage for WAGED instance-tag ("clique") isolation that the existing
 * {@link TestWagedRebalanceInstanceTagIsolation} does not reach: what happens when the instance a
 * broken clique's carried-forward assignment still names is taken out from under it (H3), when a
 * clique is repaired in the very round another one breaks (H8), and whether the healthy cliques
 * come out byte for byte identical across a controller failover that has nothing but the persisted
 * store to work from (H6).
 *
 * The existing suite always perturbs a healthy clique or repairs the broken one. Here the
 * perturbation lands on the broken clique's own nodes, the exact spot where the carry-forward keeps
 * naming an instance that is no longer live or assignable, which is the case the design has to be no
 * worse than the default global mode at.
 */
public class TestWagedRebalanceInstanceTagIsolationLifecycle extends ZkTestBase {
  private static final int CLIQUE_COUNT = 3;
  private static final int NODES_PER_CLIQUE = 3;
  private static final int START_PORT = 14318;
  private static final int PARTITIONS = 3;
  private static final int REPLICA = 2;
  private static final String CAPACITY_KEY = "DISK";
  private static final int NODE_CAPACITY = 100;
  private static final int HEALTHY_PARTITION_WEIGHT = 5;
  // Above a single node's capacity, below the whole cluster's, so the per-clique NO_CANDIDATE_NODE
  // path fires rather than the cluster wide capacity precheck.
  private static final int UNPLACEABLE_PARTITION_WEIGHT = NODE_CAPACITY + 1;

  private final String CLASS_NAME = getShortClassName();
  private final String CLUSTER_NAME = CLUSTER_PREFIX + "_" + CLASS_NAME;

  private ClusterControllerManager _controller;
  private AssignmentMetadataStore _assignmentMetadataStore;
  private final List<MockParticipantManager> _participants = new ArrayList<>();
  private final Map<Integer, List<String>> _nodesByClique = new HashMap<>();
  private ConfigAccessor _configAccessor;
  private final java.util.concurrent.atomic.AtomicInteger _touchCounter =
      new java.util.concurrent.atomic.AtomicInteger();

  private static String cliqueTag(int clique) {
    return "clique_" + clique;
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
    for (int clique = 0; clique < CLIQUE_COUNT; clique++) {
      List<String> cliqueNodes = new ArrayList<>();
      for (int i = 0; i < NODES_PER_CLIQUE; i++) {
        String node = PARTICIPANT_PREFIX + "_" + port++;
        _gSetupTool.addInstanceToCluster(CLUSTER_NAME, node);
        _gSetupTool.getClusterManagementTool().addInstanceTag(CLUSTER_NAME, node, cliqueTag(clique));
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

    _assignmentMetadataStore = new AssignmentMetadataStore(new ZkBucketDataAccessor(ZK_ADDR),
        CLUSTER_NAME) {
      public Map<String, ResourceAssignment> getBaseline() {
        super.reset();
        return new HashMap<>(super.getBaseline());
      }

      public synchronized Map<String, ResourceAssignment> getBestPossibleAssignment() {
        super.reset();
        return new HashMap<>(super.getBestPossibleAssignment());
      }
    };

    HelixDataAccessor dataAccessor = new ZKHelixDataAccessor(CLUSTER_NAME, _baseAccessor);
    ClusterConfig clusterConfig =
        dataAccessor.getProperty(dataAccessor.keyBuilder().clusterConfig());
    clusterConfig.setInstanceCapacityKeys(Collections.singletonList(CAPACITY_KEY));
    clusterConfig
        .setDefaultInstanceCapacityMap(Collections.singletonMap(CAPACITY_KEY, NODE_CAPACITY));
    clusterConfig.setDefaultPartitionWeightMap(
        Collections.singletonMap(CAPACITY_KEY, HEALTHY_PARTITION_WEIGHT));
    clusterConfig.setWagedInstanceTagIsolationEnabled(true);
    dataAccessor.setProperty(dataAccessor.keyBuilder().clusterConfig(), clusterConfig);

    for (int clique = 0; clique < CLIQUE_COUNT; clique++) {
      createTaggedResource(clique);
    }
    Assert.assertTrue(verifier().verifyByPolling(), "The cluster must converge before any test");
  }

  @AfterClass
  public void afterClass() {
    if (_assignmentMetadataStore != null) {
      _assignmentMetadataStore.close();
    }
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

  private Set<String> allResources() {
    Set<String> resources = new java.util.HashSet<>();
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

  private void setPartitionWeight(int clique, int weight) {
    ResourceConfig resourceConfig =
        _configAccessor.getResourceConfig(CLUSTER_NAME, resourceName(clique));
    if (resourceConfig == null) {
      resourceConfig = new ResourceConfig(resourceName(clique));
    }
    try {
      resourceConfig.setPartitionCapacityMap(Collections.singletonMap(
          ResourceConfig.DEFAULT_PARTITION_KEY, Collections.singletonMap(CAPACITY_KEY, weight)));
    } catch (java.io.IOException ex) {
      throw new IllegalStateException(ex);
    }
    _configAccessor.setResourceConfig(CLUSTER_NAME, resourceName(clique), resourceConfig);
  }

  private void triggerGlobalRebalance() {
    ClusterConfig clusterConfig = _configAccessor.getClusterConfig(CLUSTER_NAME);
    clusterConfig.getRecord()
        .setSimpleField("ISOLATION_TEST_TOUCH", String.valueOf(_touchCounter.incrementAndGet()));
    _configAccessor.setClusterConfig(CLUSTER_NAME, clusterConfig);
  }

  private static Map<String, Map<String, Map<String, String>>> normalize(
      Map<String, ResourceAssignment> assignment) {
    Map<String, Map<String, Map<String, String>>> normalized = new TreeMap<>();
    assignment.forEach((resource, resourceAssignment) -> {
      Map<String, Map<String, String>> byPartition = new TreeMap<>();
      resourceAssignment.getMappedPartitions().forEach(partition -> byPartition
          .put(partition.getPartitionName(),
              new TreeMap<>(resourceAssignment.getReplicaMap(partition))));
      normalized.put(resource, byPartition);
    });
    return normalized;
  }

  private Map<String, String> flatBaseline(int clique) {
    Map<String, Map<String, String>> byPartition =
        normalize(_assignmentMetadataStore.getBaseline()).get(resourceName(clique));
    Map<String, String> flat = new TreeMap<>();
    if (byPartition != null) {
      byPartition.forEach((partition, states) -> states
          .forEach((instance, state) -> flat.put(partition + "/" + instance, state)));
    }
    return flat;
  }

  private boolean namesInstanceInBestPossible(int clique, String instance) {
    ResourceAssignment assignment =
        _assignmentMetadataStore.getBestPossibleAssignment().get(resourceName(clique));
    return assignment != null && assignment.getMappedPartitions().stream()
        .anyMatch(partition -> assignment.getReplicaMap(partition).containsKey(instance));
  }

  private boolean baselineNamesInstance(int clique, String instance) {
    return flatBaseline(clique).keySet().stream()
        .anyMatch(partitionSlashInstance -> partitionSlashInstance.endsWith("/" + instance));
  }

  private MockParticipantManager participantFor(String instanceName) {
    return _participants.stream()
        .filter(participant -> participant.getInstanceName().equals(instanceName)).findFirst()
        .orElseThrow(() -> new IllegalStateException("No participant named " + instanceName));
  }

  private void setInstanceOperation(String instance, InstanceConstants.InstanceOperation operation) {
    _gSetupTool.getClusterManagementTool().setInstanceOperation(CLUSTER_NAME, instance, operation);
  }

  /**
   * H3, config path. Break clique 0, wait for it to be carried forward, then take each of the
   * instance operations that remove a node from the assignable set (DISABLE, EVACUATE, UNKNOWN) and
   * apply it to one of clique 0's own nodes, the one its carried-forward assignment still names.
   *
   * The store must keep a complete picture (clique 0 never disappears), the healthy cliques must
   * stay converged, and the carried value must not be corrupted. Then clique 0 is repaired and must
   * converge onto its remaining nodes.
   */
  @Test
  public void testInstanceOperationOnABrokenCliquesOwnCarriedNode() throws Exception {
    setPartitionWeight(0, UNPLACEABLE_PARTITION_WEIGHT);
    triggerGlobalRebalance();

    // Wait until clique 0 is carried forward and still names its own nodes.
    Assert.assertTrue(TestHelper.verify(() -> {
      Map<String, ResourceAssignment> best = _assignmentMetadataStore.getBestPossibleAssignment();
      return best.keySet().containsAll(allResources())
          && namesInstanceInBestPossible(0, _nodesByClique.get(0).get(0));
    }, TestHelper.WAIT_DURATION), "Clique 0 must be carried forward naming its own nodes");

    String victim = _nodesByClique.get(0).get(0);
    Assert.assertTrue(namesInstanceInBestPossible(0, victim),
        "Precondition: the carried assignment must name the node we perturb");
    Map<String, Map<String, Map<String, String>>> healthyBefore = new TreeMap<>();
    healthyBefore.put(resourceName(1), normalize(_assignmentMetadataStore.getBaseline())
        .get(resourceName(1)));
    healthyBefore.put(resourceName(2), normalize(_assignmentMetadataStore.getBaseline())
        .get(resourceName(2)));

    for (InstanceConstants.InstanceOperation operation : new InstanceConstants.InstanceOperation[] {
        InstanceConstants.InstanceOperation.DISABLE,
        InstanceConstants.InstanceOperation.EVACUATE,
        InstanceConstants.InstanceOperation.UNKNOWN}) {
      setInstanceOperation(victim, operation);
      triggerGlobalRebalance();

      // The store must still hold every resource, and the healthy cliques must be untouched by
      // clique 0 losing a node it could not use anyway. Fold the healthy-clique equality into the
      // poll so the store and both healthy cliques are checked at the same settled instant.
      boolean settled = TestHelper.verify(() -> {
        Map<String, ResourceAssignment> best = _assignmentMetadataStore.getBestPossibleAssignment();
        if (!best.keySet().containsAll(allResources())) {
          return false;
        }
        Map<String, Map<String, Map<String, String>>> baseline =
            normalize(_assignmentMetadataStore.getBaseline());
        return healthyBefore.get(resourceName(1)).equals(baseline.get(resourceName(1)))
            && healthyBefore.get(resourceName(2)).equals(baseline.get(resourceName(2)));
      }, TestHelper.WAIT_DURATION);

      Assert.assertEquals(normalize(_assignmentMetadataStore.getBaseline()).get(resourceName(1)),
          healthyBefore.get(resourceName(1)),
          "Healthy clique 1 moved when " + operation + " hit the broken clique's own node");
      Assert.assertEquals(normalize(_assignmentMetadataStore.getBaseline()).get(resourceName(2)),
          healthyBefore.get(resourceName(2)),
          "Healthy clique 2 moved when " + operation + " hit the broken clique's own node");
      Assert.assertTrue(settled,
          operation + " on a broken clique's node must keep every resource and hold the healthy"
              + " cliques still");
    }

    // Repair clique 0. The victim node is still non-assignable (UNKNOWN), so clique 0 must recover
    // onto its two remaining nodes. Re-enable afterwards to restore the topology for later tests.
    setPartitionWeight(0, HEALTHY_PARTITION_WEIGHT);
    Assert.assertTrue(TestHelper.verify(() -> {
      Map<String, ResourceAssignment> best = _assignmentMetadataStore.getBestPossibleAssignment();
      if (!best.keySet().containsAll(allResources())) {
        return false;
      }
      ResourceAssignment clique0 = best.get(resourceName(0));
      // Repaired clique 0 must be fully placed and must not name the non-assignable victim.
      return clique0.getMappedPartitions().size() == PARTITIONS && clique0.getMappedPartitions()
          .stream().noneMatch(partition -> clique0.getReplicaMap(partition).containsKey(victim));
    }, TestHelper.WAIT_DURATION),
        "Repaired clique 0 must converge onto its remaining assignable nodes");

    // H3 store-heal check. The best-possible drops the victim immediately (verified above). The
    // baseline is recomputed in a separate global phase, so it can briefly lag with the stale
    // carried name (baseline ignores DISABLE/OFFLINE, but ASSIGNABLE_INSTANCE_OPERATIONS is only
    // ENABLE and DISABLE, so an UNKNOWN node is not assignable and must eventually leave the
    // baseline too). Poll until the baseline also stops naming the victim, proving the persisted
    // store heals rather than carrying a non-assignable name forever.
    Assert.assertTrue(
        TestHelper.verify(() -> !baselineNamesInstance(0, victim), TestHelper.WAIT_DURATION),
        "The repaired clique's baseline must eventually stop naming the non-assignable victim");

    setInstanceOperation(victim, InstanceConstants.InstanceOperation.ENABLE);
    Assert.assertTrue(verifier().verifyByPolling(),
        "The whole cluster must converge once clique 0 is repaired and its node re-enabled");
  }

  /**
   * H3, participant death path. Break clique 0, then actually stop (kill) one of clique 0's
   * participants, the one its carried-forward assignment names. The carried value then names a dead
   * instance. This must be no worse than the default global mode, which would also retain a
   * last-known-good naming the same dead instance: the store keeps every resource, the healthy
   * cliques keep serving, and the cluster recovers once clique 0 is repaired and the node restored.
   */
  @Test(dependsOnMethods = "testInstanceOperationOnABrokenCliquesOwnCarriedNode")
  public void testKillingABrokenCliquesOwnCarriedNode() throws Exception {
    Assert.assertTrue(verifier().verifyByPolling(), "Start from a converged cluster");
    setPartitionWeight(0, UNPLACEABLE_PARTITION_WEIGHT);
    triggerGlobalRebalance();

    Assert.assertTrue(TestHelper.verify(
        () -> _assignmentMetadataStore.getBestPossibleAssignment().keySet()
            .containsAll(allResources()) && namesInstanceInBestPossible(0,
            _nodesByClique.get(0).get(1)), TestHelper.WAIT_DURATION),
        "Clique 0 must be carried forward before the kill");

    String dead = _nodesByClique.get(0).get(1);
    Assert.assertTrue(namesInstanceInBestPossible(0, dead),
        "Precondition: the carried assignment must name the participant we kill");
    Map<String, Map<String, String>> healthy1Before =
        normalize(_assignmentMetadataStore.getBaseline()).get(resourceName(1));

    MockParticipantManager participant = participantFor(dead);
    participant.syncStop();

    // Killing a node of a clique that cannot be placed anyway must not disturb the healthy cliques,
    // and the store must still carry all three resources. Fold the healthy-clique-unchanged check
    // into the poll so both hold at the same instant, avoiding a baseline read that lags the kill.
    boolean healthyHeld = TestHelper.verify(() -> {
      triggerGlobalRebalance();
      Map<String, ResourceAssignment> best = _assignmentMetadataStore.getBestPossibleAssignment();
      return best.keySet().containsAll(allResources())
          && healthy1Before.equals(normalize(_assignmentMetadataStore.getBaseline())
          .get(resourceName(1)));
    }, TestHelper.WAIT_DURATION);
    Assert.assertEquals(normalize(_assignmentMetadataStore.getBaseline()).get(resourceName(1)),
        healthy1Before, "A healthy clique moved because a broken clique's node was killed");
    Assert.assertTrue(healthyHeld,
        "The store must keep every resource and hold the healthy clique still after the kill");

    // Bring the node back and repair clique 0: the cluster must fully converge again.
    MockParticipantManager restored = new MockParticipantManager(ZK_ADDR, CLUSTER_NAME, dead);
    restored.syncStart();
    _participants.remove(participant);
    _participants.add(restored);
    setPartitionWeight(0, HEALTHY_PARTITION_WEIGHT);
    triggerGlobalRebalance();
    Assert.assertTrue(verifier().verifyByPolling(),
        "The cluster must recover after the killed node returns and clique 0 is repaired");
  }

  /**
   * H8 edge. Clique 0 is broken and carried for several rounds; then in one shot clique 0 is
   * repaired and clique 1 is broken. Clique 0 must come back fully placed, clique 1 must take over
   * the carried-forward slot, and the last healthy clique must keep serving throughout.
   */
  @Test(dependsOnMethods = "testKillingABrokenCliquesOwnCarriedNode")
  public void testRepairOneCliqueWhileBreakingAnotherInTheSameRound() throws Exception {
    Assert.assertTrue(verifier().verifyByPolling());
    setPartitionWeight(0, UNPLACEABLE_PARTITION_WEIGHT);
    triggerGlobalRebalance();
    Assert.assertTrue(TestHelper.verify(
        () -> _assignmentMetadataStore.getBestPossibleAssignment().keySet()
            .containsAll(allResources()), TestHelper.WAIT_DURATION));
    Map<String, Map<String, String>> clique2Before =
        normalize(_assignmentMetadataStore.getBestPossibleAssignment()).get(resourceName(2));

    // Same operator action window: repair 0, break 1.
    setPartitionWeight(0, HEALTHY_PARTITION_WEIGHT);
    setPartitionWeight(1, UNPLACEABLE_PARTITION_WEIGHT);
    triggerGlobalRebalance();

    boolean recovered = TestHelper.verify(() -> {
      Map<String, ResourceAssignment> best = _assignmentMetadataStore.getBestPossibleAssignment();
      if (!best.keySet().containsAll(allResources())) {
        return false;
      }
      ResourceAssignment clique0 = best.get(resourceName(0));
      // Clique 0 fully placed on its own nodes again.
      boolean clique0Healthy = clique0.getMappedPartitions().size() == PARTITIONS
          && clique0.getMappedPartitions().stream().allMatch(partition -> clique0
          .getReplicaMap(partition).keySet().stream()
          .allMatch(instance -> _nodesByClique.get(0).contains(instance)));
      // The isolated bystander clique 2 must hold still while 0 recovers and 1 breaks.
      boolean bystanderStable =
          clique2Before.equals(normalize(best).get(resourceName(2)));
      return clique0Healthy && bystanderStable;
    }, TestHelper.WAIT_DURATION);

    // The last healthy clique never lost its assignment.
    Assert.assertEquals(normalize(_assignmentMetadataStore.getBestPossibleAssignment())
        .get(resourceName(2)), clique2Before, "The bystander clique 2 must not have moved");
    Assert.assertTrue(recovered,
        "Clique 0 must recover in the same round clique 1 breaks, with the bystander held still");

    setPartitionWeight(1, HEALTHY_PARTITION_WEIGHT);
    Assert.assertTrue(verifier().verifyByPolling(), "The cluster must converge after both settle");
  }

  /**
   * H6, controller failover reproduced from the store alone. The isolation object is per-run state,
   * so a crashed controller loses nothing that matters. The successor, starting with an empty change
   * detector and only the persisted store, must reproduce the healthy cliques' assignment byte for
   * byte while still carrying the broken clique forward.
   */
  @Test(dependsOnMethods = "testRepairOneCliqueWhileBreakingAnotherInTheSameRound")
  public void testHealthyCliquesAreByteIdenticalAcrossFailover() throws Exception {
    Assert.assertTrue(verifier().verifyByPolling());
    setPartitionWeight(0, UNPLACEABLE_PARTITION_WEIGHT);
    triggerGlobalRebalance();
    Assert.assertTrue(TestHelper.verify(
        () -> _assignmentMetadataStore.getBestPossibleAssignment().keySet()
            .containsAll(allResources()), TestHelper.WAIT_DURATION));

    Map<String, Map<String, Map<String, String>>> healthyBefore = new TreeMap<>();
    Map<String, Map<String, Map<String, String>>> baseline =
        normalize(_assignmentMetadataStore.getBaseline());
    healthyBefore.put(resourceName(1), baseline.get(resourceName(1)));
    healthyBefore.put(resourceName(2), baseline.get(resourceName(2)));
    Map<String, Map<String, String>> brokenBefore = baseline.get(resourceName(0));

    // Fail the controller over.
    _controller.syncStop();
    ClusterControllerManager newController =
        new ClusterControllerManager(ZK_ADDR, CLUSTER_NAME, CONTROLLER_PREFIX + "_1");
    newController.syncStart();
    _controller = newController;

    // The successor recomputes from the store. Drive it a few rounds and poll until the healthy
    // cliques and the carried broken clique are byte for byte what the previous controller had.
    // The baseline is recomputed in a separate global phase, so a single read right after the
    // best-possible reappears can catch it mid-recompute. Polling until byte-identical removes that
    // read race, so a genuine timeout here is a real determinism finding, not a lucky-read artifact.
    boolean becameIdentical = TestHelper.verify(() -> {
      triggerGlobalRebalance();
      Map<String, ResourceAssignment> best = _assignmentMetadataStore.getBestPossibleAssignment();
      if (!best.keySet().containsAll(allResources())) {
        return false;
      }
      Map<String, Map<String, Map<String, String>>> after =
          normalize(_assignmentMetadataStore.getBaseline());
      return healthyBefore.get(resourceName(1)).equals(after.get(resourceName(1)))
          && healthyBefore.get(resourceName(2)).equals(after.get(resourceName(2)))
          && brokenBefore.equals(after.get(resourceName(0)));
    }, TestHelper.WAIT_DURATION);

    // If it never became byte-identical, surface exactly which blob diverged (a real H6 finding).
    Map<String, Map<String, Map<String, String>>> baselineAfter =
        normalize(_assignmentMetadataStore.getBaseline());
    Assert.assertEquals(baselineAfter.get(resourceName(1)), healthyBefore.get(resourceName(1)),
        "Healthy clique 1 diverged across controller failover");
    Assert.assertEquals(baselineAfter.get(resourceName(2)), healthyBefore.get(resourceName(2)),
        "Healthy clique 2 diverged across controller failover");
    Assert.assertEquals(baselineAfter.get(resourceName(0)), brokenBefore,
        "The carried broken clique diverged across controller failover");
    Assert.assertTrue(becameIdentical,
        "The baseline never became byte-identical across controller failover within the wait");

    setPartitionWeight(0, HEALTHY_PARTITION_WEIGHT);
    Assert.assertTrue(verifier().verifyByPolling(), "The cluster must converge after failover");
  }
}
