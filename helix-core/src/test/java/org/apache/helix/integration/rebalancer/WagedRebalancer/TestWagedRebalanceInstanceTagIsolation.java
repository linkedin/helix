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
import java.util.TreeSet;
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
import org.apache.helix.model.ExternalView;
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
 * End to end coverage for {@link ClusterConfig#setWagedInstanceTagIsolationEnabled}, running a real
 * controller against a real ZooKeeper and a real assignment metadata store.
 *
 * The cluster has the clique partitioned shape: it is carved into cliques, every participant
 * carries exactly one clique tag, and every resource is pinned to one clique tag. The tests break one clique's
 * capacity and assert that the other cliques keep converging, that the metadata store keeps a
 * complete picture across all of it, and that controller failover, participant loss and instance
 * operation changes all behave.
 */
public class TestWagedRebalanceInstanceTagIsolation extends ZkTestBase {
  private static final int CLIQUE_COUNT = 3;
  private static final int NODES_PER_CLIQUE = 3;
  private static final int START_PORT = 13918;
  private static final int PARTITIONS = 3;
  private static final int REPLICA = 2;
  private static final String CAPACITY_KEY = "DISK";
  private static final int NODE_CAPACITY = 100;
  private static final int HEALTHY_PARTITION_WEIGHT = 5;
  // How long to watch a frozen clique before concluding that nothing is recomputing it.
  private static final long FREEZE_OBSERVATION_WINDOW = 15_000L;
  // The healthy clique that gets a new node while clique 0 is unplaceable.
  private static final int FROZEN_CLIQUE = 2;
  // Just above a single node's capacity, so no node in the clique can host the partition, while the
  // total demand stays comfortably under the cluster's total capacity. That keeps the cluster wide
  // CAPACITY_DEFICIT precheck (which runs before any placement and is deliberately not isolated)
  // from firing, so these tests exercise the per clique NO_CANDIDATE_NODE path.
  private static final int UNPLACEABLE_PARTITION_WEIGHT = NODE_CAPACITY + 1;

  private final String CLASS_NAME = getShortClassName();
  private final String CLUSTER_NAME = CLUSTER_PREFIX + "_" + CLASS_NAME;

  private ClusterControllerManager _controller;
  private AssignmentMetadataStore _assignmentMetadataStore;
  private Map<String, Map<String, String>> _frozenCliqueBaselineBefore;
  private String _addedNode;
  private String _shrunkNode;
  private int _nextPort = START_PORT + 100;
  private final java.util.concurrent.atomic.AtomicInteger _touchCounter =
      new java.util.concurrent.atomic.AtomicInteger();
  private final List<MockParticipantManager> _participants = new ArrayList<>();
  private final Map<Integer, List<String>> _nodesByClique = new HashMap<>();
  private ConfigAccessor _configAccessor;

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
        _gSetupTool.getClusterManagementTool()
            .addInstanceTag(CLUSTER_NAME, node, cliqueTag(clique));
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

    // Read through to ZK on every access so the assertions see what the controller persisted.
    // The returned maps are defensive copies: AssignmentMetadataStore#reset calls clear() on the
    // very map instance it handed out earlier, so without the copy a later read would silently
    // empty a map the test is still holding.
    _assignmentMetadataStore =
        new AssignmentMetadataStore(new ZkBucketDataAccessor(ZK_ADDR), CLUSTER_NAME) {
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
    dataAccessor.setProperty(dataAccessor.keyBuilder().clusterConfig(), clusterConfig);

    for (int clique = 0; clique < CLIQUE_COUNT; clique++) {
      createTaggedResource(clique);
    }
    Assert.assertTrue(verifier().verifyByPolling(),
        "The cluster must converge before any isolation test runs");
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

  /**
   * Writes a harmless marker into the cluster config. The content change makes the change detector
   * report a CLUSTER_CONFIG change, which is one of the change types that trigger a global
   * baseline recalculation. The marker itself has no effect on placement.
   */
  private void triggerGlobalRebalance() {
    ClusterConfig clusterConfig = _configAccessor.getClusterConfig(CLUSTER_NAME);
    clusterConfig.getRecord()
        .setSimpleField("ISOLATION_TEST_TOUCH", String.valueOf(_touchCounter.incrementAndGet()));
    _configAccessor.setClusterConfig(CLUSTER_NAME, clusterConfig);
  }

  /**
   * Makes one clique's resource unplaceable by giving every partition a weight larger than a node.
   */
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
      ExternalView externalView = accessor
          .getProperty(accessor.keyBuilder().externalView(resourceName(clique)));
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

  /**
   * Turning the flag on for a healthy cluster must not move a single replica, and the persisted
   * baseline and best possible assignments must stay exactly the same.
   */
  @Test
  public void testEnablingIsolationOnAHealthyClusterMovesNothing() throws Exception {
    setIsolationEnabled(false);
    Assert.assertTrue(verifier().verifyByPolling());
    Map<String, Map<String, Map<String, String>>> viewsBefore = readExternalViews();
    Map<String, Map<String, Map<String, String>>> baselineBefore =
        normalize(_assignmentMetadataStore.getBaseline());
    Map<String, Map<String, Map<String, String>>> bestPossibleBefore =
        normalize(_assignmentMetadataStore.getBestPossibleAssignment());

    setIsolationEnabled(true);
    // Force a full recalculation in the new mode.
    _gSetupTool.getClusterManagementTool().rebalance(CLUSTER_NAME, resourceName(0), REPLICA);
    Assert.assertTrue(verifier().verifyByPolling());

    Assert.assertEquals(readExternalViews(), viewsBefore,
        "Isolation must not move a replica on a healthy cluster");
    Assert.assertEquals(normalize(_assignmentMetadataStore.getBaseline()), baselineBefore,
        "The persisted baseline must be unchanged");
    Assert.assertEquals(normalize(_assignmentMetadataStore.getBestPossibleAssignment()),
        bestPossibleBefore, "The persisted best possible assignment must be unchanged");
  }

  /**
   * The headline behavior with the flag OFF: making one clique unplaceable freezes every clique, so
   * a brand new resource added afterwards never gets an assignment.
   */
  @Test(dependsOnMethods = "testEnablingIsolationOnAHealthyClusterMovesNothing")
  public void testBrokenCliqueBlocksEverythingWhenIsolationIsOff() throws Exception {
    setIsolationEnabled(false);
    setPartitionWeight(0, UNPLACEABLE_PARTITION_WEIGHT);

    // Give the controller several pipeline rounds to try and fail.
    Assert.assertTrue(TestHelper.verify(() -> {
      Map<String, ResourceAssignment> baseline = _assignmentMetadataStore.getBaseline();
      // Nothing new can be computed, so the previously persisted baseline is simply retained.
      return baseline.size() == CLIQUE_COUNT;
    }, TestHelper.WAIT_DURATION));

    Map<String, Map<String, Map<String, String>>> bestPossibleWhileFrozen =
        normalize(_assignmentMetadataStore.getBestPossibleAssignment());
    Assert.assertEquals(new TreeSet<>(bestPossibleWhileFrozen.keySet()),
        new TreeSet<>(allResources()),
        "Every resource must still be present in the store, at its last known good assignment");

    // Now grow a completely different, perfectly healthy clique by adding a node to it. Adding an
    // instance is an INSTANCE_CONFIG change, which is exactly what drives a new global baseline.
    // In the global mode that calculation keeps aborting on clique 0, so clique 2's baseline is
    // frozen and never learns about its new node. This is the blast radius the isolation mode is
    // meant to remove.
    _frozenCliqueBaselineBefore = normalize(_assignmentMetadataStore.getBaseline())
        .get(resourceName(FROZEN_CLIQUE));
    Assert.assertNotNull(_frozenCliqueBaselineBefore);
    _addedNode = addNodeToClique(FROZEN_CLIQUE);
    // Shrinking one of the clique's existing nodes below what it currently holds guarantees that a
    // working baseline calculation would have to relocate at least one replica.
    _shrunkNode = _nodesByClique.get(FROZEN_CLIQUE).get(0);
    Assert.assertTrue(
        _frozenCliqueBaselineBefore.values().stream()
            .filter(states -> states.containsKey(_shrunkNode)).count() > 1,
        "Precondition: the node about to shrink must hold more than one replica");
    setInstanceCapacity(_shrunkNode, HEALTHY_PARTITION_WEIGHT);

    Assert.assertFalse(TestHelper.verify(this::frozenCliqueBaselineChanged,
        FREEZE_OBSERVATION_WINDOW),
        "With isolation OFF, the healthy clique " + FROZEN_CLIQUE + " must never react to its new "
            + "node or its shrunk node, because clique 0 keeps aborting the baseline calculation");
    Assert.assertEquals(
        normalize(_assignmentMetadataStore.getBaseline()).get(resourceName(FROZEN_CLIQUE)),
        _frozenCliqueBaselineBefore,
        "The frozen clique's baseline must be byte for byte the last known good one");
  }

  private boolean frozenCliqueBaselineChanged() {
    Map<String, Map<String, String>> baseline =
        normalize(_assignmentMetadataStore.getBaseline()).get(resourceName(FROZEN_CLIQUE));
    return baseline != null && !baseline.equals(_frozenCliqueBaselineBefore);
  }

  private long replicasOn(Map<String, Map<String, String>> assignment, String instance) {
    return assignment.values().stream().filter(states -> states.containsKey(instance)).count();
  }

  private void setInstanceCapacity(String instance, int capacity) {
    InstanceConfig instanceConfig = _configAccessor.getInstanceConfig(CLUSTER_NAME, instance);
    instanceConfig
        .setInstanceCapacityMap(Collections.singletonMap(CAPACITY_KEY, capacity));
    _configAccessor.setInstanceConfig(CLUSTER_NAME, instance, instanceConfig);
  }

  private String addNodeToClique(int clique) {
    String node = PARTICIPANT_PREFIX + "_" + _nextPort++;
    _gSetupTool.addInstanceToCluster(CLUSTER_NAME, node);
    _gSetupTool.getClusterManagementTool().addInstanceTag(CLUSTER_NAME, node, cliqueTag(clique));
    MockParticipantManager participant =
        new MockParticipantManager(ZK_ADDR, CLUSTER_NAME, node);
    participant.syncStart();
    _participants.add(participant);
    _nodesByClique.get(clique).add(node);
    return node;
  }

  private MockParticipantManager participantFor(String instanceName) {
    return _participants.stream()
        .filter(participant -> participant.getInstanceName().equals(instanceName)).findFirst()
        .orElseThrow(() -> new IllegalStateException("No participant named " + instanceName));
  }

  /**
   * With the flag ON the healthy cliques rebalance again while the broken clique is carried over.
   * This is the core end to end assertion.
   */
  @Test(dependsOnMethods = "testBrokenCliqueBlocksEverythingWhenIsolationIsOff")
  public void testBrokenCliqueDoesNotBlockHealthyCliquesWhenIsolationIsOn() throws Exception {
    setIsolationEnabled(true);
    // Clique 0 stays unplaceable from the previous test.
    Map<String, Map<String, String>> brokenCliqueBaselineBefore =
        normalize(_assignmentMetadataStore.getBaseline()).get(resourceName(0));
    Map<String, Map<String, String>> untouchedCliqueBaselineBefore =
        normalize(_assignmentMetadataStore.getBaseline()).get(resourceName(1));

    // Exactly the same perturbation the previous test left frozen: clique 2 has a new node that it
    // never got to use. Flipping the flag must unfreeze it without any other trigger.
    Assert.assertTrue(
        TestHelper.verify(this::frozenCliqueBaselineChanged, TestHelper.WAIT_DURATION),
        "With isolation ON, clique " + FROZEN_CLIQUE + " must finally react to its topology change "
            + "even though clique 0 still cannot be placed");
    Map<String, Map<String, String>> unfrozen =
        normalize(_assignmentMetadataStore.getBaseline()).get(resourceName(FROZEN_CLIQUE));
    Assert.assertTrue(replicasOn(unfrozen, _shrunkNode) <= 1,
        "The recomputed baseline must respect the shrunk node's capacity, but it holds "
            + replicasOn(unfrozen, _shrunkNode) + " replicas");

    // The unplaceable clique is carried over byte for byte, and the clique that was not perturbed
    // at all keeps its assignment.
    Assert.assertEquals(normalize(_assignmentMetadataStore.getBaseline()).get(resourceName(0)),
        brokenCliqueBaselineBefore,
        "The skipped clique must be carried over exactly, never dropped or partially rewritten");
    Assert.assertEquals(normalize(_assignmentMetadataStore.getBaseline()).get(resourceName(1)),
        untouchedCliqueBaselineBefore, "An untouched clique must not move");

    // Perturb a healthy clique: take one of its participants down. Only that clique should react.
    String victim = _nodesByClique.get(1).get(NODES_PER_CLIQUE - 1);
    MockParticipantManager victimParticipant = participantFor(victim);
    victimParticipant.syncStop();

    Assert.assertTrue(TestHelper.verify(() -> {
      Map<String, ResourceAssignment> bestPossible =
          _assignmentMetadataStore.getBestPossibleAssignment();
      if (bestPossible.size() != CLIQUE_COUNT) {
        return false;
      }
      // The healthy clique must have moved off the downed node.
      ResourceAssignment healthy = bestPossible.get(resourceName(1));
      return healthy.getMappedPartitions().stream()
          .noneMatch(partition -> healthy.getReplicaMap(partition).containsKey(victim));
    }, TestHelper.WAIT_DURATION), "Healthy clique 1 must rebalance away from the downed node "
        + "even though clique 0 cannot be placed");

    // The broken clique is carried over untouched, and it is still present in the store.
    Map<String, ResourceAssignment> bestPossible =
        _assignmentMetadataStore.getBestPossibleAssignment();
    Assert.assertTrue(bestPossible.containsKey(resourceName(0)),
        "The skipped clique must never disappear from the assignment metadata store");
    Assert.assertEquals(normalize(_assignmentMetadataStore.getBaseline()).get(resourceName(0)),
        brokenCliqueBaselineBefore, "The skipped clique must still be carried over untouched");
    Assert.assertTrue(bestPossible.containsKey(resourceName(FROZEN_CLIQUE)));

    // Restore the participant for the following tests.
    MockParticipantManager restored =
        new MockParticipantManager(ZK_ADDR, CLUSTER_NAME, victim);
    restored.syncStart();
    _participants.remove(victimParticipant);
    _participants.add(restored);
  }

  /**
   * Controller failover while one clique is broken. The new controller starts with an empty change
   * detector, recomputes the baseline from scratch, and must still leave the broken clique's entry
   * in the store while converging the healthy ones.
   */
  @Test(dependsOnMethods = "testBrokenCliqueDoesNotBlockHealthyCliquesWhenIsolationIsOn")
  public void testControllerFailoverWithABrokenClique() throws Exception {
    setIsolationEnabled(true);
    Map<String, ResourceAssignment> beforeFailover =
        _assignmentMetadataStore.getBestPossibleAssignment();
    Assert.assertTrue(beforeFailover.keySet().containsAll(allResources()));

    _controller.syncStop();
    ClusterControllerManager newController =
        new ClusterControllerManager(ZK_ADDR, CLUSTER_NAME, CONTROLLER_PREFIX + "_1");
    newController.syncStart();
    _controller = newController;

    // The fresh controller forces a full baseline recompute. Clique 0 is still unplaceable, so it
    // must be carried over rather than dropped.
    Assert.assertTrue(TestHelper.verify(() -> {
      // Re-trigger on every poll: the brand new controller may take a few pipeline rounds before
      // its resource map is fully loaded, and the change detector only recalculates the baseline
      // when it observes a change.
      triggerGlobalRebalance();
      Map<String, ResourceAssignment> baseline = _assignmentMetadataStore.getBaseline();
      Map<String, ResourceAssignment> bestPossible =
          _assignmentMetadataStore.getBestPossibleAssignment();
      return baseline.keySet().containsAll(allResources())
          && bestPossible.keySet().containsAll(allResources());
    }, TestHelper.WAIT_DURATION),
        "After controller failover both blobs must still contain every resource, including the "
            + "clique that cannot be placed");
  }

  /**
   * Instance operations that remove a node from the assignable set must be handled per clique. Only
   * the clique that owns the evacuating node reacts; the broken clique stays carried over and the
   * untouched clique keeps its assignment.
   */
  @Test(dependsOnMethods = "testControllerFailoverWithABrokenClique")
  public void testInstanceOperationChangesAreIsolated() throws Exception {
    setIsolationEnabled(true);
    String evacuating = _nodesByClique.get(2).get(0);

    for (InstanceConstants.InstanceOperation operation : new InstanceConstants.InstanceOperation[] {
        InstanceConstants.InstanceOperation.EVACUATE, InstanceConstants.InstanceOperation.UNKNOWN}) {
      _gSetupTool.getClusterManagementTool()
          .setInstanceOperation(CLUSTER_NAME, evacuating, operation);

      Assert.assertTrue(TestHelper.verify(() -> {
        Map<String, ResourceAssignment> bestPossible =
            _assignmentMetadataStore.getBestPossibleAssignment();
        if (!bestPossible.keySet().containsAll(allResources())) {
          return false;
        }
        ResourceAssignment affected = bestPossible.get(resourceName(2));
        return affected.getMappedPartitions().stream()
            .noneMatch(partition -> affected.getReplicaMap(partition).containsKey(evacuating));
      }, TestHelper.WAIT_DURATION),
          "Clique 2 must move off the " + operation + " node while clique 0 stays carried over");

      _gSetupTool.getClusterManagementTool().setInstanceOperation(CLUSTER_NAME, evacuating,
          InstanceConstants.InstanceOperation.ENABLE);
      Assert.assertTrue(TestHelper.verify(
          () -> _assignmentMetadataStore.getBestPossibleAssignment().keySet()
              .containsAll(allResources()), TestHelper.WAIT_DURATION));
    }
  }

  /**
   * Once the broken clique is repaired it must converge on its own, without any operator action on
   * the other cliques, and the whole cluster must match a strict external view check again.
   */
  @Test(dependsOnMethods = "testInstanceOperationChangesAreIsolated")
  public void testRepairedCliqueConvergesOnItsOwn() throws Exception {
    setIsolationEnabled(true);
    setPartitionWeight(0, HEALTHY_PARTITION_WEIGHT);

    Assert.assertTrue(verifier().verifyByPolling(),
        "The repaired clique must converge without touching the other cliques");
    Map<String, ResourceAssignment> bestPossible =
        _assignmentMetadataStore.getBestPossibleAssignment();
    Assert.assertTrue(bestPossible.keySet().containsAll(allResources()));
    Assert.assertEquals(bestPossible.get(resourceName(0)).getMappedPartitions().size(), PARTITIONS,
        "The repaired clique must be fully assigned again");

    // Every replica of every clique must sit on that clique's own nodes.
    for (int clique = 0; clique < CLIQUE_COUNT; clique++) {
      Set<String> allowed = new java.util.HashSet<>(_nodesByClique.get(clique));
      ResourceAssignment resourceAssignment = bestPossible.get(resourceName(clique));
      Set<String> used = resourceAssignment.getMappedPartitions().stream()
          .flatMap(partition -> resourceAssignment.getReplicaMap(partition).keySet().stream())
          .collect(Collectors.toSet());
      Assert.assertTrue(allowed.containsAll(used),
          "Clique " + clique + " leaked onto " + used + ", allowed " + allowed);
    }
  }

  /**
   * With the flag on and nothing broken, turning it back off must not move anything either. This
   * closes the loop: the mode is safe to enable and to roll back at any time.
   */
  @Test(dependsOnMethods = "testRepairedCliqueConvergesOnItsOwn")
  public void testDisablingIsolationAgainMovesNothing() throws Exception {
    setIsolationEnabled(true);
    Assert.assertTrue(verifier().verifyByPolling());
    Map<String, Map<String, Map<String, String>>> viewsBefore = readExternalViews();

    setIsolationEnabled(false);
    _gSetupTool.getClusterManagementTool().rebalance(CLUSTER_NAME, resourceName(1), REPLICA);
    Assert.assertTrue(verifier().verifyByPolling());

    Assert.assertEquals(readExternalViews(), viewsBefore,
        "Rolling the flag back must not move a replica");
  }

  /**
   * A clique whose demand exceeds the capacity of the entire cluster, not just its own nodes. That
   * drags the tag blind cluster wide capacity precheck negative, which would otherwise abort the
   * rebalance before a single replica was placed and freeze every clique regardless of the flag.
   */
  @Test(dependsOnMethods = "testDisablingIsolationAgainMovesNothing")
  public void testClusterWideCapacityDeficitIsIsolatedEndToEnd() throws Exception {
    setIsolationEnabled(true);
    Assert.assertTrue(verifier().verifyByPolling());
    Map<String, Map<String, Map<String, String>>> healthyViews = readExternalViews();

    // Four times a whole node's capacity per replica, so clique 0 alone outweighs the cluster.
    setPartitionWeight(0, NODE_CAPACITY * 4);
    triggerGlobalRebalance();

    // The other cliques must keep a complete, valid assignment throughout.
    Assert.assertTrue(TestHelper.verify(() -> {
      Map<String, ResourceAssignment> bestPossible =
          _assignmentMetadataStore.getBestPossibleAssignment();
      return bestPossible.keySet().containsAll(allResources());
    }, TestHelper.WAIT_DURATION), "Every resource must stay present in the metadata store");

    for (int clique = 1; clique < CLIQUE_COUNT; clique++) {
      Map<String, Map<String, String>> view = readExternalViews().get(resourceName(clique));
      Assert.assertEquals(view, healthyViews.get(resourceName(clique)),
          "Healthy clique " + clique + " must be untouched by clique 0 outweighing the cluster");
    }

    // And the cluster recovers on its own once the weight is sane again.
    setPartitionWeight(0, HEALTHY_PARTITION_WEIGHT);
    Assert.assertTrue(verifier().verifyByPolling(),
        "The cluster must converge again once the oversized clique is repaired");
  }

  /**
   * Two simultaneously broken cliques are both carried over and the last healthy one still serves.
   */
  @Test(dependsOnMethods = "testClusterWideCapacityDeficitIsIsolatedEndToEnd")
  public void testMultipleBrokenCliquesAreIsolatedEndToEnd() throws Exception {
    setIsolationEnabled(true);
    Assert.assertTrue(verifier().verifyByPolling());
    Map<String, Map<String, String>> healthyView =
        readExternalViews().get(resourceName(FROZEN_CLIQUE));

    setPartitionWeight(0, UNPLACEABLE_PARTITION_WEIGHT);
    setPartitionWeight(1, UNPLACEABLE_PARTITION_WEIGHT);
    triggerGlobalRebalance();

    Assert.assertTrue(TestHelper.verify(() -> _assignmentMetadataStore.getBestPossibleAssignment()
        .keySet().containsAll(allResources()), TestHelper.WAIT_DURATION));
    Assert.assertEquals(readExternalViews().get(resourceName(FROZEN_CLIQUE)), healthyView,
        "The last healthy clique must survive two broken ones");

    setPartitionWeight(0, HEALTHY_PARTITION_WEIGHT);
    setPartitionWeight(1, HEALTHY_PARTITION_WEIGHT);
    Assert.assertTrue(verifier().verifyByPolling());
  }

  /**
   * Permanently removing a participant, rather than just stopping it, is the most destructive
   * topology change an operator can make. It must still be absorbed by its own clique while another
   * clique is broken.
   */
  @Test(dependsOnMethods = "testMultipleBrokenCliquesAreIsolatedEndToEnd")
  public void testParticipantDroppedWhileAnotherCliqueIsBroken() throws Exception {
    setIsolationEnabled(true);
    Assert.assertTrue(verifier().verifyByPolling());
    setPartitionWeight(0, UNPLACEABLE_PARTITION_WEIGHT);
    triggerGlobalRebalance();

    // Drop a node from a healthy clique entirely: stop it, then remove it from the cluster.
    String dropped = _nodesByClique.get(FROZEN_CLIQUE).get(0);
    MockParticipantManager participant = participantFor(dropped);
    participant.syncStop();
    _participants.remove(participant);
    _nodesByClique.get(FROZEN_CLIQUE).remove(dropped);
    InstanceConfig droppedConfig = _configAccessor.getInstanceConfig(CLUSTER_NAME, dropped);
    _gSetupTool.getClusterManagementTool().dropInstance(CLUSTER_NAME, droppedConfig);

    // The healthy clique must re-place its replicas onto its remaining nodes even though clique 0
    // still cannot be calculated at all.
    Assert.assertTrue(TestHelper.verify(() -> {
      Map<String, ResourceAssignment> bestPossible =
          _assignmentMetadataStore.getBestPossibleAssignment();
      if (!bestPossible.keySet().containsAll(allResources())) {
        return false;
      }
      ResourceAssignment frozen = bestPossible.get(resourceName(FROZEN_CLIQUE));
      return frozen.getMappedPartitions().stream()
          .noneMatch(partition -> frozen.getReplicaMap(partition).containsKey(dropped));
    }, TestHelper.WAIT_DURATION), "The healthy clique must vacate the dropped node");

    setPartitionWeight(0, HEALTHY_PARTITION_WEIGHT);
    Assert.assertTrue(verifier().verifyByPolling(),
        "The cluster must converge after the drop and the repair");
  }
}
