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

import java.lang.management.ManagementFactory;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;
import javax.management.MBeanServer;
import javax.management.ObjectName;

import org.apache.helix.ConfigAccessor;
import org.apache.helix.HelixAdmin;
import org.apache.helix.HelixDataAccessor;
import org.apache.helix.PropertyKey;
import org.apache.helix.TestHelper;
import org.apache.helix.common.ZkTestBase;
import org.apache.helix.constants.InstanceConstants;
import org.apache.helix.controller.rebalancer.util.WagedValidationUtil;
import org.apache.helix.controller.rebalancer.waged.AssignmentMetadataStore;
import org.apache.helix.guardrail.GuardrailContext;
import org.apache.helix.guardrail.ValidationResult;
import org.apache.helix.guardrail.Violation;
import org.apache.helix.guardrail.rules.InstanceOperationRebalanceFeasibilityGuardrailRule;
import org.apache.helix.guardrail.rules.InstanceTagRebalanceFeasibilityGuardrailRule;
import org.apache.helix.integration.manager.ClusterControllerManager;
import org.apache.helix.integration.manager.MockParticipantManager;
import org.apache.helix.manager.zk.ZKHelixAdmin;
import org.apache.helix.manager.zk.ZKHelixDataAccessor;
import org.apache.helix.manager.zk.ZkBucketDataAccessor;
import org.apache.helix.model.BuiltInStateModelDefinitions;
import org.apache.helix.model.ClusterConfig;
import org.apache.helix.model.EvacuationInfo;
import org.apache.helix.model.ExternalView;
import org.apache.helix.model.IdealState;
import org.apache.helix.model.InstanceConfig;
import org.apache.helix.model.ResourceAssignment;
import org.apache.helix.model.ResourceConfig;
import org.apache.helix.tools.ClusterVerifiers.BestPossibleExternalViewVerifier;
import org.apache.helix.tools.ClusterVerifiers.StrictMatchExternalViewVerifier;
import org.apache.helix.tools.ClusterVerifiers.ZkHelixClusterVerifier;
import org.apache.helix.util.HelixUtil;
import org.apache.helix.zookeeper.datamodel.ZNRecord;
import org.apache.zookeeper.data.Stat;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

/**
 * Real ZooKeeper coverage for the consumers of WAGED's calculation when
 * {@link ClusterConfig#setWagedInstanceTagIsolationEnabled} is on: the read-only what-if that the
 * partition assignment API and the instance guard rails run through {@link HelixUtil}, the
 * rebalance feasibility guard rails themselves, the dry run of
 * {@link BestPossibleExternalViewVerifier}, the admin readiness checks an operator polls while
 * draining or swapping a node ({@code isEvacuateFinished}, {@code canCompleteSwap},
 * {@code completeSwapIfPossible}), disabled nodes, the failure metrics and the baseline divergence
 * gauge, maintenance mode (manual and automatic), the synchronous global rebalance mode, and
 * non-WAGED resources sharing the pipeline.
 *
 * The cluster is topology aware (zone, host, logical id) and carved into three cliques of three
 * nodes, one WAGED resource per clique. Clique 0 is broken by capping its resource at one partition
 * per instance, which leaves room for three of its eight replicas without adding any demand, so
 * the tests exercise the per clique NO_CANDIDATE_NODE path and never the cluster wide capacity
 * precheck.
 */
public class TestWagedIsolationConsumerParity extends ZkTestBase {
  private static final int CLIQUE_COUNT = 3;
  private static final int NODES_PER_CLIQUE = 3;
  private static final int START_PORT = 15918;
  private static final int PARTITIONS = 4;
  private static final int REPLICA = 2;
  private static final String CAPACITY_KEY = "DISK";
  private static final int NODE_CAPACITY = 100;
  private static final int HEALTHY_PARTITION_WEIGHT = 5;
  // Three replicas of 30 fit a node of 100 and two nodes hold at most six, so the clique's eight
  // replicas need all three of its nodes: taking any one of them away makes it unplaceable.
  private static final int TIGHT_PARTITION_WEIGHT = 30;
  private static final int BREAKING_MAX_PARTITIONS_PER_INSTANCE = 1;
  private static final int BROKEN_CLIQUE = 0;
  private static final String ZONE = "zone";
  private static final String HOST = "host";
  private static final String LOGICAL_ID = "logicalId";
  private static final String MAX_PARTITIONS_FIELD =
      ResourceConfig.ResourceConfigProperty.MAX_PARTITIONS_PER_INSTANCE.name();
  private static final String REBALANCER_MBEAN =
      "Rebalancer:ClusterName=%s, EntityName=WagedRebalancer";
  private static final String CLUSTER_STATUS_MBEAN = "ClusterStatus:cluster=%s";
  private static final String NODE_MAX_PARTITION_LIMIT_FAILURES =
      "HardConstraintNodeMaxPartitionLimitFailureCounter";
  private static final String BASELINE_DIVERGENCE = "BaselineDivergenceGauge";
  // Every signal the stock path raises when WAGED cannot place a resource, on both MBeans.
  private static final String[] REBALANCER_FAILURE_METRICS = {"RebalanceFailureCounter",
      "FailureCategoryCapacityDeficitCounter", "FailureCategoryNoCandidateNodeCounter",
      "FailureCategoryInvalidResourceConfigCounter", "FailureCategoryInvalidClusterConfigCounter",
      "FailureCategoryMetadataStoreIoCounter", "FailureCategoryAlgorithmInternalCounter",
      "FailureCategoryAsyncExecutionCounter", "FailureCategoryUnknownCounter"};
  private static final String[] CLUSTER_STATUS_FAILURE_METRICS = {"RebalanceFailureCounter",
      "RebalanceFailureGauge", "ContinuousResourceRebalanceFailureCount",
      "WagedCustomerActionableFailureCounter", "WagedInternalFailureCounter",
      "WagedCustomerActionableFailureGauge", "WagedInternalFailureGauge",
      "WagedFailureCapacityDeficitCounter", "WagedFailureNoCandidateNodeCounter",
      "WagedFailureInvalidResourceConfigCounter", "WagedFailureInvalidClusterConfigCounter",
      "WagedFailureMetadataStoreIoCounter", "WagedFailureAlgorithmInternalCounter",
      "WagedFailureAsyncExecutionCounter", "WagedFailureUnknownCounter", "WagedFallbackInUseGauge",
      "WagedBaselineComputeFailingGauge", "WagedRebalanceOverwriteFailingGauge"};
  // Long enough for several pipeline runs, so a replica that was going to move would have moved.
  private static final long FROZEN_WINDOW_MS = 3000L;

  private final String CLASS_NAME = getShortClassName();
  private final String CLUSTER_NAME = CLUSTER_PREFIX + "_" + CLASS_NAME;

  private ClusterControllerManager _controller;
  private int _controllerGeneration = 0;
  private AssignmentMetadataStore _assignmentMetadataStore;
  private ConfigAccessor _configAccessor;
  private HelixAdmin _admin;
  private int _nextPort = START_PORT;
  private final Map<String, MockParticipantManager> _participants = new HashMap<>();
  private final Map<Integer, List<String>> _nodesByClique = new HashMap<>();
  private final Set<String> _extraResources = new HashSet<>();

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
    _admin = _gSetupTool.getClusterManagementTool();

    ClusterConfig clusterConfig = _configAccessor.getClusterConfig(CLUSTER_NAME);
    clusterConfig.setTopology(String.format("%s/%s/%s", ZONE, HOST, LOGICAL_ID));
    clusterConfig.setFaultZoneType(ZONE);
    clusterConfig.setTopologyAwareEnabled(true);
    clusterConfig.setInstanceCapacityKeys(Collections.singletonList(CAPACITY_KEY));
    clusterConfig
        .setDefaultInstanceCapacityMap(Collections.singletonMap(CAPACITY_KEY, NODE_CAPACITY));
    clusterConfig.setDefaultPartitionWeightMap(
        Collections.singletonMap(CAPACITY_KEY, HEALTHY_PARTITION_WEIGHT));
    clusterConfig.setWagedInstanceTagIsolationEnabled(true);
    clusterConfig.setInstanceOperationRebalanceGuardrailEnabled(true);
    clusterConfig.setInstanceTagRebalanceGuardrailEnabled(true);
    clusterConfig.setPersistBestPossibleAssignment(true);
    _configAccessor.setClusterConfig(CLUSTER_NAME, clusterConfig);

    for (int clique = 0; clique < CLIQUE_COUNT; clique++) {
      List<String> cliqueNodes = new ArrayList<>();
      for (int i = 0; i < NODES_PER_CLIQUE; i++) {
        String node = PARTICIPANT_PREFIX + "_" + _nextPort++;
        // Every clique spans the same three zones, so a fault zone holds nodes of every clique.
        addNode(node, clique, ZONE + "_" + i, node, null);
        cliqueNodes.add(node);
      }
      _nodesByClique.put(clique, cliqueNodes);
    }

    _controller = newController();

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

    for (int clique = 0; clique < CLIQUE_COUNT; clique++) {
      createCliqueResource(resourceName(clique), clique, HEALTHY_PARTITION_WEIGHT, -1);
    }
    Assert.assertTrue(verifier().verifyByPolling(),
        "The cluster must converge before any consumer parity test runs");
  }

  @AfterClass(alwaysRun = true)
  public void afterClass() {
    if (_assignmentMetadataStore != null) {
      _assignmentMetadataStore.close();
    }
    if (_controller != null && _controller.isConnected()) {
      _controller.syncStop();
    }
    _participants.values().stream().filter(MockParticipantManager::isConnected)
        .forEach(MockParticipantManager::syncStop);
    deleteCluster(CLUSTER_NAME);
    System.out.println("END " + CLASS_NAME + " at " + new Date(System.currentTimeMillis()));
  }

  /**
   * Every test starts from the same converged, healthy cluster with isolation on: no clique broken,
   * every weight back to the healthy value, every node running and enabled, no extra resources, no
   * maintenance, the default asynchronous global rebalance, no offline instance limits and no
   * baseline calculation in flight.
   */
  @BeforeMethod
  public void restoreHealthyCluster() throws Exception {
    if (_controller == null || !_controller.isConnected()) {
      _controller = newController();
    }
    setIsolationEnabled(true);
    if (_admin.isInMaintenanceMode(CLUSTER_NAME)) {
      _admin.manuallyEnableMaintenanceMode(CLUSTER_NAME, false, "restore the healthy cluster",
          null);
    }
    setGlobalRebalanceAsyncMode(true);
    setOfflineInstanceLimits(-1, -1);
    for (String resource : new ArrayList<>(_extraResources)) {
      // Drops the resource config too.
      _admin.dropResource(CLUSTER_NAME, resource);
      _extraResources.remove(resource);
    }
    for (int clique = 0; clique < CLIQUE_COUNT; clique++) {
      setResourceConfig(resourceName(clique), HEALTHY_PARTITION_WEIGHT, -1);
      for (String node : _nodesByClique.get(clique)) {
        if (!_participants.get(node).isConnected()) {
          restartParticipant(node);
        }
        if (_admin.getInstanceConfig(CLUSTER_NAME, node).getInstanceOperation().getOperation()
            != InstanceConstants.InstanceOperation.ENABLE) {
          _admin.setInstanceOperation(CLUSTER_NAME, node,
              InstanceConstants.InstanceOperation.ENABLE);
        }
      }
    }
    awaitBaselineIdle();
    Assert.assertTrue(verifier().verifyByPolling(), "The cluster must converge back to healthy");
    Assert.assertTrue(TestHelper.verify(() -> isolationGauge() == 0, TestHelper.WAIT_DURATION),
        "A healthy cluster must report no skipped resources");
  }

  /**
   * The partition assignment API and both guard rails ask {@link HelixUtil} what WAGED would do.
   * With a broken clique that read-only what-if must return the carried forward view of the broken
   * clique and a fresh placement for the healthy ones, without writing the assignment metadata and
   * without touching any metric. With the flag off in the simulated config the same inputs return
   * nothing for any resource, which is the stock all-or-nothing result.
   */
  @Test
  public void testWhatIfCarriesTheBrokenCliqueWithoutSideEffects() throws Exception {
    Map<String, Set<String>> brokenBefore = placements(
        _assignmentMetadataStore.getBestPossibleAssignment().get(resourceName(BROKEN_CLIQUE)));
    breakClique(BROKEN_CLIQUE);

    // Stop the Helix controller so any write to the assignment metadata, and any registered
    // metric, can only come from the what-if itself.
    _controller.syncStop();
    Map<String, Long> metadataBefore = assignmentMetadataWriteMarkers();
    Set<ObjectName> mbeansBefore = clusterMBeans();

    ClusterConfig isolationOn = _configAccessor.getClusterConfig(CLUSTER_NAME);
    Assert.assertTrue(isolationOn.isWagedInstanceTagIsolationEnabled());
    Map<String, ResourceAssignment> target = targetWhatIf(isolationOn);
    Map<String, ResourceAssignment> immediate = immediateWhatIf(isolationOn);

    for (Map<String, ResourceAssignment> whatIf : new ArrayList<Map<String, ResourceAssignment>>() {
      {
        add(target);
        add(immediate);
      }
    }) {
      Assert.assertEquals(placements(whatIf.get(resourceName(BROKEN_CLIQUE))), brokenBefore,
          "The broken clique must be carried forward exactly as it was last assigned");
      for (int clique = 0; clique < CLIQUE_COUNT; clique++) {
        if (clique != BROKEN_CLIQUE) {
          assertFullyPlacedOnOwnClique(whatIf.get(resourceName(clique)), clique);
        }
      }
    }

    Assert.assertEquals(assignmentMetadataWriteMarkers(), metadataBefore,
        "The read-only what-if must never write the assignment metadata");
    Assert.assertEquals(clusterMBeans(), mbeansBefore,
        "The read-only what-if must not register or unregister any metric");

    ClusterConfig isolationOff = new ClusterConfig(new ZNRecord(isolationOn.getRecord()));
    isolationOff.setWagedInstanceTagIsolationEnabled(false);
    Map<String, ResourceAssignment> stock = targetWhatIf(isolationOff);
    for (int clique = 0; clique < CLIQUE_COUNT; clique++) {
      Assert.assertTrue(placements(stock.get(resourceName(clique))).values().stream()
              .allMatch(Set::isEmpty),
          "With the flag off one broken clique must still leave every resource unassigned, "
              + "but clique " + clique + " got " + placements(stock.get(resourceName(clique))));
    }
    Assert.assertEquals(assignmentMetadataWriteMarkers(), metadataBefore);

    _controller = newController();
  }

  /**
   * With nothing broken, evacuating or untagging a node of a tight clique leaves that clique unable
   * to hold its replicas. The guard rails must say so with the flag on exactly as they do with it
   * off, even though with the flag on the what-if does not fail: it carries the clique forward on
   * its previous assignment, which still names the node the mutation takes away.
   */
  @Test
  public void testGuardRailsStillCatchAMutationThatBreaksAClique() throws Exception {
    int tightClique = 1;
    setResourceConfig(resourceName(tightClique), TIGHT_PARTITION_WEIGHT, -1);
    Assert.assertTrue(verifier().verifyByPolling());
    String victim = _nodesByClique.get(tightClique).get(0);

    List<String> problems = new ArrayList<>();
    for (boolean isolation : new boolean[] {true, false}) {
      setIsolationEnabled(isolation);
      // With the flag off the whole what-if fails, so stock blames every WAGED partition; with it
      // on only the clique that loses the node may be blamed.
      checkBlocked(problems, evacuationVerdict(victim), resourceName(tightClique), isolation,
          "EVACUATE with isolation " + isolation);
      checkBlocked(problems, tagRemovalVerdict(victim, cliqueTag(tightClique)),
          resourceName(tightClique), isolation, "tag removal with isolation " + isolation);
    }
    Assert.assertTrue(problems.isEmpty(), String.join("\n", problems));
  }

  /**
   * With a clique already broken, the guard rails must still judge mutations of the healthy
   * cliques on their own merits: a node the clique can spare is approved, a node it cannot spare is
   * blocked, and only the clique that loses the node is blamed.
   */
  @Test
  public void testGuardRailsJudgeHealthyCliquesWhileAnotherIsBroken() throws Exception {
    breakClique(BROKEN_CLIQUE);
    String spareNode = _nodesByClique.get(2).get(0);
    ValidationResult spare = evacuationVerdict(spareNode);
    Assert.assertTrue(spare.isFeasible(),
        "Clique 2 can spare a node, but the guard rail said " + spare);
    ValidationResult spareTag = tagRemovalVerdict(spareNode, cliqueTag(2));
    Assert.assertTrue(spareTag.isFeasible(),
        "Clique 2 can spare a node's tag, but the guard rail said " + spareTag);

    int tightClique = 1;
    setResourceConfig(resourceName(tightClique), TIGHT_PARTITION_WEIGHT, -1);
    Assert.assertTrue(TestHelper.verify(() -> isolationGauge() == 1, TestHelper.WAIT_DURATION));
    String victim = _nodesByClique.get(tightClique).get(0);
    List<String> problems = new ArrayList<>();
    checkBlocked(problems, evacuationVerdict(victim), resourceName(tightClique), true,
        "EVACUATE of a node clique 1 cannot spare while clique 0 is broken");
    checkBlocked(problems, tagRemovalVerdict(victim, cliqueTag(tightClique)),
        resourceName(tightClique), true,
        "tag removal of a node clique 1 cannot spare while clique 0 is broken");
    Assert.assertTrue(problems.isEmpty(), String.join("\n", problems));
  }

  /**
   * An operator draining a node of a healthy clique polls isEvacuateFinished, and the REST command
   * of that name reads the detailed evacuation status. With another clique broken the healthy
   * clique must still drain and both checks must report it finished, while the broken clique stays
   * carried forward. EVACUATE keeps each replica on the draining node until its replacement is up,
   * so the healthy clique must never serve a partition with fewer replicas than it should while it
   * drains.
   */
  @Test
  public void testEvacuationOfAHealthyCliqueFinishesWhileAnotherIsBroken() throws Exception {
    Map<String, Set<String>> brokenBefore = placements(
        _assignmentMetadataStore.getBestPossibleAssignment().get(resourceName(BROKEN_CLIQUE)));
    breakClique(BROKEN_CLIQUE);
    String evacuating = _nodesByClique.get(2).get(1);
    Assert.assertTrue(readExternalView(resourceName(2)).values().stream()
        .anyMatch(stateMap -> stateMap.containsKey(evacuating)),
        "The node to drain must host replicas");
    _admin.setInstanceOperation(CLUSTER_NAME, evacuating,
        InstanceConstants.InstanceOperation.EVACUATE);

    int fewestServing = Integer.MAX_VALUE;
    boolean drained = false;
    long deadline = System.currentTimeMillis() + TestHelper.WAIT_DURATION;
    while (!drained && System.currentTimeMillis() < deadline) {
      boolean viewStillHostsTheNode = false;
      for (Map.Entry<String, Map<String, String>> partition : readExternalView(resourceName(2))
          .entrySet()) {
        Map<String, String> stateMap = partition.getValue();
        viewStillHostsTheNode |= stateMap.containsKey(evacuating);
        int serving = Collections.frequency(stateMap.values(), "LEADER")
            + Collections.frequency(stateMap.values(), "STANDBY");
        fewestServing = Math.min(fewestServing, serving);
      }
      // isEvacuateFinished reads the node's current states, which the external view trails by a
      // pipeline run.
      drained = !viewStillHostsTheNode && _admin.isEvacuateFinished(CLUSTER_NAME, evacuating);
      if (!drained) {
        Thread.sleep(10);
      }
    }
    Assert.assertTrue(drained,
        "The evacuation of a healthy clique's node must finish while clique 0 is broken");
    Assert.assertTrue(fewestServing >= REPLICA, "A partition of the draining clique was served by "
        + fewestServing + " replicas, fewer than " + REPLICA);
    // The REST isEvacuateFinished command asks for the detailed status instead.
    EvacuationInfo evacuationStatus = ((ZKHelixAdmin) _admin).getEvacuationStatus(CLUSTER_NAME,
        evacuating, Collections.emptySet());
    Assert.assertEquals(evacuationStatus.getState(), EvacuationInfo.EvacuationState.COMPLETED,
        "The detailed evacuation status must agree: " + evacuationStatus);
    Assert.assertTrue(_admin.isReadyForPreparingJoiningCluster(CLUSTER_NAME, evacuating),
        "A drained EVACUATE node must be ready to prepare for joining the cluster again");
    Assert.assertEquals(placements(_assignmentMetadataStore.getBestPossibleAssignment()
        .get(resourceName(BROKEN_CLIQUE))), brokenBefore,
        "The broken clique must stay carried forward while another clique drains");
    Assert.assertEquals(isolationGauge(), 1L);
  }

  /**
   * An operator swapping a node of a healthy clique adds a SWAP_IN node with the same logical id,
   * polls canCompleteSwap and then calls completeSwapIfPossible. With another clique broken the
   * swap must still complete.
   */
  @Test
  public void testSwapInAHealthyCliqueCompletesWhileAnotherIsBroken() throws Exception {
    breakClique(BROKEN_CLIQUE);
    swapAndAssertCompleted(2, () -> {
    });
  }

  /**
   * Same as above, but the broken clique is broken by a new resource that cannot be placed. The
   * clique fails as a unit: its existing resource is carried forward and the new one, which has
   * no previous assignment, is omitted, so it has no best possible state at all. That must not
   * stop a healthy clique's swap from completing, and while the swap is in flight the read-only
   * what-if and the guard rails, which run the same stage, must keep answering.
   */
  @Test
  public void testSwapCompletesWhileAnotherCliqueHoldsANeverPlacedResource() throws Exception {
    String neverPlaced = resourceName(BROKEN_CLIQUE) + "_never_placed";
    createCliqueResource(neverPlaced, BROKEN_CLIQUE, HEALTHY_PARTITION_WEIGHT,
        BREAKING_MAX_PARTITIONS_PER_INSTANCE);
    _extraResources.add(neverPlaced);
    Assert.assertTrue(TestHelper.verify(() -> isolationGauge() == 2, TestHelper.WAIT_DURATION),
        "Both resources of the broken clique must be reported as skipped");
    Map<String, ResourceAssignment> bestPossible =
        _assignmentMetadataStore.getBestPossibleAssignment();
    Assert.assertTrue(bestPossible.containsKey(resourceName(BROKEN_CLIQUE)),
        "The broken clique's existing resource must be carried forward");
    Assert.assertFalse(bestPossible.containsKey(neverPlaced),
        "A skipped resource with no previous assignment must be omitted, not fabricated");

    int swappingClique = 1;
    swapAndAssertCompleted(swappingClique, () -> {
      Map<String, ResourceAssignment> whatIf;
      try {
        whatIf = targetWhatIf(_configAccessor.getClusterConfig(CLUSTER_NAME));
      } catch (Exception e) {
        Assert.fail("The what-if must still answer while a swap is in flight", e);
        return;
      }
      Assert.assertEquals(placements(whatIf.get(resourceName(BROKEN_CLIQUE))),
          placements(bestPossible.get(resourceName(BROKEN_CLIQUE))));
      Assert.assertTrue(placements(whatIf.get(neverPlaced)).values().stream()
          .allMatch(Set::isEmpty), "The never placed resource must stay unassigned");
      assertFullyPlacedOnOwnClique(whatIf.get(resourceName(swappingClique)), swappingClique);
      assertFullyPlacedOnOwnClique(whatIf.get(resourceName(2)), 2);
      ValidationResult spare = evacuationVerdict(_nodesByClique.get(2).get(0));
      Assert.assertTrue(spare.isFeasible(),
          "Clique 2 can spare a node while clique 1 swaps, but the guard rail said " + spare);
    });
  }

  /**
   * Isolation absorbs the broken clique, so none of the failure signals the stock path raises may
   * move, in either global rebalance mode. The per constraint counter that explains why the clique
   * could not be placed must still tick, and the baseline divergence gauge must not move, because
   * the broken clique's baseline and best possible are carried together. With the flag off the
   * same break lights the stock failure signals, and turning the flag back on isolates the clique
   * and clears the failing baseline gauge. Synchronously the stock failure makes the Helix
   * controller fall back to the last known good assignment, and isolation takes it off that
   * fallback again. Flipping the flag either way must recompute the baseline.
   */
  @Test
  public void testFailureMetricsStayQuietWhileIsolationAbsorbsABrokenClique() throws Exception {
    try {
      for (boolean asyncGlobalRebalance : new boolean[] {true, false}) {
        setGlobalRebalanceAsyncMode(asyncGlobalRebalance);
        Assert.assertTrue(verifier().verifyByPolling());
        Map<String, Long> before = failureMetrics();
        long constraintFailures = rebalancerMetric(NODE_MAX_PARTITION_LIMIT_FAILURES);
        double divergence = rebalancerRatio(BASELINE_DIVERGENCE);
        breakClique(BROKEN_CLIQUE);
        Assert.assertTrue(TestHelper.verify(
            () -> rebalancerMetric(NODE_MAX_PARTITION_LIMIT_FAILURES) > constraintFailures,
            TestHelper.WAIT_DURATION),
            "The constraint that broke the clique must still be counted, async "
                + asyncGlobalRebalance);
        Assert.assertEquals(failureMetrics(), before,
            "Isolation absorbed the failure, so no failure metric may move, async "
                + asyncGlobalRebalance);
        Assert.assertFalse(TestHelper.verify(
            () -> rebalancerRatio(BASELINE_DIVERGENCE) != divergence, FROZEN_WINDOW_MS),
            "The broken clique is carried in both blobs, so the divergence gauge must stay at "
                + divergence + ", async " + asyncGlobalRebalance);

        setResourceConfig(resourceName(BROKEN_CLIQUE), HEALTHY_PARTITION_WEIGHT, -1);
        Assert.assertTrue(TestHelper.verify(() -> isolationGauge() == 0, TestHelper.WAIT_DURATION),
            "Healing the clique must clear the gauge, async " + asyncGlobalRebalance);
        Assert.assertTrue(verifier().verifyByPolling());
        Assert.assertEquals(failureMetrics(), before,
            "Healing must not move any failure metric either, async " + asyncGlobalRebalance);
      }

      setGlobalRebalanceAsyncMode(true);
      Map<String, Long> before = failureMetrics();
      long healthyBaselines = rebalancerMetric("GlobalBaselineCalcCounter");
      setIsolationEnabled(false);
      Assert.assertTrue(TestHelper.verify(
          () -> rebalancerMetric("GlobalBaselineCalcCounter") > healthyBaselines,
          TestHelper.WAIT_DURATION), "Turning the flag off must recompute the baseline");
      setResourceConfig(resourceName(BROKEN_CLIQUE), HEALTHY_PARTITION_WEIGHT,
          BREAKING_MAX_PARTITIONS_PER_INSTANCE);
      Assert.assertTrue(TestHelper.verify(
          () -> rebalancerMetric("RebalanceFailureCounter")
              > before.get("Rebalancer.RebalanceFailureCounter")
              && rebalancerMetric("FailureCategoryNoCandidateNodeCounter")
              > before.get("Rebalancer.FailureCategoryNoCandidateNodeCounter")
              && clusterStatusMetric("WagedFailureNoCandidateNodeCounter")
              > before.get("ClusterStatus.WagedFailureNoCandidateNodeCounter")
              && clusterStatusMetric("WagedCustomerActionableFailureCounter")
              > before.get("ClusterStatus.WagedCustomerActionableFailureCounter")
              && clusterStatusMetric("WagedBaselineComputeFailingGauge") == 1,
          TestHelper.WAIT_DURATION),
          "With the flag off the same break must fail the baseline and say so: "
              + failureMetrics());
      Assert.assertEquals(isolationGauge(), 0L, "With the flag off nothing is isolated");

      long failingBaselines = rebalancerMetric("GlobalBaselineCalcCounter");
      setIsolationEnabled(true);
      Assert.assertTrue(TestHelper.verify(
          () -> rebalancerMetric("GlobalBaselineCalcCounter") > failingBaselines
              && isolationGauge() == 1
              && clusterStatusMetric("WagedBaselineComputeFailingGauge") == 0,
          TestHelper.WAIT_DURATION),
          "Turning the flag back on must recompute the baseline, isolate the clique and clear the "
              + "failing baseline gauge: " + failureMetrics());
      Assert.assertTrue(_assignmentMetadataStore.getBaseline().containsKey(
          resourceName(BROKEN_CLIQUE)), "The broken clique must be carried in the baseline");

      // Synchronously the stock failure reaches the Helix controller, which falls back to the last
      // known good assignment. Isolation must take the cluster off that fallback again.
      setGlobalRebalanceAsyncMode(false);
      setIsolationEnabled(false);
      Assert.assertTrue(TestHelper.verify(
          () -> clusterStatusMetric("WagedFallbackInUseGauge") == 1, TestHelper.WAIT_DURATION),
          "With the flag off a synchronous failure must fall back: " + failureMetrics());
      setIsolationEnabled(true);
      Assert.assertTrue(TestHelper.verify(
          () -> clusterStatusMetric("WagedFallbackInUseGauge") == 0 && isolationGauge() == 1,
          TestHelper.WAIT_DURATION),
          "Isolation must take the cluster off the fallback: " + failureMetrics());
    } finally {
      setGlobalRebalanceAsyncMode(true);
    }
  }

  /**
   * Non-WAGED resources share the pipeline with the WAGED ones. While isolation carries a broken
   * clique, and again with the flag off where WAGED fails as a whole, a SEMI_AUTO resource must
   * still follow its preference lists and a tagged DelayedAutoRebalancer resource must still be
   * served on its own clique.
   */
  @Test
  public void testNonWagedResourcesKeepConvergingWhileACliqueIsBroken() throws Exception {
    String semiAuto = "SemiAuto_on_clique_1";
    List<String> semiAutoNodes = new ArrayList<>(_nodesByClique.get(1).subList(0, REPLICA));
    createDBInSemiAuto(_gSetupTool, CLUSTER_NAME, semiAuto, semiAutoNodes,
        BuiltInStateModelDefinitions.LeaderStandby.name(), 2, REPLICA);
    _extraResources.add(semiAuto);
    String delayedAuto = "DelayedAuto_on_clique_2";
    createResourceWithDelayedRebalance(CLUSTER_NAME, delayedAuto,
        BuiltInStateModelDefinitions.LeaderStandby.name(), 2, REPLICA, 1, -1);
    IdealState delayedAutoIdealState = _admin.getResourceIdealState(CLUSTER_NAME, delayedAuto);
    delayedAutoIdealState.setInstanceGroupTag(cliqueTag(2));
    _admin.setResourceIdealState(CLUSTER_NAME, delayedAuto, delayedAutoIdealState);
    _extraResources.add(delayedAuto);

    Map<String, Set<String>> brokenBefore = placements(
        _assignmentMetadataStore.getBestPossibleAssignment().get(resourceName(BROKEN_CLIQUE)));
    breakClique(BROKEN_CLIQUE);
    List<String> leaders = new ArrayList<>(semiAutoNodes);
    for (boolean isolation : new boolean[] {true, false}) {
      setIsolationEnabled(isolation);
      Collections.reverse(leaders);
      IdealState semiAutoIdealState = _admin.getResourceIdealState(CLUSTER_NAME, semiAuto);
      for (String partition : semiAutoIdealState.getPartitionSet()) {
        semiAutoIdealState.setPreferenceList(partition, new ArrayList<>(leaders));
      }
      _admin.setResourceIdealState(CLUSTER_NAME, semiAuto, semiAutoIdealState);
      String leader = leaders.get(0);
      Assert.assertTrue(TestHelper.verify(() -> {
        Map<String, Map<String, String>> view = readExternalView(semiAuto);
        return view.size() == 2 && view.values().stream().allMatch(
            stateMap -> "LEADER".equals(stateMap.get(leader)) && stateMap.size() == REPLICA
                && semiAutoNodes.containsAll(stateMap.keySet()));
      }, TestHelper.WAIT_DURATION),
          "The SEMI_AUTO resource must follow its new preference lists, isolation " + isolation);
      Assert.assertTrue(TestHelper.verify(() -> servedOn(delayedAuto, 2, _nodesByClique.get(2)),
          TestHelper.WAIT_DURATION),
          "The DelayedAutoRebalancer resource must be served on clique 2, isolation " + isolation);
      Assert.assertEquals(placements(_assignmentMetadataStore.getBestPossibleAssignment()
          .get(resourceName(BROKEN_CLIQUE))), brokenBefore,
          "Either way the broken clique keeps its last assignment, isolation " + isolation);
    }
  }

  /**
   * In maintenance mode the Helix controller stops running WAGED, so nothing moves, not even onto
   * a node a healthy clique just gained. The read-only what-if still answers with the broken clique
   * carried. On exit isolation picks up where it left off: the healthy clique grows onto its new
   * node and the broken clique is still carried.
   */
  @Test
  public void testMaintenanceModeFreezesPlacementAndIsolationResumesOnExit() throws Exception {
    Map<String, Set<String>> brokenBefore = placements(
        _assignmentMetadataStore.getBestPossibleAssignment().get(resourceName(BROKEN_CLIQUE)));
    breakClique(BROKEN_CLIQUE);
    int growingClique = 1;
    String newNode = PARTICIPANT_PREFIX + "_" + _nextPort++;
    List<String> grownClique = new ArrayList<>(_nodesByClique.get(growingClique));
    grownClique.add(newNode);
    try {
      _admin.manuallyEnableMaintenanceMode(CLUSTER_NAME, true, "isolation parity", null);
      addNode(newNode, growingClique, ZONE + "_0", newNode, null);
      Assert.assertFalse(TestHelper.verify(() -> hostsReplicaOf(resourceName(growingClique),
          newNode), FROZEN_WINDOW_MS), "Nothing may move onto a new node during maintenance");
      Assert.assertEquals(isolationGauge(), 1L,
          "The clique is still broken, so the gauge must keep saying so during maintenance");

      Map<String, ResourceAssignment> whatIf =
          targetWhatIf(_configAccessor.getClusterConfig(CLUSTER_NAME));
      Assert.assertEquals(placements(whatIf.get(resourceName(BROKEN_CLIQUE))), brokenBefore,
          "The what-if must carry the broken clique during maintenance");
      assertFullyPlacedOn(whatIf.get(resourceName(growingClique)), grownClique,
          "Clique " + growingClique + " in the what-if during maintenance");

      _admin.manuallyEnableMaintenanceMode(CLUSTER_NAME, false, "isolation parity", null);
      Assert.assertTrue(TestHelper.verify(() -> hostsReplicaOf(resourceName(growingClique),
          newNode) && servedOn(resourceName(growingClique), PARTITIONS, grownClique),
          TestHelper.WAIT_DURATION),
          "After maintenance the healthy clique must grow onto its new node");
      Assert.assertEquals(isolationGauge(), 1L);
      Assert.assertEquals(placements(_assignmentMetadataStore.getBestPossibleAssignment()
          .get(resourceName(BROKEN_CLIQUE))), brokenBefore,
          "After maintenance the broken clique must still be carried");
    } finally {
      if (_admin.isInMaintenanceMode(CLUSTER_NAME)) {
        _admin.manuallyEnableMaintenanceMode(CLUSTER_NAME, false, "isolation parity", null);
      }
      removeNode(newNode);
    }
  }

  /**
   * The Helix controller puts the cluster into maintenance on its own when too many instances are
   * offline and takes it out again once they are back. That cycle, which counts instances and
   * never asks WAGED, must work the same while a clique is broken, and isolation must carry the
   * broken clique through it.
   */
  @Test
  public void testAutoMaintenanceEntersAndExitsWhileACliqueIsBroken() throws Exception {
    Map<String, Set<String>> brokenBefore = placements(
        _assignmentMetadataStore.getBestPossibleAssignment().get(resourceName(BROKEN_CLIQUE)));
    breakClique(BROKEN_CLIQUE);
    String offline = _nodesByClique.get(2).get(0);
    try {
      setOfflineInstanceLimits(0, 0);
      _participants.get(offline).syncStop();
      Assert.assertTrue(TestHelper.verify(() -> _admin.isInMaintenanceMode(CLUSTER_NAME),
          TestHelper.WAIT_DURATION),
          "One offline instance over a limit of zero must put the cluster into maintenance");
      restartParticipant(offline);
      Assert.assertTrue(TestHelper.verify(() -> !_admin.isInMaintenanceMode(CLUSTER_NAME),
          TestHelper.WAIT_DURATION),
          "The cluster must leave maintenance on its own once the instance is back");
      for (int clique = 0; clique < CLIQUE_COUNT; clique++) {
        if (clique != BROKEN_CLIQUE) {
          int healthy = clique;
          Assert.assertTrue(TestHelper.verify(
              () -> servedOn(resourceName(healthy), PARTITIONS, _nodesByClique.get(healthy)),
              TestHelper.WAIT_DURATION), "Clique " + healthy + " must be served again");
        }
      }
      Assert.assertEquals(isolationGauge(), 1L);
      Assert.assertEquals(placements(_assignmentMetadataStore.getBestPossibleAssignment()
          .get(resourceName(BROKEN_CLIQUE))), brokenBefore,
          "The broken clique must be carried through the maintenance cycle");
    } finally {
      setOfflineInstanceLimits(-1, -1);
      if (!_participants.get(offline).isConnected()) {
        restartParticipant(offline);
      }
    }
  }

  /**
   * The live Helix controller runs the global rebalance asynchronously by default. In the
   * synchronous mode a healthy clique must still rebalance, here onto a node it gains, while
   * another clique is broken and carried.
   */
  @Test
  public void testHealthyCliqueGrowsWhileAnotherIsBrokenInSynchronousMode() throws Exception {
    int growingClique = 1;
    String newNode = PARTICIPANT_PREFIX + "_" + _nextPort++;
    List<String> grownClique = new ArrayList<>(_nodesByClique.get(growingClique));
    grownClique.add(newNode);
    try {
      setGlobalRebalanceAsyncMode(false);
      Map<String, Set<String>> brokenBefore = placements(
          _assignmentMetadataStore.getBestPossibleAssignment().get(resourceName(BROKEN_CLIQUE)));
      breakClique(BROKEN_CLIQUE);
      addNode(newNode, growingClique, ZONE + "_1", newNode, null);
      Assert.assertTrue(TestHelper.verify(() -> hostsReplicaOf(resourceName(growingClique),
          newNode) && servedOn(resourceName(growingClique), PARTITIONS, grownClique),
          TestHelper.WAIT_DURATION),
          "In synchronous mode the healthy clique must grow onto its new node");
      Assert.assertEquals(isolationGauge(), 1L);
      Assert.assertEquals(placements(_assignmentMetadataStore.getBestPossibleAssignment()
          .get(resourceName(BROKEN_CLIQUE))), brokenBefore,
          "In synchronous mode the broken clique must still be carried");
    } finally {
      setGlobalRebalanceAsyncMode(true);
      removeNode(newNode);
    }
  }

  /**
   * {@link BestPossibleExternalViewVerifier} dry runs WAGED to decide whether the cluster has
   * converged, without writing the assignment metadata or registering any metric. With a broken
   * clique it must accept the carried forward clique. Its dry run takes the global rebalance mode
   * from the cluster config: asynchronous, as stock, it checks against the persisted baseline;
   * synchronous, it recomputes the baseline and so sees a healthy clique's pending moves. With
   * the flag off that synchronous dry run fails as a whole, as stock does.
   */
  @Test
  public void testBestPossibleVerifierSeesHealthyCliquesWhileAnotherIsBroken() throws Exception {
    breakClique(BROKEN_CLIQUE);
    Assert.assertTrue(verifier().verifyByPolling());
    Assert.assertTrue(bestPossibleVerifies(TestHelper.WAIT_DURATION, 0, 1, 2),
        "The dry run must carry the broken clique like the Helix controller does, so a "
            + "converged cluster verifies");

    // With the Helix controller stopped the external view is frozen, so a node the healthy clique
    // gains leaves its moves pending.
    _controller.syncStop();
    int growingClique = 1;
    String newNode = PARTICIPANT_PREFIX + "_" + _nextPort++;
    try {
      addNode(newNode, growingClique, ZONE + "_1", newNode, null);
      Map<String, Long> writeMarkers = assignmentMetadataWriteMarkers();
      Set<ObjectName> mBeans = clusterMBeans();
      Assert.assertTrue(bestPossibleVerifies(TestHelper.WAIT_DURATION, 0, 1, 2),
          "An asynchronous dry run checks against the persisted baseline, as stock does, so the "
              + "new node's moves are not visible to it yet");

      setGlobalRebalanceAsyncMode(false);
      Assert.assertTrue(bestPossibleVerifies(TestHelper.WAIT_DURATION, BROKEN_CLIQUE),
          "A synchronous dry run must still carry the broken clique");
      // With the flag on a clique the dry run cannot place is carried, never failed, so a
      // mismatch here can only be a fresh placement.
      Assert.assertFalse(bestPossibleVerifies(FROZEN_WINDOW_MS, growingClique),
          "A synchronous dry run must place the healthy clique onto its new node, so the pending "
              + "moves must fail the check");
      Assert.assertEquals(assignmentMetadataWriteMarkers(), writeMarkers,
          "The dry run must never write the assignment metadata");
      Assert.assertTrue(mBeans.containsAll(clusterMBeans()),
          "The dry run must not register any metric");

      setIsolationEnabled(false);
      Assert.assertFalse(bestPossibleVerifies(FROZEN_WINDOW_MS, BROKEN_CLIQUE),
          "With the flag off the synchronous dry run must fail as a whole, as stock does");
      Assert.assertEquals(assignmentMetadataWriteMarkers(), writeMarkers);
    } finally {
      removeNode(newNode);
      _controller = newController();
    }
  }

  /**
   * A DISABLE on a healthy clique's node takes it out of the active set, so the emergency rebalance
   * moves its replicas to the rest of the clique. That scope places only the displaced replicas, so
   * it works with the flag off too. Moving replicas back once the node is enabled again is the
   * partial rebalance's job; with the flag on the healthy clique must take its node back.
   */
  @Test
  public void testDisabledNodeOfAHealthyCliqueWhileAnotherIsBroken() throws Exception {
    Map<String, Set<String>> brokenBefore = placements(
        _assignmentMetadataStore.getBestPossibleAssignment().get(resourceName(BROKEN_CLIQUE)));
    breakClique(BROKEN_CLIQUE);
    int clique = 2;
    String node = _nodesByClique.get(clique).get(1);
    List<String> remaining = new ArrayList<>(_nodesByClique.get(clique));
    remaining.remove(node);
    for (boolean isolation : new boolean[] {true, false}) {
      setIsolationEnabled(isolation);
      String mode = isolation ? "With the flag on" : "With the flag off";
      _admin.setInstanceOperation(CLUSTER_NAME, node,
          InstanceConstants.InstanceOperation.DISABLE);
      Assert.assertTrue(TestHelper.verify(
          () -> servedOn(resourceName(clique), PARTITIONS, remaining), TestHelper.WAIT_DURATION),
          mode + " the disabled node's replicas must move to the rest of its clique");
      Assert.assertEquals(placements(_assignmentMetadataStore.getBestPossibleAssignment()
          .get(resourceName(BROKEN_CLIQUE))), brokenBefore,
          mode + " the broken clique must stay where it is");

      _admin.setInstanceOperation(CLUSTER_NAME, node, InstanceConstants.InstanceOperation.ENABLE);
      if (isolation) {
        Assert.assertTrue(TestHelper.verify(() -> hostsReplicaOf(resourceName(clique), node)
            && servedOn(resourceName(clique), PARTITIONS, _nodesByClique.get(clique)),
            TestHelper.WAIT_DURATION),
            "With the flag on the healthy clique must take its node back");
        Assert.assertEquals(isolationGauge(), 1L);
      }
    }
  }

  private double rebalancerRatio(String metric) throws Exception {
    return ((Number) ManagementFactory.getPlatformMBeanServer().getAttribute(
        new ObjectName(String.format(REBALANCER_MBEAN, CLUSTER_NAME)), metric)).doubleValue();
  }

  private boolean bestPossibleVerifies(long timeout, int... cliques) {
    Set<String> resources = new HashSet<>();
    for (int clique : cliques) {
      resources.add(resourceName(clique));
    }
    try (BestPossibleExternalViewVerifier verifier =
        new BestPossibleExternalViewVerifier.Builder(CLUSTER_NAME).setZkAddr(ZK_ADDR)
            .setResources(resources)
            .setWaitTillVerify(TestHelper.DEFAULT_REBALANCE_PROCESSING_WAIT_TIME).build()) {
      return verifier.verifyByPolling(timeout, 500L);
    }
  }

  private interface DuringSwap {
    void check() throws Exception;
  }

  private void swapAndAssertCompleted(int clique, DuringSwap duringSwap) throws Exception {
    String swapOut = _nodesByClique.get(clique).get(2);
    InstanceConfig swapOutConfig = _admin.getInstanceConfig(CLUSTER_NAME, swapOut);
    String swapIn = PARTICIPANT_PREFIX + "_" + _nextPort++;
    addNode(swapIn, clique, swapOutConfig.getDomainAsMap().get(ZONE),
        swapOutConfig.getLogicalId(LOGICAL_ID), InstanceConstants.InstanceOperation.SWAP_IN);

    boolean completed = false;
    try {
      duringSwap.check();
      Assert.assertTrue(TestHelper.verify(() -> _admin.canCompleteSwap(CLUSTER_NAME, swapOut),
          TestHelper.WAIT_DURATION),
          "The SWAP_IN node of healthy clique " + clique + " must catch up with " + swapOut);
      Assert.assertTrue(_admin.completeSwapIfPossible(CLUSTER_NAME, swapOut, false));
      completed = true;
    } finally {
      if (!completed) {
        // Leave no half finished swap behind for the following tests.
        _participants.remove(swapIn).syncStop();
        _admin.dropInstance(CLUSTER_NAME, _admin.getInstanceConfig(CLUSTER_NAME, swapIn));
      }
    }
    Assert.assertTrue(TestHelper.verify(() -> {
      Map<String, Map<String, String>> view = readExternalView(resourceName(clique));
      return view.size() == PARTITIONS && view.values().stream()
          .noneMatch(stateMap -> stateMap.containsKey(swapOut)) && view.values().stream()
          .anyMatch(stateMap -> stateMap.containsKey(swapIn));
    }, TestHelper.WAIT_DURATION), "The swap must hand the swapped out node's replicas over");
    Assert.assertEquals(_admin.getInstanceConfig(CLUSTER_NAME, swapIn).getInstanceOperation()
        .getOperation(), InstanceConstants.InstanceOperation.ENABLE);

    // The swapped in node replaces the swapped out one in the clique for the following tests.
    MockParticipantManager retired = _participants.remove(swapOut);
    retired.syncStop();
    _admin.dropInstance(CLUSTER_NAME, _admin.getInstanceConfig(CLUSTER_NAME, swapOut));
    List<String> cliqueNodes = _nodesByClique.get(clique);
    cliqueNodes.set(cliqueNodes.indexOf(swapOut), swapIn);
  }

  private static void checkBlocked(List<String> problems, ValidationResult verdict,
      String expectedResource, boolean onlyThatResource, String what) {
    if (verdict.isFeasible()) {
      problems.add(what + " makes the clique unplaceable, but the guard rail approved it");
      return;
    }
    Set<String> resources = new TreeSet<>();
    for (Violation violation : verdict.getViolations()) {
      if (violation.getResourceName() != null) {
        resources.add(violation.getResourceName());
      }
    }
    boolean blamed = onlyThatResource ? resources.equals(Collections.singleton(expectedResource))
        : resources.contains(expectedResource);
    if (!blamed) {
      problems.add(what + " must be blamed on " + expectedResource
          + (onlyThatResource ? " alone" : "") + ", but the violations were " + verdict);
    }
  }

  private ValidationResult evacuationVerdict(String instance) {
    GuardrailContext context = GuardrailContext.newBuilder(CLUSTER_NAME)
        .dataAccessor(new ZKHelixDataAccessor(CLUSTER_NAME, _baseAccessor))
        .instanceName(instance)
        .proposedInstanceOperation(InstanceConstants.InstanceOperation.EVACUATE)
        .wagedAssignmentProvider(
            (cfg, instanceConfigs, liveInstances, idealStates, resourceConfigs) -> HelixUtil
                .getTargetAssignmentForWagedFullAuto(new ZkBucketDataAccessor(ZK_ADDR),
                    _baseAccessor, cfg, instanceConfigs, liveInstances, idealStates,
                    resourceConfigs))
        .build();
    return new InstanceOperationRebalanceFeasibilityGuardrailRule().validate(context);
  }

  private ValidationResult tagRemovalVerdict(String instance, String tag) {
    GuardrailContext context = GuardrailContext.newBuilder(CLUSTER_NAME)
        .dataAccessor(new ZKHelixDataAccessor(CLUSTER_NAME, _baseAccessor))
        .instanceName(instance)
        .proposedRemovedInstanceTags(Collections.singletonList(tag))
        .wagedAssignmentProvider(
            (cfg, instanceConfigs, liveInstances, idealStates, resourceConfigs) -> HelixUtil
                .getTargetAssignmentForWagedFullAuto(new ZkBucketDataAccessor(ZK_ADDR),
                    _baseAccessor, cfg, instanceConfigs, liveInstances, idealStates,
                    resourceConfigs))
        .build();
    return new InstanceTagRebalanceFeasibilityGuardrailRule().validate(context);
  }

  private Map<String, ResourceAssignment> targetWhatIf(ClusterConfig clusterConfig) {
    HelixDataAccessor accessor = new ZKHelixDataAccessor(CLUSTER_NAME, _baseAccessor);
    PropertyKey.Builder keys = accessor.keyBuilder();
    return HelixUtil.getTargetAssignmentForWagedFullAuto(new ZkBucketDataAccessor(ZK_ADDR),
        _baseAccessor, clusterConfig, accessor.getChildValues(keys.instanceConfigs(), true),
        accessor.getChildNames(keys.liveInstances()), wagedIdealStates(accessor),
        accessor.getChildValues(keys.resourceConfigs(), true));
  }

  private Map<String, ResourceAssignment> immediateWhatIf(ClusterConfig clusterConfig) {
    HelixDataAccessor accessor = new ZKHelixDataAccessor(CLUSTER_NAME, _baseAccessor);
    PropertyKey.Builder keys = accessor.keyBuilder();
    return HelixUtil.getImmediateAssignmentForWagedFullAuto(ZK_ADDR, clusterConfig,
        accessor.getChildValues(keys.instanceConfigs(), true),
        accessor.getChildNames(keys.liveInstances()), wagedIdealStates(accessor),
        accessor.getChildValues(keys.resourceConfigs(), true));
  }

  private static List<IdealState> wagedIdealStates(HelixDataAccessor accessor) {
    List<IdealState> wagedIdealStates = new ArrayList<>();
    for (IdealState idealState : accessor.<IdealState>getChildValues(
        accessor.keyBuilder().idealStates(), true)) {
      if (WagedValidationUtil.isWagedEnabled(idealState)) {
        wagedIdealStates.add(idealState);
      }
    }
    return wagedIdealStates;
  }

  private void assertFullyPlacedOnOwnClique(ResourceAssignment assignment, int clique) {
    assertFullyPlacedOn(assignment, _nodesByClique.get(clique), "Clique " + clique);
  }

  private static void assertFullyPlacedOn(ResourceAssignment assignment,
      Collection<String> nodes, String what) {
    Map<String, Set<String>> placements = placements(assignment);
    Assert.assertEquals(placements.size(), PARTITIONS, what + ": " + placements);
    for (Map.Entry<String, Set<String>> entry : placements.entrySet()) {
      Assert.assertEquals(entry.getValue().size(), REPLICA,
          what + " partition " + entry.getKey() + ": " + placements);
      Assert.assertTrue(nodes.containsAll(entry.getValue()),
          what + " partition " + entry.getKey() + " left its nodes " + nodes + ": " + placements);
    }
  }

  private boolean hostsReplicaOf(String resource, String node) {
    return readExternalView(resource).values().stream()
        .anyMatch(stateMap -> stateMap.containsKey(node));
  }

  /**
   * Whether the external view serves every partition of the resource with a full set of replicas,
   * one LEADER and the rest STANDBY, on the given nodes only.
   */
  private boolean servedOn(String resource, int partitions, Collection<String> nodes) {
    Map<String, Map<String, String>> view = readExternalView(resource);
    return view.size() == partitions && view.values().stream().allMatch(
        stateMap -> stateMap.size() == REPLICA && nodes.containsAll(stateMap.keySet())
            && Collections.frequency(stateMap.values(), "LEADER") == 1
            && Collections.frequency(stateMap.values(), "STANDBY") == REPLICA - 1);
  }

  /**
   * Stamps of the last write to both assignment metadata blobs. Any write, even one with identical
   * content, moves them.
   */
  private Map<String, Long> assignmentMetadataWriteMarkers() {
    Map<String, Long> markers = new TreeMap<>();
    for (String blob : new String[] {"BASELINE", "BEST_POSSIBLE"}) {
      for (String marker : new String[] {"LAST_WRITE", "LAST_SUCCESSFUL_WRITE"}) {
        String path = "/" + CLUSTER_NAME + "/ASSIGNMENT_METADATA/" + blob + "/" + marker;
        Stat stat = _gZkClient.getStat(path);
        markers.put(path, stat == null ? -1L : stat.getMzxid());
      }
    }
    return markers;
  }

  private Set<ObjectName> clusterMBeans() throws Exception {
    MBeanServer server = ManagementFactory.getPlatformMBeanServer();
    Set<ObjectName> names = new TreeSet<>();
    names.addAll(server.queryNames(new ObjectName("Rebalancer:ClusterName=" + CLUSTER_NAME + ",*"),
        null));
    names.addAll(server.queryNames(new ObjectName("ClusterStatus:cluster=" + CLUSTER_NAME + ",*"),
        null));
    return names;
  }

  private static Map<String, Set<String>> placements(ResourceAssignment assignment) {
    Map<String, Set<String>> placements = new TreeMap<>();
    if (assignment == null) {
      return placements;
    }
    assignment.getMappedPartitions().forEach(partition -> placements
        .put(partition.getPartitionName(),
            new TreeSet<>(assignment.getReplicaMap(partition).keySet())));
    return placements;
  }

  private Map<String, Map<String, String>> readExternalView(String resource) {
    HelixDataAccessor accessor = new ZKHelixDataAccessor(CLUSTER_NAME, _baseAccessor);
    ExternalView externalView = accessor.getProperty(accessor.keyBuilder().externalView(resource));
    Map<String, Map<String, String>> view = new TreeMap<>();
    if (externalView != null) {
      externalView.getPartitionSet()
          .forEach(partition -> view.put(partition, externalView.getStateMap(partition)));
    }
    return view;
  }

  /**
   * Caps the clique's resource at one partition per instance: three of its eight replicas fit, so
   * the clique is unplaceable without adding any demand. Waits until the Helix controller reports
   * it skipped and carried forward.
   */
  private void breakClique(int clique) throws Exception {
    setResourceConfig(resourceName(clique), HEALTHY_PARTITION_WEIGHT,
        BREAKING_MAX_PARTITIONS_PER_INSTANCE);
    Assert.assertTrue(TestHelper.verify(() -> isolationGauge() == 1, TestHelper.WAIT_DURATION),
        "Clique " + clique + " must be reported as skipped");
    Assert.assertTrue(TestHelper.verify(
        () -> _assignmentMetadataStore.getBaseline().containsKey(resourceName(clique))
            && _assignmentMetadataStore.getBestPossibleAssignment()
            .containsKey(resourceName(clique)), TestHelper.WAIT_DURATION),
        "Clique " + clique + " must be carried forward in both blobs");
  }

  private void setResourceConfig(String resource, int weight, int maxPartitionsPerInstance)
      throws Exception {
    ResourceConfig resourceConfig = _configAccessor.getResourceConfig(CLUSTER_NAME, resource);
    resourceConfig.setPartitionCapacityMap(Collections.singletonMap(
        ResourceConfig.DEFAULT_PARTITION_KEY, Collections.singletonMap(CAPACITY_KEY, weight)));
    if (maxPartitionsPerInstance >= 0) {
      resourceConfig.getRecord().setIntField(MAX_PARTITIONS_FIELD, maxPartitionsPerInstance);
    } else {
      resourceConfig.getRecord().getSimpleFields().remove(MAX_PARTITIONS_FIELD);
    }
    _configAccessor.setResourceConfig(CLUSTER_NAME, resource, resourceConfig);
  }

  /**
   * Writes the resource config (tag, weight, optional per instance cap) before the ideal state, so
   * the very first calculation already sees the final configuration.
   */
  private void createCliqueResource(String resource, int clique, int weight,
      int maxPartitionsPerInstance) throws Exception {
    ResourceConfig resourceConfig = new ResourceConfig(resource);
    resourceConfig.getRecord().setSimpleField(
        ResourceConfig.ResourceConfigProperty.INSTANCE_GROUP_TAG.name(), cliqueTag(clique));
    resourceConfig.setPartitionCapacityMap(Collections.singletonMap(
        ResourceConfig.DEFAULT_PARTITION_KEY, Collections.singletonMap(CAPACITY_KEY, weight)));
    if (maxPartitionsPerInstance >= 0) {
      resourceConfig.getRecord().setIntField(MAX_PARTITIONS_FIELD, maxPartitionsPerInstance);
    }
    _configAccessor.setResourceConfig(CLUSTER_NAME, resource, resourceConfig);
    createResourceWithWagedRebalance(CLUSTER_NAME, resource,
        BuiltInStateModelDefinitions.LeaderStandby.name(), PARTITIONS, REPLICA, REPLICA);
    IdealState idealState = _admin.getResourceIdealState(CLUSTER_NAME, resource);
    idealState.setInstanceGroupTag(cliqueTag(clique));
    _admin.setResourceIdealState(CLUSTER_NAME, resource, idealState);
  }

  private void addNode(String node, int clique, String zone, String logicalId,
      InstanceConstants.InstanceOperation operation) {
    InstanceConfig.Builder builder = new InstanceConfig.Builder()
        .setDomain(String.format("%s=%s, %s=%s, %s=%s", ZONE, zone, HOST, node, LOGICAL_ID,
            logicalId))
        .addTag(cliqueTag(clique));
    if (operation != null) {
      builder.setInstanceOperation(operation);
    }
    _admin.addInstance(CLUSTER_NAME, builder.build(node));
    MockParticipantManager participant = new MockParticipantManager(ZK_ADDR, CLUSTER_NAME, node);
    participant.syncStart();
    _participants.put(node, participant);
  }

  private ClusterControllerManager newController() {
    ClusterControllerManager controller = new ClusterControllerManager(ZK_ADDR, CLUSTER_NAME,
        CONTROLLER_PREFIX + "_" + _controllerGeneration++);
    controller.syncStart();
    return controller;
  }

  private void setIsolationEnabled(boolean enabled) {
    ClusterConfig clusterConfig = _configAccessor.getClusterConfig(CLUSTER_NAME);
    if (clusterConfig.isWagedInstanceTagIsolationEnabled() != enabled) {
      clusterConfig.setWagedInstanceTagIsolationEnabled(enabled);
      _configAccessor.setClusterConfig(CLUSTER_NAME, clusterConfig);
    }
  }

  private void setGlobalRebalanceAsyncMode(boolean async) {
    ClusterConfig clusterConfig = _configAccessor.getClusterConfig(CLUSTER_NAME);
    if (clusterConfig.isGlobalRebalanceAsyncModeEnabled() != async) {
      clusterConfig.setGlobalRebalanceAsyncMode(async);
      _configAccessor.setClusterConfig(CLUSTER_NAME, clusterConfig);
    }
  }

  /**
   * The auto exit threshold may never exceed the maximum, so the maximum is set first when the
   * limits are turned on and last when they are turned off.
   */
  private void setOfflineInstanceLimits(int maxOffline, int autoExit) {
    ClusterConfig clusterConfig = _configAccessor.getClusterConfig(CLUSTER_NAME);
    if (clusterConfig.getMaxOfflineInstancesAllowed() == maxOffline
        && clusterConfig.getNumOfflineInstancesForAutoExit() == autoExit) {
      return;
    }
    if (maxOffline >= 0) {
      clusterConfig.setMaxOfflineInstancesAllowed(maxOffline);
      clusterConfig.setNumOfflineInstancesForAutoExit(autoExit);
    } else {
      clusterConfig.setNumOfflineInstancesForAutoExit(autoExit);
      clusterConfig.setMaxOfflineInstancesAllowed(maxOffline);
    }
    _configAccessor.setClusterConfig(CLUSTER_NAME, clusterConfig);
  }

  private void restartParticipant(String node) {
    MockParticipantManager participant = new MockParticipantManager(ZK_ADDR, CLUSTER_NAME, node);
    participant.syncStart();
    _participants.put(node, participant);
  }

  private void removeNode(String node) {
    MockParticipantManager participant = _participants.remove(node);
    if (participant == null) {
      return;
    }
    if (participant.isConnected()) {
      participant.syncStop();
    }
    _admin.dropInstance(CLUSTER_NAME, _admin.getInstanceConfig(CLUSTER_NAME, node));
  }

  private ZkHelixClusterVerifier verifier() {
    Set<String> resources = new HashSet<>();
    for (int clique = 0; clique < CLIQUE_COUNT; clique++) {
      resources.add(resourceName(clique));
    }
    return new StrictMatchExternalViewVerifier.Builder(CLUSTER_NAME).setZkAddr(ZK_ADDR)
        .setDeactivatedNodeAwareness(true).setResources(resources)
        .setWaitTillVerify(TestHelper.DEFAULT_REBALANCE_PROCESSING_WAIT_TIME).build();
  }

  /**
   * Waits until no baseline calculation is running or queued. A restore write can start a
   * baseline, which lands asynchronously, and the partial rebalance that follows it can move
   * replicas, so a placement a test snapshots and later compares exactly is read only after this
   * and the verifier return. The baseline thread carries the cluster name only while a calculation
   * runs, and a queued one starts as soon as the thread is free, so the thread has to stay idle
   * with the calculation counter constant across a processing wait.
   */
  private void awaitBaselineIdle() throws Exception {
    ObjectName rebalancer = new ObjectName(String.format(REBALANCER_MBEAN, CLUSTER_NAME));
    Assert.assertTrue(TestHelper.verify(() -> {
      // A new Helix controller registers the rebalancer metrics in its first pipeline, and that
      // pipeline starts a baseline, so a missing MBean or a counter at zero counts as busy.
      if (!ManagementFactory.getPlatformMBeanServer().isRegistered(rebalancer)) {
        return false;
      }
      long started = rebalancerMetric("GlobalBaselineCalcCounter");
      if (started == 0 || baselineRunning()) {
        return false;
      }
      Thread.sleep(TestHelper.DEFAULT_REBALANCE_PROCESSING_WAIT_TIME);
      return !baselineRunning() && rebalancerMetric("GlobalBaselineCalcCounter") == started;
    }, TestHelper.WAIT_DURATION), "Every baseline calculation must land");
  }

  private boolean baselineRunning() {
    String name = "WagedGlobalRebalance-" + CLUSTER_NAME;
    return Thread.getAllStackTraces().keySet().stream().anyMatch(t -> name.equals(t.getName()));
  }

  private long isolationGauge() throws Exception {
    return clusterStatusMetric("WagedInstanceTagIsolationSkippedResourcesGauge");
  }

  private long clusterStatusMetric(String metric) throws Exception {
    return ((Number) ManagementFactory.getPlatformMBeanServer().getAttribute(
        new ObjectName(String.format(CLUSTER_STATUS_MBEAN, CLUSTER_NAME)), metric)).longValue();
  }

  private long rebalancerMetric(String metric) throws Exception {
    return ((Number) ManagementFactory.getPlatformMBeanServer().getAttribute(
        new ObjectName(String.format(REBALANCER_MBEAN, CLUSTER_NAME)), metric)).longValue();
  }

  private Map<String, Long> failureMetrics() throws Exception {
    Map<String, Long> metrics = new TreeMap<>();
    for (String metric : REBALANCER_FAILURE_METRICS) {
      metrics.put("Rebalancer." + metric, rebalancerMetric(metric));
    }
    for (String metric : CLUSTER_STATUS_FAILURE_METRICS) {
      metrics.put("ClusterStatus." + metric, clusterStatusMetric(metric));
    }
    return metrics;
  }
}
