package org.apache.helix.controller.rebalancer.waged.model;

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

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.concurrent.ConcurrentHashMap;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import java.util.stream.Collectors;

import org.apache.helix.HelixRebalanceException;
import org.apache.helix.controller.rebalancer.util.WagedRebalanceUtil;
import org.apache.helix.controller.rebalancer.waged.constraints.ConstraintBasedAlgorithm;
import org.apache.helix.controller.rebalancer.waged.constraints.HardConstraint;
import org.apache.helix.model.ClusterConfig;
import org.apache.helix.model.Partition;
import org.apache.helix.model.ResourceAssignment;
import org.apache.helix.model.ResourceConfig;
import org.testng.Assert;
import org.testng.annotations.Test;

/**
 * Capacity-facing behavior of WAGED instance-tag isolation: attributing a tag blind cluster
 * wide capacity deficit to the clique that caused it, repairing partially placed skipped
 * resources outside the baseline scope, and keeping the existing hard constraint reporters
 * firing so an operator needs no new dashboards. See
 * {@link AbstractTestWagedInstanceTagIsolation} for the shared clique partitioned topology.
 */
public class TestWagedInstanceTagIsolationCapacity extends AbstractTestWagedInstanceTagIsolation {
  // ---------------------------------------------------------------------------------------------
  // Skipped resources that are already partially placed on the nodes (PARTIAL/EMERGENCY scopes)
  // ---------------------------------------------------------------------------------------------

  /**
   * In the non baseline scopes the nodes come pre-loaded with the replicas that are already
   * allocated, so a skipped resource can still emit a PARTIAL assignment. WagedRebalanceUtil has to
   * overwrite that partial entry with the complete previous assignment, otherwise a half assigned
   * resource would be persisted.
   */
  @Test
  public void testPartiallyPlacedSkippedResourceIsReplacedByThePreviousAssignment()
      throws HelixRebalanceException, IOException {
    ClusterConfig clusterConfig = createClusterConfig(true);
    ResourceConfig brokenConfig =
        taggedResource("R_broken", cliqueTag(0), UNPLACEABLE_PARTITION_WEIGHT);

    // One partition of the broken resource is already sitting on a node, the other still needs to
    // be placed and cannot be.
    AssignableReplica alreadyPlaced =
        new AssignableReplica(clusterConfig, brokenConfig, "R_broken_0", "ONLINE", 0);
    Set<AssignableReplica> toBeAssigned = new HashSet<>();
    toBeAssigned.add(new AssignableReplica(clusterConfig, brokenConfig, "R_broken_1", "ONLINE", 0));
    addReplicas(toBeAssigned, clusterConfig,
        taggedResource("R_healthy", cliqueTag(1), HEALTHY_PARTITION_WEIGHT), 3);

    Set<AssignableNode> nodes = new HashSet<>();
    AssignableNode preloaded = taggedNode(clusterConfig, instanceName(0, 0), 0, cliqueTag(0));
    preloaded.assignInitBatch(Collections.singleton(alreadyPlaced));
    nodes.add(preloaded);
    for (int i = 1; i < 3; i++) {
      nodes.add(taggedNode(clusterConfig, instanceName(0, i), i, cliqueTag(0)));
    }
    for (int i = 0; i < 3; i++) {
      nodes.add(taggedNode(clusterConfig, instanceName(1, i), i, cliqueTag(1)));
    }

    Set<AssignableReplica> allReplicas = new HashSet<>(toBeAssigned);
    allReplicas.add(alreadyPlaced);
    ClusterContext context = new ClusterContext(allReplicas, nodes, Collections.emptyMap(),
        Collections.emptyMap(), clusterConfig);
    ClusterModel clusterModel = new ClusterModel(context, toBeAssigned, nodes,
        ClusterModel.RebalanceScopeType.PARTIAL);

    // Sanity check: without the fallback the algorithm really does emit a partial entry.
    OptimalAssignment raw = createAlgorithm().calculate(clusterModel);
    Map<String, ResourceAssignment> rawResult = raw.getOptimalResourceAssignment();
    Assert.assertEquals(raw.getSkippedResources(), Collections.singleton("R_broken"));
    Assert.assertEquals(rawResult.get("R_broken").getMappedPartitions().size(), 1,
        "Precondition: the skipped resource is emitted half assigned before the fallback runs");

    // The complete previous assignment the controller would carry forward.
    ResourceAssignment previous = new ResourceAssignment("R_broken");
    previous.addReplicaMap(new Partition("R_broken_0"),
        Collections.singletonMap(instanceName(0, 0), "ONLINE"));
    previous.addReplicaMap(new Partition("R_broken_1"),
        Collections.singletonMap(instanceName(0, 1), "ONLINE"));

    Map<String, ResourceAssignment> withFallback = WagedRebalanceUtil.calculateAssignment(
        new ClusterModel(context, toBeAssigned, nodes, ClusterModel.RebalanceScopeType.PARTIAL),
        createAlgorithm(), Collections.singletonMap("R_broken", previous));

    Assert.assertEquals(withFallback.get("R_broken").getMappedPartitions().size(), 2,
        "The partial entry must be replaced by the complete previous assignment");
    Assert.assertEquals(normalize(withFallback).get("R_broken"), normalize(
        Collections.singletonMap("R_broken", previous)).get("R_broken"));
    Assert.assertNotSame(withFallback.get("R_broken"), previous,
        "The carried over assignment must be a copy, never the caller's own object");
    Assert.assertTrue(withFallback.containsKey("R_healthy"),
        "The healthy clique keeps its newly calculated assignment");
  }

  /**
   * The cluster wide capacity check is a tag blind sum over every replica in the cluster, but the
   * replica list handed to the algorithm holds only what still has to be assigned. In the partial
   * and emergency scopes those are different sets: a clique whose replicas are already placed
   * contributes to the deficit yet appears nowhere in the outstanding list.
   *
   * Attribution therefore has to measure a block's demand over the same universe the deficit came
   * from, counting what is already sitting on the block's nodes as well as what is still waiting.
   * Without that, an oversubscribed clique looks like it demands nothing, no block is ever blamed,
   * and every healthy clique in the cluster is frozen behind it on every single pipeline run. That
   * is the exact outage this mode exists to prevent, so it must hold outside the baseline scope.
   */
  @Test
  public void testCapacityDeficitIsAttributedWhenTheGuiltyCliqueIsAlreadyPlaced()
      throws HelixRebalanceException, IOException {
    ClusterConfig clusterConfig = createClusterConfig(true);

    // Clique 0 is wildly oversubscribed: five replicas of weight 150 already sitting on three
    // nodes of capacity 100. Nothing about it is outstanding, so it contributes no entry at all to
    // the replica list the algorithm is given.
    ResourceConfig brokenConfig =
        taggedResource("R_broken", cliqueTag(0), UNPLACEABLE_PARTITION_WEIGHT);
    Set<AssignableReplica> alreadyPlaced = new HashSet<>();
    addReplicas(alreadyPlaced, clusterConfig, brokenConfig, 5);

    // Clique 1 is healthy and is the only thing that still needs assigning.
    Set<AssignableReplica> toBeAssigned = new HashSet<>();
    addReplicas(toBeAssigned, clusterConfig,
        taggedResource("R_healthy", cliqueTag(1), HEALTHY_PARTITION_WEIGHT), 3);

    Set<AssignableNode> nodes = new HashSet<>();
    List<AssignableNode> brokenNodes = new ArrayList<>();
    for (int i = 0; i < 3; i++) {
      AssignableNode node = taggedNode(clusterConfig, instanceName(0, i), i, cliqueTag(0));
      brokenNodes.add(node);
      nodes.add(node);
      nodes.add(taggedNode(clusterConfig, instanceName(1, i), i, cliqueTag(1)));
    }
    // Pre-load the broken clique's nodes exactly the way the cluster model provider does.
    int index = 0;
    for (AssignableReplica replica : alreadyPlaced) {
      brokenNodes.get(index++ % brokenNodes.size()).assignInitBatch(
          Collections.singleton(replica));
    }

    Set<AssignableReplica> everything = new HashSet<>(toBeAssigned);
    everything.addAll(alreadyPlaced);
    ClusterContext context = new ClusterContext(everything, nodes, Collections.emptyMap(),
        Collections.emptyMap(), clusterConfig);

    // Precondition: the cluster wide sum really is negative, so the default mode would throw here.
    Assert.assertTrue(context.getEstimateUtilizationMap().get(CAPACITY_KEY) < 0,
        "Precondition: the tag blind cluster wide capacity check must be in deficit");

    OptimalAssignment assignment = createAlgorithm().calculate(
        new ClusterModel(context, toBeAssigned, nodes, ClusterModel.RebalanceScopeType.PARTIAL));
    Map<String, ResourceAssignment> result = assignment.getOptimalResourceAssignment();

    Assert.assertEquals(result.get("R_healthy").getMappedPartitions().size(), 3,
        "The healthy clique must be rebalanced normally rather than frozen behind clique 0");
    Assert.assertEquals(result.get("R_broken").getMappedPartitions().size(), 5,
        "The clique that caused the deficit keeps the assignment it already had, whole");
    // It owns no replica in this run, so it never reaches the placement loop and is not "skipped"
    // in the half assigned sense the carry forward exists to repair. Nothing to repair means
    // nothing to carry forward.
    Assert.assertTrue(assignment.getSkippedResources().isEmpty());
  }

  /**
   * A narrow rebalance scope frequently carries work for one clique only, and when that clique is
   * the broken one every group in the scope fails. Judged over the scope that reads as a total
   * cluster failure, the isolation rethrows, the caller discards the whole pipeline result and
   * falls back to the last known good assignment, and every healthy clique is frozen again. The
   * count has to be taken over the blocks the cluster's nodes form instead.
   */
  @Test
  public void testNarrowScopeWhereOnlyTheBrokenCliqueHasWorkStillIsolates()
      throws HelixRebalanceException, IOException {
    ClusterConfig clusterConfig = createClusterConfig(true);

    // Six cliques of two nodes exist, but only clique 0 has anything outstanding, and its partition
    // is heavier than any single node in it so it can never be placed. The cluster wide sum stays
    // comfortably positive, so this is the tag local failure rather than a capacity deficit.
    Set<AssignableReplica> toBeAssigned = new HashSet<>();
    addReplicas(toBeAssigned, clusterConfig,
        taggedResource("R_broken", cliqueTag(0), UNPLACEABLE_PARTITION_WEIGHT), 2);

    Set<AssignableNode> nodes = new HashSet<>();
    for (int clique = 0; clique < 6; clique++) {
      for (int i = 0; i < 2; i++) {
        nodes.add(taggedNode(clusterConfig, instanceName(clique, i), i, cliqueTag(clique)));
      }
    }

    ClusterContext context = new ClusterContext(toBeAssigned, nodes, Collections.emptyMap(),
        Collections.emptyMap(), clusterConfig);
    Assert.assertTrue(context.getEstimateUtilizationMap().get(CAPACITY_KEY) > 0,
        "Precondition: this must be a tag local failure, not a cluster wide capacity deficit");

    OptimalAssignment assignment = createAlgorithm().calculate(
        new ClusterModel(context, toBeAssigned, nodes, ClusterModel.RebalanceScopeType.PARTIAL));

    Assert.assertEquals(assignment.getSkippedResources(), Collections.singleton("R_broken"),
        "The only group with work failed, but five other blocks exist, so it is skipped rather "
            + "than rethrown");
    Assert.assertTrue(assignment.getOptimalResourceAssignment().isEmpty()
            || assignment.getOptimalResourceAssignment().get("R_broken") == null
            || assignment.getOptimalResourceAssignment().get("R_broken").getMappedPartitions()
            .isEmpty(),
        "Nothing of the broken clique is half assigned; the caller carries its previous "
            + "assignment forward whole");
  }

  /**
   * The same narrow scope with the flag off still throws, so the behaviour above is opt in.
   */
  @Test
  public void testNarrowScopeWhereOnlyTheBrokenCliqueHasWorkStillThrowsWithIsolationOff()
      throws IOException {
    ClusterConfig clusterConfig = createClusterConfig(false);

    Set<AssignableReplica> toBeAssigned = new HashSet<>();
    addReplicas(toBeAssigned, clusterConfig,
        taggedResource("R_broken", cliqueTag(0), UNPLACEABLE_PARTITION_WEIGHT), 2);

    Set<AssignableNode> nodes = new HashSet<>();
    for (int clique = 0; clique < 6; clique++) {
      for (int i = 0; i < 2; i++) {
        nodes.add(taggedNode(clusterConfig, instanceName(clique, i), i, cliqueTag(clique)));
      }
    }

    ClusterContext context = new ClusterContext(toBeAssigned, nodes, Collections.emptyMap(),
        Collections.emptyMap(), clusterConfig);
    try {
      createAlgorithm().calculate(
          new ClusterModel(context, toBeAssigned, nodes, ClusterModel.RebalanceScopeType.PARTIAL))
          .getOptimalResourceAssignment();
      Assert.fail("The default mode must still fail the whole rebalance");
    } catch (HelixRebalanceException e) {
      Assert.assertEquals(e.getFailureType(),
          HelixRebalanceException.Type.FAILED_TO_CALCULATE);
    }
  }

  /**
   * The same cluster with the flag off still fails exactly as it does today, so the behaviour above
   * is opt in and parity is untouched.
   */
  @Test
  public void testAlreadyPlacedCapacityDeficitStillThrowsWithIsolationOff()
      throws IOException {
    ClusterConfig clusterConfig = createClusterConfig(false);
    ResourceConfig brokenConfig =
        taggedResource("R_broken", cliqueTag(0), UNPLACEABLE_PARTITION_WEIGHT);
    Set<AssignableReplica> alreadyPlaced = new HashSet<>();
    addReplicas(alreadyPlaced, clusterConfig, brokenConfig, 5);

    Set<AssignableReplica> toBeAssigned = new HashSet<>();
    addReplicas(toBeAssigned, clusterConfig,
        taggedResource("R_healthy", cliqueTag(1), HEALTHY_PARTITION_WEIGHT), 3);

    Set<AssignableNode> nodes = new HashSet<>();
    List<AssignableNode> brokenNodes = new ArrayList<>();
    for (int i = 0; i < 3; i++) {
      AssignableNode node = taggedNode(clusterConfig, instanceName(0, i), i, cliqueTag(0));
      brokenNodes.add(node);
      nodes.add(node);
      nodes.add(taggedNode(clusterConfig, instanceName(1, i), i, cliqueTag(1)));
    }
    int index = 0;
    for (AssignableReplica replica : alreadyPlaced) {
      brokenNodes.get(index++ % brokenNodes.size()).assignInitBatch(
          Collections.singleton(replica));
    }

    Set<AssignableReplica> everything = new HashSet<>(toBeAssigned);
    everything.addAll(alreadyPlaced);
    ClusterContext context = new ClusterContext(everything, nodes, Collections.emptyMap(),
        Collections.emptyMap(), clusterConfig);

    try {
      createAlgorithm().calculate(
          new ClusterModel(context, toBeAssigned, nodes, ClusterModel.RebalanceScopeType.PARTIAL));
      Assert.fail("The default mode must still fail the whole rebalance on a capacity deficit");
    } catch (HelixRebalanceException e) {
      Assert.assertEquals(e.getFailureCategory(),
          HelixRebalanceException.FailureCategory.CAPACITY_DEFICIT);
    }
  }

  /**
   * A brand new resource pinned to a clique that cannot be placed has no previous assignment to
   * carry forward. It must be dropped from the result rather than persisted half assigned.
   */
  @Test
  public void testSkippedResourceWithNoPreviousAssignmentIsDropped()
      throws HelixRebalanceException, IOException {
    ClusterConfig clusterConfig = createClusterConfig(true);
    Set<AssignableReplica> replicas = new HashSet<>();
    addReplicas(replicas, clusterConfig,
        taggedResource("R_broken", cliqueTag(0), UNPLACEABLE_PARTITION_WEIGHT), 2);
    addReplicas(replicas, clusterConfig,
        taggedResource("R_healthy", cliqueTag(1), HEALTHY_PARTITION_WEIGHT), 2);

    Set<AssignableNode> nodes = new HashSet<>();
    for (int i = 0; i < 3; i++) {
      nodes.add(taggedNode(clusterConfig, instanceName(0, i), i, cliqueTag(0)));
      nodes.add(taggedNode(clusterConfig, instanceName(1, i), i, cliqueTag(1)));
    }

    ClusterContext context = new ClusterContext(replicas, nodes, Collections.emptyMap(),
        Collections.emptyMap(), clusterConfig);
    Map<String, ResourceAssignment> result = WagedRebalanceUtil.calculateAssignment(
        new ClusterModel(context, replicas, nodes, ClusterModel.RebalanceScopeType.GLOBAL_BASELINE),
        createAlgorithm(), Collections.emptyMap());

    Assert.assertFalse(result.containsKey("R_broken"),
        "A skipped resource with nothing to carry forward must be dropped, not half persisted");
    Assert.assertTrue(result.containsKey("R_healthy"));
  }

  // ---------------------------------------------------------------------------------------------
  // Cluster wide capacity deficit attribution
  // ---------------------------------------------------------------------------------------------

  /**
   * A weight big enough that clique 3 alone drags the tag blind cluster wide capacity sum negative.
   * Cluster capacity is 20 x 10 x 100 = 20000 and the healthy demand is 20 x 10 x 10 = 2000, so a
   * single clique needs more than 18000 to overflow the whole cluster. 10 partitions x 2500 = 25000
   * does it.
   */
  private static final int CLUSTER_OVERFLOWING_PARTITION_WEIGHT = 2500;

  /**
   * The check that precedes the assignment pass sums capacity across the whole cluster with no
   * regard for tags, so one wildly oversubscribed clique can drag it negative while all 19 others
   * still fit comfortably on their own nodes. Left alone it would freeze the cluster before a single
   * replica was placed, which is exactly what isolation exists to prevent, so the deficit is
   * attributed to the clique that caused it.
   */
  @Test
  public void testClusterWideCapacityDeficitCausedByOneCliqueDoesNotBlockOthers()
      throws HelixRebalanceException, IOException {
    int brokenClique = 3;
    ClusterConfig clusterConfig = createClusterConfig(true);
    Map<Integer, CliqueSpec> specs = allHealthy();
    specs.put(brokenClique,
        CliqueSpec.healthy().withPartitionWeight(CLUSTER_OVERFLOWING_PARTITION_WEIGHT));

    OptimalAssignment optimalAssignment =
        createAlgorithm().calculate(createClusterModel(clusterConfig, specs));
    Map<String, ResourceAssignment> result = optimalAssignment.getOptimalResourceAssignment();

    Assert.assertEquals(optimalAssignment.getSkippedResources(),
        Collections.singleton(resourceName(brokenClique)));
    Assert.assertEquals(result.size(), CLIQUE_COUNT - 1);
    Assert.assertFalse(result.containsKey(resourceName(brokenClique)));
    for (int clique = 0; clique < CLIQUE_COUNT; clique++) {
      if (clique == brokenClique) {
        continue;
      }
      Assert.assertEquals(result.get(resourceName(clique)).getMappedPartitions().size(),
          PARTITIONS_PER_RESOURCE, "Healthy clique " + clique + " must still be fully assigned");
    }
  }

  /**
   * The same topology with the flag off keeps failing the whole cluster up front, and keeps
   * reporting it as a capacity deficit rather than as a missing candidate node.
   */
  @Test
  public void testClusterWideCapacityDeficitStillFailsWhenIsolationIsOff() throws IOException {
    ClusterConfig clusterConfig = createClusterConfig(false);
    Map<Integer, CliqueSpec> specs = allHealthy();
    specs.put(3, CliqueSpec.healthy().withPartitionWeight(CLUSTER_OVERFLOWING_PARTITION_WEIGHT));

    try {
      createAlgorithm().calculate(createClusterModel(clusterConfig, specs));
      Assert.fail("The default mode must still fail the whole cluster");
    } catch (HelixRebalanceException e) {
      Assert.assertEquals(e.getFailureCategory(),
          HelixRebalanceException.FailureCategory.CAPACITY_DEFICIT);
    }
  }

  /**
   * When every clique is oversubscribed there is nothing left to rebalance, so the original cluster
   * wide capacity deficit is reported unchanged instead of an empty assignment being returned.
   */
  @Test
  public void testClusterWideCapacityDeficitAcrossEveryCliqueStillFails() throws IOException {
    ClusterConfig clusterConfig = createClusterConfig(true);
    Map<Integer, CliqueSpec> specs = new HashMap<>();
    for (int clique = 0; clique < CLIQUE_COUNT; clique++) {
      specs.put(clique,
          CliqueSpec.healthy().withPartitionWeight(CLUSTER_OVERFLOWING_PARTITION_WEIGHT));
    }

    try {
      createAlgorithm().calculate(createClusterModel(clusterConfig, specs));
      Assert.fail("A cluster with no healthy clique left must fail");
    } catch (HelixRebalanceException e) {
      Assert.assertEquals(e.getFailureCategory(),
          HelixRebalanceException.FailureCategory.CAPACITY_DEFICIT);
    }
  }

  /**
   * The attribution only runs on the path that already throws, so a cluster that is not in deficit
   * must produce the very same assignment whether the flag is on or off.
   */
  @Test
  public void testCapacityDeficitAttributionDoesNotDisturbAHealthyCluster()
      throws HelixRebalanceException, IOException {
    Map<Integer, CliqueSpec> specs = allHealthy();
    Map<String, ResourceAssignment> withFlagOff =
        createAlgorithm().calculate(createClusterModel(createClusterConfig(false), specs))
            .getOptimalResourceAssignment();
    Map<String, ResourceAssignment> withFlagOn =
        createAlgorithm().calculate(createClusterModel(createClusterConfig(true), specs))
            .getOptimalResourceAssignment();

    Assert.assertEquals(normalize(withFlagOn), normalize(withFlagOff));
  }

  /**
   * The attributed path must be as reproducible as the ordinary one, because the baseline it feeds
   * is persisted and compared across rebalances.
   */
  @Test
  public void testCapacityDeficitAttributionIsDeterministic()
      throws HelixRebalanceException, IOException {
    ClusterConfig clusterConfig = createClusterConfig(true);
    Map<Integer, CliqueSpec> specs = allHealthy();
    specs.put(11, CliqueSpec.healthy().withPartitionWeight(CLUSTER_OVERFLOWING_PARTITION_WEIGHT));

    Map<String, Map<String, Map<String, String>>> first = null;
    for (int run = 0; run < 5; run++) {
      Map<String, Map<String, Map<String, String>>> current = normalize(
          createAlgorithm().calculate(createClusterModel(clusterConfig, specs))
              .getOptimalResourceAssignment());
      if (first == null) {
        first = current;
      } else {
        Assert.assertEquals(current, first, "Run " + run + " diverged from the first run");
      }
    }
  }

  /**
   * Several cliques can overflow the cluster wide sum at once and all of them get attributed.
   */
  @Test
  public void testClusterWideCapacityDeficitFromSeveralCliquesIsAttributedToAllOfThem()
      throws HelixRebalanceException, IOException {
    Set<Integer> brokenCliques = new HashSet<>(Arrays.asList(1, 8, 15));
    ClusterConfig clusterConfig = createClusterConfig(true);
    Map<Integer, CliqueSpec> specs = allHealthy();
    brokenCliques.forEach(clique -> specs.put(clique,
        CliqueSpec.healthy().withPartitionWeight(CLUSTER_OVERFLOWING_PARTITION_WEIGHT)));

    OptimalAssignment optimalAssignment =
        createAlgorithm().calculate(createClusterModel(clusterConfig, specs));
    Map<String, ResourceAssignment> result = optimalAssignment.getOptimalResourceAssignment();

    Assert.assertEquals(new TreeSet<>(optimalAssignment.getSkippedResources()), new TreeSet<>(
        brokenCliques.stream().map(AbstractTestWagedInstanceTagIsolation::resourceName)
            .collect(Collectors.toSet())));
    Assert.assertEquals(result.size(), CLIQUE_COUNT - brokenCliques.size());
    for (int clique = 0; clique < CLIQUE_COUNT; clique++) {
      if (brokenCliques.contains(clique)) {
        continue;
      }
      Assert.assertEquals(result.get(resourceName(clique)).getMappedPartitions().size(),
          PARTITIONS_PER_RESOURCE);
    }
  }

  // --------------------------------------------------------------------------------------------
  // Observability parity: an operator must not need new dashboards to notice a frozen clique
  // --------------------------------------------------------------------------------------------

  /**
   * Isolation must not cost the operator any visibility. A clique that gets skipped still has to
   * feed the two existing WAGED hard constraint reporters exactly as it does when the whole
   * rebalance fails, so the alerting a cluster already has keeps firing with no customer change.
   */
  @Test
  public void testSkippedCliqueStillFeedsTheExistingHardConstraintReporters()
      throws HelixRebalanceException, IOException {
    int brokenClique = 3;
    ClusterConfig clusterConfig = createClusterConfig(true);
    Map<Integer, CliqueSpec> specs = allHealthy();
    specs.put(brokenClique,
        CliqueSpec.healthy().withPartitionWeight(UNPLACEABLE_PARTITION_WEIGHT));

    Set<HardConstraint.Type> cumulative = ConcurrentHashMap.newKeySet();
    Map<ClusterModel.RebalanceScopeType, Set<HardConstraint.Type>> snapshot = new HashMap<>();
    ConstraintBasedAlgorithm algorithm = (ConstraintBasedAlgorithm) createAlgorithm();
    algorithm.setHardConstraintFailureReporter(cumulative::add);
    algorithm.setBlockingSnapshotReporter(snapshot::put);

    OptimalAssignment optimalAssignment =
        algorithm.calculate(createClusterModel(clusterConfig, specs));

    // The rebalance succeeded for the other 19 cliques ...
    Assert.assertEquals(optimalAssignment.getSkippedResources(),
        Collections.singleton(resourceName(brokenClique)));
    Assert.assertEquals(optimalAssignment.getOptimalResourceAssignment().size(), CLIQUE_COUNT - 1);
    // ... and the operator still sees exactly why clique 3 could not be placed.
    Assert.assertTrue(cumulative.contains(HardConstraint.Type.NODE_CAPACITY),
        "The cumulative per type reporter must still fire for a skipped clique, got " + cumulative);
    Assert.assertEquals(snapshot.size(), 1, "The snapshot must be published exactly once per run");
    Assert.assertTrue(
        snapshot.get(ClusterModel.RebalanceScopeType.GLOBAL_BASELINE)
            .contains(HardConstraint.Type.NODE_CAPACITY),
        "The reversible blocking snapshot must still report the skipped clique, got " + snapshot);
  }

  /**
   * The flip side: a run where every clique is placeable must publish an empty snapshot, so the
   * "currently blocking" gauge falls back to zero once the broken clique is repaired.
   */
  @Test
  public void testCleanRunPublishesAnEmptyBlockingSnapshotWithIsolationOn()
      throws HelixRebalanceException, IOException {
    ClusterConfig clusterConfig = createClusterConfig(true);

    Set<HardConstraint.Type> cumulative = ConcurrentHashMap.newKeySet();
    Map<ClusterModel.RebalanceScopeType, Set<HardConstraint.Type>> snapshot = new HashMap<>();
    ConstraintBasedAlgorithm algorithm = (ConstraintBasedAlgorithm) createAlgorithm();
    algorithm.setHardConstraintFailureReporter(cumulative::add);
    algorithm.setBlockingSnapshotReporter(snapshot::put);

    OptimalAssignment optimalAssignment =
        algorithm.calculate(createClusterModel(clusterConfig, allHealthy()));

    Assert.assertTrue(optimalAssignment.getSkippedResources().isEmpty());
    Assert.assertTrue(cumulative.isEmpty(), "A clean run must not report any blocking type");
    Assert.assertEquals(snapshot.size(), 1);
    Assert.assertTrue(snapshot.get(ClusterModel.RebalanceScopeType.GLOBAL_BASELINE).isEmpty(),
        "A clean run must publish an empty snapshot so the gauge resets");
  }

  /**
   * The same reporters must behave identically with the flag off, which is what proves isolation
   * did not quietly change the metric contract.
   */
  @Test
  public void testReportersSeeTheSameBlockingTypeWhenIsolationIsOff()
      throws IOException {
    int brokenClique = 3;
    ClusterConfig clusterConfig = createClusterConfig(false);
    Map<Integer, CliqueSpec> specs = allHealthy();
    specs.put(brokenClique,
        CliqueSpec.healthy().withPartitionWeight(UNPLACEABLE_PARTITION_WEIGHT));

    Set<HardConstraint.Type> cumulative = ConcurrentHashMap.newKeySet();
    Map<ClusterModel.RebalanceScopeType, Set<HardConstraint.Type>> snapshot = new HashMap<>();
    ConstraintBasedAlgorithm algorithm = (ConstraintBasedAlgorithm) createAlgorithm();
    algorithm.setHardConstraintFailureReporter(cumulative::add);
    algorithm.setBlockingSnapshotReporter(snapshot::put);

    try {
      algorithm.calculate(createClusterModel(clusterConfig, specs));
      Assert.fail("The default global mode must still fail the whole rebalance");
    } catch (HelixRebalanceException expected) {
      Assert.assertEquals(expected.getFailureCategory(),
          HelixRebalanceException.FailureCategory.NO_CANDIDATE_NODE);
    }

    Assert.assertTrue(cumulative.contains(HardConstraint.Type.NODE_CAPACITY));
    Assert.assertEquals(snapshot.size(), 1, "The snapshot must be published even when the run throws");
    Assert.assertTrue(snapshot.get(ClusterModel.RebalanceScopeType.GLOBAL_BASELINE)
        .contains(HardConstraint.Type.NODE_CAPACITY));
  }
}
