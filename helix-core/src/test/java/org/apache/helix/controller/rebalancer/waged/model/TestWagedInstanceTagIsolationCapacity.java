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
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.stream.Collectors;

import org.apache.helix.HelixRebalanceException;
import org.apache.helix.constants.InstanceConstants;
import org.apache.helix.controller.rebalancer.waged.constraints.ConstraintBasedAlgorithm;
import org.apache.helix.controller.rebalancer.waged.constraints.HardConstraint;
import org.apache.helix.model.ClusterConfig;
import org.apache.helix.model.InstanceConfig;
import org.apache.helix.model.Partition;
import org.apache.helix.model.ResourceAssignment;
import org.apache.helix.model.ResourceConfig;
import org.testng.Assert;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

/**
 * Capacity-facing behavior of WAGED instance-tag isolation: skipping the clique that causes a
 * cluster wide capacity deficit. See
 * {@link AbstractTestWagedInstanceTagIsolation} for the shared clique partitioned topology.
 */
public class TestWagedInstanceTagIsolationCapacity extends AbstractTestWagedInstanceTagIsolation {
  // ---------------------------------------------------------------------------------------------
  // Skipped resources that are already partially placed on the nodes (PARTIAL/EMERGENCY scopes)
  // ---------------------------------------------------------------------------------------------

  /**
   * The capacity check counts every replica, but the algorithm gets only the replicas still to
   * assign. A clique whose replicas are all placed must still be skipped for the deficit, or every
   * healthy clique stays frozen behind it.
   */
  @Test
  public void testCliqueOverCapacityIsSkippedWhenAlreadyPlaced()
      throws HelixRebalanceException, IOException {
    ClusterConfig clusterConfig = createClusterConfig(true);

    // Clique 0 is far over capacity: five replicas of weight 150 already sitting on three
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
      AssignableNode node = taggedNode(clusterConfig, instanceName(0, i), cliqueTag(0));
      brokenNodes.add(node);
      nodes.add(node);
      nodes.add(taggedNode(clusterConfig, instanceName(1, i), cliqueTag(1)));
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
    Assert.assertEquals(assignment.getSkippedResources(), Collections.singleton("R_broken"),
        "The skipped resource is reported even when all its replicas are already placed, so it "
            + "is carried forward");
  }

  // ---------------------------------------------------------------------------------------------
  // Cliques that cause a cluster wide capacity deficit
  // ---------------------------------------------------------------------------------------------

  /**
   * A weight big enough that clique 3 alone drags the tag blind cluster wide capacity sum negative.
   * Cluster capacity is 20 x 10 x 100 = 20000 and the healthy demand is 20 x 10 x 10 = 2000, so a
   * single clique needs more than 18000 to overflow the whole cluster. 10 partitions x 2500 = 25000
   * does it.
   */
  private static final int CLUSTER_OVERFLOWING_PARTITION_WEIGHT = 2500;

  /**
   * The capacity check ignores tags, so one clique far over capacity pushes it below zero. Only
   * that clique is skipped, and the other 19 still rebalance.
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
   * The same topology with the flag off fails the whole cluster up front, and reports it as a
   * capacity deficit rather than as a missing candidate node.
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
   * Clique 0 alone asks for more than the cluster holds and is skipped, and cliques 1 and 2 then
   * fail in the placement pass, so no block is left. The rebalance must fail
   * with the capacity deficit the default mode throws for the same cluster (same category, type
   * and message), not with the placement failure of clique 1 or 2. WagedRebalancer keeps a
   * failure counter per category, so the category is visible in its metrics.
   */
  @Test
  public void testCapacityDeficitIsRethrownWhenEveryBlockLeftFails()
      throws IOException {
    // Cliques 1 and 2 ask for less than their nodes hold in total, so they are not skipped, but
    // each of their partitions is heavier than any single node.
    Map<Integer, CliqueSpec> specs = new HashMap<>();
    specs.put(0, CliqueSpec.healthy().withPartitionWeight(CLUSTER_OVERFLOWING_PARTITION_WEIGHT));
    for (int clique = 1; clique < 3; clique++) {
      specs.put(clique, CliqueSpec.healthy().withPartitionWeight(UNPLACEABLE_PARTITION_WEIGHT)
          .withPartitionCount(6));
    }

    HelixRebalanceException defaultFailure = null;
    try {
      createAlgorithm().calculate(createClusterModel(createClusterConfig(false), specs));
    } catch (HelixRebalanceException e) {
      defaultFailure = e;
    }
    Assert.assertNotNull(defaultFailure, "Precondition: the default mode must fail");
    Assert.assertEquals(defaultFailure.getFailureCategory(),
        HelixRebalanceException.FailureCategory.CAPACITY_DEFICIT,
        "Precondition: the default mode fails on the cluster wide capacity deficit");

    Set<HardConstraint.Type> blocking = ConcurrentHashMap.newKeySet();
    ConstraintBasedAlgorithm algorithm = (ConstraintBasedAlgorithm) createAlgorithm();
    algorithm.setHardConstraintFailureReporter(blocking::add);
    try {
      algorithm.calculate(createClusterModel(createClusterConfig(true), specs));
      Assert.fail("No block survived, so the rebalance must fail");
    } catch (HelixRebalanceException e) {
      Assert.assertEquals(e.getFailureCategory(),
          HelixRebalanceException.FailureCategory.CAPACITY_DEFICIT,
          "The rethrown failure must be the capacity deficit, not a placement failure: " + e);
      Assert.assertEquals(e.getFailureType(), defaultFailure.getFailureType());
      Assert.assertEquals(e.getMessage(), defaultFailure.getMessage());
    }
    Assert.assertTrue(blocking.contains(HardConstraint.Type.NODE_CAPACITY),
        "Precondition: clique 0 was skipped and cliques 1 and 2 failed in the placement pass, "
            + "rather than the deficit being thrown before it, got " + blocking);
  }

  /**
   * Skipping must be as reproducible as the ordinary path, because the baseline it feeds is
   * persisted and compared across rebalances.
   */
  @Test
  public void testCapacityDeficitSkipIsDeterministic()
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
   * A node of clique 0 holds one replica each of cliques 1 and 2, with room for one. The order the
   * node returns them changes from build to build, so every build must take them in replica order
   * and skip cliques 2 and 3.
   */
  @Test
  public void testChargingReplicasLeftOnAnotherCliquesNodeDoesNotDependOnHashOrder()
      throws HelixRebalanceException, IOException {
    // Enough builds that all of them returning the replicas in replica order by chance is
    // negligible.
    int builds = 60;
    Map<String, Integer> results = new TreeMap<>();
    for (int build = 0; build < builds; build++) {
      OptimalAssignment assignment =
          createAlgorithm().calculate(fullNodeHoldingTwoForeignReplicas());
      results.merge(new TreeSet<>(assignment.getSkippedResources()).toString(), 1, Integer::sum);
    }
    String expected = new TreeSet<>(Arrays.asList(resourceName(2), resourceName(3))).toString();
    Assert.assertEquals(results, Collections.singletonMap(expected, builds),
        "Builds per set of skipped resources");
  }

  /**
   * Clique 0 has two nodes and one outstanding partition of 10, and cliques 1 to 3 have one node
   * each, all of capacity 100. Cliques 1 and 2 each have a resource of two partitions of 60: one
   * sits on the clique's own node and the other on instance_0_1. Clique 3 has three outstanding
   * partitions of 100, so the tag blind sum is 50 short.
   */
  private ClusterModel fullNodeHoldingTwoForeignReplicas() throws IOException {
    ClusterConfig clusterConfig = createClusterConfig(true);
    Set<AssignableNode> nodes = new HashSet<>();
    nodes.add(taggedNode(clusterConfig, instanceName(0, 0), cliqueTag(0)));
    AssignableNode shared = taggedNode(clusterConfig, instanceName(0, 1), cliqueTag(0));
    nodes.add(shared);
    nodes.add(taggedNode(clusterConfig, instanceName(3, 0), cliqueTag(3)));

    Set<AssignableReplica> toBeAssigned = new HashSet<>();
    addReplicas(toBeAssigned, clusterConfig,
        taggedResource(resourceName(0), cliqueTag(0), HEALTHY_PARTITION_WEIGHT), 1);
    addReplicas(toBeAssigned, clusterConfig,
        taggedResource(resourceName(3), cliqueTag(3), NODE_CAPACITY), 3);
    Set<AssignableReplica> everything = new HashSet<>(toBeAssigned);
    Set<AssignableReplica> leftOnShared = new HashSet<>();
    for (int clique = 1; clique < 3; clique++) {
      ResourceConfig resource = taggedResource(resourceName(clique), cliqueTag(clique), 60);
      AssignableReplica away = new AssignableReplica(clusterConfig, resource,
          resourceName(clique) + "_0", "ONLINE", 0);
      AssignableReplica home = new AssignableReplica(clusterConfig, resource,
          resourceName(clique) + "_1", "ONLINE", 0);
      AssignableNode own =
          taggedNode(clusterConfig, instanceName(clique, 0), cliqueTag(clique));
      own.assignInitBatch(Collections.singleton(home));
      nodes.add(own);
      leftOnShared.add(away);
      everything.add(away);
      everything.add(home);
    }
    shared.assignInitBatch(leftOnShared);

    ClusterContext context = new ClusterContext(everything, nodes, Collections.emptyMap(),
        Collections.emptyMap(), clusterConfig);
    Assert.assertTrue(context.getEstimateUtilizationMap().get(CAPACITY_KEY) < 0,
        "Precondition: the tag blind cluster wide capacity check must be in deficit");
    return new ClusterModel(context, toBeAssigned, nodes, ClusterModel.RebalanceScopeType.PARTIAL);
  }

  /**
   * Several cliques can push the totals below zero at once, and all of them are skipped.
   */
  @Test
  public void testEveryCliqueOverCapacityIsSkipped()
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

  private static final String ZONE = "zone_a";

  /**
   * The tags of a spare node no resource is pinned to: a tag of its own, none at all, or only a
   * zone label every clique node carries too. The last two leave the node in no group, so it gets
   * a block of its own, and the label must not join the cliques it spans into one block.
   */
  @DataProvider(name = "spareTags")
  public Object[][] spareTags() {
    return new Object[][] {{new String[] {"spare"}}, {new String[0]}, {new String[] {ZONE}}};
  }

  /**
   * Stale replicas on a spare node push the totals below zero while each clique fits on its own
   * nodes. Only the spare node's block is over, so nothing is skipped, while the default mode
   * throws.
   */
  @Test(dataProvider = "spareTags")
  public void testStaleReplicasOnASpareNodeAreChargedToThatNode(String[] spareTags)
      throws HelixRebalanceException, IOException {
    assertCapacityDeficit(staleReplicasOnASpareNode(createClusterConfig(false), true, spareTags));

    OptimalAssignment assignment = createAlgorithm().calculate(
        staleReplicasOnASpareNode(createClusterConfig(true), true, spareTags));
    Assert.assertEquals(assignment.getSkippedResources(), Collections.emptySet());
    Map<String, ResourceAssignment> result = assignment.getOptimalResourceAssignment();
    Set<String> cliqueZero = new HashSet<>(Arrays.asList(instanceName(0, 0), instanceName(0, 1)));
    Set<String> cliqueOne = new HashSet<>(Arrays.asList(instanceName(1, 0), instanceName(1, 1)));
    for (int p = 0; p < 6; p++) {
      Set<String> holders = result.get("RA").getReplicaMap(new Partition("RA_" + p)).keySet();
      Assert.assertEquals(holders.size(), 1, "RA_" + p);
      Assert.assertTrue(p < 4 ? holders.contains("spare_0") : cliqueZero.containsAll(holders),
          "RA_" + p + " on " + holders);
    }
    for (int p = 0; p < 4; p++) {
      Set<String> holders = result.get("RB").getReplicaMap(new Partition("RB_" + p)).keySet();
      Assert.assertEquals(holders.size(), 1, "RB_" + p);
      Assert.assertTrue(cliqueOne.containsAll(holders), "RB_" + p + " on " + holders);
    }
  }

  /**
   * The same stale spare node next to clique 0 alone. Skipping the spare node needs a second block
   * with resources left to rebalance, so the default mode's result stands.
   */
  @Test(dataProvider = "spareTags")
  public void testStaleReplicasOnASpareNodeNextToASingleCliqueThrowsLikeTheDefaultMode(
      String[] spareTags) throws IOException {
    assertCapacityDeficit(staleReplicasOnASpareNode(createClusterConfig(false), false, spareTags));
    assertCapacityDeficit(staleReplicasOnASpareNode(createClusterConfig(true), false, spareTags));
  }

  /**
   * RA on clique 0 has six partitions of 60: four still sit on a spare node (240 on a node of 100)
   * and two are outstanding. With clique 1, RB has four outstanding partitions of 50, which fill
   * its two nodes exactly. The tag blind sum is 60 short either way. Every clique node also carries
   * {@link #ZONE}.
   */
  private ClusterModel staleReplicasOnASpareNode(ClusterConfig clusterConfig,
      boolean withCliqueOne, String... spareTags) throws IOException {
    ResourceConfig ra = taggedResource("RA", cliqueTag(0), 60);
    Set<AssignableReplica> stale = new HashSet<>();
    Set<AssignableReplica> toBeAssigned = new HashSet<>();
    for (int p = 0; p < 6; p++) {
      (p < 4 ? stale : toBeAssigned)
          .add(new AssignableReplica(clusterConfig, ra, "RA_" + p, "ONLINE", 0));
    }
    Set<AssignableNode> nodes = new HashSet<>();
    for (int i = 0; i < 2; i++) {
      nodes.add(taggedNode(clusterConfig, instanceName(0, i), cliqueTag(0), ZONE));
    }
    if (withCliqueOne) {
      addReplicas(toBeAssigned, clusterConfig, taggedResource("RB", cliqueTag(1), 50), 4);
      for (int i = 0; i < 2; i++) {
        nodes.add(taggedNode(clusterConfig, instanceName(1, i), cliqueTag(1), ZONE));
      }
    }
    AssignableNode spare = taggedNode(clusterConfig, "spare_0", spareTags);
    spare.assignInitBatch(stale);
    nodes.add(spare);
    Set<AssignableReplica> everything = new HashSet<>(toBeAssigned);
    everything.addAll(stale);
    ClusterContext context = new ClusterContext(everything, nodes, Collections.emptyMap(),
        Collections.emptyMap(), clusterConfig);
    Assert.assertTrue(context.getEstimateUtilizationMap().get(CAPACITY_KEY) < 0,
        "Precondition: the tag blind cluster wide capacity check must be in deficit");
    return new ClusterModel(context, toBeAssigned, nodes, ClusterModel.RebalanceScopeType.PARTIAL);
  }

  private static final String SECOND_CAPACITY_KEY = "CPU";

  /**
   * Skipping clique 0 leaves the rest with no CPU. CPU keeps its scoring floor, so clique 1 is
   * still placed heaviest first by DISK, as in a cluster with no deficit.
   */
  @Test
  public void testResidualScoringKeepsItsFloorWithTwoCapacityKeys()
      throws HelixRebalanceException, IOException {
    ClusterConfig clusterConfig = createClusterConfig(true);
    clusterConfig.setInstanceCapacityKeys(Arrays.asList(CAPACITY_KEY, SECOND_CAPACITY_KEY));
    Map<String, Integer> defaultWeight = new HashMap<>();
    defaultWeight.put(CAPACITY_KEY, 0);
    defaultWeight.put(SECOND_CAPACITY_KEY, 0);
    clusterConfig.setDefaultPartitionWeightMap(defaultWeight);

    Set<AssignableNode> nodes = new HashSet<>();
    for (int i = 0; i < 2; i++) {
      nodes.add(twoKeyNode(clusterConfig, instanceName(0, i), cliqueTag(0), 100));
      nodes.add(twoKeyNode(clusterConfig, instanceName(1, i), cliqueTag(1), 0));
    }
    Set<AssignableReplica> replicas = new HashSet<>();
    // RA asks for more DISK than any node holds, and for some of clique 0's CPU.
    ResourceConfig ra = taggedResource("RA", cliqueTag(0), UNPLACEABLE_PARTITION_WEIGHT);
    Map<String, Integer> raWeight = new HashMap<>();
    raWeight.put(CAPACITY_KEY, UNPLACEABLE_PARTITION_WEIGHT);
    raWeight.put(SECOND_CAPACITY_KEY, 10);
    ra.setPartitionCapacityMap(
        Collections.singletonMap(ResourceConfig.DEFAULT_PARTITION_KEY, raWeight));
    addReplicas(replicas, clusterConfig, ra, 3);
    // Eight single partition resources on clique 1, each with its own DISK weight, named so that
    // neither the name order nor the index order is the weight order.
    int[] weights = {15, 40, 5, 30, 20, 35, 10, 25};
    Map<String, Integer> weightByPartition = new HashMap<>();
    for (int r = 0; r < weights.length; r++) {
      ResourceConfig rb = taggedResource("RB" + r, cliqueTag(1), weights[r]);
      addReplicas(replicas, clusterConfig, rb, 1);
      weightByPartition.put(rb.getResourceName() + "_0", weights[r]);
    }
    ClusterContext context = new ClusterContext(replicas, nodes, Collections.emptyMap(),
        Collections.emptyMap(), clusterConfig);
    Assert.assertTrue(context.getEstimateUtilizationMap().get(CAPACITY_KEY) < 0,
        "Precondition: the tag blind cluster wide capacity check must be in deficit");
    PlacementOrderModel model = new PlacementOrderModel(context, replicas, nodes);

    OptimalAssignment assignment = createAlgorithm().calculate(model);

    Assert.assertEquals(assignment.getSkippedResources(), Collections.singleton("RA"));
    Map<String, ResourceAssignment> result = assignment.getOptimalResourceAssignment();
    for (int r = 0; r < weights.length; r++) {
      Assert.assertEquals(result.get("RB" + r).getMappedPartitions().size(), 1, "RB" + r);
    }
    List<String> heaviestFirst = weightByPartition.entrySet().stream()
        .sorted(Map.Entry.<String, Integer>comparingByValue().reversed()).map(Map.Entry::getKey)
        .collect(Collectors.toList());
    Assert.assertEquals(model.placedPartitions(), heaviestFirst,
        "Clique 1 must be placed heaviest first, by its DISK weight");
  }

  /**
   * Clique 0 holds 200 and is asked for 120, one replica at 60 and six at 10. Clique 1 holds 100
   * and is asked for 300. Once clique 1 is skipped, clique 0 must be placed as in a cluster of its
   * own, not scored against the 420 asked of the cluster's 300.
   */
  @Test
  public void testCliquesLeftAfterADeficitAreScoredAsAClusterOfTheirOwn()
      throws HelixRebalanceException, IOException {
    OptimalAssignment withDeficit = createAlgorithm().calculate(survivorScoring(true, true));
    Assert.assertEquals(withDeficit.getSkippedResources(), Collections.singleton("RF"));
    Map<String, Map<String, Map<String, String>>> placed =
        normalize(withDeficit.getOptimalResourceAssignment());
    placed.remove("RF");
    Assert.assertEquals(placed, normalize(
        createAlgorithm().calculate(survivorScoring(false, false)).getOptimalResourceAssignment()));
  }

  private ClusterModel survivorScoring(boolean enabled, boolean withDeficit) throws IOException {
    ClusterConfig clusterConfig = createClusterConfig(enabled);
    Set<AssignableNode> nodes = new HashSet<>();
    nodes.add(taggedNode(clusterConfig, instanceName(0, 0), cliqueTag(0)));
    nodes.add(taggedNode(clusterConfig, instanceName(0, 1), cliqueTag(0)));
    Set<AssignableReplica> replicas = new HashSet<>();
    addReplicas(replicas, clusterConfig, taggedResource("RS_big", cliqueTag(0), 60), 1);
    addReplicas(replicas, clusterConfig,
        taggedResource("RS_small", cliqueTag(0), HEALTHY_PARTITION_WEIGHT), 6);
    if (withDeficit) {
      nodes.add(taggedNode(clusterConfig, instanceName(1, 0), cliqueTag(1)));
      addReplicas(replicas, clusterConfig, taggedResource("RF", cliqueTag(1), NODE_CAPACITY), 3);
    }
    ClusterContext context = new ClusterContext(replicas, nodes, Collections.emptyMap(),
        Collections.emptyMap(), clusterConfig);
    return new ClusterModel(context, replicas, nodes,
        ClusterModel.RebalanceScopeType.GLOBAL_BASELINE);
  }

  private static AssignableNode twoKeyNode(ClusterConfig clusterConfig, String instance,
      String tag, int secondKeyCapacity) {
    InstanceConfig instanceConfig = new InstanceConfig(instance);
    Map<String, Integer> capacity = new HashMap<>();
    capacity.put(CAPACITY_KEY, NODE_CAPACITY);
    capacity.put(SECOND_CAPACITY_KEY, secondKeyCapacity);
    instanceConfig.setInstanceCapacityMap(capacity);
    instanceConfig.addTag(tag);
    instanceConfig.setInstanceOperation(InstanceConstants.InstanceOperation.ENABLE);
    return new AssignableNode(clusterConfig, instanceConfig, instance);
  }

  /** A baseline model that records the partitions the algorithm assigns, in the order it does. */
  private static final class PlacementOrderModel extends ClusterModel {
    private final List<String> _placedPartitions = new ArrayList<>();

    PlacementOrderModel(ClusterContext context, Set<AssignableReplica> replicas,
        Set<AssignableNode> nodes) {
      super(context, replicas, nodes, RebalanceScopeType.GLOBAL_BASELINE);
    }

    @Override
    public void assign(String resourceName, String partitionName, String state,
        String instanceName) {
      _placedPartitions.add(partitionName);
      super.assign(resourceName, partitionName, state, instanceName);
    }

    List<String> placedPartitions() {
      return _placedPartitions;
    }
  }
}
