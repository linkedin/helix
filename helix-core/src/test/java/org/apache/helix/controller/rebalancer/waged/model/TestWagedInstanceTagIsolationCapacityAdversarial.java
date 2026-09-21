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
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;

import org.apache.helix.HelixException;
import org.apache.helix.HelixRebalanceException;
import org.apache.helix.constants.InstanceConstants;
import org.apache.helix.model.ClusterConfig;
import org.apache.helix.model.InstanceConfig;
import org.apache.helix.model.ResourceAssignment;
import org.apache.helix.model.ResourceConfig;
import org.testng.Assert;
import org.testng.annotations.Test;

/**
 * Adversarial attacks on the cluster wide capacity deficit attribution path only, that is
 * {@code InstanceTagIsolation.absorbCapacityDeficit(...)} and its interaction with
 * {@code ConstraintBasedAlgorithm.computeScoringCapacities(...)}. The collision cascade and the
 * convergence paths are covered elsewhere.
 *
 * These tests deliberately try to break the attribution: multiple capacity dimensions, tags that
 * no live instance carries, over assigned nodes, nodes bridging two cliques, several untagged
 * groups, and hand built models that violate the "already placed replicas are not also outstanding"
 * invariant the arithmetic leans on. Where a case needs a cluster model the real
 * {@code ClusterModelProvider} could never build, the test name and comment call it out as
 * theoretical.
 */
public class TestWagedInstanceTagIsolationCapacityAdversarial
    extends AbstractTestWagedInstanceTagIsolation {

  private static final String DISK = "DISK";
  private static final String CPU = "CPU";

  // ---------------------------------------------------------------------------------------------
  // Two dimension helpers (the shared fixture is single dimension DISK only).
  // ---------------------------------------------------------------------------------------------

  private ClusterConfig twoDimConfig(boolean isolationEnabled) {
    ClusterConfig clusterConfig = new ClusterConfig("TwoDimCliqueCluster");
    clusterConfig.setInstanceCapacityKeys(Arrays.asList(DISK, CPU));
    Map<String, Integer> zero = new HashMap<>();
    zero.put(DISK, 0);
    zero.put(CPU, 0);
    clusterConfig.setDefaultPartitionWeightMap(zero);
    clusterConfig.setWagedInstanceTagIsolationEnabled(isolationEnabled);
    return clusterConfig;
  }

  private AssignableNode twoDimNode(ClusterConfig clusterConfig, String instance, int disk, int cpu,
      String... tags) {
    InstanceConfig instanceConfig = new InstanceConfig(instance);
    Map<String, Integer> capacity = new HashMap<>();
    capacity.put(DISK, disk);
    capacity.put(CPU, cpu);
    instanceConfig.setInstanceCapacityMap(capacity);
    for (String tag : tags) {
      instanceConfig.addTag(tag);
    }
    instanceConfig.setInstanceOperation(InstanceConstants.InstanceOperation.ENABLE);
    return new AssignableNode(clusterConfig, instanceConfig, instance);
  }

  private ResourceConfig twoDimResource(String resource, String tag, int disk, int cpu)
      throws IOException {
    ResourceConfig resourceConfig = new ResourceConfig(resource);
    if (tag != null) {
      resourceConfig.getRecord()
          .setSimpleField(ResourceConfig.ResourceConfigProperty.INSTANCE_GROUP_TAG.name(), tag);
    }
    Map<String, Integer> weight = new HashMap<>();
    weight.put(DISK, disk);
    weight.put(CPU, cpu);
    resourceConfig.setPartitionCapacityMap(
        Collections.singletonMap(ResourceConfig.DEFAULT_PARTITION_KEY, weight));
    return resourceConfig;
  }

  private void addN(Set<AssignableReplica> set, ClusterConfig clusterConfig,
      ResourceConfig resourceConfig, int count) {
    for (int p = 0; p < count; p++) {
      set.add(new AssignableReplica(clusterConfig, resourceConfig,
          resourceConfig.getResourceName() + "_" + p, "ONLINE", 0));
    }
  }

  private ClusterModel globalModel(ClusterConfig clusterConfig, Set<AssignableReplica> replicas,
      Set<AssignableNode> nodes) {
    ClusterContext context = new ClusterContext(replicas, nodes, Collections.emptyMap(),
        Collections.emptyMap(), clusterConfig);
    return new ClusterModel(context, replicas, nodes,
        ClusterModel.RebalanceScopeType.GLOBAL_BASELINE);
  }

  // =============================================================================================
  // H10  MULTI DIMENSION
  // =============================================================================================

  /**
   * Baseline multi dimension case: two capacity keys, one clique drags the tag blind cluster wide
   * DISK sum negative while CPU stays positive. The guilty clique must be blamed and the four
   * healthy cliques must still be placed on both dimensions.
   */
  @Test
  public void testMultiDimDeficitOnOneDimensionAttributedToOneClique()
      throws HelixRebalanceException, IOException {
    ClusterConfig clusterConfig = twoDimConfig(true);
    Set<AssignableReplica> replicas = new HashSet<>();
    Set<AssignableNode> nodes = new HashSet<>();

    // Guilty clique g: heavy on DISK, trivial on CPU.
    for (int i = 0; i < 3; i++) {
      nodes.add(twoDimNode(clusterConfig, "g_" + i, 100, 100, "g"));
    }
    addN(replicas, clusterConfig, twoDimResource("R_g", "g", 400, 1), 10);

    // Four healthy cliques, comfortable on both dimensions.
    for (int c = 0; c < 4; c++) {
      for (int i = 0; i < 3; i++) {
        nodes.add(twoDimNode(clusterConfig, "h" + c + "_" + i, 100, 100, "h" + c));
      }
      addN(replicas, clusterConfig, twoDimResource("R_h" + c, "h" + c, 10, 10), 10);
    }

    ClusterModel model = globalModel(clusterConfig, replicas, nodes);
    Assert.assertTrue(model.getContext().getEstimateUtilizationMap().get(DISK) < 0,
        "Precondition: cluster wide DISK must be in deficit");
    Assert.assertTrue(model.getContext().getEstimateUtilizationMap().get(CPU) > 0,
        "Precondition: cluster wide CPU must be comfortably positive");

    OptimalAssignment assignment = createAlgorithm().calculate(model);
    Map<String, ResourceAssignment> result = assignment.getOptimalResourceAssignment();

    Assert.assertEquals(assignment.getSkippedResources(), Collections.singleton("R_g"),
        "Only the DISK guilty clique may be blamed");
    for (int c = 0; c < 4; c++) {
      Assert.assertEquals(result.get("R_h" + c).getMappedPartitions().size(), 10,
          "Healthy clique h" + c + " must be fully placed on both dimensions");
    }
  }

  /**
   * Cross over commit: clique X is over on DISK but has spare CPU, clique Y is the reverse, and both
   * cluster wide sums go negative. Both guilty cliques must be blamed and the two healthy cliques
   * must survive. A bug that lets one dimension's set aside pull the other dimension negative would
   * throw here instead.
   */
  @Test
  public void testMultiDimCrossOverCommitBlamesBothGuiltyCliques()
      throws HelixRebalanceException, IOException {
    ClusterConfig clusterConfig = twoDimConfig(true);
    Set<AssignableReplica> replicas = new HashSet<>();
    Set<AssignableNode> nodes = new HashSet<>();

    // X: lots of CPU, little DISK; its resource is DISK heavy so X is DISK over committed.
    for (int i = 0; i < 3; i++) {
      nodes.add(twoDimNode(clusterConfig, "x_" + i, 100, 1000, "x"));
    }
    addN(replicas, clusterConfig, twoDimResource("R_x", "x", 400, 1), 10);

    // Y: lots of DISK, little CPU; its resource is CPU heavy so Y is CPU over committed.
    for (int i = 0; i < 3; i++) {
      nodes.add(twoDimNode(clusterConfig, "y_" + i, 1000, 100, "y"));
    }
    addN(replicas, clusterConfig, twoDimResource("R_y", "y", 1, 400), 10);

    // Two healthy cliques.
    for (int c = 0; c < 2; c++) {
      for (int i = 0; i < 3; i++) {
        nodes.add(twoDimNode(clusterConfig, "h" + c + "_" + i, 100, 100, "h" + c));
      }
      addN(replicas, clusterConfig, twoDimResource("R_h" + c, "h" + c, 10, 10), 10);
    }

    ClusterModel model = globalModel(clusterConfig, replicas, nodes);
    Assert.assertTrue(model.getContext().getEstimateUtilizationMap().get(DISK) < 0,
        "Precondition: cluster wide DISK deficit");
    Assert.assertTrue(model.getContext().getEstimateUtilizationMap().get(CPU) < 0,
        "Precondition: cluster wide CPU deficit");

    OptimalAssignment assignment = createAlgorithm().calculate(model);
    Map<String, ResourceAssignment> result = assignment.getOptimalResourceAssignment();

    Assert.assertEquals(new TreeSet<>(assignment.getSkippedResources()),
        new TreeSet<>(Arrays.asList("R_x", "R_y")),
        "Both cross over committed cliques must be blamed");
    Assert.assertEquals(result.get("R_h0").getMappedPartitions().size(), 10);
    Assert.assertEquals(result.get("R_h1").getMappedPartitions().size(), 10);
  }

  /**
   * Drives the residual capacity of one dimension to exactly zero after the guilty clique is set
   * aside: the only CPU capacity in the cluster belongs to the blamed clique, so the CPU residual
   * denominator collapses to zero. The divGuard flooring must keep the score finite (no NaN or
   * Infinity from a 0/0), and the healthy clique must still be placed.
   */
  @Test
  public void testMultiDimResidualCapacityZeroOnOneDimensionStaysFinite()
      throws HelixRebalanceException, IOException {
    ClusterConfig clusterConfig = twoDimConfig(true);
    Set<AssignableReplica> replicas = new HashSet<>();
    Set<AssignableNode> nodes = new HashSet<>();

    // Guilty clique g holds every unit of CPU capacity, and is DISK over committed.
    for (int i = 0; i < 3; i++) {
      nodes.add(twoDimNode(clusterConfig, "g_" + i, 100, 100, "g"));
    }
    addN(replicas, clusterConfig, twoDimResource("R_g", "g", 400, 5), 10);

    // Healthy clique h has zero CPU capacity and a CPU weightless resource, so after g is set aside
    // the CPU residual capacity is exactly zero.
    for (int i = 0; i < 3; i++) {
      nodes.add(twoDimNode(clusterConfig, "h_" + i, 100, 0, "h"));
    }
    addN(replicas, clusterConfig, twoDimResource("R_h", "h", 10, 0), 10);

    ClusterModel model = globalModel(clusterConfig, replicas, nodes);
    Assert.assertTrue(model.getContext().getEstimateUtilizationMap().get(DISK) < 0,
        "Precondition: cluster wide DISK deficit");

    OptimalAssignment assignment = createAlgorithm().calculate(model);
    Map<String, ResourceAssignment> result = assignment.getOptimalResourceAssignment();

    Assert.assertEquals(assignment.getSkippedResources(), Collections.singleton("R_g"));
    Assert.assertEquals(result.get("R_h").getMappedPartitions().size(), 10,
        "The healthy clique must still be placed even though the CPU residual denominator is zero");
  }

  /**
   * A genuine multi dimension cluster wide shortfall with every clique over committed on some
   * dimension must still fail as a capacity deficit. This exercises the
   * {@code deficitBlocks.size() == blocks.size()} early return under two dimensions.
   */
  @Test
  public void testMultiDimEveryCliqueOverCommittedStillThrows() throws IOException {
    ClusterConfig clusterConfig = twoDimConfig(true);
    Set<AssignableReplica> replicas = new HashSet<>();
    Set<AssignableNode> nodes = new HashSet<>();

    // Clique a over on DISK, clique b over on CPU.
    for (int i = 0; i < 3; i++) {
      nodes.add(twoDimNode(clusterConfig, "a_" + i, 100, 100, "a"));
      nodes.add(twoDimNode(clusterConfig, "b_" + i, 100, 100, "b"));
    }
    addN(replicas, clusterConfig, twoDimResource("R_a", "a", 400, 1), 10);
    addN(replicas, clusterConfig, twoDimResource("R_b", "b", 1, 400), 10);

    ClusterModel model = globalModel(clusterConfig, replicas, nodes);
    try {
      createAlgorithm().calculate(model);
      Assert.fail("Every clique is over committed, so the whole rebalance must still fail");
    } catch (HelixRebalanceException e) {
      Assert.assertEquals(e.getFailureCategory(),
          HelixRebalanceException.FailureCategory.CAPACITY_DEFICIT);
    }
  }

  // =============================================================================================
  // H12  TAG WITH NO LIVE INSTANCE
  // =============================================================================================

  /**
   * A resource pinned to a tag that no live instance carries reaches no node. When that phantom
   * clique is the sole cause of the cluster wide deficit, it must be the one blamed, not a healthy
   * clique, and every real clique must be placed.
   */
  @Test
  public void testPhantomTagResourceIsBlamedAsTheOnlyFault()
      throws HelixRebalanceException, IOException {
    ClusterConfig clusterConfig = createClusterConfig(true);
    Set<AssignableReplica> replicas = new HashSet<>();
    Set<AssignableNode> nodes = new HashSet<>();

    // R_ghost is pinned to a tag no node carries, and is heavy enough to sink the cluster wide sum.
    addReplicas(replicas, clusterConfig, taggedResource("R_ghost", "ghost", 300), 10);

    // Two real, healthy cliques.
    for (int c = 0; c < 2; c++) {
      for (int i = 0; i < 3; i++) {
        nodes.add(taggedNode(clusterConfig, instanceName(c, i), i, cliqueTag(c)));
      }
      addReplicas(replicas, clusterConfig, taggedResource("R_h" + c, cliqueTag(c),
          HEALTHY_PARTITION_WEIGHT), 10);
    }

    ClusterModel model = globalModel(clusterConfig, replicas, nodes);
    Assert.assertTrue(model.getContext().getEstimateUtilizationMap().get(CAPACITY_KEY) < 0,
        "Precondition: the phantom tag resource must drag the cluster wide sum negative");

    OptimalAssignment assignment = createAlgorithm().calculate(model);
    Map<String, ResourceAssignment> result = assignment.getOptimalResourceAssignment();

    Assert.assertEquals(assignment.getSkippedResources(), Collections.singleton("R_ghost"),
        "The unplaceable phantom tag group must be blamed, not a healthy clique");
    Assert.assertEquals(result.get("R_h0").getMappedPartitions().size(), 10);
    Assert.assertEquals(result.get("R_h1").getMappedPartitions().size(), 10);
    Assert.assertFalse(result.containsKey("R_ghost"));
  }

  /**
   * A phantom tag resource plus a genuinely over committed real clique: both must be blamed and the
   * remaining healthy clique must survive.
   */
  @Test
  public void testPhantomTagResourceCombinedWithARealDeficitElsewhere()
      throws HelixRebalanceException, IOException {
    ClusterConfig clusterConfig = createClusterConfig(true);
    Set<AssignableReplica> replicas = new HashSet<>();
    Set<AssignableNode> nodes = new HashSet<>();

    // Phantom tag resource, no node carries the tag.
    addReplicas(replicas, clusterConfig, taggedResource("R_ghost", "ghost", 300), 10);

    // Clique 0 is a real clique but wildly over committed on its own nodes.
    for (int i = 0; i < 3; i++) {
      nodes.add(taggedNode(clusterConfig, instanceName(0, i), i, cliqueTag(0)));
    }
    addReplicas(replicas, clusterConfig, taggedResource("R_broken", cliqueTag(0), 200), 10);

    // Clique 1 is healthy.
    for (int i = 0; i < 3; i++) {
      nodes.add(taggedNode(clusterConfig, instanceName(1, i), i, cliqueTag(1)));
    }
    addReplicas(replicas, clusterConfig,
        taggedResource("R_healthy", cliqueTag(1), HEALTHY_PARTITION_WEIGHT), 10);

    ClusterModel model = globalModel(clusterConfig, replicas, nodes);
    Assert.assertTrue(model.getContext().getEstimateUtilizationMap().get(CAPACITY_KEY) < 0);

    OptimalAssignment assignment = createAlgorithm().calculate(model);
    Map<String, ResourceAssignment> result = assignment.getOptimalResourceAssignment();

    Assert.assertEquals(new TreeSet<>(assignment.getSkippedResources()),
        new TreeSet<>(Arrays.asList("R_broken", "R_ghost")),
        "Both the phantom tag group and the genuinely over committed clique must be blamed");
    Assert.assertEquals(result.get("R_healthy").getMappedPartitions().size(), 10,
        "The healthy clique must still be placed");
  }

  /**
   * With isolation off the phantom tag resource must still fail the whole cluster as a capacity
   * deficit, so the behaviour above is strictly opt in.
   */
  @Test
  public void testPhantomTagResourceStillThrowsWithIsolationOff() throws IOException {
    ClusterConfig clusterConfig = createClusterConfig(false);
    Set<AssignableReplica> replicas = new HashSet<>();
    Set<AssignableNode> nodes = new HashSet<>();
    addReplicas(replicas, clusterConfig, taggedResource("R_ghost", "ghost", 300), 10);
    for (int c = 0; c < 2; c++) {
      for (int i = 0; i < 3; i++) {
        nodes.add(taggedNode(clusterConfig, instanceName(c, i), i, cliqueTag(c)));
      }
      addReplicas(replicas, clusterConfig, taggedResource("R_h" + c, cliqueTag(c),
          HEALTHY_PARTITION_WEIGHT), 10);
    }
    try {
      createAlgorithm().calculate(globalModel(clusterConfig, replicas, nodes));
      Assert.fail("The default mode must fail the whole cluster on the phantom tag deficit");
    } catch (HelixRebalanceException e) {
      Assert.assertEquals(e.getFailureCategory(),
          HelixRebalanceException.FailureCategory.CAPACITY_DEFICIT);
    }
  }

  // =============================================================================================
  // H EXTRA  over assigned node, node bridging two cliques, several untagged groups, early returns
  // =============================================================================================

  /**
   * An over assigned node (used capacity above its maximum, so remaining capacity is negative) must
   * report exactly what is on it, not an absurd demand, and must not spill blame onto a healthy
   * clique. Clique 0 has five partitions of weight 150 already sitting on three nodes of capacity
   * 100, so every node is negative on remaining capacity.
   */
  @Test
  public void testOverAssignedNodeDoesNotProduceAbsurdBlame()
      throws HelixRebalanceException, IOException {
    ClusterConfig clusterConfig = createClusterConfig(true);
    ResourceConfig brokenConfig = taggedResource("R_broken", cliqueTag(0), 150);

    Set<AssignableReplica> alreadyPlaced = new HashSet<>();
    addReplicas(alreadyPlaced, clusterConfig, brokenConfig, 5);

    Set<AssignableReplica> toBeAssigned = new HashSet<>();
    addReplicas(toBeAssigned, clusterConfig,
        taggedResource("R_healthy", cliqueTag(1), HEALTHY_PARTITION_WEIGHT), 3);

    Set<AssignableNode> nodes = new HashSet<>();
    List<AssignableNode> brokenNodes = new ArrayList<>();
    for (int i = 0; i < 3; i++) {
      AssignableNode broken = taggedNode(clusterConfig, instanceName(0, i), i, cliqueTag(0));
      brokenNodes.add(broken);
      nodes.add(broken);
      nodes.add(taggedNode(clusterConfig, instanceName(1, i), i, cliqueTag(1)));
    }
    int index = 0;
    for (AssignableReplica replica : alreadyPlaced) {
      brokenNodes.get(index++ % brokenNodes.size()).assignInitBatch(Collections.singleton(replica));
    }
    // Precondition: at least one broken node really is negative on remaining capacity.
    Assert.assertTrue(brokenNodes.stream().anyMatch(n -> n.getRemainingCapacity().get(CAPACITY_KEY) < 0),
        "Precondition: an over assigned node must have negative remaining capacity");

    Set<AssignableReplica> everything = new HashSet<>(toBeAssigned);
    everything.addAll(alreadyPlaced);
    ClusterContext context = new ClusterContext(everything, nodes, Collections.emptyMap(),
        Collections.emptyMap(), clusterConfig);
    Assert.assertTrue(context.getEstimateUtilizationMap().get(CAPACITY_KEY) < 0);

    OptimalAssignment assignment = createAlgorithm().calculate(
        new ClusterModel(context, toBeAssigned, nodes, ClusterModel.RebalanceScopeType.PARTIAL));
    Map<String, ResourceAssignment> result = assignment.getOptimalResourceAssignment();

    Assert.assertEquals(result.get("R_healthy").getMappedPartitions().size(), 3,
        "The healthy clique must not be blamed for the over assigned clique's overflow");
  }

  /**
   * A production shaped partial rebalance: some replicas are already placed (loaded onto their nodes
   * and NOT in the outstanding list) and only a few are outstanding. The clique whose already placed
   * load overflows its own nodes must be blamed on the strength of that source (b) demand, and a
   * healthy clique that also carries already placed load must keep its outstanding replicas. This
   * exercises the demandByBlock source (b) path (node used capacity) the way the real partial,
   * emergency and delayed scopes do, where the outstanding list is a strict subset of the cluster.
   */
  @Test
  public void testAlreadyPlacedLoadIsAttributedToItsOwnBlockInPartialScope()
      throws HelixRebalanceException, IOException {
    ClusterConfig clusterConfig = createClusterConfig(true);
    Set<AssignableNode> nodes = new HashSet<>();

    // Guilty clique 0: three nodes already full (weight 100 each), plus outstanding replicas that
    // cannot fit. Its block demand is driven over capacity by the already placed source (b) load.
    List<AssignableNode> guiltyNodes = new ArrayList<>();
    for (int i = 0; i < 3; i++) {
      AssignableNode node = taggedNode(clusterConfig, instanceName(0, i), i, cliqueTag(0));
      guiltyNodes.add(node);
      nodes.add(node);
    }
    Set<AssignableReplica> guiltyPlaced = new HashSet<>();
    addReplicas(guiltyPlaced, clusterConfig,
        taggedResource("R_g_placed", cliqueTag(0), NODE_CAPACITY), 3);
    int gi = 0;
    for (AssignableReplica replica : guiltyPlaced) {
      guiltyNodes.get(gi++).assignInitBatch(Collections.singleton(replica));
    }
    Set<AssignableReplica> guiltyOutstanding = new HashSet<>();
    addReplicas(guiltyOutstanding, clusterConfig,
        taggedResource("R_g_new", cliqueTag(0), NODE_CAPACITY), 2);

    // Healthy clique 1: light already placed load plus light outstanding load, comfortably within
    // its own capacity.
    List<AssignableNode> healthyNodes = new ArrayList<>();
    for (int i = 0; i < 3; i++) {
      AssignableNode node = taggedNode(clusterConfig, instanceName(1, i), i, cliqueTag(1));
      healthyNodes.add(node);
      nodes.add(node);
    }
    Set<AssignableReplica> healthyPlaced = new HashSet<>();
    addReplicas(healthyPlaced, clusterConfig, taggedResource("R_h_placed", cliqueTag(1), 20), 3);
    int hi = 0;
    for (AssignableReplica replica : healthyPlaced) {
      healthyNodes.get(hi++).assignInitBatch(Collections.singleton(replica));
    }
    Set<AssignableReplica> healthyOutstanding = new HashSet<>();
    addReplicas(healthyOutstanding, clusterConfig, taggedResource("R_h_new", cliqueTag(1), 20), 3);

    // The context sees every replica (placed and outstanding), exactly as the real provider builds
    // it; the model's outstanding list is the strict subset that still needs assigning.
    Set<AssignableReplica> everything = new HashSet<>();
    everything.addAll(guiltyPlaced);
    everything.addAll(guiltyOutstanding);
    everything.addAll(healthyPlaced);
    everything.addAll(healthyOutstanding);
    Set<AssignableReplica> outstanding = new HashSet<>(guiltyOutstanding);
    outstanding.addAll(healthyOutstanding);

    ClusterContext context = new ClusterContext(everything, nodes, Collections.emptyMap(),
        Collections.emptyMap(), clusterConfig);
    Assert.assertTrue(context.getEstimateUtilizationMap().get(CAPACITY_KEY) < 0,
        "Precondition: the already placed overflow must drive a cluster wide deficit");

    OptimalAssignment assignment = createAlgorithm().calculate(
        new ClusterModel(context, outstanding, nodes, ClusterModel.RebalanceScopeType.PARTIAL));
    Map<String, ResourceAssignment> result = assignment.getOptimalResourceAssignment();

    Assert.assertTrue(assignment.getSkippedResources().contains("R_g_new"),
        "The overflowing clique's outstanding replicas must be carried over");
    Assert.assertFalse(assignment.getSkippedResources().contains("R_h_new"),
        "The healthy clique must not be blamed for the overflowing clique");
    Assert.assertEquals(result.get("R_h_new").getMappedPartitions().size(), 3,
        "The healthy clique's outstanding replicas must be placed");
  }

  /**
   * A node carrying two clique tags merges those two cliques into a single share block. With a third
   * independent healthy clique present, the merged block (over committed) must be blamed as one unit
   * and the independent clique must be placed. The shared node's capacity must be credited to the
   * merged block exactly once: a double credit would inflate the merged block's capacity and stop it
   * from being recognised as over committed.
   */
  @Test
  public void testNodeBridgingTwoCliquesCreditsSharedCapacityOnce()
      throws HelixRebalanceException, IOException {
    ClusterConfig clusterConfig = twoDimConfig(true);
    Set<AssignableReplica> replicas = new HashSet<>();
    Set<AssignableNode> nodes = new HashSet<>();

    // Cliques a and b share one node (a_b_shared carries both tags), so they are one block. The
    // block is over committed on DISK.
    nodes.add(twoDimNode(clusterConfig, "a_0", 100, 100, "a"));
    nodes.add(twoDimNode(clusterConfig, "a_b_shared", 100, 100, "a", "b"));
    nodes.add(twoDimNode(clusterConfig, "b_0", 100, 100, "b"));
    addN(replicas, clusterConfig, twoDimResource("R_a", "a", 200, 1), 10);
    addN(replicas, clusterConfig, twoDimResource("R_b", "b", 200, 1), 10);

    // Independent healthy clique c.
    for (int i = 0; i < 3; i++) {
      nodes.add(twoDimNode(clusterConfig, "c_" + i, 100, 100, "c"));
    }
    addN(replicas, clusterConfig, twoDimResource("R_c", "c", 10, 10), 10);

    ClusterModel model = globalModel(clusterConfig, replicas, nodes);
    Assert.assertTrue(model.getContext().getEstimateUtilizationMap().get(DISK) < 0,
        "Precondition: cluster wide DISK deficit");

    OptimalAssignment assignment = createAlgorithm().calculate(model);
    Map<String, ResourceAssignment> result = assignment.getOptimalResourceAssignment();

    Assert.assertEquals(new TreeSet<>(assignment.getSkippedResources()),
        new TreeSet<>(Arrays.asList("R_a", "R_b")),
        "The two cliques sharing a node must be blamed together as one block");
    Assert.assertEquals(result.get("R_c").getMappedPartitions().size(), 10,
        "The independent healthy clique must still be placed");
  }

  /**
   * A node carrying two clique tags where the two cliques are the ONLY cliques merges everything into
   * one block, so there is nothing left to rebalance around and the deficit cannot be attributed. It
   * must fail exactly like the default mode.
   */
  @Test
  public void testTwoCliquesBridgedIntoOneBlockCannotIsolate() throws IOException {
    ClusterConfig clusterConfig = twoDimConfig(true);
    Set<AssignableReplica> replicas = new HashSet<>();
    Set<AssignableNode> nodes = new HashSet<>();

    nodes.add(twoDimNode(clusterConfig, "a_0", 100, 100, "a"));
    nodes.add(twoDimNode(clusterConfig, "a_b_shared", 100, 100, "a", "b"));
    nodes.add(twoDimNode(clusterConfig, "b_0", 100, 100, "b"));
    addN(replicas, clusterConfig, twoDimResource("R_a", "a", 200, 1), 10);
    addN(replicas, clusterConfig, twoDimResource("R_b", "b", 200, 1), 10);

    ClusterModel model = globalModel(clusterConfig, replicas, nodes);
    try {
      createAlgorithm().calculate(model);
      Assert.fail("A single merged block cannot be isolated and must fail the whole rebalance");
    } catch (HelixRebalanceException e) {
      Assert.assertEquals(e.getFailureCategory(),
          HelixRebalanceException.FailureCategory.CAPACITY_DEFICIT);
    }
  }

  /**
   * Several untagged resources plus a phantom tag give at least two blocks (the big merged block and
   * the phantom one), which reaches the node crediting loop with more than one untagged group in the
   * map. The {@code findFirst()} choice among untagged groups must not change the outcome, so the
   * result has to be identical across repeated runs.
   */
  @Test
  public void testSeveralUntaggedGroupsDoNotChangeTheOutcome()
      throws HelixRebalanceException, IOException {
    Map<String, Map<String, Map<String, String>>> firstRun = null;
    Set<String> firstSkipped = null;
    for (int run = 0; run < 5; run++) {
      ClusterConfig clusterConfig = createClusterConfig(true);
      Set<AssignableReplica> replicas = new HashSet<>();
      Set<AssignableNode> nodes = new HashSet<>();

      // Three untagged resources, each placeable anywhere, all in one merged block.
      for (int u = 0; u < 3; u++) {
        addReplicas(replicas, clusterConfig, taggedResource("R_untagged" + u, null,
            HEALTHY_PARTITION_WEIGHT), 3);
      }
      // Real nodes for the untagged resources to land on.
      for (int c = 0; c < 2; c++) {
        for (int i = 0; i < 3; i++) {
          nodes.add(taggedNode(clusterConfig, instanceName(c, i), i, cliqueTag(c)));
        }
      }
      // A phantom tag resource that no node carries, forming a second block and driving the deficit.
      addReplicas(replicas, clusterConfig, taggedResource("R_ghost", "ghost", 300), 10);

      ClusterModel model = globalModel(clusterConfig, replicas, nodes);
      Assert.assertTrue(model.getContext().getEstimateUtilizationMap().get(CAPACITY_KEY) < 0);

      OptimalAssignment assignment = createAlgorithm().calculate(model);
      Map<String, Map<String, Map<String, String>>> current =
          normalize(assignment.getOptimalResourceAssignment());
      Set<String> skipped = new TreeSet<>(assignment.getSkippedResources());
      if (firstRun == null) {
        firstRun = current;
        firstSkipped = skipped;
      } else {
        Assert.assertEquals(current, firstRun, "Run " + run + " diverged from the first run");
        Assert.assertEquals(skipped, firstSkipped, "Run " + run + " blamed a different set");
      }
    }
    Assert.assertEquals(firstSkipped, Collections.singleton("R_ghost"),
        "Only the phantom tag group should be blamed regardless of untagged group iteration order");
  }

  /**
   * Very large capacities and weights must not overflow the long accumulators or cast to Infinity in
   * the float scoring denominators. One clique overflows the cluster wide DISK sum with weights near
   * the integer ceiling and the healthy clique must still be placed.
   */
  @Test
  public void testVeryLargeCapacitiesDoNotOverflowOrGoInfinite()
      throws HelixRebalanceException, IOException {
    ClusterConfig clusterConfig = twoDimConfig(true);
    Set<AssignableReplica> replicas = new HashSet<>();
    Set<AssignableNode> nodes = new HashSet<>();

    int hugeNodeCap = 2_000_000_000; // just under Integer.MAX_VALUE
    // WAGED sums capacity tag blind, so to drive a cluster wide deficit the guilty clique must
    // overflow every node's capacity, not just its own clique's. Ten of these exceed the whole
    // cluster's six huge nodes.
    int hugeWeight = 1_300_000_000;

    for (int i = 0; i < 3; i++) {
      nodes.add(twoDimNode(clusterConfig, "g_" + i, hugeNodeCap, hugeNodeCap, "g"));
    }
    addN(replicas, clusterConfig, twoDimResource("R_g", "g", hugeWeight, 1), 10);

    for (int i = 0; i < 3; i++) {
      nodes.add(twoDimNode(clusterConfig, "h_" + i, hugeNodeCap, hugeNodeCap, "h"));
    }
    addN(replicas, clusterConfig, twoDimResource("R_h", "h", 10, 10), 10);

    ClusterModel model = globalModel(clusterConfig, replicas, nodes);
    Assert.assertTrue(model.getContext().getEstimateUtilizationMap().get(DISK) < 0,
        "Precondition: cluster wide DISK deficit even with huge numbers");

    OptimalAssignment assignment = createAlgorithm().calculate(model);
    Map<String, ResourceAssignment> result = assignment.getOptimalResourceAssignment();

    Assert.assertEquals(assignment.getSkippedResources(), Collections.singleton("R_g"));
    Assert.assertEquals(result.get("R_h").getMappedPartitions().size(), 10,
        "The healthy clique must be placed; large numbers must not corrupt the scoring");
  }

  /**
   * A capacity key that is required by the cluster config but missing from a node's capacity map must
   * be rejected at model build time, which is what keeps the attribution's dimension key sets
   * aligned. This documents that a node with a partial capacity map (a mismatched dimension) is not
   * reachable through a valid cluster model.
   */
  @Test
  public void testNodeMissingARequiredCapacityKeyIsRejectedAtBuild() {
    ClusterConfig clusterConfig = twoDimConfig(true);
    InstanceConfig instanceConfig = new InstanceConfig("partial_0");
    // Only DISK is configured, CPU is required but absent, and no cluster default fills it.
    instanceConfig.setInstanceCapacityMap(Collections.singletonMap(DISK, 100));
    instanceConfig.addTag("g");
    instanceConfig.setInstanceOperation(InstanceConstants.InstanceOperation.ENABLE);
    try {
      new AssignableNode(clusterConfig, instanceConfig, "partial_0");
      Assert.fail("A node missing a required capacity key must be rejected at build time");
    } catch (HelixException expected) {
      // Expected: the model can never carry a node with a mismatched dimension.
    }
  }

  // =============================================================================================
  // H2  DOUBLE COUNTING  (theoretical: needs a model the real ClusterModelProvider cannot build)
  // =============================================================================================

  /**
   * THEORETICAL. Demonstrates that {@code absorbCapacityDeficit} double counts a replica that is
   * both outstanding (present in the replica list) AND already loaded onto a node, because its
   * demand is added once from the replica list and once from the node's used capacity. A healthy
   * clique whose replicas are counted twice is wrongly flagged as over committed and needlessly
   * carried over.
   *
   * The real {@code ClusterModelProvider} never produces such a model: in all four scopes
   * (baseline, partial, emergency, delayed overwrites) a replica is either outstanding OR loaded
   * onto a node, never both. This test builds the invalid model by hand purely to prove the
   * arithmetic is sensitive to the invariant. It asserts the clean model isolates correctly and the
   * invariant violating model blames the healthy clique, so it passes while documenting the latent
   * dependency.
   */
  @Test
  public void testDoubleCountingBlamesHealthyCliqueOnlyWhenInvariantIsViolatedTheoretical()
      throws HelixRebalanceException, IOException {
    // Clean model (the invariant holds): only the genuinely guilty clique is carried over. The
    // victim clique that fits its own nodes exactly is placed, and so is the survivor clique.
    Assert.assertEquals(skippedUnderDoubleCount(false), Collections.singleton("R_broken"),
        "Clean model (invariant holds): only the genuinely guilty clique is carried over");
    // Invariant violating model (a victim replica is both outstanding AND already loaded on a node):
    // the victim's block demand is inflated past its own capacity, so it is wrongly blamed and
    // carried over as well. The survivor clique keeps not every block blamed, so the run proceeds
    // rather than throwing, which is exactly the "healthy clique needlessly carried over" harm H2
    // predicts.
    Assert.assertEquals(skippedUnderDoubleCount(true),
        new TreeSet<>(Arrays.asList("R_broken", "R_h")),
        "Invariant violating model: the double counted victim clique is wrongly carried over too");
  }

  /**
   * Builds a three clique model. The guilty clique overflows the cluster wide sum on its own and is
   * always carried over. The victim clique fits its nodes exactly, so any double count tips it over.
   * The survivor clique always fits, so not every block is blamed and the run carries the guilty
   * blocks over instead of falling back to the throw. When {@code violateInvariant} is true, two of
   * the victim's OUTSTANDING replicas are ALSO pre loaded onto one of the victim's nodes, the exact
   * situation the real provider forbids, so the victim's block demand is counted twice.
   *
   * @return every resource that ended up carried over (skipped).
   */
  private Set<String> skippedUnderDoubleCount(boolean violateInvariant)
      throws HelixRebalanceException, IOException {
    ClusterConfig clusterConfig = createClusterConfig(true);
    Set<AssignableNode> nodes = new HashSet<>();

    // Guilty clique 0: 10 partitions of weight 400 overflow both its own three nodes and, because
    // WAGED sums tag blind, the whole cluster.
    for (int i = 0; i < 3; i++) {
      nodes.add(taggedNode(clusterConfig, instanceName(0, i), i, cliqueTag(0)));
    }
    Set<AssignableReplica> guilty = new HashSet<>();
    addReplicas(guilty, clusterConfig, taggedResource("R_broken", cliqueTag(0), 400), 10);

    // Victim clique 1: 12 partitions of weight 25 = demand 300 fit its three nodes of capacity 100
    // EXACTLY, so any double count tips it just over its own capacity.
    ResourceConfig victimConfig = taggedResource("R_h", cliqueTag(1), 25);
    List<AssignableNode> victimNodes = new ArrayList<>();
    for (int i = 0; i < 3; i++) {
      AssignableNode node = taggedNode(clusterConfig, instanceName(1, i), i, cliqueTag(1));
      victimNodes.add(node);
      nodes.add(node);
    }
    Set<AssignableReplica> victim = new HashSet<>();
    addReplicas(victim, clusterConfig, victimConfig, 12);

    // Survivor clique 2: 10 partitions of weight 20 = demand 200 with 300 of capacity, always
    // placeable, so not every block is blamed and the run proceeds rather than throwing.
    for (int i = 0; i < 3; i++) {
      nodes.add(taggedNode(clusterConfig, instanceName(2, i), i, cliqueTag(2)));
    }
    Set<AssignableReplica> survivor = new HashSet<>();
    addReplicas(survivor, clusterConfig, taggedResource("R_s", cliqueTag(2), 20), 10);

    // Everything is outstanding (GLOBAL_BASELINE style).
    Set<AssignableReplica> outstanding = new HashSet<>(guilty);
    outstanding.addAll(victim);
    outstanding.addAll(survivor);

    if (violateInvariant) {
      // Pre load two of the victim's OUTSTANDING replicas onto one of its own nodes. The real
      // provider never does this; here it makes the same demand be counted twice, once from the
      // replica list and once from the node's used capacity.
      List<AssignableReplica> two = new ArrayList<>();
      for (AssignableReplica replica : victim) {
        if (two.size() < 2) {
          two.add(replica);
        }
      }
      victimNodes.get(0).assignInitBatch(new HashSet<>(two));
    }

    ClusterContext context = new ClusterContext(outstanding, nodes, Collections.emptyMap(),
        Collections.emptyMap(), clusterConfig);
    Assert.assertTrue(context.getEstimateUtilizationMap().get(CAPACITY_KEY) < 0,
        "Precondition: the guilty clique must drive a cluster wide deficit");

    OptimalAssignment assignment = createAlgorithm().calculate(
        new ClusterModel(context, outstanding, nodes,
            ClusterModel.RebalanceScopeType.GLOBAL_BASELINE));
    Assert.assertFalse(assignment.getSkippedResources().contains("R_s"),
        "The survivor clique must never be carried over");
    return new TreeSet<>(assignment.getSkippedResources());
  }
}
