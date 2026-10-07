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
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;

import org.apache.helix.HelixRebalanceException;
import org.apache.helix.constants.InstanceConstants;
import org.apache.helix.controller.rebalancer.util.WagedRebalanceUtil;
import org.apache.helix.controller.rebalancer.waged.RebalanceAlgorithm;
import org.apache.helix.controller.rebalancer.waged.constraints.ConstraintBasedAlgorithmFactory;
import org.apache.helix.model.ClusterConfig;
import org.apache.helix.model.InstanceConfig;
import org.apache.helix.model.Partition;
import org.apache.helix.model.ResourceAssignment;
import org.apache.helix.model.ResourceConfig;
import org.testng.Assert;
import org.testng.annotations.Test;

/**
 * Models the clique partitioned topology in which the cluster is partitioned into independent
 * "cliques": every instance carries a clique tag, and every resource is pinned to exactly one
 * clique tag via {@link ResourceConfig#setInstanceGroupTag}. Cliques share no nodes, so they are
 * fully disjoint failure domains from the operator's point of view. The one exception is the
 * bridged cluster of an isolation test, where a single instance carries two clique tags.
 *
 * The tests measure how much of such a cluster one broken clique takes down, a broken clique being
 * one with a replica that fits no node.
 *
 * In the default mode WAGED does NOT honor that isolation. The rebalance algorithm builds a single
 * global cluster model and assigns every replica in one loop, so a single unplaceable replica in
 * one clique aborts the entire calculation and no clique gets a new assignment.
 *
 * With {@link ClusterConfig#setWagedInstanceTagIsolationEnabled} on, the calculation skips the
 * broken clique's share block, which is the broken clique together with every clique it shares an
 * instance with, followed transitively, and {@code WagedRebalanceUtil.calculateAssignment}
 * carries the previous assignment of the block's resources forward. Exactly the block's resources
 * are carried, and every other clique gets exactly the assignment the default mode computes when
 * the broken clique's resources are absent.
 */
public class TestCliqueFailureBlastRadius {
  private static final int CLIQUE_COUNT = 20;
  private static final int NODES_PER_CLIQUE = 10;
  private static final int PARTITIONS_PER_RESOURCE = 10;
  private static final String CAPACITY_KEY = "DISK";
  private static final int NODE_CAPACITY = 100;
  private static final int HEALTHY_PARTITION_WEIGHT = 10;
  // Larger than a single node's capacity, so NodeCapacityConstraint rejects every node in the
  // clique and the replica has no candidate at all.
  private static final int BROKEN_PARTITION_WEIGHT = 150;
  private static final int BROKEN_CLIQUE = 3;
  // Shares instance_3_0 with BROKEN_CLIQUE in the bridged cluster of the isolation tests.
  private static final int BRIDGED_CLIQUE = 4;

  private static String cliqueTag(int clique) {
    return "clique_" + clique;
  }

  private static String resourceName(int clique) {
    return "Resource_clique_" + clique;
  }

  /** The second resource each clique serves in the isolation tests. */
  private static String siblingName(int clique) {
    return "Sibling_clique_" + clique;
  }

  private static String instanceName(int clique, int node) {
    return "instance_" + clique + "_" + node;
  }

  /** Both resources of each given clique in the isolation tests, sorted by name. */
  private static Set<String> resourcesOf(int... cliques) {
    Set<String> resources = new TreeSet<>();
    for (int clique : cliques) {
      resources.add(resourceName(clique));
      resources.add(siblingName(clique));
    }
    return resources;
  }

  private ClusterConfig createClusterConfig() {
    ClusterConfig clusterConfig = new ClusterConfig("CliquePartitionedCluster");
    clusterConfig.setInstanceCapacityKeys(Collections.singletonList(CAPACITY_KEY));
    clusterConfig.setDefaultPartitionWeightMap(Collections.singletonMap(CAPACITY_KEY, 0));
    return clusterConfig;
  }

  private ClusterConfig createIsolatedClusterConfig() {
    ClusterConfig clusterConfig = createClusterConfig();
    clusterConfig.setWagedInstanceTagIsolationEnabled(true);
    return clusterConfig;
  }

  /**
   * 20 cliques x 10 nodes = 200 instances. Each instance is tagged with exactly one clique tag.
   */
  private Set<AssignableNode> createNodes(ClusterConfig clusterConfig) {
    return createNodes(clusterConfig, false);
  }

  /**
   * The same 200 instances, except that with {@code bridged} set instance_3_0 also carries
   * clique_4's tag, which joins cliques 3 and 4 into one share block.
   */
  private Set<AssignableNode> createNodes(ClusterConfig clusterConfig, boolean bridged) {
    Set<AssignableNode> nodes = new HashSet<>();
    for (int clique = 0; clique < CLIQUE_COUNT; clique++) {
      for (int i = 0; i < NODES_PER_CLIQUE; i++) {
        String instanceName = "instance_" + clique + "_" + i;
        InstanceConfig instanceConfig = new InstanceConfig(instanceName);
        instanceConfig
            .setInstanceCapacityMap(Collections.singletonMap(CAPACITY_KEY, NODE_CAPACITY));
        instanceConfig.addTag(cliqueTag(clique));
        if (bridged && clique == BROKEN_CLIQUE && i == 0) {
          instanceConfig.addTag(cliqueTag(BRIDGED_CLIQUE));
        }
        instanceConfig.setInstanceOperation(InstanceConstants.InstanceOperation.ENABLE);
        nodes.add(new AssignableNode(clusterConfig, instanceConfig, instanceName));
      }
    }
    return nodes;
  }

  /**
   * One resource per clique, pinned to that clique's tag. The resource belonging to
   * {@code brokenClique} (if any) gets a per-partition weight that exceeds a single node's
   * capacity, so none of its replicas can be placed anywhere.
   */
  private Set<AssignableReplica> createReplicas(ClusterConfig clusterConfig, int brokenClique)
      throws IOException {
    Set<AssignableReplica> replicas = new HashSet<>();
    for (int clique = 0; clique < CLIQUE_COUNT; clique++) {
      int weight = clique == brokenClique ? BROKEN_PARTITION_WEIGHT : HEALTHY_PARTITION_WEIGHT;
      replicas.addAll(createResourceReplicas(clusterConfig, resourceName(clique), clique, weight));
    }
    return replicas;
  }

  /** One replica per partition of {@code resource}, pinned to the clique's tag. */
  private Set<AssignableReplica> createResourceReplicas(ClusterConfig clusterConfig,
      String resource, int clique, int weight) throws IOException {
    ResourceConfig resourceConfig = new ResourceConfig(resource);
    resourceConfig.getRecord()
        .setSimpleField(ResourceConfig.ResourceConfigProperty.INSTANCE_GROUP_TAG.name(),
            cliqueTag(clique));
    resourceConfig.setPartitionCapacityMap(Collections
        .singletonMap(ResourceConfig.DEFAULT_PARTITION_KEY,
            Collections.singletonMap(CAPACITY_KEY, weight)));
    Set<AssignableReplica> replicas = new HashSet<>();
    for (int p = 0; p < PARTITIONS_PER_RESOURCE; p++) {
      replicas.add(
          new AssignableReplica(clusterConfig, resourceConfig, resource + "_" + p, "ONLINE", 0));
    }
    return replicas;
  }

  private ClusterModel createClusterModel(ClusterConfig clusterConfig, int brokenClique)
      throws IOException {
    Set<AssignableReplica> replicas = createReplicas(clusterConfig, brokenClique);
    Set<AssignableNode> nodes = createNodes(clusterConfig);
    ClusterContext context = new ClusterContext(replicas, nodes, Collections.emptyMap(),
        Collections.emptyMap(), clusterConfig);
    return new ClusterModel(context, replicas, nodes,
        ClusterModel.RebalanceScopeType.GLOBAL_BASELINE);
  }

  /**
   * The cluster of the isolation tests: the same instances, bridged or not, with every clique
   * serving Resource_clique_k and Sibling_clique_k, ten partitions each, so a share block always
   * holds more than one resource. Resource_clique_k of {@code brokenClique} fits no node. The
   * broken clique's two resources are left out entirely unless {@code withBrokenClique} is set.
   */
  private ClusterModel createPairedClusterModel(ClusterConfig clusterConfig, boolean bridged,
      int brokenClique, boolean withBrokenClique) throws IOException {
    Set<AssignableReplica> replicas = new HashSet<>();
    for (int clique = 0; clique < CLIQUE_COUNT; clique++) {
      if (clique == brokenClique && !withBrokenClique) {
        continue;
      }
      int weight = clique == brokenClique ? BROKEN_PARTITION_WEIGHT : HEALTHY_PARTITION_WEIGHT;
      replicas.addAll(createResourceReplicas(clusterConfig, resourceName(clique), clique, weight));
      replicas.addAll(createResourceReplicas(clusterConfig, siblingName(clique), clique,
          HEALTHY_PARTITION_WEIGHT));
    }
    Set<AssignableNode> nodes = createNodes(clusterConfig, bridged);
    ClusterContext context = new ClusterContext(replicas, nodes, Collections.emptyMap(),
        Collections.emptyMap(), clusterConfig);
    return new ClusterModel(context, replicas, nodes,
        ClusterModel.RebalanceScopeType.GLOBAL_BASELINE);
  }

  private RebalanceAlgorithm createAlgorithm() {
    return ConstraintBasedAlgorithmFactory.getInstance(Collections.emptyMap());
  }

  /**
   * Control: when no clique is broken, all 20 cliques are assigned successfully. This proves the
   * topology itself is satisfiable and that the failure in the next test is caused purely by the
   * one broken clique.
   */
  @Test
  public void testAllCliquesHealthyProducesFullAssignment()
      throws HelixRebalanceException, IOException {
    ClusterConfig clusterConfig = createClusterConfig();
    ClusterModel clusterModel = createClusterModel(clusterConfig, -1);

    OptimalAssignment assignment = createAlgorithm().calculate(clusterModel);
    Map<String, org.apache.helix.model.ResourceAssignment> result =
        assignment.getOptimalResourceAssignment();

    Assert.assertEquals(result.size(), CLIQUE_COUNT,
        "All cliques should be assigned when nothing is broken");
    for (int clique = 0; clique < CLIQUE_COUNT; clique++) {
      Assert.assertEquals(result.get(resourceName(clique)).getMappedPartitions().size(),
          PARTITIONS_PER_RESOURCE);
    }
  }

  /**
   * In the default mode, breaking a single clique aborts the whole global calculation. Even though
   * cliques are disjoint (no shared nodes, no shared resources), the 19 healthy cliques receive no
   * assignment at all because the algorithm throws on the first unplaceable replica.
   */
  @Test
  public void testSingleBrokenCliqueBlocksAllOtherCliques() throws IOException {
    ClusterConfig clusterConfig = createClusterConfig();
    ClusterModel clusterModel = createClusterModel(clusterConfig, BROKEN_CLIQUE);

    HelixRebalanceException thrown = null;
    try {
      createAlgorithm().calculate(clusterModel);
      Assert.fail("Expected the global calculation to abort because of the broken clique");
    } catch (HelixRebalanceException ex) {
      thrown = ex;
    }

    // The whole calculation aborts, so no clique -- healthy or not -- receives an assignment.
    Assert.assertEquals(thrown.getFailureType(), HelixRebalanceException.Type.FAILED_TO_CALCULATE);
    Assert.assertEquals(thrown.getFailureCategory(),
        HelixRebalanceException.FailureCategory.NO_CANDIDATE_NODE);

    // The failure is attributed to exactly one resource -- the broken clique's -- yet it takes the
    // entire cluster's rebalance down with it.
    Assert.assertTrue(thrown.getMessage().contains(resourceName(BROKEN_CLIQUE)),
        "Failure should be attributed to the broken clique's resource, but was: "
            + thrown.getMessage());
    for (int clique = 0; clique < CLIQUE_COUNT; clique++) {
      if (clique == BROKEN_CLIQUE) {
        continue;
      }
      Assert.assertFalse(thrown.getMessage().contains(resourceName(clique) + "_"),
          "Healthy clique " + clique + " is not itself unplaceable, yet it gets no assignment");
    }
  }

  /**
   * Shows the blast radius is independent of which clique breaks: every clique index, when broken,
   * takes down the entire cluster's rebalance.
   */
  @Test
  public void testAnyBrokenCliqueBlocksTheWholeCluster() throws IOException {
    ClusterConfig clusterConfig = createClusterConfig();
    List<Integer> cliquesThatBlockedEverything = new ArrayList<>();
    for (int brokenClique = 0; brokenClique < CLIQUE_COUNT; brokenClique++) {
      ClusterModel clusterModel = createClusterModel(clusterConfig, brokenClique);
      try {
        createAlgorithm().calculate(clusterModel);
      } catch (HelixRebalanceException ex) {
        cliquesThatBlockedEverything.add(brokenClique);
      }
    }

    Assert.assertEquals(cliquesThatBlockedEverything.size(), CLIQUE_COUNT,
        "Every clique, when broken, should abort the whole global rebalance. Blocked: "
            + cliquesThatBlockedEverything);
  }

  /**
   * Demonstrates the mechanism behind the blast radius: replicas are sorted and assigned in one
   * flat global loop that is not grouped by resource or by clique, so the loop cannot skip the
   * failing clique and continue with the rest.
   */
  @Test
  public void testReplicasFromAllCliquesShareOneGlobalAssignmentPass() throws IOException {
    ClusterConfig clusterConfig = createClusterConfig();
    ClusterModel clusterModel = createClusterModel(clusterConfig, -1);

    Map<String, Set<AssignableReplica>> replicasByResource =
        clusterModel.getAssignableReplicaMap();
    Assert.assertEquals(replicasByResource.size(), CLIQUE_COUNT);

    int totalReplicas = replicasByResource.values().stream().mapToInt(Set::size).sum();
    Assert.assertEquals(totalReplicas, CLIQUE_COUNT * PARTITIONS_PER_RESOURCE,
        "All cliques' replicas live in one cluster model, i.e. one shared calculation");
    Assert.assertEquals(clusterModel.getAssignableNodes().size(),
        CLIQUE_COUNT * NODES_PER_CLIQUE,
        "All cliques' nodes live in one cluster model");
  }

  /**
   * Sanity check that the clique tags really do isolate placement: a healthy clique's replicas can
   * only ever land on that clique's own nodes. This is what makes the shared-failure behavior
   * surprising -- the cliques are disjoint in every dimension except the calculation itself.
   */
  @Test
  public void testCliqueTagsIsolatePlacement() throws HelixRebalanceException, IOException {
    ClusterConfig clusterConfig = createClusterConfig();
    ClusterModel clusterModel = createClusterModel(clusterConfig, -1);

    Map<String, org.apache.helix.model.ResourceAssignment> result =
        createAlgorithm().calculate(clusterModel).getOptimalResourceAssignment();

    for (int clique = 0; clique < CLIQUE_COUNT; clique++) {
      org.apache.helix.model.ResourceAssignment resourceAssignment =
          result.get(resourceName(clique));
      Map<String, Integer> perCliquePlacements = new HashMap<>();
      resourceAssignment.getMappedPartitions().forEach(partition -> resourceAssignment
          .getReplicaMap(partition).keySet().forEach(instance -> {
            String owningClique = instance.split("_")[1];
            perCliquePlacements.merge(owningClique, 1, Integer::sum);
          }));
      Assert.assertEquals(perCliquePlacements.keySet(),
          Collections.singleton(String.valueOf(clique)),
          "Resource for clique " + clique + " must only be placed on that clique's nodes");
    }
  }

  /**
   * Breaks clique 3 in the paired cluster, where clique 3 shares no instance with another clique,
   * so its share block holds Resource_clique_3 and Sibling_clique_3. Asserts that the default mode
   * fails the cluster with NO_CANDIDATE_NODE, and that with isolation on the calculation does not
   * throw, returns all 40 resources, skips and carries forward exactly those two resources with
   * their previous assignment unchanged, and gives each of the other 38 resources exactly the
   * assignment the default mode computes on the same nodes without clique 3's resources. Each of
   * those 38 reference assignments is also asserted to differ from the resource's previous
   * assignment, so a resource carried forward by mistake cannot pass.
   */
  @Test
  public void testIsolationCarriesForwardOnlyTheBrokenCliquesShareBlock()
      throws IOException, HelixRebalanceException {
    assertOnlyShareBlockCarriedForward(false, BROKEN_CLIQUE, resourcesOf(BROKEN_CLIQUE));
  }

  /**
   * Breaks each of the 20 cliques of the paired cluster in turn and asserts for each one exactly
   * what {@link #testIsolationCarriesForwardOnlyTheBrokenCliquesShareBlock} asserts for clique 3:
   * the default mode fails, and with isolation on only the broken clique's two resources are
   * skipped and carried forward unchanged, while the other 38 get exactly the assignment computed
   * without them, an assignment that differs from their previous one.
   */
  @Test
  public void testIsolationCarriesForwardOnlyTheShareBlockOfAnyBrokenClique()
      throws IOException, HelixRebalanceException {
    for (int brokenClique = 0; brokenClique < CLIQUE_COUNT; brokenClique++) {
      assertOnlyShareBlockCarriedForward(false, brokenClique, resourcesOf(brokenClique));
    }
  }

  /**
   * In the bridged cluster instance_3_0 also carries clique_4's tag, so cliques 3 and 4 form one
   * share block of four resources. Breaks clique 3, then separately clique 4, and asserts each time
   * that the default mode fails the cluster with NO_CANDIDATE_NODE, and that with isolation on the
   * calculation does not throw, returns all 40 resources, skips and carries forward exactly the
   * four resources of cliques 3 and 4 with their previous assignment unchanged, those of the
   * clique that is not broken included, and gives each of the other 36 resources exactly the
   * assignment the default mode computes on the same nodes without the broken clique's resources,
   * an assignment that differs from its previous one.
   */
  @Test
  public void testIsolationCarriesForwardBothCliquesOfABridgedShareBlock()
      throws IOException, HelixRebalanceException {
    Set<String> shareBlock = resourcesOf(BROKEN_CLIQUE, BRIDGED_CLIQUE);
    assertOnlyShareBlockCarriedForward(true, BROKEN_CLIQUE, shareBlock);
    assertOnlyShareBlockCarriedForward(true, BRIDGED_CLIQUE, shareBlock);
  }

  /**
   * Breaks {@code brokenClique} in the paired cluster and asserts that:
   * <ul>
   * <li>the default mode fails the cluster with NO_CANDIDATE_NODE;</li>
   * <li>with isolation on, {@code WagedRebalanceUtil.calculateAssignment} does not throw;</li>
   * <li>the resources the algorithm skips, and the resources
   * {@code WagedRebalanceUtil.calculateAssignment} reports back to it as skipped after the
   * carry-forward, are as many as {@code shareBlock} holds and are exactly {@code shareBlock};</li>
   * <li>the result holds every resource;</li>
   * <li>each resource of {@code shareBlock} gets its previous assignment unchanged;</li>
   * <li>every other resource gets exactly the assignment the default mode computes on the same
   * nodes without the broken clique's resources, and that assignment differs from its previous
   * one, so a resource carried forward by mistake cannot pass.</li>
   * </ul>
   */
  private void assertOnlyShareBlockCarriedForward(boolean bridged, int brokenClique,
      Set<String> shareBlock) throws IOException, HelixRebalanceException {
    String scenario =
        (bridged ? "Bridged" : "Disjoint") + " cluster with clique " + brokenClique + " broken";

    HelixRebalanceException defaultFailure = null;
    try {
      createAlgorithm()
          .calculate(createPairedClusterModel(createClusterConfig(), bridged, brokenClique, true));
    } catch (HelixRebalanceException e) {
      defaultFailure = e;
    }
    Assert.assertNotNull(defaultFailure, scenario + ": the default mode calculation succeeded");
    Assert.assertEquals(defaultFailure.getFailureCategory(),
        HelixRebalanceException.FailureCategory.NO_CANDIDATE_NODE, scenario);

    Map<String, ResourceAssignment> previous = createPreviousAssignment();
    RecordingAlgorithm algorithm = new RecordingAlgorithm(createAlgorithm());
    Map<String, ResourceAssignment> result = null;
    try {
      result = WagedRebalanceUtil.calculateAssignment(
          createPairedClusterModel(createIsolatedClusterConfig(), bridged, brokenClique, true),
          algorithm, previous);
    } catch (HelixRebalanceException e) {
      Assert.fail(scenario + ": the isolated calculation threw", e);
    }

    Assert.assertEquals(algorithm._skipped.size(), shareBlock.size(),
        scenario + ": number of resources skipped " + algorithm._skipped);
    Assert.assertEquals(algorithm._carried.size(), shareBlock.size(),
        scenario + ": number of resources reported back as skipped after the carry-forward "
            + algorithm._carried);
    Assert.assertEquals(algorithm._skipped, shareBlock, scenario + ": resources skipped");
    Assert.assertEquals(algorithm._carried, shareBlock,
        scenario + ": resources reported back as skipped after the carry-forward");
    Assert.assertEquals(new TreeSet<>(result.keySet()), new TreeSet<>(previous.keySet()),
        scenario + ": resources returned");
    for (String resource : shareBlock) {
      Assert.assertEquals(normalize(result.get(resource)), normalize(previous.get(resource)),
          scenario + ": " + resource + " should get its previous assignment");
    }

    Map<String, ResourceAssignment> reference = createAlgorithm()
        .calculate(createPairedClusterModel(createClusterConfig(), bridged, brokenClique, false))
        .getOptimalResourceAssignment();
    for (String resource : result.keySet()) {
      if (shareBlock.contains(resource)) {
        continue;
      }
      Map<String, Map<String, String>> expected = normalize(reference.get(resource));
      Assert.assertFalse(normalize(previous.get(resource)).equals(expected),
          scenario + ": " + resource + " is calculated onto its previous assignment, so carrying"
              + " it forward would go unnoticed");
      Assert.assertEquals(normalize(result.get(resource)), expected,
          scenario + ": " + resource + " should get the assignment calculated without clique "
              + brokenClique + "'s resources");
    }
  }

  /**
   * The previous assignment the isolated calculation is given: partition p of Resource_clique_k on
   * instance_k_(p / 2) and partition p of Sibling_clique_k on instance_k_(5 + p / 2), two
   * partitions per node. A calculation spreads each resource over every node of its clique
   * instead, which the tests check, so a resource carried forward cannot pass for one calculated.
   */
  private static Map<String, ResourceAssignment> createPreviousAssignment() {
    Map<String, ResourceAssignment> previous = new HashMap<>();
    for (int clique = 0; clique < CLIQUE_COUNT; clique++) {
      previous.put(resourceName(clique), stackedAssignment(resourceName(clique), clique, 0));
      previous.put(siblingName(clique),
          stackedAssignment(siblingName(clique), clique, NODES_PER_CLIQUE / 2));
    }
    return previous;
  }

  /** Partition p of {@code resource} on instance_clique_(firstNode + p / 2). */
  private static ResourceAssignment stackedAssignment(String resource, int clique,
      int firstNode) {
    ResourceAssignment assignment = new ResourceAssignment(resource);
    for (int p = 0; p < PARTITIONS_PER_RESOURCE; p++) {
      assignment.addReplicaMap(new Partition(resource + "_" + p),
          Collections.singletonMap(instanceName(clique, firstNode + p / 2), "ONLINE"));
    }
    return assignment;
  }

  /** Partition to instance to state in sorted maps, so assignments compare by content. */
  private static Map<String, Map<String, String>> normalize(ResourceAssignment assignment) {
    if (assignment == null) {
      return null;
    }
    Map<String, Map<String, String>> replicasByPartition = new TreeMap<>();
    for (Partition partition : assignment.getMappedPartitions()) {
      replicasByPartition.put(partition.getPartitionName(),
          new TreeMap<>(assignment.getReplicaMap(partition)));
    }
    return replicasByPartition;
  }

  /**
   * Delegates to the WAGED algorithm. Keeps the resources it skipped, and the resources
   * {@code WagedRebalanceUtil.calculateAssignment} reports back to it as skipped after the
   * carry-forward, which also include any resource that yields its fresh assignment to a carried
   * one. Every skipped resource here has a previous assignment, so the reported resources are the
   * ones carried forward.
   */
  private static final class RecordingAlgorithm implements RebalanceAlgorithm {
    private final RebalanceAlgorithm _delegate;
    private Set<String> _skipped = Collections.emptySet();
    private Set<String> _carried = Collections.emptySet();

    private RecordingAlgorithm(RebalanceAlgorithm delegate) {
      _delegate = delegate;
    }

    @Override
    public OptimalAssignment calculate(ClusterModel clusterModel) throws HelixRebalanceException {
      OptimalAssignment assignment = _delegate.calculate(clusterModel);
      _skipped = new TreeSet<>(assignment.getSkippedResources());
      return assignment;
    }

    @Override
    public String getName() {
      return _delegate.getName();
    }

    @Override
    public void onAssignmentComputed(ClusterModel.RebalanceScopeType scope,
        Set<String> evaluatedResources, Set<String> skippedResources) {
      _carried = new TreeSet<>(skippedResources);
    }
  }
}
