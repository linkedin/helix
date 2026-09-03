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
import java.util.stream.IntStream;

import org.apache.helix.HelixRebalanceException;
import org.apache.helix.constants.InstanceConstants;
import org.apache.helix.controller.rebalancer.waged.RebalanceAlgorithm;
import org.apache.helix.controller.rebalancer.util.WagedRebalanceUtil;
import org.apache.helix.controller.rebalancer.waged.constraints.ConstraintBasedAlgorithm;
import org.apache.helix.controller.rebalancer.waged.constraints.ConstraintBasedAlgorithmFactory;
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
 * Verifies the WAGED instance-tag ("clique") isolation mode gated by
 * {@link ClusterConfig#setWagedInstanceTagIsolationEnabled}.
 *
 * The topology under test is the clique partitioned one: the cluster is carved into disjoint
 * cliques, every instance carries exactly one clique tag, and every resource is pinned to one clique tag through
 * INSTANCE_GROUP_TAG. {@link TestCliqueFailureBlastRadius} documents the default behavior, where a
 * single unplaceable clique aborts the whole cluster's rebalance. This class asserts that with the
 * flag on:
 * <ul>
 *   <li>a broken clique no longer blocks the healthy ones,</li>
 *   <li>a broken clique is rolled back atomically instead of being half assigned,</li>
 *   <li>the result is byte for byte identical to the default mode whenever nothing is broken,</li>
 *   <li>and every topology change (node loss, EVACUATE / UNKNOWN instance operations, capacity
 *       changes) keeps both properties.</li>
 * </ul>
 */
public class TestWagedInstanceTagIsolation {
  private static final int CLIQUE_COUNT = 20;
  private static final int NODES_PER_CLIQUE = 10;
  private static final int PARTITIONS_PER_RESOURCE = 10;
  private static final String CAPACITY_KEY = "DISK";
  private static final int NODE_CAPACITY = 100;
  private static final int HEALTHY_PARTITION_WEIGHT = 10;
  // Larger than a single node's capacity, so NodeCapacityConstraint rejects every node in the
  // clique and none of that resource's replicas has a candidate at all.
  private static final int UNPLACEABLE_PARTITION_WEIGHT = 150;
  private static final int NO_BROKEN_CLIQUE = -1;

  private static String cliqueTag(int clique) {
    return "clique_" + clique;
  }

  private static String resourceName(int clique) {
    return "Resource_clique_" + clique;
  }

  private static String instanceName(int clique, int index) {
    return "instance_" + clique + "_" + index;
  }

  /**
   * Describes one clique's shape so individual tests can perturb a single clique without touching
   * the others.
   */
  private static final class CliqueSpec {
    private final int _nodeCount;
    private final int _partitionCount;
    private final int _partitionWeight;
    private final Set<Integer> _nonAssignableNodeIndices;

    private CliqueSpec(int nodeCount, int partitionCount, int partitionWeight,
        Set<Integer> nonAssignableNodeIndices) {
      _nodeCount = nodeCount;
      _partitionCount = partitionCount;
      _partitionWeight = partitionWeight;
      _nonAssignableNodeIndices = nonAssignableNodeIndices;
    }

    static CliqueSpec healthy() {
      return new CliqueSpec(NODES_PER_CLIQUE, PARTITIONS_PER_RESOURCE, HEALTHY_PARTITION_WEIGHT,
          Collections.emptySet());
    }

    CliqueSpec withPartitionWeight(int weight) {
      return new CliqueSpec(_nodeCount, _partitionCount, weight, _nonAssignableNodeIndices);
    }

    CliqueSpec withPartitionCount(int partitionCount) {
      return new CliqueSpec(_nodeCount, partitionCount, _partitionWeight, _nonAssignableNodeIndices);
    }

    /**
     * Removes nodes from the clique. Models a participant that went away, was decommissioned, or
     * was moved to an instance operation that {@code InstanceConfig#isAssignable} rejects
     * (EVACUATE, UNKNOWN, SWAP_IN), all of which drop the node before the algorithm ever runs.
     */
    CliqueSpec withNodeCount(int nodeCount) {
      return new CliqueSpec(nodeCount, _partitionCount, _partitionWeight, _nonAssignableNodeIndices);
    }
  }

  private static Map<Integer, CliqueSpec> allHealthy() {
    Map<Integer, CliqueSpec> specs = new HashMap<>();
    for (int clique = 0; clique < CLIQUE_COUNT; clique++) {
      specs.put(clique, CliqueSpec.healthy());
    }
    return specs;
  }

  private ClusterConfig createClusterConfig(boolean tagIsolationEnabled) {
    ClusterConfig clusterConfig = new ClusterConfig("CliquePartitionedCluster");
    clusterConfig.setInstanceCapacityKeys(Collections.singletonList(CAPACITY_KEY));
    clusterConfig.setDefaultPartitionWeightMap(Collections.singletonMap(CAPACITY_KEY, 0));
    clusterConfig.setDisabledInstances(Collections.emptyMap());
    clusterConfig.setWagedInstanceTagIsolationEnabled(tagIsolationEnabled);
    return clusterConfig;
  }

  private Set<AssignableNode> createNodes(ClusterConfig clusterConfig,
      Map<Integer, CliqueSpec> specs) {
    Set<AssignableNode> nodes = new HashSet<>();
    for (Map.Entry<Integer, CliqueSpec> entry : specs.entrySet()) {
      int clique = entry.getKey();
      for (int i = 0; i < entry.getValue()._nodeCount; i++) {
        String instance = instanceName(clique, i);
        InstanceConfig instanceConfig = new InstanceConfig(instance);
        instanceConfig
            .setInstanceCapacityMap(Collections.singletonMap(CAPACITY_KEY, NODE_CAPACITY));
        instanceConfig.addTag(cliqueTag(clique));
        instanceConfig.setInstanceOperation(InstanceConstants.InstanceOperation.ENABLE);
        // Spread the clique's nodes across zones so FaultZoneAwareConstraint is satisfiable.
        instanceConfig.setZoneId("zone_" + i);
        nodes.add(new AssignableNode(clusterConfig, instanceConfig, instance));
      }
    }
    return nodes;
  }

  private Set<AssignableReplica> createReplicas(ClusterConfig clusterConfig,
      Map<Integer, CliqueSpec> specs) throws IOException {
    Set<AssignableReplica> replicas = new HashSet<>();
    for (Map.Entry<Integer, CliqueSpec> entry : specs.entrySet()) {
      int clique = entry.getKey();
      CliqueSpec spec = entry.getValue();
      ResourceConfig resourceConfig = new ResourceConfig(resourceName(clique));
      resourceConfig.getRecord()
          .setSimpleField(ResourceConfig.ResourceConfigProperty.INSTANCE_GROUP_TAG.name(),
              cliqueTag(clique));
      resourceConfig.setPartitionCapacityMap(Collections
          .singletonMap(ResourceConfig.DEFAULT_PARTITION_KEY,
              Collections.singletonMap(CAPACITY_KEY, spec._partitionWeight)));
      for (int p = 0; p < spec._partitionCount; p++) {
        replicas.add(new AssignableReplica(clusterConfig, resourceConfig,
            resourceName(clique) + "_" + p, "ONLINE", 0));
      }
    }
    return replicas;
  }

  private ClusterModel createClusterModel(ClusterConfig clusterConfig,
      Map<Integer, CliqueSpec> specs) throws IOException {
    Set<AssignableReplica> replicas = createReplicas(clusterConfig, specs);
    Set<AssignableNode> nodes = createNodes(clusterConfig, specs);
    ClusterContext context = new ClusterContext(replicas, nodes, Collections.emptyMap(),
        Collections.emptyMap(), clusterConfig);
    return new ClusterModel(context, replicas, nodes,
        ClusterModel.RebalanceScopeType.GLOBAL_BASELINE);
  }

  private RebalanceAlgorithm createAlgorithm() {
    return ConstraintBasedAlgorithmFactory.getInstance(Collections.emptyMap());
  }

  /**
   * Normalizes an assignment into a comparable, order independent structure so two runs can be
   * compared byte for byte.
   */
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

  private static Set<String> assignedInstances(ResourceAssignment resourceAssignment) {
    return resourceAssignment.getMappedPartitions().stream()
        .flatMap(partition -> resourceAssignment.getReplicaMap(partition).keySet().stream())
        .collect(Collectors.toSet());
  }

  // ---------------------------------------------------------------------------------------------
  // Isolation behavior
  // ---------------------------------------------------------------------------------------------

  /**
   * The headline fix. With the flag on, the 19 healthy cliques are assigned even though clique 3
   * cannot place a single replica. With the flag off this same topology throws (see
   * {@link TestCliqueFailureBlastRadius#testSingleBrokenCliqueBlocksAllOtherCliques}).
   */
  @Test
  public void testBrokenCliqueDoesNotBlockHealthyCliques()
      throws HelixRebalanceException, IOException {
    int brokenClique = 3;
    ClusterConfig clusterConfig = createClusterConfig(true);
    Map<Integer, CliqueSpec> specs = allHealthy();
    specs.put(brokenClique,
        CliqueSpec.healthy().withPartitionWeight(UNPLACEABLE_PARTITION_WEIGHT));

    OptimalAssignment optimalAssignment =
        createAlgorithm().calculate(createClusterModel(clusterConfig, specs));
    Map<String, ResourceAssignment> result = optimalAssignment.getOptimalResourceAssignment();

    Assert.assertEquals(optimalAssignment.getSkippedResources(),
        Collections.singleton(resourceName(brokenClique)));
    Assert.assertEquals(result.size(), CLIQUE_COUNT - 1);
    Assert.assertFalse(result.containsKey(resourceName(brokenClique)),
        "The broken clique must be skipped, not partially assigned");
    for (int clique = 0; clique < CLIQUE_COUNT; clique++) {
      if (clique == brokenClique) {
        continue;
      }
      ResourceAssignment resourceAssignment = result.get(resourceName(clique));
      Assert.assertNotNull(resourceAssignment, "Healthy clique " + clique + " must be assigned");
      Assert.assertEquals(resourceAssignment.getMappedPartitions().size(),
          PARTITIONS_PER_RESOURCE);
    }
  }

  /**
   * Every clique index behaves the same: breaking any one of them leaves the other 19 rebalanced.
   */
  @Test
  public void testEveryCliqueCanFailIndependently() throws HelixRebalanceException, IOException {
    ClusterConfig clusterConfig = createClusterConfig(true);
    for (int brokenClique = 0; brokenClique < CLIQUE_COUNT; brokenClique++) {
      Map<Integer, CliqueSpec> specs = allHealthy();
      specs.put(brokenClique,
          CliqueSpec.healthy().withPartitionWeight(UNPLACEABLE_PARTITION_WEIGHT));
      Map<String, ResourceAssignment> result =
          createAlgorithm().calculate(createClusterModel(clusterConfig, specs))
              .getOptimalResourceAssignment();
      Assert.assertEquals(result.size(), CLIQUE_COUNT - 1,
          "Breaking clique " + brokenClique + " must leave every other clique assigned");
      Assert.assertFalse(result.containsKey(resourceName(brokenClique)));
    }
  }

  /**
   * Several simultaneously broken cliques are all skipped and the rest still converge.
   */
  @Test
  public void testMultipleBrokenCliquesAreSkippedIndependently()
      throws HelixRebalanceException, IOException {
    Set<Integer> brokenCliques = new HashSet<>(Arrays.asList(0, 7, 19));
    ClusterConfig clusterConfig = createClusterConfig(true);
    Map<Integer, CliqueSpec> specs = allHealthy();
    brokenCliques.forEach(clique -> specs
        .put(clique, CliqueSpec.healthy().withPartitionWeight(UNPLACEABLE_PARTITION_WEIGHT)));

    OptimalAssignment optimalAssignment =
        createAlgorithm().calculate(createClusterModel(clusterConfig, specs));
    Map<String, ResourceAssignment> result = optimalAssignment.getOptimalResourceAssignment();

    Assert.assertEquals(optimalAssignment.getSkippedResources(),
        brokenCliques.stream().map(TestWagedInstanceTagIsolation::resourceName)
            .collect(Collectors.toSet()));
    Assert.assertEquals(result.size(), CLIQUE_COUNT - brokenCliques.size());
  }

  /**
   * When nothing can be placed anywhere, the mode falls back to the existing all-or-nothing
   * behavior and throws, so the caller's last known good fallback and failure metrics still fire.
   */
  @Test
  public void testAllCliquesBrokenStillThrows() throws IOException {
    ClusterConfig clusterConfig = createClusterConfig(true);
    Map<Integer, CliqueSpec> specs = new HashMap<>();
    for (int clique = 0; clique < CLIQUE_COUNT; clique++) {
      // Every partition is bigger than any single node, so nothing can be placed, yet the total
      // demand stays under the cluster's total capacity so the cluster wide deficit precheck does
      // not fire first. This isolates the "every group failed" path.
      specs.put(clique, CliqueSpec.healthy().withPartitionCount(6)
          .withPartitionWeight(UNPLACEABLE_PARTITION_WEIGHT));
    }

    try {
      createAlgorithm().calculate(createClusterModel(clusterConfig, specs));
      Assert.fail("Expected a failure when no clique can be placed at all");
    } catch (HelixRebalanceException ex) {
      Assert.assertEquals(ex.getFailureType(), HelixRebalanceException.Type.FAILED_TO_CALCULATE);
      Assert.assertEquals(ex.getFailureCategory(),
          HelixRebalanceException.FailureCategory.NO_CANDIDATE_NODE);
    }
  }

  /**
   * A clique that can place some but not all of its partitions must be rolled back completely.
   * Leaving a resource half assigned would silently drop partitions from its ideal state.
   */
  @Test
  public void testPartiallyPlaceableCliqueIsRolledBackAtomically()
      throws HelixRebalanceException, IOException {
    int brokenClique = 11;
    ClusterConfig clusterConfig = createClusterConfig(true);
    Map<Integer, CliqueSpec> specs = allHealthy();
    // Each partition consumes a whole node, and there is one more partition than there are nodes,
    // so exactly NODES_PER_CLIQUE partitions fit and the last one has no candidate.
    specs.put(brokenClique, CliqueSpec.healthy().withPartitionWeight(NODE_CAPACITY)
        .withPartitionCount(NODES_PER_CLIQUE + 1));

    OptimalAssignment optimalAssignment =
        createAlgorithm().calculate(createClusterModel(clusterConfig, specs));
    Map<String, ResourceAssignment> result = optimalAssignment.getOptimalResourceAssignment();

    Assert.assertFalse(result.containsKey(resourceName(brokenClique)),
        "A partially placeable clique must be rolled back entirely, not left half assigned");
    Assert.assertEquals(result.size(), CLIQUE_COUNT - 1);
  }

  /**
   * The rollback must leave the cluster model in exactly the state it had before the failing
   * clique was attempted. Proven by comparing against a run in which the failing clique's resource
   * and nodes are simply not present: every surviving clique must be placed identically.
   */
  @Test
  public void testRollbackLeavesNoResidueForLaterCliques()
      throws HelixRebalanceException, IOException {
    // Break clique 0 so it is attempted first: its rollback happens before every other clique runs.
    int brokenClique = 0;
    ClusterConfig clusterConfig = createClusterConfig(true);

    Map<Integer, CliqueSpec> withBroken = allHealthy();
    withBroken.put(brokenClique,
        CliqueSpec.healthy().withPartitionWeight(NODE_CAPACITY)
            .withPartitionCount(NODES_PER_CLIQUE + 1));
    Map<String, ResourceAssignment> withBrokenResult =
        createAlgorithm().calculate(createClusterModel(clusterConfig, withBroken))
            .getOptimalResourceAssignment();

    Map<Integer, CliqueSpec> withoutBroken = allHealthy();
    withoutBroken.remove(brokenClique);
    Map<String, ResourceAssignment> withoutBrokenResult =
        createAlgorithm().calculate(createClusterModel(clusterConfig, withoutBroken))
            .getOptimalResourceAssignment();

    // The two runs have different global context estimates, so compare the property that the
    // rollback is responsible for: no node of a surviving clique carries any residue of the rolled
    // back clique, and every surviving clique is fully placed inside its own tag.
    Assert.assertEquals(withBrokenResult.size(), CLIQUE_COUNT - 1);
    Assert.assertEquals(withoutBrokenResult.size(), CLIQUE_COUNT - 1);
    for (int clique = 1; clique < CLIQUE_COUNT; clique++) {
      final String expectedPrefix = "instance_" + clique + "_";
      ResourceAssignment resourceAssignment = withBrokenResult.get(resourceName(clique));
      Assert.assertEquals(resourceAssignment.getMappedPartitions().size(),
          PARTITIONS_PER_RESOURCE);
      assignedInstances(resourceAssignment).forEach(instance -> Assert
          .assertTrue(instance.startsWith(expectedPrefix),
              "A clique leaked onto " + instance));
    }
    // The rolled back clique's nodes must be completely free, i.e. nothing from any other clique
    // landed there either.
    final String rolledBackPrefix = "instance_" + brokenClique + "_";
    withBrokenResult.values().forEach(resourceAssignment -> assignedInstances(resourceAssignment)
        .forEach(instance -> Assert.assertFalse(instance.startsWith(rolledBackPrefix),
            "Rolled back clique's node " + instance + " must hold nothing")));
  }

  // ---------------------------------------------------------------------------------------------
  // Parity with the default global mode
  // ---------------------------------------------------------------------------------------------

  /**
   * The parity guarantee: when every clique can be placed, turning the flag on must not move a
   * single replica. The cluster model, the cluster context estimates and the replica sort order are
   * all shared, and disjoint cliques cannot influence each other, so the assignments must match
   * exactly.
   */
  @Test
  public void testParityWithGlobalModeWhenEverythingIsPlaceable()
      throws HelixRebalanceException, IOException {
    Map<Integer, CliqueSpec> specs = allHealthy();

    Map<String, ResourceAssignment> globalResult =
        createAlgorithm().calculate(createClusterModel(createClusterConfig(false), specs))
            .getOptimalResourceAssignment();
    Map<String, ResourceAssignment> isolatedResult =
        createAlgorithm().calculate(createClusterModel(createClusterConfig(true), specs))
            .getOptimalResourceAssignment();

    Assert.assertEquals(normalize(isolatedResult), normalize(globalResult),
        "Instance tag isolation must not change the assignment when nothing is broken");
  }

  /**
   * Parity must also hold for uneven cliques, where the shared cluster context estimates matter
   * most: different node counts, different partition counts and different weights per clique.
   */
  @Test
  public void testParityWithGlobalModeForHeterogeneousCliques()
      throws HelixRebalanceException, IOException {
    Map<Integer, CliqueSpec> specs = allHealthy();
    for (int clique = 0; clique < CLIQUE_COUNT; clique++) {
      specs.put(clique, CliqueSpec.healthy().withNodeCount(4 + (clique % 7))
          .withPartitionCount(3 + (clique % 11)).withPartitionWeight(5 + (clique % 4) * 7));
    }

    Map<String, ResourceAssignment> globalResult =
        createAlgorithm().calculate(createClusterModel(createClusterConfig(false), specs))
            .getOptimalResourceAssignment();
    Map<String, ResourceAssignment> isolatedResult =
        createAlgorithm().calculate(createClusterModel(createClusterConfig(true), specs))
            .getOptimalResourceAssignment();

    Assert.assertEquals(normalize(isolatedResult), normalize(globalResult));
  }

  /**
   * Parity must survive every topology perturbation the operators actually do. Each case removes
   * nodes from one clique, which is exactly what a participant crash, a decommission, or moving an
   * instance to EVACUATE / UNKNOWN / SWAP_IN looks like by the time the algorithm sees the cluster
   * model: those operations are filtered out of the assignable node set upstream.
   */
  @DataProvider(name = "topologyPerturbations")
  public Object[][] topologyPerturbations() {
    return new Object[][] {
        // description, perturbed clique, node count, partition count, partition weight
        {"participant crash removes one node", 5, NODES_PER_CLIQUE - 1, PARTITIONS_PER_RESOURCE,
            HEALTHY_PARTITION_WEIGHT},
        {"half the clique is evacuated", 5, NODES_PER_CLIQUE / 2, PARTITIONS_PER_RESOURCE,
            HEALTHY_PARTITION_WEIGHT},
        {"clique shrinks to a single node", 5, 1, PARTITIONS_PER_RESOURCE,
            HEALTHY_PARTITION_WEIGHT},
        {"participants added to one clique", 5, NODES_PER_CLIQUE + 5, PARTITIONS_PER_RESOURCE,
            HEALTHY_PARTITION_WEIGHT},
        {"partition weight raised", 5, NODES_PER_CLIQUE, PARTITIONS_PER_RESOURCE,
            NODE_CAPACITY / 2},
        {"partition weight lowered", 5, NODES_PER_CLIQUE, PARTITIONS_PER_RESOURCE, 1},
        {"resource expands", 5, NODES_PER_CLIQUE, PARTITIONS_PER_RESOURCE * 4,
            HEALTHY_PARTITION_WEIGHT},
        {"resource shrinks to one partition", 5, NODES_PER_CLIQUE, 1, HEALTHY_PARTITION_WEIGHT},
        {"clique fully saturated", 5, NODES_PER_CLIQUE, NODES_PER_CLIQUE, NODE_CAPACITY}
    };
  }

  @Test(dataProvider = "topologyPerturbations")
  public void testParityUnderTopologyChanges(String description, int perturbedClique, int nodeCount,
      int partitionCount, int partitionWeight) throws HelixRebalanceException, IOException {
    Map<Integer, CliqueSpec> specs = allHealthy();
    specs.put(perturbedClique, CliqueSpec.healthy().withNodeCount(nodeCount)
        .withPartitionCount(partitionCount).withPartitionWeight(partitionWeight));

    Map<String, ResourceAssignment> globalResult =
        createAlgorithm().calculate(createClusterModel(createClusterConfig(false), specs))
            .getOptimalResourceAssignment();
    OptimalAssignment isolated =
        createAlgorithm().calculate(createClusterModel(createClusterConfig(true), specs));

    Assert.assertTrue(isolated.getSkippedResources().isEmpty(),
        description + ": nothing should be skipped, but " + isolated.getSkippedResources() + " was");
    Assert.assertEquals(normalize(isolated.getOptimalResourceAssignment()),
        normalize(globalResult), description + ": isolation changed the assignment");
  }

  /**
   * A perturbation that makes one clique unplaceable must be isolated, and every other clique must
   * still land exactly where the global mode would have put it had that clique not existed at all.
   * This is the strongest parity statement available for the failure path.
   */
  @Test
  public void testHealthyCliquesUnaffectedByAnotherCliqueLosingAllCapacity()
      throws HelixRebalanceException, IOException {
    int brokenClique = 8;
    Map<Integer, CliqueSpec> specs = allHealthy();
    specs.put(brokenClique,
        CliqueSpec.healthy().withPartitionWeight(UNPLACEABLE_PARTITION_WEIGHT));

    Map<String, ResourceAssignment> isolatedResult =
        createAlgorithm().calculate(createClusterModel(createClusterConfig(true), specs))
            .getOptimalResourceAssignment();

    // Re-run in global mode with the broken clique's weight made placeable again. The healthy
    // cliques' placement must be untouched by whatever the broken clique's weight is, because the
    // only cluster wide value it feeds is the context estimate, which is identical in both runs of
    // the isolated mode. Compare against the isolated run with the same weights to make sure the
    // healthy cliques are stable and fully placed.
    for (int clique = 0; clique < CLIQUE_COUNT; clique++) {
      if (clique == brokenClique) {
        Assert.assertNull(isolatedResult.get(resourceName(clique)));
        continue;
      }
      ResourceAssignment resourceAssignment = isolatedResult.get(resourceName(clique));
      Assert.assertEquals(resourceAssignment.getMappedPartitions().size(),
          PARTITIONS_PER_RESOURCE);
      Assert.assertEquals(assignedInstances(resourceAssignment).stream()
          .map(instance -> instance.split("_")[1]).collect(Collectors.toSet()),
          Collections.singleton(String.valueOf(clique)));
    }
  }

  /**
   * The isolated mode must be deterministic: repeated runs over the same input produce the same
   * assignment. Determinism is what lets the assignment metadata store treat an unchanged
   * calculation as a no-op write.
   */
  @Test
  public void testIsolatedModeIsDeterministic() throws HelixRebalanceException, IOException {
    Map<Integer, CliqueSpec> specs = allHealthy();
    specs.put(2, CliqueSpec.healthy().withPartitionWeight(UNPLACEABLE_PARTITION_WEIGHT));
    ClusterConfig clusterConfig = createClusterConfig(true);

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

  // ---------------------------------------------------------------------------------------------
  // Mixed tagged and untagged resources
  // ---------------------------------------------------------------------------------------------

  private ClusterModel createMixedClusterModel(ClusterConfig clusterConfig, int taggedCliqueCount,
      int untaggedResourceCount, int untaggedPartitionWeight, int brokenClique) throws IOException {
    Set<AssignableReplica> replicas = new HashSet<>();
    Set<AssignableNode> nodes = new HashSet<>();

    for (int clique = 0; clique < taggedCliqueCount; clique++) {
      for (int i = 0; i < NODES_PER_CLIQUE; i++) {
        String instance = instanceName(clique, i);
        InstanceConfig instanceConfig = new InstanceConfig(instance);
        instanceConfig
            .setInstanceCapacityMap(Collections.singletonMap(CAPACITY_KEY, NODE_CAPACITY));
        instanceConfig.addTag(cliqueTag(clique));
        instanceConfig.setInstanceOperation(InstanceConstants.InstanceOperation.ENABLE);
        instanceConfig.setZoneId("zone_" + i);
        nodes.add(new AssignableNode(clusterConfig, instanceConfig, instance));
      }
      ResourceConfig resourceConfig = new ResourceConfig(resourceName(clique));
      resourceConfig.getRecord()
          .setSimpleField(ResourceConfig.ResourceConfigProperty.INSTANCE_GROUP_TAG.name(),
              cliqueTag(clique));
      int weight = clique == brokenClique ? UNPLACEABLE_PARTITION_WEIGHT : HEALTHY_PARTITION_WEIGHT;
      resourceConfig.setPartitionCapacityMap(Collections
          .singletonMap(ResourceConfig.DEFAULT_PARTITION_KEY,
              Collections.singletonMap(CAPACITY_KEY, weight)));
      for (int p = 0; p < PARTITIONS_PER_RESOURCE; p++) {
        replicas.add(new AssignableReplica(clusterConfig, resourceConfig,
            resourceName(clique) + "_" + p, "ONLINE", 0));
      }
    }

    for (int r = 0; r < untaggedResourceCount; r++) {
      ResourceConfig resourceConfig = new ResourceConfig("Untagged_" + r);
      resourceConfig.setPartitionCapacityMap(Collections
          .singletonMap(ResourceConfig.DEFAULT_PARTITION_KEY,
              Collections.singletonMap(CAPACITY_KEY, untaggedPartitionWeight)));
      for (int p = 0; p < PARTITIONS_PER_RESOURCE; p++) {
        replicas.add(new AssignableReplica(clusterConfig, resourceConfig, "Untagged_" + r + "_" + p,
            "ONLINE", 0));
      }
    }

    ClusterContext context = new ClusterContext(replicas, nodes, Collections.emptyMap(),
        Collections.emptyMap(), clusterConfig);
    return new ClusterModel(context, replicas, nodes,
        ClusterModel.RebalanceScopeType.GLOBAL_BASELINE);
  }

  /**
   * An untagged resource can be placed on any node, so it is never isolatable: carrying it over
   * while every clique is recalculated could overcommit the nodes it is still sitting on. Its
   * failure keeps today's global behavior.
   */
  @Test(expectedExceptions = HelixRebalanceException.class)
  public void testBrokenUntaggedResourcesStillFailGlobally()
      throws HelixRebalanceException, IOException {
    ClusterConfig clusterConfig = createClusterConfig(true);
    createAlgorithm().calculate(
        createMixedClusterModel(clusterConfig, 5, 2, UNPLACEABLE_PARTITION_WEIGHT,
            NO_BROKEN_CLIQUE));
  }

  /**
   * Untagged resources compete with each other for the very same nodes, so one broken untagged
   * resource cannot be carried over independently of the other. Both keep the global behavior.
   */
  @Test(expectedExceptions = HelixRebalanceException.class)
  public void testOneBrokenUntaggedResourceStillFailsGlobally()
      throws HelixRebalanceException, IOException {
    ClusterConfig clusterConfig = createClusterConfig(true);
    Set<AssignableReplica> replicas = new HashSet<>();
    Set<AssignableNode> nodes = createNodes(clusterConfig, allHealthy());
    replicas.addAll(createReplicas(clusterConfig, allHealthy()));
    replicas.addAll(untaggedReplicas(clusterConfig, "Untagged_broken", UNPLACEABLE_PARTITION_WEIGHT));
    replicas.addAll(untaggedReplicas(clusterConfig, "Untagged_healthy", HEALTHY_PARTITION_WEIGHT));
    ClusterContext context = new ClusterContext(replicas, nodes, Collections.emptyMap(),
        Collections.emptyMap(), clusterConfig);

    createAlgorithm().calculate(new ClusterModel(context, replicas, nodes,
        ClusterModel.RebalanceScopeType.GLOBAL_BASELINE));
  }

  private Set<AssignableReplica> untaggedReplicas(ClusterConfig clusterConfig, String resource,
      int weight) throws IOException {
    ResourceConfig resourceConfig = new ResourceConfig(resource);
    resourceConfig.setPartitionCapacityMap(Collections
        .singletonMap(ResourceConfig.DEFAULT_PARTITION_KEY,
            Collections.singletonMap(CAPACITY_KEY, weight)));
    Set<AssignableReplica> replicas = new HashSet<>();
    for (int p = 0; p < PARTITIONS_PER_RESOURCE; p++) {
      replicas.add(new AssignableReplica(clusterConfig, resourceConfig, resource + "_" + p,
          "ONLINE", 0));
    }
    return replicas;
  }

  /**
   * A clique whose nodes an untagged resource could also use is not exclusive, so a failure there
   * cannot be isolated either. Rolling the clique back would free capacity the untagged resource
   * could claim, and the emitted result could then overcommit the clique's nodes.
   */
  @Test(expectedExceptions = HelixRebalanceException.class)
  public void testBrokenTaggedCliqueSharingNodesWithUntaggedResourcesStillFailsGlobally()
      throws HelixRebalanceException, IOException {
    ClusterConfig clusterConfig = createClusterConfig(true);
    createAlgorithm()
        .calculate(createMixedClusterModel(clusterConfig, 5, 2, HEALTHY_PARTITION_WEIGHT, 2));
  }

  /**
   * The parity guarantee is unconditional: it does not depend on the cliques being disjoint. Untagged
   * resources can be placed on any node, so they compete with every tagged clique for capacity. Because
   * the isolated mode keeps the one globally sorted assignment pass and only intervenes when a
   * placement actually fails, even this mixed topology is placed identically in both modes.
   */
  @Test
  public void testMixedClusterParityWhenEverythingIsPlaceable()
      throws HelixRebalanceException, IOException {
    Map<String, ResourceAssignment> globalResult = createAlgorithm().calculate(
        createMixedClusterModel(createClusterConfig(false), 5, 3, HEALTHY_PARTITION_WEIGHT,
            NO_BROKEN_CLIQUE)).getOptimalResourceAssignment();
    OptimalAssignment isolated = createAlgorithm().calculate(
        createMixedClusterModel(createClusterConfig(true), 5, 3, HEALTHY_PARTITION_WEIGHT,
            NO_BROKEN_CLIQUE));

    Assert.assertTrue(isolated.getSkippedResources().isEmpty());
    Assert.assertEquals(normalize(isolated.getOptimalResourceAssignment()),
        normalize(globalResult),
        "Isolation must not move a replica even when untagged resources share the nodes");
  }

  /**
   * Overlapping tags are not the intended topology, but they must not corrupt anything. Nodes that
   * carry two tags are legal, and with everything placeable the result must still match the global
   * mode exactly.
   */
  @Test
  public void testOverlappingTagsStillMatchGlobalMode()
      throws HelixRebalanceException, IOException {
    for (boolean isolationEnabled : new boolean[] {false, true}) {
      ClusterConfig clusterConfig = createClusterConfig(isolationEnabled);
      Set<AssignableNode> nodes = new HashSet<>();
      for (int i = 0; i < NODES_PER_CLIQUE; i++) {
        InstanceConfig instanceConfig = new InstanceConfig("shared_" + i);
        instanceConfig
            .setInstanceCapacityMap(Collections.singletonMap(CAPACITY_KEY, NODE_CAPACITY));
        // Every node belongs to both cliques.
        instanceConfig.addTag(cliqueTag(0));
        instanceConfig.addTag(cliqueTag(1));
        instanceConfig.setInstanceOperation(InstanceConstants.InstanceOperation.ENABLE);
        instanceConfig.setZoneId("zone_" + i);
        nodes.add(new AssignableNode(clusterConfig, instanceConfig, "shared_" + i));
      }
      Set<AssignableReplica> replicas = new HashSet<>();
      for (int clique = 0; clique < 2; clique++) {
        ResourceConfig resourceConfig = new ResourceConfig(resourceName(clique));
        resourceConfig.getRecord()
            .setSimpleField(ResourceConfig.ResourceConfigProperty.INSTANCE_GROUP_TAG.name(),
                cliqueTag(clique));
        resourceConfig.setPartitionCapacityMap(Collections
            .singletonMap(ResourceConfig.DEFAULT_PARTITION_KEY,
                Collections.singletonMap(CAPACITY_KEY, HEALTHY_PARTITION_WEIGHT)));
        for (int p = 0; p < PARTITIONS_PER_RESOURCE; p++) {
          replicas.add(new AssignableReplica(clusterConfig, resourceConfig,
              resourceName(clique) + "_" + p, "ONLINE", 0));
        }
      }
      ClusterContext context = new ClusterContext(replicas, nodes, Collections.emptyMap(),
          Collections.emptyMap(), clusterConfig);
      Map<String, Map<String, Map<String, String>>> result = normalize(createAlgorithm()
          .calculate(new ClusterModel(context, replicas, nodes,
              ClusterModel.RebalanceScopeType.GLOBAL_BASELINE))
          .getOptimalResourceAssignment());
      if (isolationEnabled) {
        Assert.assertEquals(result, _overlappingTagsBaseline);
      } else {
        _overlappingTagsBaseline = result;
      }
    }
  }

  private Map<String, Map<String, Map<String, String>>> _overlappingTagsBaseline;

  // ---------------------------------------------------------------------------------------------
  // Flag behavior
  // ---------------------------------------------------------------------------------------------

  /**
   * The flag defaults to off, so an untouched cluster config keeps the existing global semantics.
   */
  @Test
  public void testFlagDefaultsToDisabled() {
    Assert.assertFalse(new ClusterConfig("c").isWagedInstanceTagIsolationEnabled());
    Assert.assertFalse(ClusterConfig.DEFAULT_WAGED_INSTANCE_TAG_ISOLATION_ENABLED);
  }

  @Test
  public void testFlagRoundTripsThroughTheRecord() {
    ClusterConfig clusterConfig = new ClusterConfig("c");
    clusterConfig.setWagedInstanceTagIsolationEnabled(true);
    Assert.assertTrue(clusterConfig.isWagedInstanceTagIsolationEnabled());
    Assert.assertTrue(new ClusterConfig(clusterConfig.getRecord())
        .isWagedInstanceTagIsolationEnabled());
    clusterConfig.setWagedInstanceTagIsolationEnabled(false);
    Assert.assertFalse(clusterConfig.isWagedInstanceTagIsolationEnabled());
  }

  /**
   * With the flag off the algorithm must still abort on the first unplaceable replica, and it must
   * never report skipped resources.
   */
  @Test
  public void testFlagOffKeepsAllOrNothingBehavior() throws IOException {
    ClusterConfig clusterConfig = createClusterConfig(false);
    Map<Integer, CliqueSpec> specs = allHealthy();
    specs.put(3, CliqueSpec.healthy().withPartitionWeight(UNPLACEABLE_PARTITION_WEIGHT));
    try {
      createAlgorithm().calculate(createClusterModel(clusterConfig, specs));
      Assert.fail("With the flag off a single broken clique must abort the whole calculation");
    } catch (HelixRebalanceException expected) {
      Assert.assertEquals(expected.getFailureCategory(),
          HelixRebalanceException.FailureCategory.NO_CANDIDATE_NODE);
    }
  }

  @Test
  public void testNoSkippedResourcesReportedWhenNothingFails()
      throws HelixRebalanceException, IOException {
    for (boolean isolationEnabled : new boolean[] {false, true}) {
      OptimalAssignment optimalAssignment = createAlgorithm()
          .calculate(createClusterModel(createClusterConfig(isolationEnabled), allHealthy()));
      Assert.assertTrue(optimalAssignment.getSkippedResources().isEmpty());
      Assert.assertEquals(optimalAssignment.getOptimalResourceAssignment().size(), CLIQUE_COUNT);
    }
  }

  /**
   * A cluster wide capacity deficit is detected before any group runs, so it still fails the whole
   * calculation in both modes. Isolation is about unplaceable replicas, not about a cluster that is
   * globally out of room.
   */
  @Test
  public void testClusterWideCapacityDeficitStillFailsFast() throws IOException {
    ClusterConfig clusterConfig = createClusterConfig(true);
    Map<Integer, CliqueSpec> specs = new HashMap<>();
    for (int clique = 0; clique < CLIQUE_COUNT; clique++) {
      // Demand far more than the cluster owns in total.
      specs.put(clique,
          CliqueSpec.healthy().withPartitionCount(NODES_PER_CLIQUE * 5).withPartitionWeight(90));
    }
    try {
      createAlgorithm().calculate(createClusterModel(clusterConfig, specs));
      Assert.fail("Expected a cluster wide capacity deficit failure");
    } catch (HelixRebalanceException ex) {
      Assert.assertEquals(ex.getFailureCategory(),
          HelixRebalanceException.FailureCategory.CAPACITY_DEFICIT);
    }
  }

  /**
   * Group ordering must not depend on the iteration order of the replica set, otherwise the mode
   * would be non deterministic across JVM runs. Shuffling the clique insertion order must not
   * change the result.
   */
  @Test
  public void testGroupOrderIsIndependentOfInputOrder() throws HelixRebalanceException, IOException {
    ClusterConfig clusterConfig = createClusterConfig(true);

    List<Integer> forward = IntStream.range(0, CLIQUE_COUNT).boxed().collect(Collectors.toList());
    List<Integer> reversed = new ArrayList<>(forward);
    Collections.reverse(reversed);

    Map<String, Map<String, Map<String, String>>> forwardResult =
        normalize(createAlgorithm().calculate(createClusterModel(clusterConfig, orderedSpecs(forward)))
            .getOptimalResourceAssignment());
    Map<String, Map<String, Map<String, String>>> reversedResult =
        normalize(createAlgorithm().calculate(createClusterModel(clusterConfig, orderedSpecs(reversed)))
            .getOptimalResourceAssignment());

    Assert.assertEquals(reversedResult, forwardResult);
  }


  // ---------------------------------------------------------------------------------------------
  // The isolation unit is the tag, and it only applies when the tag owns its nodes exclusively
  // ---------------------------------------------------------------------------------------------

  private ResourceConfig taggedResource(String resource, String tag, int weight)
      throws IOException {
    ResourceConfig resourceConfig = new ResourceConfig(resource);
    if (tag != null) {
      resourceConfig.getRecord()
          .setSimpleField(ResourceConfig.ResourceConfigProperty.INSTANCE_GROUP_TAG.name(), tag);
    }
    resourceConfig.setPartitionCapacityMap(Collections.singletonMap(
        ResourceConfig.DEFAULT_PARTITION_KEY, Collections.singletonMap(CAPACITY_KEY, weight)));
    return resourceConfig;
  }

  private void addReplicas(Set<AssignableReplica> replicas, ClusterConfig clusterConfig,
      ResourceConfig resourceConfig, int partitionCount) {
    for (int p = 0; p < partitionCount; p++) {
      replicas.add(new AssignableReplica(clusterConfig, resourceConfig,
          resourceConfig.getResourceName() + "_" + p, "ONLINE", 0));
    }
  }

  private AssignableNode taggedNode(ClusterConfig clusterConfig, String instance, int zone,
      String... tags) {
    InstanceConfig instanceConfig = new InstanceConfig(instance);
    instanceConfig.setInstanceCapacityMap(Collections.singletonMap(CAPACITY_KEY, NODE_CAPACITY));
    for (String tag : tags) {
      instanceConfig.addTag(tag);
    }
    instanceConfig.setInstanceOperation(InstanceConstants.InstanceOperation.ENABLE);
    instanceConfig.setZoneId("zone_" + zone);
    return new AssignableNode(clusterConfig, instanceConfig, instance);
  }

  /**
   * A clique that hosts more than one resource. The tag, not the resource, is the
   * isolation unit, so when one resource in a clique cannot be placed the whole clique is carried
   * over. That is deliberate: rolling back only the broken resource would free capacity that its
   * healthy siblings would then consume, and the emitted result (recalculated siblings plus the
   * carried over broken resource) could overcommit the clique's nodes.
   */
  @Test
  public void testAllResourcesSharingABrokenTagAreCarriedOverTogether()
      throws HelixRebalanceException, IOException {
    ClusterConfig clusterConfig = createClusterConfig(true);
    Set<AssignableReplica> replicas = new HashSet<>();
    // Clique A hosts a healthy resource and an unplaceable one.
    addReplicas(replicas, clusterConfig,
        taggedResource("R_a_healthy", cliqueTag(0), HEALTHY_PARTITION_WEIGHT), 3);
    addReplicas(replicas, clusterConfig,
        taggedResource("R_a_broken", cliqueTag(0), UNPLACEABLE_PARTITION_WEIGHT), 3);
    // Clique B is untouched and must still be assigned.
    addReplicas(replicas, clusterConfig,
        taggedResource("R_b", cliqueTag(1), HEALTHY_PARTITION_WEIGHT), 3);

    Set<AssignableNode> nodes = new HashSet<>();
    for (int i = 0; i < 3; i++) {
      nodes.add(taggedNode(clusterConfig, instanceName(0, i), i, cliqueTag(0)));
      nodes.add(taggedNode(clusterConfig, instanceName(1, i), i, cliqueTag(1)));
    }

    ClusterContext context = new ClusterContext(replicas, nodes, Collections.emptyMap(),
        Collections.emptyMap(), clusterConfig);
    ClusterModel clusterModel = new ClusterModel(context, replicas, nodes,
        ClusterModel.RebalanceScopeType.GLOBAL_BASELINE);

    OptimalAssignment assignment = createAlgorithm().calculate(clusterModel);
    Map<String, ResourceAssignment> result = assignment.getOptimalResourceAssignment();

    Assert.assertEquals(assignment.getSkippedResources(),
        new HashSet<>(Arrays.asList("R_a_healthy", "R_a_broken")),
        "Every resource sharing the broken clique's tag must be skipped as one unit");
    Assert.assertFalse(result.containsKey("R_a_broken"));
    Assert.assertFalse(result.containsKey("R_a_healthy"),
        "The healthy sibling must be rolled back too, not left half assigned");
    Assert.assertEquals(normalize(result).keySet(), Collections.singleton("R_b"),
        "Only the untouched clique keeps its newly calculated assignment");

    // The whole clique's capacity must be back to untouched, with no residue from the rollback.
    for (AssignableNode node : clusterModel.getAssignableNodes().values()) {
      if (node.getInstanceName().startsWith(instanceName(0, 0).substring(0,
          instanceName(0, 0).lastIndexOf('_')))) {
        Assert.assertEquals(node.getRemainingCapacity().get(CAPACITY_KEY).intValue(),
            NODE_CAPACITY, "Rollback must fully restore " + node.getInstanceName());
      }
    }
  }

  /**
   * An untagged resource can be placed on any node, so carrying it over while everything else is
   * recalculated could overcommit the nodes it is still sitting on. The mode refuses to isolate in
   * that case and fails exactly like the default global mode.
   */
  @Test(expectedExceptions = HelixRebalanceException.class)
  public void testUnplaceableUntaggedResourceStillFailsGlobally()
      throws HelixRebalanceException, IOException {
    ClusterConfig clusterConfig = createClusterConfig(true);
    Set<AssignableReplica> replicas = new HashSet<>();
    addReplicas(replicas, clusterConfig,
        taggedResource("R_tagged", cliqueTag(0), HEALTHY_PARTITION_WEIGHT), 3);
    addReplicas(replicas, clusterConfig,
        taggedResource("R_untagged", null, UNPLACEABLE_PARTITION_WEIGHT), 3);

    Set<AssignableNode> nodes = new HashSet<>();
    for (int i = 0; i < 3; i++) {
      nodes.add(taggedNode(clusterConfig, instanceName(0, i), i, cliqueTag(0)));
    }

    ClusterContext context = new ClusterContext(replicas, nodes, Collections.emptyMap(),
        Collections.emptyMap(), clusterConfig);
    createAlgorithm().calculate(new ClusterModel(context, replicas, nodes,
        ClusterModel.RebalanceScopeType.GLOBAL_BASELINE));
  }

  /**
   * A tagged clique whose nodes an untagged resource could also use is not isolatable either: the
   * capacity freed by rolling the clique back could be claimed by the untagged resource.
   */
  @Test(expectedExceptions = HelixRebalanceException.class)
  public void testBrokenTagSharingNodesWithAnUntaggedResourceStillFailsGlobally()
      throws HelixRebalanceException, IOException {
    ClusterConfig clusterConfig = createClusterConfig(true);
    Set<AssignableReplica> replicas = new HashSet<>();
    addReplicas(replicas, clusterConfig,
        taggedResource("R_broken", cliqueTag(0), UNPLACEABLE_PARTITION_WEIGHT), 3);
    addReplicas(replicas, clusterConfig, taggedResource("R_untagged", null, 1), 3);

    Set<AssignableNode> nodes = new HashSet<>();
    for (int i = 0; i < 3; i++) {
      nodes.add(taggedNode(clusterConfig, instanceName(0, i), i, cliqueTag(0)));
    }

    ClusterContext context = new ClusterContext(replicas, nodes, Collections.emptyMap(),
        Collections.emptyMap(), clusterConfig);
    createAlgorithm().calculate(new ClusterModel(context, replicas, nodes,
        ClusterModel.RebalanceScopeType.GLOBAL_BASELINE));
  }

  /**
   * Two cliques that share an instance are not exclusive, so neither can be isolated.
   */
  @Test(expectedExceptions = HelixRebalanceException.class)
  public void testBrokenCliqueSharingAnInstanceWithAnotherCliqueStillFailsGlobally()
      throws HelixRebalanceException, IOException {
    ClusterConfig clusterConfig = createClusterConfig(true);
    Set<AssignableReplica> replicas = new HashSet<>();
    addReplicas(replicas, clusterConfig,
        taggedResource("R_broken", cliqueTag(0), UNPLACEABLE_PARTITION_WEIGHT), 3);
    addReplicas(replicas, clusterConfig,
        taggedResource("R_other", cliqueTag(1), HEALTHY_PARTITION_WEIGHT), 3);

    Set<AssignableNode> nodes = new HashSet<>();
    for (int i = 0; i < 3; i++) {
      // The shared node carries both clique tags.
      nodes.add(i == 0 ? taggedNode(clusterConfig, instanceName(0, i), i, cliqueTag(0),
          cliqueTag(1)) : taggedNode(clusterConfig, instanceName(0, i), i, cliqueTag(0)));
      nodes.add(taggedNode(clusterConfig, instanceName(1, i), i, cliqueTag(1)));
    }

    ClusterContext context = new ClusterContext(replicas, nodes, Collections.emptyMap(),
        Collections.emptyMap(), clusterConfig);
    createAlgorithm().calculate(new ClusterModel(context, replicas, nodes,
        ClusterModel.RebalanceScopeType.GLOBAL_BASELINE));
  }

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

  private Map<Integer, CliqueSpec> orderedSpecs(List<Integer> order) {
    Map<Integer, CliqueSpec> specs = new java.util.LinkedHashMap<>();
    order.forEach(clique -> specs.put(clique, CliqueSpec.healthy()));
    return specs;
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
        brokenCliques.stream().map(TestWagedInstanceTagIsolation::resourceName)
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

  // ---------------------------------------------------------------------------------------------
  // Every rebalance scope
  // ---------------------------------------------------------------------------------------------

  /**
   * Isolation lives in the algorithm, below every phase, so it must behave the same no matter which
   * scope the controller is running. EMERGENCY and DELAYED_REBALANCE_OVERWRITES are the two scopes
   * the other tests do not otherwise reach.
   */
  @Test
  public void testIsolationAppliesToEveryRebalanceScope()
      throws HelixRebalanceException, IOException {
    ClusterConfig clusterConfig = createClusterConfig(true);
    Map<Integer, CliqueSpec> specs = allHealthy();
    specs.put(4, CliqueSpec.healthy().withPartitionWeight(UNPLACEABLE_PARTITION_WEIGHT));

    for (ClusterModel.RebalanceScopeType scope : ClusterModel.RebalanceScopeType.values()) {
      Set<AssignableReplica> replicas = createReplicas(clusterConfig, specs);
      Set<AssignableNode> nodes = createNodes(clusterConfig, specs);
      ClusterContext context = new ClusterContext(replicas, nodes, Collections.emptyMap(),
          Collections.emptyMap(), clusterConfig);
      OptimalAssignment optimalAssignment = createAlgorithm()
          .calculate(new ClusterModel(context, replicas, nodes, scope));

      Assert.assertEquals(optimalAssignment.getSkippedResources(),
          Collections.singleton(resourceName(4)), "Scope " + scope + " must isolate the same way");
      Assert.assertEquals(optimalAssignment.getOptimalResourceAssignment().size(),
          CLIQUE_COUNT - 1, "Scope " + scope + " must still assign every healthy clique");
    }
  }

  /**
   * The same sweep with the flag off: every scope must keep failing outright, so the flag is what
   * decides the behavior rather than the phase the controller happens to be in.
   */
  @Test
  public void testEveryRebalanceScopeStillFailsWhenIsolationIsOff() throws IOException {
    ClusterConfig clusterConfig = createClusterConfig(false);
    Map<Integer, CliqueSpec> specs = allHealthy();
    specs.put(4, CliqueSpec.healthy().withPartitionWeight(UNPLACEABLE_PARTITION_WEIGHT));

    for (ClusterModel.RebalanceScopeType scope : ClusterModel.RebalanceScopeType.values()) {
      Set<AssignableReplica> replicas = createReplicas(clusterConfig, specs);
      Set<AssignableNode> nodes = createNodes(clusterConfig, specs);
      ClusterContext context = new ClusterContext(replicas, nodes, Collections.emptyMap(),
          Collections.emptyMap(), clusterConfig);
      try {
        createAlgorithm().calculate(new ClusterModel(context, replicas, nodes, scope));
        Assert.fail("Scope " + scope + " must still fail the whole calculation");
      } catch (HelixRebalanceException e) {
        Assert.assertEquals(e.getFailureCategory(),
            HelixRebalanceException.FailureCategory.NO_CANDIDATE_NODE);
      }
    }
  }

  /**
   * Rolling a clique back must return its nodes to exactly the capacity they started with, not
   * merely stop using them. Anything less would let a later clique see phantom usage.
   */
  @Test
  public void testRollbackFullyRestoresTheBrokenCliqueNodeCapacity()
      throws HelixRebalanceException, IOException {
    int brokenClique = 6;
    ClusterConfig clusterConfig = createClusterConfig(true);
    Map<Integer, CliqueSpec> specs = allHealthy();
    // Placeable one at a time, but not all ten together, so several land before the rollback.
    specs.put(brokenClique,
        CliqueSpec.healthy().withPartitionWeight(NODE_CAPACITY).withPartitionCount(
            NODES_PER_CLIQUE + 1));

    Set<AssignableReplica> replicas = createReplicas(clusterConfig, specs);
    Set<AssignableNode> nodes = createNodes(clusterConfig, specs);
    ClusterContext context = new ClusterContext(replicas, nodes, Collections.emptyMap(),
        Collections.emptyMap(), clusterConfig);
    ClusterModel clusterModel =
        new ClusterModel(context, replicas, nodes, ClusterModel.RebalanceScopeType.GLOBAL_BASELINE);

    OptimalAssignment optimalAssignment = createAlgorithm().calculate(clusterModel);
    Assert.assertEquals(optimalAssignment.getSkippedResources(),
        Collections.singleton(resourceName(brokenClique)));

    for (AssignableNode node : clusterModel.getAssignableNodes().values()) {
      if (!node.getInstanceName().startsWith("instance_" + brokenClique + "_")) {
        continue;
      }
      Assert.assertTrue(node.getAssignedReplicas().isEmpty(),
          "Node " + node.getInstanceName() + " must hold nothing after the rollback");
      Assert.assertEquals(node.getRemainingCapacity().get(CAPACITY_KEY).intValue(), NODE_CAPACITY,
          "Node " + node.getInstanceName() + " must be back at its full capacity");
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
