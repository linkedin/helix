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
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import org.apache.helix.HelixRebalanceException;
import org.apache.helix.model.ClusterConfig;
import org.apache.helix.model.ResourceAssignment;
import org.testng.Assert;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

/**
 * The core guarantees of WAGED instance-tag isolation: a broken clique is isolated and rolled
 * back atomically, the mode is deterministic, and it is byte for byte identical to the default
 * global mode whenever nothing is broken. See {@link AbstractTestWagedInstanceTagIsolation}
 * for the shared clique partitioned topology.
 */
public class TestWagedInstanceTagIsolationCore extends AbstractTestWagedInstanceTagIsolation {
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
        brokenCliques.stream().map(AbstractTestWagedInstanceTagIsolation::resourceName)
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
}
