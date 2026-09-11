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

import org.apache.helix.HelixRebalanceException;
import org.apache.helix.constants.InstanceConstants;
import org.apache.helix.controller.rebalancer.waged.RebalanceAlgorithm;
import org.apache.helix.controller.rebalancer.waged.constraints.ConstraintBasedAlgorithmFactory;
import org.apache.helix.model.ClusterConfig;
import org.apache.helix.model.InstanceConfig;
import org.apache.helix.model.ResourceConfig;
import org.testng.Assert;
import org.testng.annotations.Test;

/**
 * Reproduces the clique partitioned topology in which the cluster is partitioned into independent
 * "cliques": every instance carries a clique tag, and every resource is pinned to exactly one
 * clique tag via {@link ResourceConfig#setInstanceGroupTag}. Cliques share no nodes, so they are
 * fully disjoint failure domains from the operator's point of view.
 *
 * These tests demonstrate that WAGED does NOT honor that isolation. The rebalance algorithm builds
 * a single global cluster model and assigns every replica in one loop, so a single unplaceable
 * replica in one clique aborts the entire calculation and no clique gets a new assignment.
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

  private static String cliqueTag(int clique) {
    return "clique_" + clique;
  }

  private static String resourceName(int clique) {
    return "Resource_clique_" + clique;
  }

  private ClusterConfig createClusterConfig() {
    ClusterConfig clusterConfig = new ClusterConfig("CliquePartitionedCluster");
    clusterConfig.setInstanceCapacityKeys(Collections.singletonList(CAPACITY_KEY));
    clusterConfig.setDefaultPartitionWeightMap(Collections.singletonMap(CAPACITY_KEY, 0));
    clusterConfig.setDisabledInstances(Collections.emptyMap());
    return clusterConfig;
  }

  /**
   * 20 cliques x 10 nodes = 200 instances. Each instance is tagged with exactly one clique tag.
   */
  private Set<AssignableNode> createNodes(ClusterConfig clusterConfig) {
    Set<AssignableNode> nodes = new HashSet<>();
    for (int clique = 0; clique < CLIQUE_COUNT; clique++) {
      for (int i = 0; i < NODES_PER_CLIQUE; i++) {
        String instanceName = "instance_" + clique + "_" + i;
        InstanceConfig instanceConfig = new InstanceConfig(instanceName);
        instanceConfig
            .setInstanceCapacityMap(Collections.singletonMap(CAPACITY_KEY, NODE_CAPACITY));
        instanceConfig.addTag(cliqueTag(clique));
        instanceConfig.setInstanceOperation(InstanceConstants.InstanceOperation.ENABLE);
        // Spread nodes of the same clique across zones so FaultZoneAwareConstraint is satisfied.
        instanceConfig.setZoneId("zone_" + i);
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
      ResourceConfig resourceConfig = new ResourceConfig(resourceName(clique));
      resourceConfig.getRecord()
          .setSimpleField(ResourceConfig.ResourceConfigProperty.INSTANCE_GROUP_TAG.name(),
              cliqueTag(clique));
      resourceConfig.setPartitionCapacityMap(Collections
          .singletonMap(ResourceConfig.DEFAULT_PARTITION_KEY,
              Collections.singletonMap(CAPACITY_KEY, weight)));
      for (int p = 0; p < PARTITIONS_PER_RESOURCE; p++) {
        replicas.add(new AssignableReplica(clusterConfig, resourceConfig,
            resourceName(clique) + "_" + p, "ONLINE", 0));
      }
    }
    return replicas;
  }

  private ClusterModel createClusterModel(ClusterConfig clusterConfig, int brokenClique)
      throws IOException {
    Set<AssignableReplica> replicas = createReplicas(clusterConfig, brokenClique);
    Set<AssignableNode> nodes = createNodes(clusterConfig);
    ClusterContext context = new ClusterContext(replicas, nodes, Collections.emptyMap(),
        Collections.emptyMap(), clusterConfig);
    return new ClusterModel(context, replicas, nodes, ClusterModel.RebalanceScopeType.GLOBAL_BASELINE);
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
  public void testAllCliquesHealthyProducesFullAssignment() throws HelixRebalanceException, IOException {
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
   * The core finding: breaking a single clique aborts the whole global calculation. Even though
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
}
