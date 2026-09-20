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
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import org.apache.helix.HelixRebalanceException;
import org.apache.helix.constants.InstanceConstants;
import org.apache.helix.model.ClusterConfig;
import org.apache.helix.model.InstanceConfig;
import org.apache.helix.model.ResourceAssignment;
import org.apache.helix.model.ResourceConfig;
import org.testng.Assert;
import org.testng.annotations.Test;

/**
 * Behavioral contract of WAGED instance-tag isolation: the cluster-config flag, deterministic
 * group ordering, whole-tag carry-forward, every rebalance scope, and the untagged and
 * overlapping-tag topologies that are not isolatable and must still fail exactly like the
 * default global mode. See {@link AbstractTestWagedInstanceTagIsolation} for the shared
 * clique partitioned topology.
 */
public class TestWagedInstanceTagIsolationBehavior extends AbstractTestWagedInstanceTagIsolation {
  private static final int NO_BROKEN_CLIQUE = -1;

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

  private Map<Integer, CliqueSpec> orderedSpecs(List<Integer> order) {
    Map<Integer, CliqueSpec> specs = new java.util.LinkedHashMap<>();
    order.forEach(clique -> specs.put(clique, CliqueSpec.healthy()));
    return specs;
  }

  // ---------------------------------------------------------------------------------------------
  // The isolation unit is the tag, and it only applies when the tag owns its nodes exclusively
  // ---------------------------------------------------------------------------------------------

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
}
