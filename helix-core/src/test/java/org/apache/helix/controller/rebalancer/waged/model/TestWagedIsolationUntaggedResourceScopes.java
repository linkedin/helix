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
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import org.apache.helix.HelixConstants;
import org.apache.helix.HelixRebalanceException;
import org.apache.helix.controller.dataproviders.ResourceControllerDataProvider;
import org.apache.helix.controller.rebalancer.util.WagedRebalanceUtil;
import org.apache.helix.controller.rebalancer.waged.RebalanceAlgorithm;
import org.apache.helix.controller.rebalancer.waged.WagedRebalancer;
import org.apache.helix.controller.rebalancer.waged.constraints.ConstraintBasedAlgorithmFactory;
import org.apache.helix.model.BuiltInStateModelDefinitions;
import org.apache.helix.model.ClusterConfig;
import org.apache.helix.model.IdealState;
import org.apache.helix.model.InstanceConfig;
import org.apache.helix.model.LiveInstance;
import org.apache.helix.model.Partition;
import org.apache.helix.model.Resource;
import org.apache.helix.model.ResourceAssignment;
import org.apache.helix.model.ResourceConfig;
import org.testng.Assert;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

/**
 * Which calculations a newly added untagged resource takes part in, and so which ones it merges
 * into one cluster wide share block. An untagged resource can be placed on any node, so a
 * calculation that includes it pulls every group it meets into one block, and a failure there fails
 * the calculation exactly as the default mode does. The partial rebalance leaves out a resource the
 * baseline does not hold yet, as the default mode does, so there a new untagged resource merges
 * nothing: the broken clique is carried over and a healthy clique still moves off a node that
 * shrank. The emergency rebalance and the delayed rebalance overwrite count every resource the
 * pipeline serves, including one absent from the assignment they start from, so the new untagged
 * resource merges the groups there although neither places any of its replicas.
 *
 * Three disjoint cliques on DISK 100 nodes: A (a0 to a2) serves RA, which is broken in one of two
 * ways, B (b0 to b2) serves RB and C (c0 to c2) serves RC, with c0 shrunk to DISK 5, too small for
 * any replica. RU is untagged and new. Every replica other than RA's weighs DISK 10.
 */
public class TestWagedIsolationUntaggedResourceScopes {
  private static final String CLUSTER = "UntaggedResourceScopes";
  private static final String DISK = "DISK";
  private static final String MASTER = "MASTER";
  private static final String SLAVE = "SLAVE";
  private static final String MASTER_SLAVE = BuiltInStateModelDefinitions.MasterSlave.name();
  private static final String RA = "RA";
  private static final String RB = "RB";
  private static final String RC = "RC";
  private static final String RU = "RU";
  private static final int WEIGHT = 10;
  private static final int CAPACITY = 100;
  private static final String SHRUNK = "c0";
  private static final int SHRUNK_CAPACITY = 5;
  private static final List<String> A_NODES = Arrays.asList("a0", "a1", "a2");
  private static final List<String> B_NODES = Arrays.asList("b0", "b1", "b2");
  private static final List<String> C_NODES = Arrays.asList("c0", "c1", "c2");
  /** The nodes that stay down in the emergency and the delayed rebalance overwrite. */
  private static final Set<String> DOWN = new HashSet<>(Arrays.asList("a1", "b1"));

  /** How clique A is broken. */
  private enum Break {
    /** RA_0 outweighs every node, so that one partition finds no candidate node. */
    TAG_LOCAL(HelixRebalanceException.FailureCategory.NO_CANDIDATE_NODE),
    /** RA outweighs the whole cluster, so the tag blind capacity check comes out negative. */
    DEFICIT(HelixRebalanceException.FailureCategory.CAPACITY_DEFICIT);

    private final HelixRebalanceException.FailureCategory _category;

    Break(HelixRebalanceException.FailureCategory category) {
      _category = category;
    }
  }

  @DataProvider(name = "breaks")
  public Object[][] breaks() {
    return new Object[][] {{Break.TAG_LOCAL}, {Break.DEFICIT}};
  }

  /**
   * The baseline places the new untagged resource, so it is part of the calculation and merges
   * every group into one block: the failure of A fails the whole baseline exactly as the default
   * mode does, so C does not move off the shrunk node either. The same baseline without the
   * untagged resource carries A over and moves C off the shrunk node.
   */
  @Test(dataProvider = "breaks")
  public void testGlobalBaselineMergesEveryGroupWhenItPlacesANewUntaggedResource(Break broken)
      throws Exception {
    Map<String, ResourceAssignment> baseline = assignments(rotation(RA, 6, A_NODES, false),
        rotation(RB, 3, B_NODES, false), rotation(RC, 3, C_NODES, false));

    ClusterModel model = baselineModel(provider(true, broken, false), resources(true), baseline);
    assertUntaggedResource(model, true, true);
    assertFailsAsStock(failure(model, baseline),
        failure(baselineModel(provider(false, broken, false), resources(true), baseline),
            baseline), broken);

    Map<String, ResourceAssignment> result = WagedRebalanceUtil.calculateAssignment(
        baselineModel(provider(true, broken, false), resources(false), baseline), algorithm(),
        baseline);
    assertSameAssignment(result.get(RA), baseline.get(RA));
    assertFullyPlaced(result.get(RB), 3, B_NODES);
    assertFullyPlaced(result.get(RC), 3, Arrays.asList("c1", "c2"));
    assertNoOvercommit(result);
    Assert.assertEquals(failure(baselineModel(provider(false, broken, false), resources(false),
        baseline), baseline).getFailureCategory(), broken._category);
  }

  /**
   * The baseline does not hold the new untagged resource, so the partial rebalance leaves it out
   * as the default mode does and it merges nothing. A is carried over with its previous best
   * possible assignment, B keeps its placement, C moves off the shrunk node, no node is
   * overcommitted and the untagged resource gets no assignment. The default mode fails the same
   * calculation.
   */
  @Test(dataProvider = "breaks")
  public void testPartialRebalanceLeavesANewUntaggedResourceOutOfTheBlocks(Break broken)
      throws Exception {
    Map<String, ResourceAssignment> baseline = assignments(rotation(RA, 6, A_NODES, false),
        rotation(RB, 3, B_NODES, false), rotation(RC, 3, C_NODES, false));
    Map<String, ResourceAssignment> bestPossible = assignments(rotation(RA, 6, A_NODES, true),
        rotation(RB, 3, B_NODES, false), rotation(RC, 3, C_NODES, true));

    ClusterModel model = partialModel(provider(true, broken, false), baseline, bestPossible);
    assertUntaggedResource(model, false, false);
    Map<String, ResourceAssignment> result =
        WagedRebalanceUtil.calculateAssignment(model, algorithm(), bestPossible);
    Assert.assertEquals(new HashSet<>(result.keySet()), new HashSet<>(Arrays.asList(RA, RB, RC)));
    assertSameAssignment(result.get(RA), bestPossible.get(RA));
    assertSameAssignment(result.get(RB), bestPossible.get(RB));
    assertFullyPlaced(result.get(RC), 3, Arrays.asList("c1", "c2"));
    assertNoOvercommit(result);

    Assert.assertEquals(failure(partialModel(provider(false, broken, false), baseline,
        bestPossible), bestPossible).getFailureCategory(), broken._category);
  }

  /**
   * Once the baseline holds the untagged resource, the partial rebalance includes it, and it
   * merges every group into one block: the failure of A fails the partial rebalance exactly as
   * the default mode does.
   */
  @Test(dataProvider = "breaks")
  public void testPartialRebalanceMergesTheGroupsOnceTheBaselineHoldsTheUntaggedResource(
      Break broken) throws Exception {
    Map<String, ResourceAssignment> baseline = assignments(rotation(RA, 6, A_NODES, false),
        rotation(RB, 3, B_NODES, false), rotation(RC, 3, C_NODES, false),
        rotation(RU, 3, B_NODES, false));
    Map<String, ResourceAssignment> bestPossible = assignments(rotation(RA, 6, A_NODES, true),
        rotation(RB, 3, B_NODES, false), rotation(RC, 3, C_NODES, true));

    ClusterModel model = partialModel(provider(true, broken, false), baseline, bestPossible);
    assertUntaggedResource(model, true, true);
    assertFailsAsStock(failure(model, bestPossible),
        failure(partialModel(provider(false, broken, false), baseline, bestPossible),
            bestPossible), broken);
  }

  /**
   * The emergency rebalance builds its groups from every resource the pipeline serves, so the new
   * untagged resource merges them although it is absent from the best possible assignment and the
   * emergency places none of its replicas: the failure of A fails the emergency exactly as the
   * default mode does. Without the untagged resource the same emergency carries A over and
   * recovers B's replicas from the down node.
   */
  @Test(dataProvider = "breaks")
  public void testEmergencyRebalanceMergesTheGroupsWithAnUntaggedResourceItDoesNotPlace(
      Break broken) throws Exception {
    Map<String, ResourceAssignment> bestPossible = served();

    ClusterModel model =
        emergencyModel(provider(true, broken, true), resources(true), bestPossible);
    assertUntaggedResource(model, true, false);
    assertFailsAsStock(failure(model, bestPossible),
        failure(emergencyModel(provider(false, broken, true), resources(true), bestPossible),
            bestPossible), broken);

    Map<String, ResourceAssignment> result = WagedRebalanceUtil.calculateAssignment(
        emergencyModel(provider(true, broken, true), resources(false), bestPossible),
        algorithm(), bestPossible);
    assertSameAssignment(result.get(RA), bestPossible.get(RA));
    assertFullyPlaced(result.get(RB), 3, Arrays.asList("b0", "b2"));
    assertSameAssignment(result.get(RC), bestPossible.get(RC));
    assertNoOvercommit(result);
    Assert.assertEquals(failure(emergencyModel(provider(false, broken, true), resources(false),
        bestPossible), bestPossible).getFailureCategory(), broken._category);
  }

  /**
   * The delayed rebalance overwrite also builds its groups from every resource the pipeline
   * serves, so the new untagged resource merges them although it is absent from the current
   * assignment and gets no replica topped up: the failure of A fails the overwrite exactly as the
   * default mode does. Without the untagged resource the same overwrite leaves A out and tops up
   * the B partitions that lost a replica to the down node.
   */
  @Test(dataProvider = "breaks")
  public void testDelayedOverwriteMergesTheGroupsWithAnUntaggedResourceItDoesNotPlace(
      Break broken) throws Exception {
    Map<String, ResourceAssignment> current = served();

    ClusterModel model = delayedModel(provider(true, broken, true), resources(true), current);
    assertUntaggedResource(model, true, false);
    assertFailsAsStock(failure(model, null),
        failure(delayedModel(provider(false, broken, true), resources(true), current), null),
        broken);

    model = delayedModel(provider(true, broken, true), resources(false), current);
    Set<String> toppedUp = new HashSet<>(model.getAssignableReplicaMap().keySet());
    Assert.assertEquals(toppedUp, new HashSet<>(Arrays.asList(RA, RB)));
    Map<String, ResourceAssignment> result =
        WagedRebalanceUtil.calculateAssignment(model, algorithm(), null);
    Assert.assertFalse(result.containsKey(RA), "A is left out of the overwrite: " + result);
    assertFullyPlaced(result.get(RB), 3, Arrays.asList("b0", "b2"));
    assertSameAssignment(result.get(RC), current.get(RC));
    assertNoOvercommit(result);
    Assert.assertEquals(failure(delayedModel(provider(false, broken, true), resources(false),
        current), null).getFailureCategory(), broken._category);
  }

  private static ClusterModel baselineModel(ResourceControllerDataProvider provider,
      Map<String, Resource> resources, Map<String, ResourceAssignment> baseline) {
    Map<HelixConstants.ChangeType, Set<String>> changes = new HashMap<>();
    changes.put(HelixConstants.ChangeType.INSTANCE_CONFIG,
        new HashSet<>(Collections.singleton(SHRUNK)));
    changes.put(HelixConstants.ChangeType.RESOURCE_CONFIG, new HashSet<>(Arrays.asList(RA, RU)));
    changes.put(HelixConstants.ChangeType.IDEAL_STATE, new HashSet<>(Collections.singleton(RU)));
    return ClusterModelProvider.generateClusterModelForBaseline(provider, resources,
        new HashSet<>(allNodes()), changes, baseline);
  }

  private static ClusterModel partialModel(ResourceControllerDataProvider provider,
      Map<String, ResourceAssignment> baseline, Map<String, ResourceAssignment> bestPossible) {
    return ClusterModelProvider.generateClusterModelForPartialRebalance(provider, resources(true),
        new HashSet<>(allNodes()), baseline, bestPossible);
  }

  private static ClusterModel emergencyModel(ResourceControllerDataProvider provider,
      Map<String, Resource> resources, Map<String, ResourceAssignment> bestPossible) {
    return ClusterModelProvider.generateClusterModelForEmergencyRebalance(provider, resources,
        upNodes(), bestPossible);
  }

  private static ClusterModel delayedModel(ResourceControllerDataProvider provider,
      Map<String, Resource> resources, Map<String, ResourceAssignment> current) {
    return ClusterModelProvider.generateClusterModelForDelayedRebalanceOverwrites(provider,
        resources, upNodes(), current);
  }

  /**
   * The cluster, with isolation on or off, A broken as asked, and a1 and b1 offline when the
   * scenario has nodes down.
   */
  private static ResourceControllerDataProvider provider(boolean isolation, Break broken,
      boolean nodesDown) throws IOException {
    ClusterConfig clusterConfig = new ClusterConfig(CLUSTER);
    clusterConfig.setInstanceCapacityKeys(Collections.singletonList(DISK));
    clusterConfig.setDefaultInstanceCapacityMap(Collections.singletonMap(DISK, CAPACITY));
    clusterConfig.setDefaultPartitionWeightMap(Collections.singletonMap(DISK, WEIGHT));
    clusterConfig.setWagedInstanceTagIsolationEnabled(isolation);
    ResourceControllerDataProvider provider = new ResourceControllerDataProvider(CLUSTER);
    provider.setClusterConfig(clusterConfig);
    Map<String, InstanceConfig> instanceConfigs = new HashMap<>();
    List<LiveInstance> liveInstances = new ArrayList<>();
    for (String node : allNodes()) {
      InstanceConfig instanceConfig = new InstanceConfig(node);
      instanceConfig.addTag(tagOf(node));
      instanceConfig.setInstanceCapacityMap(Collections.singletonMap(DISK, capacityOf(node)));
      instanceConfigs.put(node, instanceConfig);
      if (!nodesDown || !DOWN.contains(node)) {
        LiveInstance liveInstance = new LiveInstance(node);
        liveInstance.setSessionId(node + "_session");
        liveInstances.add(liveInstance);
      }
    }
    provider.setInstanceConfigMap(instanceConfigs);
    provider.setLiveInstances(liveInstances);
    provider.setStateModelDefMap(Collections.singletonMap(MASTER_SLAVE,
        BuiltInStateModelDefinitions.MasterSlave.getStateModelDefinition()));
    List<IdealState> idealStates = new ArrayList<>();
    Map<String, ResourceConfig> resourceConfigs = new HashMap<>();
    for (Resource resource : resources(true).values()) {
      String name = resource.getResourceName();
      idealStates.add(idealState(name, resource.getPartitions().size()));
      Map<String, Map<String, Integer>> capacity = new HashMap<>();
      capacity.put(ResourceConfig.DEFAULT_PARTITION_KEY, Collections.singletonMap(DISK, WEIGHT));
      if (name.equals(RA) && broken == Break.TAG_LOCAL) {
        capacity.put(RA + "_0", Collections.singletonMap(DISK, CAPACITY + 50));
      } else if (name.equals(RA)) {
        capacity.put(ResourceConfig.DEFAULT_PARTITION_KEY,
            Collections.singletonMap(DISK, CAPACITY));
      }
      ResourceConfig resourceConfig = new ResourceConfig(name);
      resourceConfig.setPartitionCapacityMap(capacity);
      resourceConfigs.put(name, resourceConfig);
    }
    provider.setIdealStates(idealStates);
    provider.setResourceConfigMap(resourceConfigs);
    return provider;
  }

  private static IdealState idealState(String name, int partitions) {
    IdealState idealState = new IdealState(name);
    idealState.setRebalanceMode(IdealState.RebalanceMode.FULL_AUTO);
    idealState.setRebalancerClassName(WagedRebalancer.class.getName());
    idealState.setStateModelDefRef(MASTER_SLAVE);
    idealState.setReplicas("2");
    idealState.setNumPartitions(partitions);
    String tag = tagOfResource(name);
    if (tag != null) {
      idealState.setInstanceGroupTag(tag);
    }
    for (int i = 0; i < partitions; i++) {
      idealState.getRecord().setListField(name + "_" + i, new ArrayList<>());
    }
    return idealState;
  }

  /** The resources the pipeline serves, with or without the untagged one. */
  private static Map<String, Resource> resources(boolean withUntagged) {
    Map<String, Resource> resources = new TreeMap<>();
    resources.put(RA, resource(RA, 6));
    resources.put(RB, resource(RB, 3));
    resources.put(RC, resource(RC, 3));
    if (withUntagged) {
      resources.put(RU, resource(RU, 3));
    }
    return resources;
  }

  private static Resource resource(String name, int partitions) {
    Resource resource = new Resource(name);
    resource.setStateModelDefRef(MASTER_SLAVE);
    for (int i = 0; i < partitions; i++) {
      resource.addPartition(name + "_" + i);
    }
    return resource;
  }

  /**
   * What the Helix controller serves while a1 and b1 are down: A and B as the baseline placed
   * them, and C on the two nodes that fit its replicas.
   */
  private static Map<String, ResourceAssignment> served() {
    ResourceAssignment rc = new ResourceAssignment(RC);
    for (int i = 0; i < 3; i++) {
      Map<String, String> replicas = new TreeMap<>();
      replicas.put("c1", i % 2 == 0 ? MASTER : SLAVE);
      replicas.put("c2", i % 2 == 0 ? SLAVE : MASTER);
      rc.addReplicaMap(new Partition(RC + "_" + i), replicas);
    }
    return assignments(rotation(RA, 6, A_NODES, false), rotation(RB, 3, B_NODES, false), rc);
  }

  /**
   * Partition i on nodes i and i + 1 (mod 3), the first as MASTER and the second as SLAVE, or with
   * the roles swapped.
   */
  private static ResourceAssignment rotation(String resource, int partitions, List<String> nodes,
      boolean swapped) {
    ResourceAssignment assignment = new ResourceAssignment(resource);
    for (int i = 0; i < partitions; i++) {
      Map<String, String> replicas = new TreeMap<>();
      replicas.put(nodes.get(i % 3), swapped ? SLAVE : MASTER);
      replicas.put(nodes.get((i + 1) % 3), swapped ? MASTER : SLAVE);
      assignment.addReplicaMap(new Partition(resource + "_" + i), replicas);
    }
    return assignment;
  }

  private static Map<String, ResourceAssignment> assignments(ResourceAssignment... assignments) {
    Map<String, ResourceAssignment> map = new HashMap<>();
    for (ResourceAssignment assignment : assignments) {
      map.put(assignment.getResourceName(), assignment);
    }
    return map;
  }

  private static List<String> allNodes() {
    List<String> nodes = new ArrayList<>(A_NODES);
    nodes.addAll(B_NODES);
    nodes.addAll(C_NODES);
    return nodes;
  }

  private static Set<String> upNodes() {
    Set<String> nodes = new HashSet<>(allNodes());
    nodes.removeAll(DOWN);
    return nodes;
  }

  private static String tagOf(String node) {
    return node.substring(0, 1).toUpperCase();
  }

  private static String tagOfResource(String resource) {
    return resource.equals(RU) ? null : resource.substring(1);
  }

  private static int capacityOf(String node) {
    return node.equals(SHRUNK) ? SHRUNK_CAPACITY : CAPACITY;
  }

  private static RebalanceAlgorithm algorithm() {
    return ConstraintBasedAlgorithmFactory.getInstance(Collections.emptyMap());
  }

  private static HelixRebalanceException failure(ClusterModel model,
      Map<String, ResourceAssignment> previous) {
    try {
      WagedRebalanceUtil.calculateAssignment(model, algorithm(), previous);
    } catch (HelixRebalanceException e) {
      return e;
    }
    throw new AssertionError("The " + model.getRebalanceScopeType() + " calculation succeeded");
  }

  /**
   * Whether the calculation counts the untagged resource among its groups, and whether it has any
   * of its replicas to assign.
   */
  private static void assertUntaggedResource(ClusterModel model, boolean grouped,
      boolean assigned) {
    Map<String, String> groups = model.getContext().getResourceInstanceGroupTags();
    Assert.assertEquals(groups.containsKey(RU), grouped,
        "RU among the groups of the " + model.getRebalanceScopeType() + " calculation " + groups);
    Assert.assertEquals(model.getAssignableReplicaMap().containsKey(RU), assigned,
        "RU among the resources the " + model.getRebalanceScopeType() + " calculation assigns "
            + model.getAssignableReplicaMap().keySet());
  }

  /**
   * The isolation on failure is the one the default mode throws: the same type and category, and
   * the same message up to the per node failure details.
   */
  private static void assertFailsAsStock(HelixRebalanceException isolationOn,
      HelixRebalanceException stock, Break broken) {
    Assert.assertEquals(stock.getFailureCategory(), broken._category, stock.getMessage());
    Assert.assertEquals(isolationOn.getFailureCategory(), stock.getFailureCategory(),
        isolationOn.getMessage());
    Assert.assertEquals(isolationOn.getFailureType(), stock.getFailureType());
    Assert.assertEquals(headline(isolationOn), headline(stock));
  }

  private static String headline(HelixRebalanceException e) {
    String message = e.getMessage();
    int details = message.indexOf("; Failure summary");
    return details < 0 ? message : message.substring(0, details);
  }

  private static void assertSameAssignment(ResourceAssignment actual,
      ResourceAssignment expected) {
    Assert.assertNotNull(actual, "Missing " + expected.getResourceName());
    Assert.assertEquals(actual.getRecord().getMapFields(), expected.getRecord().getMapFields(),
        expected.getResourceName());
  }

  /** Every partition holds one MASTER and one SLAVE, on two of the given nodes. */
  private static void assertFullyPlaced(ResourceAssignment assignment, int partitions,
      Collection<String> nodes) {
    Assert.assertNotNull(assignment);
    Assert.assertEquals(assignment.getMappedPartitions().size(), partitions,
        assignment.toString());
    for (Partition partition : assignment.getMappedPartitions()) {
      Map<String, String> replicas = assignment.getReplicaMap(partition);
      List<String> states = new ArrayList<>(replicas.values());
      Collections.sort(states);
      Assert.assertEquals(states, Arrays.asList(MASTER, SLAVE), partition + ": " + replicas);
      Assert.assertTrue(nodes.containsAll(replicas.keySet()), partition + ": " + replicas);
    }
  }

  /**
   * No node outside A holds more than its capacity. A keeps the placement it had at its previous
   * weights, which the assertions on A check separately.
   */
  private static void assertNoOvercommit(Map<String, ResourceAssignment> result) {
    Map<String, Integer> usage = new TreeMap<>();
    result.forEach((resource, assignment) -> {
      if (!resource.equals(RA)) {
        for (Partition partition : assignment.getMappedPartitions()) {
          assignment.getReplicaMap(partition).keySet()
              .forEach(node -> usage.merge(node, WEIGHT, Integer::sum));
        }
      }
    });
    usage.forEach((node, used) -> Assert.assertTrue(used <= capacityOf(node),
        node + " holds " + used + " of " + capacityOf(node) + ": " + usage));
  }
}
