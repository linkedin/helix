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
import java.util.EnumMap;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.stream.Collectors;

import org.apache.helix.HelixRebalanceException;
import org.apache.helix.constants.InstanceConstants;
import org.apache.helix.controller.rebalancer.waged.RebalanceAlgorithm;
import org.apache.helix.controller.rebalancer.waged.constraints.ConstraintBasedAlgorithmFactory;
import org.apache.helix.model.ClusterConfig;
import org.apache.helix.model.InstanceConfig;
import org.apache.helix.model.Partition;
import org.apache.helix.model.ResourceAssignment;
import org.apache.helix.model.ResourceConfig;
import org.testng.Assert;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

/**
 * Feature parity of WAGED instance tag isolation, one WAGED placement feature at a time.
 *
 * Every feature that shapes a placement inside a clique is switched on in turn: the global
 * rebalance preference (evenness, less movement, forced baseline convergence) acting on a previous
 * baseline and best possible assignment, topology aware fault zones shared by every clique, the
 * cluster and the resource partition limits per instance, uneven instance capacity, a preferred
 * scoring key over two capacity dimensions, and disabled partitions. For each one:
 * <ul>
 *   <li>with nothing broken, turning isolation on must not move a single replica;</li>
 *   <li>with one clique broken, every other clique must land exactly where a stock calculation of
 *       the same cluster puts it, the broken clique must be skipped as a unit, and its rollback
 *       must leave nothing behind on its nodes or in the fault zone bookkeeping;</li>
 *   <li>the feature itself must still hold in the isolated placement.</li>
 * </ul>
 * The clique is broken through a different hard constraint each time: the resource partition
 * limit, the fault zone constraint, a disabled partition, and node capacity, both for a partition
 * that fits nowhere and for a clique that runs out of room part way, so the rollback has real
 * placements to undo.
 */
public class TestWagedIsolationFeatureParity extends AbstractTestWagedInstanceTagIsolation {
  private static final int CLIQUES = 5;
  private static final int NODES = 6;
  private static final int ZONES = 3;
  private static final int PARTITIONS = 6;
  private static final int SLAVES = 2;
  private static final int REPLICAS = SLAVES + 1;
  private static final int BROKEN = 2;
  private static final int WEIGHT = 10;
  // Heavier than the largest node, even with uneven capacity, yet light enough that the cluster as
  // a whole still fits. A cluster wide deficit is a different path, covered by the capacity tests.
  private static final int HEAVIER_THAN_ANY_NODE = 200;
  // One replica per uniform node and at most three on the largest uneven node: fewer slots than
  // replicas, so the clique fails only after some of its replicas were placed.
  private static final int PART_WAY_WEIGHT = 60;
  private static final int UNEVEN_CAPACITY_STEP = 40;
  private static final String CPU_KEY = "CPU";
  private static final int CPU_CAPACITY = 40;
  private static final int CPU_WEIGHT = 6;
  private static final String MASTER = "MASTER";
  private static final String SLAVE = "SLAVE";
  private static final Features DEFAULTS = new Features("defaults");

  private Map<String, ResourceAssignment> _baseline;
  private Map<String, ResourceAssignment> _bestPossible;

  /** The WAGED features one calculation switches on. Every run of a case applies the same set. */
  private static final class Features {
    private final String _name;
    private Map<ClusterConfig.GlobalRebalancePreferenceKey, Integer> _preferences =
        Collections.emptyMap();
    private boolean _previousAssignment;
    private boolean _faultZones;
    private int _instancePartitionLimit = -1;
    private int _resourcePartitionLimit = -1;
    private boolean _unevenCapacity;
    private boolean _preferredScoringKey;
    private boolean _disabledPartitions;

    private Features(String name) {
      _name = name;
    }

    private Features preferences(int evenness, int lessMovement, boolean forceBaselineConverge) {
      Map<ClusterConfig.GlobalRebalancePreferenceKey, Integer> preferences =
          new EnumMap<>(ClusterConfig.GlobalRebalancePreferenceKey.class);
      preferences.put(ClusterConfig.GlobalRebalancePreferenceKey.EVENNESS, evenness);
      preferences.put(ClusterConfig.GlobalRebalancePreferenceKey.LESS_MOVEMENT, lessMovement);
      if (forceBaselineConverge) {
        preferences.put(ClusterConfig.GlobalRebalancePreferenceKey.FORCE_BASELINE_CONVERGE, 1);
      }
      _preferences = preferences;
      return previousAssignment();
    }

    private Features previousAssignment() {
      _previousAssignment = true;
      return this;
    }

    private Features faultZones() {
      _faultZones = true;
      return this;
    }

    private Features instancePartitionLimit(int limit) {
      _instancePartitionLimit = limit;
      return this;
    }

    private Features resourcePartitionLimit(int limit) {
      _resourcePartitionLimit = limit;
      return this;
    }

    private Features unevenCapacity() {
      _unevenCapacity = true;
      return this;
    }

    private Features preferredScoringKey() {
      _preferredScoringKey = true;
      return this;
    }

    private Features disabledPartitions() {
      _disabledPartitions = true;
      return this;
    }

    @Override
    public String toString() {
      return _name;
    }
  }

  /** How the broken clique is made unplaceable. Only that clique is touched. */
  private enum Break {
    NONE,
    // Its resource allows one partition per instance: six slots for eighteen replicas.
    RESOURCE_PARTITION_LIMIT,
    // All of its nodes share one fault zone, so a second replica of a partition fits nowhere.
    SINGLE_FAULT_ZONE,
    // One partition is disabled on all but two of its nodes, which cannot hold three replicas.
    DISABLED_PARTITION,
    // One partition is heavier than any node.
    UNPLACEABLE_PARTITION,
    // Every replica is so heavy that the clique runs out of room part way.
    PART_WAY_THROUGH;

    // A weight change moves the cluster wide estimates every clique is scored against.
    private boolean changesTheSharedEstimates() {
      return this == UNPLACEABLE_PARTITION || this == PART_WAY_THROUGH;
    }
  }

  private static final class Run {
    private final OptimalAssignment _result;
    private final ClusterModel _model;

    private Run(OptimalAssignment result, ClusterModel model) {
      _result = result;
      _model = model;
    }

    private Map<String, Map<String, Map<String, String>>> placement() {
      return normalize(_result.getOptimalResourceAssignment());
    }
  }

  private static Features[] featureSets() {
    return new Features[] {
        new Features("previous assignment, default preference").previousAssignment(),
        new Features("evenness preference").preferences(10, 1, false),
        new Features("less movement preference").preferences(1, 10, false),
        new Features("force baseline converge").preferences(1, 1, true),
        new Features("topology aware fault zones").faultZones(),
        new Features("instance partition limit").instancePartitionLimit(4),
        new Features("resource partition limit").resourcePartitionLimit(4),
        new Features("uneven instance capacity").unevenCapacity(),
        new Features("preferred scoring key").preferredScoringKey(),
        new Features("disabled partitions").disabledPartitions(),
        new Features("every feature at once").preferences(3, 2, false).faultZones()
            .instancePartitionLimit(4).resourcePartitionLimit(4).unevenCapacity()
            .preferredScoringKey().disabledPartitions()
    };
  }

  @DataProvider(name = "features")
  public Object[][] features() {
    Features[] featureSets = featureSets();
    Object[][] cases = new Object[featureSets.length][];
    for (int i = 0; i < featureSets.length; i++) {
      cases[i] = new Object[] {featureSets[i]};
    }
    return cases;
  }

  @DataProvider(name = "brokenCliques")
  public Object[][] brokenCliques() {
    List<Object[]> cases = new ArrayList<>();
    for (Features features : featureSets()) {
      for (Break fault : new Break[] {Break.RESOURCE_PARTITION_LIMIT, Break.DISABLED_PARTITION,
          Break.UNPLACEABLE_PARTITION, Break.PART_WAY_THROUGH}) {
        cases.add(new Object[] {features, fault});
      }
      if (features._faultZones) {
        cases.add(new Object[] {features, Break.SINGLE_FAULT_ZONE});
      }
    }
    return cases.toArray(new Object[0][]);
  }

  @Test(dataProvider = "features")
  public void testIsolationMovesNothingWhenNothingIsBroken(Features features) throws Exception {
    Run stock = run(features, Break.NONE, false);
    Run isolated = run(features, Break.NONE, true);

    List<String> problems = new ArrayList<>();
    if (!isolated._result.getSkippedResources().isEmpty()) {
      problems.add("isolation skipped " + isolated._result.getSkippedResources());
    }
    compareCliques(isolated.placement(), stock.placement(), "the stock calculation", true,
        problems);
    problems.addAll(featureViolations(features, isolated));
    Assert.assertTrue(problems.isEmpty(), features + ": " + problems);
  }

  @Test(dataProvider = "brokenCliques")
  public void testHealthyCliquesMatchStockWhileOneCliqueIsBroken(Features features, Break fault)
      throws Exception {
    List<String> problems = new ArrayList<>();
    try {
      run(features, fault, false);
      problems.add("the break does not fail a stock calculation, so this case proves nothing");
    } catch (HelixRebalanceException expected) {
      // Stock fails the whole cluster on the broken clique, which is what isolation must contain.
    }

    Run isolated = run(features, fault, true);
    if (!isolated._result.getSkippedResources()
        .equals(Collections.singleton(resourceName(BROKEN)))) {
      problems.add("expected only " + resourceName(BROKEN) + " to be skipped, but "
          + isolated._result.getSkippedResources() + " were");
    }
    Map<String, Map<String, String>> brokenPlacement =
        isolated.placement().get(resourceName(BROKEN));
    if (brokenPlacement != null && !brokenPlacement.isEmpty()) {
      problems.add("the broken clique was partly placed: " + brokenPlacement);
    }
    compareCliques(isolated.placement(), stockWithoutTheBrokenClique(features, fault).placement(),
        "stock with the broken clique's replicas left out", false, problems);
    if (!fault.changesTheSharedEstimates()) {
      compareCliques(isolated.placement(), run(features, Break.NONE, false).placement(),
          "stock with the broken clique repaired", false, problems);
    }
    problems.addAll(rollbackLeftovers(isolated._model));
    problems.addAll(featureViolations(features, isolated));
    Assert.assertTrue(problems.isEmpty(), features + " / " + fault + ": " + problems);
  }

  /**
   * The preference cases only prove something if the preference really steers the placement.
   * Less movement must keep more replicas where the best possible had them than evenness does,
   * and forced convergence must put more replicas on the baseline than less movement does.
   */
  @Test
  public void testThePreferencesUnderTestReallySteerThePlacement() throws Exception {
    Map<String, Map<String, Map<String, String>>> evenness =
        run(new Features("evenness").preferences(10, 1, false), Break.NONE, true).placement();
    Map<String, Map<String, Map<String, String>>> lessMovement =
        run(new Features("less movement").preferences(1, 10, false), Break.NONE, true)
            .placement();
    Map<String, Map<String, Map<String, String>>> converge =
        run(new Features("converge").preferences(1, 1, true), Break.NONE, true).placement();

    int keptByEvenness = sharedReplicas(evenness, normalize(bestPossible()));
    int keptByLessMovement = sharedReplicas(lessMovement, normalize(bestPossible()));
    Assert.assertTrue(keptByLessMovement > keptByEvenness, "Less movement kept "
        + keptByLessMovement + " replicas in place and evenness kept " + keptByEvenness);
    int convergedByPreference = sharedReplicas(converge, normalize(baseline()));
    int convergedByLessMovement = sharedReplicas(lessMovement, normalize(baseline()));
    Assert.assertTrue(convergedByPreference > convergedByLessMovement, "Forced convergence put "
        + convergedByPreference + " replicas on the baseline and less movement put "
        + convergedByLessMovement);
  }

  private Run run(Features features, Break fault, boolean isolation)
      throws IOException, HelixRebalanceException {
    ClusterConfig clusterConfig = clusterConfig(features, isolation);
    Set<AssignableReplica> replicas = replicas(clusterConfig, features, fault);
    Set<AssignableNode> nodes = nodes(clusterConfig, features, fault, 0, NODES);
    ClusterModel model = model(clusterConfig, features, replicas, replicas, nodes);
    return new Run(algorithm(features).calculate(model), model);
  }

  /**
   * The strongest stock reference for a broken run: the same cluster context, including the
   * broken clique's replicas in every cluster wide estimate, but with only the healthy replicas
   * handed to a stock calculation.
   */
  private Run stockWithoutTheBrokenClique(Features features, Break fault)
      throws IOException, HelixRebalanceException {
    ClusterConfig clusterConfig = clusterConfig(features, false);
    Set<AssignableReplica> replicas = replicas(clusterConfig, features, fault);
    Set<AssignableReplica> healthyReplicas = replicas.stream()
        .filter(replica -> !replica.getResourceName().equals(resourceName(BROKEN)))
        .collect(Collectors.toSet());
    Set<AssignableNode> nodes = nodes(clusterConfig, features, fault, 0, NODES);
    ClusterModel model = model(clusterConfig, features, replicas, healthyReplicas, nodes);
    return new Run(algorithm(features).calculate(model), model);
  }

  private ClusterModel model(ClusterConfig clusterConfig, Features features,
      Set<AssignableReplica> contextReplicas, Set<AssignableReplica> assignableReplicas,
      Set<AssignableNode> nodes) throws IOException, HelixRebalanceException {
    Map<String, ResourceAssignment> baseline =
        features._previousAssignment ? baseline() : Collections.emptyMap();
    Map<String, ResourceAssignment> bestPossible =
        features._previousAssignment ? bestPossible() : Collections.emptyMap();
    ClusterContext context =
        new ClusterContext(contextReplicas, nodes, baseline, bestPossible, clusterConfig);
    return new ClusterModel(context, assignableReplicas, nodes,
        ClusterModel.RebalanceScopeType.GLOBAL_BASELINE);
  }

  private static RebalanceAlgorithm algorithm(Features features) {
    return ConstraintBasedAlgorithmFactory.getInstance(features._preferences);
  }

  /**
   * The previous baseline and best possible are stock placements of the same resources on one half
   * of each clique, the first half for the baseline and the second for the best possible. Both are
   * deliberately uneven, so less movement, evenness and forced convergence pull different ways.
   */
  private Map<String, ResourceAssignment> baseline() throws IOException, HelixRebalanceException {
    if (_baseline == null) {
      _baseline = placementOnHalfOfEachClique(0);
    }
    return _baseline;
  }

  private Map<String, ResourceAssignment> bestPossible()
      throws IOException, HelixRebalanceException {
    if (_bestPossible == null) {
      _bestPossible = placementOnHalfOfEachClique(NODES / 2);
    }
    return _bestPossible;
  }

  private Map<String, ResourceAssignment> placementOnHalfOfEachClique(int firstNode)
      throws IOException, HelixRebalanceException {
    ClusterConfig clusterConfig = clusterConfig(DEFAULTS, false);
    Set<AssignableReplica> replicas = replicas(clusterConfig, DEFAULTS, Break.NONE);
    Set<AssignableNode> nodes =
        nodes(clusterConfig, DEFAULTS, Break.NONE, firstNode, firstNode + NODES / 2);
    ClusterContext context = new ClusterContext(replicas, nodes, Collections.emptyMap(),
        Collections.emptyMap(), clusterConfig);
    return algorithm(DEFAULTS).calculate(new ClusterModel(context, replicas, nodes,
        ClusterModel.RebalanceScopeType.GLOBAL_BASELINE)).getOptimalResourceAssignment();
  }

  private ClusterConfig clusterConfig(Features features, boolean isolation) {
    ClusterConfig clusterConfig = new ClusterConfig("FeatureParityCluster");
    List<String> capacityKeys = capacityKeys(features);
    clusterConfig.setInstanceCapacityKeys(capacityKeys);
    Map<String, Integer> defaultWeights = new HashMap<>();
    capacityKeys.forEach(key -> defaultWeights.put(key, 0));
    clusterConfig.setDefaultPartitionWeightMap(defaultWeights);
    if (features._faultZones) {
      clusterConfig.setTopology("/zone/instance");
      clusterConfig.setFaultZoneType("zone");
      clusterConfig.setTopologyAwareEnabled(true);
    }
    if (features._instancePartitionLimit > 0) {
      clusterConfig.setMaxPartitionsPerInstance(features._instancePartitionLimit);
    }
    if (features._preferredScoringKey) {
      clusterConfig.setPreferredScoringKeys(Collections.singletonList(CPU_KEY));
    }
    clusterConfig.setWagedInstanceTagIsolationEnabled(isolation);
    return clusterConfig;
  }

  private static List<String> capacityKeys(Features features) {
    return features._preferredScoringKey ? Arrays.asList(CAPACITY_KEY, CPU_KEY)
        : Collections.singletonList(CAPACITY_KEY);
  }

  private Set<AssignableNode> nodes(ClusterConfig clusterConfig, Features features, Break fault,
      int firstNode, int endNode) {
    Set<AssignableNode> nodes = new HashSet<>();
    for (int clique = 0; clique < CLIQUES; clique++) {
      boolean broken = clique == BROKEN;
      for (int index = firstNode; index < endNode; index++) {
        String instance = instanceName(clique, index);
        InstanceConfig instanceConfig = new InstanceConfig(instance);
        Map<String, Integer> capacity = new HashMap<>();
        capacity.put(CAPACITY_KEY, features._unevenCapacity
            ? NODE_CAPACITY + UNEVEN_CAPACITY_STEP * (index % 3) : NODE_CAPACITY);
        if (features._preferredScoringKey) {
          capacity.put(CPU_KEY, CPU_CAPACITY + 20 * (index % 2));
        }
        instanceConfig.setInstanceCapacityMap(capacity);
        instanceConfig.addTag(cliqueTag(clique));
        instanceConfig.setInstanceOperation(InstanceConstants.InstanceOperation.ENABLE);
        if (features._faultZones) {
          // Zones are shared by every clique, so fault zone bookkeeping from one clique sits next
          // to every other clique's.
          int zone = broken && fault == Break.SINGLE_FAULT_ZONE ? 0 : index % ZONES;
          instanceConfig.setDomain("zone=zone_" + zone + ",instance=" + instance);
        }
        if (features._disabledPartitions && index == 0) {
          instanceConfig.setInstanceEnabledForPartition(resourceName(clique),
              partitionName(clique, 1), false);
        }
        if (broken && fault == Break.DISABLED_PARTITION && index < NODES - 2) {
          instanceConfig.setInstanceEnabledForPartition(resourceName(clique),
              partitionName(clique, 0), false);
        }
        nodes.add(new AssignableNode(clusterConfig, instanceConfig, instance));
      }
    }
    return nodes;
  }

  private Set<AssignableReplica> replicas(ClusterConfig clusterConfig, Features features,
      Break fault) throws IOException {
    Set<AssignableReplica> replicas = new HashSet<>();
    for (int clique = 0; clique < CLIQUES; clique++) {
      boolean broken = clique == BROKEN;
      ResourceConfig resourceConfig = new ResourceConfig(resourceName(clique));
      resourceConfig.getRecord()
          .setSimpleField(ResourceConfig.ResourceConfigProperty.INSTANCE_GROUP_TAG.name(),
              cliqueTag(clique));
      int partitionLimit = broken && fault == Break.RESOURCE_PARTITION_LIMIT ? 1
          : features._resourcePartitionLimit;
      if (partitionLimit > 0) {
        resourceConfig.getRecord().setIntField(
            ResourceConfig.ResourceConfigProperty.MAX_PARTITIONS_PER_INSTANCE.name(),
            partitionLimit);
      }
      Map<String, Map<String, Integer>> capacityMap = new HashMap<>();
      capacityMap.put(ResourceConfig.DEFAULT_PARTITION_KEY,
          weights(features, broken && fault == Break.PART_WAY_THROUGH ? PART_WAY_WEIGHT : WEIGHT));
      if (broken && fault == Break.UNPLACEABLE_PARTITION) {
        capacityMap.put(partitionName(clique, 0), weights(features, HEAVIER_THAN_ANY_NODE));
      }
      resourceConfig.setPartitionCapacityMap(capacityMap);
      for (int p = 0; p < PARTITIONS; p++) {
        String partition = partitionName(clique, p);
        replicas.add(new AssignableReplica(clusterConfig, resourceConfig, partition, MASTER, 1));
        for (int s = 0; s < SLAVES; s++) {
          replicas.add(new AssignableReplica(clusterConfig, resourceConfig, partition, SLAVE, 2));
        }
      }
    }
    return replicas;
  }

  private static Map<String, Integer> weights(Features features, int diskWeight) {
    Map<String, Integer> weights = new HashMap<>();
    weights.put(CAPACITY_KEY, diskWeight);
    if (features._preferredScoringKey) {
      weights.put(CPU_KEY, CPU_WEIGHT);
    }
    return weights;
  }

  private static String partitionName(int clique, int partition) {
    return resourceName(clique) + "_" + partition;
  }

  private static void compareCliques(Map<String, Map<String, Map<String, String>>> actual,
      Map<String, Map<String, Map<String, String>>> reference, String referenceName,
      boolean includeTheBrokenClique, List<String> problems) {
    for (int clique = 0; clique < CLIQUES; clique++) {
      if (clique == BROKEN && !includeTheBrokenClique) {
        continue;
      }
      Map<String, Map<String, String>> expected = reference.get(resourceName(clique));
      Map<String, Map<String, String>> placed = actual.get(resourceName(clique));
      if (expected == null || !expected.equals(placed)) {
        problems.add(resourceName(clique) + " differs from " + referenceName + ": expected "
            + expected + " but was " + placed);
      }
    }
  }

  /** Anything the broken clique's rollback left behind on its nodes or in the zone bookkeeping. */
  private static List<String> rollbackLeftovers(ClusterModel model) {
    List<String> problems = new ArrayList<>();
    for (int index = 0; index < NODES; index++) {
      AssignableNode node = model.getAssignableNodes().get(instanceName(BROKEN, index));
      if (node.getAssignedReplicaCount() != 0) {
        problems.add(node.getInstanceName() + " still holds " + node.getAssignedReplicaCount()
            + " replicas after the rollback");
      }
      if (!node.getRemainingCapacity().equals(node.getMaxCapacity())) {
        problems.add(node.getInstanceName() + " has " + node.getRemainingCapacity()
            + " left of " + node.getMaxCapacity() + " after the rollback");
      }
    }
    model.getContext().getAssignmentForFaultZoneMap().forEach((zone, partitionsByResource) -> {
      Set<String> partitions = partitionsByResource.get(resourceName(BROKEN));
      if (partitions != null && !partitions.isEmpty()) {
        problems.add("fault zone " + zone + " still books " + partitions);
      }
    });
    return problems;
  }

  /** Every way the placement breaks a feature that is switched on, or a basic WAGED invariant. */
  private static List<String> featureViolations(Features features, Run run) {
    List<String> problems = new ArrayList<>();
    Map<String, AssignableNode> nodes = run._model.getAssignableNodes();
    Map<String, Integer> replicasPerInstance = new TreeMap<>();
    Map<String, Integer> replicasPerInstanceAndResource = new TreeMap<>();
    run._result.getOptimalResourceAssignment().forEach((resource, assignment) -> {
      if (resource.equals(resourceName(BROKEN)) && assignment.getMappedPartitions().isEmpty()) {
        return;
      }
      if (assignment.getMappedPartitions().size() != PARTITIONS) {
        problems.add(resource + " placed " + assignment.getMappedPartitions().size()
            + " of " + PARTITIONS + " partitions");
      }
      String tag = cliqueTag(Integer.parseInt(resource.substring(resource.lastIndexOf('_') + 1)));
      for (Partition partition : assignment.getMappedPartitions()) {
        Map<String, String> replicaMap = assignment.getReplicaMap(partition);
        long masters = replicaMap.values().stream().filter(MASTER::equals).count();
        if (replicaMap.size() != REPLICAS || masters != 1) {
          problems.add(partition + " has " + replicaMap);
        }
        Set<String> zones = new HashSet<>();
        for (String instance : replicaMap.keySet()) {
          AssignableNode node = nodes.get(instance);
          if (!node.getInstanceTags().contains(tag)) {
            problems.add(partition + " landed outside its clique on " + instance);
          }
          if (features._faultZones && !zones.add(node.getFaultZone())) {
            problems.add(partition + " has two replicas in " + node.getFaultZone());
          }
          List<String> disabled = node.getDisabledPartitionsMap().get(resource);
          if (disabled != null && disabled.contains(partition.getPartitionName())) {
            problems.add(partition + " landed on " + instance + ", where it is disabled");
          }
          replicasPerInstance.merge(instance, 1, Integer::sum);
          replicasPerInstanceAndResource.merge(instance + "/" + resource, 1, Integer::sum);
        }
      }
    });
    if (features._instancePartitionLimit > 0) {
      replicasPerInstance.forEach((instance, count) -> {
        if (count > features._instancePartitionLimit) {
          problems.add(instance + " holds " + count + " replicas over the instance limit");
        }
      });
    }
    if (features._resourcePartitionLimit > 0) {
      replicasPerInstanceAndResource.forEach((instanceAndResource, count) -> {
        if (count > features._resourcePartitionLimit) {
          problems.add(instanceAndResource + " holds " + count + " over the resource limit");
        }
      });
    }
    nodes.values().forEach(node -> node.getRemainingCapacity().forEach((key, left) -> {
      if (left < 0) {
        problems.add(node.getInstanceName() + " is over its " + key + " capacity by " + -left);
      }
    }));
    return problems;
  }

  /** How many (resource, partition, instance) placements two assignments share, states aside. */
  private static int sharedReplicas(Map<String, Map<String, Map<String, String>>> placement,
      Map<String, Map<String, Map<String, String>>> previous) {
    int shared = 0;
    for (Map.Entry<String, Map<String, Map<String, String>>> resource : placement.entrySet()) {
      for (Map.Entry<String, Map<String, String>> partition : resource.getValue().entrySet()) {
        Map<String, String> before = previous.getOrDefault(resource.getKey(),
            Collections.emptyMap()).getOrDefault(partition.getKey(), Collections.emptyMap());
        for (String instance : partition.getValue().keySet()) {
          if (before.containsKey(instance)) {
            shared++;
          }
        }
      }
    }
    return shared;
  }
}
