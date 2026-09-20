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
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;

import org.apache.helix.constants.InstanceConstants;
import org.apache.helix.controller.rebalancer.waged.RebalanceAlgorithm;
import org.apache.helix.controller.rebalancer.waged.constraints.ConstraintBasedAlgorithmFactory;
import org.apache.helix.model.ClusterConfig;
import org.apache.helix.model.InstanceConfig;
import org.apache.helix.model.Partition;
import org.apache.helix.model.ResourceAssignment;
import org.apache.helix.model.ResourceConfig;

/**
 * Shared fixture for the WAGED instance-tag ("clique") isolation tests, gated by
 * {@link ClusterConfig#setWagedInstanceTagIsolationEnabled}.
 *
 * The topology under test is the clique partitioned one: the cluster is carved into disjoint
 * cliques, every instance carries exactly one clique tag, and every resource is pinned to one
 * clique tag through INSTANCE_GROUP_TAG. {@link TestCliqueFailureBlastRadius} measures how far
 * one unplaceable clique reaches: the whole cluster's rebalance in the default mode, and only that
 * clique's share block with isolation on.
 *
 * The broadest suites on this fixture:
 * <ul>
 *   <li>{@link TestWagedInstanceTagIsolationCore} covers isolation, atomic rollback,
 *       determinism and parity with the default global mode,</li>
 *   <li>{@link TestWagedInstanceTagIsolationBehavior} covers the flag, an isolation outcome that
 *       does not depend on input order, whole-tag carry-forward, every rebalance scope and the
 *       untagged and overlapping-tag cases that must still fail exactly like the default global
 *       mode,</li>
 *   <li>{@link TestWagedInstanceTagIsolationCapacity} covers cluster wide capacity deficit
 *       attribution and the hard constraint reporter parity,</li>
 *   <li>{@link TestWagedIsolationFeatureParity} switches on one WAGED placement feature at a
 *       time and checks each against the default global mode, with and without a broken
 *       clique.</li>
 * </ul>
 *
 * The cluster-model builders, assertion helpers and constants the subclasses share live here.
 */
abstract class AbstractTestWagedInstanceTagIsolation {
  protected static final int CLIQUE_COUNT = 20;
  protected static final int NODES_PER_CLIQUE = 10;
  protected static final int PARTITIONS_PER_RESOURCE = 10;
  protected static final String CAPACITY_KEY = "DISK";
  protected static final int NODE_CAPACITY = 100;
  protected static final int HEALTHY_PARTITION_WEIGHT = 10;
  // Larger than a single node's capacity, so NodeCapacityConstraint rejects every node in the
  // clique and none of that resource's replicas has a candidate at all.
  protected static final int UNPLACEABLE_PARTITION_WEIGHT = 150;

  protected static String cliqueTag(int clique) {
    return "clique_" + clique;
  }

  protected static String resourceName(int clique) {
    return "Resource_clique_" + clique;
  }

  protected static String instanceName(int clique, int index) {
    return "instance_" + clique + "_" + index;
  }

  /**
   * Describes one clique's shape so individual tests can perturb a single clique without touching
   * the others.
   */
  protected static final class CliqueSpec {
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
      return new CliqueSpec(_nodeCount, partitionCount, _partitionWeight,
          _nonAssignableNodeIndices);
    }

    /**
     * Removes nodes from the clique. Models a participant that went away, was decommissioned, or
     * was moved to an instance operation that {@code InstanceConfig#isAssignable} rejects
     * (EVACUATE, UNKNOWN, SWAP_IN), all of which drop the node before the algorithm ever runs.
     */
    CliqueSpec withNodeCount(int nodeCount) {
      return new CliqueSpec(nodeCount, _partitionCount, _partitionWeight,
          _nonAssignableNodeIndices);
    }
  }

  protected static Map<Integer, CliqueSpec> allHealthy() {
    Map<Integer, CliqueSpec> specs = new HashMap<>();
    for (int clique = 0; clique < CLIQUE_COUNT; clique++) {
      specs.put(clique, CliqueSpec.healthy());
    }
    return specs;
  }

  protected ClusterConfig createClusterConfig(boolean tagIsolationEnabled) {
    ClusterConfig clusterConfig = new ClusterConfig("CliquePartitionedCluster");
    clusterConfig.setInstanceCapacityKeys(Collections.singletonList(CAPACITY_KEY));
    clusterConfig.setDefaultPartitionWeightMap(Collections.singletonMap(CAPACITY_KEY, 0));
    clusterConfig.setWagedInstanceTagIsolationEnabled(tagIsolationEnabled);
    return clusterConfig;
  }

  protected Set<AssignableNode> createNodes(ClusterConfig clusterConfig,
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
        nodes.add(new AssignableNode(clusterConfig, instanceConfig, instance));
      }
    }
    return nodes;
  }

  protected Set<AssignableReplica> createReplicas(ClusterConfig clusterConfig,
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

  protected ClusterModel createClusterModel(ClusterConfig clusterConfig,
      Map<Integer, CliqueSpec> specs) throws IOException {
    Set<AssignableReplica> replicas = createReplicas(clusterConfig, specs);
    Set<AssignableNode> nodes = createNodes(clusterConfig, specs);
    ClusterContext context = new ClusterContext(replicas, nodes, Collections.emptyMap(),
        Collections.emptyMap(), clusterConfig);
    return new ClusterModel(context, replicas, nodes,
        ClusterModel.RebalanceScopeType.GLOBAL_BASELINE);
  }

  protected RebalanceAlgorithm createAlgorithm() {
    return ConstraintBasedAlgorithmFactory.getInstance(Collections.emptyMap());
  }

  /**
   * Normalizes an assignment into a comparable, order independent structure so two runs can be
   * compared byte for byte.
   */
  protected static Map<String, Map<String, Map<String, String>>> normalize(
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

  protected ResourceConfig taggedResource(String resource, String tag, int weight)
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

  protected void addReplicas(Set<AssignableReplica> replicas, ClusterConfig clusterConfig,
      ResourceConfig resourceConfig, int partitionCount) {
    for (int p = 0; p < partitionCount; p++) {
      replicas.add(new AssignableReplica(clusterConfig, resourceConfig,
          resourceConfig.getResourceName() + "_" + p, "ONLINE", 0));
    }
  }

  protected AssignableNode taggedNode(ClusterConfig clusterConfig, String instance, int zone,
      String... tags) {
    InstanceConfig instanceConfig = new InstanceConfig(instance);
    instanceConfig.setInstanceCapacityMap(Collections.singletonMap(CAPACITY_KEY, NODE_CAPACITY));
    for (String tag : tags) {
      instanceConfig.addTag(tag);
    }
    instanceConfig.setInstanceOperation(InstanceConstants.InstanceOperation.ENABLE);
    return new AssignableNode(clusterConfig, instanceConfig, instance);
  }

  /** Like {@link #taggedNode}, with the given DISK capacity instead of {@link #NODE_CAPACITY}. */
  protected static AssignableNode sizedNode(ClusterConfig clusterConfig, String instance,
      int capacity, String... tags) {
    InstanceConfig instanceConfig = new InstanceConfig(instance);
    instanceConfig.setInstanceCapacityMap(Collections.singletonMap(CAPACITY_KEY, capacity));
    for (String tag : tags) {
      instanceConfig.addTag(tag);
    }
    instanceConfig.setInstanceOperation(InstanceConstants.InstanceOperation.ENABLE);
    return new AssignableNode(clusterConfig, instanceConfig, instance);
  }

  /**
   * Builds a delayed overwrite model the way ClusterModelProvider does. The population is every
   * replica of every resource added. Each live node holds a replica of every partition the current
   * assignment names it for, whether or not the resource config has that partition, and a
   * mapped partition with fewer live replicas than its min active count gets the missing ones to
   * assign. An instance the current assignment names that is not a live node is offline inside
   * its delay window: it is not in the model, and its replicas are in the current assignment only.
   */
  protected static final class DelayedOverwriteCluster {
    private final ClusterConfig _config;
    private final Map<String, AssignableNode> _liveNodes = new HashMap<>();
    private final Map<String, ResourceConfig> _resources = new HashMap<>();
    private final Map<String, Integer> _minActive = new HashMap<>();
    private final Set<AssignableReplica> _population = new HashSet<>();
    private final Map<String, ResourceAssignment> _current = new HashMap<>();

    DelayedOverwriteCluster(ClusterConfig config) {
      _config = config;
    }

    DelayedOverwriteCluster liveNode(String instance, int capacity, String... tags) {
      _liveNodes.put(instance, sizedNode(_config, instance, capacity, tags));
      return this;
    }

    /** Adds partitions times replicas of the resource to the population. */
    DelayedOverwriteCluster resource(ResourceConfig resource, int partitions, int replicas,
        int minActive) {
      String name = resource.getResourceName();
      _resources.put(name, resource);
      _minActive.put(name, Math.min(minActive, replicas));
      for (int p = 0; p < partitions; p++) {
        for (int r = 0; r < replicas; r++) {
          _population.add(new AssignableReplica(_config, resource, name + "_" + p, "ONLINE", 0));
        }
      }
      return this;
    }

    /** Names the instances holding one partition in the current assignment. */
    DelayedOverwriteCluster current(String resource, int partition, String... instances) {
      Map<String, String> replicas = new TreeMap<>();
      for (String instance : instances) {
        replicas.put(instance, "ONLINE");
      }
      _current.computeIfAbsent(resource, ResourceAssignment::new)
          .addReplicaMap(new Partition(resource + "_" + partition), replicas);
      return this;
    }

    ClusterModel build() {
      Set<AssignableReplica> toAssign = new HashSet<>();
      Map<String, Set<AssignableReplica>> allocated = new HashMap<>();
      _current.forEach((resource, assignment) -> {
        ResourceConfig config = _resources.get(resource);
        for (Partition partition : assignment.getMappedPartitions()) {
          String name = partition.getPartitionName();
          int live = 0;
          for (String instance : assignment.getReplicaMap(partition).keySet()) {
            if (_liveNodes.containsKey(instance)) {
              allocated.computeIfAbsent(instance, key -> new HashSet<>())
                  .add(new AssignableReplica(_config, config, name, "ONLINE", 0));
              live++;
            }
          }
          for (int i = live; i < _minActive.get(resource); i++) {
            toAssign.add(new AssignableReplica(_config, config, name, "ONLINE", 0));
          }
        }
      });
      allocated.forEach(
          (instance, replicas) -> _liveNodes.get(instance).assignInitBatch(replicas));
      Set<AssignableNode> nodes = new HashSet<>(_liveNodes.values());
      ClusterContext context = new ClusterContext(_population, nodes, Collections.emptyMap(),
          _current, _config);
      return new ClusterModel(context, toAssign, nodes,
          ClusterModel.RebalanceScopeType.DELAYED_REBALANCE_OVERWRITES);
    }
  }
}
