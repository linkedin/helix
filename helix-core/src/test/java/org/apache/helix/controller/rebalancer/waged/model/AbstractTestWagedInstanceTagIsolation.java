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
import org.apache.helix.model.ResourceAssignment;
import org.apache.helix.model.ResourceConfig;

/**
 * Shared fixture for the WAGED instance-tag ("clique") isolation tests, gated by
 * {@link ClusterConfig#setWagedInstanceTagIsolationEnabled}.
 *
 * The topology under test is the clique partitioned one: the cluster is carved into disjoint
 * cliques, every instance carries exactly one clique tag, and every resource is pinned to one
 * clique tag through INSTANCE_GROUP_TAG. {@link TestCliqueFailureBlastRadius} documents the
 * default behavior, where a single unplaceable clique aborts the whole cluster's rebalance.
 *
 * The concrete cases live in the subclasses:
 * <ul>
 *   <li>{@link TestWagedInstanceTagIsolationCore} covers isolation, atomic rollback,
 *       determinism and parity with the default global mode,</li>
 *   <li>{@link TestWagedInstanceTagIsolationBehavior} covers the flag, group ordering,
 *       carry-forward, every rebalance scope and the untagged/overlapping cases that must
 *       still fail globally,</li>
 *   <li>{@link TestWagedInstanceTagIsolationCapacity} covers cluster wide capacity deficit
 *       attribution and the hard constraint reporter parity.</li>
 * </ul>
 *
 * The cluster-model builders, assertion helpers and constants shared by all three live here.
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
}
