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
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Collections;
import java.util.HashSet;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.TreeMap;

import org.apache.helix.HelixRebalanceException;
import org.apache.helix.constants.InstanceConstants;
import org.apache.helix.controller.rebalancer.waged.constraints.ConstraintBasedAlgorithmFactory;
import org.apache.helix.model.ClusterConfig;
import org.apache.helix.model.InstanceConfig;
import org.apache.helix.model.ResourceAssignment;
import org.apache.helix.model.ResourceConfig;
import org.testng.annotations.Test;

/**
 * Dumps the assignment this branch computes for a fixed battery of pseudo random clique topologies,
 * so the file can be diffed byte for byte against the same battery run on the base branch.
 *
 * Deliberately self contained. It touches no API that only exists on one of the two branches, so
 * the identical source file compiles and runs on both, and never enables instance tag isolation.
 * That makes the diff a real parity proof rather than a within branch comparison.
 */
public class TestWagedParityDump {
  private static final String CAPACITY_KEY = "DISK";

  @Test
  public void dumpParityBattery() throws Exception {
    StringBuilder dump = new StringBuilder();
    for (int seed = 0; seed < 60; seed++) {
      dump.append("seed=").append(seed).append(' ').append(run(seed)).append('\n');
    }
    String out = System.getProperty("parity.dump", "/tmp/waged-parity.txt");
    Files.write(Paths.get(out), dump.toString().getBytes());
    System.out.println("PARITY-DUMP wrote " + out + " bytes=" + dump.length());
  }

  private Object run(int seed) throws IOException, HelixRebalanceException {
    Random random = new Random(seed);
    ClusterConfig clusterConfig = new ClusterConfig("ParityCluster");
    clusterConfig.setInstanceCapacityKeys(Collections.singletonList(CAPACITY_KEY));
    clusterConfig.setDefaultPartitionWeightMap(Collections.singletonMap(CAPACITY_KEY, 0));

    Set<AssignableNode> nodes = new HashSet<>();
    Set<AssignableReplica> replicas = new HashSet<>();
    int cliques = 3 + random.nextInt(6);
    for (int c = 0; c < cliques; c++) {
      int nodeCount = 3 + random.nextInt(5);
      boolean sharedLabel = random.nextBoolean();
      for (int i = 0; i < nodeCount; i++) {
        String instance = "n_" + c + "_" + i;
        InstanceConfig instanceConfig = new InstanceConfig(instance);
        instanceConfig.setInstanceCapacityMap(Collections.singletonMap(CAPACITY_KEY, 100));
        instanceConfig.addTag("clique_" + c);
        if (sharedLabel) {
          instanceConfig.addTag("prod_pool");
        }
        instanceConfig.setInstanceOperation(InstanceConstants.InstanceOperation.ENABLE);
        nodes.add(new AssignableNode(clusterConfig, instanceConfig, instance));
      }
      ResourceConfig resourceConfig = new ResourceConfig("Resource_" + c);
      resourceConfig.getRecord()
          .setSimpleField(ResourceConfig.ResourceConfigProperty.INSTANCE_GROUP_TAG.name(),
              "clique_" + c);
      resourceConfig.setPartitionCapacityMap(Collections.singletonMap(
          ResourceConfig.DEFAULT_PARTITION_KEY,
          Collections.singletonMap(CAPACITY_KEY, 1 + random.nextInt(8))));
      int partitions = 1 + random.nextInt(nodeCount * 2);
      for (int p = 0; p < partitions; p++) {
        replicas.add(new AssignableReplica(clusterConfig, resourceConfig,
            "Resource_" + c + "_" + p, "ONLINE", 0));
      }
    }

    ClusterContext context = new ClusterContext(replicas, nodes, Collections.emptyMap(),
        Collections.emptyMap(), clusterConfig);
    ClusterModel model = new ClusterModel(context, replicas, nodes,
        ClusterModel.RebalanceScopeType.GLOBAL_BASELINE);
    Map<String, ResourceAssignment> assignment =
        ConstraintBasedAlgorithmFactory.getInstance(Collections.emptyMap()).calculate(model)
            .getOptimalResourceAssignment();

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
}
