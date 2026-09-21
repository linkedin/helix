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
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.TreeSet;

import org.apache.helix.HelixRebalanceException;
import org.apache.helix.constants.InstanceConstants;
import org.apache.helix.controller.rebalancer.waged.constraints.ConstraintBasedAlgorithmFactory;
import org.apache.helix.model.ClusterConfig;
import org.apache.helix.model.InstanceConfig;
import org.apache.helix.model.ResourceConfig;
import org.testng.Assert;
import org.testng.annotations.Test;

/**
 * Production shaped scale checks: 20 cliques of 10 instances, one resource pinned per clique, and
 * every instance additionally carrying the ordinary operational labels a real fleet has (a pool
 * name and an availability zone). Those extra labels are the case that used to collapse the
 * attribution into a single block and silently disable the mode, so the scale runs are done with
 * them present rather than on a synthetic single tag topology.
 */
public class TestIsolationProductionScale {
  private static final String CAPACITY_KEY = "DISK";
  private static final int CLIQUES = 20;
  private static final int NODES_PER_CLIQUE = 10;
  private static final int PARTITIONS = 40;
  private static final int NODE_CAPACITY = 1000;

  private ClusterConfig config(boolean isolation) {
    ClusterConfig clusterConfig = new ClusterConfig("AmbryShaped");
    clusterConfig.setInstanceCapacityKeys(Collections.singletonList(CAPACITY_KEY));
    clusterConfig.setDefaultPartitionWeightMap(Collections.singletonMap(CAPACITY_KEY, 0));
    clusterConfig.setWagedInstanceTagIsolationEnabled(isolation);
    return clusterConfig;
  }

  /**
   * @param brokenCliques cliques whose resource is given a partition weight larger than any single
   *                      node holds, so every one of its replicas has no candidate at all.
   * @param droppedNodes  how many instances to remove from clique 7, modelling participants that
   *                      went away or moved to an instance operation that makes them unassignable.
   */
  private ClusterModel build(ClusterConfig clusterConfig, Set<Integer> brokenCliques,
      int droppedNodes, boolean retagBridge) throws IOException {
    Set<AssignableNode> nodes = new HashSet<>();
    Set<AssignableReplica> replicas = new HashSet<>();
    for (int c = 0; c < CLIQUES; c++) {
      int nodeCount = (c == 7) ? NODES_PER_CLIQUE - droppedNodes : NODES_PER_CLIQUE;
      for (int i = 0; i < nodeCount; i++) {
        String instance = "inst_" + c + "_" + i;
        InstanceConfig instanceConfig = new InstanceConfig(instance);
        instanceConfig
            .setInstanceCapacityMap(Collections.singletonMap(CAPACITY_KEY, NODE_CAPACITY));
        instanceConfig.addTag("clique_" + c);
        // The ordinary operational labels a real fleet carries.
        instanceConfig.addTag("prod_pool");
        instanceConfig.addTag("zone_" + (i % 3));
        if (retagBridge && c == 11 && i == 0) {
          // One instance retagged into a neighbouring clique, which must merge exactly those two
          // cliques and nothing else.
          instanceConfig.addTag("clique_12");
        }
        instanceConfig.setInstanceOperation(InstanceConstants.InstanceOperation.ENABLE);
        nodes.add(new AssignableNode(clusterConfig, instanceConfig, instance));
      }
      ResourceConfig resourceConfig = new ResourceConfig("Res_" + c);
      resourceConfig.getRecord()
          .setSimpleField(ResourceConfig.ResourceConfigProperty.INSTANCE_GROUP_TAG.name(),
              "clique_" + c);
      int weight = brokenCliques.contains(c) ? NODE_CAPACITY + 1 : 100;
      resourceConfig.setPartitionCapacityMap(Collections.singletonMap(
          ResourceConfig.DEFAULT_PARTITION_KEY,
          Collections.singletonMap(CAPACITY_KEY, weight)));
      for (int p = 0; p < PARTITIONS; p++) {
        replicas.add(new AssignableReplica(clusterConfig, resourceConfig, "Res_" + c + "_" + p,
            "ONLINE", 0));
      }
    }
    ClusterContext context = new ClusterContext(replicas, nodes, Collections.emptyMap(),
        Collections.emptyMap(), clusterConfig);
    return new ClusterModel(context, replicas, nodes,
        ClusterModel.RebalanceScopeType.GLOBAL_BASELINE);
  }

  @Test
  public void testOneBrokenCliqueAtScaleDoesNotFreezeTheOtherNineteen() throws Exception {
    ClusterModel model = build(config(true), Collections.singleton(3), 0, false);
    long start = System.currentTimeMillis();
    OptimalAssignment assignment =
        ConstraintBasedAlgorithmFactory.getInstance(Collections.emptyMap()).calculate(model);
    long elapsed = System.currentTimeMillis() - start;
    System.out.println("SCALE-1 elapsedMs=" + elapsed + " skipped=" + assignment
        .getSkippedResources() + " placed=" + assignment.getOptimalResourceAssignment().size());
    Assert.assertEquals(assignment.getSkippedResources(), Collections.singleton("Res_3"));
    Assert.assertEquals(assignment.getOptimalResourceAssignment().size(), CLIQUES - 1);
  }

  @Test
  public void testSeveralBrokenCliquesAtScaleIsolateIndependently() throws Exception {
    Set<Integer> broken = new HashSet<>(java.util.Arrays.asList(1, 4, 9, 15, 18));
    ClusterModel model = build(config(true), broken, 0, false);
    OptimalAssignment assignment =
        ConstraintBasedAlgorithmFactory.getInstance(Collections.emptyMap()).calculate(model);
    Set<String> expected = new TreeSet<>();
    broken.forEach(c -> expected.add("Res_" + c));
    System.out.println("SCALE-2 skipped=" + new TreeSet<>(assignment.getSkippedResources()));
    Assert.assertEquals(new TreeSet<>(assignment.getSkippedResources()), expected);
    Assert.assertEquals(assignment.getOptimalResourceAssignment().size(), CLIQUES - broken.size());
  }

  /**
   * A retagged instance bridges cliques 11 and 12, so breaking 11 must carry 12 over with it and
   * leave the other eighteen alone. This is the share block closure at production scale, with the
   * operational labels present.
   */
  @Test
  public void testBridgedCliquesCarryTogetherAndNothingElseDoes() throws Exception {
    ClusterModel model = build(config(true), Collections.singleton(11), 0, true);
    OptimalAssignment assignment =
        ConstraintBasedAlgorithmFactory.getInstance(Collections.emptyMap()).calculate(model);
    Set<String> skipped = new TreeSet<>(assignment.getSkippedResources());
    System.out.println("SCALE-3 skipped=" + skipped);
    Assert.assertEquals(skipped, new TreeSet<>(java.util.Arrays.asList("Res_11", "Res_12")));
    Assert.assertEquals(assignment.getOptimalResourceAssignment().size(), CLIQUES - 2);
  }

  /**
   * Topology churn on a clique that is not the broken one must not change which clique is isolated.
   */
  @Test
  public void testNodeLossElsewhereDoesNotWidenTheIsolation() throws Exception {
    for (int dropped = 0; dropped <= 5; dropped++) {
      ClusterModel model = build(config(true), Collections.singleton(3), dropped, false);
      OptimalAssignment assignment =
          ConstraintBasedAlgorithmFactory.getInstance(Collections.emptyMap()).calculate(model);
      Assert.assertEquals(assignment.getSkippedResources(), Collections.singleton("Res_3"),
          "isolation widened after dropping " + dropped + " nodes from clique 7");
    }
  }

  /**
   * The cost of the mode on a healthy cluster, which is the case that runs every pipeline tick.
   */
  @Test
  public void testHealthyClusterCostOfTheModeIsNegligible() throws Exception {
    List<Long> on = new ArrayList<>();
    List<Long> off = new ArrayList<>();
    for (int round = 0; round < 6; round++) {
      off.add(timeHealthy(false));
      on.add(timeHealthy(true));
    }
    long medianOn = median(on);
    long medianOff = median(off);
    System.out.println(
        "SCALE-PERF healthy medianOffMs=" + medianOff + " medianOnMs=" + medianOn + " onRuns=" + on
            + " offRuns=" + off);
    Assert.assertTrue(medianOn <= Math.max(medianOff * 2, medianOff + 250),
        "enabling the mode should not measurably slow a healthy cluster: off=" + medianOff
            + " on=" + medianOn);
  }

  private long timeHealthy(boolean isolation) throws IOException, HelixRebalanceException {
    ClusterModel model = build(config(isolation), Collections.emptySet(), 0, false);
    long start = System.nanoTime();
    ConstraintBasedAlgorithmFactory.getInstance(Collections.emptyMap()).calculate(model);
    return (System.nanoTime() - start) / 1_000_000;
  }

  private static long median(List<Long> values) {
    List<Long> sorted = new ArrayList<>(values);
    Collections.sort(sorted);
    return sorted.get(sorted.size() / 2);
  }
}
