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

import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import org.apache.helix.HelixRebalanceException;
import org.apache.helix.model.ClusterConfig;
import org.apache.helix.model.Partition;
import org.apache.helix.model.ResourceAssignment;
import org.apache.helix.model.ResourceConfig;
import org.testng.Assert;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

/**
 * Capacity deficit attribution after a node moves from a broken clique into a healthy one.
 *
 * A broken clique keeps its previous assignment, so its replicas stay on the nodes they were last
 * computed on. When one of those nodes is retagged into a healthy clique while the other is still
 * broken, the partial and emergency models preload the carried replicas onto it. That load is
 * charged to the clique that owns the replicas. Charged to the clique the node is in now, it
 * would make the healthy clique pay for the broken clique's replicas and be blamed along with it,
 * and the scope would rethrow.
 */
public class TestWagedIsolationRetaggedNodeDemand extends AbstractTestWagedInstanceTagIsolation {
  private static final String HEALTHY = resourceName(0);
  private static final String BROKEN = resourceName(1);
  private static final String RETAGGED = instanceName(1, 0);
  // Clique 2's resource in the room case, which needs 300 on a node of 100.
  private static final String OVERSIZED = resourceName(2);
  // Clique 1's replicas at their current weight, too heavy for the nodes they were placed on.
  private static final int BROKEN_WEIGHT = 90;
  private static final int CARRIED_ON_RETAGGED_NODE = 4;

  @DataProvider(name = "preloadedScopes")
  public Object[][] preloadedScopes() {
    return new Object[][] {
        {ClusterModel.RebalanceScopeType.PARTIAL, true},
        {ClusterModel.RebalanceScopeType.PARTIAL, false},
        {ClusterModel.RebalanceScopeType.EMERGENCY, true},
        {ClusterModel.RebalanceScopeType.EMERGENCY, false}
    };
  }

  /**
   * With ownNodeOverflows, clique 1 also overflows the node it kept, so charging the retagged node
   * to clique 0 would blame both cliques and make the scope rethrow. Without it, that charge would
   * blame only the healthy clique: it would be set aside and the broken clique's replicas emitted
   * where they sit, on a node of the other clique.
   */
  @Test(dataProvider = "preloadedScopes")
  public void testRetaggedNodeLoadStaysWithTheCliqueThatOwnsIt(
      ClusterModel.RebalanceScopeType scope, boolean ownNodeOverflows) throws Exception {
    // The retagged node is over full, so the healthy replica can only go to the other two.
    assertHealthyCliqueIsPlaced(createAlgorithm().calculate(model(scope, true, ownNodeOverflows)),
        instanceName(0, 0), instanceName(0, 1));
  }

  /**
   * The baseline places every replica from scratch, so nothing is preloaded onto the retagged node
   * and the attribution has nothing to misplace. It is the control for the cases above.
   */
  @Test
  public void testBaselineAttributesTheSameClique() throws Exception {
    for (boolean ownNodeOverflows : new boolean[] {true, false}) {
      assertHealthyCliqueIsPlaced(createAlgorithm().calculate(
          model(ClusterModel.RebalanceScopeType.GLOBAL_BASELINE, true, ownNodeOverflows)),
          instanceName(0, 0), instanceName(0, 1), RETAGGED);
    }
  }

  @Test(dataProvider = "preloadedScopes")
  public void testFlagOffStillThrows(ClusterModel.RebalanceScopeType scope,
      boolean ownNodeOverflows) throws Exception {
    try {
      createAlgorithm().calculate(model(scope, false, ownNodeOverflows));
      Assert.fail("The default mode must fail on the cluster wide deficit");
    } catch (HelixRebalanceException e) {
      Assert.assertEquals(e.getFailureCategory(),
          HelixRebalanceException.FailureCategory.CAPACITY_DEFICIT);
    }
  }

  /**
   * The delayed overwrite also preloads the retagged node with replicas of partitions the broken
   * resource's config does not have. They are outside the population the check sums, so they move
   * no demand: charged to clique 0 they would put it at 260 against 200 and carry it in place of
   * clique 1. Only clique 1 is carried, and clique 0's top up lands on its first node.
   */
  @Test
  public void testDelayedOverwriteReplicasOfMissingPartitionsMoveNoDemand() throws Exception {
    assertDelayedTopUpLands(createAlgorithm().calculate(delayedModel(true, false)));
  }

  /**
   * The same with second copies, on the retagged node, of broken partitions that have a single
   * replica, as the current assignment shows when the replica count is lowered while the clique
   * is carried. Each partition counts only the replica its population holds, the one on clique 1's
   * own node, so the copies move no demand either and only clique 1 is carried.
   */
  @Test
  public void testDelayedOverwriteCopiesBeyondTheReplicaCountMoveNoDemand() throws Exception {
    assertDelayedTopUpLands(createAlgorithm().calculate(delayedModel(true, true)));
  }

  /**
   * The retagged node holds 150 on 100 of capacity: partition 2 of clique 1's resource at 50,
   * which its population counts, and partitions 4 and 5 at 50 each, which clique 1's config does
   * not have. Replicas outside the population take no room on the node, so partition 2's 50 moves
   * to clique 0, which owns the node and fits at 150 of 200, and clique 1 fits at 100 on its own
   * node of 100. Only clique 2, which needs 300 on its node of 100, is carried, and clique 0's top
   * up lands on its first node.
   */
  @Test
  public void testReplicasOutsideThePopulationTakeNoRoomFromAForeignReplica() throws Exception {
    OptimalAssignment result = createAlgorithm().calculate(roomModel(true));
    Assert.assertEquals(result.getSkippedResources(), Collections.singleton(OVERSIZED));
    Map<String, ResourceAssignment> assignment = result.getOptimalResourceAssignment();
    Set<String> computed = new HashSet<>(assignment.keySet());
    computed.removeAll(result.getSkippedResources());
    Assert.assertEquals(computed, new HashSet<>(Arrays.asList(HEALTHY, resourceName(1))));
    // The retagged node is over full, so the top up can only go to the first node.
    Assert.assertEquals(
        assignment.get(HEALTHY).getReplicaMap(new Partition(HEALTHY + "_3")).keySet(),
        Collections.singleton(instanceName(0, 0)));
  }

  /** The default mode fails the three delayed overwrite clusters on the cluster wide deficit. */
  @Test
  public void testDelayedOverwriteFlagOffStillThrows() throws Exception {
    for (ClusterModel model : new ClusterModel[] {delayedModel(false, false),
        delayedModel(false, true), roomModel(false)}) {
      try {
        createAlgorithm().calculate(model);
        Assert.fail("The default mode must fail on the cluster wide deficit");
      } catch (HelixRebalanceException e) {
        Assert.assertEquals(e.getFailureCategory(),
            HelixRebalanceException.FailureCategory.CAPACITY_DEFICIT);
      }
    }
  }

  private static void assertDelayedTopUpLands(OptimalAssignment result) {
    Assert.assertEquals(result.getSkippedResources(), Collections.singleton(BROKEN));
    Map<String, ResourceAssignment> assignment = result.getOptimalResourceAssignment();
    Set<String> computed = new HashSet<>(assignment.keySet());
    computed.removeAll(result.getSkippedResources());
    Assert.assertEquals(computed, Collections.singleton(HEALTHY));
    // The retagged node is full, so the top up can only go to the other live node.
    Assert.assertEquals(
        assignment.get(HEALTHY).getReplicaMap(new Partition(HEALTHY + "_3")).keySet(),
        Collections.singleton(instanceName(0, 0)));
  }

  private static void assertHealthyCliqueIsPlaced(OptimalAssignment result, String... allowed) {
    Assert.assertEquals(result.getSkippedResources(), Collections.singleton(BROKEN));
    Map<String, ResourceAssignment> assignment = result.getOptimalResourceAssignment();
    Set<String> computed = new HashSet<>(assignment.keySet());
    computed.removeAll(result.getSkippedResources());
    Assert.assertEquals(computed, Collections.singleton(HEALTHY));
    Map<String, String> replicas =
        assignment.get(HEALTHY).getReplicaMap(new Partition(HEALTHY + "_0"));
    Assert.assertEquals(replicas.size(), 1);
    String instance = replicas.keySet().iterator().next();
    Assert.assertTrue(Arrays.asList(allowed).contains(instance), instance);
  }

  /**
   * Clique 0 has two nodes of its own plus the retagged node, which still holds four of clique 1's
   * replicas, and one replica waiting to be placed. Clique 1 is left with one node, holding one or
   * two more of its replicas. The whole cluster holds 400 and is asked for 460 or 550.
   */
  private ClusterModel model(ClusterModel.RebalanceScopeType scope, boolean enabled,
      boolean ownNodeOverflows) throws Exception {
    ClusterConfig config = createClusterConfig(enabled);
    Set<AssignableNode> nodes = new HashSet<>();
    nodes.add(taggedNode(config, instanceName(0, 0), 0, cliqueTag(0)));
    nodes.add(taggedNode(config, instanceName(0, 1), 1, cliqueTag(0)));
    AssignableNode retagged = taggedNode(config, RETAGGED, 2, cliqueTag(0));
    nodes.add(retagged);
    AssignableNode kept = taggedNode(config, instanceName(1, 1), 3, cliqueTag(1));
    nodes.add(kept);

    Set<AssignableReplica> toBeAssigned = new HashSet<>();
    addReplicas(toBeAssigned, config,
        taggedResource(HEALTHY, cliqueTag(0), HEALTHY_PARTITION_WEIGHT), 1);
    ResourceConfig broken = taggedResource(BROKEN, cliqueTag(1), BROKEN_WEIGHT);
    int brokenPartitions = CARRIED_ON_RETAGGED_NODE + (ownNodeOverflows ? 2 : 1);
    Set<AssignableReplica> onRetagged = new HashSet<>();
    Set<AssignableReplica> onKept = new HashSet<>();
    for (int p = 0; p < brokenPartitions; p++) {
      (p < CARRIED_ON_RETAGGED_NODE ? onRetagged : onKept)
          .add(new AssignableReplica(config, broken, BROKEN + "_" + p, "ONLINE", 0));
    }

    Set<AssignableReplica> all = new HashSet<>(toBeAssigned);
    all.addAll(onRetagged);
    all.addAll(onKept);
    if (scope == ClusterModel.RebalanceScopeType.GLOBAL_BASELINE) {
      toBeAssigned.addAll(onRetagged);
      toBeAssigned.addAll(onKept);
    } else {
      retagged.assignInitBatch(onRetagged);
      kept.assignInitBatch(onKept);
    }
    ClusterContext context =
        new ClusterContext(all, nodes, Collections.emptyMap(), Collections.emptyMap(), config);
    return new ClusterModel(context, toBeAssigned, nodes, scope);
  }

  /**
   * The delayed overwrite model. Clique 0 has its first node and the retagged node live, and its
   * second and third nodes offline inside their window. Its resource has four partitions of two
   * replicas at 20 and min active 1: three with one replica on the first node and one parked, and
   * one with both parked, which gets a top up. Clique 1's resource has partitions of one replica
   * at 50, three of them on its remaining node. The retagged node holds two more replicas of it,
   * of partitions its config does not have or, with extraCopies, second copies of two more
   * partitions that also sit on clique 1's node. The cluster holds 300 and is asked for 310 or
   * 410.
   */
  private ClusterModel delayedModel(boolean enabled, boolean extraCopies) throws Exception {
    String first = instanceName(0, 0);
    String kept = instanceName(1, 1);
    DelayedOverwriteCluster cluster = new DelayedOverwriteCluster(createClusterConfig(enabled))
        .liveNode(first, NODE_CAPACITY, cliqueTag(0))
        .liveNode(RETAGGED, NODE_CAPACITY, cliqueTag(0))
        .liveNode(kept, NODE_CAPACITY, cliqueTag(1))
        .resource(taggedResource(HEALTHY, cliqueTag(0), 20), 4, 2, 1)
        .resource(taggedResource(BROKEN, cliqueTag(1), 50), extraCopies ? 5 : 3, 1, 1);
    for (int p = 0; p < 3; p++) {
      cluster.current(HEALTHY, p, first, instanceName(0, 1));
      cluster.current(BROKEN, p, kept);
    }
    cluster.current(HEALTHY, 3, instanceName(0, 1), instanceName(0, 2));
    for (int p = 3; p < 5; p++) {
      if (extraCopies) {
        cluster.current(BROKEN, p, kept, RETAGGED);
      } else {
        cluster.current(BROKEN, p, RETAGGED);
      }
    }
    return cluster.build();
  }

  /**
   * The delayed overwrite model for the room left on the retagged node. Clique 0 has its first
   * node and the retagged node live, and its second node offline inside its window. Its resource
   * has four partitions of one replica at 25: three on the first node and one on the second
   * node, which gets a top up. Clique 1's resource has three partitions of one replica at 50, two
   * on its remaining node and the third on the retagged node, which also holds partitions 4 and 5
   * of it. Clique 2's resource has three partitions of one replica at 100, the first on its only
   * node of 100. The cluster holds 400 and is asked for 550.
   */
  private ClusterModel roomModel(boolean enabled) throws Exception {
    String first = instanceName(0, 0);
    String kept = instanceName(1, 1);
    String owner = resourceName(1);
    DelayedOverwriteCluster cluster = new DelayedOverwriteCluster(createClusterConfig(enabled))
        .liveNode(first, NODE_CAPACITY, cliqueTag(0))
        .liveNode(RETAGGED, NODE_CAPACITY, cliqueTag(0))
        .liveNode(kept, NODE_CAPACITY, cliqueTag(1))
        .liveNode(instanceName(2, 0), NODE_CAPACITY, cliqueTag(2))
        .resource(taggedResource(HEALTHY, cliqueTag(0), 25), 4, 1, 1)
        .resource(taggedResource(owner, cliqueTag(1), 50), 3, 1, 1)
        .resource(taggedResource(OVERSIZED, cliqueTag(2), 100), 3, 1, 1);
    for (int p = 0; p < 3; p++) {
      cluster.current(HEALTHY, p, first);
    }
    cluster.current(HEALTHY, 3, instanceName(0, 1))
        .current(owner, 0, kept)
        .current(owner, 1, kept)
        .current(OVERSIZED, 0, instanceName(2, 0));
    for (int p : new int[] {2, 4, 5}) {
      cluster.current(owner, p, RETAGGED);
    }
    return cluster.build();
  }
}
