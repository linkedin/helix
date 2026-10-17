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
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.apache.helix.HelixRebalanceException;
import org.apache.helix.model.ClusterConfig;
import org.apache.helix.model.Partition;
import org.apache.helix.model.ResourceAssignment;
import org.testng.Assert;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

/**
 * Capacity deficit attribution in the delayed overwrite scope, whose model leaves out every node
 * that is offline inside its delay window.
 *
 * The replicas on such a node are part of the cluster wide check's total, but this scope does not
 * place them: they are parked there until the node returns or the window closes. Each clique is
 * judged first by what this scope must place, so a clique holding more than its live nodes while
 * a node is away is not blamed for that. Only when this verdict leaves a deficit behind is every
 * clique judged by all the replicas it owns. Either way the cliques left to rebalance are held to
 * the check's full totals, which is how the default mode judges a cluster made of them alone, so a
 * clique above its live share keeps rebalancing only while the others' spare room covers what it
 * has parked. See {@link AbstractTestWagedInstanceTagIsolation} for the shared fixture.
 */
public class TestWagedIsolationParkedDemand extends AbstractTestWagedInstanceTagIsolation {
  private static final String HEALTHY = "RH";
  private static final String REBALANCED = "RG";
  private static final String BROKEN = "RY";
  private static final String BROKEN_SINGLE = "RY_single";
  private static final Set<String> H_LIVE = set("h1", "h2", "h3");
  private static final Set<String> G_LIVE = set("g1", "g2", "g3");
  // RG at 250 of 300, which leaves G 50 of spare room against the 75 clique H has parked.
  private static final int TIGHT_RG_WEIGHT = 25;

  @DataProvider(name = "withBrokenClique")
  public Object[][] withBrokenClique() {
    return new Object[][] {{true}, {false}};
  }

  @DataProvider(name = "spareCoveringParked")
  public Object[][] spareCoveringParked() {
    return new Object[][] {
        // RG at 200 of 300: G has 100 of spare room.
        {20, 100},
        // RG at 250 with g3 at 125: G has exactly 75, so the remainder is exactly full.
        {TIGHT_RG_WEIGHT, 125}
    };
  }

  /**
   * Clique H holds 375 against 300 of live capacity, with 75 parked on h4 and h5, so what this
   * scope must place for H is exactly its live capacity. Clique G needs 120 of 300 and clique Y
   * needs 300 of 100. The cluster holds 700 and is asked for 795, so the check fails. Only Y is
   * carried: H is topped up to min active on its live nodes and G is rebalanced.
   */
  @Test
  public void testCliqueAboveItsLiveShareIsToppedUpWhileTheBrokenCliqueIsCarried()
      throws Exception {
    OptimalAssignment result = createAlgorithm().calculate(aboveLiveShare(true));
    Assert.assertEquals(result.getSkippedResources(), set(BROKEN));
    Map<String, ResourceAssignment> assignment = result.getOptimalResourceAssignment();
    assertToppedUpWithinH(assignment.get(HEALTHY));
    assertRebalancedWithinG(assignment.get(REBALANCED));
  }

  /**
   * Clique H as above, with G at 250 of 300 and Y at 300 of 100: the cluster holds 700 and is
   * asked for 925. Judged by what this scope places, only Y is at fault, but H and G are then
   * asked for 625 against 600, since G's 50 of spare room cannot cover the 75 H has parked.
   * Judged by every replica they own, H and Y are at fault and G fits on its own at 250 of 300.
   * The survivors are held to the check's full totals, exactly as the default mode judges a
   * cluster made of them alone, so RH and RY are carried and RG_0 is topped up within G. Without
   * clique Y the cluster is H and G alone, and RH is carried all the same.
   */
  @Test(dataProvider = "withBrokenClique")
  public void testCliqueAboveItsLiveShareIsCarriedWhenTheRemainderCannotCoverItsParkedReplicas(
      boolean withBrokenClique) throws Exception {
    OptimalAssignment result = createAlgorithm()
        .calculate(aboveLiveShareWithSpare(true, withBrokenClique, TIGHT_RG_WEIGHT, 100));
    Assert.assertEquals(result.getSkippedResources(),
        withBrokenClique ? set(HEALTHY, BROKEN) : set(HEALTHY));
    assertRebalancedWithinG(result.getOptimalResourceAssignment().get(REBALANCED));
  }

  /**
   * The default mode refuses that cluster with or without clique Y: it is asked for 925 against
   * 700, and H and G alone for 625 against 600.
   */
  @Test(dataProvider = "withBrokenClique")
  public void testFlagOffThrowsWhenTheRemainderCannotCoverTheParkedReplicas(
      boolean withBrokenClique) throws Exception {
    assertCapacityDeficit(
        aboveLiveShareWithSpare(false, withBrokenClique, TIGHT_RG_WEIGHT, 100));
  }

  /**
   * The same cluster with G's spare room covering the 75 H has parked: RG at 200 of 300, or RG at
   * 250 with g3 at 125, which leaves exactly 75, so what is left holds 625 against 625. Only Y is
   * at fault and H and G fit in what is left, so RY is carried, RH_0 is topped up to min active
   * on h3 and RG_0 within G.
   */
  @Test(dataProvider = "spareCoveringParked")
  public void testCliqueAboveItsLiveShareIsToppedUpWhileTheRemainderCoversItsParkedReplicas(
      int rgWeight, int g3Capacity) throws Exception {
    OptimalAssignment result =
        createAlgorithm().calculate(aboveLiveShareWithSpare(true, true, rgWeight, g3Capacity));
    Assert.assertEquals(result.getSkippedResources(), set(BROKEN));
    Map<String, ResourceAssignment> assignment = result.getOptimalResourceAssignment();
    assertToppedUpWithinH(assignment.get(HEALTHY));
    assertRebalancedWithinG(assignment.get(REBALANCED));
  }

  /**
   * Clique Y has 80 on its live node of 100 and 60 more parked on y2, which is offline inside its
   * window, and clique H needs 180 of 200. The cluster holds 300 and is asked for 320. What this
   * scope must place fits every clique, so nothing is blamed by that measure, and Y is then the
   * only clique that cannot hold all the replicas it owns. Y is carried and H is topped up.
   */
  @Test
  public void testParkedReplicasBlameTheirCliqueWhenNothingElseExplainsTheDeficit()
      throws Exception {
    OptimalAssignment result = createAlgorithm().calculate(parkedDeficit(true));
    Assert.assertEquals(result.getSkippedResources(), set(BROKEN, BROKEN_SINGLE));
    ResourceAssignment healthy = result.getOptimalResourceAssignment().get(HEALTHY);
    // h1 already holds RH_0, so the top up can only land on h2.
    Assert.assertEquals(replicasOn(healthy, 0), set("h1", "h2"));
    Assert.assertEquals(healthy.getMappedPartitions().size(), 9);
    for (Partition partition : healthy.getMappedPartitions()) {
      Assert.assertEquals(healthy.getReplicaMap(partition).keySet(), set("h1", "h2"),
          partition.toString());
    }
  }

  /** The default mode fails both clusters on the cluster wide deficit. */
  @Test
  public void testFlagOffStillThrows() throws Exception {
    assertCapacityDeficit(aboveLiveShare(false));
    assertCapacityDeficit(parkedDeficit(false));
  }

  private void assertCapacityDeficit(ClusterModel model) {
    try {
      createAlgorithm().calculate(model);
      Assert.fail("The default mode must fail on the cluster wide deficit");
    } catch (HelixRebalanceException e) {
      Assert.assertEquals(e.getFailureType(), HelixRebalanceException.Type.FAILED_TO_CALCULATE);
      Assert.assertEquals(e.getFailureCategory(),
          HelixRebalanceException.FailureCategory.CAPACITY_DEFICIT);
    }
  }

  /** RH_0 is topped up to min active on h3, and every RH partition stays on H's live nodes. */
  private static void assertToppedUpWithinH(ResourceAssignment healthy) {
    // h1 already holds RH_0 and h2 is full, so the top up can only land on h3.
    Assert.assertEquals(replicasOn(healthy, 0), set("h1", "h3"));
    Assert.assertEquals(healthy.getMappedPartitions().size(), 5);
    for (Partition partition : healthy.getMappedPartitions()) {
      Set<String> instances = healthy.getReplicaMap(partition).keySet();
      Assert.assertTrue(instances.size() >= 2, partition + " is below min active: " + instances);
      Assert.assertTrue(H_LIVE.containsAll(instances), partition + " left H: " + instances);
    }
  }

  /** RG_0 is topped up to min active beside g1, and every RG partition stays on G's live nodes. */
  private static void assertRebalancedWithinG(ResourceAssignment rebalanced) {
    Set<String> topUp = replicasOn(rebalanced, 0);
    Assert.assertEquals(topUp.size(), 2, "RG_0 is topped up to min active: " + topUp);
    Assert.assertTrue(topUp.contains("g1"), "RG_0 keeps its live replica: " + topUp);
    for (Partition partition : rebalanced.getMappedPartitions()) {
      Assert.assertTrue(G_LIVE.containsAll(rebalanced.getReplicaMap(partition).keySet()),
          partition + " left G");
    }
  }

  /**
   * Cliques H and Y, and clique G: live g1, g2 and g3 of 100 each, g4 offline inside the window.
   * RG has six partitions of two replicas at 10 and min active 2. RG_0 has its second replica on
   * g4, so it gets a top up and nothing of it is parked.
   */
  private ClusterModel aboveLiveShare(boolean enabled) throws Exception {
    DelayedOverwriteCluster cluster = new DelayedOverwriteCluster(createClusterConfig(enabled))
        .liveNode("g1", 100, "G").liveNode("g2", 100, "G").liveNode("g3", 100, "G")
        .resource(taggedResource(REBALANCED, "G", 10), 6, 2, 2)
        .current(REBALANCED, 0, "g1", "g4")
        .current(REBALANCED, 1, "g1", "g2")
        .current(REBALANCED, 2, "g2", "g3")
        .current(REBALANCED, 3, "g3", "g1")
        .current(REBALANCED, 4, "g1", "g2")
        .current(REBALANCED, 5, "g2", "g3");
    return addCliqueY(addCliqueH(cluster)).build();
  }

  /**
   * Clique H, clique Y when withBrokenClique, and clique G: live g1 and g2 of 100 and g3 of
   * g3Capacity, g4 offline inside the window. RG has five partitions of two replicas at rgWeight
   * and min active 2, three replicas on each live node. RG_0 has its second replica on g4, so it
   * gets a top up and nothing of it is parked.
   */
  private ClusterModel aboveLiveShareWithSpare(boolean enabled, boolean withBrokenClique,
      int rgWeight, int g3Capacity) throws Exception {
    DelayedOverwriteCluster cluster = new DelayedOverwriteCluster(createClusterConfig(enabled))
        .liveNode("g1", 100, "G").liveNode("g2", 100, "G").liveNode("g3", g3Capacity, "G")
        .resource(taggedResource(REBALANCED, "G", rgWeight), 5, 2, 2)
        .current(REBALANCED, 0, "g1", "g4")
        .current(REBALANCED, 1, "g1", "g2")
        .current(REBALANCED, 2, "g2", "g3")
        .current(REBALANCED, 3, "g3", "g1")
        .current(REBALANCED, 4, "g2", "g3");
    addCliqueH(cluster);
    return (withBrokenClique ? addCliqueY(cluster) : cluster).build();
  }

  /**
   * Clique H: live h1, h2 and h3 of 100 each, h4 and h5 offline inside the window. RH has five
   * partitions of three replicas at 25 and min active 2, which leaves h1 and h2 full, h3 at 75 and
   * one top up for RH_0. Parked: one replica each of RH_0, RH_1 and RH_2, 75 in all, so what this
   * scope must place for H is exactly its live capacity of 300.
   */
  private DelayedOverwriteCluster addCliqueH(DelayedOverwriteCluster cluster) throws Exception {
    return cluster
        .liveNode("h1", 100, "H").liveNode("h2", 100, "H").liveNode("h3", 100, "H")
        .resource(taggedResource(HEALTHY, "H", 25), 5, 3, 2)
        .current(HEALTHY, 0, "h1", "h4", "h5")
        .current(HEALTHY, 1, "h1", "h2", "h4")
        .current(HEALTHY, 2, "h2", "h3", "h5")
        .current(HEALTHY, 3, "h1", "h2", "h3")
        .current(HEALTHY, 4, "h1", "h2", "h3");
  }

  /**
   * Clique Y: one live node of 100. RY has three partitions of one replica at 100, of which only
   * RY_0 is placed.
   */
  private DelayedOverwriteCluster addCliqueY(DelayedOverwriteCluster cluster) throws Exception {
    return cluster.liveNode("y1", 100, "Y")
        .resource(taggedResource(BROKEN, "Y", 100), 3, 1, 1)
        .current(BROKEN, 0, "y1");
  }

  /**
   * Clique Y: live y1 of 100, y2 offline inside the window. RY has three partitions of two
   * replicas at 20 and min active 1, one replica of each on y1 and on y2, and RY_single has one
   * replica of 20 on y1: 80 placed and 60 parked.
   *
   * Clique H: live h1 and h2 of 100 each, h3 offline inside the window. RH has nine partitions of
   * two replicas at 10 and min active 2. RH_0 has its second replica on h3 and gets a top up, the
   * other eight sit on h1 and h2, so nothing of H is parked.
   */
  private ClusterModel parkedDeficit(boolean enabled) throws Exception {
    ClusterConfig config = createClusterConfig(enabled);
    DelayedOverwriteCluster cluster = new DelayedOverwriteCluster(config)
        .liveNode("y1", 100, "Y")
        .liveNode("h1", 100, "H").liveNode("h2", 100, "H")
        .resource(taggedResource(BROKEN, "Y", 20), 3, 2, 1)
        .resource(taggedResource(BROKEN_SINGLE, "Y", 20), 1, 1, 1)
        .resource(taggedResource(HEALTHY, "H", 10), 9, 2, 2);
    for (int p = 0; p < 3; p++) {
      cluster.current(BROKEN, p, "y1", "y2");
    }
    cluster.current(BROKEN_SINGLE, 0, "y1");
    cluster.current(HEALTHY, 0, "h1", "h3");
    for (int p = 1; p < 9; p++) {
      cluster.current(HEALTHY, p, "h1", "h2");
    }
    return cluster.build();
  }

  private static Set<String> replicasOn(ResourceAssignment assignment, int partition) {
    return assignment.getReplicaMap(
        new Partition(assignment.getResourceName() + "_" + partition)).keySet();
  }

  private static Set<String> set(String... values) {
    return new HashSet<>(Arrays.asList(values));
  }
}
