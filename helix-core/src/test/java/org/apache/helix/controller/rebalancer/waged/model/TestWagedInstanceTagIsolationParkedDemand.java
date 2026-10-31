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
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.apache.helix.model.ClusterConfig;
import org.apache.helix.model.Partition;
import org.apache.helix.model.ResourceAssignment;
import org.apache.helix.model.ResourceConfig;
import org.testng.Assert;
import org.testng.annotations.Test;

/**
 * Capacity deficits in the delayed overwrite scope, whose model leaves out nodes still offline
 * inside their delay window. Replicas parked on those nodes count toward the totals but are not
 * placed here, so each clique is first judged without them. See
 * {@link AbstractTestWagedInstanceTagIsolation} for the shared fixture.
 */
public class TestWagedInstanceTagIsolationParkedDemand
    extends AbstractTestWagedInstanceTagIsolation {
  private static final String CPU = "CPU";
  private static final String HEALTHY = "RH";
  private static final String REBALANCED = "RG";
  private static final String BROKEN = "RY";
  private static final String BROKEN_SINGLE = "RY_single";
  private static final Set<String> H_LIVE = set("h1", "h2", "h3");
  private static final Set<String> G_LIVE = set("g1", "g2", "g3");

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
   * Clique Y has 80 on its live node of 100 and 60 more parked on y2, which is offline inside its
   * window, and clique H needs 180 of 200. The cluster holds 300 and is asked for 320. What this
   * scope must place fits every clique, so nothing is skipped by that measure, and Y is then the
   * only clique that cannot hold all the replicas it owns. Y is carried and H is topped up.
   */
  @Test
  public void testParkedReplicasCountAgainstTheirCliqueWhenNothingElseExplainsTheDeficit()
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

  /**
   * DISK is short and CPU is not. A needs 180 DISK of its 100, B needs 270 CPU of its 200, and 90
   * of each is parked. Counting the parked replicas only on DISK skips A alone, so B gets its top
   * up rather than being skipped for CPU the cluster has room for.
   */
  @Test
  public void testCliqueOverOnlyOnAKeyTheClusterHasRoomForIsNotSkipped() throws Exception {
    OptimalAssignment result = createAlgorithm().calculate(twoKeys(true));
    Assert.assertEquals(result.getSkippedResources(), set("RA"));
    Assert.assertEquals(replicasOn(result.getOptimalResourceAssignment().get("RB"), 0),
        set("b1", "b2"));
  }

  /**
   * Clique Y needs 240 of its 100 and clique H 210 of its 200, both only once their parked
   * replicas count. Skipping Y alone leaves 310 on 400, so only Y is carried and H still gets its
   * top up.
   */
  @Test
  public void testOnlyTheCliquesTheDeficitNeedsAreSkipped() throws Exception {
    OptimalAssignment result = createAlgorithm().calculate(twoParkedOverflows(true));
    Assert.assertEquals(result.getSkippedResources(), set(BROKEN));
    Assert.assertEquals(replicasOn(result.getOptimalResourceAssignment().get("RH2"), 0),
        set("h1", "h2"));
  }

  /** The default mode fails every cluster here on the cluster wide deficit. */
  @Test
  public void testFlagOffStillThrows() throws Exception {
    assertCapacityDeficit(aboveLiveShare(false));
    assertCapacityDeficit(parkedDeficit(false));
    assertCapacityDeficit(twoKeys(false));
    assertCapacityDeficit(twoParkedOverflows(false));
  }

  /**
   * DISK is 10 short and CPU is not. A is over by 20 DISK and 100 CPU with its parked replica, B
   * by 40 DISK. Counting parked only on DISK skips B, which leaves room on both keys. Counting it
   * on CPU too would skip A first, for CPU the cluster has room for.
   */
  @Test
  public void testParkedReplicasCountOnlyOnTheShortKeyFirst() throws Exception {
    Assert.assertEquals(createAlgorithm().calculate(
            parkedCliques(true, new int[] {100, 200, 60, 150}, new int[] {100, 100, 70, 0},
                new int[] {100, 250, 25, 50})).getSkippedResources(), set("RB"));
  }

  /**
   * DISK is 10 short and CPU is not. A is over by 30 DISK and B by 20 with their parked replicas.
   * Skipping A, the larger, leaves CPU short, so B is skipped too. The rest then fits without A,
   * so only B is skipped.
   */
  @Test
  public void testCliqueTheRestFitsWithoutIsNotSkipped() throws Exception {
    Assert.assertEquals(createAlgorithm().calculate(
            parkedCliques(true, new int[] {100, 200, 65, 50}, new int[] {100, 100, 60, 80},
                new int[] {100, 100, 30, 45})).getSkippedResources(), set("RB"));
  }

  /**
   * As above, but C is 40 CPU over with its parked replica, so skipping A, then A and B, never
   * fits, and counting parked on every key leaves nothing to rebalance. Skipping B alone fits.
   */
  @Test
  public void testSmallerOverflowIsSkippedWhenTheLargestNeverFits() throws Exception {
    Assert.assertEquals(createAlgorithm().calculate(
            parkedCliques(true, new int[] {100, 200, 65, 50}, new int[] {100, 100, 60, 45},
                new int[] {100, 100, 30, 70})).getSkippedResources(), set("RB"));
  }

  /**
   * DISK is 10 short and CPU has 10 spare. A, B and C are over by 10, 5 and 5 DISK with their
   * parked replicas. Skipping A frees DISK but leaves CPU 30 short, and no set with A fits. B and
   * C each free 5 DISK and 5 CPU, so they are skipped instead.
   */
  @Test
  public void testCliquesThatCostNoRoomAreSkippedFirst() throws Exception {
    Assert.assertEquals(createAlgorithm().calculate(
            parkedCliques(true, new int[] {100, 2000, 55, 980}, new int[] {95, 1001, 50, 503},
                new int[] {95, 1001, 50, 503}, new int[] {100, 100, 45, 60}))
        .getSkippedResources(), set("RB", "RC"));
  }

  /**
   * DISK is 15 short and CPU 20. C is over on CPU with its live replica alone, and skipping it
   * leaves 20 CPU spare. A, first by its 50 CPU overflow, frees 5 DISK and 50 CPU at no cost, and
   * B frees 20 DISK for 10 CPU. Skipping A leaves DISK 10 short with only B left to skip, which
   * would leave nothing to rebalance. Skipping B alone fits, with z1's room.
   */
  @Test
  public void testCliqueTheRestFitsWithIsSkippedBeforeOneThatCostsNoRoom() throws Exception {
    Assert.assertEquals(createAlgorithm().calculate(
            parkedCliqueCluster(true, new int[] {95, 50, 50, 50}, new int[] {80, 110, 50, 50},
                new int[] {20, 20, 10, 30}).liveNode("z1", diskAndCpu(10, 60), "Z").build())
        .getSkippedResources(), set("RB", "RC"));
  }

  /**
   * DISK and CPU are both 10 short. A frees 60 DISK for 60 CPU, B the reverse, and C and D free 5
   * of each. Skipping all four leaves room but nothing to rebalance, and the rest is short without
   * any one of them. Skipping C and D fits.
   */
  @Test
  public void testSkippingEveryCliqueIsNotAFit() throws Exception {
    Assert.assertEquals(createAlgorithm().calculate(
            parkedCliques(true, new int[] {100, 100, 80, 20}, new int[] {100, 100, 20, 80},
                new int[] {95, 95, 50, 50}, new int[] {95, 95, 50, 50})).getSkippedResources(),
        set("RC", "RD"));
  }

  /**
   * DISK is 10 short and CPU is not. Only A is over on DISK, and skipping it leaves CPU short, so
   * the parked replicas count on every key: B is over by 40 CPU and both are skipped.
   */
  @Test
  public void testParkedReplicasCountOnEveryKeyLast() throws Exception {
    Assert.assertEquals(createAlgorithm().calculate(
            parkedCliques(true, new int[] {100, 200, 65, 75}, new int[] {100, 100, 45, 70},
                new int[] {100, 100, 45, 45})).getSkippedResources(), set("RA", "RB"));
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

  /**
   * Clique Y: live y1 of 100, y2 and y3 offline inside the window. RY has four partitions of three
   * replicas at 20, one of each on y1: 80 placed and 160 parked.
   *
   * Clique H: live h1 and h2 of 100, h3 offline inside the window. RH has three partitions of three
   * replicas at 10 on h1, h2 and h3. RH2 has six partitions of two replicas at 10 and min active 2:
   * RH2_0 on h1 and h3 gets a top up, the rest sit on h1 and h2. H owns 210, 30 of it parked.
   *
   * Clique G: g1 and g2 of 100 hold RG, ten single replicas at 10. The cluster holds 500 and is
   * asked for 550.
   */
  private ClusterModel twoParkedOverflows(boolean enabled) throws Exception {
    DelayedOverwriteCluster cluster = new DelayedOverwriteCluster(createClusterConfig(enabled))
        .liveNode("y1", 100, "Y")
        .liveNode("h1", 100, "H").liveNode("h2", 100, "H")
        .liveNode("g1", 100, "G").liveNode("g2", 100, "G")
        .resource(taggedResource(BROKEN, "Y", 20), 4, 3, 1)
        .resource(taggedResource(HEALTHY, "H", 10), 3, 3, 2)
        .resource(taggedResource("RH2", "H", 10), 6, 2, 2)
        .resource(taggedResource(REBALANCED, "G", 10), 10, 1, 1);
    for (int p = 0; p < 4; p++) {
      cluster.current(BROKEN, p, "y1", "y2", "y3");
    }
    for (int p = 0; p < 3; p++) {
      cluster.current(HEALTHY, p, "h1", "h2", "h3");
    }
    cluster.current("RH2", 0, "h1", "h3");
    for (int p = 1; p < 6; p++) {
      cluster.current("RH2", p, "h1", "h2");
    }
    for (int p = 0; p < 10; p++) {
      cluster.current(REBALANCED, p, p % 2 == 0 ? "g1" : "g2");
    }
    return cluster.build();
  }

  /**
   * DISK holds 300 and is asked for 370, CPU holds 400 and is asked for 280. Clique A: live a1, a2
   * offline. RA has three partitions of two replicas at DISK 30, one on each node. Clique B: live
   * b1 and b2 with DISK 50, b3 and b4 offline. RB has three partitions of three replicas at DISK 10
   * and CPU 30, min active 2. RB_0 on b1, b3 and b4 gets a top up, RB_1 and RB_2 sit on b1, b2
   * and b3. Clique D: d1 holds four partitions of RD at DISK 25.
   */
  private ClusterModel twoKeys(boolean enabled) throws Exception {
    DelayedOverwriteCluster cluster = new DelayedOverwriteCluster(twoKeyConfig(enabled))
        .liveNode("a1", diskAndCpu(100, 100), "A")
        .liveNode("b1", diskAndCpu(50, 100), "B").liveNode("b2", diskAndCpu(50, 100), "B")
        .liveNode("d1", diskAndCpu(100, 100), "D")
        .resource(twoKeyResource("RA", "A", 30, 1), 3, 2, 1)
        .resource(twoKeyResource("RB", "B", 10, 30), 3, 3, 2)
        .resource(twoKeyResource("RD", "D", 25, 1), 4, 1, 1)
        .current("RB", 0, "b1", "b3", "b4")
        .current("RB", 1, "b1", "b2", "b3")
        .current("RB", 2, "b1", "b2", "b3");
    for (int p = 0; p < 4; p++) {
      cluster.current("RD", p, "d1");
      if (p < 3) {
        cluster.current("RA", p, "a1", "a2");
      }
    }
    return cluster.build();
  }

  /**
   * One clique per row of {node DISK, node CPU, replica DISK, replica CPU}, tagged A, B, C and on
   * in order. Each has one live node and one partition of two replicas, min active 1, with the
   * second replica parked on a node offline inside the window.
   */
  private ClusterModel parkedCliques(boolean enabled, int[]... rows) throws Exception {
    return parkedCliqueCluster(enabled, rows).build();
  }

  private DelayedOverwriteCluster parkedCliqueCluster(boolean enabled, int[]... rows)
      throws Exception {
    DelayedOverwriteCluster cluster = new DelayedOverwriteCluster(twoKeyConfig(enabled));
    for (int i = 0; i < rows.length; i++) {
      String tag = String.valueOf((char) ('A' + i));
      String node = tag.toLowerCase();
      cluster.liveNode(node + "1", diskAndCpu(rows[i][0], rows[i][1]), tag)
          .resource(twoKeyResource("R" + tag, tag, rows[i][2], rows[i][3]), 1, 2, 1)
          .current("R" + tag, 0, node + "1", node + "2");
    }
    return cluster;
  }

  private ClusterConfig twoKeyConfig(boolean enabled) {
    ClusterConfig config = createClusterConfig(enabled);
    config.setInstanceCapacityKeys(Arrays.asList(CAPACITY_KEY, CPU));
    config.setDefaultPartitionWeightMap(diskAndCpu(0, 0));
    return config;
  }

  private ResourceConfig twoKeyResource(String name, String tag, int disk, int cpu)
      throws Exception {
    ResourceConfig resource = taggedResource(name, tag, disk);
    resource.setPartitionCapacityMap(
        Collections.singletonMap(ResourceConfig.DEFAULT_PARTITION_KEY, diskAndCpu(disk, cpu)));
    return resource;
  }

  private static Map<String, Integer> diskAndCpu(int disk, int cpu) {
    Map<String, Integer> capacity = new HashMap<>();
    capacity.put(CAPACITY_KEY, disk);
    capacity.put(CPU, cpu);
    return capacity;
  }

  private static Set<String> replicasOn(ResourceAssignment assignment, int partition) {
    return assignment.getReplicaMap(
        new Partition(assignment.getResourceName() + "_" + partition)).keySet();
  }

  private static Set<String> set(String... values) {
    return new HashSet<>(Arrays.asList(values));
  }
}
