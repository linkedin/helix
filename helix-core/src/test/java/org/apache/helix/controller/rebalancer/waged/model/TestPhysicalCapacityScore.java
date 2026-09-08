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
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.apache.helix.constants.InstanceConstants;
import org.apache.helix.controller.dataproviders.ResourceControllerDataProvider;
import org.apache.helix.model.InstanceConfig;
import org.testng.Assert;
import org.testng.annotations.Test;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Properties the physical-room score has to hold for the placement fix to work. Each of these was
 * found by probing the scale at its edges rather than by reasoning about it, and two of them
 * started out false.
 */
public class TestPhysicalCapacityScore extends AbstractTestClusterModel {
  private static final String OCCUPANCY_KEY = "Resource1|Partition1";
  // The most the rest of the model can move a score by, so that "dominates" below is a claim about
  // this model rather than a vague one. The six preference-scaled constraints contribute at most
  // 13500, and FORCE_BASELINE_CONVERGE can raise BaselineInfluenceConstraint by a further 100000.
  private static final double MAX_COMPETING_INFLUENCE = 113500d;
  private static final double PHYSICAL_CAPACITY_WEIGHT = 1000000f;

  private static Map<String, Integer> map(Object... kv) {
    Map<String, Integer> m = new HashMap<>();
    for (int i = 0; i < kv.length; i += 2) {
      m.put((String) kv[i], (Integer) kv[i + 1]);
    }
    return m;
  }

  /**
   * A replica that is NOT itself part of the node's unaccounted occupancy, which is what every
   * penalty test here means by "the node is occupied". Deliberately a different partition from
   * OCCUPANCY_KEY: a replica already sitting on the node costs nothing more to place there, so its
   * own footprint is excluded from the shortfall and it would never be penalised.
   */
  private AssignableReplica replica(Map<String, Integer> capacity) {
    return replica(capacity, "PartitionUnderTest");
  }

  /** A replica that IS part of the node's unaccounted occupancy, keyed to match OCCUPANCY_KEY. */
  private AssignableReplica replicaAlreadyOnNode(Map<String, Integer> capacity) {
    return replica(capacity, "Partition1");
  }

  private AssignableReplica replica(Map<String, Integer> capacity, String partitionName) {
    AssignableReplica r = mock(AssignableReplica.class);
    when(r.getCapacity()).thenReturn(capacity);
    when(r.getResourceName()).thenReturn("Resource1");
    when(r.getPartitionName()).thenReturn(partitionName);
    return r;
  }

  private AssignableNode nodeWithOccupancy(ResourceControllerDataProvider cache,
      Map<String, Integer> occupancy) {
    AssignableNode n = new AssignableNode(cache.getClusterConfig(),
        cache.getAssignableInstanceConfigMap().get(_testInstanceId), _testInstanceId);
    n.setUnallocatedOccupancy(new HashMap<>(occupancy), Collections.singleton(OCCUPANCY_KEY));
    return n;
  }

  /**
   * With no unaccounted occupancy the score has to be inert. Anything else would apply a capacity
   * judgement the hard constraints may deliberately not have applied.
   */
  @Test
  public void testNoOccupancyScoresMaxAndIsInert() throws IOException {
    ResourceControllerDataProvider cache = setupClusterDataCache();
    AssignableNode clean = new AssignableNode(cache.getClusterConfig(),
        cache.getAssignableInstanceConfigMap().get(_testInstanceId), _testInstanceId);
    Assert.assertEquals(clean.getPhysicalRoomScore(replica(map("item1", 5))), 1d);
    // Even against a requirement the node could not possibly satisfy.
    Assert.assertEquals(clean.getPhysicalRoomScore(replica(map("item1", Integer.MAX_VALUE))), 1d);
  }

  /**
   * A replica needing none of a capacity type cannot be short of it. Scoring that dimension anyway
   * penalises an instance for a constraint that does not apply to the replica being placed, and
   * because this constraint is weighted to dominate, such a false penalty would decide placement
   * outright. Zero weights are legal, so this is reachable in production.
   */
  @Test
  public void testZeroWeightDimensionIsNeverPenalised() throws IOException {
    ResourceControllerDataProvider cache = setupClusterDataCache();
    // item3 is exhausted, but the replica needs none of it.
    AssignableNode n = nodeWithOccupancy(cache, map("item3", 30));
    Assert.assertEquals(n.getPhysicalRoomScore(replica(map("item1", 5, "item3", 0))), 1d,
        "A dimension the replica does not consume must not lower its score");

    AssignableNode allZero = nodeWithOccupancy(cache, map("item1", 20));
    Assert.assertEquals(allZero.getPhysicalRoomScore(replica(map("item1", 0))), 1d);
  }

  /**
   * The property that makes this usable as a preference rather than a filter: fitting always beats
   * not fitting by more than the rest of the model can make up, at any capacity magnitude. Sizing
   * the weight against a gap that shrinks with capacity would silently stop working on clusters
   * with large capacity numbers.
   */
  @Test
  public void testFitAlwaysDominatesNonFitAtAnyCapacityScale() throws IOException {
    ResourceControllerDataProvider cache = setupClusterDataCache();
    for (int capacity : Arrays.asList(1, 2, 20, 1000, 100000, Integer.MAX_VALUE / 2)) {
      InstanceConfig cfg = new InstanceConfig(_testInstanceId);
      cfg.setInstanceCapacityMap(map("item1", capacity, "item2", capacity, "item3", capacity));
      cfg.setInstanceOperation(InstanceConstants.InstanceOperation.ENABLE);
      cfg.setZoneId(_testFaultZoneId);
      AssignableNode full = new AssignableNode(cache.getClusterConfig(), cfg, _testInstanceId);
      full.setUnallocatedOccupancy(map("item1", capacity), Collections.singleton(OCCUPANCY_KEY));

      double score = full.getPhysicalRoomScore(replica(map("item1", 1)));
      double gap = 1d - score;
      Assert.assertTrue(gap * PHYSICAL_CAPACITY_WEIGHT > MAX_COMPETING_INFLUENCE,
          "At capacity " + capacity + " a physically full instance scored " + score
              + ", leaving only " + gap * PHYSICAL_CAPACITY_WEIGHT
              + " weighted points of separation, which the rest of the model can override");
    }
  }

  /**
   * The failure mode this whole fix exists to avoid, in miniature: a scale that bottoms out stops
   * ranking once every candidate is past the bottom. When no instance has room the rebalancer
   * still has to place the replica somewhere, and the least overcommitted instance is the only
   * defensible choice.
   */
  @Test
  public void testOrderingSurvivesArbitraryOvercommitment() throws IOException {
    ResourceControllerDataProvider cache = setupClusterDataCache();
    double previous = Double.MAX_VALUE;
    for (int occupancy : Arrays.asList(20, 25, 30, 60, 500, 100000)) {
      double score =
          nodeWithOccupancy(cache, map("item1", occupancy)).getPhysicalRoomScore(
              replica(map("item1", 5)));
      Assert.assertTrue(score < previous,
          "Score must keep decreasing as overcommitment grows; at occupancy " + occupancy
              + " it stopped at " + score);
      Assert.assertTrue(score > 0d, "Score must not bottom out and lose ordering");
      previous = score;
    }
  }

  /**
   * The worst dimension has to govern, otherwise an instance short on one capacity type could be
   * chosen on the strength of another.
   */
  @Test
  public void testWorstDimensionGoverns() throws IOException {
    ResourceControllerDataProvider cache = setupClusterDataCache();
    AssignableNode n = nodeWithOccupancy(cache, map("item2", 40));
    double constrained = n.getPhysicalRoomScore(replica(map("item1", 5, "item2", 5)));
    Assert.assertTrue(constrained < 0.5d,
        "An instance out of item2 must not score as fitting because item1 has room");
  }

  /**
   * The previous test exhausts a single dimension, so Math.max never had to choose between two
   * competing shortfalls -- returning the first, the last, or the smaller would all have passed it.
   * With both dimensions short by different relative amounts the choice becomes observable, and the
   * larger relative shortfall has to win. Taking the smaller would let an instance that is badly
   * short of one capacity type hide behind being only slightly short of another.
   */
  @Test
  public void testLargerOfTwoCompetingShortfallsGoverns() throws IOException {
    ResourceControllerDataProvider cache = setupClusterDataCache();
    // Both dimensions fully occupied: item1 capacity 20, item2 capacity 40.
    AssignableNode n = nodeWithOccupancy(cache, map("item1", 20, "item2", 40));

    // item1 short by 2 of 20 = 0.10 relative; item2 short by 8 of 40 = 0.20 relative.
    double actual = n.getPhysicalRoomScore(replica(map("item1", 2, "item2", 8)));

    double expectedFromWorse = 0.5d / (1d + 0.20d);
    double ifItTookTheSmaller = 0.5d / (1d + 0.10d);

    Assert.assertEquals(actual, expectedFromWorse, 1e-9,
        "the larger relative shortfall (item2, 0.20) must govern the score");
    Assert.assertTrue(Math.abs(actual - ifItTookTheSmaller) > 1e-6,
        "the two candidate outcomes must be distinguishable, otherwise this test cannot tell which "
            + "shortfall was used; actual=" + actual + " smaller-would-give=" + ifItTookTheSmaller);
  }

  /**
   * The relative scaling means a shortfall must be judged against the dimension's own capacity, not
   * in absolute units. A larger absolute shortfall on a roomy dimension is less serious than a
   * smaller one on a tight dimension, and scoring the absolute value would invert that.
   */
  @Test
  public void testShortfallIsRelativeToEachDimensionNotAbsolute() throws IOException {
    ResourceControllerDataProvider cache = setupClusterDataCache();
    AssignableNode n = nodeWithOccupancy(cache, map("item1", 20, "item2", 40));

    // item1: 4 of 20 = 0.20 relative. item2: 6 of 40 = 0.15 relative.
    // item2 is the larger absolute shortfall, item1 the larger relative one.
    double actual = n.getPhysicalRoomScore(replica(map("item1", 4, "item2", 6)));

    Assert.assertEquals(actual, 0.5d / (1d + 0.20d), 1e-9,
        "item1 must govern on relative shortfall (0.20) even though item2 is short by more in "
            + "absolute units (6 > 4)");
  }

  /**
   * Occupancy is a running total that assign and release both adjust, so a replica moving into and
   * back out of an assignment has to leave it exactly where it started. Drift here would corrupt
   * every later score on the node.
   */
  @Test
  public void testAssignReleaseLeavesOccupancyUnchanged() throws IOException {
    ResourceControllerDataProvider cache = setupClusterDataCache();
    AssignableReplica victim = generateReplicas(cache).iterator().next();
    String key =
        AssignableNode.occupancyKey(victim.getResourceName(), victim.getPartitionName());
    Map<String, Integer> occupancy = new HashMap<>(victim.getCapacity());
    Map<String, Integer> before = new HashMap<>(occupancy);

    AssignableNode n = new AssignableNode(cache.getClusterConfig(),
        cache.getAssignableInstanceConfigMap().get(_testInstanceId), _testInstanceId);
    n.setUnallocatedOccupancy(occupancy, new HashSet<>(Collections.singletonList(key)));

    n.assign(victim);
    Assert.assertTrue(!occupancy.equals(before),
        "assign must move the replica out of occupancy");
    n.release(victim);
    Assert.assertEquals(occupancy, before, "release must restore the occupancy exactly");
  }

  /**
   * A replica cannot be charged twice against one instance. AssignableNode.assign already rejects
   * a duplicate outright, which is what keeps the running total from drifting negative.
   */
  @Test(expectedExceptions = org.apache.helix.HelixException.class)
  public void testDuplicateAssignIsRejected() throws IOException {
    ResourceControllerDataProvider cache = setupClusterDataCache();
    AssignableReplica victim = generateReplicas(cache).iterator().next();
    String key =
        AssignableNode.occupancyKey(victim.getResourceName(), victim.getPartitionName());
    AssignableNode n = new AssignableNode(cache.getClusterConfig(),
        cache.getAssignableInstanceConfigMap().get(_testInstanceId), _testInstanceId);
    n.setUnallocatedOccupancy(new HashMap<>(victim.getCapacity()),
        new HashSet<>(Collections.singletonList(key)));
    n.assign(victim);
    n.assign(victim);
  }

  /**
   * Capacity types the node does not declare, and occupancy in types the replica does not use, both
   * have to be ignored rather than guessed at.
   */
  @Test
  public void testUnknownCapacityKeysAreIgnored() throws IOException {
    ResourceControllerDataProvider cache = setupClusterDataCache();
    Assert.assertEquals(
        nodeWithOccupancy(cache, map("unknown", 999)).getPhysicalRoomScore(replica(map("item1", 5))),
        1d, "Occupancy in a capacity type the node does not declare must not count");
    Assert.assertEquals(
        nodeWithOccupancy(cache, map("item1", 20)).getPhysicalRoomScore(replica(map("itemX", 5))),
        1d, "A requirement the node has no capacity type for is the hard constraints' business");
  }

  /**
   * A replica already physically on the node costs nothing more to place there, so it must not be
   * charged for its own footprint. Without this the node a replica currently sits on is penalised
   * by that replica's own weight every time the ideal assignment wants to move it -- the ordinary
   * case for partial rebalance -- and the constraint then argues for the physically worse option,
   * since the replica goes on occupying this node until its drop completes.
   */
  @Test
  public void testAReplicaIsNotPenalisedByItsOwnFootprint() throws IOException {
    ResourceControllerDataProvider cache = setupClusterDataCache();
    Map<String, Integer> need = map("item1", 18);

    double alreadyHere =
        nodeWithOccupancy(cache, need).getPhysicalRoomScore(replicaAlreadyOnNode(need));
    double someoneElse = nodeWithOccupancy(cache, need).getPhysicalRoomScore(replica(need));

    Assert.assertEquals(alreadyHere, 1d,
        "placing a replica where it already sits consumes no new space, so the node has room");
    Assert.assertTrue(someoneElse < 1d,
        "identical occupancy belonging to a different partition must still be penalised, "
            + "otherwise this test would pass for the wrong reason. scored " + someoneElse);
  }

  /**
   * Only the replica's own share is excused. Occupancy from anything else still counts, so a node
   * that is genuinely overloaded does not get a free pass merely because the replica under
   * consideration happens to be one of the things sitting on it.
   */
  @Test
  public void testOnlyTheReplicaOwnShareIsExcused() throws IOException {
    ResourceControllerDataProvider cache = setupClusterDataCache();
    Map<String, Integer> need = map("item1", 18);
    // 18 belonging to the replica under test, plus 10 belonging to something else.
    Map<String, Integer> occupancy = map("item1", 28);
    Set<String> keys = new HashSet<>(java.util.Arrays.asList(OCCUPANCY_KEY, "Resource1|Another"));

    AssignableNode mixed = new AssignableNode(cache.getClusterConfig(),
        cache.getAssignableInstanceConfigMap().get(_testInstanceId), _testInstanceId);
    mixed.setUnallocatedOccupancy(new HashMap<>(occupancy), keys);
    double excused = mixed.getPhysicalRoomScore(replicaAlreadyOnNode(need));

    AssignableNode same = new AssignableNode(cache.getClusterConfig(),
        cache.getAssignableInstanceConfigMap().get(_testInstanceId), _testInstanceId);
    same.setUnallocatedOccupancy(new HashMap<>(occupancy), keys);
    double notExcused = same.getPhysicalRoomScore(replica(need));

    Assert.assertTrue(excused < 1d,
        "the 10 units belonging to something else must still be held against the node, scored "
            + excused);
    Assert.assertTrue(excused > notExcused,
        "excusing the replica's own 18 units must leave it better off than a replica that has to "
            + "account for all 28. excused=" + excused + " notExcused=" + notExcused);
  }
}
