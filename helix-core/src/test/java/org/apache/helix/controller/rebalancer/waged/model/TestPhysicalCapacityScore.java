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
  // The maximum a fully-weighted soft constraint model can move a score by, so that "dominates"
  // below is a claim about this model rather than a vague one.
  private static final double MAX_COMPETING_INFLUENCE = 13500d;
  private static final double PHYSICAL_CAPACITY_WEIGHT = 100000f;

  private static Map<String, Integer> map(Object... kv) {
    Map<String, Integer> m = new HashMap<>();
    for (int i = 0; i < kv.length; i += 2) {
      m.put((String) kv[i], (Integer) kv[i + 1]);
    }
    return m;
  }

  private AssignableReplica replica(Map<String, Integer> capacity) {
    AssignableReplica r = mock(AssignableReplica.class);
    when(r.getCapacity()).thenReturn(capacity);
    when(r.getResourceName()).thenReturn("Resource1");
    when(r.getPartitionName()).thenReturn("Partition1");
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
}
