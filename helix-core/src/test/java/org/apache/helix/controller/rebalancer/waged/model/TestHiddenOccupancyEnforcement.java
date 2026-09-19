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

import org.apache.helix.controller.dataproviders.ResourceControllerDataProvider;
import org.testng.Assert;
import org.testng.annotations.Test;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * How much room {@link AssignableNode} withholds once hidden occupancy has been recorded on it.
 * The collection side decides what counts as hidden; this decides what that does to eligibility,
 * and the two failure modes here are opposite and both bad: withhold too little and the planner
 * carries on proposing placements the capacity ledger rejects, withhold too much and one stuck
 * replica takes a whole instance out of service.
 * <p>
 * These properties were originally established against the scoring form of this fix. The scoring
 * form is gone, but the properties are about the occupancy arithmetic rather than about scoring,
 * so they carry over unchanged -- including the one that started out wrong, that a replica must
 * not be charged for room it is itself the reason for.
 */
public class TestHiddenOccupancyEnforcement extends AbstractTestClusterModel {
  private static final String RESOURCE = "Resource1";
  private static final String WEDGED_PARTITION = "Partition1";
  private static final String OCCUPANCY_KEY =
      AssignableNode.occupancyKey(RESOURCE, WEDGED_PARTITION);

  private static Map<String, Integer> map(Object... kv) {
    Map<String, Integer> m = new HashMap<>();
    for (int i = 0; i < kv.length; i += 2) {
      m.put((String) kv[i], (Integer) kv[i + 1]);
    }
    return m;
  }

  /** A replica that is itself part of the node's hidden occupancy. */
  private AssignableReplica wedgedReplica(Map<String, Integer> capacity) {
    return replica(capacity, WEDGED_PARTITION);
  }

  /** A replica unrelated to the node's hidden occupancy. */
  private AssignableReplica otherReplica(Map<String, Integer> capacity) {
    return replica(capacity, "PartitionUnderTest");
  }

  private AssignableReplica replica(Map<String, Integer> capacity, String partitionName) {
    AssignableReplica r = mock(AssignableReplica.class);
    when(r.getCapacity()).thenReturn(capacity);
    when(r.getResourceName()).thenReturn(RESOURCE);
    when(r.getPartitionName()).thenReturn(partitionName);
    return r;
  }

  private AssignableNode nodeWithHiddenOccupancy(ResourceControllerDataProvider cache,
      Map<String, Integer> occupancy) {
    AssignableNode n = new AssignableNode(cache.getClusterConfig(),
        cache.getAssignableInstanceConfigMap().get(_testInstanceId), _testInstanceId);
    // Both partitions this fixture asks about are treated as short of replicas, so these tests
    // exercise the enforcement arithmetic rather than the under-replication policy that decides
    // which replicas reach it.
    n.setHiddenOccupancy(new HashMap<>(occupancy), Collections.singleton(OCCUPANCY_KEY),
        new HashSet<>(Arrays.asList(OCCUPANCY_KEY,
            AssignableNode.occupancyKey(RESOURCE, "PartitionUnderTest"))));
    return n;
  }

  /**
   * A node with nothing hidden on it must withhold nothing. Anything else would apply a capacity
   * judgement the hard constraints deliberately did not apply, on every node in the cluster.
   */
  @Test
  public void testNodeWithNoHiddenOccupancyWithholdsNothing() throws IOException {
    ResourceControllerDataProvider cache = setupClusterDataCache();
    AssignableNode clean = new AssignableNode(cache.getClusterConfig(),
        cache.getAssignableInstanceConfigMap().get(_testInstanceId), _testInstanceId);

    Assert.assertEquals(clean.getHiddenOccupancy("item1", otherReplica(map("item1", 5))), 0);
    Assert.assertTrue(clean.getHiddenOccupancy().isEmpty());
  }

  /**
   * A partition already carrying its full complement of replicas is exempt outright: the gate
   * withholds nothing from it, whatever the node is hiding.
   * <p>
   * This is what keeps the gate from generating churn. A healthy replica moves only for the
   * reasons it would have moved anyway, so hidden occupancy shifting as unrelated partitions are
   * replanned cannot drag it around the cluster.
   */
  @Test
  public void testFullyReplicatedPartitionIsExemptFromTheGate() throws IOException {
    ResourceControllerDataProvider cache = setupClusterDataCache();
    AssignableNode node = new AssignableNode(cache.getClusterConfig(),
        cache.getAssignableInstanceConfigMap().get(_testInstanceId), _testInstanceId);
    node.setHiddenOccupancy(map("item1", 5), Collections.singleton(OCCUPANCY_KEY),
        Collections.singleton(OCCUPANCY_KEY));

    AssignableReplica healthy = replica(map("item1", 1), "PartitionUnderTest");
    Assert.assertEquals(node.getHiddenOccupancy("item1", healthy), 0,
        "a partition at full strength is not subject to the gate, so nothing may be withheld "
            + "from it");

    AssignableReplica short_ = replica(map("item1", 1), WEDGED_PARTITION);
    Assert.assertEquals(node.getHiddenOccupancy("item1", short_), 4,
        "a partition that is short of replicas still sees the room the node cannot hand out, "
            + "less its own footprint");
  }

  /**
   * The case that was wrong in the scoring form of this fix and would be just as wrong here.
   * Putting a replica back where it already physically sits needs no new room, so charging it for
   * its own footprint would make the one instance that can hold it for free look like the one
   * instance that cannot -- pushing the planner to move a replica precisely where moving it helps
   * least.
   */
  @Test
  public void testReplicaIsNotChargedForItsOwnFootprint() throws IOException {
    ResourceControllerDataProvider cache = setupClusterDataCache();
    AssignableNode node = nodeWithHiddenOccupancy(cache, map("item1", 5, "item2", 10));

    Assert.assertEquals(node.getHiddenOccupancy("item1", wedgedReplica(map("item1", 5))), 0,
        "the wedged replica's own weight must be excused on the node already holding it");
  }

  /**
   * Only the replica's own share is excused, not the whole of the node's hidden occupancy. A
   * second stuck replica is still room this node cannot hand out.
   */
  @Test
  public void testOnlyTheReplicaOwnShareIsExcused() throws IOException {
    ResourceControllerDataProvider cache = setupClusterDataCache();
    // Two wedged replicas' worth of occupancy, but only one of them is keyed as the candidate.
    AssignableNode node = nodeWithHiddenOccupancy(cache, map("item1", 8));

    Assert.assertEquals(node.getHiddenOccupancy("item1", wedgedReplica(map("item1", 5))), 3,
        "the remainder left by other stuck replicas must still be withheld");
  }

  /** A replica that is not part of the hidden occupancy is charged the whole of it. */
  @Test
  public void testUnrelatedReplicaIsChargedTheFullHiddenOccupancy() throws IOException {
    ResourceControllerDataProvider cache = setupClusterDataCache();
    AssignableNode node = nodeWithHiddenOccupancy(cache, map("item1", 5));

    Assert.assertEquals(node.getHiddenOccupancy("item1", otherReplica(map("item1", 5))), 5);
  }

  /**
   * The excusal must not be able to go past zero into a credit. A negative withholding would hand
   * the node room it does not have, which is the original bug with the sign flipped.
   */
  @Test
  public void testWithheldRoomIsNeverNegative() throws IOException {
    ResourceControllerDataProvider cache = setupClusterDataCache();
    AssignableNode node = nodeWithHiddenOccupancy(cache, map("item1", 2));

    Assert.assertEquals(node.getHiddenOccupancy("item1", wedgedReplica(map("item1", 50))), 0);
  }

  /** A dimension carrying no hidden occupancy withholds nothing on that dimension. */
  @Test
  public void testDimensionWithoutHiddenOccupancyWithholdsNothing() throws IOException {
    ResourceControllerDataProvider cache = setupClusterDataCache();
    AssignableNode node = nodeWithHiddenOccupancy(cache, map("item1", 5));

    Assert.assertEquals(node.getHiddenOccupancy("item2", otherReplica(map("item2", 5))), 0);
    Assert.assertEquals(node.getHiddenOccupancy("unknownKey", otherReplica(map("item1", 5))), 0);
  }

  /**
   * Once a wedged replica is actually assigned here, its weight is charged through
   * {@code _remainingCapacity} like any other placement, so it has to stop being counted as
   * hidden. Leaving it in both would charge the same physical replica twice and make the node look
   * fuller with every round.
   */
  @Test
  public void testAssigningTheWedgedReplicaMovesItOutOfHiddenOccupancy() throws IOException {
    ResourceControllerDataProvider cache = setupClusterDataCache();
    AssignableNode node = nodeWithHiddenOccupancy(cache, map("item1", 3, "item2", 6));
    Map<String, Integer> remainingBefore = new HashMap<>(node.getRemainingCapacity());

    node.assign(realReplica());

    Assert.assertEquals(node.getHiddenOccupancy().getOrDefault("item1", 0), Integer.valueOf(0),
        "an assigned replica is charged through remaining capacity and must leave hidden occupancy");
    Assert.assertEquals(node.getHiddenOccupancy().getOrDefault("item2", 0), Integer.valueOf(0));
    Assert.assertEquals(node.getRemainingCapacity().get("item1"),
        Integer.valueOf(remainingBefore.get("item1") - 3),
        "and must be charged exactly once, through remaining capacity");
  }

  /**
   * Releasing it hands the weight back to hidden occupancy rather than to remaining capacity: the
   * replica is still physically there, it is just no longer part of the plan. Anything else would
   * leak room on every assign/release cycle the algorithm performs.
   */
  @Test
  public void testReleasingTheWedgedReplicaReturnsItToHiddenOccupancy() throws IOException {
    ResourceControllerDataProvider cache = setupClusterDataCache();
    AssignableNode node = nodeWithHiddenOccupancy(cache, map("item1", 3, "item2", 6));
    Map<String, Integer> hiddenBefore = new HashMap<>(node.getHiddenOccupancy());
    Map<String, Integer> remainingBefore = new HashMap<>(node.getRemainingCapacity());

    AssignableReplica replica = realReplica();
    node.assign(replica);
    node.release(replica);

    Assert.assertEquals(node.getHiddenOccupancy(), hiddenBefore,
        "a full assign/release cycle must leave the physical occupancy exactly as it found it");
    Assert.assertEquals(node.getRemainingCapacity(), remainingBefore);
  }

  /**
   * The shared fixture's Resource1/Partition1 replica, which is the one OCCUPANCY_KEY names, so
   * assigning it exercises the hidden-occupancy hand-off rather than an unrelated placement.
   */
  private AssignableReplica realReplica() throws IOException {
    return new AssignableReplica(setupClusterDataCache().getClusterConfig(),
        setupClusterDataCache().getResourceConfig(RESOURCE), WEDGED_PARTITION, "MASTER", 1);
  }
}
