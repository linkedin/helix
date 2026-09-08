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
import java.util.Map;
import java.util.Set;

import org.apache.helix.controller.dataproviders.ResourceControllerDataProvider;
import org.testng.Assert;
import org.testng.annotations.Test;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Scenario coverage for the physical room score, including the scenarios that describe what the
 * constraint deliberately does <i>not</i> do. Fixture capacity is item1=20, item2=40, item3=30.
 */
public class TestPhysicalCapacityScenarios extends AbstractTestClusterModel {
  private static final String OCCUPANCY_KEY = "Resource1|Pwedged";

  private static Map<String, Integer> map(Object... kv) {
    Map<String, Integer> m = new HashMap<>();
    for (int i = 0; i < kv.length; i += 2) {
      m.put((String) kv[i], (Integer) kv[i + 1]);
    }
    return m;
  }

  private AssignableReplica replica(String partition, Map<String, Integer> capacity) {
    AssignableReplica r = mock(AssignableReplica.class);
    when(r.getCapacity()).thenReturn(capacity);
    when(r.getResourceName()).thenReturn("Resource1");
    when(r.getPartitionName()).thenReturn(partition);
    return r;
  }

  private AssignableNode node(ResourceControllerDataProvider cache, Map<String, Integer> occupancy)
      throws IOException {
    return node(cache, occupancy, Collections.singleton(OCCUPANCY_KEY));
  }

  private AssignableNode node(ResourceControllerDataProvider cache, Map<String, Integer> occupancy,
      Set<String> keys) {
    AssignableNode n = new AssignableNode(cache.getClusterConfig(),
        cache.getAssignableInstanceConfigMap().get(_testInstanceId), _testInstanceId);
    if (occupancy != null) {
      n.setUnallocatedOccupancy(occupancy, keys);
    }
    return n;
  }

  /**
   * Shortfall is evaluated per capacity dimension and the worst one decides, so a node short of
   * only one dimension must still be penalised -- but only for replicas that actually need it.
   */
  @Test
  public void testShortOnASingleDimensionStillPenalises() throws IOException {
    ResourceControllerDataProvider cache = setupClusterDataCache();
    AssignableNode n = node(cache, map("item1", 0, "item2", 40));

    Assert.assertTrue(n.getPhysicalRoomScore(replica("Pnew", map("item1", 1, "item2", 1))) <= 0.5d,
        "a replica needing the exhausted dimension must be penalised");
    Assert.assertEquals(n.getPhysicalRoomScore(replica("Pnew", map("item1", 1))), 1d,
        "a replica needing only the roomy dimension must not be penalised");
  }

  /**
   * A routine migration leaves the replica on the source until the drop completes, so the source
   * carries unaccounted occupancy for a while on a perfectly healthy cluster. Given the weight this
   * constraint carries, it must stay silent there unless the source is genuinely out of room --
   * otherwise every ordinary move would repel new placements.
   */
  @Test
  public void testInFlightMigrationDoesNotRepelUntilGenuinelyFull() throws IOException {
    ResourceControllerDataProvider cache = setupClusterDataCache();
    AssignableReplica needsOne = replica("Pnew", map("item1", 1));

    for (int inFlight : new int[] {1, 5, 15, 19}) {
      Assert.assertEquals(node(cache, map("item1", inFlight)).getPhysicalRoomScore(needsOne), 1d,
          "a source holding " + inFlight + "/20 in flight still has room and must not be penalised");
    }
    Assert.assertTrue(node(cache, map("item1", 20)).getPhysicalRoomScore(needsOne) <= 0.5d,
        "a source with no room left must be penalised");
  }

  /**
   * When nothing fits anywhere the constraint cannot reject, so its only remaining job is to rank.
   * The scale must keep discriminating however far past capacity the occupancy goes, otherwise the
   * choice among full instances becomes arbitrary.
   */
  @Test
  public void testRankingSurvivesArbitraryOvercommitment() throws IOException {
    ResourceControllerDataProvider cache = setupClusterDataCache();
    AssignableReplica needsOne = replica("Pnew", map("item1", 1));

    double previous = Double.MAX_VALUE;
    for (int occupancy : new int[] {20, 22, 25, 30, 40, 60, 100, 1000}) {
      double score = node(cache, map("item1", occupancy)).getPhysicalRoomScore(needsOne);
      Assert.assertTrue(score < previous,
          "score must keep decreasing; at occupancy " + occupancy + " it was " + score
              + " against a previous " + previous);
      Assert.assertTrue(score > 0d, "the scale must never bottom out, at occupancy " + occupancy);
      previous = score;
    }
  }

  /**
   * A dimension the node does not declare cannot be reasoned about from the node's own ledger, so
   * the constraint abstains on it rather than guessing -- but that abstention must not mask a real
   * shortfall on a dimension it does know.
   */
  @Test
  public void testUnknownDimensionAbstainsWithoutMaskingKnownShortfall() throws IOException {
    ResourceControllerDataProvider cache = setupClusterDataCache();
    AssignableNode n = node(cache, map("item1", 25));

    Assert.assertEquals(n.getPhysicalRoomScore(replica("Pnew", map("nosuchdim", 5))), 1d,
        "an undeclared dimension must not be invented");
    Assert.assertTrue(
        n.getPhysicalRoomScore(replica("Pnew", map("nosuchdim", 5, "item1", 1))) <= 0.5d,
        "an undeclared dimension must not suppress a known shortfall");
  }

  /**
   * The same replica must be charged exactly once. Once the planner adopts a replica that was
   * previously only physically present, it moves from the unallocated ledger into the node's own
   * capacity, and the score must not change as a result of that bookkeeping.
   */
  @Test
  public void testAdoptingAWedgedReplicaDoesNotDoubleCharge() throws IOException {
    ResourceControllerDataProvider cache = setupClusterDataCache();
    AssignableNode n = node(cache, map("item1", 20));
    AssignableReplica probe = replica("Pnew", map("item1", 1));

    double before = n.getPhysicalRoomScore(probe);
    n.assign(replica("Pwedged", map("item1", 20)));
    double after = n.getPhysicalRoomScore(probe);

    Assert.assertEquals(after, before,
        "adopting the replica already counted as occupancy must not change the score");
    Assert.assertEquals(n.getRemainingCapacity().get("item1").intValue(), 0,
        "the replica must be charged exactly once, against the node's own capacity");
  }

  /**
   * Documents a deliberate limitation rather than a property to rely on. The score is a fit test,
   * not a load signal, so an instance holding wedged replicas is indistinguishable from an empty
   * one for as long as it still has room. The constraint bounds the damage at saturation; it does
   * not stop a sick instance attracting replicas on the way there. Correcting that means feeding
   * the same occupancy into the evenness signals, which changes placement on every cluster and so
   * belongs in its own change.
   */
  @Test
  public void testWedgedInstanceWithRoomIsNotYetPenalised() throws IOException {
    ResourceControllerDataProvider cache = setupClusterDataCache();
    AssignableReplica needsOne = replica("Pnew", map("item1", 1));

    AssignableNode empty = new AssignableNode(cache.getClusterConfig(),
        cache.getAssignableInstanceConfigMap().get(_testInstanceId), _testInstanceId);
    AssignableNode wedgedWithRoom = node(cache, map("item1", 15));

    Assert.assertEquals(wedgedWithRoom.getPhysicalRoomScore(needsOne),
        empty.getPhysicalRoomScore(needsOne),
        "known limitation: a wedged instance with room still scores as though it were empty");
    Assert.assertTrue(node(cache, map("item1", 20)).getPhysicalRoomScore(needsOne) < 1d,
        "the penalty must begin once the room is actually gone");
  }
}
