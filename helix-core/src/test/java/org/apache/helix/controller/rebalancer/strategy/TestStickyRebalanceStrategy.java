package org.apache.helix.controller.rebalancer.strategy;

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

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.helix.TestHelper;
import org.apache.helix.controller.dataproviders.ResourceControllerDataProvider;
import org.apache.helix.model.LeaderStandbySMD;
import org.apache.helix.model.StateModelDefinition;
import org.apache.helix.zookeeper.datamodel.ZNRecord;
import org.testng.Assert;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;


/**
 * Tests for {@link StickyRebalanceStrategy}.
 *
 * <p>The strategy's core contract:
 * <ol>
 *   <li>Existing assignments on live nodes are <b>never</b> moved.</li>
 *   <li>Only unassigned ("orphaned") replicas are placed via round-robin.</li>
 *   <li>The {@code maximumPerNode} constraint is respected.</li>
 * </ol>
 */
public class TestStickyRebalanceStrategy {

  private static final String RESOURCE_NAME = "testResource";
  private static final String STATE_MODEL_DEF = "LeaderStandby";
  private static final StateModelDefinition STATE_MODEL = LeaderStandbySMD.build();

  private ResourceControllerDataProvider _dataCache;

  @BeforeMethod
  public void setUp() {
    _dataCache = TestHelper.buildMockDataCache(
        RESOURCE_NAME, "2", STATE_MODEL_DEF, STATE_MODEL, Collections.emptySet());
  }

  // ─── Helper methods ──────────────────────────────────────────────────────────

  private List<String> makePartitions(int count) {
    List<String> partitions = new ArrayList<>();
    for (int i = 0; i < count; i++) {
      partitions.add(RESOURCE_NAME + "_" + i);
    }
    return partitions;
  }

  private List<String> makeNodes(String prefix, int count) {
    List<String> nodes = new ArrayList<>();
    for (int i = 0; i < count; i++) {
      nodes.add(prefix + i);
    }
    return nodes;
  }

  private LinkedHashMap<String, Integer> leaderStandbyStates() {
    LinkedHashMap<String, Integer> states = new LinkedHashMap<>();
    states.put("LEADER", 1);
    states.put("STANDBY", 1);
    return states;
  }

  private ZNRecord runStrategy(List<String> partitions, List<String> allNodes,
      List<String> liveNodes, Map<String, Map<String, String>> currentMapping,
      LinkedHashMap<String, Integer> states, int maxPerNode) {
    StickyRebalanceStrategy strategy = new StickyRebalanceStrategy();
    strategy.init(RESOURCE_NAME, partitions, states, maxPerNode);
    return strategy.computePartitionAssignment(allNodes, liveNodes, currentMapping, _dataCache);
  }

  // ─── Tests ───────────────────────────────────────────────────────────────────

  /**
   * Fresh cluster with no existing assignments — partitions should be distributed
   * evenly across all live nodes via round-robin.
   */
  @Test
  public void testFreshAssignment() {
    List<String> partitions = makePartitions(6);
    List<String> nodes = makeNodes("n", 3);
    Map<String, Map<String, String>> currentMapping = new HashMap<>();

    ZNRecord result = runStrategy(partitions, nodes, nodes, currentMapping,
        leaderStandbyStates(), Integer.MAX_VALUE);

    // Every partition should have exactly 2 replicas (LEADER + STANDBY)
    for (String partition : partitions) {
      Map<String, String> assignment = result.getMapField(partition);
      Assert.assertNotNull(assignment, "Missing assignment for " + partition);
      Assert.assertEquals(assignment.size(), 2,
          "Expected 2 replicas for " + partition + " but got " + assignment);

      // Verify states
      Assert.assertTrue(assignment.containsValue("LEADER"),
          "No LEADER for " + partition);
      Assert.assertTrue(assignment.containsValue("STANDBY"),
          "No STANDBY for " + partition);

      // Verify preference list matches
      List<String> prefList = result.getListField(partition);
      Assert.assertNotNull(prefList);
      Assert.assertEquals(prefList.size(), 2);
    }

    // No node should have more than ceil(12 / 3) = 4 replicas (6 partitions × 2 replicas / 3 nodes)
    Map<String, Integer> counts = countReplicasPerNode(result, partitions);
    for (Map.Entry<String, Integer> entry : counts.entrySet()) {
      Assert.assertTrue(entry.getValue() <= 4,
          "Node " + entry.getKey() + " has " + entry.getValue() + " replicas, expected <= 4");
    }
  }

  /**
   * Existing assignments on live nodes must NOT be moved when a new node joins.
   * Only orphaned partitions (if any) go to the new node.
   */
  @Test
  public void testPreserveExistingOnNodeAdd() {
    List<String> partitions = makePartitions(4);

    // Build an initial assignment: all partitions on n0 and n1
    Map<String, Map<String, String>> currentMapping = new HashMap<>();
    currentMapping.put(partitions.get(0), mapOf("n0", "LEADER", "n1", "STANDBY"));
    currentMapping.put(partitions.get(1), mapOf("n1", "LEADER", "n0", "STANDBY"));
    currentMapping.put(partitions.get(2), mapOf("n0", "LEADER", "n1", "STANDBY"));
    currentMapping.put(partitions.get(3), mapOf("n1", "LEADER", "n0", "STANDBY"));

    // Add a third node
    List<String> allNodes = makeNodes("n", 3);
    List<String> liveNodes = makeNodes("n", 3);

    ZNRecord result = runStrategy(partitions, allNodes, liveNodes, currentMapping,
        leaderStandbyStates(), Integer.MAX_VALUE);

    // ALL original assignments must be preserved exactly
    for (String partition : partitions) {
      Map<String, String> original = currentMapping.get(partition);
      Map<String, String> newAssignment = result.getMapField(partition);
      for (Map.Entry<String, String> entry : original.entrySet()) {
        Assert.assertEquals(newAssignment.get(entry.getKey()), entry.getValue(),
            "Assignment moved for " + partition + " node " + entry.getKey());
      }
    }

    // The new node (n2) should have 0 replicas since nothing is orphaned
    Map<String, Integer> counts = countReplicasPerNode(result, partitions);
    Assert.assertEquals(counts.getOrDefault("n2", 0).intValue(), 0,
        "New node should have no replicas when all partitions are already assigned");
  }

  /**
   * When a node goes down, its partitions become orphaned and are redistributed
   * to remaining live nodes. All other assignments stay unchanged.
   */
  @Test
  public void testPreserveExistingOnNodeRemove() {
    List<String> partitions = makePartitions(4);

    // 3-node cluster, remove n2
    Map<String, Map<String, String>> currentMapping = new HashMap<>();
    currentMapping.put(partitions.get(0), mapOf("n0", "LEADER", "n2", "STANDBY"));
    currentMapping.put(partitions.get(1), mapOf("n1", "LEADER", "n2", "STANDBY"));
    currentMapping.put(partitions.get(2), mapOf("n2", "LEADER", "n0", "STANDBY"));
    currentMapping.put(partitions.get(3), mapOf("n2", "LEADER", "n1", "STANDBY"));

    List<String> allNodes = makeNodes("n", 3);
    List<String> liveNodes = new ArrayList<>();
    liveNodes.add("n0");
    liveNodes.add("n1");
    // n2 is dead

    ZNRecord result = runStrategy(partitions, allNodes, liveNodes, currentMapping,
        leaderStandbyStates(), Integer.MAX_VALUE);

    // Surviving assignments (n0 and n1 entries) must be preserved
    Assert.assertEquals(result.getMapField(partitions.get(0)).get("n0"), "LEADER");
    Assert.assertEquals(result.getMapField(partitions.get(1)).get("n1"), "LEADER");
    Assert.assertEquals(result.getMapField(partitions.get(2)).get("n0"), "STANDBY");
    Assert.assertEquals(result.getMapField(partitions.get(3)).get("n1"), "STANDBY");

    // n2 should not appear in any assignment
    for (String partition : partitions) {
      Assert.assertFalse(result.getMapField(partition).containsKey("n2"),
          "Dead node n2 should not be assigned to " + partition);
    }

    // Each partition should still have exactly 2 replicas (orphans redistributed)
    for (String partition : partitions) {
      Assert.assertEquals(result.getMapField(partition).size(), 2,
          "Partition " + partition + " should have 2 replicas after rebalance");
    }
  }

  /**
   * Core stickiness property: even an uneven distribution is NOT rebalanced.
   * If node n0 has 80% of LEADER partitions, that stays as-is.
   */
  @Test
  public void testImbalancePreserved() {
    List<String> partitions = makePartitions(4);

    // Intentionally imbalanced: n0 is LEADER for all 4 partitions
    Map<String, Map<String, String>> currentMapping = new HashMap<>();
    for (String partition : partitions) {
      currentMapping.put(partition, mapOf("n0", "LEADER", "n1", "STANDBY"));
    }

    List<String> nodes = makeNodes("n", 3); // 3 nodes but n2 has nothing

    ZNRecord result = runStrategy(partitions, nodes, nodes, currentMapping,
        leaderStandbyStates(), Integer.MAX_VALUE);

    // Imbalance must be preserved — n0 is still LEADER for all 4
    for (String partition : partitions) {
      Assert.assertEquals(result.getMapField(partition).get("n0"), "LEADER",
          "Imbalanced LEADER on n0 should be preserved for " + partition);
      Assert.assertEquals(result.getMapField(partition).get("n1"), "STANDBY",
          "Imbalanced STANDBY on n1 should be preserved for " + partition);
    }

    // n2 should have 0 replicas (nothing orphaned)
    Map<String, Integer> counts = countReplicasPerNode(result, partitions);
    Assert.assertEquals(counts.getOrDefault("n2", 0).intValue(), 0);
  }

  /**
   * Orphaned partitions (no existing mapping) should be distributed to available live nodes.
   */
  @Test
  public void testOrphanedPartitionsAssigned() {
    List<String> partitions = makePartitions(4);

    // p0 and p1 are assigned, p2 and p3 are orphaned (empty mapping)
    Map<String, Map<String, String>> currentMapping = new HashMap<>();
    currentMapping.put(partitions.get(0), mapOf("n0", "LEADER", "n1", "STANDBY"));
    currentMapping.put(partitions.get(1), mapOf("n1", "LEADER", "n0", "STANDBY"));
    // p2 and p3 have no mapping
    currentMapping.put(partitions.get(2), new HashMap<>());
    currentMapping.put(partitions.get(3), new HashMap<>());

    List<String> nodes = makeNodes("n", 3);

    ZNRecord result = runStrategy(partitions, nodes, nodes, currentMapping,
        leaderStandbyStates(), Integer.MAX_VALUE);

    // p0 and p1 assignments unchanged
    Assert.assertEquals(result.getMapField(partitions.get(0)).get("n0"), "LEADER");
    Assert.assertEquals(result.getMapField(partitions.get(0)).get("n1"), "STANDBY");
    Assert.assertEquals(result.getMapField(partitions.get(1)).get("n1"), "LEADER");
    Assert.assertEquals(result.getMapField(partitions.get(1)).get("n0"), "STANDBY");

    // p2 and p3 should now be assigned to some live nodes
    for (int i = 2; i < 4; i++) {
      Map<String, String> assignment = result.getMapField(partitions.get(i));
      Assert.assertEquals(assignment.size(), 2,
          "Orphaned partition " + partitions.get(i) + " should have 2 replicas");
      Assert.assertTrue(assignment.containsValue("LEADER"));
      Assert.assertTrue(assignment.containsValue("STANDBY"));
    }
  }

  /**
   * Edge case: all nodes are down → empty assignment.
   */
  @Test
  public void testAllNodesDown() {
    List<String> partitions = makePartitions(4);
    Map<String, Map<String, String>> currentMapping = new HashMap<>();
    for (String partition : partitions) {
      currentMapping.put(partition, mapOf("n0", "LEADER", "n1", "STANDBY"));
    }

    List<String> allNodes = makeNodes("n", 2);
    List<String> liveNodes = Collections.emptyList(); // all dead

    ZNRecord result = runStrategy(partitions, allNodes, liveNodes, currentMapping,
        leaderStandbyStates(), Integer.MAX_VALUE);

    for (String partition : partitions) {
      Assert.assertTrue(result.getMapField(partition).isEmpty(),
          "All nodes dead — assignment for " + partition + " should be empty");
    }
  }

  /**
   * Single node cluster: all partitions assigned to the sole live node.
   */
  @Test
  public void testSingleNode() {
    List<String> partitions = makePartitions(4);
    Map<String, Map<String, String>> currentMapping = new HashMap<>();

    List<String> nodes = Collections.singletonList("n0");

    // With LEADER=1, STANDBY=1, only 1 replica per partition is possible
    // since we can't put LEADER and STANDBY on the same node
    ZNRecord result = runStrategy(partitions, nodes, nodes, currentMapping,
        leaderStandbyStates(), Integer.MAX_VALUE);

    for (String partition : partitions) {
      Map<String, String> assignment = result.getMapField(partition);
      // Only 1 replica possible on a single node
      Assert.assertEquals(assignment.size(), 1,
          "Single node — only 1 replica per partition for " + partition);
      Assert.assertEquals(assignment.get("n0"), "LEADER",
          "The sole node should be LEADER for " + partition);
    }
  }

  /**
   * The maximumPerNode constraint should be respected. Excess orphans cannot be
   * assigned if all nodes are at capacity.
   */
  @Test
  public void testMaximumPerNodeRespected() {
    List<String> partitions = makePartitions(6);
    Map<String, Map<String, String>> currentMapping = new HashMap<>();
    List<String> nodes = makeNodes("n", 2);

    // maxPerNode = 3 → max total replicas = 6, but we need 12 (6 partitions × 2 states)
    // So only some partitions can be fully assigned
    ZNRecord result = runStrategy(partitions, nodes, nodes, currentMapping,
        leaderStandbyStates(), 3);

    Map<String, Integer> counts = countReplicasPerNode(result, partitions);
    for (Map.Entry<String, Integer> entry : counts.entrySet()) {
      Assert.assertTrue(entry.getValue() <= 3,
          "Node " + entry.getKey() + " has " + entry.getValue()
              + " replicas, exceeds max of 3");
    }
  }

  /**
   * Verify preference list ordering: LEADER nodes should appear before STANDBY nodes.
   */
  @Test
  public void testPreferenceListOrdering() {
    List<String> partitions = makePartitions(3);
    Map<String, Map<String, String>> currentMapping = new HashMap<>();
    currentMapping.put(partitions.get(0), mapOf("n0", "LEADER", "n1", "STANDBY"));
    currentMapping.put(partitions.get(1), mapOf("n1", "LEADER", "n0", "STANDBY"));
    currentMapping.put(partitions.get(2), mapOf("n0", "LEADER", "n1", "STANDBY"));

    List<String> nodes = makeNodes("n", 2);

    ZNRecord result = runStrategy(partitions, nodes, nodes, currentMapping,
        leaderStandbyStates(), Integer.MAX_VALUE);

    for (String partition : partitions) {
      List<String> prefList = result.getListField(partition);
      Map<String, String> assignment = result.getMapField(partition);

      Assert.assertEquals(prefList.size(), 2);
      // First entry should be LEADER
      Assert.assertEquals(assignment.get(prefList.get(0)), "LEADER",
          "First in preference list should be LEADER for " + partition);
      // Second entry should be STANDBY
      Assert.assertEquals(assignment.get(prefList.get(1)), "STANDBY",
          "Second in preference list should be STANDBY for " + partition);
    }
  }

  /**
   * When a node dies and comes back, if its partitions were already reassigned,
   * the returning node does NOT reclaim them (stickiness means we keep the new assignments).
   */
  @Test
  public void testNodeReturnDoesNotReclaimPartitions() {
    List<String> partitions = makePartitions(4);

    // Step 1: Initial assignment with 3 nodes
    Map<String, Map<String, String>> step1Mapping = new HashMap<>();
    step1Mapping.put(partitions.get(0), mapOf("n0", "LEADER", "n1", "STANDBY"));
    step1Mapping.put(partitions.get(1), mapOf("n1", "LEADER", "n2", "STANDBY"));
    step1Mapping.put(partitions.get(2), mapOf("n2", "LEADER", "n0", "STANDBY"));
    step1Mapping.put(partitions.get(3), mapOf("n0", "LEADER", "n2", "STANDBY"));

    List<String> allNodes = makeNodes("n", 3);

    // Step 2: n2 dies — orphans reassigned to n0 and n1
    List<String> liveNodesStep2 = new ArrayList<>();
    liveNodesStep2.add("n0");
    liveNodesStep2.add("n1");

    ZNRecord step2Result = runStrategy(partitions, allNodes, liveNodesStep2, step1Mapping,
        leaderStandbyStates(), Integer.MAX_VALUE);

    // Step 3: n2 returns — use step2 result as currentMapping
    Map<String, Map<String, String>> step2Mapping = new HashMap<>();
    for (String partition : partitions) {
      step2Mapping.put(partition, new HashMap<>(step2Result.getMapField(partition)));
    }

    List<String> liveNodesStep3 = makeNodes("n", 3);

    ZNRecord step3Result = runStrategy(partitions, allNodes, liveNodesStep3, step2Mapping,
        leaderStandbyStates(), Integer.MAX_VALUE);

    // The assignments from step 2 should be preserved — n2 does NOT reclaim
    for (String partition : partitions) {
      Map<String, String> step2Assignment = step2Result.getMapField(partition);
      Map<String, String> step3Assignment = step3Result.getMapField(partition);
      for (Map.Entry<String, String> entry : step2Assignment.entrySet()) {
        Assert.assertEquals(step3Assignment.get(entry.getKey()), entry.getValue(),
            "Node return should not reclaim partition " + partition
                + " from node " + entry.getKey());
      }
    }
  }

  /**
   * Test with OnlineOffline state model (single state, multiple replicas).
   */
  @Test
  public void testOnlineOfflineStateModel() {
    List<String> partitions = makePartitions(4);
    List<String> nodes = makeNodes("n", 3);
    Map<String, Map<String, String>> currentMapping = new HashMap<>();

    LinkedHashMap<String, Integer> states = new LinkedHashMap<>();
    states.put("ONLINE", 2);

    ZNRecord result = runStrategy(partitions, nodes, nodes, currentMapping,
        states, Integer.MAX_VALUE);

    for (String partition : partitions) {
      Map<String, String> assignment = result.getMapField(partition);
      Assert.assertEquals(assignment.size(), 2, "Should have 2 ONLINE replicas for " + partition);
      for (String state : assignment.values()) {
        Assert.assertEquals(state, "ONLINE");
      }
      // Verify no node has both replicas of the same partition
      Set<String> assignedNodes = new HashSet<>(assignment.keySet());
      Assert.assertEquals(assignedNodes.size(), 2,
          "Both replicas should be on different nodes for " + partition);
    }
  }

  /**
   * Test with a large partition count to verify even distribution of orphans.
   */
  @Test
  public void testLargePartitionCountDistribution() {
    int numPartitions = 128;
    int numNodes = 4;
    List<String> partitions = makePartitions(numPartitions);
    List<String> nodes = makeNodes("n", numNodes);
    Map<String, Map<String, String>> currentMapping = new HashMap<>();

    ZNRecord result = runStrategy(partitions, nodes, nodes, currentMapping,
        leaderStandbyStates(), Integer.MAX_VALUE);

    Map<String, Integer> counts = countReplicasPerNode(result, partitions);
    // With 128 partitions × 2 replicas = 256 total, across 4 nodes → 64 each
    for (Map.Entry<String, Integer> entry : counts.entrySet()) {
      Assert.assertTrue(entry.getValue() >= 60 && entry.getValue() <= 68,
          "Node " + entry.getKey() + " has " + entry.getValue()
              + " replicas — expected ~64 (128 partitions × 2 replicas / 4 nodes)");
    }

    // All partitions should be fully assigned
    for (String partition : partitions) {
      Assert.assertEquals(result.getMapField(partition).size(), 2,
          "Partition " + partition + " should have exactly 2 replicas");
    }
  }

  /**
   * Null or missing entries in currentMapping should be handled gracefully.
   */
  @Test
  public void testNullCurrentMapping() {
    List<String> partitions = makePartitions(3);
    List<String> nodes = makeNodes("n", 2);

    // null mapping
    ZNRecord result = runStrategy(partitions, nodes, nodes, null,
        leaderStandbyStates(), Integer.MAX_VALUE);

    for (String partition : partitions) {
      Map<String, String> assignment = result.getMapField(partition);
      Assert.assertNotNull(assignment);
      Assert.assertEquals(assignment.size(), 2,
          "Each partition should have 2 replicas even with null currentMapping");
    }
  }

  // ─── Utility methods ─────────────────────────────────────────────────────────

  private Map<String, Integer> countReplicasPerNode(ZNRecord result, List<String> partitions) {
    Map<String, Integer> counts = new HashMap<>();
    for (String partition : partitions) {
      Map<String, String> assignment = result.getMapField(partition);
      if (assignment != null) {
        for (String node : assignment.keySet()) {
          counts.merge(node, 1, Integer::sum);
        }
      }
    }
    return counts;
  }

  private static Map<String, String> mapOf(String k1, String v1, String k2, String v2) {
    Map<String, String> map = new HashMap<>();
    map.put(k1, v1);
    map.put(k2, v2);
    return map;
  }
}
