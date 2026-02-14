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
import java.util.TreeMap;

import org.apache.helix.controller.dataproviders.ResourceControllerDataProvider;
import org.apache.helix.zookeeper.datamodel.ZNRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * A rebalance strategy that maximizes partition assignment stickiness.
 *
 * <p>Core principle: <b>never move a partition that is already assigned to a live node</b>.
 * Only unassigned ("orphaned") partitions — those whose previous holder is dead or that have
 * never been assigned — are placed, and they are distributed via round-robin across live nodes
 * that still have capacity for the required state.</p>
 *
 * <p>This replicates the semantics of Tendril's {@code SimpleStickinessAssigner}:
 * <ul>
 *   <li>Existing assignments for live nodes are always preserved, even if the distribution
 *       is uneven.</li>
 *   <li>New/orphaned replicas are distributed as evenly as possible among live nodes.</li>
 *   <li>The {@code maximumPerNode} constraint is respected.</li>
 * </ul>
 *
 * <p>Use case: workloads where partition movement is expensive (warm caches, open connections)
 * and even distribution is less important than stability.</p>
 */
public class StickyRebalanceStrategy
    implements RebalanceStrategy<ResourceControllerDataProvider> {

  private static final Logger LOG = LoggerFactory.getLogger(StickyRebalanceStrategy.class);

  private String _resourceName;
  private List<String> _partitions;
  private LinkedHashMap<String, Integer> _states;
  private int _maximumPerNode;

  public StickyRebalanceStrategy() {
  }

  @Override
  public void init(String resourceName, final List<String> partitions,
      final LinkedHashMap<String, Integer> states, int maximumPerNode) {
    _resourceName = resourceName;
    _partitions = partitions;
    _states = states;
    _maximumPerNode = maximumPerNode;
  }

  /**
   * Compute a partition assignment that preserves existing live-node assignments and only
   * assigns orphaned replicas.
   *
   * <p>The returned {@link ZNRecord} contains:
   * <ul>
   *   <li><b>MapFields</b>: partition → (instance → state) — the canonical assignment.</li>
   *   <li><b>ListFields</b>: partition → preference list ordered by state priority.</li>
   * </ul>
   *
   * @param allNodes       All node names (live + non-live).
   * @param liveNodes      Currently live node names.
   * @param currentMapping Current partition → (instance → state) mapping.
   * @param clusterData    Cluster data snapshot (unused by this strategy but required by the
   *                       interface).
   * @return ZNRecord with the computed assignment.
   */
  @Override
  public ZNRecord computePartitionAssignment(final List<String> allNodes,
      final List<String> liveNodes,
      final Map<String, Map<String, String>> currentMapping,
      ResourceControllerDataProvider clusterData) {

    ZNRecord result = new ZNRecord(_resourceName);

    if (liveNodes == null || liveNodes.isEmpty()) {
      // No live nodes — return empty assignment with empty entries for each partition
      for (String partition : _partitions) {
        result.setMapField(partition, Collections.emptyMap());
        result.setListField(partition, Collections.emptyList());
      }
      return result;
    }

    Set<String> liveNodeSet = new HashSet<>(liveNodes);

    // Track how many replicas each live node currently holds (across all partitions)
    Map<String, Integer> nodeReplicaCount = new HashMap<>();
    for (String node : liveNodes) {
      nodeReplicaCount.put(node, 0);
    }

    // ── Phase 1: Preserve existing assignments for live nodes ──────────────────
    // For each partition, keep every assignment where the node is still live.
    Map<String, Map<String, String>> preserved = new TreeMap<>();
    for (String partition : _partitions) {
      Map<String, String> existing = currentMapping != null
          ? currentMapping.get(partition) : null;
      Map<String, String> kept = new TreeMap<>();

      if (existing != null) {
        for (Map.Entry<String, String> entry : existing.entrySet()) {
          String node = entry.getKey();
          String state = entry.getValue();
          if (liveNodeSet.contains(node) && nodeReplicaCount.containsKey(node)) {
            kept.put(node, state);
            nodeReplicaCount.merge(node, 1, Integer::sum);
          }
        }
      }
      preserved.put(partition, kept);
    }

    // ── Phase 2: Assign orphaned replicas via round-robin ──────────────────────
    // For each partition, determine how many more replicas of each state are needed
    // and assign them to live nodes in round-robin order.
    int roundRobinIndex = 0;

    for (String partition : _partitions) {
      Map<String, String> assignment = new TreeMap<>(preserved.get(partition));

      // Count how many replicas of each state are already assigned
      Map<String, Integer> currentStateCounts = new HashMap<>();
      for (String state : assignment.values()) {
        currentStateCounts.merge(state, 1, Integer::sum);
      }

      // For each state in priority order, fill up to the required count
      for (Map.Entry<String, Integer> stateEntry : _states.entrySet()) {
        String state = stateEntry.getKey();
        int required = stateEntry.getValue();
        int current = currentStateCounts.getOrDefault(state, 0);
        int needed = required - current;

        // Try to assign 'needed' more replicas of this state
        for (int assigned = 0; assigned < needed; assigned++) {
          String candidate = findNextCandidate(
              liveNodes, assignment, nodeReplicaCount, roundRobinIndex);
          if (candidate == null) {
            if (LOG.isDebugEnabled()) {
              LOG.debug("Cannot assign state {} for partition {} — no eligible live node",
                  state, partition);
            }
            break;
          }
          assignment.put(candidate, state);
          nodeReplicaCount.merge(candidate, 1, Integer::sum);
          roundRobinIndex++;
        }
      }

      result.setMapField(partition, assignment);
      result.setListField(partition, buildPreferenceList(assignment, _states));
    }

    if (LOG.isDebugEnabled()) {
      LOG.debug("StickyRebalanceStrategy result for {}: {} partitions, {} live nodes",
          _resourceName, _partitions.size(), liveNodes.size());
    }

    return result;
  }

  /**
   * Find the next live node that can accept a replica for the given partition.
   * The node must not already be assigned to this partition and must not exceed
   * {@code _maximumPerNode}.
   *
   * @param liveNodes        Ordered list of live nodes.
   * @param partitionAssignment Current assignment for this partition (node → state).
   * @param nodeReplicaCount Global count of replicas per node.
   * @param startIndex       Round-robin start index.
   * @return The chosen node name, or {@code null} if no eligible node is found.
   */
  private String findNextCandidate(List<String> liveNodes,
      Map<String, String> partitionAssignment,
      Map<String, Integer> nodeReplicaCount,
      int startIndex) {
    int size = liveNodes.size();
    for (int i = 0; i < size; i++) {
      String candidate = liveNodes.get((startIndex + i) % size);
      // Skip if this node already has a replica for this partition
      if (partitionAssignment.containsKey(candidate)) {
        continue;
      }
      // Skip if this node is at maximum capacity
      if (_maximumPerNode > 0 && nodeReplicaCount.getOrDefault(candidate, 0) >= _maximumPerNode) {
        continue;
      }
      return candidate;
    }
    return null;
  }

  /**
   * Build a preference list from a partition's assignment map, ordered by state priority.
   * Nodes in higher-priority states appear first.
   *
   * @param assignment  Partition assignment (node → state).
   * @param states      State priority map (LinkedHashMap preserves priority order).
   * @return Ordered preference list.
   */
  private static List<String> buildPreferenceList(Map<String, String> assignment,
      LinkedHashMap<String, Integer> states) {
    List<String> preferenceList = new ArrayList<>();
    // Iterate states in priority order; for each state, add all nodes in that state
    for (String state : states.keySet()) {
      for (Map.Entry<String, String> entry : assignment.entrySet()) {
        if (state.equals(entry.getValue())) {
          preferenceList.add(entry.getKey());
        }
      }
    }
    return preferenceList;
  }
}
