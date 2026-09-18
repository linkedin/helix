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

import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import org.apache.helix.HelixException;
import org.apache.helix.controller.rebalancer.topology.Topology;
import org.apache.helix.controller.rebalancer.util.WagedValidationUtil;
import org.apache.helix.model.ClusterConfig;
import org.apache.helix.model.ClusterTopologyConfig;
import org.apache.helix.model.InstanceConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * This class represents a possible allocation of the replication.
 * Note that any usage updates to the AssignableNode are not thread safe.
 */
public class AssignableNode implements Comparable<AssignableNode> {
  private static final Logger LOG = LoggerFactory.getLogger(AssignableNode.class.getName());

  // Immutable Instance Properties
  private final String _instanceName;
  private final String _logicaId;
  private final String _faultZone;
  // maximum number of the partitions that can be assigned to the instance.
  private final int _maxPartition;
  private final ImmutableSet<String> _instanceTags;
  private final ImmutableMap<String, List<String>> _disabledPartitionsMap;
  private final ImmutableMap<String, Integer> _maxAllowedCapacity;

  // Mutable (Dynamic) Instance Properties
  // A map of <resource name, <partition name, replica>> that tracks the replicas assigned to the
  // node.
  private Map<String, Map<String, AssignableReplica>> _currentAssignedReplicaMap;
  // A map of <capacity key, capacity value> that tracks the current available node capacity
  private Map<String, Integer> _remainingCapacity;
  // Occupancy that physically exists on this node but that the plan does not account for: a
  // replica left in a state the state model leaves uncounted, whose drop never completed. The
  // capacity ledger charges such a replica while the planner cannot model it at all, so without
  // this the planner sees room the ledger will refuse to hand out and keeps re-proposing
  // placements that get vetoed.
  //
  // Deliberately kept out of both _remainingCapacity and _maxAllowedCapacity. Those feed
  // getProjectedHighestUtilization and ClusterContext's cluster-wide totals, so folding it in
  // there would shift preference scores and could manufacture a capacity deficit. This is read by
  // NodeCapacityConstraint alone, as pure eligibility.
  // Invariant: holds exactly the physical occupancy _remainingCapacity does NOT reflect, so
  // assign()/release() move weight across the boundary to keep it charged exactly once.
  private Map<String, Integer> _hiddenOccupancy = Collections.emptyMap();
  // Keys (resource|partition) making up _hiddenOccupancy, so a replica is never charged for room
  // it is itself already occupying.
  private Set<String> _hiddenOccupancyKeys = Collections.emptySet();
  private Map<String, Integer> _remainingTopStateCapacity;

  /**
   * Update the node with a ClusterDataCache. This resets the current assignment and recalculates
   * currentCapacity.
   * NOTE: While this is required to be used in the constructor, this can also be used when the
   * clusterCache needs to be
   * refreshed. This is under the assumption that the capacity mappings of InstanceConfig and
   * ResourceConfig could
   * subject to change. If the assumption is no longer true, this function should become private.
   */
  AssignableNode(ClusterConfig clusterConfig, ClusterTopologyConfig clusterTopologyConfig,
      InstanceConfig instanceConfig, String instanceName) {
    _instanceName = instanceName;
    _logicaId = clusterTopologyConfig != null ? instanceConfig.getLogicalId(
        clusterTopologyConfig.getEndNodeType())
            : instanceName;
    Map<String, Integer> instanceCapacity = fetchInstanceCapacity(clusterConfig, instanceConfig);
    _faultZone = computeFaultZone(clusterConfig, instanceConfig);
    _instanceTags = ImmutableSet.copyOf(instanceConfig.getTags());
    _disabledPartitionsMap = ImmutableMap.copyOf(instanceConfig.getDisabledPartitionsMap());
    // make a copy of max capacity
    _maxAllowedCapacity = ImmutableMap.copyOf(instanceCapacity);
    _remainingCapacity = new HashMap<>(instanceCapacity);
    _remainingTopStateCapacity = new HashMap<>(instanceCapacity);
    _maxPartition = clusterConfig.getMaxPartitionsPerInstance();
    _currentAssignedReplicaMap = new HashMap<>();
  }

  AssignableNode(ClusterConfig clusterConfig, InstanceConfig instanceConfig, String instanceName) {
    this(clusterConfig, null, instanceConfig, instanceName);
  }

  /**
   * Record occupancy that physically exists on this node but that the plan does not place here.
   * <p>
   * Nothing is deducted from {@link #_remainingCapacity} or {@link #_maxAllowedCapacity}. Both are
   * read by {@code getProjectedHighestUtilization} and by {@code ClusterContext}'s cluster-wide
   * capacity totals, so deducting there would move preference scores and could drive the cluster
   * estimate negative, which aborts the whole rebalance with CAPACITY_DEFICIT. Keeping it in a
   * separate field confines the effect to {@code NodeCapacityConstraint}, where it acts purely as
   * eligibility.
   * @param hiddenOccupancy running total of unaccounted weight, by capacity key. Must be mutable:
   *                        assign()/release() adjust it as replicas move into and out of the plan.
   * @param hiddenOccupancyKeys the (resource, partition) keys making up that total
   */
  void setHiddenOccupancy(Map<String, Integer> hiddenOccupancy, Set<String> hiddenOccupancyKeys) {
    _hiddenOccupancy = hiddenOccupancy;
    _hiddenOccupancyKeys = hiddenOccupancyKeys;
  }

  /**
   * The unaccounted physical weight this node carries for the given capacity key when considering
   * the given replica.
   * <p>
   * A replica that is itself part of the hidden occupancy is not charged for the room it already
   * physically occupies -- placing it back where it already sits needs no new room, and charging
   * it would bias the planner into moving a replica that is currently stuck exactly where moving
   * it is least likely to help.
   * @param capacityKey the capacity dimension being checked
   * @param candidate the replica being considered for this node
   * @return weight to withhold from this node's remaining capacity, never negative
   */
  public int getHiddenOccupancy(String capacityKey, AssignableReplica candidate) {
    int hidden = _hiddenOccupancy.getOrDefault(capacityKey, 0);
    if (_hiddenOccupancyKeys
        .contains(occupancyKey(candidate.getResourceName(), candidate.getPartitionName()))) {
      hidden -= candidate.getCapacity().getOrDefault(capacityKey, 0);
    }
    return Math.max(0, hidden);
  }

  /**
   * @return the unaccounted physical weight this node carries, by capacity key. Empty unless the
   *         feature is enabled and the node holds replicas the plan does not place here.
   */
  Map<String, Integer> getHiddenOccupancy() {
    return Collections.unmodifiableMap(_hiddenOccupancy);
  }

  /**
   * This function should only be used to assign a set of new partitions that are not allocated on
   * this node. It's because the any exception could occur at the middle of batch assignment and the
   * previous finished assignment cannot be reverted
   * Using this function avoids the overhead of updating capacity repeatedly.
   */
  void assignInitBatch(Collection<AssignableReplica> replicas) {
    Map<String, Integer> totalTopStatePartitionCapacity = new HashMap<>();
    Map<String, Integer> totalPartitionCapacity = new HashMap<>();
    for (AssignableReplica replica : replicas) {
      // TODO: the exception could occur in the middle of for loop and the previous added records cannot be reverted
      addToAssignmentRecord(replica);
      // increment the capacity requirement according to partition's capacity configuration.
      for (Map.Entry<String, Integer> capacity : replica.getCapacity().entrySet()) {
        if (replica.isReplicaTopState()) {
          totalTopStatePartitionCapacity.compute(capacity.getKey(),
              (key, totalValue) -> (totalValue == null) ? capacity.getValue()
                  : totalValue + capacity.getValue());
        }
        totalPartitionCapacity.compute(capacity.getKey(),
            (key, totalValue) -> (totalValue == null) ? capacity.getValue()
                : totalValue + capacity.getValue());
      }
    }

    // Update the global state after all single replications' calculation is done.
    updateRemainingCapacity(totalTopStatePartitionCapacity, _remainingTopStateCapacity, false);
    updateRemainingCapacity(totalPartitionCapacity, _remainingCapacity, false);
  }

  /**
   * Assign a replica to the node.
   * @param assignableReplica - the replica to be assigned
   */
  void assign(AssignableReplica assignableReplica) {
    addToAssignmentRecord(assignableReplica);
    updateRemainingCapacity(assignableReplica.getCapacity(), _remainingCapacity, false);
    if (assignableReplica.isReplicaTopState()) {
      updateRemainingCapacity(assignableReplica.getCapacity(), _remainingTopStateCapacity, false);
    }
    // Now reflected in _remainingCapacity, so it must leave the hidden total or it would be
    // charged twice.
    moveOutOfHiddenOccupancy(assignableReplica, true);
  }

  /**
   * Release a replica from the node.
   * If the replication is not on this node, the assignable node is not updated.
   * @param replica - the replica to be released
   */
  void release(AssignableReplica replica)
      throws IllegalArgumentException {
    String resourceName = replica.getResourceName();
    String partitionName = replica.getPartitionName();

    // Check if the release is necessary
    if (!_currentAssignedReplicaMap.containsKey(resourceName)) {
      LOG.warn("Resource {} is not on node {}. Ignore the release call.", resourceName,
          getInstanceName());
      return;
    }

    Map<String, AssignableReplica> partitionMap = _currentAssignedReplicaMap.get(resourceName);
    if (!partitionMap.containsKey(partitionName) || !partitionMap.get(partitionName)
        .equals(replica)) {
      LOG.warn("Replica {} is not assigned to node {}. Ignore the release call.",
          replica.toString(), getInstanceName());
      return;
    }

    AssignableReplica removedReplica = partitionMap.remove(partitionName);
    updateRemainingCapacity(removedReplica.getCapacity(), _remainingCapacity, true);
    if (removedReplica.isReplicaTopState()) {
      updateRemainingCapacity(removedReplica.getCapacity(), _remainingTopStateCapacity, true);
    }
    // Rollback of an assignment: the replica is physically still here, so its weight goes back to
    // the hidden total that _remainingCapacity no longer reflects.
    moveOutOfHiddenOccupancy(removedReplica, false);
  }

  /**
   * Keep the hidden-occupancy total in step with _remainingCapacity for a replica that is
   * physically present on this node but was not part of the plan the model was built with.
   * Such a replica must be charged exactly once: while unassigned it is counted in
   * _hiddenOccupancy, and once assigned it is counted in _remainingCapacity instead.
   * @param replica the replica being assigned or released
   * @param assigned true when the replica was just assigned, false when an assignment was reverted
   */
  private void moveOutOfHiddenOccupancy(AssignableReplica replica, boolean assigned) {
    if (!_hiddenOccupancyKeys
        .contains(occupancyKey(replica.getResourceName(), replica.getPartitionName()))) {
      return;
    }
    for (Map.Entry<String, Integer> capacity : replica.getCapacity().entrySet()) {
      // A dimension the node carries no hidden occupancy on has nothing to move across, and
      // merging there would leave a stray entry behind on every assign/release cycle.
      if (!_hiddenOccupancy.containsKey(capacity.getKey())) {
        continue;
      }
      _hiddenOccupancy.merge(capacity.getKey(),
          assigned ? -capacity.getValue() : capacity.getValue(), Integer::sum);
    }
  }

  /**
   * The key identifying a replica within the hidden-occupancy bookkeeping. Deliberately
   * (resource, partition) rather than (resource, partition, state) so it matches the granularity
   * the capacity check itself dedupes on.
   */
  static String occupancyKey(String resourceName, String partitionName) {
    return resourceName + "|" + partitionName;
  }

  /**
   * @return A set of all assigned replicas on the node.
   */
  Set<AssignableReplica> getAssignedReplicas() {
    return _currentAssignedReplicaMap.values().stream()
        .flatMap(replicaMap -> replicaMap.values().stream()).collect(Collectors.toSet());
  }

  /**
   * @return The current assignment in a map of <resource name, set of partition names>
   */
  Map<String, Set<String>> getAssignedPartitionsMap() {
    Map<String, Set<String>> assignmentMap = new HashMap<>();
    for (String resourceName : _currentAssignedReplicaMap.keySet()) {
      assignmentMap.put(resourceName, _currentAssignedReplicaMap.get(resourceName).keySet());
    }
    return assignmentMap;
  }

  /**
   * @param resource Resource name
   * @return A set of the current assigned replicas' partition names in the specified resource.
   */
  public Set<String> getAssignedPartitionsByResource(String resource) {
    return _currentAssignedReplicaMap.getOrDefault(resource, Collections.emptyMap()).keySet();
  }

  /**
   * @param resource Resource name
   * @return A set of the current assigned replicas' partition names with the top state in the
   *         specified resource.
   */
  Set<String> getAssignedTopStatePartitionsByResource(String resource) {
    return _currentAssignedReplicaMap.getOrDefault(resource, Collections.emptyMap()).entrySet()
        .stream().filter(partitionEntry -> partitionEntry.getValue().isReplicaTopState())
        .map(partitionEntry -> partitionEntry.getKey()).collect(Collectors.toSet());
  }

  /**
   * @return The total count of assigned top state partitions.
   */
  public int getAssignedTopStatePartitionsCount() {
    return (int) _currentAssignedReplicaMap.values().stream()
        .flatMap(replicaMap -> replicaMap.values().stream())
        .filter(AssignableReplica::isReplicaTopState).count();
  }

  /**
   * @return The total count of assigned replicas.
   */
  public int getAssignedReplicaCount() {
    return _currentAssignedReplicaMap.values().stream().mapToInt(Map::size).sum();
  }

  /**
   * @return The current available capacity.
   */
  public Map<String, Integer> getRemainingCapacity() {
    return _remainingCapacity;
  }

  /**
   * @return A map of <capacity category, capacity number> that describes the max capacity of the
   *         node.
   */
  public Map<String, Integer> getMaxCapacity() {
    return _maxAllowedCapacity;
  }

  /**
   * Return the most concerning capacity utilization number for evenly partition assignment.
   * The method dynamically calculates the projected highest utilization number among all the
   * capacity categories assuming the new capacity usage is added to the node.
   * For example, if the current node usage is {CPU: 0.9, MEM: 0.4, DISK: 0.6}. Then this call shall
   * return 0.9.
   * @param newUsage the proposed new additional capacity usage.
   * @return The highest utilization number of the node among all the capacity category.
   */
  public float getGeneralProjectedHighestUtilization(Map<String, Integer> newUsage) {
    return getProjectedHighestUtilization(newUsage, _remainingCapacity, null);
  }

  /**
   * Return the most concerning capacity utilization number for evenly partition assignment.
   * The method dynamically calculates the projected highest utilization number among all the
   * capacity categories assuming the new capacity usage is added to the node.
   *
   * If the list of preferredScoringKeys is specified then utilization number is computed based op the
   * specified capacity category (keys) in the list only.
   *
   * For example, if the current node usage is {CPU: 0.9, MEM: 0.4, DISK: 0.6}, preferredScoringKeys: [ CPU ]
   * Then this call shall return 0.9.
   *
   * @param newUsage            the proposed new additional capacity usage.
   * @param preferredScoringKeys if provided, the capacity utilization will be calculated based on
   *                            the supplied keys only, else across all capacity categories.
   * @return The highest utilization number of the node among the specified capacity category.
   */
  public float getGeneralProjectedHighestUtilization(Map<String, Integer> newUsage,
      List<String> preferredScoringKeys) {
    return getProjectedHighestUtilization(newUsage, _remainingCapacity, preferredScoringKeys);
  }

  /**
   * Return the most concerning capacity utilization number for evenly partition assignment.
   * The method dynamically calculates the projected highest utilization number among all the
   * capacity categories assuming the new capacity usage is added to the node.
   * For example, if the current node usage is {CPU: 0.9, MEM: 0.4, DISK: 0.6}. Then this call shall
   * return 0.9.
   * This function returns projected highest utilization for only top state partitions.
   * @param newUsage the proposed new additional capacity usage.
   * @return The highest utilization number of the node among all the capacity category.
   */
  public float getTopStateProjectedHighestUtilization(Map<String, Integer> newUsage) {
    return getProjectedHighestUtilization(newUsage, _remainingTopStateCapacity, null);
  }

  /**
   * Return the most concerning capacity utilization number for evenly partition assignment.
   * The method dynamically calculates the projected highest utilization number among all the
   * capacity categories assuming the new capacity usage is added to the node.
   *
   * If the list of preferredScoringKeys is specified then utilization number is computed based op the
   * specified capacity category (keys) in the list only.
   *
   * For example, if the current node usage is {CPU: 0.9, MEM: 0.4, DISK: 0.6}, preferredScoringKeys: [ CPU ]
   * Then this call shall return 0.9.
   *
   * This function returns projected highest utilization for only top state partitions.
   *
   * @param newUsage            the proposed new additional capacity usage.
   * @param preferredScoringKeys if provided, the capacity utilization will be calculated based on
   *                            the supplied keys only, else across all capacity categories.
   * @return The highest utilization number of the node among all the capacity category.
   */
  public float getTopStateProjectedHighestUtilization(Map<String, Integer> newUsage, List<String> preferredScoringKeys) {
    return getProjectedHighestUtilization(newUsage, _remainingTopStateCapacity, preferredScoringKeys);
  }

  private float getProjectedHighestUtilization(Map<String, Integer> newUsage,
      Map<String, Integer> remainingCapacity, List<String> preferredScoringKeys) {
    Set<String> capacityKeySet = _maxAllowedCapacity.keySet();
    if (preferredScoringKeys != null && preferredScoringKeys.size() != 0 && capacityKeySet.contains(preferredScoringKeys.get(0))) {
      capacityKeySet = preferredScoringKeys.stream().collect(Collectors.toSet());
    }
    float highestCapacityUtilization = 0;
    for (String capacityKey : capacityKeySet) {
      float capacityValue = _maxAllowedCapacity.get(capacityKey);
      float utilization = (capacityValue - remainingCapacity.get(capacityKey) + newUsage
          .getOrDefault(capacityKey, 0)) / capacityValue;
      highestCapacityUtilization = Math.max(highestCapacityUtilization, utilization);
    }
    return highestCapacityUtilization;
  }

  public String getInstanceName() {
    return _instanceName;
  }

  public String getLogicalId() {
    return _logicaId;
  }

  public Set<String> getInstanceTags() {
    return _instanceTags;
  }

  public String getFaultZone() {
    return _faultZone;
  }

  public boolean hasFaultZone() {
    return _faultZone != null;
  }

  /**
   * @return A map of <resource name, set of partition names> contains all the partitions that are
   *         disabled on the node.
   */
  public Map<String, List<String>> getDisabledPartitionsMap() {
    return _disabledPartitionsMap;
  }

  /**
   * @return The max partition count that are allowed to be allocated on the node.
   */
  public int getMaxPartition() {
    return _maxPartition;
  }

  /**
   * Computes the fault zone id based on the domain and fault zone type when topology is enabled.
   * For example, when
   * the domain is "zone=2, instance=testInstance" and the fault zone type is "zone", this function
   * returns "2".
   * If cannot find the fault zone type, this function leaves the fault zone id as the instance name.
   * Note the WAGED rebalancer does not require full topology tree to be created. So this logic is
   * simpler than the CRUSH based rebalancer.
   */
  private String computeFaultZone(ClusterConfig clusterConfig, InstanceConfig instanceConfig) {
    LinkedHashMap<String, String> instanceTopologyMap = Topology
        .computeInstanceTopologyMap(clusterConfig, instanceConfig.getInstanceName(), instanceConfig,
            true /*earlyQuitTillFaultZone*/);

    StringBuilder faultZoneStringBuilder = new StringBuilder();
    for (Map.Entry<String, String> entry : instanceTopologyMap.entrySet()) {
      faultZoneStringBuilder.append(entry.getValue());
      faultZoneStringBuilder.append('/');
    }
    faultZoneStringBuilder.setLength(faultZoneStringBuilder.length() - 1);
    return faultZoneStringBuilder.toString();
  }

  /**
   * @throws HelixException if the replica has already been assigned to the node.
   */
  private void addToAssignmentRecord(AssignableReplica replica) {
    String resourceName = replica.getResourceName();
    String partitionName = replica.getPartitionName();
    if (_currentAssignedReplicaMap.containsKey(resourceName) && _currentAssignedReplicaMap
        .get(resourceName).containsKey(partitionName)) {
      throw new HelixException(String
          .format("Resource %s already has a replica with state %s from partition %s on node %s",
              replica.getResourceName(), replica.getReplicaState(), replica.getPartitionName(),
              getInstanceName()));
    } else {
      _currentAssignedReplicaMap.computeIfAbsent(resourceName, key -> new HashMap<>())
          .put(partitionName, replica);
    }
  }

  private void updateRemainingCapacity(Map<String, Integer> usedCapacity, Map<String, Integer> remainingCapacity,
      boolean isRelease) {
    int multiplier = isRelease ? -1 : 1;
    // if the used capacity key does not exist in the node's capacity, ignore it
    usedCapacity.forEach((capacityKey, capacityValue) -> remainingCapacity.compute(capacityKey,
        (key, value) -> value == null ? null : value - multiplier * capacityValue));
  }

  /**
   * Get and validate the instance capacity from instance config.
   * @throws HelixException if any required capacity key is not configured in the instance config.
   */
  private Map<String, Integer> fetchInstanceCapacity(ClusterConfig clusterConfig,
      InstanceConfig instanceConfig) {
    Map<String, Integer> instanceCapacity =
        WagedValidationUtil.validateAndGetInstanceCapacity(clusterConfig, instanceConfig);
    // Remove all the non-required capacity items from the map.
    instanceCapacity.keySet().retainAll(clusterConfig.getInstanceCapacityKeys());
    return instanceCapacity;
  }

  @Override
  public int hashCode() {
    return _instanceName.hashCode();
  }

  @Override
  public int compareTo(AssignableNode o) {
    return _logicaId.compareTo(o.getLogicalId());
  }

  @Override
  public String toString() {
    return _instanceName;
  }
}
