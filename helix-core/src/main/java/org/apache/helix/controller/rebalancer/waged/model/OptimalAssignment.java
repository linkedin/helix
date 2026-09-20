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

import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import org.apache.helix.HelixException;
import org.apache.helix.model.Partition;
import org.apache.helix.model.ResourceAssignment;

/**
 * The data model represents the optimal assignment of N replicas assigned to M instances;
 * It's mostly used as the return parameter of an assignment calculation algorithm; If the algorithm
 * failed to find optimal assignment given the endeavor, the user could check the failure reasons.
 * Note that this class is not thread safe.
 */
public class OptimalAssignment {
  private Map<String, ResourceAssignment> _optimalAssignment = Collections.emptyMap();
  private Map<AssignableReplica, Map<AssignableNode, List<String>>> _failedAssignments =
      new HashMap<>();
  // Resources that were skipped because their instance-group-tag could not be fully placed while
  // instance-tag isolation is enabled. These resources are absent from _optimalAssignment and the
  // caller is expected to carry their previous assignment forward. Empty in the default global mode.
  private Set<String> _skippedResources = Collections.emptySet();

  /**
   * Update the OptimalAssignment instance with the existing assignment recorded in the input cluster model.
   *
   * @param clusterModel
   */
  public void updateAssignments(ClusterModel clusterModel) {
    Map<String, ResourceAssignment> assignmentMap = new HashMap<>();
    for (AssignableNode node : clusterModel.getAssignableNodes().values()) {
      for (AssignableReplica replica : node.getAssignedReplicas()) {
        String resourceName = replica.getResourceName();
        Partition partition = new Partition(replica.getPartitionName());
        ResourceAssignment resourceAssignment = assignmentMap
            .computeIfAbsent(resourceName, key -> new ResourceAssignment(resourceName));
        Map<String, String> partitionStateMap = resourceAssignment.getReplicaMap(partition);
        if (partitionStateMap.isEmpty()) {
          // ResourceAssignment returns immutable empty map while no such assignment recorded yet.
          // So if the returned map is empty, create a new map.
          partitionStateMap = new HashMap<>();
        }
        partitionStateMap.put(node.getInstanceName(), replica.getReplicaState());
        resourceAssignment.addReplicaMap(partition, partitionStateMap);
      }
    }
    _optimalAssignment = assignmentMap;
  }

  /**
   * @return The optimal assignment in the form of a <Resource Name, ResourceAssignment> map.
   */
  public Map<String, ResourceAssignment> getOptimalResourceAssignment() {
    if (hasAnyFailure()) {
      throw new HelixException(
          "Cannot get the optimal resource assignment since a calculation failure is recorded. "
              + getFailures());
    }
    return _optimalAssignment;
  }

  public void recordAssignmentFailure(AssignableReplica replica,
      Map<AssignableNode, List<String>> failedReasons) {
    _failedAssignments.put(replica, failedReasons);
  }

  public boolean hasAnyFailure() {
    return !_failedAssignments.isEmpty();
  }

  /**
   * Record the resources that were skipped because their instance-group-tag could not be fully
   * placed. Only used when instance-tag isolation is enabled.
   */
  public void setSkippedResources(Set<String> skippedResources) {
    // Defensive copy: the caller owns a mutable set, and an unmodifiable view over it would still
    // change underneath this object if that set were ever touched again.
    _skippedResources = skippedResources == null || skippedResources.isEmpty()
        ? Collections.emptySet()
        : Collections.unmodifiableSet(new HashSet<>(skippedResources));
  }

  /**
   * @return The resources that were skipped by instance-tag isolation. These resources have no entry
   *         in {@link #getOptimalResourceAssignment()} and their previous assignment must be carried
   *         forward by the caller. Never null; empty in the default global mode.
   */
  public Set<String> getSkippedResources() {
    return _skippedResources;
  }

  public String getFailures() {
    // TODO: format the error string
    return _failedAssignments.toString();
  }

  /**
   * @return A map of failure reason description to the number of nodes that failed for that reason,
   *         aggregated across all failed replicas.
   */
  public Map<String, Long> getFailureSummary() {
    return _failedAssignments.values().stream()
        .flatMap(nodeFailures -> nodeFailures.values().stream())
        .flatMap(List::stream)
        .collect(Collectors.groupingBy(reason -> reason != null ? reason : "Unknown",
            Collectors.counting()));
  }
}
