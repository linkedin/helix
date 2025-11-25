package org.apache.helix.manager.zk.evacuation;

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
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.helix.constants.EvacuateExclusionType;
import org.apache.helix.model.CurrentState;
import org.apache.helix.model.IdealState;

/**
 * Helper class for applying exclusion filters to partitions during evacuation checks.
 */
public class PartitionExclusionHelper {

  /**
   * Collects all partitions from current states that belong to FULL_AUTO or CUSTOMIZED resources.
   *
   * @param currentStates List of current states for the instance
   * @param allowedResources Set of resources that are FULL_AUTO/CUSTOMIZED and enabled (if required)
   * @return List of PartitionInfo objects representing all partitions on the instance
   */
  public static List<PartitionInfo> collectPartitions(List<CurrentState> currentStates,
      Set<String> allowedResources) {
    if (currentStates == null || currentStates.isEmpty()) {
      return Collections.emptyList();
    }

    List<PartitionInfo> partitions = new ArrayList<>();
    for (CurrentState cs : currentStates) {
      String resourceName = cs.getResourceName();

      // Only consider resources in the allowed set (FULL_AUTO/CUSTOMIZED and possibly enabled)
      if (!allowedResources.contains(resourceName)) {
        continue;
      }

      Map<String, String> partitionStateMap = cs.getPartitionStateMap();
      if (partitionStateMap == null || partitionStateMap.isEmpty()) {
        continue;
      }

      // Collect all partitions for this resource
      for (Map.Entry<String, String> entry : partitionStateMap.entrySet()) {
        partitions.add(new PartitionInfo(entry.getKey(), entry.getValue(), resourceName));
      }
    }

    return partitions;
  }

  /**
   * Creates exclusion filters based on the requested exclusion types.
   * This method only creates filters for exclusion types that are present in the exclusionTypes set.
   *
   * @param exclusionTypes Set of exclusion types to apply
   * @param disabledPartitionsMap Map of resource to disabled partitions (can be null if not needed)
   * @return Map of exclusion type to corresponding filter
   */
  public static Map<EvacuateExclusionType, PartitionExclusionFilter> createExclusionFilters(
      Set<EvacuateExclusionType> exclusionTypes, Map<String, List<String>> disabledPartitionsMap) {

    if (exclusionTypes == null || exclusionTypes.isEmpty()) {
      return Collections.emptyMap();
    }

    Map<EvacuateExclusionType, PartitionExclusionFilter> filters = new HashMap<>();

    for (EvacuateExclusionType exclusionType : exclusionTypes) {
      switch (exclusionType) {
        case ERROR_PARTITIONS:
          filters.put(exclusionType, new ErrorPartitionExclusionFilter());
          break;
        case DISABLED_PARTITION:
          filters.put(exclusionType, new DisabledPartitionExclusionFilter(disabledPartitionsMap));
          break;
        case DISABLED_RESOURCE:
          // DISABLED_RESOURCE is handled at resource level, not partition level
          // It's applied during resource filtering, not partition filtering
          break;
      }
    }

    return filters;
  }

  /**
   * Applies exclusion filters to a list of partitions and returns partitions that are NOT excluded.
   *
   * @param partitions List of partitions to filter
   * @param filters Map of exclusion filters to apply
   * @return List of partitions that should NOT be excluded (i.e., should block evacuation)
   */
  public static List<PartitionInfo> applyExclusions(List<PartitionInfo> partitions,
      Map<EvacuateExclusionType, PartitionExclusionFilter> filters) {

    if (partitions == null || partitions.isEmpty()) {
      return Collections.emptyList();
    }

    if (filters == null || filters.isEmpty()) {
      return partitions; // No exclusions, return all partitions
    }

    return partitions.stream()
        .filter(partition -> !shouldExcludePartition(partition, filters))
        .collect(Collectors.toList());
  }

  /**
   * Determines if a partition should be excluded based on the provided filters.
   *
   * @param partition The partition to check
   * @param filters Map of exclusion filters
   * @return true if the partition should be excluded, false otherwise
   */
  private static boolean shouldExcludePartition(PartitionInfo partition,
      Map<EvacuateExclusionType, PartitionExclusionFilter> filters) {

    // If ANY filter says to exclude this partition, then exclude it
    for (PartitionExclusionFilter filter : filters.values()) {
      if (filter.shouldExclude(partition.getPartitionName(), partition.getState(),
          partition.getResourceName())) {
        return true;
      }
    }
    return false;
  }

  /**
   * Gets partitions from current states for customized resources that are still assigned 
   * to the instance (i.e., not yet reassigned to other instances).
   * This is used for offline instances.
   *
   * @param currentStates List of current states
   * @param idealStates List of ideal states
   * @param instanceName The instance being checked
   * @param allowedResources Set of allowed resources (already filtered)
   * @param filters Exclusion filters to apply
   * @return List of partitions that are still assigned to the instance (after exclusions)
   */
  public static List<PartitionInfo> getCustomizedPartitionsStillOnInstance(
      List<CurrentState> currentStates, List<IdealState> idealStates, String instanceName,
      Set<String> allowedResources, Map<EvacuateExclusionType, PartitionExclusionFilter> filters) {

    if (currentStates == null || idealStates == null) {
      return Collections.emptyList();
    }

    // Create a map of resourceName -> CurrentState
    Map<String, CurrentState> currentStateMap = currentStates.stream()
        .collect(Collectors.toMap(CurrentState::getResourceName, cs -> cs));

    // Create a map of resourceName -> IdealState for CUSTOMIZED resources
    Map<String, IdealState> customizedIdealStateMap = idealStates.stream()
        .filter(is -> is.getRebalanceMode() == IdealState.RebalanceMode.CUSTOMIZED &&
            currentStateMap.containsKey(is.getResourceName()) &&
            allowedResources.contains(is.getResourceName()))
        .collect(Collectors.toMap(IdealState::getResourceName, is -> is));

    List<PartitionInfo> partitionsStillOnInstance = new ArrayList<>();

    // For each customized resource, check which partitions are still assigned to the instance
    for (Map.Entry<String, IdealState> entry : customizedIdealStateMap.entrySet()) {
      String resourceName = entry.getKey();
      IdealState idealState = entry.getValue();
      CurrentState cs = currentStateMap.get(resourceName);

      if (cs == null || cs.getPartitionStateMap() == null) {
        continue;
      }

      for (Map.Entry<String, String> partitionEntry : cs.getPartitionStateMap().entrySet()) {
        String partition = partitionEntry.getKey();
        String state = partitionEntry.getValue();

        PartitionInfo partitionInfo = new PartitionInfo(partition, state, resourceName);

        // Apply exclusion filters
        if (shouldExcludePartition(partitionInfo, filters)) {
          continue; // Skip excluded partitions
        }

        // Check if this partition is still assigned to the instance in IdealState
        Map<String, String> instanceStateMap = idealState.getInstanceStateMap(partition);
        if (instanceStateMap != null && instanceStateMap.containsKey(instanceName)) {
          partitionsStillOnInstance.add(partitionInfo);
        }
      }
    }

    return partitionsStillOnInstance;
  }
}
