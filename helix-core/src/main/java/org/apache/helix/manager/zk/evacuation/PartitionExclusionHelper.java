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
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.helix.HelixDataAccessor;
import org.apache.helix.constants.InstanceDrainExclusionType;
import org.apache.helix.model.CurrentState;
import org.apache.helix.model.IdealState;
import org.apache.helix.model.InstanceConfig;

/**
 * Helper class for applying exclusion filters to partitions during evacuation checks.
 */
public class PartitionExclusionHelper {

  /**
   * Collects all partitions from current states for the specified resources.
   *
   * @param currentStates List of current states for the instance
   * @param allowedResources Set of resources to consider
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

      // Only consider resources in the allowed set
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
   * @param accessor HelixDataAccessor to fetch instance configuration data if needed
   * @param instanceName Instance name for which to fetch configuration
   * @return Map of exclusion type to corresponding filter
   */
  public static Map<InstanceDrainExclusionType, PartitionExclusionFilter> createExclusionFilters(
      Set<InstanceDrainExclusionType> exclusionTypes, HelixDataAccessor accessor,
      String instanceName) {

    if (exclusionTypes == null || exclusionTypes.isEmpty()) {
      return Collections.emptyMap();
    }

    Map<InstanceDrainExclusionType, PartitionExclusionFilter> filters = new HashMap<>();

    for (InstanceDrainExclusionType exclusionType : exclusionTypes) {
      switch (exclusionType) {
        case ERROR_PARTITIONS:
          filters.put(exclusionType, new ErrorPartitionExclusionFilter());
          break;
        case DISABLED_PARTITION:
          // Fetch InstanceConfig only when DISABLED_PARTITION exclusion is requested
          InstanceConfig instanceConfig =
              accessor.getProperty(accessor.keyBuilder().instanceConfig(instanceName));
          Map<String, List<String>> disabledPartitionsMap = instanceConfig != null ?
              instanceConfig.getDisabledPartitionsMap() : Collections.emptyMap();
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
      Map<InstanceDrainExclusionType, PartitionExclusionFilter> filters) {

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
      Map<InstanceDrainExclusionType, PartitionExclusionFilter> filters) {

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
   * Gets partitions that block evacuation for an offline instance with customized resources.
   * Uses union semantics: a partition blocks evacuation if it exists in CurrentState (data still
   * on the instance) OR if it is assigned to this instance in IdealState (assignment not moved).
   * This prevents premature evacuation completion when partition names rotate in IdealState
   * (e.g., segment generation changes) while the instance still holds data.
   *
   * @param currentStates List of current states
   * @param idealStates List of ideal states
   * @param instanceName The instance being checked
   * @param allowedResources Set of allowed resources (already filtered)
   * @param filters Exclusion filters to apply
   * @return List of partitions blocking evacuation (union of CurrentState and IdealState)
   */
  public static List<PartitionInfo> getCustomizedPartitionsStillOnInstance(
      List<CurrentState> currentStates, List<IdealState> idealStates, String instanceName,
      Set<String> allowedResources, Map<InstanceDrainExclusionType, PartitionExclusionFilter> filters) {

    if (currentStates == null || idealStates == null) {
      return Collections.emptyList();
    }

    Map<String, CurrentState> currentStateMap = currentStates.stream()
        .collect(Collectors.toMap(CurrentState::getResourceName, cs -> cs));

    Map<String, IdealState> customizedIdealStateMap = idealStates.stream()
        .filter(is -> is.getRebalanceMode() == IdealState.RebalanceMode.CUSTOMIZED
            && allowedResources.contains(is.getResourceName()))
        .collect(Collectors.toMap(IdealState::getResourceName, is -> is));

    List<PartitionInfo> partitionsStillOnInstance = new ArrayList<>();

    for (Map.Entry<String, IdealState> entry : customizedIdealStateMap.entrySet()) {
      String resourceName = entry.getKey();
      IdealState idealState = entry.getValue();
      CurrentState cs = currentStateMap.get(resourceName);
      Set<String> seenPartitions = new HashSet<>();

      // Any non-excluded partition in CurrentState blocks evacuation, regardless of whether
      // it appears in IdealState. This handles partition name rotation (e.g., segment
      // generation changes) where old-gen partitions in CS no longer match new-gen IS names.
      if (cs != null && cs.getPartitionStateMap() != null) {
        for (Map.Entry<String, String> partitionEntry : cs.getPartitionStateMap().entrySet()) {
          String partition = partitionEntry.getKey();
          String state = partitionEntry.getValue();

          PartitionInfo partitionInfo = new PartitionInfo(partition, state, resourceName);

          if (shouldExcludePartition(partitionInfo, filters)) {
            continue;
          }

          if (seenPartitions.add(partition)) {
            partitionsStillOnInstance.add(partitionInfo);
          }
        }
      }

      // Any partition assigned to this instance in IdealState also blocks evacuation, even if
      // not present in CurrentState (e.g., new assignment the offline instance hasn't picked up).
      Map<String, Map<String, String>> mapFields = idealState.getRecord().getMapFields();
      if (mapFields != null) {
        for (Map.Entry<String, Map<String, String>> isEntry : mapFields.entrySet()) {
          String partition = isEntry.getKey();
          if (seenPartitions.contains(partition)) {
            continue;
          }
          Map<String, String> instanceStateMap = isEntry.getValue();
          if (instanceStateMap != null && instanceStateMap.containsKey(instanceName)) {
            String desiredState = instanceStateMap.get(instanceName);
            if (seenPartitions.add(partition)) {
              partitionsStillOnInstance.add(
                  new PartitionInfo(partition, desiredState, resourceName));
            }
          }
        }
      }
    }

    return partitionsStillOnInstance;
  }
}
