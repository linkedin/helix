package org.apache.helix.controller.rebalancer;

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
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import org.apache.helix.controller.dataproviders.ResourceControllerDataProvider;
import org.apache.helix.controller.stages.CurrentStateOutput;
import org.apache.helix.model.IdealState;
import org.apache.helix.model.Partition;
import org.apache.helix.model.StateModelDefinition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MaintenanceRebalancer extends SemiAutoRebalancer<ResourceControllerDataProvider> {
  private static final Logger LOG = LoggerFactory.getLogger(MaintenanceRebalancer.class);

  @Override
  public IdealState computeNewIdealState(String resourceName, IdealState currentIdealState,
      CurrentStateOutput currentStateOutput, ResourceControllerDataProvider clusterData) {
    LOG.info(String
        .format("Start computing ideal state for resource %s in maintenance mode.", resourceName));
    Map<Partition, Map<String, String>> currentStateMap =
        currentStateOutput.getCurrentStateMap(resourceName);
    if (currentStateMap == null || currentStateMap.size() == 0) {
      LOG.warn(String
          .format("No new partition will be assigned for %s in maintenance mode", resourceName));

      // Clear all preference lists, if the resource has not yet been rebalanced,
      // leave it as is
      for (List<String> pList : currentIdealState.getPreferenceLists().values()) {
        pList.clear();
      }
      return currentIdealState;
    }

    // One principal is to prohibit DROP -> OFFLINE and OFFLINE -> DROP state transitions.
    // Derived preference list from current state with state priority
    StateModelDefinition stateModelDef = clusterData.getStateModelDef(currentIdealState.getStateModelDefRef());

    for (Partition partition : currentStateMap.keySet()) {
      Map<String, String> stateMap = currentStateMap.get(partition);
      List<String> preferenceList = new ArrayList<>(stateMap.keySet());

      /**
       * This sorting preserves the ordering of current state hosts in the order of current IS pref list
       * Example:
       * ideal state pref-list: [A, B, C]
       * current-state: {
       *     A: FOLLOWER,
       *     B: LEADER,
       *     C: FOLLOWER
       * }
       * Lets say newPrefList = new ArrayList<>(current-state.keySet()) => [C, B, A]
       *
       * Sort 1: Sort based on preference-list order:
       * --------------------------------------------------------
       * newPrefList = [C, B, A] => [A, B, C]
       */
      Collections.sort(preferenceList, new PreferenceListNodeComparator(stateMap, stateModelDef,
          currentIdealState.getPreferenceList(partition.getPartitionName()), clusterData));

      /**
       * Sort 2: Sort based on state-priority order:
       * --------------------------------------------------------
       * newPrefList = [A, B, C] => [B, A, C]
       * Here, A will be 2nd and C will be third always as both have same priority so original (pref-list) order will be maintained.
       */
      preferenceList.sort(new StatePriorityComparator(stateMap, stateModelDef));

      currentIdealState.setPreferenceList(partition.getPartitionName(), preferenceList);
    }

    // Clear preferenceList for partitions that have no participant CurrentState.
    // Under maintenance mode the cluster is meant to be frozen: new replicas should
    // not be bootstrapped. A listFields entry without any CurrentState represents a
    // planned-but-not-executed placement (e.g., WAGED wrote a target for a partition
    // whose in-flight swap had not converged before MM activated). Preserving such an
    // entry causes the inherited mapping calculator to dispatch a bootstrap that
    // bypasses the per-pipeline capacity check in DelayedAutoRebalancer, which can
    // push hosts over their configured cap. Generalize Branch A's
    // "clear-all-when-no-CS" behavior to a per-partition rule.
    Set<String> partitionNamesWithCurrentState = currentStateMap.keySet().stream()
        .map(Partition::getPartitionName)
        .collect(Collectors.toCollection(HashSet::new));
    for (Map.Entry<String, List<String>> entry :
        currentIdealState.getPreferenceLists().entrySet()) {
      if (!partitionNamesWithCurrentState.contains(entry.getKey())) {
        if (!entry.getValue().isEmpty()) {
          LOG.info(
              "Clearing preferenceList for partition {} in resource {} under maintenance mode "
                  + "(no participant CurrentState).",
              entry.getKey(), resourceName);
          entry.getValue().clear();
        }
      }
    }

    LOG.info(String
        .format("End computing ideal state for resource %s in maintenance mode.", resourceName));
    return currentIdealState;
  }
}
