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
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.helix.controller.dataproviders.ResourceControllerDataProvider;
import org.apache.helix.controller.stages.CurrentStateOutput;
import org.apache.helix.model.IdealState;
import org.apache.helix.model.Partition;
import org.apache.helix.model.StateModelDefinition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MaintenanceRebalancer extends SemiAutoRebalancer<ResourceControllerDataProvider> {
  private static final Logger LOG = LoggerFactory.getLogger(MaintenanceRebalancer.class);

  /**
   * Under maintenance mode, the cluster is meant to be frozen: no new replicas should be
   * bootstrapped. For every partition of the resource, the rebalancer sets the
   * preferenceList to the participant-reported CurrentState hosts (or empty if no
   * participant reports state for that partition).
   *
   * <p>For partitions with CurrentState reports, the preferenceList is rebuilt from the
   * reported hosts, ordered to keep top-state hosts first (so role assignments are
   * preserved). For partitions without CurrentState, the preferenceList is set to empty,
   * which causes the inherited mapping calculator to produce no BestPossibleState and
   * therefore no OFFLINE -> ASSIGNED message dispatch.
   *
   * <p>This is the single-rule version of what was previously a two-branch implementation
   * (Branch A: clear all when the entire resource has no CurrentState; Branch B: rebuild
   * preferenceLists only for partitions with CurrentState). The previous Branch B silently
   * preserved listFields entries for partitions whose participants had not reported yet,
   * which let WAGED-written speculative placements (target hosts for in-flight swaps that
   * had not converged) be dispatched without the per-pipeline capacity check that lives
   * in DelayedAutoRebalancer. The unified rule eliminates that asymmetry.
   */
  @Override
  public IdealState computeNewIdealState(String resourceName, IdealState currentIdealState,
      CurrentStateOutput currentStateOutput, ResourceControllerDataProvider clusterData) {
    LOG.info("Start computing ideal state for resource {} in maintenance mode.", resourceName);

    // Defensive NPE guard: a null CurrentStateOutput leaves the IS unchanged.
    if (currentStateOutput == null) {
      LOG.warn("CurrentStateOutput is null for resource {} in maintenance mode; "
          + "leaving IdealState unchanged.", resourceName);
      return currentIdealState;
    }

    // Index CurrentState by partition name for O(1) lookup in the unified loop below.
    Map<Partition, Map<String, String>> currentStateMap =
        currentStateOutput.getCurrentStateMap(resourceName);
    Map<String, Map<String, String>> currentStateByPartitionName = new HashMap<>();
    if (currentStateMap != null) {
      for (Map.Entry<Partition, Map<String, String>> entry : currentStateMap.entrySet()) {
        currentStateByPartitionName.put(entry.getKey().getPartitionName(), entry.getValue());
      }
    }

    if (currentStateByPartitionName.isEmpty()) {
      LOG.warn("No partition will be assigned for {} in maintenance mode "
          + "(no participant CurrentState reports for this resource).", resourceName);
    }

    StateModelDefinition stateModelDef =
        clusterData.getStateModelDef(currentIdealState.getStateModelDefRef());

    // Single per-partition rule for every partition the IdealState knows about:
    //   preferenceList = sorted(CurrentState hosts)   if CurrentState is non-empty
    //   preferenceList = []                            otherwise
    for (String partitionName : currentIdealState.getPartitionSet()) {
      Map<String, String> stateMap = currentStateByPartitionName.get(partitionName);

      if (stateMap == null || stateMap.isEmpty()) {
        // No participant reports state for this partition. Under MM we do not
        // bootstrap new replicas, so clear any planned-but-not-executed
        // listFields entry. The inherited mapping calculator will then produce
        // an empty BestPossibleStateMap for this partition.
        currentIdealState.setPreferenceList(partitionName, new ArrayList<>());
        continue;
      }

      List<String> preferenceList = new ArrayList<>(stateMap.keySet());

      /*
       * Sort 1: preserve the ordering of CurrentState hosts in the order of the
       * existing IS preferenceList. Example:
       *   IS preferenceList: [A, B, C]
       *   CurrentState:      { A:FOLLOWER, B:LEADER, C:FOLLOWER }
       *   newPrefList = new ArrayList<>(CS.keySet())  =>  arbitrary, e.g. [C, B, A]
       *   after Sort 1                               =>  [A, B, C]
       */
      Collections.sort(preferenceList, new PreferenceListNodeComparator(stateMap, stateModelDef,
          currentIdealState.getPreferenceList(partitionName), clusterData));

      /*
       * Sort 2: state priority. Top-state hosts (e.g., MASTER/LEADER) come first.
       *   [A, B, C]  =>  [B, A, C]   (B is MASTER per stateMap)
       */
      preferenceList.sort(new StatePriorityComparator(stateMap, stateModelDef));

      currentIdealState.setPreferenceList(partitionName, preferenceList);
    }

    LOG.info("End computing ideal state for resource {} in maintenance mode.", resourceName);
    return currentIdealState;
  }
}
