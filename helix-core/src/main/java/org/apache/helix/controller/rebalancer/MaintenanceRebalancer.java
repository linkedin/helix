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
   * Under maintenance mode the cluster is meant to be frozen: no new replicas should be
   * bootstrapped. For every partition of the resource, the rebalancer sets the
   * preferenceList to the participant-reported CurrentState hosts (or empty if no
   * participant reports state for that partition).
   *
   * <p>For partitions with CurrentState reports, the preferenceList is rebuilt from the
   * reported hosts, ordered so that any top-state host (as defined by the resource's
   * state model) is listed first. No host that has the partition in CurrentState is
   * dropped from the preferenceList. For partitions without any CurrentState report,
   * the preferenceList is set to empty so the inherited mapping calculator produces
   * no BestPossibleState for the partition and the pipeline emits no state-transition
   * messages for it; this is what enforces "no new bootstrap under MM" regardless of
   * which upstream rebalancer wrote the original listFields entry or what state model
   * the resource uses.
   */
  @Override
  public IdealState computeNewIdealState(String resourceName, IdealState currentIdealState,
      CurrentStateOutput currentStateOutput, ResourceControllerDataProvider clusterData) {
    LOG.info("Start computing ideal state for resource {} in maintenance mode.", resourceName);

    // CurrentStateOutput returns Map<Partition, Map<host, state>> keyed by Partition
    // objects, but the loop below iterates partition names from
    // currentIdealState.getPartitionSet() (Set<String>). Build a name-indexed view
    // of CurrentState up front so the per-partition lookup inside the loop is a
    // constant-time HashMap.get() rather than a linear scan over Partition keys.
    //
    // A null currentStateMap (the resource has no CurrentState entries at all,
    // e.g., the resource has never been touched by any participant) leaves
    // currentStateByPartitionName empty. The loop below then treats every
    // partition as a "no CS" case and clears each preferenceList.
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

    // Invariant for every partition in the resource under maintenance mode:
    //
    //   preferenceList = sorted(participant CurrentState hosts for this partition)
    //
    // Example resource with two partitions, P0 and P1:
    //   IS.listFields before: P0 -> [hostA, hostB]
    //                         P1 -> [hostC, hostD]
    //   CurrentState:         P0 -> { hostA:LEADER, hostB:FOLLOWER }
    //                         P1 -> (no participant reports any state)
    //   IS.listFields after:  P0 -> [hostA, hostB]   (rebuilt from CS, LEADER first)
    //                         P1 -> []                (cleared because no CS report)
    //
    // Two consequences fall out of this single rule:
    //
    // 1. Partitions with at least one participant CurrentState report keep their
    //    placement: the preferenceList is rebuilt from the CS hosts and sorted so
    //    any top-state host (per the resource's state model) appears first. No host
    //    that has the partition in CurrentState is evicted by this rebalancer.
    //
    //    Example:
    //      IS preferenceList: [hostA, hostB]
    //      CurrentState:      { hostA:FOLLOWER, hostB:LEADER }
    //      -->  preferenceList after = [hostB, hostA]   (LEADER promoted to head)
    //
    // 2. Partitions without any participant CurrentState report get an empty
    //    preferenceList. With an empty preferenceList the inherited mapping
    //    calculator produces no BestPossibleState entry for the partition, so the
    //    downstream pipeline emits no state-transition messages and no replica is
    //    bootstrapped. This is what enforces the maintenance-mode contract of "no
    //    new bootstrap" for partitions whose listFields may have been written
    //    speculatively (e.g., a planned target for an in-flight move that had not
    //    yet been realized when MM activated).
    //
    //    Example:
    //      IS preferenceList: [hostX, hostY]
    //      CurrentState:      (no entries for this partition)
    //      -->  preferenceList after = []
    //
    // Iteration uses currentIdealState.getPartitionSet() so partitions that exist
    // only as listFields entries -- the dangerous case under MM -- are also visited.
    for (String partitionName : currentIdealState.getPartitionSet()) {
      Map<String, String> stateMap = currentStateByPartitionName.get(partitionName);

      if (stateMap == null || stateMap.isEmpty()) {
        // No participant CurrentState for this partition -> clear the
        // preferenceList. setPreferenceList(name, new ArrayList<>()) replaces the
        // list rather than mutating it in place, which avoids
        // UnsupportedOperationException if the stored list happens to be
        // immutable (e.g., set via Arrays.asList or Collections.emptyList).
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
