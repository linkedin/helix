package org.apache.helix.controller.stages;

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

import java.util.HashMap;
import java.util.Map;

import org.apache.helix.model.Message;
import org.apache.helix.model.Partition;
import org.apache.helix.model.StateModelDefinition;

/**
 * Utility class for common state transition logic used across the pipeline stages.
 */
public class StateTransitionHelper {

  /**
   * Checks if a state transition is upward (moving to higher priority state).
   * In state priority maps, lower numeric values indicate higher priority states.
   */
  public static boolean isUpwardTransition(String from, String to, StateModelDefinition def) {
    if (def == null) {
      return false;
    }
    Map<String, Integer> priority = def.getStatePriorityMap();
    return priority.containsKey(from) && priority.containsKey(to)
        && priority.get(from) > priority.get(to);
  }

  /**
   * Checks if a state transition is downward (moving to lower priority state).
   * In state priority maps, lower numeric values indicate higher priority states.
   */
  public static boolean isDownwardTransition(String from, String to, StateModelDefinition def) {
    if (def == null) {
      return false;
    }
    Map<String, Integer> priority = def.getStatePriorityMap();
    return priority.containsKey(from) && priority.containsKey(to)
        && priority.get(from) < priority.get(to);
  }

  /**
   * Checks if a transition represents a top state handoff (moving from top state to a lower state).
   */
  public static boolean isTopStateHandoff(String from, String to, String topState,
      StateModelDefinition def) {
    if (!from.equals(topState)) {
      return false;
    }
    return isDownwardTransition(from, to, def);
  }

  /**
   * Checks if a partition is missing the top state replica in its current state.
   */
  public static boolean isPartitionMissingTopState(String resource, Partition partition,
      String topState, CurrentStateOutput currentStateOutput) {
    Map<String, String> stateMap = currentStateOutput.getCurrentStateMap(resource, partition);
    return stateMap == null || !stateMap.containsValue(topState);
  }

  /**
   * Overload that accepts a partition name string; wraps it in a {@link Partition} and delegates.
   * Prefer the {@link Partition}-typed overload when a {@code Partition} instance is already available.
   */
  public static boolean isPartitionMissingTopState(String resource, String partition,
      String topState, CurrentStateOutput currentStateOutput) {
    return isPartitionMissingTopState(resource, new Partition(partition), topState,
        currentStateOutput);
  }

  /**
   * Counts how many replicas are currently in a state that is needed by the best-possible state,
   * respecting multiplicity (e.g., if best-possible requires two SLAVE replicas, only two
   * current SLAVE replicas count even if more exist).
   *
   * @param bestPossible map of instance → target state from the best-possible output
   * @param currentState map of instance → current state
   * @return number of replicas whose current state satisfies the best-possible requirement
   */
  public static int countActiveReplicas(Map<String, String> bestPossible,
      Map<String, String> currentState) {
    Map<String, Integer> stateCount = new HashMap<>();
    for (String state : bestPossible.values()) {
      stateCount.merge(state, 1, Integer::sum);
    }
    int count = 0;
    for (String state : currentState.values()) {
      if (stateCount.getOrDefault(state, 0) > 0) {
        count++;
        stateCount.put(state, stateCount.get(state) - 1);
      }
    }
    return count;
  }

  /**
   * Counts how many instance→state assignments in the current state exactly match
   * the best-possible state for the same instance.
   *
   * @param bestPossible map of instance → target state from the best-possible output
   * @param currentState map of instance → current state
   * @return number of instances whose current state exactly matches the best-possible assignment
   */
  public static int countIdealMatches(Map<String, String> bestPossible,
      Map<String, String> currentState) {
    int matches = 0;
    for (Map.Entry<String, String> entry : bestPossible.entrySet()) {
      if (entry.getValue().equals(currentState.get(entry.getKey()))) {
        matches++;
      }
    }
    return matches;
  }

  /**
   * Determines whether a state-transition message should be reclassified from
   * {@link RebalanceType#LOAD_BALANCE} to {@link RebalanceType#RECOVERY_BALANCE}.
   *
   * <p>Normally, a top-state downward transition (e.g., MASTER-&gt;SLAVE or LEADER-&gt;STANDBY)
   * is classified as {@code LOAD_BALANCE} by the standard rebalance-type logic because the
   * second-top-state count already satisfies {@code minActiveReplicas}. This means the transition
   * shares throttle quota with regular load-balance work, potentially delaying urgent leadership
   * handoffs when load-balance throttles are saturated.</p>
   *
   * <p>When the cluster config flag
   * {@code ENABLE_RECOVERY_REBALANCE_FOR_TOPSTATE_DOWNWARD_TRANSITION} is enabled, this method
   * returns {@code true} for any message whose {@code fromState} is the top state and whose
   * transition is downward, so callers can reclassify it as {@code RECOVERY_BALANCE} and give
   * it higher throttle priority.</p>
   *
   * @param configEnabled whether the cluster config flag is enabled
   * @param message       the state-transition message to evaluate
   * @param stateModelDef the state model definition for the resource (may be {@code null})
   * @return {@code true} if the message should be reclassified as recovery rebalance
   */
  public static boolean shouldReclassifyForTopStateHandOff(boolean configEnabled, Message message,
      StateModelDefinition stateModelDef) {
    return configEnabled && stateModelDef != null && isTopStateHandoff(message.getFromState(),
        message.getToState(), stateModelDef.getTopState(), stateModelDef);
  }
}
