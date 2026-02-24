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

import java.util.Map;

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
  public static boolean isPartitionMissingTopState(String resource, String partition,
      String topState, CurrentStateOutput currentStateOutput) {
    Map<String, String> stateMap =
        currentStateOutput.getCurrentStateMap(resource, new Partition(partition));
    return stateMap == null || !stateMap.containsValue(topState);
  }
}
