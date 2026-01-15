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

import org.apache.helix.controller.dataproviders.ResourceControllerDataProvider;
import org.apache.helix.model.Resource;


/**
 * Strategy interface for computing intermediate partition states.
 *
 * <p>Implementations of this interface define how state transitions are prioritized
 * and throttled when computing the intermediate state that serves as the bridge
 * between the current state and the best possible state.
 *
 * <p>Current implementations:
 * <ul>
 *   <li>{@link ResourcePriorityIntermediateStateCalculator} - Traditional resource-priority-based
 *       computation where resources are processed in priority order</li>
 *   <li>{@link AvailabilityAwareIntermediateStateCalculator} - Cross-resource availability-aware
 *       computation that prioritizes messages based on availability impact</li>
 * </ul>
 */
public interface IntermediateStateComputationStrategy {

  /**
   * Compute the intermediate state output for the given resources.
   *
   * <p>This method processes the messages generated for state transitions and applies
   * throttling constraints to produce an intermediate state that can be safely
   * transitioned to from the current state.
   *
   * @param event the cluster event containing pipeline context
   * @param resourceMap map of resource name to resource object for resources to rebalance
   * @param currentStateOutput the current state of all partitions
   * @param bestPossibleStateOutput the target best possible state
   * @param messageOutput the messages selected for state transitions
   * @param dataCache the cached cluster data provider
   * @return the computed intermediate state output
   */
  IntermediateStateOutput compute(
      ClusterEvent event,
      Map<String, Resource> resourceMap,
      CurrentStateOutput currentStateOutput,
      BestPossibleStateOutput bestPossibleStateOutput,
      MessageOutput messageOutput,
      ResourceControllerDataProvider dataCache);
}

