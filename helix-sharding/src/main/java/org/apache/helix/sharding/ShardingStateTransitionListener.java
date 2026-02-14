package org.apache.helix.sharding;

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


/**
 * Functional interface for receiving partition state transition callbacks.
 *
 * <p>This is the simplified API that replaces Tendril's {@code ComputeStateMachine}.
 * When a partition transitions between states (e.g., OFFLINE → STANDBY → LEADER),
 * this listener is invoked with the partition name and the old/new state.</p>
 *
 * <h3>Usage:</h3>
 * <pre>{@code
 * node.onStateTransition((partition, fromState, toState) -> {
 *     if ("LEADER".equals(toState)) {
 *         startServingPartition(partition);
 *     } else if ("LEADER".equals(fromState)) {
 *         stopServingPartition(partition);
 *     }
 * });
 * }</pre>
 */
@FunctionalInterface
public interface ShardingStateTransitionListener {

  /**
   * Called when a partition transitions to a new state.
   *
   * @param partitionName The name of the partition (e.g., "myCluster_42").
   * @param fromState     The previous state (e.g., "OFFLINE", "STANDBY", "LEADER").
   * @param toState       The new state.
   */
  void onStateTransition(String partitionName, String fromState, String toState);
}
