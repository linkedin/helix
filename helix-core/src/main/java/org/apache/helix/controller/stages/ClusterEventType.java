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

import java.util.Collections;
import java.util.EnumSet;
import java.util.Set;

public enum ClusterEventType {
  IdealStateChange,
  CurrentStateChange,
  TaskCurrentStateChange,
  CustomizedStateChange,
  ConfigChange,
  ClusterConfigChange,
  ResourceConfigChange,
  InstanceConfigChange,
  CustomizeStateConfigChange,
  LiveInstanceChange,
  MessageChange,
  ExternalViewChange,
  CustomizedViewChange,
  TargetExternalViewChange,
  Resume,
  PeriodicalRebalance,
  OnDemandRebalance,
  ControllerChange,
  RetryRebalance,
  StateVerifier,
  Unknown;

  // Subset of event types that change the cluster's logical topology -- the inputs
  // that the rebalancer's placement decisions actually depend on. Kept in lockstep
  // with the HelixConstants.ChangeType values recognized by
  // ResourceChangeDetector.determinePropertyMapByType, so the metric counts the same
  // events the rebalancer treats as topology-affecting.
  private static final Set<ClusterEventType> TOPOLOGY_CHANGE_EVENT_TYPES =
      Collections.unmodifiableSet(EnumSet.of(
          IdealStateChange,
          InstanceConfigChange,
          ResourceConfigChange,
          LiveInstanceChange,
          ClusterConfigChange));

  /**
   * @return true iff this event type represents a topology change (config / instance / resource).
   */
  public boolean isTopologyChange() {
    return TOPOLOGY_CHANGE_EVENT_TYPES.contains(this);
  }

  /**
   * @return the set of event types classified as topology changes.
   */
  public static Set<ClusterEventType> topologyChangeEventTypes() {
    return TOPOLOGY_CHANGE_EVENT_TYPES;
  }
}
