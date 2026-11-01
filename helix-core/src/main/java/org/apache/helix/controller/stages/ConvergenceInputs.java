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
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.helix.controller.dataproviders.BaseControllerDataProvider;
import org.apache.helix.controller.rebalancer.util.DelayedRebalanceUtil;
import org.apache.helix.controller.rebalancer.util.WagedValidationUtil;
import org.apache.helix.controller.rebalancer.waged.WagedRebalanceStatus;
import org.apache.helix.controller.rebalancer.waged.WagedRebalancer;
import org.apache.helix.model.ConvergenceStatus.Reason;
import org.apache.helix.model.IdealState;
import org.apache.helix.model.Message;
import org.apache.helix.model.Resource;
import org.apache.helix.model.StateModelDefinition;

/**
 * Everything the calculation reads. The event outputs are read-only once the pipeline stage that
 * produced them is done; the controller cache is mutable, so {@link #from} copies what it needs.
 */
public final class ConvergenceInputs {
  String _clusterName;
  Map<String, Resource> _resources = Collections.emptyMap();
  CurrentStateOutput _currentState = new CurrentStateOutput();
  BestPossibleStateOutput _bestPossible;
  MessageOutput _allMessages;
  MessageOutput _selectedMessages;
  MessageOutput _throttledMessages;
  List<Message> _failedMessages = Collections.emptyList();
  Reason _pausedReason;
  Set<String> _liveInstances = Collections.emptySet();
  Map<String, String> _initialStates = Collections.emptyMap();
  Set<String> _wagedResources = Collections.emptySet();
  Set<String> _delayedResources = Collections.emptySet();
  WagedRebalanceStatus _wagedStatus;
  String _controllerName;

  public ConvergenceInputs controllerName(String controllerName) {
    _controllerName = controllerName;
    return this;
  }

  public ConvergenceInputs clusterName(String clusterName) {
    _clusterName = clusterName;
    return this;
  }

  public ConvergenceInputs resources(Map<String, Resource> resources) {
    _resources = resources;
    return this;
  }

  public ConvergenceInputs currentState(CurrentStateOutput currentState) {
    _currentState = currentState;
    return this;
  }

  public ConvergenceInputs bestPossible(BestPossibleStateOutput bestPossible) {
    _bestPossible = bestPossible;
    return this;
  }

  public ConvergenceInputs messages(MessageOutput all, MessageOutput selected,
      MessageOutput throttled) {
    _allMessages = all;
    _selectedMessages = selected;
    _throttledMessages = throttled;
    return this;
  }

  public ConvergenceInputs failedMessages(List<Message> failedMessages) {
    _failedMessages = failedMessages;
    return this;
  }

  /** When set, every partition is reported PAUSED with this reason and nothing is compared. */
  public ConvergenceInputs pausedReason(Reason reason) {
    _pausedReason = reason;
    return this;
  }

  public ConvergenceInputs liveInstances(Set<String> liveInstances) {
    _liveInstances = liveInstances;
    return this;
  }

  public ConvergenceInputs initialStates(Map<String, String> initialStates) {
    _initialStates = initialStates;
    return this;
  }

  public ConvergenceInputs wagedResources(Set<String> wagedResources,
      WagedRebalanceStatus wagedStatus) {
    _wagedResources = wagedResources;
    _wagedStatus = wagedStatus;
    return this;
  }

  public ConvergenceInputs delayedResources(Set<String> delayedResources) {
    _delayedResources = delayedResources;
    return this;
  }

  /** Snapshots the event on the pipeline thread: O(resources + live instances). */
  @SuppressWarnings("unchecked")
  public static ConvergenceInputs from(ClusterEvent event, BaseControllerDataProvider cache,
      String clusterName) {
    Map<String, Resource> resources =
        event.getAttribute(AttributeName.RESOURCES_TO_REBALANCE.name());
    Map<String, String> initialStates = new HashMap<>();
    Set<String> waged = new HashSet<>();
    Set<String> delayed = new HashSet<>();
    for (Map.Entry<String, Resource> entry : resources.entrySet()) {
      StateModelDefinition stateModel =
          cache.getStateModelDef(entry.getValue().getStateModelDefRef());
      if (stateModel != null) {
        initialStates.put(entry.getKey(), stateModel.getInitialState());
      }
      IdealState idealState = cache.getIdealState(entry.getKey());
      if (idealState != null && WagedValidationUtil.isWagedEnabled(idealState)) {
        waged.add(entry.getKey());
      }
      if (idealState != null && DelayedRebalanceUtil.isDelayRebalanceEnabled(idealState,
          cache.getClusterConfig())) {
        delayed.add(entry.getKey());
      }
    }
    Object rebalancer = event.getAttribute(AttributeName.STATEFUL_REBALANCER.name());
    WagedRebalanceStatus wagedStatus = rebalancer instanceof WagedRebalancer
        ? ((WagedRebalancer) rebalancer).getConvergenceStatus() : null;
    CurrentStateOutput currentState = event.getAttribute(AttributeName.CURRENT_STATE.name());
    List<Message> failed = event.getAttribute(AttributeName.MESSAGES_DISPATCH_FAILED.name());
    return new ConvergenceInputs().clusterName(clusterName).resources(resources)
        .currentState(currentState == null ? new CurrentStateOutput() : currentState)
        .bestPossible(event.getAttribute(AttributeName.BEST_POSSIBLE_STATE.name()))
        .messages(event.getAttribute(AttributeName.MESSAGES_ALL.name()),
            event.getAttribute(AttributeName.MESSAGES_SELECTED_SNAPSHOT.name()),
            event.getAttribute(AttributeName.MESSAGES_THROTTLE.name()))
        .failedMessages(failed == null ? Collections.emptyList() : failed)
        .pausedReason(cache.isMaintenanceModeEnabled() ? Reason.MAINTENANCE_MODE : null)
        .liveInstances(new HashSet<>(cache.getLiveInstances().keySet()))
        .initialStates(initialStates).wagedResources(waged, wagedStatus)
        .delayedResources(delayed);
  }
}
