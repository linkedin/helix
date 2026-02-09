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

import java.util.Comparator;
import java.util.HashMap;
import java.util.Map;

import org.apache.helix.HelixDefinedState;
import org.apache.helix.controller.dataproviders.ResourceControllerDataProvider;
import org.apache.helix.model.IdealState;
import org.apache.helix.model.Message;
import org.apache.helix.model.Partition;
import org.apache.helix.model.StateModelDefinition;

/**
 * Comparator for cross-resource availability-aware message prioritization.
 * Sorts messages based on their availability impact score to prioritize partitions
 * with fewer active replicas over those closer to their target replica count.
 */
public class AvailabilityAwareMessageComparator implements Comparator<Message> {
  private static final double TOP_STATE_MISSING_IMPACT = Double.MAX_VALUE;
  private static final double TOP_STATE_HANDOFF_IMPACT = Double.MAX_VALUE - 1;

  private final ResourceControllerDataProvider _cache;
  private final CurrentStateOutput _currentStateOutput;
  private final Map<String, Double> _impactCache = new HashMap<>();
  private final Map<String, Integer> _activeReplicasCache = new HashMap<>();
  private final Map<String, Integer> _pendingMessageCountCache = new HashMap<>();
  private final Map<String, Integer> _messageIndexTracker = new HashMap<>();
  private String _eventId;

  public AvailabilityAwareMessageComparator(ResourceControllerDataProvider cache, CurrentStateOutput currentStateOutput) {
    _cache = cache;
    _currentStateOutput = currentStateOutput;
  }

  public void setEventId(String eventId) {
    _eventId = eventId;
  }

  @Override
  public int compare(Message m1, Message m2) {
    double score1 = computeAvailabilityImpact(m1);
    double score2 = computeAvailabilityImpact(m2);
    int delta = Double.compare(score2, score1);
    if (delta != 0) {
      return delta;
    }
    int resourceCompare = m1.getResourceName().compareTo(m2.getResourceName());
    if (resourceCompare != 0) {
      return resourceCompare;
    }
    return m1.getPartitionName().compareTo(m2.getPartitionName());
  }

  private double computeAvailabilityImpact(Message message) {
    String key = cacheKey(message);
    if (_impactCache.containsKey(key)) {
      return _impactCache.get(key);
    }

    IdealState idealState = _cache.getIdealState(message.getResourceName());
    if (idealState == null) {
      return cacheImpact(key, 0.0);
    }

    StateModelDefinition stateModelDef = _cache.getStateModelDef(idealState.getStateModelDefRef());
    if (stateModelDef == null) {
      return cacheImpact(key, 0.0);
    }

    String topState = stateModelDef.getTopState();
    boolean missingTopState = StateTransitionHelper.isPartitionMissingTopState(
        message.getResourceName(), message.getPartitionName(), topState, _currentStateOutput);

    if (missingTopState && message.getToState().equals(topState)) {
      return cacheImpact(key, TOP_STATE_MISSING_IMPACT);
    }

    if (StateTransitionHelper.isTopStateHandoff(message.getFromState(), message.getToState(), topState, stateModelDef)) {
      return cacheImpact(key, TOP_STATE_HANDOFF_IMPACT);
    }

    if (!StateTransitionHelper.isUpwardTransition(message.getFromState(), message.getToState(), stateModelDef)) {
      return cacheImpact(key, 0.0);
    }

    double impact = computeUpwardImpact(message, idealState);
    return cacheImpact(key, impact);
  }

  private double computeUpwardImpact(Message message, IdealState idealState) {
    String resource = message.getResourceName();
    String partition = message.getPartitionName();
    String partitionKey = resource + ":" + partition;

    int minActive = idealState.getMinActiveReplicas();

    int currentActive = getCurrentActiveReplicas(resource, partition);
    int pending = getPendingMessages(resource, partition);
    int index = _messageIndexTracker.getOrDefault(partitionKey, 0);
    _messageIndexTracker.put(partitionKey, index + 1);

    int effectiveCount = currentActive + pending + index;

    // When minActiveReplicas is not configured, use a base impact of 1.0 scaled
    // by current active count. 
    if (minActive == 0) {
      return 1.0 / (effectiveCount + 1);
    }

    return (double) minActive / (effectiveCount + 1);
  }

  private int getCurrentActiveReplicas(String resource, String partition) {
    String key = resource + ":" + partition;
    if (_activeReplicasCache.containsKey(key)) {
      return _activeReplicasCache.get(key);
    }

    Map<String, String> currentStates =
        _currentStateOutput.getCurrentStateMap(resource, new Partition(partition));
    int count = (currentStates == null) ? 0 :
        (int) currentStates.values().stream().filter(StateTransitionHelper::isActiveState).count();
    _activeReplicasCache.put(key, count);
    return count;
  }

  private int getPendingMessages(String resource, String partition) {
    String key = resource + ":" + partition;
    if (_pendingMessageCountCache.containsKey(key)) {
      return _pendingMessageCountCache.get(key);
    }

    int count = 0;
    Map<String, Message> pendingMsgs =
        _currentStateOutput.getPendingMessageMap(resource, new Partition(partition));

    if (pendingMsgs != null && !pendingMsgs.isEmpty()) {
      IdealState idealState = _cache.getIdealState(resource);
      if (idealState != null) {
        StateModelDefinition stateModelDef = _cache.getStateModelDef(idealState.getStateModelDefRef());
        if (stateModelDef != null) {
          count = (int) pendingMsgs.values().stream()
              .filter(msg -> StateTransitionHelper.isUpwardTransition(msg.getFromState(), msg.getToState(), stateModelDef))
              .count();
        }
      }
    }

    _pendingMessageCountCache.put(key, count);
    return count;
  }

  private double cacheImpact(String key, double impact) {
    _impactCache.put(key, impact);
    return impact;
  }

  private String cacheKey(Message message) {
    return String.join(":",
        message.getResourceName(),
        message.getPartitionName(),
        message.getFromState(),
        message.getToState(),
        message.getTgtName());
  }

}
