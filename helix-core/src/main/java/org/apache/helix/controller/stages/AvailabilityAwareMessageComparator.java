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
import org.apache.helix.controller.LogUtil;
import org.apache.helix.controller.dataproviders.ResourceControllerDataProvider;
import org.apache.helix.model.IdealState;
import org.apache.helix.model.Message;
import org.apache.helix.model.Partition;
import org.apache.helix.model.StateModelDefinition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Comparator for cross-resource availability-aware message prioritization.
 * This comparator sorts messages based on their availability impact score,
 * ensuring that partitions with fewer active replicas get prioritized over
 * partitions that are closer to their target replica count.
 *
 * <p>Prioritization order:
 * <ol>
 *   <li>Messages for partitions missing top state (highest priority)</li>
 *   <li>Top state handoff downward transitions (MASTER → SLAVE) - high priority to prevent starvation</li>
 *   <li>Higher availability impact score (calculated based on minActiveReplicas/effectiveReplicaCount)</li>
 *   <li>Deterministic ordering (resource name, partition name) for consistent behavior</li>
 * </ol>
 */
public class AvailabilityAwareMessageComparator implements Comparator<Message> {
  private static final Logger LOGGER =
      LoggerFactory.getLogger(AvailabilityAwareMessageComparator.class);

  // Impact score sentinel values
  private static final double TOP_STATE_MISSING_IMPACT = Double.MAX_VALUE;
  private static final double TOP_STATE_HANDOFF_IMPACT = Double.MAX_VALUE - 1;

  private final ResourceControllerDataProvider _cache;
  private final CurrentStateOutput _currentStateOutput;

  // Cached values to avoid recomputation
  private final Map<String, Double> _impactCache = new HashMap<>();
  private final Map<String, Integer> _activeReplicasCache = new HashMap<>();
  private final Map<String, Integer> _pendingMessageCountCache = new HashMap<>();

  // Tracks message order per partition to break ties
  private final Map<String, Integer> _messageIndexTracker = new HashMap<>();

  private String _eventId;


  public AvailabilityAwareMessageComparator(
      ResourceControllerDataProvider cache, CurrentStateOutput currentStateOutput) {
    _cache = cache;
    _currentStateOutput = currentStateOutput;
  }

  /**
   * Set the event ID for logging correlation.
   * @param eventId the event ID from the controller pipeline
   */
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

    // Deterministic fallback
    int resourceCompare = m1.getResourceName().compareTo(m2.getResourceName());
    if (resourceCompare != 0) {
      return resourceCompare;
    }

    return m1.getPartitionName().compareTo(m2.getPartitionName());
  }

  /**
   * Calculate the availability impact score for a message.
   *
   * <p>Impact calculation:
   * <ul>
   *   <li>If partition is missing top state: MAX_VALUE (highest priority)</li>
   *   <li>If message is top state handoff downward (MASTER → SLAVE): MAX_VALUE - 1</li>
   *   <li>If upward transition: effectiveMinActive / (effectiveReplicaCount + 1)</li>
   *   <li>If downward transition (not top state): 0 (lowest priority)</li>
   * </ul>
   *
   * @param message the message to calculate impact for
   * @return the availability impact score
   */
  private double computeAvailabilityImpact(Message message) {
    String key = cacheKey(message);
    if (_impactCache.containsKey(key)) {
      return _impactCache.get(key);
    }

    IdealState idealState = _cache.getIdealState(message.getResourceName());
    if (idealState == null) {
      return cacheImpact(key, 0.0, message, "NO_IDEAL_STATE");
    }

    StateModelDefinition stateModelDef =
        _cache.getStateModelDef(idealState.getStateModelDefRef());
    if (stateModelDef == null) {
      return cacheImpact(key, 0.0, message, "NO_STATE_MODEL");
    }

    String topState = stateModelDef.getTopState();
    boolean missingTopState =
        isPartitionMissingTopState(message.getResourceName(), message.getPartitionName(), topState);

    // Highest priority: missing top state *and* transitioning to top
    if (missingTopState && message.getToState().equals(topState)) {
      return cacheImpact(key, TOP_STATE_MISSING_IMPACT, message, "TOP_STATE_MISSING_TO_TOP");
    }

    // Next: downward handoff
    if (isTopStateHandoff(message.getFromState(), message.getToState(), topState, stateModelDef)) {
      return cacheImpact(key, TOP_STATE_HANDOFF_IMPACT, message, "TOP_STATE_HANDOFF");
    }

    // If not upward transition, lowest
    if (!isUpwardTransition(message.getFromState(), message.getToState(), stateModelDef)) {
      return cacheImpact(key, 0.0, message, "DOWNWARD_TRANSITION");
    }

    // Upward transition: compute normalized impact
    double impact = computeUpwardImpact(message, idealState);
    return cacheImpact(key, impact, message, missingTopState ? "UPWARD_MISSING_TOP" : "UPWARD_REGULAR");
  }

  private double computeUpwardImpact(Message message, IdealState idealState) {
    String resource = message.getResourceName();
    String partition = message.getPartitionName();
    String partitionKey = resource + ":" + partition;

    int minActive = idealState.getMinActiveReplicas();
    int targetReplicas = getTargetReplicas(resource, partition, idealState);

    int effectiveMinActive = (minActive <= 0) ? targetReplicas : minActive;

    int currentActive = getCurrentActiveReplicas(resource, partition);
    int pending = getPendingMessages(resource, partition);
    int index = _messageIndexTracker.getOrDefault(partitionKey, 0);
    _messageIndexTracker.put(partitionKey, index + 1);

    int effectiveCount = currentActive + pending + index;
    double impact = (double) effectiveMinActive / (effectiveCount + 1);

    logDebug(message, "UPWARD_CALC", impact,
        String.format("minActive=%d target=%d currentActive=%d pending=%d idx=%d",
            minActive, targetReplicas, currentActive, pending, index));
    return impact;
  }

  private int getTargetReplicas(String resource, String partition, IdealState idealState) {
    Map<String, String> instanceStateMap = idealState.getInstanceStateMap(partition);
    if (instanceStateMap != null && !instanceStateMap.isEmpty()) {
      return instanceStateMap.size();
    }
    return Math.max(idealState.getReplicaCount(_cache.getEnabledLiveInstances().size()), 1);
  }

  private int getCurrentActiveReplicas(String resource, String partition) {
    String key = resource + ":" + partition;
    if (_activeReplicasCache.containsKey(key)) {
      return _activeReplicasCache.get(key);
    }

    Map<String, String> currentStates =
        _currentStateOutput.getCurrentStateMap(resource, new Partition(partition));
    int count = (currentStates == null) ? 0 :
        (int) currentStates.values().stream().filter(this::isActiveState).count();
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
              .filter(msg -> isUpwardTransition(msg.getFromState(), msg.getToState(), stateModelDef))
              .count();
        }
      }
    }

    _pendingMessageCountCache.put(key, count);
    return count;
  }

  // ---------------------------------------------------
  // State transition helpers
  // ---------------------------------------------------

  private boolean isUpwardTransition(String from, String to, StateModelDefinition def) {
    Map<String, Integer> priority = def.getStatePriorityMap();
    return priority.containsKey(from) && priority.containsKey(to)
        && priority.get(from) > priority.get(to);
  }

  private boolean isTopStateHandoff(String from, String to, String topState,
      StateModelDefinition def) {
    if (!from.equals(topState)) {
      return false;
    }
    Map<String, Integer> priority = def.getStatePriorityMap();
    return priority.containsKey(from) && priority.containsKey(to)
        && priority.get(from) < priority.get(to);
  }

  private boolean isPartitionMissingTopState(
      String resource, String partition, String topState) {
    Map<String, String> stateMap =
        _currentStateOutput.getCurrentStateMap(resource, new Partition(partition));
    return stateMap == null || !stateMap.containsValue(topState);
  }

  private boolean isActiveState(String state) {
    return state != null
        && !state.isEmpty()
        && !state.equalsIgnoreCase(HelixDefinedState.ERROR.name())
        && !state.equalsIgnoreCase(HelixDefinedState.DROPPED.name())
        && !state.equalsIgnoreCase("OFFLINE");
  }

  private void logDebug(Message msg, String reason, double impact, String detail) {
      LogUtil.logDebug(LOGGER, _eventId,
          String.format("[%s] Resource=%s Partition=%s %s->%s Impact=%.4f %s",
              reason,
              msg.getResourceName(),
              msg.getPartitionName(),
              msg.getFromState(),
              msg.getToState(),
              impact,
              detail == null ? "" : detail));
  }

  private double cacheImpact(String key, double impact, Message message, String label) {
    _impactCache.put(key, impact);
    logDebug(message, label, impact, null);
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

  public double getAvailabilityImpact(Message message) {
    return computeAvailabilityImpact(message);
  }

  public void resetMessageIndexTracker() {
    _messageIndexTracker.clear();
  }
}
