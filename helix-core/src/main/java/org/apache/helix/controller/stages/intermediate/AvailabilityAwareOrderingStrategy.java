package org.apache.helix.controller.stages.intermediate;

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
import java.util.List;
import java.util.Map;
import org.apache.helix.controller.dataproviders.ResourceControllerDataProvider;
import org.apache.helix.controller.stages.CurrentStateOutput;
import org.apache.helix.controller.stages.StateTransitionHelper;
import org.apache.helix.model.IdealState;
import org.apache.helix.model.Message;
import org.apache.helix.model.Partition;
import org.apache.helix.model.StateModelDefinition;

/**
 * Message ordering strategy that prioritizes messages based on their availability impact.
 * Sorts messages cross-resource based on availability scores to prioritize partitions
 * with fewer active replicas over those closer to their target replica count.
 */
public class AvailabilityAwareOrderingStrategy implements MessageOrderingStrategy {
  private static final double TOP_STATE_MISSING_IMPACT = Double.MAX_VALUE;
  private static final double TOP_STATE_HANDOFF_IMPACT = Double.MAX_VALUE - 1;

  private final ResourceControllerDataProvider cache;
  private final CurrentStateOutput currentStateOutput;
  private String eventId;

  public AvailabilityAwareOrderingStrategy(ResourceControllerDataProvider cache,
      CurrentStateOutput currentStateOutput) {
    this.cache = cache;
    this.currentStateOutput = currentStateOutput;
  }

  public void setEventId(String eventId) {
    this.eventId = eventId;
  }

  @Override
  public void sortMessages(List<MessageContext> messages) {
    // Create fresh caches for this sort operation
    Map<String, Double> impactCache = new HashMap<>();
    Map<String, Integer> activeReplicasCache = new HashMap<>();
    Map<String, Integer> pendingMessageCountCache = new HashMap<>();
    Map<String, Integer> messageIndexTracker = new HashMap<>();

    messages.sort((m1, m2) -> {
      double score1 = computeAvailabilityImpact(m1.message, impactCache, activeReplicasCache,
          pendingMessageCountCache, messageIndexTracker);
      double score2 = computeAvailabilityImpact(m2.message, impactCache, activeReplicasCache,
          pendingMessageCountCache, messageIndexTracker);

      // Higher score = higher priority (reverse order)
      int delta = Double.compare(score2, score1);
      if (delta != 0) {
        return delta;
      }

      // Stable tiebreaking: resource name, then partition name
      int resourceCompare = m1.resourceName.compareTo(m2.resourceName);
      if (resourceCompare != 0) {
        return resourceCompare;
      }
      return m1.partition.getPartitionName().compareTo(m2.partition.getPartitionName());
    });
  }

  /**
   * Compute availability impact score for a message.
   * Higher score = higher priority (more urgent for availability).
   */
  private double computeAvailabilityImpact(Message message,
      Map<String, Double> impactCache,
      Map<String, Integer> activeReplicasCache,
      Map<String, Integer> pendingMessageCountCache,
      Map<String, Integer> messageIndexTracker) {

    String key = cacheKey(message);
    if (impactCache.containsKey(key)) {
      return impactCache.get(key);
    }

    IdealState idealState = cache.getIdealState(message.getResourceName());
    if (idealState == null) {
      return cacheImpact(key, 0.0, impactCache);
    }

    StateModelDefinition stateModelDef = cache.getStateModelDef(idealState.getStateModelDefRef());
    if (stateModelDef == null) {
      return cacheImpact(key, 0.0, impactCache);
    }

    String topState = stateModelDef.getTopState();

    // Highest priority: Missing top state replica (partition unavailable)
    boolean missingTopState = StateTransitionHelper.isPartitionMissingTopState(
        message.getResourceName(), message.getPartitionName(), topState, currentStateOutput);
    if (missingTopState && message.getToState().equals(topState)) {
      return cacheImpact(key, TOP_STATE_MISSING_IMPACT, impactCache);
    }

    // Second highest priority: Top state handoff (minimize unavailability window)
    if (StateTransitionHelper.isTopStateHandoff(
        message.getFromState(), message.getToState(), topState, stateModelDef)) {
      return cacheImpact(key, TOP_STATE_HANDOFF_IMPACT, impactCache);
    }

    // Only prioritize upward transitions (bringing replicas online)
    if (!StateTransitionHelper.isUpwardTransition(
        message.getFromState(), message.getToState(), stateModelDef)) {
      return cacheImpact(key, 0.0, impactCache);
    }

    // Compute impact based on current active replicas and min active threshold
    double impact = computeUpwardImpact(message, idealState, activeReplicasCache,
        pendingMessageCountCache, messageIndexTracker);
    return cacheImpact(key, impact, impactCache);
  }

  /**
   * Compute impact score for upward transitions.
   * Score is higher when current active count is below min active replicas.
   */
  private double computeUpwardImpact(Message message, IdealState idealState,
      Map<String, Integer> activeReplicasCache,
      Map<String, Integer> pendingMessageCountCache,
      Map<String, Integer> messageIndexTracker) {

    String resource = message.getResourceName();
    String partition = message.getPartitionName();
    String partitionKey = resource + ":" + partition;

    int minActive = idealState.getMinActiveReplicas();
    int currentActive = getCurrentActiveReplicas(resource, partition, activeReplicasCache);
    int pending = getPendingMessages(resource, partition, pendingMessageCountCache);

    // Track message index for this partition to account for multiple messages
    int index = messageIndexTracker.getOrDefault(partitionKey, 0);
    messageIndexTracker.put(partitionKey, index + 1);

    int effectiveCount = currentActive + pending + index;

    // When minActiveReplicas is not configured, use base impact scaled by current active count
    if (minActive == 0) {
      return 1.0 / (effectiveCount + 1);
    }

    // Impact is inversely proportional to how close we are to min active threshold
    return (double) minActive / (effectiveCount + 1);
  }

  /**
   * Count current active replicas for a partition.
   */
  private int getCurrentActiveReplicas(String resource, String partition,
      Map<String, Integer> activeReplicasCache) {

    String key = resource + ":" + partition;
    if (activeReplicasCache.containsKey(key)) {
      return activeReplicasCache.get(key);
    }

    Map<String, String> currentStates =
        currentStateOutput.getCurrentStateMap(resource, new Partition(partition));
    int count = (currentStates == null) ? 0 :
        (int) currentStates.values().stream()
            .filter(StateTransitionHelper::isActiveState)
            .count();

    activeReplicasCache.put(key, count);
    return count;
  }

  /**
   * Count pending upward transition messages for a partition.
   */
  private int getPendingMessages(String resource, String partition,
      Map<String, Integer> pendingMessageCountCache) {

    String key = resource + ":" + partition;
    if (pendingMessageCountCache.containsKey(key)) {
      return pendingMessageCountCache.get(key);
    }

    int count = 0;
    Map<String, Message> pendingMsgs =
        currentStateOutput.getPendingMessageMap(resource, new Partition(partition));

    if (pendingMsgs != null && !pendingMsgs.isEmpty()) {
      IdealState idealState = cache.getIdealState(resource);
      if (idealState != null) {
        StateModelDefinition stateModelDef =
            cache.getStateModelDef(idealState.getStateModelDefRef());
        if (stateModelDef != null) {
          count = (int) pendingMsgs.values().stream()
              .filter(msg -> StateTransitionHelper.isUpwardTransition(
                  msg.getFromState(), msg.getToState(), stateModelDef))
              .count();
        }
      }
    }

    pendingMessageCountCache.put(key, count);
    return count;
  }

  private double cacheImpact(String key, double impact, Map<String, Double> impactCache) {
    impactCache.put(key, impact);
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

