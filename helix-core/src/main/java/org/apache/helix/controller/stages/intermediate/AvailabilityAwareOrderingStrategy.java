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
import java.util.Set;

import org.apache.helix.controller.dataproviders.ResourceControllerDataProvider;
import org.apache.helix.controller.stages.CurrentStateOutput;
import org.apache.helix.controller.stages.StateTransitionHelper;
import org.apache.helix.model.IdealState;
import org.apache.helix.model.Message;
import org.apache.helix.model.Partition;
import org.apache.helix.model.ResourceConfig;
import org.apache.helix.model.StateModelDefinition;
import org.apache.helix.util.InstanceValidationUtil;

/**
 * Message ordering strategy that prioritizes messages based on their availability impact.
 * Sorts messages cross-resource based on availability scores to prioritize partitions
 * with fewer active replicas over those closer to their target replica count.
 *
 * <p>Prioritization order (highest to lowest score):
 * <ol>
 *   <li>Partitions missing top state — score {@value TOP_STATE_MISSING_SCORE}</li>
 *   <li>Top-state handoff transitions — score {@value TOP_STATE_HANDOFF_SCORE}</li>
 *   <li>Upward transitions scored by {@code minActiveReplicas / (currentActive + pending + index)}</li>
 *   <li>Downward / load-balance transitions — score 0.0</li>
 *   <li>Messages for deleted/misconfigured resources — score -1.0</li>
 * </ol>
 */
public class AvailabilityAwareOrderingStrategy implements MessageOrderingStrategy {
  // Explicit large constants instead of Double.MAX_VALUE to avoid floating-point equality issues
  private static final double TOP_STATE_MISSING_SCORE = 1_000_000.0;
  private static final double TOP_STATE_HANDOFF_SCORE = 999_999.0;

  private final ResourceControllerDataProvider _cache;
  private final CurrentStateOutput _currentStateOutput;

  public AvailabilityAwareOrderingStrategy(ResourceControllerDataProvider cache,
      CurrentStateOutput currentStateOutput) {
    _cache = cache;
    _currentStateOutput = currentStateOutput;
  }

  @Override
  public void sortMessages(List<MessageContext> messages) {
    // Caches are local to each sort invocation so the comparator lambda is self-consistent.
    Map<String, Double> impactCache = new HashMap<>();
    Map<String, Integer> activeReplicasCache = new HashMap<>();
    Map<String, Integer> pendingUpwardMsgCountCache = new HashMap<>();
    Map<String, Integer> messageIndexTracker = new HashMap<>();

    messages.sort((m1, m2) -> {
      double score1 = computeAvailabilityScore(m1.message, m1.partition, impactCache,
          activeReplicasCache, pendingUpwardMsgCountCache, messageIndexTracker);
      double score2 = computeAvailabilityScore(m2.message, m2.partition, impactCache,
          activeReplicasCache, pendingUpwardMsgCountCache, messageIndexTracker);

      int delta = Double.compare(score2, score1);
      if (delta != 0) {
        return delta;
      }

      // Stable tiebreaking: resource name, then partition name.
      int resourceCompare = m1.resourceName.compareTo(m2.resourceName);
      if (resourceCompare != 0) {
        return resourceCompare;
      }
      return m1.partition.getPartitionName().compareTo(m2.partition.getPartitionName());
    });
  }

  private double computeAvailabilityScore(Message message, Partition partition,
      Map<String, Double> impactCache,
      Map<String, Integer> activeReplicasCache,
      Map<String, Integer> pendingUpwardMsgCountCache,
      Map<String, Integer> messageIndexTracker) {

    String key = cacheKey(message);
    if (impactCache.containsKey(key)) {
      return impactCache.get(key);
    }

    IdealState idealState = _cache.getIdealState(message.getResourceName());
    if (idealState == null) {
      // Resource deleted or in bad state — deprioritize below even downward transitions.
      return cacheScore(key, -1.0, impactCache);
    }

    StateModelDefinition stateModelDef = _cache.getStateModelDef(idealState.getStateModelDefRef());
    if (stateModelDef == null) {
      // Configuration error — deprioritize below even downward transitions.
      return cacheScore(key, -1.0, impactCache);
    }

    String topState = stateModelDef.getTopState();

    boolean missingTopState = StateTransitionHelper.isPartitionMissingTopState(
        message.getResourceName(), partition, topState, _currentStateOutput);
    if (missingTopState && message.getToState().equals(topState)) {
      return cacheScore(key, TOP_STATE_MISSING_SCORE, impactCache);
    }

    if (StateTransitionHelper.isTopStateHandoff(message.getFromState(), message.getToState(),
        topState, stateModelDef)) {
      return cacheScore(key, TOP_STATE_HANDOFF_SCORE, impactCache);
    }

    if (!StateTransitionHelper.isUpwardTransition(message.getFromState(), message.getToState(),
        stateModelDef)) {
      return cacheScore(key, 0.0, impactCache);
    }

    double score = computeUpwardScore(message, partition, idealState, stateModelDef,
        activeReplicasCache, pendingUpwardMsgCountCache, messageIndexTracker);
    return cacheScore(key, score, impactCache);
  }

  /**
   * Compute score for upward transitions.
   * Score is higher when current active count is further below the min active threshold.
   */
  private double computeUpwardScore(Message message, Partition partition, IdealState idealState,
      StateModelDefinition stateModelDef,
      Map<String, Integer> activeReplicasCache,
      Map<String, Integer> pendingUpwardMsgCountCache,
      Map<String, Integer> messageIndexTracker) {

    String resource = message.getResourceName();
    String partitionKey = resource + "\0" + partition.getPartitionName();

    int minActive = idealState.getMinActiveReplicas();

    ResourceConfig resourceConfig = _cache.getResourceConfig(resource);
    int currentActive = getCurrentActiveReplicas(resource, partition, stateModelDef,
        resourceConfig, activeReplicasCache);
    int pending = getPendingUpwardMessages(resource, partition, stateModelDef,
        pendingUpwardMsgCountCache);

    int index = messageIndexTracker.getOrDefault(partitionKey, 0);
    messageIndexTracker.put(partitionKey, index + 1);

    int effectiveCount = currentActive + pending + index;

    // getMinActiveReplicas() returns -1 when unconfigured; guard covers both -1 and 0.
    if (minActive <= 0) {
      return 1.0 / (effectiveCount + 1);
    }

    return (double) minActive / (effectiveCount + 1);
  }

  /**
   * Counts active replicas for a partition. A replica is active if its state is not in the
   * resource's unhealthy state set.
   */
  private int getCurrentActiveReplicas(String resource, Partition partition,
      StateModelDefinition stateModelDef, ResourceConfig resourceConfig,
      Map<String, Integer> activeReplicasCache) {
    String key = resource + "\0" + partition.getPartitionName();
    if (activeReplicasCache.containsKey(key)) {
      return activeReplicasCache.get(key);
    }

    Map<String, String> currentStates =
        _currentStateOutput.getCurrentStateMap(resource, partition);
    if (currentStates == null) {
      activeReplicasCache.put(key, 0);
      return 0;
    }

    Set<String> unhealthyStates = InstanceValidationUtil.getUnhealthyStates(resourceConfig,
        stateModelDef);
    int count = (int) currentStates.values().stream()
        .filter(s -> !unhealthyStates.contains(s))
        .count();

    activeReplicasCache.put(key, count);
    return count;
  }

  /**
   * Counts pending upward-transition messages for a partition.
   * Only upward transitions are counted because pending downward transitions do not reduce
   * availability and should not inflate the effective replica count.
   */
  private int getPendingUpwardMessages(String resource, Partition partition,
      StateModelDefinition stateModelDef,
      Map<String, Integer> pendingUpwardMsgCountCache) {
    String key = resource + "\0" + partition.getPartitionName();
    if (pendingUpwardMsgCountCache.containsKey(key)) {
      return pendingUpwardMsgCountCache.get(key);
    }

    int count = 0;
    Map<String, Message> pendingMsgs =
        _currentStateOutput.getPendingMessageMap(resource, partition);

    if (pendingMsgs != null && !pendingMsgs.isEmpty()) {
      // isUpwardTransition already handles null stateModelDef (returns false).
      count = (int) pendingMsgs.values().stream()
          .filter(msg -> StateTransitionHelper.isUpwardTransition(msg.getFromState(),
              msg.getToState(), stateModelDef))
          .count();
    }

    pendingUpwardMsgCountCache.put(key, count);
    return count;
  }

  private double cacheScore(String key, double score, Map<String, Double> impactCache) {
    impactCache.put(key, score);
    return score;
  }

  private String cacheKey(Message message) {
    return String.join("\0",
        message.getResourceName(),
        message.getPartitionName(),
        message.getFromState(),
        message.getToState(),
        message.getTgtName());
  }
}
