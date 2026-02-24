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
      double score1 = computeAvailabilityScore(m1.message, impactCache, activeReplicasCache,
          pendingUpwardMsgCountCache, messageIndexTracker);
      double score2 = computeAvailabilityScore(m2.message, impactCache, activeReplicasCache,
          pendingUpwardMsgCountCache, messageIndexTracker);

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

  private double computeAvailabilityScore(Message message,
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
        message.getResourceName(), message.getPartitionName(), topState, _currentStateOutput);
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

    double score = computeUpwardScore(message, idealState, activeReplicasCache,
        pendingUpwardMsgCountCache, messageIndexTracker);
    return cacheScore(key, score, impactCache);
  }

  /**
   * Compute score for upward transitions.
   * Score is higher when current active count is further below the min active threshold.
   */
  private double computeUpwardScore(Message message, IdealState idealState,
      Map<String, Integer> activeReplicasCache,
      Map<String, Integer> pendingUpwardMsgCountCache,
      Map<String, Integer> messageIndexTracker) {

    String resource = message.getResourceName();
    String partition = message.getPartitionName();
    String partitionKey = resource + ":" + partition;

    int minActive = idealState.getMinActiveReplicas();

    int currentActive = getCurrentActiveReplicas(resource, partition, activeReplicasCache);
    int pending = getPendingUpwardMessages(resource, partition, pendingUpwardMsgCountCache);

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
   * resource's unhealthy state set, determined by {@link #getUnhealthyStates(String)}.
   *
   * <p>This method stays in the strategy (rather than moving to {@link ResourceControllerDataProvider})
   * because it depends on {@link CurrentStateOutput}, which is a live pipeline-stage result, not
   * cluster configuration state that the provider loads from ZooKeeper.
   */
  private int getCurrentActiveReplicas(String resource, String partition,
      Map<String, Integer> activeReplicasCache) {
    String key = resource + ":" + partition;
    if (activeReplicasCache.containsKey(key)) {
      return activeReplicasCache.get(key);
    }

    Map<String, String> currentStates =
        _currentStateOutput.getCurrentStateMap(resource, new Partition(partition));
    if (currentStates == null) {
      activeReplicasCache.put(key, 0);
      return 0;
    }

    Set<String> unhealthyStates = getUnhealthyStates(resource);
    int count = (int) currentStates.values().stream()
        .filter(s -> !unhealthyStates.contains(s))
        .count();

    activeReplicasCache.put(key, count);
    return count;
  }

  /**
   * Returns the set of states considered unhealthy (not active) for a resource.
   * Fetches {@link ResourceConfig} and {@link StateModelDefinition} from the pipeline cache and
   * delegates to {@link InstanceValidationUtil#getUnhealthyStates(ResourceConfig, StateModelDefinition)},
   * avoiding live ZooKeeper reads.
   */
  private Set<String> getUnhealthyStates(String resource) {
    IdealState idealState = _cache.getIdealState(resource);
    StateModelDefinition stateModelDef = idealState != null
        ? _cache.getStateModelDef(idealState.getStateModelDefRef())
        : null;
    ResourceConfig resourceConfig = _cache.getResourceConfig(resource);
    return InstanceValidationUtil.getUnhealthyStates(resourceConfig, stateModelDef);
  }

  /**
   * Counts pending upward-transition messages for a partition.
   * Only upward transitions are counted because pending downward transitions do not reduce
   * availability and should not inflate the effective replica count.
   *
   * <p>This method stays in the strategy (rather than moving to {@link ResourceControllerDataProvider})
   * for the same reason as {@link #getCurrentActiveReplicas}: it depends on
   * {@link CurrentStateOutput}, which is pipeline-stage output, not ZooKeeper-backed cluster state.
   */
  private int getPendingUpwardMessages(String resource, String partition,
      Map<String, Integer> pendingUpwardMsgCountCache) {
    String key = resource + ":" + partition;
    if (pendingUpwardMsgCountCache.containsKey(key)) {
      return pendingUpwardMsgCountCache.get(key);
    }

    int count = 0;
    Map<String, Message> pendingMsgs =
        _currentStateOutput.getPendingMessageMap(resource, new Partition(partition));

    if (pendingMsgs != null && !pendingMsgs.isEmpty()) {
      IdealState idealState = _cache.getIdealState(resource);
      if (idealState != null) {
        StateModelDefinition stateModelDef =
            _cache.getStateModelDef(idealState.getStateModelDefRef());
        // isUpwardTransition already handles null stateModelDef (returns false).
        count = (int) pendingMsgs.values().stream()
            .filter(msg -> StateTransitionHelper.isUpwardTransition(msg.getFromState(),
                msg.getToState(), stateModelDef))
            .count();
      }
    }

    pendingUpwardMsgCountCache.put(key, count);
    return count;
  }

  private double cacheScore(String key, double score, Map<String, Double> impactCache) {
    impactCache.put(key, score);
    return score;
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
