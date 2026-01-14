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

  private static final Logger logger = LoggerFactory.getLogger(AvailabilityAwareMessageComparator.class.getName());

  // Sentinel value for top state missing - highest priority
  private static final double TOP_STATE_MISSING_IMPACT = Double.MAX_VALUE;
  // Sentinel value for top state handoff downward transitions - very high priority
  private static final double TOP_STATE_HANDOFF_IMPACT = Double.MAX_VALUE - 1;

  private final ResourceControllerDataProvider _cache;
  private final CurrentStateOutput _currentStateOutput;
  // Cache for computed availability impact scores to avoid recomputation
  private final Map<String, Double> _availabilityImpactCache;
  // Cache for partition's current active replica count
  private final Map<String, Integer> _currentActiveReplicasCache;
  // Cache for partition's pending message count
  private final Map<String, Integer> _pendingMessageCountCache;
  // Track message index per partition for computing effective replica count
  private final Map<String, Integer> _messageIndexTracker;
  // Event ID for logging correlation
  private String _eventId;
  // Enable detailed logging during impact calculation
  private boolean _detailedLoggingEnabled = false;

  /**
   * Creates a new AvailabilityAwareMessageComparator for availability-aware prioritization.
   * Messages are sorted purely by their availability impact score.
   *
   * @param cache the resource controller data provider containing cluster metadata
   * @param currentStateOutput the current state output containing current states and pending messages
   */
  public AvailabilityAwareMessageComparator(
      ResourceControllerDataProvider cache,
      CurrentStateOutput currentStateOutput) {
    _cache = cache;
    _currentStateOutput = currentStateOutput;
    _availabilityImpactCache = new HashMap<>();
    _currentActiveReplicasCache = new HashMap<>();
    _pendingMessageCountCache = new HashMap<>();
    _messageIndexTracker = new HashMap<>();
  }

  /**
   * Set the event ID for logging correlation.
   * @param eventId the event ID from the controller pipeline
   */
  public void setEventId(String eventId) {
    _eventId = eventId;
    _detailedLoggingEnabled = true;
  }

  /**
   * Get the availability impact score for a message (for logging purposes).
   * This method returns the cached impact score without modifying any state.
   * @param message the message to get impact for
   * @return the availability impact score
   */
  public double getAvailabilityImpactForLogging(Message message) {
    String cacheKey = getCacheKey(message);
    return _availabilityImpactCache.getOrDefault(cacheKey, 0.0);
  }

  @Override
  public int compare(Message m1, Message m2) {
    // 1. Calculate availability impact for both messages
    double impact1 = getAvailabilityImpact(m1);
    double impact2 = getAvailabilityImpact(m2);

    // Higher impact = higher priority (sort in descending order)
    int impactComparison = Double.compare(impact2, impact1);
    if (impactComparison != 0) {
      return impactComparison;
    }

    // 2. Deterministic ordering by resource name
    int resourceComparison = m1.getResourceName().compareTo(m2.getResourceName());
    if (resourceComparison != 0) {
      return resourceComparison;
    }

    // 3. Deterministic ordering by partition name
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
  /**
   * Calculate and cache the availability impact score for a message.
   * Package-private to allow pre-computation from IntermediateStateCalcStage.
   */
  double getAvailabilityImpact(Message message) {
    String cacheKey = getCacheKey(message);

    // Check cache first
    if (_availabilityImpactCache.containsKey(cacheKey)) {
      return _availabilityImpactCache.get(cacheKey);
    }

    String resourceName = message.getResourceName();
    String partitionName = message.getPartitionName();
    Partition partition = new Partition(partitionName);

    IdealState idealState = _cache.getIdealState(resourceName);
    if (idealState == null) {
      // No ideal state found, use default low priority
      logImpactCalculation(message, "NO_IDEAL_STATE", 0.0, "IdealState not found");
      _availabilityImpactCache.put(cacheKey, 0.0);
      return 0.0;
    }

    StateModelDefinition stateModelDef = _cache.getStateModelDef(idealState.getStateModelDefRef());
    if (stateModelDef == null) {
      logImpactCalculation(message, "NO_STATE_MODEL", 0.0, "StateModelDefinition not found");
      _availabilityImpactCache.put(cacheKey, 0.0);
      return 0.0;
    }

    String topState = stateModelDef.getTopState();
    String fromState = message.getFromState();
    String toState = message.getToState();

    // Check if partition is missing top state AND this message transitions TO the top state
    // This ensures we prioritize messages that will actually establish the top state (e.g., STANDBY->LEADER)
    // rather than giving MAX priority to ALL messages for partitions missing top state (which defeats prioritization)
    boolean partitionMissingTopState = isPartitionMissingTopState(resourceName, partition, topState);
    if (partitionMissingTopState && toState.equals(topState)) {
      logImpactCalculation(message, "TOP_STATE_MISSING_TO_TOP", TOP_STATE_MISSING_IMPACT,
          String.format("Partition missing %s AND transitioning TO %s - HIGHEST PRIORITY", topState, topState));
      _availabilityImpactCache.put(cacheKey, TOP_STATE_MISSING_IMPACT);
      return TOP_STATE_MISSING_IMPACT;
    }

    // Check if this is a top state handoff downward transition (e.g., MASTER → SLAVE)
    // These need high priority to enable the subsequent upward transition on another node
    if (isTopStateHandoffDownward(fromState, toState, topState, stateModelDef)) {
      logImpactCalculation(message, "TOP_STATE_HANDOFF", TOP_STATE_HANDOFF_IMPACT,
          String.format("Handoff downward %s->%s - HIGH PRIORITY (prevents starvation)", fromState, toState));
      _availabilityImpactCache.put(cacheKey, TOP_STATE_HANDOFF_IMPACT);
      return TOP_STATE_HANDOFF_IMPACT;
    }

    // Check if this is an upward transition
    if (!isUpwardTransition(fromState, toState, stateModelDef)) {
      // Downward transition (not top state handoff) - lowest priority
      logImpactCalculation(message, "DOWNWARD_TRANSITION", 0.0,
          String.format("Non-top-state downward %s->%s - LOWEST PRIORITY", fromState, toState));
      _availabilityImpactCache.put(cacheKey, 0.0);
      return 0.0;
    }

    // Calculate availability impact for upward transitions
    // Note: This includes OFFLINE->STANDBY transitions even when partition is missing top state
    // These are prioritized by availability impact (how many replicas the partition already has)
    // rather than all getting the same MAX priority
    double impact = calculateUpwardTransitionImpact(resourceName, partitionName, idealState, message);
    
    // Log additional context if partition is missing top state but not transitioning to it
    if (partitionMissingTopState) {
      logImpactCalculation(message, "UPWARD_MISSING_TOP_STATE", impact,
          String.format("Partition missing %s but transition %s->%s uses availability impact (not MAX)", 
              topState, fromState, toState));
    }
    
    _availabilityImpactCache.put(cacheKey, impact);
    return impact;
  }

  /**
   * Log impact calculation details for debugging.
   */
  private void logImpactCalculation(Message message, String reason, double impact, String details) {
    if (_detailedLoggingEnabled && _eventId != null) {
      LogUtil.logDebug(logger, _eventId, String.format(
          "IMPACT_CALC: Resource=%s, Partition=%s, Transition=%s->%s, Target=%s, " +
          "Reason=%s, Impact=%s, Details=%s",
          message.getResourceName(), message.getPartitionName(),
          message.getFromState(), message.getToState(), message.getTgtName(),
          reason, formatImpactScore(impact), details));
    }
  }

  /**
   * Format impact score for human-readable logging.
   * Handles special values like MAX_VALUE (top state missing) and MAX_VALUE-1000 (top state handoff).
   */
  private String formatImpactScore(double impact) {
    if (impact >= Double.MAX_VALUE - 1) {
      return "MAX(TOP_STATE_MISSING)";
    }
    if (impact >= Double.MAX_VALUE - 1001) {
      return "MAX-1K(HANDOFF)";
    }
    return String.format("%.2f", impact);
  }

  /**
   * Check if the partition is missing its top state.
   */
  private boolean isPartitionMissingTopState(String resourceName, Partition partition, String topState) {
    Map<String, String> currentStateMap = _currentStateOutput.getCurrentStateMap(resourceName, partition);
    if (currentStateMap == null || currentStateMap.isEmpty()) {
      return true;
    }
    return !currentStateMap.containsValue(topState);
  }

  /**
   * Check if this is a top state handoff downward transition.
   * A top state handoff occurs when we're moving the top state from one instance to another.
   * The downward transition (e.g., MASTER → SLAVE) on the old node must complete before
   * the upward transition on the new node can proceed.
   */
  private boolean isTopStateHandoffDownward(String fromState, String toState,
      String topState, StateModelDefinition stateModelDef) {
    // Check if transition is FROM the top state
    if (!fromState.equals(topState)) {
      return false;
    }

    // Verify it's a downward transition from top state
    Map<String, Integer> statePriorityMap = stateModelDef.getStatePriorityMap();
    if (statePriorityMap == null || !statePriorityMap.containsKey(fromState)
        || !statePriorityMap.containsKey(toState)) {
      return false;
    }

    // Lower priority value = higher state priority
    // Downward transition means going from lower value (higher priority) to higher value (lower priority)
    return statePriorityMap.get(fromState) < statePriorityMap.get(toState);
  }

  /**
   * Check if this is an upward state transition.
   */
  private boolean isUpwardTransition(String fromState, String toState, StateModelDefinition stateModelDef) {
    Map<String, Integer> statePriorityMap = stateModelDef.getStatePriorityMap();
    if (statePriorityMap == null || !statePriorityMap.containsKey(fromState)
        || !statePriorityMap.containsKey(toState)) {
      return false;
    }

    // Lower priority value = higher state priority
    // Upward transition means going from higher value (lower priority) to lower value (higher priority)
    return statePriorityMap.get(fromState) > statePriorityMap.get(toState);
  }

  /**
   * Calculate the availability impact for an upward transition.
   *
   * <p>Formula: effectiveMinActive / (effectiveReplicaCount + 1)
   * <p>where:
   * <ul>
   *   <li>effectiveMinActive = minActiveReplicas if > 0, otherwise targetReplicas</li>
   *   <li>effectiveReplicaCount = currentActiveReplicas + pendingMessages + messageIndex</li>
   * </ul>
   */
  private double calculateUpwardTransitionImpact(String resourceName, String partitionName,
      IdealState idealState, Message message) {
    String partitionKey = resourceName + ":" + partitionName;

    // Get minActiveReplicas from ideal state
    int minActiveReplicas = idealState.getMinActiveReplicas();

    // Get target replicas count
    int targetReplicas = getTargetReplicaCount(idealState, resourceName, partitionName);

    // Use targetReplicas as fallback when minActiveReplicas is 0 or not set
    int effectiveMinActive = (minActiveReplicas <= 0) ? targetReplicas : minActiveReplicas;
    boolean usedFallback = (minActiveReplicas <= 0);

    // Get current active replicas for this partition
    int currentActiveReplicas = getCurrentActiveReplicas(resourceName, partitionName);

    // Get pending message count for this partition
    int pendingMessages = getPendingMessageCount(resourceName, partitionName);

    // Get and increment message index for this partition
    int messageIndex = _messageIndexTracker.getOrDefault(partitionKey, 0);
    _messageIndexTracker.put(partitionKey, messageIndex + 1);

    // Calculate effective replica count
    int effectiveReplicaCount = currentActiveReplicas + pendingMessages + messageIndex;

    // Calculate and return impact score
    // Higher effectiveMinActive and lower effectiveReplicaCount = higher impact
    double impact = (double) effectiveMinActive / (effectiveReplicaCount + 1);

    // Log detailed calculation
    if (_detailedLoggingEnabled && _eventId != null) {
      LogUtil.logDebug(logger, _eventId, String.format(
          "IMPACT_CALC: Resource=%s, Partition=%s, Transition=%s->%s, Target=%s, " +
          "minActiveReplicas=%d, targetReplicas=%d, effectiveMinActive=%d (fallback=%s), " +
          "currentActiveReplicas=%d, pendingMessages=%d, messageIndex=%d, " +
          "effectiveReplicaCount=%d, Formula=%d/(%d+1), Impact=%.4f",
          resourceName, partitionName,
          message.getFromState(), message.getToState(), message.getTgtName(),
          minActiveReplicas, targetReplicas, effectiveMinActive, usedFallback,
          currentActiveReplicas, pendingMessages, messageIndex,
          effectiveReplicaCount, effectiveMinActive, effectiveReplicaCount, impact));
    }

    return impact;
  }

  /**
   * Get the target replica count for a partition.
   */
  private int getTargetReplicaCount(IdealState idealState, String resourceName, String partitionName) {
    // First try to get from preference list size
    Map<String, String> instanceStateMap = idealState.getInstanceStateMap(partitionName);
    if (instanceStateMap != null && !instanceStateMap.isEmpty()) {
      return instanceStateMap.size();
    }

    // Fall back to replica count setting
    int replicaCount = idealState.getReplicaCount(_cache.getEnabledLiveInstances().size());
    return Math.max(replicaCount, 1);
  }

  /**
   * Get the current number of active replicas for a partition.
   * Active replicas are those not in ERROR, OFFLINE, or DROPPED states.
   */
  private int getCurrentActiveReplicas(String resourceName, String partitionName) {
    String cacheKey = resourceName + ":" + partitionName;
    if (_currentActiveReplicasCache.containsKey(cacheKey)) {
      return _currentActiveReplicasCache.get(cacheKey);
    }

    Partition partition = new Partition(partitionName);
    Map<String, String> currentStateMap = _currentStateOutput.getCurrentStateMap(resourceName, partition);

    int activeCount = 0;
    if (currentStateMap != null) {
      for (String state : currentStateMap.values()) {
        if (isActiveState(state)) {
          activeCount++;
        }
      }
    }

    _currentActiveReplicasCache.put(cacheKey, activeCount);
    return activeCount;
  }

  /**
   * Get the count of pending messages for a partition.
   * Pending messages are state transitions that are already in flight.
   */
  private int getPendingMessageCount(String resourceName, String partitionName) {
    String cacheKey = resourceName + ":" + partitionName;
    if (_pendingMessageCountCache.containsKey(cacheKey)) {
      return _pendingMessageCountCache.get(cacheKey);
    }

    Partition partition = new Partition(partitionName);
    Map<String, Message> pendingMessages = _currentStateOutput.getPendingMessageMap(resourceName, partition);

    int pendingUpwardCount = 0;
    if (pendingMessages != null) {
      IdealState idealState = _cache.getIdealState(resourceName);
      if (idealState != null) {
        StateModelDefinition stateModelDef = _cache.getStateModelDef(idealState.getStateModelDefRef());
        if (stateModelDef != null) {
          for (Message msg : pendingMessages.values()) {
            if (isUpwardTransition(msg.getFromState(), msg.getToState(), stateModelDef)) {
              pendingUpwardCount++;
            }
          }
        }
      }
    }

    _pendingMessageCountCache.put(cacheKey, pendingUpwardCount);
    return pendingUpwardCount;
  }

  /**
   * Check if a state is considered "active" (not ERROR, OFFLINE, DROPPED, or UNKNOWN).
   */
  private boolean isActiveState(String state) {
    return state != null
        && !state.equalsIgnoreCase("ERROR")
        && !state.equalsIgnoreCase("OFFLINE")
        && !state.equalsIgnoreCase("DROPPED")
        && !state.isEmpty();
  }

  /**
   * Generate a unique cache key for a message.
   */
  private String getCacheKey(Message message) {
    return message.getResourceName() + ":" + message.getPartitionName() + ":"
        + message.getTgtName() + ":" + message.getFromState() + ":" + message.getToState();
  }

  /**
   * Reset the message index tracker. This should be called before starting a new sorting operation
   * to ensure consistent impact calculation across multiple sort operations.
   */
  public void resetMessageIndexTracker() {
    _messageIndexTracker.clear();
  }
}


