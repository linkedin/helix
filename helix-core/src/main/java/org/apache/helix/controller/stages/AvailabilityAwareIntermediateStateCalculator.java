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

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.helix.HelixDefinedState;
import org.apache.helix.HelixException;
import org.apache.helix.api.config.StateTransitionThrottleConfig.RebalanceType;
import org.apache.helix.controller.LogUtil;
import org.apache.helix.controller.common.PartitionStateMap;
import org.apache.helix.controller.dataproviders.ResourceControllerDataProvider;
import org.apache.helix.model.ClusterConfig;
import org.apache.helix.model.IdealState;
import org.apache.helix.model.Message;
import org.apache.helix.model.Partition;
import org.apache.helix.model.Resource;
import org.apache.helix.model.StateModelDefinition;
import org.apache.helix.monitoring.mbeans.ClusterStatusMonitor;
import org.apache.helix.monitoring.mbeans.ResourceMonitor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Availability-aware intermediate state calculator that prioritizes messages across all resources
 * based on their availability impact.
 *
 * <p>Unlike the traditional resource-priority approach, this calculator:
 * <ul>
 *   <li>Collects ALL messages from ALL FULL_AUTO resources first</li>
 *   <li>Sorts them globally by availability impact (partitions missing top state get priority)</li>
 *   <li>Applies throttling in sorted order, so high-impact transitions get quota first</li>
 * </ul>
 *
 * <p>Enabled via cluster config: {@code isAvailabilityAwarePrioritizationEnabled = true}
 */
public class AvailabilityAwareIntermediateStateCalculator implements IntermediateStateComputationStrategy {

  private static final Logger LOG = LoggerFactory.getLogger(AvailabilityAwareIntermediateStateCalculator.class);

  // Instance variables to reduce parameter passing
  private String _eventId;
  private ResourceControllerDataProvider _dataCache;
  private CurrentStateOutput _currentStateOutput;
  private StateTransitionThrottleController _throttleController;

  // Tracking maps for throttling and monitoring
  private Map<String, Set<Partition>> _partitionsWithErrorByResource;
  private Map<String, Set<String>> _recoveryMessagesByResource;
  private Map<String, Set<String>> _loadMessagesByResource;
  private Map<String, Set<String>> _throttledRecoveryByResource;
  private Map<String, Set<String>> _throttledLoadByResource;
  private Map<String, Map<Partition, List<Message>>> _approvedMessagesByResource;

  @Override
  public IntermediateStateOutput compute(ClusterEvent event, Map<String, Resource> resourceMap,
      CurrentStateOutput currentStateOutput, BestPossibleStateOutput bestPossibleStateOutput,
      MessageOutput messageOutput, ResourceControllerDataProvider dataCache) {

    // Initialize instance state
    _eventId = event.getEventId();
    _dataCache = dataCache;
    _currentStateOutput = currentStateOutput;
    _throttleController = new StateTransitionThrottleController(
        resourceMap.keySet(), dataCache.getClusterConfig(), dataCache.getLiveInstances().keySet());

    initializeTrackingMaps(resourceMap.keySet());

    IntermediateStateOutput output = new IntermediateStateOutput();
    List<String> failedResources = new ArrayList<>();

    // ========== STEP 1: Collect all messages ==========
    List<MessageContext> allMessages = collectMessages(resourceMap, bestPossibleStateOutput, messageOutput, output);

    // ========== STEP 2: Sort by availability impact ==========
    sortByAvailabilityImpact(allMessages);

    // ========== STEP 3: Process with throttling ==========
    processWithThrottling(allMessages);

    // ========== STEP 4: Build intermediate state ==========
    buildIntermediateState(resourceMap, bestPossibleStateOutput, messageOutput, output, failedResources);

    // Update monitoring
    updateMonitoring(event, failedResources, output);

    return output;
  }

  // ===================================================================================
  // STEP 1: Message Collection
  // ===================================================================================

  private List<MessageContext> collectMessages(Map<String, Resource> resourceMap,
      BestPossibleStateOutput bestPossibleStateOutput, MessageOutput messageOutput,
      IntermediateStateOutput output) {

    List<MessageContext> allMessages = new ArrayList<>();

    for (Map.Entry<String, Resource> entry : resourceMap.entrySet()) {
      String resourceName = entry.getKey();
      Resource resource = entry.getValue();

      // Skip if no best possible state available
      if (!bestPossibleStateOutput.containsResource(resourceName)) {
        LogUtil.logInfo(LOG, _eventId, "Skipping resource " + resourceName + ": no best possible state");
        continue;
      }

      IdealState idealState = _dataCache.getIdealState(resourceName);
      if (idealState == null) {
        idealState = createDefaultIdealState(resourceName, resource);
      }

      // Non-FULL_AUTO resources: use best possible state directly
      if (!IdealState.RebalanceMode.FULL_AUTO.equals(idealState.getRebalanceMode())) {
        output.setState(resourceName, bestPossibleStateOutput.getPartitionStateMap(resourceName));
        continue;
      }

      Map<Partition, List<Message>> resourceMessages = messageOutput.getResourceMessageMap(resourceName);
      if (resourceMessages == null || resourceMessages.isEmpty()) {
        output.setState(resourceName, bestPossibleStateOutput.getPartitionStateMap(resourceName));
        continue;
      }

      // Track error partitions for this resource
      trackErrorPartitions(resourceName);

      // Charge pending transitions to throttle controller
      StateModelDefinition stateModelDef = _dataCache.getStateModelDef(idealState.getStateModelDefRef());
      Map<String, List<String>> preferenceLists = bestPossibleStateOutput.getPreferenceLists(resourceName);
      chargePendingTransitions(resource, preferenceLists, stateModelDef);

      // Collect messages with context
      for (Map.Entry<Partition, List<Message>> partitionEntry : resourceMessages.entrySet()) {
        Partition partition = partitionEntry.getKey();
        List<String> preferenceList = preferenceLists.get(partition.getPartitionName());
        Map<String, Integer> requiredStates = computeRequiredStates(resourceName, preferenceList);

        for (Message message : partitionEntry.getValue()) {
          allMessages.add(new MessageContext(message, partition, stateModelDef, requiredStates));
        }
      }
    }

    return allMessages;
  }

  private void trackErrorPartitions(String resourceName) {
    Set<Partition> errorPartitions = new HashSet<>();
    Map<Partition, Map<String, String>> currentStateMap = _currentStateOutput.getCurrentStateMap(resourceName);

    for (Map.Entry<Partition, Map<String, String>> entry : currentStateMap.entrySet()) {
      if (entry.getValue().containsValue(HelixDefinedState.ERROR.name())) {
        errorPartitions.add(entry.getKey());
      }
    }
    _partitionsWithErrorByResource.put(resourceName, errorPartitions);
  }

  // ===================================================================================
  // STEP 2: Availability-Aware Sorting
  // ===================================================================================

  private void sortByAvailabilityImpact(List<MessageContext> allMessages) {
    // First: deterministic sort for consistent index assignment
    allMessages.sort((m1, m2) -> {
      int cmp = m1.message.getResourceName().compareTo(m2.message.getResourceName());
      if (cmp != 0) return cmp;
      cmp = m1.message.getPartitionName().compareTo(m2.message.getPartitionName());
      if (cmp != 0) return cmp;
      return m1.message.getTgtName().compareTo(m2.message.getTgtName());
    });

    // Create comparator and pre-compute scores
    AvailabilityAwareMessageComparator comparator =
        new AvailabilityAwareMessageComparator(_dataCache, _currentStateOutput);
    comparator.setEventId(_eventId);

    for (MessageContext ctx : allMessages) {
      comparator.getAvailabilityImpact(ctx.message);
    }
    comparator.resetMessageIndexTracker();

    // Sort by availability impact (highest impact first)
    allMessages.sort((m1, m2) -> comparator.compare(m1.message, m2.message));
  }

  // ===================================================================================
  // STEP 3: Throttled Message Processing
  // ===================================================================================

  private void processWithThrottling(List<MessageContext> allMessages) {
    // Track derived state per partition (as messages are approved, state changes)
    Map<String, Map<String, String>> derivedStates = new HashMap<>();

    for (MessageContext ctx : allMessages) {
      Message message = ctx.message;
      String resourceName = message.getResourceName();
      Partition partition = ctx.partition;

      // Get current derived state for this partition
      String partitionKey = resourceName + ":" + partition.getPartitionName();
      Map<String, String> derivedState = derivedStates.computeIfAbsent(partitionKey,
          k -> new HashMap<>(_currentStateOutput.getCurrentStateMap(resourceName, partition)));

      // Classify message type
      RebalanceType rebalanceType = classifyMessage(ctx.requiredStates, message, derivedState);
      boolean isRecovery = rebalanceType == RebalanceType.RECOVERY_BALANCE;

      // Track message
      message.setSTRebalanceType(isRecovery
          ? Message.STRebalanceType.RECOVERY_REBALANCE
          : Message.STRebalanceType.LOAD_REBALANCE);

      (isRecovery ? _recoveryMessagesByResource : _loadMessagesByResource)
          .get(resourceName).add(message.getId());

      // Check if should be throttled
      boolean throttled = shouldThrottle(message, resourceName, partition, rebalanceType, ctx.stateModelDef);

      if (throttled) {
        (isRecovery ? _throttledRecoveryByResource : _throttledLoadByResource)
            .get(resourceName).add(message.getId());
      } else {
        // Approve message: update derived state and track
        derivedState.put(message.getTgtName(), message.getToState());
        _approvedMessagesByResource.get(resourceName)
            .computeIfAbsent(partition, k -> new ArrayList<>())
            .add(message);
      }
    }
  }

  private boolean shouldThrottle(Message message, String resourceName, Partition partition,
      RebalanceType rebalanceType, StateModelDefinition stateModelDef) {

    // For load balance: check if only downward transitions allowed
    if (rebalanceType == RebalanceType.LOAD_BALANCE) {
      int errorThreshold = getErrorThreshold();
      Set<Partition> errorPartitions = _partitionsWithErrorByResource.getOrDefault(resourceName, new HashSet<>());

      if (errorPartitions.size() > errorThreshold && !isDownwardTransition(message, stateModelDef)) {
        return true;  // Throttle non-downward load balance when too many errors
      }
    }

    // Check throttle limits
    if (_throttleController.shouldThrottleForResource(rebalanceType, resourceName)) {
      logThrottled(message, partition, resourceName, "resource quota full");
      return true;
    }

    String instance = message.getTgtName();
    if (!_dataCache.getDisabledInstancesForPartition(resourceName, partition.getPartitionName()).contains(instance)) {
      if (_throttleController.shouldThrottleForInstance(rebalanceType, instance)) {
        logThrottled(message, partition, resourceName, "instance quota full for " + instance);
        return true;
      }
    }

    // Charge quotas
    _throttleController.chargeCluster(rebalanceType);
    _throttleController.chargeResource(rebalanceType, resourceName);
    _throttleController.chargeInstance(rebalanceType, instance);

    return false;
  }

  // ===================================================================================
  // STEP 4: Build Intermediate State
  // ===================================================================================

  private void buildIntermediateState(Map<String, Resource> resourceMap,
      BestPossibleStateOutput bestPossibleStateOutput, MessageOutput messageOutput,
      IntermediateStateOutput output, List<String> failedResources) {

    for (String resourceName : resourceMap.keySet()) {
      if (!bestPossibleStateOutput.containsResource(resourceName)) {
        continue;
      }

      IdealState idealState = _dataCache.getIdealState(resourceName);
      if (idealState != null && !IdealState.RebalanceMode.FULL_AUTO.equals(idealState.getRebalanceMode())) {
        continue;
      }

      Map<Partition, List<Message>> resourceMessages = messageOutput.getResourceMessageMap(resourceName);
      if (resourceMessages == null || resourceMessages.isEmpty()) {
        continue;
      }

      try {
        // Start with current state
        PartitionStateMap intermediateState = new PartitionStateMap(resourceName,
            _currentStateOutput.getCurrentStateMap(resourceName));

        // Apply pending messages (already in-flight)
        applyPendingMessages(resourceName, intermediateState);

        // Apply approved messages from this round
        applyApprovedMessages(resourceName, intermediateState);

        output.setState(resourceName, intermediateState);

      } catch (HelixException ex) {
        LogUtil.logInfo(LOG, _eventId, "Failed to compute intermediate state for " + resourceName, ex);
        failedResources.add(resourceName);
      }
    }
  }

  private void applyPendingMessages(String resourceName, PartitionStateMap stateMap) {
    Map<Partition, Map<String, Message>> pendingMap = _currentStateOutput.getPendingMessageMap(resourceName);
    if (pendingMap == null) return;

    for (Map.Entry<Partition, Map<String, Message>> entry : pendingMap.entrySet()) {
      Partition partition = entry.getKey();
      for (Map.Entry<String, Message> instanceEntry : entry.getValue().entrySet()) {
        Message msg = instanceEntry.getValue();
        if (msg != null && msg.getToState() != null) {
          stateMap.setState(partition, instanceEntry.getKey(), msg.getToState());
        }
      }
    }
  }

  private void applyApprovedMessages(String resourceName, PartitionStateMap stateMap) {
    Map<Partition, List<Message>> approved = _approvedMessagesByResource.get(resourceName);
    if (approved == null) return;

    for (Map.Entry<Partition, List<Message>> entry : approved.entrySet()) {
      for (Message msg : entry.getValue()) {
        if (msg != null && msg.getTgtName() != null && msg.getToState() != null) {
          stateMap.setState(entry.getKey(), msg.getTgtName(), msg.getToState());
        }
      }
    }
  }

  // ===================================================================================
  // Helper Methods
  // ===================================================================================

  private void initializeTrackingMaps(Set<String> resourceNames) {
    _partitionsWithErrorByResource = new HashMap<>();
    _recoveryMessagesByResource = new HashMap<>();
    _loadMessagesByResource = new HashMap<>();
    _throttledRecoveryByResource = new HashMap<>();
    _throttledLoadByResource = new HashMap<>();
    _approvedMessagesByResource = new HashMap<>();

    for (String resourceName : resourceNames) {
      _recoveryMessagesByResource.put(resourceName, new HashSet<>());
      _loadMessagesByResource.put(resourceName, new HashSet<>());
      _throttledRecoveryByResource.put(resourceName, new HashSet<>());
      _throttledLoadByResource.put(resourceName, new HashSet<>());
      _approvedMessagesByResource.put(resourceName, new HashMap<>());
    }
  }

  private IdealState createDefaultIdealState(String resourceName, Resource resource) {
    LogUtil.logInfo(LOG, _eventId, "IdealState not found for " + resourceName + ", creating default");
    IdealState idealState = new IdealState(resourceName);
    idealState.setStateModelDefRef(resource.getStateModelDefRef());
    return idealState;
  }

  private void chargePendingTransitions(Resource resource, Map<String, List<String>> preferenceLists,
      StateModelDefinition stateModelDef) {
    String resourceName = resource.getResourceName();

    for (Partition partition : resource.getPartitions()) {
      Map<String, Integer> requiredStates = computeRequiredStates(resourceName,
          preferenceLists.get(partition.getPartitionName()));
      Map<String, String> currentState = _currentStateOutput.getCurrentStateMap(resourceName, partition);

      for (Message msg : _currentStateOutput.getPendingMessageMap(resourceName, partition).values()) {
        RebalanceType type = classifyMessage(requiredStates, msg, currentState);
        String instance = msg.getTgtName();
        String currState = currentState.getOrDefault(instance, stateModelDef.getInitialState());

        if (!msg.getToState().equals(currState) && msg.getFromState().equals(currState)
            && !_dataCache.getDisabledInstancesForPartition(resourceName, partition.getPartitionName())
                .contains(instance)) {
          _throttleController.chargeInstance(type, instance);
          _throttleController.chargeResource(type, resourceName);
          _throttleController.chargeCluster(type);
        }
      }
    }
  }

  private RebalanceType classifyMessage(Map<String, Integer> requiredStates, Message message,
      Map<String, String> currentStates) {
    // Check if message helps satisfy required state counts
    Map<String, Integer> remaining = new HashMap<>(requiredStates);

    for (String state : currentStates.values()) {
      if (remaining.containsKey(state)) {
        int count = remaining.get(state);
        if (count <= 1) {
          remaining.remove(state);
        } else {
          remaining.put(state, count - 1);
        }
      }
    }

    return remaining.containsKey(message.getToState())
        ? RebalanceType.RECOVERY_BALANCE
        : RebalanceType.LOAD_BALANCE;
  }

  private Map<String, Integer> computeRequiredStates(String resourceName, List<String> preferenceList) {
    IdealState idealState = _dataCache.getIdealState(resourceName);
    StateModelDefinition stateModelDef = _dataCache.getStateModelDef(idealState.getStateModelDefRef());

    int requiredReplicas = idealState.getMinActiveReplicas() == -1
        ? idealState.getReplicaCount(preferenceList == null ? 0 : preferenceList.size())
        : idealState.getMinActiveReplicas();

    int liveCount = preferenceList != null
        ? (int) preferenceList.stream().filter(_dataCache.getEnabledLiveInstances()::contains).count()
        : _dataCache.getEnabledLiveInstances().size();

    return stateModelDef.getStateCountMap(liveCount, requiredReplicas);
  }

  private boolean isDownwardTransition(Message message, StateModelDefinition stateModelDef) {
    if (stateModelDef == null) return false;

    Map<String, Integer> priorities = stateModelDef.getStatePriorityMap();
    String from = message.getFromState();
    String to = message.getToState();

    return priorities.containsKey(from) && priorities.containsKey(to)
        && priorities.get(from) < priorities.get(to);  // Lower number = higher priority
  }

  private int getErrorThreshold() {
    ClusterConfig config = _dataCache.getClusterConfig();
    if (config.getErrorOrRecoveryPartitionThresholdForLoadBalance() != -1) {
      return config.getErrorOrRecoveryPartitionThresholdForLoadBalance();
    }
    if (config.getErrorPartitionThresholdForLoadBalance() != 0) {
      return config.getErrorPartitionThresholdForLoadBalance();
    }
    return 1;
  }

  private void logThrottled(Message message, Partition partition, String resource, String reason) {
    if (LOG.isDebugEnabled()) {
      LogUtil.logDebug(LOG, _eventId, String.format(
          "Throttled message %s for %s/%s: %s", message.getId(), resource, partition.getPartitionName(), reason));
    }
  }

  private void updateMonitoring(ClusterEvent event, List<String> failedResources, IntermediateStateOutput output) {
    ClusterStatusMonitor monitor = event.getAttribute(AttributeName.clusterStatusMonitor.name());
    if (monitor == null) return;

    monitor.setResourceRebalanceStates(failedResources, ResourceMonitor.RebalanceStatus.INTERMEDIATE_STATE_CAL_FAILED);
    monitor.setResourceRebalanceStates(output.resourceSet(), ResourceMonitor.RebalanceStatus.NORMAL);

    int errorThreshold = getErrorThreshold();
    for (String resourceName : _recoveryMessagesByResource.keySet()) {
      Set<Partition> errors = _partitionsWithErrorByResource.getOrDefault(resourceName, new HashSet<>());

      monitor.updateRebalancerStats(resourceName,
          _recoveryMessagesByResource.get(resourceName).size(),
          _loadMessagesByResource.get(resourceName).size(),
          _throttledRecoveryByResource.get(resourceName).size(),
          _throttledLoadByResource.get(resourceName).size(),
          errors.size() > errorThreshold);
    }
  }

  // ===================================================================================
  // Simple Context Holder (only what's needed for processing)
  // ===================================================================================

  /**
   * Holds a message with its processing context. Kept minimal - only fields actually used.
   */
  private static class MessageContext {
    final Message message;
    final Partition partition;
    final StateModelDefinition stateModelDef;
    final Map<String, Integer> requiredStates;

    MessageContext(Message message, Partition partition, StateModelDefinition stateModelDef,
        Map<String, Integer> requiredStates) {
      this.message = message;
      this.partition = partition;
      this.stateModelDef = stateModelDef;
      this.requiredStates = requiredStates;
    }
  }
}
