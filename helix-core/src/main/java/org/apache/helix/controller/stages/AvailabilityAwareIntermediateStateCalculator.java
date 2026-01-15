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
 * Availability-aware cross-resource intermediate state calculator.
 *
 * <p>This implementation collects all messages across all resources and sorts them
 * by availability impact, then processes them in priority order with throttling
 * applied globally. This ensures that partitions with the highest availability need
 * get priority over partitions that are closer to their target replica count.
 *
 * <p>Key characteristics:
 * <ul>
 *   <li>Messages are sorted by availability impact score across all resources</li>
 *   <li>Partitions missing top state get highest priority</li>
 *   <li>Top state handoff (downward transitions from top state) get high priority</li>
 *   <li>Global throttling is applied across all messages regardless of resource</li>
 * </ul>
 *
 * <p>This strategy is enabled by setting the cluster config flag
 * {@code isAvailabilityAwarePrioritizationEnabled} to true.
 */
public class AvailabilityAwareIntermediateStateCalculator implements IntermediateStateComputationStrategy {
  private static final Logger LOG =
      LoggerFactory.getLogger(AvailabilityAwareIntermediateStateCalculator.class.getName());

  private String _eventId;

  @Override
  public IntermediateStateOutput compute(ClusterEvent event, Map<String, Resource> resourceMap,
      CurrentStateOutput currentStateOutput, BestPossibleStateOutput bestPossibleStateOutput,
      MessageOutput messageOutput, ResourceControllerDataProvider dataCache) {

    _eventId = event.getEventId();
    IntermediateStateOutput output = new IntermediateStateOutput();
    ClusterStatusMonitor clusterStatusMonitor =
        event.getAttribute(AttributeName.clusterStatusMonitor.name());
    List<String> failedResources = new ArrayList<>();

    StateTransitionThrottleController throttleController =
        new StateTransitionThrottleController(resourceMap.keySet(), dataCache.getClusterConfig(),
            dataCache.getLiveInstances().keySet());

    // Step 1: Collect messages from all FULL_AUTO resources
    List<MessageWithContext> allMessages = new ArrayList<>();
    Map<String, Set<Partition>> partitionsWithErrorByResource = new HashMap<>();
    collectMessagesFromResources(resourceMap, currentStateOutput, bestPossibleStateOutput,
        messageOutput, dataCache, throttleController, output, allMessages,
        partitionsWithErrorByResource);

    // Step 2: Sort messages by availability impact
    AvailabilityAwareMessageComparator comparator =
        new AvailabilityAwareMessageComparator(dataCache, currentStateOutput);

    sortMessagesByAvailabilityImpact(allMessages, comparator);

    // Step 3: Process messages with throttling
    ThrottlingContext throttlingContext = new ThrottlingContext(resourceMap.keySet());
    processMessagesWithThrottling(allMessages, currentStateOutput, dataCache, throttleController,
        partitionsWithErrorByResource, throttlingContext);

    // Step 4: Build intermediate state maps for each resource
    buildIntermediateStateMaps(resourceMap, currentStateOutput, bestPossibleStateOutput,
        messageOutput, dataCache, output, clusterStatusMonitor, failedResources,
        throttlingContext, partitionsWithErrorByResource);

    if (clusterStatusMonitor != null) {
      clusterStatusMonitor.setResourceRebalanceStates(failedResources,
          ResourceMonitor.RebalanceStatus.INTERMEDIATE_STATE_CAL_FAILED);
      clusterStatusMonitor.setResourceRebalanceStates(output.resourceSet(),
          ResourceMonitor.RebalanceStatus.NORMAL);
    }

    return output;
  }

  /**
   * Collects messages from all FULL_AUTO resources for availability-aware processing.
   */
  private void collectMessagesFromResources(
      Map<String, Resource> resourceMap, CurrentStateOutput currentStateOutput,
      BestPossibleStateOutput bestPossibleStateOutput, MessageOutput messageOutput,
      ResourceControllerDataProvider dataCache, StateTransitionThrottleController throttleController,
      IntermediateStateOutput output, List<MessageWithContext> allMessages,
      Map<String, Set<Partition>> partitionsWithErrorByResource) {

    for (String resourceName : resourceMap.keySet()) {
      if (!bestPossibleStateOutput.containsResource(resourceName)) {
        LogUtil.logInfo(LOG, _eventId, String.format(
            "Skip calculating intermediate state for resource %s because the best possible state is not available.",
            resourceName));
        continue;
      }

      Resource resource = resourceMap.get(resourceName);
      IdealState idealState = getIdealStateOrDefault(resourceName, resource, dataCache);

      // Skip non-FULL_AUTO resources
      if (!IdealState.RebalanceMode.FULL_AUTO.equals(idealState.getRebalanceMode())) {
        output.setState(resourceName, bestPossibleStateOutput.getPartitionStateMap(resourceName));
        continue;
      }

      Map<Partition, List<Message>> resourceMessageMap = messageOutput.getResourceMessageMap(resourceName);
      if (resourceMessageMap == null || resourceMessageMap.isEmpty()) {
        output.setState(resourceName, bestPossibleStateOutput.getPartitionStateMap(resourceName));
        continue;
      }

      // Track partitions with error state
      Set<Partition> partitionsWithError = new HashSet<>();
      for (Partition partition : currentStateOutput.getCurrentStateMap(resourceName).keySet()) {
        Map<String, String> stateMap = currentStateOutput.getCurrentStateMap(resourceName).get(partition);
        if (stateMap.containsValue(HelixDefinedState.ERROR.name())) {
          partitionsWithError.add(partition);
        }
      }
      partitionsWithErrorByResource.put(resourceName, partitionsWithError);

      StateModelDefinition stateModelDef = dataCache.getStateModelDef(idealState.getStateModelDefRef());
      Map<String, List<String>> preferenceLists = bestPossibleStateOutput.getPreferenceLists(resourceName);

      chargePendingTransition(resource, currentStateOutput, throttleController, dataCache,
          preferenceLists, stateModelDef);

      // Collect messages from this resource
      for (Map.Entry<Partition, List<Message>> entry : resourceMessageMap.entrySet()) {
        Partition partition = entry.getKey();
        List<String> preferenceList = preferenceLists.get(partition.getPartitionName());
        Map<String, Integer> requiredStates = getRequiredStates(resourceName, dataCache, preferenceList);

        for (Message message : entry.getValue()) {
          allMessages.add(new MessageWithContext(message, resource, partition, idealState,
              stateModelDef, preferenceList, requiredStates));
        }
      }
    }
  }

  /**
   * Sorts messages by availability impact with deterministic pre-sorting.
   */
  private void sortMessagesByAvailabilityImpact(List<MessageWithContext> allMessages,
      AvailabilityAwareMessageComparator comparator) {
    // Pre-sort deterministically for consistent messageIndex assignment
    allMessages.sort((m1, m2) -> {
      Message msg1 = m1.getMessage();
      Message msg2 = m2.getMessage();
      int cmp = msg1.getResourceName().compareTo(msg2.getResourceName());
      if (cmp != 0) return cmp;
      cmp = msg1.getPartitionName().compareTo(msg2.getPartitionName());
      if (cmp != 0) return cmp;
      return msg1.getTgtName().compareTo(msg2.getTgtName());
    });

    // Pre-compute impact scores to populate cache
    for (MessageWithContext msgCtx : allMessages) {
      comparator.getAvailabilityImpact(msgCtx.getMessage());
    }
    comparator.resetMessageIndexTracker();

    // Sort by availability impact
    allMessages.sort((m1, m2) -> comparator.compare(m1.getMessage(), m2.getMessage()));
  }

  /**
   * Processes messages in sorted order with throttling.
   */
  private void processMessagesWithThrottling(
      List<MessageWithContext> allMessages, CurrentStateOutput currentStateOutput,
      ResourceControllerDataProvider dataCache, StateTransitionThrottleController throttleController,
      Map<String, Set<Partition>> partitionsWithErrorByResource, ThrottlingContext ctx) {

    Map<String, Map<String, String>> derivedCurrentStateMaps = new HashMap<>();

    for (MessageWithContext msgCtx : allMessages) {
      Message message = msgCtx.getMessage();
      String resourceName = message.getResourceName();
      Partition partition = msgCtx.getPartition();

      // Get or initialize derived current state map for this partition
      String partitionKey = resourceName + ":" + partition.getPartitionName();
      Map<String, String> derivedCurrentStateMap = derivedCurrentStateMaps.computeIfAbsent(
          partitionKey, k -> new HashMap<>(currentStateOutput.getCurrentStateMap(resourceName, partition)));

      // Determine rebalance type and process message
      RebalanceType rebalanceType = getRebalanceTypePerMessage(
          msgCtx.getRequiredStates(), message, derivedCurrentStateMap);

      boolean wasThrottled = processMessage(msgCtx, message, resourceName, partition,
          derivedCurrentStateMap, rebalanceType, throttleController, dataCache,
          partitionsWithErrorByResource, ctx);

      // Update derived state if not throttled
      if (!wasThrottled) {
        derivedCurrentStateMap.put(message.getTgtName(), message.getToState());
        ctx.updatedResourceMessageMaps.get(resourceName)
            .computeIfAbsent(partition, k -> new ArrayList<>())
            .add(message);
        ctx.processedCount++;
      } else {
        ctx.throttledCount++;
      }
    }
  }

  /**
   * Processes a single message and returns whether it was throttled.
   */
  private boolean processMessage(MessageWithContext msgCtx, Message message, String resourceName,
      Partition partition, Map<String, String> derivedCurrentStateMap, RebalanceType rebalanceType,
      StateTransitionThrottleController throttleController, ResourceControllerDataProvider dataCache,
      Map<String, Set<Partition>> partitionsWithErrorByResource, ThrottlingContext ctx) {

    Set<String> messagesThrottled;
    Set<String> messagesProcessed;

    if (rebalanceType.equals(RebalanceType.RECOVERY_BALANCE)) {
      message.setSTRebalanceType(Message.STRebalanceType.RECOVERY_REBALANCE);
      messagesProcessed = ctx.messagesForRecoveryByResource.get(resourceName);
      messagesThrottled = ctx.messagesThrottledForRecoveryByResource.get(resourceName);
      messagesProcessed.add(message.getId());

      throttleStateTransitionsForReplica(throttleController, resourceName, partition,
          message, messagesThrottled, RebalanceType.RECOVERY_BALANCE, dataCache,
          ctx.updatedResourceMessageMaps.computeIfAbsent(resourceName, k -> new HashMap<>()),
          partition);
    } else {
      message.setSTRebalanceType(Message.STRebalanceType.LOAD_REBALANCE);
      messagesProcessed = ctx.messagesForLoadByResource.get(resourceName);
      messagesThrottled = ctx.messagesThrottledForLoadByResource.get(resourceName);
      messagesProcessed.add(message.getId());

      ClusterConfig clusterConfig = dataCache.getClusterConfig();
      int threshold = getErrorOrRecoveryThreshold(clusterConfig);
      Set<Partition> partitionsWithError = partitionsWithErrorByResource.getOrDefault(
          resourceName, new HashSet<>());
      boolean onlyDownwardLoadBalance = partitionsWithError.size() > threshold;

      if (onlyDownwardLoadBalance && !isLoadBalanceDownwardStateTransition(
          message, msgCtx.getStateModelDef())) {
        messagesThrottled.add(message.getId());
      } else {
        throttleStateTransitionsForReplica(throttleController, resourceName, partition,
            message, messagesThrottled, RebalanceType.LOAD_BALANCE, dataCache,
            ctx.updatedResourceMessageMaps.computeIfAbsent(resourceName, k -> new HashMap<>()),
            partition);
      }
    }

    return messagesThrottled.contains(message.getId());
  }

  /**
   * Builds intermediate state maps for all resources.
   */
  private void buildIntermediateStateMaps(
      Map<String, Resource> resourceMap, CurrentStateOutput currentStateOutput,
      BestPossibleStateOutput bestPossibleStateOutput, MessageOutput messageOutput,
      ResourceControllerDataProvider dataCache, IntermediateStateOutput output,
      ClusterStatusMonitor clusterStatusMonitor, List<String> failedResources,
      ThrottlingContext ctx, Map<String, Set<Partition>> partitionsWithErrorByResource) {

    for (String resourceName : resourceMap.keySet()) {
      if (!bestPossibleStateOutput.containsResource(resourceName)) {
        continue;
      }

      IdealState idealState = dataCache.getIdealState(resourceName);
      if (idealState != null && !IdealState.RebalanceMode.FULL_AUTO.equals(idealState.getRebalanceMode())) {
        continue;
      }

      Map<Partition, List<Message>> resourceMessageMap = messageOutput.getResourceMessageMap(resourceName);
      if (resourceMessageMap == null || resourceMessageMap.isEmpty()) {
        continue;
      }

      try {
        PartitionStateMap intermediatePartitionStateMap =
            new PartitionStateMap(resourceName, currentStateOutput.getCurrentStateMap(resourceName));

        applyPendingMessages(currentStateOutput, resourceName,
            intermediatePartitionStateMap);
        applyNonThrottledMessages(ctx.updatedResourceMessageMaps.get(resourceName),
            intermediatePartitionStateMap);

        output.setState(resourceName, intermediatePartitionStateMap);

        updateResourceMonitoringStats(clusterStatusMonitor, resourceName, dataCache, ctx,
            partitionsWithErrorByResource);

      } catch (HelixException ex) {
        LogUtil.logInfo(LOG, _eventId,
            "Failed to calculate intermediate partition states for resource " + resourceName, ex);
        failedResources.add(resourceName);
      }
    }
  }

  /**
   * Updates monitoring stats for a resource.
   */
  private void updateResourceMonitoringStats(ClusterStatusMonitor clusterStatusMonitor,
      String resourceName, ResourceControllerDataProvider dataCache, ThrottlingContext ctx,
      Map<String, Set<Partition>> partitionsWithErrorByResource) {
    if (clusterStatusMonitor != null) {
      Set<String> recoveryMsgs = ctx.messagesForRecoveryByResource.get(resourceName);
      Set<String> loadMsgs = ctx.messagesForLoadByResource.get(resourceName);
      Set<String> recoveryThrottled = ctx.messagesThrottledForRecoveryByResource.get(resourceName);
      Set<String> loadThrottled = ctx.messagesThrottledForLoadByResource.get(resourceName);

      ClusterConfig clusterConfig = dataCache.getClusterConfig();
      int threshold = getErrorOrRecoveryThreshold(clusterConfig);
      Set<Partition> partitionsWithError = partitionsWithErrorByResource.getOrDefault(
          resourceName, new HashSet<>());
      boolean onlyDownwardLoadBalance = partitionsWithError.size() > threshold;

      clusterStatusMonitor.updateRebalancerStats(resourceName,
          recoveryMsgs != null ? recoveryMsgs.size() : 0,
          loadMsgs != null ? loadMsgs.size() : 0,
          recoveryThrottled != null ? recoveryThrottled.size() : 0,
          loadThrottled != null ? loadThrottled.size() : 0,
          onlyDownwardLoadBalance);
    }
  }

  /**
   * Gets the IdealState for a resource, creating a default one if it doesn't exist.
   */
  private IdealState getIdealStateOrDefault(String resourceName, Resource resource,
      ResourceControllerDataProvider dataCache) {
    IdealState idealState = dataCache.getIdealState(resourceName);
    if (idealState == null) {
      LogUtil.logInfo(LOG, _eventId, String
          .format("IdealState for resource %s does not exist; resource may not exist anymore",
              resourceName));
      idealState = new IdealState(resourceName);
      idealState.setStateModelDefRef(resource.getStateModelDefRef());
    }
    return idealState;
  }

  private int getErrorOrRecoveryThreshold(ClusterConfig clusterConfig) {
    if (clusterConfig.getErrorOrRecoveryPartitionThresholdForLoadBalance() != -1) {
      return clusterConfig.getErrorOrRecoveryPartitionThresholdForLoadBalance();
    }
    if (clusterConfig.getErrorPartitionThresholdForLoadBalance() != 0) {
      return clusterConfig.getErrorPartitionThresholdForLoadBalance();
    }
    return 1;
  }

  private void chargePendingTransition(Resource resource, CurrentStateOutput currentStateOutput,
      StateTransitionThrottleController throttleController, ResourceControllerDataProvider cache,
      Map<String, List<String>> preferenceLists, StateModelDefinition stateModelDefinition) {
    String resourceName = resource.getResourceName();
    for (Partition partition : resource.getPartitions()) {
      Map<String, Integer> requiredStates =
          getRequiredStates(resourceName, cache, preferenceLists.get(partition.getPartitionName()));
      Map<String, String> currentStateMap =
          currentStateOutput.getCurrentStateMap(resourceName, partition);
      List<Message> pendingMessages = new ArrayList<>(
          currentStateOutput.getPendingMessageMap(resourceName, partition).values());

      for (Message message : pendingMessages) {
        RebalanceType rebalanceType =
            getRebalanceTypePerMessage(requiredStates, message, currentStateMap);
        String currentState = currentStateMap.get(message.getTgtName());
        if (currentState == null) {
          currentState = stateModelDefinition.getInitialState();
        }
        if (!message.getToState().equals(currentState) && message.getFromState()
            .equals(currentState) && !cache
            .getDisabledInstancesForPartition(resourceName, partition.getPartitionName())
            .contains(message.getTgtName())) {
          throttleController.chargeInstance(rebalanceType, message.getTgtName());
          throttleController.chargeResource(rebalanceType, resourceName);
          throttleController.chargeCluster(rebalanceType);
        }
      }
    }
  }

  private void throttleStateTransitionsForReplica(
      StateTransitionThrottleController throttleController, String resourceName,
      Partition partition, Message messageToThrottle, Set<String> messagesThrottled,
      RebalanceType rebalanceType, ResourceControllerDataProvider cache,
      Map<Partition, List<Message>> resourceMessageMap, Partition partitionKey) {
    boolean hasReachedThrottlingLimit = false;

    if (throttleController.shouldThrottleForResource(rebalanceType, resourceName)) {
      hasReachedThrottlingLimit = true;
      if (LOG.isDebugEnabled()) {
        LogUtil.logDebug(LOG, _eventId, String.format(
            "Throttled because of cluster/resource quota is full for message {%s} on partition {%s} in resource {%s}",
            messageToThrottle.getId(), partition.getPartitionName(), resourceName));
      }
    } else {
      if (!cache.getDisabledInstancesForPartition(resourceName, partition.getPartitionName())
          .contains(messageToThrottle.getTgtName())) {
        if (throttleController.shouldThrottleForInstance(rebalanceType, messageToThrottle.getTgtName())) {
          hasReachedThrottlingLimit = true;
          if (LOG.isDebugEnabled()) {
            LogUtil.logDebug(LOG, _eventId, String.format(
                "Throttled because of instance level quota is full on instance {%s} for message {%s} of partition {%s} in resource {%s}",
                messageToThrottle.getTgtName(), messageToThrottle.getId(),
                partition.getPartitionName(), resourceName));
          }
        }
      }
    }

    if (!hasReachedThrottlingLimit) {
      throttleController.chargeCluster(rebalanceType);
      throttleController.chargeResource(rebalanceType, resourceName);
      throttleController.chargeInstance(rebalanceType, messageToThrottle.getTgtName());
    } else {
      messagesThrottled.add(messageToThrottle.getId());
    }
  }

  private boolean isLoadBalanceDownwardStateTransition(Message message,
      StateModelDefinition stateModelDefinition) {
    if (stateModelDefinition == null) {
      return false;
    }
    Map<String, Integer> statePriorityMap = stateModelDefinition.getStatePriorityMap();
    return statePriorityMap.containsKey(message.getFromState())
        && statePriorityMap.containsKey(message.getToState())
        && statePriorityMap.get(message.getFromState()) < statePriorityMap.get(message.getToState());
  }

  private RebalanceType getRebalanceTypePerMessage(Map<String, Integer> desiredStates,
      Message message, Map<String, String> derivedCurrentStates) {
    Map<String, Integer> desiredStatesSnapshot = new HashMap<>(desiredStates);
    for (String state : derivedCurrentStates.values()) {
      if (desiredStatesSnapshot.containsKey(state)) {
        if (desiredStatesSnapshot.get(state) == 1) {
          desiredStatesSnapshot.remove(state);
        } else {
          desiredStatesSnapshot.put(state, desiredStatesSnapshot.get(state) - 1);
        }
      }
    }
    return desiredStatesSnapshot.containsKey(message.getToState()) ? RebalanceType.RECOVERY_BALANCE
        : RebalanceType.LOAD_BALANCE;
  }

  private Map<String, Integer> getRequiredStates(String resourceName,
      ResourceControllerDataProvider resourceControllerDataProvider, List<String> preferenceList) {
    IdealState idealState = resourceControllerDataProvider.getIdealState(resourceName);
    StateModelDefinition stateModelDefinition =
        resourceControllerDataProvider.getStateModelDef(idealState.getStateModelDefRef());
    int requiredNumReplica =
        idealState.getMinActiveReplicas() == -1 ?
            idealState.getReplicaCount(preferenceList == null ? 0 : preferenceList.size())
            : idealState.getMinActiveReplicas();

    if (preferenceList != null) {
      return stateModelDefinition.getStateCountMap((int) preferenceList.stream().filter(
              i -> resourceControllerDataProvider.getEnabledLiveInstances().contains(i))
          .count(), requiredNumReplica);
    }
    return stateModelDefinition.getStateCountMap(
        resourceControllerDataProvider.getEnabledLiveInstances().size(),
        requiredNumReplica);
  }

  private void applyPendingMessages(CurrentStateOutput currentStateOutput, String resourceName,
      PartitionStateMap intermediatePartitionStateMap) {
    Map<Partition, Map<String, Message>> pendingMessageMap =
        currentStateOutput.getPendingMessageMap(resourceName);
    if (pendingMessageMap != null) {
      for (Map.Entry<Partition, Map<String, Message>> partitionEntry : pendingMessageMap.entrySet()) {
        Partition partition = partitionEntry.getKey();
        Map<String, Message> instanceMessageMap = partitionEntry.getValue();
        if (instanceMessageMap != null) {
          for (Map.Entry<String, Message> instanceEntry : instanceMessageMap.entrySet()) {
            String instance = instanceEntry.getKey();
            Message message = instanceEntry.getValue();
            if (message != null && message.getToState() != null) {
              intermediatePartitionStateMap.setState(partition, instance, message.getToState());
            }
          }
        }
      }
    }
  }

  private void applyNonThrottledMessages(Map<Partition, List<Message>> resourceMessageMap,
      PartitionStateMap intermediatePartitionStateMap) {
    if (resourceMessageMap != null) {
      for (Map.Entry<Partition, List<Message>> entry : resourceMessageMap.entrySet()) {
        Partition partition = entry.getKey();
        List<Message> messages = entry.getValue();
        if (messages != null) {
          for (Message message : messages) {
            if (message != null && message.getTgtName() != null && message.getToState() != null) {
              intermediatePartitionStateMap.setState(partition, message.getTgtName(),
                  message.getToState());
            }
          }
        }
      }
    }
  }

  /**
   * Context object to track throttling state across message processing.
   */
  private static class ThrottlingContext {
    final Map<String, Set<String>> messagesForRecoveryByResource = new HashMap<>();
    final Map<String, Set<String>> messagesForLoadByResource = new HashMap<>();
    final Map<String, Set<String>> messagesThrottledForRecoveryByResource = new HashMap<>();
    final Map<String, Set<String>> messagesThrottledForLoadByResource = new HashMap<>();
    final Map<String, Map<Partition, List<Message>>> updatedResourceMessageMaps = new HashMap<>();
    int processedCount = 0;
    int throttledCount = 0;

    ThrottlingContext(Set<String> resourceNames) {
      for (String resourceName : resourceNames) {
        messagesForRecoveryByResource.put(resourceName, new HashSet<>());
        messagesForLoadByResource.put(resourceName, new HashSet<>());
        messagesThrottledForRecoveryByResource.put(resourceName, new HashSet<>());
        messagesThrottledForLoadByResource.put(resourceName, new HashSet<>());
        updatedResourceMessageMaps.put(resourceName, new HashMap<>());
      }
    }
  }

  /**
   * POJO to hold message along with its context for availability-aware processing.
   */
  private static class MessageWithContext {
    private final Message _message;
    private final Resource _resource;
    private final Partition _partition;
    private final IdealState _idealState;
    private final StateModelDefinition _stateModelDef;
    private final List<String> _preferenceList;
    private final Map<String, Integer> _requiredStates;

    MessageWithContext(Message message, Resource resource, Partition partition,
        IdealState idealState, StateModelDefinition stateModelDef,
        List<String> preferenceList, Map<String, Integer> requiredStates) {
      _message = message;
      _resource = resource;
      _partition = partition;
      _idealState = idealState;
      _stateModelDef = stateModelDef;
      _preferenceList = preferenceList;
      _requiredStates = requiredStates;
    }

    Message getMessage() { return _message; }
    Resource getResource() { return _resource; }
    Partition getPartition() { return _partition; }
    IdealState getIdealState() { return _idealState; }
    StateModelDefinition getStateModelDef() { return _stateModelDef; }
    List<String> getPreferenceList() { return _preferenceList; }
    Map<String, Integer> getRequiredStates() { return _requiredStates; }
  }
}

