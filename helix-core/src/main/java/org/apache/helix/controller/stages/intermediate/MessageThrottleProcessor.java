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

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.apache.helix.api.config.StateTransitionThrottleConfig.RebalanceType;
import org.apache.helix.controller.dataproviders.ResourceControllerDataProvider;
import org.apache.helix.controller.stages.BestPossibleStateOutput;
import org.apache.helix.controller.stages.CurrentStateOutput;
import org.apache.helix.controller.stages.MessageOutput;
import org.apache.helix.controller.stages.StateTransitionHelper;
import org.apache.helix.controller.stages.StateTransitionThrottleController;
import org.apache.helix.model.ClusterConfig;
import org.apache.helix.model.IdealState;
import org.apache.helix.model.Message;
import org.apache.helix.model.Partition;
import org.apache.helix.model.Resource;
import org.apache.helix.model.StateModelDefinition;

/**
 * Processes messages with throttling logic using StateTransitionThrottleController.
 * Handles classification, throttling checks, and quota charging for state transitions.
 */
public class MessageThrottleProcessor {

  /**
   * Process messages with throttling, returning approved messages by resource and partition.
   * Also charges pending transitions before processing new messages.
   *
   * @param messages All messages to process (in priority order)
   * @param currentStateOutput Current state of the cluster
   * @param throttleController Controller for managing throttle quotas
   * @param cache Cluster data provider
   * @param metricsPerResource Metrics tracking for each resource
   * @param messageOutput Message output to update with throttled messages
   * @param resourceMap Map of resources being processed
   * @param bestPossibleStateOutput Best possible state output
   * @return Map of resource -> partition -> approved messages
   */
  public Map<String, Map<Partition, List<Message>>> processMessagesWithThrottling(
      List<MessageOrderingStrategy.MessageContext> messages,
      CurrentStateOutput currentStateOutput,
      StateTransitionThrottleController throttleController,
      ResourceControllerDataProvider cache,
      Map<String, ResourceThrottleMetrics> metricsPerResource,
      MessageOutput messageOutput,
      Map<String, Resource> resourceMap,
      BestPossibleStateOutput bestPossibleStateOutput) {

    // First charge all pending transitions
    chargePendingTransitionsForAllResources(resourceMap, currentStateOutput,
        throttleController, cache, bestPossibleStateOutput);

    // Then process new messages
    return processWithThrottling(messages, currentStateOutput, throttleController,
        cache, metricsPerResource, messageOutput);
  }

  /**
   * Charge pending transitions for all resources before processing new messages.
   */
  private void chargePendingTransitionsForAllResources(
      Map<String, Resource> resourceMap,
      CurrentStateOutput currentStateOutput,
      StateTransitionThrottleController throttleController,
      ResourceControllerDataProvider cache,
      BestPossibleStateOutput bestPossibleStateOutput) {

    for (Map.Entry<String, Resource> entry : resourceMap.entrySet()) {
      String resourceName = entry.getKey();
      Resource resource = entry.getValue();

      if (!bestPossibleStateOutput.containsResource(resourceName)) {
        continue;
      }

      IdealState idealState = cache.getIdealState(resourceName);
      if (idealState == null) {
        continue;
      }

      StateModelDefinition stateModelDef =
          cache.getStateModelDef(idealState.getStateModelDefRef());
      Map<String, List<String>> preferenceLists =
          bestPossibleStateOutput.getPreferenceLists(resourceName);

      chargePendingTransitions(resource, currentStateOutput, throttleController,
          cache, preferenceLists, stateModelDef);
    }
  }

  /**
   * Process messages in order, classifying, throttling, and approving as appropriate.
   */
  private Map<String, Map<Partition, List<Message>>> processWithThrottling(
      List<MessageOrderingStrategy.MessageContext> messages,
      CurrentStateOutput currentStateOutput,
      StateTransitionThrottleController throttleController,
      ResourceControllerDataProvider cache,
      Map<String, ResourceThrottleMetrics> metricsPerResource,
      MessageOutput messageOutput) {

    Map<String, Map<Partition, List<Message>>> approvedMessages = new HashMap<>();
    Map<String, Map<String, String>> derivedStates = new HashMap<>();

    for (MessageOrderingStrategy.MessageContext ctx : messages) {
      Message message = ctx.message;
      String resourceName = ctx.resourceName;
      Partition partition = ctx.partition;

      // Initialize approved map for this resource
      approvedMessages.computeIfAbsent(resourceName, k -> new HashMap<>());

      // Get or initialize derived state for this partition
      Map<String, String> derivedState = derivedStates.computeIfAbsent(
          resourceName + ":" + partition.getPartitionName(),
          k -> new HashMap<>(currentStateOutput.getCurrentStateMap(resourceName, partition)));

      // Classify message
      RebalanceType type = classifyMessage(ctx.requiredStates, message, derivedState);
      message.setSTRebalanceType(type == RebalanceType.RECOVERY_BALANCE
          ? Message.STRebalanceType.RECOVERY_REBALANCE
          : Message.STRebalanceType.LOAD_REBALANCE);

      // Record metrics
      ResourceThrottleMetrics metrics = metricsPerResource.get(resourceName);
      metrics.recordMessage(type, message.getId());

      // Check throttle
      int errorThreshold = getErrorThreshold(cache.getClusterConfig());
      boolean throttled = shouldThrottle(message, resourceName, partition, type,
          ctx.stateModelDef, throttleController, metrics.errorPartitions.size(),
          errorThreshold, cache);

      if (throttled) {
        metrics.recordThrottled(type, message.getId());
        // Remove throttled message from input so downstream sees only approved messages
        Map<Partition, List<Message>> resourceMsgMap =
            messageOutput.getResourceMessageMap(resourceName);
        if (resourceMsgMap != null && resourceMsgMap.get(partition) != null) {
          resourceMsgMap.get(partition).remove(message);
        }
      } else {
        derivedState.put(message.getTgtName(), message.getToState());
        approvedMessages.get(resourceName)
            .computeIfAbsent(partition, k -> new ArrayList<>())
            .add(message);
      }
    }

    return approvedMessages;
  }

  /**
   * Check if a message should be throttled.
   * If not throttled, charges the appropriate quotas.
   */
  private boolean shouldThrottle(Message message, String resourceName, Partition partition,
      RebalanceType type, StateModelDefinition stateModelDef,
      StateTransitionThrottleController throttleController,
      int numErrorPartitions, int errorThreshold, ResourceControllerDataProvider cache) {

    // Block non-downward load balance when too many error partitions
    if (type == RebalanceType.LOAD_BALANCE && numErrorPartitions > errorThreshold) {
      if (!StateTransitionHelper.isDownwardTransition(
          message.getFromState(), message.getToState(), stateModelDef)) {
        return true;
      }
    }

    if (throttleController.shouldThrottleForResource(type, resourceName)) {
      return true;
    }

    String instance = message.getTgtName();
    if (!cache.getDisabledInstancesForPartition(resourceName, partition.getPartitionName())
        .contains(instance)) {
      if (throttleController.shouldThrottleForInstance(type, instance)) {
        return true;
      }
    }

    // Not throttled — charge quotas
    throttleController.chargeCluster(type);
    throttleController.chargeResource(type, resourceName);
    throttleController.chargeInstance(type, instance);

    return false;
  }

  /**
   * Classify a message as RECOVERY or LOAD balance based on required states.
   */
  public static RebalanceType classifyMessage(Map<String, Integer> requiredStates,
      Message message, Map<String, String> currentStates) {

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

  /**
   * Get error partition threshold from cluster config.
   */
  public static int getErrorThreshold(ClusterConfig config) {
    if (config.getErrorOrRecoveryPartitionThresholdForLoadBalance() != -1) {
      return config.getErrorOrRecoveryPartitionThresholdForLoadBalance();
    }
    if (config.getErrorPartitionThresholdForLoadBalance() != 0) {
      return config.getErrorPartitionThresholdForLoadBalance();
    }
    return 1;
  }

  /**
   * Charge throttle quotas for pending state transitions.
   */
  private void chargePendingTransitions(Resource resource, CurrentStateOutput currentStateOutput,
      StateTransitionThrottleController throttleController, ResourceControllerDataProvider cache,
      Map<String, List<String>> preferenceLists, StateModelDefinition stateModelDef) {

    String resourceName = resource.getResourceName();

    for (Partition partition : resource.getPartitions()) {
      List<String> preferenceList = preferenceLists.get(partition.getPartitionName());
      Map<String, Integer> requiredStates = getRequiredStates(resourceName, cache, preferenceList);
      Map<String, String> currentStateMap =
          currentStateOutput.getCurrentStateMap(resourceName, partition);

      List<Message> pendingMessages = new ArrayList<>(
          currentStateOutput.getPendingMessageMap(resourceName, partition).values());

      if (preferenceList != null && !preferenceList.isEmpty()) {
        pendingMessages.sort(new MessagePriorityComparator(
            preferenceList, stateModelDef.getStatePriorityMap()));
      }

      for (Message message : pendingMessages) {
        RebalanceType type = classifyMessage(requiredStates, message, currentStateMap);
        String currentState = currentStateMap.getOrDefault(
            message.getTgtName(), stateModelDef.getInitialState());

        if (!message.getToState().equals(currentState)
            && message.getFromState().equals(currentState)
            && !cache.getDisabledInstancesForPartition(resourceName, partition.getPartitionName())
                .contains(message.getTgtName())) {
          throttleController.chargeInstance(type, message.getTgtName());
          throttleController.chargeResource(type, resourceName);
          throttleController.chargeCluster(type);
        }
      }
    }
  }

  /**
   * Get required states for a partition based on state model and replica count.
   */
  public static Map<String, Integer> getRequiredStates(String resourceName,
      ResourceControllerDataProvider cache, List<String> preferenceList) {

    IdealState idealState = cache.getIdealState(resourceName);
    StateModelDefinition stateModelDef =
        cache.getStateModelDef(idealState.getStateModelDefRef());

    int requiredReplicas = idealState.getMinActiveReplicas() == -1
        ? idealState.getReplicaCount(preferenceList == null ? 0 : preferenceList.size())
        : idealState.getMinActiveReplicas();

    int liveCount;
    if (preferenceList != null) {
      liveCount = (int) preferenceList.stream()
          .filter(cache.getEnabledLiveInstances()::contains)
          .count();
    } else {
      liveCount = cache.getEnabledLiveInstances().size();
    }

    return stateModelDef.getStateCountMap(liveCount, requiredReplicas);
  }

  /**
   * Data class to track throttle metrics per resource.
   */
  public static class ResourceThrottleMetrics {
    public final Set<String> recoveryMessages = new HashSet<>();
    public final Set<String> loadMessages = new HashSet<>();
    public final Set<String> throttledRecovery = new HashSet<>();
    public final Set<String> throttledLoad = new HashSet<>();
    public Set<Partition> errorPartitions = new HashSet<>();

    void recordMessage(RebalanceType type, String messageId) {
      if (type == RebalanceType.RECOVERY_BALANCE) {
        recoveryMessages.add(messageId);
      } else {
        loadMessages.add(messageId);
      }
    }

    void recordThrottled(RebalanceType type, String messageId) {
      if (type == RebalanceType.RECOVERY_BALANCE) {
        throttledRecovery.add(messageId);
      } else {
        throttledLoad.add(messageId);
      }
    }
  }

  /**
   * Comparator for ordering pending messages by priority.
   */
  private static class MessagePriorityComparator implements java.util.Comparator<Message> {
    private final Map<String, Integer> preferenceInstanceMap;
    private final Map<String, Integer> statePriorityMap;

    MessagePriorityComparator(List<String> preferenceList, Map<String, Integer> statePriorityMap) {
      this.preferenceInstanceMap = IntStream.range(0, preferenceList.size()).boxed()
          .collect(Collectors.toMap(preferenceList::get, i -> i));
      this.statePriorityMap = statePriorityMap;
    }

    @Override
    public int compare(Message m1, Message m2) {
      if (!m1.getToState().equals(m2.getToState())) {
        return statePriorityMap.get(m1.getToState()).compareTo(statePriorityMap.get(m2.getToState()));
      }
      if (preferenceInstanceMap.containsKey(m1.getTgtName())
          && preferenceInstanceMap.containsKey(m2.getTgtName())) {
        return preferenceInstanceMap.get(m1.getTgtName())
            .compareTo(preferenceInstanceMap.get(m2.getTgtName()));
      }
      return m1.getTgtName().compareTo(m2.getTgtName());
    }
  }
}

