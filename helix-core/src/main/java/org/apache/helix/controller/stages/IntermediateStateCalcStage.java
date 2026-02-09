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
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import org.apache.helix.HelixDefinedState;
import org.apache.helix.HelixException;
import org.apache.helix.HelixManager;
import org.apache.helix.api.config.StateTransitionThrottleConfig;
import org.apache.helix.api.config.StateTransitionThrottleConfig.RebalanceType;
import org.apache.helix.controller.LogUtil;
import org.apache.helix.controller.common.PartitionStateMap;
import org.apache.helix.controller.dataproviders.ResourceControllerDataProvider;
import org.apache.helix.controller.pipeline.AbstractBaseStage;
import org.apache.helix.controller.pipeline.StageException;
import org.apache.helix.model.BuiltInStateModelDefinitions;
import org.apache.helix.model.ClusterConfig;
import org.apache.helix.model.IdealState;
import org.apache.helix.model.MaintenanceSignal;
import org.apache.helix.model.Message;
import org.apache.helix.model.Partition;
import org.apache.helix.model.Resource;
import org.apache.helix.model.StateModelDefinition;
import org.apache.helix.monitoring.mbeans.ClusterStatusMonitor;
import org.apache.helix.monitoring.mbeans.ResourceMonitor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Computes the Intermediate State for partitions based on BestPossibleState and CurrentState,
 * applying throttling constraints to state transitions.
 *
 * <p>Supports two computation modes:
 * <ul>
 *   <li>Resource-priority: Processes resources in priority order (default)</li>
 *   <li>Availability-aware: Prioritizes messages by availability impact across all resources</li>
 * </ul>
 */
public class IntermediateStateCalcStage extends AbstractBaseStage {
  private static final Logger logger = LoggerFactory.getLogger(IntermediateStateCalcStage.class.getName());

  @Override
  public void process(ClusterEvent event) throws Exception {
    _eventId = event.getEventId();

    PipelineInput input = extractPipelineInput(event);
    validateInput(input, event);

    IntermediateStateOutput output = computeIntermediateState(event, input);
    event.addAttribute(AttributeName.INTERMEDIATE_STATE.name(), output);

    validateMaxPartitionsPerInstance(event, input.cache, output);
  }

  // ========================================
  // Input Extraction & Validation
  // ========================================

  private PipelineInput extractPipelineInput(ClusterEvent event) {
    return new PipelineInput(
        event.getAttribute(AttributeName.CURRENT_STATE.name()),
        event.getAttribute(AttributeName.BEST_POSSIBLE_STATE.name()),
        event.getAttribute(AttributeName.RESOURCES_TO_REBALANCE.name()),
        event.getAttribute(AttributeName.ControllerDataProvider.name()),
        event.getAttribute(AttributeName.MESSAGES_SELECTED.name()),
        event.getAttribute(AttributeName.clusterStatusMonitor.name())
    );
  }

  private void validateInput(PipelineInput input, ClusterEvent event) throws StageException {
    if (input.currentStateOutput == null || input.bestPossibleStateOutput == null
        || input.resourceMap == null || input.cache == null || input.messageOutput == null) {
      throw new StageException(String.format(
          "Missing required attributes in event: %s. Requires CURRENT_STATE (%s) | BEST_POSSIBLE_STATE (%s) | "
          + "RESOURCES (%s) | MESSAGE_SELECT (%s) | DataCache (%s)",
          event, input.currentStateOutput, input.bestPossibleStateOutput,
          input.resourceMap, input.messageOutput, input.cache));
    }
  }

  // ========================================
  // Main Computation Logic
  // ========================================

  private IntermediateStateOutput computeIntermediateState(ClusterEvent event, PipelineInput input) {
    boolean useAvailabilityAware = input.cache.getClusterConfig().isAvailabilityAwarePrioritizationEnabled();

    ComputationContext context = new ComputationContext(input, _eventId);

    if (useAvailabilityAware) {
      LogUtil.logInfo(logger, _eventId, "Using availability-aware prioritization");
      return context.computeWithAvailabilityAwarePriority();
    } else {
      return context.computeWithResourcePriority();
    }
  }

  // ========================================
  // Unified Computation Context
  // ========================================

  /**
   * Shared computation context for both resource-priority and availability-aware modes.
   * Both modes use the same message processing and throttling logic, differing only in
   * the order in which messages are processed.
   */
  private class ComputationContext {
    private final PipelineInput input;
    private final String eventId;
    private final StateTransitionThrottleController throttleController;
    private final IntermediateStateOutput output;
    private final List<String> failedResources;
    private final Map<String, ResourceThrottleMetrics> metricsPerResource;

    ComputationContext(PipelineInput input, String eventId) {
      this.input = input;
      this.eventId = eventId;
      this.throttleController = new StateTransitionThrottleController(
          input.resourceMap.keySet(), input.cache.getClusterConfig(), input.cache.getLiveInstances().keySet());
      this.output = new IntermediateStateOutput();
      this.failedResources = new ArrayList<>();
      this.metricsPerResource = new HashMap<>();

      // Initialize metrics for all resources
      for (String resourceName : input.resourceMap.keySet()) {
        metricsPerResource.put(resourceName, new ResourceThrottleMetrics());
      }
    }

    // ========================================
    // Resource-Priority Mode
    // ========================================

    IntermediateStateOutput computeWithResourcePriority() {
      List<ResourcePriority> prioritizedResources = prioritizeResources();

      for (ResourcePriority resourcePriority : prioritizedResources) {
        processResource(resourcePriority.getResourceName());
      }

      updateMonitoring();
      return output;
    }

    private List<ResourcePriority> prioritizeResources() {
      List<ResourcePriority> priorities = input.resourceMap.keySet().stream()
          .map(ResourcePriority::new)
          .collect(Collectors.toList());

      String priorityField = input.cache.getClusterConfig().getResourcePriorityField();
      if (priorityField != null) {
        priorities.forEach(rp -> assignPriority(rp, priorityField));
        Collections.sort(priorities);
      }

      return priorities;
    }

    private void assignPriority(ResourcePriority resourcePriority, String priorityField) {
      String resourceName = resourcePriority.getResourceName();

      String priority = getResourceConfigPriority(resourceName, priorityField);
      if (priority == null) {
        priority = getIdealStatePriority(resourceName, priorityField);
      }

      if (priority != null) {
        resourcePriority.setPriority(priority);
      }
    }

    private String getResourceConfigPriority(String resourceName, String priorityField) {
      if (input.cache.getResourceConfig(resourceName) != null) {
        return input.cache.getResourceConfig(resourceName).getSimpleConfig(priorityField);
      }
      return null;
    }

    private String getIdealStatePriority(String resourceName, String priorityField) {
      IdealState idealState = input.cache.getIdealState(resourceName);
      if (idealState != null) {
        return idealState.getRecord().getSimpleField(priorityField);
      }
      return null;
    }

    private void processResource(String resourceName) {
      if (!input.bestPossibleStateOutput.containsResource(resourceName)) {
        LogUtil.logInfo(logger, eventId,
            "Skipping resource " + resourceName + ": no best possible state available");
        return;
      }

      Resource resource = input.resourceMap.get(resourceName);
      IdealState idealState = getOrCreateIdealState(resourceName, resource);

      try {
        PartitionStateMap intermediateState = computeResourceIntermediateState(
            resource, idealState, resourceName);
        output.setState(resourceName, intermediateState);
      } catch (HelixException ex) {
        LogUtil.logInfo(logger, eventId,
            "Failed to calculate intermediate partition states for resource " + resourceName, ex);
        failedResources.add(resourceName);
      }
    }

    private IdealState getOrCreateIdealState(String resourceName, Resource resource) {
      IdealState idealState = input.cache.getIdealState(resourceName);
      if (idealState == null) {
        LogUtil.logInfo(logger, eventId,
            "IdealState for " + resourceName + " does not exist, creating default");
        idealState = new IdealState(resourceName);
        idealState.setStateModelDefRef(resource.getStateModelDefRef());
      }
      return idealState;
    }

    private PartitionStateMap computeResourceIntermediateState(Resource resource,
        IdealState idealState, String resourceName) {

      Map<Partition, List<Message>> resourceMessages = input.messageOutput.getResourceMessageMap(resourceName);

      if (!isThrottlingNeeded(idealState, resourceMessages)) {
        return input.bestPossibleStateOutput.getPartitionStateMap(resourceName);
      }

      ResourceThrottleContext context = new ResourceThrottleContext(
          resource, idealState, resourceMessages, throttleController, input, metricsPerResource.get(resourceName));

      return context.computeThrottledState();
    }

    private boolean isThrottlingNeeded(IdealState idealState, Map<Partition, List<Message>> messages) {
      return IdealState.RebalanceMode.FULL_AUTO.equals(idealState.getRebalanceMode())
          && messages != null && !messages.isEmpty();
    }

    // ========================================
    // Availability-Aware Mode
    // ========================================

    IntermediateStateOutput computeWithAvailabilityAwarePriority() {
      Map<String, Map<Partition, List<Message>>> approvedMessages = new HashMap<>();
      for (String resourceName : input.resourceMap.keySet()) {
        approvedMessages.put(resourceName, new HashMap<>());
      }

      List<MessageContext> allMessages = collectAllMessages();
      sortByAvailabilityImpact(allMessages);
      processMessagesInOrder(allMessages, approvedMessages);
      buildIntermediateStates(approvedMessages);
      updateMonitoring();
      return output;
    }

    private List<MessageContext> collectAllMessages() {
      List<MessageContext> messages = new ArrayList<>();

      for (Map.Entry<String, Resource> entry : input.resourceMap.entrySet()) {
        String resourceName = entry.getKey();
        Resource resource = entry.getValue();

        ResourceMessageCollector collector = new ResourceMessageCollector(
            resource, resourceName, input, throttleController, eventId);

        List<MessageContext> resourceMessages = collector.collect();
        if (resourceMessages != null) {
          messages.addAll(resourceMessages);
        } else {
          // Use best possible state directly for non-FULL_AUTO or empty messages
          if (input.bestPossibleStateOutput.containsResource(resourceName)) {
            output.setState(resourceName, input.bestPossibleStateOutput.getPartitionStateMap(resourceName));
          }
        }
      }

      return messages;
    }

    private void sortByAvailabilityImpact(List<MessageContext> messages) {
      AvailabilityAwareMessageComparator comparator =
          new AvailabilityAwareMessageComparator(input.cache, input.currentStateOutput);
      comparator.setEventId(eventId);

      messages.sort((m1, m2) -> comparator.compare(m1.message, m2.message));
    }

    private void processMessagesInOrder(List<MessageContext> messages,
        Map<String, Map<Partition, List<Message>>> approvedMessages) {
      Map<String, Map<String, String>> derivedStates = new HashMap<>();

      for (MessageContext ctx : messages) {
        Message message = ctx.message;
        String resourceName = message.getResourceName();
        Partition partition = ctx.partition;

        Map<String, String> derivedState = derivedStates.computeIfAbsent(
            resourceName + ":" + partition.getPartitionName(),
            k -> new HashMap<>(input.currentStateOutput.getCurrentStateMap(resourceName, partition)));

        processMessageWithThrottling(ctx, derivedState, approvedMessages);
      }
    }

    private void processMessageWithThrottling(MessageContext ctx, Map<String, String> derivedState,
        Map<String, Map<Partition, List<Message>>> approvedMessages) {
      Message message = ctx.message;
      String resourceName = message.getResourceName();
      Partition partition = ctx.partition;

      RebalanceType type = MessageClassifier.classify(ctx.requiredStates, message, derivedState);
      message.setSTRebalanceType(type == RebalanceType.RECOVERY_BALANCE
          ? Message.STRebalanceType.RECOVERY_REBALANCE
          : Message.STRebalanceType.LOAD_REBALANCE);

      ResourceThrottleMetrics metrics = metricsPerResource.get(resourceName);
      metrics.recordMessage(type, message.getId());

      int errorThreshold = ThrottleHelper.getErrorThreshold(input.cache.getClusterConfig());
      boolean throttled = MessageThrottler.shouldThrottle(message, resourceName, partition, type,
          ctx.stateModelDef, throttleController, metrics.errorPartitions.size(), errorThreshold, input.cache);

      if (throttled) {
        metrics.recordThrottled(type, message.getId());
      } else {
        derivedState.put(message.getTgtName(), message.getToState());
        approvedMessages.get(resourceName)
            .computeIfAbsent(partition, k -> new ArrayList<>())
            .add(message);
      }
    }

    private void buildIntermediateStates(Map<String, Map<Partition, List<Message>>> approvedMessages) {
      for (String resourceName : input.resourceMap.keySet()) {
        if (!shouldBuildIntermediateState(resourceName)) {
          continue;
        }

        try {
          PartitionStateMap intermediateState = IntermediateStateBuilder.build(
              resourceName, input.currentStateOutput, approvedMessages.get(resourceName));
          output.setState(resourceName, intermediateState);
        } catch (HelixException ex) {
          LogUtil.logInfo(logger, eventId, "Failed to compute intermediate state for " + resourceName, ex);
          failedResources.add(resourceName);
        }
      }
    }

    private boolean shouldBuildIntermediateState(String resourceName) {
      if (!input.bestPossibleStateOutput.containsResource(resourceName)) {
        return false;
      }
      IdealState idealState = input.cache.getIdealState(resourceName);
      if (idealState != null && !IdealState.RebalanceMode.FULL_AUTO.equals(idealState.getRebalanceMode())) {
        return false;
      }
      Map<Partition, List<Message>> messages = input.messageOutput.getResourceMessageMap(resourceName);
      return messages != null && !messages.isEmpty();
    }

    // ========================================
    // Shared Monitoring
    // ========================================

    private void updateMonitoring() {
      if (input.monitor == null) {
        return;
      }

      input.monitor.setResourceRebalanceStates(
          failedResources, ResourceMonitor.RebalanceStatus.INTERMEDIATE_STATE_CAL_FAILED);
      input.monitor.setResourceRebalanceStates(
          output.resourceSet(), ResourceMonitor.RebalanceStatus.NORMAL);

      int errorThreshold = ThrottleHelper.getErrorThreshold(input.cache.getClusterConfig());
      for (Map.Entry<String, ResourceThrottleMetrics> entry : metricsPerResource.entrySet()) {
        ResourceThrottleMetrics metrics = entry.getValue();
        input.monitor.updateRebalancerStats(entry.getKey(),
            metrics.recoveryMessages.size(), metrics.loadMessages.size(),
            metrics.throttledRecovery.size(), metrics.throttledLoad.size(),
            metrics.errorPartitions.size() > errorThreshold);
      }
    }
  }

  // ========================================
  // Resource Throttle Context (for resource-priority mode)
  // ========================================

  private class ResourceThrottleContext {
    private final Resource resource;
    private final IdealState idealState;
    private final Map<Partition, List<Message>> resourceMessages;
    private final StateTransitionThrottleController throttleController;
    private final PipelineInput input;

    private final StateModelDefinition stateModelDef;
    private final Map<String, List<String>> preferenceLists;
    private final Set<Partition> errorPartitions;
    private final int errorThreshold;
    private final ResourceThrottleMetrics metrics;

    ResourceThrottleContext(Resource resource, IdealState idealState,
        Map<Partition, List<Message>> resourceMessages,
        StateTransitionThrottleController throttleController, PipelineInput input,
        ResourceThrottleMetrics metrics) {

      this.resource = resource;
      this.idealState = idealState;
      this.resourceMessages = resourceMessages;
      this.throttleController = throttleController;
      this.input = input;
      this.metrics = metrics;

      this.stateModelDef = input.cache.getStateModelDef(idealState.getStateModelDefRef());
      this.preferenceLists = input.bestPossibleStateOutput.getPreferenceLists(resource.getResourceName());
      this.errorPartitions = findErrorPartitions();
      this.errorThreshold = ThrottleHelper.getErrorThreshold(input.cache.getClusterConfig());
      metrics.errorPartitions = errorPartitions;
    }

    PartitionStateMap computeThrottledState() {
      LogUtil.logDebug(logger, _eventId, "Processing resource: " + resource.getResourceName());

      ThrottleHelper.chargePendingTransitions(resource, input.currentStateOutput,
          throttleController, input.cache, preferenceLists, stateModelDef);

      List<Partition> sortedPartitions = sortPartitionsByPriority();
      processPartitions(sortedPartitions);

      PartitionStateMap intermediateState = IntermediateStateBuilder.build(
          resource.getResourceName(), input.currentStateOutput, resourceMessages);

      logMetrics();
      updateMonitoringForResource();

      LogUtil.logDebug(logger, _eventId, "End processing resource: " + resource.getResourceName());
      return intermediateState;
    }

    private Set<Partition> findErrorPartitions() {
      Set<Partition> errors = new HashSet<>();
      Map<Partition, Map<String, String>> currentStates =
          input.currentStateOutput.getCurrentStateMap(resource.getResourceName());

      for (Map.Entry<Partition, Map<String, String>> entry : currentStates.entrySet()) {
        if (entry.getValue().containsValue(HelixDefinedState.ERROR.name())) {
          errors.add(entry.getKey());
        }
      }
      return errors;
    }

    private List<Partition> sortPartitionsByPriority() {
      List<Partition> partitions = new ArrayList<>(resource.getPartitions());
      PartitionStateMap bestPossibleState = input.bestPossibleStateOutput.getPartitionStateMap(resource.getResourceName());
      Map<Partition, Map<String, String>> currentStates = input.currentStateOutput.getCurrentStateMap(resource.getResourceName());

      partitions.sort(new PartitionPriorityComparator(
          bestPossibleState.getStateMap(), currentStates, stateModelDef.getTopState()));

      return partitions;
    }

    private void processPartitions(List<Partition> partitions) {
      for (Partition partition : partitions) {
        List<Message> messages = resourceMessages.get(partition);
        if (messages == null || messages.isEmpty()) {
          continue;
        }

        processPartitionMessages(partition, new ArrayList<>(messages));
      }
    }

    private void processPartitionMessages(Partition partition, List<Message> messages) {
      Map<String, String> derivedState = new HashMap<>(
          input.currentStateOutput.getCurrentStateMap(resource.getResourceName(), partition));

      List<String> preferenceList = preferenceLists.get(partition.getPartitionName());
      Map<String, Integer> requiredStates = ThrottleHelper.getRequiredStates(
          resource.getResourceName(), input.cache, preferenceList);

      if (preferenceList != null && !preferenceList.isEmpty()) {
        messages.sort(new MessagePriorityComparator(preferenceList, stateModelDef.getStatePriorityMap()));
      }

      for (Message message : messages) {
        processMessage(partition, message, derivedState, requiredStates);
      }
    }

    private void processMessage(Partition partition, Message message,
        Map<String, String> derivedState, Map<String, Integer> requiredStates) {

      RebalanceType type = MessageClassifier.classify(requiredStates, message, derivedState);
      message.setSTRebalanceType(type == RebalanceType.RECOVERY_BALANCE
          ? Message.STRebalanceType.RECOVERY_REBALANCE
          : Message.STRebalanceType.LOAD_REBALANCE);

      metrics.recordMessage(type, message.getId());

      boolean throttled = MessageThrottler.shouldThrottle(message, resource.getResourceName(),
          partition, type, stateModelDef, throttleController, errorPartitions.size(),
          errorThreshold, input.cache);

      if (throttled) {
        resourceMessages.get(partition).remove(message);
        metrics.recordThrottled(type, message.getId());
      } else {
        derivedState.put(message.getTgtName(), message.getToState());
      }
    }

    private void logMetrics() {
      if (!metrics.recoveryMessages.isEmpty()) {
        LogUtil.logInfo(logger, _eventId, String.format(
            "Recovery balance needed for %s with messages: %s",
            resource.getResourceName(), metrics.recoveryMessages));
      }
      if (!metrics.loadMessages.isEmpty()) {
        LogUtil.logInfo(logger, _eventId, String.format(
            "Load balance needed for %s with messages: %s",
            resource.getResourceName(), metrics.loadMessages));
      }
      if (!errorPartitions.isEmpty()) {
        LogUtil.logInfo(logger, _eventId, String.format(
            "Partition currently has an ERROR replica in %s partitions: %s",
            resource.getResourceName(), errorPartitions));
      }
    }

    private void updateMonitoringForResource() {
      if (input.monitor != null) {
        input.monitor.updateRebalancerStats(resource.getResourceName(),
            metrics.recoveryMessages.size(), metrics.loadMessages.size(),
            metrics.throttledRecovery.size(), metrics.throttledLoad.size(),
            errorPartitions.size() > errorThreshold);
      }
    }
  }

  // ========================================
  // Resource Message Collector (for availability-aware mode)
  // ========================================

  private static class ResourceMessageCollector {
    private final Resource resource;
    private final String resourceName;
    private final PipelineInput input;
    private final StateTransitionThrottleController throttleController;
    private final String eventId;

    ResourceMessageCollector(Resource resource, String resourceName, PipelineInput input,
        StateTransitionThrottleController throttleController, String eventId) {
      this.resource = resource;
      this.resourceName = resourceName;
      this.input = input;
      this.throttleController = throttleController;
      this.eventId = eventId;
    }

    List<MessageContext> collect() {
      if (!input.bestPossibleStateOutput.containsResource(resourceName)) {
        LogUtil.logInfo(logger, eventId, "Skipping resource " + resourceName + ": no best possible state");
        return null;
      }

      IdealState idealState = getOrCreateIdealState();

      if (!IdealState.RebalanceMode.FULL_AUTO.equals(idealState.getRebalanceMode())) {
        return null;
      }

      Map<Partition, List<Message>> resourceMessages = input.messageOutput.getResourceMessageMap(resourceName);
      if (resourceMessages == null || resourceMessages.isEmpty()) {
        return null;
      }

      StateModelDefinition stateModelDef = input.cache.getStateModelDef(idealState.getStateModelDefRef());
      Map<String, List<String>> preferenceLists = input.bestPossibleStateOutput.getPreferenceLists(resourceName);

      ThrottleHelper.chargePendingTransitions(resource, input.currentStateOutput,
          throttleController, input.cache, preferenceLists, stateModelDef);

      return collectMessagesWithContext(resourceMessages, preferenceLists, stateModelDef);
    }

    private IdealState getOrCreateIdealState() {
      IdealState idealState = input.cache.getIdealState(resourceName);
      if (idealState == null) {
        LogUtil.logInfo(logger, eventId, "IdealState not found for " + resourceName + ", creating default");
        idealState = new IdealState(resourceName);
        idealState.setStateModelDefRef(resource.getStateModelDefRef());
      }
      return idealState;
    }

    private List<MessageContext> collectMessagesWithContext(
        Map<Partition, List<Message>> resourceMessages,
        Map<String, List<String>> preferenceLists,
        StateModelDefinition stateModelDef) {

      List<MessageContext> messages = new ArrayList<>();

      for (Map.Entry<Partition, List<Message>> entry : resourceMessages.entrySet()) {
        Partition partition = entry.getKey();
        List<String> preferenceList = preferenceLists.get(partition.getPartitionName());
        Map<String, Integer> requiredStates = ThrottleHelper.getRequiredStates(
            resourceName, input.cache, preferenceList);

        for (Message message : entry.getValue()) {
          messages.add(new MessageContext(message, partition, stateModelDef, requiredStates));
        }
      }

      return messages;
    }
  }

  // ========================================
  // Helper Classes - Throttling
  // ========================================

  private static class MessageThrottler {
    static boolean shouldThrottle(Message message, String resourceName, Partition partition,
        RebalanceType type, StateModelDefinition stateModelDef,
        StateTransitionThrottleController throttleController,
        int numErrorPartitions, int errorThreshold, ResourceControllerDataProvider cache) {

      // Check downward transition rule for load balance
      if (type == RebalanceType.LOAD_BALANCE && numErrorPartitions > errorThreshold) {
        if (!isDownwardTransition(message, stateModelDef)) {
          return true;
        }
      }

      // Check resource quota
      if (throttleController.shouldThrottleForResource(type, resourceName)) {
        return true;
      }

      // Check instance quota
      String instance = message.getTgtName();
      if (!cache.getDisabledInstancesForPartition(resourceName, partition.getPartitionName()).contains(instance)) {
        if (throttleController.shouldThrottleForInstance(type, instance)) {
          return true;
        }
      }

      // Not throttled - charge quotas
      throttleController.chargeCluster(type);
      throttleController.chargeResource(type, resourceName);
      throttleController.chargeInstance(type, instance);

      return false;
    }

    private static boolean isDownwardTransition(Message message, StateModelDefinition stateModelDef) {
      return StateTransitionHelper.isDownwardTransition(
          message.getFromState(), message.getToState(), stateModelDef);
    }
  }

  private static class MessageClassifier {
    static RebalanceType classify(Map<String, Integer> requiredStates, Message message,
        Map<String, String> currentStates) {

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
  }

  private static class ThrottleHelper {
    static int getErrorThreshold(ClusterConfig config) {
      if (config.getErrorOrRecoveryPartitionThresholdForLoadBalance() != -1) {
        return config.getErrorOrRecoveryPartitionThresholdForLoadBalance();
      }
      if (config.getErrorPartitionThresholdForLoadBalance() != 0) {
        return config.getErrorPartitionThresholdForLoadBalance();
      }
      return 1;
    }

    static void chargePendingTransitions(Resource resource, CurrentStateOutput currentStateOutput,
        StateTransitionThrottleController throttleController, ResourceControllerDataProvider cache,
        Map<String, List<String>> preferenceLists, StateModelDefinition stateModelDef) {

      String resourceName = resource.getResourceName();

      for (Partition partition : resource.getPartitions()) {
        Map<String, Integer> requiredStates = getRequiredStates(
            resourceName, cache, preferenceLists.get(partition.getPartitionName()));
        Map<String, String> currentStateMap = currentStateOutput.getCurrentStateMap(resourceName, partition);

        List<Message> pendingMessages = new ArrayList<>(
            currentStateOutput.getPendingMessageMap(resourceName, partition).values());

        List<String> preferenceList = preferenceLists.get(partition.getPartitionName());
        if (preferenceList != null && !preferenceList.isEmpty()) {
          pendingMessages.sort(new MessagePriorityComparator(preferenceList, stateModelDef.getStatePriorityMap()));
        }

        for (Message message : pendingMessages) {
          chargePendingMessage(message, requiredStates, currentStateMap, stateModelDef,
              throttleController, cache, resourceName, partition);
        }
      }
    }

    private static void chargePendingMessage(Message message, Map<String, Integer> requiredStates,
        Map<String, String> currentStateMap, StateModelDefinition stateModelDef,
        StateTransitionThrottleController throttleController, ResourceControllerDataProvider cache,
        String resourceName, Partition partition) {

      RebalanceType type = MessageClassifier.classify(requiredStates, message, currentStateMap);
      String currentState = currentStateMap.getOrDefault(message.getTgtName(), stateModelDef.getInitialState());

      if (!message.getToState().equals(currentState)
          && message.getFromState().equals(currentState)
          && !cache.getDisabledInstancesForPartition(resourceName, partition.getPartitionName())
              .contains(message.getTgtName())) {

        throttleController.chargeInstance(type, message.getTgtName());
        throttleController.chargeResource(type, resourceName);
        throttleController.chargeCluster(type);
      }
    }

    static Map<String, Integer> getRequiredStates(String resourceName,
        ResourceControllerDataProvider cache, List<String> preferenceList) {

      IdealState idealState = cache.getIdealState(resourceName);
      StateModelDefinition stateModelDef = cache.getStateModelDef(idealState.getStateModelDefRef());

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
  }

  private static class IntermediateStateBuilder {
    static PartitionStateMap build(String resourceName, CurrentStateOutput currentStateOutput,
        Map<Partition, List<Message>> messages) {

      PartitionStateMap intermediateState = new PartitionStateMap(
          resourceName, currentStateOutput.getCurrentStateMap(resourceName));

      // Apply pending messages
      Map<Partition, Map<String, Message>> pendingMessages =
          currentStateOutput.getPendingMessageMap(resourceName);
      if (pendingMessages != null) {
        applyMessages(intermediateState, pendingMessages);
      }

      // Apply approved messages
      if (messages != null) {
        applyMessagesList(intermediateState, messages);
      }

      return intermediateState;
    }

    private static void applyMessages(PartitionStateMap stateMap,
        Map<Partition, Map<String, Message>> messages) {
      for (Map.Entry<Partition, Map<String, Message>> entry : messages.entrySet()) {
        for (Map.Entry<String, Message> msgEntry : entry.getValue().entrySet()) {
          Message msg = msgEntry.getValue();
          if (msg != null && msg.getToState() != null) {
            if (!msg.getToState().equals(HelixDefinedState.DROPPED.name())) {
              stateMap.setState(entry.getKey(), msgEntry.getKey(), msg.getToState());
            } else if (stateMap.getStateMap().containsKey(entry.getKey())) {
              stateMap.getStateMap().get(entry.getKey()).remove(msgEntry.getKey());
            }
          }
        }
      }
    }

    private static void applyMessagesList(PartitionStateMap stateMap,
        Map<Partition, List<Message>> messages) {
      for (Map.Entry<Partition, List<Message>> entry : messages.entrySet()) {
        for (Message msg : entry.getValue()) {
          if (msg != null && msg.getTgtName() != null && msg.getToState() != null) {
            if (!msg.getToState().equals(HelixDefinedState.DROPPED.name())) {
              stateMap.setState(entry.getKey(), msg.getTgtName(), msg.getToState());
            } else if (stateMap.getStateMap().containsKey(entry.getKey())) {
              stateMap.getStateMap().get(entry.getKey()).remove(msg.getTgtName());
            }
          }
        }
      }
    }
  }

  // ========================================
  // Validation
  // ========================================

  private void validateMaxPartitionsPerInstance(ClusterEvent event, ResourceControllerDataProvider cache,
      IntermediateStateOutput output) {

    int maxPartitionPerInstance = cache.getClusterConfig().getMaxPartitionsPerInstance();
    if (maxPartitionPerInstance <= 0) {
      return;
    }

    Map<String, Integer> instancePartitionCounts = new HashMap<>();

    for (Map.Entry<String, PartitionStateMap> entry : output.getResourceStatesMap().entrySet()) {
      String resourceName = entry.getKey();

      if (shouldSkipResourceValidation(resourceName, cache)) {
        continue;
      }

      validateResourceAssignments(resourceName, entry.getValue(), instancePartitionCounts,
          maxPartitionPerInstance, event, cache);
    }
  }

  private boolean shouldSkipResourceValidation(String resourceName, ResourceControllerDataProvider cache) {
    IdealState idealState = cache.getIdealState(resourceName);
    return idealState != null
        && idealState.getStateModelDefRef().equals(BuiltInStateModelDefinitions.Task.name());
  }

  private void validateResourceAssignments(String resourceName, PartitionStateMap partitionStateMap,
      Map<String, Integer> instanceCounts, int maxPartitionPerInstance,
      ClusterEvent event, ResourceControllerDataProvider cache) {

    for (Map.Entry<Partition, Map<String, String>> partitionEntry : partitionStateMap.getStateMap().entrySet()) {
      for (Map.Entry<String, String> instanceEntry : partitionEntry.getValue().entrySet()) {
        String instance = instanceEntry.getKey();
        String state = instanceEntry.getValue();

        if (state.equals(HelixDefinedState.DROPPED.name())) {
          continue;
        }

        int count = instanceCounts.getOrDefault(instance, 0) + 1;
        instanceCounts.put(instance, count);

        if (count > maxPartitionPerInstance) {
          handleMaxPartitionViolation(instance, maxPartitionPerInstance, resourceName, event, cache);
        }
      }
    }
  }

  private void handleMaxPartitionViolation(String instance, int maxPartitionPerInstance,
      String resourceName, ClusterEvent event, ResourceControllerDataProvider cache) {

    String errorMsg = String.format(
        "Problem: instance %s contains more replicas/partitions (%d) than maximum allowed (%d). "
        + "Putting cluster %s into maintenance mode",
        instance, maxPartitionPerInstance + 1, maxPartitionPerInstance, cache.getClusterName());

    HelixManager manager = event.getAttribute(AttributeName.helixmanager.name());
    if (manager != null) {
      if (manager.getHelixDataAccessor().getProperty(
          manager.getHelixDataAccessor().keyBuilder().maintenance()) == null) {
        manager.getClusterManagmentTool().autoEnableMaintenanceMode(
            manager.getClusterName(), true, errorMsg,
            MaintenanceSignal.AutoTriggerReason.MAX_PARTITION_PER_INSTANCE_EXCEEDED);
      }
      LogUtil.logWarn(logger, _eventId, errorMsg);
    } else {
      LogUtil.logError(logger, _eventId,
          "HelixManager is null! Failed to enable maintenance mode due to max partition violation.");
    }

    ClusterStatusMonitor monitor = event.getAttribute(AttributeName.clusterStatusMonitor.name());
    if (monitor != null) {
      monitor.setResourceRebalanceStates(
          Collections.singletonList(resourceName),
          ResourceMonitor.RebalanceStatus.INTERMEDIATE_STATE_CAL_FAILED);
    }

    throw new HelixException(errorMsg);
  }

  // ========================================
  // Data Classes
  // ========================================

  private static class PipelineInput {
    final CurrentStateOutput currentStateOutput;
    final BestPossibleStateOutput bestPossibleStateOutput;
    final Map<String, Resource> resourceMap;
    final ResourceControllerDataProvider cache;
    final MessageOutput messageOutput;
    final ClusterStatusMonitor monitor;

    PipelineInput(CurrentStateOutput currentStateOutput, BestPossibleStateOutput bestPossibleStateOutput,
        Map<String, Resource> resourceMap, ResourceControllerDataProvider cache,
        MessageOutput messageOutput, ClusterStatusMonitor monitor) {
      this.currentStateOutput = currentStateOutput;
      this.bestPossibleStateOutput = bestPossibleStateOutput;
      this.resourceMap = resourceMap;
      this.cache = cache;
      this.messageOutput = messageOutput;
      this.monitor = monitor;
    }
  }

  private static class ResourceThrottleMetrics {
    final Set<String> recoveryMessages = new HashSet<>();
    final Set<String> loadMessages = new HashSet<>();
    final Set<String> throttledRecovery = new HashSet<>();
    final Set<String> throttledLoad = new HashSet<>();
    Set<Partition> errorPartitions = new HashSet<>();

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

  // ========================================
  // Comparators
  // ========================================

  private static class ResourcePriority implements Comparable<ResourcePriority> {
    private final String resourceName;
    private int priority = Integer.MIN_VALUE;

    ResourcePriority(String resourceName) {
      this.resourceName = resourceName;
    }

    @Override
    public int compareTo(ResourcePriority other) {
      return Integer.compare(other.priority, this.priority);
    }

    String getResourceName() {
      return resourceName;
    }

    void setPriority(String priorityStr) {
      try {
        this.priority = Integer.parseInt(priorityStr);
      } catch (Exception e) {
        logger.warn("Invalid priority field {} for resource {}", priorityStr, resourceName);
      }
    }
  }

  private static class MessagePriorityComparator implements Comparator<Message> {
    private final Map<String, Integer> preferenceInstanceMap;
    private final Map<String, Integer> statePriorityMap;

    MessagePriorityComparator(List<String> preferenceList, Map<String, Integer> statePriorityMap) {
      this.preferenceInstanceMap = IntStream.range(0, preferenceList.size()).boxed()
          .collect(Collectors.toMap(preferenceList::get, i -> i));
      this.statePriorityMap = statePriorityMap;
    }

    @Override
    public int compare(Message m1, Message m2) {
      // Compare by state priority first
      if (!m1.getToState().equals(m2.getToState())) {
        return statePriorityMap.get(m1.getToState()).compareTo(statePriorityMap.get(m2.getToState()));
      }

      // Then by preference list order
      if (preferenceInstanceMap.containsKey(m1.getTgtName())
          && preferenceInstanceMap.containsKey(m2.getTgtName())) {
        return preferenceInstanceMap.get(m1.getTgtName()).compareTo(preferenceInstanceMap.get(m2.getTgtName()));
      }

      // Finally by instance name for deterministic ordering
      return m1.getTgtName().compareTo(m2.getTgtName());
    }
  }

  private static class PartitionPriorityComparator implements Comparator<Partition> {
    private final Map<Partition, Map<String, String>> bestPossibleMap;
    private final Map<Partition, Map<String, String>> currentStateMap;
    private final String topState;

    PartitionPriorityComparator(Map<Partition, Map<String, String>> bestPossibleMap,
        Map<Partition, Map<String, String>> currentStateMap, String topState) {
      this.bestPossibleMap = bestPossibleMap;
      this.currentStateMap = currentStateMap;
      this.topState = topState;
    }

    @Override
    public int compare(Partition p1, Partition p2) {
      // Priority 1: Partitions missing top state
      int missTopState1 = hasMissingTopState(p1) ? 0 : 1;
      int missTopState2 = hasMissingTopState(p2) ? 0 : 1;
      if (missTopState1 != missTopState2) {
        return Integer.compare(missTopState1, missTopState2);
      }

      // Priority 2: Fewer active replicas
      int active1 = countActiveReplicas(p1);
      int active2 = countActiveReplicas(p2);
      if (active1 != active2) {
        return Integer.compare(active1, active2);
      }

      // Priority 3: Fewer ideal state matches
      int matched1 = countIdealStateMatches(p1);
      int matched2 = countIdealStateMatches(p2);
      if (matched1 != matched2) {
        return Integer.compare(matched1, matched2);
      }

      // Deterministic fallback
      return p1.getPartitionName().compareTo(p2.getPartitionName());
    }

    private boolean hasMissingTopState(Partition partition) {
      return !currentStateMap.containsKey(partition)
          || !currentStateMap.get(partition).containsValue(topState);
    }

    private int countActiveReplicas(Partition partition) {
      if (!currentStateMap.containsKey(partition) || !bestPossibleMap.containsKey(partition)) {
        return 0;
      }

      Map<String, Integer> stateCount = new HashMap<>();
      for (String state : bestPossibleMap.get(partition).values()) {
        stateCount.put(state, stateCount.getOrDefault(state, 0) + 1);
      }

      int count = 0;
      for (String state : currentStateMap.get(partition).values()) {
        if (stateCount.containsKey(state) && stateCount.get(state) > 0) {
          count++;
          stateCount.put(state, stateCount.get(state) - 1);
        }
      }
      return count;
    }

    private int countIdealStateMatches(Partition partition) {
      if (!currentStateMap.containsKey(partition) || !bestPossibleMap.containsKey(partition)) {
        return 0;
      }

      int matches = 0;
      Map<String, String> bestPossible = bestPossibleMap.get(partition);
      Map<String, String> current = currentStateMap.get(partition);

      for (Map.Entry<String, String> entry : bestPossible.entrySet()) {
        if (entry.getValue().equals(current.get(entry.getKey()))) {
          matches++;
        }
      }
      return matches;
    }
  }
}
