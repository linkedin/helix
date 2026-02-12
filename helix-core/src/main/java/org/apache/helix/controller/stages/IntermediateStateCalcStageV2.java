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
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.helix.HelixDefinedState;
import org.apache.helix.HelixException;
import org.apache.helix.HelixManager;
import org.apache.helix.controller.LogUtil;
import org.apache.helix.controller.common.PartitionStateMap;
import org.apache.helix.controller.dataproviders.ResourceControllerDataProvider;
import org.apache.helix.controller.pipeline.AbstractBaseStage;
import org.apache.helix.controller.pipeline.StageException;
import org.apache.helix.controller.stages.intermediate.AvailabilityAwareOrderingStrategy;
import org.apache.helix.controller.stages.intermediate.MessageOrderingStrategy;
import org.apache.helix.controller.stages.intermediate.MessageThrottleProcessor;
import org.apache.helix.controller.stages.intermediate.ResourcePriorityOrderingStrategy;
import org.apache.helix.model.BuiltInStateModelDefinitions;
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
 * REFACTORED VERSION (V2) - For code review before integration.
 *
 * For partition compute the Intermediate State (instance,state) pair based on the BestPossibleState
 * and CurrentState, with all constraints applied (such as state transition throttling).
 *
 * Key improvements:
 * - Uses Strategy pattern for message ordering (AvailabilityAware vs ResourcePriority)
 * - Extracted throttling logic to MessageThrottleProcessor
 * - Reduced from 875 to 437 lines (50% reduction)
 * - Clear separation of concerns with intermediate/ package helpers
 */
public class IntermediateStateCalcStageV2 extends AbstractBaseStage {
  private static final Logger logger =
      LoggerFactory.getLogger(IntermediateStateCalcStageV2.class.getName());

  @Override
  public void process(ClusterEvent event) throws Exception {
    _eventId = event.getEventId();

    CurrentStateOutput currentStateOutput =
        event.getAttribute(AttributeName.CURRENT_STATE.name());
    BestPossibleStateOutput bestPossibleStateOutput =
        event.getAttribute(AttributeName.BEST_POSSIBLE_STATE.name());
    Map<String, Resource> resourceMap =
        event.getAttribute(AttributeName.RESOURCES_TO_REBALANCE.name());
    ResourceControllerDataProvider cache =
        event.getAttribute(AttributeName.ControllerDataProvider.name());
    MessageOutput messageOutput =
        event.getAttribute(AttributeName.MESSAGES_SELECTED.name());
    ClusterStatusMonitor monitor =
        event.getAttribute(AttributeName.clusterStatusMonitor.name());

    if (currentStateOutput == null || bestPossibleStateOutput == null
        || resourceMap == null || cache == null || messageOutput == null) {
      throw new StageException(String.format(
          "Missing attributes in %s. Requires CURRENT_STATE(%s) BEST_POSSIBLE_STATE(%s) "
              + "RESOURCES(%s) MESSAGE_SELECT(%s) DataCache(%s)",
          event, currentStateOutput, bestPossibleStateOutput,
          resourceMap, messageOutput, cache));
    }

    IntermediateStateOutput output = computeIntermediateState(
        currentStateOutput, bestPossibleStateOutput, resourceMap, cache, messageOutput, monitor);
    event.addAttribute(AttributeName.INTERMEDIATE_STATE.name(), output);

    validateMaxPartitionsPerInstance(event, cache, output);
  }

  private IntermediateStateOutput computeIntermediateState(
      CurrentStateOutput currentStateOutput, BestPossibleStateOutput bestPossibleStateOutput,
      Map<String, Resource> resourceMap, ResourceControllerDataProvider cache,
      MessageOutput messageOutput, ClusterStatusMonitor monitor) {

    StateTransitionThrottleController throttleController = new StateTransitionThrottleController(
        resourceMap.keySet(), cache.getClusterConfig(), cache.getLiveInstances().keySet());
    IntermediateStateOutput output = new IntermediateStateOutput();
    List<String> failedResources = new ArrayList<>();
    Map<String, MessageThrottleProcessor.ResourceThrottleMetrics> metricsPerResource = new HashMap<>();

    // 1. Collect messages from all resources
    List<MessageOrderingStrategy.MessageContext> allMessages = collectAllMessages(
        resourceMap, currentStateOutput, bestPossibleStateOutput, cache,
        messageOutput, output, metricsPerResource);

    // 2. Order messages using strategy pattern
    MessageOrderingStrategy orderingStrategy;
    if (cache.getClusterConfig().isAvailabilityAwarePrioritizationEnabled()) {
      LogUtil.logInfo(logger, _eventId, "Using availability-aware prioritization");
      AvailabilityAwareOrderingStrategy strategy =
          new AvailabilityAwareOrderingStrategy(cache, currentStateOutput);
      strategy.setEventId(_eventId);
      orderingStrategy = strategy;
    } else {
      orderingStrategy = new ResourcePriorityOrderingStrategy(cache,
          bestPossibleStateOutput, currentStateOutput);
    }
    orderingStrategy.sortMessages(allMessages);

    // 3. Process with throttling
    MessageThrottleProcessor throttleProcessor = new MessageThrottleProcessor();
    Map<String, Map<Partition, List<Message>>> approvedMessages =
        throttleProcessor.processMessagesWithThrottling(allMessages, currentStateOutput,
            throttleController, cache, metricsPerResource, messageOutput, resourceMap,
            bestPossibleStateOutput);

    // 4. Build intermediate states from approved messages
    buildIntermediateStates(approvedMessages, resourceMap, currentStateOutput,
        bestPossibleStateOutput, cache, messageOutput, output, failedResources);

    // 5. Update monitoring
    updateMonitoring(monitor, failedResources, output, metricsPerResource, cache);

    return output;
  }

  // ========================================
  // Step 1: Collect messages
  // ========================================

  private List<MessageOrderingStrategy.MessageContext> collectAllMessages(
      Map<String, Resource> resourceMap,
      CurrentStateOutput currentStateOutput, BestPossibleStateOutput bestPossibleStateOutput,
      ResourceControllerDataProvider cache, MessageOutput messageOutput,
      IntermediateStateOutput output,
      Map<String, MessageThrottleProcessor.ResourceThrottleMetrics> metricsPerResource) {

    List<MessageOrderingStrategy.MessageContext> allMessages = new ArrayList<>();

    for (Map.Entry<String, Resource> entry : resourceMap.entrySet()) {
      String resourceName = entry.getKey();
      Resource resource = entry.getValue();

      metricsPerResource.put(resourceName, new MessageThrottleProcessor.ResourceThrottleMetrics());

      if (!bestPossibleStateOutput.containsResource(resourceName)) {
        LogUtil.logInfo(logger, _eventId, String.format(
            "Skip calculating intermediate state for resource %s because the best possible state is not available.",
            resourceName));
        continue;
      }

      IdealState idealState = cache.getIdealState(resourceName);
      if (idealState == null) {
        LogUtil.logInfo(logger, _eventId, String
            .format("IdealState for resource %s does not exist; resource may not exist anymore",
                resourceName));
        idealState = new IdealState(resourceName);
        idealState.setStateModelDefRef(resource.getStateModelDefRef());
      }

      // Non-FULL_AUTO or no messages: use best possible state directly
      Map<Partition, List<Message>> resourceMessages =
          messageOutput.getResourceMessageMap(resourceName);
      if (!IdealState.RebalanceMode.FULL_AUTO.equals(idealState.getRebalanceMode())
          || resourceMessages == null || resourceMessages.isEmpty()) {
        output.setState(resourceName, bestPossibleStateOutput.getPartitionStateMap(resourceName));
        continue;
      }

      // Find error partitions for this resource
      StateModelDefinition stateModelDef =
          cache.getStateModelDef(idealState.getStateModelDefRef());
      Map<String, List<String>> preferenceLists =
          bestPossibleStateOutput.getPreferenceLists(resourceName);

      Set<Partition> errorPartitions = findErrorPartitions(resourceName, currentStateOutput);
      metricsPerResource.get(resourceName).errorPartitions = errorPartitions;

      // Collect messages with context
      for (Map.Entry<Partition, List<Message>> msgEntry : resourceMessages.entrySet()) {
        Partition partition = msgEntry.getKey();
        List<String> preferenceList = preferenceLists.get(partition.getPartitionName());
        Map<String, Integer> requiredStates =
            MessageThrottleProcessor.getRequiredStates(resourceName, cache, preferenceList);

        for (Message message : msgEntry.getValue()) {
          allMessages.add(new MessageOrderingStrategy.MessageContext(
              message, partition, resourceName, stateModelDef, requiredStates));
        }
      }
    }

    return allMessages;
  }

  // ========================================
  // Step 4: Build intermediate states
  // ========================================

  private void buildIntermediateStates(
      Map<String, Map<Partition, List<Message>>> approvedMessages,
      Map<String, Resource> resourceMap,
      CurrentStateOutput currentStateOutput, BestPossibleStateOutput bestPossibleStateOutput,
      ResourceControllerDataProvider cache, MessageOutput messageOutput,
      IntermediateStateOutput output, List<String> failedResources) {

    for (String resourceName : resourceMap.keySet()) {
      // Skip if already set (non-FULL_AUTO or no messages)
      if (output.getResourceStatesMap().containsKey(resourceName)) {
        continue;
      }

      if (!bestPossibleStateOutput.containsResource(resourceName)) {
        continue;
      }

      Map<Partition, List<Message>> approved = approvedMessages.get(resourceName);
      if (approved == null) {
        continue;
      }

      try {
        PartitionStateMap intermediateState =
            buildPartitionStateMap(resourceName, currentStateOutput, approved);
        output.setState(resourceName, intermediateState);
      } catch (HelixException ex) {
        LogUtil.logInfo(logger, _eventId,
            "Failed to compute intermediate state for " + resourceName, ex);
        failedResources.add(resourceName);
      }
    }
  }

  // ========================================
  // Step 5: Monitoring
  // ========================================

  private void updateMonitoring(ClusterStatusMonitor monitor,
      List<String> failedResources, IntermediateStateOutput output,
      Map<String, MessageThrottleProcessor.ResourceThrottleMetrics> metricsPerResource,
      ResourceControllerDataProvider cache) {

    if (monitor == null) {
      return;
    }

    monitor.setResourceRebalanceStates(
        failedResources, ResourceMonitor.RebalanceStatus.INTERMEDIATE_STATE_CAL_FAILED);
    monitor.setResourceRebalanceStates(
        output.resourceSet(), ResourceMonitor.RebalanceStatus.NORMAL);

    int errorThreshold = MessageThrottleProcessor.getErrorThreshold(cache.getClusterConfig());
    for (Map.Entry<String, MessageThrottleProcessor.ResourceThrottleMetrics> entry : metricsPerResource.entrySet()) {
      MessageThrottleProcessor.ResourceThrottleMetrics metrics = entry.getValue();
      monitor.updateRebalancerStats(entry.getKey(),
          metrics.recoveryMessages.size(), metrics.loadMessages.size(),
          metrics.throttledRecovery.size(), metrics.throttledLoad.size(),
          metrics.errorPartitions.size() > errorThreshold);
    }
  }


  // ========================================
  // State building helpers
  // ========================================

  private Set<Partition> findErrorPartitions(String resourceName,
      CurrentStateOutput currentStateOutput) {

    Set<Partition> errors = new HashSet<>();
    Map<Partition, Map<String, String>> currentStates =
        currentStateOutput.getCurrentStateMap(resourceName);

    for (Map.Entry<Partition, Map<String, String>> entry : currentStates.entrySet()) {
      if (entry.getValue().containsValue(HelixDefinedState.ERROR.name())) {
        errors.add(entry.getKey());
      }
    }
    return errors;
  }

  private PartitionStateMap buildPartitionStateMap(String resourceName,
      CurrentStateOutput currentStateOutput, Map<Partition, List<Message>> approvedMessages) {

    PartitionStateMap intermediateState = new PartitionStateMap(
        resourceName, currentStateOutput.getCurrentStateMap(resourceName));

    // Apply pending messages
    Map<Partition, Map<String, Message>> pendingMessages =
        currentStateOutput.getPendingMessageMap(resourceName);
    if (pendingMessages != null) {
      for (Map.Entry<Partition, Map<String, Message>> entry : pendingMessages.entrySet()) {
        for (Map.Entry<String, Message> msgEntry : entry.getValue().entrySet()) {
          Message msg = msgEntry.getValue();
          if (msg != null && msg.getToState() != null) {
            applyMessageToStateMap(intermediateState, entry.getKey(),
                msgEntry.getKey(), msg.getToState());
          }
        }
      }
    }

    // Apply approved messages
    for (Map.Entry<Partition, List<Message>> entry : approvedMessages.entrySet()) {
      for (Message msg : entry.getValue()) {
        if (msg != null && msg.getTgtName() != null && msg.getToState() != null) {
          applyMessageToStateMap(intermediateState, entry.getKey(),
              msg.getTgtName(), msg.getToState());
        }
      }
    }

    return intermediateState;
  }

  private void applyMessageToStateMap(PartitionStateMap stateMap,
      Partition partition, String instance, String toState) {
    if (!toState.equals(HelixDefinedState.DROPPED.name())) {
      stateMap.setState(partition, instance, toState);
    } else if (stateMap.getStateMap().containsKey(partition)) {
      stateMap.getStateMap().get(partition).remove(instance);
    }
  }

  // ========================================
  // Validation
  // ========================================

  private void validateMaxPartitionsPerInstance(ClusterEvent event,
      ResourceControllerDataProvider cache, IntermediateStateOutput output) {

    int maxPartitionPerInstance = cache.getClusterConfig().getMaxPartitionsPerInstance();
    if (maxPartitionPerInstance <= 0) {
      return;
    }

    Map<String, Integer> instancePartitionCounts = new HashMap<>();

    for (Map.Entry<String, PartitionStateMap> entry : output.getResourceStatesMap().entrySet()) {
      String resourceName = entry.getKey();
      IdealState idealState = cache.getIdealState(resourceName);
      if (idealState != null
          && idealState.getStateModelDefRef().equals(BuiltInStateModelDefinitions.Task.name())) {
        continue;
      }

      for (Map.Entry<Partition, Map<String, String>> partEntry
          : entry.getValue().getStateMap().entrySet()) {
        for (Map.Entry<String, String> instEntry : partEntry.getValue().entrySet()) {
          if (instEntry.getValue().equals(HelixDefinedState.DROPPED.name())) {
            continue;
          }

          int count = instancePartitionCounts.getOrDefault(instEntry.getKey(), 0) + 1;
          instancePartitionCounts.put(instEntry.getKey(), count);

          if (count > maxPartitionPerInstance) {
            handleMaxPartitionViolation(instEntry.getKey(), maxPartitionPerInstance,
                resourceName, event, cache);
          }
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

    ClusterStatusMonitor monitor =
        event.getAttribute(AttributeName.clusterStatusMonitor.name());
    if (monitor != null) {
      monitor.setResourceRebalanceStates(
          Collections.singletonList(resourceName),
          ResourceMonitor.RebalanceStatus.INTERMEDIATE_STATE_CAL_FAILED);
    }

    throw new HelixException(errorMsg);
  }
}
