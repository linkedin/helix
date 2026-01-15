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
import org.apache.helix.api.config.StateTransitionThrottleConfig;
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
 * Traditional resource-priority-based intermediate state calculator.
 *
 * <p>This implementation processes resources in priority order (based on the configured
 * priority field in ResourceConfig or IdealState), and within each resource, messages
 * are processed based on partition priority. This is the legacy behavior for intermediate
 * state calculation.
 *
 * <p>Key characteristics:
 * <ul>
 *   <li>Resources are sorted by priority field and processed in order</li>
 *   <li>Higher priority resources consume throttling quota first</li>
 *   <li>Partitions within a resource are prioritized based on top state availability</li>
 *   <li>Messages within a partition are prioritized by state priority and preference list</li>
 * </ul>
 */
public class ResourcePriorityIntermediateStateCalculator implements IntermediateStateComputationStrategy {
  private static final Logger LOG =
      LoggerFactory.getLogger(ResourcePriorityIntermediateStateCalculator.class.getName());

  private String _eventId;

  @Override
  public IntermediateStateOutput compute(ClusterEvent event, Map<String, Resource> resourceMap,
      CurrentStateOutput currentStateOutput, BestPossibleStateOutput bestPossibleStateOutput,
      MessageOutput messageOutput, ResourceControllerDataProvider dataCache) {

    _eventId = event.getEventId();
    IntermediateStateOutput output = new IntermediateStateOutput();

    StateTransitionThrottleController throttleController =
        new StateTransitionThrottleController(resourceMap.keySet(), dataCache.getClusterConfig(),
            dataCache.getLiveInstances().keySet());

    // Resource level prioritization based on the numerical (sortable) priority field.
    // If the resource priority field is null/not set, the resource will be treated as lowest
    // priority.
    List<ResourcePriority> prioritizedResourceList = new ArrayList<>();
    for (String resourceName : resourceMap.keySet()) {
      prioritizedResourceList.add(new ResourcePriority(resourceName));
    }
    // If resourcePriorityField is null at the cluster level, all resources will be considered equal
    // in priority by keeping all priorities at MIN_VALUE
    if (dataCache.getClusterConfig().getResourcePriorityField() != null) {
      String priorityField = dataCache.getClusterConfig().getResourcePriorityField();
      for (ResourcePriority resourcePriority : prioritizedResourceList) {
        String resourceName = resourcePriority.getResourceName();

        // Will take the priority from ResourceConfig first
        // If ResourceConfig does not exist or does not have this field.
        // Try to load it from the resource's IdealState. Otherwise, keep it at the lowest priority
        if (dataCache.getResourceConfig(resourceName) != null
            && dataCache.getResourceConfig(resourceName).getSimpleConfig(priorityField) != null) {
          resourcePriority.setPriority(
              dataCache.getResourceConfig(resourceName).getSimpleConfig(priorityField));
        } else if (dataCache.getIdealState(resourceName) != null
            && dataCache.getIdealState(resourceName).getRecord().getSimpleField(priorityField)
            != null) {
          resourcePriority.setPriority(
              dataCache.getIdealState(resourceName).getRecord().getSimpleField(priorityField));
        }
      }
      Collections.sort(prioritizedResourceList);
    }

    ClusterStatusMonitor clusterStatusMonitor =
        event.getAttribute(AttributeName.clusterStatusMonitor.name());
    List<String> failedResources = new ArrayList<>();

    // Priority is applied in assignment computation because higher priority by looping in order of
    // decreasing priority
    for (ResourcePriority resourcePriority : prioritizedResourceList) {
      String resourceName = resourcePriority.getResourceName();

      if (!bestPossibleStateOutput.containsResource(resourceName)) {
        LogUtil.logInfo(LOG, _eventId, String.format(
            "Skip calculating intermediate state for resource %s because the best possible state is not available.",
            resourceName));
        continue;
      }

      Resource resource = resourceMap.get(resourceName);
      IdealState idealState = getIdealStateOrDefault(resourceName, resource, dataCache);

      try {
        output.setState(resourceName,
            computeIntermediatePartitionState(dataCache, clusterStatusMonitor, idealState,
                resource, currentStateOutput,
                bestPossibleStateOutput.getPartitionStateMap(resourceName),
                bestPossibleStateOutput.getPreferenceLists(resourceName), throttleController,
                messageOutput.getResourceMessageMap(resourceName)));
      } catch (HelixException ex) {
        LogUtil.logInfo(LOG, _eventId,
            "Failed to calculate intermediate partition states for resource " + resourceName, ex);
        failedResources.add(resourceName);
      }
    }

    if (clusterStatusMonitor != null) {
      clusterStatusMonitor.setResourceRebalanceStates(failedResources,
          ResourceMonitor.RebalanceStatus.INTERMEDIATE_STATE_CAL_FAILED);
      clusterStatusMonitor.setResourceRebalanceStates(output.resourceSet(),
          ResourceMonitor.RebalanceStatus.NORMAL);
    }

    return output;
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

  /**
   * Compute intermediate partition states for a prioritized resource.
   */
  private PartitionStateMap computeIntermediatePartitionState(ResourceControllerDataProvider cache,
      ClusterStatusMonitor clusterStatusMonitor, IdealState idealState, Resource resource,
      CurrentStateOutput currentStateOutput, PartitionStateMap bestPossiblePartitionStateMap,
      Map<String, List<String>> preferenceLists,
      StateTransitionThrottleController throttleController,
      Map<Partition, List<Message>> resourceMessageMap) {
    String resourceName = resource.getResourceName();
    LogUtil.logDebug(LOG, _eventId, String.format("Processing resource: %s", resourceName));

    // Throttling is applied only on FULL-AUTO mode and if the resource message map is empty, no throttling needed.
    if (!IdealState.RebalanceMode.FULL_AUTO.equals(idealState.getRebalanceMode())
        || resourceMessageMap.isEmpty()) {
      return bestPossiblePartitionStateMap;
    }

    String stateModelDefName = idealState.getStateModelDefRef();
    StateModelDefinition stateModelDef = cache.getStateModelDef(stateModelDefName);

    Set<Partition> partitionsWithErrorStateReplica = new HashSet<>();
    Set<String> messagesForRecovery = new HashSet<>();
    Set<String> messagesForLoad = new HashSet<>();
    Set<String> messagesThrottledForRecovery = new HashSet<>();
    Set<String> messagesThrottledForLoad = new HashSet<>();
    ClusterConfig clusterConfig = cache.getClusterConfig();

    // Keep the error count as partition level
    for (Partition partition : currentStateOutput.getCurrentStateMap(resourceName).keySet()) {
      Map<String, String> entry = currentStateOutput.getCurrentStateMap(resourceName).get(partition);
      if (entry.containsValue(HelixDefinedState.ERROR.name())) {
        partitionsWithErrorStateReplica.add(partition);
      }
    }
    int numPartitionsWithErrorReplica = partitionsWithErrorStateReplica.size();

    int threshold = getErrorOrRecoveryThreshold(clusterConfig);
    boolean onlyDownwardLoadBalance = numPartitionsWithErrorReplica > threshold;

    chargePendingTransition(resource, currentStateOutput, throttleController, cache,
        preferenceLists, stateModelDef);

    // Sort partitions in case of urgent partition need to take the quota first.
    List<Partition> partitions = new ArrayList<>(resource.getPartitions());
    partitions.sort(new PartitionPriorityComparator(bestPossiblePartitionStateMap.getStateMap(),
        currentStateOutput.getCurrentStateMap(resourceName), stateModelDef.getTopState()));

    for (Partition partition : partitions) {
      if (resourceMessageMap.get(partition) == null || resourceMessageMap.get(partition).isEmpty()) {
        continue;
      }
      List<Message> messagesToThrottle = new ArrayList<>(resourceMessageMap.get(partition));
      Map<String, String> derivedCurrentStateMap =
          currentStateOutput.getCurrentStateMap(resourceName, partition).entrySet().stream()
              .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
      List<String> preferenceList = preferenceLists.get(partition.getPartitionName());
      Map<String, Integer> requiredState = getRequiredStates(resourceName, cache, preferenceList);
      if (preferenceList != null && !preferenceList.isEmpty()) {
        messagesToThrottle.sort(new MessagePriorityComparator(preferenceList, stateModelDef.getStatePriorityMap()));
      }

      for (Message message : messagesToThrottle) {
        RebalanceType rebalanceType =
            getRebalanceTypePerMessage(requiredState, message, derivedCurrentStateMap);

        if (rebalanceType.equals(RebalanceType.RECOVERY_BALANCE)) {
          message.setSTRebalanceType(Message.STRebalanceType.RECOVERY_REBALANCE);
          messagesForRecovery.add(message.getId());
          recoveryRebalance(resource, partition, throttleController, message, cache,
              messagesThrottledForRecovery, resourceMessageMap);
        } else if (rebalanceType.equals(RebalanceType.LOAD_BALANCE)) {
          message.setSTRebalanceType(Message.STRebalanceType.LOAD_REBALANCE);
          messagesForLoad.add(message.getId());
          loadRebalance(resource, partition, throttleController, message, cache,
              onlyDownwardLoadBalance, stateModelDef, messagesThrottledForLoad, resourceMessageMap);
        }

        if (!messagesThrottledForRecovery.contains(message.getId()) && !messagesThrottledForLoad
            .contains(message.getId())) {
          derivedCurrentStateMap.put(message.getTgtName(), message.getToState());
        }
      }
    }

    PartitionStateMap intermediatePartitionStateMap =
        new PartitionStateMap(resourceName, currentStateOutput.getCurrentStateMap(resourceName));
    applyPendingMessages(currentStateOutput, resourceName, intermediatePartitionStateMap);
    applyNonThrottledMessages(resourceMessageMap, intermediatePartitionStateMap);

    if (!messagesForRecovery.isEmpty()) {
      LogUtil.logInfo(LOG, _eventId, String
          .format("Recovery balance needed for %s with messages: %s", resourceName,
              messagesForRecovery));
    }
    if (!messagesForLoad.isEmpty()) {
      LogUtil.logInfo(LOG, _eventId, String
          .format("Load balance needed for %s with messages: %s", resourceName, messagesForLoad));
    }
    if (!partitionsWithErrorStateReplica.isEmpty()) {
      LogUtil.logInfo(LOG, _eventId, String
          .format("Partition currently has an ERROR replica in %s partitions: %s", resourceName,
              partitionsWithErrorStateReplica));
    }

    if (clusterStatusMonitor != null) {
      clusterStatusMonitor
          .updateRebalancerStats(resourceName, messagesForRecovery.size(), messagesForLoad.size(),
              messagesThrottledForRecovery.size(), messagesThrottledForLoad.size(),
              onlyDownwardLoadBalance);
    }

    if (LOG.isDebugEnabled()) {
      logPartitionMapState(resourceName, new HashSet<>(resource.getPartitions()),
          messagesForRecovery, messagesThrottledForRecovery, messagesForLoad,
          messagesThrottledForLoad, currentStateOutput, bestPossiblePartitionStateMap,
          intermediatePartitionStateMap);
    }

    LogUtil.logDebug(LOG, _eventId, String.format("End processing resource: %s", resourceName));
    return intermediatePartitionStateMap;
  }

  private int getErrorOrRecoveryThreshold(ClusterConfig clusterConfig) {
    int threshold = 1;
    if (clusterConfig.getErrorOrRecoveryPartitionThresholdForLoadBalance() != -1) {
      threshold = clusterConfig.getErrorOrRecoveryPartitionThresholdForLoadBalance();
    } else {
      if (clusterConfig.getErrorPartitionThresholdForLoadBalance() != 0) {
        threshold = clusterConfig.getErrorPartitionThresholdForLoadBalance();
      }
    }
    return threshold;
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
      List<String> preferenceList = preferenceLists.get(partition.getPartitionName());
      if (preferenceList != null && !preferenceList.isEmpty()) {
        pendingMessages.sort(new MessagePriorityComparator(preferenceList,
            stateModelDefinition.getStatePriorityMap()));
      }

      for (Message message : pendingMessages) {
        StateTransitionThrottleConfig.RebalanceType rebalanceType =
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

  private void recoveryRebalance(Resource resource, Partition partition,
      StateTransitionThrottleController throttleController, Message messageToThrottle,
      ResourceControllerDataProvider cache, Set<String> messagesThrottled,
      Map<Partition, List<Message>> resourceMessageMap) {
    throttleStateTransitionsForReplica(throttleController, resource.getResourceName(), partition,
        messageToThrottle, messagesThrottled, RebalanceType.RECOVERY_BALANCE, cache,
        resourceMessageMap);
  }

  private void loadRebalance(Resource resource, Partition partition,
      StateTransitionThrottleController throttleController, Message messageToThrottle,
      ResourceControllerDataProvider cache, boolean onlyDownwardLoadBalance,
      StateModelDefinition stateModelDefinition, Set<String> messagesThrottled,
      Map<Partition, List<Message>> resourceMessageMap) {
    if (onlyDownwardLoadBalance && !isLoadBalanceDownwardStateTransition(messageToThrottle,
        stateModelDefinition)) {
      resourceMessageMap.get(partition).remove(messageToThrottle);
      messagesThrottled.add(messageToThrottle.getId());
      return;
    }
    throttleStateTransitionsForReplica(throttleController, resource.getResourceName(), partition,
        messageToThrottle, messagesThrottled, RebalanceType.LOAD_BALANCE, cache,
        resourceMessageMap);
  }

  private void throttleStateTransitionsForReplica(
      StateTransitionThrottleController throttleController, String resourceName,
      Partition partition, Message messageToThrottle, Set<String> messagesThrottled,
      RebalanceType rebalanceType, ResourceControllerDataProvider cache,
      Map<Partition, List<Message>> resourceMessageMap) {
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
        if (throttleController
            .shouldThrottleForInstance(rebalanceType, messageToThrottle.getTgtName())) {
          hasReachedThrottlingLimit = true;
          if (LOG.isDebugEnabled()) {
            LogUtil.logDebug(LOG, _eventId, String.format(
                "Throttled because of instance level quota is full on instance {%s} for message {%s} of partition {%s} in resource {%s}",
                messageToThrottle.getId(), messageToThrottle.getTgtName(),
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
      resourceMessageMap.get(partition).remove(messageToThrottle);
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

  private void logPartitionMapState(String resource, Set<Partition> allPartitions,
      Set<String> recoveryPartitions, Set<String> recoveryThrottledPartitions,
      Set<String> loadbalancePartitions, Set<String> loadbalanceThrottledPartitions,
      CurrentStateOutput currentStateOutput, PartitionStateMap bestPossibleStateMap,
      PartitionStateMap intermediateStateMap) {

    if (LOG.isDebugEnabled()) {
      LogUtil.logDebug(LOG, _eventId, String
          .format("Partitions need recovery: %s\nPartitions get throttled on recovery: %s",
              recoveryPartitions, recoveryThrottledPartitions));
      LogUtil.logDebug(LOG, _eventId, String
          .format("Partitions need loadbalance: %s\nPartitions get throttled on load-balance: %s",
              loadbalancePartitions, loadbalanceThrottledPartitions));
    }

    for (Partition partition : allPartitions) {
      if (LOG.isDebugEnabled()) {
        LogUtil.logDebug(LOG, _eventId, String.format("%s : Best possible map: %s", partition,
            bestPossibleStateMap.getPartitionMap(partition)));
        LogUtil.logDebug(LOG, _eventId, String.format("%s : Current State: %s", partition,
            currentStateOutput.getCurrentStateMap(resource, partition)));
        LogUtil.logDebug(LOG, _eventId, String.format("%s: Pending state: %s", partition,
            currentStateOutput.getPendingMessageMap(resource, partition)));
        LogUtil.logDebug(LOG, _eventId, String.format("%s: Intermediate state: %s", partition,
            intermediateStateMap.getPartitionMap(partition)));
      }
    }
  }

  /**
   * POJO that maps resource name to its priority represented by an integer.
   */
  private static class ResourcePriority implements Comparable<ResourcePriority> {
    private final String _resourceName;
    private int _priority = Integer.MIN_VALUE;

    ResourcePriority(String resourceName) {
      _resourceName = resourceName;
    }

    @Override
    public int compareTo(ResourcePriority resourcePriority) {
      return Integer.compare(resourcePriority._priority, _priority);
    }

    public String getResourceName() {
      return _resourceName;
    }

    public void setPriority(String priority) {
      try {
        _priority = Integer.parseInt(priority);
      } catch (Exception e) {
        LOG.warn(
            String.format("Invalid priority field %s for resource %s", priority, _resourceName));
      }
    }
  }

  private static class MessagePriorityComparator implements Comparator<Message> {
    private final Map<String, Integer> _preferenceInstanceMap;
    private final Map<String, Integer> _statePriorityMap;

    MessagePriorityComparator(List<String> preferenceList, Map<String, Integer> statePriorityMap) {
      _preferenceInstanceMap = IntStream.range(0, preferenceList.size()).boxed()
          .collect(Collectors.toMap(preferenceList::get, index -> index));
      _statePriorityMap = statePriorityMap;
    }

    @Override
    public int compare(Message m1, Message m2) {
      if (m1.getToState().equals(m2.getToState()) && _preferenceInstanceMap
          .containsKey(m1.getTgtName()) && _preferenceInstanceMap.containsKey(m2.getTgtName())) {
        return _preferenceInstanceMap.get(m1.getTgtName())
            .compareTo(_preferenceInstanceMap.get(m2.getTgtName()));
      }
      if (!m1.getToState().equals(m2.getToState())) {
        return _statePriorityMap.get(m1.getToState())
            .compareTo(_statePriorityMap.get(m2.getToState()));
      }
      return m1.getTgtName().compareTo(m2.getTgtName());
    }
  }

  // Compare partitions according following standard:
  // 1) Partition without top state always is the highest priority.
  // 2) For partition with top-state, the more number of active replica it has, the less priority.
  private static class PartitionPriorityComparator implements Comparator<Partition> {
    private final Map<Partition, Map<String, String>> _bestPossibleMap;
    private final Map<Partition, Map<String, String>> _currentStateMap;
    private final String _topState;
    private final Map<Partition, Integer> _currentActiveReplicasCache = new HashMap<>();
    private final Map<Partition, Integer> _idealStateMatchedCache = new HashMap<>();

    PartitionPriorityComparator(Map<Partition, Map<String, String>> bestPossibleMap,
        Map<Partition, Map<String, String>> currentStateMap, String topState) {
      _bestPossibleMap = bestPossibleMap;
      _currentStateMap = currentStateMap;
      _topState = topState;
    }

    @Override
    public int compare(Partition p1, Partition p2) {
      int missTopState1 = getMissTopStateIndex(p1);
      int missTopState2 = getMissTopStateIndex(p2);
      if (missTopState1 != missTopState2) {
        return Integer.compare(missTopState1, missTopState2);
      }
      int currentActiveReplicas1 = _currentActiveReplicasCache.computeIfAbsent(p1, this::getCurrentActiveReplicas);
      int currentActiveReplicas2 = _currentActiveReplicasCache.computeIfAbsent(p2, this::getCurrentActiveReplicas);
      if (currentActiveReplicas1 != currentActiveReplicas2) {
        return Integer.compare(currentActiveReplicas1, currentActiveReplicas2);
      }
      int idealStateMatched1 = _idealStateMatchedCache.computeIfAbsent(p1, this::getIdealStateMatched);
      int idealStateMatched2 = _idealStateMatchedCache.computeIfAbsent(p2, this::getIdealStateMatched);
      if (idealStateMatched1 != idealStateMatched2) {
        return Integer.compare(idealStateMatched1, idealStateMatched2);
      }
      return p1.getPartitionName().compareTo(p2.getPartitionName());
    }

    private int getMissTopStateIndex(Partition partition) {
      if (!_currentStateMap.containsKey(partition) || !_currentStateMap.get(partition).containsValue(_topState)) {
        return 0;
      }
      return 1;
    }

    private int getCurrentActiveReplicas(Partition partition) {
      int currentActiveReplicas = 0;
      if (!_currentStateMap.containsKey(partition)) {
        return currentActiveReplicas;
      }
      Map<String, Integer> stateCountMap = new HashMap<>();
      for (String state : _bestPossibleMap.get(partition).values()) {
        if (!stateCountMap.containsKey(state)) {
          stateCountMap.put(state, 0);
        }
        stateCountMap.put(state, stateCountMap.get(state) + 1);
      }
      for (String state : _currentStateMap.get(partition).values()) {
        if (stateCountMap.containsKey(state) && stateCountMap.get(state) > 0) {
          currentActiveReplicas++;
          stateCountMap.put(state, stateCountMap.get(state) - 1);
        }
      }
      return currentActiveReplicas;
    }

    private int getIdealStateMatched(Partition partition) {
      int matchedState = 0;
      if (!_currentStateMap.containsKey(partition)) {
        return matchedState;
      }
      for (String instance : _bestPossibleMap.get(partition).keySet()) {
        if (_bestPossibleMap.get(partition).get(instance)
            .equals(_currentStateMap.get(partition).get(instance))) {
          matchedState++;
        }
      }
      return matchedState;
    }
  }
}

