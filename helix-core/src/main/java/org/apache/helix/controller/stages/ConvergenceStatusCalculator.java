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
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.EnumMap;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.helix.HelixDefinedState;
import org.apache.helix.HelixManager;
import org.apache.helix.HelixRebalanceException;
import org.apache.helix.controller.common.PartitionStateMap;
import org.apache.helix.controller.dataproviders.BaseControllerDataProvider;
import org.apache.helix.controller.rebalancer.waged.WagedRebalanceStatus;
import org.apache.helix.model.ConvergenceStatus;
import org.apache.helix.model.ConvergenceStatus.OptimizerStatus;
import org.apache.helix.model.ConvergenceStatus.PartitionDetail;
import org.apache.helix.model.ConvergenceStatus.Reason;
import org.apache.helix.model.ConvergenceStatus.Scope;
import org.apache.helix.model.ConvergenceStatus.Status;
import org.apache.helix.model.ConvergenceStatus.TargetFreshness;
import org.apache.helix.model.Message;
import org.apache.helix.model.Partition;
import org.apache.helix.model.Resource;
import org.apache.helix.task.TaskConstants;

/**
 * Computes a bounded convergence snapshot from one controller event.
 */
public class ConvergenceStatusCalculator {
  private static final Comparator<PartitionEvaluation> DETAIL_ORDER =
      Comparator.comparingInt((PartitionEvaluation evaluation) ->
              statusSeverity(evaluation._status))
          .reversed()
          .thenComparing(evaluation -> evaluation._resourceName)
          .thenComparing(evaluation -> evaluation._partitionName);

  public ConvergenceStatusSnapshot calculate(ClusterEvent event) {
    BaseControllerDataProvider cache =
        event.getAttribute(AttributeName.ControllerDataProvider.name());
    @SuppressWarnings("unchecked")
    Map<String, Resource> resources =
        event.getAttribute(AttributeName.RESOURCES_TO_REBALANCE.name());
    if (cache == null || resources == null) {
      throw new IllegalArgumentException(
          "Convergence calculation requires controller cache and resources");
    }
    return calculate(event, ConvergenceStatusContext.from(event, cache, resources));
  }

  public ConvergenceStatusSnapshot calculate(ClusterEvent event,
      ConvergenceStatusContext context) {
    @SuppressWarnings("unchecked")
    Map<String, Resource> resources =
        event.getAttribute(AttributeName.RESOURCES_TO_REBALANCE.name());
    CurrentStateOutput currentStateOutput =
        event.getAttribute(AttributeName.CURRENT_STATE.name());
    HelixManager manager = event.getAttribute(AttributeName.helixmanager.name());

    if (context == null || resources == null || currentStateOutput == null || manager == null) {
      throw new IllegalArgumentException(
          "Convergence calculation requires context, resources, current state, and manager");
    }

    if (context.isMaintenanceModeEnabled()) {
      return calculatePaused(event, resources, currentStateOutput, Reason.MAINTENANCE_MODE);
    }

    BestPossibleStateOutput bestPossibleStateOutput =
        event.getAttribute(AttributeName.BEST_POSSIBLE_STATE.name());
    MessageOutput allMessages = event.getAttribute(AttributeName.MESSAGES_ALL.name());
    MessageOutput selectedMessages = event.getAttribute(AttributeName.MESSAGES_SELECTED.name());
    MessageOutput throttledMessages = event.getAttribute(AttributeName.MESSAGES_THROTTLE.name());
    MessageDispatchResult dispatchResult =
        event.getAttribute(AttributeName.MESSAGE_DISPATCH_RESULT.name());

    DispatchIndex dispatchIndex = new DispatchIndex(dispatchResult);
    WagedRebalanceStatus wagedStatus = context.getWagedStatus();
    Map<String, ConvergenceStatus> resourceStatuses = new LinkedHashMap<>();
    List<PartitionEvaluation> clusterDetails = new ArrayList<>();
    Aggregate clusterAggregate = new Aggregate();

    List<String> resourceNames = new ArrayList<>(resources.keySet());
    Collections.sort(resourceNames);
    for (String resourceName : resourceNames) {
      Resource resource = resources.get(resourceName);
      if (TaskConstants.STATE_MODEL_NAME.equals(resource.getStateModelDefRef())) {
        continue;
      }

      ResourceEvaluation evaluation =
          evaluateResource(resource, context, currentStateOutput, bestPossibleStateOutput, allMessages,
              selectedMessages, throttledMessages, dispatchIndex);
      TargetFreshness freshness = TargetFreshness.CURRENT;
      OptimizerStatus optimizerStatus = OptimizerStatus.NOT_APPLICABLE;
      if (context.isWagedResource(resourceName)) {
        optimizerStatus = wagedStatus != null && wagedStatus.isBaselineComputationFailed()
            ? OptimizerStatus.BASELINE_FAILED : OptimizerStatus.HEALTHY;
        if (wagedStatus == null) {
          freshness = TargetFreshness.UNKNOWN;
          evaluation = overrideUnknown(evaluation, Reason.WAGED_INTERNAL_FAILURE);
          optimizerStatus = OptimizerStatus.UNKNOWN;
        } else if (wagedStatus.isLastKnownGoodFallback()) {
          freshness = TargetFreshness.LAST_KNOWN_GOOD;
          evaluation = overrideUnknown(evaluation,
              wagedFailureReason(wagedStatus, Reason.WAGED_LAST_KNOWN_GOOD));
        } else if (wagedStatus.isServingComputationFailed()) {
          freshness = TargetFreshness.UNKNOWN;
          evaluation = overrideUnknown(evaluation,
              wagedFailureReason(wagedStatus, Reason.WAGED_INTERNAL_FAILURE));
        }
      }
      ConvergenceStatus status =
          buildResourceStatus(event, manager, resourceName, evaluation, freshness, optimizerStatus);
      resourceStatuses.put(resourceName, status);
      clusterAggregate.add(evaluation._aggregate);
      evaluation._details.forEach(detail -> addBoundedDetail(clusterDetails, detail));
    }

    ConvergenceStatus clusterStatus =
        buildClusterStatus(event, manager, clusterAggregate, resourceStatuses, clusterDetails);
    return new ConvergenceStatusSnapshot(clusterStatus, resourceStatuses);
  }

  public ConvergenceStatusSnapshot calculatePaused(ClusterEvent event,
      Map<String, Resource> resources, CurrentStateOutput currentStateOutput, Reason reason) {
    return calculateOverride(event, resources, currentStateOutput, Status.PAUSED, reason);
  }

  public ConvergenceStatusSnapshot calculateUnknown(ClusterEvent event,
      Map<String, Resource> resources, CurrentStateOutput currentStateOutput, Reason reason) {
    return calculateOverride(event, resources, currentStateOutput, Status.UNKNOWN, reason);
  }

  private ConvergenceStatusSnapshot calculateOverride(ClusterEvent event,
      Map<String, Resource> resources, CurrentStateOutput currentStateOutput, Status overrideStatus,
      Reason reason) {
    HelixManager manager = event.getAttribute(AttributeName.helixmanager.name());
    if (manager == null) {
      throw new IllegalArgumentException("Convergence override calculation requires HelixManager");
    }

    Map<String, ConvergenceStatus> resourceStatuses = new LinkedHashMap<>();
    Aggregate clusterAggregate = new Aggregate();
    List<String> resourceNames = new ArrayList<>(resources.keySet());
    Collections.sort(resourceNames);
    for (String resourceName : resourceNames) {
      Resource resource = resources.get(resourceName);
      if (TaskConstants.STATE_MODEL_NAME.equals(resource.getStateModelDefRef())) {
        continue;
      }
      Set<Partition> partitions = new HashSet<>(resource.getPartitions());
      partitions.addAll(currentStateOutput.getCurrentStateMap(resourceName).keySet());
      Aggregate aggregate = new Aggregate();
      for (int i = 0; i < partitions.size(); i++) {
        aggregate.record(overrideStatus, reason);
      }
      ResourceEvaluation evaluation =
          new ResourceEvaluation(overrideStatus, reason, aggregate, Collections.emptyList());
      resourceStatuses.put(resourceName,
          buildResourceStatus(event, manager, resourceName, evaluation, TargetFreshness.UNKNOWN,
              OptimizerStatus.UNKNOWN));
      clusterAggregate.add(aggregate);
    }

    ConvergenceStatus clusterStatus =
        buildClusterStatus(event, manager, clusterAggregate, resourceStatuses,
            Collections.emptyList());
    clusterStatus.setStatus(overrideStatus);
    clusterStatus.setPrimaryReason(reason);
    clusterStatus.setTargetFreshness(TargetFreshness.UNKNOWN);
    return new ConvergenceStatusSnapshot(clusterStatus, resourceStatuses);
  }

  private ResourceEvaluation evaluateResource(Resource resource,
      ConvergenceStatusContext context, CurrentStateOutput currentStateOutput,
      BestPossibleStateOutput bestPossibleStateOutput, MessageOutput allMessages,
      MessageOutput selectedMessages, MessageOutput throttledMessages,
      DispatchIndex dispatchIndex) {
    String resourceName = resource.getResourceName();
    Aggregate aggregate = new Aggregate();
    List<PartitionEvaluation> details = new ArrayList<>();

    Set<Partition> partitions = new HashSet<>();
    partitions.addAll(resource.getPartitions());
    partitions.addAll(currentStateOutput.getCurrentStateMap(resourceName).keySet());

    boolean resourceTargetAvailable =
        bestPossibleStateOutput != null && bestPossibleStateOutput.resourceSet()
            .contains(resourceName);
    PartitionStateMap targetStateMap = resourceTargetAvailable
        ? bestPossibleStateOutput.getPartitionStateMap(resourceName) : null;
    if (targetStateMap != null) {
      partitions.addAll(targetStateMap.partitionSet());
    }

    for (Partition partition : partitions) {
      Map<String, String> current =
          copy(currentStateOutput.getCurrentStateMap(resourceName, partition));
      boolean partitionTargetAvailable =
          resourceTargetAvailable && targetStateMap.partitionSet().contains(partition);
      Map<String, String> target = partitionTargetAvailable
          ? copy(targetStateMap.getPartitionMap(partition)) : Collections.emptyMap();

      PartitionEvaluation evaluation;
      if (!context.hasStateModel(resourceName) || !partitionTargetAvailable) {
        evaluation = new PartitionEvaluation(resourceName, partition.getPartitionName(),
            Status.UNKNOWN, Reason.TARGET_ASSIGNMENT_MISSING, current, target);
      } else {
        evaluation =
            evaluatePartition(resourceName, partition, current, target,
                context.getInitialState(resourceName), context, currentStateOutput, allMessages,
                selectedMessages, throttledMessages,
                dispatchIndex);
      }
      aggregate.record(evaluation._status, evaluation._reason);
      if (evaluation._status != Status.CONVERGED) {
        addBoundedDetail(details, evaluation);
      }
    }

    Status status = aggregate.overallStatus();
    Reason reason = aggregate.primaryReason(status);
    return new ResourceEvaluation(status, reason, aggregate, details);
  }

  private PartitionEvaluation evaluatePartition(String resourceName, Partition partition,
      Map<String, String> current, Map<String, String> target,
      String initialState, ConvergenceStatusContext context,
      CurrentStateOutput currentStateOutput, MessageOutput allMessages,
      MessageOutput selectedMessages, MessageOutput throttledMessages,
      DispatchIndex dispatchIndex) {
    NormalizedAssignment normalizedCurrent =
        normalize(current, initialState);
    NormalizedAssignment normalizedTarget =
        normalize(target, initialState);

    if (!normalizedCurrent._valid || !normalizedTarget._valid) {
      return new PartitionEvaluation(resourceName, partition.getPartitionName(), Status.UNKNOWN,
          Reason.INVALID_STATE, current, target);
    }

    Map<String, Message> pending =
        currentStateOutput.getPendingMessageMap(resourceName, partition);
    Map<String, Message> cancellations =
        currentStateOutput.getCancellationMessageMap(resourceName, partition);
    Map<String, Message> relays =
        currentStateOutput.getPendingRelayMessageMap(resourceName, partition);
    boolean hasActiveMessage =
        !pending.isEmpty() || !cancellations.isEmpty() || !relays.isEmpty();

    if (normalizedCurrent._assignment.equals(normalizedTarget._assignment)
        && !hasActiveMessage) {
      return new PartitionEvaluation(resourceName, partition.getPartitionName(), Status.CONVERGED,
          Reason.NONE, current, target);
    }

    String partitionName = partition.getPartitionName();
    Reason progressReason = null;
    if (!cancellations.isEmpty()) {
      progressReason = Reason.CANCELLATION_PENDING;
    } else if (!relays.isEmpty()) {
      progressReason = Reason.RELAY_PENDING;
    } else if (!pending.isEmpty()) {
      progressReason = Reason.PENDING_TRANSITION;
    } else if (dispatchIndex.wasSent(resourceName, partitionName)) {
      progressReason = Reason.TRANSITION_DISPATCHED;
    } else if (dispatchIndex.failed(resourceName, partitionName)) {
      return new PartitionEvaluation(resourceName, partitionName, Status.BLOCKED,
          Reason.MESSAGE_DISPATCH_FAILED, current, target);
    } else {
      boolean generated = hasMessages(allMessages, resourceName, partition);
      boolean selected = hasMessages(selectedMessages, resourceName, partition);
      boolean throttled = hasMessages(throttledMessages, resourceName, partition);
      if (selected && !throttled) {
        progressReason = Reason.MESSAGE_THROTTLED;
      } else if (generated && !selected) {
        progressReason = Reason.STATE_CONSTRAINT_WAIT;
      } else if (throttled) {
        progressReason = Reason.TRANSITION_DISPATCHED;
      }
    }

    if (progressReason != null) {
      return new PartitionEvaluation(resourceName, partitionName, Status.IN_PROGRESS,
          progressReason, current, target);
    }

    if (containsState(current, HelixDefinedState.ERROR.name())) {
      return new PartitionEvaluation(resourceName, partitionName, Status.BLOCKED,
          Reason.ERROR_STATE, current, target);
    }

    Set<String> unavailableTargetInstances = new HashSet<>(normalizedTarget._assignment.keySet());
    unavailableTargetInstances.removeAll(context.getLiveInstances());
    if (!unavailableTargetInstances.isEmpty()) {
      if (context.isDelayedResource(resourceName)) {
        return new PartitionEvaluation(resourceName, partitionName, Status.IN_PROGRESS,
            Reason.WAITING_FOR_DELAY, current, target);
      }
      return new PartitionEvaluation(resourceName, partitionName, Status.BLOCKED,
          Reason.TARGET_INSTANCE_NOT_LIVE, current, target);
    }

    return new PartitionEvaluation(resourceName, partitionName, Status.BLOCKED,
        Reason.NO_PROGRESS_PATH, current, target);
  }

  private ConvergenceStatus buildResourceStatus(ClusterEvent event, HelixManager manager,
      String resourceName, ResourceEvaluation evaluation, TargetFreshness freshness,
      OptimizerStatus optimizerStatus) {
    ConvergenceStatus status = new ConvergenceStatus(resourceName);
    populateCommon(status, event, manager);
    status.setScope(Scope.RESOURCE);
    status.setResourceName(resourceName);
    status.setStatus(evaluation._status);
    status.setPrimaryReason(evaluation._reason);
    status.setTargetFreshness(freshness);
    status.setOptimizerStatus(optimizerStatus);
    populateAggregate(status, evaluation._aggregate);
    List<PartitionDetail> details = new ArrayList<>();
    evaluation._details.forEach(detail -> details.add(detail.toDetail()));
    status.setPartitionDetails(details, ConvergenceStatus.DEFAULT_MAX_PARTITION_DETAILS);
    status.setTruncatedPartitionCount(
        Math.max(0, status.getAffectedPartitionCount() - details.size()));
    return status;
  }

  private ConvergenceStatus buildClusterStatus(ClusterEvent event, HelixManager manager,
      Aggregate aggregate, Map<String, ConvergenceStatus> resourceStatuses,
      List<PartitionEvaluation> details) {
    ConvergenceStatus status = new ConvergenceStatus(manager.getClusterName());
    populateCommon(status, event, manager);
    status.setScope(Scope.CLUSTER);
    status.setStatus(overallResourceStatus(resourceStatuses.values()));
    status.setPrimaryReason(primaryResourceReason(status.getStatus(), resourceStatuses.values()));
    status.setTargetFreshness(clusterFreshness(resourceStatuses.values()));
    status.setOptimizerStatus(clusterOptimizerStatus(resourceStatuses.values()));
    status.setTotalResourceCount(resourceStatuses.size());
    populateAggregate(status, aggregate);
    List<PartitionDetail> partitionDetails = new ArrayList<>();
    details.forEach(detail -> partitionDetails.add(detail.toDetail()));
    status.setPartitionDetails(partitionDetails, ConvergenceStatus.DEFAULT_MAX_PARTITION_DETAILS);
    status.setTruncatedPartitionCount(
        Math.max(0, status.getAffectedPartitionCount() - partitionDetails.size()));
    return status;
  }

  private void populateCommon(ConvergenceStatus status, ClusterEvent event, HelixManager manager) {
    status.setGeneratedAt(System.currentTimeMillis());
    status.setControllerSessionId(manager.getSessionId());
    status.setSourceEventId(event.getEventId());
    status.setComplete(true);
  }

  private void populateAggregate(ConvergenceStatus status, Aggregate aggregate) {
    status.setTotalPartitionCount(aggregate._total);
    status.setConvergedPartitionCount(aggregate.count(Status.CONVERGED));
    status.setInProgressPartitionCount(aggregate.count(Status.IN_PROGRESS));
    status.setBlockedPartitionCount(aggregate.count(Status.BLOCKED));
    status.setUnknownPartitionCount(aggregate.count(Status.UNKNOWN));
    status.setAffectedPartitionCount(
        aggregate._total - aggregate.count(Status.CONVERGED) - aggregate.count(Status.PAUSED));
    status.setStatusCounts(aggregate._statusCounts);
    status.setReasonCounts(aggregate._reasonCounts);
  }

  private static Status overallResourceStatus(Collection<ConvergenceStatus> statuses) {
    Status result = Status.CONVERGED;
    for (ConvergenceStatus status : statuses) {
      if (statusSeverity(status.getStatus()) > statusSeverity(result)) {
        result = status.getStatus();
      }
    }
    return result;
  }

  private static Reason primaryResourceReason(Status status,
      Collection<ConvergenceStatus> statuses) {
    return statuses.stream().filter(value -> value.getStatus() == status)
        .map(ConvergenceStatus::getPrimaryReason)
        .min(Comparator.comparingInt(ConvergenceStatusCalculator::reasonPriority))
        .orElse(Reason.NONE);
  }

  private static TargetFreshness clusterFreshness(Collection<ConvergenceStatus> statuses) {
    if (statuses.stream()
        .anyMatch(status -> status.getTargetFreshness() == TargetFreshness.LAST_KNOWN_GOOD)) {
      return TargetFreshness.LAST_KNOWN_GOOD;
    }
    if (statuses.stream()
        .anyMatch(status -> status.getTargetFreshness() == TargetFreshness.UNKNOWN)) {
      return TargetFreshness.UNKNOWN;
    }
    return TargetFreshness.CURRENT;
  }

  private static OptimizerStatus clusterOptimizerStatus(
      Collection<ConvergenceStatus> statuses) {
    if (statuses.stream()
        .anyMatch(status -> status.getOptimizerStatus() == OptimizerStatus.BASELINE_FAILED)) {
      return OptimizerStatus.BASELINE_FAILED;
    }
    if (statuses.stream()
        .anyMatch(status -> status.getOptimizerStatus() == OptimizerStatus.UNKNOWN)) {
      return OptimizerStatus.UNKNOWN;
    }
    if (statuses.stream()
        .anyMatch(status -> status.getOptimizerStatus() == OptimizerStatus.HEALTHY)) {
      return OptimizerStatus.HEALTHY;
    }
    return OptimizerStatus.NOT_APPLICABLE;
  }

  private static ResourceEvaluation overrideUnknown(ResourceEvaluation evaluation, Reason reason) {
    Aggregate aggregate = new Aggregate();
    for (int i = 0; i < evaluation._aggregate._total; i++) {
      aggregate.record(Status.UNKNOWN, reason);
    }
    List<PartitionEvaluation> details = new ArrayList<>();
    evaluation._details.forEach(detail -> details.add(
        new PartitionEvaluation(detail._resourceName, detail._partitionName, Status.UNKNOWN,
            reason, detail._current, detail._target)));
    return new ResourceEvaluation(Status.UNKNOWN, reason, aggregate, details);
  }

  private static Reason wagedFailureReason(WagedRebalanceStatus status, Reason defaultReason) {
    HelixRebalanceException.FailureCategory category = status.getServingFailureCategory();
    if (category == null) {
      return defaultReason;
    }
    switch (category) {
      case CAPACITY_DEFICIT:
        return Reason.WAGED_CAPACITY_DEFICIT;
      case NO_CANDIDATE_NODE:
        return Reason.WAGED_NO_CANDIDATE_NODE;
      case INVALID_RESOURCE_CONFIG:
      case INVALID_CLUSTER_CONFIG:
        return Reason.WAGED_INVALID_CONFIGURATION;
      case METADATA_STORE_IO:
      case ALGORITHM_INTERNAL:
      case ASYNC_EXECUTION:
      case UNKNOWN:
      default:
        return Reason.WAGED_INTERNAL_FAILURE;
    }
  }

  private static boolean hasMessages(MessageOutput output, String resourceName,
      Partition partition) {
    return output != null && !output.getMessages(resourceName, partition).isEmpty();
  }

  private static NormalizedAssignment normalize(Map<String, String> assignment,
      String initialState) {
    Map<String, String> result = new HashMap<>();
    for (Map.Entry<String, String> entry : assignment.entrySet()) {
      String instance = entry.getKey();
      String state = entry.getValue();
      if (instance == null || instance.isEmpty() || state == null || state.isEmpty()) {
        return new NormalizedAssignment(Collections.emptyMap(), false);
      }
      if (state.equalsIgnoreCase(HelixDefinedState.DROPPED.name())
          || initialState != null && state.equalsIgnoreCase(initialState)) {
        continue;
      }
      result.put(instance, state);
    }
    return new NormalizedAssignment(Collections.unmodifiableMap(result), true);
  }

  private static boolean containsState(Map<String, String> assignment, String state) {
    return assignment.values().stream()
        .anyMatch(value -> value != null && value.equalsIgnoreCase(state));
  }

  private static Map<String, String> copy(Map<String, String> input) {
    return input == null || input.isEmpty() ? Collections.emptyMap() : new HashMap<>(input);
  }

  private static void addBoundedDetail(List<PartitionEvaluation> details,
      PartitionEvaluation evaluation) {
    details.add(evaluation);
    details.sort(DETAIL_ORDER);
    if (details.size() > ConvergenceStatus.DEFAULT_MAX_PARTITION_DETAILS) {
      details.remove(details.size() - 1);
    }
  }

  private static int statusSeverity(Status status) {
    switch (status) {
      case PAUSED:
        return 5;
      case BLOCKED:
        return 4;
      case UNKNOWN:
        return 3;
      case IN_PROGRESS:
        return 2;
      case CONVERGED:
      default:
        return 1;
    }
  }

  private static int reasonPriority(Reason reason) {
    switch (reason) {
      case MANAGEMENT_MODE:
      case MAINTENANCE_MODE:
        return 1;
      case WAGED_LAST_KNOWN_GOOD:
      case WAGED_CAPACITY_DEFICIT:
      case WAGED_NO_CANDIDATE_NODE:
      case WAGED_INVALID_CONFIGURATION:
      case WAGED_INTERNAL_FAILURE:
        return 2;
      case TARGET_ASSIGNMENT_MISSING:
      case INVALID_STATE:
        return 3;
      case ERROR_STATE:
      case MESSAGE_DISPATCH_FAILED:
      case NO_PROGRESS_PATH:
        return 4;
      case TARGET_INSTANCE_NOT_LIVE:
        return 5;
      case MESSAGE_THROTTLED:
      case STATE_CONSTRAINT_WAIT:
        return 6;
      case PENDING_TRANSITION:
      case CANCELLATION_PENDING:
      case RELAY_PENDING:
      case TRANSITION_DISPATCHED:
      default:
        return 7;
    }
  }

  private static final class NormalizedAssignment {
    private final Map<String, String> _assignment;
    private final boolean _valid;

    private NormalizedAssignment(Map<String, String> assignment, boolean valid) {
      _assignment = assignment;
      _valid = valid;
    }
  }

  private static final class PartitionEvaluation {
    private final String _resourceName;
    private final String _partitionName;
    private final Status _status;
    private final Reason _reason;
    private final Map<String, String> _current;
    private final Map<String, String> _target;

    private PartitionEvaluation(String resourceName, String partitionName, Status status,
        Reason reason, Map<String, String> current, Map<String, String> target) {
      _resourceName = resourceName;
      _partitionName = partitionName;
      _status = status;
      _reason = reason;
      _current = current;
      _target = target;
    }

    private PartitionDetail toDetail() {
      return new PartitionDetail(_resourceName, _partitionName, _status, _reason, _current,
          _target);
    }
  }

  private static final class ResourceEvaluation {
    private final Status _status;
    private final Reason _reason;
    private final Aggregate _aggregate;
    private final List<PartitionEvaluation> _details;

    private ResourceEvaluation(Status status, Reason reason, Aggregate aggregate,
        List<PartitionEvaluation> details) {
      _status = status;
      _reason = reason;
      _aggregate = aggregate;
      _details = details;
    }
  }

  private static final class Aggregate {
    private final Map<Status, Integer> _statusCounts = new EnumMap<>(Status.class);
    private final Map<Reason, Integer> _reasonCounts = new EnumMap<>(Reason.class);
    private int _total;

    private void record(Status status, Reason reason) {
      _total++;
      _statusCounts.merge(status, 1, Integer::sum);
      if (reason != Reason.NONE) {
        _reasonCounts.merge(reason, 1, Integer::sum);
      }
    }

    private void add(Aggregate other) {
      _total += other._total;
      other._statusCounts.forEach((key, value) -> _statusCounts.merge(key, value, Integer::sum));
      other._reasonCounts.forEach((key, value) -> _reasonCounts.merge(key, value, Integer::sum));
    }

    private int count(Status status) {
      return _statusCounts.getOrDefault(status, 0);
    }

    private Status overallStatus() {
      return _statusCounts.keySet().stream()
          .max(Comparator.comparingInt(ConvergenceStatusCalculator::statusSeverity))
          .orElse(Status.CONVERGED);
    }

    private Reason primaryReason(Status status) {
      if (status == Status.CONVERGED) {
        return Reason.NONE;
      }
      return _reasonCounts.keySet().stream()
          .min(Comparator.comparingInt(ConvergenceStatusCalculator::reasonPriority))
          .orElse(Reason.NONE);
    }
  }

  private static final class DispatchIndex {
    private final Set<String> _sent = new HashSet<>();
    private final Set<String> _failed = new HashSet<>();

    private DispatchIndex(MessageDispatchResult result) {
      if (result != null) {
        index(result.getSentMessages(), _sent);
        index(result.getFailedMessages(), _failed);
      }
    }

    private boolean wasSent(String resourceName, String partitionName) {
      return _sent.contains(key(resourceName, partitionName));
    }

    private boolean failed(String resourceName, String partitionName) {
      return _failed.contains(key(resourceName, partitionName));
    }

    private static void index(List<Message> messages, Set<String> output) {
      for (Message message : messages) {
        List<String> partitionNames = message.getPartitionNames();
        if (partitionNames.isEmpty() && message.getPartitionName() != null) {
          output.add(key(message.getResourceName(), message.getPartitionName()));
        } else {
          partitionNames.forEach(
              partitionName -> output.add(key(message.getResourceName(), partitionName)));
        }
      }
    }

    private static String key(String resourceName, String partitionName) {
      return resourceName + '\u0000' + partitionName;
    }
  }
}
