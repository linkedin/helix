package org.apache.helix.manager.zk;

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
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;
import org.apache.helix.BaseDataAccessor;
import org.apache.helix.HelixDataAccessor;
import org.apache.helix.HelixDefinedState;
import org.apache.helix.HelixException;
import org.apache.helix.HelixProperty;
import org.apache.helix.PropertyKey;
import org.apache.helix.PropertyPathBuilder;
import org.apache.helix.manager.zk.evacuation.PartitionExclusionHelper;
import org.apache.helix.manager.zk.evacuation.PartitionInfo;
import org.apache.helix.model.CurrentState;
import org.apache.helix.model.IdealState;
import org.apache.helix.model.InstanceConfig;
import org.apache.helix.model.InstanceReplicaStatus;
import org.apache.helix.model.InstanceReplicaStatus.AssignmentObservation;
import org.apache.helix.model.InstanceReplicaStatus.Blocker;
import org.apache.helix.model.InstanceReplicaStatus.BlockerCode;
import org.apache.helix.model.InstanceReplicaStatus.CoverageStatus;
import org.apache.helix.model.InstanceReplicaStatus.DrainObservation;
import org.apache.helix.model.InstanceReplicaStatus.ReplicaInfo;
import org.apache.helix.model.InstanceReplicaStatus.ReplicaStateObservation;
import org.apache.helix.model.InstanceReplicaStatus.ResourceEvaluation;
import org.apache.helix.model.InstanceReplicaStatus.ResourceInfo;
import org.apache.helix.model.LiveInstance;
import org.apache.helix.task.TaskConstants;
import org.apache.helix.zookeeper.datamodel.ZNRecord;

/**
 * Computes an instance-scoped replica observation from Helix metadata.
 */
final class InstanceReplicaStatusCalculator {
  private static final Set<IdealState.RebalanceMode> SUPPORTED_REBALANCE_MODES =
      Collections.unmodifiableSet(new LinkedHashSet<>(Arrays.asList(
          IdealState.RebalanceMode.FULL_AUTO, IdealState.RebalanceMode.CUSTOMIZED)));
  private static final List<String> SUPPORTED_REBALANCE_MODE_NAMES =
      Collections.unmodifiableList(Arrays.asList(IdealState.RebalanceMode.FULL_AUTO.name(),
          IdealState.RebalanceMode.CUSTOMIZED.name()));

  private InstanceReplicaStatusCalculator() {
  }

  static InstanceReplicaStatus calculate(HelixDataAccessor accessor,
      BaseDataAccessor<ZNRecord> baseAccessor, String clusterName, String instanceName) {
    PropertyKey.Builder keyBuilder = accessor.keyBuilder();
    List<Blocker> blockers = new ArrayList<>();

    InstanceConfig instanceConfig =
        getProperty(accessor, keyBuilder.instanceConfig(instanceName));
    CoverageStatus assignmentCoverage = instanceConfig == null
        ? CoverageStatus.INCOMPLETE : CoverageStatus.COMPLETE;
    String instanceOperation = null;
    boolean futureAssignmentEligible = false;
    if (instanceConfig == null) {
      blockers.add(blocker(BlockerCode.INSTANCE_CONFIG_MISSING,
          "Instance configuration is missing.", null, null, null));
    } else {
      instanceOperation = instanceConfig.getInstanceOperation().getOperation().name();
      futureAssignmentEligible = instanceConfig.getInstanceEnabled();
    }

    LiveInstance initialLiveInstance =
        getProperty(accessor, keyBuilder.liveInstance(instanceName));
    boolean live = initialLiveInstance != null;
    String activeSessionId = live ? initialLiveInstance.getEphemeralOwner() : null;
    List<String> currentStateSessions =
        getChildNames(baseAccessor,
            PropertyPathBuilder.instanceCurrentState(clusterName, instanceName));

    CoverageStatus replicaCoverage = CoverageStatus.COMPLETE;
    CoverageStatus drainCoverage =
        instanceConfig == null ? CoverageStatus.INCOMPLETE : CoverageStatus.COMPLETE;
    Set<String> sessionsToRead = new TreeSet<>();
    if (live) {
      if (activeSessionId == null || activeSessionId.isEmpty()) {
        replicaCoverage = CoverageStatus.INCOMPLETE;
        drainCoverage = CoverageStatus.INCOMPLETE;
        blockers.add(blocker(BlockerCode.LIVE_INSTANCE_SESSION_MISSING,
            "The live instance does not identify an active session.", null, null, null));
      } else {
        sessionsToRead.add(activeSessionId);
      }
      sessionsToRead.addAll(currentStateSessions);
      if (currentStateSessions.size() > 1) {
        replicaCoverage = CoverageStatus.INCOMPLETE;
        drainCoverage = CoverageStatus.INCOMPLETE;
        blockers.add(blocker(BlockerCode.MULTIPLE_CURRENT_STATE_SESSIONS,
            "Multiple current-state sessions were observed.", null, null,
            currentStateSessions.size()));
      }
      if (!currentStateSessions.isEmpty()
          && (activeSessionId == null || !currentStateSessions.contains(activeSessionId))) {
        replicaCoverage = CoverageStatus.INCOMPLETE;
        drainCoverage = CoverageStatus.INCOMPLETE;
        blockers.add(blocker(BlockerCode.CURRENT_STATE_SESSION_MISMATCH,
            "Current-state sessions do not include the active live session.", activeSessionId,
            null, currentStateSessions.size()));
      }
    } else {
      sessionsToRead.addAll(currentStateSessions);
      if (currentStateSessions.size() > 1) {
        replicaCoverage = CoverageStatus.INCOMPLETE;
        drainCoverage = CoverageStatus.INCOMPLETE;
        blockers.add(blocker(BlockerCode.MULTIPLE_CURRENT_STATE_SESSIONS,
            "Multiple current-state sessions were observed.", null, null,
            currentStateSessions.size()));
      }
    }

    List<ObservedCurrentState> observedCurrentStates = new ArrayList<>();
    Set<String> excludedTaskResources = new TreeSet<>();
    for (String sessionId : sessionsToRead) {
      Map<String, CurrentState> currentStates =
          accessor.getChildValuesMap(keyBuilder.currentStates(instanceName, sessionId), true);
      if (currentStates == null) {
        replicaCoverage = CoverageStatus.INCOMPLETE;
        drainCoverage = CoverageStatus.INCOMPLETE;
        blockers.add(blocker(BlockerCode.CURRENT_STATE_RECORD_MISSING,
            "Current-state metadata could not be enumerated.", sessionId, null, null));
        continue;
      }
      for (String resourceName : new TreeSet<>(currentStates.keySet())) {
        CurrentState currentState = currentStates.get(resourceName);
        if (currentState == null) {
          replicaCoverage = CoverageStatus.INCOMPLETE;
          drainCoverage = CoverageStatus.INCOMPLETE;
          blockers.add(blocker(BlockerCode.CURRENT_STATE_RECORD_MISSING,
              "A current-state record disappeared during the observation.", sessionId,
              resourceName, null));
          continue;
        }
        if (TaskConstants.STATE_MODEL_NAME.equals(currentState.getStateModelDefRef())) {
          excludedTaskResources.add(resourceName);
          continue;
        }
        observedCurrentStates.add(
            new ObservedCurrentState(sessionId, resourceName, currentState));
      }
    }
    excludedTaskResources.addAll(
        getTaskResources(baseAccessor, clusterName, instanceName));

    Map<String, Integer> stateCounts = new TreeMap<>();
    Map<String, Integer> resourceReplicaCounts = new TreeMap<>();
    List<ReplicaInfo> errorReplicas = new ArrayList<>();
    int replicaCount = 0;
    for (ObservedCurrentState observed : observedCurrentStates) {
      resourceReplicaCounts.putIfAbsent(observed.resourceName, 0);
      Map<String, String> partitionStateMap = observed.currentState.getPartitionStateMap();
      if (partitionStateMap == null || partitionStateMap.isEmpty()) {
        continue;
      }
      for (String partitionName : new TreeSet<>(partitionStateMap.keySet())) {
        replicaCount++;
        resourceReplicaCounts.merge(observed.resourceName, 1, Integer::sum);
        String state = partitionStateMap.get(partitionName);
        if (state == null || state.isEmpty()) {
          replicaCoverage = CoverageStatus.INCOMPLETE;
          drainCoverage = CoverageStatus.INCOMPLETE;
          blockers.add(blocker(BlockerCode.REPLICA_STATE_MISSING,
              "A replica has no current state.", observed.sessionId, observed.resourceName, 1));
          continue;
        }
        stateCounts.merge(state, 1, Integer::sum);
        if (HelixDefinedState.ERROR.name().equals(state)) {
          errorReplicas.add(new ReplicaInfo(observed.sessionId, observed.resourceName,
              partitionName, state));
        }
      }
    }
    errorReplicas.sort(Comparator.comparing(ReplicaInfo::getSessionId)
        .thenComparing(ReplicaInfo::getResourceName)
        .thenComparing(ReplicaInfo::getPartitionName));

    Map<String, IdealState> idealStates =
        accessor.getChildValuesMap(keyBuilder.idealStates(), true);
    if (idealStates == null) {
      drainCoverage = CoverageStatus.INCOMPLETE;
      blockers.add(blocker(BlockerCode.IDEAL_STATE_MISSING,
          "Ideal-state metadata could not be enumerated.", null, null, null));
      idealStates = Collections.emptyMap();
    }
    Map<String, ResourceEvaluation> resourceEvaluations = new TreeMap<>();
    Map<String, String> resourceModes = new TreeMap<>();
    for (String resourceName : resourceReplicaCounts.keySet()) {
      IdealState idealState = idealStates.get(resourceName);
      if (idealState == null) {
        resourceEvaluations.put(resourceName, ResourceEvaluation.MISSING_IDEAL_STATE);
        drainCoverage = CoverageStatus.INCOMPLETE;
        blockers.add(blocker(BlockerCode.IDEAL_STATE_MISSING,
            "Ideal-state metadata is missing for a current-state resource.", null, resourceName,
            resourceReplicaCounts.get(resourceName)));
        continue;
      }

      IdealState.RebalanceMode rebalanceMode = idealState.getRebalanceMode();
      resourceModes.put(resourceName, rebalanceMode.name());
      if (SUPPORTED_REBALANCE_MODES.contains(rebalanceMode)) {
        resourceEvaluations.put(resourceName, ResourceEvaluation.EVALUATED);
      } else {
        resourceEvaluations.put(resourceName,
            ResourceEvaluation.UNSUPPORTED_REBALANCE_MODE);
        if (drainCoverage == CoverageStatus.COMPLETE) {
          drainCoverage = CoverageStatus.UNSUPPORTED;
        }
        blockers.add(blocker(BlockerCode.UNSUPPORTED_REBALANCE_MODE,
            "The resource rebalance mode is outside the native drain scope.", null, resourceName,
            resourceReplicaCounts.get(resourceName)));
      }
    }

    List<String> pendingMessages =
        live ? accessor.getChildNames(keyBuilder.messages(instanceName)) : Collections.emptyList();
    if (pendingMessages == null) {
      throw new HelixException(
          "Failed to enumerate pending messages for instance " + instanceName + ".");
    }
    int pendingMessageCount = pendingMessages.size();
    if (pendingMessageCount > 0) {
      blockers.add(blocker(BlockerCode.PENDING_MESSAGES,
          "Pending messages remain on the live instance.", null, null, pendingMessageCount));
    }

    Map<String, Integer> drainBlockingCounts = new HashMap<>();
    Set<String> evaluatedResourceNames = new TreeSet<>();
    for (Map.Entry<String, ResourceEvaluation> entry : resourceEvaluations.entrySet()) {
      if (entry.getValue() == ResourceEvaluation.EVALUATED) {
        evaluatedResourceNames.add(entry.getKey());
      }
    }
    if (replicaCoverage == CoverageStatus.COMPLETE && !evaluatedResourceNames.isEmpty()) {
      List<CurrentState> currentStates = new ArrayList<>();
      for (ObservedCurrentState observed : observedCurrentStates) {
        if (evaluatedResourceNames.contains(observed.resourceName)) {
          currentStates.add(observed.currentState);
        }
      }
      List<PartitionInfo> blockingPartitions;
      if (live) {
        blockingPartitions =
            PartitionExclusionHelper.collectPartitions(currentStates, evaluatedResourceNames);
      } else {
        blockingPartitions =
            PartitionExclusionHelper.getCustomizedPartitionsStillOnInstance(currentStates,
                new ArrayList<>(idealStates.values()), instanceName, evaluatedResourceNames,
                Collections.emptyMap());
      }
      for (PartitionInfo partition : blockingPartitions) {
        drainBlockingCounts.merge(partition.getResourceName(), 1, Integer::sum);
      }
    }

    List<ResourceInfo> resourceInfos = new ArrayList<>();
    for (String resourceName : resourceReplicaCounts.keySet()) {
      ResourceEvaluation evaluation = resourceEvaluations.get(resourceName);
      int blockingCount = drainBlockingCounts.getOrDefault(resourceName, 0);
      resourceInfos.add(new ResourceInfo(resourceName, resourceModes.get(resourceName),
          resourceReplicaCounts.get(resourceName), blockingCount, evaluation));
      if (blockingCount > 0) {
        BlockerCode code = live ? BlockerCode.LIVE_REPLICAS_REMAIN
            : BlockerCode.CUSTOMIZED_ASSIGNMENTS_REMAIN;
        String message = live
            ? "Replicas remain in current state on the live instance."
            : "Customized assignments still reference the offline instance.";
        blockers.add(blocker(code, message, null, resourceName, blockingCount));
      }
    }

    LiveInstance finalLiveInstance =
        getProperty(accessor, keyBuilder.liveInstance(instanceName));
    InstanceConfig finalInstanceConfig =
        getProperty(accessor, keyBuilder.instanceConfig(instanceName));
    List<String> finalCurrentStateSessions =
        getChildNames(baseAccessor,
            PropertyPathBuilder.instanceCurrentState(clusterName, instanceName));
    String finalActiveSessionId =
        finalLiveInstance == null ? null : finalLiveInstance.getEphemeralOwner();
    String finalInstanceOperation = finalInstanceConfig == null ? null
        : finalInstanceConfig.getInstanceOperation().getOperation().name();
    if (live != (finalLiveInstance != null)
        || !Objects.equals(activeSessionId, finalActiveSessionId)
        || !currentStateSessions.equals(finalCurrentStateSessions)) {
      replicaCoverage = CoverageStatus.INCOMPLETE;
      drainCoverage = CoverageStatus.INCOMPLETE;
      blockers.add(blocker(BlockerCode.OBSERVATION_CHANGED_DURING_READ,
          "Live-instance or current-state session metadata changed during the observation.",
          finalActiveSessionId, null, null));
    }
    if (!Objects.equals(instanceOperation, finalInstanceOperation)) {
      assignmentCoverage = CoverageStatus.INCOMPLETE;
      drainCoverage = CoverageStatus.INCOMPLETE;
      blockers.add(blocker(BlockerCode.OBSERVATION_CHANGED_DURING_READ,
          "Instance-operation metadata changed during the observation.", null, null, null));
    }

    boolean replicasEmpty = replicaCount == 0;
    boolean stateCoverageComplete = replicaCoverage == CoverageStatus.COMPLETE;
    boolean hasReplicasWithStates =
        stateCoverageComplete && replicaCount > 0 && totalCount(stateCounts) == replicaCount;
    boolean allOffline = hasReplicasWithStates
        && stateCounts.getOrDefault("OFFLINE", 0) == replicaCount;
    boolean allOfflineOrError = hasReplicasWithStates
        && stateCounts.getOrDefault("OFFLINE", 0)
        + stateCounts.getOrDefault(HelixDefinedState.ERROR.name(), 0) == replicaCount;
    boolean allError = hasReplicasWithStates
        && stateCounts.getOrDefault(HelixDefinedState.ERROR.name(), 0) == replicaCount;
    boolean drained = drainCoverage == CoverageStatus.COMPLETE
        && pendingMessageCount == 0 && drainBlockingCounts.isEmpty();

    ReplicaStateObservation replicaStateObservation =
        new ReplicaStateObservation(replicaCoverage, replicaCount, replicasEmpty, allOffline,
            allOfflineOrError, allError, stateCounts, errorReplicas,
            new ArrayList<>(excludedTaskResources));
    DrainObservation drainObservation =
        new DrainObservation(drainCoverage, drained, pendingMessageCount,
            SUPPORTED_REBALANCE_MODE_NAMES, resourceInfos, blockers);
    AssignmentObservation assignmentObservation =
        new AssignmentObservation(assignmentCoverage, futureAssignmentEligible,
            instanceOperation);
    return new InstanceReplicaStatus(System.currentTimeMillis(), clusterName, instanceName, live,
        activeSessionId, currentStateSessions, replicaStateObservation, drainObservation,
        assignmentObservation);
  }

  private static List<String> getTaskResources(BaseDataAccessor<ZNRecord> baseAccessor,
      String clusterName, String instanceName) {
    Set<String> resources = new TreeSet<>();
    for (String sessionId : getChildNames(baseAccessor,
        PropertyPathBuilder.instanceTaskCurrentState(clusterName, instanceName))) {
      resources.addAll(getChildNames(baseAccessor,
          PropertyPathBuilder.instanceTaskCurrentState(clusterName, instanceName, sessionId)));
    }
    return new ArrayList<>(resources);
  }

  private static List<String> getChildNames(BaseDataAccessor<ZNRecord> baseAccessor, String path) {
    List<String> childNames = baseAccessor.getChildNames(path, 0);
    if (childNames == null) {
      throw new HelixException("Failed to enumerate metadata children at " + path + ".");
    }
    if (childNames.isEmpty()) {
      return Collections.emptyList();
    }
    List<String> sorted = new ArrayList<>(childNames);
    Collections.sort(sorted);
    return sorted;
  }

  private static <T extends HelixProperty> T getProperty(HelixDataAccessor accessor,
      PropertyKey key) {
    List<T> properties = accessor.getProperty(Collections.singletonList(key), true);
    return properties == null || properties.isEmpty() ? null : properties.get(0);
  }

  private static int totalCount(Map<String, Integer> counts) {
    return counts.values().stream().mapToInt(Integer::intValue).sum();
  }

  private static Blocker blocker(BlockerCode code, String message, String sessionId,
      String resourceName, Integer count) {
    return new Blocker(code, message, sessionId, resourceName, count);
  }

  private static final class ObservedCurrentState {
    private final String sessionId;
    private final String resourceName;
    private final CurrentState currentState;

    private ObservedCurrentState(String sessionId, String resourceName,
        CurrentState currentState) {
      this.sessionId = sessionId;
      this.resourceName = resourceName;
      this.currentState = currentState;
    }
  }
}
