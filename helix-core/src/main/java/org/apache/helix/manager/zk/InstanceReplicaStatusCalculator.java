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
import org.apache.helix.HelixProperty;
import org.apache.helix.PropertyKey;
import org.apache.helix.PropertyPathBuilder;
import org.apache.helix.manager.zk.evacuation.PartitionExclusionHelper;
import org.apache.helix.manager.zk.evacuation.PartitionInfo;
import org.apache.helix.model.CurrentState;
import org.apache.helix.model.IdealState;
import org.apache.helix.model.InstanceConfig;
import org.apache.helix.model.InstanceReplicaStatus;
import org.apache.helix.model.InstanceReplicaStatus.CoverageStatus;
import org.apache.helix.model.LiveInstance;
import org.apache.helix.model.StateModelDefinition;
import org.apache.helix.task.TaskConstants;
import org.apache.helix.zookeeper.datamodel.ZNRecord;

/**
 * Computes an instance-scoped replica observation from Helix metadata.
 */
final class InstanceReplicaStatusCalculator {
  private static final Set<IdealState.RebalanceMode> SUPPORTED_REBALANCE_MODES =
      Collections.unmodifiableSet(new LinkedHashSet<>(Arrays.asList(
          IdealState.RebalanceMode.FULL_AUTO, IdealState.RebalanceMode.CUSTOMIZED)));

  private InstanceReplicaStatusCalculator() {
  }

  static InstanceReplicaStatus calculate(HelixDataAccessor accessor,
      BaseDataAccessor<ZNRecord> baseAccessor, String clusterName, String instanceName) {
    PropertyKey.Builder keyBuilder = accessor.keyBuilder();
    List<String> blockers = new ArrayList<>();

    InstanceConfig instanceConfig =
        getProperty(accessor, keyBuilder.instanceConfig(instanceName));
    if (instanceConfig == null) {
      blockers.add("Instance configuration is missing.");
    }

    LiveInstance initialLiveInstance =
        getProperty(accessor, keyBuilder.liveInstance(instanceName));
    boolean live = initialLiveInstance != null;
    String activeSessionId = live ? initialLiveInstance.getEphemeralOwner() : null;
    List<String> currentStateSessions = getChildNames(baseAccessor,
        PropertyPathBuilder.instanceCurrentState(clusterName, instanceName));

    CoverageStatus replicaCoverage = CoverageStatus.COMPLETE;
    CoverageStatus drainCoverage =
        instanceConfig == null ? CoverageStatus.INCOMPLETE : CoverageStatus.COMPLETE;
    Set<String> sessionsToRead = new TreeSet<>();
    if (live) {
      if (activeSessionId == null || activeSessionId.isEmpty()) {
        replicaCoverage = CoverageStatus.INCOMPLETE;
        drainCoverage = CoverageStatus.INCOMPLETE;
        blockers.add("The live instance does not identify an active session.");
      } else {
        sessionsToRead.add(activeSessionId);
      }
      sessionsToRead.addAll(currentStateSessions);
      if (currentStateSessions.size() > 1) {
        replicaCoverage = CoverageStatus.INCOMPLETE;
        drainCoverage = CoverageStatus.INCOMPLETE;
        blockers.add("Multiple current-state sessions were observed: " + currentStateSessions);
      }
      if (!currentStateSessions.isEmpty()
          && (activeSessionId == null || !currentStateSessions.contains(activeSessionId))) {
        replicaCoverage = CoverageStatus.INCOMPLETE;
        drainCoverage = CoverageStatus.INCOMPLETE;
        blockers.add("Current-state sessions " + currentStateSessions
            + " do not include the active live session " + activeSessionId + ".");
      }
    } else {
      sessionsToRead.addAll(currentStateSessions);
      if (currentStateSessions.size() > 1) {
        replicaCoverage = CoverageStatus.INCOMPLETE;
        drainCoverage = CoverageStatus.INCOMPLETE;
        blockers.add("Multiple current-state sessions were observed: " + currentStateSessions);
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
        blockers.add("Current-state metadata could not be enumerated for session " + sessionId
            + ".");
        continue;
      }
      for (String resourceName : new TreeSet<>(currentStates.keySet())) {
        CurrentState currentState = currentStates.get(resourceName);
        if (currentState == null) {
          replicaCoverage = CoverageStatus.INCOMPLETE;
          drainCoverage = CoverageStatus.INCOMPLETE;
          blockers.add("The current-state record for resource " + resourceName
              + " disappeared during the observation.");
          continue;
        }
        if (TaskConstants.STATE_MODEL_NAME.equals(currentState.getStateModelDefRef())) {
          excludedTaskResources.add(resourceName);
          continue;
        }
        observedCurrentStates.add(new ObservedCurrentState(resourceName, currentState));
      }
    }
    excludedTaskResources.addAll(getTaskResources(baseAccessor, clusterName, instanceName));

    // A replica counts as "offline" when it sits in its own resource's initial state. That state is
    // state-model specific, so it is read per state model rather than assumed to be OFFLINE.
    Map<String, String> initialStatesByStateModel = new HashMap<>();
    Set<String> offlineStates = new TreeSet<>();
    Map<String, Integer> stateCounts = new TreeMap<>();
    Map<String, Integer> resourceReplicaCounts = new TreeMap<>();
    Set<String> errorPartitionNames = new TreeSet<>();
    int replicaCount = 0;
    int offlineReplicaCount = 0;
    for (ObservedCurrentState observed : observedCurrentStates) {
      Map<String, String> partitionStateMap = observed.currentState.getPartitionStateMap();
      if (partitionStateMap == null || partitionStateMap.isEmpty()) {
        continue;
      }
      String stateModelDefRef = observed.currentState.getStateModelDefRef();
      String initialState = null;
      if (stateModelDefRef != null) {
        // containsKey rather than computeIfAbsent, so an unresolvable state model is only read once.
        if (!initialStatesByStateModel.containsKey(stateModelDefRef)) {
          StateModelDefinition definition =
              getProperty(accessor, keyBuilder.stateModelDef(stateModelDefRef));
          initialStatesByStateModel.put(stateModelDefRef,
              definition == null ? null : definition.getInitialState());
        }
        initialState = initialStatesByStateModel.get(stateModelDefRef);
      }
      if (initialState == null || initialState.isEmpty()) {
        replicaCoverage = CoverageStatus.INCOMPLETE;
        drainCoverage = CoverageStatus.INCOMPLETE;
        blockers.add("The initial state of state model " + stateModelDefRef
            + " could not be resolved for resource " + observed.resourceName + ".");
      } else if (!HelixDefinedState.ERROR.name().equals(initialState)) {
        offlineStates.add(initialState);
      }
      resourceReplicaCounts.putIfAbsent(observed.resourceName, 0);
      for (String partitionName : new TreeSet<>(partitionStateMap.keySet())) {
        replicaCount++;
        resourceReplicaCounts.merge(observed.resourceName, 1, Integer::sum);
        String state = partitionStateMap.get(partitionName);
        if (state == null || state.isEmpty()) {
          replicaCoverage = CoverageStatus.INCOMPLETE;
          drainCoverage = CoverageStatus.INCOMPLETE;
          blockers.add("Partition " + partitionName + " of resource " + observed.resourceName
              + " has no current state.");
          continue;
        }
        stateCounts.merge(state, 1, Integer::sum);
        if (HelixDefinedState.ERROR.name().equals(state)) {
          errorPartitionNames.add(partitionName);
        } else if (state.equals(initialState)) {
          // Attributed per replica against its own resource's initial state, so a state that is
          // initial for one state model is not treated as offline for a resource using another.
          offlineReplicaCount++;
        }
      }
    }

    Map<String, IdealState> idealStates = accessor.getChildValuesMap(keyBuilder.idealStates(), true);
    if (idealStates == null) {
      drainCoverage = CoverageStatus.INCOMPLETE;
      blockers.add("Ideal-state metadata could not be enumerated.");
      idealStates = Collections.emptyMap();
    }
    Set<String> evaluatedResourceNames = new TreeSet<>();
    for (String resourceName : resourceReplicaCounts.keySet()) {
      IdealState idealState = idealStates.get(resourceName);
      if (idealState == null) {
        drainCoverage = CoverageStatus.INCOMPLETE;
        blockers.add("Ideal-state metadata is missing for current-state resource " + resourceName
            + ".");
        continue;
      }
      if (SUPPORTED_REBALANCE_MODES.contains(idealState.getRebalanceMode())) {
        evaluatedResourceNames.add(resourceName);
      } else {
        if (drainCoverage == CoverageStatus.COMPLETE) {
          drainCoverage = CoverageStatus.UNSUPPORTED;
        }
        blockers.add("Resource " + resourceName + " uses rebalance mode "
            + idealState.getRebalanceMode() + ", which is outside the native drain scope.");
      }
    }

    List<String> pendingMessages =
        live ? accessor.getChildNames(keyBuilder.messages(instanceName)) : Collections.emptyList();
    if (pendingMessages == null) {
      drainCoverage = CoverageStatus.INCOMPLETE;
      blockers.add("Pending messages could not be enumerated.");
      pendingMessages = Collections.emptyList();
    }
    int pendingMessageCount = pendingMessages.size();
    if (pendingMessageCount > 0) {
      blockers.add(pendingMessageCount + " pending message(s) remain on the live instance.");
    }

    Map<String, Integer> drainBlockingCounts = new HashMap<>();
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
        blockingPartitions = PartitionExclusionHelper.getCustomizedPartitionsStillOnInstance(
            currentStates, new ArrayList<>(idealStates.values()), instanceName,
            evaluatedResourceNames, Collections.emptyMap());
      }
      for (PartitionInfo partition : blockingPartitions) {
        drainBlockingCounts.merge(partition.getResourceName(), 1, Integer::sum);
      }
    }
    for (Map.Entry<String, Integer> entry : drainBlockingCounts.entrySet()) {
      blockers.add(entry.getValue() + " replica(s) of resource " + entry.getKey()
          + (live ? " remain in current state on the live instance."
          : " are still referenced by a customized assignment on the offline instance."));
    }

    LiveInstance finalLiveInstance = getProperty(accessor, keyBuilder.liveInstance(instanceName));
    List<String> finalCurrentStateSessions = getChildNames(baseAccessor,
        PropertyPathBuilder.instanceCurrentState(clusterName, instanceName));
    String finalActiveSessionId =
        finalLiveInstance == null ? null : finalLiveInstance.getEphemeralOwner();
    if (live != (finalLiveInstance != null)
        || !Objects.equals(activeSessionId, finalActiveSessionId)
        || !currentStateSessions.equals(finalCurrentStateSessions)) {
      replicaCoverage = CoverageStatus.INCOMPLETE;
      drainCoverage = CoverageStatus.INCOMPLETE;
      blockers.add("Live-instance or current-state session metadata changed during the "
          + "observation.");
    }

    boolean drained = drainCoverage == CoverageStatus.COMPLETE && pendingMessageCount == 0
        && drainBlockingCounts.isEmpty();
    return new InstanceReplicaStatus(System.currentTimeMillis(), clusterName, instanceName, live,
        replicaCoverage, replicaCount, offlineReplicaCount, new ArrayList<>(offlineStates),
        stateCounts, new ArrayList<>(errorPartitionNames), new ArrayList<>(excludedTaskResources),
        drainCoverage, drained, pendingMessageCount, blockers);
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
    if (childNames == null || childNames.isEmpty()) {
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

  private static final class ObservedCurrentState {
    private final String resourceName;
    private final CurrentState currentState;

    private ObservedCurrentState(String resourceName, CurrentState currentState) {
      this.resourceName = resourceName;
      this.currentState = currentState;
    }
  }
}
