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
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

import org.apache.helix.HelixDefinedState;
import org.apache.helix.controller.dataproviders.ResourceControllerDataProvider;
import org.apache.helix.model.IdealState;
import org.apache.helix.model.Partition;
import org.apache.helix.model.StateModelDefinition;

/**
 * A record entry in cluster data cache tracking a partition whose active replica count has
 * dropped below its configured {@code minActiveReplicas}. It captures the moment the partition
 * was first observed below the minimum so that, once the count is restored, the controller can
 * compute how long the partition remained degraded (its recovery duration).
 * <p>
 * Attribution follows the replica activation that restores the required active-replica count.
 * Only execution on that recovery path is deducted; work on a parallel replica must not hide
 * controller delay on the required path. Ambiguous paths remain unavailable.
 * As with existing handoff monitoring, attribution uses epoch-millisecond timestamps and assumes
 * synchronized participant/controller clocks. Inconsistent observations remain unavailable.
 */
public class MissingMinActiveReplicaRecord {
  private static final int MAX_NEW_PARTICIPANTS = 64;

  private final long startTimeStamp;
  private final Map<String, ParticipantObservation> participantObservations = new HashMap<>();
  private final Map<String, RecoveryPath> recoveryPaths = new HashMap<>();
  private volatile String unavailableReason;
  private Integer minActiveReplicas;
  private String stateModel;
  private String initialState;
  private Set<String> activeStates;
  private int initialActiveReplicas;
  private long observationSequence = -1L;
  private boolean hasPreviousObservation;
  private int newParticipants;
  private Map<String, ParticipantVersion> producedObservation;
  private RecoveryConfiguration producedConfiguration;
  private long producedSequence;

  public MissingMinActiveReplicaRecord(long start) {
    startTimeStamp = start;
  }

  /* package */ long getStartTimeStamp() {
    return startTimeStamp;
  }

  /* package */ boolean hasObservation() {
    return observationSequence > 0;
  }

  /* package */ synchronized long captureObservation(CurrentStateOutput output, String resource,
      Partition partition, ResourceControllerDataProvider cache) {
    if (isAttributionInvalid()) {
      return -1L;
    }
    Map<String, ParticipantVersion> snapshot = snapshotVersion(output, resource, partition);
    RecoveryConfiguration configuration = new RecoveryConfiguration(cache, resource);
    if (!snapshot.equals(producedObservation) || !configuration.equals(producedConfiguration)) {
      producedObservation = snapshot;
      producedConfiguration = configuration;
      producedSequence++;
    }
    return producedSequence;
  }

  /* package */ synchronized long matchingObservation(CurrentStateOutput output, String resource,
      Partition partition, ResourceControllerDataProvider cache) {
    if (isAttributionInvalid()) {
      return -1L;
    }
    Map<String, ParticipantVersion> snapshot = snapshotVersion(output, resource, partition);
    RecoveryConfiguration configuration = new RecoveryConfiguration(cache, resource);
    if (producedObservation == null) {
      producedObservation = snapshot;
      producedConfiguration = configuration;
      producedSequence++;
    }
    return snapshot.equals(producedObservation) && configuration.equals(producedConfiguration)
        ? producedSequence : -1L;
  }

  private Map<String, ParticipantVersion> snapshotVersion(CurrentStateOutput output,
      String resource, Partition partition) {
    Map<String, ParticipantVersion> snapshot = new HashMap<>();
    for (Map.Entry<String, String> entry : output.getCurrentStateMap(resource, partition).entrySet()) {
      String instance = entry.getKey();
      snapshot.put(instance, new ParticipantVersion(output.getParticipantSession(instance),
          entry.getValue(), output.getEndTime(resource, partition, instance)));
    }
    return snapshot;
  }

  /* package */ void observeSequence(long sequence) {
    if (sequence <= 0 || (observationSequence >= 0
        && (sequence < observationSequence || sequence > observationSequence + 1))) {
      invalidateAttribution("recovery observations were skipped or reordered");
    }
    hasPreviousObservation |= observationSequence >= 0 && sequence != observationSequence;
    observationSequence = sequence;
  }

  /* package */ void observeConfiguration(int minimum, String model, String initial,
      Set<String> active) {
    if (minimum <= 0 || model == null || initial == null || active == null || active.isEmpty()) {
      invalidateAttribution("recovery configuration is unavailable");
      return;
    }
    if (minActiveReplicas == null) {
      minActiveReplicas = minimum;
      stateModel = model;
      initialState = initial;
      activeStates = new HashSet<>(active);
    } else if (minActiveReplicas != minimum || !Objects.equals(stateModel, model)
        || !Objects.equals(initialState, initial) || !activeStates.equals(active)) {
      invalidateAttribution("recovery configuration changed");
    }
  }

  /* package */ void observeParticipant(String instance, String session, String state,
      String previousState, long executionStart, long executionEnd) {
    if (unavailableReason != null) {
      return;
    }
    if (instance == null || session == null || state == null || activeStates == null) {
      invalidateAttribution("participant identity or state is unavailable");
      return;
    }
    ParticipantObservation previous = participantObservations.get(instance);
    if (previous == null && hasPreviousObservation && !Objects.equals(state, initialState)
        && !Objects.equals(previousState, initialState)) {
      invalidateAttribution("initial participant transition history is missing");
      return;
    }
    if (previous == null && hasPreviousObservation && newParticipants == MAX_NEW_PARTICIPANTS) {
      invalidateAttribution("participant tracking limit exceeded");
      return;
    }
    if (!activeStates.contains(state) && !state.equals(initialState)
        && !state.equals(HelixDefinedState.ERROR.name())
        && !state.equals(HelixDefinedState.DROPPED.name())) {
      invalidateAttribution("participant state is not in the recovery state model");
      return;
    }
    if (previous != null && session.equals(previous.session) && state.equals(previous.state)
        && executionStart == previous.start && executionEnd == previous.end) {
      return;
    }
    participantObservations.put(instance,
        new ParticipantObservation(session, state, executionStart, executionEnd));
    if (previous == null) {
      if (hasPreviousObservation) {
        newParticipants++;
      }
      boolean initiallyActive = !hasPreviousObservation && activeStates.contains(state);
      recoveryPaths.put(instance, new RecoveryPath(initiallyActive));
      if (!hasPreviousObservation) {
        if (executionEnd > startTimeStamp) {
          invalidateAttribution("initial observation contains a post-start transition");
        } else if (initiallyActive) {
          initialActiveReplicas++;
        }
        return;
      }
    }
    if (previous != null) {
      if (!session.equals(previous.session)) {
        invalidateAttribution("participant session changed");
        return;
      }
      boolean changedState = !state.equals(previous.state);
      boolean changedTiming = executionStart != previous.start || executionEnd != previous.end;
      if (changedState && !changedTiming) {
        invalidateAttribution("state changed without transition timing");
        return;
      }
      if (changedTiming && executionEnd >= startTimeStamp
          && !Objects.equals(previousState, previous.state)) {
        invalidateAttribution("intermediate transition history is missing");
        return;
      }
      if (executionStart >= 0 && executionEnd >= 0
          && (executionStart < previous.start || executionEnd < previous.end)) {
        invalidateAttribution("participant transition timestamps moved backwards");
        return;
      }
    }
    String fromState = previous == null ? initialState : previous.state;
    if (activeStates.contains(fromState) && !activeStates.contains(state)) {
      invalidateAttribution("an active replica was lost during recovery");
      return;
    }
    if (executionStart < 0 || executionEnd < 0) {
      invalidateAttribution("transition execution timing is unavailable");
      return;
    }
    if (executionEnd < executionStart) {
      invalidateAttribution("invalid participant execution interval");
      return;
    }
    if (executionEnd < startTimeStamp) {
      if (!state.equals(fromState)) {
        invalidateAttribution("transition timing precedes the recovery observation");
      }
      return;
    }
    if (previous != null && previous.end >= startTimeStamp && executionStart < previous.end) {
      invalidateAttribution("participant transitions overlap");
      return;
    }
    if (activeStates.contains(fromState)) {
      return;
    }
    if (state.equals(HelixDefinedState.DROPPED.name())) {
      invalidateAttribution("a recovery participant was dropped");
      return;
    }
    RecoveryPath path = recoveryPaths.get(instance);
    path.executionDuration += executionEnd - Math.max(startTimeStamp, executionStart);
    if (activeStates.contains(state)) {
      path.activeAt = executionEnd;
      path.executionAtActivation = path.executionDuration;
    } else if (path.firstInactiveStepEnd < 0) {
      path.firstInactiveStepEnd = executionEnd;
    }
  }

  /* package */ void finishObservation(Set<String> instances) {
    if (unavailableReason == null && !instances.containsAll(participantObservations.keySet())) {
      invalidateAttribution("participant history disappeared during recovery");
    }
  }

  /* package */ long getHelixLatency(long recoveryEnd) {
    if (startTimeStamp < 0 || recoveryEnd < startTimeStamp) {
      invalidateAttribution("invalid recovery timestamps");
    }
    if (unavailableReason != null || minActiveReplicas == null || !hasPreviousObservation) {
      return TopStateHandoffReportStage.TIMESTAMP_NOT_RECORDED;
    }
    List<RecoveryPath> recovered = new ArrayList<>();
    for (Map.Entry<String, ParticipantObservation> entry : participantObservations.entrySet()) {
      if (entry.getValue().end > recoveryEnd) {
        invalidateAttribution("participant execution ends after the observed recovery");
        return TopStateHandoffReportStage.TIMESTAMP_NOT_RECORDED;
      }
      RecoveryPath path = recoveryPaths.get(entry.getKey());
      if (!path.initiallyActive && activeStates.contains(entry.getValue().state)
          && path.activeAt >= startTimeStamp) {
        recovered.add(path);
      }
    }
    int required = minActiveReplicas - initialActiveReplicas;
    if (required <= 0 || recovered.size() < required) {
      invalidateAttribution("minimum restoration has insufficient timed recovery paths");
      return TopStateHandoffReportStage.TIMESTAMP_NOT_RECORDED;
    }
    recovered.sort(Comparator.comparingLong(path -> path.activeAt));
    long restoredAt = recovered.get(required - 1).activeAt;
    long helixLatency = TopStateHandoffReportStage.TIMESTAMP_NOT_RECORDED;
    for (RecoveryPath path : recovered) {
      if (path.activeAt != restoredAt) {
        continue;
      }
      for (RecoveryPath other : recoveryPaths.values()) {
        if (other != path && other.firstInactiveStepEnd >= startTimeStamp
            && other.firstInactiveStepEnd <= restoredAt) {
          invalidateAttribution("cross-participant retry dependencies are unavailable");
          return TopStateHandoffReportStage.TIMESTAMP_NOT_RECORDED;
        }
      }
      long candidate = recoveryEnd - startTimeStamp - path.executionAtActivation;
      if (candidate < 0 || (helixLatency >= 0 && candidate != helixLatency)) {
        invalidateAttribution("recovery critical path is ambiguous");
        return TopStateHandoffReportStage.TIMESTAMP_NOT_RECORDED;
      }
      helixLatency = candidate;
    }
    return helixLatency;
  }

  /* package */ synchronized void invalidateAttribution(String reason) {
    if (unavailableReason == null) {
      unavailableReason = reason;
    }
    recoveryPaths.clear();
    participantObservations.clear();
    producedObservation = null;
    producedConfiguration = null;
  }

  /* package */ String getUnavailableReason() {
    return unavailableReason == null ? "no complete recovery path observed" : unavailableReason;
  }

  /* package */ boolean isAttributionInvalid() {
    return unavailableReason != null;
  }

  private static class RecoveryPath {
    private final boolean initiallyActive;
    private long executionDuration;
    private long activeAt = -1L;
    private long executionAtActivation;
    private long firstInactiveStepEnd = -1L;

    private RecoveryPath(boolean initiallyActive) {
      this.initiallyActive = initiallyActive;
    }
  }

  private static class ParticipantVersion {
    private final String session;
    private final String state;
    private final long end;

    private ParticipantVersion(String session, String state, long end) {
      this.session = session;
      this.state = state;
      this.end = end;
    }

    @Override
    public boolean equals(Object other) {
      if (!(other instanceof ParticipantVersion)) {
        return false;
      }
      ParticipantVersion version = (ParticipantVersion) other;
      return end == version.end && Objects.equals(session, version.session)
          && Objects.equals(state, version.state);
    }

    @Override
    public int hashCode() {
      return Objects.hash(session, state, end);
    }
  }

  private static class RecoveryConfiguration {
    private final int minimum;
    private final String model;
    private final String initial;
    private final Set<String> active;
    private final boolean enabled;
    private final boolean maintenance;

    private RecoveryConfiguration(ResourceControllerDataProvider cache, String resource) {
      IdealState idealState = cache.getIdealState(resource);
      StateModelDefinition definition = idealState == null ? null
          : cache.getStateModelDef(idealState.getStateModelDefRef());
      int configuredMinimum = idealState == null ? -1 : idealState.getMinActiveReplicas();
      minimum = idealState != null && configuredMinimum < 0
          ? idealState.getReplicaCount(-1) : configuredMinimum;
      enabled = idealState != null && idealState.isEnabled();
      maintenance = cache.isMaintenanceModeEnabled();
      model = definition == null ? null : definition.getId();
      initial = definition == null ? null : definition.getInitialState();
      active = definition == null ? new HashSet<>() : new HashSet<>(definition.getStatesPriorityList());
      active.remove(initial);
      active.remove(HelixDefinedState.DROPPED.name());
      active.remove(HelixDefinedState.ERROR.name());
    }

    @Override
    public boolean equals(Object other) {
      if (!(other instanceof RecoveryConfiguration)) {
        return false;
      }
      RecoveryConfiguration configuration = (RecoveryConfiguration) other;
      return minimum == configuration.minimum && enabled == configuration.enabled
          && maintenance == configuration.maintenance && Objects.equals(model, configuration.model)
          && Objects.equals(initial, configuration.initial) && active.equals(configuration.active);
    }

    @Override
    public int hashCode() {
      return Objects.hash(minimum, model, initial, active, enabled, maintenance);
    }
  }

  private static class ParticipantObservation {
    private final String session;
    private final String state;
    private final long start;
    private final long end;

    private ParticipantObservation(String session, String state, long start, long end) {
      this.session = session;
      this.state = state;
      this.start = start;
      this.end = end;
    }
  }
}
