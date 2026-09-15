package org.apache.helix.model;

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
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

/**
 * Read-only observation of replica state, drain progress, and future assignment eligibility for
 * one instance.
 *
 * <p>The three observations intentionally remain separate. In particular, replicas being
 * {@code OFFLINE}, replicas being {@code OFFLINE} or {@code ERROR}, and an instance being drained
 * answer different questions. Callers must also inspect each observation's coverage before using
 * its result.
 */
public class InstanceReplicaStatus {
  /**
   * Current JSON contract version.
   */
  public static final int SCHEMA_VERSION = 1;

  /**
   * Whether all data required for an observation was evaluated.
   */
  public enum CoverageStatus {
    COMPLETE,
    INCOMPLETE,
    UNSUPPORTED
  }

  /**
   * Replica-state scope used by this contract.
   */
  public enum ReplicaStateScope {
    NON_TASK_CURRENT_STATES
  }

  /**
   * Drain scope supported by Helix's native drain calculation.
   */
  public enum DrainScope {
    FULL_AUTO_AND_CUSTOMIZED
  }

  /**
   * Assignment scope used by this contract.
   */
  public enum AssignmentScope {
    INSTANCE_OPERATION
  }

  /**
   * How a resource participated in the drain calculation.
   */
  public enum ResourceEvaluation {
    EVALUATED,
    MISSING_IDEAL_STATE,
    UNSUPPORTED_REBALANCE_MODE
  }

  /**
   * Stable blocker codes reported by the observation.
   */
  public enum BlockerCode {
    INSTANCE_CONFIG_MISSING,
    LIVE_INSTANCE_SESSION_MISSING,
    CURRENT_STATE_SESSION_MISMATCH,
    MULTIPLE_CURRENT_STATE_SESSIONS,
    CURRENT_STATE_RECORD_MISSING,
    REPLICA_STATE_MISSING,
    IDEAL_STATE_MISSING,
    UNSUPPORTED_REBALANCE_MODE,
    PENDING_MESSAGES,
    LIVE_REPLICAS_REMAIN,
    CUSTOMIZED_ASSIGNMENTS_REMAIN,
    OBSERVATION_CHANGED_DURING_READ
  }

  /**
   * Identifies a replica observed in current state.
   */
  public static class ReplicaInfo {
    private final String sessionId;
    private final String resourceName;
    private final String partitionName;
    private final String state;

    /**
     * Creates replica information.
     *
     * @param sessionId current-state session containing the replica
     * @param resourceName resource containing the replica
     * @param partitionName partition name
     * @param state observed state
     */
    public ReplicaInfo(String sessionId, String resourceName, String partitionName, String state) {
      this.sessionId = sessionId;
      this.resourceName = resourceName;
      this.partitionName = partitionName;
      this.state = state;
    }

    /**
     * @return the current-state session containing the replica
     */
    public String getSessionId() {
      return sessionId;
    }

    /**
     * @return the resource containing the replica
     */
    public String getResourceName() {
      return resourceName;
    }

    /**
     * @return the partition name
     */
    public String getPartitionName() {
      return partitionName;
    }

    /**
     * @return the observed replica state
     */
    public String getState() {
      return state;
    }
  }

  /**
   * Describes how one resource was covered by the drain calculation.
   */
  public static class ResourceInfo {
    private final String resourceName;
    private final String rebalanceMode;
    private final int observedReplicaCount;
    private final int drainBlockingReplicaCount;
    private final ResourceEvaluation evaluation;

    /**
     * Creates resource coverage information.
     *
     * @param resourceName resource name
     * @param rebalanceMode configured rebalance mode, or {@code null} when metadata is absent
     * @param observedReplicaCount replicas observed in current state
     * @param drainBlockingReplicaCount replicas that block the native drain calculation
     * @param evaluation how the resource was evaluated
     */
    public ResourceInfo(String resourceName, String rebalanceMode, int observedReplicaCount,
        int drainBlockingReplicaCount, ResourceEvaluation evaluation) {
      this.resourceName = resourceName;
      this.rebalanceMode = rebalanceMode;
      this.observedReplicaCount = observedReplicaCount;
      this.drainBlockingReplicaCount = drainBlockingReplicaCount;
      this.evaluation = evaluation;
    }

    /**
     * @return the resource name
     */
    public String getResourceName() {
      return resourceName;
    }

    /**
     * @return the configured rebalance mode, or {@code null}
     */
    public String getRebalanceMode() {
      return rebalanceMode;
    }

    /**
     * @return the number of replicas observed for the resource
     */
    public int getObservedReplicaCount() {
      return observedReplicaCount;
    }

    /**
     * @return the number of replicas blocking native drain completion
     */
    public int getDrainBlockingReplicaCount() {
      return drainBlockingReplicaCount;
    }

    /**
     * @return how the resource participated in the drain calculation
     */
    public ResourceEvaluation getEvaluation() {
      return evaluation;
    }
  }

  /**
   * Describes a condition that limits coverage or blocks drain completion.
   */
  public static class Blocker {
    private final BlockerCode code;
    private final String message;
    private final String sessionId;
    private final String resourceName;
    private final Integer count;

    /**
     * Creates a blocker.
     *
     * @param code stable blocker code
     * @param message human-readable description
     * @param sessionId related session, or {@code null}
     * @param resourceName related resource, or {@code null}
     * @param count related count, or {@code null}
     */
    public Blocker(BlockerCode code, String message, String sessionId, String resourceName,
        Integer count) {
      this.code = code;
      this.message = message;
      this.sessionId = sessionId;
      this.resourceName = resourceName;
      this.count = count;
    }

    /**
     * @return the stable blocker code
     */
    public BlockerCode getCode() {
      return code;
    }

    /**
     * @return the human-readable blocker description
     */
    public String getMessage() {
      return message;
    }

    /**
     * @return the related session, or {@code null}
     */
    public String getSessionId() {
      return sessionId;
    }

    /**
     * @return the related resource, or {@code null}
     */
    public String getResourceName() {
      return resourceName;
    }

    /**
     * @return the related count, or {@code null}
     */
    public Integer getCount() {
      return count;
    }
  }

  /**
   * Aggregated current-state observation for non-task resources.
   */
  public static class ReplicaStateObservation {
    private final ReplicaStateScope scope;
    private final CoverageStatus coverage;
    private final int replicaCount;
    private final boolean replicasEmpty;
    private final boolean allOffline;
    private final boolean allOfflineOrError;
    private final boolean allError;
    private final Map<String, Integer> stateCounts;
    private final List<ReplicaInfo> errorReplicas;
    private final List<String> excludedTaskResources;

    /**
     * Creates a replica-state observation.
     *
     * @param coverage observation coverage
     * @param replicaCount observed replica count
     * @param replicasEmpty whether no non-task replicas were observed
     * @param allOffline whether at least one replica was observed and every replica is OFFLINE
     * @param allOfflineOrError whether at least one replica was observed and every replica is
     *                         OFFLINE or ERROR
     * @param allError whether at least one replica was observed and every replica is ERROR
     * @param stateCounts counts by observed state
     * @param errorReplicas replicas observed in ERROR
     * @param excludedTaskResources task resources excluded from this scope
     */
    public ReplicaStateObservation(CoverageStatus coverage, int replicaCount,
        boolean replicasEmpty, boolean allOffline, boolean allOfflineOrError, boolean allError,
        Map<String, Integer> stateCounts, List<ReplicaInfo> errorReplicas,
        List<String> excludedTaskResources) {
      this.scope = ReplicaStateScope.NON_TASK_CURRENT_STATES;
      this.coverage = coverage;
      this.replicaCount = replicaCount;
      this.replicasEmpty = replicasEmpty;
      this.allOffline = allOffline;
      this.allOfflineOrError = allOfflineOrError;
      this.allError = allError;
      this.stateCounts =
          Collections.unmodifiableMap(new TreeMap<>(stateCounts));
      this.errorReplicas =
          Collections.unmodifiableList(new ArrayList<>(errorReplicas));
      this.excludedTaskResources =
          Collections.unmodifiableList(new ArrayList<>(excludedTaskResources));
    }

    /**
     * @return the replica-state scope
     */
    public ReplicaStateScope getScope() {
      return scope;
    }

    /**
     * @return the replica-state coverage
     */
    public CoverageStatus getCoverage() {
      return coverage;
    }

    /**
     * @return the observed non-task replica count
     */
    public int getReplicaCount() {
      return replicaCount;
    }

    /**
     * @return whether no non-task replicas were observed
     */
    public boolean isReplicasEmpty() {
      return replicasEmpty;
    }

    /**
     * @return whether at least one replica was observed and every replica is OFFLINE
     */
    public boolean isAllOffline() {
      return allOffline;
    }

    /**
     * @return whether at least one replica was observed and every replica is OFFLINE or ERROR
     */
    public boolean isAllOfflineOrError() {
      return allOfflineOrError;
    }

    /**
     * @return whether at least one replica was observed and every replica is ERROR
     */
    public boolean isAllError() {
      return allError;
    }

    /**
     * @return immutable counts by observed state
     */
    public Map<String, Integer> getStateCounts() {
      return stateCounts;
    }

    /**
     * @return immutable details for replicas observed in ERROR
     */
    public List<ReplicaInfo> getErrorReplicas() {
      return errorReplicas;
    }

    /**
     * @return immutable task-resource names excluded from this scope
     */
    public List<String> getExcludedTaskResources() {
      return excludedTaskResources;
    }
  }

  /**
   * Native Helix drain observation.
   */
  public static class DrainObservation {
    private final DrainScope scope;
    private final CoverageStatus coverage;
    private final boolean drained;
    private final int pendingMessageCount;
    private final List<String> supportedRebalanceModes;
    private final List<ResourceInfo> resources;
    private final List<Blocker> blockers;

    /**
     * Creates a drain observation.
     *
     * @param coverage observation coverage
     * @param drained whether the supported scope has no native drain blockers
     * @param pendingMessageCount pending messages observed on a live instance
     * @param supportedRebalanceModes modes supported by the calculation
     * @param resources per-resource coverage
     * @param blockers coverage and drain blockers
     */
    public DrainObservation(CoverageStatus coverage, boolean drained, int pendingMessageCount,
        List<String> supportedRebalanceModes, List<ResourceInfo> resources,
        List<Blocker> blockers) {
      this.scope = DrainScope.FULL_AUTO_AND_CUSTOMIZED;
      this.coverage = coverage;
      this.drained = drained;
      this.pendingMessageCount = pendingMessageCount;
      this.supportedRebalanceModes =
          Collections.unmodifiableList(new ArrayList<>(supportedRebalanceModes));
      this.resources = Collections.unmodifiableList(new ArrayList<>(resources));
      this.blockers = Collections.unmodifiableList(new ArrayList<>(blockers));
    }

    /**
     * @return the native drain scope
     */
    public DrainScope getScope() {
      return scope;
    }

    /**
     * @return the native drain coverage
     */
    public CoverageStatus getCoverage() {
      return coverage;
    }

    /**
     * @return whether the completely evaluated native scope has no drain blockers
     */
    public boolean isDrained() {
      return drained;
    }

    /**
     * @return the pending message count observed on a live instance
     */
    public int getPendingMessageCount() {
      return pendingMessageCount;
    }

    /**
     * @return immutable names of rebalance modes supported by the native calculation
     */
    public List<String> getSupportedRebalanceModes() {
      return supportedRebalanceModes;
    }

    /**
     * @return immutable per-resource coverage
     */
    public List<ResourceInfo> getResources() {
      return resources;
    }

    /**
     * @return immutable coverage and drain blockers
     */
    public List<Blocker> getBlockers() {
      return blockers;
    }
  }

  /**
   * Observation of whether the instance operation permits future replica assignment.
   */
  public static class AssignmentObservation {
    private final AssignmentScope scope;
    private final CoverageStatus coverage;
    private final boolean futureAssignmentEligible;
    private final String instanceOperation;

    /**
     * Creates an assignment observation.
     *
     * @param coverage observation coverage
     * @param futureAssignmentEligible whether the operation permits future replica assignment
     * @param instanceOperation effective instance operation, or {@code null}
     */
    public AssignmentObservation(CoverageStatus coverage, boolean futureAssignmentEligible,
        String instanceOperation) {
      this.scope = AssignmentScope.INSTANCE_OPERATION;
      this.coverage = coverage;
      this.futureAssignmentEligible = futureAssignmentEligible;
      this.instanceOperation = instanceOperation;
    }

    /**
     * @return the future-assignment scope
     */
    public AssignmentScope getScope() {
      return scope;
    }

    /**
     * @return the future-assignment coverage
     */
    public CoverageStatus getCoverage() {
      return coverage;
    }

    /**
     * @return whether the effective operation permits future replica assignment
     */
    public boolean isFutureAssignmentEligible() {
      return futureAssignmentEligible;
    }

    /**
     * @return the effective instance operation, or {@code null}
     */
    public String getInstanceOperation() {
      return instanceOperation;
    }
  }

  private final int schemaVersion;
  private final long observationTime;
  private final String clusterName;
  private final String instanceName;
  private final boolean live;
  private final String activeSessionId;
  private final List<String> currentStateSessions;
  private final ReplicaStateObservation replicaStates;
  private final DrainObservation drain;
  private final AssignmentObservation assignment;

  /**
   * Creates an instance replica status.
   *
   * @param observationTime time at which the observation completed, in milliseconds since epoch
   * @param clusterName cluster name
   * @param instanceName instance name
   * @param live whether the instance was live
   * @param activeSessionId live session identifier, or {@code null}
   * @param currentStateSessions current-state sessions observed
   * @param replicaStates non-task current-state observation
   * @param drain native drain observation
   * @param assignment future assignment observation
   */
  public InstanceReplicaStatus(long observationTime, String clusterName, String instanceName,
      boolean live, String activeSessionId, List<String> currentStateSessions,
      ReplicaStateObservation replicaStates, DrainObservation drain,
      AssignmentObservation assignment) {
    this.schemaVersion = SCHEMA_VERSION;
    this.observationTime = observationTime;
    this.clusterName = clusterName;
    this.instanceName = instanceName;
    this.live = live;
    this.activeSessionId = activeSessionId;
    this.currentStateSessions =
        Collections.unmodifiableList(new ArrayList<>(currentStateSessions));
    this.replicaStates = replicaStates;
    this.drain = drain;
    this.assignment = assignment;
  }

  /**
   * @return the JSON contract version
   */
  public int getSchemaVersion() {
    return schemaVersion;
  }

  /**
   * @return the observation completion time in milliseconds since epoch
   */
  public long getObservationTime() {
    return observationTime;
  }

  /**
   * @return the observed cluster name
   */
  public String getClusterName() {
    return clusterName;
  }

  /**
   * @return the observed instance name
   */
  public String getInstanceName() {
    return instanceName;
  }

  /**
   * @return whether the instance was live
   */
  public boolean isLive() {
    return live;
  }

  /**
   * @return the active live session, or {@code null}
   */
  public String getActiveSessionId() {
    return activeSessionId;
  }

  /**
   * @return immutable current-state sessions observed for the instance
   */
  public List<String> getCurrentStateSessions() {
    return currentStateSessions;
  }

  /**
   * @return the non-task current-state observation
   */
  public ReplicaStateObservation getReplicaStates() {
    return replicaStates;
  }

  /**
   * @return the native drain observation
   */
  public DrainObservation getDrain() {
    return drain;
  }

  /**
   * @return the future-assignment observation
   */
  public AssignmentObservation getAssignment() {
    return assignment;
  }
}
