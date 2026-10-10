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

import com.fasterxml.jackson.annotation.JsonAnySetter;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.TreeSet;
import org.apache.helix.HelixDefinedState;

/**
 * Read-only observation of the replicas an instance currently holds and of whether Helix
 * considers that instance drained.
 *
 * <p>Coverage is reported separately for the two questions, because they read different metadata,
 * and it must be checked before a result is used. An observation that could not read everything it
 * needed reports {@link CoverageStatus#INCOMPLETE}, and every readiness predicate then returns
 * {@code false} rather than guessing.
 *
 * <p>The readiness predicates are derived from the observed counts rather than stored alongside
 * them, so an instance of this class cannot report a readiness that contradicts its own counts.
 * The constructor rejects counts that cannot describe a real observation.
 */
@JsonIgnoreProperties(ignoreUnknown = false)
public class InstanceReplicaStatus {
  /**
   * Whether all metadata required for an observation was evaluated.
   */
  public enum CoverageStatus {
    /** Everything needed was read and understood. */
    COMPLETE,
    /** Metadata was missing, inconsistent, or changed while being read. */
    INCOMPLETE,
    /** A resource uses a rebalance mode this calculation does not support. */
    UNSUPPORTED
  }

  private final long observationTime;
  private final String clusterName;
  private final String instanceName;
  private final boolean live;
  private final CoverageStatus replicaCoverage;
  private final int replicaCount;
  private final int offlineReplicaCount;
  private final List<String> offlineStates;
  private final Map<String, Integer> stateCounts;
  private final List<String> errorPartitionNames;
  private final List<String> excludedTaskResources;
  private final CoverageStatus drainCoverage;
  private final boolean drained;
  private final int pendingMessageCount;
  private final List<String> blockers;

  /**
   * Creates an observation.
   *
   * <p>Every collection parameter is bound through this creator, so the JSON form round-trips.
   * Derived readiness is deliberately absent from the parameter list and is never serialized.
   *
   * @param observationTime epoch milliseconds at which the observation completed
   * @param clusterName observed cluster
   * @param instanceName observed instance
   * @param live whether a live instance node was present
   * @param replicaCoverage coverage of the replica-state observation
   * @param replicaCount replicas observed on the instance, excluding task resources
   * @param offlineReplicaCount replicas observed in the initial state of their own resource's state
   *     model, attributed per replica rather than by pooling states across resources
   * @param offlineStates the distinct initial states observed, which bound
   *     {@code offlineReplicaCount}
   * @param stateCounts replica count per observed state
   * @param errorPartitionNames sorted names of partitions observed in {@code ERROR}
   * @param excludedTaskResources task resources excluded from the replica scope
   * @param drainCoverage coverage of the drain observation
   * @param drained whether Helix considers the instance drained
   * @param pendingMessageCount messages still queued for the instance
   * @param blockers human-readable reasons the observation is incomplete or the instance is not
   *     drained, in the order they were detected
   * @throws IllegalArgumentException if the arguments cannot describe a real observation
   */
  @JsonCreator
  public InstanceReplicaStatus(@JsonProperty("observationTime") long observationTime,
      @JsonProperty("clusterName") String clusterName,
      @JsonProperty("instanceName") String instanceName,
      @JsonProperty("live") boolean live,
      @JsonProperty("replicaCoverage") CoverageStatus replicaCoverage,
      @JsonProperty("replicaCount") int replicaCount,
      @JsonProperty("offlineReplicaCount") int offlineReplicaCount,
      @JsonProperty("offlineStates") List<String> offlineStates,
      @JsonProperty("stateCounts") Map<String, Integer> stateCounts,
      @JsonProperty("errorPartitionNames") List<String> errorPartitionNames,
      @JsonProperty("excludedTaskResources") List<String> excludedTaskResources,
      @JsonProperty("drainCoverage") CoverageStatus drainCoverage,
      @JsonProperty("drained") boolean drained,
      @JsonProperty("pendingMessageCount") int pendingMessageCount,
      @JsonProperty("blockers") List<String> blockers) {
    if (clusterName == null || instanceName == null || replicaCoverage == null
        || drainCoverage == null || offlineStates == null || stateCounts == null
        || errorPartitionNames == null || excludedTaskResources == null || blockers == null) {
      throw new IllegalArgumentException("Instance replica status is missing a required field");
    }
    if (observationTime < 0 || replicaCount < 0 || pendingMessageCount < 0) {
      throw new IllegalArgumentException("Instance replica status carries a negative count");
    }
    if (offlineStates.contains(HelixDefinedState.ERROR.name())) {
      throw new IllegalArgumentException(
          "Instance replica status counts ERROR as an offline state");
    }
    int statedReplicas = 0;
    for (Map.Entry<String, Integer> entry : stateCounts.entrySet()) {
      if (entry.getKey() == null || entry.getValue() == null || entry.getValue() < 0) {
        throw new IllegalArgumentException("Instance replica status carries an invalid state count");
      }
      statedReplicas += entry.getValue();
    }
    if (replicaCoverage == CoverageStatus.COMPLETE && statedReplicas != replicaCount) {
      throw new IllegalArgumentException(
          "Instance replica status claims complete coverage but its state counts do not account "
              + "for every replica");
    }
    // The offline count is attributed per replica by the producer, because "offline" is the initial
    // state of each replica's own state model. Bounding it by the states actually observed stops a
    // producer claiming more offline replicas than it saw in those states.
    int offlineCapacity = 0;
    for (String state : offlineStates) {
      offlineCapacity += stateCounts.getOrDefault(state, 0);
    }
    if (offlineReplicaCount < 0 || offlineReplicaCount > offlineCapacity) {
      throw new IllegalArgumentException(
          "Instance replica status reports more offline replicas than it observed in offline "
              + "states");
    }
    if (offlineReplicaCount + stateCounts.getOrDefault(HelixDefinedState.ERROR.name(), 0)
        > replicaCount) {
      throw new IllegalArgumentException(
          "Instance replica status reports more offline and error replicas than replicas");
    }
    if (errorPartitionNames.size() > stateCounts
        .getOrDefault(HelixDefinedState.ERROR.name(), 0)) {
      throw new IllegalArgumentException(
          "Instance replica status reports more error partitions than error replicas");
    }
    if (drained && (drainCoverage != CoverageStatus.COMPLETE || pendingMessageCount != 0)) {
      throw new IllegalArgumentException(
          "Instance replica status reports a drained instance without complete drain coverage and "
              + "no pending messages");
    }
    this.observationTime = observationTime;
    this.clusterName = clusterName;
    this.instanceName = instanceName;
    this.live = live;
    this.replicaCoverage = replicaCoverage;
    this.replicaCount = replicaCount;
    this.offlineReplicaCount = offlineReplicaCount;
    this.offlineStates =
        Collections.unmodifiableList(new ArrayList<>(new TreeSet<>(offlineStates)));
    this.stateCounts = Collections.unmodifiableMap(new TreeMap<>(stateCounts));
    this.errorPartitionNames =
        Collections.unmodifiableList(new ArrayList<>(new TreeSet<>(errorPartitionNames)));
    this.excludedTaskResources =
        Collections.unmodifiableList(new ArrayList<>(excludedTaskResources));
    this.drainCoverage = drainCoverage;
    this.drained = drained;
    this.pendingMessageCount = pendingMessageCount;
    this.blockers = Collections.unmodifiableList(new ArrayList<>(blockers));
  }

  /**
   * @return epoch milliseconds at which the observation completed
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
   * @return whether a live instance node was present
   */
  public boolean isLive() {
    return live;
  }

  /**
   * @return coverage of the replica-state observation
   */
  public CoverageStatus getReplicaCoverage() {
    return replicaCoverage;
  }

  /**
   * @return replicas observed on the instance, excluding task resources
   */
  public int getReplicaCount() {
    return replicaCount;
  }

  /**
   * Returns the distinct initial states observed across the instance's state models. A replica is
   * offline when it sits in the initial state of its own resource's state model, which is
   * state-model specific rather than always {@code OFFLINE}. These names bound
   * {@link #getOfflineReplicaCount()}; they are not themselves a per-resource attribution.
   *
   * @return immutable sorted offline state names
   */
  public List<String> getOfflineStates() {
    return offlineStates;
  }

  /**
   * @return replicas observed in the initial state of their own resource's state model
   */
  public int getOfflineReplicaCount() {
    return offlineReplicaCount;
  }

  /**
   * @return replicas observed in {@code ERROR}, derived from the observed state counts
   */
  @JsonIgnore
  public int getErrorReplicaCount() {
    return stateCounts.getOrDefault(HelixDefinedState.ERROR.name(), 0);
  }

  /**
   * @return immutable replica count per observed state
   */
  public Map<String, Integer> getStateCounts() {
    return stateCounts;
  }

  /**
   * Returns the partitions observed in {@code ERROR}. These names are advisory: they describe what
   * was seen, and they are only meaningful when {@link #getReplicaCoverage()} is
   * {@link CoverageStatus#COMPLETE}.
   *
   * @return immutable sorted partition names
   */
  public List<String> getErrorPartitionNames() {
    return errorPartitionNames;
  }

  /**
   * @return immutable task resources excluded from the replica scope
   */
  public List<String> getExcludedTaskResources() {
    return excludedTaskResources;
  }

  /**
   * @return coverage of the drain observation
   */
  public CoverageStatus getDrainCoverage() {
    return drainCoverage;
  }

  /**
   * @return whether Helix considers the instance drained
   */
  public boolean isDrained() {
    return drained;
  }

  /**
   * @return messages still queued for the instance
   */
  public int getPendingMessageCount() {
    return pendingMessageCount;
  }

  /**
   * Returns why the observation is incomplete or the instance is not drained. A caller reporting a
   * failure to an operator should surface these rather than inventing its own explanation.
   *
   * @return immutable reasons, in the order they were detected
   */
  public List<String> getBlockers() {
    return blockers;
  }

  /**
   * @return true when no non-task replica was observed on the instance
   */
  @JsonIgnore
  public boolean isReplicaScopeEmpty() {
    return replicaCount == 0;
  }

  /**
   * @return true when coverage is complete and every observed replica is in its resource's initial
   *     state
   */
  @JsonIgnore
  public boolean isAllOffline() {
    return replicaCoverage == CoverageStatus.COMPLETE && replicaCount > 0
        && offlineReplicaCount == replicaCount;
  }

  /**
   * @return true when coverage is complete and every observed replica is in its resource's initial
   *     state or in {@code ERROR}
   */
  @JsonIgnore
  public boolean isAllOfflineOrError() {
    return replicaCoverage == CoverageStatus.COMPLETE && replicaCount > 0
        && offlineReplicaCount + getErrorReplicaCount() == replicaCount;
  }

  /**
   * @return true when coverage is complete and every observed replica is in {@code ERROR}
   */
  @JsonIgnore
  public boolean isAllError() {
    return replicaCoverage == CoverageStatus.COMPLETE && replicaCount > 0
        && getErrorReplicaCount() == replicaCount;
  }

  /**
   * Reports whether the instance no longer serves replicas under the caller's {@code ERROR}
   * policy.
   *
   * <p>An instance that is not live is treated as serving nothing. Incomplete coverage always
   * returns {@code false}, so a caller never drops a host on the strength of an observation Helix
   * could not complete.
   *
   * @param treatErrorAsDrained whether {@code ERROR} replicas count as drained
   * @return true when the instance is safe to treat as drained under that policy
   */
  public boolean areReplicasOffline(boolean treatErrorAsDrained) {
    if (replicaCoverage != CoverageStatus.COMPLETE) {
      return false;
    }
    if (!live) {
      return true;
    }
    return isReplicaScopeEmpty()
        || (treatErrorAsDrained ? isAllOfflineOrError() : isAllOffline());
  }

  /**
   * @return true when Helix reports a complete drain, or when every observed replica is in
   *     {@code ERROR}
   */
  public boolean areReplicasDroppedOrError() {
    return (drainCoverage == CoverageStatus.COMPLETE && drained)
        || (replicaCoverage == CoverageStatus.COMPLETE && isAllError());
  }

  @Override
  public String toString() {
    return "InstanceReplicaStatus{cluster=" + clusterName + ", instance=" + instanceName
        + ", live=" + live + ", replicaCoverage=" + replicaCoverage
        + ", replicaCount=" + replicaCount + ", offlineReplicaCount=" + offlineReplicaCount
        + ", offlineStates=" + offlineStates
        + ", stateCounts=" + stateCounts + ", drainCoverage=" + drainCoverage
        + ", drained=" + drained + ", pendingMessageCount=" + pendingMessageCount
        + ", blockers=" + blockers + "}";
  }

  @JsonAnySetter
  void rejectUnknownJsonProperty(String name, Object value) {
    throw new IllegalArgumentException("Unknown property " + name);
  }
}
