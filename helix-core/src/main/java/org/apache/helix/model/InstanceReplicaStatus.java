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

import java.util.List;
import java.util.Map;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonInclude;

/**
 * Data class representing whether an instance's replicas are offline.
 * Used by the areReplicasOffline REST API to return the answer, the state behind it, and a summary
 * of the replicas in the current states of the instance's live session, with task resources
 * excluded.
 *
 * The replica fields are left unset while the instance is carrying over a previous session.
 */
@JsonInclude(JsonInclude.Include.NON_NULL)
@JsonIgnoreProperties(ignoreUnknown = true)
public class InstanceReplicaStatus {
  /**
   * The most partitions that {@link #getErrorPartitions()} lists.
   */
  public static final int MAX_ERROR_PARTITIONS = 100;

  public enum State {
    NOT_LIVE,          // The instance is not live, so none of its replicas are serving.
    MULTIPLE_SESSIONS, // The instance is carrying over current states from a previous session.
    IN_PROGRESS,       // Messages are pending, or a replica is neither offline nor in ERROR.
    BLOCKED_BY_ERROR,  // Only replicas in ERROR are left, and ERROR is not excluded.
    REPLICAS_OFFLINE   // Every replica is offline.
  }

  private State state;
  private Integer replicaCount;
  private Integer offlineReplicaCount;
  private Integer errorReplicaCount;
  private Integer pendingMessageCount;
  private Long lastActivityTimestamp;
  private Map<String, List<String>> errorPartitions;
  private Boolean errorPartitionsTruncated;

  /**
   * Default constructor for Jackson deserialization.
   */
  public InstanceReplicaStatus() {
  }

  public State getState() {
    return state;
  }

  public void setState(State state) {
    this.state = state;
  }

  /**
   * Returns whether every replica is offline: true when the state is NOT_LIVE or
   * REPLICAS_OFFLINE. It is derived from the state, so it is false when the state is missing.
   */
  public boolean isAllReplicasOffline() {
    return state == State.NOT_LIVE || state == State.REPLICAS_OFFLINE;
  }

  /**
   * Returns the number of replicas, including any replica without a current state.
   */
  public Integer getReplicaCount() {
    return replicaCount;
  }

  public void setReplicaCount(Integer replicaCount) {
    this.replicaCount = replicaCount;
  }

  /**
   * Returns the number of replicas in the initial state of their resource's state model, for
   * example OFFLINE for MasterSlave.
   */
  public Integer getOfflineReplicaCount() {
    return offlineReplicaCount;
  }

  public void setOfflineReplicaCount(Integer offlineReplicaCount) {
    this.offlineReplicaCount = offlineReplicaCount;
  }

  /**
   * Returns the number of replicas in ERROR.
   */
  public Integer getErrorReplicaCount() {
    return errorReplicaCount;
  }

  public void setErrorReplicaCount(Integer errorReplicaCount) {
    this.errorReplicaCount = errorReplicaCount;
  }

  /**
   * Returns the number of messages pending for the instance.
   */
  public Integer getPendingMessageCount() {
    return pendingMessageCount;
  }

  public void setPendingMessageCount(Integer pendingMessageCount) {
    this.pendingMessageCount = pendingMessageCount;
  }

  /**
   * Returns the latest modification time of the current states that the counts come from,
   * including the buckets of a bucketized resource, which shows whether the replicas are still
   * changing.
   */
  public Long getLastActivityTimestamp() {
    return lastActivityTimestamp;
  }

  public void setLastActivityTimestamp(Long lastActivityTimestamp) {
    this.lastActivityTimestamp = lastActivityTimestamp;
  }

  /**
   * Returns the partitions whose replica is in ERROR, by resource, both in name order. At most
   * {@link #MAX_ERROR_PARTITIONS} partitions are listed.
   */
  public Map<String, List<String>> getErrorPartitions() {
    return errorPartitions;
  }

  public void setErrorPartitions(Map<String, List<String>> errorPartitions) {
    this.errorPartitions = errorPartitions;
  }

  /**
   * Returns whether {@link #getErrorPartitions()} leaves out some replicas in ERROR.
   */
  public Boolean getErrorPartitionsTruncated() {
    return errorPartitionsTruncated;
  }

  public void setErrorPartitionsTruncated(Boolean errorPartitionsTruncated) {
    this.errorPartitionsTruncated = errorPartitionsTruncated;
  }

  /**
   * Returns whether every replica is in ERROR. True when there are no replicas, false when the
   * counts are missing.
   */
  public boolean areAllReplicasInError() {
    return replicaCount != null && replicaCount.equals(errorReplicaCount);
  }

  @Override
  public String toString() {
    return "InstanceReplicaStatus{" +
        "state=" + state +
        ", replicaCount=" + replicaCount +
        ", offlineReplicaCount=" + offlineReplicaCount +
        ", errorReplicaCount=" + errorReplicaCount +
        ", pendingMessageCount=" + pendingMessageCount +
        ", lastActivityTimestamp=" + lastActivityTimestamp +
        ", errorPartitions=" + errorPartitions +
        ", errorPartitionsTruncated=" + errorPartitionsTruncated +
        '}';
  }
}
