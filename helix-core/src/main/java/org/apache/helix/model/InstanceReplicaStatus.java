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

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonInclude;

/**
 * Data class representing whether an instance's replicas are offline.
 * Used by the areReplicasOffline REST API to return the answer together with how many replicas
 * the instance holds, how many of them are offline, and which are in ERROR.
 *
 * The replica fields describe the current states of the instance's live session, with task
 * resources excluded. They are empty when the instance is not live.
 */
@JsonInclude(JsonInclude.Include.NON_NULL)
@JsonIgnoreProperties(ignoreUnknown = true)
public class InstanceReplicaStatus {
  private boolean offline;
  private Integer replicaCount;
  private Integer offlineReplicaCount;
  private List<String> errorPartitions;

  /**
   * Default constructor for Jackson deserialization.
   */
  public InstanceReplicaStatus() {
  }

  /**
   * Returns whether every replica is in the initial state of its state model, for example OFFLINE
   * for MasterSlave. When ERROR partitions are excluded, a replica in ERROR also counts. True when
   * the instance holds no replicas.
   */
  public boolean isOffline() {
    return offline;
  }

  public void setOffline(boolean offline) {
    this.offline = offline;
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
   * Returns the number of replicas in the initial state of their resource's state model.
   */
  public Integer getOfflineReplicaCount() {
    return offlineReplicaCount;
  }

  public void setOfflineReplicaCount(Integer offlineReplicaCount) {
    this.offlineReplicaCount = offlineReplicaCount;
  }

  /**
   * Returns the sorted names of the partitions whose replica is in ERROR, one entry per replica.
   */
  public List<String> getErrorPartitions() {
    return errorPartitions;
  }

  public void setErrorPartitions(List<String> errorPartitions) {
    this.errorPartitions = errorPartitions;
  }

  /**
   * Returns whether every replica is in ERROR. True when there are no replicas, false when the
   * replica fields are missing.
   */
  public boolean areAllReplicasInError() {
    return replicaCount != null && errorPartitions != null
        && errorPartitions.size() == replicaCount;
  }

  @Override
  public String toString() {
    return "InstanceReplicaStatus{" +
        "offline=" + offline +
        ", replicaCount=" + replicaCount +
        ", offlineReplicaCount=" + offlineReplicaCount +
        ", errorPartitions=" + errorPartitions +
        '}';
  }
}
