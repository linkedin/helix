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

import com.fasterxml.jackson.annotation.JsonInclude;

/**
 * Data class representing the evacuation status of an instance.
 * Used by the isEvacuateFinished REST API to return detailed information
 * about the evacuation progress.
 */
@JsonInclude(JsonInclude.Include.NON_NULL)
public class EvacuationInfo {

  /**
   * Enum representing the current state of evacuation.
   */
  public enum EvacuationState {
    NOT_EVACUATING,  // Instance is not in EVACUATE operation
    IN_PROGRESS,     // Evacuation is ongoing
    COMPLETED        // Evacuation finished successfully
  }

  /**
   * Enum representing reasons why evacuation may be blocked or incomplete.
   */
  public enum ReasonCode {
    NOT_IN_EVACUATE_OPERATION("Instance is not in EVACUATE operation"),
    MULTIPLE_SESSIONS("Instance has multiple sessions and is carrying over from previous session");

    private final String message;

    ReasonCode(String message) {
      this.message = message;
    }

    public String getMessage() {
      return message;
    }
  }

  private EvacuationState state;
  private Integer remainingPartitionCount;
  private Integer pendingMessageCount;
  private String reason;
  private Long lastActivityTimestamp;

  /**
   * Default constructor for Jackson deserialization.
   * Fields are left as null so they won't be serialized when not applicable.
   */
  public EvacuationInfo() {
    this.state = EvacuationState.NOT_EVACUATING;
    // remainingPartitionCount and pendingMessageCount are intentionally left null
    // so they won't be serialized for NOT_EVACUATING state
  }

  /**
   * Constructor with all fields.
   */
  public EvacuationInfo(EvacuationState state, Integer remainingPartitionCount, Integer pendingMessageCount, String reason) {
    this.state = state;
    this.remainingPartitionCount = remainingPartitionCount;
    this.pendingMessageCount = pendingMessageCount;
    this.reason = reason;
  }

  public EvacuationState getState() {
    return state;
  }

  public void setState(EvacuationState state) {
    this.state = state;
  }

  public Integer getRemainingPartitionCount() {
    return remainingPartitionCount;
  }

  public void setRemainingPartitionCount(Integer remainingPartitionCount) {
    this.remainingPartitionCount = remainingPartitionCount;
  }

  public Integer getPendingMessageCount() {
    return pendingMessageCount;
  }

  public void setPendingMessageCount(Integer pendingMessageCount) {
    this.pendingMessageCount = pendingMessageCount;
  }

  public String getReason() {
    return reason;
  }

  public void setReason(String reason) {
    this.reason = reason;
  }

  /**
   * Sets the reason using a predefined ReasonCode.
   */
  public void setReason(ReasonCode reasonCode) {
    this.reason = reasonCode.getMessage();
  }

  public Long getLastActivityTimestamp() {
    return lastActivityTimestamp;
  }

  /**
   * Sets the timestamp of the last activity during evacuation.
   * This is the max modification time across all CurrentState ZNodes for the instance.
   */
  public void setLastActivityTimestamp(Long lastActivityTimestamp) {
    this.lastActivityTimestamp = lastActivityTimestamp;
  }

  @Override
  public String toString() {
    return "EvacuationInfo{" +
        "state=" + state +
        ", remainingPartitionCount=" + remainingPartitionCount +
        ", pendingMessageCount=" + pendingMessageCount +
        ", reason='" + reason + '\'' +
        ", lastActivityTimestamp=" + lastActivityTimestamp +
        '}';
  }
}
