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

  private boolean successful;
  private int remainingCount;
  private int pendingMessageCount;
  private String reason;

  /**
   * Default constructor for Jackson deserialization.
   */
  public EvacuationInfo() {
    this.successful = false;
    this.remainingCount = 0;
    this.pendingMessageCount = 0;
  }

  /**
   * Constructor with all fields.
   */
  public EvacuationInfo(boolean successful, int remainingCount, int pendingMessageCount, String reason) {
    this.successful = successful;
    this.remainingCount = remainingCount;
    this.pendingMessageCount = pendingMessageCount;
    this.reason = reason;
  }

  public boolean isSuccessful() {
    return successful;
  }

  public void setSuccessful(boolean successful) {
    this.successful = successful;
  }

  public int getRemainingCount() {
    return remainingCount;
  }

  public void setRemainingCount(int remainingCount) {
    this.remainingCount = remainingCount;
  }

  public int getPendingMessageCount() {
    return pendingMessageCount;
  }

  public void setPendingMessageCount(int pendingMessageCount) {
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

  @Override
  public String toString() {
    return "EvacuationInfo{" +
        "successful=" + successful +
        ", remainingCount=" + remainingCount +
        ", pendingMessageCount=" + pendingMessageCount +
        ", reason='" + reason + '\'' +
        '}';
  }
}

