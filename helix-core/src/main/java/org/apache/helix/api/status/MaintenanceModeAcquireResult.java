package org.apache.helix.api.status;

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

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

/**
 * Result of acquiring or ensuring an owned maintenance-mode window.
 */
public class MaintenanceModeAcquireResult {
  public enum Status {
    /** A new owned maintenance window was created. */
    ACQUIRED,
    /** The exact owner and window were already active. */
    ALREADY_OWNED,
    /** An unowned, manual, automatic, or legacy maintenance window is active. */
    ALREADY_COVERED,
    /** A different owned maintenance window is active. */
    FOREIGN_OWNED,
    /** The maintenance signal and its fence are inconsistent or malformed. */
    CONFLICT,
    /** The request failed before Helix returned a typed result. */
    FAILED
  }

  private Status _status;
  private MaintenanceModeOwnershipHandle _handle;
  private String _message;

  /**
   * Creates an acquisition result.
   *
   * @param status typed acquisition status
   * @param handle ownership handle, present only for {@link Status#ACQUIRED} and
   *     {@link Status#ALREADY_OWNED}
   * @param message human-readable result detail
   */
  @JsonCreator
  public MaintenanceModeAcquireResult(
      @JsonProperty("status") Status status,
      @JsonProperty("handle") MaintenanceModeOwnershipHandle handle,
      @JsonProperty("message") String message) {
    _status = status;
    _handle = handle;
    _message = message;
  }

  /**
   * Creates a conservative local failure result.
   *
   * @param message failure detail
   * @return failed acquisition result without a handle
   */
  public static MaintenanceModeAcquireResult failed(String message) {
    return new MaintenanceModeAcquireResult(Status.FAILED, null, message);
  }

  /**
   * @return typed acquisition status
   */
  public Status getStatus() {
    return _status;
  }

  /**
   * @return ownership handle, or {@code null} when the caller does not own the active window
   */
  public MaintenanceModeOwnershipHandle getHandle() {
    return _handle;
  }

  /**
   * @return human-readable result detail
   */
  public String getMessage() {
    return _message;
  }
}
