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
 * Result of releasing an owned maintenance-mode window.
 */
public class MaintenanceModeReleaseResult {
  public enum Status {
    /** The exact owned window was removed. */
    APPLIED,
    /** The exact owned window had already been removed. */
    UNCHANGED,
    /** A foreign, replaced, malformed, or otherwise unverifiable window was observed. */
    CONFLICT,
    /** The request failed before Helix returned a typed result. */
    FAILED
  }

  private Status _status;
  private String _message;

  /**
   * Creates a release result.
   *
   * @param status typed release status
   * @param message human-readable result detail
   */
  @JsonCreator
  public MaintenanceModeReleaseResult(
      @JsonProperty("status") Status status,
      @JsonProperty("message") String message) {
    _status = status;
    _message = message;
  }

  /**
   * Creates a conservative local failure result.
   *
   * @param message failure detail
   * @return failed release result
   */
  public static MaintenanceModeReleaseResult failed(String message) {
    return new MaintenanceModeReleaseResult(Status.FAILED, message);
  }

  /**
   * @return typed release status
   */
  public Status getStatus() {
    return _status;
  }

  /**
   * @return human-readable result detail
   */
  public String getMessage() {
    return _message;
  }
}
