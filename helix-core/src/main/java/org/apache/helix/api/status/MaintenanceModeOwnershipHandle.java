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

import java.util.Objects;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

/**
 * Identifies one maintenance-mode window acquired through Helix's ownership contract.
 *
 * <p>The handle is only returned when the requested window was created by, or already belongs to,
 * the exact owner and window identifiers in the request. Callers must pass the complete handle
 * back to Helix when releasing the window.
 */
public class MaintenanceModeOwnershipHandle {
  private String _clusterName;
  private String _ownerId;
  private String _windowId;
  private String _fenceId;

  /**
   * Creates an ownership handle.
   *
   * @param clusterName cluster containing the maintenance window
   * @param ownerId logical owner of the window
   * @param windowId caller-generated unique window identifier
   * @param fenceId identifier of the cluster's maintenance fence
   */
  @JsonCreator
  public MaintenanceModeOwnershipHandle(
      @JsonProperty("clusterName") String clusterName,
      @JsonProperty("ownerId") String ownerId,
      @JsonProperty("windowId") String windowId,
      @JsonProperty("fenceId") String fenceId) {
    _clusterName = clusterName;
    _ownerId = ownerId;
    _windowId = windowId;
    _fenceId = fenceId;
  }

  /**
   * @return cluster containing the maintenance window
   */
  public String getClusterName() {
    return _clusterName;
  }

  /**
   * @return logical owner of the maintenance window
   */
  public String getOwnerId() {
    return _ownerId;
  }

  /**
   * @return caller-generated unique window identifier
   */
  public String getWindowId() {
    return _windowId;
  }

  /**
   * @return identifier of the maintenance fence
   */
  public String getFenceId() {
    return _fenceId;
  }

  @Override
  public boolean equals(Object other) {
    if (this == other) {
      return true;
    }
    if (!(other instanceof MaintenanceModeOwnershipHandle)) {
      return false;
    }
    MaintenanceModeOwnershipHandle that = (MaintenanceModeOwnershipHandle) other;
    return Objects.equals(_clusterName, that._clusterName)
        && Objects.equals(_ownerId, that._ownerId)
        && Objects.equals(_windowId, that._windowId)
        && Objects.equals(_fenceId, that._fenceId);
  }

  @Override
  public int hashCode() {
    return Objects.hash(_clusterName, _ownerId, _windowId, _fenceId);
  }
}
