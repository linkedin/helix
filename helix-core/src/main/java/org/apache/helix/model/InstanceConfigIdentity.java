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

import java.util.Objects;

/**
 * The observed identity of a single instance config node at a point in time: which node it is
 * (creation id) and which revision of it was read (config version).
 * <p>
 * The two parts answer different questions and neither one is sufficient alone:
 * <ul>
 *   <li>{@code configVersion} is the metadata store data version. It changes on every write, so
 *       comparing it detects a concurrent modification of the same node.</li>
 *   <li>{@code configCreationId} identifies the node instance itself, not its content. It changes
 *       when the node is deleted and created again, so comparing it detects the case where the
 *       instance was removed and re-added between two calls. That case is invisible to
 *       {@code configVersion} alone, because a recreated node restarts at version 0.</li>
 * </ul>
 * <p>
 * Callers can pass an identity to a later mutating call to identify the config they observed.
 * The mutating API defines when the identity is checked and which conditions the store can
 * enforce. An identity is a pure observation, not ownership or an atomic incarnation fence, and
 * holding one does not reserve the instance against other writers.
 * <p>
 * Instances of this class are immutable.
 */
public class InstanceConfigIdentity {

  /**
   * Sentinel meaning the config version is not known and therefore cannot be asserted.
   */
  public static final int UNKNOWN_VERSION = -1;

  /**
   * Sentinel meaning the creation id is not known and therefore cannot be asserted.
   */
  public static final long UNKNOWN_CREATION_ID = -1L;

  private final String _instanceName;
  private final int _configVersion;
  private final long _configCreationId;

  /**
   * @param instanceName     the instance whose config this identity describes
   * @param configVersion    the data version of the config node, or {@link #UNKNOWN_VERSION}
   * @param configCreationId the creation id of the config node, or {@link #UNKNOWN_CREATION_ID}
   */
  public InstanceConfigIdentity(String instanceName, int configVersion, long configCreationId) {
    _instanceName = instanceName;
    _configVersion = configVersion;
    _configCreationId = configCreationId;
  }

  public String getInstanceName() {
    return _instanceName;
  }

  public int getConfigVersion() {
    return _configVersion;
  }

  public long getConfigCreationId() {
    return _configCreationId;
  }

  /**
   * @return true when both the config version and the creation id are known, so this identity can
   *         be asserted against an observed config.
   */
  public boolean isFullySpecified() {
    return _instanceName != null && !_instanceName.isEmpty() && _configVersion >= 0
        && _configCreationId >= 0;
  }

  @Override
  public boolean equals(Object other) {
    if (this == other) {
      return true;
    }
    if (!(other instanceof InstanceConfigIdentity)) {
      return false;
    }
    InstanceConfigIdentity that = (InstanceConfigIdentity) other;
    return _configVersion == that._configVersion && _configCreationId == that._configCreationId
        && Objects.equals(_instanceName, that._instanceName);
  }

  @Override
  public int hashCode() {
    return Objects.hash(_instanceName, _configVersion, _configCreationId);
  }

  @Override
  public String toString() {
    return "InstanceConfigIdentity{instanceName=" + _instanceName + ", configVersion="
        + _configVersion + ", configCreationId=" + _configCreationId + "}";
  }
}
