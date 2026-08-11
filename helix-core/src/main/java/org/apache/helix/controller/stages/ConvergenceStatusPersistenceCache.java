package org.apache.helix.controller.stages;

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

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;

import org.apache.helix.model.ConvergenceStatus;
import org.apache.helix.zookeeper.datamodel.ZNRecord;

/**
 * Controller-scoped cache shared by default and management pipelines.
 */
public final class ConvergenceStatusPersistenceCache {
  private String _sessionId;
  private ConvergenceStatus _clusterStatus;
  private Map<String, ConvergenceStatus> _resourceStatuses = Collections.emptyMap();
  private long _lastPersistTimestamp;

  public boolean isInitializedFor(String sessionId) {
    return sessionId != null && sessionId.equals(_sessionId);
  }

  public ConvergenceStatus getClusterStatus() {
    return copy(_clusterStatus);
  }

  public Map<String, ConvergenceStatus> getResourceStatuses() {
    return copy(_resourceStatuses);
  }

  public void update(String sessionId, ConvergenceStatus clusterStatus,
      Map<String, ConvergenceStatus> resourceStatuses) {
    if (sessionId != null && !sessionId.equals(_sessionId)) {
      _lastPersistTimestamp = 0L;
    }
    _sessionId = sessionId;
    _clusterStatus = copy(clusterStatus);
    _resourceStatuses = copy(resourceStatuses);
  }

  public long getRemainingPersistDelay(long now, long minimumIntervalMs) {
    return Math.max(0L, _lastPersistTimestamp + minimumIntervalMs - now);
  }

  public void markPersisted(long timestamp) {
    _lastPersistTimestamp = timestamp;
  }

  private static ConvergenceStatus copy(ConvergenceStatus status) {
    return status == null ? null : new ConvergenceStatus(new ZNRecord(status.getRecord()));
  }

  private static Map<String, ConvergenceStatus> copy(
      Map<String, ConvergenceStatus> statuses) {
    if (statuses == null || statuses.isEmpty()) {
      return Collections.emptyMap();
    }
    Map<String, ConvergenceStatus> result = new LinkedHashMap<>();
    statuses.forEach((name, status) -> result.put(name, copy(status)));
    return Collections.unmodifiableMap(result);
  }
}
