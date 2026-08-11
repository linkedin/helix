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
 * Immutable convergence records calculated from one controller event.
 */
public class ConvergenceStatusSnapshot {
  private final ConvergenceStatus _clusterStatus;
  private final Map<String, ConvergenceStatus> _resourceStatuses;

  public ConvergenceStatusSnapshot(ConvergenceStatus clusterStatus,
      Map<String, ConvergenceStatus> resourceStatuses) {
    _clusterStatus = new ConvergenceStatus(new ZNRecord(clusterStatus.getRecord()));
    Map<String, ConvergenceStatus> copy = new LinkedHashMap<>();
    resourceStatuses.forEach(
        (name, status) -> copy.put(name,
            new ConvergenceStatus(new ZNRecord(status.getRecord()))));
    _resourceStatuses = Collections.unmodifiableMap(copy);
  }

  public ConvergenceStatus getClusterStatus() {
    return new ConvergenceStatus(new ZNRecord(_clusterStatus.getRecord()));
  }

  public Map<String, ConvergenceStatus> getResourceStatuses() {
    Map<String, ConvergenceStatus> copy = new LinkedHashMap<>();
    _resourceStatuses.forEach(
        (name, status) -> copy.put(name,
            new ConvergenceStatus(new ZNRecord(status.getRecord()))));
    return Collections.unmodifiableMap(copy);
  }
}
