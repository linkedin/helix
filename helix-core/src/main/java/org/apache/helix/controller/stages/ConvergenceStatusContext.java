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
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.apache.helix.controller.dataproviders.BaseControllerDataProvider;
import org.apache.helix.controller.rebalancer.util.DelayedRebalanceUtil;
import org.apache.helix.controller.rebalancer.util.WagedValidationUtil;
import org.apache.helix.controller.rebalancer.waged.WagedRebalanceStatus;
import org.apache.helix.controller.rebalancer.waged.WagedRebalancer;
import org.apache.helix.model.IdealState;
import org.apache.helix.model.Resource;
import org.apache.helix.model.StateModelDefinition;

/**
 * Immutable metadata needed to calculate convergence away from the controller pipeline thread.
 */
public final class ConvergenceStatusContext {
  private final boolean _maintenanceModeEnabled;
  private final Map<String, String> _initialStateByResource;
  private final Set<String> _liveInstances;
  private final Set<String> _wagedResources;
  private final Set<String> _delayedResources;
  private final WagedRebalanceStatus _wagedStatus;

  private ConvergenceStatusContext(boolean maintenanceModeEnabled,
      Map<String, String> initialStateByResource, Set<String> liveInstances,
      Set<String> wagedResources, Set<String> delayedResources,
      WagedRebalanceStatus wagedStatus) {
    _maintenanceModeEnabled = maintenanceModeEnabled;
    _initialStateByResource =
        Collections.unmodifiableMap(new HashMap<>(initialStateByResource));
    _liveInstances = Collections.unmodifiableSet(new HashSet<>(liveInstances));
    _wagedResources = Collections.unmodifiableSet(new HashSet<>(wagedResources));
    _delayedResources = Collections.unmodifiableSet(new HashSet<>(delayedResources));
    _wagedStatus = wagedStatus;
  }

  public static ConvergenceStatusContext from(ClusterEvent event,
      BaseControllerDataProvider cache, Map<String, Resource> resources) {
    Map<String, String> initialStates = new HashMap<>();
    Set<String> wagedResources = new HashSet<>();
    Set<String> delayedResources = new HashSet<>();
    for (Map.Entry<String, Resource> entry : resources.entrySet()) {
      String resourceName = entry.getKey();
      StateModelDefinition stateModelDefinition =
          cache.getStateModelDef(entry.getValue().getStateModelDefRef());
      if (stateModelDefinition != null) {
        initialStates.put(resourceName, stateModelDefinition.getInitialState());
      }
      IdealState idealState = cache.getIdealState(resourceName);
      if (idealState != null && WagedValidationUtil.isWagedEnabled(idealState)) {
        wagedResources.add(resourceName);
      }
      if (idealState != null && cache.getClusterConfig() != null && DelayedRebalanceUtil
          .isDelayRebalanceEnabled(idealState, cache.getClusterConfig())) {
        delayedResources.add(resourceName);
      }
    }

    Object rebalancer = event.getAttribute(AttributeName.STATEFUL_REBALANCER.name());
    WagedRebalanceStatus wagedStatus = rebalancer instanceof WagedRebalancer
        ? ((WagedRebalancer) rebalancer).getConvergenceStatus() : null;
    return new ConvergenceStatusContext(cache.isMaintenanceModeEnabled(), initialStates,
        cache.getLiveInstances().keySet(), wagedResources, delayedResources, wagedStatus);
  }

  public boolean isMaintenanceModeEnabled() {
    return _maintenanceModeEnabled;
  }

  public String getInitialState(String resourceName) {
    return _initialStateByResource.get(resourceName);
  }

  public boolean hasStateModel(String resourceName) {
    return _initialStateByResource.containsKey(resourceName);
  }

  public Set<String> getLiveInstances() {
    return _liveInstances;
  }

  public boolean isWagedResource(String resourceName) {
    return _wagedResources.contains(resourceName);
  }

  public boolean isDelayedResource(String resourceName) {
    return _delayedResources.contains(resourceName);
  }

  public WagedRebalanceStatus getWagedStatus() {
    return _wagedStatus;
  }
}
