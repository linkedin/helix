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

import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import org.apache.helix.HelixDataAccessor;
import org.apache.helix.HelixManager;
import org.apache.helix.PropertyKey;
import org.apache.helix.controller.dataproviders.BaseControllerDataProvider;
import org.apache.helix.controller.pipeline.AbstractAsyncBaseStage;
import org.apache.helix.controller.pipeline.AsyncWorkerType;
import org.apache.helix.controller.pipeline.Pipeline;
import org.apache.helix.model.ConvergenceStatus;
import org.apache.helix.model.ConvergenceStatus.Reason;
import org.apache.helix.model.ConvergenceStatus.Status;
import org.apache.helix.model.Resource;
import org.apache.helix.zookeeper.datamodel.ZNRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Persists convergence snapshots without blocking the reconciliation pipeline.
 */
public class ConvergenceStatusPersistStage extends AbstractAsyncBaseStage {
  private static final Logger LOG = LoggerFactory.getLogger(ConvergenceStatusPersistStage.class);
  private static final long MINIMUM_PERSIST_INTERVAL_MS = 1_000L;
  private final ConvergenceStatusCalculator _calculator = new ConvergenceStatusCalculator();
  private final ConvergenceStatusPersistenceCache _localPersistenceCache =
      new ConvergenceStatusPersistenceCache();

  @Override
  public void process(ClusterEvent event) throws Exception {
    BaseControllerDataProvider cache =
        event.getAttribute(AttributeName.ControllerDataProvider.name());
    @SuppressWarnings("unchecked")
    Map<String, Resource> resources =
        event.getAttribute(AttributeName.RESOURCES_TO_REBALANCE.name());
    Boolean monitoringEnabled =
        event.getAttribute(AttributeName.CONVERGENCE_MONITORING_ENABLED.name());
    if (monitoringEnabled == null) {
      monitoringEnabled = cache != null && cache.getClusterConfig() != null
          && cache.getClusterConfig().isConvergenceMonitoringEnabled();
    }
    if (cache == null || resources == null || !monitoringEnabled) {
      return;
    }
    event.addAttribute(AttributeName.CONVERGENCE_STATUS_CONTEXT.name(),
        ConvergenceStatusContext.from(event, cache, resources));
    super.process(event);
  }

  @Override
  public AsyncWorkerType getAsyncWorkerType() {
    return AsyncWorkerType.ConvergenceStatusPersistWorker;
  }

  @Override
  public void execute(ClusterEvent event) {
    ConvergenceStatusSnapshot snapshot =
        event.getAttribute(AttributeName.CONVERGENCE_STATUS.name());
    HelixManager manager = event.getAttribute(AttributeName.helixmanager.name());
    if (manager == null || !isCurrentLeader(event, manager)) {
      return;
    }
    HelixDataAccessor accessor = manager.getHelixDataAccessor();
    PropertyKey.Builder keyBuilder = accessor.keyBuilder();
    ConvergenceStatusPersistenceCache persistenceCache =
        event.getAttribute(AttributeName.CONVERGENCE_STATUS_PERSISTENCE_CACHE.name());
    if (persistenceCache == null) {
      persistenceCache = _localPersistenceCache;
    }
    initializePersistedState(accessor, keyBuilder, manager.getSessionId(), persistenceCache);

    if (snapshot == null) {
      long delay = persistenceCache.getRemainingPersistDelay(System.currentTimeMillis(),
          MINIMUM_PERSIST_INTERVAL_MS);
      if (delay > 0) {
        try {
          Thread.sleep(delay);
        } catch (InterruptedException e) {
          Thread.currentThread().interrupt();
          return;
        }
      }
      if (!isCurrentLeader(event, manager)) {
        return;
      }
      snapshot = calculateSnapshot(event);
      if (snapshot == null) {
        return;
      }
    }

    ConvergenceStatus oldClusterStatus = persistenceCache.getClusterStatus();
    Map<String, ConvergenceStatus> oldResourceStatuses =
        persistenceCache.getResourceStatuses();

    long now = System.currentTimeMillis();
    Map<String, ConvergenceStatus> newResourceStatuses =
        new LinkedHashMap<>(snapshot.getResourceStatuses());
    List<PropertyKey> changedKeys = new ArrayList<>();
    List<ConvergenceStatus> changedStatuses = new ArrayList<>();
    long oldestUnconvergedSince = 0L;

    for (Map.Entry<String, ConvergenceStatus> entry : newResourceStatuses.entrySet()) {
      String resourceName = entry.getKey();
      ConvergenceStatus status =
          new ConvergenceStatus(new ZNRecord(entry.getValue().getRecord()));
      ConvergenceStatus oldStatus = oldResourceStatuses.get(resourceName);
      updateUnconvergedSince(status, oldStatus, now);
      if (status.getUnconvergedSince() > 0
          && (oldestUnconvergedSince == 0
              || status.getUnconvergedSince() < oldestUnconvergedSince)) {
        oldestUnconvergedSince = status.getUnconvergedSince();
      }
      newResourceStatuses.put(resourceName, status);
      if (oldStatus == null || !status.semanticallyEquals(oldStatus)) {
        changedKeys.add(keyBuilder.convergenceStatus(resourceName));
        changedStatuses.add(status);
      }
    }

    ConvergenceStatus newClusterStatus = snapshot.getClusterStatus();
    newClusterStatus.setUnconvergedSince(oldestUnconvergedSince);
    List<String> removedResources = new ArrayList<>();
    for (String resourceName : oldResourceStatuses.keySet()) {
      if (!newResourceStatuses.containsKey(resourceName)) {
        removedResources.add(resourceName);
      }
    }
    boolean hasChildMutations = !changedKeys.isEmpty() || !removedResources.isEmpty();

    if (!isCurrentLeader(event, manager)) {
      return;
    }

    if (hasChildMutations) {
      ConvergenceStatus incompleteStatus =
          new ConvergenceStatus(new ZNRecord(newClusterStatus.getRecord()));
      incompleteStatus.setComplete(false);
      if (!accessor.setProperty(keyBuilder.convergenceStatus(), incompleteStatus)) {
        LOG.warn("Failed to mark convergence status update incomplete for cluster {}",
          manager.getClusterName());
        return;
      }
    }

    boolean childUpdatesSucceeded = true;
    if (!changedKeys.isEmpty()) {
      boolean[] results = accessor.setChildren(changedKeys, changedStatuses);
      for (int i = 0; i < results.length; i++) {
        if (!results[i]) {
          childUpdatesSucceeded = false;
          LOG.warn("Failed to persist convergence status at {}", changedKeys.get(i).getPath());
        }
      }
    }

    for (String resourceName : removedResources) {
      if (!accessor.removeProperty(keyBuilder.convergenceStatus(resourceName))) {
        childUpdatesSucceeded = false;
        LOG.warn("Failed to remove convergence status for dropped resource {}", resourceName);
      }
    }

    if (!childUpdatesSucceeded) {
      return;
    }

    boolean rootWriteRequired = hasChildMutations || oldClusterStatus == null
        || !newClusterStatus.semanticallyEquals(oldClusterStatus);
    if (rootWriteRequired) {
      if (!accessor.setProperty(keyBuilder.convergenceStatus(), newClusterStatus)) {
        LOG.warn("Failed to persist convergence status root for cluster {}",
            manager.getClusterName());
        return;
      }
      persistenceCache.update(manager.getSessionId(), newClusterStatus, newResourceStatuses);
      persistenceCache.markPersisted(System.currentTimeMillis());
    }
  }

  private ConvergenceStatusSnapshot calculateSnapshot(ClusterEvent event) {
    @SuppressWarnings("unchecked")
    Map<String, Resource> resources =
        event.getAttribute(AttributeName.RESOURCES_TO_REBALANCE.name());
    CurrentStateOutput currentStateOutput =
        event.getAttribute(AttributeName.CURRENT_STATE.name());
    ConvergenceStatusContext context =
        event.getAttribute(AttributeName.CONVERGENCE_STATUS_CONTEXT.name());
    if (resources == null || currentStateOutput == null || context == null) {
      return null;
    }
    try {
      String pipelineType = event.getAttribute(AttributeName.PipelineType.name());
      if (Pipeline.Type.MANAGEMENT_MODE.name().equals(pipelineType)) {
        return _calculator.calculatePaused(event, resources, currentStateOutput,
            Reason.MANAGEMENT_MODE);
      }
      return _calculator.calculate(event, context);
    } catch (Exception e) {
      LOG.error("Failed to calculate convergence for event {}", event.getEventId(), e);
      return _calculator.calculateUnknown(event, resources, currentStateOutput,
          Reason.TARGET_ASSIGNMENT_MISSING);
    }
  }

  private void initializePersistedState(HelixDataAccessor accessor, PropertyKey.Builder keyBuilder,
      String sessionId, ConvergenceStatusPersistenceCache persistenceCache) {
    if (persistenceCache.isInitializedFor(sessionId)) {
      return;
    }
    ConvergenceStatus clusterStatus = accessor.getProperty(keyBuilder.convergenceStatus());
    Map<String, ConvergenceStatus> resourceStatuses =
        clusterStatus == null ? Collections.emptyMap()
            : accessor.getChildValuesMap(keyBuilder.convergenceStatus(), false);
    persistenceCache.update(sessionId, clusterStatus, resourceStatuses);
  }

  private static void updateUnconvergedSince(ConvergenceStatus status,
      ConvergenceStatus oldStatus, long now) {
    if (status.getStatus() == Status.CONVERGED) {
      status.setUnconvergedSince(0L);
      return;
    }
    if (oldStatus != null && oldStatus.getStatus() != Status.CONVERGED
        && oldStatus.getUnconvergedSince() > 0) {
      status.setUnconvergedSince(oldStatus.getUnconvergedSince());
    } else {
      status.setUnconvergedSince(now);
    }
  }

  private static boolean isCurrentLeader(ClusterEvent event, HelixManager manager) {
    if (!manager.isLeader()) {
      return false;
    }
    Optional<String> expectedSession =
        event.getAttribute(AttributeName.EVENT_SESSION.name());
    return expectedSession == null || !expectedSession.isPresent()
        || expectedSession.get().equals(manager.getSessionId());
  }
}
