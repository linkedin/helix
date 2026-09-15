package org.apache.helix.util;

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

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;

import org.apache.helix.HelixDataAccessor;
import org.apache.helix.HelixDefinedState;
import org.apache.helix.HelixException;
import org.apache.helix.PropertyKey;
import org.apache.helix.controller.dataproviders.ResourceControllerDataProvider;
import org.apache.helix.controller.rebalancer.AbstractRebalancer;
import org.apache.helix.model.ClusterConfig;
import org.apache.helix.model.ExternalView;
import org.apache.helix.model.IdealState;
import org.apache.helix.model.Partition;
import org.apache.helix.model.StateModelDefinition;
import org.apache.helix.task.TaskConstants;

/**
 * Compares the external view of every resource against the mapping its ideal state implies, and
 * reports which resources have converged.
 *
 * <p>This is the calculation
 * {@link org.apache.helix.tools.ClusterVerifiers.StrictMatchExternalViewVerifier} performs, made
 * reusable so a caller that only wants to observe convergence once does not have to run a
 * verifier, keep a connection open, or wait on a callback. The verifier itself delegates here, so
 * there is a single definition of what convergence means.
 *
 * <p>The evaluation reads cluster metadata and never writes it. Use
 * {@link #readOnlySnapshot(String, HelixDataAccessor)} to load the inputs when the caller must not
 * mutate the cluster: the controller data provider persists participant offline history during an
 * ordinary refresh, which is a write an observation must not perform. The evaluation never
 * triggers a rebalance, and observing a resource never changes whether it converges.
 *
 * <p>An evaluation is a single bounded pass: it reads the cluster once and compares what it read.
 * It does not wait, retry or subscribe, so a caller that wants to wait for convergence polls at an
 * interval it controls.
 */
public class ExternalViewConvergenceEvaluator {
  private final boolean _lenientMatch;
  private final boolean _deactivatedNodeAware;
  private final boolean _stopAtFirstIssue;
  private final boolean _failOnUnknownResources;

  private ExternalViewConvergenceEvaluator(Builder builder) {
    _lenientMatch = builder._lenientMatch;
    _deactivatedNodeAware = builder._deactivatedNodeAware;
    _stopAtFirstIssue = builder._stopAtFirstIssue;
    _failOnUnknownResources = builder._failOnUnknownResources;
  }

  public static class Builder {
    private boolean _lenientMatch = false;
    private boolean _deactivatedNodeAware = false;
    private boolean _stopAtFirstIssue = false;
    private boolean _failOnUnknownResources = true;

    /**
     * When true, entries in the state model's initial state and in
     * {@link HelixDefinedState#DROPPED} are removed from both sides before comparing, so a replica
     * Helix has already finished moving away from does not hold the resource back.
     */
    public Builder setLenientMatch(boolean lenientMatch) {
      _lenientMatch = lenientMatch;
      return this;
    }

    /**
     * When true, the expected mapping is computed from assignable live instances and the instances
     * on which the partition is disabled, instead of from enabled live instances only.
     */
    public Builder setDeactivatedNodeAware(boolean deactivatedNodeAware) {
      _deactivatedNodeAware = deactivatedNodeAware;
      return this;
    }

    /**
     * When true, the evaluation returns as soon as one resource is found not converged. A caller
     * that only needs the overall answer, such as a polling verifier, avoids computing the
     * expected mapping of the remaining resources. The reported detail is then the first issue
     * found rather than every issue.
     */
    public Builder setStopAtFirstIssue(boolean stopAtFirstIssue) {
      _stopAtFirstIssue = stopAtFirstIssue;
      return this;
    }

    /**
     * When true, a resource the caller asked about that has neither an ideal state nor an external
     * view makes the result {@link ExternalViewConvergenceResult.Status#FAILED}, because there is
     * nothing whose convergence could have been observed. When false such a resource is ignored,
     * which is the long standing verifier behavior and means a caller can be told that a resource
     * Helix does not know about has converged.
     */
    public Builder setFailOnUnknownResources(boolean failOnUnknownResources) {
      _failOnUnknownResources = failOnUnknownResources;
      return this;
    }

    public ExternalViewConvergenceEvaluator build() {
      return new ExternalViewConvergenceEvaluator(this);
    }
  }

  /**
   * Loads the cluster metadata this evaluation needs without writing any of it back.
   *
   * <p>{@link ResourceControllerDataProvider#refresh} normally records the offline timestamp of a
   * participant whose history still reads as online, which is a write to participant metadata. An
   * observation must not do that, so this method disables it: an instance whose offline time has
   * not been recorded yet is left exactly as it was found.
   *
   * @param clusterName the cluster to read
   * @param accessor an accessor already connected to that cluster. The accessor is borrowed, not
   *        owned: this method opens no connection and closes none, so a caller reusing a shared
   *        accessor leaks neither connections nor threads per evaluation.
   * @return a populated data provider to pass to
   *         {@link #evaluate(HelixDataAccessor, ResourceControllerDataProvider, Set, Set)}
   */
  public static ResourceControllerDataProvider readOnlySnapshot(String clusterName,
      HelixDataAccessor accessor) {
    ResourceControllerDataProvider cache = new ResourceControllerDataProvider(clusterName);
    cache.setPersistOfflineInstanceHistory(false);
    cache.refresh(accessor);
    return cache;
  }

  /**
   * Compares the external views against the ideal mappings of a refreshed cluster.
   *
   * @param accessor the accessor the external views are read through
   * @param cache cluster metadata already refreshed from the same cluster
   * @param resources the resources to evaluate, or null/empty to evaluate every resource in the
   *        cluster. Resources using the task state model are always skipped.
   * @param expectLiveInstances if not null and not empty, the evaluation reports the cluster as
   *        pending unless exactly these instances are live
   * @return the outcome; it is never null
   * @throws HelixException if the metadata needed to decide convergence could not be read. An
   *         unreadable cluster is reported as a failure and never as an empty converged result.
   */
  public ExternalViewConvergenceResult evaluate(HelixDataAccessor accessor,
      ResourceControllerDataProvider cache, Set<String> resources,
      Set<String> expectLiveInstances) {
    long observedAtMillis = System.currentTimeMillis();
    Map<String, ExternalViewConvergenceResult.Reason> pending = new LinkedHashMap<>();
    Map<String, ExternalViewConvergenceResult.Reason> failed = new LinkedHashMap<>();

    if (expectLiveInstances != null && !expectLiveInstances.isEmpty()
        && !expectLiveInstances.equals(cache.getLiveInstances().keySet())) {
      return new ExternalViewConvergenceResult(observedAtMillis, 0, pending, failed,
          Collections.emptySet(), Collections.emptySet(),
          ExternalViewConvergenceResult.Reason.LIVE_INSTANCES_MISMATCH);
    }

    Map<String, IdealState> idealStates = new HashMap<>(cache.getIdealStates());
    PropertyKey.Builder keyBuilder = accessor.keyBuilder();
    // Reading with throwException set keeps a failed read distinguishable from a cluster that
    // genuinely has no external views, which would otherwise look converged.
    Map<String, ExternalView> externalViews =
        accessor.getChildValuesMap(keyBuilder.externalViews(), true);
    externalViews = externalViews == null ? new HashMap<>() : new HashMap<>(externalViews);

    Set<String> unknownResources = Collections.emptySet();
    if (resources != null && !resources.isEmpty()) {
      // Computed before any resource is filtered out, so a resource that exists but is not
      // evaluated is never reported as one Helix does not know about.
      unknownResources = new TreeSet<>(resources);
      unknownResources.removeAll(idealStates.keySet());
      unknownResources.removeAll(externalViews.keySet());
      idealStates.keySet().retainAll(resources);
      externalViews.keySet().retainAll(resources);
      if (!_failOnUnknownResources) {
        unknownResources = Collections.emptySet();
      }
    }

    // Jobs are managed by the task framework and have no external view to match.
    Set<String> skippedResources = new TreeSet<>();
    idealStates.entrySet().removeIf(entry -> {
      if (TaskConstants.STATE_MODEL_NAME.equals(entry.getValue().getStateModelDefRef())) {
        skippedResources.add(entry.getKey());
        return true;
      }
      return false;
    });

    // A resource can have an external view the controller has not finished removing after its
    // ideal state is gone. Comparing it against an empty ideal state reports it as pending rather
    // than skipping it.
    for (String resource : externalViews.keySet()) {
      idealStates.computeIfAbsent(resource, IdealState::new);
    }
    skippedResources.removeAll(idealStates.keySet());

    int evaluatedResourceCount = 0;
    for (Map.Entry<String, IdealState> entry : idealStates.entrySet()) {
      String resourceName = entry.getKey();
      IdealState idealState = entry.getValue();
      ExternalView externalView = externalViews.get(resourceName);

      if (externalView == null) {
        if (idealState.isExternalViewDisabled()) {
          // The resource publishes no external view by configuration, so there is nothing to
          // compare and nothing to wait for.
          skippedResources.add(resourceName);
          continue;
        }
        evaluatedResourceCount++;
        pending.put(resourceName,
            ExternalViewConvergenceResult.Reason.EXTERNAL_VIEW_MISSING);
        if (_stopAtFirstIssue) {
          break;
        }
        continue;
      }

      evaluatedResourceCount++;
      ExternalViewConvergenceResult.Reason reason =
          evaluateResource(cache, externalView, idealState);
      if (reason == null) {
        continue;
      }
      if (isFailure(reason)) {
        failed.put(resourceName, reason);
      } else {
        pending.put(resourceName, reason);
      }
      if (_stopAtFirstIssue) {
        break;
      }
    }

    return new ExternalViewConvergenceResult(observedAtMillis, evaluatedResourceCount, pending,
        failed, unknownResources, skippedResources, null);
  }

  private static boolean isFailure(ExternalViewConvergenceResult.Reason reason) {
    return reason == ExternalViewConvergenceResult.Reason.BEST_POSSIBLE_ASSIGNMENT_NOT_PERSISTED
        || reason == ExternalViewConvergenceResult.Reason.STATE_MODEL_DEFINITION_MISSING;
  }

  /**
   * @return null when the resource has converged, otherwise why it has not.
   */
  private ExternalViewConvergenceResult.Reason evaluateResource(
      ResourceControllerDataProvider cache, ExternalView externalView, IdealState idealState) {
    Map<String, Map<String, String>> mappingInExternalView =
        externalView.getRecord().getMapFields();
    Map<String, Map<String, String>> idealPartitionState;

    switch (idealState.getRebalanceMode()) {
      case FULL_AUTO:
        // The controller computes this assignment on every pipeline run, so it can only be
        // compared against when the cluster persists it.
        ClusterConfig clusterConfig = cache.getClusterConfig();
        if (clusterConfig == null) {
          throw new HelixException(
              "Cluster config is unavailable, cannot evaluate Full-Auto resource "
                  + idealState.getResourceName());
        }
        if (!clusterConfig.isPersistBestPossibleAssignment()
            && !clusterConfig.isPersistIntermediateAssignment()) {
          return ExternalViewConvergenceResult.Reason.BEST_POSSIBLE_ASSIGNMENT_NOT_PERSISTED;
        }
        for (String partition : idealState.getPartitionSet()) {
          List<String> preferenceList = idealState.getPreferenceList(partition);
          if (preferenceList == null || preferenceList.isEmpty()) {
            return ExternalViewConvergenceResult.Reason.PREFERENCE_LIST_EMPTY;
          }
        }
        idealPartitionState = computeIdealPartitionState(cache, idealState);
        break;
      case SEMI_AUTO:
      case USER_DEFINED:
        idealPartitionState = computeIdealPartitionState(cache, idealState);
        break;
      case CUSTOMIZED:
        idealPartitionState = idealState.getRecord().getMapFields();
        break;
      case TASK:
        // Jobs are ignored.
      default:
        return null;
    }

    if (idealPartitionState == null) {
      return ExternalViewConvergenceResult.Reason.STATE_MODEL_DEFINITION_MISSING;
    }

    if (!_lenientMatch) {
      return mappingInExternalView.equals(idealPartitionState) ? null
          : ExternalViewConvergenceResult.Reason.MAPPING_MISMATCH;
    }

    StateModelDefinition stateModelDef =
        cache.getStateModelDef(idealState.getStateModelDefRef());
    return compareMappings(mappingInExternalView, idealPartitionState, stateModelDef) ? null
        : ExternalViewConvergenceResult.Reason.MAPPING_MISMATCH;
  }

  private boolean compareMappings(Map<String, Map<String, String>> actualMappings,
      Map<String, Map<String, String>> expectedMappings, StateModelDefinition stateModelDef) {
    if (!_lenientMatch || stateModelDef == null) {
      return actualMappings.equals(expectedMappings);
    }

    Set<String> ignoredStates = new HashSet<>(
        Arrays.asList(stateModelDef.getInitialState(), HelixDefinedState.DROPPED.toString()));
    return copyWithoutIgnoredStates(actualMappings, ignoredStates)
        .equals(copyWithoutIgnoredStates(expectedMappings, ignoredStates));
  }

  private static Map<String, Map<String, String>> copyWithoutIgnoredStates(
      Map<String, Map<String, String>> original, Set<String> ignoredStates) {
    Map<String, Map<String, String>> copiedMappings = deepCopyMapFields(original);
    removeEntriesWithIgnoredStates(copiedMappings.entrySet().iterator(), ignoredStates);
    return copiedMappings;
  }

  private static Map<String, Map<String, String>> deepCopyMapFields(
      Map<String, Map<String, String>> original) {
    Map<String, Map<String, String>> copy = new HashMap<>();
    for (Map.Entry<String, Map<String, String>> entry : original.entrySet()) {
      copy.put(entry.getKey(), new HashMap<>(entry.getValue()));
    }
    return copy;
  }

  private static void removeEntriesWithIgnoredStates(
      Iterator<Map.Entry<String, Map<String, String>>> partitionInstanceStateMapIter,
      Set<String> ignoredStates) {
    while (partitionInstanceStateMapIter.hasNext()) {
      Map.Entry<String, Map<String, String>> entry = partitionInstanceStateMapIter.next();
      Map<String, String> instanceStateMap = entry.getValue();
      Iterator<Map.Entry<String, String>> insIter = instanceStateMap.entrySet().iterator();
      while (insIter.hasNext()) {
        String state = insIter.next().getValue();
        if (ignoredStates.contains(state)) {
          insIter.remove();
        }
      }
      if (instanceStateMap.isEmpty()) {
        partitionInstanceStateMapIter.remove();
      }
    }
  }

  /**
   * @return the mapping the resource is expected to reach, or null when the state model definition
   *         it refers to is not available and no expectation can be computed.
   */
  private Map<String, Map<String, String>> computeIdealPartitionState(
      ResourceControllerDataProvider cache, IdealState idealState) {
    Map<String, Map<String, String>> idealPartitionState = new HashMap<>();
    Set<String> partitions = idealState.getPartitionSet();
    if (partitions.isEmpty()) {
      // Nothing to place, so no state model definition is needed. This is how a resource whose
      // ideal state is already gone but whose external view has not been removed yet is compared.
      return idealPartitionState;
    }

    StateModelDefinition stateModelDef =
        cache.getStateModelDef(idealState.getStateModelDefRef());
    if (stateModelDef == null) {
      return null;
    }

    for (String partition : partitions) {
      List<String> preferenceList = AbstractRebalancer.getPreferenceList(new Partition(partition),
          idealState, cache.getEnabledLiveInstances());
      Map<String, String> idealMapping;
      if (_deactivatedNodeAware) {
        idealMapping = HelixUtil.computeIdealMapping(preferenceList, stateModelDef,
            cache.getAssignableLiveInstances().keySet(),
            cache.getDisabledInstancesForPartition(idealState.getResourceName(), partition));
      } else {
        idealMapping = HelixUtil.computeIdealMapping(preferenceList, stateModelDef,
            cache.getEnabledLiveInstances(), Collections.emptySet());
      }
      idealPartitionState.put(partition, idealMapping);
    }

    return idealPartitionState;
  }
}
