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

import java.util.Collections;
import java.util.Map;
import java.util.Set;
import java.util.SortedMap;
import java.util.SortedSet;
import java.util.TreeMap;
import java.util.TreeSet;

/**
 * Outcome of one {@link ExternalViewConvergenceEvaluator} evaluation.
 *
 * <p>The three statuses are deliberately distinct. {@link Status#CONVERGED} means every evaluated
 * resource matched its ideal mapping at {@link #getObservedAtMillis()}. {@link Status#PENDING}
 * means the comparison succeeded and at least one resource had not converged yet, so retrying
 * later can change the answer. {@link Status#FAILED} means at least one resource could not be
 * evaluated at all, for example because a requested resource does not exist or its state model
 * definition is missing; retrying without changing the cluster produces the same answer. A caller
 * that treats {@code FAILED} as {@code PENDING} loses that distinction, and a caller that treats
 * either of them as {@code CONVERGED} acts on an assignment Helix never confirmed.
 *
 * <p>{@link #getObservedAtMillis()} is the wall clock time the evaluation started reading. The
 * inputs are read with several requests, so the result is not an atomic snapshot of the cluster
 * and is not a claim about what the controller has processed by the time a caller reads it.
 */
public class ExternalViewConvergenceResult {
  public enum Status {
    CONVERGED, PENDING, FAILED
  }

  /**
   * Why a resource, or the cluster as a whole, is not reported as converged. The constant names
   * are part of the reported detail, so renaming one is a visible contract change.
   */
  public enum Reason {
    /** The observed live instances differ from the set the caller required. */
    LIVE_INSTANCES_MISMATCH,
    /** The resource has no external view yet and its external view is not disabled. */
    EXTERNAL_VIEW_MISSING,
    /** The external view does not match the ideal mapping computed for the resource. */
    MAPPING_MISMATCH,
    /** A Full-Auto resource has a partition with no preference list persisted yet. */
    PREFERENCE_LIST_EMPTY,
    /** A Full-Auto resource requires persisted best possible or intermediate assignment. */
    BEST_POSSIBLE_ASSIGNMENT_NOT_PERSISTED,
    /** The state model definition the resource refers to could not be read. */
    STATE_MODEL_DEFINITION_MISSING,
    /** A resource the caller asked about has neither an ideal state nor an external view. */
    RESOURCE_NOT_FOUND
  }

  private final Status _status;
  private final long _observedAtMillis;
  private final int _evaluatedResourceCount;
  private final SortedMap<String, Reason> _pendingResources;
  private final SortedMap<String, Reason> _failedResources;
  private final SortedSet<String> _unknownResources;
  private final Reason _clusterReason;

  ExternalViewConvergenceResult(long observedAtMillis, int evaluatedResourceCount,
      Map<String, Reason> pendingResources, Map<String, Reason> failedResources,
      Set<String> unknownResources, Reason clusterReason) {
    _observedAtMillis = observedAtMillis;
    _evaluatedResourceCount = evaluatedResourceCount;
    _pendingResources = Collections.unmodifiableSortedMap(new TreeMap<>(pendingResources));
    _failedResources = Collections.unmodifiableSortedMap(new TreeMap<>(failedResources));
    _unknownResources = Collections.unmodifiableSortedSet(new TreeSet<>(unknownResources));
    _clusterReason = clusterReason;
    if (!_failedResources.isEmpty() || !_unknownResources.isEmpty()) {
      _status = Status.FAILED;
    } else if (!_pendingResources.isEmpty() || _clusterReason != null) {
      _status = Status.PENDING;
    } else {
      _status = Status.CONVERGED;
    }
  }

  public Status getStatus() {
    return _status;
  }

  /**
   * @return true only when every evaluated resource matched its ideal mapping. Both
   *         {@link Status#PENDING} and {@link Status#FAILED} return false.
   */
  public boolean isConverged() {
    return _status == Status.CONVERGED;
  }

  /**
   * @return the wall clock time at which the evaluation started reading cluster metadata. It
   *         orders results from the same evaluator; it is not a cluster-wide snapshot version.
   */
  public long getObservedAtMillis() {
    return _observedAtMillis;
  }

  /**
   * @return how many resources were compared. Resources using the task state model, resources
   *         excluded by a resource filter and unknown resources are not counted.
   */
  public int getEvaluatedResourceCount() {
    return _evaluatedResourceCount;
  }

  /** @return resources that have not converged yet, sorted by resource name. */
  public SortedMap<String, Reason> getPendingResources() {
    return _pendingResources;
  }

  /** @return resources that could not be evaluated, sorted by resource name. */
  public SortedMap<String, Reason> getFailedResources() {
    return _failedResources;
  }

  /** @return requested resources that exist in neither the ideal states nor the external views. */
  public SortedSet<String> getUnknownResources() {
    return _unknownResources;
  }

  /** @return a cluster wide reason, or null when the cluster level checks passed. */
  public Reason getClusterReason() {
    return _clusterReason;
  }

  @Override
  public String toString() {
    return "ExternalViewConvergenceResult{status=" + _status + ", observedAtMillis="
        + _observedAtMillis + ", evaluatedResourceCount=" + _evaluatedResourceCount
        + ", clusterReason=" + _clusterReason + ", pendingResources=" + _pendingResources
        + ", failedResources=" + _failedResources + ", unknownResources=" + _unknownResources + "}";
  }
}
