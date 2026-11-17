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
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;

import org.apache.helix.HelixRebalanceException;
import org.apache.helix.controller.rebalancer.waged.WagedRebalanceStatus;
import org.apache.helix.model.ConvergenceStatus;
import org.apache.helix.model.ConvergenceStatus.OptimizerStatus;
import org.apache.helix.model.ConvergenceStatus.Reason;
import org.apache.helix.model.ConvergenceStatus.Status;
import org.apache.helix.model.ConvergenceStatus.TargetFreshness;
import org.apache.helix.model.Message;
import org.apache.helix.model.Resource;
import org.apache.helix.task.TaskConstants;

/**
 * Decides, partition by partition, whether the current assignment has reached the target: one
 * pass with no allocation per partition, off the pipeline thread, memory bounded by the counters.
 */
public final class ConvergenceStatusCalculator {
  private ConvergenceStatusCalculator() {
  }

  public static final class Result {
    private final ConvergenceStatus _cluster;
    private final Map<String, ConvergenceStatus> _resources;

    Result(ConvergenceStatus cluster, Map<String, ConvergenceStatus> resources) {
      _cluster = cluster;
      _resources = resources;
    }

    public ConvergenceStatus getCluster() {
      return _cluster;
    }

    public Map<String, ConvergenceStatus> getResources() {
      return _resources;
    }
  }

  public static Result calculate(ConvergenceInputs in, String controllerSessionId, long now) {
    Set<String> failedKeys = new HashSet<>();
    for (Message message : in._failedMessages) {
      List<String> names = message.getPartitionNames();
      if (names.isEmpty() && message.getPartitionName() != null) {
        failedKeys.add(key(message.getResourceName(), message.getPartitionName()));
      }
      for (String name : names) {
        failedKeys.add(key(message.getResourceName(), name));
      }
    }

    Map<String, ConvergenceStatus> reports = new TreeMap<>();
    ConvergenceTally clusterTally = new ConvergenceTally();
    TargetFreshness clusterFreshness = TargetFreshness.CURRENT;
    OptimizerStatus clusterOptimizer = OptimizerStatus.NOT_APPLICABLE;

    for (Resource resource : in._resources.values()) {
      if (TaskConstants.STATE_MODEL_NAME.equals(resource.getStateModelDefRef())) {
        continue;
      }
      ConvergenceResourceEvaluation evaluation =
          new ConvergenceResourceEvaluation(in, resource, failedKeys);
      evaluation.run();

      TargetFreshness freshness = in._pausedReason != null ? TargetFreshness.UNKNOWN
          : TargetFreshness.CURRENT;
      OptimizerStatus optimizer = in._pausedReason != null ? OptimizerStatus.UNKNOWN
          : OptimizerStatus.NOT_APPLICABLE;
      if (in._pausedReason == null && in._wagedResources.contains(resource.getResourceName())) {
        WagedRebalanceStatus waged = in._wagedStatus;
        boolean known = waged != null && waged.isOutcomeKnown();
        optimizer = !known ? OptimizerStatus.UNKNOWN : waged.isBaselineComputationFailed()
            ? OptimizerStatus.BASELINE_FAILED : OptimizerStatus.HEALTHY;
        Reason override = null;
        if (waged == null) {
          freshness = TargetFreshness.UNKNOWN;
          override = Reason.WAGED_INTERNAL_FAILURE;
        } else if (!known) {
          // A new leader has not finished a run yet, so there is no verified target to compare to.
          freshness = TargetFreshness.UNKNOWN;
          override = Reason.TARGET_ASSIGNMENT_MISSING;
        } else if (waged.isLastKnownGoodFallback()) {
          freshness = TargetFreshness.LAST_KNOWN_GOOD;
          override = wagedReason(waged, Reason.WAGED_LAST_KNOWN_GOOD);
        } else if (waged.isServingComputationFailed()) {
          freshness = TargetFreshness.UNKNOWN;
          override = wagedReason(waged, Reason.WAGED_INTERNAL_FAILURE);
        }
        if (override != null) {
          evaluation.overrideAllUnknown(override);
        }
      }

      ConvergenceStatus report = new ConvergenceStatus(resource.getResourceName());
      fill(report, evaluation._tally, in._controllerName, controllerSessionId, now);
      report.setTargetFreshness(freshness);
      report.setOptimizerStatus(optimizer);
      reports.put(resource.getResourceName(), report);

      clusterTally.add(evaluation._tally);
      clusterFreshness = worse(clusterFreshness, freshness);
      clusterOptimizer = worse(clusterOptimizer, optimizer);
    }

    ConvergenceStatus cluster = new ConvergenceStatus(in._clusterName);
    fill(cluster, clusterTally, in._controllerName, controllerSessionId, now);
    cluster.setResourceCount(reports.size());
    cluster.setCoveredThrough(in._coveredThrough);
    cluster.setTargetFreshness(clusterFreshness);
    cluster.setOptimizerStatus(clusterOptimizer);
    if (in._pausedReason != null) {
      // The cluster record alone carries a pause, even for a cluster with no partitions.
      cluster.setStatus(Status.PAUSED);
      cluster.setPrimaryReason(in._pausedReason);
      cluster.setTargetFreshness(TargetFreshness.UNKNOWN);
      cluster.setOptimizerStatus(OptimizerStatus.UNKNOWN);
    }
    return new Result(cluster, Collections.unmodifiableMap(reports));
  }

  private static void fill(ConvergenceStatus report, ConvergenceTally tally, String controllerName,
      String sessionId, long now) {
    Status status = tally.overallStatus();
    report.setStatus(status);
    report.setPrimaryReason(tally.primaryReason(status));
    report.setStatusCounts(tally.statusCounts());
    report.setReasonCounts(tally.reasonCounts());
    report.setGeneratedAt(now);
    report.setControllerSessionId(sessionId);
    report.setControllerName(controllerName);
  }

  static int severity(Status status) {
    switch (status) {
      case PAUSED:
        return 5;
      case BLOCKED:
        return 4;
      case UNKNOWN:
        return 3;
      case IN_PROGRESS:
        return 2;
      default:
        return 1;
    }
  }

  /** Lower is more urgent. */
  static int priority(Reason reason) {
    switch (reason) {
      case MANAGEMENT_MODE:
      case MAINTENANCE_MODE:
        return 1;
      case WAGED_LAST_KNOWN_GOOD:
      case WAGED_CAPACITY_DEFICIT:
      case WAGED_NO_CANDIDATE_NODE:
      case WAGED_INVALID_CONFIGURATION:
      case WAGED_INTERNAL_FAILURE:
        return 2;
      case TARGET_ASSIGNMENT_MISSING:
      case INVALID_STATE:
      case ERROR_STATE:
        return 3;
      case MESSAGE_DISPATCH_FAILED:
      case NO_PROGRESS_PATH:
      case UNDER_REPLICATED:
      case OUTSIDE_PREFERENCE_LIST:
        return 4;
      case TARGET_INSTANCE_NOT_LIVE:
        return 5;
      case MESSAGE_THROTTLED:
      case STATE_CONSTRAINT_WAIT:
        return 6;
      default:
        return 7;
    }
  }

  private static TargetFreshness worse(TargetFreshness a, TargetFreshness b) {
    return a.ordinal() >= b.ordinal() ? a : b;
  }

  private static OptimizerStatus worse(OptimizerStatus a, OptimizerStatus b) {
    return rank(a) >= rank(b) ? a : b;
  }

  private static int rank(OptimizerStatus status) {
    switch (status) {
      case BASELINE_FAILED:
        return 3;
      case UNKNOWN:
        return 2;
      case HEALTHY:
        return 1;
      default:
        return 0;
    }
  }

  private static Reason wagedReason(WagedRebalanceStatus status, Reason fallback) {
    HelixRebalanceException.FailureCategory category = status.getServingFailureCategory();
    if (category == null) {
      return fallback;
    }
    switch (category) {
      case CAPACITY_DEFICIT:
        return Reason.WAGED_CAPACITY_DEFICIT;
      case NO_CANDIDATE_NODE:
        return Reason.WAGED_NO_CANDIDATE_NODE;
      case INVALID_RESOURCE_CONFIG:
      case INVALID_CLUSTER_CONFIG:
        return Reason.WAGED_INVALID_CONFIGURATION;
      default:
        return Reason.WAGED_INTERNAL_FAILURE;
    }
  }

  static String key(String resource, String partition) {
    return resource + '\u0000' + partition;
  }

}
