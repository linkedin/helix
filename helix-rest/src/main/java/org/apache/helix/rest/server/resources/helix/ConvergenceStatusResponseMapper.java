package org.apache.helix.rest.server.resources.helix;

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

import java.util.Map;

import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.helix.model.ConvergenceStatus;
import org.apache.helix.model.ConvergenceStatus.PartitionDetail;
import org.apache.helix.model.ConvergenceStatus.Reason;
import org.apache.helix.model.ConvergenceStatus.Scope;
import org.apache.helix.model.ConvergenceStatus.Status;
import org.apache.helix.model.LiveInstance;

/**
 * Maps the bounded convergence model to the customer-facing REST representation.
 */
final class ConvergenceStatusResponseMapper {
  private enum StaleReason {
    REPORT_MISSING,
    SCHEMA_UNSUPPORTED,
    REPORT_INCOMPLETE,
    REPORT_INVALID,
    CONTROLLER_LEADER_MISSING,
    CONTROLLER_SESSION_MISMATCH,
    CLUSTER_REPORT_STALE,
    MONITORING_DISABLED
  }

  private ConvergenceStatusResponseMapper() {
  }

  static ObjectNode mapCluster(String clusterId, ConvergenceStatus report, LiveInstance leader) {
    return mapCluster(clusterId, report, leader, true);
  }

  static ObjectNode mapCluster(String clusterId, ConvergenceStatus report, LiveInstance leader,
      boolean monitoringEnabled) {
    return map(clusterId, null, report, null, leader, Scope.CLUSTER, monitoringEnabled);
  }

  static ObjectNode mapResource(String clusterId, String resourceName, ConvergenceStatus report,
      LiveInstance leader) {
    return map(clusterId, resourceName, report, null, leader, Scope.RESOURCE, true);
  }

  static ObjectNode mapResource(String clusterId, String resourceName, ConvergenceStatus report,
      ConvergenceStatus clusterReport, LiveInstance leader) {
    return mapResource(clusterId, resourceName, report, clusterReport, leader, true);
  }

  static ObjectNode mapResource(String clusterId, String resourceName, ConvergenceStatus report,
      ConvergenceStatus clusterReport, LiveInstance leader, boolean monitoringEnabled) {
    return map(clusterId, resourceName, report, clusterReport, leader, Scope.RESOURCE,
        monitoringEnabled);
  }

  static boolean isFreshClusterReport(ConvergenceStatus report, LiveInstance leader) {
    return isFreshClusterReport(report, leader, true);
  }

  static boolean isFreshClusterReport(ConvergenceStatus report, LiveInstance leader,
      boolean monitoringEnabled) {
    return monitoringEnabled && staleReason(report, leader, Scope.CLUSTER, null) == null;
  }

  private static ObjectNode map(String clusterId, String resourceName, ConvergenceStatus report,
      ConvergenceStatus clusterReport, LiveInstance leader, Scope expectedScope,
      boolean monitoringEnabled) {
    ObjectNode response = JsonNodeFactory.instance.objectNode();
    response.put("clusterId", clusterId);
    if (resourceName != null) {
      response.put("resourceName", resourceName);
    }

    StaleReason staleReason = monitoringEnabled ? staleReason(report, leader, expectedScope,
        resourceName) : StaleReason.MONITORING_DISABLED;
    if (staleReason == null && clusterReport != null
        && staleReason(clusterReport, leader, Scope.CLUSTER, null) != null) {
      staleReason = StaleReason.CLUSTER_REPORT_STALE;
    }
    Status reportedStatus = report == null ? Status.UNKNOWN : report.getStatus();
    Status effectiveStatus = staleReason == null ? reportedStatus : Status.UNKNOWN;
    response.put("status", effectiveStatus.name());
    response.put("effectiveStatus", effectiveStatus.name());
    response.put("reportedStatus", reportedStatus.name());
    response.put("primaryReason",
        (report == null ? Reason.NONE : report.getPrimaryReason()).name());

    ObjectNode partitionSummary = response.putObject("partitionSummary");
    if (expectedScope == Scope.CLUSTER) {
      response.put("resourceCount", report == null ? 0 : report.getTotalResourceCount());
    }
    partitionSummary.put("total", report == null ? 0 : report.getTotalPartitionCount());
    partitionSummary.put("converged", report == null ? 0 : report.getConvergedPartitionCount());
    partitionSummary.put("inProgress",
        report == null ? 0 : report.getInProgressPartitionCount());
    partitionSummary.put("blocked", report == null ? 0 : report.getBlockedPartitionCount());
    partitionSummary.put("unknown", report == null ? 0 : report.getUnknownPartitionCount());
    partitionSummary.put("affected", report == null ? 0 : report.getAffectedPartitionCount());

    ObjectNode reasonCounts = response.putObject("reasonCounts");
    if (report != null) {
      report.getReasonCounts()
          .forEach((reason, count) -> reasonCounts.put(reason.name(), count));
    }
    ObjectNode statusCounts = response.putObject("statusCounts");
    if (report != null) {
      report.getStatusCounts()
          .forEach((status, count) -> statusCounts.put(status.name(), count));
    }

    response.put("targetFreshness",
        (report == null ? ConvergenceStatus.TargetFreshness.UNKNOWN
            : report.getTargetFreshness()).name());
    response.put("optimizerStatus",
        (report == null ? ConvergenceStatus.OptimizerStatus.UNKNOWN
            : report.getOptimizerStatus()).name());
    response.put("oldestUnconvergedMs", oldestUnconvergedMs(report));
    response.put("generatedAt", report == null ? 0L : report.getGeneratedAt());
    putNullable(response, "sourceEventId", report == null ? null : report.getSourceEventId());
    putNullable(response, "controllerSessionId",
        report == null ? null : report.getControllerSessionId());
    response.put("stale", staleReason != null);
    putNullable(response, "staleReason", staleReason == null ? null : staleReason.name());
    response.put("truncatedPartitionCount",
        report == null ? 0 : report.getTruncatedPartitionCount());

    ArrayNode affectedPartitions = response.putArray("affectedPartitions");
    if (report != null) {
      int detailCount = 0;
      for (PartitionDetail detail : report.getPartitionDetails()) {
        if (detailCount++ >= ConvergenceStatus.DEFAULT_MAX_PARTITION_DETAILS) {
          break;
        }
        ObjectNode detailNode = affectedPartitions.addObject();
        putNullable(detailNode, "resourceName", detail.getResourceName());
        putNullable(detailNode, "partitionName", detail.getPartitionName());
        detailNode.put("status", detail.getStatus().name());
        detailNode.put("reason", detail.getReason().name());
        putAssignment(detailNode.putObject("currentAssignment"), detail.getCurrentAssignment());
        putAssignment(detailNode.putObject("expectedAssignment"), detail.getTargetAssignment());
      }
    }
    return response;
  }

  private static StaleReason staleReason(ConvergenceStatus report, LiveInstance leader,
      Scope expectedScope, String expectedResourceName) {
    if (report == null) {
      return StaleReason.REPORT_MISSING;
    }
    if (report.getSchemaVersion() != ConvergenceStatus.SCHEMA_VERSION) {
      return StaleReason.SCHEMA_UNSUPPORTED;
    }
    if (!report.isComplete()) {
      return StaleReason.REPORT_INCOMPLETE;
    }
    if (!report.isValid() || report.getScope() != expectedScope
        || expectedResourceName != null
            && !expectedResourceName.equals(report.getResourceName())) {
      return StaleReason.REPORT_INVALID;
    }
    if (leader == null) {
      return StaleReason.CONTROLLER_LEADER_MISSING;
    }
    String leaderSession = leader.getEphemeralOwner();
    if (leaderSession == null || !leaderSession.equals(report.getControllerSessionId())) {
      return StaleReason.CONTROLLER_SESSION_MISMATCH;
    }
    return null;
  }

  private static long oldestUnconvergedMs(ConvergenceStatus report) {
    if (report == null || report.getUnconvergedSince() <= 0L) {
      return 0L;
    }
    return Math.max(0L, System.currentTimeMillis() - report.getUnconvergedSince());
  }

  private static void putAssignment(ObjectNode target, Map<String, String> assignment) {
    int entryCount = 0;
    for (Map.Entry<String, String> entry : assignment.entrySet()) {
      if (entryCount++ >= ConvergenceStatus.DEFAULT_MAX_ASSIGNMENT_ENTRIES) {
        break;
      }
      putNullable(target, entry.getKey(), entry.getValue());
    }
  }

  private static void putNullable(ObjectNode target, String fieldName, String value) {
    if (value == null) {
      target.putNull(fieldName);
    } else {
      target.put(fieldName, value);
    }
  }
}
