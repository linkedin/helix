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
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

import java.util.Collections;

import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.helix.api.status.ClusterManagementMode;
import org.apache.helix.model.ConvergenceStatus;
import org.apache.helix.model.ConvergenceStatus.OptimizerStatus;
import org.apache.helix.model.ConvergenceStatus.Reason;
import org.apache.helix.model.ConvergenceStatus.Status;
import org.apache.helix.model.ConvergenceStatus.TargetFreshness;
import org.apache.helix.model.LiveInstance;

/**
 * Turns a stored convergence report into the REST response. Reports are only trusted while the
 * cluster record carries the session of the controller that currently leads the cluster: the
 * controller takes that record over only once every resource report agrees with what it computed,
 * so a report left by an earlier leader is trusted only after the new one has confirmed it. Age is
 * deliberately not used: the controller only writes when something changes, so a healthy idle
 * cluster has old reports.
 */
final class ConvergenceStatusResponseMapper {
  static final String FRESH = "FRESH";

  private ConvergenceStatusResponseMapper() {
  }

  /**
   * @param report the report asked for: the cluster record, or the report of one resource
   * @param clusterReport the cluster record, which decides whether any report is trusted
   */
  static ObjectNode map(String clusterId, String resourceName, ConvergenceStatus report,
      ConvergenceStatus clusterReport, LiveInstance leader, boolean monitoringEnabled,
      boolean managementMode) {
    ObjectNode response = JsonNodeFactory.instance.objectNode();
    response.put("clusterId", clusterId);
    if (resourceName != null) {
      response.put("resourceName", resourceName);
    }
    if (managementMode) {
      return unavailable(response, Status.PAUSED, Reason.MANAGEMENT_MODE, "MANAGEMENT_MODE");
    }
    String problem = problem(report, clusterReport, leader, monitoringEnabled);
    if (problem != null) {
      return unavailable(response, Status.UNKNOWN, Reason.NONE, problem);
    }
    if (resourceName != null && clusterReport.getStatus() == Status.PAUSED) {
      report = pausedView(report, clusterReport);
    }

    response.put("status", report.getStatus().name());
    response.put("primaryReason", report.getPrimaryReason().name());
    response.put("reportState", FRESH);
    // When the content was last computed differently; it is still current, since the controller
    // named below vouches for it through the cluster record.
    response.put("generatedAt", report.getGeneratedAt());
    response.put("publishedBy", clusterReport.getControllerName());
    response.put("controllerSessionId", clusterReport.getControllerSessionId());
    response.put("unconvergedSince", report.getUnconvergedSince());
    response.put("targetFreshness", report.getTargetFreshness().name());
    response.put("optimizerStatus", report.getOptimizerStatus().name());
    if (resourceName == null) {
      response.put("resourceCount", report.getResourceCount());
    }
    ObjectNode summary = response.putObject("partitionSummary");
    summary.put("total", report.getPartitionCount());
    summary.put("affected", report.getAffectedPartitionCount());
    ObjectNode statusCounts = summary.putObject("statusCounts");
    report.getStatusCounts().forEach((status, count) -> statusCounts.put(status.name(), count));
    ObjectNode reasonCounts = summary.putObject("reasonCounts");
    report.getReasonCounts().forEach((reason, count) -> reasonCounts.put(reason.name(), count));
    return response;
  }

  // The ClusterStatus znode outlives management mode and then reads NORMAL/COMPLETED.
  static boolean isManagementMode(ClusterManagementMode mode) {
    return mode != null && !mode.isFullyInNormalMode();
  }

  private static String problem(ConvergenceStatus report, ConvergenceStatus clusterReport,
      LiveInstance leader, boolean monitoringEnabled) {
    if (!monitoringEnabled) {
      return "MONITORING_DISABLED";
    }
    if (report == null || clusterReport == null) {
      return "REPORT_MISSING";
    }
    if (!report.isValid() || !clusterReport.isValid()) {
      return "SCHEMA_UNSUPPORTED";
    }
    if (leader == null) {
      return "CONTROLLER_LEADER_MISSING";
    }
    String session = leader.getEphemeralOwner();
    if (session == null || !session.equals(clusterReport.getControllerSessionId())) {
      return "CONTROLLER_SESSION_MISMATCH";
    }
    return null;
  }

  /**
   * While the cluster is paused only the cluster record is kept current, so every partition of a
   * resource reads PAUSED for the cluster's reason.
   */
  private static ConvergenceStatus pausedView(ConvergenceStatus report,
      ConvergenceStatus clusterReport) {
    ConvergenceStatus view = new ConvergenceStatus(report.getId());
    view.setStatus(Status.PAUSED);
    view.setPrimaryReason(clusterReport.getPrimaryReason());
    view.setGeneratedAt(clusterReport.getGeneratedAt());
    view.setUnconvergedSince(clusterReport.getUnconvergedSince());
    view.setControllerSessionId(clusterReport.getControllerSessionId());
    view.setControllerName(clusterReport.getControllerName());
    view.setTargetFreshness(TargetFreshness.UNKNOWN);
    view.setOptimizerStatus(OptimizerStatus.UNKNOWN);
    int total = report.getPartitionCount();
    if (total > 0) {
      view.setStatusCounts(Collections.singletonMap(Status.PAUSED, total));
      view.setReasonCounts(Collections.singletonMap(clusterReport.getPrimaryReason(), total));
    }
    return view;
  }

  private static ObjectNode unavailable(ObjectNode response, Status status, Reason reason,
      String state) {
    response.put("status", status.name());
    response.put("primaryReason", reason.name());
    response.put("reportState", state);
    return response;
  }
}
