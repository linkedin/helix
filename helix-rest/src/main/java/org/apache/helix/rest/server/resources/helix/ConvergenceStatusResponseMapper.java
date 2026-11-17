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
import java.util.Comparator;
import java.util.List;

import com.fasterxml.jackson.databind.node.ArrayNode;
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
 * Turns the stored convergence record into the REST response. It is only trusted while it carries
 * the session of the controller that currently leads the cluster, so a record left by an earlier
 * leader is trusted only after the new one has rewritten it.
 *
 * <p>{@code reportState} is FRESH, or the first that applies of: MANAGEMENT_MODE,
 * MONITORING_DISABLED, REPORT_MISSING, SCHEMA_UNSUPPORTED, CONTROLLER_LEADER_MISSING,
 * CONTROLLER_SESSION_MISMATCH, and REPORT_MISSING for a resource the controller has not covered
 * yet. Only FRESH carries the status details.
 */
final class ConvergenceStatusResponseMapper {
  // reportState: whether the report can be trusted, and if not, why.
  static final String FRESH = "FRESH";
  static final String MONITORING_DISABLED = "MONITORING_DISABLED";
  static final String REPORT_MISSING = "REPORT_MISSING";
  static final String SCHEMA_UNSUPPORTED = "SCHEMA_UNSUPPORTED";
  static final String CONTROLLER_LEADER_MISSING = "CONTROLLER_LEADER_MISSING";
  static final String CONTROLLER_SESSION_MISMATCH = "CONTROLLER_SESSION_MISMATCH";
  static final String MANAGEMENT_MODE = "MANAGEMENT_MODE";

  // Response fields written on every path.
  private static final String STATUS = "status";
  private static final String PRIMARY_REASON = "primaryReason";
  private static final String REPORT_STATE = "reportState";

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
      return unavailable(response, Status.PAUSED, Reason.MANAGEMENT_MODE, MANAGEMENT_MODE);
    }
    String problem = problem(report, clusterReport, leader, monitoringEnabled);
    if (problem != null) {
      return unavailable(response, Status.UNKNOWN, Reason.NONE, problem);
    }
    if (resourceName != null && clusterReport.getStatus() == Status.PAUSED) {
      report = pausedView(report, clusterReport);
    }

    response.put(STATUS, report.getStatus().name());
    response.put(PRIMARY_REASON, report.getPrimaryReason().name());
    response.put(REPORT_STATE, FRESH);
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
    summary.put("notConverged", report.getNotConvergedPartitionCount());
    ObjectNode statusCounts = summary.putObject("statusCounts");
    report.getStatusCounts().forEach((status, count) -> statusCounts.put(status.name(), count));
    ObjectNode reasonCounts = summary.putObject("reasonCounts");
    report.getReasonCounts().forEach((reason, count) -> reasonCounts.put(reason.name(), count));
    return response;
  }

  /**
   * Adds the resources a FRESH cluster report lists, worst first, up to {@code limit}: every one
   * that is not converged, or converged on a WAGED target that is not current or healthy.
   * {@code listedResourceCount} is how many the record lists, {@code unlistedResourceCount} how
   * many more it left out to stay small.
   */
  static void listResources(ObjectNode response, ConvergenceStatus report, int limit) {
    if (report == null || !FRESH.equals(response.path("reportState").asText())) {
      return;
    }
    List<String> names = report.getListedResources();
    response.put("listedResourceCount", names.size());
    if (report.getUnlistedResourceCount() > 0) {
      response.put("unlistedResourceCount", report.getUnlistedResourceCount());
    }
    if (limit <= 0) {
      return;
    }
    // The record keeps them by name, so put the worst first again. The sort reads only each
    // entry's status and age; a report is built only for the resources returned.
    names.sort(Comparator.comparingInt((String name) -> severity(report.getResourceStatus(name)))
        .thenComparingLong(report::getResourceUnconvergedSince)
        .thenComparing(name -> name));
    ArrayNode listed = response.putArray("listedResources");
    for (String name : names.subList(0, Math.min(limit, names.size()))) {
      ConvergenceStatus resource = report.getResourceReport(name);
      listed.addObject().put("resourceName", name)
          .put("status", resource.getStatus().name())
          .put("primaryReason", resource.getPrimaryReason().name())
          .put("unconvergedSince", resource.getUnconvergedSince())
          .put("notConvergedPartitions", resource.getNotConvergedPartitionCount());
    }
  }

  private static int severity(Status status) {
    switch (status) {
      case BLOCKED:
        return 0;
      case UNKNOWN:
        return 1;
      case IN_PROGRESS:
        return 2;
      default:
        return 3;
    }
  }

  // The ClusterStatus znode outlives management mode and then reads NORMAL/COMPLETED.
  static boolean isManagementMode(ClusterManagementMode mode) {
    return mode != null && !mode.isFullyInNormalMode();
  }

  private static String problem(ConvergenceStatus report, ConvergenceStatus clusterReport,
      LiveInstance leader, boolean monitoringEnabled) {
    if (!monitoringEnabled) {
      return MONITORING_DISABLED;
    }
    if (clusterReport == null) {
      return REPORT_MISSING;
    }
    if (!clusterReport.isValid()) {
      return SCHEMA_UNSUPPORTED;
    }
    if (leader == null) {
      return CONTROLLER_LEADER_MISSING;
    }
    String session = leader.getEphemeralOwner();
    if (session == null || !session.equals(clusterReport.getControllerSessionId())) {
      return CONTROLLER_SESSION_MISMATCH;
    }
    return report == null ? REPORT_MISSING : null;
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
    response.put(STATUS, status.name());
    response.put(PRIMARY_REASON, reason.name());
    response.put(REPORT_STATE, state);
    return response;
  }
}
