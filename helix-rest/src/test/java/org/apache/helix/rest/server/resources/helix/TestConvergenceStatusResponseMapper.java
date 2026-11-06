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

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.helix.api.status.ClusterManagementMode;
import org.apache.helix.model.ConvergenceStatus;
import org.apache.helix.model.ConvergenceStatus.Reason;
import org.apache.helix.model.ConvergenceStatus.Status;
import org.apache.helix.model.LiveInstance;
import org.testng.Assert;
import org.testng.annotations.Test;

public class TestConvergenceStatusResponseMapper {
  private static final String SESSION = "abc123";

  private static ConvergenceStatus report(String session) {
    ConvergenceStatus report = new ConvergenceStatus("db");
    report.setStatus(Status.IN_PROGRESS);
    report.setPrimaryReason(Reason.PENDING_TRANSITION);
    report.setControllerSessionId(session);
    report.setControllerName("controller_1");
    report.setStatusCounts(Collections.singletonMap(Status.IN_PROGRESS, 1));
    report.setReasonCounts(Collections.singletonMap(Reason.PENDING_TRANSITION, 1));
    return report;
  }

  private static final long NOW = 10_000_000L;

  private static LiveInstance leader(String session) {
    LiveInstance leader = new LiveInstance("controller");
    leader.setSessionId(session);
    return leader;
  }

  @Test
  public void testFreshReportIsReturnedAsASummary() {
    ObjectNode response = ConvergenceStatusResponseMapper.map("c", "db", report(SESSION),
        report(SESSION), leader(SESSION), true, false, NOW);
    Assert.assertEquals(response.path("status").asText(), "IN_PROGRESS");
    Assert.assertEquals(response.path("reportState").asText(), "FRESH");
    Assert.assertEquals(response.path("publishedBy").asText(), "controller_1");
    Assert.assertEquals(response.path("controllerSessionId").asText(), SESSION);
    Assert.assertEquals(response.path("partitionSummary").path("affected").asInt(), 1);
    Assert.assertEquals(
        response.path("partitionSummary").path("reasonCounts").path("PENDING_TRANSITION").asInt(),
        1);
    Assert.assertTrue(response.path("partitions").isMissingNode());
  }

  @Test
  public void testTrustComesFromTheClusterRecord() {
    // A resource report an earlier leader wrote is trusted once the cluster record is current, and
    // is attributed to the controller that vouches for it now.
    ConvergenceStatus anchor = report(SESSION);
    anchor.setControllerName("controller_2");
    ObjectNode adopted = ConvergenceStatusResponseMapper.map("c", "db", report("old"), anchor,
        leader(SESSION), true, false, NOW);
    Assert.assertEquals(adopted.path("reportState").asText(), "FRESH");
    Assert.assertEquals(adopted.path("publishedBy").asText(), "controller_2");
    Assert.assertEquals(adopted.path("controllerSessionId").asText(), SESSION);
    ObjectNode stale = ConvergenceStatusResponseMapper.map("c", "db", report(SESSION),
        report("old"), leader(SESSION), true, false, NOW);
    Assert.assertEquals(stale.path("status").asText(), "UNKNOWN");
    Assert.assertEquals(stale.path("reportState").asText(), "CONTROLLER_SESSION_MISMATCH");
    Assert.assertTrue(stale.path("partitionSummary").isMissingNode());
  }

  @Test
  public void testReportFromAStuckPipelineIsStale() {
    ConvergenceStatus cluster = report(SESSION);
    cluster.setEvaluatedAt(NOW - ConvergenceStatusResponseMapper.STALE_AFTER_MS - 1);
    ObjectNode stale = ConvergenceStatusResponseMapper.map("c", "db", report(SESSION), cluster,
        leader(SESSION), true, false, NOW);
    Assert.assertEquals(stale.path("status").asText(), "UNKNOWN");
    Assert.assertEquals(stale.path("reportState").asText(), "REPORT_STALE");
    cluster.setEvaluatedAt(NOW - ConvergenceStatusResponseMapper.STALE_AFTER_MS);
    Assert.assertEquals(ConvergenceStatusResponseMapper.map("c", "db", report(SESSION), cluster,
        leader(SESSION), true, false, NOW).path("reportState").asText(), "FRESH");
  }

  @Test
  public void testPausedClusterRecordPausesEveryResource() {
    ConvergenceStatus cluster = report(SESSION);
    cluster.setStatus(Status.PAUSED);
    cluster.setPrimaryReason(Reason.MAINTENANCE_MODE);
    cluster.setGeneratedAt(42L);
    ObjectNode response = ConvergenceStatusResponseMapper.map("c", "db", report("old"), cluster,
        leader(SESSION), true, false, NOW);
    Assert.assertEquals(response.path("status").asText(), "PAUSED");
    Assert.assertEquals(response.path("primaryReason").asText(), "MAINTENANCE_MODE");
    Assert.assertEquals(response.path("reportState").asText(), "FRESH");
    Assert.assertEquals(response.path("generatedAt").asLong(), 42L);
    Assert.assertEquals(response.path("controllerSessionId").asText(), SESSION);
    JsonNode summary = response.path("partitionSummary");
    Assert.assertEquals(summary.path("total").asInt(), 1);
    Assert.assertEquals(summary.path("affected").asInt(), 0);
    Assert.assertEquals(summary.path("statusCounts").path("PAUSED").asInt(), 1);
    Assert.assertEquals(summary.path("reasonCounts").path("MAINTENANCE_MODE").asInt(), 1);
  }

  @Test
  public void testNothingToTrust() {
    String[] states = {
        ConvergenceStatusResponseMapper.map("c", null, report(SESSION), report(SESSION),
            leader(SESSION), false, false, NOW).path("reportState").asText(),
        ConvergenceStatusResponseMapper.map("c", null, null, null, leader(SESSION), true, false, NOW)
            .path("reportState").asText(),
        ConvergenceStatusResponseMapper.map("c", "db", null, report(SESSION), leader(SESSION),
            true, false, NOW).path("reportState").asText(),
        ConvergenceStatusResponseMapper.map("c", null, report(SESSION), report(SESSION), null,
            true, false, NOW).path("reportState").asText()};
    Assert.assertEquals(states, new String[] {"MONITORING_DISABLED", "REPORT_MISSING",
        "REPORT_MISSING", "CONTROLLER_LEADER_MISSING"});
  }

  @Test
  public void testManagementModeIsPausedWhateverTheReportSays() {
    ObjectNode response = ConvergenceStatusResponseMapper.map("c", null, report(SESSION),
        report(SESSION), leader(SESSION), true, true, NOW);
    Assert.assertEquals(response.path("status").asText(), "PAUSED");
    Assert.assertEquals(response.path("primaryReason").asText(), "MANAGEMENT_MODE");
  }

  @Test
  public void testLeftOverNormalClusterStatusIsNotManagementMode() {
    Assert.assertFalse(ConvergenceStatusResponseMapper.isManagementMode(null));
    Assert.assertFalse(ConvergenceStatusResponseMapper.isManagementMode(
        new ClusterManagementMode(ClusterManagementMode.Type.NORMAL,
            ClusterManagementMode.Status.COMPLETED)));
    Assert.assertTrue(ConvergenceStatusResponseMapper.isManagementMode(
        new ClusterManagementMode(ClusterManagementMode.Type.CLUSTER_FREEZE,
            ClusterManagementMode.Status.COMPLETED)));
  }
}
