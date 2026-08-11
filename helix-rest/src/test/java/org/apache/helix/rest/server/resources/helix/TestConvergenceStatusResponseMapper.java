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

import java.util.Collections;

import com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.helix.model.ConvergenceStatus;
import org.apache.helix.model.ConvergenceStatus.OptimizerStatus;
import org.apache.helix.model.ConvergenceStatus.PartitionDetail;
import org.apache.helix.model.ConvergenceStatus.Reason;
import org.apache.helix.model.ConvergenceStatus.Scope;
import org.apache.helix.model.ConvergenceStatus.Status;
import org.apache.helix.model.ConvergenceStatus.TargetFreshness;
import org.apache.helix.model.LiveInstance;
import org.testng.Assert;
import org.testng.annotations.Test;

public class TestConvergenceStatusResponseMapper {
  private static final String CLUSTER = "TestCluster";
  private static final String RESOURCE = "TestDB";
  private static final String SESSION = "abc123";

  @Test
  public void testMapResource_freshReport_returnsCustomerFields() {
    ConvergenceStatus status = createReport(SESSION);
    status.setPartitionDetails(Collections.singletonList(
        new PartitionDetail(RESOURCE, "TestDB_0", Status.IN_PROGRESS,
            Reason.PENDING_TRANSITION, Collections.singletonMap("host_0", "SLAVE"),
            Collections.singletonMap("host_0", "MASTER"))),
        ConvergenceStatus.DEFAULT_MAX_PARTITION_DETAILS);
    LiveInstance leader = createLeader(SESSION);

    ObjectNode response =
        ConvergenceStatusResponseMapper.mapResource(CLUSTER, RESOURCE, status, leader);

    Assert.assertEquals(response.path("status").asText(), Status.IN_PROGRESS.name());
    Assert.assertFalse(response.path("stale").asBoolean());
    Assert.assertEquals(response.path("targetFreshness").asText(),
        TargetFreshness.CURRENT.name());
    Assert.assertEquals(response.path("optimizerStatus").asText(),
        OptimizerStatus.HEALTHY.name());
    Assert.assertEquals(
        response.path("affectedPartitions").get(0).path("currentAssignment").path("host_0")
            .asText(), "SLAVE");
    Assert.assertEquals(
        response.path("affectedPartitions").get(0).path("expectedAssignment").path("host_0")
            .asText(), "MASTER");
  }

  @Test
  public void testMapResource_controllerSessionChanged_returnsStaleUnknown() {
    ConvergenceStatus status = createReport(SESSION);
    LiveInstance leader = createLeader("different_session");

    ObjectNode response =
        ConvergenceStatusResponseMapper.mapResource(CLUSTER, RESOURCE, status, leader);

    Assert.assertEquals(response.path("status").asText(), Status.UNKNOWN.name());
    Assert.assertEquals(response.path("reportedStatus").asText(), Status.IN_PROGRESS.name());
    Assert.assertTrue(response.path("stale").asBoolean());
    Assert.assertEquals(response.path("staleReason").asText(),
        "CONTROLLER_SESSION_MISMATCH");
  }

  @Test
  public void testMapCluster_monitoringDisabled_returnsExplicitUnknown() {
    ConvergenceStatus status = createReport(SESSION);
    status.setScope(Scope.CLUSTER);

    ObjectNode response = ConvergenceStatusResponseMapper
        .mapCluster(CLUSTER, status, createLeader(SESSION), false);

    Assert.assertEquals(response.path("status").asText(), Status.UNKNOWN.name());
    Assert.assertTrue(response.path("stale").asBoolean());
    Assert.assertEquals(response.path("staleReason").asText(), "MONITORING_DISABLED");
  }

  private ConvergenceStatus createReport(String session) {
    ConvergenceStatus status = new ConvergenceStatus(RESOURCE);
    status.setScope(Scope.RESOURCE);
    status.setResourceName(RESOURCE);
    status.setStatus(Status.IN_PROGRESS);
    status.setPrimaryReason(Reason.PENDING_TRANSITION);
    status.setTargetFreshness(TargetFreshness.CURRENT);
    status.setOptimizerStatus(OptimizerStatus.HEALTHY);
    status.setControllerSessionId(session);
    status.setGeneratedAt(System.currentTimeMillis());
    status.setTotalPartitionCount(1);
    status.setInProgressPartitionCount(1);
    status.setAffectedPartitionCount(1);
    return status;
  }

  private LiveInstance createLeader(String session) {
    LiveInstance leader = new LiveInstance("controller");
    leader.setSessionId(session);
    return leader;
  }
}
