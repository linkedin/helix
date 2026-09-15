package org.apache.helix.rest.server;

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

import java.io.IOException;
import java.util.HashSet;
import java.util.Set;
import javax.ws.rs.client.WebTarget;
import javax.ws.rs.core.Response;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.helix.AccessOption;
import org.apache.helix.HelixDataAccessor;
import org.apache.helix.PropertyKey;
import org.apache.helix.TestHelper;
import org.apache.helix.manager.zk.ZKHelixDataAccessor;
import org.apache.helix.model.ParticipantHistory;
import org.apache.zookeeper.data.Stat;
import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

/**
 * Covers the {@code getConvergenceStatus} command end to end against a running REST server.
 *
 * <p>Two clusters are used so both answers are deterministic: one has a controller and live
 * participants and therefore reaches convergence, the other has neither and therefore never
 * publishes an external view.
 */
public class TestClusterConvergenceStatus extends AbstractTestClass {
  private static final String CONVERGED_CLUSTER = "TestConvergenceStatusCluster";
  private static final String PENDING_CLUSTER = "TestPendingConvergenceStatusCluster";
  private static final String PENDING_RESOURCE = PENDING_CLUSTER + "_db_0";
  private static final String PENDING_INSTANCE = PENDING_CLUSTER + "localhost_12918";
  private static final String COMMAND = "getConvergenceStatus";
  private static final long CONVERGENCE_TIMEOUT_MS = 60_000L;

  private final ObjectMapper _objectMapper = new ObjectMapper();

  @BeforeClass
  public void beforeClass() {
    _gSetupTool.addCluster(CONVERGED_CLUSTER, true);
    Set<String> instances = new HashSet<>();
    for (int i = 0; i < 3; i++) {
      String instance = CONVERGED_CLUSTER + "localhost_" + (13918 + i);
      _gSetupTool.addInstanceToCluster(CONVERGED_CLUSTER, instance);
      instances.add(instance);
    }
    addResource(CONVERGED_CLUSTER, CONVERGED_CLUSTER + "_db_0", 4, "MasterSlave", 1, 3);
    startInstances(CONVERGED_CLUSTER, instances, 3);
    startController(CONVERGED_CLUSTER);

    // No controller and no participants, so this resource can never publish an external view.
    _gSetupTool.addCluster(PENDING_CLUSTER, true);
    _gSetupTool.addInstanceToCluster(PENDING_CLUSTER, PENDING_INSTANCE);
    addResource(PENDING_CLUSTER, PENDING_RESOURCE, 2, "MasterSlave", 1, 1);
  }

  @Test
  public void testConvergedClusterIsReportedConverged() throws Exception {
    Assert.assertTrue(TestHelper.verify(() -> {
      JsonNode status = readStatus(CONVERGED_CLUSTER, null, "LENIENT", 200);
      return "CONVERGED".equals(status.get("status").textValue());
    }, CONVERGENCE_TIMEOUT_MS), "Cluster with a controller and live participants did not converge");

    JsonNode status = readStatus(CONVERGED_CLUSTER, null, "LENIENT", 200);
    Assert.assertEquals(status.get("id").textValue(), CONVERGED_CLUSTER);
    Assert.assertEquals(status.get("scope").textValue(), "CLUSTER");
    Assert.assertEquals(status.get("matchMode").textValue(), "LENIENT");
    Assert.assertTrue(status.get("evaluatedResourceCount").intValue() >= 1);
    Assert.assertEquals(status.get("pendingResourceCount").intValue(), 0);
    Assert.assertEquals(status.get("failedResourceCount").intValue(), 0);
    Assert.assertEquals(status.get("unknownResourceCount").intValue(), 0);
    Assert.assertEquals(status.get("pendingResources").size(), 0);
    Assert.assertFalse(status.get("detailTruncated").booleanValue());
  }

  @Test
  public void testMissingExternalViewIsPendingNotConverged() throws IOException {
    JsonNode status = readStatus(PENDING_CLUSTER, null, null, 200);

    Assert.assertEquals(status.get("status").textValue(), "PENDING");
    Assert.assertEquals(status.get("matchMode").textValue(), "STRICT");
    Assert.assertEquals(status.get("pendingResourceCount").intValue(), 1);
    Assert.assertEquals(status.get("pendingResources").get(PENDING_RESOURCE).textValue(),
        "EXTERNAL_VIEW_MISSING");
    Assert.assertEquals(status.get("evaluatedResourceCount").intValue(), 1);
  }

  @Test
  public void testResourceScopeIsReportedAndHonored() throws IOException {
    JsonNode scoped = readStatus(PENDING_CLUSTER, PENDING_RESOURCE, "LENIENT", 200);
    Assert.assertEquals(scoped.get("scope").textValue(), "RESOURCES");
    Assert.assertEquals(scoped.get("status").textValue(), "PENDING");
    Assert.assertEquals(scoped.get("evaluatedResourceCount").intValue(), 1);
  }

  @Test
  public void testUnknownResourceIsReportedAsFailedNotConverged() throws IOException {
    JsonNode status = readStatus(PENDING_CLUSTER, "noSuchResource", "LENIENT", 200);

    // Nothing was observed for that resource, so the answer must not look like convergence.
    Assert.assertEquals(status.get("status").textValue(), "FAILED");
    Assert.assertEquals(status.get("unknownResourceCount").intValue(), 1);
    Assert.assertEquals(status.get("unknownResources").get(0).textValue(), "noSuchResource");
    Assert.assertEquals(status.get("evaluatedResourceCount").intValue(), 0);
  }

  @Test
  public void testInvalidParametersAreRejected() {
    Assert.assertEquals(request(PENDING_CLUSTER, null, "eventually").getStatus(), 400);
    Assert.assertEquals(request(PENDING_CLUSTER, ",", "LENIENT").getStatus(), 400);
  }

  @Test
  public void testOtherCommandValuesStillReturnClusterInfo() throws IOException {
    // This read has always ignored a command it does not act on, so the status read is selected
    // only by the exact command name and a caller cannot get convergence from a misspelling.
    for (String command : new String[] {"enableMaintenanceMode", "getConvergenceStatuss"}) {
      WebTarget target =
          target("clusters/" + PENDING_CLUSTER).queryParam("command", command);
      try (Response response = target.request().get()) {
        Assert.assertEquals(response.getStatus(), 200);
        JsonNode body = _objectMapper.readTree(response.readEntity(String.class));
        Assert.assertNull(body.get("status"));
        Assert.assertNotNull(body.get("liveInstances"));
      }
    }
  }

  @Test
  public void testMissingClusterIsNotFound() {
    Assert.assertEquals(request("noSuchClusterForConvergence", null, "LENIENT").getStatus(), 404);
  }

  @Test
  public void testResponseIsNotCacheableAndObservationTimeAdvances() throws Exception {
    long before = System.currentTimeMillis();
    JsonNode first;
    try (Response response = request(PENDING_CLUSTER, null, "LENIENT")) {
      Assert.assertEquals(response.getStatus(), 200);
      Assert.assertEquals(response.getHeaderString("Cache-Control"), "no-store");
      first = _objectMapper.readTree(response.readEntity(String.class));
    }
    Thread.sleep(5);
    JsonNode second = readStatus(PENDING_CLUSTER, null, "LENIENT", 200);
    long after = System.currentTimeMillis();

    Assert.assertTrue(first.get("observedAtMillis").longValue() >= before);
    Assert.assertTrue(
        second.get("observedAtMillis").longValue() > first.get("observedAtMillis").longValue());
    Assert.assertTrue(second.get("observedAtMillis").longValue() <= after);
  }

  @Test
  public void testStatusReadDoesNotWriteClusterMetadata() throws IOException {
    HelixDataAccessor accessor = new ZKHelixDataAccessor(PENDING_CLUSTER, _baseAccessor);
    PropertyKey historyKey = accessor.keyBuilder().participantHistory(PENDING_INSTANCE);
    ParticipantHistory history = new ParticipantHistory(PENDING_INSTANCE);
    Assert.assertTrue(accessor.setProperty(historyKey, history));
    PropertyKey idealStateKey = accessor.keyBuilder().idealStates(PENDING_RESOURCE);
    int historyVersion = statVersion(accessor, historyKey);
    int idealStateVersion = statVersion(accessor, idealStateKey);

    readStatus(PENDING_CLUSTER, null, "LENIENT", 200);

    // Observing convergence must not record an offline time or provoke a rebalance.
    Assert.assertEquals(statVersion(accessor, historyKey), historyVersion);
    Assert.assertEquals(statVersion(accessor, idealStateKey), idealStateVersion);
    Assert.assertEquals(
        ((ParticipantHistory) accessor.getProperty(historyKey)).getLastOfflineTime(),
        ParticipantHistory.ONLINE);
  }

  @Test
  public void testRepeatedReadsLeaveNoVerifierOrConnectionBehind() throws IOException {
    int zkClientThreadsBefore = countThreads("ZkClient-EventThread");

    for (int i = 0; i < 10; i++) {
      readStatus(PENDING_CLUSTER, null, "LENIENT", 200);
    }

    // A verifier would keep a dedicated connection and its threads alive per call.
    Assert.assertEquals(countThreads("ZkHelixClusterVerifier"), 0);
    Assert.assertTrue(countThreads("ZkClient-EventThread") - zkClientThreadsBefore <= 2,
        "Repeated status reads must not accumulate ZooKeeper connections");
  }

  private static int countThreads(String namePart) {
    int count = 0;
    for (Thread thread : Thread.getAllStackTraces().keySet()) {
      if (thread.getName() != null && thread.getName().contains(namePart)) {
        count++;
      }
    }
    return count;
  }

  private static int statVersion(HelixDataAccessor accessor, PropertyKey key) {
    Stat stat = accessor.getBaseDataAccessor().getStat(key.getPath(), AccessOption.PERSISTENT);
    Assert.assertNotNull(stat, "expected " + key.getPath() + " to exist");
    return stat.getVersion();
  }

  private JsonNode readStatus(String cluster, String resources, String matchMode,
      int expectedStatus) throws IOException {
    try (Response response = request(cluster, resources, matchMode)) {
      Assert.assertEquals(response.getStatus(), expectedStatus);
      return _objectMapper.readTree(response.readEntity(String.class));
    }
  }

  private Response request(String cluster, String resources, String matchMode) {
    WebTarget target = target("clusters/" + cluster).queryParam("command", COMMAND);
    if (resources != null) {
      target = target.queryParam("resources", resources);
    }
    if (matchMode != null) {
      target = target.queryParam("matchMode", matchMode);
    }
    return target.request().get();
  }
}
