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

import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import javax.ws.rs.core.Response;

import com.fasterxml.jackson.databind.JsonNode;
import org.apache.helix.NotificationContext;
import org.apache.helix.TestHelper;
import org.apache.helix.integration.manager.MockParticipantManager;
import org.apache.helix.mock.participant.MockTransition;
import org.apache.helix.model.ClusterConfig;
import org.apache.helix.model.Message;
import org.apache.helix.tools.ClusterVerifiers.BestPossibleExternalViewVerifier;
import org.testng.Assert;
import org.testng.annotations.Test;

public class TestConvergenceStatusAccessor extends AbstractTestClass {
  private static final String CLUSTER = "TestCluster_0";
  private static final String RESOURCE = "ConvergenceStatusTestDB";
  private static final long TEST_TIMEOUT_MS = 30_000L;

  @Test
  public void testConvergenceEndpoint_transitionInFlight_reportsProgressThenConverges()
      throws Exception {
    List<MockParticipantManager> participants = _mockParticipantManagers.stream()
        .filter(manager -> CLUSTER.equals(manager.getClusterName())).collect(Collectors.toList());
    Assert.assertFalse(participants.isEmpty());
    BlockingTransition transition = new BlockingTransition();
    participants.forEach(participant -> participant.setTransition(transition));
    ClusterConfig clusterConfig = _configAccessor.getClusterConfig(CLUSTER);
    boolean monitoringPreviouslyEnabled = clusterConfig.isConvergenceMonitoringEnabled();
    clusterConfig.setConvergenceMonitoringEnabled(true);
    _configAccessor.setClusterConfig(CLUSTER, clusterConfig);

    BestPossibleExternalViewVerifier verifier = null;
    try {
      addResource(CLUSTER, RESOURCE, 2, "MasterSlave", 1, 2);
      verifier = new BestPossibleExternalViewVerifier.Builder(CLUSTER).setZkAddr(ZK_ADDR)
          .setResources(java.util.Collections.singleton(RESOURCE)).build();

      Assert.assertTrue(transition.awaitStarted(TEST_TIMEOUT_MS));
      Assert.assertTrue(TestHelper.verify(
          () -> "IN_PROGRESS".equals(readStatus(resourcePath())), TEST_TIMEOUT_MS));
      Assert.assertTrue(TestHelper.verify(
          () -> "IN_PROGRESS".equals(readStatus(clusterPath())), TEST_TIMEOUT_MS));
      String inProgressResponse = readResponse(resourcePath());
      Assert.assertTrue(
          OBJECT_MAPPER.readTree(inProgressResponse).path("partitionSummary").path("affected")
              .asInt() > 0);
      System.out.println("CONVERGENCE_IN_PROGRESS=" + inProgressResponse);

      transition.release();
      Assert.assertTrue(verifier.verifyByPolling());
      Assert.assertTrue(TestHelper.verify(
          () -> "CONVERGED".equals(readStatus(resourcePath())), TEST_TIMEOUT_MS));
      Assert.assertTrue(TestHelper.verify(
          () -> "CONVERGED".equals(readStatus(clusterPath())), TEST_TIMEOUT_MS));
      String convergedResponse = readResponse(resourcePath());
      Assert.assertEquals(
          OBJECT_MAPPER.readTree(convergedResponse).path("partitionSummary").path("affected")
              .asInt(), 0);
      System.out.println("CONVERGENCE_CONVERGED=" + convergedResponse);

      _gSetupTool.getClusterManagementTool().enableCluster(CLUSTER, false);
      Assert.assertTrue(TestHelper.verify(
          () -> "PAUSED".equals(readStatus(clusterPath())), TEST_TIMEOUT_MS));
      _gSetupTool.getClusterManagementTool().enableCluster(CLUSTER, true);
      Assert.assertTrue(TestHelper.verify(
          () -> "CONVERGED".equals(readStatus(clusterPath())), TEST_TIMEOUT_MS));
    } finally {
      transition.release();
      participants.forEach(participant -> participant.setTransition(new MockTransition()));
      if (verifier != null) {
        verifier.close();
      }
      if (_resourcesMap.get(CLUSTER).contains(RESOURCE)) {
        _gSetupTool.dropResourceFromCluster(CLUSTER, RESOURCE);
        _resourcesMap.get(CLUSTER).remove(RESOURCE);
      }
      clusterConfig.setConvergenceMonitoringEnabled(monitoringPreviouslyEnabled);
      _configAccessor.setClusterConfig(CLUSTER, clusterConfig);
    }
  }

  private String readStatus(String path) {
    String body = readResponse(path);
    try {
      JsonNode response = OBJECT_MAPPER.readTree(body);
      return response.path("status").asText();
    } catch (Exception e) {
      return "";
    }
  }

  private String readResponse(String path) {
    return get(path, null, Response.Status.OK.getStatusCode(), true);
  }

  private String clusterPath() {
    return "clusters/" + CLUSTER + "/convergence";
  }

  private String resourcePath() {
    return "clusters/" + CLUSTER + "/resources/" + RESOURCE + "/convergence";
  }

  private static final class BlockingTransition extends MockTransition {
    private final CountDownLatch _started = new CountDownLatch(1);
    private final CountDownLatch _release = new CountDownLatch(1);

    @Override
    public void doTransition(Message message, NotificationContext context)
        throws InterruptedException {
      _started.countDown();
      _release.await(TEST_TIMEOUT_MS, TimeUnit.MILLISECONDS);
    }

    private boolean awaitStarted(long timeoutMs) throws InterruptedException {
      return _started.await(timeoutMs, TimeUnit.MILLISECONDS);
    }

    private void release() {
      _release.countDown();
    }
  }
}
