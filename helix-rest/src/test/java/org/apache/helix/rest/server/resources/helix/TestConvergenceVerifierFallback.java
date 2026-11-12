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
import org.apache.helix.PropertyKey;
import org.apache.helix.TestHelper;
import org.apache.helix.model.ClusterConfig;
import org.apache.helix.model.IdealState;
import org.apache.helix.rest.server.AbstractTestClass;
import org.testng.Assert;
import org.testng.annotations.Test;

public class TestConvergenceVerifierFallback extends AbstractTestClass {
  private static final String CLUSTER = "TestCluster_1";
  private static final String RESOURCE = "VerifierFallbackDB";

  private static ObjectNode response(String reportState) {
    ObjectNode node = JsonNodeFactory.instance.objectNode();
    node.put("reportState", reportState);
    return node;
  }

  @Test
  public void testOnlyAStaleReportThatAsksGetsAFallback() {
    ObjectNode fresh = new ConvergenceVerifierFallback()
        .addTo(response("FRESH"), true, null, new ClusterConfig("c"), "c", null);
    Assert.assertTrue(fresh.path("fallback").isMissingNode());
    ObjectNode notAsked = new ConvergenceVerifierFallback()
        .addTo(response("REPORT_STALE"), false, null, new ClusterConfig("c"), "c", null);
    Assert.assertTrue(notAsked.path("fallback").isMissingNode());
  }

  @Test
  public void testClusterThatDoesNotPersistItsAssignmentCannotBeChecked() {
    ObjectNode stale = new ConvergenceVerifierFallback()
        .addTo(response("REPORT_STALE"), true, null, new ClusterConfig("c"), "c", null);
    Assert.assertEquals(stale.path("fallback").path("result").asText(), "UNAVAILABLE");
    Assert.assertEquals(stale.path("fallback").path("source").asText(), "VERIFIER");
  }

  @Test
  public void testResourceIsAnsweredFromItsClustersCheck() {
    ConvergenceVerifierFallback.Check held =
        new ConvergenceVerifierFallback.Check(true, Collections.singleton("held"));
    Assert.assertEquals(ConvergenceVerifierFallback.answer(held, "ok").path("result").asText(),
        "MATCH");
    Assert.assertEquals(ConvergenceVerifierFallback.answer(held, "held").path("result").asText(),
        "UNAVAILABLE");
    Assert.assertEquals(ConvergenceVerifierFallback.answer(held, null).path("result").asText(),
        "UNAVAILABLE");
    ConvergenceVerifierFallback.Check differs =
        new ConvergenceVerifierFallback.Check(false, Collections.emptySet());
    Assert.assertEquals(ConvergenceVerifierFallback.answer(differs, null).path("result").asText(),
        "MISMATCH");
    // Not checked on its own, so a resource cannot be called a mismatch.
    Assert.assertEquals(ConvergenceVerifierFallback.answer(differs, "ok").path("result").asText(),
        "UNAVAILABLE");
  }

  private String fallbackResult() {
    return new ConvergenceVerifierFallback()
        .addTo(response("REPORT_STALE"), true, _gZkClient,
            _configAccessor.getClusterConfig(CLUSTER), CLUSTER, RESOURCE)
        .path("fallback").path("result").asText();
  }

  @Test
  public void testDisabledNodeKeptInThePreferenceListIsNotAMismatch() throws Exception {
    ClusterConfig config = _configAccessor.getClusterConfig(CLUSTER);
    boolean persist = config.isPersistBestPossibleAssignment();
    boolean delayEnabled = config.isDelayRebalaceEnabled();
    long delay = config.getRebalanceDelayTime();
    config.setPersistBestPossibleAssignment(true);
    config.setDelayRebalaceEnabled(true);
    config.setRebalanceDelayTime(600_000L);
    _configAccessor.setClusterConfig(CLUSTER, config);
    String instance = _gSetupTool.getClusterManagementTool().getInstancesInCluster(CLUSTER).get(0);
    try {
      addResource(CLUSTER, RESOURCE, 4, "MasterSlave", 1, 2);
      Assert.assertTrue(TestHelper.verify(() -> "MATCH".equals(fallbackResult()), 30_000L));
      _gSetupTool.getClusterManagementTool().enableInstance(CLUSTER, instance, false);
      // The delay window keeps the disabled node in the preference list, and its external view
      // entry is OFFLINE.
      Thread.sleep(3_000L);
      Assert.assertTrue(TestHelper.verify(() -> "MATCH".equals(fallbackResult()), 30_000L));
    } finally {
      _gSetupTool.getClusterManagementTool().enableInstance(CLUSTER, instance, true);
      if (_resourcesMap.get(CLUSTER).contains(RESOURCE)) {
        _gSetupTool.dropResourceFromCluster(CLUSTER, RESOURCE);
        _resourcesMap.get(CLUSTER).remove(RESOURCE);
      }
      config.setPersistBestPossibleAssignment(persist);
      config.setDelayRebalaceEnabled(delayEnabled);
      config.setRebalanceDelayTime(delay);
      _configAccessor.setClusterConfig(CLUSTER, config);
    }
  }

  @Test
  public void testReplicaHeldForAnOfflineNodeIsNotReportedAsAMatch() {
    String cluster = "VerifierFallbackDelayed";
    _gSetupTool.addCluster(cluster, true);
    try {
      ClusterConfig config = _configAccessor.getClusterConfig(cluster);
      config.setDelayRebalaceEnabled(true);
      config.setRebalanceDelayTime(600_000L);
      _configAccessor.setClusterConfig(cluster, config);
      _gSetupTool.addResourceToCluster(cluster, RESOURCE, 1, "MasterSlave");
      IdealState idealState =
          _gSetupTool.getClusterManagementTool().getResourceIdealState(cluster, RESOURCE);
      idealState.setRebalanceMode(IdealState.RebalanceMode.FULL_AUTO);
      idealState.setPreferenceList(RESOURCE + "_0", java.util.Arrays.asList("gone_1", "gone_2"));
      _gSetupTool.getClusterManagementTool().setResourceIdealState(cluster, RESOURCE, idealState);

      Assert.assertEquals(ConvergenceVerifierFallback.heldForDelay(_gZkClient, config, cluster),
          Collections.singleton(RESOURCE));

      // A live node is not held back, unless it is disabled.
      _gSetupTool.addInstanceToCluster(cluster, "live_1");
      String livePath = new PropertyKey.Builder(cluster).liveInstance("live_1").getPath();
      _gZkClient.createPersistent(livePath, true);
      idealState.setPreferenceList(RESOURCE + "_0", java.util.Arrays.asList("live_1"));
      _gSetupTool.getClusterManagementTool().setResourceIdealState(cluster, RESOURCE, idealState);
      Assert.assertTrue(ConvergenceVerifierFallback.heldForDelay(_gZkClient, config, cluster)
          .isEmpty());
      _gSetupTool.getClusterManagementTool().enableInstance(cluster, "live_1", false);
      Assert.assertEquals(ConvergenceVerifierFallback.heldForDelay(_gZkClient, config, cluster),
          Collections.singleton(RESOURCE));
      _gZkClient.delete(livePath);
      config.setDelayRebalaceEnabled(false);
      Assert.assertTrue(ConvergenceVerifierFallback.heldForDelay(_gZkClient, config, cluster)
          .isEmpty());
    } finally {
      _gSetupTool.deleteCluster(cluster);
    }
  }
}
