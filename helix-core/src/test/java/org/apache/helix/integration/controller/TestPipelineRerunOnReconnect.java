package org.apache.helix.integration.controller;

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

import org.apache.helix.SystemPropertyKeys;
import org.apache.helix.TestHelper;
import org.apache.helix.ZkUnitTestBase;
import org.apache.helix.integration.manager.ClusterControllerManager;
import org.apache.helix.integration.manager.MockParticipantManager;
import org.apache.helix.tools.ClusterVerifiers.BestPossibleExternalViewVerifier;
import org.apache.helix.zookeeper.zkclient.ZkClient;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher.Event.EventType;
import org.apache.zookeeper.Watcher.Event.KeeperState;
import org.testng.Assert;
import org.testng.annotations.Test;

/**
 * Pipeline events that fail while the controller is disconnected from ZooKeeper are dropped, and a
 * same-session reconnect refires no watch for them. The leader has to rerun the pipeline itself.
 */
public class TestPipelineRerunOnReconnect extends ZkUnitTestBase {
  private static final String DB = "TestDB0";
  private static final String PARTITION = DB + "_0";

  @Test
  public void testLeaderRerunsPipelineAfterSameSessionReconnect() throws Exception {
    String clusterName = TestHelper.getTestClassName() + "_" + TestHelper.getTestMethodName();
    int n = 3;
    TestHelper.setupCluster(clusterName, ZK_ADDR, 12918, "localhost", "TestDB", 1, 1, n, n,
        "MasterSlave", true);
    MockParticipantManager[] participants = new MockParticipantManager[n];
    for (int i = 0; i < n; i++) {
      participants[i] =
          new MockParticipantManager(ZK_ADDR, clusterName, "localhost_" + (12918 + i));
      participants[i].syncStart();
    }
    // Fail the connection check fast, so the event below is dropped well within the wait.
    System.setProperty(SystemPropertyKeys.ZK_WAIT_CONNECTED_TIMEOUT, "500");
    ClusterControllerManager controller =
        new ClusterControllerManager(ZK_ADDR, clusterName, "controller_0");
    System.clearProperty(SystemPropertyKeys.ZK_WAIT_CONNECTED_TIMEOUT);
    controller.syncStart();
    BestPossibleExternalViewVerifier verifier =
        new BestPossibleExternalViewVerifier.Builder(clusterName).setZkClient(_gZkClient)
            .setWaitTillVerify(TestHelper.DEFAULT_REBALANCE_PROCESSING_WAIT_TIME).build();
    try {
      Assert.assertTrue(verifier.verifyByPolling());
      String master = getMaster(clusterName);
      Assert.assertNotNull(master);

      // The controller's client reports a disconnect while the real connection stays up, so the
      // controller still gets the change below but its pipeline fails the connection check.
      ZkClient zkClient = (ZkClient) controller.getZkClient();
      zkClient.process(new WatchedEvent(EventType.None, KeeperState.Disconnected, null));
      _gSetupTool.getClusterManagementTool()
          .enablePartition(false, clusterName, master, DB, Collections.singletonList(PARTITION));
      Thread.sleep(3000);
      Assert.assertEquals(getMaster(clusterName), master);

      // Nothing else changes in the cluster, so only the leader itself can rerun the pipeline.
      zkClient.process(new WatchedEvent(EventType.None, KeeperState.SyncConnected, null));
      Assert.assertTrue(verifier.verifyByPolling(20 * 1000L, 200));
      Assert.assertFalse(master.equals(getMaster(clusterName)));
    } finally {
      controller.syncStop();
      for (MockParticipantManager participant : participants) {
        participant.syncStop();
      }
      deleteCluster(clusterName);
    }
  }

  private String getMaster(String clusterName) {
    Map<String, String> stateMap = _gSetupTool.getClusterManagementTool()
        .getResourceExternalView(clusterName, DB).getStateMap(PARTITION);
    return stateMap.entrySet().stream().filter(e -> "MASTER".equals(e.getValue()))
        .map(Map.Entry::getKey).findFirst().orElse(null);
  }
}
