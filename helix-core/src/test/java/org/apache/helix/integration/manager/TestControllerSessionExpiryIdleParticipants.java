package org.apache.helix.integration.manager;

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

import org.apache.helix.SystemPropertyKeys;
import org.apache.helix.TestHelper;
import org.apache.helix.ZkTestHelper;
import org.apache.helix.common.ZkTestBase;
import org.apache.helix.tools.ClusterVerifiers.BestPossibleExternalViewVerifier;
import org.apache.helix.tools.ClusterVerifiers.ZkHelixClusterVerifier;
import org.testng.Assert;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

/**
 * When the controller's ZK session expires, ZkClient re-fires a child-change event to every listener
 * of the old session. For an idle participant there is no CURRENTSTATES/<session> znode yet, so the
 * old, already reset handler sees null children and calls removeListener(path, listener), which used
 * to remove the new handler for the same path. The controller then never saw the participants'
 * current-state changes.
 */
public class TestControllerSessionExpiryIdleParticipants extends ZkTestBase {
  private static final String PARALLEL_REGISTRATION =
      SystemPropertyKeys.CONTROLLER_PARALLEL_INSTANCE_LISTENER_REGISTRATION_ENABLED;

  @DataProvider(name = "parallelRegistration")
  public static Object[][] parallelRegistration() {
    return new Object[][] {{true}, {false}};
  }

  @Test(dataProvider = "parallelRegistration")
  public void testCurrentStateWatchesSurviveControllerSessionExpiry(boolean parallel)
      throws Exception {
    String clusterName = TestHelper.getTestClassName() + "_" + parallel;
    String prevFlag = System.getProperty(PARALLEL_REGISTRATION);
    System.setProperty(PARALLEL_REGISTRATION, String.valueOf(parallel));
    int n = 3;
    MockParticipantManager[] participants = new MockParticipantManager[n];
    ClusterControllerManager controller = null;
    try {
      // No resources, so the participants never create a CURRENTSTATES/<session> znode.
      TestHelper.setupCluster(clusterName, ZK_ADDR, 12918, "localhost", "TestDB", 0, 1, n, n,
          "MasterSlave", true);
      for (int i = 0; i < n; i++) {
        participants[i] =
            new MockParticipantManager(ZK_ADDR, clusterName, "localhost_" + (12918 + i));
        participants[i].syncStart();
      }
      controller = new ClusterControllerManager(ZK_ADDR, clusterName, "controller_0");
      controller.syncStart();
      ClusterControllerManager c = controller;
      Assert.assertTrue(TestHelper.verify(
          () -> PerInstanceListenerTestSupport.countCurrentStateHandlers(c, clusterName) == n,
          TestHelper.WAIT_DURATION));

      ZkTestHelper.expireSession(controller.getZkClient());
      Assert.assertTrue(TestHelper.verify(c::isLeader, TestHelper.WAIT_DURATION));
      // The stale events run right after the new session is handled; give them time to land.
      Thread.sleep(3000);
      Assert.assertEquals(PerInstanceListenerTestSupport.countCurrentStateHandlers(c, clusterName),
          n, "CURRENTSTATES watches lost after controller session expiry");

      _gSetupTool.addResourceToCluster(clusterName, "TestDB0", 1, "MasterSlave");
      _gSetupTool.rebalanceStorageCluster(clusterName, "TestDB0", n);
      ZkHelixClusterVerifier verifier =
          new BestPossibleExternalViewVerifier.Builder(clusterName).setZkClient(_gZkClient)
              .setWaitTillVerify(TestHelper.DEFAULT_REBALANCE_PROCESSING_WAIT_TIME).build();
      Assert.assertTrue(verifier.verifyByPolling(30000, 500),
          "external view did not converge after controller session expiry");
    } finally {
      if (controller != null) {
        controller.syncStop();
      }
      for (MockParticipantManager p : participants) {
        if (p != null) {
          p.syncStop();
        }
      }
      deleteCluster(clusterName);
      if (prevFlag == null) {
        System.clearProperty(PARALLEL_REGISTRATION);
      } else {
        System.setProperty(PARALLEL_REGISTRATION, prevFlag);
      }
    }
  }
}
