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

import java.util.Date;
import java.util.List;

import org.apache.helix.HelixManager;
import org.apache.helix.InstanceType;
import org.apache.helix.PropertyKey;
import org.apache.helix.TestHelper;
import org.apache.helix.common.ZkTestBase;
import org.apache.helix.manager.zk.CallbackHandler;
import org.apache.helix.manager.zk.ZKHelixDataAccessor;
import org.apache.helix.manager.zk.ZKHelixManager;
import org.apache.helix.manager.zk.ZkBaseDataAccessor;
import org.apache.helix.mock.participant.MockMSModelFactory;
import org.apache.helix.model.LiveInstance;
import org.apache.helix.tools.ClusterVerifiers.BestPossibleExternalViewVerifier;
import org.apache.helix.tools.ClusterVerifiers.ZkHelixClusterVerifier;
import org.testng.Assert;
import org.testng.annotations.Test;

/**
 * Focused validation for CICP-34606: when a standby distributed controller is promoted on an
 * existing ZK session (failover via CALLBACK, not a new session), the per-instance listeners that
 * {@code checkLiveInstancesObservation} defers during INIT must still be registered. Otherwise the
 * new leader sets no {@code /INSTANCES/<inst>/CURRENTSTATES} watches, never observes replies, and
 * MissingTopState stays up.
 */
public class TestFailoverPerInstanceListenerRegistration extends ZkTestBase {

  @SuppressWarnings("unchecked")
  private int countCurrentStateHandlers(HelixManager mgr, String clusterName) throws Exception {
    java.lang.reflect.Field f = ZKHelixManager.class.getDeclaredField("_handlers");
    f.setAccessible(true);
    List<CallbackHandler> handlers = (List<CallbackHandler>) f.get(mgr);
    int count = 0;
    synchronized (mgr) {
      for (CallbackHandler h : new java.util.ArrayList<>(handlers)) {
        String p = h.getPath();
        // per-instance current-state watch path: /<cluster>/INSTANCES/<inst>/CURRENTSTATES...
        if (p.contains("/" + clusterName + "/INSTANCES/") && p.contains("/CURRENTSTATES")) {
          count++;
        }
      }
    }
    return count;
  }

  private HelixManager currentLeader(HelixManager[] controllers, String clusterName)
      throws Exception {
    ZKHelixDataAccessor accessor =
        new ZKHelixDataAccessor(clusterName, new ZkBaseDataAccessor<>(_gZkClient));
    PropertyKey.Builder kb = accessor.keyBuilder();
    LiveInstance leader = accessor.getProperty(kb.controllerLeader());
    Assert.assertNotNull(leader, "no controller leader");
    for (HelixManager c : controllers) {
      if (c.getInstanceName().equals(leader.getId())) {
        return c;
      }
    }
    Assert.fail("leader " + leader.getId() + " not among controllers");
    return null;
  }

  @Test
  public void failoverRegistersPerInstanceWatchesAndObservesState() throws Exception {
    String clusterName = TestHelper.getTestClassName() + "_" + TestHelper.getTestMethodName();
    int n = 4; // nodes, each a CONTROLLER_PARTICIPANT (participant + controller candidate)
    System.out.println("START " + clusterName + " at " + new Date(System.currentTimeMillis()));

    // Multiple resources so per-instance CURRENTSTATES fan-out is non-trivial.
    TestHelper.setupCluster(clusterName, ZK_ADDR, 12918, "localhost", "TestDB",
        5, // resources
        8, // partitions per resource
        n, 2, "MasterSlave", true);

    HelixManager[] controllers = new HelixManager[n];
    for (int i = 0; i < n; i++) {
      controllers[i] = new ZKHelixManager(clusterName, "localhost_" + (12918 + i),
          InstanceType.CONTROLLER_PARTICIPANT, ZK_ADDR);
      controllers[i].getStateMachineEngine().registerStateModelFactory("MasterSlave",
          new MockMSModelFactory());
      controllers[i].connect();
    }

    BestPossibleExternalViewVerifier verifier =
        new BestPossibleExternalViewVerifier.Builder(clusterName).setZkAddress(ZK_ADDR).build();
    Assert.assertTrue(verifier.verifyByZkCallback(30000), "initial convergence failed");

    // Repeated failover (churn) — the KSAP scenario this PR targets.
    for (int round = 0; round < 3; round++) {
      HelixManager leader = currentLeader(controllers, clusterName);
      System.out.println("Round " + round + ": killing leader " + leader.getInstanceName());
      leader.disconnect();

      // A new leader must take over on its EXISTING session (CALLBACK failover path).
      Assert.assertTrue(verifier.verifyByZkCallback(30000),
          "convergence failed after failover round " + round);

      HelixManager newLeader = currentLeader(controllers, clusterName);
      Assert.assertFalse(newLeader.getInstanceName().equals(leader.getInstanceName()),
          "leadership did not move on round " + round);

      // Core assertion: the new leader actually registered per-instance CURRENTSTATES watches.
      // Registration is async; poll briefly. Without the fix this stays 0 forever.
      final HelixManager nl = newLeader;
      boolean registered = TestHelper.verify(
          () -> countCurrentStateHandlers(nl, clusterName) > 0, 15000);
      int csHandlers = countCurrentStateHandlers(newLeader, clusterName);
      Assert.assertTrue(registered,
          "new leader " + newLeader.getInstanceName()
              + " registered NO per-instance CURRENTSTATES handlers after failover round " + round
              + " (had " + csHandlers + ") — MissingTopState would never clear");
      System.out.println("Round " + round + ": new leader " + newLeader.getInstanceName()
          + " has " + csHandlers + " per-instance CURRENTSTATES handlers");

      // Prove the watches are FUNCTIONAL: bounce a participant, forcing current-state changes the
      // new leader must observe to rebuild the external view.
      // (verifier re-convergence below exercises exactly that path.)
      Assert.assertTrue(verifier.verifyByZkCallback(30000),
          "post-failover state observation failed on round " + round);
    }

    for (HelixManager c : controllers) {
      if (c.isConnected()) {
        c.disconnect();
      }
    }
    deleteCluster(clusterName);
    System.out.println("END " + clusterName + " at " + new Date(System.currentTimeMillis()));
  }

  /**
   * STANDALONE variant (InstanceType.CONTROLLER, separate participants). Pratyush noted the bug
   * also hits STANDALONE controllers on every failover. Two standalone controllers compete for
   * leadership; participants host the resources. Kill the leader and assert the new standalone
   * leader registers per-instance CURRENTSTATES watches over the existing session (CALLBACK path).
   */
  @Test
  public void standaloneFailoverRegistersPerInstanceWatches() throws Exception {
    String clusterName = TestHelper.getTestClassName() + "_" + TestHelper.getTestMethodName();
    int nParticipants = 3;
    System.out.println("START " + clusterName + " at " + new Date(System.currentTimeMillis()));

    TestHelper.setupCluster(clusterName, ZK_ADDR, 12918, "localhost", "TestDB",
        5, // resources
        8, // partitions per resource
        nParticipants, 2, "MasterSlave", true);

    MockParticipantManager[] participants = new MockParticipantManager[nParticipants];
    for (int i = 0; i < nParticipants; i++) {
      participants[i] =
          new MockParticipantManager(ZK_ADDR, clusterName, "localhost_" + (12918 + i));
      participants[i].syncStart();
    }

    // Two STANDALONE controllers (InstanceType.CONTROLLER) competing for leadership.
    ClusterControllerManager c0 = new ClusterControllerManager(ZK_ADDR, clusterName, "controller_0");
    ClusterControllerManager c1 = new ClusterControllerManager(ZK_ADDR, clusterName, "controller_1");
    c0.syncStart();
    c1.syncStart();
    HelixManager[] controllers = new HelixManager[] {c0, c1};

    ZkHelixClusterVerifier verifier =
        new BestPossibleExternalViewVerifier.Builder(clusterName).setZkClient(_gZkClient).build();
    Assert.assertTrue(verifier.verifyByPolling(), "initial convergence failed");

    HelixManager leader = currentLeader(controllers, clusterName);
    System.out.println("STANDALONE: killing leader " + leader.getInstanceName());
    leader.disconnect();

    Assert.assertTrue(verifier.verifyByPolling(), "convergence failed after standalone failover");
    HelixManager newLeader = currentLeader(controllers, clusterName);
    Assert.assertFalse(newLeader.getInstanceName().equals(leader.getInstanceName()),
        "leadership did not move");

    final HelixManager nl = newLeader;
    boolean registered = TestHelper.verify(
        () -> countCurrentStateHandlers(nl, clusterName) > 0, 15000);
    int csHandlers = countCurrentStateHandlers(newLeader, clusterName);
    Assert.assertTrue(registered,
        "new STANDALONE leader " + newLeader.getInstanceName()
            + " registered NO per-instance CURRENTSTATES handlers after failover (had " + csHandlers
            + ") — MissingTopState would never clear");
    System.out.println("STANDALONE: new leader " + newLeader.getInstanceName() + " has " + csHandlers
        + " per-instance CURRENTSTATES handlers");

    Assert.assertTrue(verifier.verifyByPolling(), "post-failover state observation failed");

    for (HelixManager c : controllers) {
      if (c.isConnected()) {
        c.disconnect();
      }
    }
    for (MockParticipantManager p : participants) {
      p.syncStop();
    }
    deleteCluster(clusterName);
    System.out.println("END " + clusterName + " at " + new Date(System.currentTimeMillis()));
  }
}
