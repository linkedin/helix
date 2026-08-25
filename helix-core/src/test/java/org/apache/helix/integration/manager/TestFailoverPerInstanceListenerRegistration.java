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

import java.util.Set;

import org.apache.helix.HelixManager;
import org.apache.helix.HelixManagerFactory;
import org.apache.helix.InstanceType;
import org.apache.helix.SystemPropertyKeys;
import org.apache.helix.TestHelper;
import org.apache.helix.common.ZkTestBase;
import org.apache.helix.manager.zk.ZKHelixManager;
import org.apache.helix.mock.participant.MockMSModelFactory;
import org.apache.helix.model.InstanceConfig;
import org.apache.helix.spectator.RoutingTableProvider;
import org.apache.helix.tools.ClusterVerifiers.BestPossibleExternalViewVerifier;
import org.apache.helix.tools.ClusterVerifiers.ZkHelixClusterVerifier;
import org.testng.Assert;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

/**
 * Focused validation: when a standby distributed controller is promoted on an
 * existing ZK session (failover via CALLBACK, not a new session), the per-instance listeners that
 * {@code checkLiveInstancesObservation} defers during INIT must still be registered. Otherwise the
 * new leader sets no {@code /INSTANCES/<inst>/CURRENTSTATES} watches, never observes replies, and
 * MissingTopState stays up.
 */
public class TestFailoverPerInstanceListenerRegistration extends ZkTestBase {

  private static final String FEATURE_FLAG =
      SystemPropertyKeys.CONTROLLER_PARALLEL_INSTANCE_LISTENER_REGISTRATION_ENABLED;

  private String _prevFlag;

  // The feature is default-OFF and read once when a controller's GenericHelixController is built
  // (in connect()), so enable it before any controller connects, and restore the prior value
  // afterwards (not a blanket clear) so it cannot leak into other test classes in the fork.
  @BeforeMethod
  public void enableFeature() {
    _prevFlag = System.getProperty(FEATURE_FLAG);
    System.setProperty(FEATURE_FLAG, "true");
  }

  @AfterMethod
  public void restoreFeature() {
    if (_prevFlag == null) {
      System.clearProperty(FEATURE_FLAG);
    } else {
      System.setProperty(FEATURE_FLAG, _prevFlag);
    }
  }

  @DataProvider(name = "failoverTopology")
  public static Object[][] failoverTopology() {
    // "COMBINED"   -> CONTROLLER_PARTICIPANT nodes (each node is both a leader candidate and a host)
    // "STANDALONE" -> separate CONTROLLER controllers + MockParticipant hosts
    return new Object[][] {{"COMBINED"}, {"STANDALONE"}};
  }

  // Disconnect one live host (not the current leader) so its MasterSlave partitions must be
  // reassigned, and return its instance name. Used to prove the new leader's freshly-registered
  // per-instance CURRENTSTATES watches actually fire (the cluster can only re-converge if it does).
  private String bounceANonLeaderHost(boolean combined, HelixManager[] controllers,
      MockParticipantManager[] participants, String leaderName) {
    if (combined) {
      for (HelixManager c : controllers) {
        if (c.isConnected() && !c.getInstanceName().equals(leaderName)) {
          c.disconnect();
          return c.getInstanceName();
        }
      }
    } else {
      for (MockParticipantManager p : participants) {
        if (p.isConnected() && !p.getInstanceName().equals(leaderName)) {
          p.syncStop();
          return p.getInstanceName();
        }
      }
    }
    return null;
  }

  /**
   * Failover validation across both controller topologies (data-provided): when the leader dies and
   * a standby is promoted on its EXISTING ZK session (CALLBACK, not a new session), the new leader
   * must (a) register one per-instance {@code /INSTANCES/<inst>/CURRENTSTATES} watch for EVERY live
   * instance, and (b) those watches must actually FIRE - proven by bouncing a non-leader MasterSlave
   * host and requiring the cluster to re-converge (only possible if the leader observes the surviving
   * hosts' current-state changes). Otherwise MissingTopState stays up.
   */
  @Test(dataProvider = "failoverTopology")
  public void failoverRegistersPerInstanceWatchesAndObservesState(String topology) throws Exception {
    String clusterName =
        TestHelper.getTestClassName() + "_" + TestHelper.getTestMethodName() + "_" + topology;
    boolean combined = "COMBINED".equals(topology);
    int nNodes = 4; // hosts

    // Multiple resources so per-instance CURRENTSTATES fan-out is non-trivial.
    TestHelper.setupCluster(clusterName, ZK_ADDR, 12918, "localhost", "TestDB",
        5, // resources
        8, // partitions per resource
        nNodes, 2, "MasterSlave", true);

    HelixManager[] controllers;
    MockParticipantManager[] participants = null; // only used for STANDALONE
    if (combined) {
      // Each node is a CONTROLLER_PARTICIPANT: both a leadership candidate and a resource host.
      controllers = new HelixManager[nNodes];
      for (int i = 0; i < nNodes; i++) {
        ZKHelixManager m = new ZKHelixManager(clusterName, "localhost_" + (12918 + i),
            InstanceType.CONTROLLER_PARTICIPANT, ZK_ADDR);
        m.getStateMachineEngine().registerStateModelFactory("MasterSlave", new MockMSModelFactory());
        m.connect();
        controllers[i] = m;
      }
    } else {
      // Separate participant hosts + two STANDALONE controllers competing for leadership.
      participants = new MockParticipantManager[nNodes];
      for (int i = 0; i < nNodes; i++) {
        participants[i] =
            new MockParticipantManager(ZK_ADDR, clusterName, "localhost_" + (12918 + i));
        participants[i].syncStart();
      }
      ClusterControllerManager c0 =
          new ClusterControllerManager(ZK_ADDR, clusterName, "controller_0");
      ClusterControllerManager c1 =
          new ClusterControllerManager(ZK_ADDR, clusterName, "controller_1");
      c0.syncStart();
      c1.syncStart();
      controllers = new HelixManager[] {c0, c1};
    }

    ZkHelixClusterVerifier verifier =
        new BestPossibleExternalViewVerifier.Builder(clusterName).setZkClient(_gZkClient).build();
    Assert.assertTrue(verifier.verifyByZkCallback(30000),
        "initial convergence failed (" + topology + ")");

    // Failover: kill the leader; a standby must take over on its EXISTING session (CALLBACK path).
    HelixManager oldLeader =
        PerInstanceListenerTestSupport.currentLeader(_gZkClient, controllers, clusterName);
    oldLeader.disconnect();

    Assert.assertTrue(verifier.verifyByZkCallback(30000),
        "convergence failed after failover (" + topology + ")");
    HelixManager newLeader =
        PerInstanceListenerTestSupport.currentLeader(_gZkClient, controllers, clusterName);
    Assert.assertFalse(newLeader.getInstanceName().equals(oldLeader.getInstanceName()),
        "leadership did not move (" + topology + ")");

    // Exact-count assertion: one CURRENTSTATES watch per live instance (not just > 0 - a partial
    // registration is a bug). Registration is async; poll.
    final int expected =
        PerInstanceListenerTestSupport.liveInstanceCount(_gZkClient, clusterName);
    Assert.assertTrue(expected > 0, "no live instances to observe (" + topology + ")");
    boolean registered = TestHelper.verify(() -> PerInstanceListenerTestSupport
        .countCurrentStateHandlers(newLeader, clusterName) == expected, 15000);
    int csHandlers =
        PerInstanceListenerTestSupport.countCurrentStateHandlers(newLeader, clusterName);
    Assert.assertTrue(registered,
        "new leader " + newLeader.getInstanceName() + " had " + csHandlers
            + " per-instance CURRENTSTATES handlers after failover, expected " + expected
            + " (one per live instance) - partial/zero registration, MissingTopState would not clear ("
            + topology + ")");

    // Prove the freshly-registered watches actually FIRE: bounce a live non-leader MasterSlave host.
    // Its partitions must be reassigned, which the new leader can only drive by observing the
    // surviving hosts' CURRENTSTATE changes through the per-instance watches it just registered.
    String bounced =
        bounceANonLeaderHost(combined, controllers, participants, newLeader.getInstanceName());
    Assert.assertNotNull(bounced, "no non-leader host to bounce (" + topology + ")");
    final int expectedAfterBounce = expected - 1;
    Assert.assertTrue(verifier.verifyByZkCallback(30000),
        "cluster did not re-converge after bouncing host " + bounced + " (" + topology
            + ") - the new leader's per-instance CURRENTSTATES watches did not fire");
    Assert.assertTrue(
        TestHelper.verify(() -> PerInstanceListenerTestSupport
            .liveInstanceCount(_gZkClient, clusterName) == expectedAfterBounce, 15000),
        "live-instance count did not drop to " + expectedAfterBounce + " after bounce (" + topology
            + ")");

    if (participants != null) {
      for (MockParticipantManager p : participants) {
        if (p.isConnected()) {
          p.syncStop();
        }
      }
    }
    for (HelixManager c : controllers) {
      if (c.isConnected()) {
        c.disconnect();
      }
    }
    deleteCluster(clusterName);
  }

  /**
   * Reconnect regression: a controller that disconnects and reconnects must still
   * register per-instance watches on the next leadership acquisition. The deferred-registration
   * executor is shut down on disconnect(); if it is not rebuilt on connect(), every post-reconnect
   * acquisition is rejected and the reconnected leader observes nothing. Before the fix the 2nd
   * connect registered 0 CURRENTSTATES handlers.
   */
  @Test
  public void reconnectedControllerReregistersPerInstanceWatches() throws Exception {
    String clusterName = TestHelper.getTestClassName() + "_" + TestHelper.getTestMethodName();
    int nParticipants = 3;

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

    // A single standalone controller; connect -> disconnect -> connect on the SAME manager.
    ZKHelixManager controller =
        new ZKHelixManager(clusterName, "controller_0", InstanceType.CONTROLLER, ZK_ADDR);
    ZkHelixClusterVerifier verifier =
        new BestPossibleExternalViewVerifier.Builder(clusterName).setZkClient(_gZkClient).build();

    // 1st connect: becomes leader, one CURRENTSTATES handler per live participant.
    controller.connect();
    Assert.assertTrue(verifier.verifyByPolling(), "initial convergence failed");
    final int expected =
        PerInstanceListenerTestSupport.liveInstanceCount(_gZkClient, clusterName);
    Assert.assertTrue(expected > 0, "no live instances to observe");
    Assert.assertTrue(
        TestHelper.verify(() -> PerInstanceListenerTestSupport
            .countCurrentStateHandlers(controller, clusterName) == expected, 15000),
        "1st connect registered "
            + PerInstanceListenerTestSupport.countCurrentStateHandlers(controller, clusterName)
            + " CURRENTSTATES handlers, expected " + expected);

    // Disconnect then reconnect the SAME manager.
    controller.disconnect();
    controller.connect();
    Assert.assertTrue(verifier.verifyByPolling(), "convergence failed after reconnect");

    // 2nd connect MUST re-register ALL per-instance watches (the bug registered 0 here).
    boolean ok = TestHelper.verify(() -> PerInstanceListenerTestSupport
        .countCurrentStateHandlers(controller, clusterName) == expected, 15000);
    int afterReconnect =
        PerInstanceListenerTestSupport.countCurrentStateHandlers(controller, clusterName);
    Assert.assertTrue(ok, "after reconnect the controller had " + afterReconnect
        + " per-instance CURRENTSTATES handlers, expected " + expected
        + " - reconnected leader observes nothing (dead deferred-registration executor)");

    if (controller.isConnected()) {
      controller.disconnect();
    }
    for (MockParticipantManager p : participants) {
      p.syncStop();
    }
    deleteCluster(clusterName);
  }

  /**
   * Containment: the feature only changes the controller's per-instance registration.
   * Every OTHER addListener caller must be unaffected with the flag ON. A spectator's
   * RoutingTableProvider registers external-view / live-instance / config listeners through the same
   * shared addListener() path (18 APIs / 27 call sites) - with the feature ON it must still work
   * end-to-end (watches fire, routing table populates), proving participants/spectators are not
   * touched by the init-outside-lock change.
   */
  @Test
  public void spectatorRoutingTableWorksWithFeatureOn() throws Exception {
    String clusterName = TestHelper.getTestClassName() + "_" + TestHelper.getTestMethodName();
    int nParticipants = 3;

    TestHelper.setupCluster(clusterName, ZK_ADDR, 12918, "localhost", "TestDB",
        3, // resources -> TestDB0, TestDB1, TestDB2
        8, // partitions per resource
        nParticipants, 2, "MasterSlave", true);

    MockParticipantManager[] participants = new MockParticipantManager[nParticipants];
    for (int i = 0; i < nParticipants; i++) {
      participants[i] =
          new MockParticipantManager(ZK_ADDR, clusterName, "localhost_" + (12918 + i));
      participants[i].syncStart();
    }
    ClusterControllerManager controller =
        new ClusterControllerManager(ZK_ADDR, clusterName, "controller_0");
    controller.syncStart();

    // Spectator whose RoutingTableProvider registers listeners through the shared addListener().
    HelixManager spectator =
        HelixManagerFactory.getZKHelixManager(clusterName, "spectator", InstanceType.SPECTATOR,
            ZK_ADDR);
    spectator.connect();
    RoutingTableProvider rtp = new RoutingTableProvider(spectator);
    spectator.addExternalViewChangeListener(rtp);
    spectator.addLiveInstanceChangeListener(rtp);
    spectator.addInstanceConfigChangeListener(rtp);

    ZkHelixClusterVerifier verifier =
        new BestPossibleExternalViewVerifier.Builder(clusterName).setZkClient(_gZkClient).build();
    Assert.assertTrue(verifier.verifyByPolling(), "cluster did not converge");

    // With the feature ON, the spectator's listeners (added via the unchanged addListener path)
    // must still fire and populate the routing table.
    boolean populated = TestHelper.verify(() -> {
      Set<InstanceConfig> masters = rtp.getInstances("TestDB0", "MASTER");
      return masters != null && !masters.isEmpty();
    }, 15000);
    Set<InstanceConfig> masters = rtp.getInstances("TestDB0", "MASTER");
    Assert.assertTrue(populated,
        "spectator routing table did not populate with the feature ON (masters="
            + (masters == null ? "null" : masters.size()) + ") - a non-controller addListener path "
            + "was affected by the change");

    rtp.shutdown();
    spectator.disconnect();
    controller.syncStop();
    for (MockParticipantManager p : participants) {
      p.syncStop();
    }
    deleteCluster(clusterName);
  }
}
