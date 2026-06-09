package org.apache.helix.integration;

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

import java.lang.reflect.Field;
import java.util.Date;

import org.apache.helix.SystemPropertyKeys;
import org.apache.helix.TestHelper;
import org.apache.helix.ZkTestHelper;
import org.apache.helix.ZkUnitTestBase;
import org.apache.helix.integration.manager.ClusterControllerManager;
import org.apache.helix.integration.manager.MockParticipantManager;
import org.apache.helix.tools.ClusterVerifiers.BestPossibleExternalViewVerifier;
import org.apache.helix.tools.ClusterVerifiers.ZkHelixClusterVerifier;
import org.apache.helix.zookeeper.api.client.RealmAwareZkClient;
import org.apache.helix.zookeeper.zkclient.util.ZkPathRecursiveWatcherTrie;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

/**
 * End-to-end test for the persist-recursive current-state watch
 * ({@link SystemPropertyKeys#PARTICIPANT_STATE_PERSIST_RECURSIVE_WATCH_ENABLED}).
 *
 * With the flag enabled, the controller's ZkClient is built with usePersistWatcher=true and each
 * CURRENT_STATE / TASK_CURRENT_STATE CallbackHandler installs ONE PERSISTENT_RECURSIVE watch on the
 * participant's CURRENTSTATES subtree instead of one child watch plus one data watch per partition.
 *
 * The controller can only compute a correct ExternalView if it actually receives the participants'
 * current-state changes. So a green {@link BestPossibleExternalViewVerifier} both at steady state and
 * after an ongoing current-state change (a participant failure that forces masters to move) proves the
 * single recursive watch delivers initial AND incremental current-state events correctly.
 */
public class TestCurrentStatePersistRecursiveWatch extends ZkUnitTestBase {

  @BeforeClass
  public void beforeClass() {
    System.setProperty(SystemPropertyKeys.PARTICIPANT_STATE_PERSIST_RECURSIVE_WATCH_ENABLED, "true");
  }

  @AfterClass
  public void afterClass() {
    System.clearProperty(SystemPropertyKeys.PARTICIPANT_STATE_PERSIST_RECURSIVE_WATCH_ENABLED);
  }

  @Test
  public void testControllerConvergesWithRecursiveCurrentStateWatch() throws Exception {
    String className = TestHelper.getTestClassName();
    String methodName = TestHelper.getTestMethodName();
    String clusterName = className + "_" + methodName;
    final int n = 3;

    System.out.println("START " + clusterName + " at " + new Date(System.currentTimeMillis()));

    TestHelper.setupCluster(clusterName, ZK_ADDR, 12918, // participant port
        "localhost", // participant name prefix
        "TestDB", // resource name prefix
        1, // resources
        8, // partitions per resource
        n, // number of nodes
        3, // replicas
        "MasterSlave", true); // do rebalance

    MockParticipantManager[] participants = new MockParticipantManager[n];
    for (int i = 0; i < n; i++) {
      String instanceName = "localhost_" + (12918 + i);
      participants[i] = new MockParticipantManager(ZK_ADDR, clusterName, instanceName);
      participants[i].syncStart();
    }

    // The controller's ZkClient is built with usePersistWatcher=true because the flag is set, so its
    // CURRENT_STATE CallbackHandlers use the single recursive watch.
    ClusterControllerManager controller =
        new ClusterControllerManager(ZK_ADDR, clusterName, "controller_0");
    controller.syncStart();

    ZkHelixClusterVerifier verifier =
        new BestPossibleExternalViewVerifier.Builder(clusterName).setZkClient(_gZkClient)
            .setWaitTillVerify(TestHelper.DEFAULT_REBALANCE_PROCESSING_WAIT_TIME).build();

    // Steady state: controller must have observed every participant's current state through the
    // recursive watch to make ExternalView == BestPossible.
    Assert.assertTrue(verifier.verifyByPolling(),
        "Cluster did not converge at steady state with the recursive current-state watch");

    // Ongoing current-state changes: drop one participant; the masters it hosted must move, which the
    // controller can only carry out by observing OFFLINE->SLAVE->MASTER current-state transitions on
    // the surviving participants (delivered by the recursive watch). Re-convergence proves incremental
    // current-state events are delivered.
    participants[0].syncStop();
    Assert.assertTrue(verifier.verifyByPolling(),
        "Cluster did not re-converge after a participant failure; recursive current-state watch did "
            + "not deliver incremental events");

    // Cleanup.
    controller.syncStop();
    for (int i = 0; i < n; i++) {
      if (participants[i].isConnected()) {
        participants[i].syncStop();
      }
    }
    deleteCluster(clusterName);
    System.out.println("END " + clusterName + " at " + new Date(System.currentTimeMillis()));
  }

  /**
   * Regression test for the watch lifecycle: with the flag on, the controller must (a) use a small,
   * O(participants) number of CURRENTSTATES watches (one recursive watch per participant subtree, not
   * one per partition), and (b) REMOVE the recursive watch when a participant departs (reset()), with
   * no leak. The latter specifically guards against re-subscribing the persistent watch on every
   * callback / re-installing it on FINALIZE during reset.
   */
  @Test
  public void testRecursiveWatchRemovedOnParticipantDeparture() throws Exception {
    String className = TestHelper.getTestClassName();
    String methodName = TestHelper.getTestMethodName();
    String clusterName = className + "_" + methodName;
    final int n = 3;

    System.out.println("START " + clusterName + " at " + new Date(System.currentTimeMillis()));

    TestHelper.setupCluster(clusterName, ZK_ADDR, 12918, "localhost", "TestDB",
        1, // resources
        8, // partitions per resource
        n, // nodes
        3, // replicas
        "MasterSlave", true);

    MockParticipantManager[] participants = new MockParticipantManager[n];
    for (int i = 0; i < n; i++) {
      participants[i] = new MockParticipantManager(ZK_ADDR, clusterName, "localhost_" + (12918 + i));
      participants[i].syncStart();
    }
    ClusterControllerManager controller =
        new ClusterControllerManager(ZK_ADDR, clusterName, "controller_0");
    controller.syncStart();

    ZkHelixClusterVerifier verifier =
        new BestPossibleExternalViewVerifier.Builder(clusterName).setZkClient(_gZkClient)
            .setWaitTillVerify(TestHelper.DEFAULT_REBALANCE_PROCESSING_WAIT_TIME).build();
    Assert.assertTrue(verifier.verifyByPolling(), "Cluster did not converge");

    // The CURRENTSTATES subtree of the participant whose session we will expire. Use the OLD session id
    // because expireSession reconnects the participant under a NEW session (the controller will watch
    // the new session's subtree; only the OLD one must be torn down).
    String oldSession = participants[0].getSessionId();
    String oldCsPath =
        "/" + clusterName + "/INSTANCES/localhost_12918/CURRENTSTATES/" + oldSession;

    // Precondition: the controller's recursive-watch trie holds a listener for the old session's
    // CURRENTSTATES subtree. (A wchp server-side dump cannot prove the leak: after expiry the old
    // CURRENTSTATES node is deleted, and a persistent watch re-armed on a deleted path is not reported.
    // The client-side trie is the definitive location where the leaked handler is retained.)
    Assert.assertTrue(countControllerRecursiveListeners(controller, oldCsPath) >= 1,
        "precondition: controller should hold a recursive watch on the participant's CURRENTSTATES "
            + "before expiry");

    // Expire the participant session: the old session leaves LIVEINSTANCES -> controller removeListener
    // -> reset() must remove the persistent recursive watch on the OLD session's CURRENTSTATES subtree.
    // The buggy version re-installed the watch via invoke(FINALIZE) right after reset unsubscribed it,
    // permanently leaking one CallbackHandler (retained by the recursive trie) + one server-side watch
    // on the dead path. This fails on that bug and passes once reset tears the watch down.
    ZkTestHelper.expireSession(participants[0].getZkClient());

    boolean removed = TestHelper.verify(
        () -> countControllerRecursiveListeners(controller, oldCsPath) == 0,
        TestHelper.WAIT_DURATION);
    Assert.assertTrue(removed,
        "Controller leaked a recursive watch on the expired session's CURRENTSTATES subtree ("
            + oldCsPath + ") -- the CallbackHandler was re-subscribed on FINALIZE and orphaned in the "
            + "recursive-watch trie");

    controller.syncStop();
    for (int i = 0; i < n; i++) {
      if (participants[i].isConnected()) {
        participants[i].syncStop();
      }
    }
    deleteCluster(clusterName);
    System.out.println("END " + clusterName + " at " + new Date(System.currentTimeMillis()));
  }

  /**
   * Number of recursive-watch listeners the controller's client-side trie holds for {@code path}.
   * Reaches the raw {@code ZkClient} that owns the {@link ZkPathRecursiveWatcherTrie} via reflection
   * (the same reflection-into-zkclient-internals pattern used by {@code ZkTestHelper#getZkWatch}).
   */
  private int countControllerRecursiveListeners(ClusterControllerManager controller, String path)
      throws Exception {
    RealmAwareZkClient zkClient = controller.getZkClient();
    // In single-realm mode getZkClient() is the raw ZkClient (the trie is on its superclass); in
    // realm-aware mode it is a DedicatedZkClient that wraps the raw client in _rawZkClient. Handle both.
    Object trieOwner = zkClient;
    try {
      getFieldValue(zkClient, "_zkPathRecursiveWatcherTrie");
    } catch (NoSuchFieldException notRaw) {
      trieOwner = getFieldValue(zkClient, "_rawZkClient");
    }
    Object usePersist = getFieldValue(trieOwner, "_usePersistWatcher");
    if (!Boolean.TRUE.equals(usePersist)) {
      throw new IllegalStateException("controller ZkClient was not built with usePersistWatcher=true; "
          + "the persist-recursive watch is not active (usePersistWatcher=" + usePersist + ")");
    }
    ZkPathRecursiveWatcherTrie trie =
        (ZkPathRecursiveWatcherTrie) getFieldValue(trieOwner, "_zkPathRecursiveWatcherTrie");
    return trie.getAllRecursiveListeners(path).size();
  }

  private static Object getFieldValue(Object target, String fieldName) throws Exception {
    Class<?> clazz = target.getClass();
    while (clazz != null) {
      try {
        Field field = clazz.getDeclaredField(fieldName);
        field.setAccessible(true);
        return field.get(target);
      } catch (NoSuchFieldException e) {
        clazz = clazz.getSuperclass();
      }
    }
    throw new NoSuchFieldException(fieldName + " not found on " + target.getClass());
  }
}
