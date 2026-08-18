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

import java.io.ByteArrayOutputStream;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.util.Date;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;

import org.apache.helix.HelixManager;
import org.apache.helix.InstanceType;
import org.apache.helix.PropertyKey;
import org.apache.helix.SystemPropertyKeys;
import org.apache.helix.TestHelper;
import org.apache.helix.manager.zk.CallbackHandler;
import org.apache.helix.manager.zk.ZKHelixDataAccessor;
import org.apache.helix.manager.zk.ZKHelixManager;
import org.apache.helix.manager.zk.ZkBaseDataAccessor;
import org.apache.helix.model.LiveInstance;
import org.apache.helix.tools.ClusterVerifiers.BestPossibleExternalViewVerifier;
import org.apache.helix.tools.ClusterVerifiers.ZkHelixClusterVerifier;
import org.apache.helix.zookeeper.api.client.HelixZkClient;
import org.apache.helix.zookeeper.datamodel.serializer.ZNRecordSerializer;
import org.apache.helix.zookeeper.impl.factory.DedicatedZkClientFactory;
import org.testng.Assert;
import org.testng.SkipException;
import org.testng.annotations.Test;

/**
 * REAL-ZooKeeper validation for CICP-34606 (NOT the embedded TestingZooKeeper used by the rest of
 * the suite). Requires an externally running ZK ensemble whose client port is passed via
 * -DrealZkAddr=host:port (default localhost:2199) and whose 4lw whitelist enables {@code wchp}.
 *
 * <p>This test does not extend {@link org.apache.helix.common.ZkTestBase}, so it starts no embedded
 * ZK. It proves the feature end-to-end against a real server by counting SERVER-SIDE watches (via
 * the {@code wchp} four-letter word) rather than client-side handler reflection: after a standby is
 * promoted on its existing session (CALLBACK failover), the new leader must hold one
 * {@code /INSTANCES/<inst>/CURRENTSTATES} watch per live instance on the real ZK. It runs the same
 * scenario with the feature ON and OFF and asserts functional equivalence.
 */
public class TestRealZkFailoverWatches {

  private static final String REAL_ZK =
      System.getProperty("realZkAddr", "localhost:2199");
  private static final String FEATURE_FLAG =
      SystemPropertyKeys.CONTROLLER_PARALLEL_INSTANCE_LISTENER_REGISTRATION_ENABLED;

  // ---- real-ZK 4lw client (Java socket; nc is policy-blocked) ---------------------------------

  private static String send4lw(String cmd) throws Exception {
    String[] hp = REAL_ZK.split(":");
    String host = hp[0];
    int port = Integer.parseInt(hp[1]);
    try (Socket s = new Socket()) {
      s.connect(new InetSocketAddress(host, port), 5000);
      s.setSoTimeout(5000);
      OutputStream out = s.getOutputStream();
      out.write(cmd.getBytes());
      out.flush();
      InputStream in = s.getInputStream();
      ByteArrayOutputStream bout = new ByteArrayOutputStream();
      byte[] buf = new byte[4096];
      int n;
      while ((n = in.read(buf)) != -1) {
        bout.write(buf, 0, n);
      }
      return bout.toString("UTF-8");
    }
  }

  // Distinct server-side watch paths of the form /<cluster>/INSTANCES/<inst>/CURRENTSTATES...
  private static Set<String> serverSideCurrentStateWatchPaths(String clusterName) throws Exception {
    String wchp = send4lw("wchp");
    Set<String> paths = new LinkedHashSet<>();
    for (String line : wchp.split("\\r?\\n")) {
      String p = line.trim();
      if (p.startsWith("/" + clusterName + "/INSTANCES/") && p.contains("/CURRENTSTATES")) {
        paths.add(p);
      }
    }
    return paths;
  }

  // ---- client-side cross-check (same accounting the embedded test uses) -----------------------

  @SuppressWarnings("unchecked")
  private static int clientSideCurrentStateHandlers(HelixManager mgr, String clusterName)
      throws Exception {
    java.lang.reflect.Field f = ZKHelixManager.class.getDeclaredField("_handlers");
    f.setAccessible(true);
    List<CallbackHandler> handlers = (List<CallbackHandler>) f.get(mgr);
    int count = 0;
    synchronized (mgr) {
      for (CallbackHandler h : new java.util.ArrayList<>(handlers)) {
        String p = h.getPath();
        if (p.contains("/" + clusterName + "/INSTANCES/") && p.contains("/CURRENTSTATES")) {
          count++;
        }
      }
    }
    return count;
  }

  private static int liveInstanceCount(HelixZkClient zkClient, String clusterName) {
    ZKHelixDataAccessor accessor =
        new ZKHelixDataAccessor(clusterName, new ZkBaseDataAccessor<>(zkClient));
    List<String> live = accessor.getChildNames(accessor.keyBuilder().liveInstances());
    return live == null ? 0 : live.size();
  }

  private static HelixManager currentLeader(HelixZkClient zkClient, HelixManager[] controllers,
      String clusterName) {
    ZKHelixDataAccessor accessor =
        new ZKHelixDataAccessor(clusterName, new ZkBaseDataAccessor<>(zkClient));
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

  private HelixZkClient newRealZkClient() {
    HelixZkClient.ZkClientConfig clientConfig = new HelixZkClient.ZkClientConfig();
    clientConfig.setZkSerializer(new ZNRecordSerializer());
    return DedicatedZkClientFactory.getInstance()
        .buildZkClient(new HelixZkClient.ZkConnectionConfig(REAL_ZK), clientConfig);
  }

  /**
   * One full run of the scenario against real ZK: setup -> converge -> measure -> kill leader ->
   * converge -> measure again. Returns the server-side CURRENTSTATES watch count observed on the
   * post-failover leader. {@code flagOn} selects the feature state (set BEFORE any controller
   * connects, since it is read once when GenericHelixController is built).
   */
  private int runScenario(boolean flagOn) throws Exception {
    // CI has no external ZK (the suite uses embedded TestingZooKeeper). Skip - not fail - unless a
    // real ensemble with 4lw enabled is reachable at -DrealZkAddr (e.g. the EI soak or a local run).
    String ruok;
    try {
      ruok = send4lw("ruok");
    } catch (Exception e) {
      throw new SkipException("No external ZK reachable at " + REAL_ZK
          + " (set -DrealZkAddr=host:port to a real ensemble to run this real-ZK validation): "
          + e.getMessage());
    }
    if (!"imok".equals(ruok.trim())) {
      throw new SkipException("External ZK at " + REAL_ZK + " did not answer ruok=imok (got '"
          + ruok.trim() + "'); ensure 4lw whitelist includes ruok/wchp");
    }
    String prev = System.getProperty(FEATURE_FLAG);
    if (flagOn) {
      System.setProperty(FEATURE_FLAG, "true");
    } else {
      System.setProperty(FEATURE_FLAG, "false");
    }
    HelixZkClient zkClient = newRealZkClient();
    String clusterName = "RealZk_" + (flagOn ? "flagOn" : "flagOff") + "_"
        + System.currentTimeMillis();
    int nParticipants = 5;
    System.out.println("=== START real-ZK scenario flagOn=" + flagOn + " cluster=" + clusterName
        + " zk=" + REAL_ZK + " at " + new Date() + " ===");
    try {
      TestHelper.setupCluster(clusterName, REAL_ZK, 12918, "localhost", "TestDB",
          5, // resources
          8, // partitions per resource
          nParticipants, 2, "MasterSlave", true);

      MockParticipantManager[] participants = new MockParticipantManager[nParticipants];
      for (int i = 0; i < nParticipants; i++) {
        participants[i] =
            new MockParticipantManager(REAL_ZK, clusterName, "localhost_" + (12918 + i));
        participants[i].syncStart();
      }

      ClusterControllerManager c0 =
          new ClusterControllerManager(REAL_ZK, clusterName, "controller_0");
      ClusterControllerManager c1 =
          new ClusterControllerManager(REAL_ZK, clusterName, "controller_1");
      c0.syncStart();
      c1.syncStart();
      HelixManager[] controllers = new HelixManager[] {c0, c1};

      ZkHelixClusterVerifier verifier =
          new BestPossibleExternalViewVerifier.Builder(clusterName).setZkClient(zkClient).build();
      Assert.assertTrue(verifier.verifyByPolling(), "initial convergence failed (flagOn=" + flagOn
          + ")");

      int expected = liveInstanceCount(zkClient, clusterName);
      Assert.assertTrue(expected > 0, "no live instances");

      HelixManager leader0 = currentLeader(zkClient, controllers, clusterName);
      TestHelper.verify(
          () -> serverSideCurrentStateWatchPaths(clusterName).size() >= expected, 15000);
      Set<String> before = serverSideCurrentStateWatchPaths(clusterName);
      System.out.println("[flagOn=" + flagOn + "] initial leader " + leader0.getInstanceName()
          + " -> server-side CURRENTSTATES watch paths=" + before.size()
          + " client-side handlers=" + clientSideCurrentStateHandlers(leader0, clusterName)
          + " (expected >= " + expected + ")");
      Assert.assertTrue(before.size() >= expected,
          "initial leader had " + before.size() + " server-side CURRENTSTATES watches, expected >= "
              + expected);

      // Failover: kill the leader; standby must take over on its EXISTING session (CALLBACK path).
      System.out.println("[flagOn=" + flagOn + "] killing leader " + leader0.getInstanceName());
      leader0.disconnect();
      Assert.assertTrue(verifier.verifyByPolling(), "convergence failed after failover (flagOn="
          + flagOn + ")");
      HelixManager leader1 = currentLeader(zkClient, controllers, clusterName);
      Assert.assertFalse(leader1.getInstanceName().equals(leader0.getInstanceName()),
          "leadership did not move");

      int expectedAfter = liveInstanceCount(zkClient, clusterName);
      final String cn = clusterName;
      boolean ok = TestHelper.verify(
          () -> serverSideCurrentStateWatchPaths(cn).size() >= expectedAfter, 20000);
      Set<String> after = serverSideCurrentStateWatchPaths(clusterName);
      int clientAfter = clientSideCurrentStateHandlers(leader1, clusterName);
      System.out.println("[flagOn=" + flagOn + "] NEW leader " + leader1.getInstanceName()
          + " -> server-side CURRENTSTATES watch paths=" + after.size()
          + " client-side handlers=" + clientAfter + " (expected >= " + expectedAfter + ")");
      Assert.assertTrue(ok, "after failover new leader had " + after.size()
          + " server-side CURRENTSTATES watches, expected >= " + expectedAfter
          + " (MissingTopState would not clear)");

      // Prove functional: cluster stays converged (leader is observing current-state replies).
      Assert.assertTrue(verifier.verifyByPolling(), "post-failover observation failed (flagOn="
          + flagOn + ")");

      for (HelixManager c : controllers) {
        if (c.isConnected()) {
          c.disconnect();
        }
      }
      for (MockParticipantManager p : participants) {
        p.syncStop();
      }
      System.out.println("=== END real-ZK scenario flagOn=" + flagOn + " -> post-failover "
          + "server-side watches=" + after.size() + " ===");
      return after.size();
    } finally {
      try {
        TestHelper.dropCluster(clusterName, zkClient);
      } catch (Exception ignore) {
        // best-effort cleanup
      }
      zkClient.close();
      if (prev == null) {
        System.clearProperty(FEATURE_FLAG);
      } else {
        System.setProperty(FEATURE_FLAG, prev);
      }
    }
  }

  @Test
  public void realZkFailoverRegistersServerSideWatchesFeatureOn() throws Exception {
    int on = runScenario(true);
    Assert.assertTrue(on > 0, "flag ON produced no server-side CURRENTSTATES watches after failover");
  }

  @Test
  public void realZkFailoverEquivalentWithFeatureOff() throws Exception {
    int off = runScenario(false);
    int on = runScenario(true);
    // Both flag states must leave the post-failover leader observing every live instance. The
    // feature only changes HOW the watches are registered (parallel/deferred), not HOW MANY.
    Assert.assertTrue(off > 0, "flag OFF produced no server-side CURRENTSTATES watches after failover");
    Assert.assertTrue(on > 0, "flag ON produced no server-side CURRENTSTATES watches after failover");
    Assert.assertEquals(on, off,
        "server-side CURRENTSTATES watch count differs between flag ON (" + on + ") and OFF (" + off
            + ") - the feature changed observable watch coverage, not just registration strategy");
  }
}
