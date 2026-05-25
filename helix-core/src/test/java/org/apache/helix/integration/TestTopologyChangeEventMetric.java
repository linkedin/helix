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

import java.lang.management.ManagementFactory;
import java.util.Date;
import javax.management.MBeanServer;
import javax.management.ObjectName;

import org.apache.helix.HelixAdmin;
import org.apache.helix.TestHelper;
import org.apache.helix.common.ZkTestBase;
import org.apache.helix.controller.stages.ClusterEventType;
import org.apache.helix.integration.manager.ClusterControllerManager;
import org.apache.helix.integration.manager.MockParticipantManager;
import org.apache.helix.manager.zk.ZKHelixAdmin;
import org.apache.helix.tools.ClusterVerifiers.BestPossibleExternalViewVerifier;
import org.testng.Assert;
import org.testng.annotations.Test;

/**
 * Drives a real controller through topology-change events and asserts the per-event-type
 * counters on {@link org.apache.helix.monitoring.mbeans.TopologyChangeEventMonitor}
 * actually advance via the JMX surface. Complements the pure-unit test
 * {@code TestTopologyChangeEventMonitor} which exercises only the MBean wiring.
 */
public class TestTopologyChangeEventMetric extends ZkTestBase {

  private static final int N_PARTICIPANTS = 2;
  private static final int N_PARTITIONS = 4;
  private static final int N_REPLICAS = 2;
  private static final String RESOURCE = "TestDB";
  private static final String NEW_RESOURCE = "TestDB2";
  private static final int START_PORT = 12918;

  @Test
  public void testCountersAdvanceOnRealTopologyChanges() throws Exception {
    String clusterName = TestHelper.getTestMethodName();
    System.out.println("START " + clusterName + " at " + new Date(System.currentTimeMillis()));

    TestHelper.setupCluster(clusterName, ZK_ADDR, START_PORT,
        "localhost", RESOURCE,
        1,                  // # resources
        N_PARTITIONS,
        N_PARTICIPANTS,
        N_REPLICAS,
        "MasterSlave",
        true);              // do rebalance

    MockParticipantManager[] participants = new MockParticipantManager[N_PARTICIPANTS];
    for (int i = 0; i < N_PARTICIPANTS; i++) {
      String instanceName = "localhost_" + (START_PORT + i);
      participants[i] = new MockParticipantManager(ZK_ADDR, clusterName, instanceName);
      participants[i].syncStart();
    }

    ClusterControllerManager controller =
        new ClusterControllerManager(ZK_ADDR, clusterName, "controller_0");
    controller.syncStart();

    BestPossibleExternalViewVerifier verifier =
        new BestPossibleExternalViewVerifier.Builder(clusterName).setZkClient(_gZkClient)
            .setWaitTillVerify(TestHelper.DEFAULT_REBALANCE_PROCESSING_WAIT_TIME)
            .build();
    Assert.assertTrue(verifier.verifyByPolling(), "cluster did not converge after startup");

    MBeanServer server = ManagementFactory.getPlatformMBeanServer();

    // 1) Eager registration: all 5 topology-event MBeans should exist as soon as the
    //    controller acquired leadership and called ClusterStatusMonitor.active().
    for (ClusterEventType type : ClusterEventType.topologyChangeEventTypes()) {
      ObjectName name = topologyBean(clusterName, type);
      Assert.assertTrue(server.isRegistered(name),
          "Expected MBean to be registered: " + name);
    }

    // 2) Cluster startup itself drives LiveInstanceChange and IdealStateChange events
    //    through the controller pipeline. Both Received and Processed must move.
    awaitCounterAtLeast(server, clusterName, ClusterEventType.LiveInstanceChange,
        "ReceivedCounter", 1);
    awaitCounterAtLeast(server, clusterName, ClusterEventType.LiveInstanceChange,
        "ProcessedCounter", 1);
    awaitCounterAtLeast(server, clusterName, ClusterEventType.IdealStateChange,
        "ReceivedCounter", 1);
    awaitCounterAtLeast(server, clusterName, ClusterEventType.IdealStateChange,
        "ProcessedCounter", 1);

    // 3) Trigger a fresh IdealStateChange by adding a second resource and verify the
    //    IdealStateChange counter advances past the post-startup baseline.
    long isReceivedBefore = getCounter(server, clusterName,
        ClusterEventType.IdealStateChange, "ReceivedCounter");
    long isProcessedBefore = getCounter(server, clusterName,
        ClusterEventType.IdealStateChange, "ProcessedCounter");

    HelixAdmin admin = new ZKHelixAdmin(_gZkClient);
    _gSetupTool.addResourceToCluster(clusterName, NEW_RESOURCE, N_PARTITIONS, "MasterSlave");
    _gSetupTool.rebalanceStorageCluster(clusterName, NEW_RESOURCE, N_REPLICAS);

    Assert.assertTrue(verifier.verifyByPolling(), "cluster did not converge after add resource");

    awaitCounterAtLeast(server, clusterName, ClusterEventType.IdealStateChange,
        "ReceivedCounter", isReceivedBefore + 1);
    awaitCounterAtLeast(server, clusterName, ClusterEventType.IdealStateChange,
        "ProcessedCounter", isProcessedBefore + 1);

    // 4) Trigger an InstanceConfigChange by disabling one participant and verify that
    //    counter advances. Use a fresh JMX read because earlier checks may have raced
    //    with other topology events that bumped this counter incidentally.
    long icReceivedBefore = getCounter(server, clusterName,
        ClusterEventType.InstanceConfigChange, "ReceivedCounter");
    long icProcessedBefore = getCounter(server, clusterName,
        ClusterEventType.InstanceConfigChange, "ProcessedCounter");

    admin.enableInstance(clusterName, participants[0].getInstanceName(), false);
    Assert.assertTrue(verifier.verifyByPolling(), "cluster did not converge after disable");

    awaitCounterAtLeast(server, clusterName, ClusterEventType.InstanceConfigChange,
        "ReceivedCounter", icReceivedBefore + 1);
    awaitCounterAtLeast(server, clusterName, ClusterEventType.InstanceConfigChange,
        "ProcessedCounter", icProcessedBefore + 1);

    // Re-enable so cleanup proceeds cleanly.
    admin.enableInstance(clusterName, participants[0].getInstanceName(), true);
    Assert.assertTrue(verifier.verifyByPolling(), "cluster did not converge after re-enable");

    // 5) Sanity: non-topology event types never get an MBean registered for them.
    Assert.assertFalse(server.isRegistered(new ObjectName(
            "ClusterStatus:cluster=" + clusterName
                + ",eventName=TopologyChangeEvent,eventType=MessageChange")),
        "MessageChange should not have a TopologyChangeEvent MBean");

    // Cleanup
    controller.syncStop();
    for (MockParticipantManager p : participants) {
      p.syncStop();
    }
    deleteCluster(clusterName);

    System.out.println("END " + clusterName + " at " + new Date(System.currentTimeMillis()));
  }

  private static ObjectName topologyBean(String clusterName, ClusterEventType eventType)
      throws Exception {
    return new ObjectName("ClusterStatus:cluster=" + clusterName
        + ",eventName=TopologyChangeEvent,eventType=" + eventType.name());
  }

  private static long getCounter(MBeanServer server, String clusterName,
      ClusterEventType eventType, String attr) throws Exception {
    Object value = server.getAttribute(topologyBean(clusterName, eventType), attr);
    Assert.assertTrue(value instanceof Long,
        "Expected Long for " + attr + ", got " + (value == null ? "null" : value.getClass()));
    return (Long) value;
  }

  private static void awaitCounterAtLeast(MBeanServer server, String clusterName,
      ClusterEventType eventType, String attr, long minValue) throws Exception {
    boolean ok = TestHelper.verify(() -> {
      try {
        return getCounter(server, clusterName, eventType, attr) >= minValue;
      } catch (Exception e) {
        return false;
      }
    }, TestHelper.WAIT_DURATION);
    long observed = getCounter(server, clusterName, eventType, attr);
    Assert.assertTrue(ok,
        String.format("Counter %s for %s did not reach >= %d (observed=%d)",
            attr, eventType, minValue, observed));
  }
}
