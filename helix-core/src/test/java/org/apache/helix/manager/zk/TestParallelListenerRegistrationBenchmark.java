package org.apache.helix.manager.zk;

import java.lang.management.ManagementFactory;
import java.util.Date;

import org.apache.helix.InstanceType;
import org.apache.helix.PropertyKey;
import org.apache.helix.TestHelper;
import org.apache.helix.ZkUnitTestBase;
import org.apache.helix.model.LiveInstance;
import org.testng.Assert;
import org.testng.annotations.Test;


/**
 * Integration benchmark with real ZK proving parallel per-instance listener
 * registration works correctly. Uses the same pattern as TestDistControllerElection
 * but with live instances present, exercising the full addListenersToController path.
 *
 * Run: mvn test -pl helix-core -Dtest=TestParallelListenerRegistrationBenchmark
 */
public class TestParallelListenerRegistrationBenchmark extends ZkUnitTestBase {

  @Test
  public void testParallelListenerRegistrationWithRealZk() throws Exception {
    int numInstances = 20;
    String clusterName = "TestParallelBench_" + TestHelper.getTestMethodName();
    System.out.println("START " + clusterName + " instances=" + numInstances
        + " at " + new Date());

    // Set up cluster
    TestHelper.setupEmptyCluster(_gZkClient, clusterName);

    // Create live instances in ZK (ephemeral nodes)
    ZKHelixDataAccessor accessor =
        new ZKHelixDataAccessor(clusterName, new ZkBaseDataAccessor<>(_gZkClient));
    PropertyKey.Builder keyBuilder = accessor.keyBuilder();
    for (int i = 0; i < numInstances; i++) {
      String instanceName = "localhost_" + (12918 + i);
      LiveInstance li = new LiveInstance(instanceName);
      li.setSessionId("session_" + i);
      li.setHelixVersion("1.0");
      li.setLiveInstance(ManagementFactory.getRuntimeMXBean().getName());
      accessor.setProperty(keyBuilder.liveInstance(instanceName), li);
    }
    int liveCount = accessor.getChildNames(keyBuilder.liveInstances()).size();
    System.out.println("Live instances created in ZK: " + liveCount);
    Assert.assertEquals(liveCount, numInstances);

    // Use the same pattern as TestDistControllerElection:
    // real ZKHelixManager (MockZKHelixManager won't work - its addListener is no-op)
    // but call DistributedLeaderElection.onControllerChange directly.
    String controllerName = "controller_0";
    ZKHelixManager manager = new ZKHelixManager(clusterName, controllerName,
        InstanceType.CONTROLLER, ZK_ADDR);
    // connect() to establish ZK session, then we manually trigger leader election
    manager.connect();

    System.out.println("Controller connected, session: " + manager.getSessionId());

    // Count handlers after connect - this includes all primary + per-instance handlers
    int handlerCount;
    synchronized (manager) {
      handlerCount = manager._handlers.size();
    }

    // Expected:
    //   Primary: controllerMessage + controller + instanceConfig + resourceConfig +
    //            clusterConfig + customizedStateConfig + liveInstance + idealState = 8
    //   Per-instance: (currentState + taskCurrentState + message + customizedStateRoot) * N = 4N
    int expectedMin = 8 + (4 * numInstances);

    System.out.println("=== Real ZK Parallel Listener Registration Results ===");
    System.out.println("Live instances: " + numInstances);
    System.out.println("Registered handlers: " + handlerCount);
    System.out.println("Expected minimum: " + expectedMin);
    System.out.println("Leader: " + manager.isLeader());

    Assert.assertTrue(manager.isLeader(), "Controller should be leader");
    Assert.assertTrue(handlerCount >= expectedMin,
        "Expected at least " + expectedMin + " handlers but got " + handlerCount
            + ". Per-instance listeners were NOT registered.");

    System.out.println("PASS: All " + handlerCount + " handlers registered (>= " + expectedMin + " expected)");

    manager.disconnect();
    deleteCluster(clusterName);
    System.out.println("END " + clusterName + " at " + new Date());
  }
}
