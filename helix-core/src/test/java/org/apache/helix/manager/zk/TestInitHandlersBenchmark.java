package org.apache.helix.manager.zk;

import java.util.ArrayList;
import java.util.Date;
import java.util.List;

import org.apache.helix.InstanceType;
import org.apache.helix.TestHelper;
import org.apache.helix.common.ZkTestBase;
import org.apache.helix.integration.manager.ClusterControllerManager;
import org.apache.helix.integration.manager.MockParticipantManager;
import org.testng.Assert;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.Test;


public class TestInitHandlersBenchmark extends ZkTestBase {

  private final List<MockParticipantManager> _participants = new ArrayList<>();
  private ClusterControllerManager _controller;

  @AfterMethod
  public void tearDown() {
    if (_controller != null) {
      _controller.syncStop();
    }
    for (MockParticipantManager p : _participants) {
      p.syncStop();
    }
    _participants.clear();
  }

  @Test
  public void testInitHandlersTimingWithRealZk() throws Exception {
    String clusterName = TestHelper.getTestClassName() + "_" + TestHelper.getTestMethodName();
    int numParticipants = 50;
    int numResources = 20;
    int numPartitions = 10;

    System.out.println("=== initHandlers Benchmark ===");
    System.out.println("Participants: " + numParticipants);
    System.out.println("Resources: " + numResources);
    System.out.println("Partitions per resource: " + numPartitions);
    System.out.println("Expected handlers: ~" + (numParticipants * 4) + " (4 per participant)");

    TestHelper.setupCluster(clusterName, ZK_ADDR, 12918,
        "localhost",
        "TestDB",
        numResources,
        numPartitions,
        numParticipants,
        3,
        "MasterSlave", true);

    // Start participants so the controller has real instances to register watches for
    for (int i = 0; i < numParticipants; i++) {
      MockParticipantManager participant =
          new MockParticipantManager(ZK_ADDR, clusterName, "localhost_" + (12918 + i));
      participant.syncStart();
      _participants.add(participant);
    }

    // Measure controller start time (which triggers initHandlers)
    long start = System.currentTimeMillis();
    _controller = new ClusterControllerManager(ZK_ADDR, clusterName, "controller_0");
    _controller.syncStart();
    long elapsed = System.currentTimeMillis() - start;

    // Verify controller is leader and handlers are ready
    Assert.assertTrue(_controller.isLeader());
    List<CallbackHandler> handlers = _controller.getHandlers();
    Assert.assertNotNull(handlers);
    Assert.assertTrue(handlers.size() > 0, "Expected handlers to be registered");

    int readyCount = 0;
    for (CallbackHandler h : handlers) {
      if (h.isReady()) {
        readyCount++;
      }
    }

    System.out.println("=== Results ===");
    System.out.println("Controller start time: " + elapsed + " ms");
    System.out.println("Total handlers registered: " + handlers.size());
    System.out.println("Ready handlers: " + readyCount);
    System.out.println("All handlers ready: " + (readyCount == handlers.size()));

    Assert.assertEquals(readyCount, handlers.size(), "All handlers should be ready");
  }
}
