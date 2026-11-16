package org.apache.helix.controller;

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

import org.apache.helix.HelixManager;
import org.apache.helix.PropertyPathBuilder;
import org.apache.helix.TestHelper;
import org.apache.helix.common.ZkTestBase;
import org.apache.helix.integration.manager.ClusterControllerManager;
import org.apache.helix.manager.zk.ZKHelixAdmin;
import org.apache.helix.manager.zk.ZKHelixDataAccessor;
import org.apache.helix.manager.zk.ZkBaseDataAccessor;
import org.testng.Assert;
import org.testng.annotations.Test;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class TestInstanceGarbageCollector extends ZkTestBase {
  private static final long GRACE = InstanceGarbageCollector.GARBAGE_GRACE_PERIOD_MS;

  // A subtree without an InstanceConfig is deleted once it has been seen for the grace period.
  // Instances with a config and live instances are left alone.
  @Test
  public void testDeletesLeftoverSubtreeAfterGracePeriod() {
    String clusterName = CLUSTER_PREFIX + "_" + TestHelper.getTestMethodName();
    _gSetupTool.addCluster(clusterName, true);
    _gSetupTool.addInstanceToCluster(clusterName, "host_kept");
    createLeftoverSubtree(clusterName, "host_leftover");
    createLeftoverSubtree(clusterName, "host_leftover_live");
    _gZkClient.createPersistent(
        PropertyPathBuilder.liveInstance(clusterName, "host_leftover_live"));
    ClockedCollector collector = new ClockedCollector();
    HelixManager manager = mockManager(clusterName, true);

    collector.runAt(0, manager);
    assertSubtreeExists(clusterName, "host_leftover", true);
    collector.runAt(GRACE - 1, manager);
    assertSubtreeExists(clusterName, "host_leftover", true);
    collector.runAt(GRACE, manager);

    assertSubtreeExists(clusterName, "host_leftover", false);
    assertSubtreeExists(clusterName, "host_leftover_live", true);
    assertSubtreeExists(clusterName, "host_kept", true);
    _gZkClient.delete(PropertyPathBuilder.liveInstance(clusterName, "host_leftover_live"));
    deleteCluster(clusterName);
  }

  // A leftover re-added, or come live, after it was first seen is not deleted.
  @Test
  public void testKeepsInstanceReAddedOrLiveSinceFirstSeen() {
    String clusterName = CLUSTER_PREFIX + "_" + TestHelper.getTestMethodName();
    _gSetupTool.addCluster(clusterName, true);
    createLeftoverSubtree(clusterName, "host_readded");
    createLeftoverSubtree(clusterName, "host_came_live");
    ClockedCollector collector = new ClockedCollector();
    HelixManager manager = mockManager(clusterName, true);

    collector.runAt(0, manager);
    _gZkClient.createPersistent(PropertyPathBuilder.instanceConfig(clusterName, "host_readded"));
    _gZkClient.createPersistent(PropertyPathBuilder.liveInstance(clusterName, "host_came_live"));
    collector.runAt(GRACE, manager);

    assertSubtreeExists(clusterName, "host_readded", true);
    assertSubtreeExists(clusterName, "host_came_live", true);
    _gZkClient.delete(PropertyPathBuilder.liveInstance(clusterName, "host_came_live"));
    deleteCluster(clusterName);
  }

  // Only the leader controller cleans up; a controller that lost leadership forgets what it saw, so
  // a later leadership restarts the grace period.
  @Test
  public void testOnlyLeaderCollects() {
    String clusterName = CLUSTER_PREFIX + "_" + TestHelper.getTestMethodName();
    _gSetupTool.addCluster(clusterName, true);
    createLeftoverSubtree(clusterName, "host_leftover");
    ClockedCollector collector = new ClockedCollector();

    collector.runAt(0, mockManager(clusterName, true));
    collector.runAt(GRACE, mockManager(clusterName, false));
    assertSubtreeExists(clusterName, "host_leftover", true);
    collector.runAt(GRACE + 1, mockManager(clusterName, true));
    assertSubtreeExists(clusterName, "host_leftover", true);
    collector.runAt(2 * GRACE + 1, mockManager(clusterName, true));
    assertSubtreeExists(clusterName, "host_leftover", false);
    deleteCluster(clusterName);
  }

  // The leader controller schedules the cleanup on its own, without any cluster event.
  @Test
  public void testLeaderControllerSchedulesCollection() throws Exception {
    String clusterName = CLUSTER_PREFIX + "_" + TestHelper.getTestMethodName();
    _gSetupTool.addCluster(clusterName, true);
    ClusterControllerManager controller =
        new ClusterControllerManager(ZK_ADDR, clusterName, "controller_0");
    controller.syncStart();
    try {
      Assert.assertTrue(TestHelper.verify(() -> {
        GenericHelixController leader = GenericHelixController.getLeaderController(clusterName);
        return leader != null && leader.isInstanceGarbageCollectionRunning();
      }, TestHelper.WAIT_DURATION));
    } finally {
      controller.syncStop();
    }
    deleteCluster(clusterName);
  }

  private static class ClockedCollector extends InstanceGarbageCollector {
    private long _now;

    @Override
    long getCurrentTime() {
      return _now;
    }

    void runAt(long now, HelixManager manager) {
      _now = now;
      collect(manager);
    }
  }

  private static HelixManager mockManager(String clusterName, boolean isLeader) {
    HelixManager manager = mock(HelixManager.class);
    when(manager.isConnected()).thenReturn(true);
    when(manager.isLeader()).thenReturn(isLeader);
    when(manager.getClusterName()).thenReturn(clusterName);
    when(manager.getHelixDataAccessor()).thenReturn(
        new ZKHelixDataAccessor(clusterName, new ZkBaseDataAccessor<>(_gZkClient)));
    when(manager.getClusterManagmentTool()).thenReturn(new ZKHelixAdmin(_gZkClient));
    return manager;
  }

  // Simulates a dropInstance that deleted the InstanceConfig but failed before the subtree.
  private void createLeftoverSubtree(String clusterName, String instanceName) {
    _gSetupTool.addInstanceToCluster(clusterName, instanceName);
    _gZkClient.delete(PropertyPathBuilder.instanceConfig(clusterName, instanceName));
  }

  private void assertSubtreeExists(String clusterName, String instanceName, boolean expected) {
    Assert.assertEquals(_gZkClient.exists(PropertyPathBuilder.instance(clusterName, instanceName)),
        expected, instanceName);
  }
}
