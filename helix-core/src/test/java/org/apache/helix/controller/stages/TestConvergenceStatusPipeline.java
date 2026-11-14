package org.apache.helix.controller.stages;

/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

import java.util.ArrayList;
import java.util.List;

import org.apache.helix.ConfigAccessor;
import org.apache.helix.HelixDataAccessor;
import org.apache.helix.PropertyKey;
import org.apache.helix.TestHelper;
import org.apache.helix.common.ZkTestBase;
import org.apache.helix.integration.manager.ClusterControllerManager;
import org.apache.helix.integration.manager.MockParticipantManager;
import org.apache.helix.model.ClusterConfig;
import org.apache.helix.model.ConvergenceStatus;
import org.apache.helix.model.ConvergenceStatus.Status;
import org.apache.helix.model.IdealState;
import org.testng.Assert;
import org.testng.annotations.Test;

/** A real controller publishes convergence reports by default, and stops when the flag is off. */
public class TestConvergenceStatusPipeline extends ZkTestBase {
  @Test
  public void testControllerPublishesWhenEnabledAndStopsWhenDisabled() throws Exception {
    String cluster = "CONV_" + getShortClassName();
    _gSetupTool.addCluster(cluster, true);
    ConfigAccessor configs = new ConfigAccessor(_gZkClient);
    ClusterConfig enabled = configs.getClusterConfig(cluster);
    enabled.setConvergenceMonitoringEnabled(true);
    configs.setClusterConfig(cluster, enabled);
    List<MockParticipantManager> participants = new ArrayList<>();
    for (int i = 0; i < 3; i++) {
      String name = "localhost_" + (12918 + i);
      participants.add(addParticipant(cluster, name));
    }
    _gSetupTool.addResourceToCluster(cluster, "DB", 8, "MasterSlave");
    _gSetupTool.rebalanceStorageCluster(cluster, "DB", 2);
    // A FULL_AUTO resource too: a fully placed one must not read as short of replicas.
    _gSetupTool.addResourceToCluster(cluster, "AUTO", 8, "MasterSlave",
        IdealState.RebalanceMode.FULL_AUTO.name());
    _gSetupTool.rebalanceStorageCluster(cluster, "AUTO", 2);
    ClusterControllerManager controller =
        new ClusterControllerManager(ZK_ADDR, cluster, "controller_0");
    controller.syncStart();

    HelixDataAccessor accessor = controller.getHelixDataAccessor();
    PropertyKey.Builder keys = accessor.keyBuilder();
    Assert.assertTrue(TestHelper.verify(() -> {
      ConvergenceStatus root = accessor.getProperty(keys.convergenceStatus());
      return root != null && root.getStatus() == Status.CONVERGED;
    }, TestHelper.WAIT_DURATION));
    ConvergenceStatus root = accessor.getProperty(keys.convergenceStatus());
    Assert.assertTrue(root.getListedResources().isEmpty());
    Assert.assertEquals(root.getPartitionCount(), 16);
    Assert.assertEquals(root.getControllerSessionId(), controller.getSessionId());
    Assert.assertEquals(root.getControllerName(), controller.getInstanceName());

    ClusterConfig config = configs.getClusterConfig(cluster);
    config.setConvergenceMonitoringEnabled(false);
    configs.setClusterConfig(cluster, config);
    _gSetupTool.addResourceToCluster(cluster, "DB2", 4, "MasterSlave");
    _gSetupTool.rebalanceStorageCluster(cluster, "DB2", 2);
    Assert.assertTrue(TestHelper.verify(
        () -> accessor.getProperty(keys.externalView("DB2")) != null, TestHelper.WAIT_DURATION));
    Thread.sleep(2000);
    ConvergenceStatus after = accessor.getProperty(keys.convergenceStatus());
    // Not reported on once disabled: the record still covers only the first two resources.
    Assert.assertEquals(after.getResourceCount(), 2);

    controller.syncStop();
    participants.forEach(MockParticipantManager::syncStop);
    deleteCluster(cluster);
  }
}
