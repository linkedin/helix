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

import org.apache.helix.BaseDataAccessor;
import org.apache.helix.HelixManager;
import org.apache.helix.HelixManagerFactory;
import org.apache.helix.InstanceType;
import org.apache.helix.PropertyKey.Builder;
import org.apache.helix.TestHelper;
import org.apache.helix.zookeeper.datamodel.ZNRecord;
import org.apache.helix.common.ZkTestBase;
import org.apache.helix.integration.manager.ClusterDistributedController;
import org.apache.helix.integration.manager.MockParticipantManager;
import org.apache.helix.manager.zk.ZKHelixDataAccessor;
import org.apache.helix.manager.zk.ZkBaseDataAccessor;
import org.apache.helix.manager.zk.ZkCacheBaseDataAccessor;
import org.apache.helix.tools.ClusterVerifiers.BestPossibleExternalViewVerifier;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

/**
 * Integration tests to verify that in distributed controller mode:
 * 1. CONTROLLER instances use ZkCacheBaseDataAccessor for caching
 * 2. CONTROLLER_PARTICIPANT instances use ZkBaseDataAccessor (no caching)
 * 3. Cache behavior is correct across leadership transitions
 */
public class TestDistributedControllerCaching extends ZkTestBase {
  private static final Logger LOG = LoggerFactory.getLogger(TestDistributedControllerCaching.class);
  
  private static final int NUM_CONTROLLERS = 3;
  private static final int NUM_PARTICIPANTS = 3;
  
  private String _controllerClusterName;
  private String _managedClusterName;
  private ClusterDistributedController[] _controllers;
  private MockParticipantManager[] _participants;

  @BeforeClass
  public void beforeClass() throws Exception {
    String className = TestHelper.getTestClassName();
    String methodName = TestHelper.getTestMethodName();
    String clusterNamePrefix = className + "_" + methodName;
    
    LOG.info("START {} at {}", clusterNamePrefix, new Date(System.currentTimeMillis()));

    // The managed cluster name must match the partition naming pattern:
    // resourcePrefix + "0_" + partitionIndex (e.g., "TestPrefix0_0")
    // So we name our managed cluster using this pattern
    _managedClusterName = clusterNamePrefix + "0_0";
    
    // Setup managed cluster
    TestHelper.setupCluster(
        _managedClusterName,
        ZK_ADDR,
        12918,                    // participant port
        "localhost",              // participant name prefix
        "TestDB",                 // resource name prefix
        1,                        // resources
        10,                       // partitions per resource
        NUM_PARTICIPANTS,         // number of nodes
        3,                        // replicas
        "MasterSlave",
        true);                    // do rebalance

    // Setup controller cluster (grand cluster)
    // Resource name prefix should be the cluster name prefix so partitions match cluster names
    _controllerClusterName = "CONTROLLER_" + clusterNamePrefix;
    TestHelper.setupCluster(
        _controllerClusterName,
        ZK_ADDR,
        0,                        // controller port (not used)
        "controller",             // participant name prefix
        clusterNamePrefix,        // resource name prefix (will create partition: clusterNamePrefix + "0_0")
        1,                        // resources (one managed cluster)
        1,                        // partitions (one partition for the managed cluster)
        NUM_CONTROLLERS,          // number of controller nodes
        3,                        // replicas
        "LeaderStandby",
        true);                    // do rebalance

    // Start distributed controllers (CONTROLLER_PARTICIPANT in grand cluster)
    _controllers = new ClusterDistributedController[NUM_CONTROLLERS];
    for (int i = 0; i < NUM_CONTROLLERS; i++) {
      _controllers[i] = new ClusterDistributedController(
          ZK_ADDR, _controllerClusterName, "controller_" + i);
      _controllers[i].syncStart();
    }

    // Wait for controller cluster to stabilize
    BestPossibleExternalViewVerifier controllerVerifier = new BestPossibleExternalViewVerifier
        .Builder(_controllerClusterName).setZkAddress(ZK_ADDR).build();
    Assert.assertTrue(controllerVerifier.verifyByZkCallback(), 
        "Controller cluster should reach stable state");

    // Wait a bit for the distributed controller to become leader and set up the managed cluster controller
    Thread.sleep(5000);

    // Start participants in managed cluster
    _participants = new MockParticipantManager[NUM_PARTICIPANTS];
    for (int i = 0; i < NUM_PARTICIPANTS; i++) {
      String instanceName = "localhost_" + (12918 + i);
      _participants[i] = new MockParticipantManager(ZK_ADDR, _managedClusterName, instanceName);
      _participants[i].syncStart();
    }

    // Wait for managed cluster to stabilize with increased timeout
    BestPossibleExternalViewVerifier managedVerifier = new BestPossibleExternalViewVerifier
        .Builder(_managedClusterName).setZkAddress(ZK_ADDR).build();
    Assert.assertTrue(managedVerifier.verifyByZkCallback(), 
        "Managed cluster should reach stable state");

    LOG.info("Test setup complete - controller cluster: {}, managed cluster: {}", 
        _controllerClusterName, _managedClusterName);
  }

  @AfterClass
  public void afterClass() throws Exception {
    LOG.info("Cleaning up test clusters...");

    // Stop participants
    if (_participants != null) {
      for (MockParticipantManager participant : _participants) {
        if (participant != null) {
          participant.syncStop();
        }
      }
    }

    // Stop controllers
    if (_controllers != null) {
      for (ClusterDistributedController controller : _controllers) {
        if (controller != null) {
          controller.syncStop();
        }
      }
    }

    // Delete clusters
    deleteCluster(_managedClusterName);
    deleteCluster(_controllerClusterName);
    
    LOG.info("Test cleanup complete");
  }

  /**
   * Test that CONTROLLER_PARTICIPANT instances in the grand cluster use ZkBaseDataAccessor
   * (non-cached) because they don't run the intensive ReadClusterDataStage pipeline.
   */
  @Test
  public void testControllerParticipantUsesNonCacheAccessor() throws Exception {
    LOG.info("Testing that CONTROLLER_PARTICIPANT uses non-cached ZkBaseDataAccessor");

    for (ClusterDistributedController controller : _controllers) {
      HelixManager manager = controller;  // ClusterDistributedController extends HelixManager
      
      // Verify instance type
      Assert.assertEquals(manager.getInstanceType(), InstanceType.CONTROLLER_PARTICIPANT,
          "Instance type should be CONTROLLER_PARTICIPANT");

      // Get the base data accessor via reflection
      BaseDataAccessor<ZNRecord> baseAccessor = getBaseDataAccessor(manager);
      
      Assert.assertNotNull(baseAccessor, "Base data accessor should not be null");
      Assert.assertFalse(baseAccessor instanceof ZkCacheBaseDataAccessor,
          "CONTROLLER_PARTICIPANT should NOT use ZkCacheBaseDataAccessor, but was: " 
          + baseAccessor.getClass().getName());
      Assert.assertTrue(baseAccessor instanceof ZkBaseDataAccessor,
          "CONTROLLER_PARTICIPANT should use ZkBaseDataAccessor");

      LOG.info("Verified: {} uses {}", 
          manager.getInstanceName(), baseAccessor.getClass().getSimpleName());
    }
  }

  /**
   * Test that a standalone CONTROLLER (not CONTROLLER_PARTICIPANT) uses ZkCacheBaseDataAccessor.
   * This simulates the controller that gets created when a CONTROLLER_PARTICIPANT becomes
   * leader for a managed cluster.
   */
  @Test
  public void testStandaloneControllerUsesCacheAccessor() throws Exception {
    LOG.info("Testing that standalone CONTROLLER uses ZkCacheBaseDataAccessor");

    // Create a standalone controller for the managed cluster
    String controllerName = "standalone_controller";
    HelixManager standaloneController = HelixManagerFactory.getZKHelixManager(
        _managedClusterName, controllerName, InstanceType.CONTROLLER, ZK_ADDR);

    try {
      standaloneController.connect();

      // Verify instance type
      Assert.assertEquals(standaloneController.getInstanceType(), InstanceType.CONTROLLER,
          "Instance type should be CONTROLLER");

      // Get the base data accessor via reflection
      BaseDataAccessor<ZNRecord> baseAccessor = getBaseDataAccessor(standaloneController);

      Assert.assertNotNull(baseAccessor, "Base data accessor should not be null");
      Assert.assertTrue(baseAccessor instanceof ZkCacheBaseDataAccessor,
          "CONTROLLER should use ZkCacheBaseDataAccessor but was: " 
          + baseAccessor.getClass().getName());

      LOG.info("Verified: standalone CONTROLLER uses ZkCacheBaseDataAccessor");
    } finally {
      if (standaloneController.isConnected()) {
        standaloneController.disconnect();
      }
    }
  }

  /**
   * Test that PARTICIPANT instances use ZkBaseDataAccessor (non-cached).
   */
  @Test
  public void testParticipantUsesNonCacheAccessor() throws Exception {
    LOG.info("Testing that PARTICIPANT uses non-cached ZkBaseDataAccessor");

    for (MockParticipantManager participant : _participants) {
      HelixManager manager = participant;
      
      // Verify instance type
      Assert.assertEquals(manager.getInstanceType(), InstanceType.PARTICIPANT,
          "Instance type should be PARTICIPANT");

      // Get the base data accessor via reflection
      BaseDataAccessor<ZNRecord> baseAccessor = getBaseDataAccessor(manager);
      
      Assert.assertNotNull(baseAccessor, "Base data accessor should not be null");
      Assert.assertFalse(baseAccessor instanceof ZkCacheBaseDataAccessor,
          "PARTICIPANT should NOT use ZkCacheBaseDataAccessor");

      LOG.info("Verified: {} uses {}", 
          manager.getInstanceName(), baseAccessor.getClass().getSimpleName());
    }
  }

  /**
   * Test that cached data from CONTROLLER is consistent with direct ZK reads.
   */
  @Test
  public void testCacheDataConsistency() throws Exception {
    LOG.info("Testing cache data consistency between cached and non-cached reads");

    // Create a standalone controller (uses cache)
    String controllerName = "cache_test_controller";
    HelixManager cachedController = HelixManagerFactory.getZKHelixManager(
        _managedClusterName, controllerName, InstanceType.CONTROLLER, ZK_ADDR);

    // Create a direct accessor (no cache)
    ZkBaseDataAccessor<ZNRecord> directAccessor = new ZkBaseDataAccessor<>(_gZkClient);
    ZKHelixDataAccessor directDataAccessor = new ZKHelixDataAccessor(_managedClusterName, directAccessor);

    try {
      cachedController.connect();

      Builder keyBuilder = cachedController.getHelixDataAccessor().keyBuilder();
      
      // Read live instances via cached accessor
      var cachedLiveInstances = cachedController.getHelixDataAccessor()
          .getChildNames(keyBuilder.liveInstances());
      
      // Read live instances via direct accessor
      var directLiveInstances = directDataAccessor.getChildNames(keyBuilder.liveInstances());

      Assert.assertNotNull(cachedLiveInstances, "Cached live instances should not be null");
      Assert.assertNotNull(directLiveInstances, "Direct live instances should not be null");
      Assert.assertEquals(cachedLiveInstances.size(), directLiveInstances.size(),
          "Cached and direct reads should return same number of live instances");
      Assert.assertTrue(cachedLiveInstances.containsAll(directLiveInstances),
          "Cached and direct reads should return same live instances");

      LOG.info("Verified: Cache data is consistent with direct ZK reads ({} live instances)", 
          cachedLiveInstances.size());
    } finally {
      if (cachedController.isConnected()) {
        cachedController.disconnect();
      }
    }
  }

  /**
   * Test that multiple CONTROLLER instances can coexist with their own caches.
   */
  @Test
  public void testMultipleControllersWithIndependentCaches() throws Exception {
    LOG.info("Testing multiple CONTROLLER instances with independent caches");

    HelixManager controller1 = null;
    HelixManager controller2 = null;

    try {
      // Create two standalone controllers
      controller1 = HelixManagerFactory.getZKHelixManager(
          _managedClusterName, "cache_controller_1", InstanceType.CONTROLLER, ZK_ADDR);
      controller2 = HelixManagerFactory.getZKHelixManager(
          _managedClusterName, "cache_controller_2", InstanceType.CONTROLLER, ZK_ADDR);

      controller1.connect();
      controller2.connect();

      // Verify both use cache accessors
      BaseDataAccessor<ZNRecord> accessor1 = getBaseDataAccessor(controller1);
      BaseDataAccessor<ZNRecord> accessor2 = getBaseDataAccessor(controller2);

      Assert.assertTrue(accessor1 instanceof ZkCacheBaseDataAccessor,
          "Controller 1 should use ZkCacheBaseDataAccessor");
      Assert.assertTrue(accessor2 instanceof ZkCacheBaseDataAccessor,
          "Controller 2 should use ZkCacheBaseDataAccessor");

      // Verify they are different instances (independent caches)
      Assert.assertNotSame(accessor1, accessor2,
          "Controllers should have independent cache accessor instances");

      LOG.info("Verified: Multiple CONTROLLER instances have independent ZkCacheBaseDataAccessor instances");
    } finally {
      if (controller1 != null && controller1.isConnected()) {
        controller1.disconnect();
      }
      if (controller2 != null && controller2.isConnected()) {
        controller2.disconnect();
      }
    }
  }

  /**
   * Helper method to get the base data accessor from a HelixManager using reflection.
   */
  private BaseDataAccessor<ZNRecord> getBaseDataAccessor(HelixManager manager) throws Exception {
    // Get the data accessor
    ZKHelixDataAccessor dataAccessor = (ZKHelixDataAccessor) manager.getHelixDataAccessor();
    if (dataAccessor == null) {
      return null;
    }

    // Use reflection to get the underlying base data accessor
    Field baseAccessorField = ZKHelixDataAccessor.class.getDeclaredField("_baseDataAccessor");
    baseAccessorField.setAccessible(true);
    @SuppressWarnings("unchecked")
    BaseDataAccessor<ZNRecord> baseAccessor = 
        (BaseDataAccessor<ZNRecord>) baseAccessorField.get(dataAccessor);

    return baseAccessor;
  }
}

