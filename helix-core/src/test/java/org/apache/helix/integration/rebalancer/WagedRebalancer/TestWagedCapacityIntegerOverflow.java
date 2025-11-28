package org.apache.helix.integration.rebalancer.WagedRebalancer;

import java.util.ArrayList;
import java.util.Date;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import com.google.common.collect.ImmutableMap;
import org.apache.helix.HelixDataAccessor;
import org.apache.helix.TestHelper;
import org.apache.helix.common.ZkTestBase;
import org.apache.helix.controller.rebalancer.waged.AssignmentMetadataStore;
import org.apache.helix.integration.manager.ClusterControllerManager;
import org.apache.helix.integration.manager.MockParticipantManager;
import org.apache.helix.manager.zk.ZKHelixDataAccessor;
import org.apache.helix.model.BuiltInStateModelDefinitions;
import org.apache.helix.model.ClusterConfig;
import org.apache.helix.model.ExternalView;
import org.apache.helix.model.IdealState;
import org.apache.helix.model.InstanceConfig;
import org.apache.helix.model.ResourceConfig;
import org.apache.helix.tools.ClusterVerifiers.StrictMatchExternalViewVerifier;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

/**
 * Integration test for integer overflow fix in WAGED rebalancer capacity calculations.
 *
 * This test verifies that the WAGED rebalancer correctly handles capacity values that
 * would cause integer overflow when aggregated across many instances.
 *
 * Background:
 * - Prior to the fix, capacity was calculated using Integer (32-bit signed)
 * - Integer.MAX_VALUE = 2,147,483,647
 * - When total cluster capacity exceeded this, it wrapped to negative values
 * - This caused "insufficient capacity" errors even in nearly empty clusters
 *
 * Test Scenario:
 * - Creates 10 instances with 250,000,000 DISK capacity each
 * - Total DISK: 10 × 250,000,000 = 2,500,000,000 > Integer.MAX_VALUE
 * - With the Long-based fix, this should work correctly
 * - Without the fix, capacity would wrap to negative and cause failures
 *
 * @see https://github.com/apache/helix/issues/3084
 */
public class TestWagedCapacityIntegerOverflow extends ZkTestBase {
  private static final Logger LOG = LoggerFactory.getLogger(TestWagedCapacityIntegerOverflow.class);

  // Test configuration
  protected final int NUM_NODE = 10;
  protected static final int START_PORT = 14000;
  protected static final int NUM_RESOURCES = 5;
  protected static final int PARTITIONS_PER_RESOURCE = 10;
  protected static final int REPLICAS = 3;

  // Capacity values that will cause overflow when summed
  // 10 instances × 250,000,000 = 2,500,000,000 > Integer.MAX_VALUE (2,147,483,647)
  private static final int DISK_CAPACITY = 250_000_000;
  private static final int CU_CAPACITY = 13_000;
  private static final int PARTCOUNT_CAPACITY = 800;

  // Partition weights (small values)
  private static final int DISK_WEIGHT = 1000;
  private static final int CU_WEIGHT = 10;
  private static final int PARTCOUNT_WEIGHT = 1;

  protected final String CLASS_NAME = getShortClassName();
  protected final String CLUSTER_NAME = CLUSTER_PREFIX + "_" + CLASS_NAME;
  protected ClusterControllerManager _controller;
  protected AssignmentMetadataStore _assignmentMetadataStore;
  protected StrictMatchExternalViewVerifier _clusterVerifier;

  List<MockParticipantManager> _participants = new ArrayList<>();
  List<String> _nodes = new ArrayList<>();
  private Set<String> _allDBs = new HashSet<>();

  @BeforeClass
  public void beforeClass() throws Exception {
    LOG.info("START " + CLASS_NAME + " at " + new Date(System.currentTimeMillis()));

    // Create cluster
    _gSetupTool.addCluster(CLUSTER_NAME, true);

    // Configure cluster for WAGED rebalancer with high capacity values
    HelixDataAccessor dataAccessor = new ZKHelixDataAccessor(CLUSTER_NAME, _baseAccessor);
    ClusterConfig clusterConfig = dataAccessor.getProperty(dataAccessor.keyBuilder().clusterConfig());

    // Set instance capacity keys
    List<String> capacityKeys = new ArrayList<>();
    capacityKeys.add("CU");
    capacityKeys.add("DISK");
    capacityKeys.add("PARTCOUNT");
    clusterConfig.setInstanceCapacityKeys(capacityKeys);

    // Set default instance capacity with values that will overflow when summed
    Map<String, Integer> defaultInstanceCapacity = ImmutableMap.of(
        "CU", CU_CAPACITY,
        "DISK", DISK_CAPACITY,  // This is the critical value for overflow test
        "PARTCOUNT", PARTCOUNT_CAPACITY
    );
    clusterConfig.setDefaultInstanceCapacityMap(defaultInstanceCapacity);

    // Set default partition weights (small values)
    Map<String, Integer> defaultPartitionWeight = ImmutableMap.of(
        "CU", CU_WEIGHT,
        "DISK", DISK_WEIGHT,
        "PARTCOUNT", PARTCOUNT_WEIGHT
    );
    clusterConfig.setDefaultPartitionWeightMap(defaultPartitionWeight);

    dataAccessor.setProperty(dataAccessor.keyBuilder().clusterConfig(), clusterConfig);

    // Log the configuration for verification
    long totalDiskCapacity = (long) NUM_NODE * DISK_CAPACITY;
    LOG.info("Cluster configuration:");
    LOG.info("  Instances: {}", NUM_NODE);
    LOG.info("  DISK capacity per instance: {}", DISK_CAPACITY);
    LOG.info("  Total DISK capacity: {}", totalDiskCapacity);
    LOG.info("  Integer.MAX_VALUE: {}", Integer.MAX_VALUE);
    LOG.info("  Overflow test: {} > {} = {}",
        totalDiskCapacity, Integer.MAX_VALUE, totalDiskCapacity > Integer.MAX_VALUE);

    // Verify overflow condition
    Assert.assertTrue(totalDiskCapacity > Integer.MAX_VALUE,
        "Total capacity must exceed Integer.MAX_VALUE to test overflow fix");

    // Add instances
    for (int i = 0; i < NUM_NODE; i++) {
      String storageNodeName = PARTICIPANT_PREFIX + "_" + (START_PORT + i);
      _gSetupTool.addInstanceToCluster(CLUSTER_NAME, storageNodeName);
      _nodes.add(storageNodeName);

      // Optionally set per-instance capacity (to test both default and per-instance configs)
      if (i % 2 == 0) {
        InstanceConfig instanceConfig = dataAccessor.getProperty(
            dataAccessor.keyBuilder().instanceConfig(storageNodeName));
        Map<String, Integer> instanceCapacity = ImmutableMap.of(
            "CU", CU_CAPACITY,
            "DISK", DISK_CAPACITY + (i * 1000),  // Slightly vary capacity
            "PARTCOUNT", PARTCOUNT_CAPACITY
        );
        instanceConfig.setInstanceCapacityMap(instanceCapacity);
        dataAccessor.setProperty(dataAccessor.keyBuilder().instanceConfig(storageNodeName),
            instanceConfig);
      }
    }

    // Start participants
    for (String node : _nodes) {
      MockParticipantManager participant = new MockParticipantManager(ZK_ADDR, CLUSTER_NAME, node);
      participant.syncStart();
      _participants.add(participant);
    }

    // Start controller
    String controllerName = CONTROLLER_PREFIX + "_0";
    _controller = new ClusterControllerManager(ZK_ADDR, CLUSTER_NAME, controllerName);
    _controller.syncStart();

    // Enable persist best possible assignment
    enablePersistBestPossibleAssignment(_gZkClient, CLUSTER_NAME, true);

    // Initialize cluster verifier
    _clusterVerifier = new StrictMatchExternalViewVerifier.Builder(CLUSTER_NAME)
        .setZkClient(_gZkClient)
        .setDeactivatedNodeAwareness(true)
        .setResources(_allDBs)
        .setWaitTillVerify(TestHelper.DEFAULT_REBALANCE_PROCESSING_WAIT_TIME)
        .build();
  }

  @AfterClass
  public void afterClass() throws Exception {
    // Stop participants
    for (MockParticipantManager participant : _participants) {
      if (participant != null && participant.isConnected()) {
        participant.syncStop();
      }
    }
    _participants.clear();

    // Stop controller
    if (_controller != null && _controller.isConnected()) {
      _controller.syncStop();
    }

    // Stop verifier
    if (_clusterVerifier != null) {
      _clusterVerifier.close();
    }

    // Delete cluster
    deleteCluster(CLUSTER_NAME);

    LOG.info("END " + CLASS_NAME + " at " + new Date(System.currentTimeMillis()));
  }

  /**
   * Test that verifies rebalancing works correctly when total capacity exceeds Integer.MAX_VALUE.
   *
   * This test would FAIL with the old Integer-based capacity calculation due to overflow,
   * but should SUCCEED with the Long-based fix.
   */
  @Test
  public void testRebalanceWithCapacityOverflow() throws Exception {
    LOG.info("Starting testRebalanceWithCapacityOverflow");

    // Create resources with WAGED rebalancer
    for (int i = 0; i < NUM_RESOURCES; i++) {
      String dbName = "TestDB_" + i;
      createResourceWithWagedRebalance(CLUSTER_NAME, dbName,
          BuiltInStateModelDefinitions.MasterSlave.name(),
          PARTITIONS_PER_RESOURCE, REPLICAS, REPLICAS);
      _allDBs.add(dbName);
      _gSetupTool.rebalanceStorageCluster(CLUSTER_NAME, dbName, REPLICAS);

      // Set partition capacity for the resource
      HelixDataAccessor dataAccessor = new ZKHelixDataAccessor(CLUSTER_NAME, _baseAccessor);
      ResourceConfig resourceConfig = new ResourceConfig(dbName);
      Map<String, Integer> partitionCapacity = ImmutableMap.of(
          "CU", CU_WEIGHT,
          "DISK", DISK_WEIGHT,
          "PARTCOUNT", PARTCOUNT_WEIGHT
      );

      try {
        Map<String, Map<String, Integer>> partitionCapacityMap = new java.util.HashMap<>();
        partitionCapacityMap.put(ResourceConfig.DEFAULT_PARTITION_KEY, partitionCapacity);
        resourceConfig.setPartitionCapacityMap(partitionCapacityMap);
        dataAccessor.setProperty(dataAccessor.keyBuilder().resourceConfig(dbName), resourceConfig);
      } catch (Exception e) {
        LOG.warn("Failed to set partition capacity for {}: {}", dbName, e.getMessage());
      }
    }

    // Update verifier with all resources
    _clusterVerifier.close();
    _clusterVerifier = new StrictMatchExternalViewVerifier.Builder(CLUSTER_NAME)
        .setZkClient(_gZkClient)
        .setDeactivatedNodeAwareness(true)
        .setResources(_allDBs)
        .setWaitTillVerify(TestHelper.DEFAULT_REBALANCE_PROCESSING_WAIT_TIME)
        .build();

    // Verify cluster reaches stable state
    // This is the key assertion - rebalancing should succeed despite capacity > Integer.MAX_VALUE
    Assert.assertTrue(_clusterVerifier.verifyByPolling(),
        "Cluster should reach stable state with capacity exceeding Integer.MAX_VALUE");

    // Verify all resources have correct external view
    HelixDataAccessor dataAccessor = new ZKHelixDataAccessor(CLUSTER_NAME, _baseAccessor);
    for (String dbName : _allDBs) {
      IdealState idealState = dataAccessor.getProperty(
          dataAccessor.keyBuilder().idealStates(dbName));
      ExternalView externalView = dataAccessor.getProperty(
          dataAccessor.keyBuilder().externalView(dbName));

      Assert.assertNotNull(idealState, "IdealState should exist for " + dbName);
      Assert.assertNotNull(externalView, "ExternalView should exist for " + dbName);
      Assert.assertEquals(externalView.getPartitionSet().size(), PARTITIONS_PER_RESOURCE,
          "All partitions should be assigned for " + dbName);

      // Verify each partition has correct number of replicas
      for (String partition : externalView.getPartitionSet()) {
        Map<String, String> stateMap = externalView.getStateMap(partition);
        Assert.assertNotNull(stateMap, "State map should exist for partition " + partition);
        Assert.assertEquals(stateMap.size(), REPLICAS,
            "Partition " + partition + " should have " + REPLICAS + " replicas");

        // Verify there's exactly one MASTER
        long masterCount = stateMap.values().stream()
            .filter(state -> "MASTER".equals(state))
            .count();
        Assert.assertEquals(masterCount, 1,
            "Partition " + partition + " should have exactly one MASTER");
      }
    }

    LOG.info("Successfully verified cluster with total DISK capacity {} > Integer.MAX_VALUE ({})",
        (long) NUM_NODE * DISK_CAPACITY, Integer.MAX_VALUE);
  }

  /**
   * Test adding a new resource after initial rebalancing still works correctly.
   * Verifies that capacity overflow fix works for incremental changes.
   */
  @Test
  public void testAddResourceWithOverflowCapacity() throws Exception {
    LOG.info("Starting testAddResourceWithOverflowCapacity");

    String newDbName = "TestDB_New";
    createResourceWithWagedRebalance(CLUSTER_NAME, newDbName,
        BuiltInStateModelDefinitions.MasterSlave.name(),
        PARTITIONS_PER_RESOURCE, REPLICAS, REPLICAS);
    _gSetupTool.rebalanceStorageCluster(CLUSTER_NAME, newDbName, REPLICAS);
    _allDBs.add(newDbName);

    // Verify cluster still reaches stable state
    HelixDataAccessor dataAccessor = new ZKHelixDataAccessor(CLUSTER_NAME, _baseAccessor);
    Assert.assertTrue(TestHelper.verify(() -> {
      ExternalView externalView = dataAccessor.getProperty(
          dataAccessor.keyBuilder().externalView(newDbName));
      return externalView != null &&
          externalView.getPartitionSet().size() == PARTITIONS_PER_RESOURCE;
    }, TestHelper.WAIT_DURATION),
        "New resource should be assigned correctly despite capacity overflows from integer limit");

    LOG.info("Successfully added new resource");
  }
}

