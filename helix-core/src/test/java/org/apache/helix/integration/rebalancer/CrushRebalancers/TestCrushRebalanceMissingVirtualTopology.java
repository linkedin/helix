package org.apache.helix.integration.rebalancer.CrushRebalancers;

import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.helix.ConfigAccessor;
import org.apache.helix.common.ZkTestBase;
import org.apache.helix.controller.rebalancer.strategy.CrushRebalanceStrategy;
import org.apache.helix.integration.manager.ClusterControllerManager;
import org.apache.helix.integration.manager.MockParticipantManager;
import org.apache.helix.model.BuiltInStateModelDefinitions;
import org.apache.helix.model.ClusterConfig;
import org.apache.helix.model.ExternalView;
import org.apache.helix.model.IdealState.RebalanceMode;
import org.apache.helix.model.InstanceConfig;
import org.apache.helix.tools.ClusterVerifiers.BestPossibleExternalViewVerifier;
import org.apache.helix.tools.ClusterVerifiers.ZkHelixClusterVerifier;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

/**
 * Test case reproducing the Venice production incident where missing virtual topology configuration
 * leads to disproportionate partition assignment in CRUSH rebalancer.
 *
 * Background: Venice HAR cluster with topology /mz_virtualZone/host/applicationInstanceId
 * had one instance join with missing mz_virtualZone key, causing it to receive 3.26x more
 * partitions than expected (2,166 vs 664) due to becoming a singleton fault zone.
 */
public class TestCrushRebalanceMissingVirtualTopology extends ZkTestBase {

  private final String className = getShortClassName();
  private final String clusterName = CLUSTER_PREFIX + "_" + className;

  // Venice-like configuration
  private static final String TOPOLOGY = "/mz_virtualZone/host/applicationInstanceId";
  private static final String FAULT_ZONE_TYPE = "mz_virtualZone";
  private static final int PARTITIONS = 500;  // Reduced for more dramatic effect
  private static final int REPLICAS = 3;
  private static final String DB_NAME = "VeniceTest";

  private ClusterControllerManager controller;
  private List<MockParticipantManager> participants = new ArrayList<>();
  private List<String> nodes = new ArrayList<>();
  private Set<String> allDBs = new HashSet<>();

  @BeforeClass
  @Override
  public void beforeClass() throws Exception {
    super.beforeClass();
    System.out.println("START " + className + " at " + new Date(System.currentTimeMillis()));

    _gSetupTool.addCluster(clusterName, true);

    // Configure Venice-like virtual topology cluster
    ConfigAccessor configAccessor = new ConfigAccessor(_gZkClient);
    ClusterConfig clusterConfig = configAccessor.getClusterConfig(clusterName);
    clusterConfig.setTopology(TOPOLOGY);
    clusterConfig.setFaultZoneType(FAULT_ZONE_TYPE);
    clusterConfig.setTopologyAwareEnabled(true);
    configAccessor.setClusterConfig(clusterName, clusterConfig);

    // Add "good" instances with proper virtual topology configuration
    addGoodInstances();

    // Add the "bad" instance with missing virtual topology key
    addBadInstance();

    // Start all participants
    for (String node : nodes) {
      MockParticipantManager participant = new MockParticipantManager(ZK_ADDR, clusterName, node);
      participant.syncStart();
      participants.add(participant);
    }

    // Start controller
    String controllerName = CONTROLLER_PREFIX + "_0";
    controller = new ClusterControllerManager(ZK_ADDR, clusterName, controllerName);
    controller.syncStart();

    enablePersistBestPossibleAssignment(_gZkClient, clusterName, true);
    enableTopologyAwareRebalance(_gZkClient, clusterName, true);
  }

  /**
   * Add "good" instances that have proper virtual topology configuration.
   * These instances will be distributed across multiple virtual zones as expected.
   */
  private void addGoodInstances() {
    ConfigAccessor configAccessor = new ConfigAccessor(_gZkClient);

    // Add instances across 2 virtual zones with fewer instances to amplify the singleton effect
    int portBase = 12000;
    int globalAppId = 0; // Global counter to ensure unique applicationInstanceId
    for (int zoneId = 0; zoneId < 2; zoneId++) {
      for (int hostId = 0; hostId < 2; hostId++) {
        // Use hostname_port format so Helix can properly parse hostname and port
        int port = portBase + (zoneId * 100) + (hostId * 10);
        String instanceName = "host_" + zoneId + "_" + hostId + "_" + port;
        nodes.add(instanceName);

        _gSetupTool.addInstanceToCluster(clusterName, instanceName);

        // Get the existing config and modify it
        InstanceConfig instanceConfig = configAccessor.getInstanceConfig(clusterName, instanceName);

        // Set the critical virtual topology configuration
        // Use globalAppId to ensure unique applicationInstanceId (leaf node must be globally unique)
        instanceConfig.setDomain("mz_virtualZone=zone_" + zoneId + ",host=host_" + zoneId + "_" + hostId
            + ",applicationInstanceId=app_" + globalAppId);

        _gSetupTool.getClusterManagementTool().setInstanceConfig(clusterName, instanceName, instanceConfig);
        globalAppId++; // Increment for next instance
      }
    }
  }

  /**
   * Add the "bad" instance that is missing the virtual topology key.
   * This simulates the Venice production incident where one instance joined
   * without proper mz_virtualZone configuration.
   */
  private void addBadInstance() {
    // Use hostname_port format so Helix can properly parse hostname and port
    String badInstanceName = "bad_host_9999";
    nodes.add(badInstanceName);

    _gSetupTool.addInstanceToCluster(clusterName, badInstanceName);

    // Get the existing config and modify it
    ConfigAccessor configAccessor = new ConfigAccessor(_gZkClient);
    InstanceConfig badInstanceConfig = configAccessor.getInstanceConfig(clusterName, badInstanceName);

    // This is the problem: missing mz_virtualZone in domain configuration
    // When mz_virtualZone is missing, Topology.java will assign default value
    // "Helix_default_mz_virtualZone", creating a singleton fault zone
    // Use unique applicationInstanceId (leaf node must be globally unique)
    badInstanceConfig.setDomain("host=bad_host,applicationInstanceId=bad_app_999");

    _gSetupTool.getClusterManagementTool().setInstanceConfig(clusterName, badInstanceName, badInstanceConfig);
  }

  @Test
  public void testMissingVirtualTopologyOverAllocation() {
    // Add resource with CRUSH rebalancer strategy
    _gSetupTool.addResourceToCluster(clusterName, DB_NAME, PARTITIONS,
        BuiltInStateModelDefinitions.OnlineOffline.name(),
        RebalanceMode.FULL_AUTO.name(), CrushRebalanceStrategy.class.getName());
    allDBs.add(DB_NAME);

    _gSetupTool.rebalanceStorageCluster(clusterName, DB_NAME, REPLICAS);

    // Wait for cluster to stabilize
    ZkHelixClusterVerifier verifier = new BestPossibleExternalViewVerifier.Builder(clusterName)
        .setZkClient(_gZkClient).setResources(allDBs).build();
    Assert.assertTrue(verifier.verifyByPolling());

    // Analyze partition distribution
    ExternalView externalView = _gSetupTool.getClusterManagementTool().getResourceExternalView(clusterName, DB_NAME);
    Map<String, Integer> partitionCounts = countPartitionsPerInstance(externalView);

    // Print distribution for debugging
    System.out.println("Partition distribution:");
    for (Map.Entry<String, Integer> entry : partitionCounts.entrySet()) {
      System.out.println("  " + entry.getKey() + ": " + entry.getValue() + " partitions");
    }

    // Validate the Venice incident: bad instance should get disproportionate allocation
    String badInstanceName = "bad_host_9999";
    int badInstancePartitions = partitionCounts.getOrDefault(badInstanceName, 0);

    // Calculate expected partitions per instance if distributed evenly
    int totalInstances = nodes.size();
    int expectedPartitionsPerInstance = (PARTITIONS * REPLICAS) / totalInstances;

    System.out.println("Expected partitions per instance: " + expectedPartitionsPerInstance);
    System.out.println("Bad instance got: " + badInstancePartitions);

    // The bad instance should receive significantly more than expected due to singleton fault zone
    // In Venice's case, it was 3.26x more (2,166 vs 664)
    // With CRUSH and singleton fault zone, we expect at least 1.5x over-allocation
    double overAllocationRatio = (double) badInstancePartitions / expectedPartitionsPerInstance;
    System.out.println("Over-allocation ratio: " + overAllocationRatio + "x");

    Assert.assertTrue(overAllocationRatio > 1.5,
        "Bad instance should receive significantly more partitions due to singleton fault zone. " +
            "Expected >1.5x, got " + overAllocationRatio + "x");

    // Verify that good instances have more balanced distribution
    for (String instance : nodes) {
      if (!instance.equals(badInstanceName)) {
        int goodInstancePartitions = partitionCounts.getOrDefault(instance, 0);
        double goodRatio = (double) goodInstancePartitions / expectedPartitionsPerInstance;
        Assert.assertTrue(goodRatio < 1.5,
            "Good instances should have more balanced allocation. Instance " + instance +
                " has ratio " + goodRatio + "x");
      }
    }

    // Demonstrate the root cause: verify topology mapping shows default values
    validateTopologyMapping();
  }

  /**
   * Count how many partitions each instance is assigned to.
   */
  private Map<String, Integer> countPartitionsPerInstance(ExternalView externalView) {
    Map<String, Integer> counts = new HashMap<>();

    for (String partition : externalView.getPartitionSet()) {
      Map<String, String> stateMap = externalView.getStateMap(partition);
      for (String instance : stateMap.keySet()) {
        counts.put(instance, counts.getOrDefault(instance, 0) + 1);
      }
    }

    return counts;
  }

  /**
   * Validate that the topology mapping shows the root cause of the issue.
   * The bad instance should have default values for missing topology keys.
   */
  private void validateTopologyMapping() {
    ConfigAccessor configAccessor = new ConfigAccessor(_gZkClient);

    // Check instance configurations to see the topology mapping
    for (String instance : nodes) {
      InstanceConfig config = configAccessor.getInstanceConfig(clusterName, instance);
      String domain = config.getDomainAsString();
      System.out.println("Instance " + instance + " domain: " + domain);

      if (instance.contains("bad_host")) {
        // The bad instance should be missing mz_virtualZone in its domain
        Assert.assertFalse(domain.contains("mz_virtualZone"),
            "Bad instance should not have mz_virtualZone configured");
      } else {
        // Good instances should have proper mz_virtualZone
        Assert.assertTrue(domain.contains("mz_virtualZone"),
            "Good instances should have mz_virtualZone configured");
      }
    }
  }

  @AfterClass
  public void afterClass() {
    if (controller != null && controller.isConnected()) {
      controller.syncStop();
    }
    for (MockParticipantManager participant : participants) {
      if (participant != null && participant.isConnected()) {
        participant.syncStop();
      }
    }
    deleteCluster(clusterName);
    System.out.println("END " + className + " at " + new Date(System.currentTimeMillis()));
  }
}