package org.apache.helix.rest.server;

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

import java.io.IOException;
import java.util.Arrays;
import java.util.Map;
import java.util.Set;
import javax.ws.rs.client.Entity;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;

import com.fasterxml.jackson.databind.JsonNode;
import org.apache.helix.ConfigAccessor;
import org.apache.helix.model.ExternalView;
import org.apache.helix.model.ResourceConfig;
import org.apache.helix.rest.server.resources.helix.InstancesAccessor;
import org.apache.helix.rest.server.util.JerseyUriRequestBuilder;
import org.testng.Assert;
import org.testng.annotations.Test;

/**
 * Integration test for custom active states configuration in min active replica check.
 * 
 * This test uses the STOPPABLE_CLUSTER2 which has:
 * - 15 instances (instance0-14) spread across 5 zones
 * - Resources with MasterSlave state model
 * - minActiveReplicas = 2, replicas = 3
 * 
 * We test that custom active states configuration actually affects stoppability decisions.
 */
public class TestCustomActiveStatesStoppableCheck extends AbstractTestClass {
  
  @Test
  public void testWithDefaultBehavior_BothMasterAndSlaveCountAsActive() throws IOException, InterruptedException {
    System.out.println("Start test: testWithDefaultBehavior_BothMasterAndSlaveCountAsActive");
    
    String clusterName = STOPPABLE_CLUSTER2;
    String resourceName = clusterName + "_db_0";
    
    // Ensure NO custom active states configured - use default behavior
    ConfigAccessor configAccessor = new ConfigAccessor(_gZkClient);
    ResourceConfig resourceConfig = configAccessor.getResourceConfig(clusterName, resourceName);
    if (resourceConfig != null) {
      resourceConfig.setActiveStatesForMinActiveReplicaCheck(null);
      configAccessor.setResourceConfig(clusterName, resourceName, resourceConfig);
    }
    
    Thread.sleep(1000);
    
    // Get actual external view to see what replicas exist
    ExternalView ev = _gSetupTool.getClusterManagementTool().getResourceExternalView(clusterName, resourceName);
    System.out.println("External View for " + resourceName + ": " + ev.getRecord().toString());
    
    // Find instance with Slave - but cluster may use "SLAVE" not "Slave"
    String instanceWithSlave = null;
    for (String partition : ev.getPartitionSet()) {
      Map<String, String> stateMap = ev.getStateMap(partition);
      System.out.println("Partition " + partition + " states: " + stateMap);
      for (Map.Entry<String, String> entry : stateMap.entrySet()) {
        if ("SLAVE".equalsIgnoreCase(entry.getValue())) {
          instanceWithSlave = entry.getKey();
          break;
        }
      }
      if (instanceWithSlave != null) break;
    }
    
    Assert.assertNotNull(instanceWithSlave, "Could not find instance with SLAVE replica");
    System.out.println("Testing stoppability of instance with SLAVE: " + instanceWithSlave);
    
    String content = String.format(
        "{\"%s\":[\"%s\"]}",
        InstancesAccessor.InstancesProperties.instances.name(),
        instanceWithSlave);
    
    Response response = new JerseyUriRequestBuilder(
        "clusters/{}/instances?command=stoppable&skipHealthCheckCategories=CUSTOM_INSTANCE_CHECK,CUSTOM_PARTITION_CHECK")
        .format(clusterName)
        .post(this, Entity.entity(content, MediaType.APPLICATION_JSON_TYPE));
    
    JsonNode jsonNode = OBJECT_MAPPER.readTree(response.readEntity(String.class));
    System.out.println("Response: " + jsonNode.toString());
    
    Set<String> stoppableInstances = getStringSet(jsonNode,
        InstancesAccessor.InstancesProperties.instance_stoppable_parallel.name());
    
    // With default behavior (both MASTER and SLAVE count as active),
    // the instance with SLAVE should be stoppable
    Assert.assertTrue(stoppableInstances.contains(instanceWithSlave),
        "Instance with SLAVE should be stoppable with default behavior. Stoppable: " + stoppableInstances);
    
    System.out.println("✓ Default behavior: Instance with SLAVE is stoppable (MASTER + SLAVE both count as active)");
    System.out.println("End test: testWithDefaultBehavior_BothMasterAndSlaveCountAsActive");
  }
  
  @Test(dependsOnMethods = "testWithDefaultBehavior_BothMasterAndSlaveCountAsActive")
  public void testWithCustomActiveStates_OnlyMasterCountsAsActive() throws IOException, InterruptedException {
    System.out.println("Start test: testWithCustomActiveStates_OnlyMasterCountsAsActive");
    
    String clusterName = STOPPABLE_CLUSTER2;
    String resourceName = clusterName + "_db_0";
    
    // Configure custom active states - ONLY MASTER counts as active
    ConfigAccessor configAccessor = new ConfigAccessor(_gZkClient);
    ResourceConfig resourceConfig = configAccessor.getResourceConfig(clusterName, resourceName);
    if (resourceConfig == null) {
      resourceConfig = new ResourceConfig(resourceName);
    }
    resourceConfig.setActiveStatesForMinActiveReplicaCheck(Arrays.asList("MASTER"));
    configAccessor.setResourceConfig(clusterName, resourceName, resourceConfig);
    
    Thread.sleep(1000);
    
    // Verify configuration
    ResourceConfig verifyConfig = configAccessor.getResourceConfig(clusterName, resourceName);
    Assert.assertEquals(verifyConfig.getActiveStatesForMinActiveReplicaCheck(), Arrays.asList("MASTER"));
    System.out.println("✓ Successfully configured custom active states: [MASTER]");
    
    // Try to check stoppability of multiple instances
    ExternalView ev = _gSetupTool.getClusterManagementTool().getResourceExternalView(clusterName, resourceName);
    String instanceWithMaster = null;
    String instanceWithSlave = null;
    
    for (String partition : ev.getPartitionSet()) {
      Map<String, String> stateMap = ev.getStateMap(partition);
      for (Map.Entry<String, String> entry : stateMap.entrySet()) {
        if ("MASTER".equalsIgnoreCase(entry.getValue()) && instanceWithMaster == null) {
          instanceWithMaster = entry.getKey();
        }
        if ("SLAVE".equalsIgnoreCase(entry.getValue()) && instanceWithSlave == null) {
          instanceWithSlave = entry.getKey();
        }
      }
      if (instanceWithMaster != null && instanceWithSlave != null) break;
    }
    
    Assert.assertNotNull(instanceWithMaster, "Could not find instance with MASTER");
    Assert.assertNotNull(instanceWithSlave, "Could not find instance with SLAVE");
    
    // Test instance with MASTER
    System.out.println("Testing stoppability of instance with MASTER: " + instanceWithMaster);
    String content = String.format(
        "{\"%s\":[\"%s\"]}",
        InstancesAccessor.InstancesProperties.instances.name(),
        instanceWithMaster);
    
    Response response = new JerseyUriRequestBuilder(
        "clusters/{}/instances?command=stoppable&skipHealthCheckCategories=CUSTOM_INSTANCE_CHECK,CUSTOM_PARTITION_CHECK")
        .format(clusterName)
        .post(this, Entity.entity(content, MediaType.APPLICATION_JSON_TYPE));
    
    JsonNode jsonNode = OBJECT_MAPPER.readTree(response.readEntity(String.class));
    System.out.println("Response for instance with MASTER: " + jsonNode.toString());
    
    JsonNode nonStoppableInstances = jsonNode.get(
        InstancesAccessor.InstancesProperties.instance_not_stoppable_with_reasons.name());
    
    // Instance with MASTER should NOT be stoppable
    Assert.assertTrue(nonStoppableInstances.has(instanceWithMaster),
        "Instance with MASTER should NOT be stoppable when only MASTER counts as active");
    Assert.assertTrue(nonStoppableInstances.get(instanceWithMaster).toString().contains("MIN_ACTIVE_REPLICA_CHECK_FAILED"),
        "Should fail with MIN_ACTIVE_REPLICA_CHECK_FAILED");
    
    System.out.println("✓ Custom behavior: Instance with MASTER is NOT stoppable (only 1 active < minActiveReplicas=2)");
    
    // Test instance with SLAVE
    System.out.println("Testing stoppability of instance with SLAVE: " + instanceWithSlave);
    content = String.format(
        "{\"%s\":[\"%s\"]}",
        InstancesAccessor.InstancesProperties.instances.name(),
        instanceWithSlave);
    
    response = new JerseyUriRequestBuilder(
        "clusters/{}/instances?command=stoppable&skipHealthCheckCategories=CUSTOM_INSTANCE_CHECK,CUSTOM_PARTITION_CHECK")
        .format(clusterName)
        .post(this, Entity.entity(content, MediaType.APPLICATION_JSON_TYPE));
    
    jsonNode = OBJECT_MAPPER.readTree(response.readEntity(String.class));
    System.out.println("Response for instance with SLAVE: " + jsonNode.toString());
    
    // With only 1 MASTER counting as active and minActiveReplicas=2,
    // even the SLAVE instance should NOT be stoppable because the partition
    // is already in a degraded state (1 < 2)
    nonStoppableInstances = jsonNode.get(
        InstancesAccessor.InstancesProperties.instance_not_stoppable_with_reasons.name());
    
    Assert.assertTrue(nonStoppableInstances.has(instanceWithSlave),
        "Instance with SLAVE should also NOT be stoppable when partition only has 1 active (MASTER) but needs 2");
    Assert.assertTrue(nonStoppableInstances.get(instanceWithSlave).toString().contains("MIN_ACTIVE_REPLICA_CHECK_FAILED"),
        "Should fail with MIN_ACTIVE_REPLICA_CHECK_FAILED");
    
    System.out.println("✓ Custom behavior: Instance with SLAVE is also NOT stoppable (partition has 1 active < minActiveReplicas=2)");
    System.out.println("✓ Summary: When only MASTER counts as active, with just 1 MASTER per partition, NO instance is stoppable!");
    
    System.out.println("End test: testWithCustomActiveStates_OnlyMasterCountsAsActive");
  }
  
  @Test(dependsOnMethods = "testWithCustomActiveStates_OnlyMasterCountsAsActive")
  public void testRevertToDefaultBehavior() throws IOException, InterruptedException {
    System.out.println("Start test: testRevertToDefaultBehavior");
    
    String clusterName = STOPPABLE_CLUSTER2;
    String resourceName = clusterName + "_db_0";
    
    // Clear custom active states - revert to default
    ConfigAccessor configAccessor = new ConfigAccessor(_gZkClient);
    ResourceConfig resourceConfig = configAccessor.getResourceConfig(clusterName, resourceName);
    resourceConfig.setActiveStatesForMinActiveReplicaCheck(null);
    configAccessor.setResourceConfig(clusterName, resourceName, resourceConfig);
    
    Thread.sleep(1000);
    
    // Verify cleared
    ResourceConfig verifyConfig = configAccessor.getResourceConfig(clusterName, resourceName);
    Assert.assertNull(verifyConfig.getActiveStatesForMinActiveReplicaCheck());
    System.out.println("✓ Successfully cleared custom active states");
    
    // Now behavior should be back to default
    ExternalView ev = _gSetupTool.getClusterManagementTool().getResourceExternalView(clusterName, resourceName);
    String instanceWithSlave = null;
    for (String partition : ev.getPartitionSet()) {
      Map<String, String> stateMap = ev.getStateMap(partition);
      for (Map.Entry<String, String> entry : stateMap.entrySet()) {
        if ("SLAVE".equalsIgnoreCase(entry.getValue())) {
          instanceWithSlave = entry.getKey();
          break;
        }
      }
      if (instanceWithSlave != null) break;
    }
    
    String content = String.format(
        "{\"%s\":[\"%s\"]}",
        InstancesAccessor.InstancesProperties.instances.name(),
        instanceWithSlave);
    
    Response response = new JerseyUriRequestBuilder(
        "clusters/{}/instances?command=stoppable&skipHealthCheckCategories=CUSTOM_INSTANCE_CHECK,CUSTOM_PARTITION_CHECK")
        .format(clusterName)
        .post(this, Entity.entity(content, MediaType.APPLICATION_JSON_TYPE));
    
    JsonNode jsonNode = OBJECT_MAPPER.readTree(response.readEntity(String.class));
    System.out.println("Response after reverting: " + jsonNode.toString());
    
    Set<String> stoppableInstances = getStringSet(jsonNode,
        InstancesAccessor.InstancesProperties.instance_stoppable_parallel.name());
    
    // Should be stoppable again with default behavior
    Assert.assertTrue(stoppableInstances.contains(instanceWithSlave),
        "After reverting to default, instance with SLAVE should be stoppable again");
    
    System.out.println("✓ After reverting: Instance with SLAVE is stoppable again (default behavior restored)");
    System.out.println("End test: testRevertToDefaultBehavior");
  }
  
  @Test
  public void testConfigPersistsInZooKeeper() throws IOException {
    System.out.println("Start test: testConfigPersistsInZooKeeper");
    
    String clusterName = STOPPABLE_CLUSTER2;
    String resourceName = clusterName + "_db_1";
    
    ConfigAccessor configAccessor = new ConfigAccessor(_gZkClient);
    
    // Set custom active states
    ResourceConfig resourceConfig = new ResourceConfig(resourceName);
    resourceConfig.setActiveStatesForMinActiveReplicaCheck(Arrays.asList("MASTER", "SLAVE"));
    configAccessor.setResourceConfig(clusterName, resourceName, resourceConfig);
    
    // Read back
    ResourceConfig readConfig = configAccessor.getResourceConfig(clusterName, resourceName);
    Assert.assertNotNull(readConfig.getActiveStatesForMinActiveReplicaCheck());
    Assert.assertEquals(readConfig.getActiveStatesForMinActiveReplicaCheck().size(), 2);
    Assert.assertTrue(readConfig.getActiveStatesForMinActiveReplicaCheck().contains("MASTER"));
    Assert.assertTrue(readConfig.getActiveStatesForMinActiveReplicaCheck().contains("SLAVE"));
    
    System.out.println("✓ Configuration persisted in ZooKeeper: " + 
        readConfig.getActiveStatesForMinActiveReplicaCheck());
    
    // Clean up
    resourceConfig.setActiveStatesForMinActiveReplicaCheck(null);
    configAccessor.setResourceConfig(clusterName, resourceName, resourceConfig);
    
    System.out.println("End test: testConfigPersistsInZooKeeper");
  }
  
  private Set<String> getStringSet(JsonNode jsonNode, String key) {
    Set<String> result = new java.util.HashSet<>();
    jsonNode.withArray(key).forEach(s -> result.add(s.textValue()));
    return result;
  }
}
