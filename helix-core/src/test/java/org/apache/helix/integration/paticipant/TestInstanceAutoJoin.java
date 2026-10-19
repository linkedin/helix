package org.apache.helix.integration.paticipant;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import org.apache.helix.ConfigAccessor;
import org.apache.helix.HelixDataAccessor;
import org.apache.helix.HelixException;
import org.apache.helix.HelixManager;
import org.apache.helix.HelixManagerProperty;
import org.apache.helix.InstanceType;
import org.apache.helix.PropertyKey;
import org.apache.helix.TestHelper;
import org.apache.helix.api.cloud.CloudInstanceInformation;
import org.apache.helix.cloud.constants.CloudProvider;
import org.apache.helix.controller.rebalancer.strategy.CrushEdRebalanceStrategy;
import org.apache.helix.integration.common.ZkStandAloneCMTestBase;
import org.apache.helix.integration.manager.MockParticipantManager;
import org.apache.helix.manager.zk.ZKHelixManager;
import org.apache.helix.model.CloudConfig;
import org.apache.helix.model.ClusterConfig;
import org.apache.helix.model.ConfigScope;
import org.apache.helix.model.HelixConfigScope;
import org.apache.helix.model.IdealState.RebalanceMode;
import org.apache.helix.model.InstanceConfig;
import org.apache.helix.model.builder.ConfigScopeBuilder;
import org.apache.helix.model.builder.HelixConfigScopeBuilder;
import org.apache.helix.util.ConfigStringUtil;
import org.testng.Assert;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

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

public class TestInstanceAutoJoin extends ZkStandAloneCMTestBase {
  String db2 = TEST_DB + "2";
  String db3 = TEST_DB + "3";

  @DataProvider
  public Object[][] cloudAutoRegistrationModes() {
    return new Object[][] {{false}, {true}};
  }

  @Test(dataProvider = "cloudAutoRegistrationModes")
  public void testAutoRegistrationWithLegacyCloudEventMetadata(boolean cloudEnabled)
      throws Exception {
    String cluster = CLUSTER_NAME + "_LegacyCloudEvent_" + cloudEnabled;
    String instance = "localhost_279703";
    _gSetupTool.addCluster(cluster, true);
    ZKHelixManager participant = null;
    try {
      ConfigAccessor configAccessor = new ConfigAccessor(_gZkClient);
      ClusterConfig clusterConfig = configAccessor.getClusterConfig(cluster);
      clusterConfig.getRecord().setBooleanField(ZKHelixManager.ALLOW_PARTICIPANT_AUTO_JOIN, true);
      Map<String, String> disabledInfo = new HashMap<>();
      disabledInfo.put("HELIX_DISABLED_TYPE", "CLOUD_EVENT");
      disabledInfo.put("HELIX_DISABLED_REASON", "legacy cloud event");
      disabledInfo.put("HELIX_ENABLED_DISABLE_TIMESTAMP", "1");
      Map<String, String> legacyEvents =
          Collections.singletonMap(instance, ConfigStringUtil.concatenateMapping(disabledInfo));
      clusterConfig.getRecord().setMapField("DISABLED_INSTANCES_WITH_INFO", legacyEvents);
      configAccessor.setClusterConfig(cluster, clusterConfig);

      CloudConfig cloudConfig =
          new CloudConfig.Builder().setCloudEnabled(cloudEnabled)
              .setCloudProvider(CloudProvider.CUSTOMIZED)
              .setCloudInfoProcessorPackageName("org.apache.helix.integration.paticipant")
              .setCloudInfoProcessorName("CustomCloudInstanceInformationProcessor")
              .setCloudInfoSources(Collections.singletonList("https://cloud.com")).build();
      _gSetupTool.getClusterManagementTool().addCloudConfig(cluster, cloudConfig);

      participant = new ZKHelixManager(cluster, instance, InstanceType.PARTICIPANT, ZK_ADDR);
      PropertyKey.Builder keyBuilder = new PropertyKey.Builder(cluster);
      for (int cycle = 0; cycle < 2; cycle++) {
        participant.connect();
        Assert.assertTrue(participant.isConnected());
        Assert.assertNotNull(
            participant.getHelixDataAccessor().getProperty(keyBuilder.liveInstance(instance)));
        InstanceConfig instanceConfig = configAccessor.getInstanceConfig(cluster, instance);
        Assert.assertEquals(instanceConfig.getInstanceEnabled(), cycle == 0,
            "Reconnecting must preserve a manual disable");
        if (cloudEnabled) {
          Assert.assertEquals(instanceConfig.getDomainAsString(),
              CustomCloudInstanceInformation._cloudInstanceInfo
                  .get(CloudInstanceInformation.CloudInstanceField.FAULT_DOMAIN.name()));
          Assert.assertEquals(instanceConfig.getInstanceInfoMap(),
              CustomCloudInstanceInformation._cloudInstanceInfo);
        } else {
          Assert.assertNull(instanceConfig.getDomainAsString());
          Assert.assertTrue(instanceConfig.getInstanceInfoMap().isEmpty());
        }
        Assert.assertFalse(_gSetupTool.getClusterManagementTool().isInMaintenanceMode(cluster));
        Assert.assertEquals(configAccessor.getClusterConfig(cluster).getRecord()
            .getMapField("DISABLED_INSTANCES_WITH_INFO"), legacyEvents);
        if (cycle == 0) {
          _gSetupTool.getClusterManagementTool().enableInstance(cluster, instance, false);
        }
        participant.disconnect();
        Assert.assertTrue(TestHelper.verify(
            () -> !_gZkClient.exists(keyBuilder.liveInstance(instance).getPath()), 2000));
      }
    } finally {
      if (participant != null) {
        participant.disconnect();
      }
      _gSetupTool.deleteCluster(cluster);
    }
  }

  @Test
  public void testInstanceAutoJoin() throws Exception {
    HelixManager manager = _participants[0];
    HelixDataAccessor accessor = manager.getHelixDataAccessor();

    _gSetupTool.addResourceToCluster(CLUSTER_NAME, db2, 60, "OnlineOffline",
        RebalanceMode.FULL_AUTO.name(), CrushEdRebalanceStrategy.class.getName());

    _gSetupTool.rebalanceStorageCluster(CLUSTER_NAME, db2, 1);
    String instance2 = "localhost_279699";
    // StartCMResult result = TestHelper.startDummyProcess(ZK_ADDR, CLUSTER_NAME, instance2);
    MockParticipantManager newParticipant =
        new MockParticipantManager(ZK_ADDR, CLUSTER_NAME, instance2);
    newParticipant.syncStart();

    Thread.sleep(500);
    // Assert.assertFalse(result._thread.isAlive());
    Assert.assertNull(manager.getHelixDataAccessor().getProperty(accessor.keyBuilder().liveInstance(instance2)));

    ConfigScope scope = new ConfigScopeBuilder().forCluster(CLUSTER_NAME).build();

    manager.getConfigAccessor().set(scope, ZKHelixManager.ALLOW_PARTICIPANT_AUTO_JOIN, "true");

    newParticipant = new MockParticipantManager(ZK_ADDR, CLUSTER_NAME, instance2);
    newParticipant.syncStart();

    Thread.sleep(500);
    // Assert.assertTrue(result._thread.isAlive() || result2._thread.isAlive());
    for (int i = 0; i < 20; i++) {
      if (null == manager.getHelixDataAccessor()
          .getProperty(accessor.keyBuilder().liveInstance(instance2))) {
        Thread.sleep(100);
      } else {
        break;
      }
    }
    Assert.assertNotNull(
        manager.getHelixDataAccessor().getProperty(accessor.keyBuilder().liveInstance(instance2)));

    newParticipant.syncStop();
  }

  /**
   * Test auto join with a defaultInstanceConfig.
   * @throws Exception
   */
  @Test(dependsOnMethods = "testInstanceAutoJoin")
  public void testAutoJoinWithDefaultInstanceConfig() throws Exception {
    HelixManager manager = _participants[0];
    HelixDataAccessor accessor = manager.getHelixDataAccessor();
    String instance3 = "localhost_279700";

    // Enable cluster auto join.
    HelixConfigScope scope =
        new HelixConfigScopeBuilder(HelixConfigScope.ConfigScopeProperty.CLUSTER).forCluster(
            CLUSTER_NAME).build();
    manager.getConfigAccessor().set(scope, ZKHelixManager.ALLOW_PARTICIPANT_AUTO_JOIN, "true");

    // Create and start a new participant with default instance config.
    InstanceConfig.Builder defaultInstanceConfig =
        new InstanceConfig.Builder().setInstanceEnabled(false).addTag("foo");
    MockParticipantManager autoParticipant =
        new MockParticipantManager(ZK_ADDR, CLUSTER_NAME, instance3, 10, null,
            new HelixManagerProperty.Builder().setDefaultInstanceConfigBuilder(
                defaultInstanceConfig).build());
    autoParticipant.syncStart();

    Assert.assertTrue(TestHelper.verify(() -> {
      // Check that live instance is added and instance config is populated with correct fields.
      if (manager.getHelixDataAccessor().getProperty(accessor.keyBuilder().liveInstance(instance3))
          == null) {
        return false;
      }
      InstanceConfig composedInstanceConfig =
          manager.getConfigAccessor().getInstanceConfig(CLUSTER_NAME, instance3);
      return !composedInstanceConfig.getInstanceEnabled() && composedInstanceConfig.getTags()
          .contains("foo");
    }, 2000));

    autoParticipant.syncStop();
  }

  @Test(dependsOnMethods = "testInstanceAutoJoin")
  public void testAutoRegistration() throws Exception {
    // Create CloudConfig object and add to config
    CloudConfig.Builder cloudConfigBuilder = new CloudConfig.Builder();
    cloudConfigBuilder.setCloudEnabled(true);
    cloudConfigBuilder.setCloudProvider(CloudProvider.AZURE);
    CloudConfig cloudConfig = cloudConfigBuilder.build();

    HelixManager manager = _participants[0];
    HelixDataAccessor accessor = manager.getHelixDataAccessor();

    _gSetupTool.addResourceToCluster(CLUSTER_NAME, db3, 60, "OnlineOffline", RebalanceMode.FULL_AUTO.name(), CrushEdRebalanceStrategy.class.getName());
    _gSetupTool.rebalanceStorageCluster(CLUSTER_NAME, db3, 1);
    String instance4 = "localhost_279701";

    ConfigScope scope = new ConfigScopeBuilder().forCluster(CLUSTER_NAME).build();

    manager.getConfigAccessor().set(scope, ZKHelixManager.ALLOW_PARTICIPANT_AUTO_JOIN, "true");
    // Write the CloudConfig to Zookeeper
    PropertyKey.Builder keyBuilder = accessor.keyBuilder();
    accessor.setProperty(keyBuilder.cloudConfig(), cloudConfig);

    MockParticipantManager autoParticipant =
        new MockParticipantManager(ZK_ADDR, CLUSTER_NAME, instance4);
    autoParticipant.syncStart();

    // if the test is run in cloud environment, auto registration will succeed and live instance
    // will be added, otherwise, auto registration will fail and instance config will not be
    // populated. An exception will be thrown.
    try {
      manager.getConfigAccessor().getInstanceConfig(CLUSTER_NAME, instance4);
      Assert.assertTrue(TestHelper.verify(() -> {
        if (null == manager.getHelixDataAccessor()
            .getProperty(accessor.keyBuilder().liveInstance(instance4))) {
          return false;
        }
        return true;
      }, 2000));
    } catch (HelixException e) {
      Assert.assertNull(manager.getHelixDataAccessor()
          .getProperty(accessor.keyBuilder().liveInstance(instance4)));
    }

    autoParticipant.syncStop();
  }

  /**
   * Test auto registration with customized cloud info processor specified with fully qualified
   * class name.
   * @throws Exception
   */
  @Test(dependsOnMethods = "testAutoRegistration")
  public void testAutoRegistrationCustomizedFullyQualifiedInfoProcessorPath() throws Exception {
    HelixManager manager = _participants[0];
    HelixDataAccessor accessor = manager.getHelixDataAccessor();
    String instance5 = "localhost_279702";

    // Enable cluster auto join.
    HelixConfigScope scope =
        new HelixConfigScopeBuilder(HelixConfigScope.ConfigScopeProperty.CLUSTER).forCluster(
            CLUSTER_NAME).build();
    manager.getConfigAccessor().set(scope, ZKHelixManager.ALLOW_PARTICIPANT_AUTO_JOIN, "true");

    // Create CloudConfig object for CUSTOM cloud provider.
    CloudConfig cloudConfig =
        new CloudConfig.Builder().setCloudEnabled(true).setCloudProvider(CloudProvider.CUSTOMIZED)
            .setCloudInfoProcessorPackageName("org.apache.helix.integration.paticipant")
            .setCloudInfoProcessorName("CustomCloudInstanceInformationProcessor")
            .setCloudInfoSources(Collections.singletonList("https://cloud.com")).build();

    // Update CloudConfig to Zookeeper.
    PropertyKey.Builder keyBuilder = accessor.keyBuilder();
    accessor.setProperty(keyBuilder.cloudConfig(), cloudConfig);

    // Create and start a new participant.
    MockParticipantManager autoParticipant =
        new MockParticipantManager(ZK_ADDR, CLUSTER_NAME, instance5);
    autoParticipant.syncStart();

    Assert.assertTrue(TestHelper.verify(() -> {
      // Check that live instance is added and instance config is populated with correct domain.
      return null != manager.getHelixDataAccessor()
          .getProperty(accessor.keyBuilder().liveInstance(instance5)) && manager.getConfigAccessor()
          .getInstanceConfig(CLUSTER_NAME, instance5).getDomainAsString().equals(
              CustomCloudInstanceInformation._cloudInstanceInfo.get(
                  CloudInstanceInformation.CloudInstanceField.FAULT_DOMAIN.name()))
          && manager.getConfigAccessor().getInstanceConfig(CLUSTER_NAME, instance5)
          .getInstanceInfoMap().equals(CustomCloudInstanceInformation._cloudInstanceInfo);
    }, 2000));

    autoParticipant.syncStop();
  }
}
