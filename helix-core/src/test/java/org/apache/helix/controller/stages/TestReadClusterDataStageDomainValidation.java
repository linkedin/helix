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
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import javax.management.MBeanServerConnection;
import javax.management.ObjectName;

import org.apache.helix.controller.dataproviders.BaseControllerDataProvider;
import org.apache.helix.manager.zk.ZKHelixManager;
import org.apache.helix.model.ClusterConfig;
import org.apache.helix.model.InstanceConfig;
import org.apache.helix.monitoring.mbeans.ClusterStatusMonitor;
import org.apache.helix.monitoring.mbeans.MonitorDomainNames;
import org.mockito.Mockito;
import org.testng.Assert;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import static org.mockito.Mockito.when;


public class TestReadClusterDataStageDomainValidation {
  private static final String CLUSTER_NAME = "TestCluster_DomainValidation";
  private static final MBeanServerConnection _server = ManagementFactory.getPlatformMBeanServer();

  private ClusterStatusMonitor _monitor;
  private ClusterConfig _clusterConfig;
  private BaseControllerDataProvider _dataProvider;

  @BeforeMethod
  public void beforeMethod() throws Exception {
    _monitor = new ClusterStatusMonitor(CLUSTER_NAME);
    _monitor.active();

    _clusterConfig = new ClusterConfig(CLUSTER_NAME);
    _dataProvider = Mockito.mock(BaseControllerDataProvider.class);
  }

  @AfterMethod
  public void afterMethod() {
    _monitor.reset();
  }

  @Test
  public void testSkipsWhenAutoJoinDisabled() throws Exception {
    Map<String, InstanceConfig> instanceConfigMap = new HashMap<>();
    instanceConfigMap.put("instance_0", new InstanceConfig("instance_0"));
    when(_dataProvider.getInstanceConfigMap()).thenReturn(instanceConfigMap);

    registerInstances(instanceConfigMap);

    ReadClusterDataStage.validateAndReportInstanceDomainInfo(
        _clusterConfig, _dataProvider, _monitor);

    assertDomainInfoValidGauge("instance_0", 1L);
  }

  @Test
  public void testSkipsWhenMonitorIsNull() {
    _clusterConfig.getRecord().setSimpleField(ZKHelixManager.ALLOW_PARTICIPANT_AUTO_JOIN, "true");

    ReadClusterDataStage.validateAndReportInstanceDomainInfo(
        _clusterConfig, _dataProvider, null);
  }

  @Test
  public void testSkipsWhenClusterConfigIsNull() {
    ReadClusterDataStage.validateAndReportInstanceDomainInfo(
        null, _dataProvider, _monitor);
  }

  @Test
  public void testAllValidWhenTopologyAwareDisabled() throws Exception {
    _clusterConfig.getRecord().setSimpleField(ZKHelixManager.ALLOW_PARTICIPANT_AUTO_JOIN, "true");

    Map<String, InstanceConfig> instanceConfigMap = new HashMap<>();
    InstanceConfig config = new InstanceConfig("instance_0");
    instanceConfigMap.put("instance_0", config);
    when(_dataProvider.getInstanceConfigMap()).thenReturn(instanceConfigMap);

    registerInstances(instanceConfigMap);

    ReadClusterDataStage.validateAndReportInstanceDomainInfo(
        _clusterConfig, _dataProvider, _monitor);

    assertDomainInfoValidGauge("instance_0", 1L);
  }

  @Test
  public void testDetectsInvalidDomainWithTopologyAware() throws Exception {
    _clusterConfig.getRecord().setSimpleField(ZKHelixManager.ALLOW_PARTICIPANT_AUTO_JOIN, "true");
    _clusterConfig.setTopologyAwareEnabled(true);
    _clusterConfig.setTopology("/zone/rack/host/instance");
    _clusterConfig.setFaultZoneType("zone");

    Map<String, InstanceConfig> instanceConfigMap = new HashMap<>();

    InstanceConfig validConfig = new InstanceConfig("instance_valid");
    validConfig.setDomain("zone=us-west-1,rack=rack1,host=host1,instance=instance_valid");
    instanceConfigMap.put("instance_valid", validConfig);

    InstanceConfig invalidConfig = new InstanceConfig("instance_invalid");
    invalidConfig.setDomain("rack=rack2,host=host2,instance=instance_invalid");
    instanceConfigMap.put("instance_invalid", invalidConfig);

    InstanceConfig emptyDomainConfig = new InstanceConfig("instance_empty");
    instanceConfigMap.put("instance_empty", emptyDomainConfig);

    when(_dataProvider.getInstanceConfigMap()).thenReturn(instanceConfigMap);

    registerInstances(instanceConfigMap);

    ReadClusterDataStage.validateAndReportInstanceDomainInfo(
        _clusterConfig, _dataProvider, _monitor);

    assertDomainInfoValidGauge("instance_valid", 1L);
    assertDomainInfoValidGauge("instance_invalid", 0L);
    assertDomainInfoValidGauge("instance_empty", 0L);
  }

  @Test
  public void testDetectsInvalidWithLegacyTopology() throws Exception {
    _clusterConfig.getRecord().setSimpleField(ZKHelixManager.ALLOW_PARTICIPANT_AUTO_JOIN, "true");
    _clusterConfig.setTopologyAwareEnabled(true);

    Map<String, InstanceConfig> instanceConfigMap = new HashMap<>();

    InstanceConfig validConfig = new InstanceConfig("instance_with_zone");
    validConfig.setZoneId("us-west-1");
    instanceConfigMap.put("instance_with_zone", validConfig);

    InstanceConfig invalidConfig = new InstanceConfig("instance_no_zone");
    instanceConfigMap.put("instance_no_zone", invalidConfig);

    when(_dataProvider.getInstanceConfigMap()).thenReturn(instanceConfigMap);

    registerInstances(instanceConfigMap);

    ReadClusterDataStage.validateAndReportInstanceDomainInfo(
        _clusterConfig, _dataProvider, _monitor);

    assertDomainInfoValidGauge("instance_with_zone", 1L);
    assertDomainInfoValidGauge("instance_no_zone", 0L);
  }

  @Test
  public void testRecoveryFromInvalidToValid() throws Exception {
    _clusterConfig.getRecord().setSimpleField(ZKHelixManager.ALLOW_PARTICIPANT_AUTO_JOIN, "true");
    _clusterConfig.setTopologyAwareEnabled(true);
    _clusterConfig.setTopology("/zone/instance");
    _clusterConfig.setFaultZoneType("zone");

    Map<String, InstanceConfig> instanceConfigMap = new HashMap<>();

    InstanceConfig config = new InstanceConfig("instance_0");
    config.setDomain("instance=instance_0");
    instanceConfigMap.put("instance_0", config);
    when(_dataProvider.getInstanceConfigMap()).thenReturn(instanceConfigMap);

    registerInstances(instanceConfigMap);

    ReadClusterDataStage.validateAndReportInstanceDomainInfo(
        _clusterConfig, _dataProvider, _monitor);
    assertDomainInfoValidGauge("instance_0", 0L);

    config.setDomain("zone=us-west-1,instance=instance_0");

    ReadClusterDataStage.validateAndReportInstanceDomainInfo(
        _clusterConfig, _dataProvider, _monitor);
    assertDomainInfoValidGauge("instance_0", 1L);
  }

  private void registerInstances(Map<String, InstanceConfig> instanceConfigMap) {
    _monitor.setClusterInstanceStatus(
        instanceConfigMap.keySet(), instanceConfigMap.keySet(),
        Collections.emptySet(), Collections.emptyMap(), Collections.emptyMap(),
        Collections.emptyMap(), Collections.emptyMap(), Collections.emptyMap(),
        Collections.emptyMap());
  }

  private void assertDomainInfoValidGauge(String instanceName, long expectedValue)
      throws Exception {
    ObjectName objName = new ObjectName(String.format("%s:cluster=%s,instanceName=%s",
        MonitorDomainNames.ClusterStatus.name(), CLUSTER_NAME, instanceName));
    Assert.assertTrue(_server.isRegistered(objName),
        "MBean not registered for " + instanceName);
    Object value = _server.getAttribute(objName, "DomainInfoValidGauge");
    Assert.assertTrue(value instanceof Long);
    Assert.assertEquals((long) value, expectedValue,
        "DomainInfoValidGauge mismatch for " + instanceName);
  }
}
