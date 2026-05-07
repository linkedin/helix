package org.apache.helix.controller.dataproviders;

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

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import com.google.common.collect.ImmutableSet;
import org.apache.helix.constants.InstanceConstants;
import org.apache.helix.model.ClusterConfig;
import org.apache.helix.model.InstanceConfig;
import org.apache.helix.model.LiveInstance;
import org.testng.Assert;
import org.testng.annotations.Test;

/**
 * Regression coverage for CICP-34004. Verifies that the task pipeline data provider
 * (WorkflowControllerDataProvider) treats EVACUATE-flagged live instances as eligible for
 * task assignment, while the replica-placement pipeline data provider
 * (ResourceControllerDataProvider) continues to exclude them.
 */
public class TestWorkflowControllerDataProviderEvacuate {

  private static final String CLUSTER = "TestEvacuateCluster";

  @Test
  public void testEnabledLiveInstancesIncludesEvacuate() {
    String enabledLive = "host_enabled_live";
    String evacuateLive = "host_evacuate_live";
    String evacuateOffline = "host_evacuate_offline";
    String disableLive = "host_disable_live";

    Map<String, InstanceConfig> configMap = new HashMap<>();
    configMap.put(enabledLive,
        instanceWithOperation(enabledLive, InstanceConstants.InstanceOperation.ENABLE));
    configMap.put(evacuateLive,
        instanceWithOperation(evacuateLive, InstanceConstants.InstanceOperation.EVACUATE));
    configMap.put(evacuateOffline,
        instanceWithOperation(evacuateOffline, InstanceConstants.InstanceOperation.EVACUATE));
    configMap.put(disableLive,
        instanceWithOperation(disableLive, InstanceConstants.InstanceOperation.DISABLE));

    List<LiveInstance> liveInstances = Arrays.asList(new LiveInstance(enabledLive),
        new LiveInstance(evacuateLive), new LiveInstance(disableLive));

    WorkflowControllerDataProvider workflowProvider = newProvider(configMap, liveInstances);

    Set<String> taskEligible = workflowProvider.getEnabledLiveInstances();
    Assert.assertTrue(taskEligible.contains(enabledLive),
        "ENABLE+live instance must be eligible for task assignment");
    Assert.assertTrue(taskEligible.contains(evacuateLive),
        "EVACUATE+live instance must be eligible for task assignment (CICP-34004 fix)");
    Assert.assertFalse(taskEligible.contains(evacuateOffline),
        "EVACUATE+offline instance must not be eligible (not live)");
    Assert.assertFalse(taskEligible.contains(disableLive),
        "DISABLE+live instance must not be eligible (replicas are OFFLINE)");

    // Regression guard for the placement pipeline.
    ResourceControllerDataProvider resourceProvider = new ResourceControllerDataProvider(CLUSTER);
    resourceProvider.setClusterConfig(new ClusterConfig(CLUSTER));
    resourceProvider.setInstanceConfigMap(configMap);
    resourceProvider.setLiveInstances(liveInstances);

    Set<String> placementEligible = resourceProvider.getEnabledLiveInstances();
    Assert.assertTrue(placementEligible.contains(enabledLive));
    Assert.assertFalse(placementEligible.contains(evacuateLive),
        "ResourceControllerDataProvider must continue to exclude EVACUATE for replica placement");
    Assert.assertFalse(placementEligible.contains(disableLive));
  }

  @Test
  public void testEnabledLiveInstancesWithTagIncludesEvacuate() {
    String tag = "BACKUP_TAG";
    String enabledLiveTagged = "host_enabled_tagged";
    String evacuateLiveTagged = "host_evacuate_tagged";
    String evacuateLiveUntagged = "host_evacuate_untagged";

    Map<String, InstanceConfig> configMap = new HashMap<>();
    InstanceConfig enabledCfg =
        instanceWithOperation(enabledLiveTagged, InstanceConstants.InstanceOperation.ENABLE);
    enabledCfg.addTag(tag);
    configMap.put(enabledLiveTagged, enabledCfg);

    InstanceConfig evacuateTaggedCfg =
        instanceWithOperation(evacuateLiveTagged, InstanceConstants.InstanceOperation.EVACUATE);
    evacuateTaggedCfg.addTag(tag);
    configMap.put(evacuateLiveTagged, evacuateTaggedCfg);

    configMap.put(evacuateLiveUntagged,
        instanceWithOperation(evacuateLiveUntagged, InstanceConstants.InstanceOperation.EVACUATE));

    List<LiveInstance> liveInstances =
        Arrays.asList(new LiveInstance(enabledLiveTagged), new LiveInstance(evacuateLiveTagged),
            new LiveInstance(evacuateLiveUntagged));

    WorkflowControllerDataProvider workflowProvider = newProvider(configMap, liveInstances);

    Set<String> taggedEligible = workflowProvider.getEnabledLiveInstancesWithTag(tag);
    Assert.assertEquals(taggedEligible, ImmutableSet.of(enabledLiveTagged, evacuateLiveTagged),
        "Tagged task assignment must include EVACUATE+live+tagged instances and exclude untagged");
  }

  private static WorkflowControllerDataProvider newProvider(Map<String, InstanceConfig> configMap,
      List<LiveInstance> liveInstances) {
    WorkflowControllerDataProvider provider = new WorkflowControllerDataProvider(CLUSTER);
    provider.setClusterConfig(new ClusterConfig(CLUSTER));
    provider.setInstanceConfigMap(configMap);
    provider.setLiveInstances(liveInstances);
    return provider;
  }

  private static InstanceConfig instanceWithOperation(String instanceName,
      InstanceConstants.InstanceOperation operation) {
    InstanceConfig config = new InstanceConfig(instanceName);
    config.setInstanceOperation(operation);
    return config;
  }
}
