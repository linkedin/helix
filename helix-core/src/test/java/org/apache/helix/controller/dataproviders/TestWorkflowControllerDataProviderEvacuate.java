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
import org.apache.helix.controller.stages.CurrentStateOutput;
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

  /**
   * Regression guard for the throttle-path NPE fix at AbstractTaskDispatcher.java:651.
   * The pre-fix code did `cache.getAssignableInstanceConfigMap().get(instance).getMaxConcurrentTask()`
   * which NPE'd for EVACUATE-flagged live instances because they are filtered out of
   * `_assignableInstanceConfigMap` by `InstanceConfig.isAssignable()`. The fix uses
   * `cache.getInstanceConfigMap()` instead, which returns the full config including EVACUATE.
   * This test asserts both maps' contents to lock in the contract relied upon by the fix.
   */
  @Test
  public void testInstanceConfigMapIncludesEvacuateForThrottlePath() {
    String enabledLive = "host_enabled_live";
    String evacuateLive = "host_evacuate_live";

    Map<String, InstanceConfig> configMap = new HashMap<>();
    configMap.put(enabledLive,
        instanceWithOperation(enabledLive, InstanceConstants.InstanceOperation.ENABLE));
    configMap.put(evacuateLive,
        instanceWithOperation(evacuateLive, InstanceConstants.InstanceOperation.EVACUATE));

    List<LiveInstance> liveInstances =
        Arrays.asList(new LiveInstance(enabledLive), new LiveInstance(evacuateLive));

    WorkflowControllerDataProvider provider = newProvider(configMap, liveInstances);

    // Full instance config map (used by the fixed throttle path) must include EVACUATE
    Assert.assertNotNull(provider.getInstanceConfigMap().get(evacuateLive),
        "InstanceConfigMap must include EVACUATE instance - throttle path depends on this");
    Assert.assertNotNull(provider.getInstanceConfigMap().get(enabledLive));

    // Assignable instance config map (used by the BUGGY pre-fix throttle path) does NOT include
    // EVACUATE. Locking this in to document the contract that necessitated the fix.
    Assert.assertNull(provider.getAssignableInstanceConfigMap().get(evacuateLive),
        "AssignableInstanceConfigMap must NOT include EVACUATE - this is why the original throttle code path NPE'd");
    Assert.assertNotNull(provider.getAssignableInstanceConfigMap().get(enabledLive));
  }

  /**
   * Regression guard for the active-task-count throttle-path NPE (CICP-34004), distinct from the
   * instanceConfig fix above. A live EVACUATE task candidate must have a non-null active-task count
   * after resetActiveTaskCount; otherwise AbstractTaskDispatcher's throttling math unboxes a null.
   * See the PR for the full chain.
   */
  @Test
  public void testActiveTaskCountSeededForEvacuateThrottlePath() {
    String enabledLive = "host_enabled_live";
    String evacuateLive = "host_evacuate_live";

    Map<String, InstanceConfig> configMap = new HashMap<>();
    configMap.put(enabledLive,
        instanceWithOperation(enabledLive, InstanceConstants.InstanceOperation.ENABLE));
    configMap.put(evacuateLive,
        instanceWithOperation(evacuateLive, InstanceConstants.InstanceOperation.EVACUATE));

    List<LiveInstance> liveInstances =
        Arrays.asList(new LiveInstance(enabledLive), new LiveInstance(evacuateLive));

    WorkflowControllerDataProvider provider = newProvider(configMap, liveInstances);

    // Precondition: the evacuating live instance is a task-assignment candidate.
    Assert.assertTrue(provider.getEnabledLiveInstances().contains(evacuateLive),
        "EVACUATE+live instance must be a task candidate (CICP-34004 fix)");

    provider.resetActiveTaskCount(new CurrentStateOutput());

    // Every task candidate must have a non-null active-task count (the dispatcher unboxes it).
    Assert.assertNotNull(provider.getParticipantActiveTaskCount(evacuateLive),
        "Active task count for the EVACUATE task candidate must not be null after resetActiveTaskCount");
    Assert.assertNotNull(provider.getParticipantActiveTaskCount(enabledLive),
        "Active task count for the ENABLE task candidate must not be null after resetActiveTaskCount");

    // Defense in depth: the getter defaults to 0 for an unseeded instance.
    Assert.assertEquals(provider.getParticipantActiveTaskCount("never_registered_instance"),
        Integer.valueOf(0),
        "getParticipantActiveTaskCount must default to 0 for an unseeded instance (CICP-34004)");
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

  /**
   * Phase 1: instance is EVACUATE, must be eligible via the override.
   * Phase 2: same instance flipped to ENABLE on the same provider instance, must remain
   * eligible - but now through the base-class path (super.getEnabledLiveInstances()),
   * not the override's EVACUATE-additive branch. This guards against future changes that
   * might inadvertently couple the override's behavior to the initial state.
   */
  @Test
  public void testEvacuateFlipsBackToEnableExposesInstance() {
    String host = "host_flipping";

    Map<String, InstanceConfig> configMap = new HashMap<>();
    configMap.put(host, instanceWithOperation(host, InstanceConstants.InstanceOperation.EVACUATE));
    List<LiveInstance> liveInstances = Arrays.asList(new LiveInstance(host));

    WorkflowControllerDataProvider provider = newProvider(configMap, liveInstances);

    // Phase 1: EVACUATE+live - eligible via override (additive branch).
    Assert.assertTrue(provider.getEnabledLiveInstances().contains(host),
        "EVACUATE+live must be task-eligible (override path)");
    Assert.assertFalse(provider.getEvacuatingInstances().isEmpty(),
        "Sanity: instance must be in evacuating set before flip");

    // Mutate in place: flip the same instance to ENABLE. setInstanceConfigMap rebuilds
    // _derivedInstanceCache (BaseControllerDataProvider#updateInstanceSets), so the
    // override's super.getEnabledLiveInstances() now returns the host directly.
    Map<String, InstanceConfig> flippedMap = new HashMap<>();
    flippedMap.put(host, instanceWithOperation(host, InstanceConstants.InstanceOperation.ENABLE));
    provider.setInstanceConfigMap(flippedMap);

    // Phase 2: ENABLE+live - eligible via super (not via the EVACUATE-additive branch).
    Assert.assertTrue(provider.getEnabledLiveInstances().contains(host),
        "ENABLE+live must remain task-eligible after EVACUATE->ENABLE flip (base path)");
    Assert.assertTrue(provider.getEvacuatingInstances().isEmpty(),
        "After flip, evacuating set must be empty - confirms eligibility is via super, not override");
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
