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

package org.apache.helix.guardrail.rules;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.apache.helix.HelixDataAccessor;
import org.apache.helix.PropertyKey;
import org.apache.helix.controller.rebalancer.waged.WagedRebalancer;
import org.apache.helix.guardrail.GuardrailContext;
import org.apache.helix.guardrail.ValidationResult;
import org.apache.helix.guardrail.Violation;
import org.apache.helix.guardrail.WagedAssignmentProvider;
import org.apache.helix.model.ClusterConfig;
import org.apache.helix.model.IdealState;
import org.apache.helix.model.InstanceConfig;
import org.apache.helix.model.Partition;
import org.apache.helix.model.ResourceAssignment;
import org.testng.Assert;
import org.testng.annotations.Test;

import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Unit tests for {@link InstanceTagRebalanceFeasibilityGuardrailRule}. Cluster state (cluster config,
 * the target instance config, WAGED ideal states and their group tags, live instances) is supplied
 * through a mocked {@link HelixDataAccessor}; the WAGED what-if is supplied through a stubbed
 * {@link WagedAssignmentProvider} that returns controlled baseline/candidate assignments (or throws)
 * keyed on whether the target still carries the removed tag, so the rule's pre-filter, diffing and
 * short-circuit logic is exercised with no ZooKeeper and no rebalancer.
 */
public class TestInstanceTagRebalanceFeasibilityGuardrailRule {
  private static final String CLUSTER = "testCluster";
  private static final String INSTANCE = "instance0";
  private static final String RESOURCE = "testResource";
  private static final String TAG = "heavy";
  private static final PropertyKey.Builder BUILDER = new PropertyKey.Builder(CLUSTER);

  // A provider that must never be invoked: any call fails the test. Used to prove the rule
  // short-circuits before ever running the (expensive) WAGED what-if.
  private static final WagedAssignmentProvider PROVIDER_MUST_NOT_RUN =
      (cfg, instanceConfigs, liveInstances, idealStates, resourceConfigs) -> {
        throw new AssertionError("WAGED what-if must not run on this path");
      };

  private final InstanceTagRebalanceFeasibilityGuardrailRule rule =
      new InstanceTagRebalanceFeasibilityGuardrailRule();

  // ---------------------------------------------------------------------------------------------
  // Short-circuit / not-applicable paths (no simulation).
  // ---------------------------------------------------------------------------------------------

  @Test
  public void testNullInstanceNameIsFeasible() {
    GuardrailContext context = GuardrailContext.newBuilder(CLUSTER)
        .dataAccessor(mock(HelixDataAccessor.class))
        .proposedRemovedInstanceTags(ImmutableList.of(TAG))
        .wagedAssignmentProvider(PROVIDER_MUST_NOT_RUN)
        .build();
    Assert.assertTrue(rule.validate(context).isFeasible());
  }

  @Test
  public void testNullProposedTagsIsFeasible() {
    GuardrailContext context = GuardrailContext.newBuilder(CLUSTER)
        .dataAccessor(mock(HelixDataAccessor.class))
        .instanceName(INSTANCE)
        .wagedAssignmentProvider(PROVIDER_MUST_NOT_RUN)
        .build();
    Assert.assertTrue(rule.validate(context).isFeasible());
  }

  @Test
  public void testEmptyProposedTagsIsFeasible() {
    GuardrailContext context = GuardrailContext.newBuilder(CLUSTER)
        .dataAccessor(mock(HelixDataAccessor.class))
        .instanceName(INSTANCE)
        .proposedRemovedInstanceTags(ImmutableList.of())
        .wagedAssignmentProvider(PROVIDER_MUST_NOT_RUN)
        .build();
    Assert.assertTrue(rule.validate(context).isFeasible());
  }

  @Test
  public void testNullProviderIsFeasible() {
    // Not wired for simulation: certify feasible rather than block every removeInstanceTag.
    GuardrailContext context = GuardrailContext.newBuilder(CLUSTER)
        .dataAccessor(mock(HelixDataAccessor.class))
        .instanceName(INSTANCE)
        .proposedRemovedInstanceTags(ImmutableList.of(TAG))
        .build();
    Assert.assertTrue(rule.validate(context).isFeasible());
  }

  @Test
  public void testNullClusterConfigIsFeasible() {
    HelixDataAccessor dataAccessor = mock(HelixDataAccessor.class);
    when(dataAccessor.keyBuilder()).thenReturn(BUILDER);
    doReturn(null).when(dataAccessor).getProperty(BUILDER.clusterConfig());
    Assert.assertTrue(
        rule.validate(context(dataAccessor, ImmutableList.of(TAG), PROVIDER_MUST_NOT_RUN))
            .isFeasible());
  }

  @Test
  public void testGuardrailDisabledShortCircuits() {
    // Explicitly disabled (the per-cluster kill switch): the rule returns feasible before reading the
    // instance config or running the what-if. Both are stubbed to fail so a regression that reaches
    // them is caught.
    ClusterConfig clusterConfig = new ClusterConfig(CLUSTER);
    clusterConfig.setInstanceTagRebalanceGuardrailEnabled(false);
    HelixDataAccessor dataAccessor = mock(HelixDataAccessor.class);
    when(dataAccessor.keyBuilder()).thenReturn(BUILDER);
    doReturn(clusterConfig).when(dataAccessor).getProperty(BUILDER.clusterConfig());
    doReturn(taggedInstance(INSTANCE, TAG)).when(dataAccessor)
        .getProperty(BUILDER.instanceConfig(INSTANCE));
    Assert.assertTrue(
        rule.validate(context(dataAccessor, ImmutableList.of(TAG), PROVIDER_MUST_NOT_RUN))
            .isFeasible());
  }

  @Test
  public void testGuardrailEnforcedByDefault() {
    // A fresh ClusterConfig leaves the flag unset: the rule is enforced by default (opt-out), so a tag
    // removal that under-places a partition is still rejected without anyone enabling anything.
    Map<String, ResourceAssignment> baseline = ImmutableMap.of(RESOURCE, resourceAssignment(
        ImmutableMap.of(RESOURCE + "_0",
            ImmutableMap.of("instance0", "MASTER", "instance1", "SLAVE", "instance2", "SLAVE"))));
    Map<String, ResourceAssignment> candidate = ImmutableMap.of(RESOURCE, resourceAssignment(
        ImmutableMap.of(RESOURCE + "_0",
            ImmutableMap.of("instance1", "MASTER", "instance2", "SLAVE"))));
    HelixDataAccessor dataAccessor = simulationAccessor();
    // Override the explicitly-enabled config the helper installs with a default (unset-flag) one.
    doReturn(new ClusterConfig(CLUSTER)).when(dataAccessor).getProperty(BUILDER.clusterConfig());
    ValidationResult result =
        rule.validate(context(dataAccessor, ImmutableList.of(TAG), fixedProvider(baseline, candidate)));
    Assert.assertFalse(result.isFeasible());
    Assert.assertEquals(result.getViolations().get(0).getRuleId(),
        InstanceTagRebalanceFeasibilityGuardrailRule.RULE_ID);
  }

  @Test
  public void testMissingInstanceConfigIsFeasible() {
    ClusterConfig clusterConfig = enabledClusterConfig();
    HelixDataAccessor dataAccessor = mock(HelixDataAccessor.class);
    when(dataAccessor.keyBuilder()).thenReturn(BUILDER);
    doReturn(clusterConfig).when(dataAccessor).getProperty(BUILDER.clusterConfig());
    doReturn(null).when(dataAccessor).getProperty(BUILDER.instanceConfig(INSTANCE));
    Assert.assertTrue(
        rule.validate(context(dataAccessor, ImmutableList.of(TAG), PROVIDER_MUST_NOT_RUN))
            .isFeasible());
  }

  @Test
  public void testInstanceLacksTagIsFeasible() {
    // The instance does not carry the tag being removed, so the removal changes nothing and the rule
    // short-circuits before the what-if.
    ClusterConfig clusterConfig = enabledClusterConfig();
    HelixDataAccessor dataAccessor = mock(HelixDataAccessor.class);
    when(dataAccessor.keyBuilder()).thenReturn(BUILDER);
    doReturn(clusterConfig).when(dataAccessor).getProperty(BUILDER.clusterConfig());
    doReturn(taggedInstance(INSTANCE, "other")).when(dataAccessor)
        .getProperty(BUILDER.instanceConfig(INSTANCE));
    Assert.assertTrue(
        rule.validate(context(dataAccessor, ImmutableList.of(TAG), PROVIDER_MUST_NOT_RUN))
            .isFeasible());
  }

  @Test
  public void testNoWagedResourcesIsFeasible() {
    // A non-WAGED resource cannot be broken by removing a tag; the rule returns feasible before the
    // what-if.
    ClusterConfig clusterConfig = enabledClusterConfig();
    HelixDataAccessor dataAccessor = mock(HelixDataAccessor.class);
    when(dataAccessor.keyBuilder()).thenReturn(BUILDER);
    doReturn(clusterConfig).when(dataAccessor).getProperty(BUILDER.clusterConfig());
    doReturn(taggedInstance(INSTANCE, TAG)).when(dataAccessor)
        .getProperty(BUILDER.instanceConfig(INSTANCE));
    doReturn(ImmutableList.of(new IdealState("nonWagedResource"))).when(dataAccessor)
        .getChildValues(BUILDER.idealStates(), true);
    Assert.assertTrue(
        rule.validate(context(dataAccessor, ImmutableList.of(TAG), PROVIDER_MUST_NOT_RUN))
            .isFeasible());
  }

  @Test
  public void testTagNotUsedByAnyWagedResourceIsFeasible() {
    // A WAGED resource exists, but none is pinned (INSTANCE_GROUP_TAG) to the removed tag, so the
    // removal cannot shrink any resource's placeable pool. The pre-filter short-circuits before the
    // (expensive) what-if, so the provider must never run.
    ClusterConfig clusterConfig = enabledClusterConfig();
    HelixDataAccessor dataAccessor = mock(HelixDataAccessor.class);
    when(dataAccessor.keyBuilder()).thenReturn(BUILDER);
    doReturn(clusterConfig).when(dataAccessor).getProperty(BUILDER.clusterConfig());
    doReturn(taggedInstance(INSTANCE, TAG)).when(dataAccessor)
        .getProperty(BUILDER.instanceConfig(INSTANCE));
    // WAGED resource with no group tag (places on any assignable instance regardless of tags).
    doReturn(ImmutableList.of(wagedIdealState(RESOURCE))).when(dataAccessor)
        .getChildValues(BUILDER.idealStates(), true);
    Assert.assertTrue(
        rule.validate(context(dataAccessor, ImmutableList.of(TAG), PROVIDER_MUST_NOT_RUN))
            .isFeasible());
  }

  @Test
  public void testRemovingDifferentTagThanPinnedIsFeasible() {
    // The instance carries two tags; a WAGED resource is pinned to TAG, but the request removes the
    // other (unpinned) tag, so no pinned resource is affected. Pre-filter short-circuits.
    ClusterConfig clusterConfig = enabledClusterConfig();
    HelixDataAccessor dataAccessor = mock(HelixDataAccessor.class);
    when(dataAccessor.keyBuilder()).thenReturn(BUILDER);
    doReturn(clusterConfig).when(dataAccessor).getProperty(BUILDER.clusterConfig());
    doReturn(taggedInstance(INSTANCE, TAG, "other")).when(dataAccessor)
        .getProperty(BUILDER.instanceConfig(INSTANCE));
    doReturn(ImmutableList.of(wagedIdealStateWithGroupTag(RESOURCE, TAG))).when(dataAccessor)
        .getChildValues(BUILDER.idealStates(), true);
    Assert.assertTrue(
        rule.validate(context(dataAccessor, ImmutableList.of("other"), PROVIDER_MUST_NOT_RUN))
            .isFeasible());
  }

  @Test
  public void testAnyLiveInstanceWagedResourceIsExempt() {
    // An ANY_LIVEINSTANCE resource keeps one replica per live instance, so removing a tag is a
    // by-design reduction, never a deficit. It is the only WAGED resource, so after the exemption
    // there is nothing to simulate and the (expensive) provider must never run.
    ClusterConfig clusterConfig = enabledClusterConfig();
    HelixDataAccessor dataAccessor = mock(HelixDataAccessor.class);
    when(dataAccessor.keyBuilder()).thenReturn(BUILDER);
    doReturn(clusterConfig).when(dataAccessor).getProperty(BUILDER.clusterConfig());
    doReturn(taggedInstance(INSTANCE, TAG)).when(dataAccessor)
        .getProperty(BUILDER.instanceConfig(INSTANCE));
    IdealState anyLiveInstance = wagedIdealStateWithGroupTag(RESOURCE, TAG);
    anyLiveInstance.setReplicas("ANY_LIVEINSTANCE");
    doReturn(ImmutableList.of(anyLiveInstance)).when(dataAccessor)
        .getChildValues(BUILDER.idealStates(), true);
    Assert.assertTrue(
        rule.validate(context(dataAccessor, ImmutableList.of(TAG), PROVIDER_MUST_NOT_RUN))
            .isFeasible());
  }

  // ---------------------------------------------------------------------------------------------
  // Simulation paths (the pre-filter passes: instance carries the tag and a WAGED resource is
  // pinned to it, so the what-if runs).
  // ---------------------------------------------------------------------------------------------

  @Test
  public void testRemoveTagFeasibleWhenReplicasFitElsewhere() {
    // Every partition keeps the same replica count after the tag is removed: the removal is safe.
    Map<String, ResourceAssignment> assignment = ImmutableMap.of(RESOURCE, resourceAssignment(
        ImmutableMap.of(RESOURCE + "_0",
            ImmutableMap.of("instance1", "MASTER", "instance2", "SLAVE"))));
    HelixDataAccessor dataAccessor = simulationAccessor();
    ValidationResult result =
        rule.validate(context(dataAccessor, ImmutableList.of(TAG), fixedProvider(assignment, assignment)));
    Assert.assertTrue(result.isFeasible());
  }

  @Test
  public void testRemoveTagInfeasibleWhenPartitionLosesReplicas() {
    // Baseline places 3 replicas for _0; after the tag is removed only 2 fit -> a violation for _0.
    Map<String, ResourceAssignment> baseline = ImmutableMap.of(RESOURCE, resourceAssignment(
        ImmutableMap.of(
            RESOURCE + "_0", ImmutableMap.of("instance0", "MASTER", "instance1", "SLAVE", "instance2", "SLAVE"),
            RESOURCE + "_1", ImmutableMap.of("instance1", "MASTER", "instance2", "SLAVE"))));
    Map<String, ResourceAssignment> candidate = ImmutableMap.of(RESOURCE, resourceAssignment(
        ImmutableMap.of(
            RESOURCE + "_0", ImmutableMap.of("instance1", "MASTER", "instance2", "SLAVE"),
            RESOURCE + "_1", ImmutableMap.of("instance1", "MASTER", "instance2", "SLAVE"))));
    HelixDataAccessor dataAccessor = simulationAccessor();
    ValidationResult result =
        rule.validate(context(dataAccessor, ImmutableList.of(TAG), fixedProvider(baseline, candidate)));

    Assert.assertFalse(result.isFeasible());
    List<Violation> violations = result.getViolations();
    Assert.assertEquals(violations.size(), 1);
    Violation violation = violations.get(0);
    Assert.assertEquals(violation.getRuleId(),
        InstanceTagRebalanceFeasibilityGuardrailRule.RULE_ID);
    Assert.assertEquals(violation.getResourceName(), RESOURCE);
    Assert.assertEquals(violation.getPartitionName(), RESOURCE + "_0");
    Assert.assertTrue(violation.getMessage().contains("from 3 to 2"),
        "message should report the replica drop: " + violation.getMessage());
  }

  @Test
  public void testBaselineProviderThrowsFailsClosed() {
    HelixDataAccessor dataAccessor = simulationAccessor();
    WagedAssignmentProvider provider =
        (cfg, instanceConfigs, liveInstances, idealStates, resourceConfigs) -> {
          throw new RuntimeException("cannot compute");
        };
    ValidationResult result =
        rule.validate(context(dataAccessor, ImmutableList.of(TAG), provider));
    Assert.assertFalse(result.isFeasible());
    Assert.assertTrue(result.getViolations().get(0).getMessage().contains("baseline"),
        "should fail closed citing the missing baseline: "
            + result.getViolations().get(0).getMessage());
  }

  @Test
  public void testCandidateProviderThrowsFailsClosed() {
    // Baseline succeeds but removing the tag makes WAGED unable to compute any assignment.
    Map<String, ResourceAssignment> baseline = ImmutableMap.of(RESOURCE, resourceAssignment(
        ImmutableMap.of(RESOURCE + "_0", ImmutableMap.of("instance0", "MASTER"))));
    HelixDataAccessor dataAccessor = simulationAccessor();
    WagedAssignmentProvider provider =
        (cfg, instanceConfigs, liveInstances, idealStates, resourceConfigs) -> {
          boolean candidate = instanceConfigs.stream()
              .filter(ic -> ic != null && INSTANCE.equals(ic.getInstanceName())).findFirst()
              .map(ic -> !ic.containsTag(TAG)).orElse(true);
          if (candidate) {
            throw new RuntimeException("CAPACITY_DEFICIT");
          }
          return baseline;
        };
    ValidationResult result =
        rule.validate(context(dataAccessor, ImmutableList.of(TAG), provider));
    Assert.assertFalse(result.isFeasible());
    Assert.assertTrue(
        result.getViolations().get(0).getMessage().contains("unable to compute an assignment"),
        "should fail closed citing the uncomputable assignment: "
            + result.getViolations().get(0).getMessage());
  }

  @Test
  public void testViolationsCappedAtMax() {
    // 150 partitions each lose a replica -> 10 enumerated violations + 1 trailing overflow summary.
    Map<String, Map<String, String>> baselineMap = new java.util.LinkedHashMap<>();
    Map<String, Map<String, String>> candidateMap = new java.util.LinkedHashMap<>();
    for (int i = 0; i < 150; i++) {
      baselineMap.put(RESOURCE + "_" + i,
          ImmutableMap.of("instance0", "MASTER", "instance1", "SLAVE"));
      candidateMap.put(RESOURCE + "_" + i, ImmutableMap.of("instance1", "MASTER"));
    }
    Map<String, ResourceAssignment> baseline =
        ImmutableMap.of(RESOURCE, resourceAssignment(baselineMap));
    Map<String, ResourceAssignment> candidate =
        ImmutableMap.of(RESOURCE, resourceAssignment(candidateMap));
    HelixDataAccessor dataAccessor = simulationAccessor();
    ValidationResult result =
        rule.validate(context(dataAccessor, ImmutableList.of(TAG), fixedProvider(baseline, candidate)));

    Assert.assertFalse(result.isFeasible());
    List<Violation> violations = result.getViolations();
    Assert.assertEquals(violations.size(), 11);
    Violation overflow = violations.get(10);
    Assert.assertNull(overflow.getPartitionName());
    Assert.assertTrue(overflow.getMessage().contains("of 150"),
        "overflow summary should record the true total: " + overflow.getMessage());
  }

  // ---------------------------------------------------------------------------------------------
  // Helpers.
  // ---------------------------------------------------------------------------------------------

  private GuardrailContext context(HelixDataAccessor dataAccessor, List<String> removedTags,
      WagedAssignmentProvider provider) {
    return GuardrailContext.newBuilder(CLUSTER)
        .dataAccessor(dataAccessor)
        .instanceName(INSTANCE)
        .proposedRemovedInstanceTags(removedTags)
        .wagedAssignmentProvider(provider)
        .build();
  }

  // A fully-wired accessor for the simulation path: enabled cluster, a target instance carrying the
  // tag, one WAGED resource pinned to that tag, a couple of live instances, and a small instance
  // pool.
  private HelixDataAccessor simulationAccessor() {
    HelixDataAccessor dataAccessor = mock(HelixDataAccessor.class);
    when(dataAccessor.keyBuilder()).thenReturn(BUILDER);
    doReturn(enabledClusterConfig()).when(dataAccessor).getProperty(BUILDER.clusterConfig());
    doReturn(taggedInstance(INSTANCE, TAG)).when(dataAccessor)
        .getProperty(BUILDER.instanceConfig(INSTANCE));
    doReturn(ImmutableList.of(wagedIdealStateWithGroupTag(RESOURCE, TAG))).when(dataAccessor)
        .getChildValues(BUILDER.idealStates(), true);
    doReturn(ImmutableList.of()).when(dataAccessor).getChildValues(BUILDER.resourceConfigs(), true);
    doReturn(ImmutableList.of(taggedInstance(INSTANCE, TAG), taggedInstance("instance1", TAG),
        taggedInstance("instance2", TAG))).when(dataAccessor)
        .getChildValues(BUILDER.instanceConfigs(), true);
    doReturn(ImmutableList.of(INSTANCE, "instance1", "instance2")).when(dataAccessor)
        .getChildNames(BUILDER.liveInstances());
    return dataAccessor;
  }

  // Returns the baseline assignment while the target instance in the passed configs still carries
  // the tag, and the candidate assignment once the rule has removed it.
  private static WagedAssignmentProvider fixedProvider(Map<String, ResourceAssignment> baseline,
      Map<String, ResourceAssignment> candidate) {
    return (cfg, instanceConfigs, liveInstances, idealStates, resourceConfigs) -> {
      boolean isCandidate = instanceConfigs.stream()
          .filter(ic -> ic != null && INSTANCE.equals(ic.getInstanceName())).findFirst()
          .map(ic -> !ic.containsTag(TAG)).orElse(true);
      return isCandidate ? candidate : baseline;
    };
  }

  private static ClusterConfig enabledClusterConfig() {
    ClusterConfig clusterConfig = new ClusterConfig(CLUSTER);
    // Enabled by default; set it explicitly so these tests stay pinned to the enabled behavior.
    clusterConfig.setInstanceTagRebalanceGuardrailEnabled(true);
    return clusterConfig;
  }

  private static InstanceConfig taggedInstance(String name, String... tags) {
    // A default InstanceConfig is ENABLE, i.e. assignable.
    InstanceConfig instanceConfig = new InstanceConfig(name);
    for (String tag : tags) {
      instanceConfig.addTag(tag);
    }
    return instanceConfig;
  }

  private static IdealState wagedIdealState(String resource) {
    IdealState idealState = new IdealState(resource);
    idealState.setRebalancerClassName(WagedRebalancer.class.getName());
    // isWagedEnabled requires FULL_AUTO in addition to the WAGED rebalancer class; real WAGED
    // resources are always FULL_AUTO.
    idealState.setRebalanceMode(IdealState.RebalanceMode.FULL_AUTO);
    return idealState;
  }

  private static IdealState wagedIdealStateWithGroupTag(String resource, String groupTag) {
    IdealState idealState = wagedIdealState(resource);
    idealState.setInstanceGroupTag(groupTag);
    return idealState;
  }

  private static ResourceAssignment resourceAssignment(Map<String, Map<String, String>> partitions) {
    ResourceAssignment resourceAssignment = new ResourceAssignment(RESOURCE);
    List<String> partitionNames = new ArrayList<>(partitions.keySet());
    for (String partitionName : partitionNames) {
      resourceAssignment.addReplicaMap(new Partition(partitionName), partitions.get(partitionName));
    }
    return resourceAssignment;
  }
}
