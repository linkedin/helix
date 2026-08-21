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
import org.apache.helix.constants.InstanceConstants;
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
 * Unit tests for {@link InstanceOperationRebalanceFeasibilityGuardrailRule}. Cluster state (cluster
 * config, the target instance config, WAGED ideal states, live instances) is supplied through a
 * mocked {@link HelixDataAccessor}; the WAGED what-if is supplied through a stubbed
 * {@link WagedAssignmentProvider} that returns controlled baseline/candidate assignments (or throws),
 * so the rule's diffing and short-circuit logic is exercised with no ZooKeeper and no rebalancer.
 */
public class TestInstanceOperationRebalanceFeasibilityGuardrailRule {
  private static final String CLUSTER = "testCluster";
  private static final String INSTANCE = "instance0";
  private static final String RESOURCE = "testResource";
  private static final PropertyKey.Builder BUILDER = new PropertyKey.Builder(CLUSTER);

  // A provider that must never be invoked: any call fails the test. Used to prove the rule
  // short-circuits before ever running the (expensive) WAGED what-if.
  private static final WagedAssignmentProvider PROVIDER_MUST_NOT_RUN =
      (cfg, instanceConfigs, liveInstances, idealStates, resourceConfigs) -> {
        throw new AssertionError("WAGED what-if must not run on this path");
      };

  private final InstanceOperationRebalanceFeasibilityGuardrailRule rule =
      new InstanceOperationRebalanceFeasibilityGuardrailRule();

  // ---------------------------------------------------------------------------------------------
  // Short-circuit / not-applicable paths (no simulation).
  // ---------------------------------------------------------------------------------------------

  @Test
  public void testNullInstanceNameIsFeasible() {
    GuardrailContext context = GuardrailContext.newBuilder(CLUSTER)
        .dataAccessor(mock(HelixDataAccessor.class))
        .proposedInstanceOperation(InstanceConstants.InstanceOperation.EVACUATE)
        .wagedAssignmentProvider(PROVIDER_MUST_NOT_RUN)
        .build();
    Assert.assertTrue(rule.validate(context).isFeasible());
  }

  @Test
  public void testNullProposedOperationIsFeasible() {
    GuardrailContext context = GuardrailContext.newBuilder(CLUSTER)
        .dataAccessor(mock(HelixDataAccessor.class))
        .instanceName(INSTANCE)
        .wagedAssignmentProvider(PROVIDER_MUST_NOT_RUN)
        .build();
    Assert.assertTrue(rule.validate(context).isFeasible());
  }

  @Test
  public void testNullProviderIsFeasible() {
    // Not wired for simulation: certify feasible rather than block every setInstanceOperation.
    GuardrailContext context = GuardrailContext.newBuilder(CLUSTER)
        .dataAccessor(mock(HelixDataAccessor.class))
        .instanceName(INSTANCE)
        .proposedInstanceOperation(InstanceConstants.InstanceOperation.EVACUATE)
        .build();
    Assert.assertTrue(rule.validate(context).isFeasible());
  }

  @Test
  public void testNullClusterConfigIsFeasible() {
    HelixDataAccessor dataAccessor = mock(HelixDataAccessor.class);
    when(dataAccessor.keyBuilder()).thenReturn(BUILDER);
    doReturn(null).when(dataAccessor).getProperty(BUILDER.clusterConfig());
    Assert.assertTrue(rule.validate(
        context(dataAccessor, InstanceConstants.InstanceOperation.EVACUATE, PROVIDER_MUST_NOT_RUN))
        .isFeasible());
  }

  @Test
  public void testGuardrailDisabledByDefaultShortCircuits() {
    // Flag left unset (default false): the rule returns feasible before reading the instance config
    // or running the what-if. Both are stubbed to fail so a regression that reaches them is caught.
    ClusterConfig clusterConfig = new ClusterConfig(CLUSTER);
    HelixDataAccessor dataAccessor = mock(HelixDataAccessor.class);
    when(dataAccessor.keyBuilder()).thenReturn(BUILDER);
    doReturn(clusterConfig).when(dataAccessor).getProperty(BUILDER.clusterConfig());
    doReturn(assignableInstance(INSTANCE)).when(dataAccessor)
        .getProperty(BUILDER.instanceConfig(INSTANCE));
    Assert.assertTrue(rule.validate(
        context(dataAccessor, InstanceConstants.InstanceOperation.EVACUATE, PROVIDER_MUST_NOT_RUN))
        .isFeasible());
  }

  @Test
  public void testMissingInstanceConfigIsFeasible() {
    ClusterConfig clusterConfig = enabledClusterConfig();
    HelixDataAccessor dataAccessor = mock(HelixDataAccessor.class);
    when(dataAccessor.keyBuilder()).thenReturn(BUILDER);
    doReturn(clusterConfig).when(dataAccessor).getProperty(BUILDER.clusterConfig());
    doReturn(null).when(dataAccessor).getProperty(BUILDER.instanceConfig(INSTANCE));
    Assert.assertTrue(rule.validate(
        context(dataAccessor, InstanceConstants.InstanceOperation.EVACUATE, PROVIDER_MUST_NOT_RUN))
        .isFeasible());
  }

  @Test
  public void testCurrentAlreadyNonAssignableIsFeasible() {
    // The target already holds no assignable capacity (EVACUATE), so setting UNKNOWN removes none.
    // This is also the path that excludes SWAP_IN, which is only reachable from a non-assignable
    // (UNKNOWN) state.
    ClusterConfig clusterConfig = enabledClusterConfig();
    HelixDataAccessor dataAccessor = mock(HelixDataAccessor.class);
    when(dataAccessor.keyBuilder()).thenReturn(BUILDER);
    doReturn(clusterConfig).when(dataAccessor).getProperty(BUILDER.clusterConfig());
    doReturn(instance(INSTANCE, InstanceConstants.InstanceOperation.EVACUATE)).when(dataAccessor)
        .getProperty(BUILDER.instanceConfig(INSTANCE));
    Assert.assertTrue(rule.validate(
        context(dataAccessor, InstanceConstants.InstanceOperation.UNKNOWN, PROVIDER_MUST_NOT_RUN))
        .isFeasible());
  }

  @Test
  public void testSwapInFromUnknownIsFeasible() {
    // SWAP_IN's only valid predecessor is UNKNOWN (non-assignable), so the rule short-circuits and
    // never simulates -- documenting the SWAP_IN exclusion as a like-for-like trade.
    ClusterConfig clusterConfig = enabledClusterConfig();
    HelixDataAccessor dataAccessor = mock(HelixDataAccessor.class);
    when(dataAccessor.keyBuilder()).thenReturn(BUILDER);
    doReturn(clusterConfig).when(dataAccessor).getProperty(BUILDER.clusterConfig());
    doReturn(instance(INSTANCE, InstanceConstants.InstanceOperation.UNKNOWN)).when(dataAccessor)
        .getProperty(BUILDER.instanceConfig(INSTANCE));
    Assert.assertTrue(rule.validate(
        context(dataAccessor, InstanceConstants.InstanceOperation.SWAP_IN, PROVIDER_MUST_NOT_RUN))
        .isFeasible());
  }

  @Test
  public void testNonReducingTargetOpIsFeasible() {
    // DISABLE keeps the instance in the assignable pool, so the rule short-circuits before the
    // what-if: DISABLE is not a placement-feasibility concern.
    ClusterConfig clusterConfig = enabledClusterConfig();
    HelixDataAccessor dataAccessor = mock(HelixDataAccessor.class);
    when(dataAccessor.keyBuilder()).thenReturn(BUILDER);
    doReturn(clusterConfig).when(dataAccessor).getProperty(BUILDER.clusterConfig());
    doReturn(assignableInstance(INSTANCE)).when(dataAccessor)
        .getProperty(BUILDER.instanceConfig(INSTANCE));
    Assert.assertTrue(rule.validate(
        context(dataAccessor, InstanceConstants.InstanceOperation.DISABLE, PROVIDER_MUST_NOT_RUN))
        .isFeasible());
  }

  @Test
  public void testNoWagedResourcesIsFeasible() {
    // A non-WAGED resource cannot be broken by this operation, and the rule returns feasible before
    // running the what-if.
    ClusterConfig clusterConfig = enabledClusterConfig();
    HelixDataAccessor dataAccessor = mock(HelixDataAccessor.class);
    when(dataAccessor.keyBuilder()).thenReturn(BUILDER);
    doReturn(clusterConfig).when(dataAccessor).getProperty(BUILDER.clusterConfig());
    doReturn(assignableInstance(INSTANCE)).when(dataAccessor)
        .getProperty(BUILDER.instanceConfig(INSTANCE));
    doReturn(ImmutableList.of(new IdealState("nonWagedResource"))).when(dataAccessor)
        .getChildValues(BUILDER.idealStates(), true);
    Assert.assertTrue(rule.validate(
        context(dataAccessor, InstanceConstants.InstanceOperation.EVACUATE, PROVIDER_MUST_NOT_RUN))
        .isFeasible());
  }

  @Test
  public void testAnyLiveInstanceWagedResourceIsExempt() {
    // An ANY_LIVEINSTANCE resource keeps exactly one replica per live instance, so removing an
    // instance is a by-design N->N-1 reduction, never a capacity deficit. The rule must exempt such
    // a resource; here it is the only WAGED resource, so after the exemption there is nothing to
    // simulate and the (expensive) provider must never run.
    ClusterConfig clusterConfig = enabledClusterConfig();
    HelixDataAccessor dataAccessor = mock(HelixDataAccessor.class);
    when(dataAccessor.keyBuilder()).thenReturn(BUILDER);
    doReturn(clusterConfig).when(dataAccessor).getProperty(BUILDER.clusterConfig());
    doReturn(assignableInstance(INSTANCE)).when(dataAccessor)
        .getProperty(BUILDER.instanceConfig(INSTANCE));
    doReturn(ImmutableList.of(anyLiveInstanceWagedIdealState(RESOURCE))).when(dataAccessor)
        .getChildValues(BUILDER.idealStates(), true);
    Assert.assertTrue(rule.validate(
        context(dataAccessor, InstanceConstants.InstanceOperation.EVACUATE, PROVIDER_MUST_NOT_RUN))
        .isFeasible());
  }

  @Test
  public void testEvacuateFeasibleWhenReplicasFitElsewhere() {
    // Every partition keeps the same replica count after the drain: the operation is safe.
    Map<String, ResourceAssignment> assignment = ImmutableMap.of(RESOURCE, resourceAssignment(
        ImmutableMap.of(RESOURCE + "_0", ImmutableMap.of("instance1", "MASTER", "instance2", "SLAVE"))));
    HelixDataAccessor dataAccessor = simulationAccessor();
    ValidationResult result = rule.validate(context(dataAccessor,
        InstanceConstants.InstanceOperation.EVACUATE, fixedProvider(assignment, assignment)));
    Assert.assertTrue(result.isFeasible());
  }

  @Test
  public void testEvacuateInfeasibleWhenPartitionLosesReplicas() {
    // Baseline places 3 replicas for _0; after the drain only 2 fit -> a violation for _0 only.
    Map<String, ResourceAssignment> baseline = ImmutableMap.of(RESOURCE, resourceAssignment(
        ImmutableMap.of(
            RESOURCE + "_0", ImmutableMap.of("instance0", "MASTER", "instance1", "SLAVE", "instance2", "SLAVE"),
            RESOURCE + "_1", ImmutableMap.of("instance1", "MASTER", "instance2", "SLAVE"))));
    Map<String, ResourceAssignment> candidate = ImmutableMap.of(RESOURCE, resourceAssignment(
        ImmutableMap.of(
            RESOURCE + "_0", ImmutableMap.of("instance1", "MASTER", "instance2", "SLAVE"),
            RESOURCE + "_1", ImmutableMap.of("instance1", "MASTER", "instance2", "SLAVE"))));
    HelixDataAccessor dataAccessor = simulationAccessor();
    ValidationResult result = rule.validate(context(dataAccessor,
        InstanceConstants.InstanceOperation.EVACUATE, fixedProvider(baseline, candidate)));

    Assert.assertFalse(result.isFeasible());
    List<Violation> violations = result.getViolations();
    Assert.assertEquals(violations.size(), 1);
    Violation violation = violations.get(0);
    Assert.assertEquals(violation.getRuleId(),
        InstanceOperationRebalanceFeasibilityGuardrailRule.RULE_ID);
    Assert.assertEquals(violation.getResourceName(), RESOURCE);
    Assert.assertEquals(violation.getPartitionName(), RESOURCE + "_0");
    Assert.assertTrue(violation.getMessage().contains("from 3 to 2"),
        "message should report the replica drop: " + violation.getMessage());
  }

  @Test
  public void testUnknownTreatedLikeEvacuate() {
    // UNKNOWN removes the instance from the assignable pool just like EVACUATE, so an under-placed
    // partition is flagged the same way.
    Map<String, ResourceAssignment> baseline = ImmutableMap.of(RESOURCE, resourceAssignment(
        ImmutableMap.of(RESOURCE + "_0",
            ImmutableMap.of("instance0", "MASTER", "instance1", "SLAVE"))));
    Map<String, ResourceAssignment> candidate = ImmutableMap.of(RESOURCE, resourceAssignment(
        ImmutableMap.of(RESOURCE + "_0", ImmutableMap.of("instance1", "MASTER"))));
    HelixDataAccessor dataAccessor = simulationAccessor();
    ValidationResult result = rule.validate(context(dataAccessor,
        InstanceConstants.InstanceOperation.UNKNOWN, fixedProvider(baseline, candidate)));
    Assert.assertFalse(result.isFeasible());
    Assert.assertEquals(result.getViolations().get(0).getPartitionName(), RESOURCE + "_0");
  }

  @Test
  public void testBaselineProviderThrowsFailsClosed() {
    HelixDataAccessor dataAccessor = simulationAccessor();
    WagedAssignmentProvider provider =
        (cfg, instanceConfigs, liveInstances, idealStates, resourceConfigs) -> {
          throw new RuntimeException("cannot compute");
        };
    ValidationResult result = rule.validate(
        context(dataAccessor, InstanceConstants.InstanceOperation.EVACUATE, provider));
    Assert.assertFalse(result.isFeasible());
    Assert.assertTrue(result.getViolations().get(0).getMessage().contains("baseline"),
        "should fail closed citing the missing baseline: "
            + result.getViolations().get(0).getMessage());
  }

  @Test
  public void testCandidateProviderThrowsFailsClosed() {
    // Baseline succeeds but applying the operation makes WAGED unable to compute any assignment.
    Map<String, ResourceAssignment> baseline = ImmutableMap.of(RESOURCE, resourceAssignment(
        ImmutableMap.of(RESOURCE + "_0", ImmutableMap.of("instance0", "MASTER"))));
    HelixDataAccessor dataAccessor = simulationAccessor();
    WagedAssignmentProvider provider =
        (cfg, instanceConfigs, liveInstances, idealStates, resourceConfigs) -> {
          boolean candidate = instanceConfigs.stream()
              .filter(ic -> ic != null && INSTANCE.equals(ic.getInstanceName())).findFirst()
              .map(ic -> !ic.isAssignable()).orElse(true);
          if (candidate) {
            throw new RuntimeException("CAPACITY_DEFICIT");
          }
          return baseline;
        };
    ValidationResult result = rule.validate(
        context(dataAccessor, InstanceConstants.InstanceOperation.EVACUATE, provider));
    Assert.assertFalse(result.isFeasible());
    Assert.assertTrue(
        result.getViolations().get(0).getMessage().contains("unable to compute an assignment"),
        "should fail closed citing the uncomputable assignment: "
            + result.getViolations().get(0).getMessage());
  }

  @Test
  public void testViolationsCappedAtMax() {
    // 150 partitions each lose a replica -> 100 enumerated violations + 1 trailing overflow summary.
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
    ValidationResult result = rule.validate(context(dataAccessor,
        InstanceConstants.InstanceOperation.EVACUATE, fixedProvider(baseline, candidate)));

    Assert.assertFalse(result.isFeasible());
    List<Violation> violations = result.getViolations();
    Assert.assertEquals(violations.size(), 101);
    Violation overflow = violations.get(100);
    Assert.assertNull(overflow.getPartitionName());
    Assert.assertTrue(overflow.getMessage().contains("of 150"),
        "overflow summary should record the true total: " + overflow.getMessage());
  }

  // ---------------------------------------------------------------------------------------------
  // Cheap sound pre-checks: block obvious infeasibility without ever running the what-if.
  // ---------------------------------------------------------------------------------------------

  @Test
  public void testReplicaCountPrecheckBlocksWithoutSimulation() {
    // Draining instance0 leaves 2 assignable instances, but the resource needs 3 distinct instances
    // (replica=3) to place every partition -> WAGED would throw NO_CANDIDATE_NODE. The pre-check
    // blocks arithmetically; the provider must never run.
    HelixDataAccessor dataAccessor = precheckAccessor(enabledClusterConfig(),
        wagedIdealState(RESOURCE, 3, 3));
    ValidationResult result = rule.validate(
        context(dataAccessor, InstanceConstants.InstanceOperation.EVACUATE, PROVIDER_MUST_NOT_RUN));

    Assert.assertFalse(result.isFeasible());
    List<Violation> violations = result.getViolations();
    Assert.assertEquals(violations.size(), 1);
    Assert.assertEquals(violations.get(0).getResourceName(), RESOURCE);
    Assert.assertTrue(violations.get(0).getMessage().contains("NO_CANDIDATE_NODE"),
        "should cite the WAGED hard-constraint failure: " + violations.get(0).getMessage());
  }

  @Test
  public void testAggregateCapacityPrecheckBlocksWithoutSimulation() {
    // Enough instances for the replica count (replica=2, 2 remain), but the total weight of the
    // WAGED replicas (3 partitions x 2 replicas x 40 = 240 CU) exceeds the summed assignable
    // capacity (2 instances x 100 = 200 CU) -> WAGED would throw CAPACITY_DEFICIT. The pre-check
    // blocks arithmetically; the provider must never run.
    ClusterConfig clusterConfig = enabledClusterConfig();
    clusterConfig.setInstanceCapacityKeys(ImmutableList.of("CU"));
    clusterConfig.setDefaultInstanceCapacityMap(ImmutableMap.of("CU", 100));
    clusterConfig.setDefaultPartitionWeightMap(ImmutableMap.of("CU", 40));
    HelixDataAccessor dataAccessor = precheckAccessor(clusterConfig, wagedIdealState(RESOURCE, 2, 3));
    ValidationResult result = rule.validate(
        context(dataAccessor, InstanceConstants.InstanceOperation.EVACUATE, PROVIDER_MUST_NOT_RUN));

    Assert.assertFalse(result.isFeasible());
    List<Violation> violations = result.getViolations();
    Assert.assertEquals(violations.size(), 1);
    Assert.assertTrue(violations.get(0).getMessage().contains("CAPACITY_DEFICIT"),
        "should cite the WAGED hard-constraint failure: " + violations.get(0).getMessage());
  }

  @Test
  public void testAggregateCapacityPrecheckDefersWhenCapacityUnresolved() {
    // Capacity keys are required but neither the instances nor the cluster default supply them, so
    // WagedValidationUtil cannot resolve node capacity. Rather than risk a false block, the
    // capacity pre-check is inconclusive and defers to the what-if, which here certifies feasible.
    ClusterConfig clusterConfig = enabledClusterConfig();
    clusterConfig.setInstanceCapacityKeys(ImmutableList.of("CU"));
    clusterConfig.setDefaultPartitionWeightMap(ImmutableMap.of("CU", 40));
    HelixDataAccessor dataAccessor = precheckAccessor(clusterConfig, wagedIdealState(RESOURCE, 2, 3));
    Map<String, ResourceAssignment> steadyState = ImmutableMap.of(RESOURCE,
        resourceAssignment(ImmutableMap.of("p0", ImmutableMap.of("instance1", "MASTER"))));

    // A provider that must run (would throw only if the pre-check wrongly blocked) and returns an
    // unchanged assignment -> feasible.
    ValidationResult result = rule.validate(context(dataAccessor,
        InstanceConstants.InstanceOperation.EVACUATE, fixedProvider(steadyState, steadyState)));

    Assert.assertTrue(result.isFeasible());
  }

  // ---------------------------------------------------------------------------------------------
  // Helpers.
  // ---------------------------------------------------------------------------------------------

  private GuardrailContext context(HelixDataAccessor dataAccessor,
      InstanceConstants.InstanceOperation proposedOp, WagedAssignmentProvider provider) {
    return GuardrailContext.newBuilder(CLUSTER)
        .dataAccessor(dataAccessor)
        .instanceName(INSTANCE)
        .proposedInstanceOperation(proposedOp)
        .wagedAssignmentProvider(provider)
        .build();
  }

  // A fully-wired accessor for the pre-check path: the given (enabled) cluster config and WAGED
  // ideal state, an assignable target (instance0) plus two other assignable live instances, so that
  // draining the target leaves exactly two assignable instances.
  private HelixDataAccessor precheckAccessor(ClusterConfig clusterConfig, IdealState idealState) {
    HelixDataAccessor dataAccessor = mock(HelixDataAccessor.class);
    when(dataAccessor.keyBuilder()).thenReturn(BUILDER);
    doReturn(clusterConfig).when(dataAccessor).getProperty(BUILDER.clusterConfig());
    doReturn(assignableInstance(INSTANCE)).when(dataAccessor)
        .getProperty(BUILDER.instanceConfig(INSTANCE));
    doReturn(ImmutableList.of(idealState)).when(dataAccessor)
        .getChildValues(BUILDER.idealStates(), true);
    doReturn(ImmutableList.of()).when(dataAccessor).getChildValues(BUILDER.resourceConfigs(), true);
    doReturn(ImmutableList.of(assignableInstance(INSTANCE), assignableInstance("instance1"),
        assignableInstance("instance2"))).when(dataAccessor)
        .getChildValues(BUILDER.instanceConfigs(), true);
    doReturn(ImmutableList.of(INSTANCE, "instance1", "instance2")).when(dataAccessor)
        .getChildNames(BUILDER.liveInstances());
    return dataAccessor;
  }

  // A fully-wired accessor for the simulation path: enabled cluster, an assignable target instance,
  // one WAGED resource, a couple of live instances, and a small assignable instance pool.
  private HelixDataAccessor simulationAccessor() {
    HelixDataAccessor dataAccessor = mock(HelixDataAccessor.class);
    when(dataAccessor.keyBuilder()).thenReturn(BUILDER);
    doReturn(enabledClusterConfig()).when(dataAccessor).getProperty(BUILDER.clusterConfig());
    doReturn(assignableInstance(INSTANCE)).when(dataAccessor)
        .getProperty(BUILDER.instanceConfig(INSTANCE));
    doReturn(ImmutableList.of(wagedIdealState(RESOURCE))).when(dataAccessor)
        .getChildValues(BUILDER.idealStates(), true);
    doReturn(ImmutableList.of()).when(dataAccessor).getChildValues(BUILDER.resourceConfigs(), true);
    doReturn(ImmutableList.of(assignableInstance(INSTANCE), assignableInstance("instance1"),
        assignableInstance("instance2"))).when(dataAccessor)
        .getChildValues(BUILDER.instanceConfigs(), true);
    doReturn(ImmutableList.of(INSTANCE, "instance1", "instance2")).when(dataAccessor)
        .getChildNames(BUILDER.liveInstances());
    return dataAccessor;
  }

  // Returns the baseline assignment while the target instance in the passed configs is still
  // assignable, and the candidate assignment once the rule has flipped it non-assignable.
  private static WagedAssignmentProvider fixedProvider(Map<String, ResourceAssignment> baseline,
      Map<String, ResourceAssignment> candidate) {
    return (cfg, instanceConfigs, liveInstances, idealStates, resourceConfigs) -> {
      boolean isCandidate = instanceConfigs.stream()
          .filter(ic -> ic != null && INSTANCE.equals(ic.getInstanceName())).findFirst()
          .map(ic -> !ic.isAssignable()).orElse(true);
      return isCandidate ? candidate : baseline;
    };
  }

  private static ClusterConfig enabledClusterConfig() {
    ClusterConfig clusterConfig = new ClusterConfig(CLUSTER);
    clusterConfig.setInstanceOperationRebalanceGuardrailEnabled(true);
    return clusterConfig;
  }

  private static InstanceConfig assignableInstance(String name) {
    // A default InstanceConfig is ENABLE, i.e. assignable.
    return new InstanceConfig(name);
  }

  private static InstanceConfig instance(String name,
      InstanceConstants.InstanceOperation operation) {
    InstanceConfig instanceConfig = new InstanceConfig(name);
    instanceConfig.setInstanceOperation(operation);
    return instanceConfig;
  }

  private static IdealState wagedIdealState(String resource) {
    IdealState idealState = new IdealState(resource);
    idealState.setRebalancerClassName(WagedRebalancer.class.getName());
    return idealState;
  }

  private static IdealState wagedIdealState(String resource, int replicas, int numPartitions) {
    IdealState idealState = wagedIdealState(resource);
    idealState.setReplicas(Integer.toString(replicas));
    idealState.setNumPartitions(numPartitions);
    return idealState;
  }

  private static IdealState anyLiveInstanceWagedIdealState(String resource) {
    IdealState idealState = wagedIdealState(resource);
    // "ANY_LIVEINSTANCE" is exactly what IdealState.getReplicas() returns for such resources.
    idealState.setReplicas("ANY_LIVEINSTANCE");
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
