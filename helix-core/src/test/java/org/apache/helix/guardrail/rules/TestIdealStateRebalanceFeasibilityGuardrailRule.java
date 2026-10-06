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
 * Unit tests for {@link IdealStateRebalanceFeasibilityGuardrailRule}. Cluster state (cluster config,
 * the current WAGED ideal states, instance configs, live instances) is supplied through a mocked
 * {@link HelixDataAccessor}; the WAGED what-if is supplied through a stubbed
 * {@link WagedAssignmentProvider} that returns controlled baseline/candidate assignments (or throws),
 * so the rule's two checks and its short-circuit logic are exercised with no ZooKeeper and no
 * rebalancer. The stubbed provider distinguishes the candidate run from the baseline run by whether
 * the ideal-state list it is handed contains the proposed ideal state (the candidate list does).
 */
public class TestIdealStateRebalanceFeasibilityGuardrailRule {
  private static final String CLUSTER = "testCluster";
  private static final String RESOURCE = "testResource";
  private static final String OTHER_RESOURCE = "otherResource";
  private static final PropertyKey.Builder BUILDER = new PropertyKey.Builder(CLUSTER);

  // A provider that must never be invoked: any call fails the test. Used to prove the rule
  // short-circuits before ever running the (expensive) WAGED what-if.
  private static final WagedAssignmentProvider PROVIDER_MUST_NOT_RUN =
      (cfg, instanceConfigs, liveInstances, idealStates, resourceConfigs) -> {
        throw new AssertionError("WAGED what-if must not run on this path");
      };

  private final IdealStateRebalanceFeasibilityGuardrailRule rule =
      new IdealStateRebalanceFeasibilityGuardrailRule();

  // ---------------------------------------------------------------------------------------------
  // Short-circuit / not-applicable paths (no simulation).
  // ---------------------------------------------------------------------------------------------

  @Test
  public void testNullProposedIdealStateIsFeasible() {
    // No proposed ideal state in the context: this is not an ideal-state edit, so nothing to check.
    GuardrailContext context = GuardrailContext.newBuilder(CLUSTER)
        .dataAccessor(mock(HelixDataAccessor.class))
        .wagedAssignmentProvider(PROVIDER_MUST_NOT_RUN)
        .build();
    Assert.assertTrue(rule.validate(context).isFeasible());
  }

  @Test
  public void testNullProviderIsFeasible() {
    // Not wired for simulation: certify feasible rather than block every ideal-state edit.
    GuardrailContext context = GuardrailContext.newBuilder(CLUSTER)
        .dataAccessor(mock(HelixDataAccessor.class))
        .proposedIdealState(proposedWagedIdealState(RESOURCE, 3))
        .build();
    Assert.assertTrue(rule.validate(context).isFeasible());
  }

  @Test
  public void testNullClusterConfigIsFeasible() {
    HelixDataAccessor dataAccessor = mock(HelixDataAccessor.class);
    when(dataAccessor.keyBuilder()).thenReturn(BUILDER);
    doReturn(null).when(dataAccessor).getProperty(BUILDER.clusterConfig());
    Assert.assertTrue(rule
        .validate(context(dataAccessor, proposedWagedIdealState(RESOURCE, 3), PROVIDER_MUST_NOT_RUN))
        .isFeasible());
  }

  @Test
  public void testNonWagedProposedIsFeasible() {
    // A non-WAGED (e.g. SEMI_AUTO) proposed ideal state is not placed by the WAGED rebalancer, so it
    // is certified without any simulation.
    HelixDataAccessor dataAccessor = mock(HelixDataAccessor.class);
    when(dataAccessor.keyBuilder()).thenReturn(BUILDER);
    doReturn(clusterConfig()).when(dataAccessor).getProperty(BUILDER.clusterConfig());
    Assert.assertTrue(rule
        .validate(context(dataAccessor, new IdealState(RESOURCE), PROVIDER_MUST_NOT_RUN))
        .isFeasible());
  }

  @Test
  public void testAnyLiveInstanceProposedIsExempt() {
    // An ANY_LIVEINSTANCE WAGED resource keeps one replica per live instance by design; its replica
    // count is defined by the live set, never a capacity deficit, so it is exempt without simulation.
    IdealState proposed = proposedWagedIdealState(RESOURCE, 3);
    proposed.setReplicas("ANY_LIVEINSTANCE");
    HelixDataAccessor dataAccessor = mock(HelixDataAccessor.class);
    when(dataAccessor.keyBuilder()).thenReturn(BUILDER);
    doReturn(clusterConfig()).when(dataAccessor).getProperty(BUILDER.clusterConfig());
    Assert.assertTrue(rule.validate(context(dataAccessor, proposed, PROVIDER_MUST_NOT_RUN))
        .isFeasible());
  }

  // ---------------------------------------------------------------------------------------------
  // Simulation paths.
  // ---------------------------------------------------------------------------------------------

  @Test
  public void testReplicaCountFitsIsFeasible() {
    // The proposed ideal state asks for 2 replicas per partition and the candidate what-if places 2:
    // the edit is placeable, so it is feasible.
    IdealState proposed = proposedWagedIdealState(RESOURCE, 2);
    Map<String, ResourceAssignment> candidate = ImmutableMap.of(RESOURCE, resourceAssignment(RESOURCE,
        ImmutableMap.of(RESOURCE + "_0", ImmutableMap.of("instance0", "MASTER", "instance1", "SLAVE"),
            RESOURCE + "_1", ImmutableMap.of("instance1", "MASTER", "instance2", "SLAVE"))));
    HelixDataAccessor dataAccessor = simulationAccessor();
    ValidationResult result =
        rule.validate(context(dataAccessor, proposed, fixedProvider(proposed, candidate, candidate)));
    Assert.assertTrue(result.isFeasible());
  }

  @Test
  public void testReplicaIncreaseInfeasibleWhenUnplaceable() {
    // The proposed ideal state raises the replica count to 3, but the candidate what-if can only
    // place 2 for partition _0 -> a check-1 violation attributed to the edited resource's partition.
    IdealState proposed = proposedWagedIdealState(RESOURCE, 3);
    Map<String, ResourceAssignment> baseline = ImmutableMap.of(RESOURCE, resourceAssignment(RESOURCE,
        ImmutableMap.of(RESOURCE + "_0", ImmutableMap.of("instance0", "MASTER", "instance1", "SLAVE"))));
    Map<String, ResourceAssignment> candidate = ImmutableMap.of(RESOURCE, resourceAssignment(RESOURCE,
        ImmutableMap.of(RESOURCE + "_0", ImmutableMap.of("instance0", "MASTER", "instance1", "SLAVE"))));
    HelixDataAccessor dataAccessor = simulationAccessor();
    ValidationResult result =
        rule.validate(context(dataAccessor, proposed, fixedProvider(proposed, baseline, candidate)));

    Assert.assertFalse(result.isFeasible());
    List<Violation> violations = result.getViolations();
    Assert.assertEquals(violations.size(), 1);
    Violation violation = violations.get(0);
    Assert.assertEquals(violation.getRuleId(),
        IdealStateRebalanceFeasibilityGuardrailRule.RULE_ID);
    Assert.assertEquals(violation.getResourceName(), RESOURCE);
    Assert.assertEquals(violation.getPartitionName(), RESOURCE + "_0");
    Assert.assertTrue(violation.getMessage().contains("requires 3 replica(s)")
            && violation.getMessage().contains("only 2 can be placed"),
        "message should report the replica shortfall: " + violation.getMessage());
  }

  @Test
  public void testReplicaDecreaseNotFalselyFlagged() {
    // Baseline places 3 replicas; the edit lowers the replica count to 2 and the candidate places 2.
    // A deliberate decrease must NOT be flagged (placed == proposed demand), unlike a baseline-diff
    // rule which would wrongly see "3 -> 2" as a loss.
    IdealState proposed = proposedWagedIdealState(RESOURCE, 2);
    Map<String, ResourceAssignment> baseline = ImmutableMap.of(RESOURCE, resourceAssignment(RESOURCE,
        ImmutableMap.of(RESOURCE + "_0",
            ImmutableMap.of("instance0", "MASTER", "instance1", "SLAVE", "instance2", "SLAVE"))));
    Map<String, ResourceAssignment> candidate = ImmutableMap.of(RESOURCE, resourceAssignment(RESOURCE,
        ImmutableMap.of(RESOURCE + "_0", ImmutableMap.of("instance0", "MASTER", "instance1", "SLAVE"))));
    HelixDataAccessor dataAccessor = simulationAccessor();
    ValidationResult result =
        rule.validate(context(dataAccessor, proposed, fixedProvider(proposed, baseline, candidate)));
    Assert.assertTrue(result.isFeasible(),
        "a legitimate replica decrease must not be flagged as infeasible");
  }

  @Test
  public void testCollateralResourceLosesReplicasInfeasible() {
    // Check 2: a different WAGED resource loses a placeable replica between baseline and candidate as
    // collateral of the edit consuming capacity it needed -> a violation attributed to that resource.
    IdealState proposed = proposedWagedIdealState(RESOURCE, 2);
    Map<String, ResourceAssignment> baseline = ImmutableMap.of(
        RESOURCE, resourceAssignment(RESOURCE,
            ImmutableMap.of(RESOURCE + "_0", ImmutableMap.of("instance0", "MASTER"))),
        OTHER_RESOURCE, resourceAssignment(OTHER_RESOURCE,
            ImmutableMap.of(OTHER_RESOURCE + "_0",
                ImmutableMap.of("instance0", "MASTER", "instance1", "SLAVE", "instance2", "SLAVE"))));
    Map<String, ResourceAssignment> candidate = ImmutableMap.of(
        RESOURCE, resourceAssignment(RESOURCE,
            ImmutableMap.of(RESOURCE + "_0", ImmutableMap.of("instance0", "MASTER", "instance1", "SLAVE"))),
        OTHER_RESOURCE, resourceAssignment(OTHER_RESOURCE,
            ImmutableMap.of(OTHER_RESOURCE + "_0",
                ImmutableMap.of("instance0", "MASTER", "instance1", "SLAVE"))));
    HelixDataAccessor dataAccessor =
        accessor(ImmutableList.of(wagedIdealState(RESOURCE), wagedIdealState(OTHER_RESOURCE)));
    ValidationResult result =
        rule.validate(context(dataAccessor, proposed, fixedProvider(proposed, baseline, candidate)));

    Assert.assertFalse(result.isFeasible());
    List<Violation> violations = result.getViolations();
    Assert.assertEquals(violations.size(), 1);
    Violation violation = violations.get(0);
    Assert.assertEquals(violation.getResourceName(), OTHER_RESOURCE);
    Assert.assertEquals(violation.getPartitionName(), OTHER_RESOURCE + "_0");
    Assert.assertTrue(violation.getMessage().contains("from 3 to 2"),
        "message should report the collateral replica drop: " + violation.getMessage());
  }

  @Test
  public void testNewlyWagedResourceAddedAndUnplaceable() {
    // The edit newly makes the resource WAGED (no current WAGED ideal state for it): the proposed
    // ideal state is ADDED to the candidate set (not replaced), the baseline run is skipped because
    // there is no current WAGED resource, and an unplaceable replica count is still caught by check 1.
    IdealState proposed = proposedWagedIdealState(RESOURCE, 3);
    Map<String, ResourceAssignment> candidate = ImmutableMap.of(RESOURCE, resourceAssignment(RESOURCE,
        ImmutableMap.of(RESOURCE + "_0", ImmutableMap.of("instance0", "MASTER", "instance1", "SLAVE"))));
    // Baseline ideal states contain only a non-WAGED resource, so collectWagedIdealStates is empty.
    HelixDataAccessor dataAccessor = accessor(ImmutableList.of(new IdealState("nonWagedResource")));
    ValidationResult result =
        rule.validate(context(dataAccessor, proposed, fixedProvider(proposed, candidate, candidate)));

    Assert.assertFalse(result.isFeasible());
    Assert.assertEquals(result.getViolations().size(), 1);
    Assert.assertEquals(result.getViolations().get(0).getPartitionName(), RESOURCE + "_0");
  }

  @Test
  public void testBaselineUncomputableFailsOpen() {
    // The baseline what-if cannot be computed (the cluster may already be unhealthy). Because the rule
    // is always on, it fails open (certifies) rather than block every edit on an already-unhealthy
    // cluster: a deficit cannot be attributed to this edit without a baseline to compare against.
    IdealState proposed = proposedWagedIdealState(RESOURCE, 3);
    HelixDataAccessor dataAccessor = simulationAccessor();
    WagedAssignmentProvider provider =
        (cfg, instanceConfigs, liveInstances, idealStates, resourceConfigs) -> {
          throw new RuntimeException("cannot compute");
        };
    ValidationResult result = rule.validate(context(dataAccessor, proposed, provider));
    Assert.assertTrue(result.isFeasible(),
        "should fail open when no baseline WAGED assignment can be computed");
  }

  @Test
  public void testCandidateProviderThrowsFailsClosed() {
    // Baseline succeeds but applying the edit makes WAGED unable to compute any assignment (e.g. a
    // cluster-wide CAPACITY_DEFICIT) -- the strongest signal the proposed ideal state is unplaceable.
    IdealState proposed = proposedWagedIdealState(RESOURCE, 3);
    Map<String, ResourceAssignment> baseline = ImmutableMap.of(RESOURCE, resourceAssignment(RESOURCE,
        ImmutableMap.of(RESOURCE + "_0", ImmutableMap.of("instance0", "MASTER"))));
    HelixDataAccessor dataAccessor = simulationAccessor();
    WagedAssignmentProvider provider =
        (cfg, instanceConfigs, liveInstances, idealStates, resourceConfigs) -> {
          if (idealStates.contains(proposed)) {
            throw new RuntimeException("CAPACITY_DEFICIT");
          }
          return baseline;
        };
    ValidationResult result = rule.validate(context(dataAccessor, proposed, provider));
    Assert.assertFalse(result.isFeasible());
    Assert.assertTrue(
        result.getViolations().get(0).getMessage().contains("unable to compute an assignment"),
        "should fail closed citing the uncomputable assignment: "
            + result.getViolations().get(0).getMessage());
  }

  @Test
  public void testViolationsCappedAtMax() {
    // 150 partitions of the edited resource are under-placed -> 10 enumerated violations + 1 trailing
    // overflow summary that records the true total.
    IdealState proposed = proposedWagedIdealState(RESOURCE, 2);
    Map<String, Map<String, String>> candidateMap = new java.util.LinkedHashMap<>();
    for (int i = 0; i < 150; i++) {
      candidateMap.put(RESOURCE + "_" + i, ImmutableMap.of("instance0", "MASTER"));
    }
    Map<String, ResourceAssignment> candidate =
        ImmutableMap.of(RESOURCE, resourceAssignment(RESOURCE, candidateMap));
    HelixDataAccessor dataAccessor = simulationAccessor();
    ValidationResult result =
        rule.validate(context(dataAccessor, proposed, fixedProvider(proposed, candidate, candidate)));

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

  private GuardrailContext context(HelixDataAccessor dataAccessor, IdealState proposedIdealState,
      WagedAssignmentProvider provider) {
    return GuardrailContext.newBuilder(CLUSTER)
        .dataAccessor(dataAccessor)
        .proposedIdealState(proposedIdealState)
        .wagedAssignmentProvider(provider)
        .build();
  }

  // A fully-wired accessor whose only current WAGED resource is RESOURCE.
  private HelixDataAccessor simulationAccessor() {
    return accessor(ImmutableList.of(wagedIdealState(RESOURCE)));
  }

  // A fully-wired accessor for the simulation path: an enabled cluster, the given current WAGED ideal
  // states, no resource configs, and a small assignable instance pool / live set.
  private HelixDataAccessor accessor(List<IdealState> baselineIdealStates) {
    HelixDataAccessor dataAccessor = mock(HelixDataAccessor.class);
    when(dataAccessor.keyBuilder()).thenReturn(BUILDER);
    doReturn(clusterConfig()).when(dataAccessor).getProperty(BUILDER.clusterConfig());
    doReturn(baselineIdealStates).when(dataAccessor).getChildValues(BUILDER.idealStates(), true);
    doReturn(ImmutableList.of()).when(dataAccessor).getChildValues(BUILDER.resourceConfigs(), true);
    doReturn(ImmutableList.of(assignableInstance("instance0"), assignableInstance("instance1"),
        assignableInstance("instance2"))).when(dataAccessor)
        .getChildValues(BUILDER.instanceConfigs(), true);
    doReturn(ImmutableList.of("instance0", "instance1", "instance2")).when(dataAccessor)
        .getChildNames(BUILDER.liveInstances());
    return dataAccessor;
  }

  // Returns the candidate assignment when handed the candidate ideal-state list (the one containing
  // the proposed ideal state), and the baseline assignment otherwise. Mirrors how
  // WagedRebalanceFeasibilityWhatIf builds the candidate list by swapping in / adding the proposal.
  private static WagedAssignmentProvider fixedProvider(IdealState proposed,
      Map<String, ResourceAssignment> baseline, Map<String, ResourceAssignment> candidate) {
    return (cfg, instanceConfigs, liveInstances, idealStates, resourceConfigs) ->
        idealStates.contains(proposed) ? candidate : baseline;
  }

  private static ClusterConfig clusterConfig() {
    return new ClusterConfig(CLUSTER);
  }

  private static InstanceConfig assignableInstance(String name) {
    // A default InstanceConfig is ENABLE, i.e. assignable.
    return new InstanceConfig(name);
  }

  private static IdealState wagedIdealState(String resource) {
    IdealState idealState = new IdealState(resource);
    idealState.setRebalancerClassName(WagedRebalancer.class.getName());
    // isWagedEnabled requires FULL_AUTO in addition to the WAGED rebalancer class; real WAGED
    // resources are always FULL_AUTO.
    idealState.setRebalanceMode(IdealState.RebalanceMode.FULL_AUTO);
    return idealState;
  }

  private static IdealState proposedWagedIdealState(String resource, int replicaCount) {
    IdealState idealState = wagedIdealState(resource);
    idealState.setReplicas(String.valueOf(replicaCount));
    return idealState;
  }

  private static ResourceAssignment resourceAssignment(String resource,
      Map<String, Map<String, String>> partitions) {
    ResourceAssignment resourceAssignment = new ResourceAssignment(resource);
    for (String partitionName : partitions.keySet()) {
      resourceAssignment.addReplicaMap(new Partition(partitionName), partitions.get(partitionName));
    }
    return resourceAssignment;
  }
}
