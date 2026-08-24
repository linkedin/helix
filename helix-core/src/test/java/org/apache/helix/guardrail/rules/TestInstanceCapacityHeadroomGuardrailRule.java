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

import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
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
import org.apache.helix.model.ClusterConfig;
import org.apache.helix.model.IdealState;
import org.apache.helix.model.InstanceConfig;
import org.apache.helix.model.ResourceConfig;
import org.testng.Assert;
import org.testng.annotations.Test;

import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Unit tests for {@link InstanceCapacityHeadroomGuardrailRule}. Cluster state (cluster config,
 * instance configs, ideal states, resource configs) is supplied through a mocked
 * {@link HelixDataAccessor}; the proposed instance-config delta and target instance name are passed
 * directly through the {@link GuardrailContext}.
 */
public class TestInstanceCapacityHeadroomGuardrailRule {
  private static final String CLUSTER = "testCluster";
  private static final String TARGET = "instance0";
  private static final String RESOURCE = "testResource";
  private static final PropertyKey.Builder BUILDER = new PropertyKey.Builder(CLUSTER);

  private final InstanceCapacityHeadroomGuardrailRule rule =
      new InstanceCapacityHeadroomGuardrailRule();

  @Test
  public void testNullInstanceConfigIsFeasible() {
    GuardrailContext context = GuardrailContext.newBuilder(CLUSTER)
        .dataAccessor(mock(HelixDataAccessor.class))
        .instanceName(TARGET)
        .build();
    Assert.assertTrue(rule.validate(context).isFeasible());
  }

  @Test
  public void testNoCapacityDeltaIsFeasible() {
    // A topology-only update carries no capacity map; the rule is out of scope.
    GuardrailContext context = contextWith(mock(HelixDataAccessor.class), new InstanceConfig(TARGET));
    Assert.assertTrue(rule.validate(context).isFeasible());
  }

  @Test
  public void testNullClusterConfigIsFeasible() {
    HelixDataAccessor dataAccessor = mock(HelixDataAccessor.class);
    when(dataAccessor.keyBuilder()).thenReturn(BUILDER);
    doReturn(null).when(dataAccessor).getProperty(BUILDER.clusterConfig());

    ValidationResult result =
        rule.validate(contextWith(dataAccessor, delta(ImmutableMap.of("FOO", 10))));
    Assert.assertTrue(result.isFeasible());
  }

  @Test
  public void testNoCapacityKeysIsFeasible() {
    // Cluster does not use the WAGED capacity model.
    ClusterConfig clusterConfig = new ClusterConfig(CLUSTER);
    clusterConfig.setInstanceCapacityHeadroomGuardrailEnabled(true);
    HelixDataAccessor dataAccessor = mockAccessor(clusterConfig,
        ImmutableList.of(instanceConfig(TARGET, ImmutableMap.of("FOO", 100))),
        ImmutableList.of(), ImmutableList.of());

    ValidationResult result =
        rule.validate(contextWith(dataAccessor, delta(ImmutableMap.of("FOO", 10))));
    Assert.assertTrue(result.isFeasible());
  }

  @Test
  public void testCapacityRaiseIsFeasible() throws IOException {
    // Raising capacity can never newly break feasibility, even under heavy committed demand.
    ClusterConfig clusterConfig = clusterConfig("FOO");
    HelixDataAccessor dataAccessor = mockAccessor(clusterConfig, threeInstances(100),
        ImmutableList.of(wagedIdealState(RESOURCE, 10, 2)),
        ImmutableList.of(resourceConfig(RESOURCE, ImmutableMap.of("FOO", 13))));

    // FOO 100 -> 150 on the target instance is a raise, not a reduction.
    ValidationResult result =
        rule.validate(contextWith(dataAccessor, delta(ImmutableMap.of("FOO", 150))));
    Assert.assertTrue(result.isFeasible());
  }

  @Test
  public void testReductionWithinHeadroomIsFeasible() throws IOException {
    // supply=300, demand=10*2*13=260. Cutting instance0 to 80 leaves 280 >= 260.
    ClusterConfig clusterConfig = clusterConfig("FOO");
    HelixDataAccessor dataAccessor = mockAccessor(clusterConfig, threeInstances(100),
        ImmutableList.of(wagedIdealState(RESOURCE, 10, 2)),
        ImmutableList.of(resourceConfig(RESOURCE, ImmutableMap.of("FOO", 13))));

    ValidationResult result =
        rule.validate(contextWith(dataAccessor, delta(ImmutableMap.of("FOO", 80))));
    Assert.assertTrue(result.isFeasible());
    Assert.assertTrue(result.getViolations().isEmpty());
  }

  @Test
  public void testReductionBelowDemandIsInfeasible() throws IOException {
    // supply=300, demand=260. Cutting instance0 to 50 leaves 250 < 260 -> unassigned partitions.
    ClusterConfig clusterConfig = clusterConfig("FOO");
    HelixDataAccessor dataAccessor = mockAccessor(clusterConfig, threeInstances(100),
        ImmutableList.of(wagedIdealState(RESOURCE, 10, 2)),
        ImmutableList.of(resourceConfig(RESOURCE, ImmutableMap.of("FOO", 13))));

    ValidationResult result =
        rule.validate(contextWith(dataAccessor, delta(ImmutableMap.of("FOO", 50))));

    Assert.assertFalse(result.isFeasible());
    Assert.assertEquals(result.getViolations().size(), 1);
    Violation violation = result.getViolations().get(0);
    Assert.assertEquals(violation.getRuleId(), InstanceCapacityHeadroomGuardrailRule.RULE_ID);
    Assert.assertTrue(violation.getMessage().contains(TARGET));
    Assert.assertTrue(violation.getMessage().contains("FOO"));
    Assert.assertTrue(violation.getMessage().contains("250"));
    Assert.assertTrue(violation.getMessage().contains("260"));
  }

  @Test
  public void testReductionWithNoWagedResourcesIsFeasible() {
    // A reduction with zero committed WAGED demand is always safe.
    ClusterConfig clusterConfig = clusterConfig("FOO");
    HelixDataAccessor dataAccessor = mockAccessor(clusterConfig, threeInstances(100),
        ImmutableList.of(), ImmutableList.of());

    ValidationResult result =
        rule.validate(contextWith(dataAccessor, delta(ImmutableMap.of("FOO", 1))));
    Assert.assertTrue(result.isFeasible());
  }

  @Test
  public void testNonWagedResourceIgnored() throws IOException {
    // The only resource is SEMI_AUTO, so it contributes no WAGED demand: the reduction is allowed
    // even though its raw weight sum would otherwise exceed the reduced capacity.
    ClusterConfig clusterConfig = clusterConfig("FOO");
    IdealState semiAuto = wagedIdealState(RESOURCE, 10, 2);
    semiAuto.setRebalanceMode(IdealState.RebalanceMode.SEMI_AUTO);
    HelixDataAccessor dataAccessor = mockAccessor(clusterConfig, threeInstances(100),
        ImmutableList.of(semiAuto),
        ImmutableList.of(resourceConfig(RESOURCE, ImmutableMap.of("FOO", 13))));

    ValidationResult result =
        rule.validate(contextWith(dataAccessor, delta(ImmutableMap.of("FOO", 50))));
    Assert.assertTrue(result.isFeasible());
  }

  @Test
  public void testOnlyReducedDimensionsAreEvaluated() throws IOException {
    // BAR is already over-committed (demand 360 > supply 300) for unrelated reasons. Reducing only
    // FOO (still within FOO headroom) must not be blocked on BAR's pre-existing shortfall.
    ClusterConfig clusterConfig = clusterConfig("FOO", "BAR");
    HelixDataAccessor dataAccessor = mockAccessor(clusterConfig,
        ImmutableList.of(
            instanceConfig("instance0", ImmutableMap.of("FOO", 100, "BAR", 100)),
            instanceConfig("instance1", ImmutableMap.of("FOO", 100, "BAR", 100)),
            instanceConfig("instance2", ImmutableMap.of("FOO", 100, "BAR", 100))),
        ImmutableList.of(wagedIdealState(RESOURCE, 10, 2)),
        ImmutableList.of(resourceConfig(RESOURCE, ImmutableMap.of("FOO", 13, "BAR", 18))));

    // FOO 100 -> 80 while BAR is re-sent unchanged at 100. updateInstanceConfig replaces the whole
    // capacity map, so BAR must be included to stay at 100; supply_after FOO = 280 >= demand 260 and
    // BAR is not reduced, so only FOO is checked.
    ValidationResult result = rule.validate(
        contextWith(dataAccessor, delta(ImmutableMap.of("FOO", 80, "BAR", 100))));
    Assert.assertTrue(result.isFeasible());
  }

  @Test
  public void testMalformedWeightMapFailsClosed() throws IOException {
    ClusterConfig clusterConfig = clusterConfig("FOO");
    ResourceConfig malformed = new ResourceConfig(RESOURCE);
    malformed.getRecord()
        .setMapField("PARTITION_CAPACITY_MAP", ImmutableMap.of(RESOURCE + "_0", "not-json"));
    HelixDataAccessor dataAccessor = mockAccessor(clusterConfig, threeInstances(100),
        ImmutableList.of(wagedIdealState(RESOURCE, 10, 2)), ImmutableList.of(malformed));

    ValidationResult result =
        rule.validate(contextWith(dataAccessor, delta(ImmutableMap.of("FOO", 1))));

    Assert.assertFalse(result.isFeasible());
    Assert.assertEquals(result.getViolations().get(0).getRuleId(),
        InstanceCapacityHeadroomGuardrailRule.RULE_ID);
  }

  @Test
  public void testUnknownTargetInstanceIsFeasible() throws IOException {
    // The target instance is not among the existing configs; there is nothing to reduce.
    ClusterConfig clusterConfig = clusterConfig("FOO");
    HelixDataAccessor dataAccessor = mockAccessor(clusterConfig,
        ImmutableList.of(instanceConfig("instance1", ImmutableMap.of("FOO", 100))),
        ImmutableList.of(wagedIdealState(RESOURCE, 10, 2)),
        ImmutableList.of(resourceConfig(RESOURCE, ImmutableMap.of("FOO", 13))));

    ValidationResult result =
        rule.validate(contextWith(dataAccessor, delta(ImmutableMap.of("FOO", 1))));
    Assert.assertTrue(result.isFeasible());
  }

  @Test
  public void testNonAssignableInstanceExcludedFromSupply() throws IOException {
    // instance2 is EVACUATE (non-assignable), so WAGED cannot draw on its capacity: only instance0
    // and instance1 count toward supply. demand = 10*2*10 = 200. Cutting instance0 100 -> 50 leaves
    // assignable supply 150 < 200 -> blocked, even though counting the EVACUATE node would give 250.
    ClusterConfig clusterConfig = clusterConfig("FOO");
    HelixDataAccessor dataAccessor = mockAccessor(clusterConfig,
        ImmutableList.of(
            instanceConfig("instance0", ImmutableMap.of("FOO", 100)),
            instanceConfig("instance1", ImmutableMap.of("FOO", 100)),
            instanceConfig("instance2", ImmutableMap.of("FOO", 100),
                InstanceConstants.InstanceOperation.EVACUATE)),
        ImmutableList.of(wagedIdealState(RESOURCE, 10, 2)),
        ImmutableList.of(resourceConfig(RESOURCE, ImmutableMap.of("FOO", 10))));

    ValidationResult result =
        rule.validate(contextWith(dataAccessor, delta(ImmutableMap.of("FOO", 50))));

    Assert.assertFalse(result.isFeasible());
    Assert.assertEquals(result.getViolations().get(0).getRuleId(),
        InstanceCapacityHeadroomGuardrailRule.RULE_ID);
    Assert.assertTrue(result.getViolations().get(0).getMessage().contains("150"));
    Assert.assertTrue(result.getViolations().get(0).getMessage().contains("200"));
  }

  @Test
  public void testNonAssignableTargetIsFeasible() throws IOException {
    // The target itself is EVACUATE, so WAGED is not drawing on its capacity; reducing it cannot
    // lower WAGED supply. The update is allowed even though demand (260) exceeds the other two
    // assignable instances' combined 200.
    ClusterConfig clusterConfig = clusterConfig("FOO");
    HelixDataAccessor dataAccessor = mockAccessor(clusterConfig,
        ImmutableList.of(
            instanceConfig(TARGET, ImmutableMap.of("FOO", 100),
                InstanceConstants.InstanceOperation.EVACUATE),
            instanceConfig("instance1", ImmutableMap.of("FOO", 100)),
            instanceConfig("instance2", ImmutableMap.of("FOO", 100))),
        ImmutableList.of(wagedIdealState(RESOURCE, 10, 2)),
        ImmutableList.of(resourceConfig(RESOURCE, ImmutableMap.of("FOO", 13))));

    ValidationResult result =
        rule.validate(contextWith(dataAccessor, delta(ImmutableMap.of("FOO", 1))));
    Assert.assertTrue(result.isFeasible());
  }

  @Test
  public void testReductionToExactDemandIsFeasible() throws IOException {
    // Boundary: supply_after == demand is allowed (the rule blocks only supply < demand). demand =
    // 10*2*13 = 260; cutting instance0 100 -> 60 leaves 60+100+100 = 260, exactly meeting demand.
    ClusterConfig clusterConfig = clusterConfig("FOO");
    HelixDataAccessor dataAccessor = mockAccessor(clusterConfig, threeInstances(100),
        ImmutableList.of(wagedIdealState(RESOURCE, 10, 2)),
        ImmutableList.of(resourceConfig(RESOURCE, ImmutableMap.of("FOO", 13))));

    ValidationResult result =
        rule.validate(contextWith(dataAccessor, delta(ImmutableMap.of("FOO", 60))));
    Assert.assertTrue(result.isFeasible());
  }

  @Test
  public void testDemandComputedAsLongAvoidsIntOverflow() throws IOException {
    // demand = 20000 partitions * 3 replicas * 100000 weight = 6,000,000,000, which overflows a
    // 32-bit int (wrapping to ~1.7e9). Cutting instance0 1e9 -> 0 leaves assignable supply 2e9:
    // above the wrapped value (would wrongly pass) but far below the true 6e9 (correctly blocked).
    // Asserting the block, and the true demand in the message, proves demand is accumulated as long.
    ClusterConfig clusterConfig = clusterConfig("FOO");
    HelixDataAccessor dataAccessor = mockAccessor(clusterConfig,
        ImmutableList.of(
            instanceConfig("instance0", ImmutableMap.of("FOO", 1_000_000_000)),
            instanceConfig("instance1", ImmutableMap.of("FOO", 1_000_000_000)),
            instanceConfig("instance2", ImmutableMap.of("FOO", 1_000_000_000))),
        ImmutableList.of(wagedIdealState(RESOURCE, 20000, 3)),
        ImmutableList.of(resourceConfig(RESOURCE, ImmutableMap.of("FOO", 100000))));

    ValidationResult result =
        rule.validate(contextWith(dataAccessor, delta(ImmutableMap.of("FOO", 0))));

    Assert.assertFalse(result.isFeasible());
    Assert.assertTrue(result.getViolations().get(0).getMessage().contains("6000000000"));
  }

  @Test
  public void testGuardrailDisabledByDefaultIsFeasible() throws IOException {
    // The guard rail is opt-in: with the flag left at its default (off), even an over-reduction that
    // would otherwise be blocked (cf. testReductionBelowDemandIsInfeasible) is allowed through. This
    // is the kill switch operators rely on to back the rule out during an incident.
    ClusterConfig clusterConfig = new ClusterConfig(CLUSTER);
    clusterConfig.setInstanceCapacityKeys(ImmutableList.of("FOO"));
    HelixDataAccessor dataAccessor = mockAccessor(clusterConfig, threeInstances(100),
        ImmutableList.of(wagedIdealState(RESOURCE, 10, 2)),
        ImmutableList.of(resourceConfig(RESOURCE, ImmutableMap.of("FOO", 13))));

    ValidationResult result =
        rule.validate(contextWith(dataAccessor, delta(ImmutableMap.of("FOO", 50))));
    Assert.assertTrue(result.isFeasible());
  }

  @Test
  public void testDroppedDimensionIsTreatedAsReduction() throws IOException {
    // updateInstanceConfig REPLACES the whole capacity map, so a payload that omits BAR wipes BAR on
    // the target even though FOO is unchanged. supply_after BAR = 0 + 100 + 100 = 200 < demand 360, so
    // dropping an entire dimension (the most destructive change) must be blocked, not silently passed.
    ClusterConfig clusterConfig = clusterConfig("FOO", "BAR");
    HelixDataAccessor dataAccessor = mockAccessor(clusterConfig,
        ImmutableList.of(
            instanceConfig("instance0", ImmutableMap.of("FOO", 100, "BAR", 100)),
            instanceConfig("instance1", ImmutableMap.of("FOO", 100, "BAR", 100)),
            instanceConfig("instance2", ImmutableMap.of("FOO", 100, "BAR", 100))),
        ImmutableList.of(wagedIdealState(RESOURCE, 10, 2)),
        ImmutableList.of(resourceConfig(RESOURCE, ImmutableMap.of("FOO", 13, "BAR", 18))));

    // Delta keeps FOO at 100 but omits BAR entirely.
    ValidationResult result =
        rule.validate(contextWith(dataAccessor, delta(ImmutableMap.of("FOO", 100))));

    Assert.assertFalse(result.isFeasible());
    Assert.assertEquals(result.getViolations().size(), 1);
    Violation violation = result.getViolations().get(0);
    Assert.assertEquals(violation.getRuleId(), InstanceCapacityHeadroomGuardrailRule.RULE_ID);
    Assert.assertTrue(violation.getMessage().contains("BAR"));
    Assert.assertTrue(violation.getMessage().contains("200"));
    Assert.assertTrue(violation.getMessage().contains("360"));
  }

  @Test
  public void testPerPartitionWeightsBelowDefaultAreNotOvercounted() throws IOException {
    // 30 partitions x 10 replicas. The resource DEFAULT weight is 100, but every partition overrides it
    // to 1, so real demand is 30 * 1 * 10 = 300 (not the 100 * 30 * 10 = 30000 a DEFAULT-only read
    // would compute). supply_after = 150 + 200 + 200 = 550 >= 300, so reducing instance0 200 -> 150 is
    // safe and must be allowed; the old DEFAULT-only estimate would have wrongly blocked it.
    ClusterConfig clusterConfig = clusterConfig("FOO");
    HelixDataAccessor dataAccessor = mockAccessor(clusterConfig,
        ImmutableList.of(
            instanceConfig("instance0", ImmutableMap.of("FOO", 200)),
            instanceConfig("instance1", ImmutableMap.of("FOO", 200)),
            instanceConfig("instance2", ImmutableMap.of("FOO", 200))),
        ImmutableList.of(wagedIdealState(RESOURCE, 30, 10)),
        ImmutableList.of(resourceConfigWithOverrides(RESOURCE, ImmutableMap.of("FOO", 100), 30,
            ImmutableMap.of("FOO", 1))));

    ValidationResult result =
        rule.validate(contextWith(dataAccessor, delta(ImmutableMap.of("FOO", 150))));
    Assert.assertTrue(result.isFeasible());
  }

  @Test
  public void testMultipleShortDimensionsAllReportedWorstFirst() throws IOException {
    // Reducing both FOO and BAR leaves both short: FOO 250 < 260 (deficit 10), BAR 250 < 360 (deficit
    // 110). Both must be reported in one verdict, worst deficit first (BAR before FOO), so the operator
    // fixes everything in a single pass instead of one 400 at a time.
    ClusterConfig clusterConfig = clusterConfig("FOO", "BAR");
    HelixDataAccessor dataAccessor = mockAccessor(clusterConfig,
        ImmutableList.of(
            instanceConfig("instance0", ImmutableMap.of("FOO", 100, "BAR", 100)),
            instanceConfig("instance1", ImmutableMap.of("FOO", 100, "BAR", 100)),
            instanceConfig("instance2", ImmutableMap.of("FOO", 100, "BAR", 100))),
        ImmutableList.of(wagedIdealState(RESOURCE, 10, 2)),
        ImmutableList.of(resourceConfig(RESOURCE, ImmutableMap.of("FOO", 13, "BAR", 18))));

    ValidationResult result = rule.validate(
        contextWith(dataAccessor, delta(ImmutableMap.of("FOO", 50, "BAR", 50))));

    Assert.assertFalse(result.isFeasible());
    Assert.assertEquals(result.getViolations().size(), 2);
    Assert.assertTrue(result.getViolations().get(0).getMessage().contains("BAR"));
    Assert.assertTrue(result.getViolations().get(1).getMessage().contains("FOO"));
    // Defect #6: no force=true hint, since forcing the reduction through is what causes the shortfall.
    Assert.assertFalse(result.getViolations().get(0).getMessage().contains("force"));
  }

  private GuardrailContext contextWith(HelixDataAccessor dataAccessor,
      InstanceConfig proposedInstanceConfig) {
    return GuardrailContext.newBuilder(CLUSTER)
        .dataAccessor(dataAccessor)
        .instanceName(TARGET)
        .proposedInstanceConfig(proposedInstanceConfig)
        .build();
  }

  private HelixDataAccessor mockAccessor(ClusterConfig clusterConfig,
      List<InstanceConfig> instanceConfigs, List<IdealState> idealStates,
      List<ResourceConfig> resourceConfigs) {
    HelixDataAccessor dataAccessor = mock(HelixDataAccessor.class);
    when(dataAccessor.keyBuilder()).thenReturn(BUILDER);
    doReturn(clusterConfig).when(dataAccessor).getProperty(BUILDER.clusterConfig());
    doReturn(instanceConfigs).when(dataAccessor).getChildValues(BUILDER.instanceConfigs(), true);
    doReturn(idealStates).when(dataAccessor).getChildValues(BUILDER.idealStates(), true);
    doReturn(resourceConfigs).when(dataAccessor).getChildValues(BUILDER.resourceConfigs(), true);
    return dataAccessor;
  }

  private static ClusterConfig clusterConfig(String... capacityKeys) {
    ClusterConfig clusterConfig = new ClusterConfig(CLUSTER);
    clusterConfig.setInstanceCapacityKeys(Arrays.asList(capacityKeys));
    // The guard rail is opt-in (disabled by default); enable it so the behavioral tests exercise it.
    clusterConfig.setInstanceCapacityHeadroomGuardrailEnabled(true);
    return clusterConfig;
  }

  private static List<InstanceConfig> threeInstances(int fooCapacity) {
    return ImmutableList.of(
        instanceConfig("instance0", ImmutableMap.of("FOO", fooCapacity)),
        instanceConfig("instance1", ImmutableMap.of("FOO", fooCapacity)),
        instanceConfig("instance2", ImmutableMap.of("FOO", fooCapacity)));
  }

  private static InstanceConfig instanceConfig(String name, Map<String, Integer> capacity) {
    InstanceConfig instanceConfig = new InstanceConfig(name);
    instanceConfig.setInstanceCapacityMap(capacity);
    return instanceConfig;
  }

  private static InstanceConfig instanceConfig(String name, Map<String, Integer> capacity,
      InstanceConstants.InstanceOperation operation) {
    InstanceConfig instanceConfig = instanceConfig(name, capacity);
    instanceConfig.setInstanceOperation(operation);
    return instanceConfig;
  }

  private static InstanceConfig delta(Map<String, Integer> capacity) {
    InstanceConfig instanceConfig = new InstanceConfig(TARGET);
    instanceConfig.setInstanceCapacityMap(capacity);
    return instanceConfig;
  }

  private static IdealState wagedIdealState(String resource, int numPartitions, int replicas) {
    IdealState idealState = new IdealState(resource);
    idealState.setRebalanceMode(IdealState.RebalanceMode.FULL_AUTO);
    idealState.setRebalancerClassName(WagedRebalancer.class.getName());
    idealState.setReplicas(String.valueOf(replicas));
    idealState.setNumPartitions(numPartitions);
    return idealState;
  }

  private static ResourceConfig resourceConfig(String resource, Map<String, Integer> defaultWeight)
      throws IOException {
    ResourceConfig resourceConfig = new ResourceConfig(resource);
    resourceConfig.setPartitionCapacityMap(
        ImmutableMap.of(ResourceConfig.DEFAULT_PARTITION_KEY, defaultWeight));
    return resourceConfig;
  }

  private static ResourceConfig resourceConfigWithOverrides(String resource,
      Map<String, Integer> defaultWeight, int numPartitions, Map<String, Integer> perPartitionWeight)
      throws IOException {
    Map<String, Map<String, Integer>> capacityMap = new HashMap<>();
    capacityMap.put(ResourceConfig.DEFAULT_PARTITION_KEY, defaultWeight);
    for (int i = 0; i < numPartitions; i++) {
      capacityMap.put(resource + "_" + i, perPartitionWeight);
    }
    ResourceConfig resourceConfig = new ResourceConfig(resource);
    resourceConfig.setPartitionCapacityMap(capacityMap);
    return resourceConfig;
  }
}
