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
 * Unit tests for {@link PartitionWeightCapacityGuardrailRule}. Cluster state (cluster config +
 * instance configs) is supplied through a mocked {@link HelixDataAccessor}; the proposed resource
 * config and ideal state are passed directly through the {@link GuardrailContext}.
 */
public class TestPartitionWeightCapacityGuardrailRule {
  private static final String CLUSTER = "testCluster";
  private static final String RESOURCE = "testResource";
  private static final PropertyKey.Builder BUILDER = new PropertyKey.Builder(CLUSTER);

  private final PartitionWeightCapacityGuardrailRule rule =
      new PartitionWeightCapacityGuardrailRule();

  @Test
  public void testNullResourceConfigIsFeasible() {
    GuardrailContext context = GuardrailContext.newBuilder(CLUSTER)
        .dataAccessor(mock(HelixDataAccessor.class))
        .build();
    Assert.assertTrue(rule.validate(context).isFeasible());
  }

  @Test
  public void testNullClusterConfigIsFeasible() throws IOException {
    HelixDataAccessor dataAccessor = mock(HelixDataAccessor.class);
    when(dataAccessor.keyBuilder()).thenReturn(BUILDER);
    doReturn(null).when(dataAccessor).getProperty(BUILDER.clusterConfig());

    ValidationResult result = rule.validate(contextWith(dataAccessor,
        resourceConfig(ImmutableMap.of(ResourceConfig.DEFAULT_PARTITION_KEY,
            ImmutableMap.of("FOO", 1000)))));
    Assert.assertTrue(result.isFeasible());
  }

  @Test
  public void testNoCapacityKeysIsFeasible() throws IOException {
    // Cluster does not use the WAGED capacity model, so weights are not interpreted.
    ClusterConfig clusterConfig = new ClusterConfig(CLUSTER);
    HelixDataAccessor dataAccessor = mockAccessor(clusterConfig,
        ImmutableList.of(instanceConfig("instance0", ImmutableMap.of("FOO", 100))));

    ValidationResult result = rule.validate(contextWith(dataAccessor,
        resourceConfig(ImmutableMap.of(ResourceConfig.DEFAULT_PARTITION_KEY,
            ImmutableMap.of("FOO", 1000)))));
    Assert.assertTrue(result.isFeasible());
  }

  @Test
  public void testNoInstanceCapacityIsFeasible() throws IOException {
    // Capacity keys are declared but no instance advertises capacity: nothing to compare against.
    ClusterConfig clusterConfig = new ClusterConfig(CLUSTER);
    clusterConfig.setInstanceCapacityKeys(Arrays.asList("FOO", "BAR"));
    HelixDataAccessor dataAccessor = mockAccessor(clusterConfig, ImmutableList.of());

    ValidationResult result = rule.validate(contextWith(dataAccessor,
        resourceConfig(ImmutableMap.of(ResourceConfig.DEFAULT_PARTITION_KEY,
            ImmutableMap.of("FOO", 1000)))));
    Assert.assertTrue(result.isFeasible());
  }

  @Test
  public void testWeightWithinCapacityIsFeasible() throws IOException {
    ClusterConfig clusterConfig = new ClusterConfig(CLUSTER);
    clusterConfig.setInstanceCapacityKeys(Arrays.asList("FOO", "BAR"));
    HelixDataAccessor dataAccessor = mockAccessor(clusterConfig, ImmutableList.of(
        instanceConfig("instance0", ImmutableMap.of("FOO", 100, "BAR", 100))));

    ValidationResult result = rule.validate(contextWith(dataAccessor,
        resourceConfig(ImmutableMap.of(ResourceConfig.DEFAULT_PARTITION_KEY,
            ImmutableMap.of("FOO", 100, "BAR", 100)))));
    Assert.assertTrue(result.isFeasible());
    Assert.assertTrue(result.getViolations().isEmpty());
  }

  @Test
  public void testWeightExceedsCapacityIsInfeasible() throws IOException {
    ClusterConfig clusterConfig = new ClusterConfig(CLUSTER);
    clusterConfig.setInstanceCapacityKeys(Arrays.asList("FOO", "BAR"));
    HelixDataAccessor dataAccessor = mockAccessor(clusterConfig, ImmutableList.of(
        instanceConfig("instance0", ImmutableMap.of("FOO", 100, "BAR", 100)),
        instanceConfig("instance1", ImmutableMap.of("FOO", 100, "BAR", 100))));

    // FOO weight 1000 exceeds the largest instance FOO capacity (100).
    ValidationResult result = rule.validate(contextWith(dataAccessor,
        resourceConfig(ImmutableMap.of(ResourceConfig.DEFAULT_PARTITION_KEY,
            ImmutableMap.of("FOO", 1000, "BAR", 100)))));

    Assert.assertFalse(result.isFeasible());
    Assert.assertEquals(result.getViolations().size(), 1);
    Violation violation = result.getViolations().get(0);
    Assert.assertEquals(violation.getRuleId(), PartitionWeightCapacityGuardrailRule.RULE_ID);
    Assert.assertEquals(violation.getResourceName(), RESOURCE);
    // A DEFAULT-scoped weight applies to every partition, so it is reported unscoped.
    Assert.assertNull(violation.getPartitionName());
    Assert.assertTrue(violation.getMessage().contains("FOO"));
    Assert.assertTrue(violation.getMessage().contains("1000"));
    Assert.assertTrue(violation.getMessage().contains("100"));
  }

  @Test
  public void testPerPartitionOverrideExceedsIsInfeasible() throws IOException {
    ClusterConfig clusterConfig = new ClusterConfig(CLUSTER);
    clusterConfig.setInstanceCapacityKeys(Arrays.asList("FOO"));
    HelixDataAccessor dataAccessor = mockAccessor(clusterConfig,
        ImmutableList.of(instanceConfig("instance0", ImmutableMap.of("FOO", 100))));

    // DEFAULT weight is fine (50 <= 100), but the explicit override for testResource_0 is not.
    ResourceConfig resourceConfig = resourceConfig(ImmutableMap.of(
        ResourceConfig.DEFAULT_PARTITION_KEY, ImmutableMap.of("FOO", 50),
        RESOURCE + "_0", ImmutableMap.of("FOO", 1000)));
    ValidationResult result = rule.validate(contextWith(dataAccessor, resourceConfig));

    Assert.assertFalse(result.isFeasible());
    Violation violation = result.getViolations().get(0);
    Assert.assertEquals(violation.getRuleId(), PartitionWeightCapacityGuardrailRule.RULE_ID);
    Assert.assertEquals(violation.getPartitionName(), RESOURCE + "_0");
  }

  @Test
  public void testMaxCapacityAcrossInstancesUsed() throws IOException {
    // The largest instance in each dimension is what matters, not the smallest: a weight of 500 is
    // placeable as long as one instance has capacity >= 500.
    ClusterConfig clusterConfig = new ClusterConfig(CLUSTER);
    clusterConfig.setInstanceCapacityKeys(Arrays.asList("FOO"));
    HelixDataAccessor dataAccessor = mockAccessor(clusterConfig, ImmutableList.of(
        instanceConfig("instance0", ImmutableMap.of("FOO", 100)),
        instanceConfig("instance1", ImmutableMap.of("FOO", 1000))));

    ValidationResult result = rule.validate(contextWith(dataAccessor,
        resourceConfig(ImmutableMap.of(ResourceConfig.DEFAULT_PARTITION_KEY,
            ImmutableMap.of("FOO", 500)))));
    Assert.assertTrue(result.isFeasible());
  }

  @Test
  public void testGhostPartitionKeyIsIgnored() throws IOException {
    // The capacity map names a partition (testResource_99999) the resource does not have: only _0
    // and _1 are real. WAGED ignores such stale/typo'd entries at placement time, so this rule must
    // too, even though the ghost's weight (1000) far exceeds the largest instance capacity (100).
    ClusterConfig clusterConfig = new ClusterConfig(CLUSTER);
    clusterConfig.setInstanceCapacityKeys(Arrays.asList("FOO"));
    HelixDataAccessor dataAccessor = mockAccessor(clusterConfig,
        ImmutableList.of(instanceConfig("instance0", ImmutableMap.of("FOO", 100))));

    ResourceConfig resourceConfig = resourceConfig(ImmutableMap.of(
        ResourceConfig.DEFAULT_PARTITION_KEY, ImmutableMap.of("FOO", 50),
        RESOURCE + "_99999", ImmutableMap.of("FOO", 1000)));
    ValidationResult result = rule.validate(contextWith(dataAccessor, resourceConfig, 2));

    Assert.assertTrue(result.isFeasible());
    Assert.assertTrue(result.getViolations().isEmpty());
  }

  @Test
  public void testRealPartitionStillFlaggedAlongsideGhost() throws IOException {
    // Skipping ghosts must not mask a genuinely unplaceable real partition: testResource_99999 is
    // ignored, but the real testResource_1 override (999 > 100) is still caught.
    ClusterConfig clusterConfig = new ClusterConfig(CLUSTER);
    clusterConfig.setInstanceCapacityKeys(Arrays.asList("FOO"));
    HelixDataAccessor dataAccessor = mockAccessor(clusterConfig,
        ImmutableList.of(instanceConfig("instance0", ImmutableMap.of("FOO", 100))));

    ResourceConfig resourceConfig = resourceConfig(ImmutableMap.of(
        ResourceConfig.DEFAULT_PARTITION_KEY, ImmutableMap.of("FOO", 50),
        RESOURCE + "_99999", ImmutableMap.of("FOO", 1000),
        RESOURCE + "_1", ImmutableMap.of("FOO", 999)));
    ValidationResult result = rule.validate(contextWith(dataAccessor, resourceConfig, 2));

    Assert.assertFalse(result.isFeasible());
    Violation violation = result.getViolations().get(0);
    Assert.assertEquals(violation.getRuleId(), PartitionWeightCapacityGuardrailRule.RULE_ID);
    Assert.assertEquals(violation.getPartitionName(), RESOURCE + "_1");
  }

  @Test
  public void testNonAssignableInstanceCapacityIgnored() throws IOException {
    // WAGED only places on assignable instances. A large-capacity instance that is EVACUATE (being
    // decommissioned) is not assignable, so its capacity must not count toward placeability: with
    // only the assignable instance's 100 capacity, a DEFAULT weight of 5000 is unplaceable.
    ClusterConfig clusterConfig = new ClusterConfig(CLUSTER);
    clusterConfig.setInstanceCapacityKeys(Arrays.asList("FOO"));
    HelixDataAccessor dataAccessor = mockAccessor(clusterConfig, ImmutableList.of(
        instanceConfig("assignable", ImmutableMap.of("FOO", 100)),
        instanceConfig("evacuating", ImmutableMap.of("FOO", 10000),
            InstanceConstants.InstanceOperation.EVACUATE)));

    ValidationResult result = rule.validate(contextWith(dataAccessor,
        resourceConfig(ImmutableMap.of(ResourceConfig.DEFAULT_PARTITION_KEY,
            ImmutableMap.of("FOO", 5000)))));

    Assert.assertFalse(result.isFeasible());
    Violation violation = result.getViolations().get(0);
    Assert.assertEquals(violation.getRuleId(), PartitionWeightCapacityGuardrailRule.RULE_ID);
    // The 10000-capacity evacuating instance is ignored, so the reported ceiling is the assignable
    // instance's 100 rather than 10000.
    Assert.assertTrue(violation.getMessage().contains("capacity 100"));
  }

  @Test
  public void testMissingCapacityDimensionNotBlamedOnResource() throws IOException {
    // The cluster declares two capacity keys but the instances only advertise FOO. A BAR weight must
    // not be blamed on the resource as "exceeds capacity 0"; a capacity key missing from the
    // instances is an instance-side misconfiguration reported separately, so the rule defers on that
    // dimension (mirroring the missing-weight skip) and only checks the dimensions instances cover.
    ClusterConfig clusterConfig = new ClusterConfig(CLUSTER);
    clusterConfig.setInstanceCapacityKeys(Arrays.asList("FOO", "BAR"));
    HelixDataAccessor dataAccessor = mockAccessor(clusterConfig,
        ImmutableList.of(instanceConfig("instance0", ImmutableMap.of("FOO", 100))));

    ResourceConfig resourceConfig = resourceConfig(ImmutableMap.of(
        ResourceConfig.DEFAULT_PARTITION_KEY, ImmutableMap.of("FOO", 50, "BAR", 1)));
    ValidationResult result = rule.validate(contextWith(dataAccessor, resourceConfig));

    Assert.assertTrue(result.isFeasible());
    Assert.assertTrue(result.getViolations().isEmpty());
  }

  @Test
  public void testMultipleViolationsAllReportedInFixedOrder() throws IOException {
    // Both declared dimensions are over capacity. The rule must report both (not just the first) and
    // in a stable order matching the cluster's capacity-key order, so a caller sees every problem in
    // one response instead of fixing one, resubmitting, and discovering the next.
    ClusterConfig clusterConfig = new ClusterConfig(CLUSTER);
    clusterConfig.setInstanceCapacityKeys(Arrays.asList("FOO", "BAR"));
    HelixDataAccessor dataAccessor = mockAccessor(clusterConfig,
        ImmutableList.of(instanceConfig("instance0", ImmutableMap.of("FOO", 100, "BAR", 100))));

    ResourceConfig resourceConfig = resourceConfig(ImmutableMap.of(
        ResourceConfig.DEFAULT_PARTITION_KEY, ImmutableMap.of("FOO", 5000, "BAR", 9000)));
    ValidationResult result = rule.validate(contextWith(dataAccessor, resourceConfig));

    Assert.assertFalse(result.isFeasible());
    Assert.assertEquals(result.getViolations().size(), 2);
    // Fixed order: FOO before BAR, matching the declared capacity-key order.
    Assert.assertTrue(result.getViolations().get(0).getMessage().contains("'FOO'"));
    Assert.assertTrue(result.getViolations().get(1).getMessage().contains("'BAR'"));
    // No force=true suggestion: forcing an unplaceable resource is what triggers the cluster-wide
    // capacity deficit the rule exists to prevent.
    Assert.assertFalse(result.getViolations().get(0).getMessage().contains("force"));
  }

  @Test
  public void testCustomizedIdealStatePartitionsUsedDirectly() throws IOException {
    // A CUSTOMIZED ideal state already carries its partitions (in its map fields), so
    // getPartitionSet() is non-empty and realPartitionNames returns those names directly rather than
    // reconstructing <resource>_<index> from the partition count. A weight on one of those real
    // partitions is evaluated, while a canonical-scheme name that is NOT among them is a ghost.
    ClusterConfig clusterConfig = new ClusterConfig(CLUSTER);
    clusterConfig.setInstanceCapacityKeys(Arrays.asList("FOO"));
    HelixDataAccessor dataAccessor = mockAccessor(clusterConfig,
        ImmutableList.of(instanceConfig("instance0", ImmutableMap.of("FOO", 100))));

    IdealState idealState = new IdealState(RESOURCE);
    idealState.setRebalanceMode(IdealState.RebalanceMode.CUSTOMIZED);
    idealState.setPartitionState("realPartition", "instance0", "MASTER");

    ResourceConfig resourceConfig = resourceConfig(ImmutableMap.of(
        ResourceConfig.DEFAULT_PARTITION_KEY, ImmutableMap.of("FOO", 1),
        "realPartition", ImmutableMap.of("FOO", 5000),
        RESOURCE + "_0", ImmutableMap.of("FOO", 5000)));

    ValidationResult result = rule.validate(contextWith(dataAccessor, resourceConfig, idealState));

    // Only the real (declared) partition is reported; RESOURCE_0 is a ghost under this ideal state.
    Assert.assertFalse(result.isFeasible());
    Assert.assertEquals(result.getViolations().size(), 1);
    Assert.assertEquals(result.getViolations().get(0).getPartitionName(), "realPartition");
  }

  @Test
  public void testZeroPartitionsSkipsAllExplicitWeights() throws IOException {
    // NUM_PARTITIONS=0 with an empty assignment reconstructs an empty partition set, so every
    // explicit per-partition weight names a partition the resource does not have and is skipped as a
    // ghost. The verdict is feasible even though the weight would exceed capacity for a real
    // partition.
    ClusterConfig clusterConfig = new ClusterConfig(CLUSTER);
    clusterConfig.setInstanceCapacityKeys(Arrays.asList("FOO"));
    HelixDataAccessor dataAccessor = mockAccessor(clusterConfig,
        ImmutableList.of(instanceConfig("instance0", ImmutableMap.of("FOO", 100))));

    ResourceConfig resourceConfig = resourceConfig(ImmutableMap.of(
        ResourceConfig.DEFAULT_PARTITION_KEY, ImmutableMap.of("FOO", 1),
        RESOURCE + "_0", ImmutableMap.of("FOO", 5000)));

    ValidationResult result = rule.validate(contextWith(dataAccessor, resourceConfig, 0));
    Assert.assertTrue(result.isFeasible());
  }

  @Test
  public void testViolationsCappedWithSuppressedCount() throws IOException {
    // A resource that breaches capacity on many partitions at once must not enumerate every
    // violation; the list is capped and a trailing summary records how many were omitted.
    int partitionCount = 150;
    ClusterConfig clusterConfig = new ClusterConfig(CLUSTER);
    clusterConfig.setInstanceCapacityKeys(Arrays.asList("FOO"));
    HelixDataAccessor dataAccessor = mockAccessor(clusterConfig,
        ImmutableList.of(instanceConfig("instance0", ImmutableMap.of("FOO", 100))));

    Map<String, Map<String, Integer>> weights = new HashMap<>();
    weights.put(ResourceConfig.DEFAULT_PARTITION_KEY, ImmutableMap.of("FOO", 1));
    for (int i = 0; i < partitionCount; i++) {
      weights.put(RESOURCE + "_" + i, ImmutableMap.of("FOO", 5000));
    }
    ResourceConfig resourceConfig = resourceConfig(weights);

    ValidationResult result =
        rule.validate(contextWith(dataAccessor, resourceConfig, partitionCount));

    Assert.assertFalse(result.isFeasible());
    // 100 enumerated violations plus a single trailing summary entry.
    Assert.assertEquals(result.getViolations().size(), 101);
    Violation summary = result.getViolations().get(result.getViolations().size() - 1);
    Assert.assertTrue(summary.getMessage().contains("150"));
    Assert.assertTrue(summary.getMessage().contains("omitted"));
    // The summary is not scoped to any single partition.
    Assert.assertNull(summary.getPartitionName());
  }

  private GuardrailContext contextWith(HelixDataAccessor dataAccessor,
      ResourceConfig proposedResourceConfig) {
    // Default to a single-partition resource so the canonical testResource_0 partition is real.
    return contextWith(dataAccessor, proposedResourceConfig, 1);
  }

  private GuardrailContext contextWith(HelixDataAccessor dataAccessor,
      ResourceConfig proposedResourceConfig, int numPartitions) {
    // Mirror a freshly-proposed WAGED ideal state: partition count is set but the assignment (and
    // thus getPartitionSet()) is still empty, so the rule reconstructs names from numPartitions.
    IdealState idealState = new IdealState(RESOURCE);
    idealState.setRebalanceMode(IdealState.RebalanceMode.FULL_AUTO);
    idealState.setNumPartitions(numPartitions);
    return GuardrailContext.newBuilder(CLUSTER)
        .dataAccessor(dataAccessor)
        .proposedResourceConfig(proposedResourceConfig)
        .proposedIdealState(idealState)
        .build();
  }

  private GuardrailContext contextWith(HelixDataAccessor dataAccessor,
      ResourceConfig proposedResourceConfig, IdealState proposedIdealState) {
    // Use a caller-supplied ideal state (e.g. a CUSTOMIZED one with an already-populated partition
    // set) instead of one reconstructed from a partition count.
    return GuardrailContext.newBuilder(CLUSTER)
        .dataAccessor(dataAccessor)
        .proposedResourceConfig(proposedResourceConfig)
        .proposedIdealState(proposedIdealState)
        .build();
  }

  private HelixDataAccessor mockAccessor(ClusterConfig clusterConfig,
      List<InstanceConfig> instanceConfigs) {
    HelixDataAccessor dataAccessor = mock(HelixDataAccessor.class);
    when(dataAccessor.keyBuilder()).thenReturn(BUILDER);
    doReturn(clusterConfig).when(dataAccessor).getProperty(BUILDER.clusterConfig());
    doReturn(instanceConfigs).when(dataAccessor).getChildValues(BUILDER.instanceConfigs(), true);
    return dataAccessor;
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

  private static ResourceConfig resourceConfig(Map<String, Map<String, Integer>> partitionCapacity)
      throws IOException {
    ResourceConfig resourceConfig = new ResourceConfig(RESOURCE);
    resourceConfig.setPartitionCapacityMap(partitionCapacity);
    return resourceConfig;
  }
}
