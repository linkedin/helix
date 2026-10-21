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
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.apache.helix.HelixDataAccessor;
import org.apache.helix.HelixException;
import org.apache.helix.PropertyKey;
import org.apache.helix.constants.InstanceConstants;
import org.apache.helix.guardrail.GuardrailContext;
import org.apache.helix.guardrail.GuardrailPipeline;
import org.apache.helix.guardrail.ValidationResult;
import org.apache.helix.guardrail.Violation;
import org.apache.helix.model.ClusterConfig;
import org.apache.helix.model.IdealState;
import org.apache.helix.model.InstanceConfig;
import org.apache.helix.model.ResourceConfig;
import org.testng.Assert;
import org.testng.annotations.Test;

import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Unit tests for {@link CapacityKeyConsistencyGuardrailRule}, which certifies that a WAGED mutation
 * cannot leave the cluster's capacity model unsatisfiable. The rule runs on several write paths: a
 * WAGED resource add and the bulk {@code enableWagedRebalanceForAllResources} flip (both validated
 * against the committed cluster config) and a cluster-config update that changes the capacity keys
 * (validated against the proposed, not-yet-written config). On the add and cluster-config paths it
 * checks the <em>instance</em> side only (the resource side is already validated by
 * {@code ZKHelixAdmin#addResourceWithWeight} before the write); on the bulk flip it checks
 * <em>both</em> the instance side and -- because legacy resources flipped in bulk never went through
 * {@code addResourceWithWeight} -- the resource side, naming each offending resource. Cluster state
 * (cluster config, instance configs, and, for the bulk path, ideal states and resource configs) is
 * supplied through a mocked {@link HelixDataAccessor}; the proposed resource config or proposed
 * cluster config is passed directly through the {@link GuardrailContext}.
 */
public class TestCapacityKeyConsistencyGuardrailRule {
  private static final String CLUSTER = "testCluster";
  private static final String RESOURCE = "testResource";
  private static final PropertyKey.Builder BUILDER = new PropertyKey.Builder(CLUSTER);

  private final CapacityKeyConsistencyGuardrailRule rule =
      new CapacityKeyConsistencyGuardrailRule();

  @Test
  public void testNullResourceConfigIsFeasible() {
    // Not a resource-add invocation: nothing for this rule to certify.
    GuardrailContext context = GuardrailContext.newBuilder(CLUSTER)
        .dataAccessor(mock(HelixDataAccessor.class))
        .build();
    Assert.assertTrue(rule.validate(context).isFeasible());
  }

  @Test
  public void testNullClusterConfigIsFeasible() {
    HelixDataAccessor dataAccessor = mock(HelixDataAccessor.class);
    when(dataAccessor.keyBuilder()).thenReturn(BUILDER);
    doReturn(null).when(dataAccessor).getProperty(BUILDER.clusterConfig());

    Assert.assertTrue(rule.validate(contextWith(dataAccessor, resourceConfig())).isFeasible());
  }

  @Test
  public void testNoCapacityKeysIsFeasible() {
    // Cluster does not use the WAGED capacity model, so there are no required keys.
    ClusterConfig clusterConfig = new ClusterConfig(CLUSTER);
    HelixDataAccessor dataAccessor = mockAccessor(clusterConfig,
        ImmutableList.of(instanceConfig("instance0", ImmutableMap.of("FOO", 100))));

    Assert.assertTrue(rule.validate(contextWith(dataAccessor, resourceConfig())).isFeasible());
  }

  @Test
  public void testAllInstancesDeclareAllKeysIsFeasible() {
    ClusterConfig clusterConfig = clusterConfig("FOO", "BAR");
    HelixDataAccessor dataAccessor = mockAccessor(clusterConfig, ImmutableList.of(
        instanceConfig("instance0", ImmutableMap.of("FOO", 100, "BAR", 100)),
        instanceConfig("instance1", ImmutableMap.of("FOO", 100, "BAR", 100))));

    Assert.assertTrue(rule.validate(contextWith(dataAccessor, resourceConfig())).isFeasible());
  }

  @Test
  public void testInstanceMissingKeyIsInfeasible() {
    // instance0 omits BAR: WAGED cannot build a model that includes it, so the resource would be
    // accepted and never placed. This is the instance-side gap the sibling weight rule and the admin
    // API's resource-side validation both leave uncovered.
    ClusterConfig clusterConfig = clusterConfig("FOO", "BAR");
    HelixDataAccessor dataAccessor = mockAccessor(clusterConfig,
        ImmutableList.of(instanceConfig("instance0", ImmutableMap.of("FOO", 100))));

    ValidationResult result = rule.validate(contextWith(dataAccessor, resourceConfig()));

    Assert.assertFalse(result.isFeasible());
    Violation violation = result.getViolations().get(0);
    Assert.assertEquals(violation.getRuleId(), CapacityKeyConsistencyGuardrailRule.RULE_ID);
    Assert.assertEquals(violation.getResourceName(), RESOURCE);
    Assert.assertTrue(violation.getMessage().contains("instance0"));
    Assert.assertTrue(violation.getMessage().contains("BAR"));
  }

  @Test
  public void testClusterDefaultInstanceCapacityCoversKey() {
    // instance0 omits BAR, but the cluster default instance capacity supplies it, so the merged
    // effective capacity covers both keys.
    ClusterConfig clusterConfig = clusterConfig("FOO", "BAR");
    clusterConfig.setDefaultInstanceCapacityMap(ImmutableMap.of("BAR", 100));
    HelixDataAccessor dataAccessor = mockAccessor(clusterConfig,
        ImmutableList.of(instanceConfig("instance0", ImmutableMap.of("FOO", 100))));

    Assert.assertTrue(rule.validate(contextWith(dataAccessor, resourceConfig())).isFeasible());
  }

  @Test
  public void testNonAssignableInstanceMissingKeyIgnored() {
    // instance1 is EVACUATE and omits BAR, but WAGED never places on it, so its gap is irrelevant;
    // the assignable instance0 covers both keys.
    ClusterConfig clusterConfig = clusterConfig("FOO", "BAR");
    HelixDataAccessor dataAccessor = mockAccessor(clusterConfig, ImmutableList.of(
        instanceConfig("instance0", ImmutableMap.of("FOO", 100, "BAR", 100)),
        instanceConfig("instance1", ImmutableMap.of("FOO", 100),
            InstanceConstants.InstanceOperation.EVACUATE)));

    Assert.assertTrue(rule.validate(contextWith(dataAccessor, resourceConfig())).isFeasible());
  }

  @Test
  public void testMultipleInstancesMissingKeyReportedAndOrdered() {
    // Two assignable instances each omit BAR: both are reported, in deterministic instance-name
    // order regardless of the order the accessor returns them.
    ClusterConfig clusterConfig = clusterConfig("FOO", "BAR");
    HelixDataAccessor dataAccessor = mockAccessor(clusterConfig, ImmutableList.of(
        instanceConfig("instance1", ImmutableMap.of("FOO", 100)),
        instanceConfig("instance0", ImmutableMap.of("FOO", 100))));

    ValidationResult result = rule.validate(contextWith(dataAccessor, resourceConfig()));

    Assert.assertFalse(result.isFeasible());
    Assert.assertEquals(result.getViolations().size(), 2);
    Assert.assertTrue(result.getViolations().get(0).getMessage().contains("instance0"));
    Assert.assertTrue(result.getViolations().get(1).getMessage().contains("instance1"));
  }

  @Test
  public void testNullInstanceConfigInListIsSkipped() {
    // A null entry from the accessor (e.g. a znode removed mid-read) must not NPE; the real
    // assignable instance is still validated.
    ClusterConfig clusterConfig = clusterConfig("FOO", "BAR");
    HelixDataAccessor dataAccessor = mockAccessor(clusterConfig, Arrays.asList(
        null, instanceConfig("instance0", ImmutableMap.of("FOO", 100))));

    ValidationResult result = rule.validate(contextWith(dataAccessor, resourceConfig()));

    Assert.assertFalse(result.isFeasible());
    Assert.assertEquals(result.getViolations().size(), 1);
    Assert.assertTrue(result.getViolations().get(0).getMessage().contains("instance0"));
  }

  @Test
  public void testViolationsCappedWithOverflowSummary() {
    // 101 assignable instances each omit BAR. The per-instance violations are capped at
    // MAX_REPORTED_VIOLATIONS (100) and a single trailing summary reports how many were omitted, so a
    // pathological cluster (a key missing on every instance) cannot produce an unbounded 400 body.
    ClusterConfig clusterConfig = clusterConfig("FOO", "BAR");
    List<InstanceConfig> instanceConfigs = new ArrayList<>();
    for (int i = 0; i < 101; i++) {
      instanceConfigs.add(instanceConfig("instance" + i, ImmutableMap.of("FOO", 100)));
    }
    HelixDataAccessor dataAccessor = mockAccessor(clusterConfig, instanceConfigs);

    ValidationResult result = rule.validate(contextWith(dataAccessor, resourceConfig()));

    Assert.assertFalse(result.isFeasible());
    // 100 per-instance violations + 1 trailing overflow summary.
    Assert.assertEquals(result.getViolations().size(), 101);
    Assert.assertTrue(result.getViolations().get(100).getMessage().contains("were omitted"));
  }

  @Test
  public void testProposedClusterConfigMissingKeyIsInfeasible() {
    // Cluster-config path: the committed config declares no capacity keys (the resource path would be
    // feasible), but the PROPOSED cluster config adds FOO+BAR and instance0 omits BAR. Evaluating the
    // proposed config makes this infeasible, proving the rule checks the not-yet-written config.
    HelixDataAccessor dataAccessor = mockAccessor(new ClusterConfig(CLUSTER),
        ImmutableList.of(instanceConfig("instance0", ImmutableMap.of("FOO", 100))));

    ValidationResult result =
        rule.validate(contextWithClusterConfig(dataAccessor, clusterConfig("FOO", "BAR")));

    Assert.assertFalse(result.isFeasible());
    Violation violation = result.getViolations().get(0);
    Assert.assertEquals(violation.getRuleId(), CapacityKeyConsistencyGuardrailRule.RULE_ID);
    // No resource is being added on the cluster-config path, so the violation carries no resource.
    Assert.assertNull(violation.getResourceName());
    Assert.assertTrue(violation.getMessage().contains("instance0"));
    Assert.assertTrue(violation.getMessage().contains("BAR"));
    Assert.assertTrue(violation.getMessage().contains("cluster config change"));
  }

  @Test
  public void testProposedClusterConfigAllKeysPresentIsFeasible() {
    // Every assignable instance declares both proposed keys, so adding them is safe.
    HelixDataAccessor dataAccessor = mockAccessor(new ClusterConfig(CLUSTER), ImmutableList.of(
        instanceConfig("instance0", ImmutableMap.of("FOO", 100, "BAR", 100))));

    Assert.assertTrue(rule.validate(
        contextWithClusterConfig(dataAccessor, clusterConfig("FOO", "BAR"))).isFeasible());
  }

  @Test
  public void testProposedClusterConfigRemovingAllKeysIsFeasible() {
    // The committed config requires FOO (instance0 has an empty capacity map, which the resource path
    // would reject), but the proposed config clears the capacity keys, so nothing is required. This
    // exercises both the proposed-config precedence and the empty-keys short-circuit.
    HelixDataAccessor dataAccessor = mockAccessor(clusterConfig("FOO"),
        ImmutableList.of(instanceConfig("instance0", ImmutableMap.of())));

    Assert.assertTrue(rule.validate(
        contextWithClusterConfig(dataAccessor, new ClusterConfig(CLUSTER))).isFeasible());
  }

  @Test
  public void testClusterConfigCapacityUnchangedGrandfathersPreexistingGap() {
    // instance0 omits BAR, so the cluster already has a capacity gap. This update leaves
    // INSTANCE_CAPACITY_KEYS and DEFAULT_INSTANCE_CAPACITY_MAP unchanged (it touches some other
    // field), so it cannot create or widen the gap; re-litigating it would reject an unrelated edit.
    // The rule certifies it (feasible) instead of blocking on the pre-existing gap.
    ClusterConfig current = clusterConfig("FOO", "BAR");
    ClusterConfig proposed = clusterConfig("FOO", "BAR");
    HelixDataAccessor dataAccessor = mockAccessor(current,
        ImmutableList.of(instanceConfig("instance0", ImmutableMap.of("FOO", 100))));

    Assert.assertTrue(
        rule.validate(contextWithClusterConfigs(dataAccessor, current, proposed)).isFeasible());
  }

  @Test
  public void testClusterConfigCapacityKeysChangedEnforces() {
    // The update adds BAR to INSTANCE_CAPACITY_KEYS, which instance0 does not declare: the change
    // itself introduces the gap, so the rule must still block it even though a current config exists.
    ClusterConfig current = clusterConfig("FOO");
    ClusterConfig proposed = clusterConfig("FOO", "BAR");
    HelixDataAccessor dataAccessor = mockAccessor(current,
        ImmutableList.of(instanceConfig("instance0", ImmutableMap.of("FOO", 100))));

    ValidationResult result =
        rule.validate(contextWithClusterConfigs(dataAccessor, current, proposed));

    Assert.assertFalse(result.isFeasible());
    Assert.assertTrue(result.getViolations().get(0).getMessage().contains("instance0"));
    Assert.assertTrue(result.getViolations().get(0).getMessage().contains("BAR"));
  }

  @Test
  public void testClusterConfigDefaultCapacityMapChangeEnforces() {
    // INSTANCE_CAPACITY_KEYS is unchanged ([FOO, BAR]) but the update removes the cluster default
    // that was covering instance0's missing BAR, so the change re-opens the gap. Diffing the keys
    // alone would miss this; the rule also diffs DEFAULT_INSTANCE_CAPACITY_MAP and so enforces.
    ClusterConfig current = clusterConfig("FOO", "BAR");
    current.setDefaultInstanceCapacityMap(ImmutableMap.of("BAR", 100));
    ClusterConfig proposed = clusterConfig("FOO", "BAR");
    HelixDataAccessor dataAccessor = mockAccessor(current,
        ImmutableList.of(instanceConfig("instance0", ImmutableMap.of("FOO", 100))));

    ValidationResult result =
        rule.validate(contextWithClusterConfigs(dataAccessor, current, proposed));

    Assert.assertFalse(result.isFeasible());
    Assert.assertTrue(result.getViolations().get(0).getMessage().contains("instance0"));
  }

  @Test
  public void testInstanceConfigReadErrorFailsClosed() {
    // The rule reads instance configs fail-closed (getChildValues(..., true)); a transient read
    // error must surface as a (forceable) rejection rather than silently validating against partial
    // state. Routed through the pipeline -- which converts a throwing rule into an infeasible verdict,
    // exactly as the REST preflight does before force=true can override it -- the read error yields a
    // violation attributed to this rule rather than a false feasible verdict.
    HelixDataAccessor dataAccessor = mock(HelixDataAccessor.class);
    when(dataAccessor.keyBuilder()).thenReturn(BUILDER);
    doReturn(clusterConfig("FOO", "BAR")).when(dataAccessor).getProperty(BUILDER.clusterConfig());
    doThrow(new HelixException("ZooKeeper read failed"))
        .when(dataAccessor).getChildValues(BUILDER.instanceConfigs(), true);

    ValidationResult result =
        new GuardrailPipeline(rule).validate(contextWith(dataAccessor, resourceConfig()));

    Assert.assertFalse(result.isFeasible());
    Assert.assertEquals(result.getViolations().get(0).getRuleId(),
        CapacityKeyConsistencyGuardrailRule.RULE_ID);
  }

  @Test
  public void testWagedEnableForAllResourcesMissingKeyIsInfeasible() {
    // Bulk WAGED-enable path: the committed cluster config requires FOO+BAR and instance0 omits BAR.
    // Enabling WAGED for every resource would subject them all to a model WAGED cannot build, so the
    // rule blocks it -- with a cluster-scoped verdict (no single resource) phrased for the bulk flip.
    ClusterConfig clusterConfig = clusterConfig("FOO", "BAR");
    HelixDataAccessor dataAccessor = mockAccessor(clusterConfig,
        ImmutableList.of(instanceConfig("instance0", ImmutableMap.of("FOO", 100))));

    ValidationResult result = rule.validate(contextWithWagedEnableForAllResources(dataAccessor));

    Assert.assertFalse(result.isFeasible());
    Violation violation = result.getViolations().get(0);
    Assert.assertEquals(violation.getRuleId(), CapacityKeyConsistencyGuardrailRule.RULE_ID);
    // No single resource is flipped on the bulk path, so the violation carries no resource name.
    Assert.assertNull(violation.getResourceName());
    Assert.assertTrue(violation.getMessage().contains("instance0"));
    Assert.assertTrue(violation.getMessage().contains("BAR"));
    Assert.assertTrue(violation.getMessage().contains("enabling WAGED for all resources"));
  }

  @Test
  public void testWagedEnableForAllResourcesAllKeysPresentIsFeasible() {
    // Every assignable instance declares both required keys, so flipping every resource to WAGED is
    // safe.
    ClusterConfig clusterConfig = clusterConfig("FOO", "BAR");
    HelixDataAccessor dataAccessor = mockAccessor(clusterConfig, ImmutableList.of(
        instanceConfig("instance0", ImmutableMap.of("FOO", 100, "BAR", 100)),
        instanceConfig("instance1", ImmutableMap.of("FOO", 100, "BAR", 100))));

    Assert.assertTrue(
        rule.validate(contextWithWagedEnableForAllResources(dataAccessor)).isFeasible());
  }

  @Test
  public void testWagedEnableForAllResourcesNoCapacityKeysIsFeasible() {
    // The cluster does not use the WAGED capacity model (no capacity keys), so a bulk WAGED-enable
    // requires nothing of the instances even though instance0 declares no capacity at all.
    HelixDataAccessor dataAccessor = mockAccessor(new ClusterConfig(CLUSTER),
        ImmutableList.of(instanceConfig("instance0", ImmutableMap.of())));

    Assert.assertTrue(
        rule.validate(contextWithWagedEnableForAllResources(dataAccessor)).isFeasible());
  }

  // ---------------------------------------------------------------------------------------------
  // Resource-side coverage on the bulk enableWagedRebalanceForAllResources path. The bulk flip
  // sweeps every resource with an IdealState into WAGED through an admin path that never validates
  // partition weights, so a single legacy resource missing a required weight breaks the one
  // cluster-wide WAGED model. The rule validates the resource side too and names each offender.
  // ---------------------------------------------------------------------------------------------

  @Test
  public void testWagedEnableForAllResourcesLegacyResourceMissingWeightIsInfeasible() {
    // Instances cover both keys (no instance-side gap), but a legacy resource has no ResourceConfig
    // and the cluster declares no DEFAULT_PARTITION_WEIGHT_MAP, so WAGED could not weight its
    // partitions -- the resource-side gap the bulk flip would otherwise accept silently.
    ClusterConfig clusterConfig = clusterConfig("FOO", "BAR");
    HelixDataAccessor dataAccessor = mockAccessorWithResources(clusterConfig,
        ImmutableList.of(instanceConfig("instance0", ImmutableMap.of("FOO", 100, "BAR", 100))),
        ImmutableList.of(idealState("legacyResource", 3)),
        ImmutableList.of());

    ValidationResult result = rule.validate(contextWithWagedEnableForAllResources(dataAccessor));

    Assert.assertFalse(result.isFeasible());
    Violation violation = result.getViolations().get(0);
    Assert.assertEquals(violation.getRuleId(), CapacityKeyConsistencyGuardrailRule.RULE_ID);
    // The resource-side verdict names the offending resource (unlike the cluster-scoped instance side).
    Assert.assertEquals(violation.getResourceName(), "legacyResource");
    Assert.assertTrue(violation.getMessage().contains("legacyResource"));
    Assert.assertTrue(violation.getMessage().contains("DEFAULT_PARTITION_WEIGHT_MAP"));
    Assert.assertTrue(violation.getMessage().contains("BAR"));
  }

  @Test
  public void testWagedEnableForAllResourcesLegacyResourceCoveredByClusterDefaultIsFeasible() {
    // Same legacy (config-less) resource, but now the cluster's DEFAULT_PARTITION_WEIGHT_MAP declares
    // every required key, so every partition is weighted by fallback and the flip is safe.
    ClusterConfig clusterConfig = clusterConfig("FOO", "BAR");
    clusterConfig.setDefaultPartitionWeightMap(ImmutableMap.of("FOO", 1, "BAR", 1));
    HelixDataAccessor dataAccessor = mockAccessorWithResources(clusterConfig,
        ImmutableList.of(instanceConfig("instance0", ImmutableMap.of("FOO", 100, "BAR", 100))),
        ImmutableList.of(idealState("legacyResource", 3)),
        ImmutableList.of());

    Assert.assertTrue(
        rule.validate(contextWithWagedEnableForAllResources(dataAccessor)).isFeasible());
  }

  @Test
  public void testWagedEnableForAllResourcesResourceWithCompleteWeightsIsFeasible()
      throws IOException {
    // A resource whose own weight map (via the DEFAULT_PARTITION_KEY entry) declares every required
    // key needs no cluster default and is safe to flip.
    ClusterConfig clusterConfig = clusterConfig("FOO", "BAR");
    HelixDataAccessor dataAccessor = mockAccessorWithResources(clusterConfig,
        ImmutableList.of(instanceConfig("instance0", ImmutableMap.of("FOO", 100, "BAR", 100))),
        ImmutableList.of(idealState("weightedResource", 2)),
        ImmutableList.of(resourceWithDefaultWeights("weightedResource",
            ImmutableMap.of("FOO", 1, "BAR", 1))));

    Assert.assertTrue(
        rule.validate(contextWithWagedEnableForAllResources(dataAccessor)).isFeasible());
  }

  @Test
  public void testWagedEnableForAllResourcesResourceWithIncompleteWeightsIsInfeasible()
      throws IOException {
    // The resource has a ResourceConfig, but its DEFAULT_PARTITION_KEY weight omits BAR, so WAGED's
    // per-partition validation (which this rule mirrors) would reject it at model-build time.
    ClusterConfig clusterConfig = clusterConfig("FOO", "BAR");
    HelixDataAccessor dataAccessor = mockAccessorWithResources(clusterConfig,
        ImmutableList.of(instanceConfig("instance0", ImmutableMap.of("FOO", 100, "BAR", 100))),
        ImmutableList.of(idealState("partialResource", 1)),
        ImmutableList.of(resourceWithDefaultWeights("partialResource", ImmutableMap.of("FOO", 1))));

    ValidationResult result = rule.validate(contextWithWagedEnableForAllResources(dataAccessor));

    Assert.assertFalse(result.isFeasible());
    Violation violation = result.getViolations().get(0);
    Assert.assertEquals(violation.getResourceName(), "partialResource");
    Assert.assertTrue(violation.getMessage().contains("partialResource"));
    Assert.assertTrue(violation.getMessage().contains("BAR"));
  }

  @Test
  public void testWagedEnableForAllResourcesResourceConfigWithoutWeightsIsInfeasible() {
    // A ResourceConfig exists but carries no partition weights at all; with no DEFAULT_PARTITION_KEY
    // entry and no cluster default, its partitions fall back to an empty weight map.
    ClusterConfig clusterConfig = clusterConfig("FOO", "BAR");
    HelixDataAccessor dataAccessor = mockAccessorWithResources(clusterConfig,
        ImmutableList.of(instanceConfig("instance0", ImmutableMap.of("FOO", 100, "BAR", 100))),
        ImmutableList.of(idealState("weightlessResource", 2)),
        ImmutableList.of(new ResourceConfig("weightlessResource")));

    ValidationResult result = rule.validate(contextWithWagedEnableForAllResources(dataAccessor));

    Assert.assertFalse(result.isFeasible());
    Violation violation = result.getViolations().get(0);
    Assert.assertEquals(violation.getResourceName(), "weightlessResource");
    Assert.assertTrue(violation.getMessage().contains("weightlessResource"));
    Assert.assertTrue(violation.getMessage().contains("DEFAULT_PARTITION_WEIGHT_MAP"));
  }

  @Test
  public void testWagedEnableForAllResourcesReportsBothInstanceAndResourceGaps() {
    // The bulk flip surfaces both failure classes at once: instance0 omits BAR (instance side) and a
    // legacy resource has no weights (resource side). Both are reported; only the resource-side
    // violation names a resource.
    ClusterConfig clusterConfig = clusterConfig("FOO", "BAR");
    HelixDataAccessor dataAccessor = mockAccessorWithResources(clusterConfig,
        ImmutableList.of(instanceConfig("instance0", ImmutableMap.of("FOO", 100))),
        ImmutableList.of(idealState("legacyResource", 1)),
        ImmutableList.of());

    ValidationResult result = rule.validate(contextWithWagedEnableForAllResources(dataAccessor));

    Assert.assertFalse(result.isFeasible());
    boolean hasInstanceGap = result.getViolations().stream()
        .anyMatch(v -> v.getResourceName() == null && v.getMessage().contains("instance0"));
    boolean hasResourceGap = result.getViolations().stream()
        .anyMatch(v -> "legacyResource".equals(v.getResourceName()));
    Assert.assertTrue(hasInstanceGap, "expected an instance-side violation for instance0");
    Assert.assertTrue(hasResourceGap, "expected a resource-side violation naming legacyResource");
  }

  @Test
  public void testWagedEnableForAllResourcesNoResourcesIsFeasible() {
    // No ideal states to flip (empty cluster): the resource-side check has nothing to reject.
    ClusterConfig clusterConfig = clusterConfig("FOO", "BAR");
    HelixDataAccessor dataAccessor = mockAccessorWithResources(clusterConfig,
        ImmutableList.of(instanceConfig("instance0", ImmutableMap.of("FOO", 100, "BAR", 100))),
        ImmutableList.of(),
        ImmutableList.of());

    Assert.assertTrue(
        rule.validate(contextWithWagedEnableForAllResources(dataAccessor)).isFeasible());
  }

  private GuardrailContext contextWith(HelixDataAccessor dataAccessor,
      ResourceConfig proposedResourceConfig) {
    return GuardrailContext.newBuilder(CLUSTER)
        .dataAccessor(dataAccessor)
        .proposedResourceConfig(proposedResourceConfig)
        .build();
  }

  private GuardrailContext contextWithClusterConfig(HelixDataAccessor dataAccessor,
      ClusterConfig proposedClusterConfig) {
    return GuardrailContext.newBuilder(CLUSTER)
        .dataAccessor(dataAccessor)
        .proposedClusterConfig(proposedClusterConfig)
        .build();
  }

  private GuardrailContext contextWithClusterConfigs(HelixDataAccessor dataAccessor,
      ClusterConfig currentClusterConfig, ClusterConfig proposedClusterConfig) {
    return GuardrailContext.newBuilder(CLUSTER)
        .dataAccessor(dataAccessor)
        .currentClusterConfig(currentClusterConfig)
        .proposedClusterConfig(proposedClusterConfig)
        .build();
  }

  private GuardrailContext contextWithWagedEnableForAllResources(HelixDataAccessor dataAccessor) {
    return GuardrailContext.newBuilder(CLUSTER)
        .dataAccessor(dataAccessor)
        .wagedEnableForAllResources(true)
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

  private HelixDataAccessor mockAccessorWithResources(ClusterConfig clusterConfig,
      List<InstanceConfig> instanceConfigs, List<IdealState> idealStates,
      List<ResourceConfig> resourceConfigs) {
    HelixDataAccessor dataAccessor = mockAccessor(clusterConfig, instanceConfigs);
    doReturn(idealStates).when(dataAccessor).getChildValues(BUILDER.idealStates(), true);
    doReturn(resourceConfigs).when(dataAccessor).getChildValues(BUILDER.resourceConfigs(), true);
    return dataAccessor;
  }

  private static IdealState idealState(String resourceName, int numPartitions) {
    IdealState idealState = new IdealState(resourceName);
    idealState.setNumPartitions(numPartitions);
    return idealState;
  }

  private static ResourceConfig resourceWithDefaultWeights(String resourceName,
      Map<String, Integer> defaultPartitionWeights) throws IOException {
    ResourceConfig resourceConfig = new ResourceConfig(resourceName);
    resourceConfig.setPartitionCapacityMap(
        ImmutableMap.of(ResourceConfig.DEFAULT_PARTITION_KEY, defaultPartitionWeights));
    return resourceConfig;
  }

  private static ClusterConfig clusterConfig(String... capacityKeys) {
    ClusterConfig clusterConfig = new ClusterConfig(CLUSTER);
    clusterConfig.setInstanceCapacityKeys(Arrays.asList(capacityKeys));
    return clusterConfig;
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

  private static ResourceConfig resourceConfig() {
    // A well-formed resource weight map (the resource side is validated elsewhere); this rule only
    // needs the resource config to be present and named so its violations can reference it.
    return new ResourceConfig(RESOURCE);
  }
}
