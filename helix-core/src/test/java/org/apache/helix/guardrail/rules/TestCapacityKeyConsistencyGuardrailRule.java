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
import org.apache.helix.model.InstanceConfig;
import org.apache.helix.model.ResourceConfig;
import org.testng.Assert;
import org.testng.annotations.Test;

import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Unit tests for {@link CapacityKeyConsistencyGuardrailRule}, which certifies (for a WAGED resource
 * add) that every assignable instance declares every capacity key the cluster requires. The
 * resource-side coverage is deliberately not this rule's concern -- it is already validated by
 * {@code ZKHelixAdmin#addResourceWithWeight} before the write -- so these tests exercise only the
 * instance side. Cluster state (cluster config + instance configs) is supplied through a mocked
 * {@link HelixDataAccessor}; the proposed resource config is passed directly through the
 * {@link GuardrailContext}.
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
  public void testGuardrailDisabledIsFeasible() {
    // Flag off (the default) short-circuits before any coverage check even though BAR is missing on
    // instance0.
    ClusterConfig clusterConfig = clusterConfig("FOO", "BAR");
    HelixDataAccessor dataAccessor = mock(HelixDataAccessor.class);
    when(dataAccessor.keyBuilder()).thenReturn(BUILDER);
    doReturn(clusterConfig).when(dataAccessor).getProperty(BUILDER.clusterConfig());

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

  private GuardrailContext contextWith(HelixDataAccessor dataAccessor,
      ResourceConfig proposedResourceConfig) {
    return GuardrailContext.newBuilder(CLUSTER)
        .dataAccessor(dataAccessor)
        .proposedResourceConfig(proposedResourceConfig)
        .build();
  }

  private HelixDataAccessor mockAccessor(ClusterConfig clusterConfig,
      List<InstanceConfig> instanceConfigs) {
    // The guard rail is opt-in (disabled by default); these unit tests exercise its enforcement,
    // which only runs when enabled, so enable it here. The disabled-cluster behavior is covered
    // explicitly by testGuardrailDisabledIsFeasible, which builds its accessor without this helper.
    clusterConfig.setCapacityKeyConsistencyGuardrailEnabled(true);
    HelixDataAccessor dataAccessor = mock(HelixDataAccessor.class);
    when(dataAccessor.keyBuilder()).thenReturn(BUILDER);
    doReturn(clusterConfig).when(dataAccessor).getProperty(BUILDER.clusterConfig());
    doReturn(instanceConfigs).when(dataAccessor).getChildValues(BUILDER.instanceConfigs(), true);
    return dataAccessor;
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
