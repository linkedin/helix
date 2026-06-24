package org.apache.helix.util;

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
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import org.apache.helix.model.ClusterConfig;
import org.apache.helix.model.InstanceConfig;
import org.testng.Assert;
import org.testng.annotations.Test;

public class TestRebalanceFeasibilityEvaluator {
  private static final Set<String> UNHEALTHY_STATES =
      ImmutableSet.of("OFFLINE", "ERROR", "DROPPED");

  private final RebalanceFeasibilityEvaluator _evaluator = new RebalanceFeasibilityEvaluator();

  @Test
  public void testMinActiveReplicaViolationFlagged() {
    // Partition MyDB_0 has only 1 active replica (MASTER); the other two are OFFLINE/ERROR.
    Map<String, Map<String, String>> partitionStateMap = new HashMap<>();
    partitionStateMap.put("MyDB_0", ImmutableMap.of(
        "host-1", "MASTER",
        "host-2", "OFFLINE",
        "host-3", "ERROR"));

    FeasibilityResult result =
        _evaluator.checkMinActiveReplicas("MyDB", partitionStateMap, 2, UNHEALTHY_STATES);

    Assert.assertFalse(result.isFeasible());
    List<FeasibilityViolation> violations = result.getViolations();
    Assert.assertEquals(violations.size(), 1);
    FeasibilityViolation violation = violations.get(0);
    Assert.assertEquals(violation.getType(), FeasibilityViolation.Type.MIN_ACTIVE_REPLICA);
    Assert.assertEquals(violation.getResourceName(), "MyDB");
    Assert.assertEquals(violation.getPartitionName(), "MyDB_0");
  }

  @Test
  public void testMinActiveReplicaSatisfiedIsFeasible() {
    // Both partitions keep at least 2 active replicas.
    Map<String, Map<String, String>> partitionStateMap = new HashMap<>();
    partitionStateMap.put("MyDB_0", ImmutableMap.of(
        "host-1", "MASTER",
        "host-2", "SLAVE",
        "host-3", "OFFLINE"));
    partitionStateMap.put("MyDB_1", ImmutableMap.of(
        "host-1", "SLAVE",
        "host-2", "MASTER"));

    FeasibilityResult result =
        _evaluator.checkMinActiveReplicas("MyDB", partitionStateMap, 2, UNHEALTHY_STATES);

    Assert.assertTrue(result.isFeasible());
    Assert.assertTrue(result.getViolations().isEmpty());
  }

  @Test
  public void testMinActiveReplicaSkippedWhenUnset() {
    // minActiveReplicas == -1 means the resource has no constraint; never flag it.
    Map<String, Map<String, String>> partitionStateMap = new HashMap<>();
    partitionStateMap.put("MyDB_0", ImmutableMap.of("host-1", "OFFLINE"));

    FeasibilityResult result =
        _evaluator.checkMinActiveReplicas("MyDB", partitionStateMap, -1, UNHEALTHY_STATES);

    Assert.assertTrue(result.isFeasible());
  }

  @Test
  public void testMinActiveReplicaFlagsEveryViolatingPartition() {
    // Two partitions both below the threshold -> two violations.
    Map<String, Map<String, String>> partitionStateMap = new HashMap<>();
    partitionStateMap.put("MyDB_0", ImmutableMap.of("host-1", "MASTER", "host-2", "OFFLINE"));
    partitionStateMap.put("MyDB_1", ImmutableMap.of("host-1", "ERROR", "host-2", "OFFLINE"));

    FeasibilityResult result =
        _evaluator.checkMinActiveReplicas("MyDB", partitionStateMap, 2, UNHEALTHY_STATES);

    Assert.assertFalse(result.isFeasible());
    Assert.assertEquals(result.getViolations().size(), 2);
  }

  @Test
  public void testCapacityViolationWhenRequiredKeyMissing() {
    ClusterConfig clusterConfig = new ClusterConfig("cluster");
    clusterConfig.setInstanceCapacityKeys(Arrays.asList("CU", "MEM"));

    InstanceConfig instanceConfig = new InstanceConfig("host-1");
    instanceConfig.setInstanceCapacityMap(ImmutableMap.of("CU", 100)); // missing required MEM

    FeasibilityResult result =
        _evaluator.checkInstanceCapacities(clusterConfig, Collections.singletonList(instanceConfig));

    Assert.assertFalse(result.isFeasible());
    Assert.assertEquals(result.getViolations().size(), 1);
    FeasibilityViolation violation = result.getViolations().get(0);
    Assert.assertEquals(violation.getType(), FeasibilityViolation.Type.CAPACITY);
    Assert.assertEquals(violation.getInstanceName(), "host-1");
  }

  @Test
  public void testCapacityFeasibleWhenAllRequiredKeysPresent() {
    ClusterConfig clusterConfig = new ClusterConfig("cluster");
    clusterConfig.setInstanceCapacityKeys(Arrays.asList("CU", "MEM"));

    InstanceConfig instanceConfig = new InstanceConfig("host-1");
    instanceConfig.setInstanceCapacityMap(ImmutableMap.of("CU", 100, "MEM", 256));

    FeasibilityResult result =
        _evaluator.checkInstanceCapacities(clusterConfig, Collections.singletonList(instanceConfig));

    Assert.assertTrue(result.isFeasible());
  }

  @Test
  public void testCapacityFeasibleWhenNoCapacityKeysConfigured() {
    // A non-WAGED cluster declares no capacity keys -> capacity check is a no-op.
    ClusterConfig clusterConfig = new ClusterConfig("cluster");

    InstanceConfig instanceConfig = new InstanceConfig("host-1");

    FeasibilityResult result =
        _evaluator.checkInstanceCapacities(clusterConfig, Collections.singletonList(instanceConfig));

    Assert.assertTrue(result.isFeasible());
  }

  @Test
  public void testUnassignedPartitionFlaggedWhenMissing() {
    // Expected P0 and P1, but the computed assignment only placed P0.
    Map<String, Map<String, String>> partitionStateMap = new HashMap<>();
    partitionStateMap.put("MyDB_0", ImmutableMap.of("host-1", "MASTER"));

    FeasibilityResult result = _evaluator.checkNoUnassignedPartitions("MyDB", partitionStateMap,
        ImmutableSet.of("MyDB_0", "MyDB_1"));

    Assert.assertFalse(result.isFeasible());
    Assert.assertEquals(result.getViolations().size(), 1);
    FeasibilityViolation violation = result.getViolations().get(0);
    Assert.assertEquals(violation.getType(), FeasibilityViolation.Type.UNASSIGNED_PARTITION);
    Assert.assertEquals(violation.getPartitionName(), "MyDB_1");
  }

  @Test
  public void testUnassignedPartitionFlaggedWhenEmpty() {
    // P1 is present but has no instances assigned to it.
    Map<String, Map<String, String>> partitionStateMap = new HashMap<>();
    partitionStateMap.put("MyDB_0", ImmutableMap.of("host-1", "MASTER"));
    partitionStateMap.put("MyDB_1", Collections.emptyMap());

    FeasibilityResult result = _evaluator.checkNoUnassignedPartitions("MyDB", partitionStateMap,
        ImmutableSet.of("MyDB_0", "MyDB_1"));

    Assert.assertFalse(result.isFeasible());
    Assert.assertEquals(result.getViolations().size(), 1);
    Assert.assertEquals(result.getViolations().get(0).getPartitionName(), "MyDB_1");
  }

  @Test
  public void testAllPartitionsAssignedIsFeasible() {
    Map<String, Map<String, String>> partitionStateMap = new HashMap<>();
    partitionStateMap.put("MyDB_0", ImmutableMap.of("host-1", "MASTER"));
    partitionStateMap.put("MyDB_1", ImmutableMap.of("host-2", "MASTER"));

    FeasibilityResult result = _evaluator.checkNoUnassignedPartitions("MyDB", partitionStateMap,
        ImmutableSet.of("MyDB_0", "MyDB_1"));

    Assert.assertTrue(result.isFeasible());
  }

  @Test
  public void testMergeAggregatesViolationsAcrossChecks() {
    FeasibilityResult minActive = FeasibilityResult.of(Collections.singletonList(
        FeasibilityViolation.minActiveReplica("MyDB", "MyDB_0", 1, 2)));
    FeasibilityResult capacity = FeasibilityResult.of(Collections.singletonList(
        FeasibilityViolation.capacity("host-1", "missing MEM")));
    FeasibilityResult unassigned = FeasibilityResult.feasible();

    FeasibilityResult merged =
        FeasibilityResult.merge(Arrays.asList(minActive, capacity, unassigned));

    Assert.assertFalse(merged.isFeasible());
    Assert.assertEquals(merged.getViolations().size(), 2);
  }

  @Test
  public void testMergeAllFeasibleIsFeasible() {
    FeasibilityResult merged = FeasibilityResult.merge(
        Arrays.asList(FeasibilityResult.feasible(), FeasibilityResult.feasible()));

    Assert.assertTrue(merged.isFeasible());
  }
}
