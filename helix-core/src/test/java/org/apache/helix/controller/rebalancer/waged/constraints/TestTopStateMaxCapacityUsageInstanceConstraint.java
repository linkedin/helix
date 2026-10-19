package org.apache.helix.controller.rebalancer.waged.constraints;

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

import java.util.Collections;
import java.util.List;

import org.apache.helix.controller.rebalancer.waged.model.AssignableNode;
import org.apache.helix.controller.rebalancer.waged.model.AssignableReplica;
import org.apache.helix.controller.rebalancer.waged.model.ClusterContext;
import org.testng.Assert;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Matchers.anyMap;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;


public class TestTopStateMaxCapacityUsageInstanceConstraint {
  private AssignableReplica _testReplica;
  private AssignableNode _testNode;
  private ClusterContext _clusterContext;
  private final SoftConstraint _constraint = new TopStateMaxCapacityUsageInstanceConstraint();

  @BeforeMethod
  public void setUp() {
    _testNode = mock(AssignableNode.class);
    _testReplica = mock(AssignableReplica.class);
    _clusterContext = mock(ClusterContext.class);
  }

  @Test
  public void testGetNormalizedScore() {
    when(_testReplica.isReplicaTopState()).thenReturn(true);
    when(_testNode.getTopStateProjectedHighestUtilization(anyMap(), any())).thenReturn(0.8f);
    when(_clusterContext.getEstimatedTopStateMaxUtilization()).thenReturn(1f);
    double score = _constraint.getAssignmentScore(_testNode, _testReplica, _clusterContext);
    // Convert to float so as to compare with equal.
    Assert.assertEquals((float) score, 0.8f);
    double normalizedScore =
        _constraint.getAssignmentNormalizedScore(_testNode, _testReplica, _clusterContext);
    Assert.assertTrue(normalizedScore > 0.99);
  }


  @Test
  public void testGetNormalizedScoreWithPreferredScoringKey() {
    List<String> preferredScoringKeys = Collections.singletonList("CU");
    when(_testReplica.isReplicaTopState()).thenReturn(true);
    when(_testNode.getTopStateProjectedHighestUtilization(anyMap(),
        eq(preferredScoringKeys))).thenReturn(0.5f);
    when(_clusterContext.getPreferredScoringKeys()).thenReturn(preferredScoringKeys);
    when(_clusterContext.getEstimatedTopStateMaxUtilization()).thenReturn(1f);
    double score = _constraint.getAssignmentScore(_testNode, _testReplica, _clusterContext);
    // Convert to float so as to compare with equal.
    Assert.assertEquals((float) score,0.5f);
    double normalizedScore =
            _constraint.getAssignmentNormalizedScore(_testNode, _testReplica, _clusterContext);
    Assert.assertTrue(normalizedScore > 0.99);
  }

  @Test
  public void testTopStateScoringKeysReplaceThePreferredScoringKeys() {
    List<String> preferredScoringKeys = Collections.singletonList("DISK");
    List<String> topStateScoringKeys = Collections.singletonList("CU");
    when(_testReplica.isReplicaTopState()).thenReturn(true);
    when(_testNode.getTopStateProjectedHighestUtilization(anyMap(), eq(preferredScoringKeys)))
        .thenReturn(0.9f);
    when(_testNode.getTopStateProjectedHighestUtilization(anyMap(), eq(topStateScoringKeys)))
        .thenReturn(0.5f);
    when(_clusterContext.getPreferredScoringKeys()).thenReturn(preferredScoringKeys);
    when(_clusterContext.getEstimatedTopStateMaxUtilization()).thenReturn(1f);
    Assert.assertEquals(
        (float) _constraint.getAssignmentScore(_testNode, _testReplica, _clusterContext), 0.9f);

    when(_clusterContext.getTopStateScoringKeys()).thenReturn(topStateScoringKeys);
    Assert.assertEquals(
        (float) _constraint.getAssignmentScore(_testNode, _testReplica, _clusterContext), 0.5f);
  }

  @Test
  public void testTopStateScoringAlpha() {
    when(_testReplica.isReplicaTopState()).thenReturn(true);
    when(_testNode.getTopStateProjectedHighestUtilization(anyMap(), any())).thenReturn(1.1f);
    when(_clusterContext.getEstimatedTopStateMaxUtilization()).thenReturn(1f);
    // With the default steepness, a node at 1.1x of the estimate scores almost nothing...
    Assert.assertEquals(
        _constraint.getAssignmentNormalizedScore(_testNode, _testReplica, _clusterContext),
        sigmoid(-0.1 * 44), 1e-6);
    // ...while a softer curve still ranks it.
    when(_clusterContext.getTopStateScoringAlpha()).thenReturn(10d);
    Assert.assertEquals(
        _constraint.getAssignmentNormalizedScore(_testNode, _testReplica, _clusterContext),
        sigmoid(-0.1 * 10), 1e-6);
    // A non-positive steepness means not set.
    when(_clusterContext.getTopStateScoringAlpha()).thenReturn(-1d);
    Assert.assertEquals(
        _constraint.getAssignmentNormalizedScore(_testNode, _testReplica, _clusterContext),
        sigmoid(-0.1 * 44), 1e-6);
  }

  private static double sigmoid(double x) {
    return 1 / (1 + Math.exp(-x));
  }
}
