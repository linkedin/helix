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

import org.apache.helix.controller.rebalancer.waged.model.AssignableNode;
import org.apache.helix.controller.rebalancer.waged.model.AssignableReplica;
import org.apache.helix.controller.rebalancer.waged.model.ClusterContext;
import org.testng.Assert;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class TestPhysicalCapacitySoftConstraint {
  private AssignableReplica _testReplica;
  private AssignableNode _testNode;
  private ClusterContext _clusterContext;
  private final SoftConstraint _constraint = new PhysicalCapacitySoftConstraint();

  @BeforeMethod
  public void setUp() {
    _testNode = mock(AssignableNode.class);
    _testReplica = mock(AssignableReplica.class);
    _clusterContext = mock(ClusterContext.class);
  }

  @Test
  public void testReplicaThatFitsScoresMax() {
    when(_testNode.getPhysicalRoomScore(any())).thenReturn(1d);
    Assert.assertEquals(
        _constraint.getAssignmentNormalizedScore(_testNode, _testReplica, _clusterContext), 1d);
  }

  @Test
  public void testOvercommittedInstanceScoresLower() {
    when(_testNode.getPhysicalRoomScore(any())).thenReturn(0d);
    Assert.assertEquals(
        _constraint.getAssignmentNormalizedScore(_testNode, _testReplica, _clusterContext), 0d);
  }

  /**
   * The reason this constraint does not extend UsageSoftConstraint. That base class normalizes
   * through SIGMOID(-(score - 1) * 44), which collapses everything outside roughly +/-5% of the
   * cluster average to indistinguishable values -- measured at 6.1e-39 against 7.8e-20 for a full
   * instance versus an empty one, a difference no weight can recover. The scale here has to stay
   * able to separate two instances that are both above average, because that is the only situation
   * in which this constraint has anything to say.
   */
  @Test
  public void testNormalizationDoesNotSaturate() {
    when(_testNode.getPhysicalRoomScore(any())).thenReturn(0.1d);
    double nearlyFull =
        _constraint.getAssignmentNormalizedScore(_testNode, _testReplica, _clusterContext);
    when(_testNode.getPhysicalRoomScore(any())).thenReturn(0.9d);
    double roomier =
        _constraint.getAssignmentNormalizedScore(_testNode, _testReplica, _clusterContext);

    Assert.assertTrue(roomier - nearlyFull > 0.5d,
        "The scale must keep overcommitted instances distinguishable so that the least "
            + "overcommitted one is preferred; got " + nearlyFull + " and " + roomier);
  }
}
