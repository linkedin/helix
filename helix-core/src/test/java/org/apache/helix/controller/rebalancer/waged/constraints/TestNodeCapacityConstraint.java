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

import static org.mockito.Mockito.when;

import org.apache.helix.controller.rebalancer.waged.model.AssignableNode;
import org.apache.helix.controller.rebalancer.waged.model.AssignableReplica;
import org.apache.helix.controller.rebalancer.waged.model.ClusterContext;
import org.mockito.Mockito;
import org.testng.Assert;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import com.google.common.collect.ImmutableMap;

public class TestNodeCapacityConstraint {
  private AssignableReplica _testReplica;
  private AssignableNode _testNode;
  private ClusterContext _clusterContext;
  private final HardConstraint _constraint = new NodeCapacityConstraint();

  /**
   * TestNG reuses one instance of this class for every method, so mocks held in fields would carry
   * stubs across tests. A test that never stubs a given method would silently inherit whatever a
   * previously executed test left behind, and the outcome would depend on method ordering.
   */
  @BeforeMethod
  public void beforeMethod() {
    _testReplica = Mockito.mock(AssignableReplica.class);
    _testNode = Mockito.mock(AssignableNode.class);
    _clusterContext = Mockito.mock(ClusterContext.class);
  }

  @Test
  public void testConstraintValidWhenNodeHasEnoughSpace() {
    String key = "testKey";
    when(_testNode.getRemainingCapacity()).thenReturn(ImmutableMap.of(key,  10));
    when(_testReplica.getCapacity()).thenReturn(ImmutableMap.of(key, 5));
    Assert.assertTrue(_constraint.isAssignmentValid(_testNode, _testReplica, _clusterContext));
  }

  @Test
  public void testConstraintInValidWhenNodeHasInsufficientSpace() {
    String key = "testKey";
    when(_testNode.getRemainingCapacity()).thenReturn(ImmutableMap.of(key,  1));
    when(_testReplica.getCapacity()).thenReturn(ImmutableMap.of(key, 5));
    Assert.assertFalse(_constraint.isAssignmentValid(_testNode, _testReplica, _clusterContext));
  }

  /**
   * Room the node physically owes to occupancy the plan never accounted for has to come off the
   * top. Without this the node looks free, the planner keeps choosing it, and the capacity ledger
   * keeps vetoing the result -- the partition never lands anywhere.
   */
  @Test
  public void testHiddenOccupancyBlocksAnOtherwiseValidPlacement() {
    String key = "testKey";
    when(_testNode.getRemainingCapacity()).thenReturn(ImmutableMap.of(key, 10));
    when(_testNode.getHiddenOccupancy(key, _testReplica)).thenReturn(8);
    when(_testReplica.getCapacity()).thenReturn(ImmutableMap.of(key, 5));
    Assert.assertFalse(_constraint.isAssignmentValid(_testNode, _testReplica, _clusterContext));
  }

  /**
   * The gate narrows eligibility, it does not condemn the node. An instance carrying hidden
   * occupancy that still has room for the replica must stay eligible, otherwise one stuck replica
   * would take a whole instance out of service.
   */
  @Test
  public void testHiddenOccupancyLeavingEnoughRoomStillAllowsPlacement() {
    String key = "testKey";
    when(_testNode.getRemainingCapacity()).thenReturn(ImmutableMap.of(key, 10));
    when(_testNode.getHiddenOccupancy(key, _testReplica)).thenReturn(4);
    when(_testReplica.getCapacity()).thenReturn(ImmutableMap.of(key, 5));
    Assert.assertTrue(_constraint.isAssignmentValid(_testNode, _testReplica, _clusterContext));
  }

  /**
   * Capacity is multi-dimensional and a placement has to fit in every dimension, so a shortfall in
   * one is not allowed to be averaged away by headroom in another.
   */
  @Test
  public void testShortfallInAnySingleDimensionBlocksPlacement() {
    when(_testNode.getRemainingCapacity()).thenReturn(ImmutableMap.of("roomy", 100, "tight", 10));
    when(_testNode.getHiddenOccupancy("roomy", _testReplica)).thenReturn(0);
    when(_testNode.getHiddenOccupancy("tight", _testReplica)).thenReturn(8);
    when(_testReplica.getCapacity()).thenReturn(ImmutableMap.of("roomy", 5, "tight", 5));
    Assert.assertFalse(_constraint.isAssignmentValid(_testNode, _testReplica, _clusterContext));
  }

  /**
   * A dimension the node does not declare is not a dimension it can be short on. Abstaining keeps
   * the gate from inventing a shortfall out of a key mismatch.
   */
  @Test
  public void testCapacityKeysTheNodeDoesNotDeclareAreIgnored() {
    when(_testNode.getRemainingCapacity()).thenReturn(ImmutableMap.of("known", 10));
    when(_testNode.getHiddenOccupancy("known", _testReplica)).thenReturn(0);
    when(_testReplica.getCapacity()).thenReturn(ImmutableMap.of("known", 5, "unknown", 999));
    Assert.assertTrue(_constraint.isAssignmentValid(_testNode, _testReplica, _clusterContext));
  }
}
