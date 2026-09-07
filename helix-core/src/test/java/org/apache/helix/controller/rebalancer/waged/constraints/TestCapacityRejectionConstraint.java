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
import org.testng.annotations.Test;

public class TestCapacityRejectionConstraint {
  private static final String INSTANCE = "testInstance";
  private static final String RESOURCE = "testResource";
  private static final String PARTITION = "testPartition";

  private final AssignableReplica _testReplica = Mockito.mock(AssignableReplica.class);
  private final AssignableNode _testNode = Mockito.mock(AssignableNode.class);
  private final ClusterContext _clusterContext = Mockito.mock(ClusterContext.class);
  private final HardConstraint _constraint = new CapacityRejectionConstraint();

  private void setUpPlacement() {
    when(_testNode.getInstanceName()).thenReturn(INSTANCE);
    when(_testReplica.getResourceName()).thenReturn(RESOURCE);
    when(_testReplica.getPartitionName()).thenReturn(PARTITION);
  }

  @Test
  public void testConstraintValidWhenPlacementNotRejected() {
    setUpPlacement();
    when(_clusterContext.isCapacityRejected(INSTANCE, RESOURCE, PARTITION)).thenReturn(false);
    Assert.assertTrue(_constraint.isAssignmentValid(_testNode, _testReplica, _clusterContext));
  }

  @Test
  public void testConstraintInvalidWhenPlacementAlreadyRejected() {
    setUpPlacement();
    when(_clusterContext.isCapacityRejected(INSTANCE, RESOURCE, PARTITION)).thenReturn(true);
    Assert.assertFalse(_constraint.isAssignmentValid(_testNode, _testReplica, _clusterContext));
  }

  @Test
  public void testConstraintIsScopedToTheExactPlacement() {
    setUpPlacement();
    // Only the rejected (instance, resource, partition) triple is excluded. The same partition on a
    // different instance must stay eligible, otherwise feeding rejections back would starve the
    // partition instead of relocating it.
    when(_clusterContext.isCapacityRejected(INSTANCE, RESOURCE, PARTITION)).thenReturn(true);
    when(_clusterContext.isCapacityRejected("otherInstance", RESOURCE, PARTITION)).thenReturn(false);

    AssignableNode otherNode = Mockito.mock(AssignableNode.class);
    when(otherNode.getInstanceName()).thenReturn("otherInstance");
    Assert.assertTrue(_constraint.isAssignmentValid(otherNode, _testReplica, _clusterContext));
  }

  @Test
  public void testConstraintType() {
    Assert.assertEquals(_constraint.getType(), HardConstraint.Type.CAPACITY_REJECTED);
  }
}
