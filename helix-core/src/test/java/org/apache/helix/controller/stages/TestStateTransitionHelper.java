package org.apache.helix.controller.stages;

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

import org.apache.helix.model.BuiltInStateModelDefinitions;
import org.apache.helix.model.Partition;
import org.apache.helix.model.StateModelDefinition;
import org.testng.Assert;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;


public class TestStateTransitionHelper {

  private StateModelDefinition _masterSlaveSMD;

  @BeforeMethod
  public void setup() {
    _masterSlaveSMD = BuiltInStateModelDefinitions.MasterSlave.getStateModelDefinition();
  }

  // ========================================
  // Tests for isUpwardTransition
  // ========================================

  @Test
  public void testIsUpwardTransition_ValidUpwardTransitions() {
    // In MasterSlave: MASTER(0) > SLAVE(1) > OFFLINE(2)  (lower number = higher priority)
    Assert.assertTrue(StateTransitionHelper.isUpwardTransition("OFFLINE", "SLAVE", _masterSlaveSMD));
    Assert.assertTrue(StateTransitionHelper.isUpwardTransition("SLAVE", "MASTER", _masterSlaveSMD));
    Assert.assertTrue(StateTransitionHelper.isUpwardTransition("OFFLINE", "MASTER", _masterSlaveSMD));
  }

  @Test
  public void testIsUpwardTransition_DownwardTransitionsReturnFalse() {
    Assert.assertFalse(StateTransitionHelper.isUpwardTransition("MASTER", "SLAVE", _masterSlaveSMD));
    Assert.assertFalse(StateTransitionHelper.isUpwardTransition("SLAVE", "OFFLINE", _masterSlaveSMD));
  }

  @Test
  public void testIsUpwardTransition_InvalidInputs() {
    Assert.assertFalse(StateTransitionHelper.isUpwardTransition("MASTER", "MASTER", _masterSlaveSMD));
    Assert.assertFalse(StateTransitionHelper.isUpwardTransition("OFFLINE", "SLAVE", null));
    Assert.assertFalse(StateTransitionHelper.isUpwardTransition("UNKNOWN", "SLAVE", _masterSlaveSMD));
    Assert.assertFalse(StateTransitionHelper.isUpwardTransition("SLAVE", "UNKNOWN", _masterSlaveSMD));
  }

  // ========================================
  // Tests for isDownwardTransition
  // ========================================

  @Test
  public void testIsDownwardTransition_ValidDownwardTransitions() {
    Assert.assertTrue(StateTransitionHelper.isDownwardTransition("MASTER", "SLAVE", _masterSlaveSMD));
    Assert.assertTrue(StateTransitionHelper.isDownwardTransition("SLAVE", "OFFLINE", _masterSlaveSMD));
    Assert.assertTrue(StateTransitionHelper.isDownwardTransition("MASTER", "OFFLINE", _masterSlaveSMD));
  }

  @Test
  public void testIsDownwardTransition_InvalidInputs() {
    Assert.assertFalse(StateTransitionHelper.isDownwardTransition("OFFLINE", "SLAVE", _masterSlaveSMD));
    Assert.assertFalse(StateTransitionHelper.isDownwardTransition("SLAVE", "SLAVE", _masterSlaveSMD));
    Assert.assertFalse(StateTransitionHelper.isDownwardTransition("MASTER", "SLAVE", null));
  }

  // ========================================
  // Tests for isTopStateHandoff
  // ========================================

  @Test
  public void testIsTopStateHandoff_ValidHandoffs() {
    Assert.assertTrue(
        StateTransitionHelper.isTopStateHandoff("MASTER", "SLAVE", "MASTER", _masterSlaveSMD));
    Assert.assertTrue(
        StateTransitionHelper.isTopStateHandoff("MASTER", "OFFLINE", "MASTER", _masterSlaveSMD));
  }

  @Test
  public void testIsTopStateHandoff_NotFromTopState() {
    Assert.assertFalse(
        StateTransitionHelper.isTopStateHandoff("SLAVE", "OFFLINE", "MASTER", _masterSlaveSMD));
    Assert.assertFalse(
        StateTransitionHelper.isTopStateHandoff("SLAVE", "MASTER", "MASTER", _masterSlaveSMD));
    Assert.assertFalse(
        StateTransitionHelper.isTopStateHandoff("OFFLINE", "MASTER", "MASTER", _masterSlaveSMD));
  }

  @Test
  public void testIsTopStateHandoff_LeaderStandbyModel() {
    StateModelDefinition leaderStandbySMD =
        BuiltInStateModelDefinitions.LeaderStandby.getStateModelDefinition();
    Assert.assertTrue(
        StateTransitionHelper.isTopStateHandoff("LEADER", "STANDBY", "LEADER", leaderStandbySMD));
    Assert.assertFalse(
        StateTransitionHelper.isTopStateHandoff("STANDBY", "OFFLINE", "LEADER", leaderStandbySMD));
  }

  // ========================================
  // Tests for isPartitionMissingTopState
  // ========================================

  @Test
  public void testIsPartitionMissingTopState_MissingCases() {
    CurrentStateOutput cso = new CurrentStateOutput();

    // No current state at all
    Assert.assertTrue(StateTransitionHelper.isPartitionMissingTopState(
        "testResource", "testResource_0", "MASTER", cso));

    // Only SLAVE replicas
    cso.setCurrentState("testResource", new Partition("testResource_0"), "localhost_0", "SLAVE");
    cso.setCurrentState("testResource", new Partition("testResource_0"), "localhost_1", "SLAVE");
    Assert.assertTrue(StateTransitionHelper.isPartitionMissingTopState(
        "testResource", "testResource_0", "MASTER", cso));
  }

  @Test
  public void testIsPartitionMissingTopState_AllOffline() {
    CurrentStateOutput cso = new CurrentStateOutput();
    cso.setCurrentState("testResource", new Partition("testResource_0"), "localhost_0", "OFFLINE");

    Assert.assertTrue(StateTransitionHelper.isPartitionMissingTopState(
        "testResource", "testResource_0", "MASTER", cso));
  }

  @Test
  public void testIsPartitionMissingTopState_HasTopState() {
    CurrentStateOutput cso = new CurrentStateOutput();
    cso.setCurrentState("testResource", new Partition("testResource_0"), "localhost_0", "MASTER");
    cso.setCurrentState("testResource", new Partition("testResource_0"), "localhost_1", "SLAVE");

    Assert.assertFalse(StateTransitionHelper.isPartitionMissingTopState(
        "testResource", "testResource_0", "MASTER", cso));
  }

  @Test
  public void testIsPartitionMissingTopState_DifferentPartition() {
    CurrentStateOutput cso = new CurrentStateOutput();
    // MASTER exists on partition_1, but we query partition_0
    cso.setCurrentState("testResource", new Partition("testResource_1"), "localhost_0", "MASTER");

    Assert.assertTrue(StateTransitionHelper.isPartitionMissingTopState(
        "testResource", "testResource_0", "MASTER", cso));
  }

  // ========================================
  // Tests with OnlineOffline state model
  // ========================================

  @Test
  public void testTransitions_OnlineOfflineModel() {
    StateModelDefinition onlineOfflineSMD =
        BuiltInStateModelDefinitions.OnlineOffline.getStateModelDefinition();

    Assert.assertTrue(StateTransitionHelper.isUpwardTransition("OFFLINE", "ONLINE", onlineOfflineSMD));
    Assert.assertFalse(StateTransitionHelper.isUpwardTransition("ONLINE", "OFFLINE", onlineOfflineSMD));
    Assert.assertTrue(StateTransitionHelper.isDownwardTransition("ONLINE", "OFFLINE", onlineOfflineSMD));
  }
}
