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

import org.apache.helix.HelixDefinedState;
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
  // Tests for isActiveState
  // ========================================

  @Test
  public void testIsActiveState_MasterIsActive() {
    Assert.assertTrue(StateTransitionHelper.isActiveState("MASTER"));
  }

  @Test
  public void testIsActiveState_SlaveIsActive() {
    Assert.assertTrue(StateTransitionHelper.isActiveState("SLAVE"));
  }

  @Test
  public void testIsActiveState_LeaderIsActive() {
    Assert.assertTrue(StateTransitionHelper.isActiveState("LEADER"));
  }

  @Test
  public void testIsActiveState_StandbyIsActive() {
    Assert.assertTrue(StateTransitionHelper.isActiveState("STANDBY"));
  }

  @Test
  public void testIsActiveState_OnlineIsActive() {
    Assert.assertTrue(StateTransitionHelper.isActiveState("ONLINE"));
  }

  @Test
  public void testIsActiveState_OfflineIsNotActive() {
    Assert.assertFalse(StateTransitionHelper.isActiveState("OFFLINE"));
  }

  @Test
  public void testIsActiveState_ErrorIsNotActive() {
    Assert.assertFalse(StateTransitionHelper.isActiveState(HelixDefinedState.ERROR.name()));
  }

  @Test
  public void testIsActiveState_DroppedIsNotActive() {
    Assert.assertFalse(StateTransitionHelper.isActiveState(HelixDefinedState.DROPPED.name()));
  }

  @Test
  public void testIsActiveState_NullIsNotActive() {
    Assert.assertFalse(StateTransitionHelper.isActiveState(null));
  }

  @Test
  public void testIsActiveState_EmptyStringIsNotActive() {
    Assert.assertFalse(StateTransitionHelper.isActiveState(""));
  }

  @Test
  public void testIsActiveState_CaseInsensitive() {
    // ERROR, DROPPED, OFFLINE should be matched case-insensitively
    Assert.assertFalse(StateTransitionHelper.isActiveState("error"));
    Assert.assertFalse(StateTransitionHelper.isActiveState("dropped"));
    Assert.assertFalse(StateTransitionHelper.isActiveState("offline"));
    Assert.assertFalse(StateTransitionHelper.isActiveState("Offline"));
  }

  // ========================================
  // Tests for isUpwardTransition
  // ========================================

  @Test
  public void testIsUpwardTransition_OfflineToSlave() {
    // OFFLINE(priority=2) -> SLAVE(priority=1): upward (higher priority = lower number)
    Assert.assertTrue(StateTransitionHelper.isUpwardTransition("OFFLINE", "SLAVE", _masterSlaveSMD));
  }

  @Test
  public void testIsUpwardTransition_SlaveToMaster() {
    // SLAVE(priority=1) -> MASTER(priority=0): upward
    Assert.assertTrue(StateTransitionHelper.isUpwardTransition("SLAVE", "MASTER", _masterSlaveSMD));
  }

  @Test
  public void testIsUpwardTransition_OfflineToMaster() {
    // OFFLINE(priority=2) -> MASTER(priority=0): upward
    Assert.assertTrue(StateTransitionHelper.isUpwardTransition("OFFLINE", "MASTER", _masterSlaveSMD));
  }

  @Test
  public void testIsUpwardTransition_MasterToSlave_IsNotUpward() {
    // MASTER(priority=0) -> SLAVE(priority=1): downward, not upward
    Assert.assertFalse(StateTransitionHelper.isUpwardTransition("MASTER", "SLAVE", _masterSlaveSMD));
  }

  @Test
  public void testIsUpwardTransition_SlaveToOffline_IsNotUpward() {
    // SLAVE(priority=1) -> OFFLINE(priority=2): downward, not upward
    Assert.assertFalse(StateTransitionHelper.isUpwardTransition("SLAVE", "OFFLINE", _masterSlaveSMD));
  }

  @Test
  public void testIsUpwardTransition_SameState_IsNotUpward() {
    Assert.assertFalse(StateTransitionHelper.isUpwardTransition("MASTER", "MASTER", _masterSlaveSMD));
  }

  @Test
  public void testIsUpwardTransition_NullStateModelDef() {
    Assert.assertFalse(StateTransitionHelper.isUpwardTransition("OFFLINE", "SLAVE", null));
  }

  @Test
  public void testIsUpwardTransition_UnknownFromState() {
    Assert.assertFalse(StateTransitionHelper.isUpwardTransition("UNKNOWN", "SLAVE", _masterSlaveSMD));
  }

  @Test
  public void testIsUpwardTransition_UnknownToState() {
    Assert.assertFalse(StateTransitionHelper.isUpwardTransition("SLAVE", "UNKNOWN", _masterSlaveSMD));
  }

  // ========================================
  // Tests for isDownwardTransition
  // ========================================

  @Test
  public void testIsDownwardTransition_MasterToSlave() {
    // MASTER(priority=0) -> SLAVE(priority=1): downward
    Assert.assertTrue(StateTransitionHelper.isDownwardTransition("MASTER", "SLAVE", _masterSlaveSMD));
  }

  @Test
  public void testIsDownwardTransition_SlaveToOffline() {
    // SLAVE(priority=1) -> OFFLINE(priority=2): downward
    Assert.assertTrue(StateTransitionHelper.isDownwardTransition("SLAVE", "OFFLINE", _masterSlaveSMD));
  }

  @Test
  public void testIsDownwardTransition_MasterToOffline() {
    // MASTER(priority=0) -> OFFLINE(priority=2): downward
    Assert.assertTrue(StateTransitionHelper.isDownwardTransition("MASTER", "OFFLINE", _masterSlaveSMD));
  }

  @Test
  public void testIsDownwardTransition_OfflineToSlave_IsNotDownward() {
    Assert.assertFalse(StateTransitionHelper.isDownwardTransition("OFFLINE", "SLAVE", _masterSlaveSMD));
  }

  @Test
  public void testIsDownwardTransition_SameState_IsNotDownward() {
    Assert.assertFalse(StateTransitionHelper.isDownwardTransition("SLAVE", "SLAVE", _masterSlaveSMD));
  }

  @Test
  public void testIsDownwardTransition_NullStateModelDef() {
    Assert.assertFalse(StateTransitionHelper.isDownwardTransition("MASTER", "SLAVE", null));
  }

  // ========================================
  // Tests for isTopStateHandoff
  // ========================================

  @Test
  public void testIsTopStateHandoff_MasterToSlave() {
    // Transition from top state (MASTER) to a lower state
    Assert.assertTrue(
        StateTransitionHelper.isTopStateHandoff("MASTER", "SLAVE", "MASTER", _masterSlaveSMD));
  }

  @Test
  public void testIsTopStateHandoff_MasterToOffline() {
    Assert.assertTrue(
        StateTransitionHelper.isTopStateHandoff("MASTER", "OFFLINE", "MASTER", _masterSlaveSMD));
  }

  @Test
  public void testIsTopStateHandoff_SlaveToOffline_NotTopStateHandoff() {
    // From state is not the top state
    Assert.assertFalse(
        StateTransitionHelper.isTopStateHandoff("SLAVE", "OFFLINE", "MASTER", _masterSlaveSMD));
  }

  @Test
  public void testIsTopStateHandoff_SlaveToMaster_NotTopStateHandoff() {
    // From state is not the top state, and this is an upward transition
    Assert.assertFalse(
        StateTransitionHelper.isTopStateHandoff("SLAVE", "MASTER", "MASTER", _masterSlaveSMD));
  }

  @Test
  public void testIsTopStateHandoff_OfflineToMaster_NotTopStateHandoff() {
    // From state is not the top state
    Assert.assertFalse(
        StateTransitionHelper.isTopStateHandoff("OFFLINE", "MASTER", "MASTER", _masterSlaveSMD));
  }

  @Test
  public void testIsTopStateHandoff_WithLeaderStandbyModel() {
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
  public void testIsPartitionMissingTopState_NoCurrentState() {
    CurrentStateOutput currentStateOutput = new CurrentStateOutput();
    // No state set at all for this partition
    Assert.assertTrue(StateTransitionHelper.isPartitionMissingTopState(
        "testResource", "testResource_0", "MASTER", currentStateOutput));
  }

  @Test
  public void testIsPartitionMissingTopState_HasTopState() {
    CurrentStateOutput currentStateOutput = new CurrentStateOutput();
    currentStateOutput.setCurrentState("testResource", new Partition("testResource_0"),
        "localhost_0", "MASTER");
    currentStateOutput.setCurrentState("testResource", new Partition("testResource_0"),
        "localhost_1", "SLAVE");

    Assert.assertFalse(StateTransitionHelper.isPartitionMissingTopState(
        "testResource", "testResource_0", "MASTER", currentStateOutput));
  }

  @Test
  public void testIsPartitionMissingTopState_OnlySlaveReplicas() {
    CurrentStateOutput currentStateOutput = new CurrentStateOutput();
    currentStateOutput.setCurrentState("testResource", new Partition("testResource_0"),
        "localhost_0", "SLAVE");
    currentStateOutput.setCurrentState("testResource", new Partition("testResource_0"),
        "localhost_1", "SLAVE");

    Assert.assertTrue(StateTransitionHelper.isPartitionMissingTopState(
        "testResource", "testResource_0", "MASTER", currentStateOutput));
  }

  @Test
  public void testIsPartitionMissingTopState_AllOffline() {
    CurrentStateOutput currentStateOutput = new CurrentStateOutput();
    currentStateOutput.setCurrentState("testResource", new Partition("testResource_0"),
        "localhost_0", "OFFLINE");

    Assert.assertTrue(StateTransitionHelper.isPartitionMissingTopState(
        "testResource", "testResource_0", "MASTER", currentStateOutput));
  }

  @Test
  public void testIsPartitionMissingTopState_DifferentPartitions() {
    CurrentStateOutput currentStateOutput = new CurrentStateOutput();
    // Set MASTER for partition_1, but query partition_0
    currentStateOutput.setCurrentState("testResource", new Partition("testResource_1"),
        "localhost_0", "MASTER");

    Assert.assertTrue(StateTransitionHelper.isPartitionMissingTopState(
        "testResource", "testResource_0", "MASTER", currentStateOutput));
  }

  // ========================================
  // Tests with OnlineOffline state model
  // ========================================

  @Test
  public void testUpwardTransition_OnlineOfflineModel() {
    StateModelDefinition onlineOfflineSMD =
        BuiltInStateModelDefinitions.OnlineOffline.getStateModelDefinition();

    // OFFLINE -> ONLINE is upward
    Assert.assertTrue(StateTransitionHelper.isUpwardTransition("OFFLINE", "ONLINE", onlineOfflineSMD));
    // ONLINE -> OFFLINE is downward
    Assert.assertFalse(StateTransitionHelper.isUpwardTransition("ONLINE", "OFFLINE", onlineOfflineSMD));
    Assert.assertTrue(StateTransitionHelper.isDownwardTransition("ONLINE", "OFFLINE", onlineOfflineSMD));
  }
}

