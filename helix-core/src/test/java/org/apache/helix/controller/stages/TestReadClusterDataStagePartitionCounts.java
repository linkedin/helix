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

import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;

import org.apache.helix.controller.dataproviders.BaseControllerDataProvider;
import org.apache.helix.controller.stages.ReadClusterDataStage.InstancePartitionCounts;
import org.apache.helix.model.BuiltInStateModelDefinitions;
import org.apache.helix.model.CurrentState;
import org.apache.helix.model.LiveInstance;
import org.apache.helix.model.LiveInstance.LiveInstanceProperty;
import org.apache.helix.model.StateModelDefinition;
import org.mockito.Mockito;
import org.testng.Assert;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import static org.mockito.Mockito.when;


/**
 * Covers the CurrentState-derived per-instance partition counts that back the ERROR,
 * ActualPartitionGauge and ActualTopStatePartitionGauge metrics.
 */
public class TestReadClusterDataStagePartitionCounts {
  private static final String INSTANCE = "instance_0";
  private static final String SESSION = "session_0";
  private static final String MASTER_SLAVE = "MasterSlave";
  private static final String LEADER_STANDBY = "LeaderStandby";

  private ReadClusterDataStage _stage;
  private BaseControllerDataProvider _dataProvider;

  @BeforeMethod
  public void beforeMethod() {
    _stage = new ReadClusterDataStage();
    _dataProvider = Mockito.mock(BaseControllerDataProvider.class);

    LiveInstance liveInstance = new LiveInstance(INSTANCE);
    liveInstance.getRecord()
        .setSimpleField(LiveInstanceProperty.SESSION_ID.toString(), SESSION);
    when(_dataProvider.getLiveInstances())
        .thenReturn(Collections.singletonMap(INSTANCE, liveInstance));

    when(_dataProvider.getStateModelDef(MASTER_SLAVE))
        .thenReturn(stateModelDef(BuiltInStateModelDefinitions.MasterSlave));
    when(_dataProvider.getStateModelDef(LEADER_STANDBY))
        .thenReturn(stateModelDef(BuiltInStateModelDefinitions.LeaderStandby));
  }

  private static StateModelDefinition stateModelDef(BuiltInStateModelDefinitions definition) {
    return definition.getStateModelDefinition();
  }

  private void mockCurrentStates(CurrentState... currentStates) {
    Map<String, CurrentState> currentStateMap = new HashMap<>();
    for (CurrentState currentState : currentStates) {
      currentStateMap.put(currentState.getResourceName(), currentState);
    }
    when(_dataProvider.getCurrentState(INSTANCE, SESSION, false)).thenReturn(currentStateMap);
  }

  private static CurrentState currentState(String resourceName, String stateModelDefRef,
      Map<String, String> partitionStates) {
    CurrentState currentState = new CurrentState(resourceName);
    currentState.setStateModelDefRef(stateModelDefRef);
    partitionStates.forEach(currentState::setState);
    return currentState;
  }

  /**
   * OFFLINE is the MasterSlave initial state, so those partitions are not being served and must not
   * be reported as actually hosted. DROPPED is filtered defensively; participants subtract the
   * partition entry rather than persisting DROPPED, so it is not expected in a real CurrentState.
   */
  @Test
  public void testExcludesInitialStateAndDroppedPartitions() {
    Map<String, String> partitionStates = new HashMap<>();
    partitionStates.put("p0", "MASTER");
    partitionStates.put("p1", "SLAVE");
    partitionStates.put("p2", "SLAVE");
    partitionStates.put("p3", "OFFLINE");
    partitionStates.put("p4", "DROPPED");
    mockCurrentStates(currentState("db", MASTER_SLAVE, partitionStates));

    InstancePartitionCounts counts = _stage.computeInstancePartitionCounts(_dataProvider, INSTANCE);

    Assert.assertEquals(counts.actualPartitionCount, 3L,
        "OFFLINE must not count as actually hosted; DROPPED is filtered defensively");
    Assert.assertEquals(counts.actualTopStatePartitionCount, 1L);
    Assert.assertEquals(counts.errorCount, 0L);
  }

  /**
   * ERROR partitions are still hosted by the instance, so they count towards the actual partition
   * count as well as the dedicated error count.
   */
  @Test
  public void testErrorPartitionsCountAsHosted() {
    Map<String, String> partitionStates = new HashMap<>();
    partitionStates.put("p0", "MASTER");
    partitionStates.put("p1", "ERROR");
    partitionStates.put("p2", "ERROR");
    partitionStates.put("p3", "OFFLINE");
    mockCurrentStates(currentState("db", MASTER_SLAVE, partitionStates));

    InstancePartitionCounts counts = _stage.computeInstancePartitionCounts(_dataProvider, INSTANCE);

    Assert.assertEquals(counts.errorCount, 2L);
    Assert.assertEquals(counts.actualPartitionCount, 3L);
    Assert.assertEquals(counts.actualTopStatePartitionCount, 1L);
  }

  /**
   * Counts are summed across resources, and the top state is resolved per resource rather than
   * globally, so a LeaderStandby LEADER and a MasterSlave MASTER both count.
   */
  @Test
  public void testCountsAggregateAcrossResourcesWithDifferentStateModels() {
    Map<String, String> masterSlaveStates = new HashMap<>();
    masterSlaveStates.put("p0", "MASTER");
    masterSlaveStates.put("p1", "SLAVE");

    Map<String, String> leaderStandbyStates = new HashMap<>();
    leaderStandbyStates.put("p0", "LEADER");
    leaderStandbyStates.put("p1", "STANDBY");
    leaderStandbyStates.put("p2", "OFFLINE");

    mockCurrentStates(currentState("db", MASTER_SLAVE, masterSlaveStates),
        currentState("service", LEADER_STANDBY, leaderStandbyStates));

    InstancePartitionCounts counts = _stage.computeInstancePartitionCounts(_dataProvider, INSTANCE);

    Assert.assertEquals(counts.actualPartitionCount, 4L);
    Assert.assertEquals(counts.actualTopStatePartitionCount, 2L,
        "MASTER and LEADER are both top states for their own resource");
    Assert.assertEquals(counts.errorCount, 0L);
  }

  /**
   * Without a state model definition the states cannot be interpreted, so the resource contributes
   * no actual counts. ERROR is state model agnostic and is still counted.
   */
  @Test
  public void testUnresolvedStateModelSkipsActualCountsButKeepsErrorCount() {
    Map<String, String> partitionStates = new HashMap<>();
    partitionStates.put("p0", "MASTER");
    partitionStates.put("p1", "ERROR");
    CurrentState unknown = currentState("db", "UnknownStateModel", partitionStates);
    when(_dataProvider.getStateModelDef("UnknownStateModel")).thenReturn(null);
    mockCurrentStates(unknown);

    InstancePartitionCounts counts = _stage.computeInstancePartitionCounts(_dataProvider, INSTANCE);

    Assert.assertEquals(counts.errorCount, 1L);
    Assert.assertEquals(counts.actualPartitionCount, 0L);
    Assert.assertEquals(counts.actualTopStatePartitionCount, 0L);
  }

  /**
   * A resource that throws while being processed must not discard the counts already accumulated
   * from the resources that were read successfully.
   */
  @Test
  public void testFailingResourceDoesNotDiscardOtherResourceCounts() {
    Map<String, String> healthyStates = new HashMap<>();
    healthyStates.put("p0", "MASTER");
    healthyStates.put("p1", "SLAVE");

    CurrentState failing = Mockito.mock(CurrentState.class);
    when(failing.getResourceName()).thenReturn("broken");
    when(failing.getPartitionStateMap()).thenThrow(new RuntimeException("boom"));

    // Ordered so the failing resource is processed first, proving the failure is contained rather
    // than abandoning the resources that follow it.
    Map<String, CurrentState> currentStateMap = new LinkedHashMap<>();
    currentStateMap.put("broken", failing);
    currentStateMap.put("db", currentState("db", MASTER_SLAVE, healthyStates));
    when(_dataProvider.getCurrentState(INSTANCE, SESSION, false)).thenReturn(currentStateMap);

    InstancePartitionCounts counts = _stage.computeInstancePartitionCounts(_dataProvider, INSTANCE);

    Assert.assertEquals(counts.actualPartitionCount, 2L,
        "A single failing resource must not zero out counts from healthy resources");
    Assert.assertEquals(counts.actualTopStatePartitionCount, 1L);
  }

  @Test
  public void testNonLiveInstanceYieldsZeroCounts() {
    when(_dataProvider.getLiveInstances()).thenReturn(Collections.emptyMap());

    InstancePartitionCounts counts = _stage.computeInstancePartitionCounts(_dataProvider, INSTANCE);

    Assert.assertEquals(counts.errorCount, 0L);
    Assert.assertEquals(counts.actualPartitionCount, 0L);
    Assert.assertEquals(counts.actualTopStatePartitionCount, 0L);
  }

  /**
   * A failure reading live instances or current states must be contained, since the caller updates
   * every other instance metric in the same pass.
   */
  @Test
  public void testCurrentStateReadFailureIsContained() {
    when(_dataProvider.getCurrentState(INSTANCE, SESSION, false))
        .thenThrow(new RuntimeException("zk read failed"));

    InstancePartitionCounts counts = _stage.computeInstancePartitionCounts(_dataProvider, INSTANCE);

    Assert.assertEquals(counts.actualPartitionCount, 0L);
    Assert.assertEquals(counts.actualTopStatePartitionCount, 0L);
    Assert.assertEquals(counts.errorCount, 0L);
  }

  @Test
  public void testNullCurrentStateMapYieldsZeroCounts() {
    when(_dataProvider.getCurrentState(INSTANCE, SESSION, false)).thenReturn(null);

    InstancePartitionCounts counts = _stage.computeInstancePartitionCounts(_dataProvider, INSTANCE);

    Assert.assertEquals(counts.actualPartitionCount, 0L);
    Assert.assertEquals(counts.actualTopStatePartitionCount, 0L);
  }

  /**
   * Partitions carrying no current state value are skipped rather than counted or throwing.
   */
  @Test
  public void testNullPartitionStateIsSkipped() {
    CurrentState withNullState = Mockito.mock(CurrentState.class);
    when(withNullState.getResourceName()).thenReturn("db");
    when(withNullState.getStateModelDefRef()).thenReturn(MASTER_SLAVE);
    Map<String, String> partitionStates = new HashMap<>();
    partitionStates.put("p0", "MASTER");
    partitionStates.put("p1", null);
    when(withNullState.getPartitionStateMap()).thenReturn(partitionStates);
    when(_dataProvider.getCurrentState(INSTANCE, SESSION, false))
        .thenReturn(Collections.singletonMap("db", withNullState));

    InstancePartitionCounts counts = _stage.computeInstancePartitionCounts(_dataProvider, INSTANCE);

    Assert.assertEquals(counts.actualPartitionCount, 1L);
    Assert.assertEquals(counts.actualTopStatePartitionCount, 1L);
    Assert.assertEquals(counts.errorCount, 0L);
  }

  /**
   * An instance hosting nothing must report zero rather than retaining a stale value.
   */
  @Test
  public void testEmptyCurrentStateYieldsZeroCounts() {
    mockCurrentStates(currentState("db", MASTER_SLAVE, Collections.emptyMap()));

    InstancePartitionCounts counts = _stage.computeInstancePartitionCounts(_dataProvider, INSTANCE);

    Assert.assertEquals(counts.actualPartitionCount, 0L);
    Assert.assertEquals(counts.actualTopStatePartitionCount, 0L);
  }
}
