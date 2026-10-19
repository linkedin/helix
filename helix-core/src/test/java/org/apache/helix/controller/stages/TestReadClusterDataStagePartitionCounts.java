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

import org.apache.helix.constants.InstanceConstants;
import org.apache.helix.controller.dataproviders.BaseControllerDataProvider;
import org.apache.helix.controller.stages.ReadClusterDataStage.InstancePartitionCounts;
import org.apache.helix.model.BuiltInStateModelDefinitions;
import org.apache.helix.model.CurrentState;
import org.apache.helix.model.IdealState;
import org.apache.helix.model.InstanceConfig;
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

  /** Mark the instance itself as disabled, as {@code BaseControllerDataProvider} would report it. */
  private void mockDisabledInstance() {
    when(_dataProvider.getDisabledInstances()).thenReturn(Collections.singleton(INSTANCE));
  }

  /** Publish an {@link InstanceConfig} for the instance under test. */
  private void mockInstanceConfig(InstanceConfig instanceConfig) {
    when(_dataProvider.getInstanceConfigMap())
        .thenReturn(Collections.singletonMap(INSTANCE, instanceConfig));
  }

  /** Disable the named partitions of {@code resourceName} on the instance under test. */
  private void mockDisabledPartitions(String resourceName, String... partitionNames) {
    InstanceConfig instanceConfig = new InstanceConfig(INSTANCE);
    for (String partitionName : partitionNames) {
      instanceConfig.setInstanceEnabledForPartition(resourceName, partitionName, false);
    }
    mockInstanceConfig(instanceConfig);
  }

  /** Publish an IdealState for {@code resourceName} with HELIX_ENABLED set to {@code enabled}. */
  private void mockResourceEnabled(String resourceName, boolean enabled) {
    IdealState idealState = new IdealState(resourceName);
    idealState.enable(enabled);
    when(_dataProvider.getIdealState(resourceName)).thenReturn(idealState);
  }

  /**
   * OFFLINE is the MasterSlave initial state. With nothing disabled, an OFFLINE partition is one
   * whose transition has not finished, so it must not be reported as held. DROPPED is filtered
   * defensively; participants subtract the partition entry rather than persisting DROPPED, so it is
   * not expected in a real CurrentState.
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

  /**
   * A disabled instance is deliberately held in the initial state by the rebalancer, so its OFFLINE
   * partitions are exactly where the controller wants them and must be counted as held. Reporting
   * zero here would make an intentionally disabled host indistinguishable from a broken one.
   */
  @Test
  public void testDisabledInstanceCountsInitialStatePartitionsAsHeld() {
    Map<String, String> partitionStates = new HashMap<>();
    partitionStates.put("p0", "OFFLINE");
    partitionStates.put("p1", "OFFLINE");
    partitionStates.put("p2", "OFFLINE");
    mockCurrentStates(currentState("db", MASTER_SLAVE, partitionStates));
    mockDisabledInstance();

    InstancePartitionCounts counts = _stage.computeInstancePartitionCounts(_dataProvider, INSTANCE);

    Assert.assertEquals(counts.actualPartitionCount, 3L,
        "A disabled instance holds its partitions OFFLINE on purpose");
    Assert.assertEquals(counts.actualTopStatePartitionCount, 0L,
        "A disabled instance has no top-state replica, and the target it is compared against is "
            + "zero too");
    Assert.assertEquals(counts.errorCount, 0L);
  }

  /**
   * Disabling every resource on an instance is expressed with the ALL_RESOURCES key rather than the
   * DISABLE instance operation, and the data provider treats both as disabled for every partition.
   */
  @Test
  public void testAllResourcesDisabledKeyCountsInitialStatePartitionsAsHeld() {
    Map<String, String> partitionStates = new HashMap<>();
    partitionStates.put("p0", "OFFLINE");
    partitionStates.put("p1", "MASTER");
    mockCurrentStates(currentState("db", MASTER_SLAVE, partitionStates));
    mockDisabledPartitions(InstanceConstants.ALL_RESOURCES_DISABLED_PARTITION_KEY, "p0");

    InstancePartitionCounts counts = _stage.computeInstancePartitionCounts(_dataProvider, INSTANCE);

    Assert.assertEquals(counts.actualPartitionCount, 2L,
        "The ALL_RESOURCES key disables the instance for every partition");
    Assert.assertEquals(counts.actualTopStatePartitionCount, 1L);
  }

  /**
   * A disabled resource is parked in the initial state for every instance, and the credit must not
   * leak to resources that are still enabled.
   */
  @Test
  public void testDisabledResourceCountsOnlyItsOwnInitialStatePartitions() {
    Map<String, String> disabledResourceStates = new HashMap<>();
    disabledResourceStates.put("p0", "OFFLINE");
    disabledResourceStates.put("p1", "OFFLINE");

    Map<String, String> enabledResourceStates = new HashMap<>();
    enabledResourceStates.put("p0", "OFFLINE");
    enabledResourceStates.put("p1", "LEADER");

    mockCurrentStates(currentState("db", MASTER_SLAVE, disabledResourceStates),
        currentState("service", LEADER_STANDBY, enabledResourceStates));
    mockResourceEnabled("db", false);
    mockResourceEnabled("service", true);

    InstancePartitionCounts counts = _stage.computeInstancePartitionCounts(_dataProvider, INSTANCE);

    Assert.assertEquals(counts.actualPartitionCount, 3L,
        "Both partitions of the disabled resource count, but the enabled resource's OFFLINE "
            + "partition does not");
    Assert.assertEquals(counts.actualTopStatePartitionCount, 1L);
  }

  /**
   * A partition disabled on this instance is held in the initial state on purpose, while an OFFLINE
   * partition with no disablement behind it is still treated as not held.
   */
  @Test
  public void testDisabledPartitionsCountAsHeldButOtherOfflinePartitionsDoNot() {
    Map<String, String> partitionStates = new HashMap<>();
    partitionStates.put("p0", "OFFLINE");
    partitionStates.put("p1", "OFFLINE");
    partitionStates.put("p2", "MASTER");
    mockCurrentStates(currentState("db", MASTER_SLAVE, partitionStates));
    mockDisabledPartitions("db", "p0");

    InstancePartitionCounts counts = _stage.computeInstancePartitionCounts(_dataProvider, INSTANCE);

    Assert.assertEquals(counts.actualPartitionCount, 2L,
        "Only the disabled OFFLINE partition is credited, not the one that is merely OFFLINE");
    Assert.assertEquals(counts.actualTopStatePartitionCount, 1L);
  }

  /**
   * Disabled partitions are recorded per resource, so a partition name disabled on one resource
   * must not credit the same partition name on another.
   */
  @Test
  public void testDisabledPartitionsAreScopedToTheirOwnResource() {
    mockCurrentStates(
        currentState("db", MASTER_SLAVE, Collections.singletonMap("p0", "OFFLINE")),
        currentState("service", LEADER_STANDBY, Collections.singletonMap("p0", "OFFLINE")));
    mockDisabledPartitions("db", "p0");

    InstancePartitionCounts counts = _stage.computeInstancePartitionCounts(_dataProvider, INSTANCE);

    Assert.assertEquals(counts.actualPartitionCount, 1L,
        "Disabling db's p0 must not credit service's p0");
    Assert.assertEquals(counts.actualTopStatePartitionCount, 0L);
  }

  /**
   * DROPPED means the partition is gone, which disablement does not change.
   */
  @Test
  public void testDroppedIsStillExcludedWhenTheInstanceIsDisabled() {
    Map<String, String> partitionStates = new HashMap<>();
    partitionStates.put("p0", "OFFLINE");
    partitionStates.put("p1", "DROPPED");
    mockCurrentStates(currentState("db", MASTER_SLAVE, partitionStates));
    mockDisabledInstance();

    InstancePartitionCounts counts = _stage.computeInstancePartitionCounts(_dataProvider, INSTANCE);

    Assert.assertEquals(counts.actualPartitionCount, 1L,
        "Disablement credits initial-state partitions, never DROPPED ones");
  }

  /**
   * A missing IdealState means the resource is being removed, so its partitions are on their way to
   * DROPPED rather than parked in the initial state. That must not be read as a disabled resource.
   */
  @Test
  public void testMissingIdealStateIsNotTreatedAsADisabledResource() {
    mockCurrentStates(currentState("db", MASTER_SLAVE, Collections.singletonMap("p0", "OFFLINE")));
    when(_dataProvider.getIdealState("db")).thenReturn(null);

    InstancePartitionCounts counts = _stage.computeInstancePartitionCounts(_dataProvider, INSTANCE);

    Assert.assertEquals(counts.actualPartitionCount, 0L);
  }

  /**
   * If the disablement lookup fails the instance's counts must still be produced. Losing the
   * disablement credit understates the gauge, but losing the instance entirely would report zero
   * for a healthy host.
   */
  @Test
  public void testDisablementLookupFailureStillProducesCounts() {
    Map<String, String> partitionStates = new HashMap<>();
    partitionStates.put("p0", "MASTER");
    partitionStates.put("p1", "SLAVE");
    partitionStates.put("p2", "OFFLINE");
    mockCurrentStates(currentState("db", MASTER_SLAVE, partitionStates));
    when(_dataProvider.getInstanceConfigMap()).thenThrow(new RuntimeException("config read failed"));

    InstancePartitionCounts counts = _stage.computeInstancePartitionCounts(_dataProvider, INSTANCE);

    Assert.assertEquals(counts.actualPartitionCount, 2L,
        "A failed disablement lookup degrades to no disablement rather than dropping the instance");
    Assert.assertEquals(counts.actualTopStatePartitionCount, 1L);
  }

  /**
   * An instance with no InstanceConfig at all must not throw, and gets no disablement credit.
   */
  @Test
  public void testMissingInstanceConfigYieldsNoDisablementCredit() {
    Map<String, String> partitionStates = new HashMap<>();
    partitionStates.put("p0", "MASTER");
    partitionStates.put("p1", "OFFLINE");
    mockCurrentStates(currentState("db", MASTER_SLAVE, partitionStates));
    when(_dataProvider.getInstanceConfigMap()).thenReturn(Collections.emptyMap());

    InstancePartitionCounts counts = _stage.computeInstancePartitionCounts(_dataProvider, INSTANCE);

    Assert.assertEquals(counts.actualPartitionCount, 1L);
    Assert.assertEquals(counts.actualTopStatePartitionCount, 1L);
  }
}
