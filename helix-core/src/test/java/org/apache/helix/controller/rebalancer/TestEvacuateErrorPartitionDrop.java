package org.apache.helix.controller.rebalancer;

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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.helix.HelixManager;
import org.apache.helix.api.config.StateTransitionThrottleConfig;
import org.apache.helix.controller.dataproviders.ResourceControllerDataProvider;
import org.apache.helix.controller.rebalancer.constraint.MonitoredAbnormalResolver;
import org.apache.helix.controller.rebalancer.waged.WagedRebalancer;
import org.apache.helix.controller.stages.AttributeName;
import org.apache.helix.controller.stages.BestPossibleStateOutput;
import org.apache.helix.controller.stages.ClusterEvent;
import org.apache.helix.controller.stages.ClusterEventType;
import org.apache.helix.controller.stages.CurrentStateOutput;
import org.apache.helix.controller.stages.IntermediateStateCalcStage;
import org.apache.helix.controller.stages.MessageGenerationPhase;
import org.apache.helix.controller.stages.MessageOutput;
import org.apache.helix.controller.stages.MessageSelectionStage;
import org.apache.helix.model.BuiltInStateModelDefinitions;
import org.apache.helix.model.ClusterConfig;
import org.apache.helix.model.IdealState;
import org.apache.helix.model.LiveInstance;
import org.apache.helix.model.Message;
import org.apache.helix.model.Partition;
import org.apache.helix.model.Resource;
import org.apache.helix.model.StateModelDefinition;
import org.testng.Assert;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class TestEvacuateErrorPartitionDrop {
  private static final String RESOURCE = "resource";
  private static final Partition PARTITION = new Partition("partition");
  private static final String OLD_INSTANCE = "old";

  private ClusterConfig _clusterConfig;
  private IdealState _idealState;
  private StateModelDefinition _stateModel;
  private CurrentStateOutput _currentStateOutput;
  private ResourceControllerDataProvider _cache;
  private List<String> _preferenceList;
  private Set<String> _liveInstances;
  private Set<String> _enabledLiveInstances;
  private Set<String> _evacuatingInstances;
  private Set<String> _disabledInstances;

  @BeforeMethod
  public void setUp() {
    _clusterConfig = new ClusterConfig("cluster");
    _clusterConfig.setEvacuateErrorPartitionDropEnabled(true);
    _stateModel = BuiltInStateModelDefinitions.LeaderStandby.getStateModelDefinition();
    _idealState = new IdealState(RESOURCE);
    _idealState.setReplicas("3");
    _idealState.setMinActiveReplicas(2);
    _idealState.setStateModelDefRef(_stateModel.getId());
    _idealState.setRebalanceMode(IdealState.RebalanceMode.FULL_AUTO);
    _idealState.setRebalancerClassName(WagedRebalancer.class.getName());
    _preferenceList = new ArrayList<>(Arrays.asList("a", "b", "c"));
    _liveInstances = new HashSet<>(Arrays.asList("a", "b", "c", OLD_INSTANCE, "other"));
    _enabledLiveInstances = new HashSet<>(Arrays.asList("a", "b", "c", "other"));
    _evacuatingInstances = new HashSet<>(Collections.singleton(OLD_INSTANCE));
    _disabledInstances = new HashSet<>();
    setCurrentStates(Map.of("a", "LEADER", "b", "STANDBY", "c", "STANDBY",
        OLD_INSTANCE, "ERROR"));

    _cache = mock(ResourceControllerDataProvider.class);
    when(_cache.getClusterConfig()).thenReturn(_clusterConfig);
    when(_cache.getEnabledLiveInstances()).thenReturn(_enabledLiveInstances);
    when(_cache.getEvacuatingInstances()).thenReturn(_evacuatingInstances);
    when(_cache.checkAndReduceCapacity(anyString(), anyString(), anyString())).thenReturn(true);
  }

  @Test
  public void testDropAfterFullTargetConverges() {
    Assert.assertEquals(computeAssignment(),
        Map.of("a", "LEADER", "b", "STANDBY", "c", "STANDBY", OLD_INSTANCE, "DROPPED"));
  }

  @Test
  public void testPreserveErrorByDefault() {
    _clusterConfig.getRecord().getSimpleFields().remove(
        ClusterConfig.ClusterConfigProperty.EVACUATE_ERROR_PARTITION_DROP_ENABLED.name());
    Assert.assertEquals(computeAssignment().get(OLD_INSTANCE), "ERROR");
  }

  @Test
  public void testPreserveErrorWhenFeatureDisabled() {
    _clusterConfig.setEvacuateErrorPartitionDropEnabled(false);
    Assert.assertEquals(computeAssignment().get(OLD_INSTANCE), "ERROR");
  }

  @Test
  public void testPreserveErrorWithoutControllerCache() {
    Assert.assertEquals(new DelayedAutoRebalancer().computeBestPossibleStateForPartition(
        _liveInstances, _stateModel, _preferenceList, _currentStateOutput, _disabledInstances,
        _idealState, _clusterConfig, PARTITION, MonitoredAbnormalResolver.DUMMY_STATE_RESOLVER)
        .get(OLD_INSTANCE), "ERROR");
  }

  @Test
  public void testPreserveErrorOnNonEvacuatingInstance() {
    _evacuatingInstances.clear();
    Assert.assertEquals(computeAssignment().get(OLD_INSTANCE), "ERROR");
  }

  @Test
  public void testPreserveErrorStillInTargetAssignment() {
    _preferenceList = Arrays.asList("a", "b", OLD_INSTANCE);
    Assert.assertEquals(computeAssignment().get(OLD_INSTANCE), "ERROR");
  }

  @DataProvider
  public Object[][] unreadyStates() {
    return new Object[][] {{"OFFLINE"}, {"ERROR"}, {"DROPPED"}};
  }

  @Test(dataProvider = "unreadyStates")
  public void testPreserveErrorUntilReplacementReady(String replacementState) {
    _currentStateOutput.setCurrentState(RESOURCE, PARTITION, "c", replacementState);
    Assert.assertEquals(computeAssignment().get(OLD_INSTANCE), "ERROR");
  }

  @Test
  public void testPreserveErrorUntilLeaderReady() {
    _currentStateOutput.setCurrentState(RESOURCE, PARTITION, "a", "STANDBY");
    Assert.assertEquals(computeAssignment().get(OLD_INSTANCE), "ERROR");
  }

  @Test
  public void testPreserveErrorWhenTargetMissingFromCombinedList() {
    // More than RF current holders prevents admitting c into the combined list. The ordinary
    // readyToDrop check can pass for a and b alone, but that must not release an ERROR copy.
    setCurrentStates(Map.of("a", "LEADER", "b", "STANDBY", OLD_INSTANCE, "ERROR",
        "other", "ERROR"));
    Assert.assertEquals(computeAssignment().get(OLD_INSTANCE), "ERROR");
  }

  @Test
  public void testPreserveErrorWhenOnlyMinActiveReplicasRemain() {
    _preferenceList = Arrays.asList("a", "b");
    setCurrentStates(Map.of("a", "LEADER", "b", "STANDBY", OLD_INSTANCE, "ERROR"));
    Assert.assertEquals(computeAssignment().get(OLD_INSTANCE), "ERROR");
  }

  @Test
  public void testPreserveErrorWhenAllTargetsInError() {
    setCurrentStates(Map.of("a", "ERROR", "b", "ERROR", "c", "ERROR",
        OLD_INSTANCE, "ERROR"));
    Assert.assertEquals(computeAssignment().get(OLD_INSTANCE), "ERROR");
  }

  @Test
  public void testPreserveErrorWhenTargetNotLive() {
    _enabledLiveInstances.remove("c");
    _liveInstances.remove("c");
    Assert.assertEquals(computeAssignment().get(OLD_INSTANCE), "ERROR");
  }

  @Test
  public void testPreserveErrorWhenTargetNotEnabled() {
    _enabledLiveInstances.remove("c");
    Assert.assertEquals(computeAssignment().get(OLD_INSTANCE), "ERROR");
  }

  @Test
  public void testPreserveErrorWhenTargetPartitionDisabled() {
    _disabledInstances.add("c");
    _currentStateOutput.setCurrentState(RESOURCE, PARTITION, "c", "OFFLINE");
    Assert.assertEquals(computeAssignment().get(OLD_INSTANCE), "ERROR");
  }

  @Test
  public void testPreserveErrorWhileTargetTransitionPending() {
    Message pending = new Message(Message.MessageType.STATE_TRANSITION, "pending");
    pending.setFromState("STANDBY");
    pending.setToState("OFFLINE");
    _currentStateOutput.setPendingMessage(RESOURCE, PARTITION, "c", pending);
    Assert.assertEquals(computeAssignment().get(OLD_INSTANCE), "ERROR");
  }

  @Test
  public void testPreserveErrorWithEmptyTargetAssignment() {
    _preferenceList = Collections.emptyList();
    Assert.assertEquals(computeAssignment().get(OLD_INSTANCE), "ERROR");
  }

  @Test
  public void testDropOnlyEvacuatingErrorReplicas() {
    _currentStateOutput.setCurrentState(RESOURCE, PARTITION, "other", "ERROR");
    Map<String, String> assignment = computeAssignment();
    Assert.assertEquals(assignment.get(OLD_INSTANCE), "DROPPED");
    Assert.assertEquals(assignment.get("other"), "ERROR");
  }

  @Test
  public void testOnlineOfflineStateModel() {
    _stateModel = BuiltInStateModelDefinitions.OnlineOffline.getStateModelDefinition();
    _idealState.setStateModelDefRef(_stateModel.getId());
    setCurrentStates(Map.of("a", "ONLINE", "b", "ONLINE", "c", "ONLINE",
        OLD_INSTANCE, "ERROR"));
    Assert.assertEquals(computeAssignment().get(OLD_INSTANCE), "DROPPED");
  }

  @Test
  public void testResourceRemovalStillDropsError() {
    _clusterConfig.setEvacuateErrorPartitionDropEnabled(false);
    _preferenceList = null;
    Assert.assertEquals(computeAssignment().get(OLD_INSTANCE), "DROPPED");
  }

  @DataProvider
  public Object[][] pipelineModes() {
    return new Object[][] {
        {false, false, 1L}, {true, false, 1L}, {true, false, 0L},
        {false, true, 1L}, {true, true, 1L}, {true, true, 0L}
    };
  }

  @Test(dataProvider = "pipelineModes")
  public void testErrorToDroppedMessageSurvivesPipeline(boolean enabled, boolean useV2, long quota)
      throws Exception {
    _clusterConfig.setEvacuateErrorPartitionDropEnabled(enabled);
    _clusterConfig.setIntermediateStateCalcStageV2Enabled(useV2);
    _clusterConfig.setErrorOrRecoveryPartitionThresholdForLoadBalance(0);
    _clusterConfig.setStateTransitionThrottleConfigs(Collections.singletonList(
        new StateTransitionThrottleConfig(StateTransitionThrottleConfig.RebalanceType.LOAD_BALANCE,
            StateTransitionThrottleConfig.ThrottleScope.CLUSTER, quota)));
    BestPossibleStateOutput bestPossibleState = new BestPossibleStateOutput();
    bestPossibleState.setState(RESOURCE, PARTITION, computeAssignment());
    bestPossibleState.setPreferenceLists(RESOURCE,
        Collections.singletonMap(PARTITION.getPartitionName(), _preferenceList));
    Resource resource = new Resource(RESOURCE);
    resource.setStateModelDefRef(_stateModel.getId());
    resource.addPartition(PARTITION.getPartitionName());

    Map<String, LiveInstance> liveInstances = new HashMap<>();
    for (String name : _liveInstances) {
      LiveInstance liveInstance = new LiveInstance(name);
      liveInstance.setSessionId("session-" + name);
      liveInstances.put(name, liveInstance);
    }
    when(_cache.getLiveInstances()).thenReturn(liveInstances);
    when(_cache.getStateModelDef(_stateModel.getId())).thenReturn(_stateModel);
    when(_cache.getStaleMessagesByInstance(anyString())).thenReturn(Collections.emptySet());
    when(_cache.getIdealState(RESOURCE)).thenReturn(_idealState);
    HelixManager manager = mock(HelixManager.class);
    when(manager.getInstanceName()).thenReturn("controller");
    when(manager.getSessionId()).thenReturn("controller-session");

    ClusterEvent event = new ClusterEvent(ClusterEventType.InstanceConfigChange);
    event.addAttribute(AttributeName.helixmanager.name(), manager);
    event.addAttribute(AttributeName.ControllerDataProvider.name(), _cache);
    event.addAttribute(AttributeName.RESOURCES.name(),
        Collections.singletonMap(RESOURCE, resource));
    event.addAttribute(AttributeName.RESOURCES_TO_REBALANCE.name(),
        Collections.singletonMap(RESOURCE, resource));
    event.addAttribute(AttributeName.CURRENT_STATE.name(), _currentStateOutput);
    event.addAttribute(AttributeName.BEST_POSSIBLE_STATE.name(), bestPossibleState);
    new MessageGenerationPhase().process(event);
    new MessageSelectionStage().process(event);
    MessageOutput selected = event.getAttribute(AttributeName.MESSAGES_SELECTED.name());
    Assert.assertEquals(selected.getMessages(RESOURCE, PARTITION).size(), enabled ? 1 : 0);
    new IntermediateStateCalcStage().process(event);

    MessageOutput output = event.getAttribute(AttributeName.MESSAGES_SELECTED.name());
    List<Message> messages = output.getMessages(RESOURCE, PARTITION);
    boolean shouldSendDrop = enabled && quota > 0;
    Assert.assertEquals(messages.size(), shouldSendDrop ? 1 : 0);
    if (shouldSendDrop) {
      Assert.assertEquals(messages.get(0).getTgtName(), OLD_INSTANCE);
      Assert.assertEquals(messages.get(0).getFromState(), "ERROR");
      Assert.assertEquals(messages.get(0).getToState(), "DROPPED");
    }
  }

  private void setCurrentStates(Map<String, String> states) {
    _currentStateOutput = new CurrentStateOutput();
    states.forEach((instance, state) ->
        _currentStateOutput.setCurrentState(RESOURCE, PARTITION, instance, state));
  }

  private Map<String, String> computeAssignment() {
    return new DelayedAutoRebalancer().computeBestPossibleStateForPartition(_liveInstances,
        _stateModel, _preferenceList, _currentStateOutput, _disabledInstances, _idealState,
        _clusterConfig, PARTITION, MonitoredAbnormalResolver.DUMMY_STATE_RESOLVER, _cache);
  }
}
