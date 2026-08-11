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
import java.util.Map;

import org.apache.helix.MockAccessor;
import org.apache.helix.controller.dataproviders.ResourceControllerDataProvider;
import org.apache.helix.model.ClusterConfig;
import org.apache.helix.model.ConvergenceStatus;
import org.apache.helix.model.ConvergenceStatus.Reason;
import org.apache.helix.model.ConvergenceStatus.Status;
import org.apache.helix.model.IdealState;
import org.apache.helix.model.IdealState.RebalanceMode;
import org.apache.helix.model.LiveInstance;
import org.apache.helix.model.Message;
import org.apache.helix.model.Message.MessageType;
import org.apache.helix.model.Partition;
import org.apache.helix.model.Resource;
import org.apache.helix.model.StateModelDefinition;
import org.apache.helix.tools.StateModelConfigGenerator;
import org.testng.Assert;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

public class TestConvergenceStatusCalculator {
  private static final String CLUSTER = "TestConvergenceCluster";
  private static final String RESOURCE = "TestDB";
  private static final String PARTITION_NAME = "TestDB_0";
  private static final String INSTANCE = "localhost_12918";
  private static final String STATE_MODEL = "MasterSlave";

  private ClusterEvent _event;
  private Partition _partition;
  private CurrentStateOutput _currentStateOutput;
  private BestPossibleStateOutput _bestPossibleStateOutput;

  @BeforeMethod
  public void setUp() {
    MockAccessor accessor = new MockAccessor();
    accessor.setProperty(accessor.keyBuilder().clusterConfig(), new ClusterConfig(CLUSTER));

    StateModelDefinition stateModelDefinition =
        new StateModelDefinition(StateModelConfigGenerator.generateConfigForMasterSlave());
    accessor.setProperty(accessor.keyBuilder().stateModelDef(STATE_MODEL), stateModelDefinition);

    IdealState idealState = new IdealState(RESOURCE);
    idealState.setStateModelDefRef(STATE_MODEL);
    idealState.setRebalanceMode(RebalanceMode.CUSTOMIZED);
    idealState.setNumPartitions(1);
    idealState.setReplicas("1");
    accessor.setProperty(accessor.keyBuilder().idealStates(RESOURCE), idealState);

    LiveInstance liveInstance = new LiveInstance(INSTANCE);
    liveInstance.setSessionId("session_0");
    accessor.setProperty(accessor.keyBuilder().liveInstance(INSTANCE), liveInstance);

    ResourceControllerDataProvider cache = new ResourceControllerDataProvider(CLUSTER);
    cache.refresh(accessor);

    Resource resource = new Resource(RESOURCE);
    resource.setStateModelDefRef(STATE_MODEL);
    resource.addPartition(PARTITION_NAME);
    _partition = resource.getPartition(PARTITION_NAME);

    _currentStateOutput = new CurrentStateOutput();
    _bestPossibleStateOutput = new BestPossibleStateOutput();
    _event = new ClusterEvent(CLUSTER, ClusterEventType.CurrentStateChange, "event_0");
    _event.addAttribute(AttributeName.ControllerDataProvider.name(), cache);
    _event.addAttribute(AttributeName.RESOURCES_TO_REBALANCE.name(),
        Collections.singletonMap(RESOURCE, resource));
    _event.addAttribute(AttributeName.CURRENT_STATE.name(), _currentStateOutput);
    _event.addAttribute(AttributeName.BEST_POSSIBLE_STATE.name(), _bestPossibleStateOutput);
    _event.addAttribute(AttributeName.helixmanager.name(),
        new DummyClusterManager(CLUSTER, accessor, "controller_session"));
    _event.addAttribute(AttributeName.MESSAGES_ALL.name(), new MessageOutput());
    _event.addAttribute(AttributeName.MESSAGES_SELECTED.name(), new MessageOutput());
    _event.addAttribute(AttributeName.MESSAGES_THROTTLE.name(), new MessageOutput());
    _event.addAttribute(AttributeName.MESSAGE_DISPATCH_RESULT.name(),
        new MessageDispatchResult(Collections.emptyList(), Collections.emptyList()));
  }

  @Test
  public void testCalculate_matchingAssignment_reportsConverged() {
    setCurrentAndTarget("MASTER", "MASTER");

    ConvergenceStatus status = calculateResourceStatus();

    Assert.assertEquals(status.getStatus(), Status.CONVERGED);
    Assert.assertEquals(status.getConvergedPartitionCount(), 1);
    Assert.assertEquals(status.getAffectedPartitionCount(), 0);
    Assert.assertTrue(status.getPartitionDetails().isEmpty());
  }

  @Test
  public void testCalculate_pendingTransition_reportsInProgress() {
    setCurrentAndTarget("SLAVE", "MASTER");
    _currentStateOutput.setPendingMessage(RESOURCE, _partition, INSTANCE,
        transitionMessage("pending", "SLAVE", "MASTER"));

    ConvergenceStatus status = calculateResourceStatus();

    Assert.assertEquals(status.getStatus(), Status.IN_PROGRESS);
    Assert.assertEquals(status.getPrimaryReason(), Reason.PENDING_TRANSITION);
    Assert.assertEquals(status.getInProgressPartitionCount(), 1);
    Assert.assertEquals(status.getPartitionDetails().get(0).getCurrentAssignment().get(INSTANCE),
        "SLAVE");
    Assert.assertEquals(status.getPartitionDetails().get(0).getTargetAssignment().get(INSTANCE),
        "MASTER");
  }

  @Test
  public void testCalculate_generatedButUnselected_reportsConstraintWait() {
    setCurrentAndTarget("SLAVE", "MASTER");
    MessageOutput generated = new MessageOutput();
    generated.addMessage(RESOURCE, _partition, transitionMessage("generated", "SLAVE", "MASTER"));
    _event.addAttribute(AttributeName.MESSAGES_ALL.name(), generated);

    ConvergenceStatus status = calculateResourceStatus();

    Assert.assertEquals(status.getStatus(), Status.IN_PROGRESS);
    Assert.assertEquals(status.getPrimaryReason(), Reason.STATE_CONSTRAINT_WAIT);
  }

  @Test
  public void testCalculate_failedDispatch_reportsBlocked() {
    setCurrentAndTarget("SLAVE", "MASTER");
    Message failed = transitionMessage("failed", "SLAVE", "MASTER");
    _event.addAttribute(AttributeName.MESSAGE_DISPATCH_RESULT.name(),
        new MessageDispatchResult(Collections.emptyList(), Collections.singletonList(failed)));

    ConvergenceStatus status = calculateResourceStatus();

    Assert.assertEquals(status.getStatus(), Status.BLOCKED);
    Assert.assertEquals(status.getPrimaryReason(), Reason.MESSAGE_DISPATCH_FAILED);
    Assert.assertEquals(status.getBlockedPartitionCount(), 1);
  }

  @Test
  public void testCalculate_noProgressPath_reportsBlocked() {
    setCurrentAndTarget("SLAVE", "MASTER");

    ConvergenceStatus status = calculateResourceStatus();

    Assert.assertEquals(status.getStatus(), Status.BLOCKED);
    Assert.assertEquals(status.getPrimaryReason(), Reason.NO_PROGRESS_PATH);
  }

  @Test
  public void testCalculate_missingTarget_reportsUnknown() {
    _currentStateOutput.setCurrentState(RESOURCE, _partition, INSTANCE, "SLAVE");
    _event.addAttribute(AttributeName.BEST_POSSIBLE_STATE.name(), new BestPossibleStateOutput());

    ConvergenceStatus status = calculateResourceStatus();

    Assert.assertEquals(status.getStatus(), Status.UNKNOWN);
    Assert.assertEquals(status.getPrimaryReason(), Reason.TARGET_ASSIGNMENT_MISSING);
    Assert.assertEquals(status.getUnknownPartitionCount(), 1);
  }

  @Test
  public void testCalculate_initialAndDroppedStates_areTreatedAsAbsent() {
    Map<String, String> target = new HashMap<>();
    target.put(INSTANCE, "DROPPED");
    _currentStateOutput.setCurrentState(RESOURCE, _partition, INSTANCE, "OFFLINE");
    _bestPossibleStateOutput.setState(RESOURCE, _partition, target);

    ConvergenceStatus status = calculateResourceStatus();

    Assert.assertEquals(status.getStatus(), Status.CONVERGED);
  }

  @Test
  public void testCalculate_largeAffectedResource_keepsDiagnosticDetailsBounded() {
    @SuppressWarnings("unchecked")
    Map<String, Resource> resources =
        _event.getAttribute(AttributeName.RESOURCES_TO_REBALANCE.name());
    Resource resource = resources.get(RESOURCE);
    int partitionCount = 10_000;
    for (int i = 0; i < partitionCount; i++) {
      String partitionName = RESOURCE + "_" + i;
      resource.addPartition(partitionName);
      Partition partition = resource.getPartition(partitionName);
      _currentStateOutput.setCurrentState(RESOURCE, partition, INSTANCE, "SLAVE");
      _bestPossibleStateOutput.setState(RESOURCE, partition, INSTANCE, "MASTER");
    }

    ConvergenceStatus status = calculateResourceStatus();

    Assert.assertEquals(status.getTotalPartitionCount(), partitionCount);
    Assert.assertEquals(status.getAffectedPartitionCount(), partitionCount);
    Assert.assertEquals(status.getPartitionDetails().size(),
        ConvergenceStatus.DEFAULT_MAX_PARTITION_DETAILS);
    Assert.assertEquals(status.getTruncatedPartitionCount(),
        partitionCount - ConvergenceStatus.DEFAULT_MAX_PARTITION_DETAILS);
  }

  private void setCurrentAndTarget(String currentState, String targetState) {
    _currentStateOutput.setCurrentState(RESOURCE, _partition, INSTANCE, currentState);
    _bestPossibleStateOutput.setState(RESOURCE, _partition, INSTANCE, targetState);
  }

  private ConvergenceStatus calculateResourceStatus() {
    ConvergenceStatusSnapshot snapshot = new ConvergenceStatusCalculator().calculate(_event);
    return snapshot.getResourceStatuses().get(RESOURCE);
  }

  private Message transitionMessage(String id, String fromState, String toState) {
    Message message = new Message(MessageType.STATE_TRANSITION, id);
    message.setResourceName(RESOURCE);
    message.setPartitionName(PARTITION_NAME);
    message.setTgtName(INSTANCE);
    message.setFromState(fromState);
    message.setToState(toState);
    return message;
  }
}
