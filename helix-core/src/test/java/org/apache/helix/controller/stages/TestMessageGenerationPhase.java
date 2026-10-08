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

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import org.apache.helix.HelixConstants;
import org.apache.helix.controller.dataproviders.BaseControllerDataProvider;
import org.apache.helix.controller.dataproviders.ResourceControllerDataProvider;
import org.apache.helix.controller.dataproviders.WorkflowControllerDataProvider;
import org.apache.helix.controller.stages.task.TaskSchedulingStage;
import org.apache.helix.model.BuiltInStateModelDefinitions;
import org.apache.helix.model.ClusterConfig;
import org.apache.helix.model.CurrentState;
import org.apache.helix.model.IdealState;
import org.apache.helix.model.Message;
import org.apache.helix.model.Partition;
import org.apache.helix.model.Resource;
import org.apache.helix.model.ResourceConfig;
import org.apache.helix.task.JobConfig;
import org.apache.helix.task.TaskConfig;
import org.apache.helix.task.TaskConstants;
import org.apache.helix.task.WorkflowConfig;
import org.apache.helix.zookeeper.datamodel.ZNRecord;
import org.testng.Assert;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import static org.mockito.Mockito.clearInvocations;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

public class TestMessageGenerationPhase extends BaseStageTest {
  private static final String WORKFLOW = "workflow";
  private static final String JOB = "workflow_job";
  private static final String PARTITION = JOB + "_0";
  private static final String LEGACY_FACTORY = "ignoredResourceConfigFactory";

  @Test
  public void testTaskConfigFactoriesDoNotSelectExecutionFactory() throws Exception {
    prepareTaskCluster(1);
    JobConfig jobConfig = writeJobConfig();
    WorkflowConfig workflowConfig = new WorkflowConfig.Builder().setWorkflowId(WORKFLOW).build();
    workflowConfig.putSimpleConfig("STATE_MODEL_FACTORY_NAME", LEGACY_FACTORY);
    accessor.setProperty(accessor.keyBuilder().resourceConfig(WORKFLOW), workflowConfig);
    ZNRecord originalJob = new ZNRecord(jobConfig.getRecord());
    ZNRecord originalWorkflow = new ZNRecord(workflowConfig.getRecord());
    readResources(new WorkflowControllerDataProvider());

    Map<String, Resource> resources = event.getAttribute(AttributeName.RESOURCES.name());
    Assert.assertEquals(resources.get(WORKFLOW).getStateModelFactoryname(), "DEFAULT");
    Assert.assertEquals(resources.get(JOB).getStateModelFactoryname(), "DEFAULT");
    new CurrentStateComputationStage().process(event);
    BestPossibleStateOutput assignment = new BestPossibleStateOutput();
    assignment.setState(JOB, new Partition(PARTITION), HOSTNAME_PREFIX + 0, "RUNNING");
    event.addAttribute(AttributeName.BEST_POSSIBLE_STATE.name(), assignment);

    Message message = onlyMessage(generateMessages());
    Assert.assertEquals(message.getStateModelFactoryName(), "DEFAULT");
    Assert.assertEquals(message.getStateModelDef(), TaskConstants.STATE_MODEL_NAME);
    Assert.assertEquals(message.getFromState(), "INIT");
    Assert.assertEquals(message.getToState(), "RUNNING");
    Assert.assertEquals(jobConfig.getRecord(), originalJob);
    Assert.assertEquals(workflowConfig.getRecord(), originalWorkflow);
  }

  @DataProvider
  public Object[][] orphanFactories() {
    return new Object[][] {
        {true, "runtimeFactory"}, {false, "runtimeFactory"},
        {true, null}, {false, null}, {true, ""}
    };
  }

  @Test(dataProvider = "orphanFactories")
  public void testOrphanDropsUseEachParticipantsRuntimeFactory(boolean keepJobConfig,
      String factoryName) throws Exception {
    prepareTaskCluster(2);
    if (keepJobConfig) {
      writeJobConfig();
    }
    CurrentState first = writeTaskCurrentState(0, "RUNNING", factoryName);
    CurrentState second = writeTaskCurrentState(1, "RUNNING", "otherParticipantFactory");
    ZNRecord originalFirst = new ZNRecord(first.getRecord());
    ZNRecord originalSecond = new ZNRecord(second.getRecord());
    readResources(new WorkflowControllerDataProvider());
    new CurrentStateComputationStage().process(event);
    new TaskSchedulingStage().process(event);

    List<Message> messages = generateMessages();
    Map<String, String> factories = messages.stream().collect(
        Collectors.toMap(Message::getTgtName, Message::getStateModelFactoryName));
    Map<String, String> expected = new HashMap<>();
    expected.put(HOSTNAME_PREFIX + 0,
        factoryName == null ? HelixConstants.DEFAULT_STATE_MODEL_FACTORY : factoryName);
    expected.put(HOSTNAME_PREFIX + 1, "otherParticipantFactory");
    Assert.assertEquals(factories, expected);
    for (Message message : messages) {
      Assert.assertEquals(message.getMsgType(), Message.MessageType.STATE_TRANSITION.name());
      Assert.assertEquals(message.getFromState(), "RUNNING");
      Assert.assertEquals(message.getToState(), "DROPPED");
      Assert.assertEquals(message.getResourceName(), JOB);
      Assert.assertEquals(message.getPartitionName(), PARTITION);
    }
    Assert.assertEquals(first.getRecord(), originalFirst);
    Assert.assertEquals(second.getRecord(), originalSecond);
  }

  @DataProvider
  public Object[][] pendingFactories() {
    return new Object[][] {
        {"INIT", "pendingFactory", false},
        {null, "pendingFactory", true},
        {"INIT", null, false},
        {"INIT", "", false}
    };
  }

  @Test
  public void testTaskDropsReadCurrentStateOncePerParticipant() throws Exception {
    prepareTaskCluster(1);
    writeJobConfig();
    CurrentState currentState = writeTaskCurrentState(0, "RUNNING", "runtimeFactory");
    String secondPartition = JOB + "_1";
    currentState.setState(secondPartition, "RUNNING");
    accessor.setProperty(
        accessor.keyBuilder().taskCurrentState(HOSTNAME_PREFIX + 0, SESSION_PREFIX + 0, JOB),
        currentState);
    WorkflowControllerDataProvider cache = spy(new WorkflowControllerDataProvider());
    readResources(cache);
    new CurrentStateComputationStage().process(event);
    new TaskSchedulingStage().process(event);
    clearInvocations(cache);

    Message first = onlyMessage(generateMessages());
    MessageOutput output = event.getAttribute(AttributeName.MESSAGES_ALL.name());
    Message second = onlyMessage(output.getMessages(JOB, new Partition(secondPartition)));

    Assert.assertEquals(first.getPartitionName(), PARTITION);
    Assert.assertEquals(second.getPartitionName(), secondPartition);
    for (Message message : Arrays.asList(first, second)) {
      Assert.assertEquals(message.getStateModelFactoryName(), "runtimeFactory");
      Assert.assertEquals(message.getToState(), "DROPPED");
    }
    verify(cache, times(1)).getCurrentState(HOSTNAME_PREFIX + 0, SESSION_PREFIX + 0, true);
  }

  @Test(dataProvider = "pendingFactories")
  public void testTaskCancellationsUsePendingMessageFactory(String currentState,
      String pendingFactory, boolean explicitDrop) throws Exception {
    prepareTaskCluster(1);
    writeJobConfig();
    if (currentState != null) {
      writeTaskCurrentState(0, currentState, "differentCurrentStateFactory");
    }
    Message pending = new Message(Message.MessageType.STATE_TRANSITION, "pendingTransition");
    pending.setResourceName(JOB);
    pending.setPartitionName(PARTITION);
    pending.setTgtName(HOSTNAME_PREFIX + 0);
    pending.setTgtSessionId(SESSION_PREFIX + 0);
    pending.setFromState("INIT");
    pending.setToState("RUNNING");
    pending.setStateModelDef(TaskConstants.STATE_MODEL_NAME);
    pending.setStateModelFactoryName(pendingFactory);
    accessor.setProperty(accessor.keyBuilder().message(HOSTNAME_PREFIX + 0, pending.getId()), pending);
    ZNRecord originalPending = new ZNRecord(pending.getRecord());
    readResources(new WorkflowControllerDataProvider());
    new CurrentStateComputationStage().process(event);
    new TaskSchedulingStage().process(event);
    if (explicitDrop) {
      BestPossibleStateOutput assignment =
          event.getAttribute(AttributeName.BEST_POSSIBLE_STATE.name());
      assignment.setState(JOB, new Partition(PARTITION), HOSTNAME_PREFIX + 0, "DROPPED");
    }

    Message cancellation = onlyMessage(generateMessages());
    Assert.assertEquals(cancellation.getMsgType(),
        Message.MessageType.STATE_TRANSITION_CANCELLATION.name());
    Assert.assertEquals(cancellation.getStateModelFactoryName(),
        pendingFactory == null ? HelixConstants.DEFAULT_STATE_MODEL_FACTORY : pendingFactory);
    Assert.assertEquals(cancellation.getFromState(), "INIT");
    Assert.assertEquals(cancellation.getToState(), "RUNNING");
    Assert.assertEquals(cancellation.getTgtSessionId(), SESSION_PREFIX + 0);
    Assert.assertEquals(pending.getRecord(), originalPending);
  }

  @Test
  public void testDropDoesNotUseFactoryFromAnotherSession() throws Exception {
    prepareTaskCluster(1);
    writeJobConfig();
    CurrentState oldState = new CurrentState(JOB);
    oldState.setSessionId("oldSession");
    oldState.setStateModelDefRef(TaskConstants.STATE_MODEL_NAME);
    oldState.setStateModelFactoryName("oldFactory");
    oldState.setState(PARTITION, "RUNNING");
    accessor.setProperty(
        accessor.keyBuilder().taskCurrentState(HOSTNAME_PREFIX + 0, "oldSession", JOB), oldState);
    readResources(new WorkflowControllerDataProvider());
    CurrentStateOutput currentState = new CurrentStateOutput();
    currentState.setCurrentState(JOB, new Partition(PARTITION), HOSTNAME_PREFIX + 0, "RUNNING");
    event.addAttribute(AttributeName.CURRENT_STATE.name(), currentState);
    BestPossibleStateOutput assignment = new BestPossibleStateOutput();
    assignment.setState(JOB, new Partition(PARTITION), HOSTNAME_PREFIX + 0, "DROPPED");
    event.addAttribute(AttributeName.BEST_POSSIBLE_STATE.name(), assignment);

    Assert.assertEquals(generateMessages(), Collections.emptyList());
  }

  @DataProvider
  public Object[][] idealStateFactories() {
    return new Object[][] {{null, "DEFAULT"}, {"idealStateFactory", "idealStateFactory"}, {"", ""}};
  }

  @Test(dataProvider = "idealStateFactories")
  public void testOrdinaryResourcesKeepIdealStateFactory(String configuredFactory,
      String expectedFactory) throws Exception {
    setupInstances(1);
    setupLiveInstances(1);
    setupStateModel();
    IdealState idealState = new IdealState(JOB);
    idealState.setStateModelDefRef("MasterSlave");
    idealState.setRebalanceMode(IdealState.RebalanceMode.SEMI_AUTO);
    idealState.setNumPartitions(1);
    idealState.setReplicas("1");
    idealState.setPreferenceList(PARTITION, Collections.singletonList(HOSTNAME_PREFIX + 0));
    if (configuredFactory != null) {
      idealState.setStateModelFactoryName(configuredFactory);
    }
    setSingleIdealState(idealState);
    ResourceConfig resourceConfig = new ResourceConfig(JOB);
    resourceConfig.putSimpleConfig("STATE_MODEL_FACTORY_NAME", LEGACY_FACTORY);
    accessor.setProperty(accessor.keyBuilder().resourceConfig(JOB), resourceConfig);
    readResources(new ResourceControllerDataProvider());
    CurrentStateOutput currentState = new CurrentStateOutput();
    currentState.setCurrentState(JOB, new Partition(PARTITION), HOSTNAME_PREFIX + 0, "OFFLINE");
    event.addAttribute(AttributeName.CURRENT_STATE.name(), currentState);
    BestPossibleStateOutput assignment = new BestPossibleStateOutput();
    assignment.setState(JOB, new Partition(PARTITION), HOSTNAME_PREFIX + 0, "DROPPED");
    event.addAttribute(AttributeName.BEST_POSSIBLE_STATE.name(), assignment);

    Message message = onlyMessage(generateMessages());
    Assert.assertEquals(message.getStateModelFactoryName(), expectedFactory);
    Assert.assertEquals(message.getStateModelDef(), "MasterSlave");
    Assert.assertEquals(message.getFromState(), "OFFLINE");
    Assert.assertEquals(message.getToState(), "DROPPED");
    Assert.assertEquals(resourceConfig.getSimpleConfig("STATE_MODEL_FACTORY_NAME"), LEGACY_FACTORY);
  }

  private void prepareTaskCluster(int participants) {
    setupInstances(participants);
    setupLiveInstances(participants);
    accessor.setProperty(accessor.keyBuilder().stateModelDef(TaskConstants.STATE_MODEL_NAME),
        BuiltInStateModelDefinitions.Task.getStateModelDefinition());
    ClusterConfig clusterConfig = new ClusterConfig(_clusterName);
    clusterConfig.stateTransitionCancelEnabled(true);
    setClusterConfig(clusterConfig);
  }

  private JobConfig writeJobConfig() {
    TaskConfig taskConfig = new TaskConfig("command", new HashMap<>(), "task", null);
    JobConfig jobConfig = new JobConfig.Builder().setWorkflow(WORKFLOW).setJobId(JOB)
        .addTaskConfigs(Collections.singletonList(taskConfig)).build();
    jobConfig.putSimpleConfig("STATE_MODEL_FACTORY_NAME", LEGACY_FACTORY);
    accessor.setProperty(accessor.keyBuilder().resourceConfig(JOB), jobConfig);
    return jobConfig;
  }

  private CurrentState writeTaskCurrentState(int participant, String state, String factoryName) {
    CurrentState currentState = new CurrentState(JOB);
    currentState.setSessionId(SESSION_PREFIX + participant);
    currentState.setStateModelDefRef(TaskConstants.STATE_MODEL_NAME);
    currentState.setStateModelFactoryName(factoryName);
    currentState.setState(PARTITION, state);
    accessor.setProperty(accessor.keyBuilder()
        .taskCurrentState(HOSTNAME_PREFIX + participant, SESSION_PREFIX + participant, JOB),
        currentState);
    return currentState;
  }

  private void readResources(BaseControllerDataProvider cache) throws Exception {
    event.addAttribute(AttributeName.helixmanager.name(), manager);
    event.addAttribute(AttributeName.ControllerDataProvider.name(), cache);
    cache.refresh(accessor);
    new ResourceComputationStage().process(event);
  }

  private List<Message> generateMessages() throws Exception {
    new MessageGenerationPhase().process(event);
    MessageOutput output = event.getAttribute(AttributeName.MESSAGES_ALL.name());
    return output.getMessages(JOB, new Partition(PARTITION));
  }

  private Message onlyMessage(List<Message> messages) {
    Assert.assertEquals(messages.size(), 1);
    return messages.get(0);
  }
}
