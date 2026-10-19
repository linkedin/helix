package org.apache.helix.messaging.handling;

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
import java.util.List;

import org.apache.helix.HelixDataAccessor;
import org.apache.helix.HelixManager;
import org.apache.helix.HelixProperty;
import org.apache.helix.NotificationContext;
import org.apache.helix.PropertyKey;
import org.apache.helix.model.CurrentState;
import org.apache.helix.model.Message;
import org.apache.helix.model.Message.MessageType;
import org.apache.helix.participant.statemachine.StateModel;
import org.apache.helix.participant.statemachine.StateModelFactory;
import org.apache.helix.participant.statemachine.StateTransitionError;
import org.apache.helix.zookeeper.zkclient.exception.ZkTimeoutException;
import org.mockito.ArgumentCaptor;
import org.testng.Assert;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * Regression tests guarding against a race where a bulk/long-running task's final current
 * state write races with a clean instance drop (InstanceConfig + /INSTANCES/{instance} removed,
 * e.g. by a cluster-management tool during a hardware swap). Without the live-instance check in
 * {@link HelixStateTransitionHandler}, the ZK write recreates the dropped instance's znode
 * subtree as an orphan (no InstanceConfig, no live instance).
 */
public class TestHelixStateTransitionHandlerInstanceDropped {
  private static final String CLUSTER_NAME = "TestCluster";
  private static final String INSTANCE_NAME = "localhost_12918";
  private static final String RESOURCE_NAME = "TestResource";
  private static final String PARTITION_NAME = "TestResource_0";
  private static final String SESSION_ID = "session_1";

  private HelixManager _manager;
  private HelixDataAccessor _accessor;
  private PropertyKey.Builder _keyBuilder;

  @BeforeMethod
  public void setUp() {
    _manager = mock(HelixManager.class);
    _accessor = mock(HelixDataAccessor.class);
    _keyBuilder = new PropertyKey.Builder(CLUSTER_NAME);

    when(_manager.getHelixDataAccessor()).thenReturn(_accessor);
    when(_manager.getInstanceName()).thenReturn(INSTANCE_NAME);
    when(_manager.getSessionId()).thenReturn(SESSION_ID);
    when(_accessor.keyBuilder()).thenReturn(_keyBuilder);
    // CurrentState/TaskCurrentState writes always "succeed" unless explicitly stubbed otherwise;
    // the assertions below only care whether updateProperty was invoked at all.
    when(_accessor.updateProperty(any(PropertyKey.class), any(HelixProperty.class)))
        .thenReturn(true);
  }

  @Test
  public void testSkipsCurrentStateWriteWhenInstanceConfigRemoved() throws Exception {
    // Simulate ACM/cluster-management having cleanly dropped the instance: InstanceConfig is
    // gone, so getPropertyStat() returns null for it.
    when(_accessor.getPropertyStat(_keyBuilder.instanceConfig(INSTANCE_NAME))).thenReturn(null);

    runStateTransitionAndCompleteSuccessfully();

    // Only a single registration read should happen for the direct (non-batch) write path --
    // immediately before the real ZK write -- not an earlier, redundant one as well.
    verify(_accessor, times(1)).getPropertyStat(_keyBuilder.instanceConfig(INSTANCE_NAME));
    verify(_accessor, never()).updateProperty(any(PropertyKey.class), any(HelixProperty.class));
  }

  @Test
  public void testWritesCurrentStateWhenInstanceStillRegistered() throws Exception {
    // Instance is still a registered cluster member: InstanceConfig stat exists.
    when(_accessor.getPropertyStat(_keyBuilder.instanceConfig(INSTANCE_NAME)))
        .thenReturn(new HelixProperty.Stat(0, 0L, 0L, 0L));

    runStateTransitionAndCompleteSuccessfully();

    verify(_accessor, times(1)).getPropertyStat(_keyBuilder.instanceConfig(INSTANCE_NAME));
    ArgumentCaptor<PropertyKey> keyCaptor = ArgumentCaptor.forClass(PropertyKey.class);
    verify(_accessor, times(1)).updateProperty(keyCaptor.capture(), any(HelixProperty.class));
    Assert.assertEquals(keyCaptor.getValue().getPath(),
        _keyBuilder.currentState(INSTANCE_NAME, SESSION_ID, RESOURCE_NAME).getPath());
  }

  /**
   * Drives a handler through a successful OFFLINE->ONLINE-style transition completion, mirroring
   * what {@link HelixTaskExecutor} does after a state transition method returns successfully.
   */
  private void runStateTransitionAndCompleteSuccessfully() throws Exception {
    HelixStateTransitionHandler handler = newHandler(new StateModel() {
    });

    HelixTaskResult taskResult = new HelixTaskResult();
    taskResult.setSuccess(true);
    taskResult.setInfo("");
    taskResult.setCompleteTime(System.currentTimeMillis());
    // _notificationContext is protected on MessageHandler; accessible here since this test is in
    // the same package.
    handler._notificationContext
        .add(NotificationContext.MapKey.HELIX_TASK_RESULT.toString(), taskResult);

    // Package-private; same-package test can invoke it directly, mirroring what
    // HelixStateTransitionHandler#handleMessage() does in production.
    handler.postHandleMessage();
  }

  /**
   * Regression tests for the same orphan-resurrection race, but on the separate
   * {@link HelixStateTransitionHandler#onError} write path (ERROR current state), which does not
   * go through {@code updateZKCurrentState()} and therefore needs its own, independent guard.
   */
  @Test
  public void testOnErrorSkipsCurrentStateWriteWhenInstanceConfigRemoved() {
    when(_accessor.getPropertyStat(_keyBuilder.instanceConfig(INSTANCE_NAME))).thenReturn(null);

    newHandler(new StateModel() {
    }).onError(new Exception("transition failed"), MessageHandler.ErrorCode.ERROR,
        MessageHandler.ErrorType.FRAMEWORK);

    verify(_accessor, never()).updateProperty(any(PropertyKey.class), any(HelixProperty.class));
  }

  @Test
  public void testOnErrorWritesCurrentStateWhenInstanceStillRegistered() {
    when(_accessor.getPropertyStat(_keyBuilder.instanceConfig(INSTANCE_NAME)))
        .thenReturn(new HelixProperty.Stat(0, 0L, 0L, 0L));

    newHandler(new StateModel() {
    }).onError(new Exception("transition failed"), MessageHandler.ErrorCode.ERROR,
        MessageHandler.ErrorType.FRAMEWORK);

    ArgumentCaptor<PropertyKey> keyCaptor = ArgumentCaptor.forClass(PropertyKey.class);
    verify(_accessor, times(1)).updateProperty(keyCaptor.capture(), any(HelixProperty.class));
    Assert.assertEquals(keyCaptor.getValue().getPath(),
        _keyBuilder.currentState(INSTANCE_NAME, SESSION_ID, RESOURCE_NAME).getPath());
  }

  @Test
  public void testOnErrorStillRollsBackLocalStateModelWhenInstanceConfigRemoved() {
    // Even when the ZK write is skipped, local state-model rollback must still happen: the new
    // guard must not short-circuit the surrounding try/finally in onError().
    when(_accessor.getPropertyStat(_keyBuilder.instanceConfig(INSTANCE_NAME))).thenReturn(null);

    final boolean[] rolledBack = {false};
    StateModel stateModel = new StateModel() {
      @Override
      public void rollbackOnError(Message message, NotificationContext context,
          StateTransitionError error) {
        rolledBack[0] = true;
      }
    };

    newHandler(stateModel).onError(new Exception("transition failed"),
        MessageHandler.ErrorCode.ERROR, MessageHandler.ErrorType.FRAMEWORK);

    verify(_accessor, never()).updateProperty(any(PropertyKey.class), any(HelixProperty.class));
    Assert.assertTrue(rolledBack[0], "rollbackOnError() must still run when the ZK write is skipped");
  }

  @Test
  public void testOnErrorStillRollsBackLocalStateModelWhenRegistrationCheckThrows() {
    // Simulate a ZK read failure (e.g. a timeout) while checking instance registration, as
    // opposed to a clean "instance not registered" result. isInstanceRegistered() only swallows
    // ZkNoNodeException internally, so anything else propagates unless onError() itself catches
    // it. onError() must not let this exception escape: doing so would (in production) abort
    // BatchMessageHandler#onError()'s loop over sibling sub-handlers -- see the next test.
    when(_accessor.getPropertyStat(_keyBuilder.instanceConfig(INSTANCE_NAME)))
        .thenThrow(new ZkTimeoutException("timed out reading InstanceConfig stat"));

    final boolean[] rolledBack = {false};
    StateModel stateModel = new StateModel() {
      @Override
      public void rollbackOnError(Message message, NotificationContext context,
          StateTransitionError error) {
        rolledBack[0] = true;
      }
    };

    // Must not throw: the exception from the registration check has to be caught inside
    // onError(), not left to propagate past it.
    newHandler(stateModel).onError(new Exception("transition failed"),
        MessageHandler.ErrorCode.ERROR, MessageHandler.ErrorType.FRAMEWORK);

    verify(_accessor, never()).updateProperty(any(PropertyKey.class), any(HelixProperty.class));
    Assert.assertTrue(rolledBack[0],
        "rollbackOnError() must still run when the registration check throws");
  }

  @Test
  public void testOnErrorRollsBackAllSubHandlersWhenOneThrowsDuringRegistrationCheck() {
    // Mirrors BatchMessageHandler#onError(), which loops over one MessageHandler per batched
    // sub-message/partition and calls onError() on each in turn. Before this fix, if an earlier
    // sub-handler's registration check threw (e.g. a ZK read timeout), the uncaught exception
    // would propagate out of that loop iteration, and every later sub-handler in the loop would
    // receive neither its ERROR current-state update nor its rollbackOnError() call.
    when(_accessor.getPropertyStat(_keyBuilder.instanceConfig(INSTANCE_NAME)))
        .thenThrow(new ZkTimeoutException("timed out reading InstanceConfig stat"));

    int numSubHandlers = 3;
    final boolean[] rolledBack = new boolean[numSubHandlers];
    List<MessageHandler> subHandlers = new ArrayList<>();
    for (int i = 0; i < numSubHandlers; i++) {
      final int idx = i;
      StateModel stateModel = new StateModel() {
        @Override
        public void rollbackOnError(Message message, NotificationContext context,
            StateTransitionError error) {
          rolledBack[idx] = true;
        }
      };
      subHandlers.add(newHandler(stateModel));
    }

    // Same loop as BatchMessageHandler#onError(): no try/catch around each call, so a single
    // throwing sub-handler would previously have stopped the loop entirely.
    for (MessageHandler handler : subHandlers) {
      handler.onError(new Exception("transition failed"), MessageHandler.ErrorCode.ERROR,
          MessageHandler.ErrorType.FRAMEWORK);
    }

    for (int i = 0; i < numSubHandlers; i++) {
      Assert.assertTrue(rolledBack[i],
          "sub-handler " + i + "'s rollbackOnError() must still run even though an earlier "
              + "sub-handler's registration check threw");
    }
  }

  private HelixStateTransitionHandler newHandler(StateModel stateModel) {
    Message message = new Message(MessageType.STATE_TRANSITION, "msg1");
    message.setPartitionName(PARTITION_NAME);
    message.setResourceName(RESOURCE_NAME);
    message.setTgtSessionId(SESSION_ID);
    message.setTgtName(INSTANCE_NAME);
    message.setSrcName("controller");
    message.setFromState("OFFLINE");
    message.setToState("ONLINE");

    NotificationContext context = new NotificationContext(_manager);

    StateModelFactory<StateModel> stateModelFactory = new StateModelFactory<StateModel>() {
      @Override
      public StateModel createNewStateModel(String resourceName, String partitionName) {
        return stateModel;
      }
    };

    CurrentState currentStateDelta = new CurrentState(RESOURCE_NAME);
    currentStateDelta.setState(PARTITION_NAME, "OFFLINE");

    return new HelixStateTransitionHandler(stateModelFactory, stateModel, message, context,
        currentStateDelta);
  }
}
