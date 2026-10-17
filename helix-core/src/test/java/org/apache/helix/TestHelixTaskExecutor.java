package org.apache.helix;

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

import org.apache.helix.PropertyKey.Builder;
import org.apache.helix.messaging.handling.AsyncCallbackService;
import org.apache.helix.messaging.handling.HelixStateTransitionHandler;
import org.apache.helix.messaging.handling.HelixTask;
import org.apache.helix.mock.MockManager;
import org.apache.helix.mock.participant.MockHelixTaskExecutor;
import org.apache.helix.mock.statemodel.MockMasterSlaveStateModel;
import org.apache.helix.model.CurrentState;
import org.apache.helix.model.InstanceConfig;
import org.apache.helix.model.Message;
import org.apache.helix.model.Message.MessageType;
import org.apache.helix.model.StateModelDefinition;
import org.apache.helix.participant.statemachine.StateModelFactory;
import org.apache.helix.tools.StateModelConfigGenerator;
import org.testng.AssertJUnit;
import org.testng.annotations.Test;

public class TestHelixTaskExecutor {

  @Test()
  public void testCMTaskExecutor() throws Exception {
    System.out.println("START TestCMTaskExecutor");
    String msgId = "TestMessageId";
    Message message = new Message(MessageType.TASK_REPLY, msgId);

    message.setMsgId(msgId);
    message.setSrcName("cm-instance-0");
    message.setTgtName("cm-instance-1");
    message.setFromState("OFFLINE");
    message.setToState("SLAVE");
    message.setPartitionName("TestDB_0");
    message.setResourceName("TestDB");
    message.setStateModelDef("MasterSlave");

    MockManager manager = new MockManager("clusterName");
    // postHandleMessage() skips the ZK current-state write entirely if the message's target
    // session id doesn't match the manager's current session (treated as a stale message from
    // an expired session), so the message must carry the manager's actual (randomly-generated)
    // session id for the write path below to be exercised.
    message.setTgtSessionId(manager.getSessionId());
    HelixDataAccessor accessor = manager.getHelixDataAccessor();
    StateModelDefinition stateModelDef =
        new StateModelDefinition(StateModelConfigGenerator.generateConfigForMasterSlave());
    Builder keyBuilder = accessor.keyBuilder();
    accessor.setProperty(keyBuilder.stateModelDef("MasterSlave"), stateModelDef);
    // A participant cannot receive and process a STATE_TRANSITION message before it has joined
    // the cluster (which always provisions an InstanceConfig first), so the target instance
    // must be a registered cluster member for the ZK current-state write below to proceed. Note
    // the ZK write path keys off manager.getInstanceName(), not the message's target name.
    accessor.setProperty(keyBuilder.instanceConfig(manager.getInstanceName()),
        new InstanceConfig(manager.getInstanceName()));

    MockHelixTaskExecutor executor = new MockHelixTaskExecutor();
    MockMasterSlaveStateModel stateModel = new MockMasterSlaveStateModel();
    executor.registerMessageHandlerFactory(MessageType.TASK_REPLY.name(),
        new AsyncCallbackService());

    NotificationContext context = new NotificationContext(manager);
    CurrentState currentStateDelta = new CurrentState("TestDB");
    currentStateDelta.setState("TestDB_0", "OFFLINE");

    StateModelFactory<MockMasterSlaveStateModel> stateModelFactory = new StateModelFactory<MockMasterSlaveStateModel>() {

      @Override
      public MockMasterSlaveStateModel createNewStateModel(String resource, String partitionName) {
        // TODO Auto-generated method stub
        return new MockMasterSlaveStateModel();
      }

    };
    HelixStateTransitionHandler handler =
        new HelixStateTransitionHandler(stateModelFactory, stateModel, message, context,
            currentStateDelta);

    HelixTask task = new HelixTask(message, context, handler, executor);
    executor.scheduleTask(task);
    for (int i = 0; i < 10; i++) {
      if (!executor.isDone(task.getTaskId())) {
        Thread.sleep(500);
      }
    }
    AssertJUnit.assertTrue(stateModel.stateModelInvoked);
    // Guard against the ZK current-state write silently being skipped (e.g. by the
    // instance-registration check in HelixStateTransitionHandler treating the manager's instance
    // as not a registered cluster member): confirm the write actually reached ZK.
    AssertJUnit.assertNotNull(accessor.getProperty(
        keyBuilder.currentState(manager.getInstanceName(), manager.getSessionId(), "TestDB")));
    System.out.println("END TestCMTaskExecutor");
  }

}
