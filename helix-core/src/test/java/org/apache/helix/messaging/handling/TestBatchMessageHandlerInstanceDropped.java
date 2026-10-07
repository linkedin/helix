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

import java.util.concurrent.ConcurrentHashMap;

import org.apache.helix.HelixDataAccessor;
import org.apache.helix.HelixManager;
import org.apache.helix.HelixProperty;
import org.apache.helix.NotificationContext;
import org.apache.helix.NotificationContext.MapKey;
import org.apache.helix.PropertyKey;
import org.apache.helix.model.CurrentState;
import org.apache.helix.model.Message;
import org.apache.helix.model.Message.MessageType;
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
 * Regression tests for {@link BatchMessageHandler#postHandleMessage()}, which is where ZK
 * current-state writes for *batched* sub-messages are actually persisted (deferred from
 * {@link HelixStateTransitionHandler#updateZKCurrentState()}, which only stages the delta into
 * a shared map for batch messages). Prior to this fix, this deferred write path had no
 * live-instance check at all, so it was a strictly larger race window than the non-batch path:
 * any instance drop (InstanceConfig + /INSTANCES/{instance} removed) occurring at any point
 * during the batch's execution -- not just in the tiny window right before the write -- would
 * still resurrect the dropped instance's znode subtree as an orphan node.
 */
public class TestBatchMessageHandlerInstanceDropped {
  private static final String CLUSTER_NAME = "TestCluster";
  private static final String INSTANCE_NAME = "localhost_12918";
  private static final String RESOURCE_NAME = "TestResource";
  private static final String PARTITION_NAME = "TestResource_0";

  private HelixManager _manager;
  private HelixDataAccessor _accessor;
  private PropertyKey.Builder _keyBuilder;
  private MessageHandlerFactory _msgHandlerFty;
  private TaskExecutor _executor;

  @BeforeMethod
  public void setUp() {
    _manager = mock(HelixManager.class);
    _accessor = mock(HelixDataAccessor.class);
    _keyBuilder = new PropertyKey.Builder(CLUSTER_NAME);
    _msgHandlerFty = mock(MessageHandlerFactory.class);
    _executor = mock(TaskExecutor.class);

    when(_manager.getHelixDataAccessor()).thenReturn(_accessor);
    when(_manager.getInstanceName()).thenReturn(INSTANCE_NAME);
    when(_accessor.keyBuilder()).thenReturn(_keyBuilder);
    when(_accessor.updateProperty(any(PropertyKey.class), any(HelixProperty.class)))
        .thenReturn(true);
  }

  @Test
  public void testSkipsBatchedCurrentStateWriteWhenInstanceConfigRemoved() {
    // Simulate ACM/cluster-management having cleanly dropped the instance at some point while
    // the batch's sub-messages were executing: InstanceConfig is gone by the time the merged
    // batch write is about to be flushed.
    when(_accessor.getPropertyStat(_keyBuilder.instanceConfig(INSTANCE_NAME))).thenReturn(null);

    postHandleBatchMessageWithPendingCurrentStateUpdate();

    verify(_accessor, never()).updateProperty(any(PropertyKey.class), any(HelixProperty.class));
  }

  @Test
  public void testWritesBatchedCurrentStateWhenInstanceStillRegistered() {
    // Instance is still a registered cluster member: InstanceConfig stat exists.
    when(_accessor.getPropertyStat(_keyBuilder.instanceConfig(INSTANCE_NAME)))
        .thenReturn(new HelixProperty.Stat(0, 0L, 0L, 0L));

    postHandleBatchMessageWithPendingCurrentStateUpdate();

    ArgumentCaptor<PropertyKey> keyCaptor = ArgumentCaptor.forClass(PropertyKey.class);
    verify(_accessor, times(1)).updateProperty(keyCaptor.capture(), any(HelixProperty.class));
    Assert.assertTrue(keyCaptor.getValue().getPath().contains(INSTANCE_NAME));
  }

  /**
   * Builds a {@link BatchMessageHandler} with no sub-partitions (so the constructor creates no
   * real sub-message handlers), stages a single pending current-state update directly into the
   * shared {@code CURRENT_STATE_UPDATE} map the same way
   * {@link HelixStateTransitionHandler#updateZKCurrentState()} does for a batch sub-message, and
   * then invokes {@link BatchMessageHandler#postHandleMessage()} -- exercising only the deferred
   * ZK-flush path under test.
   */
  private void postHandleBatchMessageWithPendingCurrentStateUpdate() {
    Message batchMessage = new Message(MessageType.STATE_TRANSITION, "batchMsg1");
    batchMessage.setResourceName(RESOURCE_NAME);
    batchMessage.setTgtName(INSTANCE_NAME);
    batchMessage.setBatchMessageMode(false);

    NotificationContext context = new NotificationContext(_manager);

    PropertyKey currentStateKey =
        _keyBuilder.currentState(INSTANCE_NAME, "session_1", RESOURCE_NAME);
    CurrentState currentStateDelta = new CurrentState(RESOURCE_NAME);
    currentStateDelta.setState(PARTITION_NAME, "OFFLINE");

    ConcurrentHashMap<String, CurrentStateUpdate> csUpdateMap = new ConcurrentHashMap<>();
    csUpdateMap.put(currentStateKey.getPath(),
        new CurrentStateUpdate(currentStateKey, currentStateDelta));
    context.add(MapKey.CURRENT_STATE_UPDATE.toString(), csUpdateMap);

    BatchMessageHandler handler =
        new BatchMessageHandler(batchMessage, context, _msgHandlerFty, null, _executor);
    handler.postHandleMessage();
  }
}
