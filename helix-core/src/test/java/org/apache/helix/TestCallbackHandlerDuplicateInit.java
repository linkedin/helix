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

import java.lang.reflect.Field;
import java.util.List;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.helix.HelixConstants.ChangeType;
import org.apache.helix.api.listeners.IdealStateChangeListener;
import org.apache.helix.common.DedupEventBlockingQueue;
import org.apache.helix.integration.manager.ClusterSpectatorManager;
import org.apache.helix.integration.manager.ZkTestManager;
import org.apache.helix.manager.zk.CallbackEventExecutor;
import org.apache.helix.manager.zk.CallbackHandler;
import org.apache.helix.model.IdealState;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

/**
 * Regression test for CICP-52393.
 *
 * When Helix participant batch mode is enabled, {@link CallbackHandler#init()} is called twice
 * during a new ZK session (once when the handler is constructed and again by
 * {@code ZKHelixManager#initHandlers}). The duplicate init() used to call
 * {@code CallbackEventExecutor#reset()} before the out-of-order INIT event was rejected,
 * clearing the executor's dedup queue and cancelling its in-flight task. A participant callback
 * (e.g. an OFFLINE->STANDBY state-transition message) delivered in the narrow window between the
 * two init() calls was therefore silently dropped, the ZK watch was never re-armed, and the
 * message sat unread until an unrelated callback revived processing -- stalling the deployment.
 * This test proves a duplicate init() no longer discards pending batched callbacks.
 */
public class TestCallbackHandlerDuplicateInit extends ZkUnitTestBase {

  private static final String CLUSTER_NAME = TestHelper.getTestClassName();

  private ClusterSpectatorManager _spectator;
  private String _previousBatchModeProperty;

  @BeforeClass
  public void beforeClass() throws Exception {
    _previousBatchModeProperty = System.getProperty(SystemPropertyKeys.ASYNC_BATCH_MODE_ENABLED);
    // Enable participant batch mode (the configuration Venice runs with) for this cluster.
    System.setProperty(SystemPropertyKeys.ASYNC_BATCH_MODE_ENABLED, "true");

    TestHelper.setupCluster(CLUSTER_NAME, ZK_ADDR, 12918, // participant port
        "localhost", // participant name prefix
        "TestDB", // resource name prefix
        1, // resources
        4, // partitions per resource
        1, // number of nodes
        1, // replicas
        "MasterSlave", true); // do rebalance

    _spectator = new ClusterSpectatorManager(ZK_ADDR, CLUSTER_NAME, "spectator_0");
    _spectator.syncStart();
  }

  @AfterClass
  public void afterClass() {
    if (_spectator != null) {
      _spectator.syncStop();
    }
    if (_previousBatchModeProperty == null) {
      System.clearProperty(SystemPropertyKeys.ASYNC_BATCH_MODE_ENABLED);
    } else {
      System.setProperty(SystemPropertyKeys.ASYNC_BATCH_MODE_ENABLED, _previousBatchModeProperty);
    }
    deleteCluster(CLUSTER_NAME);
  }

  @Test
  public void testDuplicateInitPreservesPendingBatchedCallbacks() throws Exception {
    // A user listener on a stable IdealState path. After its one INIT callback the idle cluster
    // produces no further callbacks, so the executor's dedup queue stays under test control.
    final IdealStateChangeListener listener = new IdealStateChangeListener() {
      @Override
      public void onIdealStateChange(List<IdealState> idealState, NotificationContext context) {
      }
    };
    _spectator.addIdealStateChangeListener(listener);

    CallbackHandler handler = findHandler(_spectator, listener);
    Assert.assertNotNull(handler, "Expected a CallbackHandler for the registered listener");

    // Batch mode is on, so the handler owns a CallbackEventExecutor backed by a dedup queue.
    CallbackEventExecutor executor = getExecutor(handler);
    Assert.assertNotNull(executor,
        "Batch mode is enabled, so the handler must have a CallbackEventExecutor");
    DedupEventBlockingQueue<NotificationContext.Type, NotificationContext> queue =
        getQueue(executor);

    // The handler was already initialized once (by its constructor) and is now awaiting
    // CALLBACK/FINALIZE. A further init() is therefore the spurious duplicate-init path -- the
    // exact scenario ZKHelixManager#initHandlers triggers during handleNewSession.
    Assert.assertFalse(getExpectTypes(handler).contains(NotificationContext.Type.INIT),
        "Handler should already be initialized (awaiting CALLBACK/FINALIZE) before duplicate init");

    // Simulate a participant callback (e.g. a state-transition message) that arrived and was
    // queued for batched processing in the race window between the two init() calls.
    NotificationContext pending = new NotificationContext(_spectator);
    pending.setType(NotificationContext.Type.CALLBACK);
    pending.setChangeType(ChangeType.IDEAL_STATE);
    queue.put(NotificationContext.Type.CALLBACK, pending);
    Assert.assertEquals(queue.size(), 1, "Sanity: one pending callback should be queued");

    // The duplicate init() must be a no-op: it must NOT reset the executor or clear the queue.
    // Before the CICP-52393 fix this dropped the pending callback (queue size -> 0), leaving the
    // ZK watch un-rearmed and the state-transition message unprocessed.
    handler.init();

    Assert.assertEquals(queue.size(), 1,
        "Duplicate init() must not discard pending batched callbacks (CICP-52393)");
  }

  private static CallbackHandler findHandler(ZkTestManager manager, Object listener) {
    for (CallbackHandler handler : manager.getHandlers()) {
      if (handler.getListener() == listener) {
        return handler;
      }
    }
    return null;
  }

  private static CallbackEventExecutor getExecutor(CallbackHandler handler) throws Exception {
    Field field = CallbackHandler.class.getDeclaredField("_batchCallbackExecutorRef");
    field.setAccessible(true);
    AtomicReference<?> ref = (AtomicReference<?>) field.get(handler);
    return (CallbackEventExecutor) ref.get();
  }

  @SuppressWarnings("unchecked")
  private static DedupEventBlockingQueue<NotificationContext.Type, NotificationContext> getQueue(
      CallbackEventExecutor executor) throws Exception {
    Field field = CallbackEventExecutor.class.getDeclaredField("_callBackEventQueue");
    field.setAccessible(true);
    return (DedupEventBlockingQueue<NotificationContext.Type, NotificationContext>) field
        .get(executor);
  }

  @SuppressWarnings("unchecked")
  private static List<NotificationContext.Type> getExpectTypes(CallbackHandler handler)
      throws Exception {
    Field field = CallbackHandler.class.getDeclaredField("_expectTypes");
    field.setAccessible(true);
    return (List<NotificationContext.Type>) field.get(handler);
  }
}
