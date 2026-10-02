package org.apache.helix.controller;

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
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BooleanSupplier;

import org.apache.helix.HelixDataAccessor;
import org.apache.helix.HelixException;
import org.apache.helix.HelixManager;
import org.apache.helix.NotificationContext;
import org.apache.helix.PropertyKey;
import org.apache.helix.api.exceptions.HelixManagerNotConnectedException;
import org.apache.helix.controller.pipeline.AbstractBaseStage;
import org.apache.helix.controller.pipeline.Pipeline;
import org.apache.helix.controller.pipeline.PipelineRegistry;
import org.apache.helix.controller.stages.ClusterEvent;
import org.apache.helix.controller.stages.ClusterEventType;
import org.apache.helix.model.IdealState;
import org.testng.Assert;
import org.testng.annotations.Test;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.timeout;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * A controller pipeline event that fails because the ZooKeeper connection is lost must not be
 * silently dropped. A same-session reconnect refires no watch, so unless the controller reruns the
 * pipeline itself it stops rebalancing until an unrelated change happens to arrive.
 */
public class TestPipelineRetryAfterConnectionLoss {
  private static final String CLUSTER_NAME = "TestPipelineRetryAfterConnectionLoss";
  private static final String SESSION_ID = "session_0";
  private static final long WAIT_TIMEOUT_MS = 10000L;
  // Long enough for several connection polls (500ms initial, doubling) to have run.
  private static final long QUIET_PERIOD_MS = 2500L;
  private static final ClusterEventType RETRY_EVENT =
      GenericHelixController.CONNECTION_LOSS_RETRY_EVENT_TYPE;

  @Test
  public void testRetryEventRunsOnProductionRegistries() {
    // A retry event with no registered pipeline would run no stages and silently do nothing.
    Assert.assertFalse(GenericHelixController.createDefaultRegistry("DEFAULT")
        .getPipelinesForEvent(RETRY_EVENT).isEmpty());
    Assert.assertFalse(GenericHelixController.createTaskRegistry("TASK")
        .getPipelinesForEvent(RETRY_EVENT).isEmpty());
    Assert.assertFalse(GenericHelixController.createManagementModeRegistry("MANAGEMENT_MODE")
        .getPipelinesForEvent(RETRY_EVENT).isEmpty());
  }

  @Test
  public void testEventDroppedAtSessionCheckIsRerunAfterReconnect() throws Exception {
    AtomicBoolean connected = new AtomicBoolean(false);
    HelixManager manager = createManager(connected);
    RecordingStage resourceStage = new RecordingStage();
    RecordingStage taskStage = new RecordingStage();
    GenericHelixController controller = createController(resourceStage, taskStage);
    try {
      // Several events fail at the session check while disconnected.
      for (int i = 0; i < 3; i++) {
        notifyIdealStateChange(controller, manager);
      }
      verify(manager, timeout(WAIT_TIMEOUT_MS).atLeast(2)).getSessionId();

      Thread.sleep(QUIET_PERIOD_MS);
      Assert.assertTrue(resourceStage.events.isEmpty(),
          "No pipeline can run while disconnected, got " + resourceStage.events);

      connected.set(true);
      waitFor(() -> resourceStage.count(RETRY_EVENT) > 0
          && taskStage.count(RETRY_EVENT) > 0);

      // All dropped events collapse into one retry chain, so exactly one rerun per pipeline.
      Thread.sleep(QUIET_PERIOD_MS);
      Assert.assertEquals(resourceStage.count(RETRY_EVENT), 1);
      Assert.assertEquals(taskStage.count(RETRY_EVENT), 1);
      Assert.assertEquals(resourceStage.count(ClusterEventType.IdealStateChange), 0);
    } finally {
      controller.shutdown();
    }
  }

  @Test
  public void testPipelineFailingOnConnectionLossIsRerun() throws Exception {
    AtomicBoolean connected = new AtomicBoolean(true);
    HelixManager manager = createManager(connected);
    RecordingStage resourceStage = new RecordingStage();
    // The connection error surfaces wrapped, as it does from deep inside a pipeline stage.
    resourceStage.failFirst = new HelixException("Failed to generate message",
        new HelixManagerNotConnectedException("HelixManager is not connected"));
    RecordingStage taskStage = new RecordingStage();
    GenericHelixController controller = createController(resourceStage, taskStage);
    try {
      notifyIdealStateChange(controller, manager);
      waitFor(() -> resourceStage.count(RETRY_EVENT) > 0);
      Assert.assertEquals(resourceStage.count(ClusterEventType.IdealStateChange), 1);
    } finally {
      controller.shutdown();
    }
  }

  @Test
  public void testUnrelatedPipelineFailureIsNotRerun() throws Exception {
    AtomicBoolean connected = new AtomicBoolean(true);
    HelixManager manager = createManager(connected);
    RecordingStage resourceStage = new RecordingStage();
    resourceStage.failFirst = new IllegalStateException("not a connection problem");
    RecordingStage taskStage = new RecordingStage();
    GenericHelixController controller = createController(resourceStage, taskStage);
    try {
      notifyIdealStateChange(controller, manager);
      waitFor(() -> resourceStage.count(ClusterEventType.IdealStateChange) > 0);
      Thread.sleep(QUIET_PERIOD_MS);
      Assert.assertEquals(resourceStage.count(RETRY_EVENT), 0);
    } finally {
      controller.shutdown();
    }
  }

  @Test
  public void testLeadershipChangeCancelsPendingRerun() throws Exception {
    AtomicBoolean connected = new AtomicBoolean(false);
    HelixManager manager = createManager(connected);
    RecordingStage resourceStage = new RecordingStage();
    RecordingStage taskStage = new RecordingStage();
    GenericHelixController controller = createController(resourceStage, taskStage);
    try {
      notifyIdealStateChange(controller, manager);
      // Both the resource and the task queue have dropped the event and requested a rerun.
      waitFor(() -> controller.getConnectionLossRetryRequestCount() >= 2);

      // Leadership is relinquished while the retry is still waiting for the connection.
      NotificationContext finalize = new NotificationContext(manager);
      finalize.setType(NotificationContext.Type.FINALIZE);
      controller.onControllerChange(finalize);

      connected.set(true);
      Thread.sleep(QUIET_PERIOD_MS);
      Assert.assertEquals(resourceStage.count(RETRY_EVENT), 0);
      Assert.assertEquals(taskStage.count(RETRY_EVENT), 0);
    } finally {
      controller.shutdown();
    }
  }

  @Test
  public void testRequestAfterLeadershipChangeIsNotLost() throws Exception {
    AtomicBoolean connected = new AtomicBoolean(false);
    HelixManager manager = createManager(connected);
    RecordingStage resourceStage = new RecordingStage();
    RecordingStage taskStage = new RecordingStage();
    GenericHelixController controller = createController(resourceStage, taskStage);
    try {
      notifyIdealStateChange(controller, manager);
      waitFor(() -> controller.getConnectionLossRetryRequestCount() >= 2);

      NotificationContext finalize = new NotificationContext(manager);
      finalize.setType(NotificationContext.Type.FINALIZE);
      controller.onControllerChange(finalize);
      // Dropped after the leadership change, while the stale retry is still pending.
      notifyIdealStateChange(controller, manager);
      waitFor(() -> controller.getConnectionLossRetryRequestCount() >= 4);

      connected.set(true);
      waitFor(() -> resourceStage.count(RETRY_EVENT) > 0 && taskStage.count(RETRY_EVENT) > 0);
      Thread.sleep(QUIET_PERIOD_MS);
      Assert.assertEquals(resourceStage.count(RETRY_EVENT), 1);
      Assert.assertEquals(taskStage.count(RETRY_EVENT), 1);
    } finally {
      controller.shutdown();
    }
  }

  private static GenericHelixController createController(RecordingStage resourceStage,
      RecordingStage taskStage) {
    return new GenericHelixController(createRegistry(Pipeline.Type.DEFAULT, resourceStage),
        createRegistry(Pipeline.Type.TASK, taskStage));
  }

  private static PipelineRegistry createRegistry(Pipeline.Type type, RecordingStage stage) {
    Pipeline pipeline = new Pipeline(type.name());
    pipeline.addStage(stage);
    PipelineRegistry registry = new PipelineRegistry();
    registry.register(ClusterEventType.IdealStateChange, pipeline);
    registry.register(RETRY_EVENT, pipeline);
    return registry;
  }

  private static HelixManager createManager(AtomicBoolean connected) {
    HelixManager manager = mock(HelixManager.class);
    HelixDataAccessor accessor = mock(HelixDataAccessor.class);
    when(manager.getClusterName()).thenReturn(CLUSTER_NAME);
    when(manager.getInstanceName()).thenReturn("controller_0");
    when(manager.getHelixDataAccessor()).thenReturn(accessor);
    when(accessor.keyBuilder()).thenReturn(new PropertyKey.Builder(CLUSTER_NAME));
    when(accessor.getProperty(any(PropertyKey.class))).thenReturn(null);
    when(manager.isConnected()).thenAnswer(invocation -> connected.get());
    // Events carry the session they were queued under, as callbacks queued before the drop do.
    when(manager.getSessionIdIfLead()).thenReturn(Optional.of(SESSION_ID));
    when(manager.getSessionId()).thenAnswer(invocation -> {
      if (!connected.get()) {
        throw new HelixManagerNotConnectedException("HelixManager is not connected");
      }
      return SESSION_ID;
    });
    return manager;
  }

  private static void notifyIdealStateChange(GenericHelixController controller,
      HelixManager manager) {
    NotificationContext context = new NotificationContext(manager);
    context.setType(NotificationContext.Type.CALLBACK);
    controller.onIdealStateChange(Collections.singletonList(new IdealState("resource")), context);
  }

  private static void waitFor(BooleanSupplier condition) throws InterruptedException {
    long deadline = System.currentTimeMillis() + WAIT_TIMEOUT_MS;
    while (!condition.getAsBoolean()) {
      if (System.currentTimeMillis() > deadline) {
        Assert.fail("Condition not met within " + WAIT_TIMEOUT_MS + "ms");
      }
      Thread.sleep(50);
    }
  }

  private static class RecordingStage extends AbstractBaseStage {
    final List<ClusterEventType> events = new CopyOnWriteArrayList<>();
    final AtomicInteger runs = new AtomicInteger();
    volatile RuntimeException failFirst;

    @Override
    public void process(ClusterEvent event) {
      events.add(event.getEventType());
      if (runs.getAndIncrement() == 0 && failFirst != null) {
        throw failFirst;
      }
    }

    long count(ClusterEventType type) {
      return events.stream().filter(type::equals).count();
    }
  }
}
