package org.apache.helix.task;

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

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;

import org.apache.helix.HelixDataAccessor;
import org.apache.helix.PropertyKey.Builder;
import org.apache.helix.mock.MockManager;
import org.apache.helix.model.CurrentState;
import org.apache.helix.model.InstanceConfig;
import org.testng.Assert;
import org.testng.annotations.Test;

/**
 * Regression coverage for the instance-registration guard in
 * {@link TaskRunner#setZKCurrentState}. A bulk/long-running task (e.g. DataExpiry,
 * sync-colo, MV recovery) can still be executing -- and therefore still issuing periodic or
 * completion current-state writes -- after its instance has already been cleanly dropped from
 * the cluster (InstanceConfig + /INSTANCES/{instance} removed). Without the guard, that write
 * would silently resurrect the dropped instance's ZK node as an orphan TaskCurrentState.
 */
public class TestTaskRunner {

  private static final String RESOURCE_NAME = "TestJob";
  private static final String PARTITION_NAME = "TestJob_0";

  private TaskRunner createTaskRunner(MockManager manager, TaskResult taskResult) {
    ScheduledExecutorService executorService = Executors.newSingleThreadScheduledExecutor();
    TaskStateModel stateModel = new TaskStateModel(manager, new HashMap<String, TaskFactory>(),
        executorService, executorService);
    Task task = new Task() {
      @Override
      public TaskResult run() {
        return taskResult;
      }

      @Override
      public void cancel() {
      }
    };
    TaskRunner taskRunner =
        new TaskRunner(task, RESOURCE_NAME, PARTITION_NAME, manager.getInstanceName(), manager,
            manager.getSessionId(), stateModel);
    executorService.shutdown();
    return taskRunner;
  }

  @Test
  public void testWritesCurrentStateWhenInstanceStillRegistered() {
    MockManager manager = new MockManager("testCluster");
    HelixDataAccessor accessor = manager.getHelixDataAccessor();
    Builder keyBuilder = accessor.keyBuilder();
    accessor.setProperty(keyBuilder.instanceConfig(manager.getInstanceName()),
        new InstanceConfig(manager.getInstanceName()));

    TaskRunner taskRunner =
        createTaskRunner(manager, new TaskResult(TaskResult.Status.COMPLETED, "done"));
    taskRunner.run();

    CurrentState currentState = accessor.getProperty(
        keyBuilder.taskCurrentState(manager.getInstanceName(), manager.getSessionId(),
            RESOURCE_NAME));
    Assert.assertNotNull(currentState,
        "Expected TaskRunner to persist current state to ZK when the instance is still a "
            + "registered cluster member.");
    Assert.assertEquals(currentState.getState(PARTITION_NAME),
        TaskPartitionState.COMPLETED.name());
  }

  @Test
  public void testSkipsCurrentStateWriteWhenInstanceDropped() {
    MockManager manager = new MockManager("testCluster");
    HelixDataAccessor accessor = manager.getHelixDataAccessor();
    Builder keyBuilder = accessor.keyBuilder();
    // No InstanceConfig is provisioned: simulates a clean ACM drop (InstanceConfig +
    // /INSTANCES/{instance} removed) racing with the task's periodic/completion checkpoint
    // write while the task thread is still alive and running.

    TaskRunner taskRunner =
        createTaskRunner(manager, new TaskResult(TaskResult.Status.COMPLETED, "done"));
    taskRunner.run();

    CurrentState currentState = accessor.getProperty(
        keyBuilder.taskCurrentState(manager.getInstanceName(), manager.getSessionId(),
            RESOURCE_NAME));
    Assert.assertNull(currentState,
        "TaskRunner must not write (and thereby resurrect) current state ZK nodes for an "
            + "instance that is no longer a registered cluster member.");
  }
}
