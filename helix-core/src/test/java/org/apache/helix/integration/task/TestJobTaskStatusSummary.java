package org.apache.helix.integration.task;

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

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.helix.TestHelper;
import org.apache.helix.task.JobConfig;
import org.apache.helix.task.JobContext;
import org.apache.helix.task.TaskConfig;
import org.apache.helix.task.TaskPartitionState;
import org.apache.helix.task.TaskResult;
import org.apache.helix.task.TaskState;
import org.apache.helix.task.TaskUtil;
import org.apache.helix.task.Workflow;
import org.testng.Assert;
import org.testng.annotations.Test;

/**
 * Verifies the aggregated per-task status summary that Helix writes into the JobContext when a job
 * reaches a terminal state. The scenario mirrors a common validation setup: one job per table, one
 * task per partition, and a high FailureThreshold so every task runs even if some fail. In that
 * setup the job's own status flag is COMPLETED, which previously masked partition level failures.
 * The summary must surface those failures.
 */
public class TestJobTaskStatusSummary extends TaskTestBase {

  private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

  @Test
  public void testSummaryReflectsPartialFailureOnCompletedJob() throws Exception {
    int numTasks = 6;
    // Indices of the tasks that should fail terminally.
    final int fatalTask = 1; // FATAL_FAILED -> given up immediately -> TASK_ABORTED
    final int exceptionTask = 3; // throws -> retried to exhaustion -> TASK_ERROR
    final int expectedFailed = 2;
    final int expectedCompleted = numTasks - expectedFailed;

    String jobResource = TestHelper.getTestMethodName();
    JobConfig.Builder jobBuilder = new JobConfig.Builder();
    // FailureThreshold high (a common operator workaround) so all tasks run and the job still
    // ends COMPLETED.
    jobBuilder.setCommand(MockTask.TASK_COMMAND).setTimeoutPerTask(10000).setMaxAttemptsPerTask(2)
        .setFailureThreshold(Integer.MAX_VALUE);

    List<TaskConfig> taskConfigs = new ArrayList<>();
    for (int j = 0; j < numTasks; j++) {
      TaskConfig.Builder configBuilder = new TaskConfig.Builder().setTaskId("task_" + j);
      if (j == fatalTask) {
        configBuilder.addConfig(MockTask.TASK_RESULT_STATUS, TaskResult.Status.FATAL_FAILED.name());
      } else if (j == exceptionTask) {
        configBuilder.addConfig(MockTask.THROW_EXCEPTION, Boolean.TRUE.toString());
      }
      configBuilder.setTargetPartition(String.valueOf(j));
      taskConfigs.add(configBuilder.build());
    }
    jobBuilder.addTaskConfigs(taskConfigs);

    Workflow flow =
        WorkflowGenerator.generateSingleJobWorkflowBuilder(jobResource, jobBuilder).build();
    _driver.start(flow);

    // The job completes even though tasks failed, because FailureThreshold is high.
    _driver.pollForWorkflowState(jobResource, TaskState.COMPLETED);

    String namespacedJob = TaskUtil.getNamespacedJobName(jobResource);
    Assert.assertEquals(_driver.getWorkflowContext(jobResource).getJobState(namespacedJob),
        TaskState.COMPLETED, "Job status flag should be COMPLETED (this is what masked failures).");

    JobContext ctx = _driver.getJobContext(namespacedJob);

    // Cross-check the ground truth directly from per-partition states.
    int actualCompleted = 0;
    int actualFailed = 0;
    for (int pId : ctx.getPartitionSet()) {
      TaskPartitionState state = ctx.getPartitionState(pId);
      if (state == TaskPartitionState.COMPLETED) {
        actualCompleted++;
      } else if (state == TaskPartitionState.TASK_ABORTED || state == TaskPartitionState.TASK_ERROR
          || state == TaskPartitionState.TIMED_OUT || state == TaskPartitionState.ERROR) {
        actualFailed++;
      }
    }
    Assert.assertEquals(actualCompleted, expectedCompleted);
    Assert.assertEquals(actualFailed, expectedFailed);

    // Now the crux: the summary must be present and must match the ground truth.
    String summaryJson = ctx.getTaskStatusSummary();
    Assert.assertNotNull(summaryJson, "Task status summary should be populated on a terminal job.");
    JsonNode summary = OBJECT_MAPPER.readTree(summaryJson);

    Assert.assertEquals(summary.get("total").asInt(), numTasks);
    Assert.assertEquals(summary.get("completed").asInt(), expectedCompleted);
    Assert.assertEquals(summary.get("failed").asInt(), expectedFailed);
    Assert.assertEquals(summary.get("other").asInt(), 0);
    Assert.assertEquals(summary.get("failedTasks").size(), expectedFailed);

    JsonNode byState = summary.get("byState");
    Assert.assertEquals(byState.get(TaskPartitionState.COMPLETED.name()).asInt(), expectedCompleted);
    Assert.assertEquals(byState.get(TaskPartitionState.TASK_ABORTED.name()).asInt(), 1);
    Assert.assertEquals(byState.get(TaskPartitionState.TASK_ERROR.name()).asInt(), 1);

    // The failed partition ids reported in the summary must actually be failed partitions.
    for (JsonNode failedPidNode : summary.get("failedTasks")) {
      TaskPartitionState state = ctx.getPartitionState(failedPidNode.asInt());
      Assert.assertTrue(state != TaskPartitionState.COMPLETED,
          "Partition " + failedPidNode.asInt() + " reported as failed but state is " + state);
    }
  }
}
