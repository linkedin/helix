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
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.helix.HelixManager;
import org.apache.helix.HelixManagerFactory;
import org.apache.helix.InstanceType;
import org.apache.helix.integration.manager.ClusterControllerManager;
import org.apache.helix.integration.manager.MockParticipantManager;
import org.apache.helix.task.JobConfig;
import org.apache.helix.task.JobContext;
import org.apache.helix.task.TaskConfig;
import org.apache.helix.task.TaskConstants;
import org.apache.helix.task.TaskDriver;
import org.apache.helix.task.TaskFactory;
import org.apache.helix.task.TaskPartitionState;
import org.apache.helix.task.TaskResult;
import org.apache.helix.task.TaskState;
import org.apache.helix.task.TaskStateModelFactory;
import org.apache.helix.task.TaskUtil;
import org.apache.helix.task.Workflow;
import org.apache.helix.tools.ClusterSetup;

/**
 * Standalone end-to-end driver for the task status summary against a REAL, already
 * running ZooKeeper (default localhost:2191). It spins up an actual Helix cluster in-process
 * (controller + participants running the Task state model), submits one job with a mix of
 * completing and failing tasks and a high FailureThreshold, waits for the job to reach COMPLETED,
 * and prints / verifies the aggregated task status summary stored in the JobContext.
 *
 * Run with the helix-core test classpath, e.g.:
 * <pre>
 *   java -cp "$(cat /tmp/helix-cp.txt):helix-core/target/classes:helix-core/target/test-classes" \
 *     org.apache.helix.integration.task.JobTaskSummaryDriver localhost:2191
 * </pre>
 * Exits 0 on success, 1 on failure.
 */
public class JobTaskSummaryDriver {
  private static final String CLUSTER_NAME = "TASK_STATUS_SUMMARY_CLUSTER";
  private static final int NUM_NODES = 3;
  private static final int START_PORT = 13900;
  private static final int NUM_TASKS = 6;
  private static final int FATAL_TASK = 1; // FATAL_FAILED -> TASK_ABORTED
  private static final int EXCEPTION_TASK = 3; // throws -> retried to exhaustion -> TASK_ERROR
  private static final int EXPECTED_FAILED = 2;
  private static final int EXPECTED_COMPLETED = NUM_TASKS - EXPECTED_FAILED;

  public static void main(String[] args) throws Exception {
    String zkAddr = args.length > 0 ? args[0] : "localhost:2191";
    System.out.println("=== Task status summary e2e driver against real ZK " + zkAddr + " ===");

    ClusterSetup setupTool = new ClusterSetup(zkAddr);
    MockParticipantManager[] participants = new MockParticipantManager[NUM_NODES];
    ClusterControllerManager controller = null;
    HelixManager manager = null;
    boolean ok = false;

    try {
      // 1. (Re)create the cluster and register participants.
      setupTool.addCluster(CLUSTER_NAME, true);
      for (int i = 0; i < NUM_NODES; i++) {
        setupTool.addInstanceToCluster(CLUSTER_NAME, instanceName(i));
      }

      // 2. Start participants running the Task state model backed by MockTask.
      for (int i = 0; i < NUM_NODES; i++) {
        participants[i] = new MockParticipantManager(zkAddr, CLUSTER_NAME, instanceName(i));
        Map<String, TaskFactory> taskFactoryReg = new HashMap<>();
        taskFactoryReg.put(MockTask.TASK_COMMAND, MockTask::new);
        participants[i].getStateMachineEngine().registerStateModelFactory(
            TaskConstants.STATE_MODEL_NAME,
            new TaskStateModelFactory(participants[i], taskFactoryReg));
        participants[i].syncStart();
      }

      // 3. Start the controller.
      controller = new ClusterControllerManager(zkAddr, CLUSTER_NAME, "controller_0");
      controller.syncStart();

      // 4. Admin manager + TaskDriver.
      manager = HelixManagerFactory.getZKHelixManager(CLUSTER_NAME, "Admin",
          InstanceType.ADMINISTRATOR, zkAddr);
      manager.connect();
      TaskDriver driver = new TaskDriver(manager);

      // 5. Build one job with NUM_TASKS tasks; some fail terminally. FailureThreshold is set high
      //    (a common operator workaround) so all tasks run and the job still ends COMPLETED.
      String jobResource = "dataValidationJob";
      JobConfig.Builder jobBuilder = new JobConfig.Builder().setCommand(MockTask.TASK_COMMAND)
          .setTimeoutPerTask(10000).setMaxAttemptsPerTask(2).setFailureThreshold(Integer.MAX_VALUE);

      List<TaskConfig> taskConfigs = new ArrayList<>();
      for (int j = 0; j < NUM_TASKS; j++) {
        TaskConfig.Builder cb = new TaskConfig.Builder().setTaskId("task_" + j);
        if (j == FATAL_TASK) {
          cb.addConfig(MockTask.TASK_RESULT_STATUS, TaskResult.Status.FATAL_FAILED.name());
        } else if (j == EXCEPTION_TASK) {
          cb.addConfig(MockTask.THROW_EXCEPTION, Boolean.TRUE.toString());
        }
        cb.setTargetPartition(String.valueOf(j));
        taskConfigs.add(cb.build());
      }
      jobBuilder.addTaskConfigs(taskConfigs);

      Workflow flow =
          WorkflowGenerator.generateSingleJobWorkflowBuilder(jobResource, jobBuilder).build();
      System.out.println("Submitting workflow '" + jobResource + "' with " + NUM_TASKS
          + " tasks (" + EXPECTED_FAILED + " designed to fail) ...");
      driver.start(flow);

      // 6. Wait for terminal COMPLETED state (job flag hides the failures).
      TaskState finalState = driver.pollForWorkflowState(jobResource, TaskState.COMPLETED);
      System.out.println("Workflow reached state: " + finalState);

      String namespacedJob = TaskUtil.getNamespacedJobName(jobResource);
      JobContext ctx = driver.getJobContext(namespacedJob);

      System.out.println("\nPer-partition states:");
      for (int pId : ctx.getPartitionSet()) {
        System.out.printf("  partition %d (%s) -> %s%n", pId, ctx.getTaskIdForPartition(pId),
            ctx.getPartitionState(pId));
      }

      String summary = ctx.getTaskStatusSummary();
      System.out.println("\n>>> JobContext TASK_STATUS_SUMMARY:\n    " + summary);

      ok = verify(finalState, ctx, summary);
      System.out.println("\n=== RESULT: " + (ok ? "PASS" : "FAIL") + " ===");
    } finally {
      if (manager != null && manager.isConnected()) {
        manager.disconnect();
      }
      if (controller != null && controller.isConnected()) {
        controller.syncStop();
      }
      for (MockParticipantManager p : participants) {
        if (p != null && p.isConnected()) {
          p.syncStop();
        }
      }
      try {
        setupTool.deleteCluster(CLUSTER_NAME);
      } catch (Exception e) {
        System.out.println("Cleanup: could not delete cluster: " + e.getMessage());
      }
    }
    System.exit(ok ? 0 : 1);
  }

  private static boolean verify(TaskState finalState, JobContext ctx, String summary) {
    boolean ok = true;
    if (finalState != TaskState.COMPLETED) {
      System.out.println("ASSERT FAIL: workflow state expected COMPLETED but was " + finalState);
      ok = false;
    }
    if (summary == null) {
      System.out.println("ASSERT FAIL: task status summary is null");
      return false;
    }
    int completed = 0;
    int failed = 0;
    for (int pId : ctx.getPartitionSet()) {
      TaskPartitionState s = ctx.getPartitionState(pId);
      if (s == TaskPartitionState.COMPLETED) {
        completed++;
      } else if (s == TaskPartitionState.TASK_ABORTED || s == TaskPartitionState.TASK_ERROR
          || s == TaskPartitionState.TIMED_OUT || s == TaskPartitionState.ERROR) {
        failed++;
      }
    }
    ok &= expect("ground-truth completed", completed, EXPECTED_COMPLETED);
    ok &= expect("ground-truth failed", failed, EXPECTED_FAILED);
    ok &= expect("summary contains completed count",
        summary.contains("\"completed\":" + EXPECTED_COMPLETED) ? 1 : 0, 1);
    ok &= expect("summary contains failed count",
        summary.contains("\"failed\":" + EXPECTED_FAILED) ? 1 : 0, 1);
    ok &= expect("summary contains total count",
        summary.contains("\"total\":" + NUM_TASKS) ? 1 : 0, 1);
    // The summary must always carry the timed-out and in-progress breakdown, even when zero.
    ok &= expect("summary contains timedOut count",
        summary.contains("\"timedOut\":") ? 1 : 0, 1);
    ok &= expect("summary contains inProgress count",
        summary.contains("\"inProgress\":") ? 1 : 0, 1);
    return ok;
  }

  private static boolean expect(String what, int actual, int expected) {
    boolean pass = actual == expected;
    if (!pass) {
      System.out.printf("ASSERT FAIL: %s expected %d but was %d%n", what, expected, actual);
    }
    return pass;
  }

  private static String instanceName(int i) {
    return "localhost_" + (START_PORT + i);
  }
}
