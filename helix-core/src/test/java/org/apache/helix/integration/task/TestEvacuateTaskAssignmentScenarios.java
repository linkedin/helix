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
import java.util.Collections;
import java.util.List;
import java.util.Map;

import com.google.common.collect.ImmutableMap;
import org.apache.helix.TestHelper;
import org.apache.helix.constants.InstanceConstants.InstanceOperation;
import org.apache.helix.integration.manager.ClusterControllerManager;
import org.apache.helix.integration.manager.MockParticipantManager;
import org.apache.helix.mock.participant.SleepTransition;
import org.apache.helix.model.ExternalView;
import org.apache.helix.model.InstanceConfig;
import org.apache.helix.task.JobConfig;
import org.apache.helix.task.JobContext;
import org.apache.helix.task.TaskConfig;
import org.apache.helix.task.TaskPartitionState;
import org.apache.helix.task.TaskState;
import org.apache.helix.task.TaskUtil;
import org.apache.helix.task.Workflow;
import org.testng.Assert;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

/**
 * Real-ZK, end-to-end (no mocks) scenario coverage for CICP-34004: targeted task jobs must still be
 * scheduled when the target partition's MASTER sits on a live EVACUATE host, and the task throttle
 * path must not NPE while accounting for that EVACUATE host.
 *
 * <p>Structure is a hybrid so new scenarios are cheap to add:
 * <ul>
 *   <li><b>{@code @DataProvider} matrix</b> ({@link #scheduleOnOperatedHostScenarios}) for the large
 *       family of same-shape scenarios: pin the MASTER on a host, apply an instance operation to that
 *       host, restart the controller so the task data provider is rebuilt fresh, start a targeted
 *       MASTER job, and assert the outcome. Adding such a scenario = adding one row.</li>
 *   <li><b>Dedicated {@code @Test} methods</b> for odd-shaped scenarios that need extra steps a single
 *       matrix row cannot express: throttle-capacity accounting on a lone EVACUATE candidate, and a
 *       targeted job that runs on the EVACUATE MASTER and still completes after the evacuation resolves.</li>
 * </ul>
 *
 * <p>The controller restart in every EVACUATE scenario is essential, not cosmetic: the bug only
 * reproduces when the data provider is built fresh while the node is already EVACUATE (a restart /
 * leadership change during the evacuation - the real prod trigger). Without it a stale, pre-EVACUATE
 * seed hides the NPE and the scenario passes even on the buggy code. Verified: every EVACUATE scenario
 * here fails on the pre-fix code and passes with the fix, while the ENABLE baseline passes on both.
 *
 * <p>Cluster shape is deliberately small and controllable (1 partition, 1 replica) so the single
 * MASTER can be pinned on a chosen host by blocking the replacement's state transition, which is the
 * only reliable way to reproduce the "long swap-out window" the bug needs. See also
 * {@link TestEvacuateTargetJobAssignment} (the original single-case reproduction) and the unit-level
 * {@code TestWorkflowControllerDataProviderEvacuate}.
 */
public class TestEvacuateTaskAssignmentScenarios extends TaskTestBase {
  // Long enough to outlast the test so a blocked replacement never bootstraps.
  private static final long BLOCKED_TRANSITION_MS = 300_000L;
  // Time for the controller to react to an instance-operation change before we assert on state.
  private static final long SETTLE_MS = 5_000L;
  private static final long WORKFLOW_TIMEOUT_MS = 60_000L;
  // Short task for scenarios that only need the job to complete.
  private static final long QUICK_TASK_MS = 1_000L;
  // Longer task so the migration scenario can observe the task RUNNING during the hand-off.
  private static final long RUNNING_TASK_MS = 5_000L;
  private static final String MASTER = "MASTER";

  private final List<String> _createdWorkflows = new ArrayList<>();

  @BeforeClass
  public void beforeClass() throws Exception {
    _numNodes = 3;
    _numPartitions = 1;
    _numReplicas = 1;
    _numDbs = 1;
    super.beforeClass();
  }

  /**
   * Same-shape scenarios: apply {@code op} to the host holding the target MASTER, keep the MASTER
   * pinned there by blocking the replacement, restart the controller so the task data provider (and
   * its per-instance active-task-count map) is rebuilt fresh while the node is already under {@code op},
   * then require a targeted MASTER job to run on that host.
   *
   * <p>The controller restart is essential, not incidental: {@code resetActiveTaskCount} only ever adds
   * to {@code _participantActiveTaskCount}, so a node seeded as 0 while still ENABLE keeps that stale
   * entry across pipeline runs and hides the CICP-34004 NPE. Only a fresh data provider (restart /
   * leadership change during the evacuation - the real prod trigger) reproduces the unseeded EVACUATE
   * node. EVACUATE is the fix under test; ENABLE is the regression baseline (the fix must not change the
   * normal path).
   */
  @DataProvider(name = "scheduleOnOperatedHostScenarios")
  public static Object[][] scheduleOnOperatedHostScenarios() {
    return new Object[][] {
        {"evacuate-master", InstanceOperation.EVACUATE},
        {"enable-baseline", InstanceOperation.ENABLE}
    };
  }

  @Test(dataProvider = "scheduleOnOperatedHostScenarios")
  public void testTargetedJobSchedulesOnOperatedMasterHost(String label, InstanceOperation op)
      throws Exception {
    String tgtDb = WorkflowGenerator.DEFAULT_TGT_DB;
    String partition = tgtDb + "_0";

    String master = pollForMaster(tgtDb, partition);
    Assert.assertNotNull(master, "target DB should have a MASTER");

    // Block every other node so the replacement replica can never bootstrap; the MASTER therefore
    // stays on the operated host for the whole test (a long swap-out window).
    blockReplacementsFor(master);

    _gSetupTool.getClusterManagementTool().setInstanceOperation(CLUSTER_NAME, master, op);
    Thread.sleep(SETTLE_MS);

    Assert.assertEquals(masterOf(tgtDb, partition), master,
        "MASTER must stay on the operated host " + master + " (op=" + op + ") while replacement blocked");

    // Rebuild the controller (and its task data provider) fresh while the node is already under `op`.
    restartController();

    String wf = TestHelper.getTestMethodName() + "_" + label;
    startTargetedMasterJob(wf, tgtDb, QUICK_TASK_MS);

    TaskState state =
        _driver.pollForWorkflowState(wf, WORKFLOW_TIMEOUT_MS, TaskState.COMPLETED, TaskState.FAILED);
    Assert.assertEquals(state, TaskState.COMPLETED,
        "Targeted job must complete with MASTER on host " + master + " (op=" + op + "), got " + state);

    JobContext ctx = _driver.getJobContext(TaskUtil.getNamespacedJobName(wf, "job1"));
    Assert.assertEquals(ctx.getAssignedParticipant(0), master,
        "Task must run on the operated MASTER host " + master + " (op=" + op + ")");
  }

  /**
   * Throttle-path correctness (not just non-null) on an EVACUATE host. The other two nodes are
   * DISABLEd so the single live EVACUATE host is the only task candidate; a generic job with more
   * tasks than the host's capacity must therefore (a) actually schedule tasks onto the EVACUATE host -
   * proving it is a seeded candidate and the throttle math does not NPE - and (b) never exceed the
   * configured per-instance capacity.
   */
  @Test
  public void testGenericJobRespectsCapacityOnEvacuateHost() throws Exception {
    String evacuateHost = _participants[0].getInstanceName();
    int capacity = 2;
    int numTasks = 5;

    setMaxConcurrentTask(evacuateHost, capacity);
    // Only the EVACUATE host stays a live task candidate; DISABLE hosts are excluded from the set.
    _gSetupTool.getClusterManagementTool()
        .setInstanceOperation(CLUSTER_NAME, evacuateHost, InstanceOperation.EVACUATE);
    _gSetupTool.getClusterManagementTool()
        .setInstanceOperation(CLUSTER_NAME, _participants[1].getInstanceName(),
            InstanceOperation.DISABLE);
    _gSetupTool.getClusterManagementTool()
        .setInstanceOperation(CLUSTER_NAME, _participants[2].getInstanceName(),
            InstanceOperation.DISABLE);
    Thread.sleep(SETTLE_MS);

    // Rebuild the controller so the active-task-count map is seeded fresh while the only candidate is
    // already EVACUATE - the prod trigger. Pre-fix this leaves the EVACUATE host unseeded (null) and the
    // throttle math NPEs, so no task is ever scheduled and the assertions below fail.
    restartController();

    String wf = TestHelper.getTestMethodName();
    JobConfig.Builder job = new JobConfig.Builder().setCommand(MockTask.TASK_COMMAND)
        .setNumConcurrentTasksPerInstance(numTasks)
        .setJobCommandConfigMap(ImmutableMap.of(MockTask.JOB_DELAY, "120000"));
    // Generic (non-targeted) job: supply explicit task configs rather than a target resource.
    List<TaskConfig> taskConfigs = new ArrayList<>();
    for (int i = 0; i < numTasks; i++) {
      taskConfigs.add(new TaskConfig.Builder().setTaskId("task_" + i)
          .setCommand(MockTask.TASK_COMMAND).build());
    }
    job.addTaskConfigs(taskConfigs);
    Workflow workflow = new Workflow.Builder(wf).addJob("job1", job).build();
    _createdWorkflows.add(wf);
    _driver.start(workflow);

    String namespacedJob = TaskUtil.getNamespacedJobName(wf, "job1");
    _driver.pollForJobState(wf, namespacedJob, TaskState.IN_PROGRESS);

    // Capacity is respected: exactly `capacity` tasks run concurrently on the lone EVACUATE candidate,
    // and they all land on the EVACUATE host (proving it is a seeded, usable candidate with no NPE).
    Assert.assertTrue(TestHelper.verify(() -> countRunningOn(namespacedJob, evacuateHost) == capacity,
        TestHelper.WAIT_DURATION),
        "EVACUATE host must run exactly " + capacity + " concurrent tasks (throttle correctness)");
    Assert.assertEquals(countRunning(namespacedJob), capacity,
        "No task may run anywhere except within the EVACUATE host's capacity");

    _driver.stop(wf);
    _driver.pollForWorkflowState(wf, TaskState.STOPPED);
  }

  /**
   * Odd-shaped lifecycle scenario: a targeted job first starts and runs on the EVACUATE MASTER (this
   * requires the fix), then the replacement is unblocked so the evacuation can finally proceed and the
   * partition can hand MASTER off. The workflow must still reach COMPLETED - no wedged partition, no
   * swallowed NPE - regardless of whether the specific task instance finishes on the old MASTER or is
   * reassigned to the new one.
   */
  @Test
  public void testTargetedJobCompletesAfterEvacuationResolves() throws Exception {
    String tgtDb = WorkflowGenerator.DEFAULT_TGT_DB;
    String partition = tgtDb + "_0";

    String master = pollForMaster(tgtDb, partition);
    Assert.assertNotNull(master, "target DB should have a MASTER");

    blockReplacementsFor(master);
    _gSetupTool.getClusterManagementTool()
        .setInstanceOperation(CLUSTER_NAME, master, InstanceOperation.EVACUATE);
    Thread.sleep(SETTLE_MS);
    Assert.assertEquals(masterOf(tgtDb, partition), master,
        "MASTER must start on the EVACUATE host " + master);

    // Fresh data provider while EVACUATE (prod trigger); the task must schedule on the EVACUATE MASTER,
    // which requires the fix.
    restartController();

    String wf = TestHelper.getTestMethodName();
    startTargetedMasterJob(wf, tgtDb, RUNNING_TASK_MS);
    String namespacedJob = TaskUtil.getNamespacedJobName(wf, "job1");

    // The task is observably RUNNING on the EVACUATE MASTER before we resolve the evacuation, so the
    // hand-off below genuinely happens against a live targeted job (not one that already finished).
    Assert.assertTrue(TestHelper.verify(() -> countRunningOn(namespacedJob, master) == 1,
        TestHelper.WAIT_DURATION),
        "targeted task must be RUNNING on the EVACUATE MASTER " + master + " before the hand-off");

    // Unblock the replacements (restart their nodes to clear the in-flight sleep transition) so the
    // EVACUATE host can finally hand off MASTER and the partition migrates away.
    for (int i = 0; i < _numNodes; i++) {
      if (!_participants[i].getInstanceName().equals(master)) {
        startParticipant(ZK_ADDR, i);
      }
    }

    TaskState state =
        _driver.pollForWorkflowState(wf, WORKFLOW_TIMEOUT_MS, TaskState.COMPLETED, TaskState.FAILED);
    Assert.assertEquals(state, TaskState.COMPLETED,
        "Targeted job must complete after the evacuation resolves, got " + state);
  }

  /**
   * Reset shared cluster state between scenarios: delete any workflow this method created, clear all
   * instance operations back to ENABLE, and restart every participant to clear any in-flight
   * {@link SleepTransition}. Then wait for the cluster to reconverge so the next scenario starts clean.
   */
  @AfterMethod
  public void resetClusterState() throws Exception {
    for (String wf : _createdWorkflows) {
      try {
        _driver.delete(wf);
      } catch (Exception ignored) {
        // Best-effort cleanup; the workflow may already be gone.
      }
    }
    _createdWorkflows.clear();
    for (int i = 0; i < _numNodes; i++) {
      String name = _participants[i].getInstanceName();
      _gSetupTool.getClusterManagementTool()
          .setInstanceOperation(CLUSTER_NAME, name, InstanceOperation.ENABLE);
      InstanceConfig cfg = _gSetupTool.getClusterManagementTool().getInstanceConfig(CLUSTER_NAME, name);
      cfg.setMaxConcurrentTask(InstanceConfig.MAX_CONCURRENT_TASK_NOT_SET);
      _gSetupTool.getClusterManagementTool().setInstanceConfig(CLUSTER_NAME, name, cfg);
      // Restart clears any SleepTransition and any partition stuck mid-transition.
      startParticipant(ZK_ADDR, i);
    }
    if (_controller == null || !_controller.isConnected()) {
      _controller = new ClusterControllerManager(ZK_ADDR, CLUSTER_NAME, CONTROLLER_PREFIX + "_reset");
      _controller.syncStart();
    }
    Assert.assertTrue(_clusterVerifier.verifyByPolling(),
        "Cluster must reconverge after resetting scenario state");
  }

  // ---- helpers ----

  private void startTargetedMasterJob(String workflowName, String tgtDb, long taskDelayMs) {
    JobConfig.Builder job = new JobConfig.Builder().setCommand(MockTask.TASK_COMMAND)
        .setTargetResource(tgtDb).setTargetPartitionStates(Collections.singleton(MASTER))
        .setJobCommandConfigMap(ImmutableMap.of(MockTask.JOB_DELAY, Long.toString(taskDelayMs)));
    Workflow workflow = new Workflow.Builder(workflowName).addJob("job1", job).build();
    _createdWorkflows.add(workflowName);
    _driver.start(workflow);
  }

  private void restartController() {
    if (_controller != null && _controller.isConnected()) {
      _controller.syncStop();
    }
    _controller = new ClusterControllerManager(ZK_ADDR, CLUSTER_NAME,
        CONTROLLER_PREFIX + "_restart_" + System.nanoTime());
    _controller.syncStart();
    try {
      Thread.sleep(SETTLE_MS);
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
    }
  }

  private void blockReplacementsFor(String master) {
    for (MockParticipantManager p : _participants) {
      if (p != null && p.isConnected() && !p.getInstanceName().equals(master)) {
        p.setTransition(new SleepTransition(BLOCKED_TRANSITION_MS));
      }
    }
  }

  private void setMaxConcurrentTask(String instance, int max) {
    InstanceConfig cfg = _gSetupTool.getClusterManagementTool().getInstanceConfig(CLUSTER_NAME, instance);
    cfg.setMaxConcurrentTask(max);
    _gSetupTool.getClusterManagementTool().setInstanceConfig(CLUSTER_NAME, instance, cfg);
  }

  private String pollForMaster(String db, String partition) throws InterruptedException {
    for (int i = 0; i < 60; i++) {
      String master = masterOf(db, partition);
      if (master != null) {
        return master;
      }
      Thread.sleep(500);
    }
    return null;
  }

  private String masterOf(String db, String partition) {
    ExternalView ev = _gSetupTool.getClusterManagementTool().getResourceExternalView(CLUSTER_NAME, db);
    if (ev == null || ev.getStateMap(partition) == null) {
      return null;
    }
    for (Map.Entry<String, String> e : ev.getStateMap(partition).entrySet()) {
      if (MASTER.equals(e.getValue())) {
        return e.getKey();
      }
    }
    return null;
  }

  private int countRunning(String namespacedJob) {
    return countRunningOn(namespacedJob, null);
  }

  private int countRunningOn(String namespacedJob, String instance) {
    JobContext ctx = _driver.getJobContext(namespacedJob);
    if (ctx == null) {
      return 0;
    }
    int running = 0;
    for (int p : ctx.getPartitionSet()) {
      if (TaskPartitionState.RUNNING.equals(ctx.getPartitionState(p))
          && (instance == null || instance.equals(ctx.getAssignedParticipant(p)))) {
        running++;
      }
    }
    return running;
  }
}
