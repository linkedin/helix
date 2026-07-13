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
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import com.google.common.collect.ImmutableMap;
import org.apache.helix.TestHelper;
import org.apache.helix.constants.InstanceConstants.InstanceOperation;
import org.apache.helix.controller.rebalancer.strategy.CrushEdRebalanceStrategy;
import org.apache.helix.integration.manager.ClusterControllerManager;
import org.apache.helix.integration.manager.MockParticipantManager;
import org.apache.helix.mock.participant.SleepTransition;
import org.apache.helix.model.ExternalView;
import org.apache.helix.model.IdealState;
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
 * Real-ZK, end-to-end (no mocks) coverage: targeted task jobs must still be scheduled when the target
 * partition's MASTER sits on a live EVACUATE host, and the task throttle path must not NPE while
 * accounting for that EVACUATE host.
 *
 * <p>Structure is a hybrid so new scenarios are cheap to add:
 * <ul>
 *   <li><b>{@code @DataProvider} matrix</b> ({@link #scheduleOnOperatedHostScenarios}) for the large
 *       family of same-shape scenarios: pin the MASTER on a host, apply an instance operation to that
 *       host, restart the controller so the task data provider is rebuilt fresh, start a targeted
 *       MASTER job, and assert the outcome. Adding such a scenario = adding one row.</li>
 *   <li><b>Dedicated {@code @Test} methods</b> for odd-shaped scenarios that need extra steps a single
 *       matrix row cannot express: throttle-capacity accounting on a lone EVACUATE candidate, a
 *       targeted job that runs on the EVACUATE MASTER and completes after the evacuation resolves,
 *       failover coverage (a running task surviving a leadership change, and a queued job scheduled by
 *       the new leader onto the EVACUATE MASTER), and a multi-partition check that one MASTER on an
 *       EVACUATE host does not abort the whole job.</li>
 * </ul>
 *
 * <p>The controller restart in every EVACUATE scenario is essential: the failure only reproduces when
 * the data provider is built fresh while the node is already EVACUATE (a restart or leadership change
 * during the evacuation - the prod trigger). Without it a stale, pre-EVACUATE seed of 0 masks the
 * missing count and the scenario passes even on the buggy code; the ENABLE rows are the baseline.
 *
 * <p>Cluster shape is deliberately small (1 partition, 1 replica) so the single MASTER can be pinned on
 * a chosen host by blocking the replacement's state transition - the only reliable way to reproduce the
 * long swap-out window. Complements the unit-level {@code TestWorkflowControllerDataProviderEvacuate}
 * and the mocked {@code TestEvacuateInstanceTaskAssignment}.
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
  // Long enough that the task is still in-flight while the controller fails over and rebuilds its cache.
  private static final long FAILOVER_TASK_MS = 30_000L;
  private static final String MASTER = "MASTER";

  private final List<String> _createdWorkflows = new ArrayList<>();
  private final List<String> _createdResources = new ArrayList<>();

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
   * pinned there by blocking the replacement, restart the controller so the active-task-count map is
   * rebuilt while the node is already under {@code op}, then require a targeted MASTER job to run there.
   *
   * <p>The restart matters: {@code resetActiveTaskCount} only adds to {@code _participantActiveTaskCount},
   * so a node seeded 0 while ENABLE keeps that stale entry across runs and masks the missing count. Only
   * a fresh data provider leaves the EVACUATE node unseeded. ENABLE is the baseline; the two must behave
   * the same.
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

    // Pin the target MASTER on its host under `op` (replacement blocked so it cannot move).
    String master = pinMasterUnderOperation(tgtDb, partition, op);

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
    // already EVACUATE, leaving it unseeded on the buggy code.
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
    // Assumes the default per-instance task quota is >= capacity, so the participant capacity is the
    // binding limit here (true for the default cluster config).
    Assert.assertTrue(TestHelper.verify(() -> countRunningOn(namespacedJob, evacuateHost) == capacity,
        TestHelper.WAIT_DURATION),
        "EVACUATE host must run exactly " + capacity + " concurrent tasks (throttle correctness)");
    Assert.assertEquals(countRunningOn(namespacedJob, null), capacity,
        "No task may run anywhere except within the EVACUATE host's capacity");

    _driver.stop(wf);
    _driver.pollForWorkflowState(wf, TaskState.STOPPED);
  }

  /**
   * A targeted job first runs on the EVACUATE MASTER, then the replacement is unblocked so the
   * evacuation can proceed. Asserts the MASTER actually migrates off the EVACUATE host and the workflow
   * still reaches COMPLETED across that hand-off - no wedged partition, no swallowed NPE.
   */
  @Test
  public void testTargetedJobCompletesWhenMasterMigratesOffEvacuateHost() throws Exception {
    String tgtDb = WorkflowGenerator.DEFAULT_TGT_DB;
    String partition = tgtDb + "_0";

    String master = pinMasterUnderOperation(tgtDb, partition, InstanceOperation.EVACUATE);

    // Fresh data provider while the node is already EVACUATE.
    restartController();

    String wf = TestHelper.getTestMethodName();
    startTargetedMasterJob(wf, tgtDb, RUNNING_TASK_MS);
    String namespacedJob = TaskUtil.getNamespacedJobName(wf, "job1");

    // Confirm the task is RUNNING on the EVACUATE MASTER, so the hand-off happens against a live job.
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

    // The MASTER must actually leave the EVACUATE host (the hand-off this test is named for).
    Assert.assertTrue(TestHelper.verify(() -> {
      String cur = masterOf(tgtDb, partition);
      return cur != null && !cur.equals(master);
    }, TestHelper.WAIT_DURATION), "MASTER must migrate off the EVACUATE host " + master);

    TaskState state =
        _driver.pollForWorkflowState(wf, WORKFLOW_TIMEOUT_MS, TaskState.COMPLETED, TaskState.FAILED);
    Assert.assertEquals(state, TaskState.COMPLETED,
        "Targeted job must complete across the MASTER hand-off, got " + state);
  }

  /**
   * A targeted task already RUNNING on the EVACUATE MASTER keeps running across a controller failover
   * and the workflow still completes. This is a regression guard, not an NPE reproducer: an INIT/RUNNING
   * task seeds its own host in {@code resetActiveTaskCount} via {@code fillActiveTaskCount}, so its
   * active-task-count entry is always present and this path never hits the unseeded-candidate NPE. That
   * NPE needs an EVACUATE candidate with no running task at assignment time - see {@link
   * #testTargetedJobScheduledByNewLeaderAfterFailover}.
   */
  @Test
  public void testInFlightTaskOnEvacuateMasterSurvivesControllerFailover() throws Exception {
    String tgtDb = WorkflowGenerator.DEFAULT_TGT_DB;
    String partition = tgtDb + "_0";

    String master = pinMasterUnderOperation(tgtDb, partition, InstanceOperation.EVACUATE);

    // Start a long targeted job so the task is still RUNNING when the controller fails over.
    String wf = TestHelper.getTestMethodName();
    startTargetedMasterJob(wf, tgtDb, FAILOVER_TASK_MS);
    String namespacedJob = TaskUtil.getNamespacedJobName(wf, "job1");

    Assert.assertTrue(TestHelper.verify(() -> countRunningOn(namespacedJob, master) == 1,
        TestHelper.WAIT_DURATION),
        "targeted task must be RUNNING on the EVACUATE MASTER " + master + " before the failover");

    // Controller failover: the new leader rebuilds a cold cache while the node is already EVACUATE and
    // the task is in-flight.
    restartController();

    // The running task's assignment must not be dropped by an NPE on the unseeded EVACUATE host.
    Assert.assertTrue(TestHelper.verify(() -> countRunningOn(namespacedJob, master) == 1,
        TestHelper.WAIT_DURATION),
        "task must still be RUNNING on the EVACUATE MASTER " + master + " right after the failover");

    TaskState state =
        _driver.pollForWorkflowState(wf, WORKFLOW_TIMEOUT_MS, TaskState.COMPLETED, TaskState.FAILED);
    Assert.assertEquals(state, TaskState.COMPLETED,
        "in-flight targeted job on the EVACUATE MASTER must survive controller failover, got " + state);

    JobContext ctx = _driver.getJobContext(namespacedJob);
    Assert.assertEquals(ctx.getAssignedParticipant(0), master,
        "task must remain assigned to the EVACUATE MASTER " + master + " across the failover");
  }

  /**
   * The scheduling half of a failover: a targeted job queued while the controller is down must be
   * assigned by the newly elected leader onto the idle EVACUATE MASTER. The new leader builds a cold
   * cache while the node is already EVACUATE and no task is running there; if the active-task-count map
   * leaves that candidate unseeded, {@code getParticipantActiveTaskCount} returns null and the first
   * assignment NPEs (swallowed as a WARN, so the job never starts). Complements {@link
   * #testInFlightTaskOnEvacuateMasterSurvivesControllerFailover}, where an already-running task
   * self-seeds its host and so survives without depending on candidate seeding at all.
   */
  @Test
  public void testTargetedJobScheduledByNewLeaderAfterFailover() throws Exception {
    String tgtDb = WorkflowGenerator.DEFAULT_TGT_DB;
    String partition = tgtDb + "_0";

    String master = pinMasterUnderOperation(tgtDb, partition, InstanceOperation.EVACUATE);

    // Kill the current leader, queue the targeted job while there is no controller, then elect a new
    // leader whose cold cache is built with the node already EVACUATE and no task running on it.
    if (_controller != null && _controller.isConnected()) {
      _controller.syncStop();
    }
    String wf = TestHelper.getTestMethodName();
    startTargetedMasterJob(wf, tgtDb, QUICK_TASK_MS);
    restartController();

    TaskState state =
        _driver.pollForWorkflowState(wf, WORKFLOW_TIMEOUT_MS, TaskState.COMPLETED, TaskState.FAILED);
    Assert.assertEquals(state, TaskState.COMPLETED,
        "new leader must schedule the queued targeted job onto the EVACUATE MASTER " + master
            + " after failover, got " + state);

    JobContext ctx = _driver.getJobContext(TaskUtil.getNamespacedJobName(wf, "job1"));
    Assert.assertEquals(ctx.getAssignedParticipant(0), master,
        "task must run on the EVACUATE MASTER " + master + " after the failover");
  }

  /**
   * When the throttle loop hits the unseeded EVACUATE instance, the NPE is swallowed by
   * WorkflowDispatcher as a WARN, so the whole job's assignment is abandoned (empty JobContext), not
   * just the one partition whose MASTER sits on the EVACUATE host.
   *
   * <p>Uses a multi-partition target DB with a MASTER on the EVACUATE host and MASTERs on healthy hosts,
   * under a cold cache, and asserts every partition's task still runs - healthy-host partitions are not
   * collateral damage. On the buggy code the whole job hangs.
   */
  @Test
  public void testWholeJobNotAbortedByOneMasterOnEvacuateHost() throws Exception {
    String multiDb = "MultiPartTargetDB";
    int numParts = 6;
    _gSetupTool.addResourceToCluster(CLUSTER_NAME, multiDb, numParts, MASTER_SLAVE_STATE_MODEL,
        IdealState.RebalanceMode.FULL_AUTO.name(), CrushEdRebalanceStrategy.class.getName());
    _gSetupTool.rebalanceStorageCluster(CLUSTER_NAME, multiDb, _numReplicas);
    _createdResources.add(multiDb);

    // Every partition must have a MASTER; pick a host holding at least one and confirm other
    // partitions live on different (healthy) hosts, so the check covers healthy-host partitions too.
    Map<Integer, String> masters = pollAllMasters(multiDb, numParts);
    Assert.assertEquals(masters.size(), numParts, "all partitions of " + multiDb + " must have a MASTER");
    String evacHost = masters.get(0);
    Assert.assertTrue(masters.values().stream().anyMatch(h -> !h.equals(evacHost)),
        "test needs MASTERs on hosts other than the EVACUATE host " + evacHost);

    // Pin the EVACUATE host's MASTER partitions (block replacements), then flip it to EVACUATE.
    blockReplacementsFor(evacHost);
    _gSetupTool.getClusterManagementTool()
        .setInstanceOperation(CLUSTER_NAME, evacHost, InstanceOperation.EVACUATE);
    Thread.sleep(SETTLE_MS);

    // Cold cache: rebuild the controller while evacHost is already EVACUATE.
    restartController();

    // Re-confirm a MASTER is still pinned on the EVACUATE host after the restart, so the assertion that
    // a task ran there cannot be a false negative if pinning ever slips.
    Assert.assertTrue(
        TestHelper.verify(() -> mastersByPartition(multiDb, numParts).containsValue(evacHost),
            TestHelper.WAIT_DURATION),
        "a MASTER must still be pinned on the EVACUATE host " + evacHost + " before the job starts");

    String wf = TestHelper.getTestMethodName();
    startTargetedMasterJob(wf, multiDb, QUICK_TASK_MS);

    // On the buggy code the NPE on the unseeded evacHost aborts the entire job compute (empty
    // JobContext), so no partition - including those on healthy hosts - is scheduled.
    TaskState state =
        _driver.pollForWorkflowState(wf, WORKFLOW_TIMEOUT_MS, TaskState.COMPLETED, TaskState.FAILED);
    Assert.assertEquals(state, TaskState.COMPLETED,
        "one MASTER on an EVACUATE host must not abort the whole multi-partition job, got " + state);

    // Tasks ran both on the EVACUATE host and on at least one healthy host.
    JobContext ctx = _driver.getJobContext(TaskUtil.getNamespacedJobName(wf, "job1"));
    Set<String> assigned = new HashSet<>();
    for (int p : ctx.getPartitionSet()) {
      assigned.add(ctx.getAssignedParticipant(p));
    }
    Assert.assertTrue(assigned.contains(evacHost),
        "a task must run on the EVACUATE MASTER host " + evacHost);
    Assert.assertTrue(assigned.stream().anyMatch(h -> h != null && !h.equals(evacHost)),
        "tasks for MASTERs on healthy hosts must also be scheduled (no whole-job abort)");
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
    for (String res : _createdResources) {
      try {
        _gSetupTool.dropResourceFromCluster(CLUSTER_NAME, res);
      } catch (Exception ignored) {
        // Best-effort cleanup; the resource may already be gone.
      }
    }
    _createdResources.clear();
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

  private String pinMasterUnderOperation(String tgtDb, String partition, InstanceOperation op)
      throws InterruptedException {
    String master = pollForMaster(tgtDb, partition);
    Assert.assertNotNull(master, "target DB should have a MASTER");
    // Block every other node so the replacement replica can never bootstrap; the MASTER therefore
    // stays on the operated host for the whole test (a long swap-out window).
    blockReplacementsFor(master);
    _gSetupTool.getClusterManagementTool().setInstanceOperation(CLUSTER_NAME, master, op);
    Thread.sleep(SETTLE_MS);
    Assert.assertEquals(masterOf(tgtDb, partition), master,
        "MASTER must stay on the operated host " + master + " (op=" + op + ") while replacement blocked");
    return master;
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

  private Map<Integer, String> pollAllMasters(String db, int numParts) throws InterruptedException {
    for (int i = 0; i < 120; i++) {
      Map<Integer, String> m = mastersByPartition(db, numParts);
      if (m.size() == numParts) {
        return m;
      }
      Thread.sleep(500);
    }
    return mastersByPartition(db, numParts);
  }

  private Map<Integer, String> mastersByPartition(String db, int numParts) {
    Map<Integer, String> res = new HashMap<>();
    ExternalView ev = _gSetupTool.getClusterManagementTool().getResourceExternalView(CLUSTER_NAME, db);
    if (ev == null) {
      return res;
    }
    for (int p = 0; p < numParts; p++) {
      Map<String, String> sm = ev.getStateMap(db + "_" + p);
      if (sm != null) {
        for (Map.Entry<String, String> e : sm.entrySet()) {
          if (MASTER.equals(e.getValue())) {
            res.put(p, e.getKey());
          }
        }
      }
    }
    return res;
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
