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

import java.util.Collections;
import java.util.Map;

import org.apache.helix.TestHelper;
import org.apache.helix.constants.InstanceConstants;
import org.apache.helix.integration.manager.MockParticipantManager;
import org.apache.helix.mock.participant.SleepTransition;
import org.apache.helix.model.ExternalView;
import org.apache.helix.task.JobConfig;
import org.apache.helix.task.TaskState;
import org.apache.helix.task.Workflow;
import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

/**
 * Real-ZK reproduction for CICP-34004. The existing TestEvacuateInstanceTaskAssignment stubbed the
 * data provider; this drives the real controller pipeline end to end.
 *
 * Scenario: a target MasterSlave DB has its MASTER on an instance that is then flagged EVACUATE,
 * while the only other node is blocked from completing its state transition - so the replacement
 * replica can never bootstrap and the MASTER stays put on the EVACUATE instance. This mirrors a long
 * swap-out window where the replacement has not finished bootstrapping. A targeted (FixedTarget) job
 * targeting MASTER must still be assigned to that EVACUATE+MASTER instance.
 *
 * Pre-fix this hangs because EVACUATE instances were excluded from task assignment.
 */
public class TestEvacuateTargetJobAssignment extends TaskTestBase {
  // Long enough to outlast the test, so the blocked replica never bootstraps.
  private static final long BLOCKED_TRANSITION_MS = 300_000L;
  // Time for the controller to react to EVACUATE before we assert the MASTER did not move.
  private static final long MASTER_SETTLE_MS = 5_000L;
  private static final long WORKFLOW_TIMEOUT_MS = 30_000L;

  @BeforeClass
  public void beforeClass() throws Exception {
    _numNodes = 2;
    _numPartitions = 1;
    _numReplicas = 1;
    _numDbs = 1;
    super.beforeClass();
  }

  @Test
  public void testTaskAssignedToEvacuatingMaster() throws Exception {
    String tgtDb = WorkflowGenerator.DEFAULT_TGT_DB;
    String partition = tgtDb + "_0";

    // 1. Wait for the target DB to converge and find the MASTER instance.
    String master = pollForMaster(tgtDb, partition);
    Assert.assertNotNull(master, "target DB should have a MASTER");

    // 2. Block the OTHER (non-master) node's MasterSlave transitions so its replica can never
    //    bootstrap. Both nodes stay live; the replacement therefore stays stuck and
    //    min-active-replica keeps the MASTER pinned on the EVACUATE node.
    for (MockParticipantManager p : _participants) {
      if (p != null && p.isConnected() && !p.getInstanceName().equals(master)) {
        p.setTransition(new SleepTransition(BLOCKED_TRANSITION_MS));
      }
    }

    // 3. Flag the MASTER instance EVACUATE.
    _gSetupTool.getClusterManagementTool()
        .setInstanceOperation(CLUSTER_NAME, master, InstanceConstants.InstanceOperation.EVACUATE);

    // 4. Confirm MASTER stays on the EVACUATE instance: after the controller has reacted to EVACUATE,
    //    the blocked replacement means MASTER has nowhere to move.
    Thread.sleep(MASTER_SETTLE_MS);
    ExternalView ev =
        _gSetupTool.getClusterManagementTool().getResourceExternalView(CLUSTER_NAME, tgtDb);
    Map<String, String> stateMap = ev == null ? null : ev.getStateMap(partition);
    Assert.assertTrue(stateMap != null && "MASTER".equals(stateMap.get(master)),
        "MASTER must stay on the EVACUATE instance " + master + " while the replacement is blocked");

    // 5. Start a targeted job that targets the MASTER state of the DB (like an Espresso backup job).
    String wf = TestHelper.getTestMethodName();
    JobConfig.Builder job = new JobConfig.Builder()
        .setCommand(MockTask.TASK_COMMAND)
        .setTargetResource(tgtDb)
        .setTargetPartitionStates(Collections.singleton("MASTER"));
    Workflow workflow = new Workflow.Builder(wf).addJob("job1", job).build();
    _driver.start(workflow);

    // 6. The targeted task must be assigned to the EVACUATE+MASTER instance and complete.
    //    Pre-fix (CICP-34004) the job hangs because the EVACUATE host is excluded.
    TaskState state =
        _driver.pollForWorkflowState(wf, WORKFLOW_TIMEOUT_MS, TaskState.COMPLETED, TaskState.FAILED);
    Assert.assertEquals(state, TaskState.COMPLETED,
        "Targeted task must run on the EVACUATE+MASTER instance " + master
            + " but the workflow ended in state " + state);
  }

  private String pollForMaster(String db, String partition) throws InterruptedException {
    for (int i = 0; i < 60; i++) {
      ExternalView ev =
          _gSetupTool.getClusterManagementTool().getResourceExternalView(CLUSTER_NAME, db);
      if (ev != null && ev.getStateMap(partition) != null) {
        for (Map.Entry<String, String> e : ev.getStateMap(partition).entrySet()) {
          if ("MASTER".equals(e.getValue())) {
            return e.getKey();
          }
        }
      }
      Thread.sleep(500);
    }
    return null;
  }
}
