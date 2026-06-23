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
 * Real-ZK reproduction for CICP-34004. The mocked TestEvacuateInstanceTaskAssignment stubbed the
 * data provider; this drives the real controller pipeline end to end.
 *
 * Scenario (mirrors prod): a target MasterSlave DB has its MASTER on an instance that is then
 * flagged EVACUATE, and the replacement cannot bootstrap (here: the only other node is stopped, so
 * the replica has nowhere to move). The MASTER therefore stays on the EVACUATE instance. A targeted
 * (FixedTarget) job targeting MASTER must still be assigned to that EVACUATE+MASTER instance.
 *
 * Pre-fix this hangs because EVACUATE instances were excluded from task assignment.
 */
public class TestEvacuateTargetJobAssignment extends TaskTestBase {

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
    //    bootstrap. Both nodes stay live; this recreates the prod condition where the swap
    //    replacement is still bootstrapping, so min-active-replica keeps the MASTER pinned on the
    //    EVACUATE node.
    for (MockParticipantManager p : _participants) {
      if (p != null && p.isConnected() && !p.getInstanceName().equals(master)) {
        p.setTransition(new SleepTransition(300000));
      }
    }

    // 3. Flag the MASTER instance EVACUATE.
    _gSetupTool.getClusterManagementTool()
        .setInstanceOperation(CLUSTER_NAME, master, InstanceConstants.InstanceOperation.EVACUATE);

    // 4. Confirm MASTER stays on the EVACUATE instance (replacement is blocked from bootstrapping).
    boolean stays = false;
    for (int i = 0; i < 20; i++) {
      Thread.sleep(1000);
      ExternalView ev =
          _gSetupTool.getClusterManagementTool().getResourceExternalView(CLUSTER_NAME, tgtDb);
      Map<String, String> stateMap = ev == null ? null : ev.getStateMap(partition);
      if (stateMap != null && "MASTER".equals(stateMap.get(master))) {
        stays = true;
        if (i >= 4) {
          break; // master held steady for ~5s
        }
      }
    }
    Assert.assertTrue(stays,
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
        _driver.pollForWorkflowState(wf, 30000, TaskState.COMPLETED, TaskState.FAILED);
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
