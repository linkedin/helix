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
import java.util.HashMap;
import java.util.Map;

import org.apache.helix.ConfigAccessor;
import org.apache.helix.TestHelper;
import org.apache.helix.constants.InstanceConstants;
import org.apache.helix.integration.manager.MockParticipantManager;
import org.apache.helix.mock.participant.SleepTransition;
import org.apache.helix.model.BuiltInStateModelDefinitions;
import org.apache.helix.model.ClusterConfig;
import org.apache.helix.model.ExternalView;
import org.apache.helix.model.InstanceConfig;
import org.apache.helix.participant.StateMachineEngine;
import org.apache.helix.task.JobConfig;
import org.apache.helix.task.TaskFactory;
import org.apache.helix.task.TaskState;
import org.apache.helix.task.TaskStateModelFactory;
import org.apache.helix.task.Workflow;
import org.apache.helix.tools.ClusterSetup;
import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

/**
 * Real-ZK reproduction for CICP-34004 under the ACM-swap scenario (which is how prod evacuations
 * actually run). Differs from {@link TestEvacuateTargetJobAssignment} by adding a SWAP_IN node
 * paired (by logicalId) with the EVACUATE node, so the data provider treats the EVACUATE node as a
 * swap-out instance. The swap-in node's bootstrap is blocked, so the MASTER stays on the
 * EVACUATE/swap-out node (the prod condition). A targeted job targeting MASTER must still be
 * assigned to that node.
 */
public class TestEvacuateSwapTargetJobAssignment extends TaskTestBase {
  private static final String ZONE = "zone";
  private static final String HOST = "host";
  private static final String LOGICAL_ID = "logicalId";
  private static final String TOPOLOGY = ZONE + "/" + HOST + "/" + LOGICAL_ID;

  @BeforeClass
  public void beforeClass() throws Exception {
    _numNodes = 2;
    _numPartitions = 1;
    _numReplicas = 1;
    _numDbs = 1;
    super.beforeClass();
  }

  // Add topology + per-instance logicalId so swap-out/swap-in can be paired by logicalId.
  @Override
  protected void setupParticipants(ClusterSetup setupTool) {
    ConfigAccessor configAccessor = new ConfigAccessor(_gZkClient);
    ClusterConfig clusterConfig = configAccessor.getClusterConfig(CLUSTER_NAME);
    clusterConfig.setTopology(TOPOLOGY);
    clusterConfig.setFaultZoneType(ZONE);
    clusterConfig.setTopologyAwareEnabled(true);
    configAccessor.setClusterConfig(CLUSTER_NAME, clusterConfig);

    _participants = new MockParticipantManager[_numNodes];
    for (int i = 0; i < _numNodes; i++) {
      String node = PARTICIPANT_PREFIX + "_" + (_startPort + i);
      InstanceConfig config = new InstanceConfig.Builder().setDomain(
              String.format("%s=zone_%d, %s=%s, %s=logical_%d", ZONE, i, HOST, node, LOGICAL_ID, i))
          .build(node);
      setupTool.getClusterManagementTool().addInstance(CLUSTER_NAME, config);
    }
  }

  @Test
  public void testTaskAssignedToSwapOutMaster() throws Exception {
    String tgtDb = WorkflowGenerator.DEFAULT_TGT_DB;
    String partition = tgtDb + "_0";

    // 1. Find the MASTER instance and its logicalId.
    String master = pollForMaster(tgtDb, partition);
    Assert.assertNotNull(master, "target DB should have a MASTER");
    int masterIdx = Integer.parseInt(master.substring(master.lastIndexOf('_') + 1)) - _startPort;
    String masterLogicalId = "logical_" + masterIdx;

    // Block the other base node so the replica cannot move there either.
    for (MockParticipantManager p : _participants) {
      if (p != null && p.isConnected() && !p.getInstanceName().equals(master)) {
        p.setTransition(new SleepTransition(300000));
      }
    }

    // 2. Add a SWAP_IN node sharing the MASTER's logicalId, with its MasterSlave transitions
    //    blocked so the swap-in replica can never bootstrap (prod: swap-in still bootstrapping).
    String swapInName = PARTICIPANT_PREFIX + "_" + (_startPort + 100);
    InstanceConfig swapInConfig = new InstanceConfig.Builder().setDomain(
            String.format("%s=zone_swapin, %s=%s, %s=%s", ZONE, HOST, swapInName, LOGICAL_ID,
                masterLogicalId))
        .setInstanceOperation(InstanceConstants.InstanceOperation.SWAP_IN).build(swapInName);
    _gSetupTool.getClusterManagementTool().addInstance(CLUSTER_NAME, swapInConfig);
    MockParticipantManager swapIn = startTaskParticipant(swapInName);
    swapIn.setTransition(new SleepTransition(300000));

    // 3. Flag the MASTER instance EVACUATE -> it becomes the swap-out instance.
    _gSetupTool.getClusterManagementTool()
        .setInstanceOperation(CLUSTER_NAME, master, InstanceConstants.InstanceOperation.EVACUATE);

    // 4. Confirm MASTER stays on the EVACUATE/swap-out instance (swap-in is blocked).
    boolean stays = false;
    for (int i = 0; i < 20; i++) {
      Thread.sleep(1000);
      ExternalView ev =
          _gSetupTool.getClusterManagementTool().getResourceExternalView(CLUSTER_NAME, tgtDb);
      Map<String, String> stateMap = ev == null ? null : ev.getStateMap(partition);
      if (stateMap != null && "MASTER".equals(stateMap.get(master))) {
        stays = true;
        if (i >= 4) {
          break;
        }
      }
    }
    Assert.assertTrue(stays,
        "MASTER must stay on the EVACUATE/swap-out instance " + master + " while swap-in is blocked");

    // 5. Run a targeted job that targets MASTER (like an Espresso backup job).
    String wf = TestHelper.getTestMethodName();
    JobConfig.Builder job = new JobConfig.Builder().setCommand(MockTask.TASK_COMMAND)
        .setTargetResource(tgtDb).setTargetPartitionStates(Collections.singleton("MASTER"));
    Workflow workflow = new Workflow.Builder(wf).addJob("job1", job).build();
    _driver.start(workflow);

    // 6. The task must be assigned to the swap-out+MASTER instance and the workflow complete.
    TaskState state =
        _driver.pollForWorkflowState(wf, 30000, TaskState.COMPLETED, TaskState.FAILED);
    Assert.assertEquals(state, TaskState.COMPLETED,
        "Targeted task must run on the EVACUATE/swap-out+MASTER instance " + master
            + " but the workflow ended in state " + state);
  }

  private MockParticipantManager startTaskParticipant(String name) {
    MockParticipantManager p = new MockParticipantManager(ZK_ADDR, CLUSTER_NAME, name);
    Map<String, TaskFactory> taskFactoryReg = new HashMap<>();
    taskFactoryReg.put(MockTask.TASK_COMMAND, MockTask::new);
    StateMachineEngine engine = p.getStateMachineEngine();
    engine.registerStateModelFactory("Task", new TaskStateModelFactory(p, taskFactoryReg));
    p.syncStart();
    return p;
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
