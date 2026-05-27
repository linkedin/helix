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

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.helix.common.caches.TaskDataCache;
import org.apache.helix.constants.InstanceConstants;
import org.apache.helix.controller.dataproviders.WorkflowControllerDataProvider;
import org.apache.helix.controller.stages.BestPossibleStateOutput;
import org.apache.helix.controller.stages.CurrentStateOutput;
import org.apache.helix.model.ClusterConfig;
import org.apache.helix.model.IdealState;
import org.apache.helix.model.InstanceConfig;
import org.apache.helix.model.LiveInstance;
import org.apache.helix.model.Partition;
import org.apache.helix.zookeeper.datamodel.ZNRecord;
import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * End-to-end mocked test for CICP-34004. Reproduces the production scenario where the MASTER
 * replica of a target resource still resides on an EVACUATE-flagged live instance during a
 * swap-out window, and verifies that:
 *
 * 1. The task framework assigns the task to that EVACUATE+MASTER instance (the data-provider
 *    fix in WorkflowControllerDataProvider exposes EVACUATE+live to the assignment calculator).
 * 2. The throttle path in AbstractTaskDispatcher does NOT throw NPE when the candidate instance
 *    is EVACUATE-flagged (the fix at AbstractTaskDispatcher.java:651 sources the per-instance
 *    capacity from the full instance config map rather than the assignable-only map).
 *
 * The test mocks WorkflowControllerDataProvider exactly as the production controller flow uses
 * it. It deliberately mocks `getAssignableInstanceConfigMap()` to EXCLUDE the EVACUATE instance
 * (matching production behavior - that map is filtered by InstanceConfig.isAssignable()) and
 * mocks `getInstanceConfigMap()` to include all instances. If the fix at line 651 regresses,
 * `getAssignableInstanceConfigMap().get(evacuateInstance)` would return null and NPE.
 */
public class TestEvacuateInstanceTaskAssignment {
  private static final String CLUSTER_NAME = "TestEvacuateCluster";
  private static final String INSTANCE_PREFIX = "Instance_";
  private static final int NUM_PARTICIPANTS = 3;
  private static final String WORKFLOW_NAME = "TestEvacuateWorkflow";
  private static final String JOB_NAME = "TestEvacuateJob";
  private static final String PARTITION_NAME = "0";
  private static final String TARGET_RESOURCES = "TestDB";

  private static final String SLAVE_INSTANCE = INSTANCE_PREFIX + "0";
  /** This instance hosts the MASTER replica AND is flagged InstanceOperation.EVACUATE. */
  private static final String MASTER_EVACUATE_INSTANCE = INSTANCE_PREFIX + "1";
  private static final String SLAVE_INSTANCE_2 = INSTANCE_PREFIX + "2";

  private Map<String, LiveInstance> _liveInstances;
  private Map<String, InstanceConfig> _allInstanceConfigs;
  private Map<String, InstanceConfig> _assignableInstanceConfigs;
  private ClusterConfig _clusterConfig;
  private AssignableInstanceManager _assignableInstanceManager;

  @BeforeClass
  public void beforeClass() {
    _liveInstances = new HashMap<>();
    _allInstanceConfigs = new HashMap<>();
    _assignableInstanceConfigs = new HashMap<>();
    _clusterConfig = new ClusterConfig(CLUSTER_NAME);

    for (int i = 0; i < NUM_PARTICIPANTS; i++) {
      String instanceName = INSTANCE_PREFIX + i;
      _liveInstances.put(instanceName, new LiveInstance(instanceName));

      InstanceConfig config = new InstanceConfig(instanceName);
      if (instanceName.equals(MASTER_EVACUATE_INSTANCE)) {
        // Mark MASTER instance as EVACUATE - the production scenario from CICP-34004.
        config.setInstanceOperation(InstanceConstants.InstanceOperation.EVACUATE);
      } else {
        config.setInstanceOperation(InstanceConstants.InstanceOperation.ENABLE);
      }
      _allInstanceConfigs.put(instanceName, config);

      // Match production semantics of getAssignableInstanceConfigMap(): only ENABLE/DISABLE
      // (per InstanceConstants.ASSIGNABLE_INSTANCE_OPERATIONS) make it into this map.
      if (config.isAssignable()) {
        _assignableInstanceConfigs.put(instanceName, config);
      }
    }

    // Sanity: EVACUATE instance is in the full map but NOT in the assignable map - this is the
    // discrepancy that caused the NPE before the fix at AbstractTaskDispatcher.java:651.
    Assert.assertTrue(_allInstanceConfigs.containsKey(MASTER_EVACUATE_INSTANCE));
    Assert.assertFalse(_assignableInstanceConfigs.containsKey(MASTER_EVACUATE_INSTANCE));

    _assignableInstanceManager = new AssignableInstanceManager();
  }

  @Test
  public void testTaskAssignedToEvacuatingInstanceHostingMaster() {
    MockTestInformation mock = new MockTestInformation();
    when(mock._cache.getWorkflowConfig(WORKFLOW_NAME)).thenReturn(mock._workflowConfig);
    when(mock._cache.getJobConfig(JOB_NAME)).thenReturn(mock._jobConfig);
    when(mock._cache.getTaskDataCache()).thenReturn(mock._taskDataCache);
    when(mock._cache.getJobContext(JOB_NAME)).thenReturn(mock._jobContext);
    when(mock._cache.getIdealStates()).thenReturn(mock._idealStates);
    // Reflects the fixed WorkflowControllerDataProvider.getEnabledLiveInstances() behavior:
    // the EVACUATE+live instance IS included in the eligible set for task assignment.
    when(mock._cache.getEnabledLiveInstances()).thenReturn(_liveInstances.keySet());
    // Full instance config map - what the FIXED throttle path at AbstractTaskDispatcher.java:651
    // queries. Includes EVACUATE.
    when(mock._cache.getInstanceConfigMap()).thenReturn(_allInstanceConfigs);
    // Assignable-only instance config map - what the BUGGY pre-fix throttle path queried. Does
    // NOT include EVACUATE. Mocking this to match production semantics so that any future
    // regression that reintroduces a call to this map for an EVACUATE instance will NPE here.
    when(mock._cache.getAssignableInstanceConfigMap()).thenReturn(_assignableInstanceConfigs);
    when(mock._cache.getClusterConfig()).thenReturn(_clusterConfig);
    when(mock._taskDataCache.getRuntimeJobDag(WORKFLOW_NAME)).thenReturn(mock._runtimeJobDag);
    _assignableInstanceManager.buildAssignableInstances(_clusterConfig, mock._taskDataCache,
        _liveInstances, _allInstanceConfigs);
    when(mock._cache.getAssignableInstanceManager()).thenReturn(_assignableInstanceManager);
    when(mock._cache.getExistsLiveInstanceOrCurrentStateOrMessageChange()).thenReturn(false);
    Set<String> inflightJobDag = new HashSet<>();
    inflightJobDag.add(JOB_NAME);
    when(mock._taskDataCache.getRuntimeJobDag(WORKFLOW_NAME).getInflightJobList())
        .thenReturn(inflightJobDag);

    // Sanity: AssignableInstanceManager built from raw liveInstances must include the EVACUATE
    // instance (no operation filter at this layer).
    Assert.assertTrue(_assignableInstanceManager.getAssignableInstanceNames()
            .contains(MASTER_EVACUATE_INSTANCE),
        "AssignableInstanceManager must include EVACUATE+live instance");

    BestPossibleStateOutput bestPossibleStateOutput = new BestPossibleStateOutput();
    WorkflowDispatcher workflowDispatcher = new WorkflowDispatcher();
    workflowDispatcher.updateCache(mock._cache);

    // The actual call under test. If line 651 still used getAssignableInstanceConfigMap, this
    // would NPE before producing any output.
    workflowDispatcher.updateWorkflowStatus(WORKFLOW_NAME, mock._workflowConfig,
        mock._workflowContext, mock._currentStateOutput, bestPossibleStateOutput);

    Partition taskPartition = new Partition(JOB_NAME + "_" + PARTITION_NAME);
    Map<String, String> partitionMap =
        bestPossibleStateOutput.getPartitionStateMap(JOB_NAME).getPartitionMap(taskPartition);
    Assert.assertNotNull(partitionMap, "Task partition must have an assignment in BestPossibleStateOutput");
    String stateOnMaster = partitionMap.get(MASTER_EVACUATE_INSTANCE);
    Assert.assertEquals(stateOnMaster, TaskPartitionState.RUNNING.name(),
        "Task must be assigned to the EVACUATE+MASTER instance, since that's where the target "
            + "MASTER replica resides. Pre-fix this would have been left unassigned (CICP-34004).");
  }

  private WorkflowConfig prepareWorkflowConfig() {
    WorkflowConfig.Builder workflowConfigBuilder = new WorkflowConfig.Builder();
    workflowConfigBuilder.setWorkflowId(WORKFLOW_NAME);
    workflowConfigBuilder.setTerminable(false);
    workflowConfigBuilder.setTargetState(TargetState.START);
    workflowConfigBuilder.setJobQueue(true);
    JobDag jobDag = new JobDag();
    jobDag.addNode(JOB_NAME);
    workflowConfigBuilder.setJobDag(jobDag);
    return workflowConfigBuilder.build();
  }

  private JobConfig prepareJobConfig() {
    JobConfig.Builder jobConfigBuilder = new JobConfig.Builder();
    jobConfigBuilder.setWorkflow(WORKFLOW_NAME);
    jobConfigBuilder.setCommand("TestCommand");
    jobConfigBuilder.setTargetResource(TARGET_RESOURCES);
    jobConfigBuilder.setJobId(JOB_NAME);
    List<String> targetPartition = new ArrayList<>();
    targetPartition.add(TARGET_RESOURCES + "_0");
    jobConfigBuilder.setTargetPartitions(targetPartition);
    Set<String> targetPartitionStates = new HashSet<>();
    targetPartitionStates.add("MASTER");
    List<TaskConfig> taskConfigs = new ArrayList<>();
    TaskConfig.Builder taskConfigBuilder = new TaskConfig.Builder();
    taskConfigBuilder.setTaskId("0");
    taskConfigs.add(taskConfigBuilder.build());
    jobConfigBuilder.setTargetPartitionStates(targetPartitionStates);
    jobConfigBuilder.addTaskConfigs(taskConfigs);
    return jobConfigBuilder.build();
  }

  private WorkflowContext prepareWorkflowContext() {
    ZNRecord record = new ZNRecord(WORKFLOW_NAME);
    record.setSimpleField(WorkflowContext.WorkflowContextProperties.StartTime.name(), "0");
    record.setSimpleField(WorkflowContext.WorkflowContextProperties.NAME.name(), WORKFLOW_NAME);
    record.setSimpleField(WorkflowContext.WorkflowContextProperties.STATE.name(),
        TaskState.IN_PROGRESS.name());
    Map<String, String> jobState = new HashMap<>();
    jobState.put(JOB_NAME, TaskState.IN_PROGRESS.name());
    record.setMapField(WorkflowContext.WorkflowContextProperties.JOB_STATES.name(), jobState);
    return new WorkflowContext(record);
  }

  /**
   * Job context: target partition is mapped to TARGET_RESOURCES_0 (which has MASTER on
   * MASTER_EVACUATE_INSTANCE per IdealState below). No assignment yet.
   */
  private JobContext prepareJobContext() {
    ZNRecord record = new ZNRecord(JOB_NAME);
    JobContext jobContext = new JobContext(record);
    jobContext.setStartTime(0L);
    jobContext.setName(JOB_NAME);
    jobContext.setPartitionTarget(0, TARGET_RESOURCES + "_0");
    return jobContext;
  }

  private Map<String, IdealState> prepareIdealStates() {
    ZNRecord record = new ZNRecord(JOB_NAME);
    record.setSimpleField(IdealState.IdealStateProperty.NUM_PARTITIONS.name(), "1");
    record.setSimpleField(IdealState.IdealStateProperty.EXTERNAL_VIEW_DISABLED.name(), "true");
    record.setSimpleField(IdealState.IdealStateProperty.IDEAL_STATE_MODE.name(), "AUTO");
    record.setSimpleField(IdealState.IdealStateProperty.REBALANCE_MODE.name(), "TASK");
    record.setSimpleField(IdealState.IdealStateProperty.REPLICAS.name(), "1");
    record.setSimpleField(IdealState.IdealStateProperty.STATE_MODEL_DEF_REF.name(), "Task");
    record.setSimpleField(IdealState.IdealStateProperty.STATE_MODEL_FACTORY_NAME.name(), "DEFAULT");
    record.setSimpleField(IdealState.IdealStateProperty.REBALANCER_CLASS_NAME.name(),
        "org.apache.helix.task.JobRebalancer");
    record.setMapField(JOB_NAME + "_" + PARTITION_NAME, new HashMap<>());
    record.setListField(JOB_NAME + "_" + PARTITION_NAME, new ArrayList<>());
    Map<String, IdealState> idealStates = new HashMap<>();
    idealStates.put(JOB_NAME, new IdealState(record));

    ZNRecord recordDB = new ZNRecord(TARGET_RESOURCES);
    recordDB.setSimpleField(IdealState.IdealStateProperty.REPLICAS.name(), "3");
    recordDB.setSimpleField(IdealState.IdealStateProperty.REBALANCE_MODE.name(), "FULL_AUTO");
    recordDB.setSimpleField(IdealState.IdealStateProperty.STATE_MODEL_DEF_REF.name(), "MasterSlave");
    Map<String, String> mapping = new HashMap<>();
    mapping.put(MASTER_EVACUATE_INSTANCE, "MASTER");
    mapping.put(SLAVE_INSTANCE, "SLAVE");
    mapping.put(SLAVE_INSTANCE_2, "SLAVE");
    recordDB.setMapField(TARGET_RESOURCES + "_0", mapping);
    List<String> listField = new ArrayList<>();
    listField.add(MASTER_EVACUATE_INSTANCE);
    listField.add(SLAVE_INSTANCE);
    listField.add(SLAVE_INSTANCE_2);
    recordDB.setListField(TARGET_RESOURCES + "_0", listField);
    idealStates.put(TARGET_RESOURCES, new IdealState(recordDB));

    return idealStates;
  }

  private CurrentStateOutput prepareCurrentState() {
    CurrentStateOutput currentStateOutput = new CurrentStateOutput();
    currentStateOutput.setResourceStateModelDef(JOB_NAME, "TASK");
    currentStateOutput.setBucketSize(JOB_NAME, 0);
    currentStateOutput.setResourceStateModelDef(TARGET_RESOURCES, "MasterSlave");
    currentStateOutput.setBucketSize(TARGET_RESOURCES, 0);
    Partition dbPartition = new Partition(TARGET_RESOURCES + "_0");
    // MASTER replica still resides on the EVACUATE-flagged instance. This is the precise
    // production state described in CICP-34004.
    currentStateOutput.setEndTime(TARGET_RESOURCES, dbPartition, MASTER_EVACUATE_INSTANCE, 0L);
    currentStateOutput.setCurrentState(TARGET_RESOURCES, dbPartition, MASTER_EVACUATE_INSTANCE,
        "MASTER");
    currentStateOutput.setInfo(TARGET_RESOURCES, dbPartition, MASTER_EVACUATE_INSTANCE, "");
    currentStateOutput.setEndTime(TARGET_RESOURCES, dbPartition, SLAVE_INSTANCE, 0L);
    currentStateOutput.setCurrentState(TARGET_RESOURCES, dbPartition, SLAVE_INSTANCE, "SLAVE");
    currentStateOutput.setInfo(TARGET_RESOURCES, dbPartition, SLAVE_INSTANCE, "");
    currentStateOutput.setEndTime(TARGET_RESOURCES, dbPartition, SLAVE_INSTANCE_2, 0L);
    currentStateOutput.setCurrentState(TARGET_RESOURCES, dbPartition, SLAVE_INSTANCE_2, "SLAVE");
    currentStateOutput.setInfo(TARGET_RESOURCES, dbPartition, SLAVE_INSTANCE_2, "");
    return currentStateOutput;
  }

  private class MockTestInformation {
    private WorkflowControllerDataProvider _cache = mock(WorkflowControllerDataProvider.class);
    private TaskDataCache _taskDataCache = mock(TaskDataCache.class);
    private RuntimeJobDag _runtimeJobDag = mock(RuntimeJobDag.class);
    private WorkflowConfig _workflowConfig = prepareWorkflowConfig();
    private WorkflowContext _workflowContext = prepareWorkflowContext();
    private Map<String, IdealState> _idealStates = prepareIdealStates();
    private JobConfig _jobConfig = prepareJobConfig();
    private JobContext _jobContext = prepareJobContext();
    private CurrentStateOutput _currentStateOutput = prepareCurrentState();
  }
}
