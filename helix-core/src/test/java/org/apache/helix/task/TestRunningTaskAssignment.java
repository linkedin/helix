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

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.SortedSet;
import java.util.TreeSet;

import org.apache.helix.controller.dataproviders.WorkflowControllerDataProvider;
import org.apache.helix.controller.stages.CurrentStateOutput;
import org.apache.helix.task.AbstractTaskDispatcher.PartitionAssignment;
import org.apache.helix.zookeeper.datamodel.ZNRecord;
import org.testng.Assert;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;


public class TestRunningTaskAssignment {
  @DataProvider
  public Object[][] runningTaskAssignments() {
    Object[][] cases = new Object[16][3];
    int index = 0;
    for (boolean targeted : new boolean[] {false, true}) {
      for (boolean assignmentChanged : new boolean[] {false, true}) {
        for (String legacyFlag : new String[] {null, "false", "true", "invalid"}) {
          cases[index++] = new Object[] {targeted, assignmentChanged, legacyFlag};
        }
      }
    }
    return cases;
  }

  @Test(dataProvider = "runningTaskAssignments")
  public void testRunningTasksOnlyFollowTargetChanges(boolean targeted, boolean assignmentChanged,
      String legacyFlag) {
    JobConfig.Builder builder = new JobConfig.Builder().setWorkflow("workflow").setJobId("job")
        .setCommand("Dummy").setNumberOfTasks(1);
    if (targeted) {
      builder.setTargetResource("database");
    }
    JobConfig jobConfig = builder.build();
    if (legacyFlag != null) {
      jobConfig.getRecord().setSimpleField("RebalanceRunningTask", legacyFlag);
    }
    ZNRecord originalConfig = new ZNRecord(jobConfig.getRecord());
    JobContext jobContext = new JobContext(new ZNRecord("job"));
    jobContext.setPartitionState(0, TaskPartitionState.RUNNING);
    jobContext.setAssignedParticipant(0, "old");
    jobContext.setPartitionNumAttempts(0, 1);
    WorkflowConfig workflowConfig = new WorkflowConfig.Builder("workflow").build();
    WorkflowContext workflowContext = new WorkflowContext(new ZNRecord("workflow"));
    CurrentStateOutput currentState = new CurrentStateOutput();
    Collection<String> liveInstances = Arrays.asList("old", "new");
    Set<Integer> allPartitions = Collections.singleton(0);
    Map<String, SortedSet<Integer>> currentAssignments =
        Collections.singletonMap("old", new TreeSet<>(allPartitions));
    Map<String, SortedSet<Integer>> targetAssignments =
        Collections.singletonMap("new", new TreeSet<>(allPartitions));
    Map<String, Set<Integer>> assignedPartitions =
        Collections.singletonMap("old", allPartitions);
    Map<Integer, PartitionAssignment> assignments = new HashMap<>();
    assignments.put(0, new PartitionAssignment("old", TaskPartitionState.RUNNING.name()));

    WorkflowControllerDataProvider cache = mock(WorkflowControllerDataProvider.class);
    when(cache.getExistsLiveInstanceOrCurrentStateOrMessageChange()).thenReturn(assignmentChanged);
    when(cache.getDisabledInstances()).thenReturn(Collections.emptySet());
    when(cache.getIdealStates()).thenReturn(Collections.emptyMap());
    TaskAssignmentCalculator calculator = mock(TaskAssignmentCalculator.class);
    Set<Integer> eligible = targeted && assignmentChanged ? allPartitions : Collections.emptySet();
    when(calculator.getTaskAssignment(currentState, liveInstances, jobConfig, jobContext,
        workflowConfig, workflowContext, eligible, Collections.emptyMap()))
        .thenReturn(targetAssignments);

    AbstractTaskDispatcher dispatcher = new AbstractTaskDispatcher() { };
    dispatcher.handleAdditionalTaskAssignment(currentAssignments, Collections.emptySet(), "job",
        currentState, jobContext, jobConfig, workflowConfig, workflowContext, cache,
        assignedPartitions, assignments, Collections.emptySet(), calculator, allPartitions,
        System.currentTimeMillis(), liveInstances);

    TaskPartitionState expected =
        targeted && assignmentChanged ? TaskPartitionState.DROPPED : TaskPartitionState.RUNNING;
    Assert.assertEquals(assignments.size(), 1);
    Assert.assertEquals(assignments.get(0)._instance, "old");
    Assert.assertEquals(assignments.get(0)._state, expected.name());
    Assert.assertEquals(jobContext.getPartitionState(0), expected);
    Assert.assertEquals(jobContext.getAssignedParticipant(0), "old");
    Assert.assertEquals(jobConfig.getRecord(), originalConfig);
    verify(calculator).getTaskAssignment(currentState, liveInstances, jobConfig, jobContext,
        workflowConfig, workflowContext, eligible, Collections.emptyMap());
  }
}
