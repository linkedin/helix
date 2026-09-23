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

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import org.apache.helix.HelixProperty;
import org.apache.helix.zookeeper.datamodel.ZNRecord;
import org.testng.Assert;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

public class TestJobConfigValidation {

  @Test public void testJobConfigValidation() {
    new JobConfig.Builder().setCommand("Dummy").setNumberOfTasks(123).setWorkflow("Workflow")
        .build();
  }

  @Test(expectedExceptions = IllegalArgumentException.class)
  public void testJobConfigWithoutAnyTaskSet() {
    new JobConfig.Builder().setWorkflow("Workflow").build();
  }

  @Test(expectedExceptions = IllegalArgumentException.class)
  public void testJobConfigCommandWithoutNumOfTask() {
    new JobConfig.Builder().setWorkflow("Workflow").setCommand("Dummy").build();
  }

  @DataProvider(name = "legacyAssignmentStrategyJobs")
  public Object[][] legacyAssignmentStrategyJobs() {
    return new Object[][] {
        {null, true},
        {"Resource", false}
    };
  }

  @Test(dataProvider = "legacyAssignmentStrategyJobs")
  public void testLegacyAssignmentStrategyIgnored(String targetResource, boolean genericJob) {
    JobConfig original = new JobConfig.Builder().setWorkflow("Workflow").setJobId("Job")
        .setCommand("Dummy").setNumberOfTasks(1).setTargetResource(targetResource).build();
    Map<String, String> config = new HashMap<>(original.getRecord().getSimpleFields());
    JobConfig expected = JobConfig.Builder.fromMap(config)
        .addTaskConfigMap(original.getTaskConfigMap()).build();

    config.put("AssignmentStrategy", "legacy.AssignmentStrategy");
    JobConfig actual = JobConfig.Builder.fromMap(config)
        .addTaskConfigMap(original.getTaskConfigMap()).build();

    Assert.assertEquals(actual.getRecord(), expected.getRecord());
    Assert.assertFalse(actual.getRecord().getSimpleFields().containsKey("AssignmentStrategy"));
    Assert.assertEquals(TaskUtil.isGenericTaskJob(actual), genericJob);
  }

  @DataProvider
  public Object[][] legacyExternalViewFlags() {
    return new Object[][] {
        {false, null}, {false, "true"}, {false, "false"},
        {true, null}, {true, "true"}, {true, "false"}
    };
  }

  @Test(dataProvider = "legacyExternalViewFlags")
  public void testLegacyExternalViewFlagIgnored(boolean targeted, String legacyFlag) {
    JobConfig.Builder builder = new JobConfig.Builder().setWorkflow("workflow").setJobId("job")
        .setCommand("Dummy").setTimeoutPerTask(1000L).setTaskRetryDelay(50L)
        .setNumConcurrentTasksPerInstance(2).setMaxAttemptsPerTask(3)
        .setIgnoreDependentJobFailure(true);
    if (targeted) {
      builder.setTargetResource("database")
          .setTargetPartitions(Collections.singletonList("database_0"))
          .setTargetPartitionStates(Collections.singleton("SLAVE"));
    } else {
      builder.setNumberOfTasks(2);
    }
    JobConfig expected = builder.build();
    Assert.assertFalse(expected.getRecord().getSimpleFields().containsKey("DisableExternalView"));

    ZNRecord legacyRecord = new ZNRecord(expected.getRecord());
    if (legacyFlag != null) {
      legacyRecord.setSimpleField("DisableExternalView", legacyFlag);
    }
    ZNRecord originalRecord = new ZNRecord(legacyRecord);
    JobConfig parsed = JobConfig.Builder.fromMap(legacyRecord.getSimpleFields())
        .addTaskConfigMap(expected.getTaskConfigMap()).build();

    Assert.assertEquals(parsed.getRecord(), expected.getRecord());
    Assert.assertEquals(TaskUtil.isGenericTaskJob(parsed), !targeted);

    JobConfig wrapped = new JobConfig(new HelixProperty(legacyRecord));
    JobConfig copied = new JobConfig("copy", wrapped);
    Assert.assertEquals(copied.getRecord(), new JobConfig("copy", expected).getRecord());
    Assert.assertFalse(copied.getRecord().getSimpleFields().containsKey("DisableExternalView"));
    Assert.assertEquals(legacyRecord, originalRecord, "Reading legacy records must not rewrite them");
  }
}
