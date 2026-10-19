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
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import com.google.common.collect.Sets;
import org.apache.helix.ExternalViewChangeListener;
import org.apache.helix.NotificationContext;
import org.apache.helix.PropertyKey;
import org.apache.helix.TestHelper;
import org.apache.helix.model.ExternalView;
import org.apache.helix.model.ResourceConfig;
import org.apache.helix.task.JobConfig;
import org.apache.helix.task.JobQueue;
import org.apache.helix.task.TaskState;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.Assert;
import org.testng.annotations.Test;

public class TestDisableJobExternalView extends TaskTestBase {
  private static final Logger LOG = LoggerFactory.getLogger(TestDisableJobExternalView.class);

  /**
   * Jobs use contexts instead of ExternalViews, including requests containing the retired
   * DisableExternalView flag. Legacy inputs must not cause the flag to be persisted again.
   * @throws Exception
   */
  @Test
  public void testJobsDisableExternalView() throws Exception {
    String queueName = TestHelper.getTestMethodName();

    ExternviewChecker externviewChecker = new ExternviewChecker();
    _manager.addExternalViewChangeListener(externviewChecker);

    // Create a queue
    LOG.info("Starting job-queue: " + queueName);
    JobQueue.Builder queueBuilder = TaskTestUtil.buildJobQueue(queueName);

    JobConfig.Builder job1 = new JobConfig.Builder().setCommand(MockTask.TASK_COMMAND)
        .setTargetResource(WorkflowGenerator.DEFAULT_TGT_DB)
        .setTargetPartitionStates(Sets.newHashSet("SLAVE"));

    JobConfig.Builder job2 = JobConfig.Builder
        .fromMap(Collections.singletonMap("DisableExternalView", "true"))
        .setCommand(MockTask.TASK_COMMAND)
        .setTargetResource(WorkflowGenerator.DEFAULT_TGT_DB)
        .setTargetPartitionStates(Sets.newHashSet("SLAVE"));

    JobConfig.Builder job3 = JobConfig.Builder
        .fromMap(Collections.singletonMap("DisableExternalView", "false"))
        .setCommand(MockTask.TASK_COMMAND)
        .setTargetResource(WorkflowGenerator.DEFAULT_TGT_DB)
        .setTargetPartitionStates(Sets.newHashSet("MASTER"));

    // enqueue jobs
    queueBuilder.enqueueJob("job1", job1);
    queueBuilder.enqueueJob("job2", job2);
    queueBuilder.enqueueJob("job3", job3);

    _driver.createQueue(queueBuilder.build());

    // ensure all jobs are completed
    String namedSpaceJob3 = String.format("%s_%s", queueName, "job3");
    _driver.pollForJobState(queueName, namedSpaceJob3, TaskState.COMPLETED);

    Set<String> seenExternalViews = externviewChecker.getSeenExternalViews();
    String namedSpaceJob1 = String.format("%s_%s", queueName, "job1");
    String namedSpaceJob2 = String.format("%s_%s", queueName, "job2");

    Assert.assertTrue(!seenExternalViews.contains(namedSpaceJob1),
        "ExternalView found for " + namedSpaceJob1 + ". Jobs shouldn't be in EV!");
    Assert.assertTrue(!seenExternalViews.contains(namedSpaceJob2),
        "External View for " + namedSpaceJob2 + " shoudld not exist!");
    Assert.assertTrue(!seenExternalViews.contains(namedSpaceJob3),
        "ExternalView found for " + namedSpaceJob3 + ". Jobs shouldn't be in EV!");

    PropertyKey.Builder keyBuilder = new PropertyKey.Builder(CLUSTER_NAME);
    for (String job : new String[] {namedSpaceJob1, namedSpaceJob2, namedSpaceJob3}) {
      ResourceConfig storedJob =
          _manager.getHelixDataAccessor().getProperty(keyBuilder.resourceConfig(job));
      Assert.assertNotNull(storedJob);
      Assert.assertFalse(storedJob.getRecord().getSimpleFields().containsKey("DisableExternalView"));
      Assert.assertNull(_manager.getHelixDataAccessor().getProperty(keyBuilder.externalView(job)));
    }

    _manager
        .removeListener(new PropertyKey.Builder(CLUSTER_NAME).externalViews(), externviewChecker);
  }

  private static class ExternviewChecker implements ExternalViewChangeListener {
    private Set<String> _seenExternalViews = new HashSet<String>();

    @Override public void onExternalViewChange(List<ExternalView> externalViewList,
        NotificationContext changeContext) {
      for (ExternalView view : externalViewList) {
        _seenExternalViews.add(view.getResourceName());
      }
    }

    public Set<String> getSeenExternalViews() {
      return _seenExternalViews;
    }
  }
}
