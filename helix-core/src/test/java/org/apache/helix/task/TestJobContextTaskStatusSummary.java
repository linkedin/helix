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

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.helix.zookeeper.datamodel.ZNRecord;
import org.testng.Assert;
import org.testng.annotations.Test;

/**
 * Deterministic unit test for {@link JobContext#updateTaskStatusSummary()}. It builds a JobContext
 * with a fixed mix of per-partition task states and asserts the aggregated summary buckets every
 * state correctly, without needing a live cluster. This is the same aggregation the job detail page
 * recomputes on demand from the per-partition states, so it locks the contract both consumers rely
 * on.
 */
public class TestJobContextTaskStatusSummary {
  private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

  @Test
  public void testSummaryBucketsEveryState() throws Exception {
    JobContext ctx = new JobContext(new ZNRecord("TestJob"));
    // A representative mix covering every summary bucket.
    ctx.setPartitionState(0, TaskPartitionState.COMPLETED);
    ctx.setPartitionState(1, TaskPartitionState.COMPLETED);
    ctx.setPartitionState(2, TaskPartitionState.COMPLETED);
    ctx.setPartitionState(3, TaskPartitionState.TASK_ERROR);
    ctx.setPartitionState(4, TaskPartitionState.TASK_ABORTED);
    ctx.setPartitionState(5, TaskPartitionState.TIMED_OUT);
    ctx.setPartitionState(6, TaskPartitionState.RUNNING);
    ctx.setPartitionState(7, TaskPartitionState.INIT);
    ctx.setPartitionState(8, TaskPartitionState.STOPPED);

    ctx.updateTaskStatusSummary();

    String summaryJson = ctx.getTaskStatusSummary();
    Assert.assertNotNull(summaryJson, "Summary should be populated after updateTaskStatusSummary().");
    JsonNode summary = OBJECT_MAPPER.readTree(summaryJson);

    Assert.assertEquals(summary.get("total").asInt(), 9);
    Assert.assertEquals(summary.get("completed").asInt(), 3);
    // failed = TASK_ERROR + TASK_ABORTED + TIMED_OUT
    Assert.assertEquals(summary.get("failed").asInt(), 3);
    // timedOut is the subset of failed that specifically timed out.
    Assert.assertEquals(summary.get("timedOut").asInt(), 1);
    Assert.assertEquals(summary.get("inProgress").asInt(), 1);
    Assert.assertEquals(summary.get("pending").asInt(), 1);
    // other = STOPPED only.
    Assert.assertEquals(summary.get("other").asInt(), 1);

    // The top-level counts partition the tasks.
    Assert.assertEquals(summary.get("completed").asInt() + summary.get("failed").asInt()
        + summary.get("inProgress").asInt() + summary.get("pending").asInt()
        + summary.get("other").asInt(), 9);

    assertIntList(summary.get("failedTasks"), 3, 4, 5);
    assertIntList(summary.get("timedOutTasks"), 5);
    assertIntList(summary.get("inProgressTasks"), 6);
    assertIntList(summary.get("pendingTasks"), 7);

    JsonNode byState = summary.get("byState");
    Assert.assertEquals(byState.get(TaskPartitionState.COMPLETED.name()).asInt(), 3);
    Assert.assertEquals(byState.get(TaskPartitionState.TASK_ERROR.name()).asInt(), 1);
    Assert.assertEquals(byState.get(TaskPartitionState.TASK_ABORTED.name()).asInt(), 1);
    Assert.assertEquals(byState.get(TaskPartitionState.TIMED_OUT.name()).asInt(), 1);
    Assert.assertEquals(byState.get(TaskPartitionState.RUNNING.name()).asInt(), 1);
    Assert.assertEquals(byState.get(TaskPartitionState.INIT.name()).asInt(), 1);
    Assert.assertEquals(byState.get(TaskPartitionState.STOPPED.name()).asInt(), 1);
  }

  @Test
  public void testSummaryOnAllCompletedJob() throws Exception {
    JobContext ctx = new JobContext(new ZNRecord("TestJob"));
    for (int p = 0; p < 4; p++) {
      ctx.setPartitionState(p, TaskPartitionState.COMPLETED);
    }
    ctx.updateTaskStatusSummary();

    JsonNode summary = OBJECT_MAPPER.readTree(ctx.getTaskStatusSummary());
    Assert.assertEquals(summary.get("total").asInt(), 4);
    Assert.assertEquals(summary.get("completed").asInt(), 4);
    Assert.assertEquals(summary.get("failed").asInt(), 0);
    Assert.assertEquals(summary.get("timedOut").asInt(), 0);
    Assert.assertEquals(summary.get("inProgress").asInt(), 0);
    Assert.assertEquals(summary.get("pending").asInt(), 0);
    Assert.assertEquals(summary.get("other").asInt(), 0);
    Assert.assertEquals(summary.get("failedTasks").size(), 0);
    Assert.assertEquals(summary.get("pendingTasks").size(), 0);
  }

  private static void assertIntList(JsonNode arrayNode, int... expected) {
    Assert.assertEquals(arrayNode.size(), expected.length);
    for (int i = 0; i < expected.length; i++) {
      Assert.assertEquals(arrayNode.get(i).asInt(), expected[i]);
    }
  }
}
