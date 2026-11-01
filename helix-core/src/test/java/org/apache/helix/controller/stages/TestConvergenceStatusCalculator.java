package org.apache.helix.controller.stages;

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
import java.util.Collections;
import java.util.EnumMap;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.UUID;

import org.apache.helix.HelixRebalanceException;
import org.apache.helix.controller.rebalancer.waged.WagedRebalanceStatus;
import org.apache.helix.model.ConvergenceStatus;
import org.apache.helix.model.ConvergenceStatus.Reason;
import org.apache.helix.model.ConvergenceStatus.Status;
import org.apache.helix.model.Message;
import org.apache.helix.model.Partition;
import org.apache.helix.model.Resource;
import org.testng.Assert;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

public class TestConvergenceStatusCalculator {
  private static final String DB = "db";

  private Resource _resource;
  private CurrentStateOutput _current;
  private BestPossibleStateOutput _target;
  private Map<String, Resource> _resources;

  @BeforeMethod
  public void reset() {
    _resource = new Resource(DB);
    _current = new CurrentStateOutput();
    _target = new BestPossibleStateOutput();
    _resources = new HashMap<>();
  }

  private TestConvergenceStatusCalculator partition(String name, String[] cur, String[] tgt) {
    _resource.addPartition(name);
    Partition p = _resource.getPartition(name);
    for (int i = 0; i + 1 < cur.length; i += 2) {
      _current.setCurrentState(DB, p, cur[i], cur[i + 1]);
    }
    Map<String, String> target = new HashMap<>();
    for (int i = 0; i + 1 < tgt.length; i += 2) {
      target.put(tgt[i], tgt[i + 1]);
    }
    _target.setState(DB, p, target);
    return this;
  }

  private ConvergenceStatusCalculator.Inputs inputs() {
    _resource.setStateModelDefRef("MasterSlave");
    _resources.put(DB, _resource);
    return new ConvergenceStatusCalculator.Inputs().clusterName("c").controllerName("controller_1")
        .resources(_resources).currentState(_current).bestPossible(_target)
        .liveInstances(new HashSet<>(Arrays.asList("i1", "i2")))
        .initialStates(Collections.singletonMap(DB, "OFFLINE"));
  }

  private ConvergenceStatus run(ConvergenceStatusCalculator.Inputs in) {
    return ConvergenceStatusCalculator.calculate(in, "s1", 1000L).getResources().get(DB);
  }

  private static Message message() {
    return new Message(Message.MessageType.STATE_TRANSITION, UUID.randomUUID().toString());
  }

  @Test
  public void testConvergedIgnoresDroppedAndInitialState() {
    partition("db_0", new String[] {"i1", "MASTER", "i2", "SLAVE"},
        new String[] {"i1", "MASTER", "i2", "SLAVE"});
    // OFFLINE is the initial state and DROPPED is gone, so neither is a difference.
    partition("db_1", new String[] {"i1", "MASTER", "i2", "OFFLINE"},
        new String[] {"i1", "MASTER", "i2", "DROPPED"});
    ConvergenceStatus report = run(inputs());
    Assert.assertEquals(report.getStatus(), Status.CONVERGED);
    Assert.assertEquals(report.getPrimaryReason(), Reason.NONE);
    Assert.assertEquals(report.getPartitionCount(), 2);
    Assert.assertEquals(report.getControllerSessionId(), "s1");
    Assert.assertEquals(report.getControllerName(), "controller_1");
    Assert.assertEquals(report.getGeneratedAt(), 1000L);
  }

  @Test
  public void testClassifiesUnconvergedPartitions() {
    String[] none = {};
    partition("pending", new String[] {"i1", "SLAVE"}, new String[] {"i1", "MASTER"});
    _current.setPendingMessage(DB, _resource.getPartition("pending"), "i1", message());
    partition("error", new String[] {"i1", "ERROR"}, new String[] {"i1", "MASTER"});
    partition("down", new String[] {"i1", "SLAVE"}, new String[] {"i1", "SLAVE", "i3", "MASTER"});
    partition("stuck", new String[] {"i1", "SLAVE"}, new String[] {"i1", "MASTER"});
    partition("failed", new String[] {"i1", "SLAVE"}, new String[] {"i1", "MASTER"});
    partition("fresh", none, new String[] {"i1", "MASTER"});

    MessageOutput all = new MessageOutput();
    MessageOutput selected = new MessageOutput();
    MessageOutput throttled = new MessageOutput();
    Partition fresh = _resource.getPartition("fresh");
    all.addMessage(DB, fresh, message());
    selected.addMessage(DB, fresh, message());
    throttled.addMessage(DB, fresh, message());
    Message failed = message();
    failed.setResourceName(DB);
    failed.setPartitionName("failed");

    ConvergenceStatus report = run(inputs().messages(all, selected, throttled)
        .failedMessages(Collections.singletonList(failed)));

    Map<Reason, Integer> expected = new EnumMap<>(Reason.class);
    for (Reason reason : Arrays.asList(Reason.PENDING_TRANSITION, Reason.ERROR_STATE,
        Reason.TARGET_INSTANCE_NOT_LIVE, Reason.NO_PROGRESS_PATH, Reason.MESSAGE_DISPATCH_FAILED,
        Reason.TRANSITION_DISPATCHED)) {
      expected.put(reason, 1);
    }
    Assert.assertEquals(report.getReasonCounts(), expected);
    Assert.assertEquals(report.getStatusCounts().get(Status.IN_PROGRESS).intValue(), 2);
    Assert.assertEquals(report.getStatusCounts().get(Status.BLOCKED).intValue(), 4);
    Assert.assertEquals(report.getStatus(), Status.BLOCKED);
    Assert.assertEquals(report.getAffectedPartitionCount(), 6);
    // The most urgent reason among the BLOCKED partitions.
    Assert.assertEquals(report.getPrimaryReason(), Reason.ERROR_STATE);
  }

  @Test
  public void testThrottledAndConstraintWait() {
    partition("a", new String[] {"i1", "SLAVE"}, new String[] {"i1", "MASTER"});
    partition("b", new String[] {"i1", "SLAVE"}, new String[] {"i1", "MASTER"});
    MessageOutput all = new MessageOutput();
    MessageOutput selected = new MessageOutput();
    all.addMessage(DB, _resource.getPartition("a"), message());
    selected.addMessage(DB, _resource.getPartition("a"), message());
    all.addMessage(DB, _resource.getPartition("b"), message());

    ConvergenceStatus report = run(inputs().messages(all, selected, new MessageOutput()));
    Map<Reason, Integer> reasons = report.getReasonCounts();
    Assert.assertEquals(reasons.get(Reason.MESSAGE_THROTTLED).intValue(), 1);
    Assert.assertEquals(reasons.get(Reason.STATE_CONSTRAINT_WAIT).intValue(), 1);
    Assert.assertEquals(report.getStatus(), Status.IN_PROGRESS);
  }

  @Test
  public void testDelayedResourceWaitsInsteadOfBlocking() {
    partition("db_0", new String[] {"i1", "SLAVE"}, new String[] {"i1", "SLAVE", "i3", "MASTER"});
    ConvergenceStatus report =
        run(inputs().delayedResources(Collections.singleton(DB)));
    Assert.assertEquals(report.getStatus(), Status.IN_PROGRESS);
    Assert.assertEquals(report.getPrimaryReason(), Reason.WAITING_FOR_DELAY);
  }

  @Test
  public void testDelayWindowNodeInPreferenceListIsNotConverged() {
    partition("db_0", new String[] {"i1", "MASTER"}, new String[] {"i1", "MASTER"});
    _target.setPreferenceList(DB, "db_0", Arrays.asList("i1", "i3"));
    ConvergenceStatus report = run(inputs().delayedResources(Collections.singleton(DB)));
    Assert.assertEquals(report.getStatus(), Status.IN_PROGRESS);
    Assert.assertEquals(report.getPrimaryReason(), Reason.WAITING_FOR_DELAY);

    Assert.assertEquals(run(inputs()).getStatus(), Status.CONVERGED);
    _target.setPreferenceList(DB, "db_0", Arrays.asList("i1", "i2"));
    Assert.assertEquals(run(inputs().delayedResources(Collections.singleton(DB))).getStatus(),
        Status.CONVERGED);
  }

  @Test
  public void testMissingTargetAndInvalidState() {
    partition("db_0", new String[] {"i1", "MASTER"}, new String[] {"i1", "MASTER"});
    _resource.addPartition("db_1");
    ConvergenceStatus report = run(inputs());
    Assert.assertEquals(report.getStatus(), Status.UNKNOWN);
    Assert.assertEquals(report.getPrimaryReason(), Reason.TARGET_ASSIGNMENT_MISSING);
    Assert.assertEquals(report.getStatusCounts().get(Status.CONVERGED).intValue(), 1);

    // No best possible state at all: nothing can be judged.
    ConvergenceStatus noTarget = run(inputs().bestPossible(null));
    Assert.assertEquals(noTarget.getStatusCounts().get(Status.UNKNOWN).intValue(), 2);
  }

  @Test
  public void testPausedReportsEveryPartitionWithoutComparing() {
    partition("db_0", new String[] {"i1", "SLAVE"}, new String[] {"i1", "MASTER"});
    partition("db_1", new String[] {"i1", "MASTER"}, new String[] {"i1", "MASTER"});
    ConvergenceStatusCalculator.Result result = ConvergenceStatusCalculator
        .calculate(inputs().pausedReason(Reason.MAINTENANCE_MODE), "s1", 1L);
    ConvergenceStatus report = result.getResources().get(DB);
    Assert.assertEquals(report.getStatus(), Status.PAUSED);
    Assert.assertEquals(report.getStatusCounts().get(Status.PAUSED).intValue(), 2);
    Assert.assertEquals(result.getCluster().getPrimaryReason(), Reason.MAINTENANCE_MODE);
    Assert.assertEquals(report.getAffectedPartitionCount(), 0);
    // The cluster record carries the pause even when there is nothing to count.
    ConvergenceStatus empty = ConvergenceStatusCalculator.calculate(new ConvergenceStatusCalculator
        .Inputs().clusterName("c").pausedReason(Reason.MAINTENANCE_MODE), "s1", 1L).getCluster();
    Assert.assertEquals(empty.getStatus(), Status.PAUSED);
    Assert.assertEquals(empty.getPrimaryReason(), Reason.MAINTENANCE_MODE);
  }

  @Test
  public void testWagedLastKnownGoodMakesTheTargetUntrusted() {
    partition("db_0", new String[] {"i1", "MASTER"}, new String[] {"i1", "MASTER"});
    partition("db_1", new String[] {"i1", "MASTER"}, new String[] {"i1", "MASTER"});
    WagedRebalanceStatus fallback = new WagedRebalanceStatus(true, false, true,
        HelixRebalanceException.FailureCategory.CAPACITY_DEFICIT, true);
    ConvergenceStatusCalculator.Result result = ConvergenceStatusCalculator.calculate(
        inputs().wagedResources(Collections.singleton(DB), fallback), "s1", 1L);
    ConvergenceStatus report = result.getResources().get(DB);
    Assert.assertEquals(report.getStatus(), Status.UNKNOWN);
    Assert.assertEquals(report.getPrimaryReason(), Reason.WAGED_CAPACITY_DEFICIT);
    Assert.assertEquals(report.getTargetFreshness(),
        ConvergenceStatus.TargetFreshness.LAST_KNOWN_GOOD);
    Assert.assertEquals(report.getOptimizerStatus(),
        ConvergenceStatus.OptimizerStatus.BASELINE_FAILED);
    Assert.assertEquals(result.getCluster().getStatus(), Status.UNKNOWN);

    // A new leader that has not finished a run has no verified target either.
    WagedRebalanceStatus fresh = new WagedRebalanceStatus(false, false, false, null, false);
    report = run(inputs().wagedResources(Collections.singleton(DB), fresh));
    Assert.assertEquals(report.getPrimaryReason(), Reason.TARGET_ASSIGNMENT_MISSING);
    Assert.assertEquals(report.getOptimizerStatus(), ConvergenceStatus.OptimizerStatus.UNKNOWN);
  }

  @Test
  public void testTaskResourcesAreSkipped() {
    Resource task = new Resource("workflow");
    task.setStateModelDefRef("Task");
    partition("db_0", new String[] {"i1", "MASTER"}, new String[] {"i1", "MASTER"});
    ConvergenceStatusCalculator.Inputs in = inputs();
    _resources.put("workflow", task);
    ConvergenceStatusCalculator.Result result = ConvergenceStatusCalculator.calculate(in, "s", 1L);
    Assert.assertEquals(result.getResources().keySet(), Collections.singleton(DB));
    Assert.assertEquals(result.getCluster().getResourceCount(), 1);
  }

  @Test
  public void testCountsAreExactAndStable() {
    for (int i = 0; i < 500; i++) {
      String name = String.format("db_%03d", i);
      // Every tenth partition is blocked, the rest are in progress.
      boolean blocked = i % 10 == 0;
      partition(name, new String[] {"i1", "SLAVE"},
          blocked ? new String[] {"i1", "SLAVE", "i3", "MASTER"} : new String[] {"i1", "MASTER"});
      if (!blocked) {
        _current.setPendingMessage(DB, _resource.getPartition(name), "i1", message());
      }
    }
    ConvergenceStatusCalculator.Result result =
        ConvergenceStatusCalculator.calculate(inputs(), "s1", 1L);
    for (ConvergenceStatus report : Arrays.asList(result.getResources().get(DB),
        result.getCluster())) {
      Assert.assertEquals(report.getPartitionCount(), 500);
      Assert.assertEquals(report.getStatusCounts().get(Status.BLOCKED).intValue(), 50);
      Assert.assertEquals(report.getStatusCounts().get(Status.IN_PROGRESS).intValue(), 450);
      Assert.assertEquals(report.getPrimaryReason(), Reason.TARGET_INSTANCE_NOT_LIVE);
    }
    // Same input, same report apart from the timestamp.
    Assert.assertTrue(ConvergenceStatusCalculator.calculate(inputs(), "s1", 2L).getCluster()
        .semanticallyEquals(result.getCluster()));
  }

  @Test
  public void testLargeResourceKeepsTheRecordSmall() {
    int partitions = 100_000;
    for (int i = 0; i < partitions; i++) {
      String name = "db_" + i;
      boolean converged = i % 100 != 0;
      partition(name, new String[] {"i1", "MASTER", "i2", "SLAVE"},
          converged ? new String[] {"i1", "MASTER", "i2", "SLAVE"}
              : new String[] {"i1", "SLAVE", "i2", "MASTER"});
    }
    ConvergenceStatus report = run(inputs());
    Assert.assertEquals(report.getPartitionCount(), partitions);
    Assert.assertEquals(report.getStatusCounts().get(Status.CONVERGED).intValue(), 99_000);
    Assert.assertEquals(report.getStatusCounts().get(Status.BLOCKED).intValue(), 1_000);
    Assert.assertTrue(report.getRecord().toString().length() < 2_000);
  }
}
