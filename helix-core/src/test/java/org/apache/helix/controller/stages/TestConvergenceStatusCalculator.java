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
import java.util.Set;
import java.util.UUID;

import org.apache.helix.HelixRebalanceException;
import org.apache.helix.controller.dataproviders.BaseControllerDataProvider;
import org.apache.helix.controller.rebalancer.DelayedAutoRebalancer;
import org.apache.helix.controller.rebalancer.waged.WagedRebalanceStatus;
import org.apache.helix.model.BuiltInStateModelDefinitions;
import org.apache.helix.model.ClusterConfig;
import org.apache.helix.model.ConvergenceStatus;
import org.apache.helix.model.ConvergenceStatus.Reason;
import org.apache.helix.model.ConvergenceStatus.Status;
import org.apache.helix.model.IdealState;
import org.apache.helix.model.InstanceConfig;
import org.apache.helix.model.LiveInstance;
import org.apache.helix.model.Message;
import org.apache.helix.model.Partition;
import org.apache.helix.model.Resource;
import org.apache.helix.model.StateModelDefinition;
import org.mockito.Mockito;
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
    // The controller keeps an ERROR replica in ERROR in its target.
    partition("error", new String[] {"i1", "ERROR"}, new String[] {"i1", "ERROR"});
    partition("down", new String[] {"i1", "SLAVE"}, new String[] {"i1", "SLAVE", "i3", "MASTER"});
    partition("stuck", new String[] {"i1", "SLAVE"}, new String[] {"i1", "MASTER"});
    partition("failed", new String[] {"i1", "SLAVE"}, new String[] {"i1", "MASTER"});
    partition("fresh", none, new String[] {"i1", "MASTER"});
    partition("throttled", new String[] {"i1", "SLAVE"}, new String[] {"i1", "MASTER"});
    partition("waiting", new String[] {"i1", "SLAVE"}, new String[] {"i1", "MASTER"});

    MessageOutput all = new MessageOutput();
    MessageOutput selected = new MessageOutput();
    MessageOutput throttled = new MessageOutput();
    Partition fresh = _resource.getPartition("fresh");
    all.addMessage(DB, fresh, message());
    selected.addMessage(DB, fresh, message());
    throttled.addMessage(DB, fresh, message());
    all.addMessage(DB, _resource.getPartition("throttled"), message());
    selected.addMessage(DB, _resource.getPartition("throttled"), message());
    all.addMessage(DB, _resource.getPartition("waiting"), message());
    Message failed = message();
    failed.setResourceName(DB);
    failed.setPartitionName("failed");

    ConvergenceStatus report = run(inputs().messages(all, selected, throttled)
        .failedMessages(Collections.singletonList(failed)));

    Map<Reason, Integer> expected = new EnumMap<>(Reason.class);
    for (Reason reason : Arrays.asList(Reason.PENDING_TRANSITION, Reason.ERROR_STATE,
        Reason.TARGET_INSTANCE_NOT_LIVE, Reason.NO_PROGRESS_PATH, Reason.MESSAGE_DISPATCH_FAILED,
        Reason.TRANSITION_DISPATCHED, Reason.MESSAGE_THROTTLED, Reason.STATE_CONSTRAINT_WAIT)) {
      expected.put(reason, 1);
    }
    Assert.assertEquals(report.getReasonCounts(), expected);
    Assert.assertEquals(report.getStatusCounts().get(Status.IN_PROGRESS).intValue(), 4);
    Assert.assertEquals(report.getStatusCounts().get(Status.BLOCKED).intValue(), 4);
    Assert.assertEquals(report.getStatus(), Status.BLOCKED);
    Assert.assertEquals(report.getAffectedPartitionCount(), 8);
    // The most urgent reason among the BLOCKED partitions.
    Assert.assertEquals(report.getPrimaryReason(), Reason.ERROR_STATE);
  }

  @Test
  public void testDelayWindowNodeInPreferenceListIsNotConverged() {
    // A target on an offline node waits for the delay instead of blocking.
    partition("db_1", new String[] {"i1", "SLAVE"}, new String[] {"i1", "SLAVE", "i3", "MASTER"});
    partition("db_0", new String[] {"i1", "MASTER"}, new String[] {"i1", "MASTER"});
    _target.setPreferenceList(DB, "db_0", Arrays.asList("i1", "i3"));
    ConvergenceStatus report = run(inputs().delayedResources(Collections.singleton(DB)));
    Assert.assertEquals(report.getStatus(), Status.IN_PROGRESS);
    Assert.assertEquals(report.getPrimaryReason(), Reason.WAITING_FOR_DELAY);

    _target.setState(DB, _resource.getPartition("db_1"), Collections.singletonMap("i1", "SLAVE"));
    Assert.assertEquals(run(inputs()).getStatus(), Status.CONVERGED);
    _target.setPreferenceList(DB, "db_0", Arrays.asList("i1", "i2"));
    Assert.assertEquals(run(inputs().delayedResources(Collections.singleton(DB))).getStatus(),
        Status.CONVERGED);
    // A disabled node that is still live is held the same way.
    Assert.assertEquals(run(inputs().delayedResources(Collections.singleton(DB))
        .disabledInstances(Collections.singleton("i2"))).getReasonCounts()
        .get(Reason.WAITING_FOR_DELAY).intValue(), 1);
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
  public void testWagedHealthChangeIsAppliedToTheKeptCounts() {
    partition("db_0", new String[] {"i1", "MASTER"}, new String[] {"i1", "MASTER"});
    partition("db_1", new String[] {"i1", "SLAVE"}, new String[] {"i1", "MASTER"});
    WagedRebalanceStatus healthy = new WagedRebalanceStatus(false, false, false, null, true);
    ConvergenceStatusCalculator.Result first = ConvergenceStatusCalculator.calculate(
        inputs().wagedResources(Collections.singleton(DB), healthy), "s1", 1L);
    Assert.assertTrue(first.hasWagedResources());
    WagedRebalanceStatus failing = new WagedRebalanceStatus(false, true, false,
        HelixRebalanceException.FailureCategory.CAPACITY_DEFICIT, true);
    ConvergenceStatus failed = ConvergenceStatusCalculator.withWagedStatus(first, failing, 2L)
        .getResources().get(DB);
    Assert.assertEquals(failed.getStatus(), Status.UNKNOWN);
    Assert.assertEquals(failed.getPrimaryReason(), Reason.WAGED_CAPACITY_DEFICIT);
    Assert.assertEquals(failed.getPartitionCount(), 2);
    // Back to healthy: the counts were kept as computed, not as overridden.
    ConvergenceStatus back = ConvergenceStatusCalculator.withWagedStatus(
        ConvergenceStatusCalculator.withWagedStatus(first, failing, 2L), healthy, 3L)
        .getResources().get(DB);
    Assert.assertEquals(back.getStatusCounts(),
        first.getResources().get(DB).getStatusCounts());
  }

  @Test
  public void testErrorReplicaStaysBlockedWithoutATrustedTarget() {
    partition("db_0", new String[] {"i1", "ERROR"}, new String[] {"i1", "ERROR"});
    partition("db_1", new String[] {"i1", "MASTER"}, new String[] {"i1", "MASTER"});
    ConvergenceStatus report = run(inputs().wagedResources(Collections.singleton(DB),
        new WagedRebalanceStatus(false, false, false, null, false)));
    Assert.assertEquals(report.getStatusCounts().toString(), "{BLOCKED=1, UNKNOWN=1}");
    Assert.assertEquals(report.getReasonCounts().get(Reason.ERROR_STATE).intValue(), 1);
  }

  private ConvergenceStatusCalculator.Inputs expecting(int replicas, String... swap) {
    return inputs().expectedReplicas(Collections.singletonMap(DB, replicas),
        Collections.emptyMap(), new HashSet<>(Arrays.asList(swap)));
  }

  @Test
  public void testMatchingATargetThatIsShortOfReplicasIsNotConverged() {
    // i3 is dead, so the rebalancer leaves it out of the target and current matches it.
    partition("db_0", new String[] {"i1", "MASTER", "i2", "SLAVE"},
        new String[] {"i1", "MASTER", "i2", "SLAVE"});
    _target.setPreferenceList(DB, "db_0", Arrays.asList("i1", "i2", "i3"));
    // Every listed node is dead: target and current are both empty.
    partition("db_1", new String[] {}, new String[] {});
    _target.setPreferenceList(DB, "db_1", Collections.emptyList());
    ConvergenceStatus report = run(expecting(3));
    Assert.assertEquals(report.getStatus(), Status.BLOCKED);
    Assert.assertEquals(report.getReasonCounts().get(Reason.TARGET_INSTANCE_NOT_LIVE).intValue(),
        1);
    Assert.assertEquals(report.getReasonCounts().get(Reason.UNDER_REPLICATED).intValue(), 1);
    Assert.assertEquals(run(expecting(2)).getStatusCounts().get(Status.CONVERGED).intValue(), 1);
  }

  @Test
  public void testDisabledAndDelayedReplicasAreNotMissing() {
    partition("db_0", new String[] {"i1", "MASTER", "i2", "SLAVE"},
        new String[] {"i1", "MASTER", "i2", "SLAVE"});
    _target.setPreferenceList(DB, "db_0", Arrays.asList("i1", "i2", "i3"));
    // i3 has db_0 disabled by an operator, so two replicas is all that is asked for.
    Map<String, Set<String>> byPartition =
        Collections.singletonMap("db_0", Collections.singleton("i3"));
    Assert.assertEquals(run(expecting(3).placement(Collections.emptySet(), Collections.emptyMap(),
        Collections.singletonMap(DB, byPartition))).getStatus(), Status.CONVERGED);
    // A node of this resource is inside its delay window and min active is 2: the rebalancer
    // keeps two replicas and waits, so the third is held, not missing.
    ConvergenceStatus waiting = run(expecting(3).placement(Collections.emptySet(),
        Collections.singletonMap(DB, 2), Collections.emptyMap()));
    Assert.assertEquals(waiting.getStatus(), Status.IN_PROGRESS);
    Assert.assertEquals(waiting.getPrimaryReason(), Reason.WAITING_FOR_DELAY);
    // Below min active the rebalancer does not wait, so the replica it could not place is missing.
    ConvergenceStatus missing = run(expecting(3).placement(Collections.emptySet(),
        Collections.singletonMap(DB, 3), Collections.emptyMap()));
    Assert.assertEquals(missing.getStatus(), Status.BLOCKED);
    Assert.assertEquals(missing.getPrimaryReason(), Reason.TARGET_INSTANCE_NOT_LIVE);
    // SEMI_AUTO places as many replicas as the list names.
    _target.setPreferenceList(DB, "db_0", Arrays.asList("i1", "i2"));
    Assert.assertEquals(run(expecting(3).placement(Collections.singleton(DB),
        Collections.emptyMap(), Collections.emptyMap())).getStatus(), Status.CONVERGED);
  }

  @Test
  public void testOnlyANodeTheResourceCanUseHoldsItForTheDelay() {
    long now = System.currentTimeMillis();
    Map<String, InstanceConfig> configs = new HashMap<>();
    for (String name : Arrays.asList("A", "B", "C", "D")) {
      configs.put(name, new InstanceConfig(name));
    }
    BaseControllerDataProvider cache = Mockito.mock(BaseControllerDataProvider.class);
    Mockito.when(cache.getClusterConfig()).thenReturn(new ClusterConfig("c"));
    Mockito.when(cache.getInstanceConfigMap()).thenReturn(configs);
    Map<String, LiveInstance> live = new HashMap<>();
    live.put("A", new LiveInstance("A"));
    live.put("B", new LiveInstance("B"));
    Mockito.when(cache.getLiveInstances()).thenReturn(live);
    // C left for good a minute ago; D, which only OTHER_DB can use, went down a second ago.
    Map<String, Long> offline = new HashMap<>();
    offline.put("C", now - 60_000L);
    offline.put("D", now - 1_000L);
    Mockito.when(cache.getInstanceOfflineTimeMap()).thenReturn(offline);
    Mockito.when(cache.getAssignableInstancesWithTag("db"))
        .thenReturn(new HashSet<>(Arrays.asList("A", "B", "C")));
    Mockito.when(cache.getAssignableInstancesWithTag("other"))
        .thenReturn(new HashSet<>(Arrays.asList("A", "D")));
    Mockito.when(cache.getStateModelDef("MasterSlave"))
        .thenReturn(BuiltInStateModelDefinitions.MasterSlave.getStateModelDefinition());
    Map<String, Resource> resources = new HashMap<>();
    for (String[] spec : new String[][] {{DB, "db"}, {"OTHER_DB", "other"}}) {
      IdealState idealState = new IdealState(spec[0]);
      idealState.setRebalanceMode(IdealState.RebalanceMode.FULL_AUTO);
      idealState.setRebalancerClassName(DelayedAutoRebalancer.class.getName());
      idealState.setStateModelDefRef("MasterSlave");
      idealState.setReplicas("3");
      idealState.setInstanceGroupTag(spec[1]);
      idealState.setDelayRebalanceEnabled(true);
      idealState.setRebalanceDelay(12_000L);
      Mockito.when(cache.getIdealState(spec[0])).thenReturn(idealState);
      Resource resource = new Resource(spec[0]);
      resource.setStateModelDefRef("MasterSlave");
      resources.put(spec[0], resource);
    }
    ClusterEvent event = new ClusterEvent("c", ClusterEventType.Unknown);
    event.addAttribute(AttributeName.RESOURCES_TO_REBALANCE.name(), resources);

    ConvergenceStatusCalculator.Inputs in =
        ConvergenceStatusCalculator.Inputs.from(event, cache, "c");
    Assert.assertEquals(in._waitingForDelay, Collections.singletonMap("OTHER_DB", 3));
    Assert.assertEquals(in._expectedReplicas.get(DB).intValue(), 3);
  }

  @Test
  public void testReplicaLeftOutsideThePreferenceListIsNotConverged() {
    // WAGED moved db_0 to i3, which had no capacity, so i2 still holds it.
    partition("db_0", new String[] {"i1", "MASTER", "i2", "SLAVE"},
        new String[] {"i1", "MASTER", "i2", "SLAVE"});
    _target.setPreferenceList(DB, "db_0", Arrays.asList("i1", "i3"));
    ConvergenceStatus report = run(expecting(2));
    Assert.assertEquals(report.getStatus(), Status.BLOCKED);
    Assert.assertEquals(report.getPrimaryReason(), Reason.OUTSIDE_PREFERENCE_LIST);
    // Not while i2 is being swapped out.
    Assert.assertEquals(run(expecting(2, "i2", "i3")).getStatus(), Status.CONVERGED);
  }

  @Test
  public void testCustomizedReplicaOnADeadNodeIsNotConverged() {
    partition("db_0", new String[] {"i1", "MASTER"}, new String[] {"i1", "MASTER"});
    IdealState idealState = new IdealState(DB);
    idealState.setRebalanceMode(IdealState.RebalanceMode.CUSTOMIZED);
    Map<String, String> wanted = new HashMap<>();
    wanted.put("i1", "MASTER");
    wanted.put("i3", "SLAVE");
    idealState.setInstanceStateMap("db_0", wanted);
    ConvergenceStatus report = run(inputs().expectedReplicas(Collections.emptyMap(),
        Collections.singletonMap(DB, idealState), Collections.emptySet()));
    Assert.assertEquals(report.getPrimaryReason(), Reason.TARGET_INSTANCE_NOT_LIVE);
  }

  @Test
  public void testMessageSentThisRunMeansNotConverged() {
    partition("db_0", new String[] {"i1", "OFFLINE"}, new String[] {"i1", "DROPPED"});
    MessageOutput all = new MessageOutput();
    all.addMessage(DB, _resource.getPartition("db_0"), message());
    Assert.assertEquals(run(inputs().messages(all, new MessageOutput(), new MessageOutput()))
        .getStatus(), Status.IN_PROGRESS);
  }

  @Test
  public void testExpectedReplicasFollowTheStateModel() {
    IdealState idealState = new IdealState(DB);
    idealState.setReplicas("3");
    StateModelDefinition masterSlave =
        BuiltInStateModelDefinitions.MasterSlave.getStateModelDefinition();
    Assert.assertEquals(
        ConvergenceStatusCalculator.Inputs.expectedReplicas(idealState, masterSlave, 10, 10), 3);
    // Capped by the instances it may be placed on, and by the fault zones.
    Assert.assertEquals(
        ConvergenceStatusCalculator.Inputs.expectedReplicas(idealState, masterSlave, 2, 10), 2);
    Assert.assertEquals(
        ConvergenceStatusCalculator.Inputs.expectedReplicas(idealState, masterSlave, 10, 2), 2);
    // An unbounded state such as BOOTSTRAP takes nothing away.
    Assert.assertEquals(ConvergenceStatusCalculator.Inputs.expectedReplicas(idealState,
        BuiltInStateModelDefinitions.OnlineOfflineWithBootstrap.getStateModelDefinition(), 10, 10),
        3);
    idealState.setReplicas("ANY_LIVEINSTANCE");
    Assert.assertEquals(
        ConvergenceStatusCalculator.Inputs.expectedReplicas(idealState, masterSlave, 10, 10), -1);
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
