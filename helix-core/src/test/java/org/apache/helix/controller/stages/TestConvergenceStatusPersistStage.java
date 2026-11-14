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
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import org.apache.helix.HelixDataAccessor;
import org.apache.helix.HelixManager;
import org.apache.helix.PropertyKey;
import org.apache.helix.common.ZkTestBase;
import org.apache.helix.manager.zk.ZKHelixDataAccessor;
import org.apache.helix.manager.zk.ZkBaseDataAccessor;
import org.apache.helix.model.ConvergenceStatus;
import org.apache.helix.model.ConvergenceStatus.OptimizerStatus;
import org.apache.helix.model.ConvergenceStatus.Reason;
import org.apache.helix.model.ConvergenceStatus.Status;
import org.apache.helix.model.ConvergenceStatus.TargetFreshness;
import org.apache.helix.zookeeper.api.client.HelixZkClient;
import org.apache.helix.zookeeper.datamodel.ZNRecord;
import org.apache.helix.zookeeper.datamodel.serializer.ZNRecordSerializer;
import org.apache.helix.zookeeper.impl.factory.DedicatedZkClientFactory;
import org.mockito.Mockito;
import org.testng.Assert;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

public class TestConvergenceStatusPersistStage extends ZkTestBase {
  private static final long T0 = 1_000_000L;
  private String _cluster;
  private HelixDataAccessor _accessor;
  private PropertyKey.Builder _keys;
  private HelixManager _manager;
  private ConvergenceStatusPersistStage _stage;
  // The record is bound to the ZooKeeper session of the controller that wrote it.
  private String _session;

  @BeforeMethod
  public void setUp() {
    _cluster = "CONV_" + getShortClassName() + "_" + System.nanoTime();
    _gSetupTool.addCluster(_cluster, true);
    _accessor = new ZKHelixDataAccessor(_cluster, new ZkBaseDataAccessor<>(_gZkClient));
    _keys = _accessor.keyBuilder();
    _stage = new ConvergenceStatusPersistStage();
    _session = Long.toHexString(_gZkClient.getSessionId());
    _manager = manager(_session, true);
  }

  @AfterMethod
  public void tearDown() {
    deleteCluster(_cluster);
  }

  private HelixManager manager(String session, boolean leader) {
    return manager(session, leader, _accessor);
  }

  private HelixManager manager(String session, boolean leader, HelixDataAccessor accessor) {
    HelixManager manager = Mockito.mock(HelixManager.class);
    Mockito.when(manager.getHelixDataAccessor()).thenReturn(accessor);
    Mockito.when(manager.getSessionId()).thenReturn(session);
    Mockito.when(manager.getClusterName()).thenReturn(_cluster);
    Mockito.when(manager.isLeader()).thenReturn(leader);
    Mockito.when(manager.isConnected()).thenReturn(true);
    return manager;
  }

  private static ConvergenceStatus report(String session, Status status, Reason reason,
      int count) {
    ConvergenceStatus report = new ConvergenceStatus("r");
    report.setStatus(status);
    report.setPrimaryReason(reason);
    report.setControllerSessionId(session);
    report.setGeneratedAt(T0);
    report.setTargetFreshness(TargetFreshness.CURRENT);
    report.setOptimizerStatus(OptimizerStatus.NOT_APPLICABLE);
    report.setStatusCounts(Collections.singletonMap(status, count));
    return report;
  }

  private ConvergenceStatusCalculator.Result result(Status status, Reason reason, int count,
      String... resources) {
    Map<String, ConvergenceStatus> reports = new TreeMap<>();
    for (String name : resources) {
      reports.put(name, report(_session, status, reason, count));
    }
    return new ConvergenceStatusCalculator.Result(report(_session, status, reason, count), reports);
  }

  private long persist(HelixManager manager, ConvergenceStatusCalculator.Result result,
      long now) {
    return _stage.persist(new ClusterEvent(ClusterEventType.OnDemandRebalance), manager, result,
        now);
  }

  private ConvergenceStatus record() {
    return _accessor.getProperty(_keys.convergenceStatus());
  }

  private int version() {
    return _gZkClient.getStat(_keys.convergenceStatus().getPath()).getVersion();
  }

  @Test
  public void testFirstWriteThenNoZooKeeperCallWhenNothingChanged() {
    HelixDataAccessor accessor = Mockito.spy(_accessor);
    HelixManager manager = manager(_session, true, accessor);
    persist(manager, result(Status.IN_PROGRESS, Reason.PENDING_TRANSITION, 3, "a", "b"), T0);
    Assert.assertEquals(record().getListedResources().size(), 2);
    Assert.assertEquals(record().getResourceReport("a").getUnconvergedSince(), T0);
    Assert.assertEquals(record().getUnconvergedSince(), T0);

    int before = version();
    Mockito.clearInvocations(accessor, manager);
    Assert.assertEquals(persist(manager,
        result(Status.IN_PROGRESS, Reason.PENDING_TRANSITION, 3, "a", "b"), T0 + 30_000L), -1L);
    Mockito.verify(manager, Mockito.never()).isLeader();
    Mockito.verify(accessor, Mockito.atMost(1)).keyBuilder();
    Mockito.verifyNoMoreInteractions(accessor);
    Assert.assertEquals(version(), before);
  }

  @Test
  public void testFailedLeaderCheckKeepsTheNewestRecord() {
    HelixManager manager = manager(_session, true);
    persist(manager, result(Status.IN_PROGRESS, Reason.PENDING_TRANSITION, 1, "a"), T0);
    // The check that guards the write fails once, say a read error, then passes again.
    Mockito.when(manager.isLeader()).thenReturn(false, true);
    long converged = T0 + ConvergenceStatusPersistStage.MIN_REWRITE_MS;
    persist(manager, result(Status.CONVERGED, Reason.NONE, 0, "a"), converged);
    Assert.assertEquals(record().getStatus(), Status.IN_PROGRESS);
    _stage.tick(converged + ConvergenceStatusPersistStage.RETRY_MIN_MS);
    Assert.assertEquals(record().getStatus(), Status.CONVERGED);
  }

  @Test
  public void testFlappingGetsOneFastWrite() {
    persist(_manager, result(Status.CONVERGED, Reason.NONE, 0, "a"), T0);
    int before = version();
    persist(_manager, result(Status.IN_PROGRESS, Reason.PENDING_TRANSITION, 1, "a"), T0 + 1_000L);
    Assert.assertEquals(version(), before + 1);
    // The flip back waits like any other change, since the write before was a fast one.
    Assert.assertEquals(persist(_manager, result(Status.CONVERGED, Reason.NONE, 0, "a"),
        T0 + 2_000L), ConvergenceStatusPersistStage.MIN_REWRITE_MS - 1_000L);
  }

  @Test
  public void testHeldBackChangeIsWrittenByTheTick() {
    persist(_manager, result(Status.IN_PROGRESS, Reason.PENDING_TRANSITION, 3, "a"), T0);
    int before = version();
    persist(_manager, result(Status.BLOCKED, Reason.ERROR_STATE, 3, "a"), T0 + 1_000L);
    _stage.tick(T0 + 2_000L);
    Assert.assertEquals(version(), before);
    _stage.tick(T0 + ConvergenceStatusPersistStage.MIN_REWRITE_MS);
    Assert.assertEquals(version(), before + 1);
    Assert.assertEquals(record().getStatus(), Status.BLOCKED);
  }

  @Test
  public void testOnlyResourcesThatNeedAttentionAreListed() {
    Map<String, ConvergenceStatus> reports = new TreeMap<>();
    reports.put("slow", report(_session, Status.IN_PROGRESS, Reason.PENDING_TRANSITION, 2));
    reports.put("stuck", report(_session, Status.BLOCKED, Reason.ERROR_STATE, 1));
    reports.put("done", report(_session, Status.CONVERGED, Reason.NONE, 4));
    persist(_manager, new ConvergenceStatusCalculator.Result(
        report(_session, Status.BLOCKED, Reason.ERROR_STATE, 7), reports), T0);
    ConvergenceStatus record = record();
    Assert.assertEquals(record.getListedResources().size(), 2);
    Assert.assertEquals(record.getResourceReport("stuck").getPrimaryReason(), Reason.ERROR_STATE);
    Assert.assertNull(record.getResourceReport("done"));

    // A dropped resource is gone with the next write.
    reports.remove("stuck");
    persist(_manager, new ConvergenceStatusCalculator.Result(
        report(_session, Status.IN_PROGRESS, Reason.PENDING_TRANSITION, 6), reports),
        T0 + ConvergenceStatusPersistStage.MIN_REWRITE_MS);
    Assert.assertNull(record().getResourceReport("stuck"));
  }

  @Test
  public void testListIsWorstFirstAndFitsTheByteBudget() {
    Map<String, ConvergenceStatus> reports = new TreeMap<>();
    for (int i = 0; i < 200; i++) {
      Status status = i % 4 == 0 ? Status.BLOCKED : Status.IN_PROGRESS;
      reports.put(String.format("resource_%03d", i), report(_session, status, Reason.NONE, i));
    }
    ConvergenceStatus record = report(_session, Status.BLOCKED, Reason.NONE, 1);
    List<String> listed = ConvergenceStatusPersistStage.fold(record, reports, null, T0);
    Assert.assertEquals(listed.size(), 200);
    Assert.assertEquals(record.getResourceReport(listed.get(0)).getStatus(), Status.BLOCKED);

    ZNRecordSerializer serializer = new ZNRecordSerializer();
    ConvergenceStatus bare = new ConvergenceStatus(new ZNRecord(record.getRecord()));
    listed.forEach(bare::removeResourceReport);
    int empty = serializer.serialize(bare.getRecord()).length;
    int budget = empty + (serializer.serialize(record.getRecord()).length - empty) / 3;
    ConvergenceStatusPersistStage.fit(record, listed, budget);
    Assert.assertTrue(serializer.serialize(record.getRecord()).length <= budget);
    Assert.assertTrue(listed.size() > 0 && listed.size() <= 50, "listed " + listed.size());
    Assert.assertEquals(record.getListedResources().size(), listed.size());
    Assert.assertEquals(record.getUnlistedResourceCount(), 200 - listed.size());
    for (String name : listed) {
      Assert.assertEquals(record.getResourceReport(name).getStatus(), Status.BLOCKED);
    }
    // A resource left out is not reported as converged.
    Assert.assertNull(record.getResourceReport("resource_001"));
    Assert.assertEquals(record.getUnlistedReport("resource_001", 1, false).getStatus(),
        Status.UNKNOWN);

    // The same input trimmed the same way is unchanged, so it is not written again.
    ConvergenceStatus again = report(_session, Status.BLOCKED, Reason.NONE, 1);
    List<String> listedAgain = ConvergenceStatusPersistStage.fold(again, reports, null, T0);
    ConvergenceStatusPersistStage.fit(again, listedAgain, budget);
    Assert.assertEquals(ConvergenceStatusPersistStage.writeDelay(again,
        new ConvergenceStatusPersistStage.Written(record, T0, false), T0 + 1), -1L);
  }

  @Test
  public void testChurnIsRateLimitedAndStatusChangesLess() {
    persist(_manager, result(Status.IN_PROGRESS, Reason.PENDING_TRANSITION, 3, "a"), T0);
    int v = version();

    // Counts or reason alone: at most once per MIN_REWRITE_MS.
    Assert.assertEquals(
        persist(_manager, result(Status.IN_PROGRESS, Reason.PENDING_TRANSITION, 2, "a"),
            T0 + 1_000L), ConvergenceStatusPersistStage.MIN_REWRITE_MS - 1_000L);
    Assert.assertEquals(version(), v);
    long rewrite = T0 + ConvergenceStatusPersistStage.MIN_REWRITE_MS;
    persist(_manager, result(Status.IN_PROGRESS, Reason.MESSAGE_THROTTLED, 2, "a"), rewrite);
    Assert.assertEquals(version(), v + 1);

    // Flapping between unconverged states waits like any other change.
    Assert.assertEquals(persist(_manager, result(Status.BLOCKED, Reason.ERROR_STATE, 2, "a"),
        rewrite + 1), ConvergenceStatusPersistStage.MIN_REWRITE_MS - 1);
    // Into CONVERGED: at most once per STATUS_MIN_INTERVAL_MS.
    Assert.assertEquals(persist(_manager, result(Status.CONVERGED, Reason.NONE, 0, "a"),
        rewrite + 1), ConvergenceStatusPersistStage.STATUS_MIN_INTERVAL_MS - 1);
    persist(_manager, result(Status.CONVERGED, Reason.NONE, 0, "a"),
        rewrite + ConvergenceStatusPersistStage.STATUS_MIN_INTERVAL_MS);
    Assert.assertEquals(record().getStatus(), Status.CONVERGED);
    Assert.assertEquals(record().getUnconvergedSince(), 0L);
    Assert.assertTrue(record().getListedResources().isEmpty());
  }

  @Test
  public void testNewLeaderTakesOverAndKeepsWhenItStarted() {
    persist(_manager, result(Status.BLOCKED, Reason.ERROR_STATE, 1, "a"), T0);
    HelixZkClient other = DedicatedZkClientFactory.getInstance()
        .buildZkClient(new HelixZkClient.ZkConnectionConfig(ZK_ADDR),
            new HelixZkClient.ZkClientConfig().setZkSerializer(new ZNRecordSerializer()));
    try {
      String next2 = Long.toHexString(other.getSessionId());
      ConvergenceStatusCalculator.Result next = result(Status.BLOCKED, Reason.ERROR_STATE, 1, "a");
      next.getCluster().setControllerSessionId(next2);
      HelixDataAccessor otherAccessor =
          new ZKHelixDataAccessor(_cluster, new ZkBaseDataAccessor<>(other));
      new ConvergenceStatusPersistStage().persist(
          new ClusterEvent(ClusterEventType.OnDemandRebalance),
          manager(next2, true, otherAccessor), next, T0 + 5_000L);
      Assert.assertEquals(record().getControllerSessionId(), next2);
      Assert.assertEquals(record().getUnconvergedSince(), T0);
      Assert.assertEquals(record().getResourceReport("a").getUnconvergedSince(), T0);
    } finally {
      other.close();
    }
  }

  @Test
  public void testControllerOnAnotherSessionCannotOverwriteTheRecord() {
    persist(_manager, result(Status.BLOCKED, Reason.ERROR_STATE, 1, "a"), T0);
    int before = version();
    // A controller that lost its session still writes with the session it believed it had.
    String stale = "1";
    ConvergenceStatusCalculator.Result next = result(Status.CONVERGED, Reason.NONE, 0, "a");
    next.getCluster().setControllerSessionId(stale);
    new ConvergenceStatusPersistStage().persist(
        new ClusterEvent(ClusterEventType.OnDemandRebalance), manager(stale, true), next,
        T0 + 5_000L);
    Assert.assertEquals(version(), before);
    Assert.assertEquals(record().getControllerSessionId(), _session);
  }

  @Test
  public void testNonLeaderWritesNothing() {
    persist(manager(_session, false), result(Status.CONVERGED, Reason.NONE, 0, "a"), T0);
    Assert.assertNull(record());
  }

  @Test
  public void testMaintenanceListsNothing() {
    persist(_manager, result(Status.IN_PROGRESS, Reason.PENDING_TRANSITION, 1, "a", "b"), T0);
    persist(_manager, result(Status.PAUSED, Reason.MAINTENANCE_MODE, 1, "a", "b", "c"),
        T0 + ConvergenceStatusPersistStage.MIN_REWRITE_MS);
    Assert.assertEquals(record().getStatus(), Status.PAUSED);
    Assert.assertTrue(record().getListedResources().isEmpty());
    Assert.assertNull(record().getResourceReport("c"));
  }

  @Test
  public void testFailedWritesBackOffAndTheTickRetries() {
    HelixDataAccessor failing = Mockito.spy(_accessor);
    Mockito.doReturn(new boolean[] {false}).when(failing)
        .setChildren(Mockito.anyList(), Mockito.anyList());
    HelixManager manager = manager(_session, true, failing);
    ConvergenceStatusCalculator.Result result = result(Status.CONVERGED, Reason.NONE, 0, "a");

    Assert.assertEquals(persist(manager, result, T0), ConvergenceStatusPersistStage.RETRY_MIN_MS);
    Assert.assertNull(record());
    // Still backing off: no write is attempted.
    Assert.assertEquals(persist(manager, result, T0 + 1),
        ConvergenceStatusPersistStage.RETRY_MIN_MS - 1);
    Mockito.verify(failing, Mockito.times(1)).setChildren(Mockito.anyList(), Mockito.anyList());
    _stage.tick(T0 + ConvergenceStatusPersistStage.RETRY_MIN_MS);
    Mockito.verify(failing, Mockito.times(2)).setChildren(Mockito.anyList(), Mockito.anyList());

    persist(_manager, result, T0 + 1_500L);
    _stage.tick(T0 + 3 * ConvergenceStatusPersistStage.RETRY_MIN_MS);
    Assert.assertNotNull(record());
  }
}
