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
import java.util.Map;
import java.util.TreeMap;

import org.apache.helix.HelixDataAccessor;
import org.apache.helix.HelixManager;
import org.apache.helix.PropertyKey;
import org.apache.helix.common.ZkTestBase;
import org.apache.helix.manager.zk.ZKHelixDataAccessor;
import org.apache.helix.manager.zk.ZkBaseDataAccessor;
import org.apache.helix.model.ConvergenceStatus;
import org.apache.helix.model.ConvergenceStatus.Reason;
import org.apache.helix.model.ConvergenceStatus.Status;
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

  @BeforeMethod
  public void setUp() {
    _cluster = "CONV_" + getShortClassName() + "_" + System.nanoTime();
    _gSetupTool.addCluster(_cluster, true);
    _accessor = new ZKHelixDataAccessor(_cluster, new ZkBaseDataAccessor<>(_gZkClient));
    _keys = _accessor.keyBuilder();
    _stage = new ConvergenceStatusPersistStage();
    _manager = manager("s1", true);
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
    return manager;
  }

  private static ConvergenceStatus report(String session, Status status, Reason reason,
      int inProgress) {
    ConvergenceStatus report = new ConvergenceStatus("r");
    report.setStatus(status);
    report.setPrimaryReason(reason);
    report.setControllerSessionId(session);
    report.setGeneratedAt(T0);
    report.setStatusCounts(Collections.singletonMap(Status.IN_PROGRESS, inProgress));
    return report;
  }

  private ConvergenceStatusCalculator.Result result(Status status, Reason reason, int count,
      String... resources) {
    Map<String, ConvergenceStatus> reports = new TreeMap<>();
    for (String name : resources) {
      reports.put(name, report("s1", status, reason, count));
    }
    return new ConvergenceStatusCalculator.Result(report("s1", status, reason, count), reports);
  }

  private long persist(HelixManager manager, ConvergenceStatusCalculator.Result result,
      long now) {
    return _stage.persist(new ClusterEvent(ClusterEventType.OnDemandRebalance), manager, result,
        now);
  }

  private int version(String resource) {
    return _gZkClient.getStat(
        _keys.convergenceStatus(resource).getPath()).getVersion();
  }

  private int clusterVersion() {
    return _gZkClient.getStat(_keys.convergenceStatus().getPath()).getVersion();
  }

  @Test
  public void testFirstWriteThenNoZooKeeperCallWhenNothingChanged() {
    HelixDataAccessor accessor = Mockito.spy(_accessor);
    HelixManager manager = manager("s1", true, accessor);
    persist(manager, result(Status.IN_PROGRESS, Reason.PENDING_TRANSITION, 3, "a", "b"), T0);
    ConvergenceStatus a = _accessor.getProperty(_keys.convergenceStatus("a"));
    Assert.assertEquals(a.getStatus(), Status.IN_PROGRESS);
    Assert.assertEquals(a.getUnconvergedSince(), T0);
    ConvergenceStatus cluster = _accessor.getProperty(_keys.convergenceStatus());
    Assert.assertEquals(cluster.getUnconvergedSince(), T0);

    int before = version("a");
    Mockito.clearInvocations(accessor, manager);
    Assert.assertEquals(persist(manager,
        result(Status.IN_PROGRESS, Reason.PENDING_TRANSITION, 3, "a", "b"), T0 + 60_000L), -1L);
    Mockito.verify(manager, Mockito.never()).isLeader();
    Mockito.verify(accessor, Mockito.atMost(1)).keyBuilder();
    Mockito.verifyNoMoreInteractions(accessor);
    Assert.assertEquals(version("a"), before);
    ConvergenceStatus again = _accessor.getProperty(_keys.convergenceStatus("a"));
    Assert.assertEquals(again.getUnconvergedSince(), T0);
  }

  @Test
  public void testCountAndReasonChurnIsRateLimitedStatusChangesLess() {
    persist(_manager, result(Status.IN_PROGRESS, Reason.PENDING_TRANSITION, 3, "a"), T0);
    int v = version("a");

    // Counts or reason alone: at most once per MIN_REWRITE_MS.
    Assert.assertEquals(
        persist(_manager, result(Status.IN_PROGRESS, Reason.PENDING_TRANSITION, 2, "a"),
            T0 + 1_000L), ConvergenceStatusPersistStage.MIN_REWRITE_MS - 1_000L);
    persist(_manager, result(Status.IN_PROGRESS, Reason.MESSAGE_THROTTLED, 2, "a"), T0 + 2_000L);
    Assert.assertEquals(version("a"), v);
    long rewrite = T0 + ConvergenceStatusPersistStage.MIN_REWRITE_MS;
    persist(_manager, result(Status.IN_PROGRESS, Reason.MESSAGE_THROTTLED, 2, "a"), rewrite);
    Assert.assertEquals(version("a"), v + 1);

    // A status change: at most once per STATUS_MIN_INTERVAL_MS.
    Assert.assertEquals(persist(_manager, result(Status.CONVERGED, Reason.NONE, 0, "a"),
        rewrite + 1), ConvergenceStatusPersistStage.STATUS_MIN_INTERVAL_MS - 1);
    Assert.assertEquals(version("a"), v + 1);
    persist(_manager, result(Status.CONVERGED, Reason.NONE, 0, "a"),
        rewrite + ConvergenceStatusPersistStage.STATUS_MIN_INTERVAL_MS);
    ConvergenceStatus a = _accessor.getProperty(_keys.convergenceStatus("a"));
    Assert.assertEquals(a.getStatus(), Status.CONVERGED);
    Assert.assertEquals(a.getUnconvergedSince(), 0L);
  }

  @Test
  public void testDroppedResourceIsRemoved() {
    persist(_manager, result(Status.CONVERGED, Reason.NONE, 0, "a", "b", "c"), T0);
    persist(_manager, result(Status.CONVERGED, Reason.NONE, 0, "a", "c"), T0 + 1);
    Assert.assertNull(_accessor.getProperty(_keys.convergenceStatus("b")));
    Assert.assertNotNull(_accessor.getProperty(_keys.convergenceStatus("a")));

    // A report someone else already deleted counts as removed, so nothing is retried.
    _gZkClient.delete(_keys.convergenceStatus("c").getPath());
    Assert.assertEquals(
        persist(_manager, result(Status.CONVERGED, Reason.NONE, 0, "a"), T0 + 2), -1L);
  }

  @Test
  public void testLargeRunIsWrittenInBatchesWithinOneRun() {
    String[] names = new String[ConvergenceStatusPersistStage.BATCH_SIZE * 2 + 5];
    for (int i = 0; i < names.length; i++) {
      names[i] = String.format("r%04d", i);
    }
    Assert.assertEquals(persist(_manager, result(Status.CONVERGED, Reason.NONE, 0, names), T0),
        -1L);
    Assert.assertEquals(_accessor.getChildNames(_keys.convergenceStatus()).size(), names.length);
  }

  @Test
  public void testNewLeaderRewritesOnlyTheClusterRecord() {
    persist(_manager, result(Status.BLOCKED, Reason.ERROR_STATE, 1, "a"), T0);
    int resourceVersion = version("a");
    ConvergenceStatusPersistStage successor = new ConvergenceStatusPersistStage();
    ConvergenceStatusCalculator.Result next = result(Status.BLOCKED, Reason.ERROR_STATE, 1, "a");
    next.getCluster().setControllerSessionId("s2");
    next.getResources().get("a").setControllerSessionId("s2");
    successor.persist(new ClusterEvent(ClusterEventType.OnDemandRebalance), manager("s2", true),
        next, T0 + 5_000L);
    Assert.assertEquals(version("a"), resourceVersion);
    ConvergenceStatus cluster = _accessor.getProperty(_keys.convergenceStatus());
    Assert.assertEquals(cluster.getControllerSessionId(), "s2");
    Assert.assertEquals(cluster.getUnconvergedSince(), T0);
  }

  @Test
  public void testNonLeaderWritesNothing() {
    persist(manager("s1", false), result(Status.CONVERGED, Reason.NONE, 0, "a"), T0);
    Assert.assertNull(_accessor.getProperty(_keys.convergenceStatus("a")));
    Assert.assertNull(_accessor.getProperty(_keys.convergenceStatus()));
  }

  @Test
  public void testMaintenanceTouchesOnlyTheClusterRecord() {
    persist(_manager, result(Status.CONVERGED, Reason.NONE, 0, "a", "b"), T0);
    int a = version("a");
    int b = version("b");
    int cluster = clusterVersion();

    persist(_manager, result(Status.PAUSED, Reason.MAINTENANCE_MODE, 0, "a", "b", "c"),
        T0 + 2_000L);
    Assert.assertEquals(version("a"), a);
    Assert.assertEquals(version("b"), b);
    Assert.assertEquals(clusterVersion(), cluster + 1);
    Assert.assertEquals(_accessor.<ConvergenceStatus>getProperty(_keys.convergenceStatus())
        .getStatus(), Status.PAUSED);
    // A resource created during the pause still gets a report.
    Assert.assertNotNull(_accessor.getProperty(_keys.convergenceStatus("c")));

    persist(_manager, result(Status.CONVERGED, Reason.NONE, 0, "a", "b", "c"), T0 + 4_000L);
    Assert.assertEquals(version("a"), a);
    Assert.assertEquals(version("b"), b);
    Assert.assertEquals(clusterVersion(), cluster + 2);
  }

  @Test
  public void testFailedWritesBackOffAndDoNotTakeOverTheAnchor() {
    HelixDataAccessor failing = Mockito.spy(_accessor);
    Mockito.doReturn(new boolean[] {false}).when(failing)
        .setChildren(Mockito.anyList(), Mockito.anyList());
    HelixManager manager = manager("s1", true, failing);
    ConvergenceStatusCalculator.Result result = result(Status.CONVERGED, Reason.NONE, 0, "a");

    Assert.assertEquals(persist(manager, result, T0), ConvergenceStatusPersistStage.RETRY_MIN_MS);
    Assert.assertNull(_accessor.getProperty(_keys.convergenceStatus()));
    Assert.assertEquals(persist(manager, result, T0 + 1),
        2 * ConvergenceStatusPersistStage.RETRY_MIN_MS);
    Assert.assertEquals(persist(manager, result, T0 + 2),
        4 * ConvergenceStatusPersistStage.RETRY_MIN_MS);

    Assert.assertEquals(persist(_manager, result, T0 + 3), -1L);
    Assert.assertNotNull(_accessor.getProperty(_keys.convergenceStatus("a")));
    Assert.assertNotNull(_accessor.getProperty(_keys.convergenceStatus()));
  }
}
