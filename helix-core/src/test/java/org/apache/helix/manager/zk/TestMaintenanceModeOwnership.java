package org.apache.helix.manager.zk;

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

import java.lang.reflect.Method;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.helix.HelixDataAccessor;
import org.apache.helix.PropertyPathBuilder;
import org.apache.helix.TestHelper;
import org.apache.helix.ZkUnitTestBase;
import org.apache.helix.api.status.MaintenanceModeAcquireResult;
import org.apache.helix.api.status.MaintenanceModeOwnershipHandle;
import org.apache.helix.api.status.MaintenanceModeReleaseResult;
import org.apache.helix.model.MaintenanceSignal;
import org.apache.helix.zookeeper.api.client.RealmAwareZkClient;
import org.mockito.AdditionalAnswers;
import org.mockito.Mockito;
import org.testng.Assert;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

public class TestMaintenanceModeOwnership extends ZkUnitTestBase {
  private String _clusterName;
  private ZKHelixAdmin _admin;
  private HelixDataAccessor _accessor;

  @BeforeMethod
  public void beforeMethod(Method method) {
    _clusterName = getShortClassName() + "_" + method.getName();
    if (_gZkClient.exists("/" + _clusterName)) {
      _gZkClient.deleteRecursively("/" + _clusterName);
    }
    _admin = new ZKHelixAdmin(_gZkClient);
    _admin.addCluster(_clusterName, true);
    _accessor =
        new ZKHelixDataAccessor(_clusterName, new ZkBaseDataAccessor<>(_gZkClient));
  }

  @AfterMethod
  public void afterMethod() {
    if (_gZkClient.exists("/" + _clusterName)) {
      _gZkClient.deleteRecursively("/" + _clusterName);
    }
  }

  @Test
  public void testAcquireEnsureAndDuplicateRelease() {
    MaintenanceModeAcquireResult acquired =
        _admin.acquireMaintenanceMode(_clusterName, "owner-a", "window-a", "maintenance");

    Assert.assertEquals(acquired.getStatus(), MaintenanceModeAcquireResult.Status.ACQUIRED);
    Assert.assertNotNull(acquired.getHandle());
    MaintenanceSignal signal = getMaintenanceSignal();
    Assert.assertEquals(signal.getMaintenanceOwnerId(), "owner-a");
    Assert.assertEquals(signal.getMaintenanceWindowId(), "window-a");
    Assert.assertEquals(signal.getMaintenanceFenceId(), acquired.getHandle().getFenceId());

    MaintenanceModeAcquireResult ensured =
        _admin.acquireMaintenanceMode(_clusterName, "owner-a", "window-a", "maintenance");
    Assert.assertEquals(ensured.getStatus(), MaintenanceModeAcquireResult.Status.ALREADY_OWNED);
    Assert.assertEquals(ensured.getHandle(), acquired.getHandle());

    MaintenanceModeReleaseResult released =
        _admin.releaseMaintenanceMode(acquired.getHandle(), "complete");
    Assert.assertEquals(released.getStatus(), MaintenanceModeReleaseResult.Status.APPLIED);
    Assert.assertNull(getMaintenanceSignal());

    MaintenanceModeReleaseResult duplicate =
        _admin.releaseMaintenanceMode(acquired.getHandle(), "complete");
    Assert.assertEquals(duplicate.getStatus(), MaintenanceModeReleaseResult.Status.UNCHANGED);
  }

  @Test
  public void testEnsureRecoversWhenCreatorResultWasLost() {
    new ZKHelixAdmin(_gZkClient)
        .acquireMaintenanceMode(_clusterName, "owner-a", "window-a", "maintenance");

    MaintenanceModeAcquireResult ensured =
        _admin.acquireMaintenanceMode(_clusterName, "owner-a", "window-a", "maintenance");

    Assert.assertEquals(ensured.getStatus(), MaintenanceModeAcquireResult.Status.ALREADY_OWNED);
    Assert.assertNotNull(ensured.getHandle());
  }

  @Test
  public void testAcquireDoesNotAdoptManualOrAutomaticWindow() {
    _admin.manuallyEnableMaintenanceMode(_clusterName, true, "manual", null);
    MaintenanceModeAcquireResult manualCoverage =
        _admin.acquireMaintenanceMode(_clusterName, "owner-a", "window-a", "maintenance");
    Assert.assertEquals(manualCoverage.getStatus(),
        MaintenanceModeAcquireResult.Status.ALREADY_COVERED);
    Assert.assertNull(manualCoverage.getHandle());

    _admin.manuallyEnableMaintenanceMode(_clusterName, false, "manual done", null);
    _admin.autoEnableMaintenanceMode(_clusterName, true, "automatic",
        MaintenanceSignal.AutoTriggerReason.MAX_PARTITION_PER_INSTANCE_EXCEEDED);
    MaintenanceModeAcquireResult automaticCoverage =
        _admin.acquireMaintenanceMode(_clusterName, "owner-a", "window-b", "maintenance");
    Assert.assertEquals(automaticCoverage.getStatus(),
        MaintenanceModeAcquireResult.Status.ALREADY_COVERED);
    Assert.assertNull(automaticCoverage.getHandle());
  }

  @Test
  public void testAcquireReportsForeignOwnedWindow() {
    _admin.acquireMaintenanceMode(_clusterName, "owner-a", "window-a", "maintenance");

    MaintenanceModeAcquireResult foreign =
        _admin.acquireMaintenanceMode(_clusterName, "owner-b", "window-b", "maintenance");

    Assert.assertEquals(foreign.getStatus(), MaintenanceModeAcquireResult.Status.FOREIGN_OWNED);
    Assert.assertNull(foreign.getHandle());
  }

  @Test
  public void testReasonSubstringDoesNotConferOwnership() {
    _admin.manuallyEnableMaintenanceMode(_clusterName, true,
        "maintenance requested by owner-a for window-a", null);

    MaintenanceModeAcquireResult result =
        _admin.acquireMaintenanceMode(_clusterName, "owner-a", "window-a", "maintenance");

    Assert.assertEquals(result.getStatus(), MaintenanceModeAcquireResult.Status.ALREADY_COVERED);
    Assert.assertNull(result.getHandle());
  }

  @Test
  public void testReleaseRejectsInPlaceReplacement() {
    MaintenanceModeOwnershipHandle handle =
        _admin.acquireMaintenanceMode(_clusterName, "owner-a", "window-a", "maintenance")
            .getHandle();
    _admin.manuallyEnableMaintenanceMode(_clusterName, true, "manual replacement", null);

    MaintenanceModeReleaseResult result =
        _admin.releaseMaintenanceMode(handle, "stale release");

    Assert.assertEquals(result.getStatus(), MaintenanceModeReleaseResult.Status.CONFLICT);
    Assert.assertEquals(getMaintenanceSignal().getReason(), "manual replacement");
    Assert.assertEquals(getMaintenanceSignal().getTriggeringEntity(),
        MaintenanceSignal.TriggeringEntity.USER);
  }

  @Test
  public void testReleaseRejectsDeleteRecreateReplacement() {
    MaintenanceModeOwnershipHandle handle =
        _admin.acquireMaintenanceMode(_clusterName, "owner-a", "window-a", "maintenance")
            .getHandle();
    _admin.manuallyEnableMaintenanceMode(_clusterName, false, "remove original", null);
    _admin.manuallyEnableMaintenanceMode(_clusterName, true, "recreated replacement", null);

    MaintenanceModeReleaseResult result =
        _admin.releaseMaintenanceMode(handle, "stale release");

    Assert.assertEquals(result.getStatus(), MaintenanceModeReleaseResult.Status.CONFLICT);
    Assert.assertEquals(getMaintenanceSignal().getReason(), "recreated replacement");
  }

  @Test
  public void testUniqueFenceRejectsConcurrentDeleteRecreateAba() {
    MaintenanceModeOwnershipHandle handle =
        _admin.acquireMaintenanceMode(_clusterName, "owner-a", "window-a", "maintenance")
            .getHandle();
    RealmAwareZkClient interceptingClient = Mockito.mock(RealmAwareZkClient.class,
        AdditionalAnswers.delegatesTo(_gZkClient));
    AtomicBoolean replaced = new AtomicBoolean();
    Mockito.doAnswer(invocation -> {
      if (replaced.compareAndSet(false, true)) {
        String maintenancePath = PropertyPathBuilder.maintenance(_clusterName);
        _gZkClient.deleteRecursively(maintenancePath);
        MaintenanceSignal replacement = new MaintenanceSignal("maintenance");
        replacement.setReason("raw replacement");
        replacement.setTriggeringEntity(MaintenanceSignal.TriggeringEntity.USER);
        _gZkClient.createPersistent(maintenancePath, replacement.getRecord());
      }
      return _gZkClient.multi(invocation.getArgument(0));
    }).when(interceptingClient).multi(Mockito.any());

    MaintenanceModeReleaseResult result =
        new ZKHelixAdmin(interceptingClient).releaseMaintenanceMode(handle, "stale release");

    Assert.assertEquals(result.getStatus(), MaintenanceModeReleaseResult.Status.CONFLICT);
    Assert.assertEquals(getMaintenanceSignal().getReason(), "raw replacement");
  }

  @Test
  public void testAutomaticExitDoesNotRemoveManualReplacement() {
    MaintenanceSignal.AutoTriggerReason autoReason =
        MaintenanceSignal.AutoTriggerReason.MAX_PARTITION_PER_INSTANCE_EXCEEDED;
    _admin.autoEnableMaintenanceMode(_clusterName, true, "automatic", autoReason);
    _admin.manuallyEnableMaintenanceMode(_clusterName, true, "manual replacement", null);

    _admin.autoEnableMaintenanceMode(_clusterName, false, "automatic recovery", autoReason);

    Assert.assertNotNull(getMaintenanceSignal());
    Assert.assertEquals(getMaintenanceSignal().getReason(), "manual replacement");
    Assert.assertEquals(getMaintenanceSignal().getTriggeringEntity(),
        MaintenanceSignal.TriggeringEntity.USER);
  }

  @Test
  public void testAutomaticExitDoesNotRemoveOwnedWindow() {
    MaintenanceModeOwnershipHandle handle =
        _admin.acquireMaintenanceMode(_clusterName, "owner-a", "window-a", "maintenance")
            .getHandle();

    _admin.autoEnableMaintenanceMode(_clusterName, false, "automatic recovery",
        MaintenanceSignal.AutoTriggerReason.MAX_PARTITION_PER_INSTANCE_EXCEEDED);

    Assert.assertNotNull(getMaintenanceSignal());
    Assert.assertEquals(getMaintenanceSignal().getMaintenanceOwnerId(), "owner-a");
    Assert.assertEquals(
        _admin.releaseMaintenanceMode(handle, "complete").getStatus(),
        MaintenanceModeReleaseResult.Status.APPLIED);
  }

  @Test
  public void testConcurrentAcquireReturnsOneOwnedWindow() throws Exception {
    ExecutorService executor = Executors.newFixedThreadPool(2);
    CountDownLatch start = new CountDownLatch(1);
    try {
      Future<MaintenanceModeAcquireResult> first = executor.submit(() -> {
        start.await();
        return new ZKHelixAdmin(_gZkClient)
            .acquireMaintenanceMode(_clusterName, "owner-a", "window-a", "first");
      });
      Future<MaintenanceModeAcquireResult> second = executor.submit(() -> {
        start.await();
        return new ZKHelixAdmin(_gZkClient)
            .acquireMaintenanceMode(_clusterName, "owner-b", "window-b", "second");
      });

      start.countDown();
      List<MaintenanceModeAcquireResult> results =
          Arrays.asList(first.get(30, TimeUnit.SECONDS), second.get(30, TimeUnit.SECONDS));
      String statuses = results.stream().map(result -> result.getStatus().name())
          .collect(java.util.stream.Collectors.joining(","));
      Assert.assertEquals(results.stream()
          .filter(result -> result.getStatus() == MaintenanceModeAcquireResult.Status.ACQUIRED)
          .count(), 1L, statuses);
      Assert.assertEquals(results.stream()
          .filter(result -> result.getStatus()
              == MaintenanceModeAcquireResult.Status.FOREIGN_OWNED
              || result.getStatus() == MaintenanceModeAcquireResult.Status.CONFLICT)
          .count(), 1L, statuses);
      Assert.assertEquals(results.stream().filter(result -> result.getHandle() != null).count(),
          1L);
    } finally {
      executor.shutdownNow();
    }
  }

  @Test
  public void testConcurrentManualReplacementSurvivesStaleRelease() throws Exception {
    for (int index = 0; index < 10; index++) {
      String windowId = "window-" + index;
      MaintenanceModeOwnershipHandle handle =
          _admin.acquireMaintenanceMode(_clusterName, "owner-a", windowId, "owned").getHandle();
      CountDownLatch start = new CountDownLatch(1);
      ExecutorService executor = Executors.newFixedThreadPool(2);
      try {
        Future<MaintenanceModeReleaseResult> release = executor.submit(() -> {
          start.await();
          return new ZKHelixAdmin(_gZkClient).releaseMaintenanceMode(handle, "release");
        });
        Future<?> replace = executor.submit(() -> {
          start.await();
          new ZKHelixAdmin(_gZkClient)
              .manuallyEnableMaintenanceMode(_clusterName, true, "manual-" + windowId, null);
          return null;
        });

        start.countDown();
        MaintenanceModeReleaseResult releaseResult = release.get(30, TimeUnit.SECONDS);
        replace.get(30, TimeUnit.SECONDS);
        Assert.assertTrue(
            releaseResult.getStatus() == MaintenanceModeReleaseResult.Status.APPLIED
                || releaseResult.getStatus() == MaintenanceModeReleaseResult.Status.CONFLICT);
        Assert.assertEquals(getMaintenanceSignal().getReason(), "manual-" + windowId);
      } finally {
        executor.shutdownNow();
      }
      _admin.manuallyEnableMaintenanceMode(_clusterName, false, "reset", null);
      Assert.assertTrue(TestHelper.verify(() -> getMaintenanceSignal() == null, 1000L));
    }
  }

  private MaintenanceSignal getMaintenanceSignal() {
    return _accessor.getProperty(_accessor.keyBuilder().maintenance());
  }
}
