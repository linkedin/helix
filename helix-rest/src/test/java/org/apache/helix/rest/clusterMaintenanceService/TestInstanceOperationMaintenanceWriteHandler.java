package org.apache.helix.rest.clusterMaintenanceService;

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

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import org.apache.helix.ConfigAccessor;
import org.apache.helix.HelixAdmin;
import org.apache.helix.HelixDataAccessor;
import org.apache.helix.PropertyKey;
import org.apache.helix.model.ClusterConfig;
import org.apache.helix.model.InstanceConfig;
import org.apache.helix.rest.clusterMaintenanceService.InstanceOperationMaintenanceWriteHandler.InstanceOperationMaintenanceResult;
import org.apache.helix.zookeeper.datamodel.ZNRecord;
import org.apache.helix.zookeeper.zkclient.DataUpdater;
import org.testng.Assert;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

public class TestInstanceOperationMaintenanceWriteHandler {
  private static final String CLUSTER = "test-cluster";
  private static final long NOW_MS = 1_000_000L;
  private static final long ONE_HOUR_MS = 3_600_000L;

  private HelixAdmin _admin;
  private ConfigAccessor _configAccessor;
  private HelixDataAccessor _dataAccessor;
  private ClusterConfig _clusterConfig;
  private Map<String, InstanceConfig> _storedConfigs;
  private InstanceOperationMaintenanceWriteHandler _handler;

  @BeforeMethod
  public void setUp() {
    _admin = mock(HelixAdmin.class);
    _configAccessor = mock(ConfigAccessor.class);
    _dataAccessor = mock(HelixDataAccessor.class);
    _clusterConfig = new ClusterConfig(CLUSTER);
    _storedConfigs = new HashMap<>();
    for (String instanceName : Arrays.asList("h1", "h2", "h3", "h4")) {
      _storedConfigs.put(instanceName, new InstanceConfig(instanceName));
    }
    when(_configAccessor.getClusterConfig(CLUSTER)).thenReturn(_clusterConfig);
    when(_admin.getInstancesInCluster(CLUSTER))
        .thenAnswer(invocation -> Arrays.asList("h1", "h2", "h3", "h4"));
    when(_admin.getInstanceConfig(eq(CLUSTER), anyString()))
        .thenAnswer(invocation -> _storedConfigs.get(invocation.getArgument(1)));
    when(_dataAccessor.keyBuilder()).thenReturn(new PropertyKey.Builder(CLUSTER));
    doAnswer(invocation -> {
      DataUpdater<ZNRecord> updater = invocation.getArgument(1);
      InstanceConfig markerUpdate = invocation.getArgument(2);
      return applyUpdater(markerUpdate.getInstanceName(), updater);
    }).when(_dataAccessor).updateProperty(any(PropertyKey.class), any(), any(InstanceConfig.class));

    _handler =
        new InstanceOperationMaintenanceWriteHandler(_admin, _configAccessor, _dataAccessor);
  }

  // --- TTL resolution ----------------------------------------------------------------------

  @Test
  public void testResolveExpiresAtMillis_CallerWins() {
    long caller = NOW_MS + 5_000L;
    long resolved = InstanceOperationMaintenanceWriteHandler
        .resolveExpiresAtMillis(caller, _clusterConfig, NOW_MS);
    Assert.assertEquals(resolved, caller);
  }

  @Test
  public void testResolveExpiresAtMillis_FallsBackToClusterDefault() {
    _clusterConfig.setDefaultInstanceOperationMaintenanceDurationMs(ONE_HOUR_MS);
    long resolved = InstanceOperationMaintenanceWriteHandler.resolveExpiresAtMillis(
        InstanceOperationMaintenanceWriteHandler.EXPIRES_AT_MILLIS_UNSET, _clusterConfig, NOW_MS);
    Assert.assertEquals(resolved, NOW_MS + ONE_HOUR_MS);
  }

  @Test(expectedExceptions = InstanceOperationMaintenanceWriteHandler.BadRequestException.class)
  public void testResolveExpiresAtMillis_RejectsWhenNeitherSet() {
    InstanceOperationMaintenanceWriteHandler.resolveExpiresAtMillis(
        InstanceOperationMaintenanceWriteHandler.EXPIRES_AT_MILLIS_UNSET, _clusterConfig, NOW_MS);
  }

  @Test(expectedExceptions = InstanceOperationMaintenanceWriteHandler.BadRequestException.class)
  public void testResolveExpiresAtMillis_RejectsCallerValueInThePast() {
    InstanceOperationMaintenanceWriteHandler
        .resolveExpiresAtMillis(NOW_MS - 1L, _clusterConfig, NOW_MS);
  }

  @Test(expectedExceptions = InstanceOperationMaintenanceWriteHandler.BadRequestException.class)
  public void testResolveExpiresAtMillis_RejectsCallerValueEqualToNow() {
    InstanceOperationMaintenanceWriteHandler
        .resolveExpiresAtMillis(NOW_MS, _clusterConfig, NOW_MS);
  }

  @Test(expectedExceptions = InstanceOperationMaintenanceWriteHandler.BadRequestException.class)
  public void testResolveExpiresAtMillis_RejectsNonPositiveClusterDefault() {
    _clusterConfig.setDefaultInstanceOperationMaintenanceDurationMs(0L);
    InstanceOperationMaintenanceWriteHandler.resolveExpiresAtMillis(
        InstanceOperationMaintenanceWriteHandler.EXPIRES_AT_MILLIS_UNSET, _clusterConfig, NOW_MS);
  }

  @Test(expectedExceptions = InstanceOperationMaintenanceWriteHandler.BadRequestException.class)
  public void testResolveExpiresAtMillis_RejectsOverflowingClusterDefault() {
    _clusterConfig.setDefaultInstanceOperationMaintenanceDurationMs(Long.MAX_VALUE);
    InstanceOperationMaintenanceWriteHandler.resolveExpiresAtMillis(
        InstanceOperationMaintenanceWriteHandler.EXPIRES_AT_MILLIS_UNSET, _clusterConfig, NOW_MS);
  }

  // --- Set path ----------------------------------------------------------------------------

  @Test
  public void testApply_WritesMarker() {
    long expiresAt = NOW_MS + 60_000L;
    InstanceOperationMaintenanceResult result = _handler.apply(CLUSTER,
        Collections.singletonList("h1"), expiresAt, NOW_MS);

    Assert.assertEquals(result.getResolvedExpiresAtMillis(), expiresAt);
    Assert.assertEquals(result.getApplied(), Collections.singletonList("h1"));
    Assert.assertTrue(result.getRejected().isEmpty());

    InstanceConfig stored = _storedConfigs.get("h1");
    Assert.assertNotNull(stored);
    Assert.assertEquals(stored.getInstanceOperationMaintenanceUntilMs(), expiresAt);
  }

  @Test
  public void testApply_DeduplicatesInstanceList() {
    InstanceOperationMaintenanceResult result = _handler.apply(CLUSTER,
        Arrays.asList("h1", "h2", "h1", "h2"), NOW_MS + 60_000L, NOW_MS);

    Assert.assertEquals(result.getApplied(), Arrays.asList("h1", "h2"));
    verify(_dataAccessor, times(2)).updateProperty(any(PropertyKey.class), any(),
        any(InstanceConfig.class));
  }

  @Test(expectedExceptions = InstanceOperationMaintenanceWriteHandler.BadRequestException.class)
  public void testApply_EmptyListRejected() {
    _handler.apply(CLUSTER, Collections.emptyList(), NOW_MS + 60_000L, NOW_MS);
  }

  @Test(expectedExceptions = InstanceOperationMaintenanceWriteHandler.BadRequestException.class)
  public void testApply_NullInstanceNameRejected() {
    _handler.apply(CLUSTER, Arrays.asList("h1", null), NOW_MS + 60_000L, NOW_MS);
  }

  // --- Per-instance partial-accept ---------------------------------------------------------

  @Test
  public void testApply_MissingInstanceRejectedButOthersApplied() {
    // h99 is not in getInstancesInCluster; the other instances still get applied.
    when(_admin.getInstancesInCluster(CLUSTER))
        .thenAnswer(invocation -> Arrays.asList("h1", "h2", "h3", "h4"));

    InstanceOperationMaintenanceResult result = _handler.apply(CLUSTER,
        Arrays.asList("h1", "h99", "h2"), NOW_MS + 60_000L, NOW_MS);

    Assert.assertEquals(result.getApplied(), Arrays.asList("h1", "h2"));
    Assert.assertEquals(result.getRejected().keySet(), Collections.singleton("h99"));
    Assert.assertTrue(result.getRejected().get("h99").contains("not found"));

    verify(_dataAccessor, times(2)).updateProperty(any(PropertyKey.class), any(),
        any(InstanceConfig.class));
  }

  @Test
  public void testApply_StaleInstanceConfigIsRejectedWithoutCreation() {
    _storedConfigs.remove("h1");

    InstanceOperationMaintenanceResult result = _handler.apply(CLUSTER,
        Collections.singletonList("h1"), NOW_MS + 60_000L, NOW_MS);

    Assert.assertTrue(result.getApplied().isEmpty());
    Assert.assertEquals(result.getRejected().keySet(), Collections.singleton("h1"));
    Assert.assertTrue(result.getRejected().get("h1").contains("not found"));
    Assert.assertFalse(result.isWriteFailure("h1"));
    Assert.assertFalse(_storedConfigs.containsKey("h1"),
        "The optimistic updater must not create a missing InstanceConfig");
  }

  @Test
  public void testApply_SetRetryPreservesConcurrentUnrelatedField() {
    _storedConfigs.get("h1").getRecord().setSimpleField("EXISTING_FIELD", "existing");
    stubRetryWithConcurrentField("CONCURRENT_FIELD", "preserved");

    long expiresAt = NOW_MS + 60_000L;
    InstanceOperationMaintenanceResult result = _handler.apply(CLUSTER,
        Collections.singletonList("h1"), expiresAt, NOW_MS);

    Assert.assertEquals(result.getApplied(), Collections.singletonList("h1"));
    InstanceConfig stored = _storedConfigs.get("h1");
    Assert.assertEquals(stored.getInstanceOperationMaintenanceUntilMs(), expiresAt);
    Assert.assertEquals(stored.getRecord().getSimpleField("EXISTING_FIELD"), "existing");
    Assert.assertEquals(stored.getRecord().getSimpleField("CONCURRENT_FIELD"), "preserved");
  }

  // --- Cap enforcement (partial-accept semantics) -----------------------------------------

  @Test
  public void testEnforceCap_NoOpWhenNoBudgetSet() {
    InstanceOperationMaintenanceResult result = _handler.apply(CLUSTER,
        Arrays.asList("h1", "h2", "h3", "h4"), NOW_MS + 60_000L, NOW_MS);

    Assert.assertEquals(result.getApplied(), Arrays.asList("h1", "h2", "h3", "h4"));
    Assert.assertTrue(result.getRejected().isEmpty());
    verify(_dataAccessor, times(4)).updateProperty(any(PropertyKey.class), any(),
        any(InstanceConfig.class));
  }

  @Test
  public void testEnforceCap_AbsoluteBudgetPartialAccept() throws Exception {
    _clusterConfig.setInstanceOperationMaintenanceBudget(2);

    InstanceOperationMaintenanceResult result = _handler.apply(CLUSTER,
        Arrays.asList("h1", "h2", "h3"), NOW_MS + 60_000L, NOW_MS);

    Assert.assertEquals(result.getApplied(), Arrays.asList("h1", "h2"),
        "First N in input order get the marker");
    Assert.assertEquals(result.getRejected().keySet(), Collections.singleton("h3"));
    Assert.assertTrue(result.getRejected().get("h3")
        .contains("INSTANCE_OPERATION_MAINTENANCE_BUDGET=2"));
  }

  @Test
  public void testEnforceCap_AbsoluteBudgetAtBoundary() throws Exception {
    _clusterConfig.setInstanceOperationMaintenanceBudget(2);

    InstanceOperationMaintenanceResult result = _handler.apply(CLUSTER,
        Arrays.asList("h1", "h2"), NOW_MS + 60_000L, NOW_MS);

    Assert.assertEquals(result.getApplied(), Arrays.asList("h1", "h2"));
    Assert.assertTrue(result.getRejected().isEmpty());
  }

  @Test
  public void testEnforceCap_PercentageBudgetPartialAccept() throws Exception {
    // 25% of 4 = 1, so only the first incoming instance fits.
    _clusterConfig.setInstanceOperationMaintenanceBudgetPercentage(25);

    InstanceOperationMaintenanceResult result = _handler.apply(CLUSTER,
        Arrays.asList("h1", "h2"), NOW_MS + 60_000L, NOW_MS);

    Assert.assertEquals(result.getApplied(), Collections.singletonList("h1"));
    Assert.assertEquals(result.getRejected().keySet(), Collections.singleton("h2"));
    Assert.assertTrue(result.getRejected().get("h2")
        .contains("INSTANCE_OPERATION_MAINTENANCE_BUDGET_PERCENTAGE=25"));
  }

  @Test
  public void testEnforceCap_AccountsForExistingMarkers() throws Exception {
    // Pre-existing marker on h3 burns one slot of the budget.
    InstanceConfig h3 = new InstanceConfig("h3");
    h3.setInstanceOperationMaintenanceUntilMs(NOW_MS + 60_000L);
    _storedConfigs.put("h3", h3);

    _clusterConfig.setInstanceOperationMaintenanceBudget(2);

    InstanceOperationMaintenanceResult result = _handler.apply(CLUSTER,
        Arrays.asList("h1", "h2"), NOW_MS + 60_000L, NOW_MS);

    // Budget = 2; h3 already marked = 1 slot used; remaining quota = 1.
    Assert.assertEquals(result.getApplied(), Collections.singletonList("h1"));
    Assert.assertEquals(result.getRejected().keySet(), Collections.singleton("h2"));
  }

  @Test
  public void testEnforceCap_ExpiredMarkersDoNotCount() throws Exception {
    // h3 has an already-expired marker; it should not consume budget.
    InstanceConfig h3 = new InstanceConfig("h3");
    h3.setInstanceOperationMaintenanceUntilMs(NOW_MS - 1L);
    _storedConfigs.put("h3", h3);

    _clusterConfig.setInstanceOperationMaintenanceBudget(2);

    InstanceOperationMaintenanceResult result = _handler.apply(CLUSTER,
        Arrays.asList("h1", "h2"), NOW_MS + 60_000L, NOW_MS);

    Assert.assertEquals(result.getApplied(), Arrays.asList("h1", "h2"));
    Assert.assertTrue(result.getRejected().isEmpty());
  }

  @Test
  public void testEnforceCap_ZeroBudgetRejectsEverything() throws Exception {
    // Edge case: budget = 0 disables setting new markers entirely.
    _clusterConfig.setInstanceOperationMaintenanceBudget(0);

    InstanceOperationMaintenanceResult result = _handler.apply(CLUSTER,
        Arrays.asList("h1", "h2"), NOW_MS + 60_000L, NOW_MS);

    Assert.assertTrue(result.getApplied().isEmpty());
    Assert.assertEquals(result.getRejected().keySet().size(), 2);
    verify(_dataAccessor, never()).updateProperty(any(PropertyKey.class), any(),
        any(InstanceConfig.class));
  }

  @Test
  public void testEnforceCap_FailedWriteRejectedAndDoesNotConsumeQuota() throws Exception {
    _clusterConfig.setInstanceOperationMaintenanceBudget(1);
    doAnswer(invocation -> {
      DataUpdater<ZNRecord> updater = invocation.getArgument(1);
      InstanceConfig markerUpdate = invocation.getArgument(2);
      String instanceName = markerUpdate.getInstanceName();
      if ("h1".equals(instanceName)) {
        InstanceConfig stored = _storedConfigs.get(instanceName);
        updater.update(new ZNRecord(stored.getRecord()));
        return false;
      }
      return applyUpdater(instanceName, updater);
    }).when(_dataAccessor).updateProperty(any(PropertyKey.class), any(),
        any(InstanceConfig.class));

    InstanceOperationMaintenanceResult result = _handler.apply(CLUSTER,
        Arrays.asList("h1", "h2"), NOW_MS + 60_000L, NOW_MS);

    Assert.assertEquals(result.getApplied(), Collections.singletonList("h2"));
    Assert.assertEquals(result.getRejected().keySet(), Collections.singleton("h1"));
    Assert.assertTrue(result.isWriteFailure("h1"));
    Assert.assertTrue(result.getRejected().get("h1").contains("failed to update"));
    Assert.assertEquals(_storedConfigs.get("h1").getInstanceOperationMaintenanceUntilMs(),
        InstanceConfig.INSTANCE_OPERATION_MAINTENANCE_NOT_SET);
    Assert.assertEquals(_storedConfigs.get("h2").getInstanceOperationMaintenanceUntilMs(),
        NOW_MS + 60_000L);
  }

  // --- Clear path --------------------------------------------------------------------------

  @Test
  public void testApply_ClearWithNegativeSentinel() {
    InstanceConfig cfg = new InstanceConfig("h1");
    cfg.setInstanceOperationMaintenanceUntilMs(NOW_MS + 60_000L);
    _storedConfigs.put("h1", cfg);

    InstanceOperationMaintenanceResult result = _handler.apply(CLUSTER,
        Collections.singletonList("h1"),
        InstanceOperationMaintenanceWriteHandler.EXPIRES_AT_MILLIS_CLEAR, NOW_MS);

    Assert.assertEquals(result.getApplied(), Collections.singletonList("h1"));
    Assert.assertTrue(result.getRejected().isEmpty());
    Assert.assertEquals(result.getResolvedExpiresAtMillis(),
        InstanceConfig.INSTANCE_OPERATION_MAINTENANCE_NOT_SET);

    Assert.assertEquals(_storedConfigs.get("h1").getInstanceOperationMaintenanceUntilMs(),
        InstanceConfig.INSTANCE_OPERATION_MAINTENANCE_NOT_SET);
  }

  @Test
  public void testClearBypassesBudget() throws Exception {
    // Budget = 0 normally blocks all new markers. Clear must still succeed.
    _clusterConfig.setInstanceOperationMaintenanceBudget(0);
    InstanceConfig h1 = new InstanceConfig("h1");
    h1.setInstanceOperationMaintenanceUntilMs(NOW_MS + 60_000L);
    _storedConfigs.put("h1", h1);

    InstanceOperationMaintenanceResult result = _handler.apply(CLUSTER,
        Collections.singletonList("h1"),
        InstanceOperationMaintenanceWriteHandler.EXPIRES_AT_MILLIS_CLEAR, NOW_MS);

    Assert.assertEquals(result.getApplied(), Collections.singletonList("h1"));
    Assert.assertEquals(_storedConfigs.get("h1").getInstanceOperationMaintenanceUntilMs(),
        InstanceConfig.INSTANCE_OPERATION_MAINTENANCE_NOT_SET);
  }

  @Test
  public void testClear_MissingInstanceRejectedButOthersCleared() {
    InstanceConfig h1 = new InstanceConfig("h1");
    h1.setInstanceOperationMaintenanceUntilMs(NOW_MS + 60_000L);
    _storedConfigs.put("h1", h1);

    InstanceOperationMaintenanceResult result = _handler.apply(CLUSTER,
        Arrays.asList("h1", "h99"),
        InstanceOperationMaintenanceWriteHandler.EXPIRES_AT_MILLIS_CLEAR, NOW_MS);

    Assert.assertEquals(result.getApplied(), Collections.singletonList("h1"));
    Assert.assertEquals(result.getRejected().keySet(), Collections.singleton("h99"));
  }

  @Test
  public void testApply_ClearRetryPreservesConcurrentUnrelatedField() {
    InstanceConfig h1 = _storedConfigs.get("h1");
    h1.setInstanceOperationMaintenanceUntilMs(NOW_MS + 60_000L);
    h1.getRecord().setSimpleField("EXISTING_FIELD", "existing");
    stubRetryWithConcurrentField("CONCURRENT_FIELD", "preserved");

    InstanceOperationMaintenanceResult result = _handler.apply(CLUSTER,
        Collections.singletonList("h1"),
        InstanceOperationMaintenanceWriteHandler.EXPIRES_AT_MILLIS_CLEAR, NOW_MS);

    Assert.assertEquals(result.getApplied(), Collections.singletonList("h1"));
    InstanceConfig stored = _storedConfigs.get("h1");
    Assert.assertEquals(stored.getInstanceOperationMaintenanceUntilMs(),
        InstanceConfig.INSTANCE_OPERATION_MAINTENANCE_NOT_SET);
    Assert.assertEquals(stored.getRecord().getSimpleField("EXISTING_FIELD"), "existing");
    Assert.assertEquals(stored.getRecord().getSimpleField("CONCURRENT_FIELD"), "preserved");
  }

  private boolean applyUpdater(String instanceName, DataUpdater<ZNRecord> updater) {
    InstanceConfig stored = _storedConfigs.get(instanceName);
    ZNRecord current = stored == null ? null : new ZNRecord(stored.getRecord());
    ZNRecord updated = updater.update(current);
    if (updated != null) {
      _storedConfigs.put(instanceName, new InstanceConfig(updated));
    }
    return true;
  }

  private void stubRetryWithConcurrentField(String field, String value) {
    doAnswer(invocation -> {
      DataUpdater<ZNRecord> updater = invocation.getArgument(1);
      InstanceConfig markerUpdate = invocation.getArgument(2);
      String instanceName = markerUpdate.getInstanceName();
      InstanceConfig stored = _storedConfigs.get(instanceName);

      Assert.assertNotNull(updater.update(new ZNRecord(stored.getRecord())));
      InstanceConfig latest = new InstanceConfig(new ZNRecord(stored.getRecord()));
      latest.getRecord().setSimpleField(field, value);
      _storedConfigs.put(instanceName, latest);

      ZNRecord updated = updater.update(new ZNRecord(latest.getRecord()));
      Assert.assertNotNull(updated);
      _storedConfigs.put(instanceName, new InstanceConfig(updated));
      return true;
    }).when(_dataAccessor).updateProperty(any(PropertyKey.class), any(),
        any(InstanceConfig.class));
  }
}
