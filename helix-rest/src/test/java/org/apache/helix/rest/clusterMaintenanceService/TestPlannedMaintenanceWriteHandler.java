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
import static org.mockito.Mockito.atLeastOnce;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.helix.ConfigAccessor;
import org.apache.helix.HelixAdmin;
import org.apache.helix.model.ClusterConfig;
import org.apache.helix.model.InstanceConfig;
import org.testng.Assert;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

public class TestPlannedMaintenanceWriteHandler {
  private static final String CLUSTER = "test-cluster";
  private static final long NOW_MS = 1_000_000L;
  private static final long ONE_HOUR_MS = 3_600_000L;

  private HelixAdmin _admin;
  private ConfigAccessor _configAccessor;
  private ClusterConfig _clusterConfig;
  private Map<String, InstanceConfig> _storedConfigs;
  private PlannedMaintenanceWriteHandler _handler;

  @BeforeMethod
  public void setUp() {
    _admin = mock(HelixAdmin.class);
    _configAccessor = mock(ConfigAccessor.class);
    _clusterConfig = new ClusterConfig(CLUSTER);
    _storedConfigs = new HashMap<>();
    when(_configAccessor.getClusterConfig(CLUSTER)).thenReturn(_clusterConfig);
    when(_admin.getInstancesInCluster(CLUSTER))
        .thenAnswer(invocation -> Arrays.asList("h1", "h2", "h3", "h4"));
    when(_admin.getInstanceConfig(eq(CLUSTER), anyString()))
        .thenAnswer(invocation -> _storedConfigs.computeIfAbsent(invocation.getArgument(1),
            InstanceConfig::new));
    doAnswer(invocation -> {
      String name = invocation.getArgument(1);
      InstanceConfig written = invocation.getArgument(2);
      _storedConfigs.put(name, written);
      return null;
    }).when(_configAccessor).setInstanceConfig(eq(CLUSTER), anyString(), any(InstanceConfig.class));

    _handler = new PlannedMaintenanceWriteHandler(_admin, _configAccessor);
  }

  // --- TTL resolution ----------------------------------------------------------------------

  @Test
  public void testResolveExpiresAtMillis_CallerWins() {
    long caller = NOW_MS + 5_000L;
    long resolved =
        PlannedMaintenanceWriteHandler.resolveExpiresAtMillis(caller, _clusterConfig, NOW_MS);
    Assert.assertEquals(resolved, caller);
  }

  @Test
  public void testResolveExpiresAtMillis_FallsBackToClusterDefault() {
    _clusterConfig.setDefaultPlannedMaintenanceDurationMs(ONE_HOUR_MS);
    long resolved = PlannedMaintenanceWriteHandler.resolveExpiresAtMillis(
        PlannedMaintenanceWriteHandler.EXPIRES_AT_MILLIS_UNSET, _clusterConfig, NOW_MS);
    Assert.assertEquals(resolved, NOW_MS + ONE_HOUR_MS);
  }

  @Test(expectedExceptions = PlannedMaintenanceWriteHandler.BadRequestException.class)
  public void testResolveExpiresAtMillis_RejectsWhenNeitherSet() {
    PlannedMaintenanceWriteHandler.resolveExpiresAtMillis(
        PlannedMaintenanceWriteHandler.EXPIRES_AT_MILLIS_UNSET, _clusterConfig, NOW_MS);
  }

  @Test(expectedExceptions = PlannedMaintenanceWriteHandler.BadRequestException.class)
  public void testResolveExpiresAtMillis_RejectsCallerValueInThePast() {
    PlannedMaintenanceWriteHandler.resolveExpiresAtMillis(NOW_MS - 1L, _clusterConfig, NOW_MS);
  }

  // --- Write path: set and clear -----------------------------------------------------------

  @Test
  public void testApplyPlannedMaintenance_WritesMarkerAndMetadata() {
    long expiresAt = NOW_MS + 60_000L;
    long resolved = _handler.applyPlannedMaintenance(CLUSTER, Collections.singletonList("h1"),
        expiresAt, "venice-deploy:opId=abc", "AUTOMATION", NOW_MS);

    Assert.assertEquals(resolved, expiresAt);
    InstanceConfig stored = _storedConfigs.get("h1");
    Assert.assertNotNull(stored);
    Assert.assertEquals(stored.getPlannedMaintenanceUntilMs(), expiresAt);
    Map<String, String> meta = stored.getPlannedMaintenanceMetadata();
    Assert.assertEquals(meta.get(InstanceConfig.PlannedMaintenanceMetadataKey.REASON),
        "venice-deploy:opId=abc");
    Assert.assertEquals(meta.get(InstanceConfig.PlannedMaintenanceMetadataKey.SOURCE),
        "AUTOMATION");
    Assert.assertEquals(meta.get(InstanceConfig.PlannedMaintenanceMetadataKey.SET_AT_MS),
        Long.toString(NOW_MS));
  }

  @Test
  public void testApplyPlannedMaintenance_ClearWithNegativeSentinel() {
    InstanceConfig cfg = new InstanceConfig("h1");
    cfg.setPlannedMaintenanceUntilMs(NOW_MS + 60_000L);
    cfg.setPlannedMaintenanceMetadata(java.util.Collections.singletonMap("reason", "x"));
    _storedConfigs.put("h1", cfg);

    long resolved = _handler.applyPlannedMaintenance(CLUSTER, Collections.singletonList("h1"),
        PlannedMaintenanceWriteHandler.EXPIRES_AT_MILLIS_CLEAR, null, null, NOW_MS);

    Assert.assertEquals(resolved, InstanceConfig.PLANNED_MAINTENANCE_NOT_SET);
    InstanceConfig stored = _storedConfigs.get("h1");
    Assert.assertEquals(stored.getPlannedMaintenanceUntilMs(),
        InstanceConfig.PLANNED_MAINTENANCE_NOT_SET);
    Assert.assertTrue(stored.getPlannedMaintenanceMetadata().isEmpty());
  }

  @Test
  public void testApplyPlannedMaintenance_DeduplicatesInstanceList() {
    long expiresAt = NOW_MS + 60_000L;
    _handler.applyPlannedMaintenance(CLUSTER, Arrays.asList("h1", "h2", "h1", "h2"),
        expiresAt, null, null, NOW_MS);

    // Each unique instance should be written exactly once.
    verify(_configAccessor, times(1)).setInstanceConfig(eq(CLUSTER), eq("h1"),
        any(InstanceConfig.class));
    verify(_configAccessor, times(1)).setInstanceConfig(eq(CLUSTER), eq("h2"),
        any(InstanceConfig.class));
  }

  @Test(expectedExceptions = PlannedMaintenanceWriteHandler.BadRequestException.class)
  public void testApplyPlannedMaintenance_EmptyListRejected() {
    _handler.applyPlannedMaintenance(CLUSTER, Collections.emptyList(), NOW_MS + 60_000L, null,
        null, NOW_MS);
  }

  @Test(expectedExceptions = PlannedMaintenanceWriteHandler.BadRequestException.class)
  public void testApplyPlannedMaintenance_NullInstanceNameRejected() {
    _handler.applyPlannedMaintenance(CLUSTER, Arrays.asList("h1", null), NOW_MS + 60_000L,
        null, null, NOW_MS);
  }

  @Test
  public void testApplyPlannedMaintenance_ValidatesExistenceBeforeWriting() {
    // Force h99 to be "not found" via ConfigAccessor; verify that no writes happen for h1.
    when(_admin.getInstanceConfig(eq(CLUSTER), eq("h99"))).thenReturn(null);

    try {
      _handler.applyPlannedMaintenance(CLUSTER, Arrays.asList("h1", "h99"), NOW_MS + 60_000L,
          null, null, NOW_MS);
      Assert.fail("Expected BadRequestException for missing instance");
    } catch (PlannedMaintenanceWriteHandler.BadRequestException expected) {
      // expected
    }

    // No writes should have occurred because validation precedes any mutation.
    verify(_configAccessor, never()).setInstanceConfig(eq(CLUSTER), anyString(),
        any(InstanceConfig.class));
  }

  // --- Cap enforcement ---------------------------------------------------------------------

  @Test
  public void testEnforceCap_NoOpWhenNeitherCapSet() {
    long expiresAt = NOW_MS + 60_000L;
    _handler.applyPlannedMaintenance(CLUSTER, Arrays.asList("h1", "h2", "h3", "h4"),
        expiresAt, null, null, NOW_MS);
    verify(_configAccessor, times(4)).setInstanceConfig(eq(CLUSTER), anyString(),
        any(InstanceConfig.class));
  }

  @Test(expectedExceptions = PlannedMaintenanceWriteHandler.BadRequestException.class,
      expectedExceptionsMessageRegExp = ".*exceeds cluster cap.*")
  public void testEnforceCap_AbsoluteCapBlocks() {
    _clusterConfig.setMaxPlannedMaintenanceInstances(2);
    _handler.applyPlannedMaintenance(CLUSTER, Arrays.asList("h1", "h2", "h3"),
        NOW_MS + 60_000L, null, null, NOW_MS);
  }

  @Test
  public void testEnforceCap_AbsoluteCapAtBoundary() {
    _clusterConfig.setMaxPlannedMaintenanceInstances(2);
    _handler.applyPlannedMaintenance(CLUSTER, Arrays.asList("h1", "h2"), NOW_MS + 60_000L,
        null, null, NOW_MS);
    verify(_configAccessor, times(2)).setInstanceConfig(eq(CLUSTER), anyString(),
        any(InstanceConfig.class));
  }

  @Test(expectedExceptions = PlannedMaintenanceWriteHandler.BadRequestException.class)
  public void testEnforceCap_PercentageCapBlocks() {
    // 25% of 4 = 1, two markers exceed.
    _clusterConfig.setMaxPlannedMaintenancePercentage(25);
    _handler.applyPlannedMaintenance(CLUSTER, Arrays.asList("h1", "h2"), NOW_MS + 60_000L,
        null, null, NOW_MS);
  }

  @Test
  public void testEnforceCap_StricterOfTwoWins() {
    _clusterConfig.setMaxPlannedMaintenanceInstances(3);
    // 25% of 4 = 1, stricter than absolute 3, so 2 markers should be rejected.
    _clusterConfig.setMaxPlannedMaintenancePercentage(25);
    try {
      _handler.applyPlannedMaintenance(CLUSTER, Arrays.asList("h1", "h2"), NOW_MS + 60_000L,
          null, null, NOW_MS);
      Assert.fail("Expected BadRequestException when percentage cap is stricter");
    } catch (PlannedMaintenanceWriteHandler.BadRequestException expected) {
      // expected
    }
  }

  @Test
  public void testEnforceCap_AccountsForExistingMarkers() {
    // Pre-existing marker on h3.
    InstanceConfig h3 = new InstanceConfig("h3");
    h3.setPlannedMaintenanceUntilMs(NOW_MS + 60_000L);
    _storedConfigs.put("h3", h3);

    _clusterConfig.setMaxPlannedMaintenanceInstances(2);
    // h1 + h2 incoming -> total marked would be 3 (h1, h2, h3) > cap 2.
    try {
      _handler.applyPlannedMaintenance(CLUSTER, Arrays.asList("h1", "h2"), NOW_MS + 60_000L,
          null, null, NOW_MS);
      Assert.fail("Existing markers must count against the cap");
    } catch (PlannedMaintenanceWriteHandler.BadRequestException expected) {
      // expected
    }
  }

  @Test
  public void testEnforceCap_ExpiredMarkersDoNotCount() {
    // Pre-existing marker on h3 that has already expired.
    InstanceConfig h3 = new InstanceConfig("h3");
    h3.setPlannedMaintenanceUntilMs(NOW_MS - 1L);
    _storedConfigs.put("h3", h3);

    _clusterConfig.setMaxPlannedMaintenanceInstances(2);
    // h1 + h2 incoming, expired h3 does not count -> total 2, at the cap boundary, allowed.
    _handler.applyPlannedMaintenance(CLUSTER, Arrays.asList("h1", "h2"), NOW_MS + 60_000L,
        null, null, NOW_MS);
    verify(_configAccessor, atLeastOnce()).setInstanceConfig(eq(CLUSTER), eq("h1"),
        any(InstanceConfig.class));
  }

  @Test
  public void testClearBypassesCap() {
    // Cap is 0 (no new markers allowed). Clear must still succeed.
    _clusterConfig.setMaxPlannedMaintenanceInstances(0);
    InstanceConfig h1 = new InstanceConfig("h1");
    h1.setPlannedMaintenanceUntilMs(NOW_MS + 60_000L);
    _storedConfigs.put("h1", h1);

    _handler.applyPlannedMaintenance(CLUSTER, Collections.singletonList("h1"),
        PlannedMaintenanceWriteHandler.EXPIRES_AT_MILLIS_CLEAR, null, null, NOW_MS);

    Assert.assertEquals(_storedConfigs.get("h1").getPlannedMaintenanceUntilMs(),
        InstanceConfig.PLANNED_MAINTENANCE_NOT_SET);
  }

  @Test
  public void testApplyPlannedMaintenance_WithoutAuditFieldsOmitsMetadata() {
    long expiresAt = NOW_MS + 60_000L;
    _handler.applyPlannedMaintenance(CLUSTER, Collections.singletonList("h1"), expiresAt, null,
        null, NOW_MS);
    Assert.assertTrue(_storedConfigs.get("h1").getPlannedMaintenanceMetadata().isEmpty(),
        "No metadata fields should be written when reason and source are both null");
  }
}
