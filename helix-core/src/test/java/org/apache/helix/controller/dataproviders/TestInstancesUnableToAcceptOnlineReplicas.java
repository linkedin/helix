package org.apache.helix.controller.dataproviders;

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
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.apache.helix.constants.InstanceConstants;
import org.apache.helix.model.InstanceConfig;
import org.apache.helix.model.LiveInstance;
import org.testng.Assert;
import org.testng.annotations.Test;

/**
 * Unit tests for
 * {@link BaseControllerDataProvider#getInstancesUnableToAcceptOnlineReplicas(long)}.
 *
 * <p>Covers the full matrix of {@link InstanceConstants.InstanceOperation} values against the
 * enabled-live and instance-operation maintenance marker dimensions. The accessor is the
 * shared offline-budget computation used by both MM entry
 * ({@code BestPossibleStateCalcStage}) and MM exit ({@code MaintenanceRecoveryStage}); these
 * tests lock in the contract that both stages observe the same population.
 */
public class TestInstancesUnableToAcceptOnlineReplicas {

  private static final long NOW_MS = 1_000_000L;
  private static final long FUTURE_MS = NOW_MS + 60_000L;
  private static final long PAST_MS = NOW_MS - 1L;

  // ----- Single-operation cases (no markers) ----------------------------------------------

  @Test
  public void testEnableLiveExcluded() {
    BaseControllerDataProvider provider = providerWith(
        configs(config("h1", InstanceConstants.InstanceOperation.ENABLE)),
        liveInstances("h1"));
    Assert.assertTrue(provider.getInstancesUnableToAcceptOnlineReplicas(NOW_MS).isEmpty(),
        "Healthy ENABLE+live instance must not count toward the offline budget");
  }

  @Test
  public void testEnableOfflineIncluded() {
    BaseControllerDataProvider provider = providerWith(
        configs(config("h1", InstanceConstants.InstanceOperation.ENABLE)),
        liveInstances());
    Assert.assertEquals(provider.getInstancesUnableToAcceptOnlineReplicas(NOW_MS),
        setOf("h1"), "ENABLE+offline is the canonical 'real outage' case and must count");
  }

  @Test
  public void testDisableLiveIncluded() {
    BaseControllerDataProvider provider = providerWith(
        configs(config("h1", InstanceConstants.InstanceOperation.DISABLE)),
        liveInstances("h1"));
    Assert.assertEquals(provider.getInstancesUnableToAcceptOnlineReplicas(NOW_MS),
        setOf("h1"),
        "DISABLE instances cannot accept ONLINE replicas and must count regardless of liveness");
  }

  @Test
  public void testDisableOfflineIncluded() {
    BaseControllerDataProvider provider = providerWith(
        configs(config("h1", InstanceConstants.InstanceOperation.DISABLE)),
        liveInstances());
    Assert.assertEquals(provider.getInstancesUnableToAcceptOnlineReplicas(NOW_MS),
        setOf("h1"));
  }

  @Test
  public void testEvacuateLiveIncluded() {
    BaseControllerDataProvider provider = providerWith(
        configs(config("h1", InstanceConstants.InstanceOperation.EVACUATE)),
        liveInstances("h1"));
    Assert.assertEquals(provider.getInstancesUnableToAcceptOnlineReplicas(NOW_MS),
        setOf("h1"),
        "EVACUATE is the asymmetric op pre-fix; both entry and exit must now count it");
  }

  @Test
  public void testEvacuateOfflineIncluded() {
    BaseControllerDataProvider provider = providerWith(
        configs(config("h1", InstanceConstants.InstanceOperation.EVACUATE)),
        liveInstances());
    Assert.assertEquals(provider.getInstancesUnableToAcceptOnlineReplicas(NOW_MS),
        setOf("h1"));
  }

  @Test
  public void testSwapInExcluded() {
    BaseControllerDataProvider provider = providerWith(
        configs(config("h1", InstanceConstants.InstanceOperation.SWAP_IN)),
        liveInstances());
    Assert.assertTrue(provider.getInstancesUnableToAcceptOnlineReplicas(NOW_MS).isEmpty(),
        "SWAP_IN is in UNROUTABLE_INSTANCE_OPERATIONS and must never count");
  }

  @Test
  public void testUnknownExcluded() {
    BaseControllerDataProvider provider = providerWith(
        configs(config("h1", InstanceConstants.InstanceOperation.UNKNOWN)),
        liveInstances());
    Assert.assertTrue(provider.getInstancesUnableToAcceptOnlineReplicas(NOW_MS).isEmpty(),
        "UNKNOWN is in UNROUTABLE_INSTANCE_OPERATIONS and must never count");
  }

  // ----- Marker interactions --------------------------------------------------------------

  @Test
  public void testValidMarkerExemptsEnableOffline() {
    BaseControllerDataProvider provider = providerWith(
        configs(configWithMarker("h1", InstanceConstants.InstanceOperation.ENABLE, FUTURE_MS)),
        liveInstances());
    Assert.assertTrue(provider.getInstancesUnableToAcceptOnlineReplicas(NOW_MS).isEmpty(),
        "Valid marker on ENABLE+offline must exempt the instance from the budget");
  }

  @Test
  public void testValidMarkerExemptsEvacuate() {
    BaseControllerDataProvider provider = providerWith(
        configs(configWithMarker("h1", InstanceConstants.InstanceOperation.EVACUATE, FUTURE_MS)),
        liveInstances());
    Assert.assertTrue(provider.getInstancesUnableToAcceptOnlineReplicas(NOW_MS).isEmpty(),
        "Valid marker on EVACUATE must exempt — this is the orchestrator-driven decom case");
  }

  @Test
  public void testValidMarkerExemptsDisable() {
    BaseControllerDataProvider provider = providerWith(
        configs(configWithMarker("h1", InstanceConstants.InstanceOperation.DISABLE, FUTURE_MS)),
        liveInstances());
    Assert.assertTrue(provider.getInstancesUnableToAcceptOnlineReplicas(NOW_MS).isEmpty());
  }

  @Test
  public void testExpiredMarkerDoesNotExempt() {
    BaseControllerDataProvider provider = providerWith(
        configs(configWithMarker("h1", InstanceConstants.InstanceOperation.ENABLE, PAST_MS)),
        liveInstances());
    Assert.assertEquals(provider.getInstancesUnableToAcceptOnlineReplicas(NOW_MS),
        setOf("h1"), "Expired marker must behave as if the marker were absent");
  }

  @Test
  public void testBoundaryNowEqualsUntil() {
    // isUnderInstanceOperationMaintenance uses strict `nowMs < until`, so nowMs == until is
    // already past the window.
    BaseControllerDataProvider provider = providerWith(
        configs(configWithMarker("h1", InstanceConstants.InstanceOperation.ENABLE, NOW_MS)),
        liveInstances());
    Assert.assertEquals(provider.getInstancesUnableToAcceptOnlineReplicas(NOW_MS),
        setOf("h1"), "nowMs == untilMs is no longer under maintenance; instance must count");
  }

  @Test
  public void testMarkerOnSwapInIsIrrelevant() {
    // SWAP_IN is filtered out before the marker check; a marker on it must not change that.
    BaseControllerDataProvider provider = providerWith(
        configs(configWithMarker("h1", InstanceConstants.InstanceOperation.SWAP_IN, FUTURE_MS)),
        liveInstances());
    Assert.assertTrue(provider.getInstancesUnableToAcceptOnlineReplicas(NOW_MS).isEmpty(),
        "Marker on a SWAP_IN instance must not change the (already excluded) outcome");
  }

  // ----- Aggregate / realistic-mix cases --------------------------------------------------

  @Test
  public void testEmptyCluster() {
    BaseControllerDataProvider provider = providerWith(configs(), liveInstances());
    Assert.assertTrue(provider.getInstancesUnableToAcceptOnlineReplicas(NOW_MS).isEmpty());
  }

  @Test
  public void testMixedCluster() {
    // 8 ENABLE+live, 1 ENABLE+offline (counts), 1 ENABLE+offline w/marker (exempt),
    // 1 DISABLE (counts), 1 EVACUATE (counts), 1 EVACUATE w/marker (exempt),
    // 1 SWAP_IN (excluded), 1 UNKNOWN (excluded).
    Map<String, InstanceConfig> instanceConfigMap = new HashMap<>();
    Set<String> liveInstanceNames = new HashSet<>();
    for (int i = 0; i < 8; i++) {
      String name = "enable-live-" + i;
      instanceConfigMap.put(name, config(name, InstanceConstants.InstanceOperation.ENABLE));
      liveInstanceNames.add(name);
    }
    instanceConfigMap.put("enable-offline",
        config("enable-offline", InstanceConstants.InstanceOperation.ENABLE));
    instanceConfigMap.put("enable-offline-marked",
        configWithMarker("enable-offline-marked",
            InstanceConstants.InstanceOperation.ENABLE, FUTURE_MS));
    instanceConfigMap.put("disable",
        config("disable", InstanceConstants.InstanceOperation.DISABLE));
    instanceConfigMap.put("evacuate",
        config("evacuate", InstanceConstants.InstanceOperation.EVACUATE));
    instanceConfigMap.put("evacuate-marked",
        configWithMarker("evacuate-marked",
            InstanceConstants.InstanceOperation.EVACUATE, FUTURE_MS));
    instanceConfigMap.put("swap-in",
        config("swap-in", InstanceConstants.InstanceOperation.SWAP_IN));
    instanceConfigMap.put("unknown",
        config("unknown", InstanceConstants.InstanceOperation.UNKNOWN));

    BaseControllerDataProvider provider = providerWith(instanceConfigMap, liveInstanceNames);

    Assert.assertEquals(provider.getInstancesUnableToAcceptOnlineReplicas(NOW_MS),
        setOf("enable-offline", "disable", "evacuate"),
        "Only unmarked, non-routable instances that aren't enabled-live should count");
  }

  // ----- Returned-set contract ------------------------------------------------------------

  @Test
  public void testReturnedSetIsModifiable() {
    // Docstring promises a "fresh modifiable set"; callers (BestPossibleStateCalcStage in
    // particular) rely on this for downstream filtering. Verify both properties hold.
    BaseControllerDataProvider provider = providerWith(
        configs(config("h1", InstanceConstants.InstanceOperation.ENABLE)),
        liveInstances());
    Set<String> result = provider.getInstancesUnableToAcceptOnlineReplicas(NOW_MS);
    result.add("h99");
    Assert.assertTrue(result.contains("h99"), "Returned set must be modifiable");
  }

  @Test
  public void testReturnedSetIsIndependentOfFutureCalls() {
    BaseControllerDataProvider provider = providerWith(
        configs(config("h1", InstanceConstants.InstanceOperation.ENABLE)),
        liveInstances());
    Set<String> first = provider.getInstancesUnableToAcceptOnlineReplicas(NOW_MS);
    first.clear();
    Set<String> second = provider.getInstancesUnableToAcceptOnlineReplicas(NOW_MS);
    Assert.assertEquals(second, setOf("h1"),
        "Mutating an earlier result must not affect later calls");
  }

  // ----- Helpers --------------------------------------------------------------------------

  /**
   * Builds a {@link BaseControllerDataProvider} whose only contract is to return the
   * supplied instance-config map and live-instance set. The accessor under test depends on
   * exactly those two hooks plus {@link InstanceConfig#isUnderInstanceOperationMaintenance},
   * so overriding the two getters is sufficient and avoids initializing the full cluster
   * cache machinery. The enabled-live subset is derived from the configs the same way the
   * controller derives it, so the ENABLE filter is exercised here rather than stubbed.
   */
  private static BaseControllerDataProvider providerWith(
      Map<String, InstanceConfig> instanceConfigMap, Set<String> liveInstanceNames) {
    Map<String, LiveInstance> liveInstanceMap = new HashMap<>();
    for (String name : liveInstanceNames) {
      liveInstanceMap.put(name, new LiveInstance(name));
    }
    return new BaseControllerDataProvider() {
      @Override
      public Map<String, InstanceConfig> getInstanceConfigMap() {
        return instanceConfigMap;
      }

      @Override
      public Map<String, LiveInstance> getLiveInstances() {
        return Collections.unmodifiableMap(liveInstanceMap);
      }
    };
  }

  private static InstanceConfig config(String name,
      InstanceConstants.InstanceOperation operation) {
    InstanceConfig cfg = new InstanceConfig(name);
    cfg.setInstanceOperation(operation);
    return cfg;
  }

  private static InstanceConfig configWithMarker(String name,
      InstanceConstants.InstanceOperation operation, long untilMs) {
    InstanceConfig cfg = config(name, operation);
    cfg.setInstanceOperationMaintenanceUntilMs(untilMs);
    return cfg;
  }

  private static Map<String, InstanceConfig> configs(InstanceConfig... cfgs) {
    Map<String, InstanceConfig> map = new HashMap<>();
    for (InstanceConfig cfg : cfgs) {
      map.put(cfg.getInstanceName(), cfg);
    }
    return map;
  }

  /**
   * Test-side set builder. Used for both the live-instance input set and the expected-result
   * set so that assertions and setup share the same shape; the two readings sit at the call
   * site through the method name on the input side and explicit assertEquals on the result.
   */
  private static Set<String> setOf(String... names) {
    Set<String> set = new HashSet<>();
    Collections.addAll(set, names);
    return set;
  }

  private static Set<String> liveInstances(String... names) {
    return setOf(names);
  }
}
