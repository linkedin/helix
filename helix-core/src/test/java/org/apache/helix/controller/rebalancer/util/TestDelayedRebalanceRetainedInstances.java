package org.apache.helix.controller.rebalancer.util;

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
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.apache.helix.constants.InstanceConstants;
import org.apache.helix.model.ClusterConfig;
import org.apache.helix.model.IdealState;
import org.apache.helix.model.InstanceConfig;
import org.testng.Assert;
import org.testng.annotations.Test;

/**
 * Unit tests for
 * {@link DelayedRebalanceUtil#getDelayedRebalanceRetainedInstances(Set, Set, Map, Set, Map,
 * ClusterConfig, long)} and its explicit-delay overload.
 *
 * <p>The calculation is the one the rebalancers apply through
 * {@link DelayedRebalanceUtil#getActiveNodes(Set, Set, Map, Set, Map, ClusterConfig)}; the tests
 * at the end of this class pin that equivalence so the two cannot drift.
 */
public class TestDelayedRebalanceRetainedInstances {

  private static final long OBSERVED_MS = 1_000_000L;
  private static final long DELAY_MS = 60_000L;
  private static final String INSTANCE = "instance0";

  @Test(expectedExceptions = IllegalArgumentException.class)
  public void testMissingCandidateConfigIsAnError() {
    DelayedRebalanceUtil.getDelayedRebalanceRetainedInstances(Collections.singleton(INSTANCE),
        Collections.emptySet(), Collections.emptyMap(), Collections.emptySet(),
        Collections.emptyMap(), delayedClusterConfig(), OBSERVED_MS);
  }

  // ----- Live and enabled instances -------------------------------------------------------

  @Test
  public void testLiveEnabledInstanceIsNotRetained() {
    InstanceConfig config = enabledConfig(INSTANCE);
    Map<String, Long> retained = retainedInstances(configs(config), liveNodes(INSTANCE),
        liveEnabledNodes(INSTANCE), offlineTimes(), delayedClusterConfig());
    Assert.assertTrue(retained.isEmpty(),
        "An ordinary live and enabled instance is active on its own and is never delay-retained");
  }

  // ----- Offline instances ----------------------------------------------------------------

  @Test
  public void testOfflineInstanceIsRetainedUntilOfflineTimePlusDelay() {
    InstanceConfig config = enabledConfig(INSTANCE);
    long offlineTime = OBSERVED_MS - 10_000L;
    Map<String, Long> retained = retainedInstances(configs(config), liveNodes(),
        liveEnabledNodes(), offlineTimes(INSTANCE, offlineTime), delayedClusterConfig());
    Assert.assertEquals(retained, Collections.singletonMap(INSTANCE, offlineTime + DELAY_MS),
        "An offline instance is retained until its offline time plus the cluster delay");
  }

  @Test
  public void testOfflineInstanceIsRetainedOneMillisecondBeforeExpiry() {
    InstanceConfig config = enabledConfig(INSTANCE);
    long offlineTime = OBSERVED_MS - DELAY_MS + 1L;
    Map<String, Long> retained = retainedInstances(configs(config), liveNodes(),
        liveEnabledNodes(), offlineTimes(INSTANCE, offlineTime), delayedClusterConfig());
    Assert.assertEquals(retained, Collections.singletonMap(INSTANCE, OBSERVED_MS + 1L),
        "The window is open while the expiry is still in the future");
  }

  @Test
  public void testOfflineInstanceIsNotRetainedWhenExpiryEqualsObservedTime() {
    InstanceConfig config = enabledConfig(INSTANCE);
    Map<String, Long> retained = retainedInstances(configs(config), liveNodes(),
        liveEnabledNodes(), offlineTimes(INSTANCE, OBSERVED_MS - DELAY_MS),
        delayedClusterConfig());
    Assert.assertTrue(retained.isEmpty(),
        "An expiry that has been reached is no longer a window; the comparison is strict");
  }

  @Test
  public void testOfflineInstanceIsNotRetainedAfterExpiry() {
    InstanceConfig config = enabledConfig(INSTANCE);
    Map<String, Long> retained = retainedInstances(configs(config), liveNodes(),
        liveEnabledNodes(), offlineTimes(INSTANCE, OBSERVED_MS - DELAY_MS - 1L),
        delayedClusterConfig());
    Assert.assertTrue(retained.isEmpty());
  }

  @Test
  public void testOfflineInstanceWithoutRecordedOfflineTimeIsNotRetained() {
    InstanceConfig config = enabledConfig(INSTANCE);
    Map<String, Long> retained = retainedInstances(configs(config), liveNodes(),
        liveEnabledNodes(), offlineTimes(), delayedClusterConfig());
    Assert.assertTrue(retained.isEmpty(),
        "Without a recorded offline time there is no window to compute, so nothing is retained");
  }

  // ----- Disabled instances ---------------------------------------------------------------

  @Test
  public void testDisabledLiveInstanceIsRetainedUntilDisabledTimePlusDelay() {
    long disabledTime = OBSERVED_MS - 10_000L;
    InstanceConfig config = disabledConfig(INSTANCE, disabledTime);
    Map<String, Long> retained = retainedInstances(configs(config), liveNodes(INSTANCE),
        liveEnabledNodes(), offlineTimes(), delayedClusterConfig());
    Assert.assertEquals(retained, Collections.singletonMap(INSTANCE, disabledTime + DELAY_MS),
        "A disabled instance is retained even while it is live");
  }

  @Test
  public void testDisabledAndOfflineInstanceUsesTheEarlierTimestamp() {
    long disabledTime = OBSERVED_MS - 10_000L;
    long offlineTime = OBSERVED_MS - 20_000L;
    InstanceConfig config = disabledConfig(INSTANCE, disabledTime);
    Map<String, Long> retained = retainedInstances(configs(config), liveNodes(),
        liveEnabledNodes(), offlineTimes(INSTANCE, offlineTime), delayedClusterConfig());
    Assert.assertEquals(retained, Collections.singletonMap(INSTANCE, offlineTime + DELAY_MS),
        "The window closes at the earlier of the offline and disabled timestamps plus the delay");
  }

  @Test
  public void testBatchDisableTimestampIsUsedWhenItIsEarlier() {
    long instanceDisabledTime = OBSERVED_MS - 10_000L;
    long batchDisabledTime = OBSERVED_MS - 30_000L;
    InstanceConfig config = disabledConfig(INSTANCE, instanceDisabledTime);
    ClusterConfig clusterConfig = delayedClusterConfig();
    setBatchDisableTimestamp(clusterConfig, INSTANCE, batchDisabledTime);

    Map<String, Long> retained = retainedInstances(configs(config), liveNodes(INSTANCE),
        liveEnabledNodes(), offlineTimes(), clusterConfig);
    Assert.assertEquals(retained, Collections.singletonMap(INSTANCE, batchDisabledTime + DELAY_MS),
        "A batch disable recorded on the cluster config wins when it is the earlier timestamp");
  }

  @Test
  public void testBatchDisableTimestampIsIgnoredWhenItIsLater() {
    long instanceDisabledTime = OBSERVED_MS - 30_000L;
    long batchDisabledTime = OBSERVED_MS - 10_000L;
    InstanceConfig config = disabledConfig(INSTANCE, instanceDisabledTime);
    ClusterConfig clusterConfig = delayedClusterConfig();
    setBatchDisableTimestamp(clusterConfig, INSTANCE, batchDisabledTime);

    Map<String, Long> retained = retainedInstances(configs(config), liveNodes(INSTANCE),
        liveEnabledNodes(), offlineTimes(), clusterConfig);
    Assert.assertEquals(retained,
        Collections.singletonMap(INSTANCE, instanceDisabledTime + DELAY_MS));
  }

  // ----- Delay rebalance switches ---------------------------------------------------------

  @Test
  public void testPerInstanceDelayRebalanceDisabledIsNotRetained() {
    InstanceConfig config = enabledConfig(INSTANCE);
    config.setDelayRebalanceEnabled(false);
    Map<String, Long> retained = retainedInstances(configs(config), liveNodes(),
        liveEnabledNodes(), offlineTimes(INSTANCE, OBSERVED_MS - 10_000L), delayedClusterConfig());
    Assert.assertTrue(retained.isEmpty(),
        "An instance that opts out of delayed rebalance is never retained");
  }

  @Test
  public void testClusterDelayRebalanceDisabledRetainsNothing() {
    InstanceConfig config = enabledConfig(INSTANCE);
    ClusterConfig clusterConfig = delayedClusterConfig();
    clusterConfig.setDelayRebalaceEnabled(false);
    Map<String, Long> retained = retainedInstances(configs(config), liveNodes(),
        liveEnabledNodes(), offlineTimes(INSTANCE, OBSERVED_MS - 10_000L), clusterConfig);
    Assert.assertTrue(retained.isEmpty(),
        "The cluster-level switch turns the whole population off");
  }

  @Test
  public void testUnsetClusterDelayRetainsNothing() {
    InstanceConfig config = enabledConfig(INSTANCE);
    ClusterConfig clusterConfig = new ClusterConfig("cluster0");
    Assert.assertEquals(clusterConfig.getRebalanceDelayTime(), -1L,
        "This test relies on the unset cluster delay being negative");
    Map<String, Long> retained = retainedInstances(configs(config), liveNodes(),
        liveEnabledNodes(), offlineTimes(INSTANCE, OBSERVED_MS - 10_000L), clusterConfig);
    Assert.assertTrue(retained.isEmpty(),
        "A cluster with no configured delay retains nothing, however the switch is set");
  }

  // ----- On-demand rebalance override -----------------------------------------------------

  @Test
  public void testOnDemandRebalanceAfterTheOfflineTimeDropsTheInstance() {
    InstanceConfig config = enabledConfig(INSTANCE);
    long offlineTime = OBSERVED_MS - 10_000L;
    ClusterConfig clusterConfig = delayedClusterConfig();
    clusterConfig.setLastOnDemandRebalanceTimestamp(offlineTime + 1L);
    Map<String, Long> retained = retainedInstances(configs(config), liveNodes(),
        liveEnabledNodes(), offlineTimes(INSTANCE, offlineTime), clusterConfig);
    Assert.assertTrue(retained.isEmpty(),
        "An on-demand rebalance after the instance went offline ends its window early");
  }

  @Test
  public void testOnDemandRebalanceBeforeTheOfflineTimeKeepsTheInstance() {
    InstanceConfig config = enabledConfig(INSTANCE);
    long offlineTime = OBSERVED_MS - 10_000L;
    ClusterConfig clusterConfig = delayedClusterConfig();
    clusterConfig.setLastOnDemandRebalanceTimestamp(offlineTime - 1L);
    Map<String, Long> retained = retainedInstances(configs(config), liveNodes(),
        liveEnabledNodes(), offlineTimes(INSTANCE, offlineTime), clusterConfig);
    Assert.assertEquals(retained, Collections.singletonMap(INSTANCE, offlineTime + DELAY_MS),
        "An on-demand rebalance that predates the outage does not end the window");
  }

  @Test
  public void testOnDemandRebalanceIsEvaluatedAgainstTheObservedTime() {
    InstanceConfig config = enabledConfig(INSTANCE);
    long offlineTime = OBSERVED_MS - 10_000L;
    ClusterConfig clusterConfig = delayedClusterConfig();
    clusterConfig.setLastOnDemandRebalanceTimestamp(offlineTime + 1L);

    // Past the expiry the override is irrelevant: the instance is out of the population either
    // way. The assertion that matters is the one above, where the observed time is still inside
    // the window; both are evaluated against the timestamp passed in, never against the wall
    // clock.
    Map<String, Long> retained = DelayedRebalanceUtil.getDelayedRebalanceRetainedInstances(
        configs(config).keySet(), liveEnabledNodes(), offlineTimes(INSTANCE, offlineTime),
        liveNodes(), configs(config), clusterConfig, offlineTime + DELAY_MS + 1L);
    Assert.assertTrue(retained.isEmpty());
  }

  // ----- Observation timestamp ------------------------------------------------------------

  @Test
  public void testEveryInstanceIsComparedAgainstTheSameObservedTime() {
    InstanceConfig shortWindow = enabledConfig("shortWindow");
    InstanceConfig longWindow = enabledConfig("longWindow");
    Map<String, InstanceConfig> configs = configs(shortWindow, longWindow);
    Map<String, Long> offlineTimes = new HashMap<>();
    offlineTimes.put("shortWindow", OBSERVED_MS - DELAY_MS + 1L);
    offlineTimes.put("longWindow", OBSERVED_MS - 1L);

    Map<String, Long> atObservedTime =
        DelayedRebalanceUtil.getDelayedRebalanceRetainedInstances(configs.keySet(),
            liveEnabledNodes(), offlineTimes, liveNodes(), configs, delayedClusterConfig(),
            OBSERVED_MS);
    Map<String, Long> expected = new HashMap<>();
    expected.put("shortWindow", OBSERVED_MS + 1L);
    expected.put("longWindow", OBSERVED_MS - 1L + DELAY_MS);
    Assert.assertEquals(atObservedTime, expected);

    // Moving only the observation timestamp forward past the first expiry drops exactly that
    // instance, so the answer is a function of the timestamp passed in.
    Map<String, Long> later =
        DelayedRebalanceUtil.getDelayedRebalanceRetainedInstances(configs.keySet(),
            liveEnabledNodes(), offlineTimes, liveNodes(), configs, delayedClusterConfig(),
            OBSERVED_MS + 1L);
    Assert.assertEquals(later.keySet(), Collections.singleton("longWindow"));
    Assert.assertEquals(later.get("longWindow"), expected.get("longWindow"),
        "The expiry of an instance does not depend on when it is observed");
  }

  @Test
  public void testEveryReturnedExpiryIsAfterTheObservedTime() {
    Map<String, InstanceConfig> configs =
        configs(enabledConfig("offline0"), enabledConfig("offline1"), enabledConfig("live0"));
    Map<String, Long> offlineTimes = new HashMap<>();
    offlineTimes.put("offline0", OBSERVED_MS - 1L);
    offlineTimes.put("offline1", OBSERVED_MS - DELAY_MS);

    Map<String, Long> retained =
        DelayedRebalanceUtil.getDelayedRebalanceRetainedInstances(configs.keySet(),
            liveEnabledNodes("live0"), offlineTimes, liveNodes("live0"), configs,
            delayedClusterConfig(), OBSERVED_MS);
    Assert.assertEquals(retained.keySet(), Collections.singleton("offline0"));
    for (Long expiry : retained.values()) {
      Assert.assertTrue(expiry > OBSERVED_MS,
          "A retained instance always has an expiry in the future of the observation");
    }
  }

  // ----- Explicit delay overload ----------------------------------------------------------

  @Test
  public void testExplicitDelayOverloadAppliesTheGivenDelay() {
    InstanceConfig config = enabledConfig(INSTANCE);
    long offlineTime = OBSERVED_MS - 10_000L;
    long resourceDelay = 5 * DELAY_MS;
    Map<String, Long> retained =
        DelayedRebalanceUtil.getDelayedRebalanceRetainedInstances(configs(config).keySet(),
            liveEnabledNodes(), offlineTimes(INSTANCE, offlineTime), liveNodes(), configs(config),
            resourceDelay, delayedClusterConfig(), OBSERVED_MS);
    Assert.assertEquals(retained, Collections.singletonMap(INSTANCE, offlineTime + resourceDelay),
        "The overload applies the delay it is given, not the cluster default");
  }

  // ----- Equivalence with the rebalancer entry point --------------------------------------

  /**
   * The cases below use timestamps relative to the wall clock because
   * {@link DelayedRebalanceUtil#getActiveNodes} observes it internally. The margins are wide
   * enough that the comparison is stable regardless of how long the test takes.
   */
  @Test
  public void testActiveNodesEqualLiveEnabledPlusRetainedInstances() {
    long now = System.currentTimeMillis();
    long delay = 10 * 60 * 1000L;
    ClusterConfig clusterConfig = new ClusterConfig("cluster0");
    clusterConfig.setDelayRebalaceEnabled(true);
    clusterConfig.setRebalanceDelayTime(delay);

    Map<String, InstanceConfig> configs = configs(enabledConfig("liveEnabled"),
        enabledConfig("recentlyOffline"), enabledConfig("longOffline"),
        disabledConfig("recentlyDisabled", now - 1000L),
        disabledConfig("longDisabled", now - 2 * delay), enabledConfig("neverJoined"));
    Map<String, Long> offlineTimes = new HashMap<>();
    offlineTimes.put("recentlyOffline", now - 1000L);
    offlineTimes.put("longOffline", now - 2 * delay);

    Set<String> liveNodes = liveNodes("liveEnabled", "recentlyDisabled", "longDisabled");
    Set<String> liveEnabledNodes = liveEnabledNodes("liveEnabled");

    assertActiveNodesMatchRetainedInstances(configs, liveEnabledNodes, offlineTimes, liveNodes,
        clusterConfig);

    // The same inputs with an instance opting out, with the cluster switch off, and with no
    // configured delay: the two entry points have to agree in each of them.
    configs.get("recentlyOffline").setDelayRebalanceEnabled(false);
    assertActiveNodesMatchRetainedInstances(configs, liveEnabledNodes, offlineTimes, liveNodes,
        clusterConfig);

    clusterConfig.setDelayRebalaceEnabled(false);
    assertActiveNodesMatchRetainedInstances(configs, liveEnabledNodes, offlineTimes, liveNodes,
        clusterConfig);

    clusterConfig.setDelayRebalaceEnabled(true);
    clusterConfig.setRebalanceDelayTime(-1L);
    assertActiveNodesMatchRetainedInstances(configs, liveEnabledNodes, offlineTimes, liveNodes,
        clusterConfig);
  }

  @Test
  public void testActiveNodesForAResourceEqualLiveEnabledPlusRetainedInstances() {
    long now = System.currentTimeMillis();
    long resourceDelay = 10 * 60 * 1000L;
    ClusterConfig clusterConfig = new ClusterConfig("cluster0");
    clusterConfig.setDelayRebalaceEnabled(true);
    clusterConfig.setRebalanceDelayTime(60_000L);

    IdealState idealState = new IdealState("resource0");
    idealState.setDelayRebalanceEnabled(true);
    idealState.setRebalanceDelay(resourceDelay);

    Map<String, InstanceConfig> configs = configs(enabledConfig("liveEnabled"),
        enabledConfig("offlineWithinResourceDelay"), enabledConfig("offlineBeyondResourceDelay"));
    Map<String, Long> offlineTimes = new HashMap<>();
    // Outside the cluster delay but inside the longer resource delay, so the resource-scoped
    // answer differs from the cluster-scoped one and the equivalence is not trivially true.
    offlineTimes.put("offlineWithinResourceDelay", now - 120_000L);
    offlineTimes.put("offlineBeyondResourceDelay", now - 2 * resourceDelay);
    Set<String> liveNodes = liveNodes("liveEnabled");
    Set<String> liveEnabledNodes = liveEnabledNodes("liveEnabled");
    long delay = DelayedRebalanceUtil.getRebalanceDelay(idealState, clusterConfig);
    Assert.assertEquals(delay, resourceDelay);

    Set<String> activeNodes =
        DelayedRebalanceUtil.getActiveNodes(configs.keySet(), idealState, liveEnabledNodes,
            offlineTimes, liveNodes, configs, delay, clusterConfig);
    Set<String> expected = new HashSet<>(liveEnabledNodes);
    expected.addAll(
        DelayedRebalanceUtil.getDelayedRebalanceRetainedInstances(configs.keySet(),
            liveEnabledNodes, offlineTimes, liveNodes, configs, delay, clusterConfig,
            System.currentTimeMillis()).keySet());
    Assert.assertEquals(activeNodes, expected);
    Assert.assertTrue(activeNodes.contains("offlineWithinResourceDelay"),
        "The resource delay is the one being applied, not the shorter cluster delay");
    Assert.assertFalse(activeNodes.contains("offlineBeyondResourceDelay"));
  }

  private static void assertActiveNodesMatchRetainedInstances(Map<String, InstanceConfig> configs,
      Set<String> liveEnabledNodes, Map<String, Long> offlineTimes, Set<String> liveNodes,
      ClusterConfig clusterConfig) {
    Set<String> activeNodes =
        DelayedRebalanceUtil.getActiveNodes(configs.keySet(), liveEnabledNodes, offlineTimes,
            liveNodes, configs, clusterConfig);
    Set<String> expected = new HashSet<>(liveEnabledNodes);
    expected.addAll(
        DelayedRebalanceUtil.getDelayedRebalanceRetainedInstances(configs.keySet(),
            liveEnabledNodes, offlineTimes, liveNodes, configs, clusterConfig,
            System.currentTimeMillis()).keySet());
    Assert.assertEquals(activeNodes, expected,
        "The active nodes are the live and enabled nodes plus exactly the retained instances");
  }

  // ----- Helpers --------------------------------------------------------------------------

  private static Map<String, Long> retainedInstances(Map<String, InstanceConfig> configs,
      Set<String> liveNodes, Set<String> liveEnabledNodes, Map<String, Long> offlineTimes,
      ClusterConfig clusterConfig) {
    return DelayedRebalanceUtil.getDelayedRebalanceRetainedInstances(configs.keySet(),
        liveEnabledNodes, offlineTimes, liveNodes, configs, clusterConfig, OBSERVED_MS);
  }

  private static ClusterConfig delayedClusterConfig() {
    ClusterConfig clusterConfig = new ClusterConfig("cluster0");
    clusterConfig.setDelayRebalaceEnabled(true);
    clusterConfig.setRebalanceDelayTime(DELAY_MS);
    return clusterConfig;
  }

  private static void setBatchDisableTimestamp(ClusterConfig clusterConfig, String instance,
      long disabledTime) {
    clusterConfig.setDisabledInstancesWithInfo(Collections.singletonMap(instance,
        ClusterConfig.ClusterConfigProperty.HELIX_ENABLED_DISABLE_TIMESTAMP + "=" + disabledTime));
  }

  private static InstanceConfig enabledConfig(String name) {
    return new InstanceConfig(name);
  }

  /**
   * Builds a disabled instance whose disable happened at a chosen time. The timestamp is written
   * on the record because the only APIs that set it stamp the current time, which would make the
   * expected expiry unpredictable.
   */
  private static InstanceConfig disabledConfig(String name, long disabledTime) {
    InstanceConfig config = new InstanceConfig(name);
    config.setInstanceOperation(new InstanceConfig.InstanceOperation.Builder().setOperation(
        InstanceConstants.InstanceOperation.DISABLE).build());
    config.getRecord()
        .setLongField(InstanceConfig.InstanceConfigProperty.HELIX_ENABLED_TIMESTAMP.name(),
            disabledTime);
    Assert.assertEquals(config.getInstanceEnabledTime(), disabledTime);
    Assert.assertFalse(config.getInstanceEnabled());
    return config;
  }

  private static Map<String, InstanceConfig> configs(InstanceConfig... instanceConfigs) {
    Map<String, InstanceConfig> map = new HashMap<>();
    for (InstanceConfig config : instanceConfigs) {
      map.put(config.getInstanceName(), config);
    }
    return map;
  }

  private static Map<String, Long> offlineTimes() {
    return new HashMap<>();
  }

  private static Map<String, Long> offlineTimes(String instance, long offlineTime) {
    Map<String, Long> map = new HashMap<>();
    map.put(instance, offlineTime);
    return map;
  }

  private static Set<String> liveNodes(String... names) {
    return new HashSet<>(Arrays.asList(names));
  }

  private static Set<String> liveEnabledNodes(String... names) {
    return new HashSet<>(Arrays.asList(names));
  }
}
