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

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import org.apache.helix.controller.dataproviders.ResourceControllerDataProvider;
import org.apache.helix.model.BuiltInStateModelDefinitions;
import org.apache.helix.model.ClusterConfig;
import org.apache.helix.model.CurrentState;
import org.apache.helix.model.ExternalView;
import org.apache.helix.model.IdealState.RebalanceMode;
import org.apache.helix.model.Resource;
import org.apache.helix.monitoring.mbeans.ClusterStatusMonitor;
import org.apache.helix.monitoring.mbeans.ResourceMonitor;
import org.testng.Assert;
import org.testng.annotations.Test;

/**
 * Verifies the partition recovery duration detection wired into
 * {@link TopStateHandoffReportStage}. A partition is considered "recovering" while its active
 * replica count is below the configured {@code minActiveReplicas}; the stage records when the
 * partition first drops below the minimum and, on recovery, emits the elapsed duration.
 */
public class TestPartitionRecoveryDurationMetric extends BaseStageTest {
  private static final String TEST_RESOURCE = "TestDB";
  private static final String PARTITION = TEST_RESOURCE + "_0";
  private static final int NUM_NODES = 3;
  private static final int NUM_REPLICAS = 3;
  private static final int MIN_ACTIVE_REPLICAS = 2;
  private static final String RECOVERY_DURATION_MAX = "PartitionRecoveryDurationGauge.Max";

  private interface CacheInject {
    void doInject(ResourceControllerDataProvider cache);
  }

  private void preSetup(int minActiveReplicas) {
    setupLiveInstances(NUM_NODES);
    setupStateModel();
    setupIdealState(NUM_NODES, new String[] {TEST_RESOURCE}, 1, NUM_REPLICAS,
        RebalanceMode.SEMI_AUTO, BuiltInStateModelDefinitions.MasterSlave.name(), null, null,
        minActiveReplicas);

    Resource resource = new Resource(TEST_RESOURCE);
    resource.setStateModelDefRef(BuiltInStateModelDefinitions.MasterSlave.name());
    resource.addPartition(PARTITION);
    event.addAttribute(AttributeName.RESOURCES.name(),
        Collections.singletonMap(TEST_RESOURCE, resource));
    event.addAttribute(AttributeName.LastRebalanceFinishTimeStamp.name(),
        TopStateHandoffReportStage.TIMESTAMP_NOT_RECORDED);
    event.addAttribute(AttributeName.ControllerDataProvider.name(),
        new ResourceControllerDataProvider());

    ClusterStatusMonitor monitor = new ClusterStatusMonitor(_clusterName);
    monitor.active();
    event.addAttribute(AttributeName.clusterStatusMonitor.name(), monitor);
  }

  private CurrentState buildCurrentState(String instanceName, String state) {
    CurrentState currentState = new CurrentState(TEST_RESOURCE);
    currentState.setSessionId(SESSION_PREFIX + instanceName.split("_")[1]);
    currentState.setState(PARTITION, state);
    return currentState;
  }

  private void runPipeline(Map<String, String> instanceStates, CacheInject inject) {
    Map<String, CurrentState> currentStates = new HashMap<>();
    for (Map.Entry<String, String> entry : instanceStates.entrySet()) {
      currentStates.put(entry.getKey(), buildCurrentState(entry.getKey(), entry.getValue()));
    }
    setupCurrentStates(currentStates);
    runStage(event, new ReadClusterDataStage());
    if (inject != null) {
      inject.doInject(event.getAttribute(AttributeName.ControllerDataProvider.name()));
    }
    runStage(event, new CurrentStateComputationStage());
    runStage(event, new TopStateHandoffReportStage());
  }

  private ResourceControllerDataProvider getCache() {
    return event.getAttribute(AttributeName.ControllerDataProvider.name());
  }

  private ResourceMonitor getResourceMonitor() {
    ClusterStatusMonitor monitor = event.getAttribute(AttributeName.clusterStatusMonitor.name());
    return monitor.getResourceMonitor(TEST_RESOURCE);
  }

  private Map<String, String> statesOf(String s0, String s1, String s2) {
    Map<String, String> states = new HashMap<>();
    states.put(HOSTNAME_PREFIX + 0, s0);
    states.put(HOSTNAME_PREFIX + 1, s1);
    states.put(HOSTNAME_PREFIX + 2, s2);
    return states;
  }

  private void seedRecord(long startTimeStamp) {
    Map<String, MissingMinActiveReplicaRecord> perResource = new HashMap<>();
    perResource.put(PARTITION, new MissingMinActiveReplicaRecord(startTimeStamp));
    getCache().getMissingMinActiveReplicaMap().put(TEST_RESOURCE, perResource);
  }

  /**
   * Publish an ExternalView to the accessor so the next pipeline run loads it as the "previous"
   * ExternalView. The recovery detector uses it to confirm the partition was at or above min before
   * a drop. Must be called before {@link #runPipeline} (the ExternalView is refreshed on the cache's
   * first refresh).
   */
  private void publishExternalView(String s0, String s1, String s2) {
    ExternalView externalView = new ExternalView(TEST_RESOURCE);
    externalView.setState(PARTITION, HOSTNAME_PREFIX + 0, s0);
    externalView.setState(PARTITION, HOSTNAME_PREFIX + 1, s1);
    externalView.setState(PARTITION, HOSTNAME_PREFIX + 2, s2);
    accessor.setProperty(accessor.keyBuilder().externalView(TEST_RESOURCE), externalView);
  }

  private boolean hasRecord() {
    Map<String, Map<String, MissingMinActiveReplicaRecord>> map =
        getCache().getMissingMinActiveReplicaMap();
    return map.containsKey(TEST_RESOURCE) && map.get(TEST_RESOURCE).containsKey(PARTITION);
  }

  @Test
  public void testRecordCreatedWhenBelowMinActiveReplica() {
    preSetup(MIN_ACTIVE_REPLICAS);

    // The partition was healthy in the previously published ExternalView (3 active), so a drop is a
    // genuine healthy -> below-min transition that must open a recovery window.
    publishExternalView("MASTER", "SLAVE", "SLAVE");

    // Only 1 replica active (MASTER), the rest OFFLINE -> activeReplicaCount(1) < minActive(2).
    long beforeDetection = System.currentTimeMillis();
    runPipeline(statesOf("MASTER", "OFFLINE", "OFFLINE"), null);

    Assert.assertTrue(hasRecord(),
        "A recovery record must be created when active replicas drop below minActiveReplicas");
    MissingMinActiveReplicaRecord record =
        getCache().getMissingMinActiveReplicaMap().get(TEST_RESOURCE).get(PARTITION);
    Assert.assertTrue(record.getStartTimeStamp() >= beforeDetection,
        "Recovery start time should be stamped at detection time");

    // No recovery observed yet: nothing is emitted, so no ResourceMonitor is created for the
    // resource. Assert that explicitly rather than skipping, so a stray emission can't pass green.
    Assert.assertNull(getResourceMonitor(),
        "No ResourceMonitor should exist while the partition is still degraded (nothing emitted)");
  }

  @Test
  public void testBringUpWithoutPreviousHealthyExternalViewCreatesNoRecord() {
    preSetup(MIN_ACTIVE_REPLICAS);

    // No previously published ExternalView: this is a partition coming up from nothing (a brand-new
    // resource, partition expansion, or the first run after a leadership change clears the in-memory
    // records), not a drop from a healthy state. Even though it is below min, no recovery window may
    // open -- otherwise the initial bring-up time would later be mis-counted as a recovery.
    runPipeline(statesOf("MASTER", "OFFLINE", "OFFLINE"), null);

    Assert.assertFalse(hasRecord(),
        "A partition below min with no prior healthy ExternalView (bring-up) must not create a record");
    Assert.assertNull(getResourceMonitor(),
        "Bring-up below min must not emit any recovery metric (no ResourceMonitor created)");
  }

  @Test
  public void testDropWhenPreviousExternalViewBelowMinCreatesNoRecord() {
    preSetup(MIN_ACTIVE_REPLICAS);

    // The previous ExternalView was itself below min (1 active) -- e.g. the partition was already
    // degraded under a prior controller before clearMonitoringRecords() wiped the in-memory record.
    // We do not know the true start of that window, so we must not open a new one and fabricate a
    // recovery duration.
    publishExternalView("MASTER", "OFFLINE", "OFFLINE");
    runPipeline(statesOf("MASTER", "OFFLINE", "OFFLINE"), null);

    Assert.assertFalse(hasRecord(),
        "A below-min partition whose previous ExternalView was also below min must not create a record");
    Assert.assertNull(getResourceMonitor(),
        "A pre-existing degraded partition must not emit any recovery metric");
  }

  @Test
  public void testHealthyPartitionCreatesNoRecord() {
    preSetup(MIN_ACTIVE_REPLICAS);

    // 3 active replicas (MASTER + 2 SLAVE) -> activeReplicaCount(3) >= minActive(2).
    runPipeline(statesOf("MASTER", "SLAVE", "SLAVE"), null);

    Assert.assertFalse(hasRecord(),
        "A healthy partition (at or above minActiveReplicas) must not create a recovery record");
  }

  @Test
  public void testDisabledResourceCreatesNoRecord() {
    preSetup(MIN_ACTIVE_REPLICAS);

    // Resource is disabled: it is not expected to maintain its replicas, so a drop below min must
    // not start recovery tracking (mirrors ResourceMonitor#updateResourceState). Disable the cached
    // IdealState after ReadClusterDataStage populates it but before the recovery stage runs.
    runPipeline(statesOf("MASTER", "OFFLINE", "OFFLINE"),
        cache -> cache.getIdealState(TEST_RESOURCE).enable(false));

    Assert.assertFalse(hasRecord(),
        "A disabled resource below minActiveReplicas must not create a recovery record");
    Assert.assertNull(getResourceMonitor(),
        "A disabled resource must not emit any recovery metric (no ResourceMonitor created)");
  }

  @Test
  public void testMaintenanceModeCreatesNoRecord() {
    preSetup(MIN_ACTIVE_REPLICAS);

    // The partition was healthy before, so without the maintenance guard this drop would open a
    // recovery window -- isolating the behavior under test to maintenance mode alone.
    publishExternalView("MASTER", "SLAVE", "SLAVE");

    // During maintenance the controller intentionally holds off restoring or moving replicas (node
    // swaps, take-downs, the maintenance-timeout window), so a partition below min is expected
    // behavior, not an availability regression, and must not be tracked as a recovery. Enable
    // maintenance mode on the cache after ReadClusterDataStage but before the recovery stage runs.
    runPipeline(statesOf("MASTER", "OFFLINE", "OFFLINE"),
        cache -> cache.enableMaintenanceMode());

    Assert.assertFalse(hasRecord(),
        "A partition below min while the cluster is in maintenance mode must not create a record");
    Assert.assertNull(getResourceMonitor(),
        "Maintenance-mode below min must not emit any recovery metric (no ResourceMonitor created)");
  }

  @Test
  public void testRecoveryEmitsDurationAndClearsRecord() {
    preSetup(MIN_ACTIVE_REPLICAS);

    final long elapsedMs = 5000L;
    final long seededStart = System.currentTimeMillis() - elapsedMs;

    // Recovered layout: MASTER + SLAVE -> activeReplicaCount(2) >= minActive(2). Pre-seed a
    // below-min record started elapsedMs ago so the emitted duration is deterministic.
    runPipeline(statesOf("MASTER", "SLAVE", "OFFLINE"), cache -> seedRecord(seededStart));

    Assert.assertFalse(hasRecord(),
        "Recovery record must be removed once the partition returns to minActiveReplicas");

    ResourceMonitor resourceMonitor = getResourceMonitor();
    Assert.assertNotNull(resourceMonitor,
        "A ResourceMonitor should be created when a recovery duration is emitted");
    Assert.assertEquals(resourceMonitor.getSucceededPartitionRecoveryCounter(), 1L,
        "A successful recovery should increment the recovery counter");
    long recordedDuration = resourceMonitor.getPartitionRecoveryDurationGauge()
        .getAttributeValue(RECOVERY_DURATION_MAX).longValue();
    Assert.assertTrue(recordedDuration >= elapsedMs,
        "Recorded recovery duration should be at least the seeded elapsed time, got "
            + recordedDuration);
    // The seeded record started only elapsedMs (5s) ago, well under the default recovery threshold
    // (5 min), so this fast recovery must not increment the beyond-threshold counter.
    Assert.assertEquals(resourceMonitor.getPartitionsRecoveryDurationBeyondThresholdCounter(), 0L);
  }

  @Test
  public void testSlowRecoveryIncrementsBeyondThresholdCounter() {
    preSetup(MIN_ACTIVE_REPLICAS);
    ClusterConfig clusterConfig = new ClusterConfig(_clusterName);
    clusterConfig.setPartitionRecoveryDurationThreshold(5000L);
    setClusterConfig(clusterConfig);

    // Seed a record that started 10s ago (beyond the 5s threshold) while still degraded. A
    // still-degraded scrape must NOT increment the counter -- the breach is only counted at
    // recovery, so the monotonic counter can capture breaches that heal between scrapes.
    final long seededStart = System.currentTimeMillis() - 10000L;
    runPipeline(statesOf("MASTER", "OFFLINE", "OFFLINE"), cache -> seedRecord(seededStart));

    // Still-degraded scrape: nothing is emitted, so no ResourceMonitor exists yet, and the record
    // is still tracked. Assert both explicitly so a stray in-flight emission can't pass green.
    Assert.assertTrue(hasRecord(), "The record should still be tracked while degraded");
    Assert.assertNull(getResourceMonitor(),
        "A still-degraded partition must not emit anything (no ResourceMonitor created) in flight");

    // Now recover the partition. Its total below-min window (~10s) exceeds the 5s threshold, so
    // the counter increments exactly once, at recovery.
    runPipeline(statesOf("MASTER", "SLAVE", "OFFLINE"), null);

    Assert.assertFalse(hasRecord(), "Recovery record should be cleared after recovery");
    ResourceMonitor resourceMonitor = getResourceMonitor();
    Assert.assertNotNull(resourceMonitor,
        "A ResourceMonitor should be created when a recovery duration is emitted");
    Assert.assertEquals(resourceMonitor.getPartitionsRecoveryDurationBeyondThresholdCounter(), 1L,
        "A recovery whose below-min window exceeded the threshold should increment the counter");
    Assert.assertEquals(resourceMonitor.getSucceededPartitionRecoveryCounter(), 1L);
  }

  @Test
  public void testBeyondThresholdCounterAccumulatesAndNeverDecrements() {
    preSetup(MIN_ACTIVE_REPLICAS);
    ClusterConfig clusterConfig = new ClusterConfig(_clusterName);
    clusterConfig.setPartitionRecoveryDurationThreshold(5000L);
    setClusterConfig(clusterConfig);

    // First slow breach: seed a 10s-old record, then recover -> counter == 1.
    runPipeline(statesOf("MASTER", "OFFLINE", "OFFLINE"),
        cache -> seedRecord(System.currentTimeMillis() - 10000L));
    runPipeline(statesOf("MASTER", "SLAVE", "OFFLINE"), null);
    Assert.assertEquals(getResourceMonitor().getPartitionsRecoveryDurationBeyondThresholdCounter(),
        1L);

    // A heal followed by a fresh drop opens a NEW window. Seed a second 10s-old record and recover
    // again -> the monotonic counter accumulates to 2 (it never resets and never decrements).
    runPipeline(statesOf("MASTER", "OFFLINE", "OFFLINE"),
        cache -> seedRecord(System.currentTimeMillis() - 10000L));
    runPipeline(statesOf("MASTER", "SLAVE", "OFFLINE"), null);
    Assert.assertEquals(getResourceMonitor().getPartitionsRecoveryDurationBeyondThresholdCounter(),
        2L, "The beyond-threshold counter must accumulate across separate breach windows");
    Assert.assertEquals(getResourceMonitor().getSucceededPartitionRecoveryCounter(), 2L);
  }
}
