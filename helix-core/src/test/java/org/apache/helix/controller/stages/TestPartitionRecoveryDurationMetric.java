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

  private boolean hasRecord() {
    Map<String, Map<String, MissingMinActiveReplicaRecord>> map =
        getCache().getMissingMinActiveReplicaMap();
    return map.containsKey(TEST_RESOURCE) && map.get(TEST_RESOURCE).containsKey(PARTITION);
  }

  @Test
  public void testRecordCreatedWhenBelowMinActiveReplica() {
    preSetup(MIN_ACTIVE_REPLICAS);

    // Only 1 replica active (MASTER), the rest OFFLINE -> activeReplicaCount(1) < minActive(2).
    long beforeDetection = System.currentTimeMillis();
    runPipeline(statesOf("MASTER", "OFFLINE", "OFFLINE"), null);

    Assert.assertTrue(hasRecord(),
        "A recovery record must be created when active replicas drop below minActiveReplicas");
    MissingMinActiveReplicaRecord record =
        getCache().getMissingMinActiveReplicaMap().get(TEST_RESOURCE).get(PARTITION);
    Assert.assertTrue(record.getStartTimeStamp() >= beforeDetection,
        "Recovery start time should be stamped at detection time");
    Assert.assertFalse(record.isFailed(),
        "A freshly-degraded partition should not yet be marked beyond threshold");

    // No recovery observed yet, so no duration / counter should have been emitted.
    ResourceMonitor resourceMonitor = getResourceMonitor();
    if (resourceMonitor != null) {
      Assert.assertEquals(resourceMonitor.getSucceededPartitionRecoveryCounter(), 0L);
    }
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
    // helixLatency is a follow-up; v1 emits -1 so the beyond-threshold gauge stays untouched here.
    Assert.assertEquals(resourceMonitor.getPartitionsRecoveryDurationBeyondThresholdGauge(), 0L);
  }

  @Test
  public void testBeyondThresholdGaugeIncrementsThenClearsOnRecovery() {
    preSetup(MIN_ACTIVE_REPLICAS);
    ClusterConfig clusterConfig = new ClusterConfig(_clusterName);
    clusterConfig.setPartitionRecoveryDurationThreshold(5000L);
    setClusterConfig(clusterConfig);

    // Seed a record that started 10s ago (beyond the 5s threshold) while still degraded.
    final long seededStart = System.currentTimeMillis() - 10000L;
    runPipeline(statesOf("MASTER", "OFFLINE", "OFFLINE"), cache -> seedRecord(seededStart));

    ResourceMonitor resourceMonitor = getResourceMonitor();
    Assert.assertNotNull(resourceMonitor);
    Assert.assertEquals(resourceMonitor.getPartitionsRecoveryDurationBeyondThresholdGauge(), 1L,
        "A partition degraded beyond the threshold should increment the beyond-threshold gauge");
    Assert.assertTrue(
        getCache().getMissingMinActiveReplicaMap().get(TEST_RESOURCE).get(PARTITION).isFailed(),
        "The record should be marked failed once counted beyond threshold");

    // Now recover the partition.
    runPipeline(statesOf("MASTER", "SLAVE", "OFFLINE"), null);

    Assert.assertFalse(hasRecord(), "Recovery record should be cleared after recovery");
    resourceMonitor = getResourceMonitor();
    Assert.assertEquals(resourceMonitor.getPartitionsRecoveryDurationBeyondThresholdGauge(), 0L,
        "The beyond-threshold gauge should decrement once the partition recovers");
    Assert.assertEquals(resourceMonitor.getSucceededPartitionRecoveryCounter(), 1L);
  }

  @Test
  public void testBeyondThresholdGaugeCountedOnlyOnce() {
    preSetup(MIN_ACTIVE_REPLICAS);
    ClusterConfig clusterConfig = new ClusterConfig(_clusterName);
    clusterConfig.setPartitionRecoveryDurationThreshold(5000L);
    setClusterConfig(clusterConfig);

    final long seededStart = System.currentTimeMillis() - 10000L;
    Map<String, String> degraded = statesOf("MASTER", "OFFLINE", "OFFLINE");
    runPipeline(degraded, cache -> seedRecord(seededStart));
    // A second scrape while still degraded and still beyond threshold must not double-count.
    runPipeline(degraded, null);

    ResourceMonitor resourceMonitor = getResourceMonitor();
    Assert.assertEquals(resourceMonitor.getPartitionsRecoveryDurationBeyondThresholdGauge(), 1L,
        "A partition already counted beyond threshold must not be counted again on later scrapes");
  }
}
