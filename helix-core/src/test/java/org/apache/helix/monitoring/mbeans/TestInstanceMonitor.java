package org.apache.helix.monitoring.mbeans;

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
import java.util.List;
import java.util.Map;
import java.util.Set;
import javax.management.JMException;
import javax.management.ObjectName;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import org.apache.helix.constants.InstanceConstants;
import org.apache.helix.model.InstanceConfig;
import org.testng.Assert;
import org.testng.annotations.Test;

public class TestInstanceMonitor {
  @Test
  public void testInstanceMonitor()
      throws JMException {
    String testCluster = "testCluster";
    String testInstance = "testInstance";
    String testDomain = "testDomain:key=value";
    Set<String> tags = ImmutableSet.of("test", "DEFAULT");
    Map<String, List<String>> disabledPartitions = ImmutableMap.of("instance1",
        ImmutableList.of("partition1", "partition2", InstanceConstants.ALL_RESOURCES_DISABLED_PARTITION_KEY));
    InstanceMonitor monitor =
        new InstanceMonitor(testCluster, testInstance, new ObjectName(testDomain));

    // Verify init status.
    Assert.assertEquals(monitor.getSensorName(),
        "ParticipantStatus.testCluster.DEFAULT.testInstance");
    Assert.assertEquals(monitor.getInstanceName(), testInstance);
    Assert.assertEquals(monitor.getOnline(), 0L);
    Assert.assertEquals(monitor.getEnabled(), 0L);
    Assert.assertEquals(monitor.getTotalMessageReceived(), 0L);
    Assert.assertEquals(monitor.getDisabledPartitions(), 0L);
    Assert.assertEquals(monitor.getAllPartitionsDisabled(), 0L);
    Assert.assertEquals(monitor.getMaxCapacityUsageGauge(), 0.0d);

    // Update metrics.
    monitor.updateMaxCapacityUsage(0.5d);
    monitor.increaseMessageCount(10L);
    monitor.updateInstance(tags, disabledPartitions, Collections.emptyList(), true, true, 0L);
    monitor.updateMessageQueueSize(100L);
    monitor.updatePastDueMessageGauge(50L);

    // Verify metrics.
    Assert.assertEquals(monitor.getTotalMessageReceived(), 10L);
    Assert.assertEquals(monitor.getSensorName(),
        "ParticipantStatus.testCluster.DEFAULT|test.testInstance");
    Assert.assertEquals(monitor.getInstanceName(), testInstance);
    Assert.assertEquals(monitor.getOnline(), 1L);
    Assert.assertEquals(monitor.getEnabled(), 1L);
    Assert.assertEquals(monitor.getDisabledPartitions(), 2L);
    Assert.assertEquals(monitor.getAllPartitionsDisabled(), 1L);
    Assert.assertEquals(monitor.getMaxCapacityUsageGauge(), 0.5d);
    Assert.assertEquals(monitor.getMessageQueueSizeGauge(), 100L);
    Assert.assertEquals(monitor.getPastDueMessageGauge(), 50L);
    Assert.assertEquals(monitor.getErrorPartitions(), 0L);

    monitor.unregister();
  }

  @Test
  public void testInstanceOperationDurationMetrics() throws JMException {
    String testCluster = "testCluster";
    String testInstance = "testInstance";
    String testDomain = "testDomain:key=value";
    InstanceMonitor monitor =
        new InstanceMonitor(testCluster, testInstance, new ObjectName(testDomain));

    // Initially, all duration metrics should be 0 (instance starts in ENABLE state)
    Assert.assertEquals(monitor.getInstanceOperationDurationEnable(), 0L);
    Assert.assertEquals(monitor.getInstanceOperationDurationDisable(), 0L);
    Assert.assertEquals(monitor.getInstanceOperationDurationEvacuate(), 0L);
    Assert.assertEquals(monitor.getInstanceOperationDurationSwapIn(), 0L);
    Assert.assertEquals(monitor.getInstanceOperationDurationUnknown(), 0L);

    // Durations are reported in seconds and derived from the authoritative start timestamp, so we
    // drive the monitor with start times in the past instead of relying on sleeps.
    long now = System.currentTimeMillis();

    // EVACUATE started 120s ago.
    monitor.updateInstanceOperation(InstanceConstants.InstanceOperation.EVACUATE, now - 120_000L);
    long evacuateDuration = monitor.getInstanceOperationDurationEvacuate();
    Assert.assertTrue(evacuateDuration >= 120L,
        "EVACUATE duration should be >= 120s, but was " + evacuateDuration);
    // Switching away from ENABLE resets it immediately, and the other operations stay 0.
    Assert.assertEquals(monitor.getInstanceOperationDurationEnable(), 0L,
        "ENABLE duration should be reset to 0 when switching to EVACUATE");
    Assert.assertEquals(monitor.getInstanceOperationDurationDisable(), 0L);
    Assert.assertEquals(monitor.getInstanceOperationDurationSwapIn(), 0L);
    Assert.assertEquals(monitor.getInstanceOperationDurationUnknown(), 0L);

    // The authoritative timestamp is honored on every poll: an earlier start time yields a larger
    // duration even though the operation itself did not change.
    monitor.updateInstanceOperation(InstanceConstants.InstanceOperation.EVACUATE, now - 240_000L);
    long evacuateDuration2 = monitor.getInstanceOperationDurationEvacuate();
    Assert.assertTrue(evacuateDuration2 >= 240L,
        "EVACUATE duration should track the configured start time, but was " + evacuateDuration2);

    // Change to DISABLE, started 60s ago. All gauges except DISABLE reset to 0.
    monitor.updateInstanceOperation(InstanceConstants.InstanceOperation.DISABLE, now - 60_000L);
    Assert.assertTrue(monitor.getInstanceOperationDurationDisable() >= 60L,
        "DISABLE duration should be >= 60s");
    Assert.assertEquals(monitor.getInstanceOperationDurationEvacuate(), 0L,
        "EVACUATE duration should be reset to 0 when switching to DISABLE");
    Assert.assertEquals(monitor.getInstanceOperationDurationEnable(), 0L);

    // Change to SWAP_IN, started 30s ago.
    monitor.updateInstanceOperation(InstanceConstants.InstanceOperation.SWAP_IN, now - 30_000L);
    Assert.assertTrue(monitor.getInstanceOperationDurationSwapIn() >= 30L,
        "SWAP_IN duration should be >= 30s");
    Assert.assertEquals(monitor.getInstanceOperationDurationDisable(), 0L,
        "DISABLE should be reset to 0");
    Assert.assertEquals(monitor.getInstanceOperationDurationEvacuate(), 0L,
        "EVACUATE should be reset to 0");
    Assert.assertEquals(monitor.getInstanceOperationDurationEnable(), 0L,
        "ENABLE should be reset to 0");

    // Change to UNKNOWN, started 10s ago.
    monitor.updateInstanceOperation(InstanceConstants.InstanceOperation.UNKNOWN, now - 10_000L);
    Assert.assertTrue(monitor.getInstanceOperationDurationUnknown() >= 10L,
        "UNKNOWN duration should be >= 10s");
    Assert.assertEquals(monitor.getInstanceOperationDurationSwapIn(), 0L,
        "SWAP_IN should be reset to 0");

    // Back to ENABLE, started 50s ago. All others reset to 0.
    monitor.updateInstanceOperation(InstanceConstants.InstanceOperation.ENABLE, now - 50_000L);
    Assert.assertEquals(monitor.getInstanceOperationDurationUnknown(), 0L,
        "UNKNOWN should be reset to 0");
    Assert.assertEquals(monitor.getInstanceOperationDurationDisable(), 0L,
        "DISABLE should be reset to 0");
    Assert.assertEquals(monitor.getInstanceOperationDurationEvacuate(), 0L,
        "EVACUATE should be reset to 0");
    Assert.assertEquals(monitor.getInstanceOperationDurationSwapIn(), 0L,
        "SWAP_IN should be reset to 0");
    long enableDuration = monitor.getInstanceOperationDurationEnable();
    Assert.assertTrue(enableDuration >= 50L,
        "ENABLE duration should be >= 50s, but was " + enableDuration);

    // A null operation defaults to ENABLE and keeps tracking the same operation/timestamp.
    monitor.updateInstanceOperation(null, now - 50_000L);
    Assert.assertTrue(monitor.getInstanceOperationDurationEnable() >= 50L,
        "ENABLE duration should keep tracking after a null (defaulted) operation");

    monitor.unregister();
  }

  @Test
  public void testInstanceOperationDurationWithInstanceConfigAPI() throws JMException {
    String testCluster = "testCluster";
    String testInstance = "localhost_12345";
    String testDomain = "testDomain:key=value";

    // Create InstanceConfig using the actual API
    InstanceConfig instanceConfig = new InstanceConfig(testInstance);

    // Create InstanceMonitor
    InstanceMonitor monitor =
        new InstanceMonitor(testCluster, testInstance, new ObjectName(testDomain));

    // Verify initial state - instance starts in ENABLE
    Assert.assertEquals(instanceConfig.getInstanceOperation().getOperation(),
        InstanceConstants.InstanceOperation.ENABLE);
    Assert.assertEquals(monitor.getInstanceOperationDurationEnable(), 0L);

    // ===== Test 1: EVACUATE operation using InstanceConfig API =====
    InstanceConfig.InstanceOperation evacuateOp =
        new InstanceConfig.InstanceOperation.Builder()
            .setOperation(InstanceConstants.InstanceOperation.EVACUATE)
            .setReason("Testing evacuation")
            .setSource(InstanceConstants.InstanceOperationSource.USER)
            .build();

    instanceConfig.setInstanceOperation(evacuateOp);

    // Verify InstanceConfig state changed
    Assert.assertEquals(instanceConfig.getInstanceOperation().getOperation(),
        InstanceConstants.InstanceOperation.EVACUATE);
    Assert.assertEquals(instanceConfig.getInstanceOperation().getReason(),
        "Testing evacuation");

    // Verify timestamp was set
    long operationTimestamp = instanceConfig.getInstanceOperation().getTimestamp();
    Assert.assertTrue(operationTimestamp > 0,
        "Operation timestamp should be set");

    // Update monitor with the new operation (simulating what ClusterStatusMonitor does).
    monitor.updateInstanceOperation(instanceConfig.getInstanceOperation().getOperation(),
        operationTimestamp);

    // The API stamps the operation with the current time, so the gauge must reflect
    // (now - timestamp) in seconds - a small, non-negative number. It must NOT be the
    // wall-clock-derived value (~1.7e9 s) that the previous implementation produced for an
    // operation that was already in progress.
    long evacuateDuration = monitor.getInstanceOperationDurationEvacuate();
    long evacuateUpperBound = (System.currentTimeMillis() - operationTimestamp) / 1000L + 2L;
    Assert.assertTrue(evacuateDuration >= 0L && evacuateDuration <= evacuateUpperBound,
        "EVACUATE duration should reflect (now - timestamp) in seconds, but was " + evacuateDuration);
    // ENABLE should be reset to 0 when switching to EVACUATE
    Assert.assertEquals(monitor.getInstanceOperationDurationEnable(), 0L,
        "ENABLE should be reset to 0 when switching to EVACUATE");

    // ===== Test 2: Create new InstanceConfig for DISABLE operation =====
    // Creating a fresh instance to avoid backwards compatibility issues
    InstanceConfig instanceConfig2 = new InstanceConfig(testInstance + "_2");
    InstanceMonitor monitor2 =
        new InstanceMonitor(testCluster, testInstance + "_2", new ObjectName(testDomain + "2"));

    InstanceConfig.InstanceOperation disableOp =
        new InstanceConfig.InstanceOperation.Builder()
            .setOperation(InstanceConstants.InstanceOperation.DISABLE)
            .setReason("Maintenance window")
            .setSource(InstanceConstants.InstanceOperationSource.ADMIN)
            .build();

    instanceConfig2.setInstanceOperation(disableOp);

    // Verify state
    Assert.assertEquals(instanceConfig2.getInstanceOperation().getOperation(),
        InstanceConstants.InstanceOperation.DISABLE);
    Assert.assertEquals(instanceConfig2.getInstanceOperation().getSource(),
        InstanceConstants.InstanceOperationSource.ADMIN);

    // Update monitor
    long disableTimestamp = instanceConfig2.getInstanceOperation().getTimestamp();
    monitor2.updateInstanceOperation(instanceConfig2.getInstanceOperation().getOperation(),
        disableTimestamp);

    long disableDuration = monitor2.getInstanceOperationDurationDisable();
    long disableUpperBound = (System.currentTimeMillis() - disableTimestamp) / 1000L + 2L;
    Assert.assertTrue(disableDuration >= 0L && disableDuration <= disableUpperBound,
        "DISABLE duration should reflect (now - timestamp) in seconds, but was " + disableDuration);
    Assert.assertEquals(monitor2.getInstanceOperationDurationEvacuate(), 0L,
        "EVACUATE should be 0 for this instance");

    // Clean up
    monitor.unregister();
    monitor2.unregister();
  }

  @Test
  public void testEnableDurationUsesConfiguredTimestampNotWallClock() throws JMException {
    String testCluster = "testCluster";
    String testInstance = "testInstance";
    String testDomain = "testDomain:key=value";
    InstanceMonitor monitor =
        new InstanceMonitor(testCluster, testInstance, new ObjectName(testDomain));

    // Regression test: an instance that is already ENABLE (the monitor's default operation) never
    // triggers an operation "change". The duration must still be measured from the configured
    // enable timestamp, not from epoch 0 - the latter previously made the gauge report wall-clock
    // time, which after 32-bit truncation downstream surfaced as a negative value.
    long enabledTenMinutesAgo = System.currentTimeMillis() - 600_000L;
    monitor.updateInstanceOperation(InstanceConstants.InstanceOperation.ENABLE,
        enabledTenMinutesAgo);

    long enableDuration = monitor.getInstanceOperationDurationEnable();
    Assert.assertTrue(enableDuration >= 600L,
        "ENABLE duration should be >= 600s (time since enable), but was " + enableDuration);
    // Guard against the old wall-clock bug: an elapsed duration of a freshly-enabled instance must
    // not be anywhere near epoch-seconds (~1.7e9).
    Assert.assertTrue(enableDuration < 86_400L,
        "ENABLE duration should be an elapsed duration, not wall-clock time, but was "
            + enableDuration);

    monitor.unregister();
  }

  @Test
  public void testFutureOperationTimestampClampedToZero() throws JMException {
    String testCluster = "testCluster";
    String testInstance = "testInstance";
    String testDomain = "testDomain:key=value";
    InstanceMonitor monitor =
        new InstanceMonitor(testCluster, testInstance, new ObjectName(testDomain));

    // A timestamp in the future (e.g. clock skew between the host that wrote it and this
    // controller) must be clamped to 0 rather than producing a negative duration.
    long oneHourInFuture = System.currentTimeMillis() + 3_600_000L;
    monitor.updateInstanceOperation(InstanceConstants.InstanceOperation.ENABLE, oneHourInFuture);

    Assert.assertEquals(monitor.getInstanceOperationDurationEnable(), 0L,
        "A future-dated operation timestamp should be clamped to 0");

    monitor.unregister();
  }

  @Test
  public void testErrorPartitionsGauge() throws JMException {
    String testCluster = "testCluster";
    String testInstance = "testInstance";
    String testDomain = "testDomain:key=value";
    Set<String> tags = ImmutableSet.of("test");
    
    InstanceMonitor monitor =
        new InstanceMonitor(testCluster, testInstance, new ObjectName(testDomain));

    // Verify initial state - no error partitions
    Assert.assertEquals(monitor.getErrorPartitions(), 0L);

    // Simulate instance with 3 error partitions
    monitor.updateInstance(tags, ImmutableMap.of(), Collections.emptyList(), true, true, 3L);
    Assert.assertEquals(monitor.getErrorPartitions(), 3L);

    // Update with more error partitions
    monitor.updateInstance(tags, ImmutableMap.of(), Collections.emptyList(), true, true, 5L);
    Assert.assertEquals(monitor.getErrorPartitions(), 5L);

    // Update with zero error partitions (partitions recovered)
    monitor.updateInstance(tags, ImmutableMap.of(), Collections.emptyList(), true, true, 0L);
    Assert.assertEquals(monitor.getErrorPartitions(), 0L);

    // Test with instance offline - error partition count should still be tracked
    monitor.updateInstance(tags, ImmutableMap.of(), Collections.emptyList(), false, true, 2L);
    Assert.assertEquals(monitor.getErrorPartitions(), 2L);
    Assert.assertEquals(monitor.getOnline(), 0L);

    // Test with instance disabled - error partition count should still be tracked
    monitor.updateInstance(tags, ImmutableMap.of(), Collections.emptyList(), true, false, 4L);
    Assert.assertEquals(monitor.getErrorPartitions(), 4L);
    Assert.assertEquals(monitor.getEnabled(), 0L);

    monitor.unregister();
  }

  @Test
  public void testPartitionCountMetrics() throws JMException {
    String testCluster = "testCluster";
    String testInstance = "testInstance";
    String testDomain = "testDomain:key=value";
    InstanceMonitor monitor =
        new InstanceMonitor(testCluster, testInstance, new ObjectName(testDomain));

    // Verify initial state
    Assert.assertEquals(monitor.getPartitionCount(), 0L);
    Assert.assertEquals(monitor.getTopStatePartitionCount(), 0L);

    // Update partition counts
    monitor.updatePartitionCount(10L);
    monitor.updateTopStatePartitionCount(5L);

    // Verify updated values
    Assert.assertEquals(monitor.getPartitionCount(), 10L);
    Assert.assertEquals(monitor.getTopStatePartitionCount(), 5L);

    // Update again with different values
    monitor.updatePartitionCount(20L);
    monitor.updateTopStatePartitionCount(12L);

    // Verify new values
    Assert.assertEquals(monitor.getPartitionCount(), 20L);
    Assert.assertEquals(monitor.getTopStatePartitionCount(), 12L);

    // Test with zero counts
    monitor.updatePartitionCount(0L);
    monitor.updateTopStatePartitionCount(0L);

    Assert.assertEquals(monitor.getPartitionCount(), 0L);
    Assert.assertEquals(monitor.getTopStatePartitionCount(), 0L);

    monitor.unregister();
  }

  @Test
  public void testErrorPartitionsWithDisabledPartitions() throws JMException {
    String testCluster = "testCluster";
    String testInstance = "testInstance";
    String testDomain = "testDomain:key=value";
    Set<String> tags = ImmutableSet.of("test");
    Map<String, List<String>> disabledPartitions = ImmutableMap.of(
        "resource1", ImmutableList.of("partition1", "partition2"),
        "resource2", ImmutableList.of("partition3")
    );
    
    InstanceMonitor monitor =
        new InstanceMonitor(testCluster, testInstance, new ObjectName(testDomain));

    // Instance has both disabled partitions and error partitions
    monitor.updateInstance(tags, disabledPartitions, Collections.emptyList(), true, true, 2L);
    
    // Verify both metrics are tracked independently
    Assert.assertEquals(monitor.getDisabledPartitions(), 3L, "Should have 3 disabled partitions");
    Assert.assertEquals(monitor.getErrorPartitions(), 2L, "Should have 2 error partitions");

    // Update error partition count while keeping disabled partitions the same
    monitor.updateInstance(tags, disabledPartitions, Collections.emptyList(), true, true, 5L);
    Assert.assertEquals(monitor.getDisabledPartitions(), 3L, "Disabled partitions should remain 3");
    Assert.assertEquals(monitor.getErrorPartitions(), 5L, "Error partitions should now be 5");

    monitor.unregister();
  }

  @Test
  public void testErrorPartitionsMultipleUpdates() throws JMException {
    String testCluster = "testCluster";
    String testInstance = "testInstance";
    String testDomain = "testDomain:key=value";
    Set<String> tags = ImmutableSet.of("test");
    
    InstanceMonitor monitor =
        new InstanceMonitor(testCluster, testInstance, new ObjectName(testDomain));

    // Simulate multiple updates with varying error partition counts
    long[] errorCounts = {0L, 1L, 3L, 2L, 5L, 0L, 1L};
    
    for (long errorCount : errorCounts) {
      monitor.updateInstance(tags, ImmutableMap.of(), Collections.emptyList(), true, true, errorCount);
      Assert.assertEquals(monitor.getErrorPartitions(), errorCount,
          "Error partition count should be " + errorCount);
    }

    monitor.unregister();
  }

  @Test
  public void testDomainInfoValidGauge() throws JMException {
    String testCluster = "testCluster";
    String testInstance = "testInstance";
    String testDomain = "testDomain:key=value";
    InstanceMonitor monitor =
        new InstanceMonitor(testCluster, testInstance, new ObjectName(testDomain));

    // Default should be valid (1)
    Assert.assertEquals(monitor.getDomainInfoValid(), 1L);

    // Mark domain info as invalid
    monitor.updateDomainInfoValid(false);
    Assert.assertEquals(monitor.getDomainInfoValid(), 0L);

    // Mark domain info as valid again
    monitor.updateDomainInfoValid(true);
    Assert.assertEquals(monitor.getDomainInfoValid(), 1L);

    // Toggle multiple times
    monitor.updateDomainInfoValid(false);
    Assert.assertEquals(monitor.getDomainInfoValid(), 0L);
    monitor.updateDomainInfoValid(false);
    Assert.assertEquals(monitor.getDomainInfoValid(), 0L);
    monitor.updateDomainInfoValid(true);
    Assert.assertEquals(monitor.getDomainInfoValid(), 1L);

    monitor.unregister();
  }

  @Test
  public void testPartitionCountEdgeCases() throws JMException {
    String testCluster = "testCluster";
    String testInstance = "testInstance";
    String testDomain = "testDomain:key=value";
    InstanceMonitor monitor =
        new InstanceMonitor(testCluster, testInstance, new ObjectName(testDomain));

    // Test 1: Initial state should be 0
    Assert.assertEquals(monitor.getPartitionCount(), 0L);
    Assert.assertEquals(monitor.getTopStatePartitionCount(), 0L);

    // Test 2: Update to non-zero values
    monitor.updatePartitionCount(25L);
    monitor.updateTopStatePartitionCount(10L);
    Assert.assertEquals(monitor.getPartitionCount(), 25L);
    Assert.assertEquals(monitor.getTopStatePartitionCount(), 10L);

    // Test 3: Simulate all partitions removed (instance evacuated or offline)
    monitor.updatePartitionCount(0L);
    monitor.updateTopStatePartitionCount(0L);
    Assert.assertEquals(monitor.getPartitionCount(), 0L);
    Assert.assertEquals(monitor.getTopStatePartitionCount(), 0L);

    // Test 4: Simulate partitions reassigned after coming back online
    monitor.updatePartitionCount(30L);
    monitor.updateTopStatePartitionCount(12L);
    Assert.assertEquals(monitor.getPartitionCount(), 30L);
    Assert.assertEquals(monitor.getTopStatePartitionCount(), 12L);

    // Test 5: TopState count should never exceed total partition count
    // (this is enforced by the calculation logic, but verify metric can hold correct values)
    monitor.updatePartitionCount(100L);
    monitor.updateTopStatePartitionCount(100L);
    Assert.assertEquals(monitor.getPartitionCount(), 100L);
    Assert.assertEquals(monitor.getTopStatePartitionCount(), 100L);

    // Test 6: Large numbers
    monitor.updatePartitionCount(1000000L);
    monitor.updateTopStatePartitionCount(500000L);
    Assert.assertEquals(monitor.getPartitionCount(), 1000000L);
    Assert.assertEquals(monitor.getTopStatePartitionCount(), 500000L);

    monitor.unregister();
  }
}
