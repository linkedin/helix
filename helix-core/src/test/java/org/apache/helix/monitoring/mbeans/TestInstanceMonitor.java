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
    monitor.updateInstance(tags, disabledPartitions, Collections.emptyList(), true, true);
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

    monitor.unregister();
  }

  @Test
  public void testInstanceOperationDurationMetrics() throws JMException, InterruptedException {
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

    // Test EVACUATE operation
    monitor.updateInstanceOperation(InstanceConstants.InstanceOperation.EVACUATE);

    // Wait 100ms to let duration accumulate
    Thread.sleep(100);

    // Update again to calculate current duration
    monitor.updateInstanceOperation(InstanceConstants.InstanceOperation.EVACUATE);

    // EVACUATE duration should be > 0 and roughly >= 100ms
    long evacuateDuration = monitor.getInstanceOperationDurationEvacuate();
    Assert.assertTrue(evacuateDuration >= 100L,
        "EVACUATE duration should be >= 100ms, but was " + evacuateDuration);

    // All other operations should be 0
    Assert.assertEquals(monitor.getInstanceOperationDurationEnable(), 0L);
    Assert.assertEquals(monitor.getInstanceOperationDurationDisable(), 0L);
    Assert.assertEquals(monitor.getInstanceOperationDurationSwapIn(), 0L);
    Assert.assertEquals(monitor.getInstanceOperationDurationUnknown(), 0L);

    // Wait another 100ms
    Thread.sleep(100);

    // Update again - duration should have increased
    monitor.updateInstanceOperation(InstanceConstants.InstanceOperation.EVACUATE);
    long evacuateDuration2 = monitor.getInstanceOperationDurationEvacuate();
    Assert.assertTrue(evacuateDuration2 > evacuateDuration,
        "EVACUATE duration should increase over time");
    Assert.assertTrue(evacuateDuration2 >= 200L,
        "EVACUATE duration should be >= 200ms, but was " + evacuateDuration2);

    // Change to DISABLE operation
    monitor.updateInstanceOperation(InstanceConstants.InstanceOperation.DISABLE);

    // EVACUATE should now be 0, DISABLE should start counting
    Assert.assertEquals(monitor.getInstanceOperationDurationEvacuate(), 0L,
        "EVACUATE duration should be 0 after switching to DISABLE");
    Assert.assertEquals(monitor.getInstanceOperationDurationDisable(), 0L,
        "DISABLE duration should start at 0");

    // Wait and verify DISABLE duration increases
    Thread.sleep(100);
    monitor.updateInstanceOperation(InstanceConstants.InstanceOperation.DISABLE);
    long disableDuration = monitor.getInstanceOperationDurationDisable();
    Assert.assertTrue(disableDuration >= 100L,
        "DISABLE duration should be >= 100ms, but was " + disableDuration);
    Assert.assertEquals(monitor.getInstanceOperationDurationEvacuate(), 0L);

    // Test SWAP_IN operation
    monitor.updateInstanceOperation(InstanceConstants.InstanceOperation.SWAP_IN);
    Thread.sleep(50);
    monitor.updateInstanceOperation(InstanceConstants.InstanceOperation.SWAP_IN);

    long swapInDuration = monitor.getInstanceOperationDurationSwapIn();
    Assert.assertTrue(swapInDuration >= 50L,
        "SWAP_IN duration should be >= 50ms, but was " + swapInDuration);
    Assert.assertEquals(monitor.getInstanceOperationDurationDisable(), 0L);
    Assert.assertEquals(monitor.getInstanceOperationDurationEvacuate(), 0L);

    // Test UNKNOWN operation
    monitor.updateInstanceOperation(InstanceConstants.InstanceOperation.UNKNOWN);
    Thread.sleep(50);
    monitor.updateInstanceOperation(InstanceConstants.InstanceOperation.UNKNOWN);

    long unknownDuration = monitor.getInstanceOperationDurationUnknown();
    Assert.assertTrue(unknownDuration >= 50L,
        "UNKNOWN duration should be >= 50ms, but was " + unknownDuration);
    Assert.assertEquals(monitor.getInstanceOperationDurationSwapIn(), 0L);

    // Test going back to ENABLE - all should be 0
    monitor.updateInstanceOperation(InstanceConstants.InstanceOperation.ENABLE);
    Thread.sleep(50);
    monitor.updateInstanceOperation(InstanceConstants.InstanceOperation.ENABLE);

    Assert.assertEquals(monitor.getInstanceOperationDurationDisable(), 0L);
    Assert.assertEquals(monitor.getInstanceOperationDurationEvacuate(), 0L);
    Assert.assertEquals(monitor.getInstanceOperationDurationSwapIn(), 0L);
    Assert.assertEquals(monitor.getInstanceOperationDurationUnknown(), 0L);

    // ENABLE duration should be > 0
    long enableDuration = monitor.getInstanceOperationDurationEnable();
    Assert.assertTrue(enableDuration >= 50L,
        "ENABLE duration should be >= 50ms, but was " + enableDuration);

    // Test null operation defaults to ENABLE
    monitor.updateInstanceOperation(null);
    Thread.sleep(50);
    monitor.updateInstanceOperation(null);
    long enableDuration2 = monitor.getInstanceOperationDurationEnable();
    Assert.assertTrue(enableDuration2 > enableDuration,
        "ENABLE duration should continue increasing");

    monitor.unregister();
  }

  @Test
  public void testInstanceOperationDurationWithInstanceConfigAPI()
      throws JMException, InterruptedException {
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

    // Update monitor with the new operation (simulating what ClusterStatusMonitor does)
    monitor.updateInstanceOperation(instanceConfig.getInstanceOperation().getOperation());

    // Wait for duration to accumulate
    Thread.sleep(150);

    // Update monitor again to get current duration
    monitor.updateInstanceOperation(instanceConfig.getInstanceOperation().getOperation());

    // Verify EVACUATE duration is tracking
    long evacuateDuration = monitor.getInstanceOperationDurationEvacuate();
    Assert.assertTrue(evacuateDuration >= 150L,
        "EVACUATE duration should be >= 150ms, but was " + evacuateDuration);
    Assert.assertEquals(monitor.getInstanceOperationDurationEnable(), 0L,
        "ENABLE should be 0 when in EVACUATE");

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
    monitor2.updateInstanceOperation(instanceConfig2.getInstanceOperation().getOperation());

    Thread.sleep(100);
    monitor2.updateInstanceOperation(instanceConfig2.getInstanceOperation().getOperation());

    long disableDuration = monitor2.getInstanceOperationDurationDisable();
    Assert.assertTrue(disableDuration >= 100L,
        "DISABLE duration should be >= 100ms, but was " + disableDuration);
    Assert.assertEquals(monitor2.getInstanceOperationDurationEvacuate(), 0L,
        "EVACUATE should be 0 for this instance");

    // Clean up
    monitor.unregister();
    monitor2.unregister();
  }
}
