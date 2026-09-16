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

import org.testng.Assert;
import org.testng.annotations.Test;

/**
 * Unit tests for controller and participant handoff metrics
 */
public class TestInProgressHandoffMetric {

  private static final String CLUSTER_NAME = "TestCluster";
  private static final String RESOURCE_NAME = "TestDB";

  @Test
  public void testControllerHandoffGaugeIncrementAndDecrement() {
    ClusterStatusMonitor clusterMonitor = new ClusterStatusMonitor(CLUSTER_NAME);
    clusterMonitor.active();

    // Increment gauge - this will create the resource monitor
    clusterMonitor.incrementControllerHandoffBeyondThresholdGauge(RESOURCE_NAME);

    // Get the resource monitor and verify
    ResourceMonitor resourceMonitor = clusterMonitor.getResourceMonitor(RESOURCE_NAME);
    Assert.assertEquals(resourceMonitor.getControllerHandoffBeyondThresholdGauge(), 1L);

    // Increment again
    clusterMonitor.incrementControllerHandoffBeyondThresholdGauge(RESOURCE_NAME);
    Assert.assertEquals(resourceMonitor.getControllerHandoffBeyondThresholdGauge(), 2L);

    // Decrement
    clusterMonitor.decrementControllerHandoffBeyondThresholdGauge(RESOURCE_NAME);
    Assert.assertEquals(resourceMonitor.getControllerHandoffBeyondThresholdGauge(), 1L);

    // Decrement to 0
    clusterMonitor.decrementControllerHandoffBeyondThresholdGauge(RESOURCE_NAME);
    Assert.assertEquals(resourceMonitor.getControllerHandoffBeyondThresholdGauge(), 0L);

    // Decrement below 0 should stay at 0
    clusterMonitor.decrementControllerHandoffBeyondThresholdGauge(RESOURCE_NAME);
    Assert.assertEquals(resourceMonitor.getControllerHandoffBeyondThresholdGauge(), 0L);

    clusterMonitor.reset();
  }

  @Test
  public void testControllerHandoffMultipleResources() {
    ClusterStatusMonitor clusterMonitor = new ClusterStatusMonitor(CLUSTER_NAME);
    clusterMonitor.active();

    // Test with multiple resources
    String resource1 = "DB1";
    String resource2 = "DB2";

    clusterMonitor.incrementControllerHandoffBeyondThresholdGauge(resource1);
    clusterMonitor.incrementControllerHandoffBeyondThresholdGauge(resource1);
    clusterMonitor.incrementControllerHandoffBeyondThresholdGauge(resource2);

    ResourceMonitor monitor1 = clusterMonitor.getResourceMonitor(resource1);
    ResourceMonitor monitor2 = clusterMonitor.getResourceMonitor(resource2);

    Assert.assertEquals(monitor1.getControllerHandoffBeyondThresholdGauge(), 2L);
    Assert.assertEquals(monitor2.getControllerHandoffBeyondThresholdGauge(), 1L);

    // Decrement one resource
    clusterMonitor.decrementControllerHandoffBeyondThresholdGauge(resource1);
    Assert.assertEquals(monitor1.getControllerHandoffBeyondThresholdGauge(), 1L);
    Assert.assertEquals(monitor2.getControllerHandoffBeyondThresholdGauge(), 1L);

    clusterMonitor.reset();
  }

  @Test
  public void testControllerHandoffMetricRegistration() {
    ClusterStatusMonitor clusterMonitor = new ClusterStatusMonitor(CLUSTER_NAME);
    clusterMonitor.active();

    // Increment to create resource monitor
    clusterMonitor.incrementControllerHandoffBeyondThresholdGauge(RESOURCE_NAME);

    // Verify resource monitor exists and metric can be read
    ResourceMonitor resourceMonitor = clusterMonitor.getResourceMonitor(RESOURCE_NAME);
    Assert.assertNotNull(resourceMonitor);
    Assert.assertEquals(resourceMonitor.getControllerHandoffBeyondThresholdGauge(), 1L);

    clusterMonitor.reset();
  }

  @Test
  public void testParticipantHandoffGaugeIncrementAndDecrement() {
    ClusterStatusMonitor clusterMonitor = new ClusterStatusMonitor(CLUSTER_NAME);
    clusterMonitor.active();

    clusterMonitor.incrementParticipantHandoffBeyondThresholdGauge(RESOURCE_NAME);
    ResourceMonitor resourceMonitor = clusterMonitor.getResourceMonitor(RESOURCE_NAME);
    Assert.assertEquals(resourceMonitor.getParticipantHandoffBeyondThresholdGauge(), 1L);

    clusterMonitor.incrementParticipantHandoffBeyondThresholdGauge(RESOURCE_NAME);
    Assert.assertEquals(resourceMonitor.getParticipantHandoffBeyondThresholdGauge(), 2L);

    clusterMonitor.decrementParticipantHandoffBeyondThresholdGauge(RESOURCE_NAME);
    Assert.assertEquals(resourceMonitor.getParticipantHandoffBeyondThresholdGauge(), 1L);

    clusterMonitor.decrementParticipantHandoffBeyondThresholdGauge(RESOURCE_NAME);
    Assert.assertEquals(resourceMonitor.getParticipantHandoffBeyondThresholdGauge(), 0L);

    clusterMonitor.decrementParticipantHandoffBeyondThresholdGauge(RESOURCE_NAME);
    Assert.assertEquals(resourceMonitor.getParticipantHandoffBeyondThresholdGauge(), 0L);

    clusterMonitor.reset();
  }
}
