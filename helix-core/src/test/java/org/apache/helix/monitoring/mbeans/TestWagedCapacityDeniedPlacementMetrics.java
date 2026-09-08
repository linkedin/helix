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
 * The point of these metrics is to make a silent refusal visible, so the behaviour that matters is
 * that the gauges track the current pass rather than latching on the first bad one.
 */
public class TestWagedCapacityDeniedPlacementMetrics {
  @Test
  public void testCounterAccumulatesAndGaugesTrackTheCurrentPass() {
    ClusterStatusMonitor monitor = new ClusterStatusMonitor("TestCluster_deniedPlacements");

    Assert.assertEquals(monitor.getWagedCapacityDeniedPlacementCounter(), 0L);
    Assert.assertEquals(monitor.getWagedCapacityDeniedPlacementsLastPassGauge(), 0L);
    Assert.assertEquals(monitor.getWagedCapacityDeniedInstancesLastPassGauge(), 0L);

    monitor.reportWagedCapacityDeniedPlacements(5, 2);
    Assert.assertEquals(monitor.getWagedCapacityDeniedPlacementCounter(), 5L);
    Assert.assertEquals(monitor.getWagedCapacityDeniedPlacementsLastPassGauge(), 5L);
    Assert.assertEquals(monitor.getWagedCapacityDeniedInstancesLastPassGauge(), 2L);

    monitor.reportWagedCapacityDeniedPlacements(3, 1);
    Assert.assertEquals(monitor.getWagedCapacityDeniedPlacementCounter(), 8L,
        "the counter is the monotonic 'how often' tally and must accumulate");
    Assert.assertEquals(monitor.getWagedCapacityDeniedPlacementsLastPassGauge(), 3L,
        "the gauge describes the latest pass only");

    // A clean pass has to clear the gauges, otherwise the signal cannot distinguish a cluster that
    // is currently refusing placements from one that refused some an hour ago and recovered.
    monitor.reportWagedCapacityDeniedPlacements(0, 0);
    Assert.assertEquals(monitor.getWagedCapacityDeniedPlacementsLastPassGauge(), 0L,
        "gauges must fall back to zero on a clean pass");
    Assert.assertEquals(monitor.getWagedCapacityDeniedInstancesLastPassGauge(), 0L);
    Assert.assertEquals(monitor.getWagedCapacityDeniedPlacementCounter(), 8L,
        "a clean pass must not erase the cumulative count");
  }
}
