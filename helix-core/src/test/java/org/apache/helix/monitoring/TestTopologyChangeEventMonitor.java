package org.apache.helix.monitoring;

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

import java.lang.management.ManagementFactory;
import java.util.Set;
import javax.management.MBeanServer;
import javax.management.ObjectInstance;
import javax.management.ObjectName;

import org.apache.helix.controller.stages.ClusterEventType;
import org.apache.helix.monitoring.mbeans.ClusterStatusMonitor;
import org.apache.helix.monitoring.mbeans.TopologyChangeEventMonitor;
import org.testng.Assert;
import org.testng.annotations.Test;

public class TestTopologyChangeEventMonitor {

  private static final String CLUSTER = "TopoMetricTestCluster";

  @Test
  public void testEagerRegistrationAndCounters() throws Exception {
    ClusterStatusMonitor monitor = new ClusterStatusMonitor(CLUSTER);
    monitor.active();
    try {
      MBeanServer server = ManagementFactory.getPlatformMBeanServer();

      // 5 topology event types -> 5 MBeans registered eagerly at active().
      Set<ObjectInstance> mbeans = server.queryMBeans(
          new ObjectName(
              "ClusterStatus:cluster=" + CLUSTER + ",eventName=TopologyChangeEvent,*"),
          null);
      Assert.assertEquals(mbeans.size(),
          ClusterEventType.topologyChangeEventTypes().size(),
          "Expected one MBean per topology-change event type");

      // Every MBean has the expected attributes, all starting at zero.
      for (ObjectInstance mbean : mbeans) {
        ObjectName name = mbean.getObjectName();
        Long received = (Long) server.getAttribute(name, "ReceivedCounter");
        Long processed = (Long) server.getAttribute(name, "ProcessedCounter");
        Assert.assertEquals(received, Long.valueOf(0L),
            "ReceivedCounter should start at 0 for " + name);
        Assert.assertEquals(processed, Long.valueOf(0L),
            "ProcessedCounter should start at 0 for " + name);
      }

      // Increments only flow to the matching event type.
      monitor.incrementTopologyChangeEventReceived(ClusterEventType.IdealStateChange);
      monitor.incrementTopologyChangeEventReceived(ClusterEventType.IdealStateChange);
      monitor.incrementTopologyChangeEventProcessed(ClusterEventType.IdealStateChange);

      monitor.incrementTopologyChangeEventReceived(ClusterEventType.LiveInstanceChange);

      ObjectName idealStateBean = topologyBean(ClusterEventType.IdealStateChange);
      ObjectName liveInstanceBean = topologyBean(ClusterEventType.LiveInstanceChange);
      ObjectName clusterConfigBean = topologyBean(ClusterEventType.ClusterConfigChange);

      Assert.assertEquals(server.getAttribute(idealStateBean, "ReceivedCounter"), 2L);
      Assert.assertEquals(server.getAttribute(idealStateBean, "ProcessedCounter"), 1L);
      Assert.assertEquals(server.getAttribute(liveInstanceBean, "ReceivedCounter"), 1L);
      Assert.assertEquals(server.getAttribute(liveInstanceBean, "ProcessedCounter"), 0L);
      Assert.assertEquals(server.getAttribute(clusterConfigBean, "ReceivedCounter"), 0L);
      Assert.assertEquals(server.getAttribute(clusterConfigBean, "ProcessedCounter"), 0L);

      // Non-topology types are silently dropped.
      monitor.incrementTopologyChangeEventReceived(ClusterEventType.MessageChange);
      monitor.incrementTopologyChangeEventProcessed(ClusterEventType.CurrentStateChange);
      monitor.incrementTopologyChangeEventReceived(null);
      // No MBean ever appears for those types -- still only 5 topology MBeans.
      mbeans = server.queryMBeans(
          new ObjectName(
              "ClusterStatus:cluster=" + CLUSTER + ",eventName=TopologyChangeEvent,*"),
          null);
      Assert.assertEquals(mbeans.size(),
          ClusterEventType.topologyChangeEventTypes().size());

      // SensorName is per-type so OTel exporters can uniquely key on it.
      String sensor = (String) server.getAttribute(idealStateBean, "SensorName");
      Assert.assertTrue(sensor.contains(CLUSTER),
          "SensorName should include cluster: " + sensor);
      Assert.assertTrue(sensor.contains(ClusterEventType.IdealStateChange.name()),
          "SensorName should include event type: " + sensor);
    } finally {
      monitor.reset();

      // After reset the topology MBeans should be unregistered.
      MBeanServer server = ManagementFactory.getPlatformMBeanServer();
      Set<ObjectInstance> mbeans = server.queryMBeans(
          new ObjectName(
              "ClusterStatus:cluster=" + CLUSTER + ",eventName=TopologyChangeEvent,*"),
          null);
      Assert.assertEquals(mbeans.size(), 0,
          "Topology MBeans should be unregistered after reset()");
    }
  }

  @Test
  public void testConstructorRejectsNonTopologyType() {
    ClusterStatusMonitor monitor = new ClusterStatusMonitor(CLUSTER + "Reject");
    try {
      new TopologyChangeEventMonitor(monitor, ClusterEventType.MessageChange);
      Assert.fail("Expected IllegalArgumentException for non-topology event type");
    } catch (IllegalArgumentException expected) {
      // expected
    }
  }

  @Test
  public void testTopologyChangeEventTypesCoverage() {
    // Lock the topology set in the test so future enum additions are a conscious choice.
    Set<ClusterEventType> expected = java.util.EnumSet.of(
        ClusterEventType.IdealStateChange,
        ClusterEventType.InstanceConfigChange,
        ClusterEventType.ResourceConfigChange,
        ClusterEventType.LiveInstanceChange,
        ClusterEventType.ClusterConfigChange);
    Assert.assertEquals(ClusterEventType.topologyChangeEventTypes(), expected);
    for (ClusterEventType t : ClusterEventType.values()) {
      Assert.assertEquals(t.isTopologyChange(), expected.contains(t),
          "isTopologyChange mismatch for " + t);
    }
  }

  private ObjectName topologyBean(ClusterEventType eventType) throws Exception {
    return new ObjectName(
        "ClusterStatus:cluster=" + CLUSTER
            + ",eventName=TopologyChangeEvent,eventType=" + eventType.name());
  }
}
