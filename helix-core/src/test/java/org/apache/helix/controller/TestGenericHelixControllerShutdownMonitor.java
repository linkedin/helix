package org.apache.helix.controller;

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
import java.lang.reflect.Field;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import javax.management.MBeanServer;
import javax.management.ObjectName;

import org.apache.helix.HelixManager;
import org.apache.helix.NotificationContext;
import org.apache.helix.TestHelper;
import org.apache.helix.monitoring.mbeans.ClusterStatusMonitor;
import org.apache.helix.monitoring.mbeans.MonitorDomainNames;
import org.testng.Assert;
import org.testng.annotations.Test;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * A shut-down controller must not keep or regain its ClusterStatus MBean: once its pipelines are
 * gone, a registered bean would report a stalled pipeline forever on a host that no longer leads.
 */
public class TestGenericHelixControllerShutdownMonitor {
  private static final MBeanServer MBEAN_SERVER = ManagementFactory.getPlatformMBeanServer();

  @Test
  public void testLateLeaderCallbackAfterShutdownDoesNotRegisterMonitor() throws Exception {
    String cluster = "TestShutdownMonitorLateCallback";
    ObjectName clusterBean = clusterBean(cluster);
    GenericHelixController controller = new GenericHelixController(cluster);
    NotificationContext leaderCallback = leaderContext(cluster);
    try {
      controller.onControllerChange(leaderCallback);
      Assert.assertTrue(MBEAN_SERVER.isRegistered(clusterBean));

      controller.shutdown();
      Assert.assertFalse(MBEAN_SERVER.isRegistered(clusterBean));

      // A leader-election callback that was already in flight when the manager disconnected.
      controller.onControllerChange(leaderCallback);
      Assert.assertFalse(MBEAN_SERVER.isRegistered(clusterBean));
    } finally {
      controller.shutdown();
    }
  }

  @Test
  public void testInterruptedShutdownStillUnregistersMonitor() throws Exception {
    String cluster = "TestShutdownMonitorInterrupted";
    ObjectName clusterBean = clusterBean(cluster);
    GenericHelixController controller = new GenericHelixController(cluster);
    try {
      controller.onControllerChange(leaderContext(cluster));
      Assert.assertTrue(MBEAN_SERVER.isRegistered(clusterBean));

      // Joining the pipeline threads throws once the caller is interrupted.
      Thread.currentThread().interrupt();
      boolean interrupted = false;
      try {
        controller.shutdown();
      } catch (InterruptedException expected) {
        interrupted = true;
      } finally {
        Thread.interrupted();
      }
      Assert.assertTrue(interrupted, "shutdown() should have been interrupted");
      Assert.assertFalse(MBEAN_SERVER.isRegistered(clusterBean));
    } finally {
      controller.shutdown();
    }
  }

  @Test
  public void testStalledGaugeFollowsDefaultPipelineRuns() throws Exception {
    String cluster = "TestShutdownMonitorRunStart";
    NotificationContext leaderCallback = leaderContext(cluster);
    AtomicBoolean blockRuns = new AtomicBoolean(false);
    CountDownLatch ranOnce = new CountDownLatch(1);
    CountDownLatch inBlockedRun = new CountDownLatch(1);
    CountDownLatch interruptedInRun = new CountDownLatch(1);
    CountDownLatch release = new CountDownLatch(1);
    // Every pipeline run reads the manager session first, so this marks a DEFAULT run and can hold
    // it like a run wedged on a lock: interrupts do not end it.
    when(leaderCallback.getManager().getSessionId()).thenAnswer(invocation -> {
      if (Thread.currentThread().getName().startsWith("HelixController-pipeline-default-")) {
        ranOnce.countDown();
        if (blockRuns.get()) {
          inBlockedRun.countDown();
          boolean done = false;
          while (!done) {
            try {
              release.await(TestHelper.WAIT_DURATION, TimeUnit.MILLISECONDS);
              done = true;
            } catch (InterruptedException e) {
              interruptedInRun.countDown();
            }
          }
        }
      }
      return "session";
    });
    GenericHelixController controller = new GenericHelixController(cluster);
    Field monitorField = GenericHelixController.class.getDeclaredField("_clusterStatusMonitor");
    monitorField.setAccessible(true);
    ClusterStatusMonitor monitor = (ClusterStatusMonitor) monitorField.get(controller);
    monitor.setPipelineStallThresholdMs(1L);
    Thread shutdownThread = new Thread(() -> {
      try {
        controller.shutdown();
      } catch (InterruptedException ignored) {
      }
    });
    try {
      controller.onControllerChange(leaderCallback);
      Assert.assertTrue(ranOnce.await(TestHelper.WAIT_DURATION, TimeUnit.MILLISECONDS));

      // Idle and waiting for the next event: never stalled, even with a stale backlog.
      monitor.setControllerEventQueueSizeGauge(5L);
      Assert.assertTrue(TestHelper.verify(() -> monitor.getControllerPipelineStalledGauge() == 0L,
          TestHelper.WAIT_DURATION));
      Thread.sleep(20L);
      Assert.assertEquals(monitor.getControllerPipelineStalledGauge(), 0L);

      // A run that outlasts the threshold while another event waits behind it is stalled.
      blockRuns.set(true);
      controller.onControllerChange(leaderCallback);
      Assert.assertTrue(inBlockedRun.await(TestHelper.WAIT_DURATION, TimeUnit.MILLISECONDS));
      controller.onControllerChange(leaderCallback);
      Assert.assertTrue(TestHelper.verify(() -> monitor.getControllerPipelineStalledGauge() == 1L,
          TestHelper.WAIT_DURATION));

      // Shutdown cannot stop that run, so the controller keeps leadership: it must stay stalled.
      shutdownThread.start();
      Assert.assertTrue(interruptedInRun.await(TestHelper.WAIT_DURATION, TimeUnit.MILLISECONDS));
      Assert.assertEquals(monitor.getControllerPipelineStalledGauge(), 1L);
    } finally {
      release.countDown();
      shutdownThread.join(TestHelper.WAIT_DURATION);
      controller.shutdown();
    }
  }

  private static NotificationContext leaderContext(String cluster) {
    HelixManager manager = mock(HelixManager.class);
    when(manager.getClusterName()).thenReturn(cluster);
    when(manager.isLeader()).thenReturn(true);
    NotificationContext context = new NotificationContext(manager);
    context.setType(NotificationContext.Type.CALLBACK);
    return context;
  }

  private static ObjectName clusterBean(String cluster) throws Exception {
    return new ObjectName(String.format("%s:%s=%s", MonitorDomainNames.ClusterStatus.name(),
        ClusterStatusMonitor.CLUSTER_DN_KEY, cluster));
  }
}
