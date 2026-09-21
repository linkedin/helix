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

import java.util.Collections;
import org.apache.helix.ConfigAccessor;
import org.apache.helix.HelixDataAccessor;
import org.apache.helix.HelixManager;
import org.apache.helix.NotificationContext;
import org.apache.helix.PropertyKey;
import org.apache.helix.model.ClusterConfig;
import org.apache.helix.model.IdealState;
import org.apache.helix.zookeeper.datamodel.ZNRecord;
import org.testng.Assert;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.argThat;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.timeout;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class TestClusterRebalanceTimer {
  private static final String CLUSTER_NAME = "TestClusterRebalanceTimer";
  private static final String LEGACY_TIMER = "REBALANCE_TIMER_PERIOD";

  @DataProvider
  public Object[][] legacyTimerValues() {
    return new Object[][] {{null}, {"1"}, {"0"}, {"-1"}, {"200000"}, {"not-a-period"}};
  }

  @DataProvider
  public Object[][] disabledClusterPeriods() {
    return new Object[][] {{-1L}, {0L}, {Long.MAX_VALUE}};
  }

  @Test(dataProvider = "legacyTimerValues")
  public void testResourceCannotEnablePeriodicRebalance(String legacyPeriod) throws Exception {
    NotificationContext context = createContext(new ClusterConfig(CLUSTER_NAME));
    GenericHelixController controller = new GenericHelixController(CLUSTER_NAME);
    try {
      notifyIdealStateChange(controller, context, legacyPeriod);
      Assert.assertEquals(controller._timerPeriod, Long.MAX_VALUE);
    } finally {
      controller.shutdown();
    }
  }

  @Test(dataProvider = "legacyTimerValues")
  public void testOnlyClusterPeriodControlsSchedule(String legacyPeriod) throws Exception {
    ClusterConfig config = new ClusterConfig(CLUSTER_NAME);
    config.setRebalanceTimePeriod(100000L);
    NotificationContext context = createContext(config);
    GenericHelixController controller = new GenericHelixController(CLUSTER_NAME);
    try {
      notifyIdealStateChange(controller, context, legacyPeriod);
      Assert.assertEquals(controller._timerPeriod, 100000L);

      config.setRebalanceTimePeriod(300000L);
      notifyIdealStateChange(controller, context, legacyPeriod);
      Assert.assertEquals(controller._timerPeriod, 300000L);
    } finally {
      controller.shutdown();
    }
  }

  @Test(dataProvider = "disabledClusterPeriods")
  public void testResourceCannotKeepClusterTimerEnabled(long disabledPeriod) throws Exception {
    ClusterConfig config = new ClusterConfig(CLUSTER_NAME);
    config.setRebalanceTimePeriod(100000L);
    NotificationContext context = createContext(config);
    GenericHelixController controller = new GenericHelixController(CLUSTER_NAME);
    try {
      notifyIdealStateChange(controller, context, "1");
      Assert.assertEquals(controller._timerPeriod, 100000L);

      config.setRebalanceTimePeriod(disabledPeriod);
      notifyIdealStateChange(controller, context, "1");
      Assert.assertEquals(controller._timerPeriod, Long.MAX_VALUE);
    } finally {
      controller.shutdown();
    }
  }

  @Test
  public void testMissingClusterConfigDoesNotEnableTimer() throws Exception {
    GenericHelixController controller = new GenericHelixController(CLUSTER_NAME);
    try {
      notifyIdealStateChange(controller, createContext(null), "1");
      Assert.assertEquals(controller._timerPeriod, Long.MAX_VALUE);
    } finally {
      controller.shutdown();
    }
  }

  @Test
  public void testClusterTimerStillRefreshesLiveInstances() throws Exception {
    ClusterConfig config = new ClusterConfig(CLUSTER_NAME);
    config.setRebalanceTimePeriod(20L);
    NotificationContext context = createContext(config);
    GenericHelixController controller = new GenericHelixController(CLUSTER_NAME);
    try {
      notifyIdealStateChange(controller, context, "1");
      Assert.assertEquals(controller._timerPeriod, 20L);
      HelixDataAccessor accessor = context.getManager().getHelixDataAccessor();
      String liveInstancesPath = accessor.keyBuilder().liveInstances().getPath();
      verify(accessor, timeout(5000).atLeastOnce())
          .getChildValues(argThat(key -> key.getPath().equals(liveInstancesPath)), eq(true));
    } finally {
      controller.shutdown();
    }
  }

  private NotificationContext createContext(ClusterConfig config) {
    HelixManager manager = mock(HelixManager.class);
    HelixDataAccessor accessor = mock(HelixDataAccessor.class);
    when(manager.getConfigAccessor()).thenReturn(mock(ConfigAccessor.class));
    when(manager.getHelixDataAccessor()).thenReturn(accessor);
    when(manager.getClusterName()).thenReturn(CLUSTER_NAME);
    when(accessor.keyBuilder()).thenReturn(new PropertyKey.Builder(CLUSTER_NAME));
    when(accessor.getProperty(any(PropertyKey.class))).thenReturn(config);
    NotificationContext context = new NotificationContext(manager);
    context.setType(NotificationContext.Type.CALLBACK);
    return context;
  }

  private void notifyIdealStateChange(GenericHelixController controller,
      NotificationContext context, String legacyPeriod) {
    IdealState idealState = new IdealState("resource");
    if (legacyPeriod != null) {
      idealState.getRecord().setSimpleField(LEGACY_TIMER, legacyPeriod);
    }
    ZNRecord originalRecord = new ZNRecord(idealState.getRecord());
    controller.onIdealStateChange(Collections.singletonList(idealState), context);
    Assert.assertEquals(idealState.getRecord(), originalRecord);
  }
}
