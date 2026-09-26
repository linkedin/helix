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

package org.apache.helix.controller.rebalancer.util;

import java.util.Collections;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import org.apache.helix.model.ClusterConfig;
import org.apache.helix.model.IdealState;
import org.apache.helix.model.InstanceConfig;
import org.apache.helix.util.ConfigStringUtil;
import org.apache.helix.zookeeper.datamodel.ZNRecord;
import org.testng.Assert;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

public class TestDelayedRebalanceUtil {
  @DataProvider
  public Object[][] resourceRebalanceDelays() {
    return new Object[][] {{null, 5000L}, {-1L, 5000L}, {0L, 0L}, {1000L, 1000L}};
  }

  @Test(dataProvider = "resourceRebalanceDelays")
  public void testIdealStateDelayOverridesClusterDefault(Long resourceDelay, long expectedDelay) {
    ClusterConfig clusterConfig = new ClusterConfig("cluster");
    clusterConfig.setRebalanceDelayTime(5000L);
    IdealState idealState = new IdealState("resource");
    if (resourceDelay != null) {
      idealState.setRebalanceDelay(resourceDelay);
    }
    ZNRecord original = new ZNRecord(idealState.getRecord());

    Assert.assertEquals(DelayedRebalanceUtil.getRebalanceDelay(idealState, clusterConfig),
        expectedDelay);
    Assert.assertEquals(idealState.getRecord(), original);
  }

  @DataProvider
  public Object[][] legacyDisableTimestamps() {
    return new Object[][] {
        {null, true}, {"1", true}, {"not-a-timestamp", true}, {Long.toString(Long.MAX_VALUE), false}
    };
  }

  @Test(dataProvider = "legacyDisableTimestamps")
  public void testIgnoreRetiredClusterDisableTimestamp(String legacyTimestamp, boolean recentDisable) {
    String instance = "instance";
    ClusterConfig clusterConfig = new ClusterConfig("cluster");
    clusterConfig.setDelayRebalaceEnabled(true);
    clusterConfig.setRebalanceDelayTime(TimeUnit.HOURS.toMillis(1));
    if (legacyTimestamp != null) {
      clusterConfig.getRecord().setMapField("DISABLED_INSTANCES_WITH_INFO",
          Collections.singletonMap(instance, ConfigStringUtil.concatenateMapping(
              Collections.singletonMap("HELIX_ENABLED_DISABLE_TIMESTAMP", legacyTimestamp))));
    }
    ZNRecord original = new ZNRecord(clusterConfig.getRecord());

    InstanceConfig instanceConfig = new InstanceConfig(instance);
    instanceConfig.setInstanceEnabled(false);
    instanceConfig.setDelayRebalanceEnabled(true);
    instanceConfig.getRecord().setLongField(
        InstanceConfig.InstanceConfigProperty.HELIX_ENABLED_TIMESTAMP.name(),
        recentDisable ? System.currentTimeMillis() : 1L);

    Set<String> nodes = Collections.singleton(instance);
    Set<String> activeNodes = DelayedRebalanceUtil.getActiveNodes(nodes, Collections.emptySet(),
        Collections.emptyMap(), nodes, Collections.singletonMap(instance, instanceConfig),
        clusterConfig);
    Assert.assertEquals(activeNodes, recentDisable ? nodes : Collections.emptySet());
    Assert.assertEquals(clusterConfig.getRecord(), original);
  }
}
