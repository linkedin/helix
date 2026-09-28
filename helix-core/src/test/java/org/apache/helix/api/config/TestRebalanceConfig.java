package org.apache.helix.api.config;

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

import java.util.HashMap;
import java.util.Map;
import org.apache.helix.model.ResourceConfig;
import org.apache.helix.zookeeper.datamodel.ZNRecord;
import org.testng.Assert;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

public class TestRebalanceConfig {
  @DataProvider
  public Object[][] legacyTimerValues() {
    return new Object[][] {{null}, {"1"}, {"0"}, {"-1"}, {"not-a-period"}};
  }

  @Test(dataProvider = "legacyTimerValues")
  public void testLegacyResourceTimerIsNotEmitted(String legacyPeriod) {
    ZNRecord record = new ZNRecord("resource");
    record.setLongField("REBALANCE_DELAY", 1000L);
    record.setSimpleField("REBALANCE_MODE", "FULL_AUTO");
    record.setSimpleField("REBALANCER_CLASS_NAME", "custom.Rebalancer");
    record.setSimpleField("REBALANCE_STRATEGY", "custom.Strategy");
    Map<String, String> expected = new HashMap<>(record.getSimpleFields());
    if (legacyPeriod != null) {
      record.setSimpleField("REBALANCE_TIMER_PERIOD", legacyPeriod);
    }
    ZNRecord originalRecord = new ZNRecord(record);

    RebalanceConfig config = new RebalanceConfig(record);
    Assert.assertEquals(config.getConfigsMap(), expected);
    Assert.assertEquals(config.getRebalanceDelay(), 1000L);
    Assert.assertEquals(config.getRebalanceMode(), RebalanceConfig.RebalanceMode.FULL_AUTO);
    Assert.assertEquals(config.getRebalanceClassName(), "custom.Rebalancer");
    Assert.assertEquals(config.getRebalanceStrategy(), "custom.Strategy");

    ResourceConfig resourceConfig =
        new ResourceConfig.Builder("resource").setRebalanceConfig(config).build();
    Assert.assertFalse(resourceConfig.getRecord().getSimpleFields()
        .containsKey("REBALANCE_TIMER_PERIOD"));
    Assert.assertEquals(resourceConfig.getRebalanceConfig().getConfigsMap(), expected);
    Assert.assertEquals(record, originalRecord);
  }
}
