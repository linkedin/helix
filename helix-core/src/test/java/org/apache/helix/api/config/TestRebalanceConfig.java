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

import java.lang.reflect.Method;
import java.util.Arrays;
import java.util.Collections;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import com.google.common.collect.ImmutableMap;
import org.apache.helix.zookeeper.datamodel.ZNRecord;
import org.testng.Assert;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

public class TestRebalanceConfig {
  @Test
  public void testDefaultsDoNotSynthesizeRebalanceMode() {
    RebalanceConfig config = new RebalanceConfig(new ZNRecord("resource"));

    Assert.assertNull(config.getRebalanceStrategy());
    Assert.assertEquals(config.getRebalanceTimerPeriod(), -1L);
    Assert.assertEquals(config.getConfigsMap(), Collections.emptyMap());
    Assert.assertTrue(config.isValid());
  }

  @DataProvider
  public Object[][] legacyRebalanceFields() {
    return new Object[][] {
        {"1000", "FULL_AUTO"}, {"not-a-delay", "not-a-mode"}
    };
  }

  @Test(dataProvider = "legacyRebalanceFields")
  public void testLegacyFieldsAreNotExported(String delay, String mode) {
    ZNRecord record = new ZNRecord("resource");
    record.setSimpleField("REBALANCE_DELAY", delay);
    record.setSimpleField("REBALANCE_MODE", mode);
    record.setSimpleField("REBALANCER_CLASS_NAME", "legacy.Rebalancer");
    record.setSimpleField("REBALANCE_STRATEGY", "retained.Strategy");
    record.setLongField("REBALANCE_TIMER_PERIOD", 1000L);
    ZNRecord original = new ZNRecord(record);

    RebalanceConfig config = new RebalanceConfig(record);

    Assert.assertEquals(config.getRebalanceStrategy(), "retained.Strategy");
    Assert.assertEquals(config.getRebalanceTimerPeriod(), 1000L);
    Assert.assertEquals(config.getConfigsMap(),
        ImmutableMap.of("REBALANCE_STRATEGY", "retained.Strategy",
            "REBALANCE_TIMER_PERIOD", "1000"));
    Assert.assertEquals(record, original);
  }

  @DataProvider
  public Object[][] retainedSettings() {
    return new Object[][] {
        {null, -1L, Collections.emptyMap()},
        {"", 0L, Collections.singletonMap("REBALANCE_STRATEGY", "")},
        {null, 1L, Collections.singletonMap("REBALANCE_TIMER_PERIOD", "1")},
        {"retained.Strategy", Long.MAX_VALUE,
            ImmutableMap.of("REBALANCE_STRATEGY", "retained.Strategy",
                "REBALANCE_TIMER_PERIOD", Long.toString(Long.MAX_VALUE))}
    };
  }

  @Test(dataProvider = "retainedSettings")
  public void testRetainedSettingsSerialization(String strategy, long period,
      Map<String, String> expected) {
    RebalanceConfig config = new RebalanceConfig(new ZNRecord("resource"));
    config.setRebalanceStrategy(strategy);
    config.setRebalanceTimerPeriod(period);

    Assert.assertEquals(config.getRebalanceStrategy(), strategy);
    Assert.assertEquals(config.getRebalanceTimerPeriod(), period);
    Assert.assertEquals(config.getConfigsMap(), expected);
    ZNRecord serialized = new ZNRecord("resource");
    serialized.setSimpleFields(config.getConfigsMap());
    Assert.assertEquals(new RebalanceConfig(serialized).getConfigsMap(), expected);
  }

  @Test
  public void testRetiredPropertiesAndAccessorsAreRemoved() {
    Set<String> properties = Arrays.stream(RebalanceConfig.RebalanceConfigProperty.values())
        .map(Enum::name).collect(Collectors.toSet());
    for (String property :
        Arrays.asList("REBALANCE_DELAY", "REBALANCE_MODE", "REBALANCER_CLASS_NAME")) {
      Assert.assertFalse(properties.contains(property), property);
    }
    Set<String> methods = Arrays.stream(RebalanceConfig.class.getMethods())
        .map(Method::getName).collect(Collectors.toSet());
    for (String method : Arrays.asList("getRebalanceDelay", "setRebalanceDelay",
        "getRebalanceMode", "setRebalanceMode", "getRebalanceClassName", "setRebalanceClassName")) {
      Assert.assertFalse(methods.contains(method), method);
    }
  }

  @SuppressWarnings("deprecation")
  @Test
  public void testLegacyModeNamesRemainCompatible() {
    Assert.assertEquals(Arrays.stream(RebalanceConfig.RebalanceMode.values())
            .map(Enum::name).collect(Collectors.toList()),
        Arrays.asList("FULL_AUTO", "SEMI_AUTO", "CUSTOMIZED", "USER_DEFINED", "TASK", "NONE"));
    Assert.assertEquals(RebalanceConfig.RebalanceMode.valueOf("FULL_AUTO"),
        RebalanceConfig.RebalanceMode.FULL_AUTO);
  }
}
