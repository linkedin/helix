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
    Map<String, String> expected = Collections.singletonMap("REBALANCE_STRATEGY", "custom.Strategy");
    if (legacyPeriod != null) {
      record.setSimpleField("REBALANCE_TIMER_PERIOD", legacyPeriod);
    }
    ZNRecord originalRecord = new ZNRecord(record);

    RebalanceConfig config = new RebalanceConfig(record);
    Assert.assertEquals(config.getConfigsMap(), expected);
    Assert.assertEquals(config.getRebalanceStrategy(), "custom.Strategy");

    ResourceConfig resourceConfig =
        new ResourceConfig.Builder("resource").setRebalanceConfig(config).build();
    Assert.assertFalse(resourceConfig.getRecord().getSimpleFields()
        .containsKey("REBALANCE_TIMER_PERIOD"));
    Assert.assertEquals(resourceConfig.getRebalanceConfig().getConfigsMap(), expected);
    Assert.assertEquals(record, originalRecord);
  }

  @Test
  public void testDefaultsDoNotSynthesizeRebalanceMode() {
    RebalanceConfig config = new RebalanceConfig(new ZNRecord("resource"));

    Assert.assertNull(config.getRebalanceStrategy());
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
    Assert.assertEquals(config.getConfigsMap(),
        Collections.singletonMap("REBALANCE_STRATEGY", "retained.Strategy"));
    Assert.assertEquals(record, original);
  }

  @DataProvider
  public Object[][] retainedSettings() {
    return new Object[][] {
        {null, Collections.emptyMap()},
        {"", Collections.singletonMap("REBALANCE_STRATEGY", "")},
        {"retained.Strategy", Collections.singletonMap("REBALANCE_STRATEGY", "retained.Strategy")}
    };
  }

  @Test(dataProvider = "retainedSettings")
  public void testRetainedSettingsSerialization(String strategy,
      Map<String, String> expected) {
    RebalanceConfig config = new RebalanceConfig(new ZNRecord("resource"));
    config.setRebalanceStrategy(strategy);

    Assert.assertEquals(config.getRebalanceStrategy(), strategy);
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
        Arrays.asList("REBALANCE_DELAY", "REBALANCE_MODE", "REBALANCER_CLASS_NAME",
            "REBALANCE_TIMER_PERIOD")) {
      Assert.assertFalse(properties.contains(property), property);
    }
    Set<String> methods = Arrays.stream(RebalanceConfig.class.getMethods())
        .map(Method::getName).collect(Collectors.toSet());
    for (String method : Arrays.asList("getRebalanceDelay", "setRebalanceDelay",
        "getRebalanceMode", "setRebalanceMode", "getRebalanceClassName", "setRebalanceClassName",
        "getRebalanceTimerPeriod", "setRebalanceTimerPeriod")) {
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
