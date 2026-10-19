package org.apache.helix.model;

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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.helix.TestHelper;
import org.apache.helix.model.IdealState.RebalanceMode;
import org.apache.helix.zookeeper.datamodel.ZNRecord;
import org.testng.Assert;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

@SuppressWarnings("deprecation")
public class TestIdealState {
  @Test
  public void testDelayRebalanceEnabledByDefault() {
    IdealState idealState = new IdealState("resource");
    Assert.assertTrue(idealState.isDelayRebalanceEnabled());
    Assert.assertFalse(idealState.getRecord().getSimpleFields()
        .containsKey("DELAY_REBALANCE_ENABLED"));
  }

  @DataProvider(name = "delayRebalanceEnabled")
  public Object[][] delayRebalanceEnabled() {
    return new Object[][]{{true}, {false}};
  }

  @Test(dataProvider = "delayRebalanceEnabled")
  public void testDelayRebalanceEnabledLegacyRecordAndRoundTrip(boolean enabled) {
    ZNRecord legacyRecord = new ZNRecord("resource");
    legacyRecord.setSimpleField("DELAY_REBALANCE_ENABLED", Boolean.toString(enabled));
    IdealState idealState = new IdealState(legacyRecord);
    Assert.assertEquals(idealState.isDelayRebalanceEnabled(), enabled);

    idealState.setDelayRebalanceEnabled(!enabled);
    Assert.assertEquals(idealState.getRecord().getSimpleField("DELAY_REBALANCE_ENABLED"),
        Boolean.toString(!enabled));
    Assert.assertEquals(new IdealState(idealState.getRecord()).isDelayRebalanceEnabled(), !enabled);
    Assert.assertEquals(legacyRecord.getSimpleField("DELAY_REBALANCE_ENABLED"),
        Boolean.toString(enabled));
  }

  @Test
  public void testRebalanceSettingsRetainDefaultsAndRoundTrip() {
    IdealState idealState = new IdealState("resource");
    Assert.assertEquals(idealState.getRebalanceDelay(), -1L);
    Assert.assertEquals(idealState.getRebalanceMode(), RebalanceMode.SEMI_AUTO);
    Assert.assertNull(idealState.getRebalancerClassName());

    idealState.setRebalanceDelay(1000L);
    idealState.setRebalanceMode(RebalanceMode.USER_DEFINED);
    idealState.setRebalancerClassName("active.Rebalancer");
    Assert.assertEquals(idealState.getRecord().getSimpleField("REBALANCE_DELAY"), "1000");
    Assert.assertEquals(idealState.getRecord().getSimpleField("REBALANCE_MODE"), "USER_DEFINED");
    Assert.assertEquals(idealState.getRecord().getSimpleField("REBALANCER_CLASS_NAME"),
        "active.Rebalancer");

    IdealState restored = new IdealState(new ZNRecord(idealState.getRecord()));
    Assert.assertEquals(restored.getRebalanceDelay(), 1000L);
    Assert.assertEquals(restored.getRebalanceMode(), RebalanceMode.USER_DEFINED);
    Assert.assertEquals(restored.getRebalancerClassName(), "active.Rebalancer");
  }

  @Test
  public void testGetInstanceSet() {
    String className = TestHelper.getTestClassName();
    String methodName = TestHelper.getTestMethodName();
    String testName = className + "_" + methodName;
    System.out.println("START " + testName + " at " + new Date(System.currentTimeMillis()));

    IdealState idealState = new IdealState("idealState");
    idealState.getRecord().setListField("TestDB_0", Arrays.asList("node_1", "node_2"));
    Map<String, String> instanceState = new HashMap<String, String>();
    instanceState.put("node_3", "MASTER");
    instanceState.put("node_4", "SLAVE");
    idealState.getRecord().setMapField("TestDB_1", instanceState);

    // test SEMI_AUTO mode
    idealState.setRebalanceMode(RebalanceMode.SEMI_AUTO);
    Set<String> instances = idealState.getInstanceSet("TestDB_0");
    // System.out.println("instances: " + instances);
    Assert.assertEquals(instances.size(), 2, "Should contain node_1 and node_2");
    Assert.assertTrue(instances.contains("node_1"), "Should contain node_1 and node_2");
    Assert.assertTrue(instances.contains("node_2"), "Should contain node_1 and node_2");

    instances = idealState.getInstanceSet("TestDB_nonExist_auto");
    Assert.assertEquals(instances, Collections.emptySet(), "Should get empty set");

    // test CUSTOMIZED mode
    idealState.setRebalanceMode(RebalanceMode.CUSTOMIZED);
    instances = idealState.getInstanceSet("TestDB_1");
    // System.out.println("instances: " + instances);
    Assert.assertEquals(instances.size(), 2, "Should contain node_3 and node_4");
    Assert.assertTrue(instances.contains("node_3"), "Should contain node_3 and node_4");
    Assert.assertTrue(instances.contains("node_4"), "Should contain node_3 and node_4");

    instances = idealState.getInstanceSet("TestDB_nonExist_custom");
    Assert.assertEquals(instances, Collections.emptySet(), "Should get empty set");

    System.out.println("END " + testName + " at " + new Date(System.currentTimeMillis()));
  }

  @Test
  public void testReplicas() {
    IdealState idealState = new IdealState("test-db");
    idealState.setRebalanceMode(RebalanceMode.SEMI_AUTO);
    idealState.setNumPartitions(4);
    idealState.setStateModelDefRef("MasterSlave");

    idealState.setReplicas("" + 2);

    List<String> preferenceList = new ArrayList<String>();
    preferenceList.add("node_0");
    idealState.getRecord().setListField("test-db_0", preferenceList);
    Assert.assertFalse(idealState.isValid(),
        "should fail since replicas not equals to preference-list size");

    preferenceList.add("node_1");
    idealState.getRecord().setListField("test-db_0", preferenceList);
    Assert.assertTrue(idealState.isValid(),
        "should pass since replicas equals to preference-list size");
  }

  @DataProvider
  public Object[][] rebalanceModes() {
    return Arrays.stream(RebalanceMode.values())
        .map(mode -> new Object[]{mode, mode == RebalanceMode.NONE ? RebalanceMode.SEMI_AUTO : mode})
        .toArray(Object[][]::new);
  }

  @Test
  public void testLegacyModeApiRemoved() {
    Assert.assertFalse(Arrays.stream(IdealState.IdealStateProperty.values())
        .anyMatch(property -> property.name().equals("IDEAL_STATE_MODE")));
    Assert.assertFalse(Arrays.stream(IdealState.class.getDeclaredClasses())
        .anyMatch(type -> type.getSimpleName().equals("IdealStateModeProperty")));
    Assert.assertFalse(Arrays.stream(IdealState.class.getMethods())
        .anyMatch(method -> method.getName().equals("getIdealStateMode")
            || method.getName().equals("setIdealStateMode")));
  }

  @Test(dataProvider = "rebalanceModes")
  public void testModernModeRoundTripWithoutLegacyField(RebalanceMode mode,
      RebalanceMode expectedMode) {
    IdealState idealState = new IdealState("resource");
    idealState.setRebalanceMode(mode);
    Assert.assertEquals(idealState.getRecord().getSimpleFields(),
        Collections.singletonMap("REBALANCE_MODE", mode.name()));
    IdealState restored = new IdealState(new ZNRecord(idealState.getRecord()));
    Assert.assertEquals(restored.getRebalanceMode(), expectedMode);
    Assert.assertEquals(restored.getRecord().getSimpleFields(),
        Collections.singletonMap("REBALANCE_MODE", mode.name()));
    Assert.assertEquals(idealState.rebalanceModeFromString(mode.name(), RebalanceMode.SEMI_AUTO),
        mode);
  }

  @DataProvider
  public Object[][] modernAndLegacyModes() {
    List<Object[]> cases = new ArrayList<>();
    for (String legacy : new String[]{null, "", "AUTO", "AUTO_REBALANCE", "CUSTOMIZED", "invalid"}) {
      for (RebalanceMode modern : RebalanceMode.values()) {
        if (modern != RebalanceMode.NONE) {
          cases.add(new Object[]{modern.name(), legacy, modern});
        }
      }
      for (String modern : new String[]{null, "", "invalid", "AUTO", "AUTO_REBALANCE", "NONE"}) {
        cases.add(new Object[]{modern, legacy, RebalanceMode.SEMI_AUTO});
      }
    }
    return cases.toArray(new Object[0][]);
  }

  @Test(dataProvider = "modernAndLegacyModes")
  public void testLegacyModeIsOpaqueAndReadsDoNotMutate(String modern, String legacy,
      RebalanceMode expectedMode) {
    ZNRecord record = new ZNRecord("resource");
    if (modern != null) {
      record.setSimpleField("REBALANCE_MODE", modern);
    }
    if (legacy != null) {
      record.setSimpleField("IDEAL_STATE_MODE", legacy);
    }
    record.setSimpleField("applicationSetting", "keep");
    record.setListField("resource_0", Collections.singletonList("node"));
    record.setMapField("resource_0", Collections.singletonMap("node", "ONLINE"));
    ZNRecord original = new ZNRecord(record);
    IdealState idealState = new IdealState(record);

    Assert.assertEquals(idealState.getRebalanceMode(), expectedMode);
    Assert.assertEquals(idealState.getRebalanceMode(), expectedMode);
    Assert.assertEquals(idealState.getRecord(), original);
    Assert.assertEquals(record, original);

    idealState.setRebalanceMode(RebalanceMode.USER_DEFINED);
    ZNRecord expected = new ZNRecord(original);
    expected.setSimpleField("REBALANCE_MODE", "USER_DEFINED");
    Assert.assertEquals(idealState.getRecord(), expected);
    Assert.assertEquals(record, original);
  }

  @DataProvider
  public Object[][] invalidRebalanceModes() {
    return new Object[][]{{null}, {""}, {"invalid"}, {"AUTO"}, {"AUTO_REBALANCE"}, {"full_auto"}};
  }

  @Test(dataProvider = "invalidRebalanceModes")
  public void testModeParserUsesCallerDefaultWithoutLegacyAliases(String mode) {
    IdealState idealState = new IdealState("resource");
    Assert.assertEquals(idealState.rebalanceModeFromString(mode, RebalanceMode.CUSTOMIZED),
        RebalanceMode.CUSTOMIZED);
    Assert.assertNull(idealState.rebalanceModeFromString(mode, null));
    Assert.assertTrue(idealState.getRecord().getSimpleFields().isEmpty());
  }
}
