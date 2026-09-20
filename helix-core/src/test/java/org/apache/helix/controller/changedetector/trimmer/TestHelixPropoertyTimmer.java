package org.apache.helix.controller.changedetector.trimmer;

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
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.helix.HelixConstants;
import org.apache.helix.HelixProperty;
import org.apache.helix.controller.changedetector.ResourceChangeDetector;
import org.apache.helix.controller.changedetector.trimmer.HelixPropertyTrimmer.FieldType;
import org.apache.helix.controller.dataproviders.ResourceControllerDataProvider;
import org.apache.helix.model.ClusterConfig;
import org.apache.helix.model.IdealState;
import org.apache.helix.model.InstanceConfig;
import org.apache.helix.model.ResourceConfig;
import org.mockito.Mockito;
import org.testng.Assert;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import static org.mockito.Mockito.when;


public class TestHelixPropoertyTimmer {
  private final String CLUSTER_NAME = "CLUSTER";
  private final String INSTANCE_NAME = "INSTANCE";
  private final String RESOURCE_NAME = "RESOURCE";
  private final String PARTITION_NAME = "DEFAULT_PARTITION";

  private final Set<HelixConstants.ChangeType> _changeTypes = new HashSet<>();
  private final Map<String, InstanceConfig> _instanceConfigMap = new HashMap<>();
  private final Map<String, IdealState> _idealStateMap = new HashMap<>();
  private final Map<String, ResourceConfig> _resourceConfigMap = new HashMap<>();
  private ClusterConfig _clusterConfig;
  private ResourceControllerDataProvider _dataProvider;

  @BeforeMethod
  public void beforeMethod() {
    _changeTypes.clear();
    _instanceConfigMap.clear();
    _idealStateMap.clear();
    _resourceConfigMap.clear();

    _changeTypes.add(HelixConstants.ChangeType.INSTANCE_CONFIG);
    _changeTypes.add(HelixConstants.ChangeType.IDEAL_STATE);
    _changeTypes.add(HelixConstants.ChangeType.RESOURCE_CONFIG);
    _changeTypes.add(HelixConstants.ChangeType.CLUSTER_CONFIG);

    InstanceConfig instanceConfig = new InstanceConfig(INSTANCE_NAME);
    instanceConfig.setInstanceEnabledForPartition(RESOURCE_NAME, PARTITION_NAME, false);
    fillKeyValues(instanceConfig);
    _instanceConfigMap.put(INSTANCE_NAME, instanceConfig);

    IdealState idealState = new IdealState(RESOURCE_NAME);
    idealState.setRebalanceMode(IdealState.RebalanceMode.FULL_AUTO);
    idealState.setPreferenceList(PARTITION_NAME, new ArrayList<>());
    idealState.setPartitionState(PARTITION_NAME, INSTANCE_NAME, "LEADER");
    fillKeyValues(idealState);
    _idealStateMap.put(RESOURCE_NAME, idealState);

    ResourceConfig resourceConfig = new ResourceConfig(RESOURCE_NAME);
    Map<String, List<String>> configPreferenceList = new HashMap<>();
    configPreferenceList.put(PARTITION_NAME, new ArrayList<>());
    resourceConfig.setPreferenceLists(configPreferenceList);
    fillKeyValues(resourceConfig);
    _resourceConfigMap.put(RESOURCE_NAME, resourceConfig);

    _clusterConfig = new ClusterConfig(CLUSTER_NAME);
    fillKeyValues(_clusterConfig);

    _dataProvider =
        getMockDataProvider(_changeTypes, _instanceConfigMap, _idealStateMap, _resourceConfigMap,
            _clusterConfig);
  }

  // Fill the testing helix property to ensure that we have at least one sample in every types of data.
  private void fillKeyValues(HelixProperty helixProperty) {
    helixProperty.getRecord().setSimpleField("MockFieldKey", "MockValue");
    helixProperty.getRecord()
        .setMapField("MockFieldKey", Collections.singletonMap("MockKey", "MockValue"));
    helixProperty.getRecord().setListField("MockFieldKey", Collections.singletonList("MockValue"));
  }

  private ResourceControllerDataProvider getMockDataProvider(
      Set<HelixConstants.ChangeType> changeTypes, Map<String, InstanceConfig> instanceConfigMap,
      Map<String, IdealState> idealStateMap, Map<String, ResourceConfig> resourceConfigMap,
      ClusterConfig clusterConfig) {
    ResourceControllerDataProvider dataProvider =
        Mockito.mock(ResourceControllerDataProvider.class);
    when(dataProvider.getRefreshedChangeTypes()).thenReturn(changeTypes);
    when(dataProvider.getAssignableInstanceConfigMap()).thenReturn(instanceConfigMap);
    when(dataProvider.getIdealStates()).thenReturn(idealStateMap);
    when(dataProvider.getResourceConfigMap()).thenReturn(resourceConfigMap);
    when(dataProvider.getClusterConfig()).thenReturn(clusterConfig);
    when(dataProvider.getAssignableLiveInstances()).thenReturn(Collections.emptyMap());
    return dataProvider;
  }

  @Test
  public void testDetectNonTrimmableFieldChanges() {
    // Fill mock data to initialize the detector
    ResourceChangeDetector detector = new ResourceChangeDetector(true);
    detector.updateSnapshots(_dataProvider);

    // Verify that all the non-trimmable field changes will be detected
    // 1. Cluster Config
    changeNonTrimmableValuesAndVerifyDetector(
        ClusterConfigTrimmer.getInstance().getNonTrimmableFields(_clusterConfig), _clusterConfig,
        HelixConstants.ChangeType.CLUSTER_CONFIG, detector, _dataProvider);
    // 2. Ideal States
    for (IdealState idealState : _idealStateMap.values()) {
      changeNonTrimmableValuesAndVerifyDetector(
          IdealStateTrimmer.getInstance().getNonTrimmableFields(idealState), idealState,
          HelixConstants.ChangeType.IDEAL_STATE, detector, _dataProvider);

      modifyListMapfieldKeysAndVerifyDetector(idealState, HelixConstants.ChangeType.IDEAL_STATE,
          detector, _dataProvider);

      // Additional test to ensure Ideal State map/list fields are detected correctly according to
      // the rebalance mode.
      // For the following test, we can only focus on the smaller scope defined by the following map.
      Map<FieldType, Set<String>> overwriteFieldMap = new HashMap<>();

      // SEMI_AUTO: List fields are non-trimmable
      idealState.setRebalanceMode(IdealState.RebalanceMode.SEMI_AUTO);
      // refresh the detector cache after modification to avoid unexpected change detected.
      detector.updateSnapshots(_dataProvider);
      overwriteFieldMap.put(FieldType.LIST_FIELD, Collections.singleton(PARTITION_NAME));
      changeNonTrimmableValuesAndVerifyDetector(overwriteFieldMap, idealState,
          HelixConstants.ChangeType.IDEAL_STATE, detector, _dataProvider);

      // CUSTOMZIED: Map fields are non-trimmable
      idealState.setRebalanceMode(IdealState.RebalanceMode.CUSTOMIZED);
      // refresh the detector cache after modification to avoid unexpected change detected.
      detector.updateSnapshots(_dataProvider);
      overwriteFieldMap.clear();
      overwriteFieldMap.put(FieldType.MAP_FIELD, Collections.singleton(PARTITION_NAME));
      changeNonTrimmableValuesAndVerifyDetector(overwriteFieldMap, idealState,
          HelixConstants.ChangeType.IDEAL_STATE, detector, _dataProvider);
    }
    // 3. Resource Config
    for (ResourceConfig resourceConfig : _resourceConfigMap.values()) {
      changeNonTrimmableValuesAndVerifyDetector(
          ResourceConfigTrimmer.getInstance().getNonTrimmableFields(resourceConfig), resourceConfig,
          HelixConstants.ChangeType.RESOURCE_CONFIG, detector, _dataProvider);

      modifyListMapfieldKeysAndVerifyDetector(resourceConfig,
          HelixConstants.ChangeType.RESOURCE_CONFIG, detector, _dataProvider);
    }
    // 4. Instance Config
    for (InstanceConfig instanceConfig : _instanceConfigMap.values()) {
      changeNonTrimmableValuesAndVerifyDetector(
          InstanceConfigTrimmer.getInstance().getNonTrimmableFields(instanceConfig), instanceConfig,
          HelixConstants.ChangeType.INSTANCE_CONFIG, detector, _dataProvider);

      modifyListMapfieldKeysAndVerifyDetector(instanceConfig,
          HelixConstants.ChangeType.INSTANCE_CONFIG, detector, _dataProvider);
    }
  }

  /**
   * Flipping WAGED instance tag isolation changes how the rebalancer reacts to a group it cannot
   * place, so the flag has to survive trimming and reach the change detector. Otherwise an operator
   * could turn it on during an incident and nothing would recalculate until some unrelated config
   * change happened to come along and trigger the next global rebalance.
   */
  @Test
  public void testWagedInstanceTagIsolationFlagIsNotTrimmed() {
    ResourceChangeDetector detector = new ResourceChangeDetector(true);
    detector.updateSnapshots(_dataProvider);

    _clusterConfig.setWagedInstanceTagIsolationEnabled(true);
    detector.updateSnapshots(_dataProvider);
    Assert.assertTrue(
        detector.getChangesByType(HelixConstants.ChangeType.CLUSTER_CONFIG).contains(CLUSTER_NAME),
        "Turning instance tag isolation on must be detected as a cluster config change");

    _clusterConfig.setWagedInstanceTagIsolationEnabled(false);
    detector.updateSnapshots(_dataProvider);
    Assert.assertTrue(
        detector.getChangesByType(HelixConstants.ChangeType.CLUSTER_CONFIG).contains(CLUSTER_NAME),
        "Turning instance tag isolation back off must be detected as a cluster config change");
  }

  /**
   * An explicit false reads exactly like an absent flag. Writing the default down on a cluster that
   * never set the flag, or removing an explicit false again, must not read as a cluster config
   * change, since that triggers a full baseline recalculation for nothing. Turning it on and back
   * off must still be detected.
   */
  @Test
  public void testWagedInstanceTagIsolationFlagWrittenAsItsDefaultIsNotAChange() {
    String flag = ClusterConfig.ClusterConfigProperty.WAGED_INSTANCE_TAG_ISOLATION_ENABLED.name();
    ResourceChangeDetector detector = new ResourceChangeDetector(true);
    detector.updateSnapshots(_dataProvider);

    _clusterConfig.setWagedInstanceTagIsolationEnabled(false);
    detector.updateSnapshots(_dataProvider);
    Assert.assertFalse(
        detector.getChangesByType(HelixConstants.ChangeType.CLUSTER_CONFIG).contains(CLUSTER_NAME),
        "Writing the flag down as false where it was absent leaves its effective value unchanged");

    _clusterConfig.getRecord().setSimpleField(flag, "FALSE");
    detector.updateSnapshots(_dataProvider);
    Assert.assertFalse(
        detector.getChangesByType(HelixConstants.ChangeType.CLUSTER_CONFIG).contains(CLUSTER_NAME),
        "The flag is read ignoring case, so the default in another case is still the default");

    _clusterConfig.setWagedInstanceTagIsolationEnabled(true);
    detector.updateSnapshots(_dataProvider);
    Assert.assertTrue(
        detector.getChangesByType(HelixConstants.ChangeType.CLUSTER_CONFIG).contains(CLUSTER_NAME),
        "Turning instance tag isolation on from an explicit false must be detected");

    _clusterConfig.setWagedInstanceTagIsolationEnabled(false);
    detector.updateSnapshots(_dataProvider);
    Assert.assertTrue(
        detector.getChangesByType(HelixConstants.ChangeType.CLUSTER_CONFIG).contains(CLUSTER_NAME),
        "Turning instance tag isolation back off must be detected");

    _clusterConfig.getRecord().getSimpleFields().remove(flag);
    detector.updateSnapshots(_dataProvider);
    Assert.assertFalse(
        detector.getChangesByType(HelixConstants.ChangeType.CLUSTER_CONFIG).contains(CLUSTER_NAME),
        "Removing an explicit false leaves the effective value unchanged too");

    _clusterConfig.setTopologyAwareEnabled(!_clusterConfig.isTopologyAwareEnabled());
    detector.updateSnapshots(_dataProvider);
    Assert.assertTrue(
        detector.getChangesByType(HelixConstants.ChangeType.CLUSTER_CONFIG).contains(CLUSTER_NAME),
        "A change to any other topology field must still be detected");
  }

  /**
   * The change detector compares the flag by the value the rebalancer reads it as. Any value that
   * reads as false, boolean or not, compares equal to an absent flag, and true compares equal in
   * any case. Rewriting the flag without changing what it reads as must not start a full baseline
   * recalculation, and every write that does turn isolation on or off must still be detected.
   */
  @Test
  public void testWagedInstanceTagIsolationFlagIsComparedByTheValueItReadsAs() {
    String flag = ClusterConfig.ClusterConfigProperty.WAGED_INSTANCE_TAG_ISOLATION_ENABLED.name();
    ResourceChangeDetector detector = new ResourceChangeDetector(true);
    detector.updateSnapshots(_dataProvider);

    for (String readsAsFalse : new String[] {"0", "1", "no", "yes", "", "enabled"}) {
      _clusterConfig.getRecord().setSimpleField(flag, readsAsFalse);
      Assert.assertFalse(_clusterConfig.isWagedInstanceTagIsolationEnabled());
      assertClusterConfigChange(detector, false,
          "'" + readsAsFalse + "' reads as false, the same as the value before it");
    }

    _clusterConfig.getRecord().setSimpleField(flag, "TRUE");
    Assert.assertTrue(_clusterConfig.isWagedInstanceTagIsolationEnabled());
    assertClusterConfigChange(detector, true,
        "Turning isolation on in upper case must be detected");

    _clusterConfig.getRecord().setSimpleField(flag, "true");
    assertClusterConfigChange(detector, false,
        "Rewriting true in another case leaves isolation on");

    _clusterConfig.getRecord().setSimpleField(flag, "0");
    Assert.assertFalse(_clusterConfig.isWagedInstanceTagIsolationEnabled());
    assertClusterConfigChange(detector, true,
        "A value that reads as false turns isolation off, which must be detected");

    _clusterConfig.getRecord().getSimpleFields().remove(flag);
    assertClusterConfigChange(detector, false,
        "Removing a value that reads as false leaves isolation off");
  }

  private void assertClusterConfigChange(ResourceChangeDetector detector, boolean expected,
      String message) {
    detector.updateSnapshots(_dataProvider);
    Assert.assertEquals(
        detector.getChangesByType(HelixConstants.ChangeType.CLUSTER_CONFIG).contains(CLUSTER_NAME),
        expected, message);
  }

  @Test
  public void testIgnoreTrimmableFieldChanges() {
    // Fill mock data to initialize the detector
    ResourceChangeDetector detector = new ResourceChangeDetector(true);
    detector.updateSnapshots(_dataProvider);

    // Verify that all the trimmable field changes will not be detected
    // 1. Cluster Config
    changeTrimmableValuesAndVerifyDetector(FieldType.values(), _clusterConfig, detector,
        _dataProvider);
    // 2. Ideal States
    for (IdealState idealState : _idealStateMap.values()) {
      changeTrimmableValuesAndVerifyDetector(FieldType.values(), idealState, detector,
          _dataProvider);
      // Additional test to ensure Ideal State map/list fields are detected correctly according to
      // the rebalance mode.

      // SEMI_AUTO: List fields are non-trimmable
      idealState.setRebalanceMode(IdealState.RebalanceMode.SEMI_AUTO);
      // refresh the detector cache after modification to avoid unexpected change detected.
      detector.updateSnapshots(_dataProvider);
      changeTrimmableValuesAndVerifyDetector(
          new FieldType[]{FieldType.SIMPLE_FIELD, FieldType.MAP_FIELD}, idealState, detector,
          _dataProvider);

      // CUSTOMZIED: List and Map fields are non-trimmable
      idealState.setRebalanceMode(IdealState.RebalanceMode.CUSTOMIZED);
      // refresh the detector cache after modification to avoid unexpected change detected.
      detector.updateSnapshots(_dataProvider);
      changeTrimmableValuesAndVerifyDetector(
          new FieldType[]{FieldType.SIMPLE_FIELD, FieldType.LIST_FIELD}, idealState, detector,
          _dataProvider);
    }
    // 3. Resource Config
    for (ResourceConfig resourceConfig : _resourceConfigMap.values()) {
      // Preference lists in the list fields are non-trimmable
      changeTrimmableValuesAndVerifyDetector(
          new FieldType[]{FieldType.SIMPLE_FIELD, FieldType.MAP_FIELD}, resourceConfig, detector,
          _dataProvider);
    }
    // 4. Instance Config
    for (InstanceConfig instanceConfig : _instanceConfigMap.values()) {
      changeTrimmableValuesAndVerifyDetector(FieldType.values(), instanceConfig, detector,
          _dataProvider);
    }
  }

  @Test
  public void testLegacyGroupRoutingFieldsAreTrimmable() {
    IdealState idealState = _idealStateMap.get(RESOURCE_NAME);
    ResourceConfig resourceConfig = _resourceConfigMap.get(RESOURCE_NAME);
    idealState.setInstanceGroupTag("placementTag");
    resourceConfig.getRecord().setSimpleField("INSTANCE_GROUP_TAG", "placementTag");
    for (String legacyField :
        new String[]{"RESOURCE_GROUP_NAME", "RESOURCE_TYPE", "GROUP_ROUTING_ENABLED"}) {
      idealState.getRecord().setSimpleField(legacyField, "legacyValue");
      resourceConfig.getRecord().setSimpleField(legacyField, "legacyValue");
    }

    IdealState trimmedIdealState = IdealStateTrimmer.getInstance().trimProperty(idealState);
    ResourceConfig trimmedResourceConfig =
        ResourceConfigTrimmer.getInstance().trimProperty(resourceConfig);
    for (String legacyField :
        new String[]{"RESOURCE_GROUP_NAME", "RESOURCE_TYPE", "GROUP_ROUTING_ENABLED"}) {
      Assert.assertFalse(trimmedIdealState.getRecord().getSimpleFields().containsKey(legacyField));
      Assert.assertFalse(trimmedResourceConfig.getRecord().getSimpleFields().containsKey(legacyField));
      Assert.assertEquals(idealState.getRecord().getSimpleField(legacyField), "legacyValue");
      Assert.assertEquals(resourceConfig.getRecord().getSimpleField(legacyField), "legacyValue");
    }
    Assert.assertEquals(trimmedIdealState.getInstanceGroupTag(), "placementTag");
    Assert.assertEquals(trimmedResourceConfig.getInstanceGroupTag(), "placementTag");
  }

  private void modifyListMapfieldKeysAndVerifyDetector(HelixProperty helixProperty,
      HelixConstants.ChangeType expectedChangeType, ResourceChangeDetector detector,
      ResourceControllerDataProvider dataProvider) {
    helixProperty.getRecord()
        .setListField(helixProperty.getId() + "NewListField", Collections.singletonList("foobar"));
    helixProperty.getRecord()
        .setMapField(helixProperty.getId() + "NewMapField", Collections.singletonMap("foo", "bar"));
    detector.updateSnapshots(dataProvider);
    for (HelixConstants.ChangeType changeType : HelixConstants.ChangeType.values()) {
      Assert.assertEquals(detector.getChangesByType(changeType).size(),
          changeType == expectedChangeType ? 1 : 0,
          String.format("Any key changes in the List or Map fields shall be detected!"));
    }
  }

  private void changeNonTrimmableValuesAndVerifyDetector(
      Map<FieldType, Set<String>> nonTrimmableFieldMap, HelixProperty helixProperty,
      HelixConstants.ChangeType expectedChangeType, ResourceChangeDetector detector,
      ResourceControllerDataProvider dataProvider) {
    for (FieldType type : nonTrimmableFieldMap.keySet()) {
      for (String fieldKey : nonTrimmableFieldMap.get(type)) {
        switch (type) {
          case LIST_FIELD:
            helixProperty.getRecord().setListField(fieldKey, Collections.singletonList("foobar"));
            break;
          case MAP_FIELD:
            helixProperty.getRecord().setMapField(fieldKey, Collections.singletonMap("foo", "bar"));
            break;
          case SIMPLE_FIELD:
            helixProperty.getRecord()
                .setSimpleField(fieldKey, changedSimpleFieldValue(helixProperty, fieldKey));
            break;
          default:
            Assert.fail("Unknown field type " + type.name());
        }
        detector.updateSnapshots(dataProvider);
        for (HelixConstants.ChangeType changeType : HelixConstants.ChangeType.values()) {
          Assert.assertEquals(detector.getAdditionsByType(changeType).size(), 0,
              String.format("There should not be any additional change detected!"));
          Assert.assertEquals(detector.getRemovalsByType(changeType).size(), 0,
              String.format("There should not be any removal change detected!"));
          Assert.assertEquals(detector.getChangesByType(changeType).size(),
              changeType == expectedChangeType ? 1 : 0,
              String.format("The detected change of %s is not as expected.", fieldKey));
        }
      }
    }
  }

  // The isolation flag is compared by the value it reads as, so a change has to flip that value. An
  // arbitrary string reads as false, exactly like the absent flag, and is rightly not a change.
  private static String changedSimpleFieldValue(HelixProperty helixProperty, String fieldKey) {
    if (helixProperty instanceof ClusterConfig
        && ClusterConfig.ClusterConfigProperty.WAGED_INSTANCE_TAG_ISOLATION_ENABLED.name()
        .equals(fieldKey)) {
      return Boolean.toString(
          !((ClusterConfig) helixProperty).isWagedInstanceTagIsolationEnabled());
    }
    return "foobar";
  }

  private void changeTrimmableValuesAndVerifyDetector(FieldType[] trimmableFieldTypes,
      HelixProperty helixProperty, ResourceChangeDetector detector,
      ResourceControllerDataProvider dataProvider) {
    for (FieldType type : trimmableFieldTypes) {
      switch (type) {
        case LIST_FIELD:
          // Modify value if key exists.
          // Note if adding new keys, then the change will be detected regardless of the content.
          helixProperty.getRecord().getListFields().keySet().stream().forEach(key -> {
            helixProperty.getRecord().setListField(key,
                Collections.singletonList("new-foobar" + System.currentTimeMillis()));
          });
          break;
        case MAP_FIELD:
          // Modify value if key exists.
          // Note if adding new keys, then the change will be detected regardless of the content.
          helixProperty.getRecord().getMapFields().keySet().stream().forEach(key -> {
            helixProperty.getRecord().setMapField(key,
                Collections.singletonMap("new-foo", "bar" + System.currentTimeMillis()));
          });
          break;
        case SIMPLE_FIELD:
          helixProperty.getRecord()
              .setSimpleField("TrimmableSimpleField", "foobar" + System.currentTimeMillis());
          break;
        default:
          Assert.fail("Unknown field type " + type.name());
      }
      detector.updateSnapshots(dataProvider);
      for (HelixConstants.ChangeType changeType : HelixConstants.ChangeType.values()) {
        Assert.assertEquals(
            detector.getAdditionsByType(changeType).size() + detector.getRemovalsByType(changeType)
                .size() + detector.getChangesByType(changeType).size(), 0, String.format(
                "There should not be any change detected for the trimmable field changes!"));
      }
    }
  }
}
