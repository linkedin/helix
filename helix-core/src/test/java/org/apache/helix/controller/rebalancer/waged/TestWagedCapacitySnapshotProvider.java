package org.apache.helix.controller.rebalancer.waged;

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

import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.apache.helix.AccessOption;
import org.apache.helix.HelixDataAccessor;
import org.apache.helix.ZNRecord;
import org.apache.helix.common.ZkTestBase;
import org.apache.helix.constants.InstanceConstants;
import org.apache.helix.manager.zk.ZKHelixDataAccessor;
import org.apache.helix.model.BuiltInStateModelDefinitions;
import org.apache.helix.model.ClusterConfig;
import org.apache.helix.model.ExternalView;
import org.apache.helix.model.IdealState;
import org.apache.helix.model.InstanceConfig;
import org.apache.helix.model.LiveInstance;
import org.testng.Assert;
import org.testng.annotations.Test;

/**
 * Tests {@link WagedCapacitySnapshotProvider} against znodes written directly, with no controller
 * or participants, so each ExternalView is exactly what the test authored.
 */
public class TestWagedCapacitySnapshotProvider extends ZkTestBase {
  private static final String CAP = "CU";

  private final String CLASS_NAME = getShortClassName();

  @Test
  public void testStateFilteringAndTopStateCharges() {
    String cluster = newCluster("stateFilter");
    setCapacity(cluster, Collections.singletonList(CAP), Collections.singletonMap(CAP, 100),
        Collections.singletonMap(CAP, 1));
    for (String n : Arrays.asList("n0", "n1", "n2", "n3", "n4")) {
      addInstance(cluster, n, null);
      addLive(cluster, n);
    }
    writeIdealState(cluster, "DB", BuiltInStateModelDefinitions.MasterSlave.name(), true);
    writeExternalView(cluster, "DB", stateMap("n0", "MASTER", "n1", "SLAVE", "n2", "ERROR", "n3",
        "OFFLINE", "n4", "DROPPED"));

    WagedCapacitySnapshot snapshot = snapshotOf(cluster);
    WagedCapacitySnapshot.InstanceCapacity n0 = instance(snapshot, "n0");
    Assert.assertEquals(n0.capacityStatus, WagedCapacitySnapshot.CAPACITY_KNOWN);
    Assert.assertEquals(n0.wagedTopStatePartitions, 1);
    Assert.assertEquals(n0.used.get(CAP).longValue(), 1L);
    Assert.assertEquals(n0.topStateUsed.get(CAP).longValue(), 1L);
    WagedCapacitySnapshot.InstanceCapacity n1 = instance(snapshot, "n1");
    Assert.assertEquals(n1.used.get(CAP).longValue(), 1L);
    Assert.assertEquals(n1.topStateUsed.get(CAP).longValue(), 0L);
    // ERROR and OFFLINE replicas still occupy the node, only ERROR is counted as an error.
    Assert.assertEquals(instance(snapshot, "n2").errorReplicas, 1);
    Assert.assertEquals(instance(snapshot, "n2").used.get(CAP).longValue(), 1L);
    Assert.assertEquals(instance(snapshot, "n3").used.get(CAP).longValue(), 1L);
    Assert.assertEquals(instance(snapshot, "n3").errorReplicas, 0);
    // DROPPED is a tombstone.
    WagedCapacitySnapshot.InstanceCapacity n4 = instance(snapshot, "n4");
    Assert.assertEquals(n4.wagedPartitions, 0);
    Assert.assertTrue(n4.resources.isEmpty());
    Assert.assertEquals(snapshot.summary.totalErrorReplicas, 1);
    Assert.assertEquals(snapshot.wagedResourceCount, 1);
    deleteCluster(cluster);
  }

  @Test
  public void testUnknownIsNullNotZero() {
    String cluster = newCluster("unknown");
    // No instance capacity anywhere, so max is unknown while usage still resolves.
    setCapacity(cluster, Collections.singletonList(CAP), null, Collections.singletonMap(CAP, 1));
    addInstance(cluster, "n0", null);
    addLive(cluster, "n0");
    writeIdealState(cluster, "WagedDB", BuiltInStateModelDefinitions.MasterSlave.name(), true);
    writeExternalView(cluster, "WagedDB", stateMap("n0", "MASTER"));

    WagedCapacitySnapshot snapshot = snapshotOf(cluster);
    WagedCapacitySnapshot.InstanceCapacity n0 = instance(snapshot, "n0");
    Assert.assertEquals(n0.capacityStatus, WagedCapacitySnapshot.CAPACITY_UNKNOWN);
    Assert.assertNull(n0.max);
    Assert.assertEquals(n0.used.get(CAP).longValue(), 1L);
    Assert.assertEquals(snapshot.summary.instancesWithUnknownCapacity, 1);

    // Capacity resolves but the partition weight does not: used must be null, not 0.
    setCapacity(cluster, Collections.singletonList(CAP), Collections.singletonMap(CAP, 100), null);
    n0 = instance(snapshotOf(cluster), "n0");
    Assert.assertEquals(n0.max.get(CAP).longValue(), 100L);
    Assert.assertNull(n0.used);
    Assert.assertNull(n0.topStateUsed);
    Assert.assertNull(n0.resources.get(0).used);
    deleteCluster(cluster);
  }

  @Test
  public void testUnresolvableTopStateKeepsCapacityKnown() {
    String cluster = newCluster("topState");
    setCapacity(cluster, Collections.singletonList(CAP), Collections.singletonMap(CAP, 100),
        Collections.singletonMap(CAP, 1));
    addInstance(cluster, "n0", null);
    addLive(cluster, "n0");
    writeIdealState(cluster, "DB", "NoSuchStateModel", true);
    writeExternalView(cluster, "DB", stateMap("n0", "ONLINE"));

    WagedCapacitySnapshot.InstanceCapacity n0 = instance(snapshotOf(cluster), "n0");
    Assert.assertEquals(n0.capacityStatus, WagedCapacitySnapshot.CAPACITY_KNOWN);
    Assert.assertEquals(n0.used.get(CAP).longValue(), 1L);
    Assert.assertNull(n0.topStateUsed);
    deleteCluster(cluster);
  }

  @Test
  public void testNoCapacityModelAndUnsetMaxPartitions() {
    String cluster = newCluster("noModel");
    addInstance(cluster, "n0", null);
    addLive(cluster, "n0");
    writeIdealState(cluster, "Classic", BuiltInStateModelDefinitions.OnlineOffline.name(), false);
    writeExternalView(cluster, "Classic", stateMap("n0", "ONLINE", "n9", "ONLINE"));

    WagedCapacitySnapshot snapshot = snapshotOf(cluster);
    WagedCapacitySnapshot.InstanceCapacity n0 = instance(snapshot, "n0");
    Assert.assertEquals(n0.capacityStatus, WagedCapacitySnapshot.CAPACITY_NOT_APPLICABLE);
    Assert.assertNull(n0.max);
    Assert.assertNull(n0.used);
    Assert.assertEquals(n0.nonWagedPartitions, 1);
    Assert.assertEquals(n0.nonWagedTopStatePartitions, 1);
    Assert.assertEquals(snapshot.nonWagedResourceCount, 1);
    Assert.assertNull(snapshot.maxPartitionsPerInstance);
    // A host that is only in the ExternalView is surfaced and flagged.
    WagedCapacitySnapshot.InstanceCapacity n9 = instance(snapshot, "n9");
    Assert.assertFalse(n9.registered);
    Assert.assertEquals(snapshot.summary.unregisteredEvHosts, 1);
    deleteCluster(cluster);
  }

  @Test
  public void testEvacuatingInstanceIsNotEnabled() {
    String cluster = newCluster("evacuate");
    HelixDataAccessor acc = new ZKHelixDataAccessor(cluster, _baseAccessor);
    InstanceConfig evacuating = new InstanceConfig("n1");
    evacuating.setInstanceOperation(InstanceConstants.InstanceOperation.EVACUATE);
    evacuating.setInstanceEnabled(true);
    acc.setProperty(acc.keyBuilder().instanceConfig("n1"), evacuating);
    addLive(cluster, "n1");

    WagedCapacitySnapshot snapshot = snapshotOf(cluster);
    WagedCapacitySnapshot.InstanceCapacity n1 = instance(snapshot, "n1");
    Assert.assertTrue(n1.live);
    Assert.assertFalse(n1.enabled);
    Assert.assertEquals(n1.operation, InstanceConstants.InstanceOperation.EVACUATE.name());
    Assert.assertEquals(snapshot.summary.enabledInstances, 0);
    deleteCluster(cluster);
  }

  @Test
  public void testInstanceConfigWithoutIdIsSkipped() {
    String cluster = newCluster("noId");
    addInstance(cluster, "n0", null);
    // A config znode whose record has no id, as a bad config write can leave behind.
    _baseAccessor.create("/" + cluster + "/CONFIGS/PARTICIPANT/orphan",
        new ZNRecord((String) null), AccessOption.PERSISTENT);

    WagedCapacitySnapshot snapshot = snapshotOf(cluster);
    Assert.assertEquals(snapshot.instances.size(), 1);
    Assert.assertEquals(snapshot.instances.get(0).name, "n0");
    deleteCluster(cluster);
  }

  private String newCluster(String suffix) {
    String cluster = CLUSTER_PREFIX + "_" + CLASS_NAME + "_" + suffix;
    _gSetupTool.addCluster(cluster, true);
    return cluster;
  }

  private WagedCapacitySnapshot snapshotOf(String cluster) {
    return new WagedCapacitySnapshotProvider().computeSnapshot(cluster,
        new ZKHelixDataAccessor(cluster, _baseAccessor));
  }

  private void setCapacity(String cluster, List<String> keys,
      Map<String, Integer> defaultCapacity, Map<String, Integer> defaultWeight) {
    HelixDataAccessor acc = new ZKHelixDataAccessor(cluster, _baseAccessor);
    ClusterConfig clusterConfig = acc.getProperty(acc.keyBuilder().clusterConfig());
    clusterConfig.setInstanceCapacityKeys(keys);
    clusterConfig.setDefaultInstanceCapacityMap(
        defaultCapacity == null ? Collections.emptyMap() : defaultCapacity);
    clusterConfig.setDefaultPartitionWeightMap(
        defaultWeight == null ? Collections.emptyMap() : defaultWeight);
    acc.setProperty(acc.keyBuilder().clusterConfig(), clusterConfig);
  }

  private void addInstance(String cluster, String name, Map<String, Integer> capacity) {
    HelixDataAccessor acc = new ZKHelixDataAccessor(cluster, _baseAccessor);
    InstanceConfig config = new InstanceConfig(name);
    config.setInstanceEnabled(true);
    if (capacity != null) {
      config.setInstanceCapacityMap(capacity);
    }
    acc.setProperty(acc.keyBuilder().instanceConfig(name), config);
  }

  private void addLive(String cluster, String name) {
    HelixDataAccessor acc = new ZKHelixDataAccessor(cluster, _baseAccessor);
    LiveInstance live = new LiveInstance(name);
    live.setSessionId("session-" + name);
    live.setHelixVersion("0.0.1");
    acc.setProperty(acc.keyBuilder().liveInstance(name), live);
  }

  private void writeIdealState(String cluster, String name, String stateModel, boolean waged) {
    HelixDataAccessor acc = new ZKHelixDataAccessor(cluster, _baseAccessor);
    IdealState idealState = new IdealState(name);
    idealState.setStateModelDefRef(stateModel);
    idealState.setRebalanceMode(IdealState.RebalanceMode.FULL_AUTO);
    if (waged) {
      idealState.setRebalancerClassName(WagedRebalancer.class.getName());
    }
    idealState.setReplicas("1");
    idealState.setNumPartitions(1);
    acc.setProperty(acc.keyBuilder().idealStates(name), idealState);
  }

  private void writeExternalView(String cluster, String name, Map<String, String> states) {
    HelixDataAccessor acc = new ZKHelixDataAccessor(cluster, _baseAccessor);
    ExternalView externalView = new ExternalView(name);
    externalView.setStateMap(name + "_0", states);
    acc.setProperty(acc.keyBuilder().externalView(name), externalView);
  }

  private static Map<String, String> stateMap(String... instanceStatePairs) {
    Map<String, String> map = new LinkedHashMap<>();
    for (int i = 0; i + 1 < instanceStatePairs.length; i += 2) {
      map.put(instanceStatePairs[i], instanceStatePairs[i + 1]);
    }
    return map;
  }

  private static WagedCapacitySnapshot.InstanceCapacity instance(WagedCapacitySnapshot snapshot,
      String name) {
    for (WagedCapacitySnapshot.InstanceCapacity row : snapshot.instances) {
      if (row.name.equals(name)) {
        return row;
      }
    }
    throw new AssertionError("Instance " + name + " not found in snapshot");
  }
}
