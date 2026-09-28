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

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.helix.controller.dataproviders.ResourceControllerDataProvider;
import org.apache.helix.controller.stages.CurrentStateOutput;
import org.apache.helix.model.ClusterConfig;
import org.apache.helix.model.IdealState;
import org.apache.helix.model.InstanceConfig;
import org.apache.helix.model.Partition;
import org.apache.helix.model.Resource;
import org.apache.helix.model.ResourceConfig;
import org.mockito.Mockito;
import org.testng.Assert;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;


/**
 * Diagnostic reproduction for the chi_5 non-placement incident (cluster uic-hs-31, prod-lva1).
 *
 * HELIX-SIDE ROOT CAUSE UNDER TEST:
 *   {@link WagedInstanceCapacity#process} charges a full capacity slot for EVERY replica present in
 *   the current state, iterating {@code currentStateMap.keySet()} with NO regard for the replica's
 *   state (see WagedInstanceCapacity.processCurrentState). Therefore a replica wedged in a
 *   transient / non-serving state (in the incident: admintest25 stuck in INDEX_DOWNLOADED, which is
 *   NOT the counted top state CAUGHT_UP) occupies a capacity slot exactly like a serving replica,
 *   and can block placement of an unrelated brand-new partition (chi_5) with
 *   "... has no capacity to hold ..._5, removing it from combinedPreferenceList".
 *
 * If these tests pass, the enforcer-side mechanism of the incident is demonstrated on this exact
 * source tree: a stuck, non-serving replica consumes WAGED capacity and starves a new partition.
 */
public class TestWagedTransientStateCapacityBlocking {

  private static final String SLOT = "SLOT";
  private static final List<String> CAPACITY_KEYS = Lists.newArrayList(SLOT);
  // Each instance has exactly 2 slots -- mirrors the incident's cap = 2 per instance.
  private static final Map<String, Integer> INSTANCE_CAPACITY = ImmutableMap.of(SLOT, 2);
  // Each partition weighs 1 slot.
  private static final Map<String, Integer> PARTITION_WEIGHT = ImmutableMap.of(SLOT, 1);

  private static final String STUCK_RESOURCE = "stuck-resource"; // admintest25 analog
  private static final String NEW_RESOURCE = "new-resource";     // chi analog
  private static final String INSTANCE = "instance-0";           // app118742 analog

  // A transient, NON-top state. In the incident the occupants were wedged in INDEX_DOWNLOADED;
  // the counted / serving top state was CAUGHT_UP.
  private static final String TRANSIENT_STATE = "INDEX_DOWNLOADED";
  private static final String SERVING_TOP_STATE = "CAUGHT_UP";

  private ResourceControllerDataProvider _clusterData;

  @BeforeMethod
  public void setUp() {
    _clusterData = Mockito.spy(new ResourceControllerDataProvider());

    InstanceConfig instanceConfig = new InstanceConfig(INSTANCE);
    instanceConfig.setInstanceCapacityMap(INSTANCE_CAPACITY);
    _clusterData.setInstanceConfigMap(ImmutableMap.of(INSTANCE, instanceConfig));

    Map<String, ResourceConfig> resourceConfigMap = new HashMap<>();
    try {
      for (String resourceName : new String[] {STUCK_RESOURCE, NEW_RESOURCE}) {
        ResourceConfig rc = new ResourceConfig(resourceName);
        Map<String, Map<String, Integer>> partitionCapacityMap = new HashMap<>();
        partitionCapacityMap.put("DEFAULT", PARTITION_WEIGHT);
        rc.setPartitionCapacityMap(partitionCapacityMap);
        resourceConfigMap.put(resourceName, rc);
      }
    } catch (java.io.IOException e) {
      throw new RuntimeException("error while setting partition capacity map", e);
    }
    _clusterData.setResourceConfigMap(resourceConfigMap);

    List<IdealState> idealStates = Lists.newArrayList();
    for (String resourceName : new String[] {STUCK_RESOURCE, NEW_RESOURCE}) {
      IdealState is = new IdealState(resourceName);
      is.setRebalanceMode(IdealState.RebalanceMode.FULL_AUTO);
      is.setRebalancerClassName(WagedRebalancer.class.getName());
      idealStates.add(is);
    }
    _clusterData.setIdealStates(idealStates);

    // No pending messages -- the incident's block came from current state, not bootstrap messages.
    Mockito.doReturn(ImmutableMap.of()).when(_clusterData).getAllInstancesMessages();

    ClusterConfig clusterConfig = new ClusterConfig("test");
    clusterConfig.setTopologyAwareEnabled(false);
    clusterConfig.setInstanceCapacityKeys(CAPACITY_KEYS);
    _clusterData.setClusterConfig(clusterConfig);
  }

  private CurrentStateOutput currentStateWithBothStuckPartitionsInState(String state) {
    Resource stuck = new Resource(STUCK_RESOURCE);
    stuck.addPartition("stuck_0");
    stuck.addPartition("stuck_1");
    CurrentStateOutput cso = new CurrentStateOutput();
    for (Partition p : stuck.getPartitions()) {
      cso.setCurrentState(STUCK_RESOURCE, p, INSTANCE, state);
    }
    return cso;
  }

  private Map<String, Resource> resourceMap() {
    Resource stuck = new Resource(STUCK_RESOURCE);
    stuck.addPartition("stuck_0");
    stuck.addPartition("stuck_1");
    Resource newResource = new Resource(NEW_RESOURCE);
    newResource.addPartition("new_0");
    return ImmutableMap.of(STUCK_RESOURCE, stuck, NEW_RESOURCE, newResource);
  }

  /**
   * The incident, reproduced: two replicas wedged in INDEX_DOWNLOADED (non-serving) fill both slots
   * of the instance, and the brand-new partition is then rejected for "no capacity".
   */
  @Test
  public void transientStateReplicasConsumeCapacityAndBlockNewPartition() {
    CurrentStateOutput cso = currentStateWithBothStuckPartitionsInState(TRANSIENT_STATE);
    WagedResourceWeightsProvider weightProvider = new WagedResourceWeightsProvider(_clusterData);
    WagedInstanceCapacity capacity = new WagedInstanceCapacity(_clusterData);

    // Sanity: before processing, instance-0 has both slots free.
    Assert.assertEquals(capacity.getInstanceAvailableCapacity(INSTANCE).get(SLOT), Integer.valueOf(2));

    // The enforcer ledger accounts for current state (both partitions are in INDEX_DOWNLOADED).
    capacity.process(_clusterData, cso, resourceMap(), weightProvider);

    // ASSERTION 1: the two non-serving INDEX_DOWNLOADED replicas consumed BOTH capacity slots.
    Assert.assertEquals(capacity.getInstanceAvailableCapacity(INSTANCE).get(SLOT), Integer.valueOf(0),
        "Replicas stuck in a transient/non-serving state must still consume capacity");

    // ASSERTION 2: a brand-new partition (chi_5 analog) is rejected for "no capacity" on this
    // instance -- exactly the prune observed for 10h -- even though the occupants are NOT serving.
    Assert.assertFalse(
        capacity.checkAndReduceInstanceCapacity(INSTANCE, NEW_RESOURCE, "new_0", PARTITION_WEIGHT),
        "New partition must be rejected: instance is full of non-serving (stuck) replicas");
    Assert.assertFalse(capacity.isInstanceCapacityAvailable(INSTANCE, PARTITION_WEIGHT),
        "Instance must report no available capacity while stuck replicas occupy every slot");
  }

  /**
   * Proves the enforcer ignores the replica's state value: charging is identical whether the two
   * occupants are in the serving top state (CAUGHT_UP) or wedged in a transient state
   * (INDEX_DOWNLOADED). This is why a stuck replica is indistinguishable from a healthy one to
   * WAGED capacity accounting.
   */
  @Test
  public void capacityChargeIsIdenticalForServingAndTransientState() {
    WagedResourceWeightsProvider weightProvider = new WagedResourceWeightsProvider(_clusterData);

    WagedInstanceCapacity serving = new WagedInstanceCapacity(_clusterData);
    serving.process(_clusterData, currentStateWithBothStuckPartitionsInState(SERVING_TOP_STATE),
        resourceMap(), weightProvider);

    WagedInstanceCapacity stuck = new WagedInstanceCapacity(_clusterData);
    stuck.process(_clusterData, currentStateWithBothStuckPartitionsInState(TRANSIENT_STATE),
        resourceMap(), weightProvider);

    Assert.assertEquals(stuck.getInstanceAvailableCapacity(INSTANCE).get(SLOT),
        serving.getInstanceAvailableCapacity(INSTANCE).get(SLOT),
        "Enforcer capacity charge must be identical for a serving vs a stuck replica");
    Assert.assertEquals(stuck.getInstanceAvailableCapacity(INSTANCE).get(SLOT), Integer.valueOf(0),
        "Both slots consumed regardless of whether the occupants ever reached the serving state");
  }
}
