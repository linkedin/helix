package org.apache.helix.controller.rebalancer.waged.model;

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

import java.io.IOException;
import java.util.Collections;
import java.util.Map;
import java.util.Set;

import com.google.common.collect.ImmutableMap;
import org.apache.helix.controller.dataproviders.ResourceControllerDataProvider;
import org.apache.helix.controller.rebalancer.waged.WagedInstanceCapacity;
import org.apache.helix.controller.rebalancer.waged.WagedRebalancer;
import org.apache.helix.controller.rebalancer.waged.WagedResourceWeightsProvider;
import org.apache.helix.controller.stages.CurrentStateOutput;
import org.apache.helix.model.IdealState;
import org.apache.helix.model.Partition;
import org.apache.helix.model.Resource;
import org.apache.helix.model.ResourceAssignment;
import org.mockito.Mockito;
import org.mockito.stubbing.Answer;
import org.testng.Assert;
import org.testng.annotations.Test;

import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.when;

/**
 * Definitive, executable reproduction of the HELIX-SIDE root cause of the chi_5 non-placement
 * incident (cluster uic-hs-31, fabric prod-lva1): WAGED uses TWO independently-computed capacity
 * ledgers that DISAGREE about a replica wedged in a transient / non-top state.
 *
 * <p>The two ledgers, and why they diverge on the SAME physical occupant:
 *
 * <ol>
 *   <li><b>Assignment stage (the "planner")</b> -
 *       {@link ClusterModelProvider#generateClusterModelForPartialRebalance}. Node capacity is
 *       seeded via {@code AssignableNode.assignInitBatch(allocatedReplicas)}, and a replica only
 *       lands in {@code allocatedReplicas} when its CURRENT placement is in the counted top state
 *       (see {@code findToBeAssignedReplicasByComparingWithIdealAssignment}, Cases 1 and 3). A
 *       replica stuck in a non-top state (Case 2/4) is treated as {@code toBeAssigned} and the node
 *       is NOT charged. So the planner sees the node as having free capacity and will (re)assign a
 *       new partition onto it.</li>
 *   <li><b>Enforcement stage (the "veto")</b> -
 *       {@link WagedInstanceCapacity#process} charges a slot for EVERY replica present in the
 *       current state, iterating {@code currentStateMap.keySet()} with no regard for the replica's
 *       state. So the enforcer counts the stuck replica, finds the node full, and prunes the newly
 *       assigned partition ("... has no capacity to hold ..., removing it from
 *       combinedPreferenceList").</li>
 * </ol>
 *
 * <p>Planner assigns -> enforcer prunes -> the new partition (chi_5) is placed nowhere, every round,
 * until a stuck sibling physically vacates the slot (the observed ~10h stall).
 *
 * <p>State-name mapping to the incident's SEAS state model:
 * <ul>
 *   <li>{@code MASTER}            &lt;-&gt; {@code CAUGHT_UP}        (the counted, serving top state)</li>
 *   <li>{@code INDEX_DOWNLOADED}  &lt;-&gt; {@code INDEX_DOWNLOADED} (a transient, NON-counted state)</li>
 * </ul>
 * The mechanism only depends on the current state NOT being the counted top state; the exact string
 * is irrelevant to both ledgers (neither validates it against the state model here).
 */
public class TestWagedPlannerVsEnforcerCapacityDivergence extends AbstractTestClusterModel {

  private static final String RESOURCE = "Resource1";
  private static final String PARTITION = "Partition1";

  // The counted, serving top state (analog of CAUGHT_UP).
  private static final String SERVING_TOP_STATE = "MASTER";
  // A replica physically present on the node but NOT in the counted top state (analog of the
  // admintest25 replicas wedged mid state-transition in the incident).
  private static final String TRANSIENT_STATE = "INDEX_DOWNLOADED";

  // From AbstractTestClusterModel: instance capacity item1 = 20; Resource1 partition weight item1 = 3.
  private static final String CAPACITY_KEY = "item1";
  private static final int INSTANCE_CAPACITY_ITEM1 = 20;
  private static final int PARTITION_WEIGHT_ITEM1 = 3;

  /**
   * Build the shared cache: a single WAGED-managed resource with one partition, one instance.
   * Both the planner and the enforcer read from this exact same cache.
   */
  private ResourceControllerDataProvider buildCache() throws IOException {
    ResourceControllerDataProvider cache = setupClusterDataCache();

    // Mirror the proven partial-rebalance fixture: give the instance a distinct fault zone so its
    // logical id resolves cleanly (see TestClusterModelProvider#testGenerateClusterModelForPartialRebalance).
    cache.getAssignableInstanceConfigMap().get(_testInstanceId).setZoneId(_testInstanceId);

    // A WAGED-enabled ideal state (FULL_AUTO + WagedRebalancer) with a single partition.
    IdealState is = new IdealState(RESOURCE);
    is.setRebalanceMode(IdealState.RebalanceMode.FULL_AUTO);
    is.setStateModelDefRef("MasterSlave");
    is.setReplicas("1"); // stateCountMap => {MASTER:1}: exactly one counted replica per partition
    is.setRebalancerClassName(WagedRebalancer.class.getName());
    is.setNumPartitions(1);
    is.setPreferenceList(PARTITION, Collections.emptyList());
    when(cache.getIdealState(anyString())).thenAnswer(
        (Answer<IdealState>) inv -> RESOURCE.equals(inv.getArguments()[0]) ? is : null);

    // The enforcer's processPendingMessages() reads these; we exercise current-state charging only.
    when(cache.getEnabledLiveInstances()).thenReturn(Collections.emptySet());
    Mockito.doReturn(Collections.emptyMap()).when(cache).getAllInstancesMessages();

    return cache;
  }

  /** &lt;resource, &lt;partition -&gt; {instance: state}&gt;&gt; for the single partition under test. */
  private Map<String, ResourceAssignment> assignmentInState(String state) {
    ResourceAssignment ra = new ResourceAssignment(RESOURCE);
    ra.addReplicaMap(new Partition(PARTITION), ImmutableMap.of(_testInstanceId, state));
    return ImmutableMap.of(RESOURCE, ra);
  }

  /** Run the assignment-stage (planner) ledger and return the resulting model. */
  private ClusterModel runPlanner(ResourceControllerDataProvider cache, String currentStateOnNode) {
    Map<String, Resource> resourceMap = Collections.singletonMap(RESOURCE, new Resource(RESOURCE));
    Set<String> activeInstances = Collections.singleton(_testInstanceId);
    // ideal wants a serving MASTER on the node; current has the node in `currentStateOnNode`.
    return ClusterModelProvider.generateClusterModelForPartialRebalance(cache, resourceMap,
        activeInstances, assignmentInState(SERVING_TOP_STATE), assignmentInState(currentStateOnNode));
  }

  /** Run the enforcement-stage (veto) ledger and return remaining item1 capacity on the node. */
  private int runEnforcerRemainingItem1(ResourceControllerDataProvider cache,
      String currentStateOnNode) {
    WagedResourceWeightsProvider weightProvider = new WagedResourceWeightsProvider(cache);
    WagedInstanceCapacity enforcer = new WagedInstanceCapacity(cache);

    Resource resource = new Resource(RESOURCE);
    resource.addPartition(PARTITION);
    CurrentStateOutput cso = new CurrentStateOutput();
    cso.setCurrentState(RESOURCE, new Partition(PARTITION), _testInstanceId, currentStateOnNode);

    enforcer.process(cache, cso, Collections.singletonMap(RESOURCE, resource), weightProvider);
    return enforcer.getInstanceAvailableCapacity(_testInstanceId).get(CAPACITY_KEY);
  }

  /**
   * THE MONEY SHOT: feed the identical fact -- one replica on {@code _testInstanceId} for
   * {@code Partition1} in the transient state {@code INDEX_DOWNLOADED} -- to BOTH ledgers and prove
   * they disagree about how much capacity is left on that node.
   */
  @Test
  public void plannerAndEnforcerDivergeOnIdenticalTransientOccupant() throws IOException {
    ResourceControllerDataProvider cache = buildCache();

    // --- Planner (assignment stage) ---
    ClusterModel plannerModel = runPlanner(cache, TRANSIENT_STATE);
    AssignableNode node = plannerModel.getAssignableNodes().get(_testInstanceId);
    int plannerRemainingItem1 = node.getRemainingCapacity().get(CAPACITY_KEY);

    // The planner did NOT charge the node for the stuck replica: capacity is untouched...
    Assert.assertEquals(plannerRemainingItem1, INSTANCE_CAPACITY_ITEM1,
        "Planner must treat a node holding a transient/non-top-state replica as having full capacity");
    Assert.assertEquals(node.getAssignedReplicaCount(), 0,
        "Planner must not count the stuck replica as an assigned occupant of the node");
    // ...and it re-proposes the partition for assignment (it thinks the replica still needs placing).
    Assert.assertEquals(
        plannerModel.getAssignableReplicaMap().getOrDefault(RESOURCE, Collections.emptySet()).size(),
        1, "Planner must treat the transient-state replica as still to-be-assigned");

    // --- Enforcer (veto stage), SAME cache, SAME INDEX_DOWNLOADED occupant ---
    int enforcerRemainingItem1 = runEnforcerRemainingItem1(cache, TRANSIENT_STATE);

    // The enforcer DID charge the node for the very same stuck replica.
    Assert.assertEquals(enforcerRemainingItem1, INSTANCE_CAPACITY_ITEM1 - PARTITION_WEIGHT_ITEM1,
        "Enforcer must charge capacity for a replica present in current state, regardless of state");

    // --- The divergence: same input, two different verdicts on remaining capacity ---
    Assert.assertTrue(plannerRemainingItem1 > enforcerRemainingItem1,
        "ROOT CAUSE: planner sees more free capacity than the enforcer for the identical stuck "
            + "occupant (planner=" + plannerRemainingItem1 + ", enforcer=" + enforcerRemainingItem1
            + "). The planner keeps assigning onto a node the enforcer keeps rejecting.");
    Assert.assertEquals(plannerRemainingItem1 - enforcerRemainingItem1, PARTITION_WEIGHT_ITEM1,
        "The divergence equals exactly one uncounted partition weight");
  }

  /**
   * Control: when the SAME occupant is instead in the counted top state ({@code MASTER}), the
   * planner DOES charge the node -- proving the non-charging above is specifically caused by the
   * non-top (transient) state, not by a fixture artifact or by partial rebalance never charging.
   */
  @Test
  public void plannerChargesWhenReplicaIsInServingTopState() throws IOException {
    ResourceControllerDataProvider cache = buildCache();

    ClusterModel plannerModel = runPlanner(cache, SERVING_TOP_STATE);
    AssignableNode node = plannerModel.getAssignableNodes().get(_testInstanceId);

    Assert.assertEquals(node.getRemainingCapacity().get(CAPACITY_KEY).intValue(),
        INSTANCE_CAPACITY_ITEM1 - PARTITION_WEIGHT_ITEM1,
        "Planner must charge the node for a replica that IS in the counted top state");
    Assert.assertEquals(node.getAssignedReplicaCount(), 1,
        "A serving (top-state) replica must count as an assigned occupant");
    Assert.assertEquals(
        plannerModel.getAssignableReplicaMap().getOrDefault(RESOURCE, Collections.emptySet()).size(),
        0, "A serving (top-state) replica is already placed; nothing should be to-be-assigned");

    // And the enforcer agrees for the top-state case -- the two ledgers ONLY diverge for the
    // transient state. Same charge => no divergence here.
    int enforcerRemainingItem1 = runEnforcerRemainingItem1(cache, SERVING_TOP_STATE);
    Assert.assertEquals(node.getRemainingCapacity().get(CAPACITY_KEY).intValue(),
        enforcerRemainingItem1,
        "For a top-state replica both ledgers agree; divergence is specific to the transient state");
  }
}
