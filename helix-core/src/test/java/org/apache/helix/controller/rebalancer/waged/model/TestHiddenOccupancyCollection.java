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
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.apache.helix.controller.dataproviders.ResourceControllerDataProvider;
import org.apache.helix.controller.rebalancer.waged.WagedInstanceCapacity;
import org.apache.helix.controller.rebalancer.waged.WagedRebalancer;
import org.apache.helix.model.ClusterConfig;
import org.apache.helix.model.CurrentState;
import org.apache.helix.model.IdealState;
import org.apache.helix.model.InstanceConfig;
import org.apache.helix.model.LiveInstance;
import org.apache.helix.model.Message;
import org.apache.helix.model.Partition;
import org.apache.helix.model.Resource;
import org.apache.helix.model.ResourceAssignment;
import org.mockito.stubbing.Answer;
import org.testng.Assert;
import org.testng.annotations.Test;

import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Hidden occupancy is the disagreement between two ledgers: {@link WagedInstanceCapacity} charges
 * every current-state replica regardless of state, while the planner only builds an
 * {@link AssignableReplica} for states the state model gives a positive count. A replica stuck in
 * an uncounted state is charged but unplannable, so the planner keeps proposing placements the
 * capacity check rejects and the partition never lands anywhere.
 * <p>
 * Each condition narrowing that collection exists to keep the planner stable, and every one of
 * them is easy to drop in a refactor, so they are pinned here rather than only argued for in a
 * comment. {@link #testCountedStateIsNotRecorded} and {@link #testChargedReplicaIsNotRecorded} are
 * the two guards that stop an ordinary migration from registering as hidden occupancy, and
 * {@link #testHiddenOccupancyDoesNotChangeClusterCapacity} pins the isolation that stops it from
 * evicting anything already placed.
 */
public class TestHiddenOccupancyCollection extends AbstractTestClusterModel {
  private static final String RESOURCE = "Resource1";
  private static final String PARTITION = "Partition1";
  // Resource1's per-partition weight, from the shared fixture.
  private static final int ITEM1_WEIGHT = 3;
  private static final int ITEM2_WEIGHT = 6;

  private ResourceControllerDataProvider newCache(boolean featureEnabled) throws IOException {
    ResourceControllerDataProvider cache = super.setupClusterDataCache();
    ClusterConfig config = cache.getClusterConfig();
    config.setWagedCountUnallocatedOccupancyEnabled(featureEnabled);
    when(cache.getClusterConfig()).thenReturn(config);
    // Collection only runs when the capacity check it is reconciling with is active.
    when(cache.getWagedInstanceCapacity()).thenReturn(mock(WagedInstanceCapacity.class));

    Map<String, IdealState> idealStates = new HashMap<>();
    for (String resourceName : _resourceNames) {
      IdealState is = new IdealState(resourceName);
      is.setNumPartitions(_partitionNames.size());
      is.setRebalanceMode(IdealState.RebalanceMode.FULL_AUTO);
      is.setStateModelDefRef("MasterSlave");
      is.setReplicas("3");
      is.setRebalancerClassName(WagedRebalancer.class.getName());
      _partitionNames.forEach(p -> is.setPreferenceList(p, Collections.emptyList()));
      idealStates.put(resourceName, is);
    }
    when(cache.getIdealState(anyString()))
        .thenAnswer((Answer<IdealState>) call -> idealStates.get(call.getArguments()[0]));
    return cache;
  }

  /**
   * Replace the instance's current state so that Resource1/Partition1 reports the given state and
   * nothing else is present. Keeping exactly one replica in play makes the expected occupancy an
   * exact number rather than a bound.
   */
  private void setSoleCurrentState(ResourceControllerDataProvider cache, String state) {
    setSoleCurrentState(cache, state, PARTITION);
  }

  private void setSoleCurrentState(ResourceControllerDataProvider cache, String state,
      String partitionName) {
    Map<String, CurrentState> currentStates = new HashMap<>();
    currentStates.put(RESOURCE,
        buildCurrentState(RESOURCE, _sessionId, "MasterSlave", partitionName, state));
    when(cache.getCurrentState(_testInstanceId, _sessionId)).thenReturn(currentStates);
    when(cache.getCurrentState(_testInstanceId, _sessionId, false)).thenReturn(currentStates);
  }

  private void setPendingTransition(ResourceControllerDataProvider cache, String resourceName,
      String partitionName) {
    Message message = new Message(Message.MessageType.STATE_TRANSITION, "testMessageId");
    message.setResourceName(resourceName);
    message.setPartitionName(partitionName);
    when(cache.getMessages(_testInstanceId))
        .thenReturn(Collections.singletonMap(message.getId(), message));
  }

  private Map<String, Resource> resourceMap() {
    Map<String, Resource> resourceMap = new HashMap<>();
    for (String resourceName : _resourceNames) {
      Resource resource = new Resource(resourceName);
      _partitionNames.forEach(resource::addPartition);
      resourceMap.put(resourceName, resource);
    }
    return resourceMap;
  }

  private Map<String, ResourceAssignment> emptyPlan() {
    Map<String, ResourceAssignment> plan = new HashMap<>();
    for (String resourceName : _resourceNames) {
      plan.put(resourceName, new ResourceAssignment(resourceName));
    }
    return plan;
  }

  /** A plan that places Resource1/Partition1 on the given instance as MASTER. */
  private Map<String, ResourceAssignment> planPlacingPartitionOn(String instance) {
    Map<String, ResourceAssignment> plan = emptyPlan();
    plan.get(RESOURCE).addReplicaMap(new Partition(PARTITION),
        Collections.singletonMap(instance, "MASTER"));
    return plan;
  }

  /** A plan that places Resource1/Partition1 on the test instance as MASTER. */
  private Map<String, ResourceAssignment> planPlacingPartitionOnInstance() {
    return planPlacingPartitionOn(_testInstanceId);
  }

  /**
   * Register a second active instance on the mocked cache and return the active-instance set that
   * includes it. Built locally rather than by mutating {@code _instances}, which is shared across
   * every test in the class because the fixture initializes it once per class.
   */
  private Set<String> withSecondInstance(ResourceControllerDataProvider cache, String instance) {
    Map<String, InstanceConfig> instanceConfigs = cache.getAssignableInstanceConfigMap();
    instanceConfigs.put(instance, createMockInstanceConfig(instance));
    when(cache.getAssignableInstanceConfigMap()).thenReturn(instanceConfigs);
    when(cache.getInstanceConfigMap()).thenReturn(instanceConfigs);

    Map<String, LiveInstance> liveInstances = cache.getAssignableLiveInstances();
    liveInstances.put(instance, createMockLiveInstance(instance));
    when(cache.getAssignableLiveInstances()).thenReturn(liveInstances);

    Set<String> active = new HashSet<>(_instances);
    active.add(instance);
    return active;
  }

  private static int totalHiddenOccupancy(ClusterModel model) {
    return model.getAssignableNodes().values().stream()
        .mapToInt(node -> node.getHiddenOccupancy().values().stream().mapToInt(Integer::intValue)
            .sum())
        .sum();
  }

  private ClusterModel emergencyModel(ResourceControllerDataProvider cache,
      Map<String, ResourceAssignment> plan) {
    return emergencyModel(cache, plan, _instances);
  }

  private ClusterModel emergencyModel(ResourceControllerDataProvider cache,
      Map<String, ResourceAssignment> plan, Set<String> activeInstances) {
    return ClusterModelProvider.generateClusterModelForEmergencyRebalance(cache, resourceMap(),
        activeInstances, plan);
  }

  private ClusterModel partialModel(ResourceControllerDataProvider cache,
      Map<String, ResourceAssignment> plan) {
    return ClusterModelProvider.generateClusterModelForPartialRebalance(cache, resourceMap(),
        _instances, emptyPlan(), plan);
  }

  /**
   * The case the feature exists for: a replica physically present, in a state carrying no count in
   * the state model, that the plan does not place here and that has no transition in flight.
   */
  @Test
  public void testUncountedOffPlanOccupancyIsRecorded() throws IOException {
    ResourceControllerDataProvider cache = newCache(true);
    setSoleCurrentState(cache, "OFFLINE");

    ClusterModel model = emergencyModel(cache, emptyPlan());

    AssignableNode node = model.getAssignableNodes().get(_testInstanceId);
    Assert.assertEquals(node.getHiddenOccupancy().get("item1"), Integer.valueOf(ITEM1_WEIGHT),
        "the stuck replica's weight should be withheld on every capacity key it uses");
    Assert.assertEquals(node.getHiddenOccupancy().get("item2"), Integer.valueOf(ITEM2_WEIGHT));
  }

  /**
   * Partition weights are legitimately configured cluster-wide rather than per resource, in which
   * case {@code getResourceConfig} returns null for a perfectly normal WAGED resource. An earlier
   * version treated that as "not a WAGED resource" and skipped it, which silently disabled the
   * whole feature for every cluster configured that way. The weight has to resolve the same way
   * {@code WagedResourceWeightsProvider} resolves it for the capacity ledger, which passes the null
   * config straight through.
   */
  @Test
  public void testWeightsResolveFromClusterDefaultsWhenResourceConfigIsAbsent() throws IOException {
    ResourceControllerDataProvider cache = newCache(true);
    ClusterConfig config = cache.getClusterConfig();
    Map<String, Integer> unitWeights = new HashMap<>();
    config.getInstanceCapacityKeys().forEach(key -> unitWeights.put(key, 1));
    config.setDefaultPartitionWeightMap(unitWeights);
    when(cache.getClusterConfig()).thenReturn(config);
    when(cache.getResourceConfig(RESOURCE)).thenReturn(null);
    setSoleCurrentState(cache, "OFFLINE");

    Assert.assertEquals(totalHiddenOccupancy(emergencyModel(cache, emptyPlan())), unitWeights.size(),
        "a resource weighted by the cluster defaults must still be counted");
  }

  /**
   * A replica in a counted state is one the planner can model, so both ledgers already agree about
   * it and there is no divergence to correct. This is the guard that keeps an ordinary migration
   * from registering: a replica draining off its old instance is off-plan there for the whole of
   * the drop, and counting it would cost that instance capacity for the duration and hand it back
   * immediately afterwards.
   */
  @Test
  public void testCountedStateIsNotRecorded() throws IOException {
    for (String state : new String[] {"MASTER", "SLAVE"}) {
      ResourceControllerDataProvider cache = newCache(true);
      setSoleCurrentState(cache, state);

      Assert.assertEquals(totalHiddenOccupancy(emergencyModel(cache, emptyPlan())), 0,
          "a replica in " + state + " is visible to the planner and must not be double-charged");
    }
  }

  /**
   * A newly placed replica walks through uncounted intermediate states on its way up. Baseline and
   * best-possible agree it belongs here, so it is charged by {@code assignInitBatch}; charging it
   * again would push the planner to move a replica that is simply still starting.
   */
  @Test
  public void testChargedReplicaIsNotRecorded() throws IOException {
    ResourceControllerDataProvider cache = newCache(true);
    setSoleCurrentState(cache, "OFFLINE");

    // The plan names this live instance, so the replica settles into allocatedReplicas.
    Assert.assertEquals(
        totalHiddenOccupancy(emergencyModel(cache, planPlacingPartitionOnInstance())), 0,
        "occupancy assignInitBatch already charged must not be counted twice");
  }

  /**
   * The defect this condition is easy to get wrong in: a replica the committed assignment still
   * shows on this instance, but which baseline disagrees about, is pulled into the work list and so
   * is never charged by {@code assignInitBatch}. It is nonetheless physically resident and the
   * capacity ledger charges it, so it has to be recorded.
   * <p>
   * This is the production shape that motivated it. On the incident instance one stuck replica was
   * off-plan entirely while a second was still named by best-possible but had been pulled into the
   * work list by a baseline that placed it elsewhere. Keying the exclusion off the committed plan
   * rather than off the charged set hid that second replica, leaving the instance looking half
   * empty when it was full, which is the whole condition this feature exists to prevent.
   */
  /**
   * The defect this condition is easy to get wrong in: a replica whose assignment names an
   * instance that has gone down is pulled into the work list and so is never charged by
   * {@code assignInitBatch}. It is nonetheless still physically resident on the instance reporting
   * it, and the capacity ledger charges it there, so it has to be recorded.
   * <p>
   * This is the production shape that motivated it. On the incident instance one stuck replica was
   * off-plan entirely while a second was still named by the committed assignment but had been
   * pulled into the work list. Keying the exclusion off the plan rather than off the charged set
   * hid that second replica, leaving the instance looking half empty when it was full, which is
   * the whole condition this feature exists to prevent.
   */
  @Test
  public void testPlannedButUnchargedReplicaIsRecorded() throws IOException {
    ResourceControllerDataProvider cache = newCache(true);
    setSoleCurrentState(cache, "OFFLINE");

    // The assignment names an instance that is not active, so the replica goes to the work list
    // and nothing charges it -- while it still physically occupies the instance reporting it.
    ClusterModel model = emergencyModel(cache, planPlacingPartitionOn("downInstanceId"));

    Assert.assertEquals(
        model.getAssignableNodes().get(_testInstanceId).getHiddenOccupancy()
            .getOrDefault("item1", 0).intValue(),
        ITEM1_WEIGHT,
        "a replica named by the plan but never charged is real occupancy and must be recorded");
  }

  /**
   * The incident case, and a regression test for a pending-message filter that used to be here.
   * A replica wedged in an uncounted state is wedged precisely because its transition never
   * completes, so it carries a pending message for as long as it is stuck. Treating "has a pending
   * message" as "still in motion" therefore excluded exactly the occupancy this exists to find.
   */
  @Test
  public void testWedgedReplicaWithStuckTransitionIsRecorded() throws IOException {
    ResourceControllerDataProvider cache = newCache(true);
    setSoleCurrentState(cache, "OFFLINE");
    setPendingTransition(cache, RESOURCE, PARTITION);

    Assert.assertEquals(totalHiddenOccupancy(emergencyModel(cache, emptyPlan())), ITEM1_WEIGHT + ITEM2_WEIGHT,
        "a replica stuck mid-transition is the case this exists for and must still be counted");
  }

  /**
   * The gate has to withhold exactly what the capacity ledger charged, and that ledger charges
   * every replica an instance reports with no state filter at all
   * ({@code WagedInstanceCapacity#processCurrentState}). Excluding DROPPED here would reopen the
   * same disagreement between the two ledgers in miniature.
   */
  @Test
  public void testDroppedIsRecordedBecauseTheLedgerChargesIt() throws IOException {
    ResourceControllerDataProvider cache = newCache(true);
    setSoleCurrentState(cache, "DROPPED");

    Assert.assertEquals(totalHiddenOccupancy(emergencyModel(cache, emptyPlan())), ITEM1_WEIGHT + ITEM2_WEIGHT);
  }

  /**
   * The baseline must never see runtime occupancy. It is a from-scratch ideal placement that
   * deliberately ignores where replicas currently sit, and it is the reference the cluster
   * converges towards, so letting runtime occupancy move it would make that reference unstable.
   * <p>
   * The two scopes that place replicas against real capacity -- partial and emergency -- both see
   * it. Churn is held off not by excluding a scope but by excluding replicas: a partition already
   * carrying its full complement is exempt wherever it is planned.
   */
  @Test
  public void testBaselineScopeRecordsNothing() throws IOException {
    ResourceControllerDataProvider cache = newCache(true);
    setSoleCurrentState(cache, "OFFLINE");

    ClusterModel baseline = ClusterModelProvider.generateClusterModelForBaseline(cache,
        resourceMap(), _instances, Collections.emptyMap(), emptyPlan());

    Assert.assertEquals(totalHiddenOccupancy(baseline), 0,
        "the baseline must not see runtime occupancy");
    Assert.assertTrue(totalHiddenOccupancy(partialModel(cache, emptyPlan())) > 0,
        "the same inputs must produce occupancy in the partial scope, otherwise this test would "
            + "pass for the wrong reason");
    Assert.assertTrue(totalHiddenOccupancy(emergencyModel(cache, emptyPlan())) > 0,
        "and in the emergency scope");
  }

  /**
   * Emergency rebalance places replicas stranded on permanently downed instances, and it places
   * them onto live ones. That is real placement against real capacity, so it has to see the same
   * occupancy the partial scope does or it will strand them a second time on an instance the
   * capacity ledger will refuse.
   */
  @Test
  public void testEmergencyScopeRecordsOccupancy() throws IOException {
    ResourceControllerDataProvider cache = newCache(true);
    setSoleCurrentState(cache, "OFFLINE");

    ClusterModel emergency = ClusterModelProvider.generateClusterModelForEmergencyRebalance(cache,
        resourceMap(), _instances, emptyPlan());

    Assert.assertTrue(totalHiddenOccupancy(emergency) > 0,
        "emergency rebalance assigns against live capacity and must see hidden occupancy");
  }

  /**
   * The delayed overwrite scope is a temporary calculation for partitions below minActiveReplicas
   * inside the delay window; its result is discarded rather than persisted. Collecting there would
   * spend the walk on an answer nothing reads.
   */
  @Test
  public void testDelayedOverwriteScopeRecordsNothing() throws IOException {
    ResourceControllerDataProvider cache = newCache(true);
    setSoleCurrentState(cache, "OFFLINE");

    ClusterModel overwrites = ClusterModelProvider
        .generateClusterModelForDelayedRebalanceOverwrites(cache, resourceMap(), _instances,
            emptyPlan());

    Assert.assertEquals(totalHiddenOccupancy(overwrites), 0);
  }

  /**
   * An instance can still report a partition that no longer belongs to the resource, for instance
   * after the partition count is reduced. The capacity ledger walks the resource's partitions and
   * so never charges it, and charging it here would withhold room for something neither ledger
   * counts -- a permanent capacity leak that no operator action could clear.
   */
  @Test
  public void testStalePartitionNotInResourceIsNotRecorded() throws IOException {
    ResourceControllerDataProvider cache = newCache(true);
    setSoleCurrentState(cache, "OFFLINE", "PartitionThatNoLongerExists");

    Assert.assertEquals(totalHiddenOccupancy(emergencyModel(cache, emptyPlan())), 0);
  }

  /**
   * Hidden occupancy must stay out of the node's capacity totals. Those feed
   * {@link ClusterContext}'s cluster-wide capacity and its utilization estimate, and a negative
   * estimate aborts the entire rebalance with CAPACITY_DEFICIT -- one stuck replica would stop the
   * cluster rebalancing at all. It must narrow eligibility only.
   */
  @Test
  public void testHiddenOccupancyDoesNotChangeClusterCapacity() throws IOException {
    ResourceControllerDataProvider withoutOccupancy = newCache(true);
    setSoleCurrentState(withoutOccupancy, "MASTER");
    ClusterModel clean = emergencyModel(withoutOccupancy, emptyPlan());

    ResourceControllerDataProvider withOccupancy = newCache(true);
    setSoleCurrentState(withOccupancy, "OFFLINE");
    ClusterModel wedged = emergencyModel(withOccupancy, emptyPlan());

    Assert.assertTrue(totalHiddenOccupancy(wedged) > 0, "test setup should produce occupancy");
    Assert.assertEquals(wedged.getContext().getClusterCapacityMap(),
        clean.getContext().getClusterCapacityMap(),
        "hidden occupancy must not change the cluster's capacity totals");
    Assert.assertEquals(wedged.getContext().getEstimatedMaxUtilization(),
        clean.getContext().getEstimatedMaxUtilization(),
        "hidden occupancy must not move the cluster utilization estimate");

    AssignableNode wedgedNode = wedged.getAssignableNodes().get(_testInstanceId);
    AssignableNode cleanNode = clean.getAssignableNodes().get(_testInstanceId);
    Assert.assertEquals(wedgedNode.getMaxCapacity(), cleanNode.getMaxCapacity(),
        "hidden occupancy must not shrink the node's declared capacity");
    Assert.assertEquals(wedgedNode.getRemainingCapacity(), cleanNode.getRemainingCapacity(),
        "hidden occupancy must not be folded into remaining capacity, which feeds preference "
            + "scoring as well as the hard check");
  }

  /** Off by default: an unset flag must leave every existing cluster untouched. */
  @Test
  public void testDisabledByDefaultRecordsNothing() throws IOException {
    ResourceControllerDataProvider cache = newCache(false);
    setSoleCurrentState(cache, "OFFLINE");

    Assert.assertEquals(totalHiddenOccupancy(emergencyModel(cache, emptyPlan())), 0);
  }
}
