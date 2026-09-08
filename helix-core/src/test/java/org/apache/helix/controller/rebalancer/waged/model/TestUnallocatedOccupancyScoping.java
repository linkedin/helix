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
import java.util.stream.Collectors;

import org.apache.helix.controller.dataproviders.ResourceControllerDataProvider;
import org.apache.helix.controller.rebalancer.waged.WagedInstanceCapacity;
import org.apache.helix.controller.rebalancer.util.DelayedRebalanceUtil;
import org.apache.helix.controller.rebalancer.waged.WagedRebalancer;
import org.apache.helix.model.ClusterConfig;
import org.apache.helix.model.IdealState;
import org.apache.helix.model.Partition;
import org.apache.helix.model.Resource;
import org.apache.helix.model.ResourceAssignment;
import org.testng.Assert;
import org.mockito.stubbing.Answer;
import org.testng.annotations.Test;

import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * The unaccounted-occupancy collection is deliberately restricted to the partial rebalance scope.
 * The baseline is a from-scratch ideal placement that ignores where replicas currently sit, so
 * nothing is pre-allocated there and every instance would report its entire load as unaccounted,
 * biasing the ideal placement towards whichever instances happen to be empty. That restriction is
 * load-bearing and easy to lose in a refactor, so it is asserted here rather than only argued for
 * in a comment.
 */
public class TestUnallocatedOccupancyScoping extends AbstractTestClusterModel {
  private Set<String> _instances;

  @Override
  protected ResourceControllerDataProvider setupClusterDataCache() throws IOException {
    ResourceControllerDataProvider cache = super.setupClusterDataCache();
    ClusterConfig config = cache.getClusterConfig();
    config.setWagedCountUnallocatedOccupancyEnabled(true);
    when(cache.getClusterConfig()).thenReturn(config);
    // The collection only runs when the capacity check it is trying to agree with is active.
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

  private Map<String, Resource> resourceMap() {
    Map<String, Resource> resourceMap = new HashMap<>();
    for (String resourceName : _resourceNames) {
      Resource resource = new Resource(resourceName);
      _partitionNames.forEach(resource::addPartition);
      resourceMap.put(resourceName, resource);
    }
    return resourceMap;
  }

  private static int totalUnallocatedOccupancy(ClusterModel model) {
    return model.getAssignableNodes().values().stream()
        .mapToInt(node -> node.getUnallocatedOccupancy().values().stream()
            .mapToInt(Integer::intValue).sum())
        .sum();
  }

  @Test
  public void testOccupancyIsCollectedForPartialButNotBaseline() throws IOException {
    ResourceControllerDataProvider cache = setupClusterDataCache();
    _instances = new HashSet<>(Collections.singletonList(_testInstanceId));
    Map<String, Resource> resourceMap = resourceMap();

    // Nothing is assigned, so everything the instance physically holds is unaccounted for.
    Map<String, ResourceAssignment> emptyAssignment = new HashMap<>();
    for (String resourceName : _resourceNames) {
      emptyAssignment.put(resourceName, new ResourceAssignment(resourceName));
    }

    ClusterModel partial = ClusterModelProvider.generateClusterModelForPartialRebalance(cache,
        resourceMap, _instances, emptyAssignment, emptyAssignment);
    int partialOccupancy = totalUnallocatedOccupancy(partial);

    ClusterModel baseline = ClusterModelProvider.generateClusterModelForBaseline(cache, resourceMap,
        _instances, Collections.emptyMap(), emptyAssignment);
    int baselineOccupancy = totalUnallocatedOccupancy(baseline);

    Assert.assertTrue(partialOccupancy > 0,
        "the partial scope is the one whose assignment the capacity check prunes, so it must see "
            + "the occupancy; got " + partialOccupancy);
    Assert.assertEquals(baselineOccupancy, 0,
        "the baseline places from scratch and pre-allocates nothing, so collecting there would "
            + "report every instance's entire load as unaccounted; got " + baselineOccupancy);
  }

  /**
   * The emergency and delayed-overwrite scopes are excluded too, and it is worth recording why
   * that does not leave a gap. Emergency rebalance is a narrow correction for a permanently downed
   * node, computed from an assignment in which everything is already allocated -- so there is
   * little unaccounted occupancy for it to find. More importantly WagedRebalancer.emergencyRebalance
   * ends by calling PartialRebalanceRunner.partialRebalance unconditionally, and that runs in the
   * partial scope where this collection does apply. The occupancy is therefore accounted for on the
   * very next step rather than skipped.
   */
  @Test
  public void testNothingIsCollectedInEmergencyOrDelayedOverwriteScopes() throws IOException {
    ResourceControllerDataProvider cache = setupClusterDataCache();
    Map<String, ResourceAssignment> emptyAssignment = new HashMap<>();
    for (String resourceName : _resourceNames) {
      emptyAssignment.put(resourceName, new ResourceAssignment(resourceName));
    }
    Set<String> instances = new HashSet<>(Collections.singletonList(_testInstanceId));

    ClusterModel emergency = ClusterModelProvider.generateClusterModelForEmergencyRebalance(cache,
        resourceMap(), instances, emptyAssignment);
    Assert.assertEquals(totalUnallocatedOccupancy(emergency), 0,
        "emergency rebalance must not collect; the partial pass that follows it does");

    ClusterModel delayedOverwrites =
        ClusterModelProvider.generateClusterModelForDelayedRebalanceOverwrites(cache, resourceMap(),
            instances, emptyAssignment);
    Assert.assertEquals(totalUnallocatedOccupancy(delayedOverwrites), 0,
        "delayed rebalance overwrites must not collect");
  }

  /**
   * Excluding the delayed-overwrite scope is only safe because that scope's result is discarded.
   * It is blind to unaccounted occupancy, so it can propose a placement on an instance that is
   * physically full; what stops that from becoming permanent is that the proposal never reaches
   * the best-possible ledger the next partial pass reads.
   * <p>
   * That relies on one thing. WagedRebalancer persists the assignment before the overwrite runs,
   * and mergeAssignments then mutates the assignment object in place -- so the only reason the
   * persisted copy is not corrupted is that AssignmentManager.getBestPossibleAssignment and
   * AssignmentMetadataStore.persistBestPossibleAssignment each rebuild through
   * new ResourceAssignment(record), which deep copies. If that ever became a shallow wrap the
   * overwrite would silently start writing capacity-blind placements into the durable ledger,
   * where the next partial pass would read them back as already allocated and leave them alone.
   * The comment on that code says the result is temporary; this asserts it.
   */
  @Test
  public void testDelayedOverwriteResultCannotLeakIntoThePersistedAssignment() {
    Partition partition = new Partition("Resource_0");

    ResourceAssignment persisted = new ResourceAssignment("Resource");
    persisted.addReplicaMap(partition, Collections.singletonMap("healthyInstance", "MASTER"));

    // Obtained exactly the way the rebalancer obtains it, through the record-rebuilding copy.
    Map<String, ResourceAssignment> handedToOverwrite = Collections.singletonMap("Resource",
        new ResourceAssignment(persisted.getRecord()));

    // The overwrite decides a physically full instance should also host the partition.
    ResourceAssignment overwrite = new ResourceAssignment("Resource");
    overwrite.addReplicaMap(partition, Collections.singletonMap("saturatedInstance", "SLAVE"));

    DelayedRebalanceUtil.mergeAssignments(Collections.singletonMap("Resource", overwrite),
        handedToOverwrite);

    Assert.assertTrue(
        handedToOverwrite.get("Resource").getReplicaMap(partition).containsKey("saturatedInstance"),
        "guard: the merge must actually have applied, otherwise this test proves nothing");

    Assert.assertEquals(persisted.getReplicaMap(partition),
        Collections.singletonMap("healthyInstance", "MASTER"),
        "the delayed overwrite must not reach the persisted assignment, or a capacity-blind "
            + "placement would be read back as already allocated and never corrected");
  }

  /**
   * The flag has to gate the collection itself, not merely the scoring, otherwise a cluster with
   * the feature disabled would still pay for the current-state walk on every pass.
   */
  @Test
  public void testNothingIsCollectedWhenTheFlagIsOff() throws IOException {
    ResourceControllerDataProvider cache = setupClusterDataCache();
    ClusterConfig config = cache.getClusterConfig();
    config.setWagedCountUnallocatedOccupancyEnabled(false);
    when(cache.getClusterConfig()).thenReturn(config);

    Map<String, ResourceAssignment> emptyAssignment = new HashMap<>();
    for (String resourceName : _resourceNames) {
      emptyAssignment.put(resourceName, new ResourceAssignment(resourceName));
    }

    ClusterModel partial = ClusterModelProvider.generateClusterModelForPartialRebalance(cache,
        resourceMap(), new HashSet<>(Collections.singletonList(_testInstanceId)), emptyAssignment,
        emptyAssignment);

    Assert.assertEquals(totalUnallocatedOccupancy(partial), 0,
        "with the flag off nothing may be collected");
  }

  /**
   * The constraint carries a weight of 100000, which dwarfs PartitionMovementConstraint's maximum
   * effective influence of 2000. If a replica already sitting on a penalised instance were scored
   * against that instance, movement stickiness could not hold it there and enabling the feature
   * would evict healthy replicas off any instance holding unaccounted occupancy.
   * <p>
   * It cannot, and the reason is structural rather than a matter of weights: replicas that already
   * match the ideal assignment are separated into allocatedReplicas, pre-assigned via
   * assignInitBatch, and the ClusterModel is constructed with only toBeAssignedReplicas. Soft
   * constraints score the latter. An existing placement is therefore never a candidate for any soft
   * constraint to have an opinion about. This test pins that separation on an instance that is
   * simultaneously holding unaccounted occupancy.
   */
  @Test
  public void testExistingPlacementsAreNotScoredAndSoCannotBeEvicted() throws IOException {
    ResourceControllerDataProvider cache = setupClusterDataCache();

    // Partition1 is already placed on the instance and the ideal assignment agrees, so it is an
    // existing placement. Partition2 is wanted by the ideal assignment but is not there yet, so it
    // is a live placement decision. Both are handled in the same pass, which is what makes the
    // exclusion below meaningful rather than an artifact of there being nothing to place at all.
    String resource = _resourceNames.get(0);
    String existing = _partitionNames.get(0);
    String pending = _partitionNames.get(1);

    ResourceAssignment ideal = new ResourceAssignment(resource);
    ideal.addReplicaMap(new Partition(existing),
        Collections.singletonMap(_testInstanceId, "MASTER"));
    ideal.addReplicaMap(new Partition(pending),
        Collections.singletonMap(_testInstanceId, "MASTER"));
    Map<String, ResourceAssignment> idealAssignment = new HashMap<>();
    idealAssignment.put(resource, ideal);

    ResourceAssignment current = new ResourceAssignment(resource);
    current.addReplicaMap(new Partition(existing),
        Collections.singletonMap(_testInstanceId, "MASTER"));
    Map<String, ResourceAssignment> currentAssignment = new HashMap<>();
    currentAssignment.put(resource, current);

    ClusterModel model = ClusterModelProvider.generateClusterModelForPartialRebalance(cache,
        resourceMap(), new HashSet<>(Collections.singletonList(_testInstanceId)), idealAssignment,
        currentAssignment);

    AssignableNode node = model.getAssignableNodes().get(_testInstanceId);
    Assert.assertNotNull(node);

    Assert.assertTrue(totalUnallocatedOccupancy(model) > 0,
        "the instance must be carrying unaccounted occupancy, otherwise the constraint would be "
            + "inert here and this test would prove nothing about what happens when it is active");

    Set<String> scored = model.getAssignableReplicaMap().values().stream()
        .flatMap(Set::stream)
        .map(r -> r.getResourceName() + "|" + r.getPartitionName())
        .collect(Collectors.toSet());

    // Without this guard the exclusion below would pass trivially against an empty set.
    Assert.assertTrue(scored.contains(resource + "|" + pending),
        "the pending placement must actually be up for scoring, otherwise the exclusion of the "
            + "existing placement proves nothing; scored was " + scored);

    Assert.assertFalse(scored.contains(resource + "|" + existing),
        "an existing placement matching the ideal assignment must never be offered to the soft "
            + "constraints, no matter how heavily its instance is penalised; scored was " + scored);

    Set<String> held = node.getAssignedReplicas().stream()
        .map(r -> r.getResourceName() + "|" + r.getPartitionName())
        .collect(Collectors.toSet());
    Assert.assertTrue(held.contains(resource + "|" + existing),
        "the existing placement must instead be pre-assigned to the node before any constraint "
            + "runs; the node held " + held);
  }

  /**
   * The collection must agree with the capacity check about which replicas exist. The capacity
   * check reads CurrentStateOutput, which drops partitions no longer belonging to the resource, so
   * a stale current-state entry must not be charged here either -- charging it would reserve
   * capacity that nothing else believes is in use.
   */
  @Test
  public void testStalePartitionNotInResourceIsNotCharged() throws IOException {
    ResourceControllerDataProvider cache = setupClusterDataCache();
    Map<String, ResourceAssignment> emptyAssignment = new HashMap<>();
    for (String resourceName : _resourceNames) {
      emptyAssignment.put(resourceName, new ResourceAssignment(resourceName));
    }

    // Resources that declare none of the partitions the instance actually holds.
    Map<String, Resource> emptyResources = new HashMap<>();
    for (String resourceName : _resourceNames) {
      emptyResources.put(resourceName, new Resource(resourceName));
    }

    ClusterModel model = ClusterModelProvider.generateClusterModelForPartialRebalance(cache,
        emptyResources, new HashSet<>(Collections.singletonList(_testInstanceId)), emptyAssignment,
        emptyAssignment);

    Assert.assertEquals(totalUnallocatedOccupancy(model), 0,
        "a partition the resource no longer declares must not be charged");
  }
}
