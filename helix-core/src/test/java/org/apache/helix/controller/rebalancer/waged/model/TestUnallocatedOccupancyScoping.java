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
import org.apache.helix.model.IdealState;
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
