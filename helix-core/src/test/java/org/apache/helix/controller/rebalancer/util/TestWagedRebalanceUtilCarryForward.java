package org.apache.helix.controller.rebalancer.util;

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

import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.apache.helix.controller.rebalancer.waged.RebalanceAlgorithm;
import org.apache.helix.controller.rebalancer.waged.model.ClusterContext;
import org.apache.helix.controller.rebalancer.waged.model.ClusterModel;
import org.apache.helix.controller.rebalancer.waged.model.OptimalAssignment;
import org.apache.helix.model.Partition;
import org.apache.helix.model.ResourceAssignment;
import org.mockito.Mockito;
import org.testng.Assert;
import org.testng.annotations.Test;

/**
 * Covers what {@code calculateAssignment} does with the resources instance tag isolation skipped:
 * the previous assignment is carried forward, so a resource is never emitted half assigned.
 */
public class TestWagedRebalanceUtilCarryForward {
  private static final String PARTITION = "Resource_0";

  @Test
  public void testSkippedResourceKeepsItsPreviousAssignment() throws Exception {
    Map<String, ResourceAssignment> calculated = new HashMap<>();
    // A skipped resource can still leave a partial entry behind, because the nodes are pre-loaded
    // with the replicas that were already allocated.
    calculated.put("Skipped", assignment("Skipped", "instance-1"));
    calculated.put("Healthy", assignment("Healthy", "instance-2"));

    Map<String, ResourceAssignment> previous = new HashMap<>();
    previous.put("Skipped", assignment("Skipped", "instance-9"));

    Map<String, ResourceAssignment> result =
        calculate(calculated, Collections.singleton("Skipped"), previous);

    Assert.assertEquals(instancesOf(result.get("Skipped")), Collections.singleton("instance-9"),
        "The skipped resource keeps what it had, not the half calculated entry");
    Assert.assertEquals(instancesOf(result.get("Healthy")), Collections.singleton("instance-2"),
        "An unrelated resource keeps its freshly calculated assignment");
  }

  @Test
  public void testSkippedResourceIsDroppedWhenThereIsNoPreviousAssignment() throws Exception {
    Map<String, ResourceAssignment> calculated = new HashMap<>();
    calculated.put("Skipped", assignment("Skipped", "instance-1"));
    calculated.put("Healthy", assignment("Healthy", "instance-2"));

    Map<String, ResourceAssignment> result =
        calculate(calculated, Collections.singleton("Skipped"), Collections.emptyMap());

    Assert.assertFalse(result.containsKey("Skipped"),
        "With nothing to carry forward the partial entry is dropped, never emitted half assigned");
    Assert.assertTrue(result.containsKey("Healthy"));
  }

  @Test(expectedExceptions = NullPointerException.class)
  public void testNullPreviousAssignmentIsRejected() throws Exception {
    // Rejected even when nothing was skipped, so a wrong caller fails on its first run.
    calculate(new HashMap<>(), Collections.emptySet(), null);
  }

  @Test
  public void testWholePreviousEntryIsRestoredOverAPartialOne() throws Exception {
    ResourceAssignment previousEntry = new ResourceAssignment("Skipped");
    previousEntry.addReplicaMap(new Partition("Skipped_0"),
        replicas("instance-1", "MASTER", "instance-2", "SLAVE"));
    previousEntry.addReplicaMap(new Partition("Skipped_1"),
        replicas("instance-2", "MASTER", "instance-3", "SLAVE"));
    previousEntry.addReplicaMap(new Partition("Skipped_2"),
        replicas("instance-3", "MASTER", "instance-1", "OFFLINE"));
    Map<String, Map<String, String>> expected = copyOf(previousEntry);

    // The partial entry holds a subset: one partition, one replica, and a different state.
    ResourceAssignment partial = new ResourceAssignment("Skipped");
    partial.addReplicaMap(new Partition("Skipped_0"), replicas("instance-1", "SLAVE"));
    Map<String, ResourceAssignment> calculated = new HashMap<>();
    calculated.put("Skipped", partial);

    Map<String, ResourceAssignment> result = calculate(calculated,
        Collections.singleton("Skipped"), Collections.singletonMap("Skipped", previousEntry));

    Assert.assertEquals(result.get("Skipped").getRecord().getMapFields(), expected,
        "Every partition and every replica state of the previous entry is restored");
  }

  @Test
  public void testChangingTheResultLeavesThePreviousAssignmentAlone() throws Exception {
    ResourceAssignment previousEntry = new ResourceAssignment("Skipped");
    previousEntry.addReplicaMap(new Partition("Skipped_0"),
        replicas("instance-1", "MASTER", "instance-2", "SLAVE"));
    Map<String, Map<String, String>> expected = copyOf(previousEntry);

    Map<String, ResourceAssignment> calculated = new HashMap<>();
    calculated.put("Skipped", assignment("Skipped", "instance-1"));
    Map<String, ResourceAssignment> result = calculate(calculated,
        Collections.singleton("Skipped"), Collections.singletonMap("Skipped", previousEntry));

    ResourceAssignment carried = result.get("Skipped");
    Assert.assertNotSame(carried, previousEntry);
    Map<String, String> replicaMap = carried.getReplicaMap(new Partition("Skipped_0"));
    replicaMap.put("instance-1", "SLAVE");
    replicaMap.remove("instance-2");
    replicaMap.put("instance-3", "SLAVE");
    carried.addReplicaMap(new Partition("Skipped_1"), replicas("instance-4", "MASTER"));

    Assert.assertEquals(previousEntry.getRecord().getMapFields(), expected,
        "Writes through the result must not reach the caller's previous assignment");
  }

  @Test
  public void testNothingChangesWhenNoResourceWasSkipped() throws Exception {
    Map<String, ResourceAssignment> calculated = new HashMap<>();
    calculated.put("Healthy", assignment("Healthy", "instance-2"));

    Map<String, ResourceAssignment> previous = new HashMap<>();
    previous.put("Healthy", assignment("Healthy", "instance-5"));

    Map<String, ResourceAssignment> result =
        calculate(calculated, Collections.emptySet(), previous);

    Assert.assertEquals(instancesOf(result.get("Healthy")), Collections.singleton("instance-2"),
        "The default global path is untouched");
  }

  private static Map<String, ResourceAssignment> calculate(
      Map<String, ResourceAssignment> calculated, Set<String> skipped,
      Map<String, ResourceAssignment> previous) throws Exception {
    OptimalAssignment optimalAssignment = Mockito.mock(OptimalAssignment.class);
    Mockito.when(optimalAssignment.getOptimalResourceAssignment()).thenReturn(calculated);
    Mockito.when(optimalAssignment.getSkippedResources()).thenReturn(skipped);

    RebalanceAlgorithm algorithm = Mockito.mock(RebalanceAlgorithm.class);
    Mockito.when(algorithm.calculate(Mockito.any())).thenReturn(optimalAssignment);

    ClusterContext context = Mockito.mock(ClusterContext.class);
    Mockito.when(context.getClusterName()).thenReturn("TestCluster");
    ClusterModel clusterModel = Mockito.mock(ClusterModel.class);
    Mockito.when(clusterModel.getContext()).thenReturn(context);

    return WagedRebalanceUtil.calculateAssignment(clusterModel, algorithm, previous);
  }

  private static ResourceAssignment assignment(String resource, String instance) {
    ResourceAssignment resourceAssignment = new ResourceAssignment(resource);
    resourceAssignment.addReplicaMap(new Partition(PARTITION),
        Collections.singletonMap(instance, "MASTER"));
    return resourceAssignment;
  }

  private static Map<String, String> replicas(String... instanceStatePairs) {
    Map<String, String> replicaMap = new HashMap<>();
    for (int i = 0; i < instanceStatePairs.length; i += 2) {
      replicaMap.put(instanceStatePairs[i], instanceStatePairs[i + 1]);
    }
    return replicaMap;
  }

  private static Map<String, Map<String, String>> copyOf(ResourceAssignment resourceAssignment) {
    Map<String, Map<String, String>> copy = new HashMap<>();
    resourceAssignment.getRecord()
        .getMapFields()
        .forEach((partition, replicas) -> copy.put(partition, new HashMap<>(replicas)));
    return copy;
  }

  private static Set<String> instancesOf(ResourceAssignment resourceAssignment) {
    Assert.assertNotNull(resourceAssignment, "Expected the resource to be present in the result");
    Set<String> instances = new HashSet<>();
    resourceAssignment.getMappedPartitions()
        .forEach(partition -> instances.addAll(
            resourceAssignment.getReplicaMap(partition).keySet()));
    return instances;
  }
}
