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
 * the previous assignment is carried forward, and a resource whose brand new assignment collides
 * with a carried forward one gives its own calculation up as well.
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

  @Test
  public void testNullPreviousAssignmentDropsTheSkippedResource() throws Exception {
    Map<String, ResourceAssignment> calculated = new HashMap<>();
    calculated.put("Skipped", assignment("Skipped", "instance-1"));

    Map<String, ResourceAssignment> result =
        calculate(calculated, Collections.singleton("Skipped"), null);

    Assert.assertTrue(result.isEmpty(),
        "A null previous assignment means absent, which is what the overwrite phase wants");
  }

  @Test
  public void testResourceReusingACarriedOverInstanceIsYieldedToo() throws Exception {
    // "Skipped" is carried forward onto instance-1. "Mover" was just calculated onto that same
    // instance-1, which is only possible because it no longer belongs to the skipped group. Letting
    // both land there would double book the node, so Mover goes back to what it had.
    Map<String, ResourceAssignment> calculated = new HashMap<>();
    calculated.put("Mover", assignment("Mover", "instance-1"));
    calculated.put("Unrelated", assignment("Unrelated", "instance-7"));

    Map<String, ResourceAssignment> previous = new HashMap<>();
    previous.put("Skipped", assignment("Skipped", "instance-1"));
    previous.put("Mover", assignment("Mover", "instance-3"));
    previous.put("Unrelated", assignment("Unrelated", "instance-8"));

    Map<String, ResourceAssignment> result =
        calculate(calculated, Collections.singleton("Skipped"), previous);

    Assert.assertEquals(instancesOf(result.get("Skipped")), Collections.singleton("instance-1"));
    Assert.assertEquals(instancesOf(result.get("Mover")), Collections.singleton("instance-3"),
        "The colliding resource gives up its new assignment");
    Assert.assertEquals(instancesOf(result.get("Unrelated")), Collections.singleton("instance-7"),
        "Everything that does not collide keeps its freshly calculated assignment");
  }

  @Test
  public void testNoCollisionWhenTheCarriedOverInstanceIsUntouched() throws Exception {
    Map<String, ResourceAssignment> calculated = new HashMap<>();
    calculated.put("Mover", assignment("Mover", "instance-4"));

    Map<String, ResourceAssignment> previous = new HashMap<>();
    previous.put("Skipped", assignment("Skipped", "instance-1"));
    previous.put("Mover", assignment("Mover", "instance-3"));

    Map<String, ResourceAssignment> result =
        calculate(calculated, Collections.singleton("Skipped"), previous);

    Assert.assertEquals(instancesOf(result.get("Mover")), Collections.singleton("instance-4"),
        "No shared instance means no reason to give the new assignment up");
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

  private static Set<String> instancesOf(ResourceAssignment resourceAssignment) {
    Assert.assertNotNull(resourceAssignment, "Expected the resource to be present in the result");
    Set<String> instances = new HashSet<>();
    resourceAssignment.getMappedPartitions()
        .forEach(partition -> instances.addAll(resourceAssignment.getReplicaMap(partition).keySet()));
    return instances;
  }
}
