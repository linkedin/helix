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
import org.apache.helix.controller.rebalancer.waged.model.AssignableReplica;
import org.apache.helix.controller.rebalancer.waged.model.ClusterContext;
import org.apache.helix.controller.rebalancer.waged.model.ClusterModel;
import org.apache.helix.controller.rebalancer.waged.model.OptimalAssignment;
import org.apache.helix.model.Partition;
import org.apache.helix.model.ResourceAssignment;
import org.mockito.Mockito;
import org.testng.Assert;
import org.testng.annotations.Test;

/**
 * The delayed rebalance overwrite phase is the one scope where a skipped resource must be DROPPED
 * rather than carried forward, and these tests pin that down end to end.
 *
 * <p>{@code WagedRebalancer.handleDelayedRebalanceMinActiveReplica} runs three statements back to
 * back:
 *
 * <pre>
 *   assignment = WagedRebalanceUtil.calculateAssignment(clusterModel, algorithm, null);
 *   assignment.keySet().retainAll(clusterModel.getAssignableReplicaMap().keySet());
 *   DelayedRebalanceUtil.mergeAssignments(assignment, currentResourceAssignment);
 * </pre>
 *
 * <p>The {@code null} in the first line is load bearing. Unlike every other phase, this one merges
 * its result into the assignment that emergency and partial just computed, so carrying an older
 * snapshot forward for a skipped resource would push stale placements over fresh work. Dropping
 * leaves the skipped clique exactly where it was, which is the same place it would be if the
 * overwrite phase had never run at all.
 *
 * <p>The individual pieces are covered elsewhere ({@code TestWagedRebalanceUtilCarryForward}
 * pins the drop, {@code TestWagedInstanceTagIsolationBehavior} pins the algorithm across every
 * scope). What is only true of the three composed is what these tests assert.
 */
public class TestWagedIsolationDelayedOverwriteMerge {
  private static final String PARTITION = "_0";
  private static final String BROKEN = "Resource_clique_broken";
  private static final String BREACHED = "Resource_clique_breached";

  /**
   * The headline guarantee: the clique isolation skipped keeps byte for byte the assignment it
   * already had, and the clique that merely needed a top-up still gets one.
   */
  @Test
  public void testSkippedCliqueIsLeftUntouchedWhileTheOthersAreToppedUp() throws Exception {
    // What emergency and partial already produced and persisted. This is what the overwrite phase
    // merges into, and what must survive for the skipped clique.
    Map<String, ResourceAssignment> served = new HashMap<>();
    served.put(BROKEN, assignment(BROKEN, "broken-node-1"));
    served.put(BREACHED, assignment(BREACHED, "breached-node-1"));

    // The overwrite calculation: the breached clique gets a fresh node, the broken one is skipped
    // but still leaves a partial entry behind, because the nodes are pre-loaded with the replicas
    // that were already allocated.
    Map<String, ResourceAssignment> calculated = new HashMap<>();
    calculated.put(BROKEN, assignment(BROKEN, "partial-garbage"));
    calculated.put(BREACHED, assignment(BREACHED, "breached-node-2"));

    Map<String, ResourceAssignment> result =
        runOverwritePhase(calculated, Collections.singleton(BROKEN), served);

    Assert.assertEquals(instancesOf(result.get(BROKEN)),
        Collections.singleton("broken-node-1"),
        "The skipped clique must keep exactly the assignment emergency and partial gave it. "
            + "Neither the half calculated overwrite entry nor an older snapshot may reach it.");
    Assert.assertEquals(instancesOf(result.get(BREACHED)),
        new HashSet<>(java.util.Arrays.asList("breached-node-1", "breached-node-2")),
        "The clique that only needed a minActiveReplica top-up must still receive it, which is "
            + "the entire point of not throwing on the broken one.");
  }

  /**
   * Control case. With nothing skipped the phase behaves exactly as it does today, so the flag is
   * what changes the behavior rather than this code path existing at all.
   */
  @Test
  public void testNothingSkippedLeavesTheOverwritePhaseUnchanged() throws Exception {
    Map<String, ResourceAssignment> served = new HashMap<>();
    served.put(BREACHED, assignment(BREACHED, "breached-node-1"));

    Map<String, ResourceAssignment> calculated = new HashMap<>();
    calculated.put(BREACHED, assignment(BREACHED, "breached-node-2"));

    Map<String, ResourceAssignment> result =
        runOverwritePhase(calculated, Collections.emptySet(), served);

    Assert.assertEquals(instancesOf(result.get(BREACHED)),
        new HashSet<>(java.util.Arrays.asList("breached-node-1", "breached-node-2")),
        "The default overwrite path must be untouched when isolation skipped nothing");
  }

  /**
   * Proves the {@code null} previous assignment is load bearing rather than incidental.
   *
   * <p>This runs the identical phase with the served assignment handed in as the previous one, the
   * way every other phase does it. Carry forward then reinstates a stale placement and
   * {@code mergeAssignments} pushes it over the fresh work. Without this test a future refactor
   * that "helpfully" threads {@code currentResourceAssignment} through would look harmless and
   * would silently corrupt the served assignment inside the delay window.
   */
  @Test
  public void testCarryingForwardInsteadOfDroppingWouldCorruptTheServedAssignment()
      throws Exception {
    Map<String, ResourceAssignment> served = new HashMap<>();
    served.put(BROKEN, assignment(BROKEN, "fresh-node-from-emergency"));

    // The stale snapshot the previous pass left behind.
    Map<String, ResourceAssignment> stale = new HashMap<>();
    stale.put(BROKEN, assignment(BROKEN, "stale-node"));

    Map<String, ResourceAssignment> calculated = new HashMap<>();
    calculated.put(BROKEN, assignment(BROKEN, "partial-garbage"));

    Map<String, ResourceAssignment> corrupted =
        runOverwritePhase(calculated, Collections.singleton(BROKEN), served, stale);

    Assert.assertTrue(instancesOf(corrupted.get(BROKEN)).contains("stale-node"),
        "This is the failure mode the null guards against: a carried forward snapshot merging a "
            + "stale placement over what emergency just computed.");

    // And the real phase, with null, does not do that.
    Map<String, ResourceAssignment> servedAgain = new HashMap<>();
    servedAgain.put(BROKEN, assignment(BROKEN, "fresh-node-from-emergency"));
    Map<String, ResourceAssignment> calculatedAgain = new HashMap<>();
    calculatedAgain.put(BROKEN, assignment(BROKEN, "partial-garbage"));

    Map<String, ResourceAssignment> clean =
        runOverwritePhase(calculatedAgain, Collections.singleton(BROKEN), servedAgain);

    Assert.assertEquals(instancesOf(clean.get(BROKEN)),
        Collections.singleton("fresh-node-from-emergency"),
        "The shipped phase passes null, so nothing stale can reach the served assignment");
  }

  /**
   * Replays the three statements of handleDelayedRebalanceMinActiveReplica against the served
   * assignment and returns what the controller would go on to use.
   */
  private static Map<String, ResourceAssignment> runOverwritePhase(
      Map<String, ResourceAssignment> calculated, Set<String> skipped,
      Map<String, ResourceAssignment> served) throws Exception {
    return runOverwritePhase(calculated, skipped, served, null);
  }

  private static Map<String, ResourceAssignment> runOverwritePhase(
      Map<String, ResourceAssignment> calculated, Set<String> skipped,
      Map<String, ResourceAssignment> served, Map<String, ResourceAssignment> previous)
      throws Exception {
    OptimalAssignment optimalAssignment = Mockito.mock(OptimalAssignment.class);
    Mockito.when(optimalAssignment.getOptimalResourceAssignment()).thenReturn(calculated);
    Mockito.when(optimalAssignment.getSkippedResources()).thenReturn(skipped);

    RebalanceAlgorithm algorithm = Mockito.mock(RebalanceAlgorithm.class);
    Mockito.when(algorithm.calculate(Mockito.any())).thenReturn(optimalAssignment);

    ClusterContext context = Mockito.mock(ClusterContext.class);
    Mockito.when(context.getClusterName()).thenReturn("TestCluster");

    // Everything the overwrite model carries is outstanding, which is what retainAll keeps.
    Map<String, Set<AssignableReplica>> replicaMap = new HashMap<>();
    for (String resource : calculated.keySet()) {
      replicaMap.put(resource, Collections.emptySet());
    }

    ClusterModel clusterModel = Mockito.mock(ClusterModel.class);
    Mockito.when(clusterModel.getContext()).thenReturn(context);
    Mockito.when(clusterModel.getRebalanceScopeType())
        .thenReturn(ClusterModel.RebalanceScopeType.DELAYED_REBALANCE_OVERWRITES);
    Mockito.when(clusterModel.getAssignableReplicaMap()).thenReturn(replicaMap);

    Map<String, ResourceAssignment> assignment =
        WagedRebalanceUtil.calculateAssignment(clusterModel, algorithm, previous);
    assignment.keySet().retainAll(clusterModel.getAssignableReplicaMap().keySet());
    DelayedRebalanceUtil.mergeAssignments(assignment, served);
    return served;
  }

  private static ResourceAssignment assignment(String resource, String instance) {
    ResourceAssignment resourceAssignment = new ResourceAssignment(resource);
    resourceAssignment.addReplicaMap(new Partition(resource + PARTITION),
        Collections.singletonMap(instance, "MASTER"));
    return resourceAssignment;
  }

  private static Set<String> instancesOf(ResourceAssignment resourceAssignment) {
    Assert.assertNotNull(resourceAssignment, "Expected the resource to be present in the result");
    Set<String> instances = new HashSet<>();
    for (Partition partition : resourceAssignment.getMappedPartitions()) {
      instances.addAll(resourceAssignment.getReplicaMap(partition).keySet());
    }
    return instances;
  }
}
