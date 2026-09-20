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

import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import org.apache.helix.HelixRebalanceException;
import org.apache.helix.model.ClusterConfig;
import org.apache.helix.model.Partition;
import org.apache.helix.model.ResourceAssignment;
import org.testng.Assert;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

/**
 * Capacity deficit attribution in the scopes whose model does not carry every replica.
 *
 * The emergency model only holds the replicas that sit on a down instance plus the ones already
 * placed on an active node, and the delayed overwrite model is sparser still. A resource that has
 * never been placed, typically the one that broke its clique, is in neither, yet it is part of the
 * cluster wide demand the precheck starts from. In both cases below such a resource sits in a
 * broken clique, and a healthy clique must still be able to fail over.
 */
public class TestWagedIsolationUnplacedDemand extends AbstractTestWagedInstanceTagIsolation {
  private static final String HEALTHY = resourceName(0);
  private static final String BROKEN_PLACED = resourceName(1);
  private static final String BROKEN_NEVER_PLACED = "Resource_clique_1_new";
  // Five of these on a clique of one node is far more than it can hold, and far more than the
  // whole cluster has spare, so the tag blind precheck fails before any replica is placed.
  private static final int NEVER_PLACED_PARTITIONS = 5;
  private static final int BROKEN_WEIGHT = 80;

  @DataProvider(name = "sparseScopes")
  public Object[][] sparseScopes() {
    return new Object[][] {
        {ClusterModel.RebalanceScopeType.EMERGENCY},
        {ClusterModel.RebalanceScopeType.DELAYED_REBALANCE_OVERWRITES}
    };
  }

  /**
   * The broken clique is over committed by the replicas already on its node, so it is set aside,
   * and its never placed resource must leave the cluster wide demand with it, not only those
   * replicas. Charged to the healthy rest, that resource would make it look short and rethrow.
   */
  @Test(dataProvider = "sparseScopes")
  public void testSetAsideCliqueTakesItsUnplacedReplicasWithIt(
      ClusterModel.RebalanceScopeType scope) throws Exception {
    assertHealthyCliqueIsPlaced(createAlgorithm().calculate(model(scope, true, true)));
  }

  /**
   * The broken clique's placed replicas fit, so only its never placed resource over commits it.
   * That resource is outside the scope's model, so the attribution must still count it toward its
   * clique. Judged by the model alone the clique would look healthy, nothing would be attributed,
   * and the deficit would be reported as a cluster wide shortfall.
   */
  @Test(dataProvider = "sparseScopes")
  public void testCliqueOverCommittedOnlyByUnplacedReplicasIsAttributed(
      ClusterModel.RebalanceScopeType scope) throws Exception {
    assertHealthyCliqueIsPlaced(createAlgorithm().calculate(model(scope, true, false)));
  }

  /**
   * The same clique in a baseline, where every replica is either placed or waiting to be placed,
   * is attributed from the model alone. It is the control for the two cases above.
   */
  @Test
  public void testBaselineAttributesTheSameClique() throws Exception {
    assertHealthyCliqueIsPlaced(createAlgorithm().calculate(
        model(ClusterModel.RebalanceScopeType.GLOBAL_BASELINE, true, true)));
    assertHealthyCliqueIsPlaced(createAlgorithm().calculate(
        model(ClusterModel.RebalanceScopeType.GLOBAL_BASELINE, true, false)));
  }

  @Test(dataProvider = "sparseScopes")
  public void testFlagOffStillThrows(ClusterModel.RebalanceScopeType scope) throws Exception {
    for (boolean overPlaced : new boolean[] {true, false}) {
      try {
        createAlgorithm().calculate(model(scope, false, overPlaced));
        Assert.fail("The default mode must fail on the cluster wide deficit");
      } catch (HelixRebalanceException e) {
        Assert.assertEquals(e.getFailureCategory(),
            HelixRebalanceException.FailureCategory.CAPACITY_DEFICIT);
      }
    }
  }

  private static void assertHealthyCliqueIsPlaced(OptimalAssignment result) {
    Assert.assertEquals(result.getSkippedResources(),
        new HashSet<>(Arrays.asList(BROKEN_PLACED, BROKEN_NEVER_PLACED)));
    Map<String, ResourceAssignment> assignment = result.getOptimalResourceAssignment();
    // The skipped resources are replaced by their previous assignment one layer up.
    Set<String> computed = new HashSet<>(assignment.keySet());
    computed.removeAll(result.getSkippedResources());
    Assert.assertEquals(computed, Collections.singleton(HEALTHY));
    Map<String, String> replicas =
        assignment.get(HEALTHY).getReplicaMap(new Partition(HEALTHY + "_0"));
    Assert.assertEquals(replicas.size(), 1);
    String instance = replicas.keySet().iterator().next();
    Assert.assertTrue(instance.startsWith("instance_0_"), instance);
  }

  /**
   * Clique 0 has two nodes and one replica waiting to be placed, as if its instance just went down.
   * Clique 1 has one node holding two replicas of a resource, which overflow it when overPlaced is
   * set, and a second resource that has never been placed anywhere.
   */
  private ClusterModel model(ClusterModel.RebalanceScopeType scope, boolean enabled,
      boolean overPlaced) throws Exception {
    ClusterConfig config = createClusterConfig(enabled);
    Set<AssignableNode> nodes = new HashSet<>();
    nodes.add(taggedNode(config, instanceName(0, 0), 0, cliqueTag(0)));
    nodes.add(taggedNode(config, instanceName(0, 1), 1, cliqueTag(0)));
    AssignableNode brokenNode = taggedNode(config, instanceName(1, 0), 0, cliqueTag(1));
    nodes.add(brokenNode);

    Set<AssignableReplica> toBeAssigned = new HashSet<>();
    addReplicas(toBeAssigned, config,
        taggedResource(HEALTHY, cliqueTag(0), HEALTHY_PARTITION_WEIGHT), 1);
    Set<AssignableReplica> placed = new HashSet<>();
    addReplicas(placed, config,
        taggedResource(BROKEN_PLACED, cliqueTag(1), overPlaced ? BROKEN_WEIGHT : 20), 2);
    brokenNode.assignInitBatch(placed);
    Set<AssignableReplica> neverPlaced = new HashSet<>();
    addReplicas(neverPlaced, config,
        taggedResource(BROKEN_NEVER_PLACED, cliqueTag(1), BROKEN_WEIGHT), NEVER_PLACED_PARTITIONS);

    Set<AssignableReplica> all = new HashSet<>(toBeAssigned);
    all.addAll(placed);
    all.addAll(neverPlaced);
    if (scope == ClusterModel.RebalanceScopeType.GLOBAL_BASELINE) {
      toBeAssigned.addAll(neverPlaced);
    }
    ClusterContext context =
        new ClusterContext(all, nodes, Collections.emptyMap(), Collections.emptyMap(), config);
    return new ClusterModel(context, toBeAssigned, nodes, scope);
  }
}
