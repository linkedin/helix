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
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import org.apache.helix.HelixRebalanceException;
import org.apache.helix.controller.rebalancer.util.WagedRebalanceUtil;
import org.apache.helix.model.ClusterConfig;
import org.apache.helix.model.Partition;
import org.apache.helix.model.ResourceAssignment;
import org.testng.Assert;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

public class TestWagedIsolationIncrementalScopes extends AbstractTestWagedInstanceTagIsolation {
  @DataProvider(name = "scopes")
  public Object[][] scopes() {
    return Arrays.stream(ClusterModel.RebalanceScopeType.values())
        .map(scope -> new Object[] {scope}).toArray(Object[][]::new);
  }

  @Test(dataProvider = "scopes")
  public void testOnlyOutstandingCliqueCanFailWithoutFailingTheCluster(
      ClusterModel.RebalanceScopeType scope) throws Exception {
    ClusterModel model = sparseModel(scope, true, cliqueTag(1));
    OptimalAssignment result = createAlgorithm().calculate(model);
    Assert.assertEquals(result.getSkippedResources(), Collections.singleton(resourceName(0)));
    Assert.assertEquals(result.getOptimalResourceAssignment().keySet(),
        new HashSet<>(Arrays.asList(resourceName(1), resourceName(2))));
  }

  @Test(dataProvider = "scopes", expectedExceptions = HelixRebalanceException.class)
  public void testSparseFailureStillThrowsWithFlagOff(ClusterModel.RebalanceScopeType scope)
      throws Exception {
    createAlgorithm().calculate(sparseModel(scope, false, cliqueTag(1)));
  }

  @Test(dataProvider = "scopes", expectedExceptions = HelixRebalanceException.class)
  public void testAllocatedUntaggedResourceStillConnectsTheCluster(
      ClusterModel.RebalanceScopeType scope) throws Exception {
    createAlgorithm().calculate(sparseModel(scope, true, null));
  }

  @Test(dataProvider = "scopes")
  public void testAllocatedSiblingIsIncludedInTheSkippedSet(ClusterModel.RebalanceScopeType scope)
      throws Exception {
    OptimalAssignment result =
        createAlgorithm().calculate(sparseModel(scope, true, cliqueTag(0)));
    Assert.assertEquals(result.getSkippedResources(),
        new HashSet<>(Arrays.asList(resourceName(0), resourceName(1))));
    Assert.assertTrue(result.getOptimalResourceAssignment().containsKey(resourceName(2)));
  }

  @Test(dataProvider = "scopes")
  public void testSparseCarryForwardKeepsCompletePreviousAssignment(
      ClusterModel.RebalanceScopeType scope) throws Exception {
    Map<String, ResourceAssignment> previous = new HashMap<>();
    ResourceAssignment old = new ResourceAssignment(resourceName(0));
    old.addReplicaMap(new Partition(resourceName(0) + "_0"),
        Collections.singletonMap(instanceName(0, 0), "ONLINE"));
    previous.put(resourceName(0), old);
    Map<String, ResourceAssignment> result = WagedRebalanceUtil.calculateAssignment(
        sparseModel(scope, true, cliqueTag(1)), createAlgorithm(),
        scope == ClusterModel.RebalanceScopeType.DELAYED_REBALANCE_OVERWRITES ? null : previous);
    if (scope == ClusterModel.RebalanceScopeType.DELAYED_REBALANCE_OVERWRITES) {
      Assert.assertFalse(result.containsKey(resourceName(0)));
    } else {
      Assert.assertEquals(result.get(resourceName(0)).getRecord().getMapFields(),
          old.getRecord().getMapFields());
      Assert.assertNotSame(result.get(resourceName(0)), old);
    }
    Assert.assertTrue(result.containsKey(resourceName(2)));
  }

  private ClusterModel sparseModel(ClusterModel.RebalanceScopeType scope, boolean enabled,
      String allocatedTag) throws Exception {
    return sparseModel(scope, enabled, allocatedTag, UNPLACEABLE_PARTITION_WEIGHT);
  }

  private ClusterModel sparseModel(ClusterModel.RebalanceScopeType scope, boolean enabled,
      String allocatedTag, int outstandingWeight) throws Exception {
    return sparseModel(scope, enabled, allocatedTag, outstandingWeight, false);
  }

  private ClusterModel sparseModel(ClusterModel.RebalanceScopeType scope, boolean enabled,
      String allocatedTag, int outstandingWeight, boolean retrySibling) throws Exception {
    ClusterConfig config = createClusterConfig(enabled);
    Set<AssignableNode> nodes = new HashSet<>();
    Map<Integer, AssignableNode> firstNode = new HashMap<>();
    for (int clique = 0; clique < 3; clique++) {
      for (int index = 0; index < 2; index++) {
        AssignableNode node = taggedNode(config, instanceName(clique, index), index,
            cliqueTag(clique), "shared_operational_label");
        nodes.add(node);
        if (index == 0) {
          firstNode.put(clique, node);
        }
      }
    }
    Set<AssignableReplica> outstanding = new HashSet<>();
    addReplicas(outstanding, config,
        taggedResource(resourceName(0), cliqueTag(0), outstandingWeight), 1);
    Set<AssignableReplica> all = new HashSet<>(outstanding);
    for (int clique = 1; clique < 3; clique++) {
      String tag = clique == 1 ? allocatedTag : cliqueTag(clique);
      AssignableReplica replica = new AssignableReplica(config,
          taggedResource(resourceName(clique), tag, HEALTHY_PARTITION_WEIGHT),
          resourceName(clique) + "_0", "ONLINE", 0);
      all.add(replica);
      int owner = clique == 1 && cliqueTag(0).equals(tag) ? 0 : clique;
      if (clique == 1 && retrySibling) {
        outstanding.add(replica);
      } else {
        firstNode.get(owner).assignInitBatch(Collections.singleton(replica));
      }
    }
    ClusterContext context =
        new ClusterContext(all, nodes, Collections.emptyMap(), Collections.emptyMap(), config);
    return new ClusterModel(context, outstanding, nodes, scope);
  }
}
