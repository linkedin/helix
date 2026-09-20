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
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.helix.HelixRebalanceException;
import org.apache.helix.controller.rebalancer.util.WagedRebalanceUtil;
import org.apache.helix.model.ClusterConfig;
import org.apache.helix.model.Partition;
import org.apache.helix.model.ResourceAssignment;
import org.apache.helix.model.ResourceConfig;
import org.testng.Assert;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

/**
 * Instance tag isolation on clusters that also have nodes no resource is pinned to: a spare or
 * standby pool, or a freshly provisioned clique whose resources do not exist yet.
 *
 * Such a tag holds no work, so it can never fail and it can never be blamed for a shortfall. If it
 * counted as an independent block it would survive every failure, and a failure of every clique
 * that actually holds resources would come back as "isolated" instead of failing the rebalance the
 * way the default mode does, which silences the failure counters and the last known good fallback
 * for exactly the failures they exist for. Most tests here run in all four rebalance scopes: a
 * total failure must throw exactly what the default mode throws, and a failure contained to some
 * cliques must still be isolated with such nodes present. The ones with stale replicas on a spare
 * node that no block reaches run in the partial, emergency and delayed overwrite scopes, whose
 * model keeps a replica on the node it sits on. See {@link AbstractTestWagedInstanceTagIsolation}
 * for the shared fixture.
 *
 * A resource pinned to a tag that no node carries is the same case from the other side, such as a
 * resource added before any instance carries its tag, or a clique whose instances were all
 * retagged away. With nothing to place it never fails either, so it must not turn a failure of
 * everything that reaches a node into an isolated one. When it does have a partition to place,
 * that failure is still set aside on its own, as long as some group that reaches a node is left to
 * keep rebalancing. When no group reaches a node at all, the round fails as the default mode does.
 */
public class TestWagedIsolationNodeOnlyBlocks extends AbstractTestWagedInstanceTagIsolation {
  private static final String STANDBY_TAG = "standby";
  // Fits one node on its own, but five of them oversubscribe a two node clique.
  private static final int HEAVY_PARTITION_WEIGHT = 90;
  // A resource pinned to a tag that no node carries.
  private static final String GHOST = "GHOST";
  private static final String GHOST_TAG = "ghost";
  private static final Pattern INNERMOST_MAP = Pattern.compile("\\{([^{}]*)\\}");

  /** The tags of a spare node, which no resource is pinned to. */
  enum Spare {
    /** A tag of its own that no resource uses, which makes the node a block of its own. */
    STANDBY,
    /** No tag at all, so no block reaches the node. */
    UNTAGGED,
    /** Only a zone label that clique nodes carry too, so no block reaches the node either. */
    LABELLED
  }

  /** Where the replica of {@link #GHOST}, which has nothing to place, sits. */
  enum Ghost {
    /** On a clique 0 node, left behind when that node was retagged away from the ghost tag. */
    RETAGGED,
    /** On no node at all, as for a resource added before any instance carries its tag. */
    UNPLACED
  }

  @DataProvider(name = "scopes")
  public Object[][] scopes() {
    return Arrays.stream(ClusterModel.RebalanceScopeType.values())
        .map(scope -> new Object[] {scope}).toArray(Object[][]::new);
  }

  /** Every scope, with a spare node that no block reaches. */
  @DataProvider(name = "scopesAndUnreachedSpares")
  public Object[][] scopesAndUnreachedSpares() {
    return withUnreachedSpares(ClusterModel.RebalanceScopeType.values());
  }

  /**
   * The scopes whose model keeps a replica on the node it sits on, with a spare node that no block
   * reaches. A baseline places every replica afresh, so it never sees one left on a spare node.
   */
  @DataProvider(name = "staleScopesAndUnreachedSpares")
  public Object[][] staleScopesAndUnreachedSpares() {
    return withUnreachedSpares(ClusterModel.RebalanceScopeType.PARTIAL,
        ClusterModel.RebalanceScopeType.EMERGENCY,
        ClusterModel.RebalanceScopeType.DELAYED_REBALANCE_OVERWRITES);
  }

  private static Object[][] withUnreachedSpares(ClusterModel.RebalanceScopeType... scopes) {
    List<Object[]> rows = new ArrayList<>();
    for (ClusterModel.RebalanceScopeType scope : scopes) {
      rows.add(new Object[] {scope, Spare.UNTAGGED});
      rows.add(new Object[] {scope, Spare.LABELLED});
    }
    return rows.toArray(new Object[0][]);
  }

  /** Every scope, with each place the replica of {@link #GHOST} can sit. */
  @DataProvider(name = "scopesAndGhosts")
  public Object[][] scopesAndGhosts() {
    List<Object[]> rows = new ArrayList<>();
    for (ClusterModel.RebalanceScopeType scope : ClusterModel.RebalanceScopeType.values()) {
      for (Ghost ghost : Ghost.values()) {
        rows.add(new Object[] {scope, ghost});
      }
    }
    return rows.toArray(new Object[0][]);
  }

  /** Every scope, with each way the default mode fails the round. */
  @DataProvider(name = "scopesAndCategories")
  public Object[][] scopesAndCategories() {
    List<Object[]> rows = new ArrayList<>();
    for (ClusterModel.RebalanceScopeType scope : ClusterModel.RebalanceScopeType.values()) {
      rows.add(new Object[] {scope, HelixRebalanceException.FailureCategory.NO_CANDIDATE_NODE});
      rows.add(new Object[] {scope, HelixRebalanceException.FailureCategory.CAPACITY_DEFICIT});
    }
    return rows.toArray(new Object[0][]);
  }

  /** Builds the same cluster for either flag value, so the two runs start from equal models. */
  private interface ClusterBuilder {
    ClusterModel build(ClusterConfig config, ClusterModel.RebalanceScopeType scope)
        throws IOException;
  }

  // ---------------------------------------------------------------------------------------------
  // A failure of everything that holds work must fail exactly like the default mode
  // ---------------------------------------------------------------------------------------------

  /**
   * Every resource is pinned to one tag and cannot be placed, and one standby node carries a tag
   * no resource uses. Nothing in the cluster can keep rebalancing, so this is a total failure.
   */
  @Test(dataProvider = "scopes")
  public void testSingleCliqueFailureWithAStandbyNodeThrowsLikeTheDefaultMode(
      ClusterModel.RebalanceScopeType scope) throws IOException {
    assertThrowsLikeTheDefaultMode(scope, (config, s) -> singleClique(config, s, true),
        previousOnFirstNode("prod_0", "R1", "R2"));
  }

  /** The same cluster without the standby node, which already fails like the default mode. */
  @Test(dataProvider = "scopes")
  public void testSingleCliqueFailureWithoutAStandbyNodeThrowsLikeTheDefaultMode(
      ClusterModel.RebalanceScopeType scope) throws IOException {
    assertThrowsLikeTheDefaultMode(scope, (config, s) -> singleClique(config, s, false),
        previousOnFirstNode("prod_0", "R1", "R2"));
  }

  /**
   * Two cliques both fail next to a provisioned clique that has nodes but no resources yet. The
   * rethrown failure must be the first one, exactly as the default mode reports it.
   */
  @Test(dataProvider = "scopes")
  public void testEveryCliqueFailingNextToAnIdleCliqueThrowsLikeTheDefaultMode(
      ClusterModel.RebalanceScopeType scope) throws IOException {
    assertThrowsLikeTheDefaultMode(scope, (config, s) -> everyCliqueBroken(config, s, false),
        previousOnFirstNode(null, "RA", "RB"));
  }

  /**
   * The same total failure where every instance also carries operational labels and the spare
   * pool shares one of them. A label shared with the cliques joins nothing, and the spare pool's
   * own tag must not read as a surviving block either.
   */
  @Test(dataProvider = "scopes")
  public void testEveryCliqueFailingNextToALabelledSparePoolThrowsLikeTheDefaultMode(
      ClusterModel.RebalanceScopeType scope) throws IOException {
    assertThrowsLikeTheDefaultMode(scope, (config, s) -> everyCliqueBroken(config, s, true),
        previousOnFirstNode(null, "RA", "RB"));
  }

  /**
   * A narrow scope where the broken clique is the only one holding any resource at all, while five
   * more cliques have nodes and nothing pinned to them. With no other resource in the cluster
   * nothing is left to keep rebalancing, so this is a total failure rather than a contained one.
   */
  @Test(dataProvider = "scopes")
  public void testOnlyCliqueHoldingResourcesFailingNextToIdleCliquesThrowsLikeTheDefaultMode(
      ClusterModel.RebalanceScopeType scope) throws IOException {
    assertThrowsLikeTheDefaultMode(scope, this::onlyBrokenCliqueHoldsResources,
        previousOnFirstNode(instanceName(0, 0), "R_broken"));
  }

  /**
   * Both cliques are oversubscribed on their own nodes and the tag blind sum is negative even with
   * the idle clique's capacity counted, so this is a genuine cluster wide shortfall.
   */
  @Test(dataProvider = "scopes")
  public void testClusterWideShortfallNextToAnIdleCliqueThrowsLikeTheDefaultMode(
      ClusterModel.RebalanceScopeType scope) throws IOException {
    assertThrowsLikeTheDefaultMode(scope, (config, s) -> shortfall(config, s, false),
        previousSpreadOverCliques());
  }

  /**
   * The same shortfall where every replica is already allocated and nothing is outstanding, which
   * is the shape the partial, emergency and delayed overwrite scopes see once a cluster has been
   * running.
   */
  @Test(dataProvider = "scopes")
  public void testAllocatedClusterWideShortfallNextToAnIdleCliqueThrowsLikeTheDefaultMode(
      ClusterModel.RebalanceScopeType scope) throws IOException {
    assertThrowsLikeTheDefaultMode(scope, (config, s) -> shortfall(config, s, true),
        previousSpreadOverCliques());
  }

  /** A single oversubscribed clique next to a standby pool is a cluster wide shortfall too. */
  @Test(dataProvider = "scopes")
  public void testSingleCliqueShortfallNextToAStandbyPoolThrowsLikeTheDefaultMode(
      ClusterModel.RebalanceScopeType scope) throws IOException {
    assertThrowsLikeTheDefaultMode(scope, this::singleCliqueShortfall,
        previousSpreadOverCliques());
  }

  /**
   * Both cliques are oversubscribed on their own nodes next to an empty spare node that no block
   * reaches, which gets a block of its own for its capacity. Every block holding resources is at
   * fault and the spare node's block holds none, so nothing is left to keep rebalancing and the
   * round throws exactly what the default mode throws.
   */
  @Test(dataProvider = "scopesAndUnreachedSpares")
  public void testEveryCliqueShortNextToASpareNodeNoBlockReachesThrowsLikeTheDefaultMode(
      ClusterModel.RebalanceScopeType scope, Spare spare) throws IOException {
    assertThrowsLikeTheDefaultMode(scope, (config, s) -> shortfallNextToASpareNode(config, s,
        spare), previousSpreadOverCliques());
  }

  /**
   * A stale placement left on a standby node overcommits it and drags the tag blind sum negative,
   * while the only clique fits on its own nodes. Setting the standby node aside needs a second
   * block holding resources to rebalance around, which a single clique cluster does not have, so
   * the default mode's verdict stands, the same rule a replica failure in that cluster follows.
   */
  @Test(dataProvider = "scopes")
  public void testSingleCliqueWithAStaleOvercommitOnAStandbyNodeThrowsLikeTheDefaultMode(
      ClusterModel.RebalanceScopeType scope) throws IOException {
    assertThrowsLikeTheDefaultMode(scope,
        (config, s) -> staleOvercommit(config, s, false, Spare.STANDBY), Collections.emptyMap());
  }

  // ---------------------------------------------------------------------------------------------
  // A failure contained to some cliques is still isolated with such nodes present
  // ---------------------------------------------------------------------------------------------

  /** One broken clique, two healthy ones and a spare pool: the broken clique alone is carried. */
  @Test(dataProvider = "scopes")
  public void testOneBrokenCliqueNextToASparePoolIsStillIsolated(
      ClusterModel.RebalanceScopeType scope) throws IOException, HelixRebalanceException {
    assertOnlyTheBrokenCliqueIsCarried(scope, (config, s) -> oneBrokenClique(config, s, false),
        HelixRebalanceException.FailureCategory.NO_CANDIDATE_NODE);
  }

  /**
   * One clique oversubscribed far enough to drag the tag blind sum negative, next to healthy
   * cliques and a spare pool: the deficit is still attributed to that clique alone.
   */
  @Test(dataProvider = "scopes")
  public void testCliqueAttributableShortfallNextToASparePoolIsStillIsolated(
      ClusterModel.RebalanceScopeType scope) throws IOException, HelixRebalanceException {
    assertOnlyTheBrokenCliqueIsCarried(scope, this::oneOversubscribedClique,
        HelixRebalanceException.FailureCategory.CAPACITY_DEFICIT);
  }

  /**
   * Availability zone and hardware labels shared by every clique and by the spare pool must not
   * merge the cliques into one block, so the broken clique is still isolated on its own.
   */
  @Test(dataProvider = "scopes")
  public void testOperationalLabelsStillDoNotMergeCliquesNextToASparePool(
      ClusterModel.RebalanceScopeType scope) throws IOException, HelixRebalanceException {
    assertOnlyTheBrokenCliqueIsCarried(scope, (config, s) -> oneBrokenClique(config, s, true),
        HelixRebalanceException.FailureCategory.NO_CANDIDATE_NODE);
  }

  /**
   * A stale placement left on a standby node overcommits it and drags the tag blind sum negative,
   * while both cliques fit on their own nodes. Nothing that holds work is at fault, so the standby
   * node is set aside and both cliques are rebalanced normally rather than frozen, with nothing
   * carried over.
   */
  @Test(dataProvider = "scopes")
  public void testStaleOvercommitOnAStandbyNodeDoesNotFreezeHealthyCliques(
      ClusterModel.RebalanceScopeType scope) throws IOException, HelixRebalanceException {
    assertStaleOvercommitFreezesNothing(scope, Spare.STANDBY);
  }

  /**
   * The same stale placement on a spare node that no block reaches, because it carries no tag or
   * only a zone label that clique nodes carry too. The node is a block of its own, and it is the
   * only one at fault, so it is set aside, nothing is carried over and both cliques are
   * rebalanced.
   */
  @Test(dataProvider = "staleScopesAndUnreachedSpares")
  public void testStaleOvercommitOnASpareNodeNoBlockReachesDoesNotFreezeHealthyCliques(
      ClusterModel.RebalanceScopeType scope, Spare spare)
      throws IOException, HelixRebalanceException {
    assertStaleOvercommitFreezesNothing(scope, spare);
  }

  /**
   * Cliques A and B each need 150 of their 200 once the 200 of their replicas left on a spare node
   * of 40 is charged to that node, which no block reaches. Clique C needs 300 of 50. The cluster
   * holds 490 and is asked for 800. C is carried, A and B are rebalanced, and the spare node is
   * set aside with nothing carried for it: its stale replicas stay where they are.
   */
  @Test(dataProvider = "staleScopesAndUnreachedSpares")
  public void testSpareNodeNoBlockReachesNextToABrokenCliqueCarriesOnlyThatClique(
      ClusterModel.RebalanceScopeType scope, Spare spare)
      throws IOException, HelixRebalanceException {
    HelixRebalanceException defaultFailure = failureOf(scope,
        (config, s) -> staleSpareNextToABrokenClique(config, s, spare), Collections.emptyMap(),
        false);
    Assert.assertNotNull(defaultFailure, "Precondition: the default mode fails this cluster");
    Assert.assertEquals(defaultFailure.getFailureCategory(),
        HelixRebalanceException.FailureCategory.CAPACITY_DEFICIT);

    OptimalAssignment result = createAlgorithm().calculate(
        staleSpareNextToABrokenClique(createClusterConfig(true), scope, spare));
    Assert.assertEquals(result.getSkippedResources(), Collections.singleton("RC"),
        "Scope " + scope + ": only the broken clique is carried over");
    Map<String, ResourceAssignment> assignment = result.getOptimalResourceAssignment();
    String[] resources = {"RA", "RB"};
    for (int clique = 0; clique < resources.length; clique++) {
      ResourceAssignment healthy = assignment.get(resources[clique]);
      Assert.assertEquals(healthy.getMappedPartitions().size(), 10,
          "Scope " + scope + ": " + resources[clique] + " is fully assigned");
      Set<String> outstanding =
          healthy.getReplicaMap(new Partition(resources[clique] + "_5")).keySet();
      Assert.assertEquals(outstanding.size(), 1, "Scope " + scope + ": " + outstanding);
      Assert.assertTrue(outstanding.iterator().next().startsWith("instance_" + clique + "_"),
          "Scope " + scope + ": the outstanding replica lands on its own clique: " + outstanding);
      for (int p = 6; p < 10; p++) {
        Assert.assertEquals(
            healthy.getReplicaMap(new Partition(resources[clique] + "_" + p)).keySet(),
            Collections.singleton("spare_0"), "Scope " + scope + ": the stale replicas stay put");
      }
    }
  }

  // ---------------------------------------------------------------------------------------------
  // A resource group no node reaches never stands in for a surviving part of the cluster
  // ---------------------------------------------------------------------------------------------

  /**
   * An unplaceable untagged resource pulls the only clique into one cluster wide block, next to an
   * idle resource pinned to a tag no node carries. Nothing that reaches a node is left to keep
   * rebalancing, so the round throws the first failure, as the default mode does.
   */
  @Test(dataProvider = "scopesAndGhosts")
  public void testClusterWideBlockFailingNextToAnIdleGroupNoNodeReachesThrowsLikeTheDefaultMode(
      ClusterModel.RebalanceScopeType scope, Ghost ghost) throws IOException {
    assertThrowsLikeTheDefaultMode(scope,
        (config, s) -> untaggedResourceNextToAGhost(config, s, ghost),
        previousOnFirstNode(instanceName(0, 0), "U", "RA"));
  }

  /** Both cliques fail next to an idle resource that no node reaches. */
  @Test(dataProvider = "scopesAndGhosts")
  public void testEveryCliqueFailingNextToAnIdleGroupNoNodeReachesThrowsLikeTheDefaultMode(
      ClusterModel.RebalanceScopeType scope, Ghost ghost) throws IOException {
    assertThrowsLikeTheDefaultMode(scope,
        (config, s) -> brokenCliquesNextToAGhost(config, s, ghost, "RA", "RB"),
        previousOnFirstNode(null, "RA", "RB"));
  }

  /**
   * The only clique fails next to an idle resource that no node reaches. Without that resource the
   * cluster holds a single block, and the failure is never isolated at all.
   */
  @Test(dataProvider = "scopesAndGhosts")
  public void testOnlyCliqueFailingNextToAnIdleGroupNoNodeReachesThrowsLikeTheDefaultMode(
      ClusterModel.RebalanceScopeType scope, Ghost ghost) throws IOException {
    assertThrowsLikeTheDefaultMode(scope,
        (config, s) -> brokenCliquesNextToAGhost(config, s, ghost, "RA"),
        previousOnFirstNode(null, "RA"));
  }

  /**
   * The only clique is oversubscribed next to an idle resource that no node reaches. The capacity
   * deficit falls on the clique, which is everything that reaches a node, so the round throws the
   * deficit, as the default mode does.
   */
  @Test(dataProvider = "scopesAndGhosts")
  public void testCliqueShortfallNextToAnIdleGroupNoNodeReachesThrowsLikeTheDefaultMode(
      ClusterModel.RebalanceScopeType scope, Ghost ghost) throws IOException {
    assertThrowsLikeTheDefaultMode(scope,
        (config, s) -> cliqueShortfallNextToAGhost(config, s, ghost),
        previousOnFirstNode(instanceName(0, 0), "RA"));
  }

  /**
   * Every node of the only clique was retagged to standby while its resource still has partitions
   * to place, next to an idle resource that no node reaches either. No group reaches a node, so
   * nothing is left to keep rebalancing, and the round throws the first failure, as the default
   * mode does.
   */
  @Test(dataProvider = "scopesAndGhosts")
  public void testNoGroupReachingANodeThrowsLikeTheDefaultMode(
      ClusterModel.RebalanceScopeType scope, Ghost ghost) throws IOException {
    assertThrowsLikeTheDefaultMode(scope,
        (config, s) -> cliqueRetaggedToStandbyNextToAGhost(config, s, ghost),
        previousOnFirstNode(null, "RA"));
  }

  /** One broken clique and two healthy ones next to an idle resource that no node reaches. */
  @Test(dataProvider = "scopesAndGhosts")
  public void testOneBrokenCliqueNextToAnIdleGroupNoNodeReachesIsStillIsolated(
      ClusterModel.RebalanceScopeType scope, Ghost ghost)
      throws IOException, HelixRebalanceException {
    assertOnlyTheBrokenCliqueIsCarried(scope,
        (config, s) -> oneBrokenCliqueNextToAGhost(config, s, 2, ghost),
        HelixRebalanceException.FailureCategory.NO_CANDIDATE_NODE);
  }

  /**
   * The same cluster with the broken clique oversubscribed far enough to drag the tag blind sum
   * negative. The idle resource's replica sits on a node of the broken clique, which pays for it,
   * so the deficit still falls on that clique alone.
   */
  @Test(dataProvider = "scopes")
  public void testCliqueAttributableShortfallNextToAnIdleGroupNoNodeReachesIsStillIsolated(
      ClusterModel.RebalanceScopeType scope) throws IOException, HelixRebalanceException {
    assertOnlyTheBrokenCliqueIsCarried(scope,
        (config, s) -> oneBrokenCliqueNextToAGhost(config, s, 5, Ghost.RETAGGED),
        HelixRebalanceException.FailureCategory.CAPACITY_DEFICIT);
  }

  /**
   * Clique 0 has no node left but still has partitions to place, next to two healthy cliques. Its
   * failure is set aside on its own and the healthy cliques are rebalanced, whichever way the
   * default mode fails the round.
   */
  @Test(dataProvider = "scopesAndCategories")
  public void testWorkOnACliqueNoNodeReachesIsStillIsolated(
      ClusterModel.RebalanceScopeType scope, HelixRebalanceException.FailureCategory category)
      throws IOException, HelixRebalanceException {
    assertOnlyTheBrokenCliqueIsCarried(scope,
        (config, s) -> cliqueWithNoNodeLeft(config, s, category), category);
  }

  // ---------------------------------------------------------------------------------------------
  // Assertions
  // ---------------------------------------------------------------------------------------------

  private void assertStaleOvercommitFreezesNothing(ClusterModel.RebalanceScopeType scope,
      Spare spare) throws IOException, HelixRebalanceException {
    HelixRebalanceException defaultFailure = failureOf(scope,
        (config, s) -> staleOvercommit(config, s, true, spare), Collections.emptyMap(), false);
    Assert.assertNotNull(defaultFailure, "Precondition: the default mode fails this cluster");
    Assert.assertEquals(defaultFailure.getFailureCategory(),
        HelixRebalanceException.FailureCategory.CAPACITY_DEFICIT);

    OptimalAssignment result = createAlgorithm().calculate(
        staleOvercommit(createClusterConfig(true), scope, true, spare));
    Assert.assertEquals(result.getSkippedResources(), Collections.emptySet(),
        "Scope " + scope + ": no clique is at fault, so nothing is carried over");
    Map<String, ResourceAssignment> assignment = result.getOptimalResourceAssignment();
    Assert.assertEquals(assignment.get("RA").getMappedPartitions().size(), 6,
        "Scope " + scope + ": the stale replicas stay put and the outstanding ones are placed");
    Assert.assertEquals(assignment.get("RB").getMappedPartitions().size(), 4,
        "Scope " + scope + ": the second clique is rebalanced normally");
    assertOnOwnClique(assignment.get("RB"), 1, scope);
  }

  private void assertThrowsLikeTheDefaultMode(ClusterModel.RebalanceScopeType scope,
      ClusterBuilder builder, Map<String, ResourceAssignment> previous) throws IOException {
    HelixRebalanceException expected = failureOf(scope, builder, previous, false);
    Assert.assertNotNull(expected, "Precondition: the default mode fails this cluster");
    HelixRebalanceException actual = failureOf(scope, builder, previous, true);
    Assert.assertEquals(actual.getClass(), expected.getClass());
    Assert.assertEquals(actual.getFailureType(), expected.getFailureType(), "Scope " + scope);
    Assert.assertEquals(actual.getFailureCategory(), expected.getFailureCategory(),
        "Scope " + scope);
    Assert.assertEquals(canonical(actual.getMessage()), canonical(expected.getMessage()),
        "Scope " + scope);
  }

  /**
   * Runs the scope the way its caller does, carry forward included, and returns what it threw. A
   * flag on run that returns instead fails the test here, showing what it returned.
   */
  private HelixRebalanceException failureOf(ClusterModel.RebalanceScopeType scope,
      ClusterBuilder builder, Map<String, ResourceAssignment> previous, boolean isolation)
      throws IOException {
    try {
      Map<String, ResourceAssignment> result = WagedRebalanceUtil.calculateAssignment(
          builder.build(createClusterConfig(isolation), scope), createAlgorithm(),
          previousFor(scope, previous));
      if (isolation) {
        Assert.fail("Scope " + scope + ": nothing that holds work can keep rebalancing, so the "
            + "flag on run must fail exactly like the default mode, but it returned "
            + normalize(result));
      }
      return null;
    } catch (HelixRebalanceException e) {
      return e;
    }
  }

  private void assertOnlyTheBrokenCliqueIsCarried(ClusterModel.RebalanceScopeType scope,
      ClusterBuilder builder, HelixRebalanceException.FailureCategory defaultCategory)
      throws IOException, HelixRebalanceException {
    Map<String, ResourceAssignment> previous = previousOnFirstNode(instanceName(0, 0),
        resourceName(0));
    HelixRebalanceException defaultFailure = failureOf(scope, builder, previous, false);
    Assert.assertNotNull(defaultFailure, "Precondition: the default mode fails this cluster");
    Assert.assertEquals(defaultFailure.getFailureCategory(), defaultCategory);

    OptimalAssignment raw = createAlgorithm().calculate(builder.build(createClusterConfig(true),
        scope));
    Assert.assertEquals(raw.getSkippedResources(), Collections.singleton(resourceName(0)),
        "Scope " + scope + ": only the broken clique is skipped");

    Map<String, ResourceAssignment> result = WagedRebalanceUtil.calculateAssignment(
        builder.build(createClusterConfig(true), scope), createAlgorithm(),
        previousFor(scope, previous));
    if (scope == ClusterModel.RebalanceScopeType.DELAYED_REBALANCE_OVERWRITES) {
      Assert.assertFalse(result.containsKey(resourceName(0)),
          "The delayed overwrite omits the broken clique so its current assignment stays put");
    } else {
      Assert.assertEquals(result.get(resourceName(0)).getRecord().getMapFields(),
          previous.get(resourceName(0)).getRecord().getMapFields(),
          "Scope " + scope + ": the broken clique keeps its previous assignment, whole");
    }
    for (int clique = 1; clique <= 2; clique++) {
      ResourceAssignment healthy = result.get(resourceName(clique));
      Assert.assertNotNull(healthy, "Scope " + scope + ": clique " + clique + " is rebalanced");
      Assert.assertEquals(healthy.getMappedPartitions().size(), 3,
          "Scope " + scope + ": clique " + clique + " is fully assigned");
      assertOnOwnClique(healthy, clique, scope);
    }
  }

  private static void assertOnOwnClique(ResourceAssignment assignment, int clique,
      ClusterModel.RebalanceScopeType scope) {
    for (Partition partition : assignment.getMappedPartitions()) {
      for (String instance : assignment.getReplicaMap(partition).keySet()) {
        Assert.assertTrue(instance.startsWith("instance_" + clique + "_"),
            "Scope " + scope + ": " + partition + " must stay on clique " + clique + " but is on "
                + instance);
      }
    }
  }

  private static Map<String, ResourceAssignment> previousFor(
      ClusterModel.RebalanceScopeType scope, Map<String, ResourceAssignment> previous) {
    // The delayed overwrite runs without a previous assignment, see its caller.
    return scope == ClusterModel.RebalanceScopeType.DELAYED_REBALANCE_OVERWRITES ? null
        : previous;
  }

  /**
   * The failure detail lists the rejected nodes in hash map order, which the parallel hard
   * constraint scan can permute between two otherwise identical runs, so every innermost map in
   * the message is compared as a sorted list of its entries.
   */
  private static String canonical(String message) {
    Matcher matcher = INNERMOST_MAP.matcher(message);
    StringBuffer canonical = new StringBuffer();
    while (matcher.find()) {
      List<String> entries = new ArrayList<>(Arrays.asList(matcher.group(1).split(", ")));
      Collections.sort(entries);
      matcher.appendReplacement(canonical,
          Matcher.quoteReplacement("{" + String.join(", ", entries) + "}"));
    }
    matcher.appendTail(canonical);
    return canonical.toString();
  }

  // ---------------------------------------------------------------------------------------------
  // Clusters
  // ---------------------------------------------------------------------------------------------

  /** Two unplaceable resources on one tag with three nodes, and optionally a standby node. */
  private ClusterModel singleClique(ClusterConfig config, ClusterModel.RebalanceScopeType scope,
      boolean standby) throws IOException {
    Set<AssignableReplica> replicas = new HashSet<>();
    addReplicas(replicas, config, taggedResource("R1", "prod", UNPLACEABLE_PARTITION_WEIGHT), 1);
    addReplicas(replicas, config, taggedResource("R2", "prod", UNPLACEABLE_PARTITION_WEIGHT), 1);
    Set<AssignableNode> nodes = new HashSet<>();
    for (int i = 0; i < 3; i++) {
      nodes.add(taggedNode(config, "prod_" + i, i, "prod"));
    }
    if (standby) {
      nodes.add(taggedNode(config, "standby_0", 3, STANDBY_TAG));
    }
    return model(config, scope, replicas, replicas, nodes);
  }

  /**
   * Cliques 0 and 1 each hold one unplaceable resource. Either clique 2 is provisioned with no
   * resources, or every instance carries shared labels and a spare pool shares one of them.
   */
  private ClusterModel everyCliqueBroken(ClusterConfig config,
      ClusterModel.RebalanceScopeType scope, boolean labelledSparePool) throws IOException {
    Set<AssignableReplica> replicas = new HashSet<>();
    addReplicas(replicas, config, taggedResource("RA", cliqueTag(0), UNPLACEABLE_PARTITION_WEIGHT),
        1);
    addReplicas(replicas, config, taggedResource("RB", cliqueTag(1), UNPLACEABLE_PARTITION_WEIGHT),
        1);
    Set<AssignableNode> nodes = new HashSet<>();
    for (int clique = 0; clique < 2; clique++) {
      for (int i = 0; i < 2; i++) {
        nodes.add(labelledSparePool
            ? taggedNode(config, instanceName(clique, i), i, cliqueTag(clique), "az_" + i,
                "hw_gen_2")
            : taggedNode(config, instanceName(clique, i), i, cliqueTag(clique)));
      }
    }
    for (int i = 0; i < 2; i++) {
      nodes.add(labelledSparePool
          ? taggedNode(config, "spare_" + i, i, STANDBY_TAG, "az_0")
          : taggedNode(config, instanceName(2, i), i, cliqueTag(2)));
    }
    return model(config, scope, replicas, replicas, nodes);
  }

  /** Six cliques of two nodes, of which only clique 0 holds a resource, and it cannot be placed. */
  private ClusterModel onlyBrokenCliqueHoldsResources(ClusterConfig config,
      ClusterModel.RebalanceScopeType scope) throws IOException {
    Set<AssignableReplica> replicas = new HashSet<>();
    addReplicas(replicas, config,
        taggedResource("R_broken", cliqueTag(0), UNPLACEABLE_PARTITION_WEIGHT), 2);
    Set<AssignableNode> nodes = new HashSet<>();
    for (int clique = 0; clique < 6; clique++) {
      for (int i = 0; i < 2; i++) {
        nodes.add(taggedNode(config, instanceName(clique, i), i, cliqueTag(clique)));
      }
    }
    return model(config, scope, replicas, replicas, nodes);
  }

  /**
   * RA on clique 0 and RB on clique 1, five partitions of 90 each on two nodes of 100: 900 of
   * demand against 600 of capacity once the idle clique 2 is counted. Either everything is
   * outstanding, or everything already sits on its clique's nodes and nothing is outstanding.
   */
  private ClusterModel shortfall(ClusterConfig config, ClusterModel.RebalanceScopeType scope,
      boolean allocated) throws IOException {
    Set<AssignableReplica> replicas = new HashSet<>();
    addReplicas(replicas, config, taggedResource("RA", cliqueTag(0), HEAVY_PARTITION_WEIGHT), 5);
    addReplicas(replicas, config, taggedResource("RB", cliqueTag(1), HEAVY_PARTITION_WEIGHT), 5);
    Map<String, AssignableNode> nodes = new HashMap<>();
    for (int clique = 0; clique < 3; clique++) {
      for (int i = 0; i < 2; i++) {
        nodes.put(instanceName(clique, i),
            taggedNode(config, instanceName(clique, i), i, cliqueTag(clique)));
      }
    }
    if (!allocated) {
      return model(config, scope, replicas, replicas, new HashSet<>(nodes.values()));
    }
    for (AssignableReplica replica : replicas) {
      int clique = replica.getResourceName().equals("RA") ? 0 : 1;
      int partition = Integer.parseInt(replica.getPartitionName().split("_")[1]);
      nodes.get(instanceName(clique, partition % 2))
          .assignInitBatch(Collections.singleton(replica));
    }
    return model(config, scope, replicas, Collections.emptySet(),
        new HashSet<>(nodes.values()));
  }

  /** RA on clique 0, five partitions of 90 on two nodes of 100, next to two standby nodes. */
  private ClusterModel singleCliqueShortfall(ClusterConfig config,
      ClusterModel.RebalanceScopeType scope) throws IOException {
    Set<AssignableReplica> replicas = new HashSet<>();
    addReplicas(replicas, config, taggedResource("RA", cliqueTag(0), HEAVY_PARTITION_WEIGHT), 5);
    Set<AssignableNode> nodes = new HashSet<>();
    for (int i = 0; i < 2; i++) {
      nodes.add(taggedNode(config, instanceName(0, i), i, cliqueTag(0)));
      nodes.add(taggedNode(config, "standby_" + i, i, STANDBY_TAG));
    }
    return model(config, scope, replicas, replicas, nodes);
  }

  /**
   * RA on clique 0 has six partitions of 60. Four of them are stale placements still sitting on a
   * spare node (240 on a node of 100), which is what an instance moved out of the clique and
   * shrunk leaves behind, and two are outstanding. Optionally RB on clique 1 has four outstanding
   * partitions of 50, which fill its two nodes exactly. Each clique fits on its own nodes, but the
   * tag blind sum is 60 short either way.
   */
  private ClusterModel staleOvercommit(ClusterConfig config, ClusterModel.RebalanceScopeType scope,
      boolean secondClique, Spare spare) throws IOException {
    ResourceConfig ra = taggedResource("RA", cliqueTag(0), 60);
    Set<AssignableReplica> all = new HashSet<>();
    Set<AssignableReplica> outstanding = new HashSet<>();
    Set<AssignableReplica> stale = new HashSet<>();
    for (int p = 0; p < 6; p++) {
      AssignableReplica replica = new AssignableReplica(config, ra, "RA_" + p, "ONLINE", 0);
      all.add(replica);
      (p < 4 ? stale : outstanding).add(replica);
    }
    Set<AssignableNode> nodes = new HashSet<>();
    for (int i = 0; i < 2; i++) {
      nodes.add(cliqueNode(config, 0, i, spare));
    }
    if (secondClique) {
      Set<AssignableReplica> rb = new HashSet<>();
      addReplicas(rb, config, taggedResource("RB", cliqueTag(1), 50), 4);
      all.addAll(rb);
      outstanding.addAll(rb);
      for (int i = 0; i < 2; i++) {
        nodes.add(cliqueNode(config, 1, i, spare));
      }
    }
    AssignableNode spareNode = spareNode(config, NODE_CAPACITY, spare);
    spareNode.assignInitBatch(stale);
    nodes.add(spareNode);
    return model(config, scope, all, outstanding, nodes);
  }

  /**
   * RA on clique 0 and RB on clique 1 each have ten partitions of 25 on two nodes of 100. Five of
   * each sit on their clique's nodes, one is outstanding, and four are stale placements on a spare
   * node of 40 that carries none of the clique tags. RC on clique 2 has six partitions of 50 on a
   * single node of 50, one of them placed.
   */
  private ClusterModel staleSpareNextToABrokenClique(ClusterConfig config,
      ClusterModel.RebalanceScopeType scope, Spare spare) throws IOException {
    Set<AssignableReplica> all = new HashSet<>();
    Set<AssignableReplica> outstanding = new HashSet<>();
    Set<AssignableReplica> stale = new HashSet<>();
    Set<AssignableNode> nodes = new HashSet<>();
    String[] resources = {"RA", "RB"};
    for (int clique = 0; clique < resources.length; clique++) {
      ResourceConfig resource = taggedResource(resources[clique], cliqueTag(clique), 25);
      List<AssignableNode> cliqueNodes = Arrays.asList(cliqueNode(config, clique, 0, spare),
          cliqueNode(config, clique, 1, spare));
      nodes.addAll(cliqueNodes);
      for (int p = 0; p < 10; p++) {
        AssignableReplica replica =
            new AssignableReplica(config, resource, resources[clique] + "_" + p, "ONLINE", 0);
        all.add(replica);
        if (p < 5) {
          cliqueNodes.get(p % 2).assignInitBatch(Collections.singleton(replica));
        } else {
          (p == 5 ? outstanding : stale).add(replica);
        }
      }
    }
    AssignableNode spareNode = spareNode(config, 40, spare);
    spareNode.assignInitBatch(stale);
    nodes.add(spareNode);

    ResourceConfig rc = taggedResource("RC", cliqueTag(2), 50);
    AssignableNode broken = sizedNode(config, instanceName(2, 0), 50, cliqueTag(2));
    for (int p = 0; p < 6; p++) {
      AssignableReplica replica = new AssignableReplica(config, rc, "RC_" + p, "ONLINE", 0);
      all.add(replica);
      if (p == 0) {
        broken.assignInitBatch(Collections.singleton(replica));
      }
    }
    nodes.add(broken);
    return model(config, scope, all, outstanding, nodes);
  }

  /**
   * RA on clique 0 and RB on clique 1, five outstanding partitions of 90 each on two nodes of 100,
   * next to an empty spare node of 100: 900 of demand against 500 of capacity.
   */
  private ClusterModel shortfallNextToASpareNode(ClusterConfig config,
      ClusterModel.RebalanceScopeType scope, Spare spare) throws IOException {
    Set<AssignableReplica> replicas = new HashSet<>();
    addReplicas(replicas, config, taggedResource("RA", cliqueTag(0), HEAVY_PARTITION_WEIGHT), 5);
    addReplicas(replicas, config, taggedResource("RB", cliqueTag(1), HEAVY_PARTITION_WEIGHT), 5);
    Set<AssignableNode> nodes = new HashSet<>();
    for (int clique = 0; clique < 2; clique++) {
      for (int i = 0; i < 2; i++) {
        nodes.add(cliqueNode(config, clique, i, spare));
      }
    }
    nodes.add(spareNode(config, NODE_CAPACITY, spare));
    return model(config, scope, replicas, replicas, nodes);
  }

  /** A clique node, which also carries a zone label when the spare node is labelled. */
  private AssignableNode cliqueNode(ClusterConfig config, int clique, int index, Spare spare) {
    return spare == Spare.LABELLED
        ? taggedNode(config, instanceName(clique, index), index, cliqueTag(clique), "az_" + index)
        : taggedNode(config, instanceName(clique, index), index, cliqueTag(clique));
  }

  private static AssignableNode spareNode(ClusterConfig config, int capacity, Spare spare) {
    switch (spare) {
      case STANDBY:
        return sizedNode(config, "standby_0", capacity, STANDBY_TAG);
      case LABELLED:
        return sizedNode(config, "spare_0", capacity, "az_0");
      default:
        return sizedNode(config, "spare_0", capacity);
    }
  }

  /**
   * Clique 0 holds one unplaceable resource, cliques 1 and 2 hold three healthy partitions each,
   * and two spare nodes carry a tag no resource uses. Optionally every clique instance also carries
   * an availability zone and a hardware label, and the spare nodes share the zone label.
   */
  private ClusterModel oneBrokenClique(ClusterConfig config, ClusterModel.RebalanceScopeType scope,
      boolean labels) throws IOException {
    Set<AssignableReplica> replicas = new HashSet<>();
    addReplicas(replicas, config,
        taggedResource(resourceName(0), cliqueTag(0), UNPLACEABLE_PARTITION_WEIGHT), 2);
    for (int clique = 1; clique <= 2; clique++) {
      addReplicas(replicas, config,
          taggedResource(resourceName(clique), cliqueTag(clique), HEALTHY_PARTITION_WEIGHT), 3);
    }
    Set<AssignableNode> nodes = new HashSet<>();
    for (int clique = 0; clique <= 2; clique++) {
      for (int i = 0; i < 2; i++) {
        nodes.add(labels
            ? taggedNode(config, instanceName(clique, i), i, cliqueTag(clique), "az_" + i,
                "hw_gen_2")
            : taggedNode(config, instanceName(clique, i), i, cliqueTag(clique)));
      }
    }
    for (int i = 0; i < 2; i++) {
      nodes.add(labels ? taggedNode(config, "spare_" + i, i, STANDBY_TAG, "az_0")
          : taggedNode(config, "spare_" + i, i, STANDBY_TAG));
    }
    return model(config, scope, replicas, replicas, nodes);
  }

  /**
   * Clique 0 holds five partitions of 150 on two nodes of 100, cliques 1 and 2 hold three healthy
   * partitions each, and two spare nodes carry a tag no resource uses: 810 of demand against 800 of
   * capacity, all of the excess caused by clique 0.
   */
  private ClusterModel oneOversubscribedClique(ClusterConfig config,
      ClusterModel.RebalanceScopeType scope) throws IOException {
    Set<AssignableReplica> replicas = new HashSet<>();
    addReplicas(replicas, config,
        taggedResource(resourceName(0), cliqueTag(0), UNPLACEABLE_PARTITION_WEIGHT), 5);
    for (int clique = 1; clique <= 2; clique++) {
      addReplicas(replicas, config,
          taggedResource(resourceName(clique), cliqueTag(clique), HEALTHY_PARTITION_WEIGHT), 3);
    }
    Set<AssignableNode> nodes = new HashSet<>();
    for (int clique = 0; clique <= 2; clique++) {
      for (int i = 0; i < 2; i++) {
        nodes.add(taggedNode(config, instanceName(clique, i), i, cliqueTag(clique)));
      }
    }
    for (int i = 0; i < 2; i++) {
      nodes.add(taggedNode(config, "spare_" + i, i, STANDBY_TAG));
    }
    return model(config, scope, replicas, replicas, nodes);
  }

  /**
   * U is untagged with one partition of 150, which fits no node, and RA on clique 0 has two
   * partitions of 10 for the clique's two nodes, next to {@link #GHOST}.
   */
  private ClusterModel untaggedResourceNextToAGhost(ClusterConfig config,
      ClusterModel.RebalanceScopeType scope, Ghost ghost) throws IOException {
    Set<AssignableReplica> replicas = new HashSet<>();
    addReplicas(replicas, config, taggedResource("U", null, UNPLACEABLE_PARTITION_WEIGHT), 1);
    addReplicas(replicas, config, taggedResource("RA", cliqueTag(0), HEALTHY_PARTITION_WEIGHT),
        2);
    return modelWithAGhost(config, scope, replicas, cliqueNodes(config, 1), ghost);
  }

  /**
   * One partition of 150, which fits no node, for each given resource, pinned to cliques 0, 1 and
   * so on in order, each clique with two nodes, next to {@link #GHOST}.
   */
  private ClusterModel brokenCliquesNextToAGhost(ClusterConfig config,
      ClusterModel.RebalanceScopeType scope, Ghost ghost, String... resources) throws IOException {
    Set<AssignableReplica> replicas = new HashSet<>();
    for (int clique = 0; clique < resources.length; clique++) {
      addReplicas(replicas, config,
          taggedResource(resources[clique], cliqueTag(clique), UNPLACEABLE_PARTITION_WEIGHT), 1);
    }
    return modelWithAGhost(config, scope, replicas, cliqueNodes(config, resources.length), ghost);
  }

  /**
   * RA on clique 0 has five partitions of 90 for two nodes of 100, next to {@link #GHOST}: 460 of
   * demand against 200 of capacity.
   */
  private ClusterModel cliqueShortfallNextToAGhost(ClusterConfig config,
      ClusterModel.RebalanceScopeType scope, Ghost ghost) throws IOException {
    Set<AssignableReplica> replicas = new HashSet<>();
    addReplicas(replicas, config, taggedResource("RA", cliqueTag(0), HEAVY_PARTITION_WEIGHT), 5);
    return modelWithAGhost(config, scope, replicas, cliqueNodes(config, 1), ghost);
  }

  /**
   * RA on clique 0 has two partitions of 10, but both nodes of clique 0 now carry only the standby
   * tag, so no node carries the clique's tag, next to {@link #GHOST}.
   */
  private ClusterModel cliqueRetaggedToStandbyNextToAGhost(ClusterConfig config,
      ClusterModel.RebalanceScopeType scope, Ghost ghost) throws IOException {
    Set<AssignableReplica> replicas = new HashSet<>();
    addReplicas(replicas, config, taggedResource("RA", cliqueTag(0), HEALTHY_PARTITION_WEIGHT),
        2);
    Map<String, AssignableNode> nodes = new HashMap<>();
    for (int i = 0; i < 2; i++) {
      nodes.put(instanceName(0, i), taggedNode(config, instanceName(0, i), i, STANDBY_TAG));
    }
    return modelWithAGhost(config, scope, replicas, nodes, ghost);
  }

  /**
   * Clique 0 holds the given number of partitions of 150, cliques 1 and 2 hold three healthy
   * partitions each, and every clique has two nodes, next to {@link #GHOST}. Two broken partitions
   * make 370 of demand against 600 of capacity, five make 820.
   */
  private ClusterModel oneBrokenCliqueNextToAGhost(ClusterConfig config,
      ClusterModel.RebalanceScopeType scope, int brokenPartitions, Ghost ghost)
      throws IOException {
    Set<AssignableReplica> replicas = new HashSet<>();
    addReplicas(replicas, config,
        taggedResource(resourceName(0), cliqueTag(0), UNPLACEABLE_PARTITION_WEIGHT),
        brokenPartitions);
    for (int clique = 1; clique <= 2; clique++) {
      addReplicas(replicas, config,
          taggedResource(resourceName(clique), cliqueTag(clique), HEALTHY_PARTITION_WEIGHT), 3);
    }
    return modelWithAGhost(config, scope, replicas, cliqueNodes(config, 3), ghost);
  }

  /**
   * Clique 0 has lost both of its nodes while its resource still has partitions to place, two of
   * 10, or five of 90 for a cluster wide shortfall of 510 against 400. Cliques 1 and 2 hold three
   * healthy partitions each on their own two nodes.
   */
  private ClusterModel cliqueWithNoNodeLeft(ClusterConfig config,
      ClusterModel.RebalanceScopeType scope, HelixRebalanceException.FailureCategory category)
      throws IOException {
    boolean shortfall = category == HelixRebalanceException.FailureCategory.CAPACITY_DEFICIT;
    Set<AssignableReplica> replicas = new HashSet<>();
    addReplicas(replicas, config, taggedResource(resourceName(0), cliqueTag(0),
        shortfall ? HEAVY_PARTITION_WEIGHT : HEALTHY_PARTITION_WEIGHT), shortfall ? 5 : 2);
    for (int clique = 1; clique <= 2; clique++) {
      addReplicas(replicas, config,
          taggedResource(resourceName(clique), cliqueTag(clique), HEALTHY_PARTITION_WEIGHT), 3);
    }
    Map<String, AssignableNode> nodes = cliqueNodes(config, 3);
    nodes.remove(instanceName(0, 0));
    nodes.remove(instanceName(0, 1));
    return model(config, scope, replicas, replicas, new HashSet<>(nodes.values()));
  }

  /** Two nodes of 100 for each clique from 0 up to the given count, keyed by instance name. */
  private Map<String, AssignableNode> cliqueNodes(ClusterConfig config, int cliques) {
    Map<String, AssignableNode> nodes = new HashMap<>();
    for (int clique = 0; clique < cliques; clique++) {
      for (int i = 0; i < 2; i++) {
        nodes.put(instanceName(clique, i),
            taggedNode(config, instanceName(clique, i), i, cliqueTag(clique)));
      }
    }
    return nodes;
  }

  /**
   * The model that assigns the given replicas, next to {@link #GHOST}: one partition of 10 pinned
   * to a tag no node carries, with nothing to place. Its replica sits on a clique 0 node or on no
   * node at all.
   */
  private ClusterModel modelWithAGhost(ClusterConfig config, ClusterModel.RebalanceScopeType scope,
      Set<AssignableReplica> replicas, Map<String, AssignableNode> nodes, Ghost ghost)
      throws IOException {
    Set<AssignableReplica> idle = new HashSet<>();
    addReplicas(idle, config, taggedResource(GHOST, GHOST_TAG, HEALTHY_PARTITION_WEIGHT), 1);
    if (ghost == Ghost.RETAGGED) {
      nodes.get(instanceName(0, 1)).assignInitBatch(idle);
    }
    Set<AssignableReplica> all = new HashSet<>(replicas);
    all.addAll(idle);
    return model(config, scope, all, replicas, new HashSet<>(nodes.values()));
  }

  private static ClusterModel model(ClusterConfig config, ClusterModel.RebalanceScopeType scope,
      Set<AssignableReplica> all, Set<AssignableReplica> outstanding, Set<AssignableNode> nodes) {
    ClusterContext context =
        new ClusterContext(all, nodes, Collections.emptyMap(), Collections.emptyMap(), config);
    return new ClusterModel(context, outstanding, nodes, scope);
  }

  // ---------------------------------------------------------------------------------------------
  // Previous assignments
  // ---------------------------------------------------------------------------------------------

  /**
   * Partition 0 of every resource on one instance, or on the first node of the resource's clique
   * when the instance is null.
   */
  private static Map<String, ResourceAssignment> previousOnFirstNode(String instance,
      String... resources) {
    Map<String, ResourceAssignment> previous = new HashMap<>();
    for (int i = 0; i < resources.length; i++) {
      ResourceAssignment assignment = new ResourceAssignment(resources[i]);
      assignment.addReplicaMap(new Partition(resources[i] + "_0"), Collections.singletonMap(
          instance == null ? instanceName(i, 0) : instance, "ONLINE"));
      previous.put(resources[i], assignment);
    }
    return previous;
  }

  /** Every partition of RA and RB spread over the two nodes of cliques 0 and 1 respectively. */
  private static Map<String, ResourceAssignment> previousSpreadOverCliques() {
    Map<String, ResourceAssignment> previous = new HashMap<>();
    String[] resources = {"RA", "RB"};
    for (int clique = 0; clique < resources.length; clique++) {
      ResourceAssignment assignment = new ResourceAssignment(resources[clique]);
      for (int p = 0; p < 5; p++) {
        assignment.addReplicaMap(new Partition(resources[clique] + "_" + p),
            Collections.singletonMap(instanceName(clique, p % 2), "ONLINE"));
      }
      previous.put(resources[clique], assignment);
    }
    return previous;
  }
}
