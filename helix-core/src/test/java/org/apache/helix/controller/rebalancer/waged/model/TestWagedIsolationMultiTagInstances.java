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
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;

import org.apache.helix.HelixRebalanceException;
import org.apache.helix.constants.InstanceConstants;
import org.apache.helix.controller.rebalancer.util.WagedRebalanceUtil;
import org.apache.helix.controller.rebalancer.waged.RebalanceAlgorithm;
import org.apache.helix.controller.rebalancer.waged.constraints.ConstraintBasedAlgorithmFactory;
import org.apache.helix.model.ClusterConfig;
import org.apache.helix.model.InstanceConfig;
import org.apache.helix.model.Partition;
import org.apache.helix.model.ResourceAssignment;
import org.apache.helix.model.ResourceConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.Assert;
import org.testng.annotations.Test;

/**
 * Covers what happens when an instance carries more than one tag and has a capacity cap on how much
 * it can host, which is the case where a carried over assignment and a freshly calculated one could
 * otherwise land on the same node.
 *
 * The cases:
 *   sameTag       resources sharing one tag are isolated and carried over together, including a
 *                 sibling already placed during the run
 *   bothTags      an instance carrying two tags joins their groups into one share block, and a
 *                 block holding every group fails the run as the default mode does
 *   disjointTags  the clique topology, where isolation engages safely
 *   staleRetag    a carried over assignment names an instance retagged into another group, and only
 *                 the groups that collide with it give up their fresh result
 *   walkthrough   one partition per node, where an instance carrying both tags fails the run with
 *                 the flag on just as with it off, and disjoint tags isolate safely
 *   nullStart     a null starting assignment drops a skipped resource without tripping the guard
 *   overlap       instances shared along a chain of tags, carried over as one closure while clean
 *                 cliques keep rebalancing, and blamed as one block for a capacity deficit
 *   tagChanges    a tag added to a live instance merges two blocks and removing it splits them,
 *                 across consecutive rebalances
 *   labels        tags no resource is pinned to join no blocks, in the share closure or in the
 *                 capacity deficit attribution
 */
public class TestWagedIsolationMultiTagInstances {
  private static final Logger LOG =
      LoggerFactory.getLogger(TestWagedIsolationMultiTagInstances.class);
  private static final String CAP = "DISK";
  private static final int NODE_CAPACITY = 100;
  private static final int WEIGHT = 60;   // two of these on one node breaches the cap

  private ClusterConfig config(boolean on) {
    ClusterConfig c = new ClusterConfig("MultiTagCluster");
    c.setInstanceCapacityKeys(Collections.singletonList(CAP));
    c.setDefaultPartitionWeightMap(Collections.singletonMap(CAP, 0));
    c.setWagedInstanceTagIsolationEnabled(on);
    return c;
  }

  private AssignableNode node(ClusterConfig c, String name, int zone, String... tags) {
    InstanceConfig ic = new InstanceConfig(name);
    ic.setInstanceCapacityMap(Collections.singletonMap(CAP, NODE_CAPACITY));
    for (String t : tags) {
      ic.addTag(t);
    }
    ic.setInstanceOperation(InstanceConstants.InstanceOperation.ENABLE);
    return new AssignableNode(c, ic, name);
  }

  private ResourceConfig res(String name, String tag, int weight) throws IOException {
    ResourceConfig rc = new ResourceConfig(name);
    if (tag != null) {
      rc.getRecord()
          .setSimpleField(ResourceConfig.ResourceConfigProperty.INSTANCE_GROUP_TAG.name(), tag);
    }
    rc.setPartitionCapacityMap(Collections.singletonMap(ResourceConfig.DEFAULT_PARTITION_KEY,
        Collections.singletonMap(CAP, weight)));
    return rc;
  }

  private void add(Set<AssignableReplica> out, ClusterConfig c, ResourceConfig rc, int n) {
    for (int p = 0; p < n; p++) {
      out.add(new AssignableReplica(c, rc, rc.getResourceName() + "_" + p, "ONLINE", 0));
    }
  }

  private RebalanceAlgorithm algo() {
    return ConstraintBasedAlgorithmFactory.getInstance(Collections.emptyMap());
  }

  /** Total persisted weight landing on each instance, across every resource in the result. */
  private Map<String, Integer> loadPerInstance(Map<String, ResourceAssignment> result,
      Map<String, Integer> weightByResource) {
    Map<String, Integer> load = new HashMap<>();
    result.forEach((resource, ra) -> ra.getMappedPartitions().forEach(part -> ra.getReplicaMap(part)
        .keySet().forEach(instance -> load.merge(instance, weightByResource.get(resource),
            Integer::sum))));
    return load;
  }

  private void assertNoBreach(String label, Map<String, ResourceAssignment> result,
      Map<String, Integer> weights) {
    Map<String, Integer> load = loadPerInstance(result, weights);
    LOG.info(label + " load = " + load);
    load.forEach((inst, used) -> Assert
        .assertTrue(used <= NODE_CAPACITY, label + " capacity breach on " + inst + " = " + used));
  }

  private static TreeSet<String> setOf(String... values) {
    return new TreeSet<>(Arrays.asList(values));
  }

  /** One global baseline calculation of the given topology, starting from the given assignment. */
  private Map<String, ResourceAssignment> rebalance(RebalanceAlgorithm algorithm, ClusterConfig c,
      Set<AssignableReplica> replicas, Set<AssignableNode> nodes,
      Map<String, ResourceAssignment> start) throws HelixRebalanceException {
    ClusterContext ctx = new ClusterContext(replicas, nodes, Collections.emptyMap(),
        Collections.emptyMap(), c);
    return WagedRebalanceUtil.calculateAssignment(
        new ClusterModel(ctx, replicas, nodes, ClusterModel.RebalanceScopeType.GLOBAL_BASELINE),
        algorithm, start);
  }

  /**
   * Runs the constraint based algorithm and records two sets per run: the resources isolation
   * itself skipped, and the full skipped set reported once colliding groups have yielded. The
   * collision check carries a group over for reasons of its own, so only the first set shows which
   * groups a failure took down with it.
   */
  private static final class RecordingAlgorithm implements RebalanceAlgorithm {
    private final RebalanceAlgorithm _delegate =
        ConstraintBasedAlgorithmFactory.getInstance(Collections.emptyMap());
    private Set<String> _isolated = Collections.emptySet();
    private Set<String> _skipped = Collections.emptySet();

    @Override
    public OptimalAssignment calculate(ClusterModel clusterModel) throws HelixRebalanceException {
      _isolated = Collections.emptySet();
      _skipped = Collections.emptySet();
      OptimalAssignment assignment = _delegate.calculate(clusterModel);
      _isolated = new TreeSet<>(assignment.getSkippedResources());
      return assignment;
    }

    @Override
    public String getName() {
      return _delegate.getName();
    }

    @Override
    public void onAssignmentComputed(ClusterModel.RebalanceScopeType scope,
        Set<String> evaluatedResources, Set<String> skippedResources) {
      _skipped = new TreeSet<>(skippedResources);
      _delegate.onAssignmentComputed(scope, evaluatedResources, skippedResources);
    }
  }

  /**
   * Instance A carries T1 and T2, so the two groups form one share block, and that block holds
   * every group in the cluster. R1 cannot place its third partition and there is no second block
   * to recalculate around, so asserts that the run fails with the default mode's no candidate node
   * error instead of returning an assignment.
   */
  @Test
  public void instanceCarryingBothTagsNeverIsolates() throws IOException {
    ClusterConfig c = config(true);
    Set<AssignableReplica> replicas = new HashSet<>();
    // 3 partitions but only 2 T1 nodes with room for one each, so the third cannot be placed.
    add(replicas, c, res("R1", "T1", WEIGHT), 3);
    add(replicas, c, res("R2", "T2", WEIGHT), 1);

    Set<AssignableNode> nodes = new HashSet<>();
    nodes.add(node(c, "A", 0, "T1", "T2"));   // two tags, reachable by both groups
    nodes.add(node(c, "C", 1, "T1"));
    nodes.add(node(c, "B", 2, "T2"));

    ClusterContext ctx = new ClusterContext(replicas, nodes, Collections.emptyMap(),
        Collections.emptyMap(), c);

    ResourceAssignment prevR1 = new ResourceAssignment("R1");
    prevR1.addReplicaMap(new Partition("R1_0"), Collections.singletonMap("A", "ONLINE"));
    prevR1.addReplicaMap(new Partition("R1_1"), Collections.singletonMap("C", "ONLINE"));

    Map<String, Integer> weights = new HashMap<>();
    weights.put("R1", WEIGHT);
    weights.put("R2", WEIGHT);

    try {
      Map<String, ResourceAssignment> result = WagedRebalanceUtil.calculateAssignment(
          new ClusterModel(ctx, replicas, nodes, ClusterModel.RebalanceScopeType.GLOBAL_BASELINE),
          algo(), Collections.singletonMap("R1", prevR1));
      Assert.fail("A joins T1 and T2 into the only share block, so isolation must not engage, but "
          + "the run returned " + result + " with load " + loadPerInstance(result, weights));
    } catch (HelixRebalanceException e) {
      Assert.assertEquals(e.getFailureType(), HelixRebalanceException.Type.FAILED_TO_CALCULATE);
      Assert.assertEquals(e.getFailureCategory(),
          HelixRebalanceException.FailureCategory.NO_CANDIDATE_NODE, e.getMessage());
    }
  }

  /** Same shape, but the two groups have disjoint node domains. Isolation should engage safely. */
  @Test
  public void disjointTagsIsolateSafely() throws HelixRebalanceException, IOException {
    ClusterConfig c = config(true);
    Set<AssignableReplica> replicas = new HashSet<>();
    add(replicas, c, res("R1", "T1", WEIGHT), 3);
    add(replicas, c, res("R2", "T2", WEIGHT), 1);

    Set<AssignableNode> nodes = new HashSet<>();
    nodes.add(node(c, "A", 0, "T1"));   // single tag now
    nodes.add(node(c, "C", 1, "T1"));
    nodes.add(node(c, "B", 2, "T2"));

    ClusterContext ctx = new ClusterContext(replicas, nodes, Collections.emptyMap(),
        Collections.emptyMap(), c);

    ResourceAssignment prevR1 = new ResourceAssignment("R1");
    prevR1.addReplicaMap(new Partition("R1_0"), Collections.singletonMap("A", "ONLINE"));
    prevR1.addReplicaMap(new Partition("R1_1"), Collections.singletonMap("C", "ONLINE"));

    Map<String, ResourceAssignment> result = WagedRebalanceUtil.calculateAssignment(
        new ClusterModel(ctx, replicas, nodes, ClusterModel.RebalanceScopeType.GLOBAL_BASELINE),
        algo(), Collections.singletonMap("R1", prevR1));
    LOG.info("disjointTags result = " + result);

    Map<String, Integer> weights = new HashMap<>();
    weights.put("R1", WEIGHT);
    weights.put("R2", WEIGHT);
    assertNoBreach("disjointTags", result, weights);
    Assert.assertTrue(result.containsKey("R2"), "healthy group must still be assigned");
  }

  /**
   * R1 and R2 share tag T, so they are one group. Asserts that R1's failure isolates R1 and R2
   * together, that both are carried over exactly as the run started, and that R3 on tag U keeps
   * its freshly calculated assignment.
   */
  @Test
  public void resourcesSharingOneTagRollBackTogether() throws HelixRebalanceException, IOException {
    ClusterConfig c = config(true);
    Set<AssignableReplica> replicas = new HashSet<>();
    // The heavier R2 is placed first and takes one of the three T nodes, which leaves R1 room for
    // two of its three partitions. R1's failure then has to release R2's placement as well.
    add(replicas, c, res("R1", "T", WEIGHT), 3);
    add(replicas, c, res("R2", "T", NODE_CAPACITY), 1);
    add(replicas, c, res("R3", "U", WEIGHT), 1);   // healthy group keeps it a partial failure

    Set<AssignableNode> nodes = new HashSet<>();
    nodes.add(node(c, "A", 0, "T"));
    nodes.add(node(c, "C", 1, "T"));
    nodes.add(node(c, "E", 2, "T"));
    nodes.add(node(c, "U0", 3, "U"));

    // A valid starting assignment: A=60, C=60, E=100, all within capacity.
    ResourceAssignment prevR1 = new ResourceAssignment("R1");
    prevR1.addReplicaMap(new Partition("R1_0"), Collections.singletonMap("A", "ONLINE"));
    prevR1.addReplicaMap(new Partition("R1_1"), Collections.singletonMap("C", "ONLINE"));
    ResourceAssignment prevR2 = new ResourceAssignment("R2");
    prevR2.addReplicaMap(new Partition("R2_0"), Collections.singletonMap("E", "ONLINE"));
    Map<String, ResourceAssignment> prev = new HashMap<>();
    prev.put("R1", prevR1);
    prev.put("R2", prevR2);

    RecordingAlgorithm algorithm = new RecordingAlgorithm();
    Map<String, ResourceAssignment> result = rebalance(algorithm, c, replicas, nodes, prev);
    LOG.info("sharedTag result = " + result);

    Map<String, Integer> weights = new HashMap<>();
    weights.put("R1", WEIGHT);
    weights.put("R2", NODE_CAPACITY);
    weights.put("R3", WEIGHT);
    Map<String, Integer> load = loadPerInstance(result, weights);
    // Asserted on what isolation skipped: a fresh R2 next to carried R1 replicas would yield to
    // the collision check, and a fresh R2 on E equals its carried entry, so the result alone
    // cannot tell a group rollback from a per resource one.
    Assert.assertEquals(algorithm._isolated, setOf("R1", "R2"),
        "R1 and R2 share tag T, so R1's failure must isolate R2 with it. Load " + load);
    Assert.assertEquals(algorithm._skipped, setOf("R1", "R2"), "load " + load);
    Assert.assertEquals(result.get("R1").getRecord(), prevR1.getRecord(), "load " + load);
    Assert.assertEquals(result.get("R2").getRecord(), prevR2.getRecord(), "load " + load);
    Assert.assertEquals(instances(result.get("R3")), setOf("U0"), "load " + load);
    assertNoBreach("sharedTag", result, weights);
  }

  /**
   * Disjoint tags today, but R1's previous assignment still references a node that has since been
   * retagged into R2's group. The previous assignment was itself perfectly valid.
   */
  @Test
  public void staleCarryOverAfterRetagYieldsOnlyTheCollidingGroup()
      throws HelixRebalanceException, IOException {
    ClusterConfig c = config(true);
    Set<AssignableReplica> replicas = new HashSet<>();
    // Only one T1 node with room for one partition, so R1's second partition cannot be placed.
    add(replicas, c, res("R1", "T1", WEIGHT), 2);
    add(replicas, c, res("R2", "T2", WEIGHT), 1);

    Set<AssignableNode> nodes = new HashSet<>();
    nodes.add(node(c, "n_t1_0", 0, "T1"));
    nodes.add(node(c, "retagged", 1, "T2"));   // used to carry T1, retagged to T2

    ClusterContext ctx = new ClusterContext(replicas, nodes, Collections.emptyMap(),
        Collections.emptyMap(), c);

    // Written while "retagged" still carried T1. Valid then: n_t1_0=60, retagged=60.
    ResourceAssignment prevR1 = new ResourceAssignment("R1");
    prevR1.addReplicaMap(new Partition("R1_0"), Collections.singletonMap("n_t1_0", "ONLINE"));
    prevR1.addReplicaMap(new Partition("R1_1"), Collections.singletonMap("retagged", "ONLINE"));

    // Carrying R1 forward would put its stale replica on "retagged" alongside the R2 replica just
    // placed there, overcommitting a capacity 100 node to 120. R2 is the only group that collides,
    // so R2 alone gives up its fresh placement. R2 has no previous assignment here, so it is
    // dropped rather than carried, and the overcommit never reaches the result.
    Map<String, ResourceAssignment> result = WagedRebalanceUtil.calculateAssignment(
        new ClusterModel(ctx, replicas, nodes, ClusterModel.RebalanceScopeType.GLOBAL_BASELINE),
        algo(), Collections.singletonMap("R1", prevR1));

    Assert.assertTrue(result.containsKey("R1"), "R1 must be carried forward: " + result);
    Assert.assertEquals(result.get("R1").getRecord(), prevR1.getRecord(),
        "R1 must be carried forward byte identical");
    Assert.assertFalse(result.containsKey("R2"),
        "R2 collided on the retagged instance and had nothing to carry, so it must be dropped "
            + "rather than persisted next to R1's stale replica: " + result);

    Map<String, Integer> weights = new HashMap<>();
    weights.put("R1", WEIGHT);
    weights.put("R2", WEIGHT);
    assertNoBreach("staleRetag", result, weights);
  }

  /**
   * The headline property: a collision costs the colliding groups their fresh result, and nothing
   * else. R1 is broken and carried forward, R2 collides with it and yields, R3 is unrelated and
   * must keep the assignment that was just calculated for it.
   *
   * R1[T1] can only reach t1_a, so its two partitions do not fit and it is skipped. Its previous
   * assignment still names "moved", an instance that has since been retagged from T1 to T2. R2[T2]
   * is freshly placed onto "moved", so R2 is the group that collides and gives up its fresh result.
   * R3[T3] never touches any of those instances.
   */
  @Test
  public void collisionCostsOnlyTheCollidingGroupsNotTheCluster()
      throws HelixRebalanceException, IOException {
    ClusterConfig c = config(true);
    Set<AssignableReplica> replicas = new HashSet<>();
    add(replicas, c, res("R1", "T1", WEIGHT), 2);   // needs 120 on a single 100 node, so it fails
    add(replicas, c, res("R2", "T2", WEIGHT), 2);
    add(replicas, c, res("R3", "T3", WEIGHT), 2);

    Set<AssignableNode> nodes = new HashSet<>();
    nodes.add(node(c, "t1_a", 0, "T1"));
    nodes.add(node(c, "moved", 1, "T2"));   // carried T1 when R1's previous assignment was written
    nodes.add(node(c, "t2_b", 2, "T2"));
    nodes.add(node(c, "t3_a", 3, "T3"));
    nodes.add(node(c, "t3_b", 4, "T3"));

    ClusterContext ctx = new ClusterContext(replicas, nodes, Collections.emptyMap(),
        Collections.emptyMap(), c);

    // A previously valid full assignment: every instance holds at most 60 of its 100.
    Map<String, ResourceAssignment> prev = new HashMap<>();
    ResourceAssignment prevR1 = new ResourceAssignment("R1");
    prevR1.addReplicaMap(new Partition("R1_0"), Collections.singletonMap("t1_a", "ONLINE"));
    prevR1.addReplicaMap(new Partition("R1_1"), Collections.singletonMap("moved", "ONLINE"));
    prev.put("R1", prevR1);
    // R2 and R3 were single partition resources back then and have since been scaled to two, so a
    // carried forward entry is visibly smaller than a freshly calculated one.
    ResourceAssignment prevR2 = new ResourceAssignment("R2");
    prevR2.addReplicaMap(new Partition("R2_0"), Collections.singletonMap("t2_b", "ONLINE"));
    prev.put("R2", prevR2);
    ResourceAssignment prevR3 = new ResourceAssignment("R3");
    prevR3.addReplicaMap(new Partition("R3_0"), Collections.singletonMap("t3_a", "ONLINE"));
    prev.put("R3", prevR3);

    Map<String, ResourceAssignment> result = WagedRebalanceUtil.calculateAssignment(
        new ClusterModel(ctx, replicas, nodes, ClusterModel.RebalanceScopeType.GLOBAL_BASELINE),
        algo(), prev);

    Map<String, Integer> weights = new HashMap<>();
    weights.put("R1", WEIGHT);
    weights.put("R2", WEIGHT);
    weights.put("R3", WEIGHT);
    Map<String, Integer> load = loadPerInstance(result, weights);

    Assert.assertEquals(result.get("R1").getRecord(), prevR1.getRecord(),
        "R1 could not be placed, so it must be carried forward. Load was " + load);
    Assert.assertEquals(result.get("R2").getRecord(), prevR2.getRecord(),
        "R2 was freshly placed onto the retagged instance that carried R1 still names, so R2 must "
            + "yield. Load was " + load);

    // The key property. R3 shares no instance with R1 or R2, so a collision between
    // them must not cost R3 the assignment that was just calculated for it.
    Assert.assertFalse(result.get("R3").getRecord().equals(prevR3.getRecord()),
        "R3 is unrelated to the collision, so it must keep its freshly calculated assignment "
            + "instead of being rolled back with the others. Load was " + load);
    Assert.assertEquals(result.get("R3").getMappedPartitions().size(), 2,
        "R3's fresh assignment covers both partitions, the carried forward one covers only one. "
            + "Load was " + load);
    Assert.assertEquals(instances(result.get("R3")), new TreeSet<>(Arrays.asList("t3_a", "t3_b")),
        "R3 must still be spread across its own group. Load was " + load);

    assertNoBreach("collisionBlastRadius", result, weights);
  }

  /**
   * Carrying one group forward can expose the next collision, so the check has to repeat. Two
   * instances moved between groups here, which takes two rounds to settle, and a fourth group that
   * touches none of them must still come through with its freshly calculated assignment.
   */
  @Test
  public void cascadingCollisionsStillSpareTheUnrelatedGroup()
      throws HelixRebalanceException, IOException {
    ClusterConfig c = config(true);
    Set<AssignableReplica> replicas = new HashSet<>();
    add(replicas, c, res("R1", "T1", WEIGHT), 2);   // does not fit on t1_a alone, so it is skipped
    add(replicas, c, res("R2", "T2", WEIGHT), 1);
    add(replicas, c, res("R3", "T3", WEIGHT), 1);
    add(replicas, c, res("R4", "T4", WEIGHT), 2);

    Set<AssignableNode> nodes = new HashSet<>();
    nodes.add(node(c, "t1_a", 0, "T1"));
    nodes.add(node(c, "x", 1, "T2"));   // was T1
    nodes.add(node(c, "y", 2, "T3"));   // was T2
    nodes.add(node(c, "t4_a", 3, "T4"));
    nodes.add(node(c, "t4_b", 4, "T4"));

    ClusterContext ctx = new ClusterContext(replicas, nodes, Collections.emptyMap(),
        Collections.emptyMap(), c);

    Map<String, ResourceAssignment> prev = new HashMap<>();
    ResourceAssignment prevR1 = new ResourceAssignment("R1");
    prevR1.addReplicaMap(new Partition("R1_0"), Collections.singletonMap("t1_a", "ONLINE"));
    prevR1.addReplicaMap(new Partition("R1_1"), Collections.singletonMap("x", "ONLINE"));
    prev.put("R1", prevR1);
    ResourceAssignment prevR2 = new ResourceAssignment("R2");
    prevR2.addReplicaMap(new Partition("R2_0"), Collections.singletonMap("y", "ONLINE"));
    prev.put("R2", prevR2);
    // R3 is new, so there is nothing to carry forward for it and it is dropped instead.

    Map<String, ResourceAssignment> result = WagedRebalanceUtil.calculateAssignment(
        new ClusterModel(ctx, replicas, nodes, ClusterModel.RebalanceScopeType.GLOBAL_BASELINE),
        algo(), prev);

    Map<String, Integer> weights = new HashMap<>();
    weights.put("R1", WEIGHT);
    weights.put("R2", WEIGHT);
    weights.put("R3", WEIGHT);
    weights.put("R4", WEIGHT);
    Map<String, Integer> load = loadPerInstance(result, weights);

    // Round one: R1 is carried forward naming x, so R2 which was just placed on x yields.
    Assert.assertEquals(result.get("R1").getRecord(), prevR1.getRecord(), "load " + load);
    Assert.assertEquals(result.get("R2").getRecord(), prevR2.getRecord(), "load " + load);
    // Round two: R2's carried entry names y, so R3 which was just placed on y yields as well. R3
    // has no previous assignment, so it is dropped rather than carried.
    Assert.assertFalse(result.containsKey("R3"),
        "R3 collided once R2 was carried forward and had nothing to carry, so it must be dropped "
            + "instead of being left on y next to R2. Load was " + load);
    // R4 touches none of t1_a, x or y, so two rounds of cascade must not reach it.
    Assert.assertEquals(instances(result.get("R4")), new TreeSet<>(Arrays.asList("t4_a", "t4_b")),
        "R4 is unrelated to both collisions and must keep its fresh assignment. Load was " + load);

    assertNoBreach("cascade", result, weights);
  }

  /** The instance names a resource assignment actually places replicas on. */
  private TreeSet<String> instances(ResourceAssignment ra) {
    TreeSet<String> out = new TreeSet<>();
    ra.getMappedPartitions().forEach(p -> out.addAll(ra.getReplicaMap(p).keySet()));
    return out;
  }

  /**
   * A collision walkthrough, with a one partition per node cap.
   *
   * Before:  R1[T1] r1p1 -> A, r1p2 -> C     R2[T2] r2p2 -> B
   * After:   R1[T1] r1p1 -> D, r1p2 fails    R2[T2] r2p2 -> A
   *
   * A carries both T1 and T2. WAGED moves R1 off A, R1 then fails to place r1p2, and R2 takes the
   * capacity A just freed. Carrying R1 forward would put r1p1 back on A next to r2p2, giving A two
   * partitions when it may hold one.
   */
  @Test
  public void collisionWalkthroughOnePartitionPerNode()
      throws HelixRebalanceException, IOException {
    ClusterConfig c = config(true);
    Set<AssignableReplica> replicas = new HashSet<>();
    // Weight equal to capacity means each node may hold exactly one partition.
    // 7 partitions and 7 single partition nodes, so cluster wide capacity is exactly sufficient
    // and the failure below is purely one of tag locality, not a global deficit.
    add(replicas, c, res("R1", "T1", NODE_CAPACITY), 3);
    // R2 has as many partitions as it has T2 nodes, so it must take A, the node R1 was using.
    add(replicas, c, res("R2", "T2", NODE_CAPACITY), 4);

    Set<AssignableNode> nodes = new HashSet<>();
    nodes.add(node(c, "A", 0, "T1", "T2"));   // the dual tagged node in the walkthrough
    nodes.add(node(c, "C", 1, "T1"));
    nodes.add(node(c, "D", 2, "T1"));
    nodes.add(node(c, "B", 3, "T2"));
    nodes.add(node(c, "E", 4, "T2"));
    nodes.add(node(c, "F", 5, "T2"));
    // Untagged spare. It keeps total cluster capacity equal to total demand so the cluster wide
    // deficit check passes, while being unusable by either tagged resource.
    nodes.add(node(c, "G", 6));

    ClusterContext ctx = new ClusterContext(replicas, nodes, Collections.emptyMap(),
        Collections.emptyMap(), c);

    // The earlier, valid distribution: one partition per node, R1 sitting on A.
    ResourceAssignment prevR1 = new ResourceAssignment("R1");
    prevR1.addReplicaMap(new Partition("R1_0"), Collections.singletonMap("A", "ONLINE"));
    prevR1.addReplicaMap(new Partition("R1_1"), Collections.singletonMap("C", "ONLINE"));
    prevR1.addReplicaMap(new Partition("R1_2"), Collections.singletonMap("D", "ONLINE"));
    ResourceAssignment prevR2 = new ResourceAssignment("R2");
    prevR2.addReplicaMap(new Partition("R2_0"), Collections.singletonMap("B", "ONLINE"));
    prevR2.addReplicaMap(new Partition("R2_1"), Collections.singletonMap("E", "ONLINE"));
    prevR2.addReplicaMap(new Partition("R2_2"), Collections.singletonMap("F", "ONLINE"));
    Map<String, ResourceAssignment> prev = new HashMap<>();
    prev.put("R1", prevR1);
    prev.put("R2", prevR2);

    Map<String, Integer> weights = new HashMap<>();
    weights.put("R1", NODE_CAPACITY);
    weights.put("R2", NODE_CAPACITY);

    // A carries both T1 and T2, so the share closure merges the two groups into a single block of
    // 6 nodes holding 7 partitions. That block has a real deficit and it is the only block in the
    // cluster, so nothing is skipped and nothing can be spared: the whole calculation fails exactly
    // as the default global mode does. Yielding only the colliding groups does not help here,
    // because there is no unrelated group to protect.
    try {
      Map<String, ResourceAssignment> result = WagedRebalanceUtil.calculateAssignment(
          new ClusterModel(ctx, replicas, nodes, ClusterModel.RebalanceScopeType.GLOBAL_BASELINE),
          algo(), prev);
      // Reaching here would mean isolation engaged: R1 carried over onto A while R2 was freshly
      // placed there too. Assert on the breach directly so the failure names the doubled node.
      Map<String, Integer> load = loadPerInstance(result, weights);
      Assert.fail("Isolation must not engage when A carries both tags. Load was " + load
          + ", R2 present = " + result.containsKey("R2"));
    } catch (HelixRebalanceException expected) {
      LOG.info("collisionWalkthrough correctly failed closed: " + expected.getMessage());
      Assert.assertEquals(expected.getFailureType(),
          HelixRebalanceException.Type.FAILED_TO_CALCULATE);
      Assert.assertEquals(expected.getFailureCategory(),
          HelixRebalanceException.FailureCategory.NO_CANDIDATE_NODE);
    }
  }

  /**
   * Control for the walkthrough above: with isolation disabled the same topology fails the same
   * way, which is what "never worse than the default" has to mean concretely.
   */
  @Test
  public void collisionWalkthroughMatchesGlobalModeWhenFlagOff() throws IOException {
    ClusterConfig c = config(false);
    Set<AssignableReplica> replicas = new HashSet<>();
    add(replicas, c, res("R1", "T1", NODE_CAPACITY), 3);
    add(replicas, c, res("R2", "T2", NODE_CAPACITY), 4);

    Set<AssignableNode> nodes = new HashSet<>();
    nodes.add(node(c, "A", 0, "T1", "T2"));
    nodes.add(node(c, "C", 1, "T1"));
    nodes.add(node(c, "D", 2, "T1"));
    nodes.add(node(c, "B", 3, "T2"));
    nodes.add(node(c, "E", 4, "T2"));
    nodes.add(node(c, "F", 5, "T2"));
    nodes.add(node(c, "G", 6));

    ClusterContext ctx = new ClusterContext(replicas, nodes, Collections.emptyMap(),
        Collections.emptyMap(), c);

    ResourceAssignment prevR1 = new ResourceAssignment("R1");
    prevR1.addReplicaMap(new Partition("R1_0"), Collections.singletonMap("A", "ONLINE"));
    prevR1.addReplicaMap(new Partition("R1_1"), Collections.singletonMap("C", "ONLINE"));
    prevR1.addReplicaMap(new Partition("R1_2"), Collections.singletonMap("D", "ONLINE"));
    Map<String, ResourceAssignment> prev = new HashMap<>();
    prev.put("R1", prevR1);

    try {
      WagedRebalanceUtil.calculateAssignment(
          new ClusterModel(ctx, replicas, nodes, ClusterModel.RebalanceScopeType.GLOBAL_BASELINE),
          algo(), prev);
      Assert.fail("The default global mode should also fail on this topology");
    } catch (HelixRebalanceException expected) {
      LOG.info("flagOffControl also threw: " + expected.getFailureType() + " / "
          + expected.getFailureCategory());
      Assert.assertEquals(expected.getFailureType(),
          HelixRebalanceException.Type.FAILED_TO_CALCULATE);
      Assert.assertEquals(expected.getFailureCategory(),
          HelixRebalanceException.FailureCategory.NO_CANDIDATE_NODE);
    }
  }

  /**
   * The same walkthrough with A carrying only T1, which is the clique topology this feature
   * targets. Isolation may engage here, and A must still hold exactly one partition.
   */
  @Test
  public void collisionWalkthroughIsolatesSafelyWhenTagsAreDisjoint()
      throws HelixRebalanceException, IOException {
    ClusterConfig c = config(true);
    Set<AssignableReplica> replicas = new HashSet<>();
    add(replicas, c, res("R1", "T1", NODE_CAPACITY), 4);   // one more than T1 has room for
    add(replicas, c, res("R2", "T2", NODE_CAPACITY), 4);

    Set<AssignableNode> nodes = new HashSet<>();
    nodes.add(node(c, "A", 0, "T1"));   // single tag, the only difference from the walkthrough
    nodes.add(node(c, "C", 1, "T1"));
    nodes.add(node(c, "D", 2, "T1"));
    nodes.add(node(c, "B", 3, "T2"));
    nodes.add(node(c, "E", 4, "T2"));
    nodes.add(node(c, "F", 5, "T2"));
    nodes.add(node(c, "H", 6, "T2"));
    nodes.add(node(c, "G", 7));

    ClusterContext ctx = new ClusterContext(replicas, nodes, Collections.emptyMap(),
        Collections.emptyMap(), c);

    ResourceAssignment prevR1 = new ResourceAssignment("R1");
    prevR1.addReplicaMap(new Partition("R1_0"), Collections.singletonMap("A", "ONLINE"));
    prevR1.addReplicaMap(new Partition("R1_1"), Collections.singletonMap("C", "ONLINE"));
    prevR1.addReplicaMap(new Partition("R1_2"), Collections.singletonMap("D", "ONLINE"));

    Map<String, ResourceAssignment> result = WagedRebalanceUtil.calculateAssignment(
        new ClusterModel(ctx, replicas, nodes, ClusterModel.RebalanceScopeType.GLOBAL_BASELINE),
        algo(), Collections.singletonMap("R1", prevR1));

    Map<String, Integer> weights = new HashMap<>();
    weights.put("R1", NODE_CAPACITY);
    weights.put("R2", NODE_CAPACITY);
    assertNoBreach("disjointWalkthrough", result, weights);
    Assert.assertTrue(result.containsKey("R2"), "the healthy T2 group must still be assigned");
    Assert.assertEquals(result.get("R2").getMappedPartitions().size(), 4,
        "all four T2 partitions should be placed while T1 is carried over");
  }

  /**
   * The delayed rebalance overwrite path passes a null previous assignment, so a skipped resource
   * is dropped rather than carried over. Nothing is then carried onto any node, so the guard must
   * stay silent. A false positive here would break delayed rebalance overwrites.
   */
  @Test
  public void nullPreviousAssignmentDropsSkippedResourceWithoutTrippingTheGuard()
      throws HelixRebalanceException, IOException {
    ClusterConfig c = config(true);
    Set<AssignableReplica> replicas = new HashSet<>();
    add(replicas, c, res("R1", "T1", NODE_CAPACITY), 3);   // only two T1 nodes, so one fails
    add(replicas, c, res("R2", "T2", NODE_CAPACITY), 1);

    Set<AssignableNode> nodes = new HashSet<>();
    nodes.add(node(c, "t1a", 0, "T1"));
    nodes.add(node(c, "t1b", 1, "T1"));
    nodes.add(node(c, "t2a", 2, "T2"));
    nodes.add(node(c, "spare", 3));   // keeps total capacity at or above total demand

    ClusterContext ctx = new ClusterContext(replicas, nodes, Collections.emptyMap(),
        Collections.emptyMap(), c);

    Map<String, ResourceAssignment> result = WagedRebalanceUtil.calculateAssignment(
        new ClusterModel(ctx, replicas, nodes, ClusterModel.RebalanceScopeType.GLOBAL_BASELINE),
        algo(), null);

    LOG.info("nullPrevious result keys = " + result.keySet());
    Assert.assertFalse(result.containsKey("R1"),
        "a skipped resource with no previous assignment must be dropped, not emitted partially");
    Assert.assertTrue(result.containsKey("R2"), "the healthy group must still be assigned");
  }

  /**
   * Overlap chain: A carries T1 and T2, B carries T2 and T3. T1 and T3 share no node, yet they are
   * joined transitively through T2, so the share closure is all three. Here that is every group in
   * the cluster, so there is nothing left to rebalance around them and the run fails globally,
   * exactly as the default mode would.
   */
  @Test
  public void overlapChainCoveringEveryGroupStillFailsGlobally() throws IOException {
    ClusterConfig c = config(true);
    Set<AssignableReplica> replicas = new HashSet<>();
    add(replicas, c, res("R1", "T1", NODE_CAPACITY), 2);   // only one T1 node, so R1 must fail
    add(replicas, c, res("R2", "T2", NODE_CAPACITY), 1);
    add(replicas, c, res("R3", "T3", NODE_CAPACITY), 1);

    Set<AssignableNode> nodes = new HashSet<>();
    nodes.add(node(c, "A", 0, "T1", "T2"));
    nodes.add(node(c, "B", 1, "T2", "T3"));
    nodes.add(node(c, "spare0", 2));
    nodes.add(node(c, "spare1", 3));

    ClusterContext ctx = new ClusterContext(replicas, nodes, Collections.emptyMap(),
        Collections.emptyMap(), c);

    ResourceAssignment prevR1 = new ResourceAssignment("R1");
    prevR1.addReplicaMap(new Partition("R1_0"), Collections.singletonMap("A", "ONLINE"));

    try {
      Map<String, ResourceAssignment> result = WagedRebalanceUtil.calculateAssignment(
          new ClusterModel(ctx, replicas, nodes, ClusterModel.RebalanceScopeType.GLOBAL_BASELINE),
          algo(), Collections.singletonMap("R1", prevR1));
      Assert.fail("A closure spanning every group must fail globally, but got " + result.keySet());
    } catch (HelixRebalanceException expected) {
      // T1 and T3 share no node with each other, but each shares one with T2, so all three are
      // carried over together. That is necessary, not merely conservative, while a failing group
      // releases its capacity: T1 carried back onto A would collide with whatever T2 just placed
      // there. With no group outside the closure, this degenerates to the default global failure.
      LOG.info("overlapChain: closure spans every group, so nothing could be isolated");
    }
  }

  /**
   * The shared instance case next to a clean clique. A carries T1 and T2 so those two groups are
   * one block, but T3 sits on nodes of its own. The block is carried over and the clean clique
   * still rebalances, which is the reason to carry the closure over rather than refuse to isolate.
   */
  @Test
  public void sharedBlockIsCarriedOverWhileACleanCliqueKeepsRebalancing()
      throws HelixRebalanceException, IOException {
    ClusterConfig c = config(true);
    Set<AssignableReplica> replicas = new HashSet<>();
    add(replicas, c, res("R1", "T1", NODE_CAPACITY), 3);   // only two T1 nodes, so R1 must fail
    add(replicas, c, res("R2", "T2", NODE_CAPACITY), 1);
    add(replicas, c, res("R3", "T3", NODE_CAPACITY), 1);   // clean clique, entirely healthy

    Set<AssignableNode> nodes = new HashSet<>();
    nodes.add(node(c, "A", 0, "T1", "T2"));   // two tags, so T1 and T2 are one block
    nodes.add(node(c, "C", 1, "T1"));
    nodes.add(node(c, "B", 2, "T2"));
    nodes.add(node(c, "clean0", 3, "T3"));
    nodes.add(node(c, "clean1", 4, "T3"));
    nodes.add(node(c, "spare", 5));

    ClusterContext ctx = new ClusterContext(replicas, nodes, Collections.emptyMap(),
        Collections.emptyMap(), c);

    ResourceAssignment prevR1 = new ResourceAssignment("R1");
    prevR1.addReplicaMap(new Partition("R1_0"), Collections.singletonMap("A", "ONLINE"));
    prevR1.addReplicaMap(new Partition("R1_1"), Collections.singletonMap("C", "ONLINE"));
    ResourceAssignment prevR2 = new ResourceAssignment("R2");
    prevR2.addReplicaMap(new Partition("R2_0"), Collections.singletonMap("B", "ONLINE"));
    Map<String, ResourceAssignment> previous = new HashMap<>();
    previous.put("R1", prevR1);
    previous.put("R2", prevR2);

    Map<String, Integer> weights = new HashMap<>();
    weights.put("R1", NODE_CAPACITY);
    weights.put("R2", NODE_CAPACITY);
    weights.put("R3", NODE_CAPACITY);

    Map<String, ResourceAssignment> result = WagedRebalanceUtil.calculateAssignment(
        new ClusterModel(ctx, replicas, nodes, ClusterModel.RebalanceScopeType.GLOBAL_BASELINE),
        algo(), previous);
    LOG.info("sharedBlock survivors = " + result.keySet());
    Assert.assertTrue(result.containsKey("R3"),
        "the clean T3 clique must still rebalance, got " + result.keySet());
    // R1 and R2 are one block, so both keep their previous assignment rather than a fresh one.
    Assert.assertEquals(result.get("R1").getMappedPartitions().size(), 2,
        "R1 must be carried over verbatim, not recalculated");
    Assert.assertEquals(result.get("R2").getReplicaMap(new Partition("R2_0")).keySet(),
        Collections.singleton("B"), "R2 must be carried over verbatim, not recalculated");
    assertNoBreach("sharedBlock", result, weights);
  }

  /**
   * A cluster wide capacity deficit caused by an overlapping block. The tag blind precheck sees the
   * whole cluster short, but only the T1 plus T2 block cannot hold its own replicas on its own
   * nodes. Attributing the deficit to the whole block is what lets the clean T3 clique carry on:
   * the overlapping block is blamed as a unit, so the whole cluster does not freeze.
   */
  @Test
  public void capacityDeficitIsAttributedToTheOverlappingBlock()
      throws HelixRebalanceException, IOException {
    ClusterConfig c = config(true);
    Set<AssignableReplica> replicas = new HashSet<>();
    add(replicas, c, res("R1", "T1", NODE_CAPACITY), 5);   // 500 of demand for 200 of T1 capacity
    add(replicas, c, res("R2", "T2", NODE_CAPACITY), 1);
    add(replicas, c, res("R3", "T3", NODE_CAPACITY), 1);   // clean clique, fits exactly

    Set<AssignableNode> nodes = new HashSet<>();
    nodes.add(node(c, "A", 0, "T1", "T2"));   // joins T1 and T2 into one block
    nodes.add(node(c, "C", 1, "T1"));
    nodes.add(node(c, "B", 2, "T2"));
    nodes.add(node(c, "clean0", 3, "T3"));

    ClusterContext ctx = new ClusterContext(replicas, nodes, Collections.emptyMap(),
        Collections.emptyMap(), c);

    ResourceAssignment prevR1 = new ResourceAssignment("R1");
    prevR1.addReplicaMap(new Partition("R1_0"), Collections.singletonMap("C", "ONLINE"));
    Map<String, Integer> weights = new HashMap<>();
    weights.put("R1", NODE_CAPACITY);
    weights.put("R2", NODE_CAPACITY);
    weights.put("R3", NODE_CAPACITY);

    Map<String, ResourceAssignment> result = WagedRebalanceUtil.calculateAssignment(
        new ClusterModel(ctx, replicas, nodes, ClusterModel.RebalanceScopeType.GLOBAL_BASELINE),
        algo(), Collections.singletonMap("R1", prevR1));
    LOG.info("blockDeficit survivors = " + result.keySet());
    Assert.assertTrue(result.containsKey("R3"),
        "the clean T3 clique must survive an overlapping block's deficit, got " + result.keySet());
    Assert.assertFalse(result.containsKey("R2"),
        "R2 is in the blamed block and has no previous assignment, so it must be dropped");
    assertNoBreach("blockDeficit", result, weights);
  }

  /** Same deficit with the flag off must still fail exactly as the default mode does. */
  @Test
  public void capacityDeficitStillFailsGloballyWhenFlagOff() throws IOException {
    ClusterConfig c = config(false);
    Set<AssignableReplica> replicas = new HashSet<>();
    add(replicas, c, res("R1", "T1", NODE_CAPACITY), 5);
    add(replicas, c, res("R2", "T2", NODE_CAPACITY), 1);
    add(replicas, c, res("R3", "T3", NODE_CAPACITY), 1);

    Set<AssignableNode> nodes = new HashSet<>();
    nodes.add(node(c, "A", 0, "T1", "T2"));
    nodes.add(node(c, "C", 1, "T1"));
    nodes.add(node(c, "B", 2, "T2"));
    nodes.add(node(c, "clean0", 3, "T3"));

    ClusterContext ctx = new ClusterContext(replicas, nodes, Collections.emptyMap(),
        Collections.emptyMap(), c);

    try {
      WagedRebalanceUtil.calculateAssignment(
          new ClusterModel(ctx, replicas, nodes, ClusterModel.RebalanceScopeType.GLOBAL_BASELINE),
          algo(), Collections.emptyMap());
      Assert.fail("the flag off path must fail the whole rebalance");
    } catch (HelixRebalanceException expected) {
      LOG.info("blockDeficit flag off threw as expected: " + expected.getFailureType());
      Assert.assertEquals(expected.getFailureType(),
          HelixRebalanceException.Type.FAILED_TO_CALCULATE);
      Assert.assertEquals(expected.getFailureCategory(),
          HelixRebalanceException.FailureCategory.CAPACITY_DEFICIT);
    }
  }

  /**
   * A mis tagged instance must not disable isolation cluster wide. T1, T2 and T3 overlap on A and
   * B and so form one block, but T4 sits on nodes of its own and must still be isolated on its own
   * when it fails, leaving the overlapping block free to rebalance normally.
   */
  @Test
  public void overlapDoesNotDisableIsolationForCleanGroups()
      throws HelixRebalanceException, IOException {
    ClusterConfig c = config(true);
    Set<AssignableReplica> replicas = new HashSet<>();
    add(replicas, c, res("R1", "T1", NODE_CAPACITY), 1);
    add(replicas, c, res("R2", "T2", NODE_CAPACITY), 1);
    add(replicas, c, res("R3", "T3", NODE_CAPACITY), 1);
    add(replicas, c, res("R4", "T4", NODE_CAPACITY), 3);   // only two T4 nodes, so R4 fails

    Set<AssignableNode> nodes = new HashSet<>();
    nodes.add(node(c, "A", 0, "T1", "T2"));      // overlap
    nodes.add(node(c, "B", 1, "T2", "T3"));      // overlap
    // Each overlapping group needs a node of its own, otherwise whichever group is placed first
    // takes the single shared node and starves the others, which fails the run for an unrelated
    // reason and tells us nothing about scoping.
    nodes.add(node(c, "t1extra", 2, "T1"));
    nodes.add(node(c, "t2extra", 3, "T2"));
    nodes.add(node(c, "t3extra", 4, "T3"));
    nodes.add(node(c, "c4a", 5, "T4"));          // clean clique, the only group that fails
    nodes.add(node(c, "c4b", 6, "T4"));
    nodes.add(node(c, "spare", 7));

    ClusterContext ctx = new ClusterContext(replicas, nodes, Collections.emptyMap(),
        Collections.emptyMap(), c);

    ResourceAssignment prevR4 = new ResourceAssignment("R4");
    prevR4.addReplicaMap(new Partition("R4_0"), Collections.singletonMap("c4a", "ONLINE"));
    prevR4.addReplicaMap(new Partition("R4_1"), Collections.singletonMap("c4b", "ONLINE"));

    Map<String, ResourceAssignment> result = WagedRebalanceUtil.calculateAssignment(
        new ClusterModel(ctx, replicas, nodes, ClusterModel.RebalanceScopeType.GLOBAL_BASELINE),
        algo(), Collections.singletonMap("R4", prevR4));

    LOG.info("scopedOverlap result keys = " + result.keySet());
    Map<String, Integer> weights = new HashMap<>();
    for (String r : new String[] {"R1", "R2", "R3", "R4"}) {
      weights.put(r, NODE_CAPACITY);
    }
    assertNoBreach("scopedOverlap", result, weights);
    // The whole point: the overlapping groups were never blocked by T4's failure.
    for (String healthy : new String[] {"R1", "R2", "R3"}) {
      Assert.assertTrue(result.containsKey(healthy),
          healthy + " should still be assigned while the clean T4 clique is isolated");
    }
    Assert.assertEquals(result.get("R4").getMappedPartitions().size(), 2,
        "R4 should be carried over from its previous assignment, not recomputed");
  }

  /**
   * A failure in an overlapping group is isolated together with its whole share closure. T1 fails,
   * A joins T1 to T2 and B joins T2 to T3, so all three are carried over as one block, while the
   * clean T4 clique shares no node with them and still gets a freshly calculated assignment.
   */
  @Test
  public void closureIsolatesTheWholeOverlapAndSparesCleanCliques()
      throws IOException, HelixRebalanceException {
    ClusterConfig c = config(true);
    Set<AssignableReplica> replicas = new HashSet<>();
    add(replicas, c, res("R1", "T1", NODE_CAPACITY), 2);   // only one T1 node, so T1 fails
    add(replicas, c, res("R2", "T2", NODE_CAPACITY), 1);
    add(replicas, c, res("R3", "T3", NODE_CAPACITY), 1);
    add(replicas, c, res("R4", "T4", NODE_CAPACITY), 1);   // clean clique, entirely healthy

    Set<AssignableNode> nodes = new HashSet<>();
    nodes.add(node(c, "A", 0, "T1", "T2"));
    nodes.add(node(c, "B", 1, "T2", "T3"));
    nodes.add(node(c, "t2extra", 2, "T2"));
    nodes.add(node(c, "t3extra", 3, "T3"));
    nodes.add(node(c, "c4a", 4, "T4"));
    nodes.add(node(c, "c4b", 5, "T4"));
    nodes.add(node(c, "spare", 6));

    ClusterContext ctx = new ClusterContext(replicas, nodes, Collections.emptyMap(),
        Collections.emptyMap(), c);

    ResourceAssignment prevR1 = new ResourceAssignment("R1");
    prevR1.addReplicaMap(new Partition("R1_0"), Collections.singletonMap("A", "ONLINE"));

    Map<String, Integer> weights = new HashMap<>();
    weights.put("R1", NODE_CAPACITY);
    weights.put("R2", NODE_CAPACITY);
    weights.put("R3", NODE_CAPACITY);
    weights.put("R4", NODE_CAPACITY);

    Map<String, ResourceAssignment> result = WagedRebalanceUtil.calculateAssignment(
        new ClusterModel(ctx, replicas, nodes, ClusterModel.RebalanceScopeType.GLOBAL_BASELINE),
        algo(), Collections.singletonMap("R1", prevR1));
    LOG.info("overlapFailure ISOLATED, survivors = " + result.keySet());
    // The share closure carries T1, T2 and T3 over together, so the clean T4 clique still gets a
    // freshly calculated assignment instead of being dragged down with them.
    Assert.assertTrue(result.containsKey("R4"),
        "clean T4 clique should still rebalance, got " + result.keySet());
    Assert.assertFalse(result.containsKey("R2"),
        "R2 shares a node with T1 and has no previous assignment, so it must be dropped");
    Assert.assertFalse(result.containsKey("R3"),
        "R3 is transitively in T1's share closure and has no previous assignment");
    assertNoBreach("overlapFailure", result, weights);
  }

  /**
   * Three cliques of two nodes each, a1 and a2 on T1, b1 and b2 on T2, c1 and c2 on T3, plus two
   * untagged spares. With {@code a1AlsoCarriesT2}, a1 carries T2 as well.
   */
  private Set<AssignableNode> threeCliques(ClusterConfig c, boolean a1AlsoCarriesT2) {
    Set<AssignableNode> nodes = new HashSet<>();
    nodes.add(a1AlsoCarriesT2 ? node(c, "a1", 0, "T1", "T2") : node(c, "a1", 0, "T1"));
    nodes.add(node(c, "a2", 1, "T1"));
    nodes.add(node(c, "b1", 2, "T2"));
    nodes.add(node(c, "b2", 3, "T2"));
    nodes.add(node(c, "c1", 4, "T3"));
    nodes.add(node(c, "c2", 5, "T3"));
    nodes.add(node(c, "s0", 6));
    nodes.add(node(c, "s1", 7));
    return nodes;
  }

  /**
   * The same three cliques with a zone label on every clique node, az1 on the T1 and T2 nodes and
   * az2 on the T3 nodes, plus the given number of untagged spares. No resource is pinned to either
   * label.
   */
  private Set<AssignableNode> zonedCliques(ClusterConfig c, int spares) {
    Set<AssignableNode> nodes = new HashSet<>();
    nodes.add(node(c, "a1", 0, "T1", "az1"));
    nodes.add(node(c, "a2", 1, "T1", "az1"));
    nodes.add(node(c, "b1", 2, "T2", "az1"));
    nodes.add(node(c, "b2", 3, "T2", "az1"));
    nodes.add(node(c, "c1", 4, "T3", "az2"));
    nodes.add(node(c, "c2", 5, "T3", "az2"));
    for (int i = 0; i < spares; i++) {
      nodes.add(node(c, "s" + i, 6 + i));
    }
    return nodes;
  }

  /**
   * R1 on T1, R2 on T2 and R3 on T3 with the given partition counts. Each partition fills a whole
   * node.
   */
  private Set<AssignableReplica> cliqueReplicas(ClusterConfig c, int r1, int r2, int r3)
      throws IOException {
    Set<AssignableReplica> replicas = new HashSet<>();
    add(replicas, c, res("R1", "T1", NODE_CAPACITY), r1);
    add(replicas, c, res("R2", "T2", NODE_CAPACITY), r2);
    add(replicas, c, res("R3", "T3", NODE_CAPACITY), r3);
    return replicas;
  }

  private Map<String, Integer> cliqueWeights() {
    Map<String, Integer> weights = new HashMap<>();
    for (String r : new String[] {"R1", "R2", "R3"}) {
      weights.put(r, NODE_CAPACITY);
    }
    return weights;
  }

  /**
   * The assignment the label tests start from: R1 on a1 and a2, and one partition each of R2 on b1
   * and R3 on c1.
   */
  private Map<String, ResourceAssignment> labelStart() {
    Map<String, ResourceAssignment> start = new HashMap<>();
    ResourceAssignment r1 = new ResourceAssignment("R1");
    r1.addReplicaMap(new Partition("R1_0"), Collections.singletonMap("a1", "ONLINE"));
    r1.addReplicaMap(new Partition("R1_1"), Collections.singletonMap("a2", "ONLINE"));
    start.put("R1", r1);
    ResourceAssignment r2 = new ResourceAssignment("R2");
    r2.addReplicaMap(new Partition("R2_0"), Collections.singletonMap("b1", "ONLINE"));
    start.put("R2", r2);
    ResourceAssignment r3 = new ResourceAssignment("R3");
    r3.addReplicaMap(new Partition("R3_0"), Collections.singletonMap("c1", "ONLINE"));
    start.put("R3", r3);
    return start;
  }

  /**
   * A tag added to a live instance merges two share blocks, and removing it splits them again.
   * Three rebalances run with one algorithm, each starting from the assignment the step before it
   * returned. Per step, asserts which resources isolation skips, which are carried over and which
   * converge on a freshly calculated assignment:
   *
   *   step 1  nothing fails, so nothing is skipped and every resource is placed on its own nodes
   *   step 2  a1 also carries T2 and R1 needs a third T1 node. R1 and R2 are skipped and carried
   *           over from step 1 unchanged, and R3 converges on c1 and c2
   *   step 3  a1 drops T2 and R1 still needs a third T1 node. R1 alone is skipped and carried over
   *           from step 2, still as placed in step 1, and R2 and R3 converge on their own nodes
   */
  @Test
  public void tagAddedToALiveInstanceMergesBlocksAndRemovingItSplitsThem()
      throws HelixRebalanceException, IOException {
    ClusterConfig c = config(true);
    Map<String, Integer> weights = cliqueWeights();
    // One algorithm for every step, the way the WAGED rebalancer keeps one across rebalances, so
    // nothing a run derives from the topology may outlive that run.
    RecordingAlgorithm algorithm = new RecordingAlgorithm();

    Map<String, ResourceAssignment> step1 = rebalance(algorithm, c, cliqueReplicas(c, 2, 1, 1),
        threeCliques(c, false), Collections.emptyMap());
    Assert.assertEquals(algorithm._isolated, setOf(), "step 1: " + step1);
    Assert.assertEquals(algorithm._skipped, setOf(), "step 1: " + step1);
    Assert.assertEquals(instances(step1.get("R1")), setOf("a1", "a2"), "step 1: " + step1);
    Assert.assertEquals(step1.get("R2").getMappedPartitions().size(), 1, "step 1: " + step1);
    Assert.assertTrue(setOf("b1", "b2").containsAll(instances(step1.get("R2"))),
        "step 1: " + step1);
    Assert.assertEquals(step1.get("R3").getMappedPartitions().size(), 1, "step 1: " + step1);
    Assert.assertTrue(setOf("c1", "c2").containsAll(instances(step1.get("R3"))),
        "step 1: " + step1);

    // Step 2: a1 also carries T2, so T1 and T2 are one block. Every resource gains a partition,
    // and R1 needs a third T1 node that does not exist.
    Map<String, ResourceAssignment> step2 =
        rebalance(algorithm, c, cliqueReplicas(c, 3, 2, 2), threeCliques(c, true), step1);
    Map<String, Integer> load2 = loadPerInstance(step2, weights);
    Assert.assertEquals(algorithm._isolated, setOf("R1", "R2"),
        "step 2: a1 joins T1 and T2 into one block, so R1's failure must take R2 with it. Load "
            + load2);
    Assert.assertEquals(algorithm._skipped, setOf("R1", "R2"), "step 2 load " + load2);
    Assert.assertEquals(step2.get("R1").getRecord(), step1.get("R1").getRecord(),
        "step 2: R1 must be carried over from step 1. Load " + load2);
    Assert.assertEquals(step2.get("R2").getRecord(), step1.get("R2").getRecord(),
        "step 2: R2 must be carried over from step 1 with its single partition. Load " + load2);
    Assert.assertEquals(step2.get("R3").getMappedPartitions().size(), 2, "step 2 load " + load2);
    Assert.assertEquals(instances(step2.get("R3")), setOf("c1", "c2"), "step 2 load " + load2);
    assertNoBreach("tagAdded", step2, weights);

    // Step 3: a1 drops T2, so T1 and T2 are separate blocks. R1 still needs a third T1 node.
    Map<String, ResourceAssignment> step3 =
        rebalance(algorithm, c, cliqueReplicas(c, 3, 2, 2), threeCliques(c, false), step2);
    Map<String, Integer> load3 = loadPerInstance(step3, weights);
    Assert.assertEquals(algorithm._isolated, setOf("R1"),
        "step 3: T1 and T2 share no node, so R1's failure must not take R2 with it. Load "
            + load3);
    Assert.assertEquals(algorithm._skipped, setOf("R1"), "step 3 load " + load3);
    Assert.assertEquals(step3.get("R1").getRecord(), step1.get("R1").getRecord(),
        "step 3: R1 must be carried over from step 2, still as placed in step 1. Load " + load3);
    Assert.assertEquals(step3.get("R2").getMappedPartitions().size(), 2, "step 3 load " + load3);
    Assert.assertEquals(instances(step3.get("R2")), setOf("b1", "b2"), "step 3 load " + load3);
    Assert.assertEquals(step3.get("R3").getMappedPartitions().size(), 2, "step 3 load " + load3);
    Assert.assertEquals(instances(step3.get("R3")), setOf("c1", "c2"), "step 3 load " + load3);
    assertNoBreach("tagRemoved", step3, weights);
  }

  /**
   * Tags no resource is pinned to join no blocks in the share closure. Every clique node also
   * carries a zone label, and az1 sits on the T1 and the T2 nodes alike. Asserts that R1's failure
   * isolates R1 alone, that R1 is carried over exactly as the run started, and that R2 and R3 each
   * converge from the one partition they start with to two on their own nodes.
   */
  @Test
  public void labelsNoResourceIsPinnedToJoinNoShareBlocks()
      throws HelixRebalanceException, IOException {
    ClusterConfig c = config(true);
    Map<String, Integer> weights = cliqueWeights();
    Map<String, ResourceAssignment> start = labelStart();
    RecordingAlgorithm algorithm = new RecordingAlgorithm();

    // R1 needs a third T1 node. Two untagged spares keep total capacity above total demand, so the
    // failure is R1 finding no candidate node rather than a cluster wide deficit.
    Map<String, ResourceAssignment> result =
        rebalance(algorithm, c, cliqueReplicas(c, 3, 2, 2), zonedCliques(c, 2), start);
    Map<String, Integer> load = loadPerInstance(result, weights);
    Assert.assertEquals(algorithm._isolated, setOf("R1"),
        "no resource is pinned to az1, so it must not join T1 and T2 into one block. Load " + load);
    Assert.assertEquals(algorithm._skipped, setOf("R1"), "load " + load);
    Assert.assertEquals(result.get("R1").getRecord(), start.get("R1").getRecord(),
        "load " + load);
    Assert.assertEquals(result.get("R2").getMappedPartitions().size(), 2, "load " + load);
    Assert.assertEquals(instances(result.get("R2")), setOf("b1", "b2"), "load " + load);
    Assert.assertEquals(result.get("R3").getMappedPartitions().size(), 2, "load " + load);
    Assert.assertEquals(instances(result.get("R3")), setOf("c1", "c2"), "load " + load);
    assertNoBreach("labelClosure", result, weights);
  }

  /**
   * Tags no resource is pinned to join no blocks in the capacity deficit attribution either. The
   * same zoned cliques with no spare nodes, where R1 asks for 500 of the 200 its T1 nodes hold, so
   * the cluster as a whole is 300 short. Asserts first that the same cluster with the flag off
   * fails the tag blind capacity check, so with the flag on it is the deficit attribution that sets
   * R1 aside, before any replica is placed. Then asserts that the deficit is attributed to T1
   * alone: R1 is the only resource isolated and is carried over exactly as the run started, and R2
   * and R3 each converge from the one partition they start with to two on their own nodes.
   */
  @Test
  public void labelsNoResourceIsPinnedToJoinNoDeficitBlocks()
      throws HelixRebalanceException, IOException {
    ClusterConfig off = config(false);
    try {
      rebalance(algo(), off, cliqueReplicas(off, 5, 2, 2), zonedCliques(off, 0), labelStart());
      Assert.fail("with the flag off the cluster wide capacity check must fail the run");
    } catch (HelixRebalanceException e) {
      Assert.assertEquals(e.getFailureCategory(),
          HelixRebalanceException.FailureCategory.CAPACITY_DEFICIT, e.getMessage());
    }

    ClusterConfig c = config(true);
    Map<String, Integer> weights = cliqueWeights();
    Map<String, ResourceAssignment> start = labelStart();
    RecordingAlgorithm algorithm = new RecordingAlgorithm();

    Map<String, ResourceAssignment> result =
        rebalance(algorithm, c, cliqueReplicas(c, 5, 2, 2), zonedCliques(c, 0), start);
    Map<String, Integer> load = loadPerInstance(result, weights);
    Assert.assertEquals(algorithm._isolated, setOf("R1"),
        "no resource is pinned to az1, so it must not join T1 and T2 into one block and blame T2 "
            + "for T1's deficit. Load " + load);
    Assert.assertEquals(algorithm._skipped, setOf("R1"), "load " + load);
    Assert.assertEquals(result.get("R1").getRecord(), start.get("R1").getRecord(),
        "load " + load);
    Assert.assertEquals(result.get("R2").getMappedPartitions().size(), 2, "load " + load);
    Assert.assertEquals(instances(result.get("R2")), setOf("b1", "b2"), "load " + load);
    Assert.assertEquals(result.get("R3").getMappedPartitions().size(), 2, "load " + load);
    Assert.assertEquals(instances(result.get("R3")), setOf("c1", "c2"), "load " + load);
    assertNoBreach("labelDeficit", result, weights);
  }
}
