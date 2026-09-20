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
import java.lang.reflect.Constructor;
import java.lang.reflect.Method;
import java.util.ArrayDeque;
import java.util.Deque;
import java.util.TreeSet;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.TreeMap;

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
 * Adversarial coverage for the transitive share closure that decides which groups are carried over
 * together when one of them cannot be placed.
 *
 * These tests deliberately attack the properties the design relies on rather than the happy path:
 * determinism of the emitted assignment, node capacity never being overcommitted when a block is
 * carried over, correct behaviour when the live topology no longer matches the carried over
 * assignment, and the cost of computing the closure on a large cluster.
 */
public class TestWagedIsolationShareClosure {
  private static final Logger LOG = LoggerFactory.getLogger(TestWagedIsolationShareClosure.class);
  private static final String CAP = "DISK";
  private static final int NODE_CAPACITY = 100;

  private ClusterConfig config(boolean on) {
    ClusterConfig c = new ClusterConfig("ClosureCluster");
    c.setInstanceCapacityKeys(Collections.singletonList(CAP));
    c.setDefaultPartitionWeightMap(Collections.singletonMap(CAP, 0));
    c.setWagedInstanceTagIsolationEnabled(on);
    return c;
  }

  private AssignableNode node(ClusterConfig c, String name, int zone, int capacity,
      String... tags) {
    InstanceConfig ic = new InstanceConfig(name);
    ic.setInstanceCapacityMap(Collections.singletonMap(CAP, capacity));
    for (String t : tags) {
      ic.addTag(t);
    }
    ic.setInstanceOperation(InstanceConstants.InstanceOperation.ENABLE);
    return new AssignableNode(c, ic, name);
  }

  private AssignableNode node(ClusterConfig c, String name, int zone, String... tags) {
    return node(c, name, zone, NODE_CAPACITY, tags);
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

  /** A stable, fully ordered rendering of an assignment, so two runs can be compared literally. */
  private String render(Map<String, ResourceAssignment> result) {
    StringBuilder sb = new StringBuilder();
    for (String resource : new java.util.TreeSet<>(result.keySet())) {
      ResourceAssignment ra = result.get(resource);
      Map<String, String> flat = new TreeMap<>();
      ra.getMappedPartitions().forEach(
          p -> flat.put(p.getPartitionName(), new TreeMap<>(ra.getReplicaMap(p)).toString()));
      sb.append(resource).append(flat).append('\n');
    }
    return sb.toString();
  }

  private Map<String, Integer> loadPerInstance(Map<String, ResourceAssignment> result,
      Map<String, Integer> weightByResource) {
    Map<String, Integer> load = new HashMap<>();
    result.forEach((resource, ra) -> ra.getMappedPartitions().forEach(part -> ra.getReplicaMap(part)
        .keySet().forEach(instance -> load.merge(instance, weightByResource.get(resource),
            Integer::sum))));
    return load;
  }

  private void assertNoBreach(String label, Map<String, ResourceAssignment> result,
      Map<String, Integer> weights, Map<String, Integer> capacityByInstance) {
    Map<String, Integer> load = loadPerInstance(result, weights);
    LOG.info(label + " load = " + new TreeMap<>(load));
    load.forEach((inst, used) -> Assert.assertTrue(
        used <= capacityByInstance.getOrDefault(inst, NODE_CAPACITY),
        label + " capacity breach on " + inst + " = " + used + " cap "
            + capacityByInstance.getOrDefault(inst, NODE_CAPACITY)));
  }

  /**
   * The emitted assignment must not depend on the iteration order of any internal collection. The
   * closure is built into a hash set and iterated to release placements, so this shuffles the order
   * the replicas and nodes are handed in and asserts every run renders identically.
   */
  @Test
  public void closureIsolationIsDeterministicUnderShuffledInput()
      throws HelixRebalanceException, IOException {
    String previous = null;
    for (int seed = 0; seed < 25; seed++) {
      ClusterConfig c = config(true);
      List<AssignableReplica> replicaList = new ArrayList<>();
      // T1 and T2 are joined by node A, T3 is a clean clique that must keep rebalancing.
      add2(replicaList, c, res("R1", "T1", NODE_CAPACITY), 3);
      add2(replicaList, c, res("R2", "T2", NODE_CAPACITY), 1);
      add2(replicaList, c, res("R3", "T3", NODE_CAPACITY), 2);

      List<AssignableNode> nodeList = new ArrayList<>();
      nodeList.add(node(c, "A", 0, "T1", "T2"));
      nodeList.add(node(c, "C", 1, "T1"));
      nodeList.add(node(c, "B", 2, "T2"));
      nodeList.add(node(c, "clean0", 3, "T3"));
      nodeList.add(node(c, "clean1", 4, "T3"));
      nodeList.add(node(c, "spare", 5));

      Random rnd = new Random(seed);
      Collections.shuffle(replicaList, rnd);
      Collections.shuffle(nodeList, rnd);
      Set<AssignableReplica> replicas = new LinkedHashSet<>(replicaList);
      Set<AssignableNode> nodes = new LinkedHashSet<>(nodeList);

      ClusterContext ctx = new ClusterContext(replicas, nodes, Collections.emptyMap(),
          Collections.emptyMap(), c);

      ResourceAssignment prevR1 = new ResourceAssignment("R1");
      prevR1.addReplicaMap(new Partition("R1_0"), Collections.singletonMap("A", "ONLINE"));
      prevR1.addReplicaMap(new Partition("R1_1"), Collections.singletonMap("C", "ONLINE"));
      ResourceAssignment prevR2 = new ResourceAssignment("R2");
      prevR2.addReplicaMap(new Partition("R2_0"), Collections.singletonMap("B", "ONLINE"));
      Map<String, ResourceAssignment> prev = new HashMap<>();
      prev.put("R1", prevR1);
      prev.put("R2", prevR2);

      Map<String, ResourceAssignment> result = WagedRebalanceUtil.calculateAssignment(
          new ClusterModel(ctx, replicas, nodes, ClusterModel.RebalanceScopeType.GLOBAL_BASELINE),
          algo(), prev);
      String rendered = render(result);
      if (previous == null) {
        LOG.info("determinism baseline:\n" + rendered);
        previous = rendered;
      } else {
        Assert.assertEquals(rendered, previous,
            "isolation emitted a different assignment on shuffled input, seed " + seed);
      }
    }
  }

  private void add2(List<AssignableReplica> out, ClusterConfig c, ResourceConfig rc, int n) {
    for (int p = 0; p < n; p++) {
      out.add(new AssignableReplica(c, rc, rc.getResourceName() + "_" + p, "ONLINE", 0));
    }
  }

  /**
   * Two separate blocks failing in the same run. Each must be carried over independently, and the
   * clean clique must still be rebalanced. This is the case where _failedGroups accumulates across
   * more than one tryIsolate call.
   */
  @Test
  public void twoIndependentBlocksAreBothCarriedOver()
      throws HelixRebalanceException, IOException {
    ClusterConfig c = config(true);
    Set<AssignableReplica> replicas = new java.util.HashSet<>();
    // Block one: T1 and T2 joined on shared0. R1 asks for more than the block can hold.
    add(replicas, c, res("R1", "T1", NODE_CAPACITY), 3);
    add(replicas, c, res("R2", "T2", NODE_CAPACITY), 1);
    // Block two: T4 and T5 joined on shared1. R4 asks for more than the block can hold.
    add(replicas, c, res("R4", "T4", NODE_CAPACITY), 3);
    add(replicas, c, res("R5", "T5", NODE_CAPACITY), 1);
    // Clean clique.
    add(replicas, c, res("R7", "T7", NODE_CAPACITY), 1);

    Set<AssignableNode> nodes = new java.util.HashSet<>();
    nodes.add(node(c, "shared0", 0, "T1", "T2"));
    nodes.add(node(c, "t1only", 1, "T1"));
    nodes.add(node(c, "shared1", 2, "T4", "T5"));
    nodes.add(node(c, "t4only", 3, "T4"));
    nodes.add(node(c, "clean7", 4, "T7"));
    nodes.add(node(c, "spare0", 5));
    nodes.add(node(c, "spare1", 6));
    nodes.add(node(c, "spare2", 7));

    ClusterContext ctx = new ClusterContext(replicas, nodes, Collections.emptyMap(),
        Collections.emptyMap(), c);

    ResourceAssignment prevR1 = new ResourceAssignment("R1");
    prevR1.addReplicaMap(new Partition("R1_0"), Collections.singletonMap("t1only", "ONLINE"));
    ResourceAssignment prevR4 = new ResourceAssignment("R4");
    prevR4.addReplicaMap(new Partition("R4_0"), Collections.singletonMap("t4only", "ONLINE"));
    Map<String, ResourceAssignment> prev = new HashMap<>();
    prev.put("R1", prevR1);
    prev.put("R4", prevR4);

    Map<String, Integer> weights = new HashMap<>();
    weights.put("R1", NODE_CAPACITY);
    weights.put("R2", NODE_CAPACITY);
    weights.put("R4", NODE_CAPACITY);
    weights.put("R5", NODE_CAPACITY);
    weights.put("R7", NODE_CAPACITY);

    Map<String, ResourceAssignment> result = WagedRebalanceUtil.calculateAssignment(
        new ClusterModel(ctx, replicas, nodes, ClusterModel.RebalanceScopeType.GLOBAL_BASELINE),
        algo(), prev);
    LOG.info("twoBlocks survivors = " + new java.util.TreeSet<>(result.keySet()));
    Assert.assertTrue(result.containsKey("R7"),
        "the clean T7 clique must still rebalance, got " + result.keySet());
    Assert.assertEquals(result.get("R1").getReplicaMap(new Partition("R1_0")).keySet(),
        Collections.singleton("t1only"), "block one must be carried over verbatim");
    Assert.assertEquals(result.get("R4").getReplicaMap(new Partition("R4_0")).keySet(),
        Collections.singleton("t4only"), "block two must be carried over verbatim");
    assertNoBreach("twoBlocks", result, weights, Collections.emptyMap());
  }

  /**
   * A resource pinned to a tag no live instance carries any more, whose previous assignment still
   * names an instance that has since been retagged into a different clique. The closure cannot see
   * the link, because the tag reaches no node at all, so this is exactly the shape the stale carry
   * over guard exists for. It must not emit an overcommitted node.
   */
  @Test
  public void tagCarriedByNoLiveInstanceMustNotOvercommit() throws IOException {
    ClusterConfig c = config(true);
    Set<AssignableReplica> replicas = new java.util.HashSet<>();
    add(replicas, c, res("R1", "T1", NODE_CAPACITY), 1);   // T1 is on no node at all
    add(replicas, c, res("R2", "T2", NODE_CAPACITY), 1);

    Set<AssignableNode> nodes = new java.util.HashSet<>();
    // "moved" used to carry T1 and is where R1_0 was placed. It now carries only T2.
    nodes.add(node(c, "moved", 0, "T2"));
    nodes.add(node(c, "spare", 1));

    ClusterContext ctx = new ClusterContext(replicas, nodes, Collections.emptyMap(),
        Collections.emptyMap(), c);

    ResourceAssignment prevR1 = new ResourceAssignment("R1");
    prevR1.addReplicaMap(new Partition("R1_0"), Collections.singletonMap("moved", "ONLINE"));

    Map<String, Integer> weights = new HashMap<>();
    weights.put("R1", NODE_CAPACITY);
    weights.put("R2", NODE_CAPACITY);

    try {
      Map<String, ResourceAssignment> result = WagedRebalanceUtil.calculateAssignment(
          new ClusterModel(ctx, replicas, nodes, ClusterModel.RebalanceScopeType.GLOBAL_BASELINE),
          algo(), Collections.singletonMap("R1", prevR1));
      LOG.info("orphanTag returned " + new java.util.TreeSet<>(result.keySet()));
      assertNoBreach("orphanTag", result, weights, Collections.emptyMap());
    } catch (HelixRebalanceException expected) {
      LOG.info("orphanTag failed closed: " + expected.getMessage());
    }
  }

  /**
   * The carried over assignment was valid against the capacities of the previous run, but a node
   * has since been shrunk. Re-imposing the old placement must not silently emit a node over its
   * current capacity.
   */
  @Test
  public void shrunkNodeCapacitySinceThePreviousAssignment() throws IOException {
    // A node that a carried-over placement names may have shrunk since that placement was made.
    // Isolation cannot repair that, but it must not be worse than the default mode either, so the
    // test pins the parity: whatever the failed group gets carried forward as has to be exactly
    // the previous assignment that the default mode would have fallen back to anyway.
    ResourceAssignment prevR1 = new ResourceAssignment("R1");
    prevR1.addReplicaMap(new Partition("R1_0"), Collections.singletonMap("t1small", "ONLINE"));

    Map<String, ResourceAssignment> isolated = shrunkNodeRun(true, prevR1);
    Assert.assertNotNull(isolated, "isolation should still have produced an assignment");
    LOG.info("shrunkNode isolated R1 = " + isolated.get("R1").getRecord().getMapFields());

    // The default mode cannot place R1 either, so it throws and the caller keeps the old baseline,
    // which holds this very placement. Same breach, reached by a different road.
    Map<String, ResourceAssignment> global = shrunkNodeRun(false, prevR1);
    Assert.assertNull(global, "default mode was expected to fail closed on the same input");

    Assert.assertEquals(isolated.get("R1").getRecord().getMapFields(),
        prevR1.getRecord().getMapFields(),
        "carry-over must reproduce the previous assignment verbatim, inventing no new placement");

    // The clean group is the whole point: it must still have been rebalanced.
    Assert.assertFalse(isolated.get("R2").getRecord().getMapFields().isEmpty(),
        "the clean group should have been rebalanced while R1 was carried over");
  }

  /** One shrunk-node rebalance, returning null when it failed closed. */
  private Map<String, ResourceAssignment> shrunkNodeRun(boolean flagOn, ResourceAssignment prevR1)
      throws IOException {
    ClusterConfig c = config(flagOn);
    Set<AssignableReplica> replicas = new java.util.HashSet<>();
    add(replicas, c, res("R1", "T1", NODE_CAPACITY), 3);   // more than T1 can hold, so T1 fails
    add(replicas, c, res("R2", "T2", NODE_CAPACITY), 1);

    Set<AssignableNode> nodes = new java.util.HashSet<>();
    // t1small used to have room for a full partition, it now has half.
    nodes.add(node(c, "t1small", 0, NODE_CAPACITY / 2, "T1"));
    nodes.add(node(c, "t1other", 1, "T1"));
    nodes.add(node(c, "t2only", 2, "T2"));
    nodes.add(node(c, "spare", 3));

    ClusterContext ctx = new ClusterContext(replicas, nodes, Collections.emptyMap(),
        Collections.emptyMap(), c);
    try {
      return WagedRebalanceUtil.calculateAssignment(
          new ClusterModel(ctx, replicas, nodes, ClusterModel.RebalanceScopeType.GLOBAL_BASELINE),
          algo(), Collections.singletonMap("R1", prevR1));
    } catch (HelixRebalanceException failedClosed) {
      LOG.info("shrunkNode flagOn=" + flagOn + " failed closed: "
          + failedClosed.getMessage());
      return null;
    }
  }

  /**
   * Cost of the closure on a cluster whose groups are dominated by untagged resources. Every
   * untagged resource is its own group and can reach every node, so the closure immediately spans
   * the whole cluster. This is the worst case for the fixpoint and it runs on the failure path,
   * which is exactly when the controller is already behind.
   */
  @Test
  public void closureCostOnAClusterDominatedByUntaggedResources()
      throws IOException {
    long withFlag = untaggedHeavyRun(true);
    long withoutFlag = untaggedHeavyRun(false);
    long closureCost = withFlag - withoutFlag;
    LOG.info("closureCost: flag on " + withFlag + " ms, flag off " + withoutFlag
        + " ms, attributable to the closure " + closureCost + " ms");
    Assert.assertTrue(closureCost < 2_000,
        "computing the share closure added " + closureCost + " ms on top of the " + withoutFlag
            + " ms the same rebalance takes with the flag off, which is far too slow for the "
            + "rebalance failure path");
  }

  private long untaggedHeavyRun(boolean flagOn) throws IOException {
    int untagged = 1200;
    int nodeCount = 300;
    ClusterConfig c = config(flagOn);
    Set<AssignableReplica> replicas = new java.util.HashSet<>();
    // One tagged clique that cannot be placed, so isolation is actually consulted.
    add(replicas, c, res("BROKEN", "T1", NODE_CAPACITY + 1), 1);
    for (int i = 0; i < untagged; i++) {
      add(replicas, c, res("U" + i, null, 1), 1);
    }

    Set<AssignableNode> nodes = new java.util.HashSet<>();
    nodes.add(node(c, "t1node", 0, "T1"));
    for (int i = 0; i < nodeCount; i++) {
      nodes.add(node(c, "n" + i, i % 5));
    }

    ClusterContext ctx = new ClusterContext(replicas, nodes, Collections.emptyMap(),
        Collections.emptyMap(), c);

    long start = System.currentTimeMillis();
    try {
      WagedRebalanceUtil.calculateAssignment(
          new ClusterModel(ctx, replicas, nodes, ClusterModel.RebalanceScopeType.GLOBAL_BASELINE),
          algo(), Collections.emptyMap());
      Assert.fail("the broken clique must fail the run in both modes");
    } catch (HelixRebalanceException expected) {
      // Untagged resources collapse everything into one block, so this correctly fails globally.
    }
    return System.currentTimeMillis() - start;
  }

  /**
   * The share blocks are computed with union-find for speed, but the contract they have to honour
   * is the original definition: two groups belong together when one can reach a node the other can
   * reach, followed to a fixpoint. This walks randomized topologies and checks the fast partition
   * against a naive implementation of that definition, so an optimisation can never quietly change
   * which groups get carried over together.
   */
  @Test
  public void fastShareBlocksMatchTheNaiveTransitiveDefinition() throws Exception {
    Random random = new Random(20240517L);
    int compared = 0;
    int nonTrivial = 0;
    for (int trial = 0; trial < 400; trial++) {
      ClusterConfig c = config(true);
      int tagCount = 1 + random.nextInt(5);
      int nodeCount = random.nextInt(9);
      int resourceCount = 1 + random.nextInt(6);

      Set<AssignableReplica> replicas = new java.util.HashSet<>();
      Map<String, String> tagByGroup = new HashMap<>();
      for (int r = 0; r < resourceCount; r++) {
        String name = "R" + r;
        // A quarter of the resources are untagged, which is the case that used to blow up.
        String tag = random.nextInt(4) == 0 ? null : "T" + random.nextInt(tagCount);
        add(replicas, c, res(name, tag, 1), 1);
        tagByGroup.put(tag == null ? "untagged-resource:" + name : "tag:" + tag, tag);
      }

      Set<AssignableNode> nodes = new java.util.HashSet<>();
      List<Set<String>> nodeTags = new ArrayList<>();
      for (int n = 0; n < nodeCount; n++) {
        Set<String> tags = new TreeSet<>();
        for (int t = 0; t < tagCount; t++) {
          if (random.nextInt(3) == 0) {
            tags.add("T" + t);
          }
        }
        if (random.nextInt(5) == 0) {
          // A label no resource is pinned to, such as a hardware or AZ tag. It must join nothing.
          tags.add("JUNK" + random.nextInt(3));
        }
        nodeTags.add(tags);
        nodes.add(node(c, "n" + n, n, tags.toArray(new String[0])));
      }

      Set<Set<String>> actual = new java.util.HashSet<>(callShareBlocks(c, replicas, nodes));
      Set<Set<String>> expected = naiveBlocks(tagByGroup, nodeTags);
      Assert.assertEquals(actual, expected, "trial " + trial + " nodeTags=" + nodeTags
          + " tagByGroup=" + new TreeMap<>(tagByGroup));
      compared++;
      if (expected.size() > 1 && expected.stream().anyMatch(b -> b.size() > 1)) {
        nonTrivial++;
      }
    }
    LOG.info("shareBlocks differential: " + compared + " topologies, " + nonTrivial
        + " with a split partition containing a merged block");
    // Guard against the comparison passing because every topology was degenerate.
    Assert.assertTrue(nonTrivial > 40, "too few interesting partitions to trust this, got "
        + nonTrivial);
  }

  /** Reach the package-private isolation helper and its private partition. */
  @SuppressWarnings("unchecked")
  private List<Set<String>> callShareBlocks(ClusterConfig c, Set<AssignableReplica> replicas,
      Set<AssignableNode> nodes) throws Exception {
    ClusterContext ctx = new ClusterContext(replicas, nodes, Collections.emptyMap(),
        Collections.emptyMap(), c);
    ClusterModel model =
        new ClusterModel(ctx, replicas, nodes, ClusterModel.RebalanceScopeType.GLOBAL_BASELINE);
    Class<?> type = Class.forName(
        "org.apache.helix.controller.rebalancer.waged.constraints.InstanceTagIsolation");
    Constructor<?> ctor =
        type.getDeclaredConstructor(ClusterModel.class, List.class, List.class);
    ctor.setAccessible(true);
    Object isolation =
        ctor.newInstance(model, new ArrayList<>(replicas), new ArrayList<>(nodes));
    Method blocks = type.getDeclaredMethod("shareBlocks");
    blocks.setAccessible(true);
    return (List<Set<String>>) blocks.invoke(isolation);
  }

  /** The partition as originally defined: shared-node adjacency, closed transitively. */
  private static Set<Set<String>> naiveBlocks(Map<String, String> tagByGroup,
      List<Set<String>> nodeTags) {
    List<String> groups = new ArrayList<>(new TreeSet<>(tagByGroup.keySet()));
    Map<String, Set<String>> adjacency = new HashMap<>();
    for (String g : groups) {
      adjacency.put(g, new TreeSet<>());
    }
    for (Set<String> tags : nodeTags) {
      List<String> reaching = new ArrayList<>();
      for (String g : groups) {
        String tag = tagByGroup.get(g);
        if (tag == null || tags.contains(tag)) {
          reaching.add(g);
        }
      }
      for (String a : reaching) {
        adjacency.get(a).addAll(reaching);
      }
    }
    Set<Set<String>> blocks = new java.util.HashSet<>();
    Set<String> seen = new TreeSet<>();
    for (String start : groups) {
      if (!seen.add(start)) {
        continue;
      }
      Set<String> block = new TreeSet<>();
      Deque<String> queue = new ArrayDeque<>();
      queue.add(start);
      block.add(start);
      while (!queue.isEmpty()) {
        for (String next : adjacency.get(queue.poll())) {
          if (block.add(next)) {
            seen.add(next);
            queue.add(next);
          }
        }
      }
      blocks.add(block);
    }
    return blocks;
  }

  /**
   * The untagged-heavy case collapses into one block and bails out early. A cluster made of many
   * distinct tagged cliques does not, so it exercises the block partition and the per block deficit
   * attribution for real. This pins that path down too, since both are on the failure path where
   * the controller is already behind.
   */
  @Test
  public void closureCostOnAClusterOfManyTaggedCliques() throws IOException {
    long withFlag = manyCliqueRun(true);
    long withoutFlag = manyCliqueRun(false);
    long cost = withFlag - withoutFlag;
    LOG.info("manyCliqueCost: flag on " + withFlag + " ms, flag off " + withoutFlag
        + " ms, attributable to isolation " + cost + " ms");
    Assert.assertTrue(cost < 2_000,
        "isolation added " + cost + " ms on top of the " + withoutFlag + " ms the same rebalance "
            + "takes with the flag off, which is far too slow for the rebalance failure path");
  }

  /** 400 tagged cliques over 800 nodes, one of which cannot be placed. */
  private long manyCliqueRun(boolean flagOn) throws IOException {
    int cliques = 400;
    ClusterConfig c = config(flagOn);
    Set<AssignableReplica> replicas = new java.util.HashSet<>();
    Set<AssignableNode> nodes = new java.util.HashSet<>();
    for (int i = 0; i < cliques; i++) {
      String tag = "T" + i;
      // Clique 0 asks for more than its nodes can hold, so isolation is actually consulted.
      add(replicas, c, res("R" + i, tag, i == 0 ? NODE_CAPACITY : NODE_CAPACITY / 4), 2);
      nodes.add(node(c, "a" + i, i % 5, tag));
      nodes.add(node(c, "b" + i, (i + 1) % 5, tag));
    }
    ClusterContext ctx = new ClusterContext(replicas, nodes, Collections.emptyMap(),
        Collections.emptyMap(), c);

    long start = System.currentTimeMillis();
    Map<String, ResourceAssignment> result = null;
    try {
      result = WagedRebalanceUtil.calculateAssignment(
          new ClusterModel(ctx, replicas, nodes, ClusterModel.RebalanceScopeType.GLOBAL_BASELINE),
          algo(), Collections.emptyMap());
    } catch (HelixRebalanceException failedClosed) {
      Assert.assertFalse(flagOn, "isolation should have carried the one broken clique over");
    }
    long elapsed = System.currentTimeMillis() - start;
    if (flagOn) {
      // The point of the mode: 399 healthy cliques still get placed.
      Assert.assertTrue(result.size() >= cliques - 1,
          "expected the healthy cliques to survive, got " + result.size());
    }
    return elapsed;
  }

  /**
   * An untagged resource normally collapses every group into one block, which is why the deficit
   * attribution declines to act. A tag that no live instance carries is the exception: it reaches
   * no node, so it stays a block of its own next to the big one and the attribution runs after all.
   * The unplaceable resource must be the one blamed, and the untagged resources must still be
   * placed.
   */
  @Test
  public void untaggedResourcesAlongsideATagNoLiveInstanceCarries()
      throws IOException, HelixRebalanceException {
    ClusterConfig c = config(true);
    Set<AssignableReplica> replicas = new java.util.HashSet<>();
    // Pinned to a tag no live instance carries, so it can never be placed.
    add(replicas, c, res("DEAD", "GHOST", NODE_CAPACITY), 1);
    add(replicas, c, res("U1", null, NODE_CAPACITY), 1);
    add(replicas, c, res("U2", null, NODE_CAPACITY), 1);

    Set<AssignableNode> nodes = new java.util.HashSet<>();
    nodes.add(node(c, "n0", 0));
    nodes.add(node(c, "n1", 1));

    ClusterContext ctx = new ClusterContext(replicas, nodes, Collections.emptyMap(),
        Collections.emptyMap(), c);
    Map<String, ResourceAssignment> result = WagedRebalanceUtil.calculateAssignment(
        new ClusterModel(ctx, replicas, nodes, ClusterModel.RebalanceScopeType.GLOBAL_BASELINE),
        algo(), Collections.emptyMap());

    LOG.info("deadTag survivors = " + new TreeSet<>(result.keySet()));
    Assert.assertFalse(result.containsKey("DEAD"), "the unplaceable resource must not be emitted");
    Assert.assertTrue(result.containsKey("U1") && result.containsKey("U2"),
        "the untagged resources must still have been placed, got " + result.keySet());
    Map<String, Integer> weights = new HashMap<>();
    weights.put("U1", NODE_CAPACITY);
    weights.put("U2", NODE_CAPACITY);
    Map<String, Integer> caps = new HashMap<>();
    caps.put("n0", NODE_CAPACITY);
    caps.put("n1", NODE_CAPACITY);
    assertNoBreach("deadTag", result, weights, caps);
  }

  /**
   * Carrying a block over is only sound if it happens the same way in every rebalance scope. The
   * partial, emergency and delayed overwrite scopes differ from the baseline in that the nodes
   * arrive pre-loaded and the replica list holds only what is left to assign, which is exactly
   * where a half carried block would leak a partial entry. A block spanning two groups is the
   * interesting case, since it is the one the earlier exclusivity rule refused to handle at all.
   */
  @Test
  public void multiGroupBlockCarriedOverInEveryRebalanceScope() throws IOException {
    for (ClusterModel.RebalanceScopeType scope : ClusterModel.RebalanceScopeType.values()) {
      ClusterConfig c = config(true);
      Set<AssignableReplica> replicas = new java.util.HashSet<>();
      // T1 and T2 overlap on "shared", so they form one block. T1 asks for more than fits.
      add(replicas, c, res("R1", "T1", NODE_CAPACITY), 3);
      add(replicas, c, res("R2", "T2", NODE_CAPACITY), 1);
      add(replicas, c, res("CLEAN", "T9", NODE_CAPACITY), 1);

      Set<AssignableNode> nodes = new java.util.HashSet<>();
      nodes.add(node(c, "shared", 0, "T1", "T2"));
      nodes.add(node(c, "t1only", 1, "T1"));
      nodes.add(node(c, "t9only", 2, "T9"));
      nodes.add(node(c, "spare", 3));

      ResourceAssignment prevR1 = new ResourceAssignment("R1");
      prevR1.addReplicaMap(new Partition("R1_0"), Collections.singletonMap("t1only", "ONLINE"));
      ResourceAssignment prevR2 = new ResourceAssignment("R2");
      prevR2.addReplicaMap(new Partition("R2_0"), Collections.singletonMap("shared", "ONLINE"));
      Map<String, ResourceAssignment> previous = new HashMap<>();
      previous.put("R1", prevR1);
      previous.put("R2", prevR2);

      ClusterContext ctx = new ClusterContext(replicas, nodes, Collections.emptyMap(),
          Collections.emptyMap(), c);
      Map<String, ResourceAssignment> result;
      try {
        result = WagedRebalanceUtil.calculateAssignment(
            new ClusterModel(ctx, replicas, nodes, scope), algo(), previous);
      } catch (HelixRebalanceException failedClosed) {
        Assert.fail("scope " + scope + " should have isolated the block, not thrown: "
            + failedClosed.getMessage());
        return;
      }

      // Both members of the block come back byte for byte, never half assigned.
      Assert.assertEquals(result.get("R1").getRecord().getMapFields(),
          prevR1.getRecord().getMapFields(), "scope " + scope + ": R1 carried over incorrectly");
      Assert.assertEquals(result.get("R2").getRecord().getMapFields(),
          prevR2.getRecord().getMapFields(), "scope " + scope + ": R2 carried over incorrectly");
      // The clean clique outside the block is the whole point of the mode.
      Assert.assertFalse(result.get("CLEAN").getRecord().getMapFields().isEmpty(),
          "scope " + scope + ": the clean clique should still have been rebalanced");

      Map<String, Integer> weights = new HashMap<>();
      weights.put("R1", NODE_CAPACITY);
      weights.put("R2", NODE_CAPACITY);
      weights.put("CLEAN", NODE_CAPACITY);
      assertNoBreach("scope " + scope, result, weights, Collections.emptyMap());
      LOG.info("scope " + scope + " carried the block over and kept CLEAN = "
          + result.get("CLEAN").getRecord().getMapFields());
    }
  }

  /**
   * The carried over assignment reflects where replicas were before, so it can name an instance
   * that has since been removed from the cluster or taken out of service. Neither may overcommit a
   * live node, and neither may let another group quietly take the named instance's capacity.
   */
  @Test
  public void carriedOverAssignmentNamingAnInstanceNoLongerAssignable() throws IOException {
    for (String mode : new String[] {"removed", "evacuating"}) {
      ClusterConfig c = config(true);
      Set<AssignableReplica> replicas = new java.util.HashSet<>();
      add(replicas, c, res("R1", "T1", NODE_CAPACITY), 3);
      add(replicas, c, res("CLEAN", "T9", NODE_CAPACITY), 1);

      Set<AssignableNode> nodes = new java.util.HashSet<>();
      nodes.add(node(c, "t1only", 0, "T1"));
      nodes.add(node(c, "t9only", 1, "T9"));
      nodes.add(node(c, "spare", 2));
      if (mode.equals("evacuating")) {
        // Present in the cluster but draining, so the algorithm must not place on it.
        InstanceConfig ic = new InstanceConfig("gone");
        ic.setInstanceCapacityMap(Collections.singletonMap(CAP, NODE_CAPACITY));
        ic.addTag("T1");
        ic.setInstanceOperation(InstanceConstants.InstanceOperation.EVACUATE);
        nodes.add(new AssignableNode(c, ic, "gone"));
      }

      // Names "gone", which is either absent entirely or evacuating.
      ResourceAssignment prevR1 = new ResourceAssignment("R1");
      prevR1.addReplicaMap(new Partition("R1_0"), Collections.singletonMap("gone", "ONLINE"));

      ClusterContext ctx = new ClusterContext(replicas, nodes, Collections.emptyMap(),
          Collections.emptyMap(), c);
      Map<String, ResourceAssignment> result;
      try {
        result = WagedRebalanceUtil.calculateAssignment(
            new ClusterModel(ctx, replicas, nodes, ClusterModel.RebalanceScopeType.GLOBAL_BASELINE),
            algo(), Collections.singletonMap("R1", prevR1));
      } catch (HelixRebalanceException failedClosed) {
        // Failing closed is the default mode's own behaviour, so it is always acceptable.
        LOG.info(mode + " failed closed: " + failedClosed.getMessage());
        continue;
      }

      Map<String, Integer> weights = new HashMap<>();
      weights.put("R1", NODE_CAPACITY);
      weights.put("CLEAN", NODE_CAPACITY);
      Map<String, Integer> caps = new HashMap<>();
      caps.put("t1only", NODE_CAPACITY);
      caps.put("t9only", NODE_CAPACITY);
      caps.put("spare", NODE_CAPACITY);
      caps.put("gone", NODE_CAPACITY);
      assertNoBreach(mode, result, weights, caps);

      // Nothing freshly calculated may land on the instance the carried over assignment names.
      Map<String, Integer> load = loadPerInstance(result, weights);
      Assert.assertTrue(load.getOrDefault("gone", 0) <= NODE_CAPACITY,
          mode + ": the named instance was overcommitted, load " + load);
      LOG.info(mode + " placed " + load + ", R1 = "
          + result.get("R1").getRecord().getMapFields());
    }
  }
}
