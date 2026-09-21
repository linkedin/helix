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
import java.util.TreeSet;

import org.apache.helix.HelixRebalanceException;
import org.apache.helix.controller.rebalancer.util.WagedRebalanceUtil;
import org.apache.helix.controller.rebalancer.waged.RebalanceAlgorithm;
import org.apache.helix.model.ClusterConfig;
import org.apache.helix.model.ResourceAssignment;
import org.apache.helix.model.ResourceConfig;
import org.testng.Assert;
import org.testng.annotations.Test;

/**
 * Adversarial multi-round behaviour for WAGED instance-tag ("clique") isolation. This is the
 * "does it settle down" suite: the same broken cluster is run through the rebalancer many rounds,
 * feeding each round's output back in as the next round's previous assignment, exactly the way the
 * real controller feeds the persisted baseline back into the next GLOBAL_BASELINE recompute.
 *
 * A CLUSTER_CONFIG or INSTANCE_CONFIG change makes WAGED reassign every replica (see
 * ClusterModelProvider.findToBeAssignedReplicasByClusterChanges), so a full recompute with the
 * previous assignment fed only through the sort order and the busy-instance tie-break is the
 * realistic worst case for churn, and it is exactly what happens on a controller failover or on any
 * cluster-config touch. A broken clique keeps the controller touching config round after round, so
 * this loop is not hypothetical.
 *
 * The harness always seeds the loop with a healthy last-known-good assignment for every clique
 * before breaking one, so the broken clique has something to carry forward, matching a cluster that
 * was healthy and then degraded rather than one that started broken.
 *
 * See {@link AbstractTestWagedInstanceTagIsolation} for the shared clique topology and helpers.
 */
public class TestWagedInstanceTagIsolationConvergence
    extends AbstractTestWagedInstanceTagIsolation {

  // How many rounds to run after breaking a clique. Ten is the floor the task asks for; a couple
  // more give a wide enough tail to be sure a fixed point actually holds and is not a one-round
  // coincidence.
  private static final int ROUNDS = 12;
  // The tail that must be completely still for the loop to count as converged.
  private static final int STABLE_TAIL = 3;

  /** One round's outcome: the raw assignment that gets fed back, plus the skipped set. */
  private static final class Round {
    final Map<String, ResourceAssignment> assignment;
    final Set<String> skipped;

    Round(Map<String, ResourceAssignment> assignment, Set<String> skipped) {
      this.assignment = assignment;
      this.skipped = skipped;
    }
  }

  /**
   * Runs one faithful GLOBAL_BASELINE recompute: the previous assignment is fed into the cluster
   * context as the best-possible (so it drives the replica sort order and the busy-instance
   * tie-break) and into the carry-forward, then every replica is recomputed from empty nodes.
   */
  private Round oneRound(ClusterConfig config, Map<Integer, CliqueSpec> specs,
      Map<String, ResourceAssignment> previous) throws HelixRebalanceException, IOException {
    RebalanceAlgorithm algorithm = createAlgorithm();

    // First model: read the skipped set straight from the algorithm.
    ClusterModel skippedModel = seededModel(config, specs, previous);
    Set<String> skipped =
        new TreeSet<>(algorithm.calculate(skippedModel).getSkippedResources());

    // Second, independent model: the production carry-forward path produces the map we feed back.
    ClusterModel resultModel = seededModel(config, specs, previous);
    Map<String, ResourceAssignment> result =
        WagedRebalanceUtil.calculateAssignment(resultModel, algorithm, previous);
    return new Round(result, skipped);
  }

  private ClusterModel seededModel(ClusterConfig config, Map<Integer, CliqueSpec> specs,
      Map<String, ResourceAssignment> previousBestPossible) throws IOException {
    Set<AssignableReplica> replicas = createReplicas(config, specs);
    Set<AssignableNode> nodes = createNodes(config, specs);
    // Baseline empty, best-possible = previous, matching generateClusterModelForBaseline which
    // passes the previous baseline as the current assignment (the best-possible slot).
    ClusterContext context =
        new ClusterContext(replicas, nodes, Collections.emptyMap(), previousBestPossible, config);
    return new ClusterModel(context, replicas, nodes,
        ClusterModel.RebalanceScopeType.GLOBAL_BASELINE);
  }

  /**
   * Seeds a healthy last-known-good assignment (all cliques placeable), then breaks the requested
   * cliques and runs {@link #ROUNDS} rounds. Returns every round so the caller can measure churn.
   */
  private List<Round> runAfterBreaking(ClusterConfig config, Map<Integer, CliqueSpec> brokenSpecs,
      int rounds) throws HelixRebalanceException, IOException {
    Map<String, ResourceAssignment> seed =
        oneRound(config, allHealthy(), Collections.emptyMap()).assignment;
    Map<String, ResourceAssignment> previous = seed;
    List<Round> history = new ArrayList<>();
    for (int round = 0; round < rounds; round++) {
      Round current = oneRound(config, brokenSpecs, previous);
      history.add(current);
      previous = current.assignment;
    }
    return history;
  }

  private static Map<String, Map<String, Map<String, String>>> norm(
      Map<String, ResourceAssignment> assignment) {
    return normalize(assignment);
  }

  /**
   * The number of (resource, partition) cells whose instance-to-state map changed between two
   * assignments. Zero means a fixed point.
   */
  private static int movement(Map<String, ResourceAssignment> before,
      Map<String, ResourceAssignment> after) {
    Map<String, Map<String, Map<String, String>>> a = norm(before);
    Map<String, Map<String, Map<String, String>>> b = norm(after);
    Set<String> resources = new TreeSet<>();
    resources.addAll(a.keySet());
    resources.addAll(b.keySet());
    int moved = 0;
    for (String resource : resources) {
      Map<String, Map<String, String>> ap = a.getOrDefault(resource, Collections.emptyMap());
      Map<String, Map<String, String>> bp = b.getOrDefault(resource, Collections.emptyMap());
      Set<String> partitions = new TreeSet<>();
      partitions.addAll(ap.keySet());
      partitions.addAll(bp.keySet());
      for (String partition : partitions) {
        if (!ap.getOrDefault(partition, Collections.emptyMap())
            .equals(bp.getOrDefault(partition, Collections.emptyMap()))) {
          moved++;
        }
      }
    }
    return moved;
  }

  /** Movement between the seed round and each round, and then between consecutive rounds. */
  private List<Integer> churn(Map<String, ResourceAssignment> seed, List<Round> history) {
    List<Integer> movements = new ArrayList<>();
    Map<String, ResourceAssignment> previous = seed;
    for (Round round : history) {
      movements.add(movement(previous, round.assignment));
      previous = round.assignment;
    }
    return movements;
  }

  /**
   * The first round index after which movement is zero for the rest of the run, or -1 when the run
   * never settles.
   */
  private static int stabilizationRound(List<Integer> movements) {
    for (int i = 0; i < movements.size(); i++) {
      boolean stillFromHere = true;
      for (int j = i; j < movements.size(); j++) {
        if (movements.get(j) != 0) {
          stillFromHere = false;
          break;
        }
      }
      if (stillFromHere) {
        return i;
      }
    }
    return -1;
  }

  private void assertConverged(String scenario, Map<String, ResourceAssignment> seed,
      List<Round> history) {
    List<Integer> movements = churn(seed, history);
    int settled = stabilizationRound(movements);
    System.out.printf("[convergence] %s: per-round movement=%s settledAtRound=%s%n", scenario,
        movements, settled);
    Assert.assertTrue(settled >= 0 && settled <= history.size() - STABLE_TAIL,
        scenario + " never reached a fixed point. Per-round movement was " + movements);
  }

  // ---------------------------------------------------------------------------------------------
  // H7: convergence / no oscillation
  // ---------------------------------------------------------------------------------------------

  /**
   * Sanity floor: a healthy cluster fed its own output back must not move after the first round.
   * If this churns, every richer scenario below is meaningless.
   */
  @Test
  public void testHealthyClusterReachesAFixedPoint()
      throws HelixRebalanceException, IOException {
    ClusterConfig config = createClusterConfig(true);
    Map<String, ResourceAssignment> seed =
        oneRound(config, allHealthy(), Collections.emptyMap()).assignment;
    List<Round> history = new ArrayList<>();
    Map<String, ResourceAssignment> previous = seed;
    for (int round = 0; round < ROUNDS; round++) {
      Round current = oneRound(config, allHealthy(), previous);
      history.add(current);
      previous = current.assignment;
    }
    assertConverged("healthy steady state", seed, history);
  }

  /**
   * The headline: one broken clique, nineteen healthy. The healthy cliques must reach a fixed
   * point and the broken clique must be carried forward byte for byte, every round.
   */
  @Test
  public void testSingleBrokenCliqueConvergesAndCarriesForwardStably()
      throws HelixRebalanceException, IOException {
    int broken = 3;
    ClusterConfig config = createClusterConfig(true);
    Map<Integer, CliqueSpec> specs = allHealthy();
    specs.put(broken, CliqueSpec.healthy().withPartitionWeight(UNPLACEABLE_PARTITION_WEIGHT));

    Map<String, ResourceAssignment> seed =
        oneRound(config, allHealthy(), Collections.emptyMap()).assignment;
    Map<String, Map<String, String>> seedBrokenClique =
        normalize(seed).get(resourceName(broken));
    Assert.assertNotNull(seedBrokenClique, "The seed must contain the soon-to-break clique");

    Map<String, ResourceAssignment> previous = seed;
    List<Round> history = new ArrayList<>();
    for (int round = 0; round < ROUNDS; round++) {
      Round current = oneRound(config, specs, previous);
      history.add(current);
      // The broken clique is skipped every round and its carried value never drifts.
      Assert.assertTrue(current.skipped.contains(resourceName(broken)),
          "Round " + round + " must skip the broken clique");
      Assert.assertEquals(normalize(current.assignment).get(resourceName(broken)), seedBrokenClique,
          "Round " + round + ": the carried-forward broken clique drifted");
      previous = current.assignment;
    }
    assertConverged("single broken clique", seed, history);
  }

  /**
   * Several broken cliques at once, still surrounded by healthy ones. Everything healthy settles
   * and every broken clique stays carried.
   */
  @Test
  public void testMultipleBrokenCliquesConverge() throws HelixRebalanceException, IOException {
    Set<Integer> broken = new HashSet<>(Arrays.asList(0, 7, 13, 19));
    ClusterConfig config = createClusterConfig(true);
    Map<Integer, CliqueSpec> specs = allHealthy();
    broken.forEach(clique -> specs
        .put(clique, CliqueSpec.healthy().withPartitionWeight(UNPLACEABLE_PARTITION_WEIGHT)));

    Map<String, ResourceAssignment> seed =
        oneRound(config, allHealthy(), Collections.emptyMap()).assignment;
    List<Round> history = new ArrayList<>();
    Map<String, ResourceAssignment> previous = seed;
    for (int round = 0; round < ROUNDS; round++) {
      Round current = oneRound(config, specs, previous);
      for (int clique : broken) {
        Assert.assertTrue(current.skipped.contains(resourceName(clique)));
      }
      history.add(current);
      previous = current.assignment;
    }
    assertConverged("four broken cliques", seed, history);
  }

  /**
   * Every clique packed so each partition consumes a whole node (a perfect matching), with one
   * clique broken on top. Saturation is where the busy-instance idle-preference tie-break is most
   * likely to swap a replica between two equally scored nodes round after round, so this is the
   * strongest oscillation bait available in the clique topology.
   */
  @Test
  public void testSaturatedCliquesWithABrokenOneConverge()
      throws HelixRebalanceException, IOException {
    int broken = 9;
    ClusterConfig config = createClusterConfig(true);
    Map<Integer, CliqueSpec> specs = new HashMap<>();
    for (int clique = 0; clique < CLIQUE_COUNT; clique++) {
      // NODES_PER_CLIQUE partitions each weighing a whole node, onto NODES_PER_CLIQUE nodes.
      specs.put(clique, CliqueSpec.healthy().withPartitionWeight(NODE_CAPACITY)
          .withPartitionCount(NODES_PER_CLIQUE));
    }
    // Break one by making its partitions un-placeable (one more than fits at full-node weight).
    specs.put(broken, CliqueSpec.healthy().withPartitionWeight(NODE_CAPACITY)
        .withPartitionCount(NODES_PER_CLIQUE + 1));

    Map<String, ResourceAssignment> seed = oneRound(config, saturatedHealthy(), Collections
        .emptyMap()).assignment;
    List<Round> history = new ArrayList<>();
    Map<String, ResourceAssignment> previous = seed;
    for (int round = 0; round < ROUNDS; round++) {
      Round current = oneRound(config, specs, previous);
      history.add(current);
      previous = current.assignment;
    }
    assertConverged("saturated cliques with one broken", seed, history);
  }

  private Map<Integer, CliqueSpec> saturatedHealthy() {
    Map<Integer, CliqueSpec> specs = new HashMap<>();
    for (int clique = 0; clique < CLIQUE_COUNT; clique++) {
      specs.put(clique, CliqueSpec.healthy().withPartitionWeight(NODE_CAPACITY)
          .withPartitionCount(NODES_PER_CLIQUE));
    }
    return specs;
  }

  /**
   * The same steady broken cluster but started from a cold cache (empty previous), so no
   * pre-existing equilibrium pins anything on round one. This removes the PartitionMovement
   * constraint's stabilising pull for the first round and forces placement to be decided by the
   * scoring and the busy-instance tie-break alone, which is the most likely way to provoke a
   * period-two oscillation as the busy set flips between rounds. It must still reach a fixed point.
   */
  @Test
  public void testColdStartWithABrokenCliqueConverges()
      throws HelixRebalanceException, IOException {
    int broken = 5;
    ClusterConfig config = createClusterConfig(true);
    Map<Integer, CliqueSpec> specs = allHealthy();
    specs.put(broken, CliqueSpec.healthy().withPartitionWeight(UNPLACEABLE_PARTITION_WEIGHT));

    List<Round> history = new ArrayList<>();
    Map<String, ResourceAssignment> previous = Collections.emptyMap();
    for (int round = 0; round < ROUNDS; round++) {
      Round current = oneRound(config, specs, previous);
      history.add(current);
      previous = current.assignment;
    }
    // Compare consecutive rounds (there is no seed to compare the first against).
    List<Integer> movements = new ArrayList<>();
    for (int i = 0; i + 1 < history.size(); i++) {
      movements.add(movement(history.get(i).assignment, history.get(i + 1).assignment));
    }
    int settled = stabilizationRound(movements);
    System.out.printf("[convergence] cold start broken clique: consecutive movement=%s "
        + "settledAtRound=%s%n", movements, settled);
    Assert.assertTrue(settled >= 0 && settled <= movements.size() - STABLE_TAIL,
        "Cold start never settled. Consecutive movement was " + movements);
  }

  /**
   * Saturated cliques (a perfect matching, one full-node partition per node) started cold with one
   * broken. Saturation plus cold start is the harshest combination for the idle-node tie-break,
   * since every node is either full or empty and ties are common. Must still settle.
   */
  @Test
  public void testColdStartSaturatedConverges() throws HelixRebalanceException, IOException {
    int broken = 11;
    ClusterConfig config = createClusterConfig(true);
    Map<Integer, CliqueSpec> specs = saturatedHealthy();
    specs.put(broken, CliqueSpec.healthy().withPartitionWeight(NODE_CAPACITY)
        .withPartitionCount(NODES_PER_CLIQUE + 1));

    Map<String, ResourceAssignment> previous = Collections.emptyMap();
    List<Map<String, ResourceAssignment>> snapshots = new ArrayList<>();
    for (int round = 0; round < ROUNDS; round++) {
      previous = oneRound(config, specs, previous).assignment;
      snapshots.add(previous);
    }
    List<Integer> movements = new ArrayList<>();
    for (int i = 0; i + 1 < snapshots.size(); i++) {
      movements.add(movement(snapshots.get(i), snapshots.get(i + 1)));
    }
    int settled = stabilizationRound(movements);
    System.out.printf("[convergence] cold start saturated: consecutive movement=%s "
        + "settledAtRound=%s%n", movements, settled);
    Assert.assertTrue(settled >= 0 && settled <= movements.size() - STABLE_TAIL,
        "Cold start saturated never settled. Consecutive movement was " + movements);
  }

  /**
   * Wildly uneven cliques (different node counts, partition counts and weights) with one broken.
   * Heterogeneity is where the shared cluster-wide context estimates and the per-clique scoring
   * interact, another place churn could hide.
   */
  @Test
  public void testHeterogeneousCliquesWithABrokenOneConverge()
      throws HelixRebalanceException, IOException {
    int broken = 4;
    ClusterConfig config = createClusterConfig(true);
    Map<Integer, CliqueSpec> healthy = new HashMap<>();
    for (int clique = 0; clique < CLIQUE_COUNT; clique++) {
      healthy.put(clique, CliqueSpec.healthy().withNodeCount(4 + (clique % 7))
          .withPartitionCount(3 + (clique % 11)).withPartitionWeight(5 + (clique % 4) * 7));
    }
    Map<Integer, CliqueSpec> specs = new HashMap<>(healthy);
    specs.put(broken, healthy.get(broken).withPartitionWeight(UNPLACEABLE_PARTITION_WEIGHT));

    Map<String, ResourceAssignment> seed =
        oneRound(config, healthy, Collections.emptyMap()).assignment;
    List<Round> history = new ArrayList<>();
    Map<String, ResourceAssignment> previous = seed;
    for (int round = 0; round < ROUNDS; round++) {
      Round current = oneRound(config, specs, previous);
      history.add(current);
      previous = current.assignment;
    }
    assertConverged("heterogeneous cliques with one broken", seed, history);
  }

  // ---------------------------------------------------------------------------------------------
  // H8: recovery
  // ---------------------------------------------------------------------------------------------

  /**
   * A clique is broken, carried forward for several rounds, then repaired. It must come back and
   * be placed on its own nodes, not stay permanently skipped, and the whole cluster must then be
   * at a fixed point.
   */
  @Test
  public void testBrokenCliqueRecoversAfterRepair() throws HelixRebalanceException, IOException {
    int broken = 12;
    ClusterConfig config = createClusterConfig(true);
    Map<Integer, CliqueSpec> brokenSpecs = allHealthy();
    brokenSpecs.put(broken,
        CliqueSpec.healthy().withPartitionWeight(UNPLACEABLE_PARTITION_WEIGHT));

    Map<String, ResourceAssignment> previous =
        oneRound(config, allHealthy(), Collections.emptyMap()).assignment;
    // Break for five rounds.
    for (int round = 0; round < 5; round++) {
      Round current = oneRound(config, brokenSpecs, previous);
      Assert.assertTrue(current.skipped.contains(resourceName(broken)));
      previous = current.assignment;
    }

    // Repair and let it settle.
    List<Round> afterRepair = new ArrayList<>();
    Map<String, ResourceAssignment> repairSeed = previous;
    for (int round = 0; round < ROUNDS; round++) {
      Round current = oneRound(config, allHealthy(), previous);
      afterRepair.add(current);
      previous = current.assignment;
    }

    Round last = afterRepair.get(afterRepair.size() - 1);
    Assert.assertTrue(last.skipped.isEmpty(),
        "The repaired clique must stop being skipped, but skipped=" + last.skipped);
    ResourceAssignment repaired = last.assignment.get(resourceName(broken));
    Assert.assertNotNull(repaired, "The repaired clique must reappear in the assignment");
    Assert.assertEquals(repaired.getMappedPartitions().size(), PARTITIONS_PER_RESOURCE,
        "The repaired clique must be fully placed again");
    assertOnOwnNodes(broken, repaired);
    assertConverged("recovery after repair", repairSeed, afterRepair);
  }

  /**
   * Clique A is repaired in the very same round clique B breaks. A must come back and B must take
   * over the carried-forward slot, and the cluster must settle.
   */
  @Test
  public void testOneCliqueRepairedWhileAnotherBreaksSameRound()
      throws HelixRebalanceException, IOException {
    int first = 2;
    int second = 15;
    ClusterConfig config = createClusterConfig(true);
    Map<Integer, CliqueSpec> firstBroken = allHealthy();
    firstBroken.put(first, CliqueSpec.healthy().withPartitionWeight(UNPLACEABLE_PARTITION_WEIGHT));

    Map<String, ResourceAssignment> previous =
        oneRound(config, allHealthy(), Collections.emptyMap()).assignment;
    for (int round = 0; round < 4; round++) {
      previous = oneRound(config, firstBroken, previous).assignment;
    }

    // Same round: repair the first, break the second.
    Map<Integer, CliqueSpec> swapped = allHealthy();
    swapped.put(second, CliqueSpec.healthy().withPartitionWeight(UNPLACEABLE_PARTITION_WEIGHT));

    List<Round> history = new ArrayList<>();
    Map<String, ResourceAssignment> swapSeed = previous;
    for (int round = 0; round < ROUNDS; round++) {
      Round current = oneRound(config, swapped, previous);
      history.add(current);
      previous = current.assignment;
    }

    Round last = history.get(history.size() - 1);
    Assert.assertEquals(last.skipped, Collections.singleton(resourceName(second)),
        "Only the newly broken clique should be skipped at steady state, was " + last.skipped);
    Assert.assertEquals(last.assignment.get(resourceName(first)).getMappedPartitions().size(),
        PARTITIONS_PER_RESOURCE, "The repaired clique must be fully placed");
    assertConverged("repair one while breaking another", swapSeed, history);
  }

  /**
   * Two cliques broken, then both repaired in the same round. Both must come back and the cluster
   * must settle with nothing skipped.
   */
  @Test
  public void testTwoCliquesRepairedInTheSameRound()
      throws HelixRebalanceException, IOException {
    Set<Integer> broken = new HashSet<>(Arrays.asList(5, 16));
    ClusterConfig config = createClusterConfig(true);
    Map<Integer, CliqueSpec> brokenSpecs = allHealthy();
    broken.forEach(clique -> brokenSpecs
        .put(clique, CliqueSpec.healthy().withPartitionWeight(UNPLACEABLE_PARTITION_WEIGHT)));

    Map<String, ResourceAssignment> previous =
        oneRound(config, allHealthy(), Collections.emptyMap()).assignment;
    for (int round = 0; round < 4; round++) {
      previous = oneRound(config, brokenSpecs, previous).assignment;
    }

    List<Round> history = new ArrayList<>();
    Map<String, ResourceAssignment> repairSeed = previous;
    for (int round = 0; round < ROUNDS; round++) {
      Round current = oneRound(config, allHealthy(), previous);
      history.add(current);
      previous = current.assignment;
    }
    Round last = history.get(history.size() - 1);
    Assert.assertTrue(last.skipped.isEmpty(), "Both repaired cliques must stop being skipped");
    for (int clique : broken) {
      Assert.assertEquals(last.assignment.get(resourceName(clique)).getMappedPartitions().size(),
          PARTITIONS_PER_RESOURCE);
    }
    assertConverged("two cliques repaired at once", repairSeed, history);
  }

  /**
   * A clique is repaired in the same round one of its own nodes is removed. It must recover onto
   * the nodes that remain and settle. This is the "fixed while a node is simultaneously being
   * removed" case.
   */
  @Test
  public void testCliqueRepairedWhileANodeIsRemovedSameRound()
      throws HelixRebalanceException, IOException {
    int broken = 8;
    ClusterConfig config = createClusterConfig(true);
    Map<Integer, CliqueSpec> brokenSpecs = allHealthy();
    brokenSpecs.put(broken, CliqueSpec.healthy().withPartitionWeight(UNPLACEABLE_PARTITION_WEIGHT));

    Map<String, ResourceAssignment> previous =
        oneRound(config, allHealthy(), Collections.emptyMap()).assignment;
    for (int round = 0; round < 4; round++) {
      previous = oneRound(config, brokenSpecs, previous).assignment;
    }

    // Repair the weight but drop the clique to fewer nodes in the same round.
    Map<Integer, CliqueSpec> repaired = allHealthy();
    repaired.put(broken, CliqueSpec.healthy().withNodeCount(NODES_PER_CLIQUE - 3));

    List<Round> history = new ArrayList<>();
    Map<String, ResourceAssignment> repairSeed = previous;
    for (int round = 0; round < ROUNDS; round++) {
      Round current = oneRound(config, repaired, previous);
      history.add(current);
      previous = current.assignment;
    }
    Round last = history.get(history.size() - 1);
    Assert.assertTrue(last.skipped.isEmpty(),
        "A clique with fewer but sufficient nodes must recover, skipped=" + last.skipped);
    ResourceAssignment recovered = last.assignment.get(resourceName(broken));
    Assert.assertEquals(recovered.getMappedPartitions().size(), PARTITIONS_PER_RESOURCE);
    assertOnOwnNodes(broken, recovered);
    assertConverged("repair while removing a node", repairSeed, history);
  }

  // ---------------------------------------------------------------------------------------------
  // H6: determinism across controller failover (shuffled construction order)
  // ---------------------------------------------------------------------------------------------

  /**
   * Two controllers given byte-identical input must produce byte-identical output, including the
   * skipped set, no matter what order they happened to build their maps and sets in. The isolation
   * code leans on HashSet / HashMap iteration in several places (the group sets, the capacity
   * deficit entrySet walk, the untagged-group findFirst), so the same cluster is built many times
   * with the resources and instances inserted in different orders and the results are diffed.
   */
  @Test
  public void testDeterministicAcrossShuffledConstructionOrder()
      throws HelixRebalanceException, IOException {
    // A mix: broken cliques, saturated cliques, and healthy cliques, to exercise every ordered
    // structure at once.
    Map<Integer, CliqueSpec> specs = allHealthy();
    specs.put(1, CliqueSpec.healthy().withPartitionWeight(UNPLACEABLE_PARTITION_WEIGHT));
    specs.put(14, CliqueSpec.healthy().withPartitionWeight(UNPLACEABLE_PARTITION_WEIGHT));
    specs.put(6, CliqueSpec.healthy().withPartitionWeight(NODE_CAPACITY)
        .withPartitionCount(NODES_PER_CLIQUE));

    Map<String, Map<String, Map<String, String>>> reference = null;
    Set<String> referenceSkipped = null;
    long[] seeds = {1L, 2L, 3L, 5L, 8L, 13L, 21L, 34L};
    for (long seed : seeds) {
      ClusterConfig config = createClusterConfig(true);
      RebalanceAlgorithm algorithm = createAlgorithm();
      ClusterModel model = shuffledModel(config, specs, seed);
      OptimalAssignment optimal = algorithm.calculate(model);
      Map<String, Map<String, Map<String, String>>> normalized =
          normalize(optimal.getOptimalResourceAssignment());
      Set<String> skipped = new TreeSet<>(optimal.getSkippedResources());
      if (reference == null) {
        reference = normalized;
        referenceSkipped = skipped;
      } else {
        Assert.assertEquals(normalized, reference,
            "Shuffle seed " + seed + " produced a different assignment");
        Assert.assertEquals(skipped, referenceSkipped,
            "Shuffle seed " + seed + " produced a different skipped set");
      }
    }
  }

  /**
   * Builds a cluster model whose replica set and node set are inserted in a pseudo-random order
   * derived from {@code seed}, using LinkedHashSet so the insertion order is actually retained and
   * handed to the algorithm. If any downstream structure leaks its iteration order into the result,
   * two seeds will disagree.
   */
  private ClusterModel shuffledModel(ClusterConfig config, Map<Integer, CliqueSpec> specs, long seed)
      throws IOException {
    List<Integer> cliques = new ArrayList<>(specs.keySet());
    Collections.shuffle(cliques, new java.util.Random(seed));

    List<AssignableReplica> replicaList = new ArrayList<>();
    List<AssignableNode> nodeList = new ArrayList<>();
    for (int clique : cliques) {
      Map<Integer, CliqueSpec> single = Collections.singletonMap(clique, specs.get(clique));
      replicaList.addAll(createReplicas(config, single));
      nodeList.addAll(createNodes(config, single));
    }
    Collections.shuffle(replicaList, new java.util.Random(seed * 31 + 7));
    Collections.shuffle(nodeList, new java.util.Random(seed * 17 + 3));

    Set<AssignableReplica> replicas = new java.util.LinkedHashSet<>(replicaList);
    Set<AssignableNode> nodes = new java.util.LinkedHashSet<>(nodeList);
    ClusterContext context = new ClusterContext(replicas, nodes, Collections.emptyMap(),
        Collections.emptyMap(), config);
    return new ClusterModel(context, replicas, nodes,
        ClusterModel.RebalanceScopeType.GLOBAL_BASELINE);
  }

  // ---------------------------------------------------------------------------------------------
  // H3 (model level): carry-forward names an instance that is gone
  // ---------------------------------------------------------------------------------------------

  /**
   * While a clique is broken, one of its own nodes (named in the carried-forward assignment) goes
   * away. The mode cannot re-place the broken clique, so it keeps carrying an assignment that names
   * a node no longer in the cluster. That must be no worse than the default mode, which also
   * retains a last-known-good that names the same dead node, and the healthy cliques must keep
   * converging throughout. When the clique is finally repaired it must stop naming the dead node.
   */
  @Test
  public void testCarryForwardKeepsNamingADeadNodeButHealthyCliquesConverge()
      throws HelixRebalanceException, IOException {
    int broken = 7;
    ClusterConfig config = createClusterConfig(true);

    // Seed healthy so the broken clique has a last-known-good that names all ten of its nodes.
    Map<String, ResourceAssignment> seed =
        oneRound(config, allHealthy(), Collections.emptyMap()).assignment;
    String deadNode = instanceName(broken, NODES_PER_CLIQUE - 1);
    Assert.assertTrue(namesInstance(seed.get(resourceName(broken)), deadNode),
        "Precondition: the seed for the broken clique must name the node we remove");

    // Break the clique AND remove that one node from the topology.
    Map<Integer, CliqueSpec> brokenSpecs = allHealthy();
    brokenSpecs.put(broken, CliqueSpec.healthy()
        .withPartitionWeight(UNPLACEABLE_PARTITION_WEIGHT).withNodeCount(NODES_PER_CLIQUE - 1));

    Map<String, ResourceAssignment> previous = seed;
    List<Round> history = new ArrayList<>();
    for (int round = 0; round < ROUNDS; round++) {
      Round current = oneRound(config, brokenSpecs, previous);
      history.add(current);
      Assert.assertTrue(current.skipped.contains(resourceName(broken)));
      // The carried value still names the dead node: this is the documented, not-worse-than-default
      // behaviour, since the default mode would retain the same last-known-good.
      Assert.assertTrue(namesInstance(current.assignment.get(resourceName(broken)), deadNode),
          "Round " + round + ": the carried-forward assignment should still name the dead node");
      previous = current.assignment;
    }
    // Healthy cliques still settle despite the broken clique naming a phantom node forever.
    assertHealthyCliquesConverged("carry-forward names a dead node", seed, history, broken);

    // Now repair (placeable weight, node stays gone). The clique must re-place on its live nodes
    // only and stop naming the dead node.
    Map<Integer, CliqueSpec> repaired = allHealthy();
    repaired.put(broken, CliqueSpec.healthy().withNodeCount(NODES_PER_CLIQUE - 1));
    Map<String, ResourceAssignment> afterRepair = previous;
    for (int round = 0; round < ROUNDS; round++) {
      afterRepair = oneRound(config, repaired, afterRepair).assignment;
    }
    Assert.assertFalse(namesInstance(afterRepair.get(resourceName(broken)), deadNode),
        "After repair the clique must stop naming the removed node");
    Assert.assertEquals(afterRepair.get(resourceName(broken)).getMappedPartitions().size(),
        PARTITIONS_PER_RESOURCE);
  }

  // ---------------------------------------------------------------------------------------------
  // H-EXTRA: retag churn and flag flips over the multi-round loop
  // ---------------------------------------------------------------------------------------------

  /**
   * The retag case that {@code resolveCarriedOverNodeReuse} exists for, but exercised across many
   * rounds rather than inside one call. An instance is retagged out of the broken clique into a
   * healthy one, so the broken clique's carried-forward assignment names an instance the healthy
   * clique can now place on. The mode must not thrash: it must reach a fixed point where the
   * colliding healthy clique settles and the broken clique stays carried.
   */
  @Test
  public void testRetagIntoAHealthyCliqueConverges() throws HelixRebalanceException, IOException {
    ClusterConfig config = createClusterConfig(true);
    // Two cliques, small, so a retag actually creates a name collision that matters.
    int brokenClique = 0;
    int healthyClique = 1;

    // Seed: both healthy, each on its own three nodes.
    Set<AssignableReplica> seedReplicas = new HashSet<>();
    Set<AssignableNode> seedNodes = new HashSet<>();
    for (int n = 0; n < 3; n++) {
      seedNodes.add(taggedNode(config, instanceName(brokenClique, n), 0, cliqueTag(brokenClique)));
      seedNodes.add(taggedNode(config, instanceName(healthyClique, n), 0, cliqueTag(healthyClique)));
    }
    addReplicas(seedReplicas, config, taggedResource(resourceName(brokenClique),
        cliqueTag(brokenClique), HEALTHY_PARTITION_WEIGHT), 3);
    addReplicas(seedReplicas, config, taggedResource(resourceName(healthyClique),
        cliqueTag(healthyClique), HEALTHY_PARTITION_WEIGHT), 3);
    Map<String, ResourceAssignment> seed = calculate(config, seedReplicas, seedNodes,
        Collections.emptyMap());

    // Retag one of the broken clique's nodes into the healthy clique, and break the broken clique.
    // The broken clique's seed still names that node.
    String retagged = instanceName(brokenClique, 2);
    Map<String, Map<String, String>> seedBroken = normalize(seed).get(resourceName(brokenClique));
    Assert.assertTrue(seedBroken.values().stream().anyMatch(states -> states.containsKey(retagged)),
        "Precondition: the broken clique's seed must name the node about to be retagged, so the "
            + "carry-forward really does collide with the healthy clique");
    Map<String, ResourceAssignment> previous = seed;
    List<Map<String, ResourceAssignment>> tail = new ArrayList<>();
    for (int round = 0; round < ROUNDS; round++) {
      Set<AssignableReplica> replicas = new HashSet<>();
      Set<AssignableNode> nodes = new HashSet<>();
      for (int n = 0; n < 2; n++) {
        nodes.add(taggedNode(config, instanceName(brokenClique, n), 0, cliqueTag(brokenClique)));
      }
      for (int n = 0; n < 3; n++) {
        nodes.add(taggedNode(config, instanceName(healthyClique, n), 0, cliqueTag(healthyClique)));
      }
      // The retagged node now carries the healthy clique's tag.
      nodes.add(taggedNode(config, retagged, 0, cliqueTag(healthyClique)));
      addReplicas(replicas, config, taggedResource(resourceName(brokenClique),
          cliqueTag(brokenClique), UNPLACEABLE_PARTITION_WEIGHT), 3);
      addReplicas(replicas, config, taggedResource(resourceName(healthyClique),
          cliqueTag(healthyClique), HEALTHY_PARTITION_WEIGHT), 3);
      Map<String, ResourceAssignment> current = calculate(config, replicas, nodes, previous);
      // The broken clique cannot be placed (weight 150 > node 100), so it must be carried forward
      // byte for byte from its seed, which proves the skip and carry-forward path actually fired
      // and this scenario is not a vacuous pass.
      Assert.assertEquals(normalize(current).get(resourceName(brokenClique)), seedBroken,
          "Round " + round + ": the broken clique must be carried forward from its seed");
      tail.add(current);
      previous = current;
    }
    // The last few rounds must be identical (fixed point) even with the retag collision in play.
    for (int i = tail.size() - STABLE_TAIL; i < tail.size() - 1; i++) {
      Assert.assertEquals(movement(tail.get(i), tail.get(i + 1)), 0,
          "Retag scenario kept churning at round " + i + ": " + normalize(tail.get(i)) + " -> "
              + normalize(tail.get(i + 1)));
    }
  }

  /**
   * Flip the flag on for several rounds, then off, then on again, on a cluster with a broken
   * clique, and confirm the healthy cliques do not get a churn storm when the flag changes. With
   * the flag off a broken clique freezes everything (nothing moves at all), and with it on the
   * healthy cliques settle. The only movement permitted is the single step when isolation first
   * lets the frozen healthy cliques react.
   */
  @Test
  public void testFlagFlippedOnOffOnDoesNotStorm() throws HelixRebalanceException, IOException {
    int broken = 10;
    Map<Integer, CliqueSpec> specs = allHealthy();
    specs.put(broken, CliqueSpec.healthy().withPartitionWeight(UNPLACEABLE_PARTITION_WEIGHT));

    ClusterConfig on = createClusterConfig(true);
    // Seed healthy with the flag on.
    Map<String, ResourceAssignment> previous =
        oneRound(on, allHealthy(), Collections.emptyMap()).assignment;

    // Flag ON, broken: converge.
    for (int round = 0; round < 4; round++) {
      previous = oneRound(on, specs, previous).assignment;
    }
    Map<String, ResourceAssignment> afterOn = previous;

    // Flag OFF, broken: the whole calculation throws, so the controller keeps the last assignment.
    // Model that as "previous unchanged" for the off rounds.
    ClusterConfig off = createClusterConfig(false);
    for (int round = 0; round < 3; round++) {
      try {
        oneRound(off, specs, previous);
        Assert.fail("With the flag off a broken clique must abort the whole rebalance");
      } catch (HelixRebalanceException expected) {
        // Controller retains the previous assignment, so nothing moves.
      }
    }
    // Flag back ON: the first round may move the healthy cliques back to where isolation wants
    // them, then it must be a fixed point again.
    List<Round> afterFlipBack = new ArrayList<>();
    for (int round = 0; round < ROUNDS; round++) {
      Round current = oneRound(on, specs, previous);
      afterFlipBack.add(current);
      previous = current.assignment;
    }
    assertConverged("flag on/off/on", afterOn, afterFlipBack);
  }

  /**
   * H-EXTRA, the exact freeze this feature exists to remove. With the flag off, one broken clique
   * makes the whole rebalance throw, so nothing anywhere gets placed (the frozen cluster). Flip the
   * flag on with byte-identical input and the healthy cliques must all be placed again while the
   * broken one is set aside. This is the "prove the freeze disappears" case stated directly, as a
   * before/after placement-count contrast rather than only as a convergence check.
   */
  @Test
  public void testFlagFlipOnUnfreezesAnAlreadyFrozenCluster()
      throws HelixRebalanceException, IOException {
    int broken = 8;
    Map<Integer, CliqueSpec> specs = allHealthy();
    specs.put(broken, CliqueSpec.healthy().withPartitionWeight(UNPLACEABLE_PARTITION_WEIGHT));

    // A real last-known-good assignment from when the cluster was fully healthy.
    Map<String, ResourceAssignment> previous =
        oneRound(createClusterConfig(true), allHealthy(), Collections.emptyMap()).assignment;

    // Flag off: the frozen cluster. The whole pass throws, so the controller cannot rebalance
    // anything, not even the untouched healthy cliques.
    ClusterConfig off = createClusterConfig(false);
    boolean threw = false;
    try {
      oneRound(off, specs, previous);
    } catch (HelixRebalanceException expected) {
      threw = true;
    }
    Assert.assertTrue(threw,
        "Precondition: with the flag off the broken clique must freeze the whole rebalance");

    // Flip the flag on with the same broken input. Every healthy clique must now be fully placed on
    // its own nodes, which is exactly the movement the freeze was blocking.
    ClusterConfig on = createClusterConfig(true);
    Round unfrozen = oneRound(on, specs, previous);
    int placedHealthyPartitions = 0;
    for (int clique = 0; clique < CLIQUE_COUNT; clique++) {
      if (clique == broken) {
        continue;
      }
      ResourceAssignment assignment = unfrozen.assignment.get(resourceName(clique));
      Assert.assertNotNull(assignment,
          "Healthy clique " + clique + " stayed unplaced after the flag was flipped on");
      assertOnOwnNodes(clique, assignment);
      placedHealthyPartitions += assignment.getMappedPartitions().size();
    }
    Assert.assertEquals(placedHealthyPartitions, (CLIQUE_COUNT - 1) * PARTITIONS_PER_RESOURCE,
        "Not every healthy partition came back after the freeze was lifted");
    Assert.assertTrue(unfrozen.skipped.contains(resourceName(broken)),
        "The still-broken clique must be the one, and only one, set aside after unfreezing");

    // And it is immediately a fixed point: feeding the unfrozen output back must not churn.
    Map<String, ResourceAssignment> settled = oneRound(on, specs, unfrozen.assignment).assignment;
    Assert.assertEquals(movement(unfrozen.assignment, settled), 0,
        "The cluster churned right after the freeze was lifted");
  }

  /**
   * H-EXTRA, topology churn while a clique stays broken. One clique is broken for the whole test.
   * Around it the topology is repeatedly rewritten: a brand new clique is added, then a healthy
   * clique loses its last node (so it too becomes unplaceable), then participants are added to a
   * healthy clique, then the added clique is removed again. After each rewrite the cluster is run to
   * a fixed point, the healthy cliques must settle, and the originally broken clique must stay
   * skipped and carried byte for byte the whole way through.
   */
  @Test
  public void testTopologyChurnWhileACliqueIsBrokenConverges()
      throws HelixRebalanceException, IOException {
    int broken = 10;
    int addedClique = CLIQUE_COUNT;
    int cliqueLosingItsNodes = 5;
    int cliqueGainingNodes = 3;
    ClusterConfig on = createClusterConfig(true);

    Map<String, ResourceAssignment> seed =
        oneRound(on, allHealthy(), Collections.emptyMap()).assignment;
    Map<Integer, CliqueSpec> base = allHealthy();
    base.put(broken, CliqueSpec.healthy().withPartitionWeight(UNPLACEABLE_PARTITION_WEIGHT));
    Map<String, Map<String, String>> seedBroken = normalize(seed).get(resourceName(broken));
    Assert.assertNotNull(seedBroken, "The seed must contain the soon-to-break clique");

    // Break the clique and settle once before any churn, so the carried value is established.
    Map<String, ResourceAssignment> previous =
        churnPhase("break", on, base, seed, broken, seedBroken);

    // Add a brand new clique.
    Map<Integer, CliqueSpec> withNewClique = new HashMap<>(base);
    withNewClique.put(addedClique, CliqueSpec.healthy());
    previous = churnPhase("add-clique", on, withNewClique, previous, broken, seedBroken);
    Assert.assertNotNull(normalize(previous).get(resourceName(addedClique)),
        "The newly added clique was never placed");

    // A healthy clique loses its last node, so its resource becomes unplaceable too.
    Map<Integer, CliqueSpec> withDeletedNodes = new HashMap<>(withNewClique);
    withDeletedNodes.put(cliqueLosingItsNodes, CliqueSpec.healthy().withNodeCount(0));
    previous = churnPhase("delete-last-node", on, withDeletedNodes, previous, broken, seedBroken);

    // Add participants to a still-healthy clique (scale out).
    Map<Integer, CliqueSpec> withExtraNodes = new HashMap<>(withDeletedNodes);
    withExtraNodes.put(cliqueGainingNodes,
        CliqueSpec.healthy().withNodeCount(NODES_PER_CLIQUE + 4));
    previous = churnPhase("add-participants", on, withExtraNodes, previous, broken, seedBroken);

    // Remove the clique that was added earlier.
    Map<Integer, CliqueSpec> withoutAddedClique = new HashMap<>(withExtraNodes);
    withoutAddedClique.remove(addedClique);
    previous = churnPhase("remove-clique", on, withoutAddedClique, previous, broken, seedBroken);
    Assert.assertNull(normalize(previous).get(resourceName(addedClique)),
        "The removed clique must disappear from the output");
  }

  /**
   * Runs a fixed topology to a fixed point from a given starting assignment, asserting the broken
   * clique stays skipped and carried byte for byte throughout, and that the tail of the run is
   * completely still (two consecutive byte-identical rounds). Returns the settled assignment.
   */
  private Map<String, ResourceAssignment> churnPhase(String label, ClusterConfig config,
      Map<Integer, CliqueSpec> specs, Map<String, ResourceAssignment> start, int brokenClique,
      Map<String, Map<String, String>> seedBroken) throws HelixRebalanceException, IOException {
    int rounds = 5;
    List<Map<String, ResourceAssignment>> outputs = new ArrayList<>();
    Map<String, ResourceAssignment> previous = start;
    for (int round = 0; round < rounds; round++) {
      Round current = oneRound(config, specs, previous);
      Assert.assertTrue(current.skipped.contains(resourceName(brokenClique)),
          "churn/" + label + " round " + round + ": the broken clique stopped being skipped");
      Assert.assertEquals(normalize(current.assignment).get(resourceName(brokenClique)), seedBroken,
          "churn/" + label + " round " + round + ": the broken clique drifted during churn");
      outputs.add(current.assignment);
      previous = current.assignment;
    }
    int lastMove = movement(outputs.get(rounds - 2), outputs.get(rounds - 1));
    int priorMove = movement(outputs.get(rounds - 3), outputs.get(rounds - 2));
    System.out.printf("[convergence] churn/%s: tail movements=[%d,%d]%n", label, priorMove,
        lastMove);
    Assert.assertEquals(lastMove, 0,
        "churn/" + label + " never reached a fixed point (final round still moved)");
    Assert.assertEquals(priorMove, 0,
        "churn/" + label + " reached a fixed point that did not hold for two rounds");
    return previous;
  }

  // ---------------------------------------------------------------------------------------------
  // Helpers
  // ---------------------------------------------------------------------------------------------

  private Map<String, ResourceAssignment> calculate(ClusterConfig config,
      Set<AssignableReplica> replicas, Set<AssignableNode> nodes,
      Map<String, ResourceAssignment> previous) throws HelixRebalanceException {
    ClusterContext context =
        new ClusterContext(replicas, nodes, Collections.emptyMap(), previous, config);
    ClusterModel model = new ClusterModel(context, replicas, nodes,
        ClusterModel.RebalanceScopeType.GLOBAL_BASELINE);
    return WagedRebalanceUtil.calculateAssignment(model, createAlgorithm(), previous);
  }

  private static boolean namesInstance(ResourceAssignment assignment, String instance) {
    if (assignment == null) {
      return false;
    }
    return assignment.getMappedPartitions().stream()
        .anyMatch(partition -> assignment.getReplicaMap(partition).containsKey(instance));
  }

  private void assertOnOwnNodes(int clique, ResourceAssignment assignment) {
    String prefix = "instance_" + clique + "_";
    assignment.getMappedPartitions().forEach(partition -> assignment.getReplicaMap(partition)
        .keySet().forEach(instance -> Assert.assertTrue(instance.startsWith(prefix),
            "Clique " + clique + " leaked onto " + instance)));
  }

  private void assertHealthyCliquesConverged(String scenario,
      Map<String, ResourceAssignment> seed, List<Round> history, int brokenClique) {
    // Strip the broken clique out of every snapshot and confirm the rest reaches a fixed point.
    List<Integer> movements = new ArrayList<>();
    Map<String, ResourceAssignment> previous = withoutResource(seed, resourceName(brokenClique));
    for (Round round : history) {
      Map<String, ResourceAssignment> current =
          withoutResource(round.assignment, resourceName(brokenClique));
      movements.add(movement(previous, current));
      previous = current;
    }
    int settled = stabilizationRound(movements);
    System.out.printf("[convergence] %s (healthy only): movement=%s settledAtRound=%s%n", scenario,
        movements, settled);
    Assert.assertTrue(settled >= 0 && settled <= history.size() - STABLE_TAIL,
        scenario + " healthy cliques never settled. Movement was " + movements);
  }

  private static Map<String, ResourceAssignment> withoutResource(
      Map<String, ResourceAssignment> assignment, String resource) {
    Map<String, ResourceAssignment> copy = new HashMap<>(assignment);
    copy.remove(resource);
    return copy;
  }
}
