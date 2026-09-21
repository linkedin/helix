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

import java.util.HashMap;
import java.util.LinkedHashSet;
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
import org.testng.SkipException;
import org.testng.annotations.Test;


/**
 * Performance attack on the collision cascade (H4). resolveCarriedOverNodeReuse loops until nothing
 * collides, and every iteration rebuilds the whole carried-instance TreeSet from scratch, re-sorts
 * the entire resource key set, and rescans every resource and every partition. A retag chain forces
 * one collision per round, so the loop runs once per resource while each round costs a full pass:
 * the total is quadratic in the resource count times the partitions per resource. This runs inside
 * the controller rebalance pipeline, so a blow-up here stalls the controller.
 *
 * The light sweep runs in CI and prints a size-vs-time table plus a super-linear-growth assertion.
 * The headline 2000 x 100 x 3 case is gated behind -DrunHeavyPerf=true so CI does not pay for it;
 * run it explicitly to reproduce the raw wall-clock number in the report.
 */
public class TestWagedRebalanceUtilCascadePerformance {

  @Test
  public void testCascadeStaysLinearInResourceCount() throws Exception {
    // Warm the JIT so the reported numbers reflect steady state, not first-call compilation.
    for (int i = 0; i < 2; i++) {
      timeOnce(150, 8, 3, true);
      timeOnce(150, 8, 3, false);
    }

    int[] sizes = {150, 300, 600};
    int partitions = 12;
    int replicas = 3;

    System.out.println();
    System.out.printf("=== Cascade cost, %d partitions x %d replicas per resource ===%n",
        partitions, replicas);
    System.out.printf("%-10s %-18s %-18s %-12s%n", "resources", "cascade(ms)",
        "zeroCollision(ms)", "slowdown");

    long[] cascadeMs = new long[sizes.length];
    for (int s = 0; s < sizes.length; s++) {
      int n = sizes[s];
      long cascade = bestOfTwo(n, partitions, replicas, true);
      long baseline = bestOfTwo(n, partitions, replicas, false);
      cascadeMs[s] = cascade;
      System.out.printf("%-10d %-18d %-18d %-12s%n", n, cascade, baseline,
          baseline == 0 ? "n/a" : (cascade / Math.max(1, baseline)) + "x");
    }

    // Quadrupling the resource count (150 -> 600) quadruples the work if the resolution is linear
    // and multiplies it by about 16 if it is quadratic. The original rebuild-and-rescan-every-round
    // implementation measured 18.4x here, which is what made a broken clique plus a retag storm
    // cost seconds of pipeline stall on every tick. Allow 8x, comfortably above the linear 4x so
    // timing noise on a loaded machine cannot flake it, and far below the quadratic 16x.
    double ratio = cascadeMs[2] / (double) Math.max(1, cascadeMs[0]);
    System.out.printf("cascade time ratio N=600 vs N=150 (4x resources): %.1fx "
        + "(linear would be ~4x, quadratic ~16x)%n", ratio);

    // The absolute budget is the assertion that matters, and it is the one that is not sensitive to
    // timing noise. The quadratic implementation measured 1416 ms at N=600; the linear one measures
    // around 11 ms. A 250 ms ceiling separates the two by a wide margin in both directions, and it
    // encodes the real requirement directly: this runs inside the single threaded rebalance
    // pipeline, so it must never cost a visible stall.
    Assert.assertTrue(cascadeMs[2] < 250,
        "Cascade at N=600 must stay far below a pipeline visible stall, took " + cascadeMs[2]
            + " ms (the quadratic implementation took about 1416 ms)");

    // The growth ratio is only meaningful once the smallest measurement is big enough that a
    // millisecond of jitter cannot dominate it. Below that it is pure noise, so checking it would
    // flake under suite contention without testing anything.
    if (cascadeMs[0] >= 5) {
      Assert.assertTrue(ratio < 8.0,
          "Cascade time must stay close to linear in resource count, saw " + ratio
              + "x for a 4x resource increase (quadratic would be ~16x)");
    } else {
      System.out.println(
          "ratio check skipped: baseline " + cascadeMs[0] + " ms is too small to measure reliably");
    }
  }

  @Test
  public void testHeadlinePathologicalCaseIsGatedButMeasured() throws Exception {
    if (!Boolean.getBoolean("runHeavyPerf")) {
      throw new SkipException(
          "Headline 2000x100x3 perf case skipped. Re-run with -DrunHeavyPerf=true to measure it.");
    }
    int n = Integer.getInteger("heavyResources", 2000);
    int partitions = Integer.getInteger("heavyPartitions", 100);
    int replicas = 3;

    long cascade = timeOnce(n, partitions, replicas, true);
    long baseline = timeOnce(n, partitions, replicas, false);
    System.out.println();
    System.out.printf("=== HEADLINE %d resources x %d partitions x %d replicas ===%n",
        n, partitions, replicas);
    System.out.printf("cascade (full retag chain): %d ms%n", cascade);
    System.out.printf("zero collision (same size) : %d ms%n", baseline);
    System.out.printf("slowdown                    : %dx%n", cascade / Math.max(1, baseline));
  }

  /**
   * The realistic trigger. A full N-deep chain is contrived, but a large broken clique (a big seed)
   * plus a moderate retag storm is not: every round rebuilds the WHOLE carried-instance set, so the
   * large, unchanging seed is paid for on every one of the shallow cascade's rounds. Gated so CI does
   * not pay for it.
   */
  @Test
  public void testRealisticLargeCliqueShallowCascadeIsGatedButMeasured() throws Exception {
    if (!Boolean.getBoolean("runHeavyPerf")) {
      throw new SkipException(
          "Realistic large-clique perf case skipped. Re-run with -DrunHeavyPerf=true to measure it.");
    }
    int n = Integer.getInteger("realisticResources", 2000);
    int partitions = Integer.getInteger("realisticPartitions", 100);
    int replicas = 3;
    int seedSize = Integer.getInteger("realisticSeed", 300);   // a broken clique of 300 resources
    int cascadeDepth = Integer.getInteger("realisticDepth", 200); // a 200-node retag storm

    Map<String, ResourceAssignment> calculated = new HashMap<>();
    Map<String, ResourceAssignment> previous = new HashMap<>();
    Set<String> skipped =
        buildLargeCliqueShallowCascade(n, partitions, replicas, seedSize, cascadeDepth, calculated,
            previous);

    long start = System.nanoTime();
    Map<String, ResourceAssignment> result = calculate(calculated, skipped, previous);
    long cascade = (System.nanoTime() - start) / 1_000_000L;
    Assert.assertFalse(result.isEmpty());

    System.out.println();
    System.out.printf("=== REALISTIC %d resources x %d partitions, broken clique of %d, "
        + "retag storm depth %d ===%n", n, partitions, seedSize, cascadeDepth);
    System.out.printf("cascade wall clock: %d ms%n", cascade);
  }

  // ---------------------------------------------------------------------------------------------

  /**
   * A broken clique of seedSize skipped resources, a retag chain of cascadeDepth resources hanging
   * off the clique, and healthy filler resources for the rest of the cluster. Returns the skipped
   * set. The cascade only runs cascadeDepth rounds, but every round rebuilds the carried set that
   * already contains the whole clique.
   */
  private static Set<String> buildLargeCliqueShallowCascade(int n, int partitions, int replicas,
      int seedSize, int cascadeDepth, Map<String, ResourceAssignment> calculated,
      Map<String, ResourceAssignment> previous) {
    Set<String> skipped = new LinkedHashSet<>();
    for (int s = 0; s < seedSize; s++) {
      String resource = "S" + s;
      // The clique's previous assignment. S0 carries the hook the retag chain reuses.
      String link = (s == 0) ? "chainlink-0" : "cseed_" + s;
      previous.put(resource, buildAssignment(resource, link, "cseedp" + s, partitions, replicas));
      calculated.put(resource, buildAssignment(resource, "cseedfresh-" + s, "cf" + s, partitions,
          replicas));
      skipped.add(resource);
    }
    for (int c = 1; c <= cascadeDepth; c++) {
      String resource = "C" + c;
      previous.put(resource, buildAssignment(resource, "chainlink-" + c, "cp" + c, partitions,
          replicas));
      String freshLink = "chainlink-" + (c - 1); // collides with the previous link in the chain
      calculated.put(resource, buildAssignment(resource, freshLink, "cff" + c, partitions, replicas));
    }
    int healthy = n - seedSize - cascadeDepth;
    for (int h = 0; h < healthy; h++) {
      String resource = "H" + h;
      calculated.put(resource, buildAssignment(resource, "hnolink-" + h, "hf" + h, partitions,
          replicas));
    }
    return skipped;
  }

  private static long bestOfTwo(int n, int partitions, int replicas, boolean collide)
      throws Exception {
    long best = Long.MAX_VALUE;
    for (int i = 0; i < 2; i++) {
      best = Math.min(best, timeOnce(n, partitions, replicas, collide));
    }
    return best;
  }

  private static long timeOnce(int n, int partitions, int replicas, boolean collide)
      throws Exception {
    Map<String, ResourceAssignment> calculated = new HashMap<>();
    Map<String, ResourceAssignment> previous = new HashMap<>();
    buildChain(n, partitions, replicas, collide, calculated, previous);
    Set<String> skipped = new LinkedHashSet<>();
    skipped.add("R0");

    long start = System.nanoTime();
    Map<String, ResourceAssignment> result = calculate(calculated, skipped, previous);
    long elapsedMs = (System.nanoTime() - start) / 1_000_000L;

    // Touch the result so the JIT cannot elide the work.
    Assert.assertFalse(result.isEmpty());
    return elapsedMs;
  }

  private static void buildChain(int n, int partitions, int replicas, boolean collide,
      Map<String, ResourceAssignment> calculated, Map<String, ResourceAssignment> previous) {
    for (int k = 0; k < n; k++) {
      String resource = "R" + k;
      previous.put(resource, buildAssignment(resource, "link-" + k, "p" + k, partitions, replicas));
      String freshLink;
      if (!collide) {
        freshLink = "fnolink-" + k; // never equal to any carried instance, so no cascade
      } else if (k == 0) {
        freshLink = "throwaway-0"; // R0 is skipped, its fresh entry is discarded anyway
      } else {
        freshLink = "link-" + (k - 1); // reuse the node R_{k-1}'s carried previous still claims
      }
      calculated.put(resource, buildAssignment(resource, freshLink, "f" + k, partitions, replicas));
    }
  }

  private static ResourceAssignment buildAssignment(String resource, String linkInstance,
      String privatePrefix, int partitions, int replicas) {
    ResourceAssignment resourceAssignment = new ResourceAssignment(resource);
    for (int p = 0; p < partitions; p++) {
      Map<String, String> replicaMap = new HashMap<>();
      for (int r = 0; r < replicas; r++) {
        String instance =
            (p == 0 && r == 0) ? linkInstance : privatePrefix + "_" + p + "_" + r;
        replicaMap.put(instance, r == 0 ? "MASTER" : "SLAVE");
      }
      resourceAssignment.addReplicaMap(new Partition(resource + "_" + p), replicaMap);
    }
    return resourceAssignment;
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
    Mockito.when(context.getClusterName()).thenReturn("PerfCluster");
    ClusterModel clusterModel = Mockito.mock(ClusterModel.class);
    Mockito.when(clusterModel.getContext()).thenReturn(context);

    return WagedRebalanceUtil.calculateAssignment(clusterModel, algorithm, previous);
  }
}
