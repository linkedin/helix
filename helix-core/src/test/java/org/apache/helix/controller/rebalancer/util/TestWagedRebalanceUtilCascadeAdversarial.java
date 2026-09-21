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

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;

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
 * Adversarial attacks on the instance tag isolation carry forward and its collision cascade in
 * {@link WagedRebalanceUtil#calculateAssignment}. The goal here is to break the cascade, not to
 * confirm it works: termination, ordering, overcommit, stale partition sets, aliasing, and the
 * comparison against what the default global mode would do.
 *
 * A method whose name ends in FINDING asserts the behavior the code claims but does not deliver, so
 * it is expected to fail and stand as the proof of a flaw. Every other method documents behavior the
 * code did survive.
 */
public class TestWagedRebalanceUtilCascadeAdversarial {

  // ---------------------------------------------------------------------------------------------
  // Aliasing: carryForwardOrDrop calls new ResourceAssignment(previous.getRecord()) and comments it
  // as a "deep copy so the result never aliases the caller's previous assignment objects". The
  // ZNRecord copy constructor only putAll's the outer partition map, so the inner per partition
  // replica maps are shared by reference. Prove object identity is fine but the inner maps are not.
  // ---------------------------------------------------------------------------------------------

  @Test
  public void testCarriedForwardDoesNotAliasPreviousObjectOrRecord() throws Exception {
    Map<String, ResourceAssignment> calculated = new HashMap<>();
    calculated.put("Skipped", single("Skipped", "instance-1"));

    Map<String, ResourceAssignment> previous = new HashMap<>();
    previous.put("Skipped", single("Skipped", "instance-9"));

    Map<String, ResourceAssignment> result =
        calculate(calculated, Collections.singleton("Skipped"), previous);

    Assert.assertNotSame(result.get("Skipped"), previous.get("Skipped"),
        "The carried forward ResourceAssignment must be a fresh object");
    Assert.assertNotSame(result.get("Skipped").getRecord(), previous.get("Skipped").getRecord(),
        "The carried forward ZNRecord must be a fresh object");
  }

  /**
   * FINDING: the "deep copy" in carryForwardOrDrop is shallow at the replica map level. Mutating a
   * per partition replica map of the carried forward result writes straight through into the
   * caller's previousAssignment, which the default global mode (it never carries anything forward)
   * can never do to its input. Expected: the caller's input is untouched.
   */
  @Test
  public void testCarriedForwardSharesInnerReplicaMapWithPreviousFINDING() throws Exception {
    Map<String, ResourceAssignment> calculated = new HashMap<>();
    calculated.put("Skipped", single("Skipped", "instance-1"));

    Map<String, ResourceAssignment> previous = new HashMap<>();
    previous.put("Skipped", single("Skipped", "instance-9"));

    Map<String, ResourceAssignment> result =
        calculate(calculated, Collections.singleton("Skipped"), previous);

    // A downstream consumer mutates the returned assignment's replica map in place.
    Partition partition = new Partition(PARTITION);
    result.get("Skipped").getReplicaMap(partition).put("ghost", "MASTER");

    Assert.assertFalse(previous.get("Skipped").getReplicaMap(partition).containsKey("ghost"),
        "Mutating the returned assignment must not reach back into the caller's previousAssignment");
  }

  // ---------------------------------------------------------------------------------------------
  // Termination and ordering.
  // ---------------------------------------------------------------------------------------------

  /**
   * A long retag chain: each fresh resource is placed on the instance the previous resource just
   * gave up, so the cascade must carry them forward one after another. If it looped forever, or ran
   * more rounds than there are resources, the timeout would trip. It converges and every link ends
   * on its own previous assignment.
   */
  @Test(timeOut = 30000)
  public void testCascadeTerminatesOnLongRetagChain() throws Exception {
    int chain = 300;
    Map<String, ResourceAssignment> calculated = new HashMap<>();
    Map<String, ResourceAssignment> previous = new HashMap<>();
    // link-k is the disputed instance handed from resource k to resource k+1.
    for (int k = 0; k < chain; k++) {
      previous.put("R" + k, single("R" + k, "link-" + k));
      if (k > 0) {
        // R_k was freshly calculated onto the instance R_{k-1} used to own.
        calculated.put("R" + k, single("R" + k, "link-" + (k - 1)));
      }
    }
    // R0 is the broken group. Its fresh partial sits on a throwaway node; it is carried to link-0.
    calculated.put("R0", single("R0", "throwaway"));

    Map<String, ResourceAssignment> result =
        calculate(calculated, Collections.singleton("R0"), previous);

    Assert.assertEquals(result.size(), chain, "Every resource stays in the result");
    for (int k = 0; k < chain; k++) {
      Assert.assertEquals(instancesOf(result.get("R" + k)), Collections.singleton("link-" + k),
          "Every link in the chain gives up its fresh assignment for its own previous one");
    }
  }

  /**
   * The emitted map must not depend on the iteration order of the skipped set. Two skipped resources
   * plus a two hop cascade, run with the skipped set in both orders, must produce byte for byte the
   * same instance placement.
   */
  @Test
  public void testCascadeIsDeterministicRegardlessOfSkippedIterationOrder() throws Exception {
    Map<String, Set<String>> orderAB = runTwoSkippedCascade(new LinkedHashSet<>(orderedSet("A", "B")));
    Map<String, Set<String>> orderBA = runTwoSkippedCascade(new LinkedHashSet<>(orderedSet("B", "A")));
    Assert.assertEquals(orderAB, orderBA,
        "Carried set is a fixpoint of reachable collisions, so the result is order independent");
  }

  private Map<String, Set<String>> runTwoSkippedCascade(Set<String> skipped) throws Exception {
    Map<String, ResourceAssignment> calculated = new HashMap<>();
    calculated.put("A", single("A", "fresh-a"));
    calculated.put("B", single("B", "fresh-b"));
    calculated.put("Mover1", single("Mover1", "i1")); // collides with A carried to i1
    calculated.put("Mover2", single("Mover2", "i2")); // collides with B carried to i2
    calculated.put("Chain", single("Chain", "i3"));   // collides with Mover1 reverted to i3
    calculated.put("Free", single("Free", "i99"));

    Map<String, ResourceAssignment> previous = new HashMap<>();
    previous.put("A", single("A", "i1"));
    previous.put("B", single("B", "i2"));
    previous.put("Mover1", single("Mover1", "i3"));
    previous.put("Mover2", single("Mover2", "i4"));
    previous.put("Chain", single("Chain", "i7"));

    return snapshot(calculate(calculated, skipped, previous));
  }

  // ---------------------------------------------------------------------------------------------
  // Overcommit: the cascade exists to stop a fresh resource and a carried resource sharing a node.
  // Verify no fresh-vs-carried share survives, and characterize the carried-vs-carried case the
  // cascade deliberately does not police.
  // ---------------------------------------------------------------------------------------------

  /**
   * After a multi hop cascade every instance in the final map is owned by exactly one resource, so
   * no fresh resource is left sharing a node with a carried one. This is the property the cascade
   * promises; it holds.
   */
  @Test
  public void testNoFreshResourceSharesInstanceWithCarriedAfterCascade() throws Exception {
    Map<String, ResourceAssignment> calculated = new HashMap<>();
    calculated.put("Skipped", single("Skipped", "throwaway"));
    calculated.put("Mover", single("Mover", "i1"));   // collides with Skipped carried to i1
    calculated.put("Chain", single("Chain", "i3"));   // collides with Mover reverted to i3
    calculated.put("Free", single("Free", "i50"));

    Map<String, ResourceAssignment> previous = new HashMap<>();
    previous.put("Skipped", single("Skipped", "i1"));
    previous.put("Mover", single("Mover", "i3"));
    previous.put("Chain", single("Chain", "i7"));

    Map<String, ResourceAssignment> result =
        calculate(calculated, Collections.singleton("Skipped"), previous);

    assertNoInstanceSharedAcrossResources(result);
    Assert.assertEquals(instancesOf(result.get("Chain")), Collections.singleton("i7"),
        "The second hop resource also gives up its fresh assignment");
  }

  /**
   * Two skipped resources whose previous assignments legitimately name the same instance (same
   * clique co-location in the last good snapshot) are both carried forward and both keep that shared
   * name. The cascade never checks carried against carried, trusting the previous snapshot to be
   * internally coherent. Documented so the reliance on that invariant is explicit.
   */
  @Test
  public void testCarriedVsCarriedSharedInstanceIsEmittedUnchecked() throws Exception {
    Map<String, ResourceAssignment> calculated = new HashMap<>();
    calculated.put("A", single("A", "throwaway-a"));
    calculated.put("B", single("B", "throwaway-b"));
    calculated.put("Free", single("Free", "i2"));

    Map<String, ResourceAssignment> previous = new HashMap<>();
    previous.put("A", single("A", "shared"));
    previous.put("B", single("B", "shared"));

    Map<String, ResourceAssignment> result =
        calculate(calculated, orderedSet("A", "B"), previous);

    Assert.assertEquals(instancesOf(result.get("A")), Collections.singleton("shared"));
    Assert.assertEquals(instancesOf(result.get("B")), Collections.singleton("shared"),
        "Carried-vs-carried sharing is emitted verbatim; only the previous snapshot's coherence "
            + "keeps this from being a real overcommit");
  }

  // ---------------------------------------------------------------------------------------------
  // The dropped resources. A skipped resource with no previous, and a fresh resource pulled into the
  // cascade with no previous, are both removed from the result rather than carried.
  // ---------------------------------------------------------------------------------------------

  /**
   * A fresh, validly placed resource that merely reused a node a broken group still claims, and that
   * has no previous assignment to fall back to, is dropped from the result entirely. This is the one
   * shape where isolation gives up a resource the fresh calculation had already placed. It is not
   * worse than the default mode (a whole-cluster failure would leave the same resource unplaced),
   * but it does contradict the "keeps the returned map complete" comment.
   */
  @Test
  public void testFreshResourceWithNoPreviousIsDroppedByCascade() throws Exception {
    Map<String, ResourceAssignment> calculated = new HashMap<>();
    calculated.put("Skipped", single("Skipped", "throwaway"));
    calculated.put("NewResource", single("NewResource", "i1")); // valid fresh placement on i1

    Map<String, ResourceAssignment> previous = new HashMap<>();
    previous.put("Skipped", single("Skipped", "i1")); // broken group still claims i1

    Map<String, ResourceAssignment> result =
        calculate(calculated, Collections.singleton("Skipped"), previous);

    Assert.assertEquals(instancesOf(result.get("Skipped")), Collections.singleton("i1"));
    Assert.assertFalse(result.containsKey("NewResource"),
        "A brand new resource with no previous is dropped when it collides with a carried group");
  }

  // ---------------------------------------------------------------------------------------------
  // H9 stale partition set. The previous assignment is copied verbatim, so it can carry a partition
  // set that no longer matches the resource. Characterize each shape and compare to the default mode
  // (which keeps the identical stale snapshot for the whole cluster).
  // ---------------------------------------------------------------------------------------------

  @Test
  public void testStalePartitionSetGrewCarriesOnlyTheOldPartitions() throws Exception {
    // Resource now has two partitions, previous snapshot only knew one.
    Map<String, ResourceAssignment> calculated = new HashMap<>();
    ResourceAssignment freshTwoPartitions = new ResourceAssignment("Grew");
    freshTwoPartitions.addReplicaMap(new Partition("Grew_0"), Collections.singletonMap("i1", "MASTER"));
    freshTwoPartitions.addReplicaMap(new Partition("Grew_1"), Collections.singletonMap("i2", "MASTER"));
    calculated.put("Grew", freshTwoPartitions);

    Map<String, ResourceAssignment> previous = new HashMap<>();
    ResourceAssignment prevOnePartition = new ResourceAssignment("Grew");
    prevOnePartition.addReplicaMap(new Partition("Grew_0"), Collections.singletonMap("i9", "MASTER"));
    previous.put("Grew", prevOnePartition);

    Map<String, ResourceAssignment> result =
        calculate(calculated, Collections.singleton("Grew"), previous);

    Set<String> partitions = new HashSet<>();
    result.get("Grew").getMappedPartitions().forEach(p -> partitions.add(p.getPartitionName()));
    Assert.assertEquals(partitions, Collections.singleton("Grew_0"),
        "Only the partitions the previous snapshot knew are carried; the new partition is absent");
  }

  @Test
  public void testStalePartitionSetShrankCarriesGhostPartitions() throws Exception {
    // Resource shrank to one partition; previous snapshot still names a partition that is gone.
    Map<String, ResourceAssignment> calculated = new HashMap<>();
    calculated.put("Shrank", single("Shrank", "i1"));

    Map<String, ResourceAssignment> previous = new HashMap<>();
    ResourceAssignment prevTwoPartitions = new ResourceAssignment("Shrank");
    prevTwoPartitions.addReplicaMap(new Partition(PARTITION), Collections.singletonMap("i9", "MASTER"));
    prevTwoPartitions.addReplicaMap(new Partition("Shrank_ghost"), Collections.singletonMap("i8", "MASTER"));
    previous.put("Shrank", prevTwoPartitions);

    Map<String, ResourceAssignment> result =
        calculate(calculated, Collections.singleton("Shrank"), previous);

    Set<String> partitions = new HashSet<>();
    result.get("Shrank").getMappedPartitions().forEach(p -> partitions.add(p.getPartitionName()));
    Assert.assertTrue(partitions.contains("Shrank_ghost"),
        "A partition that no longer exists is carried forward as a ghost, same as the default mode "
            + "keeps in its last good snapshot");
  }

  // ---------------------------------------------------------------------------------------------
  // H-EXTRA edge cases.
  // ---------------------------------------------------------------------------------------------

  /**
   * A resource that was deleted (present in previous, absent from the fresh result, not skipped) is
   * never resurrected: the carry forward only ever touches skipped resources and freshly calculated
   * colliders, both of which are current.
   */
  @Test
  public void testDeletedResourceInPreviousIsNotResurrected() throws Exception {
    Map<String, ResourceAssignment> calculated = new HashMap<>();
    calculated.put("Skipped", single("Skipped", "throwaway"));
    calculated.put("Alive", single("Alive", "i5"));

    Map<String, ResourceAssignment> previous = new HashMap<>();
    previous.put("Skipped", single("Skipped", "i1"));
    previous.put("Deleted", single("Deleted", "i2"));

    Map<String, ResourceAssignment> result =
        calculate(calculated, Collections.singleton("Skipped"), previous);

    Assert.assertFalse(result.containsKey("Deleted"),
        "A resource only present in the previous snapshot is never pulled into the result");
  }

  /** A skipped resource whose previous entry is an empty assignment is carried forward as present but empty. */
  @Test
  public void testEmptyPreviousCarriesResourcePresentButEmpty() throws Exception {
    Map<String, ResourceAssignment> calculated = new HashMap<>();
    calculated.put("Skipped", single("Skipped", "throwaway"));

    Map<String, ResourceAssignment> previous = new HashMap<>();
    previous.put("Skipped", new ResourceAssignment("Skipped")); // zero partitions

    Map<String, ResourceAssignment> result =
        calculate(calculated, Collections.singleton("Skipped"), previous);

    Assert.assertTrue(result.containsKey("Skipped"), "An empty previous still counts as present");
    Assert.assertTrue(result.get("Skipped").getMappedPartitions().isEmpty(),
        "It is carried forward with no partitions, not dropped");
  }

  /** With a null previous (the delayed overwrite phase), skipped resources are dropped and nothing cascades. */
  @Test
  public void testNullPreviousDropsSkippedAndDoesNotCascade() throws Exception {
    Map<String, ResourceAssignment> calculated = new HashMap<>();
    calculated.put("Skipped", single("Skipped", "i1"));
    calculated.put("Mover", single("Mover", "i1")); // would collide if Skipped were carried

    Map<String, ResourceAssignment> result =
        calculate(calculated, Collections.singleton("Skipped"), null);

    Assert.assertFalse(result.containsKey("Skipped"), "Null previous drops the skipped resource");
    Assert.assertEquals(instancesOf(result.get("Mover")), Collections.singleton("i1"),
        "Nothing is carried, so nothing collides, so Mover keeps its fresh assignment");
  }

  /** calculateAssignment must not mutate the skipped set, which OptimalAssignment hands out unmodifiable. */
  @Test
  public void testUnmodifiableSkippedSetIsNotMutated() throws Exception {
    Map<String, ResourceAssignment> calculated = new HashMap<>();
    calculated.put("Skipped", single("Skipped", "throwaway"));
    calculated.put("Mover", single("Mover", "i1"));

    Map<String, ResourceAssignment> previous = new HashMap<>();
    previous.put("Skipped", single("Skipped", "i1"));
    previous.put("Mover", single("Mover", "i3"));

    Set<String> unmodifiable = Collections.unmodifiableSet(new HashSet<>(Collections.singleton("Skipped")));
    // Must not throw UnsupportedOperationException.
    Map<String, ResourceAssignment> result = calculate(calculated, unmodifiable, previous);
    Assert.assertEquals(instancesOf(result.get("Mover")), Collections.singleton("i3"));
  }

  /** When resources are skipped the returned map is a fresh copy, never the caller's previous map instance. */
  @Test
  public void testResultMapIsNeverThePreviousMapInstance() throws Exception {
    Map<String, ResourceAssignment> calculated = new HashMap<>();
    calculated.put("Skipped", single("Skipped", "throwaway"));

    Map<String, ResourceAssignment> previous = new HashMap<>();
    previous.put("Skipped", single("Skipped", "i1"));

    Map<String, ResourceAssignment> result =
        calculate(calculated, Collections.singleton("Skipped"), previous);

    Assert.assertNotSame(result, previous, "The result must not be the caller's previous map");
    Assert.assertNotSame(result, calculated, "The result is a copy, not the algorithm's own map");
    previous.remove("Skipped");
    Assert.assertTrue(result.containsKey("Skipped"),
        "Mutating the previous map after the fact must not change the result");
  }

  /** A skipped resource with no partial entry in the calculated map is still carried from its previous. */
  @Test
  public void testSkippedResourceAbsentFromCalculatedIsStillCarried() throws Exception {
    Map<String, ResourceAssignment> calculated = new HashMap<>();
    calculated.put("Healthy", single("Healthy", "i2"));
    // "Skipped" has no partial entry at all here.

    Map<String, ResourceAssignment> previous = new HashMap<>();
    previous.put("Skipped", single("Skipped", "i9"));

    Map<String, ResourceAssignment> result =
        calculate(calculated, Collections.singleton("Skipped"), previous);

    Assert.assertEquals(instancesOf(result.get("Skipped")), Collections.singleton("i9"),
        "A skipped resource missing from the calculated map is still restored from its previous");
  }

  /**
   * Differential fuzz. Build many random retag scenarios and compare the production cascade against
   * an independent, deliberately naive reimplementation of the intended fixpoint (carry a fresh
   * group forward whenever it names any carried instance, repeat to convergence). If production ever
   * disagrees, either it missed a collision (a surviving overcommit) or carried more than it should.
   * Across the whole run they agree, which is the strongest evidence the cascade is correct.
   */
  @Test
  public void testFuzzMatchesIndependentCascadeReference() throws Exception {
    java.util.Random random = new java.util.Random(20260920L);
    for (int iteration = 0; iteration < 500; iteration++) {
      int resourceCount = 2 + random.nextInt(10);
      int instancePool = 2 + random.nextInt(8);

      Map<String, ResourceAssignment> calculated = new HashMap<>();
      Map<String, ResourceAssignment> previous = new HashMap<>();
      Map<String, Set<String>> freshSets = new HashMap<>();
      Map<String, Set<String>> previousSets = new HashMap<>();
      Set<String> skipped = new LinkedHashSet<>();

      for (int r = 0; r < resourceCount; r++) {
        String resource = "R" + r;
        Set<String> fresh = randomInstanceSet(random, instancePool, 1 + random.nextInt(3));
        calculated.put(resource, multi(resource, fresh));
        freshSets.put(resource, fresh);
        if (random.nextBoolean()) {
          Set<String> prev = randomInstanceSet(random, instancePool, 1 + random.nextInt(3));
          previous.put(resource, multi(resource, prev));
          previousSets.put(resource, prev);
        }
        if (random.nextInt(4) == 0) {
          skipped.add(resource);
        }
      }

      Map<String, Set<String>> expected =
          referenceCascade(freshSets, previousSets, skipped);
      Map<String, Set<String>> actual =
          snapshot(calculate(calculated, skipped, previous));

      Assert.assertEquals(actual, expected,
          "Production cascade diverged from the reference fixpoint on iteration " + iteration
              + " skipped=" + skipped + " fresh=" + freshSets + " previous=" + previousSets);
    }
  }

  /** Independent, naive fixpoint used only by the fuzz test. */
  private static Map<String, Set<String>> referenceCascade(Map<String, Set<String>> fresh,
      Map<String, Set<String>> previous, Set<String> skipped) {
    Map<String, Set<String>> out = new HashMap<>();
    fresh.forEach((resource, instances) -> out.put(resource, new TreeSet<>(instances)));
    Set<String> carried = new HashSet<>(skipped);
    for (String resource : skipped) {
      carryOrDropReference(out, previous, resource);
    }
    boolean changed = true;
    while (changed) {
      changed = false;
      Set<String> carriedInstances = new TreeSet<>();
      for (String resource : carried) {
        if (out.containsKey(resource)) {
          carriedInstances.addAll(out.get(resource));
        }
      }
      if (carriedInstances.isEmpty()) {
        break;
      }
      for (String resource : new TreeSet<>(out.keySet())) {
        if (carried.contains(resource)) {
          continue;
        }
        if (!Collections.disjoint(out.get(resource), carriedInstances)) {
          carryOrDropReference(out, previous, resource);
          carried.add(resource);
          changed = true;
          break;
        }
      }
    }
    return new TreeMap<>(out);
  }

  private static void carryOrDropReference(Map<String, Set<String>> out,
      Map<String, Set<String>> previous, String resource) {
    if (previous.containsKey(resource)) {
      out.put(resource, new TreeSet<>(previous.get(resource)));
    } else {
      out.remove(resource);
    }
  }

  private static Set<String> randomInstanceSet(java.util.Random random, int pool, int size) {
    Set<String> instances = new TreeSet<>();
    for (int i = 0; i < size; i++) {
      instances.add("inst-" + random.nextInt(pool));
    }
    return instances;
  }

  // ---------------------------------------------------------------------------------------------
  // Fixture, mirrors TestWagedRebalanceUtilCarryForward.
  // ---------------------------------------------------------------------------------------------

  private static final String PARTITION = "Resource_0";

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

  private static ResourceAssignment single(String resource, String instance) {
    ResourceAssignment resourceAssignment = new ResourceAssignment(resource);
    // A mutable replica map, matching production (OptimalAssignment.updateAssignments and ZNRecord
    // deserialization both build HashMaps). A singletonMap here would hide the aliasing under an
    // UnsupportedOperationException instead of exposing the shared reference.
    Map<String, String> replicaMap = new HashMap<>();
    replicaMap.put(instance, "MASTER");
    resourceAssignment.addReplicaMap(new Partition(PARTITION), replicaMap);
    return resourceAssignment;
  }

  private static ResourceAssignment multi(String resource, Set<String> instances) {
    ResourceAssignment resourceAssignment = new ResourceAssignment(resource);
    Map<String, String> replicaMap = new HashMap<>();
    for (String instance : instances) {
      replicaMap.put(instance, "MASTER");
    }
    resourceAssignment.addReplicaMap(new Partition(PARTITION), replicaMap);
    return resourceAssignment;
  }

  private static Set<String> orderedSet(String... values) {
    Set<String> set = new LinkedHashSet<>();
    Collections.addAll(set, values);
    return set;
  }

  private static Set<String> instancesOf(ResourceAssignment resourceAssignment) {
    Assert.assertNotNull(resourceAssignment, "Expected the resource to be present in the result");
    Set<String> instances = new HashSet<>();
    resourceAssignment.getMappedPartitions()
        .forEach(partition -> instances.addAll(resourceAssignment.getReplicaMap(partition).keySet()));
    return instances;
  }

  private static Map<String, Set<String>> snapshot(Map<String, ResourceAssignment> result) {
    Map<String, Set<String>> snapshot = new TreeMap<>();
    result.forEach((resource, assignment) -> snapshot.put(resource, instancesOf(assignment)));
    return snapshot;
  }

  private static void assertNoInstanceSharedAcrossResources(Map<String, ResourceAssignment> result) {
    Map<String, String> owner = new HashMap<>();
    List<String> conflicts = new ArrayList<>();
    for (String resource : new TreeSet<>(result.keySet())) {
      for (String instance : new TreeSet<>(instancesOf(result.get(resource)))) {
        String previousOwner = owner.putIfAbsent(instance, resource);
        if (previousOwner != null) {
          conflicts.add(instance + " shared by " + previousOwner + " and " + resource);
        }
      }
    }
    Assert.assertTrue(conflicts.isEmpty(),
        "No instance should be owned by two resources after the cascade, found: " + conflicts);
  }
}
