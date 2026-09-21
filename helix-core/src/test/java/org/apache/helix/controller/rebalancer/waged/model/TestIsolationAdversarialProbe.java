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
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.TreeMap;

import org.apache.helix.HelixRebalanceException;
import org.apache.helix.controller.rebalancer.waged.RebalanceAlgorithm;
import org.apache.helix.model.ClusterConfig;
import org.apache.helix.model.ResourceAssignment;
import org.testng.Assert;
import org.testng.annotations.Test;

/**
 * Adversarial probes aimed at the guards that decide whether a failure is tolerable, and at the
 * capacity bookkeeping the rollback has to restore exactly.
 *
 * These are deliberately hostile. A test failing here is a finding about the design, not
 * necessarily a bug in a method.
 */
public class TestIsolationAdversarialProbe extends AbstractTestWagedInstanceTagIsolation {

  private ClusterModel model(ClusterConfig config, Set<AssignableReplica> replicas,
      Set<AssignableNode> nodes, ClusterModel.RebalanceScopeType scope) {
    ClusterContext context = new ClusterContext(replicas, nodes, Collections.emptyMap(),
        Collections.emptyMap(), config);
    return new ClusterModel(context, replicas, nodes, scope);
  }

  private ClusterModel model(ClusterConfig config, Set<AssignableReplica> replicas,
      Set<AssignableNode> nodes) {
    return model(config, replicas, nodes, ClusterModel.RebalanceScopeType.GLOBAL_BASELINE);
  }

  /**
   * H1. The cluster is carved into two tag blocks, but only one of them carries any resource. That
   * one resource is unplaceable, so every single replica in the cluster fails.
   *
   * The default global mode throws, which is what raises the operator's rebalance failure metric.
   * The question this asks is what the isolation mode does when the only block with work is the
   * block that failed, and the "is there a second block to recalculate around" guard is satisfied
   * only by a block that has nothing to recalculate.
   */
  @Test
  public void testTotalFailureWithAnIdleSecondBlockDoesNotThrow() throws Exception {
    ClusterConfig config = createClusterConfig(true);
    Set<AssignableNode> nodes = new HashSet<>();
    for (int i = 0; i < 10; i++) {
      nodes.add(taggedNode(config, "busy_" + i, 0, "clique_busy"));
    }
    for (int i = 0; i < 10; i++) {
      nodes.add(taggedNode(config, "idle_" + i, 0, "clique_idle"));
    }
    Set<AssignableReplica> replicas = new HashSet<>();
    addReplicas(replicas, config,
        taggedResource("R_busy", "clique_busy", UNPLACEABLE_PARTITION_WEIGHT), 10);

    OptimalAssignment assignment = null;
    HelixRebalanceException thrown = null;
    try {
      assignment = createAlgorithm().calculate(model(config, replicas, nodes));
    } catch (HelixRebalanceException e) {
      thrown = e;
    }

    // Control: the same cluster with the flag off must throw.
    ClusterConfig off = createClusterConfig(false);
    Set<AssignableNode> offNodes = new HashSet<>();
    for (int i = 0; i < 10; i++) {
      offNodes.add(taggedNode(off, "busy_" + i, 0, "clique_busy"));
    }
    for (int i = 0; i < 10; i++) {
      offNodes.add(taggedNode(off, "idle_" + i, 0, "clique_idle"));
    }
    Set<AssignableReplica> offReplicas = new HashSet<>();
    addReplicas(offReplicas, off,
        taggedResource("R_busy", "clique_busy", UNPLACEABLE_PARTITION_WEIGHT), 10);
    boolean defaultThrew = false;
    try {
      createAlgorithm().calculate(model(off, offReplicas, offNodes));
    } catch (HelixRebalanceException e) {
      defaultThrew = true;
    }
    Assert.assertTrue(defaultThrew, "precondition: the default mode must fail this cluster");

    System.out.println("PROBE-H1 isolationThrew=" + (thrown != null) + " skipped=" + (
        assignment == null ? "n/a" : assignment.getSkippedResources()) + " emitted=" + (
        assignment == null ? "n/a" : assignment.getOptimalResourceAssignment().keySet()));

    Assert.assertNotNull(thrown,
        "a global baseline run in which every group failed achieved nothing, so it must report the "
            + "failure rather than return an empty assignment and a clean bill of health, even "
            + "though an idle tag forms a second attribution block");
  }

  /**
   * H1 control. With no idle tag there is only one block, so the same total failure must throw.
   */
  @Test
  public void testTotalFailureWithNoSecondBlockStillThrows() throws Exception {
    ClusterConfig config = createClusterConfig(true);
    Set<AssignableNode> nodes = new HashSet<>();
    for (int i = 0; i < 10; i++) {
      nodes.add(taggedNode(config, "busy_" + i, 0, "clique_busy"));
    }
    Set<AssignableReplica> replicas = new HashSet<>();
    addReplicas(replicas, config,
        taggedResource("R_busy", "clique_busy", UNPLACEABLE_PARTITION_WEIGHT), 10);
    try {
      createAlgorithm().calculate(model(config, replicas, nodes));
      Assert.fail("a total failure with a single block must throw");
    } catch (HelixRebalanceException expected) {
      // expected
    }
  }

  /**
   * H13. After a clique is rolled back, no node may end up holding more than its capacity in the
   * emitted assignment. A rollback that failed to restore capacity exactly would let a healthy
   * clique overcommit the nodes it shares.
   */
  @Test
  public void testRollbackNeverOvercommitsANode() throws Exception {
    ClusterConfig config = createClusterConfig(true);
    Set<AssignableNode> nodes = new HashSet<>();
    // Two cliques bridged by one instance carrying both tags, so the rollback has to free capacity
    // that the other clique could otherwise take.
    for (int i = 0; i < 4; i++) {
      nodes.add(taggedNode(config, "a_" + i, 0, "clique_a"));
    }
    for (int i = 0; i < 4; i++) {
      nodes.add(taggedNode(config, "b_" + i, 0, "clique_b"));
    }
    for (int i = 0; i < 4; i++) {
      nodes.add(taggedNode(config, "c_" + i, 0, "clique_c"));
    }
    nodes.add(taggedNode(config, "bridge_0", 0, "clique_a", "clique_b"));

    Map<String, Integer> weights = new HashMap<>();
    Set<AssignableReplica> replicas = new HashSet<>();
    // clique_a mostly fits, then one oversized partition set breaks it.
    addReplicas(replicas, config, taggedResource("R_a", "clique_a", 30), 6);
    weights.put("R_a", 30);
    addReplicas(replicas, config, taggedResource("R_a_bad", "clique_a", 150), 3);
    weights.put("R_a_bad", 150);
    addReplicas(replicas, config, taggedResource("R_b", "clique_b", 40), 5);
    weights.put("R_b", 40);
    addReplicas(replicas, config, taggedResource("R_c", "clique_c", 25), 8);
    weights.put("R_c", 25);

    OptimalAssignment assignment = createAlgorithm().calculate(model(config, replicas, nodes));
    Map<String, ResourceAssignment> emitted = assignment.getOptimalResourceAssignment();
    System.out.println(
        "PROBE-H13 skipped=" + assignment.getSkippedResources() + " emitted=" + emitted.keySet());

    Map<String, Integer> usedByInstance = new TreeMap<>();
    emitted.forEach((resource, resourceAssignment) -> resourceAssignment.getMappedPartitions()
        .forEach(partition -> resourceAssignment.getReplicaMap(partition).keySet()
            .forEach(instance -> usedByInstance
                .merge(instance, weights.get(resource), Integer::sum))));
    System.out.println("PROBE-H13 usage=" + usedByInstance);
    usedByInstance.forEach((instance, used) -> Assert.assertTrue(used <= NODE_CAPACITY,
        "instance " + instance + " is overcommitted: " + used + " > " + NODE_CAPACITY));
  }

  /**
   * H14. With the flag off the class must be a total no-op, so a run that fails must fail the same
   * way and a run that succeeds must place identically. This drives a randomized battery so the
   * parity claim is not resting on one hand picked topology.
   *
   * The output is also dumped to a file so it can be diffed against the same battery run on the
   * base branch, which is the only way to prove parity against code that predates this feature.
   */
  @Test
  public void testFlagOnAndOffAgreeWhenNothingFails() throws Exception {
    StringBuilder dump = new StringBuilder();
    for (int seed = 0; seed < 40; seed++) {
      Map<String, Map<String, Map<String, String>>> on = runRandom(seed, true, dump);
      Map<String, Map<String, Map<String, String>>> off = runRandom(seed, false, null);
      Assert.assertEquals(on, off, "flag parity broke on seed " + seed);
    }
    Files.write(Paths.get(System.getProperty("probe.dump", "/tmp/isolation-parity-feature.txt")),
        dump.toString().getBytes());
  }

  /**
   * Builds a random but always placeable clique topology, so the isolation code never reaches any
   * path other than the no-op ones.
   */
  private Map<String, Map<String, Map<String, String>>> runRandom(int seed, boolean flag,
      StringBuilder dump) throws IOException, HelixRebalanceException {
    Random random = new Random(seed);
    ClusterConfig config = createClusterConfig(flag);
    int cliques = 3 + random.nextInt(6);
    Set<AssignableNode> nodes = new HashSet<>();
    Set<AssignableReplica> replicas = new HashSet<>();
    for (int c = 0; c < cliques; c++) {
      int nodeCount = 3 + random.nextInt(5);
      for (int i = 0; i < nodeCount; i++) {
        nodes.add(taggedNode(config, "n_" + c + "_" + i, 0, cliqueTag(c)));
      }
      int partitions = 1 + random.nextInt(nodeCount * 2);
      int weight = 1 + random.nextInt(8);
      addReplicas(replicas, config, taggedResource(resourceName(c), cliqueTag(c), weight),
          partitions);
    }
    RebalanceAlgorithm algorithm = createAlgorithm();
    OptimalAssignment assignment = algorithm.calculate(model(config, replicas, nodes));
    Map<String, Map<String, Map<String, String>>> normalized =
        normalize(assignment.getOptimalResourceAssignment());
    if (dump != null) {
      dump.append("seed=").append(seed).append(' ').append(normalized).append('\n');
    }
    return normalized;
  }

  /**
   * H15. Every instance carries its clique tag plus one ordinary operational label that is shared
   * across cliques, which is how real fleets are tagged (an availability zone, a hardware
   * generation, a pool name). No resource is pinned to that label.
   *
   * shareBlocks correctly ignores a label no resource uses. attributionBlocks does not: it adds a
   * group for every tag any node carries, so the shared label becomes a group that every clique
   * meets on its own nodes, and the union merges every clique into one block.
   */
  @Test
  public void testSharedOperationalLabelCollapsesAttributionBlocks() throws Exception {
    ClusterConfig config = createClusterConfig(true);
    Set<AssignableNode> nodes = new HashSet<>();
    Set<AssignableReplica> replicas = new HashSet<>();
    for (int c = 0; c < 6; c++) {
      for (int i = 0; i < 4; i++) {
        // "prod_pool" is carried by every instance in every clique and no resource uses it.
        nodes.add(taggedNode(config, "n_" + c + "_" + i, 0, cliqueTag(c), "prod_pool"));
      }
      int weight = (c == 3) ? UNPLACEABLE_PARTITION_WEIGHT : 20;
      addReplicas(replicas, config, taggedResource(resourceName(c), cliqueTag(c), weight), 4);
    }

    HelixRebalanceException thrown = null;
    OptimalAssignment assignment = null;
    try {
      assignment = createAlgorithm().calculate(model(config, replicas, nodes));
    } catch (HelixRebalanceException e) {
      thrown = e;
    }
    System.out.println("PROBE-H15 threw=" + (thrown != null) + " skipped=" + (assignment == null
        ? "n/a" : assignment.getSkippedResources()));

    Assert.assertNull(thrown,
        "one broken clique out of six must not fail the whole rebalance just because every "
            + "instance also carries a shared operational label");
    Assert.assertEquals(assignment.getSkippedResources(),
        Collections.singleton(resourceName(3)));
    Assert.assertEquals(assignment.getOptimalResourceAssignment().size(), 5,
        "the five healthy cliques must still be placed");
  }

  /**
   * H15b. The same shared label, but the failure arrives through the cluster wide capacity deficit
   * path instead of the per replica placement path.
   */
  @Test
  public void testSharedOperationalLabelBreaksCapacityAttribution() throws Exception {
    ClusterConfig config = createClusterConfig(true);
    Set<AssignableNode> nodes = new HashSet<>();
    Set<AssignableReplica> replicas = new HashSet<>();
    for (int c = 0; c < 4; c++) {
      for (int i = 0; i < 4; i++) {
        nodes.add(taggedNode(config, "n_" + c + "_" + i, 0, cliqueTag(c), "prod_pool"));
      }
      addReplicas(replicas, config, taggedResource(resourceName(c), cliqueTag(c), 20), 5);
    }
    // One clique demands far more than its own four nodes hold, which also drags the tag blind
    // cluster wide sum negative.
    addReplicas(replicas, config, taggedResource("R_hog", cliqueTag(1), 90), 12);

    HelixRebalanceException thrown = null;
    OptimalAssignment assignment = null;
    try {
      assignment = createAlgorithm().calculate(model(config, replicas, nodes));
    } catch (HelixRebalanceException e) {
      thrown = e;
    }
    System.out.println("PROBE-H15b threw=" + (thrown != null) + " skipped=" + (assignment == null
        ? "n/a" : assignment.getSkippedResources()));
    Assert.assertNull(thrown,
        "a deficit caused by one clique must be attributed to it even when every instance also "
            + "carries a shared operational label");
  }

  /**
   * H15 control. Identical cluster, shared label removed. This is the shape the existing tests use
   * and it is expected to pass, so the only difference between the two is the extra label.
   */
  @Test
  public void testWithoutTheSharedLabelIsolationWorks() throws Exception {
    ClusterConfig config = createClusterConfig(true);
    Set<AssignableNode> nodes = new HashSet<>();
    Set<AssignableReplica> replicas = new HashSet<>();
    for (int c = 0; c < 6; c++) {
      for (int i = 0; i < 4; i++) {
        nodes.add(taggedNode(config, "n_" + c + "_" + i, 0, cliqueTag(c)));
      }
      int weight = (c == 3) ? UNPLACEABLE_PARTITION_WEIGHT : 20;
      addReplicas(replicas, config, taggedResource(resourceName(c), cliqueTag(c), weight), 4);
    }
    OptimalAssignment assignment = createAlgorithm().calculate(model(config, replicas, nodes));
    Assert.assertEquals(assignment.getSkippedResources(), Collections.singleton(resourceName(3)));
    Assert.assertEquals(assignment.getOptimalResourceAssignment().size(), 5);
  }

  /**
   * H6 support. The same logical cluster built with its sets populated in a different order must
   * produce the same decisions, otherwise two controllers disagree after a failover.
   */
  @Test
  public void testShuffledConstructionOrderIsDeterministic() throws Exception {
    Map<String, Map<String, Map<String, String>>> reference = null;
    Set<String> referenceSkipped = null;
    for (int shuffle = 0; shuffle < 12; shuffle++) {
      ClusterConfig config = createClusterConfig(true);
      List<AssignableNode> nodeList = new ArrayList<>();
      for (int c = 0; c < 6; c++) {
        for (int i = 0; i < 4; i++) {
          nodeList.add(taggedNode(config, "n_" + c + "_" + i, 0, cliqueTag(c)));
        }
      }
      List<AssignableReplica> replicaList = new ArrayList<>();
      for (int c = 0; c < 6; c++) {
        Set<AssignableReplica> bucket = new HashSet<>();
        // Cliques 2 and 4 are unplaceable, so several groups fail in the same run.
        int weight = (c == 2 || c == 4) ? UNPLACEABLE_PARTITION_WEIGHT : 20;
        addReplicas(bucket, config, taggedResource(resourceName(c), cliqueTag(c), weight), 5);
        replicaList.addAll(bucket);
      }
      Collections.shuffle(nodeList, new Random(shuffle));
      Collections.shuffle(replicaList, new Random(shuffle * 31L + 7));

      OptimalAssignment assignment = createAlgorithm()
          .calculate(model(config, new HashSet<>(replicaList), new HashSet<>(nodeList)));
      Map<String, Map<String, Map<String, String>>> normalized =
          normalize(assignment.getOptimalResourceAssignment());
      Set<String> skipped = new HashSet<>(assignment.getSkippedResources());
      if (reference == null) {
        reference = normalized;
        referenceSkipped = skipped;
        System.out.println("PROBE-H6 skipped=" + new TreeMap<>(
            Collections.singletonMap("set", new java.util.TreeSet<>(skipped))));
      } else {
        Assert.assertEquals(skipped, referenceSkipped,
            "skipped set diverged on shuffle " + shuffle);
        Assert.assertEquals(normalized, reference, "assignment diverged on shuffle " + shuffle);
      }
    }
  }
}
