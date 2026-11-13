package org.apache.helix.controller.rebalancer.waged;

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
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Random;

import com.google.common.collect.ImmutableMap;
import org.apache.helix.AccessOption;
import org.apache.helix.HelixDataAccessor;
import org.apache.helix.PropertyKey;
import org.apache.helix.common.ZkTestBase;
import org.apache.helix.constants.InstanceConstants;
import org.apache.helix.guardrail.GuardrailContext;
import org.apache.helix.guardrail.GuardrailRule;
import org.apache.helix.guardrail.ValidationResult;
import org.apache.helix.guardrail.Violation;
import org.apache.helix.guardrail.WagedAssignmentProvider;
import org.apache.helix.guardrail.WagedAssignmentProvider.DrainFeasibility;
import org.apache.helix.guardrail.rules.InstanceOperationRebalanceFeasibilityGuardrailRule;
import org.apache.helix.manager.zk.ZKHelixDataAccessor;
import org.apache.helix.manager.zk.ZkBucketDataAccessor;
import org.apache.helix.model.ClusterConfig;
import org.apache.helix.model.CurrentState;
import org.apache.helix.model.IdealState;
import org.apache.helix.model.InstanceConfig;
import org.apache.helix.model.LiveInstance;
import org.apache.helix.model.Partition;
import org.apache.helix.model.ResourceAssignment;
import org.apache.helix.model.ResourceConfig;
import org.apache.helix.model.StateModelDefinition;
import org.apache.helix.util.HelixUtil;
import org.apache.helix.zookeeper.api.client.HelixZkClient;
import org.apache.helix.zookeeper.datamodel.serializer.ByteArraySerializer;
import org.apache.helix.zookeeper.impl.factory.DedicatedZkClientFactory;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

/**
 * Integration test that drives the REAL {@link InstanceOperationRebalanceFeasibilityGuardrailRule}
 * against a live ZK-backed cluster and proves the WAGED drain-feasibility fast path
 * ({@link WagedRebalancer#isInstanceDrainFeasibleViaPartialRebalance}, reached via
 * {@link HelixUtil#isDrainFeasibleViaPartialRebalance}) and the authoritative full two-what-if guard
 * rail agree on the verdict - the scenario the rule's unit tests cannot cover because they stub the
 * provider (so the fast path there is always the default {@code INCONCLUSIVE}).
 *
 * <p>Two scenarios, each wiring the provider exactly as {@code PerInstanceAccessor} does in
 * production (a thin lambda over the WAGED util):
 * <ul>
 *   <li><b>Feasible drain:</b> the fast path certifies the drain (FEASIBLE) and the full what-if also
 *       finds it feasible - they agree, and the rule returns without running either full what-if.</li>
 *   <li><b>Resource missing from the best possible:</b> a second resource is added to the IdealStates
 *       but never written to the persisted best possible (as during controller lag or maintenance
 *       mode). The fast path must DECLINE (its seed under-covers that resource) and fall back to the
 *       full what-if, which blocks. This is the regression guard for the seed-coverage false-accept:
 *       before the fix the probe certified this drain as feasible while the full what-if rejected it.</li>
 *   <li><b>Resource covered only in a non-target state:</b> a second resource is again absent from the
 *       persisted best possible, but this time its current-state fallback reports the partition as
 *       {@code {survivorA: MASTER, survivorB: ERROR}}. The padded ERROR entry clears a raw seed count
 *       yet leaves the SLAVE uncovered, so the fast path must still DECLINE and fall back to the full
 *       what-if, which blocks. This is the regression guard for the per-state seed-coverage follow-up
 *       (PR #365 review): before that fix a raw {@code .size()} check counted the ERROR entry and the
 *       probe certified the drain while the full what-if rejected it.</li>
 * </ul>
 */
public class TestInstanceDrainFastPathWhatIfParity extends ZkTestBase {
  private static final String CAPACITY_KEY = "DISK";
  private static final int CAPACITY = 100;
  // One byte-array client for the WAGED metadata store, shared across what-ifs as helix-rest does.
  private HelixZkClient _bucketZkClient;

  @BeforeClass
  public void connectBucketZkClient() {
    _bucketZkClient = DedicatedZkClientFactory.getInstance()
        .buildZkClient(new HelixZkClient.ZkConnectionConfig(ZK_ADDR));
    _bucketZkClient.setZkSerializer(new ByteArraySerializer());
  }

  @AfterClass
  public void closeBucketZkClient() {
    _bucketZkClient.close();
  }

  /**
   * A normal feasible drain: both the fast path and the full what-if agree the drain is feasible, and
   * the fast path proves it without running either full what-if.
   */
  @Test
  public void testFeasibleDrainFastPathCertifiesAndAgreesWithFullWhatIf() {
    String cluster = "TestDrainParityFeasible";
    // Light enough that the drained node's replicas comfortably re-place on the survivors.
    int numInstances = 3;
    int weight = 20;
    setupCluster(cluster, numInstances, weight);
    createResourceWithWagedRebalance(cluster, "Feasible", "MasterSlave", 3, 2, 2);
    // Seed the persisted best possible so the fast path's emergency model covers every replica.
    seedBestPossible(cluster, readIdealStates(cluster));

    String target = instanceName(0);
    Outcome fastPath = evaluateRule(cluster, target, true);
    Outcome fullOnly = evaluateRule(cluster, target, false);

    Assert.assertTrue(fullOnly.feasible, "the full what-if must find this drain feasible");
    Assert.assertTrue(fastPath.feasible,
        "the fast path must agree the drain is feasible (same verdict as the full what-if)");
    Assert.assertTrue(fastPath.probeCalls > 0, "the fast path must have been attempted");
    Assert.assertEquals(fastPath.probeHits, fastPath.probeCalls,
        "the fast path must certify this feasible drain (FEASIBLE short-circuit)");

    tearDownCluster(cluster);
  }

  /**
   * Regression guard for the seed-coverage false-accept (PR #365 review). A resource that is in the
   * IdealStates but not in the persisted best possible must make the fast path DECLINE, so the rule
   * falls back to the full what-if, which blocks. Before the fix the fast path certified this drain
   * as feasible while the full what-if rejected it.
   */
  @Test
  public void testResourceMissingFromBestPossibleFastPathDeclinesAndAgreesWithFullWhatIf() {
    String cluster = "TestDrainParityMissingSeed";
    // 3 nodes, DISK 100, weight 60. After draining one node the two survivors hold at most one
    // weight-60 replica each (2 x 60 = 120 > 100), i.e. two placement slots. The demand is R1 (1
    // replica) + R2 (2 replicas) = 3 replicas, total weight 180. Aggregate capacity (2 x 100 = 200)
    // covers 180, so the fast path's model does NOT trip the aggregate CAPACITY_DEFICIT gate - the
    // infeasibility is purely bin-packing (3 replicas, 2 slots), which only the full placement sees.
    // That is exactly what exposes the seed-coverage gap: with R2 absent from the seed the fast path
    // ignores it, places only R1's drained replica (fits), and would certify the drain - while the
    // full what-if, placing all 3 replicas, must drop one and blocks.
    int numInstances = 3;
    int weight = 60;
    setupCluster(cluster, numInstances, weight);

    // R1: 1 partition x 1 replica. Seed the best possible with ONLY R1.
    createResourceWithWagedRebalance(cluster, "R1", "MasterSlave", 1, 1, 1);
    Map<String, ResourceAssignment> seed = seedBestPossible(cluster, readIdealStates(cluster));

    // R2: 1 partition x 2 replicas, added to the IdealStates AFTER seeding -> absent from the
    // persisted best possible (its seed, via current-state fallback, is empty because no participant
    // is reporting state here), exactly the window the review describes.
    createResourceWithWagedRebalance(cluster, "R2", "MasterSlave", 1, 2, 2);

    // Drain the node currently hosting R1's single replica (per the review's repro).
    String target = hostOfSingleReplica(seed, "R1");

    Outcome fastPath = evaluateRule(cluster, target, true);
    Outcome fullOnly = evaluateRule(cluster, target, false);

    Assert.assertFalse(fullOnly.feasible, "the full what-if must block this drain");
    Assert.assertTrue(fastPath.probeCalls > 0, "the fast path must have been attempted");
    Assert.assertEquals(fastPath.probeHits, 0,
        "the fast path must NOT certify a drain the full what-if rejects (seed-coverage regression guard)");
    Assert.assertFalse(fastPath.feasible,
        "with the fast path declined, the rule must fall back to the full what-if and stay blocked");

    tearDownCluster(cluster);
  }

  /**
   * Regression guard for the per-state seed-coverage follow-up (PR #365 review). A resource absent
   * from the persisted best possible whose current-state fallback covers the partition only in a
   * non-target state - {@code {survivorA: MASTER, survivorB: ERROR}} - must still make the fast path
   * DECLINE, so the rule falls back to the full what-if, which blocks. A raw seed count was fooled by
   * the ERROR padding (two entries >= two replicas) and certified the drain; comparing per state
   * against what the emergency model generates closes the gap.
   */
  @Test
  public void testResourceCoveredOnlyInNonTargetStateFastPathDeclinesAndAgreesWithFullWhatIf() {
    String cluster = "TestDrainParityErrorPaddedSeed";
    // Same bin-packing-infeasible shape as the missing-seed guard: 3 nodes, DISK 100, weight 60, so
    // after draining one node the two survivors hold one weight-60 replica each (two slots) while the
    // demand is R1 (1) + R2 (2) = 3 replicas, total weight 180 <= 200 (no aggregate CAPACITY_DEFICIT).
    // The only difference from the missing-seed guard is HOW R2 under-covers the seed: here R2 is
    // reported in CurrentState as {survivorA: MASTER, survivorB: ERROR}. That padding gives R2 two
    // seed entries - enough to clear a raw count check - but only the MASTER is in a target state, so
    // the emergency model pins just the MASTER and the SLAVE silently drops to contributing only to
    // the aggregate capacity total. The per-state guard must still decline; the full what-if, placing
    // all 3 replicas, blocks.
    int numInstances = 3;
    int weight = 60;
    setupCluster(cluster, numInstances, weight);

    // R1: 1 partition x 1 replica. Seed the best possible with ONLY R1.
    createResourceWithWagedRebalance(cluster, "R1", "MasterSlave", 1, 1, 1);
    Map<String, ResourceAssignment> seed = seedBestPossible(cluster, readIdealStates(cluster));

    // R2: 1 partition x 2 replicas, added to the IdealStates AFTER seeding -> absent from the
    // persisted best possible, so its seed falls back to CurrentState.
    createResourceWithWagedRebalance(cluster, "R2", "MasterSlave", 1, 2, 2);

    // Drain R1's host; its two peers are the survivors that carry R2's padded current state.
    String target = hostOfSingleReplica(seed, "R1");
    List<String> survivors = otherInstances(numInstances, target);
    // R2's partition reported MASTER on one survivor and ERROR on the other: two seed entries, one
    // target state (OFFLINE behaves the same).
    writeCurrentState(cluster, survivors.get(0), "R2", "MasterSlave", "R2_0", "MASTER");
    writeCurrentState(cluster, survivors.get(1), "R2", "MasterSlave", "R2_0", "ERROR");

    Outcome fastPath = evaluateRule(cluster, target, true);
    Outcome fullOnly = evaluateRule(cluster, target, false);

    Assert.assertFalse(fullOnly.feasible, "the full what-if must block this drain");
    Assert.assertTrue(fastPath.probeCalls > 0, "the fast path must have been attempted");
    Assert.assertEquals(fastPath.probeHits, 0,
        "the fast path must NOT certify a drain whose seed covers a replica only in a non-target "
            + "state (per-state seed-coverage regression guard)");
    Assert.assertFalse(fastPath.feasible,
        "with the fast path declined, the rule must fall back to the full what-if and stay blocked");

    tearDownCluster(cluster);
  }

  /**
   * A pinned survivor that is already over its capacity must make the fast path DECLINE. Four nodes
   * hold two weight-30 replicas each, then n0's capacity drops to 50 (60 pinned). Draining n3 fits
   * its two replicas on n1 and n2, and the aggregate (250 vs 240) passes, so without the guard the
   * probe certified it. The full what-if can only place 1 + 3 + 3 = 7 of 8 replicas and blocks.
   */
  @Test
  public void testPinnedReplicaOverCapacityFastPathDeclinesAndAgreesWithFullWhatIf() {
    String cluster = "TestDrainParityOverCapacityPin";
    int numInstances = 4;
    setupCluster(cluster, numInstances, 30);
    createResourceWithWagedRebalance(cluster, "R", "MasterSlave", 8, 1, 1);
    ResourceAssignment assignment = new ResourceAssignment("R");
    for (int p = 0; p < 8; p++) {
      assignment.addReplicaMap(new Partition("R_" + p),
          Collections.singletonMap(instanceName(p % numInstances), "MASTER"));
    }
    AssignmentMetadataStore store = new AssignmentMetadataStore(ZK_ADDR, cluster);
    try {
      store.persistBaseline(Collections.singletonMap("R", assignment));
      store.persistBestPossibleAssignment(Collections.singletonMap("R", assignment));
    } finally {
      store.close();
    }
    ZKHelixDataAccessor accessor = new ZKHelixDataAccessor(cluster, _baseAccessor);
    InstanceConfig shrunk = accessor.getProperty(accessor.keyBuilder().instanceConfig(instanceName(0)));
    shrunk.setInstanceCapacityMap(Collections.singletonMap(CAPACITY_KEY, 50));
    accessor.setProperty(accessor.keyBuilder().instanceConfig(instanceName(0)), shrunk);

    String target = instanceName(3);
    Outcome fastPath = evaluateRule(cluster, target, true);
    Outcome fullOnly = evaluateRule(cluster, target, false);

    Assert.assertFalse(fullOnly.feasible, "the full what-if must block this drain");
    Assert.assertTrue(fastPath.probeCalls > 0, "the fast path must have been attempted");
    Assert.assertEquals(fastPath.probeHits, 0,
        "the fast path must NOT certify a drain around a pinned replica that is over capacity");
    Assert.assertFalse(fastPath.feasible, "the rule must fall back to the full what-if and block");

    tearDownCluster(cluster);
  }

  /**
   * With instance tag isolation on, WAGED skips a clique it cannot place instead of failing. The fast
   * path must not read that skip as a clean placement: it declines, and the full what-if, which
   * applies isolation, blocks the drain.
   */
  @Test
  public void testIsolatedCliqueThatCannotTakeTheDrainFastPathDeclinesAndAgreesWithFullWhatIf() {
    String cluster = "TestDrainParityIsolatedClique";
    setupCluster(cluster, 4, 30);
    ZKHelixDataAccessor accessor = new ZKHelixDataAccessor(cluster, _baseAccessor);
    PropertyKey.Builder keys = accessor.keyBuilder();
    ClusterConfig clusterConfig = accessor.getProperty(keys.clusterConfig());
    clusterConfig.setWagedInstanceTagIsolationEnabled(true);
    accessor.setProperty(keys.clusterConfig(), clusterConfig);
    // Clique A is instances 0 and 1, clique B instances 2 and 3. Each clique's resource has four
    // single-replica partitions of weight 30, which one instance of capacity 100 cannot hold.
    for (int i = 0; i < 4; i++) {
      InstanceConfig instanceConfig = accessor.getProperty(keys.instanceConfig(instanceName(i)));
      instanceConfig.addTag(i < 2 ? "A" : "B");
      accessor.setProperty(keys.instanceConfig(instanceName(i)), instanceConfig);
    }
    for (String tag : Arrays.asList("A", "B")) {
      IdealState idealState =
          createResourceWithWagedRebalance(cluster, "R" + tag, "MasterSlave", 4, 1, 1);
      idealState.setInstanceGroupTag(tag);
      accessor.setProperty(keys.idealStates("R" + tag), idealState);
    }
    seedBestPossible(cluster, readIdealStates(cluster));

    String target = instanceName(0);
    Outcome fastPath = evaluateRule(cluster, target, true);
    Outcome fullOnly = evaluateRule(cluster, target, false);

    Assert.assertFalse(fullOnly.feasible, "the full what-if must block this drain");
    Assert.assertTrue(fastPath.probeCalls > 0, "the fast path must have been attempted");
    Assert.assertEquals(fastPath.probeHits, 0,
        "the fast path must NOT certify a drain its clique cannot take");
    Assert.assertFalse(fastPath.feasible, "the rule must fall back to the full what-if and block");

    tearDownCluster(cluster);
  }

  // Real WAGED on a topology-aware cluster with a persisted assignment, then TOPOLOGY removed:
  // WAGED drops every node and places nothing. The EVACUATE what-if must be indeterminate, not
  // approved, while the public lenient util keeps returning empty replica maps.
  @Test
  public void testWagedThatCannotComputeIsIndeterminate() {
    String cluster = "TestDrainNoTopology";
    ClusterConfig clusterConfig = setupSeededCluster(cluster, 3);
    ZKHelixDataAccessor accessor = new ZKHelixDataAccessor(cluster, _baseAccessor);
    clusterConfig.getRecord().getSimpleFields()
        .remove(ClusterConfig.ClusterConfigProperty.TOPOLOGY.name());
    accessor.setProperty(accessor.keyBuilder().clusterConfig(), clusterConfig);

    Outcome outcome = evaluateRule(cluster, instanceName(0), true);
    Assert.assertFalse(outcome.feasible, "a what-if WAGED cannot compute must not pass");
    Assert.assertTrue(outcome.indeterminate, "it must be reported as indeterminate");

    ResourceAssignment lenient = HelixUtil.getTargetAssignmentForWagedFullAuto(ZK_ADDR,
        clusterConfig, accessor.getChildValues(accessor.keyBuilder().instanceConfigs(), true),
        accessor.getChildNames(accessor.keyBuilder().liveInstances()), readIdealStates(cluster),
        accessor.getChildValues(accessor.keyBuilder().resourceConfigs(), true)).get("R");
    Assert.assertTrue(lenient.getMappedPartitions().stream()
        .allMatch(partition -> lenient.getReplicaMap(partition).isEmpty()));
    tearDownCluster(cluster);
  }

  // Instance 0 lacks a required capacity key, so the cluster model only builds once it is drained,
  // and instance 1 went offline after the assignment was persisted. The baseline then takes WAGED's
  // emergency rebalance, whose INVALID_CLUSTER_STATUS falls back to the persisted assignment unless
  // the strict what-if propagates it. The fast path is off: it proves the drained cluster directly.
  @Test
  public void testWagedClusterModelFailureIsIndeterminate() {
    String cluster = "TestDrainNoClusterModel";
    ClusterConfig clusterConfig = setupSeededCluster(cluster, 5);
    clusterConfig.setInstanceCapacityKeys(Arrays.asList(CAPACITY_KEY, "CPU"));
    clusterConfig.setDefaultPartitionWeightMap(ImmutableMap.of(CAPACITY_KEY, 20, "CPU", 1));
    ZKHelixDataAccessor accessor = new ZKHelixDataAccessor(cluster, _baseAccessor);
    PropertyKey.Builder keys = accessor.keyBuilder();
    accessor.setProperty(keys.clusterConfig(), clusterConfig);
    for (int i = 1; i < 5; i++) {
      InstanceConfig config = accessor.getProperty(keys.instanceConfig(instanceName(i)));
      config.setInstanceCapacityMap(ImmutableMap.of(CAPACITY_KEY, CAPACITY, "CPU", 100));
      accessor.setProperty(keys.instanceConfig(instanceName(i)), config);
    }
    accessor.getBaseDataAccessor()
        .remove(keys.liveInstance(instanceName(1)).getPath(), AccessOption.PERSISTENT);

    Outcome outcome = evaluateRule(cluster, instanceName(0), false);
    Assert.assertFalse(outcome.feasible, "a what-if WAGED cannot compute must not pass");
    Assert.assertTrue(outcome.indeterminate, "it must be reported as indeterminate");
    tearDownCluster(cluster);
  }

  // Seeded random small clusters: per-instance capacity, per-resource weight, shared fault zones,
  // instance group tags and disabled partitions, some changed after the best possible was written.
  // Whenever the fast path certifies draining an instance, the full what-if must agree.
  @Test
  public void testFastPathNeverApprovesWhatFullWhatIfRejects() throws Exception {
    int certified = 0;
    for (int seed = 0; seed < 10; seed++) {
      Random random = new Random(seed);
      String cluster = "TestDrainParityRandom" + seed;
      int numInstances = 3 + random.nextInt(6);
      int numZones = 2 + random.nextInt(2);
      int numResources = 1 + random.nextInt(3);
      setupCluster(cluster, numInstances, 10);
      ZKHelixDataAccessor accessor = new ZKHelixDataAccessor(cluster, _baseAccessor);
      PropertyKey.Builder keys = accessor.keyBuilder();
      for (int r = 0; r < numResources; r++) {
        IdealState idealState = createResourceWithWagedRebalance(cluster, "R" + r, "MasterSlave",
            1 + random.nextInt(5), 1 + random.nextInt(3), 1);
        if (random.nextInt(3) == 0) {
          idealState.setInstanceGroupTag("tagged");
          accessor.setProperty(keys.idealStates("R" + r), idealState);
        }
        ResourceConfig resourceConfig = new ResourceConfig("R" + r);
        resourceConfig.setPartitionCapacityMap(Collections.singletonMap(
            ResourceConfig.DEFAULT_PARTITION_KEY,
            Collections.singletonMap(CAPACITY_KEY, 5 + random.nextInt(20))));
        accessor.setProperty(keys.resourceConfig("R" + r), resourceConfig);
      }
      for (int i = 0; i < numInstances; i++) {
        InstanceConfig config = accessor.getProperty(keys.instanceConfig(instanceName(i)));
        config.setDomain(String.format("zone=zone%d,instance=%s", i % numZones, instanceName(i)));
        config.setInstanceCapacityMap(
            Collections.singletonMap(CAPACITY_KEY, 40 + random.nextInt(60)));
        if (random.nextInt(4) > 0) {
          config.addTag("tagged");
        }
        if (random.nextInt(4) == 0) {
          config.setInstanceEnabledForPartition("R0", "R0_0", false);
        }
        accessor.setProperty(keys.instanceConfig(instanceName(i)), config);
      }
      seedBestPossible(cluster, readIdealStates(cluster));
      // An operator change after the best possible was written.
      String changed = instanceName(random.nextInt(numInstances));
      InstanceConfig config = accessor.getProperty(keys.instanceConfig(changed));
      int change = random.nextInt(4);
      if (change == 0) {
        config.setInstanceEnabledForPartition("R0", "R0_0", false);
      } else if (change == 1) {
        config.setInstanceCapacityMap(Collections.singletonMap(CAPACITY_KEY, 20));
      } else if (change == 2) {
        config.removeTag("tagged");
      }
      accessor.setProperty(keys.instanceConfig(changed), config);

      ClusterConfig clusterConfig = accessor.getProperty(keys.clusterConfig());
      List<InstanceConfig> instanceConfigs = accessor.getChildValues(keys.instanceConfigs(), true);
      List<String> liveInstances = accessor.getChildNames(keys.liveInstances());
      List<ResourceConfig> resourceConfigs = accessor.getChildValues(keys.resourceConfigs(), true);
      for (int i = 0; i < numInstances; i++) {
        if (HelixUtil.isDrainFeasibleViaPartialRebalance(
            new ZkBucketDataAccessor(_bucketZkClient), _baseAccessor, clusterConfig,
            instanceConfigs, liveInstances, readIdealStates(cluster), resourceConfigs,
            instanceName(i))) {
          certified++;
          Assert.assertTrue(evaluateRule(cluster, instanceName(i), false).feasible, "seed " + seed
              + ": the fast path certified draining " + instanceName(i)
              + " but the full what-if rejects it");
        }
      }
      tearDownCluster(cluster);
    }
    Assert.assertTrue(certified > 0, "the fast path never certified a drain");
  }

  // A WAGED resource on a custom (non-built-in) state model registered in the cluster must still get
  // a target assignment, instead of failing on a BuiltInStateModelDefinitions lookup.
  @Test
  public void testTargetAssignmentForCustomStateModel() {
    String cluster = "TestWhatIfCustomStateModel";
    String stateModel = "CustomLeaderStandby";
    setupCluster(cluster, 3, 20);
    _gSetupTool.getClusterManagementTool().addStateModelDef(cluster, stateModel,
        new StateModelDefinition.Builder(stateModel).addState("LEADER", 0).addState("STANDBY", 1)
            .addState("OFFLINE", 2).addState("DROPPED").initialState("OFFLINE")
            .addTransition("OFFLINE", "STANDBY").addTransition("STANDBY", "LEADER")
            .addTransition("LEADER", "STANDBY").addTransition("STANDBY", "OFFLINE")
            .addTransition("OFFLINE", "DROPPED").upperBound("LEADER", 1)
            .dynamicUpperBound("STANDBY", "R").build());
    createResourceWithWagedRebalance(cluster, "R", stateModel, 3, 2, 2);
    ZKHelixDataAccessor accessor = new ZKHelixDataAccessor(cluster, _baseAccessor);
    PropertyKey.Builder keys = accessor.keyBuilder();
    List<IdealState> idealStates = readIdealStates(cluster);
    // As in seedBestPossible: absent from ZK, the resource reads as newly added, so WAGED computes it.
    accessor.removeProperty(keys.idealStates("R"));

    ResourceAssignment assignment = HelixUtil.getTargetAssignmentForWagedFullAuto(
        new ZkBucketDataAccessor(_bucketZkClient), _baseAccessor,
        accessor.getProperty(keys.clusterConfig()),
        accessor.getChildValues(keys.instanceConfigs(), true),
        accessor.getChildNames(keys.liveInstances()), idealStates,
        accessor.getChildValues(keys.resourceConfigs(), true)).get("R");

    Assert.assertEquals(assignment.getMappedPartitions().size(), 3);
    for (Partition partition : assignment.getMappedPartitions()) {
      Map<String, String> replicaMap = assignment.getReplicaMap(partition);
      Assert.assertEquals(replicaMap.size(), 2, "unexpected replicas for " + partition);
      Assert.assertEquals(
          replicaMap.values().stream().filter("LEADER"::equals).count(), 1L,
          "expected exactly one LEADER for " + partition);
    }
    tearDownCluster(cluster);
  }

  // ---------------------------------------------------------------------------------------------
  // Helpers.
  // ---------------------------------------------------------------------------------------------

  private static final class Outcome {
    final boolean feasible;
    final boolean indeterminate;
    final int probeHits;
    final int probeCalls;

    Outcome(ValidationResult result, int probeHits, int probeCalls) {
      this.feasible = result.isFeasible();
      this.indeterminate = result.getViolations().stream().anyMatch(Violation::isIndeterminate);
      this.probeHits = probeHits;
      this.probeCalls = probeCalls;
    }
  }

  private static String instanceName(int i) {
    return String.format("localhost_120%02d", i);
  }

  // The instance names other than {@code excluded} (the survivors of its drain).
  private static List<String> otherInstances(int numInstances, String excluded) {
    List<String> others = new ArrayList<>();
    for (int i = 0; i < numInstances; i++) {
      String name = instanceName(i);
      if (!name.equals(excluded)) {
        others.add(name);
      }
    }
    return others;
  }

  // Write a PERSISTENT CurrentState znode for {@code resource} on {@code instance} reporting
  // {@code partition} in {@code state}, under the instance's live session. The data provider reads
  // current states under getEphemeralOwner(), which for the persistent live znodes this test writes
  // falls back to the SESSION_ID field, i.e. the same session set in setupCluster - so the staged
  // current state is picked up as the current-state fallback seed for a resource absent from the
  // persisted best possible.
  private void writeCurrentState(String cluster, String instance, String resource, String stateModel,
      String partition, String state) {
    ZKHelixDataAccessor accessor = new ZKHelixDataAccessor(cluster, _baseAccessor);
    LiveInstance liveInstance =
        accessor.getProperty(accessor.keyBuilder().liveInstance(instance));
    String session = liveInstance.getSessionId();
    CurrentState currentState = new CurrentState(resource);
    currentState.setSessionId(session);
    currentState.setStateModelDefRef(stateModel);
    currentState.setState(partition, state);
    accessor.getBaseDataAccessor().set(
        accessor.keyBuilder().currentState(instance, session, resource).getPath(),
        currentState.getRecord(), AccessOption.PERSISTENT);
  }

  // Build a WAGED-capable cluster: topology-aware with each instance in its own fault zone, a single
  // DISK capacity dimension, the opt-in instance-operation guard rail ON, best-possible persistence
  // ON, and {@code numInstances} live instances (persistent live znodes, as the perf harness does).
  private void setupCluster(String cluster, int numInstances, int partitionWeight) {
    _gSetupTool.addCluster(cluster, true);
    ZKHelixDataAccessor accessor = new ZKHelixDataAccessor(cluster, _baseAccessor);
    ClusterConfig clusterConfig =
        accessor.getProperty(accessor.keyBuilder().clusterConfig());
    clusterConfig.setTopologyAwareEnabled(true);
    clusterConfig.setTopology("/zone/instance");
    clusterConfig.setFaultZoneType("zone");
    clusterConfig.setInstanceCapacityKeys(Collections.singletonList(CAPACITY_KEY));
    clusterConfig.setDefaultInstanceCapacityMap(Collections.singletonMap(CAPACITY_KEY, CAPACITY));
    clusterConfig.setDefaultPartitionWeightMap(Collections.singletonMap(CAPACITY_KEY, partitionWeight));
    clusterConfig.setInstanceOperationRebalanceGuardrailEnabled(true);
    accessor.setProperty(accessor.keyBuilder().clusterConfig(), clusterConfig);
    enablePersistBestPossibleAssignment(_gZkClient, cluster, true);

    for (int i = 0; i < numInstances; i++) {
      String name = instanceName(i);
      _gSetupTool.addInstanceToCluster(cluster, name);
      InstanceConfig ic = accessor.getProperty(accessor.keyBuilder().instanceConfig(name));
      ic.setDomain(String.format("zone=zone%d,instance=%s", i, name));
      accessor.setProperty(accessor.keyBuilder().instanceConfig(name), ic);
      // Persistent live znode so the rule and the what-if see a stable live-instance list.
      LiveInstance li = new LiveInstance(name);
      li.setSessionId("session" + i);
      li.setHelixVersion("0.4");
      accessor.getBaseDataAccessor().set(accessor.keyBuilder().liveInstance(name).getPath(),
          li.getRecord(), AccessOption.PERSISTENT);
    }
  }

  // Resource R on {@code numInstances} instances, its assignment persisted. Returns the config.
  private ClusterConfig setupSeededCluster(String cluster, int numInstances) {
    setupCluster(cluster, numInstances, 20);
    createResourceWithWagedRebalance(cluster, "R", "MasterSlave", 3, 2, 2);
    seedBestPossible(cluster, readIdealStates(cluster));
    ZKHelixDataAccessor accessor = new ZKHelixDataAccessor(cluster, _baseAccessor);
    return accessor.getProperty(accessor.keyBuilder().clusterConfig());
  }

  private List<IdealState> readIdealStates(String cluster) {
    ZKHelixDataAccessor accessor = new ZKHelixDataAccessor(cluster, _baseAccessor);
    return accessor.getChildValues(accessor.keyBuilder().idealStates(), true);
  }

  // Remove the persistent live-instance znodes this test wrote (deleteCluster refuses while any live
  // instance remains), then delete the cluster.
  private void tearDownCluster(String cluster) {
    ZKHelixDataAccessor accessor = new ZKHelixDataAccessor(cluster, _baseAccessor);
    for (String name : accessor.getChildNames(accessor.keyBuilder().liveInstances())) {
      accessor.getBaseDataAccessor()
          .remove(accessor.keyBuilder().liveInstance(name).getPath(), AccessOption.PERSISTENT);
    }
    _gSetupTool.deleteCluster(cluster);
  }

  // Compute a WAGED assignment over the given ideal states and persist it as the baseline and best
  // possible, so the fast path's emergency model seeds from a real persisted assignment.
  private Map<String, ResourceAssignment> seedBestPossible(String cluster,
      List<IdealState> idealStatesToSeed) {
    ZKHelixDataAccessor accessor = new ZKHelixDataAccessor(cluster, _baseAccessor);
    ClusterConfig clusterConfig = accessor.getProperty(accessor.keyBuilder().clusterConfig());
    List<InstanceConfig> instanceConfigs =
        accessor.getChildValues(accessor.keyBuilder().instanceConfigs(), true);
    List<String> liveInstances = accessor.getChildNames(accessor.keyBuilder().liveInstances());
    List<ResourceConfig> resourceConfigs =
        accessor.getChildValues(accessor.keyBuilder().resourceConfigs(), true);
    // Temporarily remove the ideal states from ZK so the what-if's change detector sees them as newly
    // added and runs a real global rebalance - otherwise it no-ops and returns an empty/partial
    // assignment that would not cover every replica. Restore them immediately after computing.
    for (IdealState idealState : idealStatesToSeed) {
      accessor.removeProperty(accessor.keyBuilder().idealStates(idealState.getResourceName()));
    }
    Map<String, ResourceAssignment> seed;
    try {
      seed = HelixUtil.getTargetAssignmentForWagedFullAuto(ZK_ADDR, clusterConfig, instanceConfigs,
          liveInstances, idealStatesToSeed, resourceConfigs);
    } finally {
      for (IdealState idealState : idealStatesToSeed) {
        accessor.setProperty(accessor.keyBuilder().idealStates(idealState.getResourceName()),
            idealState);
      }
    }
    Assert.assertFalse(seed.isEmpty(), "failed to compute a seed assignment to persist");
    AssignmentMetadataStore store = new AssignmentMetadataStore(ZK_ADDR, cluster);
    try {
      store.persistBaseline(seed);
      store.persistBestPossibleAssignment(seed);
    } finally {
      store.close();
    }
    return seed;
  }

  // The instance holding the (single) replica of the first partition of {@code resource} in the seed.
  private static String hostOfSingleReplica(Map<String, ResourceAssignment> seed, String resource) {
    ResourceAssignment assignment = seed.get(resource);
    Assert.assertNotNull(assignment, "seed is missing " + resource);
    for (Partition partition : assignment.getMappedPartitions()) {
      Map<String, String> replicaMap = assignment.getReplicaMap(partition);
      if (!replicaMap.isEmpty()) {
        return replicaMap.keySet().iterator().next();
      }
    }
    throw new AssertionError("seed placed no replica for " + resource);
  }

  // Run the real rule for a proposed EVACUATE of {@code target}, wiring the provider the way the REST
  // endpoint does. When {@code useFastPath} is false the provider reports INCONCLUSIVE so the rule
  // runs the full two-what-if; when true it runs the real WAGED partial-rebalance probe and records
  // its hit/call counts. Returns the rule's feasibility verdict and the probe counters.
  private Outcome evaluateRule(String cluster, String target, boolean useFastPath) {
    int[] hitCall = new int[2]; // [0] = hits (FEASIBLE), [1] = calls
    GuardrailRule rule = new InstanceOperationRebalanceFeasibilityGuardrailRule();
    WagedAssignmentProvider provider = new WagedAssignmentProvider() {
      @Override
      public Map<String, ResourceAssignment> computeTargetAssignment(ClusterConfig cfg,
          List<InstanceConfig> ics, List<String> lis, List<IdealState> iss,
          List<ResourceConfig> rcs) {
        return HelixUtil.getTargetAssignmentForWagedFullAutoOrThrow(
            new ZkBucketDataAccessor(_bucketZkClient), _baseAccessor, cfg, ics, lis, iss, rcs);
      }

      @Override
      public DrainFeasibility attemptDrainFeasibilityShortCircuit(ClusterConfig cfg,
          List<InstanceConfig> ics, List<String> lis, List<IdealState> iss,
          List<ResourceConfig> rcs, String drainedInstanceName) {
        if (!useFastPath) {
          return DrainFeasibility.INCONCLUSIVE;
        }
        hitCall[1]++;
        boolean feasible = HelixUtil.isDrainFeasibleViaPartialRebalance(ZK_ADDR, cfg, ics, lis, iss,
            rcs, drainedInstanceName);
        if (feasible) {
          hitCall[0]++;
          return DrainFeasibility.FEASIBLE;
        }
        return DrainFeasibility.INCONCLUSIVE;
      }
    };
    HelixDataAccessor accessor = new ZKHelixDataAccessor(cluster, _baseAccessor);
    GuardrailContext context = GuardrailContext.newBuilder(cluster)
        .dataAccessor(accessor)
        .instanceName(target)
        .proposedInstanceOperation(InstanceConstants.InstanceOperation.EVACUATE)
        .wagedAssignmentProvider(provider)
        .build();
    ValidationResult result = rule.validate(context);
    Assert.assertNotNull(result);
    return new Outcome(result, hitCall[0], hitCall[1]);
  }
}
