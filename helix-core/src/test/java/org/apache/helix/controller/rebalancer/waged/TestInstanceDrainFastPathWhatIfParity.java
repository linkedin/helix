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
import java.util.Collections;
import java.util.List;
import java.util.Map;

import org.apache.helix.AccessOption;
import org.apache.helix.HelixDataAccessor;
import org.apache.helix.common.ZkTestBase;
import org.apache.helix.constants.InstanceConstants;
import org.apache.helix.guardrail.GuardrailContext;
import org.apache.helix.guardrail.GuardrailRule;
import org.apache.helix.guardrail.ValidationResult;
import org.apache.helix.guardrail.WagedAssignmentProvider;
import org.apache.helix.guardrail.WagedAssignmentProvider.DrainFeasibility;
import org.apache.helix.guardrail.rules.InstanceOperationRebalanceFeasibilityGuardrailRule;
import org.apache.helix.manager.zk.ZKHelixDataAccessor;
import org.apache.helix.model.ClusterConfig;
import org.apache.helix.model.IdealState;
import org.apache.helix.model.InstanceConfig;
import org.apache.helix.model.LiveInstance;
import org.apache.helix.model.Partition;
import org.apache.helix.model.ResourceAssignment;
import org.apache.helix.model.ResourceConfig;
import org.apache.helix.util.HelixUtil;
import org.testng.Assert;
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
 * </ul>
 */
public class TestInstanceDrainFastPathWhatIfParity extends ZkTestBase {
  private static final String CAPACITY_KEY = "DISK";
  private static final int CAPACITY = 100;

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

  // ---------------------------------------------------------------------------------------------
  // Helpers.
  // ---------------------------------------------------------------------------------------------

  private static final class Outcome {
    final boolean feasible;
    final int probeHits;
    final int probeCalls;

    Outcome(boolean feasible, int probeHits, int probeCalls) {
      this.feasible = feasible;
      this.probeHits = probeHits;
      this.probeCalls = probeCalls;
    }
  }

  private static String instanceName(int i) {
    return String.format("localhost_120%02d", i);
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
        return HelixUtil.getTargetAssignmentForWagedFullAuto(ZK_ADDR, cfg, ics, lis, iss, rcs);
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
    return new Outcome(result.isFeasible(), hitCall[0], hitCall[1]);
  }
}
