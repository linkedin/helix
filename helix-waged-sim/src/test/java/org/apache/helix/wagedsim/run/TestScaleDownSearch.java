package org.apache.helix.wagedsim.run;

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

import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;

import org.apache.helix.wagedsim.TestClusters;
import org.apache.helix.wagedsim.cli.WagedSimCli;
import org.apache.helix.wagedsim.cluster.ClusterFolder;
import org.apache.helix.wagedsim.cluster.ClusterState;
import org.apache.helix.wagedsim.engine.dryrun.DryRunEngine;
import org.apache.helix.wagedsim.report.RunData;
import org.apache.helix.wagedsim.scenario.RemovalOrder;
import org.apache.helix.wagedsim.scenario.Scenario;
import org.apache.helix.wagedsim.scenario.ScenarioLoader;
import org.apache.helix.wagedsim.source.SpecSource;
import org.apache.helix.wagedsim.stats.NodeStats;
import org.apache.helix.wagedsim.stats.StatsCollector;
import org.testng.Assert;
import org.testng.annotations.Test;
import org.yaml.snakeyaml.Yaml;

/**
 * Scale-down search on a cluster whose limits are known: 12 instances of 1000 CU in 3 zones, and 280
 * partitions of 3 replicas at 10 CU each. Fault-zone placement puts one replica of every partition in
 * every zone, so each zone needs 2800 CU (3 instances) and the cluster needs 8400 CU (9 instances).
 * Removing one instance per zone is the most that fits; emptying one zone first stops at one.
 */
public class TestScaleDownSearch {
  private static Map<String, Object> spec(String name) {
    return spec(name, 12, 3);
  }

  private static Map<String, Object> spec(String name, int nodes, int zones) {
    Map<String, Object> cluster = new LinkedHashMap<>();
    cluster.put("name", name);
    cluster.put("capacityKeys", Collections.singletonList("CU"));
    cluster.put("instanceCapacity", TestClusters.map("CU", 1000));
    cluster.put("defaultPartitionWeight", TestClusters.map("CU", 10));
    Map<String, Object> resource = TestClusters.map("count", 4, "prefix", "db", "partitions", 70, "replicas", 3,
        "stateModel", "MasterSlave", "weights", TestClusters.map("CU", 10));
    Map<String, Object> spec = new LinkedHashMap<>();
    spec.put("cluster", cluster);
    spec.put("instances", Collections.singletonList(TestClusters.map("count", nodes, "prefix", "node", "zones", zones)));
    spec.put("resources", Collections.singletonList(resource));
    spec.put("seed", 5);
    return spec;
  }

  private static Scenario scenario(String yaml) {
    Map<String, Object> map = new Yaml().load(yaml);
    return ScenarioLoader.fromMap(map);
  }

  private static Map<String, Runner.VariantResult> run(ClusterState state, Scenario scenario, Path out)
      throws Exception {
    ByteArrayOutputStream console = new ByteArrayOutputStream();
    Runner runner = new Runner(scenario, new RunOutput(out), new PrintStream(console, true, "UTF-8"),
        DryRunEngine::new, "CU");
    Map<String, Runner.VariantResult> results = new LinkedHashMap<>();
    for (Runner.VariantResult result : runner.run(state, 3)) {
      results.put(result.name, result);
    }
    return results;
  }

  @Test
  public void testRemovalOrders() throws Exception {
    ClusterState state = SpecSource.build(spec("TEST_ORDER"));
    StatsCollector collector = new StatsCollector(state, "CU");
    Map<String, NodeStats> nodes = collector.nodes(state.getServedLayout());
    List<String> all = new ArrayList<>(nodes.keySet());
    Random random = new Random(1);

    List<String> balanced = RemovalOrder.order(RemovalOrder.Strategy.MZ_BALANCED, all, nodes, "CU", random);
    Assert.assertEquals(new HashSet<>(balanced), new HashSet<>(all));
    for (int k = 3; k <= all.size(); k += 3) {
      Map<String, Integer> perZone = RemovalOrder.countByZone(balanced.subList(0, k), nodes);
      Assert.assertEquals(perZone, TestClusters.map("z00", k / 3, "z01", k / 3, "z02", k / 3), "after " + k);
    }

    List<String> single = RemovalOrder.order(RemovalOrder.Strategy.MZ_SINGLE, all, nodes, "CU", random);
    Assert.assertEquals(RemovalOrder.countByZone(single.subList(0, 4), nodes).size(), 1, single.toString());

    // The tightest order empties one zone before touching the next.
    List<String> tightest = RemovalOrder.order(RemovalOrder.Strategy.MZ_TIGHTEST, all, nodes, "CU", random);
    Assert.assertEquals(new HashSet<>(tightest), new HashSet<>(all));
    for (int k = 4; k <= all.size(); k += 4) {
      Assert.assertEquals(RemovalOrder.countByZone(tightest.subList(k - 4, k), nodes).size(), 1, tightest.toString());
    }

    List<String> least = RemovalOrder.order(RemovalOrder.Strategy.LEAST_LOADED, all, nodes, "CU", random);
    for (int i = 1; i < least.size(); i++) {
      Assert.assertTrue(nodes.get(least.get(i - 1)).allUtil("CU") <= nodes.get(least.get(i)).allUtil("CU"));
    }
    List<String> most = RemovalOrder.order(RemovalOrder.Strategy.MOST_LOADED, all, nodes, "CU", random);
    for (int i = 1; i < most.size(); i++) {
      Assert.assertTrue(nodes.get(most.get(i - 1)).allUtil("CU") >= nodes.get(most.get(i)).allUtil("CU"));
    }
    List<String> zones = new ArrayList<>(Arrays.asList("10", "2", "0", "1", "z10", "z2", "z01", "z1"));
    zones.sort(RemovalOrder.ZONE_ORDER);
    Assert.assertEquals(zones, Arrays.asList("0", "1", "2", "10", "z01", "z1", "z2", "z10"));
    Assert.assertEquals(RemovalOrder.Strategy.parse("mz-balanced"), RemovalOrder.Strategy.MZ_BALANCED);
    Assert.assertEquals(RemovalOrder.Strategy.MZ_SINGLE.label(), "mz-single");
  }

  @Test
  public void testBinarySearchKeepsZonesEven() throws Exception {
    ClusterState state = SpecSource.build(spec("TEST_SCALE"));
    Scenario scenario = scenario(String.join("\n",
        "name: scale",
        "search:",
        "  removeNodes: {strategy: mz-balanced, method: binary}",
        "variants:",
        "  balanced: {}",
        "  single:",
        "    searchStrategy: mz-single",
        "  least:",
        "    searchStrategy: least-loaded"));
    Path out = Files.createTempDirectory("waged-sim-scale");
    Map<String, Runner.VariantResult> results = run(state, scenario, out);

    Runner.VariantResult balanced = results.get("balanced");
    Assert.assertEquals(balanced.verdict.status, Verdict.Status.PASS, balanced.verdict.toString());
    Assert.assertEquals(balanced.search.get("servingInstances"), 12);
    Assert.assertEquals(balanced.search.get("maxRemovable"), 3, balanced.search.toString());
    Assert.assertEquals(balanced.search.get("removedPerZone"), TestClusters.map("z00", 1, "z01", 1, "z02", 1));
    Assert.assertEquals(balanced.search.get("removedFromEveryZone"), 1);
    @SuppressWarnings("unchecked")
    Map<String, Object> balancedFailure = (Map<String, Object>) balanced.search.get("firstInfeasible");
    Assert.assertEquals(balancedFailure.get("k"), 4, balanced.search.toString());
    Assert.assertTrue(balanced.verdict.reason.contains("Can remove 3 of 12"), balanced.verdict.reason);
    Assert.assertEquals(balanced.end.get("underReplicated"), 0);

    Runner.VariantResult single = results.get("single");
    Assert.assertEquals(single.verdict.status, Verdict.Status.PASS, single.verdict.toString());
    Assert.assertEquals(single.search.get("maxRemovable"), 1, single.search.toString());
    @SuppressWarnings("unchecked")
    Map<String, Object> singleFailure = (Map<String, Object>) single.search.get("firstInfeasible");
    Assert.assertEquals(singleFailure.get("k"), 2, single.search.toString());
    // The emptied zone is out of capacity and every other zone already holds a replica.
    @SuppressWarnings("unchecked")
    Map<String, Long> blocking = (Map<String, Long>) singleFailure.get("blocking");
    Assert.assertTrue(blocking.containsKey("FAULT_ZONE"), singleFailure.toString());
    Assert.assertTrue(blocking.containsKey("NODE_CAPACITY"), singleFailure.toString());

    int least = (Integer) results.get("least").search.get("maxRemovable");
    Assert.assertTrue(least >= 1 && least <= 3, results.get("least").search.toString());

    // The start and every probe are round records, and the best probe has a per-node file.
    long records = Files.readAllLines(out.resolve(RunOutput.ROUNDS), StandardCharsets.UTF_8).stream()
        .filter(line -> line.contains("\"variant\":\"balanced\"")).count();
    Assert.assertEquals(records, balanced.rounds + 1);
    Assert.assertTrue(Files.exists(out.resolve(RunOutput.nodesFile("balanced", balanced.rounds))));
  }

  @Test
  public void testLinearSearchAndRequiredCount() throws Exception {
    ClusterState state = SpecSource.build(spec("TEST_SCALE_LINEAR"));
    Scenario scenario = scenario(String.join("\n",
        "name: scale-linear",
        "search:",
        "  removeNodes: {strategy: mz-single, method: linear, step: 1, max: 6}",
        "  requireAtLeast: 2",
        "variants:",
        "  single: {}"));
    Runner.VariantResult result = run(state, scenario, Files.createTempDirectory("waged-sim-linear")).get("single");
    Assert.assertEquals(result.search.get("maxRemovable"), 1, result.search.toString());
    // Linear: probes 0, 1 and 2 (the first failure).
    Assert.assertEquals(result.rounds, 3, result.search.toString());
    Assert.assertEquals(result.verdict.status, Verdict.Status.FAIL, result.verdict.toString());
    Assert.assertTrue(result.verdict.reason.contains("needs at least 2"), result.verdict.reason);
  }

  @Test
  public void testZoneLossTolerance() throws Exception {
    // 15 instances in 5 zones: 15000 CU for 8400 CU of replicas. Losing the largest zone on top of the
    // removal must still leave 8400 CU: with 3 removed the zones are 2,2,2,3,3 instances (9000 CU left
    // after losing a zone of 3); with 4 removed only 8000 CU would be left.
    ClusterState state = SpecSource.build(spec("TEST_ZONE_LOSS", 15, 5));
    Scenario scenario = scenario(String.join("\n",
        "name: zone-loss",
        "search:",
        "  removeNodes: {strategy: mz-balanced}",
        "variants:",
        "  plain: {}",
        "  survive:",
        "    tolerateZoneLoss: largest"));
    Map<String, Runner.VariantResult> results = run(state, scenario, Files.createTempDirectory("waged-sim-zl"));
    Runner.VariantResult survive = results.get("survive");
    Assert.assertEquals(survive.verdict.status, Verdict.Status.PASS, survive.verdict.toString());
    Assert.assertEquals(survive.search.get("maxRemovable"), 3, survive.search.toString());
    Assert.assertEquals(survive.search.get("tolerateZoneLoss"), "largest");
    @SuppressWarnings("unchecked")
    Map<String, Object> failure = (Map<String, Object>) survive.search.get("firstInfeasible");
    Assert.assertEquals(failure.get("k"), 4, survive.search.toString());
    Assert.assertNotNull(failure.get("lostZone"), failure.toString());
    Assert.assertTrue(failure.get("reason").toString().contains("losing zone"), failure.toString());
    int plain = (Integer) results.get("plain").search.get("maxRemovable");
    Assert.assertTrue(plain > 3, results.get("plain").search.toString());

    // With as many zones as replicas, losing any zone leaves replicas without a zone: WAGED places
    // fewer replicas without failing, and the search reports it before removing anything.
    Runner.VariantResult threeZones = run(SpecSource.build(spec("TEST_ZONE_LOSS_3")), scenario(String.join("\n",
        "name: zone-loss-3",
        "search: {tolerateZoneLoss: every}",
        "variants:",
        "  survive: {}")), Files.createTempDirectory("waged-sim-zl3")).get("survive");
    Assert.assertEquals(threeZones.verdict.status, Verdict.Status.FAIL, threeZones.verdict.toString());
    Assert.assertTrue(threeZones.verdict.reason.contains("cannot lose zone"), threeZones.verdict.reason);
    Assert.assertTrue(threeZones.verdict.reason.contains("2 fault zone(s) left for 3 replicas"),
        threeZones.verdict.reason);
  }

  @Test
  public void testWithoutTopologyAwarenessZonesDoNotLimit() throws Exception {
    // Without topology awareness WAGED treats each instance as its own fault zone, so emptying one
    // domain zone is as good as any other removal: capacity alone allows 3.
    ClusterState state = SpecSource.build(spec("TEST_NO_TOPOLOGY"));
    Scenario scenario = scenario(String.join("\n",
        "name: no-topology",
        "clusterConfig: {topologyAwareEnabled: false}",
        "search:",
        "  removeNodes: {strategy: mz-single}",
        "  tolerateZoneLoss: largest",
        "  feasibleIf: \"rebalanceFailures == 0 and unplacedReplicas.added == 0 and maxUtil.all.CU <= 90\"",
        "variants:",
        "  single: {}"));
    Runner.VariantResult result = run(state, scenario, Files.createTempDirectory("waged-sim-nt")).get("single");
    Assert.assertEquals(result.verdict.status, Verdict.Status.PASS, result.verdict.toString());
    Assert.assertEquals(result.search.get("tolerateZoneLoss"), "none", result.search.toString());
    Assert.assertNotNull(result.search.get("note"));
    // 90% caps it below the capacity bound; the reason must not blame fault zones.
    Assert.assertTrue((Integer) result.search.get("maxRemovable") < 3, result.search.toString());
    Assert.assertFalse(result.verdict.reason.contains("fault zone"), result.verdict.reason);
  }

  @Test
  public void testWorstCaseOrderOnUnevenZones() throws Exception {
    // Zones of 10, 8 and 6 instances of 10,000 CU; with 3 replicas every zone holds one replica of each
    // of the 768 partitions (46,080 CU). Balanced removal reaches 9 (5/3/1). The tightest order takes
    // from the 6-instance zone, which can lose only one instance.
    Map<String, Object> spec = new LinkedHashMap<>();
    spec.put("cluster", TestClusters.map("name", "TEST_UNEVEN", "capacityKeys", Collections.singletonList("CU"),
        "instanceCapacity", TestClusters.map("CU", 10000), "defaultPartitionWeight", TestClusters.map("CU", 60)));
    spec.put("instances", Arrays.asList(TestClusters.map("count", 18, "prefix", "node", "zones", 3),
        TestClusters.map("count", 4, "prefix", "more", "zones", 2), TestClusters.map("count", 2, "prefix", "extra",
            "zones", 1)));
    spec.put("resources", Collections.singletonList(TestClusters.map("count", 6, "prefix", "db", "partitions", 128,
        "replicas", 3, "stateModel", "MasterSlave", "weights", TestClusters.map("CU", 60))));
    spec.put("seed", 3);
    Scenario scenario = ScenarioLoader.load("preset:scale-down-worst-case");
    scenario.variants.keySet().retainAll(Arrays.asList("mz-balanced", "mz-tightest", "random-1"));
    Map<String, Runner.VariantResult> results =
        run(SpecSource.build(spec), scenario, Files.createTempDirectory("waged-sim-worst"));
    Assert.assertEquals(results.get("mz-balanced").search.get("maxRemovable"), 9);
    Runner.VariantResult tightest = results.get("mz-tightest");
    Assert.assertEquals(tightest.search.get("maxRemovable"), 1, tightest.search.toString());
    Assert.assertEquals(tightest.search.get("removedPerZone"), TestClusters.map("z02", 1));
    Assert.assertEquals(results.get("random-1").search.get("seed"), 1L);
    int random = (Integer) results.get("random-1").search.get("maxRemovable");
    Assert.assertTrue(random >= 1 && random <= 9, results.get("random-1").search.toString());
  }

  @Test
  public void testZoneOutageWithinTheDelayWindow() throws Exception {
    // 15 instances of 1,000 CU in 5 zones of 3; 8,400 CU of replicas. Plain removal allows 6 and
    // surviving the loss of a whole zone allows 3 (15 - k - 3 >= 9 instances).
    Scenario scenario = scenario(String.join("\n",
        "name: zone-outage",
        "search:",
        "  removeNodes: {strategy: mz-balanced}",
        "  tolerateZoneOutage: largest",
        "variants:",
        "  outage: {}"));
    // MIN_ACTIVE_REPLICAS 2: a zone outage leaves 2 of 3 replicas up, WAGED adds nothing, so the
    // outage costs no capacity and the answer stays at 6.
    Runner.VariantResult minTwo = run(SpecSource.build(outageSpec("TEST_OUTAGE_MIN2", 2, true)), scenario,
        Files.createTempDirectory("waged-sim-outage")).get("outage");
    Assert.assertEquals(minTwo.search.get("maxRemovable"), 6, minTwo.search.toString());
    Assert.assertEquals(minTwo.search.get("tolerateZoneOutage"), "largest");
    // Default MIN_ACTIVE_REPLICAS (all 3): WAGED adds a temporary replica for every partition of the down
    // zone, which needs the same room as losing the zone.
    Runner.VariantResult minAll = run(SpecSource.build(outageSpec("TEST_OUTAGE_MIN3", -1, true)), scenario,
        Files.createTempDirectory("waged-sim-outage")).get("outage");
    Assert.assertEquals(minAll.search.get("maxRemovable"), 3, minAll.search.toString());
    @SuppressWarnings("unchecked")
    Map<String, Object> failure = (Map<String, Object>) minAll.search.get("firstInfeasible");
    Assert.assertTrue(failure.get("reason").toString().contains("within the delay window"), failure.toString());
    // Only asking that every partition keeps a replica up: WAGED fails to add the temporary replicas
    // beyond 3, but 2 of 3 replicas stay up, so the answer is the plain 6.
    Scenario serving = scenario(String.join("\n",
        "name: zone-outage-serving",
        "search:",
        "  removeNodes: {strategy: mz-balanced}",
        "  tolerateZoneOutage: largest",
        "variants:",
        "  outage: {outageFeasibleIf: \"noLiveReplica.added == 0\"}"));
    Runner.VariantResult servingAll = run(SpecSource.build(outageSpec("TEST_OUTAGE_SERVING", -1, true)), serving,
        Files.createTempDirectory("waged-sim-outage")).get("outage");
    Assert.assertEquals(servingAll.search.get("maxRemovable"), 6, servingAll.search.toString());
    Assert.assertEquals(servingAll.search.get("outageCriterion"), "serving");
    // Without delayed rebalance WAGED tries to re-place the zone's replicas at once and fails beyond 3,
    // but with 2 of 3 as the minimum every partition still has 2 replicas serving: 6 by default, 3 when
    // WAGED must not fail either.
    Runner.VariantResult noDelay = run(SpecSource.build(outageSpec("TEST_OUTAGE_NODELAY", 2, false)), scenario,
        Files.createTempDirectory("waged-sim-outage")).get("outage");
    Assert.assertEquals(noDelay.search.get("maxRemovable"), 6, noDelay.search.toString());
    Scenario strict = scenario(String.join("\n",
        "name: zone-outage-strict",
        "search:",
        "  removeNodes: {strategy: mz-balanced}",
        "  tolerateZoneOutage: largest",
        "  outageFeasibleIf: \"rebalanceFailures == 0 and belowMinActive.added == 0 and noLiveReplica.added == 0\"",
        "variants:",
        "  outage: {}"));
    Runner.VariantResult noDelayStrict = run(SpecSource.build(outageSpec("TEST_OUTAGE_NODELAY_STRICT", 2, false)),
        strict, Files.createTempDirectory("waged-sim-outage")).get("outage");
    Assert.assertEquals(noDelayStrict.search.get("maxRemovable"), 3, noDelayStrict.search.toString());
    Assert.assertEquals(noDelayStrict.search.get("outageCriterion"), "custom");
  }

  private static Map<String, Object> outageSpec(String name, int minActive, boolean delay) {
    Map<String, Object> spec = spec(name, 15, 5);
    @SuppressWarnings("unchecked")
    Map<String, Object> cluster = (Map<String, Object>) spec.get("cluster");
    if (delay) {
      cluster.put("delayRebalance", TestClusters.map("enabled", true, "time", "1h"));
    }
    if (minActive > 0) {
      @SuppressWarnings("unchecked")
      Map<String, Object> resource = ((List<Map<String, Object>>) spec.get("resources")).get(0);
      resource.put("minActiveReplicas", minActive);
    }
    return spec;
  }

  @Test
  public void testEvacuationKeepsReplicasServingUntilTheyMove() throws Exception {
    // EVACUATE keeps the old replicas serving until their replacements are up (n + 1), then drops them.
    ClusterState state = SpecSource.build(spec("TEST_EVACUATE"));
    try (DryRunEngine engine = new DryRunEngine()) {
      engine.start(state, new org.apache.helix.wagedsim.engine.EngineSettings());
      long before = hosted(engine.state().getServedLayout(), "node_00");
      Assert.assertTrue(before > 0);
      org.apache.helix.wagedsim.engine.StateOps.setOperation(engine.state(), "node_00",
          org.apache.helix.constants.InstanceConstants.InstanceOperation.EVACUATE);
      engine.runRound(1);
      // Replacements are placed, and the evacuating instance still serves every replica it had.
      Assert.assertEquals(hosted(engine.state().getServedLayout(), "node_00"), before);
      long left = before;
      for (int round = 2; round <= 10 && left > 0; round++) {
        StatsCollector collector = new StatsCollector(engine.state(), "CU");
        Map<String, Object> stats = collector.stats(engine.state().getServedLayout(), null, null,
            collector.nodes(engine.state().getServedLayout()));
        // Every partition keeps all its replicas and a top state on the other instances throughout.
        Assert.assertEquals(stats.get("underReplicated"), 0, "round " + round);
        Assert.assertEquals(stats.get("missingTopState"), 0, "round " + round);
        engine.runRound(round);
        long now = hosted(engine.state().getServedLayout(), "node_00");
        Assert.assertTrue(now <= left, "round " + round);
        left = now;
      }
      // Once a replacement is up, the evacuating instance's replica is dropped; top states hand off first.
      Assert.assertEquals(left, 0L);
    }

    // The search evacuates by default and gives the same answer as deleting.
    Map<String, Runner.VariantResult> results = run(SpecSource.build(spec("TEST_EVACUATE_SEARCH")), scenario(
        String.join("\n",
            "name: evacuate-or-delete",
            "search: {removeNodes: {strategy: mz-balanced}}",
            "variants:",
            "  evacuate: {}")), Files.createTempDirectory("waged-sim-evac"));
    Map<String, Runner.VariantResult> deleted = run(SpecSource.build(spec("TEST_DELETE_SEARCH")), scenario(
        String.join("\n",
            "name: evacuate-or-delete",
            "search: {removeNodes: {strategy: mz-balanced, as: delete}}",
            "variants:",
            "  delete: {}")), Files.createTempDirectory("waged-sim-del"));
    Runner.VariantResult evacuate = results.get("evacuate");
    Assert.assertEquals(evacuate.search.get("maxRemovable"), 3, evacuate.search.toString());
    Assert.assertEquals(deleted.get("delete").search.get("maxRemovable"), 3);
    Assert.assertEquals(number(evacuate.end.get("evacuationStuck")), 0L);
    Assert.assertTrue(number(evacuate.end.get("evacuation.rounds")) >= 2, evacuate.end.toString());
  }

  @Test
  public void testEvacuationBatchesAndDownInstances() throws Exception {
    // 15 instances in 5 zones, one of them down within its delay window, and at most 4 instances unable
    // to take replicas: evacuations run 3 at a time (4 minus the down one). WAGED gives some replacements
    // to the down instance, which cannot take them, so the evacuation waits out the window; after it the
    // down instance's capacity is gone, so 5 can be removed (9 live instances for 8,400 CU).
    Map<String, Object> spec = outageSpec("TEST_EVAC_BATCHES", 2, true);
    @SuppressWarnings("unchecked")
    Map<String, Object> cluster = (Map<String, Object>) spec.get("cluster");
    cluster.put("maxOfflineInstancesAllowed", 4);
    spec.put("liveness", TestClusters.map("down", 1));
    Runner.VariantResult result = run(SpecSource.build(spec), scenario(String.join("\n",
        "name: evacuate-batches",
        "search: {removeNodes: {strategy: mz-balanced}}",
        "variants:",
        "  plan: {}")), Files.createTempDirectory("waged-sim-batches")).get("plan");
    Assert.assertEquals(result.search.get("maxRemovable"), 5, result.search.toString());
    Assert.assertEquals(number(result.end.get("evacuation.batchSize")), 3L, result.end.toString());
    Assert.assertEquals(number(result.end.get("evacuation.batches")), 2L);
    Assert.assertEquals(number(result.end.get("evacuation.waitedForDelayWindow")), 1L);
    Assert.assertEquals(number(result.end.get("evacuation.maintenance")), 0L);
  }

  private static long hosted(Map<String, Map<String, Map<String, String>>> layout, String instance) {
    long count = 0;
    for (Map<String, Map<String, String>> partitions : layout.values()) {
      for (Map<String, String> replicas : partitions.values()) {
        String state = replicas.get(instance);
        if (state != null && !"OFFLINE".equals(state) && !"DROPPED".equals(state)) {
          count++;
        }
      }
    }
    return count;
  }

  private static long number(Object value) {
    return value instanceof Number ? ((Number) value).longValue() : -1;
  }

  @Test
  public void testRemovingNonServingInstances() throws Exception {
    Map<String, Object> spec = spec("TEST_NON_SERVING");
    spec.put("liveness", TestClusters.map("disabled", 1));
    ClusterState state = SpecSource.build(spec);
    Scenario scenario = scenario(String.join("\n",
        "name: non-serving",
        "search: {nonServing: remove}",
        "variants:",
        "  balanced: {}"));
    Runner.VariantResult result = run(state, scenario, Files.createTempDirectory("waged-sim-ns")).get("balanced");
    Assert.assertEquals(result.search.get("servingInstances"), 11, result.search.toString());
    Assert.assertEquals(result.search.get("nonServingInstances"), 1);
    Assert.assertEquals(result.search.get("nonServing"), "removed");
  }

  @Test
  public void testRemovalOrderSelectorsInEvents() throws Exception {
    ClusterState state = SpecSource.build(spec("TEST_SELECTOR"));
    Scenario scenario = scenario(String.join("\n",
        "name: remove-balanced",
        "variants:",
        "  a: {}",
        "events:",
        "  - at: 1",
        "    removeNode: mz-balanced:3",
        "exit:",
        "  maxRounds: 2"));
    ByteArrayOutputStream console = new ByteArrayOutputStream();
    Runner runner = new Runner(scenario, new RunOutput(Files.createTempDirectory("waged-sim-sel")),
        new PrintStream(console, true, "UTF-8"), DryRunEngine::new, "CU");
    Runner.VariantResult result = runner.run(state, 1).get(0);
    Assert.assertEquals(result.verdict.status, Verdict.Status.PASS, result.verdict.toString());
    String log = new String(console.toByteArray(), StandardCharsets.UTF_8);
    Set<String> zones = new HashSet<>();
    java.util.regex.Matcher matcher = java.util.regex.Pattern.compile("removeNode node_(\\d+)").matcher(log);
    int removed = 0;
    while (matcher.find()) {
      removed++;
      zones.add("z0" + (Integer.parseInt(matcher.group(1)) % 3));
    }
    Assert.assertEquals(removed, 3, log);
    Assert.assertEquals(zones.size(), 3, "one instance from each zone: " + log);
    Assert.assertEquals(result.end.get("nodes.total"), 9);
  }

  @Test
  public void testScenarioValidation() {
    expectFailure("name: x\nmode: local\nsearch: {}\nvariants: {a: {}}", "dry-run mode only");
    expectFailure("name: x\nsearch: {}\nevents: [{at: 1, kill: node_0}]\nvariants: {a: {}}", "does not take events");
    expectFailure("name: x\nsearch: {removeNodes: {method: random}}\nvariants: {a: {}}", "binary or linear");
    expectFailure("name: x\nsearch: {removeNodes: {strategy: biggest}}\nvariants: {a: {}}", "Unknown removal strategy");
    Scenario scenario = scenario("name: x\nsearch: {feasibleIf: \"rebalanceFailures == 0\"}\nvariants: {a: {}}");
    Assert.assertEquals(scenario.search.describe().get("feasibleIf"), "rebalanceFailures == 0");
    Assert.assertEquals(scenario.search.strategy, RemovalOrder.Strategy.MZ_BALANCED);
  }

  private static void expectFailure(String yaml, String message) {
    try {
      scenario(yaml);
      Assert.fail("Expected a failure for " + yaml);
    } catch (IllegalArgumentException e) {
      Assert.assertTrue(e.getMessage().contains(message), e.getMessage());
    }
  }

  @Test
  public void testPresetEndToEnd() throws Exception {
    Path work = Files.createTempDirectory("waged-sim-scale-cli");
    Path clusterDir = work.resolve("cluster");
    ClusterFolder.write(SpecSource.build(spec("TEST_SCALE_CLI")), clusterDir);
    Path runDir = work.resolve("run");
    ByteArrayOutputStream out = new ByteArrayOutputStream();
    int code = new WagedSimCli(new PrintStream(out, true, "UTF-8"), System.err).execute(new String[]{
        "run", clusterDir.toString(), "--scenario", "preset:scale-down", "-o", runDir.toString()});
    String text = new String(out.toByteArray(), StandardCharsets.UTF_8);
    // Three zones for three replicas: the zone-loss variant fails before removing anything.
    Assert.assertEquals(code, 1, text);
    Assert.assertTrue(text.contains("[mz-balanced-zone-loss] FAIL"), text);
    String md = new String(Files.readAllBytes(runDir.resolve("report.md")), StandardCharsets.UTF_8);
    Assert.assertTrue(md.contains("## Scale-down search"), md);
    Assert.assertTrue(md.contains("Probes for mz-balanced"), md);
    Assert.assertTrue(md.contains("Serving instances per zone"), md);
    Assert.assertFalse(md.contains("## Rounds:"), "a search has probes, not rounds");
    String html = new String(Files.readAllBytes(runDir.resolve("report.html")), StandardCharsets.UTF_8);
    Assert.assertTrue(html.contains("instances removed"), "charts are plotted against instances removed");
    RunData data = new RunData(runDir);
    Set<Object> answers = new HashSet<>();
    for (Map<String, Object> variant : data.variants()) {
      answers.add(data.map(variant.get("search")).get("maxRemovable"));
    }
    Assert.assertTrue(answers.containsAll(Arrays.asList(3, 1, -1)), answers.toString());
  }
}
