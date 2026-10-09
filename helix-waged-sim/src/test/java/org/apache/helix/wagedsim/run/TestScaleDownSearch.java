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
