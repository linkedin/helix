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
import java.util.List;
import java.util.Map;

import org.apache.helix.wagedsim.TestClusters;
import org.apache.helix.wagedsim.cli.WagedSimCli;
import org.apache.helix.wagedsim.cluster.ClusterFolder;
import org.apache.helix.wagedsim.cluster.ClusterState;
import org.apache.helix.wagedsim.engine.dryrun.DryRunEngine;
import org.apache.helix.wagedsim.report.RunData;
import org.apache.helix.wagedsim.scenario.Scenario;
import org.apache.helix.wagedsim.scenario.ScenarioLoader;
import org.apache.helix.wagedsim.source.SpecSource;
import org.testng.Assert;
import org.testng.annotations.Test;
import org.yaml.snakeyaml.Yaml;

public class TestScenarioRun {
  private static final String SCENARIO = String.join("\n",
      "name: kill-and-recover",
      "focusKey: CU",
      "variants:",
      "  prod: {}",
      "  ts-x4:",
      "    constraintWeights: {TopState: 12}",
      "events:",
      "  - at: 1",
      "    kill: hottest-top:2",
      "  - at: 2",
      "    advanceClock: 2h",
      "  - at: 3",
      "    revive: previously-killed",
      "exit:",
      "  until: \"missingTopState == 0 and underReplicated == 0\"",
      "  stableFor: 1",
      "  maxRounds: 8",
      "  timeout: 5m",
      "report:",
      "  perNode: [start, end]");

  @Test
  public void testRunnerWithEventsAndVariants() throws Exception {
    ClusterState state = SpecSource.build(TestClusters.spec("TEST_RUN", 12, 2, 24, true));
    Map<String, Object> map = new Yaml().load(SCENARIO);
    Scenario scenario = ScenarioLoader.fromMap(map);
    Path out = Files.createTempDirectory("waged-sim-run");
    ByteArrayOutputStream console = new ByteArrayOutputStream();
    Runner runner = new Runner(scenario, new RunOutput(out), new PrintStream(console, true, "UTF-8"),
        DryRunEngine::new, "CU");
    List<Runner.VariantResult> results = runner.run(state, 2);
    Assert.assertEquals(results.size(), 2);
    for (Runner.VariantResult result : results) {
      // Revived at round 3, so the earliest pass is round 3 (minRounds defaults to the last event).
      Assert.assertEquals(result.verdict.status, Verdict.Status.PASS, result.verdict.toString());
      Assert.assertTrue(result.verdict.round >= 3, result.verdict.toString());
    }
    String log = new String(console.toByteArray(), StandardCharsets.UTF_8);
    Assert.assertTrue(log.contains("kill node_"), log);
    Assert.assertTrue(Files.exists(out.resolve(RunOutput.ROUNDS)));
    Assert.assertTrue(Files.exists(out.resolve(RunOutput.nodesFile("prod", 0))));
  }

  @Test
  public void testCliEndToEnd() throws Exception {
    Path work = Files.createTempDirectory("waged-sim-cli");
    Path clusterDir = work.resolve("cluster");
    ClusterFolder.write(SpecSource.build(TestClusters.spec("TEST_CLI", 9, 2, 12, true)), clusterDir);
    Path scenarioFile = work.resolve("scenario.yaml");
    Files.write(scenarioFile, SCENARIO.getBytes(StandardCharsets.UTF_8));
    Path runDir = work.resolve("run");
    ByteArrayOutputStream out = new ByteArrayOutputStream();
    int code = new WagedSimCli(new PrintStream(out, true, "UTF-8"), System.err).execute(new String[]{
        "run", clusterDir.toString(), "--scenario", scenarioFile.toString(), "-o", runDir.toString()});
    String text = new String(out.toByteArray(), StandardCharsets.UTF_8);
    Assert.assertEquals(code, 0, text);
    Assert.assertTrue(Files.exists(runDir.resolve("report.md")), text);
    String html = new String(Files.readAllBytes(runDir.resolve("report.html")), StandardCharsets.UTF_8);
    Assert.assertTrue(html.contains("<svg"), "HTML report should contain charts");
    Assert.assertFalse(html.contains("<script"), "HTML report must not run scripts");
    Assert.assertFalse(html.matches("(?s).*(src|href)=\"https?://.*"), "HTML report must not load anything");
    String md = new String(Files.readAllBytes(runDir.resolve("report.md")), StandardCharsets.UTF_8);
    Assert.assertTrue(md.contains("## Verdict"));
    Assert.assertTrue(md.contains("## Rounds: prod"));
    RunData data = new RunData(runDir);
    Assert.assertEquals(data.variants().size(), 2);
    Assert.assertEquals(data.focusKey(), "CU");

    // Re-render from raw data.
    Files.delete(runDir.resolve("report.md"));
    Assert.assertEquals(new WagedSimCli(new PrintStream(out, true, "UTF-8"), System.err).execute(
        new String[]{"report", runDir.toString(), "--format", "md"}), 0);
    Assert.assertTrue(Files.exists(runDir.resolve("report.md")));
  }

  @Test
  public void testFailingConditionGivesExitCodeOne() throws Exception {
    Path work = Files.createTempDirectory("waged-sim-fail");
    Path clusterDir = work.resolve("cluster");
    ClusterFolder.write(SpecSource.build(TestClusters.spec("TEST_FAIL", 6, 1, 8, false)), clusterDir);
    ByteArrayOutputStream out = new ByteArrayOutputStream();
    int code = new WagedSimCli(new PrintStream(out, true, "UTF-8"), System.err).execute(new String[]{
        "run", clusterDir.toString(), "--scenario", "preset:replay", "--until", "skew.top.CU < 0.5",
        "--max-rounds", "2", "-o", work.resolve("run").toString(), "--no-report"});
    Assert.assertEquals(code, 1, new String(out.toByteArray(), StandardCharsets.UTF_8));
  }

  @Test
  public void testPresetsLoad() throws Exception {
    for (String preset : ScenarioLoader.PRESETS) {
      Scenario scenario = ScenarioLoader.load("preset:" + preset);
      Assert.assertFalse(scenario.variants.isEmpty(), preset);
    }
  }
}
