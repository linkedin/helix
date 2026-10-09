package org.apache.helix.wagedsim.cli;

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

import java.io.InputStream;
import java.io.PrintStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.security.MessageDigest;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Supplier;

import com.fasterxml.jackson.core.type.TypeReference;
import org.apache.helix.wagedsim.cluster.ClusterFolder;
import org.apache.helix.wagedsim.cluster.ClusterNormalizer;
import org.apache.helix.wagedsim.cluster.ClusterState;
import org.apache.helix.wagedsim.cluster.Manifest;
import org.apache.helix.wagedsim.engine.Engine;
import org.apache.helix.wagedsim.engine.dryrun.DryRunEngine;
import org.apache.helix.wagedsim.engine.local.LocalCluster;
import org.apache.helix.wagedsim.engine.local.LocalClusterEngine;
import org.apache.helix.wagedsim.report.Reports;
import org.apache.helix.wagedsim.run.Condition;
import org.apache.helix.wagedsim.run.RunOutput;
import org.apache.helix.wagedsim.run.Runner;
import org.apache.helix.wagedsim.run.Verdict;
import org.apache.helix.wagedsim.scenario.ClusterConfigOverrides;
import org.apache.helix.wagedsim.scenario.EventSpec;
import org.apache.helix.wagedsim.scenario.Scenario;
import org.apache.helix.wagedsim.scenario.ScenarioLoader;
import org.apache.helix.wagedsim.source.FolderSource;
import org.apache.helix.wagedsim.source.PensieveImporter;
import org.apache.helix.wagedsim.source.RestSource;
import org.apache.helix.wagedsim.source.SpecSource;
import org.apache.helix.wagedsim.stats.NodeStats;
import org.apache.helix.wagedsim.stats.StatsCollector;
import org.apache.helix.wagedsim.util.Durations;
import org.apache.helix.wagedsim.util.Json;

/** The {@code waged-sim} command line. */
public final class WagedSimCli {
  private static final String USAGE = String.join("\n",
      "waged-sim: set up a Helix cluster locally, run WAGED scenarios, and report.",
      "",
      "Commands:",
      "  setup  --from spec --spec FILE                       cluster from given constraints",
      "  setup  --from folder --path PATH                     cluster folder, legacy snapshot, Pensieve dump or spec",
      "  setup  --from rest --url BASE --cluster NAME         copy through helix-rest (BASE is the part before /clusters)",
      "         [--current-states] [--header 'Name: value'] [-j 8]",
      "  setup  --from pensieve --dump DIR [--cluster NAME]   import a Pensieve pull (see pensieve-script)",
      "         common: [-o DIR] [--set clusterConfigKey=value ...]",
      "  inspect DIR [--focus-key CU]                         stats of the cluster as it is, no simulation",
      "  run    DIR --scenario FILE|preset:NAME [--mode dry-run|local] [--focus-key KEY] [-o RUN_DIR] [-j N]",
      "         [--until COND] [--fail-if COND] [--max-rounds N] [--timeout 20m] [--variant NAME] [--format md,html]",
      "  report RUN_DIR [--format md,html]                    re-render reports from the raw results",
      "  up     DIR [--zk-port 2199] [--compression 3600]     start the cluster as a real local Helix cluster",
      "  presets                                              list bundled scenario presets",
      "  pensieve-script --cluster NAME --time 'YYYY-MM-DD HH:MM:SS' --backup-dir DIR [--jar FILE] [-o FILE]",
      "",
      "Exit codes for run: 0 all variants passed, 1 a variant failed, 2 an error.",
      "Data stays local: the default workspace is ~/.waged-sim (outside any repository).");

  private final PrintStream _out;
  private final PrintStream _err;

  public WagedSimCli(PrintStream out, PrintStream err) {
    _out = out;
    _err = err;
  }

  public static void main(String[] args) {
    int code = new WagedSimCli(System.out, System.err).execute(args);
    System.exit(code);
  }

  public int execute(String[] args) {
    if (args.length == 0 || args[0].equals("help") || args[0].equals("--help") || args[0].equals("-h")) {
      _out.println(USAGE);
      return args.length == 0 ? 2 : 0;
    }
    try {
      Args options = new Args(args, 1);
      switch (args[0]) {
        case "setup":
          return setup(options);
        case "inspect":
          return inspect(options);
        case "run":
          return run(options, args);
        case "report":
          return report(options);
        case "up":
          return up(options);
        case "presets":
          return presets();
        case "pensieve-script":
          return pensieveScript(options);
        default:
          _err.println("Unknown command '" + args[0] + "'.\n\n" + USAGE);
          return 2;
      }
    } catch (Throwable t) {
      _err.println("error: " + t.getMessage());
      if (System.getenv("WAGED_SIM_DEBUG") != null) {
        t.printStackTrace(_err);
      }
      return 2;
    }
  }

  static Path workspace() {
    String home = System.getenv("WAGED_SIM_HOME");
    return home != null ? Paths.get(home) : Paths.get(System.getProperty("user.home"), ".waged-sim");
  }

  private int setup(Args options) throws Exception {
    String from = options.require("from");
    ClusterState state;
    switch (from) {
      case "spec":
        state = SpecSource.load(Paths.get(options.require("spec")));
        break;
      case "folder":
        state = FolderSource.open(Paths.get(options.require("path")));
        break;
      case "rest": {
        Map<String, String> headers = new LinkedHashMap<>();
        for (String header : options.all("header")) {
          int colon = header.indexOf(':');
          if (colon < 0) {
            throw new IllegalArgumentException("--header needs 'Name: value'");
          }
          headers.put(header.substring(0, colon).trim(), header.substring(colon + 1).trim());
        }
        state = new RestSource(options.require("url"), headers,
            Integer.parseInt(options.get("parallel", "8")), _out)
            .copy(options.require("cluster"), options.flag("current-states"));
        break;
      }
      case "pensieve": {
        Long capturedAt = options.get("captured-at") == null ? null
            : Instant.parse(options.get("captured-at")).toEpochMilli();
        state = PensieveImporter.importDump(Paths.get(options.require("dump")), options.get("cluster"), capturedAt);
        break;
      }
      default:
        throw new IllegalArgumentException("--from must be spec, folder, rest or pensieve");
    }
    if (!options.all("set").isEmpty()) {
      Map<String, Object> overrides = new LinkedHashMap<>();
      for (String set : options.all("set")) {
        int eq = set.indexOf('=');
        if (eq < 0) {
          throw new IllegalArgumentException("--set needs key=value");
        }
        overrides.put(set.substring(0, eq).trim(), set.substring(eq + 1).trim());
      }
      for (String change : ClusterConfigOverrides.apply(state, overrides)) {
        state.getManifest().notes.add("setup override: " + change);
      }
    }
    ClusterNormalizer.updateCounts(state);
    Path out = Paths.get(options.get("out",
        workspace().resolve("clusters").resolve(state.getClusterName()).toString()));
    ClusterFolder.write(state, out);
    printManifest(state);
    List<String> problems = ClusterNormalizer.validate(state);
    if (!problems.isEmpty()) {
      _out.println("WARNING: WAGED cannot run on this cluster yet:");
      problems.forEach(p -> _out.println("  - " + p));
    }
    _out.println("Cluster folder: " + out.toAbsolutePath());
    _out.println("Next: waged-sim inspect " + out + "   or   waged-sim run " + out + " --scenario preset:replay");
    return problems.isEmpty() ? 0 : 1;
  }

  private void printManifest(ClusterState state) {
    Manifest manifest = state.getManifest();
    _out.println("Cluster " + state.getClusterName() + " (source " + manifest.source
        + (manifest.capturedAt != null ? ", captured " + manifest.capturedAt : "") + ")");
    _out.println("  " + manifest.counts);
    if (manifest.controllerHelixVersion != null) {
      _out.println("  controller Helix " + manifest.controllerHelixVersion + "; tool Helix " + manifest.toolHelixVersion);
    }
    for (String flag : manifest.fidelity) {
      _out.println("  fidelity: " + flag);
    }
    for (String note : manifest.notes) {
      _out.println("  note: " + note);
    }
  }

  private ClusterState open(Args options) throws Exception {
    if (options.positional.isEmpty()) {
      throw new IllegalArgumentException("Give the cluster folder (or a snapshot, dump or spec) as the first argument");
    }
    return FolderSource.open(Paths.get(options.positional.get(0)));
  }

  private int inspect(Args options) throws Exception {
    ClusterState state = open(options);
    printManifest(state);
    List<String> problems = ClusterNormalizer.validate(state);
    problems.forEach(p -> _out.println("  problem: " + p));
    StatsCollector collector = new StatsCollector(state, options.get("focus-key"));
    Map<String, Map<String, Map<String, String>>> served = state.getServedLayout();
    Map<String, NodeStats> nodes = collector.nodes(served);
    Map<String, Object> stats = collector.stats(served, null, state.getBaseline(), nodes);
    _out.println();
    _out.printf("%-10s %14s %14s %9s %9s %9s %9s %8s %8s %9s%n", "key", "capacity", "required", "util%",
        "topUtil%", "maxHost%", "maxTop%", "skewAll", "skewTop", "exposure");
    for (String key : collector.keys()) {
      _out.printf("%-10s %14s %14s %9s %9s %9s %9s %8s %8s %9s%n", key, fmt(stats.get("capacity." + key)),
          fmt(stats.get("required.all." + key)), fmt(stats.get("util.required." + key)),
          fmt(stats.get("util.top." + key)), fmt(stats.get("maxUtil.all." + key)), fmt(stats.get("maxUtil.top." + key)),
          fmt(stats.get("skew.all." + key)), fmt(stats.get("skew.top." + key)), fmt(stats.get("exposure." + key)));
    }
    _out.println();
    for (String name : new String[]{"floor.top." + collector.focusKey(), "skew.topCount", "yardstick.targetKey",
        "yardstick.misrated", "drift.baseline", "missingTopState", "underReplicated", "violations.capacity",
        "nodes.serving", "nodes.total", "peak.top." + collector.focusKey()}) {
      if (stats.get(name) != null) {
        _out.println("  " + name + " = " + fmt(stats.get(name)));
      }
    }
    if (options.get("out") != null) {
      Json.write(Paths.get(options.get("out")), stats);
      _out.println("Stats written to " + options.get("out"));
    }
    return problems.isEmpty() ? 0 : 1;
  }

  private static String fmt(Object value) {
    return org.apache.helix.wagedsim.report.ReportContent.number(value);
  }

  private int run(Args options, String[] rawArgs) throws Exception {
    ClusterState state = open(options);
    Scenario scenario = ScenarioLoader.load(options.require("scenario"));
    if (options.get("mode") != null) {
      scenario.mode = options.get("mode");
    }
    if (options.get("until") != null) {
      scenario.exit.until = Condition.parse(options.get("until"));
    }
    if (options.get("fail-if") != null) {
      scenario.exit.failIf = Condition.parse(options.get("fail-if"));
    }
    if (options.get("max-rounds") != null) {
      scenario.exit.maxRounds = Integer.parseInt(options.get("max-rounds"));
    }
    if (options.get("timeout") != null) {
      scenario.exit.timeoutMillis = Durations.parseMillis(options.get("timeout"));
    }
    if (options.get("format") != null) {
      scenario.report.formats = ClusterConfigOverrides.strings(options.get("format"));
    }
    if (!options.all("variant").isEmpty()) {
      scenario.variants.keySet().retainAll(options.all("variant"));
      if (scenario.variants.isEmpty()) {
        throw new IllegalArgumentException("No variant matches " + options.all("variant"));
      }
    }
    List<String> keys = state.getClusterConfig().getInstanceCapacityKeys();
    String focusKey = options.get("focus-key", scenario.focusKey);
    if (focusKey == null || !keys.contains(focusKey)) {
      if (focusKey != null) {
        _out.println("Focus key " + focusKey + " is not a capacity key; using " + keys.get(0));
      }
      focusKey = keys.get(0);
    }
    List<String> stats = new ArrayList<>();
    for (String stat : scenario.report.stats) {
      stats.add(stat.replace("${focusKey}", focusKey));
    }
    scenario.report.stats = stats;
    String stamp = DateTimeFormatter.ofPattern("yyyyMMdd-HHmmss").format(LocalDateTime.now());
    Path out = Paths.get(options.get("out", workspace().resolve("runs")
        .resolve(state.getClusterName() + "-" + scenario.name + "-" + stamp).toString()));
    RunOutput output = new RunOutput(out);
    Supplier<Engine> engines = "local".equals(scenario.mode) ? LocalClusterEngine::new : DryRunEngine::new;
    String started = Instant.now().toString();
    _out.println("Running '" + scenario.name + "' (" + scenario.mode + ") on " + state.getClusterName() + ", "
        + scenario.variants.size() + " variant(s), focus key " + focusKey + " -> " + out);
    Runner runner = new Runner(scenario, output, _out, engines, focusKey);
    List<Runner.VariantResult> results = runner.run(state, Integer.parseInt(options.get("parallel", "1")));

    Map<String, Object> summary = new LinkedHashMap<>();
    summary.put("tool", "waged-sim");
    summary.put("toolHelixVersion", ClusterNormalizer.toolHelixVersion());
    summary.put("command", "waged-sim " + String.join(" ", rawArgs));
    summary.put("startedAt", started);
    summary.put("finishedAt", Instant.now().toString());
    summary.put("cluster", Json.MAPPER.convertValue(state.getManifest(), new TypeReference<Map<String, Object>>() {
    }));
    summary.put("scenario", scenarioSummary(scenario));
    summary.put("focusKey", focusKey);
    summary.put("capacityKeys", keys);
    List<Map<String, Object>> variants = new ArrayList<>();
    int exit = 0;
    for (Runner.VariantResult result : results) {
      Map<String, Object> variant = new LinkedHashMap<>();
      variant.put("name", result.name);
      variant.put("status", result.verdict.status.name());
      variant.put("round", result.verdict.round);
      variant.put("reason", result.verdict.reason);
      variant.put("rounds", result.rounds);
      variant.put("elapsedMs", result.elapsedMillis);
      variant.put("elapsed", Durations.format(result.elapsedMillis));
      variant.put("settings", result.settings);
      variant.put("start", result.start);
      variant.put("end", result.end);
      variants.add(variant);
      if (result.verdict.status == Verdict.Status.ERROR) {
        exit = 2;
      } else if (result.verdict.status == Verdict.Status.FAIL && exit == 0) {
        exit = 1;
      }
    }
    summary.put("variants", variants);
    output.summary(summary);
    if (!options.flag("no-report")) {
      for (Path file : Reports.render(out, scenario.report.formats)) {
        _out.println("Report: " + file.toAbsolutePath());
      }
      if (scenario.report.formats.contains("xlsx")) {
        _out.println("xlsx: python3 <skill>/scripts/render_xlsx.py " + out.toAbsolutePath());
      }
    }
    _out.println("Raw results: " + out.toAbsolutePath());
    return exit;
  }

  private static Map<String, Object> scenarioSummary(Scenario scenario) throws Exception {
    Map<String, Object> map = new LinkedHashMap<>();
    map.put("name", scenario.name);
    map.put("description", scenario.description);
    map.put("mode", scenario.mode);
    map.put("seed", scenario.seed);
    MessageDigest digest = MessageDigest.getInstance("SHA-256");
    byte[] hash = digest.digest(Json.compact(scenario.source).getBytes(StandardCharsets.UTF_8));
    StringBuilder hex = new StringBuilder();
    for (byte b : hash) {
      hex.append(String.format("%02x", b));
    }
    map.put("sha256", hex.toString());
    Map<String, Object> exit = scenario.exit.describe();
    List<String> conditionStats = new ArrayList<>();
    if (scenario.exit.until != null) {
      conditionStats.addAll(scenario.exit.until.stats());
    }
    if (scenario.exit.failIf != null) {
      conditionStats.addAll(scenario.exit.failIf.stats());
    }
    exit.put("conditionStats", conditionStats);
    map.put("exit", exit);
    List<String> events = new ArrayList<>();
    for (EventSpec event : scenario.events) {
      events.add(event.describe());
    }
    map.put("events", events);
    Map<String, Object> report = new LinkedHashMap<>();
    report.put("formats", scenario.report.formats);
    report.put("stats", scenario.report.stats);
    map.put("report", report);
    map.put("variants", new ArrayList<>(scenario.variants.keySet()));
    map.put("source", scenario.source);
    return map;
  }

  private int report(Args options) throws Exception {
    if (options.positional.isEmpty()) {
      throw new IllegalArgumentException("Give the run folder");
    }
    Path dir = Paths.get(options.positional.get(0));
    List<String> formats = ClusterConfigOverrides.strings(options.get("format", "md,html"));
    for (Path file : Reports.render(dir, formats)) {
      _out.println("Report: " + file.toAbsolutePath());
    }
    return 0;
  }

  private int up(Args options) throws Exception {
    ClusterState state = open(options);
    int port = Integer.parseInt(options.get("zk-port", "0"));
    long compression = Long.parseLong(options.get("compression", "3600"));
    Path work = workspace().resolve("local").resolve(state.getClusterName());
    try (LocalCluster cluster = new LocalCluster(work, port)) {
      LocalCluster.rebaseTimestamps(state, compression);
      cluster.load(state, compression);
      cluster.startController(new LinkedHashMap<>());
      cluster.startParticipants(state, Long.parseLong(options.get("latency-ms", "0")));
      _out.println("Local cluster " + state.getClusterName() + " is up.");
      _out.println("  ZooKeeper: " + cluster.zkAddress());
      _out.println("  Inspect with helix-rest: run-rest-admin.sh --zkSvr " + cluster.zkAddress() + " --port 8100");
      _out.println("  Time compression: delays divided by " + compression + ". Press Ctrl-C to stop.");
      Runtime.getRuntime().addShutdownHook(new Thread(cluster::close));
      Thread.currentThread().join();
    }
    return 0;
  }

  private int presets() throws Exception {
    for (String preset : ScenarioLoader.PRESETS) {
      try (InputStream in = WagedSimCli.class.getResourceAsStream("/presets/" + preset + ".yaml")) {
        String description = "";
        if (in != null) {
          Map<String, Object> map = new org.yaml.snakeyaml.Yaml().load(in);
          description = String.valueOf(map.getOrDefault("description", ""));
        }
        _out.printf("preset:%-12s %s%n", preset, description.replace("\n", " ").trim());
      }
    }
    return 0;
  }

  private int pensieveScript(Args options) throws Exception {
    String cluster = options.require("cluster");
    String time = options.require("time");
    try {
      ZonedDateTime.of(LocalDateTime.parse(time.replace(' ', 'T')), ZoneOffset.UTC);
    } catch (RuntimeException e) {
      throw new IllegalArgumentException("--time must look like 'YYYY-MM-DD HH:MM:SS' (UTC)");
    }
    String script = PensieveImporter.pullScript(cluster, time,
        options.require("backup-dir"),
        options.get("jar", "/tmp/pensieve.jar"), options.get("java", "java"));
    if (options.get("out") != null) {
      Path file = Paths.get(options.get("out"));
      Files.write(file, script.getBytes(StandardCharsets.UTF_8));
      file.toFile().setExecutable(true);
      _out.println("Pull script: " + file.toAbsolutePath());
    } else {
      _out.print(script);
    }
    return 0;
  }
}
