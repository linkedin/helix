package org.apache.helix.wagedsim.report;

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
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.helix.wagedsim.cluster.Manifest;

import static org.apache.helix.wagedsim.report.ReportContent.number;

/** Builds the report content from the raw results of a run. */
public final class ReportBuilder {
  private static final Map<String, String> FIDELITY = new LinkedHashMap<>();

  static {
    FIDELITY.put(Manifest.NO_ASSIGNMENT_METADATA, "The source had no WAGED baseline or best possible "
        + "assignment, so the first rounds start from the served layout. This is the controller's own "
        + "fallback, but the movement anchor may differ from production.");
    FIDELITY.put(Manifest.CURRENT_STATES_FROM_EXTERNAL_VIEW, "Current states were derived from the "
        + "external view.");
    FIDELITY.put(Manifest.CURRENT_STATES_FROM_BEST_POSSIBLE, "The source had no served layout; current "
        + "states were derived from the best possible assignment.");
    FIDELITY.put(Manifest.NO_PARTICIPANT_HISTORY, "No participant history: offline times are unknown, "
        + "so instances that are offline at the start are treated as outside the delay window.");
    FIDELITY.put(Manifest.SCALED, "The cluster was scaled down; skew and lumpiness can differ from the "
        + "source.");
    FIDELITY.put(Manifest.ANONYMIZED, "Names were anonymized.");
  }

  private ReportBuilder() {
  }

  public static ReportContent build(RunData data) throws IOException {
    ReportContent report = new ReportContent();
    Map<String, Object> scenario = data.map(data.summary.get("scenario"));
    Map<String, Object> cluster = data.map(data.summary.get("cluster"));
    String focus = data.focusKey();
    List<String> stats = data.tableStats();
    report.title = "WAGED simulation: " + scenario.getOrDefault("name", "scenario");
    report.subtitle = cluster.get("cluster") + " · " + scenario.getOrDefault("mode", "dry-run") + " · "
        + data.summary.getOrDefault("startedAt", "");

    verdict(report, data);
    setup(report, data, scenario, cluster);
    start(report, data);
    scenarioSection(report, data, scenario);
    if (scenario.get("search") != null) {
      search(report, data, focus);
    } else {
      trends(report, data, stats);
      for (String variant : data.rounds.keySet()) {
        rounds(report, data, variant, stats);
      }
    }
    finalResult(report, data, stats, focus);
    if (data.variants().size() > 1) {
      comparison(report, data, stats);
    }
    reproducibility(report, data, cluster, scenario);
    return report;
  }

  private static boolean isSearch(RunData data) {
    return data.map(data.summary.get("scenario")).get("search") != null;
  }

  private static void verdict(ReportContent report, RunData data) {
    int pass = 0;
    int fail = 0;
    int error = 0;
    ReportContent.Table table = new ReportContent.Table("Variant", "Result", isSearch(data) ? "Probes" : "Round",
        "Reason", "Elapsed").kind(1, "verdict");
    for (Map<String, Object> variant : data.variants()) {
      String status = String.valueOf(variant.get("status"));
      if ("PASS".equals(status)) {
        pass++;
      } else if ("FAIL".equals(status)) {
        fail++;
      } else {
        error++;
      }
      table.row(Arrays.asList(String.valueOf(variant.get("name")), status, number(variant.get("round")),
          String.valueOf(variant.get("reason")), String.valueOf(variant.get("elapsed"))));
    }
    ReportContent.Section section = report.section("Verdict");
    section.text(data.variants().size() + " variant(s): " + pass + " passed, " + fail + " failed"
        + (error > 0 ? ", " + error + " errored" : "") + ".");
    section.table(table);
  }

  private static void setup(ReportContent report, RunData data, Map<String, Object> scenario,
      Map<String, Object> cluster) {
    List<String> bullets = new ArrayList<>();
    bullets.add("Cluster: " + cluster.get("cluster") + ", from " + cluster.get("source")
        + (cluster.get("sourceDetail") != null ? " (" + cluster.get("sourceDetail") + ")" : ""));
    if (cluster.get("capturedAt") != null) {
      bullets.add("Captured at: " + cluster.get("capturedAt"));
    }
    String controllerVersion = cluster.get("controllerHelixVersion") == null ? null
        : cluster.get("controllerHelixVersion").toString();
    String toolVersion = String.valueOf(data.summary.get("toolHelixVersion"));
    bullets.add("Helix: simulated with " + toolVersion + (controllerVersion == null ? ""
        : "; source controller runs " + controllerVersion
            + (controllerVersion.equals(toolVersion) ? "" : " (different version: results may differ)")));
    Map<String, Object> counts = data.map(cluster.get("counts"));
    if (!counts.isEmpty()) {
      List<String> parts = new ArrayList<>();
      counts.forEach((k, v) -> parts.add(k + " " + number(v)));
      bullets.add("Size: " + String.join(", ", parts));
    }
    bullets.add("Mode: " + scenario.get("mode") + "; focus key: " + data.focusKey()
        + "; capacity keys: " + data.capacityKeys());
    Map<String, Object> exit = data.map(scenario.get("exit"));
    if (scenario.get("search") != null) {
      List<String> searchParts = new ArrayList<>();
      data.map(scenario.get("search")).forEach((k, v) -> searchParts.add(k + " " + v));
      bullets.add("Scale-down search: " + String.join("; ", searchParts) + "; timeout " + exit.get("timeout"));
    } else {
      List<String> exitParts = new ArrayList<>();
      exit.forEach((k, v) -> {
        if (!"conditionStats".equals(k)) {
          exitParts.add(k + " " + v);
        }
      });
      bullets.add("Exit criteria: " + String.join("; ", exitParts));
    }
    ReportContent.Section section = report.section("Setup");
    section.bullets(bullets);
    List<String> fidelity = new ArrayList<>();
    for (Object flag : data.list(cluster.get("fidelity"))) {
      fidelity.add(FIDELITY.getOrDefault(flag.toString(), flag.toString()));
    }
    if (!fidelity.isEmpty()) {
      section.text("Fidelity notes:");
      section.bullets(fidelity);
    }
  }

  private static void start(ReportContent report, RunData data) {
    List<Map<String, Object>> variants = data.variants();
    if (variants.isEmpty()) {
      return;
    }
    Map<String, Object> start = data.map(variants.get(0).get("start"));
    ReportContent.Table table = new ReportContent.Table("Key", "Capacity (serving)", "Required (all replicas)",
        "Util % (required)", "Top-state util %", "Max host util %", "Max host top-state util %",
        "Skew (all)", "Skew (top state)", "Exposure vs focus")
        .kind(5, "util").kind(6, "util").kind(7, "skew").kind(8, "skew");
    for (String key : data.capacityKeys()) {
      table.row(Arrays.asList(key, number(start.get("capacity." + key)), number(start.get("required.all." + key)),
          number(start.get("util.required." + key)), number(start.get("util.top." + key)),
          number(start.get("maxUtil.all." + key)), number(start.get("maxUtil.top." + key)),
          number(start.get("skew.all." + key)), number(start.get("skew.top." + key)),
          number(start.get("exposure." + key))));
    }
    ReportContent.Section section = report.section("Cluster at start");
    section.text("Skew is max/mean of utilization (load ÷ capacity) over serving instances, empty ones "
        + "included; 1.0 is perfectly even. Exposure above 1.0 means that key, not the focus key, sets the "
        + "TopState target when no preferred scoring keys are set.");
    section.table(table);
    List<String> bullets = new ArrayList<>();
    if (start.get("floor.top." + data.focusKey()) != null) {
      bullets.add("Floor: no layout of these top-state replicas can be more even than "
          + number(start.get("floor.top." + data.focusKey())) + " on " + data.focusKey() + ".");
    }
    if (start.get("yardstick.targetKey") != null) {
      bullets.add("TopState target key: " + start.get("yardstick.targetKey") + "; instances below the mean on "
          + data.focusKey() + " that the TopState term rates at or above target: "
          + number(start.get("yardstick.misrated")) + ".");
    }
    if (start.get("drift.baseline") != null) {
      bullets.add("Replicas that differ from the WAGED baseline: "
          + percent(start.get("drift.baseline")) + ".");
    }
    if (!bullets.isEmpty()) {
      section.bullets(bullets);
    }
  }

  private static void scenarioSection(ReportContent report, RunData data, Map<String, Object> scenario) {
    ReportContent.Section section = report.section("Scenario");
    if (scenario.get("description") != null) {
      section.text(scenario.get("description").toString());
    }
    boolean search = scenario.get("search") != null;
    ReportContent.Table variants = search
        ? new ReportContent.Table("Variant", "Removal order", "Survive losing a zone", "Constraint weights",
            "Cluster config changes")
        : new ReportContent.Table("Variant", "Pass", "Constraint weights", "Cluster config changes");
    for (Map<String, Object> variant : data.variants()) {
      Map<String, Object> settings = data.map(variant.get("settings"));
      Map<String, Object> result = data.map(variant.get("search"));
      variants.row(search
          ? Arrays.asList(String.valueOf(variant.get("name")), String.valueOf(result.get("strategy")),
              String.valueOf(result.get("tolerateZoneLoss")), String.valueOf(settings.get("constraintWeights")),
              String.valueOf(settings.get("clusterConfig")))
          : Arrays.asList(String.valueOf(variant.get("name")),
              settings.get("pass") + " / " + settings.get("activeNodes"),
              String.valueOf(settings.get("constraintWeights")), String.valueOf(settings.get("clusterConfig"))));
    }
    section.table(variants);
    List<String> events = new ArrayList<>();
    for (Object event : data.list(scenario.get("events"))) {
      events.add(event.toString());
    }
    section.text(events.isEmpty() ? "No scheduled events." : "Scheduled events:");
    if (!events.isEmpty()) {
      section.bullets(events);
    }
  }

  /** The scale-down search: the answer per variant, the probes, and instances per zone. */
  private static void search(ReportContent report, RunData data, String focus) {
    Map<String, Object> spec = data.map(data.map(data.summary.get("scenario")).get("search"));
    ReportContent.Section section = report.section("Scale-down search");
    section.text("Each probe starts from the cluster as it is, removes the first k serving instances of the "
        + "removal order, and runs one controller pipeline: the global pass computes a new baseline over the "
        + "remaining instances and the partial pass re-homes the removed instances' replicas. The probe is "
        + "judged on WAGED's assignment (its best possible state); it is feasible when " + spec.get("feasibleIf")
        + ". \".added\" counts only problems the probe that removes nothing did not have. Search: "
        + spec.get("method") + ("binary".equals(spec.get("method"))
        ? ", starting at the most instances the remaining capacity allows; it assumes that if removing k "
        + "instances fails, removing more fails too" : "") + ".");
    section.bullets(Arrays.asList(
        "When WAGED fails, it keeps its last good assignment, so the removed instances' replicas have no "
            + "home: they count as unplaced.",
        "With \"survive losing a zone\", a removal also has to pass with a whole fault zone gone on top of "
            + "it (largest: the zone with the most capacity left; every: each zone), as if the zone stayed "
            + "down past the delay window. Within the window WAGED leaves a down zone's replicas in place, so "
            + "a short outage needs no spare capacity.",
        "WAGED places at most one replica of a partition per fault zone, and with fewer zones than "
            + "replicas it silently places fewer replicas instead of failing. Removing a whole zone can pass "
            + "every capacity check and still lose replicas; unplaced replicas catch it.",
        "Stats and charts of a probe describe WAGED's assignment, not the transient layout while replicas move."));
    ReportContent.Table answer = new ReportContent.Table("Variant", "Removal order", "Serving instances",
        "Can remove", "Fewest removed from a zone", "Fails at", "Why it fails");
    ReportContent.Chart utilization = new ReportContent.Chart();
    utilization.title = "Highest instance utilization on " + focus + " (all replicas) by instances removed";
    utilization.yLabel = "%";
    utilization.xLabel = "instances removed";
    ReportContent.Chart skew = new ReportContent.Chart();
    skew.title = "Top-state skew on " + focus + " by instances removed";
    skew.xLabel = "instances removed";
    skew.reference = 1.0;
    for (Map<String, Object> variant : data.variants()) {
      Map<String, Object> result = data.map(variant.get("search"));
      if (result.isEmpty()) {
        continue;
      }
      Map<String, Object> failure = data.map(result.get("firstInfeasible"));
      answer.row(Arrays.asList(String.valueOf(variant.get("name")), String.valueOf(result.get("strategy")),
          number(result.get("servingInstances")), number(result.get("maxRemovable")),
          number(result.get("removedFromEveryZone")), number(failure.get("k")),
          failure.isEmpty() ? "" : String.valueOf(failure.get("reason"))));
      List<double[]> util = new ArrayList<>();
      List<double[]> top = new ArrayList<>();
      for (Object item : data.list(result.get("probes"))) {
        Map<String, Object> probe = data.map(item);
        double k = ((Number) probe.get("k")).doubleValue();
        Object u = probe.get("maxUtil.all." + focus);
        Object t = probe.get("skew.top." + focus);
        if (u instanceof Number && Boolean.TRUE.equals(probe.get("feasible"))) {
          util.add(new double[]{k, ((Number) u).doubleValue()});
        }
        if (t instanceof Number && Boolean.TRUE.equals(probe.get("feasible"))) {
          top.add(new double[]{k, ((Number) t).doubleValue()});
        }
      }
      if (!util.isEmpty()) {
        utilization.series.put(String.valueOf(variant.get("name")), util);
      }
      if (!top.isEmpty()) {
        skew.series.put(String.valueOf(variant.get("name")), top);
      }
    }
    section.table(answer);
    if (!utilization.series.isEmpty()) {
      section.chart(utilization);
      section.chart(skew);
      section.text("Charts show feasible probes only.");
    }
    for (Map<String, Object> variant : data.variants()) {
      Map<String, Object> result = data.map(variant.get("search"));
      if (result.isEmpty()) {
        continue;
      }
      String name = String.valueOf(variant.get("name"));
      ReportContent.Table probes = new ReportContent.Table("Instances removed", "Result",
          "Util % (required, " + focus + ")", "Max host util % (" + focus + ")", "Skew (top state, " + focus + ")",
          "Replicas moved", "Unplaced replicas", "Reason").kind(3, "util").kind(4, "skew");
      section.text("Probes for " + name + " (" + result.get("strategy") + " order). Replicas moved compares "
          + "WAGED's assignment with the one at the start.");
      for (Object item : data.list(result.get("probes"))) {
        Map<String, Object> probe = data.map(item);
        boolean feasible = Boolean.TRUE.equals(probe.get("feasible"));
        probes.row(Arrays.asList(number(probe.get("k")), feasible ? "feasible" : "fails",
            number(probe.get("util.required." + focus)), feasible ? number(probe.get("maxUtil.all." + focus)) : "",
            feasible ? number(probe.get("skew.top." + focus)) : "", feasible ? number(probe.get("moves.replicas")) : "",
            number(probe.get("unplacedReplicas")), feasible ? "" : String.valueOf(probe.get("reason"))));
      }
      section.table(probes);
      if (result.get("note") != null) {
        section.text("Note for " + name + ": " + result.get("note") + ".");
      }
      long nonServing = result.get("nonServingInstances") instanceof Number
          ? ((Number) result.get("nonServingInstances")).longValue() : 0;
      if (nonServing > 0 && "removed".equals(result.get("nonServing"))) {
        section.text(nonServing + " disabled or offline instance(s) were removed in every probe; the answer "
            + "counts serving instances only.");
      } else if (nonServing > 0) {
        section.text(nonServing + " disabled or offline instance(s) stayed in the cluster. WAGED counts their "
            + "capacity for its baseline and keeps replicas on those within the delay window ("
            + number(result.get("replicasOnNonServing")) + " replicas at the largest feasible removal), so the "
            + "answer assumes they come back. To plan without them, set search.nonServing: remove.");
      }
      Map<String, Object> before = data.map(result.get("servingPerZoneBefore"));
      Map<String, Object> removed = data.map(result.get("removedPerZone"));
      Map<String, Object> after = data.map(result.get("servingPerZoneAfter"));
      Map<String, Object> utilBefore = data.map(result.get("zoneUtilBefore"));
      Map<String, Object> utilAfter = data.map(result.get("zoneUtilAfter"));
      if (!before.isEmpty()) {
        ReportContent.Table zones = new ReportContent.Table("Zone", "Serving instances", "Removed", "Left",
            "Zone util % before (" + focus + ")", "Zone util % after (" + focus + ")").kind(4, "util").kind(5, "util");
        for (String zone : before.keySet()) {
          zones.row(Arrays.asList(zone, number(before.get(zone)), number(removed.getOrDefault(zone, 0)),
              number(after.getOrDefault(zone, before.get(zone))), number(utilBefore.get(zone)),
              number(utilAfter.get(zone))));
        }
        section.text("Serving instances per zone at the largest feasible removal for " + name + " (zone "
            + "util is the zone's all-replica load over its serving capacity, after WAGED re-placed the replicas):");
        section.table(zones);
      }
    }
  }

  private static void trends(ReportContent report, RunData data, List<String> stats) {
    ReportContent.Section section = report.section("Trends");
    Set<Double> markers = new LinkedHashSet<>();
    for (List<Map<String, Object>> rounds : data.rounds.values()) {
      for (Map<String, Object> round : rounds) {
        if (!data.list(round.get("events")).isEmpty()) {
          markers.add(((Number) round.get("round")).doubleValue());
        }
      }
    }
    for (String stat : stats) {
      ReportContent.Chart chart = new ReportContent.Chart();
      chart.title = stat;
      chart.yLabel = stat;
      chart.markers.addAll(markers);
      if (stat.startsWith("skew.")) {
        chart.reference = 1.0;
      }
      boolean any = false;
      for (Map.Entry<String, List<Map<String, Object>>> variant : data.rounds.entrySet()) {
        List<double[]> points = new ArrayList<>();
        for (Map<String, Object> round : variant.getValue()) {
          Object value = data.map(round.get("stats")).get(stat);
          if (value instanceof Number) {
            points.add(new double[]{((Number) round.get("round")).doubleValue(), ((Number) value).doubleValue()});
          }
        }
        if (!points.isEmpty()) {
          chart.series.put(variant.getKey(), points);
          any = true;
        }
      }
      if (any) {
        section.chart(chart);
      }
    }
    section.text("Dashed vertical lines mark rounds with events.");
  }

  private static void rounds(ReportContent report, RunData data, String variant, List<String> stats) {
    List<String> header = new ArrayList<>(Arrays.asList("Round", "Events", "Passes"));
    header.addAll(stats);
    boolean until = false;
    boolean failIf = false;
    for (Map<String, Object> round : data.rounds.get(variant)) {
      until |= round.containsKey("until");
      failIf |= round.containsKey("failIf");
    }
    if (until) {
      header.add("until");
    }
    if (failIf) {
      header.add("failIf");
    }
    ReportContent.Table table = new ReportContent.Table(header.toArray(new String[0]));
    for (int i = 0; i < stats.size(); i++) {
      if (stats.get(i).startsWith("skew.")) {
        table.kind(3 + i, "skew");
      } else if (stats.get(i).startsWith("maxUtil.") || stats.get(i).startsWith("util.")) {
        table.kind(3 + i, "util");
      }
    }
    for (Map<String, Object> round : data.rounds.get(variant)) {
      List<String> cells = new ArrayList<>();
      cells.add(number(round.get("round")) + (round.get("verdict") != null ? " (" + round.get("verdict") + ")" : ""));
      List<Object> events = data.list(round.get("events"));
      List<String> eventText = new ArrayList<>();
      for (Object event : events) {
        eventText.add(event.toString());
      }
      cells.add(eventText.size() <= 3 ? String.join("; ", eventText)
          : String.join("; ", eventText.subList(0, 3)) + "; +" + (eventText.size() - 3) + " more");
      List<String> passes = new ArrayList<>();
      data.map(round.get("passes")).forEach((k, v) -> {
        if (v instanceof Number && ((Number) v).longValue() > 0 && !k.equals("emergency")
            && !k.equals("failures")) {
          passes.add(k);
        }
      });
      List<Object> failures = data.list(round.get("failures"));
      if (!failures.isEmpty()) {
        passes.add("FAILED " + failures.get(0));
      }
      cells.add(String.join("+", passes));
      Map<String, Object> roundStats = data.map(round.get("stats"));
      for (String stat : stats) {
        cells.add(number(roundStats.get(stat)));
      }
      if (until) {
        cells.add(round.containsKey("until") ? (Boolean.TRUE.equals(round.get("until")) ? "yes" : "no") : "");
      }
      if (failIf) {
        cells.add(round.containsKey("failIf") ? (Boolean.TRUE.equals(round.get("failIf")) ? "yes" : "no") : "");
      }
      table.row(cells);
    }
    report.section("Rounds: " + variant).table(table);
  }

  private static void finalResult(ReportContent report, RunData data, List<String> stats, String focus)
      throws IOException {
    ReportContent.Section section = report.section("Final result");
    Set<String> shown = new LinkedHashSet<>(stats);
    for (String key : data.capacityKeys()) {
      shown.add("skew.top." + key);
      shown.add("skew.all." + key);
      shown.add("maxUtil.top." + key);
    }
    if (isSearch(data)) {
      // Probes describe WAGED's assignment, where serving-layout counts such as missing top states
      // would include replicas parked on instances that are down within the delay window.
      shown.removeAll(Arrays.asList("missingTopState", "underReplicated"));
      shown.addAll(Arrays.asList("unplacedReplicas", "overCapacityInstances", "zoneConflicts",
          "replicasOnNonServing", "zones.serving", "moves.replicas", "moves.topState", "peak.top." + focus));
    } else {
      shown.addAll(Arrays.asList("missingTopState", "underReplicated", "violations.capacity", "moves.cumulative",
          "drift.baseline", "yardstick.targetKey", "peak.top." + focus));
    }
    for (Map<String, Object> variant : data.variants()) {
      String name = String.valueOf(variant.get("name"));
      Map<String, Object> start = data.map(variant.get("start"));
      Map<String, Object> end = data.map(variant.get("end"));
      ReportContent.Table table = new ReportContent.Table("Stat", "Start", "End", "Change");
      for (String stat : shown) {
        if (start.get(stat) == null && end.get(stat) == null) {
          continue;
        }
        String change = "";
        if (start.get(stat) instanceof Number && end.get(stat) instanceof Number) {
          double delta = ((Number) end.get(stat)).doubleValue() - ((Number) start.get(stat)).doubleValue();
          change = (delta > 0 ? "+" : "") + number(delta);
        }
        table.row(Arrays.asList(stat, number(start.get(stat)), number(end.get(stat)), change));
      }
      section.text("Variant " + name + ": " + variant.get("status")
          + (isSearch(data) ? " after " + number(variant.get("round")) + " probes. End is the largest feasible "
              + "removal. " : " at round " + number(variant.get("round")) + ". ") + variant.get("reason"));
      section.table(table);
      hottest(section, data, name, ((Number) variant.getOrDefault("rounds", 0)).intValue(), focus);
    }
  }

  private static void hottest(ReportContent.Section section, RunData data, String variant, int lastRound,
      String focus) throws IOException {
    if (focus == null) {
      return;
    }
    List<Map<String, String>> end = data.nodes(variant, lastRound);
    List<Map<String, String>> start = data.nodes(variant, 0);
    if (end == null) {
      return;
    }
    String util = "topUtilPct." + focus;
    List<Map<String, String>> serving = new ArrayList<>();
    for (Map<String, String> row : end) {
      if ("true".equals(row.get("serving"))) {
        serving.add(row);
      }
    }
    serving.sort(Comparator.comparingDouble((Map<String, String> r) -> parse(r.get(util))).reversed());
    int top = 10;
    ReportContent.Table table = new ReportContent.Table("Instance", "Zone", "Top-state util % (" + focus + ")",
        "All-replica util % (" + focus + ")", "Top-state replicas", "Largest top-state resources")
        .kind(2, "util").kind(3, "util");
    for (Map<String, String> row : serving.subList(0, Math.min(top, serving.size()))) {
      table.row(Arrays.asList(row.get("instance"), row.get("zone"), row.get(util),
          row.get("allUtilPct." + focus), row.get("topCount"), row.get("topResources")));
    }
    section.text("Hottest serving instances at the end, by top-state utilization on " + focus + ":");
    section.table(table);
    ReportContent.Chart chart = new ReportContent.Chart();
    chart.type = "bars";
    chart.title = "Top-state utilization on " + focus + " per instance (" + variant + ")";
    chart.yLabel = "%";
    List<double[]> endPoints = new ArrayList<>();
    for (int i = 0; i < serving.size(); i++) {
      endPoints.add(new double[]{i, parse(serving.get(i).get(util))});
    }
    if (start != null) {
      Map<String, Double> startUtil = new LinkedHashMap<>();
      start.forEach(r -> startUtil.put(r.get("instance"), parse(r.get(util))));
      List<double[]> startPoints = new ArrayList<>();
      for (int i = 0; i < serving.size(); i++) {
        startPoints.add(new double[]{i, startUtil.getOrDefault(serving.get(i).get("instance"), 0.0)});
      }
      chart.series.put("start", startPoints);
    }
    chart.series.put("end", endPoints);
    section.chart(chart);
  }

  private static void comparison(ReportContent report, RunData data, List<String> stats) {
    List<String> header = new ArrayList<>();
    header.add("Stat");
    for (Map<String, Object> variant : data.variants()) {
      header.add(String.valueOf(variant.get("name")));
    }
    ReportContent.Table table = new ReportContent.Table(header.toArray(new String[0]));
    boolean search = isSearch(data);
    Set<String> rows = new LinkedHashSet<>(search ? Arrays.asList("result", "probes", "can remove")
        : Arrays.asList("result", "round"));
    rows.addAll(stats);
    for (String row : rows) {
      List<String> cells = new ArrayList<>();
      cells.add(row);
      for (Map<String, Object> variant : data.variants()) {
        if (row.equals("result")) {
          cells.add(String.valueOf(variant.get("status")));
        } else if (row.equals("round") || row.equals("probes")) {
          cells.add(number(variant.get("round")));
        } else if (row.equals("can remove")) {
          cells.add(number(data.map(variant.get("search")).get("maxRemovable")));
        } else {
          cells.add(number(data.map(variant.get("end")).get(row)));
        }
      }
      table.row(cells);
    }
    report.section("Variant comparison").text(search ? "Values at each variant's largest feasible removal."
        : "Values at each variant's last round.").table(table);
  }

  private static void reproducibility(ReportContent report, RunData data, Map<String, Object> cluster,
      Map<String, Object> scenario) {
    List<String> bullets = new ArrayList<>();
    bullets.add("Command: " + data.summary.get("command"));
    bullets.add("Tool Helix version: " + data.summary.get("toolHelixVersion"));
    bullets.add("Cluster content sha256: " + cluster.get("contentSha256"));
    bullets.add("Scenario sha256: " + scenario.get("sha256") + "; seed " + scenario.get("seed"));
    bullets.add("Started " + data.summary.get("startedAt") + ", finished " + data.summary.get("finishedAt"));
    bullets.add("Raw data: run.json, rounds.jsonl and nodes_<variant>_<round>.csv in this folder. "
        + "Re-render with: waged-sim report " + data.dir);
    report.section("Reproducibility").bullets(bullets);
  }

  private static double parse(String value) {
    try {
      return value == null || value.isEmpty() ? 0 : Double.parseDouble(value);
    } catch (NumberFormatException e) {
      return 0;
    }
  }

  private static String percent(Object value) {
    return value instanceof Number ? number(100 * ((Number) value).doubleValue()) + "%" : "";
  }
}
