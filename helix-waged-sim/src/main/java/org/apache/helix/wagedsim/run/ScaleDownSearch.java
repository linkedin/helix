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

import java.io.PrintStream;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;

import org.apache.helix.constants.InstanceConstants;
import org.apache.helix.controller.rebalancer.util.DelayedRebalanceUtil;
import org.apache.helix.model.ClusterConfig;
import org.apache.helix.model.IdealState;
import org.apache.helix.model.ResourceConfig;
import org.apache.helix.model.StateModelDefinition;
import org.apache.helix.util.InstanceUtil;
import org.apache.helix.wagedsim.cluster.ClusterState;
import org.apache.helix.wagedsim.engine.EngineSettings;
import org.apache.helix.wagedsim.engine.RoundResult;
import org.apache.helix.wagedsim.engine.StateOps;
import org.apache.helix.wagedsim.engine.dryrun.DryRunEngine;
import org.apache.helix.wagedsim.scenario.RemovalOrder;
import org.apache.helix.wagedsim.scenario.Scenario;
import org.apache.helix.wagedsim.stats.NodeStats;
import org.apache.helix.wagedsim.stats.StatsCollector;
import org.apache.helix.wagedsim.util.Durations;

/**
 * Finds how many serving instances can be removed while WAGED still places every replica. Removal
 * follows a fixed order (by default keeping fault zones even). Each probe starts from the cluster as
 * it is, removes the first k instances of the order, and runs one controller pipeline: the removal
 * triggers the global pass (a new baseline over the instances left) and the emergency and partial
 * passes re-home the removed instances' replicas. A probe is feasible when the pipeline runs without a
 * rebalance failure and WAGED's assignment places every replica within capacity and keeps replicas of
 * a partition in different fault zones; optionally it must also survive losing a whole zone.
 */
class ScaleDownSearch {
  private final Scenario _scenario;
  private final Scenario.SearchSpec _spec;
  private final RunOutput _output;
  private final PrintStream _console;
  private final String _focusKey;
  /** Instances every probe removes before the first k of the order: the non-serving ones, if asked. */
  private List<String> _dropFirst = Collections.emptyList();
  /** Instances every probe brings back up and enables first: the non-serving ones, if asked. */
  private List<String> _reviveFirst = Collections.emptyList();
  /** largest, every or none. */
  private String _zoneLoss = "none";
  /** largest, every or none. */
  private String _zoneOutage = "none";
  private Condition _outageFeasibleIf;
  /** Whether WAGED places by fault zone; without topology awareness every instance is its own zone. */
  private boolean _faultZones = true;
  /** Resizes a capacity key to the instances left with every probe; null to keep the capacity as is. */
  private Scenario.CapacityScaling _scaling;
  /** For the resize: the total requirement of the key, and the instances holding capacity before removal. */
  private long _scaledRequired;
  private int _scaledInstances;

  /** One evaluated removal count. */
  static final class Probe {
    int index;
    int k;
    boolean feasible;
    List<String> failures = new ArrayList<>();
    Map<String, Long> blocking = new TreeMap<>();
    Map<String, Object> stats = new LinkedHashMap<>();
    Map<String, NodeStats> nodes;
    RoundResult round;
    /** Fault zones with serving instances after the removal, and the most replicas any resource wants. */
    int zonesLeft;
    int replicas;
    /** For a zone-loss check: the zone lost on top of the removal, and how that went. */
    String lostZone;
    int lostInstances;
    Probe zoneLoss;
    /** Zone-loss checks run: zone to "ok" or why it fails. */
    Map<String, String> zoneLossResults = new TreeMap<>(RemovalOrder.ZONE_ORDER);
    /** The last zone-outage check run, for its stats. */
    Probe lastOutageCheck;
    /** The failed zone-outage check, if any, and every zone-outage check run. */
    Probe zoneOutage;
    Map<String, String> zoneOutageResults = new TreeMap<>(RemovalOrder.ZONE_ORDER);

    String reason() {
      if (zoneLoss != null && !zoneLoss.feasible) {
        return "places every replica, but not after also losing zone " + zoneLoss.lostZone + " ("
            + zoneLoss.lostInstances + " instances): " + zoneLoss.reason();
      }
      if (zoneOutage != null && !zoneOutage.feasible) {
        return "places every replica, but does not ride out an outage of zone " + zoneOutage.lostZone + " ("
            + zoneOutage.lostInstances + " instances) within the delay window: " + zoneOutage.reason();
      }
      if (feasible) {
        return "places every replica";
      }
      if (!failures.isEmpty()) {
        String first = failures.get(0);
        int details = first.indexOf("; Fail reasons:");
        if (details > 0) {
          first = first.substring(0, details);
        }
        return (first.length() > 300 ? first.substring(0, 300) + "..." : first)
            + (blocking.isEmpty() ? "" : "; blocked by " + String.join(" + ", blocking.keySet()));
      }
      if (number(stats.get("evacuationStuck")) > 0) {
        return "WAGED placed the replicas elsewhere, but the evacuation did not finish: "
            + stats.get("evacuationStuck") + " replicas were still on the evacuating instances after "
            + stats.get("evacuation.rounds") + " controller rounds"
            + (number(stats.get("evacuation.waitedForDelayWindow")) > 0 ? ", even after waiting out the delay "
                + "window of the down instances WAGED had given replacements to" : "");
      }
      if (zonesLeft > 0 && zonesLeft < replicas && number(stats.get("unplacedReplicas.added")) > 0) {
        return "only " + zonesLeft + " fault zone(s) left for " + replicas + " replicas: WAGED places at most one "
            + "replica of a partition per zone, so it places fewer replicas without a rebalance failure; "
            + "unplaced replicas +" + stats.get("unplacedReplicas.added");
      }
      List<String> problems = new ArrayList<>();
      String[][] labels = {{"unplacedReplicas", "unplaced replicas"},
          {"overCapacityInstances", "instances over capacity"},
          {"zoneConflicts", "partitions with two replicas in a zone"},
          {"belowMinActive", "partitions below their minimum active replicas on instances that are up"},
          {"noLiveReplica", "partitions with no replica on an instance that is up"}};
      for (String[] label : labels) {
        long added = number(stats.get(label[0] + ".added"));
        if (added > 0) {
          problems.add(label[1] + " +" + added);
        }
      }
      return "no rebalance failure, but the feasibility condition does not hold"
          + (problems.isEmpty() ? "" : ": " + String.join(", ", problems));
    }
  }

  ScaleDownSearch(Scenario scenario, RunOutput output, PrintStream console, String focusKey) {
    _scenario = scenario;
    _spec = scenario.search;
    _output = output;
    _console = console;
    _focusKey = focusKey;
  }

  void run(ClusterState state, Scenario.Variant variant, EngineSettings settings, Runner.VariantResult result,
      long start) throws Exception {
    RemovalOrder.Strategy strategy = variant.searchStrategy != null ? variant.searchStrategy : _spec.strategy;
    _zoneLoss = variant.zoneLoss != null ? variant.zoneLoss : _spec.zoneLoss;
    _zoneOutage = variant.zoneOutage != null ? variant.zoneOutage : _spec.zoneOutage;
    _outageFeasibleIf = variant.outageFeasibleIf != null ? variant.outageFeasibleIf : _spec.outageFeasibleIf;
    _faultZones = state.getClusterConfig().isTopologyAwareEnabled();
    Map<String, Map<String, Map<String, String>>> served = state.getServedLayout();
    StatsCollector collector = new StatsCollector(state, _focusKey);
    Map<String, NodeStats> nodes = withFaultZones(collector.nodes(served));
    Map<String, Object> startStats = collector.stats(served, null, state.getBaseline(), nodes);
    record(variant.name, 0, Collections.emptyList(), new RoundResult(), startStats, null);

    List<String> candidates = new ArrayList<>();
    List<String> nonServing = new ArrayList<>();
    nodes.values().forEach(n -> (n.serving ? candidates : nonServing).add(n.instance));
    String nonServingMode = variant.nonServing != null ? variant.nonServing : _spec.nonServing;
    if ("remove".equals(nonServingMode)) {
      _dropFirst = nonServing;
    } else if ("revive".equals(nonServingMode)) {
      _reviveFirst = nonServing;
    } else if ("window".equals(nonServingMode)) {
      // WAGED keeps replicas on the instances it expects back within the delay window and has moved them
      // off the ones down for longer: bring the first back up and remove the others.
      Set<String> expectedBack = assignedInstances(state);
      _reviveFirst = new ArrayList<>();
      _dropFirst = new ArrayList<>();
      nonServing.forEach(instance -> (expectedBack.contains(instance) ? _reviveFirst : _dropFirst).add(instance));
    }
    _scaling = variant.scaleCapacity;
    if (_scaling != null) {
      _scaledRequired = number(startStats.get("required.all." + _scaling.key));
      _scaledInstances = candidates.size() + _reviveFirst.size();
      _console.println("[" + variant.name + "] every probe resizes " + _scaling + ": " + _scaledRequired
          + " required, " + _scaledInstances + " instances before removal");
    }
    String zoneLossNote = null;
    boolean zoneChecks = !"none".equals(_zoneLoss) || !"none".equals(_zoneOutage);
    if (zoneChecks && nodes.values().stream().noneMatch(n -> n.zone != null)) {
      zoneLossNote = "the cluster has no fault zones (or is not topology aware), so zone loss and outage were "
          + "not checked";
      _console.println("[" + variant.name + "] " + zoneLossNote);
      _zoneLoss = "none";
      _zoneOutage = "none";
    }
    long seed = variant.seed != null ? variant.seed : _scenario.seed;
    // Instances brought back up hold their share of their zone: order by WAGED's assignment, which still
    // has their replicas, and count their capacity.
    Map<String, NodeStats> orderNodes = !_reviveFirst.isEmpty() && state.getBestPossible() != null
        ? withFaultZones(collector.nodes(state.getBestPossible())) : nodes;
    List<String> order = RemovalOrder.order(strategy, candidates, orderNodes, _focusKey, new Random(seed),
        _reviveFirst);
    int maxK = _spec.max != null ? Math.min(_spec.max, order.size()) : order.size();
    _console.println("[" + variant.name + "] scale-down search: " + order.size() + " serving instances, order "
        + strategy.label() + ", up to " + maxK + " removed");

    List<Probe> probes = new ArrayList<>();
    Probe best = null;
    Probe firstFailure = null;
    // Moves are counted against WAGED's assignment at the start, when the source has one.
    Map<String, Map<String, Map<String, String>>> startAssignment =
        state.getBestPossible() != null ? state.getBestPossible() : served;
    Probe zero = probe(state, settings, order, 0, variant.name, probes, startAssignment, null);
    // Start and end both describe WAGED's assignment: with nothing removed, and with the most removed.
    result.start = zero.stats;
    _output.nodes(variant.name, 0, zero.nodes.values(), collector.keys());
    List<String> boundKeys = new ArrayList<>(collector.keys());
    if (_scaling != null) {
      // The resized key grows with every removal, so only the other keys bound the search.
      boundKeys.remove(_scaling.key);
    }
    int bound = capacityBound(order, nodes, startStats, boundKeys, new TreeSet<>(_dropFirst));
    if (zero.feasible) {
      best = zero;
      if ("linear".equals(_spec.method)) {
        for (int k = _spec.step; k <= maxK && !overTime(start); k += _spec.step) {
          Probe probe = probe(state, settings, order, k, variant.name, probes, startAssignment, zero);
          if (!probe.feasible) {
            firstFailure = probe;
            break;
          }
          best = probe;
        }
      } else if (maxK > 0) {
        // Invariant: low is feasible; high is infeasible, or maxK + 1 while no failure has been seen.
        int low = 0;
        int high = maxK + 1;
        // First guess: the most the remaining capacity allows, then one more to confirm the limit.
        for (int guess : new int[]{Math.min(bound, maxK), Math.min(bound, maxK) + 1}) {
          if (guess <= low || guess >= high || overTime(start)) {
            continue;
          }
          Probe probe = probe(state, settings, order, guess, variant.name, probes, startAssignment, zero);
          if (probe.feasible) {
            low = guess;
            best = probe;
          } else {
            high = guess;
            firstFailure = probe;
          }
        }
        if (high == maxK + 1 && low < maxK && !overTime(start)) {
          Probe probe = probe(state, settings, order, maxK, variant.name, probes, startAssignment, zero);
          if (probe.feasible) {
            low = maxK;
            best = probe;
          } else {
            high = maxK;
            firstFailure = probe;
          }
        }
        while (high - low > 1 && high <= maxK && !overTime(start)) {
          int mid = (low + high) >>> 1;
          Probe probe = probe(state, settings, order, mid, variant.name, probes, startAssignment, zero);
          if (probe.feasible) {
            low = mid;
            best = probe;
          } else {
            high = mid;
            firstFailure = probe;
          }
        }
      }
    } else {
      firstFailure = zero;
    }

    Map<String, Object> summary = new LinkedHashMap<>();
    summary.put("strategy", strategy.label());
    summary.put("method", _spec.method);
    summary.put("tolerateZoneLoss", _zoneLoss);
    summary.put("tolerateZoneOutage", _zoneOutage);
    if (!"none".equals(_zoneOutage)) {
      summary.put("outageFeasibleIf", _outageFeasibleIf.toString());
      summary.put("outageCriterion", Scenario.SearchSpec.outageCriterion(_outageFeasibleIf));
    }
    if (zoneLossNote != null) {
      summary.put("note", zoneLossNote);
    }
    summary.put("servingInstances", order.size());
    summary.put("searchedUpTo", maxK);
    summary.put("capacityBound", bound);
    summary.put("nonServingInstances", nonServing.size());
    summary.put("nonServing", "window".equals(nonServingMode)
        ? "window: " + _reviveFirst.size() + " revived, " + _dropFirst.size() + " removed"
        : "remove".equals(nonServingMode) ? "removed" : "revive".equals(nonServingMode) ? "revived" : "kept");
    summary.put("nonServingRevived", new ArrayList<>(_reviveFirst));
    summary.put("nonServingRemoved", new ArrayList<>(_dropFirst));
    if (_scaling != null) {
      summary.put("scaleCapacity", _scaling.toString());
      summary.put("scaleCapacity.required", _scaledRequired);
      summary.put("scaleCapacity.instances", _scaledInstances);
    }
    if (strategy == RemovalOrder.Strategy.RANDOM) {
      summary.put("seed", seed);
    }
    Map<String, Integer> before = RemovalOrder.countByZone(order, nodes);
    summary.put("servingPerZoneBefore", before);
    summary.put("zoneUtilBefore", zoneUtil(zero.nodes));
    List<Map<String, Object>> probeRows = new ArrayList<>();
    List<Probe> sorted = new ArrayList<>(probes);
    sorted.sort((a, b) -> Integer.compare(a.k, b.k));
    for (Probe probe : sorted) {
      Map<String, Object> row = new LinkedHashMap<>();
      row.put("k", probe.k);
      row.put("feasible", probe.feasible);
      row.put("reason", probe.reason());
      row.put("blocking", probe.zoneLoss != null ? probe.zoneLoss.blocking : probe.blocking);
      if (!probe.zoneLossResults.isEmpty()) {
        row.put("zoneLoss", probe.zoneLossResults);
      }
      if (!probe.zoneOutageResults.isEmpty()) {
        row.put("zoneOutage", probe.zoneOutageResults);
        Probe check = probe.zoneOutage != null ? probe.zoneOutage : probe.lastOutageCheck;
        if (check != null) {
          row.put("outage.maxUtil.top." + _focusKey, check.stats.get("outage.maxUtil.top." + _focusKey));
          row.put("outage.passes.overwrite", check.stats.get("passes.overwrite"));
        }
      }
      for (String stat : new String[]{"util.required." + _focusKey, "maxUtil.all." + _focusKey,
          "skew.all." + _focusKey, "skew.top." + _focusKey, "unplacedReplicas", "overCapacityInstances",
          "zoneConflicts", "moves.replicas", "evacuation.rounds", "evacuation.batches", "evacuation.batchSize",
          "evacuation.waitedForDelayWindow", "evacuation.waitingOnDownInstances", "evacuationStuck", "rounds"}) {
        row.put(stat, probe.stats.get(stat));
      }
      if (_scaling != null) {
        row.put("capacity.resized." + _scaling.key, probe.stats.get("capacity.resized." + _scaling.key));
      }
      probeRows.add(row);
    }
    summary.put("probes", probeRows);
    String reason;
    Verdict verdict;
    if (best == null) {
      summary.put("maxRemovable", -1);
      reason = zero.zoneLoss != null
          ? "Even before any instance is removed, the cluster cannot lose zone " + zero.zoneLoss.lostZone + " ("
              + zero.zoneLoss.lostInstances + " instances): " + zero.zoneLoss.reason()
          : zero.zoneOutage != null
          ? "Even before any instance is removed, the cluster does not ride out an outage of zone "
              + zero.zoneOutage.lostZone + " (" + zero.zoneOutage.lostInstances + " instances) within the delay "
              + "window: " + zero.zoneOutage.reason()
          : "WAGED cannot place every replica even before any instance is removed: " + zero.reason();
      verdict = Verdict.fail(probes.size(), reason);
    } else {
      List<String> removed = new ArrayList<>(order.subList(0, best.k));
      Map<String, Integer> removedPerZone = RemovalOrder.countByZone(removed, nodes);
      Map<String, Integer> remaining = new TreeMap<>(RemovalOrder.ZONE_ORDER);
      remaining.putAll(before);
      removedPerZone.forEach((zone, count) -> remaining.merge(zone, -count, Integer::sum));
      summary.put("maxRemovable", best.k);
      summary.put("removed", removed);
      summary.put("removedPerZone", removedPerZone);
      summary.put("servingPerZoneAfter", remaining);
      summary.put("zoneUtilAfter", zoneUtil(best.nodes));
      summary.put("replicasOnNonServing", best.stats.get("replicasOnNonServing"));
      int minPerZone = before.keySet().stream().mapToInt(z -> removedPerZone.getOrDefault(z, 0)).min().orElse(0);
      summary.put("removedFromEveryZone", minPerZone);
      if (firstFailure != null) {
        Map<String, Object> failure = new LinkedHashMap<>();
        failure.put("k", firstFailure.k);
        failure.put("reason", firstFailure.reason());
        failure.put("blocking", firstFailure.zoneLoss != null ? firstFailure.zoneLoss.blocking : firstFailure.blocking);
        if (firstFailure.zoneLoss != null) {
          failure.put("lostZone", firstFailure.zoneLoss.lostZone);
        }
        if (firstFailure.zoneOutage != null) {
          failure.put("outageZone", firstFailure.zoneOutage.lostZone);
          failure.put("blocking", firstFailure.zoneOutage.blocking);
        }
        summary.put("firstInfeasible", failure);
      }
      reason = "Can remove " + best.k + " of " + order.size() + " serving instances (" + strategy.label()
          + (best.k > 0 ? "; per zone " + removedPerZone : "") + ")"
          + (firstFailure != null ? "; removing " + firstFailure.k + " fails: " + firstFailure.reason()
              : maxK < order.size() ? "; search stopped at " + maxK : "");
      if (_spec.requireAtLeast != null && best.k < _spec.requireAtLeast) {
        verdict = Verdict.fail(probes.size(), reason + " (needs at least " + _spec.requireAtLeast + ")");
      } else {
        verdict = Verdict.pass(probes.size(), reason);
      }
      result.end = best.stats;
      _output.nodes(variant.name, probes.size(), best.nodes.values(), collector.keys());
    }
    if (overTime(start) && verdict.status == Verdict.Status.PASS) {
      verdict = Verdict.fail(probes.size(), "timeout " + Durations.format(_scenario.exit.timeoutMillis)
          + " before the search finished; best so far: " + reason);
    }
    verdict.probes = true;
    result.search = summary;
    result.rounds = probes.size();
    result.verdict = verdict;
  }

  private boolean overTime(long start) {
    return System.currentTimeMillis() - start > _scenario.exit.timeoutMillis;
  }

  /** @return a copy of the state whose default instance capacity has the key set to the capacity */
  static ClusterState withDefaultCapacity(ClusterState base, String key, long capacity) {
    ClusterState copy = base.copy();
    ClusterConfig config = copy.getClusterConfig();
    Map<String, Integer> defaults = new LinkedHashMap<>(config.getDefaultInstanceCapacityMap());
    defaults.put(key, (int) Math.min(Integer.MAX_VALUE, capacity));
    config.setDefaultInstanceCapacityMap(defaults);
    copy.setClusterConfig(config);
    return copy;
  }

  private Probe probe(ClusterState base, EngineSettings settings, List<String> order, int k, String variant,
      List<Probe> probes, Map<String, Map<String, Map<String, String>>> startLayout, Probe reference)
      throws Exception {
    List<String> removed = order.subList(0, k);
    Long resized = null;
    if (_scaling != null) {
      resized = _scaling.capacity(_scaledRequired, _scaledInstances - k);
      base = withDefaultCapacity(base, _scaling.key, resized);
    }
    Probe probe = evaluate(base, settings, removed, startLayout, reference);
    probe.index = probes.size() + 1;
    probe.k = k;
    probe.stats.put("search.k", k);
    if (resized != null) {
      probe.stats.put("capacity.resized." + _scaling.key, resized);
    }
    if (probe.feasible && !"none".equals(_zoneLoss)) {
      for (Map.Entry<String, List<String>> zone : zonesToLose(probe.nodes).entrySet()) {
        Probe check = evaluate(base, settings, removed, ZoneEvent.LOSS, zone.getValue(), startLayout,
            reference != null ? reference : probe);
        check.lostZone = zone.getKey();
        check.lostInstances = zone.getValue().size();
        probe.zoneLossResults.put(zone.getKey(), check.feasible ? "ok" : check.reason());
        if (!check.feasible) {
          probe.zoneLoss = check;
          probe.feasible = false;
          break;
        }
      }
      probe.stats.put("zoneLoss.checked", probe.zoneLossResults.size());
    }
    if (probe.feasible && !"none".equals(_zoneOutage)) {
      for (Map.Entry<String, List<String>> zone : zonesToLose(probe.nodes).entrySet()) {
        Probe check = evaluate(base, settings, removed, ZoneEvent.OUTAGE, zone.getValue(), startLayout,
            reference != null ? reference : probe);
        check.lostZone = zone.getKey();
        check.lostInstances = zone.getValue().size();
        probe.lastOutageCheck = check;
        probe.zoneOutageResults.put(zone.getKey(), check.feasible ? "ok" : check.reason());
        if (!check.feasible) {
          probe.zoneOutage = check;
          probe.feasible = false;
          break;
        }
      }
      probe.stats.put("zoneOutage.checked", probe.zoneOutageResults.size());
      Probe check = probe.zoneOutage != null ? probe.zoneOutage : probe.lastOutageCheck;
      if (check != null) {
        // During the outage: top-state load on the instances that are up, and whether WAGED added replicas.
        probe.stats.put("outage.maxUtil.top." + _focusKey, check.stats.get("outage.maxUtil.top." + _focusKey));
        probe.stats.put("outage.passes.overwrite", check.stats.get("passes.overwrite"));
        probe.stats.put("outage.maintenance", check.stats.get("outage.maintenance"));
      }
    }
    probe.stats.put("search.feasible", probe.feasible ? 1 : 0);
    List<String> events = new ArrayList<>();
    events.add("remove " + k + " instance(s)" + (k == 0 ? "" : ": " + summarize(removed)));
    probe.zoneLossResults.forEach((zone, result) -> events.add("then lose zone " + zone + ": " + result));
    probe.zoneOutageResults.forEach((zone, result) ->
        events.add("then zone " + zone + " down within the delay window: " + result));
    record(variant, probe.index, events, probe.round, probe.stats, probe);
    probes.add(probe);
    _console.println("[" + variant + "] probe k=" + k + ": " + (probe.feasible ? "feasible" : "infeasible")
        + " util.required." + _focusKey + "=" + probe.stats.get("util.required." + _focusKey) + "% skew.top."
        + _focusKey + "=" + probe.stats.get("skew.top." + _focusKey) + (probe.feasible ? "" : " | " + probe.reason()));
    return probe;
  }

  /**
   * @return the zones a zone-loss check takes out, with their serving instances: every zone, or the
   *         one with the most capacity on the focus key (the biggest loss)
   */
  private Map<String, List<String>> zonesToLose(Map<String, NodeStats> nodes) {
    Map<String, List<String>> zones = new TreeMap<>(RemovalOrder.ZONE_ORDER);
    Map<String, Long> capacity = new TreeMap<>(RemovalOrder.ZONE_ORDER);
    for (NodeStats node : nodes.values()) {
      if (node.serving) {
        String zone = node.zone == null ? "(none)" : node.zone;
        zones.computeIfAbsent(zone, z -> new ArrayList<>()).add(node.instance);
        capacity.merge(zone, (long) node.capacity.getOrDefault(_focusKey, 0), Long::sum);
      }
    }
    if ("every".equals(_zoneLoss) || zones.isEmpty()) {
      return zones;
    }
    String largest = Collections.max(zones.keySet(), Comparator.comparingLong((String z) -> capacity.get(z))
        .thenComparingInt(z -> zones.get(z).size()).thenComparing(Comparator.reverseOrder()));
    return Collections.singletonMap(largest, zones.get(largest));
  }

  /** What happens to a zone after the scale-down, for the zone checks. */
  enum ZoneEvent {
    /** Nothing: the scale-down alone. */
    NONE,
    /** The zone's instances go offline and stay within the delay window. */
    OUTAGE,
    /** The zone's instances are gone, as after the delay window or for good. */
    LOSS
  }

  /** The scale-down alone. */
  private Probe evaluate(ClusterState base, EngineSettings settings, List<String> removed,
      Map<String, Map<String, Map<String, String>>> startLayout, Probe reference) throws Exception {
    return evaluate(base, settings, removed, ZoneEvent.NONE, Collections.emptyList(), startLayout, reference);
  }

  /**
   * Runs the scale-down, then the zone event, and judges WAGED's assignment.
   * <ul>
   * <li>Scale-down by evacuation (the default): the instances get the EVACUATE operation and stay up.
   * WAGED places their replicas elsewhere, and the controller keeps the old replicas serving until the
   * new ones are up (n + 1), then drops them. Rounds run until the instances hold nothing (or stop
   * making progress); then the instances are removed and one more round runs.</li>
   * <li>Scale-down by deletion: the instances are removed at once and one round runs.</li>
   * <li>Zone outage: the zone's instances go offline (live instance and current states gone, offline
   * time now, so within the delay window) and one round runs. WAGED keeps their replicas and only adds
   * temporary ones where a partition falls below its minimum active replicas.</li>
   * <li>Zone loss: the zone's instances are removed and one round runs.</li>
   * </ul>
   */
  private Probe evaluate(ClusterState base, EngineSettings settings, List<String> removed, ZoneEvent event,
      List<String> zone, Map<String, Map<String, Map<String, String>>> startLayout, Probe reference)
      throws Exception {
    Probe probe = new Probe();
    try (DryRunEngine engine = new DryRunEngine()) {
      engine.start(base.copy(), settings);
      for (String instance : _dropFirst) {
        StateOps.removeInstance(engine.state(), instance);
      }
      for (String instance : _reviveFirst) {
        StateOps.enable(engine.state(), instance);
        StateOps.revive(engine.state(), instance, 1);
      }
      int rounds = 0;
      RoundResult round;
      long stuck = 0;
      int evacuationRounds = 0;
      boolean waitedForWindow = false;
      long waitingOnDown = 0;
      int batches = 0;
      int batchSizeUsed = 0;
      boolean maintenance = false;
      if (_spec.evacuate && !removed.isEmpty()) {
        // Evacuate in batches that keep the instances unable to take replicas within
        // MAX_OFFLINE_INSTANCES_ALLOWED: past it the controller enters maintenance mode and stops moving
        // replicas, so the evacuation would never finish.
        int batchSize = evacuationBatchSize(engine.state(), removed.size());
        round = null;
        long left = 0;
        for (int from = 0; from < removed.size() && left == 0; from += batchSize) {
          List<String> batch = removed.subList(from, Math.min(removed.size(), from + batchSize));
          batches++;
          for (String instance : batch) {
            StateOps.setOperation(engine.state(), instance, InstanceConstants.InstanceOperation.EVACUATE);
          }
          RoundResult first = engine.runRound(++rounds);
          if (round == null) {
            round = first;
          } else {
            merge(round, first);
          }
          left = replicasOn(engine.state().getServedLayout(), batch);
          int stalled = 0;
          int batchRounds = 1;
          while (left > 0 && round.failures.isEmpty() && batchRounds < _spec.maxEvacuationRounds) {
            merge(round, engine.runRound(++rounds));
            batchRounds++;
            long now = replicasOn(engine.state().getServedLayout(), batch);
            stalled = now < left ? 0 : stalled + 1;
            left = now;
            if (stalled >= 2) {
              // WAGED may give a replacement to an instance that is down but within its delay window; it
              // cannot come up there, so the old replica stays until the window ends. Wait it out once.
              long waiting = replacementsOnDownInstances(engine.state(), batch);
              ClusterConfig config = engine.state().getClusterConfig();
              if (waitedForWindow || waiting == 0 || !config.isDelayRebalaceEnabled()) {
                break;
              }
              waitingOnDown = waiting;
              engine.advanceClock(config.getRebalanceDelayTime() + 60_000L);
              waitedForWindow = true;
              stalled = 0;
            }
          }
          if (left == 0) {
            for (String instance : batch) {
              StateOps.removeInstance(engine.state(), instance);
            }
            merge(round, engine.runRound(++rounds));
          }
          maintenance |= StateOps.isMaintenance(engine.state());
        }
        evacuationRounds = rounds;
        stuck = left;
        batchSizeUsed = batchSize;
      } else {
        for (String instance : removed) {
          StateOps.removeInstance(engine.state(), instance);
        }
        round = engine.runRound(++rounds);
        if (event == ZoneEvent.OUTAGE) {
          // Let the moves of the deleted instances' replicas settle before the outage.
          merge(round, engine.runRound(++rounds));
        }
      }
      if (event == ZoneEvent.OUTAGE) {
        for (String instance : zone) {
          StateOps.kill(engine.state(), instance);
        }
        merge(round, engine.runRound(++rounds));
      } else if (event == ZoneEvent.LOSS) {
        for (String instance : zone) {
          StateOps.removeInstance(engine.state(), instance);
        }
        merge(round, engine.runRound(++rounds));
      }
      ClusterState after = engine.state();
      // Judge WAGED's own assignment. The served layout of a single round still has the old copies of
      // moved replicas, and leaves out replicas on instances that are offline within the delay window.
      Map<String, Map<String, Map<String, String>>> assignment = after.getBestPossible();
      if (assignment == null) {
        assignment = after.getServedLayout();
      }
      StatsCollector collector = new StatsCollector(after, _focusKey);
      probe.nodes = withFaultZones(collector.nodes(assignment));
      probe.stats = collector.stats(assignment, startLayout, after.getBaseline(), probe.nodes);
      // Minimum active replicas count what serves: WAGED's temporary replicas for instances that are down
      // within the delay window are in the controller's output, not in its stored assignment.
      placement(after, assignment, after.getServedLayout(), probe);
      probe.stats.put("rebalanceFailures", (long) round.failures.size());
      probe.stats.put("passes.global", round.passes.getOrDefault("global", 0L));
      probe.stats.put("passes.partial", round.passes.getOrDefault("partial", 0L));
      probe.stats.put("passes.overwrite", round.passes.getOrDefault("overwrite", 0L));
      probe.stats.put("rounds", rounds);
      probe.stats.put("evacuationStuck", stuck);
      if (evacuationRounds > 0) {
        probe.stats.put("evacuation.rounds", evacuationRounds);
        probe.stats.put("evacuation.batches", batches);
        probe.stats.put("evacuation.batchSize", batchSizeUsed);
        probe.stats.put("evacuation.waitedForDelayWindow", waitedForWindow ? 1 : 0);
        probe.stats.put("evacuation.waitingOnDownInstances", waitingOnDown);
        probe.stats.put("evacuation.maintenance", maintenance ? 1 : 0);
      }
      if (event == ZoneEvent.OUTAGE) {
        // What clients see during the outage: top states on the instances still serving.
        Map<String, Map<String, Map<String, String>>> served = after.getServedLayout();
        Map<String, Object> servedStats = collector.stats(served, null, null, collector.nodes(served));
        probe.stats.put("outage.missingTopState", servedStats.get("missingTopState"));
        probe.stats.put("outage.maxUtil.top." + _focusKey, servedStats.get("maxUtil.top." + _focusKey));
        probe.stats.put("outage.maintenance", round.maintenance ? 1 : 0);
        probe.stats.put("outage.rebalanceFailures", (long) round.failures.size());
      }
      for (String stat : ADDED) {
        long now = number(probe.stats.get(stat));
        long then = reference == null ? now : number(reference.stats.get(stat));
        probe.stats.put(stat + ".added", Math.max(0, now - then));
      }
      probe.failures.addAll(round.failures);
      probe.blocking.putAll(round.blockingConstraints);
      probe.feasible = (event == ZoneEvent.OUTAGE ? _outageFeasibleIf : _spec.feasibleIf).test(probe.stats);
      probe.round = round;
    }
    return probe;
  }

  /**
   * @return how many instances can be evacuated at once: what MAX_OFFLINE_INSTANCES_ALLOWED leaves after
   *         the instances that already cannot take replicas (down, disabled), at least 1; all of them
   *         when there is no limit
   */
  static int evacuationBatchSize(ClusterState state, int wanted) {
    int limit = state.getClusterConfig().getMaxOfflineInstancesAllowed();
    if (limit < 0) {
      return wanted;
    }
    int unable = InstanceUtil.getInstancesUnableToAcceptOnlineReplicas(state.getInstanceConfigs(),
        state.getLiveInstances().keySet(), System.currentTimeMillis()).size();
    return Math.max(1, Math.min(wanted, limit - unable));
  }

  /**
   * @return replacements WAGED assigned to instances that are not live, for partitions that still have a
   *         replica on the evacuating instances
   */
  private static long replacementsOnDownInstances(ClusterState state, List<String> evacuating) {
    Set<String> set = new java.util.HashSet<>(evacuating);
    Set<String> live = state.getLiveInstances().keySet();
    Map<String, Map<String, Map<String, String>>> served = state.getServedLayout();
    Map<String, Map<String, Map<String, String>>> assignment = state.getBestPossible();
    if (assignment == null) {
      return 0;
    }
    long count = 0;
    for (Map.Entry<String, Map<String, Map<String, String>>> resource : served.entrySet()) {
      for (Map.Entry<String, Map<String, String>> partition : resource.getValue().entrySet()) {
        if (partition.getValue().keySet().stream().noneMatch(set::contains)) {
          continue;
        }
        for (String instance : assignment.getOrDefault(resource.getKey(), Collections.emptyMap())
            .getOrDefault(partition.getKey(), Collections.emptyMap()).keySet()) {
          if (!partition.getValue().containsKey(instance) && !live.contains(instance)) {
            count++;
          }
        }
      }
    }
    return count;
  }

  /** @return replicas the instances still hold in the layout */
  private static long replicasOn(Map<String, Map<String, Map<String, String>>> layout, List<String> instances) {
    Set<String> set = new java.util.HashSet<>(instances);
    long count = 0;
    for (Map<String, Map<String, String>> partitions : layout.values()) {
      for (Map<String, String> replicas : partitions.values()) {
        for (String instance : replicas.keySet()) {
          if (set.contains(instance)) {
            count++;
          }
        }
      }
    }
    return count;
  }

  private static void merge(RoundResult into, RoundResult next) {
    into.failures.addAll(next.failures);
    next.blockingConstraints.forEach((type, count) -> into.blockingConstraints.merge(type, count, Long::sum));
    next.passes.forEach((pass, count) -> into.passes.merge(pass, count, Long::sum));
    into.notes.addAll(next.notes);
    into.maintenance |= next.maintenance;
  }

  /** @return whether a replica in this state holds data: not dropped, in error or in the initial state */
  private static boolean hosting(StateModelDefinition model, String state) {
    return state != null && !"DROPPED".equals(state) && !"ERROR".equals(state)
        && (model == null || !state.equals(model.getInitialState()));
  }

  /**
   * Adds placement stats of WAGED's assignment: replicas it did not place on an existing instance,
   * instances over capacity, and partitions with two replicas in one fault zone.
   */
  private static void placement(ClusterState state, Map<String, Map<String, Map<String, String>>> assignment,
      Map<String, Map<String, Map<String, String>>> served, Probe probe) {
    Set<String> instances = state.getInstanceConfigs().keySet();
    Map<String, StateModelDefinition> models = state.getStateModelDefs();
    int live = Math.max(1, state.getLiveInstances().size());
    long unplaced = 0;
    long zoneConflicts = 0;
    long belowMinActive = 0;
    long noLiveReplica = 0;
    Map<String, ResourceConfig> resourceConfigs = state.getResourceConfigs();
    for (Map.Entry<String, IdealState> entry : state.getWagedIdealStates().entrySet()) {
      IdealState idealState = entry.getValue();
      int replicas = idealState.getReplicaCount(live);
      String text = idealState.getReplicas();
      if (text != null && text.matches("\\d+")) {
        probe.replicas = Math.max(probe.replicas, replicas);
      }
      StateModelDefinition model = models.get(idealState.getStateModelDefRef());
      int minActive = DelayedRebalanceUtil.getMinActiveReplica(resourceConfigs.get(entry.getKey()), idealState,
          replicas);
      Map<String, Map<String, String>> partitions =
          assignment.getOrDefault(entry.getKey(), Collections.emptyMap());
      Map<String, Map<String, String>> servedPartitions = served == null ? Collections.emptyMap()
          : served.getOrDefault(entry.getKey(), Collections.emptyMap());
      for (String partition : idealState.getPartitionSet()) {
        int placed = 0;
        int up = 0;
        for (Map.Entry<String, String> replica : servedPartitions.getOrDefault(partition, Collections.emptyMap())
            .entrySet()) {
          NodeStats node = probe.nodes.get(replica.getKey());
          if (node != null && node.routable && hosting(model, replica.getValue())) {
            up++;
          }
        }
        Set<String> zones = new TreeSet<>();
        boolean conflict = false;
        for (Map.Entry<String, String> replica : partitions.getOrDefault(partition, Collections.emptyMap())
            .entrySet()) {
          if (!instances.contains(replica.getKey()) || !hosting(model, replica.getValue())) {
            continue;
          }
          placed++;
          NodeStats node = probe.nodes.get(replica.getKey());
          if (node != null && node.zone != null && !zones.add(node.zone)) {
            conflict = true;
          }
        }
        unplaced += Math.max(0, replicas - placed);
        if (up < Math.min(minActive, replicas)) {
          belowMinActive++;
        }
        if (up == 0) {
          noLiveReplica++;
        }
        if (conflict) {
          zoneConflicts++;
        }
      }
    }
    long overCapacity = 0;
    long onNonServing = 0;
    Set<String> zonesLeft = new TreeSet<>();
    for (NodeStats node : probe.nodes.values()) {
      if (!node.serving) {
        onNonServing += node.replicaCount;
      }
      for (Map.Entry<String, Integer> capacity : node.capacity.entrySet()) {
        if (node.allLoad.getOrDefault(capacity.getKey(), 0L) > capacity.getValue()) {
          overCapacity++;
          break;
        }
      }
      if (node.serving && node.zone != null) {
        zonesLeft.add(node.zone);
      }
    }
    probe.zonesLeft = zonesLeft.size();
    probe.stats.put("unplacedReplicas", unplaced);
    probe.stats.put("overCapacityInstances", overCapacity);
    probe.stats.put("zoneConflicts", zoneConflicts);
    probe.stats.put("replicasOnNonServing", onNonServing);
    probe.stats.put("belowMinActive", belowMinActive);
    probe.stats.put("noLiveReplica", noLiveReplica);
    probe.stats.put("zones.serving", probe.zonesLeft);
  }

  /**
   * @return the nodes, with zones cleared when the cluster is not topology aware: WAGED then treats each
   *         instance as its own fault zone, so the domain's zone does not constrain placement
   */
  private Map<String, NodeStats> withFaultZones(Map<String, NodeStats> nodes) {
    if (!_faultZones) {
      nodes.values().forEach(node -> node.zone = null);
    }
    return nodes;
  }

  /** Problems counted against the k = 0 probe, so problems the cluster already has don't count. */
  static final String[] ADDED = {"unplacedReplicas", "overCapacityInstances", "zoneConflicts", "belowMinActive",
      "noLiveReplica", "underReplicated", "missingTopState"};

  private static long number(Object value) {
    return value instanceof Number ? ((Number) value).longValue() : 0;
  }

  /**
   * @return instances WAGED's assignment gives replicas to: those serving, and those down that it
   *         expects back within the delay window. Uses the persisted best possible assignment, or the
   *         ideal states WAGED writes it to when the source has none.
   */
  static Set<String> assignedInstances(ClusterState state) {
    Set<String> instances = new TreeSet<>();
    if (state.getBestPossible() != null) {
      state.getBestPossible().values().forEach(partitions -> partitions.values()
          .forEach(replicas -> instances.addAll(replicas.keySet())));
    } else {
      state.getIdealStates().values().forEach(idealState -> idealState.getRecord().getMapFields().values()
          .forEach(replicas -> instances.addAll(replicas.keySet())));
    }
    return instances;
  }

  /**
   * @return the largest k for which the instances left still have at least the capacity all replicas
   *         require on every key. Like WAGED's baseline, it counts every assignable instance, serving or
   *         not, except those removed first. WAGED fails with a capacity deficit beyond it.
   */
  static int capacityBound(List<String> order, Map<String, NodeStats> nodes, Map<String, Object> stats,
      List<String> keys, Set<String> removedFirst) {
    Map<String, Long> spare = new TreeMap<>();
    for (String key : keys) {
      long required = number(stats.get("required.all." + key));
      if (required <= 0) {
        continue;
      }
      long capacity = 0;
      for (NodeStats node : nodes.values()) {
        if (node.assignable && !removedFirst.contains(node.instance)) {
          capacity += node.capacity.getOrDefault(key, 0);
        }
      }
      spare.put(key, capacity - required);
    }
    int k = 0;
    for (String instance : order) {
      NodeStats node = nodes.get(instance);
      boolean fits = true;
      for (Map.Entry<String, Long> entry : spare.entrySet()) {
        long left = entry.getValue() - (node == null ? 0 : node.capacity.getOrDefault(entry.getKey(), 0));
        entry.setValue(left);
        fits &= left >= 0;
      }
      if (!fits) {
        break;
      }
      k++;
    }
    return k;
  }

  /** @return zone to all-replica utilization % of its serving instances on the focus key */
  private Map<String, Double> zoneUtil(Map<String, NodeStats> nodes) {
    Map<String, long[]> sums = new TreeMap<>(RemovalOrder.ZONE_ORDER);
    for (NodeStats node : nodes.values()) {
      if (node.serving) {
        long[] sum = sums.computeIfAbsent(node.zone == null ? "(none)" : node.zone, z -> new long[2]);
        sum[0] += node.allLoad.getOrDefault(_focusKey, 0L);
        sum[1] += node.capacity.getOrDefault(_focusKey, 0);
      }
    }
    Map<String, Double> util = new TreeMap<>(RemovalOrder.ZONE_ORDER);
    sums.forEach((zone, sum) -> util.put(zone, sum[1] == 0 ? 0 : Math.round(1000.0 * sum[0] / sum[1]) / 10.0));
    return util;
  }

  private static String summarize(List<String> removed) {
    if (removed.size() <= 3) {
      return String.join(", ", removed);
    }
    return String.join(", ", removed.subList(0, 3)) + " and " + (removed.size() - 3) + " more";
  }

  private void record(String variant, int index, List<String> events, RoundResult round, Map<String, Object> stats,
      Probe probe) throws Exception {
    Map<String, Object> record = new LinkedHashMap<>();
    record.put("variant", variant);
    record.put("round", index);
    record.put("events", events);
    record.put("passes", round.passes);
    record.put("failures", round.failures);
    record.put("blocking", round.blockingConstraints);
    record.put("notes", round.notes);
    if (probe != null) {
      record.put("probe", probe.k);
      record.put("feasible", probe.feasible);
      if (!probe.zoneLossResults.isEmpty()) {
        record.put("zoneLoss", probe.zoneLossResults);
      }
      if (!probe.zoneOutageResults.isEmpty()) {
        record.put("zoneOutage", probe.zoneOutageResults);
      }
    }
    record.put("stats", stats);
    _output.round(record);
  }
}
