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

import org.apache.helix.model.IdealState;
import org.apache.helix.model.StateModelDefinition;
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
  /** largest, every or none. */
  private String _zoneLoss = "none";
  /** Whether WAGED places by fault zone; without topology awareness every instance is its own zone. */
  private boolean _faultZones = true;

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

    String reason() {
      if (zoneLoss != null && !zoneLoss.feasible) {
        return "places every replica, but not after also losing zone " + zoneLoss.lostZone + " ("
            + zoneLoss.lostInstances + " instances): " + zoneLoss.reason();
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
      if (zonesLeft > 0 && zonesLeft < replicas && number(stats.get("unplacedReplicas.added")) > 0) {
        return "only " + zonesLeft + " fault zone(s) left for " + replicas + " replicas: WAGED places at most one "
            + "replica of a partition per zone, so it places fewer replicas without a rebalance failure; "
            + "unplaced replicas +" + stats.get("unplacedReplicas.added");
      }
      return "no rebalance failure, but the assignment does not meet the feasibility condition: unplaced replicas +"
          + stats.get("unplacedReplicas.added") + ", instances over capacity +" + stats.get("overCapacityInstances.added")
          + ", partitions with two replicas in a zone +" + stats.get("zoneConflicts.added");
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
    _faultZones = state.getClusterConfig().isTopologyAwareEnabled();
    Map<String, Map<String, Map<String, String>>> served = state.getServedLayout();
    StatsCollector collector = new StatsCollector(state, _focusKey);
    Map<String, NodeStats> nodes = withFaultZones(collector.nodes(served));
    Map<String, Object> startStats = collector.stats(served, null, state.getBaseline(), nodes);
    record(variant.name, 0, Collections.emptyList(), new RoundResult(), startStats, null);

    List<String> candidates = new ArrayList<>();
    List<String> nonServing = new ArrayList<>();
    nodes.values().forEach(n -> (n.serving ? candidates : nonServing).add(n.instance));
    if (_spec.removeNonServing) {
      _dropFirst = nonServing;
    }
    String zoneLossNote = null;
    if (!"none".equals(_zoneLoss) && nodes.values().stream().noneMatch(n -> n.zone != null)) {
      zoneLossNote = "the cluster has no fault zones (or is not topology aware), so zone loss was not checked";
      _console.println("[" + variant.name + "] " + zoneLossNote);
      _zoneLoss = "none";
    }
    List<String> order = RemovalOrder.order(strategy, candidates, nodes, _focusKey, new Random(_scenario.seed));
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
    int bound = capacityBound(order, nodes, startStats, collector.keys(), _spec.removeNonServing);
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
    if (zoneLossNote != null) {
      summary.put("note", zoneLossNote);
    }
    summary.put("servingInstances", order.size());
    summary.put("searchedUpTo", maxK);
    summary.put("capacityBound", bound);
    summary.put("nonServingInstances", nonServing.size());
    summary.put("nonServing", _spec.removeNonServing ? "removed" : "kept");
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
      for (String stat : new String[]{"util.required." + _focusKey, "maxUtil.all." + _focusKey,
          "skew.all." + _focusKey, "skew.top." + _focusKey, "unplacedReplicas", "overCapacityInstances",
          "zoneConflicts", "moves.replicas"}) {
        row.put(stat, probe.stats.get(stat));
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

  private Probe probe(ClusterState base, EngineSettings settings, List<String> order, int k, String variant,
      List<Probe> probes, Map<String, Map<String, Map<String, String>>> startLayout, Probe reference)
      throws Exception {
    List<String> removed = order.subList(0, k);
    Probe probe = evaluate(base, settings, removed, startLayout, reference);
    probe.index = probes.size() + 1;
    probe.k = k;
    probe.stats.put("search.k", k);
    if (probe.feasible && !"none".equals(_zoneLoss)) {
      for (Map.Entry<String, List<String>> zone : zonesToLose(probe.nodes).entrySet()) {
        List<String> lost = new ArrayList<>(removed);
        lost.addAll(zone.getValue());
        Probe check = evaluate(base, settings, lost, startLayout, reference != null ? reference : probe);
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
    probe.stats.put("search.feasible", probe.feasible ? 1 : 0);
    List<String> events = new ArrayList<>();
    events.add("remove " + k + " instance(s)" + (k == 0 ? "" : ": " + summarize(removed)));
    probe.zoneLossResults.forEach((zone, result) -> events.add("then lose zone " + zone + ": " + result));
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

  /** Removes the instances, runs one controller pipeline, and judges WAGED's assignment. */
  private Probe evaluate(ClusterState base, EngineSettings settings, List<String> removed,
      Map<String, Map<String, Map<String, String>>> startLayout, Probe reference) throws Exception {
    Probe probe = new Probe();
    try (DryRunEngine engine = new DryRunEngine()) {
      engine.start(base.copy(), settings);
      for (String instance : _dropFirst) {
        StateOps.removeInstance(engine.state(), instance);
      }
      for (String instance : removed) {
        StateOps.removeInstance(engine.state(), instance);
      }
      RoundResult round = engine.runRound(1);
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
      placement(after, assignment, probe);
      probe.stats.put("rebalanceFailures", (long) round.failures.size());
      probe.stats.put("passes.global", round.passes.getOrDefault("global", 0L));
      probe.stats.put("passes.partial", round.passes.getOrDefault("partial", 0L));
      for (String stat : ADDED) {
        long now = number(probe.stats.get(stat));
        long then = reference == null ? now : number(reference.stats.get(stat));
        probe.stats.put(stat + ".added", Math.max(0, now - then));
      }
      probe.failures.addAll(round.failures);
      probe.blocking.putAll(round.blockingConstraints);
      probe.feasible = _spec.feasibleIf.test(probe.stats);
      probe.round = round;
    }
    return probe;
  }

  /**
   * Adds placement stats of WAGED's assignment: replicas it did not place on an existing instance,
   * instances over capacity, and partitions with two replicas in one fault zone.
   */
  private static void placement(ClusterState state, Map<String, Map<String, Map<String, String>>> assignment,
      Probe probe) {
    Set<String> instances = state.getInstanceConfigs().keySet();
    Map<String, StateModelDefinition> models = state.getStateModelDefs();
    int live = Math.max(1, state.getLiveInstances().size());
    long unplaced = 0;
    long zoneConflicts = 0;
    for (Map.Entry<String, IdealState> entry : state.getWagedIdealStates().entrySet()) {
      IdealState idealState = entry.getValue();
      int replicas = idealState.getReplicaCount(live);
      String text = idealState.getReplicas();
      if (text != null && text.matches("\\d+")) {
        probe.replicas = Math.max(probe.replicas, replicas);
      }
      StateModelDefinition model = models.get(idealState.getStateModelDefRef());
      Map<String, Map<String, String>> partitions =
          assignment.getOrDefault(entry.getKey(), Collections.emptyMap());
      for (String partition : idealState.getPartitionSet()) {
        int placed = 0;
        Set<String> zones = new TreeSet<>();
        boolean conflict = false;
        for (Map.Entry<String, String> replica : partitions.getOrDefault(partition, Collections.emptyMap())
            .entrySet()) {
          String replicaState = replica.getValue();
          if (!instances.contains(replica.getKey()) || replicaState == null || "DROPPED".equals(replicaState)
              || "ERROR".equals(replicaState) || (model != null && replicaState.equals(model.getInitialState()))) {
            continue;
          }
          placed++;
          NodeStats node = probe.nodes.get(replica.getKey());
          if (node != null && node.zone != null && !zones.add(node.zone)) {
            conflict = true;
          }
        }
        unplaced += Math.max(0, replicas - placed);
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
  static final String[] ADDED = {"unplacedReplicas", "overCapacityInstances", "zoneConflicts", "underReplicated",
      "missingTopState"};

  private static long number(Object value) {
    return value instanceof Number ? ((Number) value).longValue() : 0;
  }

  /**
   * @return the largest k for which the instances left still have at least the capacity all replicas
   *         require on every key. Like WAGED's baseline, it counts every assignable instance, serving or
   *         not, unless non-serving instances are removed. WAGED fails with a capacity deficit beyond it.
   */
  static int capacityBound(List<String> order, Map<String, NodeStats> nodes, Map<String, Object> stats,
      List<String> keys, boolean removeNonServing) {
    Map<String, Long> spare = new TreeMap<>();
    for (String key : keys) {
      long required = number(stats.get("required.all." + key));
      if (required <= 0) {
        continue;
      }
      long capacity = 0;
      for (NodeStats node : nodes.values()) {
        if (node.assignable && (node.serving || !removeNonServing)) {
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
    }
    record.put("stats", stats);
    _output.round(record);
  }
}
