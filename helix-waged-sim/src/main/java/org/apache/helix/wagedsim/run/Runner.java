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
import java.nio.file.Path;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.function.Supplier;

import org.apache.helix.wagedsim.cluster.ClusterNormalizer;
import org.apache.helix.wagedsim.cluster.ClusterState;
import org.apache.helix.wagedsim.cluster.Layouts;
import org.apache.helix.wagedsim.engine.Engine;
import org.apache.helix.wagedsim.engine.EngineSettings;
import org.apache.helix.wagedsim.engine.RoundResult;
import org.apache.helix.wagedsim.scenario.ClusterConfigOverrides;
import org.apache.helix.wagedsim.scenario.EventSpec;
import org.apache.helix.wagedsim.scenario.Events;
import org.apache.helix.wagedsim.scenario.Scenario;
import org.apache.helix.wagedsim.stats.NodeStats;
import org.apache.helix.wagedsim.stats.StatsCollector;
import org.apache.helix.wagedsim.util.Durations;

/** Runs every variant of a scenario on copies of a cluster and records each round. */
public class Runner {
  private final Scenario _scenario;
  private final RunOutput _output;
  private final PrintStream _console;
  private final Supplier<Engine> _engines;
  private final String _focusKey;

  public Runner(Scenario scenario, RunOutput output, PrintStream console, Supplier<Engine> engines,
      String focusKey) {
    _scenario = scenario;
    _output = output;
    _console = console;
    _engines = engines;
    _focusKey = focusKey;
  }

  /** Result of one variant. */
  public static class VariantResult {
    public String name;
    public Verdict verdict;
    public int rounds;
    public long elapsedMillis;
    public Map<String, Object> settings = new LinkedHashMap<>();
    public Map<String, Object> start = new LinkedHashMap<>();
    public Map<String, Object> end = new LinkedHashMap<>();
    /** Scale-down search result, for search scenarios. */
    public Map<String, Object> search;
  }

  public List<VariantResult> run(ClusterState base, int parallelism) throws Exception {
    List<VariantResult> results = new ArrayList<>();
    List<Scenario.Variant> variants = new ArrayList<>(_scenario.variants.values());
    if (parallelism <= 1 || variants.size() == 1) {
      for (Scenario.Variant variant : variants) {
        results.add(runVariant(base, variant));
      }
      return results;
    }
    ExecutorService pool = Executors.newFixedThreadPool(Math.min(parallelism, variants.size()));
    try {
      List<Future<VariantResult>> futures = new ArrayList<>();
      for (Scenario.Variant variant : variants) {
        futures.add(pool.submit(() -> runVariant(base, variant)));
      }
      for (Future<VariantResult> future : futures) {
        results.add(future.get());
      }
    } finally {
      pool.shutdownNow();
    }
    return results;
  }

  VariantResult runVariant(ClusterState base, Scenario.Variant variant) {
    VariantResult result = new VariantResult();
    result.name = variant.name;
    long start = System.currentTimeMillis();
    int round = 0;
    Engine engine = null;
    try {
      ClusterState state = base.copy();
      List<String> overrides = new ArrayList<>(ClusterConfigOverrides.apply(state,
          placeholders(_scenario.clusterConfig)));
      overrides.addAll(ClusterConfigOverrides.apply(state, placeholders(variant.clusterConfig)));
      EngineSettings settings = variant.apply(_scenario.settings);
      result.settings.put("mode", _scenario.mode);
      result.settings.put("pass", settings.pass.name().toLowerCase());
      result.settings.put("activeNodes", settings.activeNodes.name().toLowerCase().replace('_', '-'));
      result.settings.put("firstRound", settings.firstRound.name().toLowerCase());
      result.settings.put("constraintWeights", settings.constraintWeights);
      result.settings.put("clusterConfig", overrides);
      List<String> problems = ClusterNormalizer.validate(state);
      if (!problems.isEmpty()) {
        throw new IllegalArgumentException("Cluster cannot run WAGED: " + String.join("; ", problems));
      }
      if (_scenario.search != null) {
        new ScaleDownSearch(_scenario, _output, _console, _focusKey).run(state, variant, settings, result, start);
        result.elapsedMillis = System.currentTimeMillis() - start;
        _console.println("[" + variant.name + "] " + result.verdict + " (" + Durations.format(result.elapsedMillis) + ")");
        return result;
      }
      engine = _engines.get();
      engine.start(state, settings);
      long simStart = engine.now();
      Events.Context context = new Events.Context(engine, _scenario.seed, _focusKey);
      ExitCriteria.Tracker tracker = _scenario.exit.new Tracker(_scenario.lastEventRound());
      Map<String, Map<String, Map<String, String>>> previous = engine.state().getServedLayout();
      StatsCollector collector = new StatsCollector(engine.state(), _focusKey);
      Map<String, NodeStats> nodes = collector.nodes(previous);
      Map<String, Object> stats = collector.stats(previous, null, engine.state().getBaseline(), nodes);
      result.start = stats;
      writeRound(variant.name, 0, Collections.emptyList(), new RoundResult(), stats, engine, simStart, start,
          null, null);
      writeNodes(variant.name, 0, nodes, collector, false);
      context.lastNodes = nodes;
      long cumulativeMoves = 0;
      Verdict verdict = null;
      while (verdict == null) {
        round++;
        List<String> events = new ArrayList<>();
        for (EventSpec event : _scenario.events) {
          if (event.dueAt(round)) {
            events.addAll(Events.apply(event, context));
          }
        }
        RoundResult roundResult = engine.runRound(round);
        roundResult.events = events;
        Map<String, Map<String, Map<String, String>>> served = engine.state().getServedLayout();
        collector = new StatsCollector(engine.state(), _focusKey);
        nodes = collector.nodes(served);
        stats = collector.stats(served, previous, engine.state().getBaseline(), nodes);
        cumulativeMoves += ((Number) stats.getOrDefault("moves.replicas", 0L)).longValue();
        stats.put("moves.cumulative", cumulativeMoves);
        stats.put("round", round);
        stats.put("passes.global", roundResult.passes.getOrDefault("global", 0L));
        stats.put("passes.partial", roundResult.passes.getOrDefault("partial", 0L));
        stats.put("rebalanceFailures", (long) roundResult.failures.size());
        stats.put("maintenance", roundResult.maintenance ? 1 : 0);
        stats.put("time.roundMs", roundResult.computeMillis + roundResult.settleMillis);
        stats.put("simTime.hours", Math.round((engine.now() - simStart) / 36_000.0) / 100.0);
        long elapsed = System.currentTimeMillis() - start;
        verdict = tracker.afterRound(round, stats, roundResult.failures, elapsed, engine.now() - simStart);
        if (!roundResult.settled && verdict == null) {
          verdict = Verdict.fail(round, "round did not settle within the round timeout");
        }
        writeRound(variant.name, round, events, roundResult, stats, engine, simStart, start, verdict,
            tracker);
        writeNodes(variant.name, round, nodes, collector, verdict != null);
        context.lastNodes = nodes;
        previous = Layouts.copy(served);
        result.end = stats;
      }
      result.verdict = verdict;
    } catch (Throwable t) {
      result.verdict = Verdict.error(round, t.getClass().getSimpleName() + ": " + t.getMessage());
      _console.println("[" + variant.name + "] ERROR at round " + round + ": " + t);
      if (System.getenv("WAGED_SIM_DEBUG") != null) {
        t.printStackTrace(_console);
      }
    } finally {
      if (engine != null) {
        engine.close();
      }
    }
    result.rounds = round;
    result.elapsedMillis = System.currentTimeMillis() - start;
    _console.println("[" + variant.name + "] " + result.verdict + " (" + Durations.format(result.elapsedMillis) + ")");
    return result;
  }

  /** Replaces {@code ${focusKey}} in override values with the focus key of the run. */
  @SuppressWarnings("unchecked")
  private Map<String, Object> placeholders(Map<String, Object> map) {
    Map<String, Object> result = new LinkedHashMap<>();
    map.forEach((key, value) -> result.put(key, placeholder(value)));
    return result;
  }

  @SuppressWarnings("unchecked")
  private Object placeholder(Object value) {
    if (value instanceof String) {
      return ((String) value).replace("${focusKey}", String.valueOf(_focusKey));
    }
    if (value instanceof List) {
      List<Object> list = new ArrayList<>();
      for (Object item : (List<Object>) value) {
        list.add(placeholder(item));
      }
      return list;
    }
    if (value instanceof Map) {
      return placeholders((Map<String, Object>) value);
    }
    return value;
  }

  private void writeRound(String variant, int round, List<String> events, RoundResult result,
      Map<String, Object> stats, Engine engine, long simStart, long start, Verdict verdict,
      ExitCriteria.Tracker tracker) throws Exception {
    Map<String, Object> record = new LinkedHashMap<>();
    record.put("variant", variant);
    record.put("round", round);
    record.put("simTime", Instant.ofEpochMilli(engine.now()).toString());
    record.put("simElapsed", Durations.format(engine.now() - simStart));
    record.put("elapsedMs", System.currentTimeMillis() - start);
    record.put("events", events);
    record.put("passes", result.passes);
    record.put("failures", result.failures);
    record.put("notes", result.notes);
    if (_scenario.exit.until != null) {
      record.put("until", _scenario.exit.until.test(stats));
    }
    if (_scenario.exit.failIf != null) {
      record.put("failIf", _scenario.exit.failIf.test(stats));
    }
    if (verdict != null) {
      record.put("verdict", verdict.status.name());
      record.put("reason", verdict.reason);
    }
    record.put("stats", stats);
    _output.round(record);
    if (round > 0 || _console != null) {
      _console.println(progress(variant, round, stats, events, verdict));
    }
  }

  private String progress(String variant, int round, Map<String, Object> stats, List<String> events,
      Verdict verdict) {
    StringBuilder line = new StringBuilder();
    line.append('[').append(variant).append("] r").append(round);
    for (String stat : progressStats()) {
      Object value = stats.get(stat);
      if (value != null) {
        line.append(' ').append(stat).append('=').append(value);
      }
    }
    if (!events.isEmpty()) {
      line.append(" | ").append(events.size() <= 3 ? String.join("; ", events)
          : String.join("; ", events.subList(0, 3)) + "; +" + (events.size() - 3) + " more");
    }
    if (verdict != null) {
      line.append(" => ").append(verdict.status);
    }
    return line.toString();
  }

  private List<String> progressStats() {
    Set<String> names = new LinkedHashSet<>();
    if (_focusKey != null) {
      names.add("skew.top." + _focusKey);
    }
    if (_scenario.exit.until != null) {
      names.addAll(_scenario.exit.until.stats());
    }
    names.add("moves.replicas");
    names.add("moves.topState");
    return new ArrayList<>(names);
  }

  private void writeNodes(String variant, int round, Map<String, NodeStats> nodes, StatsCollector collector,
      boolean last) throws Exception {
    List<String> perNode = _scenario.report.perNode;
    boolean write = perNode.contains("all") || (round == 0 && perNode.contains("start"))
        || (last && perNode.contains("end"));
    if (write) {
      _output.nodes(variant, round, nodes.values(), collector.keys());
    }
  }
}
