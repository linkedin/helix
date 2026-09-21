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
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.function.Consumer;

import org.apache.helix.HelixRebalanceException;
import org.apache.helix.constants.InstanceConstants.InstanceOperation;
import org.apache.helix.controller.changedetector.trimmer.ClusterConfigTrimmer;
import org.apache.helix.controller.rebalancer.util.DelayedRebalanceUtil;
import org.apache.helix.controller.rebalancer.util.WagedRebalanceUtil;
import org.apache.helix.controller.rebalancer.waged.WagedFuzzSim.Driver;
import org.apache.helix.controller.rebalancer.waged.WagedFuzzSim.SimNode;
import org.apache.helix.controller.rebalancer.waged.WagedFuzzSim.SimResource;
import org.apache.helix.controller.rebalancer.waged.WagedFuzzSim.StepResult;
import org.apache.helix.controller.rebalancer.waged.WagedFuzzSim.SubStep;
import org.apache.helix.controller.rebalancer.waged.constraints.FuzzRecordingAlgorithm;
import org.apache.helix.controller.rebalancer.waged.constraints.FuzzRecordingAlgorithm.Outcome;
import org.apache.helix.controller.rebalancer.waged.model.ClusterModel;
import org.apache.helix.controller.rebalancer.waged.model.ClusterModelProvider;
import org.apache.helix.controller.rebalancer.waged.model.OptimalAssignment;
import org.apache.helix.model.ClusterConfig;
import org.apache.helix.model.IdealState;
import org.apache.helix.model.InstanceConfig;
import org.apache.helix.model.Partition;
import org.apache.helix.model.ResourceAssignment;
import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.core.config.Configurator;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

/**
 * Seeded property based tests of instance tag isolation, driven through the real WagedRebalancer
 * and the real algorithm with an in memory assignment store and no ZooKeeper.
 *
 * Properties: fuzz.iso.firstSeed, fuzz.iso.seeds, fuzz.iso.steps, and fuzz.iso.collide (true,
 * false, or by default only on even seeds) for the events that move a node out of a broken clique.
 * The defaults keep a run short; raise them to fuzz for longer. The default seeds are chosen so the
 * short run still reaches a collision yield, a broken clique a scope can still place, and a
 * healthy clique topped up while a node is out inside its window and another clique trips the
 * cluster wide deficit. Every violation names the seed, mode and step that reproduce it.
 */
public class TestWagedIsolationFuzz {
  private static final Consumer<ClusterConfig> FLAG_ON =
      config -> config.setWagedInstanceTagIsolationEnabled(true);
  static final String DELAYED = "DELAYED_REBALANCE_OVERWRITES";
  // Prefix for the counts of delayed rebalance overwrites run where an asynchronous partial
  // rebalance serves the emergency result, which is where the overwrite runs in that mode.
  static final String ASYNC_PARTIAL_SITE = "asyncPartialSite.";
  private static final boolean[] BOTH_MODES = {false, true};

  private final long _firstSeed = Long.getLong("fuzz.iso.firstSeed", 11L);
  private final int _seeds = Integer.getInteger("fuzz.iso.seeds", 9);
  private final int _steps = Integer.getInteger("fuzz.iso.steps", 16);

  @BeforeClass
  public void quiet() {
    Configurator.setLevel("org.apache.helix", Level.OFF);
  }

  @AfterClass
  public void restoreLogging() {
    Configurator.setLevel("org.apache.helix", Level.ERROR);
  }

  /** Counts what a run exercised and collects every property violation with its reproducer. */
  static final class Tally {
    final String name;
    final TreeMap<String, Long> counts = new TreeMap<>();
    final List<String> violations = new ArrayList<>();
    final TreeMap<String, List<String>> examples = new TreeMap<>();
    final long startMs = System.currentTimeMillis();
    // Healthy cliques that gave up a fresh result to a collision in the current step, and for how
    // many steps in a row each one has.
    final Set<String> yieldedThisStep = new TreeSet<>();
    final Map<String, Integer> yieldStreaks = new HashMap<>();
    // What the last persisted baseline of the current run reported as skipped, and whether a
    // baseline that calculated successfully has not reported yet.
    Set<String> lastBaselineSkipped = Collections.emptySet();
    boolean baselineAwaitingReport;

    Tally(String name) {
      this.name = name;
    }

    void count(String key) {
      counts.merge(key, 1L, Long::sum);
    }

    void add(String key, long delta) {
      counts.merge(key, delta, Long::sum);
    }

    /** Counts what was counted since the snapshot a second time, under the prefix. */
    void countAgain(Map<String, Long> snapshot, String prefix) {
      for (Map.Entry<String, Long> e : new ArrayList<>(counts.entrySet())) {
        long delta = e.getValue() - snapshot.getOrDefault(e.getKey(), 0L);
        if (delta > 0 && !e.getKey().startsWith(prefix) && !"violations".equals(e.getKey())) {
          add(prefix + e.getKey(), delta);
        }
      }
    }

    void violation(String context, String message) {
      counts.merge("violations", 1L, Long::sum);
      if (violations.size() < 100) {
        violations.add(context + ": " + message);
      }
    }

    void example(String key, String context) {
      List<String> list = examples.computeIfAbsent(key, k -> new ArrayList<>());
      if (list.size() < 5) {
        list.add(context);
      }
    }

    void endStep(String context) {
      yieldStreaks.keySet().retainAll(yieldedThisStep);
      for (String tag : yieldedThisStep) {
        int streak = yieldStreaks.merge(tag, 1, Integer::sum);
        if (streak > counts.getOrDefault("yield.longestStreakOfOneHealthyClique", 0L)) {
          counts.put("yield.longestStreakOfOneHealthyClique", (long) streak);
          examples.put("yield.longestStreakOfOneHealthyClique",
              new ArrayList<>(Collections.singletonList(context + " clique=" + tag)));
        }
      }
      yieldedThisStep.clear();
    }

    void endRun() {
      yieldedThisStep.clear();
      yieldStreaks.clear();
      lastBaselineSkipped = Collections.emptySet();
      baselineAwaitingReport = false;
    }

    void finish() {
      System.out.println(
          name + " took " + (System.currentTimeMillis() - startMs) + " ms " + counts);
      examples.forEach((key, list) -> list.forEach(
          context -> System.out.println(name + " EXAMPLE " + key + ": " + context)));
      for (String v : violations) {
        System.out.println(name + " VIOLATION " + v);
      }
      Assert.assertTrue(violations.isEmpty(), name + ": " + violations);
    }
  }

  static boolean collide(long seed) {
    String collide = System.getProperty("fuzz.iso.collide", "even");
    return "even".equals(collide) ? seed % 2 == 0 : Boolean.parseBoolean(collide);
  }

  private static String context(WagedFuzzSim sim, boolean async, boolean asyncPartial, int step,
      String event) {
    return "mode=" + sim.mode + (sim.collide ? "+collide" : "") + " seed=" + sim.seed + " async="
        + async + " asyncPartial=" + asyncPartial + " step=" + step + " event=" + event;
  }

  // ---------------------------------------------------------------------------------------------
  // The flag forms the parity dump moves through
  // ---------------------------------------------------------------------------------------------

  /**
   * Asserts that the isolation flag reads as off in every one of WagedFuzzSim.FLAG_OFF_FORMS, that
   * trimming the cluster config for change detection drops the field in each of them, and that
   * turning the flag on writes "true" under WagedFuzzSim.ISOLATION_FLAG_FIELD.
   */
  @Test
  public void testEveryFlagFormOfTheParityDumpReadsAsOff() {
    for (String form : WagedFuzzSim.FLAG_OFF_FORMS) {
      ClusterConfig config = new ClusterConfig("forms");
      if (form != null) {
        config.getRecord().setSimpleField(WagedFuzzSim.ISOLATION_FLAG_FIELD, form);
      }
      Assert.assertFalse(config.isWagedInstanceTagIsolationEnabled(), "form '" + form + "'");
      Assert.assertNull(ClusterConfigTrimmer.getInstance().trimProperty(config).getRecord()
          .getSimpleField(WagedFuzzSim.ISOLATION_FLAG_FIELD), "trimmed form '" + form + "'");
    }
    ClusterConfig on = new ClusterConfig("forms");
    on.setWagedInstanceTagIsolationEnabled(true);
    Assert.assertEquals(on.getRecord().getSimpleField(WagedFuzzSim.ISOLATION_FLAG_FIELD), "true");
  }

  // ---------------------------------------------------------------------------------------------
  // Flag on against flag off while nothing fails
  // ---------------------------------------------------------------------------------------------

  @Test
  public void testFlagOnIsByteIdenticalWhileEveryCliqueFits() throws Exception {
    Tally tally = new Tally("feasible-parity");
    for (long seed = _firstSeed; seed < _firstSeed + _seeds; seed++) {
      for (boolean async : BOTH_MODES) {
        for (boolean asyncPartial : BOTH_MODES) {
          runFeasibleParity(tally, seed, async, asyncPartial, _steps);
        }
      }
    }
    tally.finish();
  }

  static void runFeasibleParity(Tally tally, long seed, boolean async, boolean asyncPartial,
      int steps) throws Exception {
    WagedFuzzSim sim = WagedFuzzSim.generate(seed, WagedFuzzSim.Mode.FEASIBLE);
    try (Driver off = new Driver(sim.clusterName, "off", async, asyncPartial, null);
        Driver on = new Driver(sim.clusterName, "on", async, asyncPartial, FLAG_ON)) {
      off.keepFullJson = true;
      on.keepFullJson = true;
      for (int step = 0; step < steps; step++) {
        String event = step == 0 ? "INIT" : sim.nextEvent();
        StepResult a = off.step(sim, step, event);
        StepResult b = on.step(sim, step, event);
        tally.count("steps");
        String ctx = context(sim, async, asyncPartial, step, event);
        boolean offFailed = anyCalculateFailure(a);
        if (!a.sha.equals(b.sha)) {
          if (offFailed) {
            // The flag is allowed to act once the default mode fails; stop comparing this seed.
            tally.count("stopped.divergedAfterNaturalFailure");
            return;
          }
          tally.violation(ctx, "flag on differs from flag off: " + firstDifference(a.json, b.json));
          return;
        }
        tally.count(offFailed ? "steps.identicalWithFailure" : "steps.identical");
        for (SubStep sub : b.subSteps) {
          Outcome overwrite = null;
          for (Outcome o : sub.outcomes) {
            if (!o.calculate) {
              tally.count("hooks");
              if (!o.skipped.isEmpty()) {
                tally.violation(ctx, o.scope + " skipped " + o.skipped + " while feasible");
              }
              if (DELAYED.equals(o.scope)) {
                overwrite = o;
              }
            }
          }
          Outcome calculated = calculation(sub, DELAYED);
          if (calculated != null && calculated.failure == null && overwrite != null) {
            Map<String, Long> before = asyncPartial ? new TreeMap<>(tally.counts) : null;
            tally.count("identical." + DELAYED);
            countDelayedCoverage(tally, sim, event, overwrite, Collections.emptySet(), null);
            if (before != null) {
              tally.countAgain(before, ASYNC_PARTIAL_SITE);
            }
          }
        }
      }
    }
  }

  static boolean anyCalculateFailure(StepResult result) {
    for (SubStep sub : result.subSteps) {
      if (sub.failure != null) {
        return true;
      }
      for (Outcome o : sub.outcomes) {
        if (o.calculate && o.failure != null) {
          return true;
        }
      }
    }
    return false;
  }

  static String firstDifference(String a, String b) {
    int i = 0;
    while (i < a.length() && i < b.length() && a.charAt(i) == b.charAt(i)) {
      i++;
    }
    int from = Math.max(0, i - 200);
    return "offset " + i + " off=..." + a.substring(from, Math.min(a.length(), i + 200))
        + " | on=..." + b.substring(from, Math.min(b.length(), i + 200));
  }

  // ---------------------------------------------------------------------------------------------
  // Isolation properties with broken cliques, through the real WagedRebalancer
  // ---------------------------------------------------------------------------------------------

  @Test
  public void testIsolationPropertiesWithBrokenCliques() throws Exception {
    Tally tally = new Tally("break-properties");
    for (long seed = _firstSeed; seed < _firstSeed + _seeds; seed++) {
      for (boolean async : BOTH_MODES) {
        for (boolean asyncPartial : BOTH_MODES) {
          runBreakProperties(tally, seed, async, asyncPartial, _steps);
        }
      }
    }
    tally.finish();
  }

  static void runBreakProperties(Tally tally, long seed, boolean async, boolean asyncPartial,
      int steps) throws Exception {
    WagedFuzzSim sim = WagedFuzzSim.generate(seed, WagedFuzzSim.Mode.BREAK);
    // Moving nodes out of broken cliques is what makes groups collide.
    sim.collide = collide(seed);
    try (Driver on = new Driver(sim.clusterName, "on", async, asyncPartial, FLAG_ON);
        Driver twin = new Driver(sim.clusterName, "twin", async, asyncPartial, FLAG_ON)) {
      on.algorithm.capture = true;
      boolean everBroken = false;
      boolean twinEverFailed = false;
      for (int step = 0; step < steps; step++) {
        String event = step == 0 ? "INIT" : sim.nextEvent();
        String ctx = context(sim, async, asyncPartial, step, event);
        everBroken |= !sim.broken.isEmpty();
        WagedFuzzSim twinSim = sim.twinView();
        StepResult r = on.step(sim, step, event);
        StepResult t = twin.step(twinSim, step, event);
        // Events are only vetted against the cliques that are not broken, so while a clique is
        // broken its never broken twin may run short of nodes and fail on its own.
        twinEverFailed |= anyCalculateFailure(t);
        tally.count("steps");
        if (!sim.broken.isEmpty()) {
          tally.count("steps.withBrokenClique");
        }
        checkStep(tally, ctx, sim, r, asyncPartial);
        tally.endStep(ctx);
        if (!everBroken) {
          if (r.sha.equals(t.sha)) {
            tally.count("twin.identicalBeforeFirstBreak");
          } else {
            tally.violation(ctx, "differs from its twin before anything broke");
          }
        } else if (event.startsWith("REPAIR") && sim.broken.isEmpty()) {
          checkRecovery(tally, ctx, r, on);
          compareWithTwin(tally, ctx, "R6.twin" + (twinEverFailed ? ".twinHadFailed" : ""),
              event.split(" ")[1], sim, twinSim, r, t);
        }
      }
    } finally {
      tally.endRun();
    }
  }

  /**
   * After the last repair the two clusters are configured identically, which is asserted here, so
   * whatever still differs is history: the assignment store and the current states. Counted per
   * resource, split into the clique just repaired and every other clique.
   */
  static void compareWithTwin(Tally tally, String ctx, String prefix, String repairedTag,
      WagedFuzzSim sim, WagedFuzzSim twinSim, StepResult r, StepResult t) {
    if (!sim.inputsDigest().equals(twinSim.inputsDigest())) {
      tally.violation(ctx, prefix + " inputs differ from the twin after the repair: "
          + firstDifference(sim.inputsDigest(), twinSim.inputsDigest()));
      return;
    }
    tally.count(prefix + ".inputsIdentical");
    SubStep last = r.subSteps.get(r.subSteps.size() - 1);
    SubStep twinLast = t.subSteps.get(t.subSteps.size() - 1);
    int differing = 0;
    for (String resource : sim.resources.keySet()) {
      String mine = canonical(last.bestPossibleAfter.get(resource));
      String theirs = canonical(twinLast.bestPossibleAfter.get(resource));
      SimResource simResource = sim.resources.get(resource);
      boolean inRepaired = repairedTag.equals(simResource.tag);
      boolean same = mine.equals(theirs);
      differing += same ? 0 : 1;
      tally.count(prefix + "." + (inRepaired ? "repairedClique." : "otherCliques.")
          + (same ? "same" : "different"));
    }
    tally.count(prefix + ".recoveries." + (differing == 0 ? "identical" : "differing"));
  }

  /** The isolation group key of a resource, exactly as the algorithm names it. */
  static String groupOf(SimResource r) {
    return r.tag == null ? "untagged-resource:" + r.name : "tag:" + r.tag;
  }

  /**
   * The share blocks the resource groups form on these nodes: two groups are in one block when a
   * node can host both, followed transitively. Written from the definition, independently of the
   * production union find.
   */
  static List<Set<String>> shareBlocks(WagedFuzzSim sim, Map<String, Set<String>> nodeTags) {
    Map<String, Set<String>> adjacency = new TreeMap<>();
    Set<String> untagged = new TreeSet<>();
    for (SimResource r : sim.resources.values()) {
      adjacency.putIfAbsent(groupOf(r), new TreeSet<>());
      if (r.tag == null) {
        untagged.add(groupOf(r));
      }
    }
    for (Set<String> tags : nodeTags.values()) {
      Set<String> reaching = new TreeSet<>(untagged);
      for (String tag : tags) {
        if (adjacency.containsKey("tag:" + tag)) {
          reaching.add("tag:" + tag);
        }
      }
      for (String a : reaching) {
        adjacency.get(a).addAll(reaching);
      }
    }
    List<Set<String>> blocks = new ArrayList<>();
    Set<String> seen = new TreeSet<>();
    for (String start : adjacency.keySet()) {
      if (!seen.add(start)) {
        continue;
      }
      Set<String> block = new TreeSet<>();
      List<String> frontier = new ArrayList<>(Collections.singletonList(start));
      while (!frontier.isEmpty()) {
        String g = frontier.remove(frontier.size() - 1);
        block.add(g);
        for (String next : adjacency.get(g)) {
          if (seen.add(next)) {
            frontier.add(next);
          }
        }
      }
      blocks.add(block);
    }
    return blocks;
  }

  /** Groups the generator cannot vouch for: broken on purpose, or without a wide margin. */
  static Set<String> suspectGroups(WagedFuzzSim sim) {
    Set<String> suspect = new TreeSet<>();
    for (String tag : sim.cliques) {
      if (sim.broken.containsKey(tag) || !sim.robust(tag)) {
        suspect.add("tag:" + tag);
      }
    }
    boolean untaggedOk = sim.untaggedRobust();
    for (SimResource r : sim.resources.values()) {
      if (r.tag == null && !untaggedOk) {
        suspect.add(groupOf(r));
      }
    }
    return suspect;
  }

  static Set<String> instancesOf(ResourceAssignment assignment) {
    Set<String> instances = new TreeSet<>();
    for (Partition p : assignment.getMappedPartitions()) {
      instances.addAll(assignment.getReplicaMap(p).keySet());
    }
    return instances;
  }

  static String canonical(ResourceAssignment assignment) {
    if (assignment == null) {
      return "null";
    }
    StringBuilder sb = new StringBuilder();
    WagedFuzzSim.json(sb, WagedFuzzSim.recordView(assignment.getRecord()));
    return sb.toString();
  }

  /**
   * Checks every sub step of one flag on step.
   *
   * @param asyncPartial whether the driver runs the partial rebalance asynchronously, which moves
   *                     the delayed rebalance overwrite onto the emergency result
   */
  static void checkStep(Tally tally, String ctx, WagedFuzzSim sim, StepResult result,
      boolean asyncPartial) {
    Set<String> suspect = suspectGroups(sim);
    for (SubStep sub : result.subSteps) {
      String subCtx = ctx + " sub=" + sub.name;
      Map<String, Outcome> calc = new HashMap<>();
      Map<String, Outcome> hook = new HashMap<>();
      for (Outcome o : sub.outcomes) {
        if ("GLOBAL_BASELINE".equals(o.scope)) {
          if (o.calculate) {
            tally.baselineAwaitingReport = o.failure == null;
          } else if (tally.baselineAwaitingReport) {
            tally.baselineAwaitingReport = false;
            tally.lastBaselineSkipped = o.skipped;
          } else {
            // With no baseline due, and only once the last one is done, the persisted baseline's
            // skips are reported again so a monitor reset cannot hide them. That must repeat
            // exactly what the last persisted baseline reported.
            if (o.skipped.equals(tally.lastBaselineSkipped) && o.evaluated.equals(o.skipped)) {
              tally.count("republish.GLOBAL_BASELINE");
            } else {
              tally.violation(subCtx, "GLOBAL_BASELINE republished " + o.skipped
                  + " but the last persisted baseline skipped " + tally.lastBaselineSkipped);
            }
            continue;
          }
        }
        Map<String, Outcome> target = o.calculate ? calc : hook;
        if (target.put(o.scope, o) != null) {
          tally.count("scope.repeatedInOneSubStep." + o.scope);
        }
      }
      if (sub.failure != null) {
        tally.violation(subCtx,
            "computeNewIdealStates threw " + WagedFuzzSim.describe(sub.failure));
      }
      // R1: a scope may only fail when no healthy block is left to rebalance around it.
      for (Outcome c : calc.values()) {
        if (c.failure == null) {
          tally.count("calc.ok." + c.scope);
          continue;
        }
        List<Set<String>> blocks = shareBlocks(sim, c.nodeTags);
        int healthy = 0;
        for (Set<String> block : blocks) {
          if (Collections.disjoint(block, suspect)) {
            healthy++;
          }
        }
        if (blocks.size() >= 2 && healthy > 0) {
          tally.violation(subCtx, "R1 " + c.scope + " threw with " + healthy + " healthy of "
              + blocks.size() + " block(s), suspect " + suspect + ": " + c.failure);
        } else {
          tally.count("R1.rethrowJustified." + c.scope + (blocks.size() < 2 ? ".oneBlock"
              : ".everyBlockSuspect"));
        }
      }
      for (Outcome h : hook.values()) {
        Outcome c = calc.get(h.scope);
        if (c == null) {
          if (!h.skipped.isEmpty()) {
            tally.violation(subCtx, h.scope + " reported skips without a calculation");
          }
          continue;
        }
        if (!h.skipped.isEmpty()) {
          tally.count("skipping." + h.scope);
          tally.add("skipped.resources." + h.scope, h.skipped.size());
        }
        countBrokenSkips(tally, sim, h);
        countCollisionYields(tally, subCtx, sim, h, c);
        Map<String, ResourceAssignment> output = scopeOutput(h.scope, sub, calc);
        if (output == null) {
          continue;
        }
        tally.count("checked." + h.scope);
        for (SimNode n : sim.nodes.values()) {
          if (n.config() != null && n.operation() == InstanceOperation.EVACUATE) {
            tally.count("checked.withEvacuatingNode." + h.scope);
            break;
          }
        }
        checkCarryForward(tally, subCtx, h, c, output);
        checkNoSharedInstance(tally, subCtx, h, output);
        checkFreshEntries(tally, subCtx, sim, h, c, output);
      }
      Outcome overwrite = hook.get(DELAYED);
      Outcome overwriteCalc = calc.get(DELAYED);
      Map<String, Long> counted =
          asyncPartial && overwriteCalc != null ? new TreeMap<>(tally.counts) : null;
      if (overwrite != null && overwriteCalc != null && overwriteCalc.failure == null
          && overwriteCalc.storeBestPossibleAtStart != null && sub.served != null) {
        countDelayedCoverage(tally, sim, result.event, overwrite, suspect,
            overwriteCalc.estimatedRemaining.get(WagedFuzzSim.DISK));
        checkDelayedOverwrite(tally, subCtx, sim, sub, overwriteCalc, overwrite, suspect);
      }
      if (overwrite != null && !overwrite.skipped.isEmpty() && sub.idealStates != null
          && overwriteCalc != null && overwriteCalc.storeBestPossibleAtStart != null) {
        for (String r : overwrite.skipped) {
          IdealState is = sub.idealStates.get(r);
          // The overwrite's input: the best possible the store holds when it runs.
          ResourceAssignment before = overwriteCalc.storeBestPossibleAtStart.get(r);
          if (is == null || before == null) {
            continue;
          }
          for (Partition p : before.getMappedPartitions()) {
            Set<String> expected = new TreeSet<>(before.getReplicaMap(p).keySet());
            List<String> list = is.getPreferenceList(p.getPartitionName());
            Set<String> emitted =
                list == null ? Collections.emptySet() : new TreeSet<>(list);
            if (!expected.equals(emitted)) {
              tally.violation(subCtx, "overwrite changed skipped " + r + " " + p + " from "
                  + expected + " to " + emitted);
            }
          }
          tally.count("R2.overwriteSkipKept");
        }
      }
      if (counted != null) {
        tally.countAgain(counted, ASYNC_PARTIAL_SITE);
      }
    }
  }

  /** What each scope's own calculation emitted, as far as the store makes it observable. */
  static Map<String, ResourceAssignment> scopeOutput(String scope, SubStep sub,
      Map<String, Outcome> calc) {
    switch (scope) {
      case "GLOBAL_BASELINE":
        return sub.baselineAfter;
      case "PARTIAL": {
        // Handed to the in memory cache when it changed; otherwise the store already held it.
        if (sub.partialCacheUpdate != null) {
          return sub.partialCacheUpdate;
        }
        Outcome partial = calc.get("PARTIAL");
        return partial == null ? null : partial.storeBestPossibleAtStart;
      }
      case "EMERGENCY": {
        // Persisted, then found in the store by the partial rebalance that follows.
        Outcome partial = calc.get("PARTIAL");
        return partial == null ? null : partial.storeBestPossibleAtStart;
      }
      default:
        return null;
    }
  }

  /**
   * A resource the scope reported as skipped although its calculation placed it gave its fresh
   * result up because a carried assignment named one of its nodes. Counted, not asserted: the yield
   * is by design, and these counts measure how often and how long it holds a healthy clique back.
   */
  static void countCollisionYields(Tally tally, String ctx, WagedFuzzSim sim, Outcome h,
      Outcome c) {
    if (c.algorithmSkipped == null) {
      return;
    }
    Set<String> yielded = new TreeSet<>(h.skipped);
    yielded.removeAll(c.algorithmSkipped);
    if (yielded.isEmpty()) {
      return;
    }
    tally.count("yield." + h.scope);
    tally.add("yield.resources." + h.scope, yielded.size());
    for (String name : yielded) {
      SimResource r = sim.resources.get(name);
      if (r != null && r.tag != null && !sim.broken.containsKey(r.tag)) {
        tally.count("yield.healthyCliqueResources." + h.scope);
        tally.example("yield.healthyClique." + h.scope, ctx + " resource=" + name);
        tally.yieldedThisStep.add(r.tag);
      }
    }
  }

  static void countBrokenSkips(Tally tally, WagedFuzzSim sim, Outcome h) {
    for (String tag : sim.broken.keySet()) {
      for (SimResource r : sim.resourcesWithTag(tag)) {
        if (h.evaluated.contains(r.name)) {
          tally.count("R5.brokenEvaluated." + h.scope
              + (h.skipped.contains(r.name) ? ".skipped" : ".notSkipped"));
        }
      }
    }
  }

  /** R2: a skipped resource is emitted exactly as the scope's previous assignment had it. */
  static void checkCarryForward(Tally tally, String ctx, Outcome h, Outcome c,
      Map<String, ResourceAssignment> output) {
    for (String r : h.skipped) {
      ResourceAssignment previous = c.previous.get(r);
      ResourceAssignment emitted = output.get(r);
      if (previous == null) {
        if (emitted != null && !emitted.getMappedPartitions().isEmpty()) {
          tally.violation(ctx, "R2 " + h.scope + " emitted skipped " + r
              + " that its previous assignment lacked: " + canonical(emitted));
        } else {
          tally.count("R2.droppedAbsent." + h.scope);
        }
        continue;
      }
      if (!canonical(previous).equals(canonical(emitted))) {
        tally.violation(ctx, "R2 " + h.scope + " carried " + r + " as " + canonical(emitted)
            + " but its previous assignment was " + canonical(previous));
      } else {
        tally.count("R2.carriedIdentical." + h.scope);
      }
    }
  }

  /** R3: no instance is named both by a carried over resource and by a fresh one. */
  static void checkNoSharedInstance(Tally tally, String ctx, Outcome h,
      Map<String, ResourceAssignment> output) {
    if (h.skipped.isEmpty()) {
      return;
    }
    Map<String, String> carriedOn = new TreeMap<>();
    for (String r : h.skipped) {
      ResourceAssignment a = output.get(r);
      if (a != null) {
        for (String instance : instancesOf(a)) {
          carriedOn.putIfAbsent(instance, r);
        }
      }
    }
    for (Map.Entry<String, ResourceAssignment> e : output.entrySet()) {
      if (h.skipped.contains(e.getKey())) {
        continue;
      }
      for (String instance : instancesOf(e.getValue())) {
        if (carriedOn.containsKey(instance)) {
          tally.violation(ctx, "R3 " + h.scope + " instance " + instance + " hosts carried "
              + carriedOn.get(instance) + " and fresh " + e.getKey());
        }
      }
    }
    tally.count("R3.checked." + h.scope);
  }

  /**
   * R4: every resource the scope computed afresh is emitted with exactly the replicas its model
   * holds, and every replica the scope placed itself sits on a node with the tag. A node the scope
   * placed a replica on stays within capacity. The baseline model must also hold every replica
   * the resource defines. Replicas pre-loaded where they already were are exempt from the tag and
   * capacity checks, exactly as the default mode never re-checks them.
   */
  static void checkFreshEntries(Tally tally, String ctx, WagedFuzzSim sim, Outcome h, Outcome c,
      Map<String, ResourceAssignment> output) {
    boolean baseline = "GLOBAL_BASELINE".equals(h.scope);
    Set<String> carriedInstances = new TreeSet<>();
    for (String r : h.skipped) {
      ResourceAssignment a = output.get(r);
      if (a != null) {
        carriedInstances.addAll(instancesOf(a));
      }
    }
    Map<String, Long> load = new TreeMap<>();
    Set<String> placedOn = new TreeSet<>();
    Set<String> evacuating = new TreeSet<>();
    for (SimNode n : sim.nodes.values()) {
      if (n.config() != null && n.operation() == InstanceOperation.EVACUATE) {
        evacuating.add(n.name);
      }
    }
    for (Map.Entry<String, ResourceAssignment> e : output.entrySet()) {
      String r = e.getKey();
      if (h.skipped.contains(r)) {
        continue;
      }
      SimResource resource = sim.resources.get(r);
      if (resource == null) {
        tally.count("R4.deletedResourceInStore." + h.scope);
        continue;
      }
      ResourceAssignment a = e.getValue();
      Map<String, Map<String, Integer>> model = c.modelReplicas.get(r);
      Map<String, Map<String, Integer>> emitted = new TreeMap<>();
      for (Partition p : a.getMappedPartitions()) {
        Map<String, Integer> counts = new TreeMap<>();
        for (String state : a.getReplicaMap(p).values()) {
          counts.merge(state, 1, Integer::sum);
        }
        if (!counts.isEmpty()) {
          emitted.put(p.getPartitionName(), counts);
        }
      }
      if (model == null || !model.equals(emitted)) {
        tally.violation(ctx, "R4 " + h.scope + " " + r + " emitted " + emitted
            + " but its model held " + model);
        continue;
      }
      if (baseline) {
        boolean whole = model.keySet().equals(new TreeSet<>(resource.partitionNames()));
        for (String partition : model.keySet()) {
          whole &= statesValid(resource, a.getReplicaMap(new Partition(partition)));
        }
        if (!whole) {
          tally.violation(ctx, "R4 " + h.scope + " " + r + " baseline model " + model
              + " is not the whole of " + resource.replicas + " " + resource.stateModel
              + " replica(s) on " + resource.partitionNames());
          continue;
        }
      }
      Map<String, Set<String>> preloaded = c.allocated.getOrDefault(r, Collections.emptyMap());
      for (Partition p : a.getMappedPartitions()) {
        String partition = p.getPartitionName();
        for (String instance : a.getReplicaMap(p).keySet()) {
          if (evacuating.contains(instance)) {
            // An evacuating node is not assignable, so a fresh result must have moved off it.
            tally.violation(ctx, "R4 " + h.scope + " " + partition + " of fresh " + r
                + " on evacuating " + instance);
            continue;
          }
          Set<String> tags = c.nodeTags.get(instance);
          if (tags == null) {
            tally.violation(ctx, "R4 " + h.scope + " " + partition + " on " + instance
                + " which is not in the model");
            continue;
          }
          boolean placedNow =
              !preloaded.getOrDefault(partition, Collections.emptySet()).contains(instance);
          if (placedNow) {
            tally.count("R4.placedNow." + h.scope);
            placedOn.add(instance);
          }
          // A baseline re-places everything whenever a tag changes, so every replica must match.
          if ((placedNow || baseline) && resource.tag != null && !tags.contains(resource.tag)) {
            tally.violation(ctx, "R4 " + h.scope + " " + partition + " on " + instance
                + (placedNow ? " (placed now)" : " (kept)") + " which lacks tag " + resource.tag
                + " " + tags);
          }
          load.merge(instance, (long) resource.weightOf(partition), Long::sum);
        }
      }
      tally.count("R4.freshComplete." + h.scope);
      if (!evacuating.isEmpty()) {
        tally.count("R4.freshCompleteWithEvacuatingNode." + h.scope);
      }
    }
    for (Map.Entry<String, Long> e : load.entrySet()) {
      String instance = e.getKey();
      if (!placedOn.contains(instance)) {
        tally.count("R4.capacityNotChecked.onlyKeptReplicas");
        continue;
      }
      if (carriedInstances.contains(instance)) {
        tally.count("R4.capacityNotChecked.hostsCarried");
        continue;
      }
      boolean inBroken = false;
      for (String tag : c.nodeTags.getOrDefault(instance, Collections.emptySet())) {
        inBroken |= sim.broken.containsKey(tag);
      }
      if (inBroken) {
        tally.count("R4.capacityNotChecked.brokenClique");
        continue;
      }
      int capacity = c.nodeCapacity.getOrDefault(instance, 0);
      if (e.getValue() > capacity) {
        tally.violation(ctx, "R4 " + h.scope + " " + instance + " holds " + e.getValue()
            + " over capacity " + capacity);
      } else {
        tally.count("R4.capacityOk");
      }
    }
  }

  static boolean statesValid(SimResource resource, Map<String, String> states) {
    Map<String, Integer> counts = new TreeMap<>();
    for (String state : states.values()) {
      counts.merge(state, 1, Integer::sum);
    }
    Map<String, Integer> expected = new TreeMap<>();
    switch (resource.stateModel) {
      case "MasterSlave":
        expected.put("MASTER", 1);
        if (resource.replicas > 1) {
          expected.put("SLAVE", resource.replicas - 1);
        }
        break;
      case "LeaderStandby":
        expected.put("LEADER", 1);
        if (resource.replicas > 1) {
          expected.put("STANDBY", resource.replicas - 1);
        }
        break;
      default:
        expected.put("ONLINE", resource.replicas);
    }
    return expected.equals(counts);
  }

  /** R6: once the last broken clique is repaired, nothing is skipped and the gauge clears. */
  static void checkRecovery(Tally tally, String ctx, StepResult r, Driver on) {
    tally.count("R6.recoveries");
    boolean clean = true;
    for (SubStep sub : r.subSteps) {
      for (Outcome o : sub.outcomes) {
        if (o.calculate && o.failure != null) {
          clean = false;
          tally.violation(ctx, "R6 " + o.scope + " still fails after repair: " + o.failure);
        }
        if (!o.calculate && !o.skipped.isEmpty()) {
          clean = false;
          tally.violation(ctx, "R6 " + o.scope + " still skips " + o.skipped + " after repair");
        }
      }
    }
    long gauge = on.monitor.getWagedInstanceTagIsolationSkippedResourcesGauge();
    if (gauge != 0) {
      clean = false;
      tally.violation(ctx, "R6 skipped resources gauge is " + gauge + " after repair");
    }
    if (clean) {
      tally.count("R6.recoveredCleanly");
    }
  }

  // ---------------------------------------------------------------------------------------------
  // The delayed rebalance overwrite, which is served and never persisted
  // ---------------------------------------------------------------------------------------------

  static Outcome calculation(SubStep sub, String scope) {
    for (Outcome o : sub.outcomes) {
      if (o.calculate && scope.equals(o.scope)) {
        return o;
      }
    }
    return null;
  }

  /** The nodes the delayed rebalance overwrite places on: live, with a config, and enabled. */
  static Set<String> liveEnabled(WagedFuzzSim sim) {
    Set<String> nodes = new TreeSet<>();
    for (SimNode n : sim.nodes.values()) {
      if (n.healthy()) {
        nodes.add(n.name);
      }
    }
    return nodes;
  }

  /**
   * How a node that serves nothing now is still counted on by the delayed window: "offline" or
   * "disabled" while its window is open, otherwise null. Follows how the rebalancer finds its
   * active nodes, so an evacuating or unknown node, or one without a config, is never counted on.
   */
  static String awayInWindow(WagedFuzzSim sim, SimNode n) {
    InstanceOperation operation = n.operation();
    if (!sim.clusterConfig.isDelayRebalaceEnabled() || n.healthy()
        || (operation != InstanceOperation.ENABLE && operation != InstanceOperation.DISABLE)) {
      return null;
    }
    long until = Long.MAX_VALUE;
    if (!n.isLive() && n.offlineTime > 0) {
      until = n.offlineTime + WagedFuzzSim.DELAY_MS;
    }
    if (operation == InstanceOperation.DISABLE) {
      long disabledAt = n.config().getRecord().getLongField(
          InstanceConfig.InstanceConfigProperty.HELIX_ENABLED_TIMESTAMP.name(), -1L);
      if (disabledAt > 0) {
        until = Math.min(until, disabledAt + WagedFuzzSim.DELAY_MS);
      }
    }
    if (until == Long.MAX_VALUE || until <= System.currentTimeMillis()) {
      return null;
    }
    return n.isLive() ? "disabled" : "offline";
  }

  /**
   * Counts what an overwrite that calculated covered: (a) a healthy clique topped up while one of
   * its own nodes is offline or disabled inside the window, (b) a broken clique at the same time,
   * split by whether the model's cluster wide estimate still fits, and (c) nodes disabled,
   * evacuating or unknown while another is offline inside the window.
   *
   * @param remaining the model's cluster wide estimate, or null when it was not captured
   */
  static void countDelayedCoverage(Tally tally, WagedFuzzSim sim, String event, Outcome h,
      Set<String> suspect, Long remaining) {
    Set<String> offlineTags = new TreeSet<>();
    Set<String> disabledTags = new TreeSet<>();
    Set<InstanceOperation> unavailable = new TreeSet<>();
    boolean offline = false;
    for (SimNode n : sim.nodes.values()) {
      String away = awayInWindow(sim, n);
      if ("offline".equals(away)) {
        offline = true;
        offlineTags.addAll(n.tags());
      } else if ("disabled".equals(away)) {
        disabledTags.addAll(n.tags());
      }
      if (n.operation() != null && n.operation() != InstanceOperation.ENABLE) {
        unavailable.add(n.operation());
      }
    }
    boolean offlineTopUp = false;
    boolean disabledTopUp = false;
    boolean brokenNeedsTopUp = false;
    for (String name : h.evaluated) {
      SimResource r = sim.resources.get(name);
      if (r == null || r.tag == null) {
        continue;
      }
      if (sim.broken.containsKey(r.tag)) {
        brokenNeedsTopUp = true;
      } else if (!h.skipped.contains(name) && !suspect.contains(groupOf(r))) {
        offlineTopUp |= offlineTags.contains(r.tag);
        disabledTopUp |= disabledTags.contains(r.tag);
      }
    }
    tally.count("delayed.calculated");
    if (offlineTopUp) {
      tally.count("delayed.healthyTopUp.nodeOfflineInWindow");
    }
    if (disabledTopUp) {
      tally.count("delayed.healthyTopUp.nodeDisabledInWindow");
    }
    if (!sim.broken.isEmpty()) {
      String kind = remaining == null ? "" : remaining < 0 ? ".clusterWideDeficit" : ".tagLocal";
      tally.count("delayed.withBrokenClique" + kind);
      if (brokenNeedsTopUp) {
        tally.count("delayed.withBrokenClique" + kind + ".brokenNeedsTopUp");
      }
      if (!h.skipped.isEmpty()) {
        tally.count("delayed.withBrokenClique" + kind + ".skipping");
      }
      if (offlineTopUp) {
        tally.count("delayed.healthyTopUp.nodeOfflineInWindow.withBrokenClique" + kind);
      }
      if (disabledTopUp) {
        tally.count("delayed.healthyTopUp.nodeDisabledInWindow.withBrokenClique" + kind);
      }
    }
    if (offline) {
      for (InstanceOperation operation : unavailable) {
        tally.count("delayed.nodeOfflineInWindowWith." + operation);
      }
      for (String kind : new String[] {"DISABLE", "EVACUATE", "UNKNOWN"}) {
        if (event.startsWith(kind + " ") || event.contains(" with " + kind + " ")) {
          tally.count("delayed.nodeOfflineInWindowWithEvent." + kind);
        }
      }
    }
  }

  /**
   * R7 to R9 on one delayed rebalance overwrite. It starts from the best possible assignment the
   * store holds right then, which is exactly its input at either call site, since the emergency
   * rebalance persists the map it hands over: its own result before an asynchronous partial
   * rebalance, or the stored map after a synchronous one. All it may do is add the replicas that
   * bring a partition back to its minimum on live enabled nodes.
   *
   * R7: a resource it skipped or had nothing to add to is served exactly as its input. Any other
   * keeps every replica of its input and gains exactly the replicas its model had to place, each
   * on a node of the model with the resource's tag. Only blocks holding a suspect group are
   * skipped, always whole, and a resource whose top up no node of the model could hold even empty
   * is always skipped. A broken clique the overwrite can still place is topped up like any other,
   * since the overwrite carries only what it cannot place.
   * R8: every resource it did not skip ends with each partition at its minimum on live enabled
   * nodes, and the healthy ones match the default mode; see checkDelayedAgainstDefault and
   * checkDelayedTwin.
   * R9: see checkDelayedCapacity.
   */
  static void checkDelayedOverwrite(Tally tally, String ctx, WagedFuzzSim sim, SubStep sub,
      Outcome c, Outcome h, Set<String> suspect) {
    String scope = h.scope;
    tally.count("checked." + scope);
    Map<String, ResourceAssignment> input =
        WagedFuzzSim.copyAssignments(c.storeBestPossibleAtStart);
    input.keySet().retainAll(sim.resources.keySet());
    Set<String> live = liveEnabled(sim);
    if (!c.nodeTags.keySet().equals(live)) {
      tally.violation(ctx, "R7 " + scope + " model holds " + c.nodeTags.keySet()
          + " rather than the live enabled nodes " + live);
      return;
    }
    if (!h.evaluated.equals(c.toAssign.keySet())) {
      tally.violation(ctx, "R7 " + scope + " reported " + h.evaluated
          + " as evaluated but its model had replicas to place for " + c.toAssign.keySet());
    }
    Set<String> placedOn = new TreeSet<>();
    for (SimResource resource : sim.resources.values()) {
      boolean skipped = h.skipped.contains(resource.name);
      Map<String, Map<String, Integer>> toAssign = c.toAssign.get(resource.name);
      String kind = skipped ? "skipped" : toAssign == null ? "untouched" : "toppedUp";
      String problem = servedProblem(resource, input.get(resource.name),
          sub.served.get(resource.name), skipped ? null : toAssign, c.nodeTags, placedOn);
      if (problem != null) {
        tally.violation(ctx, "R7 " + scope + " " + kind + " " + resource.name + " " + problem);
      } else {
        tally.count("R7." + kind + ("toppedUp".equals(kind) ? ".exactly" : ".servedAsInput"));
      }
      if (toAssign != null && resource.tag != null && sim.broken.containsKey(resource.tag)) {
        tally.count("R7.brokenCliqueTopUp." + kind);
      }
    }

    Set<String> skippable = new TreeSet<>();
    for (Set<String> block : shareBlocks(sim, c.nodeTags)) {
      Set<String> members = new TreeSet<>();
      for (SimResource resource : sim.resources.values()) {
        if (block.contains(groupOf(resource))) {
          members.add(resource.name);
        }
      }
      if (!Collections.disjoint(block, suspect)) {
        skippable.addAll(members);
      }
      Set<String> skippedMembers = new TreeSet<>(members);
      skippedMembers.retainAll(h.skipped);
      if (!skippedMembers.isEmpty() && !skippedMembers.equals(members)) {
        members.removeAll(skippedMembers);
        tally.violation(ctx, "R7 " + scope + " skipped " + skippedMembers + " but not " + members
            + " of the same block " + block);
      }
    }
    Set<String> healthySkipped = new TreeSet<>(h.skipped);
    healthySkipped.removeAll(skippable);
    if (!healthySkipped.isEmpty()) {
      tally.violation(ctx, "R7 " + scope + " skipped " + healthySkipped
          + " outside every block with a suspect group, suspect " + suspect);
    }
    for (Map.Entry<String, Map<String, Map<String, Integer>>> e : c.toAssign.entrySet()) {
      SimResource resource = sim.resources.get(e.getKey());
      String unplaceable = resource == null ? null
          : unplaceableTopUp(resource, input.get(resource.name), e.getValue(), c);
      if (unplaceable == null) {
        continue;
      }
      if (h.skipped.contains(resource.name)) {
        tally.count("R7.unplaceableTopUpSkipped");
      } else {
        tally.violation(ctx, "R7 " + scope + " did not skip " + resource.name + " although "
            + unplaceable);
      }
    }

    for (SimResource resource : sim.resources.values()) {
      ResourceAssignment in = input.get(resource.name);
      ResourceAssignment out = sub.served.get(resource.name);
      if (in == null || out == null) {
        continue;
      }
      List<String> below = new ArrayList<>();
      for (Partition p : in.getMappedPartitions()) {
        int onLive = 0;
        for (String instance : out.getReplicaMap(p).keySet()) {
          onLive += live.contains(instance) ? 1 : 0;
        }
        if (onLive < resource.requiredActive()) {
          below.add(p.getPartitionName() + "=" + onLive);
        }
      }
      if (below.isEmpty()) {
        continue;
      }
      if (h.skipped.contains(resource.name)) {
        tally.count("R8.skippedLeftBelowMinimum");
      } else {
        tally.violation(ctx, "R8 " + scope + " left " + resource.name + " below its minimum of "
            + resource.requiredActive() + " live enabled replica(s): " + below);
      }
    }
    checkDelayedAgainstDefault(tally, ctx, sim, sub, c, h, input);
    checkDelayedTwin(tally, ctx, sim, sub, c, h, suspect, input);
    checkDelayedCapacity(tally, ctx, sim, sub, suspect, c, placedOn);
  }

  /**
   * The first way the overwrite served this resource other than R7 allows, or null.
   *
   * @param toAssign what the model had to place for it, or null when it must be served as its
   *                 input: skipped, or nothing to add
   */
  static String servedProblem(SimResource resource, ResourceAssignment in, ResourceAssignment out,
      Map<String, Map<String, Integer>> toAssign, Map<String, Set<String>> nodeTags,
      Set<String> placedOn) {
    if (toAssign == null) {
      return canonical(in).equals(canonical(out)) ? null
          : "served " + canonical(out) + " but its input was " + canonical(in);
    }
    if (in == null || out == null) {
      return "had replicas to place but input " + canonical(in) + " and output "
          + canonical(out);
    }
    Set<String> partitions = new TreeSet<>(toAssign.keySet());
    for (Partition p : in.getMappedPartitions()) {
      partitions.add(p.getPartitionName());
    }
    for (Partition p : out.getMappedPartitions()) {
      partitions.add(p.getPartitionName());
    }
    for (String partition : partitions) {
      Map<String, String> before = in.getReplicaMap(new Partition(partition));
      Map<String, String> after = out.getReplicaMap(new Partition(partition));
      for (Map.Entry<String, String> e : before.entrySet()) {
        if (!e.getValue().equals(after.get(e.getKey()))) {
          return partition + " lost or changed " + e + ": " + after;
        }
      }
      Map<String, Integer> added = new TreeMap<>();
      for (Map.Entry<String, String> e : after.entrySet()) {
        if (before.containsKey(e.getKey())) {
          continue;
        }
        Set<String> tags = nodeTags.get(e.getKey());
        if (tags == null || (resource.tag != null && !tags.contains(resource.tag))) {
          return partition + " " + e.getValue() + " placed on " + e.getKey() + " with tags "
              + tags;
        }
        added.merge(e.getValue(), 1, Integer::sum);
        placedOn.add(e.getKey());
      }
      Map<String, Integer> expected = toAssign.getOrDefault(partition, Collections.emptyMap());
      if (!added.equals(expected)) {
        return partition + " gained " + added + " but its model had " + expected + " to place";
      }
    }
    return null;
  }

  /**
   * Why no placement can give this resource the top up its model asks for, or null when one may:
   * a partition needs more replicas than the model has nodes with the resource's tag, without a
   * replica of that partition, and with the capacity to hold one even when empty.
   */
  static String unplaceableTopUp(SimResource resource, ResourceAssignment in,
      Map<String, Map<String, Integer>> toAssign, Outcome c) {
    for (Map.Entry<String, Map<String, Integer>> e : toAssign.entrySet()) {
      String partition = e.getKey();
      int needed = 0;
      for (int count : e.getValue().values()) {
        needed += count;
      }
      Set<String> holding = in == null ? Collections.emptySet()
          : in.getReplicaMap(new Partition(partition)).keySet();
      int weight = resource.weightOf(partition);
      int candidates = 0;
      for (Map.Entry<String, Set<String>> node : c.nodeTags.entrySet()) {
        if (!holding.contains(node.getKey())
            && (resource.tag == null || node.getValue().contains(resource.tag))
            && c.nodeCapacity.getOrDefault(node.getKey(), 0) >= weight) {
          candidates++;
        }
      }
      if (candidates < needed) {
        return partition + " needs " + needed + " more replica(s) of weight " + weight
            + " and only " + candidates + " node(s) of the model could hold one";
      }
    }
    return null;
  }

  /**
   * R8 against the default mode on the same cluster, with every resource the overwrite skipped
   * asking for no minimum. While the model's cluster wide estimate fits, skipping a block changes
   * nothing for the others: blocks share no node a replica may go to, the order replicas are
   * placed in compares each replica on its own, and every estimate the scores read is taken over
   * the whole population either way. So the overwrite must serve exactly what the default mode
   * serves. With a cluster wide deficit the default mode throws before placing anything, so there
   * is nothing to compare, and checkDelayedTwin covers that case.
   */
  static void checkDelayedAgainstDefault(Tally tally, String ctx, WagedFuzzSim sim, SubStep sub,
      Outcome c, Outcome h, Map<String, ResourceAssignment> input) {
    for (long remaining : c.estimatedRemaining.values()) {
      if (remaining < 0) {
        tally.count("R8.default.notComparable.clusterWideDeficit");
        return;
      }
    }
    WagedFuzzSim withoutSkipped = sim.copy();
    for (String name : h.skipped) {
      SimResource resource = withoutSkipped.resources.get(name);
      if (resource != null) {
        resource.minActive = 0;
      }
    }
    Map<String, ResourceAssignment> expected;
    try {
      expected = defaultOverwrite(withoutSkipped, input);
    } catch (HelixRebalanceException e) {
      tally.violation(ctx, "R8 " + h.scope + " the default mode threw where the overwrite placed"
          + " every resource it did not skip: " + WagedFuzzSim.describe(e));
      return;
    }
    for (String name : sim.resources.keySet()) {
      String mine = canonical(sub.served.get(name));
      String theirs = canonical(expected.get(name));
      if (!mine.equals(theirs)) {
        tally.violation(ctx, "R8 " + h.scope + " served " + name + " as " + mine
            + " but the default mode serves " + theirs);
        return;
      }
    }
    tally.count("R8.default.identical" + (h.skipped.isEmpty() ? "" : ".withSkips"));
  }

  /**
   * R8 against a twin cluster without the blocks that hold a suspect or skipped group: without
   * their resources and without every node that carries only their tags. Each healthy resource
   * must gain exactly the replicas, by state, that the default mode adds on the twin. Where they
   * go is counted, not asserted: the estimates every score reads are taken over the whole cluster,
   * so dropping a block moves them, and with them which node scores best. A resource with a
   * replica on a dropped node is left out, since the twin cannot count that replica as live.
   */
  static void checkDelayedTwin(Tally tally, String ctx, WagedFuzzSim sim, SubStep sub,
      Outcome c, Outcome h, Set<String> suspect, Map<String, ResourceAssignment> input) {
    Set<String> skippedGroups = new TreeSet<>();
    for (String name : h.skipped) {
      SimResource resource = sim.resources.get(name);
      if (resource != null) {
        skippedGroups.add(groupOf(resource));
      }
    }
    Set<String> dropped = new TreeSet<>();
    for (Set<String> block : shareBlocks(sim, c.nodeTags)) {
      if (!Collections.disjoint(block, suspect) || !Collections.disjoint(block, skippedGroups)) {
        dropped.addAll(block);
      }
    }
    if (dropped.isEmpty()) {
      tally.count("R8.twin.nothingToDrop");
      return;
    }
    WagedFuzzSim twin = sim.copy();
    twin.resources.values().removeIf(r -> dropped.contains(groupOf(r)));
    if (twin.resources.isEmpty()) {
      tally.count("R8.twin.nothingLeft");
      return;
    }
    Set<String> keptGroups = new TreeSet<>();
    for (SimResource r : twin.resources.values()) {
      keptGroups.add(groupOf(r));
    }
    Set<String> droppedNodes = new TreeSet<>();
    for (SimNode n : twin.nodes.values()) {
      boolean droppedTag = false;
      boolean keptTag = false;
      for (String tag : n.tags()) {
        droppedTag |= dropped.contains("tag:" + tag);
        keptTag |= keptGroups.contains("tag:" + tag);
      }
      if (droppedTag && !keptTag) {
        droppedNodes.add(n.name);
      }
    }
    twin.nodes.keySet().removeAll(droppedNodes);
    Map<String, ResourceAssignment> twinInput = WagedFuzzSim.copyAssignments(input);
    twinInput.keySet().retainAll(twin.resources.keySet());
    Map<String, ResourceAssignment> expected;
    try {
      expected = defaultOverwrite(twin, twinInput);
    } catch (HelixRebalanceException e) {
      tally.count("R8.twin.defaultFailed");
      tally.example("R8.twin.defaultFailed", ctx + " " + WagedFuzzSim.describe(e));
      return;
    }
    for (SimResource resource : twin.resources.values()) {
      ResourceAssignment in = input.get(resource.name);
      if (in != null && !Collections.disjoint(instancesOf(in), droppedNodes)) {
        tally.count("R8.twin.replicaOnDroppedNode");
        continue;
      }
      ResourceAssignment mine = sub.served.get(resource.name);
      ResourceAssignment theirs = expected.get(resource.name);
      Map<String, Map<String, Integer>> myTopUp = addedStates(in, mine);
      Map<String, Map<String, Integer>> theirTopUp = addedStates(in, theirs);
      if (!myTopUp.equals(theirTopUp)) {
        tally.violation(ctx, "R8 " + h.scope + " topped " + resource.name + " up by " + myTopUp
            + " but the default mode tops it up by " + theirTopUp + " on the twin without "
            + dropped);
        continue;
      }
      if (myTopUp.isEmpty()) {
        tally.count("R8.twin.healthy.noTopUp");
      } else if (canonical(mine).equals(canonical(theirs))) {
        tally.count("R8.twin.healthy.toppedUp.samePlacement");
      } else {
        tally.count("R8.twin.healthy.toppedUp.otherPlacement");
        tally.example("R8.twin.healthy.toppedUp.otherPlacement",
            ctx + " resource=" + resource.name);
      }
    }
  }

  /** Partition, state and count of the replicas the output holds on nodes the input did not. */
  static Map<String, Map<String, Integer>> addedStates(ResourceAssignment in,
      ResourceAssignment out) {
    Map<String, Map<String, Integer>> added = new TreeMap<>();
    if (out == null) {
      return added;
    }
    for (Partition p : out.getMappedPartitions()) {
      Map<String, String> before =
          in == null ? Collections.emptyMap() : in.getReplicaMap(p);
      for (Map.Entry<String, String> e : out.getReplicaMap(p).entrySet()) {
        if (!before.containsKey(e.getKey())) {
          added.computeIfAbsent(p.getPartitionName(), k -> new TreeMap<>())
              .merge(e.getValue(), 1, Integer::sum);
        }
      }
    }
    return added;
  }

  /**
   * The delayed rebalance overwrite the default mode serves for this cluster from this input,
   * computed the way the rebalancer does from a data provider freshly loaded with the cluster.
   */
  static Map<String, ResourceAssignment> defaultOverwrite(WagedFuzzSim sim,
      Map<String, ResourceAssignment> input) throws HelixRebalanceException {
    WagedFuzzSim.SimDataProvider provider = sim.dataProvider(null);
    Map<String, ResourceAssignment> served = WagedFuzzSim.copyAssignments(input);
    ClusterModel model = ClusterModelProvider.generateClusterModelForDelayedRebalanceOverwrites(
        provider, sim.resourceMap(), provider.getEnabledLiveInstances(),
        WagedFuzzSim.copyAssignments(input));
    if (model.getAssignableReplicaMap().isEmpty()) {
      return served;
    }
    Map<String, ResourceAssignment> assignment =
        WagedRebalanceUtil.calculateAssignment(model, FuzzRecordingAlgorithm.create());
    assignment.keySet().retainAll(model.getAssignableReplicaMap().keySet());
    DelayedRebalanceUtil.mergeAssignments(assignment, served);
    return served;
  }

  /**
   * R9: no live node ends over its capacity with the replicas it can host: those of a resource
   * with one of its tags, or untagged, that fit on it at all. A replica it cannot host is one a
   * retag left behind. A node the overwrite placed on must end within its capacity with everything
   * it holds. The one load over capacity allowed is one the overwrite served exactly as its input,
   * on a node it placed nothing on, in a block with a suspect group, whose carried replicas stay
   * where they are however weights and capacities change under them.
   */
  static void checkDelayedCapacity(Tally tally, String ctx, WagedFuzzSim sim, SubStep sub,
      Set<String> suspect, Outcome c, Set<String> placedOn) {
    Map<String, Long> total = new TreeMap<>();
    Map<String, Long> hostable = new TreeMap<>();
    Set<String> untaggedGroups = new TreeSet<>();
    Set<String> tagGroups = new TreeSet<>();
    for (SimResource resource : sim.resources.values()) {
      (resource.tag == null ? untaggedGroups : tagGroups).add(groupOf(resource));
      ResourceAssignment out = sub.served.get(resource.name);
      if (out == null) {
        continue;
      }
      for (Partition p : out.getMappedPartitions()) {
        long weight = resource.weightOf(p.getPartitionName());
        for (String instance : out.getReplicaMap(p).keySet()) {
          total.merge(instance, weight, Long::sum);
          SimNode n = sim.nodes.get(instance);
          if (n != null && weight <= n.capacity()
              && (resource.tag == null || n.tags().contains(resource.tag))) {
            hostable.merge(instance, weight, Long::sum);
          }
        }
      }
    }
    for (String instance : placedOn) {
      int capacity = c.nodeCapacity.getOrDefault(instance, 0);
      long load = total.getOrDefault(instance, 0L);
      if (load > capacity) {
        tally.violation(ctx, "R9 " + c.scope + " placed on " + instance + " and left it holding "
            + load + " over its capacity " + capacity);
      } else {
        tally.count("R9.placedOnWithinCapacity");
      }
    }
    Set<String> suspectBlocks = new TreeSet<>();
    for (Set<String> block : shareBlocks(sim, c.nodeTags)) {
      if (!Collections.disjoint(block, suspect)) {
        suspectBlocks.addAll(block);
      }
    }
    for (SimNode n : sim.nodes.values()) {
      if (!n.isLive() || n.config() == null) {
        continue;
      }
      long load = hostable.getOrDefault(n.name, 0L);
      if (load <= n.capacity()) {
        tally.count("R9.liveNodeWithinCapacity");
        continue;
      }
      Set<String> reaching = new TreeSet<>(untaggedGroups);
      for (String tag : n.tags()) {
        if (tagGroups.contains("tag:" + tag)) {
          reaching.add("tag:" + tag);
        }
      }
      if (!placedOn.contains(n.name) && !Collections.disjoint(reaching, suspectBlocks)) {
        tally.count("R9.suspectNodeOverCapacityAsInput");
      } else {
        tally.violation(ctx, "R9 " + c.scope + " serves " + n.name + " with " + load
            + " of replicas it can host, over its capacity " + n.capacity());
      }
    }
  }

  // ---------------------------------------------------------------------------------------------
  // Control for the twin comparison: the same schedule with harmless breaks and the flag off
  // ---------------------------------------------------------------------------------------------

  /**
   * Runs the break schedule with every break replaced by a harmless change to the same clique and
   * the flag off on both sides, so nothing is ever isolated or carried. How often this still ends
   * up different from its twin after the repair is how history dependent WAGED is on its own, the
   * yardstick for the R6 twin counts above.
   */
  @Test
  public void testTwinDriftControlWithHarmlessBreaks() throws Exception {
    Tally tally = new Tally("twin-control");
    for (long seed = _firstSeed; seed < _firstSeed + _seeds; seed++) {
      for (boolean async : BOTH_MODES) {
        for (boolean asyncPartial : BOTH_MODES) {
          runTwinControl(tally, seed, async, asyncPartial, _steps);
        }
      }
    }
    tally.finish();
  }

  static void runTwinControl(Tally tally, long seed, boolean async, boolean asyncPartial,
      int steps) throws Exception {
    WagedFuzzSim sim = WagedFuzzSim.generate(seed, WagedFuzzSim.Mode.BREAK);
    sim.collide = collide(seed);
    sim.softBreaks = true;
    try (Driver changed = new Driver(sim.clusterName, "changed", async, asyncPartial, null);
        Driver twin = new Driver(sim.clusterName, "twin", async, asyncPartial, null)) {
      boolean everFailed = false;
      for (int step = 0; step < steps; step++) {
        String event = step == 0 ? "INIT" : sim.nextEvent();
        String ctx = context(sim, async, asyncPartial, step, event);
        WagedFuzzSim twinSim = sim.twinView();
        StepResult r = changed.step(sim, step, event);
        StepResult t = twin.step(twinSim, step, event);
        tally.count("steps");
        everFailed |= anyCalculateFailure(r) || anyCalculateFailure(t);
        if (event.startsWith("REPAIR") && sim.broken.isEmpty()) {
          compareWithTwin(tally, ctx, "control.twin" + (everFailed ? ".afterAFailure" : ""),
              event.split(" ")[1], sim, twinSim, r, t);
        }
      }
    }
  }

  // ---------------------------------------------------------------------------------------------
  // Algorithm level differential: identical models, every replica to be placed
  // ---------------------------------------------------------------------------------------------

  @Test
  public void testAlgorithmLevelDifferential() throws Exception {
    Tally tally = new Tally("algorithm-differential");
    for (long seed = _firstSeed; seed < _firstSeed + _seeds; seed++) {
      for (WagedFuzzSim.Mode mode : new WagedFuzzSim.Mode[] {WagedFuzzSim.Mode.BREAK,
          WagedFuzzSim.Mode.GENERAL}) {
        runAlgorithmDifferential(tally, seed, mode, _steps * 2);
      }
    }
    tally.finish();
  }

  static void runAlgorithmDifferential(Tally tally, long seed, WagedFuzzSim.Mode mode, int steps)
      throws Exception {
    WagedFuzzSim sim = WagedFuzzSim.generate(seed, mode);
    sim.collide = mode == WagedFuzzSim.Mode.BREAK && collide(seed);
    try (Driver off = new Driver(sim.clusterName, "off", false, null);
        Driver on = new Driver(sim.clusterName, "on", false, FLAG_ON)) {
      for (int step = 0; step < steps; step++) {
        String event = step == 0 ? "INIT" : sim.nextEvent();
        String ctx = context(sim, false, false, step, event) + " level=algorithm";
        off.populate(sim);
        on.populate(sim);
        Object[] a = calculateFromScratch(off, sim);
        Object[] b = calculateFromScratch(on, sim);
        tally.count("models");
        compareAlgorithmResults(tally, ctx, sim, a, b);
      }
    }
  }

  /** Returns {assignment json, skipped, assignment map, failure} for a model built from scratch. */
  static Object[] calculateFromScratch(Driver driver, WagedFuzzSim sim) {
    ClusterModel model = ClusterModelProvider.generateClusterModelForBaseline(
        driver.dataProvider(), sim.resourceMap(), driver.dataProvider().getAssignableInstances(),
        Collections.emptyMap(), Collections.emptyMap());
    FuzzRecordingAlgorithm algorithm = FuzzRecordingAlgorithm.create();
    try {
      OptimalAssignment result = algorithm.calculate(model);
      Map<String, ResourceAssignment> assignment = result.getOptimalResourceAssignment();
      StringBuilder sb = new StringBuilder();
      WagedFuzzSim.json(sb, WagedFuzzSim.assignmentsView(assignment));
      return new Object[] {sb.toString(), new TreeSet<>(result.getSkippedResources()),
          assignment, null};
    } catch (HelixRebalanceException e) {
      StringBuilder sb = new StringBuilder();
      WagedFuzzSim.json(sb, WagedFuzzSim.describe(e));
      return new Object[] {null, null, null, sb.toString()};
    }
  }

  @SuppressWarnings("unchecked")
  static void compareAlgorithmResults(Tally tally, String ctx, WagedFuzzSim sim, Object[] off,
      Object[] on) {
    if (off[3] == null) {
      // The default mode placed everything, so the flag must not have changed a single byte.
      if (on[3] != null || !off[0].equals(on[0]) || !((Set<String>) on[1]).isEmpty()) {
        tally.violation(ctx, "flag on differs although flag off succeeded: skipped=" + on[1]
            + " failure=" + on[3]);
      } else {
        tally.count("offOk.onIdentical");
      }
      return;
    }
    Map<String, Set<String>> nodeTags = new TreeMap<>();
    for (WagedFuzzSim.SimNode n : sim.nodes.values()) {
      if (n.config() != null && n.operation() != null
          && (n.operation() == org.apache.helix.constants.InstanceConstants.InstanceOperation.ENABLE
          || n.operation()
          == org.apache.helix.constants.InstanceConstants.InstanceOperation.DISABLE)) {
        nodeTags.put(n.name, new TreeSet<>(n.tags()));
      }
    }
    List<Set<String>> blocks = shareBlocks(sim, nodeTags);
    Set<String> suspect = suspectGroups(sim);
    int healthy = 0;
    Set<String> suspectResources = new TreeSet<>();
    for (Set<String> block : blocks) {
      if (Collections.disjoint(block, suspect)) {
        healthy++;
      } else {
        for (SimResource r : sim.resources.values()) {
          if (block.contains(groupOf(r))) {
            suspectResources.add(r.name);
          }
        }
      }
    }
    if (on[3] != null) {
      if (!on[3].equals(off[3])) {
        tally.violation(ctx, "flag on threw differently: on=" + on[3] + " off=" + off[3]);
      } else if (blocks.size() >= 2 && healthy > 0) {
        tally.violation(ctx, "flag on rethrew with " + healthy + " healthy of " + blocks.size()
            + " block(s): " + on[3]);
      } else {
        tally.count("offFail.onRethrewIdentically" + (blocks.size() < 2 ? ".oneBlock"
            : ".everyBlockSuspect"));
      }
      return;
    }
    Set<String> skipped = (Set<String>) on[1];
    Map<String, ResourceAssignment> assignment = (Map<String, ResourceAssignment>) on[2];
    if (skipped.isEmpty()) {
      tally.violation(ctx, "flag on succeeded without skipping although flag off threw " + off[3]);
      return;
    }
    if (!suspectResources.containsAll(skipped)) {
      Set<String> wrong = new TreeSet<>(skipped);
      wrong.removeAll(suspectResources);
      tally.violation(ctx, "flag on skipped healthy resource(s) " + wrong);
    }
    tally.count("offFail.onIsolated");
    tally.add("offFail.onIsolated.skippedResources", skipped.size());
    // Every replica was to be placed, so the skipped resources must be absent and every other
    // resource complete, validly tagged and within capacity on every node it uses.
    Map<String, Long> load = new TreeMap<>();
    for (SimResource r : sim.resources.values()) {
      ResourceAssignment a = assignment.get(r.name);
      if (skipped.contains(r.name)) {
        if (a != null && !a.getMappedPartitions().isEmpty()) {
          tally.violation(ctx, "skipped " + r.name + " still emitted " + canonical(a));
        }
        continue;
      }
      if (a == null) {
        tally.violation(ctx, "fresh " + r.name + " missing from the result");
        continue;
      }
      for (String partition : r.partitionNames()) {
        Map<String, String> states = a.getReplicaMap(new Partition(partition));
        if (states.size() != r.replicas || !statesValid(r, states)) {
          tally.violation(ctx, partition + " emitted " + states);
          continue;
        }
        for (String instance : states.keySet()) {
          Set<String> tags = nodeTags.get(instance);
          if (tags == null || r.tag != null && !tags.contains(r.tag)) {
            tally.violation(ctx, partition + " placed on " + instance + " with tags " + tags);
          }
          load.merge(instance, (long) r.weightOf(partition), Long::sum);
        }
      }
    }
    for (Map.Entry<String, Long> e : load.entrySet()) {
      int capacity = sim.nodes.get(e.getKey()).capacity();
      if (e.getValue() > capacity) {
        tally.violation(ctx, e.getKey() + " holds " + e.getValue() + " over capacity " + capacity);
      }
    }
    tally.count("offFail.onIsolated.valid");
    if (Objects.equals(skipped, suspectResources)) {
      tally.count("offFail.onIsolated.skippedEverySuspectBlock");
    }
  }
}
