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

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileOutputStream;
import java.io.OutputStreamWriter;
import java.io.Writer;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.zip.GZIPOutputStream;

import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.core.config.Configurator;
import org.testng.Assert;
import org.testng.annotations.Test;

/**
 * Seeded dump of every observable WagedRebalancer output across random event sequences, with the
 * cluster left at its defaults. The same source runs on a build without instance tag isolation,
 * so two dumps of the same seeds, one per build, must be byte identical.
 *
 * Each dumped step also holds the writes and deletes of every assignment metadata store path,
 * per call and in total, how each baseline and partial rebalance calculation ended, and whether
 * each update of the in memory best possible cache was accepted. About one step in five is a
 * FLAG_FORM step, which only moves the isolation flag field to another of
 * WagedFuzzSim.FLAG_OFF_FORMS, so both builds must also treat every form that reads as off alike.
 *
 * Properties: fuzz.firstSeed, fuzz.seeds, fuzz.steps, fuzz.out (a file to write), fuzz.full (also
 * write the canonical JSON of every step, gzipped), fuzz.async (baseline: sync, async or both),
 * fuzz.asyncPartial (partial rebalance: sync, async or both), fuzz.mode (GENERAL, FEASIBLE or
 * BREAK; the flag reads as off in every mode), fuzz.collide (BREAK also moves nodes out of broken
 * cliques).
 */
public class TestWagedFuzzParityDump {

  @Test
  public void testDumpIsDeterministic() throws Exception {
    long firstSeed = Long.getLong("fuzz.firstSeed", 1L);
    int seeds = Integer.getInteger("fuzz.seeds", 2);
    int steps = Integer.getInteger("fuzz.steps", 12);
    String out = System.getProperty("fuzz.out");
    boolean full = Boolean.getBoolean("fuzz.full");
    String asyncMode = System.getProperty("fuzz.async", "both");
    String asyncPartialMode = System.getProperty("fuzz.asyncPartial", "both");
    WagedFuzzSim.Mode mode = WagedFuzzSim.Mode.valueOf(System.getProperty("fuzz.mode", "GENERAL"));
    Configurator.setLevel("org.apache.helix", Level.OFF);
    long start = System.currentTimeMillis();
    int totalSteps = 0;
    Writer hashes = null;
    Writer fullOut = null;
    try {
      if (out != null) {
        hashes = new BufferedWriter(
            new OutputStreamWriter(new FileOutputStream(new File(out)), StandardCharsets.UTF_8));
        if (full) {
          fullOut = new BufferedWriter(new OutputStreamWriter(
              new GZIPOutputStream(new FileOutputStream(new File(out + ".full.gz"))),
              StandardCharsets.UTF_8));
        }
      }
      for (long seed = firstSeed; seed < firstSeed + seeds; seed++) {
        for (boolean async : new boolean[] {false, true}) {
          if (skip(async, asyncMode)) {
            continue;
          }
          for (boolean asyncPartial : new boolean[] {false, true}) {
            if (skip(asyncPartial, asyncPartialMode)) {
              continue;
            }
            List<String> first = run(seed, mode, steps, async, asyncPartial, fullOut);
            totalSteps += first.size();
            if (out == null) {
              // Without a peer to compare against, at least prove the dump is reproducible.
              List<String> second = run(seed, mode, steps, async, asyncPartial, null);
              Assert.assertEquals(second, first, "nondeterministic dump for seed " + seed);
            } else {
              for (String line : first) {
                hashes.write(line);
                hashes.write('\n');
              }
            }
          }
        }
      }
    } finally {
      if (hashes != null) {
        hashes.close();
      }
      if (fullOut != null) {
        fullOut.close();
      }
      Configurator.setLevel("org.apache.helix", Level.ERROR);
    }
    System.out.println("TestWagedFuzzParityDump: " + totalSteps + " steps in "
        + (System.currentTimeMillis() - start) + " ms " + STATS);
  }

  private static boolean skip(boolean async, String mode) {
    return async && "sync".equals(mode) || !async && "async".equals(mode);
  }

  /** Tallies of what the dumped steps exercised, so a vacuous run is visible. */
  static final java.util.Map<String, Integer> STATS = new java.util.TreeMap<>();

  private static void count(String key) {
    count(key, 1);
  }

  private static void count(String key, int n) {
    STATS.merge(key, n, Integer::sum);
  }

  static List<String> run(long seed, WagedFuzzSim.Mode mode, int steps, boolean async,
      boolean asyncPartial, Writer fullOut) throws Exception {
    WagedFuzzSim sim = WagedFuzzSim.generate(seed, mode);
    sim.collide = Boolean.getBoolean("fuzz.collide");
    sim.flagForms = true;
    List<String> lines = new ArrayList<>();
    try (WagedFuzzSim.Driver driver =
        new WagedFuzzSim.Driver(sim.clusterName, "default", async, asyncPartial, null)) {
      driver.keepFullJson = fullOut != null;
      String event = "INIT";
      for (int step = 0; step < steps; step++) {
        if (step > 0) {
          event = sim.nextEvent();
        }
        WagedFuzzSim.StepResult result = driver.step(sim, step, event);
        count("steps");
        count("event." + event.split(" ")[0]);
        if (event.startsWith("FLAG_FORM ")) {
          count("flagForm." + event.substring("FLAG_FORM ".length()));
        }
        for (WagedFuzzSim.SubStep sub : result.subSteps) {
          count(sub.failure == null ? "sub.ok" : "sub.throw");
          count(sub.idealStates == null || sub.idealStates.isEmpty() ? "sub.noIdealStates"
              : "sub.withIdealStates");
          for (org.apache.helix.controller.rebalancer.waged.constraints.FuzzRecordingAlgorithm
              .Outcome o : sub.outcomes) {
            if (o.calculate) {
              count("calc." + o.scope + (o.failure == null ? ".ok" : ".fail"));
            }
          }
          for (java.util.Map.Entry<String, Integer> w : sub.writes.entrySet()) {
            count("writes." + w.getKey().substring(w.getKey().lastIndexOf('/') + 1),
                w.getValue());
          }
          for (java.util.Map.Entry<String, Integer> d : sub.deletes.entrySet()) {
            count("deletes." + d.getKey().substring(d.getKey().lastIndexOf('/') + 1),
                d.getValue());
          }
          for (java.util.Map<String, Object> task : sub.baselineTasks) {
            count("baselineTask." + task.get("result"));
          }
          for (java.util.Map<String, Object> task : sub.partialTasks) {
            count("partialTask." + task.get("result"));
          }
          for (String update : sub.cacheUpdates) {
            count("cacheUpdate." + update);
          }
        }
        lines.add("mode=" + mode + " seed=" + seed + " async=" + async + " asyncPartial="
            + asyncPartial + " step=" + step + " sha=" + result.sha + " event=" + event);
        if (fullOut != null) {
          fullOut.write(result.json);
          fullOut.write('\n');
        }
      }
    }
    return lines;
  }
}
