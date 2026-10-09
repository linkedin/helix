package org.apache.helix.wagedsim.scenario;

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
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.apache.helix.wagedsim.engine.EngineSettings;
import org.apache.helix.wagedsim.run.ExitCriteria;

/** A scenario: settings, variants, scheduled events, exit criteria and report options. */
public class Scenario {
  public String name = "scenario";
  public String description;
  public String mode = "dry-run";
  public String focusKey;
  public long seed = 7;
  public EngineSettings settings = new EngineSettings();
  public Map<String, Object> clusterConfig = new LinkedHashMap<>();
  public LinkedHashMap<String, Variant> variants = new LinkedHashMap<>();
  public List<EventSpec> events = new ArrayList<>();
  public ExitCriteria exit = new ExitCriteria();
  public ReportSpec report = new ReportSpec();
  /** When set, the scenario is a scale-down search instead of a sequence of rounds. */
  public SearchSpec search;
  /** The scenario as written, for the report. */
  public Map<String, Object> source = new LinkedHashMap<>();

  /** @return the last round with a scheduled event, or 0 */
  public int lastEventRound() {
    int last = 0;
    for (EventSpec event : events) {
      last = Math.max(last, event.lastRound());
    }
    return last;
  }

  /** One way to run the scenario. Unset fields keep the scenario's settings. */
  public static class Variant {
    public String name;
    public Map<String, Float> constraintWeights = new LinkedHashMap<>();
    public Map<String, Object> clusterConfig = new LinkedHashMap<>();
    public EngineSettings.Pass pass;
    public EngineSettings.ActiveNodes activeNodes;
    public EngineSettings.FirstRound firstRound;
    /** Removal order for a scale-down search; overrides the scenario's. */
    public RemovalOrder.Strategy searchStrategy;
    /** Zone-loss check for a scale-down search (largest, every or none); overrides the scenario's. */
    public String zoneLoss;
    public Map<String, Object> source = new LinkedHashMap<>();

    public EngineSettings apply(EngineSettings base) {
      EngineSettings settings = base.copy();
      settings.constraintWeights.putAll(constraintWeights);
      if (pass != null) {
        settings.pass = pass;
      }
      if (activeNodes != null) {
        settings.activeNodes = activeNodes;
      }
      if (firstRound != null) {
        settings.firstRound = firstRound;
      }
      return settings;
    }
  }

  /**
   * A scale-down search: the largest number of serving instances that can be removed, in a given
   * order, while WAGED still places every replica.
   */
  public static class SearchSpec {
    /**
     * No rebalance failure, and WAGED's assignment places every replica on an instance within capacity
     * with zones kept apart; ".added" counts only problems the probe that removes nothing did not have.
     */
    public static final String DEFAULT_FEASIBLE = "rebalanceFailures == 0 and unplacedReplicas.added == 0"
        + " and overCapacityInstances.added == 0 and zoneConflicts.added == 0";
    public RemovalOrder.Strategy strategy = RemovalOrder.Strategy.MZ_BALANCED;
    /** binary (fewest probes, assumes removing more never helps) or linear (every step from 0). */
    public String method = "binary";
    public int step = 1;
    /** Upper bound on instances to remove; default every serving instance. */
    public Integer max;
    public org.apache.helix.wagedsim.run.Condition feasibleIf =
        org.apache.helix.wagedsim.run.Condition.parse(DEFAULT_FEASIBLE);
    /** The search fails when fewer instances than this can be removed. */
    public Integer requireAtLeast;
    /**
     * Remove disabled and offline instances in every probe, before the search's own removals. By
     * default they stay, and WAGED counts them as it does in production: their capacity for the
     * baseline, and as active while within the delay window.
     */
    public boolean removeNonServing;
    /**
     * Also require that the cluster survives losing a whole fault zone after the removal (as if the
     * zone stayed down past the delay window): "largest" checks the zone with the most capacity,
     * "every" checks each zone, "none" skips the check.
     */
    public String zoneLoss = "none";

    public Map<String, Object> describe() {
      Map<String, Object> map = new LinkedHashMap<>();
      map.put("strategy", strategy.label());
      map.put("method", method);
      if ("linear".equals(method)) {
        map.put("step", step);
      }
      if (max != null) {
        map.put("max", max);
      }
      map.put("feasibleIf", feasibleIf.toString());
      map.put("nonServing", removeNonServing ? "remove" : "keep");
      map.put("tolerateZoneLoss", zoneLoss);
      if (requireAtLeast != null) {
        map.put("requireAtLeast", requireAtLeast);
      }
      return map;
    }
  }

  /** Report options. */
  public static class ReportSpec {
    public List<String> formats = new ArrayList<>(java.util.Arrays.asList("md", "html"));
    /** Stats shown in the per-round table; empty means the default set. */
    public List<String> stats = new ArrayList<>();
    /** Rounds with a per-node CSV: start, end, all or none. */
    public List<String> perNode = new ArrayList<>(java.util.Arrays.asList("start", "end"));
    public int top = 10;
  }
}
