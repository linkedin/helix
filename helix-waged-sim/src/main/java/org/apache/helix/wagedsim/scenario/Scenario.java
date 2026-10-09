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
