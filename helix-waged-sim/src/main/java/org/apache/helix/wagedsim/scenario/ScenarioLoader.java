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

import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.Reader;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;

import org.apache.helix.wagedsim.engine.EngineSettings;
import org.apache.helix.wagedsim.run.Condition;
import org.apache.helix.wagedsim.util.Durations;
import org.yaml.snakeyaml.Yaml;

/** Reads scenario YAML files and bundled presets ({@code preset:<name>}). */
public final class ScenarioLoader {
  public static final Set<String> EVENT_KINDS = Collections.unmodifiableSet(new LinkedHashSet<>(Arrays.asList(
      "disable", "enable", "setOperation", "kill", "revive", "restart", "addNode", "removeNode",
      "capacity", "weights", "addResource", "removeResource", "partitions", "replicas",
      "clusterConfig", "constraintWeights", "maintenance", "advanceClock", "restartController",
      "onDemandRebalance", "random")));
  public static final List<String> PRESETS =
      Collections.unmodifiableList(Arrays.asList("replay", "rca", "levers", "rehome", "converge", "restart-all"));
  private static final Set<String> TIMING = new LinkedHashSet<>(Arrays.asList("at", "every", "from", "times"));

  private ScenarioLoader() {
  }

  public static Scenario load(String pathOrPreset) throws IOException {
    Map<String, Object> map;
    if (pathOrPreset.startsWith("preset:")) {
      String name = pathOrPreset.substring("preset:".length());
      try (InputStream in = ScenarioLoader.class.getResourceAsStream("/presets/" + name + ".yaml")) {
        if (in == null) {
          throw new IOException("Unknown preset '" + name + "'; presets: " + PRESETS);
        }
        map = yaml(new InputStreamReader(in, StandardCharsets.UTF_8), pathOrPreset);
      }
    } else {
      Path path = Paths.get(pathOrPreset);
      try (Reader reader = Files.newBufferedReader(path, StandardCharsets.UTF_8)) {
        map = yaml(reader, pathOrPreset);
      }
    }
    return fromMap(map);
  }

  @SuppressWarnings("unchecked")
  private static Map<String, Object> yaml(Reader reader, String name) throws IOException {
    Object loaded = new Yaml().load(reader);
    if (!(loaded instanceof Map)) {
      throw new IOException(name + " does not contain a YAML map");
    }
    return (Map<String, Object>) loaded;
  }

  @SuppressWarnings("unchecked")
  public static Scenario fromMap(Map<String, Object> map) {
    Scenario scenario = new Scenario();
    scenario.source = map;
    for (String key : map.keySet()) {
      if (!Arrays.asList("name", "description", "mode", "focusKey", "seed", "settings", "clusterConfig",
          "variants", "events", "exit", "report").contains(key)) {
        throw new IllegalArgumentException("Unknown scenario field '" + key + "'");
      }
    }
    scenario.name = string(map.get("name"), scenario.name);
    scenario.description = string(map.get("description"), null);
    scenario.mode = string(map.get("mode"), scenario.mode).toLowerCase(Locale.ROOT);
    if (!scenario.mode.equals("dry-run") && !scenario.mode.equals("local")) {
      throw new IllegalArgumentException("mode must be dry-run or local");
    }
    scenario.focusKey = string(map.get("focusKey"), null);
    if (map.get("seed") != null) {
      scenario.seed = ((Number) map.get("seed")).longValue();
    }
    if (map.get("settings") != null) {
      settings(scenario.settings, (Map<String, Object>) map.get("settings"));
    }
    if (map.get("clusterConfig") != null) {
      scenario.clusterConfig = (Map<String, Object>) map.get("clusterConfig");
    }
    Map<String, Object> variants = map.get("variants") == null ? null : (Map<String, Object>) map.get("variants");
    if (variants == null || variants.isEmpty()) {
      Scenario.Variant variant = new Scenario.Variant();
      variant.name = "default";
      scenario.variants.put(variant.name, variant);
    } else {
      variants.forEach((name, value) -> scenario.variants.put(name,
          variant(name, value == null ? Collections.emptyMap() : (Map<String, Object>) value)));
    }
    if (map.get("events") != null) {
      for (Object item : (List<Object>) map.get("events")) {
        scenario.events.add(event((Map<String, Object>) item));
      }
    }
    if (map.get("exit") != null) {
      exit(scenario, (Map<String, Object>) map.get("exit"));
    }
    if (map.get("report") != null) {
      report(scenario.report, (Map<String, Object>) map.get("report"));
    }
    return scenario;
  }

  @SuppressWarnings("unchecked")
  static void settings(EngineSettings settings, Map<String, Object> map) {
    for (Map.Entry<String, Object> entry : map.entrySet()) {
      Object value = entry.getValue();
      switch (entry.getKey()) {
        case "pass":
          settings.pass = enumValue(EngineSettings.Pass.class, value);
          break;
        case "activeNodes":
          settings.activeNodes = enumValue(EngineSettings.ActiveNodes.class, value);
          break;
        case "firstRound":
          settings.firstRound = enumValue(EngineSettings.FirstRound.class, value);
          break;
        case "constraintWeights":
          settings.constraintWeights.putAll(weights((Map<String, Object>) value));
          break;
        case "local":
          local(settings, (Map<String, Object>) value);
          break;
        default:
          throw new IllegalArgumentException("Unknown settings field '" + entry.getKey() + "'");
      }
    }
  }

  private static void local(EngineSettings settings, Map<String, Object> map) {
    for (Map.Entry<String, Object> entry : map.entrySet()) {
      Object value = entry.getValue();
      switch (entry.getKey()) {
        case "timeCompression":
          settings.timeCompression = ((Number) value).longValue();
          break;
        case "roundTimeout":
          settings.roundTimeoutMillis = Durations.parseMillis(value);
          break;
        case "settleQuiet":
          settings.settleQuietMillis = Durations.parseMillis(value);
          break;
        case "transitionLatency":
          settings.transitionLatencyMillis = Durations.parseMillis(value);
          break;
        case "participants":
          settings.participants = value.toString();
          break;
        case "realParticipantLimit":
          settings.realParticipantLimit = ((Number) value).intValue();
          break;
        default:
          throw new IllegalArgumentException("Unknown settings.local field '" + entry.getKey() + "'");
      }
    }
  }

  public static Map<String, Float> weights(Map<String, Object> map) {
    Map<String, Float> weights = new LinkedHashMap<>();
    map.forEach((k, v) -> weights.put(EngineSettings.constraintName(k), Float.parseFloat(v.toString())));
    return weights;
  }

  @SuppressWarnings("unchecked")
  private static Scenario.Variant variant(String name, Map<String, Object> map) {
    Scenario.Variant variant = new Scenario.Variant();
    variant.name = name;
    variant.source = map;
    for (Map.Entry<String, Object> entry : map.entrySet()) {
      Object value = entry.getValue();
      switch (entry.getKey()) {
        case "constraintWeights":
          variant.constraintWeights.putAll(weights((Map<String, Object>) value));
          break;
        case "clusterConfig":
          variant.clusterConfig = (Map<String, Object>) value;
          break;
        case "pass":
          variant.pass = enumValue(EngineSettings.Pass.class, value);
          break;
        case "activeNodes":
          variant.activeNodes = enumValue(EngineSettings.ActiveNodes.class, value);
          break;
        case "firstRound":
          variant.firstRound = enumValue(EngineSettings.FirstRound.class, value);
          break;
        default:
          throw new IllegalArgumentException("Unknown field '" + entry.getKey() + "' in variant " + name);
      }
    }
    return variant;
  }

  private static EventSpec event(Map<String, Object> map) {
    EventSpec event = new EventSpec();
    event.source = map;
    List<String> kinds = new ArrayList<>();
    for (Map.Entry<String, Object> entry : map.entrySet()) {
      if (TIMING.contains(entry.getKey())) {
        continue;
      }
      if (!EVENT_KINDS.contains(entry.getKey())) {
        throw new IllegalArgumentException("Unknown event '" + entry.getKey() + "'; events: " + EVENT_KINDS);
      }
      kinds.add(entry.getKey());
    }
    if (kinds.size() != 1) {
      throw new IllegalArgumentException("Each event needs exactly one kind, got " + kinds + " in " + map);
    }
    event.kind = kinds.get(0);
    event.args = map.get(event.kind);
    if (map.get("at") != null) {
      event.at = ((Number) map.get("at")).intValue();
    } else if (map.get("every") != null) {
      event.every = ((Number) map.get("every")).intValue();
      if (map.get("from") != null) {
        event.from = ((Number) map.get("from")).intValue();
      }
      if (map.get("times") != null) {
        event.times = ((Number) map.get("times")).intValue();
      }
    } else {
      throw new IllegalArgumentException("Event needs 'at' or 'every': " + map);
    }
    if ((event.at != null && event.at < 1) || (event.every != null && event.every < 1)) {
      throw new IllegalArgumentException("Event rounds start at 1: " + map);
    }
    return event;
  }

  private static void exit(Scenario scenario, Map<String, Object> map) {
    for (Map.Entry<String, Object> entry : map.entrySet()) {
      Object value = entry.getValue();
      switch (entry.getKey()) {
        case "until":
          scenario.exit.until = Condition.parse(value.toString());
          break;
        case "failIf":
          scenario.exit.failIf = Condition.parse(value.toString());
          break;
        case "stableFor":
          scenario.exit.stableFor = ((Number) value).intValue();
          break;
        case "minRounds":
          scenario.exit.minRounds = ((Number) value).intValue();
          break;
        case "maxRounds":
          scenario.exit.maxRounds = ((Number) value).intValue();
          break;
        case "timeout":
          scenario.exit.timeoutMillis = Durations.parseMillis(value);
          break;
        case "maxSimTime":
          scenario.exit.maxSimTimeMillis = Durations.parseMillis(value);
          break;
        case "failOnRebalanceFailure":
          scenario.exit.failOnRebalanceFailure = Boolean.parseBoolean(value.toString());
          break;
        default:
          throw new IllegalArgumentException("Unknown exit field '" + entry.getKey() + "'");
      }
    }
  }

  @SuppressWarnings("unchecked")
  private static void report(Scenario.ReportSpec report, Map<String, Object> map) {
    for (Map.Entry<String, Object> entry : map.entrySet()) {
      Object value = entry.getValue();
      switch (entry.getKey()) {
        case "formats":
          report.formats = ClusterConfigOverrides.strings(value);
          break;
        case "stats":
          report.stats = ClusterConfigOverrides.strings(value);
          break;
        case "perNode":
          report.perNode = ClusterConfigOverrides.strings(value);
          break;
        case "top":
          report.top = ((Number) value).intValue();
          break;
        default:
          throw new IllegalArgumentException("Unknown report field '" + entry.getKey() + "'");
      }
    }
  }

  static String string(Object value, String defaultValue) {
    return value == null ? defaultValue : value.toString();
  }

  static <E extends Enum<E>> E enumValue(Class<E> type, Object value) {
    String text = value.toString().trim().toUpperCase(Locale.ROOT).replace('-', '_');
    try {
      return Enum.valueOf(type, text);
    } catch (IllegalArgumentException e) {
      throw new IllegalArgumentException("Invalid value '" + value + "' for " + type.getSimpleName()
          + "; use one of " + Arrays.toString(type.getEnumConstants()).toLowerCase(Locale.ROOT).replace('_', '-'));
    }
  }
}
