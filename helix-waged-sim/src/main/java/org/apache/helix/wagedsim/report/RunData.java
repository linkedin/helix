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

import java.io.BufferedReader;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import com.fasterxml.jackson.core.type.TypeReference;
import org.apache.helix.wagedsim.run.RunOutput;
import org.apache.helix.wagedsim.util.Json;

/** The raw results of a run, read back for reporting. Reports never re-run anything. */
public class RunData {
  public final Path dir;
  public final Map<String, Object> summary;
  /** variant -> rounds in order. */
  public final Map<String, List<Map<String, Object>>> rounds = new LinkedHashMap<>();

  public RunData(Path dir) throws IOException {
    this.dir = dir;
    summary = Json.MAPPER.readValue(dir.resolve(RunOutput.RUN_JSON).toFile(),
        new TypeReference<Map<String, Object>>() {
        });
    if (!Files.exists(dir.resolve(RunOutput.ROUNDS))) {
      return;
    }
    try (BufferedReader reader = Files.newBufferedReader(dir.resolve(RunOutput.ROUNDS), StandardCharsets.UTF_8)) {
      String line;
      while ((line = reader.readLine()) != null) {
        if (line.trim().isEmpty()) {
          continue;
        }
        Map<String, Object> record = Json.MAPPER.readValue(line, new TypeReference<Map<String, Object>>() {
        });
        rounds.computeIfAbsent(String.valueOf(record.get("variant")), k -> new ArrayList<>()).add(record);
      }
    }
    rounds.values().forEach(list -> list.sort((a, b) ->
        Integer.compare(((Number) a.get("round")).intValue(), ((Number) b.get("round")).intValue())));
  }

  @SuppressWarnings("unchecked")
  public Map<String, Object> map(Object value) {
    return value instanceof Map ? (Map<String, Object>) value : Collections.emptyMap();
  }

  @SuppressWarnings("unchecked")
  public List<Object> list(Object value) {
    return value instanceof List ? (List<Object>) value : Collections.emptyList();
  }

  public String focusKey() {
    Object key = summary.get("focusKey");
    return key == null ? null : key.toString();
  }

  public List<String> capacityKeys() {
    List<String> keys = new ArrayList<>();
    for (Object key : list(summary.get("capacityKeys"))) {
      keys.add(key.toString());
    }
    return keys;
  }

  public List<Map<String, Object>> variants() {
    List<Map<String, Object>> result = new ArrayList<>();
    for (Object variant : list(summary.get("variants"))) {
      result.add(map(variant));
    }
    return result;
  }

  /** The stats shown in tables and charts: the scenario's choice, or a default set. */
  public List<String> tableStats() {
    Map<String, Object> scenario = map(summary.get("scenario"));
    List<String> chosen = new ArrayList<>();
    for (Object stat : list(map(scenario.get("report")).get("stats"))) {
      chosen.add(stat.toString());
    }
    Set<String> stats = new LinkedHashSet<>();
    if (!chosen.isEmpty()) {
      stats.addAll(chosen);
    } else {
      String focus = focusKey();
      if (focus != null) {
        stats.addAll(Arrays.asList("skew.top." + focus, "skew.all." + focus, "maxUtil.top." + focus));
      }
      stats.addAll(Arrays.asList("skew.topCount", "moves.replicas", "moves.topState", "missingTopState",
          "underReplicated"));
      if (focus != null) {
        stats.add("yardstick.misrated");
      }
    }
    for (Object stat : list(map(scenario.get("exit")).get("conditionStats"))) {
      stats.add(stat.toString());
    }
    return new ArrayList<>(stats);
  }

  /** Reads one of the per-node CSV files, if it exists. */
  public List<Map<String, String>> nodes(String variant, int round) throws IOException {
    Path file = dir.resolve(RunOutput.nodesFile(variant, round));
    if (!Files.exists(file)) {
      return null;
    }
    List<String> lines = Files.readAllLines(file, StandardCharsets.UTF_8);
    List<Map<String, String>> rows = new ArrayList<>();
    if (lines.isEmpty()) {
      return rows;
    }
    List<String> header = parseCsv(lines.get(0));
    for (String line : lines.subList(1, lines.size())) {
      List<String> cells = parseCsv(line);
      Map<String, String> row = new LinkedHashMap<>();
      for (int i = 0; i < header.size() && i < cells.size(); i++) {
        row.put(header.get(i), cells.get(i));
      }
      rows.add(row);
    }
    return rows;
  }

  static List<String> parseCsv(String line) {
    List<String> cells = new ArrayList<>();
    StringBuilder cell = new StringBuilder();
    boolean quoted = false;
    for (int i = 0; i < line.length(); i++) {
      char c = line.charAt(i);
      if (quoted) {
        if (c == '"' && i + 1 < line.length() && line.charAt(i + 1) == '"') {
          cell.append('"');
          i++;
        } else if (c == '"') {
          quoted = false;
        } else {
          cell.append(c);
        }
      } else if (c == '"') {
        quoted = true;
      } else if (c == ',') {
        cells.add(cell.toString());
        cell.setLength(0);
      } else {
        cell.append(c);
      }
    }
    cells.add(cell.toString());
    return cells;
  }
}
