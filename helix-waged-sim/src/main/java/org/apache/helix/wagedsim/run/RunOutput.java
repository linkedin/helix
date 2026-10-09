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

import java.io.BufferedWriter;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.apache.helix.wagedsim.stats.NodeStats;
import org.apache.helix.wagedsim.util.Json;

/** Writes the raw results of a run: run.json, rounds.jsonl and per-node CSV files. */
public class RunOutput {
  public static final String RUN_JSON = "run.json";
  public static final String ROUNDS = "rounds.jsonl";
  private final Path _dir;

  public RunOutput(Path dir) throws IOException {
    _dir = dir;
    Files.createDirectories(dir);
    Files.deleteIfExists(dir.resolve(ROUNDS));
    Files.createFile(dir.resolve(ROUNDS));
  }

  public Path dir() {
    return _dir;
  }

  public synchronized void round(Map<String, Object> record) throws IOException {
    try (BufferedWriter writer = Files.newBufferedWriter(_dir.resolve(ROUNDS), StandardCharsets.UTF_8,
        StandardOpenOption.CREATE, StandardOpenOption.APPEND)) {
      writer.write(Json.compact(record));
      writer.newLine();
    }
  }

  public static String nodesFile(String variant, int round) {
    return "nodes_" + safe(variant) + "_" + round + ".csv";
  }

  public void nodes(String variant, int round, Collection<NodeStats> nodes, List<String> keys)
      throws IOException {
    List<String> lines = new ArrayList<>();
    List<String> header = null;
    for (NodeStats node : nodes) {
      Map<String, Object> row = node.toRow(keys);
      if (header == null) {
        header = new ArrayList<>(row.keySet());
        lines.add(String.join(",", header));
      }
      List<String> cells = new ArrayList<>();
      for (String column : header) {
        cells.add(csv(row.get(column)));
      }
      lines.add(String.join(",", cells));
    }
    Files.write(_dir.resolve(nodesFile(variant, round)), lines, StandardCharsets.UTF_8);
  }

  public void summary(Map<String, Object> summary) throws IOException {
    Json.write(_dir.resolve(RUN_JSON), summary);
  }

  static String csv(Object value) {
    if (value == null) {
      return "";
    }
    String text = value.toString();
    if (text.contains(",") || text.contains("\"") || text.contains("\n")) {
      return "\"" + text.replace("\"", "\"\"") + "\"";
    }
    return text;
  }

  static String safe(String name) {
    return name.replaceAll("[^A-Za-z0-9._-]", "_");
  }

  /** Orders a stats map so the most useful stats come first. */
  public static Map<String, Object> ordered(Map<String, Object> stats) {
    return new LinkedHashMap<>(stats);
  }
}
