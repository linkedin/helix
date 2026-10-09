package org.apache.helix.wagedsim.source;

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

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Base64;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.TreeMap;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.zip.GZIPInputStream;

import org.apache.helix.wagedsim.cluster.ClusterNormalizer;
import org.apache.helix.wagedsim.cluster.ClusterState;
import org.apache.helix.wagedsim.cluster.Manifest;
import org.apache.helix.wagedsim.util.Json;
import org.apache.helix.zookeeper.datamodel.ZNRecord;

/**
 * Imports a folder of Pensieve ("ZooKeeper Point-in-Time Viewer") outputs: recursive dumps of
 * CONFIGS, IDEALSTATES, STATEMODELDEFS, LIVEINSTANCES, EXTERNALVIEW (and optionally INSTANCES), plus
 * the WAGED assignment metadata (LAST_SUCCESSFUL_WRITE, the version tree and every bucket). File
 * names do not matter: each file says which path it holds. Runs offline; the pod-side pull is
 * {@code scripts/pensieve-pull.sh}.
 */
public final class PensieveImporter {
  private static final Pattern NODE = Pattern.compile("^((?:[|` ] {3})*)[|`]-- (\\S+)(.*)$");
  private static final Pattern DATA = Pattern.compile("^[|` ]*\\s+Data: \"(.*)$");
  private static final Pattern PATH = Pattern.compile("^(?:Path|ZNode path): (\\S+)\\s*$");
  private static final Pattern TARGET = Pattern.compile("^Target time: (.+)$");
  private static final Pattern BASE64 = Pattern.compile("[A-Za-z0-9+/=]{16,}");
  private static final DateTimeFormatter DATE_TO_STRING =
      DateTimeFormatter.ofPattern("EEE MMM dd HH:mm:ss zzz yyyy", Locale.ENGLISH);

  private PensieveImporter() {
  }

  /** @return true if the folder holds Pensieve viewer outputs */
  public static boolean looksLikeDump(Path dir) throws IOException {
    try (Stream<Path> files = Files.list(dir)) {
      for (Path file : files.filter(f -> f.toString().endsWith(".txt")).collect(Collectors.toList())) {
        try (Stream<String> lines = Files.lines(file, StandardCharsets.UTF_8)) {
          if (lines.limit(5).anyMatch(l -> l.contains("ZooKeeper Point-in-Time Viewer"))) {
            return true;
          }
        }
      }
    }
    return false;
  }

  /** What a dump folder holds: znode records, raw single-node data, and the target time. */
  static final class Dump {
    final Map<String, ZNRecord> records = new TreeMap<>();
    final Map<String, String> raw = new TreeMap<>();
    String target;
  }

  /**
   * @param cluster the cluster to import; inferred from the dumps when null
   * @param capturedAt overrides the dumps' target time when not null
   */
  public static ClusterState importDump(Path dir, String cluster, Long capturedAt) throws IOException {
    Dump dump = new Dump();
    List<Path> files;
    try (Stream<Path> list = Files.list(dir)) {
      files = list.filter(f -> f.toString().endsWith(".txt")).sorted().collect(Collectors.toList());
    }
    for (Path file : files) {
      parseFile(file, dump);
    }
    if (cluster == null) {
      cluster = inferCluster(dump);
    }
    ClusterState state = new ClusterState(cluster);
    String root = "/" + cluster + "/";
    int sessionless = 0;
    for (Map.Entry<String, ZNRecord> entry : dump.records.entrySet()) {
      if (!entry.getKey().startsWith(root)) {
        continue;
      }
      String relative = entry.getKey().substring(root.length());
      String[] parts = relative.split("/");
      if (relative.startsWith("CONFIGS/CLUSTER/") || relative.startsWith("CONFIGS/PARTICIPANT/")
          || relative.startsWith("CONFIGS/RESOURCE/")) {
        if (parts.length == 3) {
          state.put(relative, entry.getValue());
        }
      } else if ((relative.startsWith("IDEALSTATES/") || relative.startsWith("STATEMODELDEFS/")
          || relative.startsWith("LIVEINSTANCES/") || relative.startsWith("EXTERNALVIEW/")) && parts.length == 2) {
        state.put(relative, entry.getValue());
      } else if (parts.length == 3 && parts[0].equals("INSTANCES") && parts[2].equals("HISTORY")) {
        state.put(relative, entry.getValue());
      } else if (parts.length == 5 && parts[0].equals("INSTANCES") && parts[2].equals("CURRENTSTATES")) {
        state.put(ClusterState.currentStatePath(parts[1], parts[4]), entry.getValue());
        sessionless++;
      }
    }
    state.setBaseline(assignment(dump, cluster, "BASELINE"));
    state.setBestPossible(assignment(dump, cluster, "BEST_POSSIBLE"));
    Manifest manifest = state.getManifest();
    manifest.source = "pensieve";
    manifest.sourceDetail = dir.toAbsolutePath().toString();
    if (capturedAt != null) {
      manifest.capturedAtMillis = capturedAt;
    } else if (dump.target != null) {
      try {
        manifest.capturedAtMillis = ZonedDateTime.parse(dump.target, DATE_TO_STRING).toInstant().toEpochMilli();
      } catch (RuntimeException e) {
        manifest.notes.add("Unparsed target time: " + dump.target);
      }
    }
    if (manifest.capturedAtMillis != null) {
      manifest.capturedAt = java.time.Instant.ofEpochMilli(manifest.capturedAtMillis).toString();
    }
    ZNRecord leader = dump.records.get(root + "CONTROLLER/LEADER");
    if (leader != null) {
      manifest.controllerHelixVersion = leader.getSimpleField("HELIX_VERSION");
    }
    if (sessionless > 0) {
      manifest.notes.add("Imported " + sessionless + " current states.");
    }
    manifest.notes.add("Imported from " + files.size() + " Pensieve output files.");
    ClusterNormalizer.normalize(state, true);
    return state;
  }

  private static String inferCluster(Dump dump) throws IOException {
    Map<String, Integer> counts = new TreeMap<>();
    for (String path : dump.records.keySet()) {
      String[] parts = path.split("/");
      if (parts.length > 1) {
        counts.merge(parts[1], 1, Integer::sum);
      }
    }
    if (counts.isEmpty()) {
      throw new IOException("No znodes found in the Pensieve dump");
    }
    return counts.entrySet().stream().max(Map.Entry.comparingByValue()).get().getKey();
  }

  static void parseFile(Path file, Dump dump) throws IOException {
    List<String> lines = Files.readAllLines(file, StandardCharsets.UTF_8);
    String path = null;
    boolean tree = false;
    int index = 0;
    for (; index < lines.size(); index++) {
      String line = lines.get(index);
      Matcher target = TARGET.matcher(line);
      if (target.matches() && dump.target == null) {
        dump.target = target.group(1).trim();
      }
      Matcher pathMatch = PATH.matcher(line);
      if (pathMatch.matches()) {
        path = pathMatch.group(1);
      }
      if (line.startsWith("=== Recursive Tree Structure ===")) {
        tree = true;
        index++;
        break;
      }
      if (line.startsWith("Data (") && path != null) {
        StringBuilder data = new StringBuilder();
        for (int j = index + 1; j < lines.size(); j++) {
          String next = lines.get(j);
          if (next.startsWith("Children:")) {
            break;
          }
          data.append(next.startsWith("  ") ? next.substring(2) : next).append('\n');
        }
        store(dump, path, data.toString().trim());
        return;
      }
    }
    if (tree) {
      parseTree(lines, index, dump);
    }
  }

  private static void parseTree(List<String> lines, int start, Dump dump) {
    if (start >= lines.size()) {
      return;
    }
    String rootLine = lines.get(start);
    String rootPath = rootLine.split(" ")[0];
    List<String> stack = new ArrayList<>();
    String current = rootPath;
    for (int i = start + 1; i < lines.size(); i++) {
      String line = lines.get(i);
      Matcher data = DATA.matcher(line);
      if (data.matches() && current != null) {
        StringBuilder value = new StringBuilder(data.group(1));
        if (!isComplete(value.toString())) {
          while (++i < lines.size()) {
            String next = lines.get(i);
            value.append('\n').append(next);
            if (next.equals("}\"") || next.endsWith("}\"") && !next.startsWith(" ")) {
              break;
            }
          }
        }
        String text = value.toString();
        if (text.endsWith("\"")) {
          text = text.substring(0, text.length() - 1);
        }
        store(dump, current, text);
        continue;
      }
      Matcher node = NODE.matcher(line);
      if (node.matches()) {
        int depth = node.group(1).length() / 4;
        while (stack.size() > depth) {
          stack.remove(stack.size() - 1);
        }
        stack.add(node.group(2));
        current = rootPath + "/" + String.join("/", stack);
      }
    }
  }

  private static boolean isComplete(String firstLine) {
    return firstLine.endsWith("\"") && (firstLine.startsWith("{") ? firstLine.endsWith("}\"") : true);
  }

  private static void store(Dump dump, String path, String text) {
    String trimmed = text.trim();
    if (trimmed.startsWith("{")) {
      try {
        ZNRecord record = Json.toRecord(Json.MAPPER.readTree(trimmed));
        dump.records.put(path, record);
        return;
      } catch (IOException e) {
        // Not a ZNRecord (for example bucket metadata); keep the raw text.
      }
    }
    dump.raw.put(path, trimmed);
  }

  /** Reassembles a bucketized, gzip-compressed assignment from LAST_SUCCESSFUL_WRITE, METADATA and buckets. */
  static Map<String, Map<String, Map<String, String>>> assignment(Dump dump, String cluster, String kind)
      throws IOException {
    String base = "/" + cluster + "/ASSIGNMENT_METADATA/" + kind;
    String version = dump.raw.get(base + "/LAST_SUCCESSFUL_WRITE");
    if (version == null) {
      return null;
    }
    version = version.replaceAll("[^0-9]", "");
    String metadata = dump.raw.get(base + "/" + version + "/METADATA");
    long dataSize = -1;
    if (metadata != null) {
      Matcher size = Pattern.compile("\"DATA_SIZE\"\\s*:\\s*\"?(\\d+)").matcher(metadata);
      if (size.find()) {
        dataSize = Long.parseLong(size.group(1));
      }
    }
    ByteArrayOutputStream blob = new ByteArrayOutputStream();
    for (int bucket = 0; ; bucket++) {
      String data = dump.raw.get(base + "/" + version + "/" + bucket);
      if (data == null) {
        if (bucket == 0) {
          throw new IOException("No buckets for " + base + "/" + version + "; dump each bucket znode");
        }
        break;
      }
      blob.write(Base64.getDecoder().decode(longestBase64(data)));
    }
    byte[] bytes = blob.toByteArray();
    if (dataSize >= 0 && bytes.length > dataSize) {
      bytes = java.util.Arrays.copyOf(bytes, (int) dataSize);
    }
    if (dataSize >= 0 && bytes.length < dataSize) {
      throw new IOException(base + ": buckets hold " + bytes.length + " bytes, expected " + dataSize);
    }
    ZNRecord combined;
    try (InputStream in = new GZIPInputStream(new ByteArrayInputStream(bytes))) {
      combined = Json.toRecord(Json.MAPPER.readTree(in));
    }
    Map<String, Map<String, Map<String, String>>> layout = new TreeMap<>();
    for (Map.Entry<String, String> resource : combined.getSimpleFields().entrySet()) {
      ZNRecord assignment = Json.toRecord(Json.MAPPER.readTree(resource.getValue()));
      Map<String, Map<String, String>> partitions = new TreeMap<>();
      assignment.getMapFields().forEach((partition, replicas) -> partitions.put(partition, new TreeMap<>(replicas)));
      layout.put(resource.getKey(), partitions);
    }
    return layout;
  }

  private static String longestBase64(String text) {
    String candidate = text;
    int marker = text.lastIndexOf("(base64)");
    if (marker >= 0) {
      candidate = text.substring(0, marker);
    }
    Matcher matcher = BASE64.matcher(candidate.replaceAll("\\s+", ""));
    String best = "";
    while (matcher.find()) {
      if (matcher.group().length() > best.length()) {
        best = matcher.group();
      }
    }
    return best;
  }

  /** @return the pod-side pull script for a cluster and time, from the bundled template */
  public static String pullScript(String cluster, String time, String ensemblePath, String jar, String java)
      throws IOException {
    try (InputStream in = PensieveImporter.class.getResourceAsStream("/scripts/pensieve-pull.sh")) {
      if (in == null) {
        throw new IOException("Bundled pull script not found");
      }
      String template = new String(readAll(in), StandardCharsets.UTF_8);
      Map<String, String> values = new LinkedHashMap<>();
      values.put("@CLUSTER@", cluster);
      values.put("@TIME@", time);
      values.put("@BACKUP_DIR@", ensemblePath);
      values.put("@JAR@", jar);
      values.put("@JAVA@", java);
      for (Map.Entry<String, String> entry : values.entrySet()) {
        template = template.replace(entry.getKey(), entry.getValue());
      }
      return template;
    }
  }

  private static byte[] readAll(InputStream in) throws IOException {
    ByteArrayOutputStream out = new ByteArrayOutputStream();
    byte[] buffer = new byte[8192];
    int read;
    while ((read = in.read(buffer)) > 0) {
      out.write(buffer, 0, read);
    }
    return out.toByteArray();
  }
}
