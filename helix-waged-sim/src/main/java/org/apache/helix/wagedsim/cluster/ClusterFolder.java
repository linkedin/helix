package org.apache.helix.wagedsim.cluster;

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

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import com.fasterxml.jackson.core.type.TypeReference;
import org.apache.helix.wagedsim.util.Json;
import org.apache.helix.zookeeper.datamodel.ZNRecord;

/**
 * Reads and writes a cluster folder: one ZNRecord JSON file per znode at its path relative to the
 * cluster root, {@code ASSIGNMENT_METADATA/BASELINE.json} and {@code BEST_POSSIBLE.json} as plain
 * layouts, and {@code manifest.json}.
 */
public final class ClusterFolder {
  public static final String MANIFEST = "manifest.json";
  public static final String ASSIGNMENT_METADATA = "ASSIGNMENT_METADATA";
  public static final String BASELINE = "BASELINE.json";
  public static final String BEST_POSSIBLE = "BEST_POSSIBLE.json";
  private static final TypeReference<Map<String, Map<String, Map<String, String>>>> LAYOUT =
      new TypeReference<Map<String, Map<String, Map<String, String>>>>() {
      };

  private ClusterFolder() {
  }

  public static boolean isClusterFolder(Path dir) {
    return Files.isRegularFile(dir.resolve(MANIFEST));
  }

  /** @return true only for folders this tool wrote: a manifest of the current format with a content hash */
  static boolean isOwnClusterFolder(Path dir) {
    if (!isClusterFolder(dir)) {
      return false;
    }
    try {
      Manifest manifest = Json.MAPPER.readValue(dir.resolve(MANIFEST).toFile(), Manifest.class);
      return manifest.format == Manifest.FORMAT && manifest.cluster != null && manifest.contentSha256 != null;
    } catch (IOException e) {
      return false;
    }
  }

  public static void write(ClusterState state, Path dir) throws IOException {
    if (Files.exists(dir)) {
      try (Stream<Path> entries = Files.list(dir)) {
        if (entries.findAny().isPresent() && !isOwnClusterFolder(dir)) {
          throw new IOException(dir + " is not empty and is not a cluster folder written by waged-sim; "
              + "refusing to overwrite");
        }
      }
      deleteContents(dir);
    }
    Files.createDirectories(dir);
    for (Map.Entry<String, ZNRecord> entry : state.getZnodes().entrySet()) {
      String root = entry.getKey().split("/")[0];
      if (!OWN_ENTRIES.contains(root)) {
        throw new IllegalArgumentException("Unexpected znode path " + entry.getKey());
      }
      Json.writeRecord(dir.resolve(encodePath(entry.getKey()) + ".json"), entry.getValue());
    }
    if (state.getBaseline() != null) {
      Json.write(dir.resolve(ASSIGNMENT_METADATA).resolve(BASELINE), state.getBaseline());
    }
    if (state.getBestPossible() != null) {
      Json.write(dir.resolve(ASSIGNMENT_METADATA).resolve(BEST_POSSIBLE), state.getBestPossible());
    }
    Manifest manifest = state.getManifest();
    manifest.cluster = state.getClusterName();
    manifest.contentSha256 = contentHash(state);
    Json.write(dir.resolve(MANIFEST), manifest);
  }

  public static ClusterState read(Path dir) throws IOException {
    if (!isClusterFolder(dir)) {
      throw new IOException(dir + " is not a cluster folder (no " + MANIFEST + ")");
    }
    Manifest manifest = Json.MAPPER.readValue(dir.resolve(MANIFEST).toFile(), Manifest.class);
    if (manifest.cluster == null) {
      throw new IOException(MANIFEST + " has no cluster name");
    }
    ClusterState state = new ClusterState(manifest.cluster);
    state.setManifest(manifest);
    List<Path> files;
    try (Stream<Path> walk = Files.walk(dir)) {
      files = walk.filter(Files::isRegularFile).filter(p -> p.toString().endsWith(".json"))
          .sorted().collect(Collectors.toList());
    }
    for (Path file : files) {
      Path relative = dir.relativize(file);
      if (relative.toString().equals(MANIFEST)) {
        continue;
      }
      if (relative.getNameCount() == 2 && relative.getName(0).toString().equals(ASSIGNMENT_METADATA)) {
        Map<String, Map<String, Map<String, String>>> layout =
            Json.MAPPER.readValue(file.toFile(), LAYOUT);
        if (relative.getFileName().toString().equals(BASELINE)) {
          state.setBaseline(layout);
        } else if (relative.getFileName().toString().equals(BEST_POSSIBLE)) {
          state.setBestPossible(layout);
        }
        continue;
      }
      String encoded = relative.toString().replace(file.getFileSystem().getSeparator(), "/");
      encoded = encoded.substring(0, encoded.length() - ".json".length());
      state.put(decodePath(encoded), Json.readRecord(file));
    }
    return state;
  }

  /** @return a hash over every znode and assignment, independent of file layout */
  public static String contentHash(ClusterState state) {
    try {
      MessageDigest digest = MessageDigest.getInstance("SHA-256");
      for (Map.Entry<String, ZNRecord> entry : state.getZnodes().entrySet()) {
        digest.update(entry.getKey().getBytes(StandardCharsets.UTF_8));
        digest.update(Json.compact(entry.getValue()).getBytes(StandardCharsets.UTF_8));
      }
      digest.update(("BASELINE" + Json.compact(state.getBaseline())).getBytes(StandardCharsets.UTF_8));
      digest.update(("BEST_POSSIBLE" + Json.compact(state.getBestPossible()))
          .getBytes(StandardCharsets.UTF_8));
      StringBuilder hex = new StringBuilder();
      for (byte b : digest.digest()) {
        hex.append(String.format("%02x", b));
      }
      return hex.toString();
    } catch (NoSuchAlgorithmException e) {
      throw new IllegalStateException(e);
    }
  }

  static String encodePath(String path) {
    List<String> segments = new ArrayList<>();
    for (String segment : path.split("/")) {
      segments.add(encodeSegment(segment));
    }
    return String.join("/", segments);
  }

  static String decodePath(String encoded) {
    List<String> segments = new ArrayList<>();
    for (String segment : encoded.split("/")) {
      segments.add(decodeSegment(segment));
    }
    return String.join("/", segments);
  }

  static String encodeSegment(String segment) {
    if (segment.isEmpty() || segment.equals(".") || segment.equals("..")) {
      throw new IllegalArgumentException("Invalid znode name: '" + segment + "'");
    }
    StringBuilder out = new StringBuilder();
    for (byte b : segment.getBytes(StandardCharsets.UTF_8)) {
      char c = (char) (b & 0xff);
      if ((c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z') || (c >= '0' && c <= '9')
          || "._@=+,-".indexOf(c) >= 0) {
        out.append(c);
      } else {
        out.append('%').append(String.format("%02X", b & 0xff));
      }
    }
    return out.toString();
  }

  static String decodeSegment(String segment) {
    ByteArrayOutputStream bytes = new ByteArrayOutputStream();
    for (int i = 0; i < segment.length(); i++) {
      char c = segment.charAt(i);
      if (c == '%' && i + 2 < segment.length()) {
        bytes.write(Integer.parseInt(segment.substring(i + 1, i + 3), 16));
        i += 2;
      } else {
        bytes.write((byte) c);
      }
    }
    return new String(bytes.toByteArray(), StandardCharsets.UTF_8);
  }

  /** Top-level entries a cluster folder can hold; nothing else in the folder is ever deleted. */
  private static final java.util.Set<String> OWN_ENTRIES = new java.util.HashSet<>(java.util.Arrays.asList(
      MANIFEST, ASSIGNMENT_METADATA, "CONFIGS", "IDEALSTATES", "STATEMODELDEFS", "LIVEINSTANCES",
      "EXTERNALVIEW", "INSTANCES", "CONTROLLER", "PROPERTYSTORE"));

  private static void deleteContents(Path dir) throws IOException {
    List<Path> entries;
    try (Stream<Path> list = Files.list(dir)) {
      entries = list.filter(p -> OWN_ENTRIES.contains(p.getFileName().toString())).collect(Collectors.toList());
    }
    for (Path entry : entries) {
      try (Stream<Path> walk = Files.walk(entry)) {
        for (Path path : walk.sorted(Comparator.reverseOrder()).collect(Collectors.toList())) {
          Files.delete(path);
        }
      }
    }
  }
}
