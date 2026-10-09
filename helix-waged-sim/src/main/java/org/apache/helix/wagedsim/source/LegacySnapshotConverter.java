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

import java.io.IOException;
import java.nio.file.Path;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Iterator;
import java.util.Locale;
import java.util.Map;
import java.util.TreeMap;

import com.fasterxml.jackson.databind.JsonNode;
import org.apache.helix.model.ExternalView;
import org.apache.helix.wagedsim.cluster.ClusterNormalizer;
import org.apache.helix.wagedsim.cluster.ClusterState;
import org.apache.helix.wagedsim.cluster.Manifest;
import org.apache.helix.wagedsim.util.Json;
import org.apache.helix.zookeeper.datamodel.ZNRecord;

/**
 * Converts the single-file snapshots written by the leader-skew investigation
 * ({@code clusterConfig}, {@code instanceConfigs}, ..., {@code baseline}, {@code bestPossible},
 * {@code externalView}) into a cluster definition.
 */
public final class LegacySnapshotConverter {
  private static final DateTimeFormatter DATE_TO_STRING =
      DateTimeFormatter.ofPattern("EEE MMM dd HH:mm:ss zzz yyyy", Locale.ENGLISH);

  private LegacySnapshotConverter() {
  }

  public static boolean looksLegacy(JsonNode root) {
    return root.has("clusterConfig") && root.has("instanceConfigs") && root.has("idealStates");
  }

  public static ClusterState convert(Path file) throws IOException {
    JsonNode root = Json.MAPPER.readTree(file.toFile());
    if (!looksLegacy(root)) {
      throw new IOException(file + " is not a legacy snapshot");
    }
    ZNRecord clusterConfig = Json.toRecord(root.get("clusterConfig"));
    String cluster = root.has("cluster") ? root.get("cluster").asText() : clusterConfig.getId();
    ClusterState state = new ClusterState(cluster);
    state.put(ClusterState.clusterConfigPath(cluster), clusterConfig);
    for (JsonNode node : root.get("instanceConfigs")) {
      ZNRecord record = Json.toRecord(node);
      state.put(ClusterState.instanceConfigPath(record.getId()), record);
    }
    for (JsonNode node : root.path("liveInstances")) {
      ZNRecord record = Json.toRecord(node);
      state.put(ClusterState.liveInstancePath(record.getId()), record);
    }
    for (JsonNode node : root.path("resourceConfigs")) {
      ZNRecord record = Json.toRecord(node);
      state.put(ClusterState.resourceConfigPath(record.getId()), record);
    }
    for (JsonNode node : root.get("idealStates")) {
      ZNRecord record = Json.toRecord(node);
      state.put(ClusterState.idealStatePath(record.getId()), record);
    }
    for (JsonNode node : root.path("stateModelDefs")) {
      ZNRecord record = Json.toRecord(node);
      state.put(ClusterState.stateModelDefPath(record.getId()), record);
    }
    if (root.has("baseline")) {
      state.setBaseline(layout(root.get("baseline")));
    }
    if (root.has("bestPossible")) {
      state.setBestPossible(layout(root.get("bestPossible")));
    }
    if (root.has("externalView")) {
      layout(root.get("externalView")).forEach((resource, partitions) -> {
        ExternalView view = new ExternalView(resource);
        partitions.forEach(view::setStateMap);
        state.put(ClusterState.externalViewPath(resource), view.getRecord());
      });
    }
    Manifest manifest = state.getManifest();
    manifest.source = "legacy";
    manifest.sourceDetail = file.toAbsolutePath().toString();
    JsonNode provenance = root.path("provenance");
    Iterator<JsonNode> entries = provenance.elements();
    while (entries.hasNext()) {
      JsonNode entry = entries.next();
      if (entry.has("target")) {
        try {
          ZonedDateTime target = ZonedDateTime.parse(entry.get("target").asText(), DATE_TO_STRING);
          manifest.capturedAtMillis = target.toInstant().toEpochMilli();
          manifest.capturedAt = target.toInstant().toString();
        } catch (RuntimeException e) {
          manifest.notes.add("Unparsed capture time: " + entry.get("target").asText());
        }
        break;
      }
    }
    manifest.notes.add("Converted from a single-file snapshot; external view equals the best possible "
        + "assignment in these captures.");
    ClusterNormalizer.normalize(state, true);
    return state;
  }

  private static Map<String, Map<String, Map<String, String>>> layout(JsonNode node) {
    Map<String, Map<String, Map<String, String>>> layout = new TreeMap<>();
    node.fields().forEachRemaining(resource -> {
      Map<String, Map<String, String>> partitions = new TreeMap<>();
      resource.getValue().fields().forEachRemaining(partition -> {
        Map<String, String> replicas = new TreeMap<>();
        partition.getValue().fields().forEachRemaining(r -> replicas.put(r.getKey(), r.getValue().asText()));
        partitions.put(partition.getKey(), replicas);
      });
      layout.put(resource.getKey(), partitions);
    });
    return layout;
  }
}
