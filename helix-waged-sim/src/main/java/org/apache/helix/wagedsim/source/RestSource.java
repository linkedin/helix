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
import java.io.PrintStream;
import java.net.URI;
import java.net.URLEncoder;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import com.fasterxml.jackson.databind.JsonNode;
import org.apache.helix.model.LiveInstance;
import org.apache.helix.wagedsim.cluster.ClusterNormalizer;
import org.apache.helix.wagedsim.cluster.ClusterState;
import org.apache.helix.wagedsim.cluster.Manifest;
import org.apache.helix.wagedsim.util.Json;
import org.apache.helix.zookeeper.datamodel.ZNRecord;

/**
 * Copies a cluster through helix-rest read endpoints. helix-rest does not expose WAGED assignment
 * metadata, so the baseline and best possible start from the served layout (flagged in the manifest).
 * Current states come from the external views unless per-instance current states are requested.
 */
public class RestSource {
  private final String _base;
  private final Map<String, String> _headers;
  private final HttpClient _client;
  private final PrintStream _log;
  private final int _parallelism;

  /**
   * @param base helix-rest base URL, the part before {@code /clusters}, for example
   *          {@code http://localhost:8100/admin/v2}
   */
  public RestSource(String base, Map<String, String> headers, int parallelism, PrintStream log) {
    _base = base.endsWith("/") ? base.substring(0, base.length() - 1) : base;
    _headers = headers == null ? new LinkedHashMap<>() : headers;
    _parallelism = Math.max(1, parallelism);
    _log = log;
    _client = HttpClient.newBuilder().connectTimeout(Duration.ofSeconds(20))
        .followRedirects(HttpClient.Redirect.NORMAL).build();
  }

  public ClusterState copy(String cluster, boolean currentStates) throws Exception {
    String root = "/clusters/" + encode(cluster);
    ClusterState state = new ClusterState(cluster);
    long start = System.currentTimeMillis();
    state.put(ClusterState.clusterConfigPath(cluster), record(get(root + "/configs")));

    JsonNode models = get(root + "/statemodeldefs");
    for (JsonNode name : models.path("stateModelDefinitions")) {
      state.put(ClusterState.stateModelDefPath(name.asText()),
          record(get(root + "/statemodeldefs/" + encode(name.asText()))));
    }

    JsonNode instances = get(root + "/instances");
    List<String> instanceNames = texts(instances.path("instances"));
    List<String> online = texts(instances.path("online"));
    JsonNode resources = get(root + "/resources");
    List<String> idealStates = texts(resources.path("idealStates"));
    List<String> externalViews = texts(resources.path("externalViews"));
    log("copying " + instanceNames.size() + " instances (" + online.size() + " online) and "
        + idealStates.size() + " resources from " + _base);

    ExecutorService pool = Executors.newFixedThreadPool(_parallelism);
    try {
      Map<String, Future<JsonNode>> pending = new LinkedHashMap<>();
      for (String instance : instanceNames) {
        String path = root + "/instances/" + encode(instance);
        pending.put(ClusterState.instanceConfigPath(instance), pool.submit(() -> get(path + "/configs")));
        pending.put(ClusterState.historyPath(instance), pool.submit(() -> getOrNull(path + "/history")));
      }
      for (String resource : idealStates) {
        String path = root + "/resources/" + encode(resource);
        pending.put(ClusterState.idealStatePath(resource), pool.submit(() -> get(path + "/idealState")));
        pending.put(ClusterState.resourceConfigPath(resource), pool.submit(() -> getOrNull(path + "/configs")));
      }
      for (String resource : externalViews) {
        String path = root + "/resources/" + encode(resource) + "/externalView";
        pending.put(ClusterState.externalViewPath(resource), pool.submit(() -> getOrNull(path)));
      }
      if (currentStates) {
        for (String instance : online) {
          for (String resource : idealStates) {
            String path = root + "/instances/" + encode(instance) + "/resources/" + encode(resource);
            pending.put(ClusterState.currentStatePath(instance, resource), pool.submit(() -> getOrNull(path)));
          }
        }
      }
      for (Map.Entry<String, Future<JsonNode>> entry : pending.entrySet()) {
        JsonNode node = entry.getValue().get();
        if (node != null && node.has("id")) {
          state.put(entry.getKey(), record(node));
        }
      }
    } finally {
      pool.shutdownNow();
    }
    for (String instance : online) {
      LiveInstance liveInstance = new LiveInstance(instance);
      liveInstance.setSessionId(ClusterNormalizer.syntheticSession(instance, 0));
      state.putLiveInstance(liveInstance);
    }
    JsonNode controller = getOrNull(root + "/controller");
    Manifest manifest = state.getManifest();
    manifest.source = "rest";
    manifest.sourceDetail = _base + root;
    manifest.capturedAtMillis = start;
    manifest.capturedAt = java.time.Instant.ofEpochMilli(start).toString();
    if (controller != null && controller.has("HELIX_VERSION")) {
      manifest.controllerHelixVersion = controller.get("HELIX_VERSION").asText();
    }
    manifest.notes.add("Copied through helix-rest in " + (System.currentTimeMillis() - start) / 1000 + "s"
        + (currentStates ? " with per-instance current states" : "; current states from external views"));
    ClusterNormalizer.normalize(state, true);
    return state;
  }

  private JsonNode get(String path) throws IOException, InterruptedException {
    JsonNode node = getOrNull(path);
    if (node == null) {
      throw new IOException("GET " + _base + path + " returned not found");
    }
    return node;
  }

  private JsonNode getOrNull(String path) throws IOException, InterruptedException {
    HttpRequest.Builder request = HttpRequest.newBuilder(URI.create(_base + path))
        .timeout(Duration.ofSeconds(60)).header("Accept", "application/json").GET();
    _headers.forEach(request::header);
    HttpResponse<String> response = _client.send(request.build(), HttpResponse.BodyHandlers.ofString());
    if (response.statusCode() == 404 || response.statusCode() == 400) {
      return null;
    }
    if (response.statusCode() / 100 != 2) {
      throw new IOException("GET " + _base + path + " returned HTTP " + response.statusCode() + ": "
          + Json.abbreviate(response.body()));
    }
    return Json.MAPPER.readTree(response.body());
  }

  private static ZNRecord record(JsonNode node) throws IOException {
    return Json.toRecord(node);
  }

  private static List<String> texts(JsonNode array) {
    List<String> result = new ArrayList<>();
    for (JsonNode item : array) {
      result.add(item.asText());
    }
    return result;
  }

  private static String encode(String segment) {
    return URLEncoder.encode(segment, StandardCharsets.UTF_8).replace("+", "%20");
  }

  private void log(String message) {
    if (_log != null) {
      _log.println(message);
    }
  }
}
