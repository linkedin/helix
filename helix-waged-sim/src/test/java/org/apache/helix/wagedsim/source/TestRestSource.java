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

import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import com.sun.net.httpserver.HttpServer;
import org.apache.helix.model.ExternalView;
import org.apache.helix.wagedsim.TestClusters;
import org.apache.helix.wagedsim.cluster.ClusterState;
import org.apache.helix.wagedsim.cluster.Manifest;
import org.apache.helix.wagedsim.util.Json;
import org.testng.Assert;
import org.testng.annotations.Test;

/** Serves a generated cluster through the helix-rest read endpoints and copies it back. */
public class TestRestSource {

  @Test
  public void testCopy() throws Exception {
    ClusterState source = SpecSource.build(TestClusters.spec("TEST_REST", 9, 2, 12, true));
    String cluster = source.getClusterName();
    Map<String, Object> routes = new LinkedHashMap<>();
    String root = "/admin/v2/clusters/" + cluster;
    routes.put(root + "/configs", source.get(ClusterState.clusterConfigPath(cluster)));
    Map<String, Object> models = new LinkedHashMap<>();
    models.put("id", cluster);
    models.put("stateModelDefinitions", new ArrayList<>(source.getStateModelDefs().keySet()));
    routes.put(root + "/statemodeldefs", models);
    source.getStateModelDefs().forEach((name, model) -> routes.put(root + "/statemodeldefs/" + name, model.getRecord()));
    Map<String, Object> instances = new LinkedHashMap<>();
    instances.put("id", cluster);
    instances.put("instances", new ArrayList<>(source.getInstanceConfigs().keySet()));
    instances.put("online", new ArrayList<>(source.getLiveInstances().keySet()));
    routes.put(root + "/instances", instances);
    source.getInstanceConfigs().forEach((name, config) -> {
      routes.put(root + "/instances/" + name + "/configs", config.getRecord());
      routes.put(root + "/instances/" + name + "/history", source.get(ClusterState.historyPath(name)));
    });
    Map<String, Object> resources = new LinkedHashMap<>();
    resources.put("id", cluster);
    resources.put("idealStates", new ArrayList<>(source.getIdealStates().keySet()));
    resources.put("externalViews", new ArrayList<>(source.getIdealStates().keySet()));
    routes.put(root + "/resources", resources);
    Map<String, Map<String, Map<String, String>>> served = source.getServedLayout();
    source.getIdealStates().forEach((name, idealState) -> {
      routes.put(root + "/resources/" + name + "/idealState", idealState.getRecord());
      routes.put(root + "/resources/" + name + "/configs", source.get(ClusterState.resourceConfigPath(name)));
      ExternalView view = new ExternalView(name);
      served.get(name).forEach(view::setStateMap);
      routes.put(root + "/resources/" + name + "/externalView", view.getRecord());
    });
    Map<String, String> controller = new LinkedHashMap<>();
    controller.put("controller", "controller-1");
    controller.put("HELIX_VERSION", "9.9.9");
    routes.put(root + "/controller", controller);

    List<String> requests = new ArrayList<>();
    HttpServer server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
    server.createContext("/", exchange -> {
      String path = exchange.getRequestURI().getRawPath();
      synchronized (requests) {
        requests.add(path + " " + exchange.getRequestHeaders().getFirst("X-Test"));
      }
      Object body = routes.get(java.net.URLDecoder.decode(path, "UTF-8"));
      byte[] bytes = body == null ? "{}".getBytes(StandardCharsets.UTF_8)
          : Json.compact(body).getBytes(StandardCharsets.UTF_8);
      exchange.sendResponseHeaders(body == null ? 404 : 200, bytes.length);
      try (OutputStream out = exchange.getResponseBody()) {
        out.write(bytes);
      }
    });
    server.start();
    try {
      Map<String, String> headers = new LinkedHashMap<>();
      headers.put("X-Test", "yes");
      String base = "http://127.0.0.1:" + server.getAddress().getPort() + "/admin/v2";
      ClusterState copy = new RestSource(base, headers, 4, null).copy(cluster, false);
      Assert.assertEquals(copy.getIdealStates(), source.getIdealStates());
      Assert.assertEquals(copy.getInstanceConfigs().keySet(), source.getInstanceConfigs().keySet());
      Assert.assertEquals(copy.getLiveInstances().keySet(), source.getLiveInstances().keySet());
      Assert.assertEquals(copy.getServedLayout(), served);
      Assert.assertNull(copy.getBaseline());
      Assert.assertEquals(copy.getManifest().controllerHelixVersion, "9.9.9");
      Assert.assertTrue(copy.getManifest().fidelity.contains(Manifest.NO_ASSIGNMENT_METADATA));
      synchronized (requests) {
        Assert.assertTrue(requests.stream().allMatch(r -> r.endsWith(" yes")), requests.toString());
      }
    } finally {
      server.stop(0);
    }
  }
}
