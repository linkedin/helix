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

import java.io.ByteArrayOutputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Base64;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.zip.GZIPOutputStream;

import org.apache.helix.wagedsim.TestClusters;
import org.apache.helix.wagedsim.cluster.ClusterState;
import org.apache.helix.wagedsim.cluster.Layouts;
import org.apache.helix.wagedsim.cluster.Manifest;
import org.apache.helix.wagedsim.util.Json;
import org.apache.helix.zookeeper.datamodel.ZNRecord;
import org.testng.Assert;
import org.testng.SkipException;
import org.testng.annotations.Test;

public class TestPensieveImporter {
  private static final String TARGET = "Tue Sep 22 07:25:34 UTC 2026";

  /** Writes a cluster as Pensieve viewer outputs, in the formats a pod pull produces. */
  static void writeDump(ClusterState state, Path dir) throws Exception {
    String root = "/" + state.getClusterName();
    ClusterState withViews = state.copy();
    withViews.getServedLayout().forEach((resource, partitions) -> {
      org.apache.helix.model.ExternalView view = new org.apache.helix.model.ExternalView(resource);
      partitions.forEach(view::setStateMap);
      withViews.put(ClusterState.externalViewPath(resource), view.getRecord());
    });
    for (String tree : new String[]{"CONFIGS", "IDEALSTATES", "STATEMODELDEFS", "LIVEINSTANCES", "EXTERNALVIEW"}) {
      Map<String, ZNRecord> records = new TreeMap<>();
      withViews.getZnodes().forEach((path, record) -> {
        if (path.startsWith(tree + "/")) {
          records.put(path.substring(tree.length() + 1), record);
        }
      });
      Files.write(dir.resolve(tree + ".txt"), treeDump(root + "/" + tree, records).getBytes(StandardCharsets.UTF_8));
    }
    writeAssignment(dir, root, "BASELINE", state.getBaseline(), 7);
    writeAssignment(dir, root, "BEST_POSSIBLE", state.getBestPossible(), 9);
  }

  private static String header(String path) {
    return "=== ZooKeeper Point-in-Time Viewer ===\nTarget time: " + TARGET + "\nZNode path: " + path
        + "\n\n=== ZNode State at " + TARGET + " ===\nPath: " + path + "\n\n";
  }

  private static String treeDump(String rootPath, Map<String, ZNRecord> records) throws Exception {
    StringBuilder out = new StringBuilder(header(rootPath));
    out.append("=== Recursive Tree Structure ===\n").append(rootPath).append(" (n children)\n");
    List<String> printed = new ArrayList<>();
    for (Map.Entry<String, ZNRecord> entry : records.entrySet()) {
      String[] parts = entry.getKey().split("/");
      StringBuilder prefix = new StringBuilder();
      for (int depth = 0; depth < parts.length; depth++) {
        String nodePath = String.join("/", java.util.Arrays.copyOfRange(parts, 0, depth + 1));
        if (!printed.contains(nodePath)) {
          printed.add(nodePath);
          out.append(prefix).append("|-- ").append(parts[depth])
              .append(depth == parts.length - 1 ? " [100B]" : " (1 children)").append('\n');
        }
        prefix.append("|   ");
      }
      String json = Json.MAPPER.writerWithDefaultPrettyPrinter().writeValueAsString(entry.getValue());
      out.append(prefix).append("    Data: \"").append(json).append("\"\n");
    }
    return out.toString();
  }

  private static void writeAssignment(Path dir, String root, String kind,
      Map<String, Map<String, Map<String, String>>> layout, int version) throws Exception {
    ZNRecord combined = new ZNRecord(kind);
    Layouts.toAssignments(layout).forEach((resource, assignment) ->
        combined.setSimpleField(resource, Json.compact(assignment.getRecord())));
    ByteArrayOutputStream bytes = new ByteArrayOutputStream();
    try (GZIPOutputStream gzip = new GZIPOutputStream(bytes)) {
      gzip.write(Json.compact(combined).getBytes(StandardCharsets.UTF_8));
    }
    byte[] data = bytes.toByteArray();
    String base = root + "/ASSIGNMENT_METADATA/" + kind;
    Files.write(dir.resolve(kind + "__lsw.txt"), (header(base + "/LAST_SUCCESSFUL_WRITE")
        + "Data (1 bytes):\n  " + version + "\n\nChildren: (none)\n").getBytes(StandardCharsets.UTF_8));
    // Two buckets, to exercise reassembly.
    int split = data.length / 2;
    Files.write(dir.resolve(kind + "__tree.txt"), (header(base + "/" + version) + "=== Recursive Tree Structure ===\n"
        + base + "/" + version + " (3 children)\n|-- 0 [" + split + "B]\n|-- 1 [" + (data.length - split) + "B]\n"
        + "`-- METADATA [43B]\n|       Data: \"{\"BUCKET_SIZE\":\"" + split + "\",\"DATA_SIZE\":\"" + data.length
        + "\"}\"\n").getBytes(StandardCharsets.UTF_8));
    byte[][] buckets = {java.util.Arrays.copyOfRange(data, 0, split), java.util.Arrays.copyOfRange(data, split, data.length)};
    for (int i = 0; i < 2; i++) {
      Files.write(dir.resolve(kind + "__b" + i + ".txt"), (header(base + "/" + version + "/" + i) + "Data ("
          + buckets[i].length + " bytes):\n  " + Base64.getEncoder().encodeToString(buckets[i]) + " (base64)\n\n"
          + "Children: (none)\n").getBytes(StandardCharsets.UTF_8));
    }
  }

  @Test
  public void testImportMatchesSource() throws Exception {
    ClusterState source = SpecSource.build(TestClusters.spec("TEST_PENSIEVE", 9, 2, 12, true));
    Path dir = Files.createTempDirectory("waged-sim-pensieve");
    writeDump(source, dir);
    Assert.assertTrue(PensieveImporter.looksLikeDump(dir));
    ClusterState imported = PensieveImporter.importDump(dir, null, null);
    Assert.assertEquals(imported.getClusterName(), "TEST_PENSIEVE");
    Assert.assertEquals(imported.getBaseline(), source.getBaseline());
    Assert.assertEquals(imported.getBestPossible(), source.getBestPossible());
    Assert.assertEquals(imported.getInstanceConfigs().keySet(), source.getInstanceConfigs().keySet());
    Assert.assertEquals(imported.getIdealStates(), source.getIdealStates());
    Assert.assertEquals(imported.getServedLayout(), source.getServedLayout());
    Assert.assertEquals(imported.getManifest().source, "pensieve");
    Assert.assertEquals(imported.getManifest().capturedAt, "2026-09-22T07:25:34Z");
    Assert.assertTrue(imported.getManifest().fidelity.contains(Manifest.CURRENT_STATES_FROM_EXTERNAL_VIEW));
  }

  /** Optional check against a real pull kept outside the repository. */
  @Test
  public void testRealDumpIfPresent() throws Exception {
    String dump = System.getProperty("wagedsim.pensieveDump");
    if (dump == null || !Files.isDirectory(Paths.get(dump))) {
      throw new SkipException("Set -Dwagedsim.pensieveDump=<dir> to check a real Pensieve pull");
    }
    ClusterState imported = PensieveImporter.importDump(Paths.get(dump), null, null);
    Assert.assertNotNull(imported.getBaseline());
    Assert.assertNotNull(imported.getBestPossible());
    Assert.assertFalse(imported.getWagedIdealStates().isEmpty());
  }
}
