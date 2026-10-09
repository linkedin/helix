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
import java.nio.file.Files;
import java.nio.file.Path;

import com.fasterxml.jackson.databind.JsonNode;
import org.apache.helix.wagedsim.cluster.ClusterFolder;
import org.apache.helix.wagedsim.cluster.ClusterNormalizer;
import org.apache.helix.wagedsim.cluster.ClusterState;
import org.apache.helix.wagedsim.util.Json;

/** Opens a cluster from a folder of copied data: a cluster folder, a legacy snapshot or a spec. */
public final class FolderSource {
  private FolderSource() {
  }

  public static ClusterState open(Path path) throws Exception {
    if (Files.isDirectory(path)) {
      if (ClusterFolder.isClusterFolder(path)) {
        ClusterState state = ClusterFolder.read(path);
        ClusterNormalizer.normalize(state, !"spec".equals(state.getManifest().source));
        return state;
      }
      if (PensieveImporter.looksLikeDump(path)) {
        return PensieveImporter.importDump(path, null, null);
      }
      throw new IOException(path + " is neither a cluster folder (manifest.json) nor a Pensieve dump");
    }
    String name = path.getFileName().toString().toLowerCase();
    if (name.endsWith(".yaml") || name.endsWith(".yml")) {
      return SpecSource.load(path);
    }
    if (name.endsWith(".json")) {
      JsonNode root = Json.MAPPER.readTree(path.toFile());
      if (LegacySnapshotConverter.looksLegacy(root)) {
        return LegacySnapshotConverter.convert(path);
      }
    }
    throw new IOException("Unrecognized cluster input " + path
        + ": expected a cluster folder, a Pensieve dump folder, a legacy snapshot (.json) or a spec (.yaml)");
  }
}
