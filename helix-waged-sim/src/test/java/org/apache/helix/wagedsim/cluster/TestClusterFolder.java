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

import java.nio.file.Files;
import java.nio.file.Path;

import org.apache.helix.wagedsim.TestClusters;
import org.apache.helix.wagedsim.source.FolderSource;
import org.apache.helix.wagedsim.source.SpecSource;
import org.testng.Assert;
import org.testng.annotations.Test;

public class TestClusterFolder {

  @Test
  public void testRoundTrip() throws Exception {
    ClusterState state = SpecSource.build(TestClusters.spec("TEST_FOLDER", 6, 1, 8, true));
    Path dir = Files.createTempDirectory("waged-sim-folder");
    ClusterFolder.write(state, dir);
    ClusterState read = ClusterFolder.read(dir);
    Assert.assertEquals(read.getClusterName(), "TEST_FOLDER");
    Assert.assertEquals(read.getZnodes(), state.getZnodes());
    Assert.assertEquals(read.getBaseline(), state.getBaseline());
    Assert.assertEquals(read.getBestPossible(), state.getBestPossible());
    Assert.assertEquals(ClusterFolder.contentHash(read), ClusterFolder.contentHash(state));
    Assert.assertEquals(read.getManifest().source, "spec");
    ClusterState opened = FolderSource.open(dir);
    Assert.assertEquals(opened.getServedLayout(), state.getServedLayout());
  }

  @Test
  public void testRefusesToOverwriteOtherFolders() throws Exception {
    Path dir = Files.createTempDirectory("waged-sim-other");
    Files.write(dir.resolve("important.txt"), "keep".getBytes());
    try {
      ClusterFolder.write(SpecSource.build(TestClusters.spec("TEST_X", 3, 1, 3, false)), dir);
      Assert.fail("Expected a refusal");
    } catch (java.io.IOException expected) {
      Assert.assertTrue(Files.exists(dir.resolve("important.txt")));
    }
  }

  @Test
  public void testRefusesForeignManifest() throws Exception {
    Path dir = Files.createTempDirectory("waged-sim-foreign");
    Files.write(dir.resolve("manifest.json"), "{\"name\": \"some web app\"}".getBytes());
    Files.write(dir.resolve("index.html"), "<html/>".getBytes());
    try {
      ClusterFolder.write(SpecSource.build(TestClusters.spec("TEST_Y", 3, 1, 3, false)), dir);
      Assert.fail("Expected a refusal");
    } catch (java.io.IOException expected) {
      Assert.assertTrue(Files.exists(dir.resolve("index.html")));
    }
  }

  @Test
  public void testRewriteKeepsUnrelatedFiles() throws Exception {
    Path dir = Files.createTempDirectory("waged-sim-rewrite");
    ClusterState state = SpecSource.build(TestClusters.spec("TEST_Z", 3, 1, 3, false));
    ClusterFolder.write(state, dir);
    Files.write(dir.resolve("notes.txt"), "mine".getBytes());
    ClusterFolder.write(state, dir);
    Assert.assertTrue(Files.exists(dir.resolve("notes.txt")));
    Assert.assertEquals(ClusterFolder.read(dir).getZnodes(), state.getZnodes());
  }

  @Test
  public void testRejectsUnsafeClusterNames() {
    for (String name : new String[]{"../escape", "a/b", "..", ".", "/abs", "a\\b", ""}) {
      try {
        new ClusterState(name);
        Assert.fail("Accepted " + name);
      } catch (IllegalArgumentException expected) {
        // expected
      }
    }
    Assert.assertEquals(new ClusterState("MY_CLUSTER-1").getClusterName(), "MY_CLUSTER-1");
  }

  @Test
  public void testPathEncoding() {
    String path = "CONFIGS/PARTICIPANT/host:1234/odd name%";
    Assert.assertEquals(ClusterFolder.decodePath(ClusterFolder.encodePath(path)), path);
    Assert.assertFalse(ClusterFolder.encodePath(path).contains(":"));
  }
}
