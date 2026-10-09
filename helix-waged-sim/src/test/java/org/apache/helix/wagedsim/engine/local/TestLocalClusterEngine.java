package org.apache.helix.wagedsim.engine.local;

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
import java.util.Map;

import org.apache.helix.wagedsim.TestClusters;
import org.apache.helix.wagedsim.cluster.ClusterState;
import org.apache.helix.wagedsim.engine.EngineSettings;
import org.apache.helix.wagedsim.engine.RoundResult;
import org.apache.helix.wagedsim.engine.StateOps;
import org.apache.helix.wagedsim.engine.dryrun.DryRunEngine;
import org.apache.helix.wagedsim.source.SpecSource;
import org.testng.Assert;
import org.testng.annotations.Test;

/** Runs a small cluster on embedded ZooKeeper with the real controller and participants. */
public class TestLocalClusterEngine {

  /** One hour of delay becomes ten seconds: long enough to settle inside it, short enough to wait out. */
  private static EngineSettings settings() throws Exception {
    EngineSettings settings = new EngineSettings();
    settings.workDir = Files.createTempDirectory("waged-sim-local-test").toString();
    settings.settleQuietMillis = 1500;
    settings.roundTimeoutMillis = 120_000;
    settings.timeCompression = 360;
    return settings;
  }

  private static void assertServed(ClusterState state, String missing) {
    Map<String, Map<String, Map<String, String>>> served = state.getServedLayout();
    Assert.assertEquals(served.size(), 2, served.keySet().toString());
    for (Map<String, Map<String, String>> partitions : served.values()) {
      Assert.assertEquals(partitions.size(), 8);
      for (Map<String, String> replicas : partitions.values()) {
        Assert.assertEquals(replicas.values().stream().filter("MASTER"::equals).count(), 1, replicas.toString());
        if (missing != null) {
          Assert.assertFalse(replicas.containsKey(missing));
        }
      }
    }
  }

  @Test
  public void testRoundsOnARealController() throws Exception {
    ClusterState state = SpecSource.build(TestClusters.spec("TEST_LOCAL", 6, 2, 8, true));
    try (LocalClusterEngine engine = new LocalClusterEngine()) {
      engine.start(state, settings());
      assertServed(engine.state(), null);
      Assert.assertEquals(engine.participants().size(), 6);

      // A killed instance is replaced only after the (compressed) one-hour delay window.
      StateOps.kill(engine.state(), "node_0");
      RoundResult killed = engine.runRound(1);
      Assert.assertTrue(killed.settled, killed.notes.toString());
      Assert.assertEquals(engine.participants().size(), 5);
      Assert.assertTrue(engine.state().getBestPossible().values().stream()
          .flatMap(p -> p.values().stream()).anyMatch(r -> r.containsKey("node_0")));
      engine.advanceClock(2 * 3600_000L);
      RoundResult after = engine.runRound(2);
      Assert.assertTrue(after.settled, after.notes.toString());
      Assert.assertFalse(engine.state().getBestPossible().values().stream()
          .flatMap(p -> p.values().stream()).anyMatch(r -> r.containsKey("node_0")));
      assertServed(engine.state(), "node_0");
    }
  }

  @Test
  public void testRebaseKeepsTheCapturedShareOfTheDelayWindow() throws Exception {
    ClusterState state = SpecSource.build(TestClusters.spec("TEST_REBASE", 3, 1, 3, true));
    long now = System.currentTimeMillis();
    long captured = now - 48 * 3600_000L;
    state.getManifest().capturedAtMillis = captured;
    String path = ClusterState.instanceConfigPath("node_0");
    for (long compression : new long[]{1, 2}) {
      ClusterState copy = state.copy();
      copy.get(path).setSimpleField("HELIX_ENABLED_TIMESTAMP", String.valueOf(captured - 600_000L));
      LocalCluster.rebaseTimestamps(copy, compression);
      long rebased = Long.parseLong(copy.get(path).getSimpleField("HELIX_ENABLED_TIMESTAMP"));
      long expected = System.currentTimeMillis() - 600_000L / compression;
      Assert.assertTrue(Math.abs(rebased - expected) < 5_000, "compression " + compression + ": " + (expected - rebased));
    }
  }

  @Test
  public void testMaintenanceEnteredByTheControllerCanBeEnded() throws Exception {
    ClusterState state = SpecSource.build(TestClusters.spec("TEST_MAINT", 6, 1, 6, false));
    try (LocalClusterEngine engine = new LocalClusterEngine()) {
      engine.start(state, settings());
      org.apache.helix.model.MaintenanceSignal signal = new org.apache.helix.model.MaintenanceSignal("maintenance");
      signal.setReason("written by the controller");
      engine.cluster().write(StateOps.MAINTENANCE_PATH, signal.getRecord());
      engine.runRound(1);
      Assert.assertNotNull(engine.state().get(StateOps.MAINTENANCE_PATH));
      engine.state().remove(StateOps.MAINTENANCE_PATH);
      engine.runRound(2);
      Assert.assertNull(engine.cluster().read(StateOps.MAINTENANCE_PATH));
    }
  }

  @Test
  public void testAgreesWithDryRunOnPlacement() throws Exception {
    ClusterState source = SpecSource.build(TestClusters.spec("TEST_AGREE", 6, 2, 8, false));
    Map<String, Map<String, Map<String, String>>> local;
    try (LocalClusterEngine engine = new LocalClusterEngine()) {
      engine.start(source.copy(), settings());
      local = engine.state().getBestPossible();
    }
    EngineSettings restart = new EngineSettings();
    restart.firstRound = EngineSettings.FirstRound.RESTART;
    try (DryRunEngine engine = new DryRunEngine()) {
      engine.start(source.copy(), restart);
      engine.runRound(1);
      Assert.assertEquals(local, engine.state().getBestPossible());
    }
  }
}
