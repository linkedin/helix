package org.apache.helix.wagedsim.engine.dryrun;

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

import java.util.Map;

import org.apache.helix.wagedsim.TestClusters;
import org.apache.helix.wagedsim.cluster.ClusterState;
import org.apache.helix.wagedsim.cluster.Layouts;
import org.apache.helix.wagedsim.engine.EngineSettings;
import org.apache.helix.wagedsim.engine.RoundResult;
import org.apache.helix.wagedsim.engine.StateOps;
import org.apache.helix.wagedsim.source.SpecSource;
import org.apache.helix.wagedsim.stats.StatsCollector;
import org.testng.Assert;
import org.testng.annotations.Test;

public class TestDryRunEngine {

  private static ClusterState cluster(boolean delay) throws Exception {
    return SpecSource.build(TestClusters.spec("TEST_DRY", 12, 2, 24, delay));
  }

  @Test
  public void testColdStartPlacesEveryReplica() throws Exception {
    ClusterState state = cluster(false);
    Map<String, Map<String, Map<String, String>>> served = state.getServedLayout();
    Assert.assertEquals(served.size(), 2);
    for (Map<String, Map<String, String>> partitions : served.values()) {
      Assert.assertEquals(partitions.size(), 24);
      for (Map<String, String> replicas : partitions.values()) {
        Assert.assertEquals(replicas.size(), 3, replicas.toString());
        Assert.assertEquals(replicas.values().stream().filter("MASTER"::equals).count(), 1);
      }
    }
    Assert.assertNotNull(state.getBaseline());
    Assert.assertNotNull(state.getBestPossible());
    StatsCollector collector = new StatsCollector(state, "CU");
    Map<String, Object> stats = collector.stats(served, null, state.getBaseline(), collector.nodes(served));
    Assert.assertEquals(stats.get("violations.capacity"), 0);
    Assert.assertEquals(stats.get("missingTopState"), 0);
    Assert.assertEquals(stats.get("underReplicated"), 0);
  }

  @Test
  public void testSteadyStateRoundMovesNothing() throws Exception {
    ClusterState state = cluster(false);
    Map<String, Map<String, Map<String, String>>> before = Layouts.copy(state.getServedLayout());
    try (DryRunEngine engine = new DryRunEngine()) {
      engine.start(state, new EngineSettings());
      RoundResult result = engine.runRound(1);
      Assert.assertTrue(result.failures.isEmpty(), result.failures.toString());
      Assert.assertEquals((long) result.passes.get("global"), 0L);
      Assert.assertEquals(engine.state().getServedLayout(), before);
    }
  }

  @Test
  public void testKilledNodeIsReplacedWithoutDelay() throws Exception {
    ClusterState state = cluster(false);
    try (DryRunEngine engine = new DryRunEngine()) {
      engine.start(state, new EngineSettings());
      Assert.assertTrue(StateOps.kill(engine.state(), "node_00"));
      engine.runRound(1);
      Map<String, Map<String, Map<String, String>>> served = engine.state().getServedLayout();
      for (Map<String, Map<String, String>> partitions : served.values()) {
        for (Map<String, String> replicas : partitions.values()) {
          Assert.assertFalse(replicas.containsKey("node_00"));
          Assert.assertEquals(replicas.values().stream().filter("MASTER"::equals).count(), 1);
          Assert.assertEquals(replicas.size(), 3);
        }
      }
    }
  }

  @Test
  public void testDelayWindowKeepsReplicasUntilClockAdvances() throws Exception {
    ClusterState state = cluster(true);
    try (DryRunEngine engine = new DryRunEngine()) {
      engine.start(state, new EngineSettings());
      StateOps.kill(engine.state(), "node_00");
      engine.runRound(1);
      // Inside the delay window the best possible keeps node_00; its replicas are not served.
      Assert.assertTrue(engine.state().getBestPossible().values().stream()
          .flatMap(p -> p.values().stream()).anyMatch(r -> r.containsKey("node_00")));
      engine.advanceClock(2 * 3600_000L);
      engine.runRound(2);
      Assert.assertFalse(engine.state().getBestPossible().values().stream()
          .flatMap(p -> p.values().stream()).anyMatch(r -> r.containsKey("node_00")));
    }
  }

  @Test
  public void testForcedPassesAndWeights() throws Exception {
    ClusterState state = cluster(false);
    EngineSettings settings = new EngineSettings();
    settings.pass = EngineSettings.Pass.COLD;
    settings.constraintWeights.put("TopStateMaxCapacityUsageInstanceConstraint", 12f);
    try (DryRunEngine engine = new DryRunEngine()) {
      engine.start(state, settings);
      RoundResult result = engine.runRound(1);
      Assert.assertTrue(result.failures.isEmpty(), result.failures.toString());
      Assert.assertEquals((long) result.passes.get("cold"), 1L);
      Assert.assertEquals(engine.state().getBaseline(), engine.state().getBestPossible());
    }
  }

  @Test
  public void testDeterministic() throws Exception {
    ClusterState first = cluster(false);
    ClusterState second = cluster(false);
    Assert.assertEquals(first.getServedLayout(), second.getServedLayout());
    Assert.assertEquals(first.getBaseline(), second.getBaseline());
  }
}
