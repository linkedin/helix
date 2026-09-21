package org.apache.helix.controller.rebalancer.waged.model;

/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.helix.controller.rebalancer.waged.constraints.ConstraintBasedAlgorithm;
import org.apache.helix.model.ClusterConfig;
import org.apache.helix.monitoring.mbeans.ClusterStatusMonitor;
import org.testng.Assert;
import org.testng.annotations.Test;

/**
 * Isolation deliberately turns a thrown rebalance failure into a quiet partial success. That is the
 * whole point of the mode, but it also means every existing failure signal stays clean while a
 * clique goes unplaced indefinitely: no exception reaches the caller, no failure counter moves, and
 * the baseline health gauge still reads healthy.
 * <p>
 * Without a dedicated signal a permanently frozen clique is invisible, which trades a loud outage
 * for a silent one. These tests pin the signal that makes it visible again.
 */
public class TestWagedInstanceTagIsolationMetric extends AbstractTestWagedInstanceTagIsolation {

  /**
   * The reporter must fire with the skipped resources when a clique is isolated, and fire again
   * with an empty set once the cluster is healthy, so the gauge it drives is reversible rather than
   * latching on forever.
   */
  @Test
  public void testIsolationReporterFiresAndReverses() throws Exception {
    AtomicReference<Set<String>> lastReported = new AtomicReference<>();
    List<ClusterModel.RebalanceScopeType> scopes = new ArrayList<>();

    ConstraintBasedAlgorithm algorithm = (ConstraintBasedAlgorithm) createAlgorithm();
    algorithm.setIsolationSnapshotReporter((scope, skipped) -> {
      scopes.add(scope);
      lastReported.set(skipped);
    });

    ClusterConfig clusterConfig = createClusterConfig(true);

    // A healthy cluster reports an empty set, never null, so the gauge reads zero.
    algorithm.calculate(createClusterModel(clusterConfig, allHealthy()));
    Assert.assertNotNull(lastReported.get(),
        "The reporter must fire even when nothing was isolated, otherwise the gauge can never "
            + "fall back to zero after a clique recovers");
    Assert.assertTrue(lastReported.get().isEmpty(),
        "A healthy cluster must report no skipped resources, saw " + lastReported.get());

    // Breaking one clique must report exactly that clique's resource.
    Map<Integer, CliqueSpec> broken = new HashMap<>(allHealthy());
    broken.put(3, CliqueSpec.healthy().withPartitionWeight(UNPLACEABLE_PARTITION_WEIGHT));
    algorithm.calculate(createClusterModel(clusterConfig, broken));
    Assert.assertEquals(lastReported.get(), Collections.singleton(resourceName(3)),
        "The isolated clique's resource must be reported, otherwise a frozen clique is invisible: "
            + "isolation throws no exception and moves no failure counter");

    // Repairing it must bring the report back to empty.
    algorithm.calculate(createClusterModel(clusterConfig, allHealthy()));
    Assert.assertTrue(lastReported.get().isEmpty(),
        "The reported set must return to empty once the cluster is healthy again, saw "
            + lastReported.get());

    Assert.assertEquals(scopes.size(), 3, "The reporter must fire exactly once per run");
  }

  /**
   * Several broken cliques must all be counted, so the gauge reflects the true blast radius rather
   * than merely "something is wrong".
   */
  @Test
  public void testEveryIsolatedCliqueIsCounted() throws Exception {
    AtomicReference<Set<String>> lastReported = new AtomicReference<>();
    ConstraintBasedAlgorithm algorithm = (ConstraintBasedAlgorithm) createAlgorithm();
    algorithm.setIsolationSnapshotReporter((scope, skipped) -> lastReported.set(skipped));

    ClusterConfig clusterConfig = createClusterConfig(true);
    Map<Integer, CliqueSpec> specs = new HashMap<>(allHealthy());
    for (int clique : new int[] {2, 7, 11}) {
      specs.put(clique, CliqueSpec.healthy().withPartitionWeight(UNPLACEABLE_PARTITION_WEIGHT));
    }
    algorithm.calculate(createClusterModel(clusterConfig, specs));

    Assert.assertEquals(lastReported.get().size(), 3,
        "All three broken cliques must be counted, saw " + lastReported.get());
  }

  /**
   * With the feature disabled the reporter must never see a skipped resource, because the default
   * mode throws instead of skipping. The gauge therefore reads zero on every cluster that has not
   * opted in.
   */
  @Test
  public void testDisabledFeatureNeverReportsSkippedResources() throws Exception {
    AtomicReference<Set<String>> lastReported = new AtomicReference<>();
    ConstraintBasedAlgorithm algorithm = (ConstraintBasedAlgorithm) createAlgorithm();
    algorithm.setIsolationSnapshotReporter((scope, skipped) -> lastReported.set(skipped));

    ClusterConfig clusterConfig = createClusterConfig(false);
    Map<Integer, CliqueSpec> broken = new HashMap<>(allHealthy());
    broken.put(3, CliqueSpec.healthy().withPartitionWeight(UNPLACEABLE_PARTITION_WEIGHT));
    try {
      algorithm.calculate(createClusterModel(clusterConfig, broken));
      Assert.fail("The default global mode must still fail the whole rebalance");
    } catch (Exception expected) {
      // The default mode fails the whole rebalance, which is the behaviour being preserved.
    }
    Assert.assertTrue(lastReported.get() == null || lastReported.get().isEmpty(),
        "With isolation disabled nothing may ever be reported as skipped, saw "
            + lastReported.get());
  }

  /**
   * The monitor gauge itself has to be reversible and start at zero, so an alert on "greater than
   * zero for an hour" means a clique really is still frozen rather than that one ever was.
   */
  @Test
  public void testMonitorGaugeIsReversible() {
    ClusterStatusMonitor monitor = new ClusterStatusMonitor("TestIsolationMetricCluster");
    Assert.assertEquals(monitor.getWagedInstanceTagIsolationSkippedResourcesGauge(), 0L,
        "The gauge must start at zero on a cluster that never isolated anything");

    monitor.updateWagedInstanceTagIsolationSkippedResources(3L);
    Assert.assertEquals(monitor.getWagedInstanceTagIsolationSkippedResourcesGauge(), 3L);

    monitor.updateWagedInstanceTagIsolationSkippedResources(0L);
    Assert.assertEquals(monitor.getWagedInstanceTagIsolationSkippedResourcesGauge(), 0L,
        "The gauge must fall back to zero once a baseline places everything, otherwise it latches "
            + "on and a recovered clique still looks broken");
  }
}
