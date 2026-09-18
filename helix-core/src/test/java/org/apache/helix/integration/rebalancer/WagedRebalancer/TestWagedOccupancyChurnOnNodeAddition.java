package org.apache.helix.integration.rebalancer.WagedRebalancer;

/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
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
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;

import com.google.common.collect.ImmutableMap;
import org.apache.helix.HelixDataAccessor;
import org.apache.helix.integration.manager.ClusterControllerManager;
import org.apache.helix.integration.manager.MockParticipantManager;
import org.apache.helix.manager.zk.ZKHelixDataAccessor;
import org.apache.helix.model.BuiltInStateModelDefinitions;
import org.apache.helix.model.ClusterConfig;
import org.apache.helix.model.ExternalView;
import org.apache.helix.common.ZkTestBase;
import org.testng.Assert;
import org.testng.annotations.Test;

/**
 * Reproduction of the churn claim raised in review of PR #245: that feeding current state into
 * placement scoring makes best-possible unstable, so a target the planner already picked and
 * abandoned gets picked again.
 * <p>
 * Sized so the constraint is actually active. The existing no-churn test runs each instance at
 * roughly a third of capacity, so hidden occupancy is empty there for every candidate and it
 * cannot observe churn either way.
 */
public class TestWagedOccupancyChurnOnNodeAddition extends ZkTestBase {
  private static final String CAPACITY_KEY = "SLOT";
  private static final int INSTANCE_CAPACITY = 12;
  private static final int PARTITION_WEIGHT = 1;
  private static final int NUM_NODES = 10;
  private static final int NUM_PARTITIONS = 110;
  private static final int NUM_REPLICAS = 1;
  private static final String RESOURCE = "churnResource";
  /**
   * Absorbs external-view sampling jitter only: the view is polled every 150ms, so a short-lived
   * intermediate placement may be caught in one run and missed in the other. Kept far below
   * NUM_PARTITIONS so it cannot mask genuine churn.
   */
  private static final int TRANSITION_SAMPLING_SLACK = 5;

  private static class Result {
    int transitions;
    int revisits;
    final List<String> examples = new ArrayList<>();
  }

  private Result runExperiment(boolean flagOn, int startPort, String tag) throws Exception {
    String clusterName = CLUSTER_PREFIX + "_churn_" + tag;
    _gSetupTool.addCluster(clusterName, true);

    List<String> nodes = new ArrayList<>();
    for (int i = 0; i < NUM_NODES; i++) {
      String node = PARTICIPANT_PREFIX + "_" + (startPort + i);
      _gSetupTool.addInstanceToCluster(clusterName, node);
      nodes.add(node);
    }

    HelixDataAccessor accessor = new ZKHelixDataAccessor(clusterName, _baseAccessor);
    ClusterConfig cfg = accessor.getProperty(accessor.keyBuilder().clusterConfig());
    cfg.setInstanceCapacityKeys(Collections.singletonList(CAPACITY_KEY));
    cfg.setDefaultInstanceCapacityMap(ImmutableMap.of(CAPACITY_KEY, INSTANCE_CAPACITY));
    cfg.setDefaultPartitionWeightMap(ImmutableMap.of(CAPACITY_KEY, PARTITION_WEIGHT));
    cfg.setWagedCountUnallocatedOccupancyEnabled(flagOn);
    accessor.setProperty(accessor.keyBuilder().clusterConfig(), cfg);

    List<MockParticipantManager> participants = new ArrayList<>();
    for (String node : nodes) {
      MockParticipantManager p = new MockParticipantManager(ZK_ADDR, clusterName, node);
      p.syncStart();
      participants.add(p);
    }
    ClusterControllerManager controller =
        new ClusterControllerManager(ZK_ADDR, clusterName, "controller_" + tag);
    controller.syncStart();
    enablePersistBestPossibleAssignment(_gZkClient, clusterName, true);

    createResourceWithWagedRebalance(clusterName, RESOURCE,
        BuiltInStateModelDefinitions.LeaderStandby.name(), NUM_PARTITIONS, NUM_REPLICAS,
        NUM_REPLICAS);
    _gSetupTool.rebalanceStorageCluster(clusterName, RESOURCE, NUM_REPLICAS);

    // Let the initial placement settle before the perturbation under study.
    awaitFullyPlaced(accessor, 120000L);
    Thread.sleep(3000);

    // The perturbation: one new node joins a settled, fairly full cluster.
    String newNode = PARTICIPANT_PREFIX + "_" + (startPort + NUM_NODES);
    _gSetupTool.addInstanceToCluster(clusterName, newNode);
    MockParticipantManager fresh = new MockParticipantManager(ZK_ADDR, clusterName, newNode);
    fresh.syncStart();
    participants.add(fresh);

    // Record, per partition, the ordered sequence of distinct occupancy snapshots. Only changes
    // are appended, so the sequence length is the number of observed placement revisions.
    Map<String, List<Set<String>>> history = new LinkedHashMap<>();
    long deadline = System.currentTimeMillis() + 90000L;
    while (System.currentTimeMillis() < deadline) {
      ExternalView ev = accessor.getProperty(accessor.keyBuilder().externalView(RESOURCE));
      if (ev != null) {
        for (String partition : ev.getPartitionSet()) {
          Map<String, String> stateMap = ev.getStateMap(partition);
          if (stateMap == null || stateMap.isEmpty()) {
            continue;
          }
          Set<String> snapshot = new HashSet<>(stateMap.keySet());
          List<Set<String>> seq = history.computeIfAbsent(partition, k -> new ArrayList<>());
          if (seq.isEmpty() || !seq.get(seq.size() - 1).equals(snapshot)) {
            seq.add(snapshot);
          }
        }
      }
      Thread.sleep(150L);
    }

    // A revisit is an instance that hosted the partition, stopped hosting it, and hosted it again.
    // That is the signature of the planner abandoning a target and then choosing it once more.
    Result result = new Result();
    for (Map.Entry<String, List<Set<String>>> e : history.entrySet()) {
      List<Set<String>> seq = e.getValue();
      result.transitions += Math.max(0, seq.size() - 1);
      Set<String> everSeen = new HashSet<>();
      seq.forEach(everSeen::addAll);
      boolean revisited = false;
      for (String instance : everSeen) {
        boolean present = false;
        boolean departed = false;
        for (Set<String> snapshot : seq) {
          boolean here = snapshot.contains(instance);
          if (present && !here) {
            departed = true;
          } else if (departed && here) {
            revisited = true;
            break;
          }
          present = here;
        }
        if (revisited) {
          break;
        }
      }
      if (revisited) {
        result.revisits++;
        if (result.examples.size() < 8) {
          StringBuilder sb = new StringBuilder(e.getKey()).append(": ");
          for (Set<String> snapshot : seq) {
            sb.append(new TreeMap<>(snapshot.stream()
                .collect(java.util.stream.Collectors.toMap(k -> k, k -> ""))).keySet()).append(" ");
          }
          result.examples.add(sb.toString().trim());
        }
      }
    }

    for (MockParticipantManager p : participants) {
      p.syncStop();
    }
    controller.syncStop();
    deleteCluster(clusterName);
    return result;
  }

  private void awaitFullyPlaced(HelixDataAccessor accessor, long timeoutMs) throws Exception {
    long deadline = System.currentTimeMillis() + timeoutMs;
    int placed = 0;
    while (System.currentTimeMillis() < deadline) {
      ExternalView ev = accessor.getProperty(accessor.keyBuilder().externalView(RESOURCE));
      placed = 0;
      if (ev != null) {
        for (String partition : ev.getPartitionSet()) {
          Map<String, String> stateMap = ev.getStateMap(partition);
          if (stateMap != null && !stateMap.isEmpty()) {
            placed++;
          }
        }
      }
      if (placed == NUM_PARTITIONS) {
        return;
      }
      Thread.sleep(250L);
    }
    Assert.fail("resource never fully placed: " + placed + "/" + NUM_PARTITIONS);
  }

  @Test
  public void testTargetRevisitsWithFlagOffVersusOn() throws Exception {
    System.out.println("START churn experiment at " + new Date(System.currentTimeMillis()));

    Result off = runExperiment(false, 14100, "off");
    System.out.println("[CHURN] flag OFF -> transitions=" + off.transitions + " partitionsRevisited="
        + off.revisits);
    off.examples.forEach(x -> System.out.println("[CHURN]   OFF example " + x));

    Result on = runExperiment(true, 14200, "on");
    System.out.println("[CHURN] flag ON  -> transitions=" + on.transitions + " partitionsRevisited="
        + on.revisits);
    on.examples.forEach(x -> System.out.println("[CHURN]   ON example " + x));

    System.out.println("[CHURN] RESULT off(transitions=" + off.transitions + ", revisits="
        + off.revisits + ")  on(transitions=" + on.transitions + ", revisits=" + on.revisits + ")");

    // A revisit is the ping-pong signature: an instance hosts the partition, loses it, and hosts
    // it again. The gate is supposed to make this structurally impossible, because a plan's own
    // placements are excluded from hidden occupancy and hidden occupancy only ever counts states
    // the planner cannot assign. So the planner's output can never feed back into the input that
    // would move it. Zero is therefore the only acceptable value, not merely "no worse than off".
    Assert.assertEquals(on.revisits, 0,
        "enabling the flag must not make the planner revisit a target it had abandoned; examples="
            + on.examples);

    // Comparative guard, so this still fails if the flag ever becomes worse than the baseline even
    // in a scenario where the baseline itself churns.
    Assert.assertTrue(on.revisits <= off.revisits,
        "flag ON revisited more partitions than flag OFF: on=" + on.revisits + " off=" + off.revisits
            + " examples=" + on.examples);

    // Placement movement must not grow either. The slack absorbs sampling jitter only -- the
    // external view is polled every 150ms, so a short-lived intermediate placement can be observed
    // in one run and missed in the other. It is deliberately a small fraction of NUM_PARTITIONS so
    // it cannot silently absorb real churn.
    Assert.assertTrue(on.transitions <= off.transitions + TRANSITION_SAMPLING_SLACK,
        "enabling the flag increased placement movement beyond sampling jitter: on="
            + on.transitions + " off=" + off.transitions + " slack=" + TRANSITION_SAMPLING_SLACK);
  }
}
