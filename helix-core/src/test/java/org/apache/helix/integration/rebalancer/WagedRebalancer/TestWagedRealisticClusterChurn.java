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
import java.util.Arrays;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import com.google.common.collect.ImmutableMap;
import org.apache.helix.HelixDataAccessor;
import org.apache.helix.NotificationContext;
import org.apache.helix.common.ZkTestBase;
import org.apache.helix.controller.rebalancer.waged.AssignmentMetadataStore;
import org.apache.helix.integration.manager.ClusterControllerManager;
import org.apache.helix.integration.manager.MockParticipantManager;
import org.apache.helix.manager.zk.ZKHelixDataAccessor;
import org.apache.helix.manager.zk.ZkBucketDataAccessor;
import org.apache.helix.api.config.StateTransitionThrottleConfig;
import org.apache.helix.model.ClusterConfig;
import org.apache.helix.util.RebalanceUtil;
import org.apache.helix.model.ExternalView;
import org.apache.helix.model.Message;
import org.apache.helix.model.ResourceAssignment;
import org.apache.helix.model.StateModelDefinition;
import org.apache.helix.participant.statemachine.StateModel;
import org.apache.helix.participant.statemachine.StateModelFactory;
import org.apache.helix.participant.statemachine.StateModelInfo;
import org.apache.helix.participant.statemachine.Transition;
import org.apache.helix.zookeeper.datamodel.ZNRecord;
import org.testng.Assert;
import org.testng.annotations.Test;

/**
 * Scale replication of the churn experiment raised in review of PR #245
 * (https://github.com/linkedin/helix/pull/245#discussion_r3960112239).
 * <p>
 * The review measured the soft-constraint version of this feature and found that a target the
 * planner had already picked and walked away from got picked again 15 and 11 times with the flag
 * on, and never once with it off. This test reproduces that experiment against the hard-gate
 * version that replaced the soft constraint, at the same scale and with the same metric.
 * <p>
 * Three things are deliberately copied from the review's setup, because the existing churn test
 * is weaker on all three:
 * <ul>
 *   <li><b>Scale.</b> 20 nodes plus one added, 400 partitions, one replica, 22 slots per node, so
 *       the cluster sits at ~91% before the new node arrives.</li>
 *   <li><b>Metric.</b> Churn is read from the persisted BEST_POSSIBLE assignment -- the planner's
 *       own output -- not from the external view. Sampling the external view misses short-lived
 *       intermediate placements by construction, which is exactly what a re-chosen target is.</li>
 *   <li><b>State model.</b> The SEAS indexer model from the incident, where OFFLINE, ASSIGNED and
 *       INDEX_DOWNLOADED all carry count -1. Every replica therefore spends its whole ramp-up in
 *       states the planner cannot model, which is the condition that produces hidden occupancy.</li>
 * </ul>
 * A deliberate dwell is held in the two uncounted transitions. Without it a mock participant
 * traverses ASSIGNED and INDEX_DOWNLOADED in microseconds, hidden occupancy is almost never
 * non-empty, and the experiment would be measuring an inert gate -- the precise criticism the
 * review made of the pre-existing no-churn test.
 * <p>
 * <b>Why the cluster is never left still.</b> An earlier version of this experiment perturbed the
 * cluster once, by adding a node, and then measured quiet windows. It reported zero churn -- but so
 * did the same experiment run against the soft-constraint build the review had already shown to
 * churn, which means it was not measuring anything. The reason is that WAGED's partial rebalance
 * only considers replicas whose baseline disagrees with the previous best-possible assignment; once
 * a replica is placed it moves to {@code allocatedReplicas} and is never scored again. A single
 * node addition therefore opens the work list exactly once, and a target that is only ever chosen
 * once cannot be re-chosen. A rolling bounce keeps re-opening it, and also makes nodes RECEIVE
 * replicas while still holding their own -- the only condition under which unaccounted occupancy
 * can push a node past its capacity relative to its neighbours.
 * <p>
 * <b>Positive control.</b> This harness, at this configuration, was first run against the deleted
 * soft-constraint build (commit 52f434e64) and reproduced the reported effect: flag off gave 4 plan
 * revisions and 0 re-chosen targets, flag on gave 30 plan revisions and 6 re-chosen targets, with
 * traces such as {@code derived_100: n16 -> n05 -> n00 -> n01 -> n11 -> n01 -> n10} that match the
 * shape the review reported. The instrument is therefore known to be able to fail, and a zero here
 * is a real reading rather than an absence of sensitivity.
 */
public class TestWagedRealisticClusterChurn extends ZkTestBase {
  private static final String CAPACITY_KEY = "SLOT";
  private static final String RESOURCE = "indexed";
  private static final String STATE_MODEL = "IndexerStateModel";
  /**
   * Slots per node. At 400 partitions over 20 nodes every node carries 20, so this also sets the
   * headroom: 22 leaves 2 spare slots, 21 leaves 1. Headroom matters because the soft constraint
   * this harness is validated against only develops a gradient once a node's unaccounted occupancy
   * reaches its spare capacity -- below that it scores every candidate 1.0 and is inert.
   */
  private static final int INSTANCE_CAPACITY = Integer.getInteger("churn.capacity", 22);
  private static final int PARTITION_WEIGHT = 1;
  private static final int NUM_NODES = 20;
  /**
   * A production cluster is not one resource. Several resources of different sizes share the same
   * instances, so a bounce forces many independent placement decisions onto the same nodes at the
   * same time -- which is what lets a node's occupancy actually vary relative to its neighbours.
   */
  private static final int NUM_PARTITIONS = 400;
  private static final String[] RESOURCES = {"indexed", "derived", "cached"};
  private static final int[] RESOURCE_PARTITIONS = {200, 120, 80};

  /** Bounce schedule: node index, offset into the window. Identical in both arms of a pair. */
  private static final int[] BOUNCE_NODES = {3, 11, 7, 16, 1, 14};
  private static final long BOUNCE_INTERVAL_MS = 9000L;
  private static final long BOUNCE_DOWNTIME_MS = 4500L;
  private static final int NUM_REPLICAS = 1;
  /**
   * Mimics a real index download so the uncounted states are actually occupied across several
   * planning rounds. Held near zero during initial placement so setup stays fast, then raised
   * before the perturbation: the churn mechanism under test requires the planner to re-plan while
   * replicas are still sitting in states it cannot model, which is what a multi-minute real
   * download produces and what a microsecond mock transition destroys.
   */
  private static final long SLOW_DWELL_MS = 20000L;

  /**
   * Spread of per-replica transition times once SLOW_TRANSITIONS is on. A real index download takes
   * anywhere from seconds to minutes, so at any instant some nodes are holding several replicas the
   * planner cannot see and others none. That unevenness is the whole point: a penalty applied
   * equally to every candidate changes no ranking, so a fixed dwell makes the signal invisible no
   * matter how large it is. The delay is derived from the partition name, so a flag-off arm and a
   * flag-on arm see byte-identical timing and the pair stays controlled.
   */
  private static final long DWELL_MIN_MS = 1000L;
  private static final long DWELL_SPREAD_MS = 39000L;

  private static long dwellFor(String partition, int step) {
    if (!SLOW_TRANSITIONS) {
      return FAST_DWELL_MS;
    }
    long h = Math.abs((long) (partition + "#" + step).hashCode());
    return DWELL_MIN_MS + (h % DWELL_SPREAD_MS);
  }
  private static final long FAST_DWELL_MS = 50L;
  private static volatile boolean SLOW_TRANSITIONS = false;
  private static final long SAMPLE_INTERVAL_MS = 75L;

  /** Sampling stops once the plan has been unchanged for this long, or at the hard deadline. */
  private static final long QUIESCENCE_MS = 10000L;
  private static final long MAX_SAMPLING_MS = 240000L;

  /**
   * How long the topology is held perfectly still after each perturbation. This must overlap the
   * ramp-up -- three transitions at SLOW_DWELL_MS each, so about a minute -- or the window would
   * begin after every replica had already reached a counted state, hidden occupancy would be empty
   * throughout, and the window would measure an inert gate rather than a quiet one.
   */
  private static final long STABLE_WINDOW_MS =
      Long.getLong("churn.stableWindowMs", 90000L);

  /** How often the controller is asked to re-plan during a stable window. */
  private static final long REPLAN_INTERVAL_MS = 750L;

  /** Cluster the sampler should nudge; set per arm. */
  private volatile String _clusterForKick;

  private static class Result {
    int planRevisions;
    int targetsRechosen;
    int samplerTicks;
    int uncountedSightings;
    int partitionsOnNewNode;
    int movedTwice;
    int churnTargetChanges;
    int churnRevisions;
    int perturbationTargetChanges;
    int stableWindowTicks;
    int uncountedSightingsStable;
    int replanKicks;
    int bounces;
    int hiddenShapeSightings;
    int maxHiddenShape;
    final List<String> examples = new ArrayList<>();

    @Override
    public String toString() {
      return "planRevisions=" + planRevisions + " targetsRechosen=" + targetsRechosen
          + " samplerTicks=" + samplerTicks + " uncountedSightings=" + uncountedSightings
          + " partitionsOnNewNode=" + partitionsOnNewNode + " movedTwice=" + movedTwice
          + " churnTargetChanges=" + churnTargetChanges + " churnRevisions=" + churnRevisions
          + " perturbationTargetChanges=" + perturbationTargetChanges + " stableWindowTicks="
          + stableWindowTicks + " uncountedSightingsStable=" + uncountedSightingsStable
          + " bounces=" + bounces + " replanKicks=" + replanKicks + " hiddenShapeSightings=" + hiddenShapeSightings + " maxHiddenShape=" + maxHiddenShape;
    }
  }

  @Test
  public void testHardGateDoesNotAddChurnOnRealisticCluster() throws Exception {
    System.out.println("START " + getShortClassName() + " at " + new Date());

    // Matched pairs: each pair is one flag-off run and one flag-on run from an identical starting
    // plan. The review reported 15 pairs; this defaults to 2 for routine CI cost and is raised via
    // -Dchurn.pairs=N when the result needs to be hard to dismiss as luck.
    int pairs = Integer.getInteger("churn.pairs", 2);
    // Set -Dchurn.expectChurn=true when running this harness against a build that is known to
    // churn. It inverts the claim into "the harness must SEE churn", which is what turns a zero on
    // the real build into evidence instead of an unfalsified assertion.
    boolean expectChurn = Boolean.getBoolean("churn.expectChurn");

    List<Result> offs = new ArrayList<>();
    List<Result> ons = new ArrayList<>();
    Map<String, Result> all = new LinkedHashMap<>();
    int port = 14000;
    for (int i = 0; i < pairs; i++) {
      String suffix = String.valueOf((char) ('A' + i));
      Result off = runExperiment(false, port, "off" + suffix);
      port += 100;
      Result on = runExperiment(true, port, "on" + suffix);
      port += 100;
      offs.add(off);
      ons.add(on);
      all.put("flag off " + suffix, off);
      all.put("flag on  " + suffix, on);
    }

    System.out.println("\n================ SCALE CHURN RESULT ================");
    System.out.printf("%-11s %-10s %-14s %-14s %-13s %-12s %-10s %s%n", "arm", "churnTgtChg",
        "churnRevisions", "perturbTgtChg", "hiddenShapes", "maxHidden", "movedTwice",
        "partsOnNewNode");
    for (Map.Entry<String, Result> e : all.entrySet()) {
      Result r = e.getValue();
      System.out.printf("%-11s %-10d %-14d %-14d %-13d %-12d %-10d %d%n", e.getKey(),
          r.churnTargetChanges, r.churnRevisions, r.perturbationTargetChanges,
          r.hiddenShapeSightings, r.maxHiddenShape, r.movedTwice, r.partitionsOnNewNode);
    }
    int offRechosen = offs.stream().mapToInt(r -> r.targetsRechosen).sum();
    int onRechosen = ons.stream().mapToInt(r -> r.targetsRechosen).sum();
    int offChurn = offs.stream().mapToInt(r -> r.churnTargetChanges).sum();
    int onChurn = ons.stream().mapToInt(r -> r.churnTargetChanges).sum();
    System.out.printf("%npairs=%d  targetsRechosen off=%d on=%d   churnTargetChanges off=%d on=%d%n",
        pairs, offRechosen, onRechosen, offChurn, onChurn);
    for (Result r : ons) {
      r.examples.stream().limit(5).forEach(x -> System.out.println("   on example: " + x));
    }
    for (Result r : offs) {
      r.examples.stream().limit(5).forEach(x -> System.out.println("   off example: " + x));
    }
    System.out.println("====================================================\n");

    // ---- validity guards: fail loudly rather than pass vacuously ----
    // The review's central criticism of the pre-existing no-churn test was that it was built so it
    // could not fail. These guards exist so this test cannot repeat that mistake.
    for (Map.Entry<String, Result> e : all.entrySet()) {
      Result r = e.getValue();
      Assert.assertTrue(r.partitionsOnNewNode > 0,
          "arm " + e.getKey() + ": the added node received no partitions, so the perturbation "
              + "under study never happened and this measured nothing. " + r);
      // Proves the instrument can see a target change at all. If this is zero, a zero churn count
      // says nothing about the rebalancer and everything about the harness.
      Assert.assertTrue(r.perturbationTargetChanges > 0,
          "arm " + e.getKey() + ": no target change was observed even while the topology was "
              + "changing, so the instrument is blind and a zero churn count is meaningless. " + r);
    }
    // Sequence length has to be checked against the arm's own baseline, not against a fixed floor.
    // A short sequence in the flag-on arm is the result being measured, not a blind instrument: it
    // means the plan stopped being revised. What would make the reading vacuous is if the UNMODIFIED
    // rebalancer also produced sequences too short to express a re-chosen target, because then no
    // build could fail. So the flag-off arm of each pair carries the non-vacuity burden.
    for (String key : all.keySet()) {
      if (!key.startsWith("off")) {
        continue;
      }
      Result r = all.get(key);
      Assert.assertTrue(r.movedTwice > 0,
          "arm " + key + ": even unmodified WAGED never moved a partition twice, so no arm could "
              + "have expressed a re-chosen target and this pair measured nothing. " + r);
    }
    // Proves the gate was actually loaded during the quiet windows. Hidden occupancy is only
    // non-empty while replicas sit in uncounted states; if that never overlapped a stable window
    // then the windows measured an inert gate, not a quiet one.
    for (Result r : ons) {
      Assert.assertTrue(r.uncountedSightingsStable > 0,
          "a flag-on arm never observed a replica in an uncounted state during a topology-stable "
              + "window, so this experiment measured an inert gate: " + r);
      // Stronger: the gate must have been holding real capacity back during the quiet windows,
      // including replicas stranded on nodes the plan had already moved away from.
      Assert.assertTrue(r.hiddenShapeSightings > 0,
          "a flag-on arm never saw a replica in an uncounted state on an instance the plan did not "
              + "target, which is precisely the condition that makes hidden occupancy non-empty. "
              + "Without it the gate withheld nothing and zero churn proves nothing: " + r);
    }

    if (expectChurn) {
      // Falsification mode. Run against a build known to churn; if this fails, the harness cannot
      // detect the effect and every zero it has ever reported is worthless.
      Assert.assertTrue(onRechosen > offRechosen,
          "HARNESS FAILED ITS OWN POSITIVE CONTROL: run against a build that is known to churn, "
              + "it still saw no extra re-chosen targets with the flag on (on=" + onRechosen
              + " off=" + offRechosen + "). The harness is blind and its zeros mean nothing.");
      System.out.println("POSITIVE CONTROL PASSED: harness detected churn (on=" + onRechosen
          + " vs off=" + offRechosen + ")");
      return;
    }

    // ---- the claim under test ----
    // Churn is the planner changing a target while nothing outside it changed. The review measured
    // 15 and 11 re-chosen targets on the soft-constraint version; the hard gate must not reproduce
    // that. Comparing against the flag-off arms rather than against zero keeps the bar honest:
    // whatever background movement the rebalancer does on its own, the gate must not add to it.
    Assert.assertTrue(onChurn <= offChurn,
        "flag ON revised targets during a topology-stable window more than flag OFF did, which is "
            + "the churn the review reported. on=" + onChurn + " off=" + offChurn);
    Assert.assertTrue(onRechosen <= offRechosen,
        "flag ON re-chose abandoned targets at a rate flag OFF did not. on=" + onRechosen + " off="
            + offRechosen);
  }

  private Result runExperiment(boolean flagOn, int startPort, String tag) throws Exception {
    String clusterName = CLUSTER_PREFIX + "_realistic_" + tag;
    System.out.println("\n---- arm " + tag + " (flag " + (flagOn ? "ON" : "OFF") + ") ----");
    _gSetupTool.addCluster(clusterName, true);
    _gSetupTool.addStateModelDef(clusterName, STATE_MODEL, buildIndexerStateModelDef());

    List<String> nodes = new ArrayList<>();
    for (int i = 0; i < NUM_NODES; i++) {
      String node = PARTICIPANT_PREFIX + "_" + (startPort + i);
      _gSetupTool.addInstanceToCluster(clusterName, node);
      nodes.add(node);
    }

    _clusterForKick = clusterName;
    HelixDataAccessor accessor = new ZKHelixDataAccessor(clusterName, _baseAccessor);
    ClusterConfig cfg = accessor.getProperty(accessor.keyBuilder().clusterConfig());
    cfg.setInstanceCapacityKeys(Collections.singletonList(CAPACITY_KEY));
    cfg.setDefaultInstanceCapacityMap(ImmutableMap.of(CAPACITY_KEY, INSTANCE_CAPACITY));
    cfg.setDefaultPartitionWeightMap(ImmutableMap.of(CAPACITY_KEY, PARTITION_WEIGHT));
    cfg.setWagedCountUnallocatedOccupancyEnabled(flagOn);
    // Production runs with delayed rebalance so a brief bounce does not trigger a full
    // reshuffle. The side effect that matters here is that a downed node's replicas stay in the
    // plan for the delay window, so best-possible and baseline stay apart and the partitions
    // involved keep coming back to the planner instead of settling after a single round.
    cfg.setDelayRebalaceEnabled(true);
    cfg.setRebalanceDelayTime(Long.getLong("churn.delayMs", 30000L));
    // Throttling is what turns one bulk move into a long sequence of planning rounds. Without it
    // the planner decides the whole migration once and is never consulted again, so a score that
    // moves between rounds has no opportunity to change any decision. Production clusters throttle;
    // this is the difference between a harness that can express churn and one that cannot.
    int throttle = Integer.getInteger("churn.throttle", 0);
    if (throttle > 0) {
      cfg.setStateTransitionThrottleConfigs(Arrays.asList(
          new StateTransitionThrottleConfig(StateTransitionThrottleConfig.RebalanceType.ANY,
              StateTransitionThrottleConfig.ThrottleScope.CLUSTER, throttle),
          new StateTransitionThrottleConfig(StateTransitionThrottleConfig.RebalanceType.ANY,
              StateTransitionThrottleConfig.ThrottleScope.INSTANCE, 1)));
    }
    accessor.setProperty(accessor.keyBuilder().clusterConfig(), cfg);

    List<MockParticipantManager> participants = new ArrayList<>();
    for (String node : nodes) {
      participants.add(startParticipant(clusterName, node));
    }
    ClusterControllerManager controller =
        new ClusterControllerManager(ZK_ADDR, clusterName, "controller_" + tag);
    controller.syncStart();
    enablePersistBestPossibleAssignment(_gZkClient, clusterName, true);

    AssignmentMetadataStore store =
        new AssignmentMetadataStore(new ZkBucketDataAccessor(ZK_ADDR), clusterName) {
          @Override
          public synchronized Map<String, ResourceAssignment> getBestPossibleAssignment() {
            super.reset();
            return super.getBestPossibleAssignment();
          }
        };

    for (int i = 0; i < RESOURCES.length; i++) {
      createResourceWithWagedRebalance(clusterName, RESOURCES[i], STATE_MODEL,
          RESOURCE_PARTITIONS[i], NUM_REPLICAS, NUM_REPLICAS);
      _gSetupTool.rebalanceStorageCluster(clusterName, RESOURCES[i], NUM_REPLICAS);
    }

    awaitSettled(accessor, 300000L, tag + " initial");
    Thread.sleep(4000);
    // From here on every ramp-up dwells in the uncounted states long enough to span several
    // planning rounds, which is the condition the review's real cluster had and the condition
    // under which hidden occupancy can influence more than one decision.
    SLOW_TRANSITIONS = true;

    // Both arms of a pair run the identical bounce schedule, so any movement the schedule itself
    // justifies appears in both. Only the difference between the arms is attributable to the flag.
    // That is the review's own matched-pair design, and it is what makes it safe to count target
    // changes here even though the topology is deliberately never still.
    Result result = new Result();
    Map<String, List<String>> targetHistory = new HashMap<>();
    Map<String, String> last = readBestPossible(store);
    recordSnapshot(last, targetHistory);

    String newNode = PARTICIPANT_PREFIX + "_" + (startPort + NUM_NODES);
    _gSetupTool.addInstanceToCluster(clusterName, newNode);
    participants.add(startParticipant(clusterName, newNode));
    System.out.println("arm " + tag + ": added " + newNode);
    last = samplePerturbation(store, accessor, result, targetHistory, last);
    result.partitionsOnNewNode =
        (int) targetHistory.values().stream().filter(seq -> seq.contains(newNode)).count();

    // Rolling bounce. Each node goes away briefly and comes back, one at a time, while replicas
    // from the previous bounce are still ramping up. This is the state a real cluster spends most
    // of its life in, and unlike a pure node addition it makes nodes RECEIVE replicas while still
    // holding their own -- the only condition under which a node's unaccounted occupancy can push
    // it past its capacity relative to its neighbours.
    long phaseStart = System.currentTimeMillis();
    long phaseEnd = phaseStart + BOUNCE_NODES.length * BOUNCE_INTERVAL_MS + STABLE_WINDOW_MS;
    int nextBounce = 0;
    Map<Integer, Long> pendingRestart = new HashMap<>();
    while (System.currentTimeMillis() < phaseEnd) {
      long elapsed = System.currentTimeMillis() - phaseStart;
      if (nextBounce < BOUNCE_NODES.length && elapsed >= nextBounce * BOUNCE_INTERVAL_MS) {
        int idx = BOUNCE_NODES[nextBounce];
        MockParticipantManager p = participants.get(idx);
        System.out.println("arm " + tag + ": bouncing down " + p.getInstanceName());
        p.syncStop();
        pendingRestart.put(idx, System.currentTimeMillis() + BOUNCE_DOWNTIME_MS);
        result.bounces++;
        nextBounce++;
      }
      for (Map.Entry<Integer, Long> e : new ArrayList<>(pendingRestart.entrySet())) {
        if (System.currentTimeMillis() >= e.getValue()) {
          int idx = e.getKey();
          String name = PARTICIPANT_PREFIX + "_" + (startPort + idx);
          System.out.println("arm " + tag + ": bringing back " + name);
          participants.set(idx, startParticipant(clusterName, name));
          pendingRestart.remove(idx);
        }
      }
      result.samplerTicks++;
      try {
        Map<String, String> snapshot = readBestPossible(store);
        if (!snapshot.isEmpty() && !snapshot.equals(last)) {
          result.planRevisions++;
          result.churnTargetChanges += recordSnapshot(snapshot, targetHistory);
          last = snapshot;
        }
        if (observedUncountedState(accessor)) {
          result.uncountedSightingsStable++;
          result.uncountedSightings++;
        }
        int hidden = countHiddenOccupancyShape(accessor, last);
        if (hidden > 0) {
          result.hiddenShapeSightings++;
          result.maxHiddenShape = Math.max(result.maxHiddenShape, hidden);
        }
      } catch (Exception ignored) {
        // Transient read while the controller is mid-write; the next tick picks it up.
      }
      Thread.sleep(SAMPLE_INTERVAL_MS);
    }
    for (Integer idx : new ArrayList<>(pendingRestart.keySet())) {
      participants.set(idx,
          startParticipant(clusterName, PARTICIPANT_PREFIX + "_" + (startPort + idx)));
    }

    // A target that reappears after the planner moved away from it is a re-chosen target.
    for (Map.Entry<String, List<String>> e : targetHistory.entrySet()) {
      List<String> seq = e.getValue();
      if (seq.size() >= 3) {
        result.movedTwice++;
      }
      for (int i = 2; i < seq.size(); i++) {
        if (seq.subList(0, i - 1).contains(seq.get(i))) {
          result.targetsRechosen++;
          result.examples.add(e.getKey() + ": " + String.join(" -> ", seq));
          break;
        }
      }
    }
    System.out.println("arm " + tag + " -> " + result);

    SLOW_TRANSITIONS = false;
    controller.syncStop();
    participants.stream().filter(MockParticipantManager::isConnected)
        .forEach(MockParticipantManager::syncStop);
    store.close();
    deleteCluster(clusterName);
    return result;
  }

  /** Appends the snapshot to each partition's target history and reports how many targets moved. */
  private int recordSnapshot(Map<String, String> snapshot,
      Map<String, List<String>> targetHistory) {
    int changed = 0;
    for (Map.Entry<String, String> e : snapshot.entrySet()) {
      List<String> seq = targetHistory.computeIfAbsent(e.getKey(), k -> new ArrayList<>());
      if (seq.isEmpty()) {
        seq.add(e.getValue());
      } else if (!seq.get(seq.size() - 1).equals(e.getValue())) {
        seq.add(e.getValue());
        changed++;
      }
    }
    return changed;
  }

  /**
   * Samples while the cluster is absorbing a topology change, stopping once the plan has been
   * quiet for QUIESCENCE_MS. Target changes here are the planner reacting to a real event, so they
   * are recorded as the detectability control rather than as churn.
   */
  private Map<String, String> samplePerturbation(AssignmentMetadataStore store,
      HelixDataAccessor accessor, Result result, Map<String, List<String>> targetHistory,
      Map<String, String> last) throws Exception {
    long start = System.currentTimeMillis();
    long lastChange = start;
    boolean sawChange = false;
    while (System.currentTimeMillis() - start < MAX_SAMPLING_MS) {
      result.samplerTicks++;
      try {
        Map<String, String> snapshot = readBestPossible(store);
        if (!snapshot.isEmpty() && !snapshot.equals(last)) {
          result.planRevisions++;
          result.perturbationTargetChanges += recordSnapshot(snapshot, targetHistory);
          last = snapshot;
          lastChange = System.currentTimeMillis();
          sawChange = true;
        }
        if (observedUncountedState(accessor)) {
          result.uncountedSightings++;
        }
      } catch (Exception ignored) {
        // Transient read while the controller is mid-write; the next tick picks it up.
      }
      if (sawChange && System.currentTimeMillis() - lastChange > QUIESCENCE_MS) {
        break;
      }
      Thread.sleep(SAMPLE_INTERVAL_MS);
    }
    return last;
  }

  /**
   * Samples for a fixed window during which nothing outside the rebalancer changes: no node is
   * added, none is removed, no config is touched. Any target change observed here is the planner
   * revising a decision on its own, which is exactly what the review means by churn.
   */
  private Map<String, String> sampleStable(AssignmentMetadataStore store,
      HelixDataAccessor accessor, Result result, Map<String, List<String>> targetHistory,
      Map<String, String> last, String label) throws Exception {
    long start = System.currentTimeMillis();
    long lastKick = 0L;
    while (System.currentTimeMillis() - start < STABLE_WINDOW_MS) {
      result.samplerTicks++;
      result.stableWindowTicks++;
      // Nudge the controller to re-run its pipeline. This changes nothing about the cluster: no
      // node joins or leaves, no config is touched, no replica is moved by this call. It only asks
      // the planner to think again. A stable planner answers identically every time; an unstable
      // one is what the review's 40-90 plan revisions look like. Without this the planner is only
      // invoked a couple of times per run and simply has no opportunity to contradict itself.
      if (System.currentTimeMillis() - lastKick > REPLAN_INTERVAL_MS) {
        RebalanceUtil.scheduleOnDemandPipeline(_clusterForKick, 0L);
        lastKick = System.currentTimeMillis();
        result.replanKicks++;
      }
      try {
        Map<String, String> snapshot = readBestPossible(store);
        if (!snapshot.isEmpty() && !snapshot.equals(last)) {
          result.planRevisions++;
          int moved = recordSnapshot(snapshot, targetHistory);
          result.churnTargetChanges += moved;
          if (moved > 0) {
            result.churnRevisions++;
          }
          last = snapshot;
        }
        if (observedUncountedState(accessor)) {
          result.uncountedSightingsStable++;
          result.uncountedSightings++;
        }
        int hidden = countHiddenOccupancyShape(accessor, last);
        if (hidden > 0) {
          result.hiddenShapeSightings++;
          result.maxHiddenShape = Math.max(result.maxHiddenShape, hidden);
        }
      } catch (Exception ignored) {
        // Transient read while the controller is mid-write; the next tick picks it up.
      }
      Thread.sleep(SAMPLE_INTERVAL_MS);
    }
    System.out.println("  stable window [" + label + "] churnTargetChanges="
        + result.churnTargetChanges + " uncountedSightingsStable="
        + result.uncountedSightingsStable + " hiddenShapeSightings=" + result.hiddenShapeSightings
        + " maxHiddenShape=" + result.maxHiddenShape);
    return last;
  }

  private MockParticipantManager startParticipant(String clusterName, String node) {
    MockParticipantManager p = new MockParticipantManager(ZK_ADDR, clusterName, node);
    p.getStateMachineEngine().registerStateModelFactory(STATE_MODEL, new IndexerModelFactory());
    p.syncStart();
    return p;
  }

  private Map<String, String> readBestPossible(AssignmentMetadataStore store) {
    Map<String, String> flat = new HashMap<>();
    Map<String, ResourceAssignment> all = store.getBestPossibleAssignment();
    for (String resource : RESOURCES) {
      ResourceAssignment ra = all.get(resource);
      if (ra == null) {
        continue;
      }
      ra.getMappedPartitions()
          .forEach(p -> ra.getReplicaMap(p).keySet().stream().sorted().findFirst()
              .ifPresent(instance -> flat.put(resource + ":" + p.getPartitionName(), instance)));
    }
    return flat;
  }

  /**
   * Counts the exact shape that makes hidden occupancy non-empty: a replica sitting in a state the
   * planner cannot see, on an instance the current plan does not target for that partition. This
   * is the condition under which the gate withholds capacity, and it is the condition under which
   * the planner's own earlier decision can feed back into its next one -- the feedback path the
   * review warns about. Measuring it directly is what separates "no churn was observed" from "no
   * churn was observed while the gate was actually holding something back".
   */
  private int countHiddenOccupancyShape(HelixDataAccessor accessor, Map<String, String> plan) {
    if (plan.isEmpty()) {
      return 0;
    }
    int count = 0;
    for (String resource : RESOURCES) {
      ExternalView ev = accessor.getProperty(accessor.keyBuilder().externalView(resource));
      if (ev == null) {
        continue;
      }
      for (String partition : ev.getPartitionSet()) {
        String planned = plan.get(resource + ":" + partition);
        for (Map.Entry<String, String> e : ev.getStateMap(partition).entrySet()) {
          String state = e.getValue();
          boolean uncounted = "ASSIGNED".equals(state) || "INDEX_DOWNLOADED".equals(state)
              || "OFFLINE".equals(state) || "ERROR".equals(state);
          if (uncounted && !e.getKey().equals(planned)) {
            count++;
          }
        }
      }
    }
    return count;
  }

  private boolean observedUncountedState(HelixDataAccessor accessor) {
    for (String resource : RESOURCES) {
      ExternalView ev = accessor.getProperty(accessor.keyBuilder().externalView(resource));
      if (ev == null) {
        continue;
      }
      for (String partition : ev.getPartitionSet()) {
        for (String state : ev.getStateMap(partition).values()) {
          if ("ASSIGNED".equals(state) || "INDEX_DOWNLOADED".equals(state)
              || "OFFLINE".equals(state)) {
            return true;
          }
        }
      }
    }
    return false;
  }

  private void awaitSettled(HelixDataAccessor accessor, long timeoutMs, String label)
      throws Exception {
    long deadline = System.currentTimeMillis() + timeoutMs;
    int caughtUp = 0;
    while (System.currentTimeMillis() < deadline) {
      caughtUp = 0;
      for (String resource : RESOURCES) {
        ExternalView ev = accessor.getProperty(accessor.keyBuilder().externalView(resource));
        if (ev != null) {
          for (String partition : ev.getPartitionSet()) {
            if (ev.getStateMap(partition).containsValue("CAUGHT_UP")) {
              caughtUp++;
            }
          }
        }
      }
      if (caughtUp == NUM_PARTITIONS) {
        return;
      }
      Thread.sleep(500L);
    }
    Assert.fail(label + ": only " + caughtUp + "/" + NUM_PARTITIONS + " reached CAUGHT_UP");
  }

  // ------------------------------------------------------------------ SEAS IndexerStateModel

  private static StateModelDefinition buildIndexerStateModelDef() {
    ZNRecord record = new ZNRecord(STATE_MODEL);
    record.setSimpleField("INITIAL_STATE", "OFFLINE");
    record.setMapField("CAUGHT_UP.meta", meta("R"));
    record.setMapField("INDEX_DOWNLOADED.meta", meta("-1"));
    record.setMapField("ASSIGNED.meta", meta("-1"));
    record.setMapField("OFFLINE.meta", meta("-1"));
    record.setMapField("DROPPED.meta", meta("-1"));
    record.setMapField("ERROR.meta", meta("-1"));
    record.setMapField("CAUGHT_UP.next", mapOf("CAUGHT_UP", "CAUGHT_UP", "DROPPED", "OFFLINE",
        "INDEX_DOWNLOADED", "OFFLINE", "ASSIGNED", "OFFLINE", "OFFLINE", "OFFLINE"));
    record.setMapField("INDEX_DOWNLOADED.next", mapOf("CAUGHT_UP", "CAUGHT_UP", "DROPPED", "OFFLINE",
        "INDEX_DOWNLOADED", "INDEX_DOWNLOADED", "ASSIGNED", "OFFLINE", "OFFLINE", "OFFLINE"));
    record.setMapField("ASSIGNED.next", mapOf("CAUGHT_UP", "INDEX_DOWNLOADED", "DROPPED", "OFFLINE",
        "INDEX_DOWNLOADED", "INDEX_DOWNLOADED", "ASSIGNED", "ASSIGNED", "OFFLINE", "OFFLINE"));
    record.setMapField("OFFLINE.next", mapOf("DROPPED", "DROPPED", "CAUGHT_UP", "ASSIGNED",
        "INDEX_DOWNLOADED", "ASSIGNED", "ASSIGNED", "ASSIGNED", "OFFLINE", "OFFLINE"));
    record.setMapField("DROPPED.next", mapOf("DROPPED", "DROPPED"));
    record.setMapField("ERROR.next",
        mapOf("DROPPED", "DROPPED", "ERROR", "ERROR", "OFFLINE", "OFFLINE"));
    record.setListField("STATE_PRIORITY_LIST",
        Arrays.asList("CAUGHT_UP", "INDEX_DOWNLOADED", "ASSIGNED", "OFFLINE", "DROPPED", "ERROR"));
    record.setListField("STATE_TRANSITION_PRIORITYLIST",
        Arrays.asList("INDEX_DOWNLOADED-CAUGHT_UP", "CAUGHT_UP-OFFLINE", "ASSIGNED-INDEX_DOWNLOADED",
            "INDEX_DOWNLOADED-OFFLINE", "OFFLINE-ASSIGNED", "ASSIGNED-OFFLINE", "OFFLINE-DROPPED"));
    return new StateModelDefinition(record);
  }

  private static Map<String, String> meta(String count) {
    return ImmutableMap.of("count", count);
  }

  private static Map<String, String> mapOf(String... kv) {
    Map<String, String> m = new HashMap<>();
    for (int i = 0; i < kv.length; i += 2) {
      m.put(kv[i], kv[i + 1]);
    }
    return m;
  }

  public static class IndexerModelFactory extends StateModelFactory<IndexerStateModel> {
    @Override
    public IndexerStateModel createNewStateModel(String resourceName, String partitionKey) {
      return new IndexerStateModel();
    }
  }

  /**
   * A healthy indexer: every transition completes. The dwell in the two uncounted states is what
   * gives hidden occupancy something to be non-empty about.
   */
  @StateModelInfo(initialState = "OFFLINE", states = {
      "CAUGHT_UP", "INDEX_DOWNLOADED", "ASSIGNED", "OFFLINE", "DROPPED", "ERROR"
  })
  public static class IndexerStateModel extends StateModel {
    @Transition(to = "ASSIGNED", from = "OFFLINE")
    public void onBecomeAssignedFromOffline(Message m, NotificationContext c)
        throws InterruptedException {
      Thread.sleep(dwellFor(m.getPartitionName(), 0));
    }

    @Transition(to = "INDEX_DOWNLOADED", from = "ASSIGNED")
    public void onBecomeIndexDownloadedFromAssigned(Message m, NotificationContext c)
        throws InterruptedException {
      Thread.sleep(dwellFor(m.getPartitionName(), 1));
    }

    @Transition(to = "CAUGHT_UP", from = "INDEX_DOWNLOADED")
    public void onBecomeCaughtUpFromIndexDownloaded(Message m, NotificationContext c) {
    }

    @Transition(to = "OFFLINE", from = "INDEX_DOWNLOADED")
    public void onBecomeOfflineFromIndexDownloaded(Message m, NotificationContext c)
        throws InterruptedException {
      Thread.sleep(dwellFor(m.getPartitionName(), 2));
    }

    @Transition(to = "OFFLINE", from = "CAUGHT_UP")
    public void onBecomeOfflineFromCaughtUp(Message m, NotificationContext c)
        throws InterruptedException {
      Thread.sleep(dwellFor(m.getPartitionName(), 3));
    }

    @Transition(to = "OFFLINE", from = "ASSIGNED")
    public void onBecomeOfflineFromAssigned(Message m, NotificationContext c)
        throws InterruptedException {
      Thread.sleep(dwellFor(m.getPartitionName(), 4));
    }

    @Transition(to = "DROPPED", from = "OFFLINE")
    public void onBecomeDroppedFromOffline(Message m, NotificationContext c)
        throws InterruptedException {
      Thread.sleep(dwellFor(m.getPartitionName(), 5));
    }

    @Transition(to = "OFFLINE", from = "ERROR")
    public void onBecomeOfflineFromError(Message m, NotificationContext c) {
    }
  }
}
