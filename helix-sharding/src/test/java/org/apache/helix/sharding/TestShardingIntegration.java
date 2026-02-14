package org.apache.helix.sharding;

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

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import org.apache.helix.NotificationContext;
import org.apache.helix.TestHelper;
import org.apache.helix.controller.dataproviders.ResourceControllerDataProvider;
import org.apache.helix.controller.rebalancer.strategy.StickyRebalanceStrategy;
import org.apache.helix.d2.D2AnnouncingStateModelFactory;
import org.apache.helix.d2.D2PartitionAnnouncer;
import org.apache.helix.d2.HelixD2Announcer;
import org.apache.helix.model.LeaderStandbySMD;
import org.apache.helix.model.Message;
import org.apache.helix.model.StateModelDefinition;
import org.apache.helix.participant.statemachine.StateModel;
import org.apache.helix.participant.statemachine.StateModelFactory;
import org.apache.helix.participant.statemachine.StateModelInfo;
import org.apache.helix.participant.statemachine.Transition;
import org.apache.helix.sharding.internal.ListenerStateModel;
import org.apache.helix.sharding.internal.ListenerStateModelFactory;
import org.apache.helix.zookeeper.datamodel.ZNRecord;
import org.testng.Assert;
import org.testng.annotations.Test;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;


/**
 * Cross-module integration tests verifying that Phase 1 (StickyRebalanceStrategy),
 * Phase 2 (helix-d2), and Phase 3 (helix-sharding) work together correctly.
 *
 * <p>These tests do NOT require a live ZooKeeper ensemble — they test the component
 * stack in isolation. Full end-to-end tests with ZK are in the POC phase.</p>
 */
public class TestShardingIntegration {

  private static final String RESOURCE = "integrationResource";
  private static final StateModelDefinition STATE_MODEL = LeaderStandbySMD.build();

  // ─── Test 1: StickyRebalanceStrategy + HelixShardingAdmin enum mapping ───────

  @Test
  public void testStickyStrategyClassNameMatchesEnum() {
    // Verify the ShardingRebalanceStrategy.STICKY maps to the correct class
    Assert.assertEquals(ShardingRebalanceStrategy.STICKY.getHelixClassName(),
        StickyRebalanceStrategy.class.getName(),
        "STICKY enum should map to StickyRebalanceStrategy class");
  }

  @Test
  public void testAllStrategiesResolveToValidClasses() throws ClassNotFoundException {
    for (ShardingRebalanceStrategy strategy : ShardingRebalanceStrategy.values()) {
      String className = strategy.getHelixClassName();
      Assert.assertNotNull(className, "Strategy " + strategy + " has null class name");
      Class<?> clazz = Class.forName(className);
      Assert.assertNotNull(clazz, "Cannot load class for strategy " + strategy);
    }
  }

  // ─── Test 2: StickyRebalanceStrategy preserves assignments on node add ───────

  @Test
  public void testStickyStrategyWithShardingPattern() {
    // Simulate the pattern that HelixShardingAdmin creates: single resource,
    // LeaderStandby state model, STICKY strategy
    int numPartitions = 16;
    List<String> partitions = new ArrayList<>();
    for (int i = 0; i < numPartitions; i++) {
      partitions.add(RESOURCE + "_" + i);
    }

    LinkedHashMap<String, Integer> states = new LinkedHashMap<>();
    states.put("LEADER", 1);
    states.put("STANDBY", 1);

    // Initial assignment with 2 nodes
    List<String> allNodes = new ArrayList<>();
    allNodes.add("node_0");
    allNodes.add("node_1");

    ResourceControllerDataProvider dataCache =
        TestHelper.buildMockDataCache(RESOURCE, "2", "LeaderStandby",
            STATE_MODEL, Collections.emptySet());

    StickyRebalanceStrategy strategy = new StickyRebalanceStrategy();
    strategy.init(RESOURCE, partitions, states, Integer.MAX_VALUE);
    ZNRecord initial = strategy.computePartitionAssignment(allNodes, allNodes,
        new HashMap<>(), dataCache);

    // Verify initial distribution
    for (String partition : partitions) {
      Assert.assertEquals(initial.getMapField(partition).size(), 2,
          "Each partition should have 2 replicas");
    }

    // Save as currentMapping and add a third node
    Map<String, Map<String, String>> currentMapping = new HashMap<>();
    for (String partition : partitions) {
      currentMapping.put(partition, new HashMap<>(initial.getMapField(partition)));
    }

    allNodes.add("node_2");
    ZNRecord afterAdd = strategy.computePartitionAssignment(allNodes, allNodes,
        currentMapping, dataCache);

    // Stickiness: ALL original assignments must be preserved
    int preserved = 0;
    for (String partition : partitions) {
      Map<String, String> orig = initial.getMapField(partition);
      Map<String, String> after = afterAdd.getMapField(partition);
      for (Map.Entry<String, String> entry : orig.entrySet()) {
        Assert.assertEquals(after.get(entry.getKey()), entry.getValue(),
            "Sticky strategy must preserve assignment for " + partition);
        preserved++;
      }
    }
    Assert.assertEquals(preserved, numPartitions * 2,
        "All original assignments should be preserved");
  }

  // ─── Test 3: D2 + Listener state model factory chain ────────────────────────

  @Test
  public void testD2AnnouncerWithListenerFactory() throws Exception {
    // This tests the full chain: ListenerStateModelFactory → D2AnnouncingStateModelFactory
    // Simulates what HelixShardingNode does internally when D2 is configured

    // Track listener callbacks
    List<String> transitions = Collections.synchronizedList(new ArrayList<>());
    ShardingStateTransitionListener listener = (partition, from, to) ->
        transitions.add(partition + ":" + from + "->" + to);

    // Create the base factory
    ListenerStateModelFactory baseFactory = new ListenerStateModelFactory(listener);

    // Create D2 announcer
    D2PartitionAnnouncer mockD2 = mock(D2PartitionAnnouncer.class);
    HelixD2Announcer helixD2 = new HelixD2Announcer.Builder()
        .addAnnouncer(mockD2)
        .build();

    // Wrap with D2
    D2AnnouncingStateModelFactory<ListenerStateModel> wrappedFactory =
        new D2AnnouncingStateModelFactory<>(baseFactory, helixD2, "LEADER");

    // Create state models for two partitions
    StateModel model0 = wrappedFactory.createNewStateModel(RESOURCE, RESOURCE + "_0");
    StateModel model5 = wrappedFactory.createNewStateModel(RESOURCE, RESOURCE + "_5");

    Message msg = mock(Message.class);
    NotificationContext ctx = mock(NotificationContext.class);

    // Simulate OFFLINE → STANDBY → LEADER for partition 0
    invokeTransition(model0, "STANDBY", "OFFLINE");
    invokeTransition(model0, "LEADER", "STANDBY");

    // Simulate OFFLINE → STANDBY → LEADER for partition 5
    invokeTransition(model5, "STANDBY", "OFFLINE");
    invokeTransition(model5, "LEADER", "STANDBY");

    // Verify listener was called for all transitions
    Assert.assertEquals(transitions.size(), 4);
    Assert.assertTrue(transitions.contains(RESOURCE + "_0:OFFLINE->STANDBY"));
    Assert.assertTrue(transitions.contains(RESOURCE + "_0:STANDBY->LEADER"));
    Assert.assertTrue(transitions.contains(RESOURCE + "_5:OFFLINE->STANDBY"));
    Assert.assertTrue(transitions.contains(RESOURCE + "_5:STANDBY->LEADER"));

    // Verify D2 was notified
    Assert.assertEquals(wrappedFactory.getCurrentLeaderPartitions().size(), 2);
    Assert.assertTrue(wrappedFactory.getCurrentLeaderPartitions().contains(RESOURCE + "_0"));
    Assert.assertTrue(wrappedFactory.getCurrentLeaderPartitions().contains(RESOURCE + "_5"));

    // Verify D2 markDown/markUp cycles happened for state changes that
    // actually changed the leader partition set.
    // OFFLINE→STANDBY transitions don't change leader set (no-op in HelixD2Announcer).
    // Only STANDBY→LEADER transitions add to leader set and trigger cycles.
    // That's 2 changes: {res_0} and {res_0, res_5}
    verify(mockD2, times(2)).markDown();
    verify(mockD2, times(2)).markUp();

    // Now simulate LEADER → STANDBY for partition 0
    invokeTransition(model0, "STANDBY", "LEADER");

    Assert.assertEquals(wrappedFactory.getCurrentLeaderPartitions().size(), 1);
    Assert.assertFalse(wrappedFactory.getCurrentLeaderPartitions().contains(RESOURCE + "_0"));
    Assert.assertTrue(wrappedFactory.getCurrentLeaderPartitions().contains(RESOURCE + "_5"));
  }

  // ─── Test 4: Concurrent D2 announcements from multiple partitions ───────────

  @Test
  public void testConcurrentD2Announcements() throws Exception {
    Set<String> announcedSets = ConcurrentHashMap.newKeySet();
    D2PartitionAnnouncer trackingAnnouncer = new D2PartitionAnnouncer() {
      @Override
      public void setPartitionDataMap(Map<Integer, Double> partitionWeights) {
        announcedSets.add(partitionWeights.keySet().toString());
      }
      @Override
      public void markUp() {}
      @Override
      public void markDown() {}
      @Override
      public void shutdown() {}
    };

    HelixD2Announcer helixD2 = new HelixD2Announcer.Builder()
        .addAnnouncer(trackingAnnouncer)
        .build();

    ListenerStateModelFactory baseFactory =
        new ListenerStateModelFactory((p, f, t) -> {});

    D2AnnouncingStateModelFactory<ListenerStateModel> factory =
        new D2AnnouncingStateModelFactory<>(baseFactory, helixD2, "LEADER");

    // Create 10 state models
    int numModels = 10;
    List<StateModel> models = new ArrayList<>();
    for (int i = 0; i < numModels; i++) {
      models.add(factory.createNewStateModel(RESOURCE, RESOURCE + "_" + i));
    }

    // Concurrently transition all to LEADER
    CountDownLatch startLatch = new CountDownLatch(1);
    CountDownLatch doneLatch = new CountDownLatch(numModels);
    for (int i = 0; i < numModels; i++) {
      final StateModel model = models.get(i);
      new Thread(() -> {
        try {
          startLatch.await();
          invokeTransition(model, "STANDBY", "OFFLINE");
          invokeTransition(model, "LEADER", "STANDBY");
        } catch (Exception e) {
          throw new RuntimeException(e);
        } finally {
          doneLatch.countDown();
        }
      }).start();
    }

    startLatch.countDown();
    Assert.assertTrue(doneLatch.await(10, TimeUnit.SECONDS));

    // All 10 should be LEADER now
    Assert.assertEquals(factory.getCurrentLeaderPartitions().size(), numModels);
  }

  // ─── Test 5: Partition index parsing works with D2 announcement ──────────────

  @Test
  public void testPartitionIndexParsingInD2Chain() {
    D2PartitionAnnouncer mockD2 = mock(D2PartitionAnnouncer.class);
    HelixD2Announcer announcer = new HelixD2Announcer.Builder()
        .addAnnouncer(mockD2)
        .alwaysAnnouncePartitionZero(true)
        .build();

    Set<String> leaderPartitions = new HashSet<>();
    leaderPartitions.add("myCluster_0");
    leaderPartitions.add("myCluster_5");
    leaderPartitions.add("myCluster_42");

    // Trigger the announcement and verify via getLastAnnouncedPartitions
    announcer.onLeaderPartitionsChanged(leaderPartitions);
    Map<Integer, Double> lastAnnounced = announcer.getLastAnnouncedPartitions();
    Assert.assertEquals(lastAnnounced.size(), 3);
    Assert.assertTrue(lastAnnounced.containsKey(0));
    Assert.assertTrue(lastAnnounced.containsKey(5));
    Assert.assertTrue(lastAnnounced.containsKey(42));
  }

  // ─── Test 6: Strategy enum round-trip ────────────────────────────────────────

  @Test
  public void testStrategyEnumRoundTrip() {
    // Verify each strategy can be instantiated
    for (ShardingRebalanceStrategy strategy : ShardingRebalanceStrategy.values()) {
      try {
        Class<?> clazz = Class.forName(strategy.getHelixClassName());
        Object instance = clazz.getDeclaredConstructor().newInstance();
        Assert.assertNotNull(instance,
            "Could not instantiate strategy: " + strategy.getHelixClassName());
      } catch (Exception e) {
        Assert.fail("Failed to instantiate strategy " + strategy + ": " + e.getMessage());
      }
    }
  }

  // ─── Helpers ─────────────────────────────────────────────────────────────────

  /**
   * Invoke a transition on a D2AnnouncingStateModel by finding the correct method.
   */
  private void invokeTransition(StateModel model, String toState, String fromState)
      throws Exception {
    Message msg = mock(Message.class);
    NotificationContext ctx = mock(NotificationContext.class);

    java.lang.reflect.Method[] methods = model.getClass().getMethods();
    for (java.lang.reflect.Method method : methods) {
      Transition annotation = method.getAnnotation(Transition.class);
      if (annotation != null && annotation.to().equals(toState)
          && annotation.from().equals(fromState)) {
        method.invoke(model, msg, ctx);
        return;
      }
    }

    // Also check declared methods
    Class<?> current = model.getClass();
    while (current != null) {
      for (java.lang.reflect.Method method : current.getDeclaredMethods()) {
        Transition annotation = method.getAnnotation(Transition.class);
        if (annotation != null && annotation.to().equals(toState)
            && annotation.from().equals(fromState)) {
          method.setAccessible(true);
          method.invoke(model, msg, ctx);
          return;
        }
      }
      current = current.getSuperclass();
    }

    Assert.fail("No @Transition method found for " + fromState + " → " + toState
        + " on " + model.getClass().getSimpleName());
  }
}
