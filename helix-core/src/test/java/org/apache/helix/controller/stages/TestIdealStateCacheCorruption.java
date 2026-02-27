package org.apache.helix.controller.stages;

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

import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.helix.controller.dataproviders.ResourceControllerDataProvider;
import org.apache.helix.model.BuiltInStateModelDefinitions;
import org.apache.helix.model.IdealState;
import org.apache.helix.model.IdealState.RebalanceMode;
import org.apache.helix.model.Partition;
import org.apache.helix.model.Resource;
import org.apache.helix.util.StageThreadPoolHelper;
import org.apache.helix.zookeeper.datamodel.ZNRecord;
import org.testng.Assert;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.Test;


/**
 * Tests to reproduce and validate the hypothesis that concurrent HashMap corruption in
 * ResourceControllerDataProvider._idealMappingCache can cause a live, enabled instance
 * to be permanently excluded from the ideal state.
 *
 * Root cause chain (commit 0dca950):
 * 1. BestPossibleStateCalcStage parallelizes rebalancer calls via StageThreadPoolHelper
 * 2. Multiple rebalancer threads call setCachedIdealMapping() → HashMap.put() concurrently
 * 3. Concurrent put() on a plain HashMap corrupts the internal 'size' field (can go negative)
 * 4. HashMap.clear() checks "size > 0" before clearing — when size is negative, clear() is a NO-OP
 * 5. When a new instance comes online, refresh() calls clearCachedResourceAssignments() → .clear()
 * 6. clear() silently fails, stale ideal state entries survive in the cache
 * 7. Rebalancer's getCachedIdealState() returns stale mapping (computed without the new instance)
 * 8. The new instance is never assigned partitions despite being live and enabled
 * 9. Controller restart creates a fresh HashMap → cache works correctly → instance is included
 */
public class TestIdealStateCacheCorruption extends BaseStageTest {

  @AfterMethod
  public void afterMethod() {
    StageThreadPoolHelper.shutdown();
  }

  /**
   * Deterministic proof that HashMap.clear() is a no-op when the internal size field is negative.
   *
   * Java's HashMap.clear() implementation (OpenJDK 8-21):
   *   if ((tab = table) != null && size > 0) { size = 0; nullify table entries }
   *
   * When size == -1, the condition "size > 0" is false, so clear() does nothing.
   * Meanwhile, get() directly accesses the table array without checking size,
   * so it still returns entries from the un-cleared table.
   */
  @Test
  public void testHashMapClearIsNoOpWithNegativeSize() throws Exception {
    HashMap<String, String> map = new HashMap<>();
    map.put("resource1", "idealState1");
    map.put("resource2", "idealState2");
    map.put("resource3", "idealState3");

    Assert.assertEquals(map.size(), 3);
    Assert.assertEquals(map.get("resource1"), "idealState1");

    Field sizeField = HashMap.class.getDeclaredField("size");
    sizeField.setAccessible(true);
    sizeField.setInt(map, -1);

    Assert.assertEquals(map.size(), -1);

    map.clear();

    // clear() was a no-op because size > 0 was false
    Assert.assertEquals(map.get("resource1"), "idealState1",
        "get() should still return entries after clear() when size is negative. "
            + "HashMap.clear() checks 'size > 0' before nullifying table entries. "
            + "With size=-1, the check fails and clear() becomes a no-op, while "
            + "get() bypasses the size field entirely and walks the table directly.");
    Assert.assertEquals(map.get("resource2"), "idealState2");
    Assert.assertEquals(map.get("resource3"), "idealState3");

    // size is still -1 (clear() didn't reset it to 0)
    Assert.assertEquals(sizeField.getInt(map), -1,
        "size should remain -1 because clear() skipped the reset");
  }

  /**
   * Reproduces the pre-fix bug: when _idealMappingCache was a plain HashMap, concurrent
   * put() corruption could make clear() a permanent no-op.
   *
   * This test injects a plain HashMap (simulating the old vulnerable code) into the provider,
   * corrupts its size field to -1, and verifies that clearCachedResourceAssignments() fails
   * to clear stale entries. This is the exact mechanism that caused a newly-online instance
   * to be excluded from the ideal state.
   */
  @Test
  public void testIdealMappingCacheClearFailsWithCorruptedSize() throws Exception {
    ResourceControllerDataProvider provider = new ResourceControllerDataProvider();

    Field cacheField = ResourceControllerDataProvider.class.getDeclaredField("_idealMappingCache");
    cacheField.setAccessible(true);

    // Inject a plain HashMap to simulate the pre-fix vulnerable code
    HashMap<String, ZNRecord> vulnerableCache = new HashMap<>();
    cacheField.set(provider, vulnerableCache);

    // Simulate: rebalancer computed ideal states for 4 resources (without instanceX)
    List<String> preferencesWithoutNewInstance = new ArrayList<>();
    preferencesWithoutNewInstance.add("instance_0");
    preferencesWithoutNewInstance.add("instance_1");
    preferencesWithoutNewInstance.add("instance_2");

    for (int i = 0; i < 4; i++) {
      String resource = "testDB_" + i;
      ZNRecord record = new ZNRecord(resource);
      record.setListField(resource + "_0", preferencesWithoutNewInstance);
      provider.setCachedIdealMapping(resource, record);
    }

    Assert.assertNotNull(provider.getCachedIdealMapping("testDB_0"),
        "Baseline: cached ideal mapping should exist");

    // Corrupt the HashMap's size field to -1 (simulating concurrent put() corruption)
    Field sizeField = HashMap.class.getDeclaredField("size");
    sizeField.setAccessible(true);
    sizeField.setInt(vulnerableCache, -1);

    provider.clearCachedResourceAssignments();

    // With a plain HashMap and size=-1, clear() is a no-op — stale entries survive
    ZNRecord staleMapping = provider.getCachedIdealMapping("testDB_0");
    Assert.assertNotNull(staleMapping,
        "With a plain HashMap (pre-fix code) and corrupted size=-1, "
            + "clearCachedResourceAssignments() is a no-op. Stale entries survive.");

    List<String> stalePreferenceList = staleMapping.getListField("testDB_0_0");
    Assert.assertFalse(stalePreferenceList.contains("instance_3"),
        "The stale cached ideal state should NOT contain the new instance.");

    for (int i = 0; i < 4; i++) {
      Assert.assertNotNull(provider.getCachedIdealMapping("testDB_" + i),
          "All stale cache entries survive the failed clear()");
    }
  }

  /**
   * Regression test: verifies that with ConcurrentHashMap (the fix), clear() always works
   * and stale entries are properly removed even after heavy concurrent writes.
   */
  @Test
  public void testConcurrentHashMapClearAlwaysWorks() throws Exception {
    ResourceControllerDataProvider provider = new ResourceControllerDataProvider();

    for (int i = 0; i < 4; i++) {
      String resource = "testDB_" + i;
      ZNRecord record = new ZNRecord(resource);
      record.setListField(resource + "_0", Collections.singletonList("instance_0"));
      provider.setCachedIdealMapping(resource, record);
    }

    Assert.assertNotNull(provider.getCachedIdealMapping("testDB_0"));

    provider.clearCachedResourceAssignments();

    // With ConcurrentHashMap, clear() always works — no entries survive
    for (int i = 0; i < 4; i++) {
      Assert.assertNull(provider.getCachedIdealMapping("testDB_" + i),
          "With ConcurrentHashMap (the fix), clearCachedResourceAssignments() should "
              + "always remove all entries. This is the regression test for the fix.");
    }
  }

  /**
   * Reproduces the pre-fix behavior: with a plain HashMap, corruption makes clear() fail
   * permanently. A controller restart (fresh provider) fixes it because the new provider
   * gets a clean map. Post-fix, both providers use ConcurrentHashMap where clear() always works.
   */
  @Test
  public void testControllerRestartCreatesCleanCache() throws Exception {
    Field cacheField = ResourceControllerDataProvider.class.getDeclaredField("_idealMappingCache");
    cacheField.setAccessible(true);

    // Simulate the corrupted controller: inject a plain HashMap (pre-fix code)
    ResourceControllerDataProvider corruptedProvider = new ResourceControllerDataProvider();
    HashMap<String, ZNRecord> vulnerableCache = new HashMap<>();
    cacheField.set(corruptedProvider, vulnerableCache);

    ZNRecord staleRecord = new ZNRecord("testDB_0");
    staleRecord.setListField("testDB_0_0",
        new ArrayList<>(Collections.singletonList("instance_0")));
    corruptedProvider.setCachedIdealMapping("testDB_0", staleRecord);

    // Corrupt the plain HashMap's size to -1
    Field sizeField = HashMap.class.getDeclaredField("size");
    sizeField.setAccessible(true);
    sizeField.setInt(vulnerableCache, -1);

    corruptedProvider.clearCachedResourceAssignments();
    Assert.assertNotNull(corruptedProvider.getCachedIdealMapping("testDB_0"),
        "Corrupted provider with plain HashMap: clear() failed, stale entry persists");

    // Simulate controller restart: create a completely new provider
    ResourceControllerDataProvider freshProvider = new ResourceControllerDataProvider();

    Assert.assertNull(freshProvider.getCachedIdealMapping("testDB_0"),
        "Fresh provider should not have any stale entries. "
            + "This is why restarting the controller fixes the issue.");

    // Verify clear() works on the fresh provider (now ConcurrentHashMap)
    freshProvider.setCachedIdealMapping("testDB_0", staleRecord);
    Assert.assertNotNull(freshProvider.getCachedIdealMapping("testDB_0"));
    freshProvider.clearCachedResourceAssignments();
    Assert.assertNull(freshProvider.getCachedIdealMapping("testDB_0"),
        "clear() should always work on ConcurrentHashMap (the fix)");
  }

  /**
   * End-to-end test: runs the full BestPossibleStateCalcStage pipeline and demonstrates
   * that a corrupted _idealMappingCache causes the rebalancer to return stale ideal states
   * that exclude a newly-added instance.
   *
   * Sequence:
   * 1. Set up cluster with 5 instances and a FULL_AUTO resource
   * 2. Run the pipeline to populate the ideal state cache
   * 3. Add a 6th live instance
   * 4. Corrupt _idealMappingCache.size to -1
   * 5. Call clearCachedResourceAssignments() (simulates refresh detecting LIVE_INSTANCE change)
   * 6. Run the pipeline again
   * 7. Verify: the cached (stale) ideal state is returned, 6th instance gets no assignment
   */
  @Test
  public void testStaleIdealStateCacheExcludesNewInstance() throws Exception {
    int initialInstances = 5;
    int numPartitions = 5;
    int numReplica = 3;

    String[] resources = new String[]{"testResource"};
    setupIdealState(initialInstances, resources, numPartitions, numReplica,
        RebalanceMode.FULL_AUTO, BuiltInStateModelDefinitions.MasterSlave.name());
    setupLiveInstances(initialInstances);
    setupStateModel();
    setupInstances(initialInstances);

    Map<String, Resource> resourceMap = getResourceMap(resources, numPartitions,
        BuiltInStateModelDefinitions.MasterSlave.name());
    CurrentStateOutput currentStateOutput = new CurrentStateOutput();

    ResourceControllerDataProvider cache = new ResourceControllerDataProvider();
    event.addAttribute(AttributeName.RESOURCES.name(), resourceMap);
    event.addAttribute(AttributeName.RESOURCES_TO_REBALANCE.name(), resourceMap);
    event.addAttribute(AttributeName.CURRENT_STATE.name(), currentStateOutput);
    event.addAttribute(AttributeName.CURRENT_STATE_EXCLUDING_UNKNOWN.name(), currentStateOutput);
    event.addAttribute(AttributeName.ControllerDataProvider.name(), cache);

    // Run pipeline — this populates _idealMappingCache
    runStage(event, new ReadClusterDataStage());
    runStage(event, new BestPossibleStateCalcStage());

    BestPossibleStateOutput firstRunOutput =
        event.getAttribute(AttributeName.BEST_POSSIBLE_STATE.name());
    Assert.assertNotNull(firstRunOutput);

    // Capture the cached ideal mapping for the resource
    ZNRecord cachedBeforeCorruption = cache.getCachedIdealMapping("testResource");
    Assert.assertNotNull(cachedBeforeCorruption,
        "After first pipeline run, ideal mapping should be cached");

    // Record which instances were assigned in the first run
    Map<String, String> firstRunStateMap = firstRunOutput.getInstanceStateMap(
        "testResource", new Partition("testResource_0"));
    Assert.assertNotNull(firstRunStateMap);

    // Now: add a 6th instance (simulating a new node coming online)
    String newInstance = HOSTNAME_PREFIX + initialInstances;
    setupLiveInstances(initialInstances + 1);
    setupInstances(initialInstances + 1);

    // Replace _idealMappingCache with a plain HashMap to simulate the pre-fix vulnerable code,
    // then corrupt its size to -1 to reproduce the production bug
    Field cacheField = ResourceControllerDataProvider.class.getDeclaredField("_idealMappingCache");
    cacheField.setAccessible(true);

    // Copy existing entries into a plain HashMap (vulnerable to corruption)
    @SuppressWarnings("unchecked")
    Map<String, ZNRecord> currentCache = (Map<String, ZNRecord>) cacheField.get(cache);
    HashMap<String, ZNRecord> vulnerableCache = new HashMap<>(currentCache);
    cacheField.set(cache, vulnerableCache);

    Field sizeField = HashMap.class.getDeclaredField("size");
    sizeField.setAccessible(true);
    sizeField.setInt(vulnerableCache, -1);

    // Simulate what refresh() does when it detects LIVE_INSTANCE change
    cache.clearCachedResourceAssignments();

    // Verify the cache was NOT cleared (the bug)
    ZNRecord stillCached = cache.getCachedIdealMapping("testResource");
    Assert.assertNotNull(stillCached,
        "With corrupted HashMap size, clearCachedResourceAssignments() is a no-op. "
            + "The stale ideal state (computed with 5 instances) survives.");

    // The stale cached mapping should be identical to what was computed with 5 instances
    Assert.assertEquals(stillCached.getListFields(), cachedBeforeCorruption.getListFields(),
        "Stale cache should contain the original ideal state computed without the 6th instance");

    // Check all preference lists in the stale cached mapping — none should contain the new instance
    boolean newInstanceInCache = false;
    for (List<String> prefList : stillCached.getListFields().values()) {
      if (prefList.contains(newInstance)) {
        newInstanceInCache = true;
        break;
      }
    }
    Assert.assertFalse(newInstanceInCache,
        "The 6th instance (" + newInstance + ") should NOT appear in ANY cached preference list. "
            + "This is the production bug: the rebalancer returns stale cached ideal state "
            + "that was computed before the instance came online, and the cache can never be "
            + "cleared because HashMap.clear() is a no-op with negative size.");

    // Run the pipeline again — the rebalancer should hit the stale cache
    runStage(event, new ReadClusterDataStage());
    runStage(event, new BestPossibleStateCalcStage());

    // The stale cache entry should STILL be present after the second run (clear never works)
    ZNRecord stillCachedAfterSecondRun = cache.getCachedIdealMapping("testResource");
    Assert.assertNotNull(stillCachedAfterSecondRun,
        "After second pipeline run, stale cache should still persist because clear() never works "
            + "with corrupted negative size");
  }

  /**
   * Confirms that _idealMappingCache and _resourceAssignmentCache are ConcurrentHashMaps,
   * making them safe for concurrent access from parallel rebalancer threads.
   */
  @Test
  public void testIdealMappingCacheIsThreadSafe() throws Exception {
    ResourceControllerDataProvider provider = new ResourceControllerDataProvider();

    Field idealCacheField = ResourceControllerDataProvider.class.getDeclaredField("_idealMappingCache");
    idealCacheField.setAccessible(true);
    Assert.assertEquals(idealCacheField.get(provider).getClass(),
        java.util.concurrent.ConcurrentHashMap.class,
        "_idealMappingCache must be ConcurrentHashMap for safe concurrent access "
            + "from parallel rebalancer threads via setCachedIdealMapping()");

    Field assignmentCacheField = ResourceControllerDataProvider.class.getDeclaredField("_resourceAssignmentCache");
    assignmentCacheField.setAccessible(true);
    Assert.assertEquals(assignmentCacheField.get(provider).getClass(),
        java.util.concurrent.ConcurrentHashMap.class,
        "_resourceAssignmentCache must be ConcurrentHashMap for safe concurrent access "
            + "from parallel rebalancer threads via setCachedResourceAssignment()");
  }

  /**
   * Probabilistic test demonstrating that concurrent HashMap.put() operations can corrupt
   * the map's internal state. The critical corruption happens during internal resize():
   * when two threads simultaneously trigger a resize, entries can be lost and the size
   * field can become inconsistent (including negative).
   *
   * Strategy: each thread inserts UNIQUE keys so the total entry count grows rapidly,
   * forcing many resize() operations (capacity 2 → 4 → 8 → 16 → ...). The HashMap is
   * initialized with capacity 2 to trigger the first resize almost immediately.
   *
   * Detection: after concurrent puts, check for any of:
   * 1. Negative size (exactly matches the heap dump finding: size=-1)
   * 2. Size mismatch vs actual entries
   * 3. clear() failing to remove entries (the critical bug consequence)
   * 4. Lost entries (get returns null for a key that was definitely put)
   *
   * Note: this test is inherently probabilistic — race conditions are timing-dependent.
   */
  @Test
  public void testConcurrentPutsCorruptHashMapSize() throws Exception {
    Field sizeField = HashMap.class.getDeclaredField("size");
    sizeField.setAccessible(true);

    int numRounds = 500;
    int numThreads = 8;
    int putsPerThread = 200;
    AtomicBoolean corruptionDetected = new AtomicBoolean(false);
    StringBuilder report = new StringBuilder();

    for (int round = 0; round < numRounds && !corruptionDetected.get(); round++) {
      final int currentRound = round;
      // Capacity 2 forces resize() at every ~2 inserts (load factor 0.75 → threshold 1)
      // Total keys = numThreads * putsPerThread = 1600, causing ~10 resize operations
      HashMap<String, String> map = new HashMap<>(2);
      CyclicBarrier barrier = new CyclicBarrier(numThreads);

      List<Thread> threads = new ArrayList<>();
      for (int t = 0; t < numThreads; t++) {
        final int threadId = t;
        Thread thread = new Thread(() -> {
          try {
            barrier.await(5, TimeUnit.SECONDS);
            for (int i = 0; i < putsPerThread; i++) {
              // UNIQUE keys per thread — forces total entry growth and triggers resize()
              map.put("r_" + threadId + "_" + i, "v" + i);
            }
          } catch (Exception e) {
            // ConcurrentModificationException is an expected corruption symptom
            corruptionDetected.set(true);
            report.append("Round ").append(currentRound)
                .append(": ConcurrentModificationException during put()\n");
          }
        });
        threads.add(thread);
        thread.start();
      }

      for (Thread thread : threads) {
        thread.join(10_000);
      }

      if (corruptionDetected.get()) {
        break;
      }

      int reportedSize = sizeField.getInt(map);
      int expectedTotal = numThreads * putsPerThread;

      // Check 1: negative size (exactly what we found in the heap dump)
      if (reportedSize < 0) {
        corruptionDetected.set(true);
        report.append("Round ").append(currentRound).append(": NEGATIVE SIZE (size=")
            .append(reportedSize).append(") — matches heap dump finding\n");

        map.clear();
        int sizeAfterClear = sizeField.getInt(map);
        report.append("  After clear(): size=").append(sizeAfterClear)
            .append(" (negative means clear was no-op)\n");
        break;
      }

      // Check 2: size doesn't match expected total (lost entries during concurrent resize)
      if (reportedSize != expectedTotal) {
        corruptionDetected.set(true);
        report.append("Round ").append(currentRound).append(": SIZE MISMATCH (reported=")
            .append(reportedSize).append(", expected=").append(expectedTotal).append(")\n");
        break;
      }

      // Check 3: entries actually lost (get returns null for a key that was put)
      for (int t = 0; t < numThreads; t++) {
        for (int i = 0; i < putsPerThread; i++) {
          if (map.get("r_" + t + "_" + i) == null) {
            corruptionDetected.set(true);
            report.append("Round ").append(currentRound).append(": LOST ENTRY r_")
                .append(t).append("_").append(i)
                .append(" (entry lost during concurrent resize)\n");
            break;
          }
        }
        if (corruptionDetected.get()) {
          break;
        }
      }
    }

    System.out.println("=== Concurrent HashMap corruption test ===");
    System.out.println(report);

    Assert.assertTrue(corruptionDetected.get(),
        "Concurrent HashMap.put() should eventually corrupt the map's internal state, "
            + "demonstrating that the plain HashMap used for _idealMappingCache is unsafe "
            + "for concurrent access from parallel rebalancer threads.");
  }
}
