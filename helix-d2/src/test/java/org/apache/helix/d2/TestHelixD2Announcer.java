package org.apache.helix.d2;

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

import java.util.Collections;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import org.mockito.InOrder;
import org.testng.Assert;
import org.testng.annotations.Test;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;


public class TestHelixD2Announcer {

  @Test
  public void testBuildPartitionDataMap() {
    D2PartitionAnnouncer mockAnnouncer = mock(D2PartitionAnnouncer.class);
    HelixD2Announcer announcer = new HelixD2Announcer.Builder()
        .addAnnouncer(mockAnnouncer)
        .build();

    Set<String> partitions = new HashSet<>();
    partitions.add("myResource_0");
    partitions.add("myResource_5");
    partitions.add("myResource_42");

    Map<Integer, Double> result = announcer.buildPartitionDataMap(partitions);
    Assert.assertEquals(result.size(), 3);
    Assert.assertEquals(result.get(0), 1.0);
    Assert.assertEquals(result.get(5), 1.0);
    Assert.assertEquals(result.get(42), 1.0);
  }

  @Test
  public void testAlwaysAnnouncePartitionZero() {
    D2PartitionAnnouncer mockAnnouncer = mock(D2PartitionAnnouncer.class);
    HelixD2Announcer announcer = new HelixD2Announcer.Builder()
        .addAnnouncer(mockAnnouncer)
        .alwaysAnnouncePartitionZero(true)
        .build();

    // Even with only partition 5 as leader, partition 0 should be included
    Set<String> partitions = Collections.singleton("myResource_5");
    Map<Integer, Double> result = announcer.buildPartitionDataMap(partitions);
    Assert.assertEquals(result.size(), 2);
    Assert.assertTrue(result.containsKey(0), "Partition 0 should always be present");
    Assert.assertTrue(result.containsKey(5));
  }

  @Test
  public void testAlwaysAnnouncePartitionZeroWithEmptySet() {
    D2PartitionAnnouncer mockAnnouncer = mock(D2PartitionAnnouncer.class);
    HelixD2Announcer announcer = new HelixD2Announcer.Builder()
        .addAnnouncer(mockAnnouncer)
        .alwaysAnnouncePartitionZero(true)
        .build();

    Map<Integer, Double> result = announcer.buildPartitionDataMap(Collections.emptySet());
    Assert.assertEquals(result.size(), 1);
    Assert.assertTrue(result.containsKey(0));
  }

  @Test
  public void testNoOpOnUnchangedPartitions() throws Exception {
    D2PartitionAnnouncer mockAnnouncer = mock(D2PartitionAnnouncer.class);
    HelixD2Announcer announcer = new HelixD2Announcer.Builder()
        .addAnnouncer(mockAnnouncer)
        .build();

    Set<String> partitions = new HashSet<>();
    partitions.add("res_0");
    partitions.add("res_1");

    // First call — should trigger markDown/markUp
    announcer.onLeaderPartitionsChanged(partitions);
    verify(mockAnnouncer, times(1)).markDown();
    verify(mockAnnouncer, times(1)).markUp();

    // Second call with same partitions — should be a no-op
    announcer.onLeaderPartitionsChanged(partitions);
    verify(mockAnnouncer, times(1)).markDown(); // still 1 total
    verify(mockAnnouncer, times(1)).markUp();   // still 1 total
  }

  @Test
  public void testMarkDownMarkUpCycle() throws Exception {
    D2PartitionAnnouncer mockAnnouncer = mock(D2PartitionAnnouncer.class);
    HelixD2Announcer announcer = new HelixD2Announcer.Builder()
        .addAnnouncer(mockAnnouncer)
        .build();

    Set<String> partitions = Collections.singleton("res_3");
    announcer.onLeaderPartitionsChanged(partitions);

    // Verify ordering: markDown → setPartitionDataMap → markUp
    InOrder inOrder = inOrder(mockAnnouncer);
    inOrder.verify(mockAnnouncer).markDown();
    inOrder.verify(mockAnnouncer).setPartitionDataMap(any());
    inOrder.verify(mockAnnouncer).markUp();
  }

  @Test
  public void testMultipleAnnouncers() throws Exception {
    D2PartitionAnnouncer mock1 = mock(D2PartitionAnnouncer.class);
    D2PartitionAnnouncer mock2 = mock(D2PartitionAnnouncer.class);
    HelixD2Announcer announcer = new HelixD2Announcer.Builder()
        .addAnnouncer(mock1)
        .addAnnouncer(mock2)
        .build();

    Set<String> partitions = Collections.singleton("res_1");
    announcer.onLeaderPartitionsChanged(partitions);

    // Both announcers should receive the update
    verify(mock1, times(1)).markDown();
    verify(mock1, times(1)).markUp();
    verify(mock2, times(1)).markDown();
    verify(mock2, times(1)).markUp();
  }

  @Test
  public void testStartAndShutdown() throws Exception {
    D2PartitionAnnouncer mockAnnouncer = mock(D2PartitionAnnouncer.class);
    HelixD2Announcer announcer = new HelixD2Announcer.Builder()
        .addAnnouncer(mockAnnouncer)
        .build();

    Assert.assertFalse(announcer.isStarted());

    announcer.start();
    Assert.assertTrue(announcer.isStarted());
    verify(mockAnnouncer, times(1)).markUp();

    announcer.shutdown();
    Assert.assertFalse(announcer.isStarted());
    verify(mockAnnouncer, times(1)).markDown();
  }

  @Test
  public void testStartWithAlwaysAnnouncePartitionZero() throws Exception {
    D2PartitionAnnouncer mockAnnouncer = mock(D2PartitionAnnouncer.class);
    HelixD2Announcer announcer = new HelixD2Announcer.Builder()
        .addAnnouncer(mockAnnouncer)
        .alwaysAnnouncePartitionZero(true)
        .build();

    announcer.start();

    // Should have announced partition 0 during start
    verify(mockAnnouncer).setPartitionDataMap(Collections.singletonMap(0, 1.0));
    verify(mockAnnouncer).markUp();
  }

  @Test
  public void testConcurrentPartitionChanges() throws Exception {
    D2PartitionAnnouncer mockAnnouncer = mock(D2PartitionAnnouncer.class);
    HelixD2Announcer announcer = new HelixD2Announcer.Builder()
        .addAnnouncer(mockAnnouncer)
        .build();

    int numThreads = 10;
    CountDownLatch startLatch = new CountDownLatch(1);
    CountDownLatch doneLatch = new CountDownLatch(numThreads);
    ExecutorService executor = Executors.newFixedThreadPool(numThreads);

    for (int i = 0; i < numThreads; i++) {
      final int idx = i;
      executor.submit(() -> {
        try {
          startLatch.await();
          Set<String> partitions = new HashSet<>();
          for (int j = 0; j <= idx; j++) {
            partitions.add("res_" + j);
          }
          announcer.onLeaderPartitionsChanged(partitions);
        } catch (InterruptedException e) {
          Thread.currentThread().interrupt();
        } finally {
          doneLatch.countDown();
        }
      });
    }

    startLatch.countDown();
    Assert.assertTrue(doneLatch.await(10, TimeUnit.SECONDS), "Threads did not complete in time");
    executor.shutdownNow();

    // No exception should be thrown — test that concurrent access doesn't cause issues.
    // The final state should be consistent (some partition set is announced)
    Map<Integer, Double> lastAnnounced = announcer.getLastAnnouncedPartitions();
    Assert.assertFalse(lastAnnounced.isEmpty(), "Should have announced at least some partitions");
  }

  @Test
  public void testCleanup() {
    D2PartitionAnnouncer mockAnnouncer = mock(D2PartitionAnnouncer.class);
    HelixD2Announcer announcer = new HelixD2Announcer.Builder()
        .addAnnouncer(mockAnnouncer)
        .build();

    announcer.cleanup();
    verify(mockAnnouncer, times(1)).shutdown();
  }

  @Test
  public void testParsePartitionIndex() {
    Assert.assertEquals(HelixD2Announcer.parsePartitionIndex("resource_0"), 0);
    Assert.assertEquals(HelixD2Announcer.parsePartitionIndex("resource_42"), 42);
    Assert.assertEquals(HelixD2Announcer.parsePartitionIndex("my_resource_123"), 123);
    Assert.assertEquals(HelixD2Announcer.parsePartitionIndex("noindex"), -1);
    Assert.assertEquals(HelixD2Announcer.parsePartitionIndex("trailing_"), -1);
    Assert.assertEquals(HelixD2Announcer.parsePartitionIndex(null), -1);
    Assert.assertEquals(HelixD2Announcer.parsePartitionIndex("res_abc"), -1);
  }

  @Test
  public void testBuilderValidation() {
    try {
      new HelixD2Announcer.Builder().build();
      Assert.fail("Should throw on empty announcers");
    } catch (IllegalArgumentException e) {
      Assert.assertTrue(e.getMessage().contains("At least one"));
    }

    try {
      new HelixD2Announcer.Builder().addAnnouncer(null);
      Assert.fail("Should throw on null announcer");
    } catch (IllegalArgumentException e) {
      Assert.assertTrue(e.getMessage().contains("null"));
    }
  }

  @Test
  public void testDoubleStartIsNoOp() throws Exception {
    D2PartitionAnnouncer mockAnnouncer = mock(D2PartitionAnnouncer.class);
    HelixD2Announcer announcer = new HelixD2Announcer.Builder()
        .addAnnouncer(mockAnnouncer)
        .build();

    announcer.start();
    announcer.start(); // second call should be no-op

    // markUp should only be called once
    verify(mockAnnouncer, times(1)).markUp();
  }

  @Test
  public void testShutdownWithoutStart() throws Exception {
    D2PartitionAnnouncer mockAnnouncer = mock(D2PartitionAnnouncer.class);
    HelixD2Announcer announcer = new HelixD2Announcer.Builder()
        .addAnnouncer(mockAnnouncer)
        .build();

    announcer.shutdown(); // should not call markDown if never started
    verify(mockAnnouncer, never()).markDown();
  }
}
