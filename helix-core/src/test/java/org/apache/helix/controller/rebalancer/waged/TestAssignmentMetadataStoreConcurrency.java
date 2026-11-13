package org.apache.helix.controller.rebalancer.waged;

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

import java.util.AbstractSet;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.Spliterator;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;

import org.apache.helix.BucketDataAccessor;
import org.apache.helix.HelixRebalanceException;
import org.apache.helix.controller.stages.CurrentStateOutput;
import org.apache.helix.model.Partition;
import org.apache.helix.model.ResourceAssignment;
import org.apache.helix.monitoring.metrics.model.LatencyMetric;
import org.mockito.Mockito;
import org.testng.Assert;
import org.testng.annotations.Test;

/**
 * Verifies that reading the in-memory best possible assignment while it is being updated does not
 * throw a ConcurrentModificationException or return a torn snapshot, without needing ZooKeeper.
 *
 * Uses the real AssignmentMetadataStore (not MockAssignmentMetadataStore, which swaps map
 * references and therefore cannot show the race). In the controller the writer is the
 * WagedPartialRebalance thread calling asyncUpdateBestPossibleAssignmentCache while the pipeline
 * thread runs emergencyRebalance -> AssignmentManager.getBestPossibleAssignment.
 */
public class TestAssignmentMetadataStoreConcurrency {
  private static final String CLUSTER = "TestCluster";
  private static final String RESOURCE_PREFIX = "resource_";

  /**
   * Deterministic interleaving: the cache write lands after the reader has started iterating the
   * live best possible map. The exception is thrown by the real HashMap iterator; the only
   * test-only part is that the write is triggered from the first iterated element instead of from
   * a second thread.
   */
  @Test
  public void testBestPossibleReadWhileAsyncCacheUpdateRuns() {
    AssignmentMetadataStore store = newStore();
    AssignmentManager manager = new AssignmentManager(Mockito.mock(LatencyMetric.class));
    Set<String> resources = new HashSet<>();
    Map<String, ResourceAssignment> before = assignments(10, "instance_A");
    resources.addAll(before.keySet());
    Map<String, ResourceAssignment> after = assignments(10, "instance_B");

    AtomicBoolean writeDone = new AtomicBoolean();
    store._bestPossibleAssignment = new WriteOnFirstElementMap(before, () -> {
      Assert.assertTrue(store.asyncUpdateBestPossibleAssignmentCache(after, 1));
      writeDone.set(true);
    });

    Map<String, ResourceAssignment> result = null;
    try {
      result = manager.getBestPossibleAssignment(store, new CurrentStateOutput(), resources);
    } catch (HelixRebalanceException e) {
      Assert.fail("Reading the best possible assignment while the async partial rebalance updated "
          + "the cache failed: type=" + e.getFailureType() + " category=" + e.getFailureCategory()
          + " cause=" + e.getCause(), e);
    }

    Assert.assertTrue(writeDone.get(), "The concurrent cache write was not exercised");
    Assert.assertTrue(isSingleGeneration(result, resources.size()),
        "Read returned a torn or mixed snapshot: " + result);
  }

  /**
   * The same race with two real threads: a writer repeatedly updating the cache like the
   * WagedPartialRebalance thread, and a reader like the pipeline thread.
   */
  @Test(timeOut = 60_000)
  public void testBestPossibleReadsWhileWriterThreadUpdatesCache() throws Exception {
    final int resourceCount = 2000;
    final long runMillis = 3000;
    AssignmentMetadataStore store = newStore();
    AssignmentManager manager = new AssignmentManager(Mockito.mock(LatencyMetric.class));
    Map<String, ResourceAssignment> generationA = assignments(resourceCount, "instance_A");
    Map<String, ResourceAssignment> generationB = assignments(resourceCount, "instance_B");
    Set<String> resources = new HashSet<>(generationA.keySet());
    store._bestPossibleAssignment = new HashMap<>(generationA);

    AtomicBoolean stop = new AtomicBoolean();
    Thread writer = new Thread(() -> {
      boolean useB = true;
      while (!stop.get()) {
        store.asyncUpdateBestPossibleAssignmentCache(useB ? generationB : generationA,
            store.getBestPossibleVersion() + 1);
        useB = !useB;
      }
    }, "WagedPartialRebalance-" + CLUSTER);
    writer.start();

    int reads = 0;
    int failedReads = 0;
    int tornReads = 0;
    HelixRebalanceException firstFailure = null;
    long deadline = System.currentTimeMillis() + runMillis;
    try {
      while (System.currentTimeMillis() < deadline) {
        try {
          reads++;
          if (!isSingleGeneration(
              manager.getBestPossibleAssignment(store, new CurrentStateOutput(), resources),
              resourceCount)) {
            tornReads++;
          }
        } catch (HelixRebalanceException e) {
          failedReads++;
          if (firstFailure == null) {
            firstFailure = e;
          }
        }
      }
    } finally {
      stop.set(true);
      writer.join();
    }

    if (failedReads > 0 || tornReads > 0) {
      Assert.fail(String.format("%d of %d reads failed and %d returned a torn snapshot. "
          + "First failure: %s", failedReads, reads, tornReads, firstFailure), firstFailure);
    }
  }

  private static AssignmentMetadataStore newStore() {
    return new AssignmentMetadataStore(Mockito.mock(BucketDataAccessor.class), CLUSTER);
  }

  private static Map<String, ResourceAssignment> assignments(int resourceCount, String instance) {
    Map<String, ResourceAssignment> assignments = new HashMap<>();
    for (int i = 0; i < resourceCount; i++) {
      String resource = RESOURCE_PREFIX + i;
      ResourceAssignment assignment = new ResourceAssignment(resource);
      assignment.addReplicaMap(new Partition(resource + "_0"),
          Collections.singletonMap(instance, "ONLINE"));
      assignments.put(resource, assignment);
    }
    return assignments;
  }

  // A consistent read holds every resource, each fully populated, from one generation only.
  // Resources missing from the cache are back-filled with empty assignments by AssignmentManager.
  private static boolean isSingleGeneration(Map<String, ResourceAssignment> result,
      int expectedResources) {
    if (result == null || result.size() != expectedResources) {
      return false;
    }
    Set<String> instances = new HashSet<>();
    for (ResourceAssignment assignment : result.values()) {
      if (assignment.getMappedPartitions().size() != 1) {
        return false;
      }
      instances.addAll(
          assignment.getReplicaMap(assignment.getMappedPartitions().get(0)).keySet());
    }
    return instances.size() == 1;
  }

  /**
   * A HashMap whose entry spliterator runs a callback once, when the first element is consumed
   * (after HashMap has captured its expected modCount, before iteration finishes).
   */
  private static class WriteOnFirstElementMap extends HashMap<String, ResourceAssignment> {
    private final Runnable _writeDuringIteration;
    private final AtomicBoolean _fired = new AtomicBoolean();

    WriteOnFirstElementMap(Map<String, ResourceAssignment> initial, Runnable writeDuringIteration) {
      super(initial);
      _writeDuringIteration = writeDuringIteration;
    }

    @Override
    public Set<Map.Entry<String, ResourceAssignment>> entrySet() {
      final Set<Map.Entry<String, ResourceAssignment>> real = super.entrySet();
      return new AbstractSet<Map.Entry<String, ResourceAssignment>>() {
        @Override
        public Iterator<Map.Entry<String, ResourceAssignment>> iterator() {
          return real.iterator();
        }

        @Override
        public int size() {
          return real.size();
        }

        @Override
        public Spliterator<Map.Entry<String, ResourceAssignment>> spliterator() {
          return new HookedSpliterator(real.spliterator());
        }
      };
    }

    private void fireOnce() {
      if (_fired.compareAndSet(false, true)) {
        _writeDuringIteration.run();
      }
    }

    private class HookedSpliterator implements Spliterator<Map.Entry<String, ResourceAssignment>> {
      private final Spliterator<Map.Entry<String, ResourceAssignment>> _delegate;

      HookedSpliterator(Spliterator<Map.Entry<String, ResourceAssignment>> delegate) {
        _delegate = delegate;
      }

      @Override
      public boolean tryAdvance(Consumer<? super Map.Entry<String, ResourceAssignment>> action) {
        return _delegate.tryAdvance(entry -> {
          fireOnce();
          action.accept(entry);
        });
      }

      @Override
      public void forEachRemaining(
          Consumer<? super Map.Entry<String, ResourceAssignment>> action) {
        _delegate.forEachRemaining(entry -> {
          fireOnce();
          action.accept(entry);
        });
      }

      @Override
      public Spliterator<Map.Entry<String, ResourceAssignment>> trySplit() {
        return null;
      }

      @Override
      public long estimateSize() {
        return _delegate.estimateSize();
      }

      @Override
      public int characteristics() {
        return _delegate.characteristics();
      }
    }
  }
}
