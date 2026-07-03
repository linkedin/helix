package org.apache.helix.controller.common;

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
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import org.testng.Assert;
import org.testng.annotations.Test;

public class TestCapacityNode {

  @Test
  public void testCanAddBasic() {
    CapacityNode node = new CapacityNode("node-0");
    node.setCapacity(2);

    Assert.assertTrue(node.canAdd("resA", "p0"));
    // The same resource+partition cannot be reserved twice and must not consume capacity.
    Assert.assertFalse(node.canAdd("resA", "p0"));
    Assert.assertEquals(node.getCurrentlyAssigned(), 1);

    Assert.assertTrue(node.canAdd("resB", "p0"));
    Assert.assertEquals(node.getCurrentlyAssigned(), 2);

    // Capacity is exhausted, further reservations are rejected.
    Assert.assertFalse(node.canAdd("resC", "p0"));
    Assert.assertEquals(node.getCurrentlyAssigned(), 2);
  }

  /**
   * Exercises {@link CapacityNode#canAdd} the way the controller does when it computes multiple
   * resources' best-possible state in parallel over a shared CapacityNode. Before canAdd was
   * synchronized this raced on the backing HashMap and intermittently threw
   * ConcurrentModificationException.
   */
  @Test
  public void testConcurrentCanAddIsThreadSafe() throws Exception {
    final int threads = 16;
    final int opsPerThread = 500;

    CapacityNode node = new CapacityNode("node-0");
    // Large enough that every distinct reservation succeeds.
    node.setCapacity(threads * opsPerThread);

    List<Throwable> failures = runConcurrentAdds(node, threads, opsPerThread, new AtomicInteger());

    Assert.assertTrue(failures.isEmpty(), "canAdd threw under concurrency: " + failures);
    Assert.assertEquals(node.getCurrentlyAssigned(), threads * opsPerThread,
        "every distinct reservation should have been counted exactly once");
  }

  /**
   * Verifies the per-node capacity is enforced atomically under concurrency: the number of
   * successful reservations must equal capacity and never exceed it.
   */
  @Test
  public void testConcurrentCanAddRespectsCapacity() throws Exception {
    final int threads = 16;
    final int opsPerThread = 500;
    final int capacity = 100;

    CapacityNode node = new CapacityNode("node-0");
    node.setCapacity(capacity);

    AtomicInteger successful = new AtomicInteger();
    List<Throwable> failures = runConcurrentAdds(node, threads, opsPerThread, successful);

    Assert.assertTrue(failures.isEmpty(), "canAdd threw under concurrency: " + failures);
    Assert.assertEquals(node.getCurrentlyAssigned(), capacity,
        "node should be filled exactly to capacity");
    Assert.assertEquals(successful.get(), capacity,
        "successful reservations must equal capacity, never exceed it");
  }

  private List<Throwable> runConcurrentAdds(CapacityNode node, int threads, int opsPerThread,
      AtomicInteger successful) throws Exception {
    ExecutorService executor = Executors.newFixedThreadPool(threads);
    List<Throwable> failures = Collections.synchronizedList(new ArrayList<>());
    CountDownLatch startLatch = new CountDownLatch(1);
    List<Future<?>> futures = new ArrayList<>();

    for (int t = 0; t < threads; t++) {
      final int threadId = t;
      futures.add(executor.submit(() -> {
        try {
          startLatch.await();
          for (int i = 0; i < opsPerThread; i++) {
            // Distinct (resource, partition) per (thread, op) so every attempt is a fresh key,
            // maximizing structural mutation of the shared backing map.
            if (node.canAdd("resource-" + threadId + "-" + i, "partition-" + i)) {
              successful.incrementAndGet();
            }
          }
        } catch (Throwable th) {
          failures.add(th);
        }
      }));
    }

    // Release all threads simultaneously to maximize contention.
    startLatch.countDown();
    for (Future<?> future : futures) {
      future.get(30, TimeUnit.SECONDS);
    }
    executor.shutdownNow();
    return failures;
  }
}
