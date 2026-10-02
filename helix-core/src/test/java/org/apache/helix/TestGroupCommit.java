package org.apache.helix;

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

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.helix.mock.MockBaseDataAccessor;
import org.apache.helix.zookeeper.datamodel.ZNRecord;
import org.testng.Assert;
import org.testng.annotations.Test;


public class TestGroupCommit {
  // @Test
  public void testGroupCommit() throws InterruptedException {
    final BaseDataAccessor<ZNRecord> accessor = new MockBaseDataAccessor();
    final GroupCommit commit = new GroupCommit();
    ExecutorService newFixedThreadPool = Executors.newFixedThreadPool(400);
    for (int i = 0; i < 2400; i++) {
      Runnable runnable = new MyClass(accessor, commit, i);
      newFixedThreadPool.submit(runnable);
    }
    Thread.sleep(10000);
    System.out.println(accessor.get("test", null, 0));
    System.out.println(accessor.get("test", null, 0).getSimpleFields().size());
  }

  /**
   * An interrupted commit returns false, so its change must not be written later by another
   * thread that drains the same queue.
   */
  @Test(timeOut = 30000)
  public void testInterruptedCommitIsNotWrittenLater() throws Exception {
    final String key = "/CLUSTER/INSTANCES/localhost_12918/CURRENTSTATES/session/resource";
    final CountDownLatch holderInSet = new CountDownLatch(1);
    final CountDownLatch releaseHolder = new CountDownLatch(1);
    final AtomicBoolean blockNextSet = new AtomicBoolean(true);
    final BaseDataAccessor<ZNRecord> accessor = new MockBaseDataAccessor() {
      @Override
      public boolean set(String path, ZNRecord record, int options) {
        if (blockNextSet.compareAndSet(true, false)) {
          holderInSet.countDown();
          try {
            releaseHolder.await();
          } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            return false;
          }
        }
        return super.set(path, record, options);
      }
    };
    final GroupCommit commit = new GroupCommit();
    final ZNRecord holderRecord = recordWithField("holder");
    final ZNRecord staleRecord = recordWithField("stale");
    final AtomicBoolean holderResult = new AtomicBoolean(false);
    final AtomicBoolean staleResult = new AtomicBoolean(true);

    // The holder owns the queue and blocks in set(), like a write waiting on a lost ZK connection.
    Thread holder =
        new Thread(() -> holderResult.set(commit.commit(accessor, 0, key, holderRecord)));
    // The stale commit waits behind the holder and is then interrupted.
    Thread stale = new Thread(() -> staleResult.set(commit.commit(accessor, 0, key, staleRecord)));
    try {
      holder.start();
      Assert.assertTrue(holderInSet.await(10, TimeUnit.SECONDS));
      stale.start();
      waitForTimedWaiting(stale);
      stale.interrupt();
      stale.join(10000);
      Assert.assertFalse(stale.isAlive());
      Assert.assertFalse(staleResult.get());
    } finally {
      releaseHolder.countDown();
    }
    holder.join(10000);
    Assert.assertTrue(holderResult.get());

    Assert.assertTrue(commit.commit(accessor, 0, key, recordWithField("later")));
    ZNRecord stored = accessor.get(key, null, 0);
    Assert.assertEquals(stored.getSimpleField("holder"), "holder");
    Assert.assertEquals(stored.getSimpleField("later"), "later");
    Assert.assertNull(stored.getSimpleField("stale"),
        "A commit that returned false was written by a later commit");
  }

  private static ZNRecord recordWithField(String field) {
    ZNRecord record = new ZNRecord("resource");
    record.setSimpleField(field, field);
    return record;
  }

  private static void waitForTimedWaiting(Thread thread) throws InterruptedException {
    long deadline = System.currentTimeMillis() + 10000;
    while (thread.getState() != Thread.State.TIMED_WAITING) {
      Assert.assertTrue(System.currentTimeMillis() < deadline, "Commit never waited in the queue");
      Thread.sleep(1);
    }
  }
}

class MyClass implements Runnable {
  private final BaseDataAccessor<ZNRecord> store;
  private final GroupCommit commit;
  private final int i;

  public MyClass(BaseDataAccessor<ZNRecord> store, GroupCommit commit, int i) {
    this.store = store;
    this.commit = commit;
    this.i = i;
  }

  @Override
  public void run() {
    // System.out.println("START " + System.currentTimeMillis() + " --"
    // + Thread.currentThread().getId());
    ZNRecord znRecord = new ZNRecord("test");
    znRecord.setSimpleField("test_id" + i, "" + i);
    commit.commit(store, 0, "test", znRecord);
    store.get("test", null, 0).getSimpleField("");
    // System.out.println("END " + System.currentTimeMillis() + " --"
    // + Thread.currentThread().getId());
  }
}
