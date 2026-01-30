package org.apache.helix.util;

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
import java.util.Iterator;
import java.util.List;
import java.util.Collection;
import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.testng.Assert;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.Test;

public class TestStageThreadPoolHelper {

  @AfterMethod
  public void afterMethod() {
    // Clean up after each test
    StageThreadPoolHelper.shutdown();
  }

  @Test
  public void testExecuteAndWaitWithSuccessfulTasks() throws InterruptedException {
    // Test executing multiple successful tasks
    AtomicInteger counter = new AtomicInteger(0);
    List<Callable<Void>> tasks = new ArrayList<>();

    for (int i = 0; i < 5; i++) {
      tasks.add(() -> {
        counter.incrementAndGet();
        return null;
      });
    }

    StageThreadPoolHelper.executeAndWait("TestStage", tasks);

    // Verify all tasks were executed
    Assert.assertEquals(counter.get(), 5, "All tasks should be executed");
  }

  @Test
  public void testExecuteAndWaitWithEmptyTasks() throws InterruptedException {
    // Test with empty task collection - should not throw exception
    List<Callable<Void>> emptyTasks = new ArrayList<>();
    StageThreadPoolHelper.executeAndWait("EmptyStage", emptyTasks);
  }

  @Test
  public void testExecuteAndWaitWithNullTasks() throws InterruptedException {
    // Test with null task collection - should not throw exception
    StageThreadPoolHelper.executeAndWait("NullStage", null);
  }

  @Test
  public void testExecuteAndWaitWithTaskExceptions() throws InterruptedException {
    // Test that exceptions in individual tasks don't prevent other tasks from running
    AtomicInteger successCounter = new AtomicInteger(0);
    AtomicInteger exceptionCounter = new AtomicInteger(0);
    List<Callable<Void>> tasks = new ArrayList<>();

    // Add tasks that throw exceptions
    for (int i = 0; i < 3; i++) {
      final int taskNum = i;
      tasks.add(() -> {
        if (taskNum % 2 == 0) {
          exceptionCounter.incrementAndGet();
          throw new RuntimeException("Task " + taskNum + " failed");
        }
        successCounter.incrementAndGet();
        return null;
      });
    }

    // Should not throw exception even if tasks fail
    StageThreadPoolHelper.executeAndWait("FailingStage", tasks);

    // Verify that tasks were executed (some succeeded, some failed)
    Assert.assertEquals(successCounter.get(), 1, "Successful tasks should complete");
    Assert.assertEquals(exceptionCounter.get(), 2, "Failing tasks should be attempted");
  }

  @Test
  public void testParallelExecution() throws InterruptedException {
    // Test that tasks are actually executed in parallel
    int numTasks = 4;
    CountDownLatch startLatch = new CountDownLatch(numTasks);
    CountDownLatch endLatch = new CountDownLatch(numTasks);
    List<Callable<Void>> tasks = new ArrayList<>();

    for (int i = 0; i < numTasks; i++) {
      tasks.add(() -> {
        startLatch.countDown();
        // Wait for all tasks to start (proving they run in parallel)
        startLatch.await(5, TimeUnit.SECONDS);
        TimeUnit.MILLISECONDS.sleep(50); // Simulate work
        endLatch.countDown();
        return null;
      });
    }

    long startTime = System.currentTimeMillis();
    StageThreadPoolHelper.executeAndWait("ParallelStage", tasks);
    long duration = System.currentTimeMillis() - startTime;

    // Verify all tasks completed
    Assert.assertEquals(endLatch.getCount(), 0, "All tasks should complete");

    // If tasks ran sequentially, it would take ~200ms (4 * 50ms)
    // If parallel, should be around 50-100ms
    Assert.assertTrue(duration < 150,
        "Tasks should execute in parallel, duration was: " + duration + "ms");
  }

  @Test
  public void testThreadNaming() throws InterruptedException {
    // Test that thread names include stage context
    String stageName = "ThreadNamingStage";
    List<String> threadNames = Collections.synchronizedList(new ArrayList<>());
    List<Callable<Void>> tasks = new ArrayList<>();

    for (int i = 0; i < 3; i++) {
      tasks.add(() -> {
        threadNames.add(Thread.currentThread().getName());
        return null;
      });
    }

    StageThreadPoolHelper.executeAndWait(stageName, tasks);

    // Verify thread names contain the stage name
    Assert.assertEquals(threadNames.size(), 3, "Should capture all thread names");
    for (String threadName : threadNames) {
      Assert.assertTrue(threadName.startsWith(stageName + "-"),
          "Thread name should start with stage name: " + threadName);
    }
  }

  @Test
  public void testShutdown() throws InterruptedException {
    // Test shutdown functionality
    AtomicInteger counter = new AtomicInteger(0);
    List<Callable<Void>> tasks = new ArrayList<>();

    tasks.add(() -> {
      counter.incrementAndGet();
      return null;
    });

    StageThreadPoolHelper.executeAndWait("PreShutdownStage", tasks);
    Assert.assertEquals(counter.get(), 1, "Task should execute before shutdown");

    // Shutdown the executor
    StageThreadPoolHelper.shutdown();

    // Multiple shutdowns should be safe
    StageThreadPoolHelper.shutdown();
    StageThreadPoolHelper.shutdown();
  }

  @Test
  public void testExecutorReinitialization() throws InterruptedException {
    // Test that executor is reinitialized after shutdown
    AtomicInteger counter = new AtomicInteger(0);
    List<Callable<Void>> tasks = new ArrayList<>();

    tasks.add(() -> {
      counter.incrementAndGet();
      return null;
    });

    // Execute before shutdown
    StageThreadPoolHelper.executeAndWait("BeforeShutdown", tasks);
    Assert.assertEquals(counter.get(), 1);

    // Shutdown
    StageThreadPoolHelper.shutdown();

    // Execute after shutdown - should create a new executor
    counter.set(0);
    StageThreadPoolHelper.executeAndWait("AfterShutdown", tasks);
    Assert.assertEquals(counter.get(), 1, "Should work after shutdown with new executor");
  }

  @Test
  public void testTaskReturnValues() throws InterruptedException {
    // Test that tasks with return values execute correctly
    List<Callable<Integer>> tasks = new ArrayList<>();

    for (int i = 0; i < 5; i++) {
      final int value = i;
      tasks.add(() -> value * 2);
    }

    // executeAndWait should handle tasks with return values
    StageThreadPoolHelper.executeAndWait("ReturnValueStage", tasks);
  }

  @Test
  public void testLargeNumberOfTasks() throws InterruptedException {
    // Test with a large number of tasks
    int numTasks = 100;
    AtomicInteger counter = new AtomicInteger(0);
    List<Callable<Void>> tasks = new ArrayList<>();

    for (int i = 0; i < numTasks; i++) {
      tasks.add(() -> {
        counter.incrementAndGet();
        TimeUnit.MILLISECONDS.sleep(1); // Small delay
        return null;
      });
    }

    StageThreadPoolHelper.executeAndWait("LargeStage", tasks);
    Assert.assertEquals(counter.get(), numTasks, "All tasks should complete");
  }

  @Test(expectedExceptions = InterruptedException.class)
  public void testInterruptedExecution() throws InterruptedException {
    // Test behavior when thread is interrupted during execution
    List<Callable<Void>> tasks = new ArrayList<>();

    tasks.add(() -> {
      TimeUnit.MILLISECONDS.sleep(5000); // Long sleep
      return null;
    });

    // Interrupt the current thread after a short delay
    Thread testThread = Thread.currentThread();
    new Thread(() -> {
      try {
        TimeUnit.MILLISECONDS.sleep(100);
        testThread.interrupt();
      } catch (InterruptedException e) {
        // Ignore
      }
    }).start();

    StageThreadPoolHelper.executeAndWait("InterruptedStage", tasks);
  }

  @Test
  public void testMixedTaskTypes() throws InterruptedException {
    // Test with different types of tasks
    AtomicInteger shortTaskCount = new AtomicInteger(0);
    AtomicInteger longTaskCount = new AtomicInteger(0);
    List<Callable<Void>> tasks = new ArrayList<>();

    // Add short tasks
    for (int i = 0; i < 5; i++) {
      tasks.add(() -> {
        shortTaskCount.incrementAndGet();
        return null;
      });
    }

    // Add longer tasks
    for (int i = 0; i < 3; i++) {
      tasks.add(() -> {
        TimeUnit.MILLISECONDS.sleep(20);
        longTaskCount.incrementAndGet();
        return null;
      });
    }

    StageThreadPoolHelper.executeAndWait("MixedStage", tasks);

    Assert.assertEquals(shortTaskCount.get(), 5, "All short tasks should complete");
    Assert.assertEquals(longTaskCount.get(), 3, "All long tasks should complete");
  }

  @Test(timeOut = 10000)
  public void testShutdownRacingWithSubmissionDoesNotHang() throws Exception {
    // Regression for a subtle behavior with ThreadPoolExecutor.CallerRunsPolicy + submit():
    // if shutdown races with submit(), the rejected FutureTask can be silently dropped leaving a
    // Future that never completes, and Future.get() can hang forever.
    //
    // StageThreadPoolHelper should never hang here; rejected tasks should be cancelled.
    int numTasks = 200;
    List<Callable<Void>> tasks = new ArrayList<>(numTasks);
    for (int i = 0; i < numTasks; i++) {
      tasks.add(() -> null);
    }

    Collection<Callable<Void>> slowSubmittingTasks = new SlowIteratingCollection<>(tasks, 2);

    AtomicInteger failures = new AtomicInteger(0);
    Thread runner = new Thread(() -> {
      try {
        StageThreadPoolHelper.executeAndWait("ShutdownRaceStage", slowSubmittingTasks);
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        failures.incrementAndGet();
      } catch (RuntimeException e) {
        failures.incrementAndGet();
      }
    }, "testShutdownRacingWithSubmissionDoesNotHang-runner");

    runner.start();
    // Try to cut into the submit loop.
    TimeUnit.MILLISECONDS.sleep(25);
    StageThreadPoolHelper.shutdown();

    runner.join(TimeUnit.SECONDS.toMillis(5));
    Assert.assertFalse(runner.isAlive(), "executeAndWait should not hang even if shutdown races");
    Assert.assertEquals(failures.get(), 0, "executeAndWait should not throw");
  }

  private static final class SlowIteratingCollection<T> implements Collection<T> {
    private final Collection<T> _delegate;
    private final long _delayPerNextMs;

    private SlowIteratingCollection(Collection<T> delegate, long delayPerNextMs) {
      _delegate = delegate;
      _delayPerNextMs = delayPerNextMs;
    }

    @Override
    public int size() {
      return _delegate.size();
    }

    @Override
    public boolean isEmpty() {
      return _delegate.isEmpty();
    }

    @Override
    public boolean contains(Object o) {
      return _delegate.contains(o);
    }

    @Override
    public Iterator<T> iterator() {
      Iterator<T> it = _delegate.iterator();
      return new Iterator<T>() {
        @Override
        public boolean hasNext() {
          return it.hasNext();
        }

        @Override
        public T next() {
          if (_delayPerNextMs > 0) {
            try {
              TimeUnit.MILLISECONDS.sleep(_delayPerNextMs);
            } catch (InterruptedException e) {
              Thread.currentThread().interrupt();
            }
          }
          return it.next();
        }
      };
    }

    @Override
    public Object[] toArray() {
      return _delegate.toArray();
    }

    @Override
    public <E> E[] toArray(E[] a) {
      return _delegate.toArray(a);
    }

    @Override
    public boolean add(T t) {
      return _delegate.add(t);
    }

    @Override
    public boolean remove(Object o) {
      return _delegate.remove(o);
    }

    @Override
    public boolean containsAll(Collection<?> c) {
      return _delegate.containsAll(c);
    }

    @Override
    public boolean addAll(Collection<? extends T> c) {
      return _delegate.addAll(c);
    }

    @Override
    public boolean removeAll(Collection<?> c) {
      return _delegate.removeAll(c);
    }

    @Override
    public boolean retainAll(Collection<?> c) {
      return _delegate.retainAll(c);
    }

    @Override
    public void clear() {
      _delegate.clear();
    }
  }
}
