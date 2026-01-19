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
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import org.testng.Assert;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

public class TestStageThreadPoolHelper {

  private StageThreadPoolHelper _helper;

  @BeforeMethod
  public void beforeMethod() {
    _helper = new StageThreadPoolHelper("TestCluster");
  }

  @AfterMethod
  public void afterMethod() {
    // Clean up after each test
    if (_helper != null) {
      _helper.shutdown();
    }
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

    _helper.executeAndWait("TestStage", tasks);

    // Verify all tasks were executed
    Assert.assertEquals(counter.get(), 5, "All tasks should be executed");
  }

  @Test
  public void testExecuteAndWaitWithEmptyTasks() throws InterruptedException {
    // Test with empty task collection - should not throw exception
    List<Callable<Void>> emptyTasks = new ArrayList<>();
    _helper.executeAndWait("EmptyStage", emptyTasks);
  }

  @Test
  public void testExecuteAndWaitWithNullTasks() throws InterruptedException {
    // Test with null task collection - should not throw exception
    _helper.executeAndWait("NullStage", null);
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
    _helper.executeAndWait("FailingStage", tasks);

    // Verify that tasks were executed (some succeeded, some failed)
    Assert.assertEquals(successCounter.get(), 1, "Successful tasks should complete");
    Assert.assertEquals(exceptionCounter.get(), 2, "Failing tasks should be attempted");
  }

  @Test
  public void testParallelExecution() throws InterruptedException {
    _helper.setPoolSize(4);

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
    _helper.executeAndWait("ParallelStage", tasks);
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
    // Test that thread names include stage context and cluster name
    String stageName = "ThreadNamingStage";
    List<String> threadNames = Collections.synchronizedList(new ArrayList<>());
    List<Callable<Void>> tasks = new ArrayList<>();

    for (int i = 0; i < 3; i++) {
      tasks.add(() -> {
        threadNames.add(Thread.currentThread().getName());
        return null;
      });
    }

    _helper.executeAndWait(stageName, tasks);

    // Verify thread names contain the stage name
    Assert.assertEquals(threadNames.size(), 3, "Should capture all thread names");
    for (String threadName : threadNames) {
      Assert.assertTrue(threadName.startsWith(stageName + "-"),
          "Thread name should start with stage name: " + threadName);
      Assert.assertTrue(threadName.contains("TestCluster"),
          "Thread name should contain cluster name: " + threadName);
    }
  }

  @Test
  public void testShutdown() throws InterruptedException {
    AtomicInteger counter = new AtomicInteger(0);
    List<Callable<Void>> tasks = new ArrayList<>();

    tasks.add(() -> {
      counter.incrementAndGet();
      return null;
    });

    _helper.executeAndWait("PreShutdownStage", tasks);
    Assert.assertEquals(counter.get(), 1, "Task should execute before shutdown");

    // Shutdown the executor
    _helper.shutdown();

    // Multiple shutdowns should be safe
    _helper.shutdown();
    _helper.shutdown();
  }

  @Test
  public void testExecutorReinitialization() throws InterruptedException {
    AtomicInteger counter = new AtomicInteger(0);
    List<Callable<Void>> tasks = new ArrayList<>();

    tasks.add(() -> {
      counter.incrementAndGet();
      return null;
    });

    // Execute before shutdown
    _helper.executeAndWait("BeforeShutdown", tasks);
    Assert.assertEquals(counter.get(), 1);

    // Shutdown
    _helper.shutdown();

    // Execute after shutdown - should create a new executor
    counter.set(0);
    _helper.executeAndWait("AfterShutdown", tasks);
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
    _helper.executeAndWait("ReturnValueStage", tasks);
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

    _helper.executeAndWait("LargeStage", tasks);
    Assert.assertEquals(counter.get(), numTasks, "All tasks should complete");
  }

  @Test(expectedExceptions = InterruptedException.class)
  public void testInterruptedExecution() throws InterruptedException {
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

    _helper.executeAndWait("InterruptedStage", tasks);
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

    _helper.executeAndWait("MixedStage", tasks);

    Assert.assertEquals(shortTaskCount.get(), 5, "All short tasks should complete");
    Assert.assertEquals(longTaskCount.get(), 3, "All long tasks should complete");
  }

  @Test
  public void testConfigurablePoolSize() throws InterruptedException {
    // Test that the pool size can be configured
    _helper.setPoolSize(2);
    Assert.assertEquals(_helper.getPoolSize(), 2, "Pool size should be 2");

    // Test with tasks that require parallel execution
    int numTasks = 4;
    AtomicInteger concurrentCount = new AtomicInteger(0);
    AtomicInteger maxConcurrent = new AtomicInteger(0);
    List<Callable<Void>> tasks = new ArrayList<>();

    for (int i = 0; i < numTasks; i++) {
      tasks.add(() -> {
        int current = concurrentCount.incrementAndGet();
        // Track max concurrent execution
        synchronized (maxConcurrent) {
          if (current > maxConcurrent.get()) {
            maxConcurrent.set(current);
          }
        }
        TimeUnit.MILLISECONDS.sleep(100); // Simulate work
        concurrentCount.decrementAndGet();
        return null;
      });
    }

    _helper.executeAndWait("ConfiguredPoolStage", tasks);

    // With pool size of 2, max concurrent should be 2
    Assert.assertTrue(maxConcurrent.get() <= 2,
        "Max concurrent tasks should not exceed configured pool size of 2, but was: " + maxConcurrent.get());
  }

  @Test
  public void testDefaultPoolSize() {
    Assert.assertEquals(_helper.getPoolSize(), StageThreadPoolHelper.DEFAULT_POOL_SIZE,
        "Pool size should be the default");
  }

  @Test
  public void testInvalidPoolSizeFallsBackToDefault() {
    // Test that invalid pool sizes fall back to default
    _helper.setPoolSize(0);
    Assert.assertEquals(_helper.getPoolSize(), StageThreadPoolHelper.DEFAULT_POOL_SIZE,
        "Pool size 0 should fall back to default");

    _helper.setPoolSize(-1);
    Assert.assertEquals(_helper.getPoolSize(), StageThreadPoolHelper.DEFAULT_POOL_SIZE,
        "Negative pool size should fall back to default");
  }

  @Test
  public void testMultipleControllersHaveSeparatePools() throws InterruptedException {
    // Test that each controller instance has its own separate pool
    StageThreadPoolHelper helper1 = new StageThreadPoolHelper("Cluster1");
    StageThreadPoolHelper helper2 = new StageThreadPoolHelper("Cluster2");

    try {
      // Configure different pool sizes
      helper1.setPoolSize(2);
      helper2.setPoolSize(4);

      Assert.assertEquals(helper1.getPoolSize(), 2, "Cluster1 pool size should be 2");
      Assert.assertEquals(helper2.getPoolSize(), 4, "Cluster2 pool size should be 4");

      // Track concurrent executions for each helper
      AtomicInteger concurrentCount1 = new AtomicInteger(0);
      AtomicInteger maxConcurrent1 = new AtomicInteger(0);
      AtomicInteger concurrentCount2 = new AtomicInteger(0);
      AtomicInteger maxConcurrent2 = new AtomicInteger(0);

      List<Callable<Void>> tasks1 = new ArrayList<>();
      List<Callable<Void>> tasks2 = new ArrayList<>();

      for (int i = 0; i < 4; i++) {
        tasks1.add(() -> {
          int current = concurrentCount1.incrementAndGet();
          synchronized (maxConcurrent1) {
            if (current > maxConcurrent1.get()) {
              maxConcurrent1.set(current);
            }
          }
          TimeUnit.MILLISECONDS.sleep(100);
          concurrentCount1.decrementAndGet();
          return null;
        });
        tasks2.add(() -> {
          int current = concurrentCount2.incrementAndGet();
          synchronized (maxConcurrent2) {
            if (current > maxConcurrent2.get()) {
              maxConcurrent2.set(current);
            }
          }
          TimeUnit.MILLISECONDS.sleep(100);
          concurrentCount2.decrementAndGet();
          return null;
        });
      }

      // Execute on both helpers
      Thread t1 = new Thread(() -> {
        try {
          helper1.executeAndWait("Stage1", tasks1);
        } catch (InterruptedException e) {
          Thread.currentThread().interrupt();
        }
      });
      Thread t2 = new Thread(() -> {
        try {
          helper2.executeAndWait("Stage2", tasks2);
        } catch (InterruptedException e) {
          Thread.currentThread().interrupt();
        }
      });

      t1.start();
      t2.start();
      t1.join();
      t2.join();

      // Verify each pool respected its own size limit
      Assert.assertTrue(maxConcurrent1.get() <= 2,
          "Cluster1 max concurrent should be <= 2, but was: " + maxConcurrent1.get());
      Assert.assertTrue(maxConcurrent2.get() <= 4,
          "Cluster2 max concurrent should be <= 4, but was: " + maxConcurrent2.get());
    } finally {
      helper1.shutdown();
      helper2.shutdown();
    }
  }

  @Test
  public void testShutdownDoesNotAffectOtherInstances() throws InterruptedException {
    // Test that shutting down one helper doesn't affect another
    StageThreadPoolHelper helper1 = new StageThreadPoolHelper("Cluster1");
    StageThreadPoolHelper helper2 = new StageThreadPoolHelper("Cluster2");

    try {
      AtomicInteger counter1 = new AtomicInteger(0);
      AtomicInteger counter2 = new AtomicInteger(0);

      List<Callable<Void>> tasks1 = new ArrayList<>();
      tasks1.add(() -> {
        counter1.incrementAndGet();
        return null;
      });

      List<Callable<Void>> tasks2 = new ArrayList<>();
      tasks2.add(() -> {
        counter2.incrementAndGet();
        return null;
      });

      // Execute on both
      helper1.executeAndWait("Stage1", tasks1);
      helper2.executeAndWait("Stage2", tasks2);

      Assert.assertEquals(counter1.get(), 1);
      Assert.assertEquals(counter2.get(), 1);

      // Shutdown helper1
      helper1.shutdown();

      // helper2 should still work
      counter2.set(0);
      helper2.executeAndWait("Stage2AfterShutdown", tasks2);
      Assert.assertEquals(counter2.get(), 1, "helper2 should still work after helper1 shutdown");
    } finally {
      helper1.shutdown();
      helper2.shutdown();
    }
  }
}
