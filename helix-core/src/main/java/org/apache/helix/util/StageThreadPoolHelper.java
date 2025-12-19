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
import java.util.Collection;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import com.google.common.util.concurrent.ThreadFactoryBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Helper class for parallel execution in Helix controller pipeline stages.
 * Uses a shared static thread pool with configurable size.
 * <p>
 * The pool size can be configured via {@link #setPoolSize(int)} which is typically
 * called by the controller based on ClusterConfig.STAGE_PARALLEL_THREAD_POOL_SIZE.
 * <p>
 * Features:
 * <ul>
 *   <li>Single shared thread pool reused by all Helix controller stages</li>
 *   <li>Configurable pool size via cluster configuration</li>
 *   <li>Per-stage thread name prefixes for better debugging</li>
 *   <li>Ensures all stage tasks complete before returning</li>
 *   <li>Uses daemon threads - automatically cleaned up on JVM exit</li>
 * </ul>
 */
public class StageThreadPoolHelper {
  private static final Logger LOG = LoggerFactory.getLogger(StageThreadPoolHelper.class);

  /** Default pool size: min(4, available processors) */
  public static final int DEFAULT_POOL_SIZE =
      Math.min(4, Runtime.getRuntime().availableProcessors());

  private static final long THREAD_KEEP_ALIVE_MINUTES = 3;
  private static final long SHUTDOWN_TIMEOUT_SECONDS = 30;
  private static final String THREAD_NAME_PREFIX = "HelixStageWorker";

  // Shared executor reused across all stages
  private static final AtomicReference<ThreadPoolExecutor> EXECUTOR_REF = new AtomicReference<>();
  private static volatile int _configuredPoolSize = DEFAULT_POOL_SIZE;

  private StageThreadPoolHelper() {
    // Utility class - prevent instantiation
  }

  /**
   * Set the thread pool size. If the pool already exists with a different size,
   * it will be recreated with the new size on next use.
   *
   * @param poolSize the desired pool size (must be positive, otherwise default is used)
   */
  public static synchronized void setPoolSize(int poolSize) {
    if (poolSize <= 0) {
      LOG.warn("Invalid pool size {}, using default {}", poolSize, DEFAULT_POOL_SIZE);
      poolSize = DEFAULT_POOL_SIZE;
    }

    if (poolSize != _configuredPoolSize) {
      LOG.info("Updating stage thread pool size from {} to {}", _configuredPoolSize, poolSize);
      _configuredPoolSize = poolSize;

      // Shutdown existing pool so it gets recreated with new size on next use
      ThreadPoolExecutor existing = EXECUTOR_REF.get();
      if (existing != null && !existing.isShutdown()) {
        existing.shutdown();
        EXECUTOR_REF.set(null);
      }
    }
  }

  /**
   * Get the configured pool size.
   *
   * @return the configured pool size
   */
  public static int getPoolSize() {
    return _configuredPoolSize;
  }

  /**
   * Execute multiple tasks in parallel for a stage and wait for all to complete.
   *
   * @param stageName name of the stage (used for logging and thread naming)
   * @param tasks collection of tasks to execute in parallel
   * @throws InterruptedException if the current thread is interrupted while waiting
   */
  public static void executeAndWait(String stageName, Collection<? extends Callable<?>> tasks)
      throws InterruptedException {
    if (tasks == null || tasks.isEmpty()) {
      return;
    }

    ThreadPoolExecutor executor = getOrCreateExecutor();
    List<Future<?>> futures = new ArrayList<>(tasks.size());

    for (Callable<?> task : tasks) {
      futures.add(executor.submit(wrapWithStageContext(task, stageName)));
    }

    for (Future<?> future : futures) {
      try {
        future.get();
      } catch (ExecutionException e) {
        LOG.warn("Task in stage {} failed: {}", stageName, e.getCause().getMessage(), e);
      }
    }

    LOG.debug("Completed parallel execution for stage {}", stageName);
  }

  /**
   * Gracefully shutdown the shared executor.
   * <p>
   * Note: This should only be called for testing cleanup. In production, the pool
   * uses daemon threads and will be automatically cleaned up when the JVM exits.
   * Controllers should NOT call this on shutdown since the pool is shared.
   */
  public static synchronized void shutdown() {
    ThreadPoolExecutor executor = EXECUTOR_REF.get();
    if (executor == null || executor.isShutdown()) {
      return;
    }

    LOG.info("Shutting down shared stage parallel executor");
    executor.shutdown();
    try {
      if (!executor.awaitTermination(SHUTDOWN_TIMEOUT_SECONDS, TimeUnit.SECONDS)) {
        LOG.warn("Executor did not terminate gracefully, forcing shutdown");
        executor.shutdownNow();
      }
    } catch (InterruptedException e) {
      LOG.warn("Interrupted during shutdown", e);
      executor.shutdownNow();
      Thread.currentThread().interrupt();
    }
    EXECUTOR_REF.set(null);
  }

  /**
   * Lazily initialize the shared thread pool.
   */
  private static synchronized ThreadPoolExecutor getOrCreateExecutor() {
    ThreadPoolExecutor existing = EXECUTOR_REF.get();
    if (existing != null && !existing.isShutdown()) {
      return existing;
    }

    ThreadFactory threadFactory = new ThreadFactoryBuilder()
        .setNameFormat(THREAD_NAME_PREFIX + "-%d")
        .setDaemon(true)
        .setUncaughtExceptionHandler((t, e) ->
            LOG.error("Uncaught exception in stage thread {}", t.getName(), e))
        .build();

    ThreadPoolExecutor executor = new ThreadPoolExecutor(
        _configuredPoolSize,
        _configuredPoolSize,
        THREAD_KEEP_ALIVE_MINUTES,
        TimeUnit.MINUTES,
        new LinkedBlockingQueue<>(),
        threadFactory,
        new ThreadPoolExecutor.CallerRunsPolicy());

    executor.allowCoreThreadTimeOut(true);
    EXECUTOR_REF.set(executor);
    LOG.info("Initialized shared stage parallel executor with {} threads", _configuredPoolSize);
    return executor;
  }

  /**
   * Wraps the callable to temporarily rename the current thread
   * with stage-specific prefix during execution (for debugging).
   */
  private static Callable<?> wrapWithStageContext(Callable<?> task, String stageName) {
    return () -> {
      Thread current = Thread.currentThread();
      String originalName = current.getName();
      current.setName(stageName + "-" + originalName);
      try {
        return task.call();
      } finally {
        current.setName(originalName);
      }
    };
  }
}
