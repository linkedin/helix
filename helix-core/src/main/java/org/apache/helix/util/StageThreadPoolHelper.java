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

import com.google.common.util.concurrent.ThreadFactoryBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Helper class for parallel execution in Helix controller pipeline stages.
 * Each controller instance owns its own thread pool with configurable size.
 * <p>
 * The pool size can be configured via {@link #setPoolSize(int)} which is typically
 * called by the controller based on ClusterConfig.STAGE_PARALLEL_THREAD_POOL_SIZE.
 * <p>
 * Features:
 * <ul>
 *   <li>Per-controller thread pool for isolation between controllers on same node</li>
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

  /**
   * Queue capacity multiplier relative to pool size.
   * With CallerRunsPolicy, when queue is full, tasks execute in the caller thread,
   * providing natural backpressure instead of unbounded queue growth.
   */
  private static final int QUEUE_CAPACITY_MULTIPLIER = 2;

  private final String _clusterName;
  private volatile ThreadPoolExecutor _executor;
  private volatile int _configuredPoolSize;
  private final Object _lock = new Object();

  /**
   * Creates a new StageThreadPoolHelper for a specific controller/cluster.
   *
   * @param clusterName the name of the cluster this helper is associated with (may be null for legacy controllers)
   */
  public StageThreadPoolHelper(String clusterName) {
    _clusterName = clusterName != null ? clusterName : "unknown";
    _configuredPoolSize = DEFAULT_POOL_SIZE;
    LOG.info("Created StageThreadPoolHelper for cluster {}", _clusterName);
  }

  /**
   * Set the thread pool size. If the pool already exists with a different size,
   * it will be recreated with the new size on next use.
   *
   * @param poolSize the desired pool size (must be positive, otherwise default is used)
   */
  public void setPoolSize(int poolSize) {
    if (poolSize <= 0) {
      LOG.warn("Invalid pool size {}, using default {}", poolSize, DEFAULT_POOL_SIZE);
      poolSize = DEFAULT_POOL_SIZE;
    }

    synchronized (_lock) {
      if (poolSize != _configuredPoolSize) {
        int oldPoolSize = _configuredPoolSize;
        _configuredPoolSize = poolSize;

        // Shutdown existing pool so it gets recreated with new size on next use
        if (_executor != null && !_executor.isShutdown()) {
          LOG.info("Shutting down current executor with thread pool size {} for cluster {}, "
              + "next executor will be started with thread pool size {}",
              oldPoolSize, _clusterName, poolSize);
          _executor.shutdown();
          _executor = null;
        }
      }
    }
  }

  /**
   * Get the configured pool size.
   *
   * @return the configured pool size
   */
  public int getPoolSize() {
    return _configuredPoolSize;
  }

  /**
   * Execute multiple tasks in parallel for a stage and wait for all to complete.
   *
   * @param stageName name of the stage (used for logging and thread naming)
   * @param tasks collection of tasks to execute in parallel
   * @throws InterruptedException if the current thread is interrupted while waiting
   */
  public void executeAndWait(String stageName, Collection<? extends Callable<?>> tasks)
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
        LOG.warn("Task in stage {} failed for cluster {}: {}",
            stageName, _clusterName, e.getCause().getMessage(), e);
      }
    }

    LOG.debug("Completed parallel execution for stage {} in cluster {}", stageName, _clusterName);
  }

  /**
   * Gracefully shutdown the executor for this controller.
   * This should be called when the controller is shutting down.
   */
  public void shutdown() {
    synchronized (_lock) {
      if (_executor == null || _executor.isShutdown()) {
        return;
      }

      LOG.info("Shutting down stage parallel executor for cluster {}", _clusterName);
      _executor.shutdown();
      try {
        if (!_executor.awaitTermination(SHUTDOWN_TIMEOUT_SECONDS, TimeUnit.SECONDS)) {
          LOG.warn("Executor for cluster {} did not terminate gracefully, forcing shutdown",
              _clusterName);
          _executor.shutdownNow();
        }
      } catch (InterruptedException e) {
        LOG.warn("Interrupted during shutdown for cluster {}", _clusterName, e);
        _executor.shutdownNow();
        Thread.currentThread().interrupt();
      }
      _executor = null;
    }
  }

  /**
   * Lazily initialize the thread pool for this controller.
   */
  private ThreadPoolExecutor getOrCreateExecutor() {
    if (_executor != null && !_executor.isShutdown()) {
      return _executor;
    }

    synchronized (_lock) {
      if (_executor != null && !_executor.isShutdown()) {
        return _executor;
      }

      ThreadFactory threadFactory = new ThreadFactoryBuilder()
          .setNameFormat(THREAD_NAME_PREFIX + "-" + _clusterName + "-%d")
          .setDaemon(true)
          .setUncaughtExceptionHandler((t, e) ->
              LOG.error("Uncaught exception in stage thread {} for cluster {}",
                  t.getName(), _clusterName, e))
          .build();

      // Use bounded queue to prevent unbounded memory growth if tasks pile up.
      // With CallerRunsPolicy, when queue is full, tasks execute in the caller thread,
      // providing natural backpressure.
      int queueCapacity = _configuredPoolSize * QUEUE_CAPACITY_MULTIPLIER;
      _executor = new ThreadPoolExecutor(
          _configuredPoolSize,
          _configuredPoolSize,
          THREAD_KEEP_ALIVE_MINUTES,
          TimeUnit.MINUTES,
          new LinkedBlockingQueue<>(queueCapacity),
          threadFactory,
          new ThreadPoolExecutor.CallerRunsPolicy());

      _executor.allowCoreThreadTimeOut(true);
      LOG.info("Initialized stage parallel executor with {} threads and queue capacity {} for cluster {}",
          _configuredPoolSize, queueCapacity, _clusterName);
      return _executor;
    }
  }

  /**
   * Wraps the callable to temporarily rename the current thread
   * with stage-specific prefix during execution (for debugging).
   */
  private Callable<?> wrapWithStageContext(Callable<?> task, String stageName) {
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
