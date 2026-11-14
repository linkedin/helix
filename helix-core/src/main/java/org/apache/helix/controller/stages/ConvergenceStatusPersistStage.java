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
 *   http://www.apache.org/licenses/LICENSE-2.0
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
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReentrantLock;

import org.apache.helix.HelixDataAccessor;
import org.apache.helix.HelixManager;
import org.apache.helix.PropertyKey;
import org.apache.helix.controller.dataproviders.BaseControllerDataProvider;
import org.apache.helix.controller.pipeline.AbstractAsyncBaseStage;
import org.apache.helix.controller.pipeline.AsyncWorkerType;
import org.apache.helix.controller.rebalancer.waged.WagedRebalanceStatus;
import org.apache.helix.controller.rebalancer.waged.WagedRebalancer;
import org.apache.helix.model.ClusterConfig;
import org.apache.helix.model.ConvergenceStatus;
import org.apache.helix.model.ConvergenceStatus.OptimizerStatus;
import org.apache.helix.model.ConvergenceStatus.Status;
import org.apache.helix.model.ConvergenceStatus.TargetFreshness;
import org.apache.helix.zookeeper.datamodel.serializer.ZNRecordSerializer;
import org.apache.helix.zookeeper.zkclient.exception.ZkMarshallingError;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Computes the convergence report off the pipeline thread and persists it as one ZooKeeper
 * record per cluster, so the cost does not grow with the number of resources or with churn.
 *
 * <ul>
 * <li>It never changes what the pipeline does and never schedules a pipeline run. The pipeline
 * thread only snapshots the inputs and queues the work; the worker queue keeps only the latest
 * event. Writes that are held back or failed are retried by {@link #tick}, which the controller
 * calls once per {@link #TICK_MS} on its own timer thread.</li>
 * <li>The record lists only the resources that are not converged and healthy, worst first. It is
 * compressed, and kept under {@link #MAX_LISTED} entries and {@link #MAX_BYTES} by listing fewer
 * resources, never by failing the write.</li>
 * <li>The record is rewritten only when its content changes: a change into or out of CONVERGED
 * at most once per {@link #STATUS_MIN_INTERVAL_MS}, any other change at most once per
 * {@link #MIN_REWRITE_MS}. Failed writes back off up to {@link #RETRY_MAX_MS}.</li>
 * <li>The write carries the controller session, so a controller that lost it cannot overwrite its
 * successor's record, and readers trust the record only while that session leads the cluster.</li>
 * </ul>
 */
public class ConvergenceStatusPersistStage extends AbstractAsyncBaseStage {
  private static final Logger LOG = LoggerFactory.getLogger(ConvergenceStatusPersistStage.class);
  public static final long TICK_MS = 1_000L;
  static final long STATUS_MIN_INTERVAL_MS = 1_000L;
  static final long MIN_REWRITE_MS = 10_000L;
  public static final long HEARTBEAT_MS = 60_000L;
  static final long RETRY_MIN_MS = 1_000L;
  // Well under the five minutes after which readers call the record stale.
  static final long RETRY_MAX_MS = 60_000L;
  static final int MAX_LISTED = 10_000;
  static final int MAX_BYTES = 512 * 1024;
  // Below this many listed resources the record cannot come near MAX_BYTES, so it is not measured.
  static final int MEASURE_ABOVE = 1_000;

  static final class Written {
    final ConvergenceStatus _report;
    final long _writtenAt;
    // Written sooner than MIN_REWRITE_MS after the one before, so the next one may not be.
    final boolean _fast;

    Written(ConvergenceStatus report, long writtenAt, boolean fast) {
      _report = report;
      _writtenAt = writtenAt;
      _fast = fast;
    }
  }

  // Guarded by _lock: the async worker and the controller's timer both publish.
  private final ReentrantLock _lock = new ReentrantLock();
  private HelixManager _manager;
  private String _session;
  private String _loadedSession;
  private Written _written;
  private ConvergenceStatus _pending;
  private long _backoffUntil;
  private int _failures;
  // Runs that reached this stage, and the latest one whose result was handled. While some run is
  // not handled the record is not confirmed, so a run that failed in here can never look fresh.
  final AtomicLong _submitted = new AtomicLong();
  private volatile long _handled;
  // Set from other threads; the tick acts on them under the lock.
  private volatile boolean _disabled;
  private volatile boolean _forgetRequested;
  // The last result and its WAGED rebalancer, to apply a WAGED health change between runs without
  // running the pipeline. The result keeps per resource counts only, not the run's inputs.
  ConvergenceStatusCalculator.Result _lastResult;
  WagedRebalancer _wagedRebalancer;

  /**
   * Never throws: a failure here would stop every later pipeline of this event, external view
   * included, and reporting must never cost the cluster that.
   */
  @Override
  public void process(ClusterEvent event) {
    try {
      BaseControllerDataProvider cache =
          event.getAttribute(AttributeName.ControllerDataProvider.name());
      HelixManager manager = event.getAttribute(AttributeName.helixmanager.name());
      ClusterConfig config = cache == null ? null : cache.getClusterConfig();
      if (manager == null || config == null
          || event.getAttribute(AttributeName.RESOURCES_TO_REBALANCE.name()) == null) {
        return;
      }
      _disabled = !config.isConvergenceMonitoringEnabled();
      if (_disabled) {
        return;
      }
      // Counted first, so a run that fails to snapshot is not handled either.
      long generation = _submitted.incrementAndGet();
      ConvergenceStatusCalculator.Inputs inputs =
          ConvergenceStatusCalculator.Inputs.from(event, cache, manager.getClusterName())
              .controllerName(manager.getInstanceName());
      inputs._generation = generation;
      event.addAttribute(AttributeName.CONVERGENCE_STATUS_INPUTS.name(), inputs);
      super.process(event);
    } catch (Exception e) {
      LOG.warn("Skipped convergence reporting for event {}", event.getEventId(), e);
    }
  }

  @Override
  public AsyncWorkerType getAsyncWorkerType() {
    return AsyncWorkerType.ConvergenceStatusPersistWorker;
  }

  @Override
  public void execute(ClusterEvent event) {
    HelixManager manager = event.getAttribute(AttributeName.helixmanager.name());
    ConvergenceStatusCalculator.Inputs inputs =
        event.getAttribute(AttributeName.CONVERGENCE_STATUS_INPUTS.name());
    String session = manager == null ? null : manager.getSessionId();
    if (inputs == null || session == null || !isEventSession(event, session)) {
      return;
    }
    Object rebalancer = event.getAttribute(AttributeName.STATEFUL_REBALANCER.name());
    long now = System.currentTimeMillis();
    ConvergenceStatusCalculator.Result result =
        ConvergenceStatusCalculator.calculate(inputs, session, now);
    _lock.lock();
    try {
      boolean waged = rebalancer instanceof WagedRebalancer && result.hasWagedResources();
      _lastResult = waged ? result : null;
      _wagedRebalancer = waged ? (WagedRebalancer) rebalancer : null;
      publish(manager, session, result, now, inputs._generation);
    } finally {
      _lock.unlock();
    }
  }

  /**
   * Publishes a result and returns the ms until the next attempt, or -1 when nothing is left to
   * do: written, unchanged, or this controller does not lead the cluster.
   */
  long persist(ClusterEvent event, HelixManager manager, ConvergenceStatusCalculator.Result result,
      long now) {
    String session = manager.getSessionId();
    if (session == null || !isEventSession(event, session)) {
      return -1L;
    }
    _lock.lock();
    try {
      return publish(manager, session, result, now, _submitted.get());
    } finally {
      _lock.unlock();
    }
  }

  /** @param generation the run this result comes from, or 0 for one that only reapplies it */
  private long publish(HelixManager manager, String session,
      ConvergenceStatusCalculator.Result result, long now, long generation) {
    if (_forgetRequested) {
      // Leadership changed since: start over, so the leader check below runs again.
      forget();
    }
    _manager = manager;
    _session = session;
    _pending = null;
    try {
      if (!session.equals(_loadedSession)) {
        if (!isLeader(manager, session)) {
          forget();
          return -1L;
        }
        // Adopt what the previous leader wrote, so an unchanged record is not rewritten.
        ConvergenceStatus existing =
            manager.getHelixDataAccessor().getProperty(key(manager));
        _written = existing == null ? null : new Written(existing, 0L, false);
        _loadedSession = session;
      }
      ConvergenceStatus record = result.getCluster();
      List<String> listed =
          fold(record, result.getResources(), _written == null ? null : _written._report, now);
      // Before the change check, so a record trimmed the same way compares equal.
      if (listed.size() > MEASURE_ABOVE) {
        fit(record, listed, MAX_BYTES);
      }
      _pending = record;
    } catch (RuntimeException e) {
      // Stays behind, so the record is not confirmed on content this run could not produce.
      LOG.warn("Failed to prepare the convergence report of cluster {}",
          manager.getClusterName(), e);
      return -1L;
    }
    // Handled; a pending record keeps it unconfirmed until it is written.
    _handled = Math.max(_handled, generation);
    return attempt(now);
  }

  /**
   * Called by the controller on its timer: retries a held back or failed write, picks up a WAGED
   * health change, and confirms an unchanged record once per {@link #HEARTBEAT_MS} while the
   * controller is caught up. Skips the round if the worker is busy publishing.
   */
  public void tick(long now, boolean controllerCaughtUp) {
    if (!_lock.tryLock()) {
      return;
    }
    try {
      // Nothing to do while disconnected; getSessionId would block.
      if (_manager != null && !_manager.isConnected()) {
        return;
      }
      if (_disabled || _forgetRequested
          || (_manager != null && !Objects.equals(_session, _manager.getSessionId()))) {
        forget();
        return;
      }
      // Only on a caught up controller: otherwise a run for the change may still be on its way.
      boolean current = controllerCaughtUp && !isBehind();
      if (current) {
        refreshWagedStatus(now);
      }
      if (_pending != null) {
        attempt(now);
      } else if (current) {
        beat(now);
      }
    } catch (RuntimeException e) {
      LOG.warn("Convergence report tick failed", e);
    } finally {
      _lock.unlock();
    }
  }

  boolean isBehind() {
    return _handled < _submitted.get();
  }

  private void refreshWagedStatus(long now) {
    if (_wagedRebalancer == null || _lastResult == null || _manager == null) {
      return;
    }
    WagedRebalanceStatus status = _wagedRebalancer.getConvergenceStatus();
    if (sameWaged(status, _lastResult.getWagedStatus())) {
      return;
    }
    // Kept only once it was computed, so a failure computing it is retried by the next tick.
    _lastResult = ConvergenceStatusCalculator.withWagedStatus(_lastResult, status, now);
    publish(_manager, _session, _lastResult, now, 0L);
  }

  private long attempt(long now) {
    if (_pending == null) {
      return -1L;
    }
    if (now < _backoffUntil) {
      return _backoffUntil - now;
    }
    boolean takeOver = _written == null
        || !_session.equals(_written._report.getControllerSessionId());
    long wait = takeOver ? 0L : writeDelay(_pending, _written, now);
    if (wait < 0 && now - _written._writtenAt >= HEARTBEAT_MS) {
      // Unchanged, but a run confirmed it, so it doubles as the beat.
      wait = 0L;
    }
    if (wait < 0) {
      _pending = null;
      return -1L;
    }
    if (wait > 0) {
      return wait;
    }
    return write(_pending, now) ? -1L : _backoffUntil - now;
  }

  /** Confirms the unchanged record of a caught-up controller; moves only when it was confirmed. */
  private void beat(long now) {
    if (_written == null || _session == null || _manager == null
        || !_session.equals(_written._report.getControllerSessionId())
        || now - _written._writtenAt < HEARTBEAT_MS || now < _backoffUntil) {
      return;
    }
    _written._report.setEvaluatedAt(now);
    write(_written._report, now);
  }

  private boolean write(ConvergenceStatus record, long now) {
    boolean ok = false;
    try {
      // A failed check may be a failed read rather than a lost leadership, so the record is kept
      // and retried with backoff; a real loss is handled by requestForget and the session check.
      if (isLeader(_manager, _session)) {
        // Only the async write enforces the session the record expects.
        boolean[] written = _manager.getHelixDataAccessor().setChildren(
            Collections.singletonList(key(_manager)), Collections.singletonList(record));
        ok = written.length > 0 && written[0];
      }
    } catch (RuntimeException e) {
      LOG.warn("Failed to persist the convergence report of cluster {}",
          _manager.getClusterName(), e);
      ok = false;
    }
    if (ok) {
      boolean fast = _written != null && now - _written._writtenAt < MIN_REWRITE_MS;
      _written = new Written(record, now, fast);
      _pending = null;
      _failures = 0;
      _backoffUntil = 0L;
    } else {
      _failures++;
      _backoffUntil = now + retryDelay();
    }
    return ok;
  }

  /** Asks the next tick to drop everything; called when this controller stops leading. */
  public void requestForget() {
    _forgetRequested = true;
  }

  /**
   * Drops everything held for publishing once this controller does not lead the cluster, so a
   * former leader keeps no inputs in memory and its tick makes no ZooKeeper call.
   */
  private void forget() {
    _pending = null;
    _written = null;
    _loadedSession = null;
    _lastResult = null;
    _wagedRebalancer = null;
    _manager = null;
    _session = null;
    _forgetRequested = false;
    _backoffUntil = 0L;
    _failures = 0;
  }

  /**
   * Adds the resources that are not converged and healthy to the cluster record, worst first, and
   * returns them in that order. In maintenance mode the cluster record alone says PAUSED.
   */
  static List<String> fold(ConvergenceStatus record, Map<String, ConvergenceStatus> resources,
      ConvergenceStatus previous, long now) {
    boolean paused = record.getStatus() == Status.PAUSED;
    // A record left by an earlier leader long ago says nothing about when this started.
    if (previous != null && previous.getEvaluatedAt() > 0
        && now - previous.getEvaluatedAt() > 5 * HEARTBEAT_MS) {
      previous = null;
    }
    List<String> listed = new ArrayList<>();
    long oldestUnconverged = 0L;
    for (Map.Entry<String, ConvergenceStatus> entry : resources.entrySet()) {
      String name = entry.getKey();
      ConvergenceStatus report = entry.getValue();
      if (paused || isHealthy(report)) {
        continue;
      }
      carryUnconvergedSince(report, previous == null ? null : previous.getResourceReport(name),
          now);
      if (report.getUnconvergedSince() > 0
          && (oldestUnconverged == 0 || report.getUnconvergedSince() < oldestUnconverged)) {
        oldestUnconverged = report.getUnconvergedSince();
      }
      listed.add(name);
    }
    listed.sort(Comparator.comparingInt((String name) -> severity(resources.get(name)))
        .thenComparingLong(name -> resources.get(name).getUnconvergedSince())
        .thenComparing(name -> name));
    int unhealthy = listed.size();
    while (listed.size() > MAX_LISTED) {
      listed.remove(listed.size() - 1);
    }
    for (String name : listed) {
      record.putResourceReport(name, resources.get(name));
    }
    record.setUnlistedResourceCount(unhealthy - listed.size());
    if (paused) {
      carryUnconvergedSince(record, previous, now);
    } else {
      record.setUnconvergedSince(
          record.getStatus() == Status.CONVERGED ? 0L : oldestUnconverged);
    }
    return listed;
  }

  /**
   * Keeps the serialized record under {@code maxBytes} by dropping the least urgent listed
   * resources, half at a time; they still count as unlisted.
   */
  static void fit(ConvergenceStatus record, List<String> listed, int maxBytes) {
    int unlisted = record.getUnlistedResourceCount();
    while (serializedSize(record) > maxBytes) {
      if (listed.isEmpty()) {
        return;
      }
      int keep = listed.size() / 2;
      for (String name : listed.subList(keep, listed.size())) {
        record.removeResourceReport(name);
        unlisted++;
      }
      listed.subList(keep, listed.size()).clear();
      record.setUnlistedResourceCount(unlisted);
    }
  }

  private static int serializedSize(ConvergenceStatus record) {
    try {
      return new ZNRecordSerializer().serialize(record.getRecord()).length;
    } catch (ZkMarshallingError e) {
      return Integer.MAX_VALUE;
    }
  }

  private static boolean isHealthy(ConvergenceStatus report) {
    return report.getStatus() == Status.CONVERGED
        && report.getTargetFreshness() == TargetFreshness.CURRENT
        && (report.getOptimizerStatus() == OptimizerStatus.HEALTHY
            || report.getOptimizerStatus() == OptimizerStatus.NOT_APPLICABLE);
  }

  private static int severity(ConvergenceStatus report) {
    switch (report.getStatus()) {
      case BLOCKED:
        return 0;
      case UNKNOWN:
        return 1;
      case IN_PROGRESS:
        return 2;
      default:
        return 3;
    }
  }

  private static boolean sameWaged(WagedRebalanceStatus a, WagedRebalanceStatus b) {
    if (a == null || b == null) {
      return a == b;
    }
    return a.isOutcomeKnown() == b.isOutcomeKnown()
        && a.isLastKnownGoodFallback() == b.isLastKnownGoodFallback()
        && a.isServingComputationFailed() == b.isServingComputationFailed()
        && a.isBaselineComputationFailed() == b.isBaselineComputationFailed()
        && Objects.equals(a.getServingFailureCategory(), b.getServingFailureCategory());
  }

  private long retryDelay() {
    return Math.min(RETRY_MAX_MS, RETRY_MIN_MS << Math.min(Math.max(_failures - 1, 0), 20));
  }

  /**
   * -1 when the content did not change, 0 to write now, otherwise the ms until it may be written.
   * A change into or out of CONVERGED waits at most {@link #STATUS_MIN_INTERVAL_MS}, so it shows
   * quickly; anything else, including flapping between unconverged states, {@link #MIN_REWRITE_MS}.
   */
  static long writeDelay(ConvergenceStatus record, Written previous, long now) {
    if (previous == null) {
      return 0L;
    }
    ConvergenceStatus old = previous._report;
    if (record.semanticallyEquals(old)) {
      record.setGeneratedAt(old.getGeneratedAt());
      return -1L;
    }
    // A flapping cluster gets one fast write, then waits like any other change.
    boolean converging = record.getStatus() != old.getStatus() && !previous._fast
        && (record.getStatus() == Status.CONVERGED || old.getStatus() == Status.CONVERGED);
    long interval = converging ? STATUS_MIN_INTERVAL_MS : MIN_REWRITE_MS;
    return Math.max(0L, previous._writtenAt + interval - now);
  }

  private static void carryUnconvergedSince(ConvergenceStatus report, ConvergenceStatus previous,
      long now) {
    if (report.getStatus() == Status.CONVERGED) {
      report.setUnconvergedSince(0L);
    } else if (previous != null && previous.getStatus() != Status.CONVERGED
        && previous.getUnconvergedSince() > 0) {
      report.setUnconvergedSince(previous.getUnconvergedSince());
    } else {
      report.setUnconvergedSince(now);
    }
  }

  private static PropertyKey key(HelixManager manager) {
    return manager.getHelixDataAccessor().keyBuilder().convergenceStatus();
  }

  private static boolean isEventSession(ClusterEvent event, String session) {
    Optional<String> expected = event.getAttribute(AttributeName.EVENT_SESSION.name());
    return expected == null || !expected.isPresent() || expected.get().equals(session);
  }

  // Cheapest first: isConnected never blocks, getSessionId blocks while disconnected, and
  // isLeader reads ZooKeeper.
  private static boolean isLeader(HelixManager manager, String session) {
    return manager.isConnected() && session.equals(manager.getSessionId())
        && manager.isLeader();
  }
}
