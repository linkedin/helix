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
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import org.apache.helix.BaseDataAccessor;
import org.apache.helix.HelixDataAccessor;
import org.apache.helix.HelixManager;
import org.apache.helix.PropertyKey;
import org.apache.helix.controller.dataproviders.BaseControllerDataProvider;
import org.apache.helix.controller.pipeline.AbstractAsyncBaseStage;
import org.apache.helix.controller.pipeline.AsyncWorkerType;
import org.apache.helix.model.ClusterConfig;
import org.apache.helix.model.ConvergenceStatus;
import org.apache.helix.model.ConvergenceStatus.Status;
import org.apache.helix.util.RebalanceUtil;
import org.apache.helix.zookeeper.datamodel.ZNRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Computes and persists convergence reports off the pipeline thread, keeping ZooKeeper load to
 * the changes that matter.
 *
 * <ul>
 * <li>The pipeline thread only snapshots the inputs (O(resources + live instances)) and queues the
 * work; the worker queue keeps only the latest event.</li>
 * <li>When nothing changed the worker makes no ZooKeeper call at all.</li>
 * <li>A report is rewritten only when its content changes, never because of who wrote it or when.
 * A status change is written at most once per {@link #STATUS_MIN_INTERVAL_MS} per znode, any other
 * change at most once per {@link #MIN_REWRITE_MS}.</li>
 * <li>The cluster record is the trust anchor: readers trust any report only while the cluster
 * record carries the leader's session. A new leader therefore rewrites that one znode, plus the
 * reports whose content changed, instead of every report.</li>
 * <li>While the cluster is in maintenance mode only the cluster record says so; resource reports
 * are left as they are, so entering and leaving maintenance costs one write each.</li>
 * <li>Writes and deletes go out in batches of {@link #BATCH_SIZE}, one batch at a time. A held
 * back change or a failed write is picked up by a follow-up on-demand run, failures backing off
 * exponentially up to {@link #RETRY_MAX_MS}.</li>
 * </ul>
 */
public class ConvergenceStatusPersistStage extends AbstractAsyncBaseStage {
  private static final Logger LOG = LoggerFactory.getLogger(ConvergenceStatusPersistStage.class);
  static final long STATUS_MIN_INTERVAL_MS = 1_000L;
  static final long MIN_REWRITE_MS = 30_000L;
  static final int BATCH_SIZE = 500;
  static final long RETRY_MIN_MS = 1_000L;
  static final long RETRY_MAX_MS = 300_000L;

  static final class Written {
    final ConvergenceStatus _report;
    final long _writtenAt;

    Written(ConvergenceStatus report, long writtenAt) {
      _report = report;
      _writtenAt = writtenAt;
    }
  }

  // Only touched by the async worker thread.
  private String _loadedSession;
  private Written _cluster;
  private final Map<String, Written> _written = new HashMap<>();
  private int _failures;

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
      if (manager == null || config == null || !config.isConvergenceMonitoringEnabled()
          || event.getAttribute(AttributeName.RESOURCES_TO_REBALANCE.name()) == null) {
        return;
      }
      event.addAttribute(AttributeName.CONVERGENCE_STATUS_INPUTS.name(),
          ConvergenceStatusCalculator.Inputs.from(event, cache, manager.getClusterName())
              .controllerName(manager.getInstanceName()));
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
    // Leadership costs a ZooKeeper read, so it is only checked once there is something to write.
    if (inputs == null || session == null || !isEventSession(event, session)) {
      return;
    }
    long now = System.currentTimeMillis();
    ConvergenceStatusCalculator.Result result =
        ConvergenceStatusCalculator.calculate(inputs, session, now);
    persist(event, manager, result, now);
  }

  /** Writes what changed and returns the delay of the follow-up run it scheduled, or -1. */
  long persist(ClusterEvent event, HelixManager manager, ConvergenceStatusCalculator.Result result,
      long now) {
    String session = manager.getSessionId();
    if (session == null) {
      return -1L;
    }
    long followUp;
    try {
      followUp = sync(event, manager, session, result, now);
    } catch (RuntimeException e) {
      _failures++;
      followUp = retryDelay();
      LOG.warn("Failed to persist convergence reports of cluster {}, retrying in {} ms",
          manager.getClusterName(), followUp, e);
    }
    if (followUp >= 0) {
      RebalanceUtil.scheduleOnDemandPipeline(manager.getClusterName(), followUp, false);
    }
    return followUp;
  }

  private long sync(ClusterEvent event, HelixManager manager, String session,
      ConvergenceStatusCalculator.Result result, long now) {
    HelixDataAccessor accessor = manager.getHelixDataAccessor();
    PropertyKey.Builder keys = accessor.keyBuilder();
    boolean leaderConfirmed = false;
    if (!session.equals(_loadedSession)) {
      if (!isCurrentLeader(event, manager)) {
        return -1L;
      }
      leaderConfirmed = true;
      load(accessor, keys, session);
    }

    List<String> writes = new ArrayList<>();
    long followUp = -1L;
    long oldestUnconverged = 0L;
    ConvergenceStatus cluster = result.getCluster();
    // In maintenance mode every resource reads PAUSED. The cluster record says that on its own, so
    // reports already written are left alone; only new resources get one.
    boolean paused = cluster.getStatus() == Status.PAUSED;
    for (Map.Entry<String, ConvergenceStatus> entry : result.getResources().entrySet()) {
      ConvergenceStatus report = entry.getValue();
      Written previous = _written.get(entry.getKey());
      if (paused && previous != null) {
        continue;
      }
      carryUnconvergedSince(report, previous == null ? null : previous._report, now);
      if (report.getUnconvergedSince() > 0
          && (oldestUnconverged == 0 || report.getUnconvergedSince() < oldestUnconverged)) {
        oldestUnconverged = report.getUnconvergedSince();
      }
      long wait = writeDelay(report, previous, now);
      if (wait == 0) {
        writes.add(entry.getKey());
      } else if (wait > 0) {
        followUp = soonest(followUp, wait);
      }
    }
    if (paused) {
      carryUnconvergedSince(cluster, _cluster == null ? null : _cluster._report, now);
    } else {
      cluster.setUnconvergedSince(cluster.getStatus() == Status.CONVERGED ? 0L : oldestUnconverged);
    }
    // Readers trust the reports only while the cluster record carries this session.
    boolean anchorStale = _cluster == null
        || !session.equals(_cluster._report.getControllerSessionId());
    long clusterWait = anchorStale ? 0L : writeDelay(cluster, _cluster, now);
    if (clusterWait > 0) {
      followUp = soonest(followUp, clusterWait);
    }
    List<String> removed = new ArrayList<>();
    for (String name : _written.keySet()) {
      if (!result.getResources().containsKey(name)) {
        removed.add(name);
      }
    }

    if (clusterWait != 0 && writes.isEmpty() && removed.isEmpty()) {
      return followUp;
    }
    if (!leaderConfirmed && !isCurrentLeader(event, manager)) {
      return -1L;
    }

    boolean failed = false;
    for (int start = 0; start < writes.size(); start += BATCH_SIZE) {
      if (start > 0 && !isCurrentLeader(event, manager)) {
        return -1L;
      }
      List<String> names = writes.subList(start, Math.min(writes.size(), start + BATCH_SIZE));
      List<PropertyKey> batchKeys = new ArrayList<>(names.size());
      List<ConvergenceStatus> batchReports = new ArrayList<>(names.size());
      for (String name : names) {
        batchKeys.add(keys.convergenceStatus(name));
        batchReports.add(result.getResources().get(name));
      }
      boolean[] ok = accessor.setChildren(batchKeys, batchReports);
      for (int i = 0; i < names.size(); i++) {
        if (i < ok.length && ok[i]) {
          _written.put(names.get(i), new Written(batchReports.get(i), now));
        } else {
          failed = true;
          LOG.warn("Failed to persist the convergence report of {}", names.get(i));
        }
      }
    }
    BaseDataAccessor<ZNRecord> base = accessor.getBaseDataAccessor();
    for (int start = 0; start < removed.size(); start += BATCH_SIZE) {
      List<String> names = removed.subList(start, Math.min(removed.size(), start + BATCH_SIZE));
      List<String> paths = new ArrayList<>(names.size());
      names.forEach(name -> paths.add(keys.convergenceStatus(name).getPath()));
      boolean[] ok = base.remove(paths, 0);
      for (int i = 0; i < names.size(); i++) {
        // A report deleted by someone else is as good as removed.
        if ((i < ok.length && ok[i]) || !base.exists(paths.get(i), 0)) {
          _written.remove(names.get(i));
        } else {
          failed = true;
        }
      }
    }
    // The cluster record goes last. Taking over the anchor vouches for every resource report, so
    // that only happens once everything above landed.
    if (clusterWait == 0 && !(failed && anchorStale)) {
      if (accessor.setProperty(keys.convergenceStatus(), cluster)) {
        _cluster = new Written(cluster, now);
      } else {
        failed = true;
        LOG.warn("Failed to persist the convergence report of cluster {}",
            manager.getClusterName());
      }
    }

    if (failed) {
      _failures++;
      return soonest(followUp, retryDelay());
    }
    _failures = 0;
    return followUp;
  }

  private long retryDelay() {
    return Math.min(RETRY_MAX_MS, RETRY_MIN_MS << Math.min(Math.max(_failures - 1, 0), 20));
  }

  private static long soonest(long current, long candidate) {
    return current < 0 ? candidate : Math.min(current, candidate);
  }

  /**
   * -1 when the content did not change, 0 to write now, otherwise the ms until it may be written.
   * A first report goes out at once, a status change at most once per
   * {@link #STATUS_MIN_INTERVAL_MS}, anything else at most once per {@link #MIN_REWRITE_MS}.
   */
  static long writeDelay(ConvergenceStatus report, Written previous, long now) {
    if (previous == null) {
      return 0L;
    }
    ConvergenceStatus old = previous._report;
    if (report.semanticallyEquals(old)) {
      return -1L;
    }
    long interval =
        report.getStatus() != old.getStatus() ? STATUS_MIN_INTERVAL_MS : MIN_REWRITE_MS;
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

  /**
   * After a controller change, adopt what the previous leader wrote so unchanged reports are not
   * rewritten. This read happens once per session.
   */
  private void load(HelixDataAccessor accessor, PropertyKey.Builder keys, String session) {
    _written.clear();
    _cluster = null;
    ConvergenceStatus cluster = accessor.getProperty(keys.convergenceStatus());
    if (cluster != null) {
      List<String> names = accessor.getChildNames(keys.convergenceStatus());
      List<PropertyKey> childKeys = new ArrayList<>(names.size());
      names.forEach(name -> childKeys.add(keys.convergenceStatus(name)));
      List<ConvergenceStatus> reports = accessor.getProperty(childKeys);
      for (int i = 0; i < names.size(); i++) {
        if (reports.get(i) != null) {
          _written.put(names.get(i), new Written(reports.get(i), 0L));
        }
      }
    }
    _cluster = cluster == null ? null : new Written(cluster, 0L);
    _loadedSession = session;
  }

  private static boolean isEventSession(ClusterEvent event, String session) {
    Optional<String> expected = event.getAttribute(AttributeName.EVENT_SESSION.name());
    return expected == null || !expected.isPresent() || expected.get().equals(session);
  }

  private static boolean isCurrentLeader(ClusterEvent event, HelixManager manager) {
    return manager.isLeader() && isEventSession(event, manager.getSessionId());
  }
}
