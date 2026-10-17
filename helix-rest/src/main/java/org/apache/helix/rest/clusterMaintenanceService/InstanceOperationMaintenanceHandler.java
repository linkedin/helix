package org.apache.helix.rest.clusterMaintenanceService;

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

import java.time.Instant;
import java.util.ArrayList;
import java.util.EnumMap;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.UUID;
import java.util.stream.Collectors;

import com.fasterxml.jackson.annotation.JsonInclude;
import org.apache.helix.AccessOption;
import org.apache.helix.BaseDataAccessor;
import org.apache.helix.PropertyPathBuilder;
import org.apache.helix.manager.zk.ZkBaseDataAccessor;
import org.apache.helix.model.ClusterConfig;
import org.apache.helix.model.InstanceConfig;
import org.apache.helix.zookeeper.api.client.RealmAwareZkClient;
import org.apache.helix.zookeeper.datamodel.ZNRecord;
import org.apache.helix.zookeeper.zkclient.exception.ZkBadVersionException;
import org.apache.helix.zookeeper.zkclient.exception.ZkNoNodeException;
import org.apache.zookeeper.Op;
import org.apache.zookeeper.common.PathUtils;
import org.apache.zookeeper.data.Stat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Takes, renews or clears the instance-operation maintenance marker on a batch of instances, so
 * the marker cap fails closed: an instance the cap has no room for gets no marker. An instance
 * without a live marker takes one only while live markers are below the cap; renewing a live one
 * takes no new budget and never shortens it. With no cap configured, any number may take one.
 * {@link #CLEAR_MARKER} clears the marker.
 *
 * <p>A request commits every instance that passes every check, in one ZooKeeper multi: a versioned
 * write of each changed instance config, a version check of each instance left as is, and a
 * version check of the cluster config. A rejected instance is not written and does not hold back
 * the rest. If any of them changed since the read, the commit fails and the request is read and
 * judged again. The cap sees one request at a time, so it holds as long as every marker write
 * goes through this handler and each cluster has one caller that sends one request at a time, as
 * ACM does.
 *
 * <p>Every {@link Status} maps to one action for the caller, and every outcome carries a
 * one-line message that says what happened and what to do, written to be shown to an operator as
 * is.
 *
 * <p>A request that changes nothing writes nothing, so repeating a request is always safe.
 */
public class InstanceOperationMaintenanceHandler {
  private static final Logger LOG =
      LoggerFactory.getLogger(InstanceOperationMaintenanceHandler.class);

  private static final int MAX_QUOTED_LENGTH = 200;
  static final int MAX_ATTEMPTS = 5;
  /** Expiry that takes the cluster's default marker duration. */
  public static final long DEFAULT_EXPIRY = 0L;
  /** Expiry that clears the marker. */
  public static final long CLEAR_MARKER = -1L;
  /** Per-instance status, each with the action it asks of the caller. */
  public enum Status {
    /** Written. The message says what the instance's marker is now. */
    APPLIED,
    /** No such instance in the cluster. Drop it from the request. */
    INSTANCE_NOT_FOUND,
    /**
     * The marker cap has no room for this instance. Retry once markers are cleared or expire. A
     * cap of 0 admits none, and the message says it must be raised.
     */
    BUDGET_EXHAUSTED,
    /** Lost the race to concurrent writers on every attempt. Resend. */
    CONFLICT
  }

  /**
   * Per-instance result. Every {@link Status#APPLIED} instance was written in one commit, and no
   * other instance was. Every outcome carries a message, written to be shown to an operator as is.
   */
  @JsonInclude(JsonInclude.Include.NON_NULL)
  public static final class Outcome {
    private final Status _status;
    private final String _message;
    private final Long _expiresAtMillis;

    Outcome(Status status, String message) {
      this(status, message, null);
    }

    private Outcome(Status status, String message, Long expiresAtMillis) {
      _status = status;
      _message = message;
      _expiresAtMillis = expiresAtMillis;
    }

    public Status getStatus() {
      return _status;
    }

    public String getMessage() {
      return _message;
    }

    /** On APPLIED, the live marker's expiry after the request; null when it has no live marker. */
    public Long getExpiresAtMillis() {
      return _expiresAtMillis;
    }

    @Override
    public String toString() {
      return _status + (_message == null ? "" : ": " + _message)
          + (_expiresAtMillis == null ? "" : " until " + _expiresAtMillis);
    }
  }

  /** A request that is invalid as a whole. Nothing was written. */
  public static class BadRequestException extends RuntimeException {
    public BadRequestException(String message) {
      super(message);
    }
  }

  /** The marker cap and the live markers it counts against. */
  private static final class MarkerCap {
    private static final MarkerCap UNLIMITED = new MarkerCap(Integer.MAX_VALUE, 0L, null);
    private final int _cap;
    private final long _live;
    private final String _label;

    private MarkerCap(int cap, long live, String label) {
      _cap = cap;
      _live = live;
      _label = label;
    }

    int room() {
      return (int) Math.max(0L, _cap - _live);
    }

    String exhausted() {
      if (_cap == 0) {
        return "The maintenance marker cap is " + _label + ", so no instance can take a marker. "
            + "Raise the cap to allow any.";
      }
      if (room() == 0) {
        return String.format("The maintenance marker cap of %s is full with %s. Retry once "
            + "markers are cleared or expire.", _label, plural(_live, "live marker"));
      }
      return String.format("The maintenance marker cap of %s had room for %s (%d live), and "
              + "instances earlier in this request used it up. Retry once markers are cleared or "
              + "expire.",
          _label, plural(room(), "new marker"), _live);
    }
  }

  private final RealmAwareZkClient _zkClient;
  private final BaseDataAccessor<ZNRecord> _accessor;

  public InstanceOperationMaintenanceHandler(RealmAwareZkClient zkClient) {
    _zkClient = Objects.requireNonNull(zkClient, "zkClient");
    _accessor = new ZkBaseDataAccessor<>(zkClient);
  }

  /**
   * Applies the request to every instance in one commit, or to none. The cap admits new markers
   * in input order.
   *
   * @param expiresAtMillis marker expiry: a future epoch time, {@link #DEFAULT_EXPIRY}, or
   *     {@link #CLEAR_MARKER}.
   * @throws BadRequestException if the instance list is empty or has a name that is not a valid
   *     znode name, the cluster does not exist, or the expiry is in the past or the default with no
   *     positive cluster default.
   */
  public Map<String, Outcome> apply(String clusterId, List<String> instances,
      long expiresAtMillis) {
    if (instances == null || instances.isEmpty()) {
      throw new BadRequestException("instances must be a non-empty list of instance names.");
    }
    for (int i = 0; i < instances.size(); i++) {
      String name = instances.get(i);
      if (name == null || name.isEmpty() || name.contains("/") || !isZnodeName(name)) {
        throw new BadRequestException("instances[" + i + "] is "
            + (name == null ? "null" : "\"" + clip(name, MAX_QUOTED_LENGTH) + "\"")
            + ", which is not an instance name.");
      }
    }
    ZNRecord clusterRecord = _accessor.get(PropertyPathBuilder.clusterConfig(clusterId), null,
        AccessOption.PERSISTENT);
    if (clusterRecord == null) {
      throw new BadRequestException("Cluster " + clusterId + " does not exist.");
    }
    long expiresAt = expiresAtMillis;
    if (expiresAtMillis == CLEAR_MARKER) {
      expiresAt = InstanceConfig.INSTANCE_OPERATION_MAINTENANCE_NOT_SET;
    } else if (expiresAtMillis == DEFAULT_EXPIRY) {
      long defaultDurationMs = new ClusterConfig(clusterRecord)
          .getDefaultInstanceOperationMaintenanceDurationMs();
      if (defaultDurationMs <= 0L) {
        throw new BadRequestException("expiresAtMillis is missing or 0, which asks for the "
            + "cluster default, but cluster " + clusterId + " has no positive "
            + "DEFAULT_INSTANCE_OPERATION_MAINTENANCE_DURATION_MS. Give an expiry, or set the "
            + "cluster default.");
      }
      expiresAt = System.currentTimeMillis() + defaultDurationMs;
    } else if (expiresAtMillis < 0L) {
      throw new BadRequestException("expiresAtMillis " + expiresAtMillis + " is negative. Give a "
          + "future epoch time in milliseconds, 0 for the cluster default, or -1 to clear the "
          + "marker.");
    } else if (expiresAtMillis <= System.currentTimeMillis()) {
      throw new BadRequestException("expiresAtMillis " + expiresAtMillis + " ("
          + Instant.ofEpochMilli(expiresAtMillis) + ") is not in the future. Give a later expiry.");
    }

    List<String> names = new ArrayList<>(new LinkedHashSet<>(instances));
    String requestId = UUID.randomUUID().toString();
    Map<String, Outcome> outcomes = commit(clusterId, names, expiresAt, requestId);
    LOG.info("Instance operation maintenance request {} in cluster {}, marker expiry {}: {} {}",
        requestId, clusterId, expiresAt > 0L ? Instant.ofEpochMilli(expiresAt) : "clear",
        summarize(outcomes), outcomes);
    return outcomes;
  }

  private Map<String, Outcome> commit(String clusterId, List<String> instances,
      long expiresAt, String requestId) {
    for (int attempt = 1; attempt <= MAX_ATTEMPTS; attempt++) {
      Map<String, Outcome> outcomes = tryCommit(clusterId, instances, expiresAt);
      if (outcomes != null) {
        return outcomes;
      }
      LOG.info("Request {} in cluster {} lost a race on attempt {} of {}; re-reading.",
          requestId, clusterId, attempt, MAX_ATTEMPTS);
    }
    return all(instances, new Outcome(Status.CONFLICT, "Concurrent writes to this cluster won all "
        + MAX_ATTEMPTS + " attempts. Resend the request."));
  }

  /** Reads, judges and commits the request once. Returns null when the commit lost a race. */
  private Map<String, Outcome> tryCommit(String clusterId, List<String> instances,
      long expiresAt) {
    List<String> paths = instances.stream()
        .map(instance -> PropertyPathBuilder.instanceConfig(clusterId, instance))
        .collect(Collectors.toList());
    String clusterConfigPath = PropertyPathBuilder.clusterConfig(clusterId);
    Stat clusterStat = new Stat();
    ZNRecord clusterRecord = _accessor.get(clusterConfigPath, clusterStat,
        AccessOption.PERSISTENT);
    List<Stat> stats = new ArrayList<>();
    List<ZNRecord> records = _accessor.get(paths, stats, AccessOption.PERSISTENT, true);
    if (clusterRecord == null) {
      throw new BadRequestException("Cluster " + clusterId + " does not exist.");
    }
    ClusterConfig clusterConfig = new ClusterConfig(clusterRecord);
    long nowMs = System.currentTimeMillis();
    MarkerCap cap = null;
    int newMarkers = 0;
    Map<String, Outcome> outcomes = new LinkedHashMap<>();
    List<Op> ops = new ArrayList<>();
    List<Op> checks = new ArrayList<>();
    for (int i = 0; i < instances.size(); i++) {
      String instance = instances.get(i);
      if (records.get(i) == null) {
        outcomes.put(instance, new Outcome(Status.INSTANCE_NOT_FOUND,
            "No such instance in cluster " + clusterId + ". Remove it from the request."));
        continue;
      }
      InstanceConfig config = new InstanceConfig(records.get(i));
      long before = config.getInstanceOperationMaintenanceUntilMs();
      boolean wasLive = config.isUnderInstanceOperationMaintenance(nowMs);
      if (expiresAt > 0L && !wasLive) {
        if (cap == null) {
          cap = markerCap(clusterId, clusterConfig, nowMs);
        }
        if (newMarkers == cap.room()) {
          outcomes.put(instance, new Outcome(Status.BUDGET_EXHAUSTED, cap.exhausted()));
          continue;
        }
        newMarkers++;
      }
      if (expiresAt > 0L) {
        config.setInstanceOperationMaintenanceUntilMs(Math.max(expiresAt, before));
      } else {
        config.setInstanceOperationMaintenanceUntilMs(expiresAt);
      }
      long until = config.getInstanceOperationMaintenanceUntilMs();
      outcomes.put(instance, new Outcome(Status.APPLIED,
          applied(config, expiresAt, before, wasLive),
          config.isUnderInstanceOperationMaintenance(nowMs) ? until : null));
      if (until != before) {
        ops.add(Op.setData(paths.get(i), _zkClient.serialize(config.getRecord(), paths.get(i)),
            stats.get(i).getVersion()));
      } else {
        // Held as is, so the commit fails if it changed since it was judged.
        checks.add(Op.check(paths.get(i), stats.get(i).getVersion()));
      }
    }

    if (ops.isEmpty()) {
      return outcomes;
    }
    ops.addAll(checks);
    ops.add(Op.check(clusterConfigPath, clusterStat.getVersion()));
    try {
      _zkClient.multi(ops);
      return outcomes;
    } catch (ZkBadVersionException | ZkNoNodeException e) {
      return null;
    }
  }

  /** One line for the whole request, to show next to the per-instance outcomes. */
  public static String summarize(Map<String, Outcome> outcomes) {
    Map<Status, Integer> counts = new EnumMap<>(Status.class);
    outcomes.values().forEach(outcome -> counts.merge(outcome.getStatus(), 1, Integer::sum));
    int total = outcomes.size();
    int applied = counts.getOrDefault(Status.APPLIED, 0);
    if (applied == total) {
      return "Applied to " + (total == 1 ? "the instance." : "all " + total + " instances.");
    }
    if (counts.containsKey(Status.CONFLICT)) {
      // A failure of the whole request, so every instance carries the same message.
      return outcomes.values().iterator().next().getMessage();
    }
    return String.format("Applied to %d of %s (%s).", applied, plural(total, "instance"),
        counts.entrySet().stream().filter(e -> e.getKey() != Status.APPLIED)
            .map(e -> e.getValue() + " " + e.getKey()).collect(Collectors.joining(", ")));
  }

  /** What an applied request left on the instance's marker. */
  private static String applied(InstanceConfig config, long expiresAt, long before,
      boolean wasLive) {
    long until = config.getInstanceOperationMaintenanceUntilMs();
    String expiry = String.valueOf(Instant.ofEpochMilli(until));
    if (expiresAt > 0L) {
      return !wasLive ? "Took a maintenance marker until " + expiry + "."
          : until > before ? "Renewed the maintenance marker until " + expiry + "."
              : "The maintenance marker already runs until " + expiry
                  + (until > expiresAt ? ", past the requested expiry." : ".");
    }
    return before > 0L ? "Cleared the maintenance marker."
        : "It had no maintenance marker to clear.";
  }

  /** Bounds text this handler quotes but does not write, so a message stays one short line. */
  private static String clip(String text, int max) {
    if (text.length() <= max) {
      return text;
    }
    int end = max - 3;
    if (Character.isHighSurrogate(text.charAt(end - 1))) {
      end--;
    }
    return text.substring(0, end) + "...";
  }

  private static boolean isZnodeName(String name) {
    try {
      PathUtils.validatePath("/" + name);
      return true;
    } catch (IllegalArgumentException e) {
      return false;
    }
  }

  private static String plural(long count, String noun) {
    return count + " " + noun + (count == 1 ? "" : "s");
  }

  /** The marker cap and its live markers; unlimited when no cap is set. */
  private MarkerCap markerCap(String clusterId, ClusterConfig clusterConfig, long nowMs) {
    int absoluteCap = clusterConfig.getInstanceOperationMaintenanceBudget();
    int percentageCap = clusterConfig.getInstanceOperationMaintenanceBudgetPercentage();
    if (absoluteCap < 0 && percentageCap < 0) {
      return MarkerCap.UNLIMITED;
    }
    // Fails rather than under-count when a child cannot be read.
    List<ZNRecord> records = _accessor.getChildren(PropertyPathBuilder.instanceConfig(clusterId),
        null, AccessOption.PERSISTENT, 3, 100);
    int cap = absoluteCap >= 0 ? absoluteCap : (int) (percentageCap * (long) records.size() / 100);
    long live = records.stream().filter(
        r -> r != null && new InstanceConfig(r).isUnderInstanceOperationMaintenance(nowMs)).count();
    String label = absoluteCap >= 0 ? String.valueOf(cap)
        : cap + " (" + percentageCap + "% of " + plural(records.size(), "instance") + ")";
    return new MarkerCap(cap, live, label);
  }

  private static Map<String, Outcome> all(List<String> instances, Outcome outcome) {
    Map<String, Outcome> outcomes = new LinkedHashMap<>();
    instances.forEach(instance -> outcomes.put(instance, outcome));
    return outcomes;
  }
}
