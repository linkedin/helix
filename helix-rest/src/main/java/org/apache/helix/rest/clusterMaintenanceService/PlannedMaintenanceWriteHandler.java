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

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

import org.apache.helix.ConfigAccessor;
import org.apache.helix.HelixAdmin;
import org.apache.helix.model.ClusterConfig;
import org.apache.helix.model.InstanceConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Shared write path for the planned-maintenance budget exemption feature. Used by both the
 * per-instance and batch REST endpoints to keep TTL resolution, cap enforcement, and
 * InstanceConfig mutation in a single place.
 *
 * <p>Resolution rules for the effective expiry timestamp on each write:
 * <ol>
 *   <li>If the caller supplied a positive {@code expiresAtMillis}, use it.
 *   <li>Else if the cluster has {@code DEFAULT_PLANNED_MAINTENANCE_DURATION_MS} configured,
 *       use {@code now + duration}.
 *   <li>Else fail with {@link BadRequestException}.
 * </ol>
 *
 * <p>A negative {@code expiresAtMillis} (the {@code -1} sentinel) means "clear the marker."
 * Clears skip cap enforcement and TTL resolution; they just remove the timestamp and metadata.
 *
 * <p>Semantics mirror the batch stoppable check: per-instance failures (missing instance, cap
 * exceeded for the candidate's position in the input order) do not abort the call. They are
 * reported through {@link PlannedMaintenanceResult#getRejected()} so the caller can decide
 * how to handle them. Caller-side bugs that invalidate the entire request (empty input list,
 * missing/past expiry with no cluster default, missing cluster) still surface as
 * {@link BadRequestException}.
 */
public class PlannedMaintenanceWriteHandler {
  private static final Logger LOG =
      LoggerFactory.getLogger(PlannedMaintenanceWriteHandler.class);

  /** Sentinel value for the request body indicating "no expiry supplied by caller." */
  public static final long EXPIRES_AT_MILLIS_UNSET = 0L;

  /** Sentinel value for the request body indicating "clear the marker." */
  public static final long EXPIRES_AT_MILLIS_CLEAR = -1L;

  private final HelixAdmin _admin;
  private final ConfigAccessor _configAccessor;

  public PlannedMaintenanceWriteHandler(HelixAdmin admin, ConfigAccessor configAccessor) {
    _admin = Objects.requireNonNull(admin, "admin");
    _configAccessor = Objects.requireNonNull(configAccessor, "configAccessor");
  }

  /**
   * Writes (or clears) planned-maintenance markers on the supplied instances. The cap snapshot
   * is read once and instances are processed in input order; instances that fit the remaining
   * quota are written, the rest land in {@link PlannedMaintenanceResult#getRejected()}.
   *
   * @param clusterId the cluster.
   * @param instanceNames instances to mark (caller may supply duplicates; they are deduped in
   *     input order).
   * @param callerExpiresAtMillis 0 = caller did not supply a value; positive = explicit expiry;
   *     negative = clear.
   * @param reason audit metadata (optional, ignored on clear).
   * @param source audit metadata (optional, ignored on clear).
   * @param nowMs current time, supplied by the caller for testability.
   * @return a {@link PlannedMaintenanceResult} listing the applied instances, the rejected
   *     instances with reasons, and the resolved effective expiry.
   */
  public PlannedMaintenanceResult applyPlannedMaintenance(String clusterId,
      List<String> instanceNames, long callerExpiresAtMillis, String reason, String source,
      long nowMs) {
    List<String> deduped = dedup(instanceNames);

    boolean clear = callerExpiresAtMillis < 0L;
    if (clear) {
      return applyClear(clusterId, deduped);
    }

    ClusterConfig clusterConfig = _configAccessor.getClusterConfig(clusterId);
    if (clusterConfig == null) {
      throw new BadRequestException("Cluster " + clusterId + " not found");
    }
    long effectiveExpiresAtMillis =
        resolveExpiresAtMillis(callerExpiresAtMillis, clusterConfig, nowMs);
    Map<String, String> metadata = buildMetadata(reason, source, nowMs);
    return applySet(clusterId, clusterConfig, deduped, effectiveExpiresAtMillis, metadata, nowMs);
  }

  // ---- Clear path -------------------------------------------------------------------------

  private PlannedMaintenanceResult applyClear(String clusterId, List<String> deduped) {
    Set<String> clusterInstances = loadClusterInstances(clusterId);
    List<String> applied = new ArrayList<>(deduped.size());
    Map<String, String> rejected = new LinkedHashMap<>();

    for (String instanceName : deduped) {
      if (!clusterInstances.contains(instanceName)) {
        rejected.put(instanceName, instanceNotFound(clusterId, instanceName));
        continue;
      }
      InstanceConfig cfg = _admin.getInstanceConfig(clusterId, instanceName);
      cfg.setPlannedMaintenanceUntilMs(InstanceConfig.PLANNED_MAINTENANCE_NOT_SET);
      _configAccessor.setInstanceConfig(clusterId, instanceName, cfg);
      applied.add(instanceName);
    }
    LOG.info("Cleared planned-maintenance marker: cluster={}, applied={}, rejected={}",
        clusterId, applied.size(), rejected.size());
    return new PlannedMaintenanceResult(applied, rejected,
        InstanceConfig.PLANNED_MAINTENANCE_NOT_SET);
  }

  // ---- Set path ---------------------------------------------------------------------------

  private PlannedMaintenanceResult applySet(String clusterId, ClusterConfig clusterConfig,
      List<String> deduped, long effectiveExpiresAtMillis, Map<String, String> metadata,
      long nowMs) {
    Set<String> clusterInstances = loadClusterInstances(clusterId);
    Map<String, String> rejected = new LinkedHashMap<>();

    // First pass: classify input as existing-candidate vs missing-instance. Missing instances
    // are recorded against rejected and dropped from further processing; the remaining
    // candidates compete for the cap quota in input order.
    List<String> candidates = new ArrayList<>(deduped.size());
    for (String instanceName : deduped) {
      if (clusterInstances.contains(instanceName)) {
        candidates.add(instanceName);
      } else {
        rejected.put(instanceName, instanceNotFound(clusterId, instanceName));
      }
    }

    // Snapshot the cap quota that's actually available for this batch. NOTE: the snapshot is
    // a single read; concurrent writes from another caller could race past the cap. The
    // design accepts this because the cap is a safety rail (not a strict invariant) and a
    // transient overage cascades correctly into MM the moment a real outage arrives.
    int remainingQuota =
        computeRemainingQuota(clusterId, clusterConfig, clusterInstances, candidates, nowMs);

    List<String> applied = new ArrayList<>(candidates.size());
    String capRejectMessage = capRejectMessage(clusterConfig);
    for (String instanceName : candidates) {
      if (remainingQuota <= 0) {
        rejected.put(instanceName, capRejectMessage);
        continue;
      }
      InstanceConfig cfg = _admin.getInstanceConfig(clusterId, instanceName);
      cfg.setPlannedMaintenanceUntilMs(effectiveExpiresAtMillis);
      if (!metadata.isEmpty()) {
        cfg.setPlannedMaintenanceMetadata(metadata);
      }
      _configAccessor.setInstanceConfig(clusterId, instanceName, cfg);
      applied.add(instanceName);
      remainingQuota--;
    }
    LOG.info("Wrote planned-maintenance markers: cluster={}, applied={}, rejected={}, "
            + "expiresAtMillis={}", clusterId, applied.size(), rejected.size(),
        effectiveExpiresAtMillis);
    return new PlannedMaintenanceResult(applied, rejected, effectiveExpiresAtMillis);
  }

  /**
   * Returns the set of instances currently registered in the cluster. Used for existence
   * checks; HelixAdmin#getInstanceConfig throws on missing instances rather than returning
   * null, so we check membership against this set instead of catching exceptions per-instance.
   */
  private Set<String> loadClusterInstances(String clusterId) {
    List<String> all = _admin.getInstancesInCluster(clusterId);
    return all == null ? Collections.emptySet() : new HashSet<>(all);
  }

  // ---- Helpers ----------------------------------------------------------------------------

  private static List<String> dedup(List<String> instanceNames) {
    if (instanceNames == null || instanceNames.isEmpty()) {
      throw new BadRequestException("instanceNames must not be empty");
    }
    Set<String> dedupedSet = new LinkedHashSet<>(instanceNames.size());
    for (String name : instanceNames) {
      if (name == null || name.isEmpty()) {
        throw new BadRequestException("instanceNames contains an empty value");
      }
      dedupedSet.add(name);
    }
    return new ArrayList<>(dedupedSet);
  }

  private static String instanceNotFound(String clusterId, String instanceName) {
    return "instance not found in cluster " + clusterId;
  }

  /**
   * Apply the TTL resolution rules documented in the class javadoc. Visible for testing.
   */
  static long resolveExpiresAtMillis(long callerExpiresAtMillis, ClusterConfig clusterConfig,
      long nowMs) {
    if (callerExpiresAtMillis > 0L) {
      if (callerExpiresAtMillis <= nowMs) {
        throw new BadRequestException(
            "expiresAtMillis must be strictly in the future (got " + callerExpiresAtMillis
                + ", now=" + nowMs + ")");
      }
      return callerExpiresAtMillis;
    }
    // At this point the caller did not supply a positive expiry. Fall back to the cluster
    // default only if the operator configured one. The getter returns the sentinel -1L when
    // the field is absent, which is the single "feature off" signal here; we compare against
    // it directly so the intent is explicit.
    long defaultDuration = clusterConfig.getDefaultPlannedMaintenanceDurationMs();
    if (defaultDuration != -1L) {
      return nowMs + defaultDuration;
    }
    throw new BadRequestException(
        "expiresAtMillis not supplied and cluster has no DEFAULT_PLANNED_MAINTENANCE_DURATION_MS"
            + " configured");
  }

  /**
   * Returns the number of new markers this batch may apply before hitting the cluster cap.
   * Markers already held by instances that are <b>not</b> in this batch count against the
   * quota; markers on instances within the batch do not (they will be rewritten as part of
   * the same call).
   *
   * <p>Returns {@link Integer#MAX_VALUE} when no cap is configured or the cluster has no
   * instances.
   */
  private int computeRemainingQuota(String clusterId, ClusterConfig clusterConfig,
      Set<String> clusterInstances, List<String> candidates, long nowMs) {
    int absoluteCap = clusterConfig.getMaxPlannedMaintenanceInstances();
    int percentageCap = clusterConfig.getMaxPlannedMaintenancePercentage();
    if (absoluteCap < 0 && percentageCap < 0) {
      return Integer.MAX_VALUE;
    }
    if (clusterInstances.isEmpty()) {
      return Integer.MAX_VALUE;
    }

    Set<String> candidateSet = new HashSet<>(candidates);
    int markedByOthers = 0;
    for (String name : clusterInstances) {
      if (candidateSet.contains(name)) {
        continue;
      }
      InstanceConfig cfg = _admin.getInstanceConfig(clusterId, name);
      if (cfg != null && cfg.isUnderPlannedMaintenance(nowMs)) {
        markedByOthers++;
      }
    }

    int effectiveCap = Integer.MAX_VALUE;
    if (absoluteCap >= 0) {
      effectiveCap = Math.min(effectiveCap, absoluteCap);
    }
    if (percentageCap >= 0) {
      int byPercentage =
          (int) Math.floor((percentageCap * (long) clusterInstances.size()) / 100.0);
      effectiveCap = Math.min(effectiveCap, byPercentage);
    }
    return Math.max(0, effectiveCap - markedByOthers);
  }

  private static String capRejectMessage(ClusterConfig clusterConfig) {
    int absoluteCap = clusterConfig.getMaxPlannedMaintenanceInstances();
    int percentageCap = clusterConfig.getMaxPlannedMaintenancePercentage();
    if (absoluteCap >= 0 && percentageCap >= 0) {
      return String.format(
          "would exceed planned-maintenance cap (MAX_PLANNED_MAINTENANCE_INSTANCES=%d, "
              + "MAX_PLANNED_MAINTENANCE_PERCENTAGE=%d)",
          absoluteCap, percentageCap);
    }
    if (absoluteCap >= 0) {
      return "would exceed MAX_PLANNED_MAINTENANCE_INSTANCES=" + absoluteCap;
    }
    return "would exceed MAX_PLANNED_MAINTENANCE_PERCENTAGE=" + percentageCap;
  }

  private static Map<String, String> buildMetadata(String reason, String source, long nowMs) {
    Map<String, String> metadata = new HashMap<>();
    if (reason != null && !reason.isEmpty()) {
      metadata.put(InstanceConfig.PlannedMaintenanceMetadataKey.REASON, reason);
    }
    if (source != null && !source.isEmpty()) {
      metadata.put(InstanceConfig.PlannedMaintenanceMetadataKey.SOURCE, source);
    }
    if (!metadata.isEmpty()) {
      metadata.put(InstanceConfig.PlannedMaintenanceMetadataKey.SET_AT_MS, Long.toString(nowMs));
    }
    return metadata;
  }

  // ---- Types ------------------------------------------------------------------------------

  /**
   * Outcome of one {@link #applyPlannedMaintenance} invocation. {@link #getApplied()} is
   * always a list (possibly empty); {@link #getRejected()} is always a map (possibly empty)
   * keyed by instance name with a free-form reason as the value. {@link #getResolvedExpiresAtMillis()}
   * is the server-resolved expiry, or {@link InstanceConfig#PLANNED_MAINTENANCE_NOT_SET} for
   * clear operations.
   */
  public static final class PlannedMaintenanceResult {
    private final List<String> _applied;
    private final Map<String, String> _rejected;
    private final long _resolvedExpiresAtMillis;

    PlannedMaintenanceResult(List<String> applied, Map<String, String> rejected,
        long resolvedExpiresAtMillis) {
      _applied = Collections.unmodifiableList(applied);
      _rejected = Collections.unmodifiableMap(rejected);
      _resolvedExpiresAtMillis = resolvedExpiresAtMillis;
    }

    public List<String> getApplied() {
      return _applied;
    }

    public Map<String, String> getRejected() {
      return _rejected;
    }

    public long getResolvedExpiresAtMillis() {
      return _resolvedExpiresAtMillis;
    }
  }

  /**
   * Unchecked exception signalling a 400-class write failure for caller-side bugs that
   * invalidate the entire request (empty input, malformed expiry, missing cluster default).
   * Per-instance failures (missing instance, cap overflow) do <b>not</b> throw; they are
   * reported via {@link PlannedMaintenanceResult#getRejected()} instead.
   */
  public static class BadRequestException extends RuntimeException {
    public BadRequestException(String message) {
      super(message);
    }
  }
}
