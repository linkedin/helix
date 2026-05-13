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
import java.util.HashMap;
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
 * <p>A non-positive {@code expiresAtMillis} (the {@code -1} sentinel) means "clear the
 * marker." Clears skip cap enforcement and TTL resolution; they just remove the timestamp
 * and metadata.
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
   * Writes (or clears) planned-maintenance markers on the supplied instances atomically from
   * the caller's point of view: cap enforcement uses a single snapshot of current markers, and
   * either all writes succeed or the call throws.
   *
   * @param clusterId the cluster.
   * @param instanceNames instances to mark (non-empty, no duplicates).
   * @param callerExpiresAtMillis 0 = caller did not supply a value; positive = explicit expiry;
   *     negative = clear.
   * @param reason audit metadata (optional, ignored on clear).
   * @param source audit metadata (optional, ignored on clear).
   * @param nowMs current time, supplied by the caller for testability.
   * @return resolved effective expiry timestamp, or {@link InstanceConfig#PLANNED_MAINTENANCE_NOT_SET}
   *     for clears.
   */
  public long applyPlannedMaintenance(String clusterId, List<String> instanceNames,
      long callerExpiresAtMillis, String reason, String source, long nowMs) {
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
    List<String> deduped = new ArrayList<>(dedupedSet);

    assertInstancesExist(clusterId, deduped);

    boolean clear = callerExpiresAtMillis < 0L;
    if (clear) {
      for (String instanceName : deduped) {
        InstanceConfig cfg = loadInstanceConfig(clusterId, instanceName);
        cfg.setPlannedMaintenanceUntilMs(InstanceConfig.PLANNED_MAINTENANCE_NOT_SET);
        _configAccessor.setInstanceConfig(clusterId, instanceName, cfg);
      }
      LOG.info("Cleared planned-maintenance marker on {} instances in cluster {}",
          deduped.size(), clusterId);
      return InstanceConfig.PLANNED_MAINTENANCE_NOT_SET;
    }

    ClusterConfig clusterConfig = _configAccessor.getClusterConfig(clusterId);
    if (clusterConfig == null) {
      throw new BadRequestException("Cluster " + clusterId + " not found");
    }

    long effectiveExpiresAtMillis =
        resolveExpiresAtMillis(callerExpiresAtMillis, clusterConfig, nowMs);

    // NOTE: cap enforcement uses a snapshot of cluster-wide markers taken at this moment.
    // Concurrent writes from another caller could race past the cap; the design treats this
    // as acceptable because the cap is a safety rail, not a strict invariant, and a transient
    // overage cascades correctly into MM the moment a real outage arrives.
    enforceClusterCap(clusterId, clusterConfig, dedupedSet, nowMs);

    Map<String, String> metadata = buildMetadata(reason, source, nowMs);

    for (String instanceName : deduped) {
      InstanceConfig cfg = loadInstanceConfig(clusterId, instanceName);
      cfg.setPlannedMaintenanceUntilMs(effectiveExpiresAtMillis);
      if (!metadata.isEmpty()) {
        cfg.setPlannedMaintenanceMetadata(metadata);
      }
      _configAccessor.setInstanceConfig(clusterId, instanceName, cfg);
    }
    LOG.info("Wrote planned-maintenance marker on {} instances in cluster {}, expiresAtMillis={}",
        deduped.size(), clusterId, effectiveExpiresAtMillis);
    return effectiveExpiresAtMillis;
  }

  private InstanceConfig loadInstanceConfig(String clusterId, String instanceName) {
    InstanceConfig cfg = _admin.getInstanceConfig(clusterId, instanceName);
    if (cfg == null) {
      throw new BadRequestException(
          "Instance " + instanceName + " not found in cluster " + clusterId);
    }
    return cfg;
  }

  /**
   * Confirm every supplied instance exists in the cluster before mutating any of them. This
   * runs before TTL resolution and cap enforcement so that a bogus name does not inflate the
   * cap projection (one extra slot per nonexistent name) and does not leave already-mutated
   * instances behind on a partial-failure path.
   */
  private void assertInstancesExist(String clusterId, List<String> instanceNames) {
    for (String instanceName : instanceNames) {
      if (_admin.getInstanceConfig(clusterId, instanceName) == null) {
        throw new BadRequestException(
            "Instance " + instanceName + " not found in cluster " + clusterId);
      }
    }
  }

  /**
   * Apply the resolution rules documented in the class javadoc. Visible for testing.
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

  private void enforceClusterCap(String clusterId, ClusterConfig clusterConfig,
      Set<String> incomingInstances, long nowMs) {
    int absoluteCap = clusterConfig.getMaxPlannedMaintenanceInstances();
    int percentageCap = clusterConfig.getMaxPlannedMaintenancePercentage();
    if (absoluteCap < 0 && percentageCap < 0) {
      return;
    }

    List<String> allInstances = _admin.getInstancesInCluster(clusterId);
    if (allInstances == null || allInstances.isEmpty()) {
      return;
    }
    int currentlyMarked = 0;
    for (String name : allInstances) {
      // The incoming write will reset each incoming instance's marker; counting the
      // post-write state requires us to skip the pre-write value here and add the full batch
      // size below.
      if (incomingInstances.contains(name)) {
        continue;
      }
      InstanceConfig cfg = _admin.getInstanceConfig(clusterId, name);
      if (cfg != null && cfg.isUnderPlannedMaintenance(nowMs)) {
        currentlyMarked++;
      }
    }
    int projectedMarked = currentlyMarked + incomingInstances.size();

    int effectiveCap = Integer.MAX_VALUE;
    if (absoluteCap >= 0) {
      effectiveCap = Math.min(effectiveCap, absoluteCap);
    }
    if (percentageCap >= 0) {
      int byPercentage = (int) Math.floor((percentageCap * (long) allInstances.size()) / 100.0);
      effectiveCap = Math.min(effectiveCap, byPercentage);
    }
    if (projectedMarked > effectiveCap) {
      throw new BadRequestException(String.format(
          "Write would push planned-maintenance count to %d, exceeds cluster cap %d "
              + "(absoluteCap=%d, percentageCap=%d, clusterSize=%d)",
          projectedMarked, effectiveCap, absoluteCap, percentageCap, allInstances.size()));
    }
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

  /**
   * Unchecked exception signalling a 400-class write failure. Callers translate this to a
   * Response with status 400 and the message body.
   */
  public static class BadRequestException extends RuntimeException {
    public BadRequestException(String message) {
      super(message);
    }
  }
}
