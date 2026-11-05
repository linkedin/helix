package org.apache.helix.model;

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

import java.util.Arrays;
import java.util.Collections;
import java.util.EnumMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;

import org.apache.helix.HelixProperty;
import org.apache.helix.zookeeper.datamodel.ZNRecord;

/**
 * An aggregate snapshot of whether the current assignment has reached the target assignment, for
 * one resource or for the whole cluster: an overall status and reason plus the number of partitions
 * per status and per reason. The record size is constant, whatever the number of partitions.
 */
public class ConvergenceStatus extends HelixProperty {
  public static final int SCHEMA_VERSION = 1;

  private static final String STATUS_COUNTS = "STATUS_COUNTS";
  private static final String REASON_COUNTS = "REASON_COUNTS";
  // When and by whom a report was computed, as opposed to what it says.
  private static final Set<String> PROVENANCE_FIELDS = new HashSet<>(Arrays.asList(
      Field.GENERATED_AT.name(), Field.UNCONVERGED_SINCE.name(),
      Field.CONTROLLER_SESSION_ID.name(), Field.CONTROLLER_NAME.name()));

  public enum Status {
    CONVERGED,
    IN_PROGRESS,
    BLOCKED,
    UNKNOWN,
    PAUSED
  }

  public enum Reason {
    NONE,
    PENDING_TRANSITION,
    CANCELLATION_PENDING,
    RELAY_PENDING,
    TRANSITION_DISPATCHED,
    MESSAGE_THROTTLED,
    STATE_CONSTRAINT_WAIT,
    MESSAGE_DISPATCH_FAILED,
    ERROR_STATE,
    TARGET_INSTANCE_NOT_LIVE,
    NO_PROGRESS_PATH,
    TARGET_ASSIGNMENT_MISSING,
    INVALID_STATE,
    MAINTENANCE_MODE,
    MANAGEMENT_MODE,
    WAITING_FOR_DELAY,
    WAGED_LAST_KNOWN_GOOD,
    WAGED_CAPACITY_DEFICIT,
    WAGED_NO_CANDIDATE_NODE,
    WAGED_INVALID_CONFIGURATION,
    WAGED_INTERNAL_FAILURE
  }

  /** Whether the target assignment came from a fresh computation. */
  public enum TargetFreshness {
    CURRENT,
    LAST_KNOWN_GOOD,
    UNKNOWN
  }

  public enum OptimizerStatus {
    NOT_APPLICABLE,
    HEALTHY,
    BASELINE_FAILED,
    UNKNOWN
  }

  public enum Field {
    SCHEMA_VERSION,
    STATUS,
    PRIMARY_REASON,
    GENERATED_AT,
    UNCONVERGED_SINCE,
    CONTROLLER_SESSION_ID,
    CONTROLLER_NAME,
    RESOURCE_COUNT,
    TARGET_FRESHNESS,
    OPTIMIZER_STATUS
  }

  public ConvergenceStatus(String id) {
    super(id);
    _record.setIntField(Field.SCHEMA_VERSION.name(), SCHEMA_VERSION);
  }

  public ConvergenceStatus(ZNRecord record) {
    super(record);
  }

  public int getSchemaVersion() {
    return _record.getIntField(Field.SCHEMA_VERSION.name(), 0);
  }

  public void setStatus(Status status) {
    _record.setEnumField(Field.STATUS.name(), status);
  }

  public Status getStatus() {
    return _record.getEnumField(Field.STATUS.name(), Status.class, Status.UNKNOWN);
  }

  public void setPrimaryReason(Reason reason) {
    _record.setEnumField(Field.PRIMARY_REASON.name(), reason);
  }

  public Reason getPrimaryReason() {
    return _record.getEnumField(Field.PRIMARY_REASON.name(), Reason.class, Reason.NONE);
  }

  public void setGeneratedAt(long timestamp) {
    _record.setLongField(Field.GENERATED_AT.name(), timestamp);
  }

  public long getGeneratedAt() {
    return _record.getLongField(Field.GENERATED_AT.name(), 0L);
  }

  /** When this scope last stopped being converged, or 0 while it is converged. */
  public void setUnconvergedSince(long timestamp) {
    _record.setLongField(Field.UNCONVERGED_SINCE.name(), timestamp);
  }

  public long getUnconvergedSince() {
    return _record.getLongField(Field.UNCONVERGED_SINCE.name(), 0L);
  }

  /**
   * The Helix session of the controller that wrote this record. The cluster record's session is
   * what readers check against the leader before trusting any report.
   */
  public void setControllerSessionId(String sessionId) {
    _record.setSimpleField(Field.CONTROLLER_SESSION_ID.name(), sessionId);
  }

  public String getControllerSessionId() {
    return _record.getSimpleField(Field.CONTROLLER_SESSION_ID.name());
  }

  /** The instance name of the controller that published this record. */
  public void setControllerName(String name) {
    _record.setSimpleField(Field.CONTROLLER_NAME.name(), name);
  }

  public String getControllerName() {
    return _record.getSimpleField(Field.CONTROLLER_NAME.name());
  }

  /** Number of resources covered. Set on the cluster record only. */
  public void setResourceCount(int count) {
    _record.setIntField(Field.RESOURCE_COUNT.name(), Math.max(0, count));
  }

  public int getResourceCount() {
    return _record.getIntField(Field.RESOURCE_COUNT.name(), 0);
  }

  public void setTargetFreshness(TargetFreshness freshness) {
    _record.setEnumField(Field.TARGET_FRESHNESS.name(), freshness);
  }

  public TargetFreshness getTargetFreshness() {
    return _record.getEnumField(Field.TARGET_FRESHNESS.name(), TargetFreshness.class,
        TargetFreshness.UNKNOWN);
  }

  public void setOptimizerStatus(OptimizerStatus status) {
    _record.setEnumField(Field.OPTIMIZER_STATUS.name(), status);
  }

  public OptimizerStatus getOptimizerStatus() {
    return _record.getEnumField(Field.OPTIMIZER_STATUS.name(), OptimizerStatus.class,
        OptimizerStatus.UNKNOWN);
  }

  public void setStatusCounts(Map<Status, Integer> counts) {
    _record.setMapField(STATUS_COUNTS, toStringMap(counts));
  }

  public Map<Status, Integer> getStatusCounts() {
    return fromStringMap(_record.getMapField(STATUS_COUNTS), Status.class);
  }

  public void setReasonCounts(Map<Reason, Integer> counts) {
    _record.setMapField(REASON_COUNTS, toStringMap(counts));
  }

  public Map<Reason, Integer> getReasonCounts() {
    return fromStringMap(_record.getMapField(REASON_COUNTS), Reason.class);
  }

  public int getPartitionCount() {
    return getStatusCounts().values().stream().mapToInt(Integer::intValue).sum();
  }

  /** Partitions that are neither converged nor paused. */
  public int getAffectedPartitionCount() {
    Map<Status, Integer> counts = getStatusCounts();
    return counts.getOrDefault(Status.IN_PROGRESS, 0) + counts.getOrDefault(Status.BLOCKED, 0)
        + counts.getOrDefault(Status.UNKNOWN, 0);
  }

  /**
   * Same content, ignoring when and by which controller it was computed. Runs for every resource
   * on every calculation, so it compares in place without copying.
   */
  public boolean semanticallyEquals(ConvergenceStatus other) {
    if (other == null) {
      return false;
    }
    ZNRecord right = other.getRecord();
    return sameSimpleFields(_record.getSimpleFields(), right.getSimpleFields())
        && _record.getMapFields().equals(right.getMapFields())
        && _record.getListFields().equals(right.getListFields());
  }

  private static boolean sameSimpleFields(Map<String, String> left, Map<String, String> right) {
    if (left.size() - provenanceFieldCount(left) != right.size() - provenanceFieldCount(right)) {
      return false;
    }
    for (Map.Entry<String, String> entry : left.entrySet()) {
      String key = entry.getKey();
      if (PROVENANCE_FIELDS.contains(key)) {
        continue;
      }
      String value = entry.getValue();
      if (value == null ? right.get(key) != null || !right.containsKey(key)
          : !value.equals(right.get(key))) {
        return false;
      }
    }
    return true;
  }

  private static int provenanceFieldCount(Map<String, String> fields) {
    int count = 0;
    for (String field : PROVENANCE_FIELDS) {
      if (fields.containsKey(field)) {
        count++;
      }
    }
    return count;
  }

  @Override
  public boolean isValid() {
    return getSchemaVersion() == SCHEMA_VERSION;
  }

  private static <E extends Enum<E>> Map<String, String> toStringMap(Map<E, Integer> counts) {
    Map<String, String> result = new TreeMap<>();
    if (counts != null) {
      counts.forEach((key, value) -> result.put(key.name(), String.valueOf(value)));
    }
    return result;
  }

  private static <E extends Enum<E>> Map<E, Integer> fromStringMap(Map<String, String> input,
      Class<E> type) {
    Map<E, Integer> result = new EnumMap<>(type);
    if (input != null) {
      input.forEach((key, value) -> {
        E parsed = parse(type, key, null);
        if (parsed != null) {
          try {
            result.put(parsed, Integer.parseInt(value));
          } catch (NumberFormatException ignored) {
            // A malformed count is dropped rather than failing the whole report.
          }
        }
      });
    }
    return Collections.unmodifiableMap(result);
  }

  private static <E extends Enum<E>> E parse(Class<E> type, String value, E fallback) {
    try {
      return value == null ? fallback : Enum.valueOf(type, value);
    } catch (IllegalArgumentException e) {
      // A value written by a newer schema.
      return fallback;
    }
  }
}
