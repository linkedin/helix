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

import java.util.ArrayList;
import java.util.Collections;
import java.util.EnumMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import org.apache.helix.HelixProperty;
import org.apache.helix.zookeeper.datamodel.ZNRecord;

/**
 * A bounded, customer-facing snapshot of assignment convergence.
 */
public class ConvergenceStatus extends HelixProperty {
  public static final int SCHEMA_VERSION = 1;
  public static final int DEFAULT_MAX_PARTITION_DETAILS = 20;
  public static final int DEFAULT_MAX_ASSIGNMENT_ENTRIES = 64;

  private static final String STATUS_COUNTS = "STATUS_COUNTS";
  private static final String REASON_COUNTS = "REASON_COUNTS";
  private static final String DETAIL_PREFIX = "DETAIL_";
  private static final String CURRENT_PREFIX = "CURRENT_";
  private static final String TARGET_PREFIX = "TARGET_";

  public enum Scope {
    CLUSTER,
    RESOURCE
  }

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
    SCOPE,
    STATUS,
    PRIMARY_REASON,
    RESOURCE_NAME,
    GENERATED_AT,
    UNCONVERGED_SINCE,
    CONTROLLER_SESSION_ID,
    SOURCE_EVENT_ID,
    TOTAL_RESOURCE_COUNT,
    TOTAL_PARTITION_COUNT,
    CONVERGED_PARTITION_COUNT,
    IN_PROGRESS_PARTITION_COUNT,
    BLOCKED_PARTITION_COUNT,
    UNKNOWN_PARTITION_COUNT,
    AFFECTED_PARTITION_COUNT,
    TRUNCATED_PARTITION_COUNT,
    TARGET_FRESHNESS,
    OPTIMIZER_STATUS,
    COMPLETE
  }

  public static final class PartitionDetail {
    private final String _resourceName;
    private final String _partitionName;
    private final Status _status;
    private final Reason _reason;
    private final Map<String, String> _currentAssignment;
    private final Map<String, String> _targetAssignment;

    public PartitionDetail(String resourceName, String partitionName, Status status, Reason reason,
        Map<String, String> currentAssignment, Map<String, String> targetAssignment) {
      _resourceName = resourceName;
      _partitionName = partitionName;
      _status = status;
      _reason = reason;
      _currentAssignment = immutableSortedCopy(currentAssignment);
      _targetAssignment = immutableSortedCopy(targetAssignment);
    }

    public String getResourceName() {
      return _resourceName;
    }

    public String getPartitionName() {
      return _partitionName;
    }

    public Status getStatus() {
      return _status;
    }

    public Reason getReason() {
      return _reason;
    }

    public Map<String, String> getCurrentAssignment() {
      return _currentAssignment;
    }

    public Map<String, String> getTargetAssignment() {
      return _targetAssignment;
    }
  }

  public ConvergenceStatus(String id) {
    super(new ZNRecord(id));
    setSchemaVersion(SCHEMA_VERSION);
    setComplete(true);
  }

  public ConvergenceStatus(ZNRecord record) {
    super(record);
  }

  public void setSchemaVersion(int version) {
    _record.setIntField(Field.SCHEMA_VERSION.name(), version);
  }

  public int getSchemaVersion() {
    return _record.getIntField(Field.SCHEMA_VERSION.name(), 0);
  }

  public void setScope(Scope scope) {
    _record.setEnumField(Field.SCOPE.name(), scope);
  }

  public Scope getScope() {
    return _record.getEnumField(Field.SCOPE.name(), Scope.class, null);
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

  public void setResourceName(String resourceName) {
    _record.setSimpleField(Field.RESOURCE_NAME.name(), resourceName);
  }

  public String getResourceName() {
    return _record.getSimpleField(Field.RESOURCE_NAME.name());
  }

  public void setGeneratedAt(long timestamp) {
    _record.setLongField(Field.GENERATED_AT.name(), timestamp);
  }

  public long getGeneratedAt() {
    return _record.getLongField(Field.GENERATED_AT.name(), 0L);
  }

  public void setUnconvergedSince(long timestamp) {
    _record.setLongField(Field.UNCONVERGED_SINCE.name(), timestamp);
  }

  public long getUnconvergedSince() {
    return _record.getLongField(Field.UNCONVERGED_SINCE.name(), 0L);
  }

  public void setControllerSessionId(String sessionId) {
    _record.setSimpleField(Field.CONTROLLER_SESSION_ID.name(), sessionId);
  }

  public String getControllerSessionId() {
    return _record.getSimpleField(Field.CONTROLLER_SESSION_ID.name());
  }

  public void setSourceEventId(String eventId) {
    _record.setSimpleField(Field.SOURCE_EVENT_ID.name(), eventId);
  }

  public String getSourceEventId() {
    return _record.getSimpleField(Field.SOURCE_EVENT_ID.name());
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

  public void setComplete(boolean complete) {
    _record.setBooleanField(Field.COMPLETE.name(), complete);
  }

  public boolean isComplete() {
    return _record.getBooleanField(Field.COMPLETE.name(), false);
  }

  public void setTotalResourceCount(int value) {
    setCount(Field.TOTAL_RESOURCE_COUNT, value);
  }

  public int getTotalResourceCount() {
    return getCount(Field.TOTAL_RESOURCE_COUNT);
  }

  public void setTotalPartitionCount(int value) {
    setCount(Field.TOTAL_PARTITION_COUNT, value);
  }

  public int getTotalPartitionCount() {
    return getCount(Field.TOTAL_PARTITION_COUNT);
  }

  public void setConvergedPartitionCount(int value) {
    setCount(Field.CONVERGED_PARTITION_COUNT, value);
  }

  public int getConvergedPartitionCount() {
    return getCount(Field.CONVERGED_PARTITION_COUNT);
  }

  public void setInProgressPartitionCount(int value) {
    setCount(Field.IN_PROGRESS_PARTITION_COUNT, value);
  }

  public int getInProgressPartitionCount() {
    return getCount(Field.IN_PROGRESS_PARTITION_COUNT);
  }

  public void setBlockedPartitionCount(int value) {
    setCount(Field.BLOCKED_PARTITION_COUNT, value);
  }

  public int getBlockedPartitionCount() {
    return getCount(Field.BLOCKED_PARTITION_COUNT);
  }

  public void setUnknownPartitionCount(int value) {
    setCount(Field.UNKNOWN_PARTITION_COUNT, value);
  }

  public int getUnknownPartitionCount() {
    return getCount(Field.UNKNOWN_PARTITION_COUNT);
  }

  public void setAffectedPartitionCount(int value) {
    setCount(Field.AFFECTED_PARTITION_COUNT, value);
  }

  public int getAffectedPartitionCount() {
    return getCount(Field.AFFECTED_PARTITION_COUNT);
  }

  public void setTruncatedPartitionCount(int value) {
    setCount(Field.TRUNCATED_PARTITION_COUNT, value);
  }

  public int getTruncatedPartitionCount() {
    return getCount(Field.TRUNCATED_PARTITION_COUNT);
  }

  public void setStatusCounts(Map<Status, Integer> counts) {
    _record.setMapField(STATUS_COUNTS, enumCountMap(counts));
  }

  public Map<Status, Integer> getStatusCounts() {
    return parseEnumCountMap(_record.getMapField(STATUS_COUNTS), Status.class);
  }

  public void setReasonCounts(Map<Reason, Integer> counts) {
    _record.setMapField(REASON_COUNTS, enumCountMap(counts));
  }

  public Map<Reason, Integer> getReasonCounts() {
    return parseEnumCountMap(_record.getMapField(REASON_COUNTS), Reason.class);
  }

  public void setPartitionDetails(List<PartitionDetail> details, int maxDetails) {
    clearPartitionDetails();
    int retained = Math.min(details.size(), Math.max(0, maxDetails));
    for (int i = 0; i < retained; i++) {
      PartitionDetail detail = details.get(i);
      String suffix = detailSuffix(i);
      Map<String, String> metadata = new TreeMap<>();
      metadata.put(Field.RESOURCE_NAME.name(), detail.getResourceName());
      metadata.put("PARTITION_NAME", detail.getPartitionName());
      metadata.put(Field.STATUS.name(), detail.getStatus().name());
      metadata.put(Field.PRIMARY_REASON.name(), detail.getReason().name());
      metadata.put("CURRENT_ASSIGNMENT_COUNT",
          String.valueOf(detail.getCurrentAssignment().size()));
      metadata.put("TARGET_ASSIGNMENT_COUNT", String.valueOf(detail.getTargetAssignment().size()));
      _record.setMapField(DETAIL_PREFIX + suffix, metadata);
      _record.setMapField(CURRENT_PREFIX + suffix,
          boundedMap(detail.getCurrentAssignment(), DEFAULT_MAX_ASSIGNMENT_ENTRIES));
      _record.setMapField(TARGET_PREFIX + suffix,
          boundedMap(detail.getTargetAssignment(), DEFAULT_MAX_ASSIGNMENT_ENTRIES));
    }
    setTruncatedPartitionCount(Math.max(0, details.size() - retained));
  }

  public List<PartitionDetail> getPartitionDetails() {
    List<PartitionDetail> details = new ArrayList<>();
    for (int i = 0; i < DEFAULT_MAX_PARTITION_DETAILS; i++) {
      String suffix = detailSuffix(i);
      Map<String, String> metadata = _record.getMapField(DETAIL_PREFIX + suffix);
      if (metadata == null) {
        continue;
      }
      String resourceName = metadata.get(Field.RESOURCE_NAME.name());
      String partitionName = metadata.get("PARTITION_NAME");
      Status status = parseEnum(Status.class, metadata.get(Field.STATUS.name()), Status.UNKNOWN);
      Reason reason =
          parseEnum(Reason.class, metadata.get(Field.PRIMARY_REASON.name()), Reason.NONE);
      details.add(new PartitionDetail(resourceName, partitionName, status, reason,
          _record.getMapField(CURRENT_PREFIX + suffix),
          _record.getMapField(TARGET_PREFIX + suffix)));
    }
    return Collections.unmodifiableList(details);
  }

  public boolean semanticallyEquals(ConvergenceStatus other) {
    if (other == null) {
      return false;
    }
    ZNRecord left = new ZNRecord(_record);
    ZNRecord right = new ZNRecord(other.getRecord());
    removeVolatileFields(left);
    removeVolatileFields(right);
    return left.equals(right);
  }

  @Override
  public boolean isValid() {
    return getSchemaVersion() == SCHEMA_VERSION && getScope() != null && getStatus() != null;
  }

  private void clearPartitionDetails() {
    _record.getMapFields().keySet().removeIf(
        key -> key.startsWith(DETAIL_PREFIX) || key.startsWith(CURRENT_PREFIX)
            || key.startsWith(TARGET_PREFIX));
  }

  private void setCount(Field field, int value) {
    _record.setIntField(field.name(), Math.max(0, value));
  }

  private int getCount(Field field) {
    return _record.getIntField(field.name(), 0);
  }

  private static Map<String, String> immutableSortedCopy(Map<String, String> input) {
    if (input == null || input.isEmpty()) {
      return Collections.emptyMap();
    }
    return Collections.unmodifiableMap(new TreeMap<>(input));
  }

  private static Map<String, String> boundedMap(Map<String, String> input, int maxEntries) {
    Map<String, String> result = new TreeMap<>();
    input.entrySet().stream().limit(maxEntries)
        .forEach(entry -> result.put(entry.getKey(), entry.getValue()));
    return result;
  }

  private static <E extends Enum<E>> Map<String, String> enumCountMap(Map<E, Integer> counts) {
    Map<String, String> result = new TreeMap<>();
    if (counts != null) {
      counts.forEach((key, value) -> result.put(key.name(), String.valueOf(value)));
    }
    return result;
  }

  private static <E extends Enum<E>> Map<E, Integer> parseEnumCountMap(Map<String, String> input,
      Class<E> enumType) {
    Map<E, Integer> result = new EnumMap<>(enumType);
    if (input != null) {
      input.forEach((key, value) -> {
        try {
          result.put(Enum.valueOf(enumType, key), Integer.parseInt(value));
        } catch (IllegalArgumentException ignored) {
          // Ignore fields written by a newer schema.
        }
      });
    }
    return Collections.unmodifiableMap(result);
  }

  private static <E extends Enum<E>> E parseEnum(Class<E> enumType, String value, E defaultValue) {
    if (value == null) {
      return defaultValue;
    }
    try {
      return Enum.valueOf(enumType, value);
    } catch (IllegalArgumentException ignored) {
      return defaultValue;
    }
  }

  private static String detailSuffix(int index) {
    return String.format("%03d", index);
  }

  private static void removeVolatileFields(ZNRecord record) {
    record.getSimpleFields().remove(Field.GENERATED_AT.name());
    record.getSimpleFields().remove(Field.SOURCE_EVENT_ID.name());
    record.getSimpleFields().remove(Field.UNCONVERGED_SINCE.name());
  }
}
