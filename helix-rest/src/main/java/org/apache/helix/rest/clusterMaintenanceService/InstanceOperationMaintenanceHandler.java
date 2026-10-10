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
import java.util.Collection;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;

import com.fasterxml.jackson.annotation.JsonInclude;
import org.apache.helix.AccessOption;
import org.apache.helix.BaseDataAccessor;
import org.apache.helix.HelixException;
import org.apache.helix.PropertyPathBuilder;
import org.apache.helix.api.exceptions.HelixMetaDataAccessException;
import org.apache.helix.constants.InstanceConstants;
import org.apache.helix.manager.zk.ZkBaseDataAccessor;
import org.apache.helix.model.ClusterConfig;
import org.apache.helix.model.ClusterTopologyConfig;
import org.apache.helix.model.InstanceConfig;
import org.apache.helix.util.InstanceUtil;
import org.apache.helix.zookeeper.api.client.RealmAwareZkClient;
import org.apache.helix.zookeeper.datamodel.ZNRecord;
import org.apache.helix.zookeeper.zkclient.exception.ZkBadVersionException;
import org.apache.helix.zookeeper.zkclient.exception.ZkNoNodeException;
import org.apache.helix.zookeeper.zkclient.exception.ZkNodeExistsException;
import org.apache.zookeeper.Op;
import org.apache.zookeeper.data.Stat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Writes the instance-operation maintenance marker, alone or together with an instance
 * operation, so the marker cap fails closed: an instance the cap has no room for gets neither.
 * <ul>
 *   <li>DISABLE or EVACUATE sets the operation and takes or renews the marker.</li>
 *   <li>ENABLE sets the operation and clears the marker once the effective operation is ENABLE.
 *       While another source still holds an operation, the marker stays until it expires.</li>
 *   <li>No operation takes or renews the marker alone, or clears it with {@link #CLEAR_MARKER},
 *       for a restart that changes no operation.</li>
 * </ul>
 * An instance without a live marker takes one only while live markers are below the cap;
 * renewing a live one takes no new budget and never shortens it. With no cap configured, any
 * number may take one. Only USER and AUTOMATION may set an operation. SWAP_IN and UNKNOWN never
 * count as offline, so a marker would exempt nothing, and an ADMIN operation would clear every
 * other source's operation.
 *
 * <p>A request is all or nothing. It commits in one ZooKeeper multi: a versioned write of each
 * changed instance config, a versioned write of a per-cluster fence znode, and a version check of
 * the cluster config. If any instance is rejected, nothing is written. Every commit bumps the
 * fence, so two admissions judged against the same view cannot both land; the loser re-reads and
 * is judged again. The cap is strict as long as every marker write goes through this handler.
 * Two instances that share a logical ID cannot change their operation in the same request, since
 * each one's transition is validated against the other's current operation.
 *
 * <p>An operation its source already holds is not set again, so repeating a request only renews
 * the marker and never reorders the operation stack, and a request that changes nothing writes
 * nothing. Repeating a request is always safe, and is how a caller settles an unknown outcome.
 */
public class InstanceOperationMaintenanceHandler {
  private static final Logger LOG =
      LoggerFactory.getLogger(InstanceOperationMaintenanceHandler.class);

  /** Child of the cluster's property store, which no controller or participant watches. */
  static final String FENCE_NODE_NAME = "INSTANCE_OPERATION_MAINTENANCE_FENCE";
  static final int MAX_ATTEMPTS = 5;
  /**
   * Half the default jute.maxbuffer. ZooKeeper drops a larger multi together with the connection,
   * and the client resends it until its retry timeout.
   */
  static final int MAX_COMMIT_BYTES = 512 * 1024;
  /** Expiry that takes the cluster's default marker duration. */
  public static final long DEFAULT_EXPIRY = 0L;
  /** Expiry that clears the marker when no operation is given. */
  public static final long CLEAR_MARKER = -1L;
  private static final Set<InstanceConstants.InstanceOperation> OPERATIONS = EnumSet.of(
      InstanceConstants.InstanceOperation.ENABLE, InstanceConstants.InstanceOperation.DISABLE,
      InstanceConstants.InstanceOperation.EVACUATE);
  private static final Set<InstanceConstants.InstanceOperationSource> SOURCES = EnumSet.of(
      InstanceConstants.InstanceOperationSource.USER,
      InstanceConstants.InstanceOperationSource.AUTOMATION);

  public enum Status {
    APPLIED,
    INSTANCE_NOT_FOUND,
    INVALID_TRANSITION,
    GUARDRAIL_REJECTED,
    BUDGET_EXHAUSTED,
    /** Not rejected itself; nothing was written because another instance was rejected. */
    NOT_APPLIED,
    /** Lost the race to concurrent writers on every attempt. */
    CONFLICT,
    /** Failed on an unexpected error, such as a ZooKeeper failure. */
    ERROR
  }

  /**
   * Per-instance result. Either every instance is {@link Status#APPLIED}, or nothing was written.
   * A {@link Status#NOT_APPLIED} instance passed every check, so resending only those is a
   * request already admitted against the same state. A CONFLICT or ERROR normally wrote nothing,
   * but after a ZooKeeper connection loss the commit may have landed; repeating the request
   * settles it.
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

    /** On APPLIED, the marker's expiry after the request; null when the instance has none. */
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

  private final RealmAwareZkClient _zkClient;
  private final BaseDataAccessor<ZNRecord> _accessor;
  private final Function<Collection<String>, Optional<String>> _guardrail;

  /**
   * @param guardrail given the instances whose operation changes, returns why the requested
   *     operation would be unsafe on them together, or empty. Runs once per attempt, on the
   *     instances that passed every other check.
   */
  public InstanceOperationMaintenanceHandler(RealmAwareZkClient zkClient,
      Function<Collection<String>, Optional<String>> guardrail) {
    _zkClient = Objects.requireNonNull(zkClient, "zkClient");
    _accessor = new ZkBaseDataAccessor<>(zkClient);
    _guardrail = Objects.requireNonNull(guardrail, "guardrail");
  }

  /**
   * Applies the request to every instance in one commit, or to none. The cap admits new markers
   * in input order.
   *
   * @param operation the operation to set, or null to write only the marker.
   * @param expiresAtMillis marker expiry: a future epoch time, {@link #DEFAULT_EXPIRY}, or with no
   *     operation {@link #CLEAR_MARKER}. Ignored for ENABLE.
   * @throws BadRequestException if the operation or source is not supported, the instance list
   *     is empty or has a blank name, the cluster does not exist, the expiry is in the past or
   *     the default with no cluster default, or the configs to write exceed
   *     {@link #MAX_COMMIT_BYTES}.
   */
  public Map<String, Outcome> apply(String clusterId, List<String> instances,
      InstanceConfig.InstanceOperation operation, long expiresAtMillis) {
    if (operation != null && (!OPERATIONS.contains(operation.getOperation())
        || !SOURCES.contains(operation.getSource()))) {
      throw new BadRequestException("instanceOperation must be one of " + OPERATIONS
          + " and instanceOperationSource one of " + SOURCES);
    }
    if (instances == null || instances.isEmpty()
        || instances.stream().anyMatch(name -> name == null || name.isEmpty())) {
      throw new BadRequestException("instances must be a non-empty list of instance names");
    }
    ZNRecord clusterRecord = _accessor.get(PropertyPathBuilder.clusterConfig(clusterId), null,
        AccessOption.PERSISTENT);
    if (clusterRecord == null) {
      throw new BadRequestException("Cluster " + clusterId + " not found");
    }
    long expiresAt = expiresAtMillis;
    if (operation == null ? expiresAtMillis == CLEAR_MARKER
        : operation.getOperation() == InstanceConstants.InstanceOperation.ENABLE) {
      expiresAt = InstanceConfig.INSTANCE_OPERATION_MAINTENANCE_NOT_SET;
    } else if (expiresAtMillis == DEFAULT_EXPIRY) {
      long duration = new ClusterConfig(clusterRecord)
          .getDefaultInstanceOperationMaintenanceDurationMs();
      if (duration < 0L) {
        throw new BadRequestException("expiresAtMillis not supplied and cluster " + clusterId
            + " has no DEFAULT_INSTANCE_OPERATION_MAINTENANCE_DURATION_MS");
      }
      expiresAt = System.currentTimeMillis() + duration;
    } else if (expiresAtMillis <= System.currentTimeMillis()) {
      throw new BadRequestException("expiresAtMillis " + expiresAtMillis + " is not in the future");
    }
    try {
      // Never creates parents, so a cluster dropped meanwhile is not partly brought back.
      _zkClient.createPersistent(fencePath(clusterId), new ZNRecord(FENCE_NODE_NAME));
    } catch (ZkNodeExistsException e) {
      // Created by an earlier request.
    } catch (ZkNoNodeException e) {
      throw new BadRequestException("Cluster " + clusterId + " not found");
    }

    List<String> names = new ArrayList<>(new LinkedHashSet<>(instances));
    Map<String, Outcome> outcomes;
    try {
      outcomes = commit(clusterId, names, operation, expiresAt);
    } catch (BadRequestException e) {
      throw e;
    } catch (RuntimeException e) {
      LOG.warn("Failed to apply the request to {} in cluster {}", names, clusterId, e);
      outcomes = all(names, new Outcome(Status.ERROR, String.valueOf(e)));
    }
    LOG.info("Applied instance operation {} and marker expiry {} in cluster {}: {}",
        operation == null ? "none" : operation.getOperation() + " from " + operation.getSource(),
        expiresAt, clusterId, outcomes);
    return outcomes;
  }

  private Map<String, Outcome> commit(String clusterId, List<String> instances,
      InstanceConfig.InstanceOperation operation, long expiresAt) {
    List<String> paths = instances.stream()
        .map(instance -> PropertyPathBuilder.instanceConfig(clusterId, instance))
        .collect(Collectors.toList());
    String clusterConfigPath = PropertyPathBuilder.clusterConfig(clusterId);
    String fencePath = fencePath(clusterId);
    for (int attempt = 1; attempt <= MAX_ATTEMPTS; attempt++) {
      // The fence is read first, so every commit before this version is visible in what follows.
      Stat fenceStat = _accessor.getStat(fencePath, AccessOption.PERSISTENT);
      Stat clusterStat = new Stat();
      ZNRecord clusterRecord = _accessor.get(clusterConfigPath, clusterStat,
          AccessOption.PERSISTENT);
      List<Stat> stats = new ArrayList<>();
      List<ZNRecord> records = _accessor.get(paths, stats, AccessOption.PERSISTENT, true);
      if (fenceStat == null || clusterRecord == null) {
        throw new HelixException("Cluster " + clusterId + " was removed during the request");
      }
      ClusterConfig clusterConfig = new ClusterConfig(clusterRecord);
      String logicalIdKey = operation == null ? null
          : ClusterTopologyConfig.createFromClusterConfig(clusterConfig).getEndNodeType();
      long nowMs = System.currentTimeMillis();
      Integer capRoom = null;
      int newMarkers = 0;
      Map<String, String> changingByLogicalId = new HashMap<>();
      Map<String, Outcome> outcomes = new LinkedHashMap<>();
      List<Op> ops = new ArrayList<>();
      long bytes = 0L;
      for (int i = 0; i < instances.size(); i++) {
        String instance = instances.get(i);
        if (records.get(i) == null) {
          outcomes.put(instance,
              new Outcome(Status.INSTANCE_NOT_FOUND, "instance not found in cluster " + clusterId));
          continue;
        }
        InstanceConfig config = new InstanceConfig(records.get(i));
        long before = config.getInstanceOperationMaintenanceUntilMs();
        InstanceConfig.InstanceOperation held =
            operation == null ? null : config.getInstanceOperation(operation.getSource());
        // A legacy enable can override a held DISABLE; setting it again restores it, as the
        // legacy setInstanceOperation path does.
        boolean holds = operation == null || (held != null
            && held.getOperation() == operation.getOperation()
            && !(held.getOperation() == InstanceConstants.InstanceOperation.DISABLE
                && config.getInstanceEnabled()));
        if (!holds) {
          try {
            InstanceUtil.validateInstanceOperationTransition(_accessor, clusterId, config,
                config.getInstanceOperation().getOperation(), operation.getOperation());
          } catch (HelixMetaDataAccessException e) {
            throw e;
          } catch (HelixException e) {
            outcomes.put(instance, new Outcome(Status.INVALID_TRANSITION, e.getMessage()));
            continue;
          }
          String sibling = changingByLogicalId.get(config.getLogicalId(logicalIdKey));
          if (sibling != null) {
            outcomes.put(instance, new Outcome(Status.INVALID_TRANSITION, "shares its logical ID "
                + "with " + sibling + ", which this request also changes; send it separately"));
            continue;
          }
        }
        if (expiresAt > 0L && !config.isUnderInstanceOperationMaintenance(nowMs)) {
          if (capRoom == null) {
            capRoom = capRoom(clusterId, clusterConfig, nowMs);
          }
          if (newMarkers == capRoom) {
            outcomes.put(instance, new Outcome(Status.BUDGET_EXHAUSTED,
                "the marker cap has room for " + capRoom + " new markers"));
            continue;
          }
          newMarkers++;
        }
        if (!holds) {
          changingByLogicalId.put(config.getLogicalId(logicalIdKey), instance);
          config.setInstanceOperation(operation);
        }
        if (expiresAt > 0L) {
          config.setInstanceOperationMaintenanceUntilMs(Math.max(expiresAt, before));
        } else if (operation == null || config.getInstanceOperation().getOperation()
            == InstanceConstants.InstanceOperation.ENABLE) {
          config.setInstanceOperationMaintenanceUntilMs(expiresAt);
        }
        long until = config.getInstanceOperationMaintenanceUntilMs();
        outcomes.put(instance, new Outcome(Status.APPLIED, null, until > 0L ? until : null));
        if (!holds || until != before) {
          byte[] data = _zkClient.serialize(config.getRecord(), paths.get(i));
          bytes += data.length;
          ops.add(Op.setData(paths.get(i), data, stats.get(i).getVersion()));
        }
      }

      boolean rejected = outcomes.values().stream().anyMatch(o -> o.getStatus() != Status.APPLIED);
      if (bytes > MAX_COMMIT_BYTES) {
        throw new BadRequestException(String.format("The %d instance configs to write take %d "
            + "bytes, over the %d byte limit of one commit; send fewer instances", ops.size(),
            bytes, MAX_COMMIT_BYTES));
      }
      if (!changingByLogicalId.isEmpty()) {
        Optional<String> violation = _guardrail.apply(changingByLogicalId.values());
        if (violation.isPresent()) {
          changingByLogicalId.values().forEach(instance -> outcomes.put(instance,
              new Outcome(Status.GUARDRAIL_REJECTED, violation.get())));
          rejected = true;
        }
      }
      if (rejected) {
        outcomes.replaceAll((instance, outcome) -> outcome.getStatus() != Status.APPLIED ? outcome
            : new Outcome(Status.NOT_APPLIED, "nothing was written because another instance was "
                + "rejected; resend without the rejected instances"));
        return outcomes;
      }
      if (ops.isEmpty()) {
        return outcomes;
      }
      if (expiresAt > 0L && expiresAt <= System.currentTimeMillis()) {
        return all(instances, new Outcome(Status.ERROR, "marker expiry " + expiresAt
            + " passed before the commit; nothing was written"));
      }

      ops.add(Op.setData(fencePath, _zkClient.serialize(new ZNRecord(FENCE_NODE_NAME), fencePath),
          fenceStat.getVersion()));
      ops.add(Op.check(clusterConfigPath, clusterStat.getVersion()));
      try {
        _zkClient.multi(ops);
        return outcomes;
      } catch (ZkBadVersionException | ZkNoNodeException e) {
        // Also covers a resend of a multi that already landed: the next attempt sees the held
        // operations and live markers, and commits only renewals.
        LOG.info("The request in cluster {} lost a race on attempt {} of {}; re-reading.",
            clusterId, attempt, MAX_ATTEMPTS);
      }
    }
    return all(instances,
        new Outcome(Status.CONFLICT, "lost to concurrent writes " + MAX_ATTEMPTS + " times"));
  }

  /** How many new markers the cap has room for; unlimited when no cap is set. */
  private int capRoom(String clusterId, ClusterConfig clusterConfig, long nowMs) {
    int absoluteCap = clusterConfig.getInstanceOperationMaintenanceBudget();
    int percentageCap = clusterConfig.getInstanceOperationMaintenanceBudgetPercentage();
    if (absoluteCap < 0 && percentageCap < 0) {
      return Integer.MAX_VALUE;
    }
    // Fails rather than under-count when a child cannot be read.
    List<ZNRecord> records = _accessor.getChildren(PropertyPathBuilder.instanceConfig(clusterId),
        null, AccessOption.PERSISTENT, 3, 100);
    int cap = absoluteCap >= 0 ? absoluteCap : (int) (percentageCap * (long) records.size() / 100);
    long live = records.stream().filter(
        r -> r != null && new InstanceConfig(r).isUnderInstanceOperationMaintenance(nowMs)).count();
    return (int) Math.max(0L, cap - live);
  }

  private static Map<String, Outcome> all(List<String> instances, Outcome outcome) {
    Map<String, Outcome> outcomes = new LinkedHashMap<>();
    instances.forEach(instance -> outcomes.put(instance, outcome));
    return outcomes;
  }

  static String fencePath(String clusterId) {
    return PropertyPathBuilder.propertyStore(clusterId) + "/" + FENCE_NODE_NAME;
  }
}
