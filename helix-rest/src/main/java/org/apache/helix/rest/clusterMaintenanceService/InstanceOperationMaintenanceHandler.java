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
import java.util.Arrays;
import java.util.Collection;
import java.util.EnumMap;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.UUID;
import java.util.function.Function;
import java.util.stream.Collectors;

import com.fasterxml.jackson.annotation.JsonInclude;
import org.apache.helix.AccessOption;
import org.apache.helix.BaseDataAccessor;
import org.apache.helix.HelixException;
import org.apache.helix.PropertyPathBuilder;
import org.apache.helix.api.exceptions.HelixMetaDataAccessException;
import org.apache.helix.constants.InstanceConstants;
import org.apache.helix.guardrail.ValidationResult;
import org.apache.helix.guardrail.Violation;
import org.apache.helix.manager.zk.ZkBaseDataAccessor;
import org.apache.helix.model.ClusterConfig;
import org.apache.helix.model.ClusterTopologyConfig;
import org.apache.helix.model.InstanceConfig;
import org.apache.helix.util.InstanceUtil;
import org.apache.helix.zookeeper.api.client.RealmAwareZkClient;
import org.apache.helix.zookeeper.datamodel.ZNRecord;
import org.apache.helix.zookeeper.zkclient.exception.ZkBadVersionException;
import org.apache.helix.zookeeper.zkclient.exception.ZkNoNodeException;
import org.apache.helix.zookeeper.zkclient.exception.ZkTimeoutException;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.Op;
import org.apache.zookeeper.common.PathUtils;
import org.apache.zookeeper.data.Stat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Writes the instance-operation maintenance marker, alone or together with an instance
 * operation, so the marker cap fails closed: an instance the cap has no room for gets neither.
 * <ul>
 *   <li>DISABLE or EVACUATE sets the operation and takes the marker.</li>
 *   <li>ENABLE sets the operation and clears the marker, even while another source still holds
 *       an operation.</li>
 *   <li>No operation takes the marker alone, or clears it with {@link #CLEAR_MARKER}, for a
 *       restart that changes no operation.</li>
 * </ul>
 * An instance without a live marker takes one only while live markers are below the cap. One
 * with a live marker keeps it as is and takes no new budget, so a marker is written once and a
 * later request never extends or shortens it. With no cap configured, any number may take one.
 * Only USER and AUTOMATION may set an operation. SWAP_IN and UNKNOWN never count as offline, so a
 * marker would exempt nothing, and an ADMIN operation would clear every other source's operation.
 *
 * <p>A request commits every instance that passes every check, in one ZooKeeper multi: a versioned
 * write of each changed instance config, a version check of each instance left as is, and a
 * version check of the cluster config. A rejected instance is not written and does not hold back
 * the rest. If any of them changed since the read, the commit fails and the request is read and
 * judged again. The cap and the guardrail see one request at a time, so they hold as long as
 * every marker write goes through this handler and each cluster has one caller that sends one
 * request at a time, as ACM does. Two instances that share a logical ID cannot change their
 * operation in the same request, since each one's transition is validated against the other's
 * current operation.
 *
 * <p>Every {@link Status} maps to one action for the caller, and every outcome carries a
 * one-line message that says what happened and what to do, written to be shown to an operator as
 * is. Messages never carry Java exception text; the server log has it.
 *
 * <p>An operation its source already holds is not set again, so repeating a request never
 * reorders the operation stack, and a request that changes nothing writes nothing. Repeating a
 * request is always safe, and is how a caller settles an unknown outcome.
 */
public class InstanceOperationMaintenanceHandler {
  private static final Logger LOG =
      LoggerFactory.getLogger(InstanceOperationMaintenanceHandler.class);

  private static final int MAX_QUOTED_LENGTH = 200;
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
  /** How {@link org.apache.helix.guardrail.GuardrailPipeline} reports a rule that failed. */
  private static final String RULE_FAILED = "Rule evaluation failed: ";
  private static final String RULE_RETURNED_NOTHING = "Rule returned no result.";
  private static final Set<InstanceConstants.InstanceOperation> OPERATIONS = EnumSet.of(
      InstanceConstants.InstanceOperation.ENABLE, InstanceConstants.InstanceOperation.DISABLE,
      InstanceConstants.InstanceOperation.EVACUATE);
  private static final Set<InstanceConstants.InstanceOperationSource> SOURCES = EnumSet.of(
      InstanceConstants.InstanceOperationSource.USER,
      InstanceConstants.InstanceOperationSource.AUTOMATION);

  /** Per-instance status, each with the action it asks of the caller. */
  public enum Status {
    /** Written. The message says what the instance's operation and marker are now. */
    APPLIED,
    /** No such instance in the cluster. Drop it from the request. */
    INSTANCE_NOT_FOUND,
    /** The instance's current operation does not allow the requested one. Needs an operator. */
    INVALID_TRANSITION,
    /**
     * Shares its logical ID with another instance this request changes. Send it in a separate
     * request.
     */
    DUPLICATE_LOGICAL_ID,
    /**
     * The rebalance-feasibility guardrail judged the instances this request changes unsafe
     * together, or cannot judge them because the cluster's own WAGED assignment fails. Needs an
     * operator; the message says whether to change fewer instances or fix the cluster.
     */
    GUARDRAIL_REJECTED,
    /**
     * The marker cap has no room for this instance. Retry once markers are cleared or expire. A
     * cap of 0 admits none, and the message says it must be raised.
     */
    BUDGET_EXHAUSTED,
    /**
     * Not judged in this request, so not written: the marker cap had room for it only after
     * instances ahead of it were not written. Resend it.
     */
    NOT_APPLIED,
    /** Lost the race to concurrent writers on every attempt. Resend. */
    CONFLICT,
    /**
     * Failed on an unexpected error, such as a ZooKeeper failure or a guardrail that could not be
     * evaluated. The message says whether the commit may have landed. Resend to settle it.
     */
    ERROR
  }

  /**
   * Per-instance result. Every {@link Status#APPLIED} instance was written in one commit, and no
   * other instance was. A CONFLICT or ERROR normally wrote nothing, but after a ZooKeeper
   * connection loss the commit may have landed; repeating the request settles it. Every outcome
   * carries a message, written to be shown to an operator as is.
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

  /** A commit was sent and something failed after it, so whether it landed is unknown. */
  private static final class UnknownCommitException extends RuntimeException {
    private UnknownCommitException(RuntimeException cause) {
      super(cause);
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
  private final Function<Collection<String>, ValidationResult> _guardrail;

  /**
   * @param guardrail given the instances whose operation changes, judges whether the requested
   *     operation is safe on them together. Runs once per attempt, on the instances that passed
   *     every other check.
   */
  public InstanceOperationMaintenanceHandler(RealmAwareZkClient zkClient,
      Function<Collection<String>, ValidationResult> guardrail) {
    _zkClient = Objects.requireNonNull(zkClient, "zkClient");
    _accessor = new ZkBaseDataAccessor<>(zkClient);
    _guardrail = Objects.requireNonNull(guardrail, "guardrail");
  }

  /**
   * Applies the request, in one commit, to every instance that passes every check. The cap admits
   * new markers in input order.
   *
   * @param operation the operation to set, or null to write only the marker.
   * @param expiresAtMillis marker expiry: a future epoch time, {@link #DEFAULT_EXPIRY}, or with no
   *     operation {@link #CLEAR_MARKER}. Ignored for ENABLE.
   * @throws BadRequestException if the operation or source is not supported, the instance list
   *     is empty or has a name that is not a valid znode name, the cluster does not exist, the
   *     expiry is in the past or the default with no positive cluster default, or the configs to
   *     write exceed {@link #MAX_COMMIT_BYTES}.
   */
  public Map<String, Outcome> apply(String clusterId, List<String> instances,
      InstanceConfig.InstanceOperation operation, long expiresAtMillis) {
    if (operation != null && (!OPERATIONS.contains(operation.getOperation())
        || !SOURCES.contains(operation.getSource()))) {
      throw new BadRequestException(
          unsupportedOperation(operation.getOperation(), operation.getSource()));
    }
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
    long defaultDurationMs = -1L;
    if (operation == null ? expiresAtMillis == CLEAR_MARKER
        : operation.getOperation() == InstanceConstants.InstanceOperation.ENABLE) {
      expiresAt = InstanceConfig.INSTANCE_OPERATION_MAINTENANCE_NOT_SET;
    } else if (expiresAtMillis == DEFAULT_EXPIRY) {
      defaultDurationMs = new ClusterConfig(clusterRecord)
          .getDefaultInstanceOperationMaintenanceDurationMs();
      if (defaultDurationMs <= 0L) {
        throw new BadRequestException("expiresAtMillis is missing or 0, which asks for the "
            + "cluster default, but cluster " + clusterId + " has no positive "
            + "DEFAULT_INSTANCE_OPERATION_MAINTENANCE_DURATION_MS. Give an expiry, or set the "
            + "cluster default.");
      }
      expiresAt = System.currentTimeMillis() + defaultDurationMs;
    } else if (expiresAtMillis == CLEAR_MARKER) {
      throw new BadRequestException("expiresAtMillis -1 clears the marker, so it cannot go with "
          + operation.getOperation() + ". Give a future expiry, or 0 for the cluster default.");
    } else if (expiresAtMillis < 0L) {
      throw new BadRequestException("expiresAtMillis " + expiresAtMillis + " is negative. Give a "
          + "future epoch time in milliseconds, 0 for the cluster default, or -1 with no "
          + "instanceOperation to clear the marker.");
    } else if (expiresAtMillis <= System.currentTimeMillis()) {
      throw new BadRequestException("expiresAtMillis " + expiresAtMillis + " ("
          + Instant.ofEpochMilli(expiresAtMillis) + ") is not in the future. Give a later expiry.");
    }

    List<String> names = new ArrayList<>(new LinkedHashSet<>(instances));
    String requestId = UUID.randomUUID().toString();
    Map<String, Outcome> outcomes;
    try {
      outcomes = commit(clusterId, names, operation, expiresAt, defaultDurationMs, requestId);
    } catch (BadRequestException e) {
      throw e;
    } catch (RuntimeException e) {
      boolean unknown = e instanceof UnknownCommitException;
      Throwable failure = unknown ? e.getCause() : e;
      LOG.warn("Request {} failed to apply to {} in cluster {}", requestId, names, clusterId,
          failure);
      outcomes = all(names, new Outcome(Status.ERROR, failureMessage(failure, unknown)));
    }
    LOG.info("Instance operation maintenance request {} in cluster {}, operation {}, marker "
            + "expiry {}: {} {}", requestId, clusterId,
        operation == null ? "none" : operation.getOperation() + " by " + operation.getSource(),
        expiresAt > 0L ? Instant.ofEpochMilli(expiresAt) : "clear", summarize(outcomes), outcomes);
    return outcomes;
  }

  /**
   * The message for a request that failed on an unexpected error. It names the ZooKeeper error
   * when there is one and never carries Java exception text, which the server log keeps.
   *
   * @param unknown whether a commit was sent before the failure, so it may have landed.
   */
  public static String failureMessage(Throwable failure, boolean unknown) {
    String cause = zooKeeperCause(failure);
    String detail = cause == null ? "" : " (" + cause + ")";
    return unknown
        ? "Failed after the commit was sent" + detail + ", so whether it was written is unknown. "
            + "Resend the request to settle it."
        : "Nothing was written: the request failed" + detail + ". Resend the request; if it "
            + "keeps failing, check the Helix REST server log.";
  }

  private static String zooKeeperCause(Throwable failure) {
    Throwable cause = failure;
    for (int depth = 0; cause != null && depth < 20; depth++, cause = cause.getCause()) {
      if (cause instanceof KeeperException) {
        return "ZooKeeper error " + ((KeeperException) cause).code();
      }
      if (cause instanceof ZkTimeoutException) {
        return "ZooKeeper timed out";
      }
    }
    return null;
  }

  private Map<String, Outcome> commit(String clusterId, List<String> instances,
      InstanceConfig.InstanceOperation operation, long expiresAt, long defaultDurationMs,
      String requestId) {
    for (int attempt = 1; attempt <= MAX_ATTEMPTS; attempt++) {
      Map<String, Outcome> outcomes;
      try {
        outcomes = tryCommit(clusterId, instances, operation, expiresAt, defaultDurationMs);
      } catch (BadRequestException | UnknownCommitException e) {
        throw e;
      } catch (RuntimeException e) {
        // A commit that lost a race may still have landed before a connection loss.
        throw attempt == 1 ? e : new UnknownCommitException(e);
      }
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
      InstanceConfig.InstanceOperation operation, long expiresAt, long defaultDurationMs) {
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
    String logicalIdKey = operation == null ? null
        : ClusterTopologyConfig.createFromClusterConfig(clusterConfig).getEndNodeType();
    long nowMs = System.currentTimeMillis();
    MarkerCap cap = null;
    int newMarkers = 0;
    Set<String> tookMarker = new HashSet<>();
    Map<String, String> changingByLogicalId = new HashMap<>();
    Map<String, Outcome> outcomes = new LinkedHashMap<>();
    // Each instance's write, or its version check when held as is; sent only if it stays APPLIED.
    Map<String, Op> staged = new LinkedHashMap<>();
    int writes = 0;
    long bytes = 0L;
    for (int i = 0; i < instances.size(); i++) {
      String instance = instances.get(i);
      if (records.get(i) == null) {
        outcomes.put(instance, new Outcome(Status.INSTANCE_NOT_FOUND,
            "No such instance in cluster " + clusterId + ". Check the instance name, and whether "
                + "it has been added to the cluster."));
        continue;
      }
      InstanceConfig config = new InstanceConfig(records.get(i));
      long before = config.getInstanceOperationMaintenanceUntilMs();
      boolean wasLive = config.isUnderInstanceOperationMaintenance(nowMs);
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
          outcomes.put(instance, new Outcome(Status.INVALID_TRANSITION,
              invalidTransition(config, operation.getOperation(), e.getMessage())));
          continue;
        }
        String logicalId = config.getLogicalId(logicalIdKey);
        String sibling = changingByLogicalId.get(logicalId);
        if (sibling != null) {
          outcomes.put(instance, new Outcome(Status.DUPLICATE_LOGICAL_ID, "Shares logical ID "
              + logicalId + " with " + sibling + ", which this request also changes. Send it in "
              + "a separate request."));
          continue;
        }
      }
      if (expiresAt > 0L && !wasLive) {
        if (cap == null) {
          cap = markerCap(clusterId, clusterConfig, nowMs);
        }
        if (newMarkers == cap.room()) {
          outcomes.put(instance, new Outcome(Status.BUDGET_EXHAUSTED, cap.exhausted()));
          continue;
        }
        newMarkers++;
        tookMarker.add(instance);
      }
      if (!holds) {
        changingByLogicalId.put(config.getLogicalId(logicalIdKey), instance);
        config.setInstanceOperation(operation);
      }
      if (expiresAt <= 0L || !wasLive) {
        config.setInstanceOperationMaintenanceUntilMs(expiresAt);
      }
      long until = config.getInstanceOperationMaintenanceUntilMs();
      outcomes.put(instance, new Outcome(Status.APPLIED,
          applied(config, operation, holds, expiresAt, before, wasLive),
          config.isUnderInstanceOperationMaintenance(nowMs) ? until : null));
      if (!holds || until != before) {
        byte[] data = _zkClient.serialize(config.getRecord(), paths.get(i));
        writes++;
        bytes += data.length;
        staged.put(instance, Op.setData(paths.get(i), data, stats.get(i).getVersion()));
      } else {
        // Held as is, so the commit fails if it changed since it was judged.
        staged.put(instance, Op.check(paths.get(i), stats.get(i).getVersion()));
      }
    }

    if (bytes > MAX_COMMIT_BYTES) {
      throw new BadRequestException(String.format("The %d instance configs to write take %d "
          + "bytes, over the %d byte limit of one commit. Send fewer instances per request.",
          writes, bytes, MAX_COMMIT_BYTES));
    }
    if (!changingByLogicalId.isEmpty()) {
      Collection<String> changing = changingByLogicalId.values();
      ValidationResult verdict = _guardrail.apply(changing);
      List<Violation> judged = verdict.isFeasible() ? new ArrayList<>()
          : verdict.getViolations().stream()
              .filter(violation -> !failedToEvaluate(violation)).collect(Collectors.toList());
      if (!verdict.isFeasible() && judged.isEmpty()) {
        LOG.warn("The guardrail failed to evaluate {} on {} in cluster {}: {}",
            operation.getOperation(), changing, clusterId, verdict.getViolations());
        Outcome error = new Outcome(Status.ERROR, "Not written: the rebalance-feasibility "
            + "guardrail failed to evaluate " + operation.getOperation() + " on the "
            + plural(changing.size(), "instance") + " this request changes. Resend it; if it "
            + "keeps failing, check the Helix REST server log.");
        changing.forEach(instance -> outcomes.put(instance, error));
      } else if (!judged.isEmpty()) {
        LOG.warn("The guardrail rejected {} on {} in cluster {}: {}", operation.getOperation(),
            changing, clusterId, verdict.getViolations());
        Outcome outcome = new Outcome(Status.GUARDRAIL_REJECTED,
            guardrailMessage(judged, operation.getOperation(), changing.size()));
        changing.forEach(instance -> outcomes.put(instance, outcome));
      }
    }
    if (cap != null) {
      // Instances not written give back the cap room they took, so as many instances the cap
      // turned away could have taken it. They were never judged.
      long room = cap.room() - tookMarker.stream()
          .filter(instance -> outcomes.get(instance).getStatus() == Status.APPLIED).count();
      for (Map.Entry<String, Outcome> entry : outcomes.entrySet()) {
        if (room > 0L && entry.getValue().getStatus() == Status.BUDGET_EXHAUSTED) {
          entry.setValue(new Outcome(Status.NOT_APPLIED, "Not judged: the marker cap had no room "
              + "for it until instances ahead of it in this request were not written. Resend it."));
          room--;
        }
      }
    }
    List<Op> ops = new ArrayList<>();
    List<Op> checks = new ArrayList<>();
    staged.forEach((instance, op) -> {
      if (outcomes.get(instance).getStatus() == Status.APPLIED) {
        (op instanceof Op.Check ? checks : ops).add(op);
      }
    });
    if (ops.isEmpty()) {
      return outcomes;
    }
    if (expiresAt > 0L && expiresAt <= System.currentTimeMillis()) {
      Outcome error = new Outcome(Status.ERROR, defaultDurationMs > 0L
          ? "Nothing was written: the cluster default marker duration of " + defaultDurationMs
              + " ms ran out before the commit. Give an expiry, or raise "
              + "DEFAULT_INSTANCE_OPERATION_MAINTENANCE_DURATION_MS for cluster " + clusterId + "."
          : "Nothing was written: the marker expiry " + Instant.ofEpochMilli(expiresAt)
              + " passed before the commit. Resend with a later expiry.");
      outcomes.replaceAll((instance, outcome) ->
          outcome.getStatus() == Status.APPLIED ? error : outcome);
      return outcomes;
    }

    ops.addAll(checks);
    ops.add(Op.check(clusterConfigPath, clusterStat.getVersion()));
    try {
      _zkClient.multi(ops);
      return outcomes;
    } catch (ZkBadVersionException | ZkNoNodeException e) {
      // Also covers a resend of a multi that already landed; the next attempt finds the request
      // already applied, and says so.
      return null;
    } catch (RuntimeException e) {
      throw new UnknownCommitException(e);
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
    if (counts.size() == 1 && (counts.containsKey(Status.CONFLICT)
        || counts.containsKey(Status.ERROR))
        && outcomes.values().stream().map(Outcome::getMessage).distinct().count() == 1) {
      // A failure of the whole request, so every instance carries the same message.
      return outcomes.values().iterator().next().getMessage();
    }
    return String.format("Applied to %d of %s (%s).", applied, plural(total, "instance"),
        counts.entrySet().stream().filter(e -> e.getKey() != Status.APPLIED)
            .map(e -> e.getValue() + " " + e.getKey()).collect(Collectors.joining(", ")));
  }

  /** Why the given operation and source cannot be requested together. */
  public static String unsupportedOperation(Object operation, Object source) {
    return "instanceOperation must be one of " + OPERATIONS + " and instanceOperationSource one of "
        + SOURCES + ", given together; got " + operation + " and " + source + ".";
  }

  /**
   * Leads with the instance's current operation and who set it, then keeps Helix's own reason
   * without the parts the lead already says. Falls back to Helix's whole message if its wording
   * changes.
   */
  private static String invalidTransition(InstanceConfig config,
      InstanceConstants.InstanceOperation requested, String helixMessage) {
    InstanceConfig.InstanceOperation current = config.getInstanceOperation();
    String reason = current.getReason();
    String detail = String.valueOf(helixMessage);
    String transition = " instance operation transition from " + current.getOperation() + " to "
        + requested + " for instance " + config.getInstanceName() + ". ";
    for (String prefix : Arrays.asList("Invalid" + transition,
        "Failed validation for" + transition)) {
      if (detail.startsWith(prefix)) {
        detail = detail.substring(prefix.length());
        break;
      }
    }
    String suffix = " Current operation source: " + current.getSource() + ", reason: " + reason;
    if (detail.endsWith(suffix)) {
      detail = detail.substring(0, detail.length() - suffix.length());
    }
    detail = clip(oneLine(detail), 500);
    return "Cannot change to " + requested + " from its current operation "
        + current.getOperation() + ", " + setBy(current) + ". " + detail
        + (detail.endsWith(".") ? "" : ".")
        + " An operator must resolve this before the instance is resent.";
  }

  /**
   * What an applied request left on the instance: whether the requested operation is the one in
   * effect, and the marker's state.
   */
  private static String applied(InstanceConfig config, InstanceConfig.InstanceOperation requested,
      boolean held, long expiresAt, long before, boolean wasLive) {
    List<String> sentences = new ArrayList<>();
    if (requested != null) {
      String set = requested.getOperation() + (held ? " was already set by " : " is now set by ")
          + requested.getSource();
      InstanceConfig.InstanceOperation active = config.getInstanceOperation();
      sentences.add(active.getOperation() == requested.getOperation() ? set + "."
          : set + ", but " + active.getOperation() + " " + setBy(active) + " still applies.");
    }
    long until = config.getInstanceOperationMaintenanceUntilMs();
    String expiry = String.valueOf(Instant.ofEpochMilli(until));
    if (expiresAt > 0L) {
      sentences.add((wasLive ? "Kept its live maintenance marker, which runs until "
          : "Took a maintenance marker until ") + expiry + ".");
    } else if (before > 0L) {
      sentences.add("Cleared the maintenance marker.");
    } else if (requested == null) {
      sentences.add("It had no maintenance marker to clear.");
    }
    return String.join(" ", sentences);
  }

  /** Who set the operation, and why when a reason was given. */
  private static String setBy(InstanceConfig.InstanceOperation operation) {
    String reason = operation.getReason();
    return "set by " + operation.getSource()
        + (reason.isEmpty() ? "" : " (reason: " + clip(oneLine(reason), MAX_QUOTED_LENGTH) + ")");
  }

  private static String oneLine(String text) {
    return text.replaceAll("\\s+", " ").trim();
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

  /**
   * Says why the guardrail rejected the batch, from its structured violations. The violations' own
   * text is written for endpoints that accept force=true, which this one does not, so only an
   * unrecognized violation is quoted.
   */
  private static String guardrailMessage(List<Violation> violations,
      InstanceConstants.InstanceOperation operation, int changing) {
    Violation first = violations.get(0);
    String head = "Rejected by guardrail " + first.getRuleId() + ": ";
    String subject = operation + " on the " + plural(changing, "instance")
        + " this request changes";
    List<String> partitions = violations.stream().map(Violation::getPartitionName)
        .filter(Objects::nonNull).collect(Collectors.toList());
    if (!partitions.isEmpty()) {
      boolean more = partitions.size() > 3 || partitions.size() < violations.size();
      return head + subject + " would leave partitions unable to place all their replicas, "
          + "including " + listed(partitions.subList(0, Math.min(3, partitions.size())), more)
          + ". Change fewer instances at once, or add assignable capacity.";
    }
    String text = String.valueOf(first.getMessage());
    if (text.startsWith("Could not compute a baseline WAGED assignment")) {
      return head + "the cluster's current WAGED assignment cannot be computed, so " + subject
          + " cannot be shown safe. Fix the cluster's WAGED rebalance first.";
    }
    if (text.startsWith("Applying ")
        && text.contains("makes the WAGED rebalancer unable to compute")) {
      return head + subject + " would leave WAGED unable to compute any assignment for the "
          + "cluster. Change fewer instances at once, or add assignable capacity.";
    }
    return head + clip(oneLine(text), 500);
  }

  /** Whether the guardrail failed to judge rather than judged the change unsafe. */
  private static boolean failedToEvaluate(Violation violation) {
    String text = String.valueOf(violation.getMessage());
    return text.startsWith(RULE_FAILED) || text.equals(RULE_RETURNED_NOTHING);
  }

  /** "a", "a and b", "a, b and c", or with more left out, "a, b, c and more". */
  private static String listed(List<String> names, boolean more) {
    List<String> items = new ArrayList<>(names);
    if (more) {
      items.add("more");
    }
    int last = items.size() - 1;
    return last == 0 ? items.get(0)
        : String.join(", ", items.subList(0, last)) + " and " + items.get(last);
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
