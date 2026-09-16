package org.apache.helix.api.instance;

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
import java.util.Arrays;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

import javax.annotation.Nullable;

import org.apache.helix.BaseDataAccessor;
import org.apache.helix.HelixException;
import org.apache.helix.PropertyPathBuilder;
import org.apache.helix.api.mutation.CheckedMutationConflictReason;
import org.apache.helix.api.mutation.CheckedMutationExecutor;
import org.apache.helix.api.mutation.CheckedMutationResult;
import org.apache.helix.constants.InstanceConstants;
import org.apache.helix.model.InstanceConfig;
import org.apache.helix.util.InstanceUtil;
import org.apache.helix.zookeeper.datamodel.ZNRecord;
import org.apache.zookeeper.data.Stat;

/**
 * Conditional changes to an instance config: the caller states the operation or the disabled
 * partitions it wants, together with the conditions under which the change may happen, and
 * gets back whether the change was applied, was already satisfied, or was blocked, along with
 * the state that is in effect.
 *
 * <p><b>Why this exists.</b> Reading an instance config, deciding from it, and then writing
 * through the unchecked setters leaves a window in which another writer changes the very state
 * the decision was based on. The decision here is taken inside the conditional write instead,
 * on the exact content and version the write is conditioned on.
 *
 * <p><b>What is guaranteed.</b>
 * <ul>
 *   <li>Preconditions over the instance config content, and the change derived from them, are
 *       atomic with respect to every other writer of that instance config.</li>
 *   <li>A change that is already satisfied writes nothing, so no version is bumped, no
 *       timestamp is rewritten and no recorded operation changes position. Position matters:
 *       the last recorded operation is the active one.</li>
 *   <li>Another source's recorded operation is never moved, rewritten or removed. Only the
 *       source named in the request is touched.</li>
 *   <li>Deprecated instance fields are only relaxed when they can be attributed to the
 *       requesting source, unless the caller explicitly asks for the unchecked behaviour.</li>
 *   <li>By default, a change that would be recorded but would not result in the requested
 *       operation being in effect is refused rather than written, so a caller cannot mistake a
 *       write for a completed change.</li>
 *   <li>A change is refused when the instance config holds a recorded operation this version
 *       cannot read, rather than deciding against state whose meaning is unknown or dropping
 *       it on the way back out.</li>
 * </ul>
 *
 * <p><b>What is not guaranteed.</b>
 * <ul>
 *   <li>Deleting and recreating the instance config restarts its versions, so a version
 *       conditioned write can in principle land on a new incarnation. An expected creation id
 *       rejects a node that was already recreated when the change is evaluated, and the result
 *       reports the creation id observed after the write, but there is no
 *       creation-id-conditional write that would close the remaining window. This is not an
 *       identity fence, and callers must not treat it as one. Closing it needs a lease or lock
 *       protocol that every writer joins, which is a protocol change rather than an API
 *       change.</li>
 *   <li>Nothing here says anything about the process the instance describes. Whether the host
 *       is still the same incarnation, still live, or safe to act on remains the caller's
 *       responsibility.</li>
 *   <li>Transition validation that has to compare against other instances reads those other
 *       instances separately, so that part is a point in time check, exactly as it is on the
 *       unchecked path.</li>
 *   <li>The source recorded with an operation says who wrote it. It is not a capability and
 *       not a proof of exclusive ownership, because any writer can record any source.</li>
 * </ul>
 */
public final class CheckedInstanceChanges {
  private CheckedInstanceChanges() {
  }

  /**
   * Record the requested instance operation for the requesting source, if the conditions in
   * the request hold at mutation time.
   *
   * @param accessor accessor for the metadata store holding the instance config.
   * @param clusterName the cluster the instance belongs to.
   * @param instanceName the instance to change.
   * @param request the desired operation and the conditions under which to write it.
   * @return the outcome together with the instance operation state in effect.
   */
  public static CheckedMutationResult<EffectiveInstanceOperation> setInstanceOperation(
      BaseDataAccessor<ZNRecord> accessor, String clusterName, String instanceName,
      InstanceOperationChangeRequest request) {
    Objects.requireNonNull(clusterName, "clusterName must not be null");
    Objects.requireNonNull(instanceName, "instanceName must not be null");
    Objects.requireNonNull(request, "request must not be null");

    String path = PropertyPathBuilder.instanceConfig(clusterName, instanceName);
    return CheckedMutationExecutor.execute(accessor, path,
        (current, stat) -> evaluateInstanceOperation(accessor, clusterName, instanceName, request,
            current, stat));
  }

  /**
   * Bring the requested partitions of one resource into the requested disabled state, if the
   * conditions in the request hold at mutation time.
   *
   * <p>A partition that is only disabled through the deprecated cross resource list still
   * counts as disabled, so disabling it for one resource records the caller's intent
   * explicitly rather than reporting nothing to do. Enabling it is refused under the default
   * policy, because removing it from that list would enable it for every other resource too.
   *
   * @param accessor accessor for the metadata store holding the instance config.
   * @param clusterName the cluster the instance belongs to.
   * @param instanceName the instance to change.
   * @param request the desired partition state and the conditions under which to write it.
   * @return the outcome together with the disabled partition state in effect.
   */
  public static CheckedMutationResult<EffectiveDisabledPartitions> setPartitionsDisabled(
      BaseDataAccessor<ZNRecord> accessor, String clusterName, String instanceName,
      DisabledPartitionsChangeRequest request) {
    Objects.requireNonNull(clusterName, "clusterName must not be null");
    Objects.requireNonNull(instanceName, "instanceName must not be null");
    Objects.requireNonNull(request, "request must not be null");

    String path = PropertyPathBuilder.instanceConfig(clusterName, instanceName);
    return CheckedMutationExecutor.execute(accessor, path,
        (current, stat) -> evaluateDisabledPartitions(instanceName, request, current, stat));
  }

  private static CheckedMutationExecutor.Decision<EffectiveInstanceOperation>
      evaluateInstanceOperation(BaseDataAccessor<ZNRecord> accessor, String clusterName,
      String instanceName, InstanceOperationChangeRequest request, ZNRecord current, Stat stat) {
    // The evaluation works on a copy so that nothing observed is modified, not even in memory:
    // several data model setters mutate the structures they are given, and an accessor may be
    // serving a cached record. The data model copies again on construction, so the record that
    // carries the change is the one the config holds.
    ZNRecord working = CheckedMutationExecutor.deepCopy(current);
    InstanceConfig config = new InstanceConfig(working);
    InstanceConstants.InstanceOperation desired = request.getOperation();
    EffectiveInstanceOperation observed = describeOperation(config, request.getSource(), desired);

    Optional<CheckedMutationConflictReason> expectationFailure =
        request.getNodeExpectation().check(stat);
    if (expectationFailure.isPresent()) {
      return CheckedMutationExecutor.Decision.conflict(expectationFailure.get(),
          "Instance " + instanceName + " config is at version " + stat.getVersion()
              + " and creation id " + stat.getCzxid() + ", which does not match "
              + request.getNodeExpectation() + ". Nothing was written.", observed);
    }

    Optional<String> unreadable = findUnreadableRecordedOperation(config);
    if (unreadable.isPresent()) {
      return CheckedMutationExecutor.Decision.conflict(
          CheckedMutationConflictReason.UNREADABLE_STATE,
          "Instance " + instanceName + " records an operation this version cannot read: "
              + unreadable.get() + ". Writing would mean deciding against state whose meaning "
              + "is unknown, and could drop it, so nothing was written.", observed);
    }

    if (request.getExpectedOperation() != null
        && observed.getOperation() != request.getExpectedOperation()) {
      return CheckedMutationExecutor.Decision.conflict(
          CheckedMutationConflictReason.EXPECTED_STATE_MISMATCH,
          "Instance " + instanceName + " has operation " + observed.getOperation()
              + " in effect, but the request expected " + request.getExpectedOperation()
              + ". Nothing was written.", observed);
    }
    if (request.getExpectedOperationSource() != null
        && observed.getSource() != request.getExpectedOperationSource()) {
      return CheckedMutationExecutor.Decision.conflict(
          CheckedMutationConflictReason.EXPECTED_STATE_MISMATCH,
          "The operation in effect on instance " + instanceName + " records source "
              + observed.getSource() + ", but the request expected "
              + request.getExpectedOperationSource() + ". Nothing was written.", observed);
    }

    if (isAlreadySatisfied(config, request, observed)) {
      return CheckedMutationExecutor.Decision.unchanged(observed,
          "Instance " + instanceName + " already has operation " + desired + " recorded for "
              + request.getSource() + " and in effect. Nothing was written, so no recorded "
              + "operation moved and no timestamp changed.");
    }

    try {
      InstanceUtil.validateInstanceOperationTransition(accessor, clusterName, config,
          observed.getOperation(), desired);
    } catch (HelixException e) {
      return CheckedMutationExecutor.Decision.conflict(
          CheckedMutationConflictReason.INVALID_TRANSITION,
          "Instance " + instanceName + " cannot move from " + observed.getOperation() + " to "
              + desired + ": " + e.getMessage() + " Nothing was written.", observed);
    }

    boolean helixEnabled = isHelixEnabled(config);
    boolean legacyDisableOwned = isLegacyDisableOwnedBy(config, request.getSource());
    boolean mirrorToLegacyFields;
    if (desired == InstanceConstants.InstanceOperation.DISABLE) {
      // Disabling only ever tightens the deprecated state, so it is safe to write it when the
      // instance is still enabled, or to refresh an annotation this source already owns. A
      // disable written by somebody else keeps its annotation, since overwriting it would
      // erase why that writer disabled the instance while it stays disabled.
      mirrorToLegacyFields = request.getLegacyFieldPolicy() == LegacyFieldPolicy.ALLOW_OVERRIDE
          || helixEnabled || legacyDisableOwned;
    } else if (desired == InstanceConstants.InstanceOperation.ENABLE && !helixEnabled) {
      if (!legacyDisableOwned
          && request.getLegacyFieldPolicy() != LegacyFieldPolicy.ALLOW_OVERRIDE) {
        return CheckedMutationExecutor.Decision.conflict(
            CheckedMutationConflictReason.LEGACY_STATE_NOT_OWNED,
            "Instance " + instanceName + " is disabled through the deprecated enabled flag, and "
                + "that disable cannot be attributed to source " + request.getSource()
                + ". Enabling would revoke a disable this caller does not own, so nothing was "
                + "written. Resolve the deprecated state explicitly, or repeat the request with "
                + "the override policy if this caller is the authority for the instance.",
            observed);
      }
      mirrorToLegacyFields = true;
    } else {
      // EVACUATE, SWAP_IN and UNKNOWN have no representation in the deprecated fields, and an
      // ENABLE on an already enabled instance has nothing to mirror.
      mirrorToLegacyFields = false;
    }

    InstanceConfig.InstanceOperation operation =
        new InstanceConfig.InstanceOperation.Builder().setOperation(desired)
            .setSource(request.getSource()).setReason(request.getReason()).build();
    config.setInstanceOperationForSource(operation, mirrorToLegacyFields);

    EffectiveInstanceOperation afterWrite =
        describeOperation(config, request.getSource(), desired);
    if (!afterWrite.isDesiredStateInEffect() && request.isRequireDesiredStateInEffect()) {
      return CheckedMutationExecutor.Decision.conflict(
          CheckedMutationConflictReason.DESIRED_STATE_BLOCKED,
          "Recording " + desired + " for source " + request.getSource() + " on instance "
              + instanceName + " would leave " + afterWrite.getOperation()
              + " in effect instead, because " + describeBlocker(config, afterWrite, helixEnabled)
              + ". Nothing was written.", observed);
    }

    return CheckedMutationExecutor.Decision.apply(config.getRecord(), afterWrite,
        "Recorded " + desired + " for source " + request.getSource() + " on instance "
            + instanceName + ". Operation in effect: " + afterWrite.getOperation() + ".");
  }

  /**
   * A request is already satisfied when this source has the same operation and reason
   * recorded and that operation is the one in effect. Writing again would only move this
   * source's entry to the end of the recorded operations and refresh its timestamp, which
   * changes which entry is active without changing anything the caller asked for.
   *
   * <p>An ENABLE with no reason is also satisfied when this source has nothing recorded and
   * the instance is enabled: an ENABLE entry states that this source is not objecting, which
   * is exactly what having no entry already states. A caller that passes a reason is asking
   * for that annotation to be recorded, so it is written.
   */
  private static boolean isAlreadySatisfied(InstanceConfig config,
      InstanceOperationChangeRequest request, EffectiveInstanceOperation observed) {
    if (!observed.isDesiredStateInEffect()) {
      return false;
    }
    InstanceConfig.InstanceOperation recorded =
        findRecordedOperation(config, request.getSource());
    boolean intentRecorded;
    if (recorded == null) {
      intentRecorded = request.getOperation() == InstanceConstants.InstanceOperation.ENABLE
          && request.getReason().isEmpty();
    } else {
      intentRecorded = safeOperation(recorded) == request.getOperation()
          && recorded.getReason().equals(request.getReason());
    }
    if (!intentRecorded) {
      return false;
    }
    return !request.isRequireRequestedSourceActive() || isRequestedSourceActive(config, request);
  }

  /**
   * True when the operation in effect is the one this source recorded, rather than one
   * recorded by another source or one derived from the deprecated enabled flag.
   */
  private static boolean isRequestedSourceActive(InstanceConfig config,
      InstanceOperationChangeRequest request) {
    List<InstanceConfig.InstanceOperation> recorded = config.getAllInstanceOperations();
    if (recorded.isEmpty()) {
      return false;
    }
    InstanceConfig.InstanceOperation active = recorded.get(recorded.size() - 1);
    // A deprecated flag override replaces the active entry's operation, in which case the
    // state in effect is not this source's entry even if this source recorded the last one.
    return safeSource(active) == request.getSource()
        && safeOperation(active) == safeOperation(config.getInstanceOperation());
  }

  private static String describeBlocker(InstanceConfig config,
      EffectiveInstanceOperation afterWrite, boolean helixEnabled) {
    if (!helixEnabled) {
      return "the deprecated enabled flag is false and overrides it";
    }
    List<InstanceConfig.InstanceOperation> recorded = config.getAllInstanceOperations();
    if (!recorded.isEmpty()) {
      InstanceConfig.InstanceOperation active = recorded.get(recorded.size() - 1);
      return "source " + safeSourceName(active) + " has " + afterWrite.getOperation()
          + " recorded after it";
    }
    return "another writer's state takes precedence";
  }

  private static CheckedMutationExecutor.Decision<EffectiveDisabledPartitions>
      evaluateDisabledPartitions(String instanceName, DisabledPartitionsChangeRequest request,
      ZNRecord current, Stat stat) {
    ZNRecord working = CheckedMutationExecutor.deepCopy(current);
    InstanceConfig config = new InstanceConfig(working);
    EffectiveDisabledPartitions observed = describeDisabledPartitions(config, request);

    Optional<CheckedMutationConflictReason> expectationFailure =
        request.getNodeExpectation().check(stat);
    if (expectationFailure.isPresent()) {
      return CheckedMutationExecutor.Decision.conflict(expectationFailure.get(),
          "Instance " + instanceName + " config is at version " + stat.getVersion()
              + " and creation id " + stat.getCzxid() + ", which does not match "
              + request.getNodeExpectation() + ". Nothing was written.", observed);
    }

    Set<String> effective = storedPartitions(config, request.getResource());
    if (request.getExpectedDisabledPartitions() != null
        && !effective.equals(new HashSet<>(request.getExpectedDisabledPartitions()))) {
      return CheckedMutationExecutor.Decision.conflict(
          CheckedMutationConflictReason.EXPECTED_STATE_MISMATCH,
          "Resource " + request.getResource() + " on instance " + instanceName
              + " has disabled partitions " + effective + ", but the request expected "
              + request.getExpectedDisabledPartitions() + ". Nothing was written.", observed);
    }

    if (!request.isDisabled()
        && request.getLegacyFieldPolicy() == LegacyFieldPolicy.OWNED_ONLY) {
      Set<String> blockedByCrossResource = new LinkedHashSet<>(request.getPartitions());
      blockedByCrossResource.retainAll(observed.getCrossResourceDisabledPartitions());
      if (!blockedByCrossResource.isEmpty()) {
        return CheckedMutationExecutor.Decision.conflict(
            CheckedMutationConflictReason.LEGACY_STATE_NOT_OWNED,
            "Partitions " + blockedByCrossResource + " on instance " + instanceName
                + " are disabled through the deprecated list that applies to every resource, so "
                + "enabling them for resource " + request.getResource()
                + " would also enable them for every other resource. Nothing was written.",
            observed);
      }
    }

    Map<String, Map<String, String>> mapFieldsBefore = copyMapFields(config.getRecord());
    Map<String, List<String>> listFieldsBefore = copyListFields(config.getRecord());
    for (String partition : request.getPartitions()) {
      config.setInstanceEnabledForPartition(request.getResource(), partition,
          !request.isDisabled());
    }

    // The no-op test is structural on purpose: whether the requested state is already stored
    // depends on how the data model records it, which differs for a resource key that carries
    // no partition names. Comparing what the change would store to what is stored cannot drift
    // from that, and it guarantees a no-op never writes.
    EffectiveDisabledPartitions afterChange = describeDisabledPartitions(config, request);
    if (mapFieldsBefore.equals(config.getRecord().getMapFields())
        && listFieldsBefore.equals(config.getRecord().getListFields())) {
      return CheckedMutationExecutor.Decision.unchanged(afterChange,
          "Partitions " + request.getPartitions() + " of resource " + request.getResource()
              + " on instance " + instanceName + " are already "
              + (request.isDisabled() ? "disabled" : "enabled") + ". Nothing was written.");
    }

    if (!afterChange.isDesiredStateInEffect()) {
      return CheckedMutationExecutor.Decision.conflict(
          CheckedMutationConflictReason.DESIRED_STATE_BLOCKED,
          "Partitions " + request.getPartitions() + " of resource " + request.getResource()
              + " on instance " + instanceName + " would still not be "
              + (request.isDisabled() ? "disabled" : "enabled")
              + " after this change, so nothing was written. Disabled partitions in effect: "
              + afterChange.getDisabledPartitions(request.getResource()) + ".", observed);
    }

    return CheckedMutationExecutor.Decision.apply(config.getRecord(), afterChange,
        (request.isDisabled() ? "Disabled " : "Enabled ") + request.getPartitions()
            + " for resource " + request.getResource() + " on instance " + instanceName + ".");
  }

  private static EffectiveInstanceOperation describeOperation(InstanceConfig config,
      InstanceConstants.InstanceOperationSource requestedSource,
      InstanceConstants.InstanceOperation desired) {
    InstanceConfig.InstanceOperation effective = config.getInstanceOperation();
    InstanceConstants.InstanceOperation effectiveOperation = safeOperation(effective);
    InstanceConfig.InstanceOperation recordedForSource =
        findRecordedOperation(config, requestedSource);
    List<InstanceConstants.InstanceOperationSource> recordedSources =
        config.getAllInstanceOperations().stream().map(CheckedInstanceChanges::safeSource)
            .filter(Objects::nonNull).collect(Collectors.toList());

    return new EffectiveInstanceOperation(effectiveOperation, safeSourceOrDefault(effective),
        effective.getReason(),
        recordedForSource == null ? null : safeOperation(recordedForSource),
        isHelixEnabled(config), effectiveOperation == desired, recordedSources);
  }

  private static EffectiveDisabledPartitions describeDisabledPartitions(InstanceConfig config,
      DisabledPartitionsChangeRequest request) {
    Map<String, List<String>> disabledPartitions =
        new LinkedHashMap<>(config.getDisabledPartitionsMap());
    List<String> forResource = config.getDisabledPartitions(request.getResource());
    if (forResource != null && !disabledPartitions.containsKey(request.getResource())) {
      // The resource can be absent from the per resource map and still have partitions
      // disabled through the deprecated cross resource list.
      disabledPartitions.put(request.getResource(), new ArrayList<>(forResource));
    }

    Set<String> crossResourceSet = new LinkedHashSet<>();
    List<String> crossResource = config.getRecord()
        .getListField(InstanceConfig.InstanceConfigProperty.HELIX_DISABLED_PARTITION.name());
    if (crossResource != null) {
      crossResourceSet.addAll(crossResource);
    }

    Set<String> stored = storedPartitions(config, request.getResource());
    boolean desiredInEffect = request.isDisabled() ? stored.containsAll(request.getPartitions())
        : request.getPartitions().stream().noneMatch(stored::contains);

    return new EffectiveDisabledPartitions(disabledPartitions, crossResourceSet, desiredInEffect);
  }

  /**
   * The partitions recorded as disabled for one resource, read from the stored form rather
   * than through the effective view.
   *
   * <p>The two differ for a resource key whose partition list is empty, which is how a marker
   * that carries no partition names is stored: the effective view reports no partitions for it,
   * while the stored form still distinguishes a key holding an empty name from a key that is
   * not there. A change has to be decided on the stored form, otherwise such a marker would
   * look absent and be written over and over.
   */
  private static Set<String> storedPartitions(InstanceConfig config, String resource) {
    Set<String> stored = new LinkedHashSet<>();
    Map<String, String> perResource = config.getRecord()
        .getMapField(InstanceConfig.InstanceConfigProperty.HELIX_DISABLED_PARTITION.name());
    if (perResource != null && perResource.containsKey(resource)) {
      stored.addAll(Arrays.asList(perResource.get(resource).split(",", -1)));
    }
    List<String> crossResource = config.getRecord()
        .getListField(InstanceConfig.InstanceConfigProperty.HELIX_DISABLED_PARTITION.name());
    if (crossResource != null) {
      stored.addAll(crossResource);
    }
    return stored;
  }

  private static Map<String, Map<String, String>> copyMapFields(ZNRecord record) {
    Map<String, Map<String, String>> copy = new LinkedHashMap<>();
    record.getMapFields().forEach((key, value) -> copy.put(key,
        value == null ? null : new LinkedHashMap<>(value)));
    return copy;
  }

  private static Map<String, List<String>> copyListFields(ZNRecord record) {
    Map<String, List<String>> copy = new LinkedHashMap<>();
    record.getListFields().forEach((key, value) -> copy.put(key,
        value == null ? null : new ArrayList<>(value)));
    return copy;
  }

  /**
   * Whether the deprecated disable fields can be attributed to {@code source}.
   *
   * <p>They are written together with the operation that caused them, carrying that
   * operation's timestamp and reason, so a recorded operation owns them when it is the only
   * disable whose timestamp and reason match what those fields hold. Anything else, including
   * two candidates written in the same millisecond with the same reason and a disable with no
   * recorded operation behind it at all, counts as not owned. The source recorded on an
   * operation is deliberately not used as the evidence, because any writer can record any
   * source.
   */
  private static boolean isLegacyDisableOwnedBy(InstanceConfig config,
      InstanceConstants.InstanceOperationSource source) {
    if (isHelixEnabled(config)) {
      return false;
    }
    long legacyTimestamp = config.getInstanceEnabledTime();
    String legacyReason = config.getRecord()
        .getSimpleField(InstanceConfig.InstanceConfigProperty.HELIX_DISABLED_REASON.name());

    List<InstanceConfig.InstanceOperation> candidates = config.getAllInstanceOperations().stream()
        .filter(op -> safeOperation(op) == InstanceConstants.InstanceOperation.DISABLE)
        .filter(op -> Objects.equals(safeTimestamp(op), legacyTimestamp))
        .filter(op -> legacyReason == null || legacyReason.equals(op.getReason()))
        .collect(Collectors.toList());

    return candidates.size() == 1 && safeSource(candidates.get(0)) == source;
  }

  /**
   * Describes the first recorded operation this version cannot read well enough to change the
   * instance safely, if there is one.
   *
   * <p>An entry that fails to deserialise is dropped when the recorded operations are read, so
   * writing them back would silently discard another writer's state. An operation type, source
   * or timestamp that cannot be read leaves no way to order the entry, to tell whether it
   * belongs to the caller, or to attribute the deprecated fields to it. Both are refused
   * rather than guessed at: a newer writer's operation can mean something this version has no
   * way to respect.
   */
  private static Optional<String> findUnreadableRecordedOperation(InstanceConfig config) {
    List<String> stored = config.getRecord()
        .getListField(InstanceConfig.InstanceConfigProperty.HELIX_INSTANCE_OPERATIONS.name());
    List<InstanceConfig.InstanceOperation> readable = config.getAllInstanceOperations();
    int storedCount = stored == null ? 0 : stored.size();
    if (storedCount != readable.size()) {
      return Optional.of(
          storedCount + " operations are stored but only " + readable.size() + " could be read");
    }
    for (InstanceConfig.InstanceOperation operation : readable) {
      if (safeSource(operation) == null) {
        return Optional.of("an operation with an unrecognised source");
      }
      if (!isOperationTypeReadable(operation)) {
        return Optional.of("an operation from source " + safeSourceName(operation)
            + " with an unrecognised type");
      }
      if (safeTimestamp(operation) == null) {
        return Optional.of(
            "an operation from source " + safeSourceName(operation) + " with no readable "
                + "timestamp");
      }
    }
    return Optional.empty();
  }

  private static boolean isOperationTypeReadable(InstanceConfig.InstanceOperation operation) {
    try {
      operation.getOperation();
      return true;
    } catch (IllegalArgumentException e) {
      return false;
    }
  }

  @Nullable
  private static InstanceConfig.InstanceOperation findRecordedOperation(InstanceConfig config,
      InstanceConstants.InstanceOperationSource source) {
    return config.getAllInstanceOperations().stream().filter(op -> safeSource(op) == source)
        .reduce((first, second) -> second).orElse(null);
  }

  private static boolean isHelixEnabled(InstanceConfig config) {
    return config.getRecord()
        .getBooleanField(InstanceConfig.InstanceConfigProperty.HELIX_ENABLED.name(), true);
  }

  /**
   * Read an operation written by a version of Helix that may know operations this one does
   * not. An unrecognised value is reported as UNKNOWN rather than throwing, which keeps it
   * visible to the caller and keeps it from matching anything the caller asked for.
   */
  private static InstanceConstants.InstanceOperation safeOperation(
      InstanceConfig.InstanceOperation operation) {
    try {
      return operation.getOperation();
    } catch (IllegalArgumentException e) {
      return InstanceConstants.InstanceOperation.UNKNOWN;
    }
  }

  /**
   * Read a source written by a version of Helix that may know sources this one does not. An
   * unrecognised value is reported as null so that it never accidentally matches the source
   * the request names.
   */
  @Nullable
  private static InstanceConstants.InstanceOperationSource safeSource(
      InstanceConfig.InstanceOperation operation) {
    try {
      return operation.getSource();
    } catch (IllegalArgumentException e) {
      return null;
    }
  }

  /**
   * Read the timestamp of a recorded operation, reporting null when it is missing or not a
   * number rather than throwing, so a malformed entry becomes a refusal instead of a failure.
   */
  @Nullable
  private static Long safeTimestamp(InstanceConfig.InstanceOperation operation) {
    try {
      return operation.getTimestamp();
    } catch (NumberFormatException | NullPointerException e) {
      return null;
    }
  }

  private static InstanceConstants.InstanceOperationSource safeSourceOrDefault(
      InstanceConfig.InstanceOperation operation) {
    InstanceConstants.InstanceOperationSource source = safeSource(operation);
    return source == null ? InstanceConstants.InstanceOperationSource.DEFAULT : source;
  }

  private static String safeSourceName(InstanceConfig.InstanceOperation operation) {
    InstanceConstants.InstanceOperationSource source = safeSource(operation);
    return source == null ? "an unrecognised source" : source.name();
  }
}
