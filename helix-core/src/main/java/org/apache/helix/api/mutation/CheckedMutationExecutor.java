package org.apache.helix.api.mutation;

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
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

import javax.annotation.Nullable;

import org.apache.helix.AccessOption;
import org.apache.helix.BaseDataAccessor;
import org.apache.helix.zookeeper.datamodel.ZNRecord;
import org.apache.helix.zookeeper.zkclient.exception.ZkBadVersionException;
import org.apache.helix.zookeeper.zkclient.exception.ZkNoNodeException;
import org.apache.zookeeper.data.Stat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Runs a conditional single-node mutation: read the node once, let an
 * {@link Evaluator} decide on exactly that content and metadata, and write only when the
 * evaluator asks for it, conditioned on the version that was evaluated.
 *
 * <p><b>The guarantee.</b> A precondition expressed over the node content, and the mutation
 * derived from it, are atomic with respect to every other writer of that node: the decision
 * is taken on the pair (content, version) and the write is conditioned on that same version,
 * so no interleaved write can land in between. A writer that loses the race does not corrupt
 * anything; the mutation is simply re-evaluated on the new content, up to a bounded number of
 * attempts, after which {@link CheckedMutationConflictReason#CONCURRENT_MODIFICATION} is
 * returned and retrying stays safe.
 *
 * <p><b>Genuine no-ops cost nothing.</b> {@link Decision#unchanged} and
 * {@link Decision#conflict} write nothing at all, so the node keeps its version, its
 * timestamps and the order of everything stored in it.
 *
 * <p><b>What this does not give you.</b> The node is identified by path. If it is deleted and
 * recreated, the metadata store restarts its versions, so a version-conditioned write can in
 * principle land on the new incarnation. {@link NodeExpectation#ofCreationId(long)} rejects a
 * node that was already recreated when the mutation is evaluated, and the result reports the
 * creation id observed after the write so a caller can detect the remaining narrow window,
 * but there is no creation-id-conditional write to close it. Do not present this as an atomic
 * identity fence. Callers that need one have to serialise all writers through a lease or lock
 * protocol, which is a protocol change beyond a conditional write. This class also says
 * nothing about the liveness or the incarnation of whatever process the node describes;
 * checks of that kind belong to the caller.
 */
public final class CheckedMutationExecutor {
  private static final Logger LOG = LoggerFactory.getLogger(CheckedMutationExecutor.class);

  /**
   * How many times a mutation is re-evaluated when another writer wins the conditional write.
   * Kept small on purpose: a caller that loses this often is contending with a writer that is
   * not going away, and should hear about it instead of spinning.
   */
  static final int MAX_ATTEMPTS = 5;

  private CheckedMutationExecutor() {
  }

  /**
   * Copy a record so an {@link Evaluator} can mutate it without touching what it was given.
   *
   * <p>{@link ZNRecord#ZNRecord(ZNRecord)} copies the field maps but shares the lists and maps
   * stored inside them, and several data model setters mutate those in place. An evaluator
   * that used the shallow copy would therefore change the content it is deciding on, and
   * would corrupt a cached record when the accessor serves one.
   */
  public static ZNRecord deepCopy(ZNRecord record) {
    Objects.requireNonNull(record, "record must not be null");
    ZNRecord copy = new ZNRecord(record.getId());
    copy.setSimpleFields(new HashMap<>(record.getSimpleFields()));

    Map<String, List<String>> listFields = new HashMap<>();
    for (Map.Entry<String, List<String>> entry : record.getListFields().entrySet()) {
      listFields.put(entry.getKey(),
          entry.getValue() == null ? null : new ArrayList<>(entry.getValue()));
    }
    copy.setListFields(listFields);

    Map<String, Map<String, String>> mapFields = new HashMap<>();
    for (Map.Entry<String, Map<String, String>> entry : record.getMapFields().entrySet()) {
      mapFields.put(entry.getKey(),
          entry.getValue() == null ? null : new HashMap<>(entry.getValue()));
    }
    copy.setMapFields(mapFields);

    if (record.getRawPayload() != null) {
      copy.setRawPayload(Arrays.copyOf(record.getRawPayload(), record.getRawPayload().length));
    }
    return copy;
  }

  /**
   * Decides what a checked mutation should do with the node content it was given.
   *
   * @param <S> the effective state reported back to the caller.
   */
  public interface Evaluator<S> {
    /**
     * @param current the node content read for this attempt. Never null: a missing node is
     *     reported as {@link CheckedMutationOutcome#NOT_FOUND} without calling the evaluator.
     *     Implementations must not mutate it; they mutate a copy and return it with
     *     {@link Decision#apply}.
     * @param stat the node metadata read together with {@code current}. The write, if any, is
     *     conditioned on {@code stat.getVersion()}.
     */
    Decision<S> evaluate(ZNRecord current, Stat stat);
  }

  /**
   * What an {@link Evaluator} decided.
   *
   * @param <S> the effective state reported back to the caller.
   */
  public static final class Decision<S> {
    private enum Type {
      APPLY, UNCHANGED, CONFLICT
    }

    private final Type _type;
    private final ZNRecord _newRecord;
    private final S _effectiveState;
    private final CheckedMutationConflictReason _conflictReason;
    private final String _message;

    private Decision(Type type, @Nullable ZNRecord newRecord, @Nullable S effectiveState,
        @Nullable CheckedMutationConflictReason conflictReason, String message) {
      _type = type;
      _newRecord = newRecord;
      _effectiveState = effectiveState;
      _conflictReason = conflictReason;
      _message = message;
    }

    /**
     * Write {@code newRecord}, conditioned on the version that was evaluated.
     *
     * @param newRecord the full replacement content, derived from a copy of what was read.
     * @param effectiveState the state this write produces, reported when it succeeds.
     * @param message a human readable description of what was applied.
     */
    public static <S> Decision<S> apply(ZNRecord newRecord, S effectiveState, String message) {
      return new Decision<>(Type.APPLY,
          Objects.requireNonNull(newRecord, "newRecord must not be null"),
          Objects.requireNonNull(effectiveState, "effectiveState must not be null"), null,
          Objects.requireNonNull(message, "message must not be null"));
    }

    /**
     * Write nothing because the intent is already satisfied.
     */
    public static <S> Decision<S> unchanged(S effectiveState, String message) {
      return new Decision<>(Type.UNCHANGED, null,
          Objects.requireNonNull(effectiveState, "effectiveState must not be null"), null,
          Objects.requireNonNull(message, "message must not be null"));
    }

    /**
     * Write nothing because a precondition does not hold.
     */
    public static <S> Decision<S> conflict(CheckedMutationConflictReason reason, String message,
        @Nullable S effectiveState) {
      return new Decision<>(Type.CONFLICT, null, effectiveState,
          Objects.requireNonNull(reason, "reason must not be null"),
          Objects.requireNonNull(message, "message must not be null"));
    }
  }

  /**
   * Execute a checked mutation against a single node.
   *
   * @param accessor accessor for the metadata store holding the node.
   * @param path the node path. It is never created by this call.
   * @param evaluator decides, on freshly read content, whether to write.
   * @return the outcome together with the state it was decided on or produced.
   */
  public static <S> CheckedMutationResult<S> execute(BaseDataAccessor<ZNRecord> accessor,
      String path, Evaluator<S> evaluator) {
    Objects.requireNonNull(accessor, "accessor must not be null");
    Objects.requireNonNull(path, "path must not be null");
    Objects.requireNonNull(evaluator, "evaluator must not be null");

    for (int attempt = 1; attempt <= MAX_ATTEMPTS; attempt++) {
      Stat stat = new Stat();
      ZNRecord current;
      try {
        current = accessor.get(path, stat, AccessOption.PERSISTENT);
      } catch (ZkNoNodeException e) {
        return CheckedMutationResult.notFound("Node " + path + " does not exist");
      }
      if (current == null) {
        return CheckedMutationResult.notFound("Node " + path + " does not exist");
      }

      Decision<S> decision = evaluator.evaluate(current, stat);
      switch (decision._type) {
        case UNCHANGED:
          return CheckedMutationResult.unchanged(decision._message, decision._effectiveState,
              stat.getVersion(), stat.getCzxid());
        case CONFLICT:
          return CheckedMutationResult.conflict(decision._conflictReason, decision._message,
              decision._effectiveState, stat.getVersion(), stat.getCzxid());
        case APPLY:
        default:
          break;
      }

      boolean written;
      try {
        written = accessor.set(path, decision._newRecord, stat.getVersion(),
            AccessOption.PERSISTENT);
      } catch (ZkBadVersionException e) {
        written = false;
      }

      if (written) {
        return appliedResult(accessor, path, stat, decision);
      }
      LOG.debug("Conditional write on {} lost to a concurrent writer at version {}, attempt {} of "
          + "{}. Re-evaluating on fresh content.", path, stat.getVersion(), attempt, MAX_ATTEMPTS);
    }

    return CheckedMutationResult.conflict(CheckedMutationConflictReason.CONCURRENT_MODIFICATION,
        "Another writer modified " + path + " during each of the " + MAX_ATTEMPTS
            + " attempts, so nothing was written. Retrying is safe.", null,
        CheckedMutationResult.UNKNOWN_VERSION, CheckedMutationResult.UNKNOWN_CREATION_ID);
  }

  /**
   * Build the applied result.
   *
   * <p>The version reported is the one this write produced: the metadata store increments the
   * data version by exactly one per write, and the conditional write succeeded from the version
   * that was evaluated. Reporting whatever a read back happens to see instead would hand the
   * caller a version belonging to content it was never shown, and a later conditional change
   * expecting that version would then accept another writer's state as its own.
   *
   * <p>The read back is only used to notice that the node was deleted and recreated around
   * this write, which is the window an expected creation id cannot close by itself.
   */
  private static <S> CheckedMutationResult<S> appliedResult(BaseDataAccessor<ZNRecord> accessor,
      String path, Stat evaluatedStat, Decision<S> decision) {
    int version = evaluatedStat.getVersion() + 1;
    long creationId = evaluatedStat.getCzxid();
    boolean identityChanged = false;
    Stat afterWrite = new Stat();
    try {
      if (accessor.get(path, afterWrite, AccessOption.PERSISTENT) == null) {
        // The node was removed after our write landed. The write itself did happen, so the
        // outcome stays APPLIED and the caller is told which version it produced.
        LOG.warn("Node {} disappeared right after a checked write at version {}.", path, version);
      } else {
        identityChanged = afterWrite.getCzxid() != creationId;
      }
    } catch (ZkNoNodeException e) {
      LOG.warn("Node {} disappeared right after a checked write at version {}.", path, version);
    }

    if (identityChanged) {
      LOG.warn("Node {} carries creation id {} after a checked write that was evaluated against "
              + "creation id {}. The node was deleted and recreated around this write.", path,
          afterWrite.getCzxid(), creationId);
      return CheckedMutationResult.applied(decision._message
              + " Warning: the node creation id changed around this write, so it was deleted and "
              + "recreated. Re-read before trusting the reported state.", decision._effectiveState,
          version, creationId);
    }
    return CheckedMutationResult.applied(decision._message, decision._effectiveState, version,
        creationId);
  }
}
