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

import java.util.Objects;
import java.util.Optional;

import javax.annotation.Nullable;

/**
 * Result of a checked (conditional) mutation.
 *
 * <p>The result always carries the state the decision was based on, or the state the write
 * produced, so callers never need a second read to learn what is now in effect. A caller must
 * branch on {@link #getOutcome()}: {@link CheckedMutationOutcome#CONFLICT} and
 * {@link CheckedMutationOutcome#NOT_FOUND} are normal answers, not transport failures.
 *
 * @param <S> the effective state this mutation reports.
 */
public final class CheckedMutationResult<S> {
  /** Reported when the node version or creation id could not be observed. */
  public static final int UNKNOWN_VERSION = -1;
  /** Reported when the node creation id could not be observed. */
  public static final long UNKNOWN_CREATION_ID = -1L;

  private final CheckedMutationOutcome _outcome;
  private final CheckedMutationConflictReason _conflictReason;
  private final String _message;
  private final S _effectiveState;
  private final int _observedVersion;
  private final long _observedCreationId;

  private CheckedMutationResult(CheckedMutationOutcome outcome,
      @Nullable CheckedMutationConflictReason conflictReason, String message,
      @Nullable S effectiveState, int observedVersion, long observedCreationId) {
    _outcome = Objects.requireNonNull(outcome, "outcome must not be null");
    _conflictReason = conflictReason;
    _message = Objects.requireNonNull(message, "message must not be null");
    _effectiveState = effectiveState;
    _observedVersion = observedVersion;
    _observedCreationId = observedCreationId;
  }

  public static <S> CheckedMutationResult<S> applied(String message, S effectiveState,
      int version, long creationId) {
    return new CheckedMutationResult<>(CheckedMutationOutcome.APPLIED, null, message,
        effectiveState, version, creationId);
  }

  public static <S> CheckedMutationResult<S> unchanged(String message, S effectiveState,
      int version, long creationId) {
    return new CheckedMutationResult<>(CheckedMutationOutcome.UNCHANGED, null, message,
        effectiveState, version, creationId);
  }

  public static <S> CheckedMutationResult<S> conflict(CheckedMutationConflictReason reason,
      String message, @Nullable S effectiveState, int version, long creationId) {
    return new CheckedMutationResult<>(CheckedMutationOutcome.CONFLICT,
        Objects.requireNonNull(reason, "conflict reason must not be null"), message,
        effectiveState, version, creationId);
  }

  public static <S> CheckedMutationResult<S> notFound(String message) {
    return new CheckedMutationResult<>(CheckedMutationOutcome.NOT_FOUND, null, message, null,
        UNKNOWN_VERSION, UNKNOWN_CREATION_ID);
  }

  public CheckedMutationOutcome getOutcome() {
    return _outcome;
  }

  /**
   * @return the conflict reason, present exactly when the outcome is
   *     {@link CheckedMutationOutcome#CONFLICT}.
   */
  public Optional<CheckedMutationConflictReason> getConflictReason() {
    return Optional.ofNullable(_conflictReason);
  }

  /**
   * @return a human readable explanation, never null and never empty.
   */
  public String getMessage() {
    return _message;
  }

  /**
   * @return the state in effect: after the write for {@link CheckedMutationOutcome#APPLIED},
   *     and as observed at mutation time otherwise. Null only for
   *     {@link CheckedMutationOutcome#NOT_FOUND}.
   */
  @Nullable
  public S getEffectiveState() {
    return _effectiveState;
  }

  /**
   * @return the node data version this write produced for
   *     {@link CheckedMutationOutcome#APPLIED}, or the version the decision was based on
   *     otherwise. {@link #UNKNOWN_VERSION} when the node was not observed. The version and the
   *     reported state always describe each other, so a caller can chain the next conditional
   *     change on it.
   */
  public int getObservedVersion() {
    return _observedVersion;
  }

  /**
   * @return the node creation id belonging to the version above, or
   *     {@link #UNKNOWN_CREATION_ID} when the node was not observed. Comparing this value
   *     across calls detects a node that was deleted and recreated in between.
   */
  public long getObservedCreationId() {
    return _observedCreationId;
  }

  public boolean isApplied() {
    return _outcome == CheckedMutationOutcome.APPLIED;
  }

  public boolean isUnchanged() {
    return _outcome == CheckedMutationOutcome.UNCHANGED;
  }

  public boolean isConflict() {
    return _outcome == CheckedMutationOutcome.CONFLICT;
  }

  /**
   * @return true when the request needed no write or produced one, that is when the caller's
   *     intent is recorded. This is deliberately not "the desired state is in effect": that
   *     question is answered by the domain specific effective state.
   */
  public boolean isAppliedOrUnchanged() {
    return isApplied() || isUnchanged();
  }

  @Override
  public String toString() {
    return "CheckedMutationResult{outcome=" + _outcome + ", conflictReason=" + _conflictReason
        + ", version=" + _observedVersion + ", creationId=" + _observedCreationId + ", message="
        + _message + ", effectiveState=" + _effectiveState + "}";
  }
}
