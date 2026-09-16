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
import java.util.List;

/**
 * The outcome of a pair-scoped swap preparation or completion.
 * <p>
 * The outcome is a single explicit {@link Status} rather than a boolean, because the reasons a
 * swap call does not take effect are not interchangeable. A caller has to be able to tell a
 * harmless replay of a call it already made apart from a genuine conflict with another writer,
 * apart from replicas not being ready yet, apart from the named pair no longer being a pair.
 * <p>
 * {@link #getObservedSwapOutIdentity()} and {@link #getObservedSwapInIdentity()} report the
 * config metadata observed while processing the request or after its transaction. These values
 * are not an atomic snapshot of both configs and do not confer ownership or incarnation fencing.
 */
public class SwapPairResult {

  /**
   * The explicit outcome of a pair-scoped swap call.
   */
  public enum Status {
    /**
     * The preparation was applied.
     */
    PREPARED(true),
    /**
     * The swap-in was already prepared exactly as requested. Nothing was written, so a repeated
     * preparation cannot re-apply an instance operation change that was already applied once.
     */
    ALREADY_PREPARED(true),
    /**
     * The completion was applied.
     */
    COMPLETED(true),
    /**
     * The named pair was already completed. Nothing was written, so a replayed completion cannot
     * move a config a second time.
     */
    ALREADY_COMPLETED(true),
    /**
     * Completion was refused because the swap is not ready. {@link #getBlockers()} lists why.
     * Retryable once the blockers clear.
     */
    NOT_READY(false),
    /**
     * The two named instances are not a swap pair in the requested mode, for example the swap-in no
     * longer carries the swap-out's logical id, the swap-in is not marked for the mode, or a third
     * instance shares the same logical id so the pairing is ambiguous. Not retryable without
     * operator attention.
     */
    PAIR_MISMATCH(false),
    /**
     * A config is no longer the node or revision the caller asserted, so the call was refused
     * without writing. Retryable after re-reading.
     */
    IDENTITY_MISMATCH(false),
    /**
     * An asserted version-zero identity was conservatively refused without writing. Other
     * versions still do not provide atomic creation-identity fencing. See
     * {@link org.apache.helix.HelixAdmin#completeSwapPair(String, SwapPairRequest)} for the exact
     * condition and the reason a conditional write alone cannot cover it.
     */
    IDENTITY_UNVERIFIABLE(false),
    /**
     * The conditional write lost to a concurrent change of one of the two configs. Nothing was
     * written. Retryable after re-reading.
     */
    CONFLICT(false),
    /**
     * The request or the cluster state it needs is missing or malformed, for example an unknown
     * instance or a swap-out with no logical id. Nothing was written.
     */
    INVALID_REQUEST(false),
    /**
     * The write failed for a reason other than a conflict. Whether it took effect is unknown, so a
     * caller must re-read before retrying.
     */
    FAILED(false);

    private final boolean _successful;

    Status(boolean successful) {
      _successful = successful;
    }

    /**
     * @return true when the requested end state holds now, either because this call established it
     *         or because it was already established.
     */
    public boolean isSuccessful() {
      return _successful;
    }
  }

  private final Status _status;
  private final List<String> _blockers;
  private final InstanceConfigIdentity _observedSwapOutIdentity;
  private final InstanceConfigIdentity _observedSwapInIdentity;

  private SwapPairResult(Builder builder) {
    _status = builder._status;
    _blockers = Collections.unmodifiableList(new ArrayList<>(builder._blockers));
    _observedSwapOutIdentity = builder._observedSwapOutIdentity;
    _observedSwapInIdentity = builder._observedSwapInIdentity;
  }

  public Status getStatus() {
    return _status;
  }

  /**
   * @return true when the requested end state holds now. It does not mean this call wrote
   *         anything; check {@link #getStatus()} to distinguish.
   */
  public boolean isSuccessful() {
    return _status.isSuccessful();
  }

  /**
   * @return the reasons the operation did not take effect, or an empty list on success.
   */
  public List<String> getBlockers() {
    return _blockers;
  }

  /**
   * @return the swap-out config identity this call acted on or refused on, or null when the config
   *         could not be read.
   */
  public InstanceConfigIdentity getObservedSwapOutIdentity() {
    return _observedSwapOutIdentity;
  }

  /**
   * @return the swap-in config identity this call acted on or refused on, or null when the config
   *         could not be read.
   */
  public InstanceConfigIdentity getObservedSwapInIdentity() {
    return _observedSwapInIdentity;
  }

  /**
   * Narrow this result to the older boolean-plus-blockers shape, for callers that only need to
   * know whether the requested end state holds. The explicit status is lost.
   */
  public OperationCheckResult toOperationCheckResult() {
    if (isSuccessful()) {
      return OperationCheckResult.success();
    }
    return OperationCheckResult.failed(_blockers.isEmpty()
        ? Collections.singletonList("Swap pair operation failed with status " + _status)
        : _blockers);
  }

  @Override
  public String toString() {
    return "SwapPairResult{status=" + _status + ", blockers=" + _blockers
        + ", observedSwapOutIdentity=" + _observedSwapOutIdentity + ", observedSwapInIdentity="
        + _observedSwapInIdentity + "}";
  }

  public static class Builder {
    private final Status _status;
    private final List<String> _blockers = new ArrayList<>();
    private InstanceConfigIdentity _observedSwapOutIdentity;
    private InstanceConfigIdentity _observedSwapInIdentity;

    public Builder(Status status) {
      _status = status;
    }

    public Builder addBlocker(String blocker) {
      if (blocker != null) {
        _blockers.add(blocker);
      }
      return this;
    }

    public Builder addBlockers(List<String> blockers) {
      if (blockers != null) {
        blockers.forEach(this::addBlocker);
      }
      return this;
    }

    public Builder setObservedSwapOutIdentity(InstanceConfigIdentity observedSwapOutIdentity) {
      _observedSwapOutIdentity = observedSwapOutIdentity;
      return this;
    }

    public Builder setObservedSwapInIdentity(InstanceConfigIdentity observedSwapInIdentity) {
      _observedSwapInIdentity = observedSwapInIdentity;
      return this;
    }

    public SwapPairResult build() {
      return new SwapPairResult(this);
    }
  }
}
