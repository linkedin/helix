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

import java.util.Objects;

import javax.annotation.Nullable;

import org.apache.helix.api.mutation.NodeExpectation;
import org.apache.helix.constants.InstanceConstants;

/**
 * A desired instance operation, together with the conditions under which it may be written.
 *
 * <p>The request describes an intent ("this source wants the instance in this operation"),
 * not a sequence of edits. The checked change decides at mutation time whether that intent is
 * already satisfied, can be satisfied, or is blocked, and reports which of the three happened.
 */
public final class InstanceOperationChangeRequest {
  private final InstanceConstants.InstanceOperation _operation;
  private final InstanceConstants.InstanceOperationSource _source;
  private final String _reason;
  private final NodeExpectation _nodeExpectation;
  private final InstanceConstants.InstanceOperation _expectedOperation;
  private final InstanceConstants.InstanceOperationSource _expectedOperationSource;
  private final boolean _requireDesiredStateInEffect;
  private final boolean _requireRequestedSourceActive;
  private final LegacyFieldPolicy _legacyFieldPolicy;

  private InstanceOperationChangeRequest(Builder builder) {
    _operation = builder._operation;
    _source = builder._source;
    _reason = builder._reason;
    _nodeExpectation = builder._nodeExpectation;
    _expectedOperation = builder._expectedOperation;
    _expectedOperationSource = builder._expectedOperationSource;
    _requireDesiredStateInEffect = builder._requireDesiredStateInEffect;
    _requireRequestedSourceActive = builder._requireRequestedSourceActive;
    _legacyFieldPolicy = builder._legacyFieldPolicy;
  }

  public InstanceConstants.InstanceOperation getOperation() {
    return _operation;
  }

  public InstanceConstants.InstanceOperationSource getSource() {
    return _source;
  }

  public String getReason() {
    return _reason;
  }

  public NodeExpectation getNodeExpectation() {
    return _nodeExpectation;
  }

  @Nullable
  public InstanceConstants.InstanceOperation getExpectedOperation() {
    return _expectedOperation;
  }

  @Nullable
  public InstanceConstants.InstanceOperationSource getExpectedOperationSource() {
    return _expectedOperationSource;
  }

  public boolean isRequireDesiredStateInEffect() {
    return _requireDesiredStateInEffect;
  }

  public boolean isRequireRequestedSourceActive() {
    return _requireRequestedSourceActive;
  }

  public LegacyFieldPolicy getLegacyFieldPolicy() {
    return _legacyFieldPolicy;
  }

  @Override
  public String toString() {
    return "InstanceOperationChangeRequest{operation=" + _operation + ", source=" + _source
        + ", reason=" + _reason + ", expectation=" + _nodeExpectation + ", expectedOperation="
        + _expectedOperation + ", expectedOperationSource=" + _expectedOperationSource
        + ", requireDesiredStateInEffect=" + _requireDesiredStateInEffect
        + ", requireRequestedSourceActive=" + _requireRequestedSourceActive
        + ", legacyFieldPolicy=" + _legacyFieldPolicy + "}";
  }

  public static Builder newBuilder(InstanceConstants.InstanceOperation operation,
      InstanceConstants.InstanceOperationSource source) {
    return new Builder(operation, source);
  }

  public static final class Builder {
    private final InstanceConstants.InstanceOperation _operation;
    private final InstanceConstants.InstanceOperationSource _source;
    private String _reason = "";
    private NodeExpectation _nodeExpectation = NodeExpectation.none();
    private InstanceConstants.InstanceOperation _expectedOperation;
    private InstanceConstants.InstanceOperationSource _expectedOperationSource;
    private boolean _requireDesiredStateInEffect = true;
    private boolean _requireRequestedSourceActive;
    private LegacyFieldPolicy _legacyFieldPolicy = LegacyFieldPolicy.OWNED_ONLY;

    private Builder(InstanceConstants.InstanceOperation operation,
        InstanceConstants.InstanceOperationSource source) {
      _operation = Objects.requireNonNull(operation, "operation must not be null");
      _source = Objects.requireNonNull(source, "source must not be null");
      if (source == InstanceConstants.InstanceOperationSource.ADMIN) {
        throw new IllegalArgumentException(
            "ADMIN clears the operations recorded by every other source, which a checked "
                + "change cannot express without discarding state it does not own. Use the "
                + "unchecked admin path when that is genuinely intended.");
      }
    }

    /**
     * @param reason free text stored with the operation. Null becomes the empty string, which
     *     is also what a reason that was never set reads back as.
     */
    public Builder setReason(@Nullable String reason) {
      _reason = reason == null ? "" : reason;
      return this;
    }

    /**
     * Require the instance config node to still be at a known version, a known incarnation, or
     * both. See {@link NodeExpectation} for what each of those is worth.
     */
    public Builder setNodeExpectation(NodeExpectation nodeExpectation) {
      _nodeExpectation = Objects.requireNonNull(nodeExpectation,
          "nodeExpectation must not be null; use NodeExpectation.none()");
      return this;
    }

    /**
     * Require the operation in effect at mutation time to be this one, otherwise conflict.
     */
    public Builder setExpectedOperation(
        @Nullable InstanceConstants.InstanceOperation expectedOperation) {
      _expectedOperation = expectedOperation;
      return this;
    }

    /**
     * Require the source recorded on the operation in effect at mutation time to be this one,
     * otherwise conflict. This is a precondition on observed state only: a source does not
     * establish ownership, because any writer can record any source.
     */
    public Builder setExpectedOperationSource(
        @Nullable InstanceConstants.InstanceOperationSource expectedOperationSource) {
      _expectedOperationSource = expectedOperationSource;
      return this;
    }

    /**
     * When true, the default, the change is only written if it results in the requested
     * operation being the one in effect. A request that would be recorded but overridden by
     * another writer's operation, or by the deprecated enabled flag, is reported as a conflict
     * with the blocker named instead of a write that looks like success.
     *
     * <p>Set it to false to record the intent regardless, for example to withdraw this
     * source's disable while another source keeps the instance disabled. The result still
     * reports whether the desired state is in effect.
     */
    public Builder setRequireDesiredStateInEffect(boolean requireDesiredStateInEffect) {
      _requireDesiredStateInEffect = requireDesiredStateInEffect;
      return this;
    }

    /**
     * When true, a request whose operation is already in effect is still written if the
     * requested source is not the source of the operation in effect, so that this source
     * becomes the active one. It is off by default because it turns an otherwise genuine no-op
     * into a write that moves this source's entry.
     */
    public Builder setRequireRequestedSourceActive(boolean requireRequestedSourceActive) {
      _requireRequestedSourceActive = requireRequestedSourceActive;
      return this;
    }

    public Builder setLegacyFieldPolicy(LegacyFieldPolicy legacyFieldPolicy) {
      _legacyFieldPolicy =
          Objects.requireNonNull(legacyFieldPolicy, "legacyFieldPolicy must not be null");
      return this;
    }

    public InstanceOperationChangeRequest build() {
      return new InstanceOperationChangeRequest(this);
    }
  }
}
