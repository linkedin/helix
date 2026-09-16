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

import java.util.Collections;
import java.util.List;
import java.util.Objects;

import javax.annotation.Nullable;

import org.apache.helix.constants.InstanceConstants;

/**
 * The instance operation state a checked change was decided on, or produced.
 *
 * <p>It answers the three questions a caller would otherwise answer with its own reads: what
 * operation is in effect, what the caller's own source has recorded, and whether the change
 * the caller asked for is actually in effect. The last one matters because an operation can
 * be recorded and still not be in effect, for example when another source recorded a later
 * operation or when the deprecated enabled flag overrides it.
 */
public final class EffectiveInstanceOperation {
  private final InstanceConstants.InstanceOperation _operation;
  private final InstanceConstants.InstanceOperationSource _source;
  private final String _reason;
  private final InstanceConstants.InstanceOperation _requestedSourceOperation;
  private final boolean _helixEnabled;
  private final boolean _desiredStateInEffect;
  private final List<InstanceConstants.InstanceOperationSource> _recordedSources;

  EffectiveInstanceOperation(InstanceConstants.InstanceOperation operation,
      InstanceConstants.InstanceOperationSource source, String reason,
      @Nullable InstanceConstants.InstanceOperation requestedSourceOperation,
      boolean helixEnabled, boolean desiredStateInEffect,
      List<InstanceConstants.InstanceOperationSource> recordedSources) {
    _operation = Objects.requireNonNull(operation, "operation must not be null");
    _source = Objects.requireNonNull(source, "source must not be null");
    _reason = reason == null ? "" : reason;
    _requestedSourceOperation = requestedSourceOperation;
    _helixEnabled = helixEnabled;
    _desiredStateInEffect = desiredStateInEffect;
    _recordedSources = Collections.unmodifiableList(
        Objects.requireNonNull(recordedSources, "recordedSources must not be null"));
  }

  /**
   * @return the operation in effect, after the deprecated enabled flag has been applied.
   */
  public InstanceConstants.InstanceOperation getOperation() {
    return _operation;
  }

  /**
   * @return the source recorded on the operation in effect. This describes who wrote it; it is
   *     not a proof of exclusive ownership, because any writer can use any source.
   */
  public InstanceConstants.InstanceOperationSource getSource() {
    return _source;
  }

  /**
   * @return the reason recorded on the operation in effect, never null.
   */
  public String getReason() {
    return _reason;
  }

  /**
   * @return the operation recorded for the source the request used, or null when that source
   *     has nothing recorded.
   */
  @Nullable
  public InstanceConstants.InstanceOperation getRequestedSourceOperation() {
    return _requestedSourceOperation;
  }

  /**
   * @return the value of the deprecated instance enabled flag, which overrides an ENABLE or
   *     EVACUATE operation when it is false.
   */
  public boolean isHelixEnabled() {
    return _helixEnabled;
  }

  /**
   * @return true when the operation the request asked for is the operation in effect. A caller
   *     that treats a written change as a completed change must read this instead.
   */
  public boolean isDesiredStateInEffect() {
    return _desiredStateInEffect;
  }

  /**
   * @return the sources with a recorded operation, in stored order. The last one is active.
   */
  public List<InstanceConstants.InstanceOperationSource> getRecordedSources() {
    return _recordedSources;
  }

  @Override
  public String toString() {
    return "EffectiveInstanceOperation{operation=" + _operation + ", source=" + _source
        + ", reason=" + _reason + ", requestedSourceOperation=" + _requestedSourceOperation
        + ", helixEnabled=" + _helixEnabled + ", desiredStateInEffect=" + _desiredStateInEffect
        + ", recordedSources=" + _recordedSources + "}";
  }
}
