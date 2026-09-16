package org.apache.helix.messaging;

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

/**
 * Options used by optimized participant messaging APIs. Encapsulates optional parameters that were
 * previously provided via overloaded method signatures.
 */
public final class ParticipantMessageOptions {
  public static final int DEFAULT_TIMEOUT_MS = -1;
  public static final int DEFAULT_RETRY_COUNT = 0;

  private static final ParticipantMessageOptions DEFAULT =
      ParticipantMessageOptions.builder().build();

  private final boolean _sessionSpecific;
  private final boolean _selfExcluded;
  private final AsyncCallback _callbackOnReply;
  private final int _timeoutMs;
  private final int _retryCount;

  private ParticipantMessageOptions(Builder builder) {
    _sessionSpecific = builder._sessionSpecific;
    _selfExcluded = builder._selfExcluded;
    _callbackOnReply = builder._callbackOnReply;
    _timeoutMs = builder._timeoutMs;
    _retryCount = builder._retryCount;
  }

  public boolean isSessionSpecific() {
    return _sessionSpecific;
  }

  public boolean isSelfExcluded() {
    return _selfExcluded;
  }

  public AsyncCallback getCallbackOnReply() {
    return _callbackOnReply;
  }

  public int getTimeoutMs() {
    return _timeoutMs;
  }

  public int getRetryCount() {
    return _retryCount;
  }

  /**
   * Returns default immutable options instance.
   */
  public static ParticipantMessageOptions defaults() {
    return DEFAULT;
  }

  public static Builder builder() {
    return new Builder();
  }

  public static final class Builder {
    private boolean _sessionSpecific = false;
    private boolean _selfExcluded = false;
    private AsyncCallback _callbackOnReply = null;
    private int _timeoutMs = DEFAULT_TIMEOUT_MS;
    private int _retryCount = DEFAULT_RETRY_COUNT;

    public Builder sessionSpecific(boolean sessionSpecific) {
      _sessionSpecific = sessionSpecific;
      return this;
    }

    public Builder selfExcluded(boolean selfExcluded) {
      _selfExcluded = selfExcluded;
      return this;
    }

    public Builder callbackOnReply(AsyncCallback callbackOnReply) {
      _callbackOnReply = callbackOnReply;
      return this;
    }

    public Builder timeoutMs(int timeoutMs) {
      _timeoutMs = timeoutMs;
      return this;
    }

    public Builder retryCount(int retryCount) {
      _retryCount = retryCount;
      return this;
    }

    public ParticipantMessageOptions build() {
      return new ParticipantMessageOptions(this);
    }
  }
}
