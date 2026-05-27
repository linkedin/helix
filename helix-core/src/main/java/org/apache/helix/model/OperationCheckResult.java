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
 * Result of an operation readiness or validation check. Contains whether the operation
 * succeeded and, if not, the specific reasons (blockers) preventing success.
 */
public class OperationCheckResult {

  private final boolean _successful;
  private final List<String> _blockers;

  private OperationCheckResult(boolean successful, List<String> blockers) {
    _successful = successful;
    _blockers = blockers != null ? Collections.unmodifiableList(blockers) : Collections.emptyList();
  }

  /**
   * Create a successful result.
   */
  public static OperationCheckResult success() {
    return new OperationCheckResult(true, Collections.emptyList());
  }

  /**
   * Create a failed result with a single blocker reason.
   *
   * @param blocker The human-readable reason why the operation failed.
   */
  public static OperationCheckResult failed(String blocker) {
    return new OperationCheckResult(false, Collections.singletonList(blocker));
  }

  /**
   * Create a failed result with the list of blockers preventing the operation.
   *
   * @param blockers The list of human-readable reasons why the operation failed.
   */
  public static OperationCheckResult failed(List<String> blockers) {
    return new OperationCheckResult(false, blockers);
  }

  /**
   * @return True if the operation succeeded or is ready, false otherwise.
   */
  public boolean isSuccessful() {
    return _successful;
  }

  /**
   * @return An unmodifiable list of reasons why the operation failed.
   *         Empty if the operation succeeded.
   */
  public List<String> getBlockers() {
    return _blockers;
  }

  /**
   * Builder for collecting blockers during operation checks.
   */
  public static class Builder {
    private final List<String> _blockers = new ArrayList<>();

    public Builder addBlocker(String blocker) {
      _blockers.add(blocker);
      return this;
    }

    public boolean hasBlockers() {
      return !_blockers.isEmpty();
    }

    public OperationCheckResult build() {
      if (_blockers.isEmpty()) {
        return OperationCheckResult.success();
      }
      return OperationCheckResult.failed(new ArrayList<>(_blockers));
    }
  }
}
