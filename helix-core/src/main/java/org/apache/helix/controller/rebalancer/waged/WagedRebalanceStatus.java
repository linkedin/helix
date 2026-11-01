package org.apache.helix.controller.rebalancer.waged;

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

import org.apache.helix.HelixRebalanceException;

/**
 * Immutable health metadata for the WAGED assignment currently served by the controller.
 */
public final class WagedRebalanceStatus {
  // The last run failed and the controller serves the last good assignment instead.
  private final boolean _lastKnownGoodFallback;
  // The last serving (partial or emergency) computation failed.
  private final boolean _servingComputationFailed;
  // The last global baseline computation failed.
  private final boolean _baselineComputationFailed;
  // Why the serving computation failed; null when it did not.
  private final HelixRebalanceException.FailureCategory _servingFailureCategory;
  // This controller has finished a serving computation, so the flags above describe a real run.
  private final boolean _outcomeKnown;

  public WagedRebalanceStatus(boolean lastKnownGoodFallback, boolean servingComputationFailed,
      boolean baselineComputationFailed,
      HelixRebalanceException.FailureCategory servingFailureCategory, boolean outcomeKnown) {
    _outcomeKnown = outcomeKnown;
    _lastKnownGoodFallback = lastKnownGoodFallback;
    _servingComputationFailed = servingComputationFailed;
    _baselineComputationFailed = baselineComputationFailed;
    _servingFailureCategory = servingFailureCategory;
  }

  /** False until this controller has finished a serving computation, so the failure flags are not yet trustworthy. */
  public boolean isOutcomeKnown() {
    return _outcomeKnown;
  }

  public boolean isLastKnownGoodFallback() {
    return _lastKnownGoodFallback;
  }

  public boolean isServingComputationFailed() {
    return _servingComputationFailed;
  }

  public boolean isBaselineComputationFailed() {
    return _baselineComputationFailed;
  }

  public HelixRebalanceException.FailureCategory getServingFailureCategory() {
    return _servingFailureCategory;
  }
}
