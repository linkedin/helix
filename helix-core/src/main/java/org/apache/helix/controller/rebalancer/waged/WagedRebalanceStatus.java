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
  private final boolean _lastKnownGoodFallback;
  private final boolean _servingComputationFailed;
  private final boolean _baselineComputationFailed;
  private final HelixRebalanceException.FailureCategory _servingFailureCategory;

  public WagedRebalanceStatus(boolean lastKnownGoodFallback, boolean servingComputationFailed,
      boolean baselineComputationFailed,
      HelixRebalanceException.FailureCategory servingFailureCategory) {
    _lastKnownGoodFallback = lastKnownGoodFallback;
    _servingComputationFailed = servingComputationFailed;
    _baselineComputationFailed = baselineComputationFailed;
    _servingFailureCategory = servingFailureCategory;
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
