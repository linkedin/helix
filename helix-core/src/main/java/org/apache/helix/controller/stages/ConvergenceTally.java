package org.apache.helix.controller.stages;

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

import static org.apache.helix.controller.stages.ConvergenceStatusCalculator.priority;
import static org.apache.helix.controller.stages.ConvergenceStatusCalculator.severity;

import java.util.Arrays;
import java.util.EnumMap;
import java.util.Map;

import org.apache.helix.model.ConvergenceStatus.Reason;
import org.apache.helix.model.ConvergenceStatus.Status;

/** Counts of partitions per status, and per reason for the ones that are not converged. */
final class ConvergenceTally {
  private static final int STATUS_COUNT = Status.values().length;
  private static final int REASON_COUNT = Reason.values().length;
  private final int[][] _counts = new int[STATUS_COUNT][REASON_COUNT];

  void record(Status status, Reason reason) {
    _counts[status.ordinal()][reason.ordinal()]++;
  }

  void record(Status status, Reason reason, int count) {
    _counts[status.ordinal()][reason.ordinal()] += count;
  }

  void add(ConvergenceTally other) {
    for (int s = 0; s < STATUS_COUNT; s++) {
      for (int r = 0; r < REASON_COUNT; r++) {
        _counts[s][r] += other._counts[s][r];
      }
    }
  }

  void reset() {
    for (int[] row : _counts) {
      Arrays.fill(row, 0);
    }
  }

  int total() {
    int total = 0;
    for (int[] row : _counts) {
      for (int count : row) {
        total += count;
      }
    }
    return total;
  }

  Map<Status, Integer> statusCounts() {
    Map<Status, Integer> result = new EnumMap<>(Status.class);
    for (Status status : Status.values()) {
      int sum = 0;
      for (int count : _counts[status.ordinal()]) {
        sum += count;
      }
      if (sum > 0) {
        result.put(status, sum);
      }
    }
    return result;
  }

  Map<Reason, Integer> reasonCounts() {
    Map<Reason, Integer> result = new EnumMap<>(Reason.class);
    for (Reason reason : Reason.values()) {
      if (reason == Reason.NONE) {
        continue;
      }
      int sum = 0;
      for (int s = 0; s < STATUS_COUNT; s++) {
        sum += _counts[s][reason.ordinal()];
      }
      if (sum > 0) {
        result.put(reason, sum);
      }
    }
    return result;
  }

  /** The most severe status that has any partition. */
  Status overallStatus() {
    Status result = Status.CONVERGED;
    for (Status status : Status.values()) {
      if (severity(status) > severity(result) && sum(status) > 0) {
        result = status;
      }
    }
    return result;
  }

  /** The most urgent reason among the partitions that carry the overall status. */
  Reason primaryReason(Status status) {
    Reason best = Reason.NONE;
    for (Reason reason : Reason.values()) {
      if (reason != Reason.NONE && _counts[status.ordinal()][reason.ordinal()] > 0
          && (best == Reason.NONE || priority(reason) < priority(best))) {
        best = reason;
      }
    }
    return best;
  }

  private int sum(Status status) {
    int sum = 0;
    for (int count : _counts[status.ordinal()]) {
      sum += count;
    }
    return sum;
  }

  int count(Status status, Reason reason) {
    return _counts[status.ordinal()][reason.ordinal()];
  }
}
