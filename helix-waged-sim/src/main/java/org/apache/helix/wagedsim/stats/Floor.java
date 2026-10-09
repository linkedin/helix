package org.apache.helix.wagedsim.stats;

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
import java.util.Map;

/**
 * Pigeonhole lower bound on top-state max/mean: no layout of the same top-state replicas over the
 * same instances can be more even. Among the kN+1 heaviest replicas on N instances, some instance
 * holds k+1 of them, so its load is at least the sum of the k+1 lightest of those.
 */
public final class Floor {
  private Floor() {
  }

  /**
   * @return the bound as max/mean on {@code key}, or null when undefined. Exact for equal
   *         capacities; uses the mean capacity otherwise.
   */
  public static Double topStateFloor(Map<String, Map<String, Map<String, String>>> layout,
      List<NodeStats> serving, StatsCollector collector, String key) {
    int nodes = serving.size();
    if (nodes == 0) {
      return null;
    }
    List<Long> weights = collector.topWeights(layout, key);
    if (weights.isEmpty()) {
      return null;
    }
    weights.sort(Collections.reverseOrder());
    long total = 0;
    for (long weight : weights) {
      total += weight;
    }
    if (total == 0) {
      return null;
    }
    double mean = total / (double) nodes;
    long bound = weights.get(0);
    for (int k = 1; (long) k * nodes + 1 <= weights.size(); k++) {
      int m = k * nodes + 1;
      long sum = 0;
      for (int i = m - k - 1; i < m; i++) {
        sum += weights.get(i);
      }
      bound = Math.max(bound, sum);
    }
    return Math.max(1.0, bound / mean);
  }
}
