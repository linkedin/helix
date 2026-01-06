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

import java.util.Collections;
import java.util.HashSet;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.helix.controller.dataproviders.ResourceControllerDataProvider;
import org.slf4j.Logger;

/**
 * Tracks mapping-stage capacity rejections for WAGED resources and exposes a thresholded signal
 * for triggering repair logic in a later pipeline run.
 *
 * <p>Design points:
 * - Producers are mapping threads that call {@link ResourceControllerDataProvider#checkAndReduceCapacity},
 *   which records events in a thread-safe queue.
 * - Consumers drain those events once per WAGED run and update consecutive counters across runs.
 */
public class MappingCapacityRejectionTracker {
  public static final String THRESHOLD_CLUSTER_CONFIG_FIELD = "WAGED_CAPACITY_REJECTION_THRESHOLD";
  public static final int DEFAULT_THRESHOLD = 10;

  public static final class Key {
    public final String resource;
    public final String partition;
    public final String instance;

    public Key(String resource, String partition, String instance) {
      this.resource = resource;
      this.partition = partition;
      this.instance = instance;
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) {
        return true;
      }
      if (!(o instanceof Key)) {
        return false;
      }
      Key that = (Key) o;
      return Objects.equals(resource, that.resource)
          && Objects.equals(partition, that.partition)
          && Objects.equals(instance, that.instance);
    }

    @Override
    public int hashCode() {
      return Objects.hash(resource, partition, instance);
    }
  }

  private static final class Counter {
    long lastSeenRunId;
    int consecutiveCount;

    Counter(long lastSeenRunId, int consecutiveCount) {
      this.lastSeenRunId = lastSeenRunId;
      this.consecutiveCount = consecutiveCount;
    }
  }

  private long _runId = 0;
  private final Map<Key, Counter> _counters = new ConcurrentHashMap<>();
  private final AtomicReference<Set<Key>> _aboveThresholdRef =
      new AtomicReference<>(Collections.emptySet());

  public Set<Key> getAboveThreshold() {
    return _aboveThresholdRef.get();
  }

  public void clearKeys(Set<Key> keys) {
    if (keys == null || keys.isEmpty()) {
      return;
    }
    keys.forEach(_counters::remove);
    _aboveThresholdRef.set(Collections.emptySet());
  }

  public int getThreshold(ResourceControllerDataProvider clusterData) {
    try {
      if (clusterData != null && clusterData.getClusterConfig() != null) {
        return clusterData.getClusterConfig().getRecord()
            .getIntField(THRESHOLD_CLUSTER_CONFIG_FIELD, DEFAULT_THRESHOLD);
      }
    } catch (Exception ignore) {
      // fall back to default
    }
    return DEFAULT_THRESHOLD;
  }

  /**
   * Drain events from the data provider and update consecutive counters for this run.
   */
  public void update(ResourceControllerDataProvider clusterData, Logger log) {
    if (clusterData == null) {
      _aboveThresholdRef.set(Collections.emptySet());
      return;
    }

    _runId++;
    int threshold = getThreshold(clusterData);

    // Drain events and dedupe within this run (avoid inflating counts due to repeated checks).
    Set<Key> keysThisRun = new HashSet<>();
    for (ResourceControllerDataProvider.WagedCapacityRejectionEvent e
        : clusterData.drainWagedCapacityRejectionEvents()) {
      if (e == null) {
        continue;
      }
      keysThisRun.add(new Key(e.resourceName, e.partitionName, e.instance));
    }

    if (keysThisRun.isEmpty()) {
      _aboveThresholdRef.set(Collections.emptySet());
      return;
    }

    Set<Key> above = new HashSet<>();
    for (Key key : keysThisRun) {
      Counter ctr = _counters.get(key);
      if (ctr == null) {
        ctr = new Counter(_runId, 1);
        _counters.put(key, ctr);
      } else {
        int previousCount = ctr.consecutiveCount;
        if (ctr.lastSeenRunId == _runId - 1) {
          ctr.consecutiveCount++;
        } else {
          ctr.consecutiveCount = 1;
        }
        ctr.lastSeenRunId = _runId;

        // Log once when we cross threshold.
        if (log != null && previousCount < threshold && ctr.consecutiveCount >= threshold) {
          log.warn(
              "WAGED mapping-stage capacity rejection reached threshold {} (will trigger repair in emergency rebalance). "
                  + "resource={}, partition={}, instance={}",
              threshold, key.resource, key.partition, key.instance);
        }
      }
      if (ctr.consecutiveCount >= threshold) {
        above.add(key);
      }
    }

    _aboveThresholdRef.set(above.isEmpty() ? Collections.emptySet() : Collections.unmodifiableSet(above));
  }
}




