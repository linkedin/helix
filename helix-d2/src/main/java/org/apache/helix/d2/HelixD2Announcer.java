package org.apache.helix.d2;

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
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Manages D2 partition announcements for a Helix participant.
 *
 * <p>Tracks the set of LEADER partitions on this node and announces them to D2 via one or
 * more {@link D2PartitionAnnouncer} instances. When the leader partition set changes,
 * each announcer goes through a {@code markDown → setPartitionData → markUp} cycle
 * to atomically update D2 routing.</p>
 *
 * <p>Thread-safe: {@link #onLeaderPartitionsChanged(Set)} can be called from multiple
 * Helix state-transition threads concurrently.</p>
 *
 * <h3>Usage:</h3>
 * <pre>{@code
 * HelixD2Announcer announcer = new HelixD2Announcer.Builder()
 *     .addAnnouncer(zkAnnouncerAdapter1)
 *     .addAnnouncer(zkAnnouncerAdapter2)
 *     .alwaysAnnouncePartitionZero(true)
 *     .build();
 *
 * announcer.start();         // initial markUp
 * announcer.onLeaderPartitionsChanged(leaderPartitions);  // called on transitions
 * announcer.shutdown();      // markDown + cleanup
 * }</pre>
 */
public class HelixD2Announcer {

  private static final Logger LOG = LoggerFactory.getLogger(HelixD2Announcer.class);
  private static final double DEFAULT_PARTITION_WEIGHT = 1.0;

  private final List<D2PartitionAnnouncer> announcers;
  private final boolean alwaysAnnouncePartitionZero;

  // Change detection: only cycle markDown/markUp if the partition set actually changed
  private volatile Map<Integer, Double> lastAnnouncedPartitions;

  // Synchronization: the markDown → setPartitionData → markUp sequence must be atomic
  private final Object announcementLock = new Object();

  private volatile boolean started = false;

  private HelixD2Announcer(Builder builder) {
    this.announcers = Collections.unmodifiableList(new ArrayList<>(builder.announcers));
    this.alwaysAnnouncePartitionZero = builder.alwaysAnnouncePartitionZero;
    this.lastAnnouncedPartitions = Collections.emptyMap();
  }

  /**
   * Called when the set of LEADER partitions on this node changes.
   *
   * <p>Parses partition names to extract indices, builds the partition data map,
   * and cycles all announcers through {@code markDown → setPartitionData → markUp}
   * if the partition set has actually changed.</p>
   *
   * @param leaderPartitionNames Set of partition names where this node is LEADER.
   *                             Partition names are expected to end with {@code _<index>}
   *                             (e.g., "myResource_42").
   */
  public void onLeaderPartitionsChanged(Set<String> leaderPartitionNames) {
    Map<Integer, Double> newPartitionData = buildPartitionDataMap(leaderPartitionNames);

    synchronized (announcementLock) {
      if (newPartitionData.equals(lastAnnouncedPartitions)) {
        LOG.debug("Partition set unchanged, skipping D2 announcement cycle");
        return;
      }

      LOG.info("Leader partitions changed: {} → {} partitions",
          lastAnnouncedPartitions.size(), newPartitionData.size());

      for (D2PartitionAnnouncer announcer : announcers) {
        try {
          announcer.markDown();
          announcer.setPartitionDataMap(newPartitionData);
          announcer.markUp();
        } catch (Exception e) {
          LOG.error("Failed to update D2 announcer: {}", announcer, e);
        }
      }

      lastAnnouncedPartitions = newPartitionData;
    }
  }

  /**
   * Start all announcers (initial markUp with empty partition data).
   */
  public void start() {
    synchronized (announcementLock) {
      if (started) {
        LOG.warn("HelixD2Announcer already started");
        return;
      }

      Map<Integer, Double> initialPartitions = alwaysAnnouncePartitionZero
          ? Collections.singletonMap(0, DEFAULT_PARTITION_WEIGHT)
          : Collections.emptyMap();

      for (D2PartitionAnnouncer announcer : announcers) {
        try {
          announcer.setPartitionDataMap(initialPartitions);
          announcer.markUp();
        } catch (Exception e) {
          LOG.error("Failed to start D2 announcer: {}", announcer, e);
        }
      }

      lastAnnouncedPartitions = initialPartitions;
      started = true;
    }
  }

  /**
   * Shut down all announcers (markDown + cleanup).
   */
  public void shutdown() {
    synchronized (announcementLock) {
      if (!started) {
        return;
      }

      for (D2PartitionAnnouncer announcer : announcers) {
        try {
          announcer.markDown();
        } catch (Exception e) {
          LOG.error("Failed to markDown D2 announcer during shutdown: {}", announcer, e);
        }
      }

      started = false;
    }
  }

  /**
   * Clean up all announcer resources. Call after {@link #shutdown()}.
   */
  public void cleanup() {
    for (D2PartitionAnnouncer announcer : announcers) {
      try {
        announcer.shutdown();
      } catch (Exception e) {
        LOG.error("Failed to cleanup D2 announcer: {}", announcer, e);
      }
    }
  }

  /**
   * Build a partition data map from partition names.
   *
   * <p>Extracts the numeric index from partition names of the form {@code resourceName_index}
   * and creates a weight map. If {@code alwaysAnnouncePartitionZero} is enabled, partition 0
   * is always included.</p>
   *
   * @param partitionNames Set of partition names (e.g., "myResource_0", "myResource_5").
   * @return Map of partition index → weight.
   */
  Map<Integer, Double> buildPartitionDataMap(Set<String> partitionNames) {
    Map<Integer, Double> partitionData = new HashMap<>();

    if (alwaysAnnouncePartitionZero) {
      partitionData.put(0, DEFAULT_PARTITION_WEIGHT);
    }

    if (partitionNames != null) {
      for (String partitionName : partitionNames) {
        int index = parsePartitionIndex(partitionName);
        if (index >= 0) {
          partitionData.put(index, DEFAULT_PARTITION_WEIGHT);
        }
      }
    }

    return partitionData;
  }

  /**
   * Parse the numeric partition index from a partition name.
   *
   * @param partitionName Partition name (e.g., "myResource_42").
   * @return The partition index, or -1 if parsing fails.
   */
  static int parsePartitionIndex(String partitionName) {
    if (partitionName == null) {
      return -1;
    }
    int lastUnderscore = partitionName.lastIndexOf('_');
    if (lastUnderscore < 0 || lastUnderscore == partitionName.length() - 1) {
      return -1;
    }
    try {
      return Integer.parseInt(partitionName.substring(lastUnderscore + 1));
    } catch (NumberFormatException e) {
      return -1;
    }
  }

  /**
   * @return whether this announcer has been started.
   */
  public boolean isStarted() {
    return started;
  }

  /**
   * @return the last announced partition data map (for testing/diagnostics).
   */
  public Map<Integer, Double> getLastAnnouncedPartitions() {
    return Collections.unmodifiableMap(lastAnnouncedPartitions);
  }

  // ─── Builder ─────────────────────────────────────────────────────────────────

  public static class Builder {
    private final List<D2PartitionAnnouncer> announcers = new ArrayList<>();
    private boolean alwaysAnnouncePartitionZero = false;

    public Builder addAnnouncer(D2PartitionAnnouncer announcer) {
      if (announcer == null) {
        throw new IllegalArgumentException("Announcer must not be null");
      }
      announcers.add(announcer);
      return this;
    }

    public Builder alwaysAnnouncePartitionZero(boolean enabled) {
      this.alwaysAnnouncePartitionZero = enabled;
      return this;
    }

    public HelixD2Announcer build() {
      if (announcers.isEmpty()) {
        throw new IllegalArgumentException("At least one D2PartitionAnnouncer is required");
      }
      return new HelixD2Announcer(this);
    }
  }
}
