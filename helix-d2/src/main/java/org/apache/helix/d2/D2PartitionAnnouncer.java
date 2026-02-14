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

import java.util.Map;


/**
 * Interface representing a D2-compatible partition announcer.
 *
 * <p>At LinkedIn, this is typically implemented by wrapping a {@code ZooKeeperAnnouncer}
 * from the D2 library. The interface abstracts the D2 dependency so that the helix-d2 module
 * can be built and tested without requiring the LinkedIn D2 library on the classpath.</p>
 *
 * <p>Implementors must be thread-safe: {@link #setPartitionDataMap(Map)}, {@link #markUp()},
 * and {@link #markDown()} may be called from multiple Helix state-transition threads
 * concurrently.</p>
 *
 * <h3>Typical LinkedIn implementation:</h3>
 * <pre>{@code
 * public class ZkAnnouncerAdapter implements D2PartitionAnnouncer {
 *     private final ZooKeeperAnnouncer delegate;
 *
 *     public ZkAnnouncerAdapter(ZooKeeperAnnouncer announcer) {
 *         this.delegate = announcer;
 *     }
 *
 *     public void setPartitionDataMap(Map<Integer, Double> partitionWeights) {
 *         Map<Integer, PartitionData> pdMap = new HashMap<>();
 *         for (Map.Entry<Integer, Double> e : partitionWeights.entrySet()) {
 *             pdMap.put(e.getKey(), new PartitionData(e.getValue()));
 *         }
 *         delegate.setPartitionData(pdMap);
 *     }
 *
 *     public void markUp() throws Exception { delegate.markUp(); }
 *     public void markDown() throws Exception { delegate.markDown(); }
 *     public void shutdown() { delegate.shutdown(); }
 * }
 * }</pre>
 */
public interface D2PartitionAnnouncer {

  /**
   * Set the partition data map on this announcer.
   *
   * <p>The map keys are partition indices (e.g., 0, 1, 5, 42) and the values are weights
   * (typically 1.0 for uniform weighting). This corresponds to D2's
   * {@code ZooKeeperAnnouncer.setPartitionData(Map<Integer, PartitionData>)}.</p>
   *
   * @param partitionWeights Partition index → weight mapping. Never null.
   */
  void setPartitionDataMap(Map<Integer, Double> partitionWeights);

  /**
   * Mark this announcer as "up" in the D2 service registry.
   * Must be called after {@link #setPartitionDataMap(Map)} to announce the current partitions.
   *
   * @throws Exception if the mark-up operation fails.
   */
  void markUp() throws Exception;

  /**
   * Mark this announcer as "down" in the D2 service registry.
   * Called before updating partition data to ensure no stale routing during transitions.
   *
   * @throws Exception if the mark-down operation fails.
   */
  void markDown() throws Exception;

  /**
   * Shut down this announcer, releasing any underlying resources.
   * After shutdown, no further calls should be made.
   */
  void shutdown();
}
