package org.apache.helix.sharding;

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
import java.util.List;
import java.util.UUID;

import org.apache.helix.HelixManager;
import org.apache.helix.HelixManagerFactory;
import org.apache.helix.InstanceType;
import org.apache.helix.d2.D2AnnouncingStateModelFactory;
import org.apache.helix.d2.D2PartitionAnnouncer;
import org.apache.helix.d2.HelixD2Announcer;
import org.apache.helix.participant.statemachine.StateModel;
import org.apache.helix.participant.statemachine.StateModelFactory;
import org.apache.helix.sharding.internal.ListenerStateModelFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Simplified participant API for sharded Helix clusters.
 *
 * <p>Provides a Tendril-like single-call API for joining a sharded cluster as a participant.
 * Handles all Helix plumbing: HelixManager creation, state model registration,
 * optional D2 announcement integration, and clean shutdown.</p>
 *
 * <h3>Tendril equivalence:</h3>
 * <pre>{@code
 * // Tendril
 * TendrilNode node = new TendrilNode.Builder()
 *     .clusterName("myCluster")
 *     .zkAddress("zk:2181")
 *     .build();
 * node.start();
 *
 * // Helix Sharding (equivalent)
 * HelixShardingNode node = new HelixShardingNode.Builder()
 *     .clusterName("myCluster")
 *     .zkAddress("zk:2181")
 *     .onStateTransition((partition, from, to) -> { ... })
 *     .build();
 * node.start();
 * }</pre>
 *
 * <h3>With D2 integration:</h3>
 * <pre>{@code
 * HelixShardingNode node = new HelixShardingNode.Builder()
 *     .clusterName("myCluster")
 *     .zkAddress("zk:2181")
 *     .addD2Announcer(myZkAnnouncerAdapter)
 *     .alwaysAnnouncePartitionZero(true)
 *     .onStateTransition((partition, from, to) -> { ... })
 *     .build();
 * node.start();
 * }</pre>
 */
public class HelixShardingNode implements AutoCloseable {

  private static final Logger LOG = LoggerFactory.getLogger(HelixShardingNode.class);

  private final String clusterName;
  private final String instanceName;
  private final String zkAddress;
  private final String stateModelName;
  private final StateModelFactory<? extends StateModel> stateModelFactory;
  private final HelixD2Announcer d2Announcer; // nullable
  private final HelixManager helixManager;

  private volatile boolean started = false;

  private HelixShardingNode(Builder builder) {
    this.clusterName = builder.clusterName;
    this.instanceName = builder.instanceName;
    this.zkAddress = builder.zkAddress;
    this.stateModelName = builder.stateModelName;

    // Build the state model factory chain
    StateModelFactory<? extends StateModel> baseFactory;
    if (builder.stateModelFactory != null) {
      baseFactory = builder.stateModelFactory;
    } else if (builder.stateTransitionListener != null) {
      baseFactory = new ListenerStateModelFactory(builder.stateTransitionListener);
    } else {
      throw new IllegalArgumentException(
          "Either stateModelFactory or onStateTransition listener must be provided");
    }

    // Optionally wrap with D2 announcing
    if (!builder.d2Announcers.isEmpty()) {
      HelixD2Announcer.Builder d2Builder = new HelixD2Announcer.Builder();
      for (D2PartitionAnnouncer ann : builder.d2Announcers) {
        d2Builder.addAnnouncer(ann);
      }
      d2Builder.alwaysAnnouncePartitionZero(builder.alwaysAnnouncePartitionZero);
      this.d2Announcer = d2Builder.build();
      this.stateModelFactory = new D2AnnouncingStateModelFactory<>(
          baseFactory, this.d2Announcer, builder.leaderState);
    } else {
      this.d2Announcer = null;
      this.stateModelFactory = baseFactory;
    }

    this.helixManager = HelixManagerFactory.getZKHelixManager(
        clusterName, instanceName, InstanceType.PARTICIPANT, zkAddress);
  }

  /**
   * Start the participant node.
   *
   * <p>Registers the state model factory, connects to ZooKeeper, and starts
   * participating in the cluster. If D2 announcers are configured, they are
   * started as well.</p>
   *
   * @throws Exception if the connection to ZooKeeper fails.
   */
  public void start() throws Exception {
    if (started) {
      LOG.warn("HelixShardingNode already started for cluster={}, instance={}",
          clusterName, instanceName);
      return;
    }

    LOG.info("Starting HelixShardingNode: cluster={}, instance={}, stateModel={}",
        clusterName, instanceName, stateModelName);

    // Register the state model factory
    helixManager.getStateMachineEngine()
        .registerStateModelFactory(stateModelName, stateModelFactory);

    // Connect to ZK
    helixManager.connect();

    // Start D2 announcer if configured
    if (d2Announcer != null) {
      d2Announcer.start();
    }

    started = true;
    LOG.info("HelixShardingNode started successfully: cluster={}, instance={}",
        clusterName, instanceName);
  }

  /**
   * Stop the participant node and release all resources.
   */
  public void stop() {
    if (!started) {
      return;
    }

    LOG.info("Stopping HelixShardingNode: cluster={}, instance={}", clusterName, instanceName);

    if (d2Announcer != null) {
      d2Announcer.shutdown();
      d2Announcer.cleanup();
    }

    if (helixManager.isConnected()) {
      helixManager.disconnect();
    }

    started = false;
    LOG.info("HelixShardingNode stopped: cluster={}, instance={}", clusterName, instanceName);
  }

  @Override
  public void close() {
    stop();
  }

  /**
   * @return Whether this node is started and connected.
   */
  public boolean isStarted() {
    return started;
  }

  /**
   * @return Whether the underlying HelixManager is connected to ZK.
   */
  public boolean isConnected() {
    return helixManager != null && helixManager.isConnected();
  }

  /**
   * @return The cluster name.
   */
  public String getClusterName() {
    return clusterName;
  }

  /**
   * @return The instance name.
   */
  public String getInstanceName() {
    return instanceName;
  }

  /**
   * @return The underlying HelixManager (for advanced operations).
   */
  public HelixManager getHelixManager() {
    return helixManager;
  }

  // ─── Builder ─────────────────────────────────────────────────────────────────

  public static class Builder {
    private String clusterName;
    private String instanceName;
    private String zkAddress;
    private String stateModelName = HelixShardingAdmin.DEFAULT_STATE_MODEL;
    private String leaderState = "LEADER";
    private StateModelFactory<? extends StateModel> stateModelFactory;
    private ShardingStateTransitionListener stateTransitionListener;
    private final List<D2PartitionAnnouncer> d2Announcers = new ArrayList<>();
    private boolean alwaysAnnouncePartitionZero = false;

    /**
     * Set the cluster name (required).
     */
    public Builder clusterName(String clusterName) {
      this.clusterName = clusterName;
      return this;
    }

    /**
     * Set the instance name. If not provided, a unique name is generated.
     */
    public Builder instanceName(String instanceName) {
      this.instanceName = instanceName;
      return this;
    }

    /**
     * Set the ZooKeeper connection string (required).
     */
    public Builder zkAddress(String zkAddress) {
      this.zkAddress = zkAddress;
      return this;
    }

    /**
     * Set the state model name. Defaults to "LeaderStandby".
     */
    public Builder stateModelName(String stateModelName) {
      this.stateModelName = stateModelName;
      return this;
    }

    /**
     * Set the leader state name. Defaults to "LEADER".
     */
    public Builder leaderState(String leaderState) {
      this.leaderState = leaderState;
      return this;
    }

    /**
     * Provide a custom state model factory (advanced usage).
     * Mutually exclusive with {@link #onStateTransition(ShardingStateTransitionListener)}.
     */
    public Builder stateModelFactory(StateModelFactory<? extends StateModel> factory) {
      this.stateModelFactory = factory;
      return this;
    }

    /**
     * Register a state transition callback (simplified API).
     * Mutually exclusive with {@link #stateModelFactory(StateModelFactory)}.
     */
    public Builder onStateTransition(ShardingStateTransitionListener listener) {
      this.stateTransitionListener = listener;
      return this;
    }

    /**
     * Add a D2 partition announcer for automatic announcement.
     */
    public Builder addD2Announcer(D2PartitionAnnouncer announcer) {
      this.d2Announcers.add(announcer);
      return this;
    }

    /**
     * Always include partition 0 in D2 announcements.
     */
    public Builder alwaysAnnouncePartitionZero(boolean enabled) {
      this.alwaysAnnouncePartitionZero = enabled;
      return this;
    }

    public HelixShardingNode build() {
      if (clusterName == null || clusterName.isEmpty()) {
        throw new IllegalArgumentException("Cluster name is required");
      }
      if (zkAddress == null || zkAddress.isEmpty()) {
        throw new IllegalArgumentException("ZK address is required");
      }
      if (instanceName == null || instanceName.isEmpty()) {
        instanceName = clusterName + "_" + UUID.randomUUID().toString().substring(0, 8);
      }
      if (stateModelFactory == null && stateTransitionListener == null) {
        throw new IllegalArgumentException(
            "Either stateModelFactory or onStateTransition must be set");
      }
      return new HelixShardingNode(this);
    }
  }
}
