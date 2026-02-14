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

import org.apache.helix.HelixAdmin;
import org.apache.helix.manager.zk.ZKHelixAdmin;
import org.apache.helix.model.IdealState;
import org.apache.helix.model.StateModelDefinition;
import org.apache.helix.tools.StateModelConfigGenerator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Simplified admin API for managing sharded Helix clusters.
 *
 * <p>Provides a high-level, Tendril-like API for cluster and resource management.
 * Under the hood, this translates to Helix's {@link HelixAdmin} operations,
 * making the "single resource per cluster" pattern easy to set up.</p>
 *
 * <h3>Tendril equivalence:</h3>
 * <ul>
 *   <li>{@code TendrilAdmin.addCluster(name, partitions, replicas)}
 *       → {@link #addCluster(String, int, int)}</li>
 *   <li>{@code TendrilAdmin.addCluster(name, partitions, replicas, assigner)}
 *       → {@link #addCluster(String, int, int, ShardingRebalanceStrategy)}</li>
 * </ul>
 *
 * <h3>Usage:</h3>
 * <pre>{@code
 * try (HelixShardingAdmin admin = new HelixShardingAdmin.Builder()
 *         .zkAddress("zk1:2181,zk2:2181")
 *         .build()) {
 *     admin.addCluster("myCluster", 128, 2, ShardingRebalanceStrategy.STICKY);
 * }
 * }</pre>
 */
public class HelixShardingAdmin implements AutoCloseable {

  private static final Logger LOG = LoggerFactory.getLogger(HelixShardingAdmin.class);

  /** Default resource name when using single-resource-per-cluster pattern. */
  static final String DEFAULT_RESOURCE_NAME = "shardingResource";

  /** Default state model for sharding (LeaderStandby). */
  static final String DEFAULT_STATE_MODEL = "LeaderStandby";

  private final HelixAdmin helixAdmin;
  private final boolean ownsAdmin;

  private HelixShardingAdmin(Builder builder) {
    if (builder.helixAdmin != null) {
      this.helixAdmin = builder.helixAdmin;
      this.ownsAdmin = false;
    } else {
      this.helixAdmin = new ZKHelixAdmin(builder.zkAddress);
      this.ownsAdmin = true;
    }
  }

  /**
   * Create a sharded cluster with the default rebalance strategy ({@link ShardingRebalanceStrategy#AUTO}).
   *
   * @param clusterName     The cluster name.
   * @param numPartitions   The number of shards/partitions.
   * @param replicaCount    The number of replicas per partition.
   */
  public void addCluster(String clusterName, int numPartitions, int replicaCount) {
    addCluster(clusterName, numPartitions, replicaCount, ShardingRebalanceStrategy.AUTO);
  }

  /**
   * Create a sharded cluster with a specific rebalance strategy.
   *
   * <p>This single method performs all the Helix setup that Tendril does automatically:
   * <ol>
   *   <li>Creates the Helix cluster in ZooKeeper.</li>
   *   <li>Adds the LeaderStandby state model definition.</li>
   *   <li>Adds a single resource (the "shard") with FULL_AUTO rebalance mode.</li>
   *   <li>Configures the rebalance strategy.</li>
   * </ol>
   *
   * @param clusterName     The cluster name.
   * @param numPartitions   The number of shards/partitions.
   * @param replicaCount    The number of replicas per partition.
   * @param strategy        The rebalance strategy to use.
   */
  public void addCluster(String clusterName, int numPartitions, int replicaCount,
      ShardingRebalanceStrategy strategy) {
    validateArgs(clusterName, numPartitions, replicaCount);

    LOG.info("Creating sharded cluster: name={}, partitions={}, replicas={}, strategy={}",
        clusterName, numPartitions, replicaCount, strategy);

    // 1. Create cluster
    helixAdmin.addCluster(clusterName, false);

    // 2. Add LeaderStandby state model
    StateModelDefinition leaderStandby =
        new StateModelDefinition(StateModelConfigGenerator.generateConfigForLeaderStandby());
    helixAdmin.addStateModelDef(clusterName, DEFAULT_STATE_MODEL, leaderStandby);

    // 3. Add the sharding resource with FULL_AUTO mode
    helixAdmin.addResource(clusterName, DEFAULT_RESOURCE_NAME, numPartitions,
        DEFAULT_STATE_MODEL, IdealState.RebalanceMode.FULL_AUTO.toString());

    // 4. Configure rebalance strategy on the IdealState
    IdealState idealState = helixAdmin.getResourceIdealState(clusterName, DEFAULT_RESOURCE_NAME);
    idealState.setRebalanceStrategy(strategy.getHelixClassName());
    idealState.setReplicas(String.valueOf(replicaCount));
    helixAdmin.setResourceIdealState(clusterName, DEFAULT_RESOURCE_NAME, idealState);

    // 5. Rebalance to compute initial assignment
    helixAdmin.rebalance(clusterName, DEFAULT_RESOURCE_NAME, replicaCount);

    LOG.info("Cluster {} created successfully with {} partitions", clusterName, numPartitions);
  }

  /**
   * Drop a sharded cluster (removes the cluster from ZooKeeper).
   *
   * @param clusterName The cluster to drop.
   */
  public void dropCluster(String clusterName) {
    if (clusterName == null || clusterName.isEmpty()) {
      throw new IllegalArgumentException("Cluster name must not be null or empty");
    }
    helixAdmin.dropCluster(clusterName);
    LOG.info("Dropped cluster: {}", clusterName);
  }

  /**
   * Expand the number of partitions for an existing cluster.
   *
   * @param clusterName     The cluster name.
   * @param newNumPartitions The new total number of partitions (must be >= current).
   */
  public void expandPartitions(String clusterName, int newNumPartitions) {
    if (clusterName == null || clusterName.isEmpty()) {
      throw new IllegalArgumentException("Cluster name must not be null or empty");
    }
    if (newNumPartitions <= 0) {
      throw new IllegalArgumentException("Partition count must be positive");
    }

    IdealState idealState = helixAdmin.getResourceIdealState(clusterName, DEFAULT_RESOURCE_NAME);
    int currentPartitions = idealState.getNumPartitions();
    if (newNumPartitions <= currentPartitions) {
      LOG.warn("New partition count ({}) is not greater than current ({}). No-op.",
          newNumPartitions, currentPartitions);
      return;
    }

    idealState.setNumPartitions(newNumPartitions);
    helixAdmin.setResourceIdealState(clusterName, DEFAULT_RESOURCE_NAME, idealState);

    int replicaCount = Integer.parseInt(idealState.getReplicas());
    helixAdmin.rebalance(clusterName, DEFAULT_RESOURCE_NAME, replicaCount);

    LOG.info("Expanded cluster {} from {} to {} partitions",
        clusterName, currentPartitions, newNumPartitions);
  }

  /**
   * Get the ideal state (partition assignment) for the default resource.
   *
   * @param clusterName The cluster name.
   * @return The IdealState, or null if not found.
   */
  public IdealState getResourceIdealState(String clusterName) {
    return helixAdmin.getResourceIdealState(clusterName, DEFAULT_RESOURCE_NAME);
  }

  /**
   * @return The underlying Helix admin (for advanced operations).
   */
  public HelixAdmin getHelixAdmin() {
    return helixAdmin;
  }

  @Override
  public void close() {
    if (ownsAdmin) {
      helixAdmin.close();
      LOG.info("HelixShardingAdmin closed");
    }
  }

  private void validateArgs(String clusterName, int numPartitions, int replicaCount) {
    if (clusterName == null || clusterName.isEmpty()) {
      throw new IllegalArgumentException("Cluster name must not be null or empty");
    }
    if (numPartitions <= 0) {
      throw new IllegalArgumentException("Number of partitions must be positive");
    }
    if (replicaCount <= 0) {
      throw new IllegalArgumentException("Replica count must be positive");
    }
  }

  // ─── Builder ─────────────────────────────────────────────────────────────────

  public static class Builder {
    private String zkAddress;
    private HelixAdmin helixAdmin;

    /**
     * Set the ZooKeeper connection string.
     * @param zkAddress Comma-separated ZK addresses (e.g., "zk1:2181,zk2:2181").
     */
    public Builder zkAddress(String zkAddress) {
      this.zkAddress = zkAddress;
      return this;
    }

    /**
     * Provide an existing HelixAdmin (for testing or sharing connections).
     * If set, {@link #zkAddress(String)} is ignored.
     */
    public Builder helixAdmin(HelixAdmin helixAdmin) {
      this.helixAdmin = helixAdmin;
      return this;
    }

    public HelixShardingAdmin build() {
      if (helixAdmin == null && (zkAddress == null || zkAddress.isEmpty())) {
        throw new IllegalArgumentException("Either zkAddress or helixAdmin must be provided");
      }
      return new HelixShardingAdmin(this);
    }
  }
}
