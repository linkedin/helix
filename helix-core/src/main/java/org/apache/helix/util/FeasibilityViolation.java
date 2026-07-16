package org.apache.helix.util;

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

/**
 * A single reason a proposed cluster mutation would make the cluster un-rebalanceable.
 * Produced by {@link RebalanceFeasibilityEvaluator} and surfaced to REST callers as part of a
 * {@link FeasibilityResult}.
 */
public class FeasibilityViolation {
  /**
   * The class of rebalance invariant that a proposed mutation would break.
   */
  public enum Type {
    /** A partition would retain fewer than its required minimum active replicas. */
    MIN_ACTIVE_REPLICA,
    /** An instance would exceed, or fail to declare, a required WAGED capacity dimension. */
    CAPACITY,
    /** A partition that was assignable would be left unassigned. */
    UNASSIGNED_PARTITION
  }

  private final Type type;
  private final String resourceName;
  private final String partitionName;
  private final String instanceName;
  private final String detail;

  public FeasibilityViolation(Type type, String resourceName, String partitionName,
      String instanceName, String detail) {
    this.type = type;
    this.resourceName = resourceName;
    this.partitionName = partitionName;
    this.instanceName = instanceName;
    this.detail = detail;
  }

  public static FeasibilityViolation minActiveReplica(String resourceName, String partitionName,
      int currentActiveReplicas, int requiredMinActiveReplicas) {
    String detail = String.format("Partition %s has %d/%d active replicas", partitionName,
        currentActiveReplicas, requiredMinActiveReplicas);
    return new FeasibilityViolation(Type.MIN_ACTIVE_REPLICA, resourceName, partitionName, null,
        detail);
  }

  public static FeasibilityViolation capacity(String instanceName, String detail) {
    return new FeasibilityViolation(Type.CAPACITY, null, null, instanceName, detail);
  }

  public static FeasibilityViolation unassignedPartition(String resourceName,
      String partitionName) {
    String detail = String.format("Partition %s would be left unassigned", partitionName);
    return new FeasibilityViolation(Type.UNASSIGNED_PARTITION, resourceName, partitionName, null,
        detail);
  }

  public Type getType() {
    return type;
  }

  public String getResourceName() {
    return resourceName;
  }

  public String getPartitionName() {
    return partitionName;
  }

  public String getInstanceName() {
    return instanceName;
  }

  public String getDetail() {
    return detail;
  }

  @Override
  public String toString() {
    return String.format("%s: %s", type, detail);
  }
}
