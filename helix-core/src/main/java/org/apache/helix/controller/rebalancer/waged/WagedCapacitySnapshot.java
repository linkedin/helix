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

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * A read-only, point-in-time view of how loaded each instance is, built only from published
 * metadata and ExternalViews. It serializes to JSON by field name.
 * <p>
 * WAGED weights are configured placement units, not live CPU, disk or QPS. A null {@code max},
 * {@code used} or {@code topStateUsed} means unknown, never zero.
 */
public class WagedCapacitySnapshot {
  /** Every number in the snapshot comes from ExternalViews, the last published assignment. */
  public static final String SOURCE_EXTERNAL_VIEW = "EXTERNAL_VIEW";

  public static final String CAPACITY_KNOWN = "KNOWN";
  public static final String CAPACITY_UNKNOWN = "UNKNOWN";
  public static final String CAPACITY_NOT_APPLICABLE = "NOT_APPLICABLE";

  public String cluster;
  // ClusterConfig.getInstanceCapacityKeys(); empty when the cluster does not use WAGED capacity.
  public List<String> capacityKeys = new ArrayList<>();
  public String faultZoneType;
  // A per-instance ceiling on partitions, null when no cluster-wide cap is configured.
  public Integer maxPartitionsPerInstance;
  public int wagedResourceCount;
  public int nonWagedResourceCount;
  public String generatedAt;
  public String collectionStartedAt;
  public long collectionDurationMs;
  public String source = SOURCE_EXTERNAL_VIEW;
  // Presence only means a process holds leadership, not that it is making progress.
  public String controllerName;
  public boolean paused;
  public boolean maintenance;
  public String maintenanceReason;
  public List<String> warnings = new ArrayList<>();
  public List<InstanceCapacity> instances = new ArrayList<>();
  public Summary summary = new Summary();

  public static class InstanceCapacity {
    public String name;
    public String zone;
    // False for a host that appears in an ExternalView but has no InstanceConfig.
    public boolean registered;
    public boolean live;
    public boolean enabled;
    // InstanceConfig operation state (ENABLE, DISABLE, EVACUATE, ...); null when unregistered.
    public String operation;
    public String capacityStatus;
    public Map<String, Long> max;
    // WAGED weight held by all non-dropped WAGED replicas; null when a weight cannot be resolved.
    public Map<String, Long> used;
    // The part of used held by top-state replicas; null when used is null or the top state of a
    // hosted WAGED resource cannot be resolved.
    public Map<String, Long> topStateUsed;
    // Occupancy across all resources, split by whether the owning resource is WAGED.
    public int wagedPartitions;
    public int nonWagedPartitions;
    public int wagedTopStatePartitions;
    public int nonWagedTopStatePartitions;
    public int errorReplicas;
    public List<String> warnings = new ArrayList<>();
    // What this instance carries, per resource, so a hot node can be traced to its resources.
    public List<InstanceResourceContribution> resources = new ArrayList<>();
  }

  public static class InstanceResourceContribution {
    public String resourceName;
    public boolean waged;
    public int replicas;
    public int topStateReplicas;
    public int errorReplicas;
    // Null for non-WAGED resources or when the weight cannot be resolved.
    public Map<String, Long> used;
  }

  public static class Summary {
    public int totalInstances;
    public int liveInstances;
    public int enabledInstances;
    public int instancesWithUnknownCapacity;
    public int instancesOverCapacity;
    public int unregisteredEvHosts;
    public int totalErrorReplicas;
  }

  static <V> Map<String, V> orderedMap() {
    return new LinkedHashMap<>();
  }
}
