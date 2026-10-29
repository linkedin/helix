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

import java.io.IOException;
import java.time.Instant;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.TimeUnit;

import org.apache.helix.HelixDataAccessor;
import org.apache.helix.HelixDefinedState;
import org.apache.helix.HelixProperty;
import org.apache.helix.PropertyKey;
import org.apache.helix.controller.rebalancer.util.WagedValidationUtil;
import org.apache.helix.model.ClusterConfig;
import org.apache.helix.model.ExternalView;
import org.apache.helix.model.IdealState;
import org.apache.helix.model.InstanceConfig;
import org.apache.helix.model.LiveInstance;
import org.apache.helix.model.MaintenanceSignal;
import org.apache.helix.model.PauseSignal;
import org.apache.helix.model.ResourceConfig;
import org.apache.helix.model.StateModelDefinition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Computes a read-only {@link WagedCapacitySnapshot} from published metadata and ExternalViews.
 * <p>
 * It does a fixed set of bulk reads and never reads CURRENTSTATES or MESSAGES, never builds a
 * cluster model, never runs the rebalancer and never writes to ZooKeeper. WAGED {@code used} is the
 * sum of each placed replica's configured partition weight, which is how the controller charges
 * instance capacity. Unknowns stay null with a warning, they are never reported as zero. Callers
 * should cache the result.
 */
public class WagedCapacitySnapshotProvider {
  private static final Logger LOG = LoggerFactory.getLogger(WagedCapacitySnapshotProvider.class);

  private static final String ERROR_STATE = HelixDefinedState.ERROR.name();
  private static final String DROPPED_STATE = HelixDefinedState.DROPPED.name();

  public WagedCapacitySnapshot computeSnapshot(String clusterName, HelixDataAccessor dataAccessor) {
    long startNanos = System.nanoTime();
    WagedCapacitySnapshot snapshot = new WagedCapacitySnapshot();
    snapshot.cluster = clusterName;
    snapshot.collectionStartedAt = Instant.now().toString();

    PropertyKey.Builder keyBuilder = dataAccessor.keyBuilder();
    ClusterConfig clusterConfig = dataAccessor.getProperty(keyBuilder.clusterConfig());
    Map<String, IdealState> idealStates = childMap(dataAccessor, keyBuilder.idealStates());
    Map<String, InstanceConfig> instanceConfigs =
        childMap(dataAccessor, keyBuilder.instanceConfigs());
    Map<String, ExternalView> externalViews = childMap(dataAccessor, keyBuilder.externalViews());
    Map<String, ResourceConfig> resourceConfigs =
        childMap(dataAccessor, keyBuilder.resourceConfigs());
    Map<String, StateModelDefinition> stateModelDefs =
        childMap(dataAccessor, keyBuilder.stateModelDefs());
    List<String> liveNames = dataAccessor.getChildNames(keyBuilder.liveInstances());
    Set<String> liveInstances = liveNames == null ? new HashSet<>() : new HashSet<>(liveNames);

    LiveInstance leader = dataAccessor.getProperty(keyBuilder.controllerLeader());
    snapshot.controllerName = leader == null ? null : leader.getInstanceName();
    snapshot.paused = dataAccessor.getProperty(keyBuilder.pause()) != null;
    MaintenanceSignal maintenance = dataAccessor.getProperty(keyBuilder.maintenance());
    snapshot.maintenance = maintenance != null;
    snapshot.maintenanceReason = maintenance == null ? null : maintenance.getReason();

    if (clusterConfig == null) {
      snapshot.warnings.add(
          "ClusterConfig is missing; capacity dimensions, fault zone, and defaults are unknown.");
    }
    List<String> capacityKeys =
        clusterConfig == null ? Collections.emptyList() : clusterConfig.getInstanceCapacityKeys();
    snapshot.capacityKeys = capacityKeys;
    snapshot.faultZoneType = clusterConfig == null ? null : clusterConfig.getFaultZoneType();
    // ClusterConfig returns -1 when unset; a viewer must not read that as a real limit.
    snapshot.maxPartitionsPerInstance =
        (clusterConfig != null && clusterConfig.getMaxPartitionsPerInstance() >= 0)
            ? clusterConfig.getMaxPartitionsPerInstance() : null;

    Map<String, ResourceContext> resourceContexts = new HashMap<>();
    int wagedCount = 0;
    for (Map.Entry<String, IdealState> entry : idealStates.entrySet()) {
      ResourceContext ctx = new ResourceContext();
      ctx.waged = WagedValidationUtil.isWagedEnabled(entry.getValue());
      if (ctx.waged) {
        wagedCount++;
        ctx.weights =
            new WeightContext(entry.getKey(), resourceConfigs.get(entry.getKey()), clusterConfig);
      }
      String stateModelRef = entry.getValue().getStateModelDefRef();
      StateModelDefinition def = stateModelRef == null ? null : stateModelDefs.get(stateModelRef);
      ctx.topState = def == null ? null : def.getTopState();
      resourceContexts.put(entry.getKey(), ctx);
    }
    snapshot.wagedResourceCount = wagedCount;
    snapshot.nonWagedResourceCount = idealStates.size() - wagedCount;

    Map<String, InstanceAccum> accums = new HashMap<>();
    for (Map.Entry<String, ExternalView> entry : externalViews.entrySet()) {
      accumulate(entry.getKey(), entry.getValue(), resourceContexts.get(entry.getKey()),
          capacityKeys, accums);
    }

    // Every configured instance, plus any host that only appears in an ExternalView.
    Set<String> instanceNames = new TreeSet<>(instanceConfigs.keySet());
    instanceNames.addAll(accums.keySet());
    for (String name : instanceNames) {
      snapshot.instances.add(buildInstanceRow(name, instanceConfigs.get(name), accums.get(name),
          clusterConfig, capacityKeys, liveInstances.contains(name)));
    }
    populateSummary(snapshot);

    snapshot.collectionDurationMs = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - startNanos);
    snapshot.generatedAt = Instant.now().toString();
    LOG.info("Computed capacity snapshot for cluster {} in {} ms: {} instances, {} WAGED resources",
        clusterName, snapshot.collectionDurationMs, snapshot.instances.size(), wagedCount);
    return snapshot;
  }

  private void accumulate(String resourceName, ExternalView ev, ResourceContext ctx,
      List<String> capacityKeys, Map<String, InstanceAccum> accums) {
    boolean waged = ctx != null && ctx.waged;
    String topState = ctx == null ? null : ctx.topState;
    for (String partition : ev.getPartitionSet()) {
      Map<String, String> stateMap = ev.getStateMap(partition);
      if (stateMap == null) {
        continue;
      }
      Map<String, Long> weight = waged ? ctx.weights.weightFor(partition) : null;
      for (Map.Entry<String, String> replica : stateMap.entrySet()) {
        String state = replica.getValue();
        // DROPPED is a tombstone: the replica no longer occupies the node.
        if (state == null || DROPPED_STATE.equals(state)) {
          continue;
        }
        boolean isTop = state.equals(topState);
        boolean isError = ERROR_STATE.equals(state);
        InstanceAccum accum = accums.computeIfAbsent(replica.getKey(), k -> new InstanceAccum());
        ResourceAccum res = accum.byResource.computeIfAbsent(resourceName,
            k -> new ResourceAccum(resourceName, waged));
        res.replicas++;
        if (isTop) {
          res.topStateReplicas++;
        }
        if (isError) {
          res.errorReplicas++;
          accum.errorReplicas++;
        }
        if (!waged) {
          accum.nonWagedPartitions++;
          if (isTop) {
            accum.nonWagedTopStatePartitions++;
          }
          continue;
        }
        accum.wagedPartitions++;
        if (isTop) {
          accum.wagedTopStatePartitions++;
        }
        if (topState == null) {
          accum.topStateUnresolved = true;
        }
        if (weight == null) {
          accum.weightUnresolved = true;
          res.weightUnresolved = true;
          continue;
        }
        for (String key : capacityKeys) {
          long w = weight.getOrDefault(key, 0L);
          accum.used.merge(key, w, Long::sum);
          res.used.merge(key, w, Long::sum);
          if (isTop) {
            accum.topStateUsed.merge(key, w, Long::sum);
          }
        }
      }
    }
  }

  private WagedCapacitySnapshot.InstanceCapacity buildInstanceRow(String name,
      InstanceConfig config, InstanceAccum accum, ClusterConfig clusterConfig,
      List<String> capacityKeys, boolean live) {
    WagedCapacitySnapshot.InstanceCapacity row = new WagedCapacitySnapshot.InstanceCapacity();
    row.name = name;
    row.registered = config != null;
    row.live = live;
    row.enabled = config != null && config.getInstanceEnabled();
    row.operation = config == null ? null : config.getInstanceOperationState();
    row.zone = resolveZone(config, clusterConfig);

    if (accum != null) {
      row.wagedPartitions = accum.wagedPartitions;
      row.nonWagedPartitions = accum.nonWagedPartitions;
      row.wagedTopStatePartitions = accum.wagedTopStatePartitions;
      row.nonWagedTopStatePartitions = accum.nonWagedTopStatePartitions;
      row.errorReplicas = accum.errorReplicas;
      for (ResourceAccum res : accum.byResource.values()) {
        WagedCapacitySnapshot.InstanceResourceContribution c =
            new WagedCapacitySnapshot.InstanceResourceContribution();
        c.resourceName = res.resourceName;
        c.waged = res.waged;
        c.replicas = res.replicas;
        c.topStateReplicas = res.topStateReplicas;
        c.errorReplicas = res.errorReplicas;
        c.used = res.waged && !res.weightUnresolved ? res.used : null;
        row.resources.add(c);
      }
    }

    if (!row.registered) {
      row.warnings.add("Instance hosts replicas in an ExternalView but has no InstanceConfig.");
    } else if (!live) {
      row.warnings.add("Instance is registered but not live (no LIVEINSTANCES node).");
    } else if (!row.enabled) {
      row.warnings.add("Instance is live but not enabled (operation=" + row.operation + ").");
    }

    if (capacityKeys.isEmpty()) {
      row.capacityStatus = WagedCapacitySnapshot.CAPACITY_NOT_APPLICABLE;
      return row;
    }
    row.max = resolveInstanceMax(clusterConfig, config, capacityKeys);
    boolean weightsKnown = accum == null || !accum.weightUnresolved;
    row.capacityStatus = row.max != null && weightsKnown ? WagedCapacitySnapshot.CAPACITY_KNOWN
        : WagedCapacitySnapshot.CAPACITY_UNKNOWN;
    if (row.max == null) {
      row.warnings.add("Instance capacity is not fully configured for " + capacityKeys
          + "; utilization cannot be computed (not treated as zero).");
    }
    if (!weightsKnown) {
      row.warnings.add("A hosted WAGED partition has an unresolvable weight; WAGED usage is "
          + "unknown for this instance (not treated as zero).");
      return row;
    }
    row.used = sumOver(accum == null ? null : accum.used, capacityKeys);
    if (accum != null && accum.topStateUnresolved) {
      row.warnings.add("A hosted WAGED resource has an unresolvable top state; top-state usage "
          + "is unknown for this instance (not treated as zero).");
    } else {
      row.topStateUsed = sumOver(accum == null ? null : accum.topStateUsed, capacityKeys);
    }
    return row;
  }

  // Instance max capacity for the required dimensions, or null when any of them is unresolved.
  private Map<String, Long> resolveInstanceMax(ClusterConfig clusterConfig, InstanceConfig config,
      List<String> capacityKeys) {
    if (clusterConfig == null || config == null) {
      return null;
    }
    Map<String, Integer> resolved;
    try {
      resolved = WagedValidationUtil.validateAndGetInstanceCapacity(clusterConfig, config);
    } catch (RuntimeException e) {
      return null;
    }
    Map<String, Long> max = WagedCapacitySnapshot.orderedMap();
    for (String key : capacityKeys) {
      Integer value = resolved.get(key);
      if (value == null) {
        return null;
      }
      max.put(key, value.longValue());
    }
    return max;
  }

  private Map<String, Long> sumOver(Map<String, Long> sums, List<String> capacityKeys) {
    Map<String, Long> out = WagedCapacitySnapshot.orderedMap();
    for (String key : capacityKeys) {
      out.put(key, sums == null ? 0L : sums.getOrDefault(key, 0L));
    }
    return out;
  }

  private void populateSummary(WagedCapacitySnapshot snapshot) {
    WagedCapacitySnapshot.Summary summary = snapshot.summary;
    summary.totalInstances = snapshot.instances.size();
    for (WagedCapacitySnapshot.InstanceCapacity row : snapshot.instances) {
      summary.liveInstances += row.live ? 1 : 0;
      summary.enabledInstances += row.enabled ? 1 : 0;
      summary.unregisteredEvHosts += row.registered ? 0 : 1;
      summary.instancesWithUnknownCapacity +=
          WagedCapacitySnapshot.CAPACITY_UNKNOWN.equals(row.capacityStatus) ? 1 : 0;
      summary.totalErrorReplicas += row.errorReplicas;
      summary.instancesOverCapacity += isOverCapacity(row) ? 1 : 0;
    }
  }

  private boolean isOverCapacity(WagedCapacitySnapshot.InstanceCapacity row) {
    if (row.max == null || row.used == null) {
      return false;
    }
    for (Map.Entry<String, Long> entry : row.used.entrySet()) {
      Long max = row.max.get(entry.getKey());
      if (max != null && entry.getValue() > max) {
        return true;
      }
    }
    return false;
  }

  // The instance's value for the cluster's fault-zone domain key, empty when it has none.
  private String resolveZone(InstanceConfig config, ClusterConfig clusterConfig) {
    if (config == null) {
      return "";
    }
    String faultZoneType = clusterConfig == null ? null : clusterConfig.getFaultZoneType();
    Map<String, String> domain = config.getDomainAsMap();
    String zone = faultZoneType == null || domain == null ? null : domain.get(faultZoneType);
    return zone == null ? "" : zone;
  }

  private static <T extends HelixProperty> Map<String, T> childMap(HelixDataAccessor dataAccessor,
      PropertyKey key) {
    Map<String, T> result = dataAccessor.getChildValuesMap(key, true);
    return result == null ? new HashMap<>() : result;
  }

  private static final class ResourceContext {
    boolean waged;
    WeightContext weights;
    String topState;
  }

  // Resolves a WAGED partition weight with the controller's own precedence (resource per-partition,
  // resource default, cluster default). An unresolvable partition returns null, never a fake default.
  private static final class WeightContext {
    private final ResourceConfig resourceConfig;
    private final ClusterConfig clusterConfig;
    private final Map<String, Map<String, Integer>> partitionCapacityMap;
    private final Map<String, Map<String, Long>> cache = new HashMap<>();

    WeightContext(String resourceName, ResourceConfig resourceConfig, ClusterConfig clusterConfig) {
      this.resourceConfig =
          resourceConfig == null ? new ResourceConfig(resourceName) : resourceConfig;
      this.clusterConfig = clusterConfig;
      Map<String, Map<String, Integer>> map;
      try {
        map = this.resourceConfig.getPartitionCapacityMap();
      } catch (IOException e) {
        map = new HashMap<>();
      }
      this.partitionCapacityMap = map;
    }

    Map<String, Long> weightFor(String partition) {
      Map<String, Long> cached = cache.get(partition);
      if (cached != null || clusterConfig == null) {
        return cached;
      }
      try {
        Map<String, Long> weight = new HashMap<>();
        for (Map.Entry<String, Integer> entry : WagedValidationUtil
            .validateAndGetPartitionCapacity(partition, resourceConfig, partitionCapacityMap,
                clusterConfig).entrySet()) {
          weight.put(entry.getKey(), entry.getValue().longValue());
        }
        cache.put(partition, weight);
        return weight;
      } catch (RuntimeException e) {
        return null;
      }
    }
  }

  private static final class InstanceAccum {
    int wagedPartitions;
    int nonWagedPartitions;
    int wagedTopStatePartitions;
    int nonWagedTopStatePartitions;
    int errorReplicas;
    boolean weightUnresolved;
    boolean topStateUnresolved;
    final Map<String, Long> used = new HashMap<>();
    final Map<String, Long> topStateUsed = new HashMap<>();
    final Map<String, ResourceAccum> byResource = new LinkedHashMap<>();
  }

  private static final class ResourceAccum {
    final String resourceName;
    final boolean waged;
    int replicas;
    int topStateReplicas;
    int errorReplicas;
    boolean weightUnresolved;
    final Map<String, Long> used = new LinkedHashMap<>();

    ResourceAccum(String resourceName, boolean waged) {
      this.resourceName = resourceName;
      this.waged = waged;
    }
  }
}
