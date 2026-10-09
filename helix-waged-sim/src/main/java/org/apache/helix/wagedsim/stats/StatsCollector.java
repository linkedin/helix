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

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;

import org.apache.helix.controller.rebalancer.util.WagedValidationUtil;
import org.apache.helix.model.ClusterConfig;
import org.apache.helix.model.IdealState;
import org.apache.helix.model.InstanceConfig;
import org.apache.helix.model.ResourceConfig;
import org.apache.helix.model.StateModelDefinition;
import org.apache.helix.wagedsim.cluster.ClusterState;

/**
 * Computes the predefined stats for a layout of a cluster. Weights and capacities are resolved with
 * the same WAGED code the rebalancer uses; utilization is load divided by the instance's capacity,
 * measured over serving instances (enabled, live, assignable), including instances with no load.
 */
public final class StatsCollector {
  public static final List<String> DEFAULT_STATS_SUFFIXES = Collections.unmodifiableList(
      java.util.Arrays.asList("skew.top", "skew.all", "maxUtil.top", "maxUtil.all"));

  private final ClusterState _state;
  private final ClusterConfig _clusterConfig;
  private final List<String> _keys;
  private final String _focusKey;
  private final Map<String, IdealState> _idealStates;
  private final Map<String, ResourceConfig> _resourceConfigs;
  private final Map<String, StateModelDefinition> _models;
  private final Map<String, Map<String, Map<String, Integer>>> _weights = new HashMap<>();

  public StatsCollector(ClusterState state, String focusKey) {
    _state = state;
    _clusterConfig = state.getClusterConfig();
    _keys = new ArrayList<>(_clusterConfig.getInstanceCapacityKeys());
    _focusKey = focusKey != null && _keys.contains(focusKey) ? focusKey
        : (_keys.isEmpty() ? null : _keys.get(0));
    _idealStates = state.getWagedIdealStates();
    _resourceConfigs = state.getResourceConfigs();
    _models = state.getStateModelDefs();
  }

  public List<String> keys() {
    return _keys;
  }

  public String focusKey() {
    return _focusKey;
  }

  /** @return partition weights of a resource, resolved per partition as WAGED does */
  private Map<String, Map<String, Integer>> weights(String resource) {
    return _weights.computeIfAbsent(resource, r -> {
      IdealState idealState = _idealStates.get(r);
      ResourceConfig resourceConfig = _resourceConfigs.get(r);
      Map<String, Map<String, Integer>> capacityMap;
      try {
        capacityMap = resourceConfig == null ? new HashMap<>() : resourceConfig.getPartitionCapacityMap();
      } catch (IOException e) {
        throw new IllegalArgumentException("Invalid partition weights of " + r, e);
      }
      Map<String, Map<String, Integer>> result = new HashMap<>();
      for (String partition : idealState.getPartitionSet()) {
        Map<String, Integer> weight = WagedValidationUtil.validateAndGetPartitionCapacity(partition,
            resourceConfig, capacityMap, _clusterConfig);
        weight.keySet().retainAll(_keys);
        result.put(partition, weight);
      }
      return result;
    });
  }

  private Map<String, Integer> weight(String resource, String partition) {
    Map<String, Integer> weight = weights(resource).get(partition);
    return weight == null ? Collections.emptyMap() : weight;
  }

  /** @return true if a replica in this state holds data and serves requests */
  private boolean hosting(StateModelDefinition model, String state) {
    if (state == null || "DROPPED".equals(state) || "ERROR".equals(state)) {
      return false;
    }
    return model == null || !state.equals(model.getInitialState());
  }

  private String topState(String resource) {
    IdealState idealState = _idealStates.get(resource);
    StateModelDefinition model = idealState == null ? null : _models.get(idealState.getStateModelDefRef());
    return model == null ? null : model.getTopState();
  }

  /** Per-instance loads for the layout. */
  public Map<String, NodeStats> nodes(Map<String, Map<String, Map<String, String>>> layout) {
    Map<String, NodeStats> nodes = new TreeMap<>();
    Set<String> live = _state.getLiveInstances().keySet();
    String faultZoneType = _clusterConfig.getFaultZoneType();
    for (InstanceConfig config : _state.getInstanceConfigs().values()) {
      NodeStats node = new NodeStats();
      node.instance = config.getInstanceName();
      node.enabled = config.getInstanceEnabled();
      node.live = live.contains(node.instance);
      node.assignable = config.isAssignable();
      node.serving = node.enabled && node.live && node.assignable;
      Map<String, String> domain = config.getDomainAsMap();
      node.zone = faultZoneType != null && domain.containsKey(faultZoneType) ? domain.get(faultZoneType)
          : config.getRecord().getSimpleField("ZONE_ID");
      try {
        Map<String, Integer> capacity =
            WagedValidationUtil.validateAndGetInstanceCapacity(_clusterConfig, config);
        capacity.keySet().retainAll(_keys);
        node.capacity.putAll(capacity);
      } catch (RuntimeException e) {
        // Leave capacity empty; such an instance cannot take WAGED replicas.
      }
      for (String key : _keys) {
        node.topLoad.put(key, 0L);
        node.allLoad.put(key, 0L);
      }
      nodes.put(node.instance, node);
    }
    if (layout == null) {
      return nodes;
    }
    layout.forEach((resource, partitions) -> {
      if (!_idealStates.containsKey(resource)) {
        return;
      }
      StateModelDefinition model = _models.get(_idealStates.get(resource).getStateModelDefRef());
      String top = topState(resource);
      partitions.forEach((partition, replicas) -> {
        Map<String, Integer> weight = weight(resource, partition);
        replicas.forEach((instance, state) -> {
          NodeStats node = nodes.get(instance);
          if (node == null || !hosting(model, state)) {
            return;
          }
          node.replicaCount++;
          boolean isTop = state.equals(top);
          if (isTop) {
            node.topCount++;
          }
          weight.forEach((key, value) -> {
            node.allLoad.merge(key, (long) value, Long::sum);
            if (isTop) {
              node.topLoad.merge(key, (long) value, Long::sum);
            }
          });
          if (isTop && _focusKey != null) {
            node.topByResource.merge(resource, (long) weight.getOrDefault(_focusKey, 0), Long::sum);
          }
        });
      });
    });
    return nodes;
  }

  /**
   * @param layout the layout to measure
   * @param previous the layout of the previous round, for movement stats; may be null
   * @param baseline the WAGED baseline, for drift; may be null
   * @return stat name to value (Number or String); undefined values are omitted
   */
  public Map<String, Object> stats(Map<String, Map<String, Map<String, String>>> layout,
      Map<String, Map<String, Map<String, String>>> previous,
      Map<String, Map<String, Map<String, String>>> baseline, Map<String, NodeStats> nodes) {
    Map<String, Object> stats = new LinkedHashMap<>();
    List<NodeStats> serving = new ArrayList<>();
    for (NodeStats node : nodes.values()) {
      if (node.serving) {
        serving.add(node);
      }
    }
    stats.put("nodes.serving", serving.size());
    stats.put("nodes.total", nodes.size());
    Map<String, Double> topUtilByKey = new LinkedHashMap<>();
    for (String key : _keys) {
      putSkew(stats, "top", key, serving, true);
      putSkew(stats, "all", key, serving, false);
      long capacity = serving.stream().mapToLong(n -> n.capacity.getOrDefault(key, 0)).sum();
      long top = serving.stream().mapToLong(n -> n.topLoad.getOrDefault(key, 0L)).sum();
      long all = serving.stream().mapToLong(n -> n.allLoad.getOrDefault(key, 0L)).sum();
      stats.put("capacity." + key, capacity);
      stats.put("load.top." + key, top);
      stats.put("load.all." + key, all);
      stats.put("required.all." + key, required(key));
      if (capacity > 0) {
        double topUtil = 100.0 * top / capacity;
        topUtilByKey.put(key, topUtil);
        stats.put("util.top." + key, round(topUtil));
        stats.put("util.all." + key, round(100.0 * all / capacity));
        stats.put("util.required." + key, round(100.0 * required(key) / capacity));
      }
    }
    Double focusUtil = _focusKey == null ? null : topUtilByKey.get(_focusKey);
    for (String key : _keys) {
      if (focusUtil != null && focusUtil > 0 && topUtilByKey.containsKey(key)) {
        stats.put("exposure." + key, round(topUtilByKey.get(key) / focusUtil));
      }
    }
    putCountSkew(stats, "skew.topCount", serving, true);
    putCountSkew(stats, "skew.replicaCount", serving, false);
    putYardstick(stats, serving, topUtilByKey);
    if (_focusKey != null) {
      Double floor = Floor.topStateFloor(layout, serving, this, _focusKey);
      if (floor != null) {
        stats.put("floor.top." + _focusKey, round(floor));
      }
      NodeStats peak = null;
      for (NodeStats node : serving) {
        if (peak == null || node.topUtil(_focusKey) > peak.topUtil(_focusKey)) {
          peak = node;
        }
      }
      if (peak != null) {
        stats.put("peak.top." + _focusKey, peak.instance);
      }
    }
    putHealth(stats, layout, nodes, serving.size());
    if (previous != null) {
      putMoves(stats, previous, layout);
    }
    if (baseline != null && !baseline.isEmpty()) {
      stats.put("drift.baseline", round(drift(layout, baseline)));
    }
    return stats;
  }

  private long required(String key) {
    long total = 0;
    int liveCount = _state.getLiveInstances().size();
    for (Map.Entry<String, IdealState> entry : _idealStates.entrySet()) {
      int replicas = entry.getValue().getReplicaCount(liveCount);
      for (Map<String, Integer> weight : weights(entry.getKey()).values()) {
        total += (long) weight.getOrDefault(key, 0) * replicas;
      }
    }
    return total;
  }

  private void putSkew(Map<String, Object> stats, String scope, String key, List<NodeStats> serving,
      boolean top) {
    if (serving.isEmpty()) {
      return;
    }
    double sum = 0;
    double max = 0;
    double sumSq = 0;
    for (NodeStats node : serving) {
      double util = top ? node.topUtil(key) : node.allUtil(key);
      sum += util;
      sumSq += util * util;
      max = Math.max(max, util);
    }
    double mean = sum / serving.size();
    if (mean > 0) {
      stats.put("skew." + scope + "." + key, round(max / mean));
      double variance = Math.max(0, sumSq / serving.size() - mean * mean);
      stats.put("cv." + scope + "." + key, round(Math.sqrt(variance) / mean));
    }
    stats.put("maxUtil." + scope + "." + key, round(100 * max));
  }

  private static void putCountSkew(Map<String, Object> stats, String name, List<NodeStats> serving,
      boolean top) {
    if (serving.isEmpty()) {
      return;
    }
    double sum = 0;
    int max = 0;
    for (NodeStats node : serving) {
      int count = top ? node.topCount : node.replicaCount;
      sum += count;
      max = Math.max(max, count);
    }
    if (sum > 0) {
      stats.put(name, round(max / (sum / serving.size())));
    }
  }

  /**
   * The TopState constraint reads, per instance, the highest top-state utilization over the scoring
   * keys, against the cluster's highest top-state utilization. Counts instances below the mean on
   * the focus key that this reading puts at or above target.
   */
  private void putYardstick(Map<String, Object> stats, List<NodeStats> serving,
      Map<String, Double> topUtilByKey) {
    List<String> scoringKeys = new ArrayList<>(_keys);
    List<String> preferred = _clusterConfig.getPreferredScoringKeys();
    if (preferred != null && !preferred.isEmpty() && _keys.contains(preferred.get(0))) {
      scoringKeys = new ArrayList<>(preferred);
    }
    String targetKey = null;
    double target = 0;
    for (String key : scoringKeys) {
      Double util = topUtilByKey.get(key);
      if (util != null && util > target) {
        target = util;
        targetKey = key;
      }
    }
    if (targetKey == null || _focusKey == null || serving.isEmpty()) {
      return;
    }
    stats.put("yardstick.targetKey", targetKey);
    double meanFocus = serving.stream().mapToDouble(n -> n.topUtil(_focusKey)).average().orElse(0);
    int cold = 0;
    int misrated = 0;
    for (NodeStats node : serving) {
      if (node.topUtil(_focusKey) >= meanFocus) {
        continue;
      }
      cold++;
      double reading = 0;
      for (String key : scoringKeys) {
        reading = Math.max(reading, node.topUtil(key));
      }
      if (reading * 100 / target >= 1.0) {
        misrated++;
      }
    }
    stats.put("yardstick.misrated", misrated);
    if (cold > 0) {
      stats.put("yardstick.misratedShare", round(misrated / (double) cold));
    }
  }

  private void putHealth(Map<String, Object> stats, Map<String, Map<String, Map<String, String>>> layout,
      Map<String, NodeStats> nodes, int servingCount) {
    int overCapacity = 0;
    for (NodeStats node : nodes.values()) {
      for (String key : _keys) {
        Integer cap = node.capacity.get(key);
        if (cap != null && node.allLoad.getOrDefault(key, 0L) > cap) {
          overCapacity++;
          break;
        }
      }
    }
    stats.put("violations.capacity", overCapacity);
    int missingTop = 0;
    int underReplicated = 0;
    int liveCount = _state.getLiveInstances().size();
    for (Map.Entry<String, IdealState> entry : _idealStates.entrySet()) {
      String resource = entry.getKey();
      StateModelDefinition model = _models.get(entry.getValue().getStateModelDefRef());
      String top = topState(resource);
      int replicas = entry.getValue().getReplicaCount(liveCount);
      Map<String, Map<String, String>> partitions =
          layout == null ? Collections.emptyMap() : layout.getOrDefault(resource, Collections.emptyMap());
      for (String partition : entry.getValue().getPartitionSet()) {
        Map<String, String> replicaMap = partitions.getOrDefault(partition, Collections.emptyMap());
        int hostingCount = 0;
        boolean hasTop = false;
        for (Map.Entry<String, String> replica : replicaMap.entrySet()) {
          NodeStats node = nodes.get(replica.getKey());
          if (node != null && node.serving && hosting(model, replica.getValue())) {
            hostingCount++;
            hasTop |= replica.getValue().equals(top);
          }
        }
        if (!hasTop) {
          missingTop++;
        }
        if (hostingCount < Math.min(replicas, servingCount)) {
          underReplicated++;
        }
      }
    }
    stats.put("missingTopState", missingTop);
    stats.put("underReplicated", underReplicated);
  }

  private void putMoves(Map<String, Object> stats, Map<String, Map<String, Map<String, String>>> previous,
      Map<String, Map<String, Map<String, String>>> current) {
    long replicasMoved = 0;
    long topMoves = 0;
    long kept = 0;
    long roleSwap = 0;
    long toNonHolder = 0;
    for (Map.Entry<String, Map<String, Map<String, String>>> resource : current.entrySet()) {
      if (!_idealStates.containsKey(resource.getKey())) {
        continue;
      }
      StateModelDefinition model = _models.get(_idealStates.get(resource.getKey()).getStateModelDefRef());
      String top = topState(resource.getKey());
      Map<String, Map<String, String>> before =
          previous.getOrDefault(resource.getKey(), Collections.emptyMap());
      for (Map.Entry<String, Map<String, String>> partition : resource.getValue().entrySet()) {
        Map<String, String> now = partition.getValue();
        Map<String, String> then = before.getOrDefault(partition.getKey(), Collections.emptyMap());
        Set<String> holdersBefore = new HashSet<>();
        String topBefore = null;
        for (Map.Entry<String, String> replica : then.entrySet()) {
          if (hosting(model, replica.getValue())) {
            holdersBefore.add(replica.getKey());
            if (replica.getValue().equals(top)) {
              topBefore = replica.getKey();
            }
          }
        }
        String topNow = null;
        for (Map.Entry<String, String> replica : now.entrySet()) {
          if (!hosting(model, replica.getValue())) {
            continue;
          }
          if (!holdersBefore.contains(replica.getKey())) {
            replicasMoved++;
          }
          if (replica.getValue().equals(top)) {
            topNow = replica.getKey();
          }
        }
        if (topNow == null) {
          continue;
        }
        if (topNow.equals(topBefore)) {
          kept++;
        } else {
          topMoves++;
          if (holdersBefore.contains(topNow)) {
            roleSwap++;
          } else {
            toNonHolder++;
          }
        }
      }
    }
    stats.put("moves.replicas", replicasMoved);
    stats.put("moves.topState", topMoves);
    stats.put("moves.kept", kept);
    stats.put("moves.roleSwap", roleSwap);
    stats.put("moves.toNonHolder", toNonHolder);
  }

  private double drift(Map<String, Map<String, Map<String, String>>> layout,
      Map<String, Map<String, Map<String, String>>> baseline) {
    long total = 0;
    long differ = 0;
    for (Map.Entry<String, Map<String, Map<String, String>>> resource : layout.entrySet()) {
      if (!_idealStates.containsKey(resource.getKey())) {
        continue;
      }
      StateModelDefinition model = _models.get(_idealStates.get(resource.getKey()).getStateModelDefRef());
      Map<String, Map<String, String>> base = baseline.getOrDefault(resource.getKey(), Collections.emptyMap());
      for (Map.Entry<String, Map<String, String>> partition : resource.getValue().entrySet()) {
        Map<String, String> basePartition = base.getOrDefault(partition.getKey(), Collections.emptyMap());
        for (Map.Entry<String, String> replica : partition.getValue().entrySet()) {
          if (!hosting(model, replica.getValue())) {
            continue;
          }
          total++;
          if (!replica.getValue().equals(basePartition.get(replica.getKey()))) {
            differ++;
          }
        }
      }
    }
    return total == 0 ? 0 : differ / (double) total;
  }

  /** @return weights of the hosting top-state replicas on {@code key}, for the floor */
  List<Long> topWeights(Map<String, Map<String, Map<String, String>>> layout, String key) {
    List<Long> result = new ArrayList<>();
    if (layout == null) {
      return result;
    }
    layout.forEach((resource, partitions) -> {
      if (!_idealStates.containsKey(resource)) {
        return;
      }
      String top = topState(resource);
      partitions.forEach((partition, replicas) -> replicas.forEach((instance, state) -> {
        if (state.equals(top)) {
          result.add((long) weight(resource, partition).getOrDefault(key, 0));
        }
      }));
    });
    return result;
  }

  static double round(double value) {
    return Math.round(value * 1000) / 1000.0;
  }
}
