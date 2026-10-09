package org.apache.helix.wagedsim.scenario;

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
import java.util.Comparator;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.TreeMap;
import java.util.function.Predicate;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import org.apache.helix.constants.InstanceConstants;
import org.apache.helix.model.ClusterConfig;
import org.apache.helix.model.IdealState;
import org.apache.helix.model.InstanceConfig;
import org.apache.helix.model.MaintenanceSignal;
import org.apache.helix.model.ResourceConfig;
import org.apache.helix.wagedsim.cluster.ClusterState;
import org.apache.helix.wagedsim.engine.Engine;
import org.apache.helix.wagedsim.engine.StateOps;
import org.apache.helix.wagedsim.source.SpecSource;
import org.apache.helix.wagedsim.stats.NodeStats;
import org.apache.helix.wagedsim.stats.StatsCollector;
import org.apache.helix.wagedsim.util.Durations;

/** Applies scheduled events to a running engine. */
public final class Events {
  private Events() {
  }

  /** What events need besides the engine: randomness, history and the last measured loads. */
  public static class Context {
    public final Engine engine;
    public final Random random;
    public final String focusKey;
    public Map<String, NodeStats> lastNodes;
    public final Set<String> disabled = new LinkedHashSet<>();
    public final Set<String> killed = new LinkedHashSet<>();
    public final Map<String, Integer> generations = new HashMap<>();
    private int _added;

    public Context(Engine engine, long seed, String focusKey) {
      this.engine = engine;
      this.random = new Random(seed);
      this.focusKey = focusKey;
    }

    ClusterState state() {
      return engine.state();
    }
  }

  /** @return one line per change made */
  @SuppressWarnings("unchecked")
  public static List<String> apply(EventSpec event, Context ctx) throws Exception {
    List<String> log = new ArrayList<>();
    ClusterState state = ctx.state();
    Object args = event.args;
    switch (event.kind) {
      case "disable":
        for (String instance : select(args, ctx, i -> config(state, i).getInstanceEnabled())) {
          StateOps.setOperation(state, instance, InstanceConstants.InstanceOperation.DISABLE);
          ctx.disabled.add(instance);
          log.add("disable " + instance);
        }
        break;
      case "enable":
        for (String instance : select(args, ctx, i -> !config(state, i).getInstanceEnabled())) {
          StateOps.setOperation(state, instance, InstanceConstants.InstanceOperation.ENABLE);
          ctx.disabled.remove(instance);
          log.add("enable " + instance);
        }
        break;
      case "setOperation": {
        Map<String, Object> map = map(args, "setOperation");
        InstanceConstants.InstanceOperation operation =
            InstanceConstants.InstanceOperation.valueOf(map.get("operation").toString().toUpperCase());
        for (String instance : select(map.get("nodes"), ctx, i -> true)) {
          StateOps.setOperation(state, instance, operation);
          log.add("setOperation " + operation + " " + instance);
        }
        break;
      }
      case "kill":
        for (String instance : select(args, ctx, i -> isLive(state, i))) {
          StateOps.kill(state, instance);
          ctx.killed.add(instance);
          log.add("kill " + instance);
        }
        break;
      case "revive":
        for (String instance : select(args, ctx, i -> !isLive(state, i))) {
          StateOps.revive(state, instance, ctx.generations.merge(instance, 1, Integer::sum));
          ctx.killed.remove(instance);
          log.add("revive " + instance);
        }
        break;
      case "restart":
        for (String instance : select(args, ctx, i -> isLive(state, i))) {
          StateOps.kill(state, instance);
          StateOps.revive(state, instance, ctx.generations.merge(instance, 1, Integer::sum));
          log.add("restart " + instance);
        }
        break;
      case "addNode":
        log.addAll(addNodes(map(args, "addNode"), ctx));
        break;
      case "removeNode":
        for (String instance : select(args, ctx, i -> true)) {
          StateOps.removeInstance(state, instance);
          log.add("removeNode " + instance);
        }
        break;
      case "capacity":
        log.addAll(capacity(map(args, "capacity"), ctx));
        break;
      case "weights":
        log.addAll(weights(map(args, "weights"), state));
        break;
      case "addResource": {
        Map<String, Object> map = map(args, "addResource");
        String name = String.valueOf(map.getOrDefault("name", "added" + state.getIdealStates().size()));
        SpecSource.addResource(state, name, map, ctx.random);
        log.add("addResource " + name);
        break;
      }
      case "removeResource":
        for (String resource : resources(args, state)) {
          state.remove(ClusterState.idealStatePath(resource));
          state.remove(ClusterState.resourceConfigPath(resource));
          state.remove(ClusterState.externalViewPath(resource));
          for (String instance : state.getInstanceNames()) {
            state.remove(ClusterState.currentStatePath(instance, resource));
          }
          log.add("removeResource " + resource);
        }
        break;
      case "partitions":
        log.add(partitions(map(args, "partitions"), state));
        break;
      case "replicas": {
        Map<String, Object> map = map(args, "replicas");
        for (String resource : resources(map.get("resource"), state)) {
          IdealState idealState = state.getIdealStates().get(resource);
          idealState.setReplicas(String.valueOf(map.get("count")));
          state.put(ClusterState.idealStatePath(resource), idealState.getRecord());
          log.add("replicas " + resource + "=" + map.get("count"));
        }
        break;
      }
      case "clusterConfig":
        for (String change : ClusterConfigOverrides.apply(state, map(args, "clusterConfig"))) {
          log.add("clusterConfig " + change);
        }
        break;
      case "constraintWeights": {
        Map<String, Float> weights = new LinkedHashMap<>(ctx.engine.constraintWeights());
        weights.putAll(ScenarioLoader.weights(map(args, "constraintWeights")));
        ctx.engine.setConstraintWeights(weights);
        log.add("constraintWeights " + weights + " (controller restart)");
        break;
      }
      case "maintenance": {
        boolean on = "on".equalsIgnoreCase(String.valueOf(args)) || Boolean.TRUE.equals(args);
        if (on) {
          MaintenanceSignal signal = new MaintenanceSignal("maintenance");
          signal.setReason("Scenario event");
          signal.setTriggeringEntity(MaintenanceSignal.TriggeringEntity.USER);
          signal.setTimestamp(System.currentTimeMillis());
          state.put(StateOps.MAINTENANCE_PATH, signal.getRecord());
        } else {
          state.remove(StateOps.MAINTENANCE_PATH);
        }
        log.add("maintenance " + (on ? "on" : "off"));
        break;
      }
      case "advanceClock": {
        long millis = Durations.parseMillis(args);
        ctx.engine.advanceClock(millis);
        log.add("advanceClock " + Durations.format(millis));
        break;
      }
      case "restartController":
        ctx.engine.restartController();
        log.add("restartController");
        break;
      case "onDemandRebalance": {
        ClusterConfig config = state.getClusterConfig();
        config.setLastOnDemandRebalanceTimestamp(System.currentTimeMillis());
        state.setClusterConfig(config);
        log.add("onDemandRebalance");
        break;
      }
      case "random": {
        Map<String, Object> map = map(args, "random");
        EventSpec inner = new EventSpec();
        inner.kind = String.valueOf(map.get("kind"));
        if (!java.util.Arrays.asList("kill", "revive", "restart", "disable", "enable").contains(inner.kind)) {
          throw new IllegalArgumentException("random supports kill, revive, restart, disable and enable");
        }
        inner.args = "random:" + map.getOrDefault("count", 1);
        log.addAll(apply(inner, ctx));
        break;
      }
      default:
        throw new IllegalArgumentException("Unknown event " + event.kind);
    }
    if (log.isEmpty()) {
      log.add(event.kind + " (no matching target)");
    }
    return log;
  }

  private static InstanceConfig config(ClusterState state, String instance) {
    InstanceConfig config = state.getInstanceConfig(instance);
    if (config == null) {
      throw new IllegalArgumentException("No instance " + instance);
    }
    return config;
  }

  private static boolean isLive(ClusterState state, String instance) {
    return state.get(ClusterState.liveInstancePath(instance)) != null;
  }

  /**
   * Selects instances. Forms: a name, a comma-separated or YAML list of names, {@code all},
   * {@code hottest-top[:N]}, {@code hottest-all[:N]}, {@code random[:N]}, {@code zone:<zone>},
   * {@code re:<regex>}, {@code previously-disabled}, {@code previously-killed}, the removal orders
   * {@code mz-balanced[:N]}, {@code mz-single[:N]}, {@code least-loaded[:N]}, {@code most-loaded[:N]},
   * or a map with {@code pick}, {@code count}, {@code key}, {@code zone}, {@code names}, {@code pattern}.
   */
  @SuppressWarnings("unchecked")
  public static List<String> select(Object spec, Context ctx, Predicate<String> eligible) {
    ClusterState state = ctx.state();
    List<String> universe = state.getInstanceNames().stream().filter(eligible).collect(Collectors.toList());
    String pick;
    int count = 1;
    String key = ctx.focusKey;
    String argument = null;
    if (spec instanceof List) {
      return names((List<Object>) spec, state);
    }
    if (spec instanceof Map) {
      Map<String, Object> map = (Map<String, Object>) spec;
      pick = String.valueOf(map.getOrDefault("pick", map.containsKey("names") ? "names" : "all"));
      count = map.get("count") == null ? 1 : ((Number) map.get("count")).intValue();
      key = map.get("key") == null ? key : map.get("key").toString();
      if (map.get("names") != null) {
        return names(map.get("names") instanceof List ? (List<Object>) map.get("names")
            : Collections.singletonList(map.get("names")), state);
      }
      argument = map.get("zone") != null ? map.get("zone").toString()
          : map.get("pattern") != null ? map.get("pattern").toString() : null;
    } else {
      String text = String.valueOf(spec).trim();
      int colon = text.indexOf(':');
      pick = colon < 0 ? text : text.substring(0, colon);
      String rest = colon < 0 ? null : text.substring(colon + 1);
      if (pick.equals("zone") || pick.equals("re")) {
        argument = rest;
      } else if (rest != null) {
        count = Integer.parseInt(rest.trim());
      }
    }
    switch (pick) {
      case "all":
        return universe;
      case "previously-disabled":
        return ctx.disabled.stream().filter(universe::contains).collect(Collectors.toList());
      case "previously-killed":
        return ctx.killed.stream().filter(universe::contains).collect(Collectors.toList());
      case "hottest-top":
      case "hottest-all": {
        Map<String, NodeStats> nodes = ctx.lastNodes != null ? ctx.lastNodes
            : new StatsCollector(state, key).nodes(state.getServedLayout());
        String k = key;
        boolean top = pick.equals("hottest-top");
        return universe.stream().filter(i -> nodes.containsKey(i) && nodes.get(i).serving)
            .sorted(Comparator.comparingDouble((String i) ->
                top ? nodes.get(i).topUtil(k) : nodes.get(i).allUtil(k)).reversed()
                .thenComparing(Comparator.naturalOrder()))
            .limit(count).collect(Collectors.toList());
      }
      case "random": {
        List<String> shuffled = new ArrayList<>(universe);
        Collections.shuffle(shuffled, ctx.random);
        return shuffled.subList(0, Math.min(count, shuffled.size()));
      }
      case "mz-balanced":
      case "mz-single":
      case "least-loaded":
      case "most-loaded": {
        Map<String, NodeStats> nodes = ctx.lastNodes != null ? ctx.lastNodes
            : new StatsCollector(state, key).nodes(state.getServedLayout());
        List<String> serving = universe.stream().filter(i -> nodes.containsKey(i) && nodes.get(i).serving)
            .collect(Collectors.toList());
        List<String> order = RemovalOrder.order(RemovalOrder.Strategy.parse(pick), serving, nodes, key, ctx.random);
        return order.subList(0, Math.min(count, order.size()));
      }
      case "zone": {
        String zone = argument;
        Map<String, NodeStats> nodes = new StatsCollector(state, key).nodes(null);
        return universe.stream().filter(i -> nodes.containsKey(i) && zone.equals(nodes.get(i).zone))
            .collect(Collectors.toList());
      }
      case "re":
      case "regex": {
        Pattern pattern = Pattern.compile(argument);
        return universe.stream().filter(i -> pattern.matcher(i).matches()).collect(Collectors.toList());
      }
      case "names":
        return Collections.emptyList();
      default:
        return names(Collections.singletonList(spec), state);
    }
  }

  private static List<String> names(List<Object> items, ClusterState state) {
    List<String> result = new ArrayList<>();
    for (Object item : items) {
      for (String name : String.valueOf(item).split(",")) {
        String trimmed = name.trim();
        if (trimmed.isEmpty()) {
          continue;
        }
        if (state.get(ClusterState.instanceConfigPath(trimmed)) == null) {
          throw new IllegalArgumentException("No instance '" + trimmed + "'");
        }
        result.add(trimmed);
      }
    }
    return result;
  }

  @SuppressWarnings("unchecked")
  private static List<String> resources(Object spec, ClusterState state) {
    Set<String> all = state.getIdealStates().keySet();
    if (spec == null || "all".equals(spec)) {
      return new ArrayList<>(all);
    }
    List<String> result = new ArrayList<>();
    List<Object> items = spec instanceof List ? (List<Object>) spec : Collections.singletonList(spec);
    for (Object item : items) {
      String text = String.valueOf(item);
      if (text.startsWith("re:")) {
        Pattern pattern = Pattern.compile(text.substring(3));
        all.stream().filter(r -> pattern.matcher(r).matches()).forEach(result::add);
      } else if (all.contains(text)) {
        result.add(text);
      } else {
        throw new IllegalArgumentException("No resource '" + text + "'");
      }
    }
    return result;
  }

  @SuppressWarnings("unchecked")
  private static List<String> addNodes(Map<String, Object> map, Context ctx) {
    ClusterState state = ctx.state();
    List<String> log = new ArrayList<>();
    int count = ((Number) map.getOrDefault("count", 1)).intValue();
    String prefix = String.valueOf(map.getOrDefault("prefix", "added"));
    ClusterConfig clusterConfig = state.getClusterConfig();
    String faultZoneType = clusterConfig.getFaultZoneType() == null ? "zone" : clusterConfig.getFaultZoneType();
    InstanceConfig like = map.get("like") == null ? null : config(state, map.get("like").toString());
    List<String> zones = map.get("zones") != null ? ClusterConfigOverrides.strings(map.get("zones"))
        : map.get("zone") != null ? Collections.singletonList(map.get("zone").toString()) : null;
    if (zones == null) {
      Set<String> known = new LinkedHashSet<>();
      new StatsCollector(state, ctx.focusKey).nodes(null).values().forEach(n -> {
        if (n.zone != null) {
          known.add(n.zone);
        }
      });
      zones = known.isEmpty() ? Collections.singletonList("z00") : new ArrayList<>(known);
    }
    Map<String, Integer> capacity = new TreeMap<>();
    if (like != null) {
      capacity.putAll(like.getInstanceCapacityMap());
    }
    if (map.get("capacity") != null) {
      ((Map<String, Object>) map.get("capacity")).forEach((k, v) ->
          capacity.put(k, (int) Math.round(Double.parseDouble(v.toString()))));
    }
    List<String> tags = map.get("tags") != null ? ClusterConfigOverrides.strings(map.get("tags"))
        : like != null ? like.getTags() : Collections.emptyList();
    boolean live = Boolean.parseBoolean(String.valueOf(map.getOrDefault("live", true)));
    for (int i = 0; i < count; i++) {
      String name;
      do {
        name = String.format("%s_%03d", prefix, ctx._added++);
      } while (state.get(ClusterState.instanceConfigPath(name)) != null);
      String zone = zones.get(i % zones.size());
      SpecSource.addInstance(state, name, faultZoneType, zone, capacity, tags, live);
      log.add("addNode " + name + " zone=" + zone);
    }
    return log;
  }

  @SuppressWarnings("unchecked")
  private static List<String> capacity(Map<String, Object> map, Context ctx) {
    ClusterState state = ctx.state();
    List<String> log = new ArrayList<>();
    ClusterConfig clusterConfig = state.getClusterConfig();
    for (String instance : select(map.getOrDefault("nodes", "all"), ctx, i -> true)) {
      InstanceConfig config = config(state, instance);
      Map<String, Integer> capacity = new TreeMap<>(clusterConfig.getDefaultInstanceCapacityMap());
      capacity.putAll(config.getInstanceCapacityMap());
      if (map.get("factor") != null) {
        double factor = Double.parseDouble(map.get("factor").toString());
        String key = map.get("key") == null ? null : map.get("key").toString();
        capacity.replaceAll((k, v) -> key == null || key.equals(k) ? (int) Math.round(v * factor) : v);
      }
      if (map.get("set") != null) {
        ((Map<String, Object>) map.get("set")).forEach((k, v) ->
            capacity.put(k, (int) Math.round(Double.parseDouble(v.toString()))));
      }
      config.setInstanceCapacityMap(capacity);
      state.putInstanceConfig(config);
      log.add("capacity " + instance + "=" + capacity);
    }
    return log;
  }

  @SuppressWarnings("unchecked")
  private static List<String> weights(Map<String, Object> map, ClusterState state) throws IOException {
    List<String> log = new ArrayList<>();
    ClusterConfig clusterConfig = state.getClusterConfig();
    Set<String> partitionFilter = map.get("partitions") == null ? null
        : new LinkedHashSet<>(ClusterConfigOverrides.strings(map.get("partitions")));
    String key = map.get("key") == null ? null : map.get("key").toString();
    Double factor = map.get("factor") == null ? null : Double.parseDouble(map.get("factor").toString());
    Map<String, Object> set = (Map<String, Object>) map.get("set");
    if (factor == null && set == null) {
      throw new IllegalArgumentException("weights needs 'factor' or 'set'");
    }
    for (String resource : resources(map.getOrDefault("resource", "all"), state)) {
      ResourceConfig config = state.getResourceConfigs().get(resource);
      if (config == null) {
        config = new ResourceConfig(resource);
      }
      Map<String, Map<String, Integer>> capacity = new TreeMap<>();
      config.getPartitionCapacityMap().forEach((p, w) -> capacity.put(p, new TreeMap<>(w)));
      if (!capacity.containsKey(ResourceConfig.DEFAULT_PARTITION_KEY)) {
        capacity.put(ResourceConfig.DEFAULT_PARTITION_KEY,
            new TreeMap<>(clusterConfig.getDefaultPartitionWeightMap()));
      }
      Map<String, Integer> defaults = capacity.get(ResourceConfig.DEFAULT_PARTITION_KEY);
      if (partitionFilter != null) {
        for (String partition : partitionFilter) {
          capacity.computeIfAbsent(partition, p -> new TreeMap<>(defaults));
        }
      }
      for (Map.Entry<String, Map<String, Integer>> entry : capacity.entrySet()) {
        boolean selected = partitionFilter == null || partitionFilter.contains(entry.getKey());
        if (!selected) {
          continue;
        }
        Map<String, Integer> weight = entry.getValue();
        for (String k : clusterConfig.getInstanceCapacityKeys()) {
          weight.putIfAbsent(k, clusterConfig.getDefaultPartitionWeightMap().getOrDefault(k, 0));
        }
        if (factor != null) {
          weight.replaceAll((k, v) -> key == null || key.equals(k) ? (int) Math.round(v * factor) : v);
        }
        if (set != null) {
          set.forEach((k, v) -> weight.put(k, (int) Math.round(Double.parseDouble(v.toString()))));
        }
      }
      config.setPartitionCapacityMap(capacity);
      state.put(ClusterState.resourceConfigPath(resource), config.getRecord());
      log.add("weights " + resource + (factor != null ? " x" + factor : "") + (set != null ? " set " + set : "")
          + (key != null ? " on " + key : "") + (partitionFilter != null ? " for " + partitionFilter.size()
          + " partitions" : ""));
    }
    return log;
  }

  private static String partitions(Map<String, Object> map, ClusterState state) throws IOException {
    String resource = String.valueOf(map.get("resource"));
    IdealState idealState = state.getIdealStates().get(resource);
    if (idealState == null) {
      throw new IllegalArgumentException("No resource '" + resource + "'");
    }
    int count = ((Number) map.get("count")).intValue();
    int current = idealState.getPartitionSet().size();
    if (count > current) {
      for (int p = current; p < count; p++) {
        idealState.setPreferenceList(resource + "_" + p, new ArrayList<>());
      }
    } else {
      List<String> names = new ArrayList<>(idealState.getPartitionSet());
      names.sort(Comparator.comparingInt((String n) -> suffix(n)).thenComparing(Comparator.naturalOrder()));
      for (String name : names.subList(count, names.size())) {
        idealState.getRecord().getListFields().remove(name);
        idealState.getRecord().getMapFields().remove(name);
      }
    }
    idealState.setNumPartitions(count);
    state.put(ClusterState.idealStatePath(resource), idealState.getRecord());
    return "partitions " + resource + " " + current + " -> " + count;
  }

  private static int suffix(String partition) {
    int index = partition.lastIndexOf('_');
    try {
      return Integer.parseInt(partition.substring(index + 1));
    } catch (RuntimeException e) {
      return Integer.MAX_VALUE;
    }
  }

  @SuppressWarnings("unchecked")
  private static Map<String, Object> map(Object args, String kind) {
    if (!(args instanceof Map)) {
      throw new IllegalArgumentException("'" + kind + "' needs a map of options");
    }
    return (Map<String, Object>) args;
  }
}
