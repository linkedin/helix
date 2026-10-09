package org.apache.helix.wagedsim.source;

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
import java.io.Reader;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collections;
import java.util.EnumMap;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.TreeMap;

import org.apache.helix.controller.rebalancer.waged.WagedRebalancer;
import org.apache.helix.model.BuiltInStateModelDefinitions;
import org.apache.helix.model.ClusterConfig;
import org.apache.helix.model.IdealState;
import org.apache.helix.model.InstanceConfig;
import org.apache.helix.model.LiveInstance;
import org.apache.helix.model.ParticipantHistory;
import org.apache.helix.model.ResourceConfig;
import org.apache.helix.model.StateModelDefinition;
import org.apache.helix.wagedsim.cluster.ClusterNormalizer;
import org.apache.helix.wagedsim.cluster.ClusterState;
import org.apache.helix.wagedsim.cluster.Manifest;
import org.apache.helix.wagedsim.engine.EngineSettings;
import org.apache.helix.wagedsim.engine.dryrun.DryRunEngine;
import org.apache.helix.wagedsim.scenario.ClusterConfigOverrides;
import org.apache.helix.wagedsim.util.Durations;
import org.yaml.snakeyaml.Yaml;

/**
 * Builds a cluster from a spec: node groups, resource groups with weight distributions, cluster
 * config and an initial placement. Generation is seeded, so the same spec gives the same cluster.
 */
public final class SpecSource {
  private SpecSource() {
  }

  public static Map<String, Object> readYaml(Path file) throws IOException {
    try (Reader reader = Files.newBufferedReader(file, StandardCharsets.UTF_8)) {
      Object loaded = new Yaml().load(reader);
      if (!(loaded instanceof Map)) {
        throw new IOException(file + " does not contain a YAML map");
      }
      @SuppressWarnings("unchecked")
      Map<String, Object> map = (Map<String, Object>) loaded;
      return map;
    }
  }

  public static ClusterState load(Path specFile) throws Exception {
    ClusterState state = build(readYaml(specFile));
    state.getManifest().sourceDetail = specFile.toAbsolutePath().toString();
    return state;
  }

  @SuppressWarnings("unchecked")
  public static ClusterState build(Map<String, Object> spec) throws Exception {
    Map<String, Object> cluster = map(spec.get("cluster"), "cluster");
    String name = string(cluster, "name", "SIM_CLUSTER");
    long seed = ((Number) spec.getOrDefault("seed", 7)).longValue();
    Random random = new Random(seed);
    ClusterState state = new ClusterState(name);
    Manifest manifest = state.getManifest();
    manifest.source = "spec";
    manifest.capturedAtMillis = null;
    manifest.capturedAt = java.time.Instant.now().toString();

    ClusterConfig config = new ClusterConfig(name);
    List<String> keys = strings(cluster.getOrDefault("capacityKeys", Collections.singletonList("CU")));
    config.setInstanceCapacityKeys(keys);
    Map<String, Integer> defaultCapacity = ints(map(cluster.get("instanceCapacity"), "instanceCapacity"));
    config.setDefaultInstanceCapacityMap(defaultCapacity);
    Map<String, Integer> defaultWeight = new LinkedHashMap<>();
    for (String key : keys) {
      defaultWeight.put(key, 1);
    }
    if (cluster.get("defaultPartitionWeight") != null) {
      defaultWeight.putAll(ints(map(cluster.get("defaultPartitionWeight"), "defaultPartitionWeight")));
    }
    config.setDefaultPartitionWeightMap(defaultWeight);
    Map<ClusterConfig.GlobalRebalancePreferenceKey, Integer> preference =
        new EnumMap<>(ClusterConfig.GlobalRebalancePreferenceKey.class);
    preference.put(ClusterConfig.GlobalRebalancePreferenceKey.EVENNESS, 1);
    preference.put(ClusterConfig.GlobalRebalancePreferenceKey.LESS_MOVEMENT, 1);
    if (cluster.get("rebalancePreference") != null) {
      map(cluster.get("rebalancePreference"), "rebalancePreference").forEach((k, v) -> preference
          .put(ClusterConfig.GlobalRebalancePreferenceKey.valueOf(k), Integer.parseInt(v.toString())));
    }
    config.setGlobalRebalancePreference(preference);
    Map<String, Object> delay = cluster.get("delayRebalance") == null ? Collections.emptyMap()
        : map(cluster.get("delayRebalance"), "delayRebalance");
    config.setDelayRebalaceEnabled(Boolean.parseBoolean(String.valueOf(delay.getOrDefault("enabled", false))));
    if (delay.get("time") != null) {
      config.setRebalanceDelayTime(Durations.parseMillis(delay.get("time")));
    }
    Map<String, Object> topology = cluster.get("topology") == null ? Collections.emptyMap()
        : map(cluster.get("topology"), "topology");
    String faultZoneType = string(topology, "faultZoneType", "zone");
    config.setTopology(string(topology, "path", "/" + faultZoneType + "/instance"));
    config.setFaultZoneType(faultZoneType);
    config.setTopologyAwareEnabled(Boolean.parseBoolean(String.valueOf(topology.getOrDefault("enabled", true))));
    if (cluster.get("preferredScoringKeys") != null) {
      config.setPreferredScoringKeys(strings(cluster.get("preferredScoringKeys")));
    }
    if (cluster.get("maxOfflineInstancesAllowed") != null) {
      config.setMaxOfflineInstancesAllowed(Integer.parseInt(cluster.get("maxOfflineInstancesAllowed").toString()));
    }
    state.setClusterConfig(config);
    if (cluster.get("config") != null) {
      ClusterConfigOverrides.apply(state, map(cluster.get("config"), "cluster.config"));
    }

    // Instances.
    List<Object> groups = list(spec.get("instances"), "instances");
    int groupIndex = 0;
    for (Object groupSpec : groups) {
      Map<String, Object> group = map(groupSpec, "instances[" + groupIndex + "]");
      int count = ((Number) group.getOrDefault("count", 1)).intValue();
      String prefix = string(group, "prefix", groupIndex == 0 ? "node" : "node" + groupIndex);
      int zones = ((Number) group.getOrDefault("zones", Math.min(count, 3))).intValue();
      Map<String, Integer> capacity = group.get("capacity") == null ? Collections.emptyMap()
          : ints(map(group.get("capacity"), "capacity"));
      List<String> tags = group.get("tags") == null ? Collections.emptyList() : strings(group.get("tags"));
      int width = String.valueOf(Math.max(count - 1, 1)).length();
      for (int i = 0; i < count; i++) {
        String instance = String.format("%s_%0" + width + "d", prefix, i);
        String zone = String.format("z%02d", i % Math.max(1, zones));
        addInstance(state, instance, faultZoneType, zone, capacity, tags, true);
      }
      groupIndex++;
    }

    // Resources.
    List<Object> resources = list(spec.get("resources"), "resources");
    int resourceIndex = 0;
    for (Object resourceSpec : resources) {
      Map<String, Object> group = map(resourceSpec, "resources[" + resourceIndex + "]");
      int count = ((Number) group.getOrDefault("count", 1)).intValue();
      String prefix = string(group, "prefix", "db" + resourceIndex);
      for (int i = 0; i < count; i++) {
        String resource = group.get("name") != null && count == 1 ? group.get("name").toString()
            : String.format("%s%d", group.get("name") != null ? group.get("name") : prefix, i);
        addResource(state, resource, group, random);
      }
      resourceIndex++;
    }

    // Liveness.
    Map<String, Object> liveness = spec.get("liveness") == null ? Collections.emptyMap()
        : map(spec.get("liveness"), "liveness");
    List<String> instances = new ArrayList<>(state.getInstanceNames());
    for (String instance : pick(liveness.get("down"), instances, random)) {
      state.remove(ClusterState.liveInstancePath(instance));
      ParticipantHistory history = state.getHistory(instance);
      history.reportOffline();
      state.putHistory(instance, history);
    }
    for (String instance : pick(liveness.get("disabled"), instances, random)) {
      InstanceConfig instanceConfig = state.getInstanceConfig(instance);
      instanceConfig.setInstanceOperation(
          org.apache.helix.constants.InstanceConstants.InstanceOperation.DISABLE);
      state.putInstanceConfig(instanceConfig);
    }

    ClusterNormalizer.normalize(state, false);
    List<String> problems = ClusterNormalizer.validate(state);
    if (!problems.isEmpty()) {
      throw new IllegalArgumentException("Invalid spec: " + String.join("; ", problems));
    }
    String placement = String.valueOf(spec.getOrDefault("initialPlacement", "cold-start"));
    switch (placement) {
      case "cold-start":
        coldStart(state);
        break;
      case "random":
        randomPlacement(state, random);
        break;
      case "none":
        break;
      default:
        throw new IllegalArgumentException("initialPlacement must be cold-start, random or none");
    }
    ClusterNormalizer.updateCounts(state);
    manifest.notes.add("Generated from a spec with seed " + seed + ", initial placement " + placement);
    return state;
  }

  /** Adds an instance (config, live instance and history) to a cluster definition. */
  public static void addInstance(ClusterState state, String instance, String faultZoneType, String zone,
      Map<String, Integer> capacity, List<String> tags, boolean live) {
    InstanceConfig config = new InstanceConfig(instance);
    config.setHostName(instance);
    config.setPort("12000");
    Map<String, String> domain = new LinkedHashMap<>();
    domain.put(faultZoneType, zone);
    domain.put("instance", instance);
    config.setDomain(domain);
    if (!capacity.isEmpty()) {
      config.setInstanceCapacityMap(new HashMap<>(capacity));
    }
    tags.forEach(config::addTag);
    state.put(ClusterState.instanceConfigPath(instance), config.getRecord());
    ParticipantHistory history = new ParticipantHistory(instance);
    if (live) {
      String session = ClusterNormalizer.syntheticSession(instance, 0);
      LiveInstance liveInstance = new LiveInstance(instance);
      liveInstance.setSessionId(session);
      liveInstance.setHelixVersion(ClusterNormalizer.toolHelixVersion());
      state.put(ClusterState.liveInstancePath(instance), liveInstance.getRecord());
      history.reportOnline(session, ClusterNormalizer.toolHelixVersion());
    }
    state.put(ClusterState.historyPath(instance), history.getRecord());
  }

  /** Adds a WAGED resource (ideal state, resource config, state model) from a resource spec. */
  public static void addResource(ClusterState state, String resource, Map<String, Object> spec,
      Random random) throws IOException {
    int partitions = ((Number) spec.getOrDefault("partitions", 16)).intValue();
    int replicas = ((Number) spec.getOrDefault("replicas", 3)).intValue();
    String model = string(spec, "stateModel", "MasterSlave");
    IdealState idealState = new IdealState(resource);
    idealState.setRebalanceMode(IdealState.RebalanceMode.FULL_AUTO);
    idealState.setRebalancerClassName(WagedRebalancer.class.getName());
    idealState.setNumPartitions(partitions);
    idealState.setReplicas(String.valueOf(replicas));
    idealState.setStateModelDefRef(model);
    if (spec.get("minActiveReplicas") != null) {
      idealState.setMinActiveReplicas(((Number) spec.get("minActiveReplicas")).intValue());
    }
    if (spec.get("tag") != null) {
      idealState.setInstanceGroupTag(spec.get("tag").toString());
    }
    for (int p = 0; p < partitions; p++) {
      idealState.setPreferenceList(resource + "_" + p, new ArrayList<>());
    }
    state.put(ClusterState.idealStatePath(resource), idealState.getRecord());
    ensureStateModel(state, model);

    ClusterConfig config = state.getClusterConfig();
    List<String> keys = config.getInstanceCapacityKeys();
    Map<String, WeightDistribution> distributions = new LinkedHashMap<>();
    Map<String, Object> weightSpec = spec.get("weights") == null ? Collections.emptyMap()
        : map(spec.get("weights"), "weights");
    for (String key : keys) {
      Object value = weightSpec.get(key);
      distributions.put(key, WeightDistribution.parse(
          value != null ? value : config.getDefaultPartitionWeightMap().getOrDefault(key, 1)));
    }
    Map<String, Map<String, Integer>> capacityMap = new TreeMap<>();
    Map<String, Integer> defaults = new LinkedHashMap<>();
    distributions.forEach((key, distribution) -> defaults.put(key, distribution.sample(random, 0)));
    capacityMap.put(ResourceConfig.DEFAULT_PARTITION_KEY, defaults);
    boolean allConstant = distributions.values().stream().allMatch(WeightDistribution::isConstant);
    Map<String, Object> outliers = spec.get("outliers") == null ? null : map(spec.get("outliers"), "outliers");
    if (!allConstant || outliers != null) {
      for (int p = 0; p < partitions; p++) {
        Map<String, Integer> weight = new LinkedHashMap<>();
        for (Map.Entry<String, WeightDistribution> entry : distributions.entrySet()) {
          weight.put(entry.getKey(), entry.getValue().sample(random, p));
        }
        capacityMap.put(resource + "_" + p, weight);
      }
      if (outliers != null) {
        int count = ((Number) outliers.getOrDefault("count", 1)).intValue();
        double factor = Double.parseDouble(outliers.getOrDefault("factor", 10).toString());
        String key = string(outliers, "key", keys.get(0));
        // Distinct partitions: each outlier gets the factor once.
        List<Integer> indices = new ArrayList<>();
        for (int p = 0; p < partitions; p++) {
          indices.add(p);
        }
        Collections.shuffle(indices, random);
        for (int index : indices.subList(0, Math.min(count, partitions))) {
          Map<String, Integer> weight = capacityMap.get(resource + "_" + index);
          weight.put(key, (int) Math.round(weight.get(key) * factor));
        }
      }
    }
    ResourceConfig resourceConfig = new ResourceConfig(resource);
    resourceConfig.setPartitionCapacityMap(capacityMap);
    state.put(ClusterState.resourceConfigPath(resource), resourceConfig.getRecord());
  }

  public static void ensureStateModel(ClusterState state, String model) {
    if (state.get(ClusterState.stateModelDefPath(model)) != null) {
      return;
    }
    StateModelDefinition definition;
    try {
      definition = BuiltInStateModelDefinitions.valueOf(model).getStateModelDefinition();
    } catch (IllegalArgumentException e) {
      throw new IllegalArgumentException("Unknown state model " + model
          + "; built-in models: MasterSlave, LeaderStandby, OnlineOffline, OnlineOfflineWithBootstrap");
    }
    state.put(ClusterState.stateModelDefPath(model), definition.getRecord());
  }

  /** Places everything with a WAGED cold start, as a controller does for a new cluster. */
  public static void coldStart(ClusterState state) throws Exception {
    EngineSettings settings = new EngineSettings();
    settings.firstRound = EngineSettings.FirstRound.RESTART;
    try (DryRunEngine engine = new DryRunEngine()) {
      engine.start(state, settings);
      engine.runRound(1);
    }
  }

  /** Random placement on enabled live instances, spreading replicas across zones where possible. */
  private static void randomPlacement(ClusterState state, Random random) {
    List<String> nodes = new ArrayList<>();
    state.getInstanceConfigs().forEach((name, config) -> {
      if (config.getInstanceEnabled() && state.getLiveInstances().containsKey(name)) {
        nodes.add(name);
      }
    });
    Map<String, StateModelDefinition> models = state.getStateModelDefs();
    Map<String, Map<String, Map<String, String>>> layout = new TreeMap<>();
    state.getWagedIdealStates().forEach((resource, idealState) -> {
      StateModelDefinition model = models.get(idealState.getStateModelDefRef());
      List<String> states = model.getStatesPriorityList();
      String top = model.getTopState();
      String second = states.size() > 1 ? states.get(1) : top;
      int replicas = idealState.getReplicaCount(nodes.size());
      Map<String, Map<String, String>> partitions = new TreeMap<>();
      for (String partition : idealState.getPartitionSet()) {
        List<String> shuffled = new ArrayList<>(nodes);
        Collections.shuffle(shuffled, random);
        Map<String, String> replicaMap = new TreeMap<>();
        for (int i = 0; i < Math.min(replicas, shuffled.size()); i++) {
          replicaMap.put(shuffled.get(i), i == 0 ? top : second);
        }
        partitions.put(partition, replicaMap);
      }
      layout.put(resource, partitions);
    });
    state.replaceCurrentStates(layout);
    state.setBaseline(org.apache.helix.wagedsim.cluster.Layouts.copy(layout));
    state.setBestPossible(org.apache.helix.wagedsim.cluster.Layouts.copy(layout));
  }

  private static List<String> pick(Object spec, List<String> instances, Random random) {
    if (spec == null) {
      return Collections.emptyList();
    }
    if (spec instanceof Number) {
      List<String> shuffled = new ArrayList<>(instances);
      Collections.shuffle(shuffled, random);
      return shuffled.subList(0, Math.min(((Number) spec).intValue(), shuffled.size()));
    }
    return strings(spec);
  }

  @SuppressWarnings("unchecked")
  static Map<String, Object> map(Object value, String name) {
    if (!(value instanceof Map)) {
      throw new IllegalArgumentException("'" + name + "' must be a map");
    }
    return (Map<String, Object>) value;
  }

  @SuppressWarnings("unchecked")
  static List<Object> list(Object value, String name) {
    if (!(value instanceof List)) {
      throw new IllegalArgumentException("'" + name + "' must be a list");
    }
    return (List<Object>) value;
  }

  static String string(Map<String, Object> map, String key, String defaultValue) {
    Object value = map.get(key);
    return value == null ? defaultValue : value.toString();
  }

  @SuppressWarnings("unchecked")
  static List<String> strings(Object value) {
    List<String> result = new ArrayList<>();
    if (value instanceof List) {
      for (Object item : (List<Object>) value) {
        result.add(String.valueOf(item));
      }
    } else if (value != null) {
      for (String item : value.toString().split(",")) {
        if (!item.trim().isEmpty()) {
          result.add(item.trim());
        }
      }
    }
    return result;
  }

  static Map<String, Integer> ints(Map<String, Object> value) {
    Map<String, Integer> result = new LinkedHashMap<>();
    value.forEach((k, v) -> result.put(k, (int) Math.round(Double.parseDouble(v.toString()))));
    return result;
  }
}
