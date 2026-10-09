package org.apache.helix.wagedsim.cluster;

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

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.stream.Collectors;

import org.apache.helix.controller.rebalancer.waged.WagedRebalancer;
import org.apache.helix.model.ClusterConfig;
import org.apache.helix.model.CurrentState;
import org.apache.helix.model.ExternalView;
import org.apache.helix.model.IdealState;
import org.apache.helix.model.InstanceConfig;
import org.apache.helix.model.LiveInstance;
import org.apache.helix.model.ParticipantHistory;
import org.apache.helix.model.ResourceConfig;
import org.apache.helix.model.StateModelDefinition;
import org.apache.helix.zookeeper.datamodel.ZNRecord;

/**
 * In-memory definition of one cluster: its znodes, keyed by path relative to the cluster root, plus
 * the WAGED baseline and best possible assignments. Current states are keyed without the session
 * level ({@code INSTANCES/<instance>/CURRENTSTATES/<resource>}). Typed getters return copies (Helix
 * model classes copy their record); write changes back with the {@code put} methods.
 */
public final class ClusterState {
  public static final String CONFIGS_CLUSTER = "CONFIGS/CLUSTER";
  public static final String CONFIGS_PARTICIPANT = "CONFIGS/PARTICIPANT";
  public static final String CONFIGS_RESOURCE = "CONFIGS/RESOURCE";
  public static final String IDEALSTATES = "IDEALSTATES";
  public static final String STATEMODELDEFS = "STATEMODELDEFS";
  public static final String LIVEINSTANCES = "LIVEINSTANCES";
  public static final String EXTERNALVIEW = "EXTERNALVIEW";
  public static final String INSTANCES = "INSTANCES";

  private final String _clusterName;
  private final TreeMap<String, ZNRecord> _znodes = new TreeMap<>();
  private Map<String, Map<String, Map<String, String>>> _baseline;
  private Map<String, Map<String, Map<String, String>>> _bestPossible;
  private Manifest _manifest = new Manifest();

  public ClusterState(String clusterName) {
    _clusterName = checkName(clusterName);
    _manifest.cluster = clusterName;
  }

  /**
   * Cluster names become folder names in the workspace, so they must be a single safe path segment.
   *
   * @throws IllegalArgumentException for names with separators, {@code .}, {@code ..} or other characters
   */
  public static String checkName(String clusterName) {
    if (clusterName == null || !clusterName.matches("[A-Za-z0-9_][A-Za-z0-9_.@+=,:-]*")) {
      throw new IllegalArgumentException("Invalid cluster name '" + clusterName
          + "': use letters, digits and _ . @ + = , : - (no path separators)");
    }
    return clusterName;
  }

  public String getClusterName() {
    return _clusterName;
  }

  public Manifest getManifest() {
    return _manifest;
  }

  public void setManifest(Manifest manifest) {
    _manifest = manifest;
  }

  // ---- paths ----

  public static String clusterConfigPath(String cluster) {
    return CONFIGS_CLUSTER + "/" + cluster;
  }

  public static String instanceConfigPath(String instance) {
    return CONFIGS_PARTICIPANT + "/" + instance;
  }

  public static String resourceConfigPath(String resource) {
    return CONFIGS_RESOURCE + "/" + resource;
  }

  public static String idealStatePath(String resource) {
    return IDEALSTATES + "/" + resource;
  }

  public static String stateModelDefPath(String model) {
    return STATEMODELDEFS + "/" + model;
  }

  public static String liveInstancePath(String instance) {
    return LIVEINSTANCES + "/" + instance;
  }

  public static String externalViewPath(String resource) {
    return EXTERNALVIEW + "/" + resource;
  }

  public static String currentStatePath(String instance, String resource) {
    return INSTANCES + "/" + instance + "/CURRENTSTATES/" + resource;
  }

  public static String historyPath(String instance) {
    return INSTANCES + "/" + instance + "/HISTORY";
  }

  // ---- raw access ----

  public ZNRecord get(String path) {
    return _znodes.get(path);
  }

  public void put(String path, ZNRecord record) {
    _znodes.put(path, record);
  }

  public ZNRecord remove(String path) {
    return _znodes.remove(path);
  }

  /** Removes the node at {@code prefix} and every node below it. */
  public void removeTree(String prefix) {
    _znodes.remove(prefix);
    _znodes.subMap(prefix + "/", prefix + "/\uffff").clear();
  }

  public Map<String, ZNRecord> getZnodes() {
    return _znodes;
  }

  /** @return records directly under {@code parent}, keyed by child name */
  public Map<String, ZNRecord> children(String parent) {
    String prefix = parent + "/";
    Map<String, ZNRecord> result = new TreeMap<>();
    for (Map.Entry<String, ZNRecord> entry : _znodes.subMap(prefix, prefix + "\uffff").entrySet()) {
      String rest = entry.getKey().substring(prefix.length());
      if (!rest.contains("/")) {
        result.put(rest, entry.getValue());
      }
    }
    return result;
  }

  // ---- typed access ----

  public ClusterConfig getClusterConfig() {
    ZNRecord record = get(clusterConfigPath(_clusterName));
    return record == null ? null : new ClusterConfig(record);
  }

  public void setClusterConfig(ClusterConfig config) {
    put(clusterConfigPath(_clusterName), config.getRecord());
  }

  public Map<String, InstanceConfig> getInstanceConfigs() {
    Map<String, InstanceConfig> result = new TreeMap<>();
    children(CONFIGS_PARTICIPANT).forEach((name, record) -> result.put(name, new InstanceConfig(record)));
    return result;
  }

  public InstanceConfig getInstanceConfig(String instance) {
    ZNRecord record = get(instanceConfigPath(instance));
    return record == null ? null : new InstanceConfig(record);
  }

  public void putInstanceConfig(InstanceConfig config) {
    put(instanceConfigPath(config.getInstanceName()), config.getRecord());
  }

  public void putLiveInstance(LiveInstance liveInstance) {
    put(liveInstancePath(liveInstance.getInstanceName()), liveInstance.getRecord());
  }

  public ParticipantHistory getHistory(String instance) {
    ZNRecord record = get(historyPath(instance));
    return record == null ? null : new ParticipantHistory(record);
  }

  public void putHistory(String instance, ParticipantHistory history) {
    put(historyPath(instance), history.getRecord());
  }

  public void putCurrentState(String instance, CurrentState currentState) {
    put(currentStatePath(instance, currentState.getResourceName()), currentState.getRecord());
  }

  public Map<String, LiveInstance> getLiveInstances() {
    Map<String, LiveInstance> result = new TreeMap<>();
    children(LIVEINSTANCES).forEach((name, record) -> result.put(name, new LiveInstance(record)));
    return result;
  }

  public Map<String, ResourceConfig> getResourceConfigs() {
    Map<String, ResourceConfig> result = new TreeMap<>();
    children(CONFIGS_RESOURCE).forEach((name, record) -> result.put(name, new ResourceConfig(record)));
    return result;
  }

  public Map<String, IdealState> getIdealStates() {
    Map<String, IdealState> result = new TreeMap<>();
    children(IDEALSTATES).forEach((name, record) -> result.put(name, new IdealState(record)));
    return result;
  }

  /** @return resources whose ideal state uses the WAGED rebalancer */
  public Map<String, IdealState> getWagedIdealStates() {
    return getIdealStates().entrySet().stream().filter(e -> isWaged(e.getValue()))
        .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue, (a, b) -> a, TreeMap::new));
  }

  /**
   * @return false for znodes of resources that WAGED does not manage. Engines simulate WAGED
   *         resources only; other resources stay in the folder untouched.
   */
  public static boolean isSimulated(String path, java.util.Set<String> wagedResources) {
    String[] parts = path.split("/");
    if (parts.length == 2 && (parts[0].equals(IDEALSTATES) || parts[0].equals(EXTERNALVIEW))) {
      return wagedResources.contains(parts[1]);
    }
    if (parts.length == 3 && path.startsWith(CONFIGS_RESOURCE + "/")) {
      return wagedResources.contains(parts[2]);
    }
    if (parts.length == 4 && parts[0].equals(INSTANCES) && parts[2].equals("CURRENTSTATES")) {
      return wagedResources.contains(parts[3]);
    }
    return true;
  }

  public static boolean isWaged(IdealState idealState) {
    return idealState.getRebalanceMode() == IdealState.RebalanceMode.FULL_AUTO
        && WagedRebalancer.class.getName().equals(idealState.getRebalancerClassName());
  }

  public Map<String, StateModelDefinition> getStateModelDefs() {
    Map<String, StateModelDefinition> result = new TreeMap<>();
    children(STATEMODELDEFS).forEach((name, record) -> result.put(name, new StateModelDefinition(record)));
    return result;
  }

  public Map<String, ExternalView> getExternalViews() {
    Map<String, ExternalView> result = new TreeMap<>();
    children(EXTERNALVIEW).forEach((name, record) -> result.put(name, new ExternalView(record)));
    return result;
  }

  /** @return instance -> resource -> current state */
  public Map<String, Map<String, CurrentState>> getCurrentStates() {
    Map<String, Map<String, CurrentState>> result = new TreeMap<>();
    String prefix = INSTANCES + "/";
    for (Map.Entry<String, ZNRecord> entry : _znodes.subMap(prefix, prefix + "\uffff").entrySet()) {
      String[] parts = entry.getKey().split("/");
      if (parts.length == 4 && parts[2].equals("CURRENTSTATES")) {
        result.computeIfAbsent(parts[1], k -> new TreeMap<>())
            .put(parts[3], new CurrentState(entry.getValue()));
      }
    }
    return result;
  }

  public Map<String, ParticipantHistory> getHistories() {
    Map<String, ParticipantHistory> result = new TreeMap<>();
    for (String instance : instanceNamesUnderInstances()) {
      ZNRecord record = get(historyPath(instance));
      if (record != null) {
        result.put(instance, new ParticipantHistory(record));
      }
    }
    return result;
  }

  private Collection<String> instanceNamesUnderInstances() {
    TreeSet<String> names = new TreeSet<>();
    String prefix = INSTANCES + "/";
    for (String path : _znodes.subMap(prefix, prefix + "\uffff").keySet()) {
      names.add(path.split("/")[1]);
    }
    return names;
  }

  /** Removes all current states and replaces them with the given layout for live instances. */
  public void replaceCurrentStates(Map<String, Map<String, Map<String, String>>> layout) {
    for (String instance : instanceNamesUnderInstances()) {
      removeTree(INSTANCES + "/" + instance + "/CURRENTSTATES");
    }
    Map<String, LiveInstance> live = getLiveInstances();
    Map<String, IdealState> idealStates = getIdealStates();
    Map<String, Map<String, CurrentState>> byInstance = new TreeMap<>();
    layout.forEach((resource, partitions) -> partitions.forEach((partition, replicas) -> replicas
        .forEach((instance, state) -> {
          if (!live.containsKey(instance)) {
            return;
          }
          CurrentState currentState = byInstance.computeIfAbsent(instance, k -> new TreeMap<>())
              .computeIfAbsent(resource, r -> {
                CurrentState cs = new CurrentState(r);
                IdealState idealState = idealStates.get(r);
                if (idealState != null) {
                  cs.setStateModelDefRef(idealState.getStateModelDefRef());
                }
                cs.setSessionId(live.get(instance).getEphemeralOwner());
                return cs;
              });
          currentState.setState(partition, state);
        })));
    byInstance.forEach((instance, resources) -> resources
        .forEach((resource, cs) -> put(currentStatePath(instance, resource), cs.getRecord())));
  }

  /** @return resource -> partition -> instance -> state, from the current states of live instances */
  public Map<String, Map<String, Map<String, String>>> getServedLayout() {
    Map<String, Map<String, Map<String, String>>> layout = new TreeMap<>();
    Map<String, LiveInstance> live = getLiveInstances();
    getCurrentStates().forEach((instance, resources) -> {
      if (!live.containsKey(instance)) {
        return;
      }
      resources.forEach((resource, cs) -> cs.getPartitionStateMap().forEach((partition, state) ->
          layout.computeIfAbsent(resource, k -> new TreeMap<>())
              .computeIfAbsent(partition, k -> new TreeMap<>()).put(instance, state)));
    });
    return layout;
  }

  public Map<String, Map<String, Map<String, String>>> getBaseline() {
    return _baseline;
  }

  public void setBaseline(Map<String, Map<String, Map<String, String>>> baseline) {
    _baseline = baseline;
  }

  public Map<String, Map<String, Map<String, String>>> getBestPossible() {
    return _bestPossible;
  }

  public void setBestPossible(Map<String, Map<String, Map<String, String>>> bestPossible) {
    _bestPossible = bestPossible;
  }

  public ClusterState copy() {
    ClusterState copy = new ClusterState(_clusterName);
    _znodes.forEach((path, record) -> copy._znodes.put(path, new ZNRecord(record)));
    copy._baseline = Layouts.copy(_baseline);
    copy._bestPossible = Layouts.copy(_bestPossible);
    copy._manifest = _manifest.copy();
    return copy;
  }

  /** @return instance names with an instance config, in name order */
  public List<String> getInstanceNames() {
    return new java.util.ArrayList<>(children(CONFIGS_PARTICIPANT).keySet());
  }
}
