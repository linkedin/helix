package org.apache.helix.wagedsim.engine.dryrun;

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

import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.stream.Collectors;

import org.apache.helix.HelixConstants;
import org.apache.helix.HelixRebalanceException;
import org.apache.helix.controller.dataproviders.ResourceControllerDataProvider;
import org.apache.helix.controller.pipeline.Stage;
import org.apache.helix.controller.rebalancer.util.DelayedRebalanceUtil;
import org.apache.helix.controller.rebalancer.util.WagedRebalanceUtil;
import org.apache.helix.controller.rebalancer.waged.model.ClusterModel;
import org.apache.helix.controller.rebalancer.waged.model.ClusterModelProvider;
import org.apache.helix.controller.stages.AttributeName;
import org.apache.helix.controller.stages.BestPossibleStateCalcStage;
import org.apache.helix.controller.stages.BestPossibleStateOutput;
import org.apache.helix.controller.stages.ClusterEvent;
import org.apache.helix.controller.stages.ClusterEventType;
import org.apache.helix.controller.stages.CurrentStateComputationStage;
import org.apache.helix.controller.stages.ResourceComputationStage;
import org.apache.helix.controller.common.PartitionStateMap;
import org.apache.helix.manager.zk.ZKHelixDataAccessor;
import org.apache.helix.model.ClusterConfig;
import org.apache.helix.model.LiveInstance;
import org.apache.helix.model.MaintenanceSignal;
import org.apache.helix.model.Partition;
import org.apache.helix.model.Resource;
import org.apache.helix.model.ResourceAssignment;
import org.apache.helix.monitoring.metrics.WagedRebalancerMetricCollector;
import org.apache.helix.monitoring.metrics.model.CountMetric;
import org.apache.helix.util.RebalanceUtil;
import org.apache.helix.wagedsim.cluster.ClusterState;
import org.apache.helix.wagedsim.cluster.Layouts;
import org.apache.helix.wagedsim.engine.Engine;
import org.apache.helix.wagedsim.engine.EngineSettings;
import org.apache.helix.wagedsim.engine.RoundResult;
import org.apache.helix.wagedsim.engine.StateOps;
import org.apache.helix.zookeeper.datamodel.ZNRecord;

/**
 * Runs the controller's rebalance stages in process, on an in-memory copy of the cluster: the real
 * data provider, the real WAGED rebalancer (global, emergency and partial passes, delay window,
 * state mapping) and the real constraint algorithm. State transitions complete instantly, so the
 * served layout after a round is the best possible state map the stages produced.
 */
public class DryRunEngine implements Engine {
  private static final String[] SKELETON = {"CONFIGS", "CONFIGS/CLUSTER", "CONFIGS/PARTICIPANT",
      "CONFIGS/RESOURCE", "IDEALSTATES", "EXTERNALVIEW", "LIVEINSTANCES", "INSTANCES",
      "STATEMODELDEFS", "CONTROLLER", "PROPERTYSTORE"};
  private static final String[] COUNTERS = {"GlobalBaselineCalcCounter", "PartialRebalanceCounter",
      "EmergencyRebalanceCounter", "RebalanceOverwriteCounter", "RebalanceFailureCounter"};

  private ClusterState _state;
  private EngineSettings _settings;
  private String _root;
  private InMemoryBaseDataAccessor _store;
  private ZKHelixDataAccessor _accessor;
  private ResourceControllerDataProvider _provider;
  private final SimWagedRebalancer.InMemoryBuckets _buckets = new SimWagedRebalancer.InMemoryBuckets();
  private SimWagedRebalancer _rebalancer;
  private SwappableAlgorithm _algorithm;
  private WagedRebalancerMetricCollector _metrics;
  private final Map<String, ZNRecord> _synced = new HashMap<>();
  private Map<String, Long> _lastCounters = new HashMap<>();
  /** Virtual time minus wall time. */
  private long _shift;
  private boolean _restartPending;

  @Override
  public String mode() {
    return "dry-run";
  }

  @Override
  public void start(ClusterState state, EngineSettings settings) {
    _state = state;
    _settings = settings.copy();
    _root = "/" + state.getClusterName();
    long wallNow = System.currentTimeMillis();
    Long captured = state.getManifest().capturedAtMillis;
    if (captured != null && captured > 0 && captured < wallNow) {
      // Start the virtual clock at the capture time, so delay windows are as they were then.
      StateOps.shiftTimestamps(state, wallNow - captured);
      _shift = captured - wallNow;
    }
    _store = new InMemoryBaseDataAccessor();
    for (String path : SKELETON) {
      _store.create(_root + "/" + path, null, 0);
    }
    _accessor = new ZKHelixDataAccessor(state.getClusterName(), _store);
    _provider = new ResourceControllerDataProvider(state.getClusterName());
    if (state.getBaseline() != null || state.getBestPossible() != null) {
      SimWagedRebalancer.Store seed = new SimWagedRebalancer.Store(_buckets, state.getClusterName());
      if (state.getBaseline() != null) {
        seed.persistBaseline(Layouts.toAssignments(state.getBaseline()));
      }
      if (state.getBestPossible() != null) {
        seed.persistBestPossibleAssignment(Layouts.toAssignments(state.getBestPossible()));
      }
    }
    newRebalancer(_settings.firstRound == EngineSettings.FirstRound.STEADY);
  }

  private void newRebalancer(boolean prime) {
    if (_rebalancer != null) {
      _rebalancer.close();
    }
    _algorithm = new SwappableAlgorithm(preferences(), _settings.constraintWeights);
    _metrics = new WagedRebalancerMetricCollector();
    _rebalancer = new SimWagedRebalancer(
        new SimWagedRebalancer.Store(_buckets, _state.getClusterName()), _algorithm, _metrics);
    _lastCounters = counters();
    if (prime) {
      sync();
      refresh();
      _rebalancer.primeChangeDetector(_provider);
    }
  }

  private Map<ClusterConfig.GlobalRebalancePreferenceKey, Integer> preferences() {
    ClusterConfig config = _state.getClusterConfig();
    return new HashMap<>(config.getGlobalRebalancePreference());
  }

  @Override
  public ClusterState state() {
    return _state;
  }

  @Override
  public long now() {
    return System.currentTimeMillis() + _shift;
  }

  @Override
  public void advanceClock(long millis) {
    if (millis <= 0) {
      return;
    }
    StateOps.shiftTimestamps(_state, -millis);
    _shift += millis;
  }

  @Override
  public void restartController() {
    _restartPending = true;
  }

  /** Changes the constraint weights; the controller restarts, as it must in production. */
  @Override
  public void setConstraintWeights(Map<String, Float> weights) {
    _settings.constraintWeights = new LinkedHashMap<>(weights);
    _restartPending = true;
  }

  @Override
  public Map<String, Float> constraintWeights() {
    return new LinkedHashMap<>(_settings.constraintWeights);
  }

  public EngineSettings settings() {
    return _settings;
  }

  @Override
  public RoundResult runRound(int round) throws Exception {
    RoundResult result = new RoundResult();
    result.round = round;
    long start = System.currentTimeMillis();
    if (_restartPending) {
      newRebalancer(false);
      _restartPending = false;
      result.notes.add("controller restarted");
    } else {
      _algorithm.update(preferences(), _settings.constraintWeights);
    }
    sync();
    refresh();
    Map<String, Map<String, Map<String, String>>> served;
    if (_settings.pass == EngineSettings.Pass.AUTO) {
      served = runPipeline(result);
    } else {
      served = runForcedPass(result);
    }
    _state.replaceCurrentStates(served);
    pullBack();
    result.blockingConstraints.putAll(_algorithm.drainBlocking());
    result.computeMillis = System.currentTimeMillis() - start;
    result.simTimeMillis = now();
    result.maintenance = StateOps.isMaintenance(_state);
    return result;
  }

  /** @return the message of the innermost rebalance failure, without the type and category suffix */
  static String rootMessage(HelixRebalanceException failure) {
    Throwable root = failure;
    while (root.getCause() instanceof HelixRebalanceException && root.getCause() != root) {
      root = root.getCause();
    }
    String message = String.valueOf(root.getMessage());
    return message.replaceAll("\\s*Failure Type: \\S+( Category: \\S+)?\\s*$", "");
  }

  private Map<String, Map<String, Map<String, String>>> runPipeline(RoundResult result)
      throws Exception {
    ClusterEvent event = new ClusterEvent(_state.getClusterName(), ClusterEventType.Unknown);
    event.addAttribute(AttributeName.ControllerDataProvider.name(), _provider);
    event.addAttribute(AttributeName.STATEFUL_REBALANCER.name(), _rebalancer);
    run(event, new ResourceComputationStage());
    run(event, new CurrentStateComputationStage());
    run(event, new BestPossibleStateCalcStage());
    BestPossibleStateOutput output = event.getAttribute(AttributeName.BEST_POSSIBLE_STATE.name());
    Map<String, Map<String, Map<String, String>>> layout = new TreeMap<>();
    if (output != null) {
      for (String resource : output.resourceSet()) {
        PartitionStateMap map = output.getPartitionStateMap(resource);
        Map<String, Map<String, String>> partitions = new TreeMap<>();
        for (Map.Entry<Partition, Map<String, String>> entry : map.getStateMap().entrySet()) {
          Map<String, String> replicas = new TreeMap<>(entry.getValue());
          replicas.values().removeIf("DROPPED"::equals);
          partitions.put(entry.getKey().getPartitionName(), replicas);
        }
        layout.put(resource, partitions);
      }
    }
    _state.setBaseline(Layouts.fromAssignments(_rebalancer.getStore().getBaseline()));
    _state.setBestPossible(Layouts.fromAssignments(_rebalancer.getStore().getBestPossibleAssignment()));
    recordCounters(result);
    for (HelixRebalanceException failure : _rebalancer.drainFailures()) {
      result.failures.add(failure.getFailureType() + "/" + failure.getFailureCategory() + ": "
          + rootMessage(failure));
      result.failureCategories.add(String.valueOf(failure.getFailureCategory()));
    }
    if (_provider.isMaintenanceModeEnabled() && !StateOps.isMaintenance(_state)) {
      MaintenanceSignal signal = new MaintenanceSignal("maintenance");
      signal.setReason("Entered automatically: too many instances unable to accept online replicas");
      signal.setTriggeringEntity(MaintenanceSignal.TriggeringEntity.CONTROLLER);
      signal.setAutoTriggerReason(
          MaintenanceSignal.AutoTriggerReason.MAX_INSTANCES_UNABLE_TO_ACCEPT_ONLINE_REPLICAS);
      signal.setTimestamp(System.currentTimeMillis());
      _state.put(StateOps.MAINTENANCE_PATH, signal.getRecord());
      result.notes.add("controller entered maintenance mode");
    }
    return layout;
  }

  private Map<String, Map<String, Map<String, String>>> runForcedPass(RoundResult result)
      throws Exception {
    ClusterEvent event = new ClusterEvent(_state.getClusterName(), ClusterEventType.Unknown);
    event.addAttribute(AttributeName.ControllerDataProvider.name(), _provider);
    run(event, new ResourceComputationStage());
    Map<String, Resource> all = event.getAttribute(AttributeName.RESOURCES_TO_REBALANCE.name());
    Set<String> waged = _state.getWagedIdealStates().keySet();
    Map<String, Resource> resources = all.entrySet().stream().filter(e -> waged.contains(e.getKey()))
        .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
    Map<String, Map<String, Map<String, String>>> served = _state.getServedLayout();
    Map<String, ResourceAssignment> baseline = withFallback(_state.getBaseline(), served, resources);
    Map<String, ResourceAssignment> best = withFallback(_state.getBestPossible(), served, resources);
    ClusterModel model;
    Map<HelixConstants.ChangeType, Set<String>> fullRecompute = Collections.singletonMap(
        HelixConstants.ChangeType.CLUSTER_CONFIG, Collections.singleton(_state.getClusterName()));
    switch (_settings.pass) {
      case PARTIAL:
        model = ClusterModelProvider.generateClusterModelForPartialRebalance(_provider, resources,
            activeNodes(), baseline, best);
        break;
      case GLOBAL:
        model = ClusterModelProvider.generateClusterModelForBaseline(_provider, resources,
            globalNodes(), fullRecompute, baseline);
        break;
      case COLD:
        model = ClusterModelProvider.generateClusterModelForBaseline(_provider, resources,
            globalNodes(), fullRecompute, Collections.emptyMap());
        break;
      default:
        throw new IllegalStateException("Not a forced pass: " + _settings.pass);
    }
    Map<String, ResourceAssignment> assignment;
    try {
      assignment = WagedRebalanceUtil.calculateAssignment(model, _algorithm);
    } catch (HelixRebalanceException e) {
      result.failures.add(e.getFailureType() + "/" + e.getFailureCategory() + ": " + e.getMessage());
      result.failureCategories.add(String.valueOf(e.getFailureCategory()));
      return served;
    }
    Map<String, Map<String, Map<String, String>>> layout = Layouts.fromAssignments(assignment);
    result.passes.put(_settings.pass.name().toLowerCase(), 1L);
    _state.setBestPossible(layout);
    if (_settings.pass != EngineSettings.Pass.PARTIAL) {
      _state.setBaseline(Layouts.copy(layout));
      _rebalancer.getStore().persistBaseline(assignment);
    }
    _rebalancer.getStore().persistBestPossibleAssignment(assignment);
    return Layouts.copy(layout);
  }

  /** Global passes use every assignable node, unless the settings restrict them to enabled-live nodes. */
  private Set<String> globalNodes() {
    if (_settings.activeNodes == EngineSettings.ActiveNodes.ENABLED_LIVE) {
      return activeNodes();
    }
    return _provider.getAssignableInstances();
  }

  private Set<String> activeNodes() {
    if (_settings.activeNodes == EngineSettings.ActiveNodes.ENABLED_LIVE) {
      Set<String> nodes = new HashSet<>(_provider.getEnabledLiveInstances());
      nodes.retainAll(_provider.getAssignableInstances());
      return nodes;
    }
    return DelayedRebalanceUtil.getActiveNodes(_provider.getAssignableInstances(),
        _provider.getEnabledLiveInstances(), _provider.getInstanceOfflineTimeMap(),
        _provider.getAssignableLiveInstances().keySet(), _provider.getAssignableInstanceConfigMap(),
        _provider.getClusterConfig());
  }

  /** The assignment for every resource, taking missing resources from the served layout. */
  private static Map<String, ResourceAssignment> withFallback(
      Map<String, Map<String, Map<String, String>>> stored,
      Map<String, Map<String, Map<String, String>>> served, Map<String, Resource> resources) {
    Map<String, Map<String, Map<String, String>>> merged = new TreeMap<>();
    for (String resource : resources.keySet()) {
      if (stored != null && stored.containsKey(resource)) {
        merged.put(resource, stored.get(resource));
      } else if (served.containsKey(resource)) {
        merged.put(resource, served.get(resource));
      }
    }
    return Layouts.toAssignments(merged);
  }

  private static void run(ClusterEvent event, Stage stage) throws Exception {
    RebalanceUtil.runStage(event, stage);
  }

  /** Writes the cluster definition into the in-memory store; only changed nodes get new versions. */
  private void sync() {
    Map<String, ZNRecord> desired = new HashMap<>();
    Map<String, LiveInstance> live = _state.getLiveInstances();
    String clusterConfigPath = ClusterState.clusterConfigPath(_state.getClusterName());
    Set<String> waged = _state.getWagedIdealStates().keySet();
    for (Map.Entry<String, ZNRecord> entry : _state.getZnodes().entrySet()) {
      String path = entry.getKey();
      ZNRecord record = entry.getValue();
      if (!ClusterState.isSimulated(path, waged)) {
        continue;
      }
      String absolute;
      if (path.startsWith(ClusterState.INSTANCES + "/") && path.contains("/CURRENTSTATES/")) {
        String[] parts = path.split("/");
        LiveInstance liveInstance = live.get(parts[1]);
        if (liveInstance == null) {
          continue;
        }
        absolute = _root + "/INSTANCES/" + parts[1] + "/CURRENTSTATES/"
            + liveInstance.getEphemeralOwner() + "/" + parts[3];
      } else {
        absolute = _root + "/" + path;
        if (path.equals(clusterConfigPath)) {
          // Global passes run synchronously in a dry run.
          record = new ZNRecord(record);
          record.setSimpleField(ClusterConfig.ClusterConfigProperty.GLOBAL_REBALANCE_ASYNC_MODE.name(),
              "false");
        }
      }
      desired.put(absolute, record);
    }
    for (String instance : _state.getInstanceNames()) {
      _store.create(_root + "/INSTANCES/" + instance + "/MESSAGES", null, 0);
    }
    for (Map.Entry<String, ZNRecord> entry : desired.entrySet()) {
      ZNRecord previous = _synced.get(entry.getKey());
      if (previous == null || !previous.equals(entry.getValue())
          || !String.valueOf(previous.getId()).equals(entry.getValue().getId())) {
        _store.set(entry.getKey(), entry.getValue(), 0);
        _synced.put(entry.getKey(), new ZNRecord(entry.getValue()));
      }
    }
    for (String path : new HashSet<>(_synced.keySet())) {
      if (!desired.containsKey(path)) {
        _store.remove(path, 0);
        _synced.remove(path);
      }
    }
  }

  private void refresh() {
    _provider.requireFullRefresh();
    _provider.refresh(_accessor);
  }

  /** Copies back what the controller logic wrote (participant history), so the next sync keeps it. */
  private void pullBack() {
    for (String instance : _state.getInstanceNames()) {
      String path = ClusterState.historyPath(instance);
      String absolute = _root + "/" + path;
      ZNRecord stored = _store.get(absolute, null, 0);
      if (stored != null && !stored.equals(_state.get(path))) {
        ZNRecord copy = new ZNRecord(stored);
        _state.put(path, copy);
        _synced.put(absolute, new ZNRecord(copy));
      }
    }
  }

  private Map<String, Long> counters() {
    Map<String, Long> values = new HashMap<>();
    for (String name : COUNTERS) {
      CountMetric metric = _metrics.getMetric(name, CountMetric.class);
      values.put(name, metric == null ? 0L : metric.getLastEmittedMetricValue());
    }
    return values;
  }

  private void recordCounters(RoundResult result) {
    Map<String, Long> now = counters();
    result.passes.put("global", delta(now, "GlobalBaselineCalcCounter"));
    result.passes.put("partial", delta(now, "PartialRebalanceCounter"));
    result.passes.put("emergency", delta(now, "EmergencyRebalanceCounter"));
    result.passes.put("overwrite", delta(now, "RebalanceOverwriteCounter"));
    result.passes.put("failures", delta(now, "RebalanceFailureCounter"));
    _lastCounters = now;
  }

  private long delta(Map<String, Long> now, String name) {
    return now.getOrDefault(name, 0L) - _lastCounters.getOrDefault(name, 0L);
  }

  @Override
  public void close() {
    if (_rebalancer != null) {
      _rebalancer.close();
      _rebalancer = null;
    }
  }
}
