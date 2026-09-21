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
import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.Collections;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;
import org.apache.helix.HelixConstants;
import org.apache.helix.HelixException;
import org.apache.helix.HelixRebalanceException;
import org.apache.helix.constants.InstanceConstants;
import org.apache.helix.controller.dataproviders.ResourceControllerDataProvider;
import org.apache.helix.controller.rebalancer.waged.constraints.ConstraintBasedAlgorithmFactory;
import org.apache.helix.controller.rebalancer.waged.model.ClusterModel;
import org.apache.helix.controller.rebalancer.waged.model.OptimalAssignment;
import org.apache.helix.controller.stages.CurrentStateOutput;
import org.apache.helix.model.BuiltInStateModelDefinitions;
import org.apache.helix.model.ClusterConfig;
import org.apache.helix.model.IdealState;
import org.apache.helix.model.InstanceConfig;
import org.apache.helix.model.LiveInstance;
import org.apache.helix.model.Partition;
import org.apache.helix.model.Resource;
import org.apache.helix.model.ResourceAssignment;
import org.apache.helix.model.ResourceConfig;
import org.apache.helix.model.StateModelDefinition;
import org.apache.helix.monitoring.mbeans.ClusterStatusMonitor;
import org.apache.helix.zookeeper.datamodel.ZNRecord;
import org.testng.Assert;

/**
 * A small, faithful Helix controller loop that drives the real WAGED rebalancer through every
 * rebalance scope. Every pipeline run rebuilds the Helix objects from plain specs, so no two runs
 * ever share a mutable record, and the store copies on persist like the ZooKeeper backed one.
 */
final class ScopeMatrixSim implements AutoCloseable {
  static final String DISK = "DISK";
  static final String MASTER_SLAVE = BuiltInStateModelDefinitions.MasterSlave.name();
  private static final EnumSet<HelixConstants.ChangeType> ALL_CHANGES = EnumSet.of(
      HelixConstants.ChangeType.CLUSTER_CONFIG, HelixConstants.ChangeType.INSTANCE_CONFIG,
      HelixConstants.ChangeType.IDEAL_STATE, HelixConstants.ChangeType.RESOURCE_CONFIG,
      HelixConstants.ChangeType.LIVE_INSTANCE);

  static final class NodeSpec {
    final String name;
    final Set<String> tags = new TreeSet<>();
    Integer disk;
    InstanceConstants.InstanceOperation operation = InstanceConstants.InstanceOperation.ENABLE;
    boolean live = true;
    boolean hasConfig = true;
    Long offlineSince;
    String domain;
    int session;
    /** Resource to partitions disabled on this node. */
    final Map<String, List<String>> disabledPartitions = new TreeMap<>();

    NodeSpec(String name) {
      this.name = name;
    }

    InstanceConfig toConfig() {
      InstanceConfig config = new InstanceConfig(name);
      tags.forEach(config::addTag);
      if (disk != null) {
        config.setInstanceCapacityMap(Collections.singletonMap(DISK, disk));
      }
      config.setInstanceOperation(operation);
      if (domain != null) {
        config.setDomain(domain);
      }
      disabledPartitions.forEach((resource, partitions) -> partitions
          .forEach(partition -> config.setInstanceEnabledForPartition(resource, partition, false)));
      return config;
    }

    LiveInstance toLive() {
      LiveInstance live = new LiveInstance(name);
      live.setSessionId(name + "_session_" + session);
      return live;
    }
  }

  static final class ResourceSpec {
    final String name;
    String tag;
    int partitions;
    int weight;
    final Map<String, Integer> partitionWeights = new TreeMap<>();
    int replicas = 2;
    int minActive = -1;

    ResourceSpec(String name, String tag, int partitions, int weight) {
      this.name = name;
      this.tag = tag;
      this.partitions = partitions;
      this.weight = weight;
    }

    List<String> partitionNames() {
      List<String> names = new ArrayList<>();
      for (int i = 0; i < partitions; i++) {
        names.add(name + "_" + i);
      }
      return names;
    }

    int weightOf(String partition) {
      return partitionWeights.getOrDefault(partition, weight);
    }

    IdealState toIdealState() {
      IdealState idealState = new IdealState(name);
      idealState.setRebalanceMode(IdealState.RebalanceMode.FULL_AUTO);
      idealState.setRebalancerClassName(WagedRebalancer.class.getName());
      idealState.setStateModelDefRef(MASTER_SLAVE);
      idealState.setReplicas(String.valueOf(replicas));
      idealState.setNumPartitions(partitions);
      if (tag != null) {
        idealState.setInstanceGroupTag(tag);
      }
      if (minActive >= 0) {
        idealState.setMinActiveReplicas(minActive);
      }
      for (String partition : partitionNames()) {
        idealState.getRecord().setListField(partition, new ArrayList<>());
      }
      return idealState;
    }

    ResourceConfig toResourceConfig() throws IOException {
      ResourceConfig config = new ResourceConfig(name);
      Map<String, Map<String, Integer>> capacity = new HashMap<>();
      capacity.put(ResourceConfig.DEFAULT_PARTITION_KEY, Collections.singletonMap(DISK, weight));
      partitionWeights.forEach((p, w) -> capacity.put(p, Collections.singletonMap(DISK, w)));
      config.setPartitionCapacityMap(capacity);
      return config;
    }

    Resource toResource() {
      Resource resource = new Resource(name);
      resource.setStateModelDefRef(MASTER_SLAVE);
      partitionNames().forEach(resource::addPartition);
      return resource;
    }
  }

  static final class Calc {
    final ClusterModel.RebalanceScopeType scope;
    final Set<String> toAssign;
    volatile Set<String> skipped = Collections.emptySet();
    volatile HelixRebalanceException failure;

    Calc(ClusterModel.RebalanceScopeType scope, Set<String> toAssign) {
      this.scope = scope;
      this.toAssign = toAssign;
    }

    @Override
    public String toString() {
      return scope + " toAssign=" + toAssign + " skipped=" + skipped + " failure="
          + (failure == null ? null : failure.getFailureType());
    }
  }

  static final class Report {
    final ClusterModel.RebalanceScopeType scope;
    final Set<String> evaluated;
    final Set<String> skipped;

    Report(ClusterModel.RebalanceScopeType scope, Set<String> evaluated, Set<String> skipped) {
      this.scope = scope;
      this.evaluated = evaluated;
      this.skipped = skipped;
    }

    @Override
    public String toString() {
      return scope + " evaluated=" + evaluated + " skipped=" + skipped;
    }
  }

  /** Everything observed around one pipeline run. */
  static final class Run {
    Map<String, ResourceAssignment> baselineBefore;
    Map<String, ResourceAssignment> bestBefore;
    Map<String, ResourceAssignment> baselineAfter;
    Map<String, ResourceAssignment> bestAfter;
    Map<String, ResourceAssignment> emitted;
    HelixRebalanceException computeFailure;
    HelixRebalanceException pipelineFailure;
    Map<String, IdealState> idealStates;
    List<Calc> calcs;
    List<Report> reports;
    long gauge;
    Set<String> retry;

    List<Calc> calcs(ClusterModel.RebalanceScopeType scope) {
      return calcs.stream().filter(c -> c.scope == scope).collect(Collectors.toList());
    }

    Set<String> skippedIn(ClusterModel.RebalanceScopeType scope) {
      Set<String> skipped = new TreeSet<>();
      calcs(scope).forEach(c -> skipped.addAll(c.skipped));
      return skipped;
    }

    @Override
    public String toString() {
      return "calcs=" + calcs + " reports=" + reports + " gauge=" + gauge + " retry=" + retry
          + " computeFailure=" + (computeFailure == null ? null : computeFailure.getMessage());
    }
  }

  static final class SimProvider extends ResourceControllerDataProvider {
    volatile Map<String, Long> offlineTimes = new HashMap<>();

    SimProvider(String cluster) {
      super(cluster);
    }

    @Override
    public Map<String, Long> getInstanceOfflineTimeMap() {
      return offlineTimes;
    }
  }

  /**
   * Copies on every write and serves reads from memory, re-reading its durable copy after a reset,
   * which is what the ZooKeeper backed store does. The plain mock keeps references, which lets a
   * later in-place merge leak into what was "persisted".
   */
  static final class SimStore extends MockAssignmentMetadataStore {
    private volatile Map<String, ResourceAssignment> _durableBaseline;
    private volatile Map<String, ResourceAssignment> _durableBest;
    volatile boolean failBaselinePersist;
    volatile boolean failBestPersist;
    volatile int baselineWrites;
    volatile int bestWrites;

    SimStore() {
      this(Collections.emptyMap(), Collections.emptyMap());
    }

    /** A store whose durable copy already holds these maps, as a new leader finds it. */
    SimStore(Map<String, ResourceAssignment> baseline, Map<String, ResourceAssignment> best) {
      _durableBaseline = copyOf(baseline);
      _durableBest = copyOf(best);
    }

    @Override
    public Map<String, ResourceAssignment> getBaseline() {
      if (_globalBaseline == null) {
        synchronized (this) {
          if (_globalBaseline == null) {
            _globalBaseline = copyOf(_durableBaseline);
          }
        }
      }
      return _globalBaseline;
    }

    @Override
    public synchronized void persistBaseline(Map<String, ResourceAssignment> baseline) {
      if (failBaselinePersist) {
        throw new HelixException("Injected baseline persistence failure");
      }
      _durableBaseline = copyOf(baseline);
      Map<String, ResourceAssignment> memory = getBaseline();
      memory.clear();
      memory.putAll(copyOf(baseline));
      baselineWrites++;
    }

    @Override
    public Map<String, ResourceAssignment> getBestPossibleAssignment() {
      if (_bestPossibleAssignment == null) {
        synchronized (this) {
          if (_bestPossibleAssignment == null) {
            _bestPossibleAssignment = copyOf(_durableBest);
          }
        }
      }
      return _bestPossibleAssignment;
    }

    @Override
    public synchronized void persistBestPossibleAssignment(
        Map<String, ResourceAssignment> bestPossible) {
      if (failBestPersist) {
        throw new HelixException("Injected best possible persistence failure");
      }
      _durableBest = copyOf(bestPossible);
      Map<String, ResourceAssignment> memory = getBestPossibleAssignment();
      memory.clear();
      memory.putAll(copyOf(bestPossible));
      _bestPossibleVersion++;
      _lastPersistedBestPossibleVersion = _bestPossibleVersion;
      bestWrites++;
    }

    @Override
    public synchronized boolean asyncUpdateBestPossibleAssignmentCache(
        Map<String, ResourceAssignment> bestPossible, int newVersion) {
      if (newVersion > _bestPossibleVersion) {
        Map<String, ResourceAssignment> memory = getBestPossibleAssignment();
        memory.clear();
        memory.putAll(bestPossible);
        _bestPossibleVersion = newVersion;
        return true;
      }
      return false;
    }

    @Override
    protected synchronized void reset() {
      _globalBaseline = null;
      _bestPossibleAssignment = null;
    }

    Map<String, ResourceAssignment> durableBaseline() {
      return copyOf(_durableBaseline);
    }

    Map<String, ResourceAssignment> durableBest() {
      return copyOf(_durableBest);
    }
  }

  /** Records every calculation and outcome report while delegating to the real algorithm. */
  static final class RecordingAlgorithm implements RebalanceAlgorithm {
    private final RebalanceAlgorithm _delegate =
        ConstraintBasedAlgorithmFactory.getInstance(Collections.emptyMap());
    final List<Calc> calcs = new CopyOnWriteArrayList<>();
    final List<Report> reports = new CopyOnWriteArrayList<>();
    volatile ClusterModel.RebalanceScopeType failScope;
    volatile RuntimeException failWith;
    volatile CountDownLatch pauseGate;
    volatile CountDownLatch pausedSignal;
    volatile ClusterModel.RebalanceScopeType pauseScope;

    @Override
    public OptimalAssignment calculate(ClusterModel model) throws HelixRebalanceException {
      Calc calc = new Calc(model.getRebalanceScopeType(),
          new TreeSet<>(model.getAssignableReplicaMap().keySet()));
      calcs.add(calc);
      CountDownLatch gate = pauseGate;
      if (gate != null && model.getRebalanceScopeType() == pauseScope) {
        pausedSignal.countDown();
        try {
          gate.await(30, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
          Thread.currentThread().interrupt();
        }
      }
      if (failWith != null && model.getRebalanceScopeType() == failScope) {
        RuntimeException failure = failWith;
        failWith = null;
        throw failure;
      }
      try {
        OptimalAssignment result = _delegate.calculate(model);
        calc.skipped = new TreeSet<>(result.getSkippedResources());
        return result;
      } catch (HelixRebalanceException e) {
        calc.failure = e;
        throw e;
      }
    }

    @Override
    public void onAssignmentComputed(ClusterModel.RebalanceScopeType scope,
        Set<String> evaluatedResources, Set<String> skippedResources) {
      reports.add(new Report(scope, new TreeSet<>(evaluatedResources),
          new TreeSet<>(skippedResources)));
      _delegate.onAssignmentComputed(scope, evaluatedResources, skippedResources);
    }
  }

  /** Captures exactly what computeBestPossibleAssignment emitted, before the mapping calculator. */
  static final class CapturingRebalancer extends WagedRebalancer {
    volatile Map<String, ResourceAssignment> lastEmitted;
    volatile HelixRebalanceException lastFailure;

    CapturingRebalancer(AssignmentMetadataStore store, RebalanceAlgorithm algorithm) {
      super(store, algorithm, Optional.empty());
    }

    @Override
    protected Map<String, ResourceAssignment> computeBestPossibleAssignment(
        ResourceControllerDataProvider clusterData, Map<String, Resource> resourceMap,
        Set<String> activeNodes, CurrentStateOutput currentStateOutput,
        RebalanceAlgorithm algorithm) throws HelixRebalanceException {
      try {
        Map<String, ResourceAssignment> result = super.computeBestPossibleAssignment(clusterData,
            resourceMap, activeNodes, currentStateOutput, algorithm);
        lastEmitted = copyOf(result);
        return result;
      } catch (HelixRebalanceException e) {
        lastFailure = e;
        throw e;
      }
    }
  }

  final String cluster;
  final ClusterConfig clusterConfig;
  final Map<String, NodeSpec> nodes = new TreeMap<>();
  final Map<String, ResourceSpec> resources = new TreeMap<>();
  // What the Helix controller leader holds in memory. newLeader() replaces all of it.
  SimProvider provider;
  SimStore store;
  RecordingAlgorithm algorithm;
  CapturingRebalancer rebalancer;
  ClusterStatusMonitor monitor;
  CurrentStateOutput currentState = new CurrentStateOutput();
  /** Per resource partition weights as of the last run that computed the resource freshly. */
  final Map<String, Map<String, Integer>> computedWeights = new HashMap<>();
  /** What each serving phase last reported as skipped, while the flag was on. */
  final Map<ClusterModel.RebalanceScopeType, Set<String>> lastSkipped = new TreeMap<>();

  ScopeMatrixSim(String cluster, boolean isolation) {
    this.cluster = cluster;
    clusterConfig = new ClusterConfig(cluster);
    clusterConfig.setInstanceCapacityKeys(Collections.singletonList(DISK));
    clusterConfig.setDefaultInstanceCapacityMap(Collections.singletonMap(DISK, 100));
    clusterConfig.setDefaultPartitionWeightMap(Collections.singletonMap(DISK, 10));
    clusterConfig.setWagedInstanceTagIsolationEnabled(isolation);
    lead(new SimStore());
  }

  private void lead(SimStore leaderStore) {
    provider = new SimProvider(cluster);
    Map<String, StateModelDefinition> models = new HashMap<>();
    models.put(MASTER_SLAVE, BuiltInStateModelDefinitions.MasterSlave.getStateModelDefinition());
    provider.setStateModelDefMap(models);
    store = leaderStore;
    algorithm = new RecordingAlgorithm();
    rebalancer = new CapturingRebalancer(store, algorithm);
    rebalancer.setGlobalRebalanceAsyncMode(asyncGlobal);
    rebalancer.setPartialRebalanceAsyncMode(asyncPartial);
    monitor = new ClusterStatusMonitor(cluster);
    rebalancer.setClusterStatusMonitor(monitor);
  }

  /**
   * Hands the cluster to a new Helix controller leader with its own data provider, metadata store,
   * algorithm, rebalancer and monitor. Only what is durable carries over: the persisted BASELINE
   * and BEST_POSSIBLE maps and the participants' current state.
   */
  void newLeader() {
    SimStore durable = new SimStore(store.durableBaseline(), store.durableBest());
    rebalancer.close();
    lead(durable);
    lastSkipped.clear();
  }

  /** Three cliques on DISK 100 nodes: A a0..a2, B b0..b3, C c0..c2, each lightly loaded. */
  static ScopeMatrixSim standard(String cluster, boolean isolation) {
    ScopeMatrixSim sim = new ScopeMatrixSim(cluster, isolation);
    for (int i = 0; i < 3; i++) {
      sim.node("a" + i, "A", 100);
    }
    for (int i = 0; i < 4; i++) {
      sim.node("b" + i, "B", 100);
    }
    for (int i = 0; i < 3; i++) {
      sim.node("c" + i, "C", 100);
    }
    sim.resource("RA", "A", 4, 10);
    sim.resource("RB1", "B", 4, 10);
    sim.resource("RB2", "B", 4, 10);
    sim.resource("RC", "C", 4, 10);
    return sim;
  }

  NodeSpec node(String name, String tag, Integer disk) {
    NodeSpec node = new NodeSpec(name);
    if (tag != null) {
      node.tags.add(tag);
    }
    node.disk = disk;
    nodes.put(name, node);
    return node;
  }

  ResourceSpec resource(String name, String tag, int partitions, int weight) {
    ResourceSpec resource = new ResourceSpec(name, tag, partitions, weight);
    resources.put(name, resource);
    return resource;
  }

  boolean asyncGlobal;
  boolean asyncPartial;

  void setModes(boolean asyncGlobal, boolean asyncPartial) {
    this.asyncGlobal = asyncGlobal;
    this.asyncPartial = asyncPartial;
    rebalancer.setGlobalRebalanceAsyncMode(asyncGlobal);
    rebalancer.setPartialRebalanceAsyncMode(asyncPartial);
  }

  boolean anyAsync() {
    return asyncGlobal || asyncPartial;
  }

  void setIsolation(boolean enabled) {
    if (enabled != clusterConfig.isWagedInstanceTagIsolationEnabled()) {
      // The monitor drops every snapshot when the flag flips.
      lastSkipped.clear();
    }
    clusterConfig.setWagedInstanceTagIsolationEnabled(enabled);
  }

  /**
   * Reset the rebalancer the way the Helix controller does, which also drops every gauge snapshot.
   */
  void reset() {
    lastSkipped.clear();
    rebalancer.reset();
  }

  void enableDelay(long delayMs) {
    clusterConfig.setDelayRebalaceEnabled(true);
    clusterConfig.setRebalanceDelayTime(delayMs);
  }

  /** Take a node offline. With a delay window configured it stays active until the window ends. */
  void offline(String name, boolean insideDelayWindow) {
    NodeSpec node = nodes.get(name);
    node.live = false;
    node.offlineSince = insideDelayWindow ? System.currentTimeMillis()
        : System.currentTimeMillis() - TimeUnit.DAYS.toMillis(1);
  }

  void online(String name) {
    NodeSpec node = nodes.get(name);
    node.live = true;
    node.offlineSince = null;
    node.session++;
  }

  void refresh() throws IOException {
    provider.setClusterConfig(new ClusterConfig(copyRecord(clusterConfig.getRecord())));
    Map<String, InstanceConfig> configs = new HashMap<>();
    List<LiveInstance> live = new ArrayList<>();
    Map<String, Long> offline = new HashMap<>();
    for (NodeSpec node : nodes.values()) {
      if (node.hasConfig) {
        configs.put(node.name, node.toConfig());
      }
      if (node.live) {
        live.add(node.toLive());
      } else if (node.offlineSince != null) {
        offline.put(node.name, node.offlineSince);
      }
    }
    provider.setInstanceConfigMap(configs);
    provider.setLiveInstances(live);
    provider.offlineTimes = offline;
    List<IdealState> idealStates = new ArrayList<>();
    Map<String, ResourceConfig> resourceConfigs = new HashMap<>();
    for (ResourceSpec resource : resources.values()) {
      idealStates.add(resource.toIdealState());
      resourceConfigs.put(resource.name, resource.toResourceConfig());
    }
    provider.setIdealStates(idealStates);
    provider.setResourceConfigMap(resourceConfigs);
    provider.getRefreshedChangeTypes().addAll(ALL_CHANGES);
  }

  Map<String, Resource> resourceMap() {
    Map<String, Resource> map = new TreeMap<>();
    resources.values().forEach(r -> map.put(r.name, r.toResource()));
    return map;
  }

  /** One Helix controller pipeline, draining any asynchronous work before returning. */
  Run run() throws Exception {
    return run(true);
  }

  Run run(boolean drain) throws Exception {
    refresh();
    lastSkipped.values().forEach(skipped -> skipped.retainAll(resources.keySet()));
    Run run = new Run();
    run.baselineBefore = copyOf(store.getBaseline());
    run.bestBefore = copyOf(store.getBestPossibleAssignment());
    int calcStart = algorithm.calcs.size();
    int reportStart = algorithm.reports.size();
    rebalancer.lastEmitted = null;
    rebalancer.lastFailure = null;
    // The asynchronous baseline worker races the pipeline's own partial rebalance and the partial
    // worker for the persisted baseline. Hold it until both have finished so every run takes the
    // same interleaving: the one where the baseline calculation is the slow one.
    CountDownLatch slowBaseline = drain && asyncGlobal ? holdBaselineWorker() : null;
    try {
      run.idealStates = rebalancer.computeNewIdealStates(provider, resourceMap(), currentState);
    } catch (HelixRebalanceException e) {
      run.pipelineFailure = e;
    } finally {
      if (slowBaseline != null) {
        partialExecutor().submit(() -> null).get(60, TimeUnit.SECONDS);
        slowBaseline.countDown();
      }
    }
    run.emitted = rebalancer.lastEmitted;
    run.computeFailure = rebalancer.lastFailure;
    if (drain) {
      drain();
    }
    finish(run, calcStart, reportStart);
    return run;
  }

  void finish(Run run, int calcStart, int reportStart) throws Exception {
    run.baselineAfter = copyOf(store.getBaseline());
    run.bestAfter = copyOf(store.getBestPossibleAssignment());
    run.calcs = new ArrayList<>(algorithm.calcs.subList(calcStart, algorithm.calcs.size()));
    run.reports = new ArrayList<>(algorithm.reports.subList(reportStart, algorithm.reports.size()));
    run.gauge = monitor.getWagedInstanceTagIsolationSkippedResourcesGauge();
    run.retry = retrySet();
    if (clusterConfig.isWagedInstanceTagIsolationEnabled()) {
      for (Report report : run.reports) {
        if (report.scope != ClusterModel.RebalanceScopeType.GLOBAL_BASELINE) {
          lastSkipped.put(report.scope, new TreeSet<>(report.skipped));
        }
      }
    }
  }

  /**
   * What the gauge must show when the last baseline succeeded: every resource still waiting for a
   * baseline retry plus every resource the last run of each serving phase carried, counted once.
   */
  long expectedGauge(Run run) {
    Set<String> unresolved = new TreeSet<>(run.retry);
    lastSkipped.values().forEach(unresolved::addAll);
    unresolved.retainAll(resources.keySet());
    return unresolved.size();
  }

  void drain() throws Exception {
    baselineExecutor().submit(() -> null).get(60, TimeUnit.SECONDS);
    partialExecutor().submit(() -> null).get(60, TimeUnit.SECONDS);
  }

  /** Blocks the async baseline worker until the returned latch is released. */
  CountDownLatch holdBaselineWorker() {
    CountDownLatch gate = new CountDownLatch(1);
    baselineExecutor().submit(() -> {
      gate.await(60, TimeUnit.SECONDS);
      return null;
    });
    return gate;
  }

  CountDownLatch holdPartialWorker() {
    CountDownLatch gate = new CountDownLatch(1);
    partialExecutor().submit(() -> {
      gate.await(60, TimeUnit.SECONDS);
      return null;
    });
    return gate;
  }

  ExecutorService baselineExecutor() {
    return (ExecutorService) field(field(rebalancer, WagedRebalancer.class,
        "_globalRebalanceRunner"), GlobalRebalanceRunner.class, "_baselineCalculateExecutor");
  }

  ExecutorService partialExecutor() {
    return (ExecutorService) field(field(rebalancer, WagedRebalancer.class,
        "_partialRebalanceRunner"), PartialRebalanceRunner.class, "_bestPossibleCalculateExecutor");
  }

  @SuppressWarnings("unchecked")
  Set<String> retrySet() {
    AtomicReference<Set<String>> ref = (AtomicReference<Set<String>>) field(field(rebalancer,
        WagedRebalancer.class, "_globalRebalanceRunner"), GlobalRebalanceRunner.class,
        "_isolationResourcesToRetry");
    Set<String> value = ref.get();
    return value == null ? Collections.emptySet() : new TreeSet<>(value);
  }

  private static Object field(Object owner, Class<?> type, String name) {
    try {
      Field field = type.getDeclaredField(name);
      field.setAccessible(true);
      return field.get(owner);
    } catch (ReflectiveOperationException e) {
      throw new IllegalStateException(e);
    }
  }

  /** Make the current state match what was just emitted, as if every transition completed. */
  void converge(Run run) {
    CurrentStateOutput output = new CurrentStateOutput();
    Map<String, ResourceAssignment> assignment = run.emitted != null ? run.emitted : run.bestAfter;
    assignment.forEach((resource, ra) -> {
      for (Partition partition : ra.getMappedPartitions()) {
        ra.getReplicaMap(partition).forEach((instance, state) -> {
          NodeSpec node = nodes.get(instance);
          if (node != null && node.live) {
            output.setCurrentState(resource, partition, instance, state);
          }
        });
      }
    });
    currentState = output;
  }

  @Override
  public void close() {
    rebalancer.close();
  }

  // ---------------------------------------------------------------- expectations and invariants

  Set<String> resourcesOf(String... tags) {
    Set<String> tagSet = new TreeSet<>();
    Collections.addAll(tagSet, tags);
    return resources.values().stream().filter(r -> tagSet.contains(r.tag)).map(r -> r.name)
        .collect(Collectors.toCollection(TreeSet::new));
  }

  boolean assignable(NodeSpec node) {
    return node.hasConfig && (node.operation == InstanceConstants.InstanceOperation.ENABLE
        || node.operation == InstanceConstants.InstanceOperation.DISABLE);
  }

  /** Nodes a freshly computed serving assignment may use for the given tag. */
  Set<String> servingNodes(String tag) {
    boolean delay = clusterConfig.isDelayRebalaceEnabled();
    return nodes.values().stream().filter(this::assignable).filter(n -> n.tags.contains(tag))
        .filter(n -> n.operation == InstanceConstants.InstanceOperation.ENABLE || delay)
        .filter(n -> n.live || (delay && n.offlineSince != null
            && n.offlineSince + clusterConfig.getRebalanceDelayTime() > System.currentTimeMillis()))
        .map(n -> n.name).collect(Collectors.toCollection(TreeSet::new));
  }

  Set<String> baselineNodes(String tag) {
    return nodes.values().stream().filter(this::assignable).filter(n -> n.tags.contains(tag))
        .map(n -> n.name).collect(Collectors.toCollection(TreeSet::new));
  }

  int capacityOf(String node) {
    NodeSpec spec = nodes.get(node);
    if (spec != null && spec.disk != null) {
      return spec.disk;
    }
    return clusterConfig.getDefaultInstanceCapacityMap().getOrDefault(DISK, 0);
  }

  /** A freshly computed resource is complete, well formed and only on nodes it may use. */
  void assertFresh(String where, Map<String, ResourceAssignment> map, String resourceName,
      Set<String> allowed, boolean allowTopUps) {
    ResourceSpec spec = resources.get(resourceName);
    ResourceAssignment assignment = map.get(resourceName);
    Assert.assertNotNull(assignment, where + ": " + resourceName + " missing");
    Set<String> mapped = assignment.getMappedPartitions().stream()
        .map(Partition::getPartitionName).collect(Collectors.toCollection(TreeSet::new));
    Assert.assertEquals(mapped, new TreeSet<>(spec.partitionNames()),
        where + ": " + resourceName + " partitions");
    for (Partition partition : assignment.getMappedPartitions()) {
      Map<String, String> replicas = assignment.getReplicaMap(partition);
      long masters = replicas.values().stream().filter("MASTER"::equals).count();
      if (allowTopUps) {
        Assert.assertTrue(replicas.size() >= spec.replicas, where + ": " + partition + replicas);
        Assert.assertTrue(masters >= 1, where + ": " + partition + replicas);
      } else {
        Assert.assertEquals(replicas.size(), spec.replicas, where + ": " + partition + replicas);
        Assert.assertEquals(masters, 1L, where + ": " + partition + replicas);
      }
      for (String instance : replicas.keySet()) {
        Assert.assertTrue(allowed.contains(instance),
            where + ": " + resourceName + " " + partition + " on " + instance + " not in "
                + allowed);
      }
    }
  }

  /**
   * No node is over DISK capacity. Replicas of a resource carried forward are scored at the weights
   * they had when last computed, because that is what they were placed at. A node holding only
   * carried replicas of broken cliques belongs to those cliques and is not scored at all.
   */
  void assertCapacity(String where, Map<String, ResourceAssignment> map, Set<String> carried) {
    Map<String, Long> load = new TreeMap<>();
    Map<String, Boolean> hasFresh = new HashMap<>();
    map.forEach((resource, ra) -> {
      ResourceSpec spec = resources.get(resource);
      if (spec == null) {
        return;
      }
      boolean isCarried = carried.contains(resource);
      Map<String, Integer> oldWeights = computedWeights.getOrDefault(resource,
          Collections.emptyMap());
      for (Partition partition : ra.getMappedPartitions()) {
        int weight = isCarried && oldWeights.containsKey(partition.getPartitionName())
            ? oldWeights.get(partition.getPartitionName())
            : spec.weightOf(partition.getPartitionName());
        for (String instance : ra.getReplicaMap(partition).keySet()) {
          load.merge(instance, (long) weight, Long::sum);
          if (!isCarried) {
            hasFresh.put(instance, true);
          }
        }
      }
    });
    load.forEach((instance, used) -> {
      if (hasFresh.getOrDefault(instance, false)) {
        Assert.assertTrue(used <= capacityOf(instance),
            where + ": " + instance + " holds " + used + " > " + capacityOf(instance));
      }
    });
  }

  static Map<String, Map<String, String>> canon(ResourceAssignment assignment) {
    Map<String, Map<String, String>> out = new TreeMap<>();
    if (assignment == null) {
      return null;
    }
    for (Partition partition : assignment.getMappedPartitions()) {
      out.put(partition.getPartitionName(), new TreeMap<>(assignment.getReplicaMap(partition)));
    }
    return out;
  }

  static Map<String, Map<String, Map<String, String>>> canon(
      Map<String, ResourceAssignment> map, Set<String> resources) {
    Map<String, Map<String, Map<String, String>>> out = new TreeMap<>();
    if (map == null) {
      return out;
    }
    map.forEach((resource, ra) -> {
      if (resources == null || resources.contains(resource)) {
        out.put(resource, canon(ra));
      }
    });
    return out;
  }

  void recordComputedWeights(Set<String> freshResources) {
    for (String resource : freshResources) {
      ResourceSpec spec = resources.get(resource);
      if (spec == null) {
        continue;
      }
      Map<String, Integer> weights = new HashMap<>();
      spec.partitionNames().forEach(p -> weights.put(p, spec.weightOf(p)));
      computedWeights.put(resource, weights);
    }
  }

  // ---------------------------------------------------------------- copying

  static Map<String, ResourceAssignment> copyOf(Map<String, ResourceAssignment> source) {
    Map<String, ResourceAssignment> copy = new HashMap<>();
    if (source != null) {
      source.forEach((k, v) -> copy.put(k, copyOf(v)));
    }
    return copy;
  }

  static ResourceAssignment copyOf(ResourceAssignment source) {
    return new ResourceAssignment(copyRecord(source.getRecord()));
  }

  static ZNRecord copyRecord(ZNRecord source) {
    ZNRecord copy = new ZNRecord(source.getId());
    copy.setSimpleFields(new TreeMap<>(source.getSimpleFields()));
    Map<String, List<String>> lists = new TreeMap<>();
    source.getListFields().forEach((k, v) -> lists.put(k, new ArrayList<>(v)));
    copy.setListFields(lists);
    Map<String, Map<String, String>> maps = new TreeMap<>();
    source.getMapFields().forEach((k, v) -> maps.put(k, new TreeMap<>(v)));
    copy.setMapFields(maps);
    return copy;
  }
}
