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
import java.util.Arrays;
import java.util.Collections;
import java.util.EnumMap;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.helix.HelixConstants;
import org.apache.helix.controller.dataproviders.ResourceControllerDataProvider;
import org.apache.helix.controller.rebalancer.util.WagedRebalanceUtil;
import org.apache.helix.controller.rebalancer.waged.constraints.ConstraintBasedAlgorithm;
import org.apache.helix.controller.rebalancer.waged.constraints.ConstraintBasedAlgorithmFactory;
import org.apache.helix.controller.rebalancer.waged.model.ClusterModel.RebalanceScopeType;
import org.apache.helix.controller.stages.CurrentStateOutput;
import org.apache.helix.model.BuiltInStateModelDefinitions;
import org.apache.helix.model.ClusterConfig;
import org.apache.helix.model.IdealState;
import org.apache.helix.model.InstanceConfig;
import org.apache.helix.model.LiveInstance;
import org.apache.helix.model.Resource;
import org.apache.helix.model.ResourceConfig;
import org.apache.helix.monitoring.mbeans.ClusterStatusMonitor;
import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.core.LogEvent;
import org.apache.logging.log4j.core.LoggerContext;
import org.apache.logging.log4j.core.appender.AbstractAppender;
import org.apache.logging.log4j.core.config.Configuration;
import org.apache.logging.log4j.core.config.LoggerConfig;
import org.apache.logging.log4j.core.config.Property;
import org.testng.Assert;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

/**
 * The assignment calculation logs name the algorithm that computes each assignment. With instance
 * tag isolation on, a calculation can run behind a wrapper that observes its outcome, and the logs
 * still name the algorithm underneath, in every scope, exactly as they do with isolation off. The
 * test reads the log lines themselves, so a wrapper that hides the algorithm's name behind its own
 * class name, or behind the empty name of an anonymous class, fails it.
 */
public class TestWagedIsolationAlgorithmName {
  private static final String DISK = "DISK";
  private static final String MASTER_SLAVE = BuiltInStateModelDefinitions.MasterSlave.name();
  private static final String ALGORITHM = ConstraintBasedAlgorithm.class.getSimpleName();
  private static final String START = "Start calculating for an assignment with algorithm ";
  private static final String FINISH = "Finish calculating an assignment with algorithm ";
  private static final String TOOK = ". Took: ";
  private static final EnumSet<HelixConstants.ChangeType> ALL_CHANGES = EnumSet.of(
      HelixConstants.ChangeType.CLUSTER_CONFIG, HelixConstants.ChangeType.INSTANCE_CONFIG,
      HelixConstants.ChangeType.IDEAL_STATE, HelixConstants.ChangeType.RESOURCE_CONFIG,
      HelixConstants.ChangeType.LIVE_INSTANCE);
  /** The line each scope logs on its calculating thread as it starts to calculate. */
  private static final Map<String, RebalanceScopeType> SCOPE_MARKERS = new HashMap<>();
  /** The calculation lines, and the loggers of the scope markers. */
  private static final List<String> LOGGERS = Arrays.asList(WagedRebalanceUtil.class.getName(),
      GlobalRebalanceRunner.class.getName(), PartialRebalanceRunner.class.getName(),
      WagedRebalancer.class.getName());
  private static final AtomicInteger SEQUENCE = new AtomicInteger();

  static {
    SCOPE_MARKERS.put("Start calculating the new baseline.", RebalanceScopeType.GLOBAL_BASELINE);
    SCOPE_MARKERS.put("Start calculating the new best possible assignment.",
        RebalanceScopeType.PARTIAL);
    SCOPE_MARKERS.put("Emergency rebalance responding to permanent node down.",
        RebalanceScopeType.EMERGENCY);
    SCOPE_MARKERS.put("Start delayed rebalance overwrites in emergency rebalance.",
        RebalanceScopeType.DELAYED_REBALANCE_OVERWRITES);
  }

  @DataProvider(name = "baselineModes")
  public Object[][] baselineModes() {
    return new Object[][] {{false}, {true}};
  }

  /**
   * Runs the same pipelines with isolation on and off. The first pipeline calculates the baseline
   * and the partial rebalance, a node that stays down triggers the emergency rebalance, and a node
   * that goes offline inside the delay window leaves its partitions below min active, which the
   * delayed rebalance overwrite tops up. The asynchronous mode calculates the baseline on its own
   * thread.
   */
  @Test(dataProvider = "baselineModes")
  public void testCalculationLogsNameTheWrappedAlgorithmInEveryScope(boolean asyncBaseline)
      throws Exception {
    Map<RebalanceScopeType, Set<String>> isolationOn = namesByScope(true, asyncBaseline);
    Map<RebalanceScopeType, Set<String>> isolationOff = namesByScope(false, asyncBaseline);
    for (RebalanceScopeType scope : EnumSet.allOf(RebalanceScopeType.class)) {
      Assert.assertEquals(isolationOn.get(scope), Collections.singleton(ALGORITHM),
          "Algorithm named by the " + scope + " calculation with isolation on, all scopes "
              + quoted(isolationOn));
    }
    Assert.assertEquals(isolationOn, isolationOff, "Isolation on names " + quoted(isolationOn)
        + " and isolation off names " + quoted(isolationOff));
  }

  /** Renders the names in quotes, so an empty name reads as "". */
  private static String quoted(Map<RebalanceScopeType, Set<String>> names) {
    Map<RebalanceScopeType, List<String>> quoted = new EnumMap<>(RebalanceScopeType.class);
    names.forEach((scope, scopeNames) -> {
      List<String> list = new ArrayList<>();
      scopeNames.forEach(name -> list.add('"' + name + '"'));
      quoted.put(scope, list);
    });
    return quoted.toString();
  }

  /**
   * Runs the scenario once and returns, per scope, the algorithm names its calculation lines
   * carry. Fails unless every scope calculated at least once and every calculation that started
   * also finished.
   */
  private static Map<RebalanceScopeType, Set<String>> namesByScope(boolean isolation,
      boolean asyncBaseline) throws Exception {
    String cluster = "AlgorithmName_" + (isolation ? "on_" : "off_") + SEQUENCE.incrementAndGet();
    CalculationLog log = CalculationLog.attach(cluster);
    Pipeline pipeline = new Pipeline(cluster, isolation, asyncBaseline);
    try {
      pipeline.run();
      pipeline.run();
      pipeline.offline("b2", false);
      pipeline.run();
      pipeline.enableDelay(TimeUnit.HOURS.toMillis(1));
      pipeline.offline("a1", true);
      pipeline.run();
    } finally {
      pipeline.close();
      log.detach();
    }
    Assert.assertTrue(log.unattributed().isEmpty(),
        "Calculation lines outside every scope: " + log.unattributed());
    Map<RebalanceScopeType, Set<String>> names = new EnumMap<>(RebalanceScopeType.class);
    for (RebalanceScopeType scope : EnumSet.allOf(RebalanceScopeType.class)) {
      List<String> starts = log.names(scope, START);
      List<String> finishes = log.names(scope, FINISH);
      String context = scope + " with isolation " + (isolation ? "on" : "off") + ", starts "
          + starts + ", finishes " + finishes;
      Assert.assertFalse(starts.isEmpty(), "No calculation started for " + context);
      Assert.assertEquals(finishes.size(), starts.size(), "Unfinished calculation for " + context);
      Set<String> scopeNames = new TreeSet<>(starts);
      scopeNames.addAll(finishes);
      names.put(scope, scopeNames);
    }
    return names;
  }

  /**
   * Collects the calculation lines of one cluster's pipelines and attributes each to the scope
   * whose marker its thread logged last. The calculating threads of the baseline and the partial
   * rebalance carry the cluster name, and the emergency rebalance and the delayed rebalance
   * overwrite calculate on the pipeline thread.
   */
  private static final class CalculationLog extends AbstractAppender {
    private final String _threadSuffix;
    private final long _pipelineThread = Thread.currentThread().getId();
    private final Map<Long, RebalanceScopeType> _scopeByThread = new ConcurrentHashMap<>();
    /** Scope, line kind and algorithm name of every calculation line, in logging order. */
    private final List<String[]> _lines = new CopyOnWriteArrayList<>();
    private final List<String> _unattributed = new CopyOnWriteArrayList<>();
    private final Map<String, LoggerConfig> _replaced = new HashMap<>();

    private CalculationLog(String cluster) {
      super("CalculationLog-" + cluster, null, null, true, Property.EMPTY_ARRAY);
      _threadSuffix = "-" + cluster;
    }

    static CalculationLog attach(String cluster) {
      CalculationLog log = new CalculationLog(cluster);
      log.start();
      LoggerContext context = LoggerContext.getContext(false);
      Configuration configuration = context.getConfiguration();
      for (String name : LOGGERS) {
        LoggerConfig existing = configuration.getLoggers().get(name);
        if (existing != null) {
          log._replaced.put(name, existing);
          configuration.removeLogger(name);
        }
        LoggerConfig capture = new LoggerConfig(name, Level.INFO, false);
        capture.addAppender(log, Level.INFO, null);
        configuration.addLogger(name, capture);
      }
      context.updateLoggers();
      return log;
    }

    void detach() {
      LoggerContext context = LoggerContext.getContext(false);
      Configuration configuration = context.getConfiguration();
      for (String name : LOGGERS) {
        configuration.removeLogger(name);
        LoggerConfig replaced = _replaced.get(name);
        if (replaced != null) {
          configuration.addLogger(name, replaced);
        }
      }
      context.updateLoggers();
      stop();
    }

    @Override
    public void append(LogEvent event) {
      long thread = event.getThreadId();
      String threadName = event.getThreadName();
      boolean clusterThread = threadName != null && threadName.endsWith(_threadSuffix);
      if (thread != _pipelineThread && !clusterThread) {
        return;
      }
      String message = event.getMessage().getFormattedMessage();
      RebalanceScopeType marker = SCOPE_MARKERS.get(message);
      if (marker != null) {
        _scopeByThread.put(thread, marker);
        return;
      }
      String kind;
      String name;
      if (message.startsWith(START)) {
        kind = START;
        name = message.substring(START.length());
      } else if (message.startsWith(FINISH) && message.lastIndexOf(TOOK) >= FINISH.length()) {
        kind = FINISH;
        name = message.substring(FINISH.length(), message.lastIndexOf(TOOK));
      } else {
        return;
      }
      RebalanceScopeType scope = _scopeByThread.get(thread);
      if (scope == null) {
        _unattributed.add(threadName + ": " + message);
        return;
      }
      _lines.add(new String[] {scope.name(), kind, name});
    }

    List<String> names(RebalanceScopeType scope, String kind) {
      List<String> names = new ArrayList<>();
      for (String[] line : _lines) {
        if (line[0].equals(scope.name()) && line[1].equals(kind)) {
          names.add(line[2]);
        }
      }
      return names;
    }

    List<String> unattributed() {
      return _unattributed;
    }
  }

  /**
   * One Helix controller pipeline around the real WAGED rebalancer, for two cliques, A (a0 to a2)
   * and B (b0 to b2), each serving one resource of four partitions with two replicas. Every run
   * hands the rebalancer freshly built cluster data, so the change detector compares the content
   * of consecutive runs and never two views of one shared object.
   */
  private static final class Pipeline implements AutoCloseable {
    private final String _cluster;
    private final boolean _isolation;
    private final boolean _asyncBaseline;
    private final Map<String, String> _nodeTags = new TreeMap<>();
    private final Map<String, Long> _offlineSince = new HashMap<>();
    private final Map<String, String> _resourceTags = new TreeMap<>();
    private final OfflineTimeProvider _provider;
    private final WagedRebalancer _rebalancer;
    private long _delayMs = -1;

    Pipeline(String cluster, boolean isolation, boolean asyncBaseline) {
      _cluster = cluster;
      _isolation = isolation;
      _asyncBaseline = asyncBaseline;
      for (int i = 0; i < 3; i++) {
        _nodeTags.put("a" + i, "A");
        _nodeTags.put("b" + i, "B");
      }
      _resourceTags.put("RA", "A");
      _resourceTags.put("RB", "B");
      _provider = new OfflineTimeProvider(cluster);
      _provider.setStateModelDefMap(Collections.singletonMap(MASTER_SLAVE,
          BuiltInStateModelDefinitions.MasterSlave.getStateModelDefinition()));
      _rebalancer = new WagedRebalancer(new MockAssignmentMetadataStore(),
          ConstraintBasedAlgorithmFactory.getInstance(Collections.emptyMap()), Optional.empty());
      _rebalancer.setGlobalRebalanceAsyncMode(asyncBaseline);
      _rebalancer.setPartialRebalanceAsyncMode(false);
      _rebalancer.setClusterStatusMonitor(new ClusterStatusMonitor(cluster));
    }

    /** Take a node offline, either inside the delay window or down for a day. */
    void offline(String node, boolean insideDelayWindow) {
      long currentTime = System.currentTimeMillis();
      _offlineSince.put(node,
          insideDelayWindow ? currentTime : currentTime - TimeUnit.DAYS.toMillis(1));
    }

    void enableDelay(long delayMs) {
      _delayMs = delayMs;
    }

    void run() throws Exception {
      ClusterConfig clusterConfig = new ClusterConfig(_cluster);
      clusterConfig.setInstanceCapacityKeys(Collections.singletonList(DISK));
      clusterConfig.setDefaultInstanceCapacityMap(Collections.singletonMap(DISK, 100));
      clusterConfig.setDefaultPartitionWeightMap(Collections.singletonMap(DISK, 10));
      clusterConfig.setWagedInstanceTagIsolationEnabled(_isolation);
      if (_delayMs > 0) {
        clusterConfig.setDelayRebalaceEnabled(true);
        clusterConfig.setRebalanceDelayTime(_delayMs);
      }
      _provider.setClusterConfig(clusterConfig);
      Map<String, InstanceConfig> instanceConfigs = new HashMap<>();
      List<LiveInstance> liveInstances = new ArrayList<>();
      for (Map.Entry<String, String> node : _nodeTags.entrySet()) {
        InstanceConfig instanceConfig = new InstanceConfig(node.getKey());
        instanceConfig.addTag(node.getValue());
        instanceConfigs.put(node.getKey(), instanceConfig);
        if (!_offlineSince.containsKey(node.getKey())) {
          LiveInstance liveInstance = new LiveInstance(node.getKey());
          liveInstance.setSessionId(node.getKey() + "_session");
          liveInstances.add(liveInstance);
        }
      }
      _provider.setInstanceConfigMap(instanceConfigs);
      _provider.setLiveInstances(liveInstances);
      _provider._offlineTimes = new HashMap<>(_offlineSince);
      List<IdealState> idealStates = new ArrayList<>();
      Map<String, ResourceConfig> resourceConfigs = new HashMap<>();
      Map<String, Resource> resources = new TreeMap<>();
      for (Map.Entry<String, String> resource : _resourceTags.entrySet()) {
        idealStates.add(idealState(resource.getKey(), resource.getValue()));
        resourceConfigs.put(resource.getKey(), resourceConfig(resource.getKey()));
        resources.put(resource.getKey(), resource(resource.getKey()));
      }
      _provider.setIdealStates(idealStates);
      _provider.setResourceConfigMap(resourceConfigs);
      _provider.getRefreshedChangeTypes().addAll(ALL_CHANGES);
      _rebalancer.computeNewIdealStates(_provider, resources, new CurrentStateOutput());
      if (_asyncBaseline) {
        baselineExecutor().submit(() -> null).get(60, TimeUnit.SECONDS);
      }
    }

    @Override
    public void close() {
      _rebalancer.close();
    }

    private static IdealState idealState(String name, String tag) {
      IdealState idealState = new IdealState(name);
      idealState.setRebalanceMode(IdealState.RebalanceMode.FULL_AUTO);
      idealState.setRebalancerClassName(WagedRebalancer.class.getName());
      idealState.setStateModelDefRef(MASTER_SLAVE);
      idealState.setReplicas("2");
      idealState.setNumPartitions(4);
      idealState.setInstanceGroupTag(tag);
      for (String partition : partitions(name)) {
        idealState.getRecord().setListField(partition, new ArrayList<>());
      }
      return idealState;
    }

    private static ResourceConfig resourceConfig(String name) throws IOException {
      ResourceConfig resourceConfig = new ResourceConfig(name);
      resourceConfig.setPartitionCapacityMap(Collections.singletonMap(
          ResourceConfig.DEFAULT_PARTITION_KEY, Collections.singletonMap(DISK, 10)));
      return resourceConfig;
    }

    private static Resource resource(String name) {
      Resource resource = new Resource(name);
      resource.setStateModelDefRef(MASTER_SLAVE);
      partitions(name).forEach(resource::addPartition);
      return resource;
    }

    private static List<String> partitions(String resource) {
      List<String> partitions = new ArrayList<>();
      for (int i = 0; i < 4; i++) {
        partitions.add(resource + "_" + i);
      }
      return partitions;
    }

    private ExecutorService baselineExecutor() throws Exception {
      Field runner = WagedRebalancer.class.getDeclaredField("_globalRebalanceRunner");
      runner.setAccessible(true);
      Field executor = GlobalRebalanceRunner.class.getDeclaredField("_baselineCalculateExecutor");
      executor.setAccessible(true);
      return (ExecutorService) executor.get(runner.get(_rebalancer));
    }
  }

  /** Serves the offline times the scenario sets, as the Helix controller reads them from ZK. */
  private static final class OfflineTimeProvider extends ResourceControllerDataProvider {
    private volatile Map<String, Long> _offlineTimes = new HashMap<>();

    OfflineTimeProvider(String cluster) {
      super(cluster);
    }

    @Override
    public Map<String, Long> getInstanceOfflineTimeMap() {
      return _offlineTimes;
    }
  }
}
