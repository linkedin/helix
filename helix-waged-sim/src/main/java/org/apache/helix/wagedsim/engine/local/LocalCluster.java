package org.apache.helix.wagedsim.engine.local;

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

import java.io.File;
import java.io.IOException;
import java.net.ServerSocket;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.stream.Stream;

import org.apache.helix.AccessOption;
import org.apache.helix.HelixManager;
import org.apache.helix.HelixManagerFactory;
import org.apache.helix.InstanceType;
import org.apache.helix.controller.rebalancer.waged.AssignmentMetadataStore;
import org.apache.helix.manager.zk.ZKHelixAdmin;
import org.apache.helix.manager.zk.ZkBaseDataAccessor;
import org.apache.helix.manager.zk.ZkBucketDataAccessor;
import org.apache.helix.model.ClusterConfig;
import org.apache.helix.model.CurrentState;
import org.apache.helix.model.IdealState;
import org.apache.helix.model.InstanceConfig;
import org.apache.helix.model.LiveInstance;
import org.apache.helix.wagedsim.cluster.ClusterState;
import org.apache.helix.wagedsim.cluster.Layouts;
import org.apache.helix.wagedsim.engine.StateOps;
import org.apache.helix.zookeeper.datamodel.ZNRecord;
import org.apache.helix.zookeeper.zkclient.ZkServer;
import org.apache.zookeeper.data.Stat;

/**
 * A real Helix cluster on this machine: an embedded ZooKeeper, the production controller in a child
 * process, and in-process participants whose state model completes every transition. It binds to
 * localhost only and keeps its data under the work folder.
 */
public class LocalCluster implements AutoCloseable {
  private final Path _work;
  private final ZkServer _zk;
  private final String _zkAddress;
  private final ZkBaseDataAccessor<ZNRecord> _accessor;
  private final ZKHelixAdmin _admin;
  private ZkBucketDataAccessor _buckets;
  private String _cluster;
  private Process _controller;
  private final Map<String, HelixManager> _participants = new ConcurrentHashMap<>();
  private final Set<String> _models = new TreeSet<>();
  private long _latency;
  private long _compression = 1;

  public LocalCluster(Path work, int port) throws Exception {
    _work = work;
    deleteRecursively(work.resolve("zk"));
    Files.createDirectories(work);
    int zkPort = port > 0 ? port : freePort();
    _zk = new ZkServer(work.resolve("zk/data").toString(), work.resolve("zk/log").toString(), zkClient -> {
    }, zkPort);
    _zk.start();
    _zkAddress = "localhost:" + zkPort;
    _accessor = new ZkBaseDataAccessor<>(_zkAddress);
    _admin = new ZKHelixAdmin(_zkAddress);
  }

  public String zkAddress() {
    return _zkAddress;
  }

  public String cluster() {
    return _cluster;
  }

  public long compression() {
    return _compression;
  }

  private static int freePort() throws IOException {
    try (ServerSocket socket = new ServerSocket(0)) {
      return socket.getLocalPort();
    }
  }

  /**
   * Writes the cluster definition into ZooKeeper, with delay settings divided by {@code compression}.
   * Call {@link #rebaseTimestamps} on the definition first. Live instances and current states are
   * left to the participants.
   */
  public void load(ClusterState source, long compression) throws Exception {
    _cluster = source.getClusterName();
    _compression = Math.max(1, compression);
    ClusterState state = source.copy();
    compressDelays(state, _compression);
    _admin.addCluster(_cluster, true);
    for (Map.Entry<String, ZNRecord> entry : state.getZnodes().entrySet()) {
      String path = entry.getKey();
      if (path.startsWith(ClusterState.CONFIGS_PARTICIPANT + "/")) {
        _admin.addInstance(_cluster, new InstanceConfig(entry.getValue()));
      }
    }
    java.util.Set<String> waged = state.getWagedIdealStates().keySet();
    for (Map.Entry<String, ZNRecord> entry : state.getZnodes().entrySet()) {
      if ((isControllerInput(entry.getKey()) || entry.getKey().endsWith("/HISTORY"))
          && ClusterState.isSimulated(entry.getKey(), waged)) {
        write(entry.getKey(), entry.getValue());
      }
    }
    _models.addAll(state.getStateModelDefs().keySet());
    _buckets = new ZkBucketDataAccessor(_zkAddress);
    if (state.getBaseline() != null || state.getBestPossible() != null) {
      Store store = new Store(_buckets, _cluster);
      if (state.getBaseline() != null) {
        store.persistBaseline(Layouts.toAssignments(state.getBaseline()));
      }
      if (state.getBestPossible() != null) {
        store.persistBestPossibleAssignment(Layouts.toAssignments(state.getBestPossible()));
      }
    }
  }

  /** @return true for znodes the controller reads as configuration and the simulation writes */
  public static boolean isControllerInput(String path) {
    return path.startsWith(ClusterState.CONFIGS_CLUSTER + "/") || path.startsWith(ClusterState.CONFIGS_PARTICIPANT + "/")
        || path.startsWith(ClusterState.CONFIGS_RESOURCE + "/") || path.startsWith(ClusterState.IDEALSTATES + "/")
        || path.startsWith(ClusterState.STATEMODELDEFS + "/") || path.equals(StateOps.MAINTENANCE_PATH);
  }

  /** Divides the delay settings of a cluster definition by the compression factor. */
  static void compressDelays(ClusterState state, long compression) {
    if (compression <= 1) {
      return;
    }
    ZNRecord cluster = state.get(ClusterState.clusterConfigPath(state.getClusterName()));
    if (cluster != null) {
      divide(cluster, "DELAY_REBALANCE_TIME", compression, 1);
      divide(cluster, "REBALANCE_TIMER_PERIOD", compression, 1000);
    }
    for (String resource : state.getIdealStates().keySet()) {
      ZNRecord idealState = state.get(ClusterState.idealStatePath(resource));
      divide(idealState, "REBALANCE_DELAY", compression, 1);
    }
  }

  private static void divide(ZNRecord record, String field, long compression, long minimum) {
    String value = record.getSimpleField(field);
    if (value == null) {
      return;
    }
    try {
      long time = Long.parseLong(value.trim());
      if (time > 0) {
        record.setSimpleField(field, String.valueOf(Math.max(minimum, time / compression)));
      }
    } catch (NumberFormatException ignored) {
      // Leave unparsable values alone.
    }
  }

  /**
   * Moves the timestamps the delay window reads (disable times, offline times, the last on-demand
   * rebalance) from the capture's clock to this machine's clock, so that at the start the same
   * fraction of each (compressed) delay window has passed as had passed at capture time. Applies with
   * any compression factor, including 1.
   */
  public static void rebaseTimestamps(ClusterState state, long compression) {
    long k = Math.max(1, compression);
    long now = System.currentTimeMillis();
    Long captured = state.getManifest().capturedAtMillis;
    long reference = captured != null && captured > 0 ? captured : now;
    for (String instance : state.getInstanceNames()) {
      rebase(state.get(ClusterState.instanceConfigPath(instance)), "HELIX_ENABLED_TIMESTAMP", reference, now, k);
      ZNRecord history = state.get(ClusterState.historyPath(instance));
      if (history != null) {
        rebase(history, "LAST_OFFLINE_TIME", reference, now, k);
      }
    }
    ZNRecord cluster = state.get(ClusterState.clusterConfigPath(state.getClusterName()));
    if (cluster != null) {
      rebase(cluster, "LAST_ON_DEMAND_REBALANCE_TIMESTAMP", reference, now, k);
    }
  }

  private static void rebase(ZNRecord record, String field, long reference, long now, long compression) {
    String value = record.getSimpleField(field);
    if (value == null) {
      return;
    }
    try {
      long time = Long.parseLong(value.trim());
      if (time > 0) {
        long elapsed = Math.max(0, reference - time);
        record.setSimpleField(field, String.valueOf(now - elapsed / compression));
      }
    } catch (NumberFormatException ignored) {
      // Leave unparsable values alone.
    }
  }

  public void write(String relativePath, ZNRecord record) {
    _accessor.set("/" + _cluster + "/" + relativePath, record, AccessOption.PERSISTENT);
  }

  public void remove(String relativePath) {
    _accessor.remove("/" + _cluster + "/" + relativePath, AccessOption.PERSISTENT);
  }

  /** Starts the controller process with the given base constraint weights. */
  public void startController(Map<String, Float> weights) throws IOException {
    stopController();
    Path conf = _work.resolve("controller-conf");
    Files.createDirectories(conf);
    List<String> lines = new ArrayList<>();
    lines.add("# Written by waged-sim for the local controller.");
    weights.forEach((name, weight) -> lines.add(name + "=" + weight));
    Files.write(conf.resolve("soft-constraint-weight.properties"), lines, StandardCharsets.UTF_8);
    // The engine reads rebalance failures from the controller's log, so WAGED's errors stay on here even
    // though the tool's own logging turns them off.
    Path logConfig = conf.resolve("log4j2-controller.properties");
    Files.write(logConfig, Arrays.asList(
        "status = error",
        "name = waged-sim-controller",
        "appender.console.type = Console",
        "appender.console.name = STDOUT",
        "appender.console.layout.type = PatternLayout",
        "appender.console.layout.pattern = %d{HH:mm:ss} %-5p %c{1} - %m%n",
        "rootLogger.level = error",
        "rootLogger.appenderRef.stdout.ref = STDOUT",
        "logger.rebalanceutil.name = org.apache.helix.util.RebalanceUtil",
        "logger.rebalanceutil.level = off"), StandardCharsets.UTF_8);
    String java = Paths.get(System.getProperty("java.home"), "bin", "java").toString();
    String classpath = conf.toAbsolutePath() + File.pathSeparator + System.getProperty("java.class.path");
    List<String> command = new ArrayList<>();
    command.add(java);
    command.add("-Xmx" + System.getenv().getOrDefault("WAGED_SIM_CONTROLLER_XMX", "2g"));
    command.add("-Dlog4j2.formatMsgNoLookups=true");
    command.add("-Dlog4j2.configurationFile=" + logConfig.toAbsolutePath());
    command.add("-cp");
    command.add(classpath);
    command.add(ControllerMain.class.getName());
    command.add(_zkAddress);
    command.add(_cluster);
    File log = _work.resolve("controller.log").toFile();
    _controller = new ProcessBuilder(command).redirectErrorStream(true)
        .redirectOutput(ProcessBuilder.Redirect.appendTo(log)).start();
  }

  public void stopController() {
    if (_controller != null) {
      _controller.destroy();
      try {
        if (!_controller.waitFor(10, TimeUnit.SECONDS)) {
          _controller.destroyForcibly().waitFor(10, TimeUnit.SECONDS);
        }
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
      }
      _controller = null;
    }
  }

  public boolean controllerAlive() {
    return _controller != null && _controller.isAlive();
  }

  public Path controllerLog() {
    return _work.resolve("controller.log");
  }

  /** Starts participants for every instance the definition marks live. */
  public void startParticipants(ClusterState state, long latency) throws Exception {
    _latency = latency;
    for (String instance : state.getLiveInstances().keySet()) {
      if (state.getInstanceConfig(instance) != null) {
        startParticipant(instance);
      }
    }
  }

  public void startParticipant(String instance) throws Exception {
    if (_participants.containsKey(instance)) {
      return;
    }
    HelixManager manager = HelixManagerFactory.getZKHelixManager(_cluster, instance, InstanceType.PARTICIPANT,
        _zkAddress);
    for (String model : _models) {
      manager.getStateMachineEngine().registerStateModelFactory(model, new SimulatedStateModelFactory(_latency));
    }
    manager.connect();
    _participants.put(instance, manager);
  }

  public void stopParticipant(String instance) {
    HelixManager manager = _participants.remove(instance);
    if (manager != null) {
      manager.disconnect();
    }
  }

  public Set<String> participants() {
    return new TreeSet<>(_participants.keySet());
  }

  /** @return live instance name to session id, as ZooKeeper has them */
  public Map<String, String> liveSessions() {
    Map<String, String> sessions = new TreeMap<>();
    String parent = "/" + _cluster + "/LIVEINSTANCES";
    for (String instance : _accessor.getChildNames(parent, 0)) {
      ZNRecord record = _accessor.get(parent + "/" + instance, null, 0);
      if (record != null) {
        sessions.put(instance, new LiveInstance(record).getEphemeralOwner());
      }
    }
    return sessions;
  }

  /** @return the served layout: current states of live instances */
  public Map<String, Map<String, Map<String, String>>> servedLayout() {
    Map<String, Map<String, Map<String, String>>> layout = new TreeMap<>();
    for (Map.Entry<String, String> live : liveSessions().entrySet()) {
      String parent = "/" + _cluster + "/INSTANCES/" + live.getKey() + "/CURRENTSTATES/" + live.getValue();
      for (ZNRecord record : _accessor.getChildren(parent, null, 0, 0, 0)) {
        CurrentState currentState = new CurrentState(record);
        currentState.getPartitionStateMap().forEach((partition, state) -> {
          if (!"DROPPED".equals(state)) {
            layout.computeIfAbsent(currentState.getResourceName(), k -> new TreeMap<>())
                .computeIfAbsent(partition, k -> new TreeMap<>()).put(live.getKey(), state);
          }
        });
      }
    }
    return layout;
  }

  public int pendingMessages() {
    int count = 0;
    for (String instance : liveSessions().keySet()) {
      count += _accessor.getChildNames("/" + _cluster + "/INSTANCES/" + instance + "/MESSAGES", 0).size();
    }
    return count;
  }

  /** @return modification ids of the baseline and best possible writes, to detect new passes */
  public long[] assignmentVersions() {
    long[] versions = new long[2];
    String[] kinds = {"BASELINE", "BEST_POSSIBLE"};
    for (int i = 0; i < 2; i++) {
      Stat stat = _accessor.getStat("/" + _cluster + "/ASSIGNMENT_METADATA/" + kinds[i] + "/LAST_SUCCESSFUL_WRITE", 0);
      versions[i] = stat == null ? -1 : stat.getMzxid();
    }
    return versions;
  }

  public Map<String, Map<String, Map<String, String>>> baseline() {
    return Layouts.fromAssignments(new Store(_buckets, _cluster).getBaseline());
  }

  public Map<String, Map<String, Map<String, String>>> bestPossible() {
    return Layouts.fromAssignments(new Store(_buckets, _cluster).getBestPossibleAssignment());
  }

  /** @return records under a relative path written by the controller (for example histories) */
  public ZNRecord read(String relativePath) {
    return _accessor.get("/" + _cluster + "/" + relativePath, null, 0);
  }

  @Override
  public void close() {
    for (String instance : new ArrayList<>(_participants.keySet())) {
      stopParticipant(instance);
    }
    stopController();
    if (_buckets != null) {
      _buckets.disconnect();
    }
    _admin.close();
    _accessor.close();
    _zk.shutdown();
  }

  static void deleteRecursively(Path path) throws IOException {
    if (!Files.exists(path)) {
      return;
    }
    try (Stream<Path> walk = Files.walk(path)) {
      for (Path p : (Iterable<Path>) walk.sorted(Comparator.reverseOrder())::iterator) {
        Files.delete(p);
      }
    }
  }

  /**
   * The production assignment store, reading and writing ZooKeeper buckets. The store disconnects its
   * accessor when closed or finalized, so it gets a view of the shared accessor that cannot disconnect it.
   */
  static class Store extends AssignmentMetadataStore {
    Store(ZkBucketDataAccessor buckets, String cluster) {
      super(new SharedBuckets(buckets), cluster);
    }
  }

  /** Delegates to a shared bucket accessor; {@link #disconnect()} leaves it open. */
  static final class SharedBuckets implements org.apache.helix.BucketDataAccessor {
    private final ZkBucketDataAccessor _delegate;

    SharedBuckets(ZkBucketDataAccessor delegate) {
      _delegate = delegate;
    }

    @Override
    public <T extends org.apache.helix.HelixProperty> boolean compressedBucketWrite(String path, T value)
        throws IOException {
      return _delegate.compressedBucketWrite(path, value);
    }

    @Override
    public <T extends org.apache.helix.HelixProperty> org.apache.helix.HelixProperty compressedBucketRead(
        String path, Class<T> helixPropertySubType) {
      return _delegate.compressedBucketRead(path, helixPropertySubType);
    }

    @Override
    public void compressedBucketDelete(String path) {
      _delegate.compressedBucketDelete(path);
    }

    @Override
    public void disconnect() {
      // The shared accessor is closed by LocalCluster#close.
    }
  }

  /** @return per-instance names known to ZooKeeper, for diagnostics */
  public Map<String, Integer> counts() {
    Map<String, Integer> counts = new HashMap<>();
    counts.put("instances", _accessor.getChildNames("/" + _cluster + "/CONFIGS/PARTICIPANT", 0).size());
    counts.put("live", liveSessions().size());
    counts.put("participants", _participants.size());
    return counts;
  }
}
