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

import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.TreeSet;

import org.apache.helix.wagedsim.cluster.ClusterState;
import org.apache.helix.wagedsim.engine.Engine;
import org.apache.helix.wagedsim.engine.EngineSettings;
import org.apache.helix.wagedsim.engine.RoundResult;
import org.apache.helix.zookeeper.datamodel.ZNRecord;

/**
 * Runs rounds on a real local cluster. Each round writes the changed configuration to ZooKeeper,
 * starts or stops participants to match liveness, then waits until the cluster is quiet: no pending
 * messages, and current states and assignment metadata unchanged for the settle window. Virtual time
 * runs {@code timeCompression} times faster than the wall clock.
 */
public class LocalClusterEngine implements Engine {
  private ClusterState _state;
  private EngineSettings _settings;
  private LocalCluster _cluster;
  private final Map<String, ZNRecord> _written = new HashMap<>();
  private Map<String, String> _sessions = new HashMap<>();
  private long _wallStart;
  private long _simStart;
  private long _advanced;
  private boolean _restartPending;
  private long[] _versions = {-1, -1};
  private long _logOffset;

  @Override
  public String mode() {
    return "local";
  }

  @Override
  public void start(ClusterState state, EngineSettings settings) throws Exception {
    _state = state;
    _settings = settings.copy();
    Path work = _settings.workDir != null ? Paths.get(_settings.workDir)
        : Files.createTempDirectory("waged-sim-local-");
    _cluster = new LocalCluster(work, 0);
    // Keep this engine's definition on the local clock, so events never write capture-time stamps back.
    LocalCluster.rebaseTimestamps(state, _settings.timeCompression);
    _cluster.load(state, _settings.timeCompression);
    Set<String> waged = state.getWagedIdealStates().keySet();
    for (Map.Entry<String, ZNRecord> entry : state.getZnodes().entrySet()) {
      if (LocalCluster.isControllerInput(entry.getKey()) && ClusterState.isSimulated(entry.getKey(), waged)) {
        _written.put(entry.getKey(), new ZNRecord(entry.getValue()));
      }
    }
    _wallStart = System.currentTimeMillis();
    Long captured = state.getManifest().capturedAtMillis;
    _simStart = captured != null && captured > 0 ? captured : _wallStart;
    // Participants first, so the controller's first pipeline sees every live instance.
    _cluster.startParticipants(state, _settings.transitionLatencyMillis);
    _sessions = sessions(state);
    long deadline = System.currentTimeMillis() + _settings.roundTimeoutMillis;
    while (_cluster.liveSessions().size() < _sessions.size() && System.currentTimeMillis() < deadline) {
      Thread.sleep(100);
    }
    _cluster.startController(_settings.constraintWeights);
    RoundResult bootstrap = new RoundResult();
    settle(bootstrap);
    readBack();
  }

  private static Map<String, String> sessions(ClusterState state) {
    Map<String, String> sessions = new HashMap<>();
    state.getLiveInstances().forEach((name, live) -> sessions.put(name, live.getEphemeralOwner()));
    return sessions;
  }

  @Override
  public ClusterState state() {
    return _state;
  }

  @Override
  public long now() {
    return _simStart + (System.currentTimeMillis() - _wallStart) * _cluster.compression() + _advanced;
  }

  @Override
  public void advanceClock(long millis) throws InterruptedException {
    if (millis <= 0) {
      return;
    }
    // Waiting d / k of wall time moves the controller's clock as far as d of production time.
    long wait = millis / _cluster.compression();
    long before = System.currentTimeMillis();
    Thread.sleep(wait);
    _advanced += millis - (System.currentTimeMillis() - before) * _cluster.compression();
  }

  @Override
  public void restartController() {
    _restartPending = true;
  }

  @Override
  public void setConstraintWeights(Map<String, Float> weights) {
    _settings.constraintWeights = new LinkedHashMap<>(weights);
    _restartPending = true;
  }

  @Override
  public Map<String, Float> constraintWeights() {
    return new LinkedHashMap<>(_settings.constraintWeights);
  }

  @Override
  public RoundResult runRound(int round) throws Exception {
    RoundResult result = new RoundResult();
    result.round = round;
    long start = System.currentTimeMillis();
    applyConfiguration();
    applyLiveness(result);
    if (_restartPending) {
      _cluster.startController(_settings.constraintWeights);
      _restartPending = false;
      result.notes.add("controller restarted");
    }
    result.computeMillis = System.currentTimeMillis() - start;
    long settleStart = System.currentTimeMillis();
    settle(result);
    result.settleMillis = System.currentTimeMillis() - settleStart;
    readBack();
    long[] versions = _cluster.assignmentVersions();
    result.passes.put("global", versions[0] != _versions[0] ? 1L : 0L);
    result.passes.put("partial", versions[1] != _versions[1] ? 1L : 0L);
    _versions = versions;
    result.failures.addAll(newControllerErrors());
    for (String failure : result.failures) {
      result.failureCategories.add(failure.contains("CAPACITY") ? "CAPACITY_DEFICIT" : "CONTROLLER_ERROR");
    }
    if (!_cluster.controllerAlive()) {
      throw new IOException("The local controller exited; see " + _cluster.controllerLog());
    }
    result.simTimeMillis = now();
    result.maintenance = _cluster.read(org.apache.helix.wagedsim.engine.StateOps.MAINTENANCE_PATH) != null;
    return result;
  }

  /** Writes configuration znodes that changed since the last round. */
  private void applyConfiguration() {
    Map<String, ZNRecord> desired = new HashMap<>();
    Set<String> waged = _state.getWagedIdealStates().keySet();
    for (Map.Entry<String, ZNRecord> entry : _state.getZnodes().entrySet()) {
      if (LocalCluster.isControllerInput(entry.getKey()) && ClusterState.isSimulated(entry.getKey(), waged)) {
        desired.put(entry.getKey(), entry.getValue());
      }
    }
    for (Map.Entry<String, ZNRecord> entry : desired.entrySet()) {
      ZNRecord previous = _written.get(entry.getKey());
      if (previous == null || !previous.equals(entry.getValue())) {
        ZNRecord record = new ZNRecord(entry.getValue());
        if (entry.getKey().startsWith(ClusterState.CONFIGS_CLUSTER + "/")
            || entry.getKey().startsWith(ClusterState.IDEALSTATES + "/")) {
          // Keep the compressed delay settings in the local cluster.
          ClusterState scratch = new ClusterState(_state.getClusterName());
          scratch.put(entry.getKey(), record);
          LocalCluster.compressDelays(scratch, _cluster.compression());
          record = scratch.get(entry.getKey());
        }
        _cluster.write(entry.getKey(), record);
        _written.put(entry.getKey(), new ZNRecord(entry.getValue()));
      }
    }
    for (String path : new ArrayList<>(_written.keySet())) {
      if (!desired.containsKey(path)) {
        _cluster.remove(path);
        _written.remove(path);
      }
    }
  }

  /** Starts, stops or restarts participants to match the live instances in the definition. */
  private void applyLiveness(RoundResult result) throws Exception {
    Map<String, String> desired = sessions(_state);
    Set<String> running = _cluster.participants();
    for (String instance : running) {
      if (!desired.containsKey(instance)) {
        _cluster.stopParticipant(instance);
      } else if (!Objects.equals(desired.get(instance), _sessions.get(instance))) {
        _cluster.stopParticipant(instance);
        _cluster.startParticipant(instance);
      }
    }
    for (String instance : desired.keySet()) {
      if (!running.contains(instance) && _state.getInstanceConfig(instance) != null) {
        _cluster.startParticipant(instance);
      }
    }
    _sessions = desired;
  }

  private void settle(RoundResult result) throws InterruptedException {
    long deadline = System.currentTimeMillis() + _settings.roundTimeoutMillis;
    String fingerprint = null;
    long quietSince = System.currentTimeMillis();
    Thread.sleep(Math.min(500, _settings.settleQuietMillis));
    while (System.currentTimeMillis() < deadline) {
      int pending = _cluster.pendingMessages();
      long[] versions = _cluster.assignmentVersions();
      String now = pending + "|" + versions[0] + "|" + versions[1] + "|" + _cluster.servedLayout().hashCode();
      if (!now.equals(fingerprint)) {
        fingerprint = now;
        quietSince = System.currentTimeMillis();
      } else if (pending == 0 && System.currentTimeMillis() - quietSince >= _settings.settleQuietMillis) {
        result.settled = true;
        return;
      }
      Thread.sleep(200);
    }
    result.settled = false;
    result.notes.add("not settled within " + _settings.roundTimeoutMillis + " ms");
  }

  /** Copies the served layout, the assignments and the maintenance signal from ZooKeeper into the definition. */
  private void readBack() {
    _state.replaceCurrentStates(_cluster.servedLayout());
    _state.setBaseline(_cluster.baseline());
    _state.setBestPossible(_cluster.bestPossible());
    // The controller can enter maintenance on its own; track the signal so a scenario can end it.
    ZNRecord maintenance = _cluster.read(org.apache.helix.wagedsim.engine.StateOps.MAINTENANCE_PATH);
    if (maintenance != null) {
      _state.put(org.apache.helix.wagedsim.engine.StateOps.MAINTENANCE_PATH, new ZNRecord(maintenance));
      _written.put(org.apache.helix.wagedsim.engine.StateOps.MAINTENANCE_PATH, new ZNRecord(maintenance));
    } else {
      _state.remove(org.apache.helix.wagedsim.engine.StateOps.MAINTENANCE_PATH);
      _written.remove(org.apache.helix.wagedsim.engine.StateOps.MAINTENANCE_PATH);
    }
    for (String instance : _state.getInstanceNames()) {
      ZNRecord history = _cluster.read(ClusterState.historyPath(instance));
      if (history != null) {
        _state.put(ClusterState.historyPath(instance), history);
      }
    }
  }

  /** @return controller log lines about rebalance failures written since the last call */
  private List<String> newControllerErrors() {
    List<String> errors = new ArrayList<>();
    Path log = _cluster.controllerLog();
    try {
      if (!Files.exists(log)) {
        return errors;
      }
      try (RandomAccessFile file = new RandomAccessFile(log.toFile(), "r")) {
        if (file.length() < _logOffset) {
          _logOffset = 0;
        }
        file.seek(_logOffset);
        byte[] bytes = new byte[(int) (file.length() - _logOffset)];
        file.readFully(bytes);
        _logOffset = file.length();
        for (String line : new String(bytes, StandardCharsets.UTF_8).split("\n")) {
          if (line.contains("Failed to calculate") || line.contains("HelixRebalanceException")) {
            errors.add(line.length() > 300 ? line.substring(0, 300) : line);
          }
        }
      }
    } catch (IOException e) {
      errors.add("Cannot read controller log: " + e.getMessage());
    }
    return errors.size() > 5 ? new ArrayList<>(errors.subList(0, 5)) : errors;
  }

  public LocalCluster cluster() {
    return _cluster;
  }

  @Override
  public void close() {
    if (_cluster != null) {
      _cluster.close();
      _cluster = null;
    }
  }

  /** @return names of instances with participants, for tests */
  public Set<String> participants() {
    return _cluster == null ? new TreeSet<>() : _cluster.participants();
  }
}
