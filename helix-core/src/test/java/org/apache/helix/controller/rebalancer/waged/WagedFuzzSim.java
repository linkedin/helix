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
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Random;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.concurrent.AbstractExecutorService;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;

import org.apache.helix.BucketDataAccessor;
import org.apache.helix.HelixConstants;
import org.apache.helix.HelixProperty;
import org.apache.helix.HelixRebalanceException;
import org.apache.helix.constants.InstanceConstants;
import org.apache.helix.controller.dataproviders.ResourceControllerDataProvider;
import org.apache.helix.controller.rebalancer.waged.constraints.FuzzRecordingAlgorithm;
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
import org.apache.helix.monitoring.metrics.model.CountMetric;
import org.apache.helix.monitoring.metrics.model.Metric;
import org.apache.helix.zookeeper.datamodel.ZNRecord;
import org.apache.helix.zookeeper.datamodel.serializer.ZNRecordJacksonSerializer;
import org.apache.helix.zookeeper.zkclient.exception.ZkNoNodeException;

/**
 * A seeded, reproducible simulator that drives the real WagedRebalancer, with an in-memory
 * assignment store and a real ResourceControllerDataProvider populated through its setters, through
 * random topology and configuration events. Every observable output of each step is rendered to
 * canonical JSON so runs can be compared byte for byte.
 *
 * This file deliberately uses no instance tag isolation API, so the identical source compiles and
 * runs on a build without the feature. Callers that want the feature on pass a cluster
 * config customizer to the driver.
 */
public final class WagedFuzzSim {
  public enum Mode {
    /** Anything goes, including failures. Used for the flag off parity dump. */
    GENERAL,
    /** Every clique keeps a wide feasibility margin at every step. */
    FEASIBLE,
    /** FEASIBLE, plus events that break and repair whole cliques by weight or by capacity. */
    BREAK
  }

  public static final String DISK = "DISK";
  public static final String POOL_TAG = "pool";
  public static final long DELAY_MS = TimeUnit.HOURS.toMillis(3);
  static final long IN_WINDOW_AGO = TimeUnit.MINUTES.toMillis(1);
  static final long OUT_OF_WINDOW_AGO = TimeUnit.HOURS.toMillis(30);
  static final List<String> STATE_MODELS =
      Arrays.asList("MasterSlave", "LeaderStandby", "OnlineOffline");
  /**
   * The cluster config field of the instance tag isolation flag, set raw so this file compiles on
   * a build without the feature.
   */
  public static final String ISOLATION_FLAG_FIELD = "WAGED_INSTANCE_TAG_ISOLATION_ENABLED";
  /**
   * The forms a FLAG_FORM step gives the isolation flag field, null standing for the field being
   * absent. A build with the feature reads the field as on only when it is "true" in any letter
   * case, so each of these reads as off.
   */
  public static final List<String> FLAG_OFF_FORMS =
      Collections.unmodifiableList(Arrays.asList(null, "false", "FALSE", "", "notabool"));

  public static final class SimNode {
    public final String name;
    InstanceConfig config;
    InstanceConfig parkedConfig;
    boolean live = true;
    int session;
    long offlineTime = -1L;

    SimNode(String name) {
      this.name = name;
    }

    SimNode copy() {
      SimNode n = new SimNode(name);
      n.config = config == null ? null : new InstanceConfig(deepCopy(config.getRecord()));
      n.parkedConfig =
          parkedConfig == null ? null : new InstanceConfig(deepCopy(parkedConfig.getRecord()));
      n.live = live;
      n.session = session;
      n.offlineTime = offlineTime;
      return n;
    }

    public InstanceConfig config() {
      return config;
    }

    public boolean isLive() {
      return live;
    }

    public List<String> tags() {
      return config == null ? Collections.emptyList() : config.getTags();
    }

    public int capacity() {
      return config == null ? 0 : config.getInstanceCapacityMap().getOrDefault(DISK, 0);
    }

    public InstanceConstants.InstanceOperation operation() {
      return config == null ? null : config.getInstanceOperation().getOperation();
    }

    /** Live, enabled and assignable now, so not counted on for anything delayed. */
    public boolean healthy() {
      return config != null && live
          && operation() == InstanceConstants.InstanceOperation.ENABLE;
    }
  }

  public static final class SimResource {
    public final String name;
    public String tag;
    public String stateModel;
    public int partitions;
    public int replicas;
    /** Negative leaves the ideal state without a minimum, which then means every replica. */
    public int minActive;
    public int weight;
    public final TreeMap<String, Integer> partitionWeights = new TreeMap<>();

    SimResource(String name) {
      this.name = name;
    }

    SimResource copy() {
      SimResource r = new SimResource(name);
      r.tag = tag;
      r.stateModel = stateModel;
      r.partitions = partitions;
      r.replicas = replicas;
      r.minActive = minActive;
      r.weight = weight;
      r.partitionWeights.putAll(partitionWeights);
      return r;
    }

    public List<String> partitionNames() {
      List<String> names = new ArrayList<>();
      for (int i = 0; i < partitions; i++) {
        names.add(name + "_" + i);
      }
      return names;
    }

    public int weightOf(String partition) {
      return partitionWeights.getOrDefault(partition, weight);
    }

    public long demand() {
      long total = 0;
      for (String p : partitionNames()) {
        total += (long) weightOf(p) * replicas;
      }
      return total;
    }

    public int maxWeight() {
      int max = weight;
      for (int w : partitionWeights.values()) {
        max = Math.max(max, w);
      }
      return max;
    }

    /** The replicas each partition must keep on live enabled nodes while others are away. */
    public int requiredActive() {
      return minActive < 0 ? replicas : Math.min(minActive, replicas);
    }

    IdealState idealState() {
      IdealState is = new IdealState(name);
      is.setRebalanceMode(IdealState.RebalanceMode.FULL_AUTO);
      is.setRebalancerClassName(WagedRebalancer.class.getName());
      is.setStateModelDefRef(stateModel);
      is.setNumPartitions(partitions);
      is.setReplicas(String.valueOf(replicas));
      if (minActive >= 0) {
        is.setMinActiveReplicas(minActive);
      }
      if (tag != null) {
        is.setInstanceGroupTag(tag);
      }
      for (String p : partitionNames()) {
        is.getRecord().setListField(p, new ArrayList<>());
      }
      return is;
    }

    ResourceConfig resourceConfig() {
      ResourceConfig rc = new ResourceConfig(name);
      Map<String, Map<String, Integer>> capacity = new TreeMap<>();
      capacity.put(ResourceConfig.DEFAULT_PARTITION_KEY, Collections.singletonMap(DISK, weight));
      for (Map.Entry<String, Integer> e : partitionWeights.entrySet()) {
        capacity.put(e.getKey(), Collections.singletonMap(DISK, e.getValue()));
      }
      try {
        rc.setPartitionCapacityMap(capacity);
      } catch (IOException e) {
        throw new IllegalStateException(e);
      }
      return rc;
    }
  }

  /** What a BREAK event changed, so REPAIR can restore it exactly. */
  public static final class Broken {
    public final String clique;
    public final boolean byWeight;
    final Map<String, SimResource> savedResources = new TreeMap<>();
    final Map<String, Integer> savedCapacities = new TreeMap<>();
    /** A mild weight break that followOutages walks through outages. */
    boolean followed;
    /** Whether followOutages already gave the clique its fresh node. */
    boolean grown;
    /** The member followOutages took offline and brings back next. */
    String downed;

    Broken(String clique, boolean byWeight) {
      this.clique = clique;
      this.byWeight = byWeight;
    }

    Broken copy() {
      Broken b = new Broken(clique, byWeight);
      for (Map.Entry<String, SimResource> e : savedResources.entrySet()) {
        b.savedResources.put(e.getKey(), e.getValue().copy());
      }
      b.savedCapacities.putAll(savedCapacities);
      b.followed = followed;
      b.grown = grown;
      b.downed = downed;
      return b;
    }
  }

  public final long seed;
  public final Mode mode;
  public final String clusterName;
  final Random rnd;
  /**
   * Draws that steer a scenario, kept apart from rnd so they never shift its draws: whether the
   * delay is on, the minimums, whether an outage stays inside its window, how wild a break is,
   * the outages that go along with an event, and the members followOutages takes offline.
   */
  private final Random aux;
  /** Draws for FLAG_FORM: whether a step is one, and the form the flag field moves to. */
  private final Random forms;
  /**
   * Whether a scenario that does not collide makes wild breaks common, drawn at its first weight
   * break. The others keep the rnd rate, so they also spend long stretches with only mild breaks,
   * which a scope can still place.
   */
  private Boolean wildBreaks;
  ClusterConfig clusterConfig;
  public TreeMap<String, SimNode> nodes = new TreeMap<>();
  public TreeMap<String, SimResource> resources = new TreeMap<>();
  public List<String> cliques = new ArrayList<>();
  public TreeMap<String, Broken> broken = new TreeMap<>();
  public boolean pureCliques;
  boolean poolLabel;
  int nodeSeq;
  int resourceSeq;
  /** Set by the last event; drivers call reset() on the rebalancer before the step. */
  public boolean failoverRequested;
  /** Set by the last event; whether participants converge to the emitted states after the step. */
  public boolean converge = true;
  /** When false, BREAK mode never breaks every block at once. */
  public boolean allowBreakAll = true;
  /** When set, BREAK mode also moves nodes out of broken cliques into healthy ones. */
  public boolean collide;
  /**
   * When set, a BREAK is a harmless stand in with the same schedule: the victim's weight is halved
   * or the clique's capacity raised by half, so nothing becomes infeasible. A control for how far
   * WAGED alone drifts from a twin that never saw the change.
   */
  public boolean softBreaks;
  /** Optional hook that must accept the state after a candidate event, or the event is retried. */
  public java.util.function.Predicate<WagedFuzzSim> extraGuard;
  /**
   * When set, about one step in five is a FLAG_FORM step: the isolation flag field moves to
   * another of FLAG_OFF_FORMS and nothing else changes. Those draws come from a stream of their
   * own, so rnd and aux draw exactly as they do with this unset.
   */
  public boolean flagForms;
  private String overloadedClique;

  private WagedFuzzSim(long seed, Mode mode) {
    this.seed = seed;
    this.mode = mode;
    this.clusterName = "fuzz" + mode.name().toLowerCase() + seed;
    this.rnd = new Random(seed * 0x9E3779B97F4A7C15L + 31L * mode.ordinal() + 7L);
    this.aux = new Random(seed * 0xC6BC279692B5C323L + 131L * mode.ordinal() + 17L);
    this.forms = new Random(seed * 0xD6E8FEB86659FD93L + 251L * mode.ordinal() + 29L);
  }

  public static WagedFuzzSim generate(long seed, Mode mode) {
    WagedFuzzSim sim = new WagedFuzzSim(seed, mode);
    sim.init();
    return sim;
  }

  /**
   * An empty scenario with delayed rebalance off, filled in by hand through the methods below. Used
   * to turn a shrunk seed, or a case the generator cannot reach, into a deterministic test.
   */
  public static WagedFuzzSim handBuilt() {
    WagedFuzzSim sim = new WagedFuzzSim(0L, Mode.BREAK);
    sim.clusterConfig = new ClusterConfig(sim.clusterName);
    sim.clusterConfig.setInstanceCapacityKeys(Collections.singletonList(DISK));
    sim.clusterConfig.setDefaultPartitionWeightMap(Collections.singletonMap(DISK, 1));
    sim.clusterConfig.setDefaultInstanceCapacityMap(Collections.singletonMap(DISK, 100));
    sim.clusterConfig.setTopologyAwareEnabled(false);
    sim.clusterConfig.setRebalanceDelayTime(DELAY_MS);
    sim.clusterConfig.setDelayRebalaceEnabled(false);
    return sim;
  }

  public SimNode handNode(String tag, int capacity) {
    if (tag != null && !cliques.contains(tag)) {
      cliques.add(tag);
    }
    return addNode(tag, capacity);
  }

  public SimResource handResource(String tag, String stateModel, int partitions, int replicas,
      int weight) {
    SimResource r = new SimResource(String.format("r%03d", resourceSeq++));
    r.tag = tag;
    r.stateModel = stateModel;
    r.partitions = partitions;
    r.replicas = replicas;
    r.weight = weight;
    resources.put(r.name, r);
    return r;
  }

  /** Takes a node down for longer than any delay window. */
  public void handOffline(String node) {
    SimNode n = nodes.get(node);
    n.live = false;
    n.offlineTime = System.currentTimeMillis() - OUT_OF_WINDOW_AGO;
  }

  /** Takes a node down inside its delay window. */
  public void handOfflineInWindow(String node) {
    SimNode n = nodes.get(node);
    n.live = false;
    n.offlineTime = System.currentTimeMillis() - IN_WINDOW_AGO;
  }

  /** Disables a node inside its delay window. */
  public void handDisableInWindow(String node) {
    InstanceConfig config = nodes.get(node).config;
    config.setInstanceOperation(InstanceConstants.InstanceOperation.DISABLE);
    config.getRecord().setLongField(
        InstanceConfig.InstanceConfigProperty.HELIX_ENABLED_TIMESTAMP.name(),
        System.currentTimeMillis() - IN_WINDOW_AGO);
  }

  /** Moves a node from one tag to another, the way an operator would retag it. */
  public void handRetag(String node, String from, String to) {
    InstanceConfig config = nodes.get(node).config;
    config.removeTag(from);
    config.addTag(to);
  }

  private int skewed(int a0, int a1, int b0, int b1, int c0, int c1) {
    int roll = rnd.nextInt(10);
    if (roll < 7) {
      return a0 + rnd.nextInt(a1 - a0 + 1);
    } else if (roll < 9) {
      return b0 + rnd.nextInt(b1 - b0 + 1);
    }
    return c0 + rnd.nextInt(c1 - c0 + 1);
  }

  private <T> T pick(List<T> list) {
    return list.get(rnd.nextInt(list.size()));
  }

  private void init() {
    clusterConfig = new ClusterConfig(clusterName);
    clusterConfig.setInstanceCapacityKeys(Collections.singletonList(DISK));
    clusterConfig.setDefaultPartitionWeightMap(Collections.singletonMap(DISK, 1));
    clusterConfig.setDefaultInstanceCapacityMap(Collections.singletonMap(DISK, 100));
    clusterConfig.setTopologyAwareEnabled(false);
    clusterConfig.setRebalanceDelayTime(DELAY_MS);
    boolean delay = rnd.nextInt(3) == 0;
    boolean auxDelay = aux.nextInt(3) != 0;
    clusterConfig.setDelayRebalaceEnabled(delay || auxDelay);

    int cliqueCount = skewed(3, 6, 7, 12, 13, 30);
    pureCliques = mode == Mode.GENERAL ? rnd.nextBoolean() : rnd.nextInt(3) != 0;
    // Some general scenarios start with one clique the greedy algorithm cannot plausibly place.
    overloadedClique = mode == Mode.GENERAL && rnd.nextInt(10) == 0
        ? "c" + rnd.nextInt(cliqueCount) : null;
    poolLabel = rnd.nextInt(10) < 3;
    for (int c = 0; c < cliqueCount; c++) {
      String tag = "c" + c;
      cliques.add(tag);
      int size = skewed(3, 5, 6, 8, 9, 12);
      int base = 60 + rnd.nextInt(141);
      for (int i = 0; i < size; i++) {
        addNode(tag, jitter(base));
      }
    }
    for (String tag : cliques) {
      int count = 1 + rnd.nextInt(3);
      for (int r = 0; r < count; r++) {
        addResource(tag);
      }
    }
    if (rnd.nextInt(5) == 0) {
      int spare = 1 + rnd.nextInt(3);
      for (int i = 0; i < spare; i++) {
        addNode(null, 60 + rnd.nextInt(141));
      }
    }
    if (!pureCliques) {
      if (rnd.nextBoolean()) {
        int shared = 1 + rnd.nextInt(2);
        for (int i = 0; i < shared; i++) {
          SimNode node = pick(new ArrayList<>(nodes.values()));
          String extra = pick(cliques);
          if (node.config != null && !node.tags().contains(extra)) {
            node.config.addTag(extra);
          }
        }
      }
      if (rnd.nextBoolean()) {
        int untagged = 1 + rnd.nextInt(2);
        for (int i = 0; i < untagged; i++) {
          addResource(null);
        }
      }
    }
    if (mode != Mode.GENERAL) {
      // Shrink weights until every clique has a wide margin.
      for (int guard = 0; guard < 64 && !allRobust(); guard++) {
        for (SimResource r : resources.values()) {
          r.weight = Math.max(1, r.weight * 2 / 3);
          r.partitionWeights.replaceAll((k, v) -> Math.max(1, v * 2 / 3));
        }
      }
    }
  }

  private int jitter(int base) {
    return Math.max(10, (int) Math.round(base * (0.8 + 0.4 * rnd.nextDouble())));
  }

  private SimNode addNode(String cliqueTag, int capacity) {
    SimNode node = new SimNode(String.format("n%04d", nodeSeq++));
    InstanceConfig config = new InstanceConfig(new ZNRecord(node.name));
    config.setHostName(node.name);
    config.setPort("12000");
    config.setInstanceCapacityMap(Collections.singletonMap(DISK, capacity));
    if (cliqueTag != null) {
      config.addTag(cliqueTag);
    }
    if (poolLabel) {
      config.addTag(POOL_TAG);
    }
    node.config = config;
    nodes.put(node.name, node);
    return node;
  }

  private SimResource addResource(String tag) {
    SimResource r = new SimResource(String.format("r%03d", resourceSeq++));
    r.tag = tag;
    r.stateModel = pick(STATE_MODELS);
    List<SimNode> members = tag == null ? new ArrayList<>(nodes.values()) : nodesWithTag(tag);
    int maxReplicas = mode == Mode.GENERAL ? Math.min(3, Math.max(1, members.size()))
        : Math.min(3, Math.max(1, members.size() - 1));
    r.replicas = 1 + rnd.nextInt(maxReplicas);
    r.partitions = tag == null ? 1 + rnd.nextInt(4) : skewed(1, 6, 7, 10, 11, 16);
    r.minActive = r.replicas > 1 ? rnd.nextInt(r.replicas) : 0;
    // Below the replica count one away node never leaves a partition short, so a delayed window
    // only needs a top up when the minimum is left unset or equals the replica count.
    int shape = aux.nextInt(3);
    if (shape == 0) {
      r.minActive = -1;
    } else if (shape == 1) {
      r.minActive = r.replicas;
    }
    if (tag == null) {
      r.weight = 1 + rnd.nextInt(2);
    } else {
      long capacity = 0;
      int minCapacity = Integer.MAX_VALUE;
      for (SimNode n : members) {
        capacity += n.capacity();
        minCapacity = Math.min(minCapacity, n.capacity());
      }
      boolean overload = mode == Mode.GENERAL && tag.equals(overloadedClique);
      double share = overload ? 0.5 + 0.5 * rnd.nextDouble()
          : mode == Mode.GENERAL ? 0.03 + 0.15 * rnd.nextDouble() : 0.04 + 0.1 * rnd.nextDouble();
      int w = (int) Math.max(1, Math.round(share * capacity / (r.partitions * r.replicas)));
      if (minCapacity != Integer.MAX_VALUE) {
        w = Math.max(1, Math.min(w, minCapacity / (overload ? 1 : mode == Mode.GENERAL ? 3 : 4)));
      }
      r.weight = w;
    }
    if (rnd.nextInt(5) == 0) {
      for (String p : r.partitionNames()) {
        if (rnd.nextInt(3) == 0) {
          r.partitionWeights.put(p,
              Math.max(1, (int) Math.round(r.weight * (0.5 + rnd.nextDouble()))));
        }
      }
    }
    resources.put(r.name, r);
    return r;
  }

  public List<SimNode> nodesWithTag(String tag) {
    List<SimNode> result = new ArrayList<>();
    for (SimNode n : nodes.values()) {
      if (n.tags().contains(tag)) {
        result.add(n);
      }
    }
    return result;
  }

  public List<SimResource> resourcesWithTag(String tag) {
    List<SimResource> result = new ArrayList<>();
    for (SimResource r : resources.values()) {
      if (tag.equals(r.tag)) {
        result.add(r);
      }
    }
    return result;
  }

  /**
   * True when the clique can hold its resources on its healthy nodes alone with a wide margin, so
   * the greedy algorithm cannot plausibly fail on it.
   */
  public boolean robust(String tag) {
    List<SimResource> owned = resourcesWithTag(tag);
    if (owned.isEmpty()) {
      return true;
    }
    int maxReplicas = 0;
    long demand = 0;
    int maxWeight = 0;
    for (SimResource r : owned) {
      maxReplicas = Math.max(maxReplicas, r.replicas);
      demand += r.demand();
      maxWeight = Math.max(maxWeight, r.maxWeight());
    }
    long capacity = 0;
    int minCapacity = Integer.MAX_VALUE;
    int healthy = 0;
    for (SimNode n : nodesWithTag(tag)) {
      if (n.healthy()) {
        healthy++;
        capacity += n.capacity();
        minCapacity = Math.min(minCapacity, n.capacity());
      }
    }
    return healthy >= maxReplicas + 1 && demand * 2 <= capacity && maxWeight * 3 <= minCapacity;
  }

  boolean untaggedRobust() {
    long demand = 0;
    int maxReplicas = 0;
    for (SimResource r : resources.values()) {
      if (r.tag == null) {
        demand += r.demand();
        maxReplicas = Math.max(maxReplicas, r.replicas);
      }
    }
    if (demand == 0) {
      return true;
    }
    long capacity = 0;
    int healthy = 0;
    for (SimNode n : nodes.values()) {
      if (n.healthy()) {
        capacity += n.capacity();
        healthy++;
      }
    }
    return healthy >= maxReplicas + 1 && demand * 20 <= capacity;
  }

  public boolean allRobust() {
    for (String tag : cliques) {
      if (!broken.containsKey(tag) && !robust(tag)) {
        return false;
      }
    }
    return untaggedRobust();
  }

  // ---------------------------------------------------------------------------------------------
  // Events
  // ---------------------------------------------------------------------------------------------

  private static final class State {
    ClusterConfig clusterConfig;
    TreeMap<String, SimNode> nodes = new TreeMap<>();
    TreeMap<String, SimResource> resources = new TreeMap<>();
    List<String> cliques;
    TreeMap<String, Broken> broken = new TreeMap<>();
    int nodeSeq;
    int resourceSeq;
  }

  private State save() {
    State s = new State();
    s.clusterConfig = new ClusterConfig(deepCopy(clusterConfig.getRecord()));
    for (Map.Entry<String, SimNode> e : nodes.entrySet()) {
      s.nodes.put(e.getKey(), e.getValue().copy());
    }
    for (Map.Entry<String, SimResource> e : resources.entrySet()) {
      s.resources.put(e.getKey(), e.getValue().copy());
    }
    s.cliques = new ArrayList<>(cliques);
    for (Map.Entry<String, Broken> e : broken.entrySet()) {
      s.broken.put(e.getKey(), e.getValue().copy());
    }
    s.nodeSeq = nodeSeq;
    s.resourceSeq = resourceSeq;
    return s;
  }

  private void restore(State s) {
    clusterConfig = s.clusterConfig;
    nodes = s.nodes;
    resources = s.resources;
    cliques = s.cliques;
    broken = s.broken;
    nodeSeq = s.nodeSeq;
    resourceSeq = s.resourceSeq;
  }

  private static final String[] EVENTS = {"NODE_ADD", "CONFIG_REMOVE", "CONFIG_RESTORE",
      "NODE_OFFLINE", "NODE_ONLINE", "EVACUATE", "UNKNOWN", "DISABLE", "ENABLE", "CAPACITY",
      "WEIGHT", "PARTITION_WEIGHT", "RESOURCE_ADD", "RESOURCE_DELETE", "RETAG", "DELAY_TOGGLE",
      "NOOP", "FAILOVER", "BREAK", "REPAIR", "RETAG_OUT_OF_BROKEN"};
  private static final int[] GENERAL_WEIGHTS =
      {8, 4, 4, 8, 8, 5, 3, 5, 8, 6, 6, 3, 5, 3, 4, 4, 5, 2, 1, 4, 0};
  private static final int[] FEASIBLE_WEIGHTS =
      {8, 4, 4, 8, 8, 5, 3, 5, 8, 6, 6, 3, 5, 3, 4, 4, 5, 2, 0, 0, 0};
  private static final int[] BREAK_WEIGHTS =
      {6, 3, 3, 6, 6, 4, 2, 4, 6, 4, 4, 2, 4, 2, 4, 3, 5, 2, 12, 9, 0};
  private static final int[] BREAK_COLLIDE_WEIGHTS =
      {6, 3, 3, 6, 6, 4, 2, 4, 6, 4, 4, 2, 4, 2, 4, 3, 5, 2, 12, 9, 4};

  /**
   * Applies one random event that keeps the invariants of the mode and returns its label. With
   * flagForms set, the step may instead be a FLAG_FORM step, which converges and changes nothing
   * else.
   */
  public String nextEvent() {
    if (flagForms && forms.nextInt(5) == 0) {
      failoverRequested = false;
      converge = true;
      return apply("FLAG_FORM");
    }
    int[] weights = mode == Mode.GENERAL ? GENERAL_WEIGHTS
        : mode == Mode.FEASIBLE ? FEASIBLE_WEIGHTS
            : collide ? BREAK_COLLIDE_WEIGHTS : BREAK_WEIGHTS;
    int total = 0;
    for (int w : weights) {
      total += w;
    }
    for (int attempt = 0; attempt < 40; attempt++) {
      int roll = rnd.nextInt(total);
      int index = 0;
      while (roll >= weights[index]) {
        roll -= weights[index];
        index++;
      }
      State before = save();
      failoverRequested = false;
      String label = apply(EVENTS[index]);
      if (label != null && acceptable()) {
        converge = rnd.nextInt(100) < 85;
        return withOutage(followOutages(label));
      }
      restore(before);
    }
    failoverRequested = false;
    converge = true;
    return "NOOP";
  }

  /** Whether the mode accepts the current state as the outcome of an event. */
  private boolean acceptable() {
    return (mode == Mode.GENERAL || allRobust()) && (extraGuard == null || extraGuard.test(this));
  }

  /**
   * Walks a followed mild break through outages, so the scopes meet a broken clique they can still
   * place. The clique gets one fresh node, then each step alternates between taking a healthy
   * member offline past its window, which hands only that member's replicas to the emergency
   * scope, and bringing it back, which leaves the partial scope a baseline that differs from the
   * current placement. The victim of such a break is the clique's smallest resource, so the other
   * resources keep members free of it. Only aux is drawn, and an outcome the mode would not accept
   * is undone.
   */
  private String followOutages(String label) {
    for (Broken b : broken.values()) {
      if (!b.followed || resourcesWithTag(b.clique).size() < 2) {
        continue;
      }
      State before = save();
      SimNode downed = b.downed == null ? null : nodes.get(b.downed);
      String applied;
      if (downed != null && !downed.live && downed.config != null) {
        downed.live = true;
        downed.session++;
        downed.offlineTime = -1L;
        b.downed = null;
        applied = " with NODE_ONLINE " + downed.name;
      } else {
        List<SimNode> members = nodesWithTag(b.clique);
        List<SimNode> candidates = filter(members, SimNode::healthy);
        if (candidates.isEmpty()) {
          return label;
        }
        applied = "";
        if (!b.grown) {
          long sum = 0;
          for (SimNode n : members) {
            sum += n.capacity();
          }
          SimNode fresh = addNode(b.clique, (int) Math.max(10, sum / members.size()));
          b.grown = true;
          applied = " with NODE_ADD " + fresh.name + " " + b.clique;
        }
        SimNode node = candidates.get(aux.nextInt(candidates.size()));
        node.live = false;
        node.offlineTime = System.currentTimeMillis() - OUT_OF_WINDOW_AGO;
        b.downed = node.name;
        applied += " with NODE_OFFLINE " + node.name + " expired";
      }
      if (!acceptable()) {
        restore(before);
        return label;
      }
      return label + applied;
    }
    return label;
  }

  /**
   * With an aux draw, and only while no node of a clique that is not broken is offline inside its
   * window, also takes a healthy node of such a clique offline inside its window in the same step
   * as the event, and through alongside maybe a second node out of service with it. Undone when
   * the mode would not accept the outcome.
   */
  private String withOutage(String label) {
    long now = System.currentTimeMillis();
    if (aux.nextInt(3) != 0) {
      return label;
    }
    java.util.function.Predicate<SimNode> unbroken =
        n -> n.tags().stream().noneMatch(broken::containsKey);
    for (SimNode n : nodes.values()) {
      if (!n.live && n.offlineTime > now - DELAY_MS && unbroken.test(n)) {
        return label;
      }
    }
    List<SimNode> candidates = filter(new ArrayList<>(nodes.values()), n -> n.healthy()
        && unbroken.test(n)
        && n.tags().stream().anyMatch(tag -> !resourcesWithTag(tag).isEmpty()));
    if (candidates.isEmpty()) {
      return label;
    }
    SimNode node = candidates.get(aux.nextInt(candidates.size()));
    node.live = false;
    node.offlineTime = now - IN_WINDOW_AGO;
    if (!acceptable()) {
      node.live = true;
      node.offlineTime = -1L;
      return label;
    }
    return alongside(label + " with NODE_OFFLINE " + node.name + " in-window", node, now);
  }

  /**
   * Whether a node taken out of service stays inside its delay window. The rnd draw is still taken
   * so every later rnd draw stays where it is, and the aux draw puts most outages inside the
   * window, which is what the delayed rebalance overwrite needs to run at all.
   */
  private boolean drawInWindow() {
    boolean drawn = rnd.nextBoolean();
    return aux.nextInt(4) != 0 || drawn;
  }

  private static final String[] ALONGSIDE = {"DISABLE", "EVACUATE", "UNKNOWN"};

  /**
   * With an aux draw, takes a second healthy node out of service in the same step as a node that
   * went offline inside its window: disabled inside the window too, evacuating, or unknown. Undone
   * when the mode would not accept the outcome.
   */
  private String alongside(String label, SimNode offline, long now) {
    if (aux.nextInt(3) != 0) {
      return label;
    }
    List<SimNode> candidates =
        filter(new ArrayList<>(nodes.values()), n -> n != offline && n.healthy());
    if (candidates.isEmpty()) {
      return label;
    }
    SimNode other = candidates.get(aux.nextInt(candidates.size()));
    String kind = ALONGSIDE[aux.nextInt(ALONGSIDE.length)];
    InstanceConfig saved = new InstanceConfig(deepCopy(other.config.getRecord()));
    other.config.setInstanceOperation(InstanceConstants.InstanceOperation.valueOf(kind));
    boolean disable = "DISABLE".equals(kind);
    if (disable) {
      other.config.getRecord().setLongField(
          InstanceConfig.InstanceConfigProperty.HELIX_ENABLED_TIMESTAMP.name(),
          now - IN_WINDOW_AGO);
    }
    if (!acceptable()) {
      other.config = saved;
      return label;
    }
    return label + " with " + kind + " " + other.name + (disable ? " in-window" : "");
  }

  /** Applies a specific event kind, or returns null when it does not apply right now. */
  public String apply(String kind) {
    List<SimNode> all = new ArrayList<>(nodes.values());
    long now = System.currentTimeMillis();
    switch (kind) {
      case "NODE_ADD": {
        boolean spare = rnd.nextInt(10) == 0;
        String tag = spare ? null : pick(cliques);
        int base = 100;
        if (tag != null) {
          List<SimNode> members = nodesWithTag(tag);
          if (!members.isEmpty()) {
            long sum = 0;
            for (SimNode n : members) {
              sum += n.capacity();
            }
            base = (int) Math.max(10, sum / members.size());
          }
        }
        SimNode node = addNode(tag, jitter(base));
        if (tag != null && broken.containsKey(tag) && !broken.get(tag).byWeight) {
          // A node joining a clique broken by capacity arrives with the same broken capacity.
          Broken b = broken.get(tag);
          b.savedCapacities.put(node.name, node.capacity());
          node.config.setInstanceCapacityMap(
              Collections.singletonMap(DISK, softBreaks ? node.capacity() * 3 / 2 : 0));
        }
        return kind + " " + node.name + " " + tag;
      }
      case "CONFIG_REMOVE": {
        List<SimNode> candidates = filter(all, n -> n.config != null);
        if (candidates.isEmpty()) {
          return null;
        }
        SimNode node = pick(candidates);
        node.parkedConfig = node.config;
        node.config = null;
        boolean stayLive = rnd.nextBoolean();
        if (!stayLive && node.live) {
          node.live = false;
          node.offlineTime = now - OUT_OF_WINDOW_AGO;
        }
        return kind + " " + node.name + (stayLive ? " live" : " offline");
      }
      case "CONFIG_RESTORE": {
        List<SimNode> candidates = filter(all, n -> n.config == null && n.parkedConfig != null);
        if (candidates.isEmpty()) {
          return null;
        }
        SimNode node = pick(candidates);
        node.config = node.parkedConfig;
        node.parkedConfig = null;
        if (!node.live) {
          node.live = true;
          node.session++;
          node.offlineTime = -1L;
        }
        return kind + " " + node.name;
      }
      case "NODE_OFFLINE": {
        List<SimNode> candidates = filter(all, n -> n.live);
        if (candidates.isEmpty()) {
          return null;
        }
        SimNode node = pick(candidates);
        boolean inWindow = drawInWindow();
        node.live = false;
        node.offlineTime = now - (inWindow ? IN_WINDOW_AGO : OUT_OF_WINDOW_AGO);
        String label = kind + " " + node.name + (inWindow ? " in-window" : " expired");
        return inWindow ? alongside(label, node, now) : label;
      }
      case "NODE_ONLINE": {
        List<SimNode> candidates = filter(all, n -> !n.live && n.config != null);
        if (candidates.isEmpty()) {
          return null;
        }
        SimNode node = pick(candidates);
        node.live = true;
        node.session++;
        node.offlineTime = -1L;
        return kind + " " + node.name;
      }
      case "EVACUATE":
      case "UNKNOWN":
      case "DISABLE": {
        List<SimNode> candidates = filter(all,
            n -> n.config != null && n.operation() == InstanceConstants.InstanceOperation.ENABLE);
        if (candidates.isEmpty()) {
          return null;
        }
        SimNode node = pick(candidates);
        node.config.setInstanceOperation(InstanceConstants.InstanceOperation.valueOf(kind));
        String suffix = "";
        if ("DISABLE".equals(kind)) {
          boolean inWindow = drawInWindow();
          node.config.getRecord().setLongField(
              InstanceConfig.InstanceConfigProperty.HELIX_ENABLED_TIMESTAMP.name(),
              now - (inWindow ? IN_WINDOW_AGO : OUT_OF_WINDOW_AGO));
          suffix = inWindow ? " in-window" : " expired";
        }
        return kind + " " + node.name + suffix;
      }
      case "ENABLE": {
        List<SimNode> candidates = filter(all,
            n -> n.config != null && n.operation() != InstanceConstants.InstanceOperation.ENABLE);
        if (candidates.isEmpty()) {
          return null;
        }
        SimNode node = pick(candidates);
        node.config.setInstanceOperation(InstanceConstants.InstanceOperation.ENABLE);
        return kind + " " + node.name;
      }
      case "CAPACITY": {
        List<SimNode> candidates = filter(all, n -> n.config != null);
        if (candidates.isEmpty()) {
          return null;
        }
        SimNode node = pick(candidates);
        double factor = mode == Mode.BREAK ? 1.0 + 0.5 * rnd.nextDouble()
            : 0.75 + 0.6 * rnd.nextDouble();
        if (isInBrokenCapacityClique(node)) {
          return null;
        }
        int capacity = Math.max(5, (int) Math.round(node.capacity() * factor));
        node.config.setInstanceCapacityMap(Collections.singletonMap(DISK, capacity));
        return kind + " " + node.name + " " + capacity;
      }
      case "WEIGHT": {
        List<SimResource> candidates = unbrokenResources();
        if (candidates.isEmpty()) {
          return null;
        }
        SimResource r = pick(candidates);
        int weight = mode == Mode.BREAK ? Math.max(1, r.weight * (1 + rnd.nextInt(3)) / 4)
            : Math.max(1, (int) Math.round(r.weight * (0.75 + 0.6 * rnd.nextDouble())));
        r.weight = weight;
        return kind + " " + r.name + " " + weight;
      }
      case "PARTITION_WEIGHT": {
        List<SimResource> candidates = unbrokenResources();
        if (candidates.isEmpty()) {
          return null;
        }
        SimResource r = pick(candidates);
        String partition = pick(r.partitionNames());
        if (r.partitionWeights.containsKey(partition) && rnd.nextBoolean()) {
          int old = r.partitionWeights.remove(partition);
          if (mode == Mode.BREAK && r.weight > old) {
            r.partitionWeights.put(partition, old);
            return null;
          }
          return kind + " " + partition + " default";
        }
        int current = r.weightOf(partition);
        int weight = mode == Mode.BREAK ? Math.max(1, current / 2)
            : Math.max(1, (int) Math.round(current * (0.75 + 0.6 * rnd.nextDouble())));
        r.partitionWeights.put(partition, weight);
        return kind + " " + partition + " " + weight;
      }
      case "RESOURCE_ADD": {
        String tag = !pureCliques && rnd.nextInt(6) == 0 ? null : pick(cliques);
        if (tag != null && broken.containsKey(tag)) {
          return null;
        }
        SimResource r = addResource(tag);
        return kind + " " + r.name + " " + tag;
      }
      case "RESOURCE_DELETE": {
        List<SimResource> candidates = unbrokenResources();
        if (candidates.size() <= 1) {
          return null;
        }
        SimResource r = pick(candidates);
        resources.remove(r.name);
        return kind + " " + r.name;
      }
      case "RETAG": {
        List<SimNode> candidates = filter(all, n -> n.config != null && !cliqueTagsOf(n).isEmpty());
        if (candidates.isEmpty() || cliques.size() < 2) {
          return null;
        }
        SimNode node = pick(candidates);
        List<String> own = cliqueTagsOf(node);
        String from = pick(own);
        String to = pick(cliques);
        if (own.contains(to) || isInBrokenCapacityClique(node) || brokenCapacity(to)) {
          return null;
        }
        node.config.removeTag(from);
        node.config.addTag(to);
        return kind + " " + node.name + " " + from + "->" + to;
      }
      case "DELAY_TOGGLE": {
        boolean enabled = !clusterConfig.isDelayRebalaceEnabled();
        clusterConfig.setDelayRebalaceEnabled(enabled);
        return kind + " " + enabled;
      }
      case "NOOP":
        return kind;
      case "FLAG_FORM": {
        // Moves the isolation flag field to another form that reads as off.
        String current = clusterConfig.getRecord().getSimpleField(ISOLATION_FLAG_FIELD);
        List<String> others = new ArrayList<>(FLAG_OFF_FORMS);
        others.remove(current);
        String form = others.get(forms.nextInt(others.size()));
        if (form == null) {
          clusterConfig.getRecord().getSimpleFields().remove(ISOLATION_FLAG_FIELD);
        } else {
          clusterConfig.getRecord().setSimpleField(ISOLATION_FLAG_FIELD, form);
        }
        return kind + " " + (form == null ? "absent" : "\"" + form + "\"");
      }
      case "FAILOVER":
        failoverRequested = true;
        return kind;
      case "BREAK":
        return breakClique();
      case "REPAIR":
        return repairClique();
      case "RETAG_OUT_OF_BROKEN":
        return retagOutOfBroken();
      default:
        throw new IllegalArgumentException(kind);
    }
  }

  /**
   * Moves a node out of a broken clique into a healthy one, so the broken clique's carried
   * assignment still names a node the healthy clique may now be given.
   */
  private String retagOutOfBroken() {
    if (broken.isEmpty()) {
      return null;
    }
    String from = pick(new ArrayList<>(broken.keySet()));
    List<SimNode> members =
        filter(nodesWithTag(from), n -> n.config != null && cliqueTagsOf(n).size() == 1);
    List<String> healthy = filter(cliques, t -> !broken.containsKey(t));
    if (members.size() < 2 || healthy.isEmpty()) {
      return null;
    }
    SimNode node = pick(members);
    String to = pick(healthy);
    Broken b = broken.get(from);
    Integer saved = b.savedCapacities.remove(node.name);
    if (saved != null) {
      node.config.setInstanceCapacityMap(Collections.singletonMap(DISK, saved));
    }
    node.config.removeTag(from);
    node.config.addTag(to);
    // A resource new to the receiving clique has no earlier placement to stay on, so it is the
    // likeliest to be given the node the broken clique's carried assignment still names.
    String added = "";
    if (rnd.nextBoolean()) {
      added = " +" + addResource(to).name;
    }
    return "RETAG_OUT_OF_BROKEN " + node.name + " " + from + "->" + to + added;
  }

  private boolean brokenCapacity(String tag) {
    Broken b = broken.get(tag);
    return b != null && !b.byWeight;
  }

  private boolean isInBrokenCapacityClique(SimNode node) {
    for (String tag : node.tags()) {
      if (brokenCapacity(tag)) {
        return true;
      }
    }
    return false;
  }

  public List<String> cliqueTagsOf(SimNode node) {
    List<String> result = new ArrayList<>();
    for (String tag : node.tags()) {
      if (cliques.contains(tag)) {
        result.add(tag);
      }
    }
    return result;
  }

  private List<SimResource> unbrokenResources() {
    List<SimResource> result = new ArrayList<>();
    for (SimResource r : resources.values()) {
      if (r.tag == null || !broken.containsKey(r.tag)) {
        result.add(r);
      }
    }
    return result;
  }

  private static <T> List<T> filter(List<T> list, java.util.function.Predicate<T> predicate) {
    List<T> result = new ArrayList<>();
    for (T t : list) {
      if (predicate.test(t)) {
        result.add(t);
      }
    }
    return result;
  }

  private String breakClique() {
    List<String> candidates = new ArrayList<>();
    for (String tag : cliques) {
      if (!broken.containsKey(tag) && !resourcesWithTag(tag).isEmpty()) {
        candidates.add(tag);
      }
    }
    if (candidates.isEmpty()) {
      return null;
    }
    if (!allowBreakAll && candidates.size() <= 1) {
      return null;
    }
    String tag = pick(candidates);
    boolean byWeight = rnd.nextBoolean();
    Broken b = new Broken(tag, byWeight);
    long capacity = 0;
    int maxCapacity = 0;
    List<SimNode> members = nodesWithTag(tag);
    for (SimNode n : members) {
      capacity += n.capacity();
      maxCapacity = Math.max(maxCapacity, n.capacity());
    }
    String detail;
    if (byWeight) {
      // Mild keeps the cluster wide sum positive so placement fails; wild trips the precheck. The
      // aux draw makes wild common, so the precheck also meets nodes inside their delay window.
      // A scenario that does not collide makes wild common only when wildBreaks says so, and
      // follows every mild break of a clique with more than one resource through outages, with
      // the smallest resource as the victim, see followOutages. A colliding scenario keeps its
      // breaks as they are, since a collision yield needs a broken clique the scopes carry.
      boolean drawn = rnd.nextInt(3) == 0;
      if (!collide && wildBreaks == null) {
        wildBreaks = aux.nextBoolean();
      }
      boolean wild = collide ? aux.nextInt(2) == 0 || drawn
          : drawn || wildBreaks && aux.nextInt(2) == 0;
      List<SimResource> owned = resourcesWithTag(tag);
      SimResource victim = pick(owned);
      b.followed = !collide && !wild && !softBreaks && owned.size() >= 2;
      if (b.followed) {
        for (SimResource r : owned) {
          if ((long) r.partitions * r.replicas < (long) victim.partitions * victim.replicas) {
            victim = r;
          }
        }
      }
      b.savedResources.put(victim.name, victim.copy());
      long others = 0;
      for (SimResource r : owned) {
        if (r != victim) {
          others += r.demand();
        }
      }
      long target = wild ? capacity * 40L : capacity * 2L + maxCapacity;
      long perReplica =
          Math.max(1, (target - others) / ((long) victim.partitions * victim.replicas));
      victim.weight = (int) Math.min(Integer.MAX_VALUE / 4, Math.max(perReplica, maxCapacity + 1L));
      victim.partitionWeights.clear();
      if (softBreaks) {
        SimResource saved = b.savedResources.get(victim.name);
        victim.weight = Math.max(1, saved.weight / 2);
        victim.partitionWeights.putAll(saved.partitionWeights);
        victim.partitionWeights.replaceAll((partition, weight) -> Math.max(1, weight / 2));
      }
      detail = victim.name + " weight=" + victim.weight
          + (softBreaks ? " soft" : wild ? " wild" : " mild");
    } else {
      for (SimNode n : members) {
        b.savedCapacities.put(n.name, n.capacity());
        n.config.setInstanceCapacityMap(
            Collections.singletonMap(DISK, softBreaks ? n.capacity() * 3 / 2 : 0));
      }
      detail = (softBreaks ? "capacity raised by half" : "capacity=0") + " on " + members.size()
          + " node(s)";
    }
    broken.put(tag, b);
    return "BREAK " + tag + " " + (byWeight ? "weight " : "capacity ") + detail;
  }

  private String repairClique() {
    if (broken.isEmpty()) {
      return null;
    }
    String tag = pick(new ArrayList<>(broken.keySet()));
    repairTag(tag);
    return "REPAIR " + tag;
  }

  /** Puts back exactly what breaking this clique changed. */
  public void repairTag(String tag) {
    Broken b = broken.remove(tag);
    for (Map.Entry<String, SimResource> e : b.savedResources.entrySet()) {
      if (resources.containsKey(e.getKey())) {
        resources.put(e.getKey(), e.getValue().copy());
      }
    }
    for (Map.Entry<String, Integer> e : b.savedCapacities.entrySet()) {
      SimNode n = nodes.get(e.getKey());
      if (n != null) {
        InstanceConfig target = n.config != null ? n.config : n.parkedConfig;
        if (target != null) {
          target.setInstanceCapacityMap(Collections.singletonMap(DISK, e.getValue()));
        }
      }
    }
  }

  /**
   * The same cluster as it would be had nothing ever been broken: a deep copy with every broken
   * clique's saved weights and capacities put back. Events never touch what a break changed, so
   * this is exactly the never broken twin's state after the same event sequence minus the breaks
   * and repairs.
   */
  public WagedFuzzSim twinView() {
    WagedFuzzSim twin = copy();
    while (!twin.broken.isEmpty()) {
      String tag = twin.broken.firstKey();
      twin.repairTag(tag);
    }
    return twin;
  }

  /** A deep copy of the cluster as it is now, broken cliques included. */
  public WagedFuzzSim copy() {
    WagedFuzzSim copy = new WagedFuzzSim(seed, mode);
    copy.restore(save());
    copy.pureCliques = pureCliques;
    copy.poolLabel = poolLabel;
    copy.failoverRequested = failoverRequested;
    copy.converge = converge;
    return copy;
  }

  /** Breaks one specific clique; used by deterministic scenario tests. */
  public String breakSpecific(String tag, boolean byWeight) {
    State before = save();
    for (int i = 0; i < 64; i++) {
      String label = breakClique();
      if (label != null && label.startsWith("BREAK " + tag + " ")
          && broken.get(tag).byWeight == byWeight) {
        return label;
      }
      restore(before);
      before = save();
    }
    throw new IllegalStateException("could not break " + tag);
  }

  // ---------------------------------------------------------------------------------------------
  // Rendering the simulated cluster for the rebalancer
  // ---------------------------------------------------------------------------------------------

  public Map<String, Resource> resourceMap() {
    Map<String, Resource> map = new LinkedHashMap<>();
    for (SimResource r : resources.values()) {
      Resource resource = new Resource(r.name);
      resource.setStateModelDefRef(r.stateModel);
      for (String p : r.partitionNames()) {
        resource.addPartition(p);
      }
      map.put(r.name, resource);
    }
    return map;
  }

  public ClusterConfig clusterConfigCopy() {
    return new ClusterConfig(deepCopy(clusterConfig.getRecord()));
  }

  static Map<String, StateModelDefinition> stateModelDefs() {
    Map<String, StateModelDefinition> defs = new HashMap<>();
    defs.put("MasterSlave",
        BuiltInStateModelDefinitions.MasterSlave.getStateModelDefinition());
    defs.put("LeaderStandby",
        BuiltInStateModelDefinitions.LeaderStandby.getStateModelDefinition());
    defs.put("OnlineOffline",
        BuiltInStateModelDefinitions.OnlineOffline.getStateModelDefinition());
    return defs;
  }

  /**
   * Loads this cluster into a data provider the way a Helix controller reads it from ZooKeeper,
   * with the cluster config passed through the customizer first when there is one.
   */
  void load(SimDataProvider provider, Consumer<ClusterConfig> configCustomizer) {
    ClusterConfig config = clusterConfigCopy();
    if (configCustomizer != null) {
      configCustomizer.accept(config);
    }
    provider.setClusterConfig(config);
    Map<String, InstanceConfig> instanceConfigs = new TreeMap<>();
    List<LiveInstance> liveInstances = new ArrayList<>();
    Map<String, Long> offline = new TreeMap<>();
    for (SimNode n : nodes.values()) {
      if (n.config != null) {
        instanceConfigs.put(n.name, new InstanceConfig(deepCopy(n.config.getRecord())));
      }
      if (n.live) {
        LiveInstance li = new LiveInstance(n.name);
        li.setSessionId(n.name + "-session-" + n.session);
        li.setHelixVersion("1.0");
        liveInstances.add(li);
      } else if (n.offlineTime > 0) {
        offline.put(n.name, n.offlineTime);
      }
    }
    provider.setInstanceConfigMap(instanceConfigs);
    provider.setLiveInstances(liveInstances);
    provider._offlineTimes = offline;
    List<IdealState> idealStates = new ArrayList<>();
    Map<String, ResourceConfig> resourceConfigs = new TreeMap<>();
    for (SimResource r : resources.values()) {
      idealStates.add(r.idealState());
      resourceConfigs.put(r.name, r.resourceConfig());
    }
    provider.setIdealStates(idealStates);
    provider.setResourceConfigMap(resourceConfigs);
  }

  /** A fresh data provider holding this cluster, its cluster config as the customizer sets it. */
  public SimDataProvider dataProvider(Consumer<ClusterConfig> configCustomizer) {
    SimDataProvider provider = new SimDataProvider(clusterName);
    provider.setStateModelDefMap(stateModelDefs());
    load(provider, configCustomizer);
    return provider;
  }

  /** A real data provider whose inputs come from the simulator instead of ZooKeeper. */
  public static final class SimDataProvider extends ResourceControllerDataProvider {
    Map<String, Long> _offlineTimes = Collections.emptyMap();

    SimDataProvider(String clusterName) {
      super(clusterName);
    }

    @Override
    public Set<HelixConstants.ChangeType> getRefreshedChangeTypes() {
      return EnumSet.allOf(HelixConstants.ChangeType.class);
    }

    @Override
    public Map<String, Long> getInstanceOfflineTimeMap() {
      return _offlineTimes;
    }
  }

  /**
   * Stands in for the ZooKeeper bucket accessor, serializing like the real store does, and counts
   * the writes and deletes of every path.
   */
  public static final class InMemoryBucketAccessor implements BucketDataAccessor {
    private static final ZNRecordJacksonSerializer SERIALIZER = new ZNRecordJacksonSerializer();
    final Map<String, byte[]> _data = new ConcurrentHashMap<>();
    /** Writes per path since the accessor was created. */
    final Map<String, Integer> _writes = new ConcurrentHashMap<>();
    /** Deletes per path since the accessor was created. */
    final Map<String, Integer> _deletes = new ConcurrentHashMap<>();
    /** Every path read, written or deleted so far. */
    final Set<String> _paths = ConcurrentHashMap.newKeySet();

    @Override
    public <T extends HelixProperty> boolean compressedBucketWrite(String path, T value) {
      _paths.add(path);
      _data.put(path, SERIALIZER.serialize(value.getRecord()));
      _writes.merge(path, 1, Integer::sum);
      return true;
    }

    @Override
    public <T extends HelixProperty> HelixProperty compressedBucketRead(String path,
        Class<T> helixPropertySubType) {
      _paths.add(path);
      byte[] bytes = _data.get(path);
      if (bytes == null) {
        throw new ZkNoNodeException("No assignment metadata at " + path);
      }
      return new HelixProperty((ZNRecord) SERIALIZER.deserialize(bytes));
    }

    @Override
    public void compressedBucketDelete(String path) {
      _paths.add(path);
      _data.remove(path);
      _deletes.merge(path, 1, Integer::sum);
    }

    @Override
    public void disconnect() {
    }
  }

  /** The real metadata store over the in-memory accessor. */
  public static final class SimStore extends AssignmentMetadataStore {
    /** The last map the partial rebalance handed to the in memory cache, if any. */
    volatile Map<String, ResourceAssignment> lastCacheUpdate;
    /** Whether each update of the in memory cache was accepted or found stale, in order. */
    final List<String> cacheUpdates = Collections.synchronizedList(new ArrayList<>());

    SimStore(BucketDataAccessor accessor, String clusterName) {
      super(accessor, clusterName);
    }

    @Override
    public synchronized boolean asyncUpdateBestPossibleAssignmentCache(
        Map<String, ResourceAssignment> bestPossibleAssignment, int newVersion) {
      lastCacheUpdate = copyAssignments(bestPossibleAssignment);
      boolean accepted =
          super.asyncUpdateBestPossibleAssignmentCache(bestPossibleAssignment, newVersion);
      cacheUpdates.add(accepted ? "accepted" : "stale");
      return accepted;
    }
  }

  /**
   * Stands in for a runner's calculation executor and runs each calculation passed to
   * {@link #submit(Callable)} on the original one. For each of them it records how the calculation
   * ended: ok, failed with the failure the runner kept as its last one, or threw.
   */
  static final class RecordingExecutor extends AbstractExecutorService {
    final ExecutorService delegate;
    private final AtomicReference<HelixRebalanceException> _lastFailure;
    final List<Map<String, Object>> outcomes = Collections.synchronizedList(new ArrayList<>());

    RecordingExecutor(ExecutorService delegate,
        AtomicReference<HelixRebalanceException> lastFailure) {
      this.delegate = delegate;
      _lastFailure = lastFailure;
    }

    @Override
    public <T> Future<T> submit(Callable<T> task) {
      return delegate.submit(() -> {
        T value;
        try {
          value = task.call();
        } catch (Exception | Error t) {
          outcomes.add(outcome("threw", t));
          throw t;
        }
        // A runner's calculation returns false exactly when it caught a rebalance failure and
        // kept it as the last one.
        outcomes.add(Boolean.FALSE.equals(value) ? outcome("failed", _lastFailure.get())
            : outcome("ok", null));
        return value;
      });
    }

    private static Map<String, Object> outcome(String result, Throwable failure) {
      Map<String, Object> view = new TreeMap<>();
      view.put("result", result);
      view.put("failure", describe(failure));
      return view;
    }

    /**
     * Only {@link #submit(Callable)} records an outcome. A runner task that arrived here would
     * leave both modes with empty recordings that compare equal, so it is refused.
     */
    @Override
    public void execute(Runnable command) {
      throw new UnsupportedOperationException("only submit(Callable) records an outcome");
    }

    @Override
    public void shutdown() {
      delegate.shutdown();
    }

    @Override
    public List<Runnable> shutdownNow() {
      return delegate.shutdownNow();
    }

    @Override
    public boolean isShutdown() {
      return delegate.isShutdown();
    }

    @Override
    public boolean isTerminated() {
      return delegate.isTerminated();
    }

    @Override
    public boolean awaitTermination(long timeout, TimeUnit unit) throws InterruptedException {
      return delegate.awaitTermination(timeout, unit);
    }
  }

  /** The outcome of one computeNewIdealStates call. */
  public static final class SubStep {
    public final String name;
    public Map<String, IdealState> idealStates;
    public Throwable failure;
    public List<FuzzRecordingAlgorithm.Outcome> outcomes = Collections.emptyList();
    public Map<String, ResourceAssignment> baselineBefore;
    public Map<String, ResourceAssignment> bestPossibleBefore;
    public Map<String, ResourceAssignment> baselineAfter;
    public Map<String, ResourceAssignment> bestPossibleAfter;
    /** What the partial rebalance of this call handed to the in memory cache, if anything. */
    public Map<String, ResourceAssignment> partialCacheUpdate;
    /**
     * Writes this call made, per path: every path the assignment store has touched, zero
     * included, and any other path written.
     */
    public Map<String, Integer> writes = Collections.emptyMap();
    /** Deletes this call made, per path deleted. */
    public Map<String, Integer> deletes = Collections.emptyMap();
    /**
     * How each baseline calculation this call submitted ended, in order: "result" is ok, failed
     * or threw, and "failure" describes the failure, if any.
     */
    public List<Map<String, Object>> baselineTasks = Collections.emptyList();
    /** How each partial rebalance calculation this call submitted ended, in the same form. */
    public List<Map<String, Object>> partialTasks = Collections.emptyList();
    /** Whether each update of the in memory cache this call made was accepted or stale. */
    public List<String> cacheUpdates = Collections.emptyList();
    /**
     * What this call served: the best possible assignment with any delayed rebalance overwrite
     * merged in. The overwrite is never persisted, so this is the only place it shows. Null when
     * the call did not get that far.
     */
    public Map<String, ResourceAssignment> served;
    public Map<String, Map<String, Map<String, String>>> currentStatesBefore;

    SubStep(String name) {
      this.name = name;
    }
  }

  /** Everything observable about one simulated step on one driver. */
  public static final class StepResult {
    public final int step;
    public final String event;
    public final List<SubStep> subSteps = new ArrayList<>();
    public String json;
    public String sha;

    StepResult(int step, String event) {
      this.step = step;
      this.event = event;
    }
  }

  /**
   * One Helix controller: a real WagedRebalancer with its own store, data cache and simulated
   * participants. Several drivers can follow one simulator in lockstep.
   */
  public static final class Driver implements AutoCloseable {
    public final String label;
    public final boolean asyncBaseline;
    public final boolean asyncPartial;
    final Consumer<ClusterConfig> _configCustomizer;
    public final InMemoryBucketAccessor accessor = new InMemoryBucketAccessor();
    public final SimStore store;
    public final FuzzRecordingAlgorithm algorithm = FuzzRecordingAlgorithm.create();
    public final WagedRebalancer rebalancer;
    public final ClusterStatusMonitor monitor;
    final SimDataProvider _dataProvider;
    final ExecutorService _asyncPool = Executors.newSingleThreadExecutor();
    /** Records how each baseline calculation ended. The gate and drain tasks bypass it. */
    final RecordingExecutor _baselineTasks;
    /** Records how each partial rebalance calculation ended. The drain tasks bypass it. */
    final RecordingExecutor _partialTasks;
    /** resource, partition, instance, state. */
    public final Map<String, Map<String, Map<String, String>>> currentStates = new TreeMap<>();
    public boolean keepFullJson;

    public Driver(String clusterName, String label, boolean asyncBaseline,
        Consumer<ClusterConfig> configCustomizer) {
      this(clusterName, label, asyncBaseline, false, configCustomizer);
    }

    public Driver(String clusterName, String label, boolean asyncBaseline, boolean asyncPartial,
        Consumer<ClusterConfig> configCustomizer) {
      this.label = label;
      this.asyncBaseline = asyncBaseline;
      this.asyncPartial = asyncPartial;
      _configCustomizer = configCustomizer;
      store = new SimStore(accessor, clusterName);
      rebalancer = new WagedRebalancer(store, algorithm, Optional.empty()) {
        @Override
        protected Map<String, ResourceAssignment> emergencyRebalance(
            ResourceControllerDataProvider clusterData, Map<String, Resource> resourceMap,
            Set<String> activeNodes, CurrentStateOutput currentStateOutput,
            RebalanceAlgorithm rebalanceAlgorithm) throws HelixRebalanceException {
          Map<String, ResourceAssignment> served = super.emergencyRebalance(clusterData,
              resourceMap, activeNodes, currentStateOutput, rebalanceAlgorithm);
          _served = copyAssignments(served);
          return served;
        }
      };
      monitor = new ClusterStatusMonitor(clusterName);
      rebalancer.setClusterStatusMonitor(monitor);
      rebalancer.setGlobalRebalanceAsyncMode(asyncBaseline);
      rebalancer.setPartialRebalanceAsyncMode(asyncPartial);
      _baselineTasks = recordTasks("_globalRebalanceRunner", "_baselineCalculateExecutor");
      _partialTasks = recordTasks("_partialRebalanceRunner", "_bestPossibleCalculateExecutor");
      _dataProvider = new SimDataProvider(clusterName);
      _dataProvider.setAsyncTasksThreadPool(_asyncPool);
      _dataProvider.setStateModelDefMap(stateModelDefs());
      algorithm.previousSource = this::previousFor;
      algorithm.storeBestPossible = store::getBestPossibleAssignment;
    }

    private volatile CurrentStateOutput _currentStateOutput;
    private volatile Set<String> _resources = Collections.emptySet();
    private volatile Map<String, ResourceAssignment> _served;

    /**
     * The previous assignment a scope hands to the carry forward: the stored map for the resources
     * being rebalanced, completed from the current states, as the assignment manager reads it.
     */
    Map<String, ResourceAssignment> previousFor(String scope) {
      Map<String, ResourceAssignment> stored;
      if ("GLOBAL_BASELINE".equals(scope)) {
        stored = store.getBaseline();
      } else if ("DELAYED_REBALANCE_OVERWRITES".equals(scope)) {
        return new TreeMap<>();
      } else {
        stored = store.getBestPossibleAssignment();
      }
      Map<String, ResourceAssignment> previous = copyAssignments(stored);
      previous.keySet().retainAll(_resources);
      Set<String> missing = new TreeSet<>(_resources);
      missing.removeAll(previous.keySet());
      if (_currentStateOutput != null) {
        previous.putAll(copyAssignments(_currentStateOutput.getAssignment(missing)));
      }
      return previous;
    }

    public SimDataProvider dataProvider() {
      return _dataProvider;
    }

    void populate(WagedFuzzSim sim) {
      sim.load(_dataProvider, _configCustomizer);
      // Drop the current states of deleted resources and of instances that are not live.
      currentStates.keySet().retainAll(sim.resources.keySet());
      Set<String> live = new TreeSet<>();
      for (SimNode n : sim.nodes.values()) {
        if (n.live) {
          live.add(n.name);
        }
      }
      for (Map<String, Map<String, String>> partitions : currentStates.values()) {
        for (Map<String, String> states : partitions.values()) {
          states.keySet().retainAll(live);
        }
        partitions.values().removeIf(Map::isEmpty);
      }
      currentStates.values().removeIf(Map::isEmpty);
    }

    CurrentStateOutput currentStateOutput(WagedFuzzSim sim) {
      CurrentStateOutput output = new CurrentStateOutput();
      for (Map.Entry<String, Map<String, Map<String, String>>> r : currentStates.entrySet()) {
        SimResource resource = sim.resources.get(r.getKey());
        if (resource != null) {
          output.setResourceStateModelDef(r.getKey(), resource.stateModel);
        }
        for (Map.Entry<String, Map<String, String>> p : r.getValue().entrySet()) {
          for (Map.Entry<String, String> s : p.getValue().entrySet()) {
            output.setCurrentState(r.getKey(), new Partition(p.getKey()), s.getKey(),
                s.getValue());
          }
        }
      }
      return output;
    }

    private ExecutorService baselineExecutor() {
      return _baselineTasks.delegate;
    }

    private ExecutorService partialExecutor() {
      return _partialTasks.delegate;
    }

    /**
     * Puts a RecordingExecutor in place of a runner's calculation executor. Both runners keep the
     * failure of their last calculation in a field of the same name, which is where it reads it.
     */
    @SuppressWarnings("unchecked")
    private RecordingExecutor recordTasks(String runnerName, String executorName) {
      try {
        Field runnerField = WagedRebalancer.class.getDeclaredField(runnerName);
        runnerField.setAccessible(true);
        Object runner = runnerField.get(rebalancer);
        Field executorField = runner.getClass().getDeclaredField(executorName);
        executorField.setAccessible(true);
        Field failureField = runner.getClass().getDeclaredField("_lastAsyncFailure");
        failureField.setAccessible(true);
        RecordingExecutor recording =
            new RecordingExecutor((ExecutorService) executorField.get(runner),
                (AtomicReference<HelixRebalanceException>) failureField.get(runner));
        executorField.set(runner, recording);
        return recording;
      } catch (ReflectiveOperationException e) {
        throw new IllegalStateException(e);
      }
    }

    /** What each path gained since the snapshot, with every path of zeros listed even at zero. */
    static Map<String, Integer> countsSince(Map<String, Integer> counts,
        Map<String, Integer> snapshot, Collection<String> zeros) {
      Map<String, Integer> since = new TreeMap<>();
      for (String path : zeros) {
        since.put(path, 0);
      }
      for (Map.Entry<String, Integer> e : counts.entrySet()) {
        int gained = e.getValue() - snapshot.getOrDefault(e.getKey(), 0);
        if (gained != 0) {
          since.put(e.getKey(), gained);
        }
      }
      return since;
    }

    private SubStep runOnce(String name, WagedFuzzSim sim, boolean gate) throws Exception {
      SubStep sub = new SubStep(name);
      Map<String, Resource> resourceMap = sim.resourceMap();
      CurrentStateOutput cso = currentStateOutput(sim);
      _currentStateOutput = cso;
      _resources = new TreeSet<>(resourceMap.keySet());
      sub.currentStatesBefore = deepCopyStates(currentStates);
      sub.baselineBefore = copyAssignments(store.getBaseline());
      sub.bestPossibleBefore = copyAssignments(store.getBestPossibleAssignment());
      algorithm.drainOutcomes();
      store.lastCacheUpdate = null;
      store.cacheUpdates.clear();
      _baselineTasks.outcomes.clear();
      _partialTasks.outcomes.clear();
      Map<String, Integer> writesBefore = new TreeMap<>(accessor._writes);
      Map<String, Integer> deletesBefore = new TreeMap<>(accessor._deletes);
      _served = null;
      CountDownLatch latch = null;
      ExecutorService executor = null;
      if (gate) {
        executor = baselineExecutor();
        CountDownLatch gateLatch = new CountDownLatch(1);
        latch = gateLatch;
        executor.submit(() -> {
          gateLatch.await();
          return null;
        });
      }
      try {
        sub.idealStates = rebalancer.computeNewIdealStates(_dataProvider, resourceMap, cso);
      } catch (Exception e) {
        sub.failure = e;
      } finally {
        try {
          if (asyncPartial) {
            // Let the partial rebalance finish before the run is read and before the gated
            // baseline starts, so every run takes the same interleaving: the one where the
            // baseline calculation is the slow one.
            partialExecutor().submit(() -> null).get();
          }
        } finally {
          if (latch != null) {
            latch.countDown();
          }
        }
      }
      if (gate) {
        executor.submit(() -> null).get();
      }
      sub.outcomes = algorithm.drainOutcomes();
      sub.partialCacheUpdate = store.lastCacheUpdate;
      sub.writes = countsSince(accessor._writes, writesBefore, new TreeSet<>(accessor._paths));
      sub.deletes = countsSince(accessor._deletes, deletesBefore, Collections.emptySet());
      sub.baselineTasks = new ArrayList<>(_baselineTasks.outcomes);
      sub.partialTasks = new ArrayList<>(_partialTasks.outcomes);
      sub.cacheUpdates = new ArrayList<>(store.cacheUpdates);
      sub.served = _served;
      sub.baselineAfter = copyAssignments(store.getBaseline());
      sub.bestPossibleAfter = copyAssignments(store.getBestPossibleAssignment());
      return sub;
    }

    /** Participants take on exactly the emitted states. */
    void converge(WagedFuzzSim sim, SubStep sub) {
      if (sub.idealStates == null) {
        return;
      }
      Set<String> live = new TreeSet<>();
      for (SimNode n : sim.nodes.values()) {
        if (n.live) {
          live.add(n.name);
        }
      }
      for (IdealState is : sub.idealStates.values()) {
        Map<String, Map<String, String>> partitions = new TreeMap<>();
        for (Map.Entry<String, Map<String, String>> p : is.getRecord().getMapFields().entrySet()) {
          Map<String, String> states = new TreeMap<>();
          for (Map.Entry<String, String> s : p.getValue().entrySet()) {
            if (live.contains(s.getKey()) && !"DROPPED".equals(s.getValue())) {
              states.put(s.getKey(), s.getValue());
            }
          }
          if (!states.isEmpty()) {
            partitions.put(p.getKey(), states);
          }
        }
        if (partitions.isEmpty()) {
          currentStates.remove(is.getResourceName());
        } else {
          currentStates.put(is.getResourceName(), partitions);
        }
      }
    }

    public StepResult step(WagedFuzzSim sim, int step, String event) throws Exception {
      if (sim.failoverRequested) {
        rebalancer.reset();
      }
      populate(sim);
      StepResult result = new StepResult(step, event);
      SubStep first = runOnce("a", sim, asyncBaseline);
      result.subSteps.add(first);
      if (sim.converge) {
        converge(sim, first);
      }
      // A finished asynchronous baseline or partial rebalance schedules an on demand pipeline run;
      // replay it on the same input once per asynchronous calculation, so the last run serves
      // what both of them computed.
      int replays = (asyncBaseline ? 1 : 0) + (asyncPartial ? 1 : 0);
      for (int i = 0; i < replays; i++) {
        populate(sim);
        SubStep next = runOnce(String.valueOf((char) ('b' + i)), sim, asyncBaseline);
        result.subSteps.add(next);
        if (sim.converge) {
          converge(sim, next);
        }
      }
      result.json = render(result);
      result.sha = sha256(result.json);
      if (!keepFullJson) {
        result.json = null;
      }
      return result;
    }

    String render(StepResult result) {
      Map<String, Object> root = new TreeMap<>();
      root.put("step", result.step);
      root.put("event", result.event);
      List<Object> subs = new ArrayList<>();
      for (SubStep sub : result.subSteps) {
        Map<String, Object> s = new TreeMap<>();
        s.put("sub", sub.name);
        s.put("failure", describe(sub.failure));
        if (sub.idealStates != null) {
          Map<String, Object> ideal = new TreeMap<>();
          for (Map.Entry<String, IdealState> e : sub.idealStates.entrySet()) {
            ideal.put(e.getKey(), recordView(e.getValue().getRecord()));
          }
          s.put("idealStates", ideal);
        }
        List<Object> algo = new ArrayList<>();
        for (FuzzRecordingAlgorithm.Outcome o : sub.outcomes) {
          if (o.calculate) {
            algo.add(o.toString());
          }
        }
        s.put("calculate", algo);
        s.put("baseline", assignmentsView(sub.baselineAfter));
        s.put("bestPossible", assignmentsView(sub.bestPossibleAfter));
        s.put("served", sub.served == null ? null : assignmentsView(sub.served));
        s.put("writes", sub.writes);
        s.put("deletes", sub.deletes);
        s.put("baselineTasks", sub.baselineTasks);
        s.put("partialTasks", sub.partialTasks);
        s.put("cacheUpdates", sub.cacheUpdates);
        subs.add(s);
      }
      root.put("subSteps", subs);
      root.put("counts", countMetrics());
      root.put("monitor", monitorView());
      root.put("persisted", persistedView());
      // Totals since the driver started, so a write made outside a call shows as well.
      root.put("writesTotal", new TreeMap<>(accessor._writes));
      root.put("deletesTotal", new TreeMap<>(accessor._deletes));
      StringBuilder sb = new StringBuilder();
      json(sb, root);
      return sb.toString();
    }

    Map<String, Object> countMetrics() {
      Map<String, Object> counts = new TreeMap<>();
      for (Map.Entry<String, Metric> e : rebalancer.getMetricCollector().getMetricMap()
          .entrySet()) {
        if (e.getValue() instanceof CountMetric) {
          counts.put(e.getKey(), ((CountMetric) e.getValue()).getLastEmittedMetricValue());
        }
      }
      return counts;
    }

    Map<String, Object> monitorView() {
      ClusterStatusMonitor m = monitor;
      Map<String, Object> view = new TreeMap<>();
      view.put("RebalanceFailureCounter", m.getRebalanceFailureCounter());
      view.put("WagedCustomerActionableFailureCounter",
          m.getWagedCustomerActionableFailureCounter());
      view.put("WagedInternalFailureCounter", m.getWagedInternalFailureCounter());
      view.put("WagedCustomerActionableFailureGauge", m.getWagedCustomerActionableFailureGauge());
      view.put("WagedInternalFailureGauge", m.getWagedInternalFailureGauge());
      view.put("WagedBaselineComputeFailingGauge", m.getWagedBaselineComputeFailingGauge());
      view.put("WagedRebalanceOverwriteFailingGauge",
          m.getWagedRebalanceOverwriteFailingGauge());
      view.put("WagedFailureCapacityDeficitCounter", m.getWagedFailureCapacityDeficitCounter());
      view.put("WagedFailureNoCandidateNodeCounter", m.getWagedFailureNoCandidateNodeCounter());
      view.put("WagedFailureInvalidResourceConfigCounter",
          m.getWagedFailureInvalidResourceConfigCounter());
      view.put("WagedFailureInvalidClusterConfigCounter",
          m.getWagedFailureInvalidClusterConfigCounter());
      view.put("WagedFailureMetadataStoreIoCounter", m.getWagedFailureMetadataStoreIoCounter());
      view.put("WagedFailureAlgorithmInternalCounter",
          m.getWagedFailureAlgorithmInternalCounter());
      view.put("WagedFailureAsyncExecutionCounter", m.getWagedFailureAsyncExecutionCounter());
      view.put("WagedFailureUnknownCounter", m.getWagedFailureUnknownCounter());
      view.put("WagedFallbackInUseGauge", m.getWagedFallbackInUseGauge());
      view.put("HcFaultZoneFailure", m.getWagedHardConstraintFaultZoneFailureCounter());
      view.put("HcNodeCapacityFailure", m.getWagedHardConstraintNodeCapacityFailureCounter());
      view.put("HcNodeMaxPartitionLimitFailure",
          m.getWagedHardConstraintNodeMaxPartitionLimitFailureCounter());
      view.put("HcReplicaActivateFailure",
          m.getWagedHardConstraintReplicaActivateFailureCounter());
      view.put("HcSamePartitionOnInstanceFailure",
          m.getWagedHardConstraintSamePartitionOnInstanceFailureCounter());
      view.put("HcValidGroupTagFailure", m.getWagedHardConstraintValidGroupTagFailureCounter());
      view.put("HcUnknownFailure", m.getWagedHardConstraintUnknownFailureCounter());
      view.put("HcFaultZoneBlocking", m.getWagedHardConstraintFaultZoneBlockingGauge());
      view.put("HcNodeCapacityBlocking", m.getWagedHardConstraintNodeCapacityBlockingGauge());
      view.put("HcNodeMaxPartitionLimitBlocking",
          m.getWagedHardConstraintNodeMaxPartitionLimitBlockingGauge());
      view.put("HcReplicaActivateBlocking",
          m.getWagedHardConstraintReplicaActivateBlockingGauge());
      view.put("HcSamePartitionOnInstanceBlocking",
          m.getWagedHardConstraintSamePartitionOnInstanceBlockingGauge());
      view.put("HcValidGroupTagBlocking", m.getWagedHardConstraintValidGroupTagBlockingGauge());
      view.put("HcUnknownBlocking", m.getWagedHardConstraintUnknownBlockingGauge());
      return view;
    }

    Map<String, Object> persistedView() {
      Map<String, Object> view = new TreeMap<>();
      for (Map.Entry<String, byte[]> e : accessor._data.entrySet()) {
        view.put(e.getKey(), sha256(new String(e.getValue(), StandardCharsets.UTF_8)));
      }
      return view;
    }

    @Override
    public void close() {
      rebalancer.close();
      _asyncPool.shutdownNow();
    }
  }

  // ---------------------------------------------------------------------------------------------
  // Canonical rendering
  // ---------------------------------------------------------------------------------------------

  public static Map<String, ResourceAssignment> copyAssignments(
      Map<String, ResourceAssignment> assignments) {
    Map<String, ResourceAssignment> copy = new TreeMap<>();
    for (Map.Entry<String, ResourceAssignment> e : assignments.entrySet()) {
      copy.put(e.getKey(), new ResourceAssignment(deepCopy(e.getValue().getRecord())));
    }
    return copy;
  }

  public static ZNRecord deepCopy(ZNRecord record) {
    ZNRecord copy = new ZNRecord(record.getId());
    copy.setSimpleFields(new TreeMap<>(record.getSimpleFields()));
    Map<String, List<String>> lists = new TreeMap<>();
    for (Map.Entry<String, List<String>> e : record.getListFields().entrySet()) {
      lists.put(e.getKey(), new ArrayList<>(e.getValue()));
    }
    copy.setListFields(lists);
    Map<String, Map<String, String>> maps = new TreeMap<>();
    for (Map.Entry<String, Map<String, String>> e : record.getMapFields().entrySet()) {
      maps.put(e.getKey(), new TreeMap<>(e.getValue()));
    }
    copy.setMapFields(maps);
    return copy;
  }

  static Map<String, Map<String, Map<String, String>>> deepCopyStates(
      Map<String, Map<String, Map<String, String>>> states) {
    Map<String, Map<String, Map<String, String>>> copy = new TreeMap<>();
    for (Map.Entry<String, Map<String, Map<String, String>>> r : states.entrySet()) {
      Map<String, Map<String, String>> partitions = new TreeMap<>();
      for (Map.Entry<String, Map<String, String>> p : r.getValue().entrySet()) {
        partitions.put(p.getKey(), new TreeMap<>(p.getValue()));
      }
      copy.put(r.getKey(), partitions);
    }
    return copy;
  }

  public static Map<String, Object> recordView(ZNRecord record) {
    Map<String, Object> view = new TreeMap<>();
    view.put("id", record.getId());
    view.put("simple", new TreeMap<>(record.getSimpleFields()));
    view.put("list", new TreeMap<>(record.getListFields()));
    Map<String, Object> maps = new TreeMap<>();
    for (Map.Entry<String, Map<String, String>> e : record.getMapFields().entrySet()) {
      maps.put(e.getKey(), new TreeMap<>(e.getValue()));
    }
    view.put("map", maps);
    return view;
  }

  public static Map<String, Object> assignmentsView(Map<String, ResourceAssignment> assignments) {
    Map<String, Object> view = new TreeMap<>();
    if (assignments != null) {
      for (Map.Entry<String, ResourceAssignment> e : assignments.entrySet()) {
        view.put(e.getKey(), recordView(e.getValue().getRecord()));
      }
    }
    return view;
  }

  /**
   * Every configuration input the rebalancer reads from this cluster, rendered canonically. Two
   * clusters with equal digests differ only in their history: the assignment store and the current
   * states.
   */
  public String inputsDigest() {
    Map<String, Object> view = new TreeMap<>();
    view.put("clusterConfig", recordView(clusterConfig.getRecord()));
    Map<String, Object> nodeView = new TreeMap<>();
    for (SimNode n : nodes.values()) {
      Map<String, Object> v = new TreeMap<>();
      v.put("config", n.config == null ? null : recordView(n.config.getRecord()));
      v.put("parked", n.parkedConfig == null ? null : recordView(n.parkedConfig.getRecord()));
      v.put("live", n.live);
      v.put("session", n.session);
      v.put("offlineTime", n.offlineTime);
      nodeView.put(n.name, v);
    }
    view.put("nodes", nodeView);
    Map<String, Object> resourceView = new TreeMap<>();
    for (SimResource r : resources.values()) {
      Map<String, Object> v = new TreeMap<>();
      v.put("idealState", recordView(r.idealState().getRecord()));
      v.put("resourceConfig", recordView(r.resourceConfig().getRecord()));
      resourceView.put(r.name, v);
    }
    view.put("resources", resourceView);
    StringBuilder sb = new StringBuilder();
    json(sb, view);
    return sb.toString();
  }

  public static Object describe(Throwable t) {
    if (t == null) {
      return null;
    }
    Map<String, Object> view = new TreeMap<>();
    view.put("class", t.getClass().getName());
    view.put("message", canonicalMessage(t.getMessage()));
    if (t instanceof HelixRebalanceException) {
      view.put("type", String.valueOf(((HelixRebalanceException) t).getFailureType()));
      view.put("category", String.valueOf(((HelixRebalanceException) t).getFailureCategory()));
    }
    if (t.getCause() != null && t.getCause() != t) {
      view.put("cause", describe(t.getCause()));
    }
    return view;
  }

  /**
   * Failure messages embed maps filled by parallel streams, so the order of colliding entries
   * varies from run to run even on unchanged code. Sort the entries of every brace-delimited map,
   * leaving bracketed lists in order, so messages compare by content.
   */
  public static String canonicalMessage(String message) {
    if (message == null || message.indexOf('{') < 0) {
      return message;
    }
    StringBuilder out = new StringBuilder();
    int i = 0;
    while (i < message.length()) {
      char c = message.charAt(i);
      if (c == '{') {
        int end = matching(message, i);
        if (end < 0) {
          out.append(message.substring(i));
          break;
        }
        out.append(canonicalMap(message.substring(i + 1, end)));
        i = end + 1;
      } else {
        out.append(c);
        i++;
      }
    }
    return out.toString();
  }

  private static int matching(String s, int open) {
    int depth = 0;
    for (int i = open; i < s.length(); i++) {
      char c = s.charAt(i);
      if (c == '{' || c == '[') {
        depth++;
      } else if (c == '}' || c == ']') {
        depth--;
        if (depth == 0) {
          return c == '}' ? i : -1;
        }
      }
    }
    return -1;
  }

  private static String canonicalMap(String body) {
    List<String> entries = new ArrayList<>();
    int depth = 0;
    int start = 0;
    for (int i = 0; i < body.length(); i++) {
      char c = body.charAt(i);
      if (c == '{' || c == '[') {
        depth++;
      } else if (c == '}' || c == ']') {
        depth--;
      } else if (depth == 0 && c == ',' && i + 1 < body.length() && body.charAt(i + 1) == ' ') {
        entries.add(canonicalMessage(body.substring(start, i)));
        start = i + 2;
        i++;
      }
    }
    entries.add(canonicalMessage(body.substring(start)));
    Collections.sort(entries);
    return "{" + String.join(", ", entries) + "}";
  }

  @SuppressWarnings("unchecked")
  public static void json(StringBuilder sb, Object o) {
    if (o == null) {
      sb.append("null");
    } else if (o instanceof Map) {
      sb.append('{');
      boolean first = true;
      for (Map.Entry<Object, Object> e : new TreeMap<>((Map<Object, Object>) o).entrySet()) {
        if (!first) {
          sb.append(',');
        }
        first = false;
        json(sb, String.valueOf(e.getKey()));
        sb.append(':');
        json(sb, e.getValue());
      }
      sb.append('}');
    } else if (o instanceof Collection) {
      sb.append('[');
      boolean first = true;
      for (Object item : (Collection<Object>) o) {
        if (!first) {
          sb.append(',');
        }
        first = false;
        json(sb, item);
      }
      sb.append(']');
    } else if (o instanceof Number || o instanceof Boolean) {
      sb.append(o);
    } else {
      String s = o.toString();
      sb.append('"');
      for (int i = 0; i < s.length(); i++) {
        char c = s.charAt(i);
        if (c == '"' || c == '\\') {
          sb.append('\\').append(c);
        } else if (c < 0x20) {
          sb.append(String.format("\\u%04x", (int) c));
        } else {
          sb.append(c);
        }
      }
      sb.append('"');
    }
  }

  public static String sha256(String s) {
    try {
      MessageDigest digest = MessageDigest.getInstance("SHA-256");
      byte[] hash = digest.digest(s.getBytes(StandardCharsets.UTF_8));
      StringBuilder sb = new StringBuilder();
      for (int i = 0; i < 12; i++) {
        sb.append(String.format("%02x", hash[i]));
      }
      return sb.toString();
    } catch (NoSuchAlgorithmException e) {
      throw new IllegalStateException(e);
    }
  }
}
