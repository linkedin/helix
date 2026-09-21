package org.apache.helix.controller.rebalancer.waged.constraints;

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

import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.concurrent.ForkJoinPool;
import java.util.function.Function;
import java.util.function.Supplier;

import org.apache.helix.HelixRebalanceException;
import org.apache.helix.controller.rebalancer.waged.WagedFuzzSim;
import org.apache.helix.controller.rebalancer.waged.model.AssignableNode;
import org.apache.helix.controller.rebalancer.waged.model.AssignableReplica;
import org.apache.helix.controller.rebalancer.waged.model.ClusterModel;
import org.apache.helix.controller.rebalancer.waged.model.OptimalAssignment;
import org.apache.helix.model.ResourceAssignment;

/**
 * The production ConstraintBasedAlgorithm with the default constraints, plus a log of every
 * calculate() outcome and every published isolation outcome. It stays an instance of
 * ConstraintBasedAlgorithm so WagedRebalancer still wires its metric reporters exactly as in
 * production. It deliberately avoids any instance tag isolation API, so the same source also
 * compiles on a build without instance tag isolation.
 */
public class FuzzRecordingAlgorithm extends ConstraintBasedAlgorithm {
  /** One outcome of calculate() or of the isolation hook. */
  public static final class Outcome {
    public final String scope;
    public final boolean calculate;
    public final String failure;
    public final Set<String> evaluated;
    public final Set<String> skipped;
    /**
     * Set on calculate outcomes while capturing: the previous assignment the scope started from,
     * in instance name view, exactly as the rebalancer hands it to the carry forward.
     */
    public Map<String, ResourceAssignment> previous;
    /** Set on calculate outcomes while capturing: the raw best possible in the store at start. */
    public Map<String, ResourceAssignment> storeBestPossibleAtStart;
    /** Set on calculate outcomes while capturing: resource, partition, state, replica count. */
    public Map<String, Map<String, Map<String, Integer>>> modelReplicas;
    /** Set on calculate outcomes while capturing: resource, partition, pre-loaded instances. */
    public Map<String, Map<String, Set<String>>> allocated;
    /** Set on calculate outcomes while capturing: node name to its tags, in the model. */
    public Map<String, Set<String>> nodeTags;
    /** Set on calculate outcomes while capturing: node name to its DISK capacity, in the model. */
    public Map<String, Integer> nodeCapacity;
    /** Set on calculate outcomes while capturing: resources with replicas left to place. */
    public Set<String> toBeAssigned;
    /** Set on calculate outcomes while capturing: resource, partition, state, count to place. */
    public Map<String, Map<String, Map<String, Integer>>> toAssign;
    /**
     * Set on calculate outcomes while capturing: the model's cluster wide capacity left after
     * every replica it estimates for, by capacity key. Negative means a cluster wide deficit.
     */
    public Map<String, Long> estimatedRemaining;
    /** Set on calculate failures: the failure category, or null for a non Helix failure. */
    public String failureCategory;
    /**
     * Set on successful calculate outcomes: what the algorithm itself skipped, before any carried
     * forward placement made another group yield. Null on code without isolation.
     */
    public Set<String> algorithmSkipped;

    Outcome(String scope, boolean calculate, String failure, Set<String> evaluated,
        Set<String> skipped) {
      this.scope = scope;
      this.calculate = calculate;
      this.failure = failure;
      this.evaluated = evaluated;
      this.skipped = skipped;
    }

    @Override
    public String toString() {
      if (calculate) {
        return scope + " calculate " + (failure == null ? "ok" : failure);
      }
      return scope + " computed skipped=" + skipped;
    }
  }

  private final List<Outcome> _outcomes = Collections.synchronizedList(new ArrayList<>());

  private FuzzRecordingAlgorithm(List<HardConstraint> hardConstraints,
      Map<SoftConstraint, Float> softConstraints, ForkJoinPool pool) {
    super(hardConstraints, softConstraints, pool);
  }

  /** Builds the algorithm with the same constraints and weights as the production factory. */
  @SuppressWarnings("unchecked")
  public static FuzzRecordingAlgorithm create() {
    ConstraintBasedAlgorithm prototype = (ConstraintBasedAlgorithm) ConstraintBasedAlgorithmFactory
        .getInstance(Collections.emptyMap());
    return new FuzzRecordingAlgorithm(
        (List<HardConstraint>) readField(prototype, "_hardConstraints"),
        (Map<SoftConstraint, Float>) readField(prototype, "_softConstraints"),
        (ForkJoinPool) readField(prototype, "_constraintEvaluationPool"));
  }

  private static Object readField(Object target, String name) {
    try {
      Field field = ConstraintBasedAlgorithm.class.getDeclaredField(name);
      field.setAccessible(true);
      return field.get(target);
    } catch (ReflectiveOperationException e) {
      throw new IllegalStateException(e);
    }
  }

  /** When set, calculate outcomes also carry a snapshot of the model inputs. */
  public volatile boolean capture;
  /**
   * While capturing, supplies the previous assignment a scope starts from. Without it the model's
   * own logical id view is used, which leaves out instances that have no instance config.
   */
  public volatile Function<String, Map<String, ResourceAssignment>> previousSource;
  /** While capturing, supplies the raw best possible content of the metadata store. */
  public volatile Supplier<Map<String, ResourceAssignment>> storeBestPossible;

  private static final Method ASSIGNED_REPLICAS;
  // Looked up reflectively, so this compiles on a build without isolation.
  private static final Method SKIPPED_RESOURCES;

  static {
    try {
      ASSIGNED_REPLICAS = AssignableNode.class.getDeclaredMethod("getAssignedReplicas");
      ASSIGNED_REPLICAS.setAccessible(true);
    } catch (ReflectiveOperationException e) {
      throw new IllegalStateException(e);
    }
    Method skipped;
    try {
      skipped = OptimalAssignment.class.getMethod("getSkippedResources");
    } catch (NoSuchMethodException e) {
      skipped = null;
    }
    SKIPPED_RESOURCES = skipped;
  }

  @SuppressWarnings("unchecked")
  private static Set<String> skippedOf(OptimalAssignment result) {
    if (SKIPPED_RESOURCES == null) {
      return null;
    }
    try {
      return Collections.unmodifiableSet(
          new TreeSet<>((Set<String>) SKIPPED_RESOURCES.invoke(result)));
    } catch (ReflectiveOperationException e) {
      throw new IllegalStateException(e);
    }
  }

  @Override
  public OptimalAssignment calculate(ClusterModel clusterModel) throws HelixRebalanceException {
    String scope = String.valueOf(clusterModel.getRebalanceScopeType());
    Outcome snapshot = capture ? snapshot(scope, clusterModel) : null;
    try {
      OptimalAssignment result = super.calculate(clusterModel);
      Outcome outcome = withSnapshot(new Outcome(scope, true, null, null, null), snapshot);
      outcome.algorithmSkipped = skippedOf(result);
      _outcomes.add(outcome);
      return result;
    } catch (HelixRebalanceException e) {
      Outcome outcome = withSnapshot(new Outcome(scope, true,
          e.getClass().getName() + " " + e.getFailureType() + " " + e.getFailureCategory() + " "
              + WagedFuzzSim.canonicalMessage(e.getMessage()), null, null), snapshot);
      outcome.failureCategory = String.valueOf(e.getFailureCategory());
      _outcomes.add(outcome);
      throw e;
    } catch (RuntimeException e) {
      _outcomes.add(withSnapshot(new Outcome(scope, true,
          e.getClass().getName() + " " + WagedFuzzSim.canonicalMessage(e.getMessage()), null,
          null), snapshot));
      throw e;
    }
  }

  @SuppressWarnings("unchecked")
  private Outcome snapshot(String scope, ClusterModel model) {
    Outcome o = new Outcome(scope, true, null, null, null);
    Function<String, Map<String, ResourceAssignment>> source = previousSource;
    o.previous = WagedFuzzSim.copyAssignments(source != null ? source.apply(scope)
        : model.getContext().getBestPossibleAssignment());
    Supplier<Map<String, ResourceAssignment>> store = storeBestPossible;
    o.storeBestPossibleAtStart =
        store == null ? null : WagedFuzzSim.copyAssignments(store.get());
    o.modelReplicas = new TreeMap<>();
    o.allocated = new TreeMap<>();
    for (Set<AssignableReplica> replicas : model.getAssignableReplicaMap().values()) {
      for (AssignableReplica replica : replicas) {
        countReplica(o, replica);
      }
    }
    for (AssignableNode node : model.getAssignableNodes().values()) {
      Set<AssignableReplica> assigned;
      try {
        assigned = (Set<AssignableReplica>) ASSIGNED_REPLICAS.invoke(node);
      } catch (ReflectiveOperationException e) {
        throw new IllegalStateException(e);
      }
      for (AssignableReplica replica : assigned) {
        countReplica(o, replica);
        o.allocated.computeIfAbsent(replica.getResourceName(), k -> new TreeMap<>())
            .computeIfAbsent(replica.getPartitionName(), k -> new TreeSet<>())
            .add(node.getInstanceName());
      }
    }
    o.nodeTags = new TreeMap<>();
    o.nodeCapacity = new TreeMap<>();
    for (AssignableNode node : model.getAssignableNodes().values()) {
      o.nodeTags.put(node.getInstanceName(), new TreeSet<>(node.getInstanceTags()));
      o.nodeCapacity.put(node.getInstanceName(),
          node.getMaxCapacity().getOrDefault(WagedFuzzSim.DISK, 0));
    }
    o.toBeAssigned = new TreeSet<>();
    o.toAssign = new TreeMap<>();
    for (Map.Entry<String, Set<AssignableReplica>> e : model.getAssignableReplicaMap()
        .entrySet()) {
      if (!e.getValue().isEmpty()) {
        o.toBeAssigned.add(e.getKey());
      }
      for (AssignableReplica replica : e.getValue()) {
        o.toAssign.computeIfAbsent(replica.getResourceName(), k -> new TreeMap<>())
            .computeIfAbsent(replica.getPartitionName(), k -> new TreeMap<>())
            .merge(replica.getReplicaState(), 1, Integer::sum);
      }
    }
    o.estimatedRemaining = new TreeMap<>(model.getContext().getEstimateUtilizationMap());
    return o;
  }

  private static void countReplica(Outcome o, AssignableReplica replica) {
    o.modelReplicas.computeIfAbsent(replica.getResourceName(), k -> new TreeMap<>())
        .computeIfAbsent(replica.getPartitionName(), k -> new TreeMap<>())
        .merge(replica.getReplicaState(), 1, Integer::sum);
  }

  private static Outcome withSnapshot(Outcome outcome, Outcome snapshot) {
    if (snapshot != null) {
      outcome.previous = snapshot.previous;
      outcome.storeBestPossibleAtStart = snapshot.storeBestPossibleAtStart;
      outcome.modelReplicas = snapshot.modelReplicas;
      outcome.allocated = snapshot.allocated;
      outcome.nodeTags = snapshot.nodeTags;
      outcome.nodeCapacity = snapshot.nodeCapacity;
      outcome.toBeAssigned = snapshot.toBeAssigned;
      outcome.toAssign = snapshot.toAssign;
      outcome.estimatedRemaining = snapshot.estimatedRemaining;
    }
    return outcome;
  }

  /**
   * Records the isolation outcome the rebalancer publishes through the algorithm interface, as an
   * outcome that is not a calculation. A build whose interface has no such hook never calls it.
   */
  public void onAssignmentComputed(ClusterModel.RebalanceScopeType scope,
      Set<String> evaluatedResources, Set<String> skippedResources) {
    _outcomes.add(new Outcome(String.valueOf(scope), false, null,
        Collections.unmodifiableSet(new TreeSet<>(evaluatedResources)),
        Collections.unmodifiableSet(new TreeSet<>(skippedResources))));
  }

  /** Returns and clears the outcomes recorded since the last call. */
  public List<Outcome> drainOutcomes() {
    synchronized (_outcomes) {
      List<Outcome> copy = new ArrayList<>(_outcomes);
      _outcomes.clear();
      return copy;
    }
  }
}
