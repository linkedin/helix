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

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CancellationException;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BiConsumer;
import java.util.function.Consumer;
import java.util.function.Supplier;
import java.util.stream.Collectors;

import com.google.common.collect.Maps;
import org.apache.helix.HelixRebalanceException;
import org.apache.helix.controller.rebalancer.waged.RebalanceAlgorithm;
import org.apache.helix.controller.rebalancer.waged.model.AssignableNode;
import org.apache.helix.controller.rebalancer.waged.model.AssignableReplica;
import org.apache.helix.controller.rebalancer.waged.model.ClusterContext;
import org.apache.helix.controller.rebalancer.waged.model.ClusterModel;
import org.apache.helix.controller.rebalancer.waged.model.OptimalAssignment;
import org.apache.helix.model.Partition;
import org.apache.helix.model.ResourceAssignment;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * The algorithm is based on a given set of constraints
 * - HardConstraint: Approve or deny the assignment given its condition, any assignment cannot
 * bypass any "hard constraint"
 * - SoftConstraint: Evaluate the assignment by points/rewards/scores, a higher point means a better
 * assignment
 * The goal is to accumulate the most points(rewards) from "soft constraints" while avoiding any
 * "hard constraints"
 */
public class ConstraintBasedAlgorithm implements RebalanceAlgorithm {
  private static final float DIV_GUARD = 0.01f;
  // Bound single-hop relocation probes per unplaceable replica and across all unsuccessful
  // replicas in one calculate() run so a saturated tag clique (e.g. 200 unplaceable replicas
  // scanning 1,000 occupied hosts) stays fast instead of degrading into O(replicas * nodes).
  private static final int MAX_RELOCATION_EVALUATIONS = 256;
  private static final int MAX_TOTAL_UNSUCCESSFUL_RELOCATION_EVALUATIONS = 1024;
  // Host-level hard constraints whose outcome for an incoming replica does not change when a
  // single victim replica is released from that host (fault zone, instance tag, disabled
  // partition, or same partition already on the host). Used to skip whole hosts before iterating
  // over their assigned victims during single-hop relocation.
  private static final Set<HardConstraint.Type> STATIC_HOST_CONSTRAINTS = EnumSet.of(
      HardConstraint.Type.FAULT_ZONE, HardConstraint.Type.VALID_GROUP_TAG,
      HardConstraint.Type.REPLICA_ACTIVATE, HardConstraint.Type.SAME_PARTITION_ON_INSTANCE);
  private static final Logger LOG = LoggerFactory.getLogger(ConstraintBasedAlgorithm.class);
  private final List<HardConstraint> _hardConstraints;
  private final Map<SoftConstraint, Float> _softConstraints;
  private final ForkJoinPool _constraintEvaluationPool;
  // Optional listener invoked when a partition fails to find any eligible node. Fires once per
  // distinct HardConstraint.Type that contributed to the failure (set union, not sum), so
  // observers get partition-level attribution rather than node-rejection counts. May be null
  // when the algorithm runs outside a pipeline that wants metric attribution.
  private volatile Consumer<HardConstraint.Type> _hardConstraintFailureReporter;
  // Optional listener invoked once per calculate() run with the rebalance scope that produced the
  // model plus the complete set of HardConstraint.Types that blocked placement that run (empty when
  // the run placed everything). Unlike the cumulative _hardConstraintFailureReporter, this is a
  // reversible per-run snapshot, so observers can drive a "currently blocking" gauge that resets to
  // 0 on the next clean run -- letting a transient blip be told apart from a persistent failure by
  // value. The scope lets the observer attribute the snapshot to a single rebalance phase (e.g.
  // partial vs baseline) so concurrent phases don't clobber each other's gauge. May be null.
  private volatile BiConsumer<ClusterModel.RebalanceScopeType, Set<HardConstraint.Type>>
      _blockingSnapshotReporter;
  // Optional listeners for instance tag isolation: the category of each failure a successful run
  // contained, and the share blocks a global baseline found. May be null.
  private volatile Consumer<HelixRebalanceException.FailureCategory> _containedFailureReporter;
  private volatile Consumer<Integer> _shareBlocksReporter;

  ConstraintBasedAlgorithm(List<HardConstraint> hardConstraints,
      Map<SoftConstraint, Float> softConstraints, ForkJoinPool constraintEvaluationPool) {
    _hardConstraints = hardConstraints;
    _softConstraints = softConstraints;
    _constraintEvaluationPool = constraintEvaluationPool;
  }

  /**
   * Attach a per-partition-failure reporter that fires once per distinct {@link HardConstraint.Type}
   * that prevented placement. Safe to call from any thread; subsequent calls replace the
   * reference. May be null to disable reporting.
   */
  public void setHardConstraintFailureReporter(Consumer<HardConstraint.Type> reporter) {
    _hardConstraintFailureReporter = reporter;
  }

  /**
   * Attach a per-run blocking-snapshot reporter. It fires exactly once per {@link #calculate} run
   * with the rebalance scope and the complete set of {@link HardConstraint.Type}s that blocked
   * placement that run -- empty when the run placed everything. This is a reversible "currently
   * blocking" view (resets on the next clean run), complementing the monotonic per-type counters
   * from {@link #setHardConstraintFailureReporter}. The scope lets observers route the snapshot to a
   * single owning phase. Safe to call from any thread; null disables it.
   */
  public void setBlockingSnapshotReporter(
      BiConsumer<ClusterModel.RebalanceScopeType, Set<HardConstraint.Type>> reporter) {
    _blockingSnapshotReporter = reporter;
  }

  /**
   * Attach a reporter that, once a run with instance tag isolation succeeds, gets the category of
   * every failure isolation contained in it. Null disables it.
   */
  public void setContainedFailureReporter(
      Consumer<HelixRebalanceException.FailureCategory> reporter) {
    _containedFailureReporter = reporter;
  }

  /**
   * Attach a reporter that gets the number of share blocks instance tag isolation finds in each
   * global baseline. Null disables it.
   */
  public void setShareBlocksReporter(Consumer<Integer> reporter) {
    _shareBlocksReporter = reporter;
  }

  @Override
  public OptimalAssignment calculate(ClusterModel clusterModel) throws HelixRebalanceException {
    // Track every HardConstraint.Type that blocks placement during this run, then publish the
    // complete set once (success or failure) as a reversible "currently blocking" snapshot.
    Set<HardConstraint.Type> blockingTypes = ConcurrentHashMap.newKeySet();
    try {
      return calculateInternal(clusterModel, blockingTypes);
    } finally {
      BiConsumer<ClusterModel.RebalanceScopeType, Set<HardConstraint.Type>> snapshotReporter =
          _blockingSnapshotReporter;
      if (snapshotReporter != null) {
        snapshotReporter.accept(clusterModel.getRebalanceScopeType(), blockingTypes);
      }
    }
  }

  private OptimalAssignment calculateInternal(ClusterModel clusterModel,
      Set<HardConstraint.Type> blockingTypes) throws HelixRebalanceException {
    OptimalAssignment optimalAssignment = new OptimalAssignment();
    List<AssignableNode> nodes = new ArrayList<>(clusterModel.getAssignableNodes().values());
    Set<String> busyInstances =
        getBusyInstances(clusterModel.getContext().getBestPossibleAssignment().values());
    // A no-op unless WAGED_INSTANCE_TAG_ISOLATION_ENABLED is set on the cluster config.
    InstanceTagIsolation isolation = new InstanceTagIsolation(clusterModel, nodes);
    Consumer<Integer> shareBlocksReporter = _shareBlocksReporter;
    boolean bestEffort = clusterModel.getContext().isBestEffortPlacementEnabled();
    // Only a baseline covers every assignable node, and every tag change triggers one.
    if (shareBlocksReporter != null
        && clusterModel.getRebalanceScopeType() == ClusterModel.RebalanceScopeType.GLOBAL_BASELINE
        && clusterModel.getContext().isInstanceTagIsolationEnabled()) {
      shareBlocksReporter.accept(isolation.shareBlockCount());
    }

    Map<String, Float> positiveEstimateClusterRemainCap =
        computeScoringCapacities(clusterModel, isolation);

    List<AssignableReplicaWithScore> toBeAssignedReplicas =
        buildOrderedReplicas(clusterModel, nodes, positiveEstimateClusterRemainCap, bestEffort);

    Set<String> placedOnHostThisRun = bestEffort ? new HashSet<>() : Collections.emptySet();
    List<DeferredReplica> deferredForRelocation =
        bestEffort ? new ArrayList<>() : Collections.emptyList();
    int unplacedReplicas = 0;
    AssignableReplica firstUnplaced = null;

    // Pass 1: Place every replica that has a direct candidate node first so replicas with a valid
    // free spot claim it without displacing an existing resident via unnecessary 1-hop relocation.
    for (AssignableReplicaWithScore replicaWithScore : toBeAssignedReplicas) {
      AssignableReplica replica = replicaWithScore.getAssignableReplica();
      if (isolation.shouldSkip(replica)) {
        continue;
      }
      OptimalAssignment failureSink = bestEffort ? new OptimalAssignment()
          : isolation.failureSink(optimalAssignment);
      Set<HardConstraint.Type> replicaBlocking = bestEffort ? new HashSet<>() : blockingTypes;
      Optional<AssignableNode> maybeBestNode = getNodeWithHighestPoints(replica, nodes,
          clusterModel.getContext(), busyInstances, failureSink, replicaBlocking);
      // Stop if the replica has no candidate node, unless isolation can skip its share block.
      if (!maybeBestNode.isPresent() || failureSink.hasAnyFailure()) {
        if (bestEffort) {
          deferredForRelocation.add(new DeferredReplica(replica, replicaBlocking));
          continue;
        }
        // Only the first placement failure of a run can be thrown. Later ones are tolerated and
        // only logged, so they skip the per node reasons, which grow with the cluster size.
        String failReasons = isolation.hasToleratedPlacementFailure()
            ? "listed for the first failure only" : failureSink.getFailures();
        String errorMessage = String.format(
            "Unable to find any available candidate node for partition %s (resource: %s, cluster: %s); "
                + "Failure summary: %s; Fail reasons: %s",
            replica.getPartitionName(),
            replica.getResourceName(),
            clusterModel.getContext().getClusterName(),
            failureSink.getFailureSummary(),
            failReasons);
        HelixRebalanceException failure = new HelixRebalanceException(errorMessage,
            HelixRebalanceException.Type.FAILED_TO_CALCULATE,
            HelixRebalanceException.FailureCategory.NO_CANDIDATE_NODE);
        if (!isolation.tryIsolate(replica, failure)) {
          throw failure;
        }
        continue;
      }
      AssignableNode bestNode = maybeBestNode.get();
      // Assign the replica and update the cluster model.
      clusterModel
          .assign(replica.getResourceName(), replica.getPartitionName(), replica.getReplicaState(),
              bestNode.getInstanceName());
      isolation.recordPlacement(replica, bestNode);
      if (bestEffort) {
        placedOnHostThisRun.add(bestNode.getInstanceName() + ":" + replica.toString());
      }
    }

    // Pass 2: For replicas that had no direct spot in Pass 1, try a bounded single-hop swap before
    // leaving any remaining unplaceable replicas unplaced via partial commit.
    if (bestEffort && !deferredForRelocation.isEmpty()) {
      Set<String> failedRelocationKeys = new HashSet<>();
      AtomicInteger relocationBudget =
          new AtomicInteger(MAX_TOTAL_UNSUCCESSFUL_RELOCATION_EVALUATIONS);
      boolean anyRelocated = false;
      for (DeferredReplica deferred : deferredForRelocation) {
        AssignableReplica replica = deferred.replica;
        if (anyRelocated) {
          // An earlier 1-hop swap may have opened a fault zone or host slot for a later replica;
          // retry direct placement before probing another swap.
          OptimalAssignment retrySink = new OptimalAssignment();
          Set<HardConstraint.Type> retryBlocking = new HashSet<>();
          Optional<AssignableNode> directRetry = getNodeWithHighestPoints(replica, nodes,
              clusterModel.getContext(), busyInstances, retrySink, retryBlocking);
          if (directRetry.isPresent() && !retrySink.hasAnyFailure()) {
            AssignableNode bestNode = directRetry.get();
            clusterModel.assign(replica.getResourceName(), replica.getPartitionName(),
                replica.getReplicaState(), bestNode.getInstanceName());
            isolation.recordPlacement(replica, bestNode);
            placedOnHostThisRun.add(bestNode.getInstanceName() + ":" + replica.toString());
            failedRelocationKeys.clear();
            continue;
          }
          deferred.blockingTypes = retryBlocking;
        }
        if (relocationBudget.get() > 0 && !failedRelocationKeys.contains(replica.toString())
            && tryRelocateBlockingReplica(replica, nodes, clusterModel, busyInstances,
                placedOnHostThisRun, relocationBudget)) {
          anyRelocated = true;
          failedRelocationKeys.clear();
          continue;
        }
        // Memoize unsuccessful replica keys until the next placement mutates cluster occupancy so
        // identical unplaceable replicas (e.g. a missing tag) do not re-scan every host.
        failedRelocationKeys.add(replica.toString());
        blockingTypes.addAll(deferred.blockingTypes);
        if (_hardConstraintFailureReporter != null) {
          deferred.blockingTypes.forEach(_hardConstraintFailureReporter);
        }
        if (firstUnplaced == null) {
          firstUnplaced = replica;
        }
        unplacedReplicas++;
      }
    }

    if (unplacedReplicas > 0) {
      LOG.warn("Best-effort placement left {} replica(s) unplaced in {} rebalance of cluster {} "
              + "(first unplaced: {}).", unplacedReplicas, clusterModel.getRebalanceScopeType(),
          clusterModel.getContext().getClusterName(), firstUnplaced);
    }
    isolation.finish(optimalAssignment);
    Consumer<HelixRebalanceException.FailureCategory> containedFailureReporter =
        _containedFailureReporter;
    if (containedFailureReporter != null) {
      isolation.containedFailures().forEach(containedFailureReporter);
    }
    optimalAssignment.updateAssignments(clusterModel);
    return optimalAssignment;
  }

  /**
   * Estimated free capacity per key once every replica is placed, plus 1% of the total to avoid a
   * divide by zero. Used to score replicas.
   *
   * @throws HelixRebalanceException when the cluster as a whole cannot hold all the partitions.
   *         With instance tag isolation on, the blocks behind the deficit are skipped instead
   *         wherever {@link InstanceTagIsolation#absorbCapacityDeficit} can do so, and it throws
   *         where it cannot. With best-effort placement on, short keys are clamped to a positive
   *         guard so placement proceeds with what fits.
   */
  private static Map<String, Float> computeScoringCapacities(ClusterModel clusterModel,
      InstanceTagIsolation isolation) throws HelixRebalanceException {
    boolean bestEffort = clusterModel.getContext().isBestEffortPlacementEnabled();
    Map<String, Float> positiveEstimateClusterRemainCap = new HashMap<>();
    for (Map.Entry<String, Long> clusterRemainingCap : clusterModel.getContext()
        .getEstimateUtilizationMap().entrySet()) {
      String capacityKey = clusterRemainingCap.getKey();
      if (clusterRemainingCap.getValue() < 0) {
        long totalCapacity = clusterModel.getContext().getClusterCapacityMap().get(capacityKey);
        if (bestEffort) {
          // When total replica demand exceeds cluster capacity (e.g. 21 replicas on 20 single-slot
          // nodes), clamp the denominator to a positive 1% guard instead of throwing
          // CAPACITY_DEFICIT so replica urgency scores stay positive and finite.
          positiveEstimateClusterRemainCap.put(capacityKey,
              Math.max(DIV_GUARD, totalCapacity * DIV_GUARD));
          continue;
        }
        // A short key fails the run, unless isolation can skip the blocks that are over.
        long remainingCapacity = clusterRemainingCap.getValue();
        long totalUsage = totalCapacity - remainingCapacity;
        HelixRebalanceException capacityDeficit = new HelixRebalanceException(String
            .format("The cluster '%s' does not have enough %s capacity for all partitions. Total capacity: %d, Required: %d, Deficit: %d",
                clusterModel.getContext().getClusterName(), capacityKey, totalCapacity, totalUsage, Math.abs(remainingCapacity)),
            HelixRebalanceException.Type.FAILED_TO_CALCULATE,
            HelixRebalanceException.FailureCategory.CAPACITY_DEFICIT);
        // This sum ignores tags, so one clique far over capacity can push it below zero.
        Map<String, Float> residual =
            isolation.absorbCapacityDeficit(capacityDeficit, DIV_GUARD);
        if (residual == null) {
          throw capacityDeficit;
        }
        return residual;
      }
      // estimate remain capacity after assignment + %1 of current cluster capacity before assignment
      positiveEstimateClusterRemainCap.put(capacityKey,
           (float) clusterModel.getContext().getEstimateUtilizationMap().get(capacityKey) +
          (clusterModel.getContext().getClusterCapacityMap().get(capacityKey) * DIV_GUARD));
    }
    return positiveEstimateClusterRemainCap;
  }

  /**
   * Orders unassigned replicas for greedy placement. In best-effort mode, two invariants prevent
   * starvation and unnecessary live movement under tight or skewed fault zones:
   * 1. Partition ordinal round-robin (0, 1, ..., RF - 1), starting from the count of replicas
   *    already pre-allocated on nodes in PARTIAL/EMERGENCY/OVERWRITE scopes. Example: with 7
   *    partitions x RF 3 across skewed zones [10, 2, 2, 1, 1, 1, 1, 1, 1, 1] (max feasible = 18),
   *    placing ordinal 0 of P0..P6, then ordinal 1, then ordinal 2 yields [3, 3, 3, 3, 2, 2, 2]
   *    instead of greedily giving 3rd copies to P0..P5 and starving P6 at 1 replica.
   * 2. Feasible active home priority: within the same ordinal, replicas whose prior BestPossible
   *    (or Baseline) host is still assignable and has remaining capacity/slot headroom run before
   *    homeless peers so evicted replicas cannot steal an existing resident's slot first.
   */
  private List<AssignableReplicaWithScore> buildOrderedReplicas(ClusterModel clusterModel,
      List<AssignableNode> nodes, Map<String, Float> positiveEstimateClusterRemainCap,
      boolean bestEffort) {
    if (!bestEffort) {
      return clusterModel.getAssignableReplicaMap().values().stream().flatMap(Collection::stream)
          .map(replica -> new AssignableReplicaWithScore(
              replica, clusterModel, positiveEstimateClusterRemainCap))
          .sorted().collect(Collectors.toList());
    }

    // Step 1: Seed each partition's starting ordinal with the number of replicas already retained
    // on assignable nodes (e.g. in PARTIAL, EMERGENCY, or DELAYED_REBALANCE_OVERWRITES scopes) so
    // a partition that still holds 2 surviving copies orders its missing 3rd copy at ordinal 2
    // rather than competing with completely unplaced partitions at ordinal 0.
    Map<String, Integer> ordinalByPartition = new HashMap<>();
    for (AssignableNode node : nodes) {
      for (AssignableReplica assigned : node.getAssignedReplicas()) {
        String partitionKey = assigned.getResourceName() + ":" + assigned.getPartitionName();
        ordinalByPartition.merge(partitionKey, 1, Integer::sum);
      }
    }

    // Step 2: Sort unassigned replicas canonically before assigning sequential ordinals and
    // reserving prior homes so home claims do not depend on HashSet iteration order.
    List<AssignableReplica> replicas = new ArrayList<>();
    clusterModel.getAssignableReplicaMap().values().forEach(replicas::addAll);
    replicas.sort(Comparator.comparing(AssignableReplica::getResourceName)
        .thenComparing(AssignableReplica::getPartitionName)
        .thenComparingInt(AssignableReplica::getStatePriority)
        .thenComparing(AssignableReplica::getReplicaState));

    // Step 3: Assign each replica its partition ordinal and reserve feasible prior homes in two
    // passes across all partitions:
    // - Pass 1 claims live BestPossible homes first so active residents keep first claim on their
    //   current host slots and cannot be displaced by degraded peers.
    // - Pass 2 (only when BestPossible is non-empty) lets degraded partitions whose BestPossible
    //   shrunk below Baseline claim returning Baseline-only homes on any remaining free headroom.
    ClusterContext clusterContext = clusterModel.getContext();
    boolean useBestPossibleHome = !clusterContext.getBestPossibleAssignment().isEmpty();
    List<AssignableNode> sortedNodes = sortNodesDeterministically(nodes);
    Map<String, Map<String, Integer>> remainingCapacityByInstance = new HashMap<>();
    Map<String, Integer> remainingSlotsByInstance = new HashMap<>();
    Set<String> claimedHomeKeys = new HashSet<>();
    int[] replicaOrdinals = new int[replicas.size()];
    boolean[] hasReservedHome = new boolean[replicas.size()];
    for (int i = 0; i < replicas.size(); i++) {
      AssignableReplica replica = replicas.get(i);
      String partitionKey = replica.getResourceName() + ":" + replica.getPartitionName();
      replicaOrdinals[i] = ordinalByPartition.merge(partitionKey, 1, Integer::sum) - 1;
      Map<String, String> priorStateMap = useBestPossibleHome
          ? clusterContext.getBestPossibleAssignmentStateMap(
              replica.getResourceName(), replica.getPartitionName())
          : clusterContext.getBaselineAssignmentStateMap(
              replica.getResourceName(), replica.getPartitionName());
      hasReservedHome[i] = claimFeasibleActiveHome(replica, sortedNodes, clusterContext,
          priorStateMap, remainingCapacityByInstance, remainingSlotsByInstance, claimedHomeKeys);
    }
    if (useBestPossibleHome) {
      for (int i = 0; i < replicas.size(); i++) {
        if (hasReservedHome[i]) {
          continue;
        }
        AssignableReplica replica = replicas.get(i);
        Map<String, String> bestPossibleStateMap = clusterContext.getBestPossibleAssignmentStateMap(
            replica.getResourceName(), replica.getPartitionName());
        Map<String, String> baselineStateMap = clusterContext.getBaselineAssignmentStateMap(
            replica.getResourceName(), replica.getPartitionName());
        if (bestPossibleStateMap.size() < baselineStateMap.size()) {
          Map<String, String> baselineOnlyHomes = new HashMap<>(baselineStateMap);
          baselineOnlyHomes.keySet().removeAll(bestPossibleStateMap.keySet());
          hasReservedHome[i] = claimFeasibleActiveHome(replica, sortedNodes, clusterContext,
              baselineOnlyHomes, remainingCapacityByInstance, remainingSlotsByInstance,
              claimedHomeKeys);
        }
      }
    }
    List<AssignableReplicaWithScore> orderedReplicas = new ArrayList<>(replicas.size());
    for (int i = 0; i < replicas.size(); i++) {
      orderedReplicas.add(new AssignableReplicaWithScore(replicas.get(i), clusterModel,
          positiveEstimateClusterRemainCap, true, replicaOrdinals[i], hasReservedHome[i]));
    }
    Collections.sort(orderedReplicas);
    return orderedReplicas;
  }

  /**
   * Reserves one feasible prior-home slot for {@code replica} if its previous BestPossible/Baseline
   * logicalId is still valid in the current model and within remaining host capacity and partition
   * slots. Example: if n1 (cap 2) holds {P2, P3} and n2's capacity shrinks from 2 to 1 on {P0, P1},
   * P2 and P3 reserve both slots on n1 here so they are placed before the evicted n2 replica.
   */
  private boolean claimFeasibleActiveHome(AssignableReplica replica,
      List<AssignableNode> sortedNodes, ClusterContext clusterContext,
      Map<String, String> priorStateMap,
      Map<String, Map<String, Integer>> remainingCapacityByInstance,
      Map<String, Integer> remainingSlotsByInstance, Set<String> claimedHomeKeys) {
    if (priorStateMap.isEmpty()) {
      return false;
    }
    for (AssignableNode node : sortedNodes) {
      if (!replica.getReplicaState().equals(priorStateMap.get(node.getLogicalId()))) {
        continue;
      }
      String instanceName = node.getInstanceName();
      String homeKey = instanceName + ":" + replica;
      int remainingSlots = remainingSlotsByInstance.computeIfAbsent(instanceName,
          key -> node.getMaxPartition() < 0 ? Integer.MAX_VALUE
              : Math.max(0, node.getMaxPartition() - node.getAssignedReplicaCount()));
      Map<String, Integer> remainingCapacity = remainingCapacityByInstance.computeIfAbsent(
          instanceName, key -> new HashMap<>(node.getRemainingCapacity()));
      if (remainingSlots > 0 && !claimedHomeKeys.contains(homeKey)
          && isValidOnNode(node, replica, clusterContext)
          && canFitWithin(replica, remainingCapacity)) {
        // Deduct this replica's weight and slot budget from the host so an over-subscribed host
        // (e.g. capacity shrunk from 2 to 1) only grants home priority up to its real new limit.
        replica.getCapacity().forEach((capacityKey, weight) -> remainingCapacity
            .computeIfPresent(capacityKey, (k, currentFree) -> currentFree - weight));
        remainingSlotsByInstance.put(instanceName, remainingSlots - 1);
        claimedHomeKeys.add(homeKey);
        return true;
      }
    }
    return false;
  }

  private Optional<AssignableNode> getNodeWithHighestPoints(AssignableReplica replica,
      List<AssignableNode> assignableNodes, ClusterContext clusterContext, Set<String> busyInstances,
      OptimalAssignment optimalAssignment, Set<HardConstraint.Type> blockingTypes)
      throws HelixRebalanceException {
    boolean bestEffort = clusterContext.isBestEffortPlacementEnabled();
    Map<AssignableNode, List<HardConstraint>> hardConstraintFailures = new ConcurrentHashMap<>(assignableNodes.size());

    // Execute first parallelStream within custom ForkJoinPool context
    List<AssignableNode> candidateNodes = executeInPool(
        () -> assignableNodes.parallelStream()
            .filter(candidateNode -> {
              boolean isValid = true;
              for (HardConstraint hardConstraint : _hardConstraints) {
                if (!hardConstraint.isAssignmentValid(candidateNode, replica, clusterContext)) {
                  if (!hardConstraintFailures.containsKey(candidateNode)) {
                    hardConstraintFailures.put(candidateNode, new ArrayList<>());
                  }
                  hardConstraintFailures.get(candidateNode).add(hardConstraint);
                  isValid = false;
                  break;
                }
              }
              return isValid;
            })
            .collect(Collectors.toList()),
        "hard constraint filtering"
    );

    if (candidateNodes.isEmpty()) {
      Set<HardConstraint.Type> partitionBlockingTypes = hardConstraintFailures.values().stream()
          .flatMap(List::stream)
          .map(HardConstraint::getType)
          .map(type -> type == null ? HardConstraint.Type.UNKNOWN : type)
          .collect(Collectors.toSet());
      blockingTypes.addAll(partitionBlockingTypes);
      if (!bestEffort) {
        LOG.info("Found no eligible candidate nodes. Enabling hard constraint level logging for "
            + "cluster: {}", clusterContext.getClusterName());
        enableFullLoggingForCluster();
        Consumer<HardConstraint.Type> reporter = _hardConstraintFailureReporter;
        if (reporter != null) {
          partitionBlockingTypes.forEach(reporter);
        }
        optimalAssignment.recordAssignmentFailure(replica,
            Maps.transformValues(hardConstraintFailures, this::convertFailureReasons));
      }
      return Optional.empty();
    }

    LOG.debug("Disabling hard constraint level logging for cluster: {}", clusterContext.getClusterName());
    removeFullLoggingForCluster();

    Map<String, Integer> zoneCandidateCounts =
        bestEffort ? countZones(candidateNodes) : Collections.emptyMap();

    // Execute second parallelStream within custom ForkJoinPool context
    return executeInPool(
        () -> candidateNodes.parallelStream()
            .map(node -> new HashMap.SimpleEntry<>(node,
                getAssignmentNormalizedScore(node, replica, clusterContext)))
            .max((e1, e2) -> compareCandidateNodes(e1.getKey(), e1.getValue(), e2.getKey(),
                e2.getValue(), busyInstances, zoneCandidateCounts, bestEffort))
            .map(Map.Entry::getKey),
        "node scoring and selection"
    );
  }

  private static Map<String, Integer> countZones(Collection<AssignableNode> nodes) {
    return nodes.stream().filter(AssignableNode::hasFaultZone).collect(
        Collectors.groupingBy(AssignableNode::getFaultZone, Collectors.summingInt(n -> 1)));
  }

  private static int compareCandidateNodes(AssignableNode firstNode, double firstScore,
      AssignableNode secondNode, double secondScore, Set<String> busyLogicalIds,
      Map<String, Integer> candidateCountByZone, boolean bestEffort) {
    int scoreComparison = Double.compare(firstScore, secondScore);
    if (scoreComparison == 0 && bestEffort && !candidateCountByZone.isEmpty()) {
      // Break equal soft-constraint scores toward the fault zone with more remaining candidate
      // nodes so 1-node zones are not drained early while larger zones still have candidates
      // (for example on zero-spare [4, 2, 2, 2, 2, 2, 2, 2, 2, 1] fault-zone layouts).
      scoreComparison = Integer.compare(
          candidateCountByZone.getOrDefault(firstNode.getFaultZone(), 0),
          candidateCountByZone.getOrDefault(secondNode.getFaultZone(), 0));
    }
    if (scoreComparison != 0) {
      return scoreComparison;
    }
    // Prefer an idle instance (not in previous BestPossible) over an already-busy one, then break
    // remaining ties deterministically by AssignableNode order.
    boolean firstIsIdle = !busyLogicalIds.contains(firstNode.getLogicalId());
    boolean secondIsIdle = !busyLogicalIds.contains(secondNode.getLogicalId());
    if (firstIsIdle != secondIsIdle) {
      return firstIsIdle ? 1 : -1;
    }
    return -firstNode.compareTo(secondNode);
  }

  private static List<AssignableNode> sortNodesDeterministically(List<AssignableNode> nodes) {
    List<AssignableNode> sorted = new ArrayList<>(nodes);
    sorted.sort(Comparator.comparing(AssignableNode::getLogicalId)
        .thenComparing(AssignableNode::getInstanceName));
    return sorted;
  }

  private static final class DeferredReplica {
    final AssignableReplica replica;
    Set<HardConstraint.Type> blockingTypes;

    DeferredReplica(AssignableReplica replica, Set<HardConstraint.Type> blockingTypes) {
      this.replica = replica;
      this.blockingTypes = blockingTypes;
    }
  }

  /**
   * Attempts a bounded single-hop swap when {@code replica} has no direct candidate node.
   * Example: P1 (already in zone01) loses its zone04 copy while the only free slot in the cluster
   * is zone01-spare; P0 holds zone03-n1 (SLAVE) and zone02-n1 (MASTER). Releasing P0's SLAVE from
   * zone03-n1 onto zone01-spare frees zone03-n1 so P1 can be placed without violating fault zones.
   *
   * <p>Move cost prefers zero live disruption:
   * - cost 0: victim was placed on {@code sourceHost} earlier in this same run (0 live moves)
   * - cost 1: victim is a live non-top-state replica (e.g. SLAVE / STANDBY)
   * - cost 2: victim is a live top-state replica (e.g. MASTER / LEADER)
   *
   * <p>Safety invariants on live moves ({@code moveCost > 0}):
   * 1. Disallowed in {@code DELAYED_REBALANCE_OVERWRITES} (temporary min-active top-up only adds).
   * 2. In {@code EMERGENCY} / {@code PARTIAL}, both {@code sourceHost} and {@code targetNode} must
   *    be enabled-live so a delayed-offline or disabled host is never evicted or targeted.
   * 3. Requires at least {@code max(1, minActiveReplicas)} other enabled-live copies of the victim.
   * 4. Skips target nodes that still hold any replica in BEST_POSSIBLE, non-DROPPED CURRENTSTATE,
   *    or pending transitions that is absent from the node's current model assignment, preventing
   *    2-node add-before-drop bootstrap deadlocks across multi-step state-model drops.
   */
  private boolean tryRelocateBlockingReplica(AssignableReplica replica, List<AssignableNode> nodes,
      ClusterModel clusterModel, Set<String> busyInstances, Set<String> placedOnHostThisRun,
      AtomicInteger relocationBudget) {
    ClusterContext clusterContext = clusterModel.getContext();
    Map<String, Map<String, Integer>> maxRemainingByTag = computeMaxRemainingCapacityByTag(nodes);
    if (!maxRemainingByTag.containsKey(null)) {
      return false;
    }

    ClusterModel.RebalanceScopeType scope = clusterModel.getRebalanceScopeType();
    boolean preserveLive = scope != ClusterModel.RebalanceScopeType.GLOBAL_BASELINE
        && clusterContext.hasEnabledLiveLogicalIds();
    List<AssignableNode> sortedNodes = sortNodesDeterministically(nodes);
    int evaluationLimit = Math.min(MAX_RELOCATION_EVALUATIONS, relocationBudget.get());
    int evaluations = 0;
    int resourceMaxPerInstance = replica.getResourceMaxPartitionsPerInstance();
    Map<String, Map<String, Integer>> enabledLiveCopiesByPartition = null;
    Map<String, Map<String, Set<String>>> heldPartitionsByLogicalId = null;

    AssignableNode bestSourceHost = null;
    AssignableNode bestTargetNode = null;
    AssignableReplica bestVictim = null;
    int bestCost = Integer.MAX_VALUE;
    double bestCombinedScore = Double.NEGATIVE_INFINITY;

    for (AssignableNode sourceHost : sortedNodes) {
      int hostResourceCount =
          sourceHost.getAssignedPartitionsByResource(replica.getResourceName()).size();
      if ((resourceMaxPerInstance >= 0 && hostResourceCount > resourceMaxPerInstance)
          || !isStaticallyValidOnHost(sourceHost, replica, clusterContext)) {
        continue;
      }
      boolean isHostEnabledAndLive =
          clusterContext.isEnabledLiveLogicalId(sourceHost.getLogicalId());
      boolean requireLiveTarget = preserveLive && isHostEnabledAndLive;
      List<AssignableReplica> victims = new ArrayList<>(sourceHost.getAssignedReplicas());
      victims.sort(AssignableReplica::compareTo);

      for (AssignableReplica victim : victims) {
        int moveCost =
            computeVictimMoveCost(sourceHost, victim, clusterContext, placedOnHostThisRun);
        if (moveCost > 0
            && (scope == ClusterModel.RebalanceScopeType.DELAYED_REBALANCE_OVERWRITES
                || (preserveLive && !isHostEnabledAndLive))) {
          // Never perform a live move in the temporary overwrite scope, and never evict a replica
          // from a delayed-offline or disabled sourceHost (which would copy its data onto a spare
          // while parking the incoming replica on a non-serving host).
          continue;
        }
        String victimTag =
            victim.hasResourceInstanceGroupTag() ? victim.getResourceInstanceGroupTag() : null;
        Map<String, Integer> maxDestCapacity = maxRemainingByTag.get(victimTag);
        if (maxDestCapacity == null || !canFitWithin(victim, maxDestCapacity)
            || !canSwapOnHost(sourceHost, victim, replica, resourceMaxPerInstance,
                hostResourceCount)) {
          continue;
        }
        if (moveCost > 0) {
          if (preserveLive) {
            if (enabledLiveCopiesByPartition == null) {
              enabledLiveCopiesByPartition =
                  buildEnabledLiveCopyCounts(sortedNodes, clusterContext);
            }
            int totalLiveCopies = enabledLiveCopiesByPartition
                .getOrDefault(victim.getResourceName(), Collections.emptyMap())
                .getOrDefault(victim.getPartitionName(), 0);
            int otherLiveCopies = totalLiveCopies - (isHostEnabledAndLive ? 1 : 0);
            if (otherLiveCopies < Math.max(1, victim.getResourceMinActiveReplicas())) {
              continue;
            }
          }
          if (heldPartitionsByLogicalId == null) {
            heldPartitionsByLogicalId = buildHeldPartitionsByLogicalId(clusterContext);
          }
        }
        if (evaluations >= evaluationLimit) {
          break;
        }
        evaluations++;

        SwapProbe probe = probeSingleHopSwap(clusterModel, sourceHost, victim, replica, sortedNodes,
            busyInstances, moveCost, requireLiveTarget,
            moveCost > 0 ? heldPartitionsByLogicalId : Collections.emptyMap());
        if (probe == null) {
          continue;
        }
        if (probe.moveCost < bestCost
            || (probe.moveCost == bestCost && probe.combinedScore > bestCombinedScore)) {
          bestCost = probe.moveCost;
          bestCombinedScore = probe.combinedScore;
          bestSourceHost = sourceHost;
          bestVictim = victim;
          bestTargetNode = probe.targetNode;
          if (bestCost == 0) {
            break;
          }
        }
      }
      if (bestCost == 0 || evaluations >= evaluationLimit) {
        break;
      }
    }

    if (bestSourceHost == null) {
      relocationBudget.addAndGet(-evaluations);
      return false;
    }

    clusterModel.release(bestSourceHost, bestVictim);
    clusterModel.assign(bestTargetNode, bestVictim);
    clusterModel.assign(bestSourceHost, replica);
    if (placedOnHostThisRun.remove(bestSourceHost.getInstanceName() + ":" + bestVictim)) {
      placedOnHostThisRun.add(bestTargetNode.getInstanceName() + ":" + bestVictim);
    }
    placedOnHostThisRun.add(bestSourceHost.getInstanceName() + ":" + replica);
    return true;
  }

  private static int computeVictimMoveCost(AssignableNode sourceHost, AssignableReplica victim,
      ClusterContext clusterContext, Set<String> placedOnHostThisRun) {
    String hostVictimKey = sourceHost.getInstanceName() + ":" + victim;
    boolean wasPreviouslyAssignedOnHost = clusterContext
        .getBestPossibleAssignmentStateMap(victim.getResourceName(), victim.getPartitionName())
        .containsKey(sourceHost.getLogicalId());
    return (placedOnHostThisRun.contains(hostVictimKey) && !wasPreviouslyAssignedOnHost)
        ? 0
        : (victim.isReplicaTopState() ? 2 : 1);
  }

  /**
   * Pre-indexes enabled-live replica counts per (resourceName, partitionName) once per relocation
   * call so the per-victim MIN_ACTIVE check runs in O(1) instead of scanning all nodes repeatedly.
   */
  private static Map<String, Map<String, Integer>> buildEnabledLiveCopyCounts(
      List<AssignableNode> nodes, ClusterContext clusterContext) {
    Map<String, Map<String, Integer>> countsByResource = new HashMap<>();
    for (AssignableNode node : nodes) {
      if (!clusterContext.isEnabledLiveLogicalId(node.getLogicalId())) {
        continue;
      }
      for (AssignableReplica assigned : node.getAssignedReplicas()) {
        countsByResource.computeIfAbsent(assigned.getResourceName(), key -> new HashMap<>())
            .merge(assigned.getPartitionName(), 1, Integer::sum);
      }
    }
    return countsByResource;
  }

  /**
   * Unions physically held participant replicas (non-DROPPED current state + pending transitions)
   * with the prior assignment per logicalId so multi-hop drops (ONLINE -> OFFLINE -> DROPPED) stay
   * visible across back-to-back rebalance passes until DROPPED completes.
   */
  private static Map<String, Map<String, Set<String>>> buildHeldPartitionsByLogicalId(
      ClusterContext clusterContext) {
    Map<String, Map<String, Set<String>>> heldByLogicalId = new HashMap<>();
    clusterContext.getCurrentStatePartitionsByLogicalId().forEach((logicalId, byResource) -> {
      Map<String, Set<String>> destByResource =
          heldByLogicalId.computeIfAbsent(logicalId, key -> new HashMap<>());
      byResource.forEach((resourceName, partitions) -> destByResource
          .computeIfAbsent(resourceName, key -> new HashSet<>()).addAll(partitions));
    });
    for (Map.Entry<String, ResourceAssignment> resEntry : clusterContext.getBestPossibleAssignment()
        .entrySet()) {
      String resourceName = resEntry.getKey();
      ResourceAssignment assignment = resEntry.getValue();
      for (Partition partition : assignment.getMappedPartitions()) {
        String partitionName = partition.getPartitionName();
        for (String logicalId : assignment.getReplicaMap(partition).keySet()) {
          heldByLogicalId.computeIfAbsent(logicalId, key -> new HashMap<>())
              .computeIfAbsent(resourceName, key -> new HashSet<>()).add(partitionName);
        }
      }
    }
    return heldByLogicalId;
  }

  /**
   * Returns true if {@code candidateNode} still holds any replica in
   * {@code heldPartitionsByLogicalId} that is absent from its current {@link ClusterModel}
   * assignment (regardless of remaining host headroom), avoiding 2-node add-before-drop swap
   * cycles while a replica is still departing.
   */
  private static boolean hasDepartingReplica(AssignableNode candidateNode,
      Map<String, Map<String, Set<String>>> heldPartitionsByLogicalId) {
    Map<String, Set<String>> heldByResource =
        heldPartitionsByLogicalId.get(candidateNode.getLogicalId());
    if (heldByResource == null) {
      return false;
    }
    for (Map.Entry<String, Set<String>> entry : heldByResource.entrySet()) {
      Set<String> currentlyAssigned = candidateNode.getAssignedPartitionsByResource(entry.getKey());
      for (String partitionName : entry.getValue()) {
        if (!currentlyAssigned.contains(partitionName)) {
          return true;
        }
      }
    }
    return false;
  }

  /**
   * Computes, for untagged resources (key {@code null}) and each configured instance tag, the
   * maximum remaining capacity per weight key across nodes that still have at least one open
   * partition slot. Initializes the per-tag map even when {@code INSTANCE_CAPACITY_KEYS} is empty
   * so slot-only clusters still admit single-hop relocation.
   */
  private static Map<String, Map<String, Integer>> computeMaxRemainingCapacityByTag(
      List<AssignableNode> nodes) {
    Map<String, Map<String, Integer>> maxRemainingByTag = new HashMap<>();
    for (AssignableNode node : nodes) {
      if (node.getMaxPartition() >= 0 && node.getAssignedReplicaCount() >= node.getMaxPartition()) {
        continue;
      }
      Map<String, Integer> untaggedMax =
          maxRemainingByTag.computeIfAbsent(null, tag -> new HashMap<>());
      node.getRemainingCapacity().forEach((capacityKey, freeCapacity) ->
          untaggedMax.merge(capacityKey, freeCapacity, Math::max));
      for (String tag : node.getInstanceTags()) {
        Map<String, Integer> tagMax =
            maxRemainingByTag.computeIfAbsent(tag, key -> new HashMap<>());
        node.getRemainingCapacity().forEach((capacityKey, freeCapacity) ->
            tagMax.merge(capacityKey, freeCapacity, Math::max));
      }
    }
    return maxRemainingByTag;
  }

  /**
   * Speculatively releases {@code victim} from {@code sourceHost}, checks whether {@code incoming}
   * passes all hard constraints on {@code sourceHost} and whether {@code victim} has a valid
   * destination node, then always restores {@code victim} onto {@code sourceHost} before returning.
   */
  private SwapProbe probeSingleHopSwap(ClusterModel clusterModel, AssignableNode sourceHost,
      AssignableReplica victim, AssignableReplica incoming, List<AssignableNode> sortedNodes,
      Set<String> busyInstances, int moveCost, boolean requireLiveTarget,
      Map<String, Map<String, Set<String>>> heldPartitionsByLogicalId) {
    ClusterContext clusterContext = clusterModel.getContext();
    AssignableNode targetNode = null;
    double combinedScore = Double.NEGATIVE_INFINITY;
    clusterModel.release(sourceHost, victim);
    try {
      if (isValidOnNode(sourceHost, incoming, clusterContext)) {
        targetNode = selectBestCandidateSequential(victim, sortedNodes, clusterContext,
            busyInstances, sourceHost, requireLiveTarget, heldPartitionsByLogicalId);
        if (targetNode != null) {
          combinedScore = getAssignmentNormalizedScore(targetNode, victim, clusterContext)
              + getAssignmentNormalizedScore(sourceHost, incoming, clusterContext);
        }
      }
    } finally {
      clusterModel.assign(sourceHost, victim);
    }
    return targetNode == null ? null : new SwapProbe(targetNode, moveCost, combinedScore);
  }

  private static final class SwapProbe {
    final AssignableNode targetNode;
    final int moveCost;
    final double combinedScore;

    SwapProbe(AssignableNode targetNode, int moveCost, double combinedScore) {
      this.targetNode = targetNode;
      this.moveCost = moveCost;
      this.combinedScore = combinedScore;
    }
  }

  private static boolean canSwapOnHost(AssignableNode sourceHost, AssignableReplica victim,
      AssignableReplica incoming, int resourceMaxPerInstance, int hostResourceCount) {
    if (sourceHost.getMaxPartition() >= 0
        && sourceHost.getAssignedReplicaCount() - 1 >= sourceHost.getMaxPartition()) {
      return false;
    }
    if (resourceMaxPerInstance >= 0 && hostResourceCount == resourceMaxPerInstance
        && !victim.getResourceName().equals(incoming.getResourceName())) {
      return false;
    }
    Map<String, Integer> remainingCapacity = sourceHost.getRemainingCapacity();
    Map<String, Integer> victimWeight = victim.getCapacity();
    for (Map.Entry<String, Integer> weightEntry : incoming.getCapacity().entrySet()) {
      Integer freeOnHost = remainingCapacity.get(weightEntry.getKey());
      if (freeOnHost != null
          && freeOnHost + victimWeight.getOrDefault(weightEntry.getKey(), 0)
              < weightEntry.getValue()) {
        return false;
      }
    }
    return true;
  }

  private boolean isStaticallyValidOnHost(AssignableNode sourceHost, AssignableReplica replica,
      ClusterContext clusterContext) {
    for (HardConstraint hardConstraint : _hardConstraints) {
      if (STATIC_HOST_CONSTRAINTS.contains(hardConstraint.getType())
          && !hardConstraint.isAssignmentValid(sourceHost, replica, clusterContext)) {
        return false;
      }
    }
    return true;
  }

  /**
   * Selects the highest-scoring valid destination node for a vacated {@code replica} sequentially
   * on the caller thread (avoiding nested ForkJoinPool submissions during speculative probes) while
   * applying the same fault-zone headroom tie-breaker as {@link #getNodeWithHighestPoints}.
   */
  private AssignableNode selectBestCandidateSequential(AssignableReplica replica,
      List<AssignableNode> nodes, ClusterContext clusterContext, Set<String> busyInstances,
      AssignableNode excludedHost, boolean requireLiveTarget,
      Map<String, Map<String, Set<String>>> heldPartitionsByLogicalId) {
    List<AssignableNode> candidates = new ArrayList<>();
    for (AssignableNode node : nodes) {
      if (node != excludedHost
          && (!requireLiveTarget || clusterContext.isEnabledLiveLogicalId(node.getLogicalId()))
          && !hasDepartingReplica(node, heldPartitionsByLogicalId)
          && isValidOnNode(node, replica, clusterContext)) {
        candidates.add(node);
      }
    }
    if (candidates.isEmpty()) {
      return null;
    }
    Map<String, Integer> candidateCountByZone = countZones(candidates);
    AssignableNode bestNode = null;
    double bestScore = Double.NEGATIVE_INFINITY;
    for (AssignableNode candidate : candidates) {
      double score = getAssignmentNormalizedScore(candidate, replica, clusterContext);
      if (bestNode == null || compareCandidateNodes(candidate, score, bestNode, bestScore,
          busyInstances, candidateCountByZone, true) > 0) {
        bestNode = candidate;
        bestScore = score;
      }
    }
    return bestNode;
  }

  private static boolean canFitWithin(AssignableReplica replica,
      Map<String, Integer> availableCapacity) {
    for (Map.Entry<String, Integer> weightEntry : replica.getCapacity().entrySet()) {
      Integer freeCapacity = availableCapacity.get(weightEntry.getKey());
      if (freeCapacity != null && freeCapacity < weightEntry.getValue()) {
        return false;
      }
    }
    return true;
  }

  private boolean isValidOnNode(AssignableNode node, AssignableReplica replica,
      ClusterContext clusterContext) {
    for (HardConstraint hardConstraint : _hardConstraints) {
      if (!hardConstraint.isAssignmentValid(node, replica, clusterContext)) {
        return false;
      }
    }
    return true;
  }

  private double getAssignmentNormalizedScore(AssignableNode node, AssignableReplica replica,
      ClusterContext clusterContext) {
    double sum = 0;
    for (Map.Entry<SoftConstraint, Float> softConstraintEntry : _softConstraints.entrySet()) {
      SoftConstraint softConstraint = softConstraintEntry.getKey();
      float weight = softConstraintEntry.getValue();
      if (weight != 0) {
        // Skip calculating zero weighted constraints.
        sum += weight * softConstraint.getAssignmentNormalizedScore(node, replica, clusterContext);
      }
    }
    return sum;
  }

  private List<String> convertFailureReasons(List<HardConstraint> hardConstraints) {
    return hardConstraints.stream().map(HardConstraint::getDescription)
        .collect(Collectors.toList());
  }

  /**
   * Enables logging of failures in all hard constraints
   */
  private void enableFullLoggingForCluster() {
    for (HardConstraint hardConstraint : _hardConstraints) {
      hardConstraint.setEnableLogging(true);
    }
  }

  /**
   * Removes the cluster from full logging in all hard constraints (if added previously)
   */
  private void removeFullLoggingForCluster() {
    for (HardConstraint hardConstraint : _hardConstraints) {
      hardConstraint.setEnableLogging(false);
    }
  }

  private static class AssignableReplicaWithScore implements Comparable<AssignableReplicaWithScore> {
    private final AssignableReplica _replica;
    private float _score = 0;
    private final boolean _isInBestPossibleAssignment;
    private final boolean _isInBaselineAssignment;
    private final Integer _replicaHash;
    private final boolean _bestEffort;
    private final int _replicaOrdinal;
    private final boolean _hasActiveHome;

    AssignableReplicaWithScore(AssignableReplica replica, ClusterModel clusterModel,
        Map<String, Float> overallClusterRemainingCapacityMap) {
      this(replica, clusterModel, overallClusterRemainingCapacityMap, false, 0, false);
    }

    AssignableReplicaWithScore(AssignableReplica replica, ClusterModel clusterModel,
        Map<String, Float> overallClusterRemainingCapacityMap, boolean bestEffort,
        int replicaOrdinal, boolean hasActiveHome) {
      _replica = replica;
      _isInBestPossibleAssignment = clusterModel.getContext().getBestPossibleAssignment()
          .containsKey(replica.getResourceName());
      _isInBaselineAssignment =
          clusterModel.getContext().getBaselineAssignment().containsKey(replica.getResourceName());
      _bestEffort = bestEffort;
      _replicaOrdinal = replicaOrdinal;
      _hasActiveHome = hasActiveHome;

      // _replicaHash is used to randomize the replicas order so that the same replicas are not
      // always moved in each rebalance. We only use instances which satisfy the replica's instance
      // group tag to calculate the hash code because topology changes to a single instance group tag
      // should be isolated from other instance group tags. Assignable replica ordering changes only
      // change when the topology of the instance group tag changes.
      _replicaHash = bestEffort ? 0 : Objects.hash(replica.toString(),
          clusterModel.getAssignableNodesForInstanceGroupTag(
              replica.getResourceInstanceGroupTag()));
      computeScore(overallClusterRemainingCapacityMap);
    }

    public void computeScore(Map<String, Float> positiveEstimateClusterRemainCap) {
      float score = 0;
      // score = SUM(weight * (resource_capacity/cluster_capacity) where weight = 1/(1-total_util%)
      // it could be be simplified to "resource_capacity/cluster_remainingCapacity".
      for (Map.Entry<String, Integer> resourceCapacity : _replica.getCapacity().entrySet()) {
        Float remainCap = positiveEstimateClusterRemainCap.get(resourceCapacity.getKey());
        if (!_bestEffort || (remainCap != null && remainCap > 0f)) {
          score += (float) resourceCapacity.getValue() / remainCap;
        }
      }
      _score = score;
    }

    public AssignableReplica getAssignableReplica() {
      return _replica;
    }

    @Override
    public String toString() {
      return _replica.toString();
    }

    @Override
    public int compareTo(AssignableReplicaWithScore replica2) {
      // 1. Sort according if the assignment exists in the best possible and/or baseline assignment
      if (_isInBestPossibleAssignment != replica2._isInBestPossibleAssignment) {
        // If the best possible assignment contains only one replica's assignment,
        // prioritize the replica.
        return _isInBestPossibleAssignment ? -1 : 1;
      }

      if (_isInBaselineAssignment != replica2._isInBaselineAssignment) {
        // If the baseline assignment contains only one replica's assignment, prioritize the replica.
        return _isInBaselineAssignment ? -1 : 1;
      }

      // 2. Sort according to the state priority. Or the greedy algorithm will unnecessarily shuffle
      // the states between replicas.
      int statePriority1 = _replica.getStatePriority();
      int statePriority2 = replica2._replica.getStatePriority();
      if (statePriority1 != statePriority2) {
        // Note we shall prioritize the replica with a higher state priority,
        // the smaller priority number means higher priority.
        return statePriority1 - statePriority2;
      }

      // 3. Sort according to the replica impact based on the weight.
      // So the greedy algorithm will place the replicas with larger impact first.
      int result = Float.compare(replica2._score, _score);
      if (result != 0) {
        return result;
      }

      if (_bestEffort) {
        // 3a. Among equal-weight replicas, place ordinal 0 across all partitions before ordinal 1,
        //     then ordinal 2, etc., so tight or skewed fault zones never starve later partitions
        //     below MIN_ACTIVE_REPLICAS while heavier replicas still pack first on mixed weights.
        if (_replicaOrdinal != replica2._replicaOrdinal) {
          return Integer.compare(_replicaOrdinal, replica2._replicaOrdinal);
        }
        // 3b. Within the same ordinal, place replicas that still have a valid prior home before
        //     homeless peers so evicted replicas cannot steal an existing resident's slot first.
        if (_hasActiveHome != replica2._hasActiveHome) {
          return _hasActiveHome ? -1 : 1;
        }
        // 3c. Use canonical (resource, partition, state) order instead of topology-dependent
        //     _replicaHash so adding or removing a node never reshuffles equal-weight tie-breaks.
        return _replica.compareTo(replica2._replica);
      }

      // 4. Sort according to the resource/partition name.
      // If none of the above conditions is making a difference, try to randomize the replicas
      // order.
      // Otherwise, the same replicas might always be moved in each rebalancing. This is because
      // their placement calculating will always happen at the critical moment while the cluster is
      // almost close to the expected utilization.
      //
      // Note that to ensure the algorithm is deterministic with the same inputs, do not use
      // Random functions here. Use hashcode based on the cluster topology information to get
      // a controlled randomized order is good enough.
      if (!_replicaHash.equals(replica2._replicaHash)) {
        return _replicaHash.compareTo(replica2._replicaHash);
      } else {
        // In case of hash collision, return order according to the name.
        return _replica.toString().compareTo(replica2.toString());
      }
    }
  }


  /**
   * @param assignments A collection of resource replicas assignment.
   * @return A set of logicalIds that have at least one replica assigned in the input assignments.
   */
  private Set<String> getBusyInstances(Collection<ResourceAssignment> assignments) {
    return assignments.stream().flatMap(
        resourceAssignment -> resourceAssignment.getRecord().getMapFields().values().stream()
            .flatMap(instanceStateMap -> instanceStateMap.keySet().stream())
            .collect(Collectors.toSet()).stream()).collect(Collectors.toSet());
  }

  /**
   * Executes the given operation in the constraint evaluation pool, handling exceptions and
   * wrapping them in HelixRebalanceException.
   *
   * @param operation The operation to execute.
   * @param errorContext A description of the context for error reporting.
   * @param <T> The return type of the operation.
   * @return The result of the operation.
   * @throws HelixRebalanceException If the operation fails.
   */
  private <T> T executeInPool(Supplier<T> operation, String errorContext)
      throws HelixRebalanceException {
    try {
      return _constraintEvaluationPool.submit(operation::get).join();
    } catch (CancellationException | CompletionException e) {
      LOG.error("Constraint evaluation failed during {}: {}", errorContext, e.getMessage(), e);
      throw new HelixRebalanceException(
          String.format("Failed during %s: %s", errorContext, e.getMessage()),
          HelixRebalanceException.Type.FAILED_TO_CALCULATE,
          HelixRebalanceException.FailureCategory.ALGORITHM_INTERNAL, e);
    }
  }
}
