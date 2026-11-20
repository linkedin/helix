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
  private static final int MAX_RELOCATION_EVALUATIONS = 256;
  private static final int MAX_TOTAL_UNSUCCESSFUL_RELOCATION_EVALUATIONS = 1024;
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
    Set<String> failedRelocationKeys = bestEffort ? new HashSet<>() : Collections.emptySet();
    AtomicInteger relocationBudget =
        new AtomicInteger(MAX_TOTAL_UNSUCCESSFUL_RELOCATION_EVALUATIONS);
    int unplacedReplicas = 0;
    AssignableReplica firstUnplaced = null;

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
          if (relocationBudget.get() > 0 && !failedRelocationKeys.contains(replica.toString())
              && tryRelocateBlockingReplica(replica, nodes, clusterModel, busyInstances,
                  placedOnHostThisRun, relocationBudget)) {
            failedRelocationKeys.clear();
            continue;
          }
          failedRelocationKeys.add(replica.toString());
          blockingTypes.addAll(replicaBlocking);
          if (_hardConstraintFailureReporter != null) {
            replicaBlocking.forEach(_hardConstraintFailureReporter);
          }
          if (firstUnplaced == null) firstUnplaced = replica;
          unplacedReplicas++;
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
        failedRelocationKeys.clear();
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

  private List<AssignableReplicaWithScore> buildOrderedReplicas(ClusterModel clusterModel,
      List<AssignableNode> nodes, Map<String, Float> positiveEstimateClusterRemainCap,
      boolean bestEffort) {
    if (!bestEffort) {
      return clusterModel.getAssignableReplicaMap().values().stream().flatMap(Collection::stream)
          .map(r -> new AssignableReplicaWithScore(r, clusterModel, positiveEstimateClusterRemainCap))
          .sorted().collect(Collectors.toList());
    }
    Map<String, Integer> ordinalByPartition = new HashMap<>();
    for (AssignableNode n : nodes) {
      for (AssignableReplica a : n.getAssignedReplicas()) {
        ordinalByPartition.merge(a.getResourceName() + ":" + a.getPartitionName(), 1, Integer::sum);
      }
    }
    List<AssignableReplica> replicas = new ArrayList<>();
    clusterModel.getAssignableReplicaMap().values().forEach(replicas::addAll);
    replicas.sort(Comparator.comparing(AssignableReplica::getResourceName)
        .thenComparing(AssignableReplica::getPartitionName)
        .thenComparingInt(AssignableReplica::getStatePriority)
        .thenComparing(AssignableReplica::getReplicaState));
    ClusterContext ctx = clusterModel.getContext();
    boolean hasBp = !ctx.getBestPossibleAssignment().isEmpty();
    List<AssignableNode> sortedNodes = sortNodesDeterministically(nodes);
    Map<String, Map<String, Integer>> homeCap = new HashMap<>();
    Map<String, Integer> homeSlots = new HashMap<>();
    Set<String> claimedHomes = new HashSet<>();
    List<AssignableReplicaWithScore> result = new ArrayList<>(replicas.size());
    for (AssignableReplica r : replicas) {
      int ordinal = ordinalByPartition.merge(
          r.getResourceName() + ":" + r.getPartitionName(), 1, Integer::sum) - 1;
      Map<String, String> stateMap = hasBp
          ? ctx.getBestPossibleAssignmentStateMap(r.getResourceName(), r.getPartitionName())
          : ctx.getBaselineAssignmentStateMap(r.getResourceName(), r.getPartitionName());
      boolean homed = claimFeasibleActiveHome(
          r, sortedNodes, ctx, stateMap, homeCap, homeSlots, claimedHomes);
      result.add(new AssignableReplicaWithScore(
          r, clusterModel, positiveEstimateClusterRemainCap, true, ordinal, homed));
    }
    Collections.sort(result);
    return result;
  }

  private boolean claimFeasibleActiveHome(AssignableReplica r, List<AssignableNode> sortedNodes,
      ClusterContext ctx, Map<String, String> stateMap, Map<String, Map<String, Integer>> homeCap,
      Map<String, Integer> homeSlots, Set<String> claimed) {
    if (stateMap.isEmpty()) return false;
    for (AssignableNode n : sortedNodes) {
      if (!r.getReplicaState().equals(stateMap.get(n.getLogicalId()))) continue;
      String inst = n.getInstanceName(), homeKey = inst + ":" + r;
      int slots = homeSlots.computeIfAbsent(inst, k -> n.getMaxPartition() < 0
          ? Integer.MAX_VALUE : Math.max(0, n.getMaxPartition() - n.getAssignedReplicaCount()));
      Map<String, Integer> rem =
          homeCap.computeIfAbsent(inst, k -> new HashMap<>(n.getRemainingCapacity()));
      if (slots > 0 && !claimed.contains(homeKey) && isValidOnNode(n, r, ctx)
          && canFitWithin(r, rem)) {
        r.getCapacity().forEach((k, val) -> rem.computeIfPresent(k, (ck, v) -> v - val));
        homeSlots.put(inst, slots - 1);
        claimed.add(homeKey);
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
        LOG.info("Found no eligible candidate nodes. Enabling hard constraint level logging for cluster: {}", clusterContext.getClusterName());
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

  private static int compareCandidateNodes(AssignableNode n1, double s1, AssignableNode n2,
      double s2, Set<String> busy, Map<String, Integer> zoneCounts, boolean bestEffort) {
    int cmp = Double.compare(s1, s2);
    if (cmp == 0 && bestEffort && !zoneCounts.isEmpty()) {
      cmp = Integer.compare(zoneCounts.getOrDefault(n1.getFaultZone(), 0),
          zoneCounts.getOrDefault(n2.getFaultZone(), 0));
    }
    if (cmp != 0) return cmp;
    int i1 = busy.contains(n1.getLogicalId()) ? 0 : 1;
    int i2 = busy.contains(n2.getLogicalId()) ? 0 : 1;
    return i1 != i2 ? (i1 - i2) : -n1.compareTo(n2);
  }

  private static List<AssignableNode> sortNodesDeterministically(List<AssignableNode> nodes) {
    List<AssignableNode> sorted = new ArrayList<>(nodes);
    sorted.sort(Comparator.comparing(AssignableNode::getLogicalId)
        .thenComparing(AssignableNode::getInstanceName));
    return sorted;
  }

  private boolean tryRelocateBlockingReplica(AssignableReplica replica, List<AssignableNode> nodes,
      ClusterModel clusterModel, Set<String> busyInstances, Set<String> placedOnHostThisRun,
      AtomicInteger relocationBudget) {
    ClusterContext ctx = clusterModel.getContext();
    Map<String, Map<String, Integer>> maxRemByTag = new HashMap<>();
    for (AssignableNode node : nodes) {
      if (node.getMaxPartition() < 0 || node.getAssignedReplicaCount() < node.getMaxPartition()) {
        node.getRemainingCapacity().forEach((k, v) -> maxRemByTag
            .computeIfAbsent(null, t -> new HashMap<>()).merge(k, v, Math::max));
        for (String tag : node.getInstanceTags()) {
          node.getRemainingCapacity().forEach((k, v) -> maxRemByTag
              .computeIfAbsent(tag, t -> new HashMap<>()).merge(k, v, Math::max));
        }
      }
    }
    if (!maxRemByTag.containsKey(null)) return false;
    boolean preserveLive = clusterModel.getRebalanceScopeType()
        != ClusterModel.RebalanceScopeType.GLOBAL_BASELINE && ctx.hasEnabledLiveLogicalIds();
    List<AssignableNode> sortedNodes = sortNodesDeterministically(nodes);
    AssignableNode bestHost = null, bestTarget = null;
    AssignableReplica bestVictim = null;
    int bestCost = Integer.MAX_VALUE, evaluations = 0;
    int limit = Math.min(MAX_RELOCATION_EVALUATIONS, relocationBudget.get());
    double bestCombinedScore = Double.NEGATIVE_INFINITY;
    int resMax = replica.getResourceMaxPartitionsPerInstance();
    for (AssignableNode host : sortedNodes) {
      int hostResCount = host.getAssignedPartitionsByResource(replica.getResourceName()).size();
      if ((resMax >= 0 && hostResCount > resMax) || !isStaticallyValidOnHost(host, replica, ctx)) {
        continue;
      }
      boolean reqLive = preserveLive && ctx.isEnabledLiveLogicalId(host.getLogicalId());
      List<AssignableReplica> victims = new ArrayList<>(host.getAssignedReplicas());
      victims.sort(AssignableReplica::compareTo);
      for (AssignableReplica victim : victims) {
        Map<String, Integer> maxRem = maxRemByTag.get(
            victim.hasResourceInstanceGroupTag() ? victim.getResourceInstanceGroupTag() : null);
        if (maxRem == null || !canFitWithin(victim, maxRem)
            || !canSwapOnHost(host, victim, replica, resMax, hostResCount)) {
          continue;
        }
        if (evaluations++ >= limit) break;
        AssignableNode target = null;
        double combinedScore = Double.NEGATIVE_INFINITY;
        clusterModel.release(host, victim);
        try {
          if (isValidOnNode(host, replica, ctx)) {
            target = selectBestCandidateSequential(
                victim, sortedNodes, ctx, busyInstances, host, reqLive);
            if (target != null) {
              combinedScore = getAssignmentNormalizedScore(target, victim, ctx)
                  + getAssignmentNormalizedScore(host, replica, ctx);
            }
          }
        } finally {
          clusterModel.assign(host, victim);
        }
        if (target == null) continue;
        String hvKey = host.getInstanceName() + ":" + victim;
        boolean wasLive = ctx.getBestPossibleAssignmentStateMap(
            victim.getResourceName(), victim.getPartitionName()).containsKey(host.getLogicalId());
        int cost = (placedOnHostThisRun.contains(hvKey) && !wasLive) ? 0
            : (victim.isReplicaTopState() ? 2 : 1);
        if (cost < bestCost || (cost == bestCost && combinedScore > bestCombinedScore)) {
          bestCost = cost; bestCombinedScore = combinedScore;
          bestHost = host; bestVictim = victim; bestTarget = target;
          if (bestCost == 0) break;
        }
      }
      if (bestCost == 0 || evaluations >= limit) break;
    }
    if (bestHost == null) {
      relocationBudget.addAndGet(-evaluations);
      return false;
    }
    clusterModel.release(bestHost, bestVictim);
    clusterModel.assign(bestTarget, bestVictim);
    clusterModel.assign(bestHost, replica);
    if (placedOnHostThisRun.remove(bestHost.getInstanceName() + ":" + bestVictim)) {
      placedOnHostThisRun.add(bestTarget.getInstanceName() + ":" + bestVictim);
    }
    placedOnHostThisRun.add(bestHost.getInstanceName() + ":" + replica);
    return true;
  }

  private static boolean canSwapOnHost(AssignableNode host, AssignableReplica victim,
      AssignableReplica incoming, int resMax, int hostResCount) {
    if ((host.getMaxPartition() >= 0 && host.getAssignedReplicaCount() - 1 >= host.getMaxPartition())
        || (resMax >= 0 && hostResCount == resMax
            && !victim.getResourceName().equals(incoming.getResourceName()))) {
      return false;
    }
    Map<String, Integer> rem = host.getRemainingCapacity(), vCap = victim.getCapacity();
    for (Map.Entry<String, Integer> e : incoming.getCapacity().entrySet()) {
      Integer r = rem.get(e.getKey());
      if (r != null && r + vCap.getOrDefault(e.getKey(), 0) < e.getValue()) return false;
    }
    return true;
  }

  private static boolean canFitWithin(AssignableReplica r, Map<String, Integer> maxRem) {
    for (Map.Entry<String, Integer> e : r.getCapacity().entrySet()) {
      Integer m = maxRem.get(e.getKey());
      if (m != null && m < e.getValue()) return false;
    }
    return true;
  }

  private boolean isStaticallyValidOnHost(AssignableNode host, AssignableReplica replica,
      ClusterContext ctx) {
    for (HardConstraint hc : _hardConstraints) {
      if (STATIC_HOST_CONSTRAINTS.contains(hc.getType())
          && !hc.isAssignmentValid(host, replica, ctx)) {
        return false;
      }
    }
    return true;
  }

  private boolean isValidOnNode(AssignableNode node, AssignableReplica replica, ClusterContext ctx) {
    for (HardConstraint hc : _hardConstraints) if (!hc.isAssignmentValid(node, replica, ctx)) return false;
    return true;
  }

  private AssignableNode selectBestCandidateSequential(AssignableReplica replica,
      List<AssignableNode> nodes, ClusterContext ctx, Set<String> busy,
      AssignableNode excluded, boolean reqLive) {
    List<AssignableNode> cands = nodes.stream().filter(n -> n != excluded
        && (!reqLive || ctx.isEnabledLiveLogicalId(n.getLogicalId()))
        && isValidOnNode(n, replica, ctx)).collect(Collectors.toList());
    Map<String, Integer> zc = countZones(cands);
    AssignableNode best = null;
    double bestScore = Double.NEGATIVE_INFINITY;
    for (AssignableNode c : cands) {
      double s = getAssignmentNormalizedScore(c, replica, ctx);
      if (best == null || compareCandidateNodes(c, s, best, bestScore, busy, zc, true) > 0) {
        best = c; bestScore = s;
      }
    }
    return best;
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

      if (_bestEffort) {
        if (_replicaOrdinal != replica2._replicaOrdinal) {
          return Integer.compare(_replicaOrdinal, replica2._replicaOrdinal);
        }
        if (_hasActiveHome != replica2._hasActiveHome) {
          return _hasActiveHome ? -1 : 1;
        }
      }

      // 3. Sort according to the replica impact based on the weight.
      // So the greedy algorithm will place the replicas with larger impact first.
      int result = Float.compare(replica2._score, _score);
      if (result != 0) {
        return result;
      }

      if (_bestEffort) {
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
