package org.apache.helix.controller.stages;

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
import java.util.Arrays;
import java.util.Collections;
import java.util.EnumMap;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;

import org.apache.helix.HelixDefinedState;
import org.apache.helix.HelixRebalanceException;
import org.apache.helix.controller.dataproviders.BaseControllerDataProvider;
import org.apache.helix.controller.rebalancer.AutoRebalancer;
import org.apache.helix.controller.rebalancer.DelayedAutoRebalancer;
import org.apache.helix.controller.rebalancer.util.DelayedRebalanceUtil;
import org.apache.helix.controller.rebalancer.util.WagedValidationUtil;
import org.apache.helix.controller.rebalancer.waged.WagedRebalanceStatus;
import org.apache.helix.controller.rebalancer.waged.WagedRebalancer;
import org.apache.helix.model.ClusterConfig;
import org.apache.helix.model.ConvergenceStatus;
import org.apache.helix.model.ConvergenceStatus.OptimizerStatus;
import org.apache.helix.model.ConvergenceStatus.Reason;
import org.apache.helix.model.ConvergenceStatus.Status;
import org.apache.helix.model.ConvergenceStatus.TargetFreshness;
import org.apache.helix.model.IdealState;
import org.apache.helix.model.InstanceConfig;
import org.apache.helix.model.Message;
import org.apache.helix.model.Partition;
import org.apache.helix.model.Resource;
import org.apache.helix.model.ResourceConfig;
import org.apache.helix.model.StateModelDefinition;
import org.apache.helix.task.TaskConstants;

/**
 * Decides, partition by partition, whether the current assignment has reached the target: one
 * pass with no allocation per partition, off the pipeline thread, memory bounded by the counters.
 */
public final class ConvergenceStatusCalculator {
  private static final int STATUS_COUNT = Status.values().length;
  private static final int REASON_COUNT = Reason.values().length;

  private ConvergenceStatusCalculator() {
  }

  /**
   * Everything the calculation reads. The event outputs are read-only once the pipeline stage that
   * produced them is done; the controller cache is mutable, so {@link #from} copies what it needs.
   */
  public static final class Inputs {
    String _clusterName;
    Map<String, Resource> _resources = Collections.emptyMap();
    CurrentStateOutput _currentState = new CurrentStateOutput();
    BestPossibleStateOutput _bestPossible;
    MessageOutput _allMessages;
    MessageOutput _selectedMessages;
    MessageOutput _throttledMessages;
    List<Message> _failedMessages = Collections.emptyList();
    Reason _pausedReason;
    Set<String> _liveInstances = Collections.emptySet();
    Set<String> _disabledInstances = Collections.emptySet();
    Map<String, String> _initialStates = Collections.emptyMap();
    Set<String> _wagedResources = Collections.emptySet();
    Set<String> _delayedResources = Collections.emptySet();
    // Active replicas each partition should have, for resources placed by a built-in rebalancer
    // where that can be told.
    Map<String, Integer> _expectedReplicas = Collections.emptyMap();
    // SEMI_AUTO resources, which place as many replicas as each preference list names.
    Set<String> _semiAuto = Collections.emptySet();
    // Delayed resources that can be placed on an unavailable node still inside its delay window,
    // with the replicas the rebalancer keeps active while it waits (min active replicas).
    Map<String, Integer> _waitingForDelay = Collections.emptyMap();
    // Resource to partition to instances an operator disabled that partition on.
    Map<String, Map<String, Set<String>>> _disabledPartitions = Collections.emptyMap();
    // The newest ideal state creation time among the resources this run covers.
    long _coveredThrough;
    // CUSTOMIZED resources, whose ideal state names the replicas directly.
    Map<String, IdealState> _customized = Collections.emptyMap();
    // Both sides of a swap; a replica on either is expected outside the preference list.
    Set<String> _swapInstances = Collections.emptySet();
    WagedRebalanceStatus _wagedStatus;
    String _controllerName;

    public Inputs controllerName(String controllerName) {
      _controllerName = controllerName;
      return this;
    }

    public Inputs clusterName(String clusterName) {
      _clusterName = clusterName;
      return this;
    }

    public Inputs resources(Map<String, Resource> resources) {
      _resources = resources;
      return this;
    }

    public Inputs currentState(CurrentStateOutput currentState) {
      _currentState = currentState;
      return this;
    }

    public Inputs bestPossible(BestPossibleStateOutput bestPossible) {
      _bestPossible = bestPossible;
      return this;
    }

    public Inputs messages(MessageOutput all, MessageOutput selected, MessageOutput throttled) {
      _allMessages = all;
      _selectedMessages = selected;
      _throttledMessages = throttled;
      return this;
    }

    public Inputs failedMessages(List<Message> failedMessages) {
      _failedMessages = failedMessages;
      return this;
    }

    /** When set, every partition is reported PAUSED with this reason and nothing is compared. */
    public Inputs pausedReason(Reason reason) {
      _pausedReason = reason;
      return this;
    }

    public Inputs liveInstances(Set<String> liveInstances) {
      _liveInstances = liveInstances;
      return this;
    }

    public Inputs disabledInstances(Set<String> disabledInstances) {
      _disabledInstances = disabledInstances;
      return this;
    }

    public Inputs initialStates(Map<String, String> initialStates) {
      _initialStates = initialStates;
      return this;
    }

    public Inputs wagedResources(Set<String> wagedResources, WagedRebalanceStatus wagedStatus) {
      _wagedResources = wagedResources;
      _wagedStatus = wagedStatus;
      return this;
    }

    public Inputs delayedResources(Set<String> delayedResources) {
      _delayedResources = delayedResources;
      return this;
    }

    public Inputs expectedReplicas(Map<String, Integer> expectedReplicas,
        Map<String, IdealState> customized, Set<String> swapInstances) {
      _expectedReplicas = expectedReplicas;
      _customized = customized;
      _swapInstances = swapInstances;
      return this;
    }

    public Inputs placement(Set<String> semiAuto, Map<String, Integer> waitingForDelay,
        Map<String, Map<String, Set<String>>> disabledPartitions) {
      _semiAuto = semiAuto;
      _waitingForDelay = waitingForDelay;
      _disabledPartitions = disabledPartitions;
      return this;
    }

    /** Snapshots the event on the pipeline thread: O(resources + instances x instance tags). */
    @SuppressWarnings("unchecked")
    public static Inputs from(ClusterEvent event, BaseControllerDataProvider cache,
        String clusterName) {
      Map<String, Resource> resources =
          event.getAttribute(AttributeName.RESOURCES_TO_REBALANCE.name());
      Map<String, String> initialStates = new HashMap<>();
      Set<String> waged = new HashSet<>();
      Set<String> delayed = new HashSet<>();
      Map<String, Integer> expected = new HashMap<>();
      Map<String, IdealState> customized = new HashMap<>();
      Set<String> semiAuto = new HashSet<>();
      Map<String, Integer> waiting = new HashMap<>();
      ClusterConfig clusterConfig = cache.getClusterConfig();
      Map<String, Long> unavailableSince = unavailableSince(cache);
      // Most resources share a few instance tags, so each tag's instances are walked once a run.
      Map<String, TagView> tags = new HashMap<>();
      long now = System.currentTimeMillis();
      long coveredThrough = 0L;
      for (Map.Entry<String, Resource> entry : resources.entrySet()) {
        StateModelDefinition stateModel =
            cache.getStateModelDef(entry.getValue().getStateModelDefRef());
        if (stateModel != null) {
          initialStates.put(entry.getKey(), stateModel.getInitialState());
        }
        IdealState idealState = cache.getIdealState(entry.getKey());
        if (idealState == null) {
          continue;
        }
        coveredThrough = Math.max(coveredThrough, idealState.getRecord().getCreationTime());
        if (stateModel == null || !idealState.isEnabled()) {
          continue;
        }
        String name = entry.getKey();
        boolean isWaged = WagedValidationUtil.isWagedEnabled(idealState);
        if (isWaged) {
          waged.add(name);
        }
        IdealState.RebalanceMode mode = idealState.getRebalanceMode();
        TagView tag = tags.computeIfAbsent(idealState.getInstanceGroupTag(),
            t -> new TagView(cache, t, unavailableSince));
        // Only FULL_AUTO holds a replica for a node through the delay window, and only for a node
        // the resource can be placed on.
        if (mode == IdealState.RebalanceMode.FULL_AUTO
            && DelayedRebalanceUtil.isDelayRebalanceEnabled(idealState, clusterConfig)) {
          delayed.add(name);
          long delay = DelayedRebalanceUtil.getRebalanceDelay(idealState, clusterConfig);
          for (long since : tag._unavailableSince) {
            if (since <= 0 || now - since < delay) {
              waiting.put(name, DelayedRebalanceUtil.getMinActiveReplica(idealState,
                  idealState.getReplicaCount(tag._assignable.size())));
              break;
            }
          }
        }
        if (mode == IdealState.RebalanceMode.CUSTOMIZED) {
          customized.put(name, idealState);
        } else if (mode == IdealState.RebalanceMode.SEMI_AUTO
            || mode == IdealState.RebalanceMode.FULL_AUTO && isBuiltIn(idealState)) {
          if (mode == IdealState.RebalanceMode.SEMI_AUTO) {
            semiAuto.add(name);
          }
          int replicas = expectedReplicas(idealState, stateModel, tag._assignable.size(),
              isWaged || clusterConfig != null && clusterConfig.isTopologyAwareEnabled()
                  ? tag.faultZones(cache, clusterConfig) : Integer.MAX_VALUE);
          if (replicas > 0) {
            expected.put(name, replicas);
          }
        }
      }
      Set<String> swapInstances = new HashSet<>();
      cache.getSwapInToSwapOutInstancePairs().forEach((in, out) -> {
        swapInstances.add(in);
        swapInstances.add(out);
      });
      Object rebalancer = event.getAttribute(AttributeName.STATEFUL_REBALANCER.name());
      WagedRebalanceStatus wagedStatus = rebalancer instanceof WagedRebalancer
          ? ((WagedRebalancer) rebalancer).getConvergenceStatus() : null;
      CurrentStateOutput currentState = event.getAttribute(AttributeName.CURRENT_STATE.name());
      List<Message> failed = event.getAttribute(AttributeName.MESSAGES_DISPATCH_FAILED.name());
      return new Inputs().clusterName(clusterName).resources(resources)
          .currentState(currentState == null ? new CurrentStateOutput() : currentState)
          .bestPossible(event.getAttribute(AttributeName.BEST_POSSIBLE_STATE.name()))
          .messages(event.getAttribute(AttributeName.MESSAGES_ALL.name()),
              event.getAttribute(AttributeName.MESSAGES_SELECTED_SNAPSHOT.name()),
              event.getAttribute(AttributeName.MESSAGES_THROTTLE.name()))
          .failedMessages(failed == null ? Collections.emptyList() : failed)
          .pausedReason(cache.isMaintenanceModeEnabled() ? Reason.MAINTENANCE_MODE : null)
          .liveInstances(new HashSet<>(cache.getLiveInstances().keySet()))
          .disabledInstances(new HashSet<>(cache.getDisabledInstances()))
          .initialStates(initialStates).wagedResources(waged, wagedStatus)
          .delayedResources(delayed).expectedReplicas(expected, customized, swapInstances)
          .placement(semiAuto, waiting, disabledPartitions(cache)).coveredThrough(coveredThrough);
    }

    public Inputs coveredThrough(long coveredThrough) {
      _coveredThrough = coveredThrough;
      return this;
    }

    private static final Set<String> BUILT_IN_REBALANCERS = new HashSet<>(Arrays.asList(
        AutoRebalancer.class.getName(), DelayedAutoRebalancer.class.getName(),
        WagedRebalancer.class.getName()));

    /** Whose preference lists and placement this calculation understands. */
    private static boolean isBuiltIn(IdealState idealState) {
      String rebalancer = idealState.getRebalancerClassName();
      return rebalancer == null || BUILT_IN_REBALANCERS.contains(rebalancer);
    }

    /** When each unavailable instance went down or was disabled; 0 when not known. */
    private static Map<String, Long> unavailableSince(BaseControllerDataProvider cache) {
      Map<String, Long> since = new HashMap<>();
      Map<String, Long> offline = cache.getInstanceOfflineTimeMap();
      Set<String> live = cache.getLiveInstances().keySet();
      for (InstanceConfig config : cache.getInstanceConfigMap().values()) {
        String instance = config.getInstanceName();
        if (!live.contains(instance)) {
          Long time = offline == null ? null : offline.get(instance);
          since.put(instance, time == null ? 0L : time);
        } else if (cache.getDisabledInstances().contains(instance)) {
          since.put(instance, Math.max(0L, config.getInstanceEnabledTime()));
        }
      }
      return since;
    }

    /** The instances a resource with one instance tag, or none, can be placed on. */
    private static final class TagView {
      final Set<String> _assignable;
      // When each of those that is unavailable went down; 0 when not known.
      final List<Long> _unavailableSince = new ArrayList<>();
      private int _faultZones = -1;

      TagView(BaseControllerDataProvider cache, String tag, Map<String, Long> unavailableSince) {
        _assignable = tag == null ? cache.getAssignableInstances()
            : cache.getAssignableInstancesWithTag(tag);
        unavailableSince.forEach((instance, since) -> {
          if (_assignable.contains(instance)) {
            _unavailableSince.add(since);
          }
        });
      }

      int faultZones(BaseControllerDataProvider cache, ClusterConfig clusterConfig) {
        if (_faultZones < 0) {
          String zoneType = clusterConfig.getFaultZoneType();
          Set<String> zones = new HashSet<>();
          if (zoneType != null) {
            for (String instance : _assignable) {
              InstanceConfig config = cache.getInstanceConfigMap().get(instance);
              String zone = config == null ? null : config.getDomainAsMap().get(zoneType);
              if (zone != null) {
                zones.add(zone);
              }
            }
          }
          _faultZones = zones.isEmpty() ? Integer.MAX_VALUE : zones.size();
        }
        return _faultZones;
      }
    }

    private static Map<String, Map<String, Set<String>>> disabledPartitions(
        BaseControllerDataProvider cache) {
      Map<String, Map<String, Set<String>>> disabled = new HashMap<>();
      for (InstanceConfig config : cache.getInstanceConfigMap().values()) {
        for (Map.Entry<String, List<String>> entry : config.getDisabledPartitionsMap().entrySet()) {
          for (String partition : entry.getValue()) {
            disabled.computeIfAbsent(entry.getKey(), k -> new HashMap<>())
                .computeIfAbsent(partition, k -> new HashSet<>()).add(config.getInstanceName());
          }
        }
      }
      return disabled;
    }

    /**
     * The active replicas a partition should have: the ideal state's replica count, capped by what
     * the state model can hold, by the instances the resource may be placed on, dead or alive, and
     * by the fault zones when placement is zone aware. -1 when it cannot be told, such as
     * ANY_LIVEINSTANCE or a state bounded by N.
     */
    static int expectedReplicas(IdealState idealState, StateModelDefinition stateModel,
        int instances, int faultZones) {
      int replicas;
      try {
        replicas = Integer.parseInt(idealState.getReplicas());
      } catch (NumberFormatException | NullPointerException e) {
        return -1;
      }
      int capacity = 0;
      for (String state : stateModel.getStatesPriorityList()) {
        if (state.equals(stateModel.getInitialState())
            || state.equals(HelixDefinedState.DROPPED.name())
            || state.equals(HelixDefinedState.ERROR.name())) {
          continue;
        }
        String bound = stateModel.getNumInstancesPerState(state);
        if (bound == null) {
          continue;
        }
        if ("R".equals(bound)) {
          capacity += replicas;
        } else if ("N".equals(bound)) {
          return -1;
        } else {
          try {
            // A state with no bound declares -1; it adds nothing.
            capacity += Math.max(0, Integer.parseInt(bound));
          } catch (NumberFormatException e) {
            return -1;
          }
        }
      }
      return Math.min(Math.min(replicas, capacity), Math.min(instances, faultZones));
    }
  }

  public static final class Result {
    private final ConvergenceStatus _cluster;
    private final Map<String, ConvergenceStatus> _resources;

    Result(ConvergenceStatus cluster, Map<String, ConvergenceStatus> resources) {
      _cluster = cluster;
      _resources = resources;
    }

    public ConvergenceStatus getCluster() {
      return _cluster;
    }

    public Map<String, ConvergenceStatus> getResources() {
      return _resources;
    }
  }

  public static Result calculate(Inputs in, String controllerSessionId, long now) {
    Set<String> failedKeys = new HashSet<>();
    for (Message message : in._failedMessages) {
      List<String> names = message.getPartitionNames();
      if (names.isEmpty() && message.getPartitionName() != null) {
        failedKeys.add(key(message.getResourceName(), message.getPartitionName()));
      }
      for (String name : names) {
        failedKeys.add(key(message.getResourceName(), name));
      }
    }

    Map<String, ConvergenceStatus> reports = new TreeMap<>();
    Tally clusterTally = new Tally();
    TargetFreshness clusterFreshness = TargetFreshness.CURRENT;
    OptimizerStatus clusterOptimizer = OptimizerStatus.NOT_APPLICABLE;

    for (Resource resource : in._resources.values()) {
      if (TaskConstants.STATE_MODEL_NAME.equals(resource.getStateModelDefRef())) {
        continue;
      }
      ResourceEvaluation evaluation = new ResourceEvaluation(in, resource, failedKeys);
      evaluation.run();

      TargetFreshness freshness = in._pausedReason != null ? TargetFreshness.UNKNOWN
          : TargetFreshness.CURRENT;
      OptimizerStatus optimizer = in._pausedReason != null ? OptimizerStatus.UNKNOWN
          : OptimizerStatus.NOT_APPLICABLE;
      if (in._pausedReason == null && in._wagedResources.contains(resource.getResourceName())) {
        WagedRebalanceStatus waged = in._wagedStatus;
        boolean known = waged != null && waged.isOutcomeKnown();
        optimizer = !known ? OptimizerStatus.UNKNOWN : waged.isBaselineComputationFailed()
            ? OptimizerStatus.BASELINE_FAILED : OptimizerStatus.HEALTHY;
        Reason override = null;
        if (waged == null) {
          freshness = TargetFreshness.UNKNOWN;
          override = Reason.WAGED_INTERNAL_FAILURE;
        } else if (!known) {
          // A new leader has not finished a run yet, so there is no verified target to compare to.
          freshness = TargetFreshness.UNKNOWN;
          override = Reason.TARGET_ASSIGNMENT_MISSING;
        } else if (waged.isLastKnownGoodFallback()) {
          freshness = TargetFreshness.LAST_KNOWN_GOOD;
          override = wagedReason(waged, Reason.WAGED_LAST_KNOWN_GOOD);
        } else if (waged.isServingComputationFailed()) {
          freshness = TargetFreshness.UNKNOWN;
          override = wagedReason(waged, Reason.WAGED_INTERNAL_FAILURE);
        }
        if (override != null) {
          evaluation.overrideAllUnknown(override);
        }
      }

      ConvergenceStatus report = new ConvergenceStatus(resource.getResourceName());
      fill(report, evaluation._tally, in._controllerName, controllerSessionId, now);
      report.setTargetFreshness(freshness);
      report.setOptimizerStatus(optimizer);
      reports.put(resource.getResourceName(), report);

      clusterTally.add(evaluation._tally);
      clusterFreshness = worse(clusterFreshness, freshness);
      clusterOptimizer = worse(clusterOptimizer, optimizer);
    }

    ConvergenceStatus cluster = new ConvergenceStatus(in._clusterName);
    fill(cluster, clusterTally, in._controllerName, controllerSessionId, now);
    cluster.setResourceCount(reports.size());
    cluster.setCoveredThrough(in._coveredThrough);
    cluster.setTargetFreshness(clusterFreshness);
    cluster.setOptimizerStatus(clusterOptimizer);
    if (in._pausedReason != null) {
      // The cluster record alone carries a pause, even for a cluster with no partitions.
      cluster.setStatus(Status.PAUSED);
      cluster.setPrimaryReason(in._pausedReason);
      cluster.setTargetFreshness(TargetFreshness.UNKNOWN);
      cluster.setOptimizerStatus(OptimizerStatus.UNKNOWN);
    }
    return new Result(cluster, Collections.unmodifiableMap(reports));
  }

  private static void fill(ConvergenceStatus report, Tally tally, String controllerName,
      String sessionId, long now) {
    Status status = tally.overallStatus();
    report.setStatus(status);
    report.setPrimaryReason(tally.primaryReason(status));
    report.setStatusCounts(tally.statusCounts());
    report.setReasonCounts(tally.reasonCounts());
    report.setGeneratedAt(now);
    report.setControllerSessionId(sessionId);
    report.setControllerName(controllerName);
  }

  private static final String ANY_LIVEINSTANCE =
      ResourceConfig.ResourceConfigConstants.ANY_LIVEINSTANCE.name();

  /** The walk over one resource. */
  private static final class ResourceEvaluation {
    private final Inputs _in;
    private final Resource _resource;
    private final String _name;
    private final Set<String> _failedKeys;
    private final String _initialState;
    private final boolean _hasStateModel;
    private final boolean _delayed;
    private final Map<Partition, Map<String, String>> _target;
    // Resolved once per resource so each partition costs one lookup per map.
    private final Map<Partition, Map<String, String>> _current;
    private final Map<Partition, Map<String, Message>> _pending;
    private final Map<Partition, Map<String, Message>> _cancellations;
    private final Map<Partition, Map<String, Message>> _relays;
    private final Tally _tally = new Tally();

    ResourceEvaluation(Inputs in, Resource resource, Set<String> failedKeys) {
      _in = in;
      _resource = resource;
      _name = resource.getResourceName();
      _failedKeys = failedKeys;
      _initialState = in._initialStates.get(_name);
      _hasStateModel = in._initialStates.containsKey(_name);
      _delayed = in._delayedResources.contains(_name);
      _target = in._bestPossible == null ? null : in._bestPossible.getResourceMap(_name);
      _current = in._currentState.getCurrentStateMap(_name);
      _pending = in._currentState.getPendingMessageMap(_name);
      _cancellations = in._currentState.getCancellationMessageMap(_name);
      _relays = in._currentState.getPendingRelayMessageMap(_name);
    }

    void run() {
      for (Partition partition : _resource.getPartitions()) {
        visit(partition);
      }
      // Partitions that exist only in the current or target state, such as ones being dropped.
      for (Partition partition : _current.keySet()) {
        if (_resource.getPartition(partition.getPartitionName()) == null) {
          visit(partition);
        }
      }
      if (_target != null) {
        for (Partition partition : _target.keySet()) {
          if (_resource.getPartition(partition.getPartitionName()) == null
              && !_current.containsKey(partition)) {
            visit(partition);
          }
        }
      }
    }

    void overrideAllUnknown(Reason reason) {
      int errors = _tally._counts[Status.BLOCKED.ordinal()][Reason.ERROR_STATE.ordinal()];
      int total = _tally.total();
      _tally.reset();
      _tally.record(Status.BLOCKED, Reason.ERROR_STATE, errors);
      _tally.record(Status.UNKNOWN, reason, total - errors);
    }

    private void visit(Partition partition) {
      if (_in._pausedReason != null) {
        _tally.record(Status.PAUSED, _in._pausedReason);
        return;
      }
      Map<String, String> target = _target == null ? null : _target.get(partition);
      if (!_hasStateModel || target == null) {
        _tally.record(Status.UNKNOWN, Reason.TARGET_ASSIGNMENT_MISSING);
        return;
      }

      Map<String, String> current = orEmpty(_current.get(partition));
      Map<String, Message> pending = orEmpty(_pending.get(partition));
      Map<String, Message> cancellations = orEmpty(_cancellations.get(partition));
      Map<String, Message> relays = orEmpty(_relays.get(partition));
      boolean active = !pending.isEmpty() || !cancellations.isEmpty() || !relays.isEmpty();

      int comparison = compare(current, target);
      if (comparison == INVALID) {
        _tally.record(Status.UNKNOWN, Reason.INVALID_STATE);
      } else if (comparison == SAME && !active && !hasMessages(_in._allMessages, partition)) {
        if (holdsOfflineNode(partition)) {
          _tally.record(Status.IN_PROGRESS, Reason.WAITING_FOR_DELAY);
        } else {
          // The target is what the rebalancer could do, which already leaves out dead nodes and
          // full ones, so matching it is not enough.
          Reason gap = placementGap(partition, target);
          _tally.record(gap == null ? Status.CONVERGED
                  : gap == Reason.WAITING_FOR_DELAY ? Status.IN_PROGRESS : Status.BLOCKED,
              gap == null ? Reason.NONE : gap);
        }
      } else {
        classifyUnconverged(partition, current, target, pending, cancellations, relays);
      }
    }

    private void classifyUnconverged(Partition partition, Map<String, String> current,
        Map<String, String> target, Map<String, Message> pending,
        Map<String, Message> cancellations, Map<String, Message> relays) {
      Reason progress = null;
      if (!cancellations.isEmpty()) {
        progress = Reason.CANCELLATION_PENDING;
      } else if (!relays.isEmpty()) {
        progress = Reason.RELAY_PENDING;
      } else if (!pending.isEmpty()) {
        progress = Reason.PENDING_TRANSITION;
      } else if (!_failedKeys.isEmpty()
          && _failedKeys.contains(key(_name, partition.getPartitionName()))) {
        _tally.record(Status.BLOCKED, Reason.MESSAGE_DISPATCH_FAILED);
        return;
      } else if (hasMessages(_in._throttledMessages, partition)) {
        progress = Reason.TRANSITION_DISPATCHED;
      } else if (hasMessages(_in._selectedMessages, partition)) {
        progress = Reason.MESSAGE_THROTTLED;
      } else if (hasMessages(_in._allMessages, partition)) {
        progress = Reason.STATE_CONSTRAINT_WAIT;
      }
      if (progress != null) {
        _tally.record(Status.IN_PROGRESS, progress);
        return;
      }

      if (current.containsValue(HelixDefinedState.ERROR.name())) {
        _tally.record(Status.BLOCKED, Reason.ERROR_STATE);
      } else if (hasNonLiveTarget(target) || holdsOfflineNode(partition)) {
        _tally.record(_delayed ? Status.IN_PROGRESS : Status.BLOCKED,
            _delayed ? Reason.WAITING_FOR_DELAY : Reason.TARGET_INSTANCE_NOT_LIVE);
      } else {
        _tally.record(Status.BLOCKED, Reason.NO_PROGRESS_PATH);
      }
    }

    /** Best possible skips an offline or disabled node the preference list keeps for the delay. */
    private boolean holdsOfflineNode(Partition partition) {
      return _delayed && hasUnavailable(preferenceList(partition));
    }

    /** Null when there is none or it is the ANY_LIVEINSTANCE placeholder. */
    private List<String> preferenceList(Partition partition) {
      List<String> list = _in._bestPossible == null ? null
          : _in._bestPossible.getPreferenceList(_name, partition.getPartitionName());
      return list == null || list.contains(ANY_LIVEINSTANCE) ? null : list;
    }

    private boolean hasUnavailable(List<String> instances) {
      if (instances != null) {
        for (String instance : instances) {
          if (!_in._liveInstances.contains(instance)
              || _in._disabledInstances.contains(instance)) {
            return true;
          }
        }
      }
      return false;
    }

    /**
     * Why a partition that matches its target is still short of what was asked for, or null:
     * fewer active replicas than asked for, a CUSTOMIZED replica on a node that is down, or a
     * replica left outside the preference list. Replicas an operator disabled are not asked for.
     */
    private Reason placementGap(Partition partition, Map<String, String> target) {
      String name = partition.getPartitionName();
      Set<String> disabled = disabledFor(name);
      int active = 0;
      for (String state : target.values()) {
        if (!ignored(state)) {
          active++;
        }
      }
      IdealState custom = _in._customized.get(_name);
      if (custom != null) {
        Map<String, String> wanted = custom.getInstanceStateMap(name);
        int expected = 0;
        if (wanted != null) {
          for (Map.Entry<String, String> entry : wanted.entrySet()) {
            if (!ignored(entry.getValue()) && !disabled.contains(entry.getKey())) {
              expected++;
            }
          }
        }
        return active < expected ? Reason.TARGET_INSTANCE_NOT_LIVE : null;
      }
      Integer asked = _in._expectedReplicas.get(_name);
      if (asked == null) {
        return null;
      }
      List<String> preference = preferenceList(partition);
      int expected = asked;
      int excused = 0;
      if (preference != null) {
        if (_in._semiAuto.contains(_name)) {
          expected = Math.min(expected, preference.size());
        }
        for (String instance : preference) {
          if (disabled.contains(instance)) {
            excused++;
          }
        }
      }
      if (active < expected - excused) {
        // Below min active the rebalancer drops the held node and does not wait, so a replica
        // it could not place is missing, not held.
        Integer minActive = _in._waitingForDelay.get(_name);
        if (minActive != null && active >= minActive) {
          return Reason.WAITING_FOR_DELAY;
        }
        return hasNonLive(preference) ? Reason.TARGET_INSTANCE_NOT_LIVE : Reason.UNDER_REPLICATED;
      }
      // A replica kept off the list because the new target is disabled is the operator's doing.
      if (preference != null && !preference.isEmpty() && excused == 0) {
        for (Map.Entry<String, String> entry : target.entrySet()) {
          if (!ignored(entry.getValue()) && !preference.contains(entry.getKey())
              && !_in._swapInstances.contains(entry.getKey())) {
            return Reason.OUTSIDE_PREFERENCE_LIST;
          }
        }
      }
      return null;
    }

    /** Instances disabled as a whole, or for this partition. */
    private Set<String> disabledFor(String partition) {
      Map<String, Set<String>> byPartition = _in._disabledPartitions.get(_name);
      Set<String> partitionDisabled = byPartition == null ? null : byPartition.get(partition);
      if (partitionDisabled == null) {
        return _in._disabledInstances;
      }
      Set<String> all = new HashSet<>(_in._disabledInstances);
      all.addAll(partitionDisabled);
      return all;
    }

    private boolean hasNonLive(List<String> instances) {
      if (instances != null) {
        for (String instance : instances) {
          if (!_in._liveInstances.contains(instance)) {
            return true;
          }
        }
      }
      return false;
    }

    private boolean hasNonLiveTarget(Map<String, String> target) {
      for (Map.Entry<String, String> entry : target.entrySet()) {
        if (!ignored(entry.getValue()) && !_in._liveInstances.contains(entry.getKey())) {
          return true;
        }
      }
      return false;
    }

    private boolean hasMessages(MessageOutput output, Partition partition) {
      return output != null && !output.getMessages(_name, partition).isEmpty();
    }

    private static final int SAME = 0;
    private static final int DIFFERENT = 1;
    private static final int INVALID = -1;

    /** Compares assignments, ignoring DROPPED and initial state entries, without allocating. */
    private int compare(Map<String, String> current, Map<String, String> target) {
      int currentEffective = 0;
      boolean errored = false;
      for (Map.Entry<String, String> entry : current.entrySet()) {
        if (isBlank(entry.getKey()) || isBlank(entry.getValue())) {
          return INVALID;
        }
        if (!ignored(entry.getValue())) {
          currentEffective++;
        }
        // The rebalancer keeps an ERROR replica in ERROR, so it would otherwise match the target.
        if (HelixDefinedState.ERROR.name().equals(entry.getValue())) {
          errored = true;
        }
      }
      int targetEffective = 0;
      boolean same = true;
      for (Map.Entry<String, String> entry : target.entrySet()) {
        if (isBlank(entry.getKey()) || isBlank(entry.getValue())) {
          return INVALID;
        }
        if (!ignored(entry.getValue())) {
          targetEffective++;
          same &= entry.getValue().equals(current.get(entry.getKey()));
        }
      }
      return same && !errored && currentEffective == targetEffective ? SAME : DIFFERENT;
    }

    private boolean ignored(String state) {
      return state.equalsIgnoreCase(HelixDefinedState.DROPPED.name())
          || _initialState != null && state.equalsIgnoreCase(_initialState);
    }

    private static boolean isBlank(String value) {
      return value == null || value.isEmpty();
    }
  }

  /** Counts of partitions per status, and per reason for the ones that are not converged. */
  private static final class Tally {
    private final int[][] _counts = new int[STATUS_COUNT][REASON_COUNT];

    void record(Status status, Reason reason) {
      _counts[status.ordinal()][reason.ordinal()]++;
    }

    void record(Status status, Reason reason, int count) {
      _counts[status.ordinal()][reason.ordinal()] += count;
    }

    void add(Tally other) {
      for (int s = 0; s < STATUS_COUNT; s++) {
        for (int r = 0; r < REASON_COUNT; r++) {
          _counts[s][r] += other._counts[s][r];
        }
      }
    }

    void reset() {
      for (int[] row : _counts) {
        Arrays.fill(row, 0);
      }
    }

    int total() {
      int total = 0;
      for (int[] row : _counts) {
        for (int count : row) {
          total += count;
        }
      }
      return total;
    }

    Map<Status, Integer> statusCounts() {
      Map<Status, Integer> result = new EnumMap<>(Status.class);
      for (Status status : Status.values()) {
        int sum = 0;
        for (int count : _counts[status.ordinal()]) {
          sum += count;
        }
        if (sum > 0) {
          result.put(status, sum);
        }
      }
      return result;
    }

    Map<Reason, Integer> reasonCounts() {
      Map<Reason, Integer> result = new EnumMap<>(Reason.class);
      for (Reason reason : Reason.values()) {
        if (reason == Reason.NONE) {
          continue;
        }
        int sum = 0;
        for (int s = 0; s < STATUS_COUNT; s++) {
          sum += _counts[s][reason.ordinal()];
        }
        if (sum > 0) {
          result.put(reason, sum);
        }
      }
      return result;
    }

    /** The most severe status that has any partition. */
    Status overallStatus() {
      Status result = Status.CONVERGED;
      for (Status status : Status.values()) {
        if (severity(status) > severity(result) && sum(status) > 0) {
          result = status;
        }
      }
      return result;
    }

    /** The most urgent reason among the partitions that carry the overall status. */
    Reason primaryReason(Status status) {
      Reason best = Reason.NONE;
      for (Reason reason : Reason.values()) {
        if (reason != Reason.NONE && _counts[status.ordinal()][reason.ordinal()] > 0
            && (best == Reason.NONE || priority(reason) < priority(best))) {
          best = reason;
        }
      }
      return best;
    }

    private int sum(Status status) {
      int sum = 0;
      for (int count : _counts[status.ordinal()]) {
        sum += count;
      }
      return sum;
    }
  }

  static int severity(Status status) {
    switch (status) {
      case PAUSED:
        return 5;
      case BLOCKED:
        return 4;
      case UNKNOWN:
        return 3;
      case IN_PROGRESS:
        return 2;
      default:
        return 1;
    }
  }

  /** Lower is more urgent. */
  static int priority(Reason reason) {
    switch (reason) {
      case MANAGEMENT_MODE:
      case MAINTENANCE_MODE:
        return 1;
      case WAGED_LAST_KNOWN_GOOD:
      case WAGED_CAPACITY_DEFICIT:
      case WAGED_NO_CANDIDATE_NODE:
      case WAGED_INVALID_CONFIGURATION:
      case WAGED_INTERNAL_FAILURE:
        return 2;
      case TARGET_ASSIGNMENT_MISSING:
      case INVALID_STATE:
      case ERROR_STATE:
        return 3;
      case MESSAGE_DISPATCH_FAILED:
      case NO_PROGRESS_PATH:
      case UNDER_REPLICATED:
      case OUTSIDE_PREFERENCE_LIST:
        return 4;
      case TARGET_INSTANCE_NOT_LIVE:
        return 5;
      case MESSAGE_THROTTLED:
      case STATE_CONSTRAINT_WAIT:
        return 6;
      default:
        return 7;
    }
  }

  private static TargetFreshness worse(TargetFreshness a, TargetFreshness b) {
    return a.ordinal() >= b.ordinal() ? a : b;
  }

  private static OptimizerStatus worse(OptimizerStatus a, OptimizerStatus b) {
    return rank(a) >= rank(b) ? a : b;
  }

  private static int rank(OptimizerStatus status) {
    switch (status) {
      case BASELINE_FAILED:
        return 3;
      case UNKNOWN:
        return 2;
      case HEALTHY:
        return 1;
      default:
        return 0;
    }
  }

  private static Reason wagedReason(WagedRebalanceStatus status, Reason fallback) {
    HelixRebalanceException.FailureCategory category = status.getServingFailureCategory();
    if (category == null) {
      return fallback;
    }
    switch (category) {
      case CAPACITY_DEFICIT:
        return Reason.WAGED_CAPACITY_DEFICIT;
      case NO_CANDIDATE_NODE:
        return Reason.WAGED_NO_CANDIDATE_NODE;
      case INVALID_RESOURCE_CONFIG:
      case INVALID_CLUSTER_CONFIG:
        return Reason.WAGED_INVALID_CONFIGURATION;
      default:
        return Reason.WAGED_INTERNAL_FAILURE;
    }
  }

  private static String key(String resource, String partition) {
    return resource + '\u0000' + partition;
  }

  private static <K, V> Map<K, V> orEmpty(Map<K, V> map) {
    return map == null ? Collections.emptyMap() : map;
  }
}
