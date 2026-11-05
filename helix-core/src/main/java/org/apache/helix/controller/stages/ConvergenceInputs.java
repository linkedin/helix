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
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.helix.HelixDefinedState;
import org.apache.helix.controller.dataproviders.BaseControllerDataProvider;
import org.apache.helix.controller.rebalancer.AutoRebalancer;
import org.apache.helix.controller.rebalancer.DelayedAutoRebalancer;
import org.apache.helix.controller.rebalancer.util.DelayedRebalanceUtil;
import org.apache.helix.controller.rebalancer.util.WagedValidationUtil;
import org.apache.helix.controller.rebalancer.waged.WagedRebalanceStatus;
import org.apache.helix.controller.rebalancer.waged.WagedRebalancer;
import org.apache.helix.model.ClusterConfig;
import org.apache.helix.model.ConvergenceStatus.Reason;
import org.apache.helix.model.IdealState;
import org.apache.helix.model.InstanceConfig;
import org.apache.helix.model.Message;
import org.apache.helix.model.Resource;
import org.apache.helix.model.StateModelDefinition;

/**
 * Everything the calculation reads. The event outputs are read-only once the pipeline stage that
 * produced them is done; the controller cache is mutable, so {@link #from} copies what it needs.
 */
public final class ConvergenceInputs {
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

  public ConvergenceInputs controllerName(String controllerName) {
    _controllerName = controllerName;
    return this;
  }

  public ConvergenceInputs clusterName(String clusterName) {
    _clusterName = clusterName;
    return this;
  }

  public ConvergenceInputs resources(Map<String, Resource> resources) {
    _resources = resources;
    return this;
  }

  public ConvergenceInputs currentState(CurrentStateOutput currentState) {
    _currentState = currentState;
    return this;
  }

  public ConvergenceInputs bestPossible(BestPossibleStateOutput bestPossible) {
    _bestPossible = bestPossible;
    return this;
  }

  public ConvergenceInputs messages(MessageOutput all, MessageOutput selected,
      MessageOutput throttled) {
    _allMessages = all;
    _selectedMessages = selected;
    _throttledMessages = throttled;
    return this;
  }

  public ConvergenceInputs failedMessages(List<Message> failedMessages) {
    _failedMessages = failedMessages;
    return this;
  }

  /** When set, every partition is reported PAUSED with this reason and nothing is compared. */
  public ConvergenceInputs pausedReason(Reason reason) {
    _pausedReason = reason;
    return this;
  }

  public ConvergenceInputs liveInstances(Set<String> liveInstances) {
    _liveInstances = liveInstances;
    return this;
  }

  public ConvergenceInputs disabledInstances(Set<String> disabledInstances) {
    _disabledInstances = disabledInstances;
    return this;
  }

  public ConvergenceInputs initialStates(Map<String, String> initialStates) {
    _initialStates = initialStates;
    return this;
  }

  public ConvergenceInputs wagedResources(Set<String> wagedResources,
      WagedRebalanceStatus wagedStatus) {
    _wagedResources = wagedResources;
    _wagedStatus = wagedStatus;
    return this;
  }

  public ConvergenceInputs delayedResources(Set<String> delayedResources) {
    _delayedResources = delayedResources;
    return this;
  }

  public ConvergenceInputs expectedReplicas(Map<String, Integer> expectedReplicas,
      Map<String, IdealState> customized, Set<String> swapInstances) {
    _expectedReplicas = expectedReplicas;
    _customized = customized;
    _swapInstances = swapInstances;
    return this;
  }

  public ConvergenceInputs placement(Set<String> semiAuto, Map<String, Integer> waitingForDelay,
      Map<String, Map<String, Set<String>>> disabledPartitions) {
    _semiAuto = semiAuto;
    _waitingForDelay = waitingForDelay;
    _disabledPartitions = disabledPartitions;
    return this;
  }

  /** Snapshots the event on the pipeline thread: O(resources + instances x instance tags). */
  @SuppressWarnings("unchecked")
  public static ConvergenceInputs from(ClusterEvent event, BaseControllerDataProvider cache,
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
    return new ConvergenceInputs().clusterName(clusterName).resources(resources)
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

  public ConvergenceInputs coveredThrough(long coveredThrough) {
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
