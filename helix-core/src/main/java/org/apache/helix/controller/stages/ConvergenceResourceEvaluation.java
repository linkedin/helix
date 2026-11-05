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

import static org.apache.helix.controller.stages.ConvergenceStatusCalculator.key;

import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.helix.HelixDefinedState;
import org.apache.helix.model.ConvergenceStatus.Reason;
import org.apache.helix.model.ConvergenceStatus.Status;
import org.apache.helix.model.IdealState;
import org.apache.helix.model.Message;
import org.apache.helix.model.Partition;
import org.apache.helix.model.Resource;
import org.apache.helix.model.ResourceConfig;

/** The walk over one resource. */
final class ConvergenceResourceEvaluation {
  private final ConvergenceInputs _in;
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
  final ConvergenceTally _tally = new ConvergenceTally();

  ConvergenceResourceEvaluation(ConvergenceInputs in, Resource resource, Set<String> failedKeys) {
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
    int errors = _tally.count(Status.BLOCKED, Reason.ERROR_STATE);
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
    // An ERROR replica needs a reset whatever else is in flight, so it comes first.
    if (current.containsValue(HelixDefinedState.ERROR.name())) {
      _tally.record(Status.BLOCKED, Reason.ERROR_STATE);
      return;
    }
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

    if (hasNonLiveTarget(target) || holdsOfflineNode(partition)) {
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

  private static final String ANY_LIVEINSTANCE =
      ResourceConfig.ResourceConfigConstants.ANY_LIVEINSTANCE.name();

  private static <K, V> Map<K, V> orEmpty(Map<K, V> map) {
    return map == null ? Collections.emptyMap() : map;
  }
}
