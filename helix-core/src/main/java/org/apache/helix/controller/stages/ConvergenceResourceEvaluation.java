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
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.helix.HelixDefinedState;
import org.apache.helix.model.ConvergenceStatus.Reason;
import org.apache.helix.model.ConvergenceStatus.Status;
import org.apache.helix.model.Message;
import org.apache.helix.model.Partition;
import org.apache.helix.model.Resource;

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
    int total = _tally.total();
    _tally.reset();
    _tally.record(Status.UNKNOWN, reason, total);
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
    } else if (comparison == SAME && !active) {
      boolean held = holdsOfflineNode(partition);
      _tally.record(held ? Status.IN_PROGRESS : Status.CONVERGED,
          held ? Reason.WAITING_FOR_DELAY : Reason.NONE);
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

  /** Best possible lists live nodes only; a node held for the delay window is in the list. */
  private boolean holdsOfflineNode(Partition partition) {
    List<String> held = _delayed && _in._bestPossible != null
        ? _in._bestPossible.getPreferenceList(_name, partition.getPartitionName()) : null;
    return held != null && !_in._liveInstances.containsAll(held);
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
    for (Map.Entry<String, String> entry : current.entrySet()) {
      if (isBlank(entry.getKey()) || isBlank(entry.getValue())) {
        return INVALID;
      }
      if (!ignored(entry.getValue())) {
        currentEffective++;
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
    return same && currentEffective == targetEffective ? SAME : DIFFERENT;
  }

  private boolean ignored(String state) {
    return state.equalsIgnoreCase(HelixDefinedState.DROPPED.name())
        || _initialState != null && state.equalsIgnoreCase(_initialState);
  }

  private static boolean isBlank(String value) {
    return value == null || value.isEmpty();
  }

  private static <K, V> Map<K, V> orEmpty(Map<K, V> map) {
    return map == null ? Collections.emptyMap() : map;
  }
}
