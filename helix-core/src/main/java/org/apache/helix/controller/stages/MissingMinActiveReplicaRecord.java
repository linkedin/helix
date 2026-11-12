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
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.helix.model.Partition;

/**
 * Tracks a detected drop below minActiveReplicas. Only the initially active replicas are retained:
 * their subsequent active-to-active transitions must not be mistaken for restoring the minimum.
 * Transition timing is read from the restoring replica when recovery is observed.
 */
public class MissingMinActiveReplicaRecord {
  private final long startTimeStamp;
  private Set<String> initiallyActive;
  private Set<String> activeStates;
  private int minimum;
  private boolean attributionInvalid;

  public MissingMinActiveReplicaRecord(long start) {
    startTimeStamp = start;
  }

  /* package */ long getStartTimeStamp() {
    return startTimeStamp;
  }

  /* package */ boolean hasObservation() {
    return initiallyActive != null;
  }

  /* package */ boolean wasInitiallyActive(String instance) {
    return initiallyActive != null && initiallyActive.contains(instance);
  }

  /* package */ void observeInitialState(Map<String, String> states, Set<String> active,
      int minActiveReplicas) {
    if (initiallyActive == null) {
      initiallyActive = new HashSet<>();
      states.forEach((instance, state) -> {
        if (active.contains(state)) {
          initiallyActive.add(instance);
        }
      });
      activeStates = new HashSet<>(active);
      minimum = minActiveReplicas;
    } else {
      initiallyActive.removeIf(instance -> !active.contains(states.get(instance)));
      if (minimum != minActiveReplicas || !activeStates.equals(active)) {
        attributionInvalid = true;
      }
    }
  }

  /* package */ String findRestoringInstance(CurrentStateOutput output, String resource,
      Partition partition, Set<String> active, int minActiveReplicas) {
    if (!hasObservation() || minimum != minActiveReplicas || !activeStates.equals(active)) {
      return null;
    }
    int retained = 0;
    List<String> recovered = new ArrayList<>();
    for (Map.Entry<String, String> entry : output.getCurrentStateMap(resource, partition).entrySet()) {
      if (!active.contains(entry.getValue())) {
        continue;
      }
      if (initiallyActive.contains(entry.getKey())) {
        retained++;
      } else {
        long end = output.getEndTime(resource, partition, entry.getKey());
        if (end < startTimeStamp) {
          return null;
        }
        recovered.add(entry.getKey());
      }
    }
    int required = minActiveReplicas - retained;
    if (required <= 0 || recovered.size() < required) {
      return null;
    }
    recovered.sort(
        Comparator.comparingLong(instance -> output.getEndTime(resource, partition, instance)));
    return recovered.get(required - 1);
  }

  /* package */ void invalidateAttribution() {
    attributionInvalid = true;
  }

  /* package */ boolean isAttributionInvalid() {
    return attributionInvalid;
  }
}
