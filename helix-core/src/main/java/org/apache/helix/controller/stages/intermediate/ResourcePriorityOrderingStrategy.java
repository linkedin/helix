package org.apache.helix.controller.stages.intermediate;

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

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.helix.controller.common.PartitionStateMap;
import org.apache.helix.controller.dataproviders.ResourceControllerDataProvider;
import org.apache.helix.controller.stages.BestPossibleStateOutput;
import org.apache.helix.controller.stages.CurrentStateOutput;
import org.apache.helix.model.IdealState;
import org.apache.helix.model.Partition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Message ordering strategy that prioritizes messages based on:
 * 1. Resource priority (from configuration)
 * 2. Partition priority (missing top state, active replicas, ideal matches)
 * 3. Message priority (state priority, preference order)
 */
public class ResourcePriorityOrderingStrategy implements MessageOrderingStrategy {
  private static final Logger logger =
      LoggerFactory.getLogger(ResourcePriorityOrderingStrategy.class.getName());

  private final ResourceControllerDataProvider _cache;
  private final BestPossibleStateOutput _bestPossibleStateOutput;
  private final CurrentStateOutput _currentStateOutput;

  public ResourcePriorityOrderingStrategy(ResourceControllerDataProvider cache,
      BestPossibleStateOutput bestPossibleStateOutput,
      CurrentStateOutput currentStateOutput) {
    _cache = cache;
    _bestPossibleStateOutput = bestPossibleStateOutput;
    _currentStateOutput = currentStateOutput;
  }

  @Override
  public void sortMessages(List<MessageContext> messages) {
    Map<String, int[]> resourcePriorityMap = buildResourcePriorityMap(messages);
    Map<String, Map<String, int[]>> partitionPriorityScores = buildPartitionPriorityScores(messages);

    messages.sort((m1, m2) -> {
      // 1. Resource priority (higher value = higher priority).
      int[] rp1 = resourcePriorityMap.getOrDefault(m1.resourceName, new int[]{Integer.MIN_VALUE, 0});
      int[] rp2 = resourcePriorityMap.getOrDefault(m2.resourceName, new int[]{Integer.MIN_VALUE, 0});
      if (rp1[0] != rp2[0]) {
        return Integer.compare(rp2[0], rp1[0]);
      }
      // Stable tiebreak within equal resource priorities: preserve original iteration order.
      if (rp1[1] != rp2[1]) {
        return Integer.compare(rp1[1], rp2[1]);
      }

      // 2. Within same resource: partition priority.
      int[] pp1 = getPartitionScore(partitionPriorityScores, m1);
      int[] pp2 = getPartitionScore(partitionPriorityScores, m2);
      for (int i = 0; i < pp1.length; i++) {
        if (pp1[i] != pp2[i]) {
          return Integer.compare(pp1[i], pp2[i]);
        }
      }

      // 3. Within same partition: message priority (state priority, preference order).
      return compareMessagePriority(m1, m2);
    });
  }

  /**
   * Builds resource priority map: resource -> [priority, insertionOrder].
   * insertionOrder preserves the original iteration order for stable tiebreaking.
   *
   * <p>TODO: Delegate priority lookup to {@link ResourceControllerDataProvider} — it should
   * encapsulate the "check ResourceConfig first, then IdealState" fallback, hiding that logic
   * from this strategy.
   */
  private Map<String, int[]> buildResourcePriorityMap(List<MessageContext> messages) {
    Map<String, int[]> priorityMap = new HashMap<>();
    String priorityField = _cache.getClusterConfig().getResourcePriorityField();

    int index = 0;
    for (MessageContext ctx : messages) {
      if (!priorityMap.containsKey(ctx.resourceName)) {
        priorityMap.put(ctx.resourceName, new int[]{Integer.MIN_VALUE, index++});
      }
    }

    if (priorityField == null) {
      return priorityMap;
    }

    for (Map.Entry<String, int[]> entry : priorityMap.entrySet()) {
      String resourceName = entry.getKey();
      String priority = null;
      if (_cache.getResourceConfig(resourceName) != null) {
        priority = _cache.getResourceConfig(resourceName).getSimpleConfig(priorityField);
      }
      if (priority == null) {
        IdealState is = _cache.getIdealState(resourceName);
        if (is != null) {
          priority = is.getRecord().getSimpleField(priorityField);
        }
      }
      if (priority != null) {
        try {
          entry.getValue()[0] = Integer.parseInt(priority);
        } catch (NumberFormatException e) {
          logger.warn("Invalid priority '{}' for resource {}", priority, resourceName);
        }
      }
    }
    return priorityMap;
  }

  /**
   * Builds partition priority scores for sorting.
   * Returns {resource -> {partition -> [missingTopState, activeReplicas, idealMatches]}}.
   */
  private Map<String, Map<String, int[]>> buildPartitionPriorityScores(
      List<MessageContext> messages) {

    Map<String, Map<String, int[]>> scores = new HashMap<>();

    for (MessageContext ctx : messages) {
      scores.computeIfAbsent(ctx.resourceName, k -> {
        Map<String, int[]> partScores = new HashMap<>();
        PartitionStateMap bestPossibleState =
            _bestPossibleStateOutput.getPartitionStateMap(ctx.resourceName);
        Map<Partition, Map<String, String>> currentStates =
            _currentStateOutput.getCurrentStateMap(ctx.resourceName);
        String topState = ctx.stateModelDef != null ? ctx.stateModelDef.getTopState() : null;

        if (bestPossibleState != null && currentStates != null && topState != null) {
          for (Map.Entry<Partition, Map<String, String>> e :
              bestPossibleState.getStateMap().entrySet()) {
            Partition p = e.getKey();
            Map<String, String> bpMap = e.getValue();
            Map<String, String> csMap = currentStates.getOrDefault(p, Collections.emptyMap());

            int missingTop = csMap.containsValue(topState) ? 1 : 0;
            int active = countActiveReplicas(bpMap, csMap);
            int matched = countIdealMatches(bpMap, csMap);
            partScores.put(p.getPartitionName(), new int[]{missingTop, active, matched});
          }
        }
        return partScores;
      });
    }
    return scores;
  }

  private int[] getPartitionScore(Map<String, Map<String, int[]>> scores, MessageContext ctx) {
    Map<String, int[]> resourceScores = scores.get(ctx.resourceName);
    if (resourceScores != null) {
      int[] score = resourceScores.get(ctx.partition.getPartitionName());
      if (score != null) {
        return score;
      }
    }
    return new int[]{0, 0, 0};
  }

  private int compareMessagePriority(MessageContext m1, MessageContext m2) {
    // 1. Same target state and both instances in preference list → preference list order.
    if (m1.message.getToState().equals(m2.message.getToState())
        && m1.preferenceList != null
        && m1.preferenceList.contains(m1.message.getTgtName())
        && m1.preferenceList.contains(m2.message.getTgtName())) {
      return Integer.compare(
          m1.preferenceList.indexOf(m1.message.getTgtName()),
          m1.preferenceList.indexOf(m2.message.getTgtName()));
    }

    // 2. Different target states → higher priority state first.
    if (!m1.message.getToState().equals(m2.message.getToState()) && m1.stateModelDef != null) {
      Map<String, Integer> statePriorityMap = m1.stateModelDef.getStatePriorityMap();
      Integer p1 = statePriorityMap.get(m1.message.getToState());
      Integer p2 = statePriorityMap.get(m2.message.getToState());
      if (p1 != null && p2 != null && !p1.equals(p2)) {
        return p1.compareTo(p2);
      }
    }

    // 3. Tiebreak: partition name (cross-resource context), then instance name.
    int partCmp = m1.partition.getPartitionName().compareTo(m2.partition.getPartitionName());
    if (partCmp != 0) {
      return partCmp;
    }
    return m1.message.getTgtName().compareTo(m2.message.getTgtName());
  }

  private int countActiveReplicas(Map<String, String> bestPossible,
      Map<String, String> currentState) {
    Map<String, Integer> stateCount = new HashMap<>();
    for (String state : bestPossible.values()) {
      stateCount.merge(state, 1, Integer::sum);
    }

    int count = 0;
    for (String state : currentState.values()) {
      if (stateCount.containsKey(state) && stateCount.get(state) > 0) {
        count++;
        stateCount.put(state, stateCount.get(state) - 1);
      }
    }
    return count;
  }

  private int countIdealMatches(Map<String, String> bestPossible,
      Map<String, String> currentState) {
    int matches = 0;
    for (Map.Entry<String, String> entry : bestPossible.entrySet()) {
      if (entry.getValue().equals(currentState.get(entry.getKey()))) {
        matches++;
      }
    }
    return matches;
  }
}
