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
import org.apache.helix.controller.stages.StateTransitionHelper;
import org.apache.helix.model.IdealState;
import org.apache.helix.model.Partition;
import org.apache.helix.model.StateModelDefinition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Message ordering strategy that prioritizes messages based on:
 * 1. Resource priority (from configuration)
 * 2. Partition priority (missing top state, active replicas, ideal matches)
 * 3. Message priority (state priority, preference order)
 *
 * <p><b>Relationship to {@code PartitionPriorityComparator}:</b> The partition-level scoring in
 * this class (missing top state → fewer active replicas → fewer ideal matches) mirrors the logic
 * in {@code IntermediateStateCalcStage.PartitionPriorityComparator}. That comparator operates
 * per-resource within the existing per-resource throttle pipeline. This strategy extends the same
 * logic to operate cross-resource and adds resource-level priority on top. The per-resource
 * comparator is expected to be retired in favour of this strategy in Part 2 (see PR #119).
 *
 * <p><b>Thread safety:</b> Not thread-safe. Create a new instance per pipeline run.
 */
public class ResourcePriorityOrderingStrategy implements MessageOrderingStrategy {
  private static final Logger logger =
      LoggerFactory.getLogger(ResourcePriorityOrderingStrategy.class.getName());

  /**
   * Fallback priority used when a resource has no configured priority value.
   * {@code buildResourcePriorityMap} pre-populates entries for all resources seen in the message
   * list, so this default is only reached if a resource appears in the comparator but was absent
   * from the original list — an edge case that should not occur in normal operation.
   */
  private static final int DEFAULT_RESOURCE_PRIORITY = Integer.MIN_VALUE;

  /**
   * Fallback partition score used when a partition is absent from the pre-computed score map.
   * Scores are {@code [missingTopState=0, activeReplicas=0, idealMatches=0]}, which places the
   * partition at the same level as a fully-degraded partition — a safe conservative default.
   */
  private static final int[] DEFAULT_PARTITION_SCORE = {0, 0, 0};

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
    Map<String, Integer> resourcePriorityMap = buildResourcePriorityMap(messages);
    Map<String, Map<String, int[]>> partitionPriorityScores = buildPartitionPriorityScores(messages);

    messages.sort((m1, m2) -> {
      // 1. Resource priority (higher value = higher priority).
      int p1 = resourcePriorityMap.getOrDefault(m1.resourceName, DEFAULT_RESOURCE_PRIORITY);
      int p2 = resourcePriorityMap.getOrDefault(m2.resourceName, DEFAULT_RESOURCE_PRIORITY);
      if (p1 != p2) {
        return Integer.compare(p2, p1);
      }
      // Tiebreak within equal resource priorities: alphabetical by resource name.
      int nameCmp = m1.resourceName.compareTo(m2.resourceName);
      if (nameCmp != 0) {
        return nameCmp;
      }

      // 2. Within same resource: partition priority.
      int[] pp1 = getPartitionScore(partitionPriorityScores, m1);
      int[] pp2 = getPartitionScore(partitionPriorityScores, m2);
      for (int i = 0; i < pp1.length; i++) {
        if (pp1[i] != pp2[i]) {
          return Integer.compare(pp1[i], pp2[i]);
        }
      }

      // 2a. Partition scores tie → tiebreak by partition name.
      // Mirrors PartitionPriorityComparator in IntermediateStateCalcStage which also uses
      // partition-name alphabetical as the final tiebreak after the three partition scores.
      int partitionCmp = m1.partition.getPartitionName().compareTo(m2.partition.getPartitionName());
      if (partitionCmp != 0) {
        return partitionCmp;
      }

      // 3. Same partition: message priority (state priority, preference order).
      return compareMessagePriority(m1, m2);
    });
  }

  /**
   * Builds resource priority map: resource -> configuredPriority.
   * Resources without a configured priority value default to {@link Integer#MIN_VALUE}.
   * When two resources share the same priority, the caller tiebreaks alphabetically by name.
   *
   * <p>TODO: Delegate priority lookup to {@link ResourceControllerDataProvider} — it should
   * encapsulate the "check ResourceConfig first, then IdealState" fallback, hiding that logic
   * from this strategy.
   */
  private Map<String, Integer> buildResourcePriorityMap(List<MessageContext> messages) {
    Map<String, Integer> priorityMap = new HashMap<>();
    String priorityField = _cache.getClusterConfig().getResourcePriorityField();

    for (MessageContext ctx : messages) {
      priorityMap.putIfAbsent(ctx.resourceName, Integer.MIN_VALUE);
    }

    if (priorityField == null) {
      return priorityMap;
    }

    for (Map.Entry<String, Integer> entry : priorityMap.entrySet()) {
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
          entry.setValue(Integer.parseInt(priority));
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
        IdealState idealState = _cache.getIdealState(ctx.resourceName);
        StateModelDefinition stateModelDef = idealState != null
            ? _cache.getStateModelDef(idealState.getStateModelDefRef()) : null;
        String topState = stateModelDef != null ? stateModelDef.getTopState() : null;

        if (bestPossibleState != null && currentStates != null && topState != null) {
          for (Map.Entry<Partition, Map<String, String>> e :
              bestPossibleState.getStateMap().entrySet()) {
            Partition p = e.getKey();
            Map<String, String> bpMap = e.getValue();
            Map<String, String> csMap = currentStates.getOrDefault(p, Collections.emptyMap());

            int missingTop = csMap.containsValue(topState) ? 1 : 0;
            int active = StateTransitionHelper.countActiveReplicas(bpMap, csMap);
            int matched = StateTransitionHelper.countIdealMatches(bpMap, csMap);
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
    return DEFAULT_PARTITION_SCORE;
  }

  /**
   * Compares two messages from the <em>same</em> partition.
   * Callers guarantee same-partition by applying the partition-name tiebreak in
   * {@link #sortMessages} before reaching this method.
   *
   * <p>Mirrors {@code IntermediateStateCalcStage.MessagePriorityComparator}:
   * <ol>
   *   <li>Same target state + both instances in the preference list → preference-list order.</li>
   *   <li>Different target states → higher-priority state (lower numeric value) first.</li>
   *   <li>Tiebreak: instance name alphabetical.</li>
   * </ol>
   */
  private int compareMessagePriority(MessageContext m1, MessageContext m2) {
    // 1. Same target state + both instances in preference list → preference-list position order.
    if (m1.message.getToState().equals(m2.message.getToState())
        && m1.preferenceList != null
        && m1.preferenceList.contains(m1.message.getTgtName())
        && m1.preferenceList.contains(m2.message.getTgtName())) {
      return Integer.compare(
          m1.preferenceList.indexOf(m1.message.getTgtName()),
          m1.preferenceList.indexOf(m2.message.getTgtName()));
    }

    // 2. Different target states → higher priority state first (lower numeric value = higher priority).
    if (!m1.message.getToState().equals(m2.message.getToState())) {
      IdealState idealState = _cache.getIdealState(m1.resourceName);
      StateModelDefinition stateModelDef = idealState != null
          ? _cache.getStateModelDef(idealState.getStateModelDefRef()) : null;
      if (stateModelDef != null) {
        Map<String, Integer> statePriorityMap = stateModelDef.getStatePriorityMap();
        Integer p1 = statePriorityMap.get(m1.message.getToState());
        Integer p2 = statePriorityMap.get(m2.message.getToState());
        if (p1 != null && p2 != null && !p1.equals(p2)) {
          return p1.compareTo(p2);
        }
      }
    }

    // 3. Tiebreak: instance name alphabetical.
    return m1.message.getTgtName().compareTo(m2.message.getTgtName());
  }

}
