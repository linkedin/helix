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

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import org.apache.helix.HelixDefinedState;
import org.apache.helix.HelixException;
import org.apache.helix.HelixManager;
import org.apache.helix.controller.LogUtil;
import org.apache.helix.controller.common.PartitionStateMap;
import org.apache.helix.controller.dataproviders.ResourceControllerDataProvider;
import org.apache.helix.controller.pipeline.AbstractBaseStage;
import org.apache.helix.controller.pipeline.StageException;
import org.apache.helix.model.BuiltInStateModelDefinitions;
import org.apache.helix.model.IdealState;
import org.apache.helix.model.MaintenanceSignal;
import org.apache.helix.model.Partition;
import org.apache.helix.model.Resource;
import org.apache.helix.monitoring.mbeans.ClusterStatusMonitor;
import org.apache.helix.monitoring.mbeans.ResourceMonitor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * For partition compute the Intermediate State (instance,state) pair based on the BestPossibleState
 * and CurrentState, with all constraints applied (such as state transition throttling).
 *
 * <p>This stage acts as a thin orchestrator that delegates to the appropriate
 * {@link IntermediateStateComputationStrategy} implementation based on cluster configuration:
 * <ul>
 *   <li>{@link ResourcePriorityIntermediateStateCalculator} - Traditional resource-priority-based
 *       computation (default)</li>
 *   <li>{@link AvailabilityAwareIntermediateStateCalculator} - Cross-resource availability-aware
 *       computation (enabled via cluster config)</li>
 * </ul>
 */
public class IntermediateStateCalcStage extends AbstractBaseStage {
  private static final Logger LOG =
      LoggerFactory.getLogger(IntermediateStateCalcStage.class.getName());

  // Strategy instances (thread-safe as they don't maintain state between calls)
  private final IntermediateStateComputationStrategy resourcePriorityStrategy =
      new ResourcePriorityIntermediateStateCalculator();
  private final IntermediateStateComputationStrategy availabilityAwareStrategy =
      new AvailabilityAwareIntermediateStateCalculator();

  @Override
  public void process(ClusterEvent event) throws Exception {
    _eventId = event.getEventId();
    CurrentStateOutput currentStateOutput = event.getAttribute(AttributeName.CURRENT_STATE.name());
    BestPossibleStateOutput bestPossibleStateOutput =
        event.getAttribute(AttributeName.BEST_POSSIBLE_STATE.name());
    Map<String, Resource> resourceToRebalance =
        event.getAttribute(AttributeName.RESOURCES_TO_REBALANCE.name());
    ResourceControllerDataProvider cache =
        event.getAttribute(AttributeName.ControllerDataProvider.name());
    MessageOutput messageOutput = event.getAttribute(AttributeName.MESSAGES_SELECTED.name());

    if (currentStateOutput == null || bestPossibleStateOutput == null || resourceToRebalance == null
        || cache == null || messageOutput == null) {
      throw new StageException(String.format("Missing attributes in event: %s. "
              + "Requires CURRENT_STATE (%s) |BEST_POSSIBLE_STATE (%s) |RESOURCES (%s) |MESSAGE_SELECT (%s) |DataCache (%s)",
          event, currentStateOutput, bestPossibleStateOutput, resourceToRebalance, messageOutput,
          cache));
    }

    IntermediateStateOutput intermediateStateOutput =
        compute(event, resourceToRebalance, currentStateOutput, bestPossibleStateOutput,
            messageOutput, cache);
    event.addAttribute(AttributeName.INTERMEDIATE_STATE.name(), intermediateStateOutput);

    // Make sure no instance has more replicas/partitions assigned than maxPartitionPerInstance. If
    // it does, pause the rebalance and put the cluster on maintenance mode
    int maxPartitionPerInstance = cache.getClusterConfig().getMaxPartitionsPerInstance();
    if (maxPartitionPerInstance > 0) {
      validateMaxPartitionsPerInstance(event, cache, intermediateStateOutput,
          maxPartitionPerInstance);
    }
  }

  /**
   * Compute intermediate state by delegating to the appropriate strategy based on cluster config.
   *
   * @param event the cluster event
   * @param resourceMap map of resources to rebalance
   * @param currentStateOutput current state output
   * @param bestPossibleStateOutput best possible state output
   * @param messageOutput message output from selection stage
   * @param dataCache the resource controller data provider
   * @return the computed intermediate state output
   */
  private IntermediateStateOutput compute(ClusterEvent event, Map<String, Resource> resourceMap,
      CurrentStateOutput currentStateOutput, BestPossibleStateOutput bestPossibleStateOutput,
      MessageOutput messageOutput, ResourceControllerDataProvider dataCache) {

    // Select strategy based on cluster configuration
    IntermediateStateComputationStrategy strategy = selectStrategy(dataCache);

    LogUtil.logInfo(LOG, _eventId, String.format("Using %s for intermediate state computation",
        strategy.getClass().getSimpleName()));

    return strategy.compute(event, resourceMap, currentStateOutput, bestPossibleStateOutput,
        messageOutput, dataCache);
  }

  /**
   * Select the appropriate computation strategy based on cluster configuration.
   *
   * @param dataCache the resource controller data provider containing cluster config
   * @return the selected strategy implementation
   */
  private IntermediateStateComputationStrategy selectStrategy(ResourceControllerDataProvider dataCache) {
    if (dataCache.getClusterConfig().isAvailabilityAwarePrioritizationEnabled()) {
      return availabilityAwareStrategy;
    }
    return resourcePriorityStrategy;
  }

  /**
   * Go through every instance in the assignment and check that each instance does NOT have more
   * replicas for partitions assigned to it than maxPartitionsPerInstance. If the assignment
   * violates this, put the cluster on maintenance mode.
   * This logic could be integrated with compute() for IntermediateState calculation but appended
   * separately for visibility and testing. Additionally, performing validation after compute()
   * ensures that we have a full intermediate state mapping complete prior to validation.
   * @param event
   * @param cache
   * @param intermediateStateOutput
   * @param maxPartitionPerInstance
   */
  private void validateMaxPartitionsPerInstance(ClusterEvent event,
      ResourceControllerDataProvider cache, IntermediateStateOutput intermediateStateOutput,
      int maxPartitionPerInstance) {
    Map<String, PartitionStateMap> resourceStatesMap =
        intermediateStateOutput.getResourceStatesMap();
    Map<String, Integer> instancePartitionCounts = new HashMap<>();

    for (String resource : resourceStatesMap.keySet()) {
      IdealState idealState = cache.getIdealState(resource);
      if (idealState != null && idealState.getStateModelDefRef()
          .equals(BuiltInStateModelDefinitions.Task.name())) {
        // Ignore task here. Task has its own throttling logic
        continue;
      }

      PartitionStateMap partitionStateMap = resourceStatesMap.get(resource);
      Map<Partition, Map<String, String>> stateMaps = partitionStateMap.getStateMap();
      for (Partition p : stateMaps.keySet()) {
        Map<String, String> stateMap = stateMaps.get(p);
        for (String instance : stateMap.keySet()) {
          // If this replica is in DROPPED state, do not count it in the partition count since it is
          // to be dropped
          String state = stateMap.get(instance);
          if (state.equals(HelixDefinedState.DROPPED.name())) {
            continue;
          }

          if (!instancePartitionCounts.containsKey(instance)) {
            instancePartitionCounts.put(instance, 0);
          }
          int partitionCount = instancePartitionCounts.get(instance);
          partitionCount++;
          if (partitionCount > maxPartitionPerInstance) {
            HelixManager manager = event.getAttribute(AttributeName.helixmanager.name());
            String errMsg = String.format(
                "Problem: according to this assignment, instance %s contains more "
                    + "replicas/partitions than the maximum number allowed (%d). Pipeline will "
                    + "stop the rebalance and put the cluster %s into maintenance mode", instance,
                maxPartitionPerInstance, cache.getClusterName());
            if (manager != null) {
              if (manager.getHelixDataAccessor()
                  .getProperty(manager.getHelixDataAccessor().keyBuilder().maintenance()) == null) {
                manager.getClusterManagmentTool()
                    .autoEnableMaintenanceMode(manager.getClusterName(), true, errMsg,
                        MaintenanceSignal.AutoTriggerReason.MAX_PARTITION_PER_INSTANCE_EXCEEDED);
              }
              LogUtil.logWarn(LOG, _eventId, errMsg);
            } else {
              LogUtil.logError(LOG, _eventId,
                  "HelixManager is not set/null! Failed to pause this cluster/enable maintenance"
                      + " mode due to an instance being assigned more replicas/partitions than "
                      + "the limit.");
            }

            ClusterStatusMonitor clusterStatusMonitor =
                event.getAttribute(AttributeName.clusterStatusMonitor.name());
            if (clusterStatusMonitor != null) {
              clusterStatusMonitor.setResourceRebalanceStates(Collections.singletonList(resource),
                  ResourceMonitor.RebalanceStatus.INTERMEDIATE_STATE_CAL_FAILED);
            }
            // Throw an exception here so that messages won't be sent out based on this mapping
            throw new HelixException(errMsg);
          }
          instancePartitionCounts.put(instance, partitionCount);
        }
      }
    }
  }
}
