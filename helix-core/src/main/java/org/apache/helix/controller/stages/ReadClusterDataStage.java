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

import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Callable;

import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import org.apache.helix.HelixDataAccessor;
import org.apache.helix.HelixDefinedState;
import org.apache.helix.HelixManager;
import org.apache.helix.controller.LogUtil;
import org.apache.helix.controller.dataproviders.BaseControllerDataProvider;
import org.apache.helix.controller.dataproviders.ResourceControllerDataProvider;
import org.apache.helix.controller.dataproviders.WorkflowControllerDataProvider;
import org.apache.helix.controller.pipeline.AbstractBaseStage;
import org.apache.helix.controller.pipeline.StageException;
import org.apache.helix.manager.zk.ZKHelixManager;
import org.apache.helix.model.ClusterConfig;
import org.apache.helix.model.CurrentState;
import org.apache.helix.model.InstanceConfig;
import org.apache.helix.model.LiveInstance;
import org.apache.helix.model.Message;
import org.apache.helix.model.StateModelDefinition;
import org.apache.helix.monitoring.mbeans.ClusterStatusMonitor;
import org.apache.helix.util.InstanceValidationUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ReadClusterDataStage extends AbstractBaseStage {
  private static final Logger logger = LoggerFactory.getLogger(ReadClusterDataStage.class.getName());

  @Override
  public void process(ClusterEvent event) throws Exception {
    _eventId = event.getEventId();
    HelixManager manager = event.getAttribute(AttributeName.helixmanager.name());
    if (manager == null) {
      throw new StageException("HelixManager attribute value is null");
    }

    final BaseControllerDataProvider dataProvider =
        event.getAttribute(AttributeName.ControllerDataProvider.name());

    HelixDataAccessor dataAccessor = manager.getHelixDataAccessor();

    dataProvider.refresh(dataAccessor);
    final ClusterConfig clusterConfig = dataProvider.getClusterConfig();
        final ClusterStatusMonitor clusterStatusMonitor =
            event.getAttribute(AttributeName.clusterStatusMonitor.name());

    // TODO (harry): move this to separate stage for resource controller only
    if (dataProvider instanceof ResourceControllerDataProvider) {
      asyncExecute(dataProvider.getAsyncTasksThreadPool(), new Callable<Object>() {
        @Override public Object call() {
          // Update the cluster status gauges
          if (clusterStatusMonitor != null) {
            LogUtil.logDebug(logger, _eventId, "Update cluster status monitors");

            Set<String> instanceSet = Sets.newHashSet();
            Set<String> liveInstanceSet = Sets.newHashSet();
            Set<String> disabledInstanceSet = Sets.newHashSet();
            Map<String, Map<String, List<String>>> disabledPartitions = Maps.newHashMap();
            Map<String, List<String>> oldDisabledPartitions = Maps.newHashMap();
            Map<String, Set<String>> tags = Maps.newHashMap();
            Map<String, LiveInstance> liveInstanceMap = dataProvider.getLiveInstances();
            Map<String, Set<Message>> instanceMessageMap = Maps.newHashMap();
            Map<String, InstanceConfig> instanceConfigMap = dataProvider.getInstanceConfigMap();
            Map<String, Long> instanceErrorPartitionCounts = Maps.newHashMap();
            Map<String, Long> instanceActualPartitionCounts = Maps.newHashMap();
            Map<String, Long> instanceActualTopStatePartitionCounts = Maps.newHashMap();
            
            for (Map.Entry<String, InstanceConfig> e : instanceConfigMap.entrySet()) {
              String instanceName = e.getKey();
              InstanceConfig config = e.getValue();
              instanceSet.add(instanceName);
              if (liveInstanceMap.containsKey(instanceName)) {
                liveInstanceSet.add(instanceName);
                instanceMessageMap.put(instanceName,
                    Sets.newHashSet(dataProvider.getMessages(instanceName).values()));
                
                // Count partitions this live instance actually hosts, from its CurrentState
                InstancePartitionCounts partitionCounts =
                    computeInstancePartitionCounts(dataProvider, instanceName);
                instanceErrorPartitionCounts.put(instanceName, partitionCounts.errorCount);
                instanceActualPartitionCounts.put(instanceName, partitionCounts.actualPartitionCount);
                instanceActualTopStatePartitionCounts.put(instanceName,
                    partitionCounts.actualTopStatePartitionCount);
              }
              if (!config.getInstanceEnabled()) {
                disabledInstanceSet.add(instanceName);
              }

              // TODO : Get rid of this data structure once the API is removed.
              oldDisabledPartitions.put(instanceName, config.getDisabledPartitions());
              disabledPartitions.put(instanceName, config.getDisabledPartitionsMap());

              Set<String> instanceTags = Sets.newHashSet(config.getTags());
              tags.put(instanceName, instanceTags);
            }
            clusterStatusMonitor
                .setClusterInstanceStatus(liveInstanceSet, instanceSet, disabledInstanceSet,
                    disabledPartitions, oldDisabledPartitions, tags, instanceMessageMap,
                    instanceConfigMap, instanceErrorPartitionCounts);
            clusterStatusMonitor.setInstanceActualPartitionStatus(instanceActualPartitionCounts,
                instanceActualTopStatePartitionCounts);
            LogUtil.logDebug(logger, _eventId, "Complete cluster status monitors update.");
          }
          return null;
        }
      });

      asyncExecute(dataProvider.getAsyncTasksThreadPool(), new Callable<Object>() {
        @Override
        public Object call() {
          validateAndReportInstanceDomainInfo(clusterConfig, dataProvider, clusterStatusMonitor);
          return null;
        }
      });
    } else {
      asyncExecute(dataProvider.getAsyncTasksThreadPool(), new Callable<Object>() {
        @Override
        public Object call() {
          clusterStatusMonitor.refreshWorkflowsStatus((WorkflowControllerDataProvider) dataProvider);
          clusterStatusMonitor.refreshJobsStatus((WorkflowControllerDataProvider) dataProvider);
          LogUtil.logDebug(logger, _eventId, "Workflow/Job gauge status successfully refreshed");
          return null;
        }
      });
    }
  }

  /**
   * Validates domain info for all instances against the cluster's topology configuration and
   * updates the DomainInfoValidGauge metric. Only runs when allowParticipantAutoJoin is enabled.
   *
   * An instance is considered invalid if {@link InstanceConfig#validateTopologySettingInInstanceConfig}
   * throws, which covers:
   * - Empty or missing domain info on an instance when topology-aware is enabled
   * - Missing fault zone type key in the domain map
   * - Missing zone ID when using legacy (non-custom) topology
   */
  static void validateAndReportInstanceDomainInfo(ClusterConfig clusterConfig,
      BaseControllerDataProvider dataProvider, ClusterStatusMonitor clusterStatusMonitor) {
    if (clusterStatusMonitor == null || clusterConfig == null) {
      return;
    }

    String autoJoin = clusterConfig.getRecord()
        .getSimpleField(ZKHelixManager.ALLOW_PARTICIPANT_AUTO_JOIN);
    if (!Boolean.parseBoolean(autoJoin)) {
      return;
    }

    Set<String> invalidInstances = new HashSet<>();
    Map<String, InstanceConfig> instanceConfigMap = dataProvider.getInstanceConfigMap();
    for (Map.Entry<String, InstanceConfig> entry : instanceConfigMap.entrySet()) {
      String instanceName = entry.getKey();
      InstanceConfig instanceConfig = entry.getValue();
      try {
        instanceConfig.validateTopologySettingInInstanceConfig(clusterConfig, instanceName);
      } catch (Exception e) {
        invalidInstances.add(instanceName);
        logger.warn("Instance {} has invalid domain info for cluster topology configuration",
            instanceName, e);
      }
    }

    clusterStatusMonitor.updateInstanceDomainInfoValidity(invalidInstances);
  }

  /**
   * Holder for per-instance partition counts derived from an instance's CurrentState.
   */
  private static class InstancePartitionCounts {
    long errorCount = 0L;
    long actualPartitionCount = 0L;
    long actualTopStatePartitionCount = 0L;
  }

  /**
   * Compute per-instance partition counts from the instance's CurrentState in a single pass:
   * the number of partitions in ERROR state, the number of partitions actually hosted, and the
   * number of those partitions in the resource top state.
   * <p>
   * A partition counts as "actually hosted" when its current state is neither DROPPED nor the
   * state model's initial state (typically OFFLINE), matching the convention already used by
   * {@link org.apache.helix.monitoring.mbeans.PerInstanceResourceMonitor}. A resource whose state
   * model definition cannot be resolved is skipped, since its states cannot be interpreted.
   * @param dataProvider the data provider containing current state information
   * @param instanceName the name of the instance to check
   * @return the counts; all zero if the instance is not live. Resources that fail to be read are
   *         skipped, so the counts reflect every resource that could be processed.
   */
  private InstancePartitionCounts computeInstancePartitionCounts(
      BaseControllerDataProvider dataProvider, String instanceName) {
    InstancePartitionCounts counts = new InstancePartitionCounts();

    Map<String, LiveInstance> liveInstances = dataProvider.getLiveInstances();
    LiveInstance liveInstance = liveInstances == null ? null : liveInstances.get(instanceName);
    if (liveInstance == null) {
      return counts;
    }

    Map<String, CurrentState> currentStateMap;
    try {
      currentStateMap = dataProvider.getCurrentState(instanceName, liveInstance.getEphemeralOwner(),
          false);
    } catch (Exception e) {
      LogUtil.logWarn(logger, _eventId,
          "Failed to read current states for instance: " + instanceName, e);
      return counts;
    }

    if (currentStateMap == null) {
      return counts;
    }

    for (Map.Entry<String, CurrentState> entry : currentStateMap.entrySet()) {
      try {
        accumulatePartitionCounts(dataProvider, entry.getValue(), counts);
      } catch (Exception e) {
        // Skip only the offending resource so one bad resource cannot zero out the whole instance.
        LogUtil.logWarn(logger, _eventId, "Failed to compute partition counts for instance: "
            + instanceName + ", resource: " + entry.getKey(), e);
      }
    }

    return counts;
  }

  /**
   * Accumulate the partition counts contributed by a single resource's CurrentState into
   * {@code counts}.
   */
  private void accumulatePartitionCounts(BaseControllerDataProvider dataProvider,
      CurrentState currentState, InstancePartitionCounts counts) {
    if (currentState == null) {
      return;
    }

    Map<String, String> partitionStateMap = currentState.getPartitionStateMap();
    if (partitionStateMap == null || partitionStateMap.isEmpty()) {
      return;
    }

    String stateModelDefRef = currentState.getStateModelDefRef();
    StateModelDefinition stateModelDef =
        stateModelDefRef == null ? null : dataProvider.getStateModelDef(stateModelDefRef);
    if (stateModelDef == null) {
      // Without the state model we cannot tell hosted partitions from initial/dropped ones, so
      // counting them would report a misleading value. Still count ERROR, which is model agnostic.
      LogUtil.logWarn(logger, _eventId,
          "Skipping actual partition counts for resource: " + currentState.getResourceName()
              + ", unresolved state model definition: " + stateModelDefRef);
      for (String state : partitionStateMap.values()) {
        if (HelixDefinedState.ERROR.name().equalsIgnoreCase(state)) {
          counts.errorCount++;
        }
      }
      return;
    }

    String topState = stateModelDef.getTopState();
    String initialState = stateModelDef.getInitialState();

    for (String state : partitionStateMap.values()) {
      if (state == null) {
        continue;
      }
      if (HelixDefinedState.ERROR.name().equalsIgnoreCase(state)) {
        counts.errorCount++;
      }
      // DROPPED partitions are no longer hosted, and initial-state (e.g. OFFLINE) partitions are
      // not yet being served, so neither counts towards what the instance actually hosts.
      if (HelixDefinedState.DROPPED.name().equalsIgnoreCase(state)
          || state.equalsIgnoreCase(initialState)) {
        continue;
      }
      counts.actualPartitionCount++;
      if (topState != null && topState.equalsIgnoreCase(state)) {
        counts.actualTopStatePartitionCount++;
      }
    }
  }
}
