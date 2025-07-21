package org.apache.helix.participant;

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

import java.util.Optional;
import java.util.Set;
import java.util.ArrayList;
import java.util.List;

import com.google.common.collect.Sets;
import org.apache.helix.HelixManager;
import org.apache.helix.HelixManagerFactory;
import org.apache.helix.InstanceType;
import org.apache.helix.NotificationContext;
import org.apache.helix.controller.pipeline.Pipeline;
import org.apache.helix.model.Message;
import org.apache.helix.monitoring.mbeans.dynamicMBeans.SimpleDynamicMetric;
import org.apache.helix.monitoring.mbeans.dynamicMBeans.DynamicMBeanProvider;
import org.apache.helix.monitoring.mbeans.dynamicMBeans.DynamicMetric;
import org.apache.helix.participant.statemachine.StateModelInfo;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.management.JMException;


@StateModelInfo(initialState = "OFFLINE", states = {"LEADER", "STANDBY"})
public class DistClusterControllerStateModel extends AbstractHelixLeaderStandbyStateModel {
  private static Logger logger = LoggerFactory.getLogger(DistClusterControllerStateModel.class);
  protected volatile Optional<HelixManager> _controllerOpt = Optional.empty();
  private final Set<Pipeline.Type> _enabledPipelineTypes;

  // dedicated lock object to avoid cross-instance contention from Optional.empty() singleton
  private final Object _controllerLock = new Object();

  // Metrics monitor for leadership failures
  private volatile DistControllerMetricsMonitor _metricsMonitor;

  public DistClusterControllerStateModel(String zkAddr) {
    this(zkAddr, Sets.newHashSet(Pipeline.Type.DEFAULT, Pipeline.Type.TASK));
  }

  public DistClusterControllerStateModel(String zkAddr, Set<Pipeline.Type> enabledPipelineTypes) {
    super(zkAddr);
    _enabledPipelineTypes = enabledPipelineTypes;
  }

  @Override
  public void onBecomeStandbyFromOffline(Message message, NotificationContext context) {
    logStateTransition("OFFLINE", "STANDBY", message.getPartitionName(), message.getTgtName());
  }

  @Override
  public void onBecomeLeaderFromStandby(Message message, NotificationContext context)
      throws Exception {
    String clusterName = message.getPartitionName();
    String controllerName = message.getTgtName();

    logger.info(controllerName + " becoming leader from standby for " + clusterName);

    synchronized (_controllerLock) {
      if (!_controllerOpt.isPresent()) {
        HelixManager newController = HelixManagerFactory
            .getZKHelixManager(clusterName, controllerName, InstanceType.CONTROLLER, _zkAddr);
        newController.setEnabledControlPipelineTypes(_enabledPipelineTypes);
        newController.connect();
        newController.startTimerTasks();
        _controllerOpt = Optional.of(newController);
        if (!newController.isLeader()) {
          logger.warn("Controller Leader session is not the same as the current session for "
              + clusterName + ". This should not happen. Controller: " + controllerName);
          // Publish metrics through Senseo when not a leader
          //publishLeadershipFailureMetric(clusterName, controllerName);
        }
        logStateTransition("STANDBY", "LEADER", clusterName, controllerName);
      } else {
        logger.error("controller already exists:" + _controllerOpt.get().getInstanceName() + " for "
            + clusterName);
      }
    }
  }

  @Override
  public void onBecomeStandbyFromLeader(Message message, NotificationContext context) {
    String clusterName = message.getPartitionName();
    String controllerName = message.getTgtName();

    logger.info(controllerName + " becoming standby from leader for " + clusterName);

    if (_controllerOpt.isPresent()) {
      reset();
      logStateTransition("LEADER", "STANDBY", clusterName, controllerName);
    } else {
      logger.error("No controller exists for " + clusterName);
    }
  }

  @Override
  public void onBecomeOfflineFromStandby(Message message, NotificationContext context) {
    logStateTransition("STANDBY", "OFFLINE", message.getPartitionName(), message.getTgtName());
  }

  @Override
  public void onBecomeDroppedFromOffline(Message message, NotificationContext context) {
    reset();
    logStateTransition("OFFLINE", "DROPPED", message == null ? "" : message.getPartitionName(),
        message == null ? "" : message.getTgtName());
  }

  @Override
  public String getStateModeInstanceDescription(String partitionName, String instanceName) {
    return String.format("Controller for cluster %s on instance %s", partitionName, instanceName);
  }

  @Override
  public void reset() {
    synchronized (_controllerLock) {
      if (_controllerOpt.isPresent()) {
        logger.info("Disconnecting controller: " + _controllerOpt.get().getInstanceName() + " for "
            + _controllerOpt.get().getClusterName());
        _controllerOpt.get().disconnect();
        if(_controllerOpt.get().isLeader()) {
          logger.warn("Controller is still leader after disconnecting: " + _controllerOpt.get().getInstanceName()
              + " for " + _controllerOpt.get().getClusterName());

          // Publish metrics when controller is still leader during reset
//          publishStillLeaderDuringResetMetric(_controllerOpt.get().getClusterName(),
//              _controllerOpt.get().getInstanceName());
        }
        _controllerOpt = Optional.empty();
      }

      // Clean up metrics monitor
      if (_metricsMonitor != null) {
        try {
          _metricsMonitor.unregister();
          logger.info("Unregistered DistController metrics monitor");
        } catch (Exception e) {
          logger.warn("Failed to unregister DistController metrics monitor", e);
        }
        _metricsMonitor = null;
      }
    }
  }

  /**
   * Publishes metrics when controller fails to acquire leadership
   * @param clusterName the cluster name
   * @param controllerName the controller instance name
   */
  private void publishLeadershipFailureMetric(String clusterName, String controllerName) {
    try {
      // Initialize metrics monitor if not already done
        if (_metricsMonitor == null) {
          _metricsMonitor = new DistControllerMetricsMonitor(clusterName, controllerName);
          _metricsMonitor.register();
        }

      // Increment the leadership failure counter
      if (_metricsMonitor != null) {
        _metricsMonitor.incrementLeadershipFailureCount();
      }
    } catch (Exception e) {
      logger.error("Failed to publish leadership failure metric for controller {} in cluster {}",
          controllerName, clusterName, e);
    }
  }

  /**
   * Publishes metrics when controller is still leader during reset
   * @param clusterName the cluster name
   * @param controllerName the controller instance name
   */
  private void publishStillLeaderDuringResetMetric(String clusterName, String controllerName) {
    try {
      // Initialize metrics monitor if not already done
      if (_metricsMonitor == null) {
        _metricsMonitor = new DistControllerMetricsMonitor(clusterName, controllerName);
        _metricsMonitor.register();
      }

      // Increment the still leader during reset counter
      if (_metricsMonitor != null) {
        _metricsMonitor.incrementStillLeaderDuringResetCount();
      }
    } catch (Exception e) {
      logger.error("Failed to publish still-leader-during-reset metric for controller {} in cluster {}",
          controllerName, clusterName, e);
    }
  }

  /**
   * Metrics monitor for DistClusterController state model
   * This class provides Senseo integration through DynamicMBeanProvider
   */
  private static class DistControllerMetricsMonitor extends DynamicMBeanProvider {
    private static final String MBEAN_DESCRIPTION = "DistClusterController Metrics Monitor";
    private final String _clusterName;
    private final String _controllerName;
    private final String _sensorName;

    private final SimpleDynamicMetric<Long> _leadershipFailureCounter;
    private final SimpleDynamicMetric<Long> _stillLeaderDuringResetCounter;

    public DistControllerMetricsMonitor(String clusterName, String controllerName) {
      _clusterName = clusterName;
      _controllerName = controllerName;
      // Create sensor name following Helix conventions for Senseo integration
      _sensorName = String.format("ClusterController.%s.%s", clusterName, controllerName);

      // Initialize metrics
      _leadershipFailureCounter = new SimpleDynamicMetric<>("LeadershipFailureCounter", 0L);
      _stillLeaderDuringResetCounter = new SimpleDynamicMetric<>("StillLeaderDuringResetCounter", 0L);
    }

    @Override
    public String getSensorName() {
      return _sensorName;
    }

    public void incrementLeadershipFailureCount() {
      _leadershipFailureCounter.updateValue(_leadershipFailureCounter.getValue() + 1);
    }

    public void incrementStillLeaderDuringResetCount() {
      _stillLeaderDuringResetCounter.updateValue(_stillLeaderDuringResetCounter.getValue() + 1);
    }

    @Override
    public DynamicMBeanProvider register() throws JMException {
      List<DynamicMetric<?, ?>> attributeList = new ArrayList<>();
      attributeList.add(_leadershipFailureCounter);
      attributeList.add(_stillLeaderDuringResetCounter);

      // Register the metrics with JMX for Senseo to consume
      doRegister(attributeList, MBEAN_DESCRIPTION,
          String.format("DistClusterController.%s.%s", _clusterName, _controllerName));
      return this;
    }
  }
}