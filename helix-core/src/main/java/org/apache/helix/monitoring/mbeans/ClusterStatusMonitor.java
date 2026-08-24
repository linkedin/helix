package org.apache.helix.monitoring.mbeans;

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

import java.lang.management.ManagementFactory;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.BooleanSupplier;
import java.util.regex.Pattern;
import javax.management.JMException;
import javax.management.MBeanServer;
import javax.management.MalformedObjectNameException;
import javax.management.ObjectName;

import com.google.common.base.Joiner;
import com.google.common.collect.Sets;
import org.apache.helix.HelixRebalanceException;
import org.apache.helix.constants.InstanceConstants;
import org.apache.helix.controller.dataproviders.WorkflowControllerDataProvider;
import org.apache.helix.controller.rebalancer.waged.constraints.HardConstraint;
import org.apache.helix.controller.stages.BestPossibleStateOutput;
import org.apache.helix.controller.stages.ClusterEventType;
import org.apache.helix.model.ExternalView;
import org.apache.helix.model.IdealState;
import org.apache.helix.model.InstanceConfig;
import org.apache.helix.model.Message;
import org.apache.helix.model.Partition;
import org.apache.helix.model.Resource;
import org.apache.helix.model.StateModelDefinition;
import org.apache.helix.task.JobConfig;
import org.apache.helix.task.TaskState;
import org.apache.helix.task.WorkflowConfig;
import org.apache.helix.task.WorkflowContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ClusterStatusMonitor implements ClusterStatusMonitorMBean {
  private static final Logger LOG = LoggerFactory.getLogger(ClusterStatusMonitor.class);

  static final String MESSAGE_QUEUE_STATUS_KEY = "MessageQueueStatus";
  static final String RESOURCE_STATUS_KEY = "ResourceStatus";
  public static final String PARTICIPANT_STATUS_KEY = "ParticipantStatus";
  public static final String CLUSTER_DN_KEY = "cluster";
  public static final String RESOURCE_DN_KEY = "resourceName";
  static final String INSTANCE_DN_KEY = "instanceName";
  static final String MESSAGE_QUEUE_DN_KEY = "messageQueue";
  static final String JOB_TYPE_DN_KEY = "jobType";
  static final String DEFAULT_WORKFLOW_JOB_TYPE = "DEFAULT";
  public static final String DEFAULT_TAG = "DEFAULT";

  static final Pattern JMX_SPECIAL_CHARS = Pattern.compile("[,:=*?]");

  private final String _clusterName;
  private final MBeanServer _beanServer;

  private boolean _enabled = true;
  private boolean _inMaintenance = false;
  private boolean _paused = false;

  private Set<String> _liveInstances = Collections.emptySet();
  private Set<String> _instances = Collections.emptySet();
  private Set<String> _disabledInstances = Collections.emptySet();
  private Map<String, Map<String, List<String>>> _disabledPartitions = Collections.emptyMap();
  private Map<String, List<String>> _oldDisabledPartitions = Collections.emptyMap();
  private AtomicLong _totalMsgQueueSize = new AtomicLong(0L);
  private AtomicLong _maxInstanceMsgQueueSize = new AtomicLong(0L);
  private AtomicLong _totalPastDueMsgSize = new AtomicLong(0L);
  private boolean _rebalanceFailure = false;
  private AtomicLong _rebalanceFailureCount = new AtomicLong(0L);
  private AtomicLong _continuousResourceRebalanceFailureCount = new AtomicLong(0L);
  private AtomicLong _continuousTaskRebalanceFailureCount = new AtomicLong(0L);
  // DEFAULT controller cluster-event pipeline backlog. Near 0 on a healthy controller (the queue
  // dedups by event type); climbs when the controller still holds leadership but stops draining
  // events, surfacing the "zombie leader" failure mode.
  private AtomicLong _controllerEventQueueSizeGauge = new AtomicLong(0L);
  // Wall-clock time (ms) of the last completed DEFAULT controller pipeline run, reported by
  // GenericHelixController. Paired with the queue-size gauge to derive
  // ControllerPipelineStalledGauge. 0 until the first pipeline completes (treated as "no data").
  private AtomicLong _lastPipelineEndTimestamp = new AtomicLong(0L);
  // Stall threshold (ms): a non-empty event queue whose pipeline has not completed within this many
  // ms is treated as a wedged ("zombie leader") controller. Sourced from ClusterConfig
  // (CONTROLLER_PIPELINE_STALL_THRESHOLD_MS) and pushed by GenericHelixController each pipeline run;
  // uses the default until first reported. Deliberately larger than the worst-case single pipeline
  // run so a long-but-healthy rebalance (which can legitimately take minutes) is not mis-flagged;
  // detection of a truly stuck-but-alive thread is therefore bounded by this threshold, while a dead
  // pipeline thread is caught immediately via the liveness check below.
  private static final long DEFAULT_PIPELINE_STALL_THRESHOLD_MS = 300000L; // 5 minutes
  private AtomicLong _pipelineStallThresholdMs =
      new AtomicLong(DEFAULT_PIPELINE_STALL_THRESHOLD_MS);
  // Liveness check for the DEFAULT controller-event pipeline thread, wired by GenericHelixController
  // as () -> _eventThread == null || _eventThread.isAlive(). Read lazily at gauge-read time so a
  // dead processing thread is detected even though the thread itself can no longer report anything.
  // Null when not wired (e.g. unit tests or a controller without the DEFAULT pipeline), in which
  // case the gauge falls back to the stall-threshold signal alone.
  private volatile BooleanSupplier _pipelineLivenessSupplier = null;

  // WAGED per-FailureCategory counters. Populated in the constructor with a zero AtomicLong per
  // enum value so reads on never-incremented categories return 0 instead of NPE.
  private final Map<HelixRebalanceException.FailureCategory, AtomicLong> _wagedFailureCategoryCounters =
      new ConcurrentHashMap<>();
  private final AtomicLong _wagedCustomerActionableFailureCount = new AtomicLong(0L);
  private final AtomicLong _wagedInternalFailureCount = new AtomicLong(0L);
  private volatile boolean _wagedFallbackInUse = false;
  // Reversible rollup gauges: 1 while WAGED's most recent computation failed for a customer-actionable
  // reason (capacity / candidate-node / resource-config / cluster-config) vs a Helix-internal reason
  // (metadata-store / algorithm / async / unknown); reset to 0 on the next clean computation. Drives
  // "who to page right now" and self-clears on recovery. The *Count fields above stay the monotonic
  // "how often" tally.
  private volatile boolean _wagedCustomerActionableFailure = false;
  private volatile boolean _wagedInternalFailure = false;
  // Reversible gauge: 1 while WAGED's most recent Baseline (global) computation failed, reset to 0
  // when a Baseline computation next succeeds. Owned exclusively by the GLOBAL_BASELINE phase. This
  // is the latent signal -- serving may be fine (partial succeeds off the last-good baseline) while
  // WAGED can no longer recompute the ideal target. Distinct from the serving rollup gauges above,
  // which are owned by the PARTIAL phase.
  private volatile boolean _wagedBaselineComputeFailing = false;
  // Reversible gauge: 1 while the most recent delayed-rebalance-overwrite computation failed, reset
  // to 0 when one next succeeds or is not needed. Owned exclusively by the DELAYED_REBALANCE_OVERWRITES
  // phase -- its only dedicated reversible signal (it otherwise shares the fallback gauge with
  // emergency). This is the temporary min-active-replica top-up applied during the delayed window.
  private volatile boolean _wagedRebalanceOverwriteFailing = false;

  // Cluster-wide estimated max capacity utilization for WAGED resources (see
  // ClusterStatusMonitorMBean#getEstimatedMaxClusterCapacityUsageGauge). Refreshed every pipeline
  // run from the current assignment; stays 0.0 when no WAGED capacity is configured.
  private volatile double _estimatedMaxClusterCapacityUsage = 0.0d;

  // WAGED per-HardConstraint failure counters. Pre-populated for every HardConstraint.Type so
  // reads return 0 instead of NPE for constraints that have not yet fired.
  private final Map<HardConstraint.Type, AtomicLong> _wagedHardConstraintFailureCounters =
      new ConcurrentHashMap<>();

  // WAGED per-HardConstraint "currently blocking" gauges (0/1). Unlike the cumulative counters
  // above, these are reversible: 1 while a constraint blocked placement in the most recent WAGED
  // computation, reset to 0 on the next clean computation. Lets a transient blip be told apart from
  // a persistent failure by value, per reason. Pre-populated for every type to avoid NPE.
  private final Map<HardConstraint.Type, AtomicLong> _wagedHardConstraintBlockingGauges =
      new ConcurrentHashMap<>();

  // Cluster-level instance operation counts
  private final Map<InstanceConstants.InstanceOperation, AtomicLong> _perOperationInstanceCount =
      new ConcurrentHashMap<>();

  private final ConcurrentHashMap<String, ResourceMonitor> _resourceMonitorMap =
      new ConcurrentHashMap<>();
  private final ConcurrentHashMap<String, InstanceMonitor> _instanceMonitorMap =
      new ConcurrentHashMap<>();

  // phaseName -> eventMonitor
  protected final ConcurrentHashMap<String, ClusterEventMonitor> _clusterEventMonitorMap =
      new ConcurrentHashMap<>();

  // ClusterEventType -> topologyChangeEventMonitor (one entry per topology event type)
  protected final ConcurrentHashMap<ClusterEventType, TopologyChangeEventMonitor>
      _topologyChangeEventMonitorMap = new ConcurrentHashMap<>();

  private CustomizedViewMonitor _customizedViewMonitor;

  /**
   * PerInstanceResource monitor map: beanName->monitor
   */
  private final Map<PerInstanceResourceMonitor.BeanName, PerInstanceResourceMonitor> _perInstanceResourceMonitorMap =
      new ConcurrentHashMap<>();

  private final Map<String, WorkflowMonitor> _perTypeWorkflowMonitorMap = new ConcurrentHashMap<>();

  private final Map<String, JobMonitor> _perTypeJobMonitorMap = new ConcurrentHashMap<>();

  public ClusterStatusMonitor(String clusterName) {
    _clusterName = clusterName;
    _beanServer = ManagementFactory.getPlatformMBeanServer();

    // Initialize the map with all operation types
    for (InstanceConstants.InstanceOperation operation : InstanceConstants.InstanceOperation.values()) {
      _perOperationInstanceCount.put(operation, new AtomicLong(0L));
    }

    // Pre-create one AtomicLong per WAGED failure category so dashboards see a stable 0
    // for categories that have not yet fired.
    for (HelixRebalanceException.FailureCategory category :
        HelixRebalanceException.FailureCategory.values()) {
      _wagedFailureCategoryCounters.put(category, new AtomicLong(0L));
    }
    // Same for per-HardConstraint counters and the reversible per-HardConstraint blocking gauges.
    for (HardConstraint.Type type : HardConstraint.Type.values()) {
      _wagedHardConstraintFailureCounters.put(type, new AtomicLong(0L));
      _wagedHardConstraintBlockingGauges.put(type, new AtomicLong(0L));
    }
  }

  public ObjectName getObjectName(String name) throws MalformedObjectNameException {
    return new ObjectName(String.format("%s:%s", MonitorDomainNames.ClusterStatus.name(), name));
  }

  public String getClusterName() {
    return _clusterName;
  }

  @Override
  public long getDownInstanceGauge() {
    return _instances.size() - _liveInstances.size();
  }

  @Override
  public long getInstancesGauge() {
    return _instances.size();
  }

  @Override
  public long getDisabledInstancesGauge() {
    return _disabledInstances.size();
  }

  @Override
  public long getDisabledPartitionsGauge() {
    int numDisabled = 0;
    for (Map<String, List<String>> perInstance : _disabledPartitions.values()) {
      for (List<String> partitions : perInstance.values()) {
        if (partitions != null) {
          numDisabled += partitions.size();
        }
      }
    }

    // TODO : Get rid of this after old API removed.
    for (List<String> partitions : _oldDisabledPartitions.values()) {
      if (partitions != null) {
        numDisabled += partitions.size();
      }
    }

    return numDisabled;
  }

  @Override
  public long getRebalanceFailureGauge() {
    return _rebalanceFailure ? 1 : 0;
  }

  public void setRebalanceFailureGauge(boolean isFailure) {
    this._rebalanceFailure = isFailure;
  }

  public void setResourceRebalanceStates(Collection<String> resources,
      ResourceMonitor.RebalanceStatus state) {
    for (String resource : resources) {
      ResourceMonitor resourceMonitor = getOrCreateResourceMonitor(resource);
      if (resourceMonitor != null) {
        resourceMonitor.setRebalanceState(state);
      }
    }
  }

  @Override
  public long getMaxMessageQueueSizeGauge() {
    return _maxInstanceMsgQueueSize.get();
  }

  @Override
  public long getInstanceMessageQueueBacklog() {
    return _totalMsgQueueSize.get();
  }

  @Override
  public long getTotalPastDueMessageGauge() {
    return _totalPastDueMsgSize.get();
  }

  @Override
  public long getInstancesInOperationEnableGauge() {
    return _perOperationInstanceCount.getOrDefault(
        InstanceConstants.InstanceOperation.ENABLE, new AtomicLong(0L)).get();
  }

  @Override
  public long getInstancesInOperationDisableGauge() {
    return _perOperationInstanceCount.getOrDefault(
        InstanceConstants.InstanceOperation.DISABLE, new AtomicLong(0L)).get();
  }

  @Override
  public long getInstancesInOperationEvacuateGauge() {
    return _perOperationInstanceCount.getOrDefault(
        InstanceConstants.InstanceOperation.EVACUATE, new AtomicLong(0L)).get();
  }

  @Override
  public long getInstancesInOperationSwapInGauge() {
    return _perOperationInstanceCount.getOrDefault(
        InstanceConstants.InstanceOperation.SWAP_IN, new AtomicLong(0L)).get();
  }

  @Override
  public long getInstancesInOperationUnknownGauge() {
    return _perOperationInstanceCount.getOrDefault(
        InstanceConstants.InstanceOperation.UNKNOWN, new AtomicLong(0L)).get();
  }

  private void register(Object bean, ObjectName name) {
    try {
      if (_beanServer.isRegistered(name)) {
        _beanServer.unregisterMBean(name);
      }
    } catch (Exception e) {
      // OK
    }

    try {
      LOG.info("Register MBean: " + name);
      _beanServer.registerMBean(bean, name);
    } catch (Exception e) {
      LOG.warn("Could not register MBean: " + name, e);
    }
  }

  private void unregister(ObjectName name) {
    try {
      if (_beanServer.isRegistered(name)) {
        LOG.info("Unregistering " + name.toString());
        _beanServer.unregisterMBean(name);
      }
    } catch (Exception e) {
      LOG.warn("Could not unregister MBean: " + name, e);
    }
  }

  /**
   * Update the gauges for all instances in the cluster
   * @param liveInstanceSet the current set of live instances
   * @param instanceSet the current set of configured instances (live or other
   * @param disabledInstanceSet the current set of configured instances that are disabled
   * @param disabledPartitions a map of instance name to the set of partitions disabled on it
   * @param tags a map of instance name to the set of tags on it
   * @param instanceMessageMap a map of pending messages from each live instance
   * @param instanceConfigMap a map of instance name to InstanceConfig (for operation tracking)
   * @param errorPartitionCounts a map of instance name to the count of partitions in ERROR state
   */
  public void setClusterInstanceStatus(Set<String> liveInstanceSet, Set<String> instanceSet,
      Set<String> disabledInstanceSet, Map<String, Map<String, List<String>>> disabledPartitions,
      Map<String, List<String>> oldDisabledPartitions, Map<String, Set<String>> tags,
      Map<String, Set<Message>> instanceMessageMap, Map<String, InstanceConfig> instanceConfigMap,
      Map<String, Long> errorPartitionCounts) {
    synchronized (_instanceMonitorMap) {
      // Unregister beans for instances that are no longer configured
      Set<String> toUnregister = Sets.newHashSet(_instanceMonitorMap.keySet());
      toUnregister.removeAll(instanceSet);
      unregisterInstances(toUnregister);

      // Register beans for instances that are newly configured
      Set<String> toRegister = Sets.newHashSet(instanceSet);
      toRegister.removeAll(_instanceMonitorMap.keySet());
      Set<InstanceMonitor> monitorsToRegister = Sets.newHashSet();
      for (String instanceName : toRegister) {
        try {
          ObjectName objectName = getObjectName(getInstanceBeanName(instanceName));
          InstanceMonitor bean = new InstanceMonitor(_clusterName, instanceName, objectName);
          long errorPartitionCount = errorPartitionCounts != null && errorPartitionCounts.containsKey(instanceName)
              ? errorPartitionCounts.get(instanceName) : 0L;
          bean.updateInstance(tags.get(instanceName), disabledPartitions.get(instanceName),
              oldDisabledPartitions.get(instanceName), liveInstanceSet.contains(instanceName),
              !disabledInstanceSet.contains(instanceName), errorPartitionCount);
          monitorsToRegister.add(bean);
        } catch (MalformedObjectNameException ex) {
          LOG.error("Failed to create instance monitor for instance: {}.", instanceName);
        }
      }

      try {
        registerInstances(monitorsToRegister);
      } catch (JMException e) {
        LOG.error("Could not register instances with MBean server: {}.", toRegister, e);
      }

      // Update all the sets
      _instances = instanceSet;
      _liveInstances = liveInstanceSet;
      _disabledInstances = disabledInstanceSet;
      _disabledPartitions = disabledPartitions;
      _oldDisabledPartitions = oldDisabledPartitions;

      // message related counts
      long totalMsgQueueSize = 0L;
      long maxInstanceMsgQueueSize = 0L;
      long totalPastDueMsgSize = 0L;
      long now = System.currentTimeMillis();

      // Update the instance MBeans
      for (String instanceName : instanceSet) {
        if (_instanceMonitorMap.containsKey(instanceName)) {
          // Update the bean
          InstanceMonitor bean = _instanceMonitorMap.get(instanceName);
          String oldSensorName = bean.getSensorName();
          long errorPartitionCount = errorPartitionCounts != null && errorPartitionCounts.containsKey(instanceName)
              ? errorPartitionCounts.get(instanceName) : 0L;
          bean.updateInstance(tags.get(instanceName), disabledPartitions.get(instanceName),
              oldDisabledPartitions.get(instanceName), liveInstanceSet.contains(instanceName),
              !disabledInstanceSet.contains(instanceName), errorPartitionCount);

          // Update instance operation duration metrics
          if (instanceConfigMap != null && instanceConfigMap.containsKey(instanceName)) {
            InstanceConfig.InstanceOperation instanceOperation =
                instanceConfigMap.get(instanceName).getInstanceOperation();
            bean.updateInstanceOperation(instanceOperation.getOperation(),
                instanceOperation.getTimestamp());
          }

          // calculate and update instance level message related gauges
          Set<Message> messages = instanceMessageMap.get(instanceName);
          if (messages != null) {
            long msgQueueSize = messages.size();
            bean.updateMessageQueueSize(msgQueueSize);
            totalMsgQueueSize += msgQueueSize;
            if (msgQueueSize > maxInstanceMsgQueueSize) {
              maxInstanceMsgQueueSize = msgQueueSize;
            }

            long pastDueMsgCount =
                messages.stream().filter(m -> (m.getCompletionDueTimeStamp() <= now)).count();
            bean.updatePastDueMessageGauge(pastDueMsgCount);
            totalPastDueMsgSize += pastDueMsgCount;
            LOG.debug("There are totally {} messages, {} are past due on instance {}", msgQueueSize,
                pastDueMsgCount, instanceName);
          }

          // If the sensor name changed, re-register the bean so that listeners won't miss it
          String newSensorName = bean.getSensorName();
          if (!oldSensorName.equals(newSensorName)) {
            try {
              unregisterInstances(Arrays.asList(instanceName));
              registerInstances(Arrays.asList(bean));
            } catch (JMException e) {
              LOG.error("Could not refresh registration with MBean server: {}", instanceName, e);
            }
          }
        }
      }

      // Update cluster level message related gauges
      _maxInstanceMsgQueueSize.set(maxInstanceMsgQueueSize);
      _totalMsgQueueSize.set(totalMsgQueueSize);
      _totalPastDueMsgSize.set(totalPastDueMsgSize);

      // Count instances by operation type (cluster-level metrics) using map
      // First reset all counts to 0
      for (AtomicLong count : _perOperationInstanceCount.values()) {
        count.set(0L);
      }

      if (instanceConfigMap != null) {
        for (Map.Entry<String, InstanceConfig> entry : instanceConfigMap.entrySet()) {
          InstanceConfig config = entry.getValue();
          InstanceConstants.InstanceOperation operation = InstanceConstants.InstanceOperation.ENABLE;

          if (config != null && config.getInstanceOperation() != null) {
            operation = config.getInstanceOperation().getOperation();
          }

          // Increment the count for this operation
          AtomicLong count = _perOperationInstanceCount.get(operation);
          if (count != null) {
            count.incrementAndGet();
          } else {
            // If operation is not in the map (shouldn't happen), default to ENABLE
            _perOperationInstanceCount.get(InstanceConstants.InstanceOperation.ENABLE).incrementAndGet();
          }
        }
      }
    }
  }

  /**
   * Updates the domain info validity gauge for each instance. Instances in the invalidInstances
   * set will have their gauge set to 0 (invalid), all other registered instances will be set
   * to 1 (valid).
   *
   * @param invalidInstances the set of instance names whose domain info is not correctly populated
   */
  public void updateInstanceDomainInfoValidity(Set<String> invalidInstances) {
    if (invalidInstances == null) {
      invalidInstances = Collections.emptySet();
    }
    synchronized (_instanceMonitorMap) {
      for (Map.Entry<String, InstanceMonitor> entry : _instanceMonitorMap.entrySet()) {
        boolean isInvalid = invalidInstances.contains(entry.getKey());
        entry.getValue().updateDomainInfoValid(!isInvalid);
      }
    }
  }

  /**
   * Update the duration of handling a cluster event in a certain phase.
   * @param phase
   * @param duration
   */
  public void updateClusterEventDuration(String phase, long duration) {
    ClusterEventMonitor monitor = getOrCreateClusterEventMonitor(phase);
    if (monitor != null) {
      monitor.reportDuration(duration);
    }
  }

  /**
   * Lazy initialization of customized view monitor
   * @param clusterName the cluster name of the cluster to be monitored
   * @return a customized view monitor instance
   */
  public synchronized CustomizedViewMonitor getOrCreateCustomizedViewMonitor(String clusterName) {
    if (_customizedViewMonitor == null) {
      _customizedViewMonitor = new CustomizedViewMonitor(clusterName);
      try {
        _customizedViewMonitor.register();
      } catch (JMException e) {
        LOG.error("Failed to register CustomizedViewMonitorMBean for cluster " + _clusterName, e);
      }
    }
    return _customizedViewMonitor;
  }

  private ClusterEventMonitor getOrCreateClusterEventMonitor(String phase) {
    try {
      if (!_clusterEventMonitorMap.containsKey(phase)) {
        synchronized (_clusterEventMonitorMap) {
          if (!_clusterEventMonitorMap.containsKey(phase)) {
            ClusterEventMonitor monitor = new ClusterEventMonitor(this, phase);
            monitor.register();
            _clusterEventMonitorMap.put(phase, monitor);
          }
        }
      }
    } catch (JMException e) {
      LOG.error("Failed to register ClusterEventMonitorMbean for cluster " + _clusterName
          + " and phase type: " + phase, e);
    }

    return _clusterEventMonitorMap.get(phase);
  }

  /**
   * Update the total count of messages that the controller has sent to each instance and each resource so far
   * @param messages a list of messages
   */
  public void increaseMessageReceived(List<Message> messages) {
    Map<String, Long> messageCountPerInstance = new HashMap<>();
    Map<String, Long> messageCountPerResource = new HashMap<>();

    // Aggregate messages
    for (Message message : messages) {
      String instanceName = message.getAttribute(Message.Attributes.TGT_NAME);
      String resourceName = message.getAttribute(Message.Attributes.RESOURCE_NAME);

      if (instanceName != null) {
        if (!messageCountPerInstance.containsKey(instanceName)) {
          messageCountPerInstance.put(instanceName, 0L);
        }
        messageCountPerInstance.put(instanceName, messageCountPerInstance.get(instanceName) + 1L);
      }

      if (resourceName != null) {
        if (!messageCountPerResource.containsKey(resourceName)) {
          messageCountPerResource.put(resourceName, 0L);
        }
        messageCountPerResource.put(resourceName, messageCountPerResource.get(resourceName) + 1L);
      }
    }

    // Update message count sent per instance and per resource
    for (String instance : messageCountPerInstance.keySet()) {
      InstanceMonitor instanceMonitor = _instanceMonitorMap.get(instance);
      if (instanceMonitor != null) {
        instanceMonitor.increaseMessageCount(messageCountPerInstance.get(instance));
      }
    }
    for (String resource : messageCountPerResource.keySet()) {
      ResourceMonitor resourceMonitor = _resourceMonitorMap.get(resource);
      if (resourceMonitor != null) {
        resourceMonitor.increaseMessageCount(messageCountPerResource.get(resource));
        resourceMonitor.increaseMessageCountWithCounter(messageCountPerResource.get(resource));
      }
    }
  }

  /**
   * Updates instance capacity status for per instance, including max usage and capacity of each
   * capacity key. Before calling this API, we assume the instance monitors are already registered
   * in ReadClusterDataStage. If the monitor is not registered, this instance capacity status update
   * will fail.
   *
   * @param instanceName This instance name
   * @param maxUsage Max capacity usage of this instance
   * @param capacityMap A map of this instance capacity, {capacity key: capacity value}
   */
  public void updateInstanceCapacityStatus(String instanceName, double maxUsage,
      Map<String, Integer> capacityMap) {
    InstanceMonitor monitor = _instanceMonitorMap.get(instanceName);
    if (monitor == null) {
      LOG.warn("Failed to update instance capacity status because instance monitor is not found, "
          + "instance: {}.", instanceName);
      return;
    }
    monitor.updateMaxCapacityUsage(maxUsage);
    monitor.updateCapacity(capacityMap);
  }

  /**
   * Updates the cluster-wide estimated max capacity utilization gauge for WAGED resources. See
   * {@link ClusterStatusMonitorMBean#getEstimatedMaxClusterCapacityUsageGauge()} for the value
   * semantics and range.
   *
   * @param estimatedMaxClusterCapacityUsage cluster aggregate utilization ({@code >= 0.0})
   */
  public void updateClusterCapacityUsage(double estimatedMaxClusterCapacityUsage) {
    _estimatedMaxClusterCapacityUsage = estimatedMaxClusterCapacityUsage;
  }

  /**
   * Update gauges for resource at instance level
   * @param bestPossibleStates
   * @param resourceMap
   * @param stateModelDefMap
   */
  public void setPerInstanceResourceStatus(BestPossibleStateOutput bestPossibleStates,
      Map<String, InstanceConfig> instanceConfigMap, Map<String, Resource> resourceMap,
      Map<String, StateModelDefinition> stateModelDefMap) {

    // Convert to perInstanceResource beanName->partition->state
    Map<PerInstanceResourceMonitor.BeanName, Map<Partition, String>> beanMap = new HashMap<>();
    // Track partition counts per instance: instance -> total partitions
    Map<String, Long> instancePartitionCount = new HashMap<>();
    // Track top state partition counts per instance: instance -> top state partitions
    Map<String, Long> instanceTopStatePartitionCount = new HashMap<>();

    Set<String> resourceSet = new HashSet<>(bestPossibleStates.resourceSet());
    for (String resource : resourceSet) {
      Map<Partition, Map<String, String>> partitionStateMap =
          new HashMap<>(bestPossibleStates.getResourceMap(resource));
      StateModelDefinition stateModelDef = stateModelDefMap.get(
          resourceMap.get(resource).getStateModelDefRef());
      String topState = stateModelDef.getTopState();

      for (Partition partition : partitionStateMap.keySet()) {
        Map<String, String> instanceStateMap = partitionStateMap.get(partition);
        for (String instance : instanceStateMap.keySet()) {
          String state = instanceStateMap.get(instance);
          PerInstanceResourceMonitor.BeanName beanName =
              new PerInstanceResourceMonitor.BeanName(_clusterName, instance, resource);
          beanMap.computeIfAbsent(beanName, k -> new HashMap<>()).put(partition, state);

          // Count partitions per instance
          instancePartitionCount.merge(instance, 1L, Long::sum);

          // Count top state partitions per instance
          if (topState != null && topState.equals(state)) {
            instanceTopStatePartitionCount.merge(instance, 1L, Long::sum);
          }
        }
      }
    }

    // Update instance monitors with partition counts
    synchronized (_instanceMonitorMap) {
      for (String instanceName : _instanceMonitorMap.keySet()) {
        InstanceMonitor instanceMonitor = _instanceMonitorMap.get(instanceName);
        if (instanceMonitor != null) {
          long partitionCount = instancePartitionCount.getOrDefault(instanceName, 0L);
          long topStatePartitionCount = instanceTopStatePartitionCount.getOrDefault(instanceName, 0L);
          instanceMonitor.updatePartitionCount(partitionCount);
          instanceMonitor.updateTopStatePartitionCount(topStatePartitionCount);
        }
      }
    }

    synchronized (_perInstanceResourceMonitorMap) {
      // Unregister beans for per-instance resources that no longer exist
      Set<PerInstanceResourceMonitor.BeanName> toUnregister =
          Sets.newHashSet(_perInstanceResourceMonitorMap.keySet());
      toUnregister.removeAll(beanMap.keySet());
      try {
        unregisterPerInstanceResources(toUnregister);
      } catch (MalformedObjectNameException e) {
        LOG.error("Fail to unregister per-instance resource from MBean server: " + toUnregister, e);
      }
      // Register beans for per-instance resources that are newly configured
      Set<PerInstanceResourceMonitor.BeanName> toRegister = Sets.newHashSet(beanMap.keySet());
      toRegister.removeAll(_perInstanceResourceMonitorMap.keySet());
      Set<PerInstanceResourceMonitor> monitorsToRegister = Sets.newHashSet();
      for (PerInstanceResourceMonitor.BeanName beanName : toRegister) {
        PerInstanceResourceMonitor bean = new PerInstanceResourceMonitor(_clusterName,
            beanName.instanceName(), beanName.resourceName());
        String stateModelDefName = resourceMap.get(beanName.resourceName()).getStateModelDefRef();
        InstanceConfig config = instanceConfigMap.get(beanName.instanceName());
        bean.update(beanMap.get(beanName), Sets.newHashSet(config.getTags()),
            stateModelDefMap.get(stateModelDefName));
        monitorsToRegister.add(bean);
      }
      try {
        registerPerInstanceResources(monitorsToRegister);
      } catch (JMException e) {
        LOG.error("Fail to register per-instance resource with MBean server: " + toRegister, e);
      }
      // Update existing beans
      for (PerInstanceResourceMonitor.BeanName beanName : _perInstanceResourceMonitorMap.keySet()) {
        PerInstanceResourceMonitor bean = _perInstanceResourceMonitorMap.get(beanName);
        String stateModelDefName = resourceMap.get(beanName.resourceName()).getStateModelDefRef();
        InstanceConfig config = instanceConfigMap.get(beanName.instanceName());
        bean.update(beanMap.get(beanName), Sets.newHashSet(config.getTags()),
            stateModelDefMap.get(stateModelDefName));
      }
    }
  }

  /**
   * Cleanup resource monitors. Keep the monitors if only exist in the input set.
   * @param resourceNames the resources that still exist
   */
  public void retainResourceMonitor(Set<String> resourceNames) {
    Set<String> resourcesToRemove = new HashSet<>();
    synchronized (_resourceMonitorMap) {
      resourceNames.retainAll(_resourceMonitorMap.keySet());
      resourcesToRemove.addAll(_resourceMonitorMap.keySet());
    }
    resourcesToRemove.removeAll(resourceNames);

    try {
      registerResources(resourceNames);
    } catch (JMException e) {
      LOG.error(String.format("Could not register beans for the following resources: %s",
          Joiner.on(',').join(resourceNames)), e);
    }

    try {
      unregisterResources(resourcesToRemove);
    } catch (Exception e) {
      LOG.error(String.format("Could not unregister beans for the following resources: %s",
          Joiner.on(',').join(resourcesToRemove)), e);
    }
  }

  public void setResourceState(String resourceName, ExternalView externalView,
      IdealState idealState, StateModelDefinition stateModelDef) {
    try {
      ResourceMonitor resourceMonitor = getOrCreateResourceMonitor(resourceName);

      if (resourceMonitor != null) {
        resourceMonitor.updateResourceState(externalView, idealState, stateModelDef);
      }
    } catch (Exception e) {
      LOG.error("Fail to set resource status, resource: " + idealState.getResourceName(), e);
    }
  }

  public void setResourcePendingMessages(String resourceName, int messageCount) {
    try {
      ResourceMonitor resourceMonitor = getOrCreateResourceMonitor(resourceName);

      if (resourceMonitor != null) {
        resourceMonitor.updatePendingStateTransitionMessages(messageCount);
      }
    } catch (Exception e) {
      LOG.error("Fail to set pending resource messages, resource: " + resourceName, e);
    }
  }

  /**
   * Updates metrics of average partition weight per capacity key for a resource. If a resource
   * monitor is not yet existed for this resource, a new resource monitor will be created for this
   * resource.
   *
   * @param resourceName The resource name for which partition weight is updated
   * @param averageWeightMap A map of average partition weight of each capacity key:
   *                         capacity key -> average partition weight
   */
  public void updatePartitionWeight(String resourceName, Map<String, Integer> averageWeightMap) {
    ResourceMonitor monitor = getOrCreateResourceMonitor(resourceName);
    if (monitor == null) {
      LOG.warn("Failed to update partition weight metric for resource: {} because resource monitor"
          + " is not created.", resourceName);
      return;
    }
    monitor.updatePartitionWeightStats(averageWeightMap);
  }

  public void updateMissingTopStateDurationStats(String resourceName, long totalDuration,
      long helixLatency, boolean isGraceful, boolean succeeded) {
    ResourceMonitor resourceMonitor = getOrCreateResourceMonitor(resourceName);

    if (resourceMonitor != null) {
      resourceMonitor.updateStateHandoffStats(ResourceMonitor.MonitorState.TOP_STATE, totalDuration,
          helixLatency, isGraceful, succeeded);
    }
  }

  public void decrementMissingTopStateBeyondThresholdGauge(String resourceName) {
    ResourceMonitor resourceMonitor = getOrCreateResourceMonitor(resourceName);

    if (resourceMonitor != null) {
      resourceMonitor.decrementMissingTopStateBeyondThresholdGauge();
    }
  }

  public void incrementControllerHandoffBeyondThresholdGauge(String resourceName) {
    ResourceMonitor resourceMonitor = getOrCreateResourceMonitor(resourceName);

    if (resourceMonitor != null) {
      resourceMonitor.incrementControllerHandoffBeyondThresholdGauge();
    }
  }

  public void decrementControllerHandoffBeyondThresholdGauge(String resourceName) {
    ResourceMonitor resourceMonitor = getOrCreateResourceMonitor(resourceName);

    if (resourceMonitor != null) {
      resourceMonitor.decrementControllerHandoffBeyondThresholdGauge();
    }
  }

  public void incrementParticipantHandoffBeyondThresholdGauge(String resourceName) {
    ResourceMonitor resourceMonitor = getOrCreateResourceMonitor(resourceName);

    if (resourceMonitor != null) {
      resourceMonitor.incrementParticipantHandoffBeyondThresholdGauge();
    }
  }

  public void decrementParticipantHandoffBeyondThresholdGauge(String resourceName) {
    ResourceMonitor resourceMonitor = getOrCreateResourceMonitor(resourceName);

    if (resourceMonitor != null) {
      resourceMonitor.decrementParticipantHandoffBeyondThresholdGauge();
    }
  }

  public void updateRebalancerStats(String resourceName, long numPendingRecoveryRebalancePartitions,
      long numPendingLoadRebalancePartitions, long numRecoveryRebalanceThrottledPartitions,
      long numLoadRebalanceThrottledPartitions, boolean rebalanceThrottledByErrorPartitions) {
    ResourceMonitor resourceMonitor = getOrCreateResourceMonitor(resourceName);

    if (resourceMonitor != null) {
      resourceMonitor.updateRebalancerStats(numPendingRecoveryRebalancePartitions,
          numPendingLoadRebalancePartitions, numRecoveryRebalanceThrottledPartitions,
          numLoadRebalanceThrottledPartitions, rebalanceThrottledByErrorPartitions);
    }
  }

  private ResourceMonitor getOrCreateResourceMonitor(String resourceName) {
    try {
      if (!_resourceMonitorMap.containsKey(resourceName)) {
        synchronized (_resourceMonitorMap) {
          if (!_resourceMonitorMap.containsKey(resourceName)) {
            String beanName = getResourceBeanName(resourceName);
            ResourceMonitor bean =
                new ResourceMonitor(_clusterName, resourceName, getObjectName(beanName));
            _resourceMonitorMap.put(resourceName, bean);
          }
        }
      }
    } catch (JMException ex) {
      LOG.error("Fail to register resource mbean, resource: " + resourceName);
    }

    return _resourceMonitorMap.get(resourceName);
  }

  public void resetMaxMissingTopStateGauge() {
    for (ResourceMonitor monitor : _resourceMonitorMap.values()) {
      monitor.resetMaxTopStateHandoffGauge();
    }
  }

  public void active() {
    LOG.info("Active ClusterStatusMonitor");
    try {
      register(this, getObjectName(clusterBeanName()));
      // Register one MBean per topology-change event type up-front so dashboards see a
      // stable schema (and OTel exporters discover all dimensions) before the first event
      // of that type arrives. Each MBean starts at zero.
      registerAllTopologyChangeEventMonitors();
    } catch (Exception e) {
      LOG.error("Fail to register ClusterStatusMonitor", e);
    }
  }

  public void reset() {
    LOG.info("Reset ClusterStatusMonitor");
    try {
      unregisterAllResources();
      unregisterAllInstances();
      unregisterAllPerInstanceResources();
      unregister(getObjectName(clusterBeanName()));
      unregisterAllEventMonitors();
      unregisterAllTopologyChangeEventMonitors();
      unregisterAllWorkflowsMonitor();
      unregisterAllJobs();

      _liveInstances.clear();
      _instances.clear();
      _disabledInstances.clear();
      _disabledPartitions.clear();
      _oldDisabledPartitions.clear();
      _rebalanceFailure = false;
      _maxInstanceMsgQueueSize.set(0L);
      _totalPastDueMsgSize.set(0L);
      _totalMsgQueueSize.set(0L);
      _rebalanceFailureCount.set(0L);
      _continuousResourceRebalanceFailureCount.set(0L);
      _continuousTaskRebalanceFailureCount.set(0L);
      // Zero the WAGED per-category and per-HardConstraint counters along with the rollup
      // counters and the fallback gauge. Like the legacy rebalance counters above, these are
      // reset on leadership change to avoid stale numbers from a prior controller leadership
      // period being attributed to the new one.
      _wagedFailureCategoryCounters.values().forEach(c -> c.set(0L));
      _wagedHardConstraintFailureCounters.values().forEach(c -> c.set(0L));
      _wagedHardConstraintBlockingGauges.values().forEach(g -> g.set(0L));
      _wagedCustomerActionableFailureCount.set(0L);
      _wagedInternalFailureCount.set(0L);
      _wagedFallbackInUse = false;
      _wagedCustomerActionableFailure = false;
      _wagedInternalFailure = false;
      _wagedBaselineComputeFailing = false;
      _wagedRebalanceOverwriteFailing = false;
      // Zero the DEFAULT controller-event pipeline backlog gauge on leadership change, for the
      // same reason as the counters above: the ClusterStatusMonitor instance is reused across
      // leadership periods, so a stale depth from a prior leader must not be re-reported by the
      // re-registered bean after re-election (it would otherwise persist until the next
      // enqueue/dequeue refreshes it).
      _controllerEventQueueSizeGauge.set(0L);
      // Reset the pipeline-progress timestamp for the same reason: a stale value from a prior
      // leadership period must not make the re-registered bean report a spurious stall.
      _lastPipelineEndTimestamp.set(0L);
    } catch (Exception e) {
      LOG.error("Fail to reset ClusterStatusMonitor, cluster: " + _clusterName, e);
    }
  }

  public void refreshWorkflowsStatus(WorkflowControllerDataProvider cache) {
    for (Map.Entry<String, WorkflowMonitor> workflowMonitor : _perTypeWorkflowMonitorMap
        .entrySet()) {
      workflowMonitor.getValue().resetGauges();
    }

    Map<String, WorkflowConfig> workflowConfigMap = cache.getWorkflowConfigMap();
    for (String workflow : workflowConfigMap.keySet()) {
      if (workflowConfigMap.get(workflow).isRecurring() || workflow.isEmpty()) {
        continue;
      }
      WorkflowContext workflowContext = cache.getWorkflowContext(workflow);
      TaskState currentState =
          workflowContext == null ? TaskState.NOT_STARTED : workflowContext.getWorkflowState();
      updateWorkflowGauges(workflowConfigMap.get(workflow), currentState);
    }
  }

  public void updateWorkflowCounters(WorkflowConfig workflowConfig, TaskState to) {
    updateWorkflowCounters(workflowConfig, to, -1L);
  }

  public void updateWorkflowCounters(WorkflowConfig workflowConfig, TaskState to, long latency) {
    String workflowType = workflowConfig.getWorkflowType();
    workflowType = preProcessWorkflow(workflowType);
    WorkflowMonitor workflowMonitor = _perTypeWorkflowMonitorMap.get(workflowType);
    if (workflowMonitor != null) {
      workflowMonitor.updateWorkflowCounters(to, latency);
    }
  }

  private void updateWorkflowGauges(WorkflowConfig workflowConfig, TaskState current) {
    String workflowType = workflowConfig.getWorkflowType();
    workflowType = preProcessWorkflow(workflowType);
    WorkflowMonitor workflowMonitor = _perTypeWorkflowMonitorMap.get(workflowType);
    if (workflowMonitor != null) {
      workflowMonitor.updateWorkflowGauges(current);
    }
  }

  private String preProcessWorkflow(String workflowType) {
    if (workflowType == null || workflowType.length() == 0) {
      workflowType = DEFAULT_WORKFLOW_JOB_TYPE;
    }

    synchronized (_perTypeWorkflowMonitorMap) {
      if (!_perTypeWorkflowMonitorMap.containsKey(workflowType)) {
        WorkflowMonitor monitor = new WorkflowMonitor(_clusterName, workflowType);
        try {
          monitor.register();
        } catch (JMException e) {
          LOG.error("Failed to register object for workflow type : " + workflowType, e);
        }
        _perTypeWorkflowMonitorMap.put(workflowType, monitor);
      }
    }
    return workflowType;
  }

  public void refreshJobsStatus(WorkflowControllerDataProvider cache) {
    for (Map.Entry<String, JobMonitor> jobMonitor : _perTypeJobMonitorMap.entrySet()) {
      jobMonitor.getValue().resetJobGauge();
    }
    for (String workflow : cache.getWorkflowConfigMap().keySet()) {
      if (workflow.isEmpty()) {
        continue;
      }
      WorkflowConfig workflowConfig = cache.getWorkflowConfig(workflow);
      if (workflowConfig == null) {
        continue;
      }
      Set<String> allJobs = workflowConfig.getJobDag().getAllNodes();
      WorkflowContext workflowContext = cache.getWorkflowContext(workflow);
      for (String job : allJobs) {
        TaskState currentState =
            workflowContext == null ? TaskState.NOT_STARTED : workflowContext.getJobState(job);
        updateJobGauges(
            workflowConfig.getJobTypes() == null ? null : workflowConfig.getJobTypes().get(job),
            currentState);
      }
    }
  }

  public void updateJobCounters(JobConfig jobConfig, TaskState to) {
    updateJobCounters(jobConfig, to, -1L);
  }

  public void updateJobCounters(JobConfig jobConfig, TaskState to, long latency) {
    String jobType = jobConfig.getJobType();
    jobType = preProcessJobMonitor(jobType);
    JobMonitor jobMonitor = _perTypeJobMonitorMap.get(jobType);
    if (jobMonitor != null) {
      jobMonitor.updateJobMetricsWithLatency(to, latency);
    }
  }

  /**
   * For each JobType, report their total available threads across all instances to corresponding
   * jobMonitors
   * @param threadCapacityMap
   */
  public void updateAvailableThreadsPerJob(Map<String, Integer> threadCapacityMap) {
    for (String jobType : threadCapacityMap.keySet()) {
      JobMonitor jobMonitor = getJobMonitor(jobType);
      jobMonitor.updateAvailableThreadGauge((long) threadCapacityMap.get(jobType));
    }
  }

  /**
   * TODO: Separate Workflow/Job Monitors from ClusterStatusMonitor because ClusterStatusMonitor is
   * getting too big.
   * Returns the appropriate JobMonitor for the given type. If it does not exist, create one and
   * return it.
   * @param jobType
   * @return
   */
  public JobMonitor getJobMonitor(String jobType) {
    return _perTypeJobMonitorMap.get(preProcessJobMonitor(jobType));
  }

  private void updateJobGauges(String jobType, TaskState current) {
    // When first time for WorkflowRebalancer call, jobconfig may not ready.
    // Thus only check it for gauge.
    jobType = preProcessJobMonitor(jobType);
    JobMonitor jobMonitor = _perTypeJobMonitorMap.get(jobType);
    if (jobMonitor != null) {
      jobMonitor.updateJobGauge(current);
    }
  }

  private String preProcessJobMonitor(String jobType) {
    if (jobType == null || jobType.length() == 0) {
      jobType = DEFAULT_WORKFLOW_JOB_TYPE;
    }

    synchronized (_perTypeJobMonitorMap) {
      if (!_perTypeJobMonitorMap.containsKey(jobType)) {
        String jobMonitorBeanName = getJobBeanName(jobType);
        JobMonitor monitor = null;
        try {
          monitor = new JobMonitor(_clusterName, jobType, getObjectName(jobMonitorBeanName));
          monitor.register(); // Necessary for dynamic metrics
        } catch (Exception e) {
          LOG.error("Failed to register job type : " + jobType, e);
        }
        if (monitor != null) {
          _perTypeJobMonitorMap.put(jobType, monitor);
        }
      }
    }
    return jobType;
  }

  private void registerInstances(Collection<InstanceMonitor> instances)
      throws JMException {
    synchronized (_instanceMonitorMap) {
      for (InstanceMonitor monitor : instances) {
        String instanceName = monitor.getInstanceName();
        // If this instance MBean is already registered, unregister it.
        InstanceMonitor removedMonitor = _instanceMonitorMap.remove(instanceName);
        if (removedMonitor != null) {
          removedMonitor.unregister();
        }
        monitor.register();
        _instanceMonitorMap.put(instanceName, monitor);
      }
    }
  }

  private void unregisterAllInstances() {
    synchronized (_instanceMonitorMap) {
      unregisterInstances(_instanceMonitorMap.keySet());
    }
  }

  private void unregisterInstances(Collection<String> instances) {
    synchronized (_instanceMonitorMap) {
      for (String instanceName : instances) {
        InstanceMonitor monitor = _instanceMonitorMap.remove(instanceName);
        if (monitor != null) {
          monitor.unregister();
        }
      }
    }
  }

  private void registerResources(Collection<String> resources) throws JMException {
    synchronized (_resourceMonitorMap) {
      for (String resourceName : resources) {
        ResourceMonitor monitor = _resourceMonitorMap.get(resourceName);
        if (monitor != null) {
          monitor.register();
        }
      }
    }
  }

  private void unregisterAllResources() {
    synchronized (_resourceMonitorMap) {
      unregisterResources(_resourceMonitorMap.keySet());
    }
  }

  private void unregisterResources(Collection<String> resources) {
    synchronized (_resourceMonitorMap) {
      for (String resourceName : resources) {
        ResourceMonitor monitor = _resourceMonitorMap.get(resourceName);
        if (monitor != null) {
          monitor.unregister();
        }
      }
      _resourceMonitorMap.keySet().removeAll(resources);
    }
  }

  private void unregisterAllEventMonitors() {
    synchronized (_clusterEventMonitorMap) {
      for (ClusterEventMonitor monitor : _clusterEventMonitorMap.values()) {
        monitor.unregister();
      }
      _clusterEventMonitorMap.clear();
    }
  }

  /**
   * Increment the per-{@link ClusterEventType} received counter for topology-change events.
   * No-op for non-topology event types so callers don't need to filter.
   */
  public void incrementTopologyChangeEventReceived(ClusterEventType eventType) {
    if (eventType == null || !eventType.isTopologyChange()) {
      return;
    }
    TopologyChangeEventMonitor monitor = _topologyChangeEventMonitorMap.get(eventType);
    if (monitor != null) {
      monitor.incrementReceived();
    }
  }

  /**
   * Increment the per-{@link ClusterEventType} processed counter for topology-change events.
   * No-op for non-topology event types so callers don't need to filter.
   */
  public void incrementTopologyChangeEventProcessed(ClusterEventType eventType) {
    if (eventType == null || !eventType.isTopologyChange()) {
      return;
    }
    TopologyChangeEventMonitor monitor = _topologyChangeEventMonitorMap.get(eventType);
    if (monitor != null) {
      monitor.incrementProcessed();
    }
  }

  private void registerAllTopologyChangeEventMonitors() {
    synchronized (_topologyChangeEventMonitorMap) {
      for (ClusterEventType eventType : ClusterEventType.topologyChangeEventTypes()) {
        if (_topologyChangeEventMonitorMap.containsKey(eventType)) {
          continue;
        }
        try {
          TopologyChangeEventMonitor monitor = new TopologyChangeEventMonitor(this, eventType);
          monitor.register();
          _topologyChangeEventMonitorMap.put(eventType, monitor);
        } catch (JMException e) {
          LOG.error("Failed to register TopologyChangeEventMonitor for cluster {} eventType {}",
              _clusterName, eventType, e);
        }
      }
    }
  }

  private void unregisterAllTopologyChangeEventMonitors() {
    synchronized (_topologyChangeEventMonitorMap) {
      for (TopologyChangeEventMonitor monitor : _topologyChangeEventMonitorMap.values()) {
        monitor.unregister();
      }
      _topologyChangeEventMonitorMap.clear();
    }
  }

  private void registerPerInstanceResources(Collection<PerInstanceResourceMonitor> monitors)
      throws JMException {
    synchronized (_perInstanceResourceMonitorMap) {
      for (PerInstanceResourceMonitor monitor : monitors) {
        String instanceName = monitor.getInstanceName();
        String resourceName = monitor.getResourceName();
        monitor.register();
        _perInstanceResourceMonitorMap
            .put(new PerInstanceResourceMonitor.BeanName(_clusterName, instanceName, resourceName),
                monitor);
      }
    }
  }

  private void unregisterAllPerInstanceResources() throws MalformedObjectNameException {
    synchronized (_perInstanceResourceMonitorMap) {
      unregisterPerInstanceResources(_perInstanceResourceMonitorMap.keySet());
    }
  }

  private void unregisterPerInstanceResources(
      Collection<PerInstanceResourceMonitor.BeanName> beanNames)
      throws MalformedObjectNameException {
    synchronized (_perInstanceResourceMonitorMap) {
      for (PerInstanceResourceMonitor.BeanName beanName : beanNames) {
        if (_perInstanceResourceMonitorMap.get(beanName) != null) {
          _perInstanceResourceMonitorMap.get(beanName).unregister();
        }
      }
      _perInstanceResourceMonitorMap.keySet().removeAll(beanNames);
    }
  }

  private void unregisterAllWorkflowsMonitor() {
    synchronized (_perTypeWorkflowMonitorMap) {
      Iterator<Map.Entry<String, WorkflowMonitor>> workflowIter =
          _perTypeWorkflowMonitorMap.entrySet().iterator();
      while (workflowIter.hasNext()) {
        Map.Entry<String, WorkflowMonitor> workflowEntry = workflowIter.next();
        workflowEntry.getValue().unregister();
        workflowIter.remove();
      }
    }
  }

  private void unregisterAllJobs() {
    synchronized (_perTypeJobMonitorMap) {
      Iterator<Map.Entry<String, JobMonitor>> jobIter = _perTypeJobMonitorMap.entrySet().iterator();
      while (jobIter.hasNext()) {
        Map.Entry<String, JobMonitor> jobEntry = jobIter.next();
        jobEntry.getValue().unregister();
        jobIter.remove();
      }
    }
  }

  public ResourceMonitor getResourceMonitor(String resourceName) {
    return _resourceMonitorMap.get(resourceName);
  }

  protected String clusterBeanName() {
    return String.format("%s=%s", CLUSTER_DN_KEY, _clusterName);
  }

  /**
   * Build instance bean name
   * @param instanceName
   * @return instance bean name
   */
  protected String getInstanceBeanName(String instanceName) {
    return String.format("%s,%s=%s", clusterBeanName(), INSTANCE_DN_KEY, instanceName);
  }

  /**
   * Build resource bean name
   * @param resourceName
   * @return resource bean name
   */
  protected String getResourceBeanName(String resourceName) {
    // JMX ObjectName values cannot contain ':', '=', ',', '*', or '?' unquoted.
    // Quote the resource name only when it contains such characters to avoid
    // MalformedObjectNameException (e.g. URN-style names like urn:li:foo:bar),
    // while leaving normal resource names unchanged in the MBean key.
    String safeResourceName = JMX_SPECIAL_CHARS.matcher(resourceName).find()
        ? ObjectName.quote(resourceName) : resourceName;
    return String.format("%s,%s=%s", clusterBeanName(), RESOURCE_DN_KEY, safeResourceName);
  }

  /**
   * Build per-instance resource bean name:
   * "cluster={clusterName},instanceName={instanceName},resourceName={resourceName}"
   * @param instanceName
   * @param resourceName
   * @return per-instance resource bean name
   */
  protected String getPerInstanceResourceBeanName(String instanceName, String resourceName) {
    return new PerInstanceResourceMonitor.BeanName(_clusterName, instanceName, resourceName)
        .toString();
  }

  /**
   * Build job per type bean name
   * "cluster={clusterName},jobType={jobType},
   * @param jobType The job type
   * @return per job type bean name
   */
  protected String getJobBeanName(String jobType) {
    return String.format("%s, %s=%s", clusterBeanName(), JOB_TYPE_DN_KEY, jobType);
  }

  @Override
  public String getSensorName() {
    return MonitorDomainNames.ClusterStatus.name() + "." + _clusterName;
  }

  @Override
  public long getEnabled() {
    return _enabled ? 1 : 0;
  }

  @Override
  public long getMaintenance() {
    return _inMaintenance ? 1 : 0;
  }

  public void setMaintenance(boolean inMaintenance) {
    _inMaintenance = inMaintenance;
  }

  @Override
  public long getPaused() {
    return _paused ? 1 : 0;
  }

  public void setPaused(boolean paused) {
    _paused = paused;
  }

  public void setEnabled(boolean enabled) {
    this._enabled = enabled;
  }

  public void reportRebalanceFailure() {
    _rebalanceFailureCount.incrementAndGet();
  }

  /**
   * Increment the monotonic WAGED failure counters (per-category plus the customer-actionable /
   * internal rollup counter) for a classified failure. This is the scope-agnostic "how often" tally
   * -- every failing computation counts, whether baseline, partial, or emergency. It does NOT touch
   * the reversible rollup gauges; see {@link #setWagedFailureRollupGauge}. Safe to call from any
   * rebalance thread.
   */
  public void incrementWagedFailureCategoryCount(HelixRebalanceException.FailureCategory category) {
    if (category == null) {
      category = HelixRebalanceException.FailureCategory.UNKNOWN;
    }
    _wagedFailureCategoryCounters.get(category).incrementAndGet();
    if (category.isCustomerActionable()) {
      _wagedCustomerActionableFailureCount.incrementAndGet();
    } else {
      _wagedInternalFailureCount.incrementAndGet();
    }
  }

  /**
   * Light the reversible rollup failure gauge (customer-actionable vs internal) for a classified
   * failure. This is the "is serving failing right now" signal and is owned by the SERVING (partial
   * / emergency) phase only -- baseline failures must not light it, since serving can be healthy
   * while the baseline is stale. Reset on a clean partial via {@link #resetWagedFailureRollupGauges}.
   */
  public void setWagedFailureRollupGauge(HelixRebalanceException.FailureCategory category) {
    if (category == null) {
      category = HelixRebalanceException.FailureCategory.UNKNOWN;
    }
    if (category.isCustomerActionable()) {
      _wagedCustomerActionableFailure = true;
    } else {
      _wagedInternalFailure = true;
    }
  }

  /**
   * Record a serving-scope WAGED rebalance failure with its classified category: ticks the
   * monotonic counters and lights the reversible rollup gauge. Convenience for callers on the
   * serving path (partial failure, synchronous emergency / overwrite). Baseline failures should call
   * {@link #incrementWagedFailureCategoryCount} only.
   */
  public void reportWagedFailureByCategory(HelixRebalanceException.FailureCategory category) {
    incrementWagedFailureCategoryCount(category);
    setWagedFailureRollupGauge(category);
  }

  /**
   * Reset the reversible rollup failure gauges to 0. Called when the SERVING (partial) computation
   * succeeds so a prior "currently failing" reading clears on recovery. The monotonic rollup
   * counters are untouched.
   */
  public void resetWagedFailureRollupGauges() {
    _wagedCustomerActionableFailure = false;
    _wagedInternalFailure = false;
  }

  /**
   * Flip the reversible Baseline-compute-failing gauge. Set true when the Baseline (global)
   * computation fails, false when it next succeeds. Owned exclusively by the GLOBAL_BASELINE phase.
   */
  public void updateWagedBaselineComputeFailing(boolean failing) {
    _wagedBaselineComputeFailing = failing;
  }

  /**
   * Flip the reversible delayed-rebalance-overwrite-failing gauge. Set true when the overwrite
   * computation fails, false when it next succeeds or is not needed. Owned exclusively by the
   * DELAYED_REBALANCE_OVERWRITES phase.
   */
  public void updateWagedRebalanceOverwriteFailing(boolean failing) {
    _wagedRebalanceOverwriteFailing = failing;
  }

  /**
   * Record that a partition failed placement because at least one candidate node was rejected
   * by a hard constraint of the given type. Called once per partition per distinct constraint
   * type that contributed to the failure (set-union across nodes, not summed).
   */
  public void reportWagedHardConstraintFailure(HardConstraint.Type type) {
    if (type == null) {
      type = HardConstraint.Type.UNKNOWN;
    }
    _wagedHardConstraintFailureCounters.get(type).incrementAndGet();
  }

  /**
   * Publish the reversible per-HardConstraint "currently blocking" snapshot from the most recent
   * WAGED computation: each type in {@code currentlyBlocking} is set to 1, every other type to 0.
   * An empty (or null) set -- a clean computation -- resets all gauges to 0, so the signal tells a
   * transient blip apart from a persistent failure by value. See
   * {@link ClusterStatusMonitorMBean#getWagedHardConstraintFaultZoneBlockingGauge()}.
   * @param currentlyBlocking HardConstraint.Types that blocked placement in the latest computation
   */
  public void updateWagedHardConstraintBlocking(Set<HardConstraint.Type> currentlyBlocking) {
    Set<HardConstraint.Type> blocking =
        currentlyBlocking == null ? Collections.emptySet() : currentlyBlocking;
    for (Map.Entry<HardConstraint.Type, AtomicLong> entry : _wagedHardConstraintBlockingGauges
        .entrySet()) {
      entry.getValue().set(blocking.contains(entry.getKey()) ? 1L : 0L);
    }
  }

  /**
   * Flip the fallback gauge. Set to true when WAGED returns the last-known-good assignment
   * instead of a freshly computed one; reset to false when a clean calculation succeeds.
   */
  public void setWagedFallbackInUseGauge(boolean inUse) {
    _wagedFallbackInUse = inUse;
  }

  public void reportContinuousResourceRebalanceFailureCount(long newValue) {
    _continuousResourceRebalanceFailureCount.set(newValue);
  }

  public void reportContinuousTaskRebalanceFailureCount(long newValue) {
    _continuousTaskRebalanceFailureCount.set(newValue);
  }

  /**
   * Surface the DEFAULT controller cluster-event pipeline backlog as a JMX gauge. A healthy
   * controller drains events quickly so this stays near 0 (the queue dedups by event type); a
   * wedged controller that still holds leadership but stops processing lets it climb, making the
   * "zombie leader" failure mode detectable.
   */
  public void setControllerEventQueueSizeGauge(long size) {
    _controllerEventQueueSizeGauge.set(size);
  }

  /**
   * Report the wall-clock time (ms) of the most recent completed DEFAULT controller pipeline run.
   * Consumed by {@link #getControllerPipelineStalledGauge()} to tell a wedged controller (queue
   * not draining) apart from a healthy idle or busy-but-progressing one.
   */
  public void setLastPipelineEndTimestamp(long timestampMs) {
    _lastPipelineEndTimestamp.set(timestampMs);
  }

  /**
   * Set the wedged-controller stall threshold (ms), sourced from ClusterConfig
   * (CONTROLLER_PIPELINE_STALL_THRESHOLD_MS) by GenericHelixController. Non-positive values are
   * ignored so a misconfiguration cannot silently disable {@link #getControllerPipelineStalledGauge()}.
   */
  public void setPipelineStallThresholdMs(long thresholdMs) {
    if (thresholdMs > 0) {
      _pipelineStallThresholdMs.set(thresholdMs);
    }
  }

  /**
   * Wire the DEFAULT controller-event pipeline liveness check used by
   * {@link #getControllerPipelineStalledGauge()}. Supplied by GenericHelixController as
   * {@code () -> _eventThread == null || _eventThread.isAlive()} so a dead processing thread on a
   * still-leader controller is reported as wedged immediately, independent of the stall threshold.
   */
  public void setPipelineLivenessSupplier(BooleanSupplier livenessSupplier) {
    _pipelineLivenessSupplier = livenessSupplier;
  }

  @Override
  public long getRebalanceFailureCounter() {
    return _rebalanceFailureCount.get();
  }

  @Override
  public long getContinuousResourceRebalanceFailureCount() {
    return _continuousResourceRebalanceFailureCount.get();
  }

  @Override
  public long getContinuousTaskRebalanceFailureCount() {
    return _continuousTaskRebalanceFailureCount.get();
  }

  @Override
  public long getControllerEventQueueSizeGauge() {
    return _controllerEventQueueSizeGauge.get();
  }

  @Override
  public long getControllerPipelineStalledGauge() {
    // Healthy when there is no pending work: an empty queue means the pipeline is keeping up (or is
    // legitimately idle), so nothing to flag.
    if (_controllerEventQueueSizeGauge.get() <= 0) {
      return 0L;
    }
    // Definitive zombie: the controller still holds queued events (and, since this MBean is only
    // registered while it is the leader, still holds leadership) but the DEFAULT pipeline thread is
    // dead. This is caught immediately, with no dependence on the stall threshold, and does not
    // false-positive on a busy controller (whose thread is always alive).
    BooleanSupplier liveness = _pipelineLivenessSupplier;
    if (liveness != null && !liveness.getAsBoolean()) {
      return 1L;
    }
    // Best-effort fallback for a thread that is alive but not making progress (e.g. hung or looping
    // inside a run, or not picking the next event off the queue): no pipeline run has completed
    // within the stall threshold. The threshold is set above the worst-case healthy run so a long
    // legitimate rebalance is not mis-flagged; a truly stuck-but-alive thread is caught once the
    // threshold elapses. Computed lazily on read so it stays correct even without any setter firing.
    long lastEnd = _lastPipelineEndTimestamp.get();
    if (lastEnd > 0 && (System.currentTimeMillis() - lastEnd) > _pipelineStallThresholdMs.get()) {
      return 1L;
    }
    return 0L;
  }

  @Override
  public long getWagedCustomerActionableFailureCounter() {
    return _wagedCustomerActionableFailureCount.get();
  }

  @Override
  public long getWagedInternalFailureCounter() {
    return _wagedInternalFailureCount.get();
  }

  @Override
  public long getWagedCustomerActionableFailureGauge() {
    return _wagedCustomerActionableFailure ? 1L : 0L;
  }

  @Override
  public long getWagedInternalFailureGauge() {
    return _wagedInternalFailure ? 1L : 0L;
  }

  @Override
  public long getWagedBaselineComputeFailingGauge() {
    return _wagedBaselineComputeFailing ? 1L : 0L;
  }

  @Override
  public long getWagedRebalanceOverwriteFailingGauge() {
    return _wagedRebalanceOverwriteFailing ? 1L : 0L;
  }

  @Override
  public long getWagedFailureCapacityDeficitCounter() {
    return _wagedFailureCategoryCounters
        .get(HelixRebalanceException.FailureCategory.CAPACITY_DEFICIT).get();
  }

  @Override
  public long getWagedFailureNoCandidateNodeCounter() {
    return _wagedFailureCategoryCounters
        .get(HelixRebalanceException.FailureCategory.NO_CANDIDATE_NODE).get();
  }

  @Override
  public long getWagedFailureInvalidResourceConfigCounter() {
    return _wagedFailureCategoryCounters
        .get(HelixRebalanceException.FailureCategory.INVALID_RESOURCE_CONFIG).get();
  }

  @Override
  public long getWagedFailureInvalidClusterConfigCounter() {
    return _wagedFailureCategoryCounters
        .get(HelixRebalanceException.FailureCategory.INVALID_CLUSTER_CONFIG).get();
  }

  @Override
  public long getWagedFailureMetadataStoreIoCounter() {
    return _wagedFailureCategoryCounters
        .get(HelixRebalanceException.FailureCategory.METADATA_STORE_IO).get();
  }

  @Override
  public long getWagedFailureAlgorithmInternalCounter() {
    return _wagedFailureCategoryCounters
        .get(HelixRebalanceException.FailureCategory.ALGORITHM_INTERNAL).get();
  }

  @Override
  public long getWagedFailureAsyncExecutionCounter() {
    return _wagedFailureCategoryCounters
        .get(HelixRebalanceException.FailureCategory.ASYNC_EXECUTION).get();
  }

  @Override
  public long getWagedFailureUnknownCounter() {
    return _wagedFailureCategoryCounters
        .get(HelixRebalanceException.FailureCategory.UNKNOWN).get();
  }

  @Override
  public long getWagedFallbackInUseGauge() {
    return _wagedFallbackInUse ? 1L : 0L;
  }

  @Override
  public long getWagedHardConstraintFaultZoneFailureCounter() {
    return _wagedHardConstraintFailureCounters.get(HardConstraint.Type.FAULT_ZONE).get();
  }

  @Override
  public long getWagedHardConstraintNodeCapacityFailureCounter() {
    return _wagedHardConstraintFailureCounters.get(HardConstraint.Type.NODE_CAPACITY).get();
  }

  @Override
  public long getWagedHardConstraintNodeMaxPartitionLimitFailureCounter() {
    return _wagedHardConstraintFailureCounters
        .get(HardConstraint.Type.NODE_MAX_PARTITION_LIMIT).get();
  }

  @Override
  public long getWagedHardConstraintReplicaActivateFailureCounter() {
    return _wagedHardConstraintFailureCounters.get(HardConstraint.Type.REPLICA_ACTIVATE).get();
  }

  @Override
  public long getWagedHardConstraintSamePartitionOnInstanceFailureCounter() {
    return _wagedHardConstraintFailureCounters
        .get(HardConstraint.Type.SAME_PARTITION_ON_INSTANCE).get();
  }

  @Override
  public long getWagedHardConstraintValidGroupTagFailureCounter() {
    return _wagedHardConstraintFailureCounters.get(HardConstraint.Type.VALID_GROUP_TAG).get();
  }

  @Override
  public long getWagedHardConstraintUnknownFailureCounter() {
    return _wagedHardConstraintFailureCounters.get(HardConstraint.Type.UNKNOWN).get();
  }

  @Override
  public long getWagedHardConstraintFaultZoneBlockingGauge() {
    return _wagedHardConstraintBlockingGauges.get(HardConstraint.Type.FAULT_ZONE).get();
  }

  @Override
  public long getWagedHardConstraintNodeCapacityBlockingGauge() {
    return _wagedHardConstraintBlockingGauges.get(HardConstraint.Type.NODE_CAPACITY).get();
  }

  @Override
  public long getWagedHardConstraintNodeMaxPartitionLimitBlockingGauge() {
    return _wagedHardConstraintBlockingGauges.get(HardConstraint.Type.NODE_MAX_PARTITION_LIMIT).get();
  }

  @Override
  public long getWagedHardConstraintReplicaActivateBlockingGauge() {
    return _wagedHardConstraintBlockingGauges.get(HardConstraint.Type.REPLICA_ACTIVATE).get();
  }

  @Override
  public long getWagedHardConstraintSamePartitionOnInstanceBlockingGauge() {
    return _wagedHardConstraintBlockingGauges.get(HardConstraint.Type.SAME_PARTITION_ON_INSTANCE).get();
  }

  @Override
  public long getWagedHardConstraintValidGroupTagBlockingGauge() {
    return _wagedHardConstraintBlockingGauges.get(HardConstraint.Type.VALID_GROUP_TAG).get();
  }

  @Override
  public long getWagedHardConstraintUnknownBlockingGauge() {
    return _wagedHardConstraintBlockingGauges.get(HardConstraint.Type.UNKNOWN).get();
  }

  @Override
  public double getEstimatedMaxClusterCapacityUsageGauge() {
    return _estimatedMaxClusterCapacityUsage;
  }

  @Override
  public long getTotalResourceGauge() {
    return _resourceMonitorMap.size();
  }

  @Override
  public long getTotalPartitionGauge() {
    long total = 0;
    for (Map.Entry<String, ResourceMonitor> entry : _resourceMonitorMap.entrySet()) {
      total += entry.getValue().getPartitionGauge();
    }
    return total;
  }

  @Override
  public long getErrorPartitionGauge() {
    long total = 0;
    for (Map.Entry<String, ResourceMonitor> entry : _resourceMonitorMap.entrySet()) {
      total += entry.getValue().getErrorPartitionGauge();
    }
    return total;
  }

  @Override
  public long getMissingTopStatePartitionGauge() {
    long total = 0;
    for (Map.Entry<String, ResourceMonitor> entry : _resourceMonitorMap.entrySet()) {
      total += entry.getValue().getMissingTopStatePartitionGauge();
    }
    return total;
  }

  @Override
  public long getMissingMinActiveReplicaPartitionGauge() {
    long total = 0;
    for (Map.Entry<String, ResourceMonitor> entry : _resourceMonitorMap.entrySet()) {
      total += entry.getValue().getMissingMinActiveReplicaPartitionGauge();
    }
    return total;
  }

  @Override
  public long getMissingReplicaPartitionGauge() {
    long total = 0;
    for (Map.Entry<String, ResourceMonitor> entry : _resourceMonitorMap.entrySet()) {
      total += entry.getValue().getMissingReplicaPartitionGauge();
    }
    return total;
  }

  @Override
  public long getDifferenceWithIdealStateGauge() {
    long total = 0;
    for (Map.Entry<String, ResourceMonitor> entry : _resourceMonitorMap.entrySet()) {
      total += entry.getValue().getDifferenceWithIdealStateGauge();
    }
    return total;
  }

  @Override
  public long getStateTransitionCounter() {
    long total = 0;
    for (Map.Entry<String, ResourceMonitor> entry : _resourceMonitorMap.entrySet()) {
      total += entry.getValue().getTotalMessageReceived();
    }
    return total;
  }

  @Override
  public long getPendingStateTransitionGuage() {
    long total = 0;
    for (Map.Entry<String, ResourceMonitor> entry : _resourceMonitorMap.entrySet()) {
      total += entry.getValue().getNumPendingStateTransitionGauge();
    }
    return total;
  }

  @Override
  public long getNumOfResourcesRebalanceThrottledGauge() {
    long total = 0;
    for (Map.Entry<String, ResourceMonitor> entry : _resourceMonitorMap.entrySet()) {
      total += entry.getValue().getRebalanceThrottledByErrorPartitionGauge();
    }
    return total;
  }
}
