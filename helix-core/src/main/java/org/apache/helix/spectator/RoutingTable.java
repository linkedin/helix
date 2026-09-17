package org.apache.helix.spectator;

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
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;

import org.apache.helix.PropertyType;
import org.apache.helix.model.CurrentState;
import org.apache.helix.model.ExternalView;
import org.apache.helix.model.InstanceConfig;
import org.apache.helix.model.LiveInstance;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A class to consume ExternalViews or CustomizedViews of a cluster and provide
 * {resource, partition, state} to {instances} map function.
 */
class RoutingTable {
  private static final Logger logger = LoggerFactory.getLogger(RoutingTable.class);

  // mapping a resourceName to the ResourceInfo
  private final Map<String, ResourceInfo> _resourceInfoMap;

  private final Collection<LiveInstance> _liveInstances;
  protected final Collection<InstanceConfig> _instanceConfigs;
  private final Collection<ExternalView> _externalViews;

  private final PropertyType _propertyType;

  @Deprecated
  public RoutingTable() {
    this(Collections.<ExternalView> emptyList(), Collections.<InstanceConfig> emptyList(),
        Collections.<LiveInstance> emptyList());
  }

  /**
   * Initialize empty RoutingTable and set _propertyType fields.
   * @param propertyType
   */
  protected RoutingTable(PropertyType propertyType) {
    this(Collections.<ExternalView> emptyList(), Collections.<InstanceConfig> emptyList(),
        Collections.<LiveInstance> emptyList(), propertyType);
  }

  public RoutingTable(Map<String, Map<String, Map<String, CurrentState>>> currentStateMap,
      Collection<InstanceConfig> instanceConfigs, Collection<LiveInstance> liveInstances) {
    // TODO Aggregate currentState to an ExternalView in the RoutingTable, so there is no need to
    // refresh according to the currentStateMap. - jjwang
    this(Collections.<ExternalView> emptyList(),
        instanceConfigs, liveInstances, PropertyType.CURRENTSTATES);
    refresh(currentStateMap);
  }

  public RoutingTable(Collection<ExternalView> externalViews,
      Collection<InstanceConfig> instanceConfigs, Collection<LiveInstance> liveInstances) {
    this(externalViews, instanceConfigs, liveInstances,
        PropertyType.EXTERNALVIEW);
  }

  protected RoutingTable(Collection<ExternalView> externalViews, Collection<InstanceConfig> instanceConfigs,
      Collection<LiveInstance> liveInstances, PropertyType propertytype) {
    // TODO Refactor these constructors so we don't have so many constructor.
    _propertyType = propertytype;
    _resourceInfoMap = new HashMap<>();
    _liveInstances = new HashSet<>(liveInstances);
    _instanceConfigs = new HashSet<>(instanceConfigs);
    _externalViews = new HashSet<>(externalViews);
    refresh(_externalViews);
  }

  private void refresh(Collection<ExternalView> externalViewList) {
    Map<String, InstanceConfig> instanceConfigMap = new HashMap<>();
    if (externalViewList != null && !externalViewList.isEmpty()) {
      for (InstanceConfig config : _instanceConfigs) {
        instanceConfigMap.put(config.getId(), config);
      }
      for (ExternalView extView : externalViewList) {
        String resourceName = extView.getId();
        for (String partitionName : extView.getPartitionSet()) {
          Map<String, String> stateMap = extView.getStateMap(partitionName);
          for (String instanceName : stateMap.keySet()) {
            String currentState = stateMap.get(instanceName);
            if (instanceConfigMap.containsKey(instanceName)) {
              InstanceConfig instanceConfig = instanceConfigMap.get(instanceName);
              addEntry(resourceName, partitionName, currentState, instanceConfig);
            } else {
              logger.warn(
                  "Participant {} is not found with proper configuration information. It might already be removed from the cluster. "
                      + "Skip recording partition assignment entry: Partition {}, Participant {}, State {}.",
                  instanceName, partitionName, instanceName, stateMap.get(instanceName));
            }
          }
        }
      }
    }
  }

  private void refresh(Map<String, Map<String, Map<String, CurrentState>>> currentStateMap) {
    Map<String, InstanceConfig> instanceConfigMap = new HashMap<>();
    if (currentStateMap != null && !currentStateMap.isEmpty()) {
      for (InstanceConfig config : _instanceConfigs) {
        instanceConfigMap.put(config.getId(), config);
      }
      for (LiveInstance liveInstance : _liveInstances) {
        String instanceName = liveInstance.getInstanceName();
        String sessionId = liveInstance.getEphemeralOwner();
        InstanceConfig instanceConfig = instanceConfigMap.get(instanceName);
        if (instanceConfig == null) {
          logger.warn(
              "Participant {} is not found with proper configuration information. It might already be removed from the cluster. "
                  + "Skip recording partition assignments that are related to this instance.",
              instanceName);
          continue;
        }

        Map<String, CurrentState> currentStates = Collections.emptyMap();
        if (currentStateMap.containsKey(instanceName)
            && currentStateMap.get(instanceName).containsKey(sessionId)) {
          currentStates = currentStateMap.get(instanceName).get(sessionId);
        }

        for (CurrentState currentState : currentStates.values()) {
          String resourceName = currentState.getResourceName();
          Map<String, String> stateMap = currentState.getPartitionStateMap();

          for (String partitionName : stateMap.keySet()) {
            String state = stateMap.get(partitionName);
            addEntry(resourceName, partitionName, state, instanceConfig);
          }
        }
      }
    }
  }

  protected void addEntry(String resourceName, String partitionName, String state,
      InstanceConfig config) {
    if (!_resourceInfoMap.containsKey(resourceName)) {
      _resourceInfoMap.put(resourceName, new ResourceInfo());
    }
    ResourceInfo resourceInfo = _resourceInfoMap.get(resourceName);
    resourceInfo.addEntry(partitionName, state, config);
  }

  ResourceInfo get(String resourceName) {
    return _resourceInfoMap.get(resourceName);
  }

  /**
   * returns all instances for {resource} that are in a specific {state}.
   * @param resourceName
   * @param state
   * @return empty list if there is no instance in a given state
   */
  public Set<InstanceConfig> getInstancesForResource(String resourceName, String state) {
    Set<InstanceConfig> instanceSet = null;
    ResourceInfo resourceInfo = get(resourceName);
    if (resourceInfo != null) {
      instanceSet = resourceInfo.getInstances(state);
    }
    if (instanceSet == null) {
      instanceSet = Collections.emptySet();
    }
    return instanceSet;
  }

  /**
   * returns the instances for {resource,partition} pair that are in a specific
   * {state}
   * @param resourceName
   * @param partitionName
   * @param state
   * @return empty list if there is no instance in a given state
   */
  public List<InstanceConfig> getInstancesForResource(String resourceName, String partitionName,
      String state) {
    List<InstanceConfig> instanceList = null;
    ResourceInfo resourceInfo = get(resourceName);
    if (resourceInfo != null) {
      PartitionInfo keyInfo = resourceInfo.get(partitionName);
      if (keyInfo != null) {
        instanceList = keyInfo.get(state);
      }
    }
    if (instanceList == null) {
      instanceList = Collections.emptyList();
    }
    return instanceList;
  }

  /**
   * Return all liveInstances in the cluster now.
   * @return
   */
  protected Collection<LiveInstance> getLiveInstances() {
    return Collections.unmodifiableCollection(_liveInstances);
  }

  /**
   * Return all instance's config in this cluster.
   * @return
   */
  protected Collection<InstanceConfig> getInstanceConfigs() {
    return Collections.unmodifiableCollection(_instanceConfigs);
  }

  /**
   * Return names of all resources (shown in ExternalView) in this cluster.
   */
  protected Collection<String> getResources() {
    return Collections.unmodifiableCollection(_resourceInfoMap.keySet());
  }

  /**
   * Returns ExternalViews.
   * @return a collection of ExternalViews
   */
  protected Collection<ExternalView> getExternalViews() {
    return Collections.unmodifiableCollection(_externalViews);
  }

  /**
   * Returns PropertyTYpe
   * @return the PropertyTYpe of this RoutingTable
   */
  protected PropertyType getPropertyType() {
    return _propertyType;
  }

  /**
   * Returns CustomizedStateType
   * @return the CustomizedStateType of this RoutingTable (Used for CustomizedView)
   */
  protected String getStateType() {
    return RoutingTableProvider.DEFAULT_STATE_TYPE;
  }


  /**
   * Class to store instances, partitions and their states for each resource.
   */
  class ResourceInfo {
    // store PartitionInfo for each partition
    Map<String, PartitionInfo> partitionInfoMap;
    // stores the Set of Instances in a given state
    Map<String, Set<InstanceConfig>> stateInfoMap;

    public ResourceInfo() {
      partitionInfoMap = new HashMap<>();
      stateInfoMap = new HashMap<>();
    }

    public void addEntry(String stateUnitKey, String state, InstanceConfig config) {
      if (!stateInfoMap.containsKey(state)) {
        stateInfoMap.put(state, new TreeSet<>(INSTANCE_CONFIG_COMPARATOR));
      }
      Set<InstanceConfig> set = stateInfoMap.get(state);
      set.add(config);

      if (!partitionInfoMap.containsKey(stateUnitKey)) {
        partitionInfoMap.put(stateUnitKey, new PartitionInfo());
      }
      PartitionInfo stateUnitKeyInfo = partitionInfoMap.get(stateUnitKey);
      stateUnitKeyInfo.addEntry(state, config);
    }

    public Set<InstanceConfig> getInstances(String state) {
      return stateInfoMap.get(state);
    }

    PartitionInfo get(String stateUnitKey) {
      return partitionInfoMap.get(stateUnitKey);
    }
  }

  class PartitionInfo {
    Map<String, List<InstanceConfig>> stateInfoMap;

    public PartitionInfo() {
      stateInfoMap = new HashMap<>();
    }

    public void addEntry(String state, InstanceConfig config) {
      if (!stateInfoMap.containsKey(state)) {
        stateInfoMap.put(state, new ArrayList<InstanceConfig>());
      }
      List<InstanceConfig> list = stateInfoMap.get(state);
      list.add(config);
    }

    List<InstanceConfig> get(String state) {
      return stateInfoMap.get(state);
    }
  }

  private static Comparator<InstanceConfig> INSTANCE_CONFIG_COMPARATOR =
      new Comparator<InstanceConfig>() {
        @Override
        public int compare(InstanceConfig config1, InstanceConfig config2) {
          if (config1 == config2) {
            return 0;
          }
          if (config1 == null) {
            return -1;
          }
          if (config2 == null) {
            return 1;
          }
          // HELIX-936: a NPE on the hostname; compare IDs instead. IDs for InstanceConfigs are
          // concatenation of instance name, host, and port.
          return config1.getId().compareTo(config2.getId());
        }
      };
}
