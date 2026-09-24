package org.apache.helix.model;

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

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableMap;
import org.apache.helix.HelixProperty;
import org.apache.helix.api.config.HelixConfigProperty;
import org.apache.helix.api.config.RebalanceConfig;
import org.apache.helix.api.config.StateTransitionTimeoutConfig;
import org.apache.helix.zookeeper.datamodel.ZNRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Resource configurations
 */
public class ResourceConfig extends HelixProperty {
  /**
   * Configurable characteristics of a resource
   */
  public enum ResourceConfigProperty {
    MONITORING_DISABLED, // Resource-level config, do not create Mbean and report any status for the resource.
    STATE_MODEL_FACTORY_NAME,
    MIN_ACTIVE_REPLICAS,
    MAX_PARTITIONS_PER_INSTANCE,
    DELAY_REBALANCE_ENABLED,
    PARTITION_CAPACITY_MAP,
    ACTIVE_STATES_FOR_MIN_ACTIVE_REPLICA_CHECK // List of states to be considered as "active" for min active replica check
  }

  public enum ResourceConfigConstants {
    ANY_LIVEINSTANCE
  }

  private static final Logger _logger = LoggerFactory.getLogger(ResourceConfig.class.getName());
  private static final ObjectMapper _objectMapper = new ObjectMapper();

  public static final String DEFAULT_PARTITION_KEY = "DEFAULT";
  private Map<String, Map<String, Integer>> _deserializedPartitionCapacityMap;

  /**
   * Instantiate for a specific instance
   *
   * @param resourceId the instance identifier
   */
  public ResourceConfig(String resourceId) {
    super(resourceId);
  }

  /**
   * Instantiate with a pre-populated record
   *
   * @param record a ZNRecord corresponding to an instance configuration
   */
  public ResourceConfig(ZNRecord record) {
    super(record);
  }

  /**
   * Instantiate with a pre-populated record with new record id
   * @param record a ZNRecord corresponding to an instance configuration
   * @param id     new ZNRecord ID
   */
  public ResourceConfig(ZNRecord record, String id) {
    super(record, id);
  }

  public ResourceConfig(String resourceId, Boolean monitorDisabled,
      String stateModelFactoryName,
      int minActiveReplica, int maxPartitionsPerInstance,
      RebalanceConfig rebalanceConfig,
      StateTransitionTimeoutConfig stateTransitionTimeoutConfig,
      Map<String, List<String>> listFields, Map<String, Map<String, String>> mapFields,
      Boolean p2pMessageEnabled) {
    this(resourceId, monitorDisabled, stateModelFactoryName,
        minActiveReplica, maxPartitionsPerInstance,
        rebalanceConfig, stateTransitionTimeoutConfig, listFields, mapFields,
        p2pMessageEnabled, null);
  }

  private ResourceConfig(String resourceId, Boolean monitorDisabled,
      String stateModelFactoryName,
      int minActiveReplica, int maxPartitionsPerInstance,
      RebalanceConfig rebalanceConfig,
      StateTransitionTimeoutConfig stateTransitionTimeoutConfig,
      Map<String, List<String>> listFields, Map<String, Map<String, String>> mapFields,
      Boolean p2pMessageEnabled, Map<String, Map<String, Integer>> partitionCapacityMap) {
    super(resourceId);

    if (monitorDisabled != null) {
      _record.setBooleanField(ResourceConfigProperty.MONITORING_DISABLED.name(), monitorDisabled);
    }

    if (p2pMessageEnabled != null) {
      _record.setBooleanField(HelixConfigProperty.P2P_MESSAGE_ENABLED.name(), p2pMessageEnabled);
    }

    if (stateModelFactoryName != null) {
      _record.setSimpleField(ResourceConfigProperty.STATE_MODEL_FACTORY_NAME.name(), stateModelFactoryName);
    }

    if (minActiveReplica >= 0) {
      _record.setIntField(ResourceConfigProperty.MIN_ACTIVE_REPLICAS.name(), minActiveReplica);
    }

    if (maxPartitionsPerInstance >= 0) {
      _record.setIntField(ResourceConfigProperty.MAX_PARTITIONS_PER_INSTANCE.name(), maxPartitionsPerInstance);
    }

    if (rebalanceConfig != null) {
      putSimpleConfigs(rebalanceConfig.getConfigsMap());
    }

    if (stateTransitionTimeoutConfig != null) {
      putMapConfig(StateTransitionTimeoutConfig.StateTransitionTimeoutProperty.TIMEOUT.name(),
          stateTransitionTimeoutConfig.getTimeoutMap());
    }

    if (listFields != null) {
      _record.setListFields(listFields);
    }

    if (mapFields != null) {
      _record.setMapFields(mapFields);
    }

    if (partitionCapacityMap != null) {
      try {
        setPartitionCapacityMap(partitionCapacityMap);
      } catch (IOException e) {
        throw new IllegalArgumentException(
            "Failed to set partition capacity. Invalid capacity configuration.");
      }
    }
  }


  /**
   * Get the value of DisableMonitoring set.
   *
   * @return the MonitoringDisabled is true or false
   */
  public Boolean isMonitoringDisabled() {
    return _record.getBooleanField(ResourceConfigProperty.MONITORING_DISABLED.toString(), false);
  }

  /**
   * Whether the P2P state transition message is enabled for this resource.
   * By default it is disabled if not set.
   *
   * @return
   */
  public boolean isP2PMessageEnabled() {
    return _record.getBooleanField(HelixConfigProperty.P2P_MESSAGE_ENABLED.name(), false);
  }

  /**
   * Get the associated resource
   * @return the name of the resource
   */
  public String getResourceName() {
    return _record.getId();
  }

  /**
   * Get the state model factory associated with this resource
   * @return state model factory name
   */
  public String getStateModelFactoryName() {
    return _record.getSimpleField(ResourceConfigProperty.STATE_MODEL_FACTORY_NAME.name());
  }

  /**
   * Get the number of minimal active partitions for this resource.
   *
   * @return
   */
  public int getMinActiveReplica() {
    return _record.getIntField(ResourceConfigProperty.MIN_ACTIVE_REPLICAS.name(), -1);
  }

  // Delimiter used for storing active states as a comma-separated string in simpleField
  private static final String ACTIVE_STATES_DELIMITER = ",";

  /**
   * Get the list of states that should be considered as "active" for min active replica check.
   * If not configured, the default behavior applies (all states except DROPPED, ERROR, and initial state).
   * 
   * Note: The configured states should be valid states from the resource's state model definition.
   * Any state NOT in this list will NOT count as active (including top states like LEADER).
   *
   * @return List of state names to be considered active, or null if not configured
   */
  public List<String> getActiveStatesForMinActiveReplicaCheck() {
    String statesStr = _record.getSimpleField(
        ResourceConfigProperty.ACTIVE_STATES_FOR_MIN_ACTIVE_REPLICA_CHECK.name());
    if (statesStr == null || statesStr.isEmpty()) {
      return null;
    }
    return Arrays.asList(statesStr.split(ACTIVE_STATES_DELIMITER));
  }

  /**
   * Set the list of states that should be considered as "active" for min active replica check.
   * When configured, only replicas in these states will count toward the min active replica constraint.
   * 
   * IMPORTANT:
   * - The configured states should be valid states from the resource's state model definition.
   * - If a state is NOT in this list, it will NOT be counted as active, even if it's a top state like LEADER.
   *   For example, if you configure ["STANDBY"] only, then LEADER replicas will NOT count as active.
   * - State names are matched case-insensitively.
   * 
   * Example: For a state model with states [LEADER, STANDBY, BOOTSTRAP, OFFLINE],
   * setting this to ["LEADER", "STANDBY"] will only count replicas in LEADER or STANDBY states.
   *
   * @param activeStates List of state names to be considered active, or null to use default behavior
   */
  public void setActiveStatesForMinActiveReplicaCheck(List<String> activeStates) {
    if (activeStates == null || activeStates.isEmpty()) {
      _record.getSimpleFields().remove(
          ResourceConfigProperty.ACTIVE_STATES_FOR_MIN_ACTIVE_REPLICA_CHECK.name());
    } else {
      _record.setSimpleField(
          ResourceConfigProperty.ACTIVE_STATES_FOR_MIN_ACTIVE_REPLICA_CHECK.name(),
          String.join(ACTIVE_STATES_DELIMITER, activeStates));
    }
  }

  public int getMaxPartitionsPerInstance() {
    return _record.getIntField(ResourceConfigProperty.MAX_PARTITIONS_PER_INSTANCE.toString(),
        Integer.MAX_VALUE);
  }

  /**
   * Get rebalance config for this resource.
   * @return
   */
  public RebalanceConfig getRebalanceConfig() {
    RebalanceConfig rebalanceConfig = new RebalanceConfig(_record);
    return rebalanceConfig;
  }

  public StateTransitionTimeoutConfig getStateTransitionTimeoutConfig() {
    return StateTransitionTimeoutConfig.fromRecord(_record);
  }


  /**
   * Get the user-specified preference lists for all partitions
   *
   * @return map of lists of instances for all partitions in this resource.
   */
  public Map<String, List<String>> getPreferenceLists() {
    return _record.getListFields();
  }

  /**
   * Get the user-specified preference list of a partition
   * @param partitionName the name of the partition
   * @return a list of instances that can serve replicas of the partition
   */
  public List<String> getPreferenceList(String partitionName) {
    List<String> instanceStateList = _record.getListField(partitionName);

    if (instanceStateList != null) {
      return instanceStateList;
    }

    return null;
  }

  /**
   * Set the user-specified preference lists for all partitions in this resource.
   *
   * @param instanceLists the map of instance preference lists.N
   */
  public void setPreferenceLists(Map<String, List<String>> instanceLists) {
    _record.setListFields(instanceLists);
  }

  /**
   * Get the partition capacity information from a JSON among the map fields.
   * <PartitionName or DEFAULT_PARTITION_KEY, <Capacity Key, Capacity Number>>
   *
   * @return data map if it exists, or empty map
   * @throws IOException - when JSON conversion fails
   */
  public Map<String, Map<String, Integer>> getPartitionCapacityMap() throws IOException {
    // It is very expensive to deserialize the partition capacity map every time this is called.
    // Cache the deserialized map to avoid the overhead.
    if (_deserializedPartitionCapacityMap != null && !_deserializedPartitionCapacityMap.isEmpty()) {
      return _deserializedPartitionCapacityMap;
    }

    Map<String, String> partitionCapacityData =
        _record.getMapField(ResourceConfigProperty.PARTITION_CAPACITY_MAP.name());
    Map<String, Map<String, Integer>> partitionCapacityMap = new HashMap<>();
    if (partitionCapacityData != null) {
      for (String partition : partitionCapacityData.keySet()) {
        Map<String, Integer> capacities = _objectMapper
            .readValue(partitionCapacityData.get(partition),
                new TypeReference<Map<String, Integer>>() {
                });
        partitionCapacityMap.put(partition, capacities);
      }
    }

    // Only set the deserialized map when the deserialization succeeds, so we don't have the potential
    // of having a partially populated map.
    _deserializedPartitionCapacityMap = partitionCapacityMap;
    return _deserializedPartitionCapacityMap;
  }

  /**
   * Set the partition capacity information with a map <PartitionName or DEFAULT_PARTITION_KEY, <Capacity Key, Capacity Number>>
   *
   * @param partitionCapacityMap - map of partition capacity data
   * @throws IllegalArgumentException - when any of the data value is a negative number or map is incomplete
   * @throws IOException              - when JSON parsing fails
   */
  public void setPartitionCapacityMap(Map<String, Map<String, Integer>> partitionCapacityMap)
      throws IllegalArgumentException, IOException {
    if (partitionCapacityMap == null) {
      throw new IllegalArgumentException("Capacity Map is null");
    }
    if (!partitionCapacityMap.containsKey(DEFAULT_PARTITION_KEY)) {
      throw new IllegalArgumentException(String
          .format("The default partition capacity with the default key %s is required.",
              DEFAULT_PARTITION_KEY));
    }

    Map<String, String> newCapacityRecord = new HashMap<>();
    // We want a copy of the partitionCapacityMap, so that the caller can no longer modify the
    // _deserializedPartitionCapacityMap after this call through their reference to partitionCapacityMap.
    Map<String, Map<String, Integer>> newDeserializedPartitionCapacityMap = new HashMap<>();
    for (String partition : partitionCapacityMap.keySet()) {
      Map<String, Integer> capacities = partitionCapacityMap.get(partition);
      // Verify the input is valid
      if (capacities.isEmpty()) {
        throw new IllegalArgumentException("Capacity Data is empty");
      }
      if (capacities.entrySet().stream().anyMatch(entry -> entry.getValue() < 0)) {
        throw new IllegalArgumentException(
            String.format("Capacity Data contains a negative value:%s", capacities.toString()));
      }
      newCapacityRecord.put(partition, _objectMapper.writeValueAsString(capacities));
      newDeserializedPartitionCapacityMap.put(partition, ImmutableMap.copyOf(capacities));
    }

    _record.setMapField(ResourceConfigProperty.PARTITION_CAPACITY_MAP.name(), newCapacityRecord);
    // Set deserialize map after we have successfully added it to the record.
    _deserializedPartitionCapacityMap = newDeserializedPartitionCapacityMap;
  }

  /**
   * Put a set of simple configs.
   *
   * @param configsMap
   */
  public void putSimpleConfigs(Map<String, String> configsMap) {
    getRecord().getSimpleFields().putAll(configsMap);
  }

  /**
   * Get all simple configurations.
   *
   * @return all simple configurations.
   */
  public Map<String, String> getSimpleConfigs() {
    return Collections.unmodifiableMap(getRecord().getSimpleFields());
  }

  /**
   * Put a single simple config value.
   *
   * @param configKey
   * @param configVal
   */
  public void putSimpleConfig(String configKey, String configVal) {
    getRecord().getSimpleFields().put(configKey, configVal);
  }

  /**
   * Get a single simple config value.
   *
   * @param configKey
   * @return configuration value, or NULL if not exist.
   */
  public String getSimpleConfig(String configKey) {
    return getRecord().getSimpleFields().get(configKey);
  }

  /**
   * Put a single map config.
   *
   * @param configKey
   * @param configValMap
   */
  public void putMapConfig(String configKey, Map<String, String> configValMap) {
    getRecord().setMapField(configKey, configValMap);
  }

  /**
   * Get a single map config.
   *
   * @param configKey
   * @return configuration value map, or NULL if not exist.
   */
  public Map<String, String> getMapConfig(String configKey) {
    return getRecord().getMapField(configKey);
  }

  /**
   * Determine whether the given config key is in the simple config
   * @param configKey The key to check whether exists
   * @return True if exists, otherwise false
   */
  public boolean simpleConfigContains(String configKey) {
    return getRecord().getSimpleFields().containsKey(configKey);
  }

  /**
   * Determine whether the given config key is the map config
   * @param configKey The key to check whether exists
   * @return True if exists, otherwise false
   */
  public boolean mapConfigContains(String configKey) {
    return getRecord().getMapFields().containsKey(configKey);
  }

  /**
   * Get the stored map fields
   * @return a map of map fields
   */
  public Map<String, Map<String, String>> getMapConfigs() {
    return getRecord().getMapFields();
  }

  @Override
  public boolean equals(Object obj) {
    if (obj instanceof ResourceConfig) {
      ResourceConfig that = (ResourceConfig) obj;

      if (this.getId().equals(that.getId())) {
        return true;
      }
    }
    return false;
  }

  @Override
  public int hashCode() {
    return getId().hashCode();
  }

  @Override
  public boolean isValid() {
    return true;
  }

  public static class Builder {
    private String _resourceId;
    private Boolean _monitorDisabled;
    private String _stateModelFactoryName;
    private int _minActiveReplica = -1;
    private int _maxPartitionsPerInstance = -1;
    private Boolean _p2pMessageEnabled;
    private RebalanceConfig _rebalanceConfig;
    private StateTransitionTimeoutConfig _stateTransitionTimeoutConfig;
    private Map<String, List<String>> _preferenceLists;
    private Map<String, Map<String, String>> _mapFields;
    private Map<String, Map<String, Integer>> _partitionCapacityMap;

    public Builder(String resourceId) {
      _resourceId = resourceId;
    }

    public Builder setMonitorDisabled(boolean monitorDisabled) {
      _monitorDisabled = monitorDisabled;
      return this;
    }

    /**
     * Enable/Disable the p2p state transition message for this resource.
     * By default it is disabled if not set.
     *
     * @param enabled
     */
    public Builder setP2PMessageEnabled(boolean enabled) {
      _p2pMessageEnabled = enabled;
      return this;
    }

    public Boolean isMonitorDisabled() {
      return _monitorDisabled;
    }

    public String getResourceId() {
      return _resourceId;
    }

    public String getStateModelFactoryName() {
      return _stateModelFactoryName;
    }

    public Builder setStateModelFactoryName(String stateModelFactoryName) {
      _stateModelFactoryName = stateModelFactoryName;
      return this;
    }

    public int getMinActiveReplica() {
      return _minActiveReplica;
    }

    public Builder setMinActiveReplica(int minActiveReplica) {
      _minActiveReplica = minActiveReplica;
      return this;
    }

    public int getMaxPartitionsPerInstance() {
      return _maxPartitionsPerInstance;
    }

    public Builder setMaxPartitionsPerInstance(int maxPartitionsPerInstance) {
      _maxPartitionsPerInstance = maxPartitionsPerInstance;
      return this;
    }

    public Builder setRebalanceConfig(RebalanceConfig rebalanceConfig) {
      _rebalanceConfig = rebalanceConfig;
      return this;
    }

    public RebalanceConfig getRebalanceConfig() {
      return _rebalanceConfig;
    }

    public Builder setStateTransitionTimeoutConfig(
        StateTransitionTimeoutConfig stateTransitionTimeoutConfig) {
      _stateTransitionTimeoutConfig = stateTransitionTimeoutConfig;
      return this;
    }

    public StateTransitionTimeoutConfig getStateTransitionTimeoutConfig() {
      return _stateTransitionTimeoutConfig;
    }

    /**
     * Set the user-specified preference list of a partition
     *
     * @param partitionName the name of the partition
     * @param instanceList the instance preference list
     */
    public Builder setPreferenceList(String partitionName, List<String> instanceList) {
      if (_preferenceLists == null) {
        _preferenceLists = new TreeMap<>();
      }
      _preferenceLists.put(partitionName, instanceList);
      return this;
    }

    /**
     * Set the user-specified preference lists for all partitions in this resource.
     *
     * @param instanceLists the map of instance preference lists.N
     */
    public Builder setPreferenceLists(Map<String, List<String>> instanceLists) {
      _preferenceLists = new TreeMap<>(instanceLists);
      return this;
    }

    public Map<String, List<String>> getPreferenceLists() {
      return _preferenceLists;
    }

    public Builder setPartitionCapacity(Map<String, Integer> defaultCapacity) {
      setPartitionCapacity(DEFAULT_PARTITION_KEY, defaultCapacity);
      return this;
    }

    public Builder setPartitionCapacity(String partition, Map<String, Integer> capacity) {
      if (_partitionCapacityMap == null) {
        _partitionCapacityMap = new HashMap<>();
      }
      _partitionCapacityMap.put(partition, capacity);
      return this;
    }

    public Map<String, Integer> getPartitionCapacity(String partition) {
      return _partitionCapacityMap.get(partition);
    }

    public Builder setMapField(String key, Map<String, String> fields) {
      if (_mapFields == null) {
        _mapFields = new TreeMap<>();
      }
      _mapFields.put(key, fields);
      return this;
    }

    public Builder setMapFields(Map<String, Map<String, String>> mapFields) {
      _mapFields = mapFields;
      return this;
    }

    public Map<String, Map<String, String>> getMapFields() {
      return _mapFields;
    }

    private void validate() {
      if (_rebalanceConfig == null) {
        throw new IllegalArgumentException("RebalanceConfig not set!");
      } else {
        if (!_rebalanceConfig.isValid()) {
          throw new IllegalArgumentException("Invalid RebalanceConfig!");
        }
      }
      if (_partitionCapacityMap != null) {
        if (_partitionCapacityMap.keySet().stream()
            .noneMatch(partition -> partition.equals(DEFAULT_PARTITION_KEY))) {
          throw new IllegalArgumentException(
              "Partition capacity is configured without the DEFAULT capacity!");
        }
        if (_partitionCapacityMap.values().stream()
            .anyMatch(capacity -> capacity.values().stream().anyMatch(value -> value < 0))) {
          throw new IllegalArgumentException(
              "Partition capacity is configured with negative capacity value!");
        }
      }
    }

    public ResourceConfig build() {
      // TODO: Reenable the validation in the future when ResourceConfig is ready.
      // validate();

      return new ResourceConfig(_resourceId, _monitorDisabled,
          _stateModelFactoryName, _minActiveReplica, _maxPartitionsPerInstance,
          _rebalanceConfig,
          _stateTransitionTimeoutConfig, _preferenceLists, _mapFields, _p2pMessageEnabled,
          _partitionCapacityMap);
    }
  }

  /**
   * For backward compatibility, propagate the critical simple fields from the IdealState to
   * the Resource Config.
   * Instance-group placement tags are read directly from IdealState and are not propagated here.
   *
   * Note that the config fields get updated in this method shall be fully compatible with ones in the IdealState.
   *  1. The fields shall have exactly the same meaning.
   *  2. The value shall be fully compatible, no additional calculation involved.
   *  3. Resource Config items have a high priority.
   */
  public static ResourceConfig mergeIdealStateWithResourceConfig(
      final ResourceConfig resourceConfig, final IdealState idealState) {
    if (idealState == null) {
      return resourceConfig;
    }
    ResourceConfig mergedResourceConfig;
    if (resourceConfig != null) {
      if (!resourceConfig.getResourceName().equals(idealState.getResourceName())) {
        throw new IllegalArgumentException(String.format(
            "Cannot merge the IdealState of resource %s with the ResourceConfig of resource %s",
            resourceConfig.getResourceName(), idealState.getResourceName()));
      }
      // Copy the resource config to avoid the original value being modified unexpectedly.
      mergedResourceConfig = new ResourceConfig(resourceConfig.getRecord());
    } else {
      // If no resource config specified, construct one based on the Idealstate.
      mergedResourceConfig = new ResourceConfig(idealState.getResourceName());
    }
    // Fill the compatible Idealstate fields to the ResourceConfig if possible.
    ZNRecord mergedZNRecord = mergedResourceConfig.getRecord();
    mergedZNRecord.setIntFieldIfAbsent(
        ResourceConfig.ResourceConfigProperty.MAX_PARTITIONS_PER_INSTANCE.name(),
        idealState.getMaxPartitionsPerInstance());
    mergedZNRecord.setSimpleFieldIfAbsent(
        ResourceConfig.ResourceConfigProperty.STATE_MODEL_FACTORY_NAME.name(),
        idealState.getStateModelFactoryName());
    mergedZNRecord
        .setIntFieldIfAbsent(ResourceConfig.ResourceConfigProperty.MIN_ACTIVE_REPLICAS.name(),
            idealState.getMinActiveReplicas());
    mergedZNRecord.setBooleanFieldIfAbsent(
        ResourceConfig.ResourceConfigProperty.DELAY_REBALANCE_ENABLED.name(),
        idealState.isDelayRebalanceEnabled());
    return mergedResourceConfig;
  }
}
