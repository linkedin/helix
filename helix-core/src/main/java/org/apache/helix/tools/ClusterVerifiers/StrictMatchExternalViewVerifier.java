package org.apache.helix.tools.ClusterVerifiers;

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
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.helix.ConfigAccessor;
import org.apache.helix.HelixException;
import org.apache.helix.PropertyKey;
import org.apache.helix.controller.dataproviders.ResourceControllerDataProvider;
import org.apache.helix.controller.rebalancer.AbstractRebalancer;
import org.apache.helix.model.ClusterConfig;
import org.apache.helix.model.ExternalView;
import org.apache.helix.model.IdealState;
import org.apache.helix.model.Partition;
import org.apache.helix.model.StateModelDefinition;
import org.apache.helix.task.TaskConstants;
import org.apache.helix.util.HelixUtil;
import org.apache.helix.zookeeper.api.client.RealmAwareZkClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Verifier that verifies whether the ExternalViews of given resources (or all resources in the cluster)
 * match exactly as its ideal mapping (in idealstate).
 * To use this verifier on resources in Full-Auto mode, BestPossible state must be persisted in Cluster config.
 */
public class StrictMatchExternalViewVerifier extends ZkHelixClusterVerifier {
  private static Logger LOG = LoggerFactory.getLogger(StrictMatchExternalViewVerifier.class);

  private final Set<String> _resources;
  private final Set<String> _expectLiveInstances;
  private final boolean _isDeactivatedNodeAware;

  @Deprecated
  public StrictMatchExternalViewVerifier(String zkAddr, String clusterName, Set<String> resources,
      Set<String> expectLiveInstances) {
    this(zkAddr, clusterName, resources, expectLiveInstances, false, 0);
  }

  @Deprecated
  public StrictMatchExternalViewVerifier(RealmAwareZkClient zkClient, String clusterName,
      Set<String> resources, Set<String> expectLiveInstances) {
    // usesExternalZkClient = true because ZkClient is given by the caller
    // at close(), we will not close this ZkClient because it might be being used elsewhere
    super(zkClient, clusterName, true, 0);
    _resources = resources == null ? new HashSet<>() : new HashSet<>(resources);
    _expectLiveInstances =
        expectLiveInstances == null ? new HashSet<>() : new HashSet<>(expectLiveInstances);
    _isDeactivatedNodeAware = false;
  }

  @Deprecated
  private StrictMatchExternalViewVerifier(String zkAddr, String clusterName, Set<String> resources,
      Set<String> expectLiveInstances, boolean isDeactivatedNodeAware, int waitTillVerify) {
    super(zkAddr, clusterName, waitTillVerify);
    _resources = resources;
    _expectLiveInstances = expectLiveInstances;
    _isDeactivatedNodeAware = isDeactivatedNodeAware;
  }

  private StrictMatchExternalViewVerifier(RealmAwareZkClient zkClient, String clusterName,
      Set<String> resources, Set<String> expectLiveInstances, boolean isDeactivatedNodeAware,
      int waitPeriodTillVerify, boolean usesExternalZkClient) {
    // Initialize StrictMatchExternalViewVerifier with usesExternalZkClient = false so that
    // StrictMatchExternalViewVerifier::close() would close ZkClient to prevent thread leakage
    super(zkClient, clusterName, usesExternalZkClient, 0);
    _resources = resources == null ? new HashSet<>() : new HashSet<>(resources);
    _expectLiveInstances =
        expectLiveInstances == null ? new HashSet<>() : new HashSet<>(expectLiveInstances);
    _isDeactivatedNodeAware = isDeactivatedNodeAware;
  }

  public static class Builder extends ZkHelixClusterVerifier.Builder<Builder> {
    private final String _clusterName; // This is the ZK path sharding key
    private Set<String> _resources;
    private Set<String> _expectLiveInstances;
    private RealmAwareZkClient _zkClient;
    // For backward compatibility, set the default isDeactivatedNodeAware to be false.
    private boolean _isDeactivatedNodeAware = false;
    private boolean _usesExternalZkClient = false; // false by default

    public StrictMatchExternalViewVerifier build() {
      if (_clusterName == null) {
        throw new IllegalArgumentException("Cluster name is missing!");
      }

      if (_zkClient != null) {
        return new StrictMatchExternalViewVerifier(_zkClient, _clusterName, _resources,
            _expectLiveInstances, _isDeactivatedNodeAware, _waitPeriodTillVerify,
            _usesExternalZkClient);
      }

      if (_realmAwareZkConnectionConfig == null || _realmAwareZkClientConfig == null) {
        // For backward-compatibility
        return new StrictMatchExternalViewVerifier(_zkAddress, _clusterName, _resources,
            _expectLiveInstances, _isDeactivatedNodeAware, _waitPeriodTillVerify);
      }

      validate();
      return new StrictMatchExternalViewVerifier(
          createZkClient(RealmAwareZkClient.RealmMode.SINGLE_REALM, _realmAwareZkConnectionConfig,
              _realmAwareZkClientConfig, _zkAddress), _clusterName, _resources,
          _expectLiveInstances, _isDeactivatedNodeAware, _waitPeriodTillVerify,
          _usesExternalZkClient);
    }

    public Builder(String clusterName) {
      _clusterName = clusterName;
    }

    public String getClusterName() {
      return _clusterName;
    }

    public Set<String> getResources() {
      return _resources;
    }

    public Builder setResources(Set<String> resources) {
      _resources = resources;
      return this;
    }

    public Set<String> getExpectLiveInstances() {
      return _expectLiveInstances;
    }

    public Builder setExpectLiveInstances(Set<String> expectLiveInstances) {
      _expectLiveInstances = expectLiveInstances;
      return this;
    }

    public String getZkAddress() {
      return _zkAddress;
    }

    @Deprecated
    public Builder setZkClient(RealmAwareZkClient zkClient) {
      _zkClient = zkClient;
      _usesExternalZkClient = true; // Set the flag since external ZkClient is used
      return this;
    }

    public boolean getDeactivatedNodeAwareness() {
      return _isDeactivatedNodeAware;
    }

    public Builder setDeactivatedNodeAwareness(boolean isDeactivatedNodeAware) {
      _isDeactivatedNodeAware = isDeactivatedNodeAware;
      return this;
    }

    protected void validate() {
      super.validate();
      if (!_clusterName.equals(_realmAwareZkConnectionConfig.getZkRealmShardingKey())) {
        throw new IllegalArgumentException(
            "StrictMatchExternalViewVerifier: Cluster name: " + _clusterName
                + " and ZK realm sharding key: " + _realmAwareZkConnectionConfig
                .getZkRealmShardingKey() + " do not match!");
      }
    }
  }

  @Override
  public boolean verify(long timeout) {
    return verifyByZkCallback(timeout);
  }

  @Override
  public boolean verifyByZkCallback(long timeout) {
    waitTillVerify();

    List<ClusterVerifyTrigger> triggers = new ArrayList<ClusterVerifyTrigger>();

    // setup triggers
    if (_resources != null && !_resources.isEmpty()) {
      for (String resource : _resources) {
        triggers
            .add(new ClusterVerifyTrigger(_keyBuilder.idealStates(resource), true, false, false));
        triggers
            .add(new ClusterVerifyTrigger(_keyBuilder.externalView(resource), true, false, false));
      }

    } else {
      triggers.add(new ClusterVerifyTrigger(_keyBuilder.idealStates(), false, true, true));
      triggers.add(new ClusterVerifyTrigger(_keyBuilder.externalViews(), false, true, true));
    }

    return verifyByCallback(timeout, triggers);
  }

  @Override
  protected boolean verifyState() {
    try {
      PropertyKey.Builder keyBuilder = _accessor.keyBuilder();
      // read cluster once and do verification
      ResourceControllerDataProvider cache = new ResourceControllerDataProvider();
      cache.refresh(_accessor);

      Map<String, IdealState> idealStates = new HashMap<>(cache.getIdealStates());

      // filter out all resources that use Task state model
      Iterator<Map.Entry<String, IdealState>> it = idealStates.entrySet().iterator();
      while (it.hasNext()) {
        Map.Entry<String, IdealState> pair = it.next();
        if (pair.getValue().getStateModelDefRef().equals(TaskConstants.STATE_MODEL_NAME)) {
          it.remove();
        }
      }

      // verify live instances.
      if (_expectLiveInstances != null && !_expectLiveInstances.isEmpty()) {
        Set<String> actualLiveNodes = cache.getLiveInstances().keySet();
        if (!_expectLiveInstances.equals(actualLiveNodes)) {
          return false;
        }
      }

      Map<String, ExternalView> extViews =
          _accessor.getChildValuesMap(keyBuilder.externalViews(), true);
      if (extViews == null) {
        extViews = Collections.emptyMap();
      }

      // Filter resources if requested
      if (_resources != null && !_resources.isEmpty()) {
        idealStates.keySet().retainAll(_resources);
        extViews.keySet().retainAll(_resources);
      }

      // if externalView is not empty and idealState doesn't exist
      // add empty idealState for the resource
      for (String resource : extViews.keySet()) {
        if (!idealStates.containsKey(resource)) {
          idealStates.put(resource, new IdealState(resource));
        }
      }

      for (String resourceName : idealStates.keySet()) {
        ExternalView extView = extViews.get(resourceName);
        IdealState idealState = idealStates.get(resourceName);
        if (extView == null) {
          if (idealState.isExternalViewDisabled()) {
            continue;
          } else {
            LOG.debug("externalView for " + resourceName + " is not available");
            return false;
          }
        }

        boolean result = verifyExternalView(cache, extView, idealState);
        if (!result) {
          return false;
        }
      }
      return true;
    } catch (Exception e) {
      LOG.error("exception in verification", e);
      return false;
    }
  }

  private boolean verifyExternalView(ResourceControllerDataProvider dataCache, ExternalView externalView,
      IdealState idealState) {
    Map<String, Map<String, String>> mappingInExtview = externalView.getRecord().getMapFields();
    Map<String, Map<String, String>> idealPartitionState;

    switch (idealState.getRebalanceMode()) {
    case FULL_AUTO:
      ClusterConfig clusterConfig = new ConfigAccessor(_zkClient).getClusterConfig(dataCache.getClusterName());
      if (!clusterConfig.isPersistBestPossibleAssignment() && !clusterConfig.isPersistIntermediateAssignment()) {
        throw new HelixException(String.format("Full-Auto IdealState verifier requires "
            + "ClusterConfig.PERSIST_BEST_POSSIBLE_ASSIGNMENT or ClusterConfig.PERSIST_INTERMEDIATE_ASSIGNMENT "
            + "is enabled."));
      }
      for (String partition : idealState.getPartitionSet()) {
        if (idealState.getPreferenceList(partition) == null || idealState.getPreferenceList(partition).isEmpty()) {
          return false;
        }
      }
      idealPartitionState = computeIdealPartitionState(dataCache, idealState);
      break;
    case SEMI_AUTO:
    case USER_DEFINED:
      idealPartitionState = computeIdealPartitionState(dataCache, idealState);
      break;
    case CUSTOMIZED:
      idealPartitionState = idealState.getRecord().getMapFields();
      break;
    case TASK:
      // ignore jobs
    default:
      return true;
    }

    return mappingInExtview.equals(idealPartitionState);
  }

  private Map<String, Map<String, String>> computeIdealPartitionState(
      ResourceControllerDataProvider cache, IdealState idealState) {
    String stateModelDefName = idealState.getStateModelDefRef();
    StateModelDefinition stateModelDef = cache.getStateModelDef(stateModelDefName);

    Map<String, Map<String, String>> idealPartitionState = new HashMap<>();

    for (String partition : idealState.getPartitionSet()) {
      List<String> preferenceList = AbstractRebalancer.getPreferenceList(new Partition(partition),
          idealState, cache.getEnabledLiveInstances());
      Map<String, String> idealMapping;
      if (_isDeactivatedNodeAware) {
        idealMapping = HelixUtil.computeIdealMapping(preferenceList, stateModelDef,
            cache.getAssignableLiveInstances().keySet(),
                cache.getDisabledInstancesForPartition(idealState.getResourceName(), partition));
      } else {
        idealMapping = HelixUtil.computeIdealMapping(preferenceList, stateModelDef,
            cache.getEnabledLiveInstances(),
                Collections.emptySet());
      }
      idealPartitionState.put(partition, idealMapping);
    }

    return idealPartitionState;
  }

  @Override
  public String toString() {
    String verifierName = getClass().getSimpleName();
    return String
        .format("%s(%s@%s@resources[%s])", verifierName, _clusterName, _zkClient.getServers(),
            _resources != null ? Arrays.toString(_resources.toArray()) : "");
  }

  @Override
  public void finalize() {
    close();
  }
}
