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
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.helix.PropertyKey;
import org.apache.helix.controller.dataproviders.ResourceControllerDataProvider;
import org.apache.helix.util.ExternalViewConvergenceEvaluator;
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
  private final boolean _isLenientMatch;

  @Deprecated
  public StrictMatchExternalViewVerifier(String zkAddr, String clusterName, Set<String> resources,
      Set<String> expectLiveInstances) {
    this(zkAddr, clusterName, resources, expectLiveInstances, false, false, 0);
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
    _isLenientMatch = false;
  }

  @Deprecated
  private StrictMatchExternalViewVerifier(String zkAddr, String clusterName, Set<String> resources,
      Set<String> expectLiveInstances, boolean isDeactivatedNodeAware, boolean isLenientMatch,
      int waitTillVerify) {
    super(zkAddr, clusterName, waitTillVerify);
    _resources = resources;
    _expectLiveInstances = expectLiveInstances;
    _isDeactivatedNodeAware = isDeactivatedNodeAware;
    _isLenientMatch = isLenientMatch;
  }

  private StrictMatchExternalViewVerifier(RealmAwareZkClient zkClient, String clusterName,
      Set<String> resources, Set<String> expectLiveInstances, boolean isDeactivatedNodeAware,
      boolean isLenientMatch, int waitPeriodTillVerify, boolean usesExternalZkClient) {
    // When usesExternalZkClient is false, close() will close the client to prevent thread leakage.
    super(zkClient, clusterName, usesExternalZkClient, waitPeriodTillVerify);
    _resources = resources == null ? new HashSet<>() : new HashSet<>(resources);
    _expectLiveInstances =
        expectLiveInstances == null ? new HashSet<>() : new HashSet<>(expectLiveInstances);
    _isDeactivatedNodeAware = isDeactivatedNodeAware;
    _isLenientMatch = isLenientMatch;
  }

  public static class Builder extends ZkHelixClusterVerifier.Builder<Builder> {
    private final String _clusterName; // This is the ZK path sharding key
    private Set<String> _resources;
    private Set<String> _expectLiveInstances;
    private RealmAwareZkClient _zkClient;
    // For backward compatibility, set the default isDeactivatedNodeAware to be false.
    private boolean _isDeactivatedNodeAware = false;
    // For backward compatibility, set the default isLenientMatch to be false.
    // When true, OFFLINE (initialState) and DROPPED entries are stripped before comparison.
    private boolean _isLenientMatch = false;
    private boolean _usesExternalZkClient = false; // false by default

    public StrictMatchExternalViewVerifier build() {
      if (_clusterName == null) {
        throw new IllegalArgumentException("Cluster name is missing!");
      }

      if (_zkClient != null) {
        return new StrictMatchExternalViewVerifier(_zkClient, _clusterName, _resources,
            _expectLiveInstances, _isDeactivatedNodeAware, _isLenientMatch,
            _waitPeriodTillVerify, _usesExternalZkClient);
      }

      if (_realmAwareZkConnectionConfig == null || _realmAwareZkClientConfig == null) {
        // For backward-compatibility
        return new StrictMatchExternalViewVerifier(_zkAddress, _clusterName, _resources,
            _expectLiveInstances, _isDeactivatedNodeAware, _isLenientMatch,
            _waitPeriodTillVerify);
      }

      validate();
      return new StrictMatchExternalViewVerifier(
          createZkClient(RealmAwareZkClient.RealmMode.SINGLE_REALM, _realmAwareZkConnectionConfig,
              _realmAwareZkClientConfig, _zkAddress), _clusterName, _resources,
          _expectLiveInstances, _isDeactivatedNodeAware, _isLenientMatch,
          _waitPeriodTillVerify, _usesExternalZkClient);
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

    public boolean getLenientMatch() {
      return _isLenientMatch;
    }

    public Builder setLenientMatch(boolean isLenientMatch) {
      _isLenientMatch = isLenientMatch;
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
      // Read the cluster once and compare what was read. The evaluation is shared with
      // ExternalViewConvergenceEvaluator so a verifier and a one-shot convergence read cannot
      // disagree about what a converged resource is.
      ResourceControllerDataProvider cache = new ResourceControllerDataProvider();
      cache.refresh(_accessor);
      return new ExternalViewConvergenceEvaluator.Builder().setLenientMatch(_isLenientMatch)
          .setDeactivatedNodeAware(_isDeactivatedNodeAware)
          // Polling verifiers only need the overall answer, and have always ignored a requested
          // resource that does not exist.
          .setStopAtFirstIssue(true).setFailOnUnknownResources(false).build()
          .evaluate(_accessor, cache, _resources, _expectLiveInstances).isConverged();
    } catch (Exception e) {
      LOG.error("exception in verification", e);
      return false;
    }
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
