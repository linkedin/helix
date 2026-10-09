package org.apache.helix.wagedsim.cluster;

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

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.zip.CRC32;

import org.apache.helix.HelixManagerProperties;
import org.apache.helix.SystemPropertyKeys;
import org.apache.helix.controller.rebalancer.util.WagedRebalanceUtil;
import org.apache.helix.controller.rebalancer.util.WagedValidationUtil;
import org.apache.helix.model.ClusterConfig;
import org.apache.helix.model.CurrentState;
import org.apache.helix.model.ExternalView;
import org.apache.helix.model.IdealState;
import org.apache.helix.model.InstanceConfig;
import org.apache.helix.model.LiveInstance;
import org.apache.helix.model.ResourceConfig;
import org.apache.helix.zookeeper.datamodel.ZNRecord;

/** Normalizes a cluster definition from any source and checks that WAGED can run on it. */
public final class ClusterNormalizer {
  private static final String BUCKET_SIZE = "BUCKET_SIZE";

  private ClusterNormalizer() {
  }

  /** @return the Helix version this tool was built with */
  public static String toolHelixVersion() {
    try {
      return new HelixManagerProperties(SystemPropertyKeys.CLUSTER_MANAGER_VERSION).getVersion();
    } catch (RuntimeException e) {
      return "unknown";
    }
  }

  /** @return a stable synthetic session id for an instance */
  public static String syntheticSession(String instance, int generation) {
    CRC32 crc = new CRC32();
    crc.update((instance + "#" + generation).getBytes(StandardCharsets.UTF_8));
    return String.format("5e%014x", crc.getValue());
  }

  /**
   * Fills in what the engines need: session ids, current states (derived when the source has none),
   * unbucketized records, counts and fidelity flags.
   *
   * @param captured true for copies of a real cluster, false for generated clusters
   */
  public static void normalize(ClusterState state, boolean captured) {
    Manifest manifest = state.getManifest();
    manifest.toolHelixVersion = toolHelixVersion();
    Map<String, LiveInstance> live = state.getLiveInstances();
    live.forEach((instance, liveInstance) -> {
      if (liveInstance.getRecord().getSimpleField(LiveInstance.LiveInstanceProperty.SESSION_ID.name())
          == null) {
        liveInstance.setSessionId(syntheticSession(instance, 0));
        state.putLiveInstance(liveInstance);
      }
    });
    live = state.getLiveInstances();
    for (String path : new ArrayList<>(state.getZnodes().keySet())) {
      if (path.startsWith(ClusterState.EXTERNALVIEW + "/") || path.contains("/CURRENTSTATES/")) {
        state.get(path).getSimpleFields().remove(BUCKET_SIZE);
      }
    }
    if (state.getCurrentStates().isEmpty()) {
      Map<String, Map<String, Map<String, String>>> derived = new TreeMap<>();
      Map<String, ExternalView> views = state.getExternalViews();
      if (!views.isEmpty()) {
        views.forEach((resource, view) -> {
          Map<String, Map<String, String>> partitions = new TreeMap<>();
          for (String partition : view.getPartitionSet()) {
            partitions.put(partition, new TreeMap<>(view.getStateMap(partition)));
          }
          derived.put(resource, partitions);
        });
        if (captured) {
          manifest.addFidelity(Manifest.CURRENT_STATES_FROM_EXTERNAL_VIEW);
        }
      } else if (state.getBestPossible() != null) {
        derived.putAll(Layouts.copy(state.getBestPossible()));
        if (captured) {
          manifest.addFidelity(Manifest.CURRENT_STATES_FROM_BEST_POSSIBLE);
        }
      }
      if (!derived.isEmpty()) {
        state.replaceCurrentStates(derived);
      }
    } else {
      Map<String, Map<String, CurrentState>> currentStates = state.getCurrentStates();
      Map<String, LiveInstance> liveNow = live;
      currentStates.forEach((instance, resources) -> {
        LiveInstance liveInstance = liveNow.get(instance);
        if (liveInstance != null) {
          resources.values().forEach(cs -> {
            cs.setSessionId(liveInstance.getEphemeralOwner());
            state.putCurrentState(instance, cs);
          });
        }
      });
    }
    if (captured && state.getBaseline() == null && state.getBestPossible() == null) {
      manifest.addFidelity(Manifest.NO_ASSIGNMENT_METADATA);
    }
    if (captured && state.getHistories().isEmpty()) {
      manifest.addFidelity(Manifest.NO_PARTICIPANT_HISTORY);
    }
    updateCounts(state);
  }

  public static void updateCounts(ClusterState state) {
    Map<String, Long> counts = new TreeMap<>();
    Map<String, InstanceConfig> instances = state.getInstanceConfigs();
    Map<String, LiveInstance> live = state.getLiveInstances();
    counts.put("instances", (long) instances.size());
    counts.put("liveInstances", live.keySet().stream().filter(instances::containsKey).count());
    counts.put("enabledLiveInstances", instances.values().stream()
        .filter(InstanceConfig::getInstanceEnabled).filter(i -> live.containsKey(i.getInstanceName()))
        .count());
    Map<String, IdealState> waged = state.getWagedIdealStates();
    counts.put("wagedResources", (long) waged.size());
    counts.put("partitions", waged.values().stream().mapToLong(i -> i.getPartitionSet().size()).sum());
    Map<String, Map<String, Map<String, String>>> served = state.getServedLayout();
    served.keySet().retainAll(waged.keySet());
    counts.put("servedReplicas", Layouts.replicaCount(served));
    long others = state.getIdealStates().size() - waged.size();
    if (others > 0) {
      counts.put("otherResources", others);
    }
    state.getManifest().counts = new java.util.LinkedHashMap<>(counts);
  }

  /** @return problems that stop WAGED from running on this cluster; empty when it can run */
  public static List<String> validate(ClusterState state) {
    List<String> problems = new ArrayList<>();
    ClusterConfig clusterConfig = state.getClusterConfig();
    if (clusterConfig == null) {
      problems.add("No cluster config at " + ClusterState.clusterConfigPath(state.getClusterName()));
      return problems;
    }
    if (clusterConfig.getInstanceCapacityKeys() == null
        || clusterConfig.getInstanceCapacityKeys().isEmpty()) {
      problems.add("Cluster config has no INSTANCE_CAPACITY_KEYS; WAGED needs capacity keys");
    }
    Map<String, InstanceConfig> instances = state.getInstanceConfigs();
    if (instances.isEmpty()) {
      problems.add("No instance configs");
    }
    Map<String, IdealState> waged = state.getWagedIdealStates();
    if (waged.isEmpty()) {
      problems.add("No WAGED resources (FULL_AUTO ideal states with the WagedRebalancer class)");
    }
    if (!problems.isEmpty()) {
      return problems;
    }
    for (InstanceConfig instance : instances.values()) {
      if (!instance.isAssignable()) {
        continue;
      }
      try {
        WagedValidationUtil.validateAndGetInstanceCapacity(clusterConfig, instance);
      } catch (RuntimeException e) {
        problems.add("Instance " + instance.getInstanceName() + ": " + e.getMessage());
      }
    }
    Map<String, ResourceConfig> resourceConfigs = state.getResourceConfigs();
    Map<String, ?> models = state.getStateModelDefs();
    for (IdealState idealState : waged.values()) {
      String resource = idealState.getResourceName();
      if (!models.containsKey(idealState.getStateModelDefRef())) {
        problems.add("Resource " + resource + " uses unknown state model "
            + idealState.getStateModelDefRef());
      }
      if (idealState.getPartitionSet().isEmpty()) {
        problems.add("Resource " + resource + " has no partitions in its ideal state");
      }
      for (String partition : idealState.getPartitionSet()) {
        try {
          WagedRebalanceUtil.fetchCapacityUsage(partition, resourceConfigs.get(resource), clusterConfig);
        } catch (RuntimeException e) {
          problems.add("Resource " + resource + " partition " + partition + ": " + e.getMessage());
          break;
        }
      }
    }
    return problems;
  }

  /** Removes a znode record's stat-like fields that should not travel between clusters. */
  public static ZNRecord clean(ZNRecord record) {
    record.getSimpleFields().remove(BUCKET_SIZE);
    return record;
  }
}
