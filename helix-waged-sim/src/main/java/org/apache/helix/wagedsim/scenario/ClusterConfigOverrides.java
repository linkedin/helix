package org.apache.helix.wagedsim.scenario;

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
import java.util.EnumMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.apache.helix.model.ClusterConfig;
import org.apache.helix.wagedsim.cluster.ClusterState;
import org.apache.helix.wagedsim.util.Durations;
import org.apache.helix.zookeeper.datamodel.ZNRecord;

/** Applies cluster config overrides written with friendly names or raw ZNRecord fields. */
public final class ClusterConfigOverrides {
  private ClusterConfigOverrides() {
  }

  @SuppressWarnings("unchecked")
  public static List<String> apply(ClusterState state, Map<String, Object> overrides) {
    List<String> applied = new ArrayList<>();
    if (overrides == null || overrides.isEmpty()) {
      return applied;
    }
    ClusterConfig config = state.getClusterConfig();
    ZNRecord record = config.getRecord();
    for (Map.Entry<String, Object> entry : overrides.entrySet()) {
      String key = entry.getKey();
      Object value = entry.getValue();
      switch (key) {
        case "preferredScoringKeys":
          config.setPreferredScoringKeys(value == null ? null : strings(value));
          break;
        case "rebalancePreference": {
          Map<ClusterConfig.GlobalRebalancePreferenceKey, Integer> preference =
              new EnumMap<>(ClusterConfig.GlobalRebalancePreferenceKey.class);
          preference.putAll(config.getGlobalRebalancePreference());
          ((Map<String, Object>) value).forEach((k, v) -> preference.put(
              ClusterConfig.GlobalRebalancePreferenceKey.valueOf(k), Integer.parseInt(v.toString())));
          config.setGlobalRebalancePreference(preference);
          break;
        }
        case "delayRebalanceEnabled":
          config.setDelayRebalaceEnabled(Boolean.parseBoolean(value.toString()));
          break;
        case "delayRebalanceTime":
          config.setRebalanceDelayTime(Durations.parseMillis(value));
          break;
        case "instanceCapacityKeys":
          config.setInstanceCapacityKeys(strings(value));
          break;
        case "defaultInstanceCapacity":
          try {
            config.setDefaultInstanceCapacityMap(ints((Map<String, Object>) value));
          } catch (IllegalArgumentException e) {
            throw new IllegalArgumentException("defaultInstanceCapacity: " + e.getMessage(), e);
          }
          break;
        case "defaultPartitionWeight":
          config.setDefaultPartitionWeightMap(ints((Map<String, Object>) value));
          break;
        case "maxOfflineInstancesAllowed":
          config.setMaxOfflineInstancesAllowed(Integer.parseInt(value.toString()));
          break;
        case "topologyAwareEnabled":
          config.setTopologyAwareEnabled(Boolean.parseBoolean(value.toString()));
          break;
        case "topology":
          config.setTopology(value.toString());
          break;
        case "faultZoneType":
          config.setFaultZoneType(value.toString());
          break;
        case "simpleFields":
          ((Map<String, Object>) value).forEach((k, v) -> {
            if (v == null) {
              record.getSimpleFields().remove(k);
            } else {
              record.setSimpleField(k, v.toString());
            }
          });
          break;
        case "listFields":
          ((Map<String, Object>) value).forEach((k, v) -> record.setListField(k, strings(v)));
          break;
        case "mapFields":
          ((Map<String, Object>) value).forEach((k, v) -> {
            Map<String, String> map = new LinkedHashMap<>();
            ((Map<String, Object>) v).forEach((mk, mv) -> map.put(mk, String.valueOf(mv)));
            record.setMapField(k, map);
          });
          break;
        default:
          // A raw ClusterConfig simple field, for example DELAY_REBALANCE_TIME.
          if (value == null) {
            record.getSimpleFields().remove(key);
          } else {
            record.setSimpleField(key, value.toString());
          }
      }
      applied.add(key + "=" + value);
    }
    state.setClusterConfig(config);
    return applied;
  }

  @SuppressWarnings("unchecked")
  public static List<String> strings(Object value) {
    List<String> result = new ArrayList<>();
    if (value instanceof List) {
      for (Object item : (List<Object>) value) {
        result.add(String.valueOf(item));
      }
    } else if (value != null) {
      for (String item : value.toString().split(",")) {
        if (!item.trim().isEmpty()) {
          result.add(item.trim());
        }
      }
    }
    return result;
  }

  static Map<String, Integer> ints(Map<String, Object> value) {
    Map<String, Integer> result = new LinkedHashMap<>();
    value.forEach((k, v) -> result.put(k, (int) Math.round(Double.parseDouble(v.toString()))));
    return result;
  }
}
