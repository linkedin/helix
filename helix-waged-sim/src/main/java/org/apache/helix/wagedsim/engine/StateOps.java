package org.apache.helix.wagedsim.engine;

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
import java.util.List;
import java.util.Map;

import com.fasterxml.jackson.core.type.TypeReference;
import org.apache.helix.constants.InstanceConstants;
import org.apache.helix.model.ClusterConfig;
import org.apache.helix.model.InstanceConfig;
import org.apache.helix.model.LiveInstance;
import org.apache.helix.model.ParticipantHistory;
import org.apache.helix.wagedsim.cluster.ClusterNormalizer;
import org.apache.helix.wagedsim.cluster.ClusterState;
import org.apache.helix.wagedsim.util.Json;
import org.apache.helix.zookeeper.datamodel.ZNRecord;

/** Changes to a cluster definition that events and engines share. */
public final class StateOps {
  private static final String HELIX_ENABLED_TIMESTAMP = "HELIX_ENABLED_TIMESTAMP";
  private static final String INSTANCE_OPERATIONS = "HELIX_INSTANCE_OPERATIONS";
  private static final String MAINTENANCE_UNTIL = "INSTANCE_OPERATION_MAINTENANCE_UNTIL_MS";
  private static final String LAST_OFFLINE_TIME = "LAST_OFFLINE_TIME";
  private static final String LAST_ON_DEMAND = "LAST_ON_DEMAND_REBALANCE_TIMESTAMP";
  public static final String MAINTENANCE_PATH = "CONTROLLER/MAINTENANCE";

  private StateOps() {
  }

  /** Takes an instance offline: its live instance and current states go away. */
  public static boolean kill(ClusterState state, String instance) {
    if (state.remove(ClusterState.liveInstancePath(instance)) == null) {
      return false;
    }
    state.removeTree(ClusterState.INSTANCES + "/" + instance + "/CURRENTSTATES");
    ParticipantHistory history = history(state, instance);
    history.reportOffline();
    state.putHistory(instance, history);
    return true;
  }

  /** Removes an instance from the cluster: its config, live instance and everything under INSTANCES. */
  public static boolean removeInstance(ClusterState state, String instance) {
    boolean existed = state.remove(ClusterState.instanceConfigPath(instance)) != null;
    state.remove(ClusterState.liveInstancePath(instance));
    state.removeTree(ClusterState.INSTANCES + "/" + instance);
    return existed;
  }

  /** Brings an instance online with a new session and no current states. */
  public static boolean revive(ClusterState state, String instance, int generation) {
    if (state.get(ClusterState.instanceConfigPath(instance)) == null
        || state.get(ClusterState.liveInstancePath(instance)) != null) {
      return false;
    }
    String session = ClusterNormalizer.syntheticSession(instance, generation);
    LiveInstance liveInstance = new LiveInstance(instance);
    liveInstance.setSessionId(session);
    liveInstance.setHelixVersion(ClusterNormalizer.toolHelixVersion());
    state.putLiveInstance(liveInstance);
    state.removeTree(ClusterState.INSTANCES + "/" + instance + "/CURRENTSTATES");
    ParticipantHistory history = history(state, instance);
    history.reportOnline(session, ClusterNormalizer.toolHelixVersion());
    state.putHistory(instance, history);
    return true;
  }

  private static ParticipantHistory history(ClusterState state, String instance) {
    ParticipantHistory history = state.getHistory(instance);
    return history == null ? new ParticipantHistory(instance) : history;
  }

  public static void setOperation(ClusterState state, String instance,
      InstanceConstants.InstanceOperation operation) {
    InstanceConfig config = state.getInstanceConfig(instance);
    if (config == null) {
      throw new IllegalArgumentException("No instance " + instance);
    }
    config.setInstanceOperation(operation);
    state.putInstanceConfig(config);
  }

  /**
   * Shifts every timestamp the delay window reads by {@code delta} millis. The dry run keeps
   * timestamps relative to the wall clock, so moving virtual time forward by d shifts them by -d.
   */
  public static void shiftTimestamps(ClusterState state, long delta) {
    if (delta == 0) {
      return;
    }
    for (String instance : state.getInstanceNames()) {
      ZNRecord record = state.get(ClusterState.instanceConfigPath(instance));
      shiftLong(record, HELIX_ENABLED_TIMESTAMP, delta);
      shiftLong(record, MAINTENANCE_UNTIL, delta);
      List<String> operations = record.getListField(INSTANCE_OPERATIONS);
      if (operations != null) {
        List<String> shifted = new ArrayList<>();
        for (String operation : operations) {
          shifted.add(shiftOperationTimestamp(operation, delta));
        }
        record.setListField(INSTANCE_OPERATIONS, shifted);
      }
      ZNRecord history = state.get(ClusterState.historyPath(instance));
      if (history != null) {
        shiftLong(history, LAST_OFFLINE_TIME, delta);
      }
    }
    ZNRecord clusterConfig = state.get(ClusterState.clusterConfigPath(state.getClusterName()));
    if (clusterConfig != null) {
      shiftLong(clusterConfig, LAST_ON_DEMAND, delta);
    }
  }

  private static void shiftLong(ZNRecord record, String field, long delta) {
    String value = record.getSimpleField(field);
    if (value == null) {
      return;
    }
    try {
      long time = Long.parseLong(value.trim());
      if (time > 0) {
        record.setSimpleField(field, String.valueOf(time + delta));
      }
    } catch (NumberFormatException ignored) {
      // Leave unparsable values alone.
    }
  }

  private static String shiftOperationTimestamp(String serialized, long delta) {
    try {
      Map<String, String> properties =
          Json.MAPPER.readValue(serialized, new TypeReference<Map<String, String>>() {
          });
      String timestamp = properties.get("TIMESTAMP");
      if (timestamp != null) {
        long time = Long.parseLong(timestamp);
        if (time > 0) {
          properties.put("TIMESTAMP", String.valueOf(time + delta));
        }
      }
      return Json.MAPPER.writeValueAsString(properties);
    } catch (Exception e) {
      return serialized;
    }
  }

  public static boolean isMaintenance(ClusterState state) {
    return state.get(MAINTENANCE_PATH) != null;
  }
}
