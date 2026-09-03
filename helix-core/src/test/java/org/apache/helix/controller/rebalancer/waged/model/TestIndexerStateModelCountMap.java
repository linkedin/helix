package org.apache.helix.controller.rebalancer.waged.model;

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

import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.Map;

import org.apache.helix.model.StateModelDefinition;
import org.apache.helix.zookeeper.datamodel.ZNRecord;
import org.testng.Assert;
import org.testng.annotations.Test;

/**
 * Reproduces the SEAS continuous-indexing state model ("IndexerStateModel") verbatim and asserts
 * exactly which states WAGED's planner treats as capacity-consuming ("counted").
 *
 * The planner generates one AssignableReplica per (partition, state) in the state model's
 * stateCountMap (see ClusterModelProvider.getAllAssignableReplicas). A replica in a state that
 * is NOT in stateCountMap is never generated and never charged against a node's capacity, while
 * the enforcer (WagedInstanceCapacity.processCurrentState) charges every physically-present
 * replica regardless of state. This test proves that INDEX_DOWNLOADED -- the state the incident's
 * replicas were wedged in -- is NOT a counted state for this model, while CAUGHT_UP is.
 */
public class TestIndexerStateModelCountMap {

  private static StateModelDefinition buildIndexerStateModel() {
    ZNRecord record = new ZNRecord("IndexerStateModel");
    record.setSimpleField("INITIAL_STATE", "OFFLINE");

    // <state>.meta count attributes, exactly as deployed for SEAS.
    record.setMapField("CAUGHT_UP.meta", asMap("count", "R"));
    record.setMapField("INDEX_DOWNLOADED.meta", asMap("count", "-1"));
    record.setMapField("ASSIGNED.meta", asMap("count", "-1"));
    record.setMapField("OFFLINE.meta", asMap("count", "-1"));
    record.setMapField("DROPPED.meta", asMap("count", "-1"));
    record.setMapField("ERROR.meta", asMap("count", "-1"));

    // <state>.next transition tables, exactly as deployed for SEAS.
    record.setMapField("CAUGHT_UP.next", mapOf("CAUGHT_UP", "CAUGHT_UP", "DROPPED", "OFFLINE",
        "INDEX_DOWNLOADED", "OFFLINE", "ASSIGNED", "OFFLINE", "OFFLINE", "OFFLINE"));
    record.setMapField("INDEX_DOWNLOADED.next", mapOf("CAUGHT_UP", "CAUGHT_UP", "DROPPED", "OFFLINE",
        "INDEX_DOWNLOADED", "INDEX_DOWNLOADED", "ASSIGNED", "OFFLINE", "OFFLINE", "OFFLINE"));
    record.setMapField("ASSIGNED.next", mapOf("CAUGHT_UP", "INDEX_DOWNLOADED", "DROPPED", "OFFLINE",
        "INDEX_DOWNLOADED", "INDEX_DOWNLOADED", "ASSIGNED", "ASSIGNED", "OFFLINE", "OFFLINE"));
    record.setMapField("OFFLINE.next", mapOf("DROPPED", "DROPPED", "CAUGHT_UP", "ASSIGNED",
        "INDEX_DOWNLOADED", "ASSIGNED", "ASSIGNED", "ASSIGNED", "OFFLINE", "OFFLINE"));
    record.setMapField("DROPPED.next", mapOf("DROPPED", "DROPPED"));
    record.setMapField("ERROR.next", mapOf("DROPPED", "DROPPED", "ERROR", "ERROR", "OFFLINE",
        "OFFLINE"));

    record.setListField("STATE_PRIORITY_LIST",
        Arrays.asList("CAUGHT_UP", "INDEX_DOWNLOADED", "ASSIGNED", "OFFLINE", "DROPPED", "ERROR"));
    record.setListField("STATE_TRANSITION_PRIORITYLIST",
        Arrays.asList("INDEX_DOWNLOADED-CAUGHT_UP", "CAUGHT_UP-OFFLINE", "ASSIGNED-INDEX_DOWNLOADED",
            "INDEX_DOWNLOADED-OFFLINE", "OFFLINE-ASSIGNED", "ASSIGNED-OFFLINE", "OFFLINE-DROPPED"));

    return new StateModelDefinition(record);
  }

  private static Map<String, String> asMap(String k, String v) {
    Map<String, String> m = new java.util.HashMap<>();
    m.put(k, v);
    return m;
  }

  private static Map<String, String> mapOf(String... kv) {
    Map<String, String> m = new java.util.HashMap<>();
    for (int i = 0; i < kv.length; i += 2) {
      m.put(kv[i], kv[i + 1]);
    }
    return m;
  }

  private static StateModelDefinition buildLeaderStandby() {
    ZNRecord record = new ZNRecord("LeaderStandby");
    record.setSimpleField("INITIAL_STATE", "OFFLINE");

    record.setMapField("LEADER.meta", asMap("count", "1"));
    record.setMapField("STANDBY.meta", asMap("count", "R"));
    record.setMapField("OFFLINE.meta", asMap("count", "-1"));
    record.setMapField("DROPPED.meta", asMap("count", "-1"));

    record.setMapField("LEADER.next",
        mapOf("DROPPED", "STANDBY", "STANDBY", "STANDBY", "OFFLINE", "STANDBY"));
    record.setMapField("STANDBY.next",
        mapOf("LEADER", "LEADER", "DROPPED", "OFFLINE", "OFFLINE", "OFFLINE"));
    record.setMapField("OFFLINE.next",
        mapOf("LEADER", "STANDBY", "DROPPED", "DROPPED", "STANDBY", "STANDBY"));
    record.setMapField("DROPPED.next", mapOf());

    record.setListField("STATE_PRIORITY_LIST",
        Arrays.asList("LEADER", "STANDBY", "OFFLINE", "DROPPED"));
    record.setListField("STATE_TRANSITION_PRIORITYLIST",
        Arrays.asList("LEADER-STANDBY", "STANDBY-LEADER", "OFFLINE-STANDBY", "STANDBY-OFFLINE",
            "OFFLINE-DROPPED"));

    return new StateModelDefinition(record);
  }

  @Test
  public void leaderStandbyBootstrapStateStandbyIsCounted() {
    StateModelDefinition def = buildLeaderStandby();

    Assert.assertEquals(def.getTopState(), "LEADER");

    LinkedHashMap<String, Integer> stateCountMap = def.getStateCountMap(5, 3);
    System.out.println(
        "LeaderStandby getStateCountMap(candidateNodeNum=5, totalReplicas=3) = " + stateCountMap);

    // BOTH LEADER and STANDBY are counted. Critically, STANDBY -- the follower/bootstrap working
    // state where an Espresso replica loads and holds a full copy -- IS counted, so the planner and
    // the enforcer agree on its capacity. There is no heavy, non-counted intermediate state.
    Assert.assertEquals(stateCountMap.get("LEADER"), Integer.valueOf(1));
    Assert.assertEquals(stateCountMap.get("STANDBY"), Integer.valueOf(2));
    Assert.assertEquals(stateCountMap.size(), 2);

    // Only the idle (OFFLINE) and terminal (DROPPED) states are non-counted.
    Assert.assertFalse(stateCountMap.containsKey("OFFLINE"));
    Assert.assertFalse(stateCountMap.containsKey("DROPPED"));
  }

  @Test
  public void indexDownloadedIsNotACountedStateWhileCaughtUpIs() {
    StateModelDefinition def = buildIndexerStateModel();

    // CAUGHT_UP is the single top state (first in the priority list).
    Assert.assertEquals(def.getTopState(), "CAUGHT_UP");

    // Resolve the stateCountMap for a partition with 2 replicas across (say) 3 candidate nodes.
    int candidateNodeNum = 3;
    int totalReplicas = 2;
    LinkedHashMap<String, Integer> stateCountMap =
        def.getStateCountMap(candidateNodeNum, totalReplicas);
    System.out.println("IndexerStateModel getStateCountMap(candidateNodeNum=" + candidateNodeNum
        + ", totalReplicas=" + totalReplicas + ") = " + stateCountMap);

    // Only CAUGHT_UP is counted; its "R" resolves to the replica count (2).
    Assert.assertEquals(stateCountMap.size(), 1, "Exactly one state should be counted");
    Assert.assertEquals(stateCountMap.get("CAUGHT_UP"), Integer.valueOf(2),
        "CAUGHT_UP (count=R) should resolve to the replica count");

    // Every transient/terminal state -- including INDEX_DOWNLOADED where the replicas were stuck --
    // has count -1 and is therefore invisible to the planner's capacity accounting.
    for (String uncounted : new String[] {"INDEX_DOWNLOADED", "ASSIGNED", "OFFLINE", "DROPPED",
        "ERROR"}) {
      Assert.assertFalse(stateCountMap.containsKey(uncounted),
          uncounted + " must NOT be a counted state (declared count=-1)");
    }
  }
}
