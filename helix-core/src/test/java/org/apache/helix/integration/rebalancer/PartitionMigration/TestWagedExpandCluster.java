package org.apache.helix.integration.rebalancer.PartitionMigration;

/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.apache.helix.model.ClusterConfig;
import org.apache.helix.model.IdealState;
import org.apache.helix.tools.ClusterVerifiers.StrictMatchExternalViewVerifier;
import org.apache.helix.tools.ClusterVerifiers.ZkHelixClusterVerifier;

public class TestWagedExpandCluster extends TestExpandCluster {
// TODO check the movements in between
  protected ZkHelixClusterVerifier getVerifier() {
    Set<String> dbNames = new HashSet<>();
    int i = 0;
    for (String stateModel : TestStateModels) {
      dbNames.add("Test-DB-" + i++);
    }
    return new StrictMatchExternalViewVerifier.Builder(CLUSTER_NAME).setResources(dbNames)
        .setDeactivatedNodeAwareness(true).setZkAddr(ZK_ADDR).build();
  }

  protected Map<String, IdealState> createTestDBs(long delayTime) {
    Map<String, IdealState> idealStateMap = new HashMap<>();
    int i = 0;
    for (String stateModel : TestStateModels) {
      String db = "Test-DB-" + i++;
      createResourceWithWagedRebalance(CLUSTER_NAME, db, stateModel, _PARTITIONS, _replica,
          _minActiveReplica);
      _testDBs.add(db);
    }
    for (String db : _testDBs) {
      IdealState is =
          _gSetupTool.getClusterManagementTool().getResourceIdealState(CLUSTER_NAME, db);
      idealStateMap.put(db, is);
    }
    ClusterConfig clusterConfig = _configAccessor.getClusterConfig(CLUSTER_NAME);
    clusterConfig.setDelayRebalaceEnabled(true);
    clusterConfig.setRebalanceDelayTime(delayTime);
    _configAccessor.setClusterConfig(CLUSTER_NAME, clusterConfig);

    return idealStateMap;
  }
}