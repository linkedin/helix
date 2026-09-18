package org.apache.helix.util;

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

import org.apache.helix.HelixDataAccessor;
import org.apache.helix.PropertyKey;
import org.apache.helix.common.ZkTestBase;
import org.apache.helix.controller.dataproviders.ResourceControllerDataProvider;
import org.apache.helix.manager.zk.ZKHelixDataAccessor;
import org.apache.helix.manager.zk.ZkBaseDataAccessor;
import org.apache.helix.model.ParticipantHistory;
import org.apache.zookeeper.data.Stat;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

/**
 * Covers the lifecycle guarantee of a convergence read: loading the cluster metadata it needs
 * must not write any of that metadata back.
 */
public class TestConvergenceReadOnlySnapshot extends ZkTestBase {
  private static final String CLUSTER = "TestConvergenceReadOnlySnapshot";
  private static final String OFFLINE_INSTANCE = "localhost_12918";

  private HelixDataAccessor _accessor;
  private PropertyKey _historyKey;

  @BeforeClass
  public void beforeClass() {
    _gSetupTool.addCluster(CLUSTER, true);
    _gSetupTool.addInstanceToCluster(CLUSTER, OFFLINE_INSTANCE);
    _accessor = new ZKHelixDataAccessor(CLUSTER, new ZkBaseDataAccessor<>(_gZkClient));
    _historyKey = _accessor.keyBuilder().participantHistory(OFFLINE_INSTANCE);
  }

  @AfterClass
  public void afterClass() {
    deleteCluster(CLUSTER);
  }

  @Test
  public void testReadOnlySnapshotLeavesParticipantHistoryUntouched() {
    givenHistoryWithoutRecordedOfflineTime();
    Stat before = readHistoryStat();

    ResourceControllerDataProvider cache =
        ExternalViewConvergenceEvaluator.readOnlySnapshot(CLUSTER, _accessor);

    Stat after = readHistoryStat();
    Assert.assertEquals(after.getVersion(), before.getVersion(),
        "Observing convergence must not write participant history");
    ParticipantHistory history = _accessor.getProperty(_historyKey);
    Assert.assertEquals(history.getLastOfflineTime(), ParticipantHistory.ONLINE);
    // An offline time the cluster has not recorded is reported as absent, not invented.
    Assert.assertFalse(cache.getInstanceOfflineTimeMap().containsKey(OFFLINE_INSTANCE));
  }

  @Test(dependsOnMethods = "testReadOnlySnapshotLeavesParticipantHistoryUntouched")
  public void testOrdinaryRefreshStillRecordsOfflineTime() {
    givenHistoryWithoutRecordedOfflineTime();
    Stat before = readHistoryStat();

    // An ordinary controller refresh still records the offline time, which is what makes the
    // read-only expectation in this class meaningful.
    ResourceControllerDataProvider cache = new ResourceControllerDataProvider(CLUSTER);
    cache.refresh(_accessor);

    Stat after = readHistoryStat();
    Assert.assertTrue(after.getVersion() > before.getVersion(),
        "The controller refresh is expected to record the offline time");
    ParticipantHistory history = _accessor.getProperty(_historyKey);
    Assert.assertTrue(history.getLastOfflineTime() > 0,
        "The controller refresh is expected to record when the instance went offline");
    Assert.assertTrue(cache.getInstanceOfflineTimeMap().containsKey(OFFLINE_INSTANCE));
  }

  private void givenHistoryWithoutRecordedOfflineTime() {
    ParticipantHistory history = new ParticipantHistory(OFFLINE_INSTANCE);
    Assert.assertEquals(history.getLastOfflineTime(), ParticipantHistory.ONLINE);
    Assert.assertTrue(_accessor.setProperty(_historyKey, history));
  }

  private Stat readHistoryStat() {
    Stat stat = _accessor.getBaseDataAccessor().getStat(_historyKey.getPath(),
        org.apache.helix.AccessOption.PERSISTENT);
    Assert.assertNotNull(stat, "participant history was expected to exist");
    return stat;
  }
}
