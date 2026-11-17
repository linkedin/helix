package org.apache.helix.rest.server.resources.helix;

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

import org.apache.helix.HelixDataAccessor;
import org.apache.helix.PropertyKey;
import org.apache.helix.manager.zk.ZKHelixDataAccessor;
import org.apache.helix.manager.zk.ZkBaseDataAccessor;
import org.apache.helix.model.IdealState;
import org.apache.helix.rest.server.AbstractTestClass;
import org.apache.zookeeper.data.Stat;
import org.mockito.Mockito;
import org.testng.Assert;
import org.testng.annotations.Test;

public class TestConvergenceIdealStateCache extends AbstractTestClass {
  @Test
  public void testIdealStateIsReadAgainOnlyWhenItChanges() {
    String cluster = "ConvergenceIdealStateCacheCluster";
    _gSetupTool.addCluster(cluster, true);
    try {
      HelixDataAccessor accessor = Mockito.spy(
          new ZKHelixDataAccessor(cluster, new ZkBaseDataAccessor<>(_gZkClient)));
      IdealState idealState = new IdealState("db");
      idealState.setNumPartitions(4);
      idealState.setStateModelDefRef("MasterSlave");
      accessor.setProperty(accessor.keyBuilder().idealStates("db"), idealState);
      String path = accessor.keyBuilder().idealStates("db").getPath();
      ConvergenceIdealStateCache cache = ConvergenceIdealStateCache.INSTANCE;

      Stat stat = _gZkClient.getStat(path);
      Assert.assertEquals(cache.get(_gZkClient, accessor, "db", stat)._partitions, 4);
      Mockito.clearInvocations(accessor);
      Assert.assertEquals(cache.get(_gZkClient, accessor, "db", stat)._partitions, 4);
      Mockito.verify(accessor, Mockito.never()).getProperty(Mockito.any(PropertyKey.class));

      idealState.setNumPartitions(8);
      accessor.setProperty(accessor.keyBuilder().idealStates("db"), idealState);
      Assert.assertEquals(
          cache.get(_gZkClient, accessor, "db", _gZkClient.getStat(path))._partitions, 8);
    } finally {
      _gSetupTool.deleteCluster(cluster);
    }
  }
}
