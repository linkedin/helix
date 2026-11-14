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
import org.apache.helix.manager.zk.ZKHelixDataAccessor;
import org.apache.helix.manager.zk.ZkBaseDataAccessor;
import org.apache.helix.model.ConvergenceStatus;
import org.apache.helix.rest.server.AbstractTestClass;
import org.testng.Assert;
import org.testng.annotations.Test;

public class TestConvergenceRecordCache extends AbstractTestClass {
  @Test
  public void testRecordIsReadAgainOnlyWhenItChanges() {
    String cluster = "ConvergenceRecordCacheCluster";
    _gSetupTool.addCluster(cluster, true);
    try {
      HelixDataAccessor accessor =
          new ZKHelixDataAccessor(cluster, new ZkBaseDataAccessor<>(_gZkClient));
      ConvergenceRecordCache cache = ConvergenceRecordCache.INSTANCE;
      Assert.assertNull(cache.get(_gZkClient, accessor, cluster));

      ConvergenceStatus record = new ConvergenceStatus(cluster);
      record.setStatus(ConvergenceStatus.Status.IN_PROGRESS);
      accessor.setProperty(accessor.keyBuilder().convergenceStatus(), record);
      ConvergenceStatus first = cache.get(_gZkClient, accessor, cluster);
      Assert.assertEquals(first.getStatus(), ConvergenceStatus.Status.IN_PROGRESS);
      Assert.assertSame(cache.get(_gZkClient, accessor, cluster), first);

      record.setStatus(ConvergenceStatus.Status.CONVERGED);
      accessor.setProperty(accessor.keyBuilder().convergenceStatus(), record);
      Assert.assertEquals(cache.get(_gZkClient, accessor, cluster).getStatus(),
          ConvergenceStatus.Status.CONVERGED);
    } finally {
      _gSetupTool.deleteCluster(cluster);
    }
  }
}
