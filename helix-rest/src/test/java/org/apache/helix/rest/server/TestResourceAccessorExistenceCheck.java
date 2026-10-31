package org.apache.helix.rest.server;

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

import javax.ws.rs.core.Response;

import org.apache.helix.rest.server.resources.helix.ResourceAccessor;
import org.apache.helix.zookeeper.api.client.RealmAwareZkClient;
import org.apache.helix.zookeeper.zkclient.exception.ZkException;
import org.mockito.Mockito;
import org.testng.Assert;
import org.testng.annotations.Test;

/**
 * The resource summaries tell a missing cluster (404) apart from a failed ZooKeeper read, which
 * propagates and is served as a 500.
 */
public class TestResourceAccessorExistenceCheck {
  private static final String CLUSTER = "ExistenceCheckCluster";

  private ResourceAccessor accessorWith(RealmAwareZkClient zkClient) {
    return new ResourceAccessor() {
      @Override
      public RealmAwareZkClient getRealmAwareZkClient() {
        return zkClient;
      }
    };
  }

  @Test
  public void testMissingClusterIs404AndFailedReadPropagates() {
    RealmAwareZkClient missing = Mockito.mock(RealmAwareZkClient.class);
    Mockito.when(missing.exists("/" + CLUSTER)).thenReturn(false);
    Assert.assertEquals(accessorWith(missing).getResourceSummaries(CLUSTER).getStatus(),
        Response.Status.NOT_FOUND.getStatusCode());

    RealmAwareZkClient failing = Mockito.mock(RealmAwareZkClient.class);
    Mockito.when(failing.exists("/" + CLUSTER)).thenThrow(new ZkException("connection loss"));
    try {
      accessorWith(failing).getResourceSummaries(CLUSTER);
      Assert.fail("A failed existence read must not be reported as a missing cluster");
    } catch (ZkException expected) {
      // propagates to the server, which answers 500
    }
  }
}
