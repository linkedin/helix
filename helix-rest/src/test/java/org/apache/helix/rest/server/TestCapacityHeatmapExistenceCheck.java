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

import javax.servlet.http.HttpServletRequest;
import javax.ws.rs.core.Response;

import org.apache.helix.rest.server.auditlog.AuditLog;
import org.apache.helix.rest.server.resources.helix.CapacityHeatmapAccessor;
import org.apache.helix.zookeeper.api.client.RealmAwareZkClient;
import org.apache.helix.zookeeper.zkclient.exception.ZkException;
import org.mockito.Mockito;
import org.testng.Assert;
import org.testng.annotations.Test;

/**
 * The heatmap tells a missing cluster (404) apart from a failed ZooKeeper read (500).
 */
public class TestCapacityHeatmapExistenceCheck {
  private static final String CLUSTER = "ExistenceCheckCluster";

  private Response heatmap(RealmAwareZkClient zkClient) {
    HttpServletRequest request = Mockito.mock(HttpServletRequest.class);
    Mockito.when(request.getAttribute(AuditLog.ATTRIBUTE_NAME))
        .thenReturn(Mockito.mock(AuditLog.Builder.class));
    CapacityHeatmapAccessor accessor = new CapacityHeatmapAccessor() {
      {
        _servletRequest = request;
      }

      @Override
      public RealmAwareZkClient getRealmAwareZkClient() {
        return zkClient;
      }
    };
    return accessor.getCapacityHeatmap(CLUSTER, 30_000L, null);
  }

  @Test
  public void testMissingClusterIs404AndFailedReadIs500() {
    RealmAwareZkClient missing = Mockito.mock(RealmAwareZkClient.class);
    Mockito.when(missing.exists("/" + CLUSTER)).thenReturn(false);
    Assert.assertEquals(heatmap(missing).getStatus(), Response.Status.NOT_FOUND.getStatusCode());

    RealmAwareZkClient failing = Mockito.mock(RealmAwareZkClient.class);
    Mockito.when(failing.exists("/" + CLUSTER)).thenThrow(new ZkException("connection loss"));
    Assert.assertEquals(heatmap(failing).getStatus(),
        Response.Status.INTERNAL_SERVER_ERROR.getStatusCode());
  }
}
