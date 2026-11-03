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

import java.util.Collections;
import javax.servlet.http.HttpServletRequest;
import javax.ws.rs.core.Response;

import org.apache.helix.HelixDataAccessor;
import org.apache.helix.PropertyKey;
import org.apache.helix.rest.server.auditlog.AuditLog;
import org.apache.helix.rest.server.resources.helix.InstancesAccessor;
import org.apache.helix.zookeeper.api.client.RealmAwareZkClient;
import org.apache.helix.zookeeper.zkclient.exception.ZkException;
import org.mockito.Mockito;
import org.testng.Assert;
import org.testng.annotations.Test;

/**
 * The summary commands tell a missing cluster (404) apart from a failed ZooKeeper read (500).
 */
public class TestInstancesAccessorExistenceCheck {
  private static final String CLUSTER = "ExistenceCheckCluster";

  private Response getSummaries(String command, RealmAwareZkClient zkClient) {
    HelixDataAccessor dataAccessor = Mockito.mock(HelixDataAccessor.class);
    PropertyKey.Builder keyBuilder = new PropertyKey.Builder(CLUSTER);
    Mockito.when(dataAccessor.keyBuilder()).thenReturn(keyBuilder);
    Mockito.when(dataAccessor.getChildNames(keyBuilder.instanceConfigs()))
        .thenReturn(Collections.singletonList("i0"));
    HttpServletRequest request = Mockito.mock(HttpServletRequest.class);
    Mockito.when(request.getAttribute(AuditLog.ATTRIBUTE_NAME))
        .thenReturn(Mockito.mock(AuditLog.Builder.class));

    InstancesAccessor accessor = new InstancesAccessor() {
      {
        _servletRequest = request;
      }

      @Override
      public RealmAwareZkClient getRealmAwareZkClient() {
        return zkClient;
      }

      @Override
      public HelixDataAccessor getDataAccssor(String clusterName) {
        return dataAccessor;
      }
    };
    return accessor.getAllInstances(CLUSTER, command);
  }

  @Test
  public void testMissingClusterIs404AndFailedReadIs500() {
    for (String command : new String[] {"getInstanceSummaries", "getInstanceErrorSummaries"}) {
      RealmAwareZkClient missing = Mockito.mock(RealmAwareZkClient.class);
      Mockito.when(missing.exists("/" + CLUSTER)).thenReturn(false);
      Assert.assertEquals(getSummaries(command, missing).getStatus(),
          Response.Status.NOT_FOUND.getStatusCode(), command);

      RealmAwareZkClient failing = Mockito.mock(RealmAwareZkClient.class);
      Mockito.when(failing.exists("/" + CLUSTER)).thenThrow(new ZkException("connection loss"));
      Assert.assertEquals(getSummaries(command, failing).getStatus(),
          Response.Status.INTERNAL_SERVER_ERROR.getStatusCode(), command);
    }
  }
}
