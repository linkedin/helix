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
import java.util.Collections;
import java.util.List;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.helix.HelixDataAccessor;
import org.apache.helix.HelixException;
import org.apache.helix.PropertyKey;
import org.apache.helix.model.ClusterConfig;
import org.apache.helix.model.InstanceConfig;
import org.apache.helix.rest.server.auditlog.AuditLog;
import org.apache.helix.zookeeper.api.client.RealmAwareZkClient;
import org.apache.helix.zookeeper.zkclient.exception.ZkException;
import org.testng.Assert;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

public class TestDelayedRebalanceStatusFailures {
  private static final String CLUSTER = "statusCluster";
  private HelixDataAccessor _accessor;
  private RealmAwareZkClient _zkClient;
  private InstancesAccessor _resource;
  private PropertyKey.Builder _keys;
  private ClusterConfig _clusterConfig;

  @BeforeMethod
  public void setUp() {
    _accessor = mock(HelixDataAccessor.class);
    _zkClient = mock(RealmAwareZkClient.class);
    _keys = new PropertyKey.Builder(CLUSTER);
    _clusterConfig = new ClusterConfig(CLUSTER);
    _clusterConfig.setDelayRebalaceEnabled(true);
    _clusterConfig.setRebalanceDelayTime(60_000L);
    when(_accessor.keyBuilder()).thenReturn(_keys);
    when(_zkClient.exists(anyString())).thenReturn(true);
    doReturn(Collections.singletonList(_clusterConfig)).when(_accessor)
        .getProperty(anyList(), eq(true));
    doReturn(Collections.emptyList()).when(_accessor)
        .getChildValues(any(PropertyKey.class), eq(true));
    when(_accessor.getChildNames(any(PropertyKey.class))).thenReturn(Collections.emptyList());
    _resource = new InstancesAccessor() {
      {
        // The servlet filter normally supplies this request-scoped audit context.
        _auditLogBuilder = new AuditLog.Builder();
      }

      @Override
      public HelixDataAccessor getDataAccssor(String clusterName) {
        return _accessor;
      }

      @Override
      public RealmAwareZkClient getRealmAwareZkClient() {
        return _zkClient;
      }
    };
  }

  @Test
  public void testValidEmptyCluster() throws Exception {
    try (Response response = status()) {
      Assert.assertEquals(response.getStatus(), 200);
      Assert.assertEquals(response.getHeaderString("Cache-Control"), "no-store");
      Assert.assertEquals(response.getMediaType(), MediaType.APPLICATION_JSON_TYPE);
      JsonNode result = new ObjectMapper().readTree((String) response.getEntity());
      Assert.assertEquals(result.get("id").textValue(), CLUSTER);
      Assert.assertTrue(result.get("delayedInstances").isObject());
      Assert.assertEquals(result.get("delayedInstances").size(), 0);
    }
    verify(_accessor).getProperty(anyList(), eq(true));
  }

  @Test
  public void testMissingClusterReturnsNotFound() {
    doReturn(Collections.singletonList(null)).when(_accessor)
        .getProperty(anyList(), eq(true));
    assertStatus(404);
    verifyNoInteractions(_zkClient);
  }

  @Test
  public void testFailedClusterReadIsNotAnEmptyPopulation() {
    doThrow(new HelixException("config read failed")).when(_accessor)
        .getProperty(anyList(), eq(true));
    assertStatus(500);
  }

  @Test
  public void testIncompleteClusterReadIsAnError() {
    doReturn(Collections.emptyList()).when(_accessor).getProperty(anyList(), eq(true));
    assertStatus(500);
  }

  @Test
  public void testExistenceReadFailureIsNotNotFound() {
    when(_zkClient.exists(anyString())).thenThrow(new ZkException("connection failed"));
    assertStatus(500);
  }

  @Test
  public void testMissingRequiredRootIsAnError() {
    when(_zkClient.exists(anyString())).thenReturn(false);
    assertStatus(500);
  }

  @Test
  public void testFailedInstanceReadIsAnError() {
    doThrow(new HelixException("instance read failed")).when(_accessor)
        .getChildValues(any(PropertyKey.class), eq(true));
    assertStatus(500);
  }

  @Test
  public void testMissingInstanceBatchIsAnError() {
    doReturn(null).when(_accessor).getChildValues(any(PropertyKey.class), eq(true));
    assertStatus(500);
  }

  @Test
  public void testPartialInstanceBatchIsAnError() {
    doReturn(Arrays.asList(new InstanceConfig("instance0"), null)).when(_accessor)
        .getChildValues(any(PropertyKey.class), eq(true));
    assertStatus(500);
  }

  @Test
  public void testFailedLiveReadIsAnError() {
    when(_accessor.getChildNames(any(PropertyKey.class)))
        .thenThrow(new ZkException("live read failed"));
    assertStatus(500);
  }

  @Test
  public void testMissingLiveReadIsAnError() {
    when(_accessor.getChildNames(any(PropertyKey.class))).thenReturn(null);
    assertStatus(500);
  }

  @Test
  public void testFailedHistoryReadIsAnError() {
    doReturn(Collections.singletonList(new InstanceConfig("offline"))).when(_accessor)
        .getChildValues(any(PropertyKey.class), eq(true));
    doAnswer(invocation -> {
      List<PropertyKey> keys = invocation.getArgument(0);
      if (keys.get(0).getPath().equals(_keys.clusterConfig().getPath())) {
        return Collections.singletonList(_clusterConfig);
      }
      throw new HelixException("history read failed");
    }).when(_accessor).getProperty(anyList(), eq(true));
    assertStatus(500);
  }

  @Test
  public void testPartialHistoryReadIsAnError() {
    doReturn(Collections.singletonList(new InstanceConfig("offline"))).when(_accessor)
        .getChildValues(any(PropertyKey.class), eq(true));
    doAnswer(invocation -> {
      List<PropertyKey> keys = invocation.getArgument(0);
      return keys.get(0).getPath().equals(_keys.clusterConfig().getPath())
          ? Collections.singletonList(_clusterConfig) : Collections.emptyList();
    }).when(_accessor).getProperty(anyList(), eq(true));
    assertStatus(500);
  }

  private Response status() {
    return _resource.getAllInstances(CLUSTER, "getDelayedRebalanceStatus");
  }

  private void assertStatus(int expected) {
    try (Response response = status()) {
      Assert.assertEquals(response.getStatus(), expected);
    }
  }
}
