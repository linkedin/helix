package org.apache.helix.rest.metadatastore.accessor;

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

import java.lang.reflect.Field;
import java.security.NoSuchAlgorithmException;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;
import javax.net.ssl.SSLContext;

import com.google.common.collect.ImmutableMap;
import org.apache.helix.TestHelper;
import org.apache.helix.ZkTestHelper;
import org.apache.helix.msdcommon.constant.MetadataStoreRoutingConstants;
import org.apache.helix.rest.common.HttpConstants;
import org.apache.helix.rest.metadatastore.concurrency.ZkDistributedLeaderElection;
import org.apache.helix.rest.server.AbstractTestClass;
import org.apache.helix.zookeeper.datamodel.ZNRecord;
import org.apache.helix.zookeeper.api.client.HelixZkClient;
import org.apache.http.client.methods.HttpUriRequest;
import org.apache.zookeeper.Op;
import org.apache.zookeeper.OpResult;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.data.Stat;
import org.mockito.AdditionalAnswers;
import org.mockito.Mockito;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;


public class TestZkRoutingDataWriter extends AbstractTestClass {
  private static final String DUMMY_REALM = "REALM";
  private static final String DUMMY_SHARDING_KEY = "/DUMMY/SHARDING/KEY";

  private MetadataStoreRoutingDataWriter _zkRoutingDataWriter;

  // MockWriter is used for testing request forwarding features in non-leader situations
  class MockWriter extends ZkRoutingDataWriter {
    HttpUriRequest calledRequest;

    MockWriter(String namespace, String zkAddress) {
      super(namespace, zkAddress);
    }

    MockWriter(String namespace, String zkAddress, SSLContext sslContext) {
      super(namespace, zkAddress, sslContext);
    }

    // This method does not call super() because the http call should not be actually made
    @Override
    protected boolean sendRequestToLeader(HttpUriRequest request, int expectedResponseCode) {
      calledRequest = request;
      return false;
    }
  }

  @BeforeClass
  public void beforeClass() throws Exception {
    System.setProperty(MetadataStoreRoutingConstants.MSDS_SERVER_HOSTNAME_KEY,
        getBaseUri().getHost());
    System.setProperty(MetadataStoreRoutingConstants.MSDS_SERVER_PORT_KEY,
        Integer.toString(getBaseUri().getPort()));
    _zkRoutingDataWriter = new ZkRoutingDataWriter(TEST_NAMESPACE, _zkAddrTestNS);
    clearRoutingDataPath();
  }

  @AfterClass
  public void afterClass() throws Exception {
    System.clearProperty(MetadataStoreRoutingConstants.MSDS_SERVER_HOSTNAME_KEY);
    System.clearProperty(MetadataStoreRoutingConstants.MSDS_SERVER_PORT_KEY);
    _zkRoutingDataWriter.close();
    clearRoutingDataPath();
  }

  @Test
  public void testAddMetadataStoreRealm() {
    _zkRoutingDataWriter.addMetadataStoreRealm(DUMMY_REALM);
    ZNRecord znRecord = _gZkClientTestNS
        .readData(MetadataStoreRoutingConstants.ROUTING_DATA_PATH + "/" + DUMMY_REALM);
    Assert.assertNotNull(znRecord);
  }

  @Test(timeOut = 30000)
  public void testExpiredAdmissionCannotWriteOnNewSession() throws Exception {
    String realm = "EXPIRED_ADMISSION_" + java.util.UUID.randomUUID();
    String address = _zkAddrTestNS + "/expired-admission-" + java.util.UUID.randomUUID();
    String root = address.substring(_zkAddrTestNS.length());
    _gZkClientTestNS.createPersistent(root, true);
    String originalHostname = System.getProperty(MetadataStoreRoutingConstants.MSDS_SERVER_HOSTNAME_KEY);
    System.setProperty(MetadataStoreRoutingConstants.MSDS_SERVER_HOSTNAME_KEY, "localhost");
    ZkRoutingDataWriter writer = new ZkRoutingDataWriter(TEST_NAMESPACE, address) {
      @Override
      protected boolean createZkRealm(String name) {
        try {
          Field field = ZkRoutingDataWriter.class.getDeclaredField("_zkClient");
          field.setAccessible(true);
          ZkTestHelper.expireSession((HelixZkClient) field.get(this));
        } catch (Exception e) {
          throw new AssertionError(e);
        }
        return super.createZkRealm(name);
      }
    };
    try {
      Assert.assertFalse(writer.addMetadataStoreRealm(realm));
      Assert.assertFalse(_gZkClientTestNS.exists(
          root + MetadataStoreRoutingConstants.ROUTING_DATA_PATH + "/" + realm));
    } finally {
      writer.close();
      _gZkClientTestNS.deleteRecursively(root);
      if (originalHostname == null) {
        System.clearProperty(MetadataStoreRoutingConstants.MSDS_SERVER_HOSTNAME_KEY);
      } else {
        System.setProperty(MetadataStoreRoutingConstants.MSDS_SERVER_HOSTNAME_KEY, originalHostname);
      }
    }
  }

  @DataProvider(name = "realmCreationModes")
  public Object[][] realmCreationModes() {
    return new Object[][] {{false}, {true}};
  }

  @Test(dataProvider = "realmCreationModes", timeOut = 30000)
  public void testExpiryAfterRealmCreateLeavesUsableRecord(boolean replaceRoutingData)
      throws Exception {
    String root = "/atomic-realm-" + java.util.UUID.randomUUID();
    _gZkClientTestNS.createPersistent(root, true);
    String hostnameKey = MetadataStoreRoutingConstants.MSDS_SERVER_HOSTNAME_KEY;
    String originalHostname = System.getProperty(hostnameKey);
    System.setProperty(hostnameKey, "localhost");
    ZkRoutingDataWriter writer = null;
    try {
      writer = new ZkRoutingDataWriter(TEST_NAMESPACE, _zkAddrTestNS + root);
      Field clientField = ZkRoutingDataWriter.class.getDeclaredField("_zkClient");
      clientField.setAccessible(true);
      HelixZkClient realClient = (HelixZkClient) clientField.get(writer);
      Field electionField = ZkRoutingDataWriter.class.getDeclaredField("_leaderElection");
      electionField.setAccessible(true);
      ZkDistributedLeaderElection election =
          (ZkDistributedLeaderElection) electionField.get(writer);
      Assert.assertTrue(TestHelper.verify(election::isLeader, 5000));
      String path = MetadataStoreRoutingConstants.ROUTING_DATA_PATH + "/" + DUMMY_REALM;
      AtomicBoolean expired = new AtomicBoolean();
      HelixZkClient client =
          Mockito.mock(HelixZkClient.class, AdditionalAnswers.delegatesTo(realClient));
      Mockito.doAnswer(invocation -> {
        Iterable<Op> operations = invocation.getArgument(0);
        boolean createsRealm = false;
        for (Op operation : operations) {
          createsRealm |= operation.getType() == ZooDefs.OpCode.create
              && path.equals(operation.getPath());
        }
        List<OpResult> result = realClient.multi(operations);
        if (createsRealm && expired.compareAndSet(false, true)) {
          ZkTestHelper.expireSession(realClient);
          Assert.assertTrue(TestHelper.verify(election::isLeader, 10000));
        }
        return result;
      }).when(client).multi(Mockito.any());
      clientField.set(writer, client);

      Map<String, List<String>> routing =
          Collections.singletonMap(DUMMY_REALM, Collections.singletonList(DUMMY_SHARDING_KEY));
      if (replaceRoutingData) {
        writer.setRoutingData(routing);
      } else {
        writer.addMetadataStoreRealm(DUMMY_REALM);
      }
      Assert.assertTrue(expired.get(), "The realm create must commit before session expiry");
      Assert.assertNotNull(realClient.readData(path, true),
          "A committed realm create must include its initial record");
      Assert.assertTrue(replaceRoutingData
          ? writer.setRoutingData(routing) : writer.addMetadataStoreRealm(DUMMY_REALM));
      String nextKey = "/AFTER_RETRY";
      Assert.assertTrue(writer.addShardingKey(DUMMY_REALM, nextKey));
      Assert.assertTrue(writer.addMetadataStoreRealm(DUMMY_REALM));
      ZNRecord record = realClient.readData(path);
      Assert.assertTrue(record.getListField(MetadataStoreRoutingConstants.ZNRECORD_LIST_FIELD_KEY)
          .contains(nextKey));
      if (replaceRoutingData) {
        Assert.assertTrue(record.getListField(MetadataStoreRoutingConstants.ZNRECORD_LIST_FIELD_KEY)
            .contains(DUMMY_SHARDING_KEY));
      }
    } finally {
      if (writer != null) {
        writer.close();
      }
      _gZkClientTestNS.deleteRecursively(root);
      if (originalHostname == null) {
        System.clearProperty(hostnameKey);
      } else {
        System.setProperty(hostnameKey, originalHostname);
      }
    }
  }

  @Test(dataProvider = "realmCreationModes", timeOut = 15000)
  public void testRealmRetryInitializesAnExistingNullRecord(boolean concurrentInitializer)
      throws Exception {
    String root = "/uninitialized-realm-" + java.util.UUID.randomUUID();
    _gZkClientTestNS.createPersistent(root, true);
    ZkRoutingDataWriter writer = null;
    try {
      writer = new ZkRoutingDataWriter(TEST_NAMESPACE, _zkAddrTestNS + root);
      String path = root + MetadataStoreRoutingConstants.ROUTING_DATA_PATH + "/" + DUMMY_REALM;
      _gZkClientTestNS.createPersistent(path);
      if (concurrentInitializer) {
        Field clientField = ZkRoutingDataWriter.class.getDeclaredField("_zkClient");
        clientField.setAccessible(true);
        HelixZkClient realClient = (HelixZkClient) clientField.get(writer);
        HelixZkClient client =
            Mockito.mock(HelixZkClient.class, AdditionalAnswers.delegatesTo(realClient));
        String realmPath = MetadataStoreRoutingConstants.ROUTING_DATA_PATH + "/" + DUMMY_REALM;
        Mockito.doAnswer(invocation -> {
          ZNRecord record = realClient.readData(realmPath, (Stat) invocation.getArgument(1));
          if (record == null) {
            ZNRecord initialized = new ZNRecord(DUMMY_REALM);
            initialized.setListField(MetadataStoreRoutingConstants.ZNRECORD_LIST_FIELD_KEY,
                Collections.singletonList("/CONCURRENT"));
            realClient.writeData(realmPath, initialized);
          }
          return record;
        }).when(client).readData(Mockito.eq(realmPath), Mockito.any(Stat.class));
        clientField.set(writer, client);
        Assert.assertFalse(writer.addMetadataStoreRealm(DUMMY_REALM),
            "A concurrent initializer must not be overwritten");
      }
      Assert.assertTrue(writer.addMetadataStoreRealm(DUMMY_REALM));
      ZNRecord record = _gZkClientTestNS.readData(path, true);
      Assert.assertNotNull(record);
      if (concurrentInitializer) {
        Assert.assertEquals(record.getListField(MetadataStoreRoutingConstants.ZNRECORD_LIST_FIELD_KEY),
            Collections.singletonList("/CONCURRENT"));
      }
      Assert.assertTrue(writer.addShardingKey(DUMMY_REALM, DUMMY_SHARDING_KEY));
    } finally {
      if (writer != null) {
        writer.close();
      }
      _gZkClientTestNS.deleteRecursively(root);
    }
  }

  @Test(dependsOnMethods = "testAddMetadataStoreRealm")
  public void testDeleteMetadataStoreRealm() {
    _zkRoutingDataWriter.deleteMetadataStoreRealm(DUMMY_REALM);
    Assert.assertFalse(_gZkClientTestNS
        .exists(MetadataStoreRoutingConstants.ROUTING_DATA_PATH + "/" + DUMMY_REALM));
  }

  @Test(dependsOnMethods = "testDeleteMetadataStoreRealm")
  public void testAddShardingKey() {
    _zkRoutingDataWriter.addShardingKey(DUMMY_REALM, DUMMY_SHARDING_KEY);
    ZNRecord znRecord = _gZkClientTestNS
        .readData(MetadataStoreRoutingConstants.ROUTING_DATA_PATH + "/" + DUMMY_REALM);
    Assert.assertNotNull(znRecord);
    Assert.assertTrue(znRecord.getListField(MetadataStoreRoutingConstants.ZNRECORD_LIST_FIELD_KEY)
        .contains(DUMMY_SHARDING_KEY));
  }

  @Test(dependsOnMethods = "testAddShardingKey")
  public void testDeleteShardingKey() {
    _zkRoutingDataWriter.deleteShardingKey(DUMMY_REALM, DUMMY_SHARDING_KEY);
    ZNRecord znRecord = _gZkClientTestNS
        .readData(MetadataStoreRoutingConstants.ROUTING_DATA_PATH + "/" + DUMMY_REALM);
    Assert.assertNotNull(znRecord);
    Assert.assertFalse(znRecord.getListField(MetadataStoreRoutingConstants.ZNRECORD_LIST_FIELD_KEY)
        .contains(DUMMY_SHARDING_KEY));
  }

  @Test(dependsOnMethods = "testDeleteShardingKey")
  public void testSetRoutingData() {
    Map<String, List<String>> testRoutingDataMap =
        ImmutableMap.of(DUMMY_REALM, Collections.singletonList(DUMMY_SHARDING_KEY));
    _zkRoutingDataWriter.setRoutingData(testRoutingDataMap);
    ZNRecord znRecord = _gZkClientTestNS
        .readData(MetadataStoreRoutingConstants.ROUTING_DATA_PATH + "/" + DUMMY_REALM);
    Assert.assertNotNull(znRecord);
    Assert.assertEquals(
        znRecord.getListField(MetadataStoreRoutingConstants.ZNRECORD_LIST_FIELD_KEY).size(), 1);
    Assert.assertTrue(znRecord.getListField(MetadataStoreRoutingConstants.ZNRECORD_LIST_FIELD_KEY)
        .contains(DUMMY_SHARDING_KEY));
  }

  @Test(dependsOnMethods = "testSetRoutingData")
  public void testAddMetadataStoreRealmNonLeader() {
    MockWriter mockWriter = new MockWriter(TEST_NAMESPACE, _zkAddrTestNS);
    mockWriter.addMetadataStoreRealm(DUMMY_REALM);
    Assert.assertEquals(mockWriter.calledRequest.getMethod(), HttpConstants.RestVerbs.PUT.name());
    List<String> expectedUrlParams = Arrays
        .asList(MetadataStoreRoutingConstants.MSDS_NAMESPACES_URL_PREFIX, TEST_NAMESPACE,
            MetadataStoreRoutingConstants.MSDS_GET_ALL_REALMS_ENDPOINT, DUMMY_REALM);
    String expectedUrl =
        getBaseUri().toString() + String.join("/", expectedUrlParams).replaceAll("//", "/")
            .substring(1);
    Assert.assertEquals(mockWriter.calledRequest.getURI().toString(), expectedUrl);
    mockWriter.close();
  }

  @Test(dependsOnMethods = "testAddMetadataStoreRealmNonLeader")
  public void testDeleteMetadataStoreRealmNonLeader() {
    MockWriter mockWriter = new MockWriter(TEST_NAMESPACE, _zkAddrTestNS);
    mockWriter.deleteMetadataStoreRealm(DUMMY_REALM);
    Assert
        .assertEquals(mockWriter.calledRequest.getMethod(), HttpConstants.RestVerbs.DELETE.name());
    List<String> expectedUrlParams = Arrays
        .asList(MetadataStoreRoutingConstants.MSDS_NAMESPACES_URL_PREFIX, TEST_NAMESPACE,
            MetadataStoreRoutingConstants.MSDS_GET_ALL_REALMS_ENDPOINT, DUMMY_REALM);
    String expectedUrl =
        getBaseUri().toString() + String.join("/", expectedUrlParams).replaceAll("//", "/")
            .substring(1);
    Assert.assertEquals(mockWriter.calledRequest.getURI().toString(), expectedUrl);
    mockWriter.close();
  }

  @Test(dependsOnMethods = "testDeleteMetadataStoreRealmNonLeader")
  public void testAddShardingKeyNonLeader() {
    MockWriter mockWriter = new MockWriter(TEST_NAMESPACE, _zkAddrTestNS);
    mockWriter.addShardingKey(DUMMY_REALM, DUMMY_SHARDING_KEY);
    Assert.assertEquals(mockWriter.calledRequest.getMethod(), HttpConstants.RestVerbs.PUT.name());
    List<String> expectedUrlParams = Arrays
        .asList(MetadataStoreRoutingConstants.MSDS_NAMESPACES_URL_PREFIX, TEST_NAMESPACE,
            MetadataStoreRoutingConstants.MSDS_GET_ALL_REALMS_ENDPOINT, DUMMY_REALM,
            MetadataStoreRoutingConstants.MSDS_GET_ALL_SHARDING_KEYS_ENDPOINT, DUMMY_SHARDING_KEY);
    String expectedUrl =
        getBaseUri().toString() + String.join("/", expectedUrlParams).replaceAll("//", "/")
            .substring(1);
    Assert.assertEquals(mockWriter.calledRequest.getURI().toString(), expectedUrl);
    mockWriter.close();
  }

  @Test(dependsOnMethods = "testAddShardingKeyNonLeader")
  public void testDeleteShardingKeyNonLeader() {
    MockWriter mockWriter = new MockWriter(TEST_NAMESPACE, _zkAddrTestNS);
    mockWriter.deleteShardingKey(DUMMY_REALM, DUMMY_SHARDING_KEY);
    Assert
        .assertEquals(mockWriter.calledRequest.getMethod(), HttpConstants.RestVerbs.DELETE.name());
    List<String> expectedUrlParams = Arrays
        .asList(MetadataStoreRoutingConstants.MSDS_NAMESPACES_URL_PREFIX, TEST_NAMESPACE,
            MetadataStoreRoutingConstants.MSDS_GET_ALL_REALMS_ENDPOINT, DUMMY_REALM,
            MetadataStoreRoutingConstants.MSDS_GET_ALL_SHARDING_KEYS_ENDPOINT, DUMMY_SHARDING_KEY);
    String expectedUrl =
        getBaseUri().toString() + String.join("/", expectedUrlParams).replaceAll("//", "/")
            .substring(1);
    Assert.assertEquals(mockWriter.calledRequest.getURI().toString(), expectedUrl);
    mockWriter.close();
  }

  @Test(dependsOnMethods = "testDeleteShardingKeyNonLeader")
  public void testSetRoutingDataNonLeader() {
    MockWriter mockWriter = new MockWriter(TEST_NAMESPACE, _zkAddrTestNS);
    Map<String, List<String>> testRoutingDataMap =
        ImmutableMap.of(DUMMY_REALM, Collections.singletonList(DUMMY_SHARDING_KEY));
    mockWriter.setRoutingData(testRoutingDataMap);
    Assert.assertEquals(mockWriter.calledRequest.getMethod(), HttpConstants.RestVerbs.PUT.name());
    List<String> expectedUrlParams = Arrays
        .asList(MetadataStoreRoutingConstants.MSDS_NAMESPACES_URL_PREFIX, TEST_NAMESPACE,
            MetadataStoreRoutingConstants.MSDS_GET_ALL_ROUTING_DATA_ENDPOINT);
    String expectedUrl =
        getBaseUri().toString() + String.join("/", expectedUrlParams).replaceAll("//", "/")
            .substring(1);
    Assert.assertEquals(mockWriter.calledRequest.getURI().toString(), expectedUrl);
    mockWriter.close();
  }

  private void clearRoutingDataPath() throws Exception {
    Assert.assertTrue(TestHelper.verify(() -> {
      for (String zkRealm : _gZkClientTestNS
          .getChildren(MetadataStoreRoutingConstants.ROUTING_DATA_PATH)) {
        _gZkClientTestNS.delete(MetadataStoreRoutingConstants.ROUTING_DATA_PATH + "/" + zkRealm);
      }

      return _gZkClientTestNS.getChildren(MetadataStoreRoutingConstants.ROUTING_DATA_PATH)
          .isEmpty();
    }, TestHelper.WAIT_DURATION), "Routing data path should be deleted after the tests.");
  }

  @Test(dependsOnMethods = "testSetRoutingDataNonLeader")
  public void testBuildEndpointWithHttpProtocol() {
    // Test that HTTP protocol is used when no protocol is stored (backward compatibility)
    ZNRecord znRecord = new ZNRecord("test");
    znRecord.setSimpleField("hostname", "localhost");
    znRecord.setSimpleField("port", "8080");

    String endpoint = ZkRoutingDataWriter.buildEndpointFromLeaderElectionNode(znRecord);
    Assert.assertTrue(endpoint.startsWith(HttpConstants.HTTP_PROTOCOL_PREFIX),
        "Should default to HTTP protocol when protocol field is not set");
    Assert.assertEquals(endpoint, "http://localhost:8080");
  }

  @Test(dependsOnMethods = "testBuildEndpointWithHttpProtocol")
  public void testBuildEndpointWithHttpsProtocol() {
    // Test that HTTPS protocol is used when stored in ZNRecord
    ZNRecord znRecord = new ZNRecord("test");
    znRecord.setSimpleField("hostname", "localhost");
    znRecord.setSimpleField("port", "8443");
    znRecord.setSimpleField("protocol", HttpConstants.HTTPS_PROTOCOL_PREFIX);

    String endpoint = ZkRoutingDataWriter.buildEndpointFromLeaderElectionNode(znRecord);
    Assert.assertTrue(endpoint.startsWith(HttpConstants.HTTPS_PROTOCOL_PREFIX),
        "Should use HTTPS protocol when protocol field is set to https://");
    Assert.assertEquals(endpoint, "https://localhost:8443");
  }

  @Test(dependsOnMethods = "testBuildEndpointWithHttpsProtocol")
  public void testBuildEndpointWithContextUrlPrefix() {
    // Test that context URL prefix is appended correctly with HTTPS
    ZNRecord znRecord = new ZNRecord("test");
    znRecord.setSimpleField("hostname", "localhost");
    znRecord.setSimpleField("port", "8443");
    znRecord.setSimpleField("protocol", HttpConstants.HTTPS_PROTOCOL_PREFIX);
    znRecord.setSimpleField("contextUrlPrefix", "/admin/v2");

    String endpoint = ZkRoutingDataWriter.buildEndpointFromLeaderElectionNode(znRecord);
    Assert.assertEquals(endpoint, "https://localhost:8443/admin/v2");
  }

  @Test(dependsOnMethods = "testBuildEndpointWithContextUrlPrefix")
  public void testSslWriterRegistersWithHttpsProtocol() throws NoSuchAlgorithmException {
    // Test that when SSL is enabled, the server registers itself with HTTPS protocol
    // This means other servers will use HTTPS to reach this server when it becomes leader
    SSLContext sslContext = SSLContext.getDefault();

    // Create a writer with SSL - this will register with HTTPS protocol in ZK
    ZkRoutingDataWriter sslWriter = new ZkRoutingDataWriter(TEST_NAMESPACE, _zkAddrTestNS, sslContext);

    // The SSL writer is not the leader (the one from beforeClass is), but we can verify
    // that a ZNRecord with HTTPS protocol would build an HTTPS endpoint
    ZNRecord httpsRecord = new ZNRecord("ssl-server");
    httpsRecord.setSimpleField("hostname", "ssl-host.example.com");
    httpsRecord.setSimpleField("port", "8443");
    httpsRecord.setSimpleField("protocol", HttpConstants.HTTPS_PROTOCOL_PREFIX);

    String endpoint = ZkRoutingDataWriter.buildEndpointFromLeaderElectionNode(httpsRecord);
    Assert.assertTrue(endpoint.startsWith(HttpConstants.HTTPS_PROTOCOL_PREFIX),
        "Endpoint should use HTTPS when protocol field is set. Actual: " + endpoint);
    Assert.assertEquals(endpoint, "https://ssl-host.example.com:8443");

    sslWriter.close();
  }

  @Test(dependsOnMethods = "testSslWriterRegistersWithHttpsProtocol")
  public void testForwardingUsesLeaderProtocol() {
    // Test that forwarding uses the leader's protocol from ZK, not the forwarder's
    // The existing leader (_zkRoutingDataWriter from beforeClass) uses HTTP

    MockWriter mockWriter = new MockWriter(TEST_NAMESPACE, _zkAddrTestNS, null);

    mockWriter.addMetadataStoreRealm(DUMMY_REALM);
    Assert.assertEquals(mockWriter.calledRequest.getMethod(), HttpConstants.RestVerbs.PUT.name());

    // The URL should start with http:// because the leader registered with HTTP
    String requestUrl = mockWriter.calledRequest.getURI().toString();
    Assert.assertTrue(requestUrl.startsWith(HttpConstants.HTTP_PROTOCOL_PREFIX),
        "Request URL should use leader's protocol (HTTP). Actual URL: " + requestUrl);

    mockWriter.close();
  }

  @Test(dependsOnMethods = "testForwardingUsesLeaderProtocol")
  public void testBackwardCompatibilityNoProtocolField() {
    // Test backward compatibility: when protocol field is missing, default to HTTP
    ZNRecord legacyRecord = new ZNRecord("legacy-server");
    legacyRecord.setSimpleField("hostname", "legacy-host.example.com");
    legacyRecord.setSimpleField("port", "8080");
    // Note: no protocol field set (simulating old servers)

    String endpoint = ZkRoutingDataWriter.buildEndpointFromLeaderElectionNode(legacyRecord);
    Assert.assertTrue(endpoint.startsWith(HttpConstants.HTTP_PROTOCOL_PREFIX),
        "Endpoint should default to HTTP when protocol field is missing. Actual: " + endpoint);
    Assert.assertEquals(endpoint, "http://legacy-host.example.com:8080");
  }
}
