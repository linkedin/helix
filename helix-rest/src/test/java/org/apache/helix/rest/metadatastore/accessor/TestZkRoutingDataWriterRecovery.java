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
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.helix.TestHelper;
import org.apache.helix.ZkTestHelper;
import org.apache.helix.msdcommon.constant.MetadataStoreRoutingConstants;
import org.apache.helix.rest.metadatastore.concurrency.ZkDistributedLeaderElection;
import org.apache.helix.rest.server.AbstractTestClass;
import org.apache.helix.zookeeper.api.client.HelixZkClient;
import org.apache.helix.zookeeper.datamodel.ZNRecord;
import org.apache.zookeeper.Op;
import org.apache.zookeeper.OpResult;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.data.Stat;
import org.mockito.AdditionalAnswers;
import org.mockito.Mockito;
import org.testng.Assert;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;


/**
 * Session-recovery cases for {@link ZkRoutingDataWriter}. Each writer uses its own ZooKeeper root,
 * so it never joins the shared election used by the REST server tests.
 */
public class TestZkRoutingDataWriterRecovery extends AbstractTestClass {
  private static final String DUMMY_REALM = "REALM";
  private static final String DUMMY_SHARDING_KEY = "/DUMMY/SHARDING/KEY";
  private static final String HOSTNAME_KEY = MetadataStoreRoutingConstants.MSDS_SERVER_HOSTNAME_KEY;

  private String _originalHostname;
  private String _root;

  @BeforeMethod
  public void setUpWriterRoot() {
    _originalHostname = System.getProperty(HOSTNAME_KEY);
    System.setProperty(HOSTNAME_KEY, "localhost");
    _root = "/writer-recovery-" + UUID.randomUUID();
    _gZkClientTestNS.createPersistent(_root, true);
  }

  @AfterMethod(alwaysRun = true)
  public void tearDownWriterRoot() {
    _gZkClientTestNS.deleteRecursively(_root);
    if (_originalHostname == null) {
      System.clearProperty(HOSTNAME_KEY);
    } else {
      System.setProperty(HOSTNAME_KEY, _originalHostname);
    }
  }

  @Test(timeOut = 30000)
  public void testExpiredAdmissionCannotWriteOnNewSession() throws Exception {
    String realm = "EXPIRED_ADMISSION_" + UUID.randomUUID();
    ZkRoutingDataWriter writer = new ZkRoutingDataWriter(TEST_NAMESPACE, _zkAddrTestNS + _root) {
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
          _root + MetadataStoreRoutingConstants.ROUTING_DATA_PATH + "/" + realm));
    } finally {
      writer.close();
    }
  }

  @DataProvider(name = "realmCreationModes")
  public Object[][] realmCreationModes() {
    return new Object[][] {{false}, {true}};
  }

  @Test(dataProvider = "realmCreationModes", timeOut = 30000)
  public void testExpiryAfterRealmCreateLeavesUsableRecord(boolean replaceRoutingData)
      throws Exception {
    ZkRoutingDataWriter writer = null;
    try {
      writer = new ZkRoutingDataWriter(TEST_NAMESPACE, _zkAddrTestNS + _root);
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
    }
  }

  @Test(dataProvider = "realmCreationModes", timeOut = 15000)
  public void testRealmRetryInitializesAnExistingNullRecord(boolean concurrentInitializer)
      throws Exception {
    ZkRoutingDataWriter writer = null;
    try {
      writer = new ZkRoutingDataWriter(TEST_NAMESPACE, _zkAddrTestNS + _root);
      String path = _root + MetadataStoreRoutingConstants.ROUTING_DATA_PATH + "/" + DUMMY_REALM;
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
    }
  }
}
