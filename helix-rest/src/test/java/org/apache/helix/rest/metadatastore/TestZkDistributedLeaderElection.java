package org.apache.helix.rest.metadatastore;

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

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import org.apache.helix.TestHelper;
import org.apache.helix.ZkTestHelper;
import org.apache.helix.ZkUnitTestBase;
import org.apache.helix.rest.metadatastore.concurrency.ZkDistributedLeaderElection;
import org.apache.helix.zookeeper.api.client.HelixZkClient;
import org.apache.helix.zookeeper.datamodel.ZNRecord;
import org.apache.helix.zookeeper.datamodel.serializer.ZNRecordSerializer;
import org.apache.helix.zookeeper.impl.factory.DedicatedZkClientFactory;
import org.apache.helix.zookeeper.zkclient.ZkClient;
import org.apache.helix.zookeeper.zkclient.exception.ZkTimeoutException;
import org.apache.zookeeper.Watcher.Event.KeeperState;
import org.mockito.AdditionalAnswers;
import org.mockito.Mockito;
import org.testng.Assert;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

public class TestZkDistributedLeaderElection extends ZkUnitTestBase {
  private final List<HelixZkClient> _clients = new ArrayList<>();
  private final List<ZkDistributedLeaderElection> _elections = new ArrayList<>();
  private String _path;

  @BeforeMethod
  public void setUpElection() {
    _path = "/msds-election-" + UUID.randomUUID();
  }

  @AfterMethod(alwaysRun = true)
  public void tearDownElection() {
    _clients.forEach(HelixZkClient::close);
    _elections.forEach(ZkDistributedLeaderElection::close);
    _clients.clear();
    _elections.clear();
    _gZkClient.deleteRecursively(_path);
  }

  private HelixZkClient newClient() {
    HelixZkClient client = DedicatedZkClientFactory.getInstance().buildZkClient(
        new HelixZkClient.ZkConnectionConfig(ZK_ADDR).setSessionTimeout(4000),
        new HelixZkClient.ZkClientConfig().setZkSerializer(new ZNRecordSerializer())
            .setOperationRetryTimeout(5000L));
    _clients.add(client);
    return client;
  }

  private ZkDistributedLeaderElection join(String name) {
    HelixZkClient client = newClient();
    ZkDistributedLeaderElection election =
        new ZkDistributedLeaderElection(client, _path, new ZNRecord(name));
    _elections.add(election);
    return election;
  }

  private HelixZkClient withManualCallbacks(HelixZkClient realClient) {
    HelixZkClient client =
        Mockito.mock(HelixZkClient.class, AdditionalAnswers.delegatesTo(realClient));
    Mockito.doNothing().when(client).subscribeStateChanges(Mockito.any());
    Mockito.doReturn(Collections.emptyList()).when(client)
        .subscribeChildChanges(Mockito.anyString(), Mockito.any());
    return client;
  }

  @Test(timeOut = 15000)
  public void testInitialDisconnectRetriesMembershipOnReconnect() throws Exception {
    HelixZkClient realClient = newClient();
    HelixZkClient client = withManualCallbacks(realClient);
    Mockito.doReturn(false).when(client).waitUntilConnected(0, TimeUnit.MILLISECONDS);
    ZkDistributedLeaderElection election =
        new ZkDistributedLeaderElection(client, _path, new ZNRecord("first"));
    _elections.add(election);
    Assert.assertFalse(election.isLeader());
    Assert.assertFalse(_gZkClient.exists(_path));

    Mockito.doReturn(true).when(client).waitUntilConnected(0, TimeUnit.MILLISECONDS);
    election.handleStateChanged(KeeperState.SyncConnected);
    Assert.assertTrue(TestHelper.verify(election::isLeader, 5000));
    String candidate = _gZkClient.getChildren(_path).get(0);
    election.handleNewSession(Long.toHexString(realClient.getSessionId()));
    Assert.assertEquals(_gZkClient.getChildren(_path), Collections.singletonList(candidate));
  }

  @Test(timeOut = 30000)
  public void testInterruptedNewSessionRetriesMembershipOnReconnect() throws Exception {
    HelixZkClient realClient = newClient();
    HelixZkClient client = withManualCallbacks(realClient);
    ZkDistributedLeaderElection election =
        new ZkDistributedLeaderElection(client, _path, new ZNRecord("first"));
    _elections.add(election);
    Assert.assertTrue(election.isLeader());
    long oldSession = realClient.getSessionId();

    Mockito.doReturn(false).when(client).waitUntilConnected(0, TimeUnit.MILLISECONDS);
    election.handleStateChanged(KeeperState.Expired);
    ZkTestHelper.expireSession(realClient);
    Assert.assertTrue(TestHelper.verify(
        () -> realClient.waitUntilConnected(0, TimeUnit.MILLISECONDS)
            && realClient.getSessionId() != oldSession, 5000));
    String sessionId = Long.toHexString(realClient.getSessionId());
    election.handleNewSession(sessionId);
    Assert.assertFalse(election.isLeader());
    Assert.assertTrue(_gZkClient.getChildren(_path).isEmpty());

    Mockito.doReturn(true).when(client).waitUntilConnected(0, TimeUnit.MILLISECONDS);
    election.handleStateChanged(KeeperState.SyncConnected);
    Assert.assertTrue(TestHelper.verify(election::isLeader, 5000));
    String candidate = _gZkClient.getChildren(_path).get(0);
    election.handleNewSession(sessionId);
    Assert.assertEquals(_gZkClient.getChildren(_path), Collections.singletonList(candidate));
  }

  @Test(timeOut = 30000)
  public void testExpiredLeaderRejoinsAsFollower() throws Exception {
    ZkDistributedLeaderElection first = join("first");
    ZkDistributedLeaderElection second = join("second");
    Assert.assertTrue(TestHelper.verify(first::isLeader, 5000));
    ZkTestHelper.expireSession(_clients.get(0));
    Assert.assertTrue(TestHelper.verify(() -> second.isLeader() && !first.isLeader()
        && _gZkClient.getChildren(_path).size() == 2, 10000));
    Assert.assertTrue(TestHelper.verify(() -> first.getCurrentLeaderInfo() != null
        && "second".equals(first.getCurrentLeaderInfo().getId()), 5000));
  }

  @Test(timeOut = 15000)
  public void testReconnectDoesNotDuplicateCandidate() throws Exception {
    ZkDistributedLeaderElection election = join("first");
    String candidate = _gZkClient.getChildren(_path).get(0);
    ((ZkClient) _clients.get(0)).setCurrentState(KeeperState.Disconnected);
    election.handleStateChanged(KeeperState.Disconnected);
    Assert.assertFalse(election.isLeader());
    Assert.assertNull(election.getCurrentLeaderInfo());
    ((ZkClient) _clients.get(0)).setCurrentState(KeeperState.SyncConnected);
    election.handleStateChanged(KeeperState.SyncConnected);
    election.handleNewSession(Long.toHexString(_clients.get(0).getSessionId()));
    Assert.assertTrue(TestHelper.verify(election::isLeader, 5000));
    Assert.assertEquals(_gZkClient.getChildren(_path), java.util.Collections.singletonList(candidate));
  }

  @Test(timeOut = 15000)
  public void testLostCreateResponseAdoptsOneCandidate() throws Exception {
    HelixZkClient realClient = newClient();
    HelixZkClient client =
        Mockito.mock(HelixZkClient.class, AdditionalAnswers.delegatesTo(realClient));
    Mockito.doAnswer(invocation -> {
      String path = invocation.getArgument(0);
      Object data = invocation.getArgument(1);
      String sessionId = invocation.getArgument(2);
      realClient.createEphemeralSequential(path, data, sessionId);
      realClient.createEphemeralSequential(path, data, sessionId);
      throw new ZkTimeoutException("Injected loss of sequential-create responses");
    }).when(client).createEphemeralSequential(Mockito.anyString(), Mockito.any(), Mockito.anyString());
    ZkDistributedLeaderElection election =
        new ZkDistributedLeaderElection(client, _path, new ZNRecord("first"));
    _elections.add(election);
    Assert.assertTrue(TestHelper.verify(election::isLeader, 5000));
    Assert.assertEquals(_gZkClient.getChildren(_path).size(), 1);
    election.handleNewSession(Long.toHexString(realClient.getSessionId()));
    Assert.assertEquals(_gZkClient.getChildren(_path).size(), 1);
    Mockito.verify(client).createEphemeralSequential(Mockito.anyString(), Mockito.any(), Mockito.anyString());
  }

  @Test(timeOut = 20000)
  public void testThirdParticipantRefreshesLeader() throws Exception {
    join("first");
    ZkDistributedLeaderElection second = join("second");
    ZkDistributedLeaderElection third = join("third");
    _clients.get(0).close();
    Assert.assertTrue(TestHelper.verify(() -> second.isLeader()
        && third.getCurrentLeaderInfo() != null
        && "second".equals(third.getCurrentLeaderInfo().getId()), 10000));
    Assert.assertFalse(third.isLeader());
  }

  @Test(timeOut = 20000)
  public void testMissingCandidateRejoinsAndStaleSessionIsIgnored() throws Exception {
    ZkDistributedLeaderElection election = join("first");
    _gZkClient.delete(_path + "/" + _gZkClient.getChildren(_path).get(0));
    Assert.assertTrue(TestHelper.verify(() -> election.isLeader()
        && _gZkClient.getChildren(_path).size() == 1, 10000));
    String candidate = _gZkClient.getChildren(_path).get(0);
    election.handleNewSession("0");
    Assert.assertEquals(_gZkClient.getChildren(_path), java.util.Collections.singletonList(candidate));
    _clients.get(0).close();
    election.close();
    election.handleNewSession("1");
    election.handleStateChanged(KeeperState.SyncConnected);
    Assert.assertFalse(election.isLeader());
  }
}
