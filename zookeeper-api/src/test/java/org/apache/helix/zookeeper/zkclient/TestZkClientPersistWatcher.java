package org.apache.helix.zookeeper.zkclient;

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
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.helix.zookeeper.impl.TestHelper;
import org.apache.helix.zookeeper.impl.ZkTestBase;
import org.apache.helix.zookeeper.impl.ZkTestHelper;
import org.apache.helix.zookeeper.impl.client.ZkClient;
import org.apache.helix.zookeeper.zkclient.serialize.BasicZkSerializer;
import org.apache.helix.zookeeper.zkclient.serialize.SerializableSerializer;
import org.apache.zookeeper.AddWatchMode;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.Watcher.Event.EventType;
import org.apache.zookeeper.Watcher.Event.KeeperState;
import org.testng.Assert;
import org.testng.annotations.Test;


public class TestZkClientPersistWatcher extends ZkTestBase {

  @Test
  void testZkClientDataChange() throws Exception {
    org.apache.helix.zookeeper.impl.client.ZkClient.Builder builder =
        new org.apache.helix.zookeeper.impl.client.ZkClient.Builder();
    builder.setZkServer(ZkTestBase.ZK_ADDR).setMonitorRootPathOnly(false).setUsePersistWatcher(true);
    org.apache.helix.zookeeper.impl.client.ZkClient zkClient = builder.build();
    zkClient.setZkSerializer(new BasicZkSerializer(new SerializableSerializer()));
    int count = 1000;
    final int[] event_count = {0};
    CountDownLatch countDownLatch1 = new CountDownLatch(count);
    String path = "/dataChangeTestPath";
    IZkDataListener dataListener = new IZkDataListener() {
      @Override
      public void handleDataChange(String dataPath, Object data) throws Exception {
        countDownLatch1.countDown();
        event_count[0]++;
      }

      @Override
      public void handleDataDeleted(String dataPath) throws Exception {
      }
    };

    zkClient.subscribeDataChanges(path, dataListener);
    zkClient.create(path, "datat", CreateMode.PERSISTENT);
    for (int i = 0; i < count; ++i) {
      zkClient.writeData(path, ("datat" + i), -1);
    }

    Assert.assertTrue(countDownLatch1.await(15000, TimeUnit.MILLISECONDS));
    zkClient.close();
  }

  @Test(dependsOnMethods = "testZkClientDataChange")
  void testZkClientChildChange() throws Exception {
    org.apache.helix.zookeeper.impl.client.ZkClient.Builder builder =
        new org.apache.helix.zookeeper.impl.client.ZkClient.Builder();
    builder.setZkServer(ZkTestBase.ZK_ADDR).setMonitorRootPathOnly(false).setUsePersistWatcher(true);
    org.apache.helix.zookeeper.impl.client.ZkClient zkClient = builder.build();
    zkClient.setZkSerializer(new BasicZkSerializer(new SerializableSerializer()));
    int count = 100;
    final int[] event_count = {0};
    CountDownLatch countDownLatch1 = new CountDownLatch(count);
    CountDownLatch countDownLatch2 = new CountDownLatch(count);
    String path = "/testZkClientChildChange";
    IZkChildListener childListener = new IZkChildListener() {
      @Override
      public void handleChildChange(String parentPath, List<String> currentChilds) throws Exception {
        countDownLatch1.countDown();
        event_count[0]++;
      }
    };
    IZkChildListener childListener2 = new IZkChildListener() {
      @Override
      public void handleChildChange(String parentPath, List<String> currentChilds) throws Exception {
        countDownLatch2.countDown();
        event_count[0]++;
      }
    };
    zkClient.subscribeChildChanges(path, childListener);
    zkClient.subscribeChildChanges(path, childListener2);
    zkClient.create(path, "datat", CreateMode.PERSISTENT);
    for (int i = 0; i < count; ++i) {
      zkClient.create(path + "/child" + i, "datat", CreateMode.PERSISTENT);
    }
    Assert.assertTrue(countDownLatch1.await(15000, TimeUnit.MILLISECONDS));
    Assert.assertTrue(countDownLatch2.await(15000, TimeUnit.MILLISECONDS));
    zkClient.deleteRecursively(path);
    zkClient.close();
  }

  @Test(dependsOnMethods = "testZkClientChildChange")
  void testZkClientPersistRecursiveChange() throws Exception {
    org.apache.helix.zookeeper.impl.client.ZkClient.Builder builder =
        new org.apache.helix.zookeeper.impl.client.ZkClient.Builder();
    builder.setZkServer(ZkTestBase.ZK_ADDR).setMonitorRootPathOnly(false).setUsePersistWatcher(true);
    org.apache.helix.zookeeper.impl.client.ZkClient zkClient = builder.build();
    zkClient.setZkSerializer(new BasicZkSerializer(new SerializableSerializer()));
    int count = 100;
    final AtomicInteger[] event_count = {new AtomicInteger(0)};
    final AtomicInteger[] event_count2 = {new AtomicInteger(0)};
    // for each iteration, we will edit a node, create a child, create a grand child, and
    // delete child. Expect 4 event per iteration. -> total event should be count*4
    CountDownLatch countDownLatch1 = new CountDownLatch(count * 4);
    CountDownLatch countDownLatch2 = new CountDownLatch(count);
    String path = "/testZkClientPersistRecursiveChange";
    RecursivePersistListener rcListener = new RecursivePersistListener() {
      @Override
      public void handleZNodeChange(String dataPath, Watcher.Event.EventType eventType) throws Exception {
        countDownLatch1.countDown();
        event_count[0].incrementAndGet();
      }
    };
    zkClient.create(path, "datat", CreateMode.PERSISTENT);
    zkClient.subscribePersistRecursiveListener(path, rcListener);
    for (int i = 0; i < count; ++i) {
      zkClient.writeData(path, "data7" + i, -1);
      zkClient.create(path + "/c1_" + i, "datat", CreateMode.PERSISTENT);
      zkClient.create(path + "/c1_" + i + "/c2", "datat", CreateMode.PERSISTENT);
      zkClient.delete(path + "/c1_" + i + "/c2");
    }
    Assert.assertTrue(countDownLatch1.await(50000000, TimeUnit.MILLISECONDS));

    // subscribe a persist child watch, it should throw exception
    IZkChildListener childListener2 = new IZkChildListener() {
      @Override
      public void handleChildChange(String parentPath, List<String> currentChilds) throws Exception {
        countDownLatch2.countDown();
        event_count2[0].incrementAndGet();
      }
    };
    try {
      zkClient.subscribeChildChanges(path, childListener2, false);
    } catch (Exception ex) {
      Assert.assertEquals(ex.getClass().getName(), "java.lang.UnsupportedOperationException");
    }

    // unsubscribe recursive persist watcher, and subscribe persist watcher should success.
    zkClient.unsubscribePersistRecursiveListener(path, rcListener);
    zkClient.subscribeChildChanges(path, childListener2, false);
    // we should only get 100 event since only 100 direct child change.
    for (int i = 0; i < count; ++i) {
      zkClient.writeData(path, "data7" + i, -1);
      zkClient.create(path + "/c2_" + i, "datat", CreateMode.PERSISTENT);
      zkClient.create(path + "/c2_" + i + "/c3", "datat", CreateMode.PERSISTENT);
      zkClient.delete(path + "/c2_" + i + "/c3");
    }
    Assert.assertTrue(countDownLatch2.await(50000000, TimeUnit.MILLISECONDS));

    zkClient.deleteRecursively(path);
    zkClient.close();
  }

  /**
   * ZooKeeper does not deliver the changes made while the client was disconnected to persistent
   * watches (ZOOKEEPER-4698), so all persist listeners must be notified on a same-session
   * reconnect.
   */
  @Test
  void testPersistListenersResyncOnSameSessionReconnect() throws Exception {
    ZkClient zkClient = createPersistWatcherZkClient();
    String path = "/testPersistListenersResyncOnSameSessionReconnect";
    Set<String> events = subscribePersistListeners(zkClient, path);
    long sessionId = zkClient.getSessionId();

    simulateZkStateReconnected(zkClient);

    assertEvents(events, "data " + path + "/data None", "child " + path + "/child None",
        "recursive " + path + "/recursive None");
    Assert.assertEquals(zkClient.getSessionId(), sessionId);

    zkClient.deleteRecursively(path);
    zkClient.close();
  }

  /**
   * A new session does not inherit the persistent watches of the expired one, so they must be
   * re-added for the persist listeners to keep receiving events.
   */
  @Test
  void testPersistListenersSurviveSessionExpiry() throws Exception {
    ZkClient zkClient = createPersistWatcherZkClient();
    String path = "/testPersistListenersSurviveSessionExpiry";
    Set<String> events = subscribePersistListeners(zkClient, path);
    long sessionId = zkClient.getSessionId();

    ZkTestHelper.expireSession(zkClient);

    Assert.assertTrue(zkClient.getSessionId() != sessionId, "Session should have expired");
    assertEvents(events, "data " + path + "/data None", "child " + path + "/child None",
        "recursive " + path + "/recursive None");

    events.clear();
    zkClient.writeData(path + "/data", "newData");
    zkClient.create(path + "/child/c", "datat", CreateMode.PERSISTENT);
    zkClient.create(path + "/recursive/c", "datat", CreateMode.PERSISTENT);
    assertEvents(events, "data " + path + "/data NodeDataChanged",
        "child " + path + "/child NodeChildrenChanged",
        "recursive " + path + "/recursive/c NodeCreated");

    zkClient.deleteRecursively(path);
    zkClient.close();
  }

  /**
   * A persist watch that cannot be added back after a reconnect is added back on the next
   * reconnect. Its listeners are not resynced until then, as no watch tracks their path.
   */
  @Test
  void testPersistWatchAddedBackOnNextReconnectAfterFailure() throws Exception {
    FailingZkConnection connection = new FailingZkConnection(ZkTestBase.ZK_ADDR);
    ZkClient zkClient = createPersistWatcherZkClient(new ZkClient.Builder().setConnection(connection));
    String path = "/testPersistWatchAddedBackOnNextReconnectAfterFailure";
    Set<String> events = subscribePersistListeners(zkClient, path);

    // The new session has no watch, and adding the 3 watches back fails.
    connection.failAddWatch(KeeperException.Code.NOAUTH);
    ZkTestHelper.expireSession(zkClient);
    Assert.assertTrue(TestHelper.verify(() -> connection.getFailedAddWatchCount() == 3, 10000));
    zkClient.writeData(path + "/data", "missed");
    zkClient.create(path + "/child/missed", "datat", CreateMode.PERSISTENT);
    zkClient.create(path + "/recursive/missed", "datat", CreateMode.PERSISTENT);
    Thread.sleep(1000);
    Assert.assertTrue(events.isEmpty(), "No event is expected without watches: " + events);

    // The next reconnect adds the watches back and resyncs the listeners.
    connection.failAddWatch(null);
    simulateZkStateReconnected(zkClient);
    assertEvents(events, "data " + path + "/data None", "child " + path + "/child None",
        "recursive " + path + "/recursive None");

    events.clear();
    zkClient.writeData(path + "/data", "newData");
    zkClient.create(path + "/child/c", "datat", CreateMode.PERSISTENT);
    zkClient.create(path + "/recursive/c", "datat", CreateMode.PERSISTENT);
    assertEvents(events, "data " + path + "/data NodeDataChanged",
        "child " + path + "/child NodeChildrenChanged",
        "recursive " + path + "/recursive/c NodeCreated");

    zkClient.deleteRecursively(path);
    zkClient.close();
  }

  /**
   * A reconnect resyncs only the listeners registered at that time. A listener that subscribes
   * later adds its own watch and reads the current state itself, so it is not notified.
   */
  @Test
  void testPersistListenerSubscribedAfterReconnectIsNotResynced() throws Exception {
    ZkClient zkClient = createPersistWatcherZkClient();
    String path = "/testPersistListenerSubscribedAfterReconnectIsNotResynced";
    Set<String> events = subscribePersistListeners(zkClient, path + "/before");

    // Subscribe while the resync is queued behind a blocked state listener.
    CountDownLatch release = blockEventThreadOnSyncConnected(zkClient);
    simulateZkStateReconnected(zkClient);
    Set<String> lateEvents = subscribePersistListeners(zkClient, path + "/after");
    release.countDown();

    assertEvents(events, "data " + path + "/before/data None",
        "child " + path + "/before/child None", "recursive " + path + "/before/recursive None");
    Thread.sleep(1000);
    Assert.assertTrue(lateEvents.isEmpty(), "No resync is expected: " + lateEvents);

    zkClient.deleteRecursively(path);
    zkClient.close();
  }

  /**
   * A resync that loses the connection while adding the watches back stops instead of retrying,
   * so the event thread delivers the state changes that follow. The next reconnect resyncs the
   * listeners.
   */
  @Test
  void testPersistResyncStopsOnConnectionLoss() throws Exception {
    FailingZkConnection connection = new FailingZkConnection(ZkTestBase.ZK_ADDR);
    ZkClient zkClient = createPersistWatcherZkClient(new ZkClient.Builder().setConnection(connection));
    String path = "/testPersistResyncStopsOnConnectionLoss";
    Set<String> events = subscribePersistListeners(zkClient, path);
    AtomicInteger disconnects = countDisconnects(zkClient);

    connection.failAddWatch(KeeperException.Code.CONNECTIONLOSS);
    simulateZkStateReconnected(zkClient);
    Assert.assertTrue(TestHelper.verify(() -> connection.getFailedAddWatchCount() == 1, 10000));
    simulateZkState(zkClient, KeeperState.Disconnected);
    Assert.assertTrue(TestHelper.verify(() -> disconnects.get() == 2, 10000),
        "The event thread should not be blocked by the resync");
    Assert.assertEquals(connection.getFailedAddWatchCount(), 1, "The resync should not retry");
    Assert.assertTrue(events.isEmpty(), "No event is expected: " + events);

    connection.failAddWatch(null);
    simulateZkState(zkClient, KeeperState.SyncConnected);
    assertEvents(events, "data " + path + "/data None", "child " + path + "/child None",
        "recursive " + path + "/recursive None");

    zkClient.deleteRecursively(path);
    zkClient.close();
  }

  /**
   * A listener event whose read loses the connection stops instead of retrying, and so do the
   * other events queued with it, so the event thread delivers the state changes that follow. The
   * next reconnect resyncs the listeners.
   */
  @Test
  void testPersistListenerEventsStopOnConnectionLoss() throws Exception {
    FailingZkConnection connection = new FailingZkConnection(ZkTestBase.ZK_ADDR);
    ZkClient zkClient = createPersistWatcherZkClient(new ZkClient.Builder().setConnection(connection));
    String path = "/testPersistListenerEventsStopOnConnectionLoss";
    Set<String> events = subscribePersistListeners(zkClient, path);
    AtomicInteger disconnects = countDisconnects(zkClient);

    connection.failGetChildren(true);
    simulateZkStateReconnected(zkClient);
    Assert.assertTrue(TestHelper.verify(() -> connection.getFailedGetChildrenCount() == 1, 10000));
    simulateZkState(zkClient, KeeperState.Disconnected);
    Assert.assertTrue(TestHelper.verify(() -> disconnects.get() == 2, 10000),
        "The event thread should not be blocked by the listener event");
    Assert.assertEquals(connection.getFailedGetChildrenCount(), 1, "The read should not retry");
    Assert.assertTrue(events.isEmpty(), "No event is expected: " + events);

    connection.failGetChildren(false);
    simulateZkState(zkClient, KeeperState.SyncConnected);
    assertEvents(events, "data " + path + "/data None", "child " + path + "/child None",
        "recursive " + path + "/recursive None");

    zkClient.deleteRecursively(path);
    zkClient.close();
  }

  /**
   * When the connection flaps, only the resync of the latest reconnect runs, so each listener is
   * notified once.
   */
  @Test
  void testOnlyLatestPersistResyncRunsWhenConnectionFlaps() throws Exception {
    ZkClient zkClient = createPersistWatcherZkClient();
    String path = "/testOnlyLatestPersistResyncRunsWhenConnectionFlaps";
    List<String> events = new CopyOnWriteArrayList<>();
    subscribePersistListeners(zkClient, path, events);

    // Queue the reconnects behind a blocked state listener.
    CountDownLatch release = blockEventThreadOnSyncConnected(zkClient);
    for (int i = 0; i < 3; i++) {
      simulateZkStateReconnected(zkClient);
    }
    release.countDown();

    Assert.assertTrue(TestHelper.verify(() -> events.size() >= 3, 10000), "Events: " + events);
    Thread.sleep(1000);
    Assert.assertEquals(new HashSet<>(events), new HashSet<>(Arrays.asList(
        "data " + path + "/data None", "child " + path + "/child None",
        "recursive " + path + "/recursive None")));
    Assert.assertEquals(events.size(), 3, "Each listener should be resynced once: " + events);

    zkClient.deleteRecursively(path);
    zkClient.close();
  }

  // Delivers the state change from another thread, as ZooKeeper does. ZkClient rejects ZK
  // operations from the thread that delivers them.
  private static void simulateZkState(ZkClient zkClient, KeeperState state)
      throws InterruptedException {
    Thread zkEventThread =
        new Thread(() -> zkClient.process(new WatchedEvent(EventType.None, state, null)));
    zkEventThread.start();
    zkEventThread.join();
  }

  private static void simulateZkStateReconnected(ZkClient zkClient) throws InterruptedException {
    Thread zkEventThread = new Thread(() -> ZkTestHelper.simulateZkStateReconnected(zkClient));
    zkEventThread.start();
    zkEventThread.join();
  }

  // Blocks the event thread on the next SyncConnected state until the returned latch is released.
  private static CountDownLatch blockEventThreadOnSyncConnected(ZkClient zkClient) {
    CountDownLatch release = new CountDownLatch(1);
    zkClient.subscribeStateChanges(new StateListener() {
      @Override
      public void handleStateChanged(KeeperState state) throws Exception {
        if (state == KeeperState.SyncConnected) {
          release.await(30, TimeUnit.SECONDS);
        }
      }
    });
    return release;
  }

  // Returns the number of Disconnected states delivered to the state listeners.
  private static AtomicInteger countDisconnects(ZkClient zkClient) {
    AtomicInteger disconnects = new AtomicInteger();
    zkClient.subscribeStateChanges(new StateListener() {
      @Override
      public void handleStateChanged(KeeperState state) {
        if (state == KeeperState.Disconnected) {
          disconnects.incrementAndGet();
        }
      }
    });
    return disconnects;
  }

  private abstract static class StateListener implements IZkStateListener {
    @Override
    public void handleNewSession(String sessionId) {
    }

    @Override
    public void handleSessionEstablishmentError(Throwable error) {
    }
  }

  private static ZkClient createPersistWatcherZkClient() {
    return createPersistWatcherZkClient(new ZkClient.Builder().setZkServer(ZkTestBase.ZK_ADDR));
  }

  private static ZkClient createPersistWatcherZkClient(ZkClient.Builder builder) {
    builder.setMonitorRootPathOnly(false).setUsePersistWatcher(true);
    ZkClient zkClient = builder.build();
    zkClient.setZkSerializer(new BasicZkSerializer(new SerializableSerializer()));
    return zkClient;
  }

  // Subscribes a data, a child and a recursive persist listener under the path and returns the
  // set the listeners record their events into.
  private static Set<String> subscribePersistListeners(ZkClient zkClient, String path) {
    Set<String> events = ConcurrentHashMap.newKeySet();
    subscribePersistListeners(zkClient, path, events);
    return events;
  }

  private static void subscribePersistListeners(ZkClient zkClient, String path,
      Collection<String> events) {
    zkClient.createPersistent(path + "/data", true);
    zkClient.createPersistent(path + "/child", true);
    zkClient.createPersistent(path + "/recursive", true);
    zkClient.subscribeDataChanges(path + "/data", new IZkDataListener() {
      @Override
      public void handleDataChange(String dataPath, Object data) {
      }

      @Override
      public void handleDataChange(String dataPath, Object data,
          Watcher.Event.EventType eventType) {
        events.add("data " + dataPath + " " + eventType);
      }

      @Override
      public void handleDataDeleted(String dataPath) {
        events.add("data " + dataPath + " deleted");
      }
    });
    zkClient.subscribeChildChanges(path + "/child", new IZkChildListener() {
      @Override
      public void handleChildChange(String parentPath, List<String> currentChilds) {
      }

      @Override
      public void handleChildChange(String parentPath, List<String> currentChilds,
          Watcher.Event.EventType eventType) {
        events.add("child " + parentPath + " " + eventType);
      }
    });
    zkClient.subscribePersistRecursiveListener(path + "/recursive",
        (dataPath, eventType) -> events.add("recursive " + dataPath + " " + eventType));
  }

  private static void assertEvents(Set<String> events, String... expected) throws Exception {
    Set<String> expectedEvents = new HashSet<>(Arrays.asList(expected));
    Assert.assertTrue(TestHelper.verify(() -> events.equals(expectedEvents), 10000),
        "Expected events " + expectedEvents + " but got " + events);
  }

  // A connection on which adding a watch and getting children can be made to fail.
  private static class FailingZkConnection extends ZkConnection {
    private final AtomicReference<KeeperException.Code> _addWatchFailure = new AtomicReference<>();
    private final AtomicInteger _failedAddWatchCount = new AtomicInteger();
    private final AtomicBoolean _failGetChildren = new AtomicBoolean();
    private final AtomicInteger _failedGetChildrenCount = new AtomicInteger();

    FailingZkConnection(String zkServers) {
      super(zkServers);
    }

    // Adding a watch fails with the code, or succeeds if it is null.
    void failAddWatch(KeeperException.Code code) {
      _addWatchFailure.set(code);
    }

    int getFailedAddWatchCount() {
      return _failedAddWatchCount.get();
    }

    // Getting children loses the connection while set.
    void failGetChildren(boolean fail) {
      _failGetChildren.set(fail);
    }

    int getFailedGetChildrenCount() {
      return _failedGetChildrenCount.get();
    }

    @Override
    public void addWatch(String basePath, Watcher watcher, AddWatchMode mode)
        throws KeeperException, InterruptedException {
      KeeperException.Code code = _addWatchFailure.get();
      if (code != null) {
        _failedAddWatchCount.incrementAndGet();
        throw KeeperException.create(code, basePath);
      }
      super.addWatch(basePath, watcher, mode);
    }

    @Override
    public List<String> getChildren(String path, boolean watch)
        throws KeeperException, InterruptedException {
      if (_failGetChildren.get()) {
        _failedGetChildrenCount.incrementAndGet();
        throw KeeperException.create(KeeperException.Code.CONNECTIONLOSS, path);
      }
      return super.getChildren(path, watch);
    }
  }

  @Test
  void testSubscribeOneTimeChangeWhenUsingPersistWatcher() {
    org.apache.helix.zookeeper.impl.client.ZkClient.Builder builder =
        new org.apache.helix.zookeeper.impl.client.ZkClient.Builder();
    builder.setZkServer(ZkTestBase.ZK_ADDR).setMonitorRootPathOnly(false).setUsePersistWatcher(true);
    ZkClient zkClient = builder.build();
    zkClient.setZkSerializer(new BasicZkSerializer(new SerializableSerializer()));

    String path = "/testSubscribeOneTimeChangeWhenUsingPersistWatcher";
    zkClient.create(path, "datat", CreateMode.PERSISTENT);
    try {
      zkClient.exists(path, true);
      Assert.fail("Should throw exception when subscribe one time listener");
    } catch (Exception e) {
      Assert.assertEquals(e.getClass().getName(), "java.lang.IllegalArgumentException");
    }

    try {
      zkClient.readData(path, null, true);
      Assert.fail("Should throw exception when subscribe one time listener");
    } catch (Exception e) {
      Assert.assertEquals(e.getClass().getName(), "java.lang.IllegalArgumentException");
    }

    try {
      zkClient.getChildren(path, true);
      Assert.fail("Should throw exception when subscribe one time listener");
    } catch (Exception e) {
      Assert.assertEquals(e.getClass().getName(), "java.lang.IllegalArgumentException");
    }
    zkClient.delete(path);
    zkClient.close();
  }

  @Test
  void testCrudOperationWithResubscribe() {
    org.apache.helix.zookeeper.impl.client.ZkClient.Builder builder =
        new org.apache.helix.zookeeper.impl.client.ZkClient.Builder();
    builder.setZkServer(ZkTestBase.ZK_ADDR).setMonitorRootPathOnly(false).setUsePersistWatcher(false);
    ZkClient zkClient = builder.build();
    zkClient.setZkSerializer(new BasicZkSerializer(new SerializableSerializer()));

    String path = "/testCrudOperationWithResubscribe";
    zkClient.create(path, "datat", CreateMode.PERSISTENT);
    zkClient.exists(path, true);
    zkClient.readData(path, null, true);
    zkClient.getChildren(path, true);
    zkClient.delete(path);
    zkClient.close();
  }
}