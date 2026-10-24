package org.apache.helix.metaclient.impl.zk;

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

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;

import org.apache.helix.metaclient.MetaClientTestUtil;
import org.apache.helix.metaclient.api.ConnectStateChangeListener;
import org.apache.helix.metaclient.api.MetaClientInterface.ConnectState;
import org.apache.helix.metaclient.impl.zk.factory.ZkMetaClientConfig;
import org.testng.Assert;
import org.testng.annotations.Test;


/**
 * ZooKeeper persistent watches miss the changes made while the client is disconnected
 * (ZOOKEEPER-4698) and do not survive a new session. These tests verify that MetaClient listeners
 * are resynced in both cases.
 */
public class TestZkMetaClientListenerResync extends ZkMetaClientTestBase {
  private static final long TIMEOUT_MS = 30000;

  @Test
  public void testListenersResyncAfterSameSessionReconnect() throws Exception {
    String root = "/testListenersResyncAfterSameSessionReconnect";
    try (ZkProxy proxy = new ZkProxy(ZK_ADDR);
        ZkMetaClient<String> writer = createZkMetaClient();
        ZkMetaClient<String> client = new ZkMetaClient<>(
            new ZkMetaClientConfig.ZkMetaClientConfigBuilder().setConnectionAddress(
                proxy.getAddress()).build())) {
      writer.connect();
      client.connect();
      createEntries(writer, root);
      writer.create(root + "/deleted", "v0");

      Set<String> states = ConcurrentHashMap.newKeySet();
      client.subscribeStateChanges(new ConnectStateChangeListener() {
        @Override
        public void handleConnectStateChanged(ConnectState prevState, ConnectState currentState) {
          states.add(currentState.name());
        }

        @Override
        public void handleConnectionEstablishmentError(Throwable error) {
        }
      });
      Set<String> events = subscribeListeners(client, root);
      client.subscribeDataChange(root + "/deleted",
          (key, data, changeType) -> events.add("DATA " + changeType + " " + key), false);
      long sessionId = client.getZkClient().getSessionId();

      proxy.cut();
      Assert.assertTrue(MetaClientTestUtil.verify(
          () -> states.contains(ConnectState.DISCONNECTED.name()), TIMEOUT_MS));
      writer.set(root + "/data", "v1", -1);
      writer.delete(root + "/deleted");
      writer.create(root + "/directChild/gap", "v0");
      writer.create(root + "/tree/gap", "v0");
      Assert.assertTrue(events.isEmpty(), "No event is expected while disconnected: " + events);

      proxy.open();
      assertEvents(events, "DATA ENTRY_UPDATE " + root + "/data",
          "DATA ENTRY_DELETED " + root + "/deleted", "DIRECT_CHILD " + root + "/directChild",
          "CHILD ENTRY_DATA_CHANGE " + root + "/tree");
      Assert.assertEquals(client.getZkClient().getSessionId(), sessionId,
          "The client should have reconnected on the same session");

      writer.recursiveDelete(root);
    }
  }

  @Test
  public void testListenersSurviveSessionExpiry() throws Exception {
    String root = "/testListenersSurviveSessionExpiry";
    try (ZkMetaClient<String> writer = createZkMetaClient();
        ZkMetaClient<String> client = createZkMetaClient()) {
      writer.connect();
      client.connect();
      createEntries(writer, root);
      Set<String> events = subscribeListeners(client, root);

      TestUtil.expireSession(client);
      assertEvents(events, "DATA ENTRY_UPDATE " + root + "/data",
          "DIRECT_CHILD " + root + "/directChild", "CHILD ENTRY_DATA_CHANGE " + root + "/tree");

      events.clear();
      writer.set(root + "/data", "v1", -1);
      writer.create(root + "/directChild/new", "v0");
      writer.create(root + "/tree/new", "v0");
      assertEvents(events, "DATA ENTRY_UPDATE " + root + "/data",
          "DIRECT_CHILD " + root + "/directChild", "CHILD ENTRY_CREATED " + root + "/tree/new");

      writer.recursiveDelete(root);
    }
  }

  private static void createEntries(ZkMetaClient<String> writer, String root) {
    writer.create(root, "");
    writer.create(root + "/data", "v0");
    writer.create(root + "/directChild", "");
    writer.create(root + "/tree", "");
  }

  private static Set<String> subscribeListeners(ZkMetaClient<String> client, String root) {
    Set<String> events = ConcurrentHashMap.newKeySet();
    client.subscribeDataChange(root + "/data",
        (key, data, changeType) -> events.add("DATA " + changeType + " " + key), false);
    client.subscribeDirectChildChange(root + "/directChild",
        key -> events.add("DIRECT_CHILD " + key), false);
    client.subscribeChildChanges(root + "/tree",
        (changedPath, changeType) -> events.add("CHILD " + changeType + " " + changedPath), false);
    return events;
  }

  private static void assertEvents(Set<String> events, String... expected) throws Exception {
    Set<String> expectedEvents = new HashSet<>(Arrays.asList(expected));
    Assert.assertTrue(MetaClientTestUtil.verify(() -> events.equals(expectedEvents), TIMEOUT_MS),
        "Expected events " + expectedEvents + " but got " + events);
  }

  /**
   * A TCP proxy to the ZooKeeper server. Cutting it drops the client connection without expiring
   * the session, and reopening it lets the client reconnect on the same session.
   */
  private static class ZkProxy implements AutoCloseable {
    private final String _targetHost;
    private final int _targetPort;
    private final int _port;
    private final List<Socket> _sockets = new CopyOnWriteArrayList<>();
    private volatile ServerSocket _serverSocket;

    ZkProxy(String targetAddress) throws IOException {
      String[] hostPort = targetAddress.split(":");
      _targetHost = hostPort[0];
      _targetPort = Integer.parseInt(hostPort[1]);
      try (ServerSocket freePort = new ServerSocket(0)) {
        _port = freePort.getLocalPort();
      }
      open();
    }

    String getAddress() {
      return "localhost:" + _port;
    }

    synchronized void open() throws IOException {
      ServerSocket serverSocket = new ServerSocket();
      serverSocket.setReuseAddress(true);
      serverSocket.bind(new InetSocketAddress("localhost", _port));
      _serverSocket = serverSocket;
      startDaemon(() -> {
        while (!serverSocket.isClosed()) {
          try {
            Socket client = serverSocket.accept();
            Socket server = new Socket(_targetHost, _targetPort);
            _sockets.add(client);
            _sockets.add(server);
            startDaemon(() -> pipe(client, server));
            startDaemon(() -> pipe(server, client));
          } catch (IOException e) {
            return;
          }
        }
      });
    }

    synchronized void cut() {
      closeQuietly(_serverSocket);
      for (Socket socket : _sockets) {
        closeQuietly(socket);
      }
      _sockets.clear();
    }

    @Override
    public void close() {
      cut();
    }

    private static void pipe(Socket from, Socket to) {
      byte[] buffer = new byte[8192];
      try (InputStream in = from.getInputStream(); OutputStream out = to.getOutputStream()) {
        int read;
        while ((read = in.read(buffer)) >= 0) {
          out.write(buffer, 0, read);
          out.flush();
        }
      } catch (IOException ignored) {
        // The connection was cut.
      } finally {
        closeQuietly(from);
        closeQuietly(to);
      }
    }

    private static void startDaemon(Runnable runnable) {
      Thread thread = new Thread(runnable);
      thread.setDaemon(true);
      thread.start();
    }

    private static void closeQuietly(AutoCloseable closeable) {
      if (closeable == null) {
        return;
      }
      try {
        closeable.close();
      } catch (Exception ignored) {
        // Best effort.
      }
    }
  }
}
