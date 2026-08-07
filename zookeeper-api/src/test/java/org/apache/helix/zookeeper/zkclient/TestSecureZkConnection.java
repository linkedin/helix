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

import java.io.File;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import org.apache.helix.zookeeper.api.client.HelixZkClient;
import org.apache.helix.zookeeper.impl.factory.DedicatedZkClientFactory;
import org.apache.helix.zookeeper.zkclient.exception.ZkException;
import org.apache.zookeeper.server.ServerCnxnFactory;
import org.apache.zookeeper.server.ZooKeeperServer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;


/**
 * End-to-end verification that Helix actually passes TLS certs to ZooKeeper over the wire when a
 * per-connection {@link ZkClientSslConfig} is supplied.
 * <p>
 * The test stands up a real, TLS-only ZooKeeper server (Netty transport, mutual TLS / client-auth
 * required). The server reads its keystore/truststore from JVM system properties (the only way a ZK
 * server can be configured for SSL). The Helix client, by contrast, receives its certs solely
 * through a per-connection {@link ZkClientSslConfig} that is threaded down to the 4-arg
 * {@code new ZooKeeper(..., ZKClientConfig)} constructor by {@link ZkConnection}.
 * <p>
 * Because {@code zookeeper.client.secure} and {@code zookeeper.clientCnxnSocket} are client-only
 * settings that the server never sets, and they are cleared from the JVM in setup, the only thing
 * that flips the Helix client into TLS mode is the per-connection {@link ZkClientSslConfig}:
 * <ul>
 *   <li>{@code testSecureClientWithSslConfigConnects} — a client built <b>with</b> the SSL config
 *       completes a mutually-authenticated TLS handshake and can create/read/delete a znode.</li>
 *   <li>{@code testPlaintextClientRejectedBySecurePort} — a client built <b>without</b> the SSL
 *       config (same JVM, same system properties) cannot establish a session with the TLS-only
 *       port.</li>
 * </ul>
 * The delta between the two isolates the per-connection cert passing as the enabler.
 * <p>
 * {@link DedicatedZkClientFactory} is used (not the shared factory) so each client gets its own
 * {@link ZkConnection}; the shared factory pools connections by a config key that intentionally
 * ignores certs, which would otherwise let the two clients share one underlying connection.
 */
@SuppressWarnings("deprecation")
public class TestSecureZkConnection {
  private static final Logger LOG = LoggerFactory.getLogger(TestSecureZkConnection.class);

  private static final String KEYSTORE_PASSWORD = "helixtest";
  private static final String STORE_TYPE = "JKS";
  private static final String CERT_ALIAS = "helixtest";

  private File _certDir;
  private File _snapDir;
  private File _logDir;
  private String _keyStorePath;
  private String _trustStorePath;
  private int _securePort;
  private ServerCnxnFactory _cnxnFactory;
  private final Map<String, String> _originalSystemProps = new LinkedHashMap<>();

  @BeforeClass
  public void beforeClass() throws Exception {
    _certDir = Files.createTempDirectory("helix-zk-secure-certs").toFile();
    _snapDir = Files.createTempDirectory("helix-zk-secure-snap").toFile();
    _logDir = Files.createTempDirectory("helix-zk-secure-log").toFile();

    generateSelfSignedStores();

    // ZooKeeper only supports TLS over the Netty transport, so force the server to use the Netty
    // connection factory (the default is NIO, which is plaintext-only).
    overrideSystemProperty(ServerCnxnFactory.ZOOKEEPER_SERVER_CNXN_FACTORY,
        "org.apache.zookeeper.server.NettyServerCnxnFactory");
    // Require client certificates so the server authenticates the Helix client (mutual TLS).
    overrideSystemProperty("zookeeper.ssl.clientAuth", "need");
    // Register ZooKeeper's X509 authentication provider so the server can validate the client
    // certificate presented during the mutual-TLS handshake (required when clientAuth=need).
    overrideSystemProperty("zookeeper.authProvider.x509",
        "org.apache.zookeeper.server.auth.X509AuthenticationProvider");
    // Server-side SSL config can only be supplied via system properties.
    overrideSystemProperty("zookeeper.ssl.keyStore.location", _keyStorePath);
    overrideSystemProperty("zookeeper.ssl.keyStore.password", KEYSTORE_PASSWORD);
    overrideSystemProperty("zookeeper.ssl.keyStore.type", STORE_TYPE);
    overrideSystemProperty("zookeeper.ssl.trustStore.location", _trustStorePath);
    overrideSystemProperty("zookeeper.ssl.trustStore.password", KEYSTORE_PASSWORD);
    overrideSystemProperty("zookeeper.ssl.trustStore.type", STORE_TYPE);
    // Connecting to localhost/127.0.0.1 - keep cert trust as the sole check, not hostname matching.
    overrideSystemProperty("zookeeper.ssl.hostnameVerification", "false");
    // Ensure the client only goes secure via the per-connection ZkClientSslConfig, never via
    // inherited system properties.
    overrideSystemProperty("zookeeper.client.secure", null);
    overrideSystemProperty("zookeeper.clientCnxnSocket", null);

    _securePort = findFreePort();
    startSecureZkServer();
  }

  @AfterClass
  public void afterClass() {
    if (_cnxnFactory != null) {
      _cnxnFactory.shutdown();
    }
    restoreSystemProperties();
    deleteRecursively(_certDir);
    deleteRecursively(_snapDir);
    deleteRecursively(_logDir);
  }

  @Test
  public void testSecureClientWithSslConfigConnects() {
    ZkClientSslConfig sslConfig = new ZkClientSslConfig()
        .setSslEnabled(true)
        .setKeyStoreLocation(_keyStorePath)
        .setKeyStorePassword(KEYSTORE_PASSWORD)
        .setKeyStoreType(STORE_TYPE)
        .setTrustStoreLocation(_trustStorePath)
        .setTrustStorePassword(KEYSTORE_PASSWORD)
        .setTrustStoreType(STORE_TYPE);

    HelixZkClient client = DedicatedZkClientFactory.getInstance().buildZkClient(
        new HelixZkClient.ZkConnectionConfig("localhost:" + _securePort).setSessionTimeout(30000)
            .setSslConfig(sslConfig),
        new HelixZkClient.ZkClientConfig().setConnectInitTimeout(30000));
    try {
      Assert.assertTrue(client.waitUntilConnected(30, TimeUnit.SECONDS),
          "Secure Helix client should establish a TLS session with the secure ZK port");

      String path = "/helixSecureZkTest";
      client.createPersistent(path);
      Assert.assertTrue(client.exists(path),
          "znode created over the TLS connection should exist");
      client.delete(path);
    } finally {
      client.close();
    }
  }

  @Test
  public void testPlaintextClientRejectedBySecurePort() {
    HelixZkClient client = null;
    try {
      // No ZkClientSslConfig => plaintext client. It must not be able to talk to the TLS-only port.
      client = DedicatedZkClientFactory.getInstance().buildZkClient(
          new HelixZkClient.ZkConnectionConfig("localhost:" + _securePort).setSessionTimeout(30000),
          new HelixZkClient.ZkClientConfig().setConnectInitTimeout(5000));
      boolean connected = client.waitUntilConnected(6, TimeUnit.SECONDS);
      Assert.assertFalse(connected,
          "A plaintext client must NOT establish a session with the TLS-only secure port");
    } catch (ZkException expected) {
      // Expected: the plaintext client times out / fails to connect to the TLS-only port.
      LOG.info("Plaintext client was correctly rejected by the secure port: {}",
          expected.getMessage());
    } finally {
      if (client != null) {
        client.close();
      }
    }
  }

  private void startSecureZkServer() throws Exception {
    ZooKeeperServer zkServer = new ZooKeeperServer(_snapDir, _logDir, 3000);
    // createFactory() reads zookeeper.serverCnxnFactory (set to Netty in beforeClass) to pick the
    // transport; NettyServerCnxnFactory's constructor is package-private so it cannot be newed here.
    _cnxnFactory = ServerCnxnFactory.createFactory();
    // (address, maxClientCnxns, listenBacklog=-1 for default, secure=true)
    _cnxnFactory.configure(new InetSocketAddress("127.0.0.1", _securePort), 60, -1, true);
    _cnxnFactory.startup(zkServer);
    Assert.assertTrue(zkServer.isRunning(), "Secure ZooKeeper server should be running");
    LOG.info("Started TLS-only ZooKeeper server on port {}", _securePort);
  }

  private void generateSelfSignedStores() throws Exception {
    _keyStorePath = new File(_certDir, "keystore.jks").getAbsolutePath();
    _trustStorePath = new File(_certDir, "truststore.jks").getAbsolutePath();
    File certFile = new File(_certDir, "cert.pem");

    // A single self-signed cert used for both server identity and mutual-TLS client identity;
    // the truststore trusts that same cert so both directions of the handshake succeed.
    runKeytool("-genkeypair", "-alias", CERT_ALIAS, "-keyalg", "RSA", "-keysize", "2048",
        "-validity", "3650", "-dname", "CN=localhost,OU=helix,O=apache,C=US",
        "-ext", "SAN=dns:localhost,ip:127.0.0.1",
        "-keystore", _keyStorePath, "-storepass", KEYSTORE_PASSWORD, "-keypass", KEYSTORE_PASSWORD,
        "-storetype", STORE_TYPE);
    runKeytool("-exportcert", "-alias", CERT_ALIAS, "-keystore", _keyStorePath, "-storepass",
        KEYSTORE_PASSWORD, "-rfc", "-file", certFile.getAbsolutePath());
    runKeytool("-importcert", "-alias", CERT_ALIAS, "-file", certFile.getAbsolutePath(),
        "-keystore", _trustStorePath, "-storepass", KEYSTORE_PASSWORD, "-storetype", STORE_TYPE,
        "-noprompt");
  }

  private static void runKeytool(String... args) throws Exception {
    List<String> command = new ArrayList<>();
    command.add(System.getProperty("java.home") + File.separator + "bin" + File.separator + "keytool");
    command.addAll(Arrays.asList(args));
    Process process = new ProcessBuilder(command).redirectErrorStream(true).start();
    String output = new String(process.getInputStream().readAllBytes(), StandardCharsets.UTF_8);
    int exitCode = process.waitFor();
    if (exitCode != 0) {
      throw new IllegalStateException(
          "keytool failed (exit " + exitCode + "): " + output + "\ncommand=" + command);
    }
  }

  private static int findFreePort() throws Exception {
    try (ServerSocket socket = new ServerSocket(0)) {
      socket.setReuseAddress(true);
      return socket.getLocalPort();
    }
  }

  private void overrideSystemProperty(String key, String value) {
    if (!_originalSystemProps.containsKey(key)) {
      _originalSystemProps.put(key, System.getProperty(key));
    }
    if (value == null) {
      System.clearProperty(key);
    } else {
      System.setProperty(key, value);
    }
  }

  private void restoreSystemProperties() {
    for (Map.Entry<String, String> entry : _originalSystemProps.entrySet()) {
      if (entry.getValue() == null) {
        System.clearProperty(entry.getKey());
      } else {
        System.setProperty(entry.getKey(), entry.getValue());
      }
    }
    _originalSystemProps.clear();
  }

  private static void deleteRecursively(File file) {
    if (file == null || !file.exists()) {
      return;
    }
    File[] children = file.listFiles();
    if (children != null) {
      for (File child : children) {
        deleteRecursively(child);
      }
    }
    if (!file.delete()) {
      file.deleteOnExit();
    }
  }
}
