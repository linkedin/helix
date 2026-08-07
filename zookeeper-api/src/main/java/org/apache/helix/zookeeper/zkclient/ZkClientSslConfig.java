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

import org.apache.zookeeper.client.ZKClientConfig;


/**
 * SSL/TLS configuration for a single ZooKeeper connection.
 * <p>
 * This lets Helix establish a secure (TLS/mTLS) connection to ZooKeeper on a
 * <b>per-connection</b> basis, instead of relying on the caller setting global JVM
 * system properties (e.g. {@code -Dzookeeper.ssl.keyStore.location=...}) before the
 * client connects. When enabled, the config is materialized into a
 * {@link ZKClientConfig} that is passed to the underlying
 * {@code new ZooKeeper(connectString, sessionTimeout, watcher, zkClientConfig)} constructor
 * by {@link ZkConnection}.
 * <p>
 * The keystore holds the client identity certificate (used for mutual TLS) and the
 * truststore holds the CA/certs used to verify the ZooKeeper server.
 */
public class ZkClientSslConfig {
  // The ZooKeeper client connection socket. TLS is only supported over the Netty socket.
  private static final String DEFAULT_CLIENT_CNXN_SOCKET =
      "org.apache.zookeeper.ClientCnxnSocketNetty";
  private static final String DEFAULT_TRUST_STORE_TYPE = "JKS";

  // ZooKeeper client SSL system-property keys (ZK 3.6.x). Set on a per-connection ZKClientConfig
  // rather than globally via System.setProperty.
  private static final String ZK_CLIENT_SECURE = "zookeeper.client.secure";
  private static final String ZK_CLIENT_CNXN_SOCKET = "zookeeper.clientCnxnSocket";
  private static final String ZK_SSL_KEYSTORE_LOCATION = "zookeeper.ssl.keyStore.location";
  private static final String ZK_SSL_KEYSTORE_PASSWORD = "zookeeper.ssl.keyStore.password";
  private static final String ZK_SSL_KEYSTORE_TYPE = "zookeeper.ssl.keyStore.type";
  private static final String ZK_SSL_TRUSTSTORE_LOCATION = "zookeeper.ssl.trustStore.location";
  private static final String ZK_SSL_TRUSTSTORE_PASSWORD = "zookeeper.ssl.trustStore.password";
  private static final String ZK_SSL_TRUSTSTORE_TYPE = "zookeeper.ssl.trustStore.type";

  private boolean _sslEnabled;
  private String _clientCnxnSocket = DEFAULT_CLIENT_CNXN_SOCKET;
  private String _keyStoreLocation;
  private String _keyStorePassword;
  private String _keyStoreType;
  private String _trustStoreLocation;
  private String _trustStorePassword;
  private String _trustStoreType = DEFAULT_TRUST_STORE_TYPE;

  public boolean isSslEnabled() {
    return _sslEnabled;
  }

  public ZkClientSslConfig setSslEnabled(boolean sslEnabled) {
    _sslEnabled = sslEnabled;
    return this;
  }

  public String getClientCnxnSocket() {
    return _clientCnxnSocket;
  }

  public ZkClientSslConfig setClientCnxnSocket(String clientCnxnSocket) {
    _clientCnxnSocket = clientCnxnSocket;
    return this;
  }

  public String getKeyStoreLocation() {
    return _keyStoreLocation;
  }

  public ZkClientSslConfig setKeyStoreLocation(String keyStoreLocation) {
    _keyStoreLocation = keyStoreLocation;
    return this;
  }

  public String getKeyStorePassword() {
    return _keyStorePassword;
  }

  public ZkClientSslConfig setKeyStorePassword(String keyStorePassword) {
    _keyStorePassword = keyStorePassword;
    return this;
  }

  public String getKeyStoreType() {
    return _keyStoreType;
  }

  public ZkClientSslConfig setKeyStoreType(String keyStoreType) {
    _keyStoreType = keyStoreType;
    return this;
  }

  public String getTrustStoreLocation() {
    return _trustStoreLocation;
  }

  public ZkClientSslConfig setTrustStoreLocation(String trustStoreLocation) {
    _trustStoreLocation = trustStoreLocation;
    return this;
  }

  public String getTrustStorePassword() {
    return _trustStorePassword;
  }

  public ZkClientSslConfig setTrustStorePassword(String trustStorePassword) {
    _trustStorePassword = trustStorePassword;
    return this;
  }

  public String getTrustStoreType() {
    return _trustStoreType;
  }

  public ZkClientSslConfig setTrustStoreType(String trustStoreType) {
    _trustStoreType = trustStoreType;
    return this;
  }

  /**
   * Materialize this config into a {@link ZKClientConfig} carrying the SSL properties so the
   * underlying ZooKeeper client performs a TLS/mTLS handshake using the configured keystore
   * (client identity) and truststore (server verification).
   *
   * @return a {@link ZKClientConfig} with the secure-client and SSL keystore/truststore properties set
   */
  public ZKClientConfig createZKClientConfig() {
    ZKClientConfig zkClientConfig = new ZKClientConfig();
    zkClientConfig.setProperty(ZK_CLIENT_SECURE, Boolean.toString(true));
    zkClientConfig.setProperty(ZK_CLIENT_CNXN_SOCKET, _clientCnxnSocket);
    setIfNotEmpty(zkClientConfig, ZK_SSL_KEYSTORE_LOCATION, _keyStoreLocation);
    setIfNotEmpty(zkClientConfig, ZK_SSL_KEYSTORE_PASSWORD, _keyStorePassword);
    setIfNotEmpty(zkClientConfig, ZK_SSL_KEYSTORE_TYPE, _keyStoreType);
    setIfNotEmpty(zkClientConfig, ZK_SSL_TRUSTSTORE_LOCATION, _trustStoreLocation);
    setIfNotEmpty(zkClientConfig, ZK_SSL_TRUSTSTORE_PASSWORD, _trustStorePassword);
    setIfNotEmpty(zkClientConfig, ZK_SSL_TRUSTSTORE_TYPE, _trustStoreType);
    return zkClientConfig;
  }

  private static void setIfNotEmpty(ZKClientConfig zkClientConfig, String key, String value) {
    if (value != null && !value.isEmpty()) {
      zkClientConfig.setProperty(key, value);
    }
  }
}
