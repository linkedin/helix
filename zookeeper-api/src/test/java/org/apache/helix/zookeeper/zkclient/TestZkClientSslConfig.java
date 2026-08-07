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

import org.apache.helix.zookeeper.api.client.HelixZkClient;
import org.apache.helix.zookeeper.api.client.RealmAwareZkClient;
import org.apache.zookeeper.client.ZKClientConfig;
import org.testng.Assert;
import org.testng.annotations.Test;


public class TestZkClientSslConfig {

  @Test
  public void testCreateZKClientConfigWithAllFields() {
    ZkClientSslConfig sslConfig = new ZkClientSslConfig()
        .setSslEnabled(true)
        .setClientCnxnSocket("org.apache.zookeeper.ClientCnxnSocketNetty")
        .setKeyStoreLocation("/certs/identity.keystore.p12")
        .setKeyStorePassword("ks-pass")
        .setKeyStoreType("PKCS12")
        .setTrustStoreLocation("/certs/truststore.jks")
        .setTrustStorePassword("ts-pass")
        .setTrustStoreType("JKS");

    ZKClientConfig zkClientConfig = sslConfig.createZKClientConfig();

    Assert.assertEquals(zkClientConfig.getProperty("zookeeper.client.secure"), "true");
    Assert.assertEquals(zkClientConfig.getProperty("zookeeper.clientCnxnSocket"),
        "org.apache.zookeeper.ClientCnxnSocketNetty");
    Assert.assertEquals(zkClientConfig.getProperty("zookeeper.ssl.keyStore.location"),
        "/certs/identity.keystore.p12");
    Assert.assertEquals(zkClientConfig.getProperty("zookeeper.ssl.keyStore.password"), "ks-pass");
    Assert.assertEquals(zkClientConfig.getProperty("zookeeper.ssl.keyStore.type"), "PKCS12");
    Assert.assertEquals(zkClientConfig.getProperty("zookeeper.ssl.trustStore.location"),
        "/certs/truststore.jks");
    Assert.assertEquals(zkClientConfig.getProperty("zookeeper.ssl.trustStore.password"), "ts-pass");
    Assert.assertEquals(zkClientConfig.getProperty("zookeeper.ssl.trustStore.type"), "JKS");
  }

  @Test
  public void testCreateZKClientConfigUsesDefaults() {
    // Only set the required stores; cnxn socket and trust store type should fall back to defaults.
    ZkClientSslConfig sslConfig = new ZkClientSslConfig()
        .setSslEnabled(true)
        .setKeyStoreLocation("/certs/identity.keystore.p12")
        .setKeyStorePassword("ks-pass")
        .setTrustStoreLocation("/certs/truststore.jks")
        .setTrustStorePassword("ts-pass");

    ZKClientConfig zkClientConfig = sslConfig.createZKClientConfig();

    Assert.assertEquals(zkClientConfig.getProperty("zookeeper.clientCnxnSocket"),
        "org.apache.zookeeper.ClientCnxnSocketNetty");
    Assert.assertEquals(zkClientConfig.getProperty("zookeeper.ssl.trustStore.type"), "JKS");
    // Optional keystore type was never set and must not be added.
    Assert.assertNull(zkClientConfig.getProperty("zookeeper.ssl.keyStore.type"));
  }

  @Test
  public void testRealmAwareZkConnectionConfigCarriesSslConfig() {
    ZkClientSslConfig sslConfig = new ZkClientSslConfig().setSslEnabled(true);

    RealmAwareZkClient.RealmAwareZkConnectionConfig connectionConfig =
        new RealmAwareZkClient.RealmAwareZkConnectionConfig.Builder().setSslConfig(sslConfig)
            .build();

    Assert.assertSame(connectionConfig.getSslConfig(), sslConfig);
    Assert.assertTrue(connectionConfig.getSslConfig().isSslEnabled());
  }

  @Test
  public void testHelixZkConnectionConfigCarriesSslConfig() {
    ZkClientSslConfig sslConfig = new ZkClientSslConfig().setSslEnabled(true);

    HelixZkClient.ZkConnectionConfig connectionConfig =
        new HelixZkClient.ZkConnectionConfig("localhost:2281").setSslConfig(sslConfig);

    Assert.assertSame(connectionConfig.getSslConfig(), sslConfig);
  }

  @Test
  public void testDefaultConnectionConfigsHaveNoSslConfig() {
    Assert.assertNull(
        new RealmAwareZkClient.RealmAwareZkConnectionConfig.Builder().build().getSslConfig());
    Assert.assertNull(new HelixZkClient.ZkConnectionConfig("localhost:2181").getSslConfig());
  }
}
