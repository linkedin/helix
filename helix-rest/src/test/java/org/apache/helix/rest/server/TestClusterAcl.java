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

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.List;
import javax.ws.rs.core.Response;

import org.apache.helix.PropertyPathBuilder;
import org.apache.helix.TestHelper;
import org.apache.helix.rest.acl.ClusterAclParser;
import org.apache.helix.rest.acl.ClusterAclProvider;
import org.apache.helix.rest.acl.NoopAclRegister;
import org.apache.helix.rest.acl.NoopClusterAclProvider;
import org.apache.helix.rest.common.HelixRestNamespace;
import org.apache.helix.rest.server.authValidator.NoopAuthValidator;
import org.apache.helix.zookeeper.api.client.HelixZkClient;
import org.apache.helix.zookeeper.datamodel.serializer.ZNRecordSerializer;
import org.apache.helix.zookeeper.impl.factory.DedicatedZkClientFactory;
import org.apache.helix.zookeeper.zkclient.ZkConnection;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpPut;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.ACL;
import org.apache.zookeeper.data.Id;
import org.apache.zookeeper.data.Stat;
import org.apache.zookeeper.server.auth.DigestAuthenticationProvider;
import org.mockito.Mockito;
import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;

/**
 * Verifies the ZooKeeper ACL applied to clusters created through the REST server. The ACLs used
 * here include the unauthenticated test client (world:anyone) so the REST server satisfies them,
 * and differ from the default ACL and from each other so each source can be told apart.
 */
public class TestClusterAcl extends AbstractTestClass {
  private static final List<ACL> NAMESPACE_ACL =
      ClusterAclParser.parse(Collections.singletonList("world:anyone:cdrw"));
  private static final List<ACL> PROVIDER_ACL =
      ClusterAclParser.parse(Collections.singletonList("world:anyone:cdr"));

  private int _port;

  @BeforeClass
  public void beforeClass() {
    _port = getBaseUri().getPort() + 2;
  }

  @Test
  public void testNamespaceAclIsApplied() throws Exception {
    String cluster = "TestClusterAcl_namespace";
    HelixRestServer server = startServer(NAMESPACE_ACL, new NoopClusterAclProvider());
    try {
      Assert.assertEquals(createCluster(cluster), Response.Status.CREATED.getStatusCode());
      assertClusterAcl(cluster, NAMESPACE_ACL);
    } finally {
      server.shutdown();
      TestHelper.dropCluster(cluster, _gZkClient);
    }
  }

  @Test
  public void testProviderAclOverridesNamespaceAcl() throws Exception {
    String cluster = "TestClusterAcl_provider";
    ClusterAclProvider provider = Mockito.mock(ClusterAclProvider.class);
    Mockito.when(provider.getClusterAcl(any(), eq(cluster))).thenReturn(PROVIDER_ACL);
    HelixRestServer server = startServer(NAMESPACE_ACL, provider);
    try {
      Assert.assertEquals(createCluster(cluster), Response.Status.CREATED.getStatusCode());
      assertClusterAcl(cluster, PROVIDER_ACL);
      Mockito.verify(provider).getClusterAcl(any(), eq(cluster));
    } finally {
      server.shutdown();
      TestHelper.dropCluster(cluster, _gZkClient);
    }
  }

  @Test
  public void testProviderWithoutAclFallsBackToNamespaceAcl() throws Exception {
    String cluster = "TestClusterAcl_fallback";
    ClusterAclProvider provider = Mockito.mock(ClusterAclProvider.class);
    Mockito.when(provider.getClusterAcl(any(), any())).thenReturn(Collections.<ACL>emptyList());
    HelixRestServer server = startServer(NAMESPACE_ACL, provider);
    try {
      Assert.assertEquals(createCluster(cluster), Response.Status.CREATED.getStatusCode());
      assertClusterAcl(cluster, NAMESPACE_ACL);
    } finally {
      server.shutdown();
      TestHelper.dropCluster(cluster, _gZkClient);
    }
  }

  @Test
  public void testNoAclKeepsDefaultAcl() throws Exception {
    String cluster = "TestClusterAcl_default";
    HelixRestServer server = startServer(null, new NoopClusterAclProvider());
    try {
      Assert.assertEquals(createCluster(cluster), Response.Status.CREATED.getStatusCode());
      assertClusterAcl(cluster, ZooDefs.Ids.OPEN_ACL_UNSAFE);
    } finally {
      server.shutdown();
      TestHelper.dropCluster(cluster, _gZkClient);
    }
  }

  @Test
  public void testProviderFailureDoesNotCreateCluster() throws Exception {
    String cluster = "TestClusterAcl_providerFailure";
    ClusterAclProvider provider = Mockito.mock(ClusterAclProvider.class);
    Mockito.when(provider.getClusterAcl(any(), any())).thenThrow(new IllegalStateException("boom"));
    HelixRestServer server = startServer(NAMESPACE_ACL, provider);
    try {
      Assert.assertEquals(createCluster(cluster),
          Response.Status.INTERNAL_SERVER_ERROR.getStatusCode());
      Assert.assertFalse(_gZkClient.exists("/" + cluster));
    } finally {
      server.shutdown();
      TestHelper.dropCluster(cluster, _gZkClient);
    }
  }

  @Test
  public void testServerOutsideAclFailsCreation() throws Exception {
    String cluster = "TestClusterAcl_unauthorized";
    String credentials = "owner:secret";
    List<ACL> ownerOnlyAcl = Collections.singletonList(new ACL(ZooDefs.Perms.ALL,
        new Id("digest", DigestAuthenticationProvider.generateDigest(credentials))));
    HelixRestServer server = startServer(ownerOnlyAcl, new NoopClusterAclProvider());
    HelixZkClient ownerClient = DedicatedZkClientFactory.getInstance().buildZkClient(
        new HelixZkClient.ZkConnectionConfig(ZK_ADDR),
        new HelixZkClient.ZkClientConfig().setZkSerializer(new ZNRecordSerializer()));
    try {
      // The REST server's ZK session is not the digest owner, so metadata creation under the
      // owner-only root is rejected and surfaced as a server error.
      Assert.assertEquals(createCluster(cluster),
          Response.Status.INTERNAL_SERVER_ERROR.getStatusCode());
    } finally {
      server.shutdown();
      try {
        // Creation is not rolled back, and only the digest owner can remove what was left behind.
        ((org.apache.helix.zookeeper.zkclient.ZkClient) ownerClient)
            .addAuthInfo("digest", credentials.getBytes(StandardCharsets.UTF_8));
        if (ownerClient.exists("/" + cluster)) {
          ownerClient.deleteRecursively("/" + cluster);
        }
      } finally {
        ownerClient.close();
      }
    }
  }

  private HelixRestServer startServer(List<ACL> namespaceAcl, ClusterAclProvider provider)
      throws Exception {
    HelixRestNamespace namespace =
        new HelixRestNamespace(HelixRestNamespace.DEFAULT_NAMESPACE_NAME,
            HelixRestNamespace.HelixMetadataStoreType.ZOOKEEPER, ZK_ADDR, true, false, null,
            namespaceAcl);
    HelixRestServer server = new HelixRestServer(Collections.singletonList(namespace), _port,
        getBaseUri().getPath(), Collections.emptyList(), new NoopAuthValidator(),
        new NoopAuthValidator(), new NoopAclRegister(), provider);
    server.start();
    return server;
  }

  private int createCluster(String cluster) throws IOException {
    HttpPut put = new HttpPut("http://" + getBaseUri().getHost() + ":" + _port
        + getBaseUri().getPath() + "/clusters/" + cluster);
    // A new client per request, since each test restarts the server on the same port and a pooled
    // keep-alive connection to the previous server would be stale.
    try (CloseableHttpClient httpClient = HttpClients.createDefault();
        CloseableHttpResponse response = httpClient.execute(put)) {
      return response.getStatusLine().getStatusCode();
    }
  }

  private void assertClusterAcl(String cluster, List<ACL> expected) throws Exception {
    ZooKeeper zooKeeper =
        ((ZkConnection) ((org.apache.helix.zookeeper.zkclient.ZkClient) _gZkClient)
            .getConnection()).getZookeeper();
    for (String path : new String[] {
        "/" + cluster, PropertyPathBuilder.idealState(cluster),
        PropertyPathBuilder.clusterConfig(cluster), PropertyPathBuilder.liveInstance(cluster),
        PropertyPathBuilder.controller(cluster), PropertyPathBuilder.stateModelDef(cluster)
    }) {
      Assert.assertEquals(zooKeeper.getACL(path, new Stat()), expected, "unexpected ACL on " + path);
    }
  }
}
