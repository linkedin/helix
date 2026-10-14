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
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import javax.ws.rs.core.Response;

import org.apache.helix.PropertyPathBuilder;
import org.apache.helix.TestHelper;
import org.apache.helix.model.BuiltInStateModelDefinitions;
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
import org.testng.Assert;
import org.testng.annotations.Test;

/**
 * Verifies the ZooKeeper ACL applied to clusters created through the REST server when the request
 * carries acl query parameters. The ACLs used here include world:anyone, which the unauthenticated
 * test client and the REST server's session both match, and differ from the default ACL so the
 * request's ACL can be told apart. world:anyone read is expected on every requested ACL.
 */
public class TestClusterAcl extends AbstractTestClass {

  @Test
  public void testWorldReadIsMergedIntoRequestedWorldEntry() throws Exception {
    String cluster = "TestClusterAcl_worldMerge";
    try {
      Assert.assertEquals(createCluster(cluster, "world:anyone:cdw"),
          Response.Status.CREATED.getStatusCode());
      assertClusterAcl(cluster, Collections.singletonList(new ACL(
          ZooDefs.Perms.CREATE | ZooDefs.Perms.DELETE | ZooDefs.Perms.READ | ZooDefs.Perms.WRITE,
          new Id("world", "anyone"))));
    } finally {
      TestHelper.dropCluster(cluster, _gZkClient);
    }
  }

  @Test
  public void testRequestAclWithMultipleEntries() throws Exception {
    String cluster = "TestClusterAcl_requestMulti";
    try {
      Assert.assertEquals(createCluster(cluster, "world:anyone:cdw", "ip:127.0.0.1:cdrwa"),
          Response.Status.CREATED.getStatusCode());
      // Entries keep their order, and read is merged into the requested world entry
      assertClusterAcl(cluster, Arrays.asList(
          new ACL(ZooDefs.Perms.CREATE | ZooDefs.Perms.DELETE | ZooDefs.Perms.READ
              | ZooDefs.Perms.WRITE, new Id("world", "anyone")),
          new ACL(ZooDefs.Perms.ALL, new Id("ip", "127.0.0.1"))));
    } finally {
      TestHelper.dropCluster(cluster, _gZkClient);
    }
  }

  @Test
  public void testAuthSchemeWithoutAuthenticatedSessionFails() throws Exception {
    String cluster = "TestClusterAcl_auth";
    try {
      // The REST server's ZK session is unauthenticated, so "auth" has no identity to expand to.
      Assert.assertEquals(createCluster(cluster, "auth::cdrwa"),
          Response.Status.INTERNAL_SERVER_ERROR.getStatusCode());
      Assert.assertFalse(_gZkClient.exists("/" + cluster));
    } finally {
      TestHelper.dropCluster(cluster, _gZkClient);
    }
  }

  @Test
  public void testNoAclKeepsDefaultAcl() throws Exception {
    String cluster = "TestClusterAcl_default";
    try {
      Assert.assertEquals(createCluster(cluster), Response.Status.CREATED.getStatusCode());
      assertClusterAcl(cluster, ZooDefs.Ids.OPEN_ACL_UNSAFE);
    } finally {
      TestHelper.dropCluster(cluster, _gZkClient);
    }
  }

  @Test
  public void testInvalidRequestAclReturnsBadRequest() throws Exception {
    String cluster = "TestClusterAcl_requestInvalid";
    try {
      Assert.assertEquals(createCluster(cluster, "world:anyone:z"),
          Response.Status.BAD_REQUEST.getStatusCode());
      Assert.assertEquals(createCluster(cluster, "not-an-acl"),
          Response.Status.BAD_REQUEST.getStatusCode());
      Assert.assertFalse(_gZkClient.exists("/" + cluster));
    } finally {
      TestHelper.dropCluster(cluster, _gZkClient);
    }
  }

  @Test
  public void testServerOutsideAclFailsCreation() throws Exception {
    String cluster = "TestClusterAcl_unauthorized";
    String credentials = "owner:secret";
    String ownerOnlyAcl = "digest:" + DigestAuthenticationProvider.generateDigest(credentials)
        + ":cdrwa";
    HelixZkClient ownerClient = DedicatedZkClientFactory.getInstance().buildZkClient(
        new HelixZkClient.ZkConnectionConfig(ZK_ADDR),
        new HelixZkClient.ZkClientConfig().setZkSerializer(new ZNRecordSerializer()));
    try {
      // The REST server's ZK session is not the digest owner, so metadata creation under the
      // owner-only root is rejected and surfaced as a server error.
      Assert.assertEquals(createCluster(cluster, ownerOnlyAcl),
          Response.Status.INTERNAL_SERVER_ERROR.getStatusCode());
    } finally {
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

  private int createCluster(String cluster, String... requestAcl) throws IOException {
    StringBuilder query = new StringBuilder();
    for (String entry : requestAcl) {
      query.append(query.length() == 0 ? '?' : '&').append("acl=")
          .append(URLEncoder.encode(entry, "UTF-8"));
    }
    HttpPut put = new HttpPut("http://" + getBaseUri().getHost() + ":" + getBaseUri().getPort()
        + getBaseUri().getPath() + "/clusters/" + cluster + query);
    try (CloseableHttpClient httpClient = HttpClients.createDefault();
        CloseableHttpResponse response = httpClient.execute(put)) {
      return response.getStatusLine().getStatusCode();
    }
  }

  private void assertClusterAcl(String cluster, List<ACL> expected) throws Exception {
    ZooKeeper zooKeeper =
        ((ZkConnection) ((org.apache.helix.zookeeper.zkclient.ZkClient) _gZkClient)
            .getConnection()).getZookeeper();
    List<String> paths = new ArrayList<>(Arrays.asList(
        "/" + cluster, PropertyPathBuilder.idealState(cluster),
        PropertyPathBuilder.clusterConfig(cluster), PropertyPathBuilder.liveInstance(cluster),
        PropertyPathBuilder.controller(cluster), PropertyPathBuilder.stateModelDef(cluster)));
    // ZooKeeper does not inherit ACLs, so each built-in state model definition is checked as well
    for (BuiltInStateModelDefinitions def : BuiltInStateModelDefinitions.values()) {
      paths.add(PropertyPathBuilder.stateModelDef(cluster) + "/"
          + def.getStateModelDefinition().getId());
    }
    for (String path : paths) {
      Assert.assertEquals(zooKeeper.getACL(path, new Stat()), expected, "unexpected ACL on " + path);
    }
  }
}
