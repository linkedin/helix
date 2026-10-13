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

import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.helix.rest.common.HelixRestNamespace;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.data.ACL;
import org.apache.zookeeper.data.Id;
import org.testng.Assert;
import org.testng.annotations.Test;

public class TestHelixRestMainManifest {

  @Test
  public void testClusterAclInNamespaceManifest() throws IOException {
    String manifest = String.join("\n",
        "- NAME: listAcl",
        "  METADATA_STORE_TYPE: ZOOKEEPER",
        "  METADATA_STORE_ADDRESS: localhost:2181",
        "  CLUSTER_ACL:",
        "    - \"x509:CN=svc,OU=team,O=org,C=US:cdrwa\"",
        "    - \"world:anyone:r\"",
        "- NAME: singleAcl",
        "  METADATA_STORE_TYPE: ZOOKEEPER",
        "  METADATA_STORE_ADDRESS: localhost:2181",
        "  CLUSTER_ACL: \"world:anyone:cdrw\"",
        "- NAME: noAcl",
        "  METADATA_STORE_TYPE: ZOOKEEPER",
        "  METADATA_STORE_ADDRESS: localhost:2181",
        "");

    List<HelixRestNamespace> namespaces = load(manifest);

    Assert.assertEquals(namespaces.size(), 3);
    Assert.assertEquals(namespaces.get(0).getClusterAcl(),
        Arrays.asList(new ACL(ZooDefs.Perms.ALL, new Id("x509", "CN=svc,OU=team,O=org,C=US")),
            new ACL(ZooDefs.Perms.READ, new Id("world", "anyone"))));
    Assert.assertEquals(namespaces.get(1).getClusterAcl(), Arrays.asList(new ACL(
        ZooDefs.Perms.CREATE | ZooDefs.Perms.DELETE | ZooDefs.Perms.READ | ZooDefs.Perms.WRITE,
        new Id("world", "anyone"))));
    Assert.assertTrue(namespaces.get(2).getClusterAcl().isEmpty());
    // identities are never exposed through the namespaces endpoint
    Assert.assertFalse(namespaces.get(0).getRestInfo()
        .containsKey(HelixRestNamespace.HelixRestNamespaceProperty.CLUSTER_ACL.name()));
  }

  @Test(expectedExceptions = IllegalArgumentException.class)
  public void testInvalidClusterAclFailsManifestLoad() throws IOException {
    load(String.join("\n",
        "- NAME: badAcl",
        "  METADATA_STORE_TYPE: ZOOKEEPER",
        "  METADATA_STORE_ADDRESS: localhost:2181",
        "  CLUSTER_ACL:",
        "    - \"world:anyone:z\"",
        ""));
  }

  private List<HelixRestNamespace> load(String manifest) throws IOException {
    File file = File.createTempFile("namespace-manifest", ".yaml");
    try {
      Files.write(file.toPath(), manifest.getBytes(StandardCharsets.UTF_8));
      List<HelixRestNamespace> namespaces = new ArrayList<>();
      HelixRestMain.constructNamespaceFromConfigFile(file.getPath(), namespaces);
      return namespaces;
    } finally {
      file.delete();
    }
  }
}
