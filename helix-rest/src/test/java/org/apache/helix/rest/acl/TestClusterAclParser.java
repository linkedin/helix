package org.apache.helix.rest.acl;

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
import java.util.Collections;
import java.util.List;

import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.data.ACL;
import org.apache.zookeeper.data.Id;
import org.testng.Assert;
import org.testng.annotations.Test;

public class TestClusterAclParser {

  @Test
  public void testNullAndEmptyInputYieldEmptyAcl() {
    Assert.assertTrue(ClusterAclParser.parse(null).isEmpty());
    Assert.assertTrue(ClusterAclParser.parse(Collections.<String>emptyList()).isEmpty());
  }

  @Test
  public void testParsePermissions() {
    Assert.assertEquals(perms("world:anyone:r"), ZooDefs.Perms.READ);
    Assert.assertEquals(perms("world:anyone:cdrwa"), ZooDefs.Perms.ALL);
    Assert.assertEquals(perms("world:anyone:ALL"), ZooDefs.Perms.ALL);
    Assert.assertEquals(perms("world:anyone:CRW"),
        ZooDefs.Perms.CREATE | ZooDefs.Perms.READ | ZooDefs.Perms.WRITE);
    Assert.assertEquals(perms("world:anyone:da"), ZooDefs.Perms.DELETE | ZooDefs.Perms.ADMIN);
  }

  @Test
  public void testIdsWithCommasAndColonsArePreserved() {
    String dn = "CN=my-service,OU=my-team,O=my-org,C=US";
    List<ACL> acl = ClusterAclParser.parse(Arrays.asList("x509:" + dn + ":cdrwa",
        "digest:user:base64hash=:r", "world:anyone:r"));
    Assert.assertEquals(acl, Arrays.asList(new ACL(ZooDefs.Perms.ALL, new Id("x509", dn)),
        new ACL(ZooDefs.Perms.READ, new Id("digest", "user:base64hash=")),
        new ACL(ZooDefs.Perms.READ, new Id("world", "anyone"))));
  }

  @Test
  public void testParsedAclIsUnmodifiable() {
    List<ACL> acl = ClusterAclParser.parse(Collections.singletonList("world:anyone:r"));
    try {
      acl.add(new ACL(ZooDefs.Perms.ALL, new Id("world", "anyone")));
      Assert.fail("Expected the parsed ACL to be unmodifiable");
    } catch (UnsupportedOperationException expected) {
      // expected
    }
  }

  @Test
  public void testAuthSchemeAllowsEmptyId() {
    List<ACL> acl = ClusterAclParser.parse(Collections.singletonList("auth::cdrwa"));
    Assert.assertEquals(acl, Collections.singletonList(new ACL(ZooDefs.Perms.ALL, new Id("auth", ""))));
  }

  @Test
  public void testEmptyIdIsRejectedForNonAuthSchemes() {
    for (String entry : new String[] {"auth::", "auth:", "world::r", "ip::r", "x509::cdrw"}) {
      try {
        ClusterAclParser.parse(Collections.singletonList(entry));
        Assert.fail("Expected \"" + entry + "\" to be rejected");
      } catch (IllegalArgumentException expected) {
        Assert.assertTrue(expected.getMessage().contains(ClusterAclParser.ACL_FORMAT_HINT),
            "Expected the format hint in: " + expected.getMessage());
      }
    }
  }

  @Test
  public void testMalformedEntriesAreRejected() {
    for (String entry : new String[] {
        null, "", "world", "world:anyone", "world:anyone:", ":anyone:r", "world::r",
        "world:anyone:rx"
    }) {
      try {
        ClusterAclParser.parse(Collections.singletonList(entry));
        Assert.fail("Expected \"" + entry + "\" to be rejected");
      } catch (IllegalArgumentException expected) {
        Assert.assertTrue(expected.getMessage().contains(ClusterAclParser.ACL_FORMAT_HINT),
            "Expected the format hint in: " + expected.getMessage());
      }
    }
  }

  @Test
  public void testMalformedEntryMessageNamesTheEntry() {
    try {
      ClusterAclParser.parse(Collections.singletonList("not-an-acl"));
      Assert.fail("Expected the entry to be rejected");
    } catch (IllegalArgumentException expected) {
      Assert.assertTrue(expected.getMessage().contains("not-an-acl"), expected.getMessage());
    }
  }

  @Test
  public void testWithWorldReadAppendsReadOnlyWorldEntry() {
    List<ACL> input = ClusterAclParser.parse(Collections.singletonList("ip:127.0.0.1:cdrwa"));
    Assert.assertEquals(ClusterAclParser.withWorldRead(input), Arrays.asList(
        new ACL(ZooDefs.Perms.ALL, new Id("ip", "127.0.0.1")),
        new ACL(ZooDefs.Perms.READ, new Id("world", "anyone"))));
    Assert.assertEquals(input.size(), 1, "input must not be modified");
  }

  @Test
  public void testWithWorldReadMergesIntoExistingWorldEntry() {
    List<ACL> input = ClusterAclParser.parse(Arrays.asList("world:anyone:cw", "digest:u:h:cdrwa"));
    Assert.assertEquals(ClusterAclParser.withWorldRead(input), Arrays.asList(
        new ACL(ZooDefs.Perms.CREATE | ZooDefs.Perms.WRITE | ZooDefs.Perms.READ,
            new Id("world", "anyone")),
        new ACL(ZooDefs.Perms.ALL, new Id("digest", "u:h"))));
  }

  @Test
  public void testWithWorldReadResultIsUnmodifiable() {
    try {
      ClusterAclParser.withWorldRead(Collections.<ACL>emptyList())
          .add(new ACL(ZooDefs.Perms.ALL, new Id("world", "anyone")));
      Assert.fail("Expected the result to be unmodifiable");
    } catch (UnsupportedOperationException expected) {
      // expected
    }
  }

  private static int perms(String entry) {
    return ClusterAclParser.parse(Collections.singletonList(entry)).get(0).getPerms();
  }
}
