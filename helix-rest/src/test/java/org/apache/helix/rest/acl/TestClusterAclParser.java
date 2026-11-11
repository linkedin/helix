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
  public void testFacadeOnlyAddsWorldReadWhenRequested() {
    List<ACL> parsed = ClusterAclParser.parse(Collections.singletonList("auth::all"));
    ACL owner = new ACL(ZooDefs.Perms.ALL, new Id("auth", ""));
    Assert.assertEquals(parsed, Collections.singletonList(owner));
    Assert.assertEquals(ClusterAclParser.withWorldRead(parsed),
        Arrays.asList(owner, new ACL(ZooDefs.Perms.READ, new Id("world", "anyone"))));
    Assert.assertEquals(parsed, Collections.singletonList(owner));
  }

  @Test
  public void testFacadePreservesValidationErrors() {
    try {
      ClusterAclParser.parse(Collections.singletonList("not-an-acl"));
      Assert.fail("Expected malformed ACL to be rejected");
    } catch (IllegalArgumentException exception) {
      Assert.assertTrue(exception.getMessage().contains(ClusterAclParser.ACL_FORMAT_HINT));
      Assert.assertTrue(exception.getMessage().contains("not-an-acl"));
    }
  }
}
