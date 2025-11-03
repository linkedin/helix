package org.apache.helix;

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

import org.testng.Assert;
import org.testng.AssertJUnit;
import org.testng.annotations.Test;

public class TestPropertyPathBuilder {
  @Test
  public void testGetPath() {
    String actual;
    actual = PropertyPathBuilder.idealState("test_cluster");
    AssertJUnit.assertEquals(actual, "/test_cluster/IDEALSTATES");
    actual = PropertyPathBuilder.idealState("test_cluster", "resource");
    AssertJUnit.assertEquals(actual, "/test_cluster/IDEALSTATES/resource");

    actual = PropertyPathBuilder.instance("test_cluster", "instanceName1");
    AssertJUnit.assertEquals(actual, "/test_cluster/INSTANCES/instanceName1");

    actual = PropertyPathBuilder.instanceCurrentState("test_cluster", "instanceName1");
    AssertJUnit.assertEquals(actual, "/test_cluster/INSTANCES/instanceName1/CURRENTSTATES");
    actual = PropertyPathBuilder.instanceCurrentState("test_cluster", "instanceName1", "sessionId");
    AssertJUnit.assertEquals(actual, "/test_cluster/INSTANCES/instanceName1/CURRENTSTATES/sessionId");

    actual = PropertyPathBuilder.instanceTaskCurrentState("test_cluster", "instanceName1");
    AssertJUnit.assertEquals(actual, "/test_cluster/INSTANCES/instanceName1/TASKCURRENTSTATES");
    actual = PropertyPathBuilder.instanceTaskCurrentState("test_cluster", "instanceName1", "sessionId");
    AssertJUnit.assertEquals(actual, "/test_cluster/INSTANCES/instanceName1/TASKCURRENTSTATES/sessionId");

    actual = PropertyPathBuilder.instanceCustomizedState("test_cluster", "instanceName1");
    AssertJUnit.assertEquals(actual, "/test_cluster/INSTANCES/instanceName1/CUSTOMIZEDSTATES");
    actual = PropertyPathBuilder.instanceCustomizedState("test_cluster", "instanceName1", "customizedState1");
    AssertJUnit.assertEquals(actual, "/test_cluster/INSTANCES/instanceName1/CUSTOMIZEDSTATES/customizedState1");

    actual = PropertyPathBuilder.controller("test_cluster");
    AssertJUnit.assertEquals(actual, "/test_cluster/CONTROLLER");
    actual = PropertyPathBuilder.controllerMessage("test_cluster");
    AssertJUnit.assertEquals(actual, "/test_cluster/CONTROLLER/MESSAGES");

    actual = PropertyPathBuilder.clusterStatus("test_cluster");
    Assert.assertEquals(actual, "/test_cluster/STATUS/CLUSTER/test_cluster");
  }

  /**
   * Test the core getPath() method with the optimized PathTemplate implementation.
   * Tests paths with varying numbers of parameters (0-4 keys).
   */
  @Test
  public void testGetPathWithPropertyType() {
    String actual;

    actual =
        PropertyPathBuilder.getPath(PropertyType.CURRENTSTATES, "MyCluster", "instance1", "session123", "resource1");
    Assert.assertEquals(actual, "/MyCluster/INSTANCES/instance1/CURRENTSTATES/session123/resource1");

    actual =
        PropertyPathBuilder.getPath(PropertyType.CURRENTSTATES, "MyCluster", "instance1", "session123", "resource1",
            "bucket1");
    Assert.assertEquals(actual, "/MyCluster/INSTANCES/instance1/CURRENTSTATES/session123/resource1/bucket1");

    actual =
        PropertyPathBuilder.getPath(PropertyType.CUSTOMIZEDSTATES, "MyCluster", "instance1", "MyState", "MyResource");
    Assert.assertEquals(actual, "/MyCluster/INSTANCES/instance1/CUSTOMIZEDSTATES/MyState/MyResource");

    actual = PropertyPathBuilder.getPath(PropertyType.CUSTOMIZEDVIEW, "MyCluster");
    Assert.assertEquals(actual, "/MyCluster/CUSTOMIZEDVIEW");

    actual = PropertyPathBuilder.getPath(PropertyType.CUSTOMIZEDVIEW, "MyCluster", "customType");
    Assert.assertEquals(actual, "/MyCluster/CUSTOMIZEDVIEW/customType");

    actual = PropertyPathBuilder.getPath(PropertyType.CUSTOMIZEDVIEW, "MyCluster", "customType", "resource1");
    Assert.assertEquals(actual, "/MyCluster/CUSTOMIZEDVIEW/customType/resource1");

    // Test that getPath() works correctly for basic cases
    actual = PropertyPathBuilder.getPath(PropertyType.LIVEINSTANCES, "MyCluster", "node1");
    Assert.assertEquals(actual, "/MyCluster/LIVEINSTANCES/node1");

    actual = PropertyPathBuilder.getPath(PropertyType.IDEALSTATES, "MyCluster", "MyResource");
    Assert.assertEquals(actual, "/MyCluster/IDEALSTATES/MyResource");

    actual =
        PropertyPathBuilder.getPath(PropertyType.STATUSUPDATES, "MyCluster", "inst1", "sess1", "subpath", "record1");
    Assert.assertEquals(actual, "/MyCluster/INSTANCES/inst1/STATUSUPDATES/sess1/subpath/record1");

    actual = PropertyPathBuilder.getPath(PropertyType.ERRORS, "MyCluster", "inst1", "sess1", "subpath", "record1");
    Assert.assertEquals(actual, "/MyCluster/INSTANCES/inst1/ERRORS/sess1/subpath/record1");

    actual = PropertyPathBuilder.getPath(PropertyType.TASKCURRENTSTATES, "MyCluster", "inst1", "sess1", "resource1",
        "bucket1");
    Assert.assertEquals(actual, "/MyCluster/INSTANCES/inst1/TASKCURRENTSTATES/sess1/resource1/bucket1");

    actual = PropertyPathBuilder.getPath(PropertyType.CONFIGS, "MyCluster", "PARTICIPANT", "instance1");
    Assert.assertEquals(actual, "/MyCluster/CONFIGS/PARTICIPANT/instance1");
  }

  /**
   * Test edge cases and special scenarios
   */
  @Test
  public void testGetPathEdgeCases() {
    String actual;

    // Test with null clusterName - should return null and log warning
    actual = PropertyPathBuilder.getPath(PropertyType.IDEALSTATES, null);
    Assert.assertNull(actual);

    // Test with null keys array - should work (treated as empty array)
    actual = PropertyPathBuilder.getPath(PropertyType.LIVEINSTANCES, "TestCluster", (String[]) null);
    Assert.assertEquals(actual, "/TestCluster/LIVEINSTANCES");

    // Test with cluster name containing special characters
    actual = PropertyPathBuilder.getPath(PropertyType.IDEALSTATES, "test-cluster_123");
    Assert.assertEquals(actual, "/test-cluster_123/IDEALSTATES");

    // Test with resource name containing special characters
    actual = PropertyPathBuilder.getPath(PropertyType.IDEALSTATES, "cluster", "resource-name_123.test");
    Assert.assertEquals(actual, "/cluster/IDEALSTATES/resource-name_123.test");
  }
}
