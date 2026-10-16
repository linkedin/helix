package org.apache.helix.tools;

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

import java.util.Collections;
import java.util.List;

import org.apache.helix.PropertyPathBuilder;
import org.apache.helix.TestHelper;
import org.apache.helix.ZkUnitTestBase;
import org.apache.helix.manager.zk.ZKUtil;
import org.apache.helix.model.BuiltInStateModelDefinitions;
import org.apache.helix.model.StateModelDefinition;
import org.apache.helix.zookeeper.datamodel.ZNRecord;
import org.apache.helix.zookeeper.zkclient.ZkConnection;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.ACL;
import org.apache.zookeeper.data.Id;
import org.apache.zookeeper.data.Stat;
import org.testng.Assert;
import org.testng.annotations.Test;

public class TestClusterSetupWithAcl extends ZkUnitTestBase {
  // Includes the unauthenticated test client, but is distinguishable from the default ACL.
  private static final List<ACL> ACL_WITHOUT_ADMIN = Collections.singletonList(new ACL(
      ZooDefs.Perms.CREATE | ZooDefs.Perms.DELETE | ZooDefs.Perms.READ | ZooDefs.Perms.WRITE,
      new Id("world", "anyone")));

  @Test
  public void testAddClusterWithAcl() throws Exception {
    String clusterName = "TestClusterSetupWithAcl_acl";
    ClusterSetup setup = new ClusterSetup(ZK_ADDR);
    try {
      setup.addCluster(clusterName, false, null, ACL_WITHOUT_ADMIN);

      Assert.assertTrue(ZKUtil.isClusterSetup(clusterName, _gZkClient));
      Assert.assertEquals(getAcl("/" + clusterName), ACL_WITHOUT_ADMIN);
      Assert.assertEquals(getAcl(PropertyPathBuilder.stateModelDef(clusterName)),
          ACL_WITHOUT_ADMIN);
      assertBuiltInStateModelDefsHaveAcl(clusterName, ACL_WITHOUT_ADMIN);
    } finally {
      setup.close();
      TestHelper.dropCluster(clusterName, _gZkClient);
    }
  }

  @Test
  public void testRecreateClusterWithAclAppliesAclToStateModelDefs() throws Exception {
    String clusterName = "TestClusterSetupWithAcl_recreate";
    ClusterSetup setup = new ClusterSetup(ZK_ADDR);
    try {
      setup.addCluster(clusterName, false, null);
      setup.addCluster(clusterName, true, null, ACL_WITHOUT_ADMIN);

      Assert.assertEquals(getAcl("/" + clusterName), ACL_WITHOUT_ADMIN);
      assertBuiltInStateModelDefsHaveAcl(clusterName, ACL_WITHOUT_ADMIN);
    } finally {
      setup.close();
      TestHelper.dropCluster(clusterName, _gZkClient);
    }
  }

  @Test
  public void testAddStateModelDefWithAcl() throws Exception {
    String clusterName = "TestClusterSetupWithAcl_stateModelDef";
    ClusterSetup setup = new ClusterSetup(ZK_ADDR);
    try {
      setup.addCluster(clusterName, false, null);
      StateModelDefinition def = BuiltInStateModelDefinitions.MasterSlave.getStateModelDefinition();
      String path = PropertyPathBuilder.stateModelDef(clusterName) + "/" + def.getId();
      Assert.assertEquals(getAcl(path), ZooDefs.Ids.OPEN_ACL_UNSAFE);

      // An existing definition is left untouched unless recreate is requested
      setup.getClusterManagementTool().addStateModelDef(clusterName, def.getId(), def, false,
          ACL_WITHOUT_ADMIN);
      Assert.assertEquals(getAcl(path), ZooDefs.Ids.OPEN_ACL_UNSAFE);

      setup.getClusterManagementTool().addStateModelDef(clusterName, def.getId(), def, true,
          ACL_WITHOUT_ADMIN);
      Assert.assertEquals(getAcl(path), ACL_WITHOUT_ADMIN);
      Assert.assertEquals(new StateModelDefinition(_gZkClient.<ZNRecord>readData(path)).getId(),
          def.getId());
    } finally {
      setup.close();
      TestHelper.dropCluster(clusterName, _gZkClient);
    }
  }

  @Test
  public void testAddClusterWithoutAclKeepsDefaultAcl() throws Exception {
    String clusterName = "TestClusterSetupWithAcl_default";
    ClusterSetup setup = new ClusterSetup(ZK_ADDR);
    try {
      setup.addCluster(clusterName, false, null);
      Assert.assertEquals(getAcl("/" + clusterName), ZooDefs.Ids.OPEN_ACL_UNSAFE);

      setup.addCluster(clusterName, true, null, null);
      Assert.assertEquals(getAcl("/" + clusterName), ZooDefs.Ids.OPEN_ACL_UNSAFE);
      Assert.assertTrue(ZKUtil.isClusterSetup(clusterName, _gZkClient));
    } finally {
      setup.close();
      TestHelper.dropCluster(clusterName, _gZkClient);
    }
  }

  private static void assertBuiltInStateModelDefsHaveAcl(String clusterName, List<ACL> expected)
      throws Exception {
    List<String> children = _gZkClient.getChildren(PropertyPathBuilder.stateModelDef(clusterName));
    Assert.assertEquals(children.size(), BuiltInStateModelDefinitions.values().length);
    for (BuiltInStateModelDefinitions def : BuiltInStateModelDefinitions.values()) {
      String path = PropertyPathBuilder.stateModelDef(clusterName) + "/"
          + def.getStateModelDefinition().getId();
      Assert.assertEquals(getAcl(path), expected, "unexpected ACL on " + path);
    }
  }

  private static List<ACL> getAcl(String path) throws Exception {
    ZooKeeper zooKeeper = ((ZkConnection) ((org.apache.helix.zookeeper.zkclient.ZkClient) _gZkClient)
        .getConnection()).getZookeeper();
    return zooKeeper.getACL(path, new Stat());
  }
}
