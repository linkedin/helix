package org.apache.helix.sharding;

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

import org.apache.helix.HelixAdmin;
import org.apache.helix.model.IdealState;
import org.apache.helix.model.StateModelDefinition;
import org.testng.Assert;
import org.testng.annotations.Test;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;


public class TestHelixShardingAdmin {

  @Test
  public void testAddClusterDefaultStrategy() {
    HelixAdmin mockAdmin = createMockHelixAdmin();

    try (HelixShardingAdmin admin = new HelixShardingAdmin.Builder()
        .helixAdmin(mockAdmin)
        .build()) {

      admin.addCluster("testCluster", 128, 2);

      // Verify cluster was created
      verify(mockAdmin).addCluster("testCluster", false);

      // Verify state model was added
      verify(mockAdmin).addStateModelDef(eq("testCluster"), eq("LeaderStandby"),
          any(StateModelDefinition.class));

      // Verify resource was added
      verify(mockAdmin).addResource(eq("testCluster"),
          eq(HelixShardingAdmin.DEFAULT_RESOURCE_NAME), eq(128), eq("LeaderStandby"),
          eq("FULL_AUTO"));

      // Verify rebalance was called
      verify(mockAdmin).rebalance(eq("testCluster"),
          eq(HelixShardingAdmin.DEFAULT_RESOURCE_NAME), eq(2));
    }
  }

  @Test
  public void testAddClusterWithStickyStrategy() {
    HelixAdmin mockAdmin = createMockHelixAdmin();

    try (HelixShardingAdmin admin = new HelixShardingAdmin.Builder()
        .helixAdmin(mockAdmin)
        .build()) {

      admin.addCluster("testCluster", 64, 3, ShardingRebalanceStrategy.STICKY);

      // Verify IdealState was updated with strategy
      verify(mockAdmin).setResourceIdealState(eq("testCluster"),
          eq(HelixShardingAdmin.DEFAULT_RESOURCE_NAME), any(IdealState.class));
    }
  }

  @Test
  public void testDropCluster() {
    HelixAdmin mockAdmin = createMockHelixAdmin();

    try (HelixShardingAdmin admin = new HelixShardingAdmin.Builder()
        .helixAdmin(mockAdmin)
        .build()) {

      admin.dropCluster("testCluster");
      verify(mockAdmin).dropCluster("testCluster");
    }
  }

  @Test
  public void testExpandPartitions() {
    HelixAdmin mockAdmin = createMockHelixAdmin();
    IdealState mockIdealState = new IdealState(HelixShardingAdmin.DEFAULT_RESOURCE_NAME);
    mockIdealState.setNumPartitions(64);
    mockIdealState.setReplicas("2");
    when(mockAdmin.getResourceIdealState("testCluster", HelixShardingAdmin.DEFAULT_RESOURCE_NAME))
        .thenReturn(mockIdealState);

    try (HelixShardingAdmin admin = new HelixShardingAdmin.Builder()
        .helixAdmin(mockAdmin)
        .build()) {

      admin.expandPartitions("testCluster", 128);
      verify(mockAdmin).setResourceIdealState(eq("testCluster"),
          eq(HelixShardingAdmin.DEFAULT_RESOURCE_NAME), any(IdealState.class));
      verify(mockAdmin).rebalance(eq("testCluster"),
          eq(HelixShardingAdmin.DEFAULT_RESOURCE_NAME), eq(2));
    }
  }

  @Test(expectedExceptions = IllegalArgumentException.class)
  public void testAddClusterNullName() {
    HelixAdmin mockAdmin = createMockHelixAdmin();
    try (HelixShardingAdmin admin = new HelixShardingAdmin.Builder()
        .helixAdmin(mockAdmin)
        .build()) {
      admin.addCluster(null, 128, 2);
    }
  }

  @Test(expectedExceptions = IllegalArgumentException.class)
  public void testAddClusterZeroPartitions() {
    HelixAdmin mockAdmin = createMockHelixAdmin();
    try (HelixShardingAdmin admin = new HelixShardingAdmin.Builder()
        .helixAdmin(mockAdmin)
        .build()) {
      admin.addCluster("cluster", 0, 2);
    }
  }

  @Test(expectedExceptions = IllegalArgumentException.class)
  public void testBuilderNoAdminNoZk() {
    new HelixShardingAdmin.Builder().build();
  }

  @Test
  public void testGetHelixAdmin() {
    HelixAdmin mockAdmin = createMockHelixAdmin();
    try (HelixShardingAdmin admin = new HelixShardingAdmin.Builder()
        .helixAdmin(mockAdmin)
        .build()) {
      Assert.assertSame(admin.getHelixAdmin(), mockAdmin);
    }
  }

  @Test
  public void testShardingRebalanceStrategyValues() {
    Assert.assertNotNull(ShardingRebalanceStrategy.AUTO.getHelixClassName());
    Assert.assertNotNull(ShardingRebalanceStrategy.STICKY.getHelixClassName());
    Assert.assertNotNull(ShardingRebalanceStrategy.CRUSH.getHelixClassName());
    Assert.assertNotNull(ShardingRebalanceStrategy.CRUSH_ED.getHelixClassName());

    // Verify STICKY maps to our new class
    Assert.assertTrue(ShardingRebalanceStrategy.STICKY.getHelixClassName()
        .contains("StickyRebalanceStrategy"));
  }

  // ─── Helpers ─────────────────────────────────────────────────────────────────

  private HelixAdmin createMockHelixAdmin() {
    HelixAdmin mockAdmin = mock(HelixAdmin.class);
    IdealState mockIdealState = new IdealState(HelixShardingAdmin.DEFAULT_RESOURCE_NAME);
    mockIdealState.setReplicas("2");
    when(mockAdmin.getResourceIdealState(anyString(), anyString())).thenReturn(mockIdealState);
    return mockAdmin;
  }
}
