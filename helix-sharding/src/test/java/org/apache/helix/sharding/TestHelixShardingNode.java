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

import org.testng.Assert;
import org.testng.annotations.Test;


/**
 * Unit tests for {@link HelixShardingNode} builder and validation.
 *
 * <p>Note: Integration tests requiring a live ZK ensemble are in Phase 4.
 * These tests verify builder validation and configuration without connecting to ZK.</p>
 */
public class TestHelixShardingNode {

  @Test(expectedExceptions = IllegalArgumentException.class)
  public void testBuilderMissingClusterName() {
    new HelixShardingNode.Builder()
        .zkAddress("localhost:2181")
        .onStateTransition((p, f, t) -> {})
        .build();
  }

  @Test(expectedExceptions = IllegalArgumentException.class)
  public void testBuilderMissingZkAddress() {
    new HelixShardingNode.Builder()
        .clusterName("testCluster")
        .onStateTransition((p, f, t) -> {})
        .build();
  }

  @Test(expectedExceptions = IllegalArgumentException.class)
  public void testBuilderMissingStateModel() {
    new HelixShardingNode.Builder()
        .clusterName("testCluster")
        .zkAddress("localhost:2181")
        .build();
  }

  /**
   * Note: Tests that call .build() and verify node properties are integration tests
   * because HelixManagerFactory.getZKHelixManager() connects to ZK during construction.
   * Those tests belong in Phase 4 (Integration & E2E tests with a live ZK ensemble).
   * The builder validation tests above are sufficient for unit testing.
   */
  @Test
  public void testBuilderAcceptsAllParameters() {
    // Verify the builder accepts all parameters without throwing
    // (We don't call .build() because that requires ZK connectivity)
    HelixShardingNode.Builder builder = new HelixShardingNode.Builder()
        .clusterName("testCluster")
        .zkAddress("localhost:2181")
        .instanceName("myInstance")
        .stateModelName("LeaderStandby")
        .leaderState("LEADER")
        .alwaysAnnouncePartitionZero(true)
        .onStateTransition((p, f, t) -> {});

    Assert.assertNotNull(builder);
  }
}
