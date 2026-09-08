package org.apache.helix.integration.rebalancer.DelayedAutoRebalancer;

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

import org.apache.helix.integration.manager.MockParticipantManager;
import org.apache.helix.model.InstanceConfig;
import org.apache.helix.tools.ClusterVerifiers.ZkHelixClusterVerifier;
import org.testng.Assert;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import static org.mockito.Mockito.doCallRealMethod;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockingDetails;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.mockito.Mockito.withSettings;

public class TestDelayedAutoRebalanceFixture {
  @DataProvider
  public Object[][] validTimestamps() {
    return new Object[][] {{1000L}, {1100L}, {1500L}, {1600L}};
  }

  @Test(dataProvider = "validTimestamps")
  public void testTimestampWithinOperationInterval(long timestamp) {
    TestDelayedAutoRebalance.validateInstanceEnabled(instanceConfig(true, timestamp), true,
        1000L, 1600L);
  }

  @DataProvider
  public Object[][] invalidEnabledStates() {
    return new Object[][] {{true, 999L}, {true, 1601L}, {false, 1100L}};
  }

  @Test(dataProvider = "invalidEnabledStates", expectedExceptions = AssertionError.class)
  public void testRejectInvalidEnabledState(boolean enabled, long timestamp) {
    TestDelayedAutoRebalance.validateInstanceEnabled(instanceConfig(enabled, timestamp), true,
        1000L, 1600L);
  }

  @Test
  public void testCleanupAfterSkippedSetup() throws Exception {
    TestDelayedAutoRebalance fixture = newFixture();
    fixture.afterClass();
    verifyClusterDeletion(fixture);
  }

  @Test
  public void testCleanupAfterPartialSetup() throws Exception {
    TestDelayedAutoRebalance fixture = newFixture();
    MockParticipantManager participant = mock(MockParticipantManager.class);
    ZkHelixClusterVerifier verifier = mock(ZkHelixClusterVerifier.class);
    fixture._participants.add(participant);
    fixture._clusterVerifier = verifier;

    fixture.afterClass();

    verify(verifier).close();
    verify(participant).syncStop();
    verifyClusterDeletion(fixture);
  }

  private InstanceConfig instanceConfig(boolean enabled, long timestamp) {
    InstanceConfig config = mock(InstanceConfig.class);
    when(config.getInstanceEnabled()).thenReturn(enabled);
    when(config.getInstanceEnabledTime()).thenReturn(timestamp);
    return config;
  }

  private TestDelayedAutoRebalance newFixture() throws Exception {
    // Concrete subclasses inherit @Test methods and are discovered by the package-based CI suite.
    TestDelayedAutoRebalance fixture =
        mock(TestDelayedAutoRebalance.class, withSettings().useConstructor());
    doCallRealMethod().when(fixture).afterClass();
    return fixture;
  }

  private void verifyClusterDeletion(TestDelayedAutoRebalance fixture) {
    Assert.assertTrue(mockingDetails(fixture).getInvocations().stream().anyMatch(invocation ->
        invocation.getMethod().getName().equals("deleteCluster")
            && Arrays.equals(invocation.getArguments(), new Object[] {fixture.CLUSTER_NAME})),
        "Cleanup must delete the fixture's cluster");
  }
}
