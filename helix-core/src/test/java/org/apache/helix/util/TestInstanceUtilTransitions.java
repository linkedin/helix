package org.apache.helix.util;

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

import org.apache.helix.BaseDataAccessor;
import org.apache.helix.HelixException;
import org.apache.helix.constants.InstanceConstants;
import org.apache.helix.model.InstanceConfig;
import org.apache.helix.zookeeper.datamodel.ZNRecord;
import org.testng.Assert;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import static org.mockito.Mockito.mock;

/**
 * Comprehensive test class for InstanceUtil instance operation transition validation.
 * Tests all possible transitions between instance operation states to ensure proper
 * validation logic and maintain backward compatibility.
 */
public class TestInstanceUtilTransitions {
  private static final String TEST_CLUSTER = "testCluster";
  private static final String TEST_INSTANCE = "instance0";

  /**
   * Data provider for all valid instance operation transitions that use ALWAYS_ALLOWED validator.
   * These transitions work without any additional validation logic.
   */
  @DataProvider
  public Object[][] alwaysAllowedTransitions() {
    return new Object[][] {
        // From ENABLE state - all transitions are ALWAYS_ALLOWED
        {InstanceConstants.InstanceOperation.ENABLE, InstanceConstants.InstanceOperation.ENABLE},
        {InstanceConstants.InstanceOperation.ENABLE, InstanceConstants.InstanceOperation.DISABLE},
        {InstanceConstants.InstanceOperation.ENABLE, InstanceConstants.InstanceOperation.EVACUATE},
        {InstanceConstants.InstanceOperation.ENABLE, InstanceConstants.InstanceOperation.UNKNOWN},

        // From DISABLE state - all transitions are ALWAYS_ALLOWED
        {InstanceConstants.InstanceOperation.DISABLE, InstanceConstants.InstanceOperation.DISABLE},
        {InstanceConstants.InstanceOperation.DISABLE, InstanceConstants.InstanceOperation.ENABLE},
        {InstanceConstants.InstanceOperation.DISABLE, InstanceConstants.InstanceOperation.EVACUATE},
        {InstanceConstants.InstanceOperation.DISABLE, InstanceConstants.InstanceOperation.UNKNOWN},

        // From SWAP_IN state - limited transitions that are ALWAYS_ALLOWED
        {InstanceConstants.InstanceOperation.SWAP_IN, InstanceConstants.InstanceOperation.SWAP_IN},
        {InstanceConstants.InstanceOperation.SWAP_IN, InstanceConstants.InstanceOperation.UNKNOWN},

        // From EVACUATE state - self and to UNKNOWN are ALWAYS_ALLOWED
        {InstanceConstants.InstanceOperation.EVACUATE, InstanceConstants.InstanceOperation.EVACUATE},
        {InstanceConstants.InstanceOperation.EVACUATE, InstanceConstants.InstanceOperation.UNKNOWN},

        // From UNKNOWN state - self and to EVACUATE are ALWAYS_ALLOWED
        {InstanceConstants.InstanceOperation.UNKNOWN, InstanceConstants.InstanceOperation.UNKNOWN},
        {InstanceConstants.InstanceOperation.UNKNOWN, InstanceConstants.InstanceOperation.EVACUATE} // This is the new transition
    };
  }

  /**
   * Data provider for invalid instance operation transitions.
   * These transitions should be rejected by the validation logic.
   */
  @DataProvider
  public Object[][] invalidTransitions() {
    return new Object[][] {
        // From SWAP_IN state - only SWAP_IN and UNKNOWN are allowed
        {InstanceConstants.InstanceOperation.SWAP_IN, InstanceConstants.InstanceOperation.ENABLE},
        {InstanceConstants.InstanceOperation.SWAP_IN, InstanceConstants.InstanceOperation.DISABLE},
        {InstanceConstants.InstanceOperation.SWAP_IN, InstanceConstants.InstanceOperation.EVACUATE}
    };
  }

  /**
   * Data provider for self-transitions (same state to same state).
   * All states should allow transitions to themselves.
   */
  @DataProvider
  public Object[][] selfTransitions() {
    return new Object[][] {
        {InstanceConstants.InstanceOperation.ENABLE},
        {InstanceConstants.InstanceOperation.DISABLE},
        {InstanceConstants.InstanceOperation.SWAP_IN},
        {InstanceConstants.InstanceOperation.EVACUATE},
        {InstanceConstants.InstanceOperation.UNKNOWN}
    };
  }

  @Test(dataProvider = "alwaysAllowedTransitions")
  public void testAlwaysAllowedTransitions(
      InstanceConstants.InstanceOperation fromState,
      InstanceConstants.InstanceOperation toState) {

    @SuppressWarnings("unchecked")
    BaseDataAccessor<ZNRecord> mockBaseDataAccessor = mock(BaseDataAccessor.class);

    InstanceConfig instanceConfig = new InstanceConfig(TEST_INSTANCE);
    instanceConfig.setInstanceOperation(fromState);

    // These transitions should work without any exception
    InstanceUtil.validateInstanceOperationTransition(
        mockBaseDataAccessor, TEST_CLUSTER, instanceConfig, fromState, toState);
  }

  @Test(dataProvider = "invalidTransitions")
  public void testInvalidTransitions(
      InstanceConstants.InstanceOperation fromState,
      InstanceConstants.InstanceOperation toState) {

    @SuppressWarnings("unchecked")
    BaseDataAccessor<ZNRecord> mockBaseDataAccessor = mock(BaseDataAccessor.class);

    InstanceConfig instanceConfig = new InstanceConfig(TEST_INSTANCE);
    instanceConfig.setInstanceOperation(fromState);

    // Invalid transitions should throw HelixException
    try {
      InstanceUtil.validateInstanceOperationTransition(
          mockBaseDataAccessor, TEST_CLUSTER, instanceConfig, fromState, toState);
      Assert.fail(String.format("Expected invalid transition from %s to %s to fail", fromState, toState));
    } catch (HelixException e) {
      Assert.assertTrue(e.getMessage().contains("Invalid instance operation transition"),
          "Exception should indicate invalid transition: " + e.getMessage());
    }
  }

  @Test(dataProvider = "selfTransitions")
  public void testSelfTransitions(InstanceConstants.InstanceOperation state) {
    @SuppressWarnings("unchecked")
    BaseDataAccessor<ZNRecord> mockBaseDataAccessor = mock(BaseDataAccessor.class);

    InstanceConfig instanceConfig = new InstanceConfig(TEST_INSTANCE);
    instanceConfig.setInstanceOperation(state);

    // Self transitions should always be allowed
    InstanceUtil.validateInstanceOperationTransition(
        mockBaseDataAccessor, TEST_CLUSTER, instanceConfig, state, state);
  }

  @Test
  public void testUnknownToEvacuateTransition() {
    @SuppressWarnings("unchecked")
    BaseDataAccessor<ZNRecord> mockBaseDataAccessor = mock(BaseDataAccessor.class);

    InstanceConfig instanceConfig = new InstanceConfig(TEST_INSTANCE);
    instanceConfig.setInstanceOperation(InstanceConstants.InstanceOperation.UNKNOWN);

    // This should succeed without any exception
    InstanceUtil.validateInstanceOperationTransition(
        mockBaseDataAccessor, TEST_CLUSTER, instanceConfig,
        InstanceConstants.InstanceOperation.UNKNOWN,
        InstanceConstants.InstanceOperation.EVACUATE);
  }

  @Test
  public void testEvacuateToUnknownTransition() {
    @SuppressWarnings("unchecked")
    BaseDataAccessor<ZNRecord> mockBaseDataAccessor = mock(BaseDataAccessor.class);

    InstanceConfig instanceConfig = new InstanceConfig(TEST_INSTANCE);
    instanceConfig.setInstanceOperation(InstanceConstants.InstanceOperation.EVACUATE);

    // This should succeed without any exception
    InstanceUtil.validateInstanceOperationTransition(
        mockBaseDataAccessor, TEST_CLUSTER, instanceConfig,
        InstanceConstants.InstanceOperation.EVACUATE,
        InstanceConstants.InstanceOperation.UNKNOWN);
  }

  @Test
  public void testAllEnableDisableTransitions() {
    // Test that ENABLE and DISABLE states allow all transitions
    @SuppressWarnings("unchecked")
    BaseDataAccessor<ZNRecord> mockBaseDataAccessor = mock(BaseDataAccessor.class);

    InstanceConstants.InstanceOperation[] allTargets = {
        InstanceConstants.InstanceOperation.ENABLE,
        InstanceConstants.InstanceOperation.DISABLE,
        InstanceConstants.InstanceOperation.EVACUATE,
        InstanceConstants.InstanceOperation.UNKNOWN
    };

    InstanceConstants.InstanceOperation[] enableDisableStates = {
        InstanceConstants.InstanceOperation.ENABLE,
        InstanceConstants.InstanceOperation.DISABLE
    };

    for (InstanceConstants.InstanceOperation fromState : enableDisableStates) {
      InstanceConfig instanceConfig = new InstanceConfig(TEST_INSTANCE);
      instanceConfig.setInstanceOperation(fromState);

      for (InstanceConstants.InstanceOperation toState : allTargets) {
        InstanceUtil.validateInstanceOperationTransition(
            mockBaseDataAccessor, TEST_CLUSTER, instanceConfig, fromState, toState);
      }
    }
  }

  @Test
  public void testSwapInLimitedTransitions() {
    // Test that SWAP_IN only allows transitions to SWAP_IN and UNKNOWN
    @SuppressWarnings("unchecked")
    BaseDataAccessor<ZNRecord> mockBaseDataAccessor = mock(BaseDataAccessor.class);

    InstanceConfig instanceConfig = new InstanceConfig(TEST_INSTANCE);
    instanceConfig.setInstanceOperation(InstanceConstants.InstanceOperation.SWAP_IN);

    // Valid transitions from SWAP_IN
    InstanceConstants.InstanceOperation[] validTargets = {
        InstanceConstants.InstanceOperation.SWAP_IN,
        InstanceConstants.InstanceOperation.UNKNOWN
    };

    for (InstanceConstants.InstanceOperation target : validTargets) {
      InstanceUtil.validateInstanceOperationTransition(
          mockBaseDataAccessor, TEST_CLUSTER, instanceConfig,
          InstanceConstants.InstanceOperation.SWAP_IN, target);
    }

    // Invalid transitions from SWAP_IN
    InstanceConstants.InstanceOperation[] invalidTargets = {
        InstanceConstants.InstanceOperation.ENABLE,
        InstanceConstants.InstanceOperation.DISABLE,
        InstanceConstants.InstanceOperation.EVACUATE
    };

    for (InstanceConstants.InstanceOperation target : invalidTargets) {
      try {
        InstanceUtil.validateInstanceOperationTransition(
            mockBaseDataAccessor, TEST_CLUSTER, instanceConfig,
            InstanceConstants.InstanceOperation.SWAP_IN, target);
        Assert.fail("Should not allow transition from SWAP_IN to " + target);
      } catch (HelixException e) {
        Assert.assertTrue(e.getMessage().contains("Invalid instance operation transition"));
      }
    }
  }
}