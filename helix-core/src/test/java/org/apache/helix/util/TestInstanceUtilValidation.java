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

import org.apache.helix.ConfigAccessor;
import org.apache.helix.HelixException;
import org.apache.helix.constants.InstanceConstants;
import org.apache.helix.model.InstanceConfig;
import org.testng.Assert;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

/**
 * Tests for enriched error messages in InstanceUtil.validateInstanceOperationTransition.
 * These test the "invalid transition" path which does not require ZK access.
 */
public class TestInstanceUtilValidation {

  private static final String TEST_CLUSTER = "testCluster";
  private static final String TEST_INSTANCE = "testInstance";

  @DataProvider
  Object[][] invalidTransitions() {
    return new Object[][] {
        // {currentOperation, targetOperation}
        {InstanceConstants.InstanceOperation.ENABLE, InstanceConstants.InstanceOperation.SWAP_IN},
        {InstanceConstants.InstanceOperation.DISABLE, InstanceConstants.InstanceOperation.SWAP_IN},
        {InstanceConstants.InstanceOperation.SWAP_IN, InstanceConstants.InstanceOperation.ENABLE},
        {InstanceConstants.InstanceOperation.SWAP_IN, InstanceConstants.InstanceOperation.DISABLE},
        {InstanceConstants.InstanceOperation.SWAP_IN, InstanceConstants.InstanceOperation.EVACUATE},
        {InstanceConstants.InstanceOperation.EVACUATE, InstanceConstants.InstanceOperation.SWAP_IN},
    };
  }

  @Test(dataProvider = "invalidTransitions")
  public void testInvalidTransitionIncludesValidTransitions(
      InstanceConstants.InstanceOperation currentOp,
      InstanceConstants.InstanceOperation targetOp) {
    InstanceConfig instanceConfig = createInstanceConfig(currentOp, "ADMIN", "test reason");

    try {
      InstanceUtil.validateInstanceOperationTransition((ConfigAccessor) null, TEST_CLUSTER, instanceConfig,
          currentOp, targetOp);
      Assert.fail("Expected HelixException for invalid transition from " + currentOp + " to " + targetOp);
    } catch (HelixException e) {
      String msg = e.getMessage();
      // Should include instance name
      Assert.assertTrue(msg.contains(TEST_INSTANCE),
          "Error should contain instance name. Got: " + msg);
      // Should include current and target operations
      Assert.assertTrue(msg.contains(currentOp.name()),
          "Error should contain current operation. Got: " + msg);
      Assert.assertTrue(msg.contains(targetOp.name()),
          "Error should contain target operation. Got: " + msg);
      // Should include valid transitions
      Assert.assertTrue(msg.contains("Valid transitions from"),
          "Error should list valid transitions. Got: " + msg);
      // Should include source and reason
      Assert.assertTrue(msg.contains("ADMIN"),
          "Error should contain operation source. Got: " + msg);
      Assert.assertTrue(msg.contains("test reason"),
          "Error should contain operation reason. Got: " + msg);
    }
  }

  @DataProvider
  Object[][] alwaysAllowedTransitions() {
    return new Object[][] {
        // Transitions that use ALWAYS_ALLOWED validator (no ZK access needed)
        {InstanceConstants.InstanceOperation.ENABLE, InstanceConstants.InstanceOperation.ENABLE},
        {InstanceConstants.InstanceOperation.ENABLE, InstanceConstants.InstanceOperation.DISABLE},
        {InstanceConstants.InstanceOperation.ENABLE, InstanceConstants.InstanceOperation.EVACUATE},
        {InstanceConstants.InstanceOperation.ENABLE, InstanceConstants.InstanceOperation.UNKNOWN},
        {InstanceConstants.InstanceOperation.DISABLE, InstanceConstants.InstanceOperation.DISABLE},
        {InstanceConstants.InstanceOperation.DISABLE, InstanceConstants.InstanceOperation.ENABLE},
        {InstanceConstants.InstanceOperation.DISABLE, InstanceConstants.InstanceOperation.EVACUATE},
        {InstanceConstants.InstanceOperation.DISABLE, InstanceConstants.InstanceOperation.UNKNOWN},
        {InstanceConstants.InstanceOperation.SWAP_IN, InstanceConstants.InstanceOperation.SWAP_IN},
        {InstanceConstants.InstanceOperation.SWAP_IN, InstanceConstants.InstanceOperation.UNKNOWN},
        {InstanceConstants.InstanceOperation.EVACUATE, InstanceConstants.InstanceOperation.EVACUATE},
        {InstanceConstants.InstanceOperation.EVACUATE, InstanceConstants.InstanceOperation.UNKNOWN},
        {InstanceConstants.InstanceOperation.UNKNOWN, InstanceConstants.InstanceOperation.UNKNOWN},
    };
  }

  @Test(dataProvider = "alwaysAllowedTransitions")
  public void testAlwaysAllowedTransitionsPass(
      InstanceConstants.InstanceOperation currentOp,
      InstanceConstants.InstanceOperation targetOp) {
    InstanceConfig instanceConfig = createInstanceConfig(currentOp, "ADMIN", "test");

    // Should not throw — these transitions use ALWAYS_ALLOWED and need no ZK
    InstanceUtil.validateInstanceOperationTransition((ConfigAccessor) null, TEST_CLUSTER, instanceConfig,
        currentOp, targetOp);
  }

  @Test
  public void testInvalidTransitionErrorMessageFormat() {
    InstanceConfig instanceConfig = createInstanceConfig(
        InstanceConstants.InstanceOperation.ENABLE,
        "AUTOMATION", "scheduled maintenance");

    try {
      InstanceUtil.validateInstanceOperationTransition((ConfigAccessor) null, TEST_CLUSTER, instanceConfig,
          InstanceConstants.InstanceOperation.ENABLE,
          InstanceConstants.InstanceOperation.SWAP_IN);
      Assert.fail("Expected HelixException");
    } catch (HelixException e) {
      String msg = e.getMessage();
      // Verify all enriched fields are present
      Assert.assertTrue(msg.contains("Invalid instance operation transition from ENABLE to SWAP_IN"),
          "Should contain transition description. Got: " + msg);
      Assert.assertTrue(msg.contains("for instance " + TEST_INSTANCE),
          "Should contain instance name. Got: " + msg);
      Assert.assertTrue(msg.contains("Valid transitions from ENABLE:"),
          "Should list valid transitions. Got: " + msg);
      Assert.assertTrue(msg.contains("DISABLE"),
          "Valid transitions should include DISABLE. Got: " + msg);
      Assert.assertTrue(msg.contains("EVACUATE"),
          "Valid transitions should include EVACUATE. Got: " + msg);
      Assert.assertTrue(msg.contains("Current operation source: AUTOMATION"),
          "Should contain operation source. Got: " + msg);
      Assert.assertTrue(msg.contains("reason: scheduled maintenance"),
          "Should contain operation reason. Got: " + msg);
    }
  }

  private InstanceConfig createInstanceConfig(InstanceConstants.InstanceOperation operation,
      String source, String reason) {
    InstanceConfig config = new InstanceConfig(TEST_INSTANCE);
    config.setInstanceOperation(
        new InstanceConfig.InstanceOperation.Builder()
            .setOperation(operation)
            .setSource(InstanceConstants.InstanceOperationSource.valueOf(source))
            .setReason(reason)
            .build());
    return config;
  }
}
