package org.apache.helix.controller.rebalancer.waged;

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

import java.util.Optional;

import org.apache.helix.HelixRebalanceException;
import org.apache.helix.controller.rebalancer.waged.constraints.MockRebalanceAlgorithm;
import org.testng.Assert;
import org.testng.annotations.Test;

public class TestWagedRebalanceStatus {
  @Test
  public void testServingFailureAndRecovery_areExposedToConvergenceTracker() {
    WagedRebalancer rebalancer =
        new WagedRebalancer(null, new MockRebalanceAlgorithm(), Optional.empty());
    try {
      HelixRebalanceException failure =
          new HelixRebalanceException("capacity exhausted",
              HelixRebalanceException.Type.FAILED_TO_CALCULATE,
              HelixRebalanceException.FailureCategory.CAPACITY_DEFICIT);

      rebalancer.reportAsyncFailure(failure);
      WagedRebalanceStatus failed = rebalancer.getConvergenceStatus();

      Assert.assertTrue(failed.isServingComputationFailed());
      Assert.assertEquals(failed.getServingFailureCategory(),
          HelixRebalanceException.FailureCategory.CAPACITY_DEFICIT);

      rebalancer.reportPartialRebalanceSuccess();
      WagedRebalanceStatus recovered = rebalancer.getConvergenceStatus();

      Assert.assertFalse(recovered.isServingComputationFailed());
      Assert.assertNull(recovered.getServingFailureCategory());
    } finally {
      rebalancer.close();
    }
  }

  @Test
  public void testBaselineFailureAndRecovery_areExposedAsOptimizerHealth() {
    WagedRebalancer rebalancer =
        new WagedRebalancer(null, new MockRebalanceAlgorithm(), Optional.empty());
    try {
      rebalancer.reportBaselineComputeStatus(false);
      Assert.assertTrue(rebalancer.getConvergenceStatus().isBaselineComputationFailed());

      rebalancer.reportBaselineComputeStatus(true);
      Assert.assertFalse(rebalancer.getConvergenceStatus().isBaselineComputationFailed());
    } finally {
      rebalancer.close();
    }
  }
}
