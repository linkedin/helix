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
import org.testng.annotations.Test;


public class TestHelixRebalanceException {

  @Test
  public void testLegacyTwoArgConstructorPreservesMessageAndDefaultsToUnknownCategory() {
    HelixRebalanceException ex = new HelixRebalanceException("boom",
        HelixRebalanceException.Type.FAILED_TO_CALCULATE);
    // Backward-compat: when no category is given, the message must match the historical format
    // so existing log scrapers and tests don't break.
    Assert.assertEquals(ex.getMessage(), "boom Failure Type: FAILED_TO_CALCULATE");
    Assert.assertEquals(ex.getFailureType(), HelixRebalanceException.Type.FAILED_TO_CALCULATE);
    Assert.assertEquals(ex.getFailureCategory(), HelixRebalanceException.FailureCategory.UNKNOWN);
    Assert.assertFalse(ex.isCustomerActionable());
  }

  @Test
  public void testLegacyThreeArgConstructorWithCausePreservesMessageAndDefaults() {
    Throwable cause = new RuntimeException("root");
    HelixRebalanceException ex = new HelixRebalanceException("boom",
        HelixRebalanceException.Type.INVALID_CLUSTER_STATUS, cause);
    Assert.assertEquals(ex.getMessage(), "boom Failure Type: INVALID_CLUSTER_STATUS");
    Assert.assertSame(ex.getCause(), cause);
    Assert.assertEquals(ex.getFailureCategory(), HelixRebalanceException.FailureCategory.UNKNOWN);
  }

  @Test
  public void testNewThreeArgConstructorAppendsCategoryToMessage() {
    HelixRebalanceException ex = new HelixRebalanceException("not enough storage",
        HelixRebalanceException.Type.FAILED_TO_CALCULATE,
        HelixRebalanceException.FailureCategory.CAPACITY_DEFICIT);
    Assert.assertEquals(ex.getMessage(),
        "not enough storage Failure Type: FAILED_TO_CALCULATE Category: CAPACITY_DEFICIT");
    Assert.assertEquals(ex.getFailureCategory(),
        HelixRebalanceException.FailureCategory.CAPACITY_DEFICIT);
    Assert.assertTrue(ex.isCustomerActionable());
  }

  @Test
  public void testNewFourArgConstructorWithCauseAppendsCategory() {
    Throwable cause = new IllegalStateException("zk down");
    HelixRebalanceException ex = new HelixRebalanceException("read failed",
        HelixRebalanceException.Type.INVALID_REBALANCER_STATUS,
        HelixRebalanceException.FailureCategory.METADATA_STORE_IO, cause);
    Assert.assertEquals(ex.getMessage(),
        "read failed Failure Type: INVALID_REBALANCER_STATUS Category: METADATA_STORE_IO");
    Assert.assertSame(ex.getCause(), cause);
    Assert.assertEquals(ex.getFailureCategory(),
        HelixRebalanceException.FailureCategory.METADATA_STORE_IO);
    Assert.assertFalse(ex.isCustomerActionable());
  }

  @Test
  public void testCustomerActionableCategoriesAreTaggedCorrectly() {
    Assert.assertTrue(HelixRebalanceException.FailureCategory.CAPACITY_DEFICIT.isCustomerActionable());
    Assert.assertTrue(HelixRebalanceException.FailureCategory.NO_CANDIDATE_NODE.isCustomerActionable());
    Assert.assertTrue(
        HelixRebalanceException.FailureCategory.INVALID_RESOURCE_CONFIG.isCustomerActionable());
    Assert.assertTrue(
        HelixRebalanceException.FailureCategory.INVALID_CLUSTER_CONFIG.isCustomerActionable());
  }

  @Test
  public void testInternalCategoriesAreTaggedCorrectly() {
    Assert.assertFalse(
        HelixRebalanceException.FailureCategory.METADATA_STORE_IO.isCustomerActionable());
    Assert.assertFalse(
        HelixRebalanceException.FailureCategory.ALGORITHM_INTERNAL.isCustomerActionable());
    Assert.assertFalse(
        HelixRebalanceException.FailureCategory.ASYNC_EXECUTION.isCustomerActionable());
    Assert.assertFalse(HelixRebalanceException.FailureCategory.UNKNOWN.isCustomerActionable());
  }
}
