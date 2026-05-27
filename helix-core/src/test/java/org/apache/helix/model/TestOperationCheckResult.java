package org.apache.helix.model;

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

import org.testng.Assert;
import org.testng.annotations.Test;

public class TestOperationCheckResult {

  @Test
  public void testSuccess() {
    OperationCheckResult result = OperationCheckResult.success();
    Assert.assertTrue(result.isSuccessful());
    Assert.assertTrue(result.getBlockers().isEmpty());
  }

  @Test
  public void testFailedWithSingleBlocker() {
    OperationCheckResult result = OperationCheckResult.failed("instance is offline");
    Assert.assertFalse(result.isSuccessful());
    Assert.assertEquals(result.getBlockers().size(), 1);
    Assert.assertEquals(result.getBlockers().get(0), "instance is offline");
  }

  @Test
  public void testFailedWithMultipleBlockers() {
    List<String> blockers = Arrays.asList("blocker1", "blocker2", "blocker3");
    OperationCheckResult result = OperationCheckResult.failed(blockers);
    Assert.assertFalse(result.isSuccessful());
    Assert.assertEquals(result.getBlockers().size(), 3);
    Assert.assertEquals(result.getBlockers().get(0), "blocker1");
    Assert.assertEquals(result.getBlockers().get(2), "blocker3");
  }

  @Test
  public void testFailedWithEmptyBlockerList() {
    OperationCheckResult result = OperationCheckResult.failed(Collections.emptyList());
    Assert.assertFalse(result.isSuccessful());
    Assert.assertTrue(result.getBlockers().isEmpty());
  }

  @Test(expectedExceptions = UnsupportedOperationException.class)
  public void testBlockersListIsUnmodifiable() {
    OperationCheckResult result = OperationCheckResult.failed("blocker");
    result.getBlockers().add("should not be allowed");
  }

  @Test
  public void testBuilderWithNoBlockers() {
    OperationCheckResult.Builder builder = new OperationCheckResult.Builder();
    Assert.assertFalse(builder.hasBlockers());
    OperationCheckResult result = builder.build();
    Assert.assertTrue(result.isSuccessful());
    Assert.assertTrue(result.getBlockers().isEmpty());
  }

  @Test
  public void testBuilderWithBlockers() {
    OperationCheckResult.Builder builder = new OperationCheckResult.Builder();
    builder.addBlocker("blocker1");
    builder.addBlocker("blocker2");
    Assert.assertTrue(builder.hasBlockers());

    OperationCheckResult result = builder.build();
    Assert.assertFalse(result.isSuccessful());
    Assert.assertEquals(result.getBlockers().size(), 2);
    Assert.assertEquals(result.getBlockers().get(0), "blocker1");
    Assert.assertEquals(result.getBlockers().get(1), "blocker2");
  }

  @Test
  public void testBuilderChaining() {
    OperationCheckResult result = new OperationCheckResult.Builder()
        .addBlocker("a")
        .addBlocker("b")
        .build();
    Assert.assertFalse(result.isSuccessful());
    Assert.assertEquals(result.getBlockers().size(), 2);
  }

  @Test
  public void testBuilderResultIsIsolatedFromBuilder() {
    OperationCheckResult.Builder builder = new OperationCheckResult.Builder();
    builder.addBlocker("blocker1");
    OperationCheckResult result = builder.build();

    // Adding more blockers to builder should not affect the already-built result
    builder.addBlocker("blocker2");
    Assert.assertEquals(result.getBlockers().size(), 1);
  }
}
