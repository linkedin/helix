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

package org.apache.helix.guardrail.rules;

import org.apache.helix.guardrail.GuardrailContext;
import org.apache.helix.guardrail.GuardrailPipeline;
import org.apache.helix.guardrail.ValidationResult;
import org.apache.helix.model.ClusterConfig;
import org.apache.helix.zookeeper.datamodel.ZNRecord;
import org.testng.Assert;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

public class TestRetiredClusterConfigGuardrailRule {
  private static final String KEY = "ERROR_PARTITION_THRESHOLD_FOR_LOAD_BALANCE";
  private final GuardrailPipeline _pipeline =
      new GuardrailPipeline(new RetiredClusterConfigGuardrailRule());

  @DataProvider
  public Object[][] changes() {
    return new Object[][] {
        {null, "3", false}, {"3", "5", false}, {"0", "1", false},
        {"3", "03", false}, {"3", "3", true}, {"3", null, true},
        {null, null, true}, {"invalid", "invalid", true}
    };
  }

  @Test(dataProvider = "changes")
  public void testRetiredKeyChanges(String oldValue, String newValue, boolean allowed) {
    ClusterConfig current = new ClusterConfig("cluster");
    ClusterConfig proposed = new ClusterConfig("cluster");
    if (oldValue != null) {
      current.getRecord().setSimpleField(KEY, oldValue);
    }
    if (newValue != null) {
      proposed.getRecord().setSimpleField(KEY, newValue);
    }
    proposed.setErrorOrRecoveryPartitionThresholdForLoadBalance(100);
    proposed.getRecord().setSimpleField("customKey", "allowed");
    ZNRecord before = new ZNRecord(current.getRecord());
    ZNRecord after = new ZNRecord(proposed.getRecord());
    ValidationResult result = _pipeline.validate(GuardrailContext.newBuilder("cluster")
        .currentClusterConfig(current).proposedClusterConfig(proposed).build());

    Assert.assertEquals(result.isFeasible(), allowed);
    if (!allowed) {
      Assert.assertEquals(result.getViolations().size(), 1);
      Assert.assertEquals(result.getViolations().get(0).getRuleId(),
          RetiredClusterConfigGuardrailRule.RULE_ID);
      Assert.assertTrue(result.getViolations().get(0).getMessage()
          .contains("Use ERROR_OR_RECOVERY_PARTITION_THRESHOLD_FOR_LOAD_BALANCE"));
    }
    Assert.assertEquals(current.getRecord(), before);
    Assert.assertEquals(proposed.getRecord(), after);
  }

  @Test
  public void testMissingCurrentRecordAndNullValueAddition() {
    ClusterConfig proposed = new ClusterConfig("cluster");
    proposed.getRecord().setSimpleField(KEY, null);
    Assert.assertFalse(_pipeline.validate(GuardrailContext.newBuilder("cluster")
        .proposedClusterConfig(proposed).build()).isFeasible());

    ClusterConfig current = new ClusterConfig("cluster");
    GuardrailContext context = GuardrailContext.newBuilder("cluster")
        .currentClusterConfig(current).proposedClusterConfig(proposed).build();
    Assert.assertFalse(_pipeline.validate(context).isFeasible());
    current.getRecord().setSimpleField(KEY, null);
    Assert.assertTrue(_pipeline.validate(context).isFeasible());
    proposed.getRecord().setSimpleField(KEY, "3");
    Assert.assertFalse(_pipeline.validate(context).isFeasible());
    proposed.getRecord().getSimpleFields().remove(KEY);
    Assert.assertTrue(_pipeline.validate(context).isFeasible());
  }

  @Test
  public void testOtherMutationIsUnaffected() {
    Assert.assertTrue(_pipeline.validate(GuardrailContext.newBuilder("cluster").build()).isFeasible());
  }
}
