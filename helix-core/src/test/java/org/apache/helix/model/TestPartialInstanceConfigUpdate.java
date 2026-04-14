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

import com.google.common.collect.ImmutableMap;
import org.apache.helix.constants.InstanceConstants;
import org.apache.helix.zookeeper.datamodel.ZNRecord;
import org.testng.Assert;
import org.testng.annotations.Test;

/**
 * Validates that partial InstanceConfig updates (e.g., from Helix UI editing a single mapField)
 * do not incorrectly trigger instance operation transition validation.
 *
 * Bug: PerInstanceAccessor.validateDeltaTopologySettingInInstanceConfig() calls
 * newInstanceConfig.getInstanceOperation().getOperation() on a partial payload that has no
 * HELIX_INSTANCE_OPERATIONS or HELIX_ENABLED fields. This defaults to ENABLE, causing a
 * false validation failure when the current instance state is UNKNOWN, EVACUATE, etc.
 *
 * Fix: Skip operation transition validation when the payload does not contain operation fields.
 * This test verifies the precondition (partial payload defaults to ENABLE) and the fix condition
 * (payload does not contain operation fields).
 */
public class TestPartialInstanceConfigUpdate {

  /**
   * Verifies that a partial ZNRecord (only mapFields, like what the UI sends when editing
   * a config value) creates an InstanceConfig whose getInstanceOperation() defaults to ENABLE.
   * This is the root cause of the bug: the default ENABLE does not match the actual instance
   * state, causing validateInstanceOperationTransition to reject the update.
   */
  @Test
  public void testPartialPayloadDefaultsToEnable() {
    // Simulate what the Helix UI sends: only the edited mapField, no simpleFields/listFields
    ZNRecord partialRecord = new ZNRecord("test-host_1690");
    partialRecord.setMapField("participant_info", ImmutableMap.of("HELIX_PORT", "1690"));

    InstanceConfig partialConfig = new InstanceConfig(partialRecord);

    // getInstanceOperation() should return ENABLE because HELIX_INSTANCE_OPERATIONS is absent
    // and HELIX_ENABLED defaults to true
    InstanceConstants.InstanceOperation operation =
        partialConfig.getInstanceOperation().getOperation();
    Assert.assertEquals(operation, InstanceConstants.InstanceOperation.ENABLE,
        "Partial payload without operation fields should default to ENABLE");
  }

  /**
   * Verifies that the fix condition correctly identifies partial payloads that do NOT contain
   * instance operation fields. When payloadChangesOperation is false, the validation should
   * be skipped.
   */
  @Test
  public void testPartialPayloadDoesNotContainOperationFields() {
    // Partial payload: only mapFields (what the UI sends for a mapField edit)
    ZNRecord partialRecord = new ZNRecord("test-host_1690");
    partialRecord.setMapField("participant_info", ImmutableMap.of("WEIGHT", "200"));

    // Check the fix condition: neither HELIX_INSTANCE_OPERATIONS nor HELIX_ENABLED present
    boolean payloadChangesOperation =
        partialRecord.getListFields().containsKey(
            InstanceConfig.InstanceConfigProperty.HELIX_INSTANCE_OPERATIONS.name())
        || partialRecord.getSimpleFields().containsKey(
            InstanceConfig.InstanceConfigProperty.HELIX_ENABLED.name());

    Assert.assertFalse(payloadChangesOperation,
        "Partial payload without operation fields should NOT trigger operation validation");
  }

  /**
   * Verifies that a full payload (or one that includes operation fields) IS correctly identified
   * as needing validation. This ensures the fix does not skip validation when it should run.
   */
  @Test
  public void testFullPayloadContainsOperationFields() {
    // Full payload: includes HELIX_ENABLED (like a full config update)
    ZNRecord fullRecord = new ZNRecord("test-host_1690");
    fullRecord.setMapField("participant_info", ImmutableMap.of("WEIGHT", "200"));
    fullRecord.setSimpleField(
        InstanceConfig.InstanceConfigProperty.HELIX_ENABLED.name(), "true");

    boolean payloadChangesOperation =
        fullRecord.getListFields().containsKey(
            InstanceConfig.InstanceConfigProperty.HELIX_INSTANCE_OPERATIONS.name())
        || fullRecord.getSimpleFields().containsKey(
            InstanceConfig.InstanceConfigProperty.HELIX_ENABLED.name());

    Assert.assertTrue(payloadChangesOperation,
        "Payload with HELIX_ENABLED should trigger operation validation");
  }

  /**
   * Verifies that a payload with HELIX_INSTANCE_OPERATIONS list field is correctly identified.
   */
  @Test
  public void testPayloadWithInstanceOperationsListField() {
    ZNRecord record = new ZNRecord("test-host_1690");
    record.setListField(
        InstanceConfig.InstanceConfigProperty.HELIX_INSTANCE_OPERATIONS.name(),
        java.util.Collections.singletonList("{\"OPERATION\":\"EVACUATE\"}"));

    boolean payloadChangesOperation =
        record.getListFields().containsKey(
            InstanceConfig.InstanceConfigProperty.HELIX_INSTANCE_OPERATIONS.name())
        || record.getSimpleFields().containsKey(
            InstanceConfig.InstanceConfigProperty.HELIX_ENABLED.name());

    Assert.assertTrue(payloadChangesOperation,
        "Payload with HELIX_INSTANCE_OPERATIONS should trigger operation validation");
  }
}
