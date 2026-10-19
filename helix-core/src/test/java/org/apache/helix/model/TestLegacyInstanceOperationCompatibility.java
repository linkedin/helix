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

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.apache.helix.constants.InstanceConstants.InstanceOperation;
import org.apache.helix.constants.InstanceConstants.InstanceOperationSource;
import org.apache.helix.zookeeper.datamodel.ZNRecord;
import org.apache.helix.zookeeper.datamodel.serializer.ZNRecordSerializer;
import org.testng.Assert;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;


public class TestLegacyInstanceOperationCompatibility {
  @DataProvider(name = "legacyDisabledRecords")
  public Object[][] legacyDisabledRecords() {
    Object[][] operations = {
        {null, null, true},
        {"EMPTY", null, true},
        {"ENABLE", "USER", false},
        {"ENABLE", "AUTOMATION", false},
        {"DISABLE", "USER", false},
        {"DISABLE", "AUTOMATION", true}
    };
    List<Object[]> records = new ArrayList<>();
    for (String type : new String[] {null, "DEFAULT_INSTANCE_DISABLE_TYPE", "CLOUD_EVENT",
        "USER_OPERATION"}) {
      for (Object[] operation : operations) {
        records.add(new Object[] {operation[0], operation[1], type, operation[2]});
      }
    }
    return records.toArray(new Object[0][]);
  }

  @Test(dataProvider = "legacyDisabledRecords")
  public void testAutomationEnablePreservesExistingOwnership(String operation, String source,
      String retiredType, boolean automationCanEnable) {
    ZNRecord record = new ZNRecord("instance");
    record.setSimpleField("HELIX_ENABLED", "false");
    record.setSimpleField("HELIX_DISABLED_REASON", "legacy reason");
    record.setSimpleField("HELIX_ENABLED_TIMESTAMP", "123");
    if (retiredType != null) {
      record.setSimpleField("HELIX_DISABLED_TYPE", retiredType);
    }
    if ("EMPTY".equals(operation)) {
      record.setListField("HELIX_INSTANCE_OPERATIONS", Collections.emptyList());
    } else if (operation != null) {
      String legacyProperty = retiredType == null ? ""
          : ",\"LEGACY_DISABLED_TYPE\":\"" + retiredType + "\"";
      record.setListField("HELIX_INSTANCE_OPERATIONS", Collections.singletonList(
          "{\"OPERATION\":\"" + operation + "\",\"SOURCE\":\"" + source
              + "\",\"REASON\":\"operation reason\",\"TIMESTAMP\":\"123\""
              + legacyProperty + "}"));
    }

    InstanceConfig config = roundTrip(new InstanceConfig(record));
    ZNRecord beforeRead = new ZNRecord(config.getRecord());
    boolean existingDisable = "DISABLE".equals(operation);
    InstanceOperationSource originalSource = existingDisable
        ? InstanceOperationSource.valueOf(source) : InstanceOperationSource.USER;
    String originalReason = existingDisable ? "operation reason" : "legacy reason";
    Assert.assertFalse(config.getInstanceEnabled());
    Assert.assertEquals(config.getInstanceOperation().getOperation(), InstanceOperation.DISABLE);
    Assert.assertEquals(config.getInstanceOperation().getSource(), originalSource);
    Assert.assertEquals(config.getInstanceOperation().getReason(), originalReason);
    Assert.assertEquals(config.getRecord(), beforeRead, "Reading must not rewrite persisted ownership");

    config.setInstanceOperation(new InstanceConfig.InstanceOperation.Builder()
        .setOperation(InstanceOperation.ENABLE).setSource(InstanceOperationSource.AUTOMATION)
        .setReason("automation enable").build());
    config = roundTrip(config);

    Assert.assertEquals(config.getInstanceEnabled(), automationCanEnable);
    Assert.assertEquals(config.getRecord().getBooleanField("HELIX_ENABLED", true),
        automationCanEnable);
    Assert.assertEquals(config.getInstanceOperation().getOperation(),
        automationCanEnable ? InstanceOperation.ENABLE : InstanceOperation.DISABLE);
    Assert.assertEquals(config.getInstanceOperation().getSource(),
        automationCanEnable ? InstanceOperationSource.AUTOMATION : InstanceOperationSource.USER);
    Assert.assertEquals(config.getInstanceOperation().getReason(),
        automationCanEnable ? "automation enable" : originalReason);
    Assert.assertEquals(config.getInstanceDisabledReason(), automationCanEnable ? "" : originalReason);
    Assert.assertEquals(config.getInstanceOperationState(),
        automationCanEnable ? "ENABLE" : "DISABLE");

    config.setInstanceOperation(new InstanceConfig.InstanceOperation.Builder()
        .setOperation(InstanceOperation.ENABLE).setSource(InstanceOperationSource.USER).build());
    config = roundTrip(config);
    Assert.assertTrue(config.getInstanceEnabled());
    Assert.assertEquals(config.getInstanceOperation().getSource(), InstanceOperationSource.USER);
    Assert.assertEquals(config.getInstanceDisabledReason(), "");
  }

  private InstanceConfig roundTrip(InstanceConfig config) {
    ZNRecordSerializer serializer = new ZNRecordSerializer();
    return new InstanceConfig(
        (ZNRecord) serializer.deserialize(serializer.serialize(config.getRecord())));
  }
}
