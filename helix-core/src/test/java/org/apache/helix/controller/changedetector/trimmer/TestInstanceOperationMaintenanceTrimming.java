package org.apache.helix.controller.changedetector.trimmer;

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

import org.apache.helix.model.ClusterConfig;
import org.apache.helix.model.ClusterConfig.ClusterConfigProperty;
import org.apache.helix.model.InstanceConfig;
import org.apache.helix.model.InstanceConfig.InstanceConfigProperty;
import org.testng.Assert;
import org.testng.annotations.Test;

/**
 * Locks in the contract that instance-operation maintenance fields are non-topology and
 * therefore trimmed before change-detection compares old vs new InstanceConfig/ClusterConfig
 * snapshots. Marker writes must not trigger spurious rebalance pipeline runs.
 */
public class TestInstanceOperationMaintenanceTrimming {

  @Test
  public void testInstanceConfigMaintenanceUntilMsIsTrimmed() {
    InstanceConfig original = new InstanceConfig("h1");
    original.setHostName("host");
    original.setPort("1234");
    original.setInstanceOperationMaintenanceUntilMs(System.currentTimeMillis() + 60_000L);

    InstanceConfig trimmed = InstanceConfigTrimmer.getInstance().trimProperty(original);

    Assert.assertFalse(trimmed.getRecord().getSimpleFields().containsKey(
        InstanceConfigProperty.INSTANCE_OPERATION_MAINTENANCE_UNTIL_MS.name()),
        "INSTANCE_OPERATION_MAINTENANCE_UNTIL_MS must be trimmed from change-detection "
            + "snapshots so marker writes do not trigger rebalance");
    Assert.assertTrue(trimmed.getRecord().getSimpleFields()
            .containsKey(InstanceConfigProperty.HELIX_HOST.name()),
        "Sanity: topology-relevant fields are still preserved through the trimmer");
  }

  @Test
  public void testClusterConfigInstanceOperationMaintenanceFieldsAreTrimmed() throws Exception {
    ClusterConfig original = new ClusterConfig("c");
    original.setInstanceOperationMaintenanceBudget(20);
    original.setDefaultInstanceOperationMaintenanceDurationMs(3_600_000L);

    ClusterConfig trimmed = ClusterConfigTrimmer.getInstance().trimProperty(original);

    Assert.assertFalse(trimmed.getRecord().getSimpleFields().containsKey(
        ClusterConfigProperty.INSTANCE_OPERATION_MAINTENANCE_BUDGET.name()));
    Assert.assertFalse(trimmed.getRecord().getSimpleFields().containsKey(
        ClusterConfigProperty.INSTANCE_OPERATION_MAINTENANCE_BUDGET_PERCENTAGE.name()));
    Assert.assertFalse(trimmed.getRecord().getSimpleFields().containsKey(
        ClusterConfigProperty.DEFAULT_INSTANCE_OPERATION_MAINTENANCE_DURATION_MS.name()));
  }
}
