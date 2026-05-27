package org.apache.helix.controller;

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

import org.apache.helix.controller.dataproviders.BaseControllerDataProvider;
import org.apache.helix.controller.dataproviders.ManagementControllerDataProvider;
import org.apache.helix.controller.dataproviders.ResourceControllerDataProvider;
import org.apache.helix.controller.dataproviders.WorkflowControllerDataProvider;
import org.mockito.Mockito;
import org.testng.Assert;
import org.testng.annotations.Test;

/**
 * Unit tests for {@link GenericHelixController#shouldCountTopologyEventAsProcessed}.
 *
 * <p>The processed counter on {@link org.apache.helix.monitoring.mbeans.TopologyChangeEventMonitor}
 * must only advance for resource-pipeline runs that actually completed work. The management-mode
 * pipeline only handles {@code LiveInstanceChange} -- for the other four topology event types its
 * pipeline list is empty, the for-loop in {@code handleEvent} runs zero times, and
 * {@code rebalanceFail} stays false. Without an explicit data-provider gate, those events would
 * be falsely credited as "processed" while the controller is stuck in management mode -- the
 * exact false-healthy signal PRR-30 was set up to detect. This test pins the gate to fail closed.
 */
public class TestGenericHelixControllerTopologyMetricGate {

  @Test
  public void resourceControllerSuccessIsCounted() {
    BaseControllerDataProvider dp = Mockito.mock(ResourceControllerDataProvider.class);
    Assert.assertTrue(GenericHelixController.shouldCountTopologyEventAsProcessed(false, dp));
  }

  @Test
  public void resourceControllerFailureIsNotCounted() {
    BaseControllerDataProvider dp = Mockito.mock(ResourceControllerDataProvider.class);
    Assert.assertFalse(GenericHelixController.shouldCountTopologyEventAsProcessed(true, dp));
  }

  @Test
  public void managementPipelineIsNotCounted() {
    BaseControllerDataProvider dp = Mockito.mock(ManagementControllerDataProvider.class);
    Assert.assertFalse(GenericHelixController.shouldCountTopologyEventAsProcessed(false, dp),
        "Management-mode runs must not bump the processed counter -- otherwise dashboards "
            + "would show received==processed while topology events sit unhandled.");
  }

  @Test
  public void taskPipelineIsNotCounted() {
    BaseControllerDataProvider dp = Mockito.mock(WorkflowControllerDataProvider.class);
    Assert.assertFalse(GenericHelixController.shouldCountTopologyEventAsProcessed(false, dp));
  }

  @Test
  public void unknownDataProviderIsNotCounted() {
    BaseControllerDataProvider dp = Mockito.mock(BaseControllerDataProvider.class);
    Assert.assertFalse(GenericHelixController.shouldCountTopologyEventAsProcessed(false, dp));
  }
}
