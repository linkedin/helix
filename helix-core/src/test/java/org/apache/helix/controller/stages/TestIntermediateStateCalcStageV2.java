package org.apache.helix.controller.stages;

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
import org.testng.annotations.Test;


/**
 * Runs all V1 IntermediateStateCalcStage test scenarios with V2 enabled.
 * Inherits the 8 V1 tests from the parent class; the only change is that
 * {@link #setClusterConfig} injects the V2 flag before writing to ZK,
 * so the pipeline delegates to {@link IntermediateStateCalcStageV2}.
 */
public class TestIntermediateStateCalcStageV2 extends TestIntermediateStateCalcStage {

  @Override
  protected void setClusterConfig(ClusterConfig clusterConfig) {
    clusterConfig.setIntermediateStateCalcStageV2Enabled(true);
    super.setClusterConfig(clusterConfig);
  }

  // The V1 routing test asserts that V2 is NOT called when the flag is off.
  // Since this subclass always enables V2, skip this test.
  @Override
  @Test(enabled = false)
  public void testUsesV1WhenV2Disabled() {
  }
}
