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

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import org.apache.helix.HelixRebalanceException;
import org.apache.helix.controller.stages.CurrentStateOutput;
import org.apache.helix.model.ResourceAssignment;
import org.apache.helix.monitoring.metrics.model.LatencyMetric;
import org.mockito.Mockito;
import org.testng.Assert;
import org.testng.annotations.Test;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class TestAssignmentManager {

  @Test
  public void testLatencyEndedOnSuccessfulRead() throws HelixRebalanceException {
    LatencyMetric latencyMetric = mock(LatencyMetric.class);
    AssignmentManager manager = new AssignmentManager(latencyMetric);
    AssignmentMetadataStore store = mock(AssignmentMetadataStore.class);

    Map<String, ResourceAssignment> baseline = new HashMap<>();
    ResourceAssignment assignment = new ResourceAssignment("Resource1");
    baseline.put("Resource1", assignment);

    when(store.getBaseline()).thenReturn(baseline);
    when(store.getBestPossibleAssignment()).thenReturn(baseline);

    CurrentStateOutput currentStateOutput = new CurrentStateOutput();
    Set<String> resources = Collections.singleton("Resource1");

    Map<String, ResourceAssignment> resultBaseline =
        manager.getBaselineAssignment(store, currentStateOutput, resources);
    Assert.assertEquals(resultBaseline.size(), 1);
    verify(latencyMetric, times(1)).startMeasuringLatency();
    verify(latencyMetric, times(1)).endMeasuringLatency();

    Mockito.reset(latencyMetric);

    Map<String, ResourceAssignment> resultBest =
        manager.getBestPossibleAssignment(store, currentStateOutput, resources);
    Assert.assertEquals(resultBest.size(), 1);
    verify(latencyMetric, times(1)).startMeasuringLatency();
    verify(latencyMetric, times(1)).endMeasuringLatency();
  }

  @Test
  public void testLatencyEndedOnMetadataStoreIoException() {
    LatencyMetric latencyMetric = mock(LatencyMetric.class);
    AssignmentManager manager = new AssignmentManager(latencyMetric);
    AssignmentMetadataStore store = mock(AssignmentMetadataStore.class);

    when(store.getBaseline()).thenThrow(new RuntimeException("ZK connection lost"));
    when(store.getBestPossibleAssignment()).thenThrow(new RuntimeException("ZK connection lost"));

    CurrentStateOutput currentStateOutput = new CurrentStateOutput();
    Set<String> resources = Collections.singleton("Resource1");

    // Baseline read failure
    try {
      manager.getBaselineAssignment(store, currentStateOutput, resources);
      Assert.fail("Expected HelixRebalanceException on baseline store failure");
    } catch (HelixRebalanceException ex) {
      Assert.assertEquals(ex.getFailureType(), HelixRebalanceException.Type.INVALID_REBALANCER_STATUS);
      Assert.assertEquals(ex.getFailureCategory(), HelixRebalanceException.FailureCategory.METADATA_STORE_IO);
    }
    verify(latencyMetric, times(1)).startMeasuringLatency();
    verify(latencyMetric, times(1)).endMeasuringLatency();

    Mockito.reset(latencyMetric);

    // Best possible read failure
    try {
      manager.getBestPossibleAssignment(store, currentStateOutput, resources);
      Assert.fail("Expected HelixRebalanceException on best possible store failure");
    } catch (HelixRebalanceException ex) {
      Assert.assertEquals(ex.getFailureType(), HelixRebalanceException.Type.INVALID_REBALANCER_STATUS);
      Assert.assertEquals(ex.getFailureCategory(), HelixRebalanceException.FailureCategory.METADATA_STORE_IO);
    }
    verify(latencyMetric, times(1)).startMeasuringLatency();
    verify(latencyMetric, times(1)).endMeasuringLatency();
  }

  @Test
  public void testLatencyEndedOnInternalConversionException() {
    LatencyMetric latencyMetric = mock(LatencyMetric.class);
    AssignmentManager manager = new AssignmentManager(latencyMetric);
    AssignmentMetadataStore store = mock(AssignmentMetadataStore.class);

    // Provide a map that throws during stream processing / record copying (e.g. null entry value)
    Map<String, ResourceAssignment> corruptMap = new HashMap<>();
    corruptMap.put("Resource1", null);

    when(store.getBestPossibleAssignment()).thenReturn(corruptMap);

    CurrentStateOutput currentStateOutput = new CurrentStateOutput();
    Set<String> resources = Collections.singleton("Resource1");

    try {
      manager.getBestPossibleAssignment(store, currentStateOutput, resources);
      Assert.fail("Expected HelixRebalanceException on internal processing failure");
    } catch (HelixRebalanceException ex) {
      Assert.assertEquals(ex.getFailureType(), HelixRebalanceException.Type.INVALID_REBALANCER_STATUS);
      Assert.assertEquals(ex.getFailureCategory(), HelixRebalanceException.FailureCategory.ALGORITHM_INTERNAL);
    }
    verify(latencyMetric, times(1)).startMeasuringLatency();
    verify(latencyMetric, times(1)).endMeasuringLatency();
  }

  @Test
  public void testNullStoreDoesNotTriggerLatency() throws HelixRebalanceException {
    LatencyMetric latencyMetric = mock(LatencyMetric.class);
    AssignmentManager manager = new AssignmentManager(latencyMetric);
    CurrentStateOutput currentStateOutput = new CurrentStateOutput();
    Set<String> resources = Collections.singleton("Resource1");

    manager.getBaselineAssignment(null, currentStateOutput, resources);
    verify(latencyMetric, never()).startMeasuringLatency();
    verify(latencyMetric, never()).endMeasuringLatency();

    manager.getBestPossibleAssignment(null, currentStateOutput, resources);
    verify(latencyMetric, never()).startMeasuringLatency();
    verify(latencyMetric, never()).endMeasuringLatency();
  }
}
