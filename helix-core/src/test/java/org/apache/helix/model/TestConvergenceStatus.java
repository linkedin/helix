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

import java.util.Collections;
import java.util.EnumMap;
import java.util.Map;
import java.util.TreeMap;

import org.apache.helix.zookeeper.datamodel.ZNRecord;
import org.testng.Assert;
import org.testng.annotations.Test;

public class TestConvergenceStatus {
  @Test
  public void testRoundTripThroughZNRecord() {
    ConvergenceStatus status = new ConvergenceStatus("db");
    status.setStatus(ConvergenceStatus.Status.BLOCKED);
    status.setPrimaryReason(ConvergenceStatus.Reason.TARGET_INSTANCE_NOT_LIVE);
    status.setGeneratedAt(100L);
    status.setUnconvergedSince(50L);
    status.setControllerSessionId("s1");
    status.setControllerName("controller_1");
    status.setTargetFreshness(ConvergenceStatus.TargetFreshness.LAST_KNOWN_GOOD);
    status.setOptimizerStatus(ConvergenceStatus.OptimizerStatus.BASELINE_FAILED);
    Map<ConvergenceStatus.Status, Integer> counts = new EnumMap<>(ConvergenceStatus.Status.class);
    counts.put(ConvergenceStatus.Status.CONVERGED, 7);
    counts.put(ConvergenceStatus.Status.BLOCKED, 3);
    status.setStatusCounts(counts);
    Map<ConvergenceStatus.Reason, Integer> reasons = new EnumMap<>(ConvergenceStatus.Reason.class);
    reasons.put(ConvergenceStatus.Reason.TARGET_INSTANCE_NOT_LIVE, 3);
    status.setReasonCounts(reasons);

    ConvergenceStatus read = new ConvergenceStatus(new ZNRecord(status.getRecord()));
    Assert.assertTrue(read.isValid());
    Assert.assertEquals(read.getStatus(), ConvergenceStatus.Status.BLOCKED);
    Assert.assertEquals(read.getPrimaryReason(), ConvergenceStatus.Reason.TARGET_INSTANCE_NOT_LIVE);
    Assert.assertEquals(read.getGeneratedAt(), 100L);
    Assert.assertEquals(read.getUnconvergedSince(), 50L);
    Assert.assertEquals(read.getControllerSessionId(), "s1");
    Assert.assertEquals(read.getControllerName(), "controller_1");
    Assert.assertEquals(read.getTargetFreshness(), ConvergenceStatus.TargetFreshness.LAST_KNOWN_GOOD);
    Assert.assertEquals(read.getPartitionCount(), 10);
    Assert.assertEquals(read.getAffectedPartitionCount(), 3);
    Assert.assertEquals(read.getStatusCounts(), counts);
    Assert.assertEquals(read.getReasonCounts(), reasons);
  }

  @Test
  public void testSemanticEqualityIgnoresProvenanceOnly() {
    ConvergenceStatus a = new ConvergenceStatus("db");
    a.setStatus(ConvergenceStatus.Status.IN_PROGRESS);
    a.setGeneratedAt(1L);
    a.setUnconvergedSince(1L);
    ConvergenceStatus b = new ConvergenceStatus(new ZNRecord(a.getRecord()));
    b.setGeneratedAt(2L);
    b.setUnconvergedSince(2L);
    Assert.assertTrue(a.semanticallyEquals(b));
    b.getRecord().getSimpleFields().remove("UNCONVERGED_SINCE");
    b.setControllerSessionId("s2");
    b.setControllerName("c2");
    Assert.assertTrue(a.semanticallyEquals(b));
    Assert.assertTrue(b.semanticallyEquals(a));
    b.setTargetFreshness(ConvergenceStatus.TargetFreshness.LAST_KNOWN_GOOD);
    Assert.assertFalse(a.semanticallyEquals(b));
    Assert.assertFalse(b.semanticallyEquals(a));
    b.getRecord().getSimpleFields().remove("TARGET_FRESHNESS");
    b.setStatusCounts(Collections.singletonMap(ConvergenceStatus.Status.IN_PROGRESS, 1));
    Assert.assertFalse(a.semanticallyEquals(b));
    b.getRecord().getMapFields().clear();
    b.setStatus(ConvergenceStatus.Status.BLOCKED);
    Assert.assertFalse(a.semanticallyEquals(b));
    Assert.assertFalse(a.semanticallyEquals(null));
  }

  @Test
  public void testUnknownValuesFromNewerSchemaDegradeSafely() {
    ZNRecord record = new ZNRecord("db");
    record.setIntField("SCHEMA_VERSION", 1);
    record.setSimpleField("STATUS", "SOMETHING_NEW");
    Map<String, String> counts = new TreeMap<>();
    counts.put("SOMETHING_NEW", "4");
    counts.put("BLOCKED", "x");
    counts.put("CONVERGED", "2");
    record.setMapField("STATUS_COUNTS", counts);
    ConvergenceStatus status = new ConvergenceStatus(record);
    Assert.assertEquals(status.getStatus(), ConvergenceStatus.Status.UNKNOWN);
    Assert.assertEquals(status.getPartitionCount(), 2);
    Assert.assertFalse(new ConvergenceStatus(new ZNRecord("x")).isValid());
  }

  @Test
  public void testClusterConfigFlagDefaultsOn() {
    ClusterConfig config = new ClusterConfig("c");
    Assert.assertTrue(config.isConvergenceMonitoringEnabled());
    config.setConvergenceMonitoringEnabled(false);
    Assert.assertFalse(config.isConvergenceMonitoringEnabled());
  }
}
