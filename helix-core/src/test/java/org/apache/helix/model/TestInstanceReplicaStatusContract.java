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

import com.fasterxml.jackson.databind.ObjectMapper;
import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import org.apache.helix.model.InstanceReplicaStatus.CoverageStatus;
import org.testng.Assert;
import org.testng.annotations.Test;

/**
 * Contract tests for {@link InstanceReplicaStatus}: the published JSON must survive a round trip,
 * and an observation that contradicts its own counts must not be constructible.
 */
public class TestInstanceReplicaStatusContract {
  private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();
  private static final List<String> OFFLINE = Collections.singletonList("OFFLINE");

  private static InstanceReplicaStatus populated() {
    Map<String, Integer> stateCounts = new LinkedHashMap<>();
    stateCounts.put("OFFLINE", 2);
    stateCounts.put("ERROR", 1);
    stateCounts.put("MASTER", 1);
    return new InstanceReplicaStatus(1234L, "cluster", "instance", true, CoverageStatus.COMPLETE, 4,
        2, OFFLINE, stateCounts, Collections.singletonList("db_0"),
        Collections.singletonList("workflow"), CoverageStatus.COMPLETE, false, 3,
        Arrays.asList("a blocker", "another blocker"));
  }

  /**
   * The bug this guards against: a derived collection getter that Jackson auto-detects is
   * serialized but has no creator parameter, so deserialization falls back to calling the getter
   * and mutating the immutable list it returns. That throws on every response, and a test that
   * reads the body into a Map never sees it.
   */
  @Test
  public void testPublishedJsonRoundTrips() throws Exception {
    InstanceReplicaStatus original = populated();

    String json = OBJECT_MAPPER.writeValueAsString(original);
    InstanceReplicaStatus restored = OBJECT_MAPPER.readValue(json, InstanceReplicaStatus.class);

    Assert.assertEquals(restored.getObservationTime(), original.getObservationTime());
    Assert.assertEquals(restored.getClusterName(), original.getClusterName());
    Assert.assertEquals(restored.getInstanceName(), original.getInstanceName());
    Assert.assertEquals(restored.isLive(), original.isLive());
    Assert.assertEquals(restored.getReplicaCoverage(), original.getReplicaCoverage());
    Assert.assertEquals(restored.getReplicaCount(), original.getReplicaCount());
    Assert.assertEquals(restored.getOfflineReplicaCount(), original.getOfflineReplicaCount());
    Assert.assertEquals(restored.getOfflineStates(), original.getOfflineStates());
    Assert.assertEquals(restored.getStateCounts(), original.getStateCounts());
    Assert.assertEquals(restored.getErrorPartitionNames(), original.getErrorPartitionNames());
    Assert.assertEquals(restored.getExcludedTaskResources(), original.getExcludedTaskResources());
    Assert.assertEquals(restored.getDrainCoverage(), original.getDrainCoverage());
    Assert.assertEquals(restored.isDrained(), original.isDrained());
    Assert.assertEquals(restored.getPendingMessageCount(), original.getPendingMessageCount());
    Assert.assertEquals(restored.getBlockers(), original.getBlockers());

    Assert.assertEquals(restored.areReplicasOffline(true), original.areReplicasOffline(true));
    Assert.assertEquals(restored.areReplicasOffline(false), original.areReplicasOffline(false));
    Assert.assertEquals(restored.areReplicasDroppedOrError(),
        original.areReplicasDroppedOrError());
    Assert.assertEquals(OBJECT_MAPPER.writeValueAsString(restored), json);
  }

  /** Derived readiness must never reach the wire, where it could contradict the counts. */
  @Test
  public void testDerivedReadinessIsNotSerialized() throws Exception {
    String json = OBJECT_MAPPER.writeValueAsString(populated());

    for (String derived : Arrays.asList("allOffline", "allOfflineOrError", "allError",
        "replicaScopeEmpty", "errorReplicaCount")) {
      Assert.assertFalse(json.contains("\"" + derived + "\""),
          "Derived value " + derived + " must not be serialized: " + json);
    }
  }

  @Test(expectedExceptions = Exception.class)
  public void testUnknownPropertyIsRejected() throws Exception {
    String json = OBJECT_MAPPER.writeValueAsString(populated())
        .replaceFirst("\\{", "{\"futureField\":\"futureValue\",");

    OBJECT_MAPPER.readValue(json, InstanceReplicaStatus.class);
  }

  @Test
  public void testReadinessFollowsTheCounts() {
    InstanceReplicaStatus allOffline = status(CoverageStatus.COMPLETE, 3, 3, OFFLINE,
        Collections.singletonMap("OFFLINE", 3), Collections.emptyList());
    Assert.assertTrue(allOffline.isAllOffline());
    Assert.assertTrue(allOffline.isAllOfflineOrError());
    Assert.assertFalse(allOffline.isAllError());
    Assert.assertFalse(allOffline.isReplicaScopeEmpty());
    Assert.assertTrue(allOffline.areReplicasOffline(false));

    InstanceReplicaStatus allError = status(CoverageStatus.COMPLETE, 2, 0, OFFLINE,
        Collections.singletonMap("ERROR", 2), Arrays.asList("db_0", "db_1"));
    Assert.assertEquals(allError.getErrorReplicaCount(), 2);
    Assert.assertFalse(allError.isAllOffline());
    Assert.assertTrue(allError.isAllOfflineOrError());
    Assert.assertTrue(allError.isAllError());
    Assert.assertFalse(allError.areReplicasOffline(false));
    Assert.assertTrue(allError.areReplicasOffline(true));
    Assert.assertTrue(allError.areReplicasDroppedOrError());

    InstanceReplicaStatus empty = status(CoverageStatus.COMPLETE, 0, 0, Collections.emptyList(),
        Collections.emptyMap(), Collections.emptyList());
    Assert.assertTrue(empty.isReplicaScopeEmpty());
    Assert.assertFalse(empty.isAllOffline(), "An empty scope is not an all-offline scope");
    Assert.assertTrue(empty.areReplicasOffline(false));
  }

  /** A state model whose initial state is not OFFLINE must still resolve as offline. */
  @Test
  public void testOfflineIsStateModelSpecific() {
    InstanceReplicaStatus dropped = status(CoverageStatus.COMPLETE, 2, 2,
        Collections.singletonList("DROPPED"), Collections.singletonMap("DROPPED", 2),
        Collections.emptyList());

    Assert.assertTrue(dropped.isAllOffline());
    Assert.assertTrue(dropped.areReplicasOffline(false));
  }

  /**
   * Offline-ness is attributed per replica against its own resource's state model. A state that is
   * the initial state of one model must not make a serving replica of another model read as
   * offline: STANDBY replicas of a LeaderStandby resource are serving, even when some other
   * resource on the instance has STANDBY as its initial state.
   */
  @Test
  public void testOfflineStatesDoNotAliasAcrossStateModels() {
    InstanceReplicaStatus serving = status(CoverageStatus.COMPLETE, 2, 0,
        Arrays.asList("OFFLINE", "STANDBY"), Collections.singletonMap("STANDBY", 2),
        Collections.emptyList());

    Assert.assertEquals(serving.getOfflineReplicaCount(), 0);
    Assert.assertFalse(serving.isAllOffline());
    Assert.assertFalse(serving.areReplicasOffline(false));
    Assert.assertFalse(serving.areReplicasOffline(true));
  }

  @Test
  public void testIncompleteCoverageIsNeverReadiness() {
    InstanceReplicaStatus incomplete = status(CoverageStatus.INCOMPLETE, 3, 3, OFFLINE,
        Collections.singletonMap("OFFLINE", 3), Collections.emptyList());

    Assert.assertFalse(incomplete.isAllOffline(),
        "Incomplete coverage must not assert an all-offline scope");
    Assert.assertFalse(incomplete.isAllOfflineOrError());
    Assert.assertFalse(incomplete.isAllError());
    Assert.assertFalse(incomplete.areReplicasOffline(false));
    Assert.assertFalse(incomplete.areReplicasOffline(true));
    Assert.assertFalse(incomplete.areReplicasDroppedOrError());
  }

  @Test
  public void testOfflineInstanceServesNothing() {
    InstanceReplicaStatus offline = new InstanceReplicaStatus(1L, "c", "i", false,
        CoverageStatus.COMPLETE, 2, 0, OFFLINE, Collections.singletonMap("MASTER", 2),
        Collections.emptyList(), Collections.emptyList(), CoverageStatus.COMPLETE, false, 0,
        Collections.emptyList());

    Assert.assertTrue(offline.areReplicasOffline(false));
    Assert.assertFalse(offline.isAllOffline());
  }

  @Test
  public void testContradictoryObservationsAreRejected() {
    assertRejected("all replicas offline while the state counts show only MASTERs",
        () -> status(CoverageStatus.COMPLETE, 5, 5, OFFLINE,
            Collections.singletonMap("MASTER", 5), Collections.emptyList()));
    assertRejected("complete coverage with state counts that miss a replica",
        () -> status(CoverageStatus.COMPLETE, 5, 0, OFFLINE,
            Collections.singletonMap("MASTER", 4), Collections.emptyList()));
    assertRejected("more error partitions than error replicas",
        () -> status(CoverageStatus.COMPLETE, 1, 0, OFFLINE,
            Collections.singletonMap("ERROR", 1), Arrays.asList("db_0", "db_1")));
    assertRejected("offline and error replicas exceeding the replica count",
        () -> status(CoverageStatus.INCOMPLETE, 1, 1, OFFLINE,
            mapOf("OFFLINE", 1, "ERROR", 1), Collections.emptyList()));
    assertRejected("a negative replica count",
        () -> status(CoverageStatus.COMPLETE, -1, 0, OFFLINE, Collections.emptyMap(),
            Collections.emptyList()));
    assertRejected("a negative state count",
        () -> status(CoverageStatus.INCOMPLETE, 1, 0, OFFLINE,
            Collections.singletonMap("OFFLINE", -1), Collections.emptyList()));
    assertRejected("ERROR treated as an offline state",
        () -> status(CoverageStatus.COMPLETE, 1, 1, Collections.singletonList("ERROR"),
            Collections.singletonMap("ERROR", 1), Collections.emptyList()));
    assertRejected("a null required field",
        () -> new InstanceReplicaStatus(1L, "c", "i", true, CoverageStatus.COMPLETE, 0, 0, OFFLINE,
            null, Collections.emptyList(), Collections.emptyList(), CoverageStatus.COMPLETE, false,
            0, Collections.emptyList()));
    assertRejected("drained without complete drain coverage",
        () -> new InstanceReplicaStatus(1L, "c", "i", true, CoverageStatus.COMPLETE, 0, 0, OFFLINE,
            Collections.emptyMap(), Collections.emptyList(), Collections.emptyList(),
            CoverageStatus.INCOMPLETE, true, 0, Collections.emptyList()));
    assertRejected("drained while messages are still pending",
        () -> new InstanceReplicaStatus(1L, "c", "i", true, CoverageStatus.COMPLETE, 0, 0, OFFLINE,
            Collections.emptyMap(), Collections.emptyList(), Collections.emptyList(),
            CoverageStatus.COMPLETE, true, 2, Collections.emptyList()));
  }

  private static Map<String, Integer> mapOf(String first, int firstCount, String second,
      int secondCount) {
    Map<String, Integer> counts = new LinkedHashMap<>();
    counts.put(first, firstCount);
    counts.put(second, secondCount);
    return counts;
  }

  private static InstanceReplicaStatus status(CoverageStatus replicaCoverage, int replicaCount,
      int offlineReplicaCount, List<String> offlineStates, Map<String, Integer> stateCounts,
      List<String> errorPartitionNames) {
    return new InstanceReplicaStatus(1L, "cluster", "instance", true, replicaCoverage, replicaCount,
        offlineReplicaCount, offlineStates, stateCounts, errorPartitionNames,
        Collections.emptyList(), CoverageStatus.COMPLETE, false, 0, Collections.emptyList());
  }

  private static void assertRejected(String description, Runnable construction) {
    try {
      construction.run();
      Assert.fail("Expected rejection of " + description);
    } catch (IllegalArgumentException expected) {
      Assert.assertNotNull(expected.getMessage());
    }
  }
}
