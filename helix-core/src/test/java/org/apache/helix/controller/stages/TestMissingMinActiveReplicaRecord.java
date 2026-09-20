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
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

import org.testng.Assert;
import org.testng.annotations.Test;

public class TestMissingMinActiveReplicaRecord {
  private static final Set<String> ACTIVE_STATES =
      new HashSet<>(Arrays.asList("MASTER", "SLAVE"));

  private MissingMinActiveReplicaRecord recordFor(int minimum, String... repairingInstances) {
    MissingMinActiveReplicaRecord record = new MissingMinActiveReplicaRecord(1000L);
    record.observeConfiguration(minimum, "MasterSlave", "OFFLINE", ACTIVE_STATES);
    record.observeSequence(1L);
    record.observeParticipant("healthy", "session", "MASTER", "SLAVE", 100L, 500L);
    for (String instance : repairingInstances) {
      record.observeParticipant(instance, "session", "OFFLINE", null, -1L, -1L);
    }
    record.observeSequence(2L);
    return record;
  }

  @Test
  public void testParallelExecutionIsSubtractedOnce() {
    MissingMinActiveReplicaRecord record = recordFor(3, "one", "two");
    record.observeParticipant("one", "session", "SLAVE", "OFFLINE", 1200L, 2000L);
    record.observeParticipant("two", "session", "SLAVE", "OFFLINE", 1200L, 1900L);

    Assert.assertEquals(record.getHelixLatency(2000L), 200L);
  }

  @Test
  public void testParallelRecoveryDoesNotHideControllerDelay() {
    MissingMinActiveReplicaRecord record = new MissingMinActiveReplicaRecord(1000L);
    record.observeConfiguration(3, "MasterSlave", "OFFLINE", ACTIVE_STATES);
    record.observeSequence(1L);
    record.observeParticipant("healthy", "session", "MASTER", "SLAVE", 100L, 500L);
    record.observeParticipant("first-repair", "session", "OFFLINE", null, -1L, -1L);
    record.observeParticipant("second-repair", "session", "OFFLINE", null, -1L, -1L);

    // Both replicas must recover. Helix spends 800 ms dispatching the second while the first
    // executes. The metric must retain those 800 ms of Helix delay.
    record.observeSequence(2L);
    record.observeParticipant("first-repair", "session", "SLAVE", "OFFLINE", 1000L, 1900L);
    record.observeParticipant("second-repair", "session", "SLAVE", "OFFLINE", 1800L, 2000L);

    Assert.assertEquals(record.getHelixLatency(2000L), 800L,
        "Full recovery attribution must retain controller delay on the required recovery path");
  }

  @Test
  public void testHealthyReplicaExecutionCannotHideRecoveryDelay() {
    MissingMinActiveReplicaRecord record = recordFor(2, "one");
    record.observeParticipant("healthy", "session", "SLAVE", "MASTER", 1000L, 1900L);
    record.observeParticipant("one", "session", "SLAVE", "OFFLINE", 1800L, 2000L);

    Assert.assertEquals(record.getHelixLatency(2000L), 800L,
        "An already-active replica's transition does not restore the missing replica");
  }

  @Test
  public void testExtraReplicaDoesNotReplaceTheMinimumRestorationPath() {
    MissingMinActiveReplicaRecord record = recordFor(2, "one", "two");
    record.observeParticipant("one", "session", "SLAVE", "OFFLINE", 1200L, 1500L);
    record.observeParticipant("two", "session", "SLAVE", "OFFLINE", 1100L, 1900L);

    Assert.assertEquals(record.getHelixLatency(2000L), 700L,
        "The minimum was restored by one; the additional replica's work is not deducted");
  }

  @Test
  public void testAmbiguousSimultaneousRestorationIsUnavailable() {
    MissingMinActiveReplicaRecord record = recordFor(2, "one", "two");
    record.observeParticipant("one", "session", "SLAVE", "OFFLINE", 1200L, 2000L);
    record.observeParticipant("two", "session", "SLAVE", "OFFLINE", 1500L, 2000L);

    Assert.assertEquals(record.getHelixLatency(2000L), -1L);
    Assert.assertEquals(record.getUnavailableReason(), "recovery critical path is ambiguous");
  }

  @Test
  public void testEqualSimultaneousRestorationHasUnambiguousLatency() {
    MissingMinActiveReplicaRecord record = recordFor(3, "one", "two");
    record.observeParticipant("one", "session", "SLAVE", "OFFLINE", 1200L, 2000L);
    record.observeParticipant("two", "session", "SLAVE", "OFFLINE", 1200L, 2000L);

    Assert.assertEquals(record.getHelixLatency(2000L), 200L);
  }

  @Test
  public void testCrossParticipantRetryDependencyIsNotGuessed() {
    MissingMinActiveReplicaRecord record = recordFor(2, "one", "two");
    record.observeParticipant("one", "session", "ERROR", "OFFLINE", 1100L, 1200L);
    record.observeSequence(3L);
    record.observeParticipant("two", "session", "SLAVE", "OFFLINE", 1500L, 2000L);

    Assert.assertEquals(record.getHelixLatency(2000L), -1L);
    Assert.assertEquals(record.getUnavailableReason(),
        "cross-participant retry dependencies are unavailable");
  }

  @Test
  public void testLaterResetDoesNotEraseEarlierCrossParticipantFailure() {
    MissingMinActiveReplicaRecord record = recordFor(2, "one", "two");
    record.observeParticipant("one", "session", "ERROR", "OFFLINE", 1100L, 1200L);
    record.observeSequence(3L);
    record.observeParticipant("one", "session", "OFFLINE", "ERROR", 1700L, 2200L);
    record.observeParticipant("two", "session", "SLAVE", "OFFLINE", 1500L, 2000L);

    Assert.assertEquals(record.getHelixLatency(2300L), -1L,
        "A later reset cannot remove an earlier ambiguous recovery dependency");
    Assert.assertEquals(record.getUnavailableReason(),
        "cross-participant retry dependencies are unavailable");
  }

  @Test
  public void testAdditionalActiveReplicaLossIsUnavailable() {
    MissingMinActiveReplicaRecord record = recordFor(2, "one", "two");
    record.observeParticipant("healthy", "session", "ERROR", "MASTER", 1100L, 1200L);
    record.observeParticipant("one", "session", "SLAVE", "OFFLINE", 1300L, 1700L);
    record.observeParticipant("two", "session", "SLAVE", "OFFLINE", 1400L, 1800L);

    Assert.assertEquals(record.getHelixLatency(2000L), -1L);
    Assert.assertEquals(record.getUnavailableReason(), "an active replica was lost during recovery");
  }

  @Test
  public void testDisappearedParticipantHistoryIsUnavailable() {
    MissingMinActiveReplicaRecord record = recordFor(2, "one");
    record.observeParticipant("one", "session", "SLAVE", "OFFLINE", 1200L, 1500L);
    record.finishObservation(new HashSet<>(Arrays.asList("one")));

    Assert.assertEquals(record.getHelixLatency(2000L), -1L);
    Assert.assertEquals(record.getUnavailableReason(), "participant history disappeared during recovery");
  }

  @Test
  public void testSlowerParticipantDoesNotIncreaseHelixLatency() {
    MissingMinActiveReplicaRecord fast = recordFor(2, "one");
    fast.observeParticipant("one", "session", "SLAVE", "OFFLINE", 1200L, 2000L);
    MissingMinActiveReplicaRecord slow = recordFor(2, "one");
    slow.observeParticipant("one", "session", "SLAVE", "OFFLINE", 1200L, 5000L);

    Assert.assertEquals(fast.getHelixLatency(2000L), 200L);
    Assert.assertEquals(slow.getHelixLatency(5000L), 200L);
  }

  @Test
  public void testControllerWaitIsIncludedInHelixLatency() {
    MissingMinActiveReplicaRecord fast = recordFor(2, "one");
    fast.observeParticipant("one", "session", "SLAVE", "OFFLINE", 1200L, 2000L);
    MissingMinActiveReplicaRecord delayed = recordFor(2, "one");
    delayed.observeParticipant("one", "session", "SLAVE", "OFFLINE", 1500L, 2300L);

    Assert.assertEquals(fast.getHelixLatency(2000L), 200L);
    Assert.assertEquals(delayed.getHelixLatency(2300L), 500L);
  }

  @Test
  public void testSequentialRetryExecutionIsRetained() {
    MissingMinActiveReplicaRecord record = recordFor(2, "one");
    record.observeParticipant("one", "session", "ERROR", "OFFLINE", 1200L, 2000L);
    record.observeParticipant("one", "session", "OFFLINE", "ERROR", 2000L, 2000L);
    record.observeParticipant("one", "session", "SLAVE", "OFFLINE", 2300L, 3000L);

    Assert.assertEquals(record.getHelixLatency(3000L), 500L);
  }

  @Test
  public void testNonCriticalExecutionDoesNotReduceControllerLatency() {
    MissingMinActiveReplicaRecord record = recordFor(4, "one", "two", "three");
    record.observeParticipant("one", "session", "SLAVE", "OFFLINE", 1200L, 2000L);
    record.observeParticipant("one", "session", "SLAVE", "OFFLINE", 1200L, 2000L);
    record.observeParticipant("two", "session", "SLAVE", "OFFLINE", 1300L, 1500L);
    record.observeParticipant("three", "session", "SLAVE", "OFFLINE", 1100L, 1400L);

    Assert.assertEquals(record.getHelixLatency(2000L), 200L,
        "Only the execution path of the last required activation is deducted");
  }

  @Test
  public void testExecutionIsLimitedToTheRecoveryWindow() {
    MissingMinActiveReplicaRecord record = recordFor(2, "one");
    record.observeParticipant("one", "session", "SLAVE", "OFFLINE", 900L, 1500L);

    Assert.assertEquals(record.getHelixLatency(2000L), 500L);
  }

  @Test
  public void testNoRecoveryTransitionIsUnavailable() {
    MissingMinActiveReplicaRecord record = recordFor(2, "one");

    Assert.assertEquals(record.getHelixLatency(2000L), -1L);
  }

  @Test
  public void testMeasuredZeroIsValid() {
    MissingMinActiveReplicaRecord record = recordFor(2, "one");
    record.observeParticipant("one", "session", "SLAVE", "OFFLINE", 1000L, 2000L);

    Assert.assertEquals(record.getHelixLatency(2000L), 0L);
  }

  @Test
  public void testMissingTimingOnAStateChangeIsUnavailable() {
    MissingMinActiveReplicaRecord record = recordFor(2, "one");
    record.observeParticipant("one", "session", "SLAVE", "OFFLINE", -1L, -1L);

    Assert.assertEquals(record.getHelixLatency(2000L), -1L);
    Assert.assertEquals(record.getUnavailableReason(), "state changed without transition timing");
  }

  @Test
  public void testSkippedTransitionHistoryIsUnavailable() {
    MissingMinActiveReplicaRecord record = recordFor(2, "one");
    record.observeParticipant("one", "session", "MASTER", "SLAVE", 1200L, 1500L);

    Assert.assertEquals(record.getHelixLatency(2000L), -1L);
    Assert.assertEquals(record.getUnavailableReason(), "intermediate transition history is missing");
  }

  @Test
  public void testSessionChangeIsUnavailable() {
    MissingMinActiveReplicaRecord record = recordFor(2, "one");
    record.observeParticipant("one", "new", "SLAVE", "OFFLINE", 1200L, 1500L);

    Assert.assertEquals(record.getHelixLatency(2000L), -1L);
  }

  @Test
  public void testInvalidAndFutureExecutionAreUnavailable() {
    MissingMinActiveReplicaRecord backwards = recordFor(2, "one");
    backwards.observeParticipant("one", "session", "SLAVE", "OFFLINE", 1500L, 1200L);
    Assert.assertEquals(backwards.getHelixLatency(2000L), -1L);

    MissingMinActiveReplicaRecord future = recordFor(2, "one");
    future.observeParticipant("one", "session", "SLAVE", "OFFLINE", 1200L, 2100L);
    Assert.assertEquals(future.getHelixLatency(2000L), -1L);
  }

  @Test
  public void testRecoveryClockMovingBackwardsIsUnavailable() {
    MissingMinActiveReplicaRecord record = recordFor(2, "one");
    record.observeParticipant("one", "session", "SLAVE", "OFFLINE", 1100L, 1200L);

    Assert.assertEquals(record.getHelixLatency(900L), -1L);
  }

  @Test
  public void testConfigurationChangeInvalidatesAttribution() {
    MissingMinActiveReplicaRecord record = recordFor(2, "one");
    record.observeParticipant("one", "session", "SLAVE", "OFFLINE", 1200L, 1500L);
    record.observeConfiguration(1, "MasterSlave", "OFFLINE", ACTIVE_STATES);

    Assert.assertEquals(record.getHelixLatency(2000L), -1L);
  }

  @Test
  public void testSkippedObservationsInvalidateAttribution() {
    MissingMinActiveReplicaRecord record = recordFor(2, "one");
    record.observeParticipant("one", "session", "ERROR", "OFFLINE", 1100L, 1200L);
    record.observeSequence(4L);

    Assert.assertEquals(record.getHelixLatency(2000L), -1L);
    Assert.assertEquals(record.getUnavailableReason(),
        "recovery observations were skipped or reordered");
  }

  @Test
  public void testNewParticipantWithMissingTimingInvalidatesOtherValidSamples() {
    MissingMinActiveReplicaRecord record = recordFor(2, "one");
    record.observeParticipant("one", "session", "ERROR", "OFFLINE", 1100L, 1200L);
    record.observeSequence(3L);
    record.observeParticipant("two", "session", "SLAVE", "OFFLINE", -1L, -1L);

    Assert.assertEquals(record.getHelixLatency(2000L), -1L);
    Assert.assertEquals(record.getUnavailableReason(), "transition execution timing is unavailable");
  }

  @Test
  public void testRetriesUseBoundedPerParticipantAccumulation() {
    MissingMinActiveReplicaRecord record = recordFor(2, "one");
    String previous = "OFFLINE";
    for (int i = 0; i < 100; i++) {
      String state = i % 2 == 0 ? "ERROR" : "OFFLINE";
      record.observeParticipant("one", "session", state, previous, 1100L + 2 * i,
          1101L + 2 * i);
      previous = state;
    }

    record.observeParticipant("one", "session", "SLAVE", "OFFLINE", 1500L, 1600L);
    Assert.assertEquals(record.getHelixLatency(2000L), 800L);
  }

  @Test
  public void testUnboundedParticipantHistoryIsNotRetained() {
    MissingMinActiveReplicaRecord record = recordFor(2, "one");
    for (int i = 0; i < 100; i++) {
      record.observeParticipant("instance" + i, "session", "SLAVE", "OFFLINE", 1100L, 1200L);
    }

    Assert.assertEquals(record.getHelixLatency(2000L), -1L);
    Assert.assertEquals(record.getUnavailableReason(), "participant tracking limit exceeded");
  }

  @Test
  public void testLargeBaselineDoesNotExhaustParticipantChurnLimit() {
    MissingMinActiveReplicaRecord record = new MissingMinActiveReplicaRecord(1000L);
    record.observeConfiguration(65, "MasterSlave", "OFFLINE", ACTIVE_STATES);
    record.observeSequence(1L);
    for (int i = 0; i < 64; i++) {
      record.observeParticipant("healthy-" + i, "session", "SLAVE", "OFFLINE", 100L, 500L);
    }
    record.observeParticipant("repair", "session", "OFFLINE", null, -1L, -1L);
    record.observeSequence(2L);
    record.observeParticipant("repair", "session", "SLAVE", "OFFLINE", 1200L, 1500L);

    Assert.assertEquals(record.getHelixLatency(2000L), 700L,
        "The existing replica population must not consume the limit on new participant identities");
  }
}
