package org.apache.helix.wagedsim.run;

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

import org.testng.Assert;
import org.testng.annotations.Test;

public class TestConditionAndExit {

  private static Map<String, Object> stats(Object... keyValues) {
    Map<String, Object> map = new HashMap<>();
    for (int i = 0; i < keyValues.length; i += 2) {
      map.put((String) keyValues[i], keyValues[i + 1]);
    }
    return map;
  }

  @Test
  public void testComparisonsAndLogic() {
    Map<String, Object> s = stats("skew.top.CU", 1.08, "moves.replicas", 0L, "yardstick.targetKey", "DISK");
    Assert.assertTrue(Condition.parse("skew.top.CU <= 1.10").test(s));
    Assert.assertFalse(Condition.parse("skew.top.CU < 1.08").test(s));
    Assert.assertTrue(Condition.parse("skew.top.CU <= 1.1 and moves.replicas == 0").test(s));
    Assert.assertTrue(Condition.parse("skew.top.CU > 2 or moves.replicas == 0").test(s));
    Assert.assertTrue(Condition.parse("not (skew.top.CU > 2)").test(s));
    Assert.assertTrue(Condition.parse("skew.top.CU <= 1.1 && moves.replicas == 0").test(s));
    Assert.assertTrue(Condition.parse("yardstick.targetKey == 'DISK'").test(s));
    Assert.assertTrue(Condition.parse("yardstick.targetKey != \"CU\"").test(s));
  }

  @Test
  public void testMissingStatIsFalse() {
    Assert.assertFalse(Condition.parse("missing.stat < 5").test(stats()));
    Assert.assertTrue(Condition.parse("not missing.stat < 5").test(stats()));
  }

  @Test
  public void testStatsReferenced() {
    Assert.assertEquals(Condition.parse("(a.b < 1 or c >= 2) and not d == 3").stats().toString(), "[a.b, c, d]");
  }

  @Test(expectedExceptions = IllegalArgumentException.class)
  public void testRejectsSingleEquals() {
    Condition.parse("a = 1");
  }

  @Test(expectedExceptions = IllegalArgumentException.class)
  public void testRejectsTrailingTokens() {
    Condition.parse("a < 1 b");
  }

  @Test
  public void testUntilWithStableForAndMinRounds() {
    ExitCriteria exit = new ExitCriteria();
    exit.until = Condition.parse("x <= 1");
    exit.stableFor = 2;
    exit.minRounds = 3;
    ExitCriteria.Tracker tracker = exit.new Tracker(0);
    Assert.assertNull(tracker.afterRound(1, stats("x", 1), Collections.emptyList(), 0, 0));
    Assert.assertNull(tracker.afterRound(2, stats("x", 1), Collections.emptyList(), 0, 0));
    Verdict verdict = tracker.afterRound(3, stats("x", 1), Collections.emptyList(), 0, 0);
    Assert.assertEquals(verdict.status, Verdict.Status.PASS);
    Assert.assertEquals(verdict.round, 3);
  }

  @Test
  public void testStreakResets() {
    ExitCriteria exit = new ExitCriteria();
    exit.until = Condition.parse("x <= 1");
    exit.stableFor = 2;
    exit.maxRounds = 4;
    ExitCriteria.Tracker tracker = exit.new Tracker(0);
    Assert.assertNull(tracker.afterRound(1, stats("x", 1), Collections.emptyList(), 0, 0));
    Assert.assertNull(tracker.afterRound(2, stats("x", 2), Collections.emptyList(), 0, 0));
    Assert.assertNull(tracker.afterRound(3, stats("x", 1), Collections.emptyList(), 0, 0));
    Assert.assertEquals(tracker.afterRound(4, stats("x", 1), Collections.emptyList(), 0, 0).status,
        Verdict.Status.PASS);
  }

  @Test
  public void testMaxRoundsFailsWhenConditional() {
    ExitCriteria exit = new ExitCriteria();
    exit.until = Condition.parse("x <= 1");
    exit.maxRounds = 2;
    ExitCriteria.Tracker tracker = exit.new Tracker(0);
    Assert.assertNull(tracker.afterRound(1, stats("x", 5), Collections.emptyList(), 0, 0));
    Verdict verdict = tracker.afterRound(2, stats("x", 5), Collections.emptyList(), 0, 0);
    Assert.assertEquals(verdict.status, Verdict.Status.FAIL);
    Assert.assertTrue(verdict.reason.startsWith("maxRounds"), verdict.reason);
  }

  @Test
  public void testFixedRoundsPass() {
    ExitCriteria exit = new ExitCriteria();
    exit.maxRounds = 3;
    ExitCriteria.Tracker tracker = exit.new Tracker(0);
    Assert.assertNull(tracker.afterRound(1, stats(), Collections.emptyList(), 0, 0));
    Assert.assertNull(tracker.afterRound(2, stats(), Collections.emptyList(), 0, 0));
    Verdict verdict = tracker.afterRound(3, stats(), Collections.emptyList(), 0, 0);
    Assert.assertEquals(verdict.status, Verdict.Status.PASS);
    Assert.assertEquals(verdict.reason, "completed 3 rounds");
  }

  @Test
  public void testFailIfTimeoutAndRebalanceFailure() {
    ExitCriteria exit = new ExitCriteria();
    exit.until = Condition.parse("x <= 1");
    exit.failIf = Condition.parse("bad > 0");
    exit.timeoutMillis = 1000;
    ExitCriteria.Tracker tracker = exit.new Tracker(0);
    // failIf wins over until in the same round.
    Assert.assertEquals(tracker.afterRound(1, stats("x", 1, "bad", 1), Collections.emptyList(), 0, 0).status,
        Verdict.Status.FAIL);
    tracker = exit.new Tracker(0);
    Verdict timeout = tracker.afterRound(1, stats("x", 3), Collections.emptyList(), 5000, 0);
    Assert.assertEquals(timeout.status, Verdict.Status.FAIL);
    Assert.assertTrue(timeout.reason.startsWith("timeout"));
    tracker = exit.new Tracker(0);
    Verdict failure = tracker.afterRound(1, stats("x", 1), Collections.singletonList("FAILED_TO_CALCULATE"), 0, 0);
    Assert.assertEquals(failure.status, Verdict.Status.FAIL);
    Assert.assertTrue(failure.reason.startsWith("rebalanceFailure"));
  }

  @Test
  public void testStableWithoutUntil() {
    ExitCriteria exit = new ExitCriteria();
    exit.stableFor = 2;
    exit.maxRounds = 10;
    ExitCriteria.Tracker tracker = exit.new Tracker(1);
    Assert.assertNull(tracker.afterRound(1, stats("moves.replicas", 3L, "moves.topState", 1L),
        Collections.emptyList(), 0, 0));
    Assert.assertNull(tracker.afterRound(2, stats("moves.replicas", 0L, "moves.topState", 0L),
        Collections.emptyList(), 0, 0));
    Assert.assertEquals(tracker.afterRound(3, stats("moves.replicas", 0L, "moves.topState", 0L),
        Collections.emptyList(), 0, 0).status, Verdict.Status.PASS);
  }
}
