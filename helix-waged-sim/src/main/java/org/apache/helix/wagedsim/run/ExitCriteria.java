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

import java.util.LinkedHashMap;
import java.util.Map;

import org.apache.helix.wagedsim.util.Durations;

/**
 * When a run stops and whether it passed.
 * <ul>
 * <li>With {@code until}: PASS when it holds for {@code stableFor} consecutive rounds (1 by default),
 * at or after {@code minRounds}; FAIL if {@code maxRounds} is reached first.</li>
 * <li>Without {@code until} but with {@code stableFor}: PASS when no replica or top state moved for
 * that many rounds; FAIL if {@code maxRounds} is reached first.</li>
 * <li>With neither: a fixed number of rounds; PASS when {@code maxRounds} rounds complete.</li>
 * </ul>
 * In every case FAIL when {@code failIf} holds, a WAGED rebalance fails (unless disabled), or the
 * wall-clock {@code timeout} or virtual {@code maxSimTime} runs out first.
 */
public class ExitCriteria {
  public Condition until;
  public Condition failIf;
  public Integer stableFor;
  /** Defaults to the round of the last scheduled event. */
  public Integer minRounds;
  public int maxRounds = 20;
  public long timeoutMillis = 30 * 60_000L;
  public Long maxSimTimeMillis;
  public boolean failOnRebalanceFailure = true;

  public int stableRounds() {
    return stableFor != null ? stableFor : 1;
  }

  /** @return true when the run stops at a condition rather than after a fixed number of rounds */
  public boolean conditional() {
    return until != null || stableFor != null;
  }

  /** Tracks progress of one variant towards a verdict. */
  public class Tracker {
    private int _streak;
    private final int _minRounds;

    public Tracker(int lastEventRound) {
      _minRounds = minRounds != null ? minRounds : lastEventRound;
    }

    /**
     * @return the verdict reached after this round, or null to keep going
     */
    public Verdict afterRound(int round, Map<String, Object> stats, java.util.List<String> failures,
        long elapsedMillis, long simElapsedMillis) {
      if (failIf != null && failIf.test(stats)) {
        return Verdict.fail(round, "failIf (" + failIf + ")");
      }
      if (failOnRebalanceFailure && !failures.isEmpty()) {
        return Verdict.fail(round, "rebalanceFailure: " + failures.get(0));
      }
      if (conditional()) {
        boolean holds = until != null ? until.test(stats)
            : number(stats.get("moves.replicas")) == 0 && number(stats.get("moves.topState")) == 0;
        _streak = holds ? _streak + 1 : 0;
        if (_streak >= stableRounds() && round >= _minRounds) {
          return Verdict.pass(round, until != null ? "until (" + until + ")"
              + (stableRounds() > 1 ? " for " + stableRounds() + " rounds" : "")
              : "stable (nothing moved) for " + stableRounds() + " rounds");
        }
      }
      if (elapsedMillis > timeoutMillis) {
        return Verdict.fail(round, "timeout " + Durations.format(timeoutMillis));
      }
      if (maxSimTimeMillis != null && simElapsedMillis > maxSimTimeMillis) {
        return Verdict.fail(round, "maxSimTime " + Durations.format(maxSimTimeMillis));
      }
      if (round >= maxRounds) {
        if (!conditional()) {
          return Verdict.pass(round, "completed " + maxRounds + " rounds");
        }
        return Verdict.fail(round, "maxRounds " + maxRounds
            + (until != null ? " reached before until (" + until + ")" : " reached before stable"));
      }
      return null;
    }

    public boolean holdsNow() {
      return _streak > 0;
    }
  }

  private static double number(Object value) {
    return value instanceof Number ? ((Number) value).doubleValue() : 0;
  }

  public Map<String, Object> describe() {
    Map<String, Object> map = new LinkedHashMap<>();
    if (until != null) {
      map.put("until", until.toString());
    }
    if (conditional()) {
      map.put("stableFor", stableRounds());
    }
    if (minRounds != null) {
      map.put("minRounds", minRounds);
    }
    if (failIf != null) {
      map.put("failIf", failIf.toString());
    }
    map.put("maxRounds", maxRounds);
    map.put("timeout", Durations.format(timeoutMillis));
    if (maxSimTimeMillis != null) {
      map.put("maxSimTime", Durations.format(maxSimTimeMillis));
    }
    map.put("failOnRebalanceFailure", failOnRebalanceFailure);
    return map;
  }
}
