package org.apache.helix.wagedsim.scenario;

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

/**
 * One scheduled event: {@code at: N}, or {@code every: K} (with optional {@code from} and
 * {@code times}), plus exactly one event kind, for example {@code disable: hottest-top:3}.
 */
public class EventSpec {
  public Integer at;
  public Integer every;
  public int from = 1;
  public Integer times;
  public String kind;
  public Object args;
  public Map<String, Object> source = new LinkedHashMap<>();

  public boolean dueAt(int round) {
    if (at != null) {
      return round == at;
    }
    if (every != null) {
      if (round < from || (round - from) % every != 0) {
        return false;
      }
      return times == null || (round - from) / every < times;
    }
    return false;
  }

  /** @return the last round this event fires, or {@code from} for an unbounded repeat */
  public int lastRound() {
    if (at != null) {
      return at;
    }
    if (every != null && times != null) {
      return from + every * (times - 1);
    }
    return from;
  }

  public String describe() {
    String timing = at != null ? "round " + at
        : "every " + every + " rounds from " + from + (times != null ? ", " + times + " times" : "");
    return kind + " " + (args == null ? "" : String.valueOf(args)) + " (" + timing + ")";
  }
}
