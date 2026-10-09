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

/** The outcome of one variant. */
public class Verdict {
  public enum Status {
    PASS, FAIL, ERROR
  }

  public Status status;
  public int round;
  public String reason;

  public static Verdict pass(int round, String reason) {
    return of(Status.PASS, round, reason);
  }

  public static Verdict fail(int round, String reason) {
    return of(Status.FAIL, round, reason);
  }

  public static Verdict error(int round, String reason) {
    return of(Status.ERROR, round, reason);
  }

  private static Verdict of(Status status, int round, String reason) {
    Verdict verdict = new Verdict();
    verdict.status = status;
    verdict.round = round;
    verdict.reason = reason;
    return verdict;
  }

  @Override
  public String toString() {
    return status + " at round " + round + ": " + reason;
  }
}
