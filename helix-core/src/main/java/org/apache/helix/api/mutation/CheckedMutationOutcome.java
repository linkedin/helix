package org.apache.helix.api.mutation;

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

/**
 * Outcome of a checked (conditional) mutation.
 *
 * <p>Every outcome is a successful evaluation of the request. A conflict is an expected,
 * actionable answer rather than an error, so callers must branch on the outcome instead of
 * treating "no exception" as "the desired state is now in effect".
 */
public enum CheckedMutationOutcome {
  /**
   * The mutation was written. The data version reported with the result is the version the
   * write produced.
   */
  APPLIED,
  /**
   * The desired intent was already satisfied, so nothing was written. No data version bump,
   * no timestamp rewrite and no reordering of any existing state happened.
   */
  UNCHANGED,
  /**
   * A precondition did not hold at mutation time, so nothing was written. The reported state
   * is the state that failed the check.
   */
  CONFLICT,
  /**
   * The target node does not exist. Checked mutations never create the node they guard.
   */
  NOT_FOUND
}
