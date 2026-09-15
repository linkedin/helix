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
 * Why a checked mutation returned {@link CheckedMutationOutcome#CONFLICT}.
 *
 * <p>The reasons are stable enough to branch on: a caller can retry a
 * {@link #CONCURRENT_MODIFICATION} immediately, but must not retry a
 * {@link #LEGACY_STATE_NOT_OWNED} without human or policy input, because retrying cannot
 * make the caller the owner of state written by somebody else.
 */
public enum CheckedMutationConflictReason {
  /**
   * The caller supplied an expected data version and the node was at a different version when
   * the mutation was evaluated.
   */
  VERSION_MISMATCH,
  /**
   * The caller supplied an expected creation id and the node carried a different one, which
   * means the node this request was built against was deleted and recreated.
   */
  IDENTITY_MISMATCH,
  /**
   * A domain precondition over the node content (for example an expected current state) did
   * not hold at mutation time.
   */
  EXPECTED_STATE_MISMATCH,
  /**
   * The mutation was well formed, but applying it could not have produced the desired
   * effective state because other state that the caller does not own dominates it. Nothing
   * was written, so no success-shaped partial write is left behind.
   */
  DESIRED_STATE_BLOCKED,
  /**
   * Applying the mutation would have relaxed deprecated or shared state that cannot be
   * attributed to the caller, for example clearing another writer's disable marker.
   */
  LEGACY_STATE_NOT_OWNED,
  /**
   * The requested transition is not allowed from the state observed at mutation time.
   */
  INVALID_TRANSITION,
  /**
   * Another writer kept winning the conditional write, so the bounded retry budget was
   * exhausted without applying anything. Retrying is safe.
   */
  CONCURRENT_MODIFICATION,
  /**
   * The node holds state this version cannot interpret, so the change was refused rather than
   * guessed at. Writing would mean deciding against state whose meaning is unknown, and could
   * drop it.
   */
  UNREADABLE_STATE
}
