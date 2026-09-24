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

import java.util.Objects;
import java.util.Optional;

import javax.annotation.Nullable;

import org.apache.zookeeper.data.Stat;

/**
 * Optional expectations about the node a checked mutation targets: its data version and its
 * creation id (the metadata store's creation transaction id, which changes whenever the node
 * is deleted and recreated).
 *
 * <p><b>What each expectation is worth.</b>
 * <ul>
 *   <li>{@code expectedVersion} is enforced by the conditional write itself. The mutation is
 *       evaluated on exactly the version it writes against, so no other writer of that node
 *       can slip a change in between the check and the write.</li>
 *   <li>{@code expectedCreationId} is evaluated at mutation time and reported back with the
 *       result. It rejects a node that was already deleted and recreated before the check,
 *       which is the case operators actually hit. It is <b>not</b> an atomic identity fence:
 *       the store offers no creation-id-conditional write, so a delete and recreate that
 *       lands between the check and the write, at exactly the expected data version, is not
 *       excluded. Callers that need a hard identity guarantee have to serialise every writer
 *       through an external lease or lock protocol; that is a protocol change, not something
 *       a pre-read plus a version-only compare-and-set can provide.</li>
 * </ul>
 */
public final class NodeExpectation {
  private static final NodeExpectation NONE = new NodeExpectation(null, null);

  private final Integer _expectedVersion;
  private final Long _expectedCreationId;

  private NodeExpectation(@Nullable Integer expectedVersion, @Nullable Long expectedCreationId) {
    _expectedVersion = expectedVersion;
    _expectedCreationId = expectedCreationId;
  }

  /**
   * @return an expectation that accepts any version and any incarnation of the node.
   */
  public static NodeExpectation none() {
    return NONE;
  }

  /**
   * @param expectedVersion the data version the caller read the node at.
   */
  public static NodeExpectation ofVersion(int expectedVersion) {
    return new NodeExpectation(expectedVersion, null);
  }

  /**
   * @param expectedCreationId the creation id the caller read the node at.
   */
  public static NodeExpectation ofCreationId(long expectedCreationId) {
    return new NodeExpectation(null, expectedCreationId);
  }

  /**
   * @param expectedVersion the data version the caller read the node at, or null for any.
   * @param expectedCreationId the creation id the caller read the node at, or null for any.
   */
  public static NodeExpectation of(@Nullable Integer expectedVersion,
      @Nullable Long expectedCreationId) {
    if (expectedVersion == null && expectedCreationId == null) {
      return NONE;
    }
    return new NodeExpectation(expectedVersion, expectedCreationId);
  }

  /**
   * @return true when at least one expectation is set.
   */
  public boolean isSet() {
    return _expectedVersion != null || _expectedCreationId != null;
  }

  @Nullable
  public Integer getExpectedVersion() {
    return _expectedVersion;
  }

  @Nullable
  public Long getExpectedCreationId() {
    return _expectedCreationId;
  }

  /**
   * Check this expectation against the node metadata read for the mutation.
   *
   * @param stat the metadata read together with the node content the mutation is evaluated on.
   * @return the conflict reason when an expectation does not hold, otherwise empty.
   */
  public Optional<CheckedMutationConflictReason> check(Stat stat) {
    Objects.requireNonNull(stat, "stat must not be null");
    // Identity is checked first: when the node was recreated, reporting a version mismatch
    // would point the caller at the wrong problem.
    if (_expectedCreationId != null && _expectedCreationId != stat.getCzxid()) {
      return Optional.of(CheckedMutationConflictReason.IDENTITY_MISMATCH);
    }
    if (_expectedVersion != null && _expectedVersion != stat.getVersion()) {
      return Optional.of(CheckedMutationConflictReason.VERSION_MISMATCH);
    }
    return Optional.empty();
  }

  @Override
  public String toString() {
    return "NodeExpectation{version=" + _expectedVersion + ", creationId=" + _expectedCreationId
        + "}";
  }
}
