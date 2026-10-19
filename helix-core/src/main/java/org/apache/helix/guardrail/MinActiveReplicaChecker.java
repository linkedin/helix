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

package org.apache.helix.guardrail;

import java.util.Set;

import org.apache.helix.util.MinActiveReplicaCheckResult;

/**
 * A read-only seam that lets a {@link GuardrailRule} ask "if the target instance stopped serving the
 * partitions it currently hosts, would every one of those partitions still meet its
 * {@code minActiveReplicas} on the remaining instances?" without holding a full
 * {@link org.apache.helix.HelixDataAccessor}.
 * <p>
 * The underlying check ({@code InstanceValidationUtil.siblingNodesActiveReplicaCheckWithDetails})
 * needs a full data accessor that {@link ReadOnlyDataAccessor} deliberately does not expose. Keeping
 * that plumbing behind this interface lets the endpoint supply a thin lambda over the util while the
 * rule stays a pure, unit-testable function of its inputs &mdash; the same seam pattern used by
 * {@link WagedAssignmentProvider}.
 * <p>
 * Implementations must be read-only. They evaluate the immediate post-stop state (before the
 * controller can recover replicas elsewhere), which is the conservative availability question a
 * pre-write guard rail needs to answer.
 */
@FunctionalInterface
public interface MinActiveReplicaChecker {
  /**
   * Evaluate whether removing {@code instanceName}'s replicas from serving keeps every partition it
   * hosts at or above its {@code minActiveReplicas} on the remaining instances.
   *
   * @param instanceName         the instance whose replicas are presumed to stop serving
   * @param toBeStoppedInstances additional instances presumed already stopped (must not contain
   *                             {@code instanceName}); pass an empty set for a single-instance check
   * @return a passed result if every hosted partition still meets its minimum, otherwise a failed
   *         result carrying the first offending resource/partition and its active-vs-required counts
   */
  MinActiveReplicaCheckResult check(String instanceName, Set<String> toBeStoppedInstances);
}
