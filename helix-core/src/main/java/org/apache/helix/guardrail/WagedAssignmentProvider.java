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

import java.util.List;
import java.util.Map;

import org.apache.helix.model.ClusterConfig;
import org.apache.helix.model.IdealState;
import org.apache.helix.model.InstanceConfig;
import org.apache.helix.model.ResourceAssignment;
import org.apache.helix.model.ResourceConfig;

/**
 * A read-only seam that lets a {@link GuardrailRule} ask "what stable (preference-list) assignment
 * would the WAGED rebalancer compute for these inputs?" without holding ZooKeeper-connection objects.
 * <p>
 * The WAGED what-if (see {@code HelixUtil.getTargetAssignmentForWagedFullAuto}) needs low-level
 * metadata-store accessors that {@link ReadOnlyDataAccessor} does not expose and that, in realm-aware
 * deployments, are only reachable through the REST layer's live client. Keeping that plumbing behind
 * this interface lets the endpoint supply an implementation (typically a thin lambda over the WAGED
 * util) while the rule stays a pure, unit-testable function of its inputs.
 * <p>
 * Implementations must be read-only and may throw when no assignment can be computed (e.g. a
 * cluster-wide {@code CAPACITY_DEFICIT}); callers treat a thrown exception as "no assignment produced".
 */
@FunctionalInterface
public interface WagedAssignmentProvider {
  /**
   * Compute the target (preference-list based) WAGED assignment for the given cluster inputs.
   *
   * @param clusterConfig   the cluster config to evaluate against
   * @param instanceConfigs the instance configs to consider (the caller applies any proposed change
   *                        to this list before calling; WAGED honours {@code isAssignable()})
   * @param liveInstances   names of the live instances
   * @param idealStates     the WAGED resources' ideal states to compute assignment for
   * @param resourceConfigs the WAGED resources' configs (weights); may be a subset of
   *                        {@code idealStates} when some resources have no config
   * @return a map of resource name to its computed {@link ResourceAssignment}
   * @throws Exception if the rebalancer cannot compute an assignment for the given inputs
   */
  Map<String, ResourceAssignment> computeTargetAssignment(ClusterConfig clusterConfig,
      List<InstanceConfig> instanceConfigs, List<String> liveInstances, List<IdealState> idealStates,
      List<ResourceConfig> resourceConfigs) throws Exception;
}
