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

import org.apache.helix.HelixDataAccessor;
import org.apache.helix.model.IdealState;
import org.apache.helix.model.InstanceConfig;
import org.apache.helix.model.ResourceConfig;

/**
 * Immutable bundle of everything a {@link GuardrailRule} needs to evaluate a proposed mutation.
 * <p>
 * The context is intentionally small: it carries the cluster name, a narrow read-only view of
 * cluster state ({@link ReadOnlyDataAccessor}) for the target cluster, and the target instance name
 * for instance-scoped operations. When rules need the actual object a mutation would write (rather
 * than only current cluster state read through the accessor), that <em>proposed</em> object is
 * supplied here as well; {@code proposedResourceConfig} and {@code proposedIdealState} are the first
 * such fields. New object types (e.g. a proposed instance config) are added the same way, through
 * the {@link Builder}, without breaking existing rules.
 */
public class GuardrailContext {
  private final String clusterName;
  private final ReadOnlyDataAccessor dataAccessor;
  private final String instanceName;
  private final ResourceConfig proposedResourceConfig;
  private final IdealState proposedIdealState;
  private final InstanceConfig proposedInstanceConfig;

  private GuardrailContext(Builder builder) {
    this.clusterName = builder.clusterName;
    this.dataAccessor = builder.dataAccessor;
    this.instanceName = builder.instanceName;
    this.proposedResourceConfig = builder.proposedResourceConfig;
    this.proposedIdealState = builder.proposedIdealState;
    this.proposedInstanceConfig = builder.proposedInstanceConfig;
  }

  public String getClusterName() {
    return clusterName;
  }

  public ReadOnlyDataAccessor getDataAccessor() {
    return dataAccessor;
  }

  /** The instance targeted by an instance-scoped mutation, or {@code null} if not applicable. */
  public String getInstanceName() {
    return instanceName;
  }

  /**
   * The resource config a mutation proposes to write, or {@code null} if the operation is not
   * resource-scoped. Rules read the to-be-written weights/settings from here rather than from ZK,
   * since the object does not exist in ZK yet at pre-validation time.
   */
  public ResourceConfig getProposedResourceConfig() {
    return proposedResourceConfig;
  }

  /**
   * The ideal state a mutation proposes to write, or {@code null} if the operation is not
   * resource-scoped. Rules read the resource's structure (e.g. its partition count / names) from
   * here rather than from ZK, since the object does not exist in ZK yet at pre-validation time. Note
   * that a freshly-proposed ideal state has no computed assignment yet: its partition <em>count</em>
   * ({@link IdealState#getNumPartitions()}) is set, but its per-partition preference lists are still
   * empty, so {@link IdealState#getPartitionSet()} may be empty at this point.
   */
  public IdealState getProposedIdealState() {
    return proposedIdealState;
  }

  /**
   * The instance config delta a mutation proposes to merge into the target instance, or {@code null}
   * if the operation is not instance-config-scoped. This carries only the fields the caller is
   * changing (e.g. a reduced capacity map); rules combine it with the existing config read through
   * the accessor to reason about the post-merge state that would be written.
   */
  public InstanceConfig getProposedInstanceConfig() {
    return proposedInstanceConfig;
  }

  public static Builder newBuilder(String clusterName) {
    return new Builder(clusterName);
  }

  public static final class Builder {
    private final String clusterName;
    private ReadOnlyDataAccessor dataAccessor;
    private String instanceName;
    private ResourceConfig proposedResourceConfig;
    private IdealState proposedIdealState;
    private InstanceConfig proposedInstanceConfig;

    private Builder(String clusterName) {
      this.clusterName = clusterName;
    }

    public Builder dataAccessor(HelixDataAccessor dataAccessor) {
      this.dataAccessor = ReadOnlyDataAccessor.of(dataAccessor);
      return this;
    }

    public Builder instanceName(String instanceName) {
      this.instanceName = instanceName;
      return this;
    }

    public Builder proposedResourceConfig(ResourceConfig proposedResourceConfig) {
      this.proposedResourceConfig = proposedResourceConfig;
      return this;
    }

    public Builder proposedIdealState(IdealState proposedIdealState) {
      this.proposedIdealState = proposedIdealState;
      return this;
    }

    public Builder proposedInstanceConfig(InstanceConfig proposedInstanceConfig) {
      this.proposedInstanceConfig = proposedInstanceConfig;
      return this;
    }

    public GuardrailContext build() {
      return new GuardrailContext(this);
    }
  }
}
