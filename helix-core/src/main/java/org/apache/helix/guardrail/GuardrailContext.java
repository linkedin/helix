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

/**
 * Immutable bundle of everything a {@link GuardrailRule} needs to evaluate a proposed mutation.
 * <p>
 * The context is intentionally small: it carries the cluster name, a narrow read-only view of
 * cluster state ({@link ReadOnlyDataAccessor}) for the target cluster, and the target instance name
 * for instance-scoped operations. When rules for other object types (e.g. resources) are added, the
 * corresponding field can be introduced here through the {@link Builder} without breaking existing
 * rules.
 */
public class GuardrailContext {
  private final String clusterName;
  private final ReadOnlyDataAccessor dataAccessor;
  private final String instanceName;

  private GuardrailContext(Builder builder) {
    this.clusterName = builder.clusterName;
    this.dataAccessor = builder.dataAccessor;
    this.instanceName = builder.instanceName;
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

  public static Builder newBuilder(String clusterName) {
    return new Builder(clusterName);
  }

  public static final class Builder {
    private final String clusterName;
    private ReadOnlyDataAccessor dataAccessor;
    private String instanceName;

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

    public GuardrailContext build() {
      return new GuardrailContext(this);
    }
  }
}
