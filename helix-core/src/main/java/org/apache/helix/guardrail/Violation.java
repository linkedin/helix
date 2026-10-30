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

/**
 * A single reason a mutation was judged unsafe by a {@link GuardrailRule}.
 * <p>
 * Instances are immutable and are surfaced to REST callers as JSON, so every field is exposed via a
 * public getter for serialization. {@code resourceName} and {@code partitionName} are optional and
 * may be {@code null} when a violation is not scoped to a specific resource / partition.
 * {@code indeterminate} marks a rule that could not judge the mutation, as opposed to one that
 * judged it unsafe.
 */
public class Violation {
  private final String ruleId;
  private final String resourceName;
  private final String partitionName;
  private final String message;
  private final boolean indeterminate;

  private Violation(String ruleId, String resourceName, String partitionName, String message,
      boolean indeterminate) {
    this.ruleId = ruleId;
    this.resourceName = resourceName;
    this.partitionName = partitionName;
    this.message = message;
    this.indeterminate = indeterminate;
  }

  public String getRuleId() {
    return ruleId;
  }

  public String getResourceName() {
    return resourceName;
  }

  public String getPartitionName() {
    return partitionName;
  }

  public String getMessage() {
    return message;
  }

  /** Whether the rule could not judge the mutation, rather than judged it unsafe. */
  public boolean isIndeterminate() {
    return indeterminate;
  }

  public static Builder newBuilder(String ruleId) {
    return new Builder(ruleId);
  }

  @Override
  public String toString() {
    return "Violation{ruleId=" + ruleId + ", resource=" + resourceName + ", partition="
        + partitionName + ", message=" + message
        + (indeterminate ? ", indeterminate" : "") + '}';
  }

  public static final class Builder {
    private final String ruleId;
    private String resourceName;
    private String partitionName;
    private String message;
    private boolean indeterminate;

    private Builder(String ruleId) {
      this.ruleId = ruleId;
    }

    public Builder resource(String resourceName) {
      this.resourceName = resourceName;
      return this;
    }

    public Builder partition(String partitionName) {
      this.partitionName = partitionName;
      return this;
    }

    public Builder message(String message) {
      this.message = message;
      return this;
    }

    /** Marks a rule that could not judge the mutation. */
    public Builder indeterminate() {
      this.indeterminate = true;
      return this;
    }

    public Violation build() {
      return new Violation(ruleId, resourceName, partitionName, message, indeterminate);
    }
  }
}
