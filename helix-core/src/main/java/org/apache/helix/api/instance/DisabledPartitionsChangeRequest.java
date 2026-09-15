package org.apache.helix.api.instance;

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

import java.util.Collections;
import java.util.LinkedHashSet;
import java.util.Objects;
import java.util.Set;

import javax.annotation.Nullable;

import org.apache.helix.api.mutation.NodeExpectation;

/**
 * A desired disabled or enabled state for a set of partitions of one resource on one
 * instance, together with the conditions under which it may be written.
 *
 * <p>Like the operation request, this describes an intent rather than an edit: the checked
 * change decides at mutation time whether the partitions are already in the requested state.
 */
public final class DisabledPartitionsChangeRequest {
  private final String _resource;
  private final Set<String> _partitions;
  private final boolean _disabled;
  private final NodeExpectation _nodeExpectation;
  private final Set<String> _expectedDisabledPartitions;
  private final LegacyFieldPolicy _legacyFieldPolicy;

  private DisabledPartitionsChangeRequest(Builder builder) {
    _resource = builder._resource;
    _partitions = Collections.unmodifiableSet(new LinkedHashSet<>(builder._partitions));
    _disabled = builder._disabled;
    _nodeExpectation = builder._nodeExpectation;
    _expectedDisabledPartitions = builder._expectedDisabledPartitions == null ? null
        : Collections.unmodifiableSet(new LinkedHashSet<>(builder._expectedDisabledPartitions));
    _legacyFieldPolicy = builder._legacyFieldPolicy;
  }

  public String getResource() {
    return _resource;
  }

  public Set<String> getPartitions() {
    return _partitions;
  }

  /**
   * @return true when the partitions should end up disabled, false when they should end up
   *     enabled.
   */
  public boolean isDisabled() {
    return _disabled;
  }

  public NodeExpectation getNodeExpectation() {
    return _nodeExpectation;
  }

  /**
   * @return the set of disabled partitions the caller expects this resource to have at
   *     mutation time, or null when the caller does not care.
   */
  @Nullable
  public Set<String> getExpectedDisabledPartitions() {
    return _expectedDisabledPartitions;
  }

  public LegacyFieldPolicy getLegacyFieldPolicy() {
    return _legacyFieldPolicy;
  }

  @Override
  public String toString() {
    return "DisabledPartitionsChangeRequest{resource=" + _resource + ", partitions=" + _partitions
        + ", disabled=" + _disabled + ", expectation=" + _nodeExpectation
        + ", expectedDisabledPartitions=" + _expectedDisabledPartitions + ", legacyFieldPolicy="
        + _legacyFieldPolicy + "}";
  }

  /**
   * @param resource the resource whose partitions are being changed. The reserved all
   *     resources key is accepted and behaves like any other key.
   * @param partitions the partitions to change. Must not be empty. An empty partition name is
   *     accepted because it is how the all resources key is used in practice.
   * @param disabled the state the partitions should end up in.
   */
  public static Builder newBuilder(String resource, Set<String> partitions, boolean disabled) {
    return new Builder(resource, partitions, disabled);
  }

  public static final class Builder {
    private final String _resource;
    private final Set<String> _partitions;
    private final boolean _disabled;
    private NodeExpectation _nodeExpectation = NodeExpectation.none();
    private Set<String> _expectedDisabledPartitions;
    private LegacyFieldPolicy _legacyFieldPolicy = LegacyFieldPolicy.OWNED_ONLY;

    private Builder(String resource, Set<String> partitions, boolean disabled) {
      _resource = Objects.requireNonNull(resource, "resource must not be null");
      if (resource.isEmpty()) {
        throw new IllegalArgumentException("resource must not be empty");
      }
      Objects.requireNonNull(partitions, "partitions must not be null");
      if (partitions.isEmpty()) {
        throw new IllegalArgumentException(
            "partitions must not be empty: a change with no partitions has no meaning and "
                + "would be silently reported as already satisfied");
      }
      if (partitions.stream().anyMatch(Objects::isNull)) {
        throw new IllegalArgumentException("partitions must not contain null");
      }
      _partitions = new LinkedHashSet<>(partitions);
      _disabled = disabled;
    }

    /**
     * Require the instance config node to still be at a known version, a known incarnation, or
     * both. See {@link NodeExpectation} for what each of those is worth.
     */
    public Builder setNodeExpectation(NodeExpectation nodeExpectation) {
      _nodeExpectation = Objects.requireNonNull(nodeExpectation,
          "nodeExpectation must not be null; use NodeExpectation.none()");
      return this;
    }

    /**
     * Require the effective disabled partitions of this resource at mutation time to be
     * exactly this set, otherwise conflict.
     */
    public Builder setExpectedDisabledPartitions(
        @Nullable Set<String> expectedDisabledPartitions) {
      _expectedDisabledPartitions = expectedDisabledPartitions;
      return this;
    }

    /**
     * Controls the deprecated flat disabled partition list, which disables a partition for
     * every resource. Under {@link LegacyFieldPolicy#OWNED_ONLY}, the default, a request to
     * enable a partition that is only disabled through that list is a conflict rather than a
     * removal that would silently re-enable the partition for every other resource too.
     */
    public Builder setLegacyFieldPolicy(LegacyFieldPolicy legacyFieldPolicy) {
      _legacyFieldPolicy =
          Objects.requireNonNull(legacyFieldPolicy, "legacyFieldPolicy must not be null");
      return this;
    }

    public DisabledPartitionsChangeRequest build() {
      return new DisabledPartitionsChangeRequest(this);
    }
  }
}
