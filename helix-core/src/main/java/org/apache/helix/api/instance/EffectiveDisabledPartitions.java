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
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

/**
 * The disabled partition state a checked change was decided on, or produced.
 *
 * <p>The map is the effective view per resource, so a caller does not have to merge the
 * current per resource field with the deprecated flat list that disables a partition across
 * every resource. That flat list is reported separately because it is shared state: removing
 * an entry from it re-enables the partition everywhere, which is why a checked change never
 * clears it implicitly.
 */
public final class EffectiveDisabledPartitions {
  private final Map<String, List<String>> _disabledPartitions;
  private final Set<String> _crossResourceDisabledPartitions;
  private final boolean _desiredStateInEffect;

  EffectiveDisabledPartitions(Map<String, List<String>> disabledPartitions,
      Set<String> crossResourceDisabledPartitions, boolean desiredStateInEffect) {
    _disabledPartitions = Collections.unmodifiableMap(
        Objects.requireNonNull(disabledPartitions, "disabledPartitions must not be null"));
    _crossResourceDisabledPartitions = Collections.unmodifiableSet(Objects.requireNonNull(
        crossResourceDisabledPartitions, "crossResourceDisabledPartitions must not be null"));
    _desiredStateInEffect = desiredStateInEffect;
  }

  /**
   * @return the effective disabled partitions per resource, including anything contributed by
   *     the deprecated cross resource list. Never null, possibly empty.
   */
  public Map<String, List<String>> getDisabledPartitions() {
    return _disabledPartitions;
  }

  /**
   * @param resource the resource to look up.
   * @return the effective disabled partitions for one resource, never null.
   */
  public List<String> getDisabledPartitions(String resource) {
    return _disabledPartitions.getOrDefault(resource, Collections.emptyList());
  }

  /**
   * @return the partitions disabled through the deprecated flat list, which applies to every
   *     resource. Never null, possibly empty.
   */
  public Set<String> getCrossResourceDisabledPartitions() {
    return _crossResourceDisabledPartitions;
  }

  /**
   * @return true when every partition named in the request is in the requested state.
   */
  public boolean isDesiredStateInEffect() {
    return _desiredStateInEffect;
  }

  @Override
  public String toString() {
    return "EffectiveDisabledPartitions{disabledPartitions=" + _disabledPartitions
        + ", crossResourceDisabledPartitions=" + _crossResourceDisabledPartitions
        + ", desiredStateInEffect=" + _desiredStateInEffect + "}";
  }
}
