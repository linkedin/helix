package org.apache.helix.constants;

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
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * Types of exclusions that can be applied when checking if evacuation is finished.
 * These exclusions allow certain resources or partitions to be ignored when determining
 * if an instance has completed evacuation.
 */
public enum EvacuateExclusionType {
  /**
   * Exclude resources that are disabled from blocking evacuation completion
   */
  DISABLED_RESOURCE,

  /**
   * Exclude partitions that are in ERROR state from blocking evacuation completion
   */
  ERROR_PARTITIONS,

  /**
   * Exclude partitions that are disabled from blocking evacuation completion
   */
  DISABLED_PARTITION;

  /**
   * Parse a comma-separated string of exclusion types into a Set
   * @param exclusionStr comma-separated string of exclusion types (e.g., "DISABLED_RESOURCE,ERROR_PARTITIONS")
   * @return Set of EvacuateExclusionType enums
   */
  public static Set<EvacuateExclusionType> parseExclusionTypes(String exclusionStr) {
    if (exclusionStr == null || exclusionStr.trim().isEmpty()) {
      return Collections.emptySet();
    }
    return Stream.of(exclusionStr.split(","))
        .map(String::trim)
        .filter(s -> !s.isEmpty())
        .map(EvacuateExclusionType::valueOf)
        .collect(Collectors.toSet());
  }
}
