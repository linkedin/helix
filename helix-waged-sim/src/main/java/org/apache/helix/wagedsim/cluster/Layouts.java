package org.apache.helix.wagedsim.cluster;

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

import java.util.HashMap;
import java.util.Map;
import java.util.TreeMap;

import org.apache.helix.model.Partition;
import org.apache.helix.model.ResourceAssignment;

/**
 * Helpers for layouts: resource -> partition -> instance -> state. Used for the served layout, the
 * baseline and the best possible assignment.
 */
public final class Layouts {
  private Layouts() {
  }

  public static Map<String, Map<String, Map<String, String>>> copy(
      Map<String, Map<String, Map<String, String>>> layout) {
    if (layout == null) {
      return null;
    }
    Map<String, Map<String, Map<String, String>>> copy = new TreeMap<>();
    layout.forEach((resource, partitions) -> {
      Map<String, Map<String, String>> partitionCopy = new TreeMap<>();
      partitions.forEach((partition, replicas) -> partitionCopy.put(partition, new TreeMap<>(replicas)));
      copy.put(resource, partitionCopy);
    });
    return copy;
  }

  public static Map<String, ResourceAssignment> toAssignments(
      Map<String, Map<String, Map<String, String>>> layout) {
    Map<String, ResourceAssignment> result = new HashMap<>();
    if (layout == null) {
      return result;
    }
    layout.forEach((resource, partitions) -> {
      ResourceAssignment assignment = new ResourceAssignment(resource);
      partitions.forEach((partition, replicas) ->
          assignment.addReplicaMap(new Partition(partition), new TreeMap<>(replicas)));
      result.put(resource, assignment);
    });
    return result;
  }

  public static Map<String, Map<String, Map<String, String>>> fromAssignments(
      Map<String, ResourceAssignment> assignments) {
    Map<String, Map<String, Map<String, String>>> layout = new TreeMap<>();
    if (assignments == null) {
      return layout;
    }
    assignments.forEach((resource, assignment) -> {
      Map<String, Map<String, String>> partitions = new TreeMap<>();
      for (Partition partition : assignment.getMappedPartitions()) {
        partitions.put(partition.getPartitionName(), new TreeMap<>(assignment.getReplicaMap(partition)));
      }
      layout.put(resource, partitions);
    });
    return layout;
  }

  /** @return the number of replicas in the layout */
  public static long replicaCount(Map<String, Map<String, Map<String, String>>> layout) {
    if (layout == null) {
      return 0;
    }
    return layout.values().stream().flatMap(p -> p.values().stream()).mapToLong(Map::size).sum();
  }
}
