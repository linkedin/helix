package org.apache.helix.controller.rebalancer.util;

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

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import org.apache.helix.HelixRebalanceException;
import org.apache.helix.controller.rebalancer.waged.RebalanceAlgorithm;
import org.apache.helix.controller.rebalancer.waged.model.ClusterModel;
import org.apache.helix.controller.rebalancer.waged.model.OptimalAssignment;
import org.apache.helix.model.ClusterConfig;
import org.apache.helix.model.Partition;
import org.apache.helix.model.ResourceAssignment;
import org.apache.helix.model.ResourceConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class WagedRebalanceUtil {

  private static final Logger LOG = LoggerFactory.getLogger(WagedRebalanceUtil.class);

  /**
   * @param clusterModel the cluster model that contains all the cluster status for the purpose of
   *                     rebalancing.
   * @return the new optimal assignment for the resources.
   */
  public static Map<String, ResourceAssignment> calculateAssignment(ClusterModel clusterModel,
      RebalanceAlgorithm algorithm) throws HelixRebalanceException {
    return calculateAssignment(clusterModel, algorithm, null);
  }

  /**
   * Same as {@link #calculateAssignment(ClusterModel, RebalanceAlgorithm)}, but carries the previous
   * assignment forward for any resource that the algorithm skipped.
   *
   * The algorithm only skips resources when instance-tag ("clique") isolation is enabled and one
   * instance-group-tag could not be fully placed. Copying the previous assignment for those
   * resources keeps the returned map complete, so every downstream consumer (the metadata store
   * blobs, the baseline divergence metric, the ideal state conversion) behaves exactly as it does in
   * the default global mode, where a failure aborts the whole calculation instead.
   *
   * @param previousAssignment the assignment this phase started from, in instance-name view. Pass
   *                           null when a skipped resource should simply be absent from the result,
   *                           which is the right behavior for the delayed rebalance overwrite phase
   *                           because an absent resource there means "no overwrite applied".
   * @return the new optimal assignment for the resources.
   */
  public static Map<String, ResourceAssignment> calculateAssignment(ClusterModel clusterModel,
      RebalanceAlgorithm algorithm, Map<String, ResourceAssignment> previousAssignment)
      throws HelixRebalanceException {
    long startTime = System.currentTimeMillis();
    LOG.info("Start calculating for an assignment with algorithm {}",
        algorithm.getClass().getSimpleName());
    OptimalAssignment optimalAssignment = algorithm.calculate(clusterModel);
    Map<String, ResourceAssignment> newAssignment =
        optimalAssignment.getOptimalResourceAssignment();
    Set<String> skippedResources = optimalAssignment.getSkippedResources();
    if (!skippedResources.isEmpty()) {
      skippedResources = new HashSet<>(skippedResources);
      // A skipped resource may still have a partial entry in the result: in the partial, emergency
      // and delayed overwrite phases the nodes are pre-loaded with the replicas that were already
      // allocated, and updateAssignments emits whatever sits on the nodes. Replace that partial
      // entry with the complete previous assignment, or drop it, so no resource is ever emitted
      // half assigned.
      newAssignment = new HashMap<>(newAssignment);
      List<String> carriedOver = new ArrayList<>();
      List<String> dropped = new ArrayList<>();
      for (String resource : skippedResources) {
        if (carryForwardOrDrop(newAssignment, resource, previousAssignment)) {
          carriedOver.add(resource);
        } else {
          dropped.add(resource);
        }
      }
      List<String> yielded =
          resolveCarriedOverNodeReuse(newAssignment, skippedResources, previousAssignment,
              carriedOver, dropped);
      skippedResources.addAll(yielded);
      LOG.warn(
          "Instance tag isolation skipped {} resource(s) during the {} rebalance of cluster {}. "
              + "Carried the previous assignment forward for {}. Left out of this phase's result: "
              + "{}.", skippedResources.size(), clusterModel.getRebalanceScopeType(),
          clusterModel.getContext().getClusterName(), carriedOver, dropped);
      if (!yielded.isEmpty()) {
        LOG.warn(
            "Instance tag isolation also carried {} forward during the {} rebalance of cluster {} "
                + "because a previously "
                + "skipped group's assignment still names an instance that these resources were "
                + "just assigned to. This happens when an instance is retagged out of a group while "
                + "that group cannot be placed. Only the groups that actually collide give up their "
                + "freshly calculated assignment; every other group keeps its own.", yielded,
            clusterModel.getRebalanceScopeType(),
            clusterModel.getContext().getClusterName());
      }
    }
    algorithm.onAssignmentComputed(clusterModel.getRebalanceScopeType(),
        clusterModel.getAssignableReplicaMap().keySet(),
        Collections.unmodifiableSet(skippedResources));
    LOG.info("Finish calculating an assignment with algorithm {}. Took: {} ms.",
        algorithm.getClass().getSimpleName(), System.currentTimeMillis() - startTime);
    return newAssignment;
  }

  /**
   * Carry a group forward as well when it was just assigned an instance that an already carried
   * over group still names, and repeat until nothing collides.
   *
   * The share block partition in InstanceTagIsolation proves that no other group can reach the
   * failing group's nodes, but it reasons about the tags instances carry now, while the carried
   * over assignment reflects where replicas were placed before. If an instance is retagged out of a
   * group while that group is broken, the group's previous assignment still names it, and the group
   * that now owns it would be free to place there. Persisting both would overcommit the instance,
   * which is exactly what the partition exists to prevent.
   *
   * The colliding group is therefore carried forward too, which is the same mechanism the mode
   * already uses, rather than failing the whole rebalance. Its previous assignment predates the
   * retag, so it cannot name the disputed instance, and the collision is resolved by giving up one
   * group's fresh result instead of every group's. Groups that do not collide keep their freshly
   * calculated assignment and keep converging.
   *
   * Carrying one group forward can expose a second collision, when another instance moved between
   * two other groups, so the check repeats. Every round moves at least one more resource into the
   * carried over set and never moves one back, so it terminates. In the worst case every group ends
   * up carried forward, which is exactly the cluster wide fallback that would have happened anyway.
   *
   * This compares emitted instance names rather than tags, so it also covers divergences the
   * partition cannot see, such as a node whose capacity or operation changed since the carried over
   * assignment was computed. A shared name is treated as a conflict even when the instance had room
   * for both, which is deliberately conservative: in the tag partitioned deployments this mode is
   * for, an instance is owned by one group, so a shared name is a real overcommit. If a topology
   * shares instances widely enough for that to cascade, the worst case is that every group ends up
   * carried forward, which reproduces the previous assignment exactly and therefore persists
   * nothing.
   *
   * Carrying groups forward is only sound because every carried entry is drawn from the same
   * previousAssignment map, which is one internally coherent snapshot that was feasible when it was
   * written. Two carried entries therefore cannot overcommit each other, and only carried against
   * fresh has to be resolved here. A caller that passed a stitched together previous assignment
   * would break that invariant.
   *
   * @return the resources that gave up their freshly calculated assignment, in the order they did.
   */
  private static List<String> resolveCarriedOverNodeReuse(
      Map<String, ResourceAssignment> assignment, Set<String> skippedResources,
      Map<String, ResourceAssignment> previousAssignment, List<String> carriedOver,
      List<String> dropped) {
    Set<String> carriedResources = new LinkedHashSet<>(skippedResources);
    List<String> yielded = new ArrayList<>();
    String colliding;
    while ((colliding = findResourceReusingCarriedNode(assignment, carriedResources)) != null) {
      if (carryForwardOrDrop(assignment, colliding, previousAssignment)) {
        carriedOver.add(colliding);
      } else {
        dropped.add(colliding);
      }
      carriedResources.add(colliding);
      yielded.add(colliding);
    }
    return yielded;
  }

  /**
   * @return the first freshly calculated resource that names an instance some carried over resource
   *         also names, or null when nothing collides.
   */
  private static String findResourceReusingCarriedNode(Map<String, ResourceAssignment> assignment,
      Set<String> carriedResources) {
    Set<String> carriedInstances = new TreeSet<>();
    for (String resource : carriedResources) {
      ResourceAssignment carried = assignment.get(resource);
      if (carried != null) {
        carriedInstances.addAll(instancesOf(carried));
      }
    }
    if (carriedInstances.isEmpty()) {
      return null;
    }
    // Sorted so that a cluster hitting several conflicts at once always resolves them in the same
    // order, which keeps the emitted assignment stable across controller failovers.
    for (String resource : new TreeSet<>(assignment.keySet())) {
      if (carriedResources.contains(resource)) {
        continue;
      }
      ResourceAssignment fresh = assignment.get(resource);
      if (fresh == null) {
        continue;
      }
      for (String instance : instancesOf(fresh)) {
        if (carriedInstances.contains(instance)) {
          return resource;
        }
      }
    }
    return null;
  }

  /**
   * Replace a resource's entry with the assignment this phase started from, or remove it when there
   * was nothing before it, so no resource is ever emitted half assigned.
   *
   * @return true when the previous assignment was carried forward, false when the entry was dropped.
   */
  private static boolean carryForwardOrDrop(Map<String, ResourceAssignment> assignment,
      String resource, Map<String, ResourceAssignment> previousAssignment) {
    ResourceAssignment previous =
        previousAssignment == null ? null : previousAssignment.get(resource);
    if (previous == null) {
      assignment.remove(resource);
      return false;
    }
    // Deep copy so the result never aliases the caller's previous assignment objects.
    assignment.put(resource, new ResourceAssignment(previous.getRecord()));
    return true;
  }

  private static Set<String> instancesOf(ResourceAssignment resourceAssignment) {
    Set<String> instances = new TreeSet<>();
    for (Partition partition : resourceAssignment.getMappedPartitions()) {
      instances.addAll(resourceAssignment.getReplicaMap(partition).keySet());
    }
    return instances;
  }

  /**
   * Parse the resource config for the partition weight.
   */
  public static Map<String, Integer> fetchCapacityUsage(String partitionName,
      ResourceConfig resourceConfig, ClusterConfig clusterConfig) {
    Map<String, Map<String, Integer>> capacityMap;
    try {
      capacityMap = resourceConfig == null ? new HashMap<>() : resourceConfig.getPartitionCapacityMap();
    } catch (IOException ex) {
      throw new IllegalArgumentException(
          "Invalid partition capacity configuration of resource: " + resourceConfig
              .getResourceName(), ex);
    }
    Map<String, Integer> partitionCapacity = WagedValidationUtil
        .validateAndGetPartitionCapacity(partitionName, resourceConfig, capacityMap, clusterConfig);
    // Remove the non-required capacity items.
    partitionCapacity.keySet().retainAll(clusterConfig.getInstanceCapacityKeys());
    return partitionCapacity;
  }
}
