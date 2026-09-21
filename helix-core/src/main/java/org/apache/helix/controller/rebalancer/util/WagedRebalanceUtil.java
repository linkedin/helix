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
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
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
   * Same as {@link #calculateAssignment(ClusterModel, RebalanceAlgorithm)}, but carries the
   * previous assignment forward for any resource that the algorithm skipped.
   *
   * The algorithm only skips resources when instance-tag ("clique") isolation is enabled and sets
   * their share block aside. Copying the previous assignment for those resources means no resource
   * is emitted half assigned, so every downstream consumer (the metadata store blobs, the baseline
   * divergence metric, the ideal state conversion) only sees whole resource entries, as in the
   * default global mode, where a failure aborts the whole calculation instead. A skipped resource
   * with no previous assignment, such as a new one no participant hosts yet, is left out.
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
        algorithm.getName());
    OptimalAssignment optimalAssignment = algorithm.calculate(clusterModel);
    Map<String, ResourceAssignment> newAssignment =
        optimalAssignment.getOptimalResourceAssignment();
    Set<String> skippedResources = optimalAssignment.getSkippedResources();
    if (!skippedResources.isEmpty()) {
      skippedResources = new HashSet<>(skippedResources);
      // A skipped resource may still have a partial entry in the result: an incremental baseline
      // and the partial, emergency and delayed overwrite phases pre-load the nodes with the
      // replicas that were already allocated, and updateAssignments emits whatever sits on the
      // nodes. Replace that partial entry with the complete previous assignment, or drop it, so
      // no resource is ever emitted half assigned.
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
                + "because a previously skipped resource's assignment still names an instance that "
                + "these resources were just assigned to. This happens when an instance is "
                + "retagged out of a group while that group is skipped. Only the resources that "
                + "actually collide give up their freshly calculated assignment; every other "
                + "resource keeps its own.", yielded,
            clusterModel.getRebalanceScopeType(),
            clusterModel.getContext().getClusterName());
      }
    }
    algorithm.onAssignmentComputed(clusterModel.getRebalanceScopeType(),
        clusterModel.getAssignableReplicaMap().keySet(),
        Collections.unmodifiableSet(skippedResources));
    LOG.info("Finish calculating an assignment with algorithm {}. Took: {} ms.",
        algorithm.getName(), System.currentTimeMillis() - startTime);
    return newAssignment;
  }

  /**
   * Carry a resource forward as well when it was just assigned an instance that an already carried
   * over resource still names, and repeat until nothing collides.
   *
   * The share block partition in InstanceTagIsolation proves that no group outside a failed block
   * can reach the block's nodes, but it reasons about the tags instances carry now, while the
   * carried over assignment reflects where replicas were placed before. If an instance is retagged
   * out of a group in a failed block, the group's previous assignment still names it, and the group
   * that now owns it would be free to place there. Persisting both would overcommit the instance,
   * which is exactly what the partition exists to prevent.
   *
   * The colliding resource is therefore carried forward too, which is the same mechanism the mode
   * already uses, rather than failing the whole rebalance, and the collision is resolved by giving
   * up one resource's fresh result instead of every resource's. Its previous assignment usually
   * predates the retag and so does not name the disputed instance. When it does, for example
   * because the instance already carried this resource's tag before the retag, or because that
   * entry was itself filled in or carried over, the two carried over entries are left as they are
   * (see below). Resources that do not collide keep their freshly calculated assignment and keep
   * converging.
   *
   * Carrying one resource forward can expose a second collision, when another instance moved
   * between two other groups, so the check repeats. Every round moves at least one more resource
   * into the carried over set and never moves one back, so it terminates. In the worst case every
   * resource ends up carried forward, which keeps the previous assignment, much as the default
   * mode keeps its last known good assignment when a calculation fails.
   *
   * This compares emitted instance names rather than tags, so it also covers divergences the
   * partition cannot see, such as a retagged resource or a carried entry filled in from current
   * states. A shared name is treated as a conflict even when the instance had room
   * for both, which is deliberately conservative: in the tag partitioned deployments this mode is
   * for, an instance is owned by one group, so a shared name is a real overcommit. If a topology
   * shares instances widely enough for that to cascade, the worst case is that every resource ends
   * up carried forward, which reproduces the previous assignment, so no newly calculated placement
   * is persisted.
   *
   * Only carried against fresh is resolved here, never carried against carried, and the carried
   * entries are not one snapshot. The callers take the persisted baseline or best possible
   * assignment and fill in current states for the resources it does not hold yet (see
   * AssignmentManager). The persisted entries were computed together and fit together, and a filled
   * in entry is what the participants are running now, so carrying either kind forward moves no
   * replica. Two carried entries can still name one instance for more than it holds, for example
   * when what is running has not caught up with the persisted assignment, or when a capacity
   * changed since either was written. That is accepted because it is no worse than the default
   * mode, which fails the whole rebalance in the same situation: the served best possible only
   * keeps replicas where they already run or where the Helix controller was already driving them,
   * every freshly calculated placement still passes the node capacity constraint, and no instance a
   * carried entry names is left to a fresh placement. In the global baseline scope the worst case
   * is a persisted baseline that overcommits an instance. The partial rebalance treats the baseline
   * as a soft goal, so it never places past capacity to reach it, and the next baseline calculation
   * replaces it.
   *
   * @return the resources that gave up their freshly calculated assignment, in the order they did.
   */
  private static List<String> resolveCarriedOverNodeReuse(
      Map<String, ResourceAssignment> assignment, Set<String> skippedResources,
      Map<String, ResourceAssignment> previousAssignment, List<String> carriedOver,
      List<String> dropped) {
    Set<String> carriedResources = new LinkedHashSet<>(skippedResources);
    List<String> yielded = new ArrayList<>();

    // Index the freshly calculated resources by the instances they name. Rebuilding the carried
    // instance set and rescanning every resource on every round is quadratic in the resource count,
    // which on a large cluster with a broken clique and a retag storm costs seconds of pipeline
    // stall on every tick. The carried set only ever grows, so both it and the collisions it
    // exposes can be maintained incrementally instead.
    Map<String, List<String>> freshByInstance = new HashMap<>();
    for (Map.Entry<String, ResourceAssignment> entry : assignment.entrySet()) {
      if (carriedResources.contains(entry.getKey())) {
        continue;
      }
      for (String instance : instancesOf(entry.getValue())) {
        freshByInstance.computeIfAbsent(instance, key -> new ArrayList<>()).add(entry.getKey());
      }
    }

    // The carried set this converges to does not depend on the order, because carrying a resource
    // only adds instances that a freshly calculated resource must not name. Sorting only keeps
    // the yielded list, and the warnings that print it, in the same order on every run,
    // including after a Helix controller failover.
    TreeSet<String> colliding = new TreeSet<>();
    Set<String> seenInstances = new HashSet<>();
    for (String resource : carriedResources) {
      collectCollisions(assignment.get(resource), freshByInstance, seenInstances, colliding,
          carriedResources);
    }

    while (!colliding.isEmpty()) {
      String resource = colliding.pollFirst();
      if (carriedResources.contains(resource)) {
        continue;
      }
      if (carryForwardOrDrop(assignment, resource, previousAssignment)) {
        carriedOver.add(resource);
      } else {
        dropped.add(resource);
      }
      carriedResources.add(resource);
      yielded.add(resource);
      // The entry now holds the previous assignment, which names a different set of instances and
      // can therefore expose a further collision.
      collectCollisions(assignment.get(resource), freshByInstance, seenInstances, colliding,
          carriedResources);
    }
    return yielded;
  }

  /**
   * Add every freshly calculated resource that names an instance of the just carried resource to
   * the pending collision set.
   *
   * Each instance is expanded at most once for the whole run, which is what keeps the resolution
   * linear in the number of instance mentions rather than quadratic in the resource count.
   */
  private static void collectCollisions(ResourceAssignment carried,
      Map<String, List<String>> freshByInstance, Set<String> seenInstances,
      Set<String> colliding, Set<String> carriedResources) {
    if (carried == null) {
      return;
    }
    for (String instance : instancesOf(carried)) {
      if (!seenInstances.add(instance)) {
        continue;
      }
      for (String resource : freshByInstance.getOrDefault(instance, Collections.emptyList())) {
        if (!carriedResources.contains(resource)) {
          colliding.add(resource);
        }
      }
    }
  }

  /**
   * Replace a resource's entry with the assignment this phase started from, or remove it when there
   * was nothing before it, so no resource is ever emitted half assigned.
   *
   * @return true when the previous assignment was carried forward, false when the entry was
   *         dropped.
   */
  private static boolean carryForwardOrDrop(Map<String, ResourceAssignment> assignment,
      String resource, Map<String, ResourceAssignment> previousAssignment) {
    ResourceAssignment previous =
        previousAssignment == null ? null : previousAssignment.get(resource);
    if (previous == null) {
      assignment.remove(resource);
      return false;
    }
    // Deep copy so the result never aliases the caller's previous assignment objects. The ZNRecord
    // copy constructor copies the outer partition map but shares each per partition replica map by
    // reference, so without replacing them a write through the returned assignment would land in
    // the caller's previous snapshot.
    ResourceAssignment carried = new ResourceAssignment(previous.getRecord());
    Map<String, Map<String, String>> replicaMaps = new HashMap<>();
    carried.getRecord().getMapFields()
        .forEach((partition, replicas) -> replicaMaps.put(partition, new HashMap<>(replicas)));
    carried.getRecord().setMapFields(replicaMaps);
    assignment.put(resource, carried);
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
