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
import java.util.Map;
import java.util.Set;
import org.apache.helix.HelixRebalanceException;
import org.apache.helix.controller.rebalancer.waged.RebalanceAlgorithm;
import org.apache.helix.controller.rebalancer.waged.model.ClusterModel;
import org.apache.helix.controller.rebalancer.waged.model.OptimalAssignment;
import org.apache.helix.model.ClusterConfig;
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
      // A skipped resource may still have a partial entry in the result: in the partial, emergency
      // and delayed overwrite phases the nodes are pre-loaded with the replicas that were already
      // allocated, and updateAssignments emits whatever sits on the nodes. Replace that partial
      // entry with the complete previous assignment, or drop it, so no resource is ever emitted
      // half assigned.
      newAssignment = new HashMap<>(newAssignment);
      List<String> carriedOver = new ArrayList<>();
      List<String> dropped = new ArrayList<>();
      for (String resource : skippedResources) {
        ResourceAssignment previous =
            previousAssignment == null ? null : previousAssignment.get(resource);
        if (previous == null) {
          newAssignment.remove(resource);
          dropped.add(resource);
        } else {
          // Deep copy so the result never aliases the caller's previous assignment objects.
          newAssignment.put(resource, new ResourceAssignment(previous.getRecord()));
          carriedOver.add(resource);
        }
      }
      LOG.warn(
          "Instance tag isolation skipped {} resource(s) during the {} rebalance of cluster {}. "
              + "Carried the previous assignment forward for {}. Left out of this phase's result: "
              + "{}.", skippedResources.size(), clusterModel.getRebalanceScopeType(),
          clusterModel.getContext().getClusterName(), carriedOver, dropped);
    }
    LOG.info("Finish calculating an assignment with algorithm {}. Took: {} ms.",
        algorithm.getClass().getSimpleName(), System.currentTimeMillis() - startTime);
    return newAssignment;
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
