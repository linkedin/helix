package org.apache.helix.controller.stages;

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

import java.util.List;

import org.apache.helix.AccessOption;
import org.apache.helix.HelixManager;
import org.apache.helix.PropertyPathBuilder;
import org.apache.helix.controller.dataproviders.ResourceControllerDataProvider;
import org.apache.helix.controller.pipeline.AbstractAsyncBaseStage;
import org.apache.helix.controller.pipeline.AsyncWorkerType;
import org.apache.helix.model.ClusterConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Cleans up orphaned instance runtime paths when InstanceConfig is deleted but the instance
 * subtree under INSTANCES remains.
 */
public class OrphanedInstanceCleanupStage extends AbstractAsyncBaseStage {
  private static final Logger LOG = LoggerFactory.getLogger(OrphanedInstanceCleanupStage.class);

  @Override
  public AsyncWorkerType getAsyncWorkerType() {
    return AsyncWorkerType.OrphanedInstanceCleanupWorker;
  }

  @Override
  public void execute(final ClusterEvent event) {
    HelixManager manager = event.getAttribute(AttributeName.helixmanager.name());
    ResourceControllerDataProvider cache =
        event.getAttribute(AttributeName.ControllerDataProvider.name());
    if (manager == null || cache == null) {
      return;
    }

    ClusterConfig clusterConfig = cache.getClusterConfig();
    if (clusterConfig == null || !clusterConfig.isAsyncInstanceDropEnabled()) {
      return;
    }

    String clusterName = cache.getClusterName();

    try {
      List<String> instanceNames = manager.getHelixDataAccessor().getBaseDataAccessor()
          .getChildNames(PropertyPathBuilder.instance(clusterName), AccessOption.PERSISTENT);
      if (instanceNames == null || instanceNames.isEmpty()) {
        return;
      }

      for (String instanceName : instanceNames) {
        String instanceConfigPath = PropertyPathBuilder.instanceConfig(clusterName, instanceName);
        if (manager.getHelixDataAccessor().getBaseDataAccessor()
            .exists(instanceConfigPath, AccessOption.PERSISTENT)) {
          continue;
        }

        String liveInstancePath = PropertyPathBuilder.liveInstance(clusterName, instanceName);
        if (manager.getHelixDataAccessor().getBaseDataAccessor()
            .exists(liveInstancePath, AccessOption.PERSISTENT)) {
          LOG.warn("Found live instance {} without InstanceConfig in cluster {}. Skip orphan cleanup.",
              instanceName, clusterName);
          continue;
        }

        String instancePath = PropertyPathBuilder.instance(clusterName, instanceName);
        LOG.info("Cleaning up orphaned instance path {}", instancePath);
        boolean removed = manager.getHelixDataAccessor().getBaseDataAccessor()
            .remove(instancePath, AccessOption.PERSISTENT);
        if (removed) {
          LOG.info("Removed orphaned instance path {} in cluster {}.", instancePath, clusterName);
        } else {
          LOG.warn("Failed to remove orphaned instance path {} in cluster {}. Will retry.", instancePath, clusterName);
        }
      }
    } catch (Exception e) {
      LOG.warn("OrphanedInstanceCleanupStage failed for cluster {}.", clusterName, e);
    }
  }
}
