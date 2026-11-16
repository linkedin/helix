package org.apache.helix.controller;

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

import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import org.apache.helix.HelixDataAccessor;
import org.apache.helix.HelixException;
import org.apache.helix.HelixManager;
import org.apache.helix.PropertyKey;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Deletes /INSTANCES/{instance} subtrees left without an InstanceConfig by a dropInstance that
 * failed between deleting the config and deleting the subtree. The leader controller runs it every
 * {@link #COLLECTION_INTERVAL_MS} on its own thread, independent of cluster events and pipelines.
 * Leftovers are found from ZK, so those left by any caller (REST, ClusterSetup, purge, participant
 * deregistration) are cleaned up, including after a controller failover.
 *
 * <p>A leftover is deleted only once it has been seen for {@link #GARBAGE_GRACE_PERIOD_MS}, so a
 * drop that is still in flight finishes on its own. The delete itself is
 * {@link org.apache.helix.HelixAdmin#deleteDroppedInstanceGarbage}, which leaves alone an
 * instance that has been re-added or has come live meanwhile.
 */
class InstanceGarbageCollector {
  private static final Logger LOG = LoggerFactory.getLogger(InstanceGarbageCollector.class);
  static final long COLLECTION_INTERVAL_MS = 60 * 1000L;
  static final long GARBAGE_GRACE_PERIOD_MS = 60 * 1000L;

  // When each current leftover was first seen. Kept in memory only: after a failover the new leader
  // finds the leftovers again from ZK and waits one more grace period.
  private final Map<String, Long> _garbageFirstSeenTimes = new ConcurrentHashMap<>();

  /**
   * Finds the leftover instance subtrees in the manager's cluster and deletes those seen for at
   * least {@link #GARBAGE_GRACE_PERIOD_MS}. Does nothing unless the manager is the connected
   * leader.
   */
  void collect(HelixManager manager) {
    if (!manager.isConnected() || !manager.isLeader()) {
      reset();
      return;
    }
    String clusterName = manager.getClusterName();
    HelixDataAccessor accessor = manager.getHelixDataAccessor();
    PropertyKey.Builder keyBuilder = accessor.keyBuilder();
    Set<String> garbageInstances = new HashSet<>(accessor.getChildNames(keyBuilder.instances()));
    garbageInstances.removeAll(accessor.getChildNames(keyBuilder.instanceConfigs()));
    garbageInstances.removeAll(accessor.getChildNames(keyBuilder.liveInstances()));
    _garbageFirstSeenTimes.keySet().retainAll(garbageInstances);

    long now = getCurrentTime();
    for (String instanceName : garbageInstances) {
      long firstSeenTime = _garbageFirstSeenTimes.computeIfAbsent(instanceName, name -> now);
      if (now - firstSeenTime < GARBAGE_GRACE_PERIOD_MS) {
        continue;
      }
      try {
        manager.getClusterManagmentTool().deleteDroppedInstanceGarbage(clusterName, instanceName);
        _garbageFirstSeenTimes.remove(instanceName);
        LOG.info("Deleted leftover subtree of instance {} in cluster {}, left by a partial "
            + "dropInstance", instanceName, clusterName);
      } catch (HelixException e) {
        // Also thrown if the instance was re-added or came live since it was listed; the next run
        // then no longer finds it as a leftover.
        LOG.warn("Did not delete leftover subtree of instance {} in cluster {}, will retry",
            instanceName, clusterName, e);
      }
    }
  }

  /**
   * Forgets every leftover seen so far, so each one waits a full grace period again. Called when
   * the collection starts or stops, e.g. on a leadership change.
   */
  void reset() {
    _garbageFirstSeenTimes.clear();
  }

  // Overridden in tests.
  long getCurrentTime() {
    return System.currentTimeMillis();
  }
}
