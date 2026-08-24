package org.apache.helix.integration.manager;

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

import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.List;

import org.apache.helix.HelixManager;
import org.apache.helix.PropertyKey;
import org.apache.helix.manager.zk.CallbackHandler;
import org.apache.helix.manager.zk.ZKHelixDataAccessor;
import org.apache.helix.manager.zk.ZKHelixManager;
import org.apache.helix.manager.zk.ZkBaseDataAccessor;
import org.apache.helix.model.LiveInstance;
import org.apache.helix.zookeeper.api.client.HelixZkClient;
import org.testng.Assert;

/**
 * Shared assertions/introspection for the per-instance listener registration tests
 * (embedded-ZK {@link TestFailoverPerInstanceListenerRegistration} and real-ZK
 * {@link TestRealZkFailoverWatches}). Keeps the reflective handler counting and leader/live-instance
 * lookups in one place instead of duplicating them per test class.
 */
final class PerInstanceListenerTestSupport {

  private PerInstanceListenerTestSupport() {
  }

  /**
   * Number of per-instance {@code /<cluster>/INSTANCES/<inst>/CURRENTSTATES...} callback handlers a
   * manager currently holds - i.e. the count of live-instance current-state watches. Reflective
   * because the handler list is internal to {@link ZKHelixManager}.
   */
  @SuppressWarnings("unchecked")
  static int countCurrentStateHandlers(HelixManager mgr, String clusterName) throws Exception {
    Field f = ZKHelixManager.class.getDeclaredField("_handlers");
    f.setAccessible(true);
    List<CallbackHandler> handlers = (List<CallbackHandler>) f.get(mgr);
    int count = 0;
    synchronized (mgr) {
      for (CallbackHandler h : new ArrayList<>(handlers)) {
        String p = h.getPath();
        if (p.contains("/" + clusterName + "/INSTANCES/") && p.contains("/CURRENTSTATES")) {
          count++;
        }
      }
    }
    return count;
  }

  /** Number of live instances in the cluster (one per required per-instance CURRENTSTATES watch). */
  static int liveInstanceCount(HelixZkClient zkClient, String clusterName) {
    ZKHelixDataAccessor accessor =
        new ZKHelixDataAccessor(clusterName, new ZkBaseDataAccessor<>(zkClient));
    List<String> live = accessor.getChildNames(accessor.keyBuilder().liveInstances());
    return live == null ? 0 : live.size();
  }

  /** The {@link HelixManager} among {@code controllers} that currently holds cluster leadership. */
  static HelixManager currentLeader(HelixZkClient zkClient, HelixManager[] controllers,
      String clusterName) {
    ZKHelixDataAccessor accessor =
        new ZKHelixDataAccessor(clusterName, new ZkBaseDataAccessor<>(zkClient));
    PropertyKey.Builder kb = accessor.keyBuilder();
    LiveInstance leader = accessor.getProperty(kb.controllerLeader());
    Assert.assertNotNull(leader, "no controller leader");
    for (HelixManager c : controllers) {
      if (c.getInstanceName().equals(leader.getId())) {
        return c;
      }
    }
    Assert.fail("leader " + leader.getId() + " not among controllers");
    return null;
  }
}
