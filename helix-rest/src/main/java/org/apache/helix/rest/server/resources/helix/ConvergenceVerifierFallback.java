package org.apache.helix.rest.server.resources.helix;

/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.helix.HelixDataAccessor;
import org.apache.helix.PropertyKey;
import org.apache.helix.constants.InstanceConstants.InstanceOperation;
import org.apache.helix.controller.rebalancer.util.DelayedRebalanceUtil;
import org.apache.helix.manager.zk.ZKHelixDataAccessor;
import org.apache.helix.manager.zk.ZkBaseDataAccessor;
import org.apache.helix.model.ClusterConfig;
import org.apache.helix.model.IdealState;
import org.apache.helix.model.InstanceConfig;
import org.apache.helix.tools.ClusterVerifiers.StrictMatchExternalViewVerifier;
import org.apache.helix.zookeeper.api.client.RealmAwareZkClient;

/**
 * What REST can say when the controller's report is stale: whether the external view matches the
 * assignment the controller persisted in the ideal states, which is what
 * {@link StrictMatchExternalViewVerifier} checks. A match is weak evidence, since a stuck
 * controller also leaves its ideal states behind; a mismatch is trustworthy. The check reads the
 * whole cluster, so it runs off the request thread, once per cluster at a time, and the answer is
 * cached; a resource is answered from its cluster's check. A failed read is UNAVAILABLE, never a
 * mismatch.
 */
final class ConvergenceVerifierFallback {
  static final String SOURCE = "VERIFIER";
  static final String MATCH = "MATCH";
  static final String MISMATCH = "MISMATCH";
  static final String UNAVAILABLE = "UNAVAILABLE";
  static final String VERIFYING = "VERIFYING";

  private static final long CACHE_MS = 60_000L;
  private static final long WAIT_MS = 3_000L;
  private static final int MAX_CACHED = 1_000;
  private static final String HELD = "Cannot be determined: a replica is held for an offline or "
      + "disabled node by the delayed rebalance, so a matching external view does not show it is "
      + "restored.";

  /** One check of a whole cluster. */
  static final class Check {
    final boolean _match;
    final Set<String> _held;

    Check(boolean match, Set<String> held) {
      _match = match;
      _held = held;
    }
  }

  private static final class Entry {
    final CompletableFuture<Check> _result = new CompletableFuture<>();
    volatile long _doneAt;
  }

  private final Map<String, Entry> _cache = new ConcurrentHashMap<>();
  private final ExecutorService _executor = newExecutor();

  private static ExecutorService newExecutor() {
    ThreadPoolExecutor executor = new ThreadPoolExecutor(2, 2, 60L, TimeUnit.SECONDS,
        new ArrayBlockingQueue<>(16), runnable -> {
          Thread thread = new Thread(runnable, "convergence-verifier-fallback");
          thread.setDaemon(true);
          return thread;
        });
    // No idle threads are kept when nobody asks for a fallback.
    executor.allowCoreThreadTimeOut(true);
    return executor;
  }

  static final ConvergenceVerifierFallback INSTANCE = new ConvergenceVerifierFallback();

  /**
   * Adds the {@code fallback} answer to a response whose report is stale, only when the caller
   * asked for it: the check reads the whole cluster from ZooKeeper, so it is never run by default.
   */
  ObjectNode addTo(ObjectNode response, boolean requested, RealmAwareZkClient zkClient,
      ClusterConfig config, String clusterId, String resource) {
    String reportState = response.path("reportState").asText();
    if (requested && ConvergenceStatusResponseMapper.REPORT_STALE.equals(reportState)) {
      response.set("fallback", check(zkClient, config, clusterId, resource));
    }
    return response;
  }

  /** @param resource null for the whole cluster */
  private ObjectNode check(RealmAwareZkClient zkClient, ClusterConfig config, String clusterId,
      String resource) {
    if (config == null || !(config.isPersistBestPossibleAssignment()
        || config.isPersistIntermediateAssignment())) {
      return answer(UNAVAILABLE, "Cannot be determined: the cluster does not persist its best "
          + "possible assignment (PERSIST_BEST_POSSIBLE_ASSIGNMENT).");
    }
    String key = System.identityHashCode(zkClient) + "/" + clusterId;
    long now = System.currentTimeMillis();
    boolean[] created = {false};
    Entry entry = _cache.compute(key, (k, old) -> {
      if (old != null && (old._doneAt == 0 || now - old._doneAt < CACHE_MS)) {
        return old;
      }
      created[0] = true;
      return new Entry();
    });
    if (created[0]) {
      start(entry, key, zkClient, config, clusterId);
    }
    try {
      return answer(entry._result.get(WAIT_MS, TimeUnit.MILLISECONDS), resource);
    } catch (TimeoutException e) {
      return answer(VERIFYING, "The check is still running, ask again shortly.");
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      return answer(VERIFYING, "The check was interrupted.");
    } catch (ExecutionException e) {
      return answer(UNAVAILABLE, "Cannot be determined: " + e.getCause().getMessage());
    }
  }

  static ObjectNode answer(Check check, String resource) {
    if (!check._match) {
      return resource == null
          ? answer(MISMATCH, "The external view differs from the assignment the controller "
              + "persisted.")
          : answer(UNAVAILABLE, "Cannot be determined: the external view differs from the "
              + "persisted assignment somewhere in the cluster, and resources are not checked "
              + "one by one.");
    }
    if (resource == null ? !check._held.isEmpty() : check._held.contains(resource)) {
      return answer(UNAVAILABLE, HELD);
    }
    return answer(MATCH, "The external view matches the assignment the controller persisted.");
  }

  private void start(Entry entry, String key, RealmAwareZkClient zkClient, ClusterConfig config,
      String clusterId) {
    try {
      _executor.execute(() -> {
        StrictMatchExternalViewVerifier verifier = null;
        try {
          // Deactivated node awareness expects a disabled node that is still in a preference
          // list to be OFFLINE, which is what its external view says.
          verifier = new StrictMatchExternalViewVerifier.Builder(clusterId).setZkClient(zkClient)
              .setDeactivatedNodeAwareness(true).build();
          boolean match = verifier.verifyByPolling(0L, 1L);
          if (verifier.getLastError() != null) {
            throw verifier.getLastError();
          }
          entry._result.complete(new Check(match, heldForDelay(zkClient, config, clusterId)));
        } catch (Throwable t) {
          // Always completes, so a failed check can never leave a cluster stuck at VERIFYING.
          entry._result.completeExceptionally(t);
        } finally {
          if (verifier != null) {
            verifier.close();
          }
          entry._doneAt = System.currentTimeMillis();
          if (_cache.size() > MAX_CACHED) {
            _cache.clear();
          }
        }
      });
    } catch (RejectedExecutionException e) {
      _cache.remove(key, entry);
      entry._result.completeExceptionally(
          new IllegalStateException("too many checks are running."));
    }
  }

  /**
   * The delayed resources that still list an instance that is not live, or is disabled, in a
   * preference list: the persisted assignment is built from live nodes, so it matches an external
   * view that is still missing the replica the delay window holds back.
   */
  static Set<String> heldForDelay(RealmAwareZkClient zkClient, ClusterConfig config,
      String clusterId) {
    HelixDataAccessor accessor =
        new ZKHelixDataAccessor(clusterId, new ZkBaseDataAccessor<>(zkClient));
    PropertyKey.Builder keys = accessor.keyBuilder();
    Set<String> live = new HashSet<>(accessor.getChildNames(keys.liveInstances()));
    for (InstanceConfig instance : accessor.<InstanceConfig>getChildValues(
        keys.instanceConfigs(), true)) {
      // Same rule as the controller's disabled set; a swap-in node is not held back.
      if (instance.getInstanceOperation().getOperation() == InstanceOperation.DISABLE) {
        live.remove(instance.getInstanceName());
      }
    }
    Set<String> held = new HashSet<>();
    for (IdealState idealState : accessor.<IdealState>getChildValues(keys.idealStates(), true)) {
      if (idealState.getRebalanceMode() != IdealState.RebalanceMode.FULL_AUTO
          || !DelayedRebalanceUtil.isDelayRebalanceEnabled(idealState, config)) {
        continue;
      }
      for (String partition : idealState.getPartitionSet()) {
        List<String> preferred = idealState.getPreferenceList(partition);
        if (preferred != null && !live.containsAll(preferred)) {
          held.add(idealState.getResourceName());
          break;
        }
      }
    }
    return held;
  }

  private static ObjectNode answer(String result, String message) {
    ObjectNode node = JsonNodeFactory.instance.objectNode();
    node.put("source", SOURCE);
    node.put("result", result);
    node.put("message", message);
    node.put("checkedAt", System.currentTimeMillis());
    return node;
  }
}
