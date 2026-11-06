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

import java.util.Collections;
import java.util.Map;
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
import org.apache.helix.model.ClusterConfig;
import org.apache.helix.tools.ClusterVerifiers.StrictMatchExternalViewVerifier;
import org.apache.helix.zookeeper.api.client.RealmAwareZkClient;

/**
 * What REST can say when the controller's report is stale: whether the external view matches the
 * assignment the controller persisted in the ideal states, which is what
 * {@link StrictMatchExternalViewVerifier} checks. A match is weak evidence, since a stuck
 * controller also leaves its ideal states behind; a mismatch is trustworthy. Runs off the request
 * thread, one run per cluster and resource at a time, and the answer is cached.
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

  private static final class Entry {
    final CompletableFuture<ObjectNode> _result = new CompletableFuture<>();
    volatile long _doneAt;
  }

  private final Map<String, Entry> _cache = new ConcurrentHashMap<>();
  private final ExecutorService _executor = new ThreadPoolExecutor(2, 2, 60L, TimeUnit.SECONDS,
      new ArrayBlockingQueue<>(16), runnable -> {
        Thread thread = new Thread(runnable, "convergence-verifier-fallback");
        thread.setDaemon(true);
        return thread;
      });

  static final ConvergenceVerifierFallback INSTANCE = new ConvergenceVerifierFallback();

  /** Adds the {@code fallback} answer to a response whose report is stale. */
  ObjectNode addTo(ObjectNode response, RealmAwareZkClient zkClient, ClusterConfig config,
      String clusterId, String resource) {
    if (ConvergenceStatusResponseMapper.REPORT_STALE.equals(response.path("reportState").asText())) {
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
    String key = resource == null ? clusterId : clusterId + "/" + resource;
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
      start(entry, key, zkClient, clusterId, resource);
    }
    try {
      return entry._result.get(WAIT_MS, TimeUnit.MILLISECONDS).deepCopy();
    } catch (TimeoutException e) {
      return answer(VERIFYING, "The check is still running, ask again shortly.");
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      return answer(VERIFYING, "The check was interrupted.");
    } catch (ExecutionException e) {
      return answer(UNAVAILABLE, "Cannot be determined: " + e.getCause().getMessage());
    }
  }

  private void start(Entry entry, String key, RealmAwareZkClient zkClient, String clusterId,
      String resource) {
    try {
      _executor.execute(() -> {
        try {
          StrictMatchExternalViewVerifier verifier = new StrictMatchExternalViewVerifier(zkClient,
              clusterId, resource == null ? null : Collections.singleton(resource), null);
          boolean match = verifier.verifyByPolling(0L, 1L);
          entry._result.complete(match
              ? answer(MATCH, "The external view matches the assignment the controller persisted.")
              : answer(MISMATCH, "The external view differs from the assignment the controller "
                  + "persisted."));
        } catch (RuntimeException e) {
          entry._result.completeExceptionally(e);
        } finally {
          entry._doneAt = System.currentTimeMillis();
          if (_cache.size() > MAX_CACHED) {
            _cache.clear();
          }
        }
      });
    } catch (RejectedExecutionException e) {
      _cache.remove(key, entry);
      entry._result.complete(answer(UNAVAILABLE, "Cannot be determined: too many checks are "
          + "running."));
    }
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
