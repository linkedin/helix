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
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import javax.ws.rs.DefaultValue;
import javax.ws.rs.GET;
import javax.ws.rs.HeaderParam;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.EntityTag;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;

import com.codahale.metrics.annotation.ResponseMetered;
import com.codahale.metrics.annotation.Timed;
import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import org.apache.helix.controller.rebalancer.waged.WagedCapacitySnapshot;
import org.apache.helix.controller.rebalancer.waged.WagedCapacitySnapshotProvider;
import org.apache.helix.manager.zk.ZKUtil;
import org.apache.helix.rest.common.ContextPropertyKeys;
import org.apache.helix.rest.common.HttpConstants;
import org.apache.helix.rest.server.filters.ClusterAuth;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Serves a {@link WagedCapacitySnapshot}: how loaded each instance is, per capacity key. Read only;
 * it never runs the rebalancer and never writes to ZooKeeper.
 *
 * <p>A cold build scans every ExternalView, so the result is cached as serialized JSON and:
 * <ul>
 *   <li>callers choose how stale they accept via {@code maxStaleMs}, clamped to
 *       [{@link #MIN_STALE_MS}, {@link #MAX_STALE_MS}]; the floor means a caller cannot force a
 *       rebuild on every request, and an entry is dropped {@link #CACHE_EXPIRE_MINUTES} minutes
 *       after it is built whatever was asked;</li>
 *   <li>concurrent misses for one cluster share a single build;</li>
 *   <li>at most {@link #MAX_CONCURRENT_BUILDS} builds run at once; further cold requests get 503
 *       with {@code Retry-After} instead of piling large ExternalView reads onto the heap;</li>
 *   <li>a failed build is returned to every waiter and never replaced by a stale entry;</li>
 *   <li>the response carries an {@code ETag}; a matching {@code If-None-Match} gets 304.</li>
 * </ul>
 */
@ClusterAuth
@Path("/clusters/{clusterId}/capacity/heatmap")
public class CapacityHeatmapAccessor extends AbstractHelixResource {
  private static final Logger LOG = LoggerFactory.getLogger(CapacityHeatmapAccessor.class);

  static final long DEFAULT_MAX_STALE_MS = 30_000L;
  static final long MIN_STALE_MS = 5_000L;
  static final long MAX_STALE_MS = 300_000L;
  static final long CACHE_EXPIRE_MINUTES = 5L;
  static final int MAX_CONCURRENT_BUILDS = 4;
  private static final long BUILD_AWAIT_TIMEOUT_MS = 120_000L;
  private static final int MAX_CACHED_CLUSTERS = 256;

  private static final Cache<CacheKey, Built> CACHE = CacheBuilder.newBuilder()
      .maximumSize(MAX_CACHED_CLUSTERS).expireAfterWrite(CACHE_EXPIRE_MINUTES, TimeUnit.MINUTES)
      .build();
  private static final ConcurrentHashMap<CacheKey, CompletableFuture<Built>> IN_FLIGHT =
      new ConcurrentHashMap<>();
  private static final Semaphore BUILD_PERMITS = new Semaphore(MAX_CONCURRENT_BUILDS);
  private static final WagedCapacitySnapshotProvider PROVIDER = new WagedCapacitySnapshotProvider();

  @GET
  @ResponseMetered(name = HttpConstants.READ_REQUEST)
  @Timed(name = HttpConstants.READ_REQUEST)
  public Response getCapacityHeatmap(@PathParam("clusterId") String clusterId,
      @DefaultValue("" + DEFAULT_MAX_STALE_MS) @QueryParam("maxStaleMs") long maxStaleMs,
      @HeaderParam("If-None-Match") String ifNoneMatch) {
    try {
      if (!ZKUtil.isClusterSetup(clusterId, getRealmAwareZkClient())) {
        return notFound("Cluster " + clusterId + " does not exist");
      }
      long boundedStaleMs = Math.max(MIN_STALE_MS, Math.min(maxStaleMs, MAX_STALE_MS));
      Result result = getOrBuild(cacheKey(clusterId), clusterId, boundedStaleMs);
      EntityTag etag = new EntityTag(result.built.etag);
      if (ifNoneMatch != null && ifNoneMatch.contains(etag.toString())) {
        return Response.notModified(etag).build();
      }
      long ageMs = Math.max(0L, System.currentTimeMillis() - result.built.startedAtMs);
      return Response.ok(result.built.json, MediaType.APPLICATION_JSON_TYPE)
          .tag(etag).header("X-Helix-Snapshot-Cache", result.state)
          .header("X-Helix-Snapshot-Age-Ms", ageMs).build();
    } catch (BusyException e) {
      return Response.status(Response.Status.SERVICE_UNAVAILABLE).header("Retry-After", "1")
          .entity(e.getMessage()).build();
    } catch (TimeoutException e) {
      return Response.status(Response.Status.SERVICE_UNAVAILABLE)
          .entity("Timed out building the capacity heatmap for cluster " + clusterId).build();
    } catch (ExecutionException e) {
      Throwable cause = e.getCause() != null ? e.getCause() : e;
      if (cause instanceof BusyException) {
        return Response.status(Response.Status.SERVICE_UNAVAILABLE).header("Retry-After", "1")
            .entity(cause.getMessage()).build();
      }
      LOG.warn("Failed to build the capacity heatmap for cluster {}", clusterId, cause);
      return serverError(cause instanceof Exception ? (Exception) cause : e);
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      return serverError(e);
    } catch (Exception e) {
      LOG.warn("Failed to build the capacity heatmap for cluster {}", clusterId, e);
      return serverError(e);
    }
  }

  private Result getOrBuild(CacheKey key, String clusterId, long maxStaleMs)
      throws InterruptedException, ExecutionException, TimeoutException {
    Built cached = CACHE.getIfPresent(key);
    if (isFresh(cached, maxStaleMs)) {
      return new Result(cached, "HIT");
    }
    CompletableFuture<Built> mine = new CompletableFuture<>();
    CompletableFuture<Built> existing = IN_FLIGHT.putIfAbsent(key, mine);
    if (existing != null) {
      return new Result(existing.get(BUILD_AWAIT_TIMEOUT_MS, TimeUnit.MILLISECONDS), "COALESCED");
    }
    boolean permitHeld = false;
    try {
      // A build may have finished between the cache check and claiming the in-flight slot.
      cached = CACHE.getIfPresent(key);
      if (isFresh(cached, maxStaleMs)) {
        mine.complete(cached);
        return new Result(cached, "HIT");
      }
      if (!BUILD_PERMITS.tryAcquire()) {
        throw new BusyException(clusterId);
      }
      permitHeld = true;
      // Age is measured from when sampling starts, so it never understates how old the data is.
      long startedAtMs = System.currentTimeMillis();
      WagedCapacitySnapshot snapshot =
          PROVIDER.computeSnapshot(clusterId, getDataAccssor(clusterId));
      Built built = new Built(toJson(snapshot), startedAtMs);
      CACHE.put(key, built);
      mine.complete(built);
      return new Result(built, "MISS");
    } catch (BusyException e) {
      mine.completeExceptionally(e);
      throw e;
    } catch (Exception | Error e) {
      mine.completeExceptionally(e);
      throw new ExecutionException(e);
    } finally {
      if (permitHeld) {
        BUILD_PERMITS.release();
      }
      IN_FLIGHT.remove(key, mine);
    }
  }

  private static boolean isFresh(Built built, long maxStaleMs) {
    return built != null && System.currentTimeMillis() - built.startedAtMs <= maxStaleMs;
  }

  private CacheKey cacheKey(String clusterId) {
    // Each namespace has its own server context, so keying on its identity keeps same-named
    // clusters in different namespaces apart.
    return new CacheKey(_application.getProperties().get(ContextPropertyKeys.SERVER_CONTEXT.name()),
        clusterId);
  }

  private static final class BusyException extends RuntimeException {
    BusyException(String clusterId) {
      super("Capacity heatmap builds are busy, retry shortly for cluster " + clusterId);
    }
  }

  private static final class CacheKey {
    private final Object _serverContext;
    private final String _clusterId;

    CacheKey(Object serverContext, String clusterId) {
      _serverContext = serverContext;
      _clusterId = clusterId;
    }

    @Override
    public boolean equals(Object o) {
      return o instanceof CacheKey && _serverContext == ((CacheKey) o)._serverContext
          && Objects.equals(_clusterId, ((CacheKey) o)._clusterId);
    }

    @Override
    public int hashCode() {
      return 31 * System.identityHashCode(_serverContext) + Objects.hashCode(_clusterId);
    }
  }

  private static final class Built {
    final String json;
    final long startedAtMs;
    final String etag;

    Built(String json, long startedAtMs) {
      this.json = json;
      this.startedAtMs = startedAtMs;
      this.etag = startedAtMs + "-" + json.hashCode();
    }
  }

  private static final class Result {
    final Built built;
    final String state;

    Result(Built built, String state) {
      this.built = built;
      this.state = state;
    }
  }
}
