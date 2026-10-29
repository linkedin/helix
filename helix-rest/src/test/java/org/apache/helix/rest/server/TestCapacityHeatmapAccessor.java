package org.apache.helix.rest.server;

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
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import javax.ws.rs.client.Invocation;
import javax.ws.rs.core.Response;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.helix.rest.server.resources.helix.CapacityHeatmapAccessor;
import org.testng.Assert;
import org.testng.annotations.Test;

public class TestCapacityHeatmapAccessor extends AbstractTestClass {
  private static final String CACHE_HEADER = "X-Helix-Snapshot-Cache";
  private final ObjectMapper _mapper = new ObjectMapper();

  private Response rawGet(String cluster, long maxStaleMs) {
    return target("clusters/" + cluster + "/capacity/heatmap").queryParam("maxStaleMs", maxStaleMs)
        .request().get();
  }

  @Test
  public void testMissingClusterReturns404() {
    get("clusters/NoSuchCluster_capacity/capacity/heatmap", null,
        Response.Status.NOT_FOUND.getStatusCode(), false);
  }

  @Test
  public void testPayloadShapeAndCacheStates() throws Exception {
    String cluster = "TestCluster_0";
    Response miss = rawGet(cluster, 60000L);
    Assert.assertEquals(miss.getStatus(), Response.Status.OK.getStatusCode());
    Assert.assertEquals(miss.getHeaderString(CACHE_HEADER), "MISS");
    JsonNode root = _mapper.readTree(miss.readEntity(String.class));
    Assert.assertEquals(root.get("cluster").asText(), cluster);
    Assert.assertEquals(root.get("source").asText(), "EXTERNAL_VIEW");
    Assert.assertEquals(root.get("instances").size(), 10);
    JsonNode first = root.get("instances").get(0);
    for (String field : new String[] {"registered", "live", "enabled", "capacityStatus"}) {
      Assert.assertTrue(first.has(field), field);
    }
    Assert.assertTrue(root.has("summary"));

    Response hit = rawGet(cluster, 60000L);
    Assert.assertEquals(hit.getHeaderString(CACHE_HEADER), "HIT");
    Assert.assertNotNull(hit.getHeaderString("X-Helix-Snapshot-Age-Ms"));
    hit.readEntity(String.class);

    // A caller asking for "always fresh" is floored to the minimum staleness, so it still hits.
    Response floored = rawGet(cluster, 0L);
    Assert.assertEquals(floored.getHeaderString(CACHE_HEADER), "HIT");
    floored.readEntity(String.class);

    // A different cluster never reuses this entry.
    Response other = rawGet("TestCluster_1", 60000L);
    Assert.assertEquals(other.getHeaderString(CACHE_HEADER), "MISS");
    Assert.assertEquals(_mapper.readTree(other.readEntity(String.class)).get("cluster").asText(),
        "TestCluster_1");
  }

  @Test
  public void testEtagReturns304() {
    String cluster = "TestCluster_2";
    Response first = rawGet(cluster, 60000L);
    String etag = first.getHeaderString("ETag");
    Assert.assertNotNull(etag);
    first.readEntity(String.class);

    Invocation.Builder conditional = target("clusters/" + cluster + "/capacity/heatmap")
        .queryParam("maxStaleMs", 60000L).request().header("If-None-Match", etag);
    Response notModified = conditional.get();
    Assert.assertEquals(notModified.getStatus(), Response.Status.NOT_MODIFIED.getStatusCode());
    notModified.close();
  }

  @Test
  public void testConcurrentColdRequestsBuildOnce() throws Exception {
    String cluster = "HeatmapCoalesce";
    _gSetupTool.addCluster(cluster, true);
    _gSetupTool.addInstanceToCluster(cluster, "coalesce_n0");
    ExecutorService pool = Executors.newFixedThreadPool(8);
    try {
      CountDownLatch release = new CountDownLatch(1);
      List<Future<String>> futures = new ArrayList<>();
      for (int i = 0; i < 8; i++) {
        futures.add(pool.submit(() -> {
          release.await();
          Response response = rawGet(cluster, 60000L);
          try {
            response.readEntity(String.class);
            return response.getHeaderString(CACHE_HEADER);
          } finally {
            response.close();
          }
        }));
      }
      release.countDown();
      int misses = 0;
      for (Future<String> future : futures) {
        String state = future.get(120, TimeUnit.SECONDS);
        Assert.assertNotNull(state);
        if ("MISS".equals(state)) {
          misses++;
        }
      }
      Assert.assertEquals(misses, 1, "A burst of cold requests must build exactly once");
    } finally {
      pool.shutdownNow();
      _gSetupTool.deleteCluster(cluster);
    }
  }

  @Test
  public void testSaturatedBuildsReturn503() throws Exception {
    String cluster = "HeatmapBusy";
    _gSetupTool.addCluster(cluster, true);
    _gSetupTool.addInstanceToCluster(cluster, "busy_n0");
    Field permitsField = CapacityHeatmapAccessor.class.getDeclaredField("BUILD_PERMITS");
    permitsField.setAccessible(true);
    Semaphore permits = (Semaphore) permitsField.get(null);
    int drained = permits.drainPermits();
    try {
      Assert.assertTrue(drained > 0);
      Response busy = rawGet(cluster, 60000L);
      Assert.assertEquals(busy.getStatus(), Response.Status.SERVICE_UNAVAILABLE.getStatusCode());
      Assert.assertEquals(busy.getHeaderString("Retry-After"), "1");
      busy.close();
    } finally {
      permits.release(drained);
      _gSetupTool.deleteCluster(cluster);
    }
  }
}
