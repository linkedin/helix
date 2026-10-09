package org.apache.helix.wagedsim.source;

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

import java.util.Map;

import org.apache.helix.model.ResourceConfig;
import org.apache.helix.wagedsim.TestClusters;
import org.apache.helix.wagedsim.cluster.ClusterState;
import org.testng.Assert;
import org.testng.annotations.Test;

public class TestSpecSource {

  @SuppressWarnings("unchecked")
  @Test
  public void testOutliersAreDistinctPartitions() throws Exception {
    for (int seed : new int[]{4, 7, 11}) {
      Map<String, Object> spec = TestClusters.spec("TEST_OUTLIERS", 6, 1, 8, false);
      Map<String, Object> resource = (Map<String, Object>) ((java.util.List<Object>) spec.get("resources")).get(0);
      resource.put("weights", TestClusters.map("CU", 10, "DISK", 100, "PARTCOUNT", 1));
      resource.put("outliers", TestClusters.map("count", 3, "factor", 10, "key", "CU"));
      spec.put("seed", seed);
      spec.put("initialPlacement", "none");
      ClusterState state = SpecSource.build(spec);
      Map<String, Map<String, Integer>> weights =
          new ResourceConfig(state.get(ClusterState.resourceConfigPath("db0"))).getPartitionCapacityMap();
      long outliers = weights.entrySet().stream()
          .filter(e -> !e.getKey().equals(ResourceConfig.DEFAULT_PARTITION_KEY))
          .filter(e -> e.getValue().get("CU") == 100).count();
      long normal = weights.entrySet().stream()
          .filter(e -> !e.getKey().equals(ResourceConfig.DEFAULT_PARTITION_KEY))
          .filter(e -> e.getValue().get("CU") == 10).count();
      Assert.assertEquals(outliers, 3, "seed " + seed + ": " + weights);
      Assert.assertEquals(normal, 5, "seed " + seed + ": " + weights);
    }
  }

  @Test
  public void testSameSeedSameCluster() throws Exception {
    ClusterState first = SpecSource.build(TestClusters.spec("TEST_SEED", 6, 2, 8, false));
    ClusterState second = SpecSource.build(TestClusters.spec("TEST_SEED", 6, 2, 8, false));
    Assert.assertEquals(first.getResourceConfigs(), second.getResourceConfigs());
    Assert.assertEquals(first.getServedLayout(), second.getServedLayout());
  }
}
