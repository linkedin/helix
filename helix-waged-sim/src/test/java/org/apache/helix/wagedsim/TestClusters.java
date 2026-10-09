package org.apache.helix.wagedsim;

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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/** Small generated clusters for tests. */
public final class TestClusters {
  private TestClusters() {
  }

  /**
   * @param nodes instances, spread over 3 zones
   * @param resources resources of {@code partitions} partitions and 3 replicas each
   */
  public static Map<String, Object> spec(String name, int nodes, int resources, int partitions,
      boolean delay) {
    Map<String, Object> cluster = new LinkedHashMap<>();
    cluster.put("name", name);
    cluster.put("capacityKeys", Arrays.asList("CU", "DISK", "PARTCOUNT"));
    cluster.put("instanceCapacity", map("CU", 10000, "DISK", 100000, "PARTCOUNT", 500));
    cluster.put("defaultPartitionWeight", map("CU", 10, "DISK", 100, "PARTCOUNT", 1));
    cluster.put("rebalancePreference", map("EVENNESS", 1, "LESS_MOVEMENT", 2));
    if (delay) {
      cluster.put("delayRebalance", map("enabled", true, "time", "1h"));
    }
    Map<String, Object> instances = new LinkedHashMap<>();
    instances.put("count", nodes);
    instances.put("prefix", "node");
    instances.put("zones", 3);
    Map<String, Object> resource = new LinkedHashMap<>();
    resource.put("count", resources);
    resource.put("prefix", "db");
    resource.put("partitions", partitions);
    resource.put("replicas", 3);
    resource.put("stateModel", "MasterSlave");
    resource.put("weights", map("CU", "lognormal(40, 0.8)", "DISK", "uniform(100, 2000)", "PARTCOUNT", 1));
    Map<String, Object> spec = new LinkedHashMap<>();
    spec.put("cluster", cluster);
    spec.put("instances", Collections.singletonList(instances));
    spec.put("resources", Collections.singletonList(resource));
    spec.put("seed", 11);
    return spec;
  }

  public static Map<String, Object> map(Object... keyValues) {
    Map<String, Object> map = new LinkedHashMap<>();
    for (int i = 0; i < keyValues.length; i += 2) {
      map.put((String) keyValues[i], keyValues[i + 1]);
    }
    return map;
  }

  public static List<Object> list(Object... values) {
    return new ArrayList<>(Arrays.asList(values));
  }
}
