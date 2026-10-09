package org.apache.helix.wagedsim.stats;

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

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.TreeMap;

/** Load of one instance in one layout. Loads use the partition weights WAGED uses. */
public class NodeStats {
  public String instance;
  public String zone;
  public boolean enabled;
  public boolean live;
  public boolean assignable;
  /** Enabled, live and assignable: the instances whose load is measured. */
  public boolean serving;
  public Map<String, Integer> capacity = new TreeMap<>();
  public Map<String, Long> topLoad = new TreeMap<>();
  public Map<String, Long> allLoad = new TreeMap<>();
  public int topCount;
  public int replicaCount;
  /** Top-state load on the focus key per resource. */
  public Map<String, Long> topByResource = new TreeMap<>();

  public double topUtil(String key) {
    Integer cap = capacity.get(key);
    return cap == null || cap == 0 ? 0 : topLoad.getOrDefault(key, 0L) / (double) cap;
  }

  public double allUtil(String key) {
    Integer cap = capacity.get(key);
    return cap == null || cap == 0 ? 0 : allLoad.getOrDefault(key, 0L) / (double) cap;
  }

  public Map<String, Object> toRow(Iterable<String> keys) {
    Map<String, Object> row = new LinkedHashMap<>();
    row.put("instance", instance);
    row.put("zone", zone);
    row.put("enabled", enabled);
    row.put("live", live);
    row.put("serving", serving);
    row.put("topCount", topCount);
    row.put("replicaCount", replicaCount);
    for (String key : keys) {
      row.put("capacity." + key, capacity.get(key));
      row.put("topLoad." + key, topLoad.getOrDefault(key, 0L));
      row.put("allLoad." + key, allLoad.getOrDefault(key, 0L));
      row.put("topUtilPct." + key, round(100 * topUtil(key)));
      row.put("allUtilPct." + key, round(100 * allUtil(key)));
    }
    StringBuilder resources = new StringBuilder();
    topByResource.entrySet().stream()
        .sorted((a, b) -> Long.compare(b.getValue(), a.getValue())).limit(3)
        .forEach(e -> resources.append(resources.length() == 0 ? "" : "; ").append(e.getKey())
            .append('=').append(e.getValue()));
    row.put("topResources", resources.toString());
    return row;
  }

  static double round(double value) {
    return Math.round(value * 1000) / 1000.0;
  }
}
