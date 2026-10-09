package org.apache.helix.wagedsim.scenario;

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
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.LinkedList;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Random;
import java.util.TreeMap;

import org.apache.helix.wagedsim.stats.NodeStats;

/**
 * The order in which instances are taken out of a cluster. Loads are all-replica utilization on the
 * focus key, so "least loaded" means the instance whose replicas are cheapest to move.
 */
public final class RemovalOrder {
  /** How to pick the next instance to remove. */
  public enum Strategy {
    /**
     * Keep fault zones (maintenance zones) even: take the next instance from the zone that stays least
     * utilized without it (its load over the capacity it has left, on its tightest capacity key). With
     * equal instances this is the zone with the most instances left. Within a zone, the smallest
     * instance goes first, then the least loaded.
     */
    MZ_BALANCED,
    /** Worst case for zone spread: empty the largest zone first (least loaded first), then the next. */
    MZ_SINGLE,
    /** Least loaded first, ignoring zones. */
    LEAST_LOADED,
    /** Most loaded first, ignoring zones. */
    MOST_LOADED,
    /** Seeded random order. */
    RANDOM,
    /** Instance name order. */
    NAME;

    public static Strategy parse(String text) {
      try {
        return valueOf(text.trim().toUpperCase(Locale.ROOT).replace('-', '_'));
      } catch (IllegalArgumentException e) {
        throw new IllegalArgumentException("Unknown removal strategy '" + text
            + "'; use mz-balanced, mz-single, least-loaded, most-loaded, random or name");
      }
    }

    public String label() {
      return name().toLowerCase(Locale.ROOT).replace('_', '-');
    }
  }

  private static final String NO_ZONE = "(none)";

  /** Zone names in natural order, numbers by value: 2 before 10. */
  public static final Comparator<String> ZONE_ORDER = RemovalOrder::compareNatural;

  private RemovalOrder() {
  }

  /**
   * @param candidates instances that may be removed
   * @param nodes per-instance loads and zones
   * @param key the capacity key loads are measured on
   * @return every candidate, in removal order
   */
  public static List<String> order(Strategy strategy, Collection<String> candidates, Map<String, NodeStats> nodes,
      String key, Random random) {
    List<String> list = new ArrayList<>(candidates);
    Comparator<String> leastLoaded = Comparator.comparingDouble((String i) -> load(nodes, i, key))
        .thenComparing(Comparator.naturalOrder());
    switch (strategy) {
      case NAME:
        Collections.sort(list);
        return list;
      case RANDOM:
        Collections.sort(list);
        Collections.shuffle(list, random);
        return list;
      case LEAST_LOADED:
        list.sort(leastLoaded);
        return list;
      case MOST_LOADED:
        list.sort(Comparator.comparingDouble((String i) -> -load(nodes, i, key)).thenComparing(Comparator.naturalOrder()));
        return list;
      case MZ_SINGLE: {
        Map<String, List<String>> zones = byZone(list, nodes, leastLoaded);
        List<String> names = new ArrayList<>(zones.keySet());
        names.sort(Comparator.comparingInt((String z) -> -zones.get(z).size()).thenComparing(ZONE_ORDER));
        List<String> result = new ArrayList<>();
        names.forEach(z -> result.addAll(zones.get(z)));
        return result;
      }
      case MZ_BALANCED:
        return balanced(list, nodes, key, leastLoaded);
      default:
        throw new IllegalStateException(strategy.name());
    }
  }

  private static List<String> balanced(List<String> list, Map<String, NodeStats> nodes, String key,
      Comparator<String> leastLoaded) {
    Comparator<String> smallestFirst = Comparator.comparingLong((String i) -> capacity(nodes, i, key))
        .thenComparing(leastLoaded);
    Map<String, List<String>> zones = byZone(list, nodes, smallestFirst);
    Map<String, LinkedList<String>> remaining = new TreeMap<>(ZONE_ORDER);
    Map<String, Map<String, Long>> zoneCapacity = new TreeMap<>(ZONE_ORDER);
    Map<String, Map<String, Long>> zoneLoad = new TreeMap<>(ZONE_ORDER);
    zones.forEach((zone, members) -> {
      remaining.put(zone, new LinkedList<>(members));
      Map<String, Long> cap = new TreeMap<>();
      Map<String, Long> load = new TreeMap<>();
      for (String instance : members) {
        NodeStats node = nodes.get(instance);
        if (node != null) {
          node.capacity.forEach((k, v) -> cap.merge(k, (long) v, Long::sum));
          node.allLoad.forEach((k, v) -> load.merge(k, v, Long::sum));
        }
      }
      zoneCapacity.put(zone, cap);
      zoneLoad.put(zone, load);
    });
    List<String> result = new ArrayList<>();
    while (result.size() < list.size()) {
      String best = null;
      double bestUtil = 0;
      for (Map.Entry<String, LinkedList<String>> zone : remaining.entrySet()) {
        if (zone.getValue().isEmpty()) {
          continue;
        }
        double util = utilizationWithout(zoneCapacity.get(zone.getKey()), zoneLoad.get(zone.getKey()),
            nodes.get(zone.getValue().getFirst()));
        int cmp = best == null ? -1 : Double.compare(util, bestUtil);
        if (cmp == 0) {
          cmp = Integer.compare(remaining.get(best).size(), zone.getValue().size());
        }
        if (cmp == 0) {
          cmp = leastLoaded.compare(zone.getValue().getFirst(), remaining.get(best).getFirst());
        }
        if (cmp < 0) {
          best = zone.getKey();
          bestUtil = util;
        }
      }
      String instance = remaining.get(best).removeFirst();
      NodeStats node = nodes.get(instance);
      if (node != null) {
        node.capacity.forEach((k, v) -> zoneCapacity.get(zone(nodes, instance)).merge(k, (long) -v, Long::sum));
      }
      result.add(instance);
    }
    return result;
  }

  /** @return the zone's highest load-to-capacity ratio over capacity keys once {@code node} is gone */
  private static double utilizationWithout(Map<String, Long> capacity, Map<String, Long> load, NodeStats node) {
    double worst = 0;
    for (Map.Entry<String, Long> entry : capacity.entrySet()) {
      long left = entry.getValue() - (node == null ? 0 : node.capacity.getOrDefault(entry.getKey(), 0));
      long used = load.getOrDefault(entry.getKey(), 0L);
      if (used == 0) {
        continue;
      }
      worst = Math.max(worst, left <= 0 ? Double.MAX_VALUE : used / (double) left);
    }
    return worst;
  }

  private static long capacity(Map<String, NodeStats> nodes, String instance, String key) {
    NodeStats node = nodes.get(instance);
    return node == null ? 0 : node.capacity.getOrDefault(key, 0);
  }

  static int compareNatural(String a, String b) {
    int i = 0;
    int j = 0;
    while (i < a.length() && j < b.length()) {
      char ca = a.charAt(i);
      char cb = b.charAt(j);
      if (Character.isDigit(ca) && Character.isDigit(cb)) {
        int si = i;
        int sj = j;
        while (i < a.length() && Character.isDigit(a.charAt(i))) {
          i++;
        }
        while (j < b.length() && Character.isDigit(b.charAt(j))) {
          j++;
        }
        String na = a.substring(si, i).replaceFirst("^0+(?=.)", "");
        String nb = b.substring(sj, j).replaceFirst("^0+(?=.)", "");
        int cmp = na.length() != nb.length() ? Integer.compare(na.length(), nb.length()) : na.compareTo(nb);
        if (cmp != 0) {
          return cmp;
        }
      } else {
        if (ca != cb) {
          return Character.compare(ca, cb);
        }
        i++;
        j++;
      }
    }
    int cmp = Integer.compare(a.length() - i, b.length() - j);
    return cmp != 0 ? cmp : a.compareTo(b);
  }

  private static Map<String, List<String>> byZone(List<String> instances, Map<String, NodeStats> nodes,
      Comparator<String> order) {
    Map<String, List<String>> zones = new TreeMap<>(ZONE_ORDER);
    for (String instance : instances) {
      zones.computeIfAbsent(zone(nodes, instance), z -> new ArrayList<>()).add(instance);
    }
    zones.values().forEach(members -> members.sort(order));
    return zones;
  }

  public static String zone(Map<String, NodeStats> nodes, String instance) {
    NodeStats node = nodes.get(instance);
    return node == null || node.zone == null ? NO_ZONE : node.zone;
  }

  private static double load(Map<String, NodeStats> nodes, String instance, String key) {
    NodeStats node = nodes.get(instance);
    return node == null ? 0 : node.allUtil(key);
  }

  /** @return zone to number of instances, for a set of instances */
  public static Map<String, Integer> countByZone(Collection<String> instances, Map<String, NodeStats> nodes) {
    Map<String, Integer> counts = new TreeMap<>(ZONE_ORDER);
    for (String instance : instances) {
      counts.merge(zone(nodes, instance), 1, Integer::sum);
    }
    return counts;
  }
}
