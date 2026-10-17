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

package org.apache.helix.guardrail;

import java.util.AbstractMap;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Deque;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.TreeSet;

import org.apache.helix.HelixProperty;
import org.apache.helix.model.ClusterConfig;
import org.apache.helix.model.IdealState;
import org.apache.helix.model.InstanceConfig;
import org.apache.helix.model.ResourceAssignment;
import org.apache.helix.model.ResourceConfig;
import org.apache.helix.zookeeper.datamodel.ZNRecord;

/**
 * Returns the assignment computed for the same inputs by one of the last two calls instead of
 * computing it again. A guardrail call computes the cluster's assignment as it is and then with
 * the change applied, so the calls that judge subsets of one request share the first computation.
 * <p>
 * Inputs are compared by content. What the wrapped provider reads beyond them, such as current
 * states, may drift between calls without changing how many replicas can be placed, which is all
 * the guardrail compares. Use one instance per request, from one thread; it is not thread safe.
 */
public final class ReusingWagedAssignmentProvider implements WagedAssignmentProvider {
  private static final int REMEMBERED = 2;

  private final WagedAssignmentProvider _provider;
  // Most recently used first.
  private final Deque<Map.Entry<List<Object>, Map<String, ResourceAssignment>>> _recent =
      new ArrayDeque<>(REMEMBERED + 1);

  public ReusingWagedAssignmentProvider(WagedAssignmentProvider provider) {
    _provider = Objects.requireNonNull(provider, "provider");
  }

  @Override
  public Map<String, ResourceAssignment> computeTargetAssignment(ClusterConfig clusterConfig,
      List<InstanceConfig> instanceConfigs, List<String> liveInstances,
      List<IdealState> idealStates, List<ResourceConfig> resourceConfigs) throws Exception {
    // Taken before computing, which may change the inputs.
    List<Object> key = Arrays.asList(snapshot(clusterConfig), snapshots(instanceConfigs),
        new TreeSet<>(liveInstances), snapshots(idealStates), snapshots(resourceConfigs));
    for (Iterator<Map.Entry<List<Object>, Map<String, ResourceAssignment>>> it =
        _recent.iterator(); it.hasNext(); ) {
      Map.Entry<List<Object>, Map<String, ResourceAssignment>> entry = it.next();
      if (entry.getKey().equals(key)) {
        it.remove();
        _recent.addFirst(entry);
        return entry.getValue();
      }
    }
    Map<String, ResourceAssignment> computed = _provider.computeTargetAssignment(clusterConfig,
        instanceConfigs, liveInstances, idealStates, resourceConfigs);
    _recent.addFirst(new AbstractMap.SimpleImmutableEntry<>(key, computed));
    if (_recent.size() > REMEMBERED) {
      _recent.removeLast();
    }
    return computed;
  }

  private static Set<List<Object>> snapshots(List<? extends HelixProperty> properties) {
    Set<List<Object>> snapshots = new HashSet<>();
    for (HelixProperty property : properties) {
      snapshots.add(snapshot(property));
    }
    return snapshots;
  }

  /** A copy of the record deep enough that changing the record later leaves it as it was. */
  private static List<Object> snapshot(HelixProperty property) {
    if (property == null) {
      return null;
    }
    ZNRecord record = property.getRecord();
    Map<String, Map<String, String>> mapFields = new HashMap<>();
    record.getMapFields().forEach((field, map) ->
        mapFields.put(field, map == null ? null : new HashMap<>(map)));
    Map<String, List<String>> listFields = new HashMap<>();
    record.getListFields().forEach((field, list) ->
        listFields.put(field, list == null ? null : new ArrayList<>(list)));
    return Arrays.asList(record.getId(), new HashMap<>(record.getSimpleFields()), mapFields,
        listFields);
  }
}
