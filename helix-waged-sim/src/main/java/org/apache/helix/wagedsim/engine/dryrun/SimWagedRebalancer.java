package org.apache.helix.wagedsim.engine.dryrun;

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
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.helix.BucketDataAccessor;
import org.apache.helix.HelixProperty;
import org.apache.helix.HelixRebalanceException;
import org.apache.helix.controller.dataproviders.ResourceControllerDataProvider;
import org.apache.helix.controller.rebalancer.waged.AssignmentMetadataStore;
import org.apache.helix.controller.rebalancer.waged.RebalanceAlgorithm;
import org.apache.helix.controller.rebalancer.waged.WagedRebalancer;
import org.apache.helix.monitoring.metrics.WagedRebalancerMetricCollector;
import org.apache.helix.zookeeper.datamodel.ZNRecord;
import org.apache.helix.zookeeper.zkclient.exception.ZkNoNodeException;

/**
 * The production WAGED rebalancer with an in-memory assignment store, an explicit algorithm and
 * synchronous global and partial passes. It records every rebalance failure it reports.
 */
public class SimWagedRebalancer extends WagedRebalancer {
  private final List<HelixRebalanceException> _failures = new ArrayList<>();
  private final AssignmentMetadataStore _store;

  public SimWagedRebalancer(AssignmentMetadataStore store, RebalanceAlgorithm algorithm,
      WagedRebalancerMetricCollector metrics) {
    super(store, algorithm, Optional.of(metrics));
    _store = store;
  }

  @Override
  protected void reportFailureCategory(HelixRebalanceException ex) {
    super.reportFailureCategory(ex);
    synchronized (_failures) {
      _failures.add(ex);
    }
  }

  /** @return failures reported since the last call */
  public List<HelixRebalanceException> drainFailures() {
    synchronized (_failures) {
      List<HelixRebalanceException> result = new ArrayList<>(_failures);
      _failures.clear();
      return result;
    }
  }

  /** Makes the controller treat the given cluster data as already seen. */
  public void primeChangeDetector(ResourceControllerDataProvider provider) {
    getChangeDetector().updateSnapshots(provider);
  }

  public AssignmentMetadataStore getStore() {
    return _store;
  }

  /** A {@link BucketDataAccessor} kept in memory, so assignments go through the real serialization. */
  public static class InMemoryBuckets implements BucketDataAccessor {
    private final Map<String, ZNRecord> _records = new ConcurrentHashMap<>();

    @Override
    public <T extends HelixProperty> boolean compressedBucketWrite(String path, T value) {
      _records.put(path, new ZNRecord(value.getRecord()));
      return true;
    }

    @Override
    public <T extends HelixProperty> HelixProperty compressedBucketRead(String path,
        Class<T> helixPropertySubType) {
      ZNRecord record = _records.get(path);
      if (record == null) {
        throw new ZkNoNodeException("No assignment at " + path);
      }
      return new HelixProperty(new ZNRecord(record));
    }

    @Override
    public void compressedBucketDelete(String path) {
      _records.remove(path);
    }

    @Override
    public void disconnect() {
    }
  }

  /** The production assignment store over in-memory buckets. */
  public static class Store extends AssignmentMetadataStore {
    public Store(BucketDataAccessor buckets, String clusterName) {
      super(buckets, clusterName);
    }
  }
}
