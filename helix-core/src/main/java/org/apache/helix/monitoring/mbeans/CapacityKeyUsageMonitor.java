package org.apache.helix.monitoring.mbeans;

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
import javax.management.JMException;

import org.apache.helix.monitoring.mbeans.dynamicMBeans.DynamicMBeanProvider;
import org.apache.helix.monitoring.mbeans.dynamicMBeans.DynamicMetric;
import org.apache.helix.monitoring.mbeans.dynamicMBeans.SimpleDynamicMetric;

/**
 * Distribution of the WAGED weighted capacity utilization of one capacity key across the instances
 * of a cluster.
 *
 * <p>One instance is registered for each capacity key of the cluster. It exposes the same gauges as
 * the cluster-level {@code WeightedPartitionUsage*} and {@code WeightedTopStateUsage*} gauges of
 * {@link ClusterStatusMonitor} -- the max, mean, min and max-to-mean ratio of the per-instance
 * utilization -- computed on this capacity key alone instead of on each instance's busiest key.
 * Like those, they are computed from the observed current-state assignment of the enabled, live
 * instances. The top-state max-to-mean ratio of a key is the evenness of the top-state load (e.g.
 * the leader load) on that key: {@code 1.0} means perfectly even, and it is {@code 0.0} when
 * undefined.
 */
public class CapacityKeyUsageMonitor extends DynamicMBeanProvider {
  public static final String CAPACITY_KEY_DN_KEY = "capacityKey";
  private static final String SENSOR_NAME = "CapacityKeyUsage";

  private final ClusterStatusMonitor _clusterStatusMonitor;
  private final String _capacityKey;
  private final SimpleDynamicMetric<Double> _partitionUsageMax =
      new SimpleDynamicMetric<>("WeightedPartitionUsageMaxGauge", 0.0d);
  private final SimpleDynamicMetric<Double> _partitionUsageMean =
      new SimpleDynamicMetric<>("WeightedPartitionUsageMeanGauge", 0.0d);
  private final SimpleDynamicMetric<Double> _partitionUsageMin =
      new SimpleDynamicMetric<>("WeightedPartitionUsageMinGauge", 0.0d);
  private final SimpleDynamicMetric<Double> _partitionUsageMaxMeanRatio =
      new SimpleDynamicMetric<>("WeightedPartitionUsageMaxMeanRatioGauge", 0.0d);
  private final SimpleDynamicMetric<Double> _topStateUsageMax =
      new SimpleDynamicMetric<>("WeightedTopStateUsageMaxGauge", 0.0d);
  private final SimpleDynamicMetric<Double> _topStateUsageMean =
      new SimpleDynamicMetric<>("WeightedTopStateUsageMeanGauge", 0.0d);
  private final SimpleDynamicMetric<Double> _topStateUsageMin =
      new SimpleDynamicMetric<>("WeightedTopStateUsageMinGauge", 0.0d);
  private final SimpleDynamicMetric<Double> _topStateUsageMaxMeanRatio =
      new SimpleDynamicMetric<>("WeightedTopStateUsageMaxMeanRatioGauge", 0.0d);

  public CapacityKeyUsageMonitor(ClusterStatusMonitor clusterStatusMonitor, String capacityKey) {
    _clusterStatusMonitor = clusterStatusMonitor;
    _capacityKey = capacityKey;
  }

  /**
   * Updates the gauges with the distributions of this capacity key.
   *
   * @param partitionStats distribution of the per-instance general (all-replica) utilization
   * @param topStateStats  distribution of the per-instance top-state-only utilization
   */
  public void update(ClusterStatusMonitor.WeightedUsageStats partitionStats,
      ClusterStatusMonitor.WeightedUsageStats topStateStats) {
    _partitionUsageMax.updateValue(partitionStats.getMax());
    _partitionUsageMean.updateValue(partitionStats.getMean());
    _partitionUsageMin.updateValue(partitionStats.getMin());
    _partitionUsageMaxMeanRatio.updateValue(partitionStats.getMaxMeanRatio());
    _topStateUsageMax.updateValue(topStateStats.getMax());
    _topStateUsageMean.updateValue(topStateStats.getMean());
    _topStateUsageMin.updateValue(topStateStats.getMin());
    _topStateUsageMaxMeanRatio.updateValue(topStateStats.getMaxMeanRatio());
  }

  @Override
  public String getSensorName() {
    return String.format("%s.%s.%s", SENSOR_NAME, _clusterStatusMonitor.getClusterName(),
        _capacityKey);
  }

  private String getBeanName() {
    return String.format("%s,%s=%s", _clusterStatusMonitor.clusterBeanName(), CAPACITY_KEY_DN_KEY,
        _capacityKey);
  }

  @Override
  public CapacityKeyUsageMonitor register() throws JMException {
    List<DynamicMetric<?, ?>> attributeList = new ArrayList<>();
    attributeList.add(_partitionUsageMax);
    attributeList.add(_partitionUsageMean);
    attributeList.add(_partitionUsageMin);
    attributeList.add(_partitionUsageMaxMeanRatio);
    attributeList.add(_topStateUsageMax);
    attributeList.add(_topStateUsageMean);
    attributeList.add(_topStateUsageMin);
    attributeList.add(_topStateUsageMaxMeanRatio);
    doRegister(attributeList, _clusterStatusMonitor.getObjectName(getBeanName()));
    return this;
  }
}
