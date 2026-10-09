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

import java.util.Map;
import java.util.TreeMap;

import org.apache.helix.HelixRebalanceException;
import org.apache.helix.controller.rebalancer.waged.RebalanceAlgorithm;
import org.apache.helix.controller.rebalancer.waged.constraints.ConstraintBasedAlgorithm;
import org.apache.helix.controller.rebalancer.waged.constraints.ConstraintBasedAlgorithmFactory;
import org.apache.helix.controller.rebalancer.waged.constraints.HardConstraint;
import org.apache.helix.controller.rebalancer.waged.model.ClusterModel;
import org.apache.helix.controller.rebalancer.waged.model.OptimalAssignment;
import org.apache.helix.model.ClusterConfig;

/**
 * Delegates to a constraint-based algorithm built with explicit weights. A preference or weight
 * change rebuilds the delegate, which is what the controller does when preferences change. It also
 * counts, per hard constraint, the partitions that could not be placed.
 */
public class SwappableAlgorithm implements RebalanceAlgorithm {
  private volatile RebalanceAlgorithm _delegate;
  private Map<ClusterConfig.GlobalRebalancePreferenceKey, Integer> _preferences;
  private Map<String, Float> _weights;
  private final Map<String, Long> _blocking = new TreeMap<>();

  public SwappableAlgorithm(Map<ClusterConfig.GlobalRebalancePreferenceKey, Integer> preferences,
      Map<String, Float> weights) {
    update(preferences, weights);
  }

  /** @return true if the delegate was rebuilt */
  public synchronized boolean update(
      Map<ClusterConfig.GlobalRebalancePreferenceKey, Integer> preferences, Map<String, Float> weights) {
    if (_delegate != null && preferences.equals(_preferences) && weights.equals(_weights)) {
      return false;
    }
    _delegate = ConstraintBasedAlgorithmFactory.getInstance(preferences, weights);
    if (_delegate instanceof ConstraintBasedAlgorithm) {
      ((ConstraintBasedAlgorithm) _delegate).setHardConstraintFailureReporter(this::recordBlocking);
    }
    _preferences = preferences;
    _weights = weights;
    return true;
  }

  @Override
  public OptimalAssignment calculate(ClusterModel clusterModel) throws HelixRebalanceException {
    return _delegate.calculate(clusterModel);
  }

  private void recordBlocking(HardConstraint.Type type) {
    synchronized (_blocking) {
      _blocking.merge(type.name(), 1L, Long::sum);
    }
  }

  /** @return per hard constraint, how many partitions it blocked since the last call */
  public Map<String, Long> drainBlocking() {
    synchronized (_blocking) {
      Map<String, Long> result = new TreeMap<>(_blocking);
      _blocking.clear();
      return result;
    }
  }
}
