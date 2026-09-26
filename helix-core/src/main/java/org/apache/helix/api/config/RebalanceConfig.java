package org.apache.helix.api.config;

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

import java.util.HashMap;
import java.util.Map;

import org.apache.helix.model.IdealState;
import org.apache.helix.task.TaskRebalancer;
import org.apache.helix.zookeeper.datamodel.ZNRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Rebalance strategy and timer settings exposed through ResourceConfig.
 * Rebalance delay, mode, and rebalancer class are configured through {@link IdealState}.
 */
public class RebalanceConfig {
  /**
   * Configurable rebalance options of a resource
   */
  public enum RebalanceConfigProperty {
    REBALANCE_TIMER_PERIOD,
    REBALANCE_STRATEGY
  }

  /**
   * The mode used for rebalance. FULL_AUTO does both node location calculation and state
   * assignment, SEMI_AUTO only does the latter, and CUSTOMIZED does neither. USER_DEFINED
   * uses a Rebalancer implementation plugged in by the user. TASK designates that a
   * {@link TaskRebalancer} instance should be used to rebalance this resource.
   *
   * This type is retained for compatibility with callers using the mode names; it no longer
   * configures a ResourceConfig property.
   * @deprecated Use {@link IdealState.RebalanceMode}.
   */
  @Deprecated
  public enum RebalanceMode {
    FULL_AUTO,
    SEMI_AUTO,
    CUSTOMIZED,
    USER_DEFINED,
    TASK,
    NONE
  }

  private String _rebalanceStrategy;
  private long _rebalanceTimerPeriod = -1;  /* in milliseconds */

  private static final Logger _logger = LoggerFactory.getLogger(RebalanceConfig.class.getName());

  /**
   * Instantiate from an znRecord
   *
   * @param znRecord
   */
  public RebalanceConfig(ZNRecord znRecord) {
    _rebalanceStrategy = znRecord.getSimpleField(RebalanceConfigProperty.REBALANCE_STRATEGY.name());
    _rebalanceTimerPeriod =
        znRecord.getLongField(RebalanceConfigProperty.REBALANCE_TIMER_PERIOD.name(), -1);
  }

  /**
   * Get the rebalance strategy for this resource.
   *
   * @return rebalance strategy, or null if not specified.
   */
  public String getRebalanceStrategy() {
    return _rebalanceStrategy;
  }

  /**
   * Specify the strategy for Helix to use to compute the partition-instance assignment,
   * i,e, the custom rebalance strategy that implements {@link org.apache.helix.controller.rebalancer.strategy.RebalanceStrategy}
   *
   * @param rebalanceStrategy
   * @return
   */
  public void setRebalanceStrategy(String rebalanceStrategy) {
    this._rebalanceStrategy = rebalanceStrategy;
  }

  /**
   * Get the frequency with which to rebalance
   * @return the rebalancing timer period
   */
  public long getRebalanceTimerPeriod() {
    return _rebalanceTimerPeriod;
  }

  /**
   * Set the frequency with which to rebalance
   * @param  rebalanceTimerPeriod
   */
  public void setRebalanceTimerPeriod(long rebalanceTimerPeriod) {
    this._rebalanceTimerPeriod = rebalanceTimerPeriod;
  }

  /**
   * Generate the config map for RebalanceConfig.
   *
   * @return
   */
  public Map<String, String> getConfigsMap() {
    Map<String, String> simpleFieldMap = new HashMap<String, String>();

    if (_rebalanceStrategy != null) {
      simpleFieldMap.put(RebalanceConfigProperty.REBALANCE_STRATEGY.name(), _rebalanceStrategy);
    }
    if (_rebalanceTimerPeriod > 0) {
      simpleFieldMap.put(RebalanceConfigProperty.REBALANCE_TIMER_PERIOD.name(),
          String.valueOf(_rebalanceTimerPeriod));
    }

    return simpleFieldMap;
  }

  public boolean isValid() {
    return true;
  }
}
