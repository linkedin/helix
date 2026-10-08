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
import java.util.Objects;

import org.apache.helix.model.IdealState;
import org.apache.helix.task.TaskRebalancer;
import org.apache.helix.zookeeper.datamodel.ZNRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Compatibility wrapper for retired ResourceConfig rebalance settings.
 * Rebalance delay, mode, rebalancer class, and strategy are configured through {@link IdealState}.
 * Periodic rebalance is configured through
 * {@link org.apache.helix.model.ClusterConfig#setRebalanceTimePeriod(long)}, not per resource.
 * Legacy fields are ignored and {@link #getConfigsMap()} emits no fields.
 */
public class RebalanceConfig {
  /**
   * Legacy property type retained for compatibility; no supported properties remain.
   */
  public enum RebalanceConfigProperty {
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

  private static final Logger _logger = LoggerFactory.getLogger(RebalanceConfig.class.getName());

  /**
   * Retained constructor for callers wrapping legacy records; no settings are read.
   *
   * @param znRecord
   */
  public RebalanceConfig(ZNRecord znRecord) {
    Objects.requireNonNull(znRecord, "znRecord");
  }

  /**
   * Generate the config map for RebalanceConfig.
   *
   * @return
   */
  public Map<String, String> getConfigsMap() {
    return new HashMap<String, String>();
  }

  public boolean isValid() {
    return true;
  }
}
