package org.apache.helix.sharding;

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

import org.apache.helix.controller.rebalancer.strategy.AutoRebalanceStrategy;
import org.apache.helix.controller.rebalancer.strategy.CrushEdRebalanceStrategy;
import org.apache.helix.controller.rebalancer.strategy.CrushRebalanceStrategy;
import org.apache.helix.controller.rebalancer.strategy.StickyRebalanceStrategy;


/**
 * Enumeration of rebalance strategies available for sharding.
 *
 * <p>Maps Tendril assigner types to their Helix equivalents:
 * <ul>
 *   <li>{@code ROUND_ROBIN} → {@link #AUTO}</li>
 *   <li>{@code SIMPLE_STICKINESS} → {@link #STICKY}</li>
 *   <li>{@code CRUSH} → {@link #CRUSH}</li>
 *   <li>{@code CRUSHED} → {@link #CRUSH_ED}</li>
 * </ul>
 */
public enum ShardingRebalanceStrategy {

  /** Round-robin / auto-rebalance (Tendril's ROUND_ROBIN). */
  AUTO(AutoRebalanceStrategy.class.getName()),

  /** Sticky assignment — never moves existing replicas (Tendril's SIMPLE_STICKINESS). */
  STICKY(StickyRebalanceStrategy.class.getName()),

  /** CRUSH-based consistent hashing (Tendril's CRUSH). */
  CRUSH(CrushRebalanceStrategy.class.getName()),

  /** CRUSH with even distribution (Tendril's CRUSHED). */
  CRUSH_ED(CrushEdRebalanceStrategy.class.getName());

  private final String helixClassName;

  ShardingRebalanceStrategy(String helixClassName) {
    this.helixClassName = helixClassName;
  }

  /**
   * @return The fully-qualified class name of the Helix {@code RebalanceStrategy} implementation.
   */
  public String getHelixClassName() {
    return helixClassName;
  }
}
