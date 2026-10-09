package org.apache.helix.wagedsim.engine;

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

import org.apache.helix.wagedsim.cluster.ClusterState;

/**
 * Runs a cluster in rounds. Events change {@link #state()} between rounds; {@link #runRound} applies
 * those changes, lets the controller logic react, and returns the settled result.
 */
public interface Engine extends AutoCloseable {
  /** @return "dry-run" or "local" */
  String mode();

  /** Loads the cluster. The engine owns {@code state} afterwards. */
  void start(ClusterState state, EngineSettings settings) throws Exception;

  /** @return the current cluster definition; events change it in place */
  ClusterState state();

  /** @return the virtual time in epoch millis */
  long now();

  /** Moves the virtual clock forward. */
  void advanceClock(long millis) throws Exception;

  /** Restarts the controller: change detection starts over and constraint weights are reloaded. */
  void restartController() throws Exception;

  /** Replaces the soft constraint weights; takes effect through a controller restart. */
  void setConstraintWeights(java.util.Map<String, Float> weights) throws Exception;

  /** @return the soft constraint weight overrides in effect */
  java.util.Map<String, Float> constraintWeights();

  /** Applies pending changes, runs the controller logic until settled, and reports the result. */
  RoundResult runRound(int round) throws Exception;

  @Override
  void close();
}
