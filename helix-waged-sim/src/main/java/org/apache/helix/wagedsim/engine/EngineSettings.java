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

import java.util.LinkedHashMap;
import java.util.Locale;
import java.util.Map;

import org.apache.helix.controller.rebalancer.waged.constraints.ConstraintBasedAlgorithmFactory;

/** Engine options. Constraint weights use soft constraint class simple names. */
public class EngineSettings {
  /** Which pass a dry-run round runs. AUTO lets WAGED decide, as the controller does. */
  public enum Pass {
    AUTO, PARTIAL, GLOBAL, COLD
  }

  /** The node set a forced pass treats as active. */
  public enum ActiveNodes {
    /** Enabled-live nodes plus nodes still inside the delay window, as WAGED computes it. */
    DELAY_AWARE,
    /** Enabled-live nodes only. */
    ENABLED_LIVE
  }

  /** How round 1 starts. */
  public enum FirstRound {
    /** The controller has already seen the loaded state: round 1 is a steady-state pipeline run. */
    STEADY,
    /** The controller starts fresh (as after a restart or leader switch): round 1 sees every change. */
    RESTART
  }

  public Map<String, Float> constraintWeights = new LinkedHashMap<>();
  public Pass pass = Pass.AUTO;
  public ActiveNodes activeNodes = ActiveNodes.DELAY_AWARE;
  public FirstRound firstRound = FirstRound.STEADY;

  // Local cluster options.
  public long timeCompression = 3600;
  public long roundTimeoutMillis = 300_000;
  public long settleQuietMillis = 2_000;
  public long transitionLatencyMillis = 0;
  public String participants = "auto";
  public int realParticipantLimit = 100;
  public String workDir;

  public EngineSettings copy() {
    EngineSettings copy = new EngineSettings();
    copy.constraintWeights = new LinkedHashMap<>(constraintWeights);
    copy.pass = pass;
    copy.activeNodes = activeNodes;
    copy.firstRound = firstRound;
    copy.timeCompression = timeCompression;
    copy.roundTimeoutMillis = roundTimeoutMillis;
    copy.settleQuietMillis = settleQuietMillis;
    copy.transitionLatencyMillis = transitionLatencyMillis;
    copy.participants = participants;
    copy.realParticipantLimit = realParticipantLimit;
    copy.workDir = workDir;
    return copy;
  }

  /** Maps short names (TopState, PartitionMovement, ...) to soft constraint class simple names. */
  public static String constraintName(String name) {
    String normalized = name.trim();
    for (String known : ConstraintBasedAlgorithmFactory.getConstraintWeights().keySet()) {
      if (known.equalsIgnoreCase(normalized)) {
        return known;
      }
    }
    switch (normalized.toLowerCase(Locale.ROOT).replace("_", "").replace("-", "")) {
      case "topstate":
      case "topstatemaxcapacityusage":
        return "TopStateMaxCapacityUsageInstanceConstraint";
      case "maxcapacityusage":
      case "mc":
        return "MaxCapacityUsageInstanceConstraint";
      case "partitionmovement":
      case "pm":
        return "PartitionMovementConstraint";
      case "baselineinfluence":
      case "bi":
        return "BaselineInfluenceConstraint";
      case "instancepartitionscount":
        return "InstancePartitionsCountConstraint";
      case "resourcepartitionantiaffinity":
        return "ResourcePartitionAntiAffinityConstraint";
      default:
        throw new IllegalArgumentException("Unknown soft constraint '" + name + "'. Known: "
            + ConstraintBasedAlgorithmFactory.getConstraintWeights().keySet());
    }
  }
}
