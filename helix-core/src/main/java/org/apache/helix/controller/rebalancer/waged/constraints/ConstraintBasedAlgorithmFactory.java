package org.apache.helix.controller.rebalancer.waged.constraints;

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
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.ForkJoinPool.ForkJoinWorkerThreadFactory;
import java.util.concurrent.ForkJoinWorkerThread;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Maps;
import org.apache.helix.HelixManagerProperties;
import org.apache.helix.SystemPropertyKeys;
import org.apache.helix.controller.rebalancer.waged.RebalanceAlgorithm;
import org.apache.helix.model.ClusterConfig;
import org.apache.helix.util.HelixUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The factory class to create an instance of {@link ConstraintBasedAlgorithm}
 */
public class ConstraintBasedAlgorithmFactory {
  private static final Logger LOG = LoggerFactory.getLogger(ConstraintBasedAlgorithmFactory.class);
  private static final String CONSTRAINT_EVAL_THREAD_PREFIX = "Helix-ConstraintEval-worker-";
  private static final int MIN_PARALLELISM = 2;

  // Shared ForkJoinPool reused across all ConstraintBasedAlgorithm instances
  private static final AtomicReference<ForkJoinPool> SHARED_POOL_REF = new AtomicReference<>();

  /**
   * Custom ForkJoinWorkerThreadFactory that creates threads with meaningful names
   * for better debugging and monitoring.
   */
  private static class NamedForkJoinWorkerThreadFactory implements ForkJoinWorkerThreadFactory {
    private final AtomicInteger threadCounter = new AtomicInteger(0);

    @Override
    public ForkJoinWorkerThread newThread(ForkJoinPool pool) {
      ForkJoinWorkerThread thread = ForkJoinPool.defaultForkJoinWorkerThreadFactory.newThread(pool);
      thread.setName(CONSTRAINT_EVAL_THREAD_PREFIX + threadCounter.getAndIncrement());
      return thread;
    }
  }

  private static final Map<String, Float> MODEL = new HashMap<String, Float>() {
    {
      // The default setting
      put(PartitionMovementConstraint.class.getSimpleName(), 2f);
      put(BaselineInfluenceConstraint.class.getSimpleName(), 0.5f);
      put(InstancePartitionsCountConstraint.class.getSimpleName(), 1f);
      put(ResourcePartitionAntiAffinityConstraint.class.getSimpleName(), 1f);
      put(TopStateMaxCapacityUsageInstanceConstraint.class.getSimpleName(), 3f);
      put(MaxCapacityUsageInstanceConstraint.class.getSimpleName(), 6f);
    }
  };
  // The weight for BaselineInfluenceConstraint used when we are forcing a baseline converge. This
  // number, multiplied by the max score returned by BaselineInfluenceConstraint, must be greater
  // than the total maximum sum of all other constraints, in order to overpower other constraints.
  private static final float FORCE_BASELINE_CONVERGE_WEIGHT = 100000f;

  static {
    Properties properties =
        new HelixManagerProperties(SystemPropertyKeys.SOFT_CONSTRAINT_WEIGHTS).getProperties();
    // overwrite the default value with data load from property file
    properties.forEach((constraintName, weight) -> MODEL.put(String.valueOf(constraintName),
        Float.valueOf(String.valueOf(weight))));
  }

  public static RebalanceAlgorithm getInstance(
      Map<ClusterConfig.GlobalRebalancePreferenceKey, Integer> preferences) {
    List<HardConstraint> hardConstraints =
        ImmutableList.of(new FaultZoneAwareConstraint(), new NodeCapacityConstraint(),
            new ReplicaActivateConstraint(), new NodeMaxPartitionLimitConstraint(),
            new ValidGroupTagConstraint(), new SamePartitionOnInstanceConstraint());

    int evennessPreference = preferences
        .getOrDefault(ClusterConfig.GlobalRebalancePreferenceKey.EVENNESS,
            ClusterConfig.DEFAULT_GLOBAL_REBALANCE_PREFERENCE
                .get(ClusterConfig.GlobalRebalancePreferenceKey.EVENNESS));
    int movementPreference = preferences
        .getOrDefault(ClusterConfig.GlobalRebalancePreferenceKey.LESS_MOVEMENT,
            ClusterConfig.DEFAULT_GLOBAL_REBALANCE_PREFERENCE
                .get(ClusterConfig.GlobalRebalancePreferenceKey.LESS_MOVEMENT));
    boolean forceBaselineConverge = preferences
        .getOrDefault(ClusterConfig.GlobalRebalancePreferenceKey.FORCE_BASELINE_CONVERGE, 0)
        > 0;

    List<SoftConstraint> softConstraints = ImmutableList
        .of(new PartitionMovementConstraint(), new BaselineInfluenceConstraint(),
            new InstancePartitionsCountConstraint(), new ResourcePartitionAntiAffinityConstraint(),
            new TopStateMaxCapacityUsageInstanceConstraint(),
            new MaxCapacityUsageInstanceConstraint());
    Map<SoftConstraint, Float> softConstraintsWithWeight = Maps.toMap(softConstraints, key -> {
      if (key instanceof BaselineInfluenceConstraint && forceBaselineConverge) {
        return FORCE_BASELINE_CONVERGE_WEIGHT;
      }

      float weight = MODEL.get(key.getClass().getSimpleName());
      // Note that BaselineInfluenceConstraint is a constraint that promotes movement for evenness,
      // and is therefore controlled by the evenness preference. Only PartitionMovementConstraint
      // contributes to less movement.
      return key instanceof PartitionMovementConstraint ? movementPreference * weight
          : evennessPreference * weight;
    });

    ForkJoinPool constraintEvaluationPool = getSharedConstraintEvaluationPool();
    return new ConstraintBasedAlgorithm(hardConstraints, softConstraintsWithWeight, constraintEvaluationPool);
  }

  /**
   * Gets or creates a shared ForkJoinPool for constraint evaluation with controlled parallelism.
   * The parallelism can be configured via system property {@link SystemPropertyKeys#CONSTRAINT_ALGORITHM_PARALLELISM}.
   * The parallelism is capped between {@link #MIN_PARALLELISM} and half of available CPU cores
   * to prevent CPU exhaustion. Default parallelism is set to 1/4 of available cores.
   * The pool is shared across all ConstraintBasedAlgorithm instances to avoid resource waste.
   *
   * @return Shared ForkJoinPool instance for constraint evaluation
   */
  private static ForkJoinPool getSharedConstraintEvaluationPool() {
    ForkJoinPool existing = SHARED_POOL_REF.get();
    if (existing != null && !existing.isShutdown()) {
      return existing;
    }

    synchronized (ConstraintBasedAlgorithmFactory.class) {
      // Double-check after acquiring lock
      existing = SHARED_POOL_REF.get();
      if (existing != null && !existing.isShutdown()) {
        return existing;
      }

      // Calculate bounds based on available CPU cores to prevent CPU exhaustion
      int availableCores = Runtime.getRuntime().availableProcessors();
      int maxParallelism = Math.max(MIN_PARALLELISM, availableCores / 2);
      int defaultParallelism = Math.min(maxParallelism, Math.max(MIN_PARALLELISM, availableCores / 4));

      // Read configured value and clamp it within bounds
      int configuredParallelism = HelixUtil.getSystemPropertyAsInt(
          SystemPropertyKeys.CONSTRAINT_ALGORITHM_PARALLELISM,
          defaultParallelism);
      int finalParallelism = Math.min(maxParallelism, Math.max(MIN_PARALLELISM, configuredParallelism));

      LOG.info("Creating shared constraint evaluation ForkJoinPool. "
              + "Available cores: {}, configured parallelism: {}, final parallelism: {} (min: {}, max: {})",
          availableCores, configuredParallelism, finalParallelism, MIN_PARALLELISM, maxParallelism);

      ForkJoinPool pool = new ForkJoinPool(
          finalParallelism,
          new NamedForkJoinWorkerThreadFactory(),
          null,
          false
      );
      SHARED_POOL_REF.set(pool);
      return pool;
    }
  }
}
