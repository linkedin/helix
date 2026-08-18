package org.apache.helix.util;

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

import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

import javax.annotation.Nullable;

import com.google.common.collect.ImmutableMap;
import org.apache.helix.AccessOption;
import org.apache.helix.BaseDataAccessor;
import org.apache.helix.ConfigAccessor;
import org.apache.helix.HelixDataAccessor;
import org.apache.helix.HelixException;
import org.apache.helix.PropertyPathBuilder;
import org.apache.helix.constants.InstanceConstants;
import org.apache.helix.manager.zk.ZKHelixDataAccessor;
import org.apache.helix.model.ClusterConfig;
import org.apache.helix.model.ClusterTopologyConfig;
import org.apache.helix.model.HelixConfigScope;
import org.apache.helix.model.InstanceConfig;
import org.apache.helix.model.builder.HelixConfigScopeBuilder;
import org.apache.helix.zookeeper.datamodel.ZNRecord;

public class InstanceUtil {

  // Private constructor to prevent instantiation
  private InstanceUtil() {
  }

  // Validators for instance operation transitions.
  // Return Optional.empty() for success, or Optional.of(reason) for failure.
  private static final InstanceOperationValidator ALWAYS_ALLOWED =
      (baseDataAccessor, configAccessor, clusterName, instanceConfig) -> Optional.empty();
  private static final InstanceOperationValidator ALL_MATCHES_ARE_UNKNOWN =
      (baseDataAccessor, configAccessor, clusterName, instanceConfig) -> {
        List<InstanceConfig> matchingInstances =
            findInstancesWithMatchingLogicalId(baseDataAccessor, configAccessor, clusterName,
                instanceConfig);
        if (matchingInstances.isEmpty() || matchingInstances.stream().allMatch(
            instance -> instance.getInstanceOperation().getOperation()
                .equals(InstanceConstants.InstanceOperation.UNKNOWN))) {
          return Optional.empty();
        }
        return Optional.of(
            "All matching logical ID instances must be in UNKNOWN state. Matching instances: "
                + formatMatchingInstances(matchingInstances));
      };
  private static final InstanceOperationValidator ALL_MATCHES_ARE_UNKNOWN_OR_EVACUATE =
      (baseDataAccessor, configAccessor, clusterName, instanceConfig) -> {
        List<InstanceConfig> matchingInstances =
            findInstancesWithMatchingLogicalId(baseDataAccessor, configAccessor, clusterName,
                instanceConfig);
        if (matchingInstances.isEmpty() || matchingInstances.stream().allMatch(instance ->
            instance.getInstanceOperation().getOperation()
                .equals(InstanceConstants.InstanceOperation.UNKNOWN)
                || instance.getInstanceOperation().getOperation()
                .equals(InstanceConstants.InstanceOperation.EVACUATE))) {
          return Optional.empty();
        }
        return Optional.of(
            "All matching logical ID instances must be in UNKNOWN or EVACUATE state. Matching instances: "
                + formatMatchingInstances(matchingInstances));
      };
  private static final InstanceOperationValidator ANY_MATCH_ENABLE_OR_DISABLE =
      (baseDataAccessor, configAccessor, clusterName, instanceConfig) -> {
        List<InstanceConfig> matchingInstances =
            findInstancesWithMatchingLogicalId(baseDataAccessor, configAccessor, clusterName,
                instanceConfig);
        if (!matchingInstances.isEmpty() && matchingInstances.stream().anyMatch(instance ->
            instance.getInstanceOperation().getOperation()
                .equals(InstanceConstants.InstanceOperation.ENABLE)
                || instance.getInstanceOperation().getOperation()
                .equals(InstanceConstants.InstanceOperation.DISABLE))) {
          return Optional.empty();
        }
        return Optional.of(matchingInstances.isEmpty()
            ? "No matching logical ID instances found. At least one must be in ENABLE or DISABLE state."
            : "At least one matching logical ID instance must be in ENABLE or DISABLE state. Matching instances: "
                + formatMatchingInstances(matchingInstances));
      };

  // Validator map for valid instance operation transitions <currentOperation>:<targetOperation>:<validator>
  private static final ImmutableMap<InstanceConstants.InstanceOperation, ImmutableMap<InstanceConstants.InstanceOperation, InstanceOperationValidator>>
      VALID_INSTANCE_OPERATION_TRANSITIONS =
      ImmutableMap.of(InstanceConstants.InstanceOperation.ENABLE,
      // ENABLE and DISABLE can be set to UNKNOWN when matching instance is in SWAP_IN and set to ENABLE in a transaction.
          ImmutableMap.of(InstanceConstants.InstanceOperation.ENABLE, ALWAYS_ALLOWED,
          InstanceConstants.InstanceOperation.DISABLE, ALWAYS_ALLOWED,
          InstanceConstants.InstanceOperation.EVACUATE, ALWAYS_ALLOWED,
          InstanceConstants.InstanceOperation.UNKNOWN, ALWAYS_ALLOWED),
      InstanceConstants.InstanceOperation.DISABLE,
          ImmutableMap.of(InstanceConstants.InstanceOperation.DISABLE, ALWAYS_ALLOWED,
          InstanceConstants.InstanceOperation.ENABLE, ALWAYS_ALLOWED,
          InstanceConstants.InstanceOperation.EVACUATE, ALWAYS_ALLOWED,
          InstanceConstants.InstanceOperation.UNKNOWN, ALWAYS_ALLOWED),
      InstanceConstants.InstanceOperation.SWAP_IN,
      // SWAP_IN can be set to ENABLE when matching instance is in UNKNOWN state in a transaction.
          ImmutableMap.of(InstanceConstants.InstanceOperation.SWAP_IN, ALWAYS_ALLOWED,
          InstanceConstants.InstanceOperation.UNKNOWN, ALWAYS_ALLOWED),
      InstanceConstants.InstanceOperation.EVACUATE,
          ImmutableMap.of(InstanceConstants.InstanceOperation.EVACUATE, ALWAYS_ALLOWED,
          InstanceConstants.InstanceOperation.ENABLE, ALL_MATCHES_ARE_UNKNOWN,
          InstanceConstants.InstanceOperation.DISABLE, ALL_MATCHES_ARE_UNKNOWN,
          InstanceConstants.InstanceOperation.UNKNOWN, ALWAYS_ALLOWED),
      InstanceConstants.InstanceOperation.UNKNOWN,
          ImmutableMap.of(InstanceConstants.InstanceOperation.UNKNOWN, ALWAYS_ALLOWED,
          InstanceConstants.InstanceOperation.ENABLE, ALL_MATCHES_ARE_UNKNOWN_OR_EVACUATE,
          InstanceConstants.InstanceOperation.DISABLE, ALL_MATCHES_ARE_UNKNOWN_OR_EVACUATE,
          InstanceConstants.InstanceOperation.SWAP_IN, ANY_MATCH_ENABLE_OR_DISABLE));

  /**
   * Validates if the transition from the current operation to the target operation is valid.
   *
   * @param configAccessor   The ConfigAccessor instance
   * @param clusterName      The cluster name
   * @param instanceConfig   The current instance configuration
   * @param currentOperation The current operation
   * @param targetOperation  The target operation
   * @deprecated Use {@link #validateInstanceOperationTransition(BaseDataAccessor, String, InstanceConfig, InstanceConstants.InstanceOperation, InstanceConstants.InstanceOperation)}
   *        instead for better performance.
   */
  @Deprecated
  public static void validateInstanceOperationTransition(ConfigAccessor configAccessor,
      String clusterName, InstanceConfig instanceConfig,
      InstanceConstants.InstanceOperation currentOperation,
      InstanceConstants.InstanceOperation targetOperation) {

    validateInstanceOperationTransition(null, configAccessor, clusterName, instanceConfig,
        currentOperation, targetOperation);
  }

  /**
   * Validates if the transition from the current operation to the target operation is valid.
   *
   * @param baseDataAccessor The BaseDataAccessor instance
   * @param clusterName      The cluster name
   * @param instanceConfig   The current instance configuration
   * @param currentOperation The current operation
   * @param targetOperation  The target operation
   */
  public static void validateInstanceOperationTransition(
      BaseDataAccessor<ZNRecord> baseDataAccessor, String clusterName,
      InstanceConfig instanceConfig,
      InstanceConstants.InstanceOperation currentOperation,
      InstanceConstants.InstanceOperation targetOperation) {

    validateInstanceOperationTransition(baseDataAccessor, null, clusterName, instanceConfig,
        currentOperation, targetOperation);
  }

  private static void validateInstanceOperationTransition(
      @Nullable BaseDataAccessor<ZNRecord> baseDataAccessor,
      @Nullable ConfigAccessor configAccessor, String clusterName, InstanceConfig instanceConfig,
      InstanceConstants.InstanceOperation currentOperation,
      InstanceConstants.InstanceOperation targetOperation) {
    ImmutableMap<InstanceConstants.InstanceOperation, InstanceOperationValidator> transitionMap =
        VALID_INSTANCE_OPERATION_TRANSITIONS.get(currentOperation);

    if (transitionMap == null || !transitionMap.containsKey(targetOperation)) {
      String validTransitions = transitionMap != null ? transitionMap.keySet().toString() : "none";
      throw new HelixException(
          "Invalid instance operation transition from " + currentOperation + " to "
              + targetOperation + " for instance " + instanceConfig.getInstanceName()
              + ". Valid transitions from " + currentOperation + ": " + validTransitions
              + ". Current operation source: " + instanceConfig.getInstanceOperation().getSource()
              + ", reason: " + instanceConfig.getInstanceOperation().getReason());
    }

    InstanceOperationValidator validator = transitionMap.get(targetOperation);
    Optional<String> blocker = validator != null
        ? validator.validate(baseDataAccessor, configAccessor, clusterName, instanceConfig)
        : Optional.of("No validator found for transition");
    if (blocker.isPresent()) {
      throw new HelixException(
          "Failed validation for instance operation transition from " + currentOperation + " to "
              + targetOperation + " for instance " + instanceConfig.getInstanceName()
              + ". " + blocker.get());
    }
  }

  /**
   * Finds the instances that have a matching logical ID with the given instance.
   *
   * @param configAccessor  The ConfigAccessor instance
   * @param clusterName     The cluster name
   * @param instanceConfig  The instance configuration to match
   * @return A list of matching instances
   */
  @Deprecated
  public static List<InstanceConfig> findInstancesWithMatchingLogicalId(
      ConfigAccessor configAccessor, String clusterName, InstanceConfig instanceConfig) {
    String logicalIdKey =
        ClusterTopologyConfig.createFromClusterConfig(configAccessor.getClusterConfig(clusterName))
            .getEndNodeType();

    // Retrieve and filter instances with matching logical ID
    return configAccessor.getKeys(
            new HelixConfigScopeBuilder(HelixConfigScope.ConfigScopeProperty.PARTICIPANT,
                clusterName).build()).stream()
        .map(instanceName -> configAccessor.getInstanceConfig(clusterName, instanceName)).filter(
            potentialInstanceConfig ->
                !potentialInstanceConfig.getInstanceName().equals(instanceConfig.getInstanceName())
                    && potentialInstanceConfig.getLogicalId(logicalIdKey)
                    .equals(instanceConfig.getLogicalId(logicalIdKey)))
        .collect(Collectors.toList());
  }

  /**
   * Finds the instances that have a matching logical ID with the given instance.
   *
   * @param clusterName    The cluster name
   * @param instanceConfig The instance configuration to match
   * @return A list of matching instances
   */
  public static List<InstanceConfig> findInstancesWithMatchingLogicalId(
      BaseDataAccessor<ZNRecord> baseDataAccessor, String clusterName,
      InstanceConfig instanceConfig) {
    HelixDataAccessor helixDataAccessor = new ZKHelixDataAccessor(clusterName, baseDataAccessor);

    ClusterConfig clusterConfig =
        helixDataAccessor.getProperty(helixDataAccessor.keyBuilder().clusterConfig());
    String logicalIdKey =
        ClusterTopologyConfig.createFromClusterConfig(clusterConfig).getEndNodeType();

    List<InstanceConfig> instanceConfigs =
        helixDataAccessor.getChildValues(helixDataAccessor.keyBuilder().instanceConfigs(), true);

    // Retrieve and filter instances with matching logical ID
    return instanceConfigs.stream().filter(potentialInstanceConfig ->
        !potentialInstanceConfig.getInstanceName().equals(instanceConfig.getInstanceName())
            && potentialInstanceConfig.getLogicalId(logicalIdKey)
            .equals(instanceConfig.getLogicalId(logicalIdKey))).collect(Collectors.toList());
  }

  private static List<InstanceConfig> findInstancesWithMatchingLogicalId(
      @Nullable BaseDataAccessor<ZNRecord> baseDataAccessor,
      @Nullable ConfigAccessor configAccessor, String clusterName, InstanceConfig instanceConfig) {
    if (baseDataAccessor == null && configAccessor == null) {
      throw new HelixException(
          "Both BaseDataAccessor and ConfigAccessor cannot be null at the same time");
    }

    return baseDataAccessor != null ? findInstancesWithMatchingLogicalId(baseDataAccessor,
        clusterName, instanceConfig)
        : findInstancesWithMatchingLogicalId(configAccessor, clusterName, instanceConfig);
  }

  /**
   * Sets the instance operation for the given instance.
   *
   * @param configAccessor      The ConfigAccessor instance
   * @param baseDataAccessor    The BaseDataAccessor instance
   * @param clusterName         The cluster name
   * @param instanceName        The instance name
   * @param instanceOperation   The instance operation to set
   */
  public static void setInstanceOperation(ConfigAccessor configAccessor,
      BaseDataAccessor<ZNRecord> baseDataAccessor, String clusterName, String instanceName,
      InstanceConfig.InstanceOperation instanceOperation) {
    String path = PropertyPathBuilder.instanceConfig(clusterName, instanceName);

    // Retrieve the current instance configuration
    InstanceConfig instanceConfig = configAccessor.getInstanceConfig(clusterName, instanceName);
    if (instanceConfig == null) {
      throw new HelixException("Cluster " + clusterName + ", instance: " + instanceName
          + ", instance config does not exist");
    }

    // Validate the instance operation transition
    validateInstanceOperationTransition(baseDataAccessor, configAccessor, clusterName,
        instanceConfig,
        instanceConfig.getInstanceOperation().getOperation(),
        instanceOperation == null ? InstanceConstants.InstanceOperation.ENABLE
            : instanceOperation.getOperation());

    // Update the instance operation
    boolean succeeded = baseDataAccessor.update(path, currentData -> {
      if (currentData == null) {
        throw new HelixException("Cluster: " + clusterName + ", instance: " + instanceName
            + ", participant config is null");
      }

      InstanceConfig config = new InstanceConfig(currentData);
      config.setInstanceOperation(instanceOperation);
      return config.getRecord();
    }, AccessOption.PERSISTENT);

    if (!succeeded) {
      throw new HelixException(
          "Failed to update instance operation for instance " + instanceName + " in cluster "
              + clusterName + " (target operation: "
              + (instanceOperation != null ? instanceOperation.getOperation() : "ENABLE")
              + "). The ZooKeeper update did not succeed.");
    }
  }

  /**
   * Returns the live instances that are also marked with
   * {@link InstanceConstants.InstanceOperation#ENABLE}. These are the instances that can
   * currently accept ONLINE replicas.
   *
   * @param instanceConfigMap all instance configs in the cluster, keyed by instance name.
   * @param liveInstanceNames names of the currently live instances.
   * @return a fresh modifiable set of instance names.
   */
  private static Set<String> getEnabledLiveInstances(
      Map<String, InstanceConfig> instanceConfigMap, Collection<String> liveInstanceNames) {
    Set<String> enabledLiveInstances = new HashSet<>();
    for (String instanceName : liveInstanceNames) {
      InstanceConfig config = instanceConfigMap.get(instanceName);
      if (config != null && config.getInstanceOperation().getOperation()
          == InstanceConstants.InstanceOperation.ENABLE) {
        enabledLiveInstances.add(instanceName);
      }
    }
    return enabledLiveInstances;
  }

  /**
   * Returns the set of instances that count toward the cluster-wide offline budget driving
   * auto Maintenance Mode ({@code MAX_OFFLINE_INSTANCES_ALLOWED} at entry,
   * {@code NUM_OFFLINE_INSTANCES_FOR_AUTO_EXIT} at exit).
   *
   * <p>An instance counts when all three conditions hold:
   * <ul>
   *   <li>Its InstanceOperation is routable, i.e. not in
   *       {@link InstanceConstants#UNROUTABLE_INSTANCE_OPERATIONS}.</li>
   *   <li>It is not currently enabled-and-live.</li>
   *   <li>It does not carry a valid (unexpired) instance-operation maintenance marker.</li>
   * </ul>
   * EVACUATE and DISABLE instances are included because they cannot accept new ONLINE
   * replicas; ENABLE+offline instances are included for the same reason. SWAP_IN and
   * UNKNOWN are excluded because they do not represent assignable cluster capacity.
   *
   * <p>This is the single definition of the offline-budget population. The controller
   * (MM entry in {@code BestPossibleStateCalcStage}, MM exit in
   * {@code MaintenanceRecoveryStage}) reaches it through
   * {@code BaseControllerDataProvider#getInstancesUnableToAcceptOnlineReplicas}, and
   * helix-rest exposes the same computation read-only so clients never have to reimplement
   * (and drift from) these rules.
   *
   * <p><b>The caller owns the definition of "live", and it is not always the raw
   * {@code /LIVEINSTANCES} membership.</b> While the cluster is in maintenance mode and
   * {@code OFFLINE_NODE_TIME_OUT_FOR_MAINTENANCE_MODE} is set to a non-negative value, the
   * controller's {@code BaseControllerDataProvider#getLiveInstances()} withholds instances that
   * had been offline longer than that window, and keeps withholding them for the remainder of
   * the maintenance mode even after they come back up. That exclusion is sticky controller
   * state accumulated across pipeline runs, so a caller reading ZooKeeper directly cannot
   * reconstruct it and will pass a strictly larger live set, yielding a strictly smaller
   * population than the controller counts. The two agree whenever the cluster is out of
   * maintenance mode or the timeout is unset (the default, {@code -1}).
   *
   * @param instanceConfigMap all instance configs in the cluster, keyed by instance name.
   *                          Must not be null; an empty map yields an empty population.
   * @param liveInstanceNames names of the currently live instances, as defined by the caller
   *                          (see above). Must not be null.
   * @param nowMs current wall-clock millis used for marker-expiry comparison.
   * @return a fresh modifiable set of instance names.
   */
  public static Set<String> getInstancesUnableToAcceptOnlineReplicas(
      Map<String, InstanceConfig> instanceConfigMap, Collection<String> liveInstanceNames,
      long nowMs) {
    // Null is rejected rather than treated as empty: an empty population reads as "the whole
    // offline budget is free", which is the most dangerous answer this method can give.
    Objects.requireNonNull(instanceConfigMap, "instanceConfigMap must not be null");
    Objects.requireNonNull(liveInstanceNames, "liveInstanceNames must not be null");
    if (instanceConfigMap.isEmpty()) {
      return new HashSet<>();
    }
    Set<String> result = instanceConfigMap.entrySet().stream()
        .filter(e -> e.getValue() != null)
        .filter(e -> !InstanceConstants.UNROUTABLE_INSTANCE_OPERATIONS.contains(
            e.getValue().getInstanceOperation().getOperation()))
        .map(Map.Entry::getKey)
        .collect(Collectors.toCollection(HashSet::new));
    result.removeAll(getEnabledLiveInstances(instanceConfigMap, liveInstanceNames));
    result.removeIf(name -> {
      InstanceConfig config = instanceConfigMap.get(name);
      return config != null && config.isUnderInstanceOperationMaintenance(nowMs);
    });
    return result;
  }

  private static String formatMatchingInstances(List<InstanceConfig> matchingInstances) {
    return matchingInstances.stream()
        .map(ic -> ic.getInstanceName() + " (operation="
            + ic.getInstanceOperation().getOperation() + ")")
        .collect(Collectors.joining(", "));
  }

  /**
   * Validates an instance operation transition. Returns Optional.empty() if valid,
   * or Optional.of(reason) if invalid.
   */
  private interface InstanceOperationValidator {
    Optional<String> validate(@Nullable BaseDataAccessor<ZNRecord> baseDataAccessor,
        @Nullable ConfigAccessor configAccessor, String clusterName, InstanceConfig instanceConfig);
  }
}
