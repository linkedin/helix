package org.apache.helix.rest.server.resources.helix;

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

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import java.util.stream.Collectors;
import javax.ws.rs.DefaultValue;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.Response;

import com.codahale.metrics.annotation.ResponseMetered;
import com.codahale.metrics.annotation.Timed;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.helix.HelixAdmin;
import org.apache.helix.HelixDataAccessor;
import org.apache.helix.HelixException;
import org.apache.helix.PropertyKey;
import org.apache.helix.constants.InstanceConstants;
import org.apache.helix.manager.zk.ZKHelixDataAccessor;
import org.apache.helix.manager.zk.ZKUtil;
import org.apache.helix.model.ClusterConfig;
import org.apache.helix.model.InstanceConfig;
import org.apache.helix.rest.client.CustomRestClientFactory;
import org.apache.helix.rest.clusterMaintenanceService.HealthCheck;
import org.apache.helix.rest.clusterMaintenanceService.InstanceOperationMaintenanceWriteHandler;
import org.apache.helix.rest.clusterMaintenanceService.InstanceOperationMaintenanceWriteHandler.BadRequestException;
import org.apache.helix.rest.clusterMaintenanceService.MaintenanceManagementService;
import org.apache.helix.rest.common.HttpConstants;
import org.apache.helix.rest.clusterMaintenanceService.StoppableInstancesSelector;
import org.apache.helix.rest.server.filters.ClusterAuth;
import org.apache.helix.rest.server.json.cluster.ClusterTopology;
import org.apache.helix.rest.server.json.instance.StoppableCheck;
import org.apache.helix.rest.server.resources.exceptions.HelixHealthException;
import org.apache.helix.rest.server.service.ClusterService;
import org.apache.helix.rest.server.service.ClusterServiceImpl;
import org.apache.helix.util.InstanceUtil;
import org.apache.helix.util.InstanceValidationUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.helix.rest.clusterMaintenanceService.MaintenanceManagementService.ALL_HEALTH_CHECK_NONBLOCK;

@ClusterAuth
@Path("/clusters/{clusterId}/instances")
public class InstancesAccessor extends AbstractHelixResource {
  private final static Logger _logger = LoggerFactory.getLogger(InstancesAccessor.class);

  public enum InstancesProperties {
    instances,
    online,
    disabled,
    enabled,
    evacuated,
    swap_in,
    unknown,
    selection_base,
    skip_custom_check_if_instance_not_alive,
    zone_order,
    to_be_stopped_instances,
    skip_stoppable_check_list,
    customized_values,
    instance_stoppable_parallel,
    instance_not_stoppable_with_reasons,
    instances_unable_to_accept_online_replicas
  }

  public enum InstanceHealthSelectionBase {
    non_zone_based,
    zone_based,
    cross_zone_based
  }

  @ResponseMetered(name = HttpConstants.READ_REQUEST)
  @Timed(name = HttpConstants.READ_REQUEST)
  @GET
  public Response getAllInstances(@PathParam("clusterId") String clusterId,
      @DefaultValue("getAllInstances") @QueryParam("command") String command) {
    // Get the command. If not provided, the default would be "getAllInstances"
    Command cmd;
    try {
      cmd = Command.valueOf(command);
    } catch (Exception e) {
      return badRequest("Invalid command : " + command);
    }

    HelixDataAccessor accessor = getDataAccssor(clusterId);
    List<String> instances = accessor.getChildNames(accessor.keyBuilder().instanceConfigs());
    if (instances == null) {
      return notFound();
    }

    switch (cmd) {
    case getAllInstances:
      ObjectNode root = JsonNodeFactory.instance.objectNode();
      root.put(Properties.id.name(), JsonNodeFactory.instance.textNode(clusterId));

      // Initialize all arrays
      ArrayNode instancesNode =
          root.putArray(InstancesAccessor.InstancesProperties.instances.name());
      instancesNode.addAll((ArrayNode) OBJECT_MAPPER.valueToTree(instances));

      ArrayNode onlineNode = root.putArray(InstancesAccessor.InstancesProperties.online.name());
      ArrayNode enabledNode = root.putArray(InstancesAccessor.InstancesProperties.enabled.name());
      ArrayNode disabledNode = root.putArray(InstancesAccessor.InstancesProperties.disabled.name());
      ArrayNode evacuatedNode = root.putArray(InstancesAccessor.InstancesProperties.evacuated.name());
      ArrayNode swapInNode = root.putArray(InstancesAccessor.InstancesProperties.swap_in.name());
      ArrayNode unknownNode = root.putArray(InstancesAccessor.InstancesProperties.unknown.name());

      List<String> liveInstances = accessor.getChildNames(accessor.keyBuilder().liveInstances());

      // Categorize each instance by its operation state
      for (String instanceName : instances) {
        InstanceConfig instanceConfig =
            accessor.getProperty(accessor.keyBuilder().instanceConfig(instanceName));
        if (instanceConfig != null) {
          // Get the instance operation
          InstanceConfig.InstanceOperation instanceOperation = instanceConfig.getInstanceOperation();
          InstanceConstants.InstanceOperation operation = instanceOperation.getOperation();

          // Add to online list if live
          if (liveInstances.contains(instanceName)) {
            onlineNode.add(JsonNodeFactory.instance.textNode(instanceName));
          }

          // Categorize by operation state
          switch (operation) {
            case ENABLE:
              enabledNode.add(JsonNodeFactory.instance.textNode(instanceName));
              break;
            case DISABLE:
              disabledNode.add(JsonNodeFactory.instance.textNode(instanceName));
              break;
            case EVACUATE:
              evacuatedNode.add(JsonNodeFactory.instance.textNode(instanceName));
              break;
            case SWAP_IN:
              swapInNode.add(JsonNodeFactory.instance.textNode(instanceName));
              break;
            case UNKNOWN:
              unknownNode.add(JsonNodeFactory.instance.textNode(instanceName));
              break;
            default:
              _logger.warn("Unknown instance operation {} for instance {}. Adding to unknown list.",
                  operation, instanceName);
              unknownNode.add(JsonNodeFactory.instance.textNode(instanceName));
              break;
          }
        }
      }
      return JSONRepresentation(root);
    case validateWeight:
      // Validate all instances for WAGED rebalance
      HelixAdmin admin = getHelixAdmin();
      Map<String, Boolean> validationResultMap;
      try {
        validationResultMap = admin.validateInstancesForWagedRebalance(clusterId, instances);
      } catch (HelixException e) {
        return badRequest(e.getMessage());
      }
      return JSONRepresentation(validationResultMap);
    case getInstancesUnableToAcceptOnlineReplicas:
      return getInstancesUnableToAcceptOnlineReplicas(clusterId, accessor);
    default:
      _logger.error("Unsupported command :" + command);
      return badRequest("Unsupported command :" + command);
    }
  }

  /**
   * Reports the instances Helix itself counts against the cluster-wide offline budget that
   * drives auto Maintenance Mode, so clients do not have to reimplement (and drift from) the
   * controller's membership rules.
   *
   * <p>The population is computed by
   * {@link InstanceUtil#getInstancesUnableToAcceptOnlineReplicas(Map, java.util.Collection, long)},
   * the same method the controller uses on MM entry ({@code BestPossibleStateCalcStage})
   * and MM exit ({@code MaintenanceRecoveryStage}). An instance counts when it is routable,
   * not enabled-and-live, and not covered by a valid instance-operation maintenance marker.
   *
   * <p>Response (HTTP 200), shaped like the {@code getAllInstances} response on this route:
   * <pre>{@code
   * { "id": "cluster0",
   *   "instances_unable_to_accept_online_replicas": ["h3", "h4"] }
   * }</pre>
   *
   * <p>Only the population is returned. The thresholds it is compared against
   * ({@code MAX_OFFLINE_INSTANCES_ALLOWED}, {@code NUM_OFFLINE_INSTANCES_FOR_AUTO_EXIT}) are
   * already available from the cluster-config endpoint, and the resulting maintenance state is
   * available from the maintenance-signal endpoint; deriving either here would hand clients a
   * prediction where an authoritative answer already exists.
   */
  private Response
  getInstancesUnableToAcceptOnlineReplicas(String clusterId,
      HelixDataAccessor accessor) {
    try {
      return computeInstancesUnableToAcceptOnlineReplicas(clusterId, accessor);
    } catch (Exception e) {
      _logger.error("Failed to compute instances unable to accept online replicas for cluster {}",
          clusterId, e);
      return serverError(e);
    }
  }

  private Response computeInstancesUnableToAcceptOnlineReplicas(String clusterId,
      HelixDataAccessor accessor) {
    // An unknown cluster must not fall through to an empty population: to a client, "no instances
    // counted" reads as "the whole offline budget is free". The caller's null check cannot catch
    // it because HelixDataAccessor#getChildNames normalizes a missing path to an empty list, so
    // resolve the cluster explicitly, the same way ClusterAccessor does. ConfigAccessor is not
    // used here because it throws on an unknown cluster, which would surface as a 500.
    if (!ZKUtil.isClusterSetup(clusterId, getRealmAwareZkClient())) {
      return notFound();
    }

    PropertyKey.Builder keyBuilder = accessor.keyBuilder();
    List<InstanceConfig> instanceConfigs =
        accessor.getChildValues(keyBuilder.instanceConfigs(), true);
    Map<String, InstanceConfig> instanceConfigMap = new HashMap<>();
    if (instanceConfigs != null) {
      for (InstanceConfig instanceConfig : instanceConfigs) {
        if (instanceConfig != null) {
          instanceConfigMap.put(instanceConfig.getInstanceName(), instanceConfig);
        }
      }
    }
    List<String> liveInstances = accessor.getChildNames(keyBuilder.liveInstances());

    Set<String> unableToAcceptOnlineReplicas =
        InstanceUtil.getInstancesUnableToAcceptOnlineReplicas(instanceConfigMap,
            liveInstances == null ? Collections.emptyList() : liveInstances,
            System.currentTimeMillis());

    ObjectNode root = JsonNodeFactory.instance.objectNode();
    root.put(Properties.id.name(), clusterId);
    ArrayNode countedNode =
        root.putArray(InstancesProperties.instances_unable_to_accept_online_replicas.name());
    // Sorted so the payload is stable across calls for the same cluster state.
    for (String instanceName : new TreeSet<>(unableToAcceptOnlineReplicas)) {
      countedNode.add(instanceName);
    }
    return JSONRepresentation(root);
  }

  @ResponseMetered(name = HttpConstants.WRITE_REQUEST)
  @Timed(name = HttpConstants.WRITE_REQUEST)
  @POST
  public Response instancesOperations(@PathParam("clusterId") String clusterId,
      @QueryParam("command") String command,
      @QueryParam("continueOnFailures") boolean continueOnFailures,
      @QueryParam("skipZKRead") boolean skipZKRead,
      @QueryParam("skipHealthCheckCategories") String skipHealthCheckCategories,
      @DefaultValue("false") @QueryParam("random") boolean random,
      @DefaultValue("false") @QueryParam("includeDetails") boolean includeDetails, String content) {
    Command cmd;
    try {
      cmd = Command.valueOf(command);
    } catch (Exception e) {
      return badRequest("Invalid command : " + command);
    }

    Set<StoppableCheck.Category> skipHealthCheckCategorySet;
    try {
      skipHealthCheckCategorySet = skipHealthCheckCategories != null
          ? StoppableCheck.Category.categorySetFromCommaSeperatedString(skipHealthCheckCategories)
          : Collections.emptySet();
      if (!MaintenanceManagementService.SKIPPABLE_HEALTH_CHECK_CATEGORIES.containsAll(
          skipHealthCheckCategorySet)) {
        throw new IllegalArgumentException(
            "Some of the provided skipHealthCheckCategories are not skippable. The supported skippable categories are: "
                + MaintenanceManagementService.SKIPPABLE_HEALTH_CHECK_CATEGORIES);
      }
    } catch (Exception e) {
      return badRequest("Invalid skipHealthCheckCategories: " + skipHealthCheckCategories + "\n"
          + e.getMessage());
    }

    HelixAdmin admin = getHelixAdmin();
    try {
      JsonNode node = null;
      if (content.length() != 0) {
        node = OBJECT_MAPPER.readTree(content);
      }
      if (node == null) {
        return badRequest("Invalid input for content : " + content);
      }
      List<String> enableInstances = OBJECT_MAPPER
          .readValue(node.get(InstancesAccessor.InstancesProperties.instances.name()).toString(),
              OBJECT_MAPPER.getTypeFactory().constructCollectionType(List.class, String.class));
      switch (cmd) {
        case enable:
          admin.enableInstance(clusterId, enableInstances, true);
          break;
        case disable:
          admin.enableInstance(clusterId, enableInstances, false);
          break;
        case stoppable:
          return batchGetStoppableInstances(clusterId, node, skipZKRead, continueOnFailures,
              skipHealthCheckCategorySet, random, includeDetails);
        case instanceOperationMaintenance:
          return batchSetInstanceOperationMaintenance(clusterId, node);
        default:
          _logger.error("Unsupported command :" + command);
          return badRequest("Unsupported command :" + command);
      }
    } catch (HelixHealthException e) {
      _logger
          .error(String.format("Current cluster %s has issue with health checks!", clusterId), e);
      return serverError(e);
    } catch (Exception e) {
      _logger.error("Failed in updating instances : " + content, e);
      return badRequest(e.getMessage());
    }
    return OK();
  }

  /**
   * Batch counterpart of {@code POST /clusters/{c}/instances/{i}/instanceOperationMaintenance}.
   * Sets or clears the instance-operation maintenance marker on a list of instances. Mirrors
   * the partial-accept contract of the batch stoppable check: instances are processed in
   * input order, those that fit the cap quota (or that exist on a clear) are listed under
   * {@code applied}, the rest under {@code rejected} keyed by reason. Caller-side bugs that
   * invalidate the entire request (missing {@code instances}, bad JSON, past expiry,
   * missing expiry with no cluster default) are still surfaced as 400.
   *
   * <p>Request body:
   * <pre>{@code
   * { "instances": ["h1", "h2", ...],
   *   "expiresAtMillis": 1776385800000 }
   * }</pre>
   *
   * <p>Success response (HTTP 200):
   * <pre>{@code
   * { "applied":  ["h1", "h2"],
   *   "rejected": { "h3": "would exceed INSTANCE_OPERATION_MAINTENANCE_BUDGET=2" },
   *   "expiresAtMillis": 1776385800000 }
   * }</pre>
   */
  private Response batchSetInstanceOperationMaintenance(String clusterId, JsonNode node) {
    try {
      JsonNode instancesNode = node.get(InstancesProperties.instances.name());
      if (instancesNode == null || !instancesNode.isArray() || instancesNode.size() == 0) {
        return badRequest("Field 'instances' must be a non-empty array");
      }
      List<String> instances = new ArrayList<>(instancesNode.size());
      for (JsonNode element : instancesNode) {
        instances.add(element.asText());
      }
      long expiresAtMillis = node.path("expiresAtMillis")
          .asLong(InstanceOperationMaintenanceWriteHandler.EXPIRES_AT_MILLIS_UNSET);

      InstanceOperationMaintenanceWriteHandler handler =
          new InstanceOperationMaintenanceWriteHandler(getHelixAdmin(), getConfigAccessor());
      InstanceOperationMaintenanceWriteHandler.InstanceOperationMaintenanceResult result =
          handler.apply(clusterId, instances, expiresAtMillis, System.currentTimeMillis());

      ObjectNode body = JsonNodeFactory.instance.objectNode();
      ArrayNode appliedArr = body.putArray("applied");
      for (String name : result.getApplied()) {
        appliedArr.add(name);
      }
      ObjectNode rejectedNode = body.putObject("rejected");
      for (Map.Entry<String, String> entry : result.getRejected().entrySet()) {
        rejectedNode.put(entry.getKey(), entry.getValue());
      }
      body.put("expiresAtMillis", result.getResolvedExpiresAtMillis());
      return JSONRepresentation(body);
    } catch (BadRequestException e) {
      return badRequest(e.getMessage());
    } catch (Exception e) {
      _logger.error("Failed to set instance-operation maintenance batch in cluster {}",
          clusterId, e);
      return serverError(e);
    }
  }

  private Response batchGetStoppableInstances(String clusterId, JsonNode node, boolean skipZKRead,
      boolean continueOnFailures, Set<StoppableCheck.Category> skipHealthCheckCategories,
      boolean random, boolean includeDetails) throws IOException {
    try {
      // TODO: Process input data from the content
      // TODO: Implement the logic to automatically detect the selection base. https://github.com/apache/helix/issues/2968#issue-2691677799
      InstancesAccessor.InstanceHealthSelectionBase selectionBase =
          node.get(InstancesAccessor.InstancesProperties.selection_base.name()) == null
              ? InstanceHealthSelectionBase.non_zone_based : InstanceHealthSelectionBase.valueOf(
              node.get(InstancesAccessor.InstancesProperties.selection_base.name()).textValue());

      List<String> instances = OBJECT_MAPPER.readValue(
          node.get(InstancesAccessor.InstancesProperties.instances.name()).toString(),
          OBJECT_MAPPER.getTypeFactory().constructCollectionType(List.class, String.class));
      ClusterService clusterService =
          new ClusterServiceImpl(getDataAccssor(clusterId), getConfigAccessor());

      List<String> orderOfZone = null;
      String customizedInput = null;
      List<String> toBeStoppedInstances = Collections.emptyList();
      // By default, if skip_stoppable_check_list is unset, all checks are performed to maintain
      // backward compatibility with existing clients.
      List<HealthCheck> skipStoppableCheckList = Collections.emptyList();
      if (node.get(InstancesAccessor.InstancesProperties.customized_values.name()) != null) {
        customizedInput =
            node.get(InstancesAccessor.InstancesProperties.customized_values.name()).toString();
      }

      if (node.get(InstancesAccessor.InstancesProperties.zone_order.name()) != null) {
        orderOfZone = OBJECT_MAPPER.readValue(
            node.get(InstancesAccessor.InstancesProperties.zone_order.name()).toString(),
            OBJECT_MAPPER.getTypeFactory().constructCollectionType(List.class, String.class));
        if (!orderOfZone.isEmpty() && random) {
          String message =
              "Both 'zone_order' and 'random' parameters are set. Please specify only one option.";
          _logger.error(message);
          return badRequest(message);
        }
        if (!orderOfZone.isEmpty() && selectionBase == InstanceHealthSelectionBase.non_zone_based) {
          String message =
              "'zone_order' is set but 'selection_base' is 'non_zone_based'. Please set 'selection_base' to 'zone_based' or 'cross_zone_based'.";
          _logger.error(message);
          return badRequest(message);
        }
      }

      if (node.get(InstancesAccessor.InstancesProperties.to_be_stopped_instances.name()) != null) {
        toBeStoppedInstances = OBJECT_MAPPER.readValue(
            node.get(InstancesProperties.to_be_stopped_instances.name()).toString(),
            OBJECT_MAPPER.getTypeFactory().constructCollectionType(List.class, String.class));
        Set<String> instanceSet = new HashSet<>(instances);
        instanceSet.retainAll(toBeStoppedInstances);
        if (!instanceSet.isEmpty()) {
          String message =
              "'to_be_stopped_instances' and 'instances' have intersection: " + instanceSet
                  + ". Please make them mutually exclusive.";
          _logger.error(message);
          return badRequest(message);
        }
      }

      if (node.get(InstancesProperties.skip_stoppable_check_list.name()) != null) {
        List<String> list = OBJECT_MAPPER.readValue(
            node.get(InstancesProperties.skip_stoppable_check_list.name()).toString(),
            OBJECT_MAPPER.getTypeFactory().constructCollectionType(List.class, String.class));
        try {
          skipStoppableCheckList =
              list.stream().map(HealthCheck::valueOf).collect(Collectors.toList());
        } catch (IllegalArgumentException e) {
          String message =
              "'skip_stoppable_check_list' has invalid check names: " + list
                  + ". Supported checks: " + HealthCheck.STOPPABLE_CHECK_LIST;
          _logger.error(message, e);
          return badRequest(message);
        }
      }

      boolean skipCustomChecksIfNoLiveness = false;
      if (node.get(InstancesProperties.skip_custom_check_if_instance_not_alive.name()) != null) {
        skipCustomChecksIfNoLiveness = node.get(
                InstancesAccessor.InstancesProperties.skip_custom_check_if_instance_not_alive.name())
            .asBoolean();
      }

      ClusterTopology clusterTopology = clusterService.getClusterTopology(clusterId);
      if (selectionBase != InstanceHealthSelectionBase.non_zone_based) {
        if (!clusterService.isClusterTopologyAware(clusterId)) {
          String message = "Cluster " + clusterId
              + " is not topology aware. Please enable the topology in cluster config or set "
              + "'selection_base' to 'non_zone_based'.";
          _logger.error(message);
          return badRequest(message);
        }

        // Find instances that lack topology information
        Set<String> instancesWithTopology =
            clusterTopology.toZoneMapping().entrySet().stream().flatMap(entry -> entry.getValue().stream())
                .collect(Collectors.toSet());
        Set<String> allInstances = clusterTopology.getAllInstances();
        Set<String> topologyUnawareInstances = new HashSet<>(instances).stream().filter(
                instance -> !instancesWithTopology.contains(instance) && allInstances.contains(instance))
            .collect(Collectors.toSet());
        if (!topologyUnawareInstances.isEmpty()) {
          String message = "Instances " + topologyUnawareInstances
              + " do not have topology information. Please set topology information in instance config or"
              + " set 'selection_base' to 'non_zone_based'.";
          _logger.error(message);
          return badRequest(message);
        }
      }

      String namespace = getNamespace();
      MaintenanceManagementService maintenanceService =
          new MaintenanceManagementService.MaintenanceManagementServiceBuilder()
              .setDataAccessor((ZKHelixDataAccessor) getDataAccssor(clusterId))
              .setConfigAccessor(getConfigAccessor())
              .setSkipZKRead(skipZKRead)
              .setNonBlockingHealthChecks(
                  continueOnFailures ? Collections.singleton(ALL_HEALTH_CHECK_NONBLOCK) : null)
              .setCustomRestClient(CustomRestClientFactory.get())
              .setSkipHealthCheckCategories(skipHealthCheckCategories)
              .setNamespace(namespace)
              .setSkipStoppableHealthCheckList(skipStoppableCheckList)
              .setSkipCustomChecksIfNoLiveness(skipCustomChecksIfNoLiveness)
              .build();

      StoppableInstancesSelector stoppableInstancesSelector =
          new StoppableInstancesSelector.StoppableInstancesSelectorBuilder()
              .setClusterId(clusterId)
              .setOrderOfZone(orderOfZone)
              .setCustomizedInput(customizedInput)
              .setMaintenanceService(maintenanceService)
              .setClusterTopology(clusterTopology)
              .setDataAccessor((ZKHelixDataAccessor) getDataAccssor(clusterId))
              .setIncludeDetails(includeDetails)
              .build();
      ObjectNode result;

      switch (selectionBase) {
        case zone_based:
          stoppableInstancesSelector.calculateOrderOfZone(instances, random);
          result = stoppableInstancesSelector.getStoppableInstancesInSingleZone(instances, toBeStoppedInstances);
          break;
        case cross_zone_based:
          stoppableInstancesSelector.calculateOrderOfZone(instances, random);
          result = stoppableInstancesSelector.getStoppableInstancesCrossZones(instances, toBeStoppedInstances);
          break;
        case non_zone_based:
          result = stoppableInstancesSelector.getStoppableInstancesNonZoneBased(instances, toBeStoppedInstances);
          break;
        default:
          throw new UnsupportedOperationException("instance_based selection is not supported yet!");
      }
      return JSONRepresentation(result);
    } catch (HelixException e) {
      _logger
              .error(String.format("Current cluster %s has issue with health checks!", clusterId), e);
      throw new HelixHealthException(e);
    } catch (Exception e) {
      _logger.error(String.format(
              "Failed to get parallel stoppable instances for cluster %s with a HelixException!",
              clusterId), e);
      throw e;
    }
  }

}
