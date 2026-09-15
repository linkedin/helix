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
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import javax.ws.rs.DELETE;
import javax.ws.rs.DefaultValue;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.PUT;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.Response;

import com.codahale.metrics.annotation.ResponseMetered;
import com.codahale.metrics.annotation.Timed;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.helix.ConfigAccessor;
import org.apache.helix.HelixAdmin;
import org.apache.helix.HelixException;
import org.apache.helix.PropertyPathBuilder;
import org.apache.helix.guardrail.GuardrailContext;
import org.apache.helix.guardrail.GuardrailPipeline;
import org.apache.helix.guardrail.rules.PartitionWeightCapacityGuardrailRule;
import org.apache.helix.model.CustomizedView;
import org.apache.helix.model.ExternalView;
import org.apache.helix.model.HelixConfigScope;
import org.apache.helix.model.IdealState;
import org.apache.helix.model.ResourceConfig;
import org.apache.helix.model.StateModelDefinition;
import org.apache.helix.model.builder.HelixConfigScopeBuilder;
import org.apache.helix.rest.common.HttpConstants;
import org.apache.helix.rest.server.filters.ClusterAuth;
import org.apache.helix.rest.server.service.ResourceReplicaCountService;
import org.apache.helix.zookeeper.api.client.RealmAwareZkClient;
import org.apache.helix.zookeeper.datamodel.ZNRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@ClusterAuth
@Path("/clusters/{clusterId}/resources")
public class ResourceAccessor extends AbstractHelixResource {
  private final static Logger _logger = LoggerFactory.getLogger(ResourceAccessor.class);

  public enum ResourceProperties {
    idealState,
    idealStates,
    externalView,
    externalViews,
    resourceConfig,
  }

  public enum HealthStatus {
    HEALTHY,
    PARTIAL_HEALTHY,
    UNHEALTHY
  }

  /**
   * Fields of the {@code updateReplicaCounts} request body. Any other field is rejected, so that a
   * misspelled field cannot be silently dropped and reported as a successful update.
   */
  public enum ReplicaCountUpdateRequestProperties {
    selection,
    resources,
    replicas,
    minActiveReplicas
  }

  /**
   * Fields of the {@code updateReplicaCounts} response body.
   */
  public enum ReplicaCountUpdateResponseProperties {
    selection,
    replicas,
    minActiveReplicas,
    selectedResourceCount,
    allAtDesiredValues,
    statusCounts,
    resourceResults,
    status,
    version,
    message
  }

  @ResponseMetered(name = HttpConstants.READ_REQUEST)
  @Timed(name = HttpConstants.READ_REQUEST)
  @GET
  public Response getResources(@PathParam("clusterId") String clusterId) {
    ObjectNode root = JsonNodeFactory.instance.objectNode();
    root.put(Properties.id.name(), JsonNodeFactory.instance.textNode(clusterId));

    RealmAwareZkClient zkClient = getRealmAwareZkClient();

    ArrayNode idealStatesNode = root.putArray(ResourceProperties.idealStates.name());
    ArrayNode externalViewsNode = root.putArray(ResourceProperties.externalViews.name());

    List<String> idealStates = zkClient.getChildren(PropertyPathBuilder.idealState(clusterId));
    List<String> externalViews = zkClient.getChildren(PropertyPathBuilder.externalView(clusterId));

    if (idealStates != null) {
      idealStatesNode.addAll((ArrayNode) OBJECT_MAPPER.valueToTree(idealStates));
    } else {
      return notFound();
    }

    if (externalViews != null) {
      externalViewsNode.addAll((ArrayNode) OBJECT_MAPPER.valueToTree(externalViews));
    }

    return JSONRepresentation(root);
  }

  /**
   * Applies one replica count and/or one minimum active replica count to a scoped set of resources
   * in a single request, and returns what happened to each of them.
   *
   * <p>The desired values stay with the caller; this endpoint does not derive them. What it removes
   * from the caller is the resource enumeration, the per-resource write sequencing and the
   * partial-failure bookkeeping.
   *
   * <p>The request body is a JSON object:
   *
   * <pre>
   * {
   *   "selection": "ALL_RESOURCES" | "EXPLICIT",
   *   "resources": ["resource1", "resource2"],
   *   "replicas": 3,
   *   "minActiveReplicas": 2
   * }
   * </pre>
   *
   * {@code resources} is required for {@code EXPLICIT} and must be absent for
   * {@code ALL_RESOURCES}, which selects every resource that has an IdealState when the request is
   * served. At least one of {@code replicas} and {@code minActiveReplicas} is required; an omitted
   * one is left untouched, although the resulting combination is still checked per resource. Any
   * other field is rejected.
   *
   * <p>A {@code 200} means the request was processed, not that every resource was updated. The
   * operation is not atomic and does not roll back, so the caller must read
   * {@code resourceResults} and treat anything other than {@code APPLIED} or {@code UNCHANGED} as
   * "desired values not confirmed on this resource". Re-sending the same request is safe.
   *
   * @param clusterId the cluster that owns the resources
   * @param commandStr must be {@code updateReplicaCounts}
   * @param content the JSON request body
   * @return the per-resource outcomes, or an error when the request or the cluster is not valid
   */
  @ResponseMetered(name = HttpConstants.WRITE_REQUEST)
  @Timed(name = HttpConstants.WRITE_REQUEST)
  @POST
  public Response updateResources(@PathParam("clusterId") String clusterId,
      @QueryParam("command") String commandStr, String content) {
    Command command;
    try {
      command = getCommand(commandStr);
    } catch (HelixException ex) {
      return badRequest(ex.getMessage());
    }
    if (command != Command.updateReplicaCounts) {
      return badRequest(String.format("Unsupported command: %s", commandStr));
    }

    JsonNode request;
    try {
      request = content == null ? null : OBJECT_MAPPER.readTree(content);
    } catch (IOException e) {
      return badRequest("Input is not valid JSON: " + e.getMessage());
    }
    if (request == null || !request.isObject()) {
      return badRequest("Input must be a JSON object.");
    }
    Optional<String> unknownField = findUnknownRequestField(request);
    if (unknownField.isPresent()) {
      return badRequest("Unsupported request field: " + unknownField.get());
    }

    ResourceReplicaCountService.ResourceSelection selection;
    Integer replicas;
    Integer minActiveReplicas;
    try {
      selection = readSelection(request);
      replicas =
          readOptionalInt(request, ReplicaCountUpdateRequestProperties.replicas.name());
      minActiveReplicas =
          readOptionalInt(request, ReplicaCountUpdateRequestProperties.minActiveReplicas.name());
      // Reject values that cannot be honoured before the cluster is read at all, so that an
      // invalid request never reaches the metadata store.
      ResourceReplicaCountService.validateReplicaCounts(replicas, minActiveReplicas);
    } catch (IllegalArgumentException ex) {
      return badRequest(ex.getMessage());
    }

    ResourceReplicaCountService service =
        new ResourceReplicaCountService(getDataAccssor(clusterId).getBaseDataAccessor());
    List<String> selectedResources;
    try {
      if (selection == ResourceReplicaCountService.ResourceSelection.EXPLICIT) {
        selectedResources = readExplicitResources(request);
      } else {
        if (request.has(ReplicaCountUpdateRequestProperties.resources.name())) {
          return badRequest(String.format("%s must not be set when selection is %s.",
              ReplicaCountUpdateRequestProperties.resources.name(),
              ResourceReplicaCountService.ResourceSelection.ALL_RESOURCES.name()));
        }
        selectedResources = service.listResources(clusterId);
        if (selectedResources == null) {
          return notFound("Cluster " + clusterId + " has no IdealState path.");
        }
      }
    } catch (IllegalArgumentException ex) {
      return badRequest(ex.getMessage());
    } catch (Exception ex) {
      _logger.error("Failed to select resources of cluster {}.", clusterId, ex);
      return serverError(ex);
    }

    ResourceReplicaCountService.BulkReplicaCountUpdateResult result;
    try {
      result = service.updateReplicaCounts(clusterId, selectedResources, replicas,
          minActiveReplicas);
    } catch (IllegalArgumentException ex) {
      return badRequest(ex.getMessage());
    } catch (Exception ex) {
      _logger.error("Failed to update replica counts of cluster {}.", clusterId, ex);
      return serverError(ex);
    }
    return JSONRepresentation(
        toReplicaCountUpdateNode(clusterId, selection, replicas, minActiveReplicas, result));
  }

  private static Optional<String> findUnknownRequestField(JsonNode request) {
    Set<String> known = new HashSet<>();
    for (ReplicaCountUpdateRequestProperties property : ReplicaCountUpdateRequestProperties
        .values()) {
      known.add(property.name());
    }
    Iterator<String> fields = request.fieldNames();
    while (fields.hasNext()) {
      String field = fields.next();
      if (!known.contains(field)) {
        return Optional.of(field);
      }
    }
    return Optional.empty();
  }

  private static ResourceReplicaCountService.ResourceSelection readSelection(JsonNode request) {
    JsonNode node = request.get(ReplicaCountUpdateRequestProperties.selection.name());
    if (node == null || !node.isTextual()) {
      return throwMissingField(ReplicaCountUpdateRequestProperties.selection.name(),
          "one of " + Arrays.toString(
              ResourceReplicaCountService.ResourceSelection.values()));
    }
    try {
      return ResourceReplicaCountService.ResourceSelection.valueOf(node.textValue());
    } catch (IllegalArgumentException ex) {
      throw new IllegalArgumentException(String.format("Unsupported %s: %s. Expected one of %s.",
          ReplicaCountUpdateRequestProperties.selection.name(), node.textValue(),
          Arrays.toString(ResourceReplicaCountService.ResourceSelection.values())));
    }
  }

  private static List<String> readExplicitResources(JsonNode request) {
    JsonNode node = request.get(ReplicaCountUpdateRequestProperties.resources.name());
    if (node == null || !node.isArray() || node.size() == 0) {
      return throwMissingField(ReplicaCountUpdateRequestProperties.resources.name(),
          "a non-empty array of resource names");
    }
    List<String> resources = new ArrayList<>(node.size());
    for (JsonNode entry : node) {
      if (!entry.isTextual()) {
        throw new IllegalArgumentException(String.format("%s must contain only resource names.",
            ReplicaCountUpdateRequestProperties.resources.name()));
      }
      resources.add(entry.textValue());
    }
    return resources;
  }

  private static Integer readOptionalInt(JsonNode request, String field) {
    JsonNode node = request.get(field);
    if (node == null || node.isNull()) {
      return null;
    }
    if (!node.isInt()) {
      throw new IllegalArgumentException(field + " must be an integer.");
    }
    return node.intValue();
  }

  private static <T> T throwMissingField(String field, String expected) {
    throw new IllegalArgumentException(field + " is required and must be " + expected + ".");
  }

  private static ObjectNode toReplicaCountUpdateNode(String clusterId,
      ResourceReplicaCountService.ResourceSelection selection, Integer replicas,
      Integer minActiveReplicas,
      ResourceReplicaCountService.BulkReplicaCountUpdateResult result) {
    ObjectNode root = JsonNodeFactory.instance.objectNode();
    root.put(Properties.id.name(), clusterId);
    root.put(ReplicaCountUpdateResponseProperties.selection.name(), selection.name());
    if (replicas != null) {
      root.put(ReplicaCountUpdateResponseProperties.replicas.name(), replicas.intValue());
    }
    if (minActiveReplicas != null) {
      root.put(ReplicaCountUpdateResponseProperties.minActiveReplicas.name(),
          minActiveReplicas.intValue());
    }
    root.put(ReplicaCountUpdateResponseProperties.selectedResourceCount.name(),
        result.getSelectedResources().size());
    root.put(ReplicaCountUpdateResponseProperties.allAtDesiredValues.name(),
        result.isAllAtDesiredValues());

    ObjectNode statusCounts =
        root.putObject(ReplicaCountUpdateResponseProperties.statusCounts.name());
    result.getStatusCounts().forEach((status, count) -> statusCounts.put(status.name(), count));

    ObjectNode resourceResults =
        root.putObject(ReplicaCountUpdateResponseProperties.resourceResults.name());
    for (String resourceName : result.getSelectedResources()) {
      ResourceReplicaCountService.ResourceUpdateOutcome outcome =
          result.getOutcomes().get(resourceName);
      ObjectNode outcomeNode = resourceResults.putObject(resourceName);
      outcomeNode.put(ReplicaCountUpdateResponseProperties.status.name(),
          outcome.getStatus().name());
      outcomeNode.put(ReplicaCountUpdateResponseProperties.version.name(), outcome.getVersion());
      if (outcome.getMessage() != null) {
        outcomeNode.put(ReplicaCountUpdateResponseProperties.message.name(),
            outcome.getMessage());
      }
    }
    return root;
  }

  /**
   * Returns health profile of all resources in the cluster
   * @param clusterId
   * @return JSON result
   */
  @ResponseMetered(name = HttpConstants.READ_REQUEST)
  @Timed(name = HttpConstants.READ_REQUEST)
  @GET
  @Path("health")
  public Response getResourceHealth(@PathParam("clusterId") String clusterId) {

    RealmAwareZkClient zkClient = getRealmAwareZkClient();

    List<String> resourcesInIdealState =
        zkClient.getChildren(PropertyPathBuilder.idealState(clusterId));
    List<String> resourcesInExternalView =
        zkClient.getChildren(PropertyPathBuilder.externalView(clusterId));

    Map<String, String> resourceHealthResult = new HashMap<>();

    for (String resourceName : resourcesInIdealState) {
      if (resourcesInExternalView.contains(resourceName)) {
        Map<String, String> partitionHealth = computePartitionHealth(clusterId, resourceName);

        if (partitionHealth.isEmpty()
            || partitionHealth.values().contains(HealthStatus.UNHEALTHY.name())) {
          // No partitions for a resource or there exists one or more UNHEALTHY partitions in this
          // resource, UNHEALTHY
          resourceHealthResult.put(resourceName, HealthStatus.UNHEALTHY.name());
        } else if (partitionHealth.values().contains(HealthStatus.PARTIAL_HEALTHY.name())) {
          // No UNHEALTHY partition, but one or more partially healthy partitions, resource is
          // partially healthy
          resourceHealthResult.put(resourceName, HealthStatus.PARTIAL_HEALTHY.name());
        } else {
          // No UNHEALTHY or partially healthy partitions and non-empty, resource is healthy
          resourceHealthResult.put(resourceName, HealthStatus.HEALTHY.name());
        }
      } else {
        // If a resource is not in ExternalView, then it is UNHEALTHY
        resourceHealthResult.put(resourceName, HealthStatus.UNHEALTHY.name());
      }
    }

    return JSONRepresentation(resourceHealthResult);
  }

  /**
   * Returns health profile of all partitions for the corresponding resource in the cluster
   * @param clusterId
   * @param resourceName
   * @return JSON result
   * @throws IOException
   */
  @ResponseMetered(name = HttpConstants.READ_REQUEST)
  @Timed(name = HttpConstants.READ_REQUEST)
  @GET
  @Path("{resourceName}/health")
  public Response getPartitionHealth(@PathParam("clusterId") String clusterId,
      @PathParam("resourceName") String resourceName) {

    return JSONRepresentation(computePartitionHealth(clusterId, resourceName));
  }

  @ResponseMetered(name = HttpConstants.READ_REQUEST)
  @Timed(name = HttpConstants.READ_REQUEST)
  @GET
  @Path("{resourceName}")
  public Response getResource(@PathParam("clusterId") String clusterId,
      @PathParam("resourceName") String resourceName,
      @DefaultValue("getResource") @QueryParam("command") String command) {
    // Get the command. If not provided, the default would be "getResource"
    Command cmd;
    try {
      cmd = Command.valueOf(command);
    } catch (Exception e) {
      return badRequest("Invalid command : " + command);
    }
    ConfigAccessor accessor = getConfigAccessor();
    HelixAdmin admin = getHelixAdmin();

    switch (cmd) {
    case getResource:
      ResourceConfig resourceConfig = accessor.getResourceConfig(clusterId, resourceName);
      IdealState idealState = admin.getResourceIdealState(clusterId, resourceName);
      ExternalView externalView = admin.getResourceExternalView(clusterId, resourceName);

      Map<String, ZNRecord> resourceMap = new HashMap<>();
      if (idealState != null) {
        resourceMap.put(ResourceProperties.idealState.name(), idealState.getRecord());
      } else {
        return notFound();
      }

      resourceMap.put(ResourceProperties.resourceConfig.name(), null);
      resourceMap.put(ResourceProperties.externalView.name(), null);

      if (resourceConfig != null) {
        resourceMap.put(ResourceProperties.resourceConfig.name(), resourceConfig.getRecord());
      }

      if (externalView != null) {
        resourceMap.put(ResourceProperties.externalView.name(), externalView.getRecord());
      }
      return JSONRepresentation(resourceMap);
    case validateWeight:
      // Validate ResourceConfig for WAGED rebalance
      Map<String, Boolean> validationResultMap;
      try {
        validationResultMap = admin.validateResourcesForWagedRebalance(clusterId,
            Collections.singletonList(resourceName));
      } catch (HelixException e) {
        return badRequest(e.getMessage());
      }
      return JSONRepresentation(validationResultMap);
    default:
      _logger.error("Unsupported command :" + command);
      return badRequest("Unsupported command :" + command);
    }
  }

  @ResponseMetered(name = HttpConstants.WRITE_REQUEST)
  @Timed(name = HttpConstants.WRITE_REQUEST)
  @PUT
  @Path("{resourceName}")
  public Response addResource(@PathParam("clusterId") String clusterId,
      @PathParam("resourceName") String resourceName,
      @DefaultValue("-1") @QueryParam("numPartitions") int numPartitions,
      @DefaultValue("") @QueryParam("stateModelRef") String stateModelRef,
      @DefaultValue("SEMI_AUTO") @QueryParam("rebalancerMode") String rebalancerMode,
      @DefaultValue("DEFAULT") @QueryParam("rebalanceStrategy") String rebalanceStrategy,
      @DefaultValue("0") @QueryParam("bucketSize") int bucketSize,
      @DefaultValue("-1") @QueryParam("maxPartitionsPerInstance") int maxPartitionsPerInstance,
      @DefaultValue("addResource") @QueryParam("command") String command,
      @DefaultValue("false") @QueryParam("force") boolean force,
      @DefaultValue("false") @QueryParam("dryRun") boolean dryRun, String content) {
    // Get the command. If not provided, the default would be "addResource"
    Command cmd;
    try {
      cmd = Command.valueOf(command);
    } catch (Exception e) {
      return badRequest("Invalid command : " + command);
    }
    // force and dryRun are only honored by commands that run a guard rail pipeline (currently only
    // addWagedResource). For any other command they are silently ignored and, worse, dryRun=true on
    // a plain addResource would still perform a real write — the opposite of a simulation. Reject
    // them up front for unsupported commands so callers are never misled into thinking a mutation
    // was simulated or its violations overridden.
    if ((force || dryRun) && cmd != Command.addWagedResource) {
      return badRequest(String.format(
          "The 'force' and 'dryRun' flags are only supported for the 'addWagedResource' command, "
              + "not '%s'.", command));
    }
    HelixAdmin admin = getHelixAdmin();
    try {
      switch (cmd) {
      case addResource:
        if (content.length() != 0) {
          ZNRecord record;
          try {
            record = toZNRecord(content);
          } catch (IOException e) {
            _logger.error("Failed to deserialize user's input " + content + ", Exception: " + e);
            return badRequest("Input is not a valid ZNRecord!");
          }

          if (record.getSimpleFields() != null) {
            admin.addResource(clusterId, resourceName, new IdealState(record));
          }
        } else {
          admin.addResource(clusterId, resourceName, numPartitions, stateModelRef, rebalancerMode,
              rebalanceStrategy, bucketSize, maxPartitionsPerInstance);
        }
        break;
      case addWagedResource:
        // Check if content is valid
        if (content == null || content.length() == 0) {
          _logger.error("Input is null or empty!");
          return badRequest("Input is null or empty!");
        }
        Map<String, ZNRecord> input;
        // Content must supply both IdealState and ResourceConfig
        try {
          TypeReference<Map<String, ZNRecord>> typeRef =
              new TypeReference<Map<String, ZNRecord>>() {
              };
          input = ZNRECORD_READER.forType(typeRef).readValue(content);
        } catch (IOException e) {
          _logger.error("Failed to deserialize user's input {}, Exception: {}", content, e);
          return badRequest("Input is not a valid map of String-ZNRecord pairs!");
        }
        // Check if the map contains both IdealState and ResourceConfig
        ZNRecord idealStateRecord =
            input.get(ResourceAccessor.ResourceProperties.idealState.name());
        ZNRecord resourceConfigRecord =
            input.get(ResourceAccessor.ResourceProperties.resourceConfig.name());

        if (idealStateRecord == null || resourceConfigRecord == null) {
          _logger.error("Input does not contain both IdealState and ResourceConfig!");
          return badRequest("Input does not contain both IdealState and ResourceConfig!");
        }

        ResourceConfig proposedResourceConfig = new ResourceConfig(resourceConfigRecord);
        IdealState proposedIdealState = new IdealState(idealStateRecord);

        // Cheap, local structural validation before any ZK-backed guard rail work. Running it here
        // means these failures are reflected by a dry-run (instead of a misleading feasible verdict)
        // and are caught before the guard rail's instance-config scan, so a structurally invalid
        // request never reaches ZooKeeper.
        Optional<Response> structuralError =
            validateWagedResourceStructure(proposedIdealState, proposedResourceConfig);
        if (structuralError.isPresent()) {
          return structuralError.get();
        }

        // Guard rail: block (or simulate) adding a resource whose partition weight exceeds the
        // largest single instance's capacity in any dimension, which would make it permanently
        // unplaceable. force=true overrides; dryRun=true only reports the verdict without writing.
        GuardrailContext context = GuardrailContext.newBuilder(clusterId)
            .dataAccessor(getDataAccssor(clusterId))
            .proposedResourceConfig(proposedResourceConfig)
            .proposedIdealState(proposedIdealState)
            .build();
        GuardrailPipeline pipeline =
            new GuardrailPipeline(new PartitionWeightCapacityGuardrailRule());
        Optional<Response> preflightResponse = preflight(pipeline, context, force, dryRun);
        if (preflightResponse.isPresent()) {
          return preflightResponse.get();
        }

        // Add using HelixAdmin API
        try {
          admin.addResourceWithWeight(clusterId, proposedIdealState, proposedResourceConfig);
        } catch (HelixException e) {
          String errMsg = String.format("Failed to add resource %s with weight in cluster %s!",
              idealStateRecord.getId(), clusterId);
          _logger.error(errMsg, e);
          return badRequest(errMsg);
        }
        break;
      default:
        _logger.error("Unsupported command :" + command);
        return badRequest("Unsupported command :" + command);
      }
    } catch (Exception e) {
      _logger.error("Error in adding a resource: " + resourceName, e);
      return serverError(e);
    }
    return OK();
  }

  /**
   * Cheap, local (no ZooKeeper) structural checks for an addWagedResource request. Returns a
   * {@code 400} response if the request is malformed, or {@link Optional#empty()} if it is
   * structurally sound. These are validated before the guard rail pipeline so that a dry-run
   * reflects them and a structurally invalid request never triggers the guard rail's instance-config
   * read.
   */
  private Optional<Response> validateWagedResourceStructure(IdealState idealState,
      ResourceConfig resourceConfig) {
    // IdealState and ResourceConfig must describe the same resource. addResourceWithWeight enforces
    // this on the write path, but checking here means a dry-run reports it instead of returning a
    // feasible verdict for a request that would then fail for real.
    if (!idealState.getResourceName().equals(resourceConfig.getResourceName())) {
      return Optional.of(badRequest(String.format(
          "Resource names in IdealState (%s) and ResourceConfig (%s) are different!",
          idealState.getResourceName(), resourceConfig.getResourceName())));
    }

    // Partition weights must be non-negative. ResourceConfig#setPartitionCapacityMap rejects
    // negatives, but this endpoint constructs the ResourceConfig straight from a raw ZNRecord and
    // bypasses that setter, so a negative weight would otherwise slip through (the guard rail's
    // "weight > capacity" check does not catch it either). Validate it explicitly.
    Map<String, Map<String, Integer>> partitionCapacityMap;
    try {
      partitionCapacityMap = resourceConfig.getPartitionCapacityMap();
    } catch (IOException e) {
      return Optional.of(badRequest(String.format(
          "Could not parse partition weight map for resource %s: %s",
          resourceConfig.getResourceName(), e.getMessage())));
    }
    for (Map.Entry<String, Map<String, Integer>> partitionEntry : partitionCapacityMap.entrySet()) {
      for (Map.Entry<String, Integer> dimensionEntry : partitionEntry.getValue().entrySet()) {
        if (dimensionEntry.getValue() != null && dimensionEntry.getValue() < 0) {
          return Optional.of(badRequest(String.format(
              "Partition weight for resource %s, partition '%s', dimension '%s' is negative (%d); "
                  + "weights must be non-negative.", resourceConfig.getResourceName(),
              partitionEntry.getKey(), dimensionEntry.getKey(), dimensionEntry.getValue())));
        }
      }
    }
    return Optional.empty();
  }

  @ResponseMetered(name = HttpConstants.WRITE_REQUEST)
  @Timed(name = HttpConstants.WRITE_REQUEST)
  @POST
  @Path("{resourceName}")
  public Response updateResource(@PathParam("clusterId") String clusterId,
      @PathParam("resourceName") String resourceName, @QueryParam("command") String command,
      @DefaultValue("-1") @QueryParam("replicas") int replicas,
      @DefaultValue("") @QueryParam("keyPrefix") String keyPrefix,
      @DefaultValue("") @QueryParam("group") String group) {
    Command cmd;
    try {
      cmd = Command.valueOf(command);
    } catch (Exception e) {
      return badRequest("Invalid command : " + command);
    }

    HelixAdmin admin = getHelixAdmin();
    try {
      switch (cmd) {
      case enable:
        admin.enableResource(clusterId, resourceName, true);
        break;
      case disable:
        admin.enableResource(clusterId, resourceName, false);
        break;
      case rebalance:
        if (replicas == -1) {
          return badRequest("Number of replicas is needed for rebalancing!");
        }
        keyPrefix = keyPrefix.length() == 0 ? resourceName : keyPrefix;
        admin.rebalance(clusterId, resourceName, replicas, keyPrefix, group);
        break;
      case enableWagedRebalance:
        try {
          admin.enableWagedRebalance(clusterId, Collections.singletonList(resourceName));
        } catch (HelixException e) {
          return badRequest(e.getMessage());
        }
        break;
      default:
        _logger.error("Unsupported command :" + command);
        return badRequest("Unsupported command :" + command);
      }
    } catch (Exception e) {
      _logger.error("Failed in updating resource : " + resourceName, e);
      return badRequest(e.getMessage());
    }
    return OK();
  }

  @ResponseMetered(name = HttpConstants.WRITE_REQUEST)
  @Timed(name = HttpConstants.WRITE_REQUEST)
  @DELETE
  @Path("{resourceName}")
  public Response deleteResource(@PathParam("clusterId") String clusterId,
      @PathParam("resourceName") String resourceName) {
    HelixAdmin admin = getHelixAdmin();
    try {
      admin.dropResource(clusterId, resourceName);
    } catch (Exception e) {
      _logger.error("Error in deleting a resource: " + resourceName, e);
      return serverError();
    }
    return OK();
  }

  @ResponseMetered(name = HttpConstants.READ_REQUEST)
  @Timed(name = HttpConstants.READ_REQUEST)
  @GET
  @Path("{resourceName}/configs")
  public Response getResourceConfig(@PathParam("clusterId") String clusterId,
      @PathParam("resourceName") String resourceName) {
    ConfigAccessor accessor = getConfigAccessor();
    ResourceConfig resourceConfig = accessor.getResourceConfig(clusterId, resourceName);
    if (resourceConfig != null) {
      return JSONRepresentation(resourceConfig.getRecord());
    }

    return notFound();
  }

  @ResponseMetered(name = HttpConstants.WRITE_REQUEST)
  @Timed(name = HttpConstants.WRITE_REQUEST)
  @POST
  @Path("{resourceName}/configs")
  public Response updateResourceConfig(@PathParam("clusterId") String clusterId,
      @PathParam("resourceName") String resourceName, @QueryParam("command") String commandStr,
      String content) {
    Command command;
    if (commandStr == null || commandStr.isEmpty()) {
      command = Command.update; // Default behavior to keep it backward-compatible
    } else {
      try {
        command = getCommand(commandStr);
      } catch (HelixException ex) {
        return badRequest(ex.getMessage());
      }
    }

    ZNRecord record;
    try {
      record = toZNRecord(content);
    } catch (IOException e) {
      _logger.error("Failed to deserialize user's input " + content + ", Exception: " + e);
      return badRequest("Input is not a valid ZNRecord!");
    }

    if (!resourceName.equals(record.getId())) {
      return badRequest("ID does not match the resourceName name in input!");
    }

    ResourceConfig resourceConfig = new ResourceConfig(record);
    ConfigAccessor configAccessor = getConfigAccessor();
    try {
      switch (command) {
      case update:
        configAccessor.updateResourceConfig(clusterId, resourceName, resourceConfig);
        break;
      case delete:
        HelixConfigScope resourceScope =
            new HelixConfigScopeBuilder(HelixConfigScope.ConfigScopeProperty.RESOURCE)
                .forCluster(clusterId).forResource(resourceName).build();
        configAccessor.remove(resourceScope, record);
        break;
      default:
        return badRequest(String.format("Unsupported command: %s", command));
      }
    } catch (HelixException ex) {
      return notFound(ex.getMessage());
    } catch (Exception ex) {
      _logger.error(String.format("Error in update resource config for resource: %s", resourceName),
          ex);
      return serverError(ex);
    }
    return OK();
  }

  @ResponseMetered(name = HttpConstants.READ_REQUEST)
  @Timed(name = HttpConstants.READ_REQUEST)
  @GET
  @Path("{resourceName}/idealState")
  public Response getResourceIdealState(@PathParam("clusterId") String clusterId,
      @PathParam("resourceName") String resourceName) {
    HelixAdmin admin = getHelixAdmin();
    IdealState idealState = admin.getResourceIdealState(clusterId, resourceName);
    if (idealState != null) {
      return JSONRepresentation(idealState.getRecord());
    }

    return notFound();
  }

  @ResponseMetered(name = HttpConstants.WRITE_REQUEST)
  @Timed(name = HttpConstants.WRITE_REQUEST)
  @POST
  @Path("{resourceName}/idealState")
  public Response updateResourceIdealState(@PathParam("clusterId") String clusterId,
      @PathParam("resourceName") String resourceName, @QueryParam("command") String commandStr,
      String content) {
    Command command;
    if (commandStr == null || commandStr.isEmpty()) {
      command = Command.update; // Default behavior is update
    } else {
      try {
        command = getCommand(commandStr);
      } catch (HelixException ex) {
        return badRequest(ex.getMessage());
      }
    }

    ZNRecord record;
    try {
      record = toZNRecord(content);
    } catch (IOException e) {
      _logger.error("Failed to deserialize user's input " + content + ", Exception: " + e);
      return badRequest("Input is not a valid ZNRecord!");
    }
    IdealState idealState = new IdealState(record);
    HelixAdmin helixAdmin = getHelixAdmin();
    try {
      switch (command) {
      case update:
        helixAdmin.updateIdealState(clusterId, resourceName, idealState);
        break;
      case delete: {
        helixAdmin.removeFromIdealState(clusterId, resourceName, idealState);
      }
        break;
      default:
        return badRequest(String.format("Unsupported command: %s", command));
      }
    } catch (HelixException ex) {
      return notFound(ex.getMessage()); // HelixAdmin throws a HelixException if it doesn't
                                        // exist already
    } catch (Exception ex) {
      _logger.error(String.format("Failed to update the IdealState for resource: %s", resourceName),
          ex);
      return serverError(ex);
    }
    return OK();
  }

  @ResponseMetered(name = HttpConstants.READ_REQUEST)
  @Timed(name = HttpConstants.READ_REQUEST)
  @GET
  @Path("{resourceName}/externalView")
  public Response getResourceExternalView(@PathParam("clusterId") String clusterId,
      @PathParam("resourceName") String resourceName) {
    HelixAdmin admin = getHelixAdmin();
    ExternalView externalView = admin.getResourceExternalView(clusterId, resourceName);
    if (externalView != null) {
      return JSONRepresentation(externalView.getRecord());
    }

    return notFound();
  }

  @ResponseMetered(name = HttpConstants.READ_REQUEST)
  @Timed(name = HttpConstants.READ_REQUEST)
  @GET
  @Path("{resourceName}/{customizedStateType}/customizedView")
  public Response getResourceCustomizedView(@PathParam("clusterId") String clusterId,
      @PathParam("resourceName") String resourceName,
      @PathParam("customizedStateType") String customizedStateType) {
    HelixAdmin admin = getHelixAdmin();
    CustomizedView customizedView =
        admin.getResourceCustomizedView(clusterId, resourceName, customizedStateType);
    if (customizedView != null) {
      return JSONRepresentation(customizedView.getRecord());
    }

    return notFound();
  }

  private Map<String, String> computePartitionHealth(String clusterId, String resourceName) {
    HelixAdmin admin = getHelixAdmin();
    IdealState idealState = admin.getResourceIdealState(clusterId, resourceName);
    ExternalView externalView = admin.getResourceExternalView(clusterId, resourceName);
    StateModelDefinition stateModelDef =
        admin.getStateModelDef(clusterId, idealState.getStateModelDefRef());
    String initialState = stateModelDef.getInitialState();
    List<String> statesPriorityList = stateModelDef.getStatesPriorityList();
    statesPriorityList = statesPriorityList.subList(0, statesPriorityList.indexOf(initialState)); // Trim
                                                                                                  // stateList
                                                                                                  // to
                                                                                                  // initialState
                                                                                                  // and
                                                                                                  // above
    int minActiveReplicas = idealState.getMinActiveReplicas();

    // Start the logic that determines the health status of each partition
    Map<String, String> partitionHealthResult = new HashMap<>();
    Set<String> allPartitionNames = idealState.getPartitionSet();
    if (!allPartitionNames.isEmpty()) {
      for (String partitionName : allPartitionNames) {
        int replicaCount =
            idealState.getReplicaCount(idealState.getPreferenceList(partitionName).size());
        // Simplify expectedStateCountMap by assuming that all instances are available to reduce
        // computation load on this REST endpoint
        LinkedHashMap<String, Integer> expectedStateCountMap =
            stateModelDef.getStateCountMap(replicaCount, replicaCount);
        // Extract all states into Collections from ExternalView
        Map<String, String> stateMapInExternalView = externalView.getStateMap(partitionName);
        Collection<String> allReplicaStatesInExternalView =
            (stateMapInExternalView != null && !stateMapInExternalView.isEmpty())
                ? stateMapInExternalView.values()
                : Collections.<String> emptyList();
        int numActiveReplicasInExternalView = 0;
        HealthStatus status = HealthStatus.HEALTHY;

        // Go through all states that are "active" states (higher priority than InitialState)
        for (int statePriorityIndex = 0; statePriorityIndex < statesPriorityList
            .size(); statePriorityIndex++) {
          String currentState = statesPriorityList.get(statePriorityIndex);
          int currentStateCountInIdealState = expectedStateCountMap.get(currentState);
          int currentStateCountInExternalView =
              Collections.frequency(allReplicaStatesInExternalView, currentState);
          numActiveReplicasInExternalView += currentStateCountInExternalView;
          // Top state counts must match, if not, unhealthy
          if (statePriorityIndex == 0
              && currentStateCountInExternalView != currentStateCountInIdealState) {
            status = HealthStatus.UNHEALTHY;
            break;
          } else if (currentStateCountInExternalView < currentStateCountInIdealState) {
            // For non-top states, if count in ExternalView is less than count in IdealState,
            // partially healthy
            status = HealthStatus.PARTIAL_HEALTHY;
          }
        }
        if (numActiveReplicasInExternalView < minActiveReplicas) {
          // If this partition does not satisfy the number of minimum active replicas, unhealthy
          status = HealthStatus.UNHEALTHY;
        }
        partitionHealthResult.put(partitionName, status.name());
      }
    }
    return partitionHealthResult;
  }
}
