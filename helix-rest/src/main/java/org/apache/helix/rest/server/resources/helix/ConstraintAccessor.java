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
import java.util.Arrays;
import java.util.Map;
import javax.ws.rs.DELETE;
import javax.ws.rs.GET;
import javax.ws.rs.PUT;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.core.Response;

import com.codahale.metrics.annotation.ResponseMetered;
import com.codahale.metrics.annotation.Timed;
import com.fasterxml.jackson.core.type.TypeReference;
import org.apache.commons.lang3.StringUtils;
import org.apache.helix.manager.zk.ZKUtil;
import org.apache.helix.model.ClusterConstraints;
import org.apache.helix.model.ClusterConstraints.ConstraintType;
import org.apache.helix.model.ConstraintItem;
import org.apache.helix.model.builder.ConstraintItemBuilder;
import org.apache.helix.rest.common.HttpConstants;
import org.apache.helix.rest.server.filters.ClusterAuth;
import org.apache.helix.zookeeper.api.client.RealmAwareZkClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import io.swagger.annotations.Api;
import io.swagger.annotations.ApiOperation;

/**
 * REST accessor for cluster-level {@link ClusterConstraints} (e.g. MESSAGE_CONSTRAINT,
 * STATE_CONSTRAINT). These constraints live at
 * {@code /{clusterName}/CONFIGS/CONSTRAINT/{constraintType}} in ZooKeeper. This accessor lets
 * operators create, read, and delete individual constraint items through the REST API instead of
 * editing ZNodes by hand.
 */
@Path("/clusters/{clusterId}/constraints")
@Api(value = "", description = "Helix REST Cluster Constraints APIs")
public class ConstraintAccessor extends AbstractHelixResource {
  private static final Logger LOG = LoggerFactory.getLogger(ConstraintAccessor.class.getName());

  /**
   * Return all constraint items for the given constraint type.
   * @param clusterId cluster name
   * @param constraintTypeStr one of {@link ConstraintType} (e.g. MESSAGE_CONSTRAINT)
   * @return the {@link ClusterConstraints} record for the type
   */
  @ClusterAuth
  @ResponseMetered(name = HttpConstants.READ_REQUEST)
  @Timed(name = HttpConstants.READ_REQUEST)
  @GET
  @Path("{constraintType}")
  @ApiOperation(value = "Get all constraints of a type", notes = "Helix REST Constraints Get API")
  public Response getConstraints(@PathParam("clusterId") String clusterId,
      @PathParam("constraintType") String constraintTypeStr) {
    if (!doesClusterExist(clusterId)) {
      return notFound("Cluster " + clusterId + " does not exist");
    }
    ConstraintType constraintType = parseConstraintType(constraintTypeStr);
    if (constraintType == null) {
      return badRequest(invalidConstraintTypeMessage(constraintTypeStr));
    }

    ClusterConstraints constraints = getHelixAdmin().getConstraints(clusterId, constraintType);
    if (constraints == null) {
      return notFound(
          "No " + constraintType.name() + " constraints found for cluster " + clusterId);
    }
    return JSONRepresentation(constraints.getRecord());
  }

  /**
   * Return a single constraint item.
   * @param clusterId cluster name
   * @param constraintTypeStr one of {@link ConstraintType}
   * @param constraintId the constraint item id (an arbitrary, caller-chosen unique label)
   * @return the constraint item's attribute map
   */
  @ClusterAuth
  @ResponseMetered(name = HttpConstants.READ_REQUEST)
  @Timed(name = HttpConstants.READ_REQUEST)
  @GET
  @Path("{constraintType}/{constraintId}")
  @ApiOperation(value = "Get a single constraint item", notes = "Helix REST Constraints Get API")
  public Response getConstraintItem(@PathParam("clusterId") String clusterId,
      @PathParam("constraintType") String constraintTypeStr,
      @PathParam("constraintId") String constraintId) {
    if (!doesClusterExist(clusterId)) {
      return notFound("Cluster " + clusterId + " does not exist");
    }
    ConstraintType constraintType = parseConstraintType(constraintTypeStr);
    if (constraintType == null) {
      return badRequest(invalidConstraintTypeMessage(constraintTypeStr));
    }

    ClusterConstraints constraints = getHelixAdmin().getConstraints(clusterId, constraintType);
    Map<String, String> item =
        constraints == null ? null : constraints.getRecord().getMapField(constraintId);
    if (item == null) {
      return notFound("Constraint " + constraintId + " of type " + constraintType.name()
          + " does not exist for cluster " + clusterId);
    }
    return JSONRepresentation(item);
  }

  /**
   * Create or overwrite a constraint item. The request body is a flat JSON object of constraint
   * attributes, for example:
   *
   * <pre>
   * {
   *   "MESSAGE_TYPE": "STATE_TRANSITION",
   *   "TRANSITION": "OFFLINE-BOOTSTRAP",
   *   "INSTANCE": "localhost_12918",
   *   "CONSTRAINT_VALUE": "0"
   * }
   * </pre>
   *
   * Attribute keys must be members of {@link ClusterConstraints.ConstraintAttribute}. A valid
   * {@code CONSTRAINT_VALUE} (an integer or {@code ANY}) is required. If a constraint
   * with the same {@code constraintId} already exists it is overwritten.
   *
   * @param clusterId cluster name
   * @param constraintTypeStr one of {@link ConstraintType}
   * @param constraintId the constraint item id (an arbitrary, caller-chosen unique label)
   * @param content JSON object mapping constraint attribute to value
   * @return 200 OK on success
   */
  @ClusterAuth
  @ResponseMetered(name = HttpConstants.WRITE_REQUEST)
  @Timed(name = HttpConstants.WRITE_REQUEST)
  @PUT
  @Path("{constraintType}/{constraintId}")
  @ApiOperation(value = "Create or overwrite a constraint item",
      notes = "Helix REST Constraints Put API")
  public Response setConstraint(@PathParam("clusterId") String clusterId,
      @PathParam("constraintType") String constraintTypeStr,
      @PathParam("constraintId") String constraintId, String content) {
    if (!doesClusterExist(clusterId)) {
      return notFound("Cluster " + clusterId + " does not exist");
    }
    ConstraintType constraintType = parseConstraintType(constraintTypeStr);
    if (constraintType == null) {
      return badRequest(invalidConstraintTypeMessage(constraintTypeStr));
    }
    if (StringUtils.isBlank(constraintId)) {
      return badRequest("constraintId cannot be empty");
    }

    Map<String, String> attributes;
    try {
      attributes = OBJECT_MAPPER.readValue(content, new TypeReference<Map<String, String>>() {
      });
    } catch (IOException e) {
      String errMsg = "Failed to parse constraint attributes from request body: " + content;
      LOG.warn(errMsg, e);
      return badRequest(errMsg + " Exception: " + e.getMessage());
    }
    if (attributes == null || attributes.isEmpty()) {
      return badRequest("Constraint attributes cannot be empty");
    }

    ConstraintItemBuilder builder = new ConstraintItemBuilder();
    builder.addConstraintAttributes(attributes);
    ConstraintItem item = builder.build();
    // Mirror the validation ClusterConstraints applies when loading from ZK: an item must carry at
    // least one recognized attribute and a valid constraint value. Unrecognized attribute keys or
    // an invalid CONSTRAINT_VALUE are dropped by the builder, so an empty result means bad input.
    if (item.getAttributes().isEmpty() || item.getConstraintValue() == null) {
      return badRequest("Invalid constraint. Requires at least one valid constraint attribute from "
          + Arrays.toString(ClusterConstraints.ConstraintAttribute.values())
          + " and a valid CONSTRAINT_VALUE (an integer or ANY). Parsed input: "
          + attributes);
    }

    try {
      getHelixAdmin().setConstraint(clusterId, constraintType, constraintId, item);
    } catch (Exception e) {
      LOG.error("Failed to set constraint {} of type {} for cluster {}.", constraintId,
          constraintType, clusterId, e);
      return serverError(e);
    }
    return OK();
  }

  /**
   * Remove a constraint item.
   * @param clusterId cluster name
   * @param constraintTypeStr one of {@link ConstraintType}
   * @param constraintId the constraint item id to remove
   * @return 200 OK on success
   */
  @ClusterAuth
  @ResponseMetered(name = HttpConstants.WRITE_REQUEST)
  @Timed(name = HttpConstants.WRITE_REQUEST)
  @DELETE
  @Path("{constraintType}/{constraintId}")
  @ApiOperation(value = "Remove a constraint item", notes = "Helix REST Constraints Delete API")
  public Response removeConstraint(@PathParam("clusterId") String clusterId,
      @PathParam("constraintType") String constraintTypeStr,
      @PathParam("constraintId") String constraintId) {
    if (!doesClusterExist(clusterId)) {
      return notFound("Cluster " + clusterId + " does not exist");
    }
    ConstraintType constraintType = parseConstraintType(constraintTypeStr);
    if (constraintType == null) {
      return badRequest(invalidConstraintTypeMessage(constraintTypeStr));
    }

    try {
      getHelixAdmin().removeConstraint(clusterId, constraintType, constraintId);
    } catch (Exception e) {
      LOG.error("Failed to remove constraint {} of type {} for cluster {}.", constraintId,
          constraintType, clusterId, e);
      return serverError(e);
    }
    return OK();
  }

  private static ConstraintType parseConstraintType(String constraintTypeStr) {
    if (constraintTypeStr == null) {
      return null;
    }
    try {
      return ConstraintType.valueOf(constraintTypeStr);
    } catch (IllegalArgumentException e) {
      return null;
    }
  }

  private static String invalidConstraintTypeMessage(String constraintTypeStr) {
    return "Invalid constraint type: " + constraintTypeStr + ". Valid types are "
        + Arrays.toString(ConstraintType.values());
  }

  private boolean doesClusterExist(String cluster) {
    RealmAwareZkClient zkClient = getRealmAwareZkClient();
    return ZKUtil.isClusterSetup(cluster, zkClient);
  }
}
