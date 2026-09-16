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
import java.util.EnumMap;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.regex.Pattern;
import java.util.regex.PatternSyntaxException;
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
import org.apache.helix.model.ClusterConstraints.ConstraintAttribute;
import org.apache.helix.model.ClusterConstraints.ConstraintType;
import org.apache.helix.model.ClusterConstraints.ConstraintValue;
import org.apache.helix.model.ConstraintItem;
import org.apache.helix.model.Message.MessageType;
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
   * {@code CONSTRAINT_VALUE} (a non-negative integer or {@code ANY}) is required, along with at
   * least one other attribute. The body is fully validated before anything is written, so an
   * invalid attribute name, an invalid value, or a missing value is rejected with 400 rather than
   * being silently dropped. If a constraint with the same {@code constraintId} already exists it
   * is overwritten.
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

    String error = validateConstraintAttributes(attributes);
    if (error != null) {
      return badRequest("Invalid constraint " + constraintId + ". " + error);
    }

    ConstraintItem item;
    try {
      item = buildConstraintItem(attributes);
    } catch (RuntimeException e) {
      LOG.warn("Failed to build constraint {} of type {} for cluster {}.", constraintId,
          constraintType, clusterId, e);
      return badRequest("Invalid constraint " + constraintId + ": " + attributes + ". " + e);
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
   * Create or overwrite several constraint items of the same type in a single atomic write. The
   * request body maps each constraint id to its attribute map, for example:
   *
   * <pre>
   * {
   *   "limitBootstrapPerInstance": {
   *     "MESSAGE_TYPE": "STATE_TRANSITION",
   *     "TRANSITION": "OFFLINE-BOOTSTRAP",
   *     "INSTANCE": ".*",
   *     "CONSTRAINT_VALUE": "1"
   *   },
   *   "limitBootstrapPerResource": {
   *     "MESSAGE_TYPE": "STATE_TRANSITION",
   *     "TRANSITION": "OFFLINE-BOOTSTRAP",
   *     "RESOURCE": "myDB",
   *     "CONSTRAINT_VALUE": "5"
   *   }
   * }
   * </pre>
   *
   * Every item is validated before any of them is written, and the batch is applied as one update
   * to the constraint ZNode. Either all items land or none do, so a caller cannot end up with a
   * partially applied set of throttles.
   *
   * @param clusterId cluster name
   * @param constraintTypeStr one of {@link ConstraintType}
   * @param content JSON object mapping constraint id to its attribute map
   * @return 200 OK on success
   */
  @ClusterAuth
  @ResponseMetered(name = HttpConstants.WRITE_REQUEST)
  @Timed(name = HttpConstants.WRITE_REQUEST)
  @PUT
  @Path("{constraintType}")
  @ApiOperation(value = "Create or overwrite multiple constraint items atomically",
      notes = "Helix REST Constraints Batch Put API")
  public Response setConstraints(@PathParam("clusterId") String clusterId,
      @PathParam("constraintType") String constraintTypeStr, String content) {
    if (!doesClusterExist(clusterId)) {
      return notFound("Cluster " + clusterId + " does not exist");
    }
    ConstraintType constraintType = parseConstraintType(constraintTypeStr);
    if (constraintType == null) {
      return badRequest(invalidConstraintTypeMessage(constraintTypeStr));
    }

    Map<String, Map<String, String>> constraints;
    try {
      constraints =
          OBJECT_MAPPER.readValue(content, new TypeReference<Map<String, Map<String, String>>>() {
          });
    } catch (IOException e) {
      String errMsg = "Failed to parse constraints from request body: " + content;
      LOG.warn(errMsg, e);
      return badRequest(errMsg + " Exception: " + e.getMessage());
    }
    if (constraints == null || constraints.isEmpty()) {
      return badRequest("Request body must contain at least one constraint");
    }

    // Validate and build everything up front. Nothing is handed to the admin until the whole batch
    // is known to be good, so a single bad item cannot leave half a batch behind.
    Map<String, ConstraintItem> items = new LinkedHashMap<>();
    for (Map.Entry<String, Map<String, String>> entry : constraints.entrySet()) {
      String constraintId = entry.getKey();
      if (StringUtils.isBlank(constraintId)) {
        return badRequest("constraintId cannot be empty");
      }
      String error = validateConstraintAttributes(entry.getValue());
      if (error != null) {
        return badRequest("Invalid constraint " + constraintId + ". " + error);
      }
      try {
        items.put(constraintId, buildConstraintItem(entry.getValue()));
      } catch (RuntimeException e) {
        LOG.warn("Failed to build constraint {} of type {} for cluster {}.", constraintId,
            constraintType, clusterId, e);
        return badRequest(
            "Invalid constraint " + constraintId + ": " + entry.getValue() + ". " + e);
      }
    }

    try {
      getHelixAdmin().setConstraints(clusterId, constraintType, items);
    } catch (Exception e) {
      LOG.error("Failed to set constraints {} of type {} for cluster {}.", items.keySet(),
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

  /**
   * Validate a constraint attribute map before anything is written to ZooKeeper.
   * <p>
   * {@link ConstraintItemBuilder} logs and then silently drops any attribute it does not
   * understand, so relying on it alone persists a constraint that is not the one the caller asked
   * for. It also calls {@code ConstraintValue.valueOf} on the raw value, which throws a
   * {@link NullPointerException} for a null {@code CONSTRAINT_VALUE}. That is not an
   * {@link IllegalArgumentException}, so the builder's own catch misses it and the caller gets a
   * 500 for what is plainly bad input. Everything is therefore checked up front here.
   *
   * @param attributes constraint attribute name to value, as sent by the caller
   * @return a message describing the first problem found, or null when the input is valid
   */
  private static String validateConstraintAttributes(Map<String, String> attributes) {
    if (attributes == null || attributes.isEmpty()) {
      return "Constraint attributes cannot be empty";
    }

    // Keys are upper cased but never trimmed, matching what ConstraintItemBuilder does, so a key
    // that validates here is guaranteed to be the key the builder stores.
    Map<ConstraintAttribute, String> parsed = new EnumMap<>(ConstraintAttribute.class);
    for (Map.Entry<String, String> entry : attributes.entrySet()) {
      String key = entry.getKey();
      String value = entry.getValue();
      if (StringUtils.isBlank(key)) {
        return "Constraint attribute name cannot be empty";
      }
      ConstraintAttribute attribute;
      try {
        attribute = ConstraintAttribute.valueOf(key.toUpperCase());
      } catch (IllegalArgumentException e) {
        return "Unknown constraint attribute: " + key + ". Valid attributes are "
            + Arrays.toString(ConstraintAttribute.values());
      }
      if (StringUtils.isBlank(value)) {
        return "Constraint attribute " + attribute.name() + " requires a non-empty value";
      }
      if (parsed.put(attribute, value) != null) {
        return "Duplicate constraint attribute: " + attribute.name();
      }
    }

    String constraintValue = parsed.remove(ConstraintAttribute.CONSTRAINT_VALUE);
    if (constraintValue == null) {
      return "CONSTRAINT_VALUE is required. Use a non-negative integer or "
          + ConstraintValue.ANY.name();
    }
    if (!ConstraintValue.ANY.name().equals(constraintValue)) {
      int value;
      try {
        value = Integer.parseInt(constraintValue);
      } catch (NumberFormatException e) {
        return "Invalid CONSTRAINT_VALUE: " + constraintValue
            + ". Expected a non-negative integer or " + ConstraintValue.ANY.name();
      }
      if (value < 0) {
        return "Invalid CONSTRAINT_VALUE: " + constraintValue
            + ". A constraint value cannot be negative";
      }
    }
    if (parsed.isEmpty()) {
      return "Requires at least one constraint attribute besides CONSTRAINT_VALUE. Valid "
          + "attributes are " + Arrays.toString(ConstraintAttribute.values());
    }

    for (Map.Entry<ConstraintAttribute, String> entry : parsed.entrySet()) {
      // Attribute values are matched as regular expressions against an outgoing message, so an
      // uncompilable pattern blows up inside the controller pipeline instead of here.
      try {
        Pattern.compile(entry.getValue());
      } catch (PatternSyntaxException e) {
        return "Constraint attribute " + entry.getKey().name() + " value " + entry.getValue()
            + " is not a valid regular expression: " + e.getDescription();
      }
    }

    String messageType = parsed.get(ConstraintAttribute.MESSAGE_TYPE);
    if (messageType != null && !matchesAnyMessageType(messageType)) {
      return "Invalid MESSAGE_TYPE: " + messageType
          + ". It matches none of the known message types "
          + Arrays.toString(MessageType.values());
    }
    return null;
  }

  /**
   * MESSAGE_TYPE is matched as a regular expression against the message type of an outgoing
   * message, so a pattern such as {@code STATE_TRANSITION.*} is legal and must keep working. A
   * value that matches no known message type can never throttle anything, which is a typo rather
   * than a deliberately inert constraint.
   */
  private static boolean matchesAnyMessageType(String messageTypePattern) {
    for (MessageType messageType : MessageType.values()) {
      if (messageType.name().matches(messageTypePattern)) {
        return true;
      }
    }
    return false;
  }

  private static ConstraintItem buildConstraintItem(Map<String, String> attributes) {
    ConstraintItemBuilder builder = new ConstraintItemBuilder();
    builder.addConstraintAttributes(attributes);
    return builder.build();
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
