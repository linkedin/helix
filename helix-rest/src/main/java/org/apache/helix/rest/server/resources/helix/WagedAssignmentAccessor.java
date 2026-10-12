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

import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.stream.Collectors;
import javax.ws.rs.DefaultValue;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.Response;

import com.codahale.metrics.annotation.ResponseMetered;
import com.codahale.metrics.annotation.Timed;
import org.apache.helix.AccessOption;
import org.apache.helix.BaseDataAccessor;
import org.apache.helix.HelixProperty;
import org.apache.helix.controller.rebalancer.waged.AssignmentMetadataStore;
import org.apache.helix.model.Partition;
import org.apache.helix.model.ResourceAssignment;
import org.apache.helix.rest.common.HttpConstants;
import org.apache.helix.rest.server.filters.ClusterAuth;
import org.apache.zookeeper.data.Stat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Read-only access to the WAGED assignments the controller persisted in the assignment metadata
 * store, decoded into plain JSON.
 * <p>
 * The controller writes these assignments through
 * {@link org.apache.helix.manager.zk.ZkBucketDataAccessor}, which GZIPs the serialized record and
 * splits it across numbered bucket ZNodes. Reading the raw ZNodes, for example through
 * {@code /zookeeper/{path}?command=getBinaryData}, therefore returns opaque compressed chunks that
 * a caller cannot interpret. This accessor performs the bucket reassembly, decompression, and
 * per-resource deserialization server side and returns the resulting partition placements.
 * <p>
 * Note that this reports what WAGED last <i>persisted</i>, not the live mapping. The controller
 * still applies delayed-rebalance min-active adjustments and user-defined preference lists on top
 * of it, the states are provisional, and it is only rewritten on a successful rebalance. Use the
 * IdealState or ExternalView for the effective mapping, and
 * {@code /clusters/{clusterId}/partitionAssignment} for what-if computation.
 */
@ClusterAuth
@Path("/clusters/{clusterId}/wagedAssignment")
public class WagedAssignmentAccessor extends AbstractHelixResource {
  private static final Logger LOG = LoggerFactory.getLogger(WagedAssignmentAccessor.class);

  // Bookkeeping ZNodes written by ZkBucketDataAccessor alongside the versioned payload.
  private static final String LAST_SUCCESSFUL_WRITE_KEY = "LAST_SUCCESSFUL_WRITE";
  private static final String LAST_WRITE_KEY = "LAST_WRITE";
  private static final String METADATA_KEY = "METADATA";
  private static final int MAX_READ_ATTEMPTS = 2;

  private static final String CLUSTER_ID_FIELD = "cluster";
  private static final String ASSIGNMENT_TYPE_FIELD = "assignmentType";
  private static final String FORMAT_FIELD = "format";
  private static final String METADATA_FIELD = "metadata";
  private static final String ASSIGNMENT_FIELD = "assignment";

  public enum AssignmentType {
    BASELINE,
    BEST_POSSIBLE
  }

  /**
   * Result shape. Mirrors the options offered by
   * {@link ResourceAssignmentOptimizerAccessor} so both assignment APIs can be consumed the
   * same way.
   */
  public enum AssignmentFormat {
    /** resource -> partition -> instance -> state. */
    IdealStateFormat,
    /** instance -> resource -> partition -> state. */
    CurrentStateFormat
  }

  /**
   * Sample HTTP URL:
   * {@code GET /clusters/{clusterId}/wagedAssignment/bestPossible?format=IdealStateFormat&resources=db0,db1}
   * <p>
   * Returns the best possible assignment last persisted by the WAGED rebalancer. This is the
   * rebalancer's own output, before the controller applies delayed-rebalance min-active
   * adjustments and user-defined preference lists, and the states are provisional. It is only
   * updated on a successful rebalance. Use the IdealState or ExternalView for the effective
   * mapping.
   *
   * @param clusterId       the cluster to read
   * @param formatStr       {@link AssignmentFormat}, defaults to {@code IdealStateFormat}
   * @param resources       optional comma separated resource allowlist
   * @param instances       optional comma separated instance allowlist
   * @param partitions      optional comma separated partition allowlist
   * @param includeMetadata whether to include the persisted write bookkeeping, defaults to true
   * @return the decoded assignment
   */
  @ResponseMetered(name = HttpConstants.READ_REQUEST)
  @Timed(name = HttpConstants.READ_REQUEST)
  @GET
  @Path("bestPossible")
  public Response getBestPossibleAssignment(@PathParam("clusterId") String clusterId,
      @QueryParam("format") @DefaultValue("IdealStateFormat") String formatStr,
      @QueryParam("resources") String resources, @QueryParam("instances") String instances,
      @QueryParam("partitions") String partitions,
      @QueryParam("includeMetadata") @DefaultValue("true") boolean includeMetadata) {
    return getAssignment(clusterId, AssignmentType.BEST_POSSIBLE, formatStr, resources, instances,
        partitions, includeMetadata);
  }

  /**
   * Sample HTTP URL:
   * {@code GET /clusters/{clusterId}/wagedAssignment/baseline?format=CurrentStateFormat}
   * <p>
   * Returns the baseline assignment last persisted by the WAGED rebalancer, which is the steady
   * state placement it computes ignoring transient conditions such as instances being temporarily
   * down. It is only updated on a successful global rebalance.
   *
   * @param clusterId       the cluster to read
   * @param formatStr       {@link AssignmentFormat}, defaults to {@code IdealStateFormat}
   * @param resources       optional comma separated resource allowlist
   * @param instances       optional comma separated instance allowlist
   * @param partitions      optional comma separated partition allowlist
   * @param includeMetadata whether to include the persisted write bookkeeping, defaults to true
   * @return the decoded assignment
   */
  @ResponseMetered(name = HttpConstants.READ_REQUEST)
  @Timed(name = HttpConstants.READ_REQUEST)
  @GET
  @Path("baseline")
  public Response getBaselineAssignment(@PathParam("clusterId") String clusterId,
      @QueryParam("format") @DefaultValue("IdealStateFormat") String formatStr,
      @QueryParam("resources") String resources, @QueryParam("instances") String instances,
      @QueryParam("partitions") String partitions,
      @QueryParam("includeMetadata") @DefaultValue("true") boolean includeMetadata) {
    return getAssignment(clusterId, AssignmentType.BASELINE, formatStr, resources, instances,
        partitions, includeMetadata);
  }

  private Response getAssignment(String clusterId, AssignmentType assignmentType, String formatStr,
      String resources, String instances, String partitions, boolean includeMetadata) {
    AssignmentFormat format;
    try {
      format = AssignmentFormat.valueOf(formatStr);
    } catch (IllegalArgumentException e) {
      return badRequest(String.format("Invalid format: %s. Supported formats are %s", formatStr,
          Arrays.toString(AssignmentFormat.values())));
    }

    Set<String> resourceFilter = parseFilter(resources);
    Set<String> instanceFilter = parseFilter(instances);
    Set<String> partitionFilter = parseFilter(partitions);
    String rootPath = getAssignmentPath(clusterId, assignmentType);
    String lastSuccessfulWritePath = rootPath + "/" + LAST_SUCCESSFUL_WRITE_KEY;
    BaseDataAccessor<byte[]> accessor = getByteArrayDataAccessor();

    // The read is not atomic with the controller's writes and the GC of old versions, so the
    // version being read can vanish mid read. A read only counts if LAST_SUCCESSFUL_WRITE is the
    // same before and after it; versions only grow, so that pins the payload to that version.
    String version = null;
    Map<String, ResourceAssignment> assignments = null;
    Exception lastError = null;
    for (int attempt = 0; attempt < MAX_READ_ATTEMPTS && assignments == null; attempt++) {
      try {
        String before = readString(accessor, lastSuccessfulWritePath);
        if (before == null) {
          return notFound(String.format(
              "No %s assignment found at %s. The cluster may have no WAGED resources, or the "
                  + "controller may not have persisted an assignment yet.", assignmentType,
              rootPath));
        }
        HelixProperty combined =
            getZkBucketDataAccessor().compressedBucketRead(rootPath, HelixProperty.class);
        if (before.equals(readString(accessor, lastSuccessfulWritePath))) {
          assignments = AssignmentMetadataStore.splitAssignments(combined);
          version = before;
        }
      } catch (Exception e) {
        lastError = e;
      }
    }
    if (assignments == null) {
      LOG.error("Failed to read a consistent {} assignment for cluster {} at path {}",
          assignmentType, clusterId, rootPath, lastError);
      return serverError(String.format(
          "Failed to read a consistent %s assignment at %s. A concurrent write may have replaced "
              + "it, retry the request.%s", assignmentType, rootPath,
          lastError == null ? "" : " Last error: " + lastError.getMessage()));
    }

    Map<String, Object> response = new LinkedHashMap<>();
    response.put(CLUSTER_ID_FIELD, clusterId);
    response.put(ASSIGNMENT_TYPE_FIELD, assignmentType.name());
    response.put(FORMAT_FIELD, format.name());
    if (includeMetadata) {
      response.put(METADATA_FIELD, readWriteMetadata(accessor, rootPath, version));
    }
    response.put(ASSIGNMENT_FIELD,
        format == AssignmentFormat.CurrentStateFormat ? toCurrentStateFormat(assignments,
            resourceFilter, instanceFilter, partitionFilter)
            : toIdealStateFormat(assignments, resourceFilter, instanceFilter, partitionFilter));
    return JSONRepresentation(response);
  }

  private static String getAssignmentPath(String clusterId, AssignmentType assignmentType) {
    return assignmentType == AssignmentType.BASELINE ? AssignmentMetadataStore
        .getBaselinePath(clusterId) : AssignmentMetadataStore.getBestPossiblePath(clusterId);
  }

  private static Set<String> parseFilter(String csv) {
    if (csv == null || csv.trim().isEmpty()) {
      return Collections.emptySet();
    }
    return Arrays.stream(csv.split(",")).map(String::trim).filter(s -> !s.isEmpty())
        .collect(Collectors.toSet());
  }

  private static boolean included(Set<String> filter, String value) {
    return filter.isEmpty() || filter.contains(value);
  }

  /**
   * Describes the decoded version: when it was written and how large the persisted payload was.
   * {@code lastWriteVersion} is read live, so a value above {@code version} means a newer write is
   * in flight or failed. Best effort: if the version was garbage collected after the decode, the
   * missing fields are left out rather than failing the whole request.
   */
  private Map<String, Object> readWriteMetadata(BaseDataAccessor<byte[]> accessor, String rootPath,
      String version) {
    Map<String, Object> metadata = new LinkedHashMap<>();
    metadata.put("version", version);
    try {
      metadata.put("lastWriteVersion", readString(accessor, rootPath + "/" + LAST_WRITE_KEY));
      String versionPath = rootPath + "/" + version;
      Stat stat = accessor.getStat(versionPath, AccessOption.PERSISTENT);
      if (stat != null) {
        metadata.put("writeTimeMs", stat.getCtime());
      }
      byte[] rawBucketMetadata =
          accessor.get(versionPath + "/" + METADATA_KEY, null, AccessOption.PERSISTENT);
      if (rawBucketMetadata != null) {
        metadata.put("bucketMetadata", OBJECT_MAPPER.readValue(rawBucketMetadata, Map.class));
      }
    } catch (Exception e) {
      // Metadata is diagnostic only, so never fail the assignment read because of it.
      LOG.warn("Failed to read assignment write metadata at path {}", rootPath, e);
    }
    return metadata;
  }

  private static String readString(BaseDataAccessor<byte[]> accessor, String path) {
    byte[] bytes = accessor.get(path, null, AccessOption.PERSISTENT);
    return bytes == null ? null : new String(bytes);
  }

  /**
   * Builds resource -> partition -> instance -> state, matching the layout of an IdealState
   * map field.
   */
  private static Map<String, Map<String, Map<String, String>>> toIdealStateFormat(
      Map<String, ResourceAssignment> assignments, Set<String> resourceFilter,
      Set<String> instanceFilter, Set<String> partitionFilter) {
    Map<String, Map<String, Map<String, String>>> result = new TreeMap<>();
    for (Map.Entry<String, ResourceAssignment> entry : assignments.entrySet()) {
      String resource = entry.getKey();
      if (!included(resourceFilter, resource)) {
        continue;
      }
      Map<String, Map<String, String>> partitionMap = new TreeMap<>();
      for (Partition partition : entry.getValue().getMappedPartitions()) {
        String partitionName = partition.getPartitionName();
        if (!included(partitionFilter, partitionName)) {
          continue;
        }
        Map<String, String> replicaMap =
            filterReplicas(entry.getValue().getReplicaMap(partition), instanceFilter);
        if (!replicaMap.isEmpty()) {
          partitionMap.put(partitionName, replicaMap);
        }
      }
      if (!partitionMap.isEmpty()) {
        result.put(resource, partitionMap);
      }
    }
    return result;
  }

  /**
   * Builds instance -> resource -> partition -> state, which is the convenient shape when
   * asking what a single host is expected to carry.
   */
  private static Map<String, Map<String, Map<String, String>>> toCurrentStateFormat(
      Map<String, ResourceAssignment> assignments, Set<String> resourceFilter,
      Set<String> instanceFilter, Set<String> partitionFilter) {
    Map<String, Map<String, Map<String, String>>> result = new TreeMap<>();
    for (Map.Entry<String, ResourceAssignment> entry : assignments.entrySet()) {
      String resource = entry.getKey();
      if (!included(resourceFilter, resource)) {
        continue;
      }
      for (Partition partition : entry.getValue().getMappedPartitions()) {
        String partitionName = partition.getPartitionName();
        if (!included(partitionFilter, partitionName)) {
          continue;
        }
        for (Map.Entry<String, String> replica : entry.getValue().getReplicaMap(partition)
            .entrySet()) {
          if (!included(instanceFilter, replica.getKey())) {
            continue;
          }
          result.computeIfAbsent(replica.getKey(), k -> new TreeMap<>())
              .computeIfAbsent(resource, k -> new TreeMap<>())
              .put(partitionName, replica.getValue());
        }
      }
    }
    return result;
  }

  private static Map<String, String> filterReplicas(Map<String, String> replicaMap,
      Set<String> instanceFilter) {
    if (instanceFilter.isEmpty()) {
      return new TreeMap<>(replicaMap);
    }
    Map<String, String> filtered = new TreeMap<>();
    for (Map.Entry<String, String> replica : replicaMap.entrySet()) {
      if (instanceFilter.contains(replica.getKey())) {
        filtered.put(replica.getKey(), replica.getValue());
      }
    }
    return filtered;
  }
}
