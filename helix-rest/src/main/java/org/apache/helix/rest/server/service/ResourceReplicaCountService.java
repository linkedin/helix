package org.apache.helix.rest.server.service;

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

import java.util.ArrayList;
import java.util.Collection;
import java.util.EnumMap;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;

import org.apache.helix.AccessOption;
import org.apache.helix.BaseDataAccessor;
import org.apache.helix.PropertyPathBuilder;
import org.apache.helix.model.IdealState;
import org.apache.helix.zookeeper.datamodel.ZNRecord;
import org.apache.helix.zookeeper.zkclient.exception.ZkBadVersionException;
import org.apache.zookeeper.data.Stat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Applies a caller-supplied replica count and/or minimum active replica count to a scoped set of
 * resources, and reports what happened to every selected resource.
 *
 * <p>This service exists so that a caller that wants the same two values on many resources does not
 * have to enumerate resources, build partial IdealStates and sequence one write per resource on its
 * own. The desired values themselves stay with the caller: this service never derives them.
 *
 * <p>Guarantees, and the limits of those guarantees:
 * <ul>
 *   <li><b>Not atomic.</b> Each resource is updated independently. A result that contains both
 *       applied and failed resources means the cluster really is in a mixed state. There is no
 *       rollback, because re-sending previous values would be a second unguarded write that could
 *       clobber whoever won the race.</li>
 *   <li><b>Never creates a resource.</b> Writes are version guarded, and a version guarded write
 *       does not create a missing znode. A resource that disappears between selection and its write
 *       is reported as {@link ResourceUpdateStatus#NOT_FOUND} and stays absent.</li>
 *   <li><b>Preserves unrelated fields.</b> Each attempt reads the current IdealState record, copies
 *       it, sets only the requested replica fields on the copy and writes the copy back guarded by
 *       the version that was read. A concurrent change to any other field invalidates the guard, so
 *       the concurrent change is re-read on the next attempt instead of being overwritten.</li>
 *   <li><b>Detects recreation, does not prevent it.</b> A delete and recreate resets the ZooKeeper
 *       data version to zero, so a version guard alone cannot prove that a write landed on the same
 *       znode incarnation. Each successful write is therefore followed by a check that the creation
 *       id is still the one that was read. A mismatch is reported as
 *       {@link ResourceUpdateStatus#CONFLICT} after the fact; it is not prevented.</li>
 *   <li><b>Checks the resulting values, not only the requested ones.</b> Setting one of the two
 *       counts can leave a resource whose minimum exceeds its replica count, so each resource is
 *       compared against the values it would end up with and is reported as
 *       {@link ResourceUpdateStatus#REJECTED} without a write when the combination cannot be
 *       satisfied.</li>
 * </ul>
 *
 * <p>Every outcome other than {@link ResourceUpdateStatus#APPLIED} and
 * {@link ResourceUpdateStatus#UNCHANGED} means the desired values were not observed on that
 * resource. Re-running the operation with the same desired values is safe: resources that already
 * carry them report {@link ResourceUpdateStatus#UNCHANGED} and are not written again.
 */
public class ResourceReplicaCountService {
  private static final Logger LOG = LoggerFactory.getLogger(ResourceReplicaCountService.class);

  /**
   * Number of read-compare-write attempts made per resource before a losing race is reported as a
   * conflict. Bounded so that one contended resource cannot hold up the rest of the selection.
   */
  static final int MAX_ATTEMPTS_PER_RESOURCE = 3;

  private static final String REPLICAS_FIELD = IdealState.IdealStateProperty.REPLICAS.name();

  /**
   * What happened to one selected resource.
   */
  public enum ResourceUpdateStatus {
    /** The resource did not carry the desired values and this operation wrote them. */
    APPLIED,
    /** The resource already carried the desired values, so nothing was written. */
    UNCHANGED,
    /** Another writer held the resource, so the desired values were not confirmed on it. */
    CONFLICT,
    /** The resource had no IdealState; this operation does not create one. */
    NOT_FOUND,
    /**
     * The desired values cannot be honoured on this resource, so nothing was written. This is a
     * rejection decided from the resource's own state, not an error.
     */
    REJECTED,
    /** The attempt ended in an error, so the state of the resource is unknown. */
    FAILED
  }

  /**
   * How the set of resources to update is chosen.
   */
  public enum ResourceSelection {
    /**
     * Every resource that has an IdealState in the cluster at the moment the selection is taken.
     * The selection is a snapshot: a resource created afterwards is not touched and is not
     * reported.
     */
    ALL_RESOURCES,
    /** Exactly the resources named by the caller. */
    EXPLICIT
  }

  /**
   * The outcome for a single resource, together with the IdealState data version that the outcome
   * was decided against.
   */
  public static class ResourceUpdateOutcome {
    private final ResourceUpdateStatus _status;
    private final int _version;
    private final String _message;

    ResourceUpdateOutcome(ResourceUpdateStatus status, int version, String message) {
      _status = status;
      _version = version;
      _message = message;
    }

    public ResourceUpdateStatus getStatus() {
      return _status;
    }

    /**
     * @return for {@link ResourceUpdateStatus#APPLIED} the data version produced by the write, for
     *         {@link ResourceUpdateStatus#UNCHANGED} the data version that already carried the
     *         desired values, and -1 when no version could be established.
     */
    public int getVersion() {
      return _version;
    }

    /**
     * @return an explanation for a non-applied outcome, or null.
     */
    public String getMessage() {
      return _message;
    }
  }

  /**
   * The complete result of one bulk operation: the resources that were selected, in selection
   * order, and one outcome for each of them.
   */
  public static class BulkReplicaCountUpdateResult {
    private final List<String> _selectedResources;
    private final Map<String, ResourceUpdateOutcome> _outcomes;

    BulkReplicaCountUpdateResult(List<String> selectedResources,
        Map<String, ResourceUpdateOutcome> outcomes) {
      _selectedResources = selectedResources;
      _outcomes = outcomes;
    }

    public List<String> getSelectedResources() {
      return _selectedResources;
    }

    public Map<String, ResourceUpdateOutcome> getOutcomes() {
      return _outcomes;
    }

    /**
     * @return a count for every status, including the statuses that did not occur, so that a
     *         consumer cannot mistake an absent key for a zero count.
     */
    public Map<ResourceUpdateStatus, Integer> getStatusCounts() {
      Map<ResourceUpdateStatus, Integer> counts = new EnumMap<>(ResourceUpdateStatus.class);
      for (ResourceUpdateStatus status : ResourceUpdateStatus.values()) {
        counts.put(status, 0);
      }
      for (ResourceUpdateOutcome outcome : _outcomes.values()) {
        counts.merge(outcome.getStatus(), 1, Integer::sum);
      }
      return counts;
    }

    /**
     * @return true only when every selected resource was observed carrying the desired values at
     *         the end of its own attempt. This is not a claim that the values are still in place
     *         now, only that no selected resource was left unconfirmed by this operation.
     */
    public boolean isAllAtDesiredValues() {
      if (_outcomes.size() != _selectedResources.size()) {
        return false;
      }
      return _outcomes.values().stream()
          .allMatch(outcome -> outcome.getStatus() == ResourceUpdateStatus.APPLIED
              || outcome.getStatus() == ResourceUpdateStatus.UNCHANGED);
    }
  }

  private final BaseDataAccessor<ZNRecord> _baseDataAccessor;

  public ResourceReplicaCountService(BaseDataAccessor<ZNRecord> baseDataAccessor) {
    _baseDataAccessor = baseDataAccessor;
  }

  /**
   * Takes a snapshot of the resources that currently have an IdealState in the cluster.
   *
   * @param clusterName the cluster to list
   * @return the resource names, or null when the cluster has no IdealState path, which callers
   *         should treat as an unknown cluster rather than as an empty selection.
   */
  public List<String> listResources(String clusterName) {
    List<String> resources = _baseDataAccessor
        .getChildNames(PropertyPathBuilder.idealState(clusterName), AccessOption.PERSISTENT);
    return resources == null ? null : new ArrayList<>(resources);
  }

  /**
   * Applies the desired replica counts to each given resource and reports one outcome per resource.
   *
   * <p>Resources are processed independently and the method does not stop at the first problem, so
   * that the caller receives the state of the whole selection rather than a prefix of it.
   *
   * @param clusterName the cluster that owns the resources
   * @param resources the selected resource names; duplicates are collapsed and selection order is
   *        preserved. An empty selection yields an empty result whose
   *        {@link BulkReplicaCountUpdateResult#isAllAtDesiredValues()} is true, because no resource
   *        was left unconfirmed. A caller that requires a non-empty scope must check the selection
   *        itself.
   * @param replicas the desired replica count, or null to leave the replica count untouched
   * @param minActiveReplicas the desired minimum active replica count, or null to leave it
   *        untouched
   * @return the outcome for every selected resource
   * @throws IllegalArgumentException if the desired values or the resource names are not valid.
   *         Validation happens before any write, so a rejected request changes nothing.
   */
  public BulkReplicaCountUpdateResult updateReplicaCounts(String clusterName,
      Collection<String> resources, Integer replicas, Integer minActiveReplicas) {
    validateReplicaCounts(replicas, minActiveReplicas);
    List<String> selected = validateAndDeduplicate(resources);

    Map<String, ResourceUpdateOutcome> outcomes = new LinkedHashMap<>();
    for (String resourceName : selected) {
      outcomes.put(resourceName,
          applyToResource(clusterName, resourceName, replicas, minActiveReplicas));
    }
    BulkReplicaCountUpdateResult result = new BulkReplicaCountUpdateResult(selected, outcomes);
    LOG.info(
        "Bulk replica count update on cluster {} for {} resources with replicas={}, "
            + "minActiveReplicas={} ended with {}.",
        clusterName, selected.size(), replicas, minActiveReplicas, result.getStatusCounts());
    return result;
  }

  /**
   * Rejects values that the rebalancer cannot honour. A replica count must be at least one, a
   * minimum active replica count cannot be negative, and a minimum cannot exceed the replica count
   * that is being set in the same request. Exposed so that a caller can reject a request before it
   * reads the metadata store.
   *
   * @param replicas the desired replica count, or null when it is not being set
   * @param minActiveReplicas the desired minimum active replica count, or null when it is not being
   *        set
   * @throws IllegalArgumentException when the combination cannot be honoured
   */
  public static void validateReplicaCounts(Integer replicas, Integer minActiveReplicas) {
    if (replicas == null && minActiveReplicas == null) {
      throw new IllegalArgumentException(
          "At least one of replicas or minActiveReplicas must be provided.");
    }
    if (replicas != null && replicas < 1) {
      throw new IllegalArgumentException(
          "replicas must be at least 1, but was " + replicas + ".");
    }
    if (minActiveReplicas != null && minActiveReplicas < 0) {
      throw new IllegalArgumentException(
          "minActiveReplicas must not be negative, but was " + minActiveReplicas + ".");
    }
    if (replicas != null && minActiveReplicas != null && minActiveReplicas > replicas) {
      throw new IllegalArgumentException("minActiveReplicas " + minActiveReplicas
          + " must not exceed replicas " + replicas + ".");
    }
  }

  /**
   * A resource name is used to build a znode path, so a name that carries a path separator would
   * address a different znode than the caller named. Such a name is rejected rather than resolved.
   */
  private static List<String> validateAndDeduplicate(Collection<String> resources) {
    if (resources == null) {
      throw new IllegalArgumentException("The selected resources must not be null.");
    }
    for (String resourceName : resources) {
      if (resourceName == null || resourceName.trim().isEmpty()) {
        throw new IllegalArgumentException("Resource names must not be blank.");
      }
      if (resourceName.contains("/")) {
        throw new IllegalArgumentException(
            "Resource name must not contain '/', but was " + resourceName + ".");
      }
    }
    return new ArrayList<>(new LinkedHashSet<>(resources));
  }

  private ResourceUpdateOutcome applyToResource(String clusterName, String resourceName,
      Integer replicas, Integer minActiveReplicas) {
    String path = PropertyPathBuilder.idealState(clusterName, resourceName);
    for (int attempt = 1; attempt <= MAX_ATTEMPTS_PER_RESOURCE; attempt++) {
      Stat stat = new Stat();
      ZNRecord current;
      try {
        current = _baseDataAccessor.get(path, stat, AccessOption.PERSISTENT);
      } catch (Exception e) {
        LOG.error("Failed to read the IdealState of resource {} in cluster {}.", resourceName,
            clusterName, e);
        return new ResourceUpdateOutcome(ResourceUpdateStatus.FAILED, -1,
            "Failed to read the IdealState: " + e.getMessage());
      }
      if (current == null) {
        return new ResourceUpdateOutcome(ResourceUpdateStatus.NOT_FOUND, -1,
            "The resource has no IdealState. This operation does not create one.");
      }
      String unsatisfiable = findUnsatisfiableCombination(current, replicas, minActiveReplicas);
      if (unsatisfiable != null) {
        return new ResourceUpdateOutcome(ResourceUpdateStatus.REJECTED, stat.getVersion(),
            unsatisfiable);
      }
      if (isAtDesiredValues(current, replicas, minActiveReplicas)) {
        return new ResourceUpdateOutcome(ResourceUpdateStatus.UNCHANGED, stat.getVersion(), null);
      }

      int readVersion = stat.getVersion();
      long readCreationId = stat.getCzxid();
      IdealState desired = new IdealState(new ZNRecord(current));
      if (replicas != null) {
        desired.setReplicas(Integer.toString(replicas));
      }
      if (minActiveReplicas != null) {
        desired.setMinActiveReplicas(minActiveReplicas);
      }

      boolean written;
      try {
        written = _baseDataAccessor.set(path, desired.getRecord(), readVersion,
            AccessOption.PERSISTENT);
      } catch (ZkBadVersionException e) {
        // The guard rejected the write because another writer committed after this read, so
        // nothing was changed here. Re-read on the next attempt rather than retrying against a
        // version that is already stale.
        LOG.debug(
            "Attempt {} to set replica counts on resource {} in cluster {} lost to a concurrent "
                + "write of version {}.",
            attempt, resourceName, clusterName, readVersion);
        continue;
      } catch (Exception e) {
        LOG.error("Failed to write the IdealState of resource {} in cluster {}.", resourceName,
            clusterName, e);
        return new ResourceUpdateOutcome(ResourceUpdateStatus.FAILED, -1,
            "Failed to write the IdealState: " + e.getMessage());
      }

      if (written) {
        return confirmIncarnation(clusterName, resourceName, path, readCreationId);
      }
      // A guarded write that reports failure without a version conflict did not reach the node at
      // all, which is how a removal between the read and the write surfaces. The next attempt
      // re-reads, so the resource is reported as missing instead of being recreated.
      LOG.debug("Attempt {} to set replica counts on resource {} in cluster {} was not applied.",
          attempt, resourceName, clusterName);
    }
    return new ResourceUpdateOutcome(ResourceUpdateStatus.CONFLICT, -1,
        "The desired values were not confirmed on this resource: each of the "
            + MAX_ATTEMPTS_PER_RESOURCE + " attempts either lost a race or was not applied.");
  }

  /**
   * Confirms that the write landed on the same znode incarnation that was read. A delete and
   * recreate resets the data version, so the version guard alone cannot rule that out; comparing
   * the creation id turns an otherwise silent case into a reported conflict.
   */
  private ResourceUpdateOutcome confirmIncarnation(String clusterName, String resourceName,
      String path, long readCreationId) {
    Stat afterWrite;
    try {
      afterWrite = _baseDataAccessor.getStat(path, AccessOption.PERSISTENT);
    } catch (Exception e) {
      LOG.error("Failed to confirm the IdealState write of resource {} in cluster {}.",
          resourceName, clusterName, e);
      return new ResourceUpdateOutcome(ResourceUpdateStatus.FAILED, -1,
          "The write was accepted but could not be confirmed: " + e.getMessage());
    }
    if (afterWrite == null) {
      return new ResourceUpdateOutcome(ResourceUpdateStatus.CONFLICT, -1,
          "The resource was removed right after the write, so the desired values are not in place.");
    }
    if (afterWrite.getCzxid() != readCreationId) {
      return new ResourceUpdateOutcome(ResourceUpdateStatus.CONFLICT, -1,
          "The resource was recreated during the update, so the write may have landed on a "
              + "different IdealState than the one that was read.");
    }
    return new ResourceUpdateOutcome(ResourceUpdateStatus.APPLIED, afterWrite.getVersion(), null);
  }

  /**
   * Compares against the stored replica field rather than {@link IdealState#getReplicas()}, because
   * that getter falls back to a derived value when the field is absent and would report a resource
   * as already correct while the field itself is still missing.
   */
  private static boolean isAtDesiredValues(ZNRecord record, Integer replicas,
      Integer minActiveReplicas) {
    if (replicas != null
        && !Integer.toString(replicas).equals(record.getSimpleField(REPLICAS_FIELD))) {
      return false;
    }
    return minActiveReplicas == null
        || new IdealState(record).getMinActiveReplicas() == minActiveReplicas;
  }

  /**
   * Checks the values the resource would end up with, not only the values named in the request, so
   * that changing one of the two counts cannot leave a resource whose minimum exceeds its replica
   * count. The check is skipped when the resulting replica count is not a plain number, for example
   * one of the documented special replica values, because those cannot be compared to a minimum.
   *
   * @return a description of why the combination cannot be honoured, or null when it can
   */
  private static String findUnsatisfiableCombination(ZNRecord record, Integer replicas,
      Integer minActiveReplicas) {
    String resultingReplicas = replicas != null ? Integer.toString(replicas)
        : record.getSimpleField(REPLICAS_FIELD);
    int resultingMinActive = minActiveReplicas != null ? minActiveReplicas
        : new IdealState(record).getMinActiveReplicas();
    if (resultingReplicas == null || resultingMinActive < 0) {
      return null;
    }
    int replicaCount;
    try {
      replicaCount = Integer.parseInt(resultingReplicas);
    } catch (NumberFormatException e) {
      return null;
    }
    if (resultingMinActive <= replicaCount) {
      return null;
    }
    return "The resource would be left with minActiveReplicas " + resultingMinActive
        + " above a replica count of " + replicaCount + ", which cannot be satisfied.";
  }
}
