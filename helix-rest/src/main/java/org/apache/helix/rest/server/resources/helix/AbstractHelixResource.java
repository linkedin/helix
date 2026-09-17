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
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;

import org.apache.helix.BaseDataAccessor;
import org.apache.helix.ConfigAccessor;
import org.apache.helix.HelixAdmin;
import org.apache.helix.HelixDataAccessor;
import org.apache.helix.PropertyKey;
import org.apache.helix.controller.rebalancer.util.DelayedRebalanceUtil;
import org.apache.helix.guardrail.GuardrailContext;
import org.apache.helix.guardrail.GuardrailPipeline;
import org.apache.helix.guardrail.ValidationResult;
import org.apache.helix.manager.zk.ZkBucketDataAccessor;
import org.apache.helix.model.ClusterConfig;
import org.apache.helix.model.InstanceConfig;
import org.apache.helix.model.ParticipantHistory;
import org.apache.helix.rest.common.ContextPropertyKeys;
import org.apache.helix.rest.server.ServerContext;
import org.apache.helix.rest.server.resources.AbstractResource;
import org.apache.helix.task.TaskDriver;
import org.apache.helix.tools.ClusterSetup;
import org.apache.helix.tools.ClusterVerifiers.StrictMatchExternalViewVerifier;
import org.apache.helix.tools.ClusterVerifiers.ZkHelixClusterVerifier;
import org.apache.helix.zookeeper.api.client.RealmAwareZkClient;
import org.apache.helix.zookeeper.datamodel.ZNRecord;
import org.apache.helix.zookeeper.impl.client.ZkClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * This class provides methods to access Helix specific objects
 * such as cluster, instance, job, resource, workflow, etc in
 * metadata store.
 */
public class AbstractHelixResource extends AbstractResource {

  private static final Logger LOG = LoggerFactory.getLogger(AbstractHelixResource.class);

  private static final long CONVERGENCE_CHECK_PERIOD_MS = 1000;

  public RealmAwareZkClient getRealmAwareZkClient() {
    ServerContext serverContext = getServerContext();
    return serverContext.getRealmAwareZkClient();
  }

  @Deprecated
  public ZkClient getZkClient() {
    return (ZkClient) getRealmAwareZkClient();
  }

  public HelixAdmin getHelixAdmin() {
    ServerContext serverContext = getServerContext();
    return serverContext.getHelixAdmin();
  }

  public ClusterSetup getClusterSetup() {
    ServerContext serverContext = getServerContext();
    return serverContext.getClusterSetup();
  }

  public TaskDriver getTaskDriver(String clusterName) {
    ServerContext serverContext = getServerContext();
    return serverContext.getTaskDriver(clusterName);
  }

  public ConfigAccessor getConfigAccessor() {
    ServerContext serverContext = getServerContext();
    return serverContext.getConfigAccessor();
  }

  public HelixDataAccessor getDataAccssor(String clusterName) {
    ServerContext serverContext = getServerContext();
    return serverContext.getDataAccessor(clusterName);
  }

  protected BaseDataAccessor<byte[]> getByteArrayDataAccessor() {
    return getServerContext().getByteArrayZkBaseDataAccessor();
  }

  protected ZkBucketDataAccessor getZkBucketDataAccessor() {
    return getServerContext().getZkBucketDataAccessor();
  }

  protected static ZNRecord toZNRecord(String data)
      throws IOException {
    return ZNRECORD_READER.readValue(data);
  }

  private ServerContext getServerContext() {
    return (ServerContext) _application.getProperties()
        .get(ContextPropertyKeys.SERVER_CONTEXT.name());
  }

  /**
   * Runs guard rail rules against a proposed mutation before it is applied, supporting three modes:
   * <ul>
   *   <li><b>enforce</b> (default): if the mutation is unsafe, returns a {@code 400} response
   *       carrying the violations so the caller can abort before touching ZooKeeper;</li>
   *   <li><b>dryRun</b> ({@code dryRun=true}): never proceeds with the write and always returns a
   *       {@code 200} response with the verdict, so callers can "simulate" the operation;</li>
   *   <li><b>force</b> ({@code force=true}): proceeds even when the mutation is unsafe, logging the
   *       overridden violations. {@code dryRun} takes precedence over {@code force}.</li>
   * </ul>
   * Note: the verdict reflects only the guard rail rules evaluated here, not the full feasibility of
   * the underlying mutation. A feasible dry-run does not guarantee the subsequent write will
   * succeed, since the mutation may enforce additional preconditions of its own.
   * <p>
   * Note also that the verdict is computed from a snapshot of cluster state read at preflight time.
   * Because the cluster is a live, eventually-consistent system &mdash; the controller keeps
   * rebalancing and participants join and leave independently of this call &mdash; that state can
   * change between this read and the subsequent write. This check is therefore a best-effort early
   * abort, not a transactional gate: a {@code feasible} verdict does not lock the cluster, so a
   * mutation judged safe here may still race with a concurrent state change. The mutation's own
   * preconditions and the controller remain the authoritative safety net.
   * <p>
   * When this method returns {@link Optional#empty()} the caller should proceed with the mutation;
   * when it returns a response, the caller should return that response as-is.
   *
   * @param pipeline the rules to evaluate for this endpoint
   * @param context  the cluster state and mutation target
   * @param force    proceed even if the mutation is judged unsafe
   * @param dryRun   only simulate: return the verdict without ever performing the mutation
   * @return a response to return immediately, or empty if the caller should proceed
   */
  protected Optional<Response> preflight(GuardrailPipeline pipeline, GuardrailContext context,
      boolean force, boolean dryRun) {
    ValidationResult result = pipeline.validate(context);
    if (dryRun) {
      return Optional.of(verdictResponse(result, Response.Status.OK));
    }
    if (result.isFeasible()) {
      return Optional.empty();
    }
    if (force) {
      LOG.warn("Guard rail violations for cluster {} overridden via force=true: {}",
          context.getClusterName(), result.getViolations());
      return Optional.empty();
    }
    return Optional.of(verdictResponse(result, Response.Status.BAD_REQUEST));
  }

  private Response verdictResponse(ValidationResult result, Response.Status status) {
    try {
      return Response.status(status).entity(toJson(result))
          .type(MediaType.APPLICATION_JSON).build();
    } catch (IOException e) {
      LOG.error("Failed to serialize guard rail validation result", e);
      return serverError();
    }
  }

  /**
   * Requests an on-demand rebalance for a caller that asked to rebalance when a stoppable check
   * reported a min active replica failure. It is only requested when at least one instance is
   * offline or disabled inside the delayed rebalance window, which is the state an on-demand
   * rebalance overrides, and the cluster is converged, so the rebalance is not piled on top of
   * transitions still in flight. An instance that went offline before the last on-demand rebalance
   * is no longer inside that window, so a rebalance does not repeat itself.
   * <p>
   * The rebalance is requested and not waited on, because a REST request must not block on the
   * cluster converging. Failures are logged and swallowed because this is a side effect of the
   * check, not the answer the caller asked for.
   *
   * @param clusterId the cluster to rebalance
   */
  protected void rebalanceOnMinActiveReplicaFailure(String clusterId) {
    try {
      ClusterConfig clusterConfig = getConfigAccessor().getClusterConfig(clusterId);
      if (clusterConfig == null) {
        return;
      }
      if (!hasInstanceInDelayedRebalanceWindow(clusterId, clusterConfig)) {
        LOG.info("Cluster {}: skipping on-demand rebalance, no instance is held by the delayed "
            + "rebalance window", clusterId);
        return;
      }
      if (!isConverged(clusterId)) {
        LOG.info("Cluster {}: skipping on-demand rebalance, cluster is not converged", clusterId);
        return;
      }
      getHelixAdmin().onDemandRebalance(clusterId);
      LOG.info("Cluster {}: requested on-demand rebalance after a min active replica check failure",
          clusterId);
    } catch (Exception e) {
      LOG.error("Cluster {}: failed to request on-demand rebalance", clusterId, e);
    }
  }

  /**
   * @return true if the delayed rebalance window is currently keeping at least one offline or
   * disabled instance in the assignment, which is the only state an on-demand rebalance changes.
   */
  private boolean hasInstanceInDelayedRebalanceWindow(String clusterId,
      ClusterConfig clusterConfig) {
    HelixDataAccessor accessor = getDataAccssor(clusterId);
    PropertyKey.Builder keyBuilder = accessor.keyBuilder();
    Map<String, InstanceConfig> instanceConfigMap =
        accessor.getChildValuesMap(keyBuilder.instanceConfigs(), true);
    Set<String> allNodes = instanceConfigMap.keySet();
    Set<String> liveNodes = new HashSet<>(accessor.getChildNames(keyBuilder.liveInstances()));
    Set<String> liveEnabledNodes = liveNodes.stream()
        .filter(node -> instanceConfigMap.containsKey(node) && instanceConfigMap.get(node)
            .getInstanceEnabled()).collect(Collectors.toSet());

    Map<String, Long> instanceOfflineTimeMap = new HashMap<>();
    for (String instance : allNodes) {
      if (liveNodes.contains(instance)) {
        continue;
      }
      ParticipantHistory history = accessor.getProperty(keyBuilder.participantHistory(instance));
      if (history != null) {
        instanceOfflineTimeMap.put(instance, history.getLastOfflineTime());
      }
    }

    Set<String> activeNodes = DelayedRebalanceUtil.getActiveNodes(allNodes, liveEnabledNodes,
        instanceOfflineTimeMap, liveNodes, instanceConfigMap, clusterConfig);
    return activeNodes.size() > liveEnabledNodes.size();
  }

  /**
   * @return true if the cluster is converged right now. The timeout matches the polling period so
   * the state is evaluated at most twice, which keeps this an immediate check rather than a wait.
   */
  private boolean isConverged(String clusterId) {
    ZkHelixClusterVerifier verifier =
        new StrictMatchExternalViewVerifier.Builder(clusterId).setZkClient(getRealmAwareZkClient())
            .setLenientMatch(true).build();
    try {
      return verifier.verifyByPolling(CONVERGENCE_CHECK_PERIOD_MS, CONVERGENCE_CHECK_PERIOD_MS);
    } finally {
      // The zk client is owned by the server context, so this only drops the verifier's own state.
      verifier.close();
    }
  }
}
