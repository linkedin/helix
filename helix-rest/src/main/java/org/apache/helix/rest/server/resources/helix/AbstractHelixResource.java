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
import java.util.Optional;

import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;

import org.apache.helix.BaseDataAccessor;
import org.apache.helix.ConfigAccessor;
import org.apache.helix.HelixAdmin;
import org.apache.helix.HelixDataAccessor;
import org.apache.helix.guardrail.GuardrailContext;
import org.apache.helix.guardrail.GuardrailPipeline;
import org.apache.helix.guardrail.ValidationResult;
import org.apache.helix.manager.zk.ZkBucketDataAccessor;
import org.apache.helix.rest.common.ContextPropertyKeys;
import org.apache.helix.rest.server.ServerContext;
import org.apache.helix.rest.server.resources.AbstractResource;
import org.apache.helix.task.TaskDriver;
import org.apache.helix.tools.ClusterSetup;
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
}
