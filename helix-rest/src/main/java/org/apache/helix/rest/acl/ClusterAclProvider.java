package org.apache.helix.rest.acl;

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

import java.util.List;
import javax.servlet.http.HttpServletRequest;

import org.apache.zookeeper.data.ACL;


/**
 * Supplies the ZooKeeper ACL applied to the metadata nodes of a cluster created through the REST
 * server. See {@link org.apache.helix.HelixAdmin#addCluster(String, boolean, List)} for what the
 * ACL covers.
 * <p>
 * The REST server's own ZooKeeper identity must be granted {@code CREATE} by the returned ACL,
 * and {@code WRITE} when a cloud config is supplied at creation.
 * <p>
 * Not consulted when the creation request itself carries {@code acl} query parameters.
 */
public interface ClusterAclProvider {
  /**
   * @param request the cluster creation request
   * @param clusterId the cluster being created
   * @return the ACL for the cluster, or {@code null}/empty to fall back to the namespace's
   *         configured cluster ACL, and then to the client default ACL
   */
  List<ACL> getClusterAcl(HttpServletRequest request, String clusterId);
}
