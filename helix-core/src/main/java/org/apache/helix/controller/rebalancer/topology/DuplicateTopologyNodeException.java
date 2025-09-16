package org.apache.helix.controller.rebalancer.topology;

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

import org.apache.helix.HelixException;

/**
 * Exception thrown when duplicate leaf nodes are detected in the cluster topology.
 * This typically happens when multiple instances have the same end node identifier
 * in the topology configuration.
 */
public class DuplicateTopologyNodeException extends HelixException {
  private final String duplicateNodeName;
  private final String instanceName;

  public DuplicateTopologyNodeException(String duplicateNodeName, String instanceName) {
    super(String.format(
        "Failed to add topology node because duplicate leaf nodes are not allowed. " +
        "Duplicate node name: %s, Instance: %s", duplicateNodeName, instanceName));
    this.duplicateNodeName = duplicateNodeName;
    this.instanceName = instanceName;
  }

  public String getDuplicateNodeName() {
    return duplicateNodeName;
  }

  public String getInstanceName() {
    return instanceName;
  }
}