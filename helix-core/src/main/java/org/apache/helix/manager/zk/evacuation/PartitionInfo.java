package org.apache.helix.manager.zk.evacuation;

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

/**
 * Data class representing partition information for evacuation checks.
 */
public class PartitionInfo {
  private final String partitionName;
  private final String state;
  private final String resourceName;
  
  public PartitionInfo(String partitionName, String state, String resourceName) {
    this.partitionName = partitionName;
    this.state = state;
    this.resourceName = resourceName;
  }
  
  public String getPartitionName() {
    return partitionName;
  }
  
  public String getState() {
    return state;
  }
  
  public String getResourceName() {
    return resourceName;
  }
  
  @Override
  public String toString() {
    return String.format("PartitionInfo{partition=%s, state=%s, resource=%s}", 
        partitionName, state, resourceName);
  }
}
