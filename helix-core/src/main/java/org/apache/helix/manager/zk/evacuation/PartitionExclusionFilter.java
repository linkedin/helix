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
 * Interface for filtering partitions during evacuation completion checks.
 * Each exclusion type should implement this interface to provide its own filtering logic.
 */
public interface PartitionExclusionFilter {
  
  /**
   * Determines if a partition should be excluded from evacuation completion check.
   *
   * @param partition The partition name
   * @param state The current state of the partition
   * @param resourceName The resource name
   * @return true if partition should be excluded (not block evacuation), false otherwise
   */
  boolean shouldExclude(String partition, String state, String resourceName);
}
