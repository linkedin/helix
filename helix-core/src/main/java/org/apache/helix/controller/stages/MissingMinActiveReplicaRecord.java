package org.apache.helix.controller.stages;

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
 * A record entry in cluster data cache tracking a partition whose active replica count has
 * dropped below its configured {@code minActiveReplicas}. It captures the moment the partition
 * was first observed below the minimum so that, once the count is restored, the controller can
 * compute how long the partition remained degraded (its recovery duration).
 * <p>
 * This mirrors {@link MissingTopStateRecord}, but tracks the active-replica-set boundary instead
 * of the single top-state holder. For v1 the start timestamp is stamped at detection time
 * (Option B); the field is intentionally the only durable input needed to derive the end-to-end
 * recovery duration when the partition heals.
 */
public class MissingMinActiveReplicaRecord {
  private final long startTimeStamp;
  private boolean failed;

  public MissingMinActiveReplicaRecord(long start) {
    startTimeStamp = start;
    failed = false;
  }

  /* package */ long getStartTimeStamp() {
    return startTimeStamp;
  }

  /* package */ void setFailed() {
    // Mark the record as failed once the partition has stayed below minActiveReplicas beyond the
    // configured recovery duration threshold, so the beyond-threshold gauge is only incremented once.
    failed = true;
  }

  /* package */ boolean isFailed() {
    return failed;
  }
}
