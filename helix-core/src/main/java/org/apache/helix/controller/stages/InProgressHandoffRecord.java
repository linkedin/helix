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
 * A record entry in cluster data cache containing information about a partition's
 * in-progress top state handoff that exceeds threshold
 */
public class InProgressHandoffRecord {
  private final long startTimeStamp;
  private boolean beyondThreshold;

  public InProgressHandoffRecord(long start) {
    startTimeStamp = start;
    beyondThreshold = false;
  }

  public long getStartTimeStamp() {
    return startTimeStamp;
  }

  public void setBeyondThreshold() {
    // Mark InProgressHandoffRecord as beyond threshold if partition handoff duration exceeds threshold value.
    beyondThreshold = true;
  }

  public boolean isBeyondThreshold() {
    return beyondThreshold;
  }
}
