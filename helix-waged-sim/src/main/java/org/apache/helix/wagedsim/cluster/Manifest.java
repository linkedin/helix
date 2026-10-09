package org.apache.helix.wagedsim.cluster;

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
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import com.fasterxml.jackson.annotation.JsonInclude;

/** Describes where a cluster folder came from and how faithful it is. Serialized as manifest.json. */
@JsonInclude(JsonInclude.Include.NON_NULL)
public class Manifest {
  public static final int FORMAT = 1;

  /** Fidelity flag: WAGED baseline and best possible were not available from the source. */
  public static final String NO_ASSIGNMENT_METADATA = "assignmentMetadata=seeded-from-current-state";
  /** Fidelity flag: current states were derived from the external view. */
  public static final String CURRENT_STATES_FROM_EXTERNAL_VIEW = "currentStates=derived-from-externalview";
  /** Fidelity flag: current states were derived from the best possible assignment. */
  public static final String CURRENT_STATES_FROM_BEST_POSSIBLE = "currentStates=derived-from-bestpossible";
  /** Fidelity flag: no participant history, so offline times are unknown. */
  public static final String NO_PARTICIPANT_HISTORY = "participantHistory=absent";
  /** Fidelity flag: the cluster was scaled down. */
  public static final String SCALED = "scaled";
  /** Fidelity flag: names were anonymized. */
  public static final String ANONYMIZED = "anonymized";

  public int format = FORMAT;
  public String cluster;
  /** rest, pensieve, folder, legacy or spec. */
  public String source;
  public String sourceDetail;
  /** Capture time in epoch millis; the dry run starts its virtual clock here. */
  public Long capturedAtMillis;
  public String capturedAt;
  public String controllerHelixVersion;
  public String toolHelixVersion;
  public List<String> fidelity = new ArrayList<>();
  public Map<String, Long> counts = new LinkedHashMap<>();
  public String contentSha256;
  public List<String> notes = new ArrayList<>();

  public void addFidelity(String flag) {
    if (!fidelity.contains(flag)) {
      fidelity.add(flag);
    }
  }

  public Manifest copy() {
    Manifest copy = new Manifest();
    copy.format = format;
    copy.cluster = cluster;
    copy.source = source;
    copy.sourceDetail = sourceDetail;
    copy.capturedAtMillis = capturedAtMillis;
    copy.capturedAt = capturedAt;
    copy.controllerHelixVersion = controllerHelixVersion;
    copy.toolHelixVersion = toolHelixVersion;
    copy.fidelity = new ArrayList<>(fidelity);
    copy.counts = new LinkedHashMap<>(counts);
    copy.contentSha256 = contentSha256;
    copy.notes = new ArrayList<>(notes);
    return copy;
  }
}
