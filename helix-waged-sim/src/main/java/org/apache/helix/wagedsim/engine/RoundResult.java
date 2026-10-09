package org.apache.helix.wagedsim.engine;

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

/** What one round did. Layouts are resource -> partition -> instance -> state. */
public class RoundResult {
  public int round;
  public List<String> events = new ArrayList<>();
  /** Passes run in this round: global, partial, emergency, overwrite. */
  public Map<String, Long> passes = new LinkedHashMap<>();
  /** WAGED failures in this round, as TYPE/CATEGORY: message. */
  public List<String> failures = new ArrayList<>();
  public List<String> failureCategories = new ArrayList<>();
  public boolean maintenance;
  public boolean settled = true;
  public long computeMillis;
  public long settleMillis;
  public long simTimeMillis;
  public long messagesSent;
  public List<String> notes = new ArrayList<>();
}
