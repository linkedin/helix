package org.apache.helix.controller.stages.intermediate;

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

/**
 * Strategy interface for ordering messages before throttling.
 * Different implementations can provide different prioritization schemes
 * (e.g., availability-aware vs. resource-priority based).
 */
public interface MessageOrderingStrategy {
  /**
   * Sort the given list of message contexts in-place according to the strategy's
   * prioritization logic.
   *
   * @param messages List of message contexts to sort (will be modified in-place)
   */
  void sortMessages(List<MessageContext> messages);

  /**
   * Data class to encapsulate message with its context for ordering decisions.
   */
  class MessageContext {
    public final org.apache.helix.model.Message message;
    public final org.apache.helix.model.Partition partition;
    public final String resourceName;
    public final org.apache.helix.model.StateModelDefinition stateModelDef;
    public final java.util.Map<String, Integer> requiredStates;

    public MessageContext(org.apache.helix.model.Message message,
        org.apache.helix.model.Partition partition,
        String resourceName,
        org.apache.helix.model.StateModelDefinition stateModelDef,
        java.util.Map<String, Integer> requiredStates) {
      this.message = message;
      this.partition = partition;
      this.resourceName = resourceName;
      this.stateModelDef = stateModelDef;
      this.requiredStates = requiredStates;
    }
  }
}

