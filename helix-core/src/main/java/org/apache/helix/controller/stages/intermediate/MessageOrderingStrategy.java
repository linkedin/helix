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

import org.apache.helix.model.Message;
import org.apache.helix.model.Partition;

/**
 * Strategy interface for ordering messages before throttling.
 * Different implementations can provide different prioritization schemes
 * (e.g., availability-aware vs. resource-priority based).
 */
@FunctionalInterface
public interface MessageOrderingStrategy {
  /**
   * Sort the given list of message contexts in-place according to the strategy's
   * prioritization logic.
   *
   * @param messages List of message contexts to sort (will be modified in-place)
   */
  void sortMessages(List<MessageContext> messages);

  /**
   * Data class to encapsulate a message with its context for ordering decisions.
   */
  class MessageContext {
    public final Message message;
    public final Partition partition;
    public final String resourceName;

    /**
     * The ordered preference list for this partition (instance names in preferred assignment order).
     * When two messages target the same state, the one targeting the instance that appears earlier
     * in this list is processed first.
     * <p>Used by: {@link ResourcePriorityOrderingStrategy} (preference-list ordering within a
     * partition).
     * <p>Not used by: {@link AvailabilityAwareOrderingStrategy}.
     * May be {@code null} if the strategy does not use it.
     */
    public final List<String> preferenceList;

    public MessageContext(Message message,
        Partition partition,
        String resourceName,
        List<String> preferenceList) {
      this.message = message;
      this.partition = partition;
      this.resourceName = resourceName;
      this.preferenceList = preferenceList;
    }
  }
}
