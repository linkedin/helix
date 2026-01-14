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

import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.helix.controller.LogUtil;
import org.apache.helix.model.Message;
import org.apache.helix.model.Partition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Debug logger for availability-aware prioritization feature.
 * 
 * <p>This class contains verbose debug logging for the availability-aware feature rollout.
 * All logging is at DEBUG level and will only execute if debug logging is enabled.
 * 
 * <p>TODO: Remove this class after the availability-aware feature is stable.
 * To remove: Delete this file and remove calls from IntermediateStateCalcStage.
 */
public class AvailabilityAwarePrioritizationDebugLogger {

  private static final Logger LOG = LoggerFactory.getLogger(AvailabilityAwarePrioritizationDebugLogger.class);

  private final String _eventId;

  public AvailabilityAwarePrioritizationDebugLogger(String eventId) {
    _eventId = eventId;
  }

  /**
   * Check if debug logging is enabled.
   */
  public boolean isDebugEnabled() {
    return LOG.isDebugEnabled();
  }

  /**
   * Log collected messages before sorting.
   */
  public void logMessagesCollected(int messageCount) {
    if (!LOG.isDebugEnabled()) {
      return;
    }
    LogUtil.logDebug(LOG, _eventId, String.format(
        "AVAILABILITY_AWARE: Collected %d messages for cross-resource prioritization", messageCount));
  }

  /**
   * Log messages after sorting by availability impact.
   */
  public void logMessagesSorted(List<Message> messages, AvailabilityAwareMessageComparator comparator) {
    if (!LOG.isDebugEnabled()) {
      return;
    }
    LogUtil.logDebug(LOG, _eventId, String.format(
        "AVAILABILITY_AWARE: Sorted %d messages by availability impact", messages.size()));

    // Log top messages with their impact scores
    int logCount = Math.min(messages.size(), 10);
    for (int i = 0; i < logCount; i++) {
      Message msg = messages.get(i);
      double impact = comparator.getAvailabilityImpactForLogging(msg);
      LogUtil.logDebug(LOG, _eventId, String.format(
          "  [%d] Resource=%s, Partition=%s, %s->%s, Target=%s, Impact=%s",
          i, msg.getResourceName(), msg.getPartitionName(),
          msg.getFromState(), msg.getToState(), msg.getTgtName(),
          formatImpactScore(impact)));
    }
    if (messages.size() > logCount) {
      LogUtil.logDebug(LOG, _eventId, String.format("  ... and %d more messages", messages.size() - logCount));
    }
  }

  /**
   * Log message processing result (throttled or processed).
   */
  public void logMessageProcessed(Message message, boolean wasThrottled, String rebalanceType,
      double impactScore) {
    if (!LOG.isDebugEnabled()) {
      return;
    }
    LogUtil.logDebug(LOG, _eventId, String.format(
        "AVAILABILITY_AWARE: %s message Resource=%s, Partition=%s, %s->%s, Type=%s, Impact=%s",
        wasThrottled ? "THROTTLED" : "PROCESSED",
        message.getResourceName(), message.getPartitionName(),
        message.getFromState(), message.getToState(),
        rebalanceType, formatImpactScore(impactScore)));
  }

  /**
   * Log throttling summary.
   */
  public void logThrottlingSummary(int processedCount, int throttledCount, int totalCount) {
    if (!LOG.isDebugEnabled()) {
      return;
    }
    LogUtil.logDebug(LOG, _eventId, String.format(
        "AVAILABILITY_AWARE: Processing complete - Processed=%d, Throttled=%d, Total=%d",
        processedCount, throttledCount, totalCount));
  }

  /**
   * Log messages by target instance.
   */
  public void logMessagesByInstance(Map<String, Integer> messageCountByInstance) {
    if (!LOG.isDebugEnabled()) {
      return;
    }
    LogUtil.logDebug(LOG, _eventId, String.format(
        "AVAILABILITY_AWARE: Message distribution across %d instances", messageCountByInstance.size()));
    for (Map.Entry<String, Integer> entry : messageCountByInstance.entrySet()) {
      LogUtil.logDebug(LOG, _eventId, String.format("  Instance=%s, Messages=%d",
          entry.getKey(), entry.getValue()));
    }
  }

  /**
   * Log dispatched vs throttled messages summary.
   */
  public void logDispatchSummary(int dispatchedCount, int throttledRecovery, int throttledLoad) {
    if (!LOG.isDebugEnabled()) {
      return;
    }
    LogUtil.logDebug(LOG, _eventId, String.format(
        "AVAILABILITY_AWARE: Dispatch summary - Dispatched=%d, ThrottledRecovery=%d, ThrottledLoad=%d",
        dispatchedCount, throttledRecovery, throttledLoad));
  }

  /**
   * Log resource processing start.
   */
  public void logResourceProcessingStart(String resourceName, int partitionCount, int messageCount) {
    if (!LOG.isDebugEnabled()) {
      return;
    }
    LogUtil.logDebug(LOG, _eventId, String.format(
        "AVAILABILITY_AWARE: Processing resource=%s, partitions=%d, messages=%d",
        resourceName, partitionCount, messageCount));
  }

  /**
   * Log partition with error state.
   */
  public void logPartitionsWithError(String resourceName, Set<Partition> partitionsWithError) {
    if (!LOG.isDebugEnabled() || partitionsWithError.isEmpty()) {
      return;
    }
    LogUtil.logDebug(LOG, _eventId, String.format(
        "AVAILABILITY_AWARE: Resource=%s has %d partitions with ERROR state",
        resourceName, partitionsWithError.size()));
  }

  private String formatImpactScore(double impact) {
    if (impact >= Double.MAX_VALUE - 1) {
      return "MAX(TOP_STATE_MISSING)";
    }
    if (impact >= Double.MAX_VALUE - 1001) {
      return "MAX-1K(HANDOFF)";
    }
    return String.format("%.4f", impact);
  }
}

