package org.apache.helix.sharding.internal;

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

import org.apache.helix.NotificationContext;
import org.apache.helix.model.Message;
import org.apache.helix.participant.statemachine.StateModel;
import org.apache.helix.participant.statemachine.StateModelInfo;
import org.apache.helix.participant.statemachine.Transition;
import org.apache.helix.sharding.ShardingStateTransitionListener;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * A LeaderStandby {@link StateModel} that delegates all transitions to a
 * {@link ShardingStateTransitionListener}.
 *
 * <p>This provides the simplest possible state model for users who just want
 * a callback when their partition state changes. Covers all LeaderStandby
 * transitions: OFFLINE↔STANDBY, STANDBY↔LEADER, OFFLINE→DROPPED.</p>
 */
@StateModelInfo(initialState = "OFFLINE", states = {"LEADER", "STANDBY"})
public class ListenerStateModel extends StateModel {

  private static final Logger LOG = LoggerFactory.getLogger(ListenerStateModel.class);

  private final String partitionName;
  private final ShardingStateTransitionListener listener;

  public ListenerStateModel(String partitionName, ShardingStateTransitionListener listener) {
    this.partitionName = partitionName;
    this.listener = listener;
  }

  @Transition(to = "STANDBY", from = "OFFLINE")
  public void onBecomeStandbyFromOffline(Message message, NotificationContext context) {
    LOG.debug("Partition {} transitioning OFFLINE → STANDBY", partitionName);
    listener.onStateTransition(partitionName, "OFFLINE", "STANDBY");
  }

  @Transition(to = "LEADER", from = "STANDBY")
  public void onBecomeLeaderFromStandby(Message message, NotificationContext context) {
    LOG.debug("Partition {} transitioning STANDBY → LEADER", partitionName);
    listener.onStateTransition(partitionName, "STANDBY", "LEADER");
  }

  @Transition(to = "STANDBY", from = "LEADER")
  public void onBecomeStandbyFromLeader(Message message, NotificationContext context) {
    LOG.debug("Partition {} transitioning LEADER → STANDBY", partitionName);
    listener.onStateTransition(partitionName, "LEADER", "STANDBY");
  }

  @Transition(to = "OFFLINE", from = "STANDBY")
  public void onBecomeOfflineFromStandby(Message message, NotificationContext context) {
    LOG.debug("Partition {} transitioning STANDBY → OFFLINE", partitionName);
    listener.onStateTransition(partitionName, "STANDBY", "OFFLINE");
  }

  @Transition(to = "DROPPED", from = "OFFLINE")
  public void onBecomeDroppedFromOffline(Message message, NotificationContext context) {
    LOG.debug("Partition {} transitioning OFFLINE → DROPPED", partitionName);
    listener.onStateTransition(partitionName, "OFFLINE", "DROPPED");
  }
}
