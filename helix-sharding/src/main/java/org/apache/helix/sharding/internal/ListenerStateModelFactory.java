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

import org.apache.helix.participant.statemachine.StateModelFactory;
import org.apache.helix.sharding.ShardingStateTransitionListener;


/**
 * Internal factory that creates {@link ListenerStateModel} instances backed by
 * a {@link ShardingStateTransitionListener}.
 *
 * <p>This is the bridge between the simplified callback API
 * ({@link ShardingStateTransitionListener}) and Helix's
 * {@link StateModelFactory}/{@link org.apache.helix.participant.statemachine.StateModel}
 * mechanism.</p>
 */
public class ListenerStateModelFactory extends StateModelFactory<ListenerStateModel> {

  private final ShardingStateTransitionListener listener;

  public ListenerStateModelFactory(ShardingStateTransitionListener listener) {
    if (listener == null) {
      throw new IllegalArgumentException("State transition listener must not be null");
    }
    this.listener = listener;
  }

  @Override
  public ListenerStateModel createNewStateModel(String resourceName, String partitionName) {
    return new ListenerStateModel(partitionName, listener);
  }
}
