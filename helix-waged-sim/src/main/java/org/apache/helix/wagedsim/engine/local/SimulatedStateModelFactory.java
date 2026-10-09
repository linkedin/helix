package org.apache.helix.wagedsim.engine.local;

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
import org.apache.helix.participant.statemachine.StateModelFactory;
import org.apache.helix.participant.statemachine.StateModelInfo;
import org.apache.helix.participant.statemachine.Transition;

/** A participant state model that completes any transition, optionally after a fixed latency. */
public class SimulatedStateModelFactory extends StateModelFactory<StateModel> {
  private final long _latencyMillis;

  public SimulatedStateModelFactory(long latencyMillis) {
    _latencyMillis = latencyMillis;
  }

  @Override
  public StateModel createNewStateModel(String resourceName, String partitionKey) {
    return new AnyTransition(_latencyMillis);
  }

  /** Accepts every transition. */
  @StateModelInfo(initialState = "OFFLINE", states = {})
  public static class AnyTransition extends StateModel {
    private final long _latency;

    AnyTransition(long latency) {
      _latency = latency;
    }

    @Transition(from = "*", to = "*")
    public void onTransition(Message message, NotificationContext context) throws InterruptedException {
      if (_latency > 0) {
        Thread.sleep(_latency);
      }
    }

    @Override
    public void reset() {
    }
  }
}
