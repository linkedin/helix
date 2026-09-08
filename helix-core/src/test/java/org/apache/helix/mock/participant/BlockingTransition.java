package org.apache.helix.mock.participant;

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

import java.util.concurrent.CountDownLatch;
import org.apache.helix.NotificationContext;
import org.apache.helix.model.Message;

/** Blocks the selected transition until timeout or shutdown interrupts it. */
public class BlockingTransition extends MockTransition {
  private final String _fromState;
  private final String _toState;

  public BlockingTransition(String fromState, String toState) {
    _fromState = fromState;
    _toState = toState;
  }

  @Override
  public void doTransition(Message message, NotificationContext context)
      throws InterruptedException {
    if (_fromState.equals(message.getFromState()) && _toState.equals(message.getToState())) {
      new CountDownLatch(1).await();
    }
  }
}
