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

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.apache.helix.model.Message;

/**
 * Immutable result of one physical message dispatch attempt.
 */
public class MessageDispatchResult {
  private final List<Message> _sentMessages;
  private final List<Message> _failedMessages;

  public MessageDispatchResult(List<Message> sentMessages, List<Message> failedMessages) {
    _sentMessages =
        Collections.unmodifiableList(new ArrayList<>(sentMessages == null
            ? Collections.emptyList() : sentMessages));
    _failedMessages =
        Collections.unmodifiableList(new ArrayList<>(failedMessages == null
            ? Collections.emptyList() : failedMessages));
  }

  public List<Message> getSentMessages() {
    return _sentMessages;
  }

  public List<Message> getFailedMessages() {
    return _failedMessages;
  }
}
